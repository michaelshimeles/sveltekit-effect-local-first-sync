import assert from 'node:assert/strict';

const baseUrl = process.env.BASE_URL ?? 'http://127.0.0.1:5174';
const runId = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
const clientId = `replication-verifier-${runId}`;
const workspaces = [`verify-a-${runId}`, `verify-b-${runId}`];
const itemId = `same-item-${runId}`;

function transaction(id, name, updatedAt = Date.now(), itemIds = [itemId]) {
	return {
		id,
		createdAt: updatedAt,
		changes: itemIds.map((idForItem, index) =>
			({
				mutationId: `${id}-mutation-${index}`,
				op: 'upsert',
				item: {
					id: idForItem,
					name: index === 0 ? name : `${name} extra`,
					note: 'Replication verification',
					stage: 'todo',
					updatedAt: updatedAt + index,
					deletedAt: null
				}
			})
		)
	};
}

async function postSync(workspaceId, input) {
	const response = await fetch(`${baseUrl}/api/sync`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({
			protocolVersion: 2,
			clientId,
			workspaceId,
			cursor: '0',
			snapshot: null,
			limit: 1,
			transactions: [],
			...input
		})
	});
	const body = await response.json();
	assert.equal(response.status, 200, JSON.stringify(body));
	assert.equal(response.headers.get('x-self-sync-cursor'), body.latestCursor);
	return body;
}

async function cleanupPostgres() {
	if (!process.env.DATABASE_URL) return;
	const pg = await import('pg');
	const url = new URL(process.env.DATABASE_URL);
	const sslMode = url.searchParams.get('sslmode')?.toLowerCase();
	if (sslMode === 'prefer' || sslMode === 'require' || sslMode === 'verify-ca') {
		url.searchParams.set('sslmode', 'verify-full');
	}
	const pool = new pg.default.Pool({ connectionString: url.toString() });
	const client = await pool.connect();
	try {
		await client.query('begin');
		await client.query('delete from sync_changes where workspace_id = any($1::text[])', [workspaces]);
		await client.query('delete from sync_commits where workspace_id = any($1::text[])', [workspaces]);
		await client.query('delete from sync_transactions where workspace_id = any($1::text[])', [workspaces]);
		await client.query('delete from sync_clients where workspace_id = any($1::text[])', [workspaces]);
		await client.query('delete from sync_items where workspace_id = any($1::text[])', [workspaces]);
		await client.query('delete from sync_workspace_state where workspace_id = any($1::text[])', [workspaces]);
		await client.query('commit');
	} catch (error) {
		await client.query('rollback').catch(() => {});
		throw error;
	} finally {
		client.release();
		await pool.end();
	}
}

async function cleanupMySql() {
	if (!process.env.DATABASE_URL) return;
	const mysql = await import('mysql2/promise');
	const pool = mysql.default.createPool({ uri: process.env.DATABASE_URL });
	const connection = await pool.getConnection();
	try {
		await connection.beginTransaction();
		for (const table of [
			'sync_changes',
			'sync_commits',
			'sync_transactions',
			'sync_clients',
			'sync_items',
			'sync_workspace_state'
		]) {
			await connection.query(`delete from ${table} where workspace_id in (?, ?)`, workspaces);
		}
		await connection.commit();
	} catch (error) {
		await connection.rollback().catch(() => {});
		throw error;
	} finally {
		connection.release();
		await pool.end();
	}
}

const healthResponse = await fetch(`${baseUrl}/api/health`);
const health = await healthResponse.json();
assert.equal(healthResponse.status, 200, JSON.stringify(health));

try {
	const timestamp = Date.now();
	const txId = `same-transaction-${runId}`;
	const extraItemId = `extra-item-${runId}`;
	const first = await postSync(workspaces[0], {
		transactions: [transaction(txId, 'Workspace A', timestamp, [itemId, extraItemId])]
	});
	const second = await postSync(workspaces[1], {
		transactions: [transaction(txId, 'Workspace B', timestamp)]
	});
	assert.equal(first.transactions[0]?.status, 'applied');
	assert.equal(second.transactions[0]?.status, 'applied');
	assert.equal(first.stats.transactionsProcessed, 1);
	assert.equal(first.stats.commitsReturned, 1);
	assert.equal(first.commits[0].items.length, 2);
	assert.equal(first.hasMore, false);

	const duplicate = await postSync(workspaces[0], {
		cursor: first.latestCursor,
		transactions: [transaction(txId, 'Workspace A', timestamp, [itemId, extraItemId])]
	});
	assert.equal(duplicate.transactions[0]?.status, 'duplicate');

	const snapshotA = await postSync(workspaces[0], {
		cursor: null,
		snapshot: { id: `snapshot-a-${runId}`, cursor: null, after: null },
		limit: 100
	});
	const snapshotB = await postSync(workspaces[1], {
		cursor: null,
		snapshot: { id: `snapshot-b-${runId}`, cursor: null, after: null }
	});
	assert.deepEqual(
		snapshotA.snapshot.items.map((item) => item.name).sort(),
		['Workspace A', 'Workspace A extra']
	);
	assert.deepEqual(snapshotB.snapshot.items.map((item) => item.name), ['Workspace B']);

	const deletedAt = timestamp + 1;
	const deleted = await postSync(workspaces[0], {
		cursor: first.latestCursor,
		transactions: [
			{
				id: `delete-${runId}`,
				createdAt: deletedAt,
				changes: [
					{
						mutationId: `delete-mutation-${runId}`,
						op: 'delete',
						item: {
							id: itemId,
							name: 'Workspace A',
							note: 'Replication verification',
							stage: 'todo',
							updatedAt: deletedAt,
							deletedAt
						}
					}
				]
			}
		]
	});
	assert.equal(deleted.transactions[0]?.status, 'applied');
	assert.equal(deleted.commits.at(-1)?.items[0]?.deletedAt, deletedAt);

	let oversizedRejected = false;
	try {
		const oversizedResponse = await fetch(`${baseUrl}/api/sync`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ padding: 'x'.repeat(1_000_000) })
		});
		oversizedRejected = oversizedResponse.status === 413;
	} catch (error) {
		// Some development servers terminate an oversized request before SvelteKit dispatches it.
		oversizedRejected = error?.cause?.code === 'ECONNRESET';
	}
	assert.equal(oversizedRejected, true, 'Oversized sync requests must be rejected');

	console.log(
		JSON.stringify(
			{
				ok: true,
				databaseMode: health.databaseMode,
				workspaceIsolation: true,
				transactionRetry: duplicate.transactions[0].status,
				cursor: deleted.latestCursor,
				tombstone: true,
				requestLimit: true
			},
			null,
			2
		)
	);
} finally {
	if (health.databaseMode === 'postgres') await cleanupPostgres();
	if (health.databaseMode === 'mysql') await cleanupMySql();
}
