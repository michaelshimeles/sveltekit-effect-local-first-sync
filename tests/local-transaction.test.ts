import 'fake-indexeddb/auto';
import assert from 'node:assert/strict';
import test from 'node:test';
import Dexie from 'dexie';
import { LocalFirstDatabase } from '../src/lib/client/local-db.ts';
import { runLocalTransaction } from '../src/lib/client/local-transaction.ts';

function idFactory(prefix: string) {
	let sequence = 0;
	return () => `${prefix}-${sequence++}`;
}

function createDatabase(label: string) {
	return new LocalFirstDatabase(`self-sync-test-${label}-${crypto.randomUUID()}`);
}

function options(db: LocalFirstDatabase, prefix: string, now: number) {
	return {
		db,
		clientId: 'client-a',
		workspaceId: 'workspace-a',
		createId: idFactory(prefix),
		now: () => now
	};
}

test('commits local records and grouped outbox mutations atomically', async (t) => {
	const db = createDatabase('commit');
	t.after(() => db.delete());

	const result = await runLocalTransaction(
		options(db, 'commit', 100),
		async (tx) => {
			await tx.insert({ id: 'a', name: 'A', note: '' });
			await tx.insert({ id: 'b', name: 'B', note: '', stage: 'doing' });
			return 'done';
		}
	);
	const items = await db.workspaceItems.where('workspaceId').equals('workspace-a').sortBy('id');
	const outbox = (await db.workspaceOutbox.toArray()).sort((a, b) => a.sequence - b.sequence);

	assert.equal(result.transactionId, 'commit-0');
	assert.equal(result.value, 'done');
	assert.equal(result.changeCount, 2);
	assert.deepEqual(
		items.map((item) => [item.id, item.syncStatus]),
		[
			['a', 'pending'],
			['b', 'pending']
		]
	);
	assert.deepEqual(
		outbox.map((mutation) => [mutation.transactionId, mutation.sequence, mutation.itemId]),
		[
			['commit-0', 0, 'a'],
			['commit-0', 1, 'b']
		]
	);
});

test('rolls back records and outbox entries when the callback throws', async (t) => {
	const db = createDatabase('rollback');
	t.after(() => db.delete());

	await assert.rejects(
		runLocalTransaction(
			options(db, 'rollback', 100),
			async (tx) => {
				await tx.insert({ id: 'a', name: 'A', note: '' });
				await tx.insert({ id: 'b', name: 'B', note: '' });
				throw new Error('abort');
			}
		),
		/abort/
	);

	assert.equal(await db.workspaceItems.count(), 0);
	assert.equal(await db.workspaceOutbox.count(), 0);
	assert.equal(await db.meta.count(), 0);
});

test('preserves transaction order when writes share a wall-clock millisecond', async (t) => {
	const db = createDatabase('clock');
	t.after(() => db.delete());

	await runLocalTransaction(
		options(db, 'first', 100),
		(tx) => tx.insert({ id: 'a', name: 'A', note: '' })
	);
	await runLocalTransaction(
		options(db, 'second', 100),
		(tx) => tx.insert({ id: 'b', name: 'B', note: '' })
	);

	const outbox = await db.workspaceOutbox
		.where('workspaceId')
		.equals('workspace-a')
		.sortBy('createdAt');
	assert.deepEqual(
		outbox.map((mutation) => [mutation.transactionId, mutation.createdAt]),
		[
			['first-0', 100],
			['second-0', 101]
		]
	);
});

test('compacts single-item edits without splitting multi-item transactions', async (t) => {
	const db = createDatabase('compact');
	t.after(() => db.delete());

	await runLocalTransaction(
		options(db, 'one', 100),
		(tx) => tx.insert({ id: 'a', name: 'A', note: '' })
	);
	await runLocalTransaction(
		options(db, 'two', 200),
		(tx) => tx.patch('a', { name: 'A2' })
	);

	assert.deepEqual(
		(await db.workspaceOutbox.toArray()).map((mutation) => mutation.transactionId),
		['two-0']
	);

	await runLocalTransaction(
		options(db, 'multi', 300),
		async (tx) => {
			await tx.patch('a', { stage: 'doing' });
			await tx.insert({ id: 'b', name: 'B', note: '' });
		}
	);
	await runLocalTransaction(
		options(db, 'latest', 400),
		(tx) => tx.patch('a', { name: 'A3' })
	);

	const transactionSizes = new Map<string, number>();
	for (const mutation of await db.workspaceOutbox.toArray()) {
		transactionSizes.set(
			mutation.transactionId,
			(transactionSizes.get(mutation.transactionId) ?? 0) + 1
		);
	}

	assert.deepEqual([...transactionSizes.entries()].sort(), [
		['latest-0', 1],
		['multi-0', 2]
	]);
});

test('an insert deleted in the same callback leaves no local trace', async (t) => {
	const db = createDatabase('cancel');
	t.after(() => db.delete());

	const result = await runLocalTransaction(
		options(db, 'cancel', 100),
		async (tx) => {
			await tx.insert({ id: 'a', name: 'A', note: '' });
			await tx.delete('a');
		}
	);

	assert.equal(result.changeCount, 0);
	assert.equal(await db.workspaceItems.count(), 0);
	assert.equal(await db.workspaceOutbox.count(), 0);
});

test('migrates v3 records and grouped outbox rows into the default workspace', async (t) => {
	const name = `self-sync-test-migration-${crypto.randomUUID()}`;
	const legacy = new Dexie(name);
	legacy.version(3).stores({
		items: 'id, syncStatus, updatedAt, deletedAt, revision, stage',
		outbox: 'id, transactionId, [transactionId+sequence], itemId, op, createdAt, attempts',
		meta: 'key'
	});
	await legacy.open();
	await legacy.table('items').put({
		id: 'legacy-item',
		name: 'Legacy',
		note: '',
		stage: 'doing',
		revision: 1,
		updatedAt: 100,
		deletedAt: null,
		sourceClientId: 'legacy-client',
		syncStatus: 'pending',
		lastError: null
	});
	await legacy.table('outbox').put({
		id: 'legacy-mutation',
		transactionId: 'legacy-transaction',
		sequence: 0,
		itemId: 'legacy-item',
		op: 'upsert',
		item: {
			id: 'legacy-item',
			name: 'Legacy',
			note: '',
			stage: 'doing',
			updatedAt: 100,
			deletedAt: null
		},
		createdAt: 100,
		attempts: 0,
		lastError: null
	});
	legacy.close();

	const migrated = new LocalFirstDatabase(name);
	t.after(() => migrated.delete());
	await migrated.open();
	const item = await migrated.workspaceItems.get(['default', 'legacy-item']);
	const mutation = await migrated.workspaceOutbox.get(['default', 'legacy-mutation']);

	assert.equal(item?.workspaceId, 'default');
	assert.equal(item?.stage, 'doing');
	assert.equal(mutation?.workspaceId, 'default');
	assert.equal(mutation?.transactionId, 'legacy-transaction');
});
