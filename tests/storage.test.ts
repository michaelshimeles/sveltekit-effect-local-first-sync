import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { SyncRequest, SyncTransaction } from '../src/lib/shared/types.ts';
import { MemoryStorage } from '../src/lib/server/replication-store.ts';

function transaction(id: string, items: Array<{ id: string; name?: string }>): SyncTransaction {
	return {
		id,
		createdAt: Date.now(),
		changes: items.map((item, index) => ({
			mutationId: `${id}-mutation-${index}`,
			op: 'upsert',
			item: {
				id: item.id,
				name: item.name ?? item.id,
				note: '',
				stage: 'todo',
				updatedAt: Date.now() + index,
				deletedAt: null
			}
		}))
	};
}

function request(input: Partial<SyncRequest> & Pick<SyncRequest, 'clientId' | 'workspaceId'>): SyncRequest {
	return {
		protocolVersion: 2,
		cursor: '0',
		snapshot: null,
		limit: 100,
		transactions: [],
		...input
	};
}

test('isolates identical item IDs by workspace and paginates bootstrap snapshots', async () => {
	const storage = new MemoryStorage();
	await storage.applyChanges(
		request({
			clientId: 'client-a',
			workspaceId: 'workspace-a',
			transactions: [transaction('a-1', [{ id: 'same', name: 'A' }, { id: 'z', name: 'Z' }])]
		})
	);
	await storage.applyChanges(
		request({
			clientId: 'client-b',
			workspaceId: 'workspace-b',
			transactions: [transaction('b-1', [{ id: 'same', name: 'B' }])]
		})
	);

	const first = await storage.applyChanges(
		request({
			clientId: 'reader',
			workspaceId: 'workspace-a',
			cursor: null,
			snapshot: { id: 'snapshot-a', cursor: null, after: null },
			limit: 1
		})
	);
	assert.equal(first.snapshot?.items.length, 1);
	assert.equal(first.snapshot?.items[0].name, 'A');
	assert.equal(first.snapshot?.hasMore, true);

	const second = await storage.applyChanges(
		request({
			clientId: 'reader',
			workspaceId: 'workspace-a',
			cursor: null,
			snapshot: {
				id: 'snapshot-a',
				cursor: first.snapshot?.cursor ?? null,
				after: first.snapshot?.after ?? null
			},
			limit: 1
		})
	);
	assert.deepEqual(second.snapshot?.items.map((item) => item.name), ['Z']);
	assert.equal(second.snapshot?.hasMore, false);
});

test('pages whole multi-item commits and drains with an opaque cursor', async () => {
	const storage = new MemoryStorage();
	const first = await storage.applyChanges(
		request({
			clientId: 'writer',
			workspaceId: 'workspace',
			limit: 1,
			transactions: [
				transaction('tx-1', [{ id: 'a' }, { id: 'b' }]),
				transaction('tx-2', [{ id: 'c' }])
			]
		})
	);
	assert.equal(first.commits.length, 1);
	assert.equal(first.commits[0].transactionId, 'tx-1');
	assert.deepEqual(first.commits[0].items.map((item) => item.id), ['a', 'b']);
	assert.equal(first.hasMore, true);

	const second = await storage.applyChanges(
		request({
			clientId: 'reader',
			workspaceId: 'workspace',
			cursor: first.cursor,
			limit: 1
		})
	);
	assert.deepEqual(second.commits.map((commit) => commit.transactionId), ['tx-2']);
	assert.equal(second.hasMore, false);
	assert.equal(second.cursor, second.latestCursor);
});

test('bounds delta pages by records instead of multiplying commit and transaction limits', async () => {
	const storage = new MemoryStorage();
	const first = await storage.applyChanges(
		request({
			clientId: 'writer',
			workspaceId: 'workspace',
			limit: 2,
			transactions: [
				transaction('tx-1', [{ id: 'a' }]),
				transaction('tx-2', [{ id: 'b' }]),
				transaction('tx-3', [{ id: 'c' }])
			]
		})
	);
	assert.equal(first.commits.length, 2);
	assert.equal(first.commits.flatMap((commit) => commit.items).length, 2);
	assert.equal(first.hasMore, true);

	const second = await storage.applyChanges(
		request({
			clientId: 'reader',
			workspaceId: 'workspace',
			cursor: first.cursor,
			limit: 2
		})
	);
	assert.deepEqual(second.commits.map((commit) => commit.transactionId), ['tx-3']);
	assert.equal(second.hasMore, false);
});

test('retries are idempotent and return authoritative reconciliation', async () => {
	const storage = new MemoryStorage();
	const tx = transaction('retry', [{ id: 'a' }, { id: 'b' }]);
	await storage.applyChanges(
		request({ clientId: 'writer', workspaceId: 'workspace', transactions: [tx] })
	);
	const duplicate = await storage.applyChanges(
		request({ clientId: 'writer', workspaceId: 'workspace', transactions: [tx] })
	);

	assert.equal(duplicate.transactions[0].status, 'duplicate');
	assert.deepEqual(duplicate.reconciledItemIds, ['a', 'b']);
	assert.equal(duplicate.reconciledItems.length, 2);
	assert.equal(duplicate.latestCursor, '1');
});

test('compaction advances the floor and resets a stale client before accepting pushes', async () => {
	const storage = new MemoryStorage();
	const applied = await storage.applyChanges(
		request({
			clientId: 'writer',
			workspaceId: 'workspace',
			transactions: [transaction('old', [{ id: 'a' }])]
		})
	);
	const future = Date.now() + 31 * 24 * 60 * 60 * 1000;
	const compacted = await storage.compact('workspace', future);
	assert.equal(compacted.compactedCommits, 1);

	const stale = await storage.applyChanges(
		request({
			clientId: 'stale',
			workspaceId: 'workspace',
			cursor: '0',
			transactions: [transaction('must-not-apply', [{ id: 'resurrected' }])]
		})
	);
	assert.equal(stale.resetRequired, true);
	assert.equal(stale.transactions.length, 0);
	assert.equal(stale.latestCursor, applied.latestCursor);
});

test('resets a cursor ahead of server state after an ephemeral store restart', async () => {
	const storage = new MemoryStorage();
	const response = await storage.applyChanges(
		request({ clientId: 'old-client', workspaceId: 'workspace', cursor: '42' })
	);
	assert.equal(response.resetRequired, true);
	assert.equal(response.latestCursor, '0');
	assert.equal(response.cursor, null);
});

test('protocol v1 remains a full-snapshot compatibility path', async () => {
	const storage = new MemoryStorage();
	const response = await storage.applyChanges({
		protocolVersion: 1,
		clientId: 'legacy',
		workspaceId: 'default',
		cursor: null,
		snapshot: null,
		limit: 100,
		transactions: [transaction('legacy-tx', [{ id: 'legacy-item' }])]
	});
	assert.equal(response.protocolVersion, 1);
	assert.deepEqual(response.items?.map((item) => item.id), ['legacy-item']);
	assert.deepEqual(response.commits, []);
});
