import { browser } from '$app/environment';
import { liveQuery } from 'dexie';
import { getClientId, getWorkspaceId } from './identity';
import { db } from './local-db';
import { markOutboxFailed, realtimeActivity, syncActivity } from './local-store';
import {
	DEFAULT_SYNC_PAGE_LIMIT,
	MAX_CHANGES_PER_REQUEST,
	MAX_SYNC_TRANSACTIONS,
	SYNC_PROTOCOL_VERSION,
	compareReplicationCursors,
	workspaceMetaKey
} from '$lib/shared/replication';
import type {
	LocalItem,
	OutboxMutation,
	ReplicationCommit,
	ServerItem,
	SnapshotPage,
	SnapshotRequest,
	SyncResponse,
	SyncTransaction
} from '$lib/shared/types';

let syncInFlight = false;
let queuedSyncReason: SyncReason | null = null;
let scheduledSyncReason: SyncReason | null = null;
let scheduledSyncTimer: number | null = null;

type SyncReason =
	| 'manual'
	| 'interval'
	| 'online'
	| 'startup'
	| 'realtime'
	| 'local'
	| 'catchup';

const reasonPriority: Record<SyncReason, number> = {
	interval: 0,
	startup: 1,
	catchup: 2,
	online: 3,
	realtime: 4,
	local: 5,
	manual: 6
};

function preferredReason(current: SyncReason | null, next: SyncReason) {
	if (!current) return next;
	return reasonPriority[next] >= reasonPriority[current] ? next : current;
}

function syncMessage(reason: SyncReason) {
	if (reason === 'manual') return 'Syncing now';
	if (reason === 'local') return 'Syncing local changes';
	if (reason === 'realtime') return 'Realtime change received';
	if (reason === 'online') return 'Back online, syncing';
	if (reason === 'catchup') return 'Catching up';
	return 'Background sync';
}

function toLocalItem(item: ServerItem, snapshotId: string | null = null): LocalItem {
	return {
		...item,
		syncStatus: 'synced',
		lastError: null,
		snapshotId
	};
}

interface StoredSnapshotState extends SnapshotRequest {}

async function readReplicationState(workspaceId: string) {
	const cursor = (await db.meta.get(workspaceMetaKey(workspaceId, 'cursor')))?.value ?? null;
	const snapshotValue = (await db.meta.get(workspaceMetaKey(workspaceId, 'snapshot')))?.value;
	let snapshot: StoredSnapshotState | null = null;

	if (snapshotValue) {
		try {
			snapshot = JSON.parse(snapshotValue) as StoredSnapshotState;
		} catch {
			await db.meta.delete(workspaceMetaKey(workspaceId, 'snapshot'));
		}
	}

	if (cursor !== null) return { cursor, snapshot: null };
	if (snapshot) return { cursor: null, snapshot };

	const next: StoredSnapshotState = {
		id: crypto.randomUUID(),
		cursor: null,
		after: null
	};
	await db.meta.put({
		key: workspaceMetaKey(workspaceId, 'snapshot'),
		value: JSON.stringify(next)
	});
	return { cursor: null, snapshot: next };
}

function groupTransactions(queued: OutboxMutation[]): SyncTransaction[] {
	const groups = new Map<string, SyncTransaction>();
	const ordered = [...queued].sort(
		(a, b) => a.createdAt - b.createdAt || a.sequence - b.sequence || a.id.localeCompare(b.id)
	);

	for (const mutation of ordered) {
		const transaction = groups.get(mutation.transactionId) ?? {
			id: mutation.transactionId,
			createdAt: mutation.createdAt,
			changes: []
		};
		groups.set(mutation.transactionId, {
			...transaction,
			changes: [
				...transaction.changes,
				{ mutationId: mutation.id, op: mutation.op, item: mutation.item }
			]
		});
	}

	return [...groups.values()];
}

function selectTransactionBatch(queued: OutboxMutation[]) {
	const transactions = groupTransactions(queued);
	const selected: SyncTransaction[] = [];
	let changes = 0;

	for (const transaction of transactions) {
		if (selected.length >= MAX_SYNC_TRANSACTIONS) break;
		if (selected.length > 0 && changes + transaction.changes.length > MAX_CHANGES_PER_REQUEST) break;
		selected.push(transaction);
		changes += transaction.changes.length;
	}

	const transactionIds = new Set(selected.map((transaction) => transaction.id));
	return {
		transactions: selected,
		sent: queued.filter((mutation) => transactionIds.has(mutation.transactionId)),
		hasMore: selected.length < transactions.length
	};
}

async function deleteCompletedOutbox(
	workspaceId: string,
	response: SyncResponse,
	sent: OutboxMutation[]
) {
	const completedTransactionIds = new Set(
		(response.transactions ?? []).map((outcome) => outcome.transactionId)
	);
	const completedMutationIds = new Set(response.applied.map((outcome) => outcome.mutationId));

	for (const transactionId of completedTransactionIds) {
		await db.workspaceOutbox
			.where('[workspaceId+transactionId]')
			.equals([workspaceId, transactionId])
			.delete();
	}
	for (const mutation of sent) {
		if (completedMutationIds.has(mutation.id)) {
			await db.workspaceOutbox.delete([workspaceId, mutation.id]);
		}
	}
}

async function applyAuthoritativeItem(
	workspaceId: string,
	item: ServerItem,
	pendingItemIds: Set<string>,
	snapshotId: string | null = null
) {
	if (pendingItemIds.has(item.id)) {
		if (snapshotId) {
			await db.workspaceItems.update([workspaceId, item.id], { snapshotId });
		}
		return;
	}

	if (item.deletedAt !== null) {
		await db.workspaceItems.delete([workspaceId, item.id]);
		return;
	}

	await db.workspaceItems.put(toLocalItem(item, snapshotId));
}

async function applySnapshot(
	workspaceId: string,
	snapshot: SnapshotPage,
	pendingItemIds: Set<string>
) {
	for (const item of snapshot.items) {
		await applyAuthoritativeItem(workspaceId, item, pendingItemIds, snapshot.id);
	}

	if (snapshot.hasMore) {
		const nextState: StoredSnapshotState = {
			id: snapshot.id,
			cursor: snapshot.cursor,
			after: snapshot.after
		};
		await db.meta.put({
			key: workspaceMetaKey(workspaceId, 'snapshot'),
			value: JSON.stringify(nextState)
		});
		return;
	}

	const rows = await db.workspaceItems.where('workspaceId').equals(workspaceId).toArray();
	for (const item of rows) {
		if (pendingItemIds.has(item.id)) continue;
		if (item.snapshotId !== snapshot.id) {
			await db.workspaceItems.delete([workspaceId, item.id]);
		} else {
			await db.workspaceItems.update([workspaceId, item.id], { snapshotId: null });
		}
	}
	await db.meta.put({ key: workspaceMetaKey(workspaceId, 'cursor'), value: snapshot.cursor });
	await db.meta.delete(workspaceMetaKey(workspaceId, 'snapshot'));
}

async function applyCommits(
	workspaceId: string,
	commits: ReplicationCommit[],
	cursor: string | null,
	pendingItemIds: Set<string>
) {
	for (const commit of commits) {
		for (const item of commit.items) {
			await applyAuthoritativeItem(workspaceId, item, pendingItemIds);
		}
	}

	if (cursor !== null) {
		await db.meta.put({ key: workspaceMetaKey(workspaceId, 'cursor'), value: cursor });
	}
}

async function applyReconciliation(
	workspaceId: string,
	response: SyncResponse,
	pendingItemIds: Set<string>
) {
	const items = new Map(response.reconciledItems.map((item) => [item.id, item]));
	for (const itemId of response.reconciledItemIds) {
		if (pendingItemIds.has(itemId)) continue;
		const item = items.get(itemId);
		if (!item || item.deletedAt !== null) {
			await db.workspaceItems.delete([workspaceId, itemId]);
		} else {
			await db.workspaceItems.put(toLocalItem(item));
		}
	}
}

async function applyLegacySnapshot(
	workspaceId: string,
	items: ServerItem[],
	pendingItemIds: Set<string>
) {
	const serverIds = new Set(items.map((item) => item.id));
	for (const item of items) {
		await applyAuthoritativeItem(workspaceId, item, pendingItemIds);
	}
	const rows = await db.workspaceItems.where('workspaceId').equals(workspaceId).toArray();
	for (const item of rows) {
		if (item.syncStatus === 'synced' && !serverIds.has(item.id) && !pendingItemIds.has(item.id)) {
			await db.workspaceItems.delete([workspaceId, item.id]);
		}
	}
}

async function mergeSyncResponse(
	workspaceId: string,
	response: SyncResponse,
	sent: OutboxMutation[]
) {
	await db.transaction(
		'rw',
		db.workspaceItems,
		db.workspaceOutbox,
		db.meta,
		async () => {
			if (response.resetRequired) {
				await db.meta.delete(workspaceMetaKey(workspaceId, 'cursor'));
				await db.meta.delete(workspaceMetaKey(workspaceId, 'snapshot'));
				return;
			}

			await deleteCompletedOutbox(workspaceId, response, sent);
			const pending = await db.workspaceOutbox
				.where('workspaceId')
				.equals(workspaceId)
				.toArray();
			const pendingItemIds = new Set(pending.map((mutation) => mutation.itemId));

			await applyReconciliation(workspaceId, response, pendingItemIds);
			if (response.protocolVersion === 1 && response.items) {
				await applyLegacySnapshot(workspaceId, response.items, pendingItemIds);
				return;
			}
			if (response.snapshot) {
				await applySnapshot(workspaceId, response.snapshot, pendingItemIds);
			}
			await applyCommits(workspaceId, response.commits, response.cursor, pendingItemIds);
		}
	);
}

export function requestSync(reason: SyncReason = 'local', delay = 25) {
	if (!browser) return;
	scheduledSyncReason = preferredReason(scheduledSyncReason, reason);
	if (scheduledSyncTimer !== null) window.clearTimeout(scheduledSyncTimer);
	scheduledSyncTimer = window.setTimeout(() => {
		const nextReason = scheduledSyncReason ?? reason;
		scheduledSyncTimer = null;
		scheduledSyncReason = null;
		void syncNow(nextReason);
	}, delay);
}

export async function syncNow(reason: SyncReason = 'manual') {
	if (!browser) return;
	if (scheduledSyncTimer !== null) {
		window.clearTimeout(scheduledSyncTimer);
		scheduledSyncTimer = null;
		scheduledSyncReason = null;
	}
	if (syncInFlight) {
		queuedSyncReason = preferredReason(queuedSyncReason, reason);
		return;
	}
	if (!navigator.onLine) {
		syncActivity.set({
			status: 'offline',
			lastSyncedAt: null,
			lastMessage: 'Offline',
			error: null,
			databaseMode: 'unknown'
		});
		return;
	}

	syncInFlight = true;
	syncActivity.update((state) => ({
		...state,
		status: 'syncing',
		lastMessage: syncMessage(reason),
		error: null
	}));

	try {
		const workspaceId = getWorkspaceId();
		const replication = await readReplicationState(workspaceId);
		const queued = await db.workspaceOutbox
			.where('workspaceId')
			.equals(workspaceId)
			.sortBy('createdAt');
		const batch = replication.cursor === null
			? { transactions: [], sent: [], hasMore: queued.length > 0 }
			: selectTransactionBatch(queued);

		const response = await fetch('/api/sync', {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({
				protocolVersion: SYNC_PROTOCOL_VERSION,
				clientId: getClientId(),
				workspaceId,
				cursor: replication.cursor,
				snapshot: replication.snapshot,
				limit: DEFAULT_SYNC_PAGE_LIMIT,
				transactions: batch.transactions
			})
		});
		if (!response.ok) {
			const body = await response.text();
			throw new Error(body || `Sync failed with ${response.status}`);
		}

		const payload = (await response.json()) as SyncResponse;
		await mergeSyncResponse(workspaceId, payload, batch.sent);
		const appliedCount = payload.applied.filter((outcome) => outcome.status !== 'conflict').length;
		const conflictCount = payload.applied.filter((outcome) => outcome.status === 'conflict').length;
		const localCursor = (await db.meta.get(workspaceMetaKey(workspaceId, 'cursor')))?.value ?? null;
		const needsCatchup =
			payload.resetRequired ||
			payload.hasMore ||
			batch.hasMore ||
			(localCursor !== null && compareReplicationCursors(localCursor, payload.latestCursor) < 0);

		syncActivity.set({
			status: 'synced',
			lastSyncedAt: Date.now(),
			lastMessage:
				queued.length === 0
					? payload.hasMore
						? 'Applying server changes'
						: 'Server state current'
					: `${appliedCount} synced${conflictCount ? `, ${conflictCount} reconciled` : ''}`,
			error: null,
			databaseMode: payload.databaseMode
		});
		if (needsCatchup) requestSync('catchup', 0);
	} catch (error) {
		const message = error instanceof Error ? error.message : 'Sync failed';
		await markOutboxFailed(message);
		syncActivity.update((state) => ({
			...state,
			status: 'error',
			lastMessage: 'Sync failed',
			error: message
		}));
	} finally {
		syncInFlight = false;
		if (queuedSyncReason) {
			const nextReason = queuedSyncReason;
			queuedSyncReason = null;
			requestSync(nextReason, 0);
		}
	}
}

function realtimeUrl() {
	const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
	const clientId = encodeURIComponent(getClientId());
	const workspaceId = encodeURIComponent(getWorkspaceId());
	return `${protocol}//${window.location.host}/api/realtime?clientId=${clientId}&workspaceId=${workspaceId}`;
}

function startRealtimeConnection() {
	if (!browser) return () => {};
	let socket: WebSocket | null = null;
	let reconnectTimer: number | null = null;
	let stopped = false;
	let reconnectAttempt = 0;
	const seenMessages = new Set<string>();

	const scheduleReconnect = () => {
		if (stopped || reconnectTimer !== null) return;
		const delay = Math.min(1000 * 2 ** reconnectAttempt, 15000);
		reconnectAttempt += 1;
		realtimeActivity.set({
			status: 'disconnected',
			lastConnectedAt: null,
			lastMessageAt: null,
			message: `Realtime reconnecting in ${Math.round(delay / 1000)}s`
		});
		reconnectTimer = window.setTimeout(() => {
			reconnectTimer = null;
			connect();
		}, delay);
	};

	const connect = () => {
		if (stopped) return;
		realtimeActivity.update((state) => ({
			...state,
			status: 'connecting',
			message: 'Connecting realtime'
		}));
		socket = new WebSocket(realtimeUrl());
		socket.addEventListener('open', () => {
			reconnectAttempt = 0;
			realtimeActivity.set({
				status: 'connected',
				lastConnectedAt: Date.now(),
				lastMessageAt: null,
				message: 'Realtime connected'
			});
			requestSync('online', 0);
		});
		socket.addEventListener('message', (event) => {
			if (typeof event.data !== 'string') return;
			try {
				const message = JSON.parse(event.data) as {
					type?: string;
					id?: string;
					sourceClientId?: string;
					workspaceId?: string;
					cursor?: string;
				};
				if (message.type === 'connected') return;
				if (
					message.type !== 'sync_changed' ||
					!message.id ||
					message.workspaceId !== getWorkspaceId() ||
					seenMessages.has(message.id)
				) return;
				seenMessages.add(message.id);
				if (seenMessages.size > 200) seenMessages.clear();
				realtimeActivity.update((state) => ({
					...state,
					status: 'connected',
					lastMessageAt: Date.now(),
					message: message.sourceClientId === getClientId()
						? 'Realtime confirmed local sync'
						: 'Realtime change received'
				}));
				if (message.sourceClientId !== getClientId()) requestSync('realtime', 0);
			} catch (error) {
				console.error('Invalid realtime message', error);
			}
		});
		socket.addEventListener('close', () => {
			socket = null;
			scheduleReconnect();
		});
		socket.addEventListener('error', () => {
			realtimeActivity.update((state) => ({
				...state,
				status: 'error',
				message: 'Realtime connection failed'
			}));
			socket?.close();
		});
	};

	connect();
	return () => {
		stopped = true;
		if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
		socket?.close();
	};
}

function startLocalOutboxSync() {
	let lastSignature: string | null = null;
	const subscription = liveQuery(async () => {
		const rows = await db.workspaceOutbox
			.where('workspaceId')
			.equals(getWorkspaceId())
			.sortBy('createdAt');
		return rows.map((mutation) => `${mutation.id}:${mutation.createdAt}`).join('|');
	}).subscribe({
		next(signature) {
			if (lastSignature === null) {
				lastSignature = signature;
				if (signature) requestSync('startup', 0);
				return;
			}
			if (signature === lastSignature) return;
			lastSignature = signature;
			if (signature) requestSync('local', 25);
		},
		error(error) {
			console.error('Outbox sync watcher failed', error);
		}
	});
	return () => subscription.unsubscribe();
}

export function startSyncLoop() {
	if (!browser) return () => {};
	const sync = (reason: 'interval' | 'online') => requestSync(reason, 0);
	const interval = window.setInterval(() => sync('interval'), 15000);
	const stopRealtime = startRealtimeConnection();
	const stopOutboxSync = startLocalOutboxSync();
	const onlineHandler = () => sync('online');
	const visibilityHandler = () => {
		if (document.visibilityState === 'visible') sync('interval');
	};
	window.addEventListener('online', onlineHandler);
	document.addEventListener('visibilitychange', visibilityHandler);
	void syncNow('startup');
	return () => {
		window.clearInterval(interval);
		stopRealtime();
		stopOutboxSync();
		window.removeEventListener('online', onlineHandler);
		document.removeEventListener('visibilitychange', visibilityHandler);
	};
}
