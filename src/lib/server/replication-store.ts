import {
	COMPACTION_INTERVAL_MS,
	DEFAULT_WORKSPACE_ID,
	REPLICATION_RETENTION_MS,
	compareReplicationCursors,
	maxReplicationCursor
} from '../shared/replication.ts';
import type {
	DatabaseMode,
	ReplicationCommit,
	ReplicationCursor,
	ServerItem,
	SnapshotPage,
	SyncRequest,
	SyncResponse,
	SyncTransaction
} from '../shared/types.ts';
import {
	planSyncTransaction,
	sortItems,
	toServerItem,
	type StoredSyncItem,
	type TerminalTransactionStatus,
	type TransactionPlan
} from './transaction.ts';

export interface CommittedPlan {
	plan: TransactionPlan;
	cursor: ReplicationCursor | null;
}

export interface WorkspaceState {
	latestCursor: ReplicationCursor;
	floorCursor: ReplicationCursor;
}

export interface CommitPage {
	commits: ReplicationCommit[];
	cursor: ReplicationCursor;
	hasMore: boolean;
}

export interface CompactionResult {
	compactedCommits: number;
	floorCursor: ReplicationCursor;
}

/**
 * Deep replication Module. Implementations own atomic commit, snapshots, cursor reads,
 * replica watermarks, and retention so the service layer does not depend on a database API.
 */
export interface SyncStorage {
	mode: DatabaseMode;
	listItems(options?: {
		includeDeleted?: boolean;
		workspaceId?: string;
	}): Promise<ServerItem[]>;
	applyChanges(request: SyncRequest): Promise<SyncResponse>;
	compact(workspaceId: string, now?: number): Promise<CompactionResult>;
}

export abstract class ReplicationStore implements SyncStorage {
	abstract mode: DatabaseMode;
	private lastCompaction = new Map<string, number>();

	protected abstract commitTransaction(
		workspaceId: string,
		clientId: string,
		transaction: SyncTransaction
	): Promise<CommittedPlan>;
	protected abstract readItems(
		workspaceId: string,
		options: { includeDeleted: boolean; itemIds?: string[] }
	): Promise<ServerItem[]>;
	protected abstract readSnapshotPage(input: {
		workspaceId: string;
		id: string;
		cursor: ReplicationCursor;
		after: string | null;
		limit: number;
	}): Promise<SnapshotPage>;
	protected abstract readCommitPage(input: {
		workspaceId: string;
		cursor: ReplicationCursor;
		limit: number;
	}): Promise<CommitPage>;
	protected abstract readWorkspaceState(workspaceId: string): Promise<WorkspaceState>;
	protected abstract acknowledge(
		workspaceId: string,
		clientId: string,
		cursor: ReplicationCursor,
		now: number
	): Promise<void>;
	abstract compact(workspaceId: string, now?: number): Promise<CompactionResult>;

	async listItems(
		options: { includeDeleted?: boolean; workspaceId?: string } = {}
	): Promise<ServerItem[]> {
		return this.readItems(options.workspaceId ?? DEFAULT_WORKSPACE_ID, {
			includeDeleted: options.includeDeleted ?? false
		});
	}

	private async maybeCompact(workspaceId: string, now: number) {
		const previous = this.lastCompaction.get(workspaceId) ?? 0;
		if (now - previous < COMPACTION_INTERVAL_MS) return 0;
		this.lastCompaction.set(workspaceId, now);

		try {
			return (await this.compact(workspaceId, now)).compactedCommits;
		} catch (error) {
			console.error('Replication compaction failed', error);
			return 0;
		}
	}

	async applyChanges(request: SyncRequest): Promise<SyncResponse> {
		const now = Date.now();
		const initialState = await this.readWorkspaceState(request.workspaceId);
		const requestedFloorCursor = request.snapshot?.cursor ?? request.cursor;
		const resetRequired =
			request.protocolVersion === 2 &&
			requestedFloorCursor !== null &&
			(compareReplicationCursors(requestedFloorCursor, initialState.floorCursor) < 0 ||
				compareReplicationCursors(requestedFloorCursor, initialState.latestCursor) > 0);

		if (resetRequired) {
			return {
				protocolVersion: 2,
				serverTime: now,
				databaseMode: this.mode,
				workspaceId: request.workspaceId,
				cursor: null,
				latestCursor: initialState.latestCursor,
				hasMore: false,
				resetRequired: true,
				commits: [],
				snapshot: null,
				transactions: [],
				applied: [],
				reconciledItemIds: [],
				reconciledItems: [],
				stats: {
					transactionsProcessed: 0,
					commitsReturned: 0,
					itemsReturned: 0
				}
			};
		}

		if (request.protocolVersion === 2 && request.cursor !== null) {
			await this.acknowledge(request.workspaceId, request.clientId, request.cursor, now);
		}

		const committedPlans: CommittedPlan[] = [];
		for (const transaction of request.transactions) {
			committedPlans.push(
				await this.commitTransaction(request.workspaceId, request.clientId, transaction)
			);
		}

		const reconciledItemIds = [
			...new Set(request.transactions.flatMap((transaction) => transaction.changes.map((c) => c.item.id)))
		];
		const reconciledItems =
			reconciledItemIds.length === 0
				? []
				: await this.readItems(request.workspaceId, {
						includeDeleted: true,
						itemIds: reconciledItemIds
					});
		const state = await this.readWorkspaceState(request.workspaceId);
		const plans = committedPlans.map((result) => result.plan);

		if (request.protocolVersion === 1) {
			const items = await this.readItems(request.workspaceId, { includeDeleted: true });
			return {
				protocolVersion: 1,
				serverTime: now,
				databaseMode: this.mode,
				workspaceId: request.workspaceId,
				cursor: state.latestCursor,
				latestCursor: state.latestCursor,
				hasMore: false,
				resetRequired: false,
				commits: [],
				snapshot: null,
				transactions: plans.map((plan) => plan.transaction),
				applied: plans.flatMap((plan) => plan.outcomes),
				reconciledItemIds,
				reconciledItems,
				stats: {
					transactionsProcessed: plans.length,
					commitsReturned: 0,
					itemsReturned: items.length
				},
				items
			};
		}

		let snapshot: SnapshotPage | null = null;
		let commitPage: CommitPage = {
			commits: [],
			cursor: request.cursor ?? '0',
			hasMore: false
		};

		if (request.cursor === null) {
			if (!request.snapshot) throw new Error('A snapshot request is required without a cursor');
			const snapshotCursor = request.snapshot.cursor ?? state.latestCursor;
			snapshot = await this.readSnapshotPage({
				workspaceId: request.workspaceId,
				id: request.snapshot.id,
				cursor: snapshotCursor,
				after: request.snapshot.after,
				limit: request.limit
			});
		} else {
			commitPage = await this.readCommitPage({
				workspaceId: request.workspaceId,
				cursor: request.cursor,
				limit: request.limit
			});
		}

		const compactedCommits = await this.maybeCompact(request.workspaceId, now);
		const itemsReturned =
			(snapshot?.items.length ?? 0) +
			commitPage.commits.reduce((total, commit) => total + commit.items.length, 0) +
			reconciledItems.length;

		return {
			protocolVersion: 2,
			serverTime: now,
			databaseMode: this.mode,
			workspaceId: request.workspaceId,
			cursor: request.cursor === null ? null : commitPage.cursor,
			latestCursor: state.latestCursor,
			hasMore: snapshot?.hasMore ?? commitPage.hasMore,
			resetRequired: false,
			commits: commitPage.commits,
			snapshot,
			transactions: plans.map((plan) => plan.transaction),
			applied: plans.flatMap((plan) => plan.outcomes),
			reconciledItemIds,
			reconciledItems,
			stats: {
				transactionsProcessed: plans.length,
				commitsReturned: commitPage.commits.length,
				itemsReturned,
				compactedCommits
			}
		};
	}
}

interface MemoryLedger {
	status: TerminalTransactionStatus;
	cursor: ReplicationCursor | null;
	committedAt: number;
}

interface MemoryCommit extends ReplicationCommit {
	committedAt: number;
}

export interface MemoryStorageState {
	items: Map<string, StoredSyncItem>;
	itemCursors: Map<string, ReplicationCursor>;
	transactions: Map<string, MemoryLedger>;
	commits: Map<string, MemoryCommit[]>;
	latestCursors: Map<string, ReplicationCursor>;
	floorCursors: Map<string, ReplicationCursor>;
	clients: Map<string, { cursor: ReplicationCursor; lastSeenAt: number }>;
}

export function createMemoryState(): MemoryStorageState {
	return {
		items: new Map(),
		itemCursors: new Map(),
		transactions: new Map(),
		commits: new Map(),
		latestCursors: new Map(),
		floorCursors: new Map(),
		clients: new Map()
	};
}

function scopedKey(workspaceId: string, id: string) {
	return `${workspaceId}\u0000${id}`;
}

function transactionKey(workspaceId: string, clientId: string, transactionId: string) {
	return `${workspaceId}\u0000${clientId}\u0000${transactionId}`;
}

export class MemoryStorage extends ReplicationStore {
	mode: DatabaseMode = 'memory';
	private state: MemoryStorageState;

	constructor(state: MemoryStorageState = createMemoryState()) {
		super();
		this.state = state;
	}

	protected async readWorkspaceState(workspaceId: string): Promise<WorkspaceState> {
		return {
			latestCursor: this.state.latestCursors.get(workspaceId) ?? '0',
			floorCursor: this.state.floorCursors.get(workspaceId) ?? '0'
		};
	}

	protected async readItems(
		workspaceId: string,
		options: { includeDeleted: boolean; itemIds?: string[] }
	) {
		const itemIds = options.itemIds ? new Set(options.itemIds) : null;
		return sortItems(
			[...this.state.items.values()]
				.filter((item) => item.workspaceId === workspaceId)
				.filter((item) => !itemIds || itemIds.has(item.id))
				.filter((item) => options.includeDeleted || item.deletedAt === null)
				.map(toServerItem)
		);
	}

	protected async commitTransaction(
		workspaceId: string,
		clientId: string,
		transaction: SyncTransaction
	): Promise<CommittedPlan> {
		const currentItems = new Map<string, StoredSyncItem>();
		for (const change of transaction.changes) {
			const current = this.state.items.get(scopedKey(workspaceId, change.item.id));
			if (current) currentItems.set(current.id, current);
		}

		const key = transactionKey(workspaceId, clientId, transaction.id);
		const ledger = this.state.transactions.get(key);
		const plan = planSyncTransaction({
			workspaceId,
			clientId,
			transaction,
			currentItems,
			terminalStatus: ledger?.status
		});
		let cursor = ledger?.cursor ?? null;

		if (plan.status === 'applied') {
			cursor = (BigInt(this.state.latestCursors.get(workspaceId) ?? '0') + 1n).toString();
			this.state.latestCursors.set(workspaceId, cursor);
			for (const item of plan.writes) {
				const key = scopedKey(workspaceId, item.id);
				this.state.items.set(key, item);
				this.state.itemCursors.set(key, cursor);
			}
			const commits = this.state.commits.get(workspaceId) ?? [];
			commits.push({
				cursor,
				transactionId: transaction.id,
				items: plan.writes.map(toServerItem),
				committedAt: Date.now()
			});
			this.state.commits.set(workspaceId, commits);
		}

		if (plan.recordStatus) {
			this.state.transactions.set(key, {
				status: plan.recordStatus,
				cursor,
				committedAt: Date.now()
			});
		}

		return { plan, cursor };
	}

	protected async readSnapshotPage(input: {
		workspaceId: string;
		id: string;
		cursor: ReplicationCursor;
		after: string | null;
		limit: number;
	}): Promise<SnapshotPage> {
		const rows = [...this.state.items.values()]
			.filter((item) => item.workspaceId === input.workspaceId && item.deletedAt === null)
			.filter((item) => input.after === null || item.id > input.after)
			.sort((a, b) => a.id.localeCompare(b.id));
		const hasMore = rows.length > input.limit;
		const items = rows.slice(0, input.limit).map(toServerItem);

		return {
			id: input.id,
			cursor: input.cursor,
			items,
			after: hasMore ? (items.at(-1)?.id ?? null) : null,
			hasMore
		};
	}

	protected async readCommitPage(input: {
		workspaceId: string;
		cursor: ReplicationCursor;
		limit: number;
	}): Promise<CommitPage> {
		const rows = (this.state.commits.get(input.workspaceId) ?? []).filter(
			(commit) => compareReplicationCursors(commit.cursor, input.cursor) > 0
		);
		const selected: MemoryCommit[] = [];
		let itemCount = 0;
		for (const commit of rows) {
			if (selected.length > 0 && itemCount + commit.items.length > input.limit) break;
			selected.push(commit);
			itemCount += commit.items.length;
			if (itemCount >= input.limit) break;
		}
		const hasMore = selected.length < rows.length;
		const commits = selected.map(({ committedAt: _, ...commit }) => commit);
		return {
			commits,
			cursor: commits.at(-1)?.cursor ?? input.cursor,
			hasMore
		};
	}

	protected async acknowledge(
		workspaceId: string,
		clientId: string,
		cursor: ReplicationCursor,
		now: number
	) {
		const key = scopedKey(workspaceId, clientId);
		const existing = this.state.clients.get(key);
		this.state.clients.set(key, {
			cursor: existing ? maxReplicationCursor(existing.cursor, cursor) : cursor,
			lastSeenAt: now
		});
	}

	async compact(workspaceId: string, now = Date.now()): Promise<CompactionResult> {
		const cutoff = now - REPLICATION_RETENTION_MS;
		const latestCursor = this.state.latestCursors.get(workspaceId) ?? '0';
		const activeClients = [...this.state.clients.entries()].filter(
			([key, client]) => key.startsWith(`${workspaceId}\u0000`) && client.lastSeenAt >= cutoff
		);
		const safeCursor = activeClients.reduce(
			(cursor, [, client]) =>
				compareReplicationCursors(client.cursor, cursor) < 0 ? client.cursor : cursor,
			latestCursor
		);
		const commits = this.state.commits.get(workspaceId) ?? [];
		const removable = commits.filter(
			(commit) =>
				commit.committedAt < cutoff && compareReplicationCursors(commit.cursor, safeCursor) <= 0
		);
		const floorCursor = removable.reduce(
			(cursor, commit) => maxReplicationCursor(cursor, commit.cursor),
			this.state.floorCursors.get(workspaceId) ?? '0'
		);
		const removableCursors = new Set(removable.map((commit) => commit.cursor));
		this.state.commits.set(
			workspaceId,
			commits.filter((commit) => !removableCursors.has(commit.cursor))
		);
		this.state.floorCursors.set(workspaceId, floorCursor);

		for (const [key, client] of this.state.clients) {
			if (key.startsWith(`${workspaceId}\u0000`) && client.lastSeenAt < cutoff) {
				this.state.clients.delete(key);
			}
		}

		for (const [key, item] of this.state.items) {
			const itemCursor = this.state.itemCursors.get(key);
			if (
				item.workspaceId === workspaceId &&
				item.deletedAt !== null &&
				item.deletedAt < cutoff &&
				itemCursor &&
				compareReplicationCursors(itemCursor, floorCursor) <= 0
			) {
				this.state.items.delete(key);
				this.state.itemCursors.delete(key);
			}
		}

		for (const [key, ledger] of this.state.transactions) {
			if (
				key.startsWith(`${workspaceId}\u0000`) &&
				ledger.committedAt < cutoff &&
				(ledger.cursor === null || compareReplicationCursors(ledger.cursor, floorCursor) <= 0)
			) {
				this.state.transactions.delete(key);
			}
		}

		return { compactedCommits: removable.length, floorCursor };
	}
}
