export type SyncStatus = 'synced' | 'pending' | 'error';
export type MutationOperation = 'upsert' | 'delete';
export type DatabaseMode = 'memory' | 'postgres' | 'mysql';
export type KanbanStage = 'todo' | 'doing' | 'done';
export type ReplicationCursor = string;

export interface ServerItem {
	id: string;
	workspaceId: string;
	name: string;
	note: string;
	stage: KanbanStage;
	revision: number;
	updatedAt: number;
	deletedAt: number | null;
	sourceClientId: string | null;
}

export interface LocalItem extends ServerItem {
	syncStatus: SyncStatus;
	lastError: string | null;
	snapshotId?: string | null;
}

export interface ClientSyncItem {
	id: string;
	name: string;
	note: string;
	stage: KanbanStage;
	updatedAt: number;
	deletedAt: number | null;
}

export interface OutboxMutation {
	id: string;
	workspaceId: string;
	transactionId: string;
	sequence: number;
	itemId: string;
	op: MutationOperation;
	item: ClientSyncItem;
	createdAt: number;
	attempts: number;
	lastError: string | null;
}

export interface MetaRecord {
	key: string;
	value: string;
}

export interface SyncChange {
	mutationId: string;
	op: MutationOperation;
	item: ClientSyncItem;
}

export interface SyncTransaction {
	id: string;
	createdAt: number;
	changes: ReadonlyArray<SyncChange>;
}

export interface SnapshotRequest {
	id: string;
	cursor: ReplicationCursor | null;
	after: string | null;
}

export interface SyncRequest {
	protocolVersion: 1 | 2;
	clientId: string;
	workspaceId: string;
	cursor: ReplicationCursor | null;
	snapshot: SnapshotRequest | null;
	limit: number;
	transactions: ReadonlyArray<SyncTransaction>;
}

export interface LegacySyncRequest {
	clientId: string;
	changes: ReadonlyArray<SyncChange>;
}

export interface SyncOutcome {
	transactionId: string;
	mutationId: string;
	itemId: string;
	status: 'applied' | 'conflict' | 'duplicate';
	revision: number;
}

export interface SyncTransactionOutcome {
	transactionId: string;
	status: 'applied' | 'conflict' | 'duplicate';
	mutationIds: string[];
	itemIds: string[];
}

export interface ReplicationCommit {
	cursor: ReplicationCursor;
	transactionId: string;
	items: ServerItem[];
}

export interface SnapshotPage {
	id: string;
	cursor: ReplicationCursor;
	items: ServerItem[];
	after: string | null;
	hasMore: boolean;
}

export interface SyncResponseStats {
	transactionsProcessed: number;
	commitsReturned: number;
	itemsReturned: number;
	compactedCommits?: number;
}

export interface SyncResponse {
	protocolVersion: 1 | 2;
	serverTime: number;
	databaseMode: DatabaseMode;
	workspaceId: string;
	cursor: ReplicationCursor | null;
	latestCursor: ReplicationCursor;
	hasMore: boolean;
	resetRequired: boolean;
	commits: ReplicationCommit[];
	snapshot: SnapshotPage | null;
	transactions: SyncTransactionOutcome[];
	applied: SyncOutcome[];
	reconciledItemIds: string[];
	reconciledItems: ServerItem[];
	stats: SyncResponseStats;
	items?: ServerItem[];
}

export interface SyncActivity {
	status: 'idle' | 'syncing' | 'synced' | 'offline' | 'error';
	lastSyncedAt: number | null;
	lastMessage: string;
	error: string | null;
	databaseMode: DatabaseMode | 'unknown';
}

export interface RealtimeActivity {
	status: 'connecting' | 'connected' | 'disconnected' | 'error';
	lastConnectedAt: number | null;
	lastMessageAt: number | null;
	message: string;
}
