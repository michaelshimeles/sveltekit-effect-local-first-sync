export const SYNC_PROTOCOL_VERSION = 2 as const;
export const DEFAULT_WORKSPACE_ID = 'default';

export const DEFAULT_SYNC_PAGE_LIMIT = 100;
export const MAX_SYNC_PAGE_LIMIT = 250;
export const MAX_SYNC_TRANSACTIONS = 100;
export const MAX_CHANGES_PER_TRANSACTION = 100;
export const MAX_CHANGES_PER_REQUEST = 500;
export const MAX_SYNC_REQUEST_BYTES = 1_000_000;
export const MAX_ITEM_NAME_LENGTH = 500;
export const MAX_ITEM_NOTE_LENGTH = 20_000;
export const MAX_WORKSPACE_ID_LENGTH = 128;
export const REPLICATION_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
export const COMPACTION_INTERVAL_MS = 5 * 60 * 1000;

const CURSOR_PATTERN = /^(0|[1-9]\d*)$/;

export function isReplicationCursor(value: string): boolean {
	return CURSOR_PATTERN.test(value);
}

export function parseReplicationCursor(value: string): bigint {
	if (!isReplicationCursor(value)) throw new Error(`Invalid replication cursor: ${value}`);
	return BigInt(value);
}

export function compareReplicationCursors(left: string, right: string): number {
	const leftValue = parseReplicationCursor(left);
	const rightValue = parseReplicationCursor(right);
	return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

export function maxReplicationCursor(left: string, right: string): string {
	return compareReplicationCursors(left, right) >= 0 ? left : right;
}

export function clampSyncPageLimit(value: number | undefined): number {
	if (value === undefined) return DEFAULT_SYNC_PAGE_LIMIT;
	if (!Number.isInteger(value) || value < 1 || value > MAX_SYNC_PAGE_LIMIT) {
		throw new Error(`Sync page limits must be between 1 and ${MAX_SYNC_PAGE_LIMIT}`);
	}
	return value;
}

export function workspaceMetaKey(workspaceId: string, name: string): string {
	return `replication:${workspaceId}:${name}`;
}
