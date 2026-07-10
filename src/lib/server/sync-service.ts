import { Context, Data, Effect, Schema } from 'effect';
import { SyncRequestSchema, type SyncRequestInput } from '$lib/shared/schema';
import type { SyncRequest } from '$lib/shared/types';
import {
	DEFAULT_SYNC_PAGE_LIMIT,
	DEFAULT_WORKSPACE_ID,
	MAX_CHANGES_PER_REQUEST,
	MAX_CHANGES_PER_TRANSACTION,
	MAX_ITEM_NAME_LENGTH,
	MAX_ITEM_NOTE_LENGTH,
	MAX_SYNC_TRANSACTIONS,
	MAX_WORKSPACE_ID_LENGTH,
	clampSyncPageLimit,
	isReplicationCursor
} from '$lib/shared/replication';
import { getStorage, type SyncStorage } from './storage';
import { validateSyncTransaction } from './transaction';

export class StorageFailure extends Data.TaggedError('StorageFailure')<{
	message: string;
	cause: unknown;
}> {}

export class InvalidSyncRequest extends Data.TaggedError('InvalidSyncRequest')<{
	message: string;
	cause: unknown;
}> {}

class StorageContext extends Context.Tag('StorageContext')<StorageContext, SyncStorage>() {}

function provideStorage<A, E, R>(effect: Effect.Effect<A, E, R>) {
	return Effect.provideService(effect, StorageContext, getStorage());
}

function normaliseRequest(decoded: SyncRequestInput): SyncRequest {
	if (!decoded.clientId || decoded.clientId.length > 128) {
		throw new Error('Client IDs must be between 1 and 128 characters');
	}

	const isV2 = 'protocolVersion' in decoded && decoded.protocolVersion === 2;
	const workspaceId = isV2 ? decoded.workspaceId : DEFAULT_WORKSPACE_ID;
	if (!workspaceId || workspaceId.length > MAX_WORKSPACE_ID_LENGTH) {
		throw new Error(`Workspace IDs must be between 1 and ${MAX_WORKSPACE_ID_LENGTH} characters`);
	}

	const transactions =
		'transactions' in decoded
			? decoded.transactions
			: decoded.changes.map((change) => ({
					id:
						`legacy:${change.mutationId}`.length <= 128
							? `legacy:${change.mutationId}`
							: change.mutationId,
					createdAt: change.item.updatedAt || 1,
					changes: [change]
				}));
	const transactionIds = new Set<string>();
	if (transactions.length > MAX_SYNC_TRANSACTIONS) {
		throw new Error(`A sync request can contain at most ${MAX_SYNC_TRANSACTIONS} transactions`);
	}
	let changeCount = 0;

	for (const transaction of transactions) {
		if (transactionIds.has(transaction.id)) {
			throw new Error(`Transaction ${transaction.id} appears more than once`);
		}

		validateSyncTransaction(transaction);
		if (transaction.changes.length > MAX_CHANGES_PER_TRANSACTION) {
			throw new Error(
				`Transaction ${transaction.id} exceeds ${MAX_CHANGES_PER_TRANSACTION} changes`
			);
		}
		for (const change of transaction.changes) {
			if (change.item.name.length > MAX_ITEM_NAME_LENGTH) {
				throw new Error(`Item names cannot exceed ${MAX_ITEM_NAME_LENGTH} characters`);
			}
			if (change.item.note.length > MAX_ITEM_NOTE_LENGTH) {
				throw new Error(`Item notes cannot exceed ${MAX_ITEM_NOTE_LENGTH} characters`);
			}
		}
		changeCount += transaction.changes.length;
		transactionIds.add(transaction.id);
	}
	if (changeCount > MAX_CHANGES_PER_REQUEST) {
		throw new Error(`A sync request can contain at most ${MAX_CHANGES_PER_REQUEST} changes`);
	}

	if (isV2) {
		if (decoded.cursor !== null && !isReplicationCursor(decoded.cursor)) {
			throw new Error('The replication cursor is invalid');
		}
		if (!decoded.snapshot && decoded.cursor === null) {
			throw new Error('A snapshot request is required without a cursor');
		}
		if (decoded.snapshot && decoded.cursor !== null) {
			throw new Error('Snapshot continuation cannot be combined with a replication cursor');
		}
		if (decoded.snapshot) {
			if (!decoded.snapshot.id || decoded.snapshot.id.length > 128) {
				throw new Error('Snapshot IDs must be between 1 and 128 characters');
			}
			if (decoded.snapshot.cursor !== null && !isReplicationCursor(decoded.snapshot.cursor)) {
				throw new Error('The snapshot cursor is invalid');
			}
			if (decoded.snapshot.after !== null && decoded.snapshot.after.length > 64) {
				throw new Error('Snapshot continuation IDs cannot exceed 64 characters');
			}
			if (decoded.snapshot.after !== null && decoded.snapshot.cursor === null) {
				throw new Error('Snapshot continuation requires a fixed snapshot cursor');
			}
		}

		return {
			protocolVersion: 2,
			clientId: decoded.clientId,
			workspaceId,
			cursor: decoded.cursor,
			snapshot: decoded.snapshot,
			limit: clampSyncPageLimit(decoded.limit),
			transactions
		};
	}

	return {
		protocolVersion: 1,
		clientId: decoded.clientId,
		workspaceId,
		cursor: null,
		snapshot: null,
		limit: DEFAULT_SYNC_PAGE_LIMIT,
		transactions
	};
}

export function syncProgram(input: unknown) {
	return provideStorage(
		Effect.gen(function* () {
			const decoded = yield* Schema.decodeUnknown(SyncRequestSchema)(input);
			const request = yield* Effect.try({
				try: () => normaliseRequest(decoded),
				catch: (error) =>
					new InvalidSyncRequest({
						message: error instanceof Error ? error.message : 'Invalid sync request',
						cause: error
					})
			});
			const storage = yield* StorageContext;

			return yield* Effect.tryPromise({
				try: () => storage.applyChanges(request),
				catch: (error) =>
					new StorageFailure({
						message: error instanceof Error ? error.message : 'Storage operation failed',
						cause: error
					})
			});
		})
	);
}

export function listItemsProgram(options: { includeDeleted?: boolean } = {}) {
	return provideStorage(
		Effect.gen(function* () {
			const storage = yield* StorageContext;

			return yield* Effect.tryPromise({
				try: () => storage.listItems(options),
				catch: (error) =>
					new StorageFailure({
						message: error instanceof Error ? error.message : 'Storage operation failed',
						cause: error
					})
			});
		})
	);
}

export function healthProgram() {
	return provideStorage(
		Effect.gen(function* () {
			const storage = yield* StorageContext;
			const items = yield* Effect.tryPromise({
				try: () => storage.listItems({ includeDeleted: true }),
				catch: (error) =>
					new StorageFailure({
						message: error instanceof Error ? error.message : 'Storage operation failed',
						cause: error
					})
			});

			return {
				ok: true,
				databaseMode: storage.mode,
				itemCount: items.length,
				checkedAt: Date.now()
			};
		})
	);
}
