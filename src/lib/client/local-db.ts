import Dexie, { type Table } from 'dexie';
import type { LocalItem, MetaRecord, OutboxMutation } from '$lib/shared/types';

export class LocalFirstDatabase extends Dexie {
	workspaceItems!: Table<LocalItem, [string, string]>;
	workspaceOutbox!: Table<OutboxMutation, [string, string]>;
	meta!: Table<MetaRecord, string>;

	constructor(name = 'self-sync') {
		super(name);

		this.version(1).stores({
			items: 'id, syncStatus, updatedAt, deletedAt, revision',
			outbox: 'id, itemId, op, createdAt, attempts',
			meta: 'key'
		});

		this.version(2)
			.stores({
				items: 'id, syncStatus, updatedAt, deletedAt, revision, stage',
				outbox: 'id, itemId, op, createdAt, attempts',
				meta: 'key'
			})
			.upgrade((tx) =>
				tx
					.table<LocalItem, string>('items')
					.toCollection()
					.modify((item) => {
						item.stage ??= 'todo';
					})
			);

		this.version(3)
			.stores({
				items: 'id, syncStatus, updatedAt, deletedAt, revision, stage',
				outbox:
					'id, transactionId, [transactionId+sequence], itemId, op, createdAt, attempts',
				meta: 'key'
			})
			.upgrade((tx) =>
				tx
					.table<OutboxMutation, string>('outbox')
					.toCollection()
					.modify((mutation) => {
						mutation.transactionId ??= `legacy:${mutation.id}`;
						mutation.sequence ??= 0;
					})
			);

		this.version(4)
			.stores({
				items: 'id, syncStatus, updatedAt, deletedAt, revision, stage',
				outbox:
					'id, transactionId, [transactionId+sequence], itemId, op, createdAt, attempts',
				workspaceItems:
					'[workspaceId+id], workspaceId, [workspaceId+updatedAt], [workspaceId+syncStatus], deletedAt, revision, stage',
				workspaceOutbox:
					'[workspaceId+id], workspaceId, [workspaceId+transactionId], [workspaceId+itemId], [workspaceId+createdAt], transactionId, itemId, op, attempts',
				meta: 'key'
			})
			.upgrade(async (tx) => {
				const legacyItems = await tx.table<LocalItem, string>('items').toArray();
				const legacyOutbox = await tx.table<OutboxMutation, string>('outbox').toArray();

				if (legacyItems.length > 0) {
					await tx.table('workspaceItems').bulkPut(
						legacyItems.map((item) => ({ ...item, workspaceId: 'default' }))
					);
				}

				if (legacyOutbox.length > 0) {
					await tx.table('workspaceOutbox').bulkPut(
						legacyOutbox.map((mutation) => ({ ...mutation, workspaceId: 'default' }))
					);
				}
			});

		this.version(5).stores({
			items: null,
			outbox: null,
			workspaceItems:
				'[workspaceId+id], workspaceId, [workspaceId+updatedAt], [workspaceId+syncStatus], deletedAt, revision, stage',
			workspaceOutbox:
				'[workspaceId+id], workspaceId, [workspaceId+transactionId], [workspaceId+itemId], [workspaceId+createdAt], transactionId, itemId, op, attempts',
			meta: 'key'
		});
	}
}

export const db = new LocalFirstDatabase();
