import Dexie, { type Table } from 'dexie';
import type { LocalItem, MetaRecord, OutboxMutation } from '$lib/shared/types';

class LocalFirstDatabase extends Dexie {
	items!: Table<LocalItem, string>;
	outbox!: Table<OutboxMutation, string>;
	meta!: Table<MetaRecord, string>;

	constructor() {
		super('self-sync');

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
	}
}

export const db = new LocalFirstDatabase();
