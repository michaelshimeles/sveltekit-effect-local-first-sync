import { env } from '$env/dynamic/private';
import mysql from 'mysql2/promise';
import pg from 'pg';
import type { ResultSetHeader, RowDataPacket } from 'mysql2/promise';
import {
	REPLICATION_RETENTION_MS,
	maxReplicationCursor
} from '../shared/replication.ts';
import type {
	DatabaseMode,
	ReplicationCursor,
	ServerItem,
	SnapshotPage,
	SyncTransaction
} from '../shared/types.ts';
import {
	normaliseStage,
	planSyncTransaction,
	toServerItem,
	type StoredSyncItem,
	type TerminalTransactionStatus
} from './transaction.ts';
import {
	MemoryStorage,
	ReplicationStore,
	createMemoryState,
	type CommitPage,
	type CommittedPlan,
	type CompactionResult,
	type MemoryStorageState,
	type SyncStorage,
	type WorkspaceState
} from './replication-store.ts';

export { MemoryStorage } from './replication-store.ts';
export type {
	CompactionResult,
	MemoryStorageState,
	SyncStorage
} from './replication-store.ts';

const { Pool } = pg;
const MAX_TRANSACTION_ATTEMPTS = 4;

function selectCommitRows<T extends { item_count: number | string }>(rows: T[], limit: number) {
	const selected: T[] = [];
	let itemCount = 0;
	for (const row of rows) {
		const nextCount = Number(row.item_count);
		if (selected.length > 0 && itemCount + nextCount > limit) break;
		selected.push(row);
		itemCount += nextCount;
		if (itemCount >= limit) break;
	}
	return { selected, hasMore: selected.length < rows.length };
}


function normalisePostgresConnectionString(connectionString: string) {
	try {
		const url = new URL(connectionString);
		const sslMode = url.searchParams.get('sslmode')?.toLowerCase();
		if (sslMode === 'prefer' || sslMode === 'require' || sslMode === 'verify-ca') {
			url.searchParams.set('sslmode', 'verify-full');
			return url.toString();
		}
	} catch {
		return connectionString;
	}
	return connectionString;
}

const postgresSchema = `
create table if not exists sync_items (
	workspace_id text not null default 'default', id text not null, name text not null,
	note text not null default '', stage text not null default 'todo', revision integer not null default 0,
	updated_at bigint not null, deleted_at bigint, source_client_id text, last_mutation_id text,
	last_cursor bigint, primary key (workspace_id, id)
);
alter table sync_items add column if not exists workspace_id text not null default 'default';
alter table sync_items add column if not exists stage text not null default 'todo';
alter table sync_items add column if not exists last_cursor bigint;
do $$ begin
	if exists (
		select 1 from pg_constraint c where c.conrelid = 'sync_items'::regclass and c.contype = 'p'
		and array_length(c.conkey, 1) = 1
		and (select a.attname from pg_attribute a where a.attrelid = c.conrelid and a.attnum = c.conkey[1]) = 'id'
	) then
		alter table sync_items drop constraint sync_items_pkey;
		alter table sync_items add constraint sync_items_pkey primary key (workspace_id, id);
	end if;
end $$;
create index if not exists sync_items_workspace_updated_idx on sync_items (workspace_id, updated_at desc);

create table if not exists sync_transactions (
	workspace_id text not null default 'default', client_id text not null, id text not null,
	status text not null check (status in ('applied', 'conflict')), cursor bigint, committed_at bigint not null,
	primary key (workspace_id, client_id, id)
);
alter table sync_transactions add column if not exists workspace_id text not null default 'default';
alter table sync_transactions add column if not exists cursor bigint;
do $$ begin
	if exists (
		select 1 from pg_constraint c where c.conrelid = 'sync_transactions'::regclass and c.contype = 'p'
		and array_length(c.conkey, 1) = 2
	) then
		alter table sync_transactions drop constraint sync_transactions_pkey;
		alter table sync_transactions add constraint sync_transactions_pkey primary key (workspace_id, client_id, id);
	end if;
end $$;

create table if not exists sync_commits (
	cursor bigserial primary key, workspace_id text not null, client_id text not null,
	transaction_id text not null, committed_at bigint not null,
	unique (workspace_id, client_id, transaction_id)
);
create index if not exists sync_commits_workspace_cursor_idx on sync_commits (workspace_id, cursor);
create table if not exists sync_changes (
	cursor bigint not null references sync_commits(cursor) on delete cascade, workspace_id text not null,
	item_id text not null, name text not null, note text not null, stage text not null,
	revision integer not null, updated_at bigint not null, deleted_at bigint, source_client_id text,
	primary key (cursor, item_id)
);
create table if not exists sync_clients (
	workspace_id text not null, client_id text not null, acknowledged_cursor bigint not null,
	last_seen_at bigint not null, primary key (workspace_id, client_id)
);
create table if not exists sync_workspace_state (
	workspace_id text primary key, latest_cursor bigint not null default 0, floor_cursor bigint not null default 0
);
`;

function fromPostgresRow(row: Record<string, unknown>): StoredSyncItem {
	return {
		id: String(row.id ?? row.item_id),
		workspaceId: String(row.workspace_id),
		name: String(row.name),
		note: String(row.note ?? ''),
		stage: normaliseStage(row.stage),
		revision: Number(row.revision),
		updatedAt: Number(row.updated_at),
		deletedAt: row.deleted_at === null ? null : Number(row.deleted_at),
		sourceClientId: row.source_client_id === null ? null : String(row.source_client_id),
		lastMutationId: row.last_mutation_id === null || row.last_mutation_id === undefined
			? null
			: String(row.last_mutation_id)
	};
}

function postgresErrorCode(error: unknown) {
	return typeof error === 'object' && error && 'code' in error ? String(error.code) : '';
}

function isRetryablePostgresError(error: unknown) {
	return ['40001', '40P01', '23505'].includes(postgresErrorCode(error));
}

class PostgresStorage extends ReplicationStore {
	mode: DatabaseMode = 'postgres';
	private pool: InstanceType<typeof Pool>;
	private ready = false;

	constructor(connectionString: string) {
		super();
		this.pool = new Pool({ connectionString: normalisePostgresConnectionString(connectionString) });
	}

	private async ensureSchema() {
		if (this.ready) return;
		const client = await this.pool.connect();
		try {
			await client.query('begin');
			await client.query(`select pg_advisory_xact_lock(hashtext('self_sync_schema_v2'))`);
			await client.query(postgresSchema);
			await client.query('commit');
			this.ready = true;
		} catch (error) {
			await client.query('rollback').catch(() => {});
			throw error;
		} finally {
			client.release();
		}
	}

	protected async readWorkspaceState(workspaceId: string): Promise<WorkspaceState> {
		await this.ensureSchema();
		const result = await this.pool.query(
			`select latest_cursor::text, floor_cursor::text from sync_workspace_state where workspace_id = $1`,
			[workspaceId]
		);
		return {
			latestCursor: result.rows[0]?.latest_cursor ?? '0',
			floorCursor: result.rows[0]?.floor_cursor ?? '0'
		};
	}

	protected async readItems(
		workspaceId: string,
		options: { includeDeleted: boolean; itemIds?: string[] }
	) {
		await this.ensureSchema();
		const values: unknown[] = [workspaceId];
		const predicates = ['workspace_id = $1'];
		if (!options.includeDeleted) predicates.push('deleted_at is null');
		if (options.itemIds) {
			values.push(options.itemIds);
			predicates.push(`id = any($${values.length}::text[])`);
		}
		const result = await this.pool.query(
			`select * from sync_items where ${predicates.join(' and ')} order by updated_at desc`,
			values
		);
		return result.rows.map(fromPostgresRow).map(toServerItem);
	}

	protected async commitTransaction(
		workspaceId: string,
		clientId: string,
		transaction: SyncTransaction
	): Promise<CommittedPlan> {
		await this.ensureSchema();
		for (let attempt = 1; attempt <= MAX_TRANSACTION_ATTEMPTS; attempt += 1) {
			const client = await this.pool.connect();
			let started = false;
			try {
				await client.query('begin isolation level serializable');
				started = true;
				const ledgerResult = await client.query(
					`select status, cursor::text from sync_transactions
					 where workspace_id = $1 and client_id = $2 and id = $3 for update`,
					[workspaceId, clientId, transaction.id]
				);
				const terminalStatus = ledgerResult.rows[0]?.status as TerminalTransactionStatus | undefined;
				const existingCursor = ledgerResult.rows[0]?.cursor ?? null;
				const itemIds = [...new Set(transaction.changes.map((change) => change.item.id))].sort();
				const itemResult = await client.query(
					`select * from sync_items where workspace_id = $1 and id = any($2::text[])
					 order by id for update`,
					[workspaceId, itemIds]
				);
				const currentItems = new Map(
					itemResult.rows.map((row) => {
						const item = fromPostgresRow(row);
						return [item.id, item] as const;
					})
				);
				const plan = planSyncTransaction({
					workspaceId,
					clientId,
					transaction,
					currentItems,
					terminalStatus
				});
				let cursor: ReplicationCursor | null = existingCursor;

				if (plan.status === 'applied') {
					const commitResult = await client.query(
						`insert into sync_commits (workspace_id, client_id, transaction_id, committed_at)
						 values ($1, $2, $3, $4) returning cursor::text`,
						[workspaceId, clientId, transaction.id, Date.now()]
					);
					cursor = commitResult.rows[0].cursor;

					for (const item of plan.writes) {
						await client.query(
							`insert into sync_items
							 (workspace_id, id, name, note, stage, revision, updated_at, deleted_at,
							  source_client_id, last_mutation_id, last_cursor)
							 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
							 on conflict (workspace_id, id) do update set
							 name=excluded.name, note=excluded.note, stage=excluded.stage,
							 revision=excluded.revision, updated_at=excluded.updated_at,
							 deleted_at=excluded.deleted_at, source_client_id=excluded.source_client_id,
							 last_mutation_id=excluded.last_mutation_id, last_cursor=excluded.last_cursor`,
							[
								workspaceId, item.id, item.name, item.note, item.stage, item.revision,
								item.updatedAt, item.deletedAt, item.sourceClientId, item.lastMutationId, cursor
							]
						);
						await client.query(
							`insert into sync_changes
							 (cursor, workspace_id, item_id, name, note, stage, revision, updated_at,
							  deleted_at, source_client_id)
							 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
							[
								cursor, workspaceId, item.id, item.name, item.note, item.stage,
								item.revision, item.updatedAt, item.deletedAt, item.sourceClientId
							]
						);
					}
					await client.query(
						`insert into sync_workspace_state (workspace_id, latest_cursor, floor_cursor)
						 values ($1, $2, 0) on conflict (workspace_id) do update
						 set latest_cursor = greatest(sync_workspace_state.latest_cursor, excluded.latest_cursor)`,
						[workspaceId, cursor]
					);
				}

				if (plan.recordStatus) {
					const insert = await client.query(
						`insert into sync_transactions
						 (workspace_id, client_id, id, status, cursor, committed_at)
						 values ($1,$2,$3,$4,$5,$6) on conflict do nothing`,
						[workspaceId, clientId, transaction.id, plan.recordStatus, cursor, Date.now()]
					);
					if (insert.rowCount !== 1) {
						const error = Object.assign(new Error('Concurrent transaction ledger write'), {
							code: '40001'
						});
						throw error;
					}
				}

				await client.query('commit');
				return { plan, cursor };
			} catch (error) {
				if (started) await client.query('rollback').catch(() => {});
				if (attempt < MAX_TRANSACTION_ATTEMPTS && isRetryablePostgresError(error)) continue;
				throw error;
			} finally {
				client.release();
			}
		}
		throw new Error(`Transaction ${transaction.id} exhausted its retry budget`);
	}

	protected async readSnapshotPage(input: {
		workspaceId: string;
		id: string;
		cursor: ReplicationCursor;
		after: string | null;
		limit: number;
	}): Promise<SnapshotPage> {
		await this.ensureSchema();
		const result = await this.pool.query(
			`select * from sync_items where workspace_id = $1 and deleted_at is null
			 and ($2::text is null or id > $2) order by id limit $3`,
			[input.workspaceId, input.after, input.limit + 1]
		);
		const hasMore = result.rows.length > input.limit;
		const items = result.rows.slice(0, input.limit).map(fromPostgresRow).map(toServerItem);
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
		await this.ensureSchema();
		const commitsResult = await this.pool.query(
			`select c.cursor::text, c.transaction_id, count(ch.item_id)::int as item_count
			 from sync_commits c join sync_changes ch on ch.cursor = c.cursor
			 where c.workspace_id = $1 and c.cursor > $2
			 group by c.cursor, c.transaction_id order by c.cursor limit $3`,
			[input.workspaceId, input.cursor, input.limit + 1]
		);
		const { selected: commitRows, hasMore } = selectCommitRows(
			commitsResult.rows as Array<Record<string, unknown> & { item_count: number }>,
			input.limit
		);
		if (commitRows.length === 0) return { commits: [], cursor: input.cursor, hasMore: false };
		const cursors = commitRows.map((row) => row.cursor);
		const changesResult = await this.pool.query(
			`select * from sync_changes where workspace_id = $1 and cursor = any($2::bigint[])
			 order by cursor, item_id`,
			[input.workspaceId, cursors]
		);
		const itemsByCursor = new Map<string, ServerItem[]>();
		for (const row of changesResult.rows) {
			const cursor = String(row.cursor);
			const items = itemsByCursor.get(cursor) ?? [];
			items.push(toServerItem(fromPostgresRow(row)));
			itemsByCursor.set(cursor, items);
		}
		const commits = commitRows.map((row) => ({
			cursor: String(row.cursor),
			transactionId: String(row.transaction_id),
			items: itemsByCursor.get(String(row.cursor)) ?? []
		}));
		return { commits, cursor: commits.at(-1)?.cursor ?? input.cursor, hasMore };
	}

	protected async acknowledge(
		workspaceId: string,
		clientId: string,
		cursor: ReplicationCursor,
		now: number
	) {
		await this.ensureSchema();
		await this.pool.query(
			`insert into sync_clients (workspace_id, client_id, acknowledged_cursor, last_seen_at)
			 values ($1,$2,$3,$4) on conflict (workspace_id, client_id) do update set
			 acknowledged_cursor=greatest(sync_clients.acknowledged_cursor, excluded.acknowledged_cursor),
			 last_seen_at=excluded.last_seen_at`,
			[workspaceId, clientId, cursor, now]
		);
	}

	async compact(workspaceId: string, now = Date.now()): Promise<CompactionResult> {
		await this.ensureSchema();
		const client = await this.pool.connect();
		try {
			await client.query('begin');
			const cutoff = now - REPLICATION_RETENTION_MS;
			const stateResult = await client.query(
				`select latest_cursor::text, floor_cursor::text from sync_workspace_state
				 where workspace_id=$1 for update`,
				[workspaceId]
			);
			const latestCursor = stateResult.rows[0]?.latest_cursor ?? '0';
			const currentFloor = stateResult.rows[0]?.floor_cursor ?? '0';
			const watermark = await client.query(
				`select min(acknowledged_cursor)::text as cursor from sync_clients
				 where workspace_id=$1 and last_seen_at >= $2`,
				[workspaceId, cutoff]
			);
			const safeCursor = watermark.rows[0]?.cursor ?? latestCursor;
			const candidate = await client.query(
				`select max(cursor)::text as cursor, count(*)::int as count from sync_commits
				 where workspace_id=$1 and cursor <= $2 and committed_at < $3`,
				[workspaceId, safeCursor, cutoff]
			);
			const floorCursor = candidate.rows[0]?.cursor
				? maxReplicationCursor(currentFloor, candidate.rows[0].cursor)
				: currentFloor;
			const compactedCommits = Number(candidate.rows[0]?.count ?? 0);
			if (compactedCommits > 0) {
				await client.query(
					`delete from sync_commits where workspace_id=$1 and cursor <= $2 and committed_at < $3`,
					[workspaceId, floorCursor, cutoff]
				);
				await client.query(
					`insert into sync_workspace_state (workspace_id, latest_cursor, floor_cursor)
					 values ($1,$2,$3) on conflict (workspace_id) do update
					 set floor_cursor=greatest(sync_workspace_state.floor_cursor, excluded.floor_cursor)`,
					[workspaceId, latestCursor, floorCursor]
				);
				await client.query(
					`delete from sync_items where workspace_id=$1 and deleted_at is not null
					 and deleted_at < $2 and last_cursor <= $3`,
					[workspaceId, cutoff, floorCursor]
				);
			}
			await client.query(
				`delete from sync_transactions where workspace_id=$1 and committed_at < $2
				 and (cursor is null or cursor <= $3)`,
				[workspaceId, cutoff, floorCursor]
			);
			await client.query(
				`delete from sync_clients where workspace_id=$1 and last_seen_at < $2`,
				[workspaceId, cutoff]
			);
			await client.query('commit');
			return { compactedCommits, floorCursor };
		} catch (error) {
			await client.query('rollback').catch(() => {});
			throw error;
		} finally {
			client.release();
		}
	}
}

type MySqlItemRow = RowDataPacket & {
	id: string;
	item_id?: string;
	workspace_id: string;
	name: string;
	note: string;
	stage?: string | null;
	revision: number;
	updated_at: number | string;
	deleted_at: number | string | null;
	source_client_id: string | null;
	last_mutation_id?: string | null;
};

type MySqlTransactionRow = RowDataPacket & {
	status: TerminalTransactionStatus;
	commit_cursor: string | number | null;
};

function fromMySqlRow(row: MySqlItemRow): StoredSyncItem {
	return {
		id: row.id ?? String(row.item_id),
		workspaceId: row.workspace_id,
		name: row.name,
		note: row.note ?? '',
		stage: normaliseStage(row.stage),
		revision: Number(row.revision),
		updatedAt: Number(row.updated_at),
		deletedAt: row.deleted_at === null ? null : Number(row.deleted_at),
		sourceClientId: row.source_client_id,
		lastMutationId: row.last_mutation_id ?? null
	};
}

function mysqlErrorDetails(error: unknown) {
	if (typeof error !== 'object' || !error) return { code: '', errno: 0, sqlState: '' };
	return {
		code: 'code' in error ? String(error.code) : '',
		errno: 'errno' in error ? Number(error.errno) : 0,
		sqlState: 'sqlState' in error ? String(error.sqlState) : ''
	};
}

function isRetryableMySqlError(error: unknown) {
	const details = mysqlErrorDetails(error);
	return (
		['ER_LOCK_DEADLOCK', 'ER_LOCK_WAIT_TIMEOUT', 'ER_DUP_ENTRY'].includes(details.code) ||
		[1205, 1213, 1062].includes(details.errno) ||
		details.sqlState === '40001'
	);
}

const mysqlItemsSchema = `create table if not exists sync_items (
	workspace_id varchar(128) not null default 'default', id varchar(64) not null,
	name text not null, note text not null, stage varchar(16) not null default 'todo',
	revision int not null default 0, updated_at bigint not null, deleted_at bigint null,
	source_client_id varchar(128) null, last_mutation_id varchar(128) null, last_cursor bigint null,
	primary key (workspace_id, id), index sync_items_workspace_updated_idx (workspace_id, updated_at desc)
)`;
const mysqlTransactionsSchema = `create table if not exists sync_transactions (
	workspace_id varchar(128) not null default 'default', client_id varchar(128) not null,
	id varchar(128) not null, status varchar(16) not null, commit_cursor bigint null, committed_at bigint not null,
	primary key (workspace_id, client_id, id)
)`;
const mysqlCommitsSchema = `create table if not exists sync_commits (
	commit_cursor bigint not null auto_increment primary key, workspace_id varchar(128) not null,
	client_id varchar(128) not null, transaction_id varchar(128) not null, committed_at bigint not null,
	unique key sync_commits_transaction_key (workspace_id, client_id, transaction_id),
	index sync_commits_workspace_cursor_idx (workspace_id, commit_cursor)
)`;
const mysqlChangesSchema = `create table if not exists sync_changes (
	commit_cursor bigint not null, workspace_id varchar(128) not null, item_id varchar(64) not null,
	name text not null, note text not null, stage varchar(16) not null, revision int not null,
	updated_at bigint not null, deleted_at bigint null, source_client_id varchar(128) null,
	primary key (commit_cursor, item_id), constraint sync_changes_commit_fk foreign key (commit_cursor)
	references sync_commits(commit_cursor) on delete cascade
)`;
const mysqlClientsSchema = `create table if not exists sync_clients (
	workspace_id varchar(128) not null, client_id varchar(128) not null,
	acknowledged_cursor bigint not null, last_seen_at bigint not null,
	primary key (workspace_id, client_id)
)`;
const mysqlStateSchema = `create table if not exists sync_workspace_state (
	workspace_id varchar(128) primary key, latest_cursor bigint not null default 0,
	floor_cursor bigint not null default 0
)`;

class MySqlStorage extends ReplicationStore {
	mode: DatabaseMode = 'mysql';
	private pool: mysql.Pool;
	private ready = false;

	constructor(uri: string) {
		super();
		this.pool = mysql.createPool({ uri, supportBigNumbers: true, bigNumberStrings: true });
	}

	private async addColumn(connection: mysql.PoolConnection, sql: string) {
		try {
			await connection.query(sql);
		} catch (error) {
			if (mysqlErrorDetails(error).code !== 'ER_DUP_FIELDNAME') throw error;
		}
	}

	private async ensurePrimaryKey(
		connection: mysql.PoolConnection,
		table: string,
		expected: string[],
		definition: string
	) {
		const [rows] = await connection.query<(RowDataPacket & { column_name: string })[]>(
			`select column_name from information_schema.key_column_usage
			 where table_schema=database() and table_name=? and constraint_name='PRIMARY'
			 order by ordinal_position`,
			[table]
		);
		if (rows.map((row) => row.column_name).join(',') === expected.join(',')) return;
		await connection.query(`alter table ${table} drop primary key, add primary key (${definition})`);
	}

	private async ensureSchema() {
		if (this.ready) return;
		const connection = await this.pool.getConnection();
		let lockAcquired = false;
		try {
			const [lockRows] = await connection.query<(RowDataPacket & { acquired: number })[]>(
				`select get_lock('self_sync_schema_v2', 30) as acquired`
			);
			lockAcquired = Number(lockRows[0]?.acquired) === 1;
			if (!lockAcquired) throw new Error('Timed out waiting for the Self Sync schema lock');

			await connection.query(mysqlItemsSchema);
			await this.addColumn(
				connection,
				`alter table sync_items add column workspace_id varchar(128) not null default 'default' first`
			);
			await this.addColumn(
				connection,
				`alter table sync_items add column stage varchar(16) not null default 'todo'`
			);
			await this.addColumn(connection, `alter table sync_items add column last_cursor bigint null`);
			await this.ensurePrimaryKey(
				connection,
				'sync_items',
				['workspace_id', 'id'],
				'workspace_id, id'
			);

			await connection.query(mysqlTransactionsSchema);
			await this.addColumn(
				connection,
				`alter table sync_transactions add column workspace_id varchar(128) not null default 'default' first`
			);
			await this.addColumn(
				connection,
				`alter table sync_transactions add column commit_cursor bigint null`
			);
			await this.ensurePrimaryKey(
				connection,
				'sync_transactions',
				['workspace_id', 'client_id', 'id'],
				'workspace_id, client_id, id'
			);
			await connection.query(mysqlCommitsSchema);
			await connection.query(mysqlChangesSchema);
			await connection.query(mysqlClientsSchema);
			await connection.query(mysqlStateSchema);
			this.ready = true;
		} finally {
			if (lockAcquired) {
				await connection.query(`select release_lock('self_sync_schema_v2')`).catch(() => {});
			}
			connection.release();
		}
	}

	protected async readWorkspaceState(workspaceId: string): Promise<WorkspaceState> {
		await this.ensureSchema();
		const [rows] = await this.pool.query<
			(RowDataPacket & { latest_cursor: string | number; floor_cursor: string | number })[]
		>('select latest_cursor, floor_cursor from sync_workspace_state where workspace_id=?', [workspaceId]);
		return {
			latestCursor: rows[0] ? String(rows[0].latest_cursor) : '0',
			floorCursor: rows[0] ? String(rows[0].floor_cursor) : '0'
		};
	}

	protected async readItems(
		workspaceId: string,
		options: { includeDeleted: boolean; itemIds?: string[] }
	) {
		await this.ensureSchema();
		const values: unknown[] = [workspaceId];
		const predicates = ['workspace_id = ?'];
		if (!options.includeDeleted) predicates.push('deleted_at is null');
		if (options.itemIds) {
			predicates.push(`id in (${options.itemIds.map(() => '?').join(',')})`);
			values.push(...options.itemIds);
		}
		const [rows] = await this.pool.query<MySqlItemRow[]>(
			`select * from sync_items where ${predicates.join(' and ')} order by updated_at desc`,
			values
		);
		return rows.map(fromMySqlRow).map(toServerItem);
	}

	protected async commitTransaction(
		workspaceId: string,
		clientId: string,
		transaction: SyncTransaction
	): Promise<CommittedPlan> {
		await this.ensureSchema();
		for (let attempt = 1; attempt <= MAX_TRANSACTION_ATTEMPTS; attempt += 1) {
			const connection = await this.pool.getConnection();
			let started = false;
			try {
				await connection.query('set transaction isolation level serializable');
				await connection.beginTransaction();
				started = true;
				const [ledgerRows] = await connection.query<MySqlTransactionRow[]>(
					`select status, commit_cursor from sync_transactions
					 where workspace_id=? and client_id=? and id=? for update`,
					[workspaceId, clientId, transaction.id]
				);
				const itemIds = [...new Set(transaction.changes.map((change) => change.item.id))].sort();
				const [itemRows] = await connection.query<MySqlItemRow[]>(
					`select * from sync_items where workspace_id=? and id in (${itemIds.map(() => '?').join(',')})
					 order by id for update`,
					[workspaceId, ...itemIds]
				);
				const currentItems = new Map(
					itemRows.map((row) => {
						const item = fromMySqlRow(row);
						return [item.id, item] as const;
					})
				);
				const plan = planSyncTransaction({
					workspaceId,
					clientId,
					transaction,
					currentItems,
					terminalStatus: ledgerRows[0]?.status
				});
				let cursor = ledgerRows[0]?.commit_cursor === null || ledgerRows[0]?.commit_cursor === undefined
					? null
					: String(ledgerRows[0].commit_cursor);

				if (plan.status === 'applied') {
					const [commit] = await connection.query<ResultSetHeader>(
						`insert into sync_commits (workspace_id, client_id, transaction_id, committed_at)
						 values (?,?,?,?)`,
						[workspaceId, clientId, transaction.id, Date.now()]
					);
					cursor = String(commit.insertId);
					for (const item of plan.writes) {
						await connection.query(
							`insert into sync_items
							 (workspace_id,id,name,note,stage,revision,updated_at,deleted_at,
							  source_client_id,last_mutation_id,last_cursor)
							 values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update
							 name=values(name),note=values(note),stage=values(stage),revision=values(revision),
							 updated_at=values(updated_at),deleted_at=values(deleted_at),
							 source_client_id=values(source_client_id),last_mutation_id=values(last_mutation_id),
							 last_cursor=values(last_cursor)`,
							[
								workspaceId,item.id,item.name,item.note,item.stage,item.revision,item.updatedAt,
								item.deletedAt,item.sourceClientId,item.lastMutationId,cursor
							]
						);
						await connection.query(
							`insert into sync_changes
							 (commit_cursor,workspace_id,item_id,name,note,stage,revision,updated_at,deleted_at,source_client_id)
							 values (?,?,?,?,?,?,?,?,?,?)`,
							[
								cursor,workspaceId,item.id,item.name,item.note,item.stage,item.revision,
								item.updatedAt,item.deletedAt,item.sourceClientId
							]
						);
					}
					await connection.query(
						`insert into sync_workspace_state (workspace_id,latest_cursor,floor_cursor)
						 values (?,?,0) on duplicate key update latest_cursor=greatest(latest_cursor,values(latest_cursor))`,
						[workspaceId, cursor]
					);
				}

				if (plan.recordStatus) {
					await connection.query(
						`insert into sync_transactions
						 (workspace_id,client_id,id,status,commit_cursor,committed_at) values (?,?,?,?,?,?)`,
						[workspaceId, clientId, transaction.id, plan.recordStatus, cursor, Date.now()]
					);
				}
				await connection.commit();
				return { plan, cursor };
			} catch (error) {
				if (started) await connection.rollback().catch(() => {});
				if (attempt < MAX_TRANSACTION_ATTEMPTS && isRetryableMySqlError(error)) continue;
				throw error;
			} finally {
				connection.release();
			}
		}
		throw new Error(`Transaction ${transaction.id} exhausted its retry budget`);
	}

	protected async readSnapshotPage(input: {
		workspaceId: string;
		id: string;
		cursor: ReplicationCursor;
		after: string | null;
		limit: number;
	}): Promise<SnapshotPage> {
		await this.ensureSchema();
		const [rows] = await this.pool.query<MySqlItemRow[]>(
			`select * from sync_items where workspace_id=? and deleted_at is null
			 and (? is null or id > ?) order by id limit ?`,
			[input.workspaceId, input.after, input.after, input.limit + 1]
		);
		const hasMore = rows.length > input.limit;
		const items = rows.slice(0, input.limit).map(fromMySqlRow).map(toServerItem);
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
		await this.ensureSchema();
		const [allCommitRows] = await this.pool.query<
			(RowDataPacket & {
				cursor_value: string | number;
				transaction_id: string;
				item_count: string | number;
			})[]
		>(
			`select c.commit_cursor as cursor_value, c.transaction_id, count(ch.item_id) as item_count
			 from sync_commits c join sync_changes ch on ch.commit_cursor=c.commit_cursor
			 where c.workspace_id=? and c.commit_cursor>? group by c.commit_cursor,c.transaction_id
			 order by c.commit_cursor limit ?`,
			[input.workspaceId, input.cursor, input.limit + 1]
		);
		const { selected, hasMore } = selectCommitRows(allCommitRows, input.limit);
		if (selected.length === 0) return { commits: [], cursor: input.cursor, hasMore: false };
		const cursors = selected.map((row) => String(row.cursor_value));
		const [changeRows] = await this.pool.query<MySqlItemRow[]>(
			`select commit_cursor as cursor_value, workspace_id, item_id, item_id as id, name, note, stage, revision,
			 updated_at, deleted_at, source_client_id from sync_changes
			 where workspace_id=? and commit_cursor in (${cursors.map(() => '?').join(',')})
			 order by commit_cursor, item_id`,
			[input.workspaceId, ...cursors]
		);
		const itemsByCursor = new Map<string, ServerItem[]>();
		for (const row of changeRows as Array<MySqlItemRow & { cursor_value: string | number }>) {
			const cursor = String(row.cursor_value);
			const items = itemsByCursor.get(cursor) ?? [];
			items.push(toServerItem(fromMySqlRow(row)));
			itemsByCursor.set(cursor, items);
		}
		const commits = selected.map((row) => ({
			cursor: String(row.cursor_value),
			transactionId: row.transaction_id,
			items: itemsByCursor.get(String(row.cursor_value)) ?? []
		}));
		return { commits, cursor: commits.at(-1)?.cursor ?? input.cursor, hasMore };
	}

	protected async acknowledge(
		workspaceId: string,
		clientId: string,
		cursor: ReplicationCursor,
		now: number
	) {
		await this.ensureSchema();
		await this.pool.query(
			`insert into sync_clients (workspace_id,client_id,acknowledged_cursor,last_seen_at)
			 values (?,?,?,?) on duplicate key update
			 acknowledged_cursor=greatest(acknowledged_cursor,values(acknowledged_cursor)),
			 last_seen_at=values(last_seen_at)`,
			[workspaceId, clientId, cursor, now]
		);
	}

	async compact(workspaceId: string, now = Date.now()): Promise<CompactionResult> {
		await this.ensureSchema();
		const connection = await this.pool.getConnection();
		try {
			await connection.beginTransaction();
			const cutoff = now - REPLICATION_RETENTION_MS;
			const [states] = await connection.query<
				(RowDataPacket & { latest_cursor: string | number; floor_cursor: string | number })[]
			>('select latest_cursor,floor_cursor from sync_workspace_state where workspace_id=? for update', [workspaceId]);
			const latestCursor = states[0] ? String(states[0].latest_cursor) : '0';
			const currentFloor = states[0] ? String(states[0].floor_cursor) : '0';
			const [watermarks] = await connection.query<
				(RowDataPacket & { cursor_value: string | number | null })[]
			>('select min(acknowledged_cursor) as cursor_value from sync_clients where workspace_id=? and last_seen_at>=?', [workspaceId, cutoff]);
			const safeCursor = watermarks[0]?.cursor_value === null
				? latestCursor
				: String(watermarks[0].cursor_value);
			const [candidates] = await connection.query<
				(RowDataPacket & { cursor_value: string | number | null; count: string | number })[]
			>('select max(commit_cursor) as cursor_value,count(*) as count from sync_commits where workspace_id=? and commit_cursor<=? and committed_at<?', [workspaceId, safeCursor, cutoff]);
			const floorCursor = candidates[0]?.cursor_value === null
				? currentFloor
				: maxReplicationCursor(currentFloor, String(candidates[0].cursor_value));
			const compactedCommits = Number(candidates[0]?.count ?? 0);
			if (compactedCommits > 0) {
				await connection.query(
					'delete from sync_commits where workspace_id=? and commit_cursor<=? and committed_at<?',
					[workspaceId, floorCursor, cutoff]
				);
				await connection.query(
					`insert into sync_workspace_state (workspace_id,latest_cursor,floor_cursor)
					 values (?,?,?) on duplicate key update floor_cursor=greatest(floor_cursor,values(floor_cursor))`,
					[workspaceId, latestCursor, floorCursor]
				);
				await connection.query(
					`delete from sync_items where workspace_id=? and deleted_at is not null
					 and deleted_at<? and last_cursor<=?`,
					[workspaceId, cutoff, floorCursor]
				);
			}
			await connection.query(
				`delete from sync_transactions where workspace_id=? and committed_at<?
				 and (commit_cursor is null or commit_cursor<=?)`,
				[workspaceId, cutoff, floorCursor]
			);
			await connection.query('delete from sync_clients where workspace_id=? and last_seen_at<?', [workspaceId, cutoff]);
			await connection.commit();
			return { compactedCommits, floorCursor };
		} catch (error) {
			await connection.rollback().catch(() => {});
			throw error;
		} finally {
			connection.release();
		}
	}
}

const globalStore = globalThis as typeof globalThis & {
	__selfSyncMemoryState?: MemoryStorageState;
};

let storageSingleton: SyncStorage | null = null;
let storageKey = '';

export function getStorage(): SyncStorage {
	const connectionString = env.DATABASE_URL;
	const explicitDriver = (env.DATABASE_DRIVER || env.DB_DRIVER || '').toLowerCase();
	const driver = explicitDriver ||
		(connectionString?.startsWith('mysql') ? 'mysql' : connectionString ? 'postgres' : 'memory');
	const nextKey = `${driver}:${connectionString ?? 'memory'}`;
	if (storageSingleton && storageKey === nextKey) return storageSingleton;

	if (!connectionString || driver === 'memory') {
		globalStore.__selfSyncMemoryState ??= createMemoryState();
		storageSingleton = new MemoryStorage(globalStore.__selfSyncMemoryState);
	} else if (driver === 'mysql') {
		storageSingleton = new MySqlStorage(connectionString);
	} else {
		storageSingleton = new PostgresStorage(connectionString);
	}

	storageKey = nextKey;
	return storageSingleton;
}
