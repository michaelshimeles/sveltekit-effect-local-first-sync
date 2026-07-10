create table if not exists sync_items (
  workspace_id text not null default 'default',
  id text not null,
  name text not null,
  note text not null default '',
  stage text not null default 'todo',
  revision integer not null default 0,
  updated_at bigint not null,
  deleted_at bigint,
  source_client_id text,
  last_mutation_id text,
  last_cursor bigint,
  primary key (workspace_id, id)
);

create index if not exists sync_items_workspace_updated_idx
  on sync_items (workspace_id, updated_at desc);

create table if not exists sync_transactions (
  workspace_id text not null default 'default',
  client_id text not null,
  id text not null,
  status text not null check (status in ('applied', 'conflict')),
  cursor bigint,
  committed_at bigint not null,
  primary key (workspace_id, client_id, id)
);

create table if not exists sync_commits (
  cursor bigserial primary key,
  workspace_id text not null,
  client_id text not null,
  transaction_id text not null,
  committed_at bigint not null,
  unique (workspace_id, client_id, transaction_id)
);

create index if not exists sync_commits_workspace_cursor_idx
  on sync_commits (workspace_id, cursor);

create table if not exists sync_changes (
  cursor bigint not null references sync_commits(cursor) on delete cascade,
  workspace_id text not null,
  item_id text not null,
  name text not null,
  note text not null,
  stage text not null,
  revision integer not null,
  updated_at bigint not null,
  deleted_at bigint,
  source_client_id text,
  primary key (cursor, item_id)
);

create table if not exists sync_clients (
  workspace_id text not null,
  client_id text not null,
  acknowledged_cursor bigint not null,
  last_seen_at bigint not null,
  primary key (workspace_id, client_id)
);

create table if not exists sync_workspace_state (
  workspace_id text primary key,
  latest_cursor bigint not null default 0,
  floor_cursor bigint not null default 0
);
