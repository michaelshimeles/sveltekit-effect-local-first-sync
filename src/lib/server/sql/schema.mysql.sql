create table if not exists sync_items (
  workspace_id varchar(128) not null default 'default',
  id varchar(64) not null,
  name text not null,
  note text not null,
  stage varchar(16) not null default 'todo',
  revision int not null default 0,
  updated_at bigint not null,
  deleted_at bigint null,
  source_client_id varchar(128) null,
  last_mutation_id varchar(128) null,
  last_cursor bigint null,
  primary key (workspace_id, id),
  index sync_items_workspace_updated_idx (workspace_id, updated_at desc)
);

create table if not exists sync_transactions (
  workspace_id varchar(128) not null default 'default',
  client_id varchar(128) not null,
  id varchar(128) not null,
  status varchar(16) not null,
  commit_cursor bigint null,
  committed_at bigint not null,
  primary key (workspace_id, client_id, id)
);

create table if not exists sync_commits (
  commit_cursor bigint not null auto_increment primary key,
  workspace_id varchar(128) not null,
  client_id varchar(128) not null,
  transaction_id varchar(128) not null,
  committed_at bigint not null,
  unique key sync_commits_transaction_key (workspace_id, client_id, transaction_id),
  index sync_commits_workspace_cursor_idx (workspace_id, commit_cursor)
);

create table if not exists sync_changes (
  commit_cursor bigint not null,
  workspace_id varchar(128) not null,
  item_id varchar(64) not null,
  name text not null,
  note text not null,
  stage varchar(16) not null,
  revision int not null,
  updated_at bigint not null,
  deleted_at bigint null,
  source_client_id varchar(128) null,
  primary key (commit_cursor, item_id),
  constraint sync_changes_commit_fk foreign key (commit_cursor)
    references sync_commits(commit_cursor) on delete cascade
);

create table if not exists sync_clients (
  workspace_id varchar(128) not null,
  client_id varchar(128) not null,
  acknowledged_cursor bigint not null,
  last_seen_at bigint not null,
  primary key (workspace_id, client_id)
);

create table if not exists sync_workspace_state (
  workspace_id varchar(128) primary key,
  latest_cursor bigint not null default 0,
  floor_cursor bigint not null default 0
);
