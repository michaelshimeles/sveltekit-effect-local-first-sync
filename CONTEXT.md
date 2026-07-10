# Self Sync Context

Self Sync is a transactional local-first replication engine. The browser database is the UI's
immediate source of truth; the server database establishes durable order and convergence.

## Domain Vocabulary

- **Workspace**: the replication partition. Every item, mutation, cursor, and realtime signal is
  scoped to one workspace. Workspace IDs are routing keys, not authorization by themselves.
- **Local Transaction**: one atomic IndexedDB write containing item changes and their outbox
  mutations.
- **Server Transaction**: one serializable database transaction that validates and commits every
  mutation in a local transaction together.
- **Replication Cursor**: an opaque decimal string identifying a durable server commit. Clients
  persist it and request only later commits.
- **Bootstrap Snapshot**: a bounded, paginated view of current non-deleted rows used when a client
  has no cursor or its cursor is older than retained history.
- **Commit Page**: a bounded set of whole server transactions after a cursor. A transaction is
  never split across pages.
- **Client Watermark**: the newest cursor a client says it has durably applied. Active watermarks
  define the safe compaction boundary.
- **Realtime Invalidation**: a workspace-scoped WebSocket wake-up containing the latest cursor.
  It is advisory; reconnect and periodic cursor catch-up remain authoritative.
- **Replication Store**: the deep server Module that atomically accepts transactions, records
  commits, serves snapshots/deltas, tracks watermarks, and compacts history. Memory, Postgres, and
  MySQL are Adapter Implementations of this Interface.

## Invariants

1. UI reads and writes never wait for the network.
2. Item changes and outbox records commit atomically in IndexedDB.
3. Multi-item transactions either converge in full or do not commit.
4. Cursor advancement and the corresponding local item writes are atomic.
5. Commit pages contain whole transactions.
6. Missing realtime messages cannot cause missing data; cursor catch-up repairs the gap.
7. A workspace cannot read or mutate another workspace's rows.
8. History is compacted only behind active client watermarks; stale clients bootstrap again.
