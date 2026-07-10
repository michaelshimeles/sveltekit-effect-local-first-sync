# ADR 0001: Cursor-based transactional replication

- Status: accepted
- Date: 2026-07-10

## Context

The original protocol returned every server row on every sync and used WebSockets only to trigger
another full pull. That has simple convergence semantics, but request cost grows with total data
and every connected client can amplify one write into a full-table read.

## Decision

Protocol v2 uses workspace-scoped, transaction-aligned cursors:

1. A new or reset client receives a stable, paginated bootstrap snapshot.
2. An initialized client receives record-bounded pages of whole commits after its durable cursor.
3. Server commits store immutable item snapshots, including tombstones, for each transaction.
4. The client applies a commit page and advances its cursor in one IndexedDB transaction.
5. WebSockets carry only `{ workspaceId, cursor }` invalidations. HTTP catch-up is authoritative.
6. Active client watermarks permit bounded retention. A client behind the compaction floor is told
   to discard its cursor and bootstrap again.
   A cursor ahead of server state is also reset, which covers an ephemeral development store restart.
7. Protocol v1 remains available for rolling compatibility and is intentionally the only path that
   returns a full-table response.

## Consequences

- Normal sync cost is proportional to new transactions, not total workspace size.
- Multi-item atomicity remains visible to clients because pages never split a commit.
- A single transaction may exceed the target page size, but request validation caps its changes.
- Bootstrap is bounded but can require several requests.
- Postgres/MySQL require commit-log, client-watermark, and workspace-state tables.
- Workspace routing must be bound to an authenticated principal before multi-tenant production use.
- `LISTEN/NOTIFY` and WebSockets remain replaceable Adapters; correctness does not depend on them.
