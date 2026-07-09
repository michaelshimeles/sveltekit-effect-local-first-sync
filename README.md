# Self Sync

Self Sync is a local-first sync framework for SvelteKit and Effect.

It gives you instant local UI, durable SQL-backed sync, and realtime convergence without making realtime the source of truth. Apps read and write to IndexedDB first, queue mutations while offline, sync to Postgres or MySQL when a connection is available, and use WebSockets to wake up other clients as changes land.

In one line:

> Local-first UX, SQL durability, realtime convergence.

Live app: https://sveltekit-effect-local-first-sync.vercel.app

## How To Explain It

Self Sync is both local-first and realtime, but local-first is the core.

Realtime is the acceleration layer. It does not carry authoritative app state or replace sync. It only tells connected clients that something changed, then each client pulls through the same sync endpoint and merges the result into its local database.

That means the app stays useful offline, feels instant while online, and still converges across tabs, browsers, and devices.

## What This Gives You

- **Instant UI:** reads and writes hit IndexedDB first, and Svelte updates from Dexie `liveQuery`.
- **Offline writes:** creates, edits, drag/drop moves, and deletes queue locally when the network is unavailable.
- **Durable sync:** the server persists records in Postgres or MySQL through a shared sync contract.
- **Realtime convergence:** WebSockets send invalidations so other clients pull fresh state immediately.
- **Deterministic conflict handling:** Effect validates requests, mutation IDs make retries idempotent, revisions and timestamps resolve stale writes, and tombstones make deletes sync correctly.
- **Zero-config development:** memory storage works locally without a database.

## Mental Model

```text
User action
  -> IndexedDB write
  -> reactive Svelte UI update
  -> outbox mutation
  -> POST /api/sync
  -> Postgres/MySQL
  -> WebSocket invalidation
  -> other clients pull and merge
```

The local database is the render source. SQL is the durable shared source. WebSockets are only the notification path.

## Demo App

The demo shows the framework with two views over the same synced records:

- **Chat:** type a message and press Enter. The message appears immediately and syncs in the background.
- **Kanban:** cards can be edited, deleted, and dragged between Trello-style columns. Moves are local-first mutations and sync like any other write.

## Stack

- SvelteKit 2 and Svelte 5
- Effect for request validation and server-side sync programs
- Dexie and IndexedDB as the reactive local source of truth
- WebSockets for low-latency cross-client invalidation
- Postgres or MySQL storage adapters
- Vercel Fluid Compute for production WebSocket support

## Run Locally

```sh
npm install
npm run dev
```

Open the local URL printed by Vite. No database is required for local development; the server falls back to an in-memory store.

## Configure SQL

Set `DATABASE_URL` to make sync durable.

Postgres:

```sh
DATABASE_DRIVER=postgres
DATABASE_URL=postgres://user:password@localhost:5432/self_sync
```

MySQL:

```sh
DATABASE_DRIVER=mysql
DATABASE_URL=mysql://user:password@localhost:3306/self_sync
```

The server creates the `sync_items` table automatically on first use. The raw schema files are also available in:

- `src/lib/server/sql/schema.postgres.sql`
- `src/lib/server/sql/schema.mysql.sql`

## Realtime

The app keeps IndexedDB as the render source and uses WebSockets as an invalidation channel. When `POST /api/sync` applies a create, update, or delete, the server publishes `sync_changed`. Connected clients that did not originate the change immediately run `syncNow('realtime')` and merge the authoritative server state into Dexie.

Local development uses the Vite WebSocket plugin:

```text
ws://localhost:5173/api/realtime
```

Production uses the Vercel function:

```text
wss://sveltekit-effect-local-first-sync.vercel.app/api/realtime
```

For Postgres deployments, the realtime broker uses `LISTEN/NOTIFY` so clients connected to different function instances still receive invalidations. Use an unpooled connection string for listening when your provider supplies one:

```sh
DATABASE_URL_UNPOOLED=postgres://user:password@host:5432/self_sync
# or
POSTGRES_URL_NON_POOLING=postgres://user:password@host:5432/self_sync
```

MySQL sync still works through the outbox and background pull loop. Cross-instance realtime for MySQL should use an external pub/sub service such as Redis, Ably, Pusher, or a binlog-backed bridge.

## API

- `POST /api/sync` applies queued mutations and returns authoritative server state.
- `GET /api/items` returns non-deleted server items.
- `GET /api/health` reports the active storage mode.
- `GET /api/realtime` upgrades to a WebSocket connection for realtime sync invalidations.

## Sync Model

Local writes update IndexedDB first and enqueue a latest mutation per item in the outbox. The UI renders from Dexie `liveQuery`, so creates, edits, and deletes appear immediately without waiting for the network.

`POST /api/sync` runs an Effect program that validates the request, applies queued mutations through the selected storage adapter, and returns the authoritative server state. The client merges that response back into IndexedDB and clears completed outbox mutations.

Conflict handling is deterministic: newer `updatedAt` wins, stale mutations are reconciled, and mutation IDs make retries idempotent. Deletes are tombstones, not hard local removals, so offline deletes sync correctly and other clients converge through the same merge path.

## Vercel Notes

The production deployment uses Vercel Fluid Compute for WebSockets. New Vercel projects have Fluid Compute enabled by default; older projects may need it enabled in project settings.

The public production domain is:

```text
https://sveltekit-effect-local-first-sync.vercel.app
```
