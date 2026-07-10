import { env } from '$env/dynamic/private';
import { json, type RequestHandler } from '@sveltejs/kit';
import { Effect } from 'effect';
import { publishSyncChange } from '$lib/server/realtime';
import { syncProgram } from '$lib/server/sync-service';
import { MAX_SYNC_REQUEST_BYTES } from '$lib/shared/replication';

function errorResponse(error: unknown) {
	const tag = typeof error === 'object' && error && '_tag' in error ? String(error._tag) : '';
	const message = error instanceof Error ? error.message : 'Sync failed';
	const status =
		tag.includes('Parse') || tag.includes('InvalidSyncRequest') || error instanceof SyntaxError
			? 400
			: 500;

	return json({ error: message, tag }, { status });
}

export const POST: RequestHandler = async ({ request }) => {
	try {
		const contentLength = Number(request.headers.get('content-length') ?? 0);
		if (contentLength > MAX_SYNC_REQUEST_BYTES) {
			return json({ error: 'Sync request is too large' }, { status: 413 });
		}
		const text = await request.text();
		if (new TextEncoder().encode(text).byteLength > MAX_SYNC_REQUEST_BYTES) {
			return json({ error: 'Sync request is too large' }, { status: 413 });
		}
		const body = JSON.parse(text);
		const result = await Effect.runPromise(syncProgram(body));

		const changedItemIds = [
			...new Set(
				result.applied
					.filter((outcome) => outcome.status === 'applied')
					.map((outcome) => outcome.itemId)
			)
		];

		if (
			changedItemIds.length > 0 &&
			typeof body === 'object' &&
			body !== null &&
			'clientId' in body &&
			typeof body.clientId === 'string'
		) {
			await publishSyncChange({
				sourceClientId: body.clientId,
				workspaceId: result.workspaceId,
				cursor: result.latestCursor,
				itemIds: changedItemIds,
				databaseMode: result.databaseMode,
				serverTime: result.serverTime
			}, {
				publishDatabaseUrl: env.DATABASE_URL
			});
		}

		return json(result, {
			headers: {
				'x-self-sync-cursor': result.latestCursor,
				'x-self-sync-has-more': String(result.hasMore),
				'server-timing': `sync;desc="${result.databaseMode}"`
			}
		});
	} catch (error) {
		return errorResponse(error);
	}
};
