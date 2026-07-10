import { browser } from '$app/environment';
import { DEFAULT_WORKSPACE_ID } from '$lib/shared/replication';

const CLIENT_ID_KEY = 'self-sync-client-id';
const WORKSPACE_ID_KEY = 'self-sync-workspace-id';

export function getClientId() {
	if (!browser) return 'server';

	const existing = localStorage.getItem(CLIENT_ID_KEY);
	if (existing) return existing;

	const next = crypto.randomUUID();
	localStorage.setItem(CLIENT_ID_KEY, next);
	return next;
}

export function getWorkspaceId() {
	if (!browser) return DEFAULT_WORKSPACE_ID;
	return localStorage.getItem(WORKSPACE_ID_KEY) || DEFAULT_WORKSPACE_ID;
}

export function setWorkspaceId(workspaceId: string) {
	if (!browser) return;
	localStorage.setItem(WORKSPACE_ID_KEY, workspaceId);
}
