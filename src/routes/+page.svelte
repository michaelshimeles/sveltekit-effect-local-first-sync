<script lang="ts">
	import {
		AlertCircle,
		CheckCircle2,
		Cloud,
		Database,
		HardDrive,
		Plus,
		RefreshCw,
		Server,
		Trash2,
		Wifi,
		WifiOff
	} from '@lucide/svelte';
	import { onMount } from 'svelte';
	import { Badge } from '$lib/components/ui/badge';
	import { Button } from '$lib/components/ui/button';
	import * as Card from '$lib/components/ui/card';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Input } from '$lib/components/ui/input';
	import { Label } from '$lib/components/ui/label';
	import { Separator } from '$lib/components/ui/separator';
	import { Textarea } from '$lib/components/ui/textarea';
	import {
		addLocalItem,
		deleteLocalItem,
		localItems,
		localSummary,
		outboxItems,
		realtimeActivity,
		updateLocalItem
	} from '$lib/client/local-store';
	import { startSyncLoop, syncNow } from '$lib/client/sync-engine';
	import type { LocalItem } from '$lib/shared/types';
	import { cn } from '$lib/utils';

	let name = $state('');
	let note = $state('');
	let formError = $state('');
	let pendingDelete = $state<LocalItem | null>(null);
	let deleteDialogOpen = $state(false);
	let cleanup = $state<() => void>(() => {});

	onMount(() => {
		cleanup = startSyncLoop();
		return () => cleanup();
	});

	async function handleAdd(event: SubmitEvent) {
		event.preventDefault();
		formError = '';

		if (!name.trim()) {
			formError = 'Name is required.';
			return;
		}

		await addLocalItem({ name, note });
		name = '';
		note = '';
	}

	async function handleDeleteConfirmed() {
		if (!pendingDelete) return;

		await deleteLocalItem(pendingDelete.id);
		pendingDelete = null;
		deleteDialogOpen = false;
		void syncNow('manual');
	}

	function requestDelete(item: LocalItem) {
		pendingDelete = item;
		deleteDialogOpen = true;
	}

	function handleDeleteDialogOpenChange(open: boolean) {
		deleteDialogOpen = open;
		if (!open) pendingDelete = null;
	}

	function fieldValue(event: Event) {
		const target = event.currentTarget;
		if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
			return target.value;
		}
		return '';
	}

	function formatTime(value: number | null) {
		if (!value) return 'Never';
		return new Intl.DateTimeFormat(undefined, {
			hour: 'numeric',
			minute: '2-digit',
			second: '2-digit'
		}).format(value);
	}

	function statusLabel(item: LocalItem) {
		if (item.syncStatus === 'synced') return 'Synced';
		if (item.syncStatus === 'error') return 'Retrying';
		return 'Queued';
	}
</script>

<div class="min-h-dvh bg-background text-foreground">
	<header class="border-b bg-card">
		<div class="mx-auto flex max-w-7xl flex-col gap-4 px-4 py-5 sm:px-6 lg:px-8">
			<div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
				<div class="space-y-1">
					<p class="text-xs font-medium text-muted-foreground">SvelteKit + Effect + Local-first</p>
					<h1 class="text-balance text-2xl font-semibold tracking-tight sm:text-3xl">Self Sync</h1>
				</div>

				<div class="flex flex-wrap items-center gap-2">
					<Badge
						variant="outline"
						class={cn(
							'h-7 gap-2 rounded-md px-3 text-xs',
							$localSummary.online
								? 'border-emerald-200 bg-emerald-50 text-emerald-800'
								: 'border-amber-200 bg-amber-50 text-amber-800'
						)}
					>
						{#if $localSummary.online}
							<Wifi class="size-3.5" />
							Online
						{:else}
							<WifiOff class="size-3.5" />
							Offline
						{/if}
					</Badge>

					<Button size="lg" onclick={() => syncNow('manual')}>
						<RefreshCw class="size-4" />
						Sync
					</Button>
				</div>
			</div>

			<div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
				<Card.Root size="sm">
					<Card.Content>
						<div class="flex items-center justify-between gap-3">
							<p class="text-xs font-medium text-muted-foreground">Local records</p>
							<HardDrive class="size-4 text-muted-foreground" />
						</div>
						<p class="mt-2 text-2xl font-semibold tabular-nums">{$localSummary.total}</p>
					</Card.Content>
				</Card.Root>

				<Card.Root size="sm">
					<Card.Content>
						<div class="flex items-center justify-between gap-3">
							<p class="text-xs font-medium text-muted-foreground">Queued writes</p>
							<Database class="size-4 text-muted-foreground" />
						</div>
						<p class="mt-2 text-2xl font-semibold tabular-nums">{$localSummary.queued}</p>
					</Card.Content>
				</Card.Root>

				<Card.Root size="sm">
					<Card.Content>
						<div class="flex items-center justify-between gap-3">
							<p class="text-xs font-medium text-muted-foreground">Server mode</p>
							<Server class="size-4 text-muted-foreground" />
						</div>
						<p class="mt-2 truncate text-2xl font-semibold">{$localSummary.activity.databaseMode}</p>
					</Card.Content>
				</Card.Root>

				<Card.Root size="sm">
					<Card.Content>
						<div class="flex items-center justify-between gap-3">
							<p class="text-xs font-medium text-muted-foreground">Last sync</p>
							<Cloud class="size-4 text-muted-foreground" />
						</div>
						<p class="mt-2 text-2xl font-semibold tabular-nums">
							{formatTime($localSummary.activity.lastSyncedAt)}
						</p>
					</Card.Content>
				</Card.Root>
			</div>
		</div>
	</header>

	<main class="mx-auto grid max-w-7xl gap-6 px-4 py-6 sm:px-6 lg:grid-cols-[minmax(0,1fr)_360px] lg:px-8">
		<section class="space-y-4">
			<Card.Root>
				<Card.Content>
					<form onsubmit={handleAdd}>
						<div class="grid gap-3 md:grid-cols-[minmax(0,280px)_minmax(0,1fr)_auto]">
							<div class="grid gap-1.5">
								<Label for="item-name">Name</Label>
								<Input
									id="item-name"
									bind:value={name}
									class="h-9"
									placeholder="Item name"
									type="text"
								/>
							</div>

							<div class="grid gap-1.5">
								<Label for="item-note">Note</Label>
								<Input
									id="item-note"
									bind:value={note}
									class="h-9"
									placeholder="Optional note"
									type="text"
								/>
							</div>

							<Button type="submit" size="lg" class="h-9 w-full md:mt-auto md:w-auto">
								<Plus class="size-4" />
								Add
							</Button>
						</div>

						{#if formError}
							<p class="mt-2 text-sm text-destructive">{formError}</p>
						{/if}
					</form>
				</Card.Content>
			</Card.Root>

			<div class="space-y-3">
				{#if $localItems.length === 0}
					<Card.Root class="border-dashed">
						<Card.Content class="py-8 text-center">
							<p class="text-base font-medium">No local records</p>
							<p class="mt-1 text-pretty text-sm text-muted-foreground">
								Create one and it will sync immediately.
							</p>
						</Card.Content>
					</Card.Root>
				{:else}
					{#each $localItems as item (item.id)}
						<Card.Root size="sm">
							<Card.Content>
								<div class="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
									<div class="grid gap-3">
										<Input
											class="h-9 bg-muted/40 text-base font-medium"
											value={item.name}
											oninput={(event) =>
												updateLocalItem(item.id, {
													name: fieldValue(event)
												})}
										/>
										<Textarea
											class="min-h-20 resize-y bg-muted/40 text-sm leading-6"
											value={item.note}
											oninput={(event) =>
												updateLocalItem(item.id, {
													note: fieldValue(event)
												})}
										/>
									</div>

									<div class="flex items-start justify-between gap-3 md:flex-col md:items-end">
										<Badge
											variant="outline"
											class={cn(
												'h-7 gap-2 rounded-md px-2.5',
												item.syncStatus === 'synced' &&
													'border-emerald-200 bg-emerald-50 text-emerald-800',
												item.syncStatus === 'pending' &&
													'border-amber-200 bg-amber-50 text-amber-800',
												item.syncStatus === 'error' && 'border-red-200 bg-red-50 text-red-800'
											)}
										>
											{#if item.syncStatus === 'synced'}
												<CheckCircle2 class="size-3.5" />
											{:else}
												<AlertCircle class="size-3.5" />
											{/if}
											{statusLabel(item)}
										</Badge>

										<Button
											type="button"
											variant="outline"
											size="icon-lg"
											aria-label={`Delete ${item.name}`}
											title="Delete"
											onclick={() => requestDelete(item)}
										>
											<Trash2 class="size-4" />
										</Button>
									</div>
								</div>

								{#if item.lastError}
									<p class="mt-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
										{item.lastError}
									</p>
								{/if}
							</Card.Content>
						</Card.Root>
					{/each}
				{/if}
			</div>
		</section>

		<aside class="space-y-4">
			<Card.Root>
				<Card.Header>
					<div class="flex items-center justify-between gap-3">
						<Card.Title>Sync status</Card.Title>
						<Badge
							variant="outline"
							class={cn(
								'rounded-md capitalize',
								$localSummary.activity.status === 'synced' && 'bg-emerald-50 text-emerald-800',
								$localSummary.activity.status === 'syncing' && 'bg-sky-50 text-sky-800',
								$localSummary.activity.status === 'offline' && 'bg-amber-50 text-amber-800',
								$localSummary.activity.status === 'error' && 'bg-red-50 text-red-800',
								$localSummary.activity.status === 'idle' && 'bg-muted text-muted-foreground'
							)}
						>
							{$localSummary.activity.status}
						</Badge>
					</div>
					<Card.Description>{$localSummary.activity.lastMessage}</Card.Description>
				</Card.Header>
				<Card.Content class="space-y-3">
					{#if $localSummary.activity.error}
						<p class="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
							{$localSummary.activity.error}
						</p>
					{/if}

					<Separator />

					<div class="rounded-md bg-muted/50 px-3 py-2">
						<div class="flex items-center justify-between gap-3">
							<p class="text-sm font-medium">Realtime</p>
							<Badge
								variant="outline"
								class={cn(
									'rounded-md capitalize',
									$realtimeActivity.status === 'connected' && 'bg-emerald-50 text-emerald-800',
									$realtimeActivity.status === 'connecting' && 'bg-sky-50 text-sky-800',
									$realtimeActivity.status === 'disconnected' && 'bg-amber-50 text-amber-800',
									$realtimeActivity.status === 'error' && 'bg-red-50 text-red-800'
								)}
							>
								{$realtimeActivity.status}
							</Badge>
						</div>
						<p class="mt-1 text-pretty text-xs text-muted-foreground">{$realtimeActivity.message}</p>
					</div>
				</Card.Content>
			</Card.Root>

			<Card.Root>
				<Card.Header>
					<Card.Title>Outbox</Card.Title>
				</Card.Header>
				<Card.Content class="space-y-2">
					{#if $outboxItems.length === 0}
						<p class="rounded-md bg-muted/50 px-3 py-3 text-sm text-muted-foreground">
							No queued writes.
						</p>
					{:else}
						{#each $outboxItems as mutation (mutation.id)}
							<div class="rounded-md border px-3 py-2">
								<div class="flex items-center justify-between gap-3">
									<p class="truncate text-sm font-medium">{mutation.item.name}</p>
									<Badge variant="secondary" class="rounded-md">{mutation.op}</Badge>
								</div>
								<p class="mt-1 text-xs text-muted-foreground tabular-nums">
									Attempts: {mutation.attempts}
								</p>
							</div>
						{/each}
					{/if}
				</Card.Content>
			</Card.Root>
		</aside>
	</main>
</div>

<Dialog.Root bind:open={deleteDialogOpen} onOpenChange={handleDeleteDialogOpenChange}>
	<Dialog.Content>
		<Dialog.Header>
			<Dialog.Title>Delete record</Dialog.Title>
			<Dialog.Description>
				This queues a delete mutation for {pendingDelete?.name ?? 'this record'}.
			</Dialog.Description>
		</Dialog.Header>
		<Dialog.Footer>
			<Button
				type="button"
				variant="outline"
				onclick={() => handleDeleteDialogOpenChange(false)}
			>
				Cancel
			</Button>
			<Button type="button" variant="destructive" onclick={handleDeleteConfirmed}>Delete</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
