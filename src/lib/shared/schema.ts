import { Schema } from 'effect';

const KanbanStageSchema = Schema.Literal('todo', 'doing', 'done');

export const ClientSyncItemSchema = Schema.Struct({
	id: Schema.String,
	name: Schema.String,
	note: Schema.String,
	stage: Schema.optionalWith(KanbanStageSchema, { default: () => 'todo' as const }),
	updatedAt: Schema.Number,
	deletedAt: Schema.NullOr(Schema.Number)
});

export const SyncChangeSchema = Schema.Struct({
	mutationId: Schema.String,
	op: Schema.Literal('upsert', 'delete'),
	item: ClientSyncItemSchema
});

export const SyncRequestSchema = Schema.Struct({
	clientId: Schema.String,
	changes: Schema.Array(SyncChangeSchema)
});

export type SyncRequestInput = Schema.Schema.Type<typeof SyncRequestSchema>;
