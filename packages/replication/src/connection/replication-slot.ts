import { Data, Effect, Option, Schema } from 'effect'
import { escapeIdentifier, escapeLiteral } from 'pg'

import { Lsn, ReplicationOperationFailure, ReplicationSlotPosition } from './schemas'

export interface ReplicationSlotQueryResult {
	readonly rows: ReadonlyArray<unknown>
}

export interface ReplicationSlotConnection {
	readonly query: (queryText: string) => Promise<ReplicationSlotQueryResult>
}

interface EnsureReplicationSlotEffectInput {
	readonly connection: ReplicationSlotConnection
	readonly slotName: string
}

const ExistingReplicationSlotResult = Schema.Struct({
	slot_type: Schema.String,
	plugin: Schema.NullOr(Schema.String),
	database_matches: Schema.Boolean,
	active: Schema.Boolean,
	confirmed_flush_lsn: Schema.NullOr(Lsn),
})

const CreatedReplicationSlotResult = Schema.Struct({
	slot_name: Schema.String,
	consistent_point: Lsn,
	output_plugin: Schema.String,
})

const setupCommandFailed = ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'setup-command-failed' })
const replicationPrerequisiteInvalid = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'replication-prerequisite-invalid',
})
const pgOutputProtocolIncompatible = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'pgoutput-protocol-incompatible',
})

class ReplicationSlotQueryFailed extends Data.TaggedError('ReplicationSlotQueryFailed')<{
	readonly cause: unknown
}> {}

const PostgresErrorCode = Schema.Struct({ code: Schema.String })

const classifyReplicationSlotQueryFailure = (error: ReplicationSlotQueryFailed) => {
	const postgresError = Schema.decodeUnknownOption(PostgresErrorCode)(error.cause)
	return Option.isSome(postgresError) && postgresError.value.code === '42501'
		? ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'replication-slot-permission-denied' })
		: setupCommandFailed
}

const executeReplicationSlotQuery = Effect.fn('replication_slot.execute_query')(
	(connection: ReplicationSlotConnection, queryText: string) =>
		Effect.tryPromise({
			try: () => connection.query(queryText),
			catch: (cause) => new ReplicationSlotQueryFailed({ cause }),
		}).pipe(
			Effect.tapError((error) => Effect.logError('Replication slot query failed', error)),
			Effect.mapError(classifyReplicationSlotQueryFailure),
		),
)

export const ensureReplicationSlot = Effect.fn('replication_slot.ensure_replication_slot')(function* ({
	connection,
	slotName,
}: EnsureReplicationSlotEffectInput) {
	const existingResult = yield* executeReplicationSlotQuery(
		connection,
		`SELECT slot_type, plugin, database = current_database() AS database_matches, active,
			confirmed_flush_lsn::text AS confirmed_flush_lsn
		FROM pg_replication_slots
		WHERE slot_name = ${escapeLiteral(slotName)}`,
	)
	const existingSlot = Schema.decodeUnknownOption(ExistingReplicationSlotResult)(existingResult.rows.at(0))

	if (Option.isSome(existingSlot)) {
		const slot = existingSlot.value
		if (
			slot.slot_type !== 'logical' ||
			!slot.database_matches ||
			slot.active ||
			slot.confirmed_flush_lsn === null
		) {
			return yield* Effect.fail(replicationPrerequisiteInvalid)
		}
		if (slot.plugin !== 'pgoutput') {
			return yield* Effect.fail(pgOutputProtocolIncompatible)
		}
		return ReplicationSlotPosition.make({ confirmedFlushLsn: slot.confirmed_flush_lsn, slotWasCreated: false })
	}

	const createdResult = yield* executeReplicationSlotQuery(
		connection,
		`CREATE_REPLICATION_SLOT ${escapeIdentifier(slotName)} LOGICAL pgoutput NOEXPORT_SNAPSHOT`,
	)
	const createdSlot = yield* Schema.decodeUnknownEffect(CreatedReplicationSlotResult)(createdResult.rows.at(0)).pipe(
		Effect.tapError((error) => Effect.logError('Invalid CREATE_REPLICATION_SLOT response', error)),
		Effect.mapError(() => replicationPrerequisiteInvalid),
	)
	if (createdSlot.slot_name !== slotName || createdSlot.output_plugin !== 'pgoutput') {
		return yield* Effect.fail(pgOutputProtocolIncompatible)
	}
	return ReplicationSlotPosition.make({ confirmedFlushLsn: createdSlot.consistent_point, slotWasCreated: true })
})
