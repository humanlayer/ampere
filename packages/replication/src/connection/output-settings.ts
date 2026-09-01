import { Data, Effect, Schema } from 'effect'

import { ReplicationOperationFailure } from './schemas'

export interface OutputSettingsQueryResult {
	readonly rows: ReadonlyArray<unknown>
}

export interface OutputSettingsConnection {
	readonly query: (queryText: string) => Promise<OutputSettingsQueryResult>
}

interface PinOutputSettingsEffectInput {
	readonly connection: OutputSettingsConnection
}

const PinnedOutputSettingsResult = Schema.Struct({
	bytea_output: Schema.String,
	date_style: Schema.String,
	time_zone: Schema.String,
	extra_float_digits: Schema.String,
	interval_style: Schema.String,
})

class OutputSettingsQueryFailed extends Data.TaggedError('OutputSettingsQueryFailed')<{
	readonly cause: unknown
}> {}

const setupCommandFailed = ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'setup-command-failed' })
const replicationPrerequisiteInvalid = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'replication-prerequisite-invalid',
})

const executeOutputSettingsQuery = Effect.fn('replication_output_settings.execute_query')(
	(connection: OutputSettingsConnection, queryText: string) =>
		Effect.tryPromise({
			try: () => connection.query(queryText),
			catch: (cause) => new OutputSettingsQueryFailed({ cause }),
		}).pipe(
			Effect.tapError((error) => Effect.logError('Replication output settings query failed', error)),
			Effect.mapError(() => setupCommandFailed),
		),
)

const outputSettingsArePinned = (settings: typeof PinnedOutputSettingsResult.Type): boolean =>
	settings.bytea_output === 'hex' &&
	settings.date_style === 'ISO, DMY' &&
	settings.time_zone === 'UTC' &&
	settings.extra_float_digits === '1' &&
	settings.interval_style === 'iso_8601'

export const pinOutputSettings = Effect.fn('replication_output_settings.pin_output_settings')(function* ({
	connection,
}: PinOutputSettingsEffectInput) {
	yield* executeOutputSettingsQuery(
		connection,
		`SET bytea_output = 'hex';
		SET DateStyle = 'ISO, DMY';
		SET TimeZone = 'UTC';
		SET extra_float_digits = 1;
		SET IntervalStyle = 'iso_8601'`,
	)
	const result = yield* executeOutputSettingsQuery(
		connection,
		`SELECT
			current_setting('bytea_output') AS bytea_output,
			current_setting('DateStyle') AS date_style,
			current_setting('TimeZone') AS time_zone,
			current_setting('extra_float_digits') AS extra_float_digits,
			current_setting('IntervalStyle') AS interval_style`,
	)
	const settings = yield* Schema.decodeUnknownEffect(PinnedOutputSettingsResult)(result.rows.at(0)).pipe(
		Effect.tapError((error) => Effect.logError('Invalid replication output settings response', error)),
		Effect.mapError(() => replicationPrerequisiteInvalid),
	)
	if (!outputSettingsArePinned(settings)) {
		return yield* Effect.fail(replicationPrerequisiteInvalid)
	}
	return yield* Effect.void
})
