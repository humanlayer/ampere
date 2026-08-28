import { Config, Effect, Layer, Redacted, Schema } from 'effect'
import { Client } from 'pg'

import {
	IdentifySystemResult,
	ReadServerInfoResult,
	ReplicationOperationFailure,
	ReplicationServerInfo,
	ReplicationSourceIdentity,
} from './schemas.ts'
import { ReplicationOperations } from './service'
import type { ReplicationOperationsApi } from './service'

const notImplemented = (operation: string): Effect.Effect<never> =>
	Effect.die(new Error(`Replication operation ${operation} is not implemented`))

export const ReplicationOperationsLayer = Layer.effect(
	ReplicationOperations,
	Effect.gen(function* () {
		const pgConnectionString = yield* Config.redacted('POSTGRES_URL').pipe(
			Config.withDefault(Redacted.make('postgres://postgres:postgres@localhost:55432/ampere')),
		)
		const keepaliveIntervalMilliseconds = yield* Config.int('REPLICATION_KEEPALIVE_INTERVAL_MILLISECONDS').pipe(
			Config.withDefault(10_000),
		)

		const connection = yield* Effect.acquireRelease(
			Effect.sync(() => {
				const connectionUrl = new URL(Redacted.value(pgConnectionString))
				connectionUrl.searchParams.set('replication', 'database')
				return new Client({ connectionString: connectionUrl.toString() })
			}),
			(acquiredClient) =>
				Effect.promise(() => acquiredClient.end()).pipe(
					Effect.catchCause((cause) => Effect.logWarning('Failed to close node-postgres client', cause)),
				),
		)

		const operations: ReplicationOperationsApi = {
			openReplicationSession: Effect.fn('replication_operations.open_replication_session')(function* () {
				yield* Effect.promise(() => connection.connect()).pipe(
					Effect.tapError((error) => Effect.logError('Error opening replication session', error)),
					Effect.mapError(() =>
						ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'connection-open-failed' }),
					),
				)
			}),
			identifySource: Effect.fn('replication_operations.identify_source')(function* () {
				const result = yield* Effect.promise(() => connection.query(`IDENTIFY_SYSTEM`)).pipe(
					Effect.tapError((e) => Effect.logError('Error identifying source:', e)),
					Effect.mapError(() =>
						ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'setup-command-failed' }),
					),
				)
				const identifiedSystem = yield* Schema.decodeUnknownEffect(IdentifySystemResult)(
					result.rows.at(0),
				).pipe(
					Effect.tapError((error) => Effect.logError('Invalid IDENTIFY_SYSTEM response', error)),
					Effect.mapError(() =>
						ReplicationOperationFailure.cases.SourceRejected.make({
							reason: 'replication-prerequisite-invalid',
						}),
					),
				)

				return ReplicationSourceIdentity.make({
					systemIdentifier: identifiedSystem.systemid,
					timelineId: identifiedSystem.timeline,
					databaseName: identifiedSystem.dbname,
					currentWalFlushLsn: identifiedSystem.xlogpos,
				})
			}),
			readServerInfo: Effect.fn('replication_operations.read_server_info')(function* () {
				const result = yield* Effect.promise(() =>
					connection.query(`
					SELECT
						current_setting('server_version_num') AS server_version_number,
						pg_backend_pid() AS backend_process_id,
						EXTRACT(
							EPOCH FROM current_setting('wal_sender_timeout')::interval
						) * 1000 AS wal_sender_timeout_milliseconds
					`),
				).pipe(
					Effect.tapError((e) => Effect.logError('Error reading server info', e)),
					Effect.mapError(() =>
						ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'setup-command-failed' }),
					),
				)

				const serverInfo = yield* Schema.decodeUnknownEffect(ReadServerInfoResult)(result.rows.at(0)).pipe(
					Effect.tapError((error) => Effect.logError('Invalid identify system response', error)),
					Effect.mapError(() =>
						ReplicationOperationFailure.cases.SourceRejected.make({
							reason: 'replication-prerequisite-invalid',
						}),
					),
				)
				return ReplicationServerInfo.make({
					serverVersionNumber: serverInfo.server_version_number,
					backendProcessId: serverInfo.backend_process_id,
					walSenderTimeoutMilliseconds: serverInfo.wal_sender_timeout_milliseconds,
					keepaliveIntervalMilliseconds,
				})
			}),
			acquireSlotLease: Effect.fn('replication_operations.acquire_slot_lease')(() =>
				notImplemented('acquireSlotLease'),
			),
			ensureReplicationContract: Effect.fn('replication_operations.ensure_replication_contract')(() =>
				notImplemented('ensureReplicationContract'),
			),
			ensureReplicationSlot: Effect.fn('replication_operations.ensure_replication_slot')(() =>
				notImplemented('ensureReplicationSlot'),
			),
			pinOutputSettings: Effect.fn('replication_operations.pin_output_settings')(() =>
				notImplemented('pinOutputSettings'),
			),
			startPgOutput: Effect.fn('replication_operations.start_pgoutput')(() => notImplemented('startPgOutput')),
			consumePgOutput: Effect.fn('replication_operations.consume_pgoutput')(() =>
				notImplemented('consumePgOutput'),
			),
		}

		return operations
	}),
)
