import { Buffer } from 'node:buffer'

import { Cause, Data, Effect, Match, Option, Predicate, Queue, Ref, Schema, Stream } from 'effect'
import { escapeIdentifier, escapeLiteral } from 'pg'
import type { Client, Connection, Submittable } from 'pg'

import { PostgresLsnValue, ReplicationOperationFailure, ReplicationProtocolFrameFromBytes } from './schemas.ts'
import type {
	AcknowledgeReplicationLsnInput,
	StartPgOutputInput,
	StreamReplicationFramesInput,
	ReplicationProtocolFrame,
} from './schemas.ts'

export const NodePostgresCopyDataPayload = Schema.Struct({
	chunk: Schema.Uint8Array,
})

interface NodePostgresReplicationConnection extends Connection {
	readonly sendCopyFromChunk: (chunk: Uint8Array) => void
}

interface ReplicationQuery extends Submittable {
	readonly handleCopyData: (message: typeof NodePostgresCopyDataPayload.Encoded) => void
	readonly handleError: (cause: unknown) => void
	readonly handleReadyForQuery: () => void
}

export interface ReplicationProtocolConnection {
	readonly startPgOutput: (
		input: typeof StartPgOutputInput.Type,
	) => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly streamReplicationFrames: (
		input: typeof StreamReplicationFramesInput.Type,
	) => Stream.Stream<typeof ReplicationProtocolFrame.Type, typeof ReplicationOperationFailure.Type>
	readonly acknowledgeReplicationLsn: (
		input: typeof AcknowledgeReplicationLsnInput.Type,
	) => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
}

class ReplicationProtocolAdapterUnavailable extends Data.TaggedError('ReplicationProtocolAdapterUnavailable')<{
	readonly reason: string
}> {}

class PgOutputStartupFailed extends Data.TaggedError('PgOutputStartupFailed')<{
	readonly cause: unknown
}> {}

const PostgresErrorCode = Schema.Struct({ code: Schema.String })
const postgresEpochMilliseconds = Date.UTC(2000, 0, 1)
const replicationFrameQueueCapacity = 256

const connectionErrored = ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'connection-errored' })
const serverEndedStream = ReplicationOperationFailure.cases.SessionUnavailable.make({
	reason: 'server-ended-stream',
})
const protocolIncompatible = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'pgoutput-protocol-incompatible',
})

const hasRequiredProtocolHooks = (connection: Connection): connection is NodePostgresReplicationConnection =>
	Predicate.isObject(connection) &&
	Predicate.hasProperty(connection, 'once') &&
	Predicate.hasProperty(connection, 'removeListener') &&
	Predicate.hasProperty(connection, 'query') &&
	Predicate.hasProperty(connection, 'sendCopyFromChunk') &&
	Predicate.isFunction(connection.sendCopyFromChunk) &&
	Predicate.hasProperty(connection, 'stream') &&
	Predicate.isObject(connection.stream) &&
	Predicate.hasProperty(connection.stream, 'pause') &&
	Predicate.hasProperty(connection.stream, 'resume')

const readProtocolConnection = (
	client: Client,
): Effect.Effect<NodePostgresReplicationConnection, ReplicationProtocolAdapterUnavailable> => {
	const connection = client.connection
	if (hasRequiredProtocolHooks(connection)) {
		return Effect.succeed(connection)
	}
	return Effect.fail(
		new ReplicationProtocolAdapterUnavailable({
			reason: 'Installed node-postgres client does not expose the required replication protocol hooks',
		}),
	)
}

const formatPostgresLsn = (lsn: bigint): string => {
	const segment = (lsn >> 32n).toString(16).toUpperCase()
	const offset = (lsn & 0xffff_ffffn).toString(16).toUpperCase()
	return `${segment}/${offset}`
}

export const makeStartPgOutputCommand = ({
	slotName,
	publicationName,
	startLsn,
}: typeof StartPgOutputInput.Type): string =>
	`START_REPLICATION SLOT ${escapeIdentifier(slotName)} LOGICAL ${formatPostgresLsn(startLsn)} (proto_version '1', publication_names ${escapeLiteral(escapeIdentifier(publicationName))})`

export const parseReplicationProtocolFrame = (
	bytes: Uint8Array,
): Effect.Effect<typeof ReplicationProtocolFrame.Type, typeof ReplicationOperationFailure.Type> =>
	Schema.decodeEffect(ReplicationProtocolFrameFromBytes)(bytes).pipe(Effect.mapError(() => protocolIncompatible))

export const makeStandbyStatusUpdate = (safeFlushLsn: bigint, timestampMilliseconds: number): Uint8Array => {
	const bytes = new Uint8Array(34)
	const view = new DataView(bytes.buffer)
	bytes[0] = 0x72
	view.setBigUint64(1, safeFlushLsn, false)
	view.setBigUint64(9, safeFlushLsn, false)
	view.setBigUint64(17, safeFlushLsn, false)
	view.setBigInt64(25, BigInt(timestampMilliseconds - postgresEpochMilliseconds) * 1000n, false)
	bytes[33] = 0
	return bytes
}

export const advanceSafeFlushLsn = (
	currentLsn: typeof PostgresLsnValue.Type,
	candidateLsn: typeof PostgresLsnValue.Type,
): typeof PostgresLsnValue.Type => (candidateLsn > currentLsn ? candidateLsn : currentLsn)

const classifyPgOutputStartupFailure = (error: PgOutputStartupFailed) => {
	const postgresError = Schema.decodeUnknownOption(PostgresErrorCode)(error.cause)
	if (Option.isNone(postgresError)) {
		return ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'setup-command-failed' })
	}
	if (postgresError.value.code === '42501') {
		return ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'replication-slot-permission-denied' })
	}
	if (postgresError.value.code === '0A000') {
		return protocolIncompatible
	}
	return ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'replication-prerequisite-invalid' })
}

const offerReplicationFrame = (
	connection: NodePostgresReplicationConnection,
	queue: Queue.Queue<typeof ReplicationProtocolFrame.Type, typeof ReplicationOperationFailure.Type>,
	frame: typeof ReplicationProtocolFrame.Type,
): void => {
	if (Queue.offerUnsafe(queue, frame)) {
		return
	}
	connection.stream.pause()
	Effect.runFork(
		Queue.offer(queue, frame).pipe(
			Effect.ensuring(
				Effect.sync(() => {
					connection.stream.resume()
				}),
			),
		),
	)
}

const sendStandbyStatusUpdate = (
	connection: NodePostgresReplicationConnection,
	safeFlushLsn: Ref.Ref<typeof PostgresLsnValue.Type>,
): Effect.Effect<void, typeof ReplicationOperationFailure.Type> =>
	Ref.get(safeFlushLsn).pipe(
		Effect.flatMap((lsn) =>
			Effect.try({
				try: () => connection.sendCopyFromChunk(Buffer.from(makeStandbyStatusUpdate(lsn, Date.now()))),
				catch: () => connectionErrored,
			}),
		),
	)

export const validateReplicationProtocolConnection = Effect.fn('replication_protocol.validate_connection')(function* (
	client: Client,
) {
	const connection = yield* readProtocolConnection(client).pipe(
		Effect.tapError((error) => Effect.logError('Invalid node-postgres replication protocol adapter', error)),
		Effect.mapError(() => protocolIncompatible),
	)
	const frameQueue = yield* Queue.bounded<
		typeof ReplicationProtocolFrame.Type,
		typeof ReplicationOperationFailure.Type
	>(replicationFrameQueueCapacity)
	const safeFlushLsn = yield* Ref.make(PostgresLsnValue.make(0n))
	const context = yield* Effect.context()
	const runFork = Effect.runForkWith(context)
	let replicationStarted = false

	const failFrameQueue = (failure: typeof ReplicationOperationFailure.Type): void => {
		Queue.failCauseUnsafe(frameQueue, Cause.fail(failure))
	}
	const handleCopyData = (message: typeof NodePostgresCopyDataPayload.Encoded): void => {
		runFork(
			Schema.decodeEffect(NodePostgresCopyDataPayload)(message).pipe(
				Effect.tapError((error) => Effect.logError('Invalid node-postgres CopyData message', error)),
				Effect.mapError(() => protocolIncompatible),
				Effect.flatMap(({ chunk }) => parseReplicationProtocolFrame(chunk)),
				Effect.tap((frame) =>
					Match.value(frame).pipe(
						Match.tag('PrimaryKeepalive', (keepalive) =>
							keepalive.replyRequested ? sendStandbyStatusUpdate(connection, safeFlushLsn) : Effect.void,
						),
						Match.orElse(() => Effect.void),
					),
				),
				Effect.tap((frame) => Effect.sync(() => offerReplicationFrame(connection, frameQueue, frame))),
				Effect.catch((failure) => Effect.sync(() => failFrameQueue(failure))),
			),
		)
	}

	const startPgOutput = (input: typeof StartPgOutputInput.Type) =>
		Effect.callback<void, PgOutputStartupFailed>((resume) => {
			const onReplicationStart = () => {
				replicationStarted = true
				connection.removeListener('replicationStart', onReplicationStart)
				resume(Effect.void)
			}
			const query: ReplicationQuery = {
				submit: () => connection.query(makeStartPgOutputCommand(input)),
				handleCopyData,
				handleError: (cause: unknown) => {
					connection.removeListener('replicationStart', onReplicationStart)
					if (replicationStarted) {
						failFrameQueue(connectionErrored)
					} else {
						resume(Effect.fail(new PgOutputStartupFailed({ cause })))
					}
				},
				handleReadyForQuery: () => failFrameQueue(serverEndedStream),
			}
			connection.once('replicationStart', onReplicationStart)
			client.query(query)
			return Effect.sync(() => connection.removeListener('replicationStart', onReplicationStart))
		})

	const acknowledgeReplicationLsn = Effect.fn('replication_protocol.acknowledge_replication_lsn')(
		({ safeFlushLsn: acknowledgedLsn }: typeof AcknowledgeReplicationLsnInput.Type) =>
			Ref.updateAndGet(safeFlushLsn, (currentLsn) => advanceSafeFlushLsn(currentLsn, acknowledgedLsn)).pipe(
				Effect.andThen(sendStandbyStatusUpdate(connection, safeFlushLsn)),
			),
	)

	return {
		startPgOutput: Effect.fn('replication_protocol.start_pgoutput')((input) =>
			startPgOutput(input).pipe(
				Effect.tapError((error) => Effect.logError('Failed to start pgoutput replication', error)),
				Effect.mapError(classifyPgOutputStartupFailure),
			),
		),
		streamReplicationFrames: (input) =>
			Stream.unwrap(
				Effect.gen(function* () {
					yield* Ref.update(safeFlushLsn, (currentLsn) =>
						advanceSafeFlushLsn(currentLsn, input.initialSafeFlushLsn),
					)
					yield* Effect.forever(
						Effect.sleep(input.keepaliveIntervalMilliseconds).pipe(
							Effect.andThen(sendStandbyStatusUpdate(connection, safeFlushLsn)),
							Effect.catch((failure) => Effect.sync(() => failFrameQueue(failure))),
						),
					).pipe(Effect.forkScoped)
					return Stream.fromQueue(frameQueue)
				}),
			),
		acknowledgeReplicationLsn,
	} satisfies ReplicationProtocolConnection
})
