import { decodePgOutputMessage } from '@ampere/schemas/wal'
import { Effect, Layer, Match, Ref, Stream } from 'effect'

import { createCommittedChangeBatch } from '../change-feed/events'
import { ChangeFeedApi } from '../change-feed/service'
import { ReplicationOperationFailure, ReplicationProtocolFrame } from '../connection/schemas'
import { ReplicationOperations } from '../connection/service'
import type { AssembledPgOutputEvent } from '../transaction-assembly/assembled-events'
import { PgOutputTransactionAssembler } from '../transaction-assembly/assembler-service'
import { ReplicationIngestApi } from './service'
import type { ConsumeReplicationSessionInput } from './service'

const replicationIngestFailed = ReplicationOperationFailure.cases.SessionUnavailable.make({
	reason: 'ingest-pipeline-failed',
})

const consumeReplicationSession = (input: ConsumeReplicationSessionInput) =>
	Effect.gen(function* () {
		const operations = yield* ReplicationOperations
		const changeFeed = yield* ChangeFeedApi
		const assembler = yield* PgOutputTransactionAssembler
		const nextBatchFromLsn = yield* Ref.make(input.initialSafeFlushLsn)

		const publishAssembledEvent = (event: AssembledPgOutputEvent) =>
			Match.value(event).pipe(
				Match.tag('RelationChanged', () => Effect.void),
				Match.tag('CommittedTransaction', (transaction) =>
					Effect.gen(function* () {
						const fromLsn = yield* Ref.get(nextBatchFromLsn)
						const batch = createCommittedChangeBatch({ fromLsn, transaction })
						yield* changeFeed.publishCommittedChangeBatch({ batch })
						yield* Ref.set(nextBatchFromLsn, batch.toLsn)
						yield* operations.acknowledgeReplicationLsn({ safeFlushLsn: batch.toLsn })
					}),
				),
				Match.exhaustive,
			)

		const consumeAssemblerEvents = assembler.events.pipe(Stream.runForEach(publishAssembledEvent))
		const sendDecodedFrames = operations.streamReplicationFrames(input).pipe(
			Stream.filter(ReplicationProtocolFrame.guards.XLogData),
			Stream.mapEffect((frame) => decodePgOutputMessage({ bytes: frame.payload })),
			Stream.runForEach((message) => assembler.send({ message })),
		)

		yield* Effect.all([consumeAssemblerEvents, sendDecodedFrames], { concurrency: 'unbounded', discard: true })
	}).pipe(
		Effect.tapError((error) => Effect.logError('Replication ingest session failed', error)),
		Effect.mapError(() => replicationIngestFailed),
	)

export const ReplicationIngestApiLive = Layer.succeed(ReplicationIngestApi, {
	consumeReplicationSession,
})
