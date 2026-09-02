import { Lsn } from '@ampere/schemas/wal'
import { describe, it } from '@effect/vitest'
import { Deferred, Effect, Fiber, Layer, Queue, Ref, Stream } from 'effect'

import type { CommittedChangeBatch } from '../../src/change-feed/events'
import { ChangeFeedApi, ChangeFeedUnavailable } from '../../src/change-feed/service'
import type { ChangeFeedService } from '../../src/change-feed/service'
import { ReplicationOperationFailure, ReplicationProtocolFrame } from '../../src/connection/schemas'
import { ReplicationOperations } from '../../src/connection/service'
import type { ReplicationOperationsApi } from '../../src/connection/service'
import { ReplicationIngestApiLive } from '../../src/ingest/layer'
import { ReplicationIngestApi } from '../../src/ingest/service'
import { PgOutputTransactionAssemblerLive } from '../../src/transaction-assembly/assembler-layer'

const initialSafeFlushLsn = Lsn.make(1n)
const transactionEndLsn = Lsn.make((2n << 32n) | 2_817_829_040n)

const beginPayload = new Uint8Array([66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107])
const relationPayload = new Uint8Array([
	82, 0, 0, 96, 0, 112, 117, 98, 108, 105, 99, 0, 102, 111, 111, 0, 100, 0, 2, 0, 98, 97, 114, 0, 0, 0, 0, 25, 255,
	255, 255, 255, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255,
])
const insertPayload = new Uint8Array([
	73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48,
])
const commitPayload = new Uint8Array([
	67, 0, 0, 0, 0, 2, 167, 244, 168, 128, 0, 0, 0, 2, 167, 244, 168, 176, 0, 2, 48, 246, 88, 88, 213, 242,
])

const makeXLogData = (payload: Uint8Array) =>
	ReplicationProtocolFrame.cases.XLogData.make({
		walStart: Lsn.make(1n),
		serverWalEnd: transactionEndLsn,
		serverTimestampMicroseconds: 0n,
		payload,
	})

const transactionFrames = [
	makeXLogData(beginPayload),
	makeXLogData(relationPayload),
	makeXLogData(insertPayload),
	makeXLogData(commitPayload),
]

const unexpectedOperation = () => Effect.die('Unexpected replication operation')

const makeReplicationOperations = ({
	frames,
	acknowledgements,
	acknowledged,
}: {
	readonly frames: Stream.Stream<typeof ReplicationProtocolFrame.Type, typeof ReplicationOperationFailure.Type>
	readonly acknowledgements: Ref.Ref<ReadonlyArray<bigint>>
	readonly acknowledged?: Deferred.Deferred<void>
}): ReplicationOperationsApi => ({
	openReplicationSession: unexpectedOperation,
	identifySource: unexpectedOperation,
	readServerInfo: unexpectedOperation,
	acquireSlotLease: unexpectedOperation,
	ensureReplicationContract: unexpectedOperation,
	ensureReplicationSlot: unexpectedOperation,
	pinOutputSettings: unexpectedOperation,
	startPgOutput: unexpectedOperation,
	streamReplicationFrames: () => frames,
	acknowledgeReplicationLsn: ({ safeFlushLsn }) =>
		Ref.update(acknowledgements, (recorded) => [...recorded, safeFlushLsn]).pipe(
			Effect.andThen(acknowledged === undefined ? Effect.void : Deferred.succeed(acknowledged, undefined)),
			Effect.asVoid,
		),
})

const makeReplicationIngestLayer = (operations: ReplicationOperationsApi, changeFeed: ChangeFeedService) =>
	Layer.merge(
		Layer.merge(
			Layer.merge(Layer.succeed(ReplicationOperations, operations), Layer.succeed(ChangeFeedApi, changeFeed)),
			PgOutputTransactionAssemblerLive,
		),
		ReplicationIngestApiLive,
	)

describe('ReplicationIngestApi', () => {
	it.effect('publishes a complete transaction before acknowledging its end LSN', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const acknowledgements = yield* Ref.make<ReadonlyArray<bigint>>([])
				const publishedBatches = yield* Queue.unbounded<CommittedChangeBatch>()
				const acknowledged = yield* Deferred.make<void>()
				const changeFeed: ChangeFeedService = {
					publishCommittedChangeBatch: ({ batch }) =>
						Queue.offer(publishedBatches, batch).pipe(Effect.asVoid),
					committedChangeBatches: Stream.empty,
				}
				const operations = makeReplicationOperations({
					frames: Stream.concat(Stream.fromIterable(transactionFrames), Stream.never),
					acknowledgements,
					acknowledged,
				})
				yield* Effect.gen(function* () {
					const ingest = yield* ReplicationIngestApi
					const consumer = yield* ingest
						.consumeReplicationSession({
							keepaliveIntervalMilliseconds: 15_000,
							initialSafeFlushLsn,
						})
						.pipe(Effect.forkChild)

					const batch = yield* Queue.take(publishedBatches)
					yield* Deferred.await(acknowledged)

					expect(batch.fromLsn).toBe(initialSafeFlushLsn)
					expect(batch.toLsn).toBe(transactionEndLsn)
					expect(batch.transaction.changes).toHaveLength(1)
					expect(yield* Ref.get(acknowledgements)).toStrictEqual([transactionEndLsn])

					yield* Fiber.interrupt(consumer)
				}).pipe(Effect.provide(makeReplicationIngestLayer(operations, changeFeed)))
			}),
		),
	)

	it.effect('does not acknowledge when the change feed rejects a transaction', ({ expect }) =>
		Effect.gen(function* () {
			const acknowledgements = yield* Ref.make<ReadonlyArray<bigint>>([])
			const changeFeed: ChangeFeedService = {
				publishCommittedChangeBatch: () => Effect.fail(new ChangeFeedUnavailable({ reason: 'unavailable' })),
				committedChangeBatches: Stream.empty,
			}
			const operations = makeReplicationOperations({
				frames: Stream.concat(Stream.fromIterable(transactionFrames), Stream.never),
				acknowledgements,
			})
			const failure = yield* Effect.gen(function* () {
				const ingest = yield* ReplicationIngestApi
				return yield* Effect.flip(
					ingest.consumeReplicationSession({
						keepaliveIntervalMilliseconds: 15_000,
						initialSafeFlushLsn,
					}),
				)
			}).pipe(Effect.provide(makeReplicationIngestLayer(operations, changeFeed)))

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'ingest-pipeline-failed' }),
			)
			expect(yield* Ref.get(acknowledgements)).toStrictEqual([])
		}),
	)
})
