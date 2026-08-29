import { Machine, simulate } from '@ampere/effect-machine'
import { describe, it } from '@effect/vitest'
import { Deferred, Effect, Layer, Ref } from 'effect'
import { TestClock } from 'effect/testing'
import { expect } from 'vitest'

import { makeReplicationConnection, ReplicationEvents, ReplicationStates } from '../src/machine.ts'
import {
	Lsn,
	ReplicationOperationFailure,
	ReplicationPlan,
	ReplicationRelation,
	ReplicationServerInfo,
	ReplicationSlotPosition,
	ReplicationSourceIdentity,
	SlotLeaseOutcome,
} from '../src/schemas.ts'
import { ReplicationOperations } from '../src/service.ts'
import type { ReplicationOperationsApi } from '../src/service.ts'

const replicationPlan = ReplicationPlan.make({
	slotName: 'ampere_slot',
	publicationName: 'ampere_publication',
	relations: [
		ReplicationRelation.make({
			schemaName: 'public',
			tableName: 'todos',
			partitionColumnNames: ['id'],
		}),
	],
})

const sourceIdentity = ReplicationSourceIdentity.make({
	systemIdentifier: 'ampere-test-system',
	timelineId: 1,
	databaseName: 'ampere',
	currentWalFlushLsn: Lsn.make(0n),
})

const serverInfo = ReplicationServerInfo.make({
	serverVersionNumber: 180_000,
	backendProcessId: 1,
	walSenderTimeoutMilliseconds: 60_000,
	keepaliveIntervalMilliseconds: 15_000,
})

const slotPosition = ReplicationSlotPosition.make({
	confirmedFlushLsn: Lsn.make(0n),
	slotWasCreated: true,
})

const successfulSetupEvents = [
	ReplicationEvents.ConnectionOpened,
	ReplicationEvents.SourceIdentified({ sourceIdentity }),
	ReplicationEvents.ServerInfoRead({ serverInfo }),
	ReplicationEvents.SlotLeaseAcquired,
	ReplicationEvents.ReplicationContractEnsured,
	ReplicationEvents.ReplicationSlotReady({ slotPosition }),
	ReplicationEvents.OutputSettingsPinned,
	ReplicationEvents.PgOutputStarted,
] as const

type OperationCall =
	| 'openReplicationSession'
	| 'identifySource'
	| 'readServerInfo'
	| 'acquireSlotLease'
	| 'ensureReplicationContract'
	| 'ensureReplicationSlot'
	| 'pinOutputSettings'
	| 'startPgOutput'
	| 'consumePgOutput'

interface MakeTestOperationsInput {
	readonly calls: Ref.Ref<ReadonlyArray<OperationCall>>
	readonly acquireSlotLease?: ReplicationOperationsApi['acquireSlotLease']
	readonly identifySource?: ReplicationOperationsApi['identifySource']
	readonly consumePgOutput?: ReplicationOperationsApi['consumePgOutput']
	readonly consumeStarted?: Deferred.Deferred<void>
}

const makeTestOperations = ({
	calls,
	acquireSlotLease,
	identifySource,
	consumePgOutput,
	consumeStarted,
}: MakeTestOperationsInput): ReplicationOperationsApi => {
	const recordCall = (call: OperationCall): Effect.Effect<void> =>
		Ref.update(calls, (recorded) => [...recorded, call])

	return {
		openReplicationSession: () => recordCall('openReplicationSession'),
		identifySource: () =>
			recordCall('identifySource').pipe(
				Effect.andThen(identifySource === undefined ? Effect.succeed(sourceIdentity) : identifySource()),
			),
		readServerInfo: () => recordCall('readServerInfo').pipe(Effect.as(serverInfo)),
		acquireSlotLease: (input) =>
			recordCall('acquireSlotLease').pipe(
				Effect.andThen(
					acquireSlotLease === undefined
						? Effect.succeed(SlotLeaseOutcome.cases.Acquired.make({}))
						: acquireSlotLease(input),
				),
			),
		ensureReplicationContract: () => recordCall('ensureReplicationContract'),
		ensureReplicationSlot: () => recordCall('ensureReplicationSlot').pipe(Effect.as(slotPosition)),
		pinOutputSettings: () => recordCall('pinOutputSettings'),
		startPgOutput: () => recordCall('startPgOutput'),
		consumePgOutput: (input) =>
			recordCall('consumePgOutput').pipe(
				Effect.andThen(
					consumeStarted === undefined ? Effect.void : Deferred.succeed(consumeStarted, undefined),
				),
				Effect.andThen(consumePgOutput === undefined ? Effect.never : consumePgOutput(input)),
			),
	}
}

const makeTestOperationsLayer = (operations: ReplicationOperationsApi) =>
	Layer.succeed(ReplicationOperations, operations)

describe('ReplicationConnection', () => {
	it.effect('models the complete setup path as pure transitions', () =>
		Effect.gen(function* () {
			const calls = yield* Ref.make<ReadonlyArray<OperationCall>>([])
			const machine = makeReplicationConnection({ plan: replicationPlan })
			const result = yield* simulate(machine, successfulSetupEvents).pipe(
				Effect.provide(makeTestOperationsLayer(makeTestOperations({ calls }))),
			)

			expect(result.states).toEqual([
				ReplicationStates.Connecting({ plan: replicationPlan }),
				ReplicationStates.IdentifyingSource({ plan: replicationPlan }),
				ReplicationStates.ReadingServerInfo({ plan: replicationPlan, sourceIdentity }),
				ReplicationStates.AcquiringSlotLease({
					plan: replicationPlan,
					sourceIdentity,
					serverInfo,
					leaseAttempt: 1,
				}),
				ReplicationStates.EnsuringReplicationContract({ plan: replicationPlan, sourceIdentity, serverInfo }),
				ReplicationStates.EnsuringReplicationSlot({ plan: replicationPlan, sourceIdentity, serverInfo }),
				ReplicationStates.PinningOutputSettings({
					plan: replicationPlan,
					sourceIdentity,
					serverInfo,
					slotPosition,
				}),
				ReplicationStates.StartingPgOutput({
					plan: replicationPlan,
					sourceIdentity,
					serverInfo,
					slotPosition,
				}),
				ReplicationStates.Streaming({
					plan: replicationPlan,
					sourceIdentity,
					serverInfo,
					slotPosition,
				}),
			])
			expect(yield* Ref.get(calls)).toEqual([])
		}),
	)

	it.effect('runs task and spawn operations from the provided service layer', () =>
		Effect.scoped(
			Machine.scoped(
				Effect.gen(function* () {
					const calls = yield* Ref.make<ReadonlyArray<OperationCall>>([])
					const consumeStarted = yield* Deferred.make<void>()
					const operationsLayer = makeTestOperationsLayer(makeTestOperations({ calls, consumeStarted }))
					const machine = makeReplicationConnection({ plan: replicationPlan })
					const actor = yield* Machine.spawn(machine, { id: 'successful-replication' }).pipe(
						Effect.provide(operationsLayer),
					)

					// The layer only wraps spawn. actor.start uses the service context captured by Machine.spawn.
					yield* actor.start
					const streamingState = yield* actor.waitFor(ReplicationStates.Streaming)
					yield* Deferred.await(consumeStarted)

					expect(ReplicationStates.$is('Streaming')(streamingState)).toBe(true)
					expect(yield* Ref.get(calls)).toEqual([
						'openReplicationSession',
						'identifySource',
						'readServerInfo',
						'acquireSlotLease',
						'ensureReplicationContract',
						'ensureReplicationSlot',
						'pinOutputSettings',
						'startPgOutput',
						'consumePgOutput',
					])
				}),
			),
		),
	)

	it.effect('uses the test clock for slot lease retry tasks', () =>
		Effect.scoped(
			Machine.scoped(
				Effect.gen(function* () {
					const calls = yield* Ref.make<ReadonlyArray<OperationCall>>([])
					const leaseAttempts = yield* Ref.make(0)
					const acquireSlotLease: ReplicationOperationsApi['acquireSlotLease'] = () =>
						Ref.updateAndGet(leaseAttempts, (attempt) => attempt + 1).pipe(
							Effect.map((attempt) =>
								attempt === 1
									? SlotLeaseOutcome.cases.WaitRequired.make({})
									: SlotLeaseOutcome.cases.Acquired.make({}),
							),
						)
					const operationsLayer = makeTestOperationsLayer(makeTestOperations({ calls, acquireSlotLease }))
					const actor = yield* Machine.spawn(makeReplicationConnection({ plan: replicationPlan }), {
						id: 'retrying-replication',
					}).pipe(Effect.provide(operationsLayer))
					yield* actor.start

					const waitingState = yield* actor.waitFor(ReplicationStates.WaitingToRetrySlotLease)
					expect(ReplicationStates.$is('WaitingToRetrySlotLease')(waitingState)).toBe(true)
					if (ReplicationStates.$is('WaitingToRetrySlotLease')(waitingState)) {
						expect(waitingState.retry.attempt).toBe(1)
						expect(waitingState.retry.delayMilliseconds).toBe(100)
					}

					yield* TestClock.adjust('100 millis')
					const streamingState = yield* actor.waitFor(ReplicationStates.Streaming)

					expect(ReplicationStates.$is('Streaming')(streamingState)).toBe(true)
					expect(yield* Ref.get(leaseAttempts)).toBe(2)
				}),
			),
		),
	)

	it.effect('maps task failures to the active terminal phase', () =>
		Effect.scoped(
			Machine.scoped(
				Effect.gen(function* () {
					const calls = yield* Ref.make<ReadonlyArray<OperationCall>>([])
					const identifySource: ReplicationOperationsApi['identifySource'] = () =>
						Effect.fail(
							ReplicationOperationFailure.cases.SourceRejected.make({
								reason: 'replication-prerequisite-invalid',
							}),
						)
					const operationsLayer = makeTestOperationsLayer(makeTestOperations({ calls, identifySource }))
					const actor = yield* Machine.spawn(makeReplicationConnection({ plan: replicationPlan }), {
						id: 'rejected-replication',
					}).pipe(Effect.provide(operationsLayer))
					yield* actor.start

					const rejectedState = yield* actor.awaitFinal

					expect(ReplicationStates.$is('SourceConfigurationRejected')(rejectedState)).toBe(true)
					if (ReplicationStates.$is('SourceConfigurationRejected')(rejectedState)) {
						expect(rejectedState.phase).toBe('IdentifyingSource')
						expect(rejectedState.reason).toBe('replication-prerequisite-invalid')
					}
				}),
			),
		),
	)

	it.effect('maps a streaming spawn failure to reconnect required', () =>
		Effect.scoped(
			Machine.scoped(
				Effect.gen(function* () {
					const calls = yield* Ref.make<ReadonlyArray<OperationCall>>([])
					const consumePgOutput: ReplicationOperationsApi['consumePgOutput'] = () =>
						Effect.fail(
							ReplicationOperationFailure.cases.SessionUnavailable.make({ reason: 'connection-closed' }),
						)
					const operationsLayer = makeTestOperationsLayer(makeTestOperations({ calls, consumePgOutput }))
					const actor = yield* Machine.spawn(makeReplicationConnection({ plan: replicationPlan }), {
						id: 'disconnected-replication',
					}).pipe(Effect.provide(operationsLayer))
					yield* actor.start

					const reconnectState = yield* actor.awaitFinal

					expect(ReplicationStates.$is('ReconnectRequired')(reconnectState)).toBe(true)
					if (ReplicationStates.$is('ReconnectRequired')(reconnectState)) {
						expect(reconnectState.phase).toBe('Streaming')
						expect(reconnectState.reason).toBe('connection-closed')
					}
				}),
			),
		),
	)
})
