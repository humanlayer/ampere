import { describe, it } from '@effect/vitest'
import { MachineTest } from '@typeonce/effect-machine/testing'
import { Effect, Option, Schema } from 'effect'
import { expect } from 'vitest'

import { ReplicationConnection, ReplicationStates } from '../src/machine.ts'
import {
	Lsn,
	ReplicationEventFields,
	ReplicationPlan,
	ReplicationRelation,
	ReplicationServerInfo,
	ReplicationSlotPosition,
	ReplicationSourceIdentity,
} from '../src/schemas.ts'

const ReplicationEvent = Schema.TaggedUnion(ReplicationEventFields)

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

const sourceIdentified = ReplicationEvent.cases.SourceIdentified.make({
	sourceIdentity: ReplicationSourceIdentity.make({
		systemIdentifier: 'ampere-test-system',
		timelineId: 1,
		databaseName: 'ampere',
		currentWalFlushLsn: Lsn.make(0n),
	}),
})

const serverInfoRead = ReplicationEvent.cases.ServerInfoRead.make({
	serverInfo: ReplicationServerInfo.make({
		serverVersionNumber: 180000,
		backendProcessId: 1,
		walSenderTimeoutMilliseconds: 60_000,
		keepaliveIntervalMilliseconds: 15_000,
	}),
})

const replicationSlotReady = ReplicationEvent.cases.ReplicationSlotReady.make({
	slotPosition: ReplicationSlotPosition.make({
		confirmedFlushLsn: Lsn.make(0n),
		slotWasCreated: true,
	}),
})

const successfulSetupEvents = [
	ReplicationEvent.cases.ConnectionOpened.make({}),
	sourceIdentified,
	serverInfoRead,
	ReplicationEvent.cases.SlotLeaseAcquired.make({}),
	ReplicationEvent.cases.ReplicationContractEnsured.make({}),
	replicationSlotReady,
	ReplicationEvent.cases.OutputSettingsPinned.make({}),
	ReplicationEvent.cases.PgOutputStarted.make({}),
] as const

describe('ReplicationConnection', () => {
	it.effect('reaches Streaming only after every setup prerequisite succeeds', () =>
		Effect.gen(function* () {
			const trace = yield* MachineTest.run(ReplicationConnection, {
				input: replicationPlan,
				events: successfulSetupEvents,
			})
			const configurations = [trace.initial.configuration, ...trace.steps.map((step) => step.afterConfiguration)]

			expect(configurations).toEqual([
				['Connecting'],
				['IdentifyingSource'],
				['ReadingServerInfo'],
				['AcquiringSlotLease'],
				['EnsuringReplicationContract'],
				['EnsuringReplicationSlot'],
				['PinningOutputSettings'],
				['StartingPgOutput'],
				['Streaming'],
			])
		}),
	)

	it.effect('waits with an exponential delay before incrementing the slot lease attempt', () =>
		Effect.gen(function* () {
			const trace = yield* MachineTest.run(ReplicationConnection, {
				input: replicationPlan,
				events: [
					ReplicationEvent.cases.ConnectionOpened.make({}),
					sourceIdentified,
					serverInfoRead,
					ReplicationEvent.cases.SlotLeaseWaitRequired.make({}),
					ReplicationEvent.cases.SlotLeaseRetryElapsed.make({}),
				],
			})
			const waitingSnapshot = trace.steps
				.map((step) => step.after)
				.find((snapshot) => ReplicationStates.matches(snapshot, 'WaitingToRetrySlotLease'))

			expect(waitingSnapshot).toBeDefined()
			const waitingState = Option.getOrThrow(
				ReplicationStates.get(waitingSnapshot ?? trace.final, 'WaitingToRetrySlotLease'),
			)
			expect(waitingState.retry.attempt).toBe(1)
			expect(waitingState.retry.delayMilliseconds).toBe(100)

			expect(ReplicationStates.matches(trace.final, 'AcquiringSlotLease')).toBe(true)
			const finalState = Option.getOrThrow(ReplicationStates.get(trace.final, 'AcquiringSlotLease'))
			expect(finalState.leaseAttempt).toBe(2)
		}),
	)

	it.effect('records the actual active phase when a session becomes unavailable', () =>
		Effect.gen(function* () {
			const trace = yield* MachineTest.run(ReplicationConnection, {
				input: replicationPlan,
				events: [
					ReplicationEvent.cases.ConnectionOpened.make({}),
					sourceIdentified,
					serverInfoRead,
					ReplicationEvent.cases.SessionUnavailable.make({ reason: 'setup-command-failed' }),
				],
			})
			const finalState = Option.getOrThrow(ReplicationStates.get(trace.final, 'ReconnectRequired'))

			expect(finalState.phase).toBe('AcquiringSlotLease')
			expect(finalState.reason).toBe('setup-command-failed')
		}),
	)

	it.effect('stops cleanly from Streaming', () =>
		Effect.gen(function* () {
			const trace = yield* MachineTest.run(ReplicationConnection, {
				input: replicationPlan,
				events: [...successfulSetupEvents, ReplicationEvent.cases.StopRequested.make({})],
			})

			expect(ReplicationStates.matches(trace.final, 'Stopped')).toBe(true)
		}),
	)
})
