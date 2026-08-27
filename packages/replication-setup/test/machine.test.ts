import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'
import { simulate } from 'effect-machine'
import { expect } from 'vitest'

import {
	Lsn,
	ReplicationConnection,
	ReplicationEvent,
	ReplicationPlan,
	ReplicationRelation,
	ReplicationServerInfo,
	ReplicationSlotPosition,
	ReplicationSourceIdentity,
	ReplicationState,
} from '../src/machine.ts'

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

const sourceIdentified = ReplicationEvent.SourceIdentified({
	sourceIdentity: ReplicationSourceIdentity.make({
		systemIdentifier: 'ampere-test-system',
		timelineId: 1,
		databaseName: 'ampere',
		currentWalFlushLsn: Lsn.make(0n),
	}),
})

const serverInfoRead = ReplicationEvent.ServerInfoRead({
	serverInfo: ReplicationServerInfo.make({
		serverVersionNumber: 170000,
		backendProcessId: 1,
		walSenderTimeoutMilliseconds: 60_000,
		keepaliveIntervalMilliseconds: 15_000,
	}),
})

const replicationSlotReady = ReplicationEvent.ReplicationSlotReady({
	slotPosition: ReplicationSlotPosition.make({
		confirmedFlushLsn: Lsn.make(0n),
		slotWasCreated: true,
	}),
})

const replicationSlotHandlers = {
	openReplicationSession: () => Effect.void,
	identifySource: () => Effect.void,
	readServerInfo: () => Effect.void,
	acquireSlotLease: () => Effect.void,
	ensureReplicationContract: () => Effect.void,
	ensureReplicationSlot: () => Effect.void,
	pinOutputSettings: () => Effect.void,
	startPgOutput: () => Effect.void,
	consumePgOutput: () => Effect.void,
}

const successfulSetupEvents = [
	ReplicationEvent.ConnectionOpened,
	sourceIdentified,
	serverInfoRead,
	ReplicationEvent.SlotLeaseAcquired,
	ReplicationEvent.ReplicationContractEnsured,
	replicationSlotReady,
	ReplicationEvent.OutputSettingsPinned,
	ReplicationEvent.PgOutputStarted,
] as const

const getStateTag = ReplicationState.$match({
	Connecting: () => 'Connecting',
	IdentifyingSource: () => 'IdentifyingSource',
	ReadingServerInfo: () => 'ReadingServerInfo',
	AcquiringSlotLease: () => 'AcquiringSlotLease',
	WaitingToRetrySlotLease: () => 'WaitingToRetrySlotLease',
	EnsuringReplicationContract: () => 'EnsuringReplicationContract',
	EnsuringReplicationSlot: () => 'EnsuringReplicationSlot',
	PinningOutputSettings: () => 'PinningOutputSettings',
	StartingPgOutput: () => 'StartingPgOutput',
	Streaming: () => 'Streaming',
	ReconnectRequired: () => 'ReconnectRequired',
	SourceConfigurationRejected: () => 'SourceConfigurationRejected',
	Stopped: () => 'Stopped',
})

describe('ReplicationConnection', () => {
	it.effect('reaches Streaming only after every setup prerequisite succeeds', () =>
		Effect.gen(function* () {
			const result = yield* simulate(ReplicationConnection(replicationPlan), successfulSetupEvents, {
				slots: replicationSlotHandlers,
			})

			expect(result.states.map(getStateTag)).toEqual([
				'Connecting',
				'IdentifyingSource',
				'ReadingServerInfo',
				'AcquiringSlotLease',
				'EnsuringReplicationContract',
				'EnsuringReplicationSlot',
				'PinningOutputSettings',
				'StartingPgOutput',
				'Streaming',
			])
		}),
	)

	it.effect('waits with an exponential delay before incrementing the slot lease attempt', () =>
		Effect.gen(function* () {
			const result = yield* simulate(
				ReplicationConnection(replicationPlan),
				[
					ReplicationEvent.ConnectionOpened,
					sourceIdentified,
					serverInfoRead,
					ReplicationEvent.SlotLeaseWaitRequired,
					ReplicationEvent.SlotLeaseRetryElapsed,
				],
				{ slots: replicationSlotHandlers },
			)
			const waitingState = result.states.find(ReplicationState.$is('WaitingToRetrySlotLease'))

			expect(waitingState).toMatchObject({
				retry: { attempt: 1, delayMilliseconds: 100 },
			})
			expect(getStateTag(result.finalState)).toBe('AcquiringSlotLease')
			ReplicationState.$match(result.finalState, {
				Connecting: () => undefined,
				IdentifyingSource: () => undefined,
				ReadingServerInfo: () => undefined,
				AcquiringSlotLease: (state) => expect(state.leaseAttempt).toBe(2),
				WaitingToRetrySlotLease: () => undefined,
				EnsuringReplicationContract: () => undefined,
				EnsuringReplicationSlot: () => undefined,
				PinningOutputSettings: () => undefined,
				StartingPgOutput: () => undefined,
				Streaming: () => undefined,
				ReconnectRequired: () => undefined,
				SourceConfigurationRejected: () => undefined,
				Stopped: () => undefined,
			})
		}),
	)

	it.effect('records the actual active phase when a session becomes unavailable', () =>
		Effect.gen(function* () {
			const result = yield* simulate(
				ReplicationConnection(replicationPlan),
				[
					ReplicationEvent.ConnectionOpened,
					sourceIdentified,
					serverInfoRead,
					ReplicationEvent.SessionUnavailable({ reason: 'setup-command-failed' }),
				],
				{ slots: replicationSlotHandlers },
			)

			ReplicationState.$match(result.finalState, {
				Connecting: () => undefined,
				IdentifyingSource: () => undefined,
				ReadingServerInfo: () => undefined,
				AcquiringSlotLease: () => undefined,
				WaitingToRetrySlotLease: () => undefined,
				EnsuringReplicationContract: () => undefined,
				EnsuringReplicationSlot: () => undefined,
				PinningOutputSettings: () => undefined,
				StartingPgOutput: () => undefined,
				Streaming: () => undefined,
				ReconnectRequired: (state) => {
					expect(state.phase).toBe('AcquiringSlotLease')
					expect(state.reason).toBe('setup-command-failed')
				},
				SourceConfigurationRejected: () => undefined,
				Stopped: () => undefined,
			})
		}),
	)

	it.effect('stops cleanly from Streaming', () =>
		Effect.gen(function* () {
			const result = yield* simulate(
				ReplicationConnection(replicationPlan),
				[...successfulSetupEvents, ReplicationEvent.StopRequested],
				{ slots: replicationSlotHandlers },
			)

			expect(getStateTag(result.finalState)).toBe('Stopped')
		}),
	)
})
