import { Machine } from '@typeonce/effect-machine'
import { Match } from 'effect'

import {
	ConnectionPhaseId,
	ReplicationActivity,
	ReplicationEventSchema,
	ReplicationPlan,
	ReplicationStateSchema,
	SlotLeaseRetry,
} from './schemas.ts'
import type { ReconnectReason, ReplicationOperationFailure, SourceRejectionReason } from './schemas.ts'
import { ReplicationOperations } from './service.ts'

export const ReplicationStates = Machine.states({
	Connecting: ReplicationStateSchema.cases.Connecting,
	IdentifyingSource: ReplicationStateSchema.cases.IdentifyingSource,
	ReadingServerInfo: ReplicationStateSchema.cases.ReadingServerInfo,
	AcquiringSlotLease: ReplicationStateSchema.cases.AcquiringSlotLease,
	WaitingToRetrySlotLease: ReplicationStateSchema.cases.WaitingToRetrySlotLease,
	EnsuringReplicationContract: ReplicationStateSchema.cases.EnsuringReplicationContract,
	EnsuringReplicationSlot: ReplicationStateSchema.cases.EnsuringReplicationSlot,
	PinningOutputSettings: ReplicationStateSchema.cases.PinningOutputSettings,
	StartingPgOutput: ReplicationStateSchema.cases.StartingPgOutput,
	Streaming: ReplicationStateSchema.cases.Streaming,
	ReconnectRequired: { schema: ReplicationStateSchema.cases.ReconnectRequired, type: 'final' },
	SourceConfigurationRejected: { schema: ReplicationStateSchema.cases.SourceConfigurationRejected, type: 'final' },
	Stopped: { schema: ReplicationStateSchema.cases.Stopped, type: 'final' },
})

export const ReplicationEvents = Machine.events(ReplicationEventSchema)

const calculateSlotLeaseRetryDelayMilliseconds = (attempt: number): number => Math.min(100 * 2 ** (attempt - 1), 5_000)

const raiseOperationFailure = (
	failure: typeof ReplicationOperationFailure.Type,
	handlers: {
		readonly sessionUnavailable: (reason: typeof ReconnectReason.Type) => void
		readonly sourceRejected: (reason: typeof SourceRejectionReason.Type) => void
	},
): void =>
	Match.value(failure).pipe(
		Match.tag('SessionUnavailable', ({ reason }) => handlers.sessionUnavailable(reason)),
		Match.tag('SourceRejected', ({ reason }) => handlers.sourceRejected(reason)),
		Match.exhaustive,
	)

export const ReplicationConnection = Machine.make({
	id: 'ReplicationConnection',
	states: ReplicationStates.states,
	events: ReplicationEvents,
	input: ReplicationPlan,
	initial: (to) =>
		to
			.Connecting()
			.resolve(({ input, target }) =>
				target.decoded(ReplicationStateSchema.cases.Connecting.make({ plan: input })),
			),
}).handle({
	Connecting: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.OpenReplicationSession, () =>
					ReplicationOperations.use((operations) => operations.openReplicationSession()),
				)
				.onDone((to) =>
					to.none.resolve((_context, enqueue) => {
						enqueue.raise(ReplicationEvents.ConnectionOpened())
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			ConnectionOpened: (to) =>
				to.full.IdentifyingSource().resolve(({ state, target }) => target.from({ plan: state.plan })),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.Connecting, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	IdentifyingSource: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.IdentifySource, () =>
					ReplicationOperations.use((operations) => operations.identifySource()),
				)
				.onDone((to) =>
					to.none.resolve(({ output }, enqueue) => {
						enqueue.raise(ReplicationEvents.SourceIdentified({ sourceIdentity: output }))
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			SourceIdentified: (to) =>
				to.full
					.ReadingServerInfo()
					.resolve(({ event, state, target }) =>
						target.from({ plan: state.plan, sourceIdentity: event.sourceIdentity }),
					),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.IdentifyingSource, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	ReadingServerInfo: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.ReadServerInfo, () =>
					ReplicationOperations.use((operations) => operations.readServerInfo()),
				)
				.onDone((to) =>
					to.none.resolve(({ output }, enqueue) => {
						enqueue.raise(ReplicationEvents.ServerInfoRead({ serverInfo: output }))
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			ServerInfoRead: (to) =>
				to.full.AcquiringSlotLease().resolve(({ event, state, target }) =>
					target.from({
						leaseAttempt: 1,
						plan: state.plan,
						sourceIdentity: state.sourceIdentity,
						serverInfo: event.serverInfo,
					}),
				),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.ReadingServerInfo, reason: event.reason }),
					),
			SourceRejected: (to) =>
				to.full
					.SourceConfigurationRejected()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.ReadingServerInfo, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	AcquiringSlotLease: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.AcquireSlotLease, ({ state }) =>
					ReplicationOperations.use((operations) =>
						operations.acquireSlotLease({ slotName: state.plan.slotName }),
					),
				)
				.onDone((to) =>
					to.none.resolve(({ output }, enqueue) => {
						Match.value(output).pipe(
							Match.tag('Acquired', () => enqueue.raise(ReplicationEvents.SlotLeaseAcquired())),
							Match.tag('WaitRequired', () => enqueue.raise(ReplicationEvents.SlotLeaseWaitRequired())),
							Match.exhaustive,
						)
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			SlotLeaseAcquired: (to) =>
				to.full.EnsuringReplicationContract().resolve(({ state, target }) =>
					target.from({
						plan: state.plan,
						sourceIdentity: state.sourceIdentity,
						serverInfo: state.serverInfo,
					}),
				),
			SlotLeaseWaitRequired: (to) =>
				to.full.WaitingToRetrySlotLease().resolve(({ state, target }) =>
					target.from({
						plan: state.plan,
						sourceIdentity: state.sourceIdentity,
						serverInfo: state.serverInfo,
						retry: SlotLeaseRetry.make({
							attempt: state.leaseAttempt,
							delayMilliseconds: calculateSlotLeaseRetryDelayMilliseconds(state.leaseAttempt),
						}),
					}),
				),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.AcquiringSlotLease, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	WaitingToRetrySlotLease: {
		invoke: (from) =>
			from
				.timer(ReplicationActivity.enums.WaitToRetrySlotLease, ({ state }) => state.retry.delayMilliseconds)
				.onDone((to) =>
					to.none.resolve((_context, enqueue) => {
						enqueue.raise(ReplicationEvents.SlotLeaseRetryElapsed())
					}),
				),
		on: {
			SlotLeaseRetryElapsed: (to) =>
				to.full.AcquiringSlotLease().resolve(({ state, target }) =>
					target.from({
						plan: state.plan,
						sourceIdentity: state.sourceIdentity,
						serverInfo: state.serverInfo,
						leaseAttempt: state.retry.attempt + 1,
					}),
				),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.WaitingToRetrySlotLease, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	EnsuringReplicationContract: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.EnsureReplicationContract, ({ state }) =>
					ReplicationOperations.use((operations) =>
						operations.ensureReplicationContract({
							publicationName: state.plan.publicationName,
							relations: state.plan.relations,
							serverVersionNumber: state.serverInfo.serverVersionNumber,
						}),
					),
				)
				.onDone((to) =>
					to.none.resolve((_context, enqueue) => {
						enqueue.raise(ReplicationEvents.ReplicationContractEnsured())
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			ReplicationContractEnsured: (to) =>
				to.full.EnsuringReplicationSlot().resolve(({ state, target }) =>
					target.from({
						plan: state.plan,
						sourceIdentity: state.sourceIdentity,
						serverInfo: state.serverInfo,
					}),
				),
			SessionUnavailable: (to) =>
				to.full.ReconnectRequired().resolve(({ event, target }) =>
					target.from({
						phase: ConnectionPhaseId.EnsuringReplicationContract,
						reason: event.reason,
					}),
				),
			SourceRejected: (to) =>
				to.full.SourceConfigurationRejected().resolve(({ event, target }) =>
					target.from({
						phase: ConnectionPhaseId.EnsuringReplicationContract,
						reason: event.reason,
					}),
				),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	EnsuringReplicationSlot: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.EnsureReplicationSlot, ({ state }) =>
					ReplicationOperations.use((operations) =>
						operations.ensureReplicationSlot({ slotName: state.plan.slotName }),
					),
				)
				.onDone((to) =>
					to.none.resolve(({ output }, enqueue) => {
						enqueue.raise(ReplicationEvents.ReplicationSlotReady({ slotPosition: output }))
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			ReplicationSlotReady: (to) =>
				to.full.PinningOutputSettings().resolve(({ event, state, target }) =>
					target.from({
						plan: state.plan,
						serverInfo: state.serverInfo,
						sourceIdentity: state.sourceIdentity,
						slotPosition: event.slotPosition,
					}),
				),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.EnsuringReplicationSlot, reason: event.reason }),
					),
			SourceRejected: (to) =>
				to.full
					.SourceConfigurationRejected()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.EnsuringReplicationSlot, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	PinningOutputSettings: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.PinOutputSettings, () =>
					ReplicationOperations.use((operations) => operations.pinOutputSettings()),
				)
				.onDone((to) =>
					to.none.resolve((_context, enqueue) => {
						enqueue.raise(ReplicationEvents.OutputSettingsPinned())
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			OutputSettingsPinned: (to) =>
				to.full.StartingPgOutput().resolve(({ state, target }) =>
					target.from({
						plan: state.plan,
						serverInfo: state.serverInfo,
						slotPosition: state.slotPosition,
						sourceIdentity: state.sourceIdentity,
					}),
				),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.PinningOutputSettings, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	StartingPgOutput: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.StartPgOutput, ({ state }) =>
					ReplicationOperations.use((operations) =>
						operations.startPgOutput({
							slotName: state.plan.slotName,
							publicationName: state.plan.publicationName,
						}),
					),
				)
				.onDone((to) =>
					to.none.resolve((_context, enqueue) => {
						enqueue.raise(ReplicationEvents.PgOutputStarted())
					}),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			PgOutputStarted: (to) =>
				to.full.Streaming().resolve(({ state, target }) =>
					target.from({
						slotPosition: state.slotPosition,
						serverInfo: state.serverInfo,
						sourceIdentity: state.sourceIdentity,
						plan: state.plan,
					}),
				),
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.StartingPgOutput, reason: event.reason }),
					),
			SourceRejected: (to) =>
				to.full
					.SourceConfigurationRejected()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.StartingPgOutput, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	Streaming: {
		invoke: (from) =>
			from
				.effect(ReplicationActivity.enums.ConsumePgOutput, ({ state }) =>
					ReplicationOperations.use((operations) =>
						operations.consumePgOutput({
							keepaliveIntervalMilliseconds: state.serverInfo.keepaliveIntervalMilliseconds,
							initialSafeFlushLsn: state.slotPosition.confirmedFlushLsn,
						}),
					),
				)
				.onFailure((to) =>
					to.none.resolve(({ error }, enqueue) => {
						raiseOperationFailure(error, {
							sessionUnavailable: (reason) =>
								enqueue.raise(ReplicationEvents.SessionUnavailable({ reason })),
							sourceRejected: (reason) => enqueue.raise(ReplicationEvents.SourceRejected({ reason })),
						})
					}),
				),
		on: {
			SessionUnavailable: (to) =>
				to.full
					.ReconnectRequired()
					.resolve(({ event, target }) =>
						target.from({ phase: ConnectionPhaseId.Streaming, reason: event.reason }),
					),
			StopRequested: (to) => to.full.Stopped().resolve(({ target }) => target.from({})),
		},
	},
	ReconnectRequired: {},
	SourceConfigurationRejected: {},
	Stopped: {},
})
