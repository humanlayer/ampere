import { Machine } from '@typeonce/effect-machine'
import { Context, Match, Schema } from 'effect'
import type { Effect } from 'effect'

import {
	ReconnectReason,
	ReplicationEventFields,
	ReplicationPlan,
	ReplicationStateFields,
	SlotLeaseRetry,
	SourceRejectionReason,
} from './schemas.ts'
import type {
	PostgresLsnValue,
	ReplicationServerInfo,
	ReplicationSlotPosition,
	ReplicationSourceIdentity,
} from './schemas.ts'

export {
	Lsn,
	ReplicationPlan,
	ReplicationRelation,
	ReplicationServerInfo,
	ReplicationSlotPosition,
	ReplicationSourceIdentity,
} from './schemas.ts'

const ReplicationStateSchema = Schema.TaggedUnion(ReplicationStateFields)
const ReplicationEventSchema = Schema.TaggedUnion(ReplicationEventFields)

const SlotLeaseOutcome = Schema.TaggedUnion({
	Acquired: {},
	WaitRequired: {},
})

const ReplicationOperationFailure = Schema.TaggedUnion({
	SessionUnavailable: { reason: ReconnectReason },
	SourceRejected: { reason: SourceRejectionReason },
})

enum ReplicationActivityId {
	OpenReplicationSession = 'replication.open_session',
	IdentifySource = 'replication.identify_source',
	ReadServerInfo = 'replication.read_server_info',
	AcquireSlotLease = 'replication.acquire_slot_lease',
	WaitToRetrySlotLease = 'replication.slot_lease_retry',
	EnsureReplicationContract = 'replication.ensure_contract',
	EnsureReplicationSlot = 'replication.ensure_slot',
	PinOutputSettings = 'replication.pin_output_settings',
	StartPgOutput = 'replication.start_pgoutput',
	ConsumePgOutput = 'replication.consume_pgoutput',
}

const ReplicationActivity = Schema.Enum(ReplicationActivityId)

enum ConnectionPhaseId {
	Connecting = 'Connecting',
	IdentifyingSource = 'IdentifyingSource',
	ReadingServerInfo = 'ReadingServerInfo',
	AcquiringSlotLease = 'AcquiringSlotLease',
	WaitingToRetrySlotLease = 'WaitingToRetrySlotLease',
	EnsuringReplicationContract = 'EnsuringReplicationContract',
	EnsuringReplicationSlot = 'EnsuringReplicationSlot',
	PinningOutputSettings = 'PinningOutputSettings',
	StartingPgOutput = 'StartingPgOutput',
	Streaming = 'Streaming',
}

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

export interface ReplicationOperationsApi {
	readonly openReplicationSession: Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly identifySource: Effect.Effect<
		typeof ReplicationSourceIdentity.Type,
		typeof ReplicationOperationFailure.Type
	>
	readonly readServerInfo: Effect.Effect<typeof ReplicationServerInfo.Type, typeof ReplicationOperationFailure.Type>
	readonly acquireSlotLease: (input: {
		readonly slotName: string
	}) => Effect.Effect<typeof SlotLeaseOutcome.Type, typeof ReplicationOperationFailure.Type>
	readonly ensureReplicationContract: (input: {
		readonly publicationName: string
		readonly relations: typeof ReplicationPlan.Type.relations
		readonly serverVersionNumber: number
	}) => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly ensureReplicationSlot: (input: {
		readonly slotName: string
	}) => Effect.Effect<typeof ReplicationSlotPosition.Type, typeof ReplicationOperationFailure.Type>
	readonly pinOutputSettings: Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly startPgOutput: (input: {
		readonly slotName: string
		readonly publicationName: string
	}) => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly consumePgOutput: (input: {
		readonly keepaliveIntervalMilliseconds: number
		readonly initialSafeFlushLsn: typeof PostgresLsnValue.Type
	}) => Effect.Effect<never, typeof ReplicationOperationFailure.Type>
}

export class ReplicationOperations extends Context.Service<ReplicationOperations, ReplicationOperationsApi>()(
	'@ampere/replication/ReplicationOperations',
) {}

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
					ReplicationOperations.use((operations) => operations.openReplicationSession),
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
					ReplicationOperations.use((operations) => operations.identifySource),
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
					ReplicationOperations.use((operations) => operations.readServerInfo),
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
					ReplicationOperations.use((operations) => operations.pinOutputSettings),
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
