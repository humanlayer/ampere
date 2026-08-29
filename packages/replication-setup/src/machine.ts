import { Event, Machine, State } from '@humanlayer/effect-machine'
import { Cause, Effect, Match, Option } from 'effect'

import {
	ConnectionPhaseId,
	ReplicationActivity,
	ReplicationEventFields,
	ReplicationStateFields,
	SlotLeaseRetry,
} from './schemas.ts'
import type { ReplicationOperationFailure, ReplicationPlan } from './schemas.ts'
import { ReplicationOperations } from './service.ts'

export const ReplicationStates = State(ReplicationStateFields)
export const ReplicationEvents = Event(ReplicationEventFields)

const calculateSlotLeaseRetryDelayMilliseconds = (attempt: number): number => Math.min(100 * 2 ** (attempt - 1), 5_000)

const operationFailureToEvent = (failure: typeof ReplicationOperationFailure.Type): typeof ReplicationEvents.Type =>
	Match.value(failure).pipe(
		Match.tag('SessionUnavailable', ({ reason }) => ReplicationEvents.SessionUnavailable({ reason })),
		Match.tag('SourceRejected', ({ reason }) => ReplicationEvents.SourceRejected({ reason })),
		Match.exhaustive,
	)

const operationFailureCauseToEvent = (
	cause: Cause.Cause<typeof ReplicationOperationFailure.Type>,
): typeof ReplicationEvents.Type => {
	const failure = Cause.findErrorOption(cause)
	if (Option.isNone(failure)) {
		throw Cause.squash(cause)
	}
	return operationFailureToEvent(failure.value)
}

export interface MakeReplicationConnectionInput {
	readonly plan: typeof ReplicationPlan.Type
}

export const makeReplicationConnection = ({ plan }: MakeReplicationConnectionInput) =>
	Machine.make({
		state: ReplicationStates,
		event: ReplicationEvents,
		initial: ReplicationStates.Connecting({ plan }),
	})
		.task(
			ReplicationStates.Connecting,
			() => ReplicationOperations.use((operations) => operations.openReplicationSession()),
			{
				name: ReplicationActivity.enums.OpenReplicationSession,
				onSuccess: () => ReplicationEvents.ConnectionOpened,
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.Connecting, ReplicationEvents.ConnectionOpened, ({ state }) =>
			ReplicationStates.IdentifyingSource.with(state),
		)
		.on(ReplicationStates.Connecting, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({ phase: ConnectionPhaseId.Connecting, reason: event.reason }),
		)
		.on(ReplicationStates.Connecting, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.Connecting,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.Connecting, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.IdentifyingSource,
			() => ReplicationOperations.use((operations) => operations.identifySource()),
			{
				name: ReplicationActivity.enums.IdentifySource,
				onSuccess: (sourceIdentity) => ReplicationEvents.SourceIdentified({ sourceIdentity }),
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.IdentifyingSource, ReplicationEvents.SourceIdentified, ({ event, state }) =>
			ReplicationStates.ReadingServerInfo.with(state, { sourceIdentity: event.sourceIdentity }),
		)
		.on(ReplicationStates.IdentifyingSource, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.IdentifyingSource,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.IdentifyingSource, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.IdentifyingSource,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.IdentifyingSource, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.ReadingServerInfo,
			() => ReplicationOperations.use((operations) => operations.readServerInfo()),
			{
				name: ReplicationActivity.enums.ReadServerInfo,
				onSuccess: (serverInfo) => ReplicationEvents.ServerInfoRead({ serverInfo }),
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.ReadingServerInfo, ReplicationEvents.ServerInfoRead, ({ event, state }) =>
			ReplicationStates.AcquiringSlotLease.with(state, {
				serverInfo: event.serverInfo,
				leaseAttempt: 1,
			}),
		)
		.on(ReplicationStates.ReadingServerInfo, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.ReadingServerInfo,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.ReadingServerInfo, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.ReadingServerInfo,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.ReadingServerInfo, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.AcquiringSlotLease,
			({ state }) =>
				ReplicationOperations.use((operations) =>
					operations.acquireSlotLease({ slotName: state.plan.slotName }),
				),
			{
				name: ReplicationActivity.enums.AcquireSlotLease,
				onSuccess: (outcome) =>
					Match.value(outcome).pipe(
						Match.tag('Acquired', () => ReplicationEvents.SlotLeaseAcquired),
						Match.tag('WaitRequired', () => ReplicationEvents.SlotLeaseWaitRequired),
						Match.exhaustive,
					),
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.AcquiringSlotLease, ReplicationEvents.SlotLeaseAcquired, ({ state }) =>
			ReplicationStates.EnsuringReplicationContract.with(state),
		)
		.on(ReplicationStates.AcquiringSlotLease, ReplicationEvents.SlotLeaseWaitRequired, ({ state }) =>
			ReplicationStates.WaitingToRetrySlotLease.with(state, {
				retry: SlotLeaseRetry.make({
					attempt: state.leaseAttempt,
					delayMilliseconds: calculateSlotLeaseRetryDelayMilliseconds(state.leaseAttempt),
				}),
			}),
		)
		.on(ReplicationStates.AcquiringSlotLease, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.AcquiringSlotLease,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.AcquiringSlotLease, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.AcquiringSlotLease,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.AcquiringSlotLease, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.WaitingToRetrySlotLease,
			({ state }) =>
				Effect.sleep(state.retry.delayMilliseconds).pipe(Effect.as(ReplicationEvents.SlotLeaseRetryElapsed)),
			{ name: ReplicationActivity.enums.WaitToRetrySlotLease },
		)
		.on(ReplicationStates.WaitingToRetrySlotLease, ReplicationEvents.SlotLeaseRetryElapsed, ({ state }) =>
			ReplicationStates.AcquiringSlotLease.with(state, { leaseAttempt: state.retry.attempt + 1 }),
		)
		.on(ReplicationStates.WaitingToRetrySlotLease, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.WaitingToRetrySlotLease,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.WaitingToRetrySlotLease, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.EnsuringReplicationContract,
			({ state }) =>
				ReplicationOperations.use((operations) =>
					operations.ensureReplicationContract({
						publicationName: state.plan.publicationName,
						relations: state.plan.relations,
						serverVersionNumber: state.serverInfo.serverVersionNumber,
					}),
				),
			{
				name: ReplicationActivity.enums.EnsureReplicationContract,
				onSuccess: () => ReplicationEvents.ReplicationContractEnsured,
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.EnsuringReplicationContract, ReplicationEvents.ReplicationContractEnsured, ({ state }) =>
			ReplicationStates.EnsuringReplicationSlot.with(state),
		)
		.on(ReplicationStates.EnsuringReplicationContract, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.EnsuringReplicationContract,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.EnsuringReplicationContract, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.EnsuringReplicationContract,
				reason: event.reason,
			}),
		)
		.on(
			ReplicationStates.EnsuringReplicationContract,
			ReplicationEvents.StopRequested,
			() => ReplicationStates.Stopped,
		)
		.task(
			ReplicationStates.EnsuringReplicationSlot,
			({ state }) =>
				ReplicationOperations.use((operations) =>
					operations.ensureReplicationSlot({ slotName: state.plan.slotName }),
				),
			{
				name: ReplicationActivity.enums.EnsureReplicationSlot,
				onSuccess: (slotPosition) => ReplicationEvents.ReplicationSlotReady({ slotPosition }),
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.EnsuringReplicationSlot, ReplicationEvents.ReplicationSlotReady, ({ event, state }) =>
			ReplicationStates.PinningOutputSettings.with(state, { slotPosition: event.slotPosition }),
		)
		.on(ReplicationStates.EnsuringReplicationSlot, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.EnsuringReplicationSlot,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.EnsuringReplicationSlot, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.EnsuringReplicationSlot,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.EnsuringReplicationSlot, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.PinningOutputSettings,
			() => ReplicationOperations.use((operations) => operations.pinOutputSettings()),
			{
				name: ReplicationActivity.enums.PinOutputSettings,
				onSuccess: () => ReplicationEvents.OutputSettingsPinned,
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.PinningOutputSettings, ReplicationEvents.OutputSettingsPinned, ({ state }) =>
			ReplicationStates.StartingPgOutput.with(state),
		)
		.on(ReplicationStates.PinningOutputSettings, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.PinningOutputSettings,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.PinningOutputSettings, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.PinningOutputSettings,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.PinningOutputSettings, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.task(
			ReplicationStates.StartingPgOutput,
			({ state }) =>
				ReplicationOperations.use((operations) =>
					operations.startPgOutput({
						slotName: state.plan.slotName,
						publicationName: state.plan.publicationName,
					}),
				),
			{
				name: ReplicationActivity.enums.StartPgOutput,
				onSuccess: () => ReplicationEvents.PgOutputStarted,
				onFailure: operationFailureCauseToEvent,
			},
		)
		.on(ReplicationStates.StartingPgOutput, ReplicationEvents.PgOutputStarted, ({ state }) =>
			ReplicationStates.Streaming.with(state),
		)
		.on(ReplicationStates.StartingPgOutput, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({
				phase: ConnectionPhaseId.StartingPgOutput,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.StartingPgOutput, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({
				phase: ConnectionPhaseId.StartingPgOutput,
				reason: event.reason,
			}),
		)
		.on(ReplicationStates.StartingPgOutput, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.spawn(ReplicationStates.Streaming, ({ self, state }) =>
			ReplicationOperations.use((operations) =>
				operations
					.consumePgOutput({
						keepaliveIntervalMilliseconds: state.serverInfo.keepaliveIntervalMilliseconds,
						initialSafeFlushLsn: state.slotPosition.confirmedFlushLsn,
					})
					.pipe(Effect.catch((failure) => self.send(operationFailureToEvent(failure)))),
			),
		)
		.on(ReplicationStates.Streaming, ReplicationEvents.SessionUnavailable, ({ event }) =>
			ReplicationStates.ReconnectRequired({ phase: ConnectionPhaseId.Streaming, reason: event.reason }),
		)
		.on(ReplicationStates.Streaming, ReplicationEvents.SourceRejected, ({ event }) =>
			ReplicationStates.SourceConfigurationRejected({ phase: ConnectionPhaseId.Streaming, reason: event.reason }),
		)
		.on(ReplicationStates.Streaming, ReplicationEvents.StopRequested, () => ReplicationStates.Stopped)
		.final(ReplicationStates.ReconnectRequired)
		.final(ReplicationStates.SourceConfigurationRejected)
		.final(ReplicationStates.Stopped)

export type ReplicationConnection = ReturnType<typeof makeReplicationConnection>
