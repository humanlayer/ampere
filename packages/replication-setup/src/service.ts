/** @effect-diagnostics lazyEffect:skip-file */
import { Context } from 'effect'
import type { Effect, Stream } from 'effect'

import type {
	AcquireSlotLeaseInput,
	AcknowledgeReplicationLsnInput,
	EnsureReplicationContractInput,
	EnsureReplicationSlotInput,
	ReplicationOperationFailure,
	ReplicationProtocolFrame,
	ReplicationServerInfo,
	ReplicationSlotPosition,
	ReplicationSourceIdentity,
	SlotLeaseOutcome,
	StartPgOutputInput,
	StreamReplicationFramesInput,
} from './schemas.ts'

export interface ReplicationOperationsApi {
	readonly openReplicationSession: () => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly identifySource: () => Effect.Effect<
		typeof ReplicationSourceIdentity.Type,
		typeof ReplicationOperationFailure.Type
	>
	readonly readServerInfo: () => Effect.Effect<
		typeof ReplicationServerInfo.Type,
		typeof ReplicationOperationFailure.Type
	>
	readonly acquireSlotLease: (
		input: typeof AcquireSlotLeaseInput.Type,
	) => Effect.Effect<typeof SlotLeaseOutcome.Type, typeof ReplicationOperationFailure.Type>
	readonly ensureReplicationContract: (
		input: typeof EnsureReplicationContractInput.Type,
	) => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
	readonly ensureReplicationSlot: (
		input: typeof EnsureReplicationSlotInput.Type,
	) => Effect.Effect<typeof ReplicationSlotPosition.Type, typeof ReplicationOperationFailure.Type>
	readonly pinOutputSettings: () => Effect.Effect<void, typeof ReplicationOperationFailure.Type>
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

export class ReplicationOperations extends Context.Service<ReplicationOperations, ReplicationOperationsApi>()(
	'@ampere/replication/ReplicationOperations',
) {}
