import { Schema, SchemaGetter } from 'effect'
import { Event, Machine, Slot, State } from 'effect-machine'

/**
 * Postgres WAL LSN - Log Sequence Number - is a uint64
 */
const maximumPostgresLsn = 0xffff_ffff_ffff_ffffn

/**
 * We have a custom type for the LSN and a codec for encode/decode based on how postgres represents it (`/`-delimited)
 */
const PostgresLsnValue = Schema.BigInt.pipe(
	Schema.check(Schema.isGreaterThanOrEqualToBigInt(0n), Schema.isLessThanOrEqualToBigInt(maximumPostgresLsn)),
	Schema.brand('PostgresLsn'),
)

const parsePostgresLsnText = (text: string): bigint => {
	const separatorIndex = text.indexOf('/')
	const segment = BigInt(`0x${text.slice(0, separatorIndex)}`)
	const offset = BigInt(`0x${text.slice(separatorIndex + 1)}`)
	return (segment << 32n) | offset
}

const formatPostgresLsnText = (lsn: bigint): string => {
	const segment = (lsn >> 32n).toString(16).toUpperCase()
	const offset = (lsn & 0xffff_ffffn).toString(16).toUpperCase()
	return `${segment}/${offset}`
}

const Lsn = Schema.String.pipe(
	Schema.check(Schema.isPattern(/^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/)),
	Schema.decodeTo(PostgresLsnValue, {
		decode: SchemaGetter.transform(parsePostgresLsnText),
		encode: SchemaGetter.transform(formatPostgresLsnText),
	}),
)

const PostgresIdentifier = Schema.NonEmptyString
const NonNegativeInteger = Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)))
const PositiveInteger = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)))

/**
 * Defines a relation that is being replicated - schema (public?) + table + partition columns
 */
const ReplicationRelation = Schema.TaggedStruct('ReplicationRelation', {
	schemaName: PostgresIdentifier,
	tableName: PostgresIdentifier,
	partitionColumnNames: Schema.NonEmptyArray(PostgresIdentifier),
})

/**
 * Define the what we're replicating (relations) and where it comes from
 */
const ReplicationPlan = Schema.TaggedStruct('ReplicationPlan', {
	slotName: PostgresIdentifier,
	publicationName: PostgresIdentifier,
	relations: Schema.NonEmptyArray(ReplicationRelation),
})

const ReplicationSourceIdentity = Schema.TaggedStruct('ReplicationSourceIdentity', {
	systemIdentifier: Schema.NonEmptyString,
	timelineId: PositiveInteger,
	databaseName: Schema.NonEmptyString,
	currentWalFlushLsn: Lsn,
})

const ReplicationServerInfo = Schema.TaggedStruct('ReplicationServerInfo', {
	serverVersionNumber: PositiveInteger,
	backendProcessId: PositiveInteger,
	walSenderTimeoutMilliseconds: NonNegativeInteger,
	keepaliveIntervalMilliseconds: PositiveInteger,
})

const ReplicationSlotPosition = Schema.TaggedStruct('ReplicationSlotPosition', {
	confirmedFlushLsn: Lsn,
	slotWasCreated: Schema.Boolean,
})

const SlotLeaseRetry = Schema.TaggedStruct('SlotLeaseRetry', {
	attempt: PositiveInteger,
	delayMilliseconds: PositiveInteger,
})

const ConnectionPhase = Schema.Literals([
	'Connecting',
	'IdentifyingSource',
	'ReadingServerInfo',
	'AcquiringSlotLease',
	'WaitingToRetrySlotLease',
	'EnsuringReplicationContract',
	'EnsuringReplicationSlot',
	'PinningOutputSettings',
	'StartingPgOutput',
	'Streaming',
])

const ReconnectReason = Schema.Literals([
	'connection-open-failed',
	'connection-closed',
	'connection-errored',
	'setup-command-failed',
	'server-ended-stream',
])

const SourceRejectionReason = Schema.Literals([
	'unsupported-postgres-version',
	'publication-permission-denied',
	'replica-identity-permission-denied',
	'replication-slot-permission-denied',
	'publication-missing-required-operations',
	'replication-prerequisite-invalid',
	'pgoutput-protocol-incompatible',
])

export const ReplicationState = State({
	Connecting: { plan: ReplicationPlan }, // opens one client with replication: 'database', saves the client and attaches its finalizer to the outer scope
	IdentifyingSource: { plan: ReplicationPlan }, // run IDENTIFY_SYSTEM and get current WAL location, timeline, database name etc
	ReadingServerInfo: { plan: ReplicationPlan, sourceIdentity: ReplicationSourceIdentity }, // we know the source cluster but not whether its version and liveness settings are supported so we have to check info
	AcquiringSlotLease: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		leaseAttempt: PositiveInteger,
	}, // acquiring a lease on the slot
	WaitingToRetrySlotLease: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		retry: SlotLeaseRetry,
	}, // when the lease acquisition failed and we need to retry because the lock timed out
	EnsuringReplicationContract: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
	}, // when a slot lease has been acquired
	EnsuringReplicationSlot: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
	}, // source tables are safe to publish so establish the durable WAL replay boundary
	PinningOutputSettings: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		slotPosition: ReplicationSlotPosition,
	}, // the WAL slot is ready but session-generated text values are not yet canonical - set the slot to use hex output, date style, UTC time, extra float digits, interval style etc
	StartingPgOutput: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		slotPosition: ReplicationSlotPosition,
	}, // all SQL setup is complete, this is the last point we can issue SQL commands - send START_REPLICATION SLOT <slot> LOGICAL 0/0 with protocol version and publication name
	Streaming: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		slotPosition: ReplicationSlotPosition,
	}, // db has switched this connection to COPY BOTH. setup SQL is no longer legal. a dedicated stream controller behind one slot owns queueing, socket pause/resume, output decoding, tx assembly, etc
	ReconnectRequired: {
		phase: ConnectionPhase,
		reason: ReconnectReason,
	}, // terminal state after disconnection
	SourceConfigurationRejected: {
		phase: ConnectionPhase,
		reason: SourceRejectionReason,
	},
	Stopped: {},
})

export const ReplicationEvent = Event({
	ConnectionOpened: {},
	SourceIdentified: { sourceIdentity: ReplicationSourceIdentity },
	ServerInfoRead: { serverInfo: ReplicationServerInfo },
	SlotLeaseAcquired: {},
	SlotLeaseWaitRequired: {},
	SlotLeaseRetryElapsed: {},
	ReplicationContractEnsured: {},
	ReplicationSlotReady: { slotPosition: ReplicationSlotPosition },
	OutputSettingsPinned: {},
	PgOutputStarted: {},
	SessionUnavailable: {
		reason: ReconnectReason,
	},
	SourceRejected: {
		reason: SourceRejectionReason,
	},
	StopRequested: {},
})

/**
 * Slots are side effects of the machine that we can supply at runtime or test-time
 */
export const ReplicationSlot = Slot.define({
	openReplicationSession: Slot.fn({}),
	identifySource: Slot.fn({}),
	readServerInfo: Slot.fn({}),
	acquireSlotLease: Slot.fn({ slotName: PostgresIdentifier }),
	ensureReplicationContract: Slot.fn({
		publicationName: PostgresIdentifier,
		relations: Schema.NonEmptyArray(ReplicationRelation),
		serverVersionNumber: PositiveInteger,
	}),
	ensureReplicationSlot: Slot.fn({ slotName: PostgresIdentifier }),
	pinOutputSettings: Slot.fn({}),
	startPgOutput: Slot.fn({
		slotName: PostgresIdentifier,
		publicationName: PostgresIdentifier,
	}),
	consumePgOutput: Slot.fn({
		keepaliveIntervalMilliseconds: PositiveInteger,
		initialSafeFlushLsn: PostgresLsnValue,
	}),
})

/**
 * Pure helper for slot acquisition retry
 */

const calculateSlotLeaseRetryDelayMilliseconds = (attempt: number): number => Math.min(100 * 2 ** (attempt - 1), 5_000)

/**
 * Create a replication connection for a given plan
 */

export const ReplicationConnection = (plan: typeof ReplicationPlan.Type) =>
	Machine.make({
		state: ReplicationState,
		event: ReplicationEvent,
		slots: ReplicationSlot,
		initial: ReplicationState.Connecting({ plan }),
	})
		// on connecting state, open the session - when connection is opened, then we move to identifying source
		.spawn(ReplicationState.Connecting, ({ slots }) => slots.openReplicationSession())
		.on(ReplicationState.Connecting, ReplicationEvent.ConnectionOpened, ({ state }) =>
			ReplicationState.IdentifyingSource.with(state),
		)

		// Source Identification
		.spawn(ReplicationState.IdentifyingSource, ({ slots }) => slots.identifySource())
		.on(ReplicationState.IdentifyingSource, ReplicationEvent.SourceIdentified, ({ state, event }) =>
			ReplicationState.ReadingServerInfo({
				plan: state.plan,
				sourceIdentity: event.sourceIdentity,
			}),
		)

		// Read Server info
		.spawn(ReplicationState.ReadingServerInfo, ({ slots }) => slots.readServerInfo())
		.on(ReplicationState.ReadingServerInfo, ReplicationEvent.ServerInfoRead, ({ state, event }) =>
			ReplicationState.AcquiringSlotLease({
				leaseAttempt: 1,
				plan: state.plan,
				sourceIdentity: state.sourceIdentity,
				serverInfo: event.serverInfo,
			}),
		)

		// Acquire slot release - may succeed and move to replication contract, or have to wait and retry
		.spawn(ReplicationState.AcquiringSlotLease, ({ slots, state }) =>
			slots.acquireSlotLease({ slotName: state.plan.slotName }),
		)
		.on(ReplicationState.AcquiringSlotLease, ReplicationEvent.SlotLeaseAcquired, ({ state }) =>
			ReplicationState.EnsuringReplicationContract({
				plan: state.plan,
				sourceIdentity: state.sourceIdentity,
				serverInfo: state.serverInfo,
			}),
		)
		.on(ReplicationState.AcquiringSlotLease, ReplicationEvent.SlotLeaseWaitRequired, ({ state }) =>
			ReplicationState.WaitingToRetrySlotLease({
				plan: state.plan,
				sourceIdentity: state.sourceIdentity,
				serverInfo: state.serverInfo,
				retry: SlotLeaseRetry.make({
					attempt: state.leaseAttempt,
					delayMilliseconds: calculateSlotLeaseRetryDelayMilliseconds(state.leaseAttempt),
				}),
			}),
		)

		// when we're waiting, when the wait is elapsed, move back to the acquisition state and ALSO schedule that
		.on(ReplicationState.WaitingToRetrySlotLease, ReplicationEvent.SlotLeaseRetryElapsed, ({ state }) =>
			ReplicationState.AcquiringSlotLease({
				plan: state.plan,
				sourceIdentity: state.sourceIdentity,
				serverInfo: state.serverInfo,
				leaseAttempt: state.retry.attempt + 1,
			}),
		)
		.timeout(ReplicationState.WaitingToRetrySlotLease, {
			duration: (state) => state.retry.delayMilliseconds,
			event: ReplicationEvent.SlotLeaseRetryElapsed,
		})

		// Ensuring replication constract - getting all the stuff setup
		.spawn(ReplicationState.EnsuringReplicationContract, ({ slots, state }) =>
			slots.ensureReplicationContract({
				publicationName: state.plan.publicationName,
				relations: state.plan.relations,
				serverVersionNumber: state.serverInfo.serverVersionNumber,
			}),
		)
		.on(ReplicationState.EnsuringReplicationContract, ReplicationEvent.ReplicationContractEnsured, ({ state }) =>
			ReplicationState.EnsuringReplicationSlot({
				plan: state.plan,
				serverInfo: state.serverInfo,
				sourceIdentity: state.sourceIdentity,
			}),
		)

		// Handle replication setuyp
		.spawn(ReplicationState.EnsuringReplicationSlot, ({ slots, state }) =>
			slots.ensureReplicationSlot({ slotName: state.plan.slotName }),
		)
		.on(ReplicationState.EnsuringReplicationSlot, ReplicationEvent.ReplicationSlotReady, ({ state, event }) =>
			ReplicationState.PinningOutputSettings({
				plan: state.plan,
				serverInfo: state.serverInfo,
				sourceIdentity: state.sourceIdentity,
				slotPosition: event.slotPosition,
			}),
		)

		// Pinning Output settings
		.spawn(ReplicationState.PinningOutputSettings, ({ slots }) => slots.pinOutputSettings())
		.on(ReplicationState.PinningOutputSettings, ReplicationEvent.OutputSettingsPinned, ({ state }) =>
			ReplicationState.StartingPgOutput({
				plan: state.plan,
				serverInfo: state.serverInfo,
				slotPosition: state.slotPosition,
				sourceIdentity: state.sourceIdentity,
			}),
		)

		// start pg output
		.spawn(ReplicationState.StartingPgOutput, ({ slots, state }) =>
			slots.startPgOutput({ slotName: state.plan.slotName, publicationName: state.plan.publicationName }),
		)
		.on(ReplicationState.StartingPgOutput, ReplicationEvent.PgOutputStarted, ({ state }) =>
			ReplicationState.Streaming({
				slotPosition: state.slotPosition,
				serverInfo: state.serverInfo,
				sourceIdentity: state.sourceIdentity,
				plan: state.plan,
			}),
		)
		.spawn(ReplicationState.Streaming, ({ slots, state }) =>
			slots.consumePgOutput({
				keepaliveIntervalMilliseconds: state.serverInfo.keepaliveIntervalMilliseconds,
				initialSafeFlushLsn: state.slotPosition.confirmedFlushLsn,
			}),
		)
		// Error states - Stop Requested, Session Unavaiable, Source Rejected
		.onAny(ReplicationEvent.StopRequested, () => ReplicationState.Stopped)
		.on(ReplicationState.Connecting, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'Connecting', reason: event.reason }),
		)
		.on(ReplicationState.IdentifyingSource, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'IdentifyingSource', reason: event.reason }),
		)
		.on(ReplicationState.ReadingServerInfo, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'ReadingServerInfo', reason: event.reason }),
		)
		.on(ReplicationState.AcquiringSlotLease, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'AcquiringSlotLease', reason: event.reason }),
		)
		.on(ReplicationState.WaitingToRetrySlotLease, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'WaitingToRetrySlotLease', reason: event.reason }),
		)
		.on(ReplicationState.EnsuringReplicationContract, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'EnsuringReplicationContract', reason: event.reason }),
		)
		.on(ReplicationState.EnsuringReplicationSlot, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'EnsuringReplicationSlot', reason: event.reason }),
		)
		.on(ReplicationState.PinningOutputSettings, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'PinningOutputSettings', reason: event.reason }),
		)
		.on(ReplicationState.StartingPgOutput, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'StartingPgOutput', reason: event.reason }),
		)
		.on(ReplicationState.Streaming, ReplicationEvent.SessionUnavailable, ({ event }) =>
			ReplicationState.ReconnectRequired({ phase: 'Streaming', reason: event.reason }),
		)
		.on(ReplicationState.ReadingServerInfo, ReplicationEvent.SourceRejected, ({ event }) =>
			ReplicationState.SourceConfigurationRejected({ phase: 'ReadingServerInfo', reason: event.reason }),
		)
		.on(ReplicationState.EnsuringReplicationContract, ReplicationEvent.SourceRejected, ({ event }) =>
			ReplicationState.SourceConfigurationRejected({ phase: 'EnsuringReplicationContract', reason: event.reason }),
		)
		.on(ReplicationState.EnsuringReplicationSlot, ReplicationEvent.SourceRejected, ({ event }) =>
			ReplicationState.SourceConfigurationRejected({ phase: 'EnsuringReplicationSlot', reason: event.reason }),
		)
		.on(ReplicationState.StartingPgOutput, ReplicationEvent.SourceRejected, ({ event }) =>
			ReplicationState.SourceConfigurationRejected({ phase: 'StartingPgOutput', reason: event.reason }),
		)

		// Final States
		.final(ReplicationState.ReconnectRequired)
		.final(ReplicationState.SourceConfigurationRejected)
		.final(ReplicationState.Stopped)
