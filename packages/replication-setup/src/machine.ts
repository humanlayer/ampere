import { Schema, SchemaGetter } from 'effect'
import { Event, Slot, State } from 'effect-machine'

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
	SlotLeaseWaitRequired: { retry: SlotLeaseRetry },
	SlotLeaseRetryElapsed: {},
	ReplicationContractEnsured: {},
	ReplicationSlotReady: { slotPosition: ReplicationSlotPosition },
	OutputSettingsPinned: {},
	PgOutputStarted: {},
	SessionUnavailable: {
		phase: ConnectionPhase,
		reason: ReconnectReason,
	},
	SourceRejected: {
		phase: ConnectionPhase,
		reason: SourceRejectionReason,
	},
	StopRequested: {},
})

/**
 * Slots are side effects of the machine that we can supply at runtime or test-time
 */
export const ReplicationFunctionSlots = Slot.define({
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
