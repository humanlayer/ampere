import { Schema, SchemaGetter } from 'effect'

const maximumPostgresLsn = 0xffff_ffff_ffff_ffffn

export const PostgresLsnValue = Schema.BigInt.pipe(
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

export const Lsn = Schema.String.pipe(
	Schema.check(Schema.isPattern(/^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/)),
	Schema.decodeTo(PostgresLsnValue, {
		decode: SchemaGetter.transform(parsePostgresLsnText),
		encode: SchemaGetter.transform(formatPostgresLsnText),
	}),
)

export const PostgresIdentifier = Schema.NonEmptyString
export const NonNegativeInteger = Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)))
export const PositiveInteger = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)))

export const ReplicationRelation = Schema.TaggedStruct('ReplicationRelation', {
	schemaName: PostgresIdentifier,
	tableName: PostgresIdentifier,
	partitionColumnNames: Schema.NonEmptyArray(PostgresIdentifier),
})

export const ReplicationPlan = Schema.TaggedStruct('ReplicationPlan', {
	slotName: PostgresIdentifier,
	publicationName: PostgresIdentifier,
	relations: Schema.NonEmptyArray(ReplicationRelation),
})

export const ReplicationSourceIdentity = Schema.TaggedStruct('ReplicationSourceIdentity', {
	systemIdentifier: Schema.NonEmptyString,
	timelineId: PositiveInteger,
	databaseName: Schema.NonEmptyString,
	currentWalFlushLsn: Lsn,
})

export const ReplicationServerInfo = Schema.TaggedStruct('ReplicationServerInfo', {
	serverVersionNumber: PositiveInteger,
	backendProcessId: PositiveInteger,
	walSenderTimeoutMilliseconds: NonNegativeInteger,
	keepaliveIntervalMilliseconds: PositiveInteger,
})

export const ReplicationSlotPosition = Schema.TaggedStruct('ReplicationSlotPosition', {
	confirmedFlushLsn: Lsn,
	slotWasCreated: Schema.Boolean,
})

export const SlotLeaseRetry = Schema.TaggedStruct('SlotLeaseRetry', {
	attempt: PositiveInteger,
	delayMilliseconds: PositiveInteger,
})

export const ConnectionPhase = Schema.Literals([
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

export const ReconnectReason = Schema.Literals([
	'connection-open-failed',
	'connection-closed',
	'connection-errored',
	'setup-command-failed',
	'server-ended-stream',
])

export const SourceRejectionReason = Schema.Literals([
	'unsupported-postgres-version',
	'publication-permission-denied',
	'replica-identity-permission-denied',
	'replication-slot-permission-denied',
	'publication-missing-required-operations',
	'replication-prerequisite-invalid',
	'pgoutput-protocol-incompatible',
])

export const ReplicationStateFields = {
	Connecting: { plan: ReplicationPlan },
	IdentifyingSource: { plan: ReplicationPlan },
	ReadingServerInfo: { plan: ReplicationPlan, sourceIdentity: ReplicationSourceIdentity },
	AcquiringSlotLease: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		leaseAttempt: PositiveInteger,
	},
	WaitingToRetrySlotLease: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		retry: SlotLeaseRetry,
	},
	EnsuringReplicationContract: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
	},
	EnsuringReplicationSlot: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
	},
	PinningOutputSettings: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		slotPosition: ReplicationSlotPosition,
	},
	StartingPgOutput: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		slotPosition: ReplicationSlotPosition,
	},
	Streaming: {
		plan: ReplicationPlan,
		sourceIdentity: ReplicationSourceIdentity,
		serverInfo: ReplicationServerInfo,
		slotPosition: ReplicationSlotPosition,
	},
	ReconnectRequired: { phase: ConnectionPhase, reason: ReconnectReason },
	SourceConfigurationRejected: { phase: ConnectionPhase, reason: SourceRejectionReason },
	Stopped: {},
} as const

export const ReplicationEventFields = {
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
	SessionUnavailable: { reason: ReconnectReason },
	SourceRejected: { reason: SourceRejectionReason },
	StopRequested: {},
} as const
