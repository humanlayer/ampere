export const UnimplementedPgOutputV1MessageTypeByte = {
	Commit: 0x43,
	Origin: 0x4f,
	Relation: 0x52,
	Type: 0x59,
	Insert: 0x49,
	Update: 0x55,
	Delete: 0x44,
	Truncate: 0x54,
	Message: 0x4d,
} as const
export type UnimplementedPgOutputV1MessageTypeName = keyof typeof UnimplementedPgOutputV1MessageTypeByte

export const PgOutputV1MessageTypeByte = {
	Begin: 0x42,
	...UnimplementedPgOutputV1MessageTypeByte,
} as const
export type PgOutputV1MessageTypeName = keyof typeof PgOutputV1MessageTypeByte

export const LaterPgOutputProtocolMessageTypeByte = {
	StreamStart: 0x53,
	StreamStop: 0x45,
	StreamCommit: 0x63,
	StreamAbort: 0x41,
	BeginPrepare: 0x62,
	Prepare: 0x50,
	CommitPrepared: 0x4b,
	RollbackPrepared: 0x72,
	StreamPrepare: 0x70,
} as const
export type LaterPgOutputProtocolMessageTypeName = keyof typeof LaterPgOutputProtocolMessageTypeByte

export const unimplementedPgOutputV1MessageTypes = [
	['Commit', UnimplementedPgOutputV1MessageTypeByte.Commit],
	['Origin', UnimplementedPgOutputV1MessageTypeByte.Origin],
	['Relation', UnimplementedPgOutputV1MessageTypeByte.Relation],
	['Type', UnimplementedPgOutputV1MessageTypeByte.Type],
	['Insert', UnimplementedPgOutputV1MessageTypeByte.Insert],
	['Update', UnimplementedPgOutputV1MessageTypeByte.Update],
	['Delete', UnimplementedPgOutputV1MessageTypeByte.Delete],
	['Truncate', UnimplementedPgOutputV1MessageTypeByte.Truncate],
	['Message', UnimplementedPgOutputV1MessageTypeByte.Message],
] as const

export const laterPgOutputProtocolMessageTypes = [
	['StreamStart', LaterPgOutputProtocolMessageTypeByte.StreamStart],
	['StreamStop', LaterPgOutputProtocolMessageTypeByte.StreamStop],
	['StreamCommit', LaterPgOutputProtocolMessageTypeByte.StreamCommit],
	['StreamAbort', LaterPgOutputProtocolMessageTypeByte.StreamAbort],
	['BeginPrepare', LaterPgOutputProtocolMessageTypeByte.BeginPrepare],
	['Prepare', LaterPgOutputProtocolMessageTypeByte.Prepare],
	['CommitPrepared', LaterPgOutputProtocolMessageTypeByte.CommitPrepared],
	['RollbackPrepared', LaterPgOutputProtocolMessageTypeByte.RollbackPrepared],
	['StreamPrepare', LaterPgOutputProtocolMessageTypeByte.StreamPrepare],
] as const

export const findUnimplementedPgOutputV1MessageTypeName = (
	typeByte: number,
): UnimplementedPgOutputV1MessageTypeName | undefined => {
	for (const [messageTypeName, messageTypeByte] of unimplementedPgOutputV1MessageTypes) {
		if (messageTypeByte === typeByte) {
			return messageTypeName
		}
	}
}

export const findLaterPgOutputProtocolMessageTypeName = (
	typeByte: number,
): LaterPgOutputProtocolMessageTypeName | undefined => {
	for (const [messageTypeName, messageTypeByte] of laterPgOutputProtocolMessageTypes) {
		if (messageTypeByte === typeByte) {
			return messageTypeName
		}
	}
}
