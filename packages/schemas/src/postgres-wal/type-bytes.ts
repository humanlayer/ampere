export const ImplementedPgOutputV1MessageTypeByte = {
	Begin: 0x42,
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
export type ImplementedPgOutputV1MessageTypeName = keyof typeof ImplementedPgOutputV1MessageTypeByte

export const PgOutputV1MessageTypeByte = {
	...ImplementedPgOutputV1MessageTypeByte,
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

export const findLaterPgOutputProtocolMessageTypeName = (
	typeByte: number,
): LaterPgOutputProtocolMessageTypeName | undefined => {
	for (const [messageTypeName, messageTypeByte] of laterPgOutputProtocolMessageTypes) {
		if (messageTypeByte === typeByte) {
			return messageTypeName
		}
	}
}
