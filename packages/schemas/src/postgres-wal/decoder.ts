import { Effect, Schema } from 'effect'

import { BeginMessageFromBytes } from './messages/begin.ts'
import { CommitMessageFromBytes } from './messages/commit.ts'
import {
	IncompatibleProtocolError,
	mapPgOutputSchemaError,
	MessageDecodeError,
	UnsupportedMessageError,
} from './errors.ts'
import { OriginMessageFromBytes } from './messages/origin.ts'
import {
	findLaterPgOutputProtocolMessageTypeName,
	findUnimplementedPgOutputV1MessageTypeName,
	PgOutputV1MessageTypeByte,
} from './type-bytes.ts'

export const DecodePgOutputMessageInput = Schema.Struct({
	bytes: Schema.Uint8Array,
})
export interface DecodePgOutputMessageInput extends Schema.Schema.Type<typeof DecodePgOutputMessageInput> {}

export const decodePgOutputMessage = Effect.fn('pgoutput.decode_message')(function* ({
	bytes,
}: DecodePgOutputMessageInput) {
	const typeByte = bytes.at(0)
	if (typeByte === undefined) {
		return yield* new MessageDecodeError({ reason: 'empty-payload' })
	}

	if (typeByte === PgOutputV1MessageTypeByte.Begin) {
		return yield* Schema.decodeEffect(BeginMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Commit) {
		return yield* Schema.decodeEffect(CommitMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Origin) {
		return yield* Schema.decodeEffect(OriginMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}

	const unimplementedMessageTypeName = findUnimplementedPgOutputV1MessageTypeName(typeByte)
	if (unimplementedMessageTypeName !== undefined) {
		return yield* new MessageDecodeError({
			reason: 'known-v1-type-not-yet-decoded',
			typeByte,
			messageTypeName: unimplementedMessageTypeName,
		})
	}

	const laterProtocolMessageTypeName = findLaterPgOutputProtocolMessageTypeName(typeByte)
	if (laterProtocolMessageTypeName !== undefined) {
		return yield* new IncompatibleProtocolError({
			typeByte,
			messageTypeName: laterProtocolMessageTypeName,
		})
	}

	return yield* new UnsupportedMessageError({
		typeByte,
		rawPayload: bytes,
	})
})
