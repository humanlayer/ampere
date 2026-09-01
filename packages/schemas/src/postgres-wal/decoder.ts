import { Effect, Schema } from 'effect'

import {
	IncompatibleProtocolError,
	mapPgOutputSchemaError,
	MessageDecodeError,
	UnsupportedMessageError,
} from './errors.ts'
import { BeginMessageFromBytes } from './messages/begin.ts'
import { CommitMessageFromBytes } from './messages/commit.ts'
import { DeleteMessageFromBytes } from './messages/delete.ts'
import { InsertMessageFromBytes } from './messages/insert.ts'
import { LogicalDecodingMessageFromBytes } from './messages/message.ts'
import { OriginMessageFromBytes } from './messages/origin.ts'
import { RelationMessageFromBytes } from './messages/relation.ts'
import { TruncateMessageFromBytes } from './messages/truncate.ts'
import { TypeMessageFromBytes } from './messages/type.ts'
import { UpdateMessageFromBytes } from './messages/update.ts'
import { findLaterPgOutputProtocolMessageTypeName, PgOutputV1MessageTypeByte } from './type-bytes.ts'

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
	if (typeByte === PgOutputV1MessageTypeByte.Relation) {
		return yield* Schema.decodeEffect(RelationMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Type) {
		return yield* Schema.decodeEffect(TypeMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Insert) {
		return yield* Schema.decodeEffect(InsertMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Update) {
		return yield* Schema.decodeEffect(UpdateMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Delete) {
		return yield* Schema.decodeEffect(DeleteMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Truncate) {
		return yield* Schema.decodeEffect(TruncateMessageFromBytes)(bytes).pipe(Effect.mapError(mapPgOutputSchemaError))
	}
	if (typeByte === PgOutputV1MessageTypeByte.Message) {
		return yield* Schema.decodeEffect(LogicalDecodingMessageFromBytes)(bytes).pipe(
			Effect.mapError(mapPgOutputSchemaError),
		)
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
