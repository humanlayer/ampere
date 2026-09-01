import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor.ts'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors.ts'
import { PostgresLsnValue } from '../lsn.ts'
import { PgOutputV1MessageTypeByte } from '../type-bytes.ts'

export const LogicalDecodingMessage = Schema.TaggedStruct('Message', {
	transactional: Schema.Boolean,
	lsn: PostgresLsnValue,
	prefix: Schema.String,
	content: Schema.Uint8Array,
})
export interface LogicalDecodingMessage extends Schema.Schema.Type<typeof LogicalDecodingMessage> {}

const failMessageDecode = (reason: 'truncated-message' | 'trailing-bytes', message: string) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Message,
			messageTypeName: 'Message',
		}),
		message,
	)

export const LogicalDecodingMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(LogicalDecodingMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const flags = cursor.readUint8()
				if (flags === undefined) {
					return yield* failMessageDecode('truncated-message', 'Message is missing flags.')
				}

				const lsn = cursor.readBigUint64()
				if (lsn === undefined) {
					return yield* failMessageDecode('truncated-message', 'Message is missing an LSN.')
				}

				const prefix = cursor.readNullTerminatedString()
				if (prefix === undefined) {
					return yield* failMessageDecode('truncated-message', 'Message is missing a prefix.')
				}

				const contentLength = cursor.readUint32()
				if (contentLength === undefined) {
					return yield* failMessageDecode('truncated-message', 'Message is missing a content length.')
				}

				const content = cursor.readBytes(contentLength)
				if (content === undefined) {
					return yield* failMessageDecode('truncated-message', 'Message is truncated inside its content.')
				}

				if (cursor.hasRemaining()) {
					return yield* failMessageDecode('trailing-bytes', 'Message has trailing bytes.')
				}

				return yield* Effect.succeed(
					LogicalDecodingMessage.make({
						transactional: flags === 1,
						lsn: PostgresLsnValue.make(lsn),
						prefix,
						content,
					}),
				)
			}),
		),
	}),
)
