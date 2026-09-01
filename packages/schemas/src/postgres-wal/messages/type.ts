import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor.ts'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors.ts'
import { PostgresOid } from '../oids.ts'
import { PgOutputV1MessageTypeByte } from '../type-bytes.ts'

export const TypeMessage = Schema.TaggedStruct('Type', {
	typeOid: PostgresOid,
	namespace: Schema.String,
	name: Schema.String,
})
export interface TypeMessage extends Schema.Schema.Type<typeof TypeMessage> {}

const failTypeDecode = (reason: 'truncated-message' | 'trailing-bytes', message: string) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Type,
			messageTypeName: 'Type',
		}),
		message,
	)

export const TypeMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(TypeMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Type message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const typeOid = cursor.readUint32()
				if (typeOid === undefined) {
					return yield* failTypeDecode('truncated-message', 'Type message is missing a type OID.')
				}

				const namespace = cursor.readNullTerminatedString()
				if (namespace === undefined) {
					return yield* failTypeDecode('truncated-message', 'Type message is missing a namespace.')
				}

				const name = cursor.readNullTerminatedString()
				if (name === undefined) {
					return yield* failTypeDecode('truncated-message', 'Type message is missing a type name.')
				}

				if (cursor.hasRemaining()) {
					return yield* failTypeDecode('trailing-bytes', 'Type message has trailing bytes.')
				}

				return yield* Effect.succeed(
					TypeMessage.make({
						typeOid,
						namespace,
						name,
					}),
				)
			}),
		),
	}),
)
