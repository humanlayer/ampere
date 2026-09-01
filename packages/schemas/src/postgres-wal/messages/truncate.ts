import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor.ts'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors.ts'
import { PostgresOid } from '../oids.ts'
import { PgOutputV1MessageTypeByte } from '../type-bytes.ts'

export const TruncateMessage = Schema.TaggedStruct('Truncate', {
	optionFlags: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(255))),
	relationOids: Schema.Array(PostgresOid),
})
export interface TruncateMessage extends Schema.Schema.Type<typeof TruncateMessage> {}

const failTruncateDecode = (reason: 'truncated-message' | 'trailing-bytes', message: string) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Truncate,
			messageTypeName: 'Truncate',
		}),
		message,
	)

export const TruncateMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(TruncateMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Truncate message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const relationCount = cursor.readUint32()
				if (relationCount === undefined) {
					return yield* failTruncateDecode(
						'truncated-message',
						'Truncate message is missing a relation count.',
					)
				}

				const optionFlags = cursor.readUint8()
				if (optionFlags === undefined) {
					return yield* failTruncateDecode('truncated-message', 'Truncate message is missing option flags.')
				}

				const relationOids: Array<PostgresOid> = []
				for (let relationIndex = 0; relationIndex < relationCount; relationIndex += 1) {
					const relationOid = cursor.readUint32()
					if (relationOid === undefined) {
						return yield* failTruncateDecode(
							'truncated-message',
							'Truncate message is truncated inside relation OIDs.',
						)
					}
					relationOids.push(relationOid)
				}

				if (cursor.hasRemaining()) {
					return yield* failTruncateDecode('trailing-bytes', 'Truncate message has trailing bytes.')
				}

				return yield* Effect.succeed(
					TruncateMessage.make({
						optionFlags,
						relationOids,
					}),
				)
			}),
		),
	}),
)
