import { Effect, Schema, SchemaGetter } from 'effect'

import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors'
import { PostgresLsnValue } from '../lsn'
import { PgOutputV1MessageTypeByte } from '../type-bytes'

const maximumPostgresTransactionId = 0xffff_ffff
const beginMessageLength = 21

export const PostgresTransactionId = Schema.Int.pipe(
	Schema.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(maximumPostgresTransactionId)),
)
export type PostgresTransactionId = typeof PostgresTransactionId.Type

export const BeginMessage = Schema.TaggedStruct('Begin', {
	finalLsn: PostgresLsnValue,
	commitTimestampMicroseconds: Schema.BigInt,
	xid: PostgresTransactionId,
})
export interface BeginMessage extends Schema.Schema.Type<typeof BeginMessage> {}

export const BeginMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(BeginMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Begin message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				if (bytes.byteLength < beginMessageLength) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'truncated-message',
							typeByte: PgOutputV1MessageTypeByte.Begin,
							messageTypeName: 'Begin',
						}),
						'Begin message is shorter than 21 bytes.',
					)
				}
				if (bytes.byteLength > beginMessageLength) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'trailing-bytes',
							typeByte: PgOutputV1MessageTypeByte.Begin,
							messageTypeName: 'Begin',
						}),
						'Begin message has trailing bytes.',
					)
				}

				const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
				return yield* Effect.succeed(
					BeginMessage.make({
						finalLsn: PostgresLsnValue.make(view.getBigUint64(1, false)),
						commitTimestampMicroseconds: view.getBigInt64(9, false),
						xid: view.getUint32(17, false),
					}),
				)
			}),
		),
	}),
)
