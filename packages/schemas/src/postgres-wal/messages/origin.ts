import { Effect, Schema, SchemaGetter } from 'effect'

import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors.ts'
import { PostgresLsnValue } from '../lsn.ts'
import { PgOutputV1MessageTypeByte } from '../type-bytes.ts'

const originMessageHeaderLength = 9
const textDecoder = new TextDecoder()

const readNullTerminatedString = (bytes: Uint8Array, start: number) => {
	let end = start
	while (end < bytes.byteLength && bytes[end] !== 0) {
		end += 1
	}
	if (end === bytes.byteLength) {
		return undefined
	}
	return {
		value: textDecoder.decode(bytes.subarray(start, end)),
		nextOffset: end + 1,
	}
}

export const OriginMessage = Schema.TaggedStruct('Origin', {
	originCommitLsn: PostgresLsnValue,
	name: Schema.String,
})
export interface OriginMessage extends Schema.Schema.Type<typeof OriginMessage> {}

export const OriginMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(OriginMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Origin message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				if (bytes.byteLength < originMessageHeaderLength) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'truncated-message',
							typeByte: PgOutputV1MessageTypeByte.Origin,
							messageTypeName: 'Origin',
						}),
						'Origin message is shorter than its 9-byte header.',
					)
				}

				const name = readNullTerminatedString(bytes, originMessageHeaderLength)
				if (name === undefined) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'truncated-message',
							typeByte: PgOutputV1MessageTypeByte.Origin,
							messageTypeName: 'Origin',
						}),
						'Origin message is missing a null-terminated name.',
					)
				}
				if (name.nextOffset !== bytes.byteLength) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'trailing-bytes',
							typeByte: PgOutputV1MessageTypeByte.Origin,
							messageTypeName: 'Origin',
						}),
						'Origin message has trailing bytes.',
					)
				}

				const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
				return yield* Effect.succeed(
					OriginMessage.make({
						originCommitLsn: PostgresLsnValue.make(view.getBigUint64(1, false)),
						name: name.value,
					}),
				)
			}),
		),
	}),
)
