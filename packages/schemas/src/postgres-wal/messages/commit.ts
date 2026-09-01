import { Effect, Schema, SchemaGetter } from 'effect'

import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors'
import { PostgresLsnValue } from '../lsn'
import { PgOutputV1MessageTypeByte } from '../type-bytes'

const commitMessageLength = 26

export const CommitMessage = Schema.TaggedStruct('Commit', {
	flags: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(255))),
	commitLsn: PostgresLsnValue,
	endLsn: PostgresLsnValue,
	commitTimestampMicroseconds: Schema.BigInt,
})
export interface CommitMessage extends Schema.Schema.Type<typeof CommitMessage> {}

export const CommitMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(CommitMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Commit message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				if (bytes.byteLength < commitMessageLength) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'truncated-message',
							typeByte: PgOutputV1MessageTypeByte.Commit,
							messageTypeName: 'Commit',
						}),
						'Commit message is shorter than 26 bytes.',
					)
				}
				if (bytes.byteLength > commitMessageLength) {
					return yield* failPgOutputDecode(
						PgOutputDecodeFailure.cases.MessageDecodeError.make({
							reason: 'trailing-bytes',
							typeByte: PgOutputV1MessageTypeByte.Commit,
							messageTypeName: 'Commit',
						}),
						'Commit message has trailing bytes.',
					)
				}

				const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
				return yield* Effect.succeed(
					CommitMessage.make({
						flags: view.getUint8(1),
						commitLsn: PostgresLsnValue.make(view.getBigUint64(2, false)),
						endLsn: PostgresLsnValue.make(view.getBigUint64(10, false)),
						commitTimestampMicroseconds: view.getBigInt64(18, false),
					}),
				)
			}),
		),
	}),
)
