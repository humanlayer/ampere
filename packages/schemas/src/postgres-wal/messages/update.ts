import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors'
import { PostgresOid } from '../oids'
import { readTupleDataFromCursor, TupleData, TupleDataKindByte } from '../tuple-data'
import { PgOutputV1MessageTypeByte } from '../type-bytes'

export const UpdateMessage = Schema.TaggedStruct('Update', {
	relationOid: PostgresOid,
	keyTuple: Schema.optionalKey(TupleData),
	oldTuple: Schema.optionalKey(TupleData),
	newTuple: TupleData,
})
export interface UpdateMessage extends Schema.Schema.Type<typeof UpdateMessage> {}

const failUpdateDecode = (
	reason: 'truncated-message' | 'trailing-bytes' | 'unknown-tuple-kind' | 'missing-new-tuple',
	message: string,
) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Update,
			messageTypeName: 'Update',
		}),
		message,
	)

export const UpdateMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(UpdateMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Update message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const relationOid = cursor.readUint32()
				if (relationOid === undefined) {
					return yield* failUpdateDecode('truncated-message', 'Update message is missing a relation OID.')
				}

				const firstKind = cursor.readUint8()
				if (firstKind === undefined) {
					return yield* failUpdateDecode('truncated-message', 'Update message is missing a tuple marker.')
				}

				if (firstKind === TupleDataKindByte.New) {
					const newTuple = yield* readTupleDataFromCursor(cursor, PgOutputV1MessageTypeByte.Update, 'Update')
					if (cursor.hasRemaining()) {
						return yield* failUpdateDecode('trailing-bytes', 'Update message has trailing bytes.')
					}
					return yield* Effect.succeed(
						UpdateMessage.make({
							relationOid,
							newTuple,
						}),
					)
				}

				if (firstKind !== TupleDataKindByte.Old && firstKind !== TupleDataKindByte.Key) {
					return yield* failUpdateDecode('unknown-tuple-kind', 'Update message has an unknown tuple marker.')
				}

				const extraTuple = yield* readTupleDataFromCursor(cursor, PgOutputV1MessageTypeByte.Update, 'Update')
				const newKind = cursor.readUint8()
				if (newKind === undefined) {
					return yield* failUpdateDecode('truncated-message', 'Update message is missing a new-tuple marker.')
				}
				if (newKind !== TupleDataKindByte.New) {
					return yield* failUpdateDecode('missing-new-tuple', 'Update message is missing a new-tuple marker.')
				}

				const newTuple = yield* readTupleDataFromCursor(cursor, PgOutputV1MessageTypeByte.Update, 'Update')
				if (cursor.hasRemaining()) {
					return yield* failUpdateDecode('trailing-bytes', 'Update message has trailing bytes.')
				}

				return yield* Effect.succeed(
					UpdateMessage.make({
						relationOid,
						newTuple,
						...(firstKind === TupleDataKindByte.Key ? { keyTuple: extraTuple } : { oldTuple: extraTuple }),
					}),
				)
			}),
		),
	}),
)
