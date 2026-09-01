import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor.ts'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors.ts'
import { PostgresOid } from '../oids.ts'
import { readTupleDataFromCursor, TupleData, TupleDataKindByte } from '../tuple-data.ts'
import { PgOutputV1MessageTypeByte } from '../type-bytes.ts'

export const DeleteMessage = Schema.TaggedStruct('Delete', {
	relationOid: PostgresOid,
	keyTuple: Schema.optionalKey(TupleData),
	oldTuple: Schema.optionalKey(TupleData),
})
export interface DeleteMessage extends Schema.Schema.Type<typeof DeleteMessage> {}

const failDeleteDecode = (reason: 'truncated-message' | 'trailing-bytes' | 'unknown-tuple-kind', message: string) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Delete,
			messageTypeName: 'Delete',
		}),
		message,
	)

export const DeleteMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(DeleteMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Delete message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const relationOid = cursor.readUint32()
				if (relationOid === undefined) {
					return yield* failDeleteDecode('truncated-message', 'Delete message is missing a relation OID.')
				}

				const tupleKind = cursor.readUint8()
				if (tupleKind === undefined) {
					return yield* failDeleteDecode('truncated-message', 'Delete message is missing a tuple marker.')
				}
				if (tupleKind !== TupleDataKindByte.Old && tupleKind !== TupleDataKindByte.Key) {
					return yield* failDeleteDecode('unknown-tuple-kind', 'Delete message has an unknown tuple marker.')
				}

				const tuple = yield* readTupleDataFromCursor(cursor, PgOutputV1MessageTypeByte.Delete, 'Delete')
				if (cursor.hasRemaining()) {
					return yield* failDeleteDecode('trailing-bytes', 'Delete message has trailing bytes.')
				}

				return yield* Effect.succeed(
					DeleteMessage.make({
						relationOid,
						...(tupleKind === TupleDataKindByte.Key ? { keyTuple: tuple } : { oldTuple: tuple }),
					}),
				)
			}),
		),
	}),
)
