import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors'
import { PostgresOid } from '../oids'
import { readTupleDataFromCursor, TupleData, TupleDataKindByte } from '../tuple-data'
import { PgOutputV1MessageTypeByte } from '../type-bytes'

export const InsertMessage = Schema.TaggedStruct('Insert', {
	relationOid: PostgresOid,
	newTuple: TupleData,
})
export interface InsertMessage extends Schema.Schema.Type<typeof InsertMessage> {}

const failInsertDecode = (
	reason: 'truncated-message' | 'trailing-bytes' | 'unknown-tuple-kind' | 'missing-new-tuple',
	message: string,
) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Insert,
			messageTypeName: 'Insert',
		}),
		message,
	)

export const InsertMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(InsertMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Insert message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const relationOid = cursor.readUint32()
				if (relationOid === undefined) {
					return yield* failInsertDecode('truncated-message', 'Insert message is missing a relation OID.')
				}

				const tupleKind = cursor.readUint8()
				if (tupleKind === undefined) {
					return yield* failInsertDecode('truncated-message', 'Insert message is missing a new-tuple marker.')
				}
				if (tupleKind !== TupleDataKindByte.New) {
					return yield* failInsertDecode('missing-new-tuple', 'Insert message is missing a new-tuple marker.')
				}

				const newTuple = yield* readTupleDataFromCursor(cursor, PgOutputV1MessageTypeByte.Insert, 'Insert')
				if (cursor.hasRemaining()) {
					return yield* failInsertDecode('trailing-bytes', 'Insert message has trailing bytes.')
				}

				return yield* Effect.succeed(
					InsertMessage.make({
						relationOid,
						newTuple,
					}),
				)
			}),
		),
	}),
)
