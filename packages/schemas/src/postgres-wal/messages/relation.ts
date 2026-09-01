import { Effect, Schema, SchemaGetter } from 'effect'

import { createPgOutputBytesCursor } from '../bytes-cursor'
import { failPgOutputDecode, PgOutputDecodeFailure } from '../errors'
import { PostgresOid } from '../oids'
import { PgOutputV1MessageTypeByte } from '../type-bytes'

export const ReplicaIdentity = Schema.Literals(['default', 'nothing', 'all-columns', 'index'])
export type ReplicaIdentity = typeof ReplicaIdentity.Type

export const RelationColumn = Schema.Struct({
	isKey: Schema.Boolean,
	name: Schema.String,
	typeOid: PostgresOid,
	typeModifier: Schema.Int,
})
export interface RelationColumn extends Schema.Schema.Type<typeof RelationColumn> {}

export const RelationMessage = Schema.TaggedStruct('Relation', {
	relationOid: PostgresOid,
	namespace: Schema.String,
	name: Schema.String,
	replicaIdentity: ReplicaIdentity,
	columns: Schema.Array(RelationColumn),
})
export interface RelationMessage extends Schema.Schema.Type<typeof RelationMessage> {}

interface ReplicaIdentityByByte {
	readonly [identityByte: number]: ReplicaIdentity
}

const replicaIdentityByByte: ReplicaIdentityByByte = {
	[0x64]: 'default',
	[0x6e]: 'nothing',
	[0x66]: 'all-columns',
	[0x69]: 'index',
}

const failRelationDecode = (
	reason: 'truncated-message' | 'trailing-bytes' | 'unknown-replica-identity',
	message: string,
) =>
	failPgOutputDecode(
		PgOutputDecodeFailure.cases.MessageDecodeError.make({
			reason,
			typeByte: PgOutputV1MessageTypeByte.Relation,
			messageTypeName: 'Relation',
		}),
		message,
	)

export const RelationMessageFromBytes = Schema.Uint8Array.pipe(
	Schema.decodeTo(RelationMessage, {
		encode: SchemaGetter.forbidden(() => 'Encoding a pgoutput Relation message is not supported.'),
		decode: SchemaGetter.transformOrFail((bytes) =>
			Effect.gen(function* () {
				const cursor = createPgOutputBytesCursor(bytes, 1)
				const relationOid = cursor.readUint32()
				if (relationOid === undefined) {
					return yield* failRelationDecode('truncated-message', 'Relation message is missing a relation OID.')
				}

				const namespace = cursor.readNullTerminatedString()
				if (namespace === undefined) {
					return yield* failRelationDecode('truncated-message', 'Relation message is missing a namespace.')
				}

				const name = cursor.readNullTerminatedString()
				if (name === undefined) {
					return yield* failRelationDecode(
						'truncated-message',
						'Relation message is missing a relation name.',
					)
				}

				const replicaIdentityByte = cursor.readUint8()
				if (replicaIdentityByte === undefined) {
					return yield* failRelationDecode(
						'truncated-message',
						'Relation message is missing a replica identity.',
					)
				}

				const replicaIdentity = replicaIdentityByByte[replicaIdentityByte]
				if (replicaIdentity === undefined) {
					return yield* failRelationDecode(
						'unknown-replica-identity',
						'Relation message has an unknown replica identity.',
					)
				}

				const columnCount = cursor.readUint16()
				if (columnCount === undefined) {
					return yield* failRelationDecode('truncated-message', 'Relation message is missing a column count.')
				}

				const columns: Array<RelationColumn> = []
				for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
					const flags = cursor.readUint8()
					if (flags === undefined) {
						return yield* failRelationDecode(
							'truncated-message',
							'Relation message is truncated inside a column.',
						)
					}

					const columnName = cursor.readNullTerminatedString()
					if (columnName === undefined) {
						return yield* failRelationDecode(
							'truncated-message',
							'Relation message is missing a column name.',
						)
					}

					const typeOid = cursor.readUint32()
					if (typeOid === undefined) {
						return yield* failRelationDecode(
							'truncated-message',
							'Relation message is missing a column type OID.',
						)
					}

					const typeModifier = cursor.readInt32()
					if (typeModifier === undefined) {
						return yield* failRelationDecode(
							'truncated-message',
							'Relation message is missing a column type modifier.',
						)
					}

					columns.push(
						RelationColumn.make({
							isKey: flags === 1,
							name: columnName,
							typeOid,
							typeModifier,
						}),
					)
				}

				if (cursor.hasRemaining()) {
					return yield* failRelationDecode('trailing-bytes', 'Relation message has trailing bytes.')
				}

				return yield* Effect.succeed(
					RelationMessage.make({
						relationOid,
						namespace,
						name,
						replicaIdentity,
						columns,
					}),
				)
			}),
		),
	}),
)
