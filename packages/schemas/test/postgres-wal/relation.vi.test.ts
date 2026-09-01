import { describe, it } from '@effect/vitest'
import { Effect, Schema } from 'effect'

import {
	decodePgOutputMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	RelationColumn,
	RelationMessage,
} from '../../src/postgres-wal/index'

const electricRelationMessageBytes = new Uint8Array([
	82, 0, 0, 96, 0, 112, 117, 98, 108, 105, 99, 0, 102, 111, 111, 0, 100, 0, 2, 0, 98, 97, 114, 0, 0, 0, 0, 25, 255,
	255, 255, 255, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255,
])

const electricRelationWithArrayTypesBytes = new Uint8Array([
	82, 0, 0, 64, 18, 112, 117, 98, 108, 105, 99, 0, 99, 111, 109, 112, 108, 101, 120, 0, 102, 0, 3, 1, 105, 100, 0, 0,
	0, 11, 134, 255, 255, 255, 255, 1, 110, 117, 109, 98, 101, 114, 115, 0, 0, 0, 3, 239, 255, 255, 255, 255, 1, 116,
	101, 120, 116, 95, 109, 97, 116, 114, 105, 120, 0, 0, 0, 3, 241, 255, 255, 255, 255,
])

const relationIdentityPrefix = [82, 0, 0, 96, 0, 112, 117, 98, 108, 105, 99, 0, 102, 111, 111, 0]

describe('pgoutput Relation decoder', () => {
	it.effect('decodes Electric Relation fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricRelationMessageBytes })

			expect(message).toStrictEqual(
				RelationMessage.make({
					relationOid: 24576,
					namespace: 'public',
					name: 'foo',
					replicaIdentity: 'default',
					columns: [
						RelationColumn.make({
							isKey: false,
							name: 'bar',
							typeOid: 25,
							typeModifier: -1,
						}),
						RelationColumn.make({
							isKey: true,
							name: 'id',
							typeOid: 23,
							typeModifier: -1,
						}),
					],
				}),
			)
		}),
	)

	it.effect('decodes Electric Relation replica-identity fixtures', ({ expect }) =>
		Effect.gen(function* () {
			const replicaIdentities = [
				['default', 0x64],
				['nothing', 0x6e],
				['all-columns', 0x66],
				['index', 0x69],
			] as const

			for (const [replicaIdentity, identityByte] of replicaIdentities) {
				const message = yield* decodePgOutputMessage({
					bytes: new Uint8Array([...relationIdentityPrefix, identityByte, 0, 0]),
				})

				const relationMessage = yield* Schema.decodeUnknownEffect(RelationMessage)(message)
				expect(relationMessage.replicaIdentity).toBe(replicaIdentity)
			}
		}),
	)

	it.effect('decodes Electric Relation fixture with array types', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricRelationWithArrayTypesBytes })

			expect(message).toStrictEqual(
				RelationMessage.make({
					relationOid: 16402,
					namespace: 'public',
					name: 'complex',
					replicaIdentity: 'all-columns',
					columns: [
						RelationColumn.make({
							isKey: true,
							name: 'id',
							typeOid: 2950,
							typeModifier: -1,
						}),
						RelationColumn.make({
							isKey: true,
							name: 'numbers',
							typeOid: 1007,
							typeModifier: -1,
						}),
						RelationColumn.make({
							isKey: true,
							name: 'text_matrix',
							typeOid: 1009,
							typeModifier: -1,
						}),
					],
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Relation is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Relation]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Relation,
					messageTypeName: 'Relation',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Relation has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const relationWithTrailingBytes = new Uint8Array(electricRelationMessageBytes.length + 1)
			relationWithTrailingBytes.set(electricRelationMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: relationWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Relation,
					messageTypeName: 'Relation',
				}),
			)
		}),
	)
})
