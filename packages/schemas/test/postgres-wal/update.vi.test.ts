import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	TupleCell,
	UpdateMessage,
} from '../../src/postgres-wal/index'

const textEncoder = new TextEncoder()
const textCell = (value: string) => TupleCell.cases.Text.make({ bytes: textEncoder.encode(value) })

const electricUpdateDefaultReplicaIdentityBytes = new Uint8Array([
	85, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112, 108, 101, 116, 0, 0, 0, 3, 53, 54, 48,
])
const electricUpdateFullReplicaIdentityBytes = new Uint8Array([
	85, 0, 0, 96, 0, 79, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48, 78, 0, 2, 116, 0, 0, 0, 7,
	101, 120, 97, 109, 112, 108, 101, 116, 0, 0, 0, 3, 53, 54, 48,
])
const electricUpdateIndexReplicaIdentityBytes = new Uint8Array([
	85, 0, 0, 96, 0, 75, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 110, 78, 0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112,
	108, 101, 116, 0, 0, 0, 3, 53, 54, 48,
])

describe('pgoutput Update decoder', () => {
	it.effect('decodes Electric Update fixture with default replica identity', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricUpdateDefaultReplicaIdentityBytes })

			expect(message).toStrictEqual(
				UpdateMessage.make({
					relationOid: 24576,
					newTuple: [textCell('example'), textCell('560')],
				}),
			)
		}),
	)

	it.effect('decodes Electric Update fixture with FULL replica identity', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricUpdateFullReplicaIdentityBytes })

			expect(message).toStrictEqual(
				UpdateMessage.make({
					relationOid: 24576,
					oldTuple: [textCell('baz'), textCell('560')],
					newTuple: [textCell('example'), textCell('560')],
				}),
			)
		}),
	)

	it.effect('decodes Electric Update fixture with USING INDEX replica identity', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricUpdateIndexReplicaIdentityBytes })

			expect(message).toStrictEqual(
				UpdateMessage.make({
					relationOid: 24576,
					keyTuple: [textCell('baz'), TupleCell.cases.Null.make({})],
					newTuple: [textCell('example'), textCell('560')],
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Update is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Update]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Update,
					messageTypeName: 'Update',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Update has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const updateWithTrailingBytes = new Uint8Array(electricUpdateDefaultReplicaIdentityBytes.length + 1)
			updateWithTrailingBytes.set(electricUpdateDefaultReplicaIdentityBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: updateWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Update,
					messageTypeName: 'Update',
				}),
			)
		}),
	)
})
