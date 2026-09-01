import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	DeleteMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	TupleCell,
} from '../../src/postgres-wal/index.ts'

const textEncoder = new TextEncoder()
const textCell = (value: string) => TupleCell.cases.Text.make({ bytes: textEncoder.encode(value) })

const electricDeleteIndexReplicaIdentityBytes = new Uint8Array([
	68, 0, 0, 96, 0, 75, 0, 2, 116, 0, 0, 0, 7, 101, 120, 97, 109, 112, 108, 101, 110,
])
const electricDeleteFullReplicaIdentityBytes = new Uint8Array([
	68, 0, 0, 96, 0, 79, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48,
])

describe('pgoutput Delete decoder', () => {
	it.effect('decodes Electric Delete fixture with USING INDEX replica identity', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricDeleteIndexReplicaIdentityBytes })

			expect(message).toStrictEqual(
				DeleteMessage.make({
					relationOid: 24576,
					keyTuple: [textCell('example'), TupleCell.cases.Null.make({})],
				}),
			)
		}),
	)

	it.effect('decodes Electric Delete fixture with FULL replica identity', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricDeleteFullReplicaIdentityBytes })

			expect(message).toStrictEqual(
				DeleteMessage.make({
					relationOid: 24576,
					oldTuple: [textCell('baz'), textCell('560')],
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Delete is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Delete]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Delete,
					messageTypeName: 'Delete',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Delete has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const deleteWithTrailingBytes = new Uint8Array(electricDeleteFullReplicaIdentityBytes.length + 1)
			deleteWithTrailingBytes.set(electricDeleteFullReplicaIdentityBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: deleteWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Delete,
					messageTypeName: 'Delete',
				}),
			)
		}),
	)
})
