import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	InsertMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	TupleCell,
} from '../../src/postgres-wal/index'

const textEncoder = new TextEncoder()
const textCell = (value: string) => TupleCell.cases.Text.make({ bytes: textEncoder.encode(value) })

const electricInsertMessageBytes = new Uint8Array([
	73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48,
])
const electricInsertWithNullBytes = new Uint8Array([73, 0, 0, 96, 0, 78, 0, 2, 110, 116, 0, 0, 0, 3, 53, 54, 48])
const electricInsertWithUnchangedToastBytes = new Uint8Array([
	73, 0, 0, 96, 0, 78, 0, 2, 117, 116, 0, 0, 0, 3, 53, 54, 48,
])

describe('pgoutput Insert decoder', () => {
	it.effect('decodes Electric Insert fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricInsertMessageBytes })

			expect(message).toStrictEqual(
				InsertMessage.make({
					relationOid: 24576,
					newTuple: [textCell('baz'), textCell('560')],
				}),
			)
		}),
	)

	it.effect('decodes Electric Insert fixture with a null cell', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricInsertWithNullBytes })

			expect(message).toStrictEqual(
				InsertMessage.make({
					relationOid: 24576,
					newTuple: [TupleCell.cases.Null.make({}), textCell('560')],
				}),
			)
		}),
	)

	it.effect('decodes Electric Insert fixture with an unchanged TOAST cell', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricInsertWithUnchangedToastBytes })

			expect(message).toStrictEqual(
				InsertMessage.make({
					relationOid: 24576,
					newTuple: [TupleCell.cases.UnchangedToast.make({}), textCell('560')],
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Insert is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Insert]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Insert,
					messageTypeName: 'Insert',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Insert has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const insertWithTrailingBytes = new Uint8Array(electricInsertMessageBytes.length + 1)
			insertWithTrailingBytes.set(electricInsertMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: insertWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Insert,
					messageTypeName: 'Insert',
				}),
			)
		}),
	)
})
