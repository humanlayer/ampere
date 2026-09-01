import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	TypeMessage,
} from '../../src/postgres-wal/index.ts'

const electricTypeMessageBytes = new Uint8Array([
	89, 0, 0, 128, 52, 112, 117, 98, 108, 105, 99, 0, 101, 120, 97, 109, 112, 108, 101, 95, 116, 121, 112, 101, 0,
])

describe('pgoutput Type decoder', () => {
	it.effect('decodes Electric Type fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricTypeMessageBytes })

			expect(message).toStrictEqual(
				TypeMessage.make({
					typeOid: 32820,
					namespace: 'public',
					name: 'example_type',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Type is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Type]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Type,
					messageTypeName: 'Type',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Type has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const typeWithTrailingBytes = new Uint8Array(electricTypeMessageBytes.length + 1)
			typeWithTrailingBytes.set(electricTypeMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: typeWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Type,
					messageTypeName: 'Type',
				}),
			)
		}),
	)
})
