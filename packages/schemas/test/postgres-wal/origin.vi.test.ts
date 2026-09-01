import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	MessageDecodeError,
	OriginMessage,
	PgOutputV1MessageTypeByte,
	PostgresLsnValue,
} from '../../src/postgres-wal/index.ts'

const electricOriginMessageBytes = new Uint8Array([
	79, 0, 0, 0, 2, 167, 244, 168, 128, 69, 108, 109, 101, 114, 32, 70, 117, 100, 0,
])

describe('pgoutput Origin decoder', () => {
	it.effect('decodes Electric Origin fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricOriginMessageBytes })

			expect(message).toStrictEqual(
				OriginMessage.make({
					originCommitLsn: PostgresLsnValue.make((2n << 32n) | 2_817_828_992n),
					name: 'Elmer Fud',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Origin is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Origin]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Origin,
					messageTypeName: 'Origin',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Origin is missing a null terminator', ({ expect }) =>
		Effect.gen(function* () {
			const truncatedOrigin = electricOriginMessageBytes.slice(0, electricOriginMessageBytes.length - 1)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: truncatedOrigin }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Origin,
					messageTypeName: 'Origin',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Origin has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const originWithTrailingBytes = new Uint8Array(electricOriginMessageBytes.length + 1)
			originWithTrailingBytes.set(electricOriginMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: originWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Origin,
					messageTypeName: 'Origin',
				}),
			)
		}),
	)
})
