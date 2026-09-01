import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	TruncateMessage,
} from '../../src/postgres-wal/index.ts'

const electricTruncateBytes = new Uint8Array([84, 0, 0, 0, 1, 0, 0, 0, 96, 0])
const electricTruncateCascadeBytes = new Uint8Array([84, 0, 0, 0, 1, 1, 0, 0, 96, 0])
const electricTruncateRestartIdentityBytes = new Uint8Array([84, 0, 0, 0, 1, 2, 0, 0, 96, 0])
const electricTruncateCascadeAndRestartIdentityBytes = new Uint8Array([84, 0, 0, 0, 1, 3, 0, 0, 96, 0])

describe('pgoutput Truncate decoder', () => {
	it.effect('decodes Electric Truncate fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricTruncateBytes })

			expect(message).toStrictEqual(
				TruncateMessage.make({
					optionFlags: 0,
					relationOids: [24576],
				}),
			)
		}),
	)

	it.effect('decodes Electric Truncate fixture with CASCADE', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricTruncateCascadeBytes })

			expect(message).toStrictEqual(
				TruncateMessage.make({
					optionFlags: 1,
					relationOids: [24576],
				}),
			)
		}),
	)

	it.effect('decodes Electric Truncate fixture with RESTART IDENTITY', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricTruncateRestartIdentityBytes })

			expect(message).toStrictEqual(
				TruncateMessage.make({
					optionFlags: 2,
					relationOids: [24576],
				}),
			)
		}),
	)

	it.effect('decodes Electric Truncate fixture with CASCADE and RESTART IDENTITY', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricTruncateCascadeAndRestartIdentityBytes })

			expect(message).toStrictEqual(
				TruncateMessage.make({
					optionFlags: 3,
					relationOids: [24576],
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Truncate is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Truncate]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Truncate,
					messageTypeName: 'Truncate',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Truncate has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const truncateWithTrailingBytes = new Uint8Array(electricTruncateBytes.length + 1)
			truncateWithTrailingBytes.set(electricTruncateBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: truncateWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Truncate,
					messageTypeName: 'Truncate',
				}),
			)
		}),
	)
})
