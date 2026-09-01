import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	decodePgOutputMessage,
	LogicalDecodingMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	PostgresLsnValue,
} from '../../src/postgres-wal/index.ts'

const electricLogicalDecodingMessageBytes = new Uint8Array([
	77, 1, 0, 0, 0, 0, 0, 0, 0, 1, 104, 101, 108, 108, 111, 0, 0, 0, 0, 5, 119, 111, 114, 108, 100,
])

describe('pgoutput Message decoder', () => {
	it.effect('decodes Electric logical decoding Message fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricLogicalDecodingMessageBytes })

			expect(message).toStrictEqual(
				LogicalDecodingMessage.make({
					transactional: true,
					lsn: PostgresLsnValue.make(1n),
					prefix: 'hello',
					content: new TextEncoder().encode('world'),
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Message is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Message]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Message,
					messageTypeName: 'Message',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Message has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const messageWithTrailingBytes = new Uint8Array(electricLogicalDecodingMessageBytes.length + 1)
			messageWithTrailingBytes.set(electricLogicalDecodingMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: messageWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Message,
					messageTypeName: 'Message',
				}),
			)
		}),
	)
})
