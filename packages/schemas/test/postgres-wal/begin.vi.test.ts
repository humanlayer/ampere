import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	BeginMessage,
	decodePgOutputMessage,
	IncompatibleProtocolError,
	LaterPgOutputProtocolMessageTypeByte,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	PostgresLsnValue,
	UnsupportedMessageError,
} from '../../src/postgres-wal/index'

const electricBeginMessageBytes = new Uint8Array([
	66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107,
])

const electricUnsupportedMessageBytes = new TextEncoder().encode("!what's this message")

describe('pgoutput Begin decoder', () => {
	it.effect('decodes Electric Begin fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricBeginMessageBytes })

			expect(message).toStrictEqual(
				BeginMessage.make({
					finalLsn: PostgresLsnValue.make((2n << 32n) | 2_817_828_992n),
					commitTimestampMicroseconds: 0x0002_30f6_5858_d5f2n,
					xid: 619,
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Begin is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Begin]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Begin,
					messageTypeName: 'Begin',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Begin is one byte short', ({ expect }) =>
		Effect.gen(function* () {
			const truncatedBegin = electricBeginMessageBytes.slice(0, 20)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: truncatedBegin }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Begin,
					messageTypeName: 'Begin',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Begin has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const beginWithTrailingBytes = new Uint8Array(electricBeginMessageBytes.length + 1)
			beginWithTrailingBytes.set(electricBeginMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: beginWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Begin,
					messageTypeName: 'Begin',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when the payload is empty', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: new Uint8Array() }))

			expect(failure).toStrictEqual(new MessageDecodeError({ reason: 'empty-payload' }))
		}),
	)

	it.effect('fails with UnsupportedMessageError for Electric unknown type-byte fixture', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: electricUnsupportedMessageBytes }))

			expect(failure).toStrictEqual(
				new UnsupportedMessageError({
					typeByte: 0x21,
					rawPayload: electricUnsupportedMessageBytes,
				}),
			)
		}),
	)

	it.effect('fails with IncompatibleProtocolError for Stream Start', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([LaterPgOutputProtocolMessageTypeByte.StreamStart]) }),
			)

			expect(failure).toStrictEqual(
				new IncompatibleProtocolError({
					typeByte: LaterPgOutputProtocolMessageTypeByte.StreamStart,
					messageTypeName: 'StreamStart',
				}),
			)
		}),
	)
})
