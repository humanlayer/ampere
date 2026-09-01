import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import {
	CommitMessage,
	decodePgOutputMessage,
	MessageDecodeError,
	PgOutputV1MessageTypeByte,
	PostgresLsnValue,
} from '../../src/postgres-wal/index.ts'

const electricCommitMessageBytes = new Uint8Array([
	67, 0, 0, 0, 0, 2, 167, 244, 168, 128, 0, 0, 0, 2, 167, 244, 168, 176, 0, 2, 48, 246, 88, 88, 213, 242,
])

describe('pgoutput Commit decoder', () => {
	it.effect('decodes Electric Commit fixture bytes', ({ expect }) =>
		Effect.gen(function* () {
			const message = yield* decodePgOutputMessage({ bytes: electricCommitMessageBytes })

			expect(message).toStrictEqual(
				CommitMessage.make({
					flags: 0,
					commitLsn: PostgresLsnValue.make((2n << 32n) | 2_817_828_992n),
					endLsn: PostgresLsnValue.make((2n << 32n) | 2_817_829_040n),
					commitTimestampMicroseconds: 0x0002_30f6_5858_d5f2n,
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Commit is truncated to the type byte', ({ expect }) =>
		Effect.gen(function* () {
			const failure = yield* Effect.flip(
				decodePgOutputMessage({ bytes: new Uint8Array([PgOutputV1MessageTypeByte.Commit]) }),
			)

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Commit,
					messageTypeName: 'Commit',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Commit is one byte short', ({ expect }) =>
		Effect.gen(function* () {
			const truncatedCommit = electricCommitMessageBytes.slice(0, 25)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: truncatedCommit }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'truncated-message',
					typeByte: PgOutputV1MessageTypeByte.Commit,
					messageTypeName: 'Commit',
				}),
			)
		}),
	)

	it.effect('fails with MessageDecodeError when Commit has trailing bytes', ({ expect }) =>
		Effect.gen(function* () {
			const commitWithTrailingBytes = new Uint8Array(electricCommitMessageBytes.length + 1)
			commitWithTrailingBytes.set(electricCommitMessageBytes)
			const failure = yield* Effect.flip(decodePgOutputMessage({ bytes: commitWithTrailingBytes }))

			expect(failure).toStrictEqual(
				new MessageDecodeError({
					reason: 'trailing-bytes',
					typeByte: PgOutputV1MessageTypeByte.Commit,
					messageTypeName: 'Commit',
				}),
			)
		}),
	)
})
