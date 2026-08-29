import { describe, it } from '@effect/vitest'
import { Effect, Exit, Option } from 'effect'

import {
	advanceSafeFlushLsn,
	makeStandbyStatusUpdate,
	parseReplicationProtocolFrame,
} from '../src/replication-protocol.ts'
import { PostgresLsnValue, ReplicationOperationFailure, ReplicationProtocolFrame } from '../src/schemas.ts'

const writeUnsignedInt64 = (bytes: Uint8Array, offset: number, value: bigint): void => {
	new DataView(bytes.buffer).setBigUint64(offset, value, false)
}

const writeSignedInt64 = (bytes: Uint8Array, offset: number, value: bigint): void => {
	new DataView(bytes.buffer).setBigInt64(offset, value, false)
}

describe('PostgreSQL replication protocol', () => {
	it.effect('parses an XLogData frame without copying its header into the payload', ({ expect }) =>
		Effect.gen(function* () {
			const bytes = new Uint8Array(28)
			bytes[0] = 0x77
			writeUnsignedInt64(bytes, 1, 0x10n)
			writeUnsignedInt64(bytes, 9, 0x20n)
			writeSignedInt64(bytes, 17, 30n)
			bytes.set([0x42, 0x43, 0x44], 25)

			const frame = yield* parseReplicationProtocolFrame(bytes)

			expect(frame).toStrictEqual(
				ReplicationProtocolFrame.cases.XLogData.make({
					walStart: PostgresLsnValue.make(0x10n),
					serverWalEnd: PostgresLsnValue.make(0x20n),
					serverTimestampMicroseconds: 30n,
					payload: new Uint8Array([0x42, 0x43, 0x44]),
				}),
			)
		}),
	)

	it.effect('parses a primary keepalive reply request', ({ expect }) =>
		Effect.gen(function* () {
			const bytes = new Uint8Array(18)
			bytes[0] = 0x6b
			writeUnsignedInt64(bytes, 1, 0x30n)
			writeSignedInt64(bytes, 9, 40n)
			bytes[17] = 1

			const frame = yield* parseReplicationProtocolFrame(bytes)

			expect(frame).toStrictEqual(
				ReplicationProtocolFrame.cases.PrimaryKeepalive.make({
					serverWalEnd: PostgresLsnValue.make(0x30n),
					serverTimestampMicroseconds: 40n,
					replyRequested: true,
				}),
			)
		}),
	)

	it.effect('rejects malformed and unsupported replication frames', ({ expect }) =>
		Effect.gen(function* () {
			const malformedXLogData = yield* Effect.exit(parseReplicationProtocolFrame(new Uint8Array([0x77])))
			const unsupportedMessage = yield* Effect.exit(parseReplicationProtocolFrame(new Uint8Array([0x78])))

			for (const exit of [malformedXLogData, unsupportedMessage]) {
				expect(Exit.isFailure(exit)).toBe(true)
				if (Exit.isFailure(exit)) {
					expect(Option.getOrUndefined(Exit.findErrorOption(exit))).toStrictEqual(
						ReplicationOperationFailure.cases.SourceRejected.make({
							reason: 'pgoutput-protocol-incompatible',
						}),
					)
				}
			}
		}),
	)

	it('encodes a standby status update at one explicit safe boundary', ({ expect }) => {
		const timestampMilliseconds = Date.UTC(2000, 0, 1) + 1234
		const bytes = makeStandbyStatusUpdate(0x1a_0000_002bn, timestampMilliseconds)
		const view = new DataView(bytes.buffer)

		expect(bytes.byteLength).toBe(34)
		expect(bytes[0]).toBe(0x72)
		expect(view.getBigUint64(1, false)).toBe(0x1a_0000_002bn)
		expect(view.getBigUint64(9, false)).toBe(0x1a_0000_002bn)
		expect(view.getBigUint64(17, false)).toBe(0x1a_0000_002bn)
		expect(view.getBigInt64(25, false)).toBe(1_234_000n)
		expect(bytes[33]).toBe(0)
	})

	it('never moves the safe flush boundary backward', ({ expect }) => {
		const currentLsn = PostgresLsnValue.make(0x20n)

		expect(advanceSafeFlushLsn(currentLsn, PostgresLsnValue.make(0x10n))).toBe(currentLsn)
		expect(advanceSafeFlushLsn(currentLsn, PostgresLsnValue.make(0x30n))).toBe(0x30n)
	})
})
