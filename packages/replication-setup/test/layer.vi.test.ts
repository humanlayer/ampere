import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import { ReplicationOperationsLayer } from '../src/layer'
import { ReplicationOperations } from '../src/service'

describe('Live Layer tests', () => {
	it.effect('Connection opens successfully if container is running', ({ expect }) =>
		Effect.gen(function* () {
			const layer = yield* ReplicationOperations

			const result = yield* layer.openReplicationSession()
			expect(result).toStrictEqual(undefined)
		}).pipe(Effect.provide(ReplicationOperationsLayer)),
	)

	it.effect('identifies the connected PostgreSQL source', ({ expect }) =>
		Effect.gen(function* () {
			const operations = yield* ReplicationOperations
			yield* operations.openReplicationSession()

			const sourceIdentity = yield* operations.identifySource()

			expect(sourceIdentity.systemIdentifier).not.toBe('')
			expect(sourceIdentity.timelineId).toBeGreaterThan(0)
			expect(sourceIdentity.databaseName).toBe('ampere')
			expect(sourceIdentity.currentWalFlushLsn).toBeTypeOf('bigint')
		}).pipe(Effect.provide(ReplicationOperationsLayer)),
	)

	it.effect('identifies the connected PostgreSQL source', ({ expect }) =>
		Effect.gen(function* () {
			const operations = yield* ReplicationOperations
			yield* operations.openReplicationSession()
			yield* operations.identifySource()
			const serverInfo = yield* operations.readServerInfo()

			expect(serverInfo.serverVersionNumber).toBeGreaterThan(18_0000)
			expect(serverInfo.backendProcessId).toBeGreaterThan(0)
			expect(serverInfo.walSenderTimeoutMilliseconds).toEqual(60_000)
			expect(serverInfo.keepaliveIntervalMilliseconds).toEqual(10_000)
		}).pipe(Effect.provide(ReplicationOperationsLayer)),
	)
})
