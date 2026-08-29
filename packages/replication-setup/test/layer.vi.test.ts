import { describe, it } from '@effect/vitest'
import { Cause, Context, Effect, Exit, Layer } from 'effect'
import { Client, escapeIdentifier } from 'pg'

import { classifyCreatePublicationError, ReplicationOperationsLayer } from '../src/layer'
import {
	CreatePublicationAlreadyExists,
	CreatePublicationFailed,
	CreatePublicationPermissionDenied,
	ReplicationOperationFailure,
	ReplicationRelation,
	ReplicationSlotLeaseAlreadyAcquired,
	SlotLeaseOutcome,
} from '../src/schemas'
import { ReplicationOperations } from '../src/service'

const testDatabaseUrl = 'postgres://postgres:postgres@localhost:55432/ampere'

const acquireAdminClient = Effect.acquireRelease(
	Effect.promise(async () => {
		const client = new Client({ connectionString: testDatabaseUrl })
		await client.connect()
		return client
	}),
	(client) =>
		Effect.promise(() => client.end()).pipe(
			Effect.catchCause((cause) => Effect.logWarning('Failed to close publication test client', cause)),
		),
)

const replicationContractInput = (publicationName: string) => ({
	publicationName,
	relations: [
		ReplicationRelation.make({
			schemaName: 'public',
			tableName: 'todos',
			partitionColumnNames: ['id'],
		}),
	] as const,
	serverVersionNumber: 180_000,
})

describe('Live Layer tests', () => {
	it('classifies create publication errors by PostgreSQL SQLSTATE', ({ expect }) => {
		const duplicateCause = { code: '42710' }
		const permissionCause = { code: '42501' }
		const unexpectedCause = { code: '08006' }

		expect(classifyCreatePublicationError(duplicateCause)).toStrictEqual(
			new CreatePublicationAlreadyExists({ cause: duplicateCause }),
		)
		expect(classifyCreatePublicationError(permissionCause)).toStrictEqual(
			new CreatePublicationPermissionDenied({ cause: permissionCause }),
		)
		expect(classifyCreatePublicationError(unexpectedCause)).toStrictEqual(
			new CreatePublicationFailed({ cause: unexpectedCause }),
		)
	})

	it.effect('Connection opens successfully if container is running', ({ expect }) =>
		Effect.gen(function* () {
			const layer = yield* ReplicationOperations

			const result = yield* layer.openReplicationSession()
			expect(result).toStrictEqual(undefined)
		}).pipe(Effect.provide(ReplicationOperationsLayer)),
	)

	it.effect('Identifies the connected PostgreSQL source', ({ expect }) =>
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

	it.effect('Reads server info successfully', ({ expect }) =>
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

	it.effect('Acquires exactly one connection', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const firstContext = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const secondContext = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const thirdContext = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))

				const firstOperations = Context.get(firstContext, ReplicationOperations)
				const secondOperations = Context.get(secondContext, ReplicationOperations)
				const thirdOperations = Context.get(thirdContext, ReplicationOperations)

				yield* firstOperations.openReplicationSession()
				yield* secondOperations.openReplicationSession()
				yield* thirdOperations.openReplicationSession()

				const firstOutcome = yield* firstOperations.acquireSlotLease({ slotName: 'ampere_contention_test' })
				const secondOutcome = yield* secondOperations.acquireSlotLease({ slotName: 'ampere_contention_test' })
				// ask for different slot
				const thirdOutcome = yield* thirdOperations.acquireSlotLease({ slotName: 'something_else' })

				expect(firstOutcome).toStrictEqual(SlotLeaseOutcome.cases.Acquired.make({}))
				expect(secondOutcome).toStrictEqual(SlotLeaseOutcome.cases.WaitRequired.make({}))
				expect(thirdOutcome).toStrictEqual(SlotLeaseOutcome.cases.Acquired.make({}))
			}),
		),
	)

	it.effect('Rejects repeated acquisition and releases the slot lease with its scope', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const contenderContext = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const contenderOperations = Context.get(contenderContext, ReplicationOperations)
				yield* contenderOperations.openReplicationSession()

				yield* Effect.scoped(
					Effect.gen(function* () {
						const ownerContext = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
						const ownerOperations = Context.get(ownerContext, ReplicationOperations)
						yield* ownerOperations.openReplicationSession()

						const firstOutcome = yield* ownerOperations.acquireSlotLease({
							slotName: 'ampere_scope_release_test',
						})
						const repeatedOutcome = yield* Effect.exit(
							ownerOperations.acquireSlotLease({
								slotName: 'ampere_scope_release_test',
							}),
						)
						const contendedOutcome = yield* contenderOperations.acquireSlotLease({
							slotName: 'ampere_scope_release_test',
						})

						expect(firstOutcome).toStrictEqual(SlotLeaseOutcome.cases.Acquired.make({}))
						expect(Exit.isFailure(repeatedOutcome)).toBe(true)
						if (Exit.isFailure(repeatedOutcome)) {
							const reason = repeatedOutcome.cause.reasons.at(0)
							expect(reason !== undefined && Cause.isDieReason(reason)).toBe(true)
							if (reason !== undefined && Cause.isDieReason(reason)) {
								expect(reason.defect).toStrictEqual(
									new ReplicationSlotLeaseAlreadyAcquired({
										acquiredSlotName: 'ampere_scope_release_test',
										requestedSlotName: 'ampere_scope_release_test',
									}),
								)
							}
						}
						expect(contendedOutcome).toStrictEqual(SlotLeaseOutcome.cases.WaitRequired.make({}))
					}),
				)

				const outcomeAfterOwnerScopeClosed = yield* contenderOperations.acquireSlotLease({
					slotName: 'ampere_scope_release_test',
				})
				expect(outcomeAfterOwnerScopeClosed).toStrictEqual(SlotLeaseOutcome.cases.Acquired.make({}))
			}),
		),
	)

	it.effect('creates a publication with every required operation enabled', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const publicationName = 'ampere_create_publication_test'
				const publicationIdentifier = escapeIdentifier(publicationName)
				const adminClient = yield* acquireAdminClient
				yield* Effect.promise(() => adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`))
				yield* Effect.addFinalizer(() =>
					Effect.promise(() => adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`)).pipe(
						Effect.asVoid,
						Effect.catchCause((cause) => Effect.logWarning('Failed to clean up test publication', cause)),
					),
				)

				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				yield* operations.openReplicationSession()
				const result = yield* operations.ensureReplicationContract(replicationContractInput(publicationName))

				expect(result).toBeUndefined()
				const publicationRows = yield* Effect.promise(() =>
					adminClient.query(
						`SELECT pubinsert, pubupdate, pubdelete, pubtruncate
						FROM pg_publication
						WHERE pubname = '${publicationName}'`,
					),
				)
				expect(publicationRows.rows).toStrictEqual([
					{ pubinsert: true, pubupdate: true, pubdelete: true, pubtruncate: true },
				])
			}),
		),
	)

	it.effect('rejects an existing publication missing required operations', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const publicationName = 'ampere_missing_operations"test'
				const publicationIdentifier = escapeIdentifier(publicationName)
				const adminClient = yield* acquireAdminClient
				yield* Effect.promise(() => adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`))
				yield* Effect.promise(() =>
					adminClient.query(`CREATE PUBLICATION ${publicationIdentifier} WITH (publish = 'insert')`),
				)
				yield* Effect.addFinalizer(() =>
					Effect.promise(() => adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`)).pipe(
						Effect.asVoid,
						Effect.catchCause((cause) => Effect.logWarning('Failed to clean up test publication', cause)),
					),
				)

				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				yield* operations.openReplicationSession()
				const failure = yield* Effect.flip(
					operations.ensureReplicationContract(replicationContractInput(publicationName)),
				)

				expect(failure).toStrictEqual(
					ReplicationOperationFailure.cases.SourceRejected.make({
						reason: 'publication-missing-required-operations',
					}),
				)
			}),
		),
	)
})
