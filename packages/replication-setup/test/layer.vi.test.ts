import { describe, it } from '@effect/vitest'
import { Cause, Context, Effect, Exit, Layer } from 'effect'
import { Client, escapeIdentifier, escapeLiteral } from 'pg'

import { ReplicationOperationsLayer } from '../src/layer'
import {
	classifyReplicationContractQueryError,
	ensureReplicationContract,
	ensureReplicationRelation,
} from '../src/replication-contract'
import type { ReplicationContractConnection, ReplicationContractQueryResult } from '../src/replication-contract'
import {
	ReplicationContractAlreadyConfigured,
	ReplicationContractPermissionDenied,
	ReplicationContractQueryFailed,
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

const makeReplicationContractConnection = (client: Client): ReplicationContractConnection => ({
	query: (queryText) => client.query(queryText),
})

const replicationContractInput = (publicationName: string, tableName = 'todos') => ({
	publicationName,
	relations: [
		ReplicationRelation.make({
			schemaName: 'public',
			tableName,
			partitionColumnNames: ['id'],
		}),
	] as const,
	serverVersionNumber: 180_000,
})

describe('Live Layer tests', () => {
	it('classifies replication contract query errors by PostgreSQL SQLSTATE', ({ expect }) => {
		const duplicateCause = { code: '42710' }
		const permissionCause = { code: '42501' }
		const unexpectedCause = { code: '08006' }

		expect(classifyReplicationContractQueryError(duplicateCause, 'create-publication')).toStrictEqual(
			new ReplicationContractAlreadyConfigured({ operation: 'create-publication', cause: duplicateCause }),
		)
		expect(classifyReplicationContractQueryError(permissionCause, 'create-publication')).toStrictEqual(
			new ReplicationContractPermissionDenied({ operation: 'create-publication', cause: permissionCause }),
		)
		expect(classifyReplicationContractQueryError(unexpectedCause, 'create-publication')).toStrictEqual(
			new ReplicationContractQueryFailed({ operation: 'create-publication', cause: unexpectedCause }),
		)
	})

	it.effect('configures each relation transactionally in lock-safe order', ({ expect }) =>
		Effect.gen(function* () {
			const queries: Array<string> = []
			const responses: Array<ReplicationContractQueryResult> = [
				{ rows: [] },
				{ rows: [{ publication_member: false, replica_identity_full: false }] },
				{ rows: [] },
				{ rows: [] },
				{ rows: [{ publication_member: true, replica_identity_full: true }] },
				{ rows: [] },
			]
			let responseIndex = 0
			const connection: ReplicationContractConnection = {
				query: (queryText) => {
					queries.push(queryText)
					const response = responses.at(responseIndex)
					responseIndex += 1
					return response === undefined
						? Promise.reject(new Error(`Unexpected replication contract query: ${queryText}`))
						: Promise.resolve(response)
				},
			}
			const relation = ReplicationRelation.make({
				schemaName: 'tenant schema',
				tableName: 'events"archive',
				partitionColumnNames: ['organization_id'],
			})

			yield* ensureReplicationRelation({
				connection,
				publicationName: 'ampere"publication',
				relation,
			})

			expect(queries).toHaveLength(6)
			expect(queries.at(0)).toBe('BEGIN')
			expect(queries.at(1)).toContain('FROM pg_publication_rel AS publication_relation')
			expect(queries.at(2)).toBe(
				'ALTER PUBLICATION "ampere""publication" ADD TABLE ONLY "tenant schema"."events""archive"',
			)
			expect(queries.at(3)).toBe('ALTER TABLE ONLY "tenant schema"."events""archive" REPLICA IDENTITY FULL')
			expect(queries.at(4)).toContain('FROM pg_publication_rel AS publication_relation')
			expect(queries.at(5)).toBe('COMMIT')
		}),
	)

	it.effect('rolls back relation configuration when setting replica identity fails', ({ expect }) =>
		Effect.gen(function* () {
			const queries: Array<string> = []
			const connection: ReplicationContractConnection = {
				query: (queryText) => {
					queries.push(queryText)
					if (queryText.includes('REPLICA IDENTITY FULL')) {
						return Promise.reject({ code: '42501' })
					}
					return queryText.includes('FROM pg_publication_rel AS publication_relation')
						? Promise.resolve({ rows: [{ publication_member: false, replica_identity_full: false }] })
						: Promise.resolve({ rows: [] })
				},
			}
			const relation = ReplicationRelation.make({
				schemaName: 'public',
				tableName: 'events',
				partitionColumnNames: ['organization_id'],
			})

			const failure = yield* Effect.flip(
				ensureReplicationRelation({ connection, publicationName: 'ampere_publication', relation }),
			)

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SourceRejected.make({
					reason: 'replica-identity-permission-denied',
				}),
			)
			expect(queries).toEqual([
				'BEGIN',
				expect.stringContaining('FROM pg_publication_rel AS publication_relation'),
				'ALTER PUBLICATION "ampere_publication" ADD TABLE ONLY "public"."events"',
				'ALTER TABLE ONLY "public"."events" REPLICA IDENTITY FULL',
				'ROLLBACK',
			])
		}),
	)

	it.effect('rejects unsupported PostgreSQL versions before issuing setup SQL', ({ expect }) =>
		Effect.gen(function* () {
			let queryCount = 0
			const connection: ReplicationContractConnection = {
				query: (queryText) => {
					queryCount += 1
					return Promise.reject(new Error(`Unexpected replication contract query: ${queryText}`))
				},
			}
			const contract = {
				...replicationContractInput('ampere_unsupported_version_test'),
				serverVersionNumber: 170_000,
			}

			const failure = yield* Effect.flip(ensureReplicationContract({ connection, contract }))

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'unsupported-postgres-version' }),
			)
			expect(queryCount).toBe(0)
		}),
	)

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

	it.effect('creates an idempotent publication contract with FULL replica identity', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const publicationName = 'ampere contract"publication'
				const tableName = 'ampere contract"relation'
				const publicationIdentifier = escapeIdentifier(publicationName)
				const tableIdentifier = escapeIdentifier(tableName)
				const adminClient = yield* acquireAdminClient
				yield* Effect.promise(() => adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`))
				yield* Effect.promise(() => adminClient.query(`DROP TABLE IF EXISTS ${tableIdentifier}`))
				yield* Effect.promise(() =>
					adminClient.query(`CREATE TABLE ${tableIdentifier} (id bigint PRIMARY KEY, value text NOT NULL)`),
				)
				yield* Effect.addFinalizer(() =>
					Effect.all(
						[
							Effect.promise(() =>
								adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`),
							),
							Effect.promise(() => adminClient.query(`DROP TABLE IF EXISTS ${tableIdentifier}`)),
						],
						{ concurrency: 1, discard: true },
					).pipe(
						Effect.catchCause((cause) =>
							Effect.logWarning('Failed to clean up replication contract test', cause),
						),
					),
				)

				const contract = replicationContractInput(publicationName, tableName)
				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				yield* operations.openReplicationSession()
				const result = yield* operations.ensureReplicationContract(contract)
				yield* operations.ensureReplicationContract(contract)

				expect(result).toBeUndefined()
				const contractRows = yield* Effect.promise(() =>
					adminClient.query(
						`SELECT
							publication.puballtables,
							publication.pubinsert,
							publication.pubupdate,
							publication.pubdelete,
							publication.pubtruncate,
							relation.relreplident,
							COUNT(*)::int AS publication_memberships
						FROM pg_publication AS publication
						JOIN pg_publication_rel AS publication_relation ON publication_relation.prpubid = publication.oid
						JOIN pg_class AS relation ON relation.oid = publication_relation.prrelid
						JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
						WHERE publication.pubname = ${escapeLiteral(publicationName)}
							AND namespace.nspname = 'public'
							AND relation.relname = ${escapeLiteral(tableName)}
						GROUP BY publication.oid, relation.oid`,
					),
				)
				expect(contractRows.rows).toStrictEqual([
					{
						puballtables: false,
						pubinsert: true,
						pubupdate: true,
						pubdelete: true,
						pubtruncate: true,
						relreplident: 'f',
						publication_memberships: 1,
					},
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

				const failure = yield* Effect.flip(
					ensureReplicationContract({
						connection: makeReplicationContractConnection(adminClient),
						contract: replicationContractInput(publicationName),
					}),
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
