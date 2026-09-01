import { describe, it } from '@effect/vitest'
import { Cause, Context, Effect, Exit, Fiber, Layer, Option, Stream } from 'effect'
import { Client, escapeIdentifier, escapeLiteral } from 'pg'

import { ReplicationOperationsLayer } from '../../src/connection/layer'
import { pinOutputSettings } from '../../src/connection/output-settings'
import type { OutputSettingsConnection } from '../../src/connection/output-settings'
import {
	classifyReplicationContractQueryError,
	ensureReplicationRelation,
} from '../../src/connection/replication-contract'
import type {
	ReplicationContractConnection,
	ReplicationContractQueryResult,
} from '../../src/connection/replication-contract'
import { makeStartPgOutputCommand } from '../../src/connection/replication-protocol'
import { ensureReplicationSlot } from '../../src/connection/replication-slot'
import type { ReplicationSlotConnection } from '../../src/connection/replication-slot'
import {
	ReplicationContractAlreadyConfigured,
	ReplicationContractPermissionDenied,
	ReplicationContractQueryFailed,
	ReplicationOperationFailure,
	ReplicationProtocolFrame,
	ReplicationRelation,
	PostgresLsnValue,
	ReplicationSlotPosition,
	ReplicationSlotLeaseAlreadyAcquired,
	SlotLeaseOutcome,
} from '../../src/connection/schemas'
import { ReplicationOperations } from '../../src/connection/service'

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
	it('builds an escaped pgoutput version 1 startup command', ({ expect }) => {
		expect(
			makeStartPgOutputCommand({
				slotName: 'ampere slot"test',
				publicationName: 'ampere publication"test',
				startLsn: PostgresLsnValue.make(0x1a_0000_002bn),
			}),
		).toBe(
			`START_REPLICATION SLOT "ampere slot""test" LOGICAL 1A/2B (proto_version '1', publication_names '"ampere publication""test"')`,
		)
	})

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

	it.effect('resumes an existing pgoutput slot from its confirmed flush LSN', ({ expect }) =>
		Effect.gen(function* () {
			const queries: Array<string> = []
			const connection: ReplicationSlotConnection = {
				query: (queryText) => {
					queries.push(queryText)
					return Promise.resolve({
						rows: [
							{
								slot_type: 'logical',
								plugin: 'pgoutput',
								database_matches: true,
								active: false,
								confirmed_flush_lsn: '1A/2B',
							},
						],
					})
				},
			}

			const position = yield* ensureReplicationSlot({ connection, slotName: 'ampere_resume_test' })

			expect(position.confirmedFlushLsn).toBe(0x1a_0000_002bn)
			expect(position.slotWasCreated).toBe(false)
			expect(queries).toHaveLength(1)
			expect(queries.at(0)).toContain("WHERE slot_name = 'ampere_resume_test'")
		}),
	)

	it.effect('rejects an existing slot using a different output plugin', ({ expect }) =>
		Effect.gen(function* () {
			const connection: ReplicationSlotConnection = {
				query: () =>
					Promise.resolve({
						rows: [
							{
								slot_type: 'logical',
								plugin: 'test_decoding',
								database_matches: true,
								active: false,
								confirmed_flush_lsn: '0/10',
							},
						],
					}),
			}

			const failure = yield* Effect.flip(ensureReplicationSlot({ connection, slotName: 'incompatible_slot' }))

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'pgoutput-protocol-incompatible' }),
			)
		}),
	)

	it.effect('reports replication slot permission failures as source rejection', ({ expect }) =>
		Effect.gen(function* () {
			const connection: ReplicationSlotConnection = {
				query: () => Promise.reject({ code: '42501' }),
			}

			const failure = yield* Effect.flip(ensureReplicationSlot({ connection, slotName: 'permission_test' }))

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SourceRejected.make({
					reason: 'replication-slot-permission-denied',
				}),
			)
		}),
	)

	it.effect('pins and verifies deterministic output settings', ({ expect }) =>
		Effect.gen(function* () {
			const queries: Array<string> = []
			const connection: OutputSettingsConnection = {
				query: (queryText) => {
					queries.push(queryText)
					return Promise.resolve({
						rows: queryText.startsWith('SELECT')
							? [
									{
										bytea_output: 'hex',
										date_style: 'ISO, DMY',
										time_zone: 'UTC',
										extra_float_digits: '1',
										interval_style: 'iso_8601',
									},
								]
							: [],
					})
				},
			}

			yield* pinOutputSettings({ connection })

			expect(queries).toHaveLength(2)
			expect(queries.at(0)).toContain("SET bytea_output = 'hex'")
			expect(queries.at(0)).toContain("SET DateStyle = 'ISO, DMY'")
			expect(queries.at(0)).toContain("SET TimeZone = 'UTC'")
			expect(queries.at(0)).toContain('SET extra_float_digits = 1')
			expect(queries.at(0)).toContain("SET IntervalStyle = 'iso_8601'")
			expect(queries.at(1)).toContain("current_setting('bytea_output')")
		}),
	)

	it.effect('rejects output settings that PostgreSQL did not pin', ({ expect }) =>
		Effect.gen(function* () {
			const connection: OutputSettingsConnection = {
				query: (queryText) =>
					Promise.resolve({
						rows: queryText.startsWith('SELECT')
							? [
									{
										bytea_output: 'escape',
										date_style: 'ISO, DMY',
										time_zone: 'UTC',
										extra_float_digits: '1',
										interval_style: 'iso_8601',
									},
								]
							: [],
					}),
			}

			const failure = yield* Effect.flip(pinOutputSettings({ connection }))

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'replication-prerequisite-invalid' }),
			)
		}),
	)

	it.effect('rejects unsupported PostgreSQL versions before issuing setup SQL', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				const contract = {
					...replicationContractInput('ampere_unsupported_version_test'),
					serverVersionNumber: 170_000,
				}

				const failure = yield* Effect.flip(operations.ensureReplicationContract(contract))

				expect(failure).toStrictEqual(
					ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'unsupported-postgres-version' }),
				)
			}),
		),
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

	it.effect('creates and then resumes a durable pgoutput replication slot', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const slotName = 'ampere_ensure_slot_test'
				const adminClient = yield* acquireAdminClient
				yield* Effect.promise(() =>
					adminClient.query(
						`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
					),
				)
				yield* Effect.addFinalizer(() =>
					Effect.promise(() =>
						adminClient.query(
							`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
						),
					).pipe(
						Effect.asVoid,
						Effect.catchCause((cause) =>
							Effect.logWarning('Failed to clean up replication slot test', cause),
						),
					),
				)

				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				yield* operations.openReplicationSession()
				const createdPosition = yield* operations.ensureReplicationSlot({ slotName })
				const resumedPosition = yield* operations.ensureReplicationSlot({ slotName })

				expect(createdPosition.slotWasCreated).toBe(true)
				expect(createdPosition.confirmedFlushLsn).toBeTypeOf('bigint')
				expect(resumedPosition).toStrictEqual(
					ReplicationSlotPosition.make({
						confirmedFlushLsn: createdPosition.confirmedFlushLsn,
						slotWasCreated: false,
					}),
				)
				const slotRows = yield* Effect.promise(() =>
					adminClient.query(
						`SELECT slot_type, plugin, database, active FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
					),
				)
				expect(slotRows.rows).toStrictEqual([
					{ slot_type: 'logical', plugin: 'pgoutput', database: 'ampere', active: false },
				])
			}),
		),
	)

	it.effect('pins output settings on the live replication session', ({ expect }) =>
		Effect.gen(function* () {
			const operations = yield* ReplicationOperations
			yield* operations.openReplicationSession()
			yield* operations.pinOutputSettings()

			expect(yield* operations.pinOutputSettings()).toBeUndefined()
		}).pipe(Effect.provide(ReplicationOperationsLayer)),
	)

	it.live('streams and acknowledges pgoutput frames against PostgreSQL', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const slotName = 'ampere_start_pgoutput_test'
				const publicationName = 'ampere_start_pgoutput_test'
				const tableName = 'ampere_start_pgoutput_test'
				const publicationIdentifier = escapeIdentifier(publicationName)
				const tableIdentifier = escapeIdentifier(tableName)
				const adminClient = yield* acquireAdminClient
				yield* Effect.promise(() =>
					adminClient.query(
						`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
					),
				)
				yield* Effect.promise(() => adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`))
				yield* Effect.promise(() => adminClient.query(`DROP TABLE IF EXISTS ${tableIdentifier}`))
				yield* Effect.promise(() =>
					adminClient.query(`CREATE TABLE ${tableIdentifier} (id bigint PRIMARY KEY, value text NOT NULL)`),
				)
				yield* Effect.promise(() =>
					adminClient.query(`CREATE PUBLICATION ${publicationIdentifier} FOR TABLE ${tableIdentifier}`),
				)
				yield* Effect.addFinalizer(() =>
					Effect.all(
						[
							Effect.promise(() =>
								adminClient.query(
									`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
								),
							),
							Effect.promise(() =>
								adminClient.query(`DROP PUBLICATION IF EXISTS ${publicationIdentifier}`),
							),
							Effect.promise(() => adminClient.query(`DROP TABLE IF EXISTS ${tableIdentifier}`)),
						],
						{ concurrency: 1, discard: true },
					).pipe(
						Effect.catchCause((cause) =>
							Effect.logWarning('Failed to clean up pgoutput startup test', cause),
						),
					),
				)

				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				yield* operations.openReplicationSession()
				const slotPosition = yield* operations.ensureReplicationSlot({ slotName })
				yield* operations.pinOutputSettings()
				const result = yield* operations.startPgOutput({
					slotName,
					publicationName,
					startLsn: slotPosition.confirmedFlushLsn,
				})
				const xLogDataFiber = yield* operations
					.streamReplicationFrames({
						keepaliveIntervalMilliseconds: 100,
						initialSafeFlushLsn: slotPosition.confirmedFlushLsn,
					})
					.pipe(Stream.filter(ReplicationProtocolFrame.guards.XLogData), Stream.runHead, Effect.forkChild)
				yield* Effect.promise(() =>
					adminClient.query(`INSERT INTO ${tableIdentifier} (id, value) VALUES (1, 'streamed')`),
				)
				const frameOption = yield* Fiber.join(xLogDataFiber)
				const frame = Option.getOrThrowWith(
					frameOption,
					() => new Error('Expected PostgreSQL to emit XLogData'),
				)
				yield* operations.acknowledgeReplicationLsn({ safeFlushLsn: frame.serverWalEnd })
				yield* Effect.sleep('100 millis')

				expect(result).toBeUndefined()
				expect(frame.payload.byteLength).toBeGreaterThan(0)
				const slotRows = yield* Effect.promise(() =>
					adminClient.query(
						`SELECT active, confirmed_flush_lsn >= ${escapeLiteral(
							`${(frame.serverWalEnd >> 32n).toString(16)}/${(frame.serverWalEnd & 0xffff_ffffn).toString(16)}`,
						)}::pg_lsn AS acknowledged FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
					),
				)
				expect(slotRows.rows).toStrictEqual([{ active: true, acknowledged: true }])
			}),
		),
	)

	it.effect('rejects pgoutput startup when the durable slot is missing', ({ expect }) =>
		Effect.gen(function* () {
			const operations = yield* ReplicationOperations
			yield* operations.openReplicationSession()

			const failure = yield* Effect.flip(
				operations.startPgOutput({
					slotName: 'ampere_missing_startup_slot',
					publicationName: 'ampere_missing_startup_publication',
					startLsn: PostgresLsnValue.make(0n),
				}),
			)

			expect(failure).toStrictEqual(
				ReplicationOperationFailure.cases.SourceRejected.make({ reason: 'replication-prerequisite-invalid' }),
			)
		}).pipe(Effect.provide(ReplicationOperationsLayer)),
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
