import { decodePgOutputMessage, Lsn } from '@ampere/schemas/wal'
import { describe, it } from '@effect/vitest'
import { Context, Effect, Fiber, HashSet, Layer, Option, Queue, Ref, Schema, Stream } from 'effect'
import { Client, escapeIdentifier, escapeLiteral } from 'pg'

import { ReplicationOperationsLayer } from '../../src/connection/layer'
import { ReplicationProtocolFrame, ReplicationRelation } from '../../src/connection/schemas'
import { ReplicationOperations } from '../../src/connection/service'
import { AssembledPgOutputEvent, PgOutputTransactionAssembler, PgOutputTransactionAssemblerLive } from '../../src/index'
import type { CommittedTransaction } from '../../src/index'

const testDatabaseUrl = 'postgres://postgres:postgres@localhost:55432/ampere'
const hourInMilliseconds = 3_600_000

const PostgresTransactionIdResult = Schema.Struct({
	xid: Schema.FiniteFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
})

const ConfirmedFlushLsnResult = Schema.Struct({
	confirmed_flush_lsn: Lsn,
})

const acquireAdminClient = Effect.acquireRelease(
	Effect.promise(async () => {
		const client = new Client({ connectionString: testDatabaseUrl })
		await client.connect()
		return client
	}),
	(client) =>
		Effect.promise(() => client.end()).pipe(
			Effect.catchCause((cause) => Effect.logWarning('Failed to close pgoutput decode test client', cause)),
		),
)

const runAdminQuery = (client: Client, queryText: string) => Effect.promise(() => client.query(queryText))

const captureTransactionXid = Effect.fn('pgoutput_decode_test.capture_transaction_xid')(function* ({
	client,
	queryText,
}: {
	client: Client
	queryText: string
}) {
	yield* runAdminQuery(client, 'BEGIN')
	yield* runAdminQuery(client, queryText)
	const xidResult = yield* runAdminQuery(client, `SELECT pg_current_xact_id()::xid::text AS xid`)
	yield* runAdminQuery(client, 'COMMIT')
	const decoded = yield* Schema.decodeUnknownEffect(PostgresTransactionIdResult)(xidResult.rows.at(0))
	return decoded.xid
})

describe('Live pgoutput decode and transaction assembly', () => {
	it.effect(
		'assembles insert, update, delete, and truncate before acknowledging the last committed transaction',
		({ expect }) =>
			Effect.gen(function* () {
				const fixtureId = crypto.randomUUID().replaceAll('-', '')
				const slotName = `ampere_pgoutput_decode_${fixtureId}`
				const publicationName = `ampere_pgoutput_decode_${fixtureId}`
				const tableName = `ampere_pgoutput_decode_${fixtureId}`
				const publicationIdentifier = escapeIdentifier(publicationName)
				const tableIdentifier = escapeIdentifier(tableName)
				const adminClient = yield* acquireAdminClient

				yield* runAdminQuery(
					adminClient,
					`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
				)
				yield* runAdminQuery(adminClient, `DROP PUBLICATION IF EXISTS ${publicationIdentifier}`)
				yield* runAdminQuery(adminClient, `DROP TABLE IF EXISTS ${tableIdentifier}`)
				yield* runAdminQuery(
					adminClient,
					`CREATE TABLE ${tableIdentifier} (id bigint PRIMARY KEY, value text NOT NULL)`,
				)
				yield* Effect.addFinalizer(() =>
					Effect.all(
						[
							runAdminQuery(
								adminClient,
								`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
							),
							runAdminQuery(adminClient, `DROP PUBLICATION IF EXISTS ${publicationIdentifier}`),
							runAdminQuery(adminClient, `DROP TABLE IF EXISTS ${tableIdentifier}`),
						],
						{ concurrency: 1, discard: true },
					).pipe(
						Effect.catchCause((cause) =>
							Effect.logWarning('Failed to clean up pgoutput decode test', cause),
						),
					),
				)

				const context = yield* Layer.build(Layer.fresh(ReplicationOperationsLayer))
				const operations = Context.get(context, ReplicationOperations)
				yield* operations.openReplicationSession()
				yield* operations.ensureReplicationContract({
					publicationName,
					relations: [
						ReplicationRelation.make({
							schemaName: 'public',
							tableName,
							partitionColumnNames: ['id'],
						}),
					],
					serverVersionNumber: 180_000,
				})
				const slotPosition = yield* operations.ensureReplicationSlot({ slotName })
				yield* operations.pinOutputSettings()
				yield* operations.startPgOutput({
					slotName,
					publicationName,
					startLsn: slotPosition.confirmedFlushLsn,
				})

				const committedXids = yield* Ref.make(HashSet.empty<number>())
				const committedTransactions = yield* Queue.unbounded<CommittedTransaction>()
				const assemblerContext = yield* Layer.build(Layer.fresh(PgOutputTransactionAssemblerLive))
				const assembler = Context.get(assemblerContext, PgOutputTransactionAssembler)
				const assembledTransactionConsumer = yield* assembler.events.pipe(
					Stream.filter(AssembledPgOutputEvent.guards.CommittedTransaction),
					Stream.runForEach((transaction) =>
						Ref.update(committedXids, HashSet.add(transaction.xid)).pipe(
							Effect.andThen(Queue.offer(committedTransactions, transaction)),
						),
					),
					Effect.forkChild,
				)

				const decodeConsumer = yield* operations
					.streamReplicationFrames({
						keepaliveIntervalMilliseconds: hourInMilliseconds,
						initialSafeFlushLsn: slotPosition.confirmedFlushLsn,
					})
					.pipe(
						Stream.filter(ReplicationProtocolFrame.guards.XLogData),
						Stream.mapEffect((frame) => decodePgOutputMessage({ bytes: frame.payload })),
						Stream.runForEach((message) => assembler.send({ message })),
						Effect.forkChild,
					)

				const insertXid = yield* captureTransactionXid({
					client: adminClient,
					queryText: `INSERT INTO ${tableIdentifier} (id, value) VALUES (1, 'inserted')`,
				})
				const updateXid = yield* captureTransactionXid({
					client: adminClient,
					queryText: `UPDATE ${tableIdentifier} SET value = 'updated' WHERE id = 1`,
				})
				const deleteXid = yield* captureTransactionXid({
					client: adminClient,
					queryText: `DELETE FROM ${tableIdentifier} WHERE id = 1`,
				})
				const truncateXid = yield* captureTransactionXid({
					client: adminClient,
					queryText: `TRUNCATE ${tableIdentifier}`,
				})
				const expectedXids = [insertXid, updateXid, deleteXid, truncateXid]

				const lastCommittedTransaction = yield* Ref.make<Option.Option<CommittedTransaction>>(Option.none())
				yield* Stream.fromQueue(committedTransactions).pipe(
					Stream.takeUntil((transaction) => transaction.xid === truncateXid),
					Stream.runForEach((transaction) => Ref.set(lastCommittedTransaction, Option.some(transaction))),
				)
				const lastCommitted = Option.getOrThrowWith(
					yield* Ref.get(lastCommittedTransaction),
					() => new Error('Expected the truncate transaction to be assembled.'),
				)
				yield* operations.acknowledgeReplicationLsn({ safeFlushLsn: lastCommitted.endLsn })
				const confirmedFlushLsnResult = yield* runAdminQuery(
					adminClient,
					`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = ${escapeLiteral(slotName)}`,
				)
				const confirmedFlushLsn = yield* Schema.decodeUnknownEffect(ConfirmedFlushLsnResult)(
					confirmedFlushLsnResult.rows.at(0),
				)
				expect(confirmedFlushLsn.confirmed_flush_lsn).toBeGreaterThanOrEqual(lastCommitted.endLsn)
				yield* Fiber.interrupt(decodeConsumer)
				yield* Fiber.interrupt(assembledTransactionConsumer)

				const committed = yield* Ref.get(committedXids)
				expect(expectedXids.every((xid) => HashSet.has(committed, xid))).toBe(true)
			}),
	)
})
