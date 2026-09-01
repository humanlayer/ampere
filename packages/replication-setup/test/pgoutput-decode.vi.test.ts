import { decodePgOutputMessage } from '@ampere/schemas/wal'
import { describe, it } from '@effect/vitest'
import { Context, Effect, Fiber, HashSet, Layer, Match, Queue, Ref, Schema, Stream } from 'effect'
import { Client, escapeIdentifier, escapeLiteral } from 'pg'

import { ReplicationOperationsLayer } from '../src/layer.ts'
import { ReplicationProtocolFrame, ReplicationRelation } from '../src/schemas.ts'
import { ReplicationOperations } from '../src/service.ts'

const testDatabaseUrl = 'postgres://postgres:postgres@localhost:55432/ampere'
const hourInMilliseconds = 3_600_000

const PostgresTransactionIdResult = Schema.Struct({
	xid: Schema.FiniteFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
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

describe('Live pgoutput decode', () => {
	it.effect('decodes every XLogData payload from insert, update, delete, and truncate', ({ expect }) =>
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
					Effect.catchCause((cause) => Effect.logWarning('Failed to clean up pgoutput decode test', cause)),
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

			const currentBeginXid = yield* Ref.make<number | undefined>(undefined)
			const committedXids = yield* Ref.make(HashSet.empty<number>())
			const committedXidNotifications = yield* Queue.unbounded<number>()

			const decodeConsumer = yield* operations
				.streamReplicationFrames({
					keepaliveIntervalMilliseconds: hourInMilliseconds,
					initialSafeFlushLsn: slotPosition.confirmedFlushLsn,
				})
				.pipe(
					Stream.filter(ReplicationProtocolFrame.guards.XLogData),
					Stream.mapEffect((frame) => decodePgOutputMessage({ bytes: frame.payload })),
					Stream.runForEach((message) =>
						Match.value(message).pipe(
							Match.tag('Begin', ({ xid }) => Ref.set(currentBeginXid, xid)),
							Match.tag('Commit', () =>
								Effect.gen(function* () {
									const xid = yield* Ref.get(currentBeginXid)
									if (xid === undefined) {
										return
									}
									yield* Ref.update(committedXids, HashSet.add(xid))
									yield* Queue.offer(committedXidNotifications, xid)
								}),
							),
							Match.orElse(() => Effect.void),
						),
					),
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

			yield* Stream.fromQueue(committedXidNotifications).pipe(
				Stream.takeUntil((xid) => xid === truncateXid),
				Stream.runDrain,
			)
			yield* Fiber.interrupt(decodeConsumer)

			const committed = yield* Ref.get(committedXids)
			expect(expectedXids.every((xid) => HashSet.has(committed, xid))).toBe(true)
		}),
	)
})
