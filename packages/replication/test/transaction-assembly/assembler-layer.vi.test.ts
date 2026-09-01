import {
	BeginMessage,
	CommitMessage,
	DeleteMessage,
	InsertMessage,
	PostgresLsnValue,
	RelationColumn,
	RelationMessage,
	TruncateMessage,
	TupleCell,
	UpdateMessage,
} from '@ampere/schemas/wal'
import { describe, it } from '@effect/vitest'
import { Context, Effect, Fiber, Layer, Ref, Stream } from 'effect'

import {
	CommittedChange,
	CommittedTransaction,
	PgOutputTransactionAssembler,
	PgOutputTransactionAssemblerLive,
	RelationChanged,
	RelationIdentity,
	TransactionAssemblyError,
	TruncatedRelation,
} from '../../src/index'
import type { AssembledPgOutputEvent } from '../../src/index'

const textEncoder = new TextEncoder()
const relationOid = 24_576
const transactionXid = 736

const textCell = (value: string) => TupleCell.cases.Text.make({ bytes: textEncoder.encode(value) })

const relation = RelationMessage.make({
	relationOid,
	namespace: 'public',
	name: 'todos',
	replicaIdentity: 'all-columns',
	columns: [
		RelationColumn.make({ isKey: true, name: 'id', typeOid: 23, typeModifier: -1 }),
		RelationColumn.make({ isKey: false, name: 'title', typeOid: 25, typeModifier: -1 }),
	],
})

const relationIdentity = RelationIdentity.make({ namespace: relation.namespace, name: relation.name })

const begin = BeginMessage.make({
	finalLsn: PostgresLsnValue.make(10n),
	commitTimestampMicroseconds: 11n,
	xid: transactionXid,
})

const commit = CommitMessage.make({
	flags: 0,
	commitLsn: PostgresLsnValue.make(12n),
	endLsn: PostgresLsnValue.make(13n),
	commitTimestampMicroseconds: 14n,
})

const makeAssembler = Effect.gen(function* () {
	const context = yield* Layer.build(PgOutputTransactionAssemblerLive)
	return Context.get(context, PgOutputTransactionAssembler)
})

describe('pgoutput transaction assembler', () => {
	it.effect('emits relation metadata and a complete transaction after Commit', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const assembler = yield* makeAssembler
				const events = yield* Ref.make<ReadonlyArray<AssembledPgOutputEvent>>([])
				const eventConsumer = yield* assembler.events.pipe(
					Stream.take(2),
					Stream.runForEach((event) => Ref.update(events, (collected) => [...collected, event])),
					Effect.forkChild,
				)

				yield* assembler.send({ message: begin })
				yield* assembler.send({ message: relation })
				yield* assembler.send({
					message: InsertMessage.make({ relationOid, newTuple: [textCell('1'), textCell('first')] }),
				})
				yield* assembler.send({
					message: UpdateMessage.make({
						relationOid,
						oldTuple: [textCell('1'), textCell('first')],
						newTuple: [textCell('1'), textCell('second')],
					}),
				})
				yield* assembler.send({
					message: DeleteMessage.make({ relationOid, oldTuple: [textCell('1'), textCell('second')] }),
				})
				yield* assembler.send({
					message: TruncateMessage.make({ optionFlags: 0, relationOids: [relationOid] }),
				})
				yield* assembler.send({ message: commit })
				yield* Fiber.join(eventConsumer)

				expect(yield* Ref.get(events)).toStrictEqual([
					RelationChanged.make({
						relationOid,
						namespace: 'public',
						name: 'todos',
						replicaIdentity: 'all-columns',
						columns: relation.columns,
					}),
					CommittedTransaction.make({
						xid: transactionXid,
						commitLsn: PostgresLsnValue.make(12n),
						endLsn: PostgresLsnValue.make(13n),
						commitTimestampMicroseconds: 14n,
						changes: [
							CommittedChange.cases.Insert.make({
								relationOid,
								relation: relationIdentity,
								newTuple: [textCell('1'), textCell('first')],
							}),
							CommittedChange.cases.Update.make({
								relationOid,
								relation: relationIdentity,
								oldTuple: [textCell('1'), textCell('first')],
								newTuple: [textCell('1'), textCell('second')],
							}),
							CommittedChange.cases.Delete.make({
								relationOid,
								relation: relationIdentity,
								oldTuple: [textCell('1'), textCell('second')],
							}),
							CommittedChange.cases.Truncate.make({
								optionFlags: 0,
								relations: [TruncatedRelation.make({ relationOid, relation: relationIdentity })],
							}),
						],
						affectedRelations: [relationIdentity],
					}),
				])
			}),
		),
	)

	it.effect('fails the actor when Begin arrives while a transaction is open', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const assembler = yield* makeAssembler
				const failure = yield* assembler.events.pipe(Stream.runDrain, Effect.flip, Effect.forkChild)

				yield* assembler.send({ message: begin })
				yield* assembler.send({ message: begin })

				expect(yield* Fiber.join(failure)).toStrictEqual(
					new TransactionAssemblyError({ reason: 'begin-while-transaction-open' }),
				)
				expect(yield* Effect.flip(assembler.send({ message: begin }))).toStrictEqual(
					new TransactionAssemblyError({ reason: 'begin-while-transaction-open' }),
				)
			}),
		),
	)

	it.effect('fails the actor when DML arrives while idle', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const assembler = yield* makeAssembler
				const failure = yield* assembler.events.pipe(Stream.runDrain, Effect.flip, Effect.forkChild)

				yield* assembler.send({
					message: InsertMessage.make({ relationOid, newTuple: [textCell('1'), textCell('first')] }),
				})

				expect(yield* Fiber.join(failure)).toStrictEqual(
					new TransactionAssemblyError({ reason: 'message-while-idle' }),
				)
			}),
		),
	)

	it.effect('fails the actor when DML references an unseen relation', ({ expect }) =>
		Effect.scoped(
			Effect.gen(function* () {
				const assembler = yield* makeAssembler
				const failure = yield* assembler.events.pipe(Stream.runDrain, Effect.flip, Effect.forkChild)

				yield* assembler.send({ message: begin })
				yield* assembler.send({
					message: InsertMessage.make({ relationOid, newTuple: [textCell('1'), textCell('first')] }),
				})

				expect(yield* Fiber.join(failure)).toStrictEqual(
					new TransactionAssemblyError({ reason: 'unknown-relation' }),
				)
			}),
		),
	)
})
