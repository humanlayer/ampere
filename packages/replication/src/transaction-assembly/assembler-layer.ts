import type {
	CommitMessage,
	PgOutputMessage,
	PostgresOid,
	PostgresTransactionId,
	RelationMessage,
	TupleData,
} from '@ampere/schemas/wal'
import { Cause, Data, Effect, Layer, Match, Option, Queue, Ref, Stream } from 'effect'

import {
	CommittedChange,
	CommittedTransaction,
	RelationChanged,
	RelationIdentity,
	TruncatedRelation,
} from './assembled-events'
import type {
	AssembledPgOutputEvent,
	CommittedChange as CommittedChangeType,
	RelationIdentity as RelationIdentityType,
	TruncatedRelation as TruncatedRelationType,
} from './assembled-events'
import { PgOutputTransactionAssembler, TransactionAssemblyError } from './assembler-service'

const transactionAssemblyMailboxCapacity = 256

type TransactionAssemblyState = Data.TaggedEnum<{
	Idle: Record<never, never>
	Open: {
		readonly xid: PostgresTransactionId
		readonly changes: ReadonlyArray<CommittedChangeType>
		readonly affectedRelations: ReadonlyMap<PostgresOid, RelationIdentityType>
	}
}>

type OpenTransactionAssemblyState = Extract<TransactionAssemblyState, { readonly _tag: 'Open' }>

interface OptionalTupleIdentityFields {
	keyTuple?: TupleData
	oldTuple?: TupleData
}

const TransactionAssemblyState = Data.taggedEnum<TransactionAssemblyState>()

const transactionAssemblyError = (
	reason: 'begin-while-transaction-open' | 'message-while-idle' | 'unknown-relation',
): TransactionAssemblyError => new TransactionAssemblyError({ reason })

const relationIdentity = (relation: RelationMessage): RelationIdentityType =>
	RelationIdentity.make({ namespace: relation.namespace, name: relation.name })

const relationChanged = (relation: RelationMessage): AssembledPgOutputEvent =>
	RelationChanged.make({
		relationOid: relation.relationOid,
		namespace: relation.namespace,
		name: relation.name,
		replicaIdentity: relation.replicaIdentity,
		columns: relation.columns,
	})

const tupleIdentityFields = (
	keyTuple: TupleData | undefined,
	oldTuple: TupleData | undefined,
): OptionalTupleIdentityFields => {
	const fields: OptionalTupleIdentityFields = {}
	if (keyTuple !== undefined) {
		fields.keyTuple = keyTuple
	}
	if (oldTuple !== undefined) {
		fields.oldTuple = oldTuple
	}
	return fields
}

const addAffectedRelations = (
	affectedRelations: ReadonlyMap<PostgresOid, RelationIdentityType>,
	relations: ReadonlyArray<{ readonly relationOid: PostgresOid; readonly relation: RelationIdentityType }>,
): ReadonlyMap<PostgresOid, RelationIdentityType> => {
	const nextAffectedRelations = new Map(affectedRelations)
	for (const { relationOid, relation } of relations) {
		nextAffectedRelations.set(relationOid, relation)
	}
	return nextAffectedRelations
}

const requireOpenTransaction = (
	state: TransactionAssemblyState,
): Effect.Effect<OpenTransactionAssemblyState, TransactionAssemblyError> => {
	if (TransactionAssemblyState.$is('Open')(state)) {
		return Effect.succeed(state)
	}
	return Effect.fail(transactionAssemblyError('message-while-idle'))
}

const requireKnownRelation = (
	relationCache: ReadonlyMap<PostgresOid, RelationMessage>,
	relationOid: PostgresOid,
): Effect.Effect<RelationMessage, TransactionAssemblyError> =>
	Option.fromUndefinedOr(relationCache.get(relationOid)).pipe(
		Option.match({
			onNone: () => Effect.fail(transactionAssemblyError('unknown-relation')),
			onSome: Effect.succeed,
		}),
	)

const appendCommittedChange = (
	transactionState: Ref.Ref<TransactionAssemblyState>,
	transaction: OpenTransactionAssemblyState,
	change: CommittedChangeType,
	relations: ReadonlyArray<{ readonly relationOid: PostgresOid; readonly relation: RelationIdentityType }>,
): Effect.Effect<void> =>
	Ref.set(
		transactionState,
		TransactionAssemblyState.Open({
			xid: transaction.xid,
			changes: [...transaction.changes, change],
			affectedRelations: addAffectedRelations(transaction.affectedRelations, relations),
		}),
	)

const appendRelationChange = (
	transactionState: Ref.Ref<TransactionAssemblyState>,
	relationCache: Ref.Ref<ReadonlyMap<PostgresOid, RelationMessage>>,
	relationOid: PostgresOid,
	makeChange: (relation: RelationMessage) => CommittedChangeType,
): Effect.Effect<void, TransactionAssemblyError> =>
	Effect.gen(function* () {
		const transaction = yield* requireOpenTransaction(yield* Ref.get(transactionState))
		const relation = yield* requireKnownRelation(yield* Ref.get(relationCache), relationOid)
		const identity = relationIdentity(relation)
		yield* appendCommittedChange(transactionState, transaction, makeChange(relation), [
			{ relationOid, relation: identity },
		])
	})

const appendTruncateChange = (
	transactionState: Ref.Ref<TransactionAssemblyState>,
	relationCache: Ref.Ref<ReadonlyMap<PostgresOid, RelationMessage>>,
	optionFlags: number,
	relationOids: ReadonlyArray<PostgresOid>,
): Effect.Effect<void, TransactionAssemblyError> =>
	Effect.gen(function* () {
		const transaction = yield* requireOpenTransaction(yield* Ref.get(transactionState))
		const relations = yield* Ref.get(relationCache)
		const truncatedRelations: Array<TruncatedRelationType> = []
		for (const relationOid of relationOids) {
			const relation = yield* requireKnownRelation(relations, relationOid)
			truncatedRelations.push(TruncatedRelation.make({ relationOid, relation: relationIdentity(relation) }))
		}
		yield* appendCommittedChange(
			transactionState,
			transaction,
			CommittedChange.cases.Truncate.make({ optionFlags, relations: truncatedRelations }),
			truncatedRelations,
		)
	})

const completeTransaction = (
	transactionState: Ref.Ref<TransactionAssemblyState>,
	events: Queue.Queue<AssembledPgOutputEvent, TransactionAssemblyError>,
	commit: CommitMessage,
): Effect.Effect<void, TransactionAssemblyError> =>
	Effect.gen(function* () {
		const transaction = yield* requireOpenTransaction(yield* Ref.get(transactionState))
		yield* Queue.offer(
			events,
			CommittedTransaction.make({
				xid: transaction.xid,
				commitLsn: commit.commitLsn,
				endLsn: commit.endLsn,
				commitTimestampMicroseconds: commit.commitTimestampMicroseconds,
				changes: transaction.changes,
				affectedRelations: Array.from(transaction.affectedRelations.values()),
			}),
		)
		yield* Ref.set(transactionState, TransactionAssemblyState.Idle())
	})

const processPgOutputMessage = (
	transactionState: Ref.Ref<TransactionAssemblyState>,
	relationCache: Ref.Ref<ReadonlyMap<PostgresOid, RelationMessage>>,
	events: Queue.Queue<AssembledPgOutputEvent, TransactionAssemblyError>,
	message: PgOutputMessage,
): Effect.Effect<void, TransactionAssemblyError> =>
	Match.value(message).pipe(
		Match.tagsExhaustive({
			Begin: ({ xid }) =>
				Ref.get(transactionState).pipe(
					Effect.flatMap(
						TransactionAssemblyState.$match({
							Idle: () =>
								Ref.set(
									transactionState,
									TransactionAssemblyState.Open({ xid, changes: [], affectedRelations: new Map() }),
								),
							Open: () => Effect.fail(transactionAssemblyError('begin-while-transaction-open')),
						}),
					),
				),
			Commit: (commit) => completeTransaction(transactionState, events, commit),
			Relation: (relation) =>
				Ref.update(relationCache, (relations) => new Map(relations).set(relation.relationOid, relation)).pipe(
					Effect.andThen(Queue.offer(events, relationChanged(relation))),
					Effect.asVoid,
				),
			Insert: ({ relationOid, newTuple }) =>
				appendRelationChange(transactionState, relationCache, relationOid, (relation) =>
					CommittedChange.cases.Insert.make({
						relationOid,
						relation: relationIdentity(relation),
						newTuple,
					}),
				),
			Update: ({ relationOid, keyTuple, oldTuple, newTuple }) =>
				appendRelationChange(transactionState, relationCache, relationOid, (relation) =>
					CommittedChange.cases.Update.make({
						relationOid,
						relation: relationIdentity(relation),
						...tupleIdentityFields(keyTuple, oldTuple),
						newTuple,
					}),
				),
			Delete: ({ relationOid, keyTuple, oldTuple }) =>
				appendRelationChange(transactionState, relationCache, relationOid, (relation) =>
					CommittedChange.cases.Delete.make({
						relationOid,
						relation: relationIdentity(relation),
						...tupleIdentityFields(keyTuple, oldTuple),
					}),
				),
			Truncate: ({ optionFlags, relationOids }) =>
				appendTruncateChange(transactionState, relationCache, optionFlags, relationOids),
			Origin: () => Effect.void,
			Type: () => Effect.void,
			Message: () => Effect.void,
		}),
	)

export const PgOutputTransactionAssemblerLive = Layer.effect(
	PgOutputTransactionAssembler,
	Effect.gen(function* () {
		const mailbox = yield* Queue.bounded<PgOutputMessage, TransactionAssemblyError>(
			transactionAssemblyMailboxCapacity,
		)
		const events = yield* Queue.bounded<AssembledPgOutputEvent, TransactionAssemblyError>(
			transactionAssemblyMailboxCapacity,
		)
		const relationCache = yield* Ref.make<ReadonlyMap<PostgresOid, RelationMessage>>(new Map())
		const transactionState = yield* Ref.make<TransactionAssemblyState>(TransactionAssemblyState.Idle())
		const terminalFailure = yield* Ref.make<Option.Option<TransactionAssemblyError>>(Option.none())

		const failQueues = (failure: TransactionAssemblyError): Effect.Effect<void> =>
			Ref.set(terminalFailure, Option.some(failure)).pipe(
				Effect.andThen(Effect.sync(() => Queue.failCauseUnsafe(mailbox, Cause.fail(failure)))),
				Effect.andThen(Effect.sync(() => Queue.failCauseUnsafe(events, Cause.fail(failure)))),
			)

		yield* Effect.forever(
			Queue.take(mailbox).pipe(
				Effect.flatMap((message) => processPgOutputMessage(transactionState, relationCache, events, message)),
			),
		).pipe(
			Effect.tapError((failure) => Effect.logError('pgoutput transaction assembly failed', failure)),
			Effect.catch(failQueues),
			Effect.forkScoped,
		)

		return {
			send: Effect.fn('pgoutput_transaction_assembler.send')((input) =>
				Ref.get(terminalFailure).pipe(
					Effect.flatMap(
						Option.match({
							onNone: () => Queue.offer(mailbox, input.message).pipe(Effect.asVoid),
							onSome: Effect.fail,
						}),
					),
				),
			),
			events: Stream.fromQueue(events),
		}
	}),
)
