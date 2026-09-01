import {
	PostgresLsnValue,
	PostgresOid,
	PostgresTransactionId,
	RelationColumn,
	ReplicaIdentity,
	TupleData,
} from '@ampere/schemas/wal'
import { Schema } from 'effect'

export const RelationIdentity = Schema.Struct({
	namespace: Schema.String,
	name: Schema.String,
})
export interface RelationIdentity extends Schema.Schema.Type<typeof RelationIdentity> {}

export const TruncatedRelation = Schema.Struct({
	relationOid: PostgresOid,
	relation: RelationIdentity,
})
export interface TruncatedRelation extends Schema.Schema.Type<typeof TruncatedRelation> {}

export const CommittedChange = Schema.TaggedUnion({
	Insert: {
		relationOid: PostgresOid,
		relation: RelationIdentity,
		newTuple: TupleData,
	},
	Update: {
		relationOid: PostgresOid,
		relation: RelationIdentity,
		keyTuple: Schema.optionalKey(TupleData),
		oldTuple: Schema.optionalKey(TupleData),
		newTuple: TupleData,
	},
	Delete: {
		relationOid: PostgresOid,
		relation: RelationIdentity,
		keyTuple: Schema.optionalKey(TupleData),
		oldTuple: Schema.optionalKey(TupleData),
	},
	Truncate: {
		optionFlags: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(255))),
		relations: Schema.Array(TruncatedRelation),
	},
})
export type CommittedChange = typeof CommittedChange.Type

export const AssembledPgOutputEvent = Schema.TaggedUnion({
	RelationChanged: {
		relationOid: PostgresOid,
		namespace: Schema.String,
		name: Schema.String,
		replicaIdentity: ReplicaIdentity,
		columns: Schema.Array(RelationColumn),
	},
	CommittedTransaction: {
		xid: PostgresTransactionId,
		commitLsn: PostgresLsnValue,
		endLsn: PostgresLsnValue,
		commitTimestampMicroseconds: Schema.BigInt,
		changes: Schema.Array(CommittedChange),
		affectedRelations: Schema.Array(RelationIdentity),
	},
})
export type AssembledPgOutputEvent = typeof AssembledPgOutputEvent.Type

export const RelationChanged = AssembledPgOutputEvent.cases.RelationChanged
export const CommittedTransaction = AssembledPgOutputEvent.cases.CommittedTransaction
export type RelationChanged = typeof RelationChanged.Type
export type CommittedTransaction = typeof CommittedTransaction.Type
