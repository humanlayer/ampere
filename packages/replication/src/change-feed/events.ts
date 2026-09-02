import { PostgresLsnValue } from '@ampere/schemas/wal'
import { Schema } from 'effect'

import { CommittedTransaction } from '../transaction-assembly/assembled-events'

export const ChangeFeedEvent = Schema.TaggedUnion({
	CommittedChangeBatch: {
		fromLsn: PostgresLsnValue,
		toLsn: PostgresLsnValue,
		transaction: CommittedTransaction,
	},
	StreamWatermark: {
		lsn: PostgresLsnValue,
	},
})
export type ChangeFeedEvent = typeof ChangeFeedEvent.Type

export const CommittedChangeBatch = ChangeFeedEvent.cases.CommittedChangeBatch
export type CommittedChangeBatch = typeof CommittedChangeBatch.Type

export const StreamWatermark = ChangeFeedEvent.cases.StreamWatermark
export type StreamWatermark = typeof StreamWatermark.Type

export const createCommittedChangeBatch = ({
	fromLsn,
	transaction,
}: {
	readonly fromLsn: PostgresLsnValue
	readonly transaction: CommittedTransaction
}): CommittedChangeBatch =>
	CommittedChangeBatch.make({
		fromLsn,
		toLsn: transaction.endLsn,
		transaction,
	})
