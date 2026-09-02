import { Context, Schema } from 'effect'
import type { Effect, Stream } from 'effect'

import { CommittedChangeBatch } from './events'
import type { CommittedChangeBatch as CommittedChangeBatchType } from './events'

export const PublishCommittedChangeBatchInput = Schema.Struct({
	batch: CommittedChangeBatch,
})
export interface PublishCommittedChangeBatchInput extends Schema.Schema.Type<typeof PublishCommittedChangeBatchInput> {}

export const ChangeFeedUnavailableReason = Schema.Literals(['closed', 'unavailable'])
export type ChangeFeedUnavailableReason = typeof ChangeFeedUnavailableReason.Type

export class ChangeFeedUnavailable extends Schema.TaggedError<ChangeFeedUnavailable>()('ChangeFeedUnavailable', {
	reason: ChangeFeedUnavailableReason,
}) {}

export interface ChangeFeedService {
	readonly publishCommittedChangeBatch: (
		input: PublishCommittedChangeBatchInput,
	) => Effect.Effect<void, ChangeFeedUnavailable>

	readonly committedChangeBatches: Stream.Stream<CommittedChangeBatchType, ChangeFeedUnavailable>
}

export class ChangeFeedApi extends Context.Service<ChangeFeedApi, ChangeFeedService>()(
	'@ampere/replication/ChangeFeedApi',
) {}
