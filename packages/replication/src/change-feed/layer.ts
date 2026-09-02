import { Effect, Layer, Queue, Stream } from 'effect'

import type { CommittedChangeBatch } from './events'
import { ChangeFeedApi, ChangeFeedUnavailable } from './service'

const inMemoryChangeFeedCapacity = 256

export const ChangeFeedApiInMemory = Layer.effect(
	ChangeFeedApi,
	Effect.gen(function* () {
		const batches = yield* Queue.bounded<CommittedChangeBatch, ChangeFeedUnavailable>(inMemoryChangeFeedCapacity)

		return ChangeFeedApi.of({
			publishCommittedChangeBatch: Effect.fn('change_feed.publish_committed_change_batch')((input) =>
				Queue.offer(batches, input.batch).pipe(
					Effect.flatMap((wasAccepted) =>
						wasAccepted ? Effect.void : Effect.fail(new ChangeFeedUnavailable({ reason: 'closed' })),
					),
				),
			),
			committedChangeBatches: Stream.fromQueue(batches),
		})
	}),
)
