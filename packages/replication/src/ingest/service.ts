import { Context, Schema } from 'effect'
import type { Effect } from 'effect'

import type { ChangeFeedApi } from '../change-feed/service'
import type { ReplicationOperationFailure } from '../connection/schemas'
import { PositiveInteger, PostgresLsnValue } from '../connection/schemas'
import type { ReplicationOperations } from '../connection/service'
import type { PgOutputTransactionAssembler } from '../transaction-assembly/assembler-service'

export const ConsumeReplicationSessionInput = Schema.Struct({
	keepaliveIntervalMilliseconds: PositiveInteger,
	initialSafeFlushLsn: PostgresLsnValue,
})
export interface ConsumeReplicationSessionInput extends Schema.Schema.Type<typeof ConsumeReplicationSessionInput> {}

export interface ReplicationIngestService {
	readonly consumeReplicationSession: (
		input: ConsumeReplicationSessionInput,
	) => Effect.Effect<
		void,
		typeof ReplicationOperationFailure.Type,
		ReplicationOperations | ChangeFeedApi | PgOutputTransactionAssembler
	>
}

export class ReplicationIngestApi extends Context.Service<ReplicationIngestApi, ReplicationIngestService>()(
	'@ampere/replication/ReplicationIngestApi',
) {}
