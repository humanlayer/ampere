import { PgOutputMessage } from '@ampere/schemas/wal'
import { Context, Schema } from 'effect'
import type { Effect, Stream } from 'effect'

import type { AssembledPgOutputEvent } from './assembled-events'

export const SendPgOutputMessageInput = Schema.Struct({
	message: PgOutputMessage,
})
export interface SendPgOutputMessageInput extends Schema.Schema.Type<typeof SendPgOutputMessageInput> {}

export const TransactionAssemblyFailureReason = Schema.Literals([
	'begin-while-transaction-open',
	'message-while-idle',
	'unknown-relation',
])
export type TransactionAssemblyFailureReason = typeof TransactionAssemblyFailureReason.Type

export class TransactionAssemblyError extends Schema.TaggedError<TransactionAssemblyError>()(
	'TransactionAssemblyError',
	{
		reason: TransactionAssemblyFailureReason,
	},
) {}

export interface PgOutputTransactionAssemblerApi {
	readonly send: (input: SendPgOutputMessageInput) => Effect.Effect<void, TransactionAssemblyError>

	readonly events: Stream.Stream<AssembledPgOutputEvent, TransactionAssemblyError>
}

export class PgOutputTransactionAssembler extends Context.Service<
	PgOutputTransactionAssembler,
	PgOutputTransactionAssemblerApi
>()('@ampere/replication/PgOutputTransactionAssembler') {}
