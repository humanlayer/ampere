import { Context, Layer } from 'effect'
import type { Effect, Schema } from 'effect'

import { decodePgOutputMessage } from './decoder'
import type { DecodePgOutputMessageInput } from './decoder'
import type { IncompatibleProtocolError, MessageDecodeError, UnsupportedMessageError } from './errors'
import type { PgOutputMessage } from './messages'

export type DecodePgOutputMessageError =
	| MessageDecodeError
	| IncompatibleProtocolError
	| UnsupportedMessageError
	| Schema.SchemaError

export interface PgOutputMessageDecoderApi {
	readonly decodeMessage: (
		input: DecodePgOutputMessageInput,
	) => Effect.Effect<PgOutputMessage, DecodePgOutputMessageError>
}

export class PgOutputMessageDecoder extends Context.Service<PgOutputMessageDecoder, PgOutputMessageDecoderApi>()(
	'@ampere/schemas/wal/PgOutputMessageDecoder',
) {}

export const PgOutputMessageDecoderLive = Layer.succeed(PgOutputMessageDecoder, {
	decodeMessage: decodePgOutputMessage,
})
