import { Context, Layer, Schema } from 'effect'
import type { Effect } from 'effect'

import { decodePgOutputMessage } from './decoder.ts'
import type { DecodePgOutputMessageInput } from './decoder.ts'
import type { IncompatibleProtocolError, MessageDecodeError, UnsupportedMessageError } from './errors.ts'
import type { PgOutputMessage } from './messages.ts'

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
