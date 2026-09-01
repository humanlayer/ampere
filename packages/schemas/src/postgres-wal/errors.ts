import { Effect, Match, Option, Schema, SchemaIssue } from 'effect'

export const PgOutputMessageTypeByte = Schema.Int.pipe(
	Schema.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(255)),
)
export type PgOutputMessageTypeByte = typeof PgOutputMessageTypeByte.Type

export const PgOutputV1MessageTypeNameSchema = Schema.Literals([
	'Begin',
	'Commit',
	'Origin',
	'Relation',
	'Type',
	'Insert',
	'Update',
	'Delete',
	'Truncate',
	'Message',
])

export const LaterPgOutputProtocolMessageTypeNameSchema = Schema.Literals([
	'StreamStart',
	'StreamStop',
	'StreamCommit',
	'StreamAbort',
	'BeginPrepare',
	'Prepare',
	'CommitPrepared',
	'RollbackPrepared',
	'StreamPrepare',
])

export const MessageDecodeFailureReason = Schema.Literals([
	'empty-payload',
	'truncated-message',
	'trailing-bytes',
	'known-v1-type-not-yet-decoded',
])
export type MessageDecodeFailureReason = typeof MessageDecodeFailureReason.Type

export const PgOutputDecodeFailure = Schema.TaggedUnion({
	MessageDecodeError: {
		reason: MessageDecodeFailureReason,
		typeByte: Schema.optionalKey(PgOutputMessageTypeByte),
		messageTypeName: Schema.optionalKey(PgOutputV1MessageTypeNameSchema),
	},
	IncompatibleProtocolError: {
		typeByte: PgOutputMessageTypeByte,
		messageTypeName: LaterPgOutputProtocolMessageTypeNameSchema,
	},
	UnsupportedMessageError: {
		typeByte: PgOutputMessageTypeByte,
		rawPayload: Schema.Uint8Array,
	},
})
export type PgOutputDecodeFailure = typeof PgOutputDecodeFailure.Type

export class MessageDecodeError extends Schema.TaggedError<MessageDecodeError>()('MessageDecodeError', {
	reason: MessageDecodeFailureReason,
	typeByte: Schema.optionalKey(PgOutputMessageTypeByte),
	messageTypeName: Schema.optionalKey(PgOutputV1MessageTypeNameSchema),
}) {}

export class IncompatibleProtocolError extends Schema.TaggedError<IncompatibleProtocolError>()(
	'IncompatibleProtocolError',
	{
		typeByte: PgOutputMessageTypeByte,
		messageTypeName: LaterPgOutputProtocolMessageTypeNameSchema,
	},
) {}

export class UnsupportedMessageError extends Schema.TaggedError<UnsupportedMessageError>()('UnsupportedMessageError', {
	typeByte: PgOutputMessageTypeByte,
	rawPayload: Schema.Uint8Array,
}) {}

const pgOutputDecodeFailureAnnotationKey = 'pgOutputDecodeFailure'

export const failPgOutputDecode = (failure: PgOutputDecodeFailure, message: string) =>
	Effect.fail(
		new SchemaIssue.InvalidValue({
			message,
			[pgOutputDecodeFailureAnnotationKey]: failure,
		}),
	)

const findPgOutputDecodeFailureAnnotation = (issue: SchemaIssue.Issue): unknown =>
	Match.value(issue).pipe(
		Match.tagsExhaustive({
			InvalidValue: ({ annotations }) => annotations?.[pgOutputDecodeFailureAnnotationKey],
			InvalidType: () => undefined,
			MissingKey: () => undefined,
			UnexpectedKey: () => undefined,
			Forbidden: () => undefined,
			OneOf: () => undefined,
			Filter: ({ issue: nestedIssue }) => findPgOutputDecodeFailureAnnotation(nestedIssue),
			Encoding: ({ issue: nestedIssue }) => findPgOutputDecodeFailureAnnotation(nestedIssue),
			Pointer: ({ issue: nestedIssue }) => findPgOutputDecodeFailureAnnotation(nestedIssue),
			Composite: ({ issues }) => {
				for (const nestedIssue of issues) {
					const found = findPgOutputDecodeFailureAnnotation(nestedIssue)
					if (found !== undefined) {
						return found
					}
				}
				return undefined
			},
			AnyOf: ({ issues }) => {
				for (const nestedIssue of issues) {
					const found = findPgOutputDecodeFailureAnnotation(nestedIssue)
					if (found !== undefined) {
						return found
					}
				}
				return undefined
			},
		}),
	)

const pgOutputDecodeFailureToError = Match.type<PgOutputDecodeFailure>().pipe(
	Match.tagsExhaustive({
		MessageDecodeError: (failure) => new MessageDecodeError(failure),
		IncompatibleProtocolError: (failure) => new IncompatibleProtocolError(failure),
		UnsupportedMessageError: (failure) => new UnsupportedMessageError(failure),
	}),
)

export const mapPgOutputSchemaError = (
	schemaError: Schema.SchemaError,
): Schema.SchemaError | MessageDecodeError | IncompatibleProtocolError | UnsupportedMessageError => {
	const decodedFailure = Schema.decodeUnknownOption(PgOutputDecodeFailure)(
		findPgOutputDecodeFailureAnnotation(schemaError.issue),
	)
	if (Option.isNone(decodedFailure)) {
		return schemaError
	}
	return pgOutputDecodeFailureToError(decodedFailure.value)
}
