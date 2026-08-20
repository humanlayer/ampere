import { describe, it } from '@effect/vitest'
import { Effect, Schema, SchemaGetter, SchemaIssue } from 'effect'

const ArrayToString = Schema.Uint8Array.pipe(
	Schema.decodeTo(Schema.String, {
		encode: SchemaGetter.forbidden(() => 'Encoding a WAL message is not supported.'),
		decode: SchemaGetter.transformOrFail((arr) =>
			Effect.gen(function* () {
				let s = ''
				for (const byte of arr) {
					if (byte < 32 || byte > 126) {
						return yield* Effect.fail(
							new SchemaIssue.InvalidValue({ message: `${byte} is not a valid ascii character` }),
						)
					}
					s += String.fromCharCode(byte)
				}
				return yield* Effect.succeed(s)
			}),
		),
	}),
)

describe('Schemas can decode from a buffer', () => {
	it.effect('Sanity Check ', ({ expect }) =>
		Effect.gen(function* () {
			const s = yield* Schema.decodeUnknownEffect(ArrayToString)(new Uint8Array([97, 98, 99, 100]))
			expect(s).toBe('abcd')
		}),
	)
})
