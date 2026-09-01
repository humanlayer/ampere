import { Schema } from 'effect'

export const PostgresOid = Schema.Int.pipe(
	Schema.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(0xffff_ffff)),
)
export type PostgresOid = typeof PostgresOid.Type
