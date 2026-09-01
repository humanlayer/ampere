import { Schema, SchemaGetter } from 'effect'

const maximumPostgresLsn = 0xffff_ffff_ffff_ffffn

export const PostgresLsnValue = Schema.BigInt.pipe(
	Schema.check(Schema.isGreaterThanOrEqualToBigInt(0n), Schema.isLessThanOrEqualToBigInt(maximumPostgresLsn)),
	Schema.brand('PostgresLsn'),
)
export type PostgresLsnValue = typeof PostgresLsnValue.Type

const parsePostgresLsnText = (text: string): bigint => {
	const separatorIndex = text.indexOf('/')
	const segment = BigInt(`0x${text.slice(0, separatorIndex)}`)
	const offset = BigInt(`0x${text.slice(separatorIndex + 1)}`)
	return (segment << 32n) | offset
}

const formatPostgresLsnText = (lsn: bigint): string => {
	const segment = (lsn >> 32n).toString(16).toUpperCase()
	const offset = (lsn & 0xffff_ffffn).toString(16).toUpperCase()
	return `${segment}/${offset}`
}

export const Lsn = Schema.String.pipe(
	Schema.check(Schema.isPattern(/^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/)),
	Schema.decodeTo(PostgresLsnValue, {
		decode: SchemaGetter.transform(parsePostgresLsnText),
		encode: SchemaGetter.transform(formatPostgresLsnText),
	}),
)
