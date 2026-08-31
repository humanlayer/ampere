import * as PgClient from '@effect/sql-pg/PgClient'
import { Config, Redacted } from 'effect'

const databaseUrl = Config.redacted('DATABASE_URL').pipe(
	Config.withDefault(Redacted.make('postgres://postgres:postgres@localhost:55432/ampere')),
)

export const PgLive = PgClient.layerConfig({
	url: databaseUrl,
	applicationName: Config.succeed('ampere'),
})
