import * as PgClient from '@effect/sql-pg/PgClient'
import { Config, Redacted } from 'effect'

/**
 * Connection string for the local dev/test PostgreSQL from docker-compose.yaml.
 * Override with `DATABASE_URL`.
 */
const databaseUrl = Config.redacted('DATABASE_URL').pipe(
	Config.withDefault(Redacted.make('postgres://postgres:postgres@localhost:55432/ampere')),
)

/**
 * PostgreSQL client layer configured from the environment. Provides both
 * `PgClient` and the generic `SqlClient`.
 */
export const PgLive = PgClient.layerConfig({
	url: databaseUrl,
	applicationName: Config.succeed('ampere'),
})
