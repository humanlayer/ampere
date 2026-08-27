import { Config } from 'effect'

export const replicationConnectionSettings = Config.all({
	databaseUrl: Config.redacted('DATABASE_URL'),
	connectTimeout: Config.duration('REPLICATION_CONNECT_TIMEOUT').pipe(Config.withDefault('10 seconds')),
	setupCommandTimeout: Config.duration('REPLICATION_SETUP_COMMAND_TIMEOUT').pipe(Config.withDefault('10 seconds')),
	slotLeaseWaitTimeout: Config.duration('REPLICATION_SLOT_LEASE_WAIT_TIMEOUT').pipe(Config.withDefault('5 seconds')),
	startPgOutputTimeout: Config.duration('REPLICATION_START_TIMEOUT').pipe(Config.withDefault('10 seconds')),
	shutdownTimeout: Config.duration('REPLICATION_SHUTDOWN_TIMEOUT').pipe(Config.withDefault('5 seconds')),
})
