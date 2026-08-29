import { Effect, Exit, Option, Schema } from 'effect'
import { escapeIdentifier, escapeLiteral } from 'pg'

import {
	PublicationConfigurationResult,
	ReplicationContractAlreadyConfigured,
	ReplicationContractPermissionDenied,
	ReplicationContractQueryFailed,
	ReplicationOperationFailure,
	ReplicationRelationConfigurationResult,
} from './schemas.ts'
import type {
	EnsureReplicationContractInput,
	ReplicationContractQueryOperation,
	ReplicationRelation,
} from './schemas.ts'

export interface ReplicationContractQueryResult {
	readonly rows: ReadonlyArray<unknown>
}

export interface ReplicationContractConnection {
	readonly query: (queryText: string) => Promise<ReplicationContractQueryResult>
}

interface ExecuteReplicationContractQueryInput {
	readonly connection: ReplicationContractConnection
	readonly operation: typeof ReplicationContractQueryOperation.Type
	readonly queryText: string
}

interface ReplicationPublicationInput {
	readonly connection: ReplicationContractConnection
	readonly publicationName: string
}

interface ReplicationRelationConnectionInput {
	readonly connection: ReplicationContractConnection
	readonly relation: typeof ReplicationRelation.Type
}

type ReplicationRelationInput = ReplicationPublicationInput & ReplicationRelationConnectionInput

export interface EnsureReplicationContractEffectInput {
	readonly connection: ReplicationContractConnection
	readonly contract: typeof EnsureReplicationContractInput.Type
}

const PostgresErrorCode = Schema.Struct({ code: Schema.String })
export const supportedPostgresMajorVersion = 18

export const compareReplicationRelations = (
	left: typeof ReplicationRelation.Type,
	right: typeof ReplicationRelation.Type,
): number => {
	if (left.schemaName !== right.schemaName) {
		return left.schemaName < right.schemaName ? -1 : 1
	}
	if (left.tableName === right.tableName) {
		return 0
	}
	return left.tableName < right.tableName ? -1 : 1
}

export const classifyReplicationContractQueryError = (
	cause: unknown,
	operation: typeof ReplicationContractQueryOperation.Type,
): ReplicationContractAlreadyConfigured | ReplicationContractPermissionDenied | ReplicationContractQueryFailed => {
	const postgresError = Schema.decodeUnknownOption(PostgresErrorCode)(cause)
	if (Option.isNone(postgresError)) {
		return new ReplicationContractQueryFailed({ operation, cause })
	}
	if (postgresError.value.code === '42710') {
		return new ReplicationContractAlreadyConfigured({ operation, cause })
	}
	if (postgresError.value.code === '42501') {
		return new ReplicationContractPermissionDenied({ operation, cause })
	}
	return new ReplicationContractQueryFailed({ operation, cause })
}

const executeReplicationContractQuery = Effect.fn('replication_contract.execute_query')(
	({ connection, operation, queryText }: ExecuteReplicationContractQueryInput) =>
		Effect.tryPromise({
			try: () => connection.query(queryText),
			catch: (cause) => classifyReplicationContractQueryError(cause, operation),
		}).pipe(
			Effect.tapErrorTag('ReplicationContractPermissionDenied', (error) =>
				Effect.logError('Replication contract query failed', error).pipe(Effect.annotateLogs({ operation })),
			),
			Effect.tapErrorTag('ReplicationContractQueryFailed', (error) =>
				Effect.logError('Replication contract query failed', error).pipe(Effect.annotateLogs({ operation })),
			),
		),
)

const setupCommandFailed = ReplicationOperationFailure.cases.SessionUnavailable.make({
	reason: 'setup-command-failed',
})

const publicationPermissionDenied = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'publication-permission-denied',
})

const replicaIdentityPermissionDenied = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'replica-identity-permission-denied',
})

const replicationPrerequisiteInvalid = ReplicationOperationFailure.cases.SourceRejected.make({
	reason: 'replication-prerequisite-invalid',
})

export const ensureReplicationPublication = Effect.fn('replication_contract.ensure_publication')(function* ({
	connection,
	publicationName,
}: ReplicationPublicationInput) {
	yield* executeReplicationContractQuery({
		connection,
		operation: 'create-publication',
		queryText: `CREATE PUBLICATION ${escapeIdentifier(publicationName)} WITH (publish = 'insert, update, delete, truncate')`,
	}).pipe(
		Effect.asVoid,
		Effect.catchTags({
			ReplicationContractAlreadyConfigured: () => Effect.void,
			ReplicationContractPermissionDenied: () => Effect.fail(publicationPermissionDenied),
			ReplicationContractQueryFailed: () => Effect.fail(setupCommandFailed),
		}),
	)

	const result = yield* executeReplicationContractQuery({
		connection,
		operation: 'read-publication-configuration',
		queryText: `SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
			FROM pg_publication
			WHERE pubname = ${escapeLiteral(publicationName)}`,
	}).pipe(
		Effect.catchTags({
			ReplicationContractAlreadyConfigured: () => Effect.fail(setupCommandFailed),
			ReplicationContractPermissionDenied: () => Effect.fail(publicationPermissionDenied),
			ReplicationContractQueryFailed: () => Effect.fail(setupCommandFailed),
		}),
	)

	const publicationConfiguration = yield* Schema.decodeUnknownEffect(PublicationConfigurationResult)(
		result.rows.at(0),
	).pipe(
		Effect.tapError((error) => Effect.logError('Invalid replication publication configuration response', error)),
		Effect.mapError(() => replicationPrerequisiteInvalid),
	)

	if (publicationConfiguration.puballtables) {
		return yield* Effect.fail(replicationPrerequisiteInvalid)
	}
	if (
		!publicationConfiguration.pubinsert ||
		!publicationConfiguration.pubupdate ||
		!publicationConfiguration.pubdelete ||
		!publicationConfiguration.pubtruncate
	) {
		return yield* Effect.fail(
			ReplicationOperationFailure.cases.SourceRejected.make({
				reason: 'publication-missing-required-operations',
			}),
		)
	}
	return yield* Effect.void
})

export const addRelationToReplicationPublication = Effect.fn('replication_contract.add_relation_to_publication')(
	function* ({ connection, publicationName, relation }: ReplicationRelationInput) {
		const qualifiedRelation = `${escapeIdentifier(relation.schemaName)}.${escapeIdentifier(relation.tableName)}`
		yield* executeReplicationContractQuery({
			connection,
			operation: 'add-relation-to-publication',
			queryText: `ALTER PUBLICATION ${escapeIdentifier(publicationName)} ADD TABLE ONLY ${qualifiedRelation}`,
		}).pipe(
			Effect.asVoid,
			Effect.catchTags({
				ReplicationContractAlreadyConfigured: () => Effect.fail(setupCommandFailed),
				ReplicationContractPermissionDenied: () => Effect.fail(publicationPermissionDenied),
				ReplicationContractQueryFailed: () => Effect.fail(setupCommandFailed),
			}),
		)
	},
)

export const setRelationReplicaIdentityFull = Effect.fn('replication_contract.set_relation_replica_identity_full')(
	function* ({ connection, relation }: ReplicationRelationConnectionInput) {
		const qualifiedRelation = `${escapeIdentifier(relation.schemaName)}.${escapeIdentifier(relation.tableName)}`
		yield* executeReplicationContractQuery({
			connection,
			operation: 'set-relation-replica-identity-full',
			queryText: `ALTER TABLE ONLY ${qualifiedRelation} REPLICA IDENTITY FULL`,
		}).pipe(
			Effect.asVoid,
			Effect.catchTags({
				ReplicationContractAlreadyConfigured: () => Effect.fail(setupCommandFailed),
				ReplicationContractPermissionDenied: () => Effect.fail(replicaIdentityPermissionDenied),
				ReplicationContractQueryFailed: () => Effect.fail(setupCommandFailed),
			}),
		)
	},
)

export const readReplicationRelationConfiguration = Effect.fn('replication_contract.read_relation_configuration')(
	function* ({ connection, publicationName, relation }: ReplicationRelationInput) {
		const result = yield* executeReplicationContractQuery({
			connection,
			operation: 'read-relation-configuration',
			queryText: `SELECT
			EXISTS (
				SELECT 1
				FROM pg_publication_rel AS publication_relation
				JOIN pg_publication AS publication ON publication.oid = publication_relation.prpubid
				WHERE publication.pubname = ${escapeLiteral(publicationName)}
					AND publication_relation.prrelid = relation.oid
			) AS publication_member,
			relation.relreplident = 'f' AS replica_identity_full
			FROM pg_class AS relation
			JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = ${escapeLiteral(relation.schemaName)}
				AND relation.relname = ${escapeLiteral(relation.tableName)}`,
		}).pipe(
			Effect.catchTags({
				ReplicationContractAlreadyConfigured: () => Effect.fail(setupCommandFailed),
				ReplicationContractPermissionDenied: () => Effect.fail(replicaIdentityPermissionDenied),
				ReplicationContractQueryFailed: () => Effect.fail(setupCommandFailed),
			}),
		)

		const configuration = yield* Schema.decodeUnknownEffect(ReplicationRelationConfigurationResult)(
			result.rows.at(0),
		).pipe(
			Effect.tapError((error) => Effect.logError('Invalid replication relation configuration response', error)),
			Effect.mapError(() => replicationPrerequisiteInvalid),
		)
		return configuration
	},
)

export const verifyReplicationRelationConfiguration = Effect.fn('replication_contract.verify_relation_configuration')(
	function* (input: ReplicationRelationInput) {
		const configuration = yield* readReplicationRelationConfiguration(input)
		if (!configuration.publication_member || !configuration.replica_identity_full) {
			return yield* Effect.fail(replicationPrerequisiteInvalid)
		}
		return yield* Effect.void
	},
)

const executeTransactionControlQuery = (
	connection: ReplicationContractConnection,
	operation: typeof ReplicationContractQueryOperation.Type,
	queryText: string,
) =>
	executeReplicationContractQuery({ connection, operation, queryText }).pipe(
		Effect.asVoid,
		Effect.catchTags({
			ReplicationContractAlreadyConfigured: () => Effect.fail(setupCommandFailed),
			ReplicationContractPermissionDenied: () => Effect.fail(setupCommandFailed),
			ReplicationContractQueryFailed: () => Effect.fail(setupCommandFailed),
		}),
	)

const rollbackRelationConfiguration = (connection: ReplicationContractConnection) =>
	executeTransactionControlQuery(connection, 'rollback-relation-configuration', 'ROLLBACK').pipe(
		Effect.catchTag('SessionUnavailable', () => Effect.void),
	)

export const ensureReplicationRelation = Effect.fn('replication_contract.ensure_relation')(
	({ connection, publicationName, relation }: ReplicationRelationInput) =>
		Effect.acquireUseRelease(
			executeTransactionControlQuery(connection, 'begin-relation-configuration', 'BEGIN'),
			() =>
				Effect.gen(function* () {
					const configuration = yield* readReplicationRelationConfiguration({
						connection,
						publicationName,
						relation,
					})
					if (configuration.publication_member && configuration.replica_identity_full) {
						return yield* Effect.void
					}
					if (!configuration.publication_member) {
						yield* addRelationToReplicationPublication({ connection, publicationName, relation })
					}
					if (!configuration.replica_identity_full) {
						yield* setRelationReplicaIdentityFull({ connection, relation })
					}
					yield* verifyReplicationRelationConfiguration({ connection, publicationName, relation })
					return yield* Effect.void
				}),
			(_transaction, exit) =>
				Exit.isSuccess(exit)
					? executeTransactionControlQuery(connection, 'commit-relation-configuration', 'COMMIT')
					: rollbackRelationConfiguration(connection),
		),
)
