import { describe, it } from '@effect/vitest'
import { eq } from 'drizzle-orm'
import * as PgDrizzle from 'drizzle-orm/effect-postgres'
import { Data, Effect } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'
import { expect } from 'vitest'

import { PgLive } from '../src/pg.layer.ts'
import { todoComments, todos } from '../src/schema.ts'

const resetTodosTable = Effect.gen(function* () {
	const sql = yield* SqlClient.SqlClient
	yield* sql`DROP TABLE IF EXISTS todo_comments`
	yield* sql`DROP TABLE IF EXISTS todos`
	yield* sql`
		CREATE TABLE todos (
			id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			title text NOT NULL,
			completed boolean NOT NULL DEFAULT false,
			"createdAt" timestamptz NOT NULL DEFAULT now()
		)
	`
	yield* sql`
		CREATE TABLE todo_comments (
			id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
			"todoId" integer NOT NULL REFERENCES todos(id) ON DELETE CASCADE,
			body text NOT NULL,
			"createdAt" timestamptz NOT NULL DEFAULT now()
		)
	`
})

describe('PostgreSQL through Effect SQL', () => {
	it.effect('runs raw SQL through the SqlClient service', () =>
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient
			const rows = yield* sql<{ answer: number }>`SELECT 41 + 1 AS answer`
			expect(rows).toEqual([{ answer: 42 }])
		}).pipe(Effect.provide(PgLive)),
	)

	class MissingReturnedRowError extends Data.TaggedError('MissingReturnedRowError') {}

	it.effect('runs Drizzle queries as Effects against PostgreSQL', () =>
		Effect.gen(function* () {
			yield* resetTodosTable
			const db = yield* PgDrizzle.makeWithDefaults()

			const [inserted] = yield* db.insert(todos).values({ title: 'wire up postgres' }).returning()
			if (inserted === undefined) {
				return yield* new MissingReturnedRowError()
			}
			expect(inserted.title).toBe('wire up postgres')
			expect(inserted.completed).toBe(false)

			const [updated] = yield* db
				.update(todos)
				.set({ completed: true })
				.where(eq(todos.id, inserted.id))
				.returning()
			expect(updated?.completed).toBe(true)

			const all = yield* db.select().from(todos)
			expect(all).toHaveLength(1)
			return undefined
		}).pipe(Effect.provide(PgLive)),
	)

	class AbortTransactionError extends Data.TaggedError('AbortTransactionError') {}

	it.effect('rolls back Drizzle transactions on failure', () =>
		Effect.gen(function* () {
			yield* resetTodosTable
			const db = yield* PgDrizzle.makeWithDefaults()

			const failing = db.transaction((tx) =>
				Effect.gen(function* () {
					yield* tx.insert(todos).values({ title: 'doomed' })
					return yield* new AbortTransactionError()
				}),
			)
			yield* Effect.flip(failing)

			const all = yield* db.select().from(todos)
			expect(all).toHaveLength(0)
		}).pipe(Effect.provide(PgLive)),
	)

	it.effect('persists comments for a todo', () =>
		Effect.gen(function* () {
			yield* resetTodosTable
			const db = yield* PgDrizzle.makeWithDefaults()

			const [todo] = yield* db.insert(todos).values({ title: 'write a comment' }).returning()
			if (todo === undefined) {
				return yield* new MissingReturnedRowError()
			}

			const [comment] = yield* db
				.insert(todoComments)
				.values({ todoId: todo.id, body: 'a related row' })
				.returning()
			expect(comment?.todoId).toBe(todo.id)
			return undefined
		}).pipe(Effect.provide(PgLive)),
	)
})
