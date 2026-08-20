import { boolean, integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core'

export const todos = pgTable('todos', {
	id: integer().primaryKey().generatedAlwaysAsIdentity(),
	title: text().notNull(),
	completed: boolean().notNull().default(false),
	createdAt: timestamp({ withTimezone: true }).notNull().defaultNow(),
})
