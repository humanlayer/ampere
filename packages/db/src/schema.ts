import { boolean, integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core'

export const todos = pgTable('todos', {
	id: integer().primaryKey().generatedAlwaysAsIdentity(),
	title: text().notNull(),
	completed: boolean().notNull().default(false),
	createdAt: timestamp({ withTimezone: true }).notNull().defaultNow(),
})

export const todoComments = pgTable('todo_comments', {
	id: integer().primaryKey().generatedAlwaysAsIdentity(),
	todoId: integer().notNull().references(() => todos.id, { onDelete: 'cascade' }),
	body: text().notNull(),
	createdAt: timestamp({ withTimezone: true }).notNull().defaultNow(),
})
