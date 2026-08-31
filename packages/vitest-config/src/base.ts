import { defineConfig } from 'vitest/config'

export const base = defineConfig({
	test: {
		environment: 'node',
		pool: 'forks',
		testTimeout: 15_000,
	},
})
