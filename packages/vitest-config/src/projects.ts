import type { TestProjectInlineConfiguration } from 'vitest/config'

export function projects(
	defs: Record<string, Omit<NonNullable<TestProjectInlineConfiguration['test']>, 'name'>>,
): TestProjectInlineConfiguration[] {
	return Object.entries(defs).map(([name, test]) => ({
		extends: true,
		test: { ...test, name },
	}))
}
