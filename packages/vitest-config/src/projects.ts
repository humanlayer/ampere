import type { TestProjectInlineConfiguration } from 'vitest/config'

type ProjectTestOptions = NonNullable<TestProjectInlineConfiguration['test']>

export function projects(defs: Record<string, Omit<ProjectTestOptions, 'name'>>): TestProjectInlineConfiguration[] {
	return Object.entries(defs).map(([name, test]) => ({
		extends: true,
		test: { ...test, name },
	}))
}
