import { readFileSync } from 'node:fs'

import diffCheck from './diff-check/plugin.ts'
import antiSlop from './oxlint/anti-slop/index.ts'
import automation from './oxlint/automation/index.ts'
import { typedRules, workspaceRules } from './typed-lint/registry.ts'

const rootConfig = readFileSync(new URL('../vite.config.ts', import.meta.url), 'utf8')
const packageManifest = readFileSync(new URL('../package.json', import.meta.url), 'utf8')

const requireInventoryItem = ({ item, source }: { item: string; source: string }) => {
	if (!source.includes(item)) throw new Error(`Missing automation inventory item: ${item}`)
}

const requireCount = ({ actual, expected, kind }: { actual: number; expected: number; kind: string }) => {
	if (actual !== expected) throw new Error(`Expected ${expected} ${kind}; found ${actual}`)
}

requireCount({ actual: Object.keys(automation.rules).length, expected: 50, kind: 'automation rules' })
requireCount({ actual: Object.keys(antiSlop.rules).length, expected: 17, kind: 'anti-slop rules' })
requireCount({ actual: Object.keys(typedRules).length, expected: 3, kind: 'typed rules' })
requireCount({ actual: Object.keys(workspaceRules).length, expected: 1, kind: 'workspace rules' })
requireCount({ actual: Object.keys(diffCheck.rules).length, expected: 2, kind: 'diff rules' })

const builtInPolicies = [
	'no-console',
	'no-empty',
	'no-empty-function',
	'no-eq-null',
	'no-unused-vars',
	'unicorn/filename-case',
	'require-yield',
	'no-shadow',
	'no-img-element',
	'react-in-jsx-scope',
	'exhaustive-deps',
]
const tailwindPolicies = [
	'no-concatenated-classes',
	'no-conflicting-classes',
	'no-deprecated-classes',
	'no-duplicate-classes',
	'no-restricted-classes',
	'no-unknown-classes',
]

for (const policy of builtInPolicies) requireInventoryItem({ item: `'${policy}'`, source: rootConfig })
for (const policy of tailwindPolicies)
	requireInventoryItem({ item: `'better-tailwindcss/${policy}'`, source: rootConfig })
for (const category of ['correctness', 'suspicious']) requireInventoryItem({ item: `${category}:`, source: rootConfig })
for (const dependency of [
	'eslint-plugin-better-tailwindcss',
	'fast-glob',
	'tailwindcss',
	'typescript',
	'typescript-compiler',
])
	requireInventoryItem({ item: `"${dependency}"`, source: packageManifest })

requireInventoryItem({ item: "'anti-slop/no-conditional-empty-array-spread': 'error'", source: rootConfig })
requireInventoryItem({ item: "'automation/prefer-tagged-error-handling': 'error'", source: rootConfig })
requireInventoryItem({ item: 'tools/typed-lint/cli.ts check', source: packageManifest })
requireInventoryItem({ item: 'tools/diff-check/main.ts', source: packageManifest })

process.stdout.write(
	`Automation inventory verified: ${Object.keys(automation.rules).length} automation, ${Object.keys(antiSlop.rules).length} anti-slop, ${Object.keys(typedRules).length} typed, ${Object.keys(workspaceRules).length} workspace, ${Object.keys(diffCheck.rules).length} diff, ${builtInPolicies.length} built-in, 2 categories, ${tailwindPolicies.length} Tailwind.\n`,
)
