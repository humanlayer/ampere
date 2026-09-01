# Automation policy provenance

Ampere keeps automation policy executable and local so normal checks do not depend on remote source availability.

## TypeOnce automation

The original 49-rule `automation` Oxlint plugin, its catalog and profiles were vendored from
[`typeonce-dev/ai-automation`](https://github.com/typeonce-dev/ai-automation) commit
[`0bca096fe6fe9878cd15303a623dd2cd85915ddd`](https://github.com/typeonce-dev/ai-automation/commit/0bca096fe6fe9878cd15303a623dd2cd85915ddd).
The remaining TypeOnce catalog is represented by:

- the compiler-API engine and all three typed rules in `tools/typed-lint`, plus `workspace/no-svg-files`;
- the added-lines runtime, both diff rules, and key-file alert in `tools/diff-check`;
- the built-in Oxlint policy, disabled legacy policy, and `correctness`/`suspicious` categories in `vite.config.ts`;
- all six `better-tailwindcss` rules and the closed policy stylesheet at `tools/oxlint/tailwind/policy.css`.

The typed layer runs as part of `bun run check`. The review-only diff layer remains explicit as
`bun run check:diff` because it evaluates changes relative to a Git ref rather than the complete source tree.
The repository's TypeScript 7 package intentionally exposes only the native compiler CLI, so the vendored
Compiler API engine directly depends on the `typescript-compiler` alias pinned to TypeScript 5.9.3.

## Additional rules

- `anti-slop/no-conditional-empty-array-spread` comes from
  [`humanlayer/synclayer`](https://github.com/humanlayer/synclayer) commit
  [`e77ab8b1a905eb27d6ce4ceafef203b121990894`](https://github.com/humanlayer/synclayer/commit/e77ab8b1a905eb27d6ce4ceafef203b121990894).
- `automation/prefer-tagged-error-handling` comes from
  [`humanlayer/fold`](https://github.com/humanlayer/fold) commit
  [`a158288da0fecd5593352e33201993d900709151`](https://github.com/humanlayer/fold/commit/a158288da0fecd5593352e33201993d900709151).

Run `bun run check:automation-inventory` after changing registration or policy. It imports each custom registry
and verifies runtime counts, configuration entries, categories, Tailwind policy, scripts, and direct dependencies.
