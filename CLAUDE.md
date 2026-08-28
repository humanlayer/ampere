# ampere

Bun monorepo (`apps/*`, `packages/*`) built on Effect v4 and the Vite+ toolchain.

## Toolchain

- **Vite+** (`vp`) runs everything from the root `vite.config.ts`: `bun run check` (format + lint + type check in one pass), `bun run test`, `bun run lint`, `bun run format:write`, `bun run typecheck`, `bun run build`. Lint and format config live in `vite.config.ts`, not in `.oxlintrc`/`.oxfmtrc` files.
- **TypeScript 7** (`typescript@7`, the native tsgo compiler) with the `@effect/tsgo` patch. `bun install` runs `effect-tsgo patch --typescript --oxlint`, which embeds Effect diagnostics into `tsc` and into oxlint's type-aware `effecttsgo` plugin. Do not remove the `prepare` script.
- **anti-slop** oxlint rules are vendored at `tools/oxlint/anti-slop/` (from https://github.com/dmmulroy/anti-slop) and registered as `jsPlugins` in `vite.config.ts`. Note: `anti-slop/no-shape-in-symbol-names` bans `*Shape` names; use `*Api` for service interfaces.
- Dependency versions are pinned in the root `catalog`; packages declare `"catalog:"`. Effect packages track the v4 **rc** dist-tag. `bunfig.toml` enforces a 7-day minimum release age with excludes for the Effect/tsgo family.

## Database

- Local PostgreSQL 18 via `docker compose up -d` (port 55432, `wal_level=logical` for replication-log consumers).
- Logical replication uses a directly pinned `pg` dependency. Drizzle remains only for declarative application table schemas in `@ampere/db`.

## Effect

- Effect v4 (`4.0.0-rc.*`) with `@effect/platform-node`, tests via `@effect/vitest` (`it.effect`).
- Follow the `.claude/skills/effect-program-design` skill for all Effect work.
- Effect v4 source for API reference is cloned at `~/projects/effect-smol`; docs at <https://effect-ts-effect-smol.mintlify.app/>.
