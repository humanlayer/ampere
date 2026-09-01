# Automation Oxlint plugin

Vendored from [`typeonce-dev/ai-automation`](https://github.com/typeonce-dev/ai-automation/tree/0bca096fe6fe9878cd15303a623dd2cd85915ddd/rules/oxlint/src) at commit [`0bca096fe6fe9878cd15303a623dd2cd85915ddd`](https://github.com/typeonce-dev/ai-automation/commit/0bca096fe6fe9878cd15303a623dd2cd85915ddd).

The runtime plugin includes the registry, catalog, profiles, and rule implementations. `config.ts` is intentionally omitted because it is an example configuration generator for the upstream repository rather than part of the plugin runtime. Ampere registers the plugin and explicitly configures all 49 registered rules in the root `vite.config.ts`.
