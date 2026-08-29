# @ampere/effect-machine

Vendored state-machine runtime for Effect v4.

This package is derived from [`cevr/effect-machine`](https://github.com/cevr/effect-machine) at commit
`87072f1e62c2c04f68dc51e08a7fa4e2d7fa7804`. The original MIT license is preserved in `LICENSE`.

The vendored source intentionally contains only the in-process machine and actor runtime. The upstream
`@effect/cluster` integration is not included because Ampere does not use it and it would add unrelated runtime
dependencies.

Ampere maintains this copy against the Effect version pinned in the root catalog.

The initial Effect RC port replaces the removed `Schema.TaggedErrorClass` API with `Schema.TaggedError` and the
removed binary `Schedule.both` combinator with `Schedule.max` over both schedules.

The upstream implementation is excluded from repository lint rules so the vendored source remains recognizable and
can be compared with upstream. It is still compiled by TypeScript and covered by package compatibility tests.

## Effect services

Transition handlers registered with `.on()` remain pure. State-scoped `.task()`, `.spawn()`, and `.background()`
handlers may yield Effect services. Their requirements accumulate in the machine type, so `Machine.spawn(machine)`
requires the corresponding layer. The actor captures that service context for work started later by `actor.start`.

```ts
const machine = Machine.make({ state, event, initial: state.Idle })
	.on(state.Idle, event.Start, () => state.Loading)
	.task(state.Loading, () => WidgetOperations.use((operations) => operations.load.pipe(Effect.map(event.Loaded))), {
		name: 'load-widget',
	})

const actor = yield * Machine.spawn(machine).pipe(Effect.provide(WidgetOperationsLive))
yield * actor.start
```

Use `.task()` for one operation that emits one success or failure event. Use `.spawn()` for long-lived state work that
may send multiple events through `self` and must be interrupted when the actor leaves that state.
