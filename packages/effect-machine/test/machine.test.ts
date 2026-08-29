import { describe, it } from '@effect/vitest'
import { Context, Effect, Layer, Ref, Schema } from 'effect'
import { expect } from 'vitest'

import { Event, Machine, State, simulate } from '../src/index.ts'

const CounterState = State({
	Idle: {},
	Counting: { count: Schema.Finite },
	Done: { count: Schema.Finite },
})

const CounterEvent = Event({
	Start: {},
	Increment: {},
	Finish: {},
})

const CounterMachine = Machine.make({
	state: CounterState,
	event: CounterEvent,
	initial: CounterState.Idle,
})
	.on(CounterState.Idle, CounterEvent.Start, () => CounterState.Counting({ count: 0 }))
	.on(CounterState.Counting, CounterEvent.Increment, ({ state }) => CounterState.Counting({ count: state.count + 1 }))
	.on(CounterState.Counting, CounterEvent.Finish, ({ state }) => CounterState.Done.with(state))
	.final(CounterState.Done)

const ServiceState = State({
	Idle: {},
	Loading: {},
	Running: { value: Schema.Finite },
	Done: { value: Schema.Finite },
})

const ServiceEvent = Event({
	Start: {},
	Loaded: { value: Schema.Finite },
	Finished: {},
})

interface WorkflowOperationsApi {
	readonly load: Effect.Effect<number>
	readonly finish: (input: { readonly value: number }) => Effect.Effect<void>
}

class WorkflowOperations extends Context.Service<WorkflowOperations, WorkflowOperationsApi>()(
	'@ampere/effect-machine/test/WorkflowOperations',
) {}

const ServiceMachine = Machine.make({
	state: ServiceState,
	event: ServiceEvent,
	initial: ServiceState.Idle,
})
	.on(ServiceState.Idle, ServiceEvent.Start, () => ServiceState.Loading)
	.task(
		ServiceState.Loading,
		() =>
			WorkflowOperations.use((operations) =>
				operations.load.pipe(Effect.map((value) => ServiceEvent.Loaded({ value }))),
			),
		{ name: 'load' },
	)
	.on(ServiceState.Loading, ServiceEvent.Loaded, ({ event }) => ServiceState.Running({ value: event.value }))
	.spawn(ServiceState.Running, ({ self, state }) =>
		WorkflowOperations.use((operations) =>
			operations.finish({ value: state.value }).pipe(Effect.andThen(self.send(ServiceEvent.Finished))),
		),
	)
	.on(ServiceState.Running, ServiceEvent.Finished, ({ state }) => ServiceState.Done.with(state))
	.final(ServiceState.Done)

describe('vendored effect-machine', () => {
	it.effect('simulates schema-first transitions', () =>
		Effect.gen(function* () {
			const result = yield* simulate(CounterMachine, [
				CounterEvent.Start,
				CounterEvent.Increment,
				CounterEvent.Finish,
			])

			expect(result.states).toEqual([
				CounterState.Idle,
				CounterState.Counting({ count: 0 }),
				CounterState.Counting({ count: 1 }),
				CounterState.Done({ count: 1 }),
			])
			expect(result.finalState).toEqual(CounterState.Done({ count: 1 }))
		}),
	)

	it.effect('runs actors on the pinned Effect runtime', () =>
		Effect.scoped(
			Machine.scoped(
				Effect.gen(function* () {
					const actor = yield* Machine.spawn(CounterMachine, { id: 'counter-test' })
					yield* actor.start
					yield* actor.send(CounterEvent.Start)
					yield* actor.send(CounterEvent.Increment)
					const finalState = yield* actor.sendAndWait(CounterEvent.Finish)

					expect(finalState).toEqual(CounterState.Done({ count: 1 }))
				}),
			),
		),
	)

	it.effect('captures service layers for task and spawn effects', () =>
		Effect.scoped(
			Machine.scoped(
				Effect.gen(function* () {
					const finishedValues = yield* Ref.make<ReadonlyArray<number>>([])
					const workflowLayer = Layer.succeed(WorkflowOperations, {
						load: Effect.succeed(42),
						finish: ({ value }) => Ref.update(finishedValues, (values) => [...values, value]),
					})
					const actor = yield* Machine.spawn(ServiceMachine, { id: 'service-test' }).pipe(
						Effect.provide(workflowLayer),
					)

					// Starting outside Effect.provide proves Machine.spawn captured the service context.
					yield* actor.start
					yield* actor.send(ServiceEvent.Start)
					const finalState = yield* actor.awaitFinal

					expect(finalState).toEqual(ServiceState.Done({ value: 42 }))
					expect(yield* Ref.get(finishedValues)).toEqual([42])
				}),
			),
		),
	)
})
