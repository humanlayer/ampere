import { GreetingService, GreetingServiceLive } from '@ampere/core/greeting'
import { NodeRuntime } from '@effect/platform-node'
import { Effect } from 'effect'

const main = Effect.gen(function* () {
	const greeting = yield* GreetingService
	yield* Effect.log(yield* greeting.greetPersonByName({ name: 'Ampere' }))
})

NodeRuntime.runMain(main.pipe(Effect.provide(GreetingServiceLive)))
