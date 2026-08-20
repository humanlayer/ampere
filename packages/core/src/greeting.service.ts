import { Context, Effect, Layer } from 'effect'

export interface GreetPersonByNameInput {
	readonly name: string
}

export interface GreetingServiceApi {
	readonly greetPersonByName: (input: GreetPersonByNameInput) => Effect.Effect<string>
}

export class GreetingService extends Context.Service<GreetingService, GreetingServiceApi>()('core/GreetingService') {}

export const greetPersonByName = (input: GreetPersonByNameInput) =>
	Effect.succeed(`Hello, ${input.name}!`).pipe(Effect.withSpan('greeting.greet_person_by_name'))

export const GreetingServiceLive = Layer.succeed(
	GreetingService,
	GreetingService.of({
		greetPersonByName,
	}),
)
