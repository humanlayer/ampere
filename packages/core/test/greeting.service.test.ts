import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'
import { expect } from 'vitest'

import { GreetingService, GreetingServiceLive } from '../src/greeting.service.ts'

describe('GreetingService', () => {
	it.effect('greets a person by name', () =>
		Effect.gen(function* () {
			const greeting = yield* GreetingService
			expect(yield* greeting.greetPersonByName({ name: 'Ada' })).toBe('Hello, Ada!')
		}).pipe(Effect.provide(GreetingServiceLive)),
	)
})
