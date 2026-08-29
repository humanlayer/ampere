import { Context, Effect, Match, Option, type Schema } from 'effect'

// ============================================================================
// Type-level helpers
// ============================================================================

/**
 * Resolve a type param: if it's a Schema, extract `.Type`; otherwise use as-is.
 */
type ResolveType<T> = T extends Schema.Schema<infer A> ? A : T

// ============================================================================
// Inspection Events
// ============================================================================

/**
 * Event emitted when an actor is spawned
 */
export interface SpawnEvent<S> {
	readonly type: '@machine.spawn'
	readonly actorId: string
	readonly initialState: S
	readonly timestamp: number
}

/**
 * Event emitted when an actor receives an event
 */
export interface EventReceivedEvent<S, E> {
	readonly type: '@machine.event'
	readonly actorId: string
	readonly state: S
	readonly event: E
	readonly timestamp: number
}

/**
 * Event emitted when a transition occurs
 */
export interface TransitionEvent<S, E> {
	readonly type: '@machine.transition'
	readonly actorId: string
	readonly fromState: S
	readonly toState: S
	readonly event: E
	readonly timestamp: number
}

/**
 * Event emitted when a spawn effect runs
 */
export interface EffectEvent<S> {
	readonly type: '@machine.effect'
	readonly actorId: string
	readonly effectType: 'spawn'
	readonly state: S
	readonly timestamp: number
}

export interface TaskEvent<S> {
	readonly type: '@machine.task'
	readonly actorId: string
	readonly state: S
	readonly taskName?: string
	readonly phase: 'start' | 'success' | 'failure' | 'interrupt'
	readonly error?: string
	readonly timestamp: number
}

/**
 * Event emitted when a transition handler or spawn effect fails with a defect
 */
export interface ErrorEvent<S, E> {
	readonly type: '@machine.error'
	readonly actorId: string
	readonly phase: 'transition' | 'spawn'
	readonly state: S
	readonly event: E
	readonly error: string
	readonly timestamp: number
}

/**
 * Event emitted when an actor stops
 */
export interface StopEvent<S> {
	readonly type: '@machine.stop'
	readonly actorId: string
	readonly finalState: S
	readonly timestamp: number
}

/**
 * Union of all inspection events
 */
export type InspectionEvent<S, E> =
	| SpawnEvent<S>
	| EventReceivedEvent<S, E>
	| TransitionEvent<S, E>
	| EffectEvent<S>
	| TaskEvent<S>
	| ErrorEvent<S, E>
	| StopEvent<S>

/**
 * Convenience alias for untyped inspection events.
 * Useful for general-purpose inspectors that don't need specific state/event types.
 * State and event fields are typed as `{ readonly _tag: string }` so discriminated
 * access to `_tag` works without casting.
 */
export type AnyInspectionEvent = InspectionEvent<{ readonly _tag: string }, { readonly _tag: string }>

// ============================================================================
// InspectorService Service
// ============================================================================

/**
 * Inspector interface for observing machine behavior
 */
export type InspectorHandler<S, E> = (event: InspectionEvent<S, E>) => void | Effect.Effect<void>

export interface InspectorService<S, E> {
	readonly onInspect: InspectorHandler<S, E>
}

/**
 * Inspector service tag - optional service for machine introspection
 * Uses `any` types to allow variance flexibility when providing the service
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class Inspector extends Context.Service<Inspector, InspectorService<any, any>>()(
	'effect-machine/inspection/Inspector',
) {}

/**
 * Create an inspector from a callback function.
 *
 * Type params accept either raw tagged types or Schema constructors:
 * - `makeInspector(cb)` — defaults to `AnyInspectionEvent`
 * - `makeInspector<MyState, MyEvent>(cb)` — explicit tagged types
 * - `makeInspector<typeof MyState, typeof MyEvent>(cb)` — schema constructors (auto-extracts `.Type`)
 */
export const makeInspector = <S = { readonly _tag: string }, E = { readonly _tag: string }>(
	onInspect: InspectorHandler<ResolveType<S>, ResolveType<E>>,
): InspectorService<ResolveType<S>, ResolveType<E>> => ({ onInspect })

export const makeInspectorEffect = <S = { readonly _tag: string }, E = { readonly _tag: string }>(
	onInspect: (event: InspectionEvent<ResolveType<S>, ResolveType<E>>) => Effect.Effect<void>,
): InspectorService<ResolveType<S>, ResolveType<E>> => ({ onInspect })

const inspectionEffect = <S, E>(
	inspector: InspectorService<S, E>,
	event: InspectionEvent<S, E>,
): Effect.Effect<void> => {
	const result = inspector.onInspect(event)
	return Effect.isEffect(result) ? result : Effect.void
}

export const combineInspectors = <S, E>(
	...inspectors: ReadonlyArray<InspectorService<S, E>>
): InspectorService<S, E> => ({
	onInspect: (event) =>
		Effect.forEach(
			inspectors,
			(inspector) => inspectionEffect(inspector, event).pipe(Effect.catchCause(() => Effect.void)),
			{ concurrency: 'unbounded', discard: true },
		),
})

export interface TracingInspectorOptions<S, E> {
	readonly spanName?: string | ((event: InspectionEvent<S, E>) => string)
	readonly attributes?: (event: InspectionEvent<S, E>) => Readonly<Record<string, string | number | boolean>>
	readonly eventName?: (event: InspectionEvent<S, E>) => string
}

const inspectionSpanName = <S extends { readonly _tag: string }, E extends { readonly _tag: string }>(
	event: InspectionEvent<S, E>,
) =>
	Match.value(event).pipe(
		Match.discriminatorsExhaustive('type')({
			'@machine.spawn': (spawnEvent) => `Machine.inspect ${spawnEvent.initialState._tag}`,
			'@machine.event': (receivedEvent) => `Machine.inspect ${receivedEvent.event._tag}`,
			'@machine.transition': (transitionEvent) =>
				`Machine.inspect ${transitionEvent.fromState._tag}->${transitionEvent.toState._tag}`,
			'@machine.effect': (effectEvent) => `Machine.inspect ${effectEvent.effectType}`,
			'@machine.task': (taskEvent) => `Machine.inspect task:${taskEvent.phase}`,
			'@machine.error': (errorEvent) => `Machine.inspect ${errorEvent.phase}`,
			'@machine.stop': (stopEvent) => `Machine.inspect ${stopEvent.finalState._tag}`,
		}),
	)

const inspectionTraceName = <S extends { readonly _tag: string }, E extends { readonly _tag: string }>(
	event: InspectionEvent<S, E>,
) =>
	Match.value(event).pipe(
		Match.discriminatorsExhaustive('type')({
			'@machine.spawn': (spawnEvent) => `machine.spawn ${spawnEvent.initialState._tag}`,
			'@machine.event': (receivedEvent) => `machine.event ${receivedEvent.event._tag}`,
			'@machine.transition': (transitionEvent) =>
				`machine.transition ${transitionEvent.fromState._tag}->${transitionEvent.toState._tag}`,
			'@machine.effect': (effectEvent) => `machine.effect ${effectEvent.effectType}`,
			'@machine.task': (taskEvent) =>
				`machine.task ${taskEvent.phase}${taskEvent.taskName === undefined ? '' : ` ${taskEvent.taskName}`}`,
			'@machine.error': (errorEvent) => `machine.error ${errorEvent.phase}`,
			'@machine.stop': (stopEvent) => `machine.stop ${stopEvent.finalState._tag}`,
		}),
	)

const inspectionAttributes = <S extends { readonly _tag: string }, E extends { readonly _tag: string }>(
	event: InspectionEvent<S, E>,
): Record<string, string | number | boolean> => {
	const shared = {
		'machine.actor.id': event.actorId,
		'machine.inspection.type': event.type,
	}

	return Match.value(event).pipe(
		Match.discriminatorsExhaustive('type')({
			'@machine.spawn': (spawnEvent) => ({ ...shared, 'machine.state.initial': spawnEvent.initialState._tag }),
			'@machine.event': (receivedEvent) => ({
				...shared,
				'machine.state.current': receivedEvent.state._tag,
				'machine.event.tag': receivedEvent.event._tag,
			}),
			'@machine.transition': (transitionEvent) => ({
				...shared,
				'machine.state.from': transitionEvent.fromState._tag,
				'machine.state.to': transitionEvent.toState._tag,
				'machine.event.tag': transitionEvent.event._tag,
			}),
			'@machine.effect': (effectEvent) => ({
				...shared,
				'machine.state.current': effectEvent.state._tag,
				'machine.effect.kind': effectEvent.effectType,
			}),
			'@machine.task': (taskEvent) => ({
				...shared,
				'machine.state.current': taskEvent.state._tag,
				'machine.task.phase': taskEvent.phase,
				...(taskEvent.taskName === undefined ? {} : { 'machine.task.name': taskEvent.taskName }),
			}),
			'@machine.error': (errorEvent) => ({
				...shared,
				'machine.phase': errorEvent.phase,
				'machine.state.current': errorEvent.state._tag,
			}),
			'@machine.stop': (stopEvent) => ({ ...shared, 'machine.state.final': stopEvent.finalState._tag }),
		}),
	)
}

export const tracingInspector = <S extends { readonly _tag: string }, E extends { readonly _tag: string }>(
	options?: TracingInspectorOptions<S, E>,
): InspectorService<S, E> => ({
	onInspect: (event) => {
		const spanName = typeof options?.spanName === 'function' ? options.spanName(event) : options?.spanName
		const traceName = options?.eventName?.(event) ?? inspectionTraceName(event)
		const attributes = {
			...inspectionAttributes(event),
			...(options?.attributes?.(event) ?? {}),
		}

		return Effect.gen(function* () {
			const currentSpan = yield* Effect.option(Effect.currentSpan)
			if (Option.isSome(currentSpan)) {
				currentSpan.value.event(traceName, BigInt(event.timestamp) * 1_000_000n, {
					actorId: event.actorId,
					inspectionType: event.type,
				})
			}
		}).pipe(Effect.withSpan(spanName ?? inspectionSpanName(event), { attributes }))
	},
})

// ============================================================================
// Built-in Inspectors
// ============================================================================

/**
 * Console inspector that logs events in a readable format
 */
export const consoleInspector = (): InspectorService<{ readonly _tag: string }, { readonly _tag: string }> =>
	makeInspectorEffect((event) => {
		const prefix = `[${event.actorId}]`
		return Match.value(event).pipe(
			Match.discriminatorsExhaustive('type')({
				'@machine.spawn': (spawnEvent) => Effect.log(`${prefix} spawned -> ${spawnEvent.initialState._tag}`),
				'@machine.event': (receivedEvent) =>
					Effect.log(`${prefix} received ${receivedEvent.event._tag} in ${receivedEvent.state._tag}`),
				'@machine.transition': (transitionEvent) =>
					Effect.log(`${prefix} ${transitionEvent.fromState._tag} -> ${transitionEvent.toState._tag}`),
				'@machine.effect': (effectEvent) =>
					Effect.log(`${prefix} ${effectEvent.effectType} effect in ${effectEvent.state._tag}`),
				'@machine.task': (taskEvent) =>
					Effect.log(
						`${prefix} task ${taskEvent.phase} ${taskEvent.taskName ?? '<unnamed>'} in ${taskEvent.state._tag}`,
					),
				'@machine.error': (errorEvent) =>
					Effect.log(`${prefix} error in ${errorEvent.phase} ${errorEvent.state._tag} - ${errorEvent.error}`),
				'@machine.stop': (stopEvent) => Effect.log(`${prefix} stopped in ${stopEvent.finalState._tag}`),
			}),
		)
	})

/**
 * Collecting inspector that stores events in an array for testing
 */
export const collectingInspector = <S extends { readonly _tag: string }, E extends { readonly _tag: string }>(
	events: InspectionEvent<S, E>[],
): InspectorService<S, E> => ({
	onInspect: (event) => {
		events.push(event)
	},
})
