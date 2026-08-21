/**
 * Sketch: delivery-graded effects via phantom R-channel entries.
 *
 * The observation this rests on (r-channel brief, verified against source):
 * `R` accumulates as a bare type-level union with zero runtime bookkeeping —
 * a free join-semilattice on service identifiers. Phantom services inject
 * arbitrary semilattice elements into it; a discharge combinator is a
 * semilattice-homomorphism evaluation at the boundary. Here the elements are
 * rungs of the NATS delivery-guarantee ladder, and the boundary evaluation is
 * the MEET (weakest rung present), because a pipeline is only as strong as
 * its weakest delivery step.
 */
import { Effect } from "effect"

// ————— The grade ladder as phantom services —————
// Never materialized: these exist only in `R`. Branded so they stay
// structurally disjoint; namespaced so an eventual materialization cannot
// collide with real keys or the two scanned prefixes.
export interface AtMostOnce {
  readonly "effect-nats-jetstream/grade": "at-most-once"
}
export interface AtLeastOnce {
  readonly "effect-nats-jetstream/grade": "at-least-once"
}
export interface DedupWindow {
  readonly "effect-nats-jetstream/grade": "dedup-window"
}
export type Grade = AtMostOnce | AtLeastOnce | DedupWindow

/** An effect whose delivery grade rides the R channel. */
export type Graded<G extends Grade, A, E = never, R = never> = Effect.Effect<A, E, R | G>

type ServicesOf<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never

/**
 * Meet over whatever the union accumulated: the weakest rung present wins.
 * This is the type-level mirror of `Guarantee.meet` in the Lean model.
 */
export type EffectiveGrade<R> = [Extract<R, AtMostOnce>] extends [never]
  ? [Extract<R, AtLeastOnce>] extends [never]
    ? [Extract<R, DedupWindow>] extends [never] ? "pure"
    : "dedup-window"
  : "at-least-once"
  : "at-most-once"

/** Rungs strictly below a floor. */
export type Below<F extends Grade> = F extends DedupWindow ? AtMostOnce | AtLeastOnce
  : F extends AtLeastOnce ? AtMostOnce
  : never

/**
 * Discharge a grade: the pure-cast shape `unstable/workflow/Activity.ts:134`
 * already ships (type-only `Exclude` with no runtime provision). Use when a
 * supervising layer takes responsibility for the rung — e.g. an outbox
 * upgrade, or a test harness that asserts on the trace instead.
 */
export const attest = <G extends Grade>() =>
<A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E, Exclude<R, G>> =>
  self as Effect.Effect<A, E, Exclude<R, G>>

/**
 * Boundary gate: composing in any op below the floor is a compile error.
 * Generalizes the run gate — `runFork` demands `R = never`; this demands
 * `R ∩ Below<Floor> = ∅`. Same mechanism, same zero runtime cost.
 */
export const requireFloor = <F extends Grade>() =>
<A, E, R>(
  self:
    & Effect.Effect<A, E, R>
    & ([Extract<R, Below<F>>] extends [never] ? unknown
      : ["delivery grade below required floor; offending rungs:", Extract<R, Below<F>>])
): Effect.Effect<A, E, R> => self

// ————— CALM-style coordination tracking, same mechanism —————
export interface Coordination {
  readonly "effect-nats-jetstream/coordination": true
}

/**
 * Only coordination-free programs (monotone/join-homomorphic over their KV
 * footprint) may run replicated without consensus. Non-monotone ops inject
 * `Coordination`; this gate refuses them.
 */
export const runCoordinationFree = <A, E, R>(
  self:
    & Effect.Effect<A, E, R>
    & ([Extract<R, Coordination>] extends [never] ? unknown
      : ["program requires coordination; run it on the consensus path instead"])
): Effect.Effect<A, E, R> => self

// ————— Type-level tests —————
type Equal<X, Y> = (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? true
  : false
type Expect<T extends true> = T

declare const corePub: Graded<AtMostOnce, void>
declare const jsPub: Graded<AtLeastOnce, { readonly seq: number }>
declare const jsPubDedup: Graded<DedupWindow, { readonly seq: number; readonly duplicate: boolean }>

// Typechecked but never executed — the declared ops have no runtime values.
const _examples = () => {
  // Composition accumulates grades in R for free (real flatMap, not a stub):
  const pipeline = Effect.flatMap(jsPub, () => corePub)
  type _T1 = Expect<Equal<EffectiveGrade<ServicesOf<typeof pipeline>>, "at-most-once">>
  type _T2 = Expect<Equal<EffectiveGrade<ServicesOf<typeof jsPub>>, "at-least-once">>
  type _T3 = Expect<Equal<EffectiveGrade<ServicesOf<typeof jsPubDedup>>, "dedup-window">>
  type _T4 = Expect<Equal<EffectiveGrade<ServicesOf<Effect.Effect<number>>>, "pure">>

  // A floor admits its own rung and stronger:
  const ok1 = requireFloor<AtLeastOnce>()(jsPub)
  const ok2 = requireFloor<AtLeastOnce>()(jsPubDedup)

  // @ts-expect-error — the pipeline contains an at-most-once op; the floor refuses it
  const bad1 = requireFloor<AtLeastOnce>()(pipeline)

  // Discharging the weak rung (responsibility explicitly taken) re-admits it:
  const ok3 = requireFloor<AtLeastOnce>()(attest<AtMostOnce>()(pipeline))

  return [ok1, ok2, ok3, bad1] as const
}
void _examples
