/**
 * Interest — the compile-time counterpart of NATS interest.
 *
 * Core-NATS fact (notes/0004): publishing with no subscriber means the
 * message is silently discarded. We refuse to model silence: `publish`
 * DEMANDS `Interest<Subject>` in the R channel, and nothing discharges it
 * except an explicit subscription boundary. Since run entry points gate on
 * `R = never`, "someone fire-and-forgot into the void" stops compiling.
 *
 * Interest has NO runtime representation. Discharge by cast is sound because
 * Effect's R channel is fully erased at runtime; precedent ships in-tree
 * (`RpcMiddleware.ApplyServices`, scope-cast in Activity).
 */
declare const InterestBrand: unique symbol

import type { Effect } from "effect"

export interface Interest<Name extends string> {
  readonly [InterestBrand]: "Interest"
  readonly subject: Name
}

export type AnyInterest = Interest<string>

/**
 * Run `self` as a process subscribed to `subjects`. Compile-time only:
 * discharges the matching phantom requirements, changes nothing at runtime.
 */
export const subscribe = <
  const Names extends readonly string[],
  A,
  E,
  R,
>(
  subjects: Names,
  self: Effect.Effect<A, E, R | Interest<Names[number]>>,
): Effect.Effect<A, E, Exclude<R, Interest<Names[number]>>> =>
  self as Effect.Effect<A, E, Exclude<R, Interest<Names[number]>>>
