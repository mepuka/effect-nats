/**
 * Sketch: the NATS subject algebra at the type level, and schema-typed
 * publish/subscribe over a subject registry.
 *
 * `Matches<S, P>` is a type-level implementation of the same `matches`
 * relation the Lean model defines and the server's sublist implements.
 * Three implementations, one relation: the Lean one is the referee, and its
 * test vectors should exercise both this type and the runtime matcher.
 */
import { Schema } from "effect"
import type { Stream } from "effect"

// ————— The matching relation, type level —————

type TokenMatch<S extends string, P extends string> = P extends "*" ? (S extends "" ? false : true)
  : S extends P ? P extends S ? true : false
  : false

export type Matches<S extends string, P extends string> = P extends ">" ? (S extends "" ? false : true)
  : P extends `${infer PH}.${infer PT}` ? S extends `${infer SH}.${infer ST}` ? SH extends "" ? false
      : TokenMatch<SH, PH> extends true ? Matches<ST, PT>
      : false
    : false
  : S extends `${string}.${string}` ? false
  : TokenMatch<S, P>

/** `>` only as the final token; no empty tokens. */
export type IsValidPattern<P extends string> = P extends ">" ? true
  : P extends `${infer H}.${infer T}` ? H extends "" | ">" ? false : IsValidPattern<T>
  : P extends "" ? false
  : true

// ————— Schema-typed registry —————

export interface Registry {
  readonly [subject: string]: Schema.Top
}

export type MatchingSubjects<R extends Registry, P extends string> = {
  [K in keyof R & string]: Matches<K, P> extends true ? K : never
}[keyof R & string]

export type MatchingMessage<R extends Registry, P extends string> = {
  [K in MatchingSubjects<R, P>]: { readonly subject: K; readonly payload: R[K]["Type"] }
}[MatchingSubjects<R, P>]

declare class NatsError {
  readonly _tag: "NatsError"
}

/**
 * Subscribe typed by the registry: the element type is the union of
 * (subject, payload) pairs for exactly the subjects the pattern matches.
 * The payload type sharpens automatically when narrowing on `subject`.
 */
export declare const subscribe: <R extends Registry>(registry: R) => <const P extends string>(
  pattern: P & (IsValidPattern<P> extends true ? unknown : ["invalid pattern"])
) => Stream.Stream<MatchingMessage<R, P>, NatsError>

// ————— Type-level tests —————
type Equal<X, Y> = (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? true
  : false
type Expect<T extends true> = T

const OrderCreated = Schema.Struct({ orderId: Schema.String, amount: Schema.Number })
const OrderShipped = Schema.Struct({ orderId: Schema.String, carrier: Schema.String })
const PaymentSettled = Schema.Struct({ paymentId: Schema.String })

const registry = {
  "orders.eu.created": OrderCreated,
  "orders.us.created": OrderCreated,
  "orders.eu.shipped": OrderShipped,
  "payments.eu.settled": PaymentSettled
} as const
type Reg = typeof registry

// `*` matches exactly one token:
type _S1 = Expect<Equal<MatchingSubjects<Reg, "orders.*.created">, "orders.eu.created" | "orders.us.created">>
// `>` matches one-or-more:
type _S2 = Expect<
  Equal<MatchingSubjects<Reg, "orders.>">, "orders.eu.created" | "orders.us.created" | "orders.eu.shipped">
>
// exact patterns are singletons:
type _S3 = Expect<Equal<MatchingSubjects<Reg, "orders.eu.created">, "orders.eu.created">>
// arity is strict — a longer pattern matches nothing here:
type _S4 = Expect<Equal<MatchingSubjects<Reg, "orders.eu.created.extra">, never>>
// `>` needs at least one token past the prefix:
type _S5 = Expect<Equal<Matches<"orders", "orders.>">, false>>
// validity:
type _S6 = Expect<Equal<IsValidPattern<"orders.>.created">, false>>
type _S7 = Expect<Equal<IsValidPattern<"orders..created">, false>>

// The payload union sharpens under subject narrowing:
declare const msg: MatchingMessage<Reg, "orders.eu.>">
export const orderId = msg.subject === "orders.eu.created" ? msg.payload.amount : msg.payload.carrier
