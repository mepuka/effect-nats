import type { NodeConnectionOptions } from "@nats-io/transport-node"
import { Duration } from "effect"

export type ReconnectAttemptLimit =
  | { readonly _tag: "Bounded"; readonly attempts: number }
  | { readonly _tag: "Unbounded" }

export type ReconnectDelay =
  | {
    readonly _tag: "AdditiveJitter"
    readonly base: Duration.Input
    readonly jitter: Duration.Input
    readonly jitterTls: Duration.Input
  }
  | {
    /** Official compatibility callback; it replaces base and jitter wholesale. */
    readonly _tag: "Custom"
    readonly nextDelayMillis: () => number
  }

export type ReconnectPolicy =
  | { readonly _tag: "Disabled" }
  | {
    readonly _tag: "Enabled"
    readonly attempts: ReconnectAttemptLimit
    readonly delay: ReconnectDelay
    /** Fail the initial connect, or let the official client keep rotating. */
    readonly firstConnect: "FailFast" | "Wait"
    /** Abort on consecutive auth-family errors, or explicitly keep trying. */
    readonly authErrors: "Abort" | "Continue"
    readonly serverOrder: "Randomized" | "Configured"
  }

export const defaultPolicy = {
  _tag: "Enabled",
  attempts: { _tag: "Bounded", attempts: 10 },
  delay: {
    _tag: "AdditiveJitter",
    base: "2 seconds",
    jitter: "100 millis",
    jitterTls: "1 second"
  },
  firstConnect: "FailFast",
  authErrors: "Abort",
  serverOrder: "Randomized"
} as const satisfies ReconnectPolicy

export type OfficialReconnectOptions = Pick<
  NodeConnectionOptions,
  | "reconnect"
  | "maxReconnectAttempts"
  | "reconnectTimeWait"
  | "reconnectJitter"
  | "reconnectJitterTLS"
  | "reconnectDelayHandler"
  | "waitOnFirstConnect"
  | "ignoreAuthErrorAbort"
  | "noRandomize"
>

const milliseconds = (input: Duration.Input, name: string, allowZero: boolean): number => {
  const value = Duration.toMillis(Duration.fromInputUnsafe(input))
  if (!Number.isFinite(value) || value < 0 || (!allowZero && value === 0)) {
    throw new Error(`${name} must be ${allowZero ? "non-negative" : "positive"} and finite`)
  }
  return value
}

const delayOptions = (delay: ReconnectDelay): Partial<OfficialReconnectOptions> => {
  if (delay._tag === "AdditiveJitter") {
    return {
      reconnectTimeWait: milliseconds(delay.base, "reconnect base delay", false),
      reconnectJitter: milliseconds(delay.jitter, "reconnect jitter", true),
      reconnectJitterTLS: milliseconds(delay.jitterTls, "TLS reconnect jitter", true)
    }
  }
  return {
    reconnectDelayHandler: () => {
      const value = delay.nextDelayMillis()
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("custom reconnect delay must return positive finite milliseconds")
      }
      return value
    }
  }
}

/** Compiles the typed ADR-0001 policy at the official-client boundary. */
export const compile = (policy: ReconnectPolicy): OfficialReconnectOptions => {
  if (policy._tag === "Disabled") return { reconnect: false }
  if (
    policy.attempts._tag === "Bounded"
    && (!Number.isInteger(policy.attempts.attempts) || policy.attempts.attempts <= 0)
  ) {
    throw new Error("bounded reconnect attempts must be a positive integer")
  }
  const maxReconnectAttempts = policy.attempts._tag === "Unbounded"
    ? -1
    : policy.attempts.attempts
  return {
    reconnect: true,
    maxReconnectAttempts,
    ...delayOptions(policy.delay),
    waitOnFirstConnect: policy.firstConnect === "Wait",
    ignoreAuthErrorAbort: policy.authErrors === "Continue",
    noRandomize: policy.serverOrder === "Configured"
  }
}
