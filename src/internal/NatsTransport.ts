import { Context, Effect, Layer, Schedule } from "effect"
import type { Duration } from "effect"
import { Connector } from "./Connector.ts"
import { ConnectExhaustedError, ConnectionClosedError } from "./Errors.ts"
import type { ReconnectPolicy } from "./ReconnectPolicy.ts"

/**
 * Structural subset of the official `@nats-io` public client surface that this
 * seam adapts. Keeping the shape structural keeps the transport boundary
 * adapter-neutral and fakeable; only lifecycle members are consumed so far.
 */
export interface ServerStatus {
  readonly type: string
  readonly data?: unknown
}

export interface Connection {
  readonly closed: () => Promise<void | Error>
  readonly close: () => Promise<void>
  readonly drain: () => Promise<void>
  readonly flush: () => Promise<void>
  readonly isClosed: () => boolean
  readonly isDraining: () => boolean
  readonly status: () => AsyncIterable<ServerStatus>
}

export interface Interface {
  /** Protocol round trip: resolves when the server answers the flush ping. */
  readonly flush: () => Effect.Effect<void, ConnectionClosedError>
}

export class NatsTransport extends Context.Service<NatsTransport, Interface>()(
  "effect-nats/NatsTransport"
) {}

/**
 * Builds the transport over an already-parameterized connection.
 *
 * Release is idempotent and ordered: one bounded drain attempt, then close —
 * the forced-close fallback runs even when the drain fails or exceeds the
 * optional `drainTimeout`. The underlying drain promise cannot be
 * cancelled, so a timeout abandons (not cancels) it.
 */
export const layerTransport = (
  connection: Connection,
  options?: {
    readonly drainTimeout?: Duration.Input | undefined
  }
) => Layer.effect(NatsTransport, transportFromConnection(connection, options?.drainTimeout))

const transportFromConnection = (connection: Connection, drainTimeout?: Duration.Input) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      let released = false

      const release = Effect.gen(function*() {
        if (released) return
        released = true
        // Finalizer outcomes are observable, never typed failures: a release
        // that fails or times out logs the fact and proceeds to forced close.
        const drainAttempt = Effect.tryPromise({
          try: () => connection.drain(),
          catch: (cause) =>
            new ConnectionClosedError({
              operation: "NatsTransport.release/drain",
              cause
            })
        }).pipe(
          Effect.catch((error) => Effect.logWarning("NatsTransport release: drain failed", error))
        )
        yield* drainTimeout !== undefined
          ? drainAttempt.pipe(
            Effect.timeoutOrElse({
              duration: drainTimeout,
              orElse: () =>
                Effect.logWarning(
                  "NatsTransport release: drain deadline passed; abandoning drain and forcing close"
                )
            })
          )
          : drainAttempt
        // close is not covered by the ADR-0007 correctness-over-liveness drain
        // rule: it should settle promptly, so it gets a fixed bound
        yield* Effect.tryPromise({
          try: () => connection.close(),
          catch: (cause) =>
            new ConnectionClosedError({ operation: "NatsTransport.release/close", cause })
        }).pipe(
          Effect.catch((error) => Effect.logWarning("NatsTransport release: close failed", error)),
          Effect.timeoutOrElse({
            duration: "5 seconds",
            orElse: () => Effect.logWarning("NatsTransport release: close did not settle in 5s")
          })
        )
      })

      const flush = Effect.fn("NatsTransport.flush")(function*(): Generator<
        Effect.Effect<void, ConnectionClosedError>,
        void
      > {
        if (connection.isClosed()) {
          return yield* new ConnectionClosedError({ operation: "NatsTransport.flush" })
        }
        yield* Effect.tryPromise({
          try: () => connection.flush(),
          catch: (cause) => new ConnectionClosedError({ operation: "NatsTransport.flush", cause })
        })
      })

      return { flush, release }
    }),
    (service) => service.release
  )

/**
 * Production default for the supervised first-connect loop: exponential
 * backoff, jittered, with a thirty-second retry window. The window is
 * evaluated between attempts, so an in-flight attempt may land past it
 * (Schedule.upTo is a retry-window bound, not an attempt deadline). Bounded
 * and jittered per the repository style; exhaustion surfaces typed.
 */
const defaultConnectSchedule = Schedule.exponential("200 millis").pipe(
  Schedule.jittered,
  Schedule.upTo({ duration: "30 seconds" })
)

/**
 * Builds the transport by acquiring a connection through the
 * `Connector` seam under a supervised retry policy.
 *
 * Only transient issues are retried, on the given bounded schedule (bounded,
 * jittered default); fatal issues fall through immediately and unretried.
 * Schedule exhaustion surfaces once as `ConnectExhaustedError` carrying the
 * attempt count and last transient issue. Release matches
 * `layerTransport`.
 */
export const layerConnecting = (options: {
  readonly servers: ReadonlyArray<string>
  readonly reconnect?: ReconnectPolicy | undefined
  readonly schedule?: Schedule.Schedule<unknown> | undefined
  readonly drainTimeout?: Duration.Input | undefined
}) =>
  Layer.effect(
    NatsTransport,
    Effect.gen(function*() {
      const connector = yield* Connector
      let attempts = 0
      const connection = yield* Effect.suspend(() => {
        // Re-evaluated once per retry, so the count equals every invocation
        // the schedule drives - including the final one that exhausts it.
        attempts++
        return connector.connect({ servers: options.servers, reconnect: options.reconnect })
      }).pipe(
        Effect.retry({
          schedule: options.schedule ?? defaultConnectSchedule,
          while: (issue) => issue._tag === "ConnectTransientError"
        }),
        Effect.catch((issue) =>
          Effect.gen(function*() {
            if (issue._tag !== "ConnectTransientError") {
              return yield* issue
            }
            yield* Effect.logError("NatsTransport connect exhausted", issue)
            return yield* new ConnectExhaustedError({
              operation: "NatsTransport.connect",
              attempts,
              last: issue
            })
          })
        )
      )
      return yield* transportFromConnection(connection, options.drainTimeout)
    })
  )
