import { Effect, Stream } from "effect"
import type { Scope } from "effect"
import { ConnectionClosedError, SlowConsumerError } from "./Errors.ts"
import type { Connection, OfficialStatus } from "./NatsTransport.ts"
import { make as makeSubscriptionBridge } from "./SubscriptionBridge.ts"

export interface Interface {
  /**
   * Status events while open. Completes when the official feed ends; fails
   * with `SlowConsumerError` if the bridge overflows.
   */
  readonly events: Stream.Stream<OfficialStatus, SlowConsumerError>
}

/**
 * Bridges the official status feed into a bounded pull-based stream.
 *
 * One scoped fiber pulls the official iterator and admits into the
 * `SubscriptionBridge` queue without suspending; overflow terminates exactly
 * as the subscription contract does. Release order satisfies the rule-11
 * proof obligations: the interrupt canceler settles `iterator.return()` once,
 * before any queue shutdown can matter, and a released pump never offers
 * again. The real official status iterator is the same queued-iterator shape
 * pinned by the evidence tests, whose parked pulls only settle via `stop()`
 * - which is why interruption abandons (never awaits) an in-flight pull.
 */
export const make = ({ connection, capacity }: {
  readonly connection: Connection
  readonly capacity: number
}): Effect.Effect<Interface, never, Scope.Scope> =>
  Effect.gen(function*() {
    const bridge = yield* makeSubscriptionBridge<OfficialStatus>({
      operation: "NatsTransport.status",
      subject: "status",
      capacity
    })

    let released = false

    const makePull = () => {
      const iter = connection.status()[Symbol.asyncIterator]()
      return (): Effect.Effect<IteratorResult<OfficialStatus>, ConnectionClosedError> =>
        Effect.callback<IteratorResult<OfficialStatus>, ConnectionClosedError>((resume) => {
          let settled = false
          iter.next().then(
            (result) => {
              settled = true
              resume(Effect.succeed(result))
            },
            (cause) => {
              settled = true
              resume(
                Effect.fail(
                  new ConnectionClosedError({ operation: "NatsTransport.status/pull", cause })
                )
              )
            }
          )
          // Cleanup on interruption: settle the generator's return once;
          // never await it here, or a queued-iterator-style parked pull would
          // deadlock scope close.
          return Effect.sync(() => {
            if (!settled && !released) void iter.return?.()
            released = true
          })
        })
    }

    const pump = Effect.gen(function*() {
      const pull = makePull()
      for (;;) {
        // Pull failures terminate like feed end: drain what was admitted.
        // Overflow failure needs no end - the bridge already failed its queue
        // with the shared SlowConsumerError.
        const result = yield* Effect.result(pull())
        if (result._tag === "Failure") {
          yield* bridge.end
          return
        }
        if (result.success.done) {
          yield* bridge.end
          return
        }
        const outcome = yield* Effect.result(bridge.offer(result.success.value))
        if (outcome._tag === "Failure") return
      }
    })

    yield* Effect.forkScoped(pump)
    return { events: bridge.stream }
  })
