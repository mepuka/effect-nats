import { Effect, Stream } from "effect"
import type { Scope } from "effect"
import { ConnectionClosedError, SlowConsumerError } from "./Errors.ts"
import { make as makeSubscriptionBridge } from "./SubscriptionBridge.ts"

/** The callback-only official subscription subset used by this adapter. */
export interface Subscription<A> extends AsyncIterable<A> {
  readonly closed: Promise<void | Error>
  readonly unsubscribe: () => void
}

/** The callback-only official connection subset used by this adapter. */
export interface Connection<A> {
  readonly subscribe: (
    subject: string,
    options: {
      readonly queue?: string
      readonly callback: (error: Error | null, value: A) => void
    }
  ) => Subscription<A>
}

export interface Interface<A> {
  readonly messages: Stream.Stream<A, SlowConsumerError | ConnectionClosedError>
}

/**
 * Acquires one official Core callback subscription in the current Scope.
 *
 * The official parser invokes callbacks synchronously and cannot be suspended,
 * so delivery crosses one positive finite message-capacity queue. Overflow is
 * terminal with drain-first ordering: the triggering message is rejected,
 * admitted messages drain, then the stream fails with `SlowConsumerError`.
 *
 * Release flips the local admission gate before calling official
 * `unsubscribe()`, then ends the queue. A callback that arrives after release
 * cannot enqueue. The official async iterator is never constructed.
 */
export const make = <A>(
  connection: Connection<A>,
  options: {
    readonly subject: string
    readonly queue?: string | undefined
    readonly capacity: number
  }
): Effect.Effect<Interface<A>, ConnectionClosedError, Scope.Scope> =>
  Effect.gen(function*() {
    const bridge = yield* makeSubscriptionBridge<A, ConnectionClosedError>({
      operation: "Core.subscribe",
      subject: options.subject,
      capacity: options.capacity
    })
    let active = true
    let subscription: Subscription<A> | undefined

    const unsubscribe = (current: Subscription<A>) => {
      if (!active) return false
      active = false
      current.unsubscribe()
      return true
    }

    const callback = (cause: Error | null, value: A) => {
      if (!active) return
      if (cause !== null) {
        active = false
        bridge.failUnsafe(
          new ConnectionClosedError({ operation: "Core.subscribe/callback", cause })
        )
        subscription?.unsubscribe()
        return
      }
      const admission = bridge.offerUnsafe(value)
      if (admission?._tag === "SlowConsumerError") {
        active = false
        subscription?.unsubscribe()
      }
    }

    const current = yield* Effect.acquireRelease(
      Effect.try({
        try: () => {
          const created = connection.subscribe(options.subject, {
            ...(options.queue === undefined ? {} : { queue: options.queue }),
            callback
          })
          subscription = created
          // Defensive for structural adapters that can invoke the callback
          // synchronously during subscribe(). Official nats.js does not.
          if (!active) created.unsubscribe()
          return created
        },
        catch: (cause) => new ConnectionClosedError({ operation: "Core.subscribe/register", cause })
      }),
      (created) =>
        Effect.sync(() => {
          try {
            unsubscribe(created)
          } finally {
            bridge.endUnsafe()
          }
        })
    )

    current.closed.then(
      (cause) => {
        if (!active) return
        active = false
        if (cause instanceof Error) {
          bridge.failUnsafe(
            new ConnectionClosedError({ operation: "Core.subscribe/closed", cause })
          )
        } else {
          bridge.endUnsafe()
        }
      },
      (cause: unknown) => {
        if (!active) return
        active = false
        bridge.failUnsafe(
          new ConnectionClosedError({ operation: "Core.subscribe/closed", cause })
        )
      }
    )

    return { messages: bridge.stream }
  })
