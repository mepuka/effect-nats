import { Cause, Effect, Queue, Stream } from "effect"
import { ConnectionClosedError, SlowConsumerError } from "./Errors.ts"

/**
 * Bridges NATS delivery into a pull-based Effect Stream.
 *
 * The bridge owns one finite queue with the caller-declared `capacity`
 * (validated: a positive integer, or construction is a defect). Overflow
 * policy is fail-and-terminate with drain-first ordering: when admission
 * exceeds capacity the offer fails, already-admitted values still drain to
 * the consumer, and the stream then fails with the same `SlowConsumerError`
 * instance; further offers keep failing it. The triggering value is the one
 * rejected - admitted values are never silently dropped (ADR-0005: overflow
 * is terminal and typed, never lossy).
 *
 * After `end`, offers fail with `ConnectionClosedError` while buffered values
 * still drain to the consumer.
 *
 * Producers never suspend: `offer` is a bounded non-suspending admission, so a
 * synchronous NATS callback cannot silently stall delivery.
 */
export interface SubscriptionBridge<A> {
  readonly offer: (
    value: A
  ) => Effect.Effect<void, SlowConsumerError | ConnectionClosedError>
  readonly end: Effect.Effect<void>
  readonly stream: Stream.Stream<A, SlowConsumerError>
}

export const make = <A>(options: {
  readonly operation: string
  readonly subject: string
  readonly capacity: number
}): Effect.Effect<SubscriptionBridge<A>> =>
  Effect.gen(function*() {
    if (!Number.isInteger(options.capacity) || options.capacity <= 0) {
      return yield* Effect.die(
        new Error(`SubscriptionBridge capacity must be a positive integer, got ${options.capacity}`)
      )
    }
    const queue = yield* Queue.bounded<A, Cause.Done | SlowConsumerError>(options.capacity)
    let terminal: SlowConsumerError | ConnectionClosedError | undefined
    let ended = false

    const offer = (
      value: A
    ): Effect.Effect<void, SlowConsumerError | ConnectionClosedError> =>
      Effect.gen(function*() {
        if (terminal !== undefined) return yield* terminal
        if (!ended && Queue.offerUnsafe(queue, value)) return
        const error = ended
          ? new ConnectionClosedError({
            operation: options.operation
          })
          : new SlowConsumerError({
            operation: options.operation,
            subject: options.subject,
            capacity: options.capacity,
            pending: Queue.sizeUnsafe(queue)
          })
        terminal = error
        if (!ended) {
          Queue.failCauseUnsafe(queue, Cause.fail(error as SlowConsumerError))
        }
        return yield* error
      })

    const end: Effect.Effect<void> = Effect.andThen(
      Effect.sync(() => {
        ended = true
      }),
      Effect.asVoid(Queue.end(queue))
    )

    return {
      offer,
      end,
      stream: Stream.fromQueue(queue)
    }
  })
