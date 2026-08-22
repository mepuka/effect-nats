import { Cause, Effect, Queue, Stream } from "effect"
import { ConnectionClosedError, SlowConsumerError } from "./Errors.ts"

/**
 * Bridges NATS delivery into a pull-based Effect Stream.
 *
 * The bridge owns one finite queue with the caller-declared `capacity`.
 * Overflow policy is fail-and-terminate: when admission exceeds capacity the
 * offer fails, the stream fails with the same `SlowConsumerError` instance,
 * and further offers keep failing it. Buffered values are not delivered
 * because they were never processed.
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
