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
export interface SubscriptionBridge<A, E = never> {
  /** Synchronous, non-suspending admission for callback producers. */
  readonly offerUnsafe: (value: A) => SlowConsumerError | ConnectionClosedError | undefined
  readonly offer: (
    value: A
  ) => Effect.Effect<void, SlowConsumerError | ConnectionClosedError>
  /** Terminates the stream with an adapter error and rejects later admission. */
  readonly failUnsafe: (error: E) => boolean
  /** Ends the stream synchronously and rejects later admission. */
  readonly endUnsafe: () => boolean
  readonly end: Effect.Effect<void>
  readonly stream: Stream.Stream<A, SlowConsumerError | E>
}

export const make = <A, E = never>(options: {
  readonly operation: string
  readonly subject: string
  readonly capacity: number
}): Effect.Effect<SubscriptionBridge<A, E>> =>
  Effect.gen(function*() {
    if (!Number.isInteger(options.capacity) || options.capacity <= 0) {
      return yield* Effect.die(
        new Error(`SubscriptionBridge capacity must be a positive integer, got ${options.capacity}`)
      )
    }
    const queue = yield* Queue.bounded<A, Cause.Done | SlowConsumerError | E>(options.capacity)
    let admissionTerminal: SlowConsumerError | ConnectionClosedError | undefined
    let ended = false

    const offerUnsafe = (value: A): SlowConsumerError | ConnectionClosedError | undefined => {
      if (admissionTerminal !== undefined) return admissionTerminal
      if (!ended && Queue.offerUnsafe(queue, value)) return undefined
      if (ended) {
        const error = new ConnectionClosedError({ operation: options.operation })
        admissionTerminal = error
        return error
      }
      const error = new SlowConsumerError({
        operation: options.operation,
        subject: options.subject,
        capacity: options.capacity,
        pending: Queue.sizeUnsafe(queue)
      })
      admissionTerminal = error
      Queue.failCauseUnsafe(queue, Cause.fail(error))
      return error
    }

    const offer = (value: A): Effect.Effect<void, SlowConsumerError | ConnectionClosedError> => {
      const error = offerUnsafe(value)
      return error === undefined ? Effect.void : Effect.fail(error)
    }

    const failUnsafe = (error: E): boolean => {
      if (ended || admissionTerminal !== undefined) return false
      ended = true
      admissionTerminal = new ConnectionClosedError({ operation: options.operation })
      return Queue.failCauseUnsafe(queue, Cause.fail(error))
    }

    const endUnsafe = (): boolean => {
      if (ended || admissionTerminal !== undefined) return false
      ended = true
      admissionTerminal = new ConnectionClosedError({ operation: options.operation })
      return Queue.endUnsafe(queue)
    }

    const end: Effect.Effect<void> = Effect.sync(() => {
      endUnsafe()
    })

    return {
      offerUnsafe,
      offer,
      failUnsafe,
      endUnsafe,
      end,
      stream: Stream.fromQueue(queue)
    }
  })
