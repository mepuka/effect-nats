import { expect, it } from "@effect/vitest"
import { Cause, Effect, Exit, Fiber, Latch, Scope, Stream } from "effect"
import * as CoreSubscription from "../src/internal/CoreSubscription.ts"

const makeHarness = () => {
  let callback: ((error: Error | null, value: number) => void) | undefined
  let settleClosed: (error: void | Error) => void = () => {}
  const closed = new Promise<void | Error>((resolve) => {
    settleClosed = resolve
  })
  let unsubscribeCalls = 0
  let iteratorAccesses = 0

  const connection: CoreSubscription.Connection<number> = {
    subscribe: (_subject, options) => {
      callback = options.callback
      return {
        closed,
        unsubscribe: () => {
          unsubscribeCalls++
        },
        [Symbol.asyncIterator]: () => {
          iteratorAccesses++
          throw new Error("the Core adapter must not use the official iterator")
        }
      }
    }
  }

  return {
    connection,
    deliver: (value: number) => callback?.(null, value),
    fail: (error: Error) => callback?.(error, 0),
    settleClosed,
    unsubscribeCalls: () => unsubscribeCalls,
    iteratorAccesses: () => iteratorAccesses
  }
}

it.effect("scope release unsubscribes once, wakes a parked pull, and rejects late delivery", () =>
  Effect.gen(function*() {
    const harness = makeHarness()
    const scope = yield* Scope.make()
    const source = yield* CoreSubscription.make(harness.connection, {
      subject: "orders.created",
      capacity: 8
    }).pipe(Scope.provide(scope))
    const seen: Array<number> = []
    const first = yield* Latch.make()
    const consumer = yield* source.messages.pipe(
      Stream.tap((value) => Effect.sync(() => seen.push(value)).pipe(Effect.andThen(first.open))),
      Stream.runDrain,
      Effect.forkChild
    )

    harness.deliver(1)
    yield* first.await
    yield* Scope.close(scope, Exit.void)
    yield* Fiber.join(consumer)

    harness.deliver(2)
    yield* Effect.yieldNow
    expect(seen).toEqual([1])
    expect(harness.unsubscribeCalls()).toBe(1)
    expect(harness.iteratorAccesses()).toBe(0)
  }))

it.effect("scope release completes an idle stream with no latent blocked pull", () =>
  Effect.gen(function*() {
    const harness = makeHarness()
    const scope = yield* Scope.make()
    const source = yield* CoreSubscription.make(harness.connection, {
      subject: "orders.created",
      capacity: 8
    }).pipe(Scope.provide(scope))
    const consumer = yield* Stream.runDrain(source.messages).pipe(Effect.forkChild)
    yield* Effect.yieldNow

    yield* Scope.close(scope, Exit.void)

    yield* Fiber.join(consumer)
    expect(harness.unsubscribeCalls()).toBe(1)
  }))

it.effect("overflow rejects the triggering message, drains admitted messages, and unsubscribes", () =>
  Effect.gen(function*() {
    const harness = makeHarness()
    const outcome = yield* Effect.scoped(
      Effect.gen(function*() {
        const source = yield* CoreSubscription.make(harness.connection, {
          subject: "orders.created",
          capacity: 1
        })
        harness.deliver(1)
        harness.deliver(2)
        const seen: Array<number> = []
        const exit = yield* source.messages.pipe(
          Stream.runForEach((value) => Effect.sync(() => seen.push(value))),
          Effect.exit
        )
        return { exit, seen }
      })
    )

    expect(outcome.seen).toEqual([1])
    expect(outcome.exit._tag).toBe("Failure")
    if (outcome.exit._tag === "Failure") {
      const failure = outcome.exit.cause.reasons.find(Cause.isFailReason)
      expect(failure?.error._tag).toBe("SlowConsumerError")
    }
    expect(harness.unsubscribeCalls()).toBe(1)
  }))

it.effect("an official callback error fails the stream with its preserved cause", () =>
  Effect.scoped(
    Effect.gen(function*() {
      const harness = makeHarness()
      const source = yield* CoreSubscription.make(harness.connection, {
        subject: "orders.created",
        capacity: 8
      })
      const cause = new Error("permissions violation")
      harness.fail(cause)

      const failure = yield* Effect.flip(Stream.runDrain(source.messages))
      expect(failure._tag).toBe("ConnectionClosedError")
      expect(failure.cause).toBe(cause)
      expect(harness.unsubscribeCalls()).toBe(1)
    })
  ))
