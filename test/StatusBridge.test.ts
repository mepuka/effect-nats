import { expect, it } from "@effect/vitest"
import { Cause, Effect, Exit, Fiber, Queue, Scope, Stream } from "effect"
import { make as makeStatusBridge } from "../src/internal/StatusBridge.ts"
import { makeFakeConnection, statusOf } from "./Kit.ts"

it.effect("delivers statuses until the server ends the feed, then completes", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const collected = yield* Effect.scoped(
      makeStatusBridge({ connection: fake.connection, capacity: 8 }).pipe(
        Effect.flatMap(({ events }) =>
          Effect.gen(function*() {
            const collector = yield* Stream.runCollect(events).pipe(Effect.forkChild)
            // deterministic start: the pump is parked inside its first pull
            yield* Queue.take(fake.statusStarted)
            yield* fake.deliverStatus(statusOf("disconnect"))
            yield* fake.deliverStatus(statusOf("reconnect"))
            yield* fake.endStatus
            return yield* Fiber.join(collector)
          })
        )
      )
    )
    expect(Array.from(collected).map((s) => s.type)).toEqual(["disconnect", "reconnect"])
  }))

it.effect("scope release during a parked pull settles the iterator exactly once", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const scope = yield* Scope.make()
    const program = makeStatusBridge({ connection: fake.connection, capacity: 8 }).pipe(
      Effect.flatMap(({ events }) => Stream.runDrain(events))
    )
    yield* Effect.forkChild(Scope.provide(scope)(program))
    // deterministic parking: the status iterator is inside its first pull
    yield* Queue.take(fake.statusStarted)
    yield* Scope.close(scope, Exit.void)
    expect(fake.observed.statusReturnCalls).toBe(1)
    // late delivery after release must not resurrect the pump or double-return
    yield* fake.deliverStatus(statusOf("ping"))
    yield* Effect.yieldNow
    expect(fake.observed.statusReturnCalls).toBe(1)
  }))

it.effect("overflow terminates with a shared SlowConsumerError", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const outcome = yield* Effect.scoped(
      makeStatusBridge({ connection: fake.connection, capacity: 1 }).pipe(
        Effect.flatMap(({ events }) =>
          Effect.gen(function*() {
            const collector = yield* Stream.runCollect(events).pipe(Effect.exit, Effect.forkChild)
            yield* Queue.take(fake.statusStarted)
            yield* fake.deliverStatus(statusOf("a"))
            yield* fake.deliverStatus(statusOf("b"))
            return yield* Fiber.join(collector)
          })
        )
      )
    )
    expect(outcome._tag).toBe("Failure")
    if (outcome._tag === "Failure") {
      const failReason = outcome.cause.reasons.find(Cause.isFailReason)
      expect(failReason?.error._tag).toBe("SlowConsumerError")
    }
  }))
