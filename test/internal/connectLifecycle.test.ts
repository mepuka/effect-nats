import { expect, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Queue, Schedule } from "effect"
import type { Duration, Fiber as FiberT } from "effect"
import { TestClock } from "effect/testing"
import { ConnectFatalError, ConnectTransientError } from "../../src/internal/Errors.ts"
import { layerConnecting, NatsTransport } from "../../src/internal/NatsTransport.ts"
import { type FakeConnect, makeFakeConnect } from "./kit.ts"

const servers = ["nats://localhost:4222"]

const refused = () => new ConnectTransientError({ operation: "connect", reason: "refused" })

/** Flips the acquisition failure of the connecting layer over the fake. */
const acquireFailure = (fake: FakeConnect, schedule?: Schedule.Schedule<Duration.Duration>) =>
  Effect.flip(
    Effect.scoped(
      Layer.build(Layer.provide(layerConnecting({ servers, schedule }), fake.layer))
    )
  )

/**
 * Advances virtual time in fixed steps until the fiber exits. Registration of
 * a backoff sleep happens one scheduler turn after the attempt it follows, so
 * a single blind `adjust` can race the child; stepping until `pollUnsafe`
 * observes an exit keeps the drive deterministic under TestClock.
 */
const advanceToExit = <A, E>(fiber: FiberT.Fiber<A, E>) =>
  Effect.gen(function*() {
    for (;;) {
      yield* TestClock.adjust("1 second")
      if (fiber.pollUnsafe() !== undefined) return
    }
  })

it.effect("retries a transient refusal once and acquires the next connection", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnect([refused(), "succeed"])
    const program = Effect.scoped(
      NatsTransport.use((transport) => transport.flush()).pipe(
        Effect.provide(
          Layer.provide(
            layerConnecting({ servers, schedule: Schedule.spaced("1 second") }),
            fake.layer
          )
        )
      )
    ).pipe(Effect.forkChild)
    const fiber = yield* program
    yield* Queue.take(fake.firstAttempt)
    yield* advanceToExit(fiber)
    yield* Fiber.join(fiber)
    expect(fake.observed.connectCalls).toBe(2)
    expect(fake.observed.attemptTimes).toEqual([0, 1000])
    expect(fake.connections).toHaveLength(1)
    expect(fake.connections[0]?.observed.flushCalls).toBe(1)
    expect(fake.connections[0]?.observed.drainCalls).toBe(1)
    expect(fake.connections[0]?.observed.closeCalls).toBe(1)
  }))

it.effect("a fatal auth issue surfaces unwrapped after exactly one attempt", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnect([
      new ConnectFatalError({ operation: "connect", reason: "auth", detail: "bad credentials" })
    ])
    const failure = yield* acquireFailure(fake)
    expect(failure._tag).toBe("ConnectFatalError")
    if (failure._tag === "ConnectFatalError") {
      expect(failure.reason).toBe("auth")
    }
    expect(fake.observed.connectCalls).toBe(1)
  }))

it.effect("exhaustion surfaces typed with attempt count and last issue", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnect([refused()])
    const schedule = Schedule.exponential("1 second").pipe(
      Schedule.upTo({ duration: "10 seconds" })
    )
    const fiber = yield* acquireFailure(fake, schedule).pipe(Effect.forkChild)
    yield* Queue.take(fake.firstAttempt)
    yield* advanceToExit(fiber)
    const failure = yield* Fiber.join(fiber)
    expect(failure._tag).toBe("ConnectExhaustedError")
    if (failure._tag === "ConnectExhaustedError") {
      expect(failure.last.reason).toBe("refused")
      expect(failure.attempts).toBe(5)
    }
    // exponential 1s base grants delays 1s, 2s, 4s, 8s: the duration bound is
    // detected on the step FOLLOWING the one that crosses it (7s elapsed still
    // grants the 8s delay), so the fifth attempt lands past the bound
    expect(fake.observed.connectCalls).toBe(5)
    expect(fake.observed.attemptTimes).toEqual([0, 1000, 3000, 7000, 15000])
  }))

it.effect("interruption during backoff completes promptly and schedules no retry", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnect([refused()])
    const schedule = Schedule.spaced("5 seconds")
    const acquisition = Layer.build(
      Layer.provide(layerConnecting({ servers, schedule }), fake.layer)
    ).pipe(Effect.scoped, Effect.asVoid, Effect.forkChild)
    const fiber = yield* acquisition
    yield* Queue.take(fake.firstAttempt)
    yield* Fiber.interrupt(fiber)
    yield* TestClock.adjust("1 hour")
    expect(fake.observed.connectCalls).toBe(1)
  }))
