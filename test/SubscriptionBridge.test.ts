import { expect, it } from "@effect/vitest"
import { Effect, Fiber, Latch, Stream } from "effect"
import { make } from "../src/internal/SubscriptionBridge.ts"

const options = {
  capacity: 2,
  operation: "Core.subscribe",
  subject: "orders.*"
} as const

it.effect("delivers offered values in arrival order", () =>
  Effect.gen(function*() {
    const bridge = yield* make<number>({ ...options, capacity: 8 })
    yield* bridge.offer(1)
    yield* bridge.offer(2)
    yield* bridge.offer(3)
    yield* bridge.end
    const values = yield* Stream.runCollect(bridge.stream)
    expect(values).toEqual([1, 2, 3])
  }))

it.effect("offer beyond capacity fails naming capacity and pending", () =>
  Effect.gen(function*() {
    const bridge = yield* make<number>(options)
    yield* bridge.offer(1)
    yield* bridge.offer(2)
    const failure = yield* Effect.flip(bridge.offer(3))
    expect(failure._tag).toBe("SlowConsumerError")
    if (failure._tag === "SlowConsumerError") {
      expect(failure.capacity).toBe(2)
      expect(failure.pending).toBe(2)
    }
  }))

it.effect("stream terminates with the same error after overflow", () =>
  Effect.gen(function*() {
    const bridge = yield* make<number>(options)
    yield* bridge.offer(1)
    yield* bridge.offer(2)
    const offerFailure = yield* Effect.flip(bridge.offer(3))
    const streamFailure = yield* Effect.flip(Stream.runCollect(bridge.stream))
    expect(streamFailure).toBe(offerFailure)
  }))

it.effect("admission remains stopped after overflow", () =>
  Effect.gen(function*() {
    const bridge = yield* make<number>(options)
    yield* bridge.offer(1)
    yield* bridge.offer(2)
    const first = yield* Effect.flip(bridge.offer(3))
    const second = yield* Effect.flip(bridge.offer(4))
    expect(second).toBe(first)
  }))

it.effect("offer after end fails as closed while buffered values still drain", () =>
  Effect.gen(function*() {
    const bridge = yield* make<number>({ ...options, capacity: 8 })
    yield* bridge.offer(1)
    yield* bridge.offer(2)
    yield* bridge.end
    const late = yield* Effect.flip(bridge.offer(3))
    expect(late._tag).toBe("ConnectionClosedError")
    const values = yield* Stream.runCollect(bridge.stream)
    expect(values).toEqual([1, 2])
  }))

it.effect("interrupting a blocked consumer leaves no phantom delivery", () =>
  Effect.gen(function*() {
    const bridge = yield* make<number>({ ...options, capacity: 8 })
    const seen: Array<number> = []
    const deliveredFirst = yield* Latch.make()
    const consumer = yield* bridge.stream.pipe(
      Stream.tap((n) =>
        Effect.andThen(
          Effect.sync(() => {
            seen.push(n)
          }),
          deliveredFirst.open
        )
      ),
      Stream.runCollect,
      Effect.asVoid,
      Effect.forkScoped
    )
    yield* bridge.offer(1)
    yield* deliveredFirst.await
    // the consumer pulled one value and is now blocked pulling the next
    yield* Fiber.interrupt(consumer)
    expect(seen).toEqual([1])
    yield* bridge.end
    const values = yield* Stream.runCollect(bridge.stream)
    expect(values).toEqual([])
  }))
