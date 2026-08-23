import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Exit, Fiber, Queue, Result, Stream } from "effect"
import type { ConsumeEvent } from "../src/internal/JetStream.ts"
import * as Memory from "../src/internal/JetStreamMemory.ts"
import { conformance } from "./JetStreamConformance.ts"
import { leanTraceReplay } from "./LeanTraceReplay.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()

const kvConfig = (bucket: string) => ({
  name: `KV_${bucket}`,
  subjects: [`$KV.${bucket}.>`],
  maxMessagesPerSubject: 0,
  allowRollup: true
})

conformance("memory", (f) => Effect.flatMap(Memory.make, (memory) => f(memory.service)))
leanTraceReplay("memory", (f) => Effect.flatMap(Memory.make, (memory) => f(memory.service)))

// Memory-only observations below: the diagnostic subscriber counter and the
// publisher-suspension face of PullWindow — on this interpreter the publisher
// IS the producer, so a full window suspends the publish itself, which the
// live adapter must never do (its live-only test proves the opposite face).

const pump = <E>(events: Stream.Stream<ConsumeEvent, E>) =>
  Effect.gen(function*() {
    const queue = yield* Queue.unbounded<ConsumeEvent>()
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(events, (event) => Queue.offer(queue, event)),
      { startImmediately: true }
    )
    return { queue, fiber }
  })

it.effect("subscriber accounting returns to baseline after completion and interruption", () =>
  Effect.gen(function*() {
    const memory = yield* Memory.make
    const { service } = memory
    yield* service.createStream(kvConfig("b"))
    expect(yield* memory.subscriberCount("KV_b")).toBe(0)
    const a = yield* pump(
      service.consume("KV_b", {
        filters: ["$KV.b.>"],
        start: { _tag: "NewOnly" },
        buffer: { _tag: "TerminateOnLag", messages: 64 }
      })
    )
    const b = yield* pump(
      service.consume("KV_b", {
        filters: ["$KV.b.>"],
        start: { _tag: "NewOnly" },
        buffer: { _tag: "TerminateOnLag", messages: 64 }
      })
    )
    expect((yield* Queue.take(a.queue))._tag).toBe("CaughtUp")
    expect((yield* Queue.take(b.queue))._tag).toBe("CaughtUp")
    expect(yield* memory.subscriberCount("KV_b")).toBe(2)
    yield* Fiber.interrupt(a.fiber)
    expect(yield* memory.subscriberCount("KV_b")).toBe(1)
    yield* Fiber.interrupt(b.fiber)
    expect(yield* memory.subscriberCount("KV_b")).toBe(0)
  }))

it.effect("a PullWindow buffer suspends production and delivers in order without loss", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    const { fiber, queue } = yield* pump(
      service.consume("KV_b", {
        filters: ["$KV.b.>"],
        start: { _tag: "NewOnly" },
        buffer: { _tag: "PullWindow", messages: 1 }
      })
    )
    expect((yield* Queue.take(queue))._tag).toBe("CaughtUp")
    const publisher = yield* Effect.forkChild(
      Effect.forEach(
        [1, 2, 3, 4, 5],
        (n) => service.publish("KV_b", "$KV.b.k", utf8.encode(`v${n}`)),
        { discard: true }
      ),
      { startImmediately: true }
    )
    const seen: Array<string> = []
    for (let i = 0; i < 5; i++) {
      const event = yield* Queue.take(queue)
      if (event._tag === "Entry") seen.push(decoder.decode(event.message.payload))
    }
    yield* Fiber.join(publisher)
    expect(seen).toEqual(["v1", "v2", "v3", "v4", "v5"])
    yield* Fiber.interrupt(fiber)
  }))

it.effect(
  "memory: PullWindow(1) admits its first entry before CaughtUp is consumed",
  () =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      yield* service.createStream(kvConfig("pull-caught-up"))
      const ready = yield* Deferred.make<void>()
      const pulled = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<void>()
      const observed: Array<string> = []
      const consumer = yield* Effect.forkChild(
        Stream.runForEach(
          service.consume("KV_pull-caught-up", {
            filters: ["$KV.pull-caught-up.>"],
            start: { _tag: "NewOnly" },
            buffer: { _tag: "PullWindow", messages: 1 }
          }),
          (event) =>
            event._tag === "CaughtUp"
              ? Deferred.succeed(ready, void 0).pipe(
                Effect.andThen(Deferred.await(gate)),
                Effect.andThen(Effect.sync(() => observed.push("CaughtUp")))
              )
              : Effect.sync(() => observed.push(decoder.decode(event.message.payload))).pipe(
                Effect.andThen(Deferred.succeed(pulled, void 0)),
                Effect.asVoid
              )
        ),
        { startImmediately: true }
      )

      yield* Deferred.await(ready)
      expect(
        yield* service.publish(
          "KV_pull-caught-up",
          "$KV.pull-caught-up.k",
          utf8.encode("v1")
        )
      ).toBe(1n)
      yield* Deferred.succeed(gate, void 0)
      yield* Deferred.await(pulled)
      expect(observed).toEqual(["CaughtUp", "v1"])
      yield* Fiber.interrupt(consumer)
    })
)

it.effect("invalid ConsumeBuffer capacities die before acquiring a subscription", () =>
  Effect.gen(function*() {
    const memory = yield* Memory.make
    const { service } = memory
    yield* service.createStream(kvConfig("invalid"))

    for (
      const buffer of [
        { _tag: "PullWindow", messages: 0 },
        { _tag: "TerminateOnLag", messages: 0 },
        { _tag: "PullWindow", messages: 1.5 }
      ] as const
    ) {
      const exit = yield* service.consume("KV_invalid", {
        filters: ["$KV.invalid.>"],
        start: { _tag: "NewOnly" },
        buffer
      }).pipe(
        Stream.take(1),
        Stream.runDrain,
        Effect.exit
      )
      expect(Exit.hasDies(exit)).toBe(true)
      const defect = Exit.findDefect(exit)
      expect(Result.isSuccess(defect)).toBe(true)
      if (Result.isSuccess(defect)) {
        expect(defect.success).toMatchObject({ _tag: "InvalidConsumeBuffer" })
      }
      expect(yield* memory.subscriberCount("KV_invalid")).toBe(0)
    }
  }))

it.effect("a full PullWindow suspends unrelated operations until the consumer pulls", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("a"))
    yield* service.createStream(kvConfig("b"))
    const ready = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const firstPublished = yield* Deferred.make<void>()

    const consumer = yield* Effect.forkChild(
      Stream.runForEach(
        service.consume("KV_a", {
          filters: ["$KV.a.>"],
          start: { _tag: "NewOnly" },
          buffer: { _tag: "PullWindow", messages: 1 }
        }),
        (event) =>
          event._tag === "CaughtUp"
            ? Deferred.succeed(ready, void 0).pipe(Effect.andThen(Deferred.await(gate)))
            : Effect.void
      ),
      { startImmediately: true }
    )
    yield* Deferred.await(ready)

    const publisherA = yield* Effect.forkChild(
      Effect.gen(function*() {
        const first = yield* service.publish("KV_a", "$KV.a.k", utf8.encode("a1"))
        yield* Deferred.succeed(firstPublished, void 0)
        const second = yield* service.publish("KV_a", "$KV.a.k", utf8.encode("a2"))
        return [first, second] as const
      }),
      { startImmediately: true }
    )
    yield* Deferred.await(firstPublished)
    yield* Effect.yieldNow
    expect(publisherA.pollUnsafe()).toBeUndefined()

    const publisherB = yield* Effect.forkChild(
      service.publish("KV_b", "$KV.b.k", utf8.encode("b1")),
      { startImmediately: true }
    )
    yield* Effect.yieldNow
    expect(publisherB.pollUnsafe()).toBeUndefined()

    yield* Deferred.succeed(gate, void 0)
    expect(yield* Fiber.join(publisherA)).toEqual([1n, 2n])
    expect(yield* Fiber.join(publisherB)).toBe(1n)
    const b = yield* service.lastMessageForSubject("KV_b", "$KV.b.k")
    expect(decoder.decode(b.payload)).toBe("b1")
    yield* Fiber.interrupt(consumer)
  }))
