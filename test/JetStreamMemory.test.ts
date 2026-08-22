import { expect, it } from "@effect/vitest"
import { Effect, Fiber, Queue, Stream } from "effect"
import type { ConsumeEvent } from "../src/internal/JetStream.ts"
import * as Memory from "../src/internal/JetStreamMemory.ts"
import { conformance } from "./JetStreamConformance.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()

const kvConfig = (bucket: string) => ({
  name: `KV_${bucket}`,
  subjects: [`$KV.${bucket}.>`],
  maxMessagesPerSubject: 0,
  allowRollup: true
})

conformance("memory", (f) => Effect.flatMap(Memory.make, (memory) => f(memory.service)))

// Memory-only observations below: the diagnostic subscriber counter and the
// PullWindow suspension contract (live PullWindow arrives with the
// pull-consumer slice, ADR-0005).

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
