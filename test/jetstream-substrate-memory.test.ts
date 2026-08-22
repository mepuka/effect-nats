import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Queue, Stream } from "effect"
import * as Memory from "../src/internal/jetstream/memory.ts"
import type { ConsumeEvent, StoredMessage } from "../src/internal/jetstream/substrate.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()

const kvConfig = (bucket: string) => ({
  name: `KV_${bucket}`,
  subjects: [`$KV.${bucket}.>`],
  maxMessagesPerSubject: 0,
  allowRollup: true
})

const text = (message: StoredMessage): string => decoder.decode(message.payload)

// ADR-0005: consumption declares its buffer policy explicitly.
const pull = { _tag: "PullWindow", messages: 64 } as const

// Pumps a consume stream into a queue so tests can await events one at a time
// without sleeps: taking the CaughtUp event proves registration completed.
const pump = <E>(events: Stream.Stream<ConsumeEvent, E>) =>
  Effect.gen(function*() {
    const queue = yield* Queue.unbounded<ConsumeEvent>()
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(events, (event) => Queue.offer(queue, event)),
      { startImmediately: true }
    )
    return { queue, fiber }
  })

it.effect("creates, gets, and idempotently re-creates streams", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    const config = yield* service.getStream("KV_b")
    expect(config.subjects).toEqual(["$KV.b.>"])
    // identical config: idempotent success (server deep-equal semantics)
    yield* service.createStream(kvConfig("b"))
    // differing config: typed conflict
    const conflict = yield* Effect.flip(
      service.createStream({ ...kvConfig("b"), maxMessagesPerSubject: 5 })
    )
    expect(conflict._tag).toBe("StreamConfigConflict")
    const missing = yield* Effect.flip(service.getStream("KV_nope"))
    expect(missing._tag).toBe("StreamNotFound")
  }))

it.effect("assigns monotonic sequences and rejects unbound subjects", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    const s1 = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("one"))
    const s2 = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("two"))
    expect([s1, s2]).toEqual([1n, 2n])
    const unbound = yield* Effect.flip(service.publish("KV_b", "$KV.other.k", utf8.encode("x")))
    expect(unbound._tag).toBe("SubjectNotBound")
    const gone = yield* Effect.flip(service.publish("KV_nope", "$KV.b.k", utf8.encode("x")))
    expect(gone._tag).toBe("StreamNotFound")
  }))

it.effect("enforces per-subject compare-and-set", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    // create-if-absent: expected 0 on a fresh subject
    const first = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"), {
      expectedLastSubjectSequence: 0n
    })
    expect(first).toBe(1n)
    // stale expectation: typed conflict carrying both sides
    const conflict = yield* Effect.flip(
      service.publish("KV_b", "$KV.b.k", utf8.encode("v2"), { expectedLastSubjectSequence: 0n })
    )
    expect(conflict._tag).toBe("WrongLastSequence")
    if (conflict._tag === "WrongLastSequence") {
      expect(conflict.expected).toBe(0n)
      expect(conflict.actual).toBe(1n)
    }
    // per-subject isolation: another subject's sequence does not interfere
    yield* service.publish("KV_b", "$KV.b.other", utf8.encode("x"))
    const second = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v2"), {
      expectedLastSubjectSequence: 1n
    })
    expect(second).toBe(3n)
  }))

it.effect("returns the last message per subject and typed absence", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("old"))
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("new"))
    const last = yield* service.lastMessageForSubject("KV_b", "$KV.b.k")
    expect(text(last)).toBe("new")
    expect(last.sequence).toBe(2n)
    const absent = yield* Effect.flip(service.lastMessageForSubject("KV_b", "$KV.b.none"))
    expect(absent._tag).toBe("NoMessageForSubject")
  }))

it.effect("prunes history beyond maxMessagesPerSubject, oldest first", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream({ ...kvConfig("b"), maxMessagesPerSubject: 2 })
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"))
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v2"))
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v3"))
    const events = yield* Stream.runCollect(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "AllHistory" }, buffer: pull })
        .pipe(
          Stream.takeWhile((event) => event._tag !== "CaughtUp")
        )
    )
    const values = events.map((event) => event._tag === "Entry" ? text(event.message) : "?")
    expect(values).toEqual(["v2", "v3"])
  }))

it.effect("rollup removes prior messages for that subject only", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"))
    yield* service.publish("KV_b", "$KV.b.other", utf8.encode("keep"))
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("purged"), {
      headers: new Map([["Nats-Rollup", "sub"]])
    })
    const events = yield* Stream.runCollect(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "AllHistory" }, buffer: pull })
        .pipe(
          Stream.takeWhile((event) => event._tag !== "CaughtUp")
        )
    )
    const values = events.map((event) => event._tag === "Entry" ? text(event.message) : "?")
    expect(values).toEqual(["keep", "purged"])
    const denied = yield* Memory.make.pipe(
      Effect.flatMap((m) =>
        m.service.createStream({ ...kvConfig("x"), allowRollup: false }).pipe(
          Effect.andThen(
            Effect.flip(m.service.publish("KV_x", "$KV.x.k", utf8.encode("v"), {
              headers: new Map([["Nats-Rollup", "sub"]])
            }))
          )
        )
      )
    )
    expect(denied._tag).toBe("RollupNotPermitted")
  }))

it.effect("consume replays history, marks CaughtUp, then continues live", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    yield* service.publish("KV_b", "$KV.b.a", utf8.encode("e1"))
    yield* service.publish("KV_b", "$KV.b.b", utf8.encode("e2"))
    const { fiber, queue } = yield* pump(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "AllHistory" }, buffer: pull })
    )
    const replay1 = yield* Queue.take(queue)
    const replay2 = yield* Queue.take(queue)
    const marker = yield* Queue.take(queue)
    expect(replay1._tag === "Entry" && text(replay1.message)).toBe("e1")
    expect(replay2._tag === "Entry" && text(replay2.message)).toBe("e2")
    expect(marker._tag).toBe("CaughtUp")
    // CaughtUp taken => registration done => this publish is observed live
    yield* service.publish("KV_b", "$KV.b.c", utf8.encode("e3"))
    const live = yield* Queue.take(queue)
    expect(live._tag === "Entry" && text(live.message)).toBe("e3")
    yield* Fiber.interrupt(fiber)
  }))

it.effect("consume start positions: LastPerSubject, NewOnly, FromSequence", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    yield* service.publish("KV_b", "$KV.b.k1", utf8.encode("k1-old"))
    yield* service.publish("KV_b", "$KV.b.k1", utf8.encode("k1-new"))
    yield* service.publish("KV_b", "$KV.b.k2", utf8.encode("k2"))

    const lastPer = yield* Stream.runCollect(
      service.consume("KV_b", {
        filters: ["$KV.b.>"],
        start: { _tag: "LastPerSubject" },
        buffer: pull
      }).pipe(
        Stream.takeWhile((event) => event._tag !== "CaughtUp")
      )
    )
    expect(lastPer.map((e) => e._tag === "Entry" ? text(e.message) : "?")).toEqual(["k1-new", "k2"])

    const fromSeq = yield* Stream.runCollect(
      service.consume("KV_b", {
        filters: ["$KV.b.>"],
        start: { _tag: "FromSequence", sequence: 2n },
        buffer: pull
      }).pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
    )
    expect(fromSeq.map((e) => e._tag === "Entry" ? text(e.message) : "?")).toEqual(["k1-new", "k2"])

    const { fiber, queue } = yield* pump(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "NewOnly" }, buffer: pull })
    )
    const marker = yield* Queue.take(queue)
    expect(marker._tag).toBe("CaughtUp")
    yield* service.publish("KV_b", "$KV.b.k3", utf8.encode("k3"))
    const live = yield* Queue.take(queue)
    expect(live._tag === "Entry" && text(live.message)).toBe("k3")
    yield* Fiber.interrupt(fiber)
  }))

it.effect("consume filters restrict delivery per subscription", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    yield* service.publish("KV_b", "$KV.b.auth.user", utf8.encode("in"))
    yield* service.publish("KV_b", "$KV.b.other", utf8.encode("out"))
    const events = yield* Stream.runCollect(
      service.consume("KV_b", {
        filters: ["$KV.b.auth.>"],
        start: { _tag: "AllHistory" },
        buffer: pull
      }).pipe(
        Stream.takeWhile((event) => event._tag !== "CaughtUp")
      )
    )
    expect(events.map((e) => e._tag === "Entry" ? text(e.message) : "?")).toEqual(["in"])
  }))

it.effect("deleting a stream fails active consumers with StreamNotFound", () =>
  Effect.gen(function*() {
    const { service } = yield* Memory.make
    yield* service.createStream(kvConfig("b"))
    const { fiber, queue } = yield* pump(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "NewOnly" }, buffer: pull })
    )
    const marker = yield* Queue.take(queue)
    expect(marker._tag).toBe("CaughtUp")
    yield* service.deleteStream("KV_b")
    const exit = yield* Effect.flip(Fiber.join(fiber))
    expect(exit._tag).toBe("StreamNotFound")
    const gone = yield* Effect.flip(service.getStream("KV_b"))
    expect(gone._tag).toBe("StreamNotFound")
  }))

it.effect("subscriber accounting returns to baseline after completion and interruption", () =>
  Effect.gen(function*() {
    const memory = yield* Memory.make
    const { service } = memory
    yield* service.createStream(kvConfig("b"))
    expect(yield* memory.subscriberCount("KV_b")).toBe(0)
    const a = yield* pump(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "NewOnly" }, buffer: pull })
    )
    const b = yield* pump(
      service.consume("KV_b", { filters: ["$KV.b.>"], start: { _tag: "NewOnly" }, buffer: pull })
    )
    expect((yield* Queue.take(a.queue))._tag).toBe("CaughtUp")
    expect((yield* Queue.take(b.queue))._tag).toBe("CaughtUp")
    expect(yield* memory.subscriberCount("KV_b")).toBe(2)
    // interruption mid-flight releases the subscription in bounded time
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
      if (event._tag === "Entry") seen.push(text(event.message))
    }
    yield* Fiber.join(publisher)
    expect(seen).toEqual(["v1", "v2", "v3", "v4", "v5"])
    yield* Fiber.interrupt(fiber)
  }))

it.effect("a TerminateOnLag buffer fails the lagging subscription typed, sparing the publisher", () =>
  Effect.gen(function*() {
    const memory = yield* Memory.make
    const { service } = memory
    yield* service.createStream(kvConfig("b"))
    // Deterministic protocol: the consumer signals `pulled` on each live entry
    // and then waits on `gate`, so exactly one entry is in flight while the
    // capacity-1 buffer holds the next and the one after overflows.
    const pulled = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const events = service.consume("KV_b", {
      filters: ["$KV.b.>"],
      start: { _tag: "NewOnly" },
      buffer: { _tag: "TerminateOnLag", messages: 1 }
    })
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(events, (event) =>
        event._tag === "CaughtUp"
          ? Effect.void
          : Deferred.succeed(pulled, void 0).pipe(Effect.andThen(Deferred.await(gate)))),
      { startImmediately: true }
    )
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"))
    yield* Deferred.await(pulled) // seq 1 pulled; the buffer is empty again
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v2")) // buffered
    yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v3")) // overflows
    // the publisher is unaffected by the lagged subscriber's failure
    const publisherStillFine = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v4"))
    expect(publisherStillFine).toBe(4n)
    expect(yield* memory.subscriberCount("KV_b")).toBe(0)
    yield* Deferred.succeed(gate, void 0)
    const failure = yield* Effect.flip(Fiber.join(fiber))
    expect(failure._tag).toBe("ConsumerLagged")
    if (failure._tag === "ConsumerLagged") {
      expect(failure.lastDeliveredSequence).toBe(2n)
    }
  }))
