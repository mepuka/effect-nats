import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Queue, Stream } from "effect"
import type { ConsumeEvent, JetStream, StoredMessage } from "../src/internal/JetStream.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()

const kvConfig = (bucket: string) => ({
  name: `KV_${bucket}`,
  subjects: [`$KV.${bucket}.>`],
  maxMessagesPerSubject: 0,
  allowRollup: true
})

const text = (message: StoredMessage): string => decoder.decode(message.payload)

// Conformance cases use TerminateOnLag with headroom: identical observable
// behavior on both interpreters as long as no test overflows it.
const roomy = { _tag: "TerminateOnLag", messages: 4096 } as const

const pump = <E>(events: Stream.Stream<ConsumeEvent, E>) =>
  Effect.gen(function*() {
    const queue = yield* Queue.unbounded<ConsumeEvent>()
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(events, (event) => Queue.offer(queue, event)),
      { startImmediately: true }
    )
    return { queue, fiber }
  })

export type WithService = <A, E>(
  f: (service: JetStream["Service"]) => Effect.Effect<A, E>
) => Effect.Effect<A, unknown>

/**
 * Behavioral contract of the JetStream seam, run identically against the
 * memory interpreter and the live adapter (real nats-server). Anything
 * observable only on one implementation stays in that implementation's file.
 */
export const conformance = (label: string, withService: WithService, timeout = 20_000) => {
  const its = (name: string, f: (service: JetStream["Service"]) => Effect.Effect<void, any>) =>
    it.effect(`${label}: ${name}`, () => withService(f), timeout)

  its("creates, gets, and idempotently re-creates streams", (service) =>
    Effect.gen(function*() {
      yield* service.createStream(kvConfig("b"))
      const config = yield* service.getStream("KV_b")
      expect(config.subjects).toEqual(["$KV.b.>"])
      yield* service.createStream(kvConfig("b"))
      const conflict = yield* Effect.flip(
        service.createStream({ ...kvConfig("b"), maxMessagesPerSubject: 5 })
      )
      expect(conflict._tag).toBe("StreamConfigConflict")
      const missing = yield* Effect.flip(service.getStream("KV_nope"))
      expect(missing._tag).toBe("StreamNotFound")
    }))

  its(
    "assigns monotonic sequences and rejects unbound subjects",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        const s1 = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("one"))
        const s2 = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("two"))
        expect([s1, s2]).toEqual([1n, 2n])
        const unbound = yield* Effect.flip(service.publish("KV_b", "$KV.other.k", utf8.encode("x")))
        expect(unbound._tag).toBe("SubjectNotBound")
        const gone = yield* Effect.flip(service.publish("KV_nope", "$KV.nope.k", utf8.encode("x")))
        expect(gone._tag).toBe("StreamNotFound")
      })
  )

  its("enforces per-subject compare-and-set", (service) =>
    Effect.gen(function*() {
      yield* service.createStream(kvConfig("b"))
      const first = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"), {
        expectedLastSubjectSequence: 0n
      })
      expect(first).toBe(1n)
      const conflict = yield* Effect.flip(
        service.publish("KV_b", "$KV.b.k", utf8.encode("v2"), { expectedLastSubjectSequence: 0n })
      )
      expect(conflict._tag).toBe("WrongLastSequence")
      if (conflict._tag === "WrongLastSequence") {
        expect(conflict.expected).toBe(0n)
        expect(conflict.actual).toBe(1n)
      }
      yield* service.publish("KV_b", "$KV.b.other", utf8.encode("x"))
      const second = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v2"), {
        expectedLastSubjectSequence: 1n
      })
      expect(second).toBe(3n)
    }))

  its(
    "returns the last message per subject and typed absence",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("old"))
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("new"), {
          headers: new Map([["KV-Operation", "DEL"]])
        })
        const last = yield* service.lastMessageForSubject("KV_b", "$KV.b.k")
        expect(text(last)).toBe("new")
        expect(last.sequence).toBe(2n)
        // user headers survive; transport metadata (the live adapter's
        // DIRECT.GET Nats-* headers) must not leak into the seam's map
        expect(last.headers.get("KV-Operation")).toBe("DEL")
        expect(last.headers.has("Nats-Sequence")).toBe(false)
        expect(last.headers.has("Nats-Subject")).toBe(false)
        const absent = yield* Effect.flip(service.lastMessageForSubject("KV_b", "$KV.b.none"))
        expect(absent._tag).toBe("NoMessageForSubject")
      })
  )

  its(
    "prunes history beyond maxMessagesPerSubject, oldest first",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream({ ...kvConfig("b"), maxMessagesPerSubject: 2 })
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"))
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v2"))
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v3"))
        const events = yield* Stream.runCollect(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "AllHistory" },
            buffer: roomy
          })
            .pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
        )
        const values = events.map((event) => event._tag === "Entry" ? text(event.message) : "?")
        expect(values).toEqual(["v2", "v3"])
      })
  )

  its(
    "rollup removes prior messages for that subject only, gated by config",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"))
        yield* service.publish("KV_b", "$KV.b.other", utf8.encode("keep"))
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("purged"), {
          headers: new Map([["Nats-Rollup", "sub"]])
        })
        const events = yield* Stream.runCollect(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "AllHistory" },
            buffer: roomy
          })
            .pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
        )
        const values = events.map((event) => event._tag === "Entry" ? text(event.message) : "?")
        expect(values).toEqual(["keep", "purged"])
        yield* service.createStream({ ...kvConfig("x"), allowRollup: false })
        const denied = yield* Effect.flip(
          service.publish("KV_x", "$KV.x.k", utf8.encode("v"), {
            headers: new Map([["Nats-Rollup", "sub"]])
          })
        )
        expect(denied._tag).toBe("RollupNotPermitted")
      })
  )

  its(
    "consume replays history, marks CaughtUp, then continues live",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        yield* service.publish("KV_b", "$KV.b.a", utf8.encode("e1"))
        yield* service.publish("KV_b", "$KV.b.b", utf8.encode("e2"))
        const { fiber, queue } = yield* pump(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "AllHistory" },
            buffer: roomy
          })
        )
        const replay1 = yield* Queue.take(queue)
        const replay2 = yield* Queue.take(queue)
        const marker = yield* Queue.take(queue)
        expect(replay1._tag === "Entry" && text(replay1.message)).toBe("e1")
        expect(replay2._tag === "Entry" && text(replay2.message)).toBe("e2")
        expect(marker._tag).toBe("CaughtUp")
        yield* service.publish("KV_b", "$KV.b.c", utf8.encode("e3"))
        const live = yield* Queue.take(queue)
        expect(live._tag === "Entry" && text(live.message)).toBe("e3")
        yield* Fiber.interrupt(fiber)
      })
  )

  its(
    "consume start positions: LastPerSubject, NewOnly, FromSequence",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        yield* service.publish("KV_b", "$KV.b.k1", utf8.encode("k1-old"))
        yield* service.publish("KV_b", "$KV.b.k1", utf8.encode("k1-new"))
        yield* service.publish("KV_b", "$KV.b.k2", utf8.encode("k2"))

        const lastPer = yield* Stream.runCollect(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "LastPerSubject" },
            buffer: roomy
          })
            .pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
        )
        expect(lastPer.map((e) => e._tag === "Entry" ? text(e.message) : "?")).toEqual([
          "k1-new",
          "k2"
        ])

        const fromSeq = yield* Stream.runCollect(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "FromSequence", sequence: 2n },
            buffer: roomy
          }).pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
        )
        expect(fromSeq.map((e) => e._tag === "Entry" ? text(e.message) : "?")).toEqual([
          "k1-new",
          "k2"
        ])

        const { fiber, queue } = yield* pump(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "NewOnly" },
            buffer: roomy
          })
        )
        const marker = yield* Queue.take(queue)
        expect(marker._tag).toBe("CaughtUp")
        yield* service.publish("KV_b", "$KV.b.k3", utf8.encode("k3"))
        const live = yield* Queue.take(queue)
        expect(live._tag === "Entry" && text(live.message)).toBe("k3")
        yield* Fiber.interrupt(fiber)
      })
  )

  its("consume filters restrict delivery per subscription", (service) =>
    Effect.gen(function*() {
      yield* service.createStream(kvConfig("b"))
      yield* service.publish("KV_b", "$KV.b.auth.user", utf8.encode("in"))
      yield* service.publish("KV_b", "$KV.b.other", utf8.encode("out"))
      const events = yield* Stream.runCollect(
        service.consume("KV_b", {
          filters: ["$KV.b.auth.>"],
          start: { _tag: "AllHistory" },
          buffer: roomy
        })
          .pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
      )
      expect(events.map((e) => e._tag === "Entry" ? text(e.message) : "?")).toEqual(["in"])
    }))

  its(
    "deleting a stream fails active consumers with StreamNotFound",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        const { fiber, queue } = yield* pump(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "NewOnly" },
            buffer: roomy
          })
        )
        const marker = yield* Queue.take(queue)
        expect(marker._tag).toBe("CaughtUp")
        yield* service.deleteStream("KV_b")
        const exit = yield* Effect.flip(Fiber.join(fiber))
        expect(exit._tag).toBe("StreamNotFound")
        const gone = yield* Effect.flip(service.getStream("KV_b"))
        expect(gone._tag).toBe("StreamNotFound")
      })
  )

  its(
    "a PullWindow buffer replays through a tiny window without loss, then continues live",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        // all publishes land before the subscription exists, so the window
        // paces only the replay: identical observable behavior on both
        // interpreters even though live pulls and memory snapshots
        yield* Effect.forEach(
          [1, 2, 3, 4, 5, 6, 7, 8],
          (n) => service.publish("KV_b", "$KV.b.k", utf8.encode(`v${n}`)),
          { discard: true }
        )
        const { fiber, queue } = yield* pump(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "AllHistory" },
            buffer: { _tag: "PullWindow", messages: 2 }
          })
        )
        const replayed: Array<string> = []
        for (let i = 0; i < 8; i++) {
          const event = yield* Queue.take(queue)
          if (event._tag === "Entry") replayed.push(text(event.message))
        }
        expect(replayed).toEqual(["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"])
        expect((yield* Queue.take(queue))._tag).toBe("CaughtUp")
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("live"))
        const live = yield* Queue.take(queue)
        expect(live._tag === "Entry" && text(live.message)).toBe("live")
        yield* Fiber.interrupt(fiber)
      })
  )

  its(
    "deleting a stream fails an active PullWindow consumer with StreamNotFound",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        const { fiber, queue } = yield* pump(
          service.consume("KV_b", {
            filters: ["$KV.b.>"],
            start: { _tag: "NewOnly" },
            buffer: { _tag: "PullWindow", messages: 4 }
          })
        )
        expect((yield* Queue.take(queue))._tag).toBe("CaughtUp")
        yield* service.deleteStream("KV_b")
        const exit = yield* Effect.flip(Fiber.join(fiber))
        expect(exit._tag).toBe("StreamNotFound")
      })
  )

  its(
    "a TerminateOnLag buffer fails the lagging subscription typed, sparing the publisher",
    (service) =>
      Effect.gen(function*() {
        yield* service.createStream(kvConfig("b"))
        const ready = yield* Deferred.make<void>()
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
              ? Deferred.succeed(ready, void 0).pipe(Effect.asVoid)
              : Deferred.succeed(pulled, void 0).pipe(Effect.andThen(Deferred.await(gate)))),
          { startImmediately: true }
        )
        // CaughtUp is the registration witness: publish only after it
        yield* Deferred.await(ready)
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v1"))
        yield* Deferred.await(pulled) // seq 1 pulled; buffer empty
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v2")) // buffered
        yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v3")) // overflows
        const publisherStillFine = yield* service.publish("KV_b", "$KV.b.k", utf8.encode("v4"))
        expect(publisherStillFine).toBe(4n)
        yield* Deferred.succeed(gate, void 0)
        const failure = yield* Effect.flip(Fiber.join(fiber))
        expect(failure._tag).toBe("ConsumerLagged")
        if (failure._tag === "ConsumerLagged") {
          expect(failure.lastDeliveredSequence).toBe(2n)
        }
      })
  )
}
