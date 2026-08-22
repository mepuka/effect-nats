import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Option, Queue, Result, Schema, Stream } from "effect"
import * as Memory from "../src/internal/jetstream/memory.ts"
import { makeKv } from "../src/internal/kv/kv.ts"
import type { KvBucket, KvWatchEvent } from "../src/internal/kv/kv.ts"
import { KvBucketName, KvKey } from "../src/internal/kv/naming.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()

const bucket = (name: string) => Result.getOrThrow(Schema.decodeUnknownResult(KvBucketName)(name))
const key = (name: string) => Result.getOrThrow(Schema.decodeUnknownResult(KvKey)(name))
const bytes = (value: string) => utf8.encode(value)

const setup = (options?: { history?: number }) =>
  Effect.gen(function*() {
    const memory = yield* Memory.make
    const kv = makeKv(memory.service)
    const store = yield* kv.open(bucket("config"), options)
    return { kv, memory, store }
  })

const pump = <E>(events: Stream.Stream<KvWatchEvent, E>) =>
  Effect.gen(function*() {
    const queue = yield* Queue.unbounded<KvWatchEvent>()
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(events, (event) => Queue.offer(queue, event)),
      { startImmediately: true }
    )
    return { queue, fiber }
  })

const entryText = (event: KvWatchEvent): string =>
  event._tag === "Entry" ? `${event.entry.operation}:${event.entry.key}` : "CaughtUp"

it.effect("put assigns revisions and get returns the latest live entry", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    expect(Option.isNone(yield* store.get(key("missing")))).toBe(true)
    const r1 = yield* store.put(key("k"), bytes("v1"))
    const r2 = yield* store.put(key("k"), bytes("v2"))
    expect([r1, r2]).toEqual([1n, 2n])
    const entry = yield* store.get(key("k"))
    expect(Option.isSome(entry)).toBe(true)
    if (Option.isSome(entry)) {
      expect(decoder.decode(entry.value.value)).toBe("v2")
      expect(entry.value.revision).toBe(2n)
      expect(entry.value.operation).toBe("Put")
    }
  }))

it.effect("create fails on live keys but succeeds over delete and purge markers", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    yield* store.create(key("k"), bytes("v1"))
    const conflict = yield* Effect.flip(store.create(key("k"), bytes("again")))
    expect(conflict._tag).toBe("KvConflict")
    // deleted key: create must succeed through the tombstone (official semantics)
    yield* store.delete(key("k"))
    const recreated = yield* store.create(key("k"), bytes("v2"))
    expect(recreated > 2n).toBe(true)
    // purged key: same
    yield* store.purge(key("k"))
    yield* store.create(key("k"), bytes("v3"))
    const entry = yield* store.get(key("k"))
    expect(Option.isSome(entry) && decoder.decode(entry.value.value)).toBe("v3")
  }))

it.effect("update enforces compare-and-set with both sides reported", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    const r1 = yield* store.put(key("k"), bytes("v1"))
    const r2 = yield* store.update(key("k"), bytes("v2"), r1)
    expect(r2).toBe(2n)
    const conflict = yield* Effect.flip(store.update(key("k"), bytes("v3"), r1))
    expect(conflict._tag).toBe("KvConflict")
    if (conflict._tag === "KvConflict") {
      expect(conflict.expected).toBe(r1)
      expect(conflict.actual).toBe(r2)
    }
  }))

it.effect("delete tombstones a key and purge collapses its history", () =>
  Effect.gen(function*() {
    const { store } = yield* setup({ history: 10 })
    yield* store.put(key("k"), bytes("v1"))
    yield* store.put(key("k"), bytes("v2"))
    yield* store.delete(key("k"))
    expect(Option.isNone(yield* store.get(key("k")))).toBe(true)
    const afterDelete = yield* Stream.runCollect(store.history(key("k")))
    expect(afterDelete.map((e) => e.operation)).toEqual(["Put", "Put", "Del"])
    // purge is a rollup: prior history for the key disappears entirely
    yield* store.purge(key("k"))
    const afterPurge = yield* Stream.runCollect(store.history(key("k")))
    expect(afterPurge.map((e) => e.operation)).toEqual(["Purge"])
  }))

it.effect("history honors the bucket's per-key depth", () =>
  Effect.gen(function*() {
    const { store } = yield* setup({ history: 2 })
    yield* store.put(key("k"), bytes("v1"))
    yield* store.put(key("k"), bytes("v2"))
    yield* store.put(key("k"), bytes("v3"))
    const entries = yield* Stream.runCollect(store.history(key("k")))
    expect(entries.map((e) => decoder.decode(e.value))).toEqual(["v2", "v3"])
  }))

it.effect("watch replays last-per-key, marks CaughtUp, then streams live operations", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    yield* store.put(key("a"), bytes("old"))
    yield* store.put(key("a"), bytes("new"))
    yield* store.put(key("b"), bytes("x"))
    const { fiber, queue } = yield* pump(store.watch())
    expect(entryText(yield* Queue.take(queue))).toBe("Put:a")
    expect(entryText(yield* Queue.take(queue))).toBe("Put:b")
    expect(entryText(yield* Queue.take(queue))).toBe("CaughtUp")
    yield* store.delete(key("a"))
    expect(entryText(yield* Queue.take(queue))).toBe("Del:a")
    yield* Fiber.interrupt(fiber)
  }))

it.effect("watch supports selectors, updates-only, and resume from a revision", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    yield* store.put(key("auth.user"), bytes("u"))
    yield* store.put(key("other"), bytes("o"))

    const filtered = yield* pump(store.watch({ selector: "auth.>" }))
    expect(entryText(yield* Queue.take(filtered.queue))).toBe("Put:auth.user")
    expect(entryText(yield* Queue.take(filtered.queue))).toBe("CaughtUp")
    yield* Fiber.interrupt(filtered.fiber)

    const updates = yield* pump(store.watch({ updatesOnly: true }))
    expect(entryText(yield* Queue.take(updates.queue))).toBe("CaughtUp")
    yield* store.put(key("late"), bytes("l"))
    expect(entryText(yield* Queue.take(updates.queue))).toBe("Put:late")
    yield* Fiber.interrupt(updates.fiber)

    const resumed = yield* pump(store.watch({ fromRevision: 2n }))
    expect(entryText(yield* Queue.take(resumed.queue))).toBe("Put:other")
    expect(entryText(yield* Queue.take(resumed.queue))).toBe("Put:late")
    expect(entryText(yield* Queue.take(resumed.queue))).toBe("CaughtUp")
    yield* Fiber.interrupt(resumed.fiber)
  }))

it.effect("watch rejects invalid selectors before touching the wire", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    const events = store.watch({ selector: "a.>.b" })
    const failure = yield* Effect.flip(Stream.runCollect(events))
    expect(failure._tag).toBe("KvInvalidFilter")
  }))

it.effect("keys lists live keys only and terminates", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    yield* store.put(key("a"), bytes("1"))
    yield* store.put(key("b"), bytes("2"))
    yield* store.put(key("c"), bytes("3"))
    yield* store.delete(key("b"))
    const keys = yield* Stream.runCollect(store.keys())
    expect([...keys].sort()).toEqual(["a", "c"])
  }))

it.effect("non-conformant keys written by lax clients fail reads typed (ADR-0003)", () =>
  Effect.gen(function*() {
    const { memory, store } = yield* setup()
    // a foreign client writes a server-legal but grammar-violating key directly
    yield* memory.service.publish("KV_config", "$KV.config.bad$key", bytes("x"))
    const failure = yield* Effect.flip(Stream.runCollect(store.keys()))
    expect(failure._tag).toBe("KvEntryViolation")
    if (failure._tag === "KvEntryViolation") {
      expect(failure.subject).toBe("$KV.config.bad$key")
    }
  }))

it.effect("open binds only to streams shaped like the bucket (the missing upstream guard)", () =>
  Effect.gen(function*() {
    const memory = yield* Memory.make
    const kv = makeKv(memory.service)
    // bind to a missing bucket
    const missing = yield* Effect.flip(kv.open(bucket("nope"), { mode: "bind" }))
    expect(missing._tag).toBe("KvBucketMissing")
    // a plain stream squatting on the KV_ name with the wrong subjects
    yield* memory.service.createStream({
      name: "KV_squat",
      subjects: ["orders.>"],
      maxMessagesPerSubject: 0,
      allowRollup: false
    })
    const mismatch = yield* Effect.flip(kv.open(bucket("squat"), { mode: "bind" }))
    expect(mismatch._tag).toBe("KvBucketShapeMismatch")
    // create then bind round-trips
    yield* kv.open(bucket("real"))
    const bound: KvBucket = yield* kv.open(bucket("real"), { mode: "bind" })
    yield* bound.put(key("k"), bytes("v"))
    expect(Option.isSome(yield* bound.get(key("k")))).toBe(true)
    // re-open with a different history is a typed conflict
    const conflict = yield* Effect.flip(kv.open(bucket("real"), { history: 5 }))
    expect(conflict._tag).toBe("KvOpenConflict")
  }))

it.effect("status reports the bucket configuration", () =>
  Effect.gen(function*() {
    const { store } = yield* setup({ history: 3 })
    const status = yield* store.status()
    expect(status.bucket).toBe("config")
    expect(status.history).toBe(3)
  }))

it.effect("destroy removes the bucket and fails active watchers typed", () =>
  Effect.gen(function*() {
    const { kv, memory, store } = yield* setup()
    const { fiber, queue } = yield* pump(store.watch())
    expect(entryText(yield* Queue.take(queue))).toBe("CaughtUp")
    yield* kv.destroy(bucket("config"))
    const failure = yield* Effect.flip(Fiber.join(fiber))
    expect(failure._tag).toBe("KvBucketMissing")
    expect(yield* memory.subscriberCount("KV_config")).toBe(0)
    const gone = yield* Effect.flip(store.get(key("k")))
    expect(gone._tag).toBe("KvBucketMissing")
  }))

it.effect("watch subscriptions release on interruption", () =>
  Effect.gen(function*() {
    const { memory, store } = yield* setup()
    const a = yield* pump(store.watch())
    const b = yield* pump(store.watch())
    expect(entryText(yield* Queue.take(a.queue))).toBe("CaughtUp")
    expect(entryText(yield* Queue.take(b.queue))).toBe("CaughtUp")
    expect(yield* memory.subscriberCount("KV_config")).toBe(2)
    yield* Fiber.interrupt(a.fiber)
    yield* Fiber.interrupt(b.fiber)
    expect(yield* memory.subscriberCount("KV_config")).toBe(0)
  }))

it.effect("a lagging watch terminates typed with the last observed revision (ADR-0005)", () =>
  Effect.gen(function*() {
    const { store } = yield* setup()
    // same deterministic protocol as the substrate lag test: one entry in
    // flight, one buffered, the next overflows the capacity-1 buffer
    const pulled = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(
        store.watch({ updatesOnly: true, bufferMessages: 1 }),
        (event) =>
          event._tag === "CaughtUp"
            ? Effect.void
            : Deferred.succeed(pulled, void 0).pipe(Effect.andThen(Deferred.await(gate)))
      ),
      { startImmediately: true }
    )
    yield* store.put(key("k"), bytes("v1"))
    yield* Deferred.await(pulled) // revision 1 pulled; the buffer is empty
    yield* store.put(key("k"), bytes("v2")) // buffered
    yield* store.put(key("k"), bytes("v3")) // overflows
    yield* Deferred.succeed(gate, void 0)
    const failure = yield* Effect.flip(Fiber.join(fiber))
    expect(failure._tag).toBe("KvWatchLagged")
    if (failure._tag === "KvWatchLagged") {
      expect(failure.lastRevision).toBe(2n)
    }
  }))
