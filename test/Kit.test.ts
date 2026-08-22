import { expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { makeFakeConnection, statusOf } from "./Kit.ts"

it.effect("fake closed() remains pending until close and settles once", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const closed = fake.connection.closed()
    let settled = false
    void closed.then(() => {
      settled = true
    })
    // A resolved fake closes in this microtask turn; a faithful open
    // connection keeps the promise pending without using wall time.
    yield* Effect.promise(() => Promise.resolve())
    expect(settled).toBe(false)

    yield* Effect.promise(() => fake.connection.close())
    yield* Effect.promise(() => fake.connection.close())
    expect(yield* Effect.promise(() => closed)).toBeUndefined()
    expect(fake.observed.closedSettlements).toBe(1)
  }))

it.effect("fake status end wakes a parked pull and releases every waiter", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const iterator = fake.connection.status()[Symbol.asyncIterator]()
    const pull = iterator.next()
    yield* Effect.yieldNow
    expect(fake.observed.statusWaiters).toBe(1)

    yield* fake.endStatus

    expect(yield* Effect.promise(() => pull)).toEqual({ value: undefined, done: true })
    expect(fake.observed.statusWaiters).toBe(0)
  }))

it.effect("fake status return cancels a parked pull exactly once", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const iterator = fake.connection.status()[Symbol.asyncIterator]()
    const pull = iterator.next()
    yield* Effect.yieldNow
    expect(fake.observed.statusWaiters).toBe(1)

    const returned = iterator.return?.()
    if (returned === undefined) return yield* Effect.die("status iterator has no return")
    expect(yield* Effect.promise(() => returned)).toEqual({ value: undefined, done: true })
    expect(yield* Effect.promise(() => pull)).toEqual({ value: undefined, done: true })
    expect(fake.observed.statusReturnCalls).toBe(1)
    expect(fake.observed.statusWaiters).toBe(0)
  }))

it.effect("fake status capacity reports the triggering item as rejected", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    for (let i = 0; i < 64; i++) {
      expect(yield* fake.deliverStatus(statusOf(`status-${i}`))).toBe(true)
    }
    expect(yield* fake.deliverStatus(statusOf("overflow"))).toBe(false)
    expect(fake.observed.statusPending).toBe(64)
  }))

it.effect("fake close terminates both closed() and the status feed", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const iterator = fake.connection.status()[Symbol.asyncIterator]()
    const pull = iterator.next()
    yield* Effect.yieldNow

    yield* Effect.promise(() => fake.connection.close())

    expect(yield* Effect.promise(() => fake.connection.closed())).toBeUndefined()
    expect(yield* Effect.promise(() => pull)).toEqual({ value: undefined, done: true })
    expect(fake.connection.isClosed()).toBe(true)
    expect(fake.observed.statusWaiters).toBe(0)
  }))
