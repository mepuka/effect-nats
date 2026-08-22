/**
 * Evidence tests for the async-iterator bridge question.
 *
 * These do not assert our code; they pin the behavior of the two systems any
 * iterator bridge would stand on, so the wrap-versus-bridge decision is made
 * against observed facts instead of case-study hearsay. No server is needed:
 * the mechanism under test lives entirely in client-side iterator semantics.
 */
import { expect, it } from "@effect/vitest"
import { QueuedIteratorImpl } from "@nats-io/nats-core/internal"
import { Effect, Fiber, Stream } from "effect"
import { TestClock } from "effect/testing"

it("official QueuedIteratorImpl: return() cannot settle a parked pull until stop()", async () => {
  const qi = new QueuedIteratorImpl<unknown>()
  const gen = qi[Symbol.asyncIterator]()
  const pull = gen.next()

  // if the pull had settled, race resolves with it; otherwise the sentinel wins
  expect(await Promise.race([pull, Promise.resolve("unsettled")])).toBe("unsettled")

  const returnFn = gen.return
  if (returnFn === undefined) throw new Error("iterator lacks return()")
  const ret = returnFn.call(gen, undefined)
  expect(await Promise.race([ret, Promise.resolve("unsettled")])).toBe("unsettled")
  expect(await Promise.race([pull, Promise.resolve("unsettled")])).toBe("unsettled")

  qi.stop()
  expect(await pull).toEqual({ value: undefined, done: true })
  expect(await ret).toEqual({ value: undefined, done: true })
})

it("official QueuedIteratorImpl: upstream buffering is unbounded", async () => {
  const qi = new QueuedIteratorImpl<number>()
  for (let i = 0; i < 10_000; i++) {
    qi.push(i)
  }
  // nothing has been consumed: all 10_000 sit in the growable yields array
  expect(qi.getPending()).toBe(10_000)
  qi.stop()
})

it.effect(
  "fromAsyncIterable finalization awaits iter.return(): interruption blocks while it pends",
  () =>
    Effect.gen(function*() {
      let openNext!: () => void
      let openReturn!: () => void
      let releaseNext!: () => void
      let releaseReturn!: () => void
      let returnCalls = 0

      const nextSignal = new Promise<void>((resolve) => {
        openNext = resolve
      })
      const returnSignal = new Promise<void>((resolve) => {
        openReturn = resolve
      })

      const iterable: AsyncIterable<string> = {
        [Symbol.asyncIterator]: () => ({
          next: () => {
            openNext()
            return new Promise<IteratorResult<string>>((resolve) => {
              releaseNext = () => resolve({ value: "x", done: false })
            })
          },
          return: () => {
            returnCalls++
            openReturn()
            return new Promise<IteratorResult<string>>((resolve) => {
              releaseReturn = () => resolve({ value: undefined, done: true })
            })
          }
        })
      }

      const collector = yield* Stream.fromAsyncIterable(iterable, () => "error").pipe(
        Stream.runCollect,
        Effect.forkChild
      )
      yield* Effect.promise(() => nextSignal)

      const interrupter = yield* Fiber.interrupt(collector).pipe(Effect.forkChild)
      yield* Effect.promise(() => returnSignal)
      expect(returnCalls).toBe(1)

      // bounded observation under virtual time: while return() pends, the
      // interruption cannot complete - proven by a clock-stepped deadline,
      // never by hanging the suite
      const watcher = yield* Fiber.await(interrupter).pipe(
        Effect.timeoutOption("1 second"),
        Effect.forkChild
      )
      yield* TestClock.adjust("5 seconds")
      expect((yield* Fiber.join(watcher))._tag).toBe("None")

      releaseReturn()
      releaseNext()
      const final = yield* Fiber.await(interrupter)
      expect(final._tag).toBe("Success")
    })
)
