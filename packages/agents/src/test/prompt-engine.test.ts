/**
 * Read-path engine under a scripted Wire + TestClock: timeout boundaries,
 * gap resets, header-first classification, terminator recognition, and
 * exactly-once finalization. Zero wall-clock sleeps.
 */
import { describe, expect, test } from "bun:test"
import { Effect, Exit, Fiber, Stream } from "effect"
import { TestClock } from "effect/testing"
import { promptStream } from "../agents/prompt-engine.js"
import type { PromptError } from "../agents/errors.js"
import type { Chunk } from "../agents/protocol/chunks.js"
import { Wire } from "../agents/wire.js"
import { at, chunk, makeScriptedWire, msg, terminator, type ScriptEntry } from "./kit.js"

const runScripted = <A, E>(
  script: ReadonlyArray<ScriptEntry>,
  use: (
    collect: Effect.Effect<Array<Chunk>, PromptError>,
    wire: ReturnType<typeof makeScriptedWire>,
  ) => Effect.Effect<A, E>,
  options?: Parameters<typeof makeScriptedWire>[1],
): Promise<A> => {
  const scripted = makeScriptedWire(script, options)
  const stream = promptStream({
    subject: "agents.prompt.cc.o.n",
    payload: new TextEncoder().encode("hi"),
    inactivityMs: 60_000,
  }).pipe(Stream.provideService(Wire, scripted.wire))
  return Effect.runPromise(
    use(Stream.runCollect(stream), scripted).pipe(Effect.provide(TestClock.layer())),
  )
}

const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const ACK_CHUNK = { type: "status", data: "ack" }
const TEXT_CHUNK = { type: "response", data: "hello" }

describe("prompt engine (Protocol §6)", () => {
  test("ack + chunks + terminator yields the chunk sequence and ends", async () => {
    const collected = await runScripted(
      [chunk(0, ACK_CHUNK), chunk(10, TEXT_CHUNK), chunk(10, TEXT_CHUNK), terminator(10)],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(100)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(collected.map((c) => c._tag)).toEqual(["Status", "Response", "Response"])
  })

  test("§6.6 gap just under the window passes; just over times out", async () => {
    const ok = await runScripted(
      [chunk(0, ACK_CHUNK), chunk(59_999, TEXT_CHUNK), terminator(1)],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(60_001)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(ok).toHaveLength(2)

    const exit = await runScripted([chunk(0, ACK_CHUNK), chunk(60_001, TEXT_CHUNK)], (collect) =>
      Effect.gen(function* () {
        const fiber = yield* forkNow(Effect.exit(collect))
        yield* TestClock.adjust(120_000)
        return yield* Fiber.join(fiber)
      }),
    )
    expect(Exit.isFailure(exit)).toBe(true)
  })

  test("§6.6 every observed frame resets the gap timer — unknown chunks included", async () => {
    const collected = await runScripted(
      [
        chunk(0, ACK_CHUNK),
        chunk(50_000, { type: "future-extension", data: 1 }),
        chunk(50_000, TEXT_CHUNK),
        terminator(50_000),
      ],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(200_000)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(collected.map((c) => c._tag)).toEqual(["Status", "Unknown", "Response"])
  })

  test("§9 error headers fail the stream typed, checked before body", async () => {
    const error = await runScripted(
      [chunk(0, ACK_CHUNK), at(10, msg("ignored-body", { errorCode: 429, errorMessage: "busy" }))],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(Effect.flip(collect))
          yield* TestClock.adjust(20)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(error._tag).toBe("PromptFailed")
    expect(error._tag === "PromptFailed" && error.code).toBe(429)
    expect(error._tag === "PromptFailed" && error.message).toBe("busy")
  })

  test("§6.5 zero-byte WITH headers is not a terminator", async () => {
    const error = await runScripted(
      [chunk(0, ACK_CHUNK), at(10, msg(new Uint8Array(0), { hasHeaders: true }))],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(Effect.flip(collect))
          yield* TestClock.adjust(20)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(error._tag).toBe("ProtocolViolation")
  })

  test("reply stream ending without terminator is a protocol violation", async () => {
    const error = await runScripted(
      [chunk(0, ACK_CHUNK), chunk(10, TEXT_CHUNK)],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(Effect.flip(collect))
          yield* TestClock.adjust(20)
          return yield* Fiber.join(fiber)
        }),
      { endAfterScript: true },
    )
    expect(error._tag).toBe("ProtocolViolation")
  })

  test("non-JSON chunk is a protocol violation", async () => {
    const exit = await runScripted(
      [chunk(0, ACK_CHUNK), at(10, msg("not json"))],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(Effect.exit(collect))
          yield* TestClock.adjust(20)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(Exit.isFailure(exit)).toBe(true)
  })

  test("interruption releases the subscription exactly once", async () => {
    await runScripted(
      [chunk(0, ACK_CHUNK)],
      (collect, scripted) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(10)
          yield* Fiber.interrupt(fiber)
          expect(scripted.finalizers()).toBe(1)
          return undefined
        }),
    )
  })

  test("normal completion also releases exactly once", async () => {
    await runScripted(
      [chunk(0, ACK_CHUNK), terminator(5)],
      (collect, scripted) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(10)
          yield* Fiber.join(fiber)
          expect(scripted.finalizers()).toBe(1)
          return undefined
        }),
    )
  })
})
