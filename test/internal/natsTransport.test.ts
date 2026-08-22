import { expect, it } from "@effect/vitest"
import { Effect, Exit, Fiber, Layer, Scope } from "effect"
import { TestClock } from "effect/testing"
import { layerTransport, NatsTransport } from "../../src/internal/NatsTransport.ts"
import { makeFakeConnection } from "./kit.ts"

it.effect("flush round-trips while the layer is open", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const layer = layerTransport(fake.connection)
    yield* Effect.scoped(
      NatsTransport.use((transport) => transport.flush()).pipe(Effect.provide(layer))
    )
    expect(fake.observed.flushCalls).toBe(1)
  }))

it.effect("closing the layer drains and closes the underlying connection once", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const layer = layerTransport(fake.connection)
    yield* Effect.scoped(
      NatsTransport.use(() => Effect.void).pipe(Effect.provide(layer))
    )
    expect(fake.observed.drainCalls).toBe(1)
    expect(fake.observed.closeCalls).toBe(1)
  }))

it.effect("flush fails with ConnectionClosedError once the connection is closed", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const layer = layerTransport(fake.connection)
    yield* Effect.scoped(
      NatsTransport.use((transport) =>
        Effect.gen(function*() {
          yield* fake.markClosed
          const failure = yield* Effect.flip(transport.flush())
          expect(failure._tag).toBe("ConnectionClosedError")
          expect(fake.observed.flushCalls).toBe(0)
        })
      ).pipe(Effect.provide(layer))
    )
  }))

it.effect("a hanging drain is abandoned at the deadline and close still runs", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    fake.controls.drain = "hang"
    const scope = yield* Scope.make()
    yield* Layer.build(layerTransport(fake.connection, { drainTimeout: "5 seconds" })).pipe(
      Scope.provide(scope)
    )
    // closing blocks on the release finalizer, which is stuck in the drain
    const closer = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild)
    yield* TestClock.adjust("10 seconds")
    yield* Fiber.join(closer)
    expect(fake.observed.drainCalls).toBe(1)
    expect(fake.observed.closeCalls).toBe(1)
  }))
