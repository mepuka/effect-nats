import { expect, it } from "@effect/vitest"
import { connect } from "@nats-io/transport-node"
import { Deferred, Effect, Fiber, Queue, Stream } from "effect"
import type { JetStream } from "../src/internal/JetStream.ts"
import * as JetStreamLive from "../src/internal/JetStreamLive.ts"
import { conformance } from "./JetStreamConformance.ts"
import * as TestServer from "./TestServer.ts"

// The live adapter against a real nats-server: one fresh server, connection,
// and JetStream domain per test, all scoped, so cases cannot interfere.
const withLive = <A, E>(
  f: (service: JetStream["Service"]) => Effect.Effect<A, E>
): Effect.Effect<A, unknown> =>
  Effect.scoped(
    Effect.gen(function*() {
      const server = yield* TestServer.start
      const nc = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () => connect({ servers: [server.url] }),
          catch: (cause) =>
            new TestServer.TestServerError({ reason: `connect failed: ${String(cause)}` })
        }),
        (connection) => Effect.promise(() => connection.close())
      )
      const service = yield* JetStreamLive.make(nc)
      return yield* f(service)
    })
  )

conformance("live", withLive, 30_000)

const utf8 = new TextEncoder()
const decoder = new TextDecoder()

// Live-only: on the memory interpreter a full PullWindow suspends the
// in-process publisher, so this scenario belongs only here, where the server
// absorbs the backlog and the publisher must never block.
it.effect(
  "live: a stalled PullWindow consumer never blocks the publisher and loses nothing",
  () =>
    withLive((service) =>
      Effect.gen(function*() {
        yield* service.createStream({
          name: "KV_b",
          subjects: ["$KV.b.>"],
          maxMessagesPerSubject: 0,
          allowRollup: true
        })
        const ready = yield* Deferred.make<void>()
        const gate = yield* Deferred.make<void>()
        const relay = yield* Queue.unbounded<string>()
        const events = service.consume("KV_b", {
          filters: ["$KV.b.>"],
          start: { _tag: "NewOnly" },
          buffer: { _tag: "PullWindow", messages: 2 }
        })
        const fiber = yield* Effect.forkChild(
          Stream.runForEach(events, (event) =>
            event._tag === "CaughtUp"
              ? Deferred.succeed(ready, void 0).pipe(Effect.asVoid)
              : Deferred.await(gate).pipe(
                Effect.andThen(Queue.offer(relay, decoder.decode(event.message.payload))),
                Effect.asVoid
              )),
          { startImmediately: true }
        )
        // CaughtUp is the registration witness: publish only after it
        yield* Deferred.await(ready)
        const sequences = yield* Effect.forEach(
          [1, 2, 3, 4, 5, 6, 7, 8],
          (n) => service.publish("KV_b", "$KV.b.k", utf8.encode(`v${n}`))
        )
        // every publish completed while the consumer was stalled at a window of
        // two: the publisher was never suspended
        expect(sequences).toEqual([1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n])
        yield* Deferred.succeed(gate, void 0)
        const seen: Array<string> = []
        for (let i = 0; i < 8; i++) {
          seen.push(yield* Queue.take(relay))
        }
        expect(seen).toEqual(["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"])
        yield* Fiber.interrupt(fiber)
      })
    ),
  30_000
)
