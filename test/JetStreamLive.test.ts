import { expect, it } from "@effect/vitest"
import { jetstreamManager } from "@nats-io/jetstream"
import type { NatsConnection } from "@nats-io/nats-core"
import { connect } from "@nats-io/transport-node"
import { Deferred, Effect, Fiber, Queue, Stream } from "effect"
import type { JetStream } from "../src/internal/JetStream.ts"
import * as JetStreamLive from "../src/internal/JetStreamLive.ts"
import { conformance } from "./JetStreamConformance.ts"
import { leanSubscriberReplay } from "./LeanSubscriberReplay.ts"
import { leanTraceReplay } from "./LeanTraceReplay.ts"
import * as TestServer from "./TestServer.ts"

// The live adapter against a real nats-server: one fresh server, connection,
// and JetStream domain per test, all scoped, so cases cannot interfere.
const withLive = <A, E>(
  f: (service: JetStream["Service"], nc: NatsConnection) => Effect.Effect<A, E>
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
      return yield* f(service, nc)
    })
  )

conformance("live", (f) => withLive(f), 30_000)
leanTraceReplay("live", withLive, 30_000)
leanSubscriberReplay("live", (f) => withLive((service) => f({ service })), 30_000)

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

// Live-only: only a real server can mint sequences beyond 2^53 (first_seq is
// a raw stream option the seam deliberately does not expose). ADR-0004: JSON
// number paths fail closed and typed; header/ack-token string paths stay exact.
it.effect(
  "live: sequences beyond 2^53 fail closed on JSON paths and stay exact on string paths",
  () =>
    withLive((service, nc) =>
      Effect.gen(function*() {
        const jsm = yield* Effect.promise(() => jetstreamManager(nc))
        // 2^60: exactly representable as a double, far beyond Number.MAX_SAFE_INTEGER
        yield* Effect.promise(() =>
          jsm.streams.add({
            name: "KV_big",
            subjects: ["$KV.big.>"],
            max_msgs_per_subject: 0,
            allow_rollup_hdrs: true,
            allow_direct: true,
            first_seq: 2 ** 60
          })
        )
        // rule 2: PubAck.seq arrives as a JSON number the parse already
        // rounded, so the publish fails closed instead of laundering it
        const failed = yield* Effect.flip(
          service.publish("KV_big", "$KV.big.k", utf8.encode("v1"))
        )
        expect(failed._tag).toBe("SequenceNotProvablyExact")
        // rule 1: the message itself landed; the DIRECT.GET header sequence is
        // a decimal string and decodes without loss. Note the server never saw
        // 2^60: the client's JSON encoding of first_seq already rendered the
        // double as 1152921504606847000, and THAT exact uint64 is what the
        // string paths must preserve.
        const last = yield* service.lastMessageForSubject("KV_big", "$KV.big.k")
        expect(last.sequence).toBe(1152921504606847000n)
        // rule 1: consume sequences come from the raw ack-subject token
        const events = yield* Stream.runCollect(
          service.consume("KV_big", {
            filters: ["$KV.big.>"],
            start: { _tag: "AllHistory" },
            buffer: { _tag: "TerminateOnLag", messages: 64 }
          }).pipe(Stream.takeWhile((event) => event._tag !== "CaughtUp"))
        )
        expect(events.map((e) => e._tag === "Entry" ? e.message.sequence : 0n))
          .toEqual([1152921504606847000n])
      })
    ),
  30_000
)
