import { expect, it } from "@effect/vitest"
import type { Msg } from "@nats-io/nats-core"
import { connect } from "@nats-io/transport-node"
import { Effect, Exit, Fiber, Latch, Scope, Stream } from "effect"
import * as CoreSubscription from "../src/internal/CoreSubscription.ts"
import * as TestServer from "./TestServer.ts"

it.effect(
  "real callback subscription releases an idle pull and admits nothing after release",
  () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* TestServer.start
        const connection = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: () => connect({ servers: [server.url] }),
            catch: (cause) =>
              new TestServer.TestServerError({ reason: `connect failed: ${String(cause)}` })
          }),
          (current) => Effect.promise(() => current.close())
        )

        const subscriptionScope = yield* Scope.make()
        const source = yield* CoreSubscription.make<Msg>(connection, {
          subject: "effect-nats.release-proof",
          capacity: 8
        }).pipe(Scope.provide(subscriptionScope))
        // Official subscribe() is local enqueue. This flush is the registration
        // witness that the server has processed SUB before the first publish.
        yield* Effect.promise(() => connection.flush())

        const seen: Array<string> = []
        const first = yield* Latch.make()
        const consumer = yield* source.messages.pipe(
          Stream.tap((message) =>
            Effect.sync(() => seen.push(new TextDecoder().decode(message.data))).pipe(
              Effect.andThen(first.open)
            )
          ),
          Stream.runDrain,
          Effect.forkChild
        )

        connection.publish("effect-nats.release-proof", new TextEncoder().encode("before"))
        yield* Effect.promise(() => connection.flush())
        yield* first.await

        yield* Scope.close(subscriptionScope, Exit.void)
        yield* Fiber.join(consumer)

        connection.publish("effect-nats.release-proof", new TextEncoder().encode("after"))
        yield* Effect.promise(() => connection.flush())
        yield* Effect.yieldNow
        expect(seen).toEqual(["before"])
      })
    ),
  30_000
)
