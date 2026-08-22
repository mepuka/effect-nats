import { expect, it } from "@effect/vitest"
import type { Msg, NatsConnection, Status } from "@nats-io/nats-core"
import { connect } from "@nats-io/transport-node"
import type { NodeConnectionOptions } from "@nats-io/transport-node"
import { Effect, Exit, Fiber, Queue, Scope, Stream } from "effect"
import { fileURLToPath } from "node:url"
import * as CoreSubscription from "../src/internal/CoreSubscription.ts"
import { compile, defaultPolicy } from "../src/internal/ReconnectPolicy.ts"
import * as TestServer from "./TestServer.ts"

const certificate = fileURLToPath(new URL("./fixtures/tls/server-cert.pem", import.meta.url))
const privateKey = fileURLToPath(new URL("./fixtures/tls/server-key.pem", import.meta.url))

const reconnectOptions = compile({
  ...defaultPolicy,
  attempts: { _tag: "Bounded", attempts: 2 },
  delay: {
    _tag: "AdditiveJitter",
    base: "25 millis",
    jitter: 0,
    jitterTls: 0
  },
  serverOrder: "Configured"
})

const observeStatuses = (connection: NatsConnection) => {
  const events: Array<Status> = []
  let resolveReconnect: (events: ReadonlyArray<Status>) => void = () => {}
  const reconnected = new Promise<ReadonlyArray<Status>>((resolve) => {
    resolveReconnect = resolve
  })
  const finished = (async () => {
    for await (const status of connection.status()) {
      events.push(status)
      if (status.type === "reconnect") resolveReconnect(events)
    }
    return events
  })()
  return { events, reconnected, finished }
}

const awaitWithin = <A>(promise: Promise<A>, operation: string) =>
  Effect.tryPromise({
    try: () => promise,
    catch: (cause) => new TestServer.TestServerError({ reason: `${operation}: ${String(cause)}` })
  }).pipe(
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () =>
        Effect.fail(
          new TestServer.TestServerError({ reason: `${operation} did not settle in 10s` })
        )
    })
  )

for (const encrypted of [false, true]) {
  const transport = encrypted ? "TLS" : "plain TCP"
  const tlsServer = encrypted ? { certFile: certificate, keyFile: privateKey } : undefined
  const tlsClient: Pick<NodeConnectionOptions, "tls"> = encrypted
    ? { tls: { caFile: certificate } }
    : {}

  it.live(
    `${transport}: reconnects to the second server and restores a callback subscription`,
    () =>
      Effect.scoped(
        Effect.gen(function*() {
          const first = yield* TestServer.startWith({
            jetStream: false,
            name: `effect-nats-${encrypted ? "tls" : "plain"}-one`,
            tls: tlsServer
          })
          const second = yield* TestServer.startWith({
            jetStream: false,
            name: `effect-nats-${encrypted ? "tls" : "plain"}-two`,
            tls: tlsServer
          })
          const connection = yield* Effect.acquireRelease(
            Effect.tryPromise({
              try: () =>
                connect({
                  servers: [first.url, second.url],
                  ...reconnectOptions,
                  ...tlsClient
                }),
              catch: (cause) =>
                new TestServer.TestServerError({ reason: `connect failed: ${String(cause)}` })
            }),
            (current) => Effect.promise(() => current.close())
          )
          expect(connection.getServer()).toContain(String(first.port))

          const status = observeStatuses(connection)
          const subscriptionScope = yield* Scope.make()
          yield* Effect.addFinalizer((exit) => Scope.close(subscriptionScope, exit))
          const source = yield* CoreSubscription.make<Msg>(connection, {
            subject: "effect-nats.reconnect-proof",
            capacity: 8
          }).pipe(Scope.provide(subscriptionScope))
          // Witness that the server processed SUB before the first publish.
          yield* Effect.promise(() => connection.flush())

          const delivered = yield* Queue.bounded<string>(2)
          const consumer = yield* source.messages.pipe(
            Stream.runForEach((message) =>
              Queue.offer(delivered, new TextDecoder().decode(message.data))
            ),
            Effect.forkChild
          )

          connection.publish("effect-nats.reconnect-proof", new TextEncoder().encode("before"))
          yield* Effect.promise(() => connection.flush())
          expect(yield* Queue.take(delivered)).toBe("before")

          yield* first.stop
          const statuses = yield* awaitWithin(status.reconnected, `${transport} reconnect`)
          expect(
            statuses
              .map((event) => event.type)
              .filter((type) =>
                type === "disconnect" || type === "reconnecting" || type === "reconnect"
              )
          ).toEqual(["disconnect", "reconnecting", "reconnect"])
          expect(connection.getServer()).toContain(String(second.port))
          expect(consumer.pollUnsafe()).toBeUndefined()

          // Witness that the restored SUB reached the second server.
          yield* Effect.promise(() => connection.flush())
          connection.publish("effect-nats.reconnect-proof", new TextEncoder().encode("after"))
          yield* Effect.promise(() => connection.flush())
          const restored = yield* Queue.take(delivered).pipe(
            Effect.timeoutOrElse({
              duration: "5 seconds",
              orElse: () =>
                Effect.fail(
                  new TestServer.TestServerError({
                    reason: `${transport} restored delivery missing`
                  })
                )
            })
          )
          expect(restored).toBe("after")

          yield* Scope.close(subscriptionScope, Exit.void)
          yield* Fiber.join(consumer)
          yield* Effect.promise(() => connection.close())
          yield* awaitWithin(status.finished, `${transport} status close`)
          yield* second.stop
        })
      ),
    30_000
  )

  it.live(
    `${transport}: exhausts exactly two attempts per server and emits terminal close`,
    () =>
      Effect.scoped(
        Effect.gen(function*() {
          const first = yield* TestServer.startWith({ jetStream: false, tls: tlsServer })
          const second = yield* TestServer.startWith({ jetStream: false, tls: tlsServer })
          const connection = yield* Effect.tryPromise({
            try: () =>
              connect({
                servers: [first.url, second.url],
                ...reconnectOptions,
                ...tlsClient
              }),
            catch: (cause) =>
              new TestServer.TestServerError({ reason: `connect failed: ${String(cause)}` })
          })
          expect(connection.getServer()).toContain(String(first.port))
          const status = observeStatuses(connection)

          // Stop the standby first, then the active server that triggers the
          // reconnect loop. Both endpoints are now deterministic refusals.
          yield* second.stop
          yield* first.stop
          const [closed, statuses] = yield* awaitWithin(
            Promise.all([connection.closed(), status.finished]),
            `${transport} reconnect exhaustion`
          )

          expect(closed).toBeInstanceOf(Error)
          expect(statuses.map((event) => event.type)).toEqual([
            "disconnect",
            "reconnecting",
            "reconnecting",
            "reconnecting",
            "reconnecting",
            "close"
          ])
        })
      ),
    30_000
  )
}
