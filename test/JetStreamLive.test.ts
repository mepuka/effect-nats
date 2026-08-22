import { connect } from "@nats-io/transport-node"
import { Effect } from "effect"
import * as JetStreamLive from "../src/internal/JetStreamLive.ts"
import { conformance } from "./JetStreamConformance.ts"
import * as TestServer from "./TestServer.ts"

// The live adapter against a real nats-server: one fresh server, connection,
// and JetStream domain per test, all scoped, so cases cannot interfere.
conformance(
  "live",
  (f) =>
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
    ),
  30_000
)
