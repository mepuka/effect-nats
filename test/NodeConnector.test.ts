import { expect, it } from "@effect/vitest"
import {
  AuthorizationError,
  ConnectionError,
  InvalidArgumentError,
  ProtocolError,
  TimeoutError,
  UserAuthenticationExpiredError
} from "@nats-io/nats-core"
import { Effect, Fiber } from "effect"
import type { ConnectFatalError, ConnectTransientError } from "../src/internal/Errors.ts"
import type { Connection } from "../src/internal/NatsTransport.ts"
import {
  classifyConnectCause,
  connectorFromDial,
  nodeConnector
} from "../src/internal/NodeConnector.ts"

it("classifies connect-time error classes", () => {
  const auth = classifyConnectCause(new AuthorizationError("authorization violation"))
  const expired = classifyConnectCause(new UserAuthenticationExpiredError("creds expired"))
  for (const issue of [auth, expired]) {
    expect(issue._tag).toBe("ConnectFatalError")
    expect((issue as ConnectFatalError).reason).toBe("auth")
  }

  const protocol = classifyConnectCause(new ProtocolError("bad line"))
  expect(protocol._tag).toBe("ConnectFatalError")
  expect((protocol as ConnectFatalError).reason).toBe("protocol")

  const config = classifyConnectCause(new InvalidArgumentError("bad option"))
  expect(config._tag).toBe("ConnectFatalError")
  expect((config as ConnectFatalError).reason).toBe("config")

  const timeout = classifyConnectCause(new TimeoutError())
  expect(timeout._tag).toBe("ConnectTransientError")
  expect((timeout as ConnectTransientError).reason).toBe("timeout")
})

it("classifies refused dials by their system cause code", () => {
  const refusedCause = Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:4222"), {
    code: "ECONNREFUSED"
  })
  const refused = classifyConnectCause(
    new ConnectionError("connection refused", { cause: refusedCause })
  )
  expect(refused._tag).toBe("ConnectTransientError")
  expect((refused as ConnectTransientError).reason).toBe("refused")

  const unreachableCause = Object.assign(new Error("read EHOSTUNREACH"), {
    code: "EHOSTUNREACH"
  })
  const unavailable = classifyConnectCause(
    new ConnectionError("error connecting", { cause: unreachableCause })
  )
  expect(unavailable._tag).toBe("ConnectTransientError")
  expect((unavailable as ConnectTransientError).reason).toBe("unavailable")
})

/** Live OS boundary: a closed local port refuses deterministically, exercising
 * the real promise rejection path rather than synthesized causes. */
it.effect("a live dial to a closed local port surfaces transient refusal", () =>
  Effect.gen(function*() {
    const failure = yield* Effect.flip(
      nodeConnector.connect({ servers: ["nats://127.0.0.1:1"] })
    )
    expect(failure._tag).toBe("ConnectTransientError")
    expect((failure as ConnectTransientError).reason).toBe("refused")
  }))

it.effect("a connection resolving after interruption is closed exactly once, never leaked", () =>
  Effect.gen(function*() {
    let closes = 0
    let resolveDial: (connection: Connection) => void = () => {}
    const lateConnection: Connection = {
      closed: () => new Promise(() => {}),
      close: () => {
        closes++
        return Promise.resolve()
      },
      drain: () => Promise.resolve(),
      flush: () => Promise.resolve(),
      isClosed: () => false,
      isDraining: () => false,
      status: () => ({ [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => {}) }) })
    }
    const connector = connectorFromDial(() =>
      new Promise((resolve) => {
        resolveDial = resolve
      })
    )
    const fiber = yield* Effect.forkChild(connector.connect({ servers: ["fake"] }), {
      startImmediately: true
    })
    yield* Fiber.interrupt(fiber)
    resolveDial(lateConnection)
    yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 0)))
    expect(closes).toBe(1)
  }))

it("classifies TLS certificate failures as fatal config, not retryable weather", () => {
  const certError = Object.assign(new Error("unable to verify the first certificate"), {
    code: "UNABLE_TO_VERIFY_LEAF_SIGNATURE"
  })
  const issue = classifyConnectCause(certError)
  expect(issue._tag).toBe("ConnectFatalError")
  if (issue._tag === "ConnectFatalError") expect(issue.reason).toBe("config")
})

it("redacts server userinfo out of error detail", () => {
  const issue = classifyConnectCause(new Error("dial nats://admin:hunter2@10.0.0.5:4222 failed"))
  expect(issue.detail).toBe("dial nats://<redacted>@10.0.0.5:4222 failed")
})

it("redacts token and password assignments out of error detail", () => {
  const issue = classifyConnectCause(
    new Error("dial failed token=hunter2 password=\"swordfish\" auth_token: abc123")
  )
  expect(issue.detail).toBe(
    "dial failed token=<redacted> password=\"<redacted>\" auth_token: <redacted>"
  )
})
