import { expect, it } from "@effect/vitest"
import {
  AuthorizationError,
  ConnectionError,
  InvalidArgumentError,
  ProtocolError,
  TimeoutError,
  UserAuthenticationExpiredError
} from "@nats-io/nats-core"
import { Effect } from "effect"
import type { ConnectFatalError, ConnectTransientError } from "../../src/internal/Errors.ts"
import { classifyConnectCause, nodeConnector } from "../../src/internal/NodeConnector.ts"

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
