import { expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { connectorFromDial } from "../src/internal/NodeConnector.ts"
import { compile, defaultPolicy, type ReconnectPolicy } from "../src/internal/ReconnectPolicy.ts"
import { makeFakeConnection } from "./Kit.ts"

it("compiles ADR-0001 defaults to the official additive-jitter options", () => {
  expect(compile(defaultPolicy)).toEqual({
    reconnect: true,
    maxReconnectAttempts: 10,
    reconnectTimeWait: 2_000,
    reconnectJitter: 100,
    reconnectJitterTLS: 1_000,
    waitOnFirstConnect: false,
    ignoreAuthErrorAbort: false,
    noRandomize: false
  })
})

it("maps typed disabled and unbounded policies without exposing numeric sentinels", () => {
  expect(compile({ _tag: "Disabled" })).toEqual({ reconnect: false })

  const policy: ReconnectPolicy = {
    ...defaultPolicy,
    attempts: { _tag: "Unbounded" },
    firstConnect: "Wait",
    authErrors: "Continue",
    serverOrder: "Configured"
  }
  expect(compile(policy)).toMatchObject({
    reconnect: true,
    maxReconnectAttempts: -1,
    waitOnFirstConnect: true,
    ignoreAuthErrorAbort: true,
    noRandomize: true
  })
})

it("custom delay replaces the official base and jitter handler wholesale", () => {
  let calls = 0
  const options = compile({
    ...defaultPolicy,
    delay: {
      _tag: "Custom",
      nextDelayMillis: () => ++calls * 25
    }
  })
  expect(options.reconnectTimeWait).toBeUndefined()
  expect(options.reconnectJitter).toBeUndefined()
  expect(options.reconnectJitterTLS).toBeUndefined()
  expect(options.reconnectDelayHandler?.()).toBe(25)
  expect(options.reconnectDelayHandler?.()).toBe(50)
})

it("rejects invalid bounded attempt counts before they reach the compatibility sentinel", () => {
  for (const attempts of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    expect(() =>
      compile({
        ...defaultPolicy,
        attempts: { _tag: "Bounded", attempts }
      })
    ).toThrow(/positive integer/)
  }
})

it.effect("the Node connector hands one compiled policy to one official connect call", () =>
  Effect.gen(function*() {
    const fake = yield* makeFakeConnection()
    const seen: Array<Record<string, unknown>> = []
    const connector = connectorFromDial((options) => {
      seen.push(options)
      return Promise.resolve(fake.connection)
    })

    yield* connector.connect({
      servers: ["nats://one:4222", "nats://two:4222"],
      reconnect: {
        ...defaultPolicy,
        attempts: { _tag: "Bounded", attempts: 3 },
        serverOrder: "Configured"
      }
    })

    expect(seen).toEqual([{
      servers: ["nats://one:4222", "nats://two:4222"],
      reconnect: true,
      maxReconnectAttempts: 3,
      reconnectTimeWait: 2_000,
      reconnectJitter: 100,
      reconnectJitterTLS: 1_000,
      waitOnFirstConnect: false,
      ignoreAuthErrorAbort: false,
      noRandomize: true
    }])
  }))
