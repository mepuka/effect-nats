import { expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { probeNatsServer } from "./TestServer.ts"

it.effect("preflight names the required server and install step when the binary is absent", () =>
  Effect.gen(function*() {
    const failure = yield* Effect.flip(probeNatsServer("effect-nats-missing-nats-server"))
    expect(failure.reason).toContain("nats-server v2.14.5 is required")
    expect(failure.reason).toContain("\"effect-nats-missing-nats-server\" was not found on PATH")
    expect(failure.reason).toContain("Install v2.14.5")
  }))

it.effect("preflight accepts the declared server and reports the integration receipt", () =>
  Effect.gen(function*() {
    const receipt = yield* probeNatsServer()
    expect(receipt.serverVersion).toBe("v2.14.5")
    expect(receipt.jetStreamAndStorage).toContain("per TestServer.startWith")
    expect(receipt.clientRuntime).toContain(`Node ${process.version}`)
    expect(receipt.clientTransport).toBe("@nats-io/transport-node 3.4.0")
    expect(receipt.isolation).toContain("per-scope free port")
  }))
