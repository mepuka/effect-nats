/**
 * Host pump + client round trips over the in-memory loopback bus, all under
 * TestClock. The pump's actual wire output is checked against the SAME DFA
 * (`wellFormed`) derived from the upstream wire contract — model and implementation must
 * agree.
 */
import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Layer, Result, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AgentFailure } from "../agents/errors.js"
import { AgentClient, layerClient } from "../agents/client.js"
import { AgentHost, layerHost, type AgentHostConfig } from "../agents/host.js"
import { classify } from "../agents/prompt-engine.js"
import { wellFormed, type Frame } from "../agents/protocol/chunks.js"
import { Wire } from "../agents/wire.js"
import { makeLoopbackBus, msg, type LoopbackBus } from "./kit.js"

const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const ADDRESS = { agent: "cc", owner: "own", name: "main" }

const hostConfig = (overrides?: Partial<AgentHostConfig>): AgentHostConfig => ({
  address: ADDRESS,
  instanceId: "inst-1",
  heartbeatIntervalS: 30,
  handler: (req) =>
    Stream.make(
      { _tag: "Text", text: `echo:${req.prompt}` } as const,
      { _tag: "Status", status: "done" } as const,
    ),
  ...overrides,
})

const withBus = <A, E>(
  config: AgentHostConfig,
  body: (bus: LoopbackBus) => Effect.Effect<A, E, AgentClient | AgentHost | Wire>,
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bus = yield* makeLoopbackBus()
      const wireLayer = Layer.succeed(Wire)(bus.wire)
      const layer = Layer.mergeAll(
        layerHost(config).pipe(Layer.provide(wireLayer)),
        layerClient().pipe(Layer.provide(wireLayer)),
        wireLayer,
      )
      return yield* Effect.gen(function* () {
        yield* TestClock.adjust(1)
        return yield* body(bus)
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.provide(TestClock.layer())),
  )

const framesOn = (bus: LoopbackBus, subject: string): Frame[] =>
  bus.log
    .filter((e) => e.subject === subject)
    .map((e) =>
      classify(
        msg(e.payload, {
          errorCode: e.options?.errorCode,
          errorMessage: e.options?.errorMessage,
        }),
      ),
    )
    .filter(Result.isSuccess)
    .map((r) => r.success)

const promptRaw = (
  bus: LoopbackBus,
  payload: Uint8Array | string,
  inbox: string,
) =>
  bus.wire.publish(
    `agents.prompt.${ADDRESS.agent}.${ADDRESS.owner}.${ADDRESS.name}`,
    typeof payload === "string" ? new TextEncoder().encode(payload) : payload,
    { replyTo: inbox },
  )

const settle = TestClock.adjust(10)

describe("host pump (Protocol §6, §9)", () => {
  test("accepted prompt produces ack · chunks · terminator — DFA-valid", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* promptRaw(bus, "hello", "_inbox.t1")
        yield* settle
        const frames = framesOn(bus, "_inbox.t1")
        expect(wellFormed(frames)).toBe(true)
        expect(frames.map((f) => (f._tag === "ChunkFrame" ? f.chunk._tag : f._tag))).toEqual([
          "Status",
          "Response",
          "Status",
          "Terminator",
        ])
      }),
    )
  })

  test("§6.4 malformed request: 400 error · terminator, NO ack", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* promptRaw(bus, new Uint8Array(0), "_inbox.t2")
        yield* settle
        const frames = framesOn(bus, "_inbox.t2")
        expect(wellFormed(frames)).toBe(true)
        expect(frames).toHaveLength(2)
        expect(frames[0]?._tag).toBe("ErrorFrame")
        expect(frames[0]?._tag === "ErrorFrame" && frames[0].code).toBe(400)
        expect(frames[1]?._tag).toBe("Terminator")
      }),
    )
  })

  test("§9.3 handler failure: ack · error · terminator with the declared code", async () => {
    const config = hostConfig({
      handler: () => Stream.fail(new AgentFailure({ code: 403, message: "nope" })),
    })
    await withBus(config, (bus) =>
      Effect.gen(function* () {
        yield* promptRaw(bus, "hi", "_inbox.t3")
        yield* settle
        const frames = framesOn(bus, "_inbox.t3")
        expect(wellFormed(frames)).toBe(true)
        expect(frames.map((f) => f._tag)).toEqual(["ChunkFrame", "ErrorFrame", "Terminator"])
        expect(frames[1]?._tag === "ErrorFrame" && frames[1].code).toBe(403)
      }),
    )
  })

  test("handler-emitted ack is dropped — the pump owns the single ack", async () => {
    const config = hostConfig({
      handler: () =>
        Stream.make(
          { _tag: "Status", status: "ack" } as const,
          { _tag: "Text", text: "x" } as const,
        ),
    })
    await withBus(config, (bus) =>
      Effect.gen(function* () {
        yield* promptRaw(bus, "hi", "_inbox.t4")
        yield* settle
        expect(wellFormed(framesOn(bus, "_inbox.t4"))).toBe(true)
      }),
    )
  })

  test("§6 overflow: second concurrent prompt gets 429 · terminator", async () => {
    const config = hostConfig({
      maxInFlight: 1,
      handler: () =>
        Stream.fromIterable([{ _tag: "Text", text: "slow" } as const]).pipe(
          Stream.mapEffect((c) => Effect.as(Effect.sleep(1_000), c)),
        ),
    })
    await withBus(config, (bus) =>
      Effect.gen(function* () {
        yield* promptRaw(bus, "first", "_inbox.a")
        yield* TestClock.adjust(1)
        yield* promptRaw(bus, "second", "_inbox.b")
        yield* TestClock.adjust(2_000)
        const first = framesOn(bus, "_inbox.a")
        const second = framesOn(bus, "_inbox.b")
        expect(wellFormed(first)).toBe(true)
        expect(wellFormed(second)).toBe(true)
        expect(second[0]?._tag === "ErrorFrame" && second[0].code).toBe(429)
      }),
    )
  })

  test("§8 heartbeats publish on schedule from the shared builder", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* TestClock.adjust(90_000)
        const beats = bus.log.filter((e) => e.subject === "agents.hb.cc.own.main")
        expect(beats.length).toBeGreaterThanOrEqual(3)
        const body = JSON.parse(new TextDecoder().decode(beats[0]!.payload))
        expect(body.agent).toBe("cc")
        expect(body.owner).toBe("own")
        expect(body.instance_id).toBe("inst-1")
        expect(body.interval_s).toBe(30)
      }),
    )
  })

  test("§8.7.1 status endpoint replies from the same builder", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("agents.status.cc.own.main", new Uint8Array(0), {
          replyTo: "_inbox.status",
        })
        yield* settle
        const replies = bus.log.filter((e) => e.subject === "_inbox.status")
        expect(replies).toHaveLength(1)
        const body = JSON.parse(new TextDecoder().decode(replies[0]!.payload))
        expect(body.instance_id).toBe("inst-1")
        expect(body.interval_s).toBe(30)
      }),
    )
  })
})

describe("client ⇄ host round trip", () => {
  test("discover finds the host with required metadata, then prompt streams", async () => {
    await withBus(hostConfig(), () =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const fiber = yield* forkNow(client.discover())
        yield* TestClock.adjust(5_000)
        const found = yield* Fiber.join(fiber)
        expect(found).toHaveLength(1)
        const agentInfo = found[0]!
        expect(agentInfo.agent).toBe("cc")
        expect(agentInfo.owner).toBe("own")
        expect(agentInfo.instanceId).toBe("inst-1")
        const prompt = agentInfo.endpoints.find((e) => e.name === "prompt")
        expect(prompt?.maxPayload).toBe("1MB")
        expect(prompt?.attachmentsOk).toBe(true)

        const run = yield* forkNow(Stream.runCollect(client.prompt(agentInfo, "ping")))
        yield* TestClock.adjust(1_000)
        const chunks = yield* Fiber.join(run)
        expect(chunks.map((c) => c._tag)).toEqual(["Status", "Response", "Status"])
        expect(chunks[1]?._tag === "Response" && chunks[1].text).toBe("echo:ping")
      }),
    )
  })

  test("liveness flips off after 3× the declared interval (§8.2)", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const beat = JSON.stringify({
          agent: "other",
          owner: "own",
          instance_id: "manual-inst",
          ts: "2026-01-01T00:00:00Z",
          interval_s: 10,
        })
        yield* bus.wire.publish("agents.hb.other.own.x", new TextEncoder().encode(beat))
        yield* TestClock.adjust(1_000)
        expect(yield* client.isLive("manual-inst")).toBe(true)
        yield* TestClock.adjust(28_000)
        expect(yield* client.isLive("manual-inst")).toBe(true)
        yield* TestClock.adjust(2_000)
        expect(yield* client.isLive("manual-inst")).toBe(false)
        expect((yield* client.liveness()).has("manual-inst")).toBe(true)
      }),
    )
  })

  test("independent heartbeat subscribers both observe beats", async () => {
    await withBus(hostConfig(), () =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const a = yield* forkNow(Stream.runCollect(Stream.take(client.heartbeats(), 2)))
        const b = yield* forkNow(Stream.runCollect(Stream.take(client.heartbeats(), 2)))
        yield* TestClock.adjust(120_000)
        const [beatsA, beatsB] = [yield* Fiber.join(a), yield* Fiber.join(b)]
        expect(beatsA).toHaveLength(2)
        expect(beatsB).toHaveLength(2)
        expect(beatsA[0]?.instance_id).toBe("inst-1")
      }),
    )
  })

  test("§5.4 local validation blocks the wire on oversize payloads", async () => {
    await withBus(hostConfig({ maxPayload: "1KB" }), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const fiber = yield* forkNow(client.discover())
        yield* TestClock.adjust(5_000)
        const agentInfo = (yield* Fiber.join(fiber))[0]!
        const before = bus.log.length
        const error = yield* Effect.flip(
          Stream.runCollect(client.prompt(agentInfo, "x".repeat(2048))),
        )
        expect(error._tag).toBe("LocalViolation")
        expect(bus.log.length).toBe(before)
      }),
    )
  })

  test("prompt against an endpoint without required metadata fails locally", async () => {
    await withBus(hostConfig(), () =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const error = yield* Effect.flip(
          Stream.runCollect(
            client.prompt(
              {
                agent: "cc",
                owner: "own",
                instanceId: "z",
                metadata: {},
                endpoints: [{ name: "prompt", subject: "agents.prompt.cc.own.main" }],
              },
              "hi",
            ),
          ),
        )
        expect(error._tag).toBe("LocalViolation")
      }),
    )
  })
})
