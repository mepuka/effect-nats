/**
 * Robustness slices (TDD): host-side MUST enforcement, config validation,
 * numeric safety, and liveness hygiene. Seams: wire frames out of the host
 * pump, AgentClient surface, and wire-contract functions.
 */
import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Layer, Result, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AgentClient, layerClient } from "../agents/client.js"
import { layerHost, type AgentHostConfig } from "../agents/host.js"
import { classify, promptStream } from "../agents/prompt-engine.js"
import { decodeChunk, wellFormed, type Frame } from "../agents/protocol/chunks.js"
import { parseRequest } from "../agents/protocol/envelope.js"
import { WireFailure } from "../agents/errors.js"
import { Wire, type SubscribeOptions, type WireShape } from "../agents/wire.js"
import { chunk, makeLoopbackBus, makeScriptedWire, msg, terminator, type LoopbackBus, type ScriptEntry } from "./kit.js"

const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const ADDRESS = { agent: "cc", owner: "own", name: "main" }
const PROMPT_SUBJECT = "agents.prompt.cc.own.main"

const hostConfig = (overrides?: Partial<AgentHostConfig>): AgentHostConfig => ({
  address: ADDRESS,
  instanceId: "inst-r",
  handler: (req) => Stream.make({ _tag: "Text", text: `echo:${req.prompt}` } as const),
  ...overrides,
})

const promptIdentity = {
  agent: "cc",
  owner: "own",
  instanceId: "inst-r",
  metadata: {},
  endpoints: [
    { name: "prompt", subject: PROMPT_SUBJECT, maxPayload: "1MB", attachmentsOk: true },
  ],
}

const layersFor = (bus: LoopbackBus, config: AgentHostConfig) => {
  const wireLayer = Layer.succeed(Wire)(bus.wire)
  return Layer.mergeAll(
    layerHost(config).pipe(Layer.provide(wireLayer)),
    layerClient().pipe(Layer.provide(wireLayer)),
    wireLayer,
  )
}

const withBus = <A, E>(
  config: AgentHostConfig,
  body: (bus: LoopbackBus) => Effect.Effect<A, E, AgentClient | Wire>,
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bus = yield* makeLoopbackBus()
      return yield* Effect.gen(function* () {
        yield* TestClock.adjust(1)
        return yield* body(bus)
      }).pipe(Effect.provide(layersFor(bus, config)))
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

describe("host-side MUST enforcement", () => {
  test("§5.1 attachments with attachments_ok=false → 400 · terminator, no ack", async () => {
    const payload = JSON.stringify({
      prompt: "go",
      attachments: [{ filename: "a.txt", content: "aGVsbG8=" }],
    })
    await withBus(hostConfig({ attachmentsOk: false }), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish(PROMPT_SUBJECT, new TextEncoder().encode(payload), {
          replyTo: "_inbox.r1",
        })
        yield* TestClock.adjust(10)
        const frames = framesOn(bus, "_inbox.r1")
        expect(wellFormed(frames)).toBe(true)
        expect(frames.map((f) => f._tag)).toEqual(["ErrorFrame", "Terminator"])
        expect(frames[0]?._tag === "ErrorFrame" && frames[0].code).toBe(400)
      }),
    )
  })

  test("§5.4 oversize request enforced server-side → 400 · terminator, no ack", async () => {
    await withBus(hostConfig({ maxPayload: "1KB" }), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish(PROMPT_SUBJECT, new TextEncoder().encode("x".repeat(2048)), {
          replyTo: "_inbox.r2",
        })
        yield* TestClock.adjust(10)
        const frames = framesOn(bus, "_inbox.r2")
        expect(wellFormed(frames)).toBe(true)
        expect(frames.map((f) => f._tag)).toEqual(["ErrorFrame", "Terminator"])
        expect(frames[0]?._tag === "ErrorFrame" && frames[0].code).toBe(400)
      }),
    )
  })
})

describe("host config validation", () => {
  test("invalid max_payload format fails layer construction", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        const layer = layerHost(hostConfig({ maxPayload: "banana" })).pipe(
          Layer.provide(Layer.succeed(Wire)(bus.wire)),
        )
        return yield* Effect.flip(Effect.provide(Effect.void, layer))
      }).pipe(Effect.provide(TestClock.layer())),
    )
    expect(error._tag).toBe("HostConfigError")
  })
})

describe("client input validation", () => {
  const agentInfo = {
    agent: "cc",
    owner: "own",
    instanceId: "z",
    metadata: {},
    endpoints: [
      {
        name: "prompt",
        subject: PROMPT_SUBJECT,
        maxPayload: "1MB",
        attachmentsOk: true,
      },
    ],
  }

  test("non-positive inactivityMs fails locally before any wire traffic", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const before = bus.log.length
        for (const inactivityMs of [0, -1, Number.NaN]) {
          const error = yield* Effect.flip(
            Stream.runCollect(client.prompt(agentInfo, "hi", { inactivityMs })),
          )
          expect(error._tag).toBe("LocalViolation")
        }
        expect(bus.log.length).toBe(before)
      }),
    )
  })
})

describe("liveness hygiene", () => {
  test("§8.3 beats with non-positive interval_s are ignored by the tracker", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const beat = (intervalS: number) =>
          new TextEncoder().encode(
            JSON.stringify({
              agent: "bad",
              owner: "own",
              instance_id: "bad-inst",
              ts: "2026-01-01T00:00:00Z",
              interval_s: intervalS,
            }),
          )
        yield* bus.wire.publish("agents.hb.bad.own.x", beat(-5))
        yield* bus.wire.publish("agents.hb.bad.own.x", beat(0))
        yield* bus.wire.publish("agents.hb.bad.own.x", beat(Number.NaN))
        yield* TestClock.adjust(10)
        expect((yield* client.liveness()).has("bad-inst")).toBe(false)
        expect(yield* client.isLive("bad-inst")).toBe(false)
      }),
    )
  })

  test("malformed heartbeat payloads never kill the tracker", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        yield* bus.wire.publish("agents.hb.bad.own.x", new TextEncoder().encode("not json"))
        yield* bus.wire.publish("agents.hb.bad.own.x", new TextEncoder().encode('{"agent":1}'))
        yield* TestClock.adjust(30_001)
        expect((yield* client.liveness()).has("inst-r")).toBe(true)
      }),
    )
  })
})

describe("pump concurrency", () => {
  test("a slow handler never blocks ingestion of the next prompt", async () => {
    const config = hostConfig({
      maxInFlight: 2,
      handler: (req) =>
        req.prompt === "slow"
          ? Stream.fromIterable([{ _tag: "Text", text: "slow-done" } as const]).pipe(
              Stream.mapEffect((c) => Effect.as(Effect.sleep(5_000), c)),
            )
          : Stream.make({ _tag: "Text", text: "fast-done" } as const),
    })
    await withBus(config, (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish(PROMPT_SUBJECT, new TextEncoder().encode("slow"), {
          replyTo: "_inbox.slow",
        })
        yield* TestClock.adjust(1)
        yield* bus.wire.publish(PROMPT_SUBJECT, new TextEncoder().encode("fast"), {
          replyTo: "_inbox.fast",
        })
        yield* TestClock.adjust(10)
        expect(framesOn(bus, "_inbox.fast").map((f) => f._tag)).toEqual([
          "ChunkFrame",
          "ChunkFrame",
          "Terminator",
        ])
        expect(framesOn(bus, "_inbox.slow").some((f) => f._tag === "Terminator")).toBe(false)
        yield* TestClock.adjust(5_000)
        expect(wellFormed(framesOn(bus, "_inbox.slow"))).toBe(true)
      }),
    )
  })
})

describe("caller-side reply DFA (§6.4/§6.5)", () => {
  const engineError = (script: ReadonlyArray<ScriptEntry>) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const scripted = makeScriptedWire(script)
        const stream = promptStream({
          subject: "agents.prompt.cc.o.n",
          payload: new TextEncoder().encode("hi"),
        }).pipe(Stream.provideService(Wire, scripted.wire))
        const fiber = yield* forkNow(Effect.flip(Stream.runCollect(stream)))
        yield* TestClock.adjust(100)
        return yield* Fiber.join(fiber)
      }).pipe(Effect.provide(TestClock.layer())),
    )

  test("bare terminator without ack is a protocol violation", async () => {
    expect((await engineError([terminator(0)]))._tag).toBe("ProtocolViolation")
  })

  test("content before ack is a protocol violation", async () => {
    expect(
      (await engineError([chunk(0, { type: "response", data: "x" }), terminator(1)]))._tag,
    ).toBe("ProtocolViolation")
  })

  test("duplicate ack is a protocol violation", async () => {
    expect(
      (
        await engineError([
          chunk(0, { type: "status", data: "ack" }),
          chunk(1, { type: "status", data: "ack" }),
          terminator(2),
        ])
      )._tag,
    ).toBe("ProtocolViolation")
  })
})

describe("boundary strictness", () => {
  test("§5.1 invalid UTF-8 is a typed 400, not replacement-decoded content", () => {
    expect(parseRequest(Uint8Array.of(0xff))).toEqual({ _tag: "MalformedUtf8" })
    expect(parseRequest(Uint8Array.of(0x7b, 0xff))).toEqual({ _tag: "MalformedUtf8" })
  })

  test("§7.1 query reply_subject must be a publishable subject", () => {
    const bad = (replySubject: string) =>
      Result.isFailure(
        decodeChunk({
          type: "query",
          data: { id: "q", reply_subject: replySubject, prompt: "p" },
        }),
      )
    expect(bad("a.*")).toBe(true)
    expect(bad("")).toBe(true)
    expect(bad("a..b")).toBe(true)
    expect(bad("a b")).toBe(true)
    expect(bad("_INBOX.Xj7k9Q2pA")).toBe(false)
  })

  test("§5.4 answerQuery validates subject, serializability, and broker cap", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const before = bus.log.length
        const badSubject = yield* Effect.flip(
          client.answerQuery({ replySubject: "a.>" }, "yes"),
        )
        expect(badSubject._tag).toBe("LocalViolation")
        const oversize = yield* Effect.flip(
          client.answerQuery({ replySubject: "_inbox.q" }, "x".repeat(2_000_000)),
        )
        expect(oversize._tag).toBe("LocalViolation")
        expect(bus.log.length).toBe(before)
      }),
    )
  })

  test("unserializable extras fail locally, not as defects", async () => {
    await withBus(hostConfig(), (bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        const before = bus.log.length
        const error = yield* Effect.flip(
          Stream.runCollect(
            client.prompt(promptIdentity, "hi", { extra: { value: 1n as unknown as string } }),
          ),
        )
        expect(error._tag).toBe("LocalViolation")
        expect(bus.log.length).toBe(before)
      }),
    )
  })
})

describe("registration and discovery readiness", () => {
  test("a prompt published immediately after layer build round-trips (no races)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        yield* Effect.gen(function* () {
          const client = yield* AgentClient
          const run = yield* forkNow(Stream.runCollect(client.prompt(promptIdentity, "now")))
          yield* TestClock.adjust(10)
          const chunks = yield* Fiber.join(run)
          expect(chunks.map((c) => c._tag)).toEqual(["Status", "Response"])
        }).pipe(Effect.provide(layersFor(bus, hostConfig())))
      }).pipe(Effect.provide(TestClock.layer())),
    )
  })

  test("transient subscribe failures delay readiness instead of faking it", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        const flakyWire: WireShape = {
          ...bus.wire,
          subscribe: (() => {
            const failedOnce = new Set<string>()
            return (subject: string, opts?: SubscribeOptions) => {
              if (failedOnce.has(subject)) return bus.wire.subscribe(subject, opts)
              failedOnce.add(subject)
              return Effect.fail(new WireFailure({ reason: "transient subscribe failure" }))
            }
          })(),
        }
        const wireLayer = Layer.succeed(Wire)(flakyWire)
        const layer = Layer.mergeAll(
          layerHost(hostConfig({ restartMs: 50 })).pipe(Layer.provide(wireLayer)),
          layerClient().pipe(Layer.provide(wireLayer)),
          wireLayer,
        )
        const program = Effect.gen(function* () {
          const client = yield* AgentClient
          const run = yield* forkNow(Stream.runCollect(client.prompt(promptIdentity, "now")))
          yield* TestClock.adjust(10)
          return yield* Fiber.join(run)
        }).pipe(Effect.provide(layer))
        const fiber = yield* forkNow(program)
        yield* TestClock.adjust(70_000)
        const chunks = yield* Fiber.join(fiber)
        expect(chunks.map((c) => c._tag)).toEqual(["Status", "Response"])
      }).pipe(Effect.provide(TestClock.layer())),
    )
  })

  test("$SRV.PING replies carry the service version and metadata", async () => {
    await withBus(hostConfig({ version: "2.1.0" }), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("$SRV.PING.agents", new Uint8Array(0), {
          replyTo: "_inbox.pingv",
        })
        yield* TestClock.adjust(10)
        const reply = bus.log.find((e) => e.subject === "_inbox.pingv")
        expect(reply).toBeDefined()
        const body = JSON.parse(new TextDecoder().decode(reply!.payload))
        expect(body.version).toBe("2.1.0")
        expect(body.metadata.protocol_version).toBe("0.3")
      }),
    )
  })
})

describe("numeric safety", () => {
  test("max_payload byte counts beyond MAX_SAFE_INTEGER are rejected", async () => {
    const { parseMaxPayload } = await import("../agents/protocol/validation.js")
    expect(parseMaxPayload("8589934592GB")).toBeNull()
    expect(parseMaxPayload("9007199254740993B")).toBeNull()
    expect(parseMaxPayload("4GB")).toBe(4 * 1024 ** 3)
  })
})
