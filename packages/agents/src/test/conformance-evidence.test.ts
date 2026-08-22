/**
 * Conformance evidence for Synadia Agent Protocol v0.3 stipulations not already
 * exercised by the model/property/engine/agent-host suites. Each test names the
 * spec line(s) it witnesses; the catalog in docs/conformance references these.
 */
import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Layer, Result, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AgentClient, isCompatibleProtocolVersion, layerClient } from "../agents/client.js"
import { AgentFailure, type PromptError } from "../agents/errors.js"
import { AgentHost, layerHost, type AgentHostConfig } from "../agents/host.js"
import { classify, promptStream } from "../agents/prompt-engine.js"
import {
  decodeChunk,
  decodeHeartbeat,
  decodeServiceInfo,
  ERROR_CODES,
  wellFormed,
  type Chunk,
  type Frame,
} from "../agents/protocol/chunks.js"
import { isRequestError, parseRequest } from "../agents/protocol/envelope.js"
import { isConventionalToken, parseAgentSubject } from "../agents/protocol/subjects.js"
import { Wire } from "../agents/wire.js"
import {
  chunk,
  makeLoopbackBus,
  makeScriptedWire,
  msg,
  terminator,
  type LoopbackBus,
  type ScriptEntry,
} from "./kit.js"

const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const enc = (s: string): Uint8Array => new TextEncoder().encode(s)
const dec = (b: Uint8Array): string => new TextDecoder().decode(b)
const EMPTY = new Uint8Array(0)

// -- wire-contract model evidence ---------------------------------------------

describe("§11 protocol version compatibility", () => {
  test("§11.1/§11.2 exact 0.3 accepted; patch and pre-release qualifiers ignored", () => {
    expect(isCompatibleProtocolVersion("0.3")).toBe(true)
    expect(isCompatibleProtocolVersion("0.3.0")).toBe(true)
    expect(isCompatibleProtocolVersion("0.3.7-beta")).toBe(true)
  })

  test("§11.2 0.x pins to exact MINOR; different MAJOR and malformed rejected", () => {
    expect(isCompatibleProtocolVersion("0.2")).toBe(false)
    expect(isCompatibleProtocolVersion("0.4")).toBe(false)
    expect(isCompatibleProtocolVersion("1.0")).toBe(false)
    expect(isCompatibleProtocolVersion("3")).toBe(false)
    expect(isCompatibleProtocolVersion("banana")).toBe(false)
    expect(isCompatibleProtocolVersion("")).toBe(false)
  })
})

describe("§6.3 response chunk forms", () => {
  test("string and object data forms decode to the same text", () => {
    const s = decodeChunk({ type: "response", data: "Hello, world." })
    const o = decodeChunk({ type: "response", data: { text: "Hello, world." } })
    expect(Result.isSuccess(s) && s.success._tag === "Response" && s.success.text).toBe(
      "Hello, world.",
    )
    expect(Result.isSuccess(o) && o.success._tag === "Response" && o.success.text).toBe(
      "Hello, world.",
    )
  })

  test("§6.6 unknown fields inside data objects are preserved for relay", () => {
    const r = decodeChunk({ type: "response", data: { text: "t", trace: "tr-1" } })
    expect(
      Result.isSuccess(r) && r.success._tag === "Response" && r.success.extra["trace"],
    ).toBe("tr-1")
    const q = decodeChunk({
      type: "query",
      data: { id: "i", reply_subject: "_INBOX.q", prompt: "?", hint: "h" },
    })
    expect(Result.isSuccess(q) && q.success._tag === "Query" && q.success.extra["hint"]).toBe("h")
  })
})

describe("§7.1 query chunks", () => {
  test("decodes id, reply_subject, prompt; attachments optional", () => {
    const r = decodeChunk({
      type: "query",
      data: {
        id: "a8f1c2e4",
        reply_subject: "_INBOX.Xj7k9Q2pA",
        prompt: "Confirm? (yes/no)",
      },
    })
    expect(Result.isSuccess(r)).toBe(true)
    if (Result.isSuccess(r) && r.success._tag === "Query") {
      expect(r.success.id).toBe("a8f1c2e4")
      expect(r.success.replySubject).toBe("_INBOX.Xj7k9Q2pA")
      expect(r.success.prompt).toBe("Confirm? (yes/no)")
      expect(r.success.attachments).toEqual([])
    }
  })

  test("missing reply_subject is a malformed known chunk", () => {
    const r = decodeChunk({ type: "query", data: { id: "i", prompt: "?" } })
    expect(Result.isFailure(r)).toBe(true)
  })
})

describe("§9.1 error wire shape (caller side)", () => {
  test("JSON body `message` is preferred over the Nats-Service-Error header", () => {
    const r = classify(
      msg(JSON.stringify({ error: "rate_limited", message: "from body", retry_after_s: 30 }), {
        errorCode: 429,
        errorMessage: "from header",
      }),
    )
    expect(Result.isSuccess(r)).toBe(true)
    if (Result.isSuccess(r) && r.success._tag === "ErrorFrame") {
      expect(r.success.code).toBe(429)
      expect(r.success.message).toBe("from body")
    }
  })

  test("empty body falls back to the header; headers checked before body", () => {
    const r = classify(msg(EMPTY, { errorCode: 500, errorMessage: "hdr" }))
    expect(Result.isSuccess(r)).toBe(true)
    if (Result.isSuccess(r) && r.success._tag === "ErrorFrame") {
      expect(r.success.code).toBe(500)
      expect(r.success.message).toBe("hdr")
    }
  })
})

describe("§9.2 status code taxonomy", () => {
  test("all seven codes are pinned", () => {
    expect(ERROR_CODES).toEqual({
      badRequest: 400,
      unauthorized: 401,
      forbidden: 403,
      notFound: 404,
      conflict: 409,
      rateLimited: 429,
      internal: 500,
    })
  })
})

describe("§5.2 base64 strictness", () => {
  test("whitespace inside attachment content is rejected", () => {
    const r = parseRequest(
      enc(JSON.stringify({ prompt: "p", attachments: [{ filename: "f", content: "ab =" }] })),
    )
    expect(r).toEqual({ _tag: "InvalidAttachment", filename: "f" })
  })
})

describe("§2.2 token length rule", () => {
  test("conventional tokens are 1–63 chars", () => {
    expect(isConventionalToken("a".repeat(63))).toBe(true)
    expect(isConventionalToken("a".repeat(64))).toBe(false)
    expect(isConventionalToken("")).toBe(false)
  })
})

describe("§8.3 heartbeat decoding", () => {
  test("callers tolerate additional unknown heartbeat fields", () => {
    const r = decodeHeartbeat({
      agent: "cc",
      owner: "o",
      instance_id: "i-1",
      ts: "2026-04-28T14:23:01Z",
      interval_s: 30,
      cost_usd: 0.25,
      queue_depth: 3,
    })
    expect(Result.isSuccess(r)).toBe(true)
  })
})

// -- scripted-wire engine evidence --------------------------------------------

const runScripted = <A, E>(
  script: ReadonlyArray<ScriptEntry>,
  use: (collect: Effect.Effect<Array<Chunk>, PromptError>) => Effect.Effect<A, E>,
): Promise<A> => {
  const scripted = makeScriptedWire(script)
  const stream = promptStream({
    subject: "agents.prompt.cc.o.n",
    payload: enc("hi"),
    inactivityMs: 60_000,
  }).pipe(Stream.provideService(Wire, scripted.wire))
  return Effect.runPromise(
    use(Stream.runCollect(stream)).pipe(Effect.provide(TestClock.layer())),
  )
}

const ACK_CHUNK = { type: "status", data: "ack" }

describe("§6.4 status handling (caller side)", () => {
  test("unrecognized status values pass through without failing the stream", async () => {
    const collected = await runScripted(
      [
        chunk(0, ACK_CHUNK),
        chunk(1, { type: "status", data: "warming-up" }),
        chunk(1, { type: "response", data: "ok" }),
        terminator(1),
      ],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(10)
          return yield* Fiber.join(fiber)
        }),
    )
    expect(collected.map((c) => c._tag)).toEqual(["Status", "Status", "Response"])
    expect(collected[1]?._tag === "Status" && collected[1].status).toBe("warming-up")
  })
})

describe("§6.3/§6.6 publication order", () => {
  test("multiple response chunks concatenate in publication order", async () => {
    const collected = await runScripted(
      [
        chunk(0, ACK_CHUNK),
        chunk(1, { type: "response", data: "a" }),
        chunk(1, { type: "response", data: { text: "b" } }),
        chunk(1, { type: "response", data: "c" }),
        terminator(1),
      ],
      (collect) =>
        Effect.gen(function* () {
          const fiber = yield* forkNow(collect)
          yield* TestClock.adjust(10)
          return yield* Fiber.join(fiber)
        }),
    )
    const text = collected
      .filter((c): c is Extract<Chunk, { _tag: "Response" }> => c._tag === "Response")
      .map((c) => c.text)
      .join("")
    expect(text).toBe("abc")
  })
})

// -- loopback host/client evidence ---------------------------------------------

const ADDRESS = { agent: "cc", owner: "own", name: "main" }

const hostConfig = (overrides?: Partial<AgentHostConfig>): AgentHostConfig => ({
  address: ADDRESS,
  instanceId: "inst-e",
  heartbeatIntervalS: 30,
  handler: (req) => Stream.make({ _tag: "Text", text: `echo:${req.prompt}` } as const),
  ...overrides,
})

const withHost = <A, E>(
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

const withClientOnly = <A, E>(
  body: (bus: LoopbackBus) => Effect.Effect<A, E, AgentClient | Wire>,
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bus = yield* makeLoopbackBus()
      const wireLayer = Layer.succeed(Wire)(bus.wire)
      const layer = Layer.mergeAll(layerClient().pipe(Layer.provide(wireLayer)), wireLayer)
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

/** Reply with fixed payloads to every request arriving on `subject`. */
const respondOn = (
  bus: LoopbackBus,
  subject: string,
  replies: ReadonlyArray<Uint8Array>,
): Effect.Effect<void> =>
  Effect.asVoid(
    forkNow(
      Effect.scoped(
        Effect.gen(function* () {
          const msgs = yield* bus.wire.subscribe(subject)
          yield* msgs.pipe(
            Stream.runForEach((m) =>
              m.replyTo === undefined
                ? Effect.void
                : Effect.forEach(replies, (bytes) => bus.wire.publish(m.replyTo!, bytes)),
            ),
          )
        }),
      ),
    ),
  )

describe("§3 service registration shape", () => {
  test("service info: name, metadata, endpoint names/subjects/queue groups per §2/§3/§12", async () => {
    await withHost(hostConfig(), () =>
      Effect.gen(function* () {
        const host = yield* AgentHost
        const info = decodeServiceInfo(host.serviceInfo)
        expect(Result.isSuccess(info)).toBe(true)
        if (!Result.isSuccess(info)) return
        const i = info.success
        expect(i.name).toBe("agents")
        expect(i.id).toBe("inst-e")
        const md = i.metadata ?? {}
        expect(md["agent"]).toBe("cc")
        expect(md["owner"]).toBe("own")
        expect(md["protocol_version"]).toBe("0.3")
        // §2/§3.2: instance name lives only in the subject, never in metadata
        expect("name" in md).toBe(false)
        expect("session" in md).toBe(false)
        const names = i.endpoints.map((e) => e.name).sort()
        expect(names).toEqual(["prompt", "status"])
        for (const e of i.endpoints) {
          // §2: endpoint name MUST match the verb token of its default subject
          expect(parseAgentSubject(e.subject)?.verb).toBe(e.name as never)
          expect(e.queue_group).toBe("agents")
        }
        const prompt = i.endpoints.find((e) => e.name === "prompt")!
        expect(prompt.subject).toBe("agents.prompt.cc.own.main")
        expect(prompt.metadata?.["max_payload"]).toBe("1MB")
        expect(prompt.metadata?.["attachments_ok"]).toBe(true)
        const status = i.endpoints.find((e) => e.name === "status")!
        expect(status.subject).toBe("agents.status.cc.own.main")
      }),
    )
  })

  test("§3.2/§8.3 session flows into metadata and heartbeat payload when configured", async () => {
    await withHost(hostConfig({ session: "synadia-com-2" }), (bus) =>
      Effect.gen(function* () {
        const host = yield* AgentHost
        const md = host.serviceInfo["metadata"] as Record<string, unknown>
        expect(md["session"]).toBe("synadia-com-2")
        yield* TestClock.adjust(30_001)
        const beats = bus.log.filter((e) => e.subject === "agents.hb.cc.own.main")
        expect(beats.length).toBeGreaterThanOrEqual(1)
        const body = JSON.parse(dec(beats[0]!.payload))
        expect(body.session).toBe("synadia-com-2")
      }),
    )
  })
})

describe("§8 heartbeat payload", () => {
  test("§8.3 ts is UTC ISO 8601, interval_s matches config, session absent when unset", async () => {
    await withHost(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* TestClock.adjust(30_001)
        const beats = bus.log.filter((e) => e.subject === "agents.hb.cc.own.main")
        expect(beats.length).toBeGreaterThanOrEqual(1)
        const body = JSON.parse(dec(beats[0]!.payload))
        expect(Result.isSuccess(decodeHeartbeat(body))).toBe(true)
        expect(body.ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/)
        expect(Number.isNaN(Date.parse(body.ts))).toBe(false)
        expect(body.interval_s).toBe(30)
        expect(body.instance_id).toBe("inst-e")
        expect("session" in body).toBe(false)
      }),
    )
  })
})

describe("§4 / §12 $SRV responders", () => {
  test("$SRV.PING.agents replies with a ping-shaped record", async () => {
    await withHost(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("$SRV.PING.agents", EMPTY, { replyTo: "_inbox.ping" })
        yield* TestClock.adjust(10)
        const replies = bus.log.filter((e) => e.subject === "_inbox.ping")
        expect(replies).toHaveLength(1)
        const body = JSON.parse(dec(replies[0]!.payload))
        expect(body.type).toBe("io.nats.micro.v1.ping_response")
        expect(body.name).toBe("agents")
        expect(body.id).toBe("inst-e")
      }),
    )
  })

  test("$SRV.INFO.agents.{instance_id} direct lookup replies with full service info", async () => {
    await withHost(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("$SRV.INFO.agents.inst-e", EMPTY, { replyTo: "_inbox.info" })
        yield* TestClock.adjust(10)
        const replies = bus.log.filter((e) => e.subject === "_inbox.info")
        expect(replies).toHaveLength(1)
        const info = decodeServiceInfo(JSON.parse(dec(replies[0]!.payload)))
        expect(Result.isSuccess(info)).toBe(true)
        if (Result.isSuccess(info)) {
          expect(info.success.id).toBe("inst-e")
          expect(info.success.endpoints.some((e) => e.name === "prompt")).toBe(true)
        }
      }),
    )
  })
})

describe("§8.7 status endpoint", () => {
  test("ignores a non-empty request body and replies exactly §8.3-shaped", async () => {
    await withHost(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("agents.status.cc.own.main", enc("garbage {{{ not json"), {
          replyTo: "_inbox.st",
        })
        yield* TestClock.adjust(10)
        const replies = bus.log.filter((e) => e.subject === "_inbox.st")
        expect(replies).toHaveLength(1)
        const body = JSON.parse(dec(replies[0]!.payload))
        const hb = decodeHeartbeat(body)
        expect(Result.isSuccess(hb)).toBe(true)
        if (Result.isSuccess(hb)) {
          expect(hb.success.agent).toBe("cc")
          expect(hb.success.owner).toBe("own")
          expect(hb.success.instance_id).toBe("inst-e")
          expect(hb.success.interval_s).toBe(30)
        }
      }),
    )
  })
})

describe("§9 host error emission", () => {
  test("§9.1 400 rejection body carries snake_case error code and message", async () => {
    await withHost(hostConfig(), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("agents.prompt.cc.own.main", EMPTY, { replyTo: "_inbox.e1" })
        yield* TestClock.adjust(10)
        const entries = bus.log.filter((e) => e.subject === "_inbox.e1")
        expect(entries).toHaveLength(2)
        expect(entries[0]!.options?.errorCode).toBe(400)
        const body = JSON.parse(dec(entries[0]!.payload))
        expect(body.error).toBe("bad_request")
        expect(typeof body.message).toBe("string")
        expect(entries[1]!.payload.byteLength).toBe(0)
        expect(entries[1]!.options?.errorCode).toBeUndefined()
      }),
    )
  })

  test("§9.2 handler-declared codes outside the taxonomy degrade to 500", async () => {
    const config = hostConfig({
      handler: () => Stream.fail(new AgentFailure({ code: 999, message: "weird" })),
    })
    await withHost(config, (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish("agents.prompt.cc.own.main", enc("hi"), {
          replyTo: "_inbox.e2",
        })
        yield* TestClock.adjust(10)
        const frames = framesOn(bus, "_inbox.e2")
        expect(wellFormed(frames)).toBe(true)
        const err = frames.find((f) => f._tag === "ErrorFrame")
        expect(err?._tag === "ErrorFrame" && err.code).toBe(500)
      }),
    )
  })
})

describe("§5.6 host passes unknown envelope fields through", () => {
  test("handler observes top-level extras from a raw JSON request", async () => {
    const config = hostConfig({
      handler: (req) =>
        Stream.make({ _tag: "Text", text: String(req.extra["trace_id"] ?? "none") } as const),
    })
    await withHost(config, (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish(
          "agents.prompt.cc.own.main",
          enc(JSON.stringify({ prompt: "p", trace_id: "t-9" })),
          { replyTo: "_inbox.x1" },
        )
        yield* TestClock.adjust(10)
        const frames = framesOn(bus, "_inbox.x1")
        const resp = frames.find(
          (f) => f._tag === "ChunkFrame" && f.chunk._tag === "Response",
        )
        expect(
          resp?._tag === "ChunkFrame" && resp.chunk._tag === "Response" && resp.chunk.text,
        ).toBe("t-9")
      }),
    )
  })
})

describe("§3.3/§3.4 queue group load-balancing", () => {
  test("two hosts at the same agent address: a prompt is handled by exactly one", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        const wireLayer = Layer.succeed(Wire)(bus.wire)
        const layer = Layer.mergeAll(
          layerHost(hostConfig({ instanceId: "inst-a" })).pipe(Layer.provide(wireLayer)),
          layerHost(hostConfig({ instanceId: "inst-b" })).pipe(Layer.provide(wireLayer)),
          wireLayer,
        )
        yield* Effect.gen(function* () {
          yield* TestClock.adjust(1)
          yield* bus.wire.publish("agents.prompt.cc.own.main", enc("hello"), {
            replyTo: "_inbox.q1",
          })
          yield* TestClock.adjust(10)
          const frames = framesOn(bus, "_inbox.q1")
          expect(wellFormed(frames)).toBe(true)
          const acks = frames.filter(
            (f) => f._tag === "ChunkFrame" && f.chunk._tag === "Status" && f.chunk.status === "ack",
          )
          expect(acks).toHaveLength(1)
        }).pipe(Effect.provide(layer))
      }).pipe(Effect.provide(TestClock.layer())),
    )
  })
})

// -- discovery-record evidence (crafted responders, no real host) ---------------

const record = (over: Record<string, unknown>): Uint8Array =>
  enc(
    JSON.stringify({
      type: "io.nats.micro.v1.info_response",
      name: "agents",
      id: "good-1",
      version: "1.0.0",
      metadata: { agent: "gg", owner: "oo", protocol_version: "0.3" },
      endpoints: [
        {
          name: "prompt",
          subject: "agents.prompt.gg.oo.n1",
          queue_group: "agents",
          metadata: { max_payload: "1MB", attachments_ok: true },
        },
      ],
      ...over,
    }),
  )

describe("§4/§11.2 discovery filtering", () => {
  test("incompatible or non-compliant records are excluded; discovery uses $SRV.INFO.agents only", async () => {
    await withClientOnly((bus) =>
      Effect.gen(function* () {
        yield* respondOn(bus, "$SRV.INFO.agents", [
          record({}),
          record({ id: "bad-ver", metadata: { agent: "a", owner: "o", protocol_version: "0.2" } }),
          record({ id: "bad-name", name: "other" }),
          record({ id: "bad-owner", metadata: { agent: "a", protocol_version: "0.3" } }),
        ])
        yield* TestClock.adjust(1)
        const client = yield* AgentClient
        const fiber = yield* forkNow(client.discover())
        yield* TestClock.adjust(5_000)
        const found = yield* Fiber.join(fiber)
        expect(found.map((f) => f.instanceId)).toEqual(["good-1"])
        const srvPublishes = bus.log.filter((e) => e.subject.startsWith("$SRV"))
        expect(srvPublishes.map((e) => e.subject)).toEqual(["$SRV.INFO.agents"])
      }),
    )
  })

  test("§3.2/§12 unknown metadata keys and endpoint names are preserved on AgentInfo", async () => {
    await withClientOnly((bus) =>
      Effect.gen(function* () {
        yield* respondOn(bus, "$SRV.INFO.agents", [
          record({
            metadata: { agent: "gg", owner: "oo", protocol_version: "0.3", region: "eu-1" },
            endpoints: [
              {
                name: "prompt",
                subject: "agents.prompt.gg.oo.n1",
                metadata: { max_payload: "1MB", attachments_ok: true },
              },
              { name: "future-endpoint", subject: "agents.future.gg.oo.n1" },
            ],
          }),
        ])
        yield* TestClock.adjust(1)
        const client = yield* AgentClient
        const fiber = yield* forkNow(client.discover())
        yield* TestClock.adjust(5_000)
        const found = yield* Fiber.join(fiber)
        expect(found).toHaveLength(1)
        expect(found[0]!.metadata["region"]).toBe("eu-1")
        expect(found[0]!.endpoints.map((e) => e.name)).toEqual(["prompt", "future-endpoint"])
      }),
    )
  })

  test("§4.3/§12 the client publishes to the advertised subject verbatim — no construction", async () => {
    await withClientOnly((bus) =>
      Effect.gen(function* () {
        yield* respondOn(bus, "$SRV.INFO.agents", [
          record({
            endpoints: [
              {
                name: "prompt",
                subject: "custom.tree.x",
                metadata: { max_payload: "1MB", attachments_ok: true },
              },
            ],
          }),
        ])
        yield* respondOn(bus, "custom.tree.x", [
          enc(JSON.stringify({ type: "status", data: "ack" })),
          EMPTY,
        ])
        yield* TestClock.adjust(1)
        const client = yield* AgentClient
        const fiber = yield* forkNow(client.discover())
        yield* TestClock.adjust(5_000)
        const agentInfo = (yield* Fiber.join(fiber))[0]!
        const run = yield* forkNow(Stream.runCollect(client.prompt(agentInfo, "ping")))
        yield* TestClock.adjust(1_000)
        const chunks = yield* Fiber.join(run)
        expect(chunks.map((c) => c._tag)).toEqual(["Status"])
        const requests = bus.log.filter((e) => e.subject === "custom.tree.x")
        expect(requests).toHaveLength(1)
        const parsed = parseRequest(requests[0]!.payload)
        expect(!isRequestError(parsed) && parsed.prompt).toBe("ping")
        expect(bus.log.some((e) => e.subject.startsWith("agents.prompt"))).toBe(false)
      }),
    )
  })
})

describe("§7.2 query replies", () => {
  test("answerQuery publishes exactly one §5.1-shaped payload to reply_subject", async () => {
    await withClientOnly((bus) =>
      Effect.gen(function* () {
        const client = yield* AgentClient
        yield* client.answerQuery({ replySubject: "_INBOX.q77" }, "yes")
        const published = bus.log.filter((e) => e.subject === "_INBOX.q77")
        expect(published).toHaveLength(1)
        const parsed = parseRequest(published[0]!.payload)
        expect(isRequestError(parsed)).toBe(false)
        if (!isRequestError(parsed)) {
          expect(parsed.prompt).toBe("yes")
          expect(parsed.attachments).toEqual([])
        }
      }),
    )
  })
})
