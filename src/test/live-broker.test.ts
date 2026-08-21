/**
 * Live-broker integration: the real nats-wire interpreter against a real
 * nats-server (skipped when the binary is absent). Real clock, tight
 * windows — everything here exercises paths the loopback bus cannot:
 * socket-level subscribe-before-publish, no-responders, genuine inactivity
 * timeout, queue-group balancing, and cancellation by unsubscribe.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { connect as netConnect } from "node:net"
import { Effect, Exit, Fiber, Layer, Stream } from "effect"
import { AgentClient, layerClient } from "../agents/client.js"
import { layerHost, type AgentHostConfig } from "../agents/host.js"
import { NatsConnection } from "../agents/nats-connection.js"
import { layerLive } from "../agents/nats-wire.js"
import { promptStream } from "../agents/prompt-engine.js"
import { Wire } from "../agents/wire.js"

const NATS_BIN = Bun.which("nats-server")
const PORT = 24000 + Math.floor(Math.random() * 1000)

const waitForPort = (port: number, timeoutMs = 10_000): Promise<void> =>
  new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs
    const attempt = () => {
      const socket = netConnect({ host: "127.0.0.1", port }, () => {
        socket.end()
        resolve()
      })
      socket.on("error", () => {
        socket.destroy()
        if (Date.now() > deadline) reject(new Error(`nats-server not up on :${port}`))
        else setTimeout(attempt, 100)
      })
    }
    attempt()
  })

let server: ReturnType<typeof Bun.spawn> | undefined

beforeAll(async () => {
  if (NATS_BIN === null) return
  server = Bun.spawn([NATS_BIN, "-a", "127.0.0.1", "-p", String(PORT)], {
    stdout: "ignore",
    stderr: "ignore",
  })
  await waitForPort(PORT)
})

afterAll(() => {
  server?.kill()
})

const connLayer = NatsConnection.layer({ servers: [`127.0.0.1:${PORT}`] })
const wireLayer = layerLive.pipe(Layer.provide(connLayer))

const stack = (config: AgentHostConfig) =>
  Layer.mergeAll(
    layerHost(config).pipe(Layer.provide(wireLayer)),
    layerClient().pipe(Layer.provide(wireLayer)),
    wireLayer,
  )

const hostConfig = (
  name: string,
  overrides?: Partial<AgentHostConfig>,
): AgentHostConfig => ({
  identity: { agent: "cc", owner: "live", name },
  heartbeatIntervalS: 1,
  handler: (req) => Stream.make({ _tag: "Text", text: `echo:${req.prompt}` } as const),
  ...overrides,
})

const run = <A, E>(
  config: AgentHostConfig,
  body: Effect.Effect<A, E, AgentClient | Wire>,
): Promise<A> => Effect.runPromise(Effect.provide(body, stack(config)))

const discoverOne = (name: string) =>
  Effect.gen(function* () {
    const client = yield* AgentClient
    const found = yield* client.discover({ quietMs: 400, deadlineMs: 4_000 })
    const identity = found.find((i) =>
      i.endpoints.some((e) => e.subject.endsWith(`.${name}`)),
    )
    if (identity === undefined) throw new Error(`host '${name}' not discovered`)
    return identity
  })

describe.skipIf(NATS_BIN === null)("live broker (nats-server)", () => {
  test("discovery finds the host and a prompt round-trips end to end", async () => {
    await run(hostConfig("rt"), Effect.gen(function* () {
      const client = yield* AgentClient
      const identity = yield* discoverOne("rt")
      expect(identity.agent).toBe("cc")
      expect(identity.protocolVersion).toBe("0.3")
      const chunks = yield* Stream.runCollect(client.prompt(identity, "ping"))
      expect(chunks.map((c) => c._tag)).toEqual(["Status", "Response"])
      expect(chunks[1]?._tag === "Response" && chunks[1].text).toBe("echo:ping")
    }))
  }, 15_000)

  test("malformed request gets a 400 over the real wire, DFA intact", async () => {
    await run(hostConfig("bad"), Effect.gen(function* () {
      const identity = yield* discoverOne("bad")
      const subject = identity.endpoints.find((e) => e.name === "prompt")!.subject
      const error = yield* Effect.flip(
        Stream.runCollect(promptStream({ subject, payload: new Uint8Array(0) })),
      )
      expect(error._tag).toBe("PromptFailed")
      expect(error._tag === "PromptFailed" && error.code).toBe(400)
    }))
  }, 15_000)

  test("second concurrent prompt is rejected 429 while the first completes", async () => {
    const config = hostConfig("busy", {
      maxInFlight: 1,
      handler: (req) =>
        Stream.fromIterable([{ _tag: "Text", text: `done:${req.prompt}` } as const]).pipe(
          Stream.mapEffect((c) => Effect.as(Effect.sleep(500), c)),
        ),
    })
    await run(config, Effect.gen(function* () {
      const client = yield* AgentClient
      const identity = yield* discoverOne("busy")
      const attempt = (text: string) =>
        Stream.runCollect(client.prompt(identity, text)).pipe(
          Effect.map(() => "ok"),
          Effect.catchTag("PromptFailed", (e) => Effect.succeed(`fail:${e.code}`)),
        )
      const outcomes = yield* Effect.all(
        [attempt("one"), Effect.flatMap(Effect.sleep(100), () => attempt("two"))],
        { concurrency: 2 },
      )
      expect([...outcomes].sort()).toEqual(["fail:429", "ok"])
    }))
  }, 20_000)

  test("no responders: the server's 503 fails fast as a transport error", async () => {
    await run(hostConfig("idle"), Effect.gen(function* () {
      const started = Date.now()
      const error = yield* Effect.flip(
        Stream.runCollect(
          promptStream({
            subject: "agents.prompt.ghost.nobody.here",
            payload: new TextEncoder().encode("hello?"),
            inactivityMs: 5_000,
          }),
        ),
      )
      expect(error._tag).toBe("TransportFailed")
      expect(Date.now() - started).toBeLessThan(1_000)
    }))
  }, 15_000)

  test("lost terminator: ack then silence fires StreamTimeout at the window", async () => {
    const config = hostConfig("silent", {
      handler: () =>
        Stream.fromIterable([{ _tag: "Text", text: "never" } as const]).pipe(
          Stream.mapEffect((c) => Effect.as(Effect.sleep(30_000), c)),
        ),
    })
    await run(config, Effect.gen(function* () {
      const client = yield* AgentClient
      const identity = yield* discoverOne("silent")
      const started = Date.now()
      const error = yield* Effect.flip(
        Stream.runCollect(client.prompt(identity, "speak", { inactivityMs: 700 })),
      )
      expect(error._tag).toBe("StreamTimeout")
      const elapsed = Date.now() - started
      expect(elapsed).toBeGreaterThanOrEqual(650)
      expect(elapsed).toBeLessThan(5_000)
    }))
  }, 15_000)

  test("heartbeats arrive live and drive the liveness tracker", async () => {
    await run(hostConfig("hb"), Effect.gen(function* () {
      const client = yield* AgentClient
      const beat = yield* Stream.runCollect(Stream.take(client.heartbeats(), 1)).pipe(
        Effect.timeout(5_000),
      )
      expect(beat[0]?.agent).toBe("cc")
      expect(beat[0]?.interval_s).toBe(1)
      expect(yield* client.isLive(beat[0]!.instance_id)).toBe(true)
    }))
  }, 15_000)

  test("interrupting a prompt mid-stream cancels cleanly by unsubscribe", async () => {
    const config = hostConfig("cancel", {
      handler: () =>
        Stream.fromIterable(
          Array.from({ length: 100 }, (_, i) => ({ _tag: "Text", text: `c${i}` }) as const),
        ).pipe(Stream.mapEffect((c) => Effect.as(Effect.sleep(50), c))),
    })
    await run(config, Effect.gen(function* () {
      const client = yield* AgentClient
      const identity = yield* discoverOne("cancel")
      const fiber = yield* Effect.forkChild(
        Stream.runCollect(client.prompt(identity, "long")),
        { startImmediately: true },
      )
      yield* Effect.sleep(200)
      const started = Date.now()
      yield* Fiber.interrupt(fiber)
      expect(Date.now() - started).toBeLessThan(1_000)
    }))
  }, 15_000)

  test("two same-identity hosts load-balance via the queue group", async () => {
    const second = layerHost(
      hostConfig("lb", {
        instanceId: "lb-b",
        handler: (req) => Stream.make({ _tag: "Text", text: `b:${req.prompt}` } as const),
      }),
    ).pipe(Layer.provide(wireLayer))
    const config = hostConfig("lb", {
      instanceId: "lb-a",
      handler: (req) => Stream.make({ _tag: "Text", text: `a:${req.prompt}` } as const),
    })
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* AgentClient
        const identity = yield* discoverOne("lb")
        const results: string[] = []
        for (let i = 0; i < 12; i++) {
          const chunks = yield* Stream.runCollect(client.prompt(identity, `m${i}`))
          const texts = chunks.filter((c) => c._tag === "Response")
          expect(texts).toHaveLength(1)
          const text = (texts[0] as { text: string }).text
          expect(text).toMatch(new RegExp(`^(a|b):m${i}$`))
          results.push(text)
        }
        const members = new Set(results.map((t) => t[0]))
        expect(members.size).toBe(2)
      }).pipe(Effect.provide(Layer.mergeAll(stack(config), second))),
    )
  }, 20_000)
})
