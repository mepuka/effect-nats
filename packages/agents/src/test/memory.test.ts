/**
 * Memory-behavior tests: bounded liveness tracking, subscription cleanup
 * across prompt lifecycles, and heap stability under sustained load.
 */
import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Layer, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AgentClient, layerClient } from "../agents/client.js"
import { layerHost, type AgentHostConfig } from "../agents/host.js"
import { Wire } from "../agents/wire.js"
import { makeLoopbackBus, type LoopbackBus } from "./kit.js"

const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const ADDRESS = { agent: "cc", owner: "own", name: "main" }
const PROMPT_SUBJECT = "agents.prompt.cc.own.main"

const hostConfig = (overrides?: Partial<AgentHostConfig>): AgentHostConfig => ({
  address: ADDRESS,
  instanceId: "inst-m",
  handler: (req) => Stream.make({ _tag: "Text", text: `echo:${req.prompt}` } as const),
  ...overrides,
})

const promptIdentity = {
  agent: "cc",
  owner: "own",
  instanceId: "inst-m",
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

const beatBytes = (instanceId: string, intervalS: number): Uint8Array =>
  new TextEncoder().encode(
    JSON.stringify({
      agent: "x",
      owner: "own",
      instance_id: instanceId,
      ts: "2026-01-01T00:00:00Z",
      interval_s: intervalS,
    }),
  )

describe("subscription hygiene", () => {
  test("completed and interrupted prompts leave zero extra subscriptions", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        yield* Effect.gen(function* () {
          yield* TestClock.adjust(1)
          const client = yield* AgentClient
          const baseline = bus.subscriptionCount()

          for (let i = 0; i < 25; i++) {
            const run = yield* forkNow(
              Stream.runCollect(client.prompt(promptIdentity, `p${i}`)),
            )
            yield* TestClock.adjust(10)
            yield* Fiber.join(run)
          }
          expect(bus.subscriptionCount()).toBe(baseline)

          const slow = yield* forkNow(
            Stream.runCollect(client.prompt(promptIdentity, "hang")),
          )
          yield* TestClock.adjust(1)
          yield* Fiber.interrupt(slow)
          expect(bus.subscriptionCount()).toBe(baseline)
        }).pipe(
          Effect.provide(
            layersFor(
              bus,
              hostConfig({
                handler: (req) =>
                  req.prompt === "hang"
                    ? Stream.fromIterable([{ _tag: "Text", text: "late" } as const]).pipe(
                        Stream.mapEffect((c) => Effect.as(Effect.sleep(60_000), c)),
                      )
                    : Stream.make({ _tag: "Text", text: "ok" } as const),
              }),
            ),
          ),
        )
      }).pipe(Effect.provide(TestClock.layer())),
    )
  })
})

const heapUsedAfterGc = (): number => {
  Bun.gc(true)
  return process.memoryUsage().heapUsed
}

const awaitQuiescentSubscriptions = (bus: LoopbackBus): Effect.Effect<void> =>
  Effect.gen(function* () {
    let last = -1
    let stable = 0
    while (stable < 3) {
      yield* Effect.sleep(5)
      const now = bus.subscriptionCount()
      stable = now === last ? stable + 1 : 0
      last = now
    }
  })

const HEAP_HEADROOM_BYTES = 25 * 1024 * 1024

describe("heap stability (real clock)", () => {
  test("500 prompt round-trips do not grow the heap", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        yield* Effect.gen(function* () {
          const client = yield* AgentClient
          yield* awaitQuiescentSubscriptions(bus)
          const roundTrip = (i: number) =>
            Stream.runCollect(client.prompt(promptIdentity, `p${i}`))
          for (let i = 0; i < 50; i++) yield* roundTrip(i)
          bus.log.length = 0
          const before = heapUsedAfterGc()
          for (let i = 0; i < 500; i++) yield* roundTrip(i)
          bus.log.length = 0
          const grown = heapUsedAfterGc() - before
          expect(grown).toBeLessThan(HEAP_HEADROOM_BYTES)
          expect(bus.subscriptionCount()).toBeLessThan(20)
        }).pipe(Effect.provide(layersFor(bus, hostConfig())))
      }),
    )
  }, 30_000)

  test("10k heartbeats with an idle subscriber stay bounded", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        yield* Effect.gen(function* () {
          const client = yield* AgentClient
          yield* awaitQuiescentSubscriptions(bus)
          yield* forkNow(
            Stream.runDrain(
              client.heartbeats().pipe(Stream.mapEffect(() => Effect.never)),
            ),
          )
          for (let i = 0; i < 500; i++) {
            yield* bus.wire.publish(`agents.hb.x.own.n${i % 20}`, beatBytes(`inst-${i % 20}`, 30))
          }
          bus.log.length = 0
          const before = heapUsedAfterGc()
          for (let i = 0; i < 10_000; i++) {
            yield* bus.wire.publish(`agents.hb.x.own.n${i % 20}`, beatBytes(`inst-${i % 20}`, 30))
          }
          yield* Effect.sleep(20)
          bus.log.length = 0
          const grown = heapUsedAfterGc() - before
          expect(grown).toBeLessThan(HEAP_HEADROOM_BYTES)
          const map = yield* client.liveness()
          expect(map.size).toBeLessThanOrEqual(21)
        }).pipe(Effect.provide(layersFor(bus, hostConfig())))
      }),
    )
  })
})

describe("bounded liveness tracking", () => {
  test("absurd declared intervals cannot pin entries; capacity evicts stalest", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        const wireLayer = Layer.succeed(Wire)(bus.wire)
        const layer = Layer.mergeAll(
          layerClient({ livenessCapacity: 3 }).pipe(Layer.provide(wireLayer)),
          wireLayer,
        )
        yield* Effect.gen(function* () {
          yield* TestClock.adjust(1)
          const client = yield* AgentClient
          yield* bus.wire.publish("agents.hb.x.own.pin", beatBytes("pinned", 1e9))
          yield* TestClock.adjust(1)
          expect((yield* client.liveness()).has("pinned")).toBe(true)
          // 10 × clamped-1h horizon passes with fresh beats arriving
          yield* TestClock.adjust(37_000_000)
          yield* bus.wire.publish("agents.hb.x.own.f", beatBytes("fresh", 30))
          yield* TestClock.adjust(1)
          expect((yield* client.liveness()).has("pinned")).toBe(false)

          for (let i = 0; i < 6; i++) {
            yield* bus.wire.publish(`agents.hb.x.own.c${i}`, beatBytes(`cap-${i}`, 30))
            yield* TestClock.adjust(1)
          }
          const map = yield* client.liveness()
          expect(map.size).toBeLessThanOrEqual(3)
          expect(map.has("cap-5")).toBe(true)
        }).pipe(Effect.provide(layer))
      }).pipe(Effect.provide(TestClock.layer())),
    )
  })

  test("instances silent for many intervals are pruned from the map", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const bus = yield* makeLoopbackBus()
        yield* Effect.gen(function* () {
          yield* TestClock.adjust(1)
          const client = yield* AgentClient
          yield* bus.wire.publish("agents.hb.x.own.gone", beatBytes("gone-inst", 10))
          yield* TestClock.adjust(1)
          expect((yield* client.liveness()).has("gone-inst")).toBe(true)
          yield* TestClock.adjust(500_000)
          yield* bus.wire.publish("agents.hb.x.own.fresh", beatBytes("fresh-inst", 10))
          yield* TestClock.adjust(1)
          const map = yield* client.liveness()
          expect(map.has("fresh-inst")).toBe(true)
          expect(map.has("gone-inst")).toBe(false)
        }).pipe(Effect.provide(layersFor(bus, hostConfig())))
      }).pipe(Effect.provide(TestClock.layer())),
    )
  })
})
