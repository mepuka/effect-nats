/**
 * Performance and memory sanity suite for the transport slice.
 *
 * These are informational measurements with deliberately loose assertions so
 * they stay stable on CI hardware; the printed numbers are the real output.
 * They use wall time by nature, so they live apart from the deterministic
 * correctness suites. Memory checks warm up before baselining and force a
 * double GC before sampling, because first-run JIT dominates naive deltas.
 */
import { expect, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Stream } from "effect"
import {
  type Connection,
  layerConnecting,
  layerTransport,
  NatsTransport
} from "../../src/internal/NatsTransport.ts"
import { nodeOfficialConnect } from "../../src/internal/NodeOfficialConnect.ts"
import { OfficialConnect } from "../../src/internal/OfficialConnect.ts"
import { make as makeStatusBridge } from "../../src/internal/StatusBridge.ts"
import { make as makeSubscriptionBridge } from "../../src/internal/SubscriptionBridge.ts"

const bun = (globalThis as Record<string, unknown>).Bun as
  | { gc: (force: boolean) => void }
  | undefined
const proc = (globalThis as Record<string, unknown>).process as
  | { memoryUsage: () => { heapUsed: number } }
  | undefined

/** Forced double GC, then sampled JS heap. */
const heapUsed = (): number => {
  bun?.gc(true)
  bun?.gc(true)
  return proc?.memoryUsage().heapUsed ?? 0
}

const mb = (bytes: number): string => `${(bytes / 1024 / 1024).toFixed(2)}MB`

const measure = async <A>(fn: () => Promise<A>): Promise<{ out: A; ms: number }> => {
  const t0 = performance.now()
  const out = await fn()
  return { out, ms: performance.now() - t0 }
}

/** Pull-driven connection fake: next() resolves immediately forever. */
const infiniteStatusConnection = (): Connection => {
  let i = 0
  return {
    closed: () => Promise.resolve(undefined),
    close: () => Promise.resolve(),
    drain: () => Promise.resolve(),
    flush: () => Promise.resolve(),
    isClosed: () => false,
    isDraining: () => false,
    status: () => ({
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.resolve({ value: { type: `s${i++}` }, done: false }),
        return: () => Promise.resolve({ value: undefined, done: true })
      })
    })
  }
}

const resolvingConnection = (): Connection => ({
  ...infiniteStatusConnection(),
  status: () => ({
    [Symbol.asyncIterator]: () => ({
      next: () => Promise.resolve({ value: undefined, done: true }),
      return: () => Promise.resolve({ value: undefined, done: true })
    })
  })
})

it("SubscriptionBridge admits and drains 200k values", async () => {
  const n = 200_000
  // Capacity above n removes scheduler races from the measurement: the run
  // then reports max sustainable end-to-end throughput.
  const program = Effect.gen(function*() {
    const bridge = yield* makeSubscriptionBridge<{ i: number }>({
      operation: "perf",
      subject: "perf",
      capacity: n + 1
    })
    const collector = yield* Stream.runCollect(Stream.take(bridge.stream, n)).pipe(
      Effect.forkChild
    )
    for (let i = 0; i < n; i++) {
      yield* bridge.offer({ i })
    }
    return yield* Fiber.join(collector)
  })
  const { out, ms } = await measure(() => Effect.runPromise(program))
  expect(out.length).toBe(n)
  console.log(
    `bridge throughput: ${n} values in ${ms.toFixed(1)}ms (${Math.round(n / ms * 1000)}/s)`
  )
})

it("SubscriptionBridge overflow terminates fast and does not grow memory", async () => {
  const n = 200_000
  const program = Effect.gen(function*() {
    const bridge = yield* makeSubscriptionBridge<unknown>({
      operation: "perf-flood",
      subject: "perf-flood",
      capacity: 1024
    })
    let admitted = 0
    for (let i = 0; i < n; i++) {
      const outcome = yield* Effect.result(bridge.offer(i))
      if (outcome._tag === "Success") admitted++
    }
    return admitted
  })
  const { out, ms } = await measure(() => Effect.runPromise(program))
  expect(out).toBe(1024)
  console.log(`overflow flood: ${n} offers in ${ms.toFixed(1)}ms (${Math.round(n / ms * 1000)}/s)`)
})

it("StatusBridge pipelines 100k events end to end", async () => {
  const n = 100_000
  const program = Effect.scoped(
    Effect.gen(function*() {
      const { events } = yield* makeStatusBridge({
        connection: infiniteStatusConnection(),
        capacity: n + 1
      })
      const collector = yield* Stream.runCollect(Stream.take(events, n)).pipe(
        Effect.forkChild
      )
      return yield* Fiber.join(collector)
    })
  )
  const { out, ms } = await measure(() => Effect.runPromise(program))
  expect(out.length).toBe(n)
  console.log(`status pipeline: ${n} events in ${ms.toFixed(1)}ms (${Math.round(n / ms * 1000)}/s)`)
})

it("Transport flush round-trips 20k times through the service path", async () => {
  const n = 20_000
  const program = Effect.scoped(
    NatsTransport.use((transport) =>
      Effect.gen(function*() {
        for (let i = 0; i < n; i++) {
          yield* transport.flush()
        }
      })
    ).pipe(Effect.provide(layerTransport(resolvingConnection())))
  )
  const { ms } = await measure(() => Effect.runPromise(program))
  console.log(`flush round-trips: ${n} in ${ms.toFixed(1)}ms (${Math.round(n / ms * 1000)}/s)`)
})

it("Supervised acquire+release cycles hold steady across 2k iterations", async () => {
  const cycles = 2_000
  const succeedLayer = Layer.succeed(OfficialConnect, {
    connect: () => Effect.succeed(infiniteStatusConnection())
  })
  const program = Effect.gen(function*() {
    let done = 0
    while (done < cycles) {
      yield* Effect.scoped(
        NatsTransport.use(() => Effect.void).pipe(
          Effect.provide(Layer.provide(layerConnecting({ servers: ["nats://perf"] }), succeedLayer))
        )
      )
      done++
    }
    return done
  })
  const { out, ms } = await measure(() => Effect.runPromise(program))
  expect(out).toBe(cycles)
  console.log(
    `supervised cycles: ${cycles} acquire/release in ${ms.toFixed(1)}ms (${
      (ms / cycles).toFixed(3)
    }ms each)`
  )
})

it("Real refused dials stay cheap and classified", async () => {
  const attempts = 30
  const durations: Array<number> = []
  for (let i = 0; i < attempts; i++) {
    const { ms } = await measure(() =>
      Effect.runPromise(
        Effect.result(nodeOfficialConnect.connect({ servers: ["nats://127.0.0.1:1"] }))
      )
    )
    durations.push(ms)
  }
  const avg = durations.reduce((a, b) => a + b, 0) / attempts
  const max = Math.max(...durations)
  console.log(
    `refused dial: avg ${avg.toFixed(1)}ms, max ${max.toFixed(1)}ms over ${attempts} attempts`
  )
  expect(avg).toBeLessThan(300)
})

it(
  "Repeated scoped lifecycles stay flat (informational; gated by scripts/leak-check.ts)",
  async () => {
    const cycle = Effect.scoped(
      Effect.gen(function*() {
        const { events } = yield* makeStatusBridge({
          connection: infiniteStatusConnection(),
          capacity: 64
        })
        yield* Stream.runDrain(Stream.take(events, 4))
      })
    )

    // Warmup absorbs JIT and allocator ramp-up before the baseline samples.
    for (let i = 0; i < 300; i++) await Effect.runPromise(cycle)
    const before = heapUsed()

    const cycles = 2_000
    for (let i = 0; i < cycles; i++) await Effect.runPromise(cycle)

    const growth = heapUsed() - before
    console.log(
      `lifecycle leak check: ${cycles} open/close cycles after warmup, growth ${mb(growth)}`
    )
  },
  30_000
)
