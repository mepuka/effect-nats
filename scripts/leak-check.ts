/**
 * Enforceable memory-stability gate for the transport lifecycle, run under
 * plain Bun (no vitest worker noise). Exits non-zero when retained JS heap
 * grows beyond the budget after warmup.
 */
import { Effect, Stream } from "effect"
import type { Connection } from "../src/internal/NatsTransport.ts"
import { make as makeStatusBridge } from "../src/internal/StatusBridge.ts"

const bun = (globalThis as Record<string, unknown>).Bun as { gc: (force: boolean) => void }
const proc = (globalThis as Record<string, unknown>).process as {
  memoryUsage: () => { heapUsed: number }
}

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

const cycle = Effect.scoped(
  Effect.gen(function*() {
    const { events } = yield* makeStatusBridge({
      connection: infiniteStatusConnection(),
      capacity: 64
    })
    yield* Stream.runDrain(Stream.take(events, 4))
  })
)

const sample = (): number => {
  bun.gc(true)
  bun.gc(true)
  return proc.memoryUsage().heapUsed
}

const mb = (bytes: number): string => `${(bytes / 1024 / 1024).toFixed(2)}MB`

const warmup = 300
const measured = 2_000
for (let i = 0; i < warmup; i++) await Effect.runPromise(cycle)
const before = sample()
for (let i = 0; i < measured; i++) await Effect.runPromise(cycle)
const after = sample()
const growth = after - before

console.log(
  `leak-check: ${measured} open/close cycles after ${warmup} warmup cycles, growth ${mb(growth)}`
)
if (growth > 5 * 1024 * 1024) {
  console.error(`leak-check FAILED: growth exceeds 5MB budget`)
  process.exit(1)
}
console.log("leak-check PASSED")
