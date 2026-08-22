import { Effect, Stream } from "effect"
import type { Connection } from "../src/internal/NatsTransport.ts"
import { make as makeStatusBridge } from "../src/internal/StatusBridge.ts"

// ts-ignore-next-line
import process from "node:process"

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

const bun = (globalThis as Record<string, unknown>).Bun as { gc: (f: boolean) => void }

const cycle = Effect.scoped(
  Effect.gen(function*() {
    const { events } = yield* makeStatusBridge({
      connection: infiniteStatusConnection(),
      capacity: 64
    })
    yield* Stream.runDrain(Stream.take(events, 4))
  })
)

const batches = 30
const perBatch = 100
const samples: Array<number> = []
for (let b = 0; b < batches; b++) {
  for (let i = 0; i < perBatch; i++) await Effect.runPromise(cycle)
  // batch-gc removed
  samples.push(process.memoryUsage().heapUsed)
}
const base = samples[0]
console.log("per-batch heapUsed deltas (MB):")
for (let b = 1; b < batches; b++) {
  const d = (samples[b] - samples[b - 1]) / 1024 / 1024
  process.stdout.write(`${d.toFixed(2)} `)
}
console.log()
console.log(
  `total drift over ${batches} batches: ${
    ((samples[batches - 1] - base) / 1024 / 1024).toFixed(2)
  }MB`
)
