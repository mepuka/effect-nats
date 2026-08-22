/**
 * Demo: reactive cells over the simulated KV.
 * A cell is one schema-typed KV message; updates are CAS'd and invalidate
 * reactively. This is the whole user-facing surface.
 */
import { Effect, Fiber, Layer, Schema, Stream } from "effect"
import { Reactivity } from "effect/unstable/reactivity"
import { make as makeCell } from "./reactive/cell.js"
import { Nats } from "./nats/service.js"

const Counter = Schema.Number

const program = Effect.gen(function* () {
  const visits = yield* makeCell({
    key: "app.visits",
    schema: Counter,
    initialValue: 0,
  })


  yield* visits.update((n) => n + 1).pipe(
    Effect.tap(() => Effect.log("saw: " + visits.get)),
  )
  yield* visits.update((n) => n + 1).pipe(
    Effect.tap(() => Effect.log("saw: " + visits.get)),
  ) 
  yield* visits.update((n) => n * 10).pipe(
    Effect.tap(() => Effect.log("saw: " + visits.get)),
  )

  const final = yield* visits.get

  return { final, }
})

Effect.runPromise(
  Effect.provide(program, Layer.mergeAll(Nats.layer, Reactivity.layer)),
).then(console.log)
