/**
 * P4 — Telemetry firehose.
 *
 * Sensor readings enter a bounded suspend queue (backpressure is a named
 * choice, never an unbounded default), are batched by count-or-window, get a
 * monotone batch sequence for the dedup id, and cross the uplink durably.
 *
 * The delivery-grade machinery earns its keep here: the pipeline is gated
 * `requireFloor<AtLeastOnce>` — if anyone routes a batch through a
 * fire-and-forget path, this module stops compiling — and only then is the
 * grade attested away at the boundary it guards.
 */
import { Context, Duration, Effect, Layer, Queue, Ref, Stream } from "effect"
import type { AtLeastOnce, Grade } from "../sketch/graded"
import { attest, requireFloor } from "../sketch/graded"
import type { TelemetryBatch, TelemetryPoint } from "./Model"
import { Uplink } from "./Uplink"

export interface TelemetryApi {
  readonly record: (point: TelemetryPoint) => Effect.Effect<void>
}

export class Telemetry extends Context.Service<Telemetry, TelemetryApi>()("mesh/Telemetry") {}

export const layerTelemetry = (options: {
  readonly batchSize: number
  readonly window: Duration.Input
  readonly queueCapacity: number
}): Layer.Layer<Telemetry, never, Uplink> =>
  Layer.effect(
    Telemetry,
    Effect.gen(function*() {
      const uplink = yield* Uplink
      const queue = yield* Queue.make<TelemetryPoint>({
        capacity: options.queueCapacity,
        strategy: "suspend"
      })
      const batchSeq = yield* Ref.make(0)

      const drain = Stream.fromQueue(queue).pipe(
        Stream.groupedWithin(options.batchSize, options.window),
        Stream.mapEffect((points) =>
          Effect.gen(function*() {
            const head = points[0]
            if (head === undefined) return
            const seq = yield* Ref.getAndUpdate(batchSeq, (n) => n + 1)
            const batch: TelemetryBatch = { device: head.device, batchSeq: seq, points }
            yield* uplink.durable("telemetry.batch", batch, `${head.device}:${seq}`)
          })
        ),
        Stream.runDrain
      )

      // Compile-time proof that nothing below at-least-once crosses this
      // boundary, then discharge the grades — this fiber IS the boundary.
      yield* attest<Grade>()(requireFloor<AtLeastOnce>()(drain)).pipe(Effect.forkScoped)

      const api: TelemetryApi = {
        record: (point) => Effect.asVoid(Queue.offer(queue, point))
      }
      return api
    })
  )
