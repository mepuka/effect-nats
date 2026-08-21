/**
 * P3 — The durable uplink boundary.
 *
 * The topology fact this encodes: the docs promise no store-and-forward for
 * core NATS across a down leaf link. So `durable` means "landed in the
 * edge-local stream" (local PubAck; the cloud stream sources it later), and
 * `bestEffort` means "core publish across the link" — and the two carry
 * different delivery grades in `R`, so a boundary can *refuse* fire-and-forget
 * at compile time.
 *
 * The in-memory layer simulates exactly that: an edge spool, a replicator
 * fiber that drains it cloudward on an interval, a dedup set (infinite
 * window — same named divergence as the Lean model), and lag as a Metric.
 */
import { Context, Duration, Effect, Layer, Metric, Ref, Schedule } from "effect"
import type { AtLeastOnce, AtMostOnce } from "../sketch/graded"
import type { SiteId } from "./Model"
import { UplinkDown } from "./Model"

export interface UplinkMessage {
  readonly subject: string
  readonly msgId: string
  readonly payload: unknown
  readonly seq: number
}

export interface UplinkApi {
  /** Lands durably in the edge stream; idempotent per msgId (dedup window). */
  readonly durable: (
    subject: string,
    payload: unknown,
    msgId: string
  ) => Effect.Effect<number, UplinkDown, AtLeastOnce>
  /** Core publish across the link — gone if the link is down. The grade says so. */
  readonly bestEffort: (subject: string, payload: unknown) => Effect.Effect<void, never, AtMostOnce>
}

export class Uplink extends Context.Service<Uplink, UplinkApi>()("mesh/Uplink") {}

export interface UplinkProbeApi {
  readonly edgeSpool: Effect.Effect<ReadonlyArray<UplinkMessage>>
  readonly cloud: Effect.Effect<ReadonlyArray<UplinkMessage>>
  readonly lag: Effect.Effect<number>
}

/** Test/ops visibility into the simulated topology. */
export class UplinkProbe extends Context.Service<UplinkProbe, UplinkProbeApi>()("mesh/UplinkProbe") {}

/** Source lag is the RPO — first-class, monitored, never an afterthought. */
export const uplinkLag = Metric.gauge("mesh_uplink_lag")

export const layerUplinkMemory = (options: {
  readonly site: SiteId
  readonly replicationInterval?: Duration.Input
  readonly spoolCapacity?: number
}): Layer.Layer<Uplink | UplinkProbe> =>
  Layer.effectContext(
    Effect.gen(function*() {
      const spoolCapacity = options.spoolCapacity ?? 10_000
      const edge = yield* Ref.make<ReadonlyArray<UplinkMessage>>([])
      const cloud = yield* Ref.make<ReadonlyArray<UplinkMessage>>([])
      const replicatedTo = yield* Ref.make(0)
      const seen = yield* Ref.make<ReadonlyMap<string, number>>(new Map())
      const lost = yield* Ref.make(0)

      const durable = (subject: string, payload: unknown, msgId: string) =>
        Effect.gen(function*() {
          const dedup = yield* Ref.get(seen)
          const existing = dedup.get(msgId)
          if (existing !== undefined) return existing
          const log = yield* Ref.get(edge)
          if (log.length >= spoolCapacity) {
            return yield* Effect.fail(new UplinkDown({ site: options.site }))
          }
          const seq = log.length + 1
          yield* Ref.set(edge, [...log, { subject, msgId, payload, seq }])
          yield* Ref.update(seen, (m) => new Map(m).set(msgId, seq))
          return seq
        })

      const bestEffort = (_subject: string, _payload: unknown) =>
        Ref.update(lost, (n) => n) // a live link would send; a down link drops. Either way: no receipt.

      const replicate = Effect.gen(function*() {
        const log = yield* Ref.get(edge)
        const done = yield* Ref.get(replicatedTo)
        if (log.length > done) {
          yield* Ref.update(cloud, (c) => [...c, ...log.slice(done)])
          yield* Ref.set(replicatedTo, log.length)
        }
        yield* Metric.update(uplinkLag, log.length - Math.min(log.length, done))
      })

      yield* replicate.pipe(
        Effect.repeat(Schedule.spaced(options.replicationInterval ?? "1 second")),
        Effect.forkScoped
      )

      const uplink: UplinkApi = { durable, bestEffort }
      const probe: UplinkProbeApi = {
        edgeSpool: Ref.get(edge),
        cloud: Ref.get(cloud),
        lag: Effect.gen(function*() {
          return (yield* Ref.get(edge)).length - (yield* Ref.get(replicatedTo))
        })
      }
      return Context.make(Uplink, uplink).pipe(Context.add(UplinkProbe, probe))
    })
  )
