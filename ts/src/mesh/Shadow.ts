/**
 * P2 — Two-bucket device shadow (digital twin).
 *
 * Desired state is written by the cloud, reported state by the device; each
 * side CASes on the bucket it owns (KV semantics: create = revision 0,
 * update = expected revision, mismatch rejects). Watch mirrors KV watch:
 * current value first, then updates — which is exactly
 * `SubscriptionRef.changes` semantics, so the in-memory layer and the future
 * NATS KV layer share an observable contract.
 */
import { Context, Effect, Layer, Result, Stream, SubscriptionRef } from "effect"
import type { DeviceId, FirmwareVersion, ShadowDesired, ShadowReported } from "./Model"
import { ShadowConflict } from "./Model"

export interface Versioned<A> {
  readonly value: A
  readonly revision: number
}

export class ShadowStore extends Context.Service<ShadowStore, {
  readonly getDesired: (device: DeviceId) => Effect.Effect<Versioned<ShadowDesired> | undefined>
  readonly putDesired: (
    device: DeviceId,
    value: ShadowDesired,
    expectedRevision: number
  ) => Effect.Effect<number, ShadowConflict>
  /** current value first, then every change — KV-watch shaped */
  readonly watchDesired: (device: DeviceId) => Stream.Stream<Versioned<ShadowDesired>>
  readonly getReported: (device: DeviceId) => Effect.Effect<Versioned<ShadowReported> | undefined>
  readonly putReported: (
    device: DeviceId,
    value: ShadowReported,
    expectedRevision: number
  ) => Effect.Effect<number, ShadowConflict>
}>()("mesh/ShadowStore") {}

/**
 * CAS loop as a value: read, transform, put at the read revision, retry on
 * conflict. The retry policy is data — swap `times` for a jittered
 * exponential `Schedule` without touching the logic.
 */
export const updateDesired = (
  device: DeviceId,
  f: (current: ShadowDesired | undefined) => ShadowDesired
): Effect.Effect<number, ShadowConflict, ShadowStore> =>
  Effect.gen(function*() {
    const store = yield* ShadowStore
    const current = yield* store.getDesired(device)
    return yield* store.putDesired(device, f(current?.value), current?.revision ?? 0)
  }).pipe(Effect.retry({ while: (e) => e._tag === "ShadowConflict", times: 5 }))

export const updateDesiredFirmware = (device: DeviceId, version: FirmwareVersion) =>
  updateDesired(device, (current) => ({
    firmware: version,
    reportIntervalMs: current?.reportIntervalMs ?? 60_000,
    features: current?.features ?? {}
  }))

type ShadowMap<A> = ReadonlyMap<DeviceId, Versioned<A>>

const makeHalf = <A>() =>
  Effect.gen(function*() {
    const ref = yield* SubscriptionRef.make<ShadowMap<A>>(new Map())
    const get = (device: DeviceId) =>
      SubscriptionRef.get(ref).pipe(Effect.map((m) => m.get(device)))
    const put = (device: DeviceId, value: A, expectedRevision: number) =>
      SubscriptionRef.modifyEffect(ref, (map) => {
        const actual = map.get(device)?.revision ?? 0
        if (actual !== expectedRevision) {
          return Effect.fail(
            new ShadowConflict({ device, expectedRevision, actualRevision: actual })
          )
        }
        const revision = actual + 1
        const next = new Map(map)
        next.set(device, { value, revision })
        return Effect.succeed([revision, next as ShadowMap<A>] as const)
      })
    const watch = (device: DeviceId) =>
      SubscriptionRef.changes(ref).pipe(
        Stream.filterMap((m) => {
          const v = m.get(device)
          return v === undefined ? Result.fail(undefined) : Result.succeed(v)
        }),
        Stream.changesWith((a, b) => a.revision === b.revision)
      )
    return { get, put, watch } as const
  })

/** In-memory store — the test/edge-sim wiring. Per-key revisions (the real
KV bucket uses one counter per bucket; same CAS contract either way). */
export const layerShadowMemory: Layer.Layer<ShadowStore> = Layer.effect(
  ShadowStore,
  Effect.gen(function*() {
    const desired = yield* makeHalf<ShadowDesired>()
    const reported = yield* makeHalf<ShadowReported>()
    return {
      getDesired: desired.get,
      putDesired: desired.put,
      watchDesired: desired.watch,
      getReported: reported.get,
      putReported: reported.put
    }
  })
)
