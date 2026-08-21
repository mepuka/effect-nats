/**
 * P6 — OTA rollout as a durable workflow.
 *
 * Waves are activities (each durably checkpointed under a deterministic
 * name), desired firmware is written through the shadow's CAS, health is a
 * fleet observation, and the pause between waves is a DurableDeferred — an
 * approval that survives restarts of everything, resolvable from anywhere
 * that can derive the token from the deterministic execution id.
 */
import { Context, Effect, Layer, Schema } from "effect"
import { Activity, DurableDeferred, Workflow } from "effect/unstable/workflow"
import type { DeviceId, FirmwareVersion, RolloutStatus } from "./Model"
import { RolloutPlan, RolloutStatus as RolloutStatusSchema, ShadowConflict } from "./Model"
import { updateDesiredFirmware } from "./Shadow"

export interface FleetApi {
  readonly devices: Effect.Effect<ReadonlyArray<DeviceId>>
  readonly failureRatio: (
    cohort: ReadonlyArray<DeviceId>,
    version: FirmwareVersion
  ) => Effect.Effect<number>
}

export class Fleet extends Context.Service<Fleet, FleetApi>()("mesh/Fleet") {}

/** One approval gate per rollout execution; completed once, later waves pass. */
export const WaveApproval = DurableDeferred.make("mesh/OtaRollout/WaveApproval")

export const OtaRollout = Workflow.make("OtaRollout", {
  payload: { plan: RolloutPlan },
  success: RolloutStatusSchema,
  error: ShadowConflict,
  idempotencyKey: ({ plan }) => plan.rolloutId
})

export const OtaRolloutLayer = OtaRollout.toLayer(({ plan }) =>
  Effect.gen(function*() {
    const fleet = yield* Fleet
    const all = yield* fleet.devices
    let updated = 0
    let covered = 0
    for (let i = 0; i < plan.waves.length; i++) {
      const fraction = plan.waves[i] ?? 1
      const target = Math.min(all.length, Math.ceil(all.length * fraction))
      const cohort = all.slice(covered, target)
      covered = Math.max(covered, target)

      // Durably checkpointed: a crash after wave N resumes at wave N+1 with
      // the recorded ratio, not by re-flashing the cohort.
      const ratio = yield* Activity.make({
        name: `wave-${i}`,
        success: Schema.Number,
        error: ShadowConflict,
        execute: Effect.gen(function*() {
          const f = yield* Fleet
          for (const device of cohort) {
            yield* updateDesiredFirmware(device, plan.version)
          }
          return yield* f.failureRatio(cohort, plan.version)
        })
      })

      if (ratio > plan.maxFailureRatio) {
        const status: RolloutStatus = {
          rolloutId: plan.rolloutId,
          wave: i,
          updated,
          failed: cohort.length,
          state: "aborted"
        }
        return status
      }
      updated += cohort.length

      if (i < plan.waves.length - 1) {
        yield* DurableDeferred.await(WaveApproval)
      }
    }
    const done: RolloutStatus = {
      rolloutId: plan.rolloutId,
      wave: plan.waves.length - 1,
      updated,
      failed: 0,
      state: "done"
    }
    return done
  })
)

/** Static test/sim fleet. */
export const layerFleetStatic = (
  devices: ReadonlyArray<DeviceId>,
  failureRatio = 0
): Layer.Layer<Fleet> =>
  Layer.succeed(Fleet, {
    devices: Effect.succeed(devices),
    failureRatio: () => Effect.succeed(failureRatio)
  })
