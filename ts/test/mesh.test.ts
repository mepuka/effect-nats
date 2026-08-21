import { assert, describe, it } from "@effect/vitest"
import { Effect, Fiber, Layer } from "effect"
import { TestClock } from "effect/testing"
import { WorkflowEngine } from "effect/unstable/workflow"
import { DurableDeferred } from "effect/unstable/workflow"
import type { RolloutPlan, TelemetryBatch } from "../src/mesh/Model"
import { DeviceId, FirmwareVersion, SiteId } from "../src/mesh/Model"
import { layerFleetStatic, OtaRollout, OtaRolloutLayer, WaveApproval } from "../src/mesh/Rollout"
import { layerShadowMemory, ShadowStore, updateDesiredFirmware } from "../src/mesh/Shadow"
import { layerTelemetry, Telemetry } from "../src/mesh/Telemetry"
import { layerUplinkMemory, UplinkProbe } from "../src/mesh/Uplink"

const site = SiteId.make("factory-1")
const dev = (n: number) => DeviceId.make(`dev-${n}`)
const v2 = FirmwareVersion.make("2.0.0")

describe("mesh", () => {
  it.effect("shadow CAS rejects stale writes and the retrying update converges", () =>
    Effect.gen(function*() {
      const store = yield* ShadowStore
      const d = dev(1)
      const rev1 = yield* store.putDesired(
        d,
        { firmware: FirmwareVersion.make("1.0.0"), reportIntervalMs: 60_000, features: {} },
        0
      )
      assert.strictEqual(rev1, 1)
      // stale writer (still believes revision 0) is rejected, typed:
      const conflict = yield* store
        .putDesired(d, { firmware: v2, reportIntervalMs: 1, features: {} }, 0)
        .pipe(Effect.flip)
      assert.strictEqual(conflict._tag, "ShadowConflict")
      assert.strictEqual(conflict.actualRevision, 1)
      // the CAS-loop helper reads fresh and lands:
      yield* updateDesiredFirmware(d, v2)
      const after = yield* store.getDesired(d)
      assert.strictEqual(after?.value.firmware, v2)
      assert.strictEqual(after?.revision, 2)
    }).pipe(Effect.provide(layerShadowMemory)))

  it.effect("telemetry batches by window, dedups by msgId, replicates with visible lag", () =>
    Effect.gen(function*() {
      const telemetry = yield* Telemetry
      const probe = yield* UplinkProbe
      const point = (n: number) => ({
        device: dev(1),
        site,
        metric: "temp",
        value: 20 + n,
        measuredAt: n
      })
      yield* telemetry.record(point(1))
      yield* telemetry.record(point(2))
      yield* telemetry.record(point(3))
      // window flush is TestClock-driven — no real time in tests
      yield* TestClock.adjust("2 seconds")
      const spool = yield* probe.edgeSpool
      assert.strictEqual(spool.length, 1)
      const batch = spool[0]?.payload as TelemetryBatch
      assert.strictEqual(batch.points.length, 3)
      // durably at the edge, not yet replicated: lag is honest
      assert.strictEqual(yield* probe.lag, 1)
      yield* TestClock.adjust("5 seconds")
      assert.strictEqual((yield* probe.cloud).length, 1)
      assert.strictEqual(yield* probe.lag, 0)
    }).pipe(
      Effect.provide(
        layerTelemetry({ batchSize: 100, window: "1 second", queueCapacity: 128 }).pipe(
          Layer.provideMerge(layerUplinkMemory({ site, replicationInterval: "5 seconds" }))
        )
      )
    ))

  it.effect("OTA rollout runs waves through the shadow, pauses on a durable approval", () =>
    Effect.gen(function*() {
      const plan: RolloutPlan = {
        rolloutId: "r-1",
        version: v2,
        site,
        waves: [0.5, 1],
        maxFailureRatio: 0.1
      }
      const fiber = yield* OtaRollout.execute({ plan }).pipe(Effect.forkChild)
      // approve wave 1 from "outside": the token derives from the
      // deterministic execution id — no channel back to the workflow needed
      const executionId = yield* OtaRollout.executionId({ plan })
      const token = DurableDeferred.tokenFromExecutionId(WaveApproval, {
        workflow: OtaRollout,
        executionId
      })
      yield* DurableDeferred.succeed(WaveApproval, { token, value: undefined })
      const status = yield* Fiber.join(fiber)
      assert.strictEqual(status.state, "done")
      assert.strictEqual(status.updated, 4)
      // the waves really went through the shadow's CAS:
      const store = yield* ShadowStore
      const shadow = yield* store.getDesired(dev(3))
      assert.strictEqual(shadow?.value.firmware, v2)
    }).pipe(
      Effect.provide(
        OtaRolloutLayer.pipe(
          Layer.provideMerge(WorkflowEngine.layerMemory),
          Layer.provideMerge(layerFleetStatic([dev(1), dev(2), dev(3), dev(4)])),
          Layer.provideMerge(layerShadowMemory)
        )
      )
    ))

  it.effect("rollout aborts a wave whose failure ratio exceeds the plan", () =>
    Effect.gen(function*() {
      const plan: RolloutPlan = {
        rolloutId: "r-2",
        version: v2,
        site,
        waves: [1],
        maxFailureRatio: 0.1
      }
      const status = yield* OtaRollout.execute({ plan })
      assert.strictEqual(status.state, "aborted")
      assert.strictEqual(status.updated, 0)
    }).pipe(
      Effect.provide(
        OtaRolloutLayer.pipe(
          Layer.provideMerge(WorkflowEngine.layerMemory),
          Layer.provideMerge(layerFleetStatic([dev(1), dev(2)], 0.5)),
          Layer.provideMerge(layerShadowMemory)
        )
      )
    ))
})
