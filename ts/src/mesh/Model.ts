/**
 * Domain model for the mesh IoT abstractions. Everything on the wire or in a
 * KV bucket is Schema-defined — the layering law from the corpus: framing
 * below, Schema as the spine, descriptors above.
 */
import { Schema } from "effect"

export const DeviceId = Schema.String.pipe(Schema.brand("DeviceId"))
export type DeviceId = typeof DeviceId.Type

export const SiteId = Schema.String.pipe(Schema.brand("SiteId"))
export type SiteId = typeof SiteId.Type

export const FirmwareVersion = Schema.String.pipe(Schema.brand("FirmwareVersion"))
export type FirmwareVersion = typeof FirmwareVersion.Type

// ————— Telemetry —————

export const TelemetryPoint = Schema.Struct({
  device: DeviceId,
  site: SiteId,
  metric: Schema.String,
  value: Schema.Number,
  /** device wall-clock, epoch millis — the mesh never trusts it for ordering */
  measuredAt: Schema.Number
})
export type TelemetryPoint = typeof TelemetryPoint.Type

/** What actually travels: a batch, with a device-scoped sequence for dedup. */
export const TelemetryBatch = Schema.Struct({
  device: DeviceId,
  /** monotone per-device counter; `${device}:${batchSeq}` is the Nats-Msg-Id */
  batchSeq: Schema.Number,
  points: Schema.Array(TelemetryPoint)
})
export type TelemetryBatch = typeof TelemetryBatch.Type

// ————— Device shadow (digital twin) —————

export const ShadowDesired = Schema.Struct({
  firmware: FirmwareVersion,
  reportIntervalMs: Schema.Number,
  features: Schema.Record(Schema.String, Schema.Boolean)
})
export type ShadowDesired = typeof ShadowDesired.Type

export const ShadowReported = Schema.Struct({
  firmware: FirmwareVersion,
  online: Schema.Boolean,
  lastSeenAt: Schema.Number,
  batteryPct: Schema.Number
})
export type ShadowReported = typeof ShadowReported.Type

// ————— Command plane —————

export const Reboot = Schema.TaggedStruct("Reboot", {})
export const SetReportInterval = Schema.TaggedStruct("SetReportInterval", {
  intervalMs: Schema.Number
})
export const ApplyFirmware = Schema.TaggedStruct("ApplyFirmware", {
  version: FirmwareVersion,
  checksum: Schema.String
})

export const Command = Schema.Union([Reboot, SetReportInterval, ApplyFirmware])
export type Command = typeof Command.Type

export const CommandEnvelope = Schema.Struct({
  /** idempotency key — device-side dedup, cloud-side correlation */
  commandId: Schema.String,
  device: DeviceId,
  issuedAt: Schema.Number,
  command: Command
})
export type CommandEnvelope = typeof CommandEnvelope.Type

export const CommandAck = Schema.Struct({
  commandId: Schema.String,
  device: DeviceId,
  outcome: Schema.Literals(["applied", "rejected", "duplicate"]),
  detail: Schema.String
})
export type CommandAck = typeof CommandAck.Type

// ————— OTA rollout —————

export const RolloutPlan = Schema.Struct({
  rolloutId: Schema.String,
  version: FirmwareVersion,
  site: SiteId,
  /** wave sizes as fractions of the fleet, e.g. [0.01, 0.1, 1.0] */
  waves: Schema.Array(Schema.Number),
  /** abort when failure ratio in a wave exceeds this */
  maxFailureRatio: Schema.Number
})
export type RolloutPlan = typeof RolloutPlan.Type

export const RolloutStatus = Schema.Struct({
  rolloutId: Schema.String,
  wave: Schema.Number,
  updated: Schema.Number,
  failed: Schema.Number,
  state: Schema.Literals(["running", "awaiting-approval", "done", "aborted"])
})
export type RolloutStatus = typeof RolloutStatus.Type

// ————— Errors (typed, on the E channel — never stringly) —————

export class DeviceUnreachable extends Schema.TaggedError<DeviceUnreachable>()(
  "DeviceUnreachable",
  { device: DeviceId }
) {}

export class ShadowConflict extends Schema.TaggedError<ShadowConflict>()(
  "ShadowConflict",
  {
    device: DeviceId,
    expectedRevision: Schema.Number,
    actualRevision: Schema.Number
  }
) {}

export class UplinkDown extends Schema.TaggedError<UplinkDown>()("UplinkDown", {
  site: SiteId
}) {}
