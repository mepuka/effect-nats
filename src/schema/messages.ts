/**
 * Message schemas — the single source of truth.
 *
 * These are real Effect Schemas: codecs, arbitrary generators, JSON Schema,
 * equivalence all derive from these definitions. The Lean model
 * (lean/EffectNats) referees the SEMANTICS (round-trip-if-iso, canonicality,
 * subject collision-freedom); this file is the production encoding.
 */
import { Schema } from "effect"

/** Device/user profile event. Optional fields are wire-absent, not null. */
export const UserCreated = Schema.Struct({
  name: Schema.String,
  age: Schema.optionalKey(Schema.Number),
})
export type UserCreated = typeof UserCreated.Type

/**
 * Tagged union on the wire: `{"_tag":"Quit"}`, `{"_tag":"Move","dx":2}`.
 * Discriminator survives every codec — this is what schema-derived subject
 * naming dispatches on.
 */
export const DeviceEvent = Schema.TaggedUnion({
  Quit: {},
  Move: { dx: Schema.Number },
})
export type DeviceEvent = typeof DeviceEvent.Type
