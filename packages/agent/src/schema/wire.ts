/**
 * Wire helpers — the boundary where schema values become JSON text.
 *
 * `Schema.fromJsonString` folds parsing into the codec itself, so encode/decode
 * are plain Schema operations; there is no hand-rolled boundary to trust.
 *
 * The Lean model's `rt_ed` (encode then decode recovers the value) is the law
 * this boundary obeys; property tests assert it on this side.
 */
import { Effect, Schema } from "effect"

/** Schema value → JSON wire text. */
export const encodeWire =
  <A>(schema: Schema.Codec<A>) =>
  (value: A): string =>
    JSON.stringify(Schema.encodeSync(schema)(value))

/**
 * JSON wire text → schema value. Throws on undecodable input — an error
 * channel entry callers cannot ignore (notes/0005: decode failure is typed,
 * never a silent skip). Prefer `decodeWireEffect` at Effect call sites.
 */
export const decodeWire =
  <A>(schema: Schema.Codec<A>) =>
  (text: string): A =>
    Schema.decodeUnknownSync(schema)(JSON.parse(text))

/** Same, as an Effect with the failure in the E channel. */
export const decodeWireEffect =
  <A>(schema: Schema.Codec<A>) =>
  (text: string): Effect.Effect<A, Error> => Schema.decodeEffect(Schema.fromJsonString(schema))(text)
