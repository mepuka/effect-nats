import Lean

namespace EffectNats

/-!
# Codec effects

This module models encoding and decoding as two small effects. It deliberately
does not formalize a recursive schema language or JSON object canonicalization.
Those belong to a later refinement between concrete Effect Schema codecs and
this interface.

The core assumption is visible in `Codec.roundTrip`: every codec admitted to
the model must prove that a successful encoding decodes to its source value.
-/

/-- A small, observable failure returned by a codec effect. -/
inductive CodecError where
  | expected (kind : String)
  | rejected (reason : String)
  deriving Repr, DecidableEq

/-- Encoding and decoding use one ordinary typed-error effect. -/
abbrev CodecM (α : Type) := Except CodecError α

/--
A lawful bidirectional codec. Concrete codecs own their implementation and
must discharge the successful encode/decode round-trip at construction.
-/
structure Codec (α : Type) where
  encode : α → CodecM Lean.Json
  decode : Lean.Json → CodecM α
  roundTrip : ∀ (value : α) (json : Lean.Json),
    encode value = .ok json → decode json = .ok value

namespace Codec

/-- The string codec is a primitive lawful codec. -/
def string : Codec String where
  encode value := .ok (.str value)
  decode
    | .str value => .ok value
    | _ => .error (.expected "string")
  roundTrip := by
    intro value json h
    cases h
    rfl

/-- The boolean codec is a primitive lawful codec. -/
def bool : Codec Bool where
  encode value := .ok (.bool value)
  decode
    | .bool value => .ok value
    | _ => .error (.expected "boolean")
  roundTrip := by
    intro value json h
    cases h
    rfl

/-- Transport a codec across an explicit isomorphism. -/
def iso {α β : Type} (base : Codec α) (forward : α → β) (backward : β → α)
    (inverse : Function.LeftInverse forward backward) : Codec β where
  encode value := base.encode (backward value)
  decode json := (base.decode json).map forward
  roundTrip := by
    intro value json hEncode
    have hDecode := base.roundTrip (backward value) json hEncode
    rw [hDecode]
    change Except.ok (forward (backward value)) = Except.ok value
    exact congrArg Except.ok (inverse value)

end Codec

/-- First-order requests made by the codec effect layer. -/
inductive CodecEffect : Type → Type 1 where
  | encode {α : Type} (codec : Codec α) (value : α) :
      CodecEffect (CodecM Lean.Json)
  | decode {α : Type} (codec : Codec α) (json : Lean.Json) :
      CodecEffect (CodecM α)

namespace CodecEffect

/-- Pure interpreter for one codec request. -/
def run : {α : Type} → CodecEffect α → α
  | _, .encode codec value => codec.encode value
  | _, .decode codec json => codec.decode json

/--
The effect-level round trip. This is the old headline claim with its actual
assumption made explicit: the selected codec is lawful and encoding succeeded.
-/
theorem rt_ed {α : Type} (codec : Codec α) (value : α) (json : Lean.Json)
    (h : run (.encode codec value) = .ok json) :
    run (.decode codec json) = .ok value :=
  codec.roundTrip value json h

/-- Running a successful string encode followed by its decode recovers it. -/
example (value : String) :
    run (.decode Codec.string (.str value)) = .ok value := by
  rfl

/-- A mismatched wire value stays an ordinary typed failure. -/
example : run (.decode Codec.string (.bool true)) =
    .error (.expected "string") := by
  rfl

end CodecEffect

end EffectNats
