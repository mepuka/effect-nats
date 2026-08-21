namespace EffectNats

/--
A subject token. Well-formedness (nonempty, no reserved characters) is
carried extrinsically so the matcher stays total over raw data; the wire
boundary validates before this layer.
-/
abbrev Token := String

def Token.wf (t : Token) : Prop :=
  t ≠ "" ∧ ¬t.contains '.' ∧ t ≠ "*" ∧ t ≠ ">"

/-- Pattern tokens: a literal or the single-token wildcard `*`. -/
inductive PTok where
  | lit (s : String)
  | star
  deriving Repr, DecidableEq

/--
A subscription pattern: per-token matchers, optionally terminated by `>`.
`>` anywhere but final position is unrepresentable by construction — the
one intrinsic invariant this slice takes.
-/
structure Pattern where
  toks : List PTok
  gt : Bool
  deriving Repr, DecidableEq

def PTok.matches : PTok → Token → Bool
  | .star, _ => true
  | .lit s, t => s == t

/--
The interest relation: `*` matches exactly one token; a final `>` matches
one or more remaining tokens.
-/
def matchSubject : List Token → Pattern → Bool
  | [], ⟨[], false⟩ => true
  | [], ⟨[], true⟩ => false
  | [], ⟨_ :: _, _⟩ => false
  | _ :: _, ⟨[], g⟩ => g
  | t :: ts, ⟨p :: ps, g⟩ => p.matches t && matchSubject ts ⟨ps, g⟩

/-- `>` alone matches every nonempty subject. -/
theorem gt_top {s : List Token} (h : s ≠ []) : matchSubject s ⟨[], true⟩ = true := by
  cases s with
  | nil => exact absurd rfl h
  | cons t ts => rfl

/-- `>` requires at least one token past the prefix. -/
theorem gt_needs_token : matchSubject [] ⟨[], true⟩ = false := rfl

/-- `*` matches exactly one token: arity is strict. -/
theorem star_one (t : Token) : matchSubject [t] ⟨[.star], false⟩ = true := rfl

theorem star_not_zero : matchSubject [] ⟨[.star], false⟩ = false := rfl

theorem star_not_two (t u : Token) : matchSubject [t, u] ⟨[.star], false⟩ = false := rfl

/-- Denotational refinement order on patterns. -/
def Refines (p q : Pattern) : Prop :=
  ∀ s, matchSubject s p = true → matchSubject s q = true

theorem Refines.refl (p : Pattern) : Refines p p := fun _ h => h

theorem Refines.trans {p q r : Pattern} (h₁ : Refines p q) (h₂ : Refines q r) :
    Refines p r := fun s h => h₂ s (h₁ s h)

/-- Weakening a literal to `*` moves up the refinement order. -/
theorem lit_refines_star (x : String) (ps : List PTok) (g : Bool) :
    Refines ⟨.lit x :: ps, g⟩ ⟨.star :: ps, g⟩ := by
  intro s h
  cases s with
  | nil => simp [matchSubject] at h
  | cons t ts =>
    simp only [matchSubject, PTok.matches, Bool.and_eq_true] at h ⊢
    exact ⟨trivial, h.2⟩

/--
Exact patterns are singletons: an all-literal, non-`>` pattern matches
precisely its own subject. This is the collision-freedom fact that
schema-derived subject naming will lean on.
-/
theorem exact_singleton (ts : List String) (s : List Token) :
    matchSubject s ⟨ts.map .lit, false⟩ = true ↔ s = ts := by
  induction ts generalizing s with
  | nil =>
    cases s with
    | nil => simp [matchSubject]
    | cons t ts' => simp [matchSubject]
  | cons t ts' ih =>
    cases s with
    | nil => simp [matchSubject]
    | cons u us =>
      simp only [List.map_cons, matchSubject, PTok.matches, Bool.and_eq_true,
        beq_iff_eq, ih, List.cons.injEq]
      constructor
      · rintro ⟨h₁, h₂⟩; exact ⟨h₁.symm, h₂⟩
      · rintro ⟨h₁, h₂⟩; exact ⟨h₁.symm, h₂⟩

end EffectNats
