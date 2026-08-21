namespace EffectNats

/--
Delivery-guarantee ladder with `pure` adjoined as the top element — the grade
of a program with no delivery obligations. The carrier deliberately matches
the TS `EffectiveGrade` result type (`"pure" | "dedup-window" |
"at-least-once" | "at-most-once"`) so the Lean model referees that encoding
one-to-one. `dedupWindow` names the window-bounded dedup mechanism; it does
not claim exactly-once.
-/
inductive Guarantee where
  | atMostOnce
  | atLeastOnce
  | dedupWindow
  | pure
  deriving Repr, DecidableEq

namespace Guarantee

/-- Strength rank; the ladder is a finite total order. -/
def rank : Guarantee → Nat
  | .atMostOnce => 0
  | .atLeastOnce => 1
  | .dedupWindow => 2
  | .pure => 3

instance : LE Guarantee := ⟨fun a b => a.rank ≤ b.rank⟩

instance (a b : Guarantee) : Decidable (a ≤ b) :=
  inferInstanceAs (Decidable (a.rank ≤ b.rank))

/--
Meet: the weaker of two rungs. Sequential composition of graded programs
carries the meet of the component grades — a pipeline is only as strong as
its weakest delivery step.
-/
def meet (a b : Guarantee) : Guarantee :=
  if a.rank ≤ b.rank then a else b

/-- The neutral element of `meet`. -/
def top : Guarantee := .pure

theorem le_refl (a : Guarantee) : a ≤ a := Nat.le_refl _

theorem le_trans {a b c : Guarantee} : a ≤ b → b ≤ c → a ≤ c := Nat.le_trans

theorem le_total (a b : Guarantee) : a ≤ b ∨ b ≤ a := Nat.le_total a.rank b.rank

theorem meet_comm : ∀ a b, meet a b = meet b a := by
  intro a b; cases a <;> cases b <;> decide

theorem meet_assoc : ∀ a b c, meet (meet a b) c = meet a (meet b c) := by
  intro a b c; cases a <;> cases b <;> cases c <;> decide

theorem meet_idem : ∀ a, meet a a = a := by
  intro a; cases a <;> decide

theorem meet_top_left : ∀ a, meet top a = a := by
  intro a; cases a <;> decide

theorem meet_top_right : ∀ a, meet a top = a := by
  intro a; cases a <;> decide

theorem meet_le_left : ∀ a b, meet a b ≤ a := by
  intro a b; cases a <;> cases b <;> decide

theorem meet_le_right : ∀ a b, meet a b ≤ b := by
  intro a b; cases a <;> cases b <;> decide

/-- `atMostOnce` is absorbing: one fire-and-forget step drags the whole
pipeline to the bottom rung. -/
theorem meet_atMostOnce_left : ∀ a, meet .atMostOnce a = .atMostOnce := by
  intro a; cases a <;> decide

theorem meet_atMostOnce_right : ∀ a, meet a .atMostOnce = .atMostOnce := by
  intro a; cases a <;> decide

end Guarantee

end EffectNats
