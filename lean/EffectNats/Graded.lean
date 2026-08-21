import EffectNats.Guarantee
import EffectNats.Subject

namespace EffectNats

/-- Placeholder wire payload. -/
abbrev Payload := List UInt8

abbrev MsgId := String

structure PubAck where
  seq : Nat
  duplicate : Bool
  deriving Repr, DecidableEq

/-- One published message as the simulator records it. -/
structure Published where
  subject : List Token
  payload : Payload
  guarantee : Guarantee
  deriving Repr

/--
Minimal server model: an append-only log and the seen dedup ids. Named
divergence from the real system: the dedup set never expires, so this models
an INFINITE dedup window. Window expiry is a refinement obligation owed to
the natsA semantics report, not silently assumed away.
-/
structure ServerModel where
  log : List Published
  seen : List MsgId
  deriving Repr

def ServerModel.empty : ServerModel := ⟨[], []⟩

/--
Grade-indexed reified NATS programs. The index is a static LOWER BOUND:
every delivery step a run executes provides at least the program's grade.
`bind` carries the meet, so no combinator can launder a weak guarantee into
a strong one.

TS correspondence: grades ride Effect's `R` channel as phantom services —
`R`'s union is a free join-semilattice on identifiers — and the type-level
`EffectiveGrade<R>` evaluates this meet at the run boundary. `bind`'s index
here is the Lean statement of that evaluation.

Deliberately invalid (fails elaboration — the index computes the analysis):
`example : Prog .dedupWindow Unit := .corePublish [] []`
-/
inductive Prog : Guarantee → Type → Type 1 where
  | pure {α : Type} (a : α) : Prog Guarantee.pure α
  | corePublish (subject : List Token) (payload : Payload) : Prog Guarantee.atMostOnce Unit
  | jsPublish (subject : List Token) (payload : Payload) : Prog Guarantee.atLeastOnce PubAck
  | jsPublishDedup (id : MsgId) (subject : List Token) (payload : Payload) :
      Prog Guarantee.dedupWindow PubAck
  | bind {γ₁ γ₂ : Guarantee} {α β : Type} (p : Prog γ₁ α) (k : α → Prog γ₂ β) :
      Prog (Guarantee.meet γ₁ γ₂) β

namespace Prog

/--
Interpreter 1 — pure simulator. Threads the server model, appends to the
log, answers dedup publishes from the seen set. Happy path only: failures
and redelivery belong to the consumer model, not this slice.
-/
def sim : {γ : Guarantee} → {α : Type} → Prog γ α → ServerModel → α × ServerModel
  | _, _, .pure a, s => (a, s)
  | _, _, .corePublish subj pl, s =>
      ((), { s with log := s.log ++ [⟨subj, pl, .atMostOnce⟩] })
  | _, _, .jsPublish subj pl, s =>
      (⟨s.log.length + 1, false⟩, { s with log := s.log ++ [⟨subj, pl, .atLeastOnce⟩] })
  | _, _, .jsPublishDedup id subj pl, s =>
      if s.seen.contains id then
        (⟨s.log.length, true⟩, s)
      else
        (⟨s.log.length + 1, false⟩,
          ⟨s.log ++ [⟨subj, pl, .dedupWindow⟩], id :: s.seen⟩)
  | _, _, .bind p k, s => sim (k (sim p s).1) (sim p s).2

/--
Interpreter 2 — reachability analyzer (relational): the program can execute
a core (fire-and-forget) publish along some path.
-/
inductive UsesCore : ∀ {γ : Guarantee} {α : Type}, Prog γ α → Prop where
  | corePublish {subj pl} : UsesCore (.corePublish subj pl)
  | bindL {γ₁ γ₂ : Guarantee} {α β : Type} {p : Prog γ₁ α} {k : α → Prog γ₂ β} :
      UsesCore p → UsesCore (p.bind k)
  | bindR {γ₁ γ₂ : Guarantee} {α β : Type} {p : Prog γ₁ α} {k : α → Prog γ₂ β}
      (a : α) : UsesCore (k a) → UsesCore (p.bind k)

/--
Syntactic grade soundness — the exact statement the TS phantom encoding
computes: reach a core publish anywhere and the whole composition is graded
at-most-once.
-/
theorem usesCore_grade {γ : Guarantee} {α : Type} {p : Prog γ α}
    (h : UsesCore p) : γ = .atMostOnce := by
  induction h with
  | corePublish => rfl
  | bindL _ ih => rw [ih]; exact Guarantee.meet_atMostOnce_left _
  | bindR _ _ ih => rw [ih]; exact Guarantee.meet_atMostOnce_right _

/--
Semantic grade soundness: every message a run APPENDS to the log carries at
least the program's grade. The static index is an honest lower bound on
delivered guarantees — the theorem that makes `EffectiveGrade` mean
something operational.
-/
theorem sim_guarantee_sound {γ : Guarantee} {α : Type} (p : Prog γ α) :
    ∀ (s : ServerModel) (m : Published),
      m ∈ (sim p s).2.log → m ∈ s.log ∨ γ ≤ m.guarantee := by
  induction p with
  | pure a =>
    intro s m h
    simp only [sim] at h
    exact Or.inl h
  | corePublish subj pl =>
    intro s m h
    simp only [sim, List.mem_append, List.mem_singleton] at h
    rcases h with h | h
    · exact Or.inl h
    · subst h; exact Or.inr (Guarantee.le_refl _)
  | jsPublish subj pl =>
    intro s m h
    simp only [sim, List.mem_append, List.mem_singleton] at h
    rcases h with h | h
    · exact Or.inl h
    · subst h; exact Or.inr (Guarantee.le_refl _)
  | jsPublishDedup id subj pl =>
    intro s m h
    simp only [sim] at h
    split at h
    · exact Or.inl h
    · simp only [List.mem_append, List.mem_singleton] at h
      rcases h with h | h
      · exact Or.inl h
      · subst h; exact Or.inr (Guarantee.le_refl _)
  | bind p k ihp ihk =>
    intro s m h
    simp only [sim] at h
    rcases ihk _ _ _ h with h' | h'
    · rcases ihp _ _ h' with h'' | h''
      · exact Or.inl h''
      · exact Or.inr (Guarantee.le_trans (Guarantee.meet_le_left _ _) h'')
    · exact Or.inr (Guarantee.le_trans (Guarantee.meet_le_right _ _) h')

/--
Positive scenario: a dedup publish with a dependent fire-and-forget follow-up.
The composite grade collapses to `atMostOnce` by `meet` — checked by the
elaborator, no proof needed.
-/
example : Prog Guarantee.atMostOnce Unit :=
  .bind (.jsPublishDedup "id-1" ["orders", "eu", "created"] []) fun ack =>
    .corePublish ["metrics", "published"] (if ack.duplicate then [] else [1])

/-- The simulator honors dedup: publishing the same id twice appends once. -/
example :
    ((sim (.bind (.jsPublishDedup "a" ["s"] []) fun _ => .jsPublishDedup "a" ["s"] [])
      ServerModel.empty).2.log.length) = 1 := by decide

end Prog

end EffectNats
