# 0003 — System-model record: the graded instruction algebra (slice 1)

Produced by `lean-algebraic-systems` against the Pass A contract (notes/0002). Everything
below is implemented and proved in `lean/EffectNats/` (Guarantee, Subject, Graded); `lake
build` green, zero `sorry`. TS counterpart in `ts/src/sketch/{graded,subjects}.ts`,
typechecked against effect 4.0.0-rc.111 including the negative (`@ts-expect-error`) gates.

## Pass A amendments (representation answers, for Pass B to ratify)

1. **Carrier gained a fourth rung.** `Guarantee.pure` is adjoined as top, so the Lean
   carrier equals the TS `EffectiveGrade` result carrier exactly
   (`"pure" | "dedup-window" | "at-least-once" | "at-most-once"`). Rationale: the grade is
   interpreted as a LOWER BOUND on the guarantee of every delivery step a run executes;
   `pure` at top is vacuously sound and is the meet-neutral element.
2. **Open question 3 resolved: higher-order continuation `bind`.** Matches Effect's shape;
   `UsesCore` handles it with an ∃-witnessed `bindR`. Recorded cost: programs have no
   canonical serialization and branch-folds are relational, not computable. The first-order
   dependent-response free family (PolyFun shape) remains the alternative when
   choreography/serialization lands — this is the notes/0001 fork, deliberately deferred.
3. **No subeffecting constructor this slice.** The preorder exists (`rank`-induced, total);
   a `weaken : γ' ≤ γ → Prog γ α → Prog γ' α` rule is compatible with lower-bound semantics
   but omitted — the TS side has no counterpart (R only accumulates).

## Grade design record (per the graded-modeling discipline)

- Carrier `Guarantee`; interpretation: exact for single ops, lower bound for composites.
- Unit `pure` (= `top`); sequential composition `meet` (proved: comm, assoc, idem,
  neutral, lower-bound both sides, `atMostOnce` absorbing, order total).
- Choice/parallel composition: not in this slice. When `race`/`all` arrive, sequential
  meet is expected to remain correct for delivery grade, but ordering guarantees compose
  differently — keep them a separate grade axis (do not force one monoid).
- Grade per primitive: pure→`pure`, corePublish→`atMostOnce`, jsPublish→`atLeastOnce`,
  jsPublishDedup→`dedupWindow`.
- What the grade cannot establish: redelivery timing, dedup-window expiry, server truth
  (owed to natsA), scheduling.

## Interpreters and laws (implemented)

| Artifact | Status |
|---|---|
| `sim` — pure simulator threading `ServerModel` (append-only log + seen ids) | def; constructor equations definitional |
| `UsesCore` — relational reachability analyzer | inductive Prop |
| `usesCore_grade : UsesCore p → γ = atMostOnce` | **proved** — the syntactic statement the TS phantom encoding computes |
| `sim_guarantee_sound : m ∈ (sim p s).2.log → m ∈ s.log ∨ γ ≤ m.guarantee` | **proved** — semantic soundness: the static index is an honest lower bound on delivered guarantees |
| dedup behavior: same id twice appends once | proved by `decide` |
| Positive scenario: dedup publish + dependent core publish elaborates at grade `atMostOnce` | elaborator-checked |
| Negative program: `Prog dedupWindow Unit := corePublish …` | fails elaboration (recorded in docstring) |

Named model divergence: the simulator's dedup window is infinite (seen set never expires).
Window expiry is a refinement obligation against natsA, not silently assumed.

## Subject algebra (implemented alongside)

`matchSubject` (NB: `matches` is a Lean term-level keyword), `Refines` order with
refl/trans, and proved: `>`-top on nonempty subjects, `>` needs ≥1 token, `*` arity
strictness (0/1/2 witnesses), literal→`*` weakening, and `exact_singleton` — the
collision-freedom fact schema-derived subject naming will rest on.

## Adequacy obligations still open (Pass B / later slices)

- TS ⟷ Lean matcher agreement on generated vectors (deployment test, not theorem).
- TS `EffectiveGrade` ⟷ `meet`-fold agreement: statement is `usesCore_grade` +
  `sim_guarantee_sound`; the mechanical bridge (extract vectors, run tsc) is unbuilt.
- Consumer-side model (ack states, redelivery) — next algebraic slice; will make the
  at-least-once rung mean something on the subscribe path.
