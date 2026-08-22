# 0002 — Lean Pass A contract: subject algebra, guarantee ladder, graded programs

Produced by `lean-formalization-strategy` Pass A. Status: **approved for exploratory modeling**
(first slice; representations may still move — Pass B freezes signatures before serious proof
work). Target: `lean/` package `EffectNats`, lean4 v4.33.0, Core-only.

## Domain contract

### Objects and observations

| Term | Meaning | Lean carrier (proposed) |
|---|---|---|
| Token | One subject segment; nonempty, no `.` `*` `>` or whitespace | `String` + well-formedness predicate (extrinsic for now) |
| Subject | Nonempty token list; `orders.eu.created` ⇝ `["orders","eu","created"]` | `List Token` (nonemptiness extrinsic) |
| Pattern | Per-token matchers (`lit s` \| `*`), optionally terminated by `>` | `structure Pattern (toks : List PTok) (gt : Bool)` — `>`-only-final is **intrinsic** (unrepresentable otherwise) |
| matches | The subscription-interest relation | `matches : List Token → Pattern → Bool` (executable, total) |
| Refines | Denotational pattern order: `p ⊑ q ↔ ∀ s, matches s p → matches s q` | `Prop` |
| Guarantee | Delivery rung: `atMostOnce < atLeastOnce < dedupWindow` | 3-constructor enum, rank into `Nat` |
| meet | Weakest of two rungs; grade of sequential composition | `Guarantee → Guarantee → Guarantee` |
| Prog γ α | Reified NATS program of grade γ producing α | grade-indexed inductive (see algebraic-systems handoff) |

Semantic vs representational: subject *strings* vs token *lists* is representational (a parse
relates them; parsing is out of scope this slice). `dedupWindow` deliberately does NOT claim
exactly-once — it names the window-bounded dedup mechanism; the boundary conditions live with
the (unwritten) natsA report and must not be strengthened here.

### Examples

Positive: `matches ["orders","eu","created"] (lit orders :: star :: lit created) = true`;
`>` alone matches every nonempty subject; exact patterns match exactly themselves.
Forbidden: `>` matching the empty remainder (`matches ["orders"] ⟨[lit orders], gt⟩ = false`
— `>` requires ≥1 token); `*` matching zero or two tokens; a pattern longer than the subject.
Overclaim counterexample (must stay refutable): "`meet` is a join" — it is a MEET; and
"`Refines` is total" — `⟨[lit a],false⟩` and `⟨[lit b],false⟩` are incomparable.

### Assumptions vs facts-to-prove vs deployment facts

- Assumed: token well-formedness at the boundary (TS/schema layer validates; Lean carries the
  predicate but this slice does not prove parser correctness).
- To prove (this slice): ladder is a total order; `meet` comm/assoc/idem/neutral; `>`-top;
  `*`-weakening refinement; exact-pattern singleton; grade soundness of `Prog`.
- Deployment facts (tests, not theorems): TS `Matches<S,P>` and the runtime matcher agree
  with Lean `matches` on generated vectors; server sublist agreement is evidence-tier only
  until natsA exists.

## Prior-art ledger

| Source | Class | Note |
|---|---|---|
| Foldable `research/2026-08-21-nats-docs-corpus-map.md` subject grammar | pattern | Grammar facts (`*` one token, `>` final one-or-more, reserved chars); docs-tier, natsA will verify against `sublist.go` |
| Foldable choreographic doc §3.2 semilattice laws | pattern | Law checklist for the ladder; scope warnings on CALM adopted |
| Foldable r-channel brief §6 phantom verdict | pattern | The TS encoding this model referees (grades on R; `EffectiveGrade` = meet) |
| Mathlib `Order.Lattice`, `SemilatticeInf` | adapt-later | Правильный home for the ladder once Mathlib is pinned; for a 3-element enum, from-scratch `meet` + `decide`-style case proofs is the smaller trusted base now |
| nats.js subtree `core/src/protocol.ts` matcher | pattern | Runtime reference for test vectors; read-only |

## Unresolved representation questions (for model-invariants / Pass B)

1. Token/Subject nonemptiness: extrinsic predicate now; revisit subtype vs raw+wf when the
   parser lands (raw wire tree + checked core is the corpus-recommended hybrid).
2. `Refines` as `Prop` vs decidable `Bool` procedure: keep `Prop` now; decidability is a
   later theorem (patterns are finite-depth, overlap is decidable).
3. `Prog` continuation shape: `bind` with function continuation (higher-order, matches Effect)
   vs first-order sequencing — algebraic-systems decides; grade-soundness statement must
   survive the choice.

## Declaration DAG (definitions and theorem signatures only)

```
Token, Token.wf                    (def, def)
PTok, Pattern                      (inductive, structure)
PTok.matches, matches              (def, def)         ← Token, Pattern
Refines                            (def)              ← matches
gt_top          : s ≠ [] → matches s ⟨[], true⟩ = true
star_weakens    : Refines ⟨lit x :: ps, g⟩ ⟨star :: ps, g⟩
exact_singleton : matches s ⟨ts.map lit, false⟩ = true ↔ s = ts
refines_refl/trans
Guarantee, rank, LE, meet          (inductive, defs)
meet_comm/assoc/idem, meet_top, meet_le_left/right, le_total
Prog (grade-indexed)               ← Guarantee        [algebraic-systems]
UsesCore        : Prog γ α → Prop  [algebraic-systems]
usesCore_grade  : UsesCore p → γ = atMostOnce         [cross-domain obligation]
```

Cross-domain obligation (algebra ⟷ CS-semantics): `Prog`'s grade index must be exactly
`meet` of component grades, so `usesCore_grade` is the Lean statement of the TS
`EffectiveGrade` boundary evaluation. Any change to `meet` re-opens `Prog`.

## Obligation ledger

| Obligation | Kind | Falsified by |
|---|---|---|
| ladder total order | law | incomparable pair |
| meet laws (comm/assoc/idem/neutral/lower-bound) | law | any counterexample; all finite — `cases`+`decide` |
| gt_top | positive witness | an unmatched nonempty subject |
| `>` needs ≥1 token | negative case | `matches ts ⟨ts.map lit ++ [>]-form⟩` succeeding on exact length |
| exact_singleton | characterization | a second subject matching an exact pattern |
| star_weakens | refinement law | a subject matched by the lit form, unmatched by star form |
| usesCore_grade | soundness / adequacy of TS encoding | a Prog reaching corePublish with grade ≠ atMostOnce |
| TS/Lean matcher agreement | deployment test | vector divergence (tsc-level + runtime) |

Trust boundary: this model referees the TS types; nothing here yet relates to `sublist.go`
or the wire. That refinement obligation is owned by the (future) conformance oracle.
