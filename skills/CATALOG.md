# Lean systems skill suite

These are standalone, repository-ready skill packages stored together so their boundaries and
evidence can be reviewed as one system. They are intentionally not nested Git repositories: nested
metadata would hide their contents from this repository. Each child can be published or
installed independently.

The research review uses responsibility labels (`project-foundation`, `model-boundaries`,
`compositional-systems`, `formalization-design`, `proof-development`, `assurance-audit`). The
package names below are the shorter invocation names for those same six roles. Formalization
strategy is intentionally two-pass: Pass A decomposes/retrieves before representation work; Pass B
rechecks the completed boundary/system model and freezes exact public signatures before proof work.

## Why these six packages

| Skill                                                                 | Responsibility                                                                                                                   | Lean workflow position                          | Why it is separate                                                                                                             |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| [`lean-project-bootstrap`](lean-project-bootstrap/SKILL.md)           | Create or repair a reproducible Lean package without silently changing global toolchains or dependency policy                    | Environment and repository setup                | Lake/Elan mutation and reproducibility decisions occur before modeling and have a different safety boundary                    |
| [`lean-formalization-strategy`](lean-formalization-strategy/SKILL.md) | Decompose an informal problem, search exact-project and pinned prior art, then choose reuse, adaptation, or a from-scratch model | Domain contract and formalization planning      | It freezes intent and emits a declaration DAG/obligation ledger without prematurely choosing representations or filling proofs |
| [`lean-model-invariants`](lean-model-invariants/SKILL.md)             | Choose intrinsic, extrinsic, bundled, canonical, or runtime-validated representations and state their proof obligations          | Domain model and trusted-boundary design        | Representation choice changes every later theorem; it should finish before proof search starts                                 |
| [`lean-algebraic-systems`](lean-algebraic-systems/SKILL.md)           | Design reified instruction algebras, interpreters, indexed/graded composition, transition systems, and histories                 | System semantics and compositional architecture | `Monad` laws, workflow syntax, state transitions, and causal traces are related but are not proof tactics or setup concerns    |
| [`lean-llm-proof-loop`](lean-llm-proof-loop/SKILL.md)                 | Use an LLM as a proposal/search engine while Lean, approved statements, and reproducible gates remain authoritative              | Iterative formalization and proof repair        | Proof generation needs rapid diagnostic feedback and a strict statement-change boundary                                        |
| [`lean-assurance-review`](lean-assurance-review/SKILL.md)             | Audit specification, model, proof, implementation, and external trust as independent evidence axes                               | Acceptance, release, or adversarial review      | Kernel success cannot establish model fidelity, implementation refinement, FFI behavior, or operational assumptions            |

The split follows the strongest recurring result in the local corpus: a practical Lean workflow is a
sequence of distinct trust transformations—raw project state to reproducible environment, raw data
to checked model, syntax to semantics, proof proposal to kernel-accepted theorem, and theorem to an
explicit assurance claim. See the source-backed synthesis in
[`research/2026-08-21-lean4-workflow-skill-design-review.md`](../../Foldable/research/2026-08-21-lean4-workflow-skill-design-review.md).
The strategy package's progressive topic selector and reusable algebra, computer-science,
Mathlib/Std, state/distributed-systems, and quantitative/physical modeling capsules are derived in
[`research/2026-08-21-lean4-formalization-topic-map.md`](../../Foldable/research/2026-08-21-lean4-formalization-topic-map.md).

## Evidence behind the suite

- Lean's reference identifies Lake as the standard build tool and recommends project-specific,
  fixed Elan toolchains; this motivates a dedicated reproducibility package
  ([Lake](https://lean-lang.org/doc/reference/latest/Build-Tools-and-Distribution/Lake/),
  [Elan](https://lean-lang.org/doc/reference/latest/Build-Tools-and-Distribution/Managing-Toolchains-with-Elan/)).
- Indexed families make illegal constructor combinations uninhabited, but their dependent matching
  cost is real; the local invariant survey therefore recommends them only for stable, local,
  compositional legality
  ([Lean indexed families](https://lean-lang.org/functional_programming_in_lean/Programming-with-Dependent-Types/Indexed-Families/),
  [`research/2026-08-20-lean-typescript-go-modular-codegen-and-invariants.md`](../../Foldable/research/2026-08-20-lean-typescript-go-modular-codegen-and-invariants.md)).
- The monadic survey shows that ordinary `Monad` composition is executable but does not itself give
  a serializable workflow, static analysis, replay, or concurrent history; those require reified
  operations, interpreters, laws, and an explicit trace model
  ([`research/2026-08-20-lean4-lawful-monadic-history-coordination.md`](../../Foldable/research/2026-08-20-lean4-lawful-monadic-history-coordination.md)).
- LeanDojo, Lean Copilot, and Pantograph all place learned search around a formal checker or proof
  state. They justify an LLM loop in which generated steps are proposals and exact-project
  retrieval matters
  ([LeanDojo](https://arxiv.org/abs/2306.15626),
  [Lean Copilot](https://arxiv.org/abs/2404.12534),
  [Pantograph](https://doi.org/10.1007/978-3-031-90643-5_6)).
- Lean's toolchain includes `leanchecker`, and Mathlib exposes `#print axioms`; neither says that a
  theorem is the right theorem or that an external implementation refines it. The assurance package
  therefore audits those links separately
  ([Lean build tools](https://lean-lang.org/doc/reference/latest/Build-Tools-and-Distribution/),
  [Mathlib axiom inspection](https://leanprover-community.github.io/mathlib4_docs/Mathlib/Logic/Basic.html),
  [`research/2026-08-20-lean4-engineering-assurance-modeling.md`](../../Foldable/research/2026-08-20-lean4-engineering-assurance-modeling.md)).

## Composition

Use only the packages that match the current phase:

```text
project-bootstrap
  -> formalization-strategy (Pass A: contract and prior art)
  -> model-invariants
  -> algebraic-systems (when the domain is effectful/stateful/protocol-shaped)
  -> formalization-strategy (Pass B: validate and freeze signatures)
  -> llm-proof-loop
  -> assurance-review
```

The arrows are handoff boundaries, not a mandatory ceremony. A small pure theorem may need only the
proof loop; an existing verified model may need only assurance review. The strategy package owns the
reusable topic library and routes to only the relevant topic reference or references. A finding that changes the
statement or representation returns to the corresponding earlier package instead of being patched
inside a proof.
