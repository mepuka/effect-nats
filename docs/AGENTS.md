# Client documentation and decision records

The root `AGENTS.md` applies. This subtree separates evidence, proposals, and accepted decisions
for the principal NATS client.

## Document classes

- **Architecture:** `docs/architecture/README.md` is the status registry. Documents listed under
  “Accepted ADRs” constrain implementation. Proposed ADRs and draft workbench material do not.
- **Research:** `docs/research/**` records verified observations and design evidence. It may motivate an
  ADR but cannot authorize a public API or runtime promise.
- **Implementation style:** `docs/STYLE.md` governs code shape after a semantic decision exists; it does
  not define NATS behavior or approve an API.
- **Compatibility:** `docs/UPSTREAM_COMPATIBILITY.md` records supported or observed upstream behavior.
  Compatibility claims require exact version/source evidence and the matching test tier.

## Semantic declarations

An accepted configuration or operation specification is a formal declaration, not commentary. It
defines the semantic model through the applicable:

- carriers, constructors, and well-formedness judgments;
- relations, orders, equivalences, and refinement/subtyping judgments;
- operation signatures, preconditions, transitions, results, and typed failures;
- normalization or elaboration functions and their observable output; and
- equations, coherence requirements, algebraic laws, and trace properties.

Implementation modules are interpreters of these declarations. Examples, comments, and docstrings
may explain or link to them but cannot supply missing semantics or override them. A semantic change
edits or supersedes the declaration first, identifies every affected obligation, and then updates
interpreters and evidence.

“Formal declaration” here means explicit and normative. The sequential JetStream core is already
mechanized at `../Foldable/formal/effect_nats_substrate/`; claims inside its frozen signature may
be called machine-checked after a fresh package gate and exact theorem citation. Call a TypeScript,
Effect-runtime, or NATS-server behavior formally refined only when its separate relation and
evidence exist.

## Writing and changing decisions

Before architecture work, read `docs/architecture/README.md`, the affected accepted ADRs, and any
documents they explicitly depend on.

- Put a visible status near the title: draft, proposed, accepted, superseded, or research only.
- Separate sourced facts, local decisions, inferences, assumptions, and open questions.
- Give every accepted behavior a verification route: exact unit/property/model test, shared
  interpreter suite, live-server probe, interoperability run, or named formal obligation.
- State lifecycle, interruption, capacity/backpressure, error, retry, configuration/defaulting,
  compatibility, and observability consequences when the decision touches them.
- Update `docs/architecture/README.md` when an ADR is added or changes status. A material change to an
  accepted contract must record its amendment or supersession rather than silently rewriting
  history.

## Workflow specifications

A workflow document must state its status, entry conditions, authoritative inputs, ordered work,
produced artifacts/evidence, and checkable completion gates. Keep task-specific detail in that
document. Once accepted, add a compact trigger pointer in the closest owning `AGENTS.md`; the
pointer itself never makes the workflow normative.

## Evidence hygiene

Open every source before writing an exact path/line or version citation. Use repository-relative
paths in committed documents. Label server observations with the tested server/client versions.
Do not call an in-memory interpreter a server oracle, a test a proof, or a theorem a runtime
refinement unless the missing relation is itself established.
