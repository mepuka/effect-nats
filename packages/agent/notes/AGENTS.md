# Exploratory notes and formalization handoffs

The root and `packages/agent/AGENTS.md` files apply. Notes are research, strategy, and handoff
artifacts. They are not upstream protocol authority, accepted runtime design, or conformance
evidence.

## Required claim boundaries

- Put a visible status and scope near the title.
- Separate verified facts, proposed representations, assumptions, theorems to prove, deployment
  tests, and unresolved obligations.
- Cite exact pinned sources after opening them; use repository/commit identities for cross-repo
  inputs instead of machine-specific absolute paths.
- Never let a local algebra, schema, subject model, or grade rename or strengthen a NATS guarantee.

## Formalization handoffs

A Lean handoff names the domain objects and observations, semantic versus representational choices,
declaration DAG, theorem signatures, counterexamples, trust boundary, and refinement/deployment
obligations. It must identify the exact TypeScript declaration or trace format it intends to
referee.

Compilation proves only that the formal artifact typechecks in its stated Lean project and
toolchain. A theorem about the model is not evidence about the TypeScript implementation, wire
codec, NATS server, or deployment until the corresponding relation is defined and checked.

## Promotion

When exploratory work becomes a runtime or public decision, promote it into an accepted ADR or
development specification with executable evidence and update the nearest instruction pointer.
Leave the note as provenance; do not make implementation depend on a note's unstated status.
