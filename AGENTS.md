# effect-nats — workspace contract

This Bun workspace contains two semantic contexts:

- the principal Effect v4 NATS client at the repository root; and
- `packages/agent`, an independent adapter for the externally owned Synadia Agent Protocol v0.3.

The client public contract is still under design. `src/index.ts` stays empty until an accepted ADR
approves a surface. Read `CONTEXT-MAP.md` before changing cross-package terminology or moving a
capability between the two contexts.

## Scope routing

The nearest nested `AGENTS.md` adds rules; it never replaces this file.

- **Client internals:** before changing `src/internal/**`, read `src/internal/AGENTS.md`.
- **Client evidence:** before changing `test/**`, read `test/AGENTS.md`.
- **Client decisions or research:** before changing `docs/**`, read `docs/AGENTS.md`.
- **Agent Protocol adapter:** before changing `packages/agent/**`, read
  `packages/agent/AGENTS.md` and any nearer instructions it names.

## Task routing

- **Client planning, implementation, or review:** read `TO_FIX.md`. Confirmed findings block
  overlapping work until corrected and reverified; watch items guide inspection but do not settle
  an open design.
- **Implementation or review:** invoke `.agents/skills/effect-nats-implementation/SKILL.md` and
  follow its fidelity receipt.
- **Feature, fix, stream, lifecycle, or integration-test work:** also invoke
  `.agents/skills/tdd/SKILL.md` and work in one-seam red/green vertical slices.
- **Stream, subscription, queue, fiber, readiness, timeout, or concurrency work:** also invoke
  `.agents/skills/effect4-stream-discipline/SKILL.md`. Its case studies are methodology, never NATS
  semantic authority.
- **Effect code, services, streams, errors, retries, configuration, or tests:** also read
  `docs/STYLE.md` and verify API shapes in the pinned `effect` package source.
- **Sequential JetStream semantics or formal claims:** read the sibling Foldable package contract
  at `../Foldable/formal/effect_nats_substrate/AGENTS.md`,
  `../Foldable/formal/effect_nats_substrate/docs/signature-snapshot.md`, and
  `../Foldable/research/2026-08-22-first-slice-jetstream-memory-lean-model.md` before changing the
  modeled declarations or their evidence boundary.
- **Lean modeling, proof, or reasoning about the Lean model (including slice documents, briefs,
  reviews, and refinement claims):** invoke the Lean systems suite — installed at
  `.agents/skills/lean-*`, catalogued in `skills/CATALOG.md` — in catalogue order:
  `lean-formalization-strategy` Pass A before any contract or slice document; `lean-model-invariants`
  and `lean-algebraic-systems` for representation and transition design; Pass B to freeze signatures
  before proofs; `lean-llm-proof-loop` for proof bodies against frozen statements;
  `lean-assurance-review` before any claim is called verified. This routing is mandatory, not
  advisory.

## Workspace laws

1. **Design gate.** A new public export, service method, error variant, encoded type, compatibility
   promise, or runtime promise requires an accepted ADR. A proposal, workbench note, test helper,
   or `AGENTS.md` pointer does not confer acceptance.
2. **Semantic declarations govern.** Accepted configuration and operation specifications are
   formal declarations of the model: their carriers, relations, signatures, equations, errors,
   observations, and laws constrain every interpreter. Source comments explain a declaration; they
   do not replace or amend it. The covered sequential JetStream slice is mechanized in the sibling
   Lean package. Change semantics through the governing declaration and reopen its proofs and
   implementation evidence.
3. **Source precedence.** Pinned NATS protocol/server and official client source > conformance and
   integration observations > official documentation > third-party projects. Record
   disagreements; do not blend the tiers.
4. **Scope owns lifetime.** Connections, readers, writers, subscriptions, requests, reconnect
   loops, heartbeats, and drains acquire and finalize within `Scope`. Long-lived fibers are scoped.
5. **Flow control is explicit.** Every producer/consumer boundary names capacity, unit, overflow
   and terminal ordering, producer suspension authority, and release behavior.
6. **Boundaries are typed.** Decode untrusted wire, configuration, and storage values; preserve
   protocol status and headers; translate transport defects without swallowing interruption.
7. **Retry is truthful.** Apply `Schedule` only at a named idempotent boundary. Publish, request,
   reconnect, KV mutation, and object transfer have different retry consequences.
8. **Claims name their evidence.** Distinguish accepted design, local deterministic tests,
   in-memory model behavior, live-server observations, interoperability evidence, formal theorems,
   assumptions, and open obligations. None silently stands in for another.
9. **No accidental endorsement.** State the repository's actual governance and release status; do
   not imply NATS-project or Synadia acceptance.

## Keeping instructions current

When an accepted ADR or workflow specification begins governing a subtree, add one short trigger
pointer to the nearest applicable `AGENTS.md` in the same change. Keep the evolving rule in its
canonical document rather than copying it into several instruction files. When a document is
superseded, update or remove its pointers.

## Completion

Code changes are complete only after `bun run format:check`, `bun run lint`, `bun run check`, and
`bun run test` pass, affected public decisions are cited, and lifecycle/backpressure behavior is
covered. Documentation-only instruction changes require formatting plus a pointer/scope audit.
Always report which claims were tested locally, tested against a server, proved, assumed, or left
open, and do not include unrelated working-tree changes.
