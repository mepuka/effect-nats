# Synadia Agent Protocol adapter contract

The root `AGENTS.md` applies. This package independently implements the externally owned Synadia
Agent Protocol for NATS v0.3 with `effect@4.0.0-rc.111`. Correct wire behavior comes before
convenience or API surface area. Passing examples alone is not conformance.

Read `packages/agent/CONTEXT.md` and use its canonical terms. Never describe local codecs, state machines,
extensions, Effect APIs, or formal models as defining or improving the protocol.

## Scope routing

- **Implementation:** before changing `packages/agent/src/**`, read
  `packages/agent/src/AGENTS.md`; test work also reads `packages/agent/src/test/AGENTS.md`.
- **Protocol/specification documents:** before changing `packages/agent/docs/**`, read
  `packages/agent/docs/AGENTS.md`.
- **Exploratory design or formalization handoffs:** before changing `packages/agent/notes/**`, read
  `packages/agent/notes/AGENTS.md`.

## Authority

Use these sources in order. A lower source may clarify, but never weaken, a higher one.

1. `packages/agent/docs/upstream/core-protocol-v0.3.0.md` is the vendored wire contract.
2. `packages/agent/CONTEXT.md` mirrors upstream vocabulary and separates it from local terms.
3. `packages/agent/docs/01-SPEC.md` contains local implementation requirements; it cannot amend
   the protocol.
4. The workspace-installed `node_modules/effect/src` is authoritative for the pinned Effect API.
5. Root research and package notes are comparison/exploration evidence only.

Before changing protocol, discovery, heartbeat, streaming, errors, or wire schemas, read the
affected upstream sections, `packages/agent/docs/conformance/README.md`, and affected
requirement-ledger IDs.
Before choosing an Effect combinator, inspect its signature and tests in the pinned package source.

## Extension boundary

Non-standard durability, cancellation, control-plane, orchestration, work queues, or JetStream
designs are separate local extension protocols. They require an accepted local ADR, explicit
capability negotiation, vanilla-peer fallback, non-reserved subjects, and a separate test ledger.
They never count toward upstream conformance or change standard v0.3 endpoint/reply semantics.

## Completion

Run `bun run test:agents` and `bun run check:agents` from the monorepo root plus the applicable
workspace gates. A full-protocol conformance claim additionally requires zero unmapped
requirements, no accidental Agent SDK runtime dependency, and a real NATS interoperability run.
Record deviations beside their requirement IDs rather than weakening tests.
