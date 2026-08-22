# effect-nats — monorepo rules

This Bun workspace develops the principal Effect v4 NATS client and one subsystem:

- the root `effect-nats` client for NATS Core and the JetStream primitives required by Key/Value
  and Object Store; and
- `packages/agents`, the `effect-nats-agents` subsystem implementing Synadia Agent Protocol v0.3.

The client public contract is under design; its root export stays empty until an ADR approves the
first API surface. Agent-protocol work also reads `packages/agents/AGENTS.md`.

Read `CONTEXT-MAP.md` before changing cross-package terminology or moving a capability between the
principal NATS client and the Agents subsystem.

## Read before changing

- **Client findings:** before client planning, implementation, or review, read `TO_FIX.md`.
  Confirmed items block overlapping work until corrected and reverified; watch items guide
  inspection but do not authorize unresolved design choices.
- **Agent Protocol:** before changing `packages/agents`, read its `AGENTS.md`, vendored protocol,
  affected requirement-ledger entries, and conformance rules.
- **Implementation or review:** invoke the repository-local `effect-nats-implementation` skill at
  `.agents/skills/effect-nats-implementation/SKILL.md` and follow its fidelity receipt.
- **Feature, fix, stream, lifecycle, or integration-test work:** also invoke the repository-local
  `tdd` skill at `.agents/skills/tdd/SKILL.md` and work in one-seam red/green vertical slices.
- **Stream, subscription, queue, fiber, readiness, timeout, or concurrency work:** also invoke
  `.agents/skills/effect4-stream-discipline/SKILL.md` for hardening. Its agent-protocol case studies
  are methodology, never NATS semantic authority.
- **Effect code, services, streams, errors, retries, config, or tests:** also read `docs/STYLE.md`.
- **Protocol, compatibility, Core, KV, or Object Store behavior:** read
  `docs/research/nats-js-reference.md` and the applicable ADR under `docs/architecture/`.
- **Third-party precedent:** read `docs/research/ecosystem-survey.md`; treat it as comparison
  evidence, never protocol authority.
- **Effect architecture rationale:** read `docs/research/effect-native-client-architecture.md`.

## Laws

1. **Design gate.** A new public export, service method, error variant, encoded type, compatibility
   promise, or runtime promise requires an accepted ADR. Experiments remain internal.
2. **Source precedence.** Pinned NATS protocol/server and official client source > conformance and
   integration observations > official docs > third-party projects. Record disagreements.
3. **Scope owns lifetime.** Connections, readers, writers, subscriptions, requests, reconnect
   loops, heartbeats, and drains acquire and finalize within `Scope`. Layers finish acquisition;
   their long-lived fibers use scoped forks.
4. **Stream handling is the first implementation gate.** Every producer/consumer boundary names
   its capacity, unit, overflow and terminal ordering, producer suspension authority, and release
   behavior. Public subscriptions expose pull-based `Stream`; private callbacks bridge through a
   bounded `Queue`, `PubSub`, or `SubscriptionRef` chosen for the delivery contract.
5. **Typed boundaries.** Decode untrusted wire/config/storage values, preserve protocol status and
   headers, and translate transport defects into operation-labeled typed errors without swallowing
   interruption.
6. **Truthful retry.** Use `Schedule` only at the narrowest idempotent boundary. Publish, request,
   reconnect, KV mutation, and object transfer each document their distinct retry consequences.
7. **Layered tests.** Pure protocol tests use bytes and deterministic state machines; Effect tests
   use fakes and test services; server integration tests run separately against declared NATS
   versions. Time and concurrency tests use `TestClock`, `Deferred`, `Queue`, or `Latch`.
8. **No accidental endorsement.** The repository may target upstream quality, but documentation
   states its actual governance and release status until it is accepted by the NATS project.

## Change workflow

1. State the protocol or API contract and its evidence.
2. Add a failing unit, type, conformance, or integration test at the narrowest layer.
3. Implement through internal modules; export only ADR-approved surface.
4. Run `bun run format:check`, `bun run lint`, `bun run check`, and `bun run test`.
5. Report which claims are tested locally, tested against a server, assumed, or still undecided.

A change is complete when all four checks pass, public-surface changes cite an ADR, lifecycle and
backpressure behavior are test-covered, and no unrelated working-tree changes are included.
