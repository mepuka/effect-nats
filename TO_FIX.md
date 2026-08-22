# To fix

Reviewed baseline: `49ad0ec`
Reviewed HEAD: `847b8a2`

## P1 — missing outcome or Effect/stream contract violations

- [ ] `.agents/skills/effect4-stream-discipline/SKILL.md:216` — replace the absolute prohibition on
      async-iterator subscription bridges with an adapter-specific proof obligation.
  - **Official behavior and source:** official NATS.js transports, subscriptions, and status feeds
    are async iterables (`docs/research/nats-js-reference.md:37,57`). The implementation gate stops
    when the callback/iterator choice changes suspension or buffering
    (`.agents/skills/effect-nats-implementation/SKILL.md:208`).
  - **Why it fails:** one demonstrated NATS.js cancellation deadlock is generalized into a universal
    rule, potentially rejecting a verified iterator adapter or the official transport seam before
    an ADR chooses wrap versus wire implementation.
  - **Required correction:** require proof of cancellation, finalization, and upstream buffering for
    each concrete iterator. Prefer callback-to-bounded-Queue for the known NATS.js subscription
    hazard, but retain async iteration when its lifecycle is verified.
  - **Proof/test gate:** against the real adapter, interrupt a pending pull and prove bounded
    completion, exactly-once unsubscribe/return, no post-release delivery, and no hidden growable
    upstream queue.

- [ ] `.agents/skills/effect4-stream-discipline/SKILL.md:178` and
      `.agents/skills/effect4-stream-discipline/references/effect4-api-notes.md:97-99` — replace the
      universal supervisor that retries every failure and normal completion forever.
  - **Official behavior and source:** pinned Effect `Schedule.spaced` has no stop condition
    (`effect@648f566d:packages/effect/src/Schedule.ts:1198-1200`). The analogous RPC client uses
    this unbounded policy at
    `effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:1163-1166`; the repository's
    verified
    pattern map explicitly says not to copy it
    (`docs/research/official-effect-client-patterns.md:386`). NATS reconnect supports a
    maximum-attempt policy (`docs/research/nats-js-reference.md:53`).
  - **Why it fails:** fatal authorization, protocol, configuration, or invariant errors could loop
    permanently; exhaustion is never observable; normal completion is always treated as failure.
  - **Required correction:** classify retryable typed reasons, bound and jitter policies by default,
    expose exhaustion, and permit continuous reconnect only through explicit configuration or ADR
    with observable status.
  - **Proof/test gate:** use `TestClock` to prove transient retry timing, fatal-error non-retry, exact
    exhaustion, schedule reset, interruption, and scope finalization.

- [ ] `.agents/skills/tdd/SKILL.md:14,20-22` — define test seams so they include agreed internal
      protocol boundaries without requiring package-public exports.
  - **Official behavior and source:** the official NATS parser is an internal byte-oriented state
    machine (`docs/research/nats-js-reference.md:45`). This repository intentionally keeps
    parser/transport machinery internal (`docs/STYLE.md:9`) while requiring parser tests
    (`docs/STYLE.md:76`) and exporting nothing until ADR approval
    (`docs/architecture/README.md:14`).
  - **Why it fails:** the TDD gate can block necessary parser/transport tests or pressure an agent
    to expose internals prematurely.
  - **Required correction:** define a seam as an agreed observable module boundary, not necessarily a
    package-public export. Allow ADR/task-approved internal protocol and adapter seams while still
    prohibiting private-function and implementation-coupled assertions.
  - **Proof/test gate:** forward-test the skill with a split-frame parser bug before any public API
    exists; the agent must select an internal parser-module seam without modifying `src/index.ts`.

## Watch next

- [ ] `docs/architecture/README.md:3-14` — no ADR yet chooses wrapped official client versus wire
      implementation, producer suspension authority, queue capacities, overflow terminal ordering, or
      restoration behavior. Any implementation encoding those choices should become a confirmed
      finding immediately.
- [ ] `.agents/skills/effect4-stream-discipline/` and
      `.claude/skills/effect4-stream-discipline/` are manually mirrored; watch for semantic drift
      between agent environments.

## Outcome coverage

- **Review-discipline change:** inspected TDD invocation, stream-first gating, NATS-authority
  disclaimer, bounded-buffer requirements, interruption/finalization checks, and outcome-ledger
  requirements.
- **NATS runtime:** no runtime operation or public API changed, so runtime success, wire response,
  transport, KV, Object Store, cancellation, reconnect, and terminal-stream branches are not yet
  implementable or testable.
- **Explicitly not inspected against a live server:** none of the reviewed changes execute NATS
  behavior.

## Verification

- `pnpm format:check` — passed.
- `pnpm lint` — passed.
- `pnpm check` — passed.
- `pnpm test` — one file and one test passed.
- All three repository skills passed structural validation.
- Working tree was clean; `main` matched `origin/main` at the reviewed HEAD.
- GitHub CI for `847b8a2` passed.
- Inspected pinned NATS.js `3a7b736…`, Effect `4.0.0-rc.111`, repository style rules, architecture
  gate, implementation skill, TDD skill, and stream-discipline skill.
- No accepted architecture ADR exists yet.
