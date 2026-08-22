---
name: effect-nats-implementation
description: Implement or review Effect v4 code in effect-nats with gates for fidelity to the pinned first-party Effect client and resource patterns, scoped NATS lifecycle, explicit backpressure, typed protocol failures, truthful retry, and deterministic tests. Use for NATS Core, KV, Object Store, transport, codec, connection, retry, public API, or test changes. Do not use for research-only prose.
---

# effect-nats implementation

Implement the smallest ADR-approved slice while preserving the semantics of NATS and the idioms of
the pinned Effect v4 tree. Treat this skill as a change gate, not a source of protocol truth.

## Read first

1. Read the closest `AGENTS.md` and `docs/STYLE.md`.
2. Read the accepted ADR governing the requested public contract. If none exists, keep the work
   internal or stop before adding a public export.
3. Read the applicable parts of:
   - `docs/research/official-effect-client-patterns.md` for first-party Effect analogues;
   - `docs/research/nats-js-reference.md` for official NATS behavior;
   - `docs/research/effect-native-client-architecture.md` for this repository's architecture;
   - `docs/research/ecosystem-survey.md` only for comparative evidence.
4. Read [references/testing-gates.md](references/testing-gates.md) before adding or changing tests,
   lifecycle, concurrency, retry, streaming, KV, or Object Store behavior.

If a referenced document does not exist or does not cover the requested behavior, record that gap.
Do not manufacture an API from analogy alone.

## Source precedence

Use sources in this order:

1. the pinned NATS protocol, server, and official client source;
2. observations from declared server-version conformance tests;
3. the pinned Effect v4 source tree;
4. accepted repository ADRs and verified research notes;
5. third-party clients and examples.

The Effect tree decides implementation style; it does not redefine NATS semantics. The NATS tree
decides protocol semantics; it does not decide the Effect API shape.

## Classify the operation before coding

Write down the operation's shape and choose the narrowest matching Effect abstraction:

| Shape                                  | Default abstraction                     |
| -------------------------------------- | --------------------------------------- |
| One result or one command              | `Effect`                                |
| Owned long-lived capability            | `Context.Service` acquired by a `Layer` |
| Multiple values over time              | pull-based `Stream`                     |
| One-consumer callback handoff          | bounded `Queue`                         |
| True fan-out                           | bounded `PubSub`                        |
| Current state plus changes             | `SubscriptionRef`                       |
| A set of scope-owned workers           | `FiberSet` or `FiberMap`                |
| External configuration                 | `Config` decoded into concrete options  |
| Untrusted JSON/config/storage boundary | `Schema`                                |
| Byte protocol parser state             | explicit internal state machine         |

Do not use `Stream`, `Queue`, `PubSub`, `Schema`, or a service tag merely for consistency. The
operation's cardinality, ownership, fan-out, and trust boundary must justify the module.

## Retrieve an official analogue

Before implementation, select at least one relevant first-party analogue from
`docs/research/official-effect-client-patterns.md` and open the cited source. Record:

- the exact pinned file and line range inspected;
- the lifecycle, error, stream, retry, or testing pattern being reused;
- any part deliberately not copied and why NATS requires a different contract.

Typical starting points in the pinned Effect tree are:

- `packages/effect/src/unstable/socket/Socket.ts` for scoped socket ownership, writers, channels,
  socket errors, and `FiberSet` supervision;
- `packages/platform/node-shared/src/NodeSocket.ts` for a runtime transport adapter behind the
  common socket capability;
- `packages/effect/src/unstable/rpc/RpcClient.ts` for multiplexed requests, bounded per-stream
  queues, scoped background work, and transport retry policy;
- `packages/effect/src/unstable/persistence/KeyValueStore.ts` for service construction and typed
  storage failure shape, but not for NATS KV revision/watch semantics;
- first-party tests beside those modules for scope, interruption, retry, and deterministic
  synchronization patterns.

Never copy an unstable module wholesale. Reuse the smallest proven construction that matches the
declared contract and compile it against the exact repository pin.

## Implementation gates

### 1. Contract gate

- Do not add a public export, service method, error variant, encoded type, runtime promise, or
  compatibility claim without an accepted ADR.
- Keep experiments under internal modules.
- Preserve absence versus empty, Core publish versus JetStream acknowledgement, KV revision
  preconditions, and Object Store metadata/chunk completion.

### 2. Effect-fidelity gate

- Model owned capabilities as services supplied by layers; acquire live resources in `Scope`.
- Register finalizers before returning a usable connection.
- Run readers, reconnect loops, heartbeats, dispatchers, and transfer workers with scoped forks,
  `FiberSet`, or `FiberMap`. Never detach a raw promise or fiber.
- Keep one-shot operations as `Effect` and ongoing delivery as `Stream`.
- Use the pinned v4 API names. At the current pin, expected public failures use
  `Schema.TaggedError`, not examples from other releases such as `Schema.TaggedErrorClass`.
- Wrap unavoidable promise/callback boundaries once in the transport adapter with `Effect.async`,
  `Effect.tryPromise`, or an equivalent first-party bridge. Do not spread `async`/`await` through
  Effect workflows.
- Preserve interruption. Cause-level recovery belongs only in a named supervisor.

### 3. Backpressure and lifecycle gate

- Name every queue or buffer's capacity, overflow behavior, ownership, and shutdown behavior.
- Do not assume `Stream.fromAsyncIterable` bounds an upstream producer.
- A subscription's finalizer must unsubscribe or drain according to its declared contract.
- Drain stops admission, waits only for bounded owned work, flushes when meaningful, and has a
  tested forced-close path.
- Preserve per-subscription ordering unless the API explicitly opts into bounded unordered work.
- Reconnect may restore transport state; it must not silently strengthen Core delivery semantics.

### 4. Error and retry gate

- Decode untrusted structured boundaries and parse wire bytes with explicit size limits.
- Return typed, operation-labelled expected failures; do not leak credentials in errors or spans.
- Keep configuration, transport, protocol, authorization, timeout, no-responder, JetStream API,
  KV conflict, and object-integrity errors distinct when callers can recover differently.
- Retry at the narrowest idempotent boundary with an operation-specific `Schedule`.
- Never retry Core publish as though it were exactly-once. Publish retry requires an explicit
  duplicate-risk policy or stable server-supported deduplication identity.
- KV mutation retry must preserve its expected revision. Object transfer retry must preserve
  chunk, metadata, and digest invariants.

### 5. Test gate

- Add the narrowest failing test before implementation.
- Use `it.effect` for Effect behavior and deterministic controls such as `TestClock`, `Deferred`,
  `Queue`, or `Latch`. Do not use timing sleeps as synchronization.
- Test interruption and finalization, not only successful values.
- Add server integration coverage for behavior whose truth depends on NATS server state or version.
- Follow the detailed matrix in `references/testing-gates.md`.

## Required workflow

1. State the contract, evidence, operation shape, and selected official analogue.
2. Add a failing unit, type, conformance, or integration test at the narrowest layer.
3. Implement through internal modules with explicit resource ownership and buffer policy.
4. Re-open the analogue and compare the completed code for service, layer, scope, error, stream,
   and test fidelity. Explain intentional deviations.
5. Run:

   ```text
   pnpm format:check
   pnpm lint
   pnpm check
   pnpm test
   ```

6. Run the applicable NATS integration/conformance suite when server semantics are involved.
7. Produce a fidelity receipt in the handoff or PR description:
   - contract/ADR;
   - NATS authority inspected;
   - pinned Effect analogue inspected;
   - modules selected and rejected;
   - lifecycle and buffer policy;
   - tests run and server versions observed;
   - assumptions, untested claims, and remaining risks.

## Stop conditions

Stop and request a design decision when:

- no accepted ADR permits the requested public contract;
- NATS and Effect analogues imply incompatible semantics;
- queue capacity or overflow behavior is unknowable from the contract;
- retry safety depends on an unstated idempotency guarantee;
- a server-dependent claim cannot be tested against a declared version;
- implementing the slice would require broad unsafe casts, detached work, or swallowed defects.

Do not weaken the contract or tests to make an implementation pass.
