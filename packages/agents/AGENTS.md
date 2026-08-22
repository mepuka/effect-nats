# Synadia Agent Protocol adapter engineering contract

The root `AGENTS.md` applies first. This file adds the rules specific to
`packages/agents`.

This package independently implements the Synadia Agent Protocol for NATS v0.3 with
`effect@4.0.0-rc.111`. Correct wire behavior comes before convenience or API
surface area. Passing examples alone is not conformance.

The protocol is external semantic space owned by Synadia. Read `CONTEXT.md` and use its canonical
terms. Never describe local codecs, state machines, extensions, or Effect APIs as defining or
improving the protocol.

## Authority and required reading

Use these sources in order. A lower source may clarify, but never weaken, a
higher one.

1. `docs/upstream/core-protocol-v0.3.0.md` is the vendored wire contract.
2. `CONTEXT.md` mirrors upstream vocabulary and separates it from local terms.
3. `docs/01-SPEC.md` contains local implementation requirements; it cannot amend the protocol.
4. The workspace-installed `effect/src` is authoritative for the pinned Effect API.
5. Root research is comparison evidence only; verify a pattern before adapting it.

Before changing protocol, discovery, heartbeat, streaming, errors, or wire
schemas, read the affected upstream sections and
`docs/conformance/README.md`. Before choosing an Effect combinator, inspect its
signature and tests in the pinned package source. Do not code from memory of a
different Effect release.

## Invariants

Make protocol invariants explicit in types, Schemas, state machines, and tests:

- Endpoint names are fixed; prompt/status subjects are discovered, never
  reconstructed. Only the heartbeat subject is protocol-fixed.
- Validate final UTF-8 wire bytes and endpoint capabilities before publish.
- Every response stream emits one `ack` first, one zero-byte headerless
  terminator last, and nothing after it. Stream errors precede the terminator.
- The inactivity timeout resets after every observed message. Interruption,
  timeout, query failure, and handler defects run cleanup exactly once.
- Unknown extensibility fields and chunk types are accepted as specified and
  preserved by relays. Invalid boundary values fail in the typed error channel.
- Heartbeats start after registration and periodic/status payloads share one
  builder so common fields cannot drift.
- All queues and concurrent work have explicit bounds, overflow behavior, and
  scoped ownership. Broker I/O must not wait on arbitrary user callbacks.

## Effect-native implementation

Prefer small declarative Effect programs: `Schema` at untrusted boundaries,
`Effect.fn`/`Effect.gen` for workflows, `Context.Service` plus `Layer.effect`
for capabilities, `Stream` combinators for streaming, and scoped resources for
subscriptions and fibers. Use `Queue`, `PubSub`, `Deferred`, `Latch`, `Schedule`,
and `TestClock` rather than ad-hoc promises, timers, or mutable coordination.
Keep expected failures in the `E` channel; defects are not protocol errors.

For the pinned `4.0.0-rc.111` release, verify and use the actual shapes,
including `Schema.TaggedError`, `Schema.Union([members])`,
`Schema.Record(key, value)`, `Schema.StructWithRest`, `Stream.unwrap`, and
`Stream.timeoutOrElse({ duration, orElse: () => ... })`. Later Effect examples
that use renamed APIs are not evidence for this repository.

Do not use `as any`, unchecked assertion chains, non-null assertions, swallowed
stream failures, real sleeps in tests, or manual pull loops unless pinned source
demonstrates that no native combinator preserves the required semantics. Hoist
Schemas, encoders, decoders, and constant byte sequences out of hot paths.

## Performance contract

Correctness fixes must not introduce avoidable copying or unbounded retention.
Subject matching and message classification must be O(tokens/bytes) with no
recursive array slicing. Decode each wire payload once. Keep per-message
allocation bounded, preserve backpressure, and isolate slow consumers with an
explicit bounded policy. Measure hot-path changes with representative payloads;
retain a regression benchmark or complexity assertion when a regression is
plausible. Optimize only after the invariant tests are green.

## Conformance workflow

Every upstream table row, ordered procedure, RFC 2119/8174 clause, exact wire
rule, and §12 checklist item has a stable ID in
`docs/conformance/protocol-requirements.json`.

For each affected ID:

1. Add executable evidence through a public boundary and list the exact test
   file/name in the catalog. A `SHOULD` needs a test or a documented deviation
   with interoperability risk; an implemented `MAY` needs enabled/disabled tests.
2. Cover success, boundary, malformed input, failure, interruption, and cleanup
   where applicable. Every bug gets a regression test.
3. Use property tests (`effect/testing/FastCheck` or `TestSchema`) for schemas and
   parsers; model/DFA tests for ordering and terminal states; deterministic
   `TestClock` tests for cadence/timeouts; and real local `nats-server`
   integration/differential fixtures for broker and cross-SDK behavior.
4. Do not declare completion while any impacted catalog entry is `unmapped`, any
   conformance test is skipped/todo, or only example tests exist.

Required gates are: vendored-spec integrity, catalog extraction/traceability,
all conformance tests, `bun run test:agents`, and `bun run check:agents` from the
monorepo root. A full-protocol completion claim requires zero unmapped
requirements, no accidental Agent SDK dependency, and a real NATS integration
run. Record justified deviations next to their requirement IDs rather than
weakening a test.

## Extension boundary

Non-standard durability, cancellation, control-plane, orchestration, and work-queue designs must be
documented as local extension protocols outside Synadia's reserved subjects. They require explicit
capability negotiation, vanilla-peer fallback, and their own ADR and test ledger. They never count
toward the upstream conformance catalog and must not change standard v0.3 endpoint or reply-stream
semantics.
