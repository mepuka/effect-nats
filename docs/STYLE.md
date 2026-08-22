# Effect-native implementation style

This file governs implementation shape. Protocol meaning and public API shape belong in ADRs.

## Modules and visibility

- Keep public modules domain-first and small: Core, KV, Object Store, codecs, and explicitly
  approved shared types.
- Keep parser state, protocol operations, muxing, inbox generation, transport adapters, JetStream
  request subjects, and compatibility shims internal until an ADR promotes them.
- Export only intentional package surface from `src/index.ts`; package exports are compatibility
  commitments.
- Prefer one canonical module identity and ordinary ES modules. Avoid TypeScript namespaces.

## Data and wire contracts

- Model boundary records with `Schema.Struct` plus a same-name interface.
- Model constrained scalar identifiers and subjects with branded schemas after their grammar is
  specified.
- Use `Data.TaggedEnum` for internal parser, connection, subscription, and transfer states.
- Use `Schema.TaggedUnion` only when variants cross a wire, persistence, documentation, or public
  API boundary.
- Decode unknown JSON/config/status payloads with `Schema.decodeUnknownEffect`; parse the NATS text
  and binary protocol in dedicated byte-level modules with explicit size bounds.
- Preserve the difference between absent, empty, zero, defaulted, and server-supplied values.

## Services, layers, and scope

- Define public capabilities with `Context.Service`; define public and non-trivial internal methods
  with `Effect.fn("Module.operation")`.
- Build real implementations with `Layer.effect` and explicit transport/config dependencies.
- Acquire sockets and protocol machinery with scoped effects. Register finalizers before exposing a
  usable connection.
- Own read loops, reconnect loops, ping timers, subscription dispatchers, and object-transfer
  workers in the acquiring scope with `Effect.forkScoped`, `FiberSet`, or `FiberMap`.
- Make drain a modeled state transition that stops admission, waits for bounded work, flushes when
  the protocol allows, and closes through the same finalizer path.
- Keep transport authority visible. Node TCP/TLS, WebSocket, and future runtime adapters provide a
  narrow internal transport service rather than conditionals spread through Core logic.

## Streams and backpressure

- Expose message sequences as `Stream`; callers control demand and interruption.
- Use `Queue` for one-consumer handoff, `PubSub` for fan-out, and `SubscriptionRef` for current
  connection status plus changes.
- Start with natural pull backpressure. Any buffer states capacity, suspend/drop/slide behavior,
  and how that choice relates to NATS slow-consumer semantics.
- Bound request mux, pending publish, reconnect, parser, subscription, KV watch, and object chunk
  queues independently.
- Preserve per-subscription ordering unless an API explicitly opts into unordered bounded
  concurrency.

## Failures, interruption, and retry

- Define expected failures with the pinned v4 `Schema.TaggedError`; include the operation and relevant
  subject, stream, bucket, object, or server context without leaking credentials.
- Separate configuration, transport, protocol, authorization, timeout, no-responder, JetStream API,
  KV conflict, and object-integrity failures when callers can act differently.
- Preserve interruption through broad transport/parser supervision. Cause-level recovery belongs
  only at a named supervisor boundary.
- Use `Schedule` for reconnect and retry policies. Bound and jitter retries; expose exhaustion.
- Retry only with an explicit idempotency argument. A reconnecting publisher does not silently
  become exactly-once, and KV compare-and-set retries must preserve the expected revision.

## Configuration and observability

- Accept concrete typed options in library layers and optionally provide `layerConfig` using
  Effect `Config`; library logic does not read `process.env` directly.
- Model credentials as redacted values and keep auth refresh as an explicit scoped service.
- Trace public operations with stable `Effect.fn` names. Annotate logs and spans with safe protocol
  identifiers and server observations.
- Expose connection status and operational events without making internal mutable state public.

## Tests

- Pure parser/encoder tests use byte fixtures, chunk boundaries, malformed frames, size limits, and
  round trips.
- State-machine tests cover connect, auth, ping/pong, reconnect, subscription, drain, and close.
- `it.effect` is the default Effect test form; use deterministic synchronization instead of sleeps.
- Test services expose controls and observations through a `TestService` backed by the same object
  as the production service tag.
- Integration suites declare server versions and features, create isolated resources, and clean
  them through scopes.
- KV tests cover revision conflicts, deletes/purges, watch initialization, history, TTL, and
  reconnect. Object Store tests cover chunking, metadata, digest verification, partial failure,
  resume policy, links, deletion, and cleanup.

## Upgrade discipline

Effect v4 is pinned exactly while it remains a release candidate. An upgrade changes the lockfile
in isolation and reruns type, lifecycle, interruption, and stream tests. NATS compatibility changes
record the server/client commit or released version used as evidence.

The pinned `4.0.0-rc.111` tree is API authority. General Effect examples that use a different
release name—such as `Schema.TaggedErrorClass` instead of this pin's `Schema.TaggedError`—must be
translated and compile-checked before use.

## Module and naming conventions

- One PascalCase module per concern under `src/internal/`, flat: the module name is the service
  or domain it owns (`NatsTransport.ts`, `JetStream.ts`, `Kv.ts`, `KvNaming.ts`, `Subject.ts`).
  Implementations of a contract get the contract's name plus a role suffix (`JetStreamMemory.ts`,
  `JetStreamLive.ts`).
- Services are `Context.Service` classes named like the module, with ids `effect-nats/<Module>`.
  Constructors are `make`; layers are `layer` or `layer<Role>`; a class-owned layer is a
  `static readonly layer`.
- Helpers exported from a module read as namespaced calls (`Subject.matches`), so exported names
  never repeat the module name.
- Test files mirror module names (`test/<Module>.test.ts`); shared test utilities are PascalCase
  non-test modules (`test/Kit.ts`). Performance suites carry `.perf.test.ts` and are excluded
  from default runs only via explicit scripts, never hidden.
