# Agent Protocol implementation

The root and `packages/agent/AGENTS.md` files apply. This subtree implements a derived wire-contract
model, caller, agent host, adapters, and tests; it does not own protocol semantics.

## Protocol invariants

- Endpoint names are fixed; prompt/status subjects are discovered and used verbatim. Only the
  heartbeat subject is protocol-fixed.
- Validate final UTF-8 wire bytes and endpoint capabilities before publish.
- Every accepted response stream emits one `ack` first, one zero-byte headerless terminator last,
  and nothing after it. Errors precede the terminator.
- The inactivity timeout resets after every observed message. Interruption, timeout, query failure,
  and handler defects run cleanup exactly once.
- Accept and preserve unknown extensibility fields/chunk types as specified. Invalid boundary
  values fail in the typed error channel.
- Heartbeats start after registration; periodic and status payloads share one builder.
- Every queue and concurrent activity has an explicit bound, overflow behavior, and scoped owner.
  Broker I/O must not wait on arbitrary user callbacks.

## Effect-native shape

Prefer `Schema` at untrusted boundaries, `Effect.fn`/`Effect.gen` for workflows,
`Context.Service` plus `Layer.effect` for capabilities, `Stream` for sequences, and scoped
resources for subscriptions/fibers. Use native `Queue`, `PubSub`, `Deferred`, `Latch`, `Schedule`,
and `TestClock`; expected failures stay in `E`, while defects are not protocol errors.

Verify the pinned rc.111 APIs in source. In particular, do not import shapes from another Effect
release. Do not use `as any`, unchecked assertion chains, non-null assertions, swallowed stream
failures, real sleeps in tests, or manual pull loops without pinned-source evidence that a native
combinator cannot preserve the required semantics.

## Performance and allocation

Decode each payload once. Keep subject matching/message classification O(tokens/bytes), per-message
allocation and retention bounded, and slow-consumer policy explicit. Hoist stable schemas, codecs,
and constant bytes. A plausible hot-path regression needs a representative benchmark or complexity
assertion after invariant tests are green.

## Change rule

For each affected protocol requirement ID, add or update executable evidence through a public or
wire boundary. Local bridge behavior and extension behavior use their own accepted specification
and evidence ledger; they do not acquire upstream requirement IDs merely by reusing protocol code.
