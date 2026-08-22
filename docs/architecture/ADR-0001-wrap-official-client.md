# ADR-0001: Wrap the official @nats-io client behind an internal transport seam

Status: Proposed

## Context

The repository must choose how Core NATS reaches the wire before any public
contract is frozen:

1. wrap the official TypeScript packages (`@nats-io/nats-core`,
   `@nats-io/transport-node`) behind an internal Effect seam; or
2. implement the NATS wire protocol directly (own parser, reconnect, auth).

`docs/research/nats-js-reference.md` recommends option 1 as the first
milestone and keeps protocol authority upstream. The ecosystem survey shows no
third-party project provides a complete blueprint, so neither choice can be
borrowed wholesale.

## Decision

Wrap the official public client contracts. An internal, structural
`Connection` seam (`src/internal/NatsTransport.ts`) names the exact
subset of the official surface this client depends on; official packages are
acquired inside a Layer scope; Effect services, typed errors, bounded queues,
and streams live above the seam and never leak official types.

Everything remains internal until accepted ADRs promote it. This decision
becomes binding only on acceptance of this ADR together with the follow-up
decisions it defers (runtime promise, buffer capacities, drain semantics,
error taxonomy promotion).

## Evidence

- Installed at decision time: `@nats-io/nats-core@3.4.0`,
  `@nats-io/transport-node@3.4.0`; the wrapped surface was verified against
  `node_modules/@nats-io/nats-core/lib/core.d.ts:305-460`.
- Subscription delivery must not be bridged through generic async iteration:
  evidence tests pin that `Stream.fromAsyncIterable` finalization awaits
  `iterator.return()` while interruption blocks, and that the official
  `QueuedIteratorImpl` cannot settle `return()` while a pull pends until
  `stop()` runs, with unbounded upstream buffering
  (`test/internal/iteratorBridgeEvidence.test.ts`). Subscriptions will bridge
  callback-to-bounded-Queue unless a future adapter passes the per-adapter
  proof obligation.
- Lifecycle semantics implemented so far (bounded drain attempt then forced
  close) are unit-tested against a fake boundary; none are server-compatibility
  claims yet.

## Consequences

- Protocol truth stays upstream; wire-level conformance work is inherited, not
  duplicated.
- Client-side iterator/transport defects upstream become ours to detect;
  behavior-pinning evidence tests guard the seams we depend on.
- A later wire implementation remains possible as a separately tested
  refinement behind the same seam.
