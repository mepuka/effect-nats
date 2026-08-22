# ADR-0002: Wrap the official client first

Status: accepted, 2026-08-21. Ruled by the project owner in-session.

## Decision

The first shipping implementation is a strict Effect semantic layer over the official public
`@nats-io/*` packages (Core, JetStream, KV). Wire reimplementation remains a separately tested
refinement target behind the internal substrate seams; nothing in the public API may leak the
adapter choice. This follows the pinned research recommendation
([nats-js-reference.md](../research/nats-js-reference.md), primary result) and prioritizes
shipping over interface cleverness; richer interfaces layer on later without breaking the
substrate seam.

Guardrails that make wrapping safe, all already established:

- Naming validation runs in this library's branded boundary before any upstream call, so the
  upstream validation gaps recorded in
  [jetstream-kv-bucket-naming.md](../research/jetstream-kv-bucket-naming.md) are never
  reachable.
- Subscription-like sources bridge via callback-to-bounded-Queue, never the upstream async
  iterators (stream-discipline law 11, case S6).
- Reconnection: the wrapped client's reconnect loop is the single loop of ADR-0001 decision 1;
  the connection supervisor configures it through the typed options of ADR-0001 and translates
  its status events. No Effect retry layer stacks on top.
- KV sources/mirrors stay unexposed (scoping exclusion) while the verified nats.js
  cross-account sourcing defect and ADR-57 conformance gap stand upstream.

Revisit triggers for the wire target: upstream governance/dependency policy conflict, a
measured performance ceiling attributable to the wrapper, or a protocol behavior the public
packages cannot express.

## Implementation seam

The transport slice realizes this decision as an internal structural `Connection` seam
([src/internal/NatsTransport.ts](../../src/internal/NatsTransport.ts)) naming the exact subset of
the official surface this client depends on; official packages are acquired inside a Layer scope
and official types never leak above the seam. (Absorbed from the transport branch's proposed
wrap ADR, which this accepted ADR supersedes.) Connect/drain supervision details are ADR-0007.

## Consequences

Workbench decision items 1 and 5 are discharged for the first implementation pass; the
substrate interface is the compatibility contract a future wire implementation must satisfy.
