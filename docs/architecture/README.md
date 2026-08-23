# Architecture workbench

Public API work begins after the first decision pass records:

1. runtime and transport support;
2. protocol parser/writer ownership and compatibility targets;
3. scoped connection, subscription, request, drain, and reconnect lifecycles;
4. Core publish/subscribe/request semantics and backpressure;
5. the internal JetStream primitives required by KV and Object Store;
6. codecs, headers, status messages, typed failures, and observability;
7. deterministic unit, protocol-conformance, integration, and fault tests;
8. package, release, compatibility, and upstream-governance policy.

The initial package exports nothing. Research notes under `../research/` inform the decision pass;
accepted decisions become numbered ADRs here before they constrain the implementation.

Accepted ADRs:

- [ADR-0001: Reconnect limits and policy](adr-0001-reconnect-limits.md)
- [ADR-0002: Wrap the official client first](adr-0002-wrap-official-client.md)
- [ADR-0003: KV naming strictness](adr-0003-kv-naming-strictness.md)
- [ADR-0004: Revision and sequence representation](adr-0004-revision-representation.md)
- [ADR-0005: Configuration layers and buffer honesty](adr-0005-configuration-layers.md)
- [ADR-0007: Connection lifecycle policy](adr-0007-connection-lifecycle-policy.md) — connect
  retry/drain supervision from the transport branch; composes with ADR-0001 (reconnect) and
  ADR-0002 (wrap seam); its deferred subscription-queue decision is discharged by ADR-0005
- [ADR-0008: Consume resume and buffer semantics](adr-0008-consume-resume-and-buffer-semantics.md)
- [ADR-0009: Live adapter observation fidelity](adr-0009-live-adapter-observation-fidelity.md)

Proposed ADRs (not accepted decisions):

- [ADR-0006: Object Store stream and Schema boundary](adr-0006-object-store-stream-and-schema-boundary.md)

Draft workbench material (not decisions):

- [ADR-0001 implementation and conformance status](adr-0001-conformance-status.md)
- [KV slice scoping](kv-slice-scoping.md)
- [Object Store slice scoping](object-store-slice-scoping.md)
- [Lean trace replay brief](lean-trace-replay-brief.md)
- [Live-adapter observation fidelity brief](live-adapter-observation-fidelity-brief.md)
- [Lean subscriber-trace replay brief](lean-subscriber-trace-replay-brief.md) — dispatched 2026-08-23, immutable
- [Lean subscriber-trace replay follow-up brief](lean-subscriber-trace-replay-followup-brief.md) — dispatched 2026-08-23, immutable
- [Test hardening brief](test-hardening-brief.md) — draft, not dispatched

Research evidence (not decisions):

- [Lean sequential-trace live replay findings](../research/lean-trace-live-findings.md)
