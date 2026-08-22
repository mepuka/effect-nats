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

Draft workbench material (not decisions): [kv-slice-scoping.md](kv-slice-scoping.md)
