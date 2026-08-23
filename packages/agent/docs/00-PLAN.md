# 0000 — Implementation plan for `effect-nats-agents`

Status: active

This package independently implements the
[Synadia Agent Protocol for NATS v0.3](https://github.com/synadia-ai/synadia-agent-sdk-docs/blob/d907f5ca080528ee5e9e1385f0591c42f7fdb9b8/core-protocol.md)
with Effect. The upstream specification owns the wire contract. The
[official SDK repository](https://github.com/synadia-ai/synadia-agents) is the primary
implementation comparison; this package does not wrap `@synadia-ai/*` at runtime.

Exact standard behavior comes first. Local Effect APIs may improve type safety, resource safety,
and testability, but they may not weaken, reinterpret, or selectively implement protocol
requirements.

## Modules

```text
packages/agent/
  src/agents/protocol/  # protocol-derived subjects, codecs, validators, reply DFA
  src/agents/           # Wire seam, caller service, agent-host service, prompt engine
  src/test/             # conformance, state-machine, lifecycle, memory, live-broker evidence
  docs/upstream/        # immutable vendored protocol and provenance
  docs/conformance/     # requirement ledger derived from the vendored protocol
```

## Delivery slices

1. **P0 — wire-contract model**: subject parsing, envelope discrimination, local validation,
   chunk decoding, and reply-sequence checking derived from the pinned protocol.
2. **P1 — caller path**: scoped NATS connection, discovery, heartbeat liveness, prompt Stream,
   inactivity timeout, error-header decoding, and interruption-safe subscription release.
3. **P2 — agent-host path**: `AgentHost` with Effect handlers, protocol registration, ack-first
   accepted replies, exactly-once terminator emission, heartbeats, status, `$SRV` responders, and
   bounded admission.
4. **P3 — interoperability hardening**: native-broker tests plus differential tests against the
   official TypeScript/Python caller and host SDKs.

## Conformance stance

Conformance means the caller and agent-host behavior passes the upstream §12 checklist and the
repository's strict requirement ledger. The implementation must publish and consume standard
v0.3 frames exactly as specified and must tolerate upstream extension points exactly where the
protocol requires.

JetStream-backed replies, cancellation signals, work queues, orchestration, and control-plane
subjects are not v0.3 behavior. Research for those capabilities lives in
[`extensions/durable-cancellation-workbench.md`](extensions/durable-cancellation-workbench.md)
and cannot count as protocol conformance or alter vanilla-peer behavior.
