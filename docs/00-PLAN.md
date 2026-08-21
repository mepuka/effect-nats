# 0000 — Plan: `effect-agents` — an Effect-native Agent Protocol runtime

Status: active · Supersedes the wrapper idea (notes/0006 amended): we do NOT wrap
`@synadia-ai/*`; we implement the [Agent Protocol for NATS v0.3](https://github.com/synadia-agent-sdk-docs/core-protocol.md)
natively in Effect, wire-compatible where it matters, better where it counts.
Their code may be consulted/reused piecewise (Apache-2.0), but it is not a dependency.

## Why native

The protocol's own deferrals are our design space:

| Protocol gap (v0.3) | Our answer |
|---|---|
| Core-NATS-only responses: chunks are at-most-once, a lost terminator hangs callers until inactivity timeout (§6.6) | Dual transport: core NATS for interactive streams + **JetStream-backed durable mode** (dedup id per chunk-sequence, terminator committed once) |
| No cancellation signal (§6.7) — agents burn tokens for abandoned callers | Control-plane subjects + KV lease: cancel propagates to the handler Effect as fiber interruption; mid-stream query doubles as liveness probe |
| Untyped wire (JSON blobs, stringly `max_payload`) | effect/`Schema` end-to-end internally; wire stays byte-compatible |
| Errors as headers + ad-hoc JSON body (§9) | Mirrored `Schema.TaggedError` taxonomy; E channel is inhabited by construction |
| Heartbeat death is silent | Supervised repeat loops; liveness is a reactive Cell |

## Modules

```
effect-agents/
  protocol/    # pure: subject algebra, envelope/chunk schemas, validation rules (§5.3, §5.4)
  transport/   # core-nats interpreter + jetstream interpreter behind one interface
  host/        # AgentHost: registration ($SRV micro API), prompt handler pump,
               # heartbeats, status endpoint — all Effects, scoped lifecycle
  client/      # AgentClient: discover/prompt/liveness; async-iterable ⇄ Stream bridges
  queue/       # typed decentralized work queue on JetStream WQ retention + pull consumers
  orchestrate/ # scatter-gather, budgets (Schedule admission), supervision trees, sagas
```

## Slices

1. **P0 — protocol kernel** (pure, no I/O): subject algebra (verb-first, §2),
   envelope discrimination rule (§5.3), local validation (§5.4), chunk schema +
   terminator model (§6). Property-testable against the spec text. Lean can
   referee the subject algebra (we already have the token/refinement machinery).
2. **P1 — connection + client read path**: scoped `NatsConnection` Layer;
   `prompt()` as interruptible `Stream`; inactivity timeout; error-header
   decoding; tested against a local `nats-server`.
3. **P2 — host path**: `AgentHost` with Effect handlers; mandatory-first `ack`
   chunk (§6.4); terminator exactly-once; heartbeats + status endpoint;
   echo agent, then an `effect/ai` `LanguageModel` agent.
4. **P3 — durable transport**: JetStream response mode; dedup ids; cancellation
   control plane.
5. **P4 — work queue + orchestration**: the coordination playground opens.

## Compliance stance

Wire-compliant caller AND host per the §12 checklist, with additive extensions
that degrade gracefully against vanilla peers (they simply don't subscribe to
our control plane).
