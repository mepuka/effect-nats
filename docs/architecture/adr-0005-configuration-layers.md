# ADR-0005: Configuration layers and buffer honesty

Status: accepted, 2026-08-21. Ruled by the project owner from the JetStream workflow-model
review delivered in-session; the review's corrections are binding guidance on this repository.

## Decision: four configuration layers

`effect-nats` does not expose one flat bag of NATS options. Configuration splits into four
layers, each with its own types, and the library compiles developer intent into raw NATS
configuration while explaining the resulting guarantees:

1. Runtime connection — reconnect policy (ADR-0001), offline-publish policy, finite
   message/byte capacities with explicit overflow behavior, drain timeout. The library derives
   official-client defaults, fatal/transient classification, and resolved status.
2. Resource provisioning — profile-first stream configuration: `KeyValueBucket`,
   `ObjectStoreBucket`, `WorkflowHistory`, `ActivityWorkQueue`, and expert-mode `RawJetStream`.
   The library derives compatible retention/discard/admin controls and server-version gates.
   Experimental capabilities (message schedules, atomic publish, fast ingest) are
   version-gated expert features, never silently enabled by a profile.
3. Operational envelope — expected rate, message-size distribution, worker count and
   concurrency scopes, processing latency, designed outage duration. The library derives
   buffer/storage estimates and overload warnings. The core printed warning: buffers absorb a
   bounded outage; they do not stabilize a system whose sustained arrival rate exceeds its
   slowest stage.
4. Observed deployment capabilities — operator-side limits (server/account ceilings, storage
   resources, placement, HA asset limits) are read or supplied, compared against the requested
   plan, and reported as diagnostics. They are never ordinary connection options.

Cross-cutting rules, binding on all layers:

- Numeric `0`/`-1` sentinels never appear in public types; they normalize to tagged variants
  (`Default`, `Unlimited`, `Finite` — consistent with ADR-0001's `"unbounded"`).
- Pull capacity is a discriminated union (message-window or byte-window, each with its refill
  threshold), never two simultaneous caps (ADR-37 consume design).
- Concurrency scopes are distinct types: worker instances, per-worker concurrency, fleet-wide
  in-flight bound, local prefetch window, and server `maxAckPending` (normally derived from
  the fleet-wide bound and checked against stream/account/server limits).
- Redelivery is one configuration object (ack wait or backoff schedule as the single
  authority, plus lease-extension policy), compiled into consumer config and Effect
  scheduling; the pieces are not independently exposed.
- Every acceptance boundary keeps its own name and result: Core local enqueue, Core flush,
  JetStream PubAck, KV revision commit, Object Store metadata commit, workflow receipt. A
  timeout after a write is `OutcomeUnknown`, never auto-reduced to failure; retrying requires
  a stable ID and reconciliation, and the broker duplicate window is finite.
- Named buffers stay distinct (publisher, reconnect, ingest, storage, prefetch, ack-pending);
  no `bufferSize` catch-all.
- History and work-queue resources are configured independently; a single-deployment model is
  not lifted into the client API. SQL outbox / edge WAL policy belongs in a workflow package,
  not this client.

## First governed change: replace `bufferCapacity`

The substrate's `bufferCapacity?: number` claimed "a full buffer suspends publishers" — true
for the memory interpreter, unimplementable for a live push producer the server drives.
Replaced with an explicit per-subscription policy:

- `PullWindow` — production is genuinely controllable upstream (a pull window, or an
  in-process producer); a full buffer suspends the producer. The live adapter may use this
  only where a real pull window exists.
- `TerminateOnLag` — the producer cannot be suspended; a full buffer terminates the
  subscription with a typed `ConsumerLagged` carrying the last delivered sequence, so the
  consumer can resume from a known position. Dropping/sliding is not offered; loss requires a
  future ADR.

KV watch, keys, and history run on `TerminateOnLag` and surface the overflow as a typed
`KvWatchLagged` outcome carrying the last observed revision — the gap is explicit and
resumable via `fromRevision`. Core push subscriptions (future slice) get finite message/byte
caps with a typed slow-consumer error by default.

## Consequences

- The substrate seam changes before the live adapter exists, which is the cheap moment.
- The scoping doc's watch-overflow open decision is discharged: overflow is terminal and
  typed, never silent.
- Deferred to future slices, governed here: connection capacities, provisioning profiles,
  envelope calculators, capability reports, and the guidance list from the review (quorum
  placement, mirror-is-DR, PubAck limits, dedup window, ack-after-receipt, archival plans).
