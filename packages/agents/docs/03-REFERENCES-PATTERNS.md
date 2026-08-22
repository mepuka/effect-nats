# 0003 — Non-standard extension design heuristics

Status: non-normative research. These patterns may guide a future local extension ADR; they are not
Synadia Agent Protocol semantics or conformance evidence. NATS citations are indexed in
`docs/02-REFERENCES-NATS.md`.

## P1 — At-least-once delivery ⇒ idempotent effects

An explicitly configured at-least-once design can redeliver after its acknowledgement deadline or
an ambiguous acknowledgement outcome. Therefore a future work-queue ADR must define stable job ids,
deduplication bounds, and idempotent application boundaries. Otherwise a redelivery can repeat an
LLM call or send the same chat message twice.

## P2 — Effectively-once = at-least-once + dedup + idempotency

Exactly-once *delivery* does not exist on asynchronous networks (two-generals
family of results). What exists: exactly-once *observable effect* when
at-least-once delivery meets a dedup window and an idempotent, ordered apply.
Any future model must preserve the distinction `dedupWindow ≠ exactlyOnce`.

## P3 — Heartbeats + leases for failure detection

Protocol §8 carries `interval_s` in-band and defines the offline threshold as three intervals per
`instance_id`. A future local extension could separately model a KV entry as a lease, acquired and
renewed through revision-aware operations, but its expiry observation and reassignment rules need
native-server evidence and an accepted ADR. A single-key CAS can prevent two successful updates at
the same expected revision; it does not by itself prove exclusive execution of external work.

## P4 — Work queues via storage-backed competing consumers

Core queue groups distribute messages among active members and do not add durable redelivery. A
future work-queue extension may instead use WorkQueue retention and a shared durable pull consumer,
but it must specify acknowledgement, redelivery, maximum-delivery, poison-message, delayed retry,
and in-progress behavior from official JetStream evidence. Those mechanics do not make the handler's
external effects exactly once.

## P5 — Backpressure must be end-to-end and bounded

An unbounded buffer converts overload into latency into collapse. Every wire⇄user boundary needs a
bounded, operation-specific policy. Standard protocol reply chunks are loss-intolerant:
slide/drop/coalesce is forbidden unless a separate extension contract changes the data model and
exposes loss diagnostics. Broker I/O fibers must not silently accumulate or block forever on user
code.

## P6 — Supervision, not hope

Long-lived loops (heartbeats, consumers, pumps) can terminate on failure, interruption, or resource
loss. Each extension contract must state which terminations are final and which are retried with a
bounded schedule; repeated failure must remain observable rather than silently changing protocol
semantics.

## P7 — Scatter-gather with bounded concurrency and quorum

A non-standard orchestration layer may fan a request to multiple agents and choose a first-good or
quorum result. It must bound concurrency, distinguish per-reply inactivity from the overall
deadline, and release losing reply subscriptions. Remote cancellation is unavailable unless a
separate extension has been negotiated.

## P8 — Cancellation is cooperative and two-phase

Signal propagation (control subject / AbortSignal) only requests cancellation;
the worker decides where to stop safely (between tool calls, at token
boundaries) and reports terminal state. Abandoned-work reclamation falls back
to leases (P3): if no renewal arrives, the slot frees regardless. The standard protocol defines no
cancellation signal. Dropping reply interest stops local delivery only; a future extension must
define whether and how remote cancellation is acknowledged.

## P9 — Total order per log, partial order across logs

A JetStream stream is a totally ordered append-only log per subject set;
cross-stream there is no order. Design so anything needing a global sequence
lives in ONE stream (conversation log), and everything else tolerates
reordering via causality tags (correlation ids, revision numbers).
EventLog brief (Foldable corpus) reaches the same conclusion from Effect's
side: local journal + per-remote watermarks + replay-from-sequence.

## P10 — Consistency models are chosen per stream, not assumed

ADR-56: stream types trade availability vs consistency (cluster, mirror,
sources are eventually consistent; lag IS the RPO). Anything we build on
mirrors/sources must treat lag as a first-class monitored quantity, not an
accident (note 0004 §4: cross-domain misconfig fails silently).

## P11 — Deterministic correlation ids make replays safe

Every retriable extension round-trip needs a correlation or idempotency identifier whose scope,
retention window, and duplicate rules are explicit. An identifier alone does not collapse
duplicates; the receiving state transition or effect boundary must enforce that behavior.

## P12 — Consensus exists below you; know what it buys

Clustered JetStream replicates through an in-tree Raft (B natsC: raft.go is
NATS-native, not etcd/hashicorp). It buys durable, replicated stream state —
it does NOT buy transactional spans across streams, nor ordering between
separate streams. Don't design as if it did.
