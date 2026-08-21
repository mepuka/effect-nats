# 0003 — References: distributed-systems patterns applied here

Each pattern: statement → how it binds our design → what could go wrong if
ignored. Cross-references: `SPEC` = docs/01-SPEC.md, `ND/ADR/B` ledger in
docs/02-REFERENCES-NATS.md.

## P1 — At-least-once delivery ⇒ idempotent effects

Delivery retries are inevitable (AckWait redelivery; lost acks — ND
delivery-and-acknowledgment). Therefore every effect a message triggers MUST
be idempotent or keyed-deduplicated. **SPEC §7**: job ids are deterministic
functions of intent; apply is CAS-guarded. Failure mode without it: duplicate
LLM calls billed twice, double-sent chat messages.

## P2 — Effectively-once = at-least-once + dedup + idempotency

Exactly-once *delivery* does not exist on asynchronous networks (two-generals
family of results). What exists: exactly-once *observable effect* when
at-least-once delivery meets a dedup window and an idempotent, ordered apply.
Our Lean grade ladder encodes this honestly (`dedupWindow ≠ exactlyOnce`).
**SPEC §3 D1, §7.**

## P3 — Heartbeats + leases for failure detection

Failure detection is always timeout-based; the art is choosing thresholds.
Protocol §8: interval_s carried in-band, offline threshold 3×interval per
instance_id. KV TTL (ADR-48) turns liveness into a *lease*: acquire by CAS
create (revision 0), renew by CAS update, expiry visible to watchers as a
purge marker. Use leases for agent-slot ownership so a crashed host's work
becomes claimable instead of stuck. Failure mode: split-brain workers both
claiming one job — prevented by CAS-on-revision being server-side atomic.

## P4 — Work queues via storage-backed competing consumers

Queue groups (core NATS) lose messages on member crash (at-most-once). The
durable pattern: WorkQueue retention + shared durable pull consumer — first
ack removes the task for everyone; crashed worker's in-flight job redelivered
after AckWait (ND worker-pool). Scale by adding processes, not by partitioning
logic. **SPEC §7.** Failure modes: poison jobs → `term` after max-deliver +
advisory-driven quarantine; slow jobs → periodic `in-progress` (WITNESS) to
reset AckWait.

## P5 — Backpressure must be end-to-end and bounded

An unbounded buffer converts overload into latency into collapse. Every
wire⇄user boundary gets a bounded queue with an explicit policy:
suspend (correctness-critical), slide/drop-latest (token deltas — losing old
partials is fine, they're superseded), or coalesce (batch on a flush cadence).
Backpressure applies to the *offending* path only; broker I/O fibers never
block on user code (**SPEC §6**).

## P6 — Supervision, not hope

Long-lived loops (heartbeats, consumers, pumps) die: on exception, on
interrupt, on resource loss. Each runs under a supervisor that restarts with
backoff and escalates repeated death into observable status (a Cell), never
silence. This is Erlang's let-it-crash discipline applied to fibers.

## P7 — Scatter-gather with bounded concurrency and quorum

Fan a request to N agents, take first-good or majority-vote. Bound in-flight
fan-out (no thundering herd); per-reply timeout independent of overall
deadline; late losers get cancelled (P8). NATS gives the transport shape for
free: multi-response request on `$SRV.PING`-style subjects (ND request-reply,
scatter-gather chapters).

## P8 — Cancellation is cooperative and two-phase

Signal propagation (control subject / AbortSignal) only requests cancellation;
the worker decides where to stop safely (between tool calls, at token
boundaries) and reports terminal state. Abandoned-work reclamation falls back
to leases (P3): if no renewal arrives, the slot frees regardless. Protocol
§6.7's "drop subscription" is phase one only.

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

Every async round-trip (mid-stream query §7, job, cancel) carries an id that
is stable across retries. Replies and effects key on it; duplicates collapse.
This is the thread that ties P1/P2/P4 together and it costs nothing.

## P12 — Consensus exists below you; know what it buys

Clustered JetStream replicates through an in-tree Raft (B natsC: raft.go is
NATS-native, not etcd/hashicorp). It buys durable, replicated stream state —
it does NOT buy transactional spans across streams, nor ordering between
separate streams. Don't design as if it did.
