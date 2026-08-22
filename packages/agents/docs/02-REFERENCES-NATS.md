# 0002 — References: NATS / JetStream literature

Citation ledger. Paths are local mirrors: `ND:` = `C:\Users\kokok\Dev\Foldable\nats-docs-foldable`,
`ADR:` = `C:\Users\kokok\Dev\Foldable\nats-adr-foldable\adr`, `B:` = `C:\Users\kokok\Dev\Foldable\research\briefs\nats`.
Each entry: source → load-bearing fact → where our spec uses it.

## Core NATS delivery semantics

- ND `concepts/pub-sub-basics.md` — "If no subscribers exist, the message is
  simply discarded"; core delivery is at-most-once → **SPEC §3** (why the
  caller needs an inactivity timeout; why durable mode is an extension).
- ND `learn/core-nats/publish-subscribe.md` — success is indistinguishable
  from zero delivery → cancellation-by-unsubscribe costs the server nothing
  (**SPEC §4**).
- ND `concepts/queue-groups.md` — queue group = random per-message member
  choice, not load-aware; member loss falls back to survivors → **SPEC §6**
  (queue group gives distribution, NOT fairness; our own admission control
  still needed).

## JetStream streams & publishing

- ND `learn/jetstream/retention-policies.md` — exactly one policy: Limits /
  Interest / WorkQueue → **SPEC §7** picks WorkQueue for jobs.
- ND `reference/jetstream/api/stream/pub-ack.md` — PubAck carries
  `duplicate` ("not stored due to the Nats-Msg-Id header") → **SPEC §3 D1**
  dedup ids.
- ND `learn/jetstream/advanced-publishing.md` — duplicate window default
  2 min; a publish timeout does NOT mean not-stored ⇒ retries must be
  dedup-safe → **SPEC §7** job id determinism.

## Consumers & acknowledgments

- ND `learn/jetstream/acknowledgment.md` — explicit ack policy; verbs ack /
  nak(+delay) / term / in-progress; AckWait default 30s → **SPEC §7** retry &
  quarantine verbs.
- ND `learn/jetstream/delivery-and-acknowledgment.md` — unacked redelivery ⇒
  at-least-once; dual cursors (consumer seq + stream seq) → **SPEC §3 D1**
  replay-from-seq on reconnect.
- ND `learn/jetstream/pull-consumers.md` — Fetch vs Consume patterns → queue
  worker loop shape.
- ND `learn/jetstream/worker-pool.md` — N processes on one named durable:
  server hands each stored order to exactly one worker; crashed worker's
  in-flight message redelivered after AckWait → **SPEC §7** scaling story.
- ND `reference/jetstream/advisory/{max-deliver,terminated,nak}.md` — server
  advisories → quarantine observability events.

## KV buckets

- ND `learn/key-value/history-and-revisions.md` — bucket-wide monotonic
  revision counter; create = CAS against revision 0; update = CAS against
  last-read revision → Cell CAS + lease bookkeeping.
- ND `learn/key-value/watching.md` — watch = snapshot then ordered live
  changes → reactive Cells.
- ND `learn/key-value/ttl-and-limits.md` — per-key TTL create-only; expiry
  surfaces as purge marker → lease expiry design.
- ND `learn/key-value/under-the-hood.md` — KV is a JetStream stream ($KV)
  → mirror/source composition applies to cells too.

## Micro services ($SRV)

- ND `reference/services/{ping,info,stats}-response.md` — `$SRV.PING.agents`,
  `$SRV.INFO.agents[.{id}]` schemas → discovery implementation.
- ND `learn/services/scaling.md` — endpoints auto-join queue group `"q"`;
  protocol overrides it to `"agents"` deliberately (Protocol §3.3).

## ADRs

- ADR-1 (JS JSON API), ADR-4 (headers — enables Nats-Msg-Id), ADR-8 (KV over
  JetStream), ADR-9 (push idle heartbeats), ADR-13 (pull internals), ADR-17
  (ordered consumer — D1's consumer), ADR-19 (API prefixes), ADR-31 (direct
  get), ADR-32 (Service API — the $SRV spec), ADR-37 (JS simplification),
  ADR-42 (pull priority groups), ADR-48 (KV TTL), ADR-56 (consistency models),
  ADR-57 (KV subject transforms), ADR-59 (mirror/source), ADR-60 (sourcing off
  WQ/Interest upstreams). All under `ADR:` root, file names = `adr-NN.md`.

## Semantic briefs (our own corpus)

- B `2026-08-21-natsA-core-jetstream-semantics.md` — pinned sources
  (nats-server v2.15.0-dev @8e2b49f), LTS modeling discipline, precedence
  tree > ADRs > docs → any invariant we claim about server behavior gets
  checked against the pinned tree before it enters this spec.
- B `...natsB-jetstream-internals.md` — filestore/consumer/cluster internals;
  correctness-only throughput facts.
- B `...natsC-raft-companion.md` — in-tree raft.go (not etcd/hashicorp);
  meta-layer consensus under clustered JetStream → what "durable mode"
  actually guarantees under failover.

## Known gaps in the corpus

No docs page defines "work-group" consumers (that vocabulary belongs to
client libs); closest normative material is ADR-42 priority groups +
pinned-consumer advisories. `$JS.API` literal subjects are not enumerated in
the api.md overview — cite ADR-1/ADR-19 plus per-operation reference pages.
