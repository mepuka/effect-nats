# 0004 — Mesh IoT deployment patterns: NATS topology × Effect abstractions

Grounded in the docs-mirror/ADR survey (leafnodes, JetStream domains, ADR-19 API prefixes,
ADR-59 mirrors/sources, ADR-8/ADR-57 KV). Citations live in the survey; this note records
the patterns we build against and the load-bearing facts each depends on.

## The topology facts that shape everything

1. **A leafnode is an outbound bridge by interest.** Local clients never appear on the hub;
   a thousand devices behind a leaf are one link. Subject isolation exists only when the
   leaf is bound to its own account. `isolate_leafnode_interest` keeps sibling sites from
   seeing each other.
2. **Edge durability requires an edge JetStream domain.** `jetstream { domain: "site-x" }`;
   without it, enabling JetStream on the leaf silently extends the *hub's* JetStream.
   Domains are addressed by API prefix `$JS.<domain>.API`, with the leaf boundary mapping
   stripping the prefix (ADR-19, partially implemented).
3. **The docs are silent on leaf-link buffering of core NATS traffic.** No page promises
   store-and-forward across a down link. Consequence: core publish across the link is
   at-most-once in practice, and the only durable uplink is stream-to-stream: publish into
   an edge-local stream, cloud *sources* it (`external { api: "$JS.<domain>.API" }`).
   This is the physical justification for the grade ladder: crossing the link fire-and-forget
   IS `atMostOnce`; landing in the edge stream first is what buys `atLeastOnce`.
4. **Mirror/source is eventually consistent and lag is the RPO.** PubAck precedes
   replication. Cross-domain misconfiguration (service vs stream export types) fails
   *silently* — "the mirror never catches up" — so lag must be a first-class monitored
   metric, not an afterthought. Cross-domain cycle detection does not exist; topology is
   operator-owned.
5. **KV: one revision counter per bucket, CAS via create(rev 0)/update(rev n); no
   read-after-write on reads; mirrors are read-only; per-key TTL is create-only** (put/update
   silently drop it) and expiry surfaces to watchers as a `PURGE` with reason `MaxAge`.
   Watch = snapshot, end-of-initial-data marker, then ordered updates, backed by an
   ephemeral ordered consumer. Bucket limits REJECT writes (discard=new) — backpressure
   arrives as a typed error, not silent eviction.

## Patterns

### P1 — Edge autonomy cell
One leafnode per site, own account, own JetStream domain. All device programs talk to the
*local* server only. Uplink loss degrades write targets from "synced" to "accumulating lag";
nothing at the site changes behavior. In Effect: the site is a `Layer` — the same program
gets `EdgeRuntime` (local JS domain, spooling) or `CloudRuntime` (hub JS) wiring; topology
is Layer composition, not code branching.

### P2 — Two-bucket shadow (digital twin)
Desired state lives in a hub bucket, mirrored down to each site (ADR-57 subject transforms,
MirrorDirect); reported state lives in an edge bucket, sourced up. Writes always go to the
origin bucket (mirrors are read-only), so CAS is meaningful exactly where writes happen:
cloud CAS on desired, device CAS on reported. Revision conflicts surface as typed
`ShadowConflict` with both revisions — retry is a `Schedule`, not a hand-rolled loop.
Read staleness is honest: reads are "local replica, possibly behind" by KV's own contract.

### P3 — Durable uplink (store-and-forward done right)
Telemetry and events are *never* core-published across the link. Device → edge stream
(local PubAck = durably at the site) → cloud stream sources it. The uplink's health is one
number (source lag) plus one timestamp (last seen), both exposed as Effect `Metric`s. The
grade floor `requireFloor<AtLeastOnce>` on the uplink boundary makes "someone fire-and-forgot
across the WAN" a compile error.

### P4 — Telemetry firehose
Device readings are batched by count-or-time (`Stream.aggregateWithin`-shaped), stamped with
a per-device monotone `batchSeq`, published with `Nats-Msg-Id = device:batchSeq` into the
edge stream (dedup window absorbs redeliveries and reconnect double-sends). Backpressure is
explicit: the queue strategy between sensor callback and publisher is a named choice
(suspend on-device; never unbounded), and bucket/stream limit rejections are typed errors.

### P5 — Command plane
Commands are written to a per-site command stream in the *edge* domain (addressed from the
hub via the `$JS.<domain>.API` prefix), consumed on-site with explicit acks; `commandId` is
the idempotency key; the device replies on a correlated subject. At-least-once delivery +
device-side dedup + bounded redelivery (MaxDeliver) — each knob surfaced, none hidden.
Deadline = `Effect.timeout`; retry policy = `Schedule` value passed in, not baked in.

### P6 — OTA rollout as a durable workflow
Waves ([1%, 10%, 100%]) as activities: pick cohort → write desired firmware via P2 → await
health window (telemetry aggregates from P4) → gate. Manual approval between waves is a
`DurableDeferred`. Abort writes desired back. The workflow engine owns resume-after-crash;
the rollout survives operator laptops, hub restarts, and site outages because every step's
effect lives in KV/streams, and the orchestration state is the workflow's, not a process's.

### P7 — Presence without polling (with an honest caveat)
Device heartbeat creates a TTL'd presence key; expiry emits `PURGE(MaxAge)` to watchers —
offline detection is an ordinary KV-watch event. Caveat, from the KV contract: TTL is
create-only, so a heartbeat "refresh" must delete+create (two revisions) — or presence is
derived instead from `reported.lastSeenAt` with a Clock-driven sweep. Both variants are
Effect programs against the same `Shadow`/`Presence` service; TestClock makes the sweep
testable in milliseconds.

## What Effect buys, concretely, pattern by pattern

- **Layers**: P1's entire "edge vs cloud" story; also test wiring (in-memory KV, TestClock).
- **Scope**: watcher/consumer/subscription lifecycle — P2's watches and P5's consumers
  can't leak; interruption runs unsubscribe/drain by construction.
- **Stream + Queue strategies**: P4's batching and named backpressure.
- **Schedule**: reconnect/backoff/CAS-retry as composable values (the docs don't even give
  a default leaf reconnect interval — policy is ours, so make it a value).
- **Typed errors**: CAS conflicts, limit rejections, unreachable devices — `E` channel,
  catchable by tag.
- **Clock/TestClock**: P7 sweeps, P5 deadlines, P6 health windows — deterministic in tests.
- **Durable workflow**: P6 is unbuildable at this robustness level without it — or rather,
  building it by hand is exactly the ten-minute-lease/no-fencing machinery the cluster
  audit warned about.
- **Graded R channel**: the ladder is now physical (fact 3): the compiler refuses
  fire-and-forget across the WAN where the pattern demands durability.
