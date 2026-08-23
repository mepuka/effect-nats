# Brief: live-adapter observation fidelity — the three seam rules the Lean replay exposed

Status: implemented, 2026-08-22 — completed by the ADR-0009 implementation commit (this commit).
Source: the Lean sequential-trace live replay (`../research/lean-trace-live-findings.md`, three recorded
divergences at `nats-server v2.14.5`) and the Foldable Verify pass
`../../../Foldable/research/2026-08-22-effect-nats-substrate-vp1.md` (finding VP1-02, applied to
`../../../Foldable/research/2026-08-22-effect-nats-subscriber-model-design-note.md` §4 item 10 and
§8 finding 1). Rules 1 and 2 were ruled by the project owner on 2026-08-22; rule 3 follows from
VP1-02 and is included on the same instruction. Work under the `effect-nats-implementation` and
`effect4-stream-discipline` skills; produce the fidelity receipt.

Pin: every `path:line` below is at `872bd7f`. Effect is `4.0.0-rc.111`.

## 0. How to work

1. **The declaration does not move.** Workspace law 2: the Lean package
   (`../../../Foldable/formal/effect_nats_substrate/`) declares the sequential seam — `0` is
   the only encoding of "unlimited" per-subject history, stored headers are the publisher's
   headers (`List (String × String)`, only `Nats-Rollup: sub` interpreted), and the subscriber
   design note places `CaughtUp` outside the bounded buffer. All three rules below bring the
   **live adapter** to that declaration; the memory interpreter already satisfies them. No
   Lean change, no fixture edit by hand, no `src/internal/JetStreamMemory.ts` change.
2. Tests first, in the conformance style (`test/JetStreamConformance.ts`, `its(...)`, `pump`,
   `roomy`; 15 shared cases today, the last at `:431`). Anything observable on one
   interpreter only stays in that interpreter's file (`:35-39`).
3. Binding ADRs: ADR-0004 (comparison-only sequences; no `+ 1n`), ADR-0005 (no numeric
   sentinels in public types, `adr-0005-configuration-layers.md:31-32`; buffer honesty),
   ADR-0008 (resume and buffer semantics; the live replay-lag difference it records stays).
4. Gate: `bun run check`, `bun run lint`, `bun run test:client` with the live suite; the Lean
   replay (`test/LeanTraceReplay.ts`) must pass on live with an **empty** override set.

## 1. `maxMessagesPerSubject`: the server's `-1` is the seam's `0`

**Finding** (`../research/lean-trace-live-findings.md:31`). `positive` step 1: live
`getStream` reports `maxMessagesPerSubject: -1` where the model and memory report `0`.
`createStream` sends `max_msgs_per_subject: config.maxMessagesPerSubject`
(`src/internal/JetStreamLive.ts:183`), the server stores `0` as `-1` (its unlimited sentinel),
and `getStream` maps it back with `config.max_msgs_per_subject ?? 0` (`:211`), which passes
`-1` through. The seam declares `maxMessagesPerSubject: number` with `0` = unlimited
(`src/internal/JetStream.ts:14`; memory treats `limit <= 0` as unlimited; the Lean validator
rejects negatives).

**Decision to record (rule 1).** The live adapter normalises the server's per-subject limit on
read: any value `<= 0` (including `-1` and absent) reports as `0`. Creation keeps sending the
seam's value. A negative `maxMessagesPerSubject` supplied by a caller is not given a new
meaning here — it stays the memory interpreter's "unlimited" and the model's typed rejection,
exactly as documented in the first-slice proposal §3.1.

**Test (shared).** **C16 — "getStream reports unlimited per-subject history as 0"**: create with
`maxMessagesPerSubject: 0`, `getStream` → `maxMessagesPerSubject` is `0` and the config
round-trips equal; create with `3`, `getStream` → `3`.

## 2. Stored headers are the publisher's headers; expectation headers are options

**Finding** (`../research/lean-trace-live-findings.md:32-33`). `positive` step 4 and `cas`
step 5: after a successful CAS publish, live `lastMessageForSubject` returns the message with
`Nats-Expected-Last-Subject-Sequence: 1` in its headers. The adapter carries the expectation
as a message header because that is NATS's mechanism (`src/internal/JetStreamLive.ts:227-236`),
the server stores every header, and the read path strips only the DIRECT.GET metadata
(`directMetadataHeaders`, `:86`; applied at `:313-314`). The `consume` path maps headers
unstripped (`entryOf`, `:383`, `:405`). Memory stores `options.headers` only
(`src/internal/JetStreamMemory.ts:162`), as does the model. `Nats-Rollup: sub` is a
publisher header on both interpreters and is retained by live (`:42` of the findings) — it is
content.

**Decision to record (rule 2).** `StoredMessage.headers` (`src/internal/JetStream.ts:22`) are
the headers the publisher supplied (`PublishOptions.headers`, `:40`). Publish-time expectation
headers — `Nats-Expected-Last-Subject-Sequence` and the rest of the `Nats-Expected-*` family —
are options, not content: the live adapter removes them on every read path, the same way it
removes DIRECT.GET metadata. One shared normaliser next to `fromMsgHdrs` (`:122`) applied at
`:313-314` and inside `entryOf` (`:405`) and the `PullWindow` loop's entry mapping. Do not
strip `Nats-Rollup` or any user header. Check that KV never exposes the expectation header
(`src/internal/Kv.ts:193` reads only `operationHeader`; `:355`, `:376`, `:396` publish with
`expectedLastSubjectSequence`) and say so in the receipt; add nothing there unless it does.

**Test (shared).** **C17 — "stored headers are the publisher's headers"**: publish with
`expectedLastSubjectSequence: 0n` and `headers: { "x-trace": "1" }`; `lastMessageForSubject`
headers equal exactly `[["x-trace", "1"]]`; `consume(AllHistory, roomy)` delivers the entry
with the same headers; a second publish with `Nats-Rollup: sub` (stream created with
`allowRollup: true`) is read back with `Nats-Rollup: sub` present.

## 3. `CaughtUp` never counts toward the buffer

**Finding** (Foldable design note §4 item 10 and §8 finding 1 as corrected on 2026-08-22;
VP1-02). On live, the `TerminateOnLag` queue is bounded at `capacity + 1`
(`src/internal/JetStreamLive.ts:469`) to hold `CaughtUp`, which is offered into that same
queue at registration when `num_pending === 0` (`:472-474`) or later when pending drains
(`:488`, `:505`); the admission guard `Queue.sizeUnsafe(queue) < capacity` (`:492`) counts
it. Until the consumer's first pull drains `CaughtUp`, a `messages: 1` subscription refuses
its first entry and fails with `ConsumerLagged { lastDeliveredSequence: 0n }` — the adapter's
never-delivered initial value (`:473`) — where memory admits it: memory's `CaughtUp` is
concatenated ahead of `Stream.fromQueue` (`src/internal/JetStreamMemory.ts:248-251`) and its
guard `size >= messages` (`:187`) sees an empty buffer. In general an undrained live
`CaughtUp` admits one entry fewer than `messages`. The `PullWindow` loop has the same shape:
`CaughtUp` is offered into the pull queue (`:420-423`, `:442-444`, `:458`), so the loop
suspends one message early. ADR-0008's "zero is invalid" validation does not reach this path.

**Decision to record (rule 3).** `ConsumeBuffer.messages` bounds **entries**; `CaughtUp`
occupies no buffer slot on either interpreter. On live: a `CaughtUp` known at registration
(`num_pending === 0`) is emitted ahead of the bounded queue — memory's shape
(`Stream.make(caughtUp)` then `Stream.fromQueue`) — for both policies. A `CaughtUp` that
becomes known later (pending drains during replay) still travels through the queue, in order;
ADR-0009 records that single-slot occupancy as a live-only replay-time effect next to
ADR-0008's "the live adapter may lag during replay", and says why it is not removed (ordering
with the entries already queued). With the registration-time `CaughtUp` outside the queue, the
initial `lastDelivered` value (`:473`) is unobservable for every `messages >= 1`; keep it
`0n` with a comment saying so.

**Test (shared).** **C18 — "a TerminateOnLag(1) NewOnly subscription admits its first entry
before the consumer pulls CaughtUp"**: C13's blocked-consumer protocol
(`test/JetStreamConformance.ts:336-372`: `Deferred`s `ready`/`pulled`/`gate`) with
`start: NewOnly`, `buffer: TerminateOnLag 1`; block the consumer before its first pull, publish
`v1`, release; observed `[CaughtUp, Entry v1]`, no `ConsumerLagged`. And the `PullWindow`
twin: `PullWindow 1`, same protocol, the publish completes without suspending on `CaughtUp`
(memory-only file if the live pull loop makes the timing unobservable; say which).

## 4. The Lean replay after §1–§3

- Regenerate the fixture with `sh scripts/lean-traces.sh` against the Foldable commit that
  contains the r2.1 errata (`EffectNatsSubstrate/Traces.lean`: `configOrderTrace.mirrors := []`;
  the fixture's only change besides `foldableCommit` is that `mirrors` line — confirm with
  `git diff --stat test/fixtures/lean/`). The `config-order` test name becomes
  `lean trace config-order (mirrors )`; acceptable, or render an empty list as `probe` in
  `test/LeanTraceReplay.ts` — either way, note it.
- `test/fixtures/lean/live-overrides.json` loses all three entries (`{ "schema": 1, "traces": {} }`):
  live must now match the model on `positive` steps 1 and 4 and `cas` step 5. Keep the file;
  the harness reads it when present.
- `../research/lean-trace-live-findings.md` is a record: add a dated **Resolved** section
  naming the commit, the rule that resolved each row, and the server version the replay
  passed on; do not delete the rows.

## 5. ADR-0009 — "Live adapter observation fidelity"

Write `adr-0009-live-adapter-observation-fidelity.md` in ADR-0008's shape (Context / Decision /
Consequences; status "accepted, 2026-08-22, ruled by the project owner"), recording rules 1–3
exactly as the decisions above, the KV check of §2, and the single remaining live-only
replay-time difference of §3. Add it to `README.md` under accepted ADRs and this brief under
draft workbench material; mark this brief implemented with the commit.

## Gate

- `bun run check`, `bun run lint`, `bun run test:client` green, live suite included; shared
  cases 15 → 18.
- Lean replay green on both interpreters with an empty override set; fixture regenerated from
  the named Foldable commit; `docs/research/lean-trace-live-findings.md` updated, rows kept.
- No change to `src/internal/JetStreamMemory.ts`, to the Lean package, or to the fixture by
  hand; no `+ 1n`.

## Receipt

Per the implementation skill: the fixture's `foldableCommit` and `snapshot`; the server
version; the three resolved findings with the rule that resolved each; the KV header check;
whether C18's `PullWindow` twin is shared or memory-only and why; any divergence that remains.
