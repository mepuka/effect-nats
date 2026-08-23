# Brief: consume resume and buffer semantics — tests and changes for the implementing agent

Status: brief, 2026-08-22 — **implemented** by commit `40cb295` (ADR-0008 accepted and indexed in
`docs/architecture/README.md`). Kept as the record of what was asked and why; the line anchors
below are at `d06223f`, before that commit moved them. The decisions it asked for landed as
**ADR-0008** (§7). Source: the Lean subscriber-model design note in the Foldable
repository (`research/2026-08-22-effect-nats-subscriber-model-design-note.md`, §4 consequences,
§6 decisions, §8 findings), ratified by the project owner on 2026-08-22.

Pin: every `path:line` below is at `d06223f`; `src/`, `test/`, and `docs/architecture/` are
byte-identical between `d06223f` and `HEAD` (`3a1c1cf`), so the anchors hold. Effect is
`4.0.0-rc.111` (`package.json:25`); the Effect facts quoted are from that version's
`node_modules/effect/src/Queue.ts` and are the reason several of these semantics exist.

## 0. How to work

1. Tests first. The shared conformance suite (`test/JetStreamConformance.ts`) is the contract
   and runs on both interpreters; "anything observable only on one implementation stays in that
   implementation's file" (`test/JetStreamConformance.ts:35-39`). Memory-only observations go in
   `test/JetStreamMemory.test.ts`, live-only in `test/JetStreamLive.test.ts` (needs the
   `TestServer`). Write each test in §1–§5 red, then make both interpreters green.
2. Binding ADRs: ADR-0004 (`bigint` sequences; no arithmetic on revisions outside comparison and
   equality, `docs/architecture/adr-0004-revision-representation.md:39-40`) and ADR-0005 (buffer
   honesty: never silent loss, `adr-0005-configuration-layers.md:53-68`). Nothing below may add
   a `+ 1n` to a revision anywhere in `src/` or `docs/`.
3. Gate: `bun run check`, `bun run lint`, `bun run test:client` (live cases included). The
   conformance count goes from 13 to 15 shared cases; keep the suite's style (`its(...)`,
   `pump`, `roomy`).
4. Do not touch the Lean model; §8 says which of its assumptions this work changes.

## 1. Resume without arithmetic: `AfterSequence`

**Finding** (design note §8.2). `WatchOptions.bufferMessages` and `KvWatchLagged` tell the
user to resume at `lastRevision + 1n` (`src/internal/Kv.ts:62-67`, `:103-107`);
`ConsumerLagged` says "resume from `lastDeliveredSequence`" without saying inclusive or
exclusive (`src/internal/JetStream.ts:101-105`); ADR-0004 forbids the arithmetic.

**Decision to record.** Add an exclusive start position and make it the documented resume:

```ts
// src/internal/JetStream.ts:30-34
export type StartPosition =
  | { readonly _tag: "AllHistory" }
  | { readonly _tag: "LastPerSubject" }
  | { readonly _tag: "NewOnly" }
  | { readonly _tag: "FromSequence"; readonly sequence: bigint } // inclusive, unchanged
  | { readonly _tag: "AfterSequence"; readonly sequence: bigint } // exclusive: sequence > n
```

- `ConsumerLagged` doc (`JetStream.ts:101-105`): "the subscription delivers every entry it had
  buffered, then fails; `lastDeliveredSequence` is the sequence of the last entry it delivered;
  resume with `start: { _tag: "AfterSequence", sequence: lastDeliveredSequence }`".
- KV: add `afterRevision?: bigint` to `WatchOptions` (`Kv.ts:57-68`), exclusive; `startFor`
  (`Kv.ts:267-272`) maps it to `AfterSequence`. `fromRevision` stays inclusive for explicit
  user positions. Setting both is a programmer error: `Effect.die` with a named defect, not a
  silent precedence. `KvWatchLagged` doc (`Kv.ts:103`) and `bufferMessages` doc (`Kv.ts:62-66`)
  say "resume with `afterRevision: lastRevision`". Remove every `+ 1n` from docs.
- Memory (`src/internal/JetStreamMemory.ts:64-84`): `case "AfterSequence"` returns
  `matching.filter((m) => m.sequence > from)`.
- Live (`src/internal/JetStreamLive.ts:323-358`): the server's `DeliverPolicy.StartSequence`
  is inclusive and `opt_start_seq` is number-typed. Map `AfterSequence n` to
  `opt_start_seq: Number(n)` under the existing `> 2^53` guard (`:328-334`, extend the guard
  to the new tag), and **drop delivered entries with `sequence <= n`** in the callback
  (`:464-485`) and in the `PullWindow` loop (`:438-447`) — comparison only, so ADR-0004's
  consequence stays intact, and a pruned message `n` needs no special case. A dropped entry
  must still drive `CaughtUp` (`message.info.pending === 0`, `:444-447`, `:482-485`).
  `lastDelivered` (§2) is not updated by a dropped entry.

**Tests (shared, `test/JetStreamConformance.ts`).**

- **C14 — "resumes after a TerminateOnLag failure with AfterSequence, gap-free and without
  duplicates"**: C13's protocol (`:336-371`) with the consumer's callback _recording_ entries
  instead of discarding them (`:349-355`): `["v1", "v2"]` observed, then
  `ConsumerLagged { lastDeliveredSequence: 2n }`; then a second consumer
  `start: AfterSequence(2n)`, `roomy`, `runCollect … takeWhile (≠ CaughtUp)` yields
  `["v3", "v4"]`. This subsumes §4's tightening of C13; keep C13 as is or fold it in.
- **C15 — "AfterSequence at the head replays nothing; at zero it replays everything"**:
  publish v1, v2; `AfterSequence(2n)` via `pump` → first event `CaughtUp`; publish v3 → `Entry
  v3`. Then `AfterSequence(0n)` with `runCollect … takeWhile` → `["v1", "v2", "v3"]`, and
  `FromSequence(1n)` yields the same list.
- **`test/Kv.test.ts`** (extend `:238-265`): after `KvWatchLagged { lastRevision: 2n }`,
  `store.watch({ afterRevision: 2n })` yields `Put k=v3`, then `CaughtUp`; no `v2`. Add a
  case that `watch({ fromRevision: 1n, afterRevision: 1n })` dies.

## 2. `lastDeliveredSequence` is always a delivered sequence

**Finding** (design note §4 items 3, 5, 10; §8.1). `lastDelivered` advances on enqueue
(`JetStreamMemory.ts:194-195`), not on the overflowing message (`:180-192`); the failed queue
delivers its buffer before the error (Effect `Queue`: `failCauseUnsafe` keeps the buffer and
moves to `Closing`, `takeUnsafe` drains it before `Done`). The initial values differ — memory
`nextSequence - 1n` (`:231`), live `0n` (`JetStreamLive.ts:461`) — but that value is only
observable when the very first matching message overflows, which needs `messages = 0` (memory:
`size >= 0` at `:182`; live: `sizeUnsafe < 0` at `:471-472` never holds). §3 makes it
unreachable.

**Decision to record.** `ConsumerLagged.lastDeliveredSequence` is the sequence of the last
entry the subscription delivered before failing; every buffered entry is delivered before the
failure. No code change beyond §1's docs and §3; the live adapter may keep `0n` as its
unreachable initial value, with a comment saying why it is unreachable.

**Tests.** C14 above asserts both halves (entries before the failure; the carried sequence is
the last observed). Optional live-only (`test/JetStreamLive.test.ts`): **"live: a TerminateOnLag
buffer smaller than the replayed history lags during replay and resumes with
AfterSequence"** — publish v1..v3, consume `AllHistory, TerminateOnLag 1` with a blocked
consumer; the failure carries the last admitted history sequence; `AfterSequence(that)`
completes the history. This pins design note §8.6: on the live adapter replay passes through
the bounded queue (`JetStreamLive.ts:471-481`), on the memory interpreter it bypasses it
(`JetStreamMemory.ts:239-245`) — typed and resumable on both, so a documented difference in
_when_ a `TerminateOnLag` subscription can fail, not a contract violation. Record it in
ADR-0008 either way.

## 3. `ConsumeBuffer.messages` must be `>= 1`

**Finding** (design note §8.4, decision 4). `messages: 0` reaches `Queue.bounded(0)` on memory
(`JetStreamMemory.ts:220-225`) and `Queue.bounded(1)` with an always-failing admission test on
live (`JetStreamLive.ts:453-459`, `:471-472`); nothing specifies it, and it is the only way to
observe §2's divergent initial value. ADR-0005 rules that numeric sentinels never appear in
public types (`adr-0005-configuration-layers.md:31-32`).

**Decision to record.** A `ConsumeBuffer` with `messages < 1` (or a non-integer) is a
programmer error: `consume` dies with a named defect (e.g. `InvalidConsumeBuffer`) before any
resource is acquired, on both interpreters, through one shared validator in
`src/internal/JetStream.ts`. A defect rather than a typed failure keeps `consume`'s error
union and every KV mapping (`Kv.ts:222-229`) unchanged; if the owner prefers a typed error,
thread it through `consumeError` and say so in ADR-0008. KV's `bufferMessages` (`Kv.ts:294`)
and `history`'s depth fallback (`Kv.ts:413-419`, already `>= 1`) go through the same validator.

**Tests (memory-only, `test/JetStreamMemory.test.ts`).** `consume` with
`{ _tag: "PullWindow", messages: 0 }` and with `{ _tag: "TerminateOnLag", messages: 0 }` — the
stream's `Effect.exit` is a `Die` with the named defect, and `subscriberCount` stays `0`. KV:
`watch({ bufferMessages: 0 })` dies. (The validator is shared, so one interpreter's test is
enough; a live test would only cost server time.)

## 4. C13 asserts what it relies on

**Finding** (design note §8.3 and §4 item 3). C13's callback discards entries (`:349-355`), so
"the consumer sees v2 before the error" is derived from `Queue.ts`, not asserted.

**Change.** Covered by C14 (§1) if C13 is folded into it; otherwise make C13 record entries and
assert `["v1", "v2"]` before `ConsumerLagged`.

## 5. A full `PullWindow` on the memory interpreter stalls every stream

**Finding** (design note §4 item 1; §8.5). The memory `publish` fans out inside the single
permit (`JetStreamMemory.ts:86-91`, `:136`, `:176-198`); a suspended `Queue.offer` on a full
`PullWindow` keeps the permit, so every operation on every stream waits until that consumer
pulls or unsubscribes. M2 states the suspension (`test/JetStreamMemory.test.ts:19-22`) but not
its reach; ADR-0005 (`:57-59`) and the seam doc (`JetStream.ts:42-52`) do not state it.

**Decision to record (recommended: document and pin; do not change).** The memory
interpreter is the in-process producer ADR-0005 names; the stall is the honest face of
`PullWindow` there, and the Lean model represents it as explicit blocked-publish state. Add the
sentence to the `ConsumeBuffer` doc (`JetStream.ts:43-49`) and to ADR-0008: "on the memory
interpreter a full window suspends the publishing fiber while it holds the interpreter's single
permit — every operation on every stream waits". Alternative, not recommended now: per-stream
permits (a `Map<string, Semaphore>` under a map-level lock), which changes the interpreter's
atomicity story and would require the Lean subscriber model to block per stream.

**Test (memory-only).** "a full PullWindow suspends unrelated operations until the consumer
pulls": streams `KV_a` and `KV_b`; a `PullWindow 1` consumer on `KV_a` whose callback blocks on
a `Deferred` after `CaughtUp`; publish `a1`, `a2` to `KV_a` in a forked fiber (the second
suspends); then fork `publish("KV_b", …)` and assert it has **not** completed (poll the fiber
after yielding); open the gate; both publishes complete with their sequences and `KV_b`'s
publish observes nothing lost.

## 6. What not to change

- `deleteStream` leaves the Set untouched (`JetStreamMemory.ts:122-133`); the Set is discarded
  with the stream state and `subscriberCount` of an absent name is `0` (`:255`). Correct as is.
- The overflowing message never advances `lastDelivered` (`:180-192`). Correct as is.
- Unsubscribe order — Set delete, then `Queue.shutdown` (reverse of acquisition at
  `:220-238`) — and a suspended publisher resuming with `false` from `shutdown` are Effect
  semantics the interpreter relies on; nothing to change, but ADR-0008 may cite them.

## 7. ADR-0008 — "Consume resume and buffer semantics"

One ADR, accepted by the owner, indexed in `docs/architecture/README.md:17-26`, recording:
§1 (`AfterSequence`, exclusive; `afterRevision`; resume never does arithmetic; the live
adapter's inclusive start plus comparison-only drop), §2 (`lastDeliveredSequence` = last
delivered; buffered entries precede the failure; live replay can lag, memory replay cannot),
§3 (`messages >= 1`, defect), §5 (memory `PullWindow` stall, documented). Keep the factual
register of ADR-0004/0005.

## 8. Effect on the Lean model (for the record; no Lean work in this brief)

The sequential core (T1–T7) is untouched: none of this changes `createStream`, `getStream`,
`deleteStream`, `publish`, or `lastMessageForSubject`. The subscriber design note absorbs:
`StartPosition` gains `afterSequence` (one `selectReplay` case, `sequence > n`); decision 4
(`messages >= 1`) is now an implementation rule, not only a model boundary; the pre-enqueue
`lastEnqueued` parameter becomes unreachable but stays a parameter. No change to the note's
representation record is needed before the subscriber slice.

## 9. Acceptance

- `bun run check`, `bun run lint`, `bun run test:client` green on memory and live.
- Shared conformance cases: 15 (C14, C15 added); `test/Kv.test.ts` extended; memory-only
  cases +2 (§3 defect, §5 stall); live-only +1 optional (§2).
- `grep -rn "+ 1n" src docs` finds nothing that touches a revision or sequence.
- ADR-0008 accepted and indexed; `ConsumerLagged`, `ConsumeBuffer`, `WatchOptions`, and
  `KvWatchLagged` doc comments match it.
- `docs/architecture/README.md` lists ADR-0008 under accepted ADRs and this brief under draft
  workbench material.
