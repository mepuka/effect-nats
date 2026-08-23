# Brief: subscriber-trace replay, follow-up — terminal histories, the ninth trace, the receipt

Status: brief, 2026-08-23 — draft workbench material, **dispatched 2026-08-23; immutable**.
Follow-up to `lean-subscriber-trace-replay-brief.md` (merged at `23566bd`) for the decisions the
owner ratified on the overwatch's review of that lane (Foldable
`research/2026-08-23-effect-nats-lanes-plan.md`; overwatch findings F5, F7, F8, F9, F11, F12 in
Foldable `research/logs/overwatch.md`). Tests and fixtures only: no public API change, no ADR, no
change to `src/`. Work under the `effect-nats-implementation`, `tdd`, and
`effect4-stream-discipline` skills; produce the fidelity receipt.

Pin: effect-nats `4c74ee7` (`scripts/lean-traces.sh` already runs lake inside the package, so
the pinned 4.33.0 toolchain is used); Foldable `1c8ddc5`, the producing commit (published as
`effect-nats-verified` `10d2efb`).

## What changed in the fixture producer

1. **A ninth trace, `sa-replay-lag`** (snapshot r3.2 addition): three stored messages, then a
   registration with `AllHistory` and `TerminateOnLag 1`, one admitted message, one overflow,
   the drain, the failure carrying `4`. The model replays its snapshot outside the queue, so the
   registration's `events` are the three entries then `CaughtUp`; the live adapter replays
   through the `capacity + 1` subscription queue (`src/internal/JetStreamLive.ts:491`, ADR-0008),
   so **live is expected to diverge here** — this is override class (a) of the first brief, the
   one no earlier trace could reach. Record what live does as findings rows (class (a)), with
   override rows, and keep the memory replay exact.
2. **`freeRunning.terminalOutcomes`** beside `freeRunning.outcomes`: per subscriber, the histories
   after which the model has no enabled pull. The acceptance set is closed under chunk prefix
   (a consumer that never pulls passes membership); terminal membership is the stronger check.

## Work

1. **Regenerate** both fixtures with `sh scripts/lean-traces.sh` from this checkout with
   `../Foldable` at `1c8ddc5` (`git -C ../Foldable rev-parse HEAD`); `sequential-traces.json`
   changes only its `foldableCommit`; `subscriber-traces.json` gains the ninth trace and
   `terminalOutcomes`. Commit the fixtures alone.
2. **Gated replay** (`test/LeanSubscriberReplay.ts`): nothing to change for the ninth trace on
   memory — it must be exact. On live: record the divergence rows in
   `test/fixtures/lean/live-overrides-subscriber.json`, each pointing at the findings document;
   if an override is needed for `finalObserved`, add that override shape (the first brief's F8
   gap: `events` overrides were unusable as built) and document it in the findings section.
3. **Free-running** (`test/LeanSubscriberFreeRun.ts`): decode `terminalOutcomes`; when a run
   reaches quiescence with the consumer fiber exited or parked — i.e. the survivor was _not_
   interrupted with a non-empty history still growing — assert membership in
   `terminalOutcomes[id]`; keep plain `outcomes` membership for interrupted survivors. Make the
   assertion message name the interpreter (a memory history outside the set is class A4; a
   live one is class (a)/(c) of the first brief after `historyOf`'s `CaughtUp` split). Run the
   ninth trace in this mode on both interpreters; on live, the registration chunk will differ —
   record, don't suppress.
4. **Findings document** (`docs/research/lean-trace-live-findings.md`, the schema-2 section):
   the ninth trace's rows; the toolchain that produced the fixtures (4.33.0 now); name
   `settleBeforePull` as a harness convention with its reason; state the prefix-closure
   limitation and that terminal membership now covers it; sharpen the class-(a) row from "not
   observed" to "reached by `sa-replay-lag`: …".
5. **Property suite**: no change required; note in the receipt that it covers one live
   subscriber at a time (the fixture traces cover fan-out to several).
6. **Receipt**: per the first brief's Receipt section, plus: which gate legs ran (`dprint check`,
   `bun run check:client`, `bun run test:client`) and which are blocked by the owner's pending
   instruction-file commit (`lint`, `check`, `verify` — `package.json` still names
   `packages/agents`); the fixture `foldableCommit` and toolchain; the live rows for
   `sa-replay-lag`; the terminal-membership counts per trace (how many of the 25 runs reached
   quiescence uninterrupted and passed the terminal check).

Ownership as in the first brief; `dprint.json` is already reconciled on `main` (both the
owner's rename and the fixture excludes) — do not touch it. Do not commit `bun.lock`. No `src/`
change; a live divergence is a finding, never a fix.

## Gate

`dprint check`, `bun run check:client`, `bun run test:client` green with `nats-server v2.14.5`;
memory exact on all nine traces; live rows recorded for `sa-replay-lag`; terminal membership
asserted and green on memory for every uninterrupted run; the findings section updated; the
receipt in the final message.
