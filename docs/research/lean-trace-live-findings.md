# Lean sequential-trace live replay findings

Status: research only, 2026-08-22. These are compatibility observations for the committed Lean
fixture, not an accepted API decision, a refinement proof, or a general JetStream claim.

## Evidence boundary

The replay used `test/fixtures/lean/sequential-traces.json` through the internal
`JetStream["Service"]` seam. Each trace ran against a fresh scoped server and connection using
`test/TestServer.ts`, which starts `nats-server` with JetStream enabled (`-js`) and a fresh
temporary store directory. Resource cleanup is the existing scope finalizer.

- NATS Server: `nats-server v2.14.5`
- NATS clients: `@nats-io/jetstream`, `@nats-io/nats-core`, and
  `@nats-io/transport-node` `3.4.0`
- Runtime: Bun `1.3.14`
- Fixture producer: `EffectNatsSubstrate`
- Fixture schema/snapshot: `1` / `r2.1`
- Fixture model pin: effect-nats `d06223f`
- Fixture Foldable commit: `ef5c3ebe1157`
- Comparison ignored: `timestampMillis` only

At the time of the original replay, the exact observed outcomes below were asserted by the
then-current `test/fixtures/lean/live-overrides.json`. The resolved section records when the adapter
made those overrides unnecessary; the findings remain the historical record.

## Findings

| Trace      | Step | Model expectation                                                              | Observed live outcome                                                                                                                                         |
| ---------- | ---: | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `positive` |    1 | `getStream("KV_b")` returns the created config with `maxMessagesPerSubject: 0` | The returned config is otherwise equal but reports `maxMessagesPerSubject: -1`, the server/client sentinel for unlimited per-subject history.                 |
| `positive` |    4 | `lastMessageForSubject("KV_b", "$KV.b.k")` returns `h2` with no headers        | The message matches after ignoring its timestamp, but its headers contain `Nats-Expected-Last-Subject-Sequence: 1`, retained from the successful CAS publish. |
| `cas`      |    5 | `lastMessageForSubject("KV_b", "$KV.b.k")` returns `v2` with no headers        | The message matches after ignoring its timestamp, but its headers contain `Nats-Expected-Last-Subject-Sequence: 1`, retained from the successful CAS publish. |

These were reported divergences only. The originating replay slice did not change `src/`, the Lean
fixture, or the memory interpreter to reconcile them.

## Resolved — 2026-08-22

The ADR-0009 implementation commit (this commit) resolved all three recorded divergences. The
fixture was regenerated from Foldable
`8b6cf8acc7fe2735184e30dac51bbd60b979b80f` at snapshot `r2.1`, and the live override file now has
an empty trace map.

| Trace      | Step | Resolution                                                                                      |
| ---------- | ---: | ----------------------------------------------------------------------------------------------- |
| `positive` |    1 | Rule 1 normalizes absent or non-positive server per-subject limits to the seam's `0` on read.   |
| `positive` |    4 | Rule 2 removes `Nats-Expected-*` publish-option headers from every stored-message read path.    |
| `cas`      |    5 | Rule 2 applies the same stored-header normalization after a successful compare-and-set publish. |

The memory and live replays both passed all eight generated traces with the empty override set on
`nats-server v2.14.5`. The rows above remain as the historical observations that motivated the
adapter rules; this replay establishes compatibility only for the recorded histories.

## Explicit non-findings

- `config-order`, step 1: re-creating the stream with the same subjects in the opposite order
  produced `StreamConfigConflict`, matching the Lean model and memory interpreter.
- `rollup`, step 4: `lastMessageForSubject` retained `Nats-Rollup: sub`, matching the fixture.
- Every final view in all eight traces matched the fixture.
- The memory replay matched every replayed step and view exactly. The `rejected` trace reports one
  `modelOnlySteps` boundary-validation step, which the exporter intentionally omitted.

## Limits

This run establishes compatibility only for the recorded sequential histories on the versions
above. It does not establish equivalence between Lean and either TypeScript interpreter, clustered
JetStream behavior, persistence under failure, authentication/authorization behavior, or any
subscriber/buffer theorem.

## Subscriber traces (schema 2)

Status: research only, 2026-08-23. This is compatibility evidence for the generated subscriber
histories, not a refinement proof or a general claim about JetStream scheduling.

### Evidence boundary

The gated replay used `test/fixtures/lean/subscriber-traces.json` through the internal
`JetStream["Service"]` seam. Each live trace used a fresh scoped server, connection, JetStream
domain, and store directory through `test/TestServer.ts`. `settleBeforePull` is the live harness
convention that waits for `NatsConnection.flush()` before a labeled `pull`, while the consumer
remains parked on its `Deferred` gate. The convention prevents a PubAck from resolving before all
earlier message callbacks are dispatched and splitting one model pull into an artificial
one-entry chunk. It is a harness synchronization boundary, not a claim that NATS `flush` generally
witnesses completion of subscriber application work.

- NATS Server: `nats-server v2.14.5`
- NATS clients: `@nats-io/jetstream`, `@nats-io/nats-core`, and
  `@nats-io/transport-node` `3.4.0`
- Runtime/client foundation: Bun `1.3.14`; Effect `4.0.0-rc.111`
- Fixture toolchain: `leanprover/lean4:v4.33.0`
- Fixture producer: `EffectNatsSubstrate`
- Fixture schema/snapshot: `2` / `r3.1`
- Fixture model pin: effect-nats `872bd7f`
- Fixture Foldable commit: `1c8ddc5084d2ddafd8924571df5494e1a593eda5`
- Comparison ignored: `timestampMillis` only

### Gated replay findings

| Class                                      | Result                               | Evidence                                                                                                                                                                                                                                                                                              |
| ------------------------------------------ | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) replay-through-queue lag               | Reached by `sa-replay-lag`           | The model registers with replay `[1, 2, 3, CaughtUp]`; live admits replay entry `1` and then reports `ConsumerLagged(lastDeliveredSequence = 1)` before `CaughtUp`. The later modeled pulls are unavailable. This is ADR-0008's declared implementation difference, now reached by a generated trace. |
| (b) `ConsumerLagged.lastDeliveredSequence` | Exact outside the class-(a) boundary | `sa-lag` and `sa-resume` both observed `2`. In `sa-replay-lag`, the model reaches `4` after replay outside the queue, while live terminates during replay at `1`; that difference is part of the class-(a) event override rather than a separate diagnostic discrepancy.                              |
| (c) `CaughtUp` position                    | Exact outside the class-(a) boundary | Every earlier registration placed the marker exactly where the fixture does. `sa-replay-lag` live terminates before the marker, recorded explicitly rather than normalized away.                                                                                                                      |
| (d) `subscriberCount`                      | Unobservable on live                 | Fifteen non-empty count assertions are skipped through explicit `counts: "unobservable"` rows. Memory asserts every one exactly.                                                                                                                                                                      |

There are no live `outcome` overrides. `sa-replay-lag` adds the following class-(a) event and final
history rows; every row points back to this findings document:

| Trace           | Point           | Model assertion                                                    | Live observation                              |
| --------------- | --------------- | ------------------------------------------------------------------ | --------------------------------------------- |
| `sa-replay-lag` | step 4 register | `[Entry 1, Entry 2, Entry 3, CaughtUp]`                            | `[Entry 1, Failed(ConsumerLagged, last = 1)]` |
| `sa-replay-lag` | step 7 pull     | `[Entry 4]`                                                        | `[]` — the consumer already terminated        |
| `sa-replay-lag` | step 8 pull     | `[Failed(ConsumerLagged, last = 4)]`                               | `[]` — the consumer already terminated        |
| `sa-replay-lag` | `finalObserved` | `[Entry 1, Entry 2, Entry 3, CaughtUp, Entry 4, Failed(last = 4)]` | `[Entry 1, Failed(last = 1)]`                 |

The override decoder now supports a finding-linked `finalObserved` row; this closes the earlier F8
gap in which step-event overrides could not express a divergent final history. Live count
observation-boundary declarations are:

| Trace           | Step | Fixture assertion skipped on live |
| --------------- | ---: | --------------------------------- |
| `sa-replay`     |    3 | `subscriberCount("KV_b") = 1`     |
| `sa-starts`     |    6 | `subscriberCount("KV_b") = 3`     |
| `sa-starts`     |   12 | `subscriberCount("KV_b") = 7`     |
| `sa-delete`     |    1 | `subscriberCount("KV_b") = 1`     |
| `sa-delete`     |    2 | `subscriberCount("KV_b") = 0`     |
| `sa-delete`     |    7 | `subscriberCount("KV_b") = 1`     |
| `sa-lag`        |    5 | `subscriberCount("KV_b") = 0`     |
| `sa-resume`     |    9 | `subscriberCount("KV_b") = 1`     |
| `sa-accounting` |    1 | `subscriberCount("KV_b") = 0`     |
| `sa-accounting` |    3 | `subscriberCount("KV_b") = 2`     |
| `sa-accounting` |    4 | `subscriberCount("KV_b") = 1`     |
| `sa-accounting` |    5 | `subscriberCount("KV_b") = 0`     |
| `sa-drain`      |    7 | `subscriberCount("KV_b") = 1`     |
| `sa-replay-lag` |    4 | `subscriberCount("KV_b") = 1`     |
| `sa-replay-lag` |    6 | `subscriberCount("KV_b") = 0`     |

Skipped-count totals per trace are therefore: `sa-replay` 1, `sa-starts` 2, `sa-filters` 0,
`sa-delete` 3, `sa-lag` 1, `sa-resume` 1, `sa-accounting` 4, `sa-drain` 1, and
`sa-replay-lag` 2.

The generated `finalObserved` field is a selective assertion list, matching
`SubTraces.lean.checkFinal`: the harness compares every subscriber ID named by the fixture and does
not invent assertions for IDs that the generated trace omits.

### Free-running schedules

Both interpreters ran 25 fresh executions per trace. Each execution used `FastCheck.sample` with
the recorded seed to draw one `Effect.yieldNow` count in `0..3` between every pair of fixture
labels; `pull` labels performed no consumer action. The seed ranges below are inclusive and contain
every integer in the range, so they record all 25 seeds per trace and are identical across the two
interpreters.

Distinct results are shown as `observed / prefix-closed acceptance-set size` per subscriber ID.

| Trace           | Seeds         | Memory                         | Live                           |
| --------------- | ------------- | ------------------------------ | ------------------------------ |
| `sa-replay`     | 731000–731024 | id 0: 1 / 2                    | id 0: 1 / 2                    |
| `sa-starts`     | 731100–731124 | ids 0–2: 1 / 2; ids 3–6: 1 / 1 | ids 0–2: 1 / 2; ids 3–6: 1 / 1 |
| `sa-filters`    | 731200–731224 | id 0: 1 / 2                    | id 0: 1 / 2                    |
| `sa-delete`     | 731300–731324 | id 0: 1 / 2; id 1: 1 / 1       | id 0: 1 / 2; id 1: 1 / 1       |
| `sa-lag`        | 731400–731424 | id 0: 4 / 8                    | id 0: 4 / 8                    |
| `sa-resume`     | 731500–731524 | id 0: 4 / 8; id 1: 1 / 1       | id 0: 4 / 8; id 1: 1 / 1       |
| `sa-accounting` | 731600–731624 | ids 0–1: 1 / 1                 | ids 0–1: 1 / 1                 |
| `sa-drain`      | 731700–731724 | id 0: 4 / 14                   | id 0: 3 / 14                   |
| `sa-replay-lag` | 731800–731824 | id 0: 2 / 4                    | id 0: 1 / 4 (class (a))        |

`freeRunning.outcomes` is closed under chunk prefix, so membership alone can accept a consumer that
never takes another enabled pull. The driver now uses `terminalOutcomes` for a consumer that exits
naturally or is parked after 16 stable yields; subscribers interrupted by an explicit
`unsubscribe` label retain ordinary prefix membership. Terminal runs passed/reached were 25/25 on
both interpreters for every trace except `sa-accounting` (0/0 because both subscribers are
explicitly interrupted). Memory also passed 25/25 for `sa-replay-lag`; live reached terminal state
in 25/25 but passed model terminal membership in 0/25.

Every live seed 731800–731824 produced the same outside history for `sa-replay-lag`:
`[[Entry 1, Failed(ConsumerLagged, lastDeliveredSequence = 1)]]`. It is adjudicated as the expected
class-(a) live replay-through-queue divergence already recorded by the gated rows, not as a memory
class-A4 finding. No memory history fell outside either applicable set. The sampled yields exposed
multiple accepted lag/resume histories and more memory than live chunk placements for `sa-drain`;
they do not exhaust scheduler placements or interleavings inside fan-out.
