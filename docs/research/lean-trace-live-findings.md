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
domain, and store directory through `test/TestServer.ts`. Before a labeled `pull`, the live harness
waited for `NatsConnection.flush()` so prior publishes had reached the client callbacks; the
consumer remained parked on its `Deferred` gate throughout that wire barrier. Without the barrier,
a PubAck could resolve before both callbacks were dispatched, letting one model pull become an
artificial one-entry chunk.

- NATS Server: `nats-server v2.14.5`
- NATS clients: `@nats-io/jetstream`, `@nats-io/nats-core`, and
  `@nats-io/transport-node` `3.4.0`
- Runtime/client foundation: Bun `1.3.14`; Effect `4.0.0-rc.111`
- Fixture producer: `EffectNatsSubstrate`
- Fixture schema/snapshot: `2` / `r3.1`
- Fixture model pin: effect-nats `872bd7f`
- Fixture Foldable commit: `0c118600cf7edd670311d5d5b998e348be74eb7c`
- Comparison ignored: `timestampMillis` only

### Gated replay findings

| Class                                      | Result                          | Evidence                                                                                                                                                                                         |
| ------------------------------------------ | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| (a) replay-through-queue lag               | Not observed in these histories | All registration event lists matched exactly. No schema-2 registration combines replay larger than its declared capacity, so this does not refute ADR-0008's general live replay-lag difference. |
| (b) `ConsumerLagged.lastDeliveredSequence` | Exact match; no override        | `sa-lag` and `sa-resume` both observed `2`, after draining sequence 2 and before the terminal failure.                                                                                           |
| (c) `CaughtUp` position                    | Exact match; no override        | Every registration and final gated history placed the marker exactly where the fixture does.                                                                                                     |
| (d) `subscriberCount`                      | Unobservable on live            | Thirteen non-empty count assertions were skipped through explicit `counts: "unobservable"` rows. Memory asserted every one exactly.                                                              |

There are no live `outcome` or `events` overrides. The schema-2 override rows are only the following
observation-boundary declarations:

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

Skipped-count totals per trace are therefore: `sa-replay` 1, `sa-starts` 2, `sa-filters` 0,
`sa-delete` 3, `sa-lag` 1, `sa-resume` 1, `sa-accounting` 4, and `sa-drain` 1.

The generated `finalObserved` field is a selective assertion list, matching
`SubTraces.lean.checkFinal`: the harness compares every subscriber ID named by the fixture and does
not invent assertions for IDs that the generated trace omits.

### Free-running schedules

Both interpreters ran 25 fresh executions per trace. Each execution used `FastCheck.sample` with
the recorded seed to draw one `Effect.yieldNow` count in `0..3` between every pair of fixture
labels; `pull` labels performed no consumer action. The seed ranges below are inclusive and contain
every integer in the range, so they record all 25 seeds per trace and are identical across the two
interpreters.

Distinct results are shown as `observed / acceptance-set size` per subscriber ID.

| Trace           | Seeds         | Memory                         | Live                           |
| --------------- | ------------- | ------------------------------ | ------------------------------ |
| `sa-replay`     | 731000–731024 | id 0: 1 / 2                    | id 0: 1 / 2                    |
| `sa-starts`     | 731100–731124 | ids 0–2: 1 / 2; ids 3–6: 1 / 1 | ids 0–2: 1 / 2; ids 3–6: 1 / 1 |
| `sa-filters`    | 731200–731224 | id 0: 1 / 2                    | id 0: 1 / 2                    |
| `sa-delete`     | 731300–731324 | id 0: 1 / 2; id 1: 1 / 1       | id 0: 1 / 2; id 1: 1 / 1       |
| `sa-lag`        | 731400–731424 | id 0: 4 / 8                    | id 0: 4 / 8                    |
| `sa-resume`     | 731500–731524 | id 0: 4 / 8; id 1: 1 / 1       | id 0: 4 / 8; id 1: 1 / 1       |
| `sa-accounting` | 731600–731624 | ids 0–1: 1 / 1                 | ids 0–1: 1 / 1                 |
| `sa-drain`      | 731700–731724 | id 0: 4 / 14                   | id 0: 2 / 14                   |

No observed history fell outside its generated acceptance set, so this pass records no class-A4
finding and no adjudication row. The sampled yields exposed multiple accepted lag/resume histories
and more memory than live chunk placements for `sa-drain`; they did not diversify the other
recorded histories. This mode does not exhaust scheduler placements or the interleavings inside
fan-out.
