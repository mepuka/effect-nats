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

The exact observed outcomes below are asserted by
`test/fixtures/lean/live-overrides.json`. A later client or server behavior change therefore fails
the replay instead of silently replacing the record.

## Findings

| Trace      | Step | Model expectation                                                              | Observed live outcome                                                                                                                                         |
| ---------- | ---: | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `positive` |    1 | `getStream("KV_b")` returns the created config with `maxMessagesPerSubject: 0` | The returned config is otherwise equal but reports `maxMessagesPerSubject: -1`, the server/client sentinel for unlimited per-subject history.                 |
| `positive` |    4 | `lastMessageForSubject("KV_b", "$KV.b.k")` returns `h2` with no headers        | The message matches after ignoring its timestamp, but its headers contain `Nats-Expected-Last-Subject-Sequence: 1`, retained from the successful CAS publish. |
| `cas`      |    5 | `lastMessageForSubject("KV_b", "$KV.b.k")` returns `v2` with no headers        | The message matches after ignoring its timestamp, but its headers contain `Nats-Expected-Last-Subject-Sequence: 1`, retained from the successful CAS publish. |

These are reported divergences only. This slice does not change `src/`, the Lean fixture, or the
memory interpreter to reconcile them.

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
