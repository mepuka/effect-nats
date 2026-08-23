# Brief: replay the Lean model's traces against both JetStream interpreters

Status: brief, 2026-08-22 — draft workbench material. Slice 2 of the Foldable slices plan
(`../../../Foldable/research/2026-08-22-effect-nats-lean-slices-plan.md`, decisions D2 and D3).
Adds tests and fixtures only: no public API change, no ADR. Work under the
`effect-nats-implementation` and `effect4-stream-discipline` skills; produce the fidelity receipt.

## What exists

- The Lean package `Foldable/formal/effect_nats_substrate/` proves T1–T7 over the sequential
  JetStream core and kernel-checks eight worked traces. `lake exe effect_nats_traces` prints them
  as JSON; the fixture contract is the header of that package's `Main.lean`.
- A copy of the fixture is committed here as `test/fixtures/lean/sequential-traces.json`. Its
  `foldableCommit` field names the Foldable commit that produced it; `snapshot` is the Lean
  signature-snapshot revision; `pin` is the effect-nats commit the model was transliterated from.
- The fixture shape:
  - top level: `schema` (1), `producer`, `snapshot`, `pin`, `foldableCommit`, `compare.ignore`
    (fields never compared — today `timestampMillis`), `traces`.
  - trace: `name`, `mirrors` (conformance case labels, C1–C13 in file order of
    `test/JetStreamConformance.ts`), `steps`, `modelOnlySteps` (steps omitted because the memory
    interpreter does not perform the model's boundary validation), `views`.
  - step: `op` is one of `createStream{config}`, `getStream{name}`, `deleteStream{name}`,
    `publish{stream, subject, payload, headers, expectedLastSubjectSequence | null, timestampMillis}`,
    `lastMessageForSubject{stream, subject}`; `expect` is `{ok: {_tag: unit | config | sequence |
    message, …}}` or `{error: {_tag, …}}` with the seam's tag names (`StreamNotFound`,
    `StreamConfigConflict`, `SubjectNotBound`, `WrongLastSequence{expected, actual}`,
    `RollupNotPermitted`, `NoMessageForSubject`).
  - encodings: sequences are decimal strings (→ `BigInt`); payloads are opaque tokens (publish
    `new TextEncoder().encode(token)`, compare after `decode`); headers are `[key, value]` pairs
    (→ `new Map`); JSON object key order carries no meaning.
  - view: `stream`, `subject` (string, or `null` for the whole stream), `payloads` — the payload
    tokens of that subject's messages (or all messages), in storage order.

## Tests to write

1. **`test/LeanTraceReplay.ts`** exporting `leanTraceReplay(label, withService, timeout?)` in the
   shape of `conformance(...)` (`test/JetStreamConformance.ts:40-42`): one `it.effect` per trace,
   named `${label}: lean trace ${name} (mirrors ${mirrors})`. For each trace, on a fresh service:
   run the steps in order, calling the seam operation and comparing the outcome to `expect`
   (`Effect.exit`; success value vs the `ok` shape, typed failure `_tag` and fields vs the
   `error` shape; skip every field listed in `compare.ignore`); then, for each view,
   `consume(stream, { filters, start: AllHistory, buffer: roomy })` (`roomy` as at
   `test/JetStreamConformance.ts:17-19`) with `takeWhile(event => event._tag !== "CaughtUp")` and
   `runCollect`, map payloads, compare. `filters` is `[subject]` for a subject view and the
   stream's configured subjects for a whole-stream view — keep a `name → subjects` map while
   replaying `createStream` steps. Stream names inside the fixture (`KV_b`, `KV_p`, …) assume a
   fresh namespace per trace; reuse whatever isolation `withLive` already provides (see the top of
   `test/JetStreamLive.test.ts`) and the memory interpreter's `Memory.make` per test.
   Register it in `test/JetStreamMemory.test.ts` (`leanTraceReplay("memory", …)`) and
   `test/JetStreamLive.test.ts` (`leanTraceReplay("live", withLive, 30_000)`).
2. **Memory must match exactly.** A memory mismatch is a failing test and a model/implementation
   divergence to adjudicate under workspace law 2 — report it in the receipt; do not edit `src/`
   or the fixture to make it pass.
3. **Live mismatches are findings, recorded then asserted.** Write
   `docs/research/lean-trace-live-findings.md` — per trace and step index: the model's expected
   outcome, the observed live outcome, and the exact `nats-server` version the `TestServer` ran.
   The harness reads `test/fixtures/lean/live-overrides.json` (keyed by trace name and step or
   view index; each entry names the findings document) and asserts the _recorded_ live outcome,
   so a later server change is caught rather than absorbed. Expected first findings: the
   `config-order` trace (does the server treat a subjects-order change as a config conflict?) and
   possibly `Nats-Rollup` header retention on `lastMessageForSubject` in the `rollup` trace.
4. **Property suite `test/JetStreamMemory.properties.test.ts`** (memory only) with `FastCheck` from
   `effect/testing`: generate op sequences over a small alphabet (one or two streams, three
   subjects, `maxMessagesPerSubject` in `{0, 1, 2, 3}`, `allowRollup` on/off, CAS expectations from
   `{none, 0, the last known sequence, a stale one}`). After each successful publish assert the two
   view equations — the published subject's view equals `keepLatest(limit, prior ++ [new])` and
   every other subject's view is unchanged — and at the end: stored sequences strictly increase;
   per-subject count ≤ limit whenever limit > 0; a CAS publish succeeds iff the expectation equals
   the subject's last sequence, with `0` meaning "empty". Views are observed through
   `consume AllHistory` per subject. The exact statements are theorems T2, T4, T5, T6 in
   `../../../Foldable/formal/effect_nats_substrate/docs/signature-snapshot.md`.
5. **Regeneration**: a documented command (a `package.json` script or `scripts/lean-traces.sh`)
   equivalent to
   `lake -d ../Foldable/formal/effect_nats_substrate exe effect_nats_traces -- --foldable-commit $(git -C ../Foldable rev-parse HEAD) > test/fixtures/lean/sequential-traces.json`.
   CI never runs it; the committed copy is the contract, and a regenerate is a reviewed change.

## Gate

- `bun run check`, `bun run lint`, `bun run test:client` green on memory; the live replay executed
  at least once with the findings document written and the server version recorded.
- `docs/architecture/README.md` lists this brief under draft workbench material; the findings
  document is listed with the research documents.
- No change to `src/`.

## Receipt

Per the implementation skill's required workflow: the fixture's `foldableCommit` and `snapshot`,
the server version, the live findings list, the comparisons ignored, and any memory mismatch.
