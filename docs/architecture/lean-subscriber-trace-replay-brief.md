# Brief: replay the Lean model's subscriber traces against both JetStream interpreters

Status: brief, 2026-08-23 — draft workbench material, **dispatched 2026-08-23; immutable**.
Decisions P1 and P2 of `../../../Foldable/research/2026-08-23-effect-nats-lanes-plan.md` are
ratified and the Foldable exporter emits schema 2 at the commit in the Pin line. Closes the bridge
extension of slice 4 (`../../../Foldable/research/2026-08-22-effect-nats-lean-slices-plan.md`,
slice-4 row; obligations EXT1/EXT2 and A4 in
`../../../Foldable/research/2026-08-22-subscriber-stage-a.md` §13 `:496-499`; fixture schema in
its §14 `:501-512` as corrected by L0). Adds tests, fixtures, a regeneration step, and a findings
section: no public API change, no ADR, no change to `src/`. Work under the
`effect-nats-implementation`, `tdd`, and `effect4-stream-discipline` skills; produce the fidelity
receipt. Any reasoning about the Lean model itself goes through the Lean systems suite
(`skills/CATALOG.md`), in particular `lean-assurance-review` for the refinement claim this harness
supports.

Pin: every `path:line` below is at `bec02ac` (HEAD `a9157ef` differs only under `skills/`).
Effect is `4.0.0-rc.111`. Foldable citations are at Foldable `0c11860`, the commit whose exporter
produces the fixture: extend `scripts/lean-traces.sh` (item 7), run it from this checkout with
`../Foldable` at that commit, and commit the output (`foldableCommit` is the full hash).

## What exists

- **The sequential bridge, untouched by this brief.** `test/LeanTraceReplay.ts` decodes
  `test/fixtures/lean/sequential-traces.json` with `schema` pinned to `1` (`:95`, overrides `:117`),
  applies each `op` through the seam (`observeStep`, `:166`), compares with `normalize` (`:140`:
  bigint → decimal string, `Uint8Array` → text, `Map` → `[key, value]` pairs, `compare.ignore`
  keys dropped at every depth), and applies live overrides keyed by trace name and step index
  (`:237`, `override?.outcome ?? step.expect` at `:248`). It is registered once per interpreter:
  `test/JetStreamMemory.test.ts:19` and `test/JetStreamLive.test.ts:34`. A schema-2 file cannot
  pass its decoder, which is why this brief adds a second module instead of editing it. The only
  permitted edit to `LeanTraceReplay.ts` is exporting what the new module reuses — the fixture
  schemas (`Config`, `Message`, `Op`, `ExpectedOutcome`), `observe`, `observeStep`, and `normalize` —
  with no behaviour change.
- **The model.** Stage A of the subscriber model: eight traces `sa-replay`, `sa-starts`,
  `sa-filters`, `sa-delete`, `sa-lag`, `sa-resume`, `sa-accounting`, `sa-drain`
  (Foldable `formal/effect_nats_substrate/EffectNatsSubstrate/SubTraces.lean:327`), over labels
  `op | register | pull | unsubscribe` (`Next.lean:25-39`); every trace is kernel-checked by
  `decide`; SA1–SA7 are proved under snapshot r3.1. The model's `pull` drains the whole buffer
  (assumption Q1), is enabled only on a non-empty buffer, and is placed _between_ whole operations.
  **Assumption A4** (slice §2.4 `:101-113`): stage A is a quiescent abstraction — pulls and scope
  closures never take the permit, only registration does; the runtime may interleave a pull or an
  unsubscribe between the steps of one fan-out, and the model assumes that for `TerminateOnLag`
  every such interleaving is observationally equivalent to one whole-label placement. This brief's
  free-running mode is the empirical attack on A4 before stage B proves or refutes it.
- **The seam the traces exercise** (`src/internal/JetStreamMemory.ts`): one permit serialises
  every mutation and registration (`:96`); `consume` registers at the consumer's first pull
  (`Stream.unwrap`, `:221`), acquires a bounded queue of exactly `messages` (`:227`), registers the
  subscriber in scope (`:242`, release = removal, which is the only "unsubscribe"), selects the
  replay snapshot under the permit (`:246`), and emits replay entries and `CaughtUp` _outside_ the
  queue (`:249-251`). `publish` fans out inside the permit: `Queue.size` (`:186`), the lag check
  (`:187`), removal and `Queue.fail` (`:188`), else `Queue.offer` (`:199`) and
  `lastDelivered` advance (`:200`). `subscriberCount` (`:42`) is a memory-only diagnostic. The live
  adapter (`src/internal/JetStreamLive.ts`) replays `TerminateOnLag` histories _through_ a
  `capacity + 1` queue (`:491`), starts `lastDelivered` at `0n` (`:500`, advanced at `:528`), and
  hoists a registration-time `CaughtUp` outside the queue only when nothing is pending (`:544`).
- **The gating precedent.** C13 (`test/JetStreamConformance.ts:336-371`) blocks the consumer
  callback on a `Deferred` after each entry (`:349-354`), treats `CaughtUp` as the registration
  witness (`:356-357`), and publishes into a known buffer state before opening the gate.
- **The fixture this brief consumes** — `test/fixtures/lean/subscriber-traces.json`, schema 2,
  written by `scripts/lean-traces.sh` from `lake exe effect_nats_traces -- --subscriber
  --foldable-commit <hash>` (decision P2: a second file; the sequential fixture stays byte-identical).
  Provenance keys as schema 1 (`schema: 2`, `producer`, `snapshot: "r3.1"`, `pin: "872bd7f"`,
  `foldableCommit`, `compare.ignore`). Each trace: `name`, `mirrors`, `kind: "subscriber"`,
  `steps`, `finalObserved`, `freeRunning`. Steps are labels:

  | `label`         | fields                                                                                                             | model source                      |
  | --------------- | ------------------------------------------------------------------------------------------------------------------ | --------------------------------- |
  | `"op"`          | `op`, `expect` (the schema-1 step shape); `counts`                                                                 | `Label.op`                        |
  | `"register"`    | `stream`, `options: {filters, start, buffer}`, `lastEnqueued` (decimal string), `id`, `expect`, `events`, `counts` | `Label.register` (`Next.lean:33`) |
  | `"pull"`        | `id`, `events`, `counts`                                                                                           | `Label.pull`                      |
  | `"unsubscribe"` | `id`, `counts`                                                                                                     | `Label.unsubscribe`               |

  `events` is the exact list the label appends to the subscriber's `observed`, each an event in
  the seam's vocabulary: `{"_tag": "Entry", "message": …}` (schema-1 message shape),
  `{"_tag": "CaughtUp"}`, or `{"_tag": "Failed", "error": …}` with the error
  `{"_tag": "StreamNotFound", "stream"}` or `{"_tag": "ConsumerLagged", "stream",
  "lastDeliveredSequence"}` (the seam's field, `src/internal/JetStream.ts:130-133`; a decimal
  string). `counts` is a list of `[stream, n]` pairs to check against `subscriberCount` after the
  label (empty when the model records none). `finalObserved` is a list of `{id, events}` — every
  subscriber's complete `observed` list at the end. `freeRunning.outcomes` is an object
  `id → [history, …]`: the set of consumer **histories** the model admits for that subscriber over
  every placement of its pulls (decision P1). A history is a list of chunks — the registration
  prefix first, then one chunk per pull, each chunk the events that pull yielded — with summary
  events `{"_tag": "Entry", "sequence"}`, `{"_tag": "CaughtUp"}`, or the `Failed` object above.
  Histories rather than final lists: final `observed` lists do not discriminate a wrong queue model
  (W1's one-element pull reaches final lists on `sa-drain` that the model also reaches); chunk
  histories do, and the package proves `gated_in_outcomes`, `w1_outside_outcomes`, and
  `w2_outside_outcomes` by `decide` (Foldable `EffectNatsSubstrate/SubPlacements.lean`). Key
  order in the file is not meaningful (`Lean.Json` orders keys by string).

## Tests to write

1. **`test/LeanSubscriberReplay.ts`** — the gated replay, exported as
   `leanSubscriberReplay(label, withService, timeout?)` with the sequential module's signature, and
   registered in the same two places (`test/JetStreamMemory.test.ts` after `:19`,
   `test/JetStreamLive.test.ts` after `:34`). One `it.effect` per trace, named
   `` `${label}: lean subscriber trace ${trace.name} (mirrors ${trace.mirrors.join(", ")})` ``. The
   `withService` closure for memory must expose `subscriberCount` as well as the service.

   The label protocol — this is the contract, not a suggestion:
   - **`op`**: apply through the exported `observeStep`, compare with `normalize` exactly as the
     sequential harness does; then check `counts`.
   - **`register`**: fork a consumer fiber (`Effect.forkChild`, `startImmediately: true`) that
     consumes `service.consume(stream, options)` **by chunk**, appending every event to the
     subscriber's observed list, and that, once `CaughtUp` has been observed, awaits a per-subscriber
     gate at the end of every chunk callback before the stream may pull again. Wait until the
     observed list equals `events` (replay entries then `caughtUp`); if the fiber fails before that,
     the failure is the observation (for a `register` whose `expect` is an error, assert it). Then
     check `counts`. A consumer fiber that exits with `ConsumerLagged` or `StreamNotFound` is not a
     test error: the harness records the exit as the event `{"_tag": "Failed", "error": …}` on that
     subscriber, because the model observes the error on the pull after the buffer is drained
     (`pullStep`, Foldable `Next.lean:75-87`: `done e` pulls to `[failed e]`; `closing e` drains first
     and becomes `done e`). `lastEnqueued` is not an input to `consume`; it is the model's record of what
     the adapter supplies (`src/internal/JetStreamMemory.ts:237`) and is compared only through the
     `ConsumerLagged.lastDeliveredSequence` it predicts.
   - **`pull`**: release the gate exactly once, then wait until the subscriber's observed list has
     grown by exactly `events`; fail if it grows by anything else, and fail if the consumer fiber is
     not blocked on the gate again afterwards (the next chunk callback must have re-armed it). A
     `pull` is one `takeAll` of the subscription queue; a harness that lets the stream pull while no
     `pull` label is active has broken the model's "never pulls otherwise" rule, and a pull on an
     empty queue would suspend and silently take the next publish — the gate must be awaited
     _before_ every queue pull after `CaughtUp`, never only after a delivery. A `pull` whose `events`
     is `[Failed …]` is realised the same way: the release lets the stream pull once more, the queue
     yields its failure, the fiber exits, and the harness records the `failed` event.
   - **`unsubscribe`**: interrupt the consumer fiber and await its exit, then check `counts`.
   - **End of trace**: compare every subscriber's observed list with `finalObserved` (normalize
     both; `compare.ignore` applies to messages inside events).

   `counts` is asserted on memory. On live it is not observable: skip it, and report the skipped
   count per trace in the receipt (an override class, see 3).
2. **The harness proves its own gate.** Add, in the same module and registered on memory only, a
   self-check: register a `TerminateOnLag` consumer, observe `CaughtUp`, publish twice, and assert
   that no entry is observed across a bounded number of `Effect.yieldNow` before the gate is
   released, that one release observes exactly the two entries as one chunk, and that the fiber is
   blocked on the gate again afterwards. Without this test a green replay establishes nothing about
   pull placement.
3. **Memory must match exactly.** Every `events`, `counts`, and `finalObserved` comparison on the
   memory interpreter is `toEqual` after `normalize`. A mismatch is a failing test and a
   model/implementation divergence to adjudicate under workspace law 2 — report it in the receipt;
   do not edit `src/`, the Lean package, or the fixture to make it pass.
4. **Live mismatches are findings, recorded then asserted.** Overrides live in
   `test/fixtures/lean/live-overrides-subscriber.json` (`schema: 2`; the schema-1 override file is
   untouched) with the same `{finding, outcome}` discipline as the sequential harness, extended
   with `events` overrides per step and `counts: "unobservable"`. Expected classes, each to be
   confirmed or refuted, never assumed: (a) replay-through-the-queue lag — live's `TerminateOnLag`
   replay enters the subscription queue (`JetStreamLive.ts:491`) while the model replays a snapshot,
   so traces whose replay exceeds the buffer may lag on live where the model does not (ADR-0008);
   (b) `ConsumerLagged.lastDeliveredSequence` — live's counter starts at `0n` and advances on
   admission (`:500`, `:528`), the model's is `lastEnqueued`; (c) `CaughtUp` position — after
   ADR-0009 it should match on every trace; a mismatch is a finding, not an override to accept
   silently; (d) `subscriberCount` unobservable. Record each finding as a row of a new section
   "Subscriber traces (schema 2)" in `docs/research/lean-trace-live-findings.md` with its own
   evidence boundary (server version, client versions, fixture `foldableCommit`, snapshot, pin).
5. **Free-running mode** — `test/LeanSubscriberFreeRun.ts`, registered on both interpreters. For
   each trace, K = 25 runs: consumers are forked **ungated** (the chunk callback only records);
   labels are driven in fixture order, `pull` labels are skipped (the consumer pulls whenever it is
   scheduled), and between consecutive labels the driver performs `r` `Effect.yieldNow` with `r`
   drawn from a seeded `FastCheck` generator over `0..3`. After the last label, wait until every
   consumer fiber has exited or its observed list has been stable across 16 consecutive yields, then
   interrupt the survivors. Assert, per subscriber, that the summary of its observed list is a
   member of `freeRunning.outcomes[id]`: the history is the list of chunks the consumer received
   after `CaughtUp` was observed, prefixed by the registration chunk (everything up to and
   including `CaughtUp`), each event reduced to its summary (sequence / `CaughtUp` / the `Failed`
   error); compare after normalize. Record the seed of every run. An observed list outside the set is a **finding of class
   A4**: log it with the seed, the trace, and the full observed list in the findings section; it
   blocks the receipt until adjudicated as a harness artefact (fixed in the harness, never in
   `src/`) or a genuine interleaving, which is the result this mode exists to produce. Coverage of
   the interleavings inside a fan-out is opportunistic — record the runtime's yield behaviour you
   observed, and do not claim the mode exhausts A4.
6. **Random label sequences** — `test/JetStreamMemory.subscriber.properties.test.ts`, memory only,
   `FastCheck` generator over sequences of `createStream`, `publish` (a small subject/payload
   domain), `register` (filters, `start` from all five positions, `TerminateOnLag` with
   `messages ∈ 1..3`), `pull`, `unsubscribe`, `deleteStream`, realised with the gated protocol of
   item 1. Properties, each lifted from a frozen r3.1 statement and stated on observables only:
   entry sequences in any observed list are strictly increasing (`visible_sequences_strict`); after
   a matching publish into a buffer with room, one gated pull observes exactly that entry
   (`publish_visible`, room case); a matching publish into a full buffer ends the subscriber with
   `ConsumerLagged` and no further entries (`publish_visible`, full case, `lagged_iff`); a
   non-matching publish or any `getStream`/`lastMessageForSubject` changes nothing a pull observes
   (`op_visible_frame`); `deleteStream` ends every subscriber of that stream with `StreamNotFound`
   (`delete_ends`); the replay prefix observed at registration equals the selection rule for the
   chosen `start` over the messages published so far (`register_observed`, `selectReplay_mem`).
   Take statements from Foldable
   `formal/effect_nats_substrate/docs/signature-snapshot.md` "Stage A (r3.1)"; name the theorem in
   each property's title. Minimum 200 runs per property; shrink and record any counterexample as a
   law-2 divergence.
7. **Regeneration.** `scripts/lean-traces.sh` writes both fixtures (second invocation with
   `--subscriber`); CI never runs it; the committed copies are the contract and a regenerate is a
   reviewed change. The fixture files and `live-overrides-subscriber.json` join the `dprint`
   excludes — note the exclude edit for the owner if `dprint.json` is still uncommitted at dispatch.

## Gate

- `bun run check`, `bun run lint`, `bun run test:client` green with the live suite, `nats-server`
  at the declared `v2.14.5`; the gated replay exact on memory for all eight traces; the self-check
  (item 2) present and green; free-running mode executed K runs per trace on both interpreters with
  seeds recorded; property suite ≥ 200 runs per property.
- The live replay executed with `live-overrides-subscriber.json` containing only recorded findings,
  and the findings section written with its evidence boundary.
- `docs/architecture/README.md` draft-workbench list gains this brief (defer with a note if the file
  is still uncommitted at dispatch).
- No change to `src/`, to the Lean package, to any fixture by hand, or to `test/LeanTraceReplay.ts`
  beyond the two exports.

## Receipt

Per the implementation skill's fidelity receipt, plus: the fixture's `foldableCommit`, `snapshot`,
`pin`; server and client versions; memory result per trace; live findings by class with their
override rows and the `counts` skipped per trace; free-running runs per trace, seeds, the count of
distinct outcomes observed against the size of `freeRunning.outcomes`, and every outcome outside
the set with its adjudication; property names, run counts, any counterexample; what was not
tested (`PullWindow` and `Blocked`: stage B).
