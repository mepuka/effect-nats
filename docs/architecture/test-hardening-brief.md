# Brief: test hardening of the principal client — a lane that runs beside feature slices

Status: brief, 2026-08-23 — draft workbench material, **dispatched 2026-08-23 at `39e4773`;
immutable**. Its precondition — the owner's instruction-file commit (`0e11aa2`, `39e4773`: the
`AGENTS.md` rewrite, the nested `AGENTS.md` files, `dprint.json`, `package.json`, `bun.lock`) — has
landed, so the lane never edits an uncommitted file. Not a slice of the Foldable plan; it is
lane L2 of `../../../Foldable/research/2026-08-23-effect-nats-lanes-plan.md`, designed to run in
parallel with the subscriber-replay lane (`lean-subscriber-trace-replay-brief.md`) and every later
feature slice. Tests, CI, scripts, evidence documents, and fixes of confirmed defects in modules
this lane owns: no public API change; no ADR unless a confirmed defect's fix would change an
accepted promise, in which case the item stops and is reported. Work under the
`effect-nats-implementation`, `tdd`, and `effect4-stream-discipline` skills; produce a fidelity
receipt per item.

Pin: every `path:line` below is at `bec02ac` (HEAD `a9157ef` differs only under `skills/`).
Effect is `4.0.0-rc.111`; `nats-server` is `v2.14.5` (`.github/workflows/ci.yml:18`).

## 0. How to work

1. **Ownership boundary — the reason this lane can run in parallel.** This lane owns: `test/*`
   except the subscriber-replay lane's files (`test/LeanTraceReplay.ts`,
   `test/LeanSubscriberReplay.ts`, `test/LeanSubscriberFreeRun.ts`,
   `test/JetStreamMemory.subscriber.properties.test.ts`, `test/fixtures/lean/**`,
   `test/JetStreamConformance.ts`, `test/JetStreamMemory.properties.test.ts`, and the registration
   lines of `test/JetStreamMemory.test.ts` and `test/JetStreamLive.test.ts`); `scripts/*` except
   `scripts/lean-traces.sh`; `.github/workflows/*`; `docs/research/*` except
   `lean-trace-live-findings.md`; `TO_FIX.md`; and `src/internal/*` except `JetStream.ts`,
   `JetStreamMemory.ts`, `JetStreamLive.ts`, and `Kv.ts` (the JetStream seam is governed by the
   Lean model and exercised by the replay lanes; `Kv.ts` builds on it). Nothing outside that set is
   edited; an item that needs to is deferred and listed in the receipt. Feature slices declare
   their own file sets in their briefs; on overlap the feature lane wins and this lane rebases.
2. **One item, one commit, red first.** Each item below is a commit (or a short coherent group),
   begun with a failing test per the `tdd` skill. The universal gates
   (`.agents/skills/effect-nats-implementation/references/testing-gates.md:8-19`: exact success
   semantics, each typed failure, interruption while blocked, finalizer after success/failure/
   interruption, saturation at producer/consumer boundaries, redaction) are the definition of
   done for every operation an item touches.
3. **Laws 2 and 8 bind.** A defect in an accepted declaration is reported, not patched; every
   receipt separates what was tested locally, against a server, assumed, or left open.
4. **Gate per commit:** `bun run verify` (format, lint, check, test) with `nats-server v2.14.5`
   on `PATH`.

## 1. Triage `TO_FIX.md` against the tree (first item; nothing else before it)

`TO_FIX.md` is a review of a baseline that no longer exists: its reviewed SHAs (`:3-4`:
`262b77a`, `f506b48`) are not in `git log`; it cites paths that have since moved
(`src/internal/NodeOfficialConnect.ts` → `NodeConnector.ts`, `test/internal/kit.ts` → `test/Kit.ts`,
`test/kv-naming.test.ts` → `test/KvNaming.test.ts`) and `pnpm` commands; all 21 checkbox items are
unchecked and none carries a status. The workspace rules still route every client task through it
(`AGENTS.md`, "Task routing"), so it must become true before anything is built on it.

For each of the 21 items, re-verify the claim at `bec02ac` by opening the cited code at its
current path and running the cited gate, then tick the box and append one line in the item's own
vocabulary: `Status (2026-08-23, bec02ac): fixed in <sha>` / `still open — <current path:line>` /
`obsolete — <reason: path gone, superseded by ADR-000N, design changed>`. Items still open and
inside this lane's ownership (from the P0 set: `:15` transport gates, `:57` connect leak on
interruption, `:82` unbounded/unobservable finalizer, `:100` overflow terminal ordering; from P1:
`:150` outcome ledger defaulting and unredacted detail, `:173` the fake connection's missing
close/termination model, `:276` the naming tests' missing red/green receipt) become items 3–N
below, one commit each. Items still open but outside the ownership (ADR wording, skill files, KV
scoping documents, `adr-0004`'s `BigInt` claim at `:34`) are listed for the owner in the receipt
with the evidence line, untouched. Replace the `## Verification` section (`:416-450`) with the
commands and results of this triage; keep the old one under a dated heading.

## 2. Universal-gate coverage matrix

For every module this lane owns in `src/internal/` — `Subject.ts`, `Connector.ts`, `Errors.ts`,
`StatusBridge.ts`, `SubscriptionBridge.ts`, `ReconnectPolicy.ts`, `KvNaming.ts`,
`CoreSubscription.ts`, `NodeConnector.ts`, `NatsTransport.ts` — and each operation it exports,
produce `docs/research/test-coverage-matrix.md`: one row per operation, one column per universal
gate, each cell the exact `test file › test name` that establishes it or `gap`. Evidence tiers per
`test/AGENTS.md`; a wall-clock `timeout` that only guards against hanging does not fill a cell.
Then fill the gaps, highest tier first (interruption while blocked; finalizer after interruption;
saturation), each gap a red test then the smallest change. The matrix is committed with the first
gap fix and updated by every later commit — it is the lane's progress record and the next lane's
starting point.

## 3. Live-tier preflight, not silent failure

`vitest run` collects every `test/**/*.test.ts` (`vitest.config.ts:5`); three files start a real
server through `test/TestServer.ts` (`spawn("nats-server", …)`, `:98`), and without the binary on
`PATH` each of those tests fails on `ENOENT` with no statement of what was required — there is no
environment gate, no skip, and no version check. Decision to record: **no skip switch** (a test
that does not run establishes nothing; `test/AGENTS.md` requires live tiers to declare their
server). Instead: a one-time preflight in `TestServer.ts` that resolves the binary, runs
`nats-server --version`, and fails the first live test with one actionable message naming the
declared version and the install step, and records the integration-matrix receipt
(`testing-gates.md:130-134`: server version, JetStream/storage flags, client runtime, isolation
path) once per run in the test output.

## 4. `leak` into CI as its own job

`scripts/leak-check.ts` describes itself as an enforceable gate (300 warm-up, 2 000 measured
cycles, fail above 5 MiB growth, `:62`) but neither `bun run verify` nor either workflow runs it.
Add a separate, non-blocking job to `ci.yml` that runs `bun run leak`; record its pass/fail history
in the receipt; promotion to a required check is the owner's call after the history exists.
`bun run perf` stays manual (`test/AGENTS.md`: measured suites establish the stated workload, not
a production guarantee).

## 5. Evidence claims that are no longer true

Three instruction files say no TypeScript replay bridge exists: `testing-gates.md:88-90` ("three
kernel-checked worked traces and there is no TypeScript JSONL replay bridge"),
`test/AGENTS.md:39-41` ("two internal kernel-checked worked traces but no TypeScript JSONL replay
harness"), `src/internal/AGENTS.md:73`, `:88`. The bridge landed in `872bd7f`
(`test/LeanTraceReplay.ts`, eight traces), and the subscriber bridge is being added. Once the
owner's commit has landed these files: replace each sentence with the current evidence boundary
(the two fixtures, their schemas and snapshots, what replay establishes and does not — compatibility
on recorded histories, never equivalence) in one commit, citing this brief.

## 6. Fixture and override shapes are only recoverable from code

`test/fixtures/lean/live-overrides.json` is `{schema, traces: {}}` and its full shape exists
only in the decoder (`test/LeanTraceReplay.ts:104-119`). Out of this lane's ownership (fixtures
belong to the replay lane) — noted here so the replay lane's receipt documents both override files'
shapes in `docs/research/lean-trace-live-findings.md`; do not edit.

## 7. What not to change

No edit to `src/internal/JetStream.ts`, `JetStreamMemory.ts`, `JetStreamLive.ts`, `Kv.ts`, to any
fixture, to the Lean package, or to any file the owner has uncommitted at dispatch. No new public
export. No `sleep`-based ordering proof anywhere (`test/AGENTS.md`, "Test construction"). No
relaxation of an existing assertion to make a flaky test pass — a flaky test is a finding, recorded
with its seed or interleaving in the receipt.

## Gate

- `bun run verify` green per commit with the live suite; the triage committed with every box
  ticked and every still-open item either a later commit of this lane or a receipt line for the
  owner; the coverage matrix committed and current; the preflight message exercised once with the
  binary absent and once present; the `leak` job present in `ci.yml` with at least one recorded run.
- `docs/architecture/README.md` draft-workbench list gains this brief.
- No change outside the ownership set of §0.

## Receipt

Per item: the failing test that opened it, the change, the gate output, and which universal gates
the operation now meets. For the lane: the triage table (21 rows, status and evidence line), the
matrix's gap count before and after, the items deferred to the owner with their evidence lines,
every flaky observation with its seed, and what was tested locally, against `v2.14.5`, assumed, or
left open.
