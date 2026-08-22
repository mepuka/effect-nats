---
name: effect4-stream-discipline
description: >-
  Debugging, testing, and hardening discipline for Effect 4 (effect@4.x /
  4.0.0-rc.*) code that uses Streams, fibers, Deferred/Semaphore/Queue/PubSub,
  Layers with background loops, or TestClock. Use this skill whenever you are
  writing or reviewing Effect 4 streaming/concurrent code, whenever a test
  hangs, times out, or flakes, whenever a suite passes locally but fails in CI,
  whenever you suspect a memory leak or unbounded growth, and BEFORE declaring
  any Effect 4 implementation "done" — it defines what done means. Also use it
  when porting Effect 3 knowledge to Effect 4, or when wiring wire-protocol
  state machines (ack/terminator/heartbeat-style) over Streams.
---

# Effect 4 stream, concurrency & performance discipline

This skill is a distilled repair log from building a wire-protocol runtime
(client + host) on `effect@4.0.0-rc.111`: every rule below was paid for by a
real bug — the error, the code that produced it, the test that revealed it,
and the repair are all recorded in [references/case-studies.md](references/case-studies.md).
Exact rc-era API shapes live in [references/effect4-api-notes.md](references/effect4-api-notes.md).

The one-sentence version: **compiling is not done, green-once is not done;
done is a claim you have made falsifiable and then failed to falsify.**

## effect-nats applicability gate

In this repository, use this skill as stream, concurrency, testing, and hardening evidence—not as
NATS protocol authority. Read the repository's `effect-nats-implementation` and `tdd` skills plus
the applicable official NATS source before applying a case study.

- The case-study `ack · chunk* · error? · terminator` grammar belongs to a separate agent protocol;
  it is not Core NATS, JetStream, KV, or Object Store wire grammar.
- Copy the method—preserve wire facts, model legal sequences, bound resources, witness readiness,
  and test interruption—not the example's frames, timeout, retry, or acknowledgement semantics.
- Queue capacity and overflow are contract decisions. `Queue.dropping` in a case study is not a
  default for Core subscriptions, KV watches, Object Store data, or any durable stream. Silent loss
  is forbidden unless an accepted ADR explicitly defines a lossy observation API and diagnostics.
- Callback-to-Queue bridges require proof of actual producer suspension or an explicit
  overflow-to-error/unsubscribe/close policy. `Queue.offerUnsafe` does not create backpressure.
- Every changed NATS operation needs a bidirectional outcome ledger from official responses,
  statuses, errors, transport states, and wire branches to typed errors, Stream terminals, and
  deterministic tests.

## The process

Work these phases in order. Each exists because skipping it produced a
shipped bug in the source project.

### Phase 0 — Establish seams before logic

Put everything nondeterministic behind two seams:

- **Time** behind `Clock` (never `Date.now()`), so `TestClock` from
  `effect/testing` drives it.
- **I/O** behind one small `Context.Service` interface (a `Wire`, a `Db`,
  whatever), so tests provide a scripted interpreter and an in-memory
  loopback interpreter.

Everything above the seams becomes deterministic pure orchestration. This is
what makes the rest of the process cheap: every test below runs in
milliseconds with zero servers and zero wall-clock sleeps. If you find
yourself wanting `sleep` in a test, a seam is missing.

The seam must carry _all_ observable wire facts. We lost a protocol rule
(zero-byte + headers ≠ terminator) because the seam originally dropped
header _presence_ and only kept parsed header values — the classifier could
not be right about data it never saw (case study S5).

### Phase 1 — Model invariants as data, then reuse the model everywhere

If the domain has a legal-sequence rule ("exactly one ack first, terminator
last, nothing after"), write it as an explicit state machine — a plain
`step(state, event) => state | null` function — before writing either side
of the wire. Then use that ONE function three ways:

1. structure the producer (the writer can only emit what the DFA allows),
2. check the consumer at runtime (illegal frame → typed error),
3. as the oracle in a model-based test: generate random event sequences and
   assert `checker(seq) === referenceRegex.test(encode(seq))`.

We first wrote a boolean `wellFormed(first, hasError)` heuristic; it
accepted empty streams, duplicate acks, and errors without terminators. The
honest DFA replaced it and immediately exposed that the _caller_ enforced
nothing (case studies S4, and the model test in the suite). A property that
compares your checker to an independent reference encoding is worth twenty
example tests.

### Phase 2 — Red before green, on the seams

One slice at a time: write the failing test against an agreed observable seam (package-public, or an internal boundary a task or accepted ADR names), watch it
fail for the _expected reason_, make the minimal change, watch it pass. Two
Effect-4-specific mechanics matter here:

- Fork test consumers with `Effect.forkChild(eff, { startImmediately: true })`
  (there is no `Effect.fork` in 4.x). Without `startImmediately`, the fiber
  may not have registered its first sleep/take before your
  `TestClock.adjust(...)`, and the adjust fires zero timers — the test hangs
  on a join forever instead of failing.
- Test timeout boundaries just-under / just-over (59_999 vs 60_001 for a
  60s window), never at the boundary — simultaneous timers have unspecified
  order.

### Phase 3 — A hang is a lost signal, not slowness

When a test times out, do NOT raise the timeout and do NOT start optimizing.
Both were tried in the source project; both were wrong. Instead:

1. Extract a minimal probe script (inside the repo — see the two-copies trap
   in the API notes) that does ONE iteration of the failing thing with
   `performance.now()` timestamps and printed state.
2. Find which await never returns, then ask: _what signal was this waiting
   for, and who was supposed to send it, and had they started yet?_

Our "500 round-trips exceeds 30s" heap test was not slow code — one probe
showed a single round trip hung, a second probe printed the live
subscription count and revealed the request was published before the
server's forked subscription loop had bound interest. After fixing the
race, 500 round trips took 141ms. Every hang in the whole project traced to
a lost signal: a message published before interest existed, or a fiber that
had not started when the clock advanced (case studies C1, C3, P1).

### Phase 4 — Green once is not done

Run these four passes before you may claim completion. Each found real bugs
after the suite was fully green.

**4a. Flake hunt.** Run the full suite in a loop (8–10×). Timing-dependent
bugs and generator-dependent property failures hide behind single green
runs. If CI differs from local, reproduce with the CI seed/config _before_
patching — our CI failure reproduced with a different runtime version, not
a different seed, which changed the correct fix from "tweak the test" to
"pin the toolchain" (case study E2).

**4b. Adversarial input pass.** For every boundary decoder, feed it:
invalid UTF-8, zero bytes with metadata present, huge and non-integer and
negative and `NaN` numbers where positives are expected, unknown fields
(must survive round-trip, not be dropped), wildcard/empty tokens where
concrete ones are expected, and payloads that break `JSON.stringify`
(`BigInt`, cycles). Rule: invalid boundary values fail in the typed error
channel — a defect escaping a boundary is a bug even when no spec clause
names it (case studies P2, and the hardening pass).

**4c. Memory pass.** Three concrete tests, all cheap:

- a **leak counter** on the test interpreter (`subscriptionCount()`):
  N completed operations + one interrupted mid-flight must return the count
  to baseline exactly;
- a **bounded-growth check** for every map/cache keyed by remote input —
  then attack your own bound: what if the attacker controls the field your
  expiry math uses? (`interval_s: 1e9` pinned entries forever until the
  horizon was clamped; `interval_s: 0` made the threshold `3 × 0` and
  poisoned liveness — case study P2);
- a **heap smoke test** under the real clock: warm up, `gc()`, baseline,
  run ~500 operations, `gc()`, assert growth under a generous fixed bound.
  Clear any test-harness logs before measuring or you measure the harness.

**4d. Witness audit.** For every readiness/registration/latch claim in the
code, ask: _what does this actually witness, versus what does it claim?_ A
latch that a failure path also releases witnesses "an attempt happened"
while claiming "we are bound". This exact bug — `tapError` releasing a
readiness `Deferred` so a transient subscribe failure faked registration —
survived the full green suite and two reviews, and was the last fault found
(case study C2). Release a readiness latch only on the success path; let
the supervisor retry and let waiters keep waiting. Waiting honestly beats
lying quickly.

### Phase 5 — Exit criteria

You are done when ALL of these hold, and not before:

- typecheck clean, full suite green in a repeat loop, on the pinned
  toolchain that CI also runs;
- every invariant you claim has a test that fails when the invariant is
  broken (spot-check by mentally reverting a fix: which test catches it?);
- every queue/map/fan-out has an explicit bound with tested overflow
  behavior;
- background loops are supervised per the api-notes supervision rules —
  classified, bounded, jittered retry with observable exhaustion; `repeat`
  only where normal completion is actually a fault; `forkScoped` for
  shutdown — and their readiness is witnessed, not assumed.

## The laws (each backed by a case study)

1. **Read the installed `.d.ts`, not your memory.** Effect 4 renamed and
   reshaped APIs (`Effect.forkChild` not `fork`; `Schema.Union([members])`
   array not variadic — variadic _runtime-crashes_ with
   `members.map is not a function`; `Result` not `Either`; `Stream.unwrap`
   not `unwrapScoped`). Verify signatures in `node_modules/effect/dist`
   before use. → api-notes.
2. **`Stream.timeoutOrElse` is per-pull** — it already is an idle-gap timer
   that resets on every element. Place it where every observed frame
   (including ones you later filter) flows through it. → S2.
3. **`Stream.unwrap` joins the effect's error channel into the stream's.**
   Map seam errors to your domain errors _before_ returning the stream, or
   the leak surfaces as a compile error far away. → S3.
4. **Distinguish "upstream ended" from "protocol said end"** with a
   sentinel: `Stream.concat(frames, Stream.make(END))` — a legal terminator
   makes `takeWhile` stop pulling so the sentinel is never reached; an
   upstream end reaches it and becomes a typed violation. → S1.
5. **Per-stream mutable state is safe inside the `Stream.unwrap` closure**
   — allocate the DFA cursor there and thread it through `mapEffect`. → S4.
6. **Readiness latches release on success only.** Failure paths retry; they
   never release. Layer construction awaits the latch so "constructed"
   means "bound". → C1, C2.
7. **Ingestion never waits on user work; fan-out is still bounded.** Fork
   per message _behind_ a `Semaphore.takeIfAvailable` gate; overflow gets a
   cheap inline rejection, not a fiber. → C4.
8. **Bounds need adversarial review of their own parameters.** Prune
   horizons computed from remote-controlled fields must clamp those fields;
   add a hard capacity with stalest-first eviction as the backstop. → P2.
9. **Hoist codecs out of hot paths.** One module-level
   `TextEncoder`/`TextDecoder` (non-streaming decode is stateless); never
   per-message construction. → P3.
10. **A slow test is a race until proven slow.** Probe and timestamp before
    optimizing or raising timeouts. → P1.
11. **Bridging subscriptions through async iterators requires per-adapter
    proof, not a blanket rule.** Generic stream cleanup awaits
    `iterator.return()` while interruption waits on it (the
    `Channel.fromAsyncIterable` finalizer), so any adapter whose `return()`
    cannot settle while a pull pends deadlocks interruption — the official
    NATS.js queued iterator behaves exactly this way until `stop()` resolves
    its signal, and its `yields` buffer grows without bound. Default to
    callback-to-bounded-Queue, released LIFO: unsubscribe first, then queue
    shutdown. If you bridge an iterator anyway, prove against the REAL
    dependency: a pending-pull interruption completes in bounded time,
    unsubscribe/`return` runs exactly once, no post-release delivery, and no
    hidden growable upstream buffer. → S6.

## When to read the references

- Fixing or writing anything listed above → open
  [references/case-studies.md](references/case-studies.md) and find the
  matching case (S=streaming, C=concurrency, P=performance/memory,
  E=environment). Each gives the exact error, the offending code, the
  revealing test, and the repaired code.
- Unsure of an Effect 4 API shape, or writing TestClock/FastCheck tests →
  open [references/effect4-api-notes.md](references/effect4-api-notes.md).
