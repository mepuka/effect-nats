# Case studies — real repair cycles on effect@4.0.0-rc.111

Every case: the symptom, the code that produced it, the test that revealed
it, the repair, and the transferable principle. Codes: S = streaming,
C = concurrency, P = performance/memory, E = environment. All code is from a
NATS agent-protocol runtime (reply streams shaped
`ack · chunk* · error? · terminator`), lightly compacted.

## Contents

- [S1 — Upstream end vs protocol end](#s1)
- [S2 — Idle-gap timeout is per-pull](#s2)
- [S3 — Error channel leaks through Stream.unwrap](#s3)
- [S4 — The consumer enforced nothing (DFA)](#s4)
- [S5 — The seam dropped a wire fact](#s5)
- [C1 — Fork-then-return: the fake-ready race](#c1)
- [C2 — The lying latch: released on failure](#c2)
- [C3 — TestClock and fibers that never started](#c3)
- [C4 — Bounded fan-out without blocking ingestion](#c4)
- [P1 — The "slow" test that was a race](#p1)
- [P2 — Unbounded maps and hostile numbers](#p2)
- [P3 — Hot-path allocations and honest queues](#p3)
- [P4 — Leak counters and heap smoke tests](#p4)
- [E1 — Two copies of effect](#e1)
- [E2 — CI green-red split: pin what you verified](#e2)
- [E3 — Effect 3 memory produces Effect 4 crashes](#e3)

---

<a name="s1"></a>
## S1 — Upstream end vs protocol end

**Symptom.** A reply stream whose subscription closed without ever sending
the protocol terminator completed *successfully* — the caller could not
tell "the server finished" from "the connection evaporated".

**Offending shape.** `Stream.takeWhile(f => f._tag !== "Terminator")` ends
the stream on the terminator — but a plain upstream end *also* just ends
the stream. Same observable outcome, different meanings.

**Revealing test.** Scripted wire configured to end the reply stream after
`ack, chunk` with no terminator:

```ts
const error = await runScripted(
  [chunk(0, ACK_CHUNK), chunk(10, TEXT_CHUNK)],
  (collect) => /* fork, adjust clock, Effect.flip, join */,
  { endAfterScript: true },
)
expect(error._tag).toBe("ProtocolViolation")   // was: succeeded with 2 chunks
```

**Repair.** Append a sentinel AFTER the frames; a legal terminator makes
`takeWhile` stop pulling, so the sentinel is only ever observed when the
upstream ended without one:

```ts
type EngineFrame = Frame | { readonly _tag: "EndOfWire" }
const END_OF_WIRE: EngineFrame = { _tag: "EndOfWire" }

return Stream.concat(frames, Stream.make(END_OF_WIRE)).pipe(
  Stream.timeoutOrElse({ duration, orElse: () => Stream.fail(new StreamTimeout({ inactivityMs })) }),
  Stream.takeWhile((f) => f._tag !== "Terminator"),
  Stream.mapEffect((f) =>
    f._tag === "ChunkFrame"  ? Effect.succeed(f.chunk)
    : f._tag === "ErrorFrame" ? Effect.fail(new PromptFailed({ code: f.code, message: f.message }))
    : Effect.fail(new ProtocolViolation({ reason: "reply stream ended without terminator" })),
  ),
)
```

**Principle.** Pull-based streams make "done" ambiguous. When the domain
has its own end-of-stream marker, reify the transport's end as a value too,
and let the type system force you to handle both.

---

<a name="s2"></a>
## S2 — Idle-gap timeout is per-pull

**Symptom risk.** The spec required "timeout N seconds since the *last
observed message*" — a resetting idle timer. Hand-rolling this with races
or `Stream.toPull` loops was the first (broken, wrong-API) attempt.

**Repair.** In Effect 4, `Stream.timeoutOrElse` is checked *around each
pull* — it already IS the resetting idle-gap timer:

```ts
Stream.timeoutOrElse({
  duration: Duration.millis(inactivityMs),
  orElse: () => Stream.fail(new StreamTimeout({ inactivityMs })),
})
```

Placement matters: it must sit where every observed frame flows through —
BEFORE any filtering that drops frames (unknown chunk types still reset the
timer per spec) and upstream of `takeWhile`.

**Revealing tests.** Under `TestClock`, boundary pair plus a reset proof:

```ts
// 59_999ms gap then terminator → completes; 60_001ms gap → StreamTimeout
// three 50_000ms gaps in a row → completes (each frame reset the timer),
// even though total elapsed (150s) far exceeds the 60s window
```

**Principle.** Before building timing machinery, read what the combinator
already guarantees (the source/docs say "timeout is reset around each
pull"). The correct primitive existed; the bug would have been rebuilding
it.

---

<a name="s3"></a>
## S3 — Error channel leaks through Stream.unwrap

**Symptom.** Compile error far from the cause:
`Type 'Stream<Chunk, WireFailure | PromptError, Wire>' is not assignable to 'Stream<Chunk, PromptError, Wire>'`.

**Offending code.** The stream-producing effect's own failure joins the
stream's error channel under `Stream.unwrap`:

```ts
Stream.unwrap(Effect.gen(function* () {
  const wire = yield* Wire
  const replies = yield* wire.request(subject, payload)   // Effect<..., WireFailure, Scope>
  return replies.pipe(Stream.mapError(toDomainError), ...) // maps STREAM errors only
}))
```

`wire.request`'s `WireFailure` (the acquisition failure) was never mapped —
`Stream.unwrap : Effect<Stream<A,E2,R2>, E, R> → Stream<A, E | E2, ...>`.

**Repair.** Map the seam error at the seam, before unwrap sees it:

```ts
const replies = yield* wire.request(subject, payload).pipe(
  Effect.mapError((e) => new TransportFailed({ reason: e.reason })),
)
```

**Principle.** `Stream.unwrap` (and `Layer.effect`, and every
channel-joining combinator) merges error channels. Translate transport
errors to domain errors at the seam boundary itself, not "somewhere
downstream" — downstream may only see one of the two channels.

---

<a name="s4"></a>
## S4 — The consumer enforced nothing (DFA)

**Symptom.** The caller accepted: a bare terminator (yielding a successful
empty stream), content before the mandatory ack, and duplicate acks. All
protocol violations, all silently tolerated. Found not by a failing test
but by an adversarial review asking "where does the consumer check the
sequence rule it claims?"

**Offending shape.** The classifier mapped each message to a frame and
downstream code consumed frames — no cross-frame state anywhere.

**Revealing tests.** Scripted wire, three cases, each expecting a typed
failure:

```ts
expect((await engineError([terminator(0)]))._tag).toBe("ProtocolViolation")
expect((await engineError([chunk(0, { type: "response", data: "x" }), terminator(1)]))._tag)
  .toBe("ProtocolViolation")
expect((await engineError([ack(0), ack(1), terminator(2)]))._tag).toBe("ProtocolViolation")
```

**Repair.** One shared transition function (also used by the producer and
the model-based test oracle), threaded through the stream with per-stream
mutable state — safe because the `Stream.unwrap` closure runs once per
stream instance:

```ts
export const stepWellFormed = (state: PumpState, frame: Frame): PumpState | null => {
  switch (state) {
    case "AwaitAck":  return frame._tag === "ChunkFrame" ? (isAck(frame.chunk) ? "Streaming" : null)
                         : frame._tag === "ErrorFrame" ? "ErrorSent" : null
    case "Streaming": return frame._tag === "ChunkFrame" ? (isAck(frame.chunk) ? null : "Streaming")
                         : frame._tag === "ErrorFrame" ? "ErrorSent" : "Done"
    case "ErrorSent": return frame._tag === "Terminator" ? "Done" : null
    case "Done":      return null
  }
}

// in the engine, inside Stream.unwrap's Effect.gen:
let state: PumpState = PUMP_START
Stream.mapEffect((m) =>
  Effect.flatMap(Effect.fromResult(classify(m)), (frame) => {
    const next = stepWellFormed(state, frame)
    if (next === null) return Effect.fail(new ProtocolViolation({
      reason: `illegal ${frame._tag} while ${state} (reply DFA)`,
    }))
    state = next
    return Effect.succeed(frame)
  }),
)
```

And the oracle test that keeps the DFA honest:

```ts
const REFERENCE = /^(A[C]*E?T|ET)$/   // independent encoding of the grammar
fc.assert(fc.property(fc.array(frameArb, { maxLength: 8 }), (frames) =>
  wellFormed(frames) === REFERENCE.test(frames.map(symbolOf).join(""))
), { numRuns: 2000 })
```

**Principle.** A sequence invariant that lives only in prose is enforced
nowhere. Write it once as `step(state, event)`, then make producer,
consumer, and test oracle all consume that one function — three users keep
it honest; one user lets it rot.

---

<a name="s5"></a>
## S5 — The seam dropped a wire fact

**Symptom.** The protocol says a terminator is a zero-byte message *with no
headers*; a zero-byte message WITH headers is not a terminator. The
original seam type only carried parsed error-header values — header
*presence* was gone, so the classifier literally could not implement the
rule.

**Offending seam.**

```ts
interface ReplyMessage {
  readonly payload: Uint8Array
  readonly errorCode?: number    // parsed — presence of OTHER headers lost
  readonly errorMessage?: string
}
```

**Repair + revealing test.** Add the fact to the seam, check it first:

```ts
interface ReplyMessage {
  readonly payload: Uint8Array
  readonly hasHeaders: boolean   // the fact the rule needs
  readonly errorCode?: number | undefined
  readonly errorMessage?: string | undefined
  readonly replyTo?: string | undefined
}

// classifier: headers first, then body
if (m.payload.length === 0) {
  return m.hasHeaders
    ? Result.fail(new ProtocolViolation({ reason: "zero-byte message with headers" }))
    : Result.succeed(TERMINATOR)
}
```

```ts
// test: zero-byte WITH headers must NOT terminate
const error = await runScripted(
  [chunk(0, ACK_CHUNK), at(10, msg(new Uint8Array(0), { hasHeaders: true }))], ...)
expect(error._tag).toBe("ProtocolViolation")
```

**Principle.** Audit the seam against the spec's *distinctions*, not its
happy paths: for every rule of the form "X but not when Y", the seam must
carry enough to observe Y. Design the seam from the rules, then write the
interpreter.

---

<a name="c1"></a>
## C1 — Fork-then-return: the fake-ready race

**Symptom.** A heap test doing 500 request/response round trips timed out
at 30s (see P1 for the diagnosis process). Root cause: the server layer
forked its subscription loops and returned immediately; the first request
was published before any interest was bound, so it vanished (at-most-once
delivery) and the caller waited out its 60s inactivity window.

**Offending code.**

```ts
const promptLoop = Effect.scoped(Effect.gen(function* () {
  const msgs = yield* wire.subscribe(promptSubject, { queueGroup })
  yield* msgs.pipe(Stream.runForEach(handle))
}))
yield* supervised(promptLoop)          // forkScoped — starts *eventually*
return AgentHost.of({ ... })           // "ready"... while nothing is bound
```

Tests had been masking this with a ritual `TestClock.adjust(1)` after layer
build — a smell worth noticing: a test that needs a warm-up yield to pass
is documenting a race.

**Revealing test (after the fix, as a regression pin).**

```ts
test("a prompt published immediately after layer build round-trips", ...
  const client = yield* AgentClient          // layer construction just finished
  const run = yield* forkNow(Stream.runCollect(client.prompt(identity, "now")))
  yield* TestClock.adjust(10)                // NO warm-up adjust before publishing
  expect((yield* Fiber.join(run)).map(c => c._tag)).toEqual(["Status", "Response"])
```

**Repair.** Each loop signals a `Deferred` after its subscribe succeeds;
construction awaits all of them; dependent loops (heartbeats, per spec
"only after registration") start after the await:

```ts
const bound = yield* Deferred.make<void>()
const msgs = yield* wire.subscribe(subject, { queueGroup }).pipe(
  Effect.tap(() => Deferred.succeed(bound, void 0)),
)
// ...fork all loops, collect their Deferreds...
yield* Effect.all(bounds.map(Deferred.await))   // constructed ⇔ bound
yield* supervised(heartbeatLoop)                // starts only after registration
```

**Principle.** `forkScoped` + return is a *claim* of readiness with no
witness. Anything that says "started/registered/subscribed" must await a
signal produced by the actual success of that operation.

---

<a name="c2"></a>
## C2 — The lying latch: released on failure

**Symptom.** The C1 fix itself shipped a subtler bug that survived a fully
green 140-test suite and two adversarial reviews, and was caught last by a
human asking what the latch witnesses. The first version released the latch
on failure too — reasoning "don't deadlock construction if the broker is
down":

**Offending code.**

```ts
wire.subscribe(subject, { queueGroup }).pipe(
  Effect.tap(() => Deferred.succeed(bound, void 0)),
  Effect.tapError(() => Deferred.succeed(bound, void 0)),  // ← the lie
)
```

A transient subscribe failure now released every latch: construction
completed, the service claimed registered, the first request was lost —
the exact fake-ready race C1 existed to kill, resurrected through the
failure path.

**Revealing test.** A wire wrapper that fails the FIRST subscribe per
subject and succeeds on the supervised retry:

```ts
const flakyWire: WireShape = {
  ...bus.wire,
  subscribe: (() => {
    const failedOnce = new Set<string>()
    return (subject, opts) => {
      if (failedOnce.has(subject)) return bus.wire.subscribe(subject, opts)
      failedOnce.add(subject)
      return Effect.fail(new WireFailure({ reason: "transient subscribe failure" }))
    }
  })(),
}
// build layer (restartMs: 50), publish immediately, adjust past the retry,
// expect the full reply — red before the fix (stream timed out, prompt lost)
```

**Repair.** Delete the `tapError` line. Success releases; failure retries
under the supervisor while construction and `discover` keep waiting.
Waiting honestly beats lying quickly — with `waitOnFirstConnect` transport
semantics, blocking-until-bound is the truthful contract, and callers who
want bounded startup can race the layer with their own timeout.

**Principle.** For any latch/flag/readiness bit, enumerate every code path
that sets it and ask what each path actually proves. A latch with a
failure-path release proves "an attempt happened". Also: run the witness
audit *again after* fixing a race — the fix is new concurrent code and
deserves the same suspicion that found the original.

---

<a name="c3"></a>
## C3 — TestClock and fibers that never started

**Symptom.** `error TS2339: Property 'fork' does not exist` — then, after
switching to the right API, tests that hang: `TestClock.adjust` completes
instantly having fired zero timers, and a later `Fiber.join` waits forever.

**Cause.** Effect 4 has `Effect.forkChild` / `forkScoped` / `forkIn` /
`forkDetach` — no `Effect.fork`. And a forked fiber starts on a scheduler
tick: if the test adjusts the clock before the fiber has run to its first
suspension, the fiber's sleep/take is not yet registered, so the adjust has
nothing to fire.

**Repair (test helper used everywhere).**

```ts
const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

// pattern:
const fiber = yield* forkNow(Effect.flip(Stream.runCollect(stream)))
yield* TestClock.adjust(60_001)
const error = yield* Fiber.join(fiber)
```

Also: extract typed failures with `Effect.flip` and join the flipped fiber
— cleaner than digging through `Exit`/`Cause` internals, whose shapes
shifted in 4.x.

**Principle.** `TestClock.adjust` fires *registered* timers. Determinism
requires the consumer fiber to reach its first suspension before the clock
moves — `startImmediately: true` guarantees exactly that.

---

<a name="c4"></a>
## C4 — Bounded fan-out without blocking ingestion

**Symptom (review finding).** The ingestion loop forked one fiber per
inbound message *before* admission control decided anything — correct that
broker ingestion never waits on user work, but a burst of 100k messages
creates 100k waiting/validating fibers even though handler concurrency was
bounded at 1.

**Offending code.**

```ts
Stream.runForEach((m) => Effect.forkScoped(respond(m)))   // unbounded pre-admission
```

**Repair.** Two semaphores: `admission` bounds in-flight handlers (overflow
→ protocol 429), and `pending` bounds responder fibers themselves; past
that, reject inline — the rejection is two cheap publishes, bounded work
the ingestion fiber can afford:

```ts
const admission = Semaphore.makeUnsafe(maxInFlight)
const pending = Semaphore.makeUnsafe(maxInFlight * 8 + 32)

Stream.runForEach((m) =>
  Effect.flatMap(Semaphore.takeIfAvailable(pending, 1), (took) =>
    took
      ? Effect.forkScoped(respond(m).pipe(Effect.ensuring(Semaphore.release(pending, 1))))
      : overloaded(m),     // inline: error(429) then terminator, then continue ingesting
  ),
)
```

The invariant is pinned by a wire-level test: a handler sleeping 5s must
not delay the next request's complete reply (assert on published frames per
reply subject under TestClock).

**Principle.** "Don't block ingestion" and "bound concurrency" are BOTH
required; fork-per-message satisfies only the first. Put a
`takeIfAvailable` gate in front of the fork and make the overflow path
cheap enough to run inline. Release permits in `Effect.ensuring` so
interruption can't leak them.

---

<a name="p1"></a>
## P1 — The "slow" test that was a race

**Symptom.** Heap test: 500 request/response round trips. Timed out at the
5s default. Raised to 30s — timed out again. The wrong moves would have
been raising it further or "optimizing".

**Process that found it.**

1. Probe script, ONE round trip, `performance.now()` prints → hung with no
   output at all. So not slow: *stuck*. (First probe also crashed with an
   effect-internals `yieldWrapGet` bug — see E1 — worth recognizing fast.)
2. Second probe printed observable state on a cadence:

```ts
for (let i = 0; i < 10; i++) {
  yield* Effect.sleep(50)
  console.log(`t+${(i + 1) * 50}ms subs=${bus.subscriptionCount()}`)
}
// → subs=9, forever. The waiter wanted 10. Count the real loops: 8 server + 1 client = 9.
```

The hang was an off-by-one in the probe's own readiness wait — and pulling
that thread exposed that the *tests* had no principled readiness at all
(the `TestClock.adjust(1)` ritual), which led to C1/C2.

3. After the readiness fix: 500 round trips = **141ms** (~280µs each). The
   code had never been slow.

**Principle.** Timeout ≠ slow. Isolate one iteration, timestamp it, print
the state the blocked party is waiting on, and identify the missing signal.
Only optimize after a probe shows real elapsed work. And: a diagnostic
probe is code too — check its own assumptions when it misbehaves.

---

<a name="p2"></a>
## P2 — Unbounded maps and hostile numbers

Three linked repair cycles on one liveness tracker (a map keyed by remote
`instance_id`, expiry math using remote `interval_s`).

**Cycle 1 — unbounded growth.** Every instance ever seen was retained
forever. Red test: publish a beat, advance the clock far past any horizon,
publish a beat for a *different* instance (pruning ran on beats), assert
the stale entry is gone. Repair: prune stale entries on each recorded beat.

**Cycle 2 — the attacker controls the expiry math.** Review question: what
if `interval_s` is huge? `pruneFactor * 1e9 s` pins an entry effectively
forever; unique ids with huge intervals = unbounded again. Repair: clamp
the remote field in the horizon computation, and add a hard capacity with
stalest-first eviction as the backstop that holds regardless:

```ts
const horizon = pruneFactor * Math.min(entry.intervalS, MAX_PRUNE_INTERVAL_S) * 1000
if (now - entry.lastSeenMs > horizon) beats.delete(id)
while (beats.size > capacity) { /* evict entry with smallest lastSeenMs */ }
```

**Cycle 3 — non-positive intervals poison the threshold.** Liveness was
`now - lastSeen < 3 * interval_s * 1000`; a beat with `interval_s: 0` makes
the threshold 0 (never live), negative is worse — and the entry still
occupied the map. Red test: publish beats with `-5`, `0`, `NaN`; assert the
tracker holds none of them. Repair in two layers: filter at the tracker
(`Number.isFinite(h.interval_s) && h.interval_s > 0`), then push it into
the boundary schema so no consumer can forget:

```ts
interval_s: Schema.Number.pipe(Schema.check(Schema.isGreaterThan(0)))
```

**Principle.** For every map keyed by remote input: (1) an expiry
mechanism, (2) whose parameters clamp remote-controlled fields, (3) plus a
hard capacity that holds even if 1–2 are wrong. Then attack your own
numbers: `0`, negative, `NaN`, `1e9`, non-integer — each should land in a
typed rejection, not in arithmetic.

---

<a name="p3"></a>
## P3 — Hot-path allocations and honest queues

**Allocation.** `new TextDecoder(...)` / `new TextEncoder()` were
constructed *per message* in the classifier, heartbeat decoder, and every
publish. Non-streaming `decode()` is stateless, so share module-level
singletons:

```ts
const decoder = new TextDecoder("utf-8", { fatal: false })   // module level, once
const encoder = new TextEncoder()
```

(One decoder per *purpose*: the request-boundary decoder is
`{ fatal: true }` because invalid UTF-8 there must become a typed 400, not
silent U+FFFD replacement — a separate repair cycle found via adversarial
input `Uint8Array.of(0xff)`.)

**Queues.** The in-memory test bus used `Queue.unbounded` with no shutdown.
Repair: `Queue.dropping(1024)` — matching real at-most-once transport
semantics where slow consumers drop — plus `Queue.shutdown` in the scope
release finalizer. Choosing the overflow policy is a semantic decision:
`dropping` = shed new (slow-consumer), `sliding` = shed old (latest-wins
fan-out; used for the heartbeat `PubSub.sliding(256)`), suspending = real
backpressure (never on a broker-facing path).

**Principle.** Grep `new Text(Encoder|Decoder)|JSON\.` on hot paths; hoist
what is stateless. Every queue names its bound AND its overflow policy on
purpose; "unbounded" in test kits still lies to you about production
behavior.

---

<a name="p4"></a>
## P4 — Leak counters and heap smoke tests

Memory claims need tests or they are hopes. Three cheap, deterministic
mechanisms:

**Leak counter on the test interpreter.** The loopback bus exposes
`subscriptionCount()`; scope hygiene is then a one-line assertion:

```ts
const baseline = bus.subscriptionCount()
for (let i = 0; i < 25; i++) { /* full round trip, joined */ }
expect(bus.subscriptionCount()).toBe(baseline)
const hung = yield* forkNow(Stream.runCollect(client.prompt(identity, "hang")))
yield* TestClock.adjust(1); yield* Fiber.interrupt(hung)
expect(bus.subscriptionCount()).toBe(baseline)   // interruption releases exactly once
```

This works because release is an explicit `Effect.acquireRelease` finalizer
— never trust an async iterator's `return` to unsubscribe for you.

**Heap smoke, real clock.** Warm up, `gc`, baseline, work, `gc`, generous
fixed bound; clear harness logs before measuring or you measure the
harness:

```ts
const heapUsedAfterGc = () => { Bun.gc(true); return process.memoryUsage().heapUsed }
for (let i = 0; i < 50; i++) yield* roundTrip(i)     // warm-up
bus.log.length = 0
const before = heapUsedAfterGc()
for (let i = 0; i < 500; i++) yield* roundTrip(i)
bus.log.length = 0
expect(heapUsedAfterGc() - before).toBeLessThan(25 * 1024 * 1024)
```

**Readiness for real-clock tests.** These run without TestClock, so the
fake-ready race (C1) bites here first — that is exactly where it was
caught. Wait for a witnessed condition, not a sleep. (The interim
poll-until-quiescent helper was retired once construction itself witnessed
readiness — the right fix removed the need.)

**Principle.** Leak detection wants an *exact* counter at a seam
(baseline in/out equality, including an interrupted path); heap growth
wants a *generous* bound (absorb JIT noise, still catch real leaks). Use
both; they fail on different bugs.

---

<a name="e1"></a>
## E1 — Two copies of effect

**Symptom.** Probe script crashes inside effect internals:

```
Error: BUG: yieldWrapGet - please report an issue at https://github.com/Effect-TS/effect/issues
```

**Cause.** The probe lived outside the repo and imported project modules by
absolute path while resolving `effect` from a different `node_modules` —
two module instances, two symbol registries; `yield*` handoff between them
explodes with this exact "BUG" message.

**Repair.** Run probes from inside the repo with relative imports (a
`.scratch/` dir, deleted after).

**Principle.** `yieldWrapGet`/TypeId-mismatch "BUG" errors from effect
internals mean duplicate effect instances, not an effect bug. Check import
resolution before filing anything.

---

<a name="e2"></a>
## E2 — CI green-red split: pin what you verified

**Symptom.** Full suite green locally for dozens of runs; first CI run
fails two legacy property tests:

```
Property failed after 1 tests
{ seed: -466859633, path: "0:0", endOnFailure: true }
Counterexample: [{__proto__:null,"_tag":"Move","dx":0}]
```

**Diagnosis before patching.** Reproduce with the CI *seed* locally:

```ts
await new TestSchema.Asserts(DeviceEvent).verifyLosslessTransformation({
  params: { numRuns: 200, seed: -466859633 },
})
// → PASSES locally
```

Same seed, same lockfile, different outcome ⇒ not the seed, the
*environment*: CI ran `bun-version: latest`, local was 1.3.14, and the
newer runtime diverges on null-prototype object equality inside the test
framework. That flips the correct fix from "tweak the test/seed" to:

```yaml
- uses: oven-sh/setup-bun@v2
  with:
    bun-version: 1.3.14   # the version the suite is verified against
```

**Principle.** Never patch a CI-only failure until you have reproduced it —
the reproduction *attempt* is the diagnosis. Seed reproduces → generator
edge case (fix code or generator). Seed does not reproduce → environment
delta (pin the toolchain you actually verified; upgrade deliberately
later). CI on "latest" anything tests a moving target.

---

<a name="e3"></a>
## E3 — Effect 3 memory produces Effect 4 crashes

A compressed list of the rc.111 API-shape traps that produced real compile
AND runtime failures when written from Effect 3 muscle memory — full table
in [effect4-api-notes.md](effect4-api-notes.md):

- `Schema.Union(A, B)` **runtime-crashes** at module load:
  `TypeError: members.map is not a function`. It takes an array:
  `Schema.Union([A, B])`. Likewise `Schema.Record(keySchema, valueSchema)`
  takes two positional args.
- `Effect.fork` does not exist → `Effect.forkChild` (see C3).
- `Either` is gone from this role → `Result`
  (`Result.succeed/fail/isSuccess/isFailure`; interop via
  `Effect.fromResult`; `Schema.decodeUnknownResult` returns
  `Result<A, SchemaError>`).
- `Stream.unwrapScoped` does not exist → `Stream.unwrap` already handles
  the `Scope` (excludes it from R; scope closes with the stream).
- `Layer.effect(Tag)(effect)` excludes `Scope` from R — scoped acquisition
  inside layers just works; finalizers run at layer teardown.
- `Stream.runCollect` returns `Array<A>` (not `Chunk`).
- Unknown-field passthrough: `Schema.StructWithRest(struct, [Schema.Record(...)])`.
- Typed errors: `Schema.TaggedError<Self>()("Tag", fields)` — yieldable,
  serializable, and `Effect.catchTag` matches on the tag string.

**Principle.** The installed package is the only authority:
`grep "export declare" node_modules/effect/dist/<Module>.d.ts` before
using any API you have not used *in this exact version*. Ten seconds of
grep beats a module-load-time crash in someone else's test run.
