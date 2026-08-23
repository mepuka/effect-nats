# Effect 4 (rc-era) API notes for streaming, concurrency, and testing

Verified against `effect@4.0.0-rc.111`. Effect 4 is close enough to Effect 3
that memory will betray you, and different enough that it does. The
installed package is the authority — when in doubt:

```bash
grep -n "export declare" node_modules/effect/dist/Stream.d.ts | grep <name>
```

## Renames and reshapes that bite

| You remember (v3)               | rc.111 reality                                                                                                                                             |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Effect.fork`                   | `Effect.forkChild(eff, { startImmediately?: boolean })`; also `forkScoped`, `forkIn`, `forkDetach`                                                         |
| `Either.left/right`             | `Result.fail/succeed`, `Result.isFailure/isSuccess`; `Effect.fromResult`                                                                                   |
| `Schema.Union(A, B)` (variadic) | `Schema.Union([A, B])` — variadic form **crashes at runtime**: `members.map is not a function`                                                             |
| `Schema.Record({ key, value })` | `Schema.Record(keySchema, valueSchema)`                                                                                                                    |
| `Stream.unwrapScoped`           | `Stream.unwrap` — signature `Effect<Stream<A,E2,R2>, E, R> → Stream<A, E \| E2, R2 \| Exclude<R, Scope>>`; it owns the scope                               |
| `Stream.runCollect → Chunk<A>`  | returns `Array<A>`                                                                                                                                         |
| `Schema.TaggedError` variants   | `class E extends Schema.TaggedError<E>()("Tag", { field: Schema.String }) {}` — yieldable (`yield* new E({...})`), catch via `Effect.catchTag("Tag", ...)` |
| custom passthrough decoding     | `Schema.StructWithRest(Schema.Struct({...}), [Schema.Record(Schema.String, Schema.Unknown)])`                                                              |
| `Schema.filter` refinements     | `Schema.Number.pipe(Schema.check(Schema.isGreaterThan(0)))` and friends                                                                                    |

Other load-bearing facts:

- `Layer.effect(Tag)(effect)` excludes `Scope` from R — `acquireRelease`
  inside a layer works; finalizers run at layer teardown. Layer error/env
  types INFER; don't annotate them narrower than reality (exactOptional
  builds surface this as confusing assignability errors).
- `Schema.decodeUnknownResult(S)(u)` → `Result<S["Type"], SchemaError>`.
- `Semaphore`: `makeUnsafe(n)`, `withPermits`, `withPermitsIfAvailable`
  (returns `Option<A>`), `takeIfAvailable(sem, n)` → `Effect<boolean>`,
  `release(sem, n)`. Pair take/release through `Effect.ensuring`.
- `Deferred.make<A>()`, `Deferred.succeed(d, value)`, `Deferred.await(d)`.
- `PubSub.sliding(capacity)` / `bounded` / `dropping`; per-subscriber
  streams via `Stream.fromSubscription` after `PubSub.subscribe` (scoped).
- `Queue.bounded/dropping/sliding/unbounded<A, E>`; end-capable queues take
  `Cause.Done` in E; `Stream.fromQueue` strips it. `bounded(n)` alone means
  `strategy: "suspend"` — see "What the runtime decides" below for what that
  and every other queue/scope default actually do.
- `Effect.catchDefect` for defects; `Cause.hasInterruptsOnly(cause)` to let
  pure interruption through while translating real defects.
- Filters: `Stream.filterMap` takes `(a) => Result<B, X>` — any
  Result-returning decode function slots in directly.

## Stream semantics worth relying on

- **`Stream.timeoutOrElse({ duration, orElse })` is per-pull** — the timer
  resets around each pull; it is already an idle-gap timeout. `orElse`
  switches to a fallback stream (`() => Stream.fail(...)` for a typed
  timeout error).
- **`Stream.takeWhile` stops pulling upstream** after the predicate fails —
  a `Stream.concat(upstream, sentinel)` continuation is only reached when
  upstream ends _without_ the terminating element (the S1 pattern).
- **`Stream.fromAsyncIterable(it, onError)`** registers the iterator's
  `return` as a finalizer — but for resources with explicit lifecycle
  (subscriptions), still wrap acquisition in `Effect.acquireRelease` and
  release explicitly; don't rely on iterator semantics of a third-party
  library.
- Error channels JOIN under `Stream.unwrap` / `mapEffect` — translate seam
  errors to domain errors at the seam.

## What the runtime decides: Queue, Scope, Stream.unwrap, Semaphore (rc.111, cited)

Paths are `node_modules/effect/src/…`. Record these as named assumptions before modelling a
protocol over Streams — they decide what your consumer observes.

- **`bounded` names the bound, not the behavior at it.** `Queue.bounded(n)` is
  `make({ capacity: n })` (`Queue.ts:500`); `make` defaults `strategy` to `"suspend"`
  (`Queue.ts:448-468`).
- **A parked producer is not an error.** `offer` on a non-`Open` queue returns `false` without
  failing; at capacity under `"suspend"` it parks the fiber as a `Single` entry in `state.offers`
  (`Queue.ts:645-668`, `:2002-2015`). Discarding `offer`'s boolean silently accepts "the consumer
  was gone".
- **`size` counts buffered items only** — `sizeUnsafe` is `messages.length`, `0` when `Done`
  (`Queue.ts:1711`, `:1789`). An occupancy check never sees parked offerers.
- **`fail` delivers the buffer, then the error.** `failCauseUnsafe` finalizes at once only when
  buffer and parked offers are both empty; otherwise the state becomes `Closing` and the buffer is
  kept (`Queue.ts:872`, `:958-962`, `:1000-1015`). `takeUnsafe` returns the stored exit only when
  `Done` and keeps dequeuing while `Closing` (`:1606-1613`); `releaseCapacity` finalizes a
  `Closing` queue only once buffer and offers are empty, and otherwise re-admits parked offers up
  to capacity, resuming each with `true` (`:2037-2069`).
- **`shutdown` discards the buffer** and resumes every parked `Single` offer with `false` — no
  error, no interruption (`Queue.ts:1191-1210`, constants `:1950-1953`). `Queue.interrupt` is a
  different operation: `failCause` with an interrupt cause (`:1153-1154`).
- **One `Stream.fromQueue` pull drains the whole buffer.** `fromQueue` → `Channel.fromQueueArray`
  → `Queue.takeAll` = `takeBetween(1, +∞)`; `takeBetweenUnsafe` takes
  `MutableList.takeN(messages, max)` (clamped to the buffer length), then releases capacity
  (`Stream.ts:1132-1133`, `Channel.ts:1229-1231`, `Queue.ts:1297-1298`, `:1426-1433`,
  `:1977-1998`, `MutableList.ts:418-425`). Pull count is not message count; occupancy drops to
  `0`, not by one, and parked offers are re-admitted in the same step.
- **The consumer never runs inside the producer's critical section.** Taker wake-up after an offer
  is `dispatcher.scheduleTask(…, 0)` (`Queue.ts:1969-1975`); who pulls when is scheduler-chosen.
- **`capacity <= 0` has its own paths** (`Queue.ts:654-658`, `:1614-1620`, `:1986-1993`).
  `bounded(0)` is a rendezvous — reject a `0` capacity at the boundary instead of letting a
  sentinel reach it.
- **Scope releases run LIFO.** `acquireRelease` stores the release in the scope's
  insertion-ordered finalizer `Map` (`internal/effect.ts:3942-3958`, `:3863-3873`); close walks it
  backwards (`:3802-3823`, loop at `:3811`), sequentially by default (`:3886-3891`).
- **`Stream.unwrap`'s setup runs at the first pull.** `unwrap` is `Channel.unwrap`
  (`Stream.ts:1633-1635`), which runs the effect with the channel's scope provided and memoizes
  the pull (`Channel.ts:9039-9052`). Holding the stream value proves nothing was registered.
- **`Effect.forEach` is sequential by default** — `concurrency` defaults to `1` and dispatches to
  `forEachSequential` (`internal/effect.ts:4645-4653`, `:4677-4700`).
- **A parked fiber keeps its permit.** `Semaphore.withPermits(n)` runs the body after `taken += n`
  and releases in an exit handler around the body (`Semaphore.ts:287-307`; `withPermit` is
  `withPermits(1)`, `:309`). A suspension inside a critical section extends the critical section.

## effect/testing

```ts
import { FastCheck as fc, TestClock } from "effect/testing"
```

- Provide with `Effect.provide(TestClock.layer())` — it provides the
  `Clock` service, so all `Effect.sleep` / `Schedule` / `Stream` timeouts
  are driven by `TestClock.adjust(millis)`.
- `adjust` fires timers _registered at that moment_, running woken fibers
  as it goes. Consumers must reach their first suspension before you
  adjust — hence:

```ts
const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const fiber = yield * forkNow(Effect.flip(Stream.runCollect(stream)))
yield * TestClock.adjust(60_001)
const typedError = yield * Fiber.join(fiber) // flip: error surfaces as success
```

- `FastCheck` is a fast-check re-export. `Filter`-style arbitraries compose
  with Result-returning functions. For round-trip properties over records,
  exclude `__proto__` keys and stick to integers/strings/booleans unless
  you specifically mean to test float/prototype edge cases (see case E2).
- `TestSchema.Asserts(schema).verifyLosslessTransformation({ params })`
  accepts fast-check params including `seed` — the tool for reproducing CI
  property failures locally.

## Supervision pattern

Background loops that must never die silently — classify, bound, and expose
exhaustion. `Schedule.spaced` has no stop condition, so a bare `spaced` retry
loops forever over fatal errors (authorization, protocol, configuration) and
never reports exhaustion:

```ts
// Sanctioned shape: bounded + jittered, classified, observable exhaustion.
class LoopError extends Data.TaggedError("LoopError")<{
  readonly reason: "transient" | "fatal"
}> {}

const restart = Schedule.exponential("200 millis").pipe(
  Schedule.jittered,
  Schedule.upTo({ duration: "30 seconds" }) // retry-window bound: checked between attempts; an in-flight attempt may overrun
)

const supervised = (loop: Effect.Effect<void, LoopError>) =>
  loop.pipe(
    // classify: only typed transient reasons retry; fatal variants fall through
    Effect.retry({ schedule: restart, while: (error) => error.reason === "transient" }),
    // fatal errors and exhaustion surface here, and the loop ENDS after this
    // recovery: wire a real status surface (Deferred/Queue/state) at this spot
    // in production code — the log below is the observable minimum, not the
    // whole contract
    Effect.catch((error) => Effect.logError("background loop exhausted", error)),
    Effect.forkScoped
  )
```

Rules: (1) retry only typed transient reasons at the narrowest idempotent
boundary; (2) always bound with `upTo` (a retry-window bound evaluated between attempts — never an attempt deadline; model hard deadlines separately) or an attempt cap, and jitter;
(3) exhaustion must be visible — log, metric, or status event — never a
silent stop; (4) `repeat` for unexpected normal completion only where normal
completion is actually a fault; (5) continuous reconnect-style looping is
allowed only through explicit configuration plus an ADR, with observable
status; (6) prove exact timing, non-retry of fatal reasons, exhaustion,
schedule reset, interruption, and scope finalization under `TestClock`.
Combine with a success-only readiness `Deferred` (case C1/C2) so consumers
can await the loop actually being bound.
