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
  `Cause.Done` in E; `Stream.fromQueue` strips it.
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

Background loops that must never die silently:

```ts
const restart = Schedule.spaced(restartMs)
const supervised = <A, E>(loop: Effect.Effect<A, E, never>) =>
  loop.pipe(Effect.retry(restart), Effect.repeat(restart), Effect.forkScoped)
```

`retry` covers failure, `repeat` covers unexpected normal completion, the
scope covers shutdown. Combine with a success-only readiness `Deferred`
(case C1/C2) so consumers can await the loop actually being bound.
