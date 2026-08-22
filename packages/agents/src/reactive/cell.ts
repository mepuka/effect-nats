/**
 * Cell — a reactive, schema-typed value backed by exactly one KV message.
 *
 * The user-facing facade: all the machinery (subjects, registry, grades,
 * interest, CAS) is internal. A cell IS one JetStream KV entry:
 * - `Nats-Msg-Id = key@revision` → dedup window catches replays
 * - CAS on revision → apply is idempotent → redelivery is a no-op
 *   (this is "effectively-once per key": at-least-once delivery + idempotent
 *   apply; our Lean model keeps us honest that it is NOT exactly-once
 *   delivery)
 * - Reactivity service keys off the cell key: updates invalidate, `changes`
 *   re-runs.
 */
import { Effect, Option, Schema, Stream } from "effect"
import { Reactivity } from "effect/unstable/reactivity/Reactivity"
import { issue, Registry } from "../core/registry.js"
import { mkSubject, type Subject } from "../core/subjects.js"
import { encodeWire, decodeWire } from "../schema/wire.js"
import { Nats, type KvConflict } from "../nats/service.js"

/** Cells share one internal registry: subjects stay closed-world, invisibly. */
const cells = new Registry()

export interface Cell<A> {
  readonly key: string
  readonly subject: Subject<readonly string[]>
  /** Current value (creates the cell with its initial value if needed). */
  readonly get: Effect.Effect<A>
  /** CAS'd update; retries conflict once against a fresh revision. */
  readonly update: (f: (a: A) => A) => Effect.Effect<A>
  /** Reactive stream of values — re-runs when the cell changes. */
  readonly changes: Stream.Stream<A>
}

export const make = <A>(options: {
  readonly key: string
  readonly schema: Schema.Codec<A>
  readonly initialValue: A
}): Effect.Effect<Cell<A>, never, Nats | Reactivity> =>
  Effect.gen(function* () {
    const nats = yield* Nats
    const reactivity = yield* Reactivity

    // closed-world bookkeeping: the cell's subject exists because WE made it
    void issue(cells, ["cell", ...options.key.split(".")], options.schema)
    const subject = mkSubject(["cell", ...options.key.split(".")])
    if (subject === null) {
      return yield* Effect.die(new Error(`invalid cell key: ${options.key}`))
    }

    const encode = encodeWire(options.schema)
    const decode = decodeWire(options.schema)

    const readEntry = Effect.gen(function* () {
      const entry = yield* nats.kvGet(options.key)
      return Option.isSome(entry) ? decode(entry.value.payload) : options.initialValue
    })

    // ensure exactly one message exists for this cell from birth
    yield* Effect.gen(function* () {
      const entry = yield* nats.kvGet(options.key)
      if (Option.isNone(entry)) {
        yield* nats
          .kvPut(options.key, 0, encode(options.initialValue))
          .pipe(Effect.catchTag("effect-nats-jetstream/KvConflict", () => Effect.void)) // someone else created it
      }
    })

    const get: Effect.Effect<A> = readEntry

    const update = (f: (a: A) => A): Effect.Effect<A> => {
      const attempt: Effect.Effect<A, KvConflict> = Effect.gen(function* () {
        const entry = yield* nats.kvGet(options.key)
        const current = Option.isSome(entry) ? decode(entry.value.payload) : options.initialValue
        const next = f(current)
        const revision = entry._tag === "Some" ? entry.value.revision : 0
        yield* nats.kvPut(options.key, revision, encode(next))
        return next
      })
      // CAS retry: on conflict, re-read fresh state and try again
      const loop = (): Effect.Effect<A> =>
        attempt.pipe(
          Effect.catchTag("effect-nats-jetstream/KvConflict", () => loop()),
          Effect.tap(() => reactivity.invalidate([options.key])),
        )
      return loop()
    }

    const changes: Stream.Stream<A> = reactivity.stream([options.key], get)

    return {
      key: options.key,
      subject,
      get,
      update,
      changes,
    }
  })
