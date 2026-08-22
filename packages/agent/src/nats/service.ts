/**
 * The NATS service — Context key + simulator layer.
 *
 * Algebraic shape: the interface methods are descriptions of work (Effects),
 * not executed wrappers. State lives in a SynchronizedRef so dedup
 * check-then-append is serialized across fibers; the connection-like resource
 * lifecycle is modeled with Layer.effect + finalizers; taps are Stream views
 * over a bounded PubSub.
 *
 * Named divergence from real JetStream (mirrors Lean): the dedup set never
 * expires — this models an INFINITE dedup window. Window expiry is a
 * refinement obligation, not silently assumed away.
 */
import {
  Context,
  Effect,
  Layer,
  Option,
  PubSub,
  Ref,
  Schema,
  Stream,
  SynchronizedRef,
} from "effect"
import type { Guarantee } from "./grade.js"
import type { Subject } from "../core/subjects.js"

export class PublishError extends Schema.TaggedError<PublishError>()(
  "effect-nats-agents/PublishError",
  { subject: Schema.String },
) {}

/** CAS failure: someone else wrote `key` between our read and write. */
export class KvConflict extends Schema.TaggedError<KvConflict>()(
  "effect-nats-agents/KvConflict",
  {
    key: Schema.String,
    expectedRevision: Schema.Number,
    actualRevision: Schema.Number,
  },
) {}

export interface PubAck {
  readonly seq: number
  readonly duplicate: boolean
}

/** One published message as the simulator records it. Payload is wire JSON. */
export interface Published {
  readonly subject: Subject
  readonly payload: string
  readonly guarantee: Guarantee
}

/**
 * Minimal server model: append-only log + seen dedup ids.
 */
export interface ServerModel {
  readonly log: readonly Published[]
  readonly seen: readonly string[]
}

export class Nats extends Context.Service<Nats, NatsShape>()("effect-nats-agents/Nats") {
  static readonly layer: Layer.Layer<Nats> = Layer.effect(Nats)(
    Effect.gen(function* () {
      const state = yield* SynchronizedRef.make<ServerModel>({
        log: [],
        seen: [],
      })

      /** Fan-out of everything that lands in the simulated stream. */
      const inbox = yield* PubSub.bounded<Published>({ capacity: 256 })
      yield* Effect.addFinalizer(() => PubSub.shutdown(inbox))

      const corePublish = Effect.fn("Nats.corePublish")(function* (
        subject: Subject,
        payload: string,
      ) {
        const published: Published = { subject, payload, guarantee: "atMostOnce" }
        yield* SynchronizedRef.update(state, (m) => ({
          log: [...m.log, published],
          seen: m.seen,
        }))
        yield* PubSub.publish(inbox, published)
      })

      const jsPublish = Effect.fn("Nats.jsPublish")(function* (
        subject: Subject,
        payload: string,
      ) {
        const published: Published = { subject, payload, guarantee: "atLeastOnce" }
        const seq = yield* SynchronizedRef.modify(state, (m) => [
          m.log.length + 1,
          { log: [...m.log, published], seen: m.seen },
        ] as const)
        yield* PubSub.publish(inbox, published)
        return { seq, duplicate: false } satisfies PubAck
      })

      /**
       * Dedup check + append + ack resolve as ONE serialized transition:
       * a replayed id returns `duplicate: true` and lands nowhere.
       */
      const jsPublishDedup = Effect.fn("Nats.jsPublishDedup")(function* (
        id: string,
        subject: Subject,
        payload: string,
      ) {
        return yield* SynchronizedRef.modifyEffect(
          state,
          (m): Effect.Effect<readonly [PubAck, ServerModel]> => {
            if (m.seen.includes(id)) {
              return Effect.succeed([{ seq: m.log.length, duplicate: true }, m] as const)
            }
            const published: Published = { subject, payload, guarantee: "dedupWindow" }
            return Effect.gen(function* () {
              yield* PubSub.publish(inbox, published)
              return [
                { seq: m.log.length + 1, duplicate: false },
                { log: [...m.log, published], seen: [id, ...m.seen] },
              ] as const
            })
          },
        )
      })

      /**
       * Tap the simulated stream: every message that lands in the log is
       * offered to subscribers (backpressure at capacity 256).
       */
      const tap = (): Stream.Stream<Published> => Stream.fromPubSub(inbox)

      // -- KV: one revision counter per key, CAS by expected revision --------
      const store = new Map<string, KvEntry>()
      const kvGet = (key: string): Effect.Effect<Option.Option<KvEntry>> =>
        Effect.sync(() => Option.fromNullishOr(store.get(key)))
      const kvPut = (
        key: string,
        expectedRevision: number,
        payload: string,
      ): Effect.Effect<number, KvConflict> =>
        Effect.suspend(() => {
          const actual = store.get(key)?.revision ?? 0
          if (actual !== expectedRevision) {
            return Effect.fail(
              new KvConflict({ key, expectedRevision, actualRevision: actual }),
            )
          }
          const revision = actual + 1
          store.set(key, { revision, payload })
          return Effect.succeed(revision)
        })

      const model: Effect.Effect<ServerModel> = Ref.get(state)

      return Nats.of({
        corePublish,
        jsPublish,
        jsPublishDedup,
        tap,
        model,
        kvGet,
        kvPut,
      })
    }),
  )
}

export interface NatsShape {
  /** Fire-and-forget core publish. Grade: atMostOnce. */
  readonly corePublish: (subject: Subject, payload: string) => Effect.Effect<void>
  /** JetStream publish. Grade: atLeastOnce. */
  readonly jsPublish: (subject: Subject, payload: string) => Effect.Effect<PubAck>
  /** JetStream publish with dedup id. Grade: dedupWindow. */
  readonly jsPublishDedup: (
    id: string,
    subject: Subject,
    payload: string,
  ) => Effect.Effect<PubAck>
  /** Subscribe to everything landing in the simulated stream. */
  readonly tap: () => Stream.Stream<Published>
  /** Simulator inspection — not part of a production connection's shape. */
  readonly model: Effect.Effect<ServerModel>
  /**
   * KV surface (note 0004 P2 semantics): one revision counter per key,
   * CAS by expected revision (0 = create). Conflicts are typed errors.
   * Single internal bucket for now — cells namespace their own keys.
   */
  readonly kvGet: (key: string) => Effect.Effect<Option.Option<KvEntry>>
  readonly kvPut: (
    key: string,
    expectedRevision: number,
    payload: string,
  ) => Effect.Effect<number, KvConflict>
}

export interface KvEntry {
  readonly revision: number
  readonly payload: string
}
