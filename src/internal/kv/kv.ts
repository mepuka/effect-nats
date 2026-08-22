import { Context, Effect, Layer, Option, Result, Schema, Stream } from "effect"
import type {
  ConsumeEvent,
  ConsumerLagged,
  RollupNotPermitted,
  StartPosition,
  StoredMessage,
  StreamNotFound,
  SubjectNotBound
} from "../jetstream/substrate.ts"
import { JetStreamSubstrate, rollupHeader, rollupSubject } from "../jetstream/substrate.ts"
import {
  keyFromSubject,
  KvBucketName,
  KvKey,
  KvKeyFilter,
  streamForBucket,
  subjectForKey,
  subjectPrefix
} from "./naming.ts"

// KV materialized view over the JetStream substrate, per kv-slice-scoping.md.
// Operation identity travels in headers, never in names (research doc): a
// delete is a `KV-Operation: DEL` message, a purge additionally rolls up.
const operationHeader = "KV-Operation"
const deleteOperation = "DEL"
const purgeOperation = "PURGE"

export type KvOperation = "Put" | "Del" | "Purge"

export interface KvEntry {
  readonly bucket: KvBucketName
  readonly key: KvKey
  readonly value: Uint8Array
  readonly revision: bigint
  readonly operation: KvOperation
  readonly timestampMillis: number
}

export type KvWatchEvent =
  | { readonly _tag: "Entry"; readonly entry: KvEntry }
  | { readonly _tag: "CaughtUp" }

export interface KvStatus {
  readonly bucket: KvBucketName
  readonly history: number
}

export interface OpenOptions {
  /** Per-key history depth for created buckets; defaults to 1. */
  readonly history?: number
  /** "create" (default) creates or verifies; "bind" requires a KV-shaped stream. */
  readonly mode?: "create" | "bind"
}

export interface WatchOptions {
  /** Raw selector, parsed against the strict filter grammar (ADR-0003). */
  readonly selector?: string
  readonly updatesOnly?: boolean
  readonly fromRevision?: bigint
  /**
   * Watch buffer bound (default 1024). Overflow terminates the watch with a
   * typed `KvWatchLagged` carrying the last observed revision (ADR-0005);
   * resume with `fromRevision: lastRevision + 1n`.
   */
  readonly bufferMessages?: number
}

export class KvBucketMissing extends Schema.TaggedError<KvBucketMissing>()("KvBucketMissing", {
  bucket: Schema.String
}) {}

export class KvOpenConflict extends Schema.TaggedError<KvOpenConflict>()("KvOpenConflict", {
  bucket: Schema.String
}) {}

export class KvBucketShapeMismatch
  extends Schema.TaggedError<KvBucketShapeMismatch>()("KvBucketShapeMismatch", {
    bucket: Schema.String
  })
{}

export class KvConflict extends Schema.TaggedError<KvConflict>()("KvConflict", {
  bucket: Schema.String,
  key: Schema.String,
  expected: Schema.BigInt,
  actual: Schema.BigInt
}) {}

export class KvInvalidFilter extends Schema.TaggedError<KvInvalidFilter>()("KvInvalidFilter", {
  bucket: Schema.String,
  selector: Schema.String
}) {}

/** A wire entry violating the naming or operation grammar (ADR-0003: reads fail typed). */
export class KvEntryViolation extends Schema.TaggedError<KvEntryViolation>()("KvEntryViolation", {
  bucket: Schema.String,
  subject: Schema.String,
  reason: Schema.Literals(["prefix-mismatch", "invalid-key", "invalid-operation"])
}) {}

/** The watch buffer overflowed (ADR-0005); resume from `lastRevision + 1n`. */
export class KvWatchLagged extends Schema.TaggedError<KvWatchLagged>()("KvWatchLagged", {
  bucket: Schema.String,
  lastRevision: Schema.BigInt
}) {}

export interface KvBucket {
  readonly get: (
    key: KvKey
  ) => Effect.Effect<Option.Option<KvEntry>, KvBucketMissing | KvEntryViolation>
  readonly put: (key: KvKey, value: Uint8Array) => Effect.Effect<bigint, KvBucketMissing>
  readonly create: (
    key: KvKey,
    value: Uint8Array
  ) => Effect.Effect<bigint, KvBucketMissing | KvConflict>
  readonly update: (
    key: KvKey,
    value: Uint8Array,
    expected: bigint
  ) => Effect.Effect<bigint, KvBucketMissing | KvConflict>
  readonly delete: (key: KvKey) => Effect.Effect<void, KvBucketMissing>
  readonly purge: (key: KvKey) => Effect.Effect<void, KvBucketMissing>
  readonly history: (
    key: KvKey
  ) => Stream.Stream<KvEntry, KvBucketMissing | KvEntryViolation | KvWatchLagged>
  readonly watch: (
    options?: WatchOptions
  ) => Stream.Stream<
    KvWatchEvent,
    KvBucketMissing | KvEntryViolation | KvInvalidFilter | KvWatchLagged
  >
  readonly keys: (
    options?: WatchOptions
  ) => Stream.Stream<KvKey, KvBucketMissing | KvEntryViolation | KvInvalidFilter | KvWatchLagged>
  readonly status: () => Effect.Effect<KvStatus, KvBucketMissing>
}

export interface KvService {
  readonly open: (
    bucket: KvBucketName,
    options?: OpenOptions
  ) => Effect.Effect<KvBucket, KvBucketMissing | KvOpenConflict | KvBucketShapeMismatch>
  readonly destroy: (bucket: KvBucketName) => Effect.Effect<void, KvBucketMissing>
}

const decodeFilter = Schema.decodeUnknownResult(KvKeyFilter)

const resultToEffect = <A, E>(result: Result.Result<A, E>): Effect.Effect<A, E> =>
  Result.match(result, { onFailure: Effect.fail, onSuccess: Effect.succeed })

const operationOf = (message: StoredMessage): KvOperation | undefined => {
  const value = message.headers.get(operationHeader)
  if (value === undefined) return "Put"
  if (value === deleteOperation) return "Del"
  if (value === purgeOperation) return "Purge"
  return undefined
}

const emptyPayload = new Uint8Array(0)

export const makeKv = (substrate: JetStreamSubstrate["Service"]): KvService => {
  const open: KvService["open"] = (bucket, options) =>
    Effect.gen(function*() {
      const stream = streamForBucket(bucket)
      const subjects = [`${subjectPrefix(bucket)}>`]
      if (options?.mode === "bind") {
        const config = yield* substrate.getStream(stream).pipe(
          Effect.mapError(() => new KvBucketMissing({ bucket }))
        )
        // The bind-time shape guard the reference clients lack (research doc):
        // a plain stream squatting on the KV_ name must not pass as a bucket.
        const shaped = config.subjects.length === 1 && config.subjects[0] === subjects[0]
        if (!shaped) {
          return yield* new KvBucketShapeMismatch({ bucket })
        }
        return makeBucket(bucket, stream, config.maxMessagesPerSubject)
      }
      const history = options?.history ?? 1
      yield* substrate.createStream({
        name: stream,
        subjects,
        maxMessagesPerSubject: history,
        allowRollup: true
      }).pipe(Effect.mapError(() => new KvOpenConflict({ bucket })))
      return makeBucket(bucket, stream, history)
    })

  const makeBucket = (bucket: KvBucketName, stream: string, depth: number): KvBucket => {
    const bucketMissing = () => new KvBucketMissing({ bucket })

    const consumeError = (
      error: StreamNotFound | ConsumerLagged
    ): KvBucketMissing | KvWatchLagged =>
      error._tag === "ConsumerLagged"
        ? new KvWatchLagged({ bucket, lastRevision: error.lastDeliveredSequence })
        : bucketMissing()

    // Unreachable by construction: subjects derive from parsed names and buckets
    // are created with rollup allowed, so these are defects, not outcomes.
    const publishInvariants = <A, E>(
      effect: Effect.Effect<A, E | SubjectNotBound | RollupNotPermitted>
    ) =>
      effect.pipe(Effect.catchTag(["SubjectNotBound", "RollupNotPermitted"], (e) => Effect.die(e)))

    const toEntry = (
      key: KvKey,
      message: StoredMessage
    ): Result.Result<KvEntry, KvEntryViolation> => {
      const operation = operationOf(message)
      if (operation === undefined) {
        return Result.fail(
          new KvEntryViolation({ bucket, subject: message.subject, reason: "invalid-operation" })
        )
      }
      return Result.succeed({
        bucket,
        key,
        value: message.payload,
        revision: message.sequence,
        operation,
        timestampMillis: message.timestampMillis
      })
    }

    const parseEntry = (message: StoredMessage): Result.Result<KvEntry, KvEntryViolation> =>
      Result.match(keyFromSubject(bucket, message.subject), {
        onFailure: (violation) =>
          Result.fail(
            new KvEntryViolation({ bucket, subject: message.subject, reason: violation.reason })
          ),
        onSuccess: (key) => toEntry(key, message)
      })

    const startFor = (options?: WatchOptions): StartPosition => {
      if (options?.fromRevision !== undefined) {
        return { _tag: "FromSequence", sequence: options.fromRevision }
      }
      return options?.updatesOnly === true ? { _tag: "NewOnly" } : { _tag: "LastPerSubject" }
    }

    const filterFor = (
      options?: WatchOptions
    ): Result.Result<string, KvInvalidFilter> => {
      if (options?.selector === undefined) {
        return Result.succeed(`${subjectPrefix(bucket)}>`)
      }
      return Result.match(decodeFilter(options.selector), {
        onFailure: () =>
          Result.fail(new KvInvalidFilter({ bucket, selector: options.selector ?? "" })),
        onSuccess: (filter) => Result.succeed(`${subjectPrefix(bucket)}${filter}`)
      })
    }

    const events = (options?: WatchOptions) =>
      Stream.unwrap(
        Effect.gen(function*() {
          const filter = yield* resultToEffect(filterFor(options))
          return substrate.consume(stream, {
            filters: [filter],
            start: startFor(options),
            buffer: { _tag: "TerminateOnLag", messages: options?.bufferMessages ?? 1024 }
          }).pipe(
            Stream.mapError(consumeError),
            Stream.mapEffect((event: ConsumeEvent) =>
              event._tag === "CaughtUp"
                ? Effect.succeed<KvWatchEvent>({ _tag: "CaughtUp" })
                : Effect.map(
                  resultToEffect(parseEntry(event.message)),
                  (entry): KvWatchEvent => ({ _tag: "Entry", entry })
                )
            )
          )
        })
      )

    return {
      get: (key) =>
        substrate.lastMessageForSubject(stream, subjectForKey(bucket, key)).pipe(
          Effect.flatMap((message) =>
            Effect.map(resultToEffect(toEntry(key, message)), (entry) =>
              entry.operation === "Put" ? Option.some(entry) : Option.none())
          ),
          Effect.catchTag("NoMessageForSubject", () =>
            Effect.succeedNone),
          Effect.catchTag("StreamNotFound", () => Effect.fail(bucketMissing()))
        ),

      put: (key, value) =>
        publishInvariants(substrate.publish(stream, subjectForKey(bucket, key), value)).pipe(
          Effect.catchTag("WrongLastSequence", (e) => Effect.die(e)),
          Effect.mapError(bucketMissing)
        ),

      create: (key, value) =>
        Effect.gen(function*() {
          const subject = subjectForKey(bucket, key)
          const first = yield* publishInvariants(
            substrate.publish(stream, subject, value, { expectedLastSubjectSequence: 0n })
          ).pipe(Effect.result)
          if (Result.isSuccess(first)) return first.success
          const failure = first.failure
          if (failure._tag === "StreamNotFound") return yield* bucketMissing()
          // key exists: succeed through a tombstone, conflict on a live value
          const last = yield* substrate.lastMessageForSubject(stream, subject).pipe(
            Effect.catchTag("StreamNotFound", () => Effect.fail(bucketMissing())),
            Effect.catchTag(
              "NoMessageForSubject",
              () => Effect.fail(new KvConflict({ bucket, key, expected: 0n, actual: 0n }))
            )
          )
          const operation = operationOf(last)
          if (operation === "Del" || operation === "Purge") {
            return yield* publishInvariants(
              substrate.publish(stream, subject, value, {
                expectedLastSubjectSequence: last.sequence
              })
            ).pipe(
              Effect.catchTag("StreamNotFound", () => Effect.fail(bucketMissing())),
              Effect.catchTag(
                "WrongLastSequence",
                (e) =>
                  Effect.fail(
                    new KvConflict({ bucket, key, expected: e.expected, actual: e.actual })
                  )
              )
            )
          }
          return yield* new KvConflict({ bucket, key, expected: 0n, actual: last.sequence })
        }),

      update: (key, value, expected) =>
        publishInvariants(
          substrate.publish(stream, subjectForKey(bucket, key), value, {
            expectedLastSubjectSequence: expected
          })
        ).pipe(
          Effect.catchTag("StreamNotFound", () => Effect.fail(bucketMissing())),
          Effect.catchTag(
            "WrongLastSequence",
            (e) =>
              Effect.fail(new KvConflict({ bucket, key, expected: e.expected, actual: e.actual }))
          )
        ),

      delete: (key) =>
        publishInvariants(
          substrate.publish(stream, subjectForKey(bucket, key), emptyPayload, {
            headers: new Map([[operationHeader, deleteOperation]])
          })
        ).pipe(
          Effect.catchTag("WrongLastSequence", (e) => Effect.die(e)),
          Effect.mapError(bucketMissing),
          Effect.asVoid
        ),

      purge: (key) =>
        publishInvariants(
          substrate.publish(stream, subjectForKey(bucket, key), emptyPayload, {
            headers: new Map([
              [operationHeader, purgeOperation],
              [rollupHeader, rollupSubject]
            ])
          })
        ).pipe(
          Effect.catchTag("WrongLastSequence", (e) => Effect.die(e)),
          Effect.mapError(bucketMissing),
          Effect.asVoid
        ),

      history: (key) =>
        substrate.consume(stream, {
          filters: [subjectForKey(bucket, key)],
          start: { _tag: "AllHistory" },
          // one key's full recorded history fits by construction (bucket depth
          // captured at open); depth 0 means unlimited history, so fall back
          buffer: { _tag: "TerminateOnLag", messages: depth > 0 ? depth : 1024 }
        }).pipe(
          Stream.mapError(consumeError),
          Stream.takeWhile((event) => event._tag !== "CaughtUp"),
          Stream.mapEffect((event) => resultToEffect(toEntry(key, event.message)))
        ),

      watch: events,

      keys: (options) =>
        events(options).pipe(
          Stream.takeWhile((event) => event._tag !== "CaughtUp"),
          Stream.filter((event) => event.entry.operation === "Put"),
          Stream.map((event) => event.entry.key)
        ),

      status: () =>
        substrate.getStream(stream).pipe(
          Effect.mapError(bucketMissing),
          Effect.map((config) => ({ bucket, history: config.maxMessagesPerSubject }))
        )
    }
  }

  return {
    open,
    destroy: (bucket) =>
      substrate.deleteStream(streamForBucket(bucket)).pipe(
        Effect.mapError(() => new KvBucketMissing({ bucket }))
      )
  }
}

export class Kv extends Context.Service<Kv, KvService>()("effect-nats/internal/kv/Kv") {
  static readonly layer: Layer.Layer<Kv, never, JetStreamSubstrate> = Layer.effect(
    Kv,
    Effect.gen(function*() {
      return makeKv(yield* JetStreamSubstrate)
    })
  )
}
