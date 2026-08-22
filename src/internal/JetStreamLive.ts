import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  JetStreamApiError,
  jetstreamManager
} from "@nats-io/jetstream"
import type { JsMsg } from "@nats-io/jetstream"
import { headers as makeHeaders } from "@nats-io/nats-core"
import type { MsgHdrs, NatsConnection } from "@nats-io/nats-core"
import { Cause, Effect, Layer, Queue, Result, Schema, Stream } from "effect"
import type { ConsumeEvent, ConsumeOptions, StoredMessage, StreamConfig } from "./JetStream.ts"
import {
  ConsumerLagged,
  JetStream,
  NoMessageForSubject,
  RollupNotPermitted,
  SequenceNotProvablyExact,
  StreamConfigConflict,
  StreamNotFound,
  SubjectNotBound,
  WrongLastSequence
} from "./JetStream.ts"

// Live adapter over the official @nats-io/jetstream client (ADR-0002). This is
// the single number<->bigint conversion point of ADR-0004. Server error codes
// map to the seam's semantic tags here; each mapping is pinned by an
// integration test against a real nats-server.

type Attempt<T> = { readonly _tag: "ok"; readonly value: T } | {
  readonly _tag: "err"
  readonly cause: unknown
}

const attempt = <T>(run: () => Promise<T>): Effect.Effect<Attempt<T>> =>
  Effect.promise(() =>
    run().then(
      (value) => ({ _tag: "ok" as const, value }),
      (cause: unknown) => ({ _tag: "err" as const, cause })
    )
  )

const decodeSequenceString = Schema.decodeUnknownResult(Schema.BigIntFromString)

// ADR-0004 rule 1: string-borne sequences (DIRECT.GET headers, ack-subject
// tokens) decode losslessly; the digit-string guard turns a malformed value
// into a typed failure instead of a rounded number.
const exactFromString = (
  raw: string | undefined,
  stream: string,
  operation: string
): Result.Result<bigint, SequenceNotProvablyExact> => {
  if (raw !== undefined && raw !== "") {
    const decoded = decodeSequenceString(raw)
    if (Result.isSuccess(decoded) && decoded.success >= 0n) {
      return Result.succeed(decoded.success)
    }
  }
  return Result.fail(new SequenceNotProvablyExact({ stream, operation, raw: String(raw) }))
}

// ADR-0004 rule 2: a sequence that reached the adapter only as a JSON number
// converts only when provably exact — JSON.parse has already rounded anything
// beyond 2^53, so the operation fails closed.
const exactFromNumber = (
  value: number,
  stream: string,
  operation: string
): Result.Result<bigint, SequenceNotProvablyExact> =>
  Number.isSafeInteger(value)
    ? Result.succeed(BigInt(value))
    : Result.fail(new SequenceNotProvablyExact({ stream, operation, raw: String(value) }))

// Mirrors the official ack-subject parse (jsmsg.js parseInfo): the 9-token v1
// form gains "_","" at index 2, then the stream sequence is token 7.
const ackStreamSequenceToken = (reply: string): string | undefined => {
  const tokens = reply.split(".")
  if (tokens.length === 9) tokens.splice(2, 0, "_", "")
  if (tokens.length < 11 || tokens[0] !== "$JS" || tokens[1] !== "ACK") return undefined
  return tokens[7]
}

// DIRECT.GET responses carry their metadata as headers; these are transport
// facts, not message headers, and must not leak into the seam's header map.
const directMetadataHeaders = [
  "Nats-Stream",
  "Nats-Sequence",
  "Nats-Time-Stamp",
  "Nats-Subject",
  "Nats-Last-Sequence",
  "Nats-Num-Pending"
]

const errCode = (cause: unknown): number | undefined =>
  cause instanceof JetStreamApiError ? cause.apiError().err_code : undefined

const messageOf = (cause: unknown): string =>
  typeof cause === "object" && cause !== null && "message" in cause
    ? String((cause as { message: unknown }).message)
    : String(cause)

const causeChainIncludes = (cause: unknown, needle: string): boolean => {
  let current: unknown = cause
  for (let depth = 0; depth < 6 && typeof current === "object" && current !== null; depth++) {
    if (messageOf(current).includes(needle)) return true
    current = (current as { cause?: unknown }).cause
  }
  return false
}

const isStreamNotFound = (cause: unknown): boolean =>
  errCode(cause) === 10059 || messageOf(cause).includes("stream not found")

const toMsgHdrs = (input: ReadonlyMap<string, string> | undefined): MsgHdrs | undefined => {
  if (input === undefined || input.size === 0) return undefined
  const hdrs = makeHeaders()
  for (const [key, value] of input) hdrs.set(key, value)
  return hdrs
}

const fromMsgHdrs = (hdrs: MsgHdrs | undefined): ReadonlyMap<string, string> => {
  const map = new Map<string, string>()
  if (hdrs !== undefined) {
    for (const key of hdrs.keys()) {
      const value = hdrs.get(key)
      if (value !== "") map.set(key, value)
    }
  }
  return map
}

export const make = (connection: NatsConnection): Effect.Effect<JetStream["Service"]> =>
  Effect.gen(function*() {
    const js = jetstream(connection)
    const jsm = yield* Effect.promise(() => jetstreamManager(connection))

    // Fallback revision source for streams without allow_direct: the JSON
    // STREAM.MSG.GET body, whose number-typed sequence rule 2 fails closed on.
    const jsonLastMessage = (
      stream: string,
      subject: string
    ): Effect.Effect<
      StoredMessage,
      StreamNotFound | NoMessageForSubject | SequenceNotProvablyExact
    > =>
      Effect.gen(function*() {
        const result = yield* attempt(() =>
          jsm.streams.getMessage(stream, { last_by_subj: subject })
        )
        if (result._tag === "err") {
          const cause = result.cause
          if (errCode(cause) === 10037 || messageOf(cause).includes("no message found")) {
            return yield* new NoMessageForSubject({ stream, subject })
          }
          return yield* isStreamNotFound(cause)
            ? new StreamNotFound({ stream })
            : Effect.die(cause)
        }
        const stored = result.value
        if (stored === null) {
          return yield* new NoMessageForSubject({ stream, subject })
        }
        const sequence = yield* Effect.fromResult(
          exactFromNumber(stored.seq, stream, "lastMessageForSubject StoredMsg.seq")
        )
        return {
          subject: stored.subject,
          sequence,
          payload: stored.data,
          headers: fromMsgHdrs(stored.header),
          timestampMillis: stored.time.getTime()
        } satisfies StoredMessage
      })

    const service = JetStream.of({
      createStream: (config: StreamConfig) =>
        Effect.gen(function*() {
          const result = yield* attempt(() =>
            jsm.streams.add({
              name: config.name,
              subjects: [...config.subjects],
              max_msgs_per_subject: config.maxMessagesPerSubject,
              allow_rollup_hdrs: config.allowRollup,
              // ADR-0004 rule 1: DIRECT.GET is the lossless revision source,
              // so every stream this seam creates answers direct gets
              allow_direct: true
            })
          )
          if (result._tag === "ok") return
          if (
            errCode(result.cause) === 10058 || messageOf(result.cause).includes("already in use")
          ) {
            return yield* new StreamConfigConflict({ stream: config.name })
          }
          return yield* Effect.die(result.cause)
        }),

      getStream: (name) =>
        Effect.gen(function*() {
          const result = yield* attempt(() => jsm.streams.info(name))
          if (result._tag === "err") {
            return yield* isStreamNotFound(result.cause)
              ? new StreamNotFound({ stream: name })
              : Effect.die(result.cause)
          }
          const config = result.value.config
          return {
            name: config.name,
            subjects: [...config.subjects ?? []],
            maxMessagesPerSubject: config.max_msgs_per_subject ?? 0,
            allowRollup: config.allow_rollup_hdrs ?? false
          } satisfies StreamConfig
        }),

      deleteStream: (name) =>
        Effect.gen(function*() {
          const result = yield* attempt(() => jsm.streams.delete(name))
          if (result._tag === "ok") return
          return yield* isStreamNotFound(result.cause)
            ? new StreamNotFound({ stream: name })
            : Effect.die(result.cause)
        }),

      publish: (stream, subject, payload, options) =>
        Effect.gen(function*() {
          const expected = options?.expectedLastSubjectSequence
          // ADR-0004 rule 3: the CAS expectation is formatted from the bigint
          // by this adapter, never via a lossy number option
          const hdrs = toMsgHdrs(options?.headers)
            ?? (expected !== undefined ? makeHeaders() : undefined)
          if (expected !== undefined && hdrs !== undefined) {
            hdrs.set("Nats-Expected-Last-Subject-Sequence", `${expected}`)
          }
          const result = yield* attempt(() =>
            js.publish(subject, payload, hdrs !== undefined ? { headers: hdrs } : {})
          )
          if (result._tag === "ok") {
            // subject-routed: another stream capturing the publish means the
            // declared stream did not, which is this seam's StreamNotFound
            if (result.value.stream !== stream) {
              return yield* new StreamNotFound({ stream })
            }
            return yield* Effect.fromResult(
              exactFromNumber(result.value.seq, stream, "publish PubAck.seq")
            )
          }
          const cause = result.cause
          const message = messageOf(cause)
          if (errCode(cause) === 10071 || message.includes("wrong last sequence")) {
            const actual = /wrong last sequence: (\d+)/.exec(message)?.[1]
            return yield* new WrongLastSequence({
              stream,
              subject,
              expected: expected ?? 0n,
              actual: actual !== undefined ? BigInt(actual) : 0n
            })
          }
          if (message.toLowerCase().includes("rollup")) {
            return yield* new RollupNotPermitted({ stream })
          }
          if (causeChainIncludes(cause, "no responders") || message.includes("503")) {
            // nothing bound the subject: distinguish missing stream from a
            // live stream whose subjects do not cover this publish
            const exists = yield* Effect.promise(() =>
              jsm.streams.info(stream).then(() => true, () => false)
            )
            return yield* exists
              ? new SubjectNotBound({ stream, subject })
              : new StreamNotFound({ stream })
          }
          return yield* Effect.die(cause)
        }),

      lastMessageForSubject: (stream, subject) =>
        Effect.gen(function*() {
          // ADR-0004 rule 1: DIRECT.GET carries the sequence as the decimal
          // string header Nats-Sequence, so the revision source is lossless.
          const result = yield* attempt(() =>
            jsm.direct.getMessage(stream, { last_by_subj: subject })
          )
          if (result._tag === "err") {
            const cause = result.cause
            if (errCode(cause) === 10037 || messageOf(cause).includes("no message found")) {
              return yield* new NoMessageForSubject({ stream, subject })
            }
            if (isStreamNotFound(cause)) {
              return yield* new StreamNotFound({ stream })
            }
            if (causeChainIncludes(cause, "no responders") || messageOf(cause).includes("503")) {
              // no direct responder: the stream is missing, or it is a foreign
              // bind without allow_direct — then degrade to the JSON path,
              // which rule 2 guards with the safe-integer check
              const exists = yield* Effect.promise(() =>
                jsm.streams.info(stream).then(() => true, () => false)
              )
              if (!exists) {
                return yield* new StreamNotFound({ stream })
              }
              return yield* jsonLastMessage(stream, subject)
            }
            return yield* Effect.die(cause)
          }
          const stored = result.value
          if (stored === null) {
            return yield* new NoMessageForSubject({ stream, subject })
          }
          const sequence = yield* Effect.fromResult(exactFromString(
            stored.header.last("Nats-Sequence"),
            stream,
            "lastMessageForSubject Nats-Sequence"
          ))
          const headers = new Map(fromMsgHdrs(stored.header))
          for (const key of directMetadataHeaders) headers.delete(key)
          return {
            subject: stored.subject,
            sequence,
            payload: stored.data,
            headers,
            timestampMillis: stored.time.getTime()
          } satisfies StoredMessage
        }),

      consume: (stream, options: ConsumeOptions) =>
        Stream.unwrap(
          Effect.gen(function*() {
            const capacity = options.buffer.messages
            const start = options.start
            if (start._tag === "FromSequence" && start.sequence > 9007199254740991n) {
              return yield* new SequenceNotProvablyExact({
                stream,
                operation: "consume opt_start_seq",
                raw: `${start.sequence}`
              })
            }
            // Explicit ephemeral consumer: `add` resolving is the real
            // registration witness (probe: an ordered consumer created lazily
            // at consume() lost a message published after CaughtUp — the C1
            // fake-ready race, live edition).
            const created = yield* attempt(() =>
              jsm.consumers.add(stream, {
                ack_policy: AckPolicy.None,
                filter_subjects: [...options.filters],
                inactive_threshold: 60_000_000_000,
                ...(start._tag === "AllHistory" ? { deliver_policy: DeliverPolicy.All } : {}),
                ...(start._tag === "LastPerSubject"
                  ? { deliver_policy: DeliverPolicy.LastPerSubject }
                  : {}),
                ...(start._tag === "NewOnly" ? { deliver_policy: DeliverPolicy.New } : {}),
                ...(start._tag === "FromSequence"
                  ? {
                    deliver_policy: DeliverPolicy.StartSequence,
                    // lib option is number-typed; a start beyond 2^53 was
                    // refused above rather than silently rounded (ADR-0004)
                    opt_start_seq: Number(start.sequence)
                  }
                  : {})
              })
            )
            if (created._tag === "err") {
              return yield* new StreamNotFound({ stream })
            }
            const info = created.value
            yield* Effect.addFinalizer(() =>
              Effect.promise(() => jsm.consumers.delete(stream, info.name).then(() => {}, () => {}))
            )
            const gotten = yield* attempt(() => js.consumers.get(stream, info.name))
            if (gotten._tag === "err") {
              return yield* new StreamNotFound({ stream })
            }
            const consumer = gotten.value
            // ADR-0004 rule 1: the stream sequence is read from the raw
            // ack-subject token, not the already-parseInt'd DeliveryInfo
            const entryOf = (
              message: JsMsg
            ): Result.Result<
              { readonly _tag: "Entry"; readonly message: StoredMessage },
              SequenceNotProvablyExact
            > =>
              Result.map(
                exactFromString(
                  // `reply` is on JsMsgImpl but omitted from the public JsMsg
                  // type; an absent value fails typed through the same guard
                  ackStreamSequenceToken(
                    (message as { readonly reply?: string }).reply ?? ""
                  ),
                  stream,
                  "consume ack streamSequence"
                ),
                (sequence) => ({
                  _tag: "Entry" as const,
                  message: {
                    subject: message.subject,
                    sequence,
                    payload: message.data,
                    headers: fromMsgHdrs(message.headers),
                    timestampMillis: message.time.getTime()
                  }
                })
              )
            if (options.buffer._tag === "PullWindow") {
              // ADR-0005 pull window: the queue bound is the whole window. The
              // loop requests one message per `next()` and `Queue.offer`
              // suspends while the window is full, so the server is never
              // asked for more than the consumer can hold; publishers are
              // never suspended — the stream absorbs the backlog.
              const queue = yield* Effect.acquireRelease(
                Queue.bounded<ConsumeEvent, StreamNotFound | SequenceNotProvablyExact>(capacity),
                (q) => Queue.shutdown(q)
              )
              const caughtUpEvent: ConsumeEvent = { _tag: "CaughtUp" }
              const pullLoop = Effect.gen(function*() {
                let caughtUp = info.num_pending === 0
                if (caughtUp) yield* Queue.offer(queue, caughtUpEvent)
                while (true) {
                  const next = yield* attempt(() => consumer.next({ expires: 5_000 }))
                  if (next._tag === "err") {
                    // a terminal pull status (consumer or stream deleted)
                    // rejects `next`
                    yield* Queue.failCause(queue, Cause.fail(new StreamNotFound({ stream })))
                    return
                  }
                  const message = next.value
                  if (message === null) {
                    // idle expiry delivers nothing: probe the consumer so a
                    // silently vanished stream still terminates typed, and so
                    // history pruned before delivery still yields CaughtUp
                    const probe = yield* attempt(() => jsm.consumers.info(stream, info.name))
                    if (probe._tag === "err") {
                      yield* Queue.failCause(queue, Cause.fail(new StreamNotFound({ stream })))
                      return
                    }
                    if (!caughtUp && probe.value.num_pending === 0) {
                      caughtUp = true
                      yield* Queue.offer(queue, caughtUpEvent)
                    }
                    continue
                  }
                  const entry = entryOf(message)
                  if (Result.isFailure(entry)) {
                    yield* Queue.failCause(queue, Cause.fail(entry.failure))
                    return
                  }
                  yield* Queue.offer(queue, entry.success)
                  if (!caughtUp && message.info.pending === 0) {
                    caughtUp = true
                    yield* Queue.offer(queue, caughtUpEvent)
                  }
                }
              })
              yield* Effect.forkScoped(pullLoop, { startImmediately: true })
              return Stream.fromQueue(queue)
            }
            const queue = yield* Effect.acquireRelease(
              Queue.bounded<
                ConsumeEvent,
                StreamNotFound | ConsumerLagged | SequenceNotProvablyExact
              >(capacity + 1),
              (q) => Queue.shutdown(q)
            )
            let caughtUp = info.num_pending === 0
            let lastDelivered = 0n
            if (caughtUp) Queue.offerUnsafe(queue, { _tag: "CaughtUp" })
            let stop = () => {}
            const callback = (message: JsMsg) => {
              const entry = entryOf(message)
              if (Result.isFailure(entry)) {
                Queue.failCauseUnsafe(queue, Cause.fail(entry.failure))
                stop()
                return
              }
              const admitted = Queue.sizeUnsafe(queue) < capacity
                && Queue.offerUnsafe(queue, entry.success)
              if (!admitted) {
                Queue.failCauseUnsafe(
                  queue,
                  Cause.fail(new ConsumerLagged({ stream, lastDeliveredSequence: lastDelivered }))
                )
                stop()
                return
              }
              lastDelivered = entry.success.message.sequence
              if (!caughtUp && message.info.pending === 0) {
                caughtUp = true
                Queue.offerUnsafe(queue, { _tag: "CaughtUp" })
              }
            }
            const messages = yield* Effect.acquireRelease(
              Effect.promise(() => consumer.consume({ callback, abort_on_missing_resource: true })),
              (m) => Effect.sync(() => m.stop())
            )
            stop = () => messages.stop()
            // stream deletion (or any terminal consumer loss) fails the queue
            messages.closed().then(
              () => Queue.failCauseUnsafe(queue, Cause.fail(new StreamNotFound({ stream }))),
              () => Queue.failCauseUnsafe(queue, Cause.fail(new StreamNotFound({ stream })))
            )
            return Stream.fromQueue(queue)
          })
        )
    })

    return service
  })

export const layer = (connection: NatsConnection): Layer.Layer<JetStream> =>
  Layer.effect(JetStream, make(connection))
