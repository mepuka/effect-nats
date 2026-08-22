import type { Effect, Stream } from "effect"
import { Context, Schema } from "effect"

// The JetStream substrate seam: the minimal stream-store contract KV consumes
// (kv-slice-scoping.md). Implemented by the in-memory interpreter for tests and
// by the wrap adapter (ADR-0002) for the live client. Sequences are bigint per
// ADR-0004. Error codes are semantic tags here; numeric server codes map onto
// them in the live adapter, pinned at adapter implementation time.

export interface StreamConfig {
  readonly name: string
  readonly subjects: ReadonlyArray<string>
  /** 0 means unlimited history. */
  readonly maxMessagesPerSubject: number
  readonly allowRollup: boolean
}

export interface StoredMessage {
  readonly subject: string
  readonly sequence: bigint
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, string>
  readonly timestampMillis: number
}

export type ConsumeEvent =
  | { readonly _tag: "Entry"; readonly message: StoredMessage }
  | { readonly _tag: "CaughtUp" }

export type StartPosition =
  | { readonly _tag: "AllHistory" }
  | { readonly _tag: "LastPerSubject" }
  | { readonly _tag: "NewOnly" }
  | { readonly _tag: "FromSequence"; readonly sequence: bigint }

export interface PublishOptions {
  /** Per-subject compare-and-set: 0n asserts the subject has no message yet. */
  readonly expectedLastSubjectSequence?: bigint
  readonly headers?: ReadonlyMap<string, string>
}

/**
 * Per-subscription buffer policy (ADR-0005). `PullWindow` is legal only where
 * production is genuinely controllable upstream (a pull window or an in-process
 * producer): a full buffer suspends the producer. `TerminateOnLag` is for
 * producers that cannot be suspended: a full buffer terminates the subscription
 * with a typed `ConsumerLagged` carrying the last delivered sequence, so the
 * consumer can resume from a known position. Silent loss is not offered.
 */
export type ConsumeBuffer =
  | { readonly _tag: "PullWindow"; readonly messages: number }
  | { readonly _tag: "TerminateOnLag"; readonly messages: number }

export interface ConsumeOptions {
  readonly filters: ReadonlyArray<string>
  readonly start: StartPosition
  readonly buffer: ConsumeBuffer
}

/** `Nats-Rollup: sub` on a publish replaces all prior messages for its subject. */
export const rollupHeader = "Nats-Rollup"
export const rollupSubject = "sub"

export class StreamNotFound extends Schema.TaggedError<StreamNotFound>()("StreamNotFound", {
  stream: Schema.String
}) {}

export class StreamConfigConflict
  extends Schema.TaggedError<StreamConfigConflict>()("StreamConfigConflict", {
    stream: Schema.String
  })
{}

export class SubjectNotBound extends Schema.TaggedError<SubjectNotBound>()("SubjectNotBound", {
  stream: Schema.String,
  subject: Schema.String
}) {}

export class WrongLastSequence
  extends Schema.TaggedError<WrongLastSequence>()("WrongLastSequence", {
    stream: Schema.String,
    subject: Schema.String,
    expected: Schema.BigInt,
    actual: Schema.BigInt
  })
{}

export class NoMessageForSubject
  extends Schema.TaggedError<NoMessageForSubject>()("NoMessageForSubject", {
    stream: Schema.String,
    subject: Schema.String
  })
{}

export class RollupNotPermitted
  extends Schema.TaggedError<RollupNotPermitted>()("RollupNotPermitted", {
    stream: Schema.String
  })
{}

/** A TerminateOnLag subscription overflowed; resume from `lastDeliveredSequence`. */
export class ConsumerLagged extends Schema.TaggedError<ConsumerLagged>()("ConsumerLagged", {
  stream: Schema.String,
  lastDeliveredSequence: Schema.BigInt
}) {}

export class JetStream extends Context.Service<JetStream, {
  /** Create-only; an existing stream with identical config succeeds (idempotent). */
  createStream(config: StreamConfig): Effect.Effect<void, StreamConfigConflict>
  getStream(name: string): Effect.Effect<StreamConfig, StreamNotFound>
  /** Fails every active consumer of the stream with `StreamNotFound`. */
  deleteStream(name: string): Effect.Effect<void, StreamNotFound>
  publish(
    stream: string,
    subject: string,
    payload: Uint8Array,
    options?: PublishOptions
  ): Effect.Effect<
    bigint,
    StreamNotFound | SubjectNotBound | WrongLastSequence | RollupNotPermitted
  >
  lastMessageForSubject(
    stream: string,
    subject: string
  ): Effect.Effect<StoredMessage, StreamNotFound | NoMessageForSubject>
  /**
   * Scoped ordered consumption: replays per `start`, emits one `CaughtUp`, then
   * continues live. Per-subscription ordering is preserved; overflow behavior
   * follows the declared `buffer` policy — never silent loss.
   */
  consume(
    stream: string,
    options: ConsumeOptions
  ): Stream.Stream<ConsumeEvent, StreamNotFound | ConsumerLagged>
}>()("effect-nats/JetStream") {}
