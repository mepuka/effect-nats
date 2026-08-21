/**
 * Response streaming model — Protocol §6 + §9.
 *
 * Wire shapes are effect Schema codecs; decoding is TOTAL via
 * `Schema.decodeUnknownResult` — boundary code has no opportunity to throw.
 *
 * The reply state machine is explicit: every wire message classifies to a
 * `Frame` (`ChunkFrame | ErrorFrame | Terminator`), and `stepWellFormed` is
 * the DFA accepting exactly the sequences a compliant host may produce:
 *
 *   accepted ::= Ack · Chunk* · ErrorFrame? · Terminator     (§6.4, §6.5, §9.3)
 *   rejected ::= ErrorFrame · Terminator                      (§6.4: no ack before acceptance)
 *
 * with "exactly one ack" (a second `status:"ack"` chunk is a violation) and
 * nothing after the terminator. The host pump and the caller-side checker
 * both derive from this one transition function.
 */
import { Result, Schema } from "effect"
import { isStandardBase64 } from "./envelope.js"
import { isConcreteSubject } from "./subjects.js"

// -- §5.2 attachments (shared by requests and response objects) ---------------

const Rest = Schema.Record(Schema.String, Schema.Unknown)

/** Wire shape: known fields plus §5.6 passthrough of unknown fields. */
export const AttachmentWire = Schema.StructWithRest(
  Schema.Struct({
    filename: Schema.String,
    content: Schema.String,
  }),
  [Rest],
)

export interface ChunkAttachment {
  readonly filename: string
  readonly content: string
  /** §5.6: unknown attachment fields, preserved for relay. */
  readonly extra: Readonly<Record<string, unknown>>
}

const toChunkAttachment = (a: typeof AttachmentWire.Type): ChunkAttachment => {
  const { filename, content, ...extra } = a
  return { filename, content, extra }
}

const Attachments = Schema.optionalKey(Schema.Array(AttachmentWire))

// -- §6 typed chunks -----------------------------------------------------------

export const ChunkWire = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("response"),
    data: Schema.Union([
      Schema.String,
      Schema.StructWithRest(
        Schema.Struct({ text: Schema.String, attachments: Attachments }),
        [Rest],
      ),
    ]),
  }),
  Schema.Struct({ type: Schema.Literal("status"), data: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("query"),
    data: Schema.StructWithRest(
      Schema.Struct({
        id: Schema.String,
        reply_subject: Schema.String,
        prompt: Schema.String,
        attachments: Attachments,
      }),
      [Rest],
    ),
  }),
])

export const AnyChunkWire = Schema.Struct({
  type: Schema.String,
  data: Schema.Unknown,
})

/** Decoded chunk domain. Unknown types are carried so callers may skip them. */
export type Chunk =
  | {
      readonly _tag: "Response"
      readonly text: string
      readonly attachments: readonly ChunkAttachment[]
      /** §6.6: unknown fields in the `data` object, preserved for relay. */
      readonly extra: Readonly<Record<string, unknown>>
    }
  | { readonly _tag: "Status"; readonly status: string }
  | {
      readonly _tag: "Query"
      readonly id: string
      readonly replySubject: string
      readonly prompt: string
      readonly attachments: readonly ChunkAttachment[]
      /** §6.6: unknown fields in the `data` object, preserved for relay. */
      readonly extra: Readonly<Record<string, unknown>>
    }
  | { readonly _tag: "Unknown"; readonly type: string; readonly data: unknown }

export type DecodeIssue =
  | { readonly _tag: "NotAnObject" }
  | { readonly _tag: "MalformedKnown"; readonly type: string }

/**
 * Total chunk decoder: unknown `type`s pass through as `Unknown` (§6.6 skip
 * rule); known types that fail their schema are a ProtocolViolation upstream.
 */
export const decodeChunk = (u: unknown): Result.Result<Chunk, DecodeIssue> => {
  const anyParsed = Schema.decodeUnknownResult(AnyChunkWire)(u)
  if (Result.isFailure(anyParsed)) return Result.fail({ _tag: "NotAnObject" })
  const { type } = anyParsed.success
  if (type !== "response" && type !== "status" && type !== "query") {
    return Result.succeed({ _tag: "Unknown", type, data: anyParsed.success.data })
  }
  const known = Schema.decodeUnknownResult(ChunkWire)(u)
  if (Result.isFailure(known)) return Result.fail({ _tag: "MalformedKnown", type })
  const c = known.success
  if (
    (c.type === "response" || c.type === "query") &&
    typeof c.data !== "string" &&
    c.data.attachments?.some((attachment) => !isStandardBase64(attachment.content))
  ) {
    return Result.fail({ _tag: "MalformedKnown", type })
  }
  switch (c.type) {
    case "response": {
      if (typeof c.data === "string") {
        return Result.succeed({ _tag: "Response", text: c.data, attachments: [], extra: {} })
      }
      const { text, attachments, ...extra } = c.data
      return Result.succeed({
        _tag: "Response",
        text,
        attachments: (attachments ?? []).map(toChunkAttachment),
        extra,
      })
    }
    case "status":
      return Result.succeed({ _tag: "Status", status: c.data })
    case "query": {
      const { id, reply_subject, prompt, attachments, ...extra } = c.data
      // §7.1: reply_subject must be a publishable NATS subject
      if (!isConcreteSubject(reply_subject)) {
        return Result.fail({ _tag: "MalformedKnown", type })
      }
      return Result.succeed({
        _tag: "Query",
        id,
        replySubject: reply_subject,
        prompt,
        attachments: (attachments ?? []).map(toChunkAttachment),
        extra,
      })
    }
  }
}

/** §6.4: exactly one ack status chunk, always first on the reply subject. */
export const ACK: Chunk = { _tag: "Status", status: "ack" }

export const isAck = (c: Chunk): boolean => c._tag === "Status" && c.status === "ack"

// -- reply frame model (docs/01-SPEC.md §3) -----------------------------------

export type Frame =
  | { readonly _tag: "ChunkFrame"; readonly chunk: Chunk }
  | { readonly _tag: "ErrorFrame"; readonly code: number; readonly message: string }
  | { readonly _tag: "Terminator" }

export const chunkFrame = (chunk: Chunk): Frame => ({ _tag: "ChunkFrame", chunk })
export const errorFrame = (code: number, message: string): Frame => ({
  _tag: "ErrorFrame",
  code,
  message,
})
export const TERMINATOR: Frame = { _tag: "Terminator" }

/**
 * DFA states for one reply stream. `AwaitAck` = nothing observed yet;
 * `Streaming` = accepted (ack seen); `ErrorSent` = error observed, only the
 * terminator may follow; `Done` = terminator observed, nothing may follow.
 */
export type PumpState = "AwaitAck" | "Streaming" | "ErrorSent" | "Done"

export const PUMP_START: PumpState = "AwaitAck"

/** One DFA transition; null = the frame is illegal in this state. */
export const stepWellFormed = (state: PumpState, frame: Frame): PumpState | null => {
  switch (state) {
    case "AwaitAck":
      if (frame._tag === "ChunkFrame") return isAck(frame.chunk) ? "Streaming" : null
      return frame._tag === "ErrorFrame" ? "ErrorSent" : null // bare terminator: no
    case "Streaming":
      if (frame._tag === "ChunkFrame") return isAck(frame.chunk) ? null : "Streaming"
      return frame._tag === "ErrorFrame" ? "ErrorSent" : "Done"
    case "ErrorSent":
      return frame._tag === "Terminator" ? "Done" : null
    case "Done":
      return null
  }
}

/** A complete reply stream is well-formed iff the DFA lands on `Done`. */
export const wellFormed = (frames: Iterable<Frame>): boolean => {
  let state: PumpState = PUMP_START
  for (const frame of frames) {
    const next = stepWellFormed(state, frame)
    if (next === null) return false
    state = next
  }
  return state === "Done"
}

// -- §8.3 heartbeat --------------------------------------------------------------

export const Heartbeat = Schema.Struct({
  agent: Schema.String,
  owner: Schema.String,
  session: Schema.optionalKey(Schema.String),
  instance_id: Schema.String,
  ts: Schema.String,
  // §8.3: cadence in seconds, > 0
  interval_s: Schema.Number.pipe(Schema.check(Schema.isGreaterThan(0))),
})
export type Heartbeat = typeof Heartbeat.Type

export const decodeHeartbeat: (u: unknown) => Result.Result<Heartbeat, Schema.SchemaError> =
  Schema.decodeUnknownResult(Heartbeat)

// -- §B.12 / $SRV.INFO discovery record --------------------------------------------

export const ServiceEndpoint = Schema.Struct({
  name: Schema.String,
  subject: Schema.String,
  queue_group: Schema.optionalKey(Schema.String),
  metadata: Schema.optionalKey(Rest),
})

export const ServiceInfo = Schema.Struct({
  name: Schema.String,
  id: Schema.String,
  version: Schema.optionalKey(Schema.String),
  description: Schema.optionalKey(Schema.String),
  metadata: Schema.optionalKey(Rest),
  endpoints: Schema.Array(ServiceEndpoint),
})
export type ServiceInfo = typeof ServiceInfo.Type

export const decodeServiceInfo: (u: unknown) => Result.Result<ServiceInfo, Schema.SchemaError> =
  Schema.decodeUnknownResult(ServiceInfo)

// -- §9.2 status-code taxonomy -----------------------------------------------------

export const ERROR_CODES = {
  badRequest: 400,
  unauthorized: 401,
  forbidden: 403,
  notFound: 404,
  conflict: 409,
  rateLimited: 429,
  internal: 500,
} as const
