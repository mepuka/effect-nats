/**
 * Read-path engine — turns a Wire request into a typed chunk Stream.
 * Pure Stream combinators over Clock + Wire; TestClock-testable.
 */
import { Duration, Effect, Result, Stream } from "effect"
import { ProtocolViolation, PromptFailed, StreamTimeout, TransportFailed } from "./errors.js"
import type { PromptError } from "./errors.js"
import {
  decodeChunk,
  PUMP_START,
  stepWellFormed,
  TERMINATOR,
  type Chunk,
  type Frame,
  type PumpState,
} from "./protocol/chunks.js"
import { Wire, type ReplyMessage } from "./wire.js"

const decoder = new TextDecoder("utf-8", { fatal: false })

const errorBodyMessage = (payload: Uint8Array): string | undefined => {
  if (payload.length === 0) return undefined
  try {
    const body: unknown = JSON.parse(decoder.decode(payload))
    if (typeof body === "object" && body !== null && !Array.isArray(body)) {
      const message = (body as Record<string, unknown>)["message"]
      if (typeof message === "string") return message
    }
  } catch {
    // §9.1: fall back to the Nats-Service-Error header
  }
  return undefined
}

export const classify = (m: ReplyMessage): Result.Result<Frame, PromptError> => {
  if (m.errorCode !== undefined) {
    if (!Number.isInteger(m.errorCode)) {
      return Result.fail(new ProtocolViolation({ reason: "invalid service error code" }))
    }
    return Result.succeed({
      _tag: "ErrorFrame",
      code: m.errorCode,
      message: errorBodyMessage(m.payload) ?? m.errorMessage ?? "",
    })
  }
  if (m.payload.length === 0) {
    // §6.5: only a zero-byte HEADER-LESS message terminates
    return m.hasHeaders
      ? Result.fail(new ProtocolViolation({ reason: "zero-byte message with headers" }))
      : Result.succeed(TERMINATOR)
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(decoder.decode(m.payload))
  } catch {
    return Result.fail(new ProtocolViolation({ reason: "chunk is not JSON" }))
  }
  const decoded = decodeChunk(parsed)
  if (Result.isFailure(decoded)) {
    return Result.fail(
      new ProtocolViolation({
        reason:
          decoded.failure._tag === "NotAnObject"
            ? "chunk is not an object"
            : `known chunk type '${decoded.failure.type}' violated its schema`,
      }),
    )
  }
  return Result.succeed({ _tag: "ChunkFrame", chunk: decoded.success })
}

type EngineFrame = Frame | { readonly _tag: "EndOfWire" }

const END_OF_WIRE: EngineFrame = { _tag: "EndOfWire" }

export interface PromptParams {
  readonly subject: string
  readonly payload: Uint8Array
  /** Gap timeout since the last observed message. Default: 60s (§6.6). */
  readonly inactivityMs?: number | undefined
}

export const DEFAULT_INACTIVITY_MS = 60_000

/**
 * §6 reply stream: every message resets the gap timer (unknown chunks
 * included), the terminator ends the stream, an upstream end without a
 * terminator is a protocol violation, and closing the stream's scope
 * unsubscribes exactly once.
 *
 * The frame sequence is checked against the reply DFA (`stepWellFormed`):
 * a missing/duplicate ack, a bare terminator, or content before the ack all
 * fail as ProtocolViolation. An ErrorFrame fails the stream immediately —
 * its trailing terminator cannot be awaited from a failed stream.
 */
export const promptStream = (
  params: PromptParams,
): Stream.Stream<Chunk, PromptError, Wire> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const wire = yield* Wire
      const replies = yield* wire
        .request(params.subject, params.payload)
        .pipe(Effect.mapError((e) => new TransportFailed({ reason: e.reason })))
      const inactivityMs = params.inactivityMs ?? DEFAULT_INACTIVITY_MS

      let state: PumpState = PUMP_START
      const frames: Stream.Stream<EngineFrame, PromptError> = replies.pipe(
        Stream.mapError((e) => new TransportFailed({ reason: e.reason })),
        Stream.mapEffect((m) =>
          Effect.flatMap(Effect.fromResult(classify(m)), (frame) => {
            const next = stepWellFormed(state, frame)
            if (next === null) {
              return Effect.fail(
                new ProtocolViolation({
                  reason: `illegal ${frame._tag} while ${state} (reply DFA)`,
                }),
              )
            }
            state = next
            return Effect.succeed<EngineFrame>(frame)
          }),
        ),
      )

      return Stream.concat(frames, Stream.make(END_OF_WIRE)).pipe(
        Stream.timeoutOrElse({
          duration: Duration.millis(inactivityMs),
          orElse: () => Stream.fail(new StreamTimeout({ inactivityMs })),
        }),
        Stream.takeWhile((f) => f._tag !== "Terminator"),
        Stream.mapEffect(
          (f): Effect.Effect<Chunk, PromptError> =>
            f._tag === "ChunkFrame"
              ? Effect.succeed(f.chunk)
              : f._tag === "ErrorFrame"
                ? Effect.fail(new PromptFailed({ code: f.code, message: f.message }))
                : Effect.fail(
                    new ProtocolViolation({ reason: "reply stream ended without terminator" }),
                  ),
        ),
      )
    }),
  )
