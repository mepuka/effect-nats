/**
 * Transport seam — the ONLY thing the engines know about NATS wire I/O.
 *
 * Everything above this line (prompt engine, discovery, liveness, host pump)
 * is pure orchestration over Clock + Wire, which is what makes it
 * TestClock-testable with zero servers. Production interpreter: nats-wire.ts.
 * Scripted and loopback interpreters live in the test suite.
 */
import { Context } from "effect"
import type { Effect, Scope, Stream } from "effect"
import type { WireFailure } from "./errors.js"

/** One message arriving on a subscription or bound reply inbox. */
export interface ReplyMessage {
  readonly payload: Uint8Array
  /**
   * §6.5: a terminator is a ZERO-BYTE, HEADER-LESS message. A zero-byte
   * message that carries any header is NOT a terminator, so header presence
   * must survive the seam.
   */
  readonly hasHeaders: boolean
  /** §9: Nats-Service-Error-Code header, if present. */
  readonly errorCode?: number | undefined
  /** §9: Nats-Service-Error header, if present. */
  readonly errorMessage?: string | undefined
  /** Reply-to subject (host side: where the response stream goes). */
  readonly replyTo?: string | undefined
}

export interface PublishOptions {
  readonly replyTo?: string | undefined
  /** §9: set both service-error headers on this message. */
  readonly errorCode?: number | undefined
  readonly errorMessage?: string | undefined
}

export interface SubscribeOptions {
  /** Queue-group membership (host prompt endpoints load-balance, §6). */
  readonly queueGroup?: string | undefined
}

/**
 * Multi-response request primitive. Binds an inbox BEFORE publishing
 * (subscribe-before-publish ordering, Protocol §4/§8.5 discipline), publishes,
 * and returns the live reply stream. The stream is scoped: closing the scope
 * unsubscribes (which IS cancellation per Protocol §6.7 — interest drops and
 * the server discards subsequent messages at no cost).
 */
export class Wire extends Context.Service<Wire, WireShape>()("effect-agents/Wire") {}

export interface WireShape {
  /** Our broker's max payload (§5.4: min binds against advertised limits). */
  readonly brokerMaxPayload: number

  /**
   * Publish to `subject` and return the reply stream on a fresh inbox.
   * Multiple replies may arrive (discovery); ack + chunks + terminator for
   * prompts (§6). Transport death surfaces as WireFailure in the stream's
   * error channel — the engine maps it to a typed PromptError.
   */
  readonly request: (
    subject: string,
    payload: Uint8Array,
  ) => Effect.Effect<Stream.Stream<ReplyMessage, WireFailure>, WireFailure, Scope.Scope>

  /** Plain pub/sub subscription (heartbeats, host endpoints). Scoped. */
  readonly subscribe: (
    subject: string,
    options?: SubscribeOptions,
  ) => Effect.Effect<Stream.Stream<ReplyMessage, WireFailure>, WireFailure, Scope.Scope>

  /** Fire-and-forget publish (host response pump, heartbeats). */
  readonly publish: (
    subject: string,
    payload: Uint8Array,
    options?: PublishOptions,
  ) => Effect.Effect<void, WireFailure>
}
