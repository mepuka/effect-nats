/**
 * AgentHost — the server side (docs/01-SPEC.md §§3, 5, 6; Protocol §§6, 8, 9).
 *
 * The reply pump is a single sequential writer per prompt, so the wire
 * sequence is correct by construction:
 *
 *   accepted:  ack · chunk* · error? · terminator
 *   rejected:  error(400|429) · terminator          (no ack, §6.4)
 *
 * The terminator publish sits in a finalizer guarded to run exactly once,
 * including on interruption. Heartbeats, the status endpoint, and the $SRV
 * responders are supervised loops: failure or completion reacquires the
 * subscription after a pause; they cannot die silently.
 */
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
  Schedule,
  Semaphore,
  Stream,
} from "effect"
import { HostConfigError, WireFailure } from "./errors.js"
import type { AgentFailure } from "./errors.js"
import { isRequestError, parseRequest } from "./protocol/envelope.js"
import type { Attachment, RequestEnvelope, RequestError } from "./protocol/envelope.js"
import { ERROR_CODES } from "./protocol/chunks.js"
import { parseMaxPayload } from "./protocol/validation.js"
import { hostDefaultSubject } from "./protocol/subjects.js"
import { Wire, type ReplyMessage } from "./wire.js"

export interface HostIdentity {
  readonly agent: string
  readonly owner: string
  readonly name: string
}

export type HandlerChunk =
  | {
      readonly _tag: "Text"
      readonly text: string
      readonly attachments?: readonly Attachment[] | undefined
    }
  | { readonly _tag: "Status"; readonly status: string }

export interface PromptRequest {
  readonly prompt: string
  readonly attachments: readonly Attachment[]
  readonly extra: Readonly<Record<string, unknown>>
}

export type PromptHandler = (
  request: PromptRequest,
) => Stream.Stream<HandlerChunk, AgentFailure>

export interface AgentHostConfig {
  readonly identity: HostIdentity
  readonly handler: PromptHandler
  readonly version?: string | undefined
  readonly description?: string | undefined
  /** §3.2: MUST be set for session-aware harnesses; echoed in heartbeats (§8.3). */
  readonly session?: string | undefined
  readonly heartbeatIntervalS?: number | undefined
  readonly maxPayload?: string | undefined
  readonly attachmentsOk?: boolean | undefined
  readonly maxInFlight?: number | undefined
  readonly instanceId?: string | undefined
  /** Backoff between supervised-loop reacquisitions. Default 1s. */
  readonly restartMs?: number | undefined
}

export const DEFAULT_HEARTBEAT_INTERVAL_S = 30
/** §11.1: MAJOR.MINOR only. */
export const PROTOCOL_VERSION = "0.3"
export const QUEUE_GROUP = "agents"

const ERROR_NAMES: Record<number, string> = {
  400: "bad_request",
  401: "unauthorized",
  403: "forbidden",
  404: "not_found",
  409: "conflict",
  429: "rate_limited",
  500: "internal",
}

export interface AgentHostShape {
  readonly instanceId: string
  readonly identity: HostIdentity
  readonly serviceInfo: Readonly<Record<string, unknown>>
}

export class AgentHost extends Context.Service<AgentHost, AgentHostShape>()(
  "effect-agents/AgentHost",
) {}

const encoder = new TextEncoder()

const encodeJson = (value: unknown): Uint8Array => encoder.encode(JSON.stringify(value))

const EMPTY = new Uint8Array(0)

const describeRequestError = (e: RequestError): string => {
  switch (e._tag) {
    case "EmptyPayload":
      return "empty request payload"
    case "EmptyPrompt":
      return "prompt must be non-empty"
    case "MalformedJson":
      return "request is not valid JSON"
    case "MalformedUtf8":
      return "request is not valid UTF-8"
    case "MissingPrompt":
      return "request object has no string prompt"
    case "InvalidAttachment":
      return `invalid attachment '${e.filename}'`
    case "NotAnObject":
      return "request JSON is not an object"
  }
}

const toPromptRequest = (envelope: RequestEnvelope): PromptRequest => ({
  prompt: envelope.prompt,
  attachments: envelope.attachments,
  extra: envelope.extra,
})

const chunkToWire = (c: HandlerChunk): Uint8Array =>
  c._tag === "Status"
    ? encodeJson({ type: "status", data: c.status })
    : c.attachments !== undefined && c.attachments.length > 0
      ? encodeJson({
          type: "response",
          data: {
            text: c.text,
            attachments: c.attachments.map((a) => ({
              ...a.extra,
              filename: a.filename,
              content: a.content,
            })),
          },
        })
      : encodeJson({ type: "response", data: c.text })

export const layerHost = (
  config: AgentHostConfig,
): Layer.Layer<AgentHost, HostConfigError, Wire> =>
  Layer.effect(AgentHost)(
    Effect.gen(function* () {
      const wire = yield* Wire
      const { agent, owner, name } = config.identity

      const promptSubject = hostDefaultSubject("prompt", agent, owner, name)
      const statusSubject = hostDefaultSubject("status", agent, owner, name)
      const hbSubject = hostDefaultSubject("hb", agent, owner, name)
      if (promptSubject === null || statusSubject === null || hbSubject === null) {
        return yield* new HostConfigError({
          reason: `invalid identity tokens: ${agent}.${owner}.${name}`,
        })
      }
      const intervalS = config.heartbeatIntervalS ?? DEFAULT_HEARTBEAT_INTERVAL_S
      if (!Number.isInteger(intervalS) || intervalS <= 0) {
        return yield* new HostConfigError({ reason: `invalid heartbeat interval ${intervalS}` })
      }
      const maxInFlight = config.maxInFlight ?? 1
      if (!Number.isInteger(maxInFlight) || maxInFlight <= 0) {
        return yield* new HostConfigError({ reason: `invalid maxInFlight ${maxInFlight}` })
      }
      const maxPayload = config.maxPayload ?? "1MB"
      const advertisedLimit = parseMaxPayload(maxPayload)
      if (advertisedLimit === null) {
        return yield* new HostConfigError({ reason: `invalid max_payload '${maxPayload}'` })
      }

      const instanceId = config.instanceId ?? crypto.randomUUID()
      const restartMs = config.restartMs ?? 1_000
      const restart = Schedule.spaced(restartMs)

      const serviceInfo = {
        type: "io.nats.micro.v1.info_response",
        name: "agents",
        id: instanceId,
        version: config.version ?? "0.0.0",
        // §3.1: description is a required service field
        description: config.description ?? `${agent} agent (${owner}/${name})`,
        metadata: {
          agent,
          owner,
          ...(config.session !== undefined ? { session: config.session } : {}),
          protocol_version: PROTOCOL_VERSION,
        },
        endpoints: [
          {
            name: "prompt",
            subject: promptSubject,
            queue_group: QUEUE_GROUP,
            metadata: {
              max_payload: maxPayload,
              attachments_ok: config.attachmentsOk ?? true,
            },
          },
          {
            name: "status",
            subject: statusSubject,
            queue_group: QUEUE_GROUP,
            metadata: {},
          },
        ],
      }
      const infoBytes = encodeJson(serviceInfo)
      const pingBytes = encodeJson({
        type: "io.nats.micro.v1.ping_response",
        name: "agents",
        id: instanceId,
        version: config.version ?? "0.0.0",
        metadata: serviceInfo.metadata,
      })

      // §8.7.1: periodic beats and the status endpoint share this builder
      const heartbeatBody = Effect.map(Clock.currentTimeMillis, (now) =>
        encodeJson({
          agent,
          owner,
          ...(config.session !== undefined ? { session: config.session } : {}),
          instance_id: instanceId,
          ts: new Date(now).toISOString(),
          interval_s: intervalS,
        }),
      )

      const admission = Semaphore.makeUnsafe(maxInFlight)

      const sendError = (replyTo: string, code: number, message: string) =>
        wire.publish(
          replyTo,
          encodeJson({ error: ERROR_NAMES[code] ?? "error", message }),
          { errorCode: code, errorMessage: message },
        )

      const sendTerminator = (replyTo: string) =>
        wire.publish(replyTo, EMPTY).pipe(Effect.ignore)

      const sendAck = (replyTo: string) =>
        wire.publish(replyTo, encodeJson({ type: "status", data: "ack" }))

      const runAccepted = (replyTo: string, envelope: RequestEnvelope) =>
        Effect.gen(function* () {
          yield* sendAck(replyTo)
          yield* config.handler(toPromptRequest(envelope)).pipe(
            Stream.filter((c) => !(c._tag === "Status" && c.status === "ack")),
            Stream.runForEach((c) => wire.publish(replyTo, chunkToWire(c))),
          )
        })

      const respond = (m: ReplyMessage): Effect.Effect<void> => {
        const replyTo = m.replyTo
        if (replyTo === undefined) return Effect.void
        if (m.payload.byteLength > advertisedLimit) {
          return sendError(replyTo, ERROR_CODES.badRequest, "request exceeds max_payload").pipe(
            Effect.catchTag("WireFailure", () => Effect.void),
            Effect.ensuring(sendTerminator(replyTo)),
          )
        }
        const parsed = parseRequest(m.payload)
        const rejection = isRequestError(parsed)
          ? describeRequestError(parsed)
          : parsed.attachments.length > 0 && !(config.attachmentsOk ?? true)
            ? "attachments not supported by this endpoint"
            : null
        const body = rejection !== null || isRequestError(parsed)
          ? sendError(replyTo, ERROR_CODES.badRequest, rejection ?? "bad request")
          : Semaphore.withPermitsIfAvailable(admission, 1)(runAccepted(replyTo, parsed)).pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () => sendError(replyTo, ERROR_CODES.rateLimited, "agent busy"),
                  onSome: () => Effect.void,
                }),
              ),
              // §9.2: agents MUST use taxonomy codes — unknown codes degrade to 500
              Effect.catchTag("AgentFailure", (e) =>
                sendError(replyTo, ERROR_NAMES[e.code] !== undefined ? e.code : 500, e.message),
              ),
            )
        return body.pipe(
          Effect.catchTag("WireFailure", () => Effect.void),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.failCause(cause)
              : sendError(replyTo, ERROR_CODES.internal, "internal agent error").pipe(
                  Effect.catchTag("WireFailure", () => Effect.void),
                )
          ),
          Effect.ensuring(sendTerminator(replyTo)),
        )
      }

      const supervised = <A, E>(loop: Effect.Effect<A, E, never>) =>
        loop.pipe(Effect.retry(restart), Effect.repeat(restart), Effect.forkScoped)

      // the latch releases only on a SUCCESSFUL bind — a failed subscribe is
      // retried by the supervisor, and construction keeps waiting: a host is
      // not registered until interest actually exists
      const subscribeBound = (
        bound: Deferred.Deferred<void>,
        subject: string,
        queueGroup?: string,
      ) =>
        wire.subscribe(subject, { queueGroup }).pipe(
          Effect.tap(() => Deferred.succeed(bound, void 0)),
        )

      const overloaded = (m: ReplyMessage) => {
        const replyTo = m.replyTo
        if (replyTo === undefined) return Effect.void
        return sendError(replyTo, ERROR_CODES.rateLimited, "agent overloaded").pipe(
          Effect.catchTag("WireFailure", () => Effect.void),
          Effect.ensuring(sendTerminator(replyTo)),
        )
      }

      // pre-admission fan-out is bounded: beyond maxPending waiting/validating
      // responders, requests are rejected inline with bounded work
      const maxPending = maxInFlight * 8 + 32
      const pending = Semaphore.makeUnsafe(maxPending)

      const promptLoop = (bound: Deferred.Deferred<void>) =>
        Effect.scoped(
          Effect.gen(function* () {
            const msgs = yield* subscribeBound(bound, promptSubject, QUEUE_GROUP)
            yield* msgs.pipe(
              Stream.runForEach((m) =>
                Effect.flatMap(Semaphore.takeIfAvailable(pending, 1), (took) =>
                  took
                    ? Effect.forkScoped(
                        respond(m).pipe(Effect.ensuring(Semaphore.release(pending, 1))),
                      )
                    : overloaded(m),
                ),
              ),
            )
          }),
        )

      const replyLoop = (
        bound: Deferred.Deferred<void>,
        subject: string,
        reply: Effect.Effect<Uint8Array>,
        queueGroup?: string,
      ) =>
        Effect.scoped(
          Effect.gen(function* () {
            const msgs = yield* subscribeBound(bound, subject, queueGroup)
            yield* msgs.pipe(
              Stream.runForEach((m) => {
                const replyTo = m.replyTo
                if (replyTo === undefined) return Effect.void
                return Effect.flatMap(reply, (bytes) => wire.publish(replyTo, bytes)).pipe(
                  // §8.7.1: a payload-construction defect answers 500
                  Effect.catchDefect(() =>
                    sendError(replyTo, ERROR_CODES.internal, "reply construction failed"),
                  ),
                )
              }),
            )
          }),
        )

      const heartbeatLoop = Effect.flatMap(heartbeatBody, (bytes) =>
        wire.publish(hbSubject, bytes),
      ).pipe(Effect.repeat(Schedule.spaced(intervalS * 1000)))

      const responders: Array<
        (bound: Deferred.Deferred<void>) => Effect.Effect<unknown, WireFailure, never>
      > = [
        promptLoop,
        (b) => replyLoop(b, statusSubject, heartbeatBody, QUEUE_GROUP),
        (b) => replyLoop(b, "$SRV.PING", Effect.succeed(pingBytes)),
        (b) => replyLoop(b, "$SRV.PING.agents", Effect.succeed(pingBytes)),
        (b) => replyLoop(b, `$SRV.PING.agents.${instanceId}`, Effect.succeed(pingBytes)),
        (b) => replyLoop(b, "$SRV.INFO", Effect.succeed(infoBytes)),
        (b) => replyLoop(b, "$SRV.INFO.agents", Effect.succeed(infoBytes)),
        (b) => replyLoop(b, `$SRV.INFO.agents.${instanceId}`, Effect.succeed(infoBytes)),
      ]
      const bounds: Array<Effect.Effect<void>> = []
      for (const responder of responders) {
        const bound = yield* Deferred.make<void>()
        yield* supervised(responder(bound))
        bounds.push(Deferred.await(bound))
      }
      // registration complete before the service is visible and before the
      // first heartbeat (§8.2)
      yield* Effect.all(bounds)
      yield* supervised(heartbeatLoop)

      return AgentHost.of({ instanceId, identity: config.identity, serviceInfo })
    }),
  )
