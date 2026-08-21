/**
 * AgentClient — discovery, liveness, and prompting (docs/01-SPEC.md §§2, 5).
 *
 * Discovery collects `$SRV.INFO.agents` replies until a quiet gap or an
 * overall deadline, both Clock-driven. Liveness tracks heartbeats through one
 * layer-owned subscription fanned out via a bounded PubSub; the tracker is a
 * supervised loop that reacquires its subscription on failure or completion.
 * Prompting validates locally (§5.4) through the same envelope contract the
 * host parses, then runs the read-path engine.
 */
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
  PubSub,
  Result,
  Schedule,
  Stream,
} from "effect"
import { DiscoveryFailed, LocalViolation, TransportFailed } from "./errors.js"
import type { PromptCallError } from "./errors.js"
import { encodeRequest, isRequestError, parseRequest } from "./protocol/envelope.js"
import type { Attachment } from "./protocol/envelope.js"
import { decodeHeartbeat, decodeServiceInfo } from "./protocol/chunks.js"
import type { Chunk, Heartbeat, ServiceInfo } from "./protocol/chunks.js"
import { HEARTBEAT_WILDCARD, isConcreteSubject } from "./protocol/subjects.js"
import { parseMaxPayload, validateLocal } from "./protocol/validation.js"
import { promptStream } from "./prompt-engine.js"
import { Wire } from "./wire.js"

export interface AgentEndpointInfo {
  readonly name: string
  readonly subject: string
  readonly queueGroup?: string | undefined
  readonly maxPayload?: string | undefined
  readonly attachmentsOk?: boolean | undefined
}

export interface AgentIdentity {
  readonly agent: string
  readonly owner: string
  readonly session?: string | undefined
  readonly protocolVersion?: string | undefined
  readonly instanceId: string
  readonly description?: string | undefined
  /** §5.6: full metadata record preserved for relay. */
  readonly metadata: Readonly<Record<string, unknown>>
  readonly endpoints: readonly AgentEndpointInfo[]
}

export const CALLER_PROTOCOL_VERSION = "0.3"

/**
 * §11.1/§11.2: compare MAJOR.MINOR prefixes only. The 0.x line is unstable,
 * so 0.x requires an exact MINOR match; from 1.0 on, same MAJOR is
 * compatible.
 */
export const isCompatibleProtocolVersion = (declared: string): boolean => {
  const m = /^(\d+)\.(\d+)/.exec(declared)
  if (m === null) return false
  const ours = /^(\d+)\.(\d+)$/.exec(CALLER_PROTOCOL_VERSION)!
  const [major, minor] = [m[1], m[2]]
  return major === "0" ? major === ours[1] && minor === ours[2] : major === ours[1]
}

/**
 * §3.2: a compliant record carries string `metadata.agent`, `metadata.owner`,
 * and `metadata.protocol_version` passing §11.2; non-compliant records are
 * excluded rather than laundered into identities with manufactured fields.
 */
const toIdentity = (info: ServiceInfo): Option.Option<AgentIdentity> => {
  if (info.name !== "agents") return Option.none()
  const md = info.metadata ?? {}
  const agent = md["agent"]
  const owner = md["owner"]
  const version = md["protocol_version"]
  if (typeof agent !== "string" || agent.length === 0) return Option.none()
  if (typeof owner !== "string" || owner.length === 0) return Option.none()
  if (typeof version !== "string" || !isCompatibleProtocolVersion(version)) return Option.none()
  return Option.some({
    agent,
    owner,
    session: typeof md["session"] === "string" ? md["session"] : undefined,
    protocolVersion:
      typeof md["protocol_version"] === "string" ? md["protocol_version"] : undefined,
    instanceId: info.id,
    description: info.description,
    metadata: md,
    endpoints: info.endpoints.map((e) => ({
      name: e.name,
      subject: e.subject,
      queueGroup: e.queue_group,
      maxPayload:
        e.metadata !== undefined && typeof e.metadata["max_payload"] === "string"
          ? e.metadata["max_payload"]
          : undefined,
      attachmentsOk:
        e.metadata !== undefined && typeof e.metadata["attachments_ok"] === "boolean"
          ? e.metadata["attachments_ok"]
          : undefined,
    })),
  })
}

export interface PromptOptions {
  readonly attachments?: readonly { readonly filename: string; readonly content: string }[]
  readonly extra?: Readonly<Record<string, unknown>> | undefined
  readonly inactivityMs?: number | undefined
}

export interface DiscoverOptions {
  readonly quietMs?: number | undefined
  readonly deadlineMs?: number | undefined
}

export interface LivenessEntry {
  readonly instanceId: string
  readonly lastSeenMs: number
  readonly intervalS: number
}

export interface ClientOptions {
  /** Backoff between heartbeat-tracker reacquisitions. Default 1s. */
  readonly heartbeatRestartMs?: number | undefined
  /** Capacity of the heartbeat fan-out PubSub. Default 256, sliding. */
  readonly heartbeatBuffer?: number | undefined
  /**
   * Instances silent for this many multiples of their declared interval are
   * evicted from the liveness map, keeping it bounded by live-ish instances.
   * Must exceed the §8.2 offline threshold factor of 3. Default 10.
   */
  readonly livenessPruneFactor?: number | undefined
  /** Hard cap on tracked instances; stalest evicted first. Default 4096. */
  readonly livenessCapacity?: number | undefined
}

/** Pruning clamps declared intervals so absurd values cannot pin entries. */
export const MAX_PRUNE_INTERVAL_S = 3_600

export const DEFAULT_QUIET_MS = 300
export const DEFAULT_DEADLINE_MS = 3_000

export class AgentClient extends Context.Service<AgentClient, AgentClientShape>()(
  "effect-agents/AgentClient",
) {}

export interface AgentClientShape {
  readonly discover: (
    options?: DiscoverOptions,
  ) => Effect.Effect<readonly AgentIdentity[], DiscoveryFailed>

  readonly liveness: () => Effect.Effect<ReadonlyMap<string, LivenessEntry>>

  /** True when the instance beat within 3× its declared interval (§8.2). */
  readonly isLive: (instanceId: string) => Effect.Effect<boolean>

  /** Independent per-subscriber heartbeat streams (Protocol §8.3). */
  readonly heartbeats: () => Stream.Stream<Heartbeat>

  readonly prompt: (
    identity: AgentIdentity,
    text: string,
    options?: PromptOptions,
  ) => Stream.Stream<Chunk, PromptCallError>

  /** §7.2: publish exactly one §5.1-shaped reply to a mid-stream query. */
  readonly answerQuery: (
    query: { readonly replySubject: string },
    text: string,
  ) => Effect.Effect<void, PromptCallError>
}

export const layerClient = (options?: ClientOptions): Layer.Layer<AgentClient, never, Wire> =>
  Layer.effect(AgentClient)(
    Effect.gen(function* () {
      const wire = yield* Wire
      const beats = new Map<string, LivenessEntry>()
      const pruneFactor = Math.max(options?.livenessPruneFactor ?? 10, 4)
      const capacity = Math.max(options?.livenessCapacity ?? 4_096, 1)
      const hbPubSub = yield* PubSub.sliding<Heartbeat>(options?.heartbeatBuffer ?? 256)
      // §8.5: discovery is gated on the heartbeat wildcard being bound
      const hbBound = yield* Deferred.make<void>()

      const prune = (now: number) => {
        for (const [id, entry] of beats) {
          const horizon = pruneFactor * Math.min(entry.intervalS, MAX_PRUNE_INTERVAL_S) * 1000
          if (now - entry.lastSeenMs > horizon) beats.delete(id)
        }
        while (beats.size > capacity) {
          let oldest: string | undefined
          let oldestSeen = Infinity
          for (const [id, entry] of beats) {
            if (entry.lastSeenMs < oldestSeen) {
              oldestSeen = entry.lastSeenMs
              oldest = id
            }
          }
          if (oldest === undefined) break
          beats.delete(oldest)
        }
      }

      const trackOnce = Effect.scoped(
        Effect.gen(function* () {
          const hb = yield* wire.subscribe(HEARTBEAT_WILDCARD).pipe(
            Effect.tap(() => Deferred.succeed(hbBound, void 0)),
          )
          yield* hb.pipe(
            Stream.filterMap((m) => safeJson(m.payload)),
            Stream.filterMap(decodeHeartbeat),
            Stream.filter((h) => Number.isFinite(h.interval_s) && h.interval_s > 0),
            Stream.runForEach((h) =>
              Effect.gen(function* () {
                const now = yield* Clock.currentTimeMillis
                beats.set(h.instance_id, {
                  instanceId: h.instance_id,
                  lastSeenMs: now,
                  intervalS: h.interval_s,
                })
                prune(now)
                yield* PubSub.publish(hbPubSub, h)
              }),
            ),
          )
        }),
      )
      const restart = Schedule.spaced(options?.heartbeatRestartMs ?? 1_000)
      yield* trackOnce.pipe(
        Effect.retry(restart),
        Effect.repeat(restart),
        Effect.forkScoped,
      )

      const discover = Effect.fn("AgentClient.discover")(function* (opts?: DiscoverOptions) {
        const quietMs = opts?.quietMs ?? DEFAULT_QUIET_MS
        const deadlineMs = opts?.deadlineMs ?? DEFAULT_DEADLINE_MS
        yield* Deferred.await(hbBound)
        const found: AgentIdentity[] = []
        yield* Effect.scoped(
          Effect.gen(function* () {
            const replies = yield* wire.request("$SRV.INFO.agents", new Uint8Array(0))
            const collect = replies.pipe(
              Stream.filterMap((m) => safeJson(m.payload)),
              Stream.filterMap(decodeServiceInfo),
              Stream.filterMap((info) =>
                Option.match(toIdentity(info), {
                  onNone: () => Result.fail(info),
                  onSome: Result.succeed,
                }),
              ),
              Stream.timeout(quietMs),
              Stream.runForEach((identity) => Effect.sync(() => found.push(identity))),
            )
            yield* Effect.race(collect, Effect.sleep(deadlineMs))
          }),
        ).pipe(Effect.mapError((e) => new DiscoveryFailed({ reason: e.reason })))
        return found
      })

      return AgentClient.of({
        discover,

        liveness: () => Effect.sync(() => new Map(beats)),

        isLive: (instanceId) =>
          Effect.gen(function* () {
            const entry = beats.get(instanceId)
            if (entry === undefined) return false
            const now = yield* Clock.currentTimeMillis
            return now - entry.lastSeenMs < 3 * entry.intervalS * 1000
          }),

        heartbeats: () =>
          Stream.unwrap(Effect.map(PubSub.subscribe(hbPubSub), Stream.fromSubscription)),

        prompt: (identity, text, opts) => {
          const endpoint = identity.endpoints.find((e) => e.name === "prompt")
          if (endpoint === undefined) {
            return failLocal("agent exposes no prompt endpoint")
          }
          if (endpoint.maxPayload === undefined) {
            return failLocal("prompt endpoint missing required max_payload metadata")
          }
          const maxPayload = parseMaxPayload(endpoint.maxPayload)
          if (maxPayload === null) {
            return failLocal(`invalid max_payload metadata '${endpoint.maxPayload}'`)
          }
          if (endpoint.attachmentsOk === undefined) {
            return failLocal("prompt endpoint missing required attachments_ok metadata")
          }
          if (opts?.inactivityMs !== undefined && !(opts.inactivityMs > 0)) {
            return failLocal(`inactivityMs must be positive, got ${opts.inactivityMs}`)
          }
          const attachments: readonly Attachment[] = (opts?.attachments ?? []).map((a) => ({
            filename: a.filename,
            content: a.content,
            extra: {},
          }))
          const encoded = safeEncodeRequest({
            prompt: text,
            attachments,
            extra: opts?.extra ?? {},
          })
          if (Result.isFailure(encoded)) return failLocal(encoded.failure)
          const payload = encoded.success
          const parsed = parseRequest(payload)
          if (isRequestError(parsed)) {
            return failLocal(`invalid request: ${parsed._tag}`)
          }
          const violation = validateLocal(parsed, payload.byteLength, {
            maxPayload,
            brokerMaxPayload: wire.brokerMaxPayload,
            attachmentsOk: endpoint.attachmentsOk,
          })
          if (violation !== null) {
            return failLocal(
              violation._tag === "PayloadTooLarge"
                ? `PayloadTooLarge: ${violation.actual} > ${violation.limit}`
                : violation._tag,
            )
          }
          return promptStream({
            subject: endpoint.subject,
            payload,
            inactivityMs: opts?.inactivityMs,
          }).pipe(
            Stream.filter((c) => c._tag !== "Unknown"),
            Stream.provideService(Wire, wire),
          )
        },

        answerQuery: (query, text) => {
          if (!isConcreteSubject(query.replySubject)) {
            return Effect.fail(
              new LocalViolation({ reason: `invalid reply_subject '${query.replySubject}'` }),
            )
          }
          const encoded = safeEncodeRequest({ prompt: text, attachments: [], extra: {} })
          if (Result.isFailure(encoded)) {
            return Effect.fail(new LocalViolation({ reason: encoded.failure }))
          }
          const payload = encoded.success
          const parsed = parseRequest(payload)
          if (isRequestError(parsed)) {
            return Effect.fail(new LocalViolation({ reason: `invalid reply: ${parsed._tag}` }))
          }
          if (payload.byteLength > wire.brokerMaxPayload) {
            return Effect.fail(
              new LocalViolation({
                reason: `PayloadTooLarge: ${payload.byteLength} > ${wire.brokerMaxPayload}`,
              }),
            )
          }
          return wire
            .publish(query.replySubject, payload)
            .pipe(Effect.mapError((e) => new TransportFailed({ reason: e.reason })))
        },
      })
    }),
  )

const failLocal = (reason: string): Stream.Stream<never, PromptCallError> =>
  Stream.fail(new LocalViolation({ reason }))

/** Caller-supplied extras may be unserializable (BigInt, cycles): typed, not thrown. */
const safeEncodeRequest = (
  envelope: Parameters<typeof encodeRequest>[0],
): Result.Result<Uint8Array, string> => {
  try {
    return Result.succeed(encodeRequest(envelope))
  } catch (e) {
    return Result.fail(`unserializable request: ${String(e)}`)
  }
}

const jsonDecoder = new TextDecoder("utf-8", { fatal: false })

const safeJson = (payload: Uint8Array): Result.Result<unknown, Uint8Array> => {
  try {
    return Result.succeed(JSON.parse(jsonDecoder.decode(payload)))
  } catch {
    return Result.fail(payload)
  }
}
