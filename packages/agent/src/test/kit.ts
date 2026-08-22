/**
 * Test interpreters for the Wire seam: a Clock-scripted wire for
 * deterministic engine tests and an in-memory loopback bus (using the real
 * subject matcher) for host⇄client round trips. No servers, no wall time.
 */
import { Cause, Effect, Queue, Stream } from "effect"
import type { Pattern } from "../core/subjects.js"
import { matchSubject } from "../core/subjects.js"
import type {
  PublishOptions,
  ReplyMessage,
  SubscribeOptions,
  WireShape,
} from "../protocol/wire.js"

const encoder = new TextEncoder()

export const msg = (
  payload: string | Uint8Array,
  opts?: Partial<Omit<ReplyMessage, "payload">>,
): ReplyMessage => ({
  payload: typeof payload === "string" ? encoder.encode(payload) : payload,
  hasHeaders: opts?.hasHeaders ?? opts?.errorCode !== undefined,
  errorCode: opts?.errorCode,
  errorMessage: opts?.errorMessage,
  replyTo: opts?.replyTo,
})

export interface ScriptEntry {
  readonly afterMs: number
  readonly message: ReplyMessage
}

export const at = (afterMs: number, message: ReplyMessage): ScriptEntry => ({ afterMs, message })

export const chunk = (afterMs: number, value: unknown): ScriptEntry =>
  at(afterMs, msg(JSON.stringify(value)))

export const terminator = (afterMs: number): ScriptEntry => at(afterMs, msg(new Uint8Array(0)))

export interface ScriptedWire {
  readonly wire: WireShape
  readonly requests: Array<{ readonly subject: string; readonly payload: Uint8Array }>
  readonly published: Array<{
    readonly subject: string
    readonly payload: Uint8Array
    readonly options: PublishOptions | undefined
  }>
  readonly finalizers: () => number
}

export const makeScriptedWire = (
  script: ReadonlyArray<ScriptEntry>,
  options?: {
    readonly brokerMaxPayload?: number
    /** End the reply stream after the script (a real subscription stays open). */
    readonly endAfterScript?: boolean
  },
): ScriptedWire => {
  const requests: ScriptedWire["requests"] = []
  const published: ScriptedWire["published"] = []
  let finalizerCount = 0

  const scripted = Stream.fromIterable(script).pipe(
    Stream.mapEffect((e) => Effect.as(Effect.sleep(e.afterMs), e.message)),
  )
  const replies =
    options?.endAfterScript === true ? scripted : Stream.concat(scripted, Stream.never)

  const open = Effect.acquireRelease(Effect.void, () =>
    Effect.sync(() => {
      finalizerCount++
    }),
  )

  const wire: WireShape = {
    brokerMaxPayload: options?.brokerMaxPayload ?? 1_048_576,
    request: (subject, payload) =>
      Effect.gen(function* () {
        requests.push({ subject, payload })
        yield* open
        return replies
      }),
    subscribe: () => Effect.as(open, replies),
    publish: (subject, payload, opts) =>
      Effect.sync(() => {
        published.push({ subject, payload, options: opts })
      }),
  }

  return { wire, requests, published, finalizers: () => finalizerCount }
}

const patternOf = (subject: string): Pattern => {
  const parts = subject.split(".")
  const gt = parts[parts.length - 1] === ">"
  const toks = (gt ? parts.slice(0, -1) : parts).map((t) =>
    t === "*" ? ({ _tag: "star" } as const) : ({ _tag: "lit", value: t } as const),
  )
  return { toks, gt }
}

interface BusSub {
  readonly pattern: Pattern
  readonly queueGroup: string | undefined
  readonly queue: Queue.Queue<ReplyMessage, Cause.Done>
}

export interface LoopbackBus {
  readonly wire: WireShape
  /** Every publish observed on the bus, in order. */
  readonly log: Array<{
    readonly subject: string
    readonly payload: Uint8Array
    readonly options: PublishOptions | undefined
  }>
  /** Live subscriptions right now — a leak detector for scope hygiene. */
  readonly subscriptionCount: () => number
}

export const makeLoopbackBus = (options?: {
  readonly brokerMaxPayload?: number
}): Effect.Effect<LoopbackBus> =>
  Effect.sync(() => {
    const subs = new Set<BusSub>()
    const log: LoopbackBus["log"] = []
    let inboxCounter = 0

    const groupCursor = new Map<string, number>()

    const deliver = (
      subject: string,
      payload: Uint8Array,
      opts: PublishOptions | undefined,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        log.push({ subject, payload, options: opts })
        const tokens = subject.split(".")
        const message = msg(payload, {
          errorCode: opts?.errorCode,
          errorMessage: opts?.errorMessage,
          replyTo: opts?.replyTo,
        })
        const grouped = new Map<string, BusSub[]>()
        for (const sub of subs) {
          if (!matchSubject(tokens, sub.pattern)) continue
          if (sub.queueGroup === undefined) {
            yield* Queue.offer(sub.queue, message)
          } else {
            const members = grouped.get(sub.queueGroup) ?? []
            members.push(sub)
            grouped.set(sub.queueGroup, members)
          }
        }
        // one copy per queue group, rotated across members (load-balancing)
        for (const [group, members] of grouped) {
          const cursor = groupCursor.get(group) ?? 0
          groupCursor.set(group, cursor + 1)
          yield* Queue.offer(members[cursor % members.length]!.queue, message)
        }
      })

    const subscribe = (subject: string, opts?: SubscribeOptions) =>
      Effect.gen(function* () {
        // slow-consumer overflow drops (at-most-once, like real NATS)
        const queue = yield* Queue.dropping<ReplyMessage, Cause.Done>(1024)
        const sub: BusSub = {
          pattern: patternOf(subject),
          queueGroup: opts?.queueGroup,
          queue,
        }
        yield* Effect.acquireRelease(
          Effect.sync(() => subs.add(sub)),
          () =>
            Effect.gen(function* () {
              subs.delete(sub)
              yield* Queue.shutdown(queue)
            }),
        )
        return Stream.fromQueue(queue)
      })

    const wire: WireShape = {
      brokerMaxPayload: options?.brokerMaxPayload ?? 1_048_576,
      request: (subject, payload) =>
        Effect.gen(function* () {
          const inbox = `_inbox.${inboxCounter++}`
          const replies = yield* subscribe(inbox)
          yield* deliver(subject, payload, { replyTo: inbox })
          return replies
        }),
      subscribe,
      publish: deliver,
    }

    return { wire, log, subscriptionCount: () => subs.size }
  })
