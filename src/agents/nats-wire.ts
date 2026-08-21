/**
 * Wire interpreter over a live NatsConnection.
 *
 * Subscriptions run in CALLBACK mode feeding a scoped Queue — never the
 * async-iterator bridge. The iterator's `return()` does not settle while a
 * `next()` is pending, which deadlocks interruption (a timeout or race
 * loser waits on stream cleanup, which waits on a message that will never
 * come). Callback mode has no pending pull: scope close unsubscribes
 * synchronously and shuts the queue down.
 *
 * Subscribe-before-publish ordering is structural: `request` acquires the
 * inbox subscription before publishing. The queue is bounded dropping —
 * the transport is at-most-once and the real client sheds on slow
 * consumers too; correctness never depends on unbounded buffering.
 */
import { Cause, Effect, Layer, Queue, Stream } from "effect"
import { createInbox, headers as mkHeaders } from "@nats-io/nats-core"
import type { Msg } from "@nats-io/nats-core"
import { WireFailure } from "./errors.js"
import { NatsConnection } from "./nats-connection.js"
import { Wire, type PublishOptions, type ReplyMessage, type SubscribeOptions } from "./wire.js"

const toWireFailure = (context: string) => (cause: unknown) =>
  new WireFailure({ reason: `${context}: ${String(cause)}` })

const headerValue = (m: Msg, key: string): string | undefined => {
  const h = m.headers
  if (h === undefined || h === null) return undefined
  const v = h.get(key)
  if (v === undefined || v === "") return undefined
  return Array.isArray(v) ? (v[0] ?? undefined) : v
}

const toReplyMessage = (m: Msg): ReplyMessage => {
  const rawCode = headerValue(m, "Nats-Service-Error-Code")
  const code = rawCode === undefined ? undefined : Number(rawCode)
  return {
    payload: m.data,
    hasHeaders: m.headers !== undefined && m.headers !== null,
    errorCode: code !== undefined && Number.isFinite(code) ? code : undefined,
    errorMessage: headerValue(m, "Nats-Service-Error"),
    replyTo: m.reply,
  }
}

const SUBSCRIPTION_BUFFER = 8_192

export const layerLive: Layer.Layer<Wire, never, NatsConnection> = Layer.effect(Wire)(
  Effect.gen(function* () {
    const conn = yield* NatsConnection
    const nc = conn.nc

    const acquireReplies = (subject: string, options?: SubscribeOptions) =>
      Effect.gen(function* () {
        const queue = yield* Effect.acquireRelease(
          Queue.dropping<ReplyMessage, WireFailure | Cause.Done>(SUBSCRIPTION_BUFFER),
          (q) => Queue.shutdown(q),
        )
        yield* Effect.acquireRelease(
          Effect.try({
            try: () =>
              nc.subscribe(subject, {
                ...(options?.queueGroup !== undefined ? { queue: options.queueGroup } : {}),
                callback: (err, m) => {
                  if (err !== null) {
                    Queue.failCauseUnsafe(
                      queue,
                      Cause.fail(
                        new WireFailure({ reason: `subscription ${subject}: ${String(err)}` }),
                      ),
                    )
                  } else if (m.headers?.code === 503 && m.data.length === 0) {
                    // server-generated no-responders status: nobody is bound
                    Queue.failCauseUnsafe(
                      queue,
                      Cause.fail(new WireFailure({ reason: `no responders for ${m.subject}` })),
                    )
                  } else {
                    Queue.offerUnsafe(queue, toReplyMessage(m))
                  }
                },
              }),
            catch: toWireFailure(`subscribe ${subject}`),
          }),
          (sub) =>
            Effect.sync(() => {
              try {
                sub.unsubscribe()
              } catch {
                // connection already closed: interest is gone either way
              }
            }),
        )
        return Stream.fromQueue(queue)
      })

    const publish = (
      subject: string,
      payload: Uint8Array,
      options?: PublishOptions,
    ): Effect.Effect<void, WireFailure> =>
      Effect.try({
        try: () => {
          let hdrs
          if (options?.errorCode !== undefined) {
            hdrs = mkHeaders()
            hdrs.set("Nats-Service-Error-Code", String(options.errorCode))
            hdrs.set("Nats-Service-Error", options.errorMessage ?? "")
          }
          nc.publish(subject, payload, {
            ...(options?.replyTo !== undefined ? { reply: options.replyTo } : {}),
            ...(hdrs !== undefined ? { headers: hdrs } : {}),
          })
        },
        catch: toWireFailure(`publish ${subject}`),
      })

    return Wire.of({
      brokerMaxPayload: conn.maxPayload,

      request: (subject, payload) =>
        Effect.gen(function* () {
          const inbox = createInbox()
          const replies = yield* acquireReplies(inbox)
          yield* publish(subject, payload, { replyTo: inbox })
          return replies
        }),

      subscribe: acquireReplies,

      publish,
    })
  }),
)
