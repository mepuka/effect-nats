/**
 * Wire interpreter over a live NatsConnection.
 *
 * Subscribe-before-publish ordering is structural: the inbox subscription is
 * bound before the request publishes (SUB precedes PUB on one connection, so
 * the server registers interest first). The subscription lives in the
 * caller's scope — dropping it IS cancellation (Protocol §6.7: interest
 * drops, server discards at no cost). Unsubscribe is an explicit release
 * finalizer, not an iterator side effect, so it runs exactly once on scope
 * close regardless of how the stream ended.
 */
import { Effect, Layer, Stream } from "effect"
import { createInbox, headers as mkHeaders } from "@nats-io/nats-core"
import type { Msg, Subscription } from "@nats-io/nats-core"
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

export const layerLive: Layer.Layer<Wire, never, NatsConnection> = Layer.effect(Wire)(
  Effect.gen(function* () {
    const conn = yield* NatsConnection
    const nc = conn.nc

    const acquireSubscription = (subject: string, options?: SubscribeOptions) =>
      Effect.acquireRelease(
        Effect.try({
          try: () =>
            nc.subscribe(
              subject,
              options?.queueGroup !== undefined ? { queue: options.queueGroup } : {},
            ),
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

    const subscriptionStream = (sub: Subscription): Stream.Stream<ReplyMessage, WireFailure> =>
      Stream.fromAsyncIterable(sub, toWireFailure("subscription terminated abnormally")).pipe(
        Stream.map(toReplyMessage),
      )

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
          // bind first…
          const sub = yield* acquireSubscription(inbox)
          // …then publish (Protocol §8.5 subscribe-before-publish ordering)
          yield* publish(subject, payload, { replyTo: inbox })
          return subscriptionStream(sub)
        }),

      subscribe: (subject, options) =>
        Effect.map(acquireSubscription(subject, options), subscriptionStream),

      publish,
    })
  }),
)
