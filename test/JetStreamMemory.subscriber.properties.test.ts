import { expect, it } from "@effect/vitest"
import { Effect, Exit } from "effect"
import { FastCheck } from "effect/testing"
import type { ConsumeOptions, StartPosition, StoredMessage } from "../src/internal/JetStream.ts"
import * as Memory from "../src/internal/JetStreamMemory.ts"
import type { GatedSubscriber } from "./LeanSubscriberReplay.ts"
import { makeGatedSubscriber } from "./LeanSubscriberReplay.ts"
import { normalize } from "./LeanTraceReplay.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()
const ignored = new Set(["timestampMillis"])
const stream = "PROP_SUBSCRIBER"
const matchingPattern = "prop.match.>"
const otherPattern = "prop.other.>"
const config = {
  name: stream,
  subjects: [matchingPattern, otherPattern],
  maxMessagesPerSubject: 0,
  allowRollup: true
} as const

const capacityArbitrary = FastCheck.integer({ min: 1, max: 3 })
const payloadArbitrary = FastCheck.integer({ min: 0, max: 999 })
const startArbitrary = FastCheck.oneof(
  FastCheck.constant({ _tag: "AllHistory" as const }),
  FastCheck.constant({ _tag: "LastPerSubject" as const }),
  FastCheck.constant({ _tag: "NewOnly" as const }),
  FastCheck.record({
    _tag: FastCheck.constant("FromSequence" as const),
    sequence: FastCheck.integer({ min: 0, max: 10 })
  }),
  FastCheck.record({
    _tag: FastCheck.constant("AfterSequence" as const),
    sequence: FastCheck.integer({ min: 0, max: 10 })
  })
)
type StartSpec =
  | { readonly _tag: "AllHistory" }
  | { readonly _tag: "LastPerSubject" }
  | { readonly _tag: "NewOnly" }
  | { readonly _tag: "FromSequence"; readonly sequence: number }
  | { readonly _tag: "AfterSequence"; readonly sequence: number }

const toStart = (start: StartSpec): StartPosition =>
  start._tag === "FromSequence" || start._tag === "AfterSequence"
    ? { ...start, sequence: BigInt(start.sequence) }
    : start

const options = (
  start: StartPosition,
  messages: number,
  filters: ReadonlyArray<string> = [matchingPattern]
): ConsumeOptions => ({
  filters,
  start,
  buffer: { _tag: "TerminateOnLag", messages }
})

interface EntryEvent {
  readonly _tag: "Entry"
  readonly message: StoredMessage
}

const isEntry = (event: unknown): event is EntryEvent =>
  typeof event === "object" && event !== null && "_tag" in event && event._tag === "Entry"
  && "message" in event

const entrySequences = (events: ReadonlyArray<unknown>): ReadonlyArray<bigint> =>
  events.filter(isEntry).map((event) => event.message.sequence)

const strictlyIncreasing = (sequences: ReadonlyArray<bigint>): boolean =>
  sequences.every((sequence, index) => index === 0 || (sequences[index - 1] ?? 0n) < sequence)

const payloads = (events: ReadonlyArray<unknown>): ReadonlyArray<string> =>
  events.filter(isEntry).map((event) => decoder.decode(event.message.payload))

const cleanup = (subscribers: Iterable<GatedSubscriber>): Effect.Effect<void> =>
  Effect.forEach(subscribers, (subscriber) => subscriber.interrupt, { discard: true })

type RandomLabel =
  | { readonly _tag: "createStream" }
  | { readonly _tag: "publish"; readonly matching: boolean; readonly payload: number }
  | {
    readonly _tag: "register"
    readonly capacity: number
    readonly matching: boolean
    readonly start: StartSpec
  }
  | { readonly _tag: "pull" }
  | { readonly _tag: "unsubscribe" }
  | { readonly _tag: "deleteStream" }
  | { readonly _tag: "getStream" }
  | { readonly _tag: "lastMessageForSubject" }

const randomLabelArbitrary: FastCheck.Arbitrary<RandomLabel> = FastCheck.oneof(
  FastCheck.constant({ _tag: "createStream" as const }),
  FastCheck.record({
    _tag: FastCheck.constant("publish" as const),
    matching: FastCheck.boolean(),
    payload: payloadArbitrary
  }),
  FastCheck.record({
    _tag: FastCheck.constant("register" as const),
    capacity: capacityArbitrary,
    matching: FastCheck.boolean(),
    start: startArbitrary
  }),
  FastCheck.constant({ _tag: "pull" as const }),
  FastCheck.constant({ _tag: "unsubscribe" as const }),
  FastCheck.constant({ _tag: "deleteStream" as const }),
  FastCheck.constant({ _tag: "getStream" as const }),
  FastCheck.constant({ _tag: "lastMessageForSubject" as const })
)
const randomLabelsArbitrary = FastCheck.array(randomLabelArbitrary, {
  minLength: 1,
  maxLength: 30
})

it.effect.prop(
  "visible_sequences_strict — observed entry sequences stay strictly increasing",
  { labels: randomLabelsArbitrary },
  ({ labels }) =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      const subscribers: Array<GatedSubscriber> = []
      let active: GatedSubscriber | undefined
      let activeCapacity = 0
      let activeFilter = matchingPattern
      let activeAccepting = false
      let pending = 0
      let failurePending = false
      let streamExists = false
      let nextId = 0
      let publishIndex = 0
      const body = Effect.gen(function*() {
        for (const label of labels) {
          switch (label._tag) {
            case "createStream":
              if (!streamExists) {
                yield* service.createStream(config)
                streamExists = true
              }
              break
            case "publish": {
              if (!streamExists) break
              const subject = label.matching ? "prop.match.k" : "prop.other.k"
              const exit = yield* service.publish(
                stream,
                subject,
                utf8.encode(`p${publishIndex++}-${label.payload}`)
              ).pipe(Effect.exit)
              if (
                Exit.isSuccess(exit) && active !== undefined && activeAccepting
                && (label.matching
                  ? activeFilter === matchingPattern
                  : activeFilter === otherPattern)
              ) {
                if (pending < activeCapacity) pending++
                else {
                  failurePending = true
                  activeAccepting = false
                }
              }
              break
            }
            case "register":
              if (!streamExists) break
              if (active !== undefined) yield* active.interrupt
              activeFilter = label.matching ? matchingPattern : otherPattern
              activeCapacity = label.capacity
              activeAccepting = true
              pending = 0
              failurePending = false
              active = yield* makeGatedSubscriber(
                service,
                nextId++,
                stream,
                options(toStart(label.start), label.capacity, [activeFilter])
              )
              subscribers.push(active)
              yield* active.awaitRegistration
              break
            case "pull":
              if (active === undefined) break
              if (pending > 0) {
                yield* active.pull
                pending = 0
              } else if (failurePending) {
                yield* active.pull
                failurePending = false
              }
              break
            case "unsubscribe":
              if (active !== undefined) yield* active.interrupt
              active = undefined
              activeAccepting = false
              pending = 0
              failurePending = false
              break
            case "deleteStream":
              if (!streamExists) break
              yield* service.deleteStream(stream)
              streamExists = false
              if (active !== undefined) {
                activeAccepting = false
                failurePending = true
              }
              break
            case "getStream":
              yield* service.getStream(stream).pipe(Effect.exit)
              break
            case "lastMessageForSubject":
              yield* service.lastMessageForSubject(stream, "prop.match.k").pipe(Effect.exit)
              break
          }
          yield* Effect.yieldNow
        }
        for (const subscriber of subscribers) {
          expect(strictlyIncreasing(entrySequences(subscriber.observed))).toBe(true)
        }
      })
      yield* body.pipe(Effect.ensuring(Effect.suspend(() => cleanup(subscribers))))
    }),
  { fastCheck: { numRuns: 200 } }
)

it.effect.prop(
  "publish_visible (room) — one matching publish with room is the next gated entry",
  { capacity: capacityArbitrary, payload: payloadArbitrary },
  ({ capacity, payload }) =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      yield* service.createStream(config)
      const subscriber = yield* makeGatedSubscriber(
        service,
        0,
        stream,
        options({ _tag: "NewOnly" }, capacity)
      )
      const body = Effect.gen(function*() {
        expect(yield* subscriber.awaitRegistration).toEqual([{ _tag: "CaughtUp" }])
        const sequence = yield* service.publish(
          stream,
          "prop.match.k",
          utf8.encode(`room-${payload}`)
        )
        const pulled = yield* subscriber.pull
        expect(entrySequences(pulled)).toEqual([sequence])
        expect(payloads(pulled)).toEqual([`room-${payload}`])
      })
      yield* body.pipe(Effect.ensuring(subscriber.interrupt))
    }),
  { fastCheck: { numRuns: 200 } }
)

it.effect.prop(
  "publish_visible (full) / lagged_iff — overflow drains the full buffer then fails",
  { capacity: capacityArbitrary, payload: payloadArbitrary },
  ({ capacity, payload }) =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      yield* service.createStream(config)
      const subscriber = yield* makeGatedSubscriber(
        service,
        0,
        stream,
        options({ _tag: "NewOnly" }, capacity)
      )
      const body = Effect.gen(function*() {
        yield* subscriber.awaitRegistration
        for (let index = 1; index <= capacity; index++) {
          yield* service.publish(
            stream,
            "prop.match.k",
            utf8.encode(`full-${payload}-${index}`)
          )
        }
        yield* service.publish(stream, "prop.match.k", utf8.encode(`overflow-${payload}`))
        expect(entrySequences(yield* subscriber.pull)).toEqual(
          Array.from({ length: capacity }, (_, index) => BigInt(index + 1))
        )
        expect(normalize(yield* subscriber.pull, ignored)).toEqual([{
          _tag: "Failed",
          error: {
            _tag: "ConsumerLagged",
            stream,
            lastDeliveredSequence: String(capacity)
          }
        }])
        const before = subscriber.observed.length
        yield* service.publish(stream, "prop.match.k", utf8.encode(`after-${payload}`))
        for (let turn = 0; turn < 4; turn++) yield* Effect.yieldNow
        expect(subscriber.observed).toHaveLength(before)
      })
      yield* body.pipe(Effect.ensuring(subscriber.interrupt))
    }),
  { fastCheck: { numRuns: 200 } }
)

it.effect.prop(
  "op_visible_frame — non-matching and read-only operations add no subscriber event",
  {
    frame: FastCheck.constantFrom("nonMatchingPublish", "getStream", "lastMessage"),
    payload: payloadArbitrary
  },
  ({ frame, payload }) =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      yield* service.createStream(config)
      yield* service.publish(stream, "prop.match.seed", utf8.encode("seed"))
      const subscriber = yield* makeGatedSubscriber(
        service,
        0,
        stream,
        options({ _tag: "NewOnly" }, 3)
      )
      const body = Effect.gen(function*() {
        yield* subscriber.awaitRegistration
        if (frame === "nonMatchingPublish") {
          yield* service.publish(stream, "prop.other.k", utf8.encode(`other-${payload}`))
        } else if (frame === "getStream") {
          yield* service.getStream(stream)
        } else {
          yield* service.lastMessageForSubject(stream, "prop.match.seed")
        }
        for (let turn = 0; turn < 4; turn++) yield* Effect.yieldNow
        expect(subscriber.observed).toEqual([{ _tag: "CaughtUp" }])
        yield* service.publish(stream, "prop.match.k", utf8.encode(`sentinel-${payload}`))
        expect(payloads(yield* subscriber.pull)).toEqual([`sentinel-${payload}`])
      })
      yield* body.pipe(Effect.ensuring(subscriber.interrupt))
    }),
  { fastCheck: { numRuns: 200 } }
)

it.effect.prop(
  "delete_ends — deletion drains admitted entries then reports StreamNotFound",
  { capacity: capacityArbitrary, bufferedRaw: FastCheck.integer({ min: 0, max: 9 }) },
  ({ capacity, bufferedRaw }) =>
    Effect.gen(function*() {
      const buffered = bufferedRaw % (capacity + 1)
      const { service } = yield* Memory.make
      yield* service.createStream(config)
      const subscriber = yield* makeGatedSubscriber(
        service,
        0,
        stream,
        options({ _tag: "NewOnly" }, capacity)
      )
      const body = Effect.gen(function*() {
        yield* subscriber.awaitRegistration
        for (let index = 1; index <= buffered; index++) {
          yield* service.publish(stream, "prop.match.k", utf8.encode(`delete-${index}`))
        }
        yield* service.deleteStream(stream)
        if (buffered > 0) {
          expect(entrySequences(yield* subscriber.pull)).toEqual(
            Array.from({ length: buffered }, (_, index) => BigInt(index + 1))
          )
        }
        expect(normalize(yield* subscriber.pull, ignored)).toEqual([{
          _tag: "Failed",
          error: { _tag: "StreamNotFound", stream }
        }])
      })
      yield* body.pipe(Effect.ensuring(subscriber.interrupt))
    }),
  { fastCheck: { numRuns: 200 } }
)

interface ReplayInput {
  readonly subject: number
  readonly payload: number
}

const replayMessagesArbitrary = FastCheck.array(
  FastCheck.record({
    subject: FastCheck.integer({ min: 0, max: 2 }),
    payload: payloadArbitrary
  }),
  { minLength: 0, maxLength: 8 }
)

const selectReplay = (
  messages: ReadonlyArray<StoredMessage>,
  start: StartPosition,
  filter: string
): ReadonlyArray<StoredMessage> => {
  const matching = messages.filter((message) =>
    filter === matchingPattern
      ? message.subject.startsWith("prop.match.")
      : message.subject === filter
  )
  switch (start._tag) {
    case "AllHistory":
      return matching
    case "NewOnly":
      return []
    case "FromSequence":
      return matching.filter((message) => message.sequence >= start.sequence)
    case "AfterSequence":
      return matching.filter((message) => message.sequence > start.sequence)
    case "LastPerSubject": {
      const last = new Map<string, StoredMessage>()
      for (const message of matching) last.set(message.subject, message)
      return Array.from(last.values()).sort((a, b) => a.sequence < b.sequence ? -1 : 1)
    }
  }
}

it.effect.prop(
  "register_observed / selectReplay_mem — registration is exactly the selected replay prefix",
  {
    inputs: replayMessagesArbitrary,
    start: startArbitrary,
    capacity: capacityArbitrary,
    exactFilter: FastCheck.boolean()
  },
  ({ inputs, start, capacity, exactFilter }) =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      yield* service.createStream(config)
      const stored: Array<StoredMessage> = []
      for (let index = 0; index < inputs.length; index++) {
        const input: ReplayInput | undefined = inputs[index]
        if (input === undefined) continue
        const subject = `prop.match.k${input.subject}`
        const payload = utf8.encode(`replay-${input.payload}`)
        const sequence = yield* service.publish(stream, subject, payload)
        stored.push({ subject, sequence, payload, headers: new Map(), timestampMillis: 0 })
      }
      const startPosition = toStart(start)
      const filter = exactFilter ? "prop.match.k0" : matchingPattern
      const subscriber = yield* makeGatedSubscriber(
        service,
        0,
        stream,
        options(startPosition, capacity, [filter])
      )
      const body = Effect.gen(function*() {
        const registration = yield* subscriber.awaitRegistration
        const actual = registration.filter(isEntry).map((event) => ({
          subject: event.message.subject,
          sequence: event.message.sequence,
          payload: decoder.decode(event.message.payload)
        }))
        const expected = selectReplay(stored, startPosition, filter).map((message) => ({
          subject: message.subject,
          sequence: message.sequence,
          payload: decoder.decode(message.payload)
        }))
        expect(actual).toEqual(expected)
        expect(registration.at(-1)).toEqual({ _tag: "CaughtUp" })
      })
      yield* body.pipe(Effect.ensuring(subscriber.interrupt))
    }),
  { fastCheck: { numRuns: 200 } }
)
