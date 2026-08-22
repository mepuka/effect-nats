import { Clock, Effect, Layer, Queue, Semaphore, Stream } from "effect"
import type {
  ConsumeEvent,
  ConsumeOptions,
  PublishOptions,
  StoredMessage,
  StreamConfig
} from "./JetStream.ts"
import {
  ConsumerLagged,
  JetStream,
  NoMessageForSubject,
  rollupHeader,
  RollupNotPermitted,
  rollupSubject,
  StreamConfigConflict,
  StreamNotFound,
  SubjectNotBound,
  WrongLastSequence
} from "./JetStream.ts"
import type { ConsumeBuffer } from "./JetStream.ts"
import * as Subject from "./Subject.ts"

interface Subscriber {
  readonly queue: Queue.Queue<StoredMessage, StreamNotFound | ConsumerLagged>
  readonly filters: ReadonlyArray<string>
  readonly buffer: ConsumeBuffer
  lastDelivered: bigint
}

interface StreamState {
  readonly config: StreamConfig
  messages: Array<StoredMessage>
  nextSequence: bigint
  readonly subscribers: Set<Subscriber>
}

export interface MemoryJetStream {
  readonly service: JetStream["Service"]
  /** Test diagnostic: live subscription count for the leak-counter pass. */
  readonly subscriberCount: (stream: string) => Effect.Effect<number>
}

const sameConfig = (a: StreamConfig, b: StreamConfig): boolean =>
  a.name === b.name
  && a.maxMessagesPerSubject === b.maxMessagesPerSubject
  && a.allowRollup === b.allowRollup
  && a.subjects.length === b.subjects.length
  && a.subjects.every((subject, i) => subject === b.subjects[i])

const matchesAny = (filters: ReadonlyArray<string>, subject: string): boolean =>
  filters.some((filter) => Subject.matches(filter, subject))

const lastForSubject = (
  messages: ReadonlyArray<StoredMessage>,
  subject: string
): StoredMessage | undefined => {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]?.subject === subject) return messages[i]
  }
  return undefined
}

const selectReplay = (
  messages: ReadonlyArray<StoredMessage>,
  options: ConsumeOptions
): Array<StoredMessage> => {
  const matching = messages.filter((message) => matchesAny(options.filters, message.subject))
  switch (options.start._tag) {
    case "AllHistory":
      return matching
    case "NewOnly":
      return []
    case "FromSequence": {
      const from = options.start.sequence
      return matching.filter((message) => message.sequence >= from)
    }
    case "LastPerSubject": {
      const last = new Map<string, StoredMessage>()
      for (const message of matching) last.set(message.subject, message)
      return [...last.values()].sort((a, b) => a.sequence < b.sequence ? -1 : 1)
    }
  }
}

export const make: Effect.Effect<MemoryJetStream> = Effect.gen(function*() {
  const streams = new Map<string, StreamState>()
  // One permit serializes every mutation and subscription registration, so
  // sequence assignment, fan-out order, and snapshot-versus-live boundaries
  // are atomic with respect to each other.
  const lock = yield* Semaphore.make(1)

  const requireStream = (name: string) =>
    Effect.suspend(() => {
      const state = streams.get(name)
      return state === undefined
        ? Effect.fail(new StreamNotFound({ stream: name }))
        : Effect.succeed(state)
    })

  const service = JetStream.of({
    createStream: (config) =>
      Semaphore.withPermit(lock)(Effect.suspend(() => {
        const existing = streams.get(config.name)
        if (existing !== undefined) {
          return sameConfig(existing.config, config)
            ? Effect.void
            : Effect.fail(new StreamConfigConflict({ stream: config.name }))
        }
        streams.set(config.name, {
          config: { ...config, subjects: [...config.subjects] },
          messages: [],
          nextSequence: 1n,
          subscribers: new Set()
        })
        return Effect.void
      })),

    getStream: (name) =>
      Semaphore.withPermit(lock)(Effect.map(requireStream(name), (s) => s.config)),

    deleteStream: (name) =>
      Semaphore.withPermit(lock)(
        Effect.gen(function*() {
          const state = yield* requireStream(name)
          streams.delete(name)
          yield* Effect.forEach(
            [...state.subscribers],
            (subscriber) => Queue.fail(subscriber.queue, new StreamNotFound({ stream: name })),
            { discard: true }
          )
        })
      ),

    publish: (stream, subject, payload, options?: PublishOptions) =>
      Semaphore.withPermit(lock)(
        Effect.gen(function*() {
          const state = yield* requireStream(stream)
          if (!matchesAny(state.config.subjects, subject)) {
            return yield* new SubjectNotBound({ stream, subject })
          }
          const rollup = options?.headers?.get(rollupHeader) === rollupSubject
          if (rollup && !state.config.allowRollup) {
            return yield* new RollupNotPermitted({ stream })
          }
          const expected = options?.expectedLastSubjectSequence
          if (expected !== undefined) {
            const actual = lastForSubject(state.messages, subject)?.sequence ?? 0n
            if (actual !== expected) {
              return yield* new WrongLastSequence({ stream, subject, expected, actual })
            }
          }
          const message: StoredMessage = {
            subject,
            sequence: state.nextSequence,
            payload,
            headers: new Map(options?.headers ?? []),
            timestampMillis: Number(yield* Clock.currentTimeMillis)
          }
          state.nextSequence = state.nextSequence + 1n
          if (rollup) {
            state.messages = state.messages.filter((m) => m.subject !== subject)
          }
          state.messages.push(message)
          const limit = state.config.maxMessagesPerSubject
          if (limit > 0) {
            const forSubject = state.messages.filter((m) => m.subject === subject)
            if (forSubject.length > limit) {
              const drop = new Set(forSubject.slice(0, forSubject.length - limit))
              state.messages = state.messages.filter((m) => !drop.has(m))
            }
          }
          // Bounded fan-out inside the permit, per each subscriber's declared
          // policy (ADR-0005): PullWindow suspends this publish; TerminateOnLag
          // fails the lagging subscription and spares the publisher.
          yield* Effect.forEach(
            [...state.subscribers].filter((s) => matchesAny(s.filters, subject)),
            (subscriber) =>
              Effect.gen(function*() {
                if (subscriber.buffer._tag === "TerminateOnLag") {
                  const size = yield* Queue.size(subscriber.queue)
                  if (size >= subscriber.buffer.messages) {
                    state.subscribers.delete(subscriber)
                    yield* Queue.fail(
                      subscriber.queue,
                      new ConsumerLagged({
                        stream,
                        lastDeliveredSequence: subscriber.lastDelivered
                      })
                    )
                    return
                  }
                }
                yield* Queue.offer(subscriber.queue, message)
                subscriber.lastDelivered = message.sequence
              }),
            { discard: true }
          )
          return message.sequence
        })
      ),

    lastMessageForSubject: (stream, subject) =>
      Semaphore.withPermit(lock)(
        Effect.gen(function*() {
          const state = yield* requireStream(stream)
          const last = lastForSubject(state.messages, subject)
          if (last === undefined) {
            return yield* new NoMessageForSubject({ stream, subject })
          }
          return last
        })
      ),

    consume: (stream, options) =>
      Stream.unwrap(
        Semaphore.withPermit(lock)(
          Effect.gen(function*() {
            const state = yield* requireStream(stream)
            const queue = yield* Effect.acquireRelease(
              Queue.bounded<StoredMessage, StreamNotFound | ConsumerLagged>(
                options.buffer.messages
              ),
              (q) => Queue.shutdown(q)
            )
            const subscriber: Subscriber = {
              queue,
              filters: options.filters,
              buffer: options.buffer,
              // resume point before any live delivery: the head at registration
              lastDelivered: state.nextSequence - 1n
            }
            // Registration before snapshot selection, both under the permit:
            // no message can land between snapshot and live (case C1).
            yield* Effect.acquireRelease(
              Effect.sync(() => state.subscribers.add(subscriber)),
              () => Effect.sync(() => state.subscribers.delete(subscriber))
            )
            const snapshot = selectReplay(state.messages, options)
            const entry = (message: StoredMessage): ConsumeEvent => ({ _tag: "Entry", message })
            const caughtUp: ConsumeEvent = { _tag: "CaughtUp" }
            return Stream.fromIterable(snapshot.map(entry)).pipe(
              Stream.concat(Stream.make(caughtUp)),
              Stream.concat(Stream.fromQueue(queue).pipe(Stream.map(entry)))
            )
          })
        )
      )
  })

  return {
    service,
    subscriberCount: (stream) =>
      Semaphore.withPermit(lock)(
        Effect.sync(() => streams.get(stream)?.subscribers.size ?? 0)
      )
  }
})

export const layer: Layer.Layer<JetStream> = Layer.effect(
  JetStream,
  Effect.map(make, (memory) => memory.service)
)
