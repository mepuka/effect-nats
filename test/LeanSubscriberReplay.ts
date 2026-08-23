import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Exit, Fiber, Queue, Result, Schema, Stream } from "effect"
import { existsSync, readFileSync } from "node:fs"
import type {
  ConsumeOptions as RuntimeConsumeOptions,
  ConsumerLagged,
  JetStream,
  StreamNotFound
} from "../src/internal/JetStream.ts"
import { ExpectedOutcome, Message, normalize, observeStep, Op } from "./LeanTraceReplay.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()
const findingsDocument = "docs/research/lean-trace-live-findings.md"

const StartPosition = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("AllHistory") }),
  Schema.Struct({ _tag: Schema.Literal("LastPerSubject") }),
  Schema.Struct({ _tag: Schema.Literal("NewOnly") }),
  Schema.Struct({ _tag: Schema.Literal("FromSequence"), sequence: Schema.String }),
  Schema.Struct({ _tag: Schema.Literal("AfterSequence"), sequence: Schema.String })
])
const ConsumeBuffer = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("PullWindow"), messages: Schema.Number }),
  Schema.Struct({ _tag: Schema.Literal("TerminateOnLag"), messages: Schema.Number })
])
const ConsumeOptions = Schema.Struct({
  filters: Schema.Array(Schema.String),
  start: StartPosition,
  buffer: ConsumeBuffer
})
const StreamNotFoundError = Schema.Struct({
  _tag: Schema.Literal("StreamNotFound"),
  stream: Schema.String
})
const ConsumerLaggedError = Schema.Struct({
  _tag: Schema.Literal("ConsumerLagged"),
  stream: Schema.String,
  lastDeliveredSequence: Schema.String
})
const SubscriberError = Schema.Union([StreamNotFoundError, ConsumerLaggedError])
const SubscriberEvent = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Entry"), message: Message }),
  Schema.Struct({ _tag: Schema.Literal("CaughtUp") }),
  Schema.Struct({ _tag: Schema.Literal("Failed"), error: SubscriberError })
])
const Count = Schema.Tuple([Schema.String, Schema.Number])
const Counts = Schema.Array(Count)
const OpStep = Schema.Struct({
  label: Schema.Literal("op"),
  op: Op,
  expect: ExpectedOutcome,
  counts: Counts
})
const RegisterStep = Schema.Struct({
  label: Schema.Literal("register"),
  id: Schema.Number,
  stream: Schema.String,
  options: ConsumeOptions,
  lastEnqueued: Schema.String,
  expect: ExpectedOutcome,
  events: Schema.Array(SubscriberEvent),
  counts: Counts
})
const PullStep = Schema.Struct({
  label: Schema.Literal("pull"),
  id: Schema.Number,
  events: Schema.Array(SubscriberEvent),
  counts: Counts
})
const UnsubscribeStep = Schema.Struct({
  label: Schema.Literal("unsubscribe"),
  id: Schema.Number,
  counts: Counts
})
const SubscriberStep = Schema.Union([OpStep, RegisterStep, PullStep, UnsubscribeStep])

export const SummaryEvent = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Entry"), sequence: Schema.String }),
  Schema.Struct({ _tag: Schema.Literal("CaughtUp") }),
  Schema.Struct({ _tag: Schema.Literal("Failed"), error: SubscriberError })
])
export const SubscriberHistory = Schema.Array(Schema.Array(SummaryEvent))
const FreeRunning = Schema.Struct({
  outcomes: Schema.Record(Schema.String, Schema.Array(SubscriberHistory))
})
export const SubscriberTrace = Schema.Struct({
  name: Schema.String,
  mirrors: Schema.Array(Schema.String),
  steps: Schema.Array(SubscriberStep),
  finalObserved: Schema.Array(Schema.Struct({
    id: Schema.Number,
    events: Schema.Array(SubscriberEvent)
  })),
  freeRunning: FreeRunning
})
export const SubscriberTraceFixture = Schema.Struct({
  schema: Schema.Literal(2),
  producer: Schema.String,
  snapshot: Schema.String,
  pin: Schema.String,
  foldableCommit: Schema.NullOr(Schema.String),
  compare: Schema.Struct({ ignore: Schema.Array(Schema.String) }),
  traces: Schema.Array(SubscriberTrace)
})
const SubscriberStepOverride = Schema.Struct({
  finding: Schema.String,
  outcome: Schema.optional(ExpectedOutcome),
  events: Schema.optional(Schema.Array(SubscriberEvent)),
  counts: Schema.optional(Schema.Literal("unobservable"))
})
const SubscriberTraceOverrides = Schema.Struct({
  steps: Schema.Record(Schema.String, SubscriberStepOverride)
})
const SubscriberLiveOverrides = Schema.Struct({
  schema: Schema.Literal(2),
  traces: Schema.Record(Schema.String, SubscriberTraceOverrides)
})

export type FixtureSubscriberTrace = typeof SubscriberTrace.Type
export type FixtureSubscriberStep = typeof SubscriberStep.Type

const fixtureUrl = new URL("./fixtures/lean/subscriber-traces.json", import.meta.url)
const fixtureInput: unknown = JSON.parse(readFileSync(fixtureUrl, "utf8"))
export const subscriberTraceFixture = Result.getOrThrow(
  Schema.decodeUnknownResult(SubscriberTraceFixture)(fixtureInput)
)
const overridesUrl = new URL("./fixtures/lean/live-overrides-subscriber.json", import.meta.url)
const subscriberLiveOverrides = existsSync(overridesUrl)
  ? Result.getOrThrow(
    Schema.decodeUnknownResult(SubscriberLiveOverrides)(
      JSON.parse(readFileSync(overridesUrl, "utf8"))
    )
  )
  : { schema: 2 as const, traces: {} }

export interface SubscriberReplayService {
  readonly service: JetStream["Service"]
  readonly subscriberCount?: (stream: string) => Effect.Effect<number>
  /** Live-only wire barrier: prior publishes reach callbacks before a modeled takeAll. */
  readonly settleBeforePull?: Effect.Effect<void, unknown>
}

export type WithSubscriberService = <A, E>(
  f: (target: SubscriberReplayService) => Effect.Effect<A, E>
) => Effect.Effect<A, unknown>

type RuntimeFailure = StreamNotFound | ConsumerLagged | unknown

interface Gate {
  readonly release: Deferred.Deferred<void>
}

interface ConsumerState {
  readonly id: number
  readonly observed: Array<unknown>
  readonly chunks: Array<ReadonlyArray<unknown>>
  readonly gates: Queue.Queue<Gate>
  readonly signals: Queue.Queue<void>
  readonly fiber: Fiber.Fiber<void, RuntimeFailure>
  gate: Gate | undefined
  exit: Exit.Exit<void, RuntimeFailure> | undefined
  unexpected: unknown | undefined
}

const optionsOf = (options: typeof ConsumeOptions.Type) => ({
  filters: options.filters,
  start: options.start._tag === "FromSequence" || options.start._tag === "AfterSequence"
    ? { ...options.start, sequence: BigInt(options.start.sequence) }
    : options.start,
  buffer: options.buffer
})

const failureEvent = (error: unknown): unknown => ({ _tag: "Failed", error })

const isRecordedFailure = (error: unknown): boolean =>
  typeof error === "object"
  && error !== null
  && "_tag" in error
  && (error._tag === "ConsumerLagged" || error._tag === "StreamNotFound")

const startGatedState = (
  service: JetStream["Service"],
  id: number,
  stream: string,
  options: RuntimeConsumeOptions
): Effect.Effect<ConsumerState> =>
  Effect.gen(function*() {
    const observed: Array<unknown> = []
    const chunks: Array<ReadonlyArray<unknown>> = []
    const gates = yield* Queue.unbounded<Gate>()
    const signals = yield* Queue.unbounded<void>()
    let caughtUp = false
    let exit: Exit.Exit<void, RuntimeFailure> | undefined
    let unexpected: unknown | undefined

    const run = Stream.runForEachArray(
      service.consume(stream, options),
      (chunk) =>
        Effect.gen(function*() {
          const recorded = Array.from(chunk)
          observed.push(...recorded)
          chunks.push(recorded)
          if (recorded.some((event) => event._tag === "CaughtUp")) caughtUp = true
          yield* Queue.offer(signals, void 0)
          if (caughtUp) {
            const release = yield* Deferred.make<void>()
            const gate = { release }
            yield* Queue.offer(gates, gate)
            yield* Deferred.await(release)
          }
        })
    ).pipe(
      Effect.onExit((fiberExit) =>
        Effect.gen(function*() {
          exit = fiberExit
          if (Exit.isFailure(fiberExit)) {
            const error = Exit.findError(fiberExit)
            if (Result.isSuccess(error)) {
              if (isRecordedFailure(error.success)) {
                observed.push(failureEvent(error.success))
              } else {
                unexpected = error.success
              }
            } else if (!Exit.hasInterrupts(fiberExit)) {
              unexpected = Result.getOrThrow(Exit.findDefect(fiberExit))
            }
          }
          yield* Queue.offer(signals, void 0)
        })
      )
    )
    const fiber = yield* Effect.forkChild(run, { startImmediately: true })
    return {
      id,
      observed,
      chunks,
      gates,
      signals,
      fiber,
      gate: undefined,
      get exit() {
        return exit
      },
      set exit(value) {
        exit = value
      },
      get unexpected() {
        return unexpected
      },
      set unexpected(value) {
        unexpected = value
      }
    }
  })

const startConsumer = (
  service: JetStream["Service"],
  step: typeof RegisterStep.Type
): Effect.Effect<ConsumerState> =>
  startGatedState(service, step.id, step.stream, optionsOf(step.options))

const waitForLength = (
  state: ConsumerState,
  length: number
): Effect.Effect<void> =>
  Effect.gen(function*() {
    while (state.observed.length < length && state.exit === undefined) {
      yield* Queue.take(state.signals)
    }
  })

const assertExpected = (
  actual: unknown,
  expected: unknown,
  ignored: ReadonlySet<string>,
  context: string
): void => {
  expect(normalize(actual, ignored), context).toEqual(normalize(expected, ignored))
}

const armGate = (state: ConsumerState, context: string): Effect.Effect<void> =>
  Effect.gen(function*() {
    state.gate = yield* Queue.take(state.gates)
    yield* Effect.yieldNow
    expect(yield* Deferred.isDone(state.gate.release), `${context}: gate remains pending`).toBe(
      false
    )
    expect(state.fiber.pollUnsafe(), `${context}: consumer fiber remains parked`).toBeUndefined()
  })

export interface GatedSubscriber {
  readonly observed: ReadonlyArray<unknown>
  readonly chunks: ReadonlyArray<ReadonlyArray<unknown>>
  readonly awaitRegistration: Effect.Effect<ReadonlyArray<unknown>>
  readonly pull: Effect.Effect<ReadonlyArray<unknown>>
  readonly interrupt: Effect.Effect<void>
}

/** Test-only controller implementing the schema-2 one-gate-per-takeAll protocol. */
export const makeGatedSubscriber = (
  service: JetStream["Service"],
  id: number,
  stream: string,
  options: RuntimeConsumeOptions
): Effect.Effect<GatedSubscriber> =>
  Effect.map(startGatedState(service, id, stream, options), (state) => {
    const awaitRegistration = Effect.gen(function*() {
      const caughtUp = () =>
        state.observed.some((event) =>
          typeof event === "object" && event !== null && "_tag" in event
          && event._tag === "CaughtUp"
        )
      while (!caughtUp() && state.exit === undefined) yield* Queue.take(state.signals)
      if (state.unexpected !== undefined) return yield* Effect.die(state.unexpected)
      if (!caughtUp()) {
        return yield* Effect.die(new Error(`subscriber ${id} exited before CaughtUp`))
      }
      yield* armGate(state, `subscriber ${id} registration`)
      return state.observed.slice()
    })
    const pull = Effect.gen(function*() {
      const gate = state.gate
      if (gate === undefined) return yield* Effect.die(new Error(`subscriber ${id} has no gate`))
      state.gate = undefined
      const beforeObserved = state.observed.length
      const beforeChunks = state.chunks.length
      if (!(yield* Deferred.succeed(gate.release, void 0))) {
        return yield* Effect.die(new Error(`subscriber ${id} gate was already released`))
      }
      while (state.chunks.length === beforeChunks && state.exit === undefined) {
        yield* Queue.take(state.signals)
      }
      if (state.unexpected !== undefined) return yield* Effect.die(state.unexpected)
      const events = state.observed.slice(beforeObserved)
      if (state.exit === undefined) yield* armGate(state, `subscriber ${id} after pull`)
      return events
    })
    return {
      observed: state.observed,
      chunks: state.chunks,
      awaitRegistration,
      pull,
      interrupt: Fiber.interrupt(state.fiber).pipe(Effect.asVoid)
    }
  })

const checkCounts = (
  target: SubscriberReplayService,
  counts: ReadonlyArray<readonly [string, number]>,
  context: string,
  override: typeof SubscriberStepOverride.Type | undefined
): Effect.Effect<void> =>
  target.subscriberCount === undefined
    ? Effect.sync(() => {
      if (counts.length > 0) {
        expect(override?.counts, `${context}: live count override`).toBe("unobservable")
      }
    })
    : Effect.forEach(
      counts,
      ([stream, expected]) =>
        Effect.map(target.subscriberCount!(stream), (actual) => {
          expect(actual, `${context}: subscriberCount(${stream})`).toBe(expected)
        }),
      { discard: true }
    )

const cleanup = (states: Iterable<ConsumerState>): Effect.Effect<void> =>
  Effect.forEach(states, (state) => Fiber.interrupt(state.fiber), { discard: true })

const replayTrace = (
  label: string,
  target: SubscriberReplayService,
  trace: FixtureSubscriberTrace
): Effect.Effect<void, unknown> => {
  const ignored = new Set(subscriberTraceFixture.compare.ignore)
  const states = new Map<number, ConsumerState>()
  const overrides = label === "live" ? subscriberLiveOverrides.traces[trace.name] : undefined
  const program = Effect.gen(function*() {
    for (let index = 0; index < trace.steps.length; index++) {
      const step = trace.steps[index]
      if (step === undefined) continue
      const override = overrides?.steps[String(index)]
      if (override !== undefined) {
        expect(override.finding, `${trace.name} step ${index}: finding link`).toBe(findingsDocument)
      }
      const context = override === undefined
        ? `${trace.name} step ${index} (${step.label})`
        : `${trace.name} step ${index} (${step.label}); recorded in ${override.finding}`
      switch (step.label) {
        case "op": {
          const actual = yield* observeStep(target.service, step.op)
          assertExpected(actual, override?.outcome ?? step.expect, ignored, context)
          yield* checkCounts(target, step.counts, context, override)
          break
        }
        case "register": {
          const state = yield* startConsumer(target.service, step)
          states.set(step.id, state)
          const expectedOutcome = override?.outcome ?? step.expect
          if ("error" in expectedOutcome) {
            yield* Fiber.await(state.fiber)
            const failed = state.observed.find((event) =>
              typeof event === "object" && event !== null && "_tag" in event
              && event._tag === "Failed"
            ) as { readonly error?: unknown } | undefined
            assertExpected(
              failed === undefined ? { error: state.unexpected } : { error: failed.error },
              expectedOutcome,
              ignored,
              context
            )
          } else {
            const expectedEvents = override?.events ?? step.events
            yield* waitForLength(state, expectedEvents.length)
            expect(state.unexpected, `${context}: unexpected consumer failure`).toBeUndefined()
            assertExpected(
              state.observed,
              expectedEvents,
              ignored,
              `${context}: registration events`
            )
            assertExpected(
              { ok: { _tag: "unit" } },
              expectedOutcome,
              ignored,
              context
            )
            yield* armGate(state, context)
          }
          yield* checkCounts(target, step.counts, context, override)
          break
        }
        case "pull": {
          const state = states.get(step.id)
          expect(state, `${context}: registered subscriber ${step.id}`).toBeDefined()
          if (state === undefined) break
          const gate = state.gate
          expect(gate, `${context}: pull has one armed gate`).toBeDefined()
          if (gate === undefined) break
          if (target.settleBeforePull !== undefined) yield* target.settleBeforePull
          state.gate = undefined
          const before = state.observed.length
          const expectedEvents = override?.events ?? step.events
          const targetLength = before + expectedEvents.length
          expect(yield* Deferred.succeed(gate.release, void 0), `${context}: release once`).toBe(
            true
          )
          yield* waitForLength(state, targetLength)
          expect(state.unexpected, `${context}: unexpected consumer failure`).toBeUndefined()
          expect(state.observed.length, `${context}: exact event growth`).toBe(targetLength)
          assertExpected(
            state.observed.slice(before),
            expectedEvents,
            ignored,
            `${context}: pulled events`
          )
          const terminal = expectedEvents.some((event) => event._tag === "Failed")
          if (terminal) {
            yield* Fiber.await(state.fiber)
            expect(state.fiber.pollUnsafe(), `${context}: terminal fiber exit`).toBeDefined()
          } else {
            yield* armGate(state, context)
          }
          yield* checkCounts(target, step.counts, context, override)
          break
        }
        case "unsubscribe": {
          const state = states.get(step.id)
          expect(state, `${context}: registered subscriber ${step.id}`).toBeDefined()
          if (state !== undefined) {
            yield* Fiber.interrupt(state.fiber)
            expect(state.fiber.pollUnsafe(), `${context}: interrupted fiber exit`).toBeDefined()
          }
          yield* checkCounts(target, step.counts, context, override)
          break
        }
      }
    }

    // SubTraces.checkFinal is a selective assertion list: compare every id the
    // fixture names, without inventing assertions for registered ids it omits.
    const actualFinal = trace.finalObserved.map(({ id }) => ({
      id,
      events: states.get(id)?.observed ?? []
    }))
    assertExpected(actualFinal, trace.finalObserved, ignored, `${trace.name}: finalObserved`)
  })
  return program.pipe(Effect.ensuring(Effect.suspend(() => cleanup(states.values()))))
}

/** Replay the kernel-checked schema-2 label protocol through one interpreter. */
export const leanSubscriberReplay = (
  label: string,
  withService: WithSubscriberService,
  timeout = 20_000
): void => {
  for (const trace of subscriberTraceFixture.traces) {
    it.effect(
      `${label}: lean subscriber trace ${trace.name} (mirrors ${trace.mirrors.join(", ")})`,
      () => withService((target) => replayTrace(label, target, trace)),
      timeout
    )
  }
}

/** Memory-only witness that one label release corresponds to one queue chunk pull. */
export const leanSubscriberGateSelfCheck = (
  withService: WithSubscriberService,
  timeout = 20_000
): void => {
  it.effect(
    "memory: Lean subscriber replay gate releases exactly one queued chunk",
    () =>
      withService((target) =>
        Effect.gen(function*() {
          yield* target.service.createStream({
            name: "GATE_SELF_CHECK",
            subjects: ["gate.>"],
            maxMessagesPerSubject: 0,
            allowRollup: true
          })
          const step: typeof RegisterStep.Type = {
            label: "register",
            id: 0,
            stream: "GATE_SELF_CHECK",
            options: {
              filters: ["gate.>"],
              start: { _tag: "NewOnly" },
              buffer: { _tag: "TerminateOnLag", messages: 2 }
            },
            lastEnqueued: "0",
            expect: { ok: { _tag: "unit" } },
            events: [{ _tag: "CaughtUp" }],
            counts: []
          }
          const state = yield* startConsumer(target.service, step)
          const body = Effect.gen(function*() {
            yield* waitForLength(state, 1)
            assertExpected(state.observed, step.events, new Set(), "gate self-check registration")
            yield* armGate(state, "gate self-check registration")
            const chunksBefore = state.chunks.length
            yield* target.service.publish("GATE_SELF_CHECK", "gate.k", utf8.encode("v1"))
            yield* target.service.publish("GATE_SELF_CHECK", "gate.k", utf8.encode("v2"))
            for (let i = 0; i < 8; i++) yield* Effect.yieldNow
            expect(
              state.observed.map((event) =>
                typeof event === "object" && event !== null && "_tag" in event ? event._tag : event
              )
            ).toEqual(["CaughtUp"])

            const gate = state.gate
            expect(gate).toBeDefined()
            if (gate === undefined) return
            state.gate = undefined
            expect(yield* Deferred.succeed(gate.release, void 0)).toBe(true)
            yield* waitForLength(state, 3)
            expect(state.chunks.slice(chunksBefore)).toHaveLength(1)
            expect(
              state.chunks.at(-1)?.map((event) =>
                typeof event === "object" && event !== null && "_tag" in event
                  ? event._tag === "Entry" && "message" in event
                    ? decoder.decode(
                      (event.message as { readonly payload: Uint8Array }).payload
                    )
                    : event._tag
                  : event
              )
            ).toEqual(["v1", "v2"])
            yield* armGate(state, "gate self-check after release")
            for (let i = 0; i < 8; i++) yield* Effect.yieldNow
            expect(state.observed).toHaveLength(3)
          })
          yield* body.pipe(Effect.ensuring(Fiber.interrupt(state.fiber)))
        })
      ),
    timeout
  )
}
