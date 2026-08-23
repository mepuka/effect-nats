import { expect, it } from "@effect/vitest"
import { Effect, Exit, Fiber, Queue, Result, Stream } from "effect"
import { FastCheck } from "effect/testing"
import {
  type FixtureSubscriberStep,
  subscriberFreeRunningOverride,
  type SubscriberReplayService,
  subscriberTraceFixture,
  type WithSubscriberService
} from "./LeanSubscriberReplay.ts"
import { normalize, observeStep } from "./LeanTraceReplay.ts"

const runsPerTrace = 25
const stableYields = 16
const baseSeed = 731_000
const yieldCountArbitrary = FastCheck.integer({ min: 0, max: 3 })

type RegisterStep = Extract<FixtureSubscriberStep, { readonly label: "register" }>

interface FreeConsumerState {
  readonly id: number
  readonly observed: Array<unknown>
  readonly chunks: Array<ReadonlyArray<unknown>>
  readonly signals: Queue.Queue<void>
  readonly fiber: Fiber.Fiber<void, unknown>
  exit: Exit.Exit<void, unknown> | undefined
  unexpected: unknown | undefined
  interruptedByLabel: boolean
}

interface RunObservation {
  readonly id: number
  readonly history: ReadonlyArray<ReadonlyArray<unknown>>
  readonly allowed: ReadonlyArray<ReadonlyArray<ReadonlyArray<unknown>>>
  readonly terminalAllowed: ReadonlyArray<ReadonlyArray<ReadonlyArray<unknown>>>
  readonly terminal: boolean
}

export const freeRunningSeeds = (traceIndex: number): ReadonlyArray<number> =>
  Array.from({ length: runsPerTrace }, (_, run) => baseSeed + traceIndex * 100 + run)

const optionsOf = (options: RegisterStep["options"]) => ({
  filters: options.filters,
  start: options.start._tag === "FromSequence" || options.start._tag === "AfterSequence"
    ? { ...options.start, sequence: BigInt(options.start.sequence) }
    : options.start,
  buffer: options.buffer
})

const isRecordedFailure = (error: unknown): boolean =>
  typeof error === "object"
  && error !== null
  && "_tag" in error
  && (error._tag === "ConsumerLagged" || error._tag === "StreamNotFound")

const startConsumer = (
  target: SubscriberReplayService,
  step: RegisterStep
): Effect.Effect<FreeConsumerState> =>
  Effect.gen(function*() {
    const observed: Array<unknown> = []
    const chunks: Array<ReadonlyArray<unknown>> = []
    const signals = yield* Queue.unbounded<void>()
    let exit: Exit.Exit<void, unknown> | undefined
    let unexpected: unknown | undefined
    const run = Stream.runForEachArray(
      target.service.consume(step.stream, optionsOf(step.options)),
      (chunk) =>
        Effect.gen(function*() {
          const recorded = Array.from(chunk)
          observed.push(...recorded)
          chunks.push(recorded)
          yield* Queue.offer(signals, void 0)
        })
    ).pipe(
      Effect.onExit((fiberExit) =>
        Effect.gen(function*() {
          exit = fiberExit
          if (Exit.isFailure(fiberExit)) {
            const error = Exit.findError(fiberExit)
            if (Result.isSuccess(error)) {
              if (isRecordedFailure(error.success)) {
                const failed = { _tag: "Failed", error: error.success }
                observed.push(failed)
                chunks.push([failed])
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
      id: step.id,
      observed,
      chunks,
      signals,
      fiber,
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
      },
      interruptedByLabel: false
    }
  })

const awaitRegistration = (state: FreeConsumerState, context: string): Effect.Effect<boolean> =>
  Effect.gen(function*() {
    const caughtUp = () =>
      state.observed.some((event) =>
        typeof event === "object" && event !== null && "_tag" in event
        && event._tag === "CaughtUp"
      )
    while (!caughtUp() && state.exit === undefined) yield* Queue.take(state.signals)
    expect(state.unexpected, `${context}: unexpected registration failure`).toBeUndefined()
    return caughtUp()
  })

const summarize = (event: unknown): unknown => {
  if (typeof event !== "object" || event === null || !("_tag" in event)) return event
  if (event._tag === "Entry" && "message" in event) {
    const message = event.message as { readonly sequence: bigint }
    return { _tag: "Entry", sequence: message.sequence.toString() }
  }
  if (event._tag === "CaughtUp") return { _tag: "CaughtUp" }
  if (event._tag === "Failed" && "error" in event) {
    return { _tag: "Failed", error: event.error }
  }
  return event
}

const historyOf = (state: FreeConsumerState): ReadonlyArray<ReadonlyArray<unknown>> => {
  const history: Array<ReadonlyArray<unknown>> = []
  const registration: Array<unknown> = []
  let registered = false
  for (const chunk of state.chunks) {
    if (registered) {
      history.push(chunk.map(summarize))
      continue
    }
    const caughtUpIndex = chunk.findIndex((event) =>
      typeof event === "object" && event !== null && "_tag" in event
      && event._tag === "CaughtUp"
    )
    if (caughtUpIndex === -1) {
      registration.push(...chunk)
      continue
    }
    registration.push(...chunk.slice(0, caughtUpIndex + 1))
    history.push(registration.map(summarize))
    registered = true
    const afterRegistration = chunk.slice(caughtUpIndex + 1)
    if (afterRegistration.length > 0) history.push(afterRegistration.map(summarize))
  }
  if (!registered && registration.length > 0) history.push(registration.map(summarize))
  return history
}

const canonical = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`
  if (typeof value === "object" && value !== null) {
    return `{${
      Object.entries(value).sort(([a], [b]) => a.localeCompare(b))
        .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")
    }}`
  }
  return JSON.stringify(value)
}

const waitForQuiescence = (states: ReadonlyArray<FreeConsumerState>): Effect.Effect<void> =>
  Effect.gen(function*() {
    let stable = 0
    let previous = states.map((state) => state.observed.length).join(",")
    while (
      stable < stableYields && states.some((state) => state.fiber.pollUnsafe() === undefined)
    ) {
      yield* Effect.yieldNow
      const current = states.map((state) => state.observed.length).join(",")
      if (current === previous) {
        stable++
      } else {
        stable = 0
        previous = current
      }
    }
  })

const cleanup = (states: Iterable<FreeConsumerState>): Effect.Effect<void> =>
  Effect.forEach(states, (state) => Fiber.interrupt(state.fiber), { discard: true })

const runOnce = (
  label: string,
  target: SubscriberReplayService,
  trace: typeof subscriberTraceFixture.traces[number],
  seed: number
): Effect.Effect<ReadonlyArray<RunObservation>, unknown> => {
  const ignored = new Set(subscriberTraceFixture.compare.ignore)
  const states = new Map<number, FreeConsumerState>()
  const yieldCounts = FastCheck.sample(yieldCountArbitrary, {
    seed,
    numRuns: Math.max(1, trace.steps.length - 1)
  })
  const program = Effect.gen(function*() {
    for (let index = 0; index < trace.steps.length; index++) {
      const step = trace.steps[index]
      if (step === undefined) continue
      const context = `${trace.name} seed ${seed} step ${index} (${step.label})`
      switch (step.label) {
        case "op": {
          const actual = yield* observeStep(target.service, step.op)
          expect(normalize(actual, ignored), context).toEqual(normalize(step.expect, ignored))
          break
        }
        case "register": {
          const state = yield* startConsumer(target, step)
          states.set(step.id, state)
          const reachedCaughtUp = yield* awaitRegistration(state, context)
          expect(
            reachedCaughtUp || (label === "live" && trace.name === "sa-replay-lag"),
            `${context}: ${label} registration reached CaughtUp or a recorded live divergence`
          ).toBe(true)
          break
        }
        case "pull":
          break
        case "unsubscribe": {
          const state = states.get(step.id)
          expect(state, `${context}: registered subscriber ${step.id}`).toBeDefined()
          if (state !== undefined) {
            state.interruptedByLabel = true
            yield* Fiber.interrupt(state.fiber)
          }
          break
        }
      }
      if (index < trace.steps.length - 1) {
        const count = yieldCounts[index] ?? 0
        for (let turn = 0; turn < count; turn++) yield* Effect.yieldNow
      }
    }

    const consumers = Array.from(states.values())
    yield* waitForQuiescence(consumers)
    // A natural exit or a survivor parked after 16 stable yields is quiescent.
    // A label-driven unsubscribe remains only prefix-closed and uses outcomes.
    const terminalIds = new Set(
      consumers.filter((state) => !state.interruptedByLabel).map((state) => state.id)
    )
    yield* cleanup(consumers.filter((state) => state.fiber.pollUnsafe() === undefined))

    return consumers.map((state) => {
      expect(state.unexpected, `${trace.name} seed ${seed} subscriber ${state.id}`).toBeUndefined()
      const allowed = trace.freeRunning.outcomes[String(state.id)]
      const terminalAllowed = trace.freeRunning.terminalOutcomes[String(state.id)]
      expect(allowed, `${trace.name} seed ${seed}: acceptance set for subscriber ${state.id}`)
        .toBeDefined()
      expect(
        terminalAllowed,
        `${trace.name} seed ${seed}: terminal set for subscriber ${state.id}`
      ).toBeDefined()
      return {
        id: state.id,
        history: historyOf(state),
        allowed: allowed ?? [],
        terminalAllowed: terminalAllowed ?? [],
        terminal: terminalIds.has(state.id)
      }
    })
  })
  return program.pipe(Effect.ensuring(Effect.suspend(() => cleanup(states.values()))))
}

/** Run 25 seeded ungated schedules per trace and check Lean's accepted chunk histories. */
export const leanSubscriberFreeRun = (
  label: string,
  withService: WithSubscriberService,
  timeout = 20_000
): void => {
  for (let traceIndex = 0; traceIndex < subscriberTraceFixture.traces.length; traceIndex++) {
    const trace = subscriberTraceFixture.traces[traceIndex]
    if (trace === undefined) continue
    const seeds = freeRunningSeeds(traceIndex)
    it.effect(
      `${label}: lean subscriber free-running ${trace.name} (${runsPerTrace} seeds)`,
      () =>
        Effect.gen(function*() {
          const distinct = new Map<number, Set<string>>()
          const outcomeSetSizes = new Map<number, number>()
          const terminalSetSizes = new Map<number, number>()
          const terminalChecks = new Map<number, number>()
          let terminalRunsReached = 0
          let terminalRunsPassed = 0
          const outsideOutcomes: Array<{
            readonly seed: number
            readonly id: number
            readonly history: unknown
            readonly adjudication: string
          }> = []
          for (const seed of seeds) {
            const observations = yield* withService((target) => runOnce(label, target, trace, seed))
            const runReachedTerminal = observations.length > 0
              && observations.every((observation) => observation.terminal)
            let runPassedTerminal = runReachedTerminal
            for (const observation of observations) {
              const actual = normalize(
                observation.history,
                new Set(subscriberTraceFixture.compare.ignore)
              )
              const encoded = canonical(actual)
              const expectedSet = observation.terminal
                ? observation.terminalAllowed
                : observation.allowed
              const allowed = expectedSet.map((history) =>
                canonical(normalize(history, new Set(subscriberTraceFixture.compare.ignore)))
              )
              const member = allowed.includes(encoded)
              const liveFinding = label === "live"
                ? subscriberFreeRunningOverride(trace.name, observation.id)
                : undefined
              if (!member && liveFinding !== undefined) {
                expect(
                  liveFinding.finding,
                  `${label} ${trace.name} seed ${seed}: finding link`
                ).toBe("docs/research/lean-trace-live-findings.md")
                const recorded = liveFinding.histories.map((history) =>
                  canonical(normalize(history, new Set(subscriberTraceFixture.compare.ignore)))
                )
                expect(
                  recorded.includes(encoded),
                  `${label} ${trace.name} seed ${seed}: recorded class-(a)/(c) history: ${encoded}`
                ).toBe(true)
                outsideOutcomes.push({
                  seed,
                  id: observation.id,
                  history: actual,
                  adjudication: "finding-linked-live-divergence"
                })
              } else {
                const findingClass = label === "memory" ? "class-A4" : "class-(a)/(c)"
                expect(
                  member,
                  `${label} ${trace.name} seed ${seed} subscriber ${observation.id} ${findingClass} history: ${encoded}`
                ).toBe(true)
              }
              if (observation.terminal && member) {
                terminalChecks.set(
                  observation.id,
                  (terminalChecks.get(observation.id) ?? 0) + 1
                )
              }
              if (observation.terminal && !member) runPassedTerminal = false
              const outcomes = distinct.get(observation.id) ?? new Set<string>()
              outcomes.add(encoded)
              distinct.set(observation.id, outcomes)
              outcomeSetSizes.set(observation.id, observation.allowed.length)
              terminalSetSizes.set(observation.id, observation.terminalAllowed.length)
            }
            if (runReachedTerminal) terminalRunsReached++
            if (runPassedTerminal) terminalRunsPassed++
          }
          console.info(`[lean-subscriber-free-run] ${
            JSON.stringify({
              interpreter: label,
              trace: trace.name,
              seeds,
              terminalRunsReached,
              terminalRunsPassed,
              subscribers: Array.from(distinct, ([id, outcomes]) => ({
                id,
                distinctOutcomes: outcomes.size,
                acceptanceSetSize: outcomeSetSizes.get(id) ?? 0,
                terminalSetSize: terminalSetSizes.get(id) ?? 0,
                terminalChecksPassed: terminalChecks.get(id) ?? 0
              })),
              outsideOutcomes
            })
          }`)
        }),
      timeout
    )
  }
}
