import { expect, it } from "@effect/vitest"
import { Effect, Exit, Result, Stream } from "effect"
import { FastCheck } from "effect/testing"
import type { JetStream, StoredMessage } from "../src/internal/JetStream.ts"
import * as Memory from "../src/internal/JetStreamMemory.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()
const roomy = { _tag: "TerminateOnLag", messages: 4096 } as const
const limits = [0, 1, 2, 3] as const
const casModes = ["none", "zero", "last", "stale"] as const

const configArbitrary = FastCheck.record({
  limit: FastCheck.constantFrom(...limits),
  allowRollup: FastCheck.boolean()
})
const operationArbitrary = FastCheck.record({
  streamIndex: FastCheck.integer({ min: 0, max: 1 }),
  subjectIndex: FastCheck.integer({ min: 0, max: 2 }),
  payloadToken: FastCheck.integer({ min: 0, max: 99 }),
  rollup: FastCheck.boolean(),
  cas: FastCheck.constantFrom(...casModes)
})
const scenarioArbitrary = FastCheck.record({
  streamCount: FastCheck.integer({ min: 1, max: 2 }),
  configs: FastCheck.tuple(configArbitrary, configArbitrary),
  operations: FastCheck.array(operationArbitrary, { minLength: 1, maxLength: 20 })
})

interface ViewEntry {
  readonly payload: string
  readonly sequence: bigint
  readonly headers: ReadonlyArray<readonly [string, string]>
}

const streamName = (index: number): string => `TRACE_${index}`
const streamPattern = (index: number): string => `trace.s${index}.>`
const subjectName = (streamIndex: number, subjectIndex: number): string =>
  `trace.s${streamIndex}.k${subjectIndex}`

const viewEntry = (message: StoredMessage): ViewEntry => ({
  payload: decoder.decode(message.payload),
  sequence: message.sequence,
  headers: Array.from(message.headers)
})

const observe = (
  service: JetStream["Service"],
  stream: string,
  filters: ReadonlyArray<string>
): Effect.Effect<ReadonlyArray<ViewEntry>, unknown> =>
  service.consume(stream, {
    filters,
    start: { _tag: "AllHistory" },
    buffer: roomy
  }).pipe(
    Stream.takeWhile((event) => event._tag !== "CaughtUp"),
    Stream.runCollect,
    Effect.map((events) =>
      events.map((event) => event._tag === "Entry" ? viewEntry(event.message) : undefined)
        .filter((entry): entry is ViewEntry => entry !== undefined)
    )
  )

const keepLatest = (
  limit: number,
  entries: ReadonlyArray<ViewEntry>
): ReadonlyArray<ViewEntry> => limit === 0 ? entries : entries.slice(-limit)

const strictlyIncreasing = (entries: ReadonlyArray<ViewEntry>): boolean =>
  entries.every((entry, index) => {
    const previous = entries[index - 1]
    return previous === undefined || previous.sequence < entry.sequence
  })

it.effect.prop(
  "memory satisfies the Lean T2/T4/T5/T6 publish and view equations",
  { scenario: scenarioArbitrary },
  ({ scenario }) =>
    Effect.gen(function*() {
      const { service } = yield* Memory.make
      for (let index = 0; index < scenario.streamCount; index++) {
        const config = scenario.configs[index]
        if (config === undefined) continue
        yield* service.createStream({
          name: streamName(index),
          subjects: [streamPattern(index)],
          maxMessagesPerSubject: config.limit,
          allowRollup: config.allowRollup
        })
      }

      for (let stepIndex = 0; stepIndex < scenario.operations.length; stepIndex++) {
        const operation = scenario.operations[stepIndex]
        if (operation === undefined) continue
        const index = operation.streamIndex % scenario.streamCount
        const config = scenario.configs[index]
        if (config === undefined) continue
        const stream = streamName(index)
        const subject = subjectName(index, operation.subjectIndex)
        const subjects = [0, 1, 2].map((subjectIndex) => subjectName(index, subjectIndex))
        const before = new Map<string, ReadonlyArray<ViewEntry>>()
        for (const candidate of subjects) {
          before.set(candidate, yield* observe(service, stream, [candidate]))
        }
        const prior = before.get(subject) ?? []
        const lastSequence = prior.at(-1)?.sequence ?? 0n
        const expectedSequence = operation.cas === "none"
          ? undefined
          : operation.cas === "zero"
          ? 0n
          : operation.cas === "last"
          ? lastSequence
          : lastSequence === 0n
          ? 1n
          : lastSequence - 1n
        const headers = operation.rollup
          ? new Map<string, string>([["Nats-Rollup", "sub"]])
          : new Map<string, string>()

        const exit = yield* service.publish(
          stream,
          subject,
          utf8.encode(`p${stepIndex}-${operation.payloadToken}`),
          {
            headers,
            ...(expectedSequence === undefined
              ? {}
              : { expectedLastSubjectSequence: expectedSequence })
          }
        ).pipe(Effect.exit)

        const rollupAdmissible = !operation.rollup || config.allowRollup
        const casAdmissible = expectedSequence === undefined || expectedSequence === lastSequence
        const shouldSucceed = rollupAdmissible && casAdmissible
        expect(Exit.isSuccess(exit), `step ${stepIndex} success characterization`).toBe(
          shouldSucceed
        )

        if (Exit.isSuccess(exit)) {
          const fresh: ViewEntry = {
            payload: `p${stepIndex}-${operation.payloadToken}`,
            sequence: exit.value,
            headers: Array.from(headers)
          }
          const afterPublished = yield* observe(service, stream, [subject])
          expect(afterPublished, `step ${stepIndex} published-subject view`).toEqual(
            operation.rollup ? [fresh] : keepLatest(config.limit, [...prior, fresh])
          )
          for (const other of subjects) {
            if (other === subject) continue
            expect(yield* observe(service, stream, [other]), `step ${stepIndex} other-subject view`)
              .toEqual(before.get(other))
          }
        } else {
          const failure = Result.getOrThrow(Exit.findError(exit))
          expect(failure._tag, `step ${stepIndex} typed failure`).toBe(
            rollupAdmissible ? "WrongLastSequence" : "RollupNotPermitted"
          )
          for (const candidate of subjects) {
            expect(
              yield* observe(service, stream, [candidate]),
              `step ${stepIndex} failed-state view`
            )
              .toEqual(before.get(candidate))
          }
        }
      }

      for (let index = 0; index < scenario.streamCount; index++) {
        const config = scenario.configs[index]
        if (config === undefined) continue
        const stream = streamName(index)
        const all = yield* observe(service, stream, [streamPattern(index)])
        expect(strictlyIncreasing(all), `${stream} stored sequences`).toBe(true)
        for (let subjectIndex = 0; subjectIndex < 3; subjectIndex++) {
          const subject = subjectName(index, subjectIndex)
          const view = yield* observe(service, stream, [subject])
          if (config.limit > 0) {
            expect(view.length, `${stream}/${subject} capacity`).toBeLessThanOrEqual(config.limit)
          }
        }
      }
    }),
  { fastCheck: { numRuns: 100 } }
)
