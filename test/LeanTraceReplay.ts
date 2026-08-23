import { expect, it } from "@effect/vitest"
import { Effect, Exit, Result, Schema, Stream } from "effect"
import { existsSync, readFileSync } from "node:fs"
import type { JetStream } from "../src/internal/JetStream.ts"

const utf8 = new TextEncoder()
const decoder = new TextDecoder()
const findingsDocument = "docs/research/lean-trace-live-findings.md"
const roomy = { _tag: "TerminateOnLag", messages: 4096 } as const

const Headers = Schema.Array(Schema.Tuple([Schema.String, Schema.String]))
const Config = Schema.Struct({
  name: Schema.String,
  subjects: Schema.Array(Schema.String),
  maxMessagesPerSubject: Schema.Number,
  allowRollup: Schema.Boolean
})
const Message = Schema.Struct({
  subject: Schema.String,
  sequence: Schema.String,
  payload: Schema.String,
  headers: Headers,
  timestampMillis: Schema.Number
})

const Op = Schema.Union([
  Schema.Struct({ op: Schema.Literal("createStream"), config: Config }),
  Schema.Struct({ op: Schema.Literal("getStream"), name: Schema.String }),
  Schema.Struct({ op: Schema.Literal("deleteStream"), name: Schema.String }),
  Schema.Struct({
    op: Schema.Literal("publish"),
    stream: Schema.String,
    subject: Schema.String,
    payload: Schema.String,
    headers: Headers,
    expectedLastSubjectSequence: Schema.NullOr(Schema.String),
    timestampMillis: Schema.Number
  }),
  Schema.Struct({
    op: Schema.Literal("lastMessageForSubject"),
    stream: Schema.String,
    subject: Schema.String
  })
])

const Ok = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("unit") }),
  Schema.Struct({ _tag: Schema.Literal("config"), config: Config }),
  Schema.Struct({ _tag: Schema.Literal("sequence"), sequence: Schema.String }),
  Schema.Struct({ _tag: Schema.Literal("message"), message: Message })
])
const ExpectedError = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("InvalidConfig"),
    name: Schema.String,
    value: Schema.Number
  }),
  Schema.Struct({ _tag: Schema.Literal("StreamNotFound"), stream: Schema.String }),
  Schema.Struct({ _tag: Schema.Literal("StreamConfigConflict"), stream: Schema.String }),
  Schema.Struct({
    _tag: Schema.Literal("SubjectNotBound"),
    stream: Schema.String,
    subject: Schema.String
  }),
  Schema.Struct({
    _tag: Schema.Literal("WrongLastSequence"),
    stream: Schema.String,
    subject: Schema.String,
    expected: Schema.String,
    actual: Schema.String
  }),
  Schema.Struct({ _tag: Schema.Literal("RollupNotPermitted"), stream: Schema.String }),
  Schema.Struct({
    _tag: Schema.Literal("NoMessageForSubject"),
    stream: Schema.String,
    subject: Schema.String
  })
])
const ExpectedOutcome = Schema.Union([
  Schema.Struct({ ok: Ok }),
  Schema.Struct({ error: ExpectedError })
])
const Trace = Schema.Struct({
  name: Schema.String,
  mirrors: Schema.Array(Schema.String),
  steps: Schema.Array(Schema.Struct({ op: Op, expect: ExpectedOutcome })),
  modelOnlySteps: Schema.Number,
  views: Schema.Array(Schema.Struct({
    stream: Schema.String,
    subject: Schema.NullOr(Schema.String),
    payloads: Schema.Array(Schema.String)
  }))
})
const TraceFixture = Schema.Struct({
  schema: Schema.Literal(1),
  producer: Schema.String,
  snapshot: Schema.String,
  pin: Schema.String,
  foldableCommit: Schema.NullOr(Schema.String),
  compare: Schema.Struct({ ignore: Schema.Array(Schema.String) }),
  traces: Schema.Array(Trace)
})

const StepOverride = Schema.Struct({
  finding: Schema.String,
  outcome: ExpectedOutcome
})
const ViewOverride = Schema.Struct({
  finding: Schema.String,
  payloads: Schema.Array(Schema.String)
})
const TraceOverrides = Schema.Struct({
  steps: Schema.Record(Schema.String, StepOverride),
  views: Schema.Record(Schema.String, ViewOverride)
})
const LiveOverrides = Schema.Struct({
  schema: Schema.Literal(1),
  traces: Schema.Record(Schema.String, TraceOverrides)
})

type Fixture = typeof TraceFixture.Type
type FixtureOp = typeof Op.Type
type Overrides = typeof LiveOverrides.Type

const decodeJson = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  url: URL
): S["Type"] => {
  const input: unknown = JSON.parse(readFileSync(url, "utf8"))
  return Result.getOrThrow(Schema.decodeUnknownResult(schema)(input))
}

const fixtureUrl = new URL("./fixtures/lean/sequential-traces.json", import.meta.url)
const overridesUrl = new URL("./fixtures/lean/live-overrides.json", import.meta.url)
const fixture: Fixture = decodeJson(TraceFixture, fixtureUrl)
const liveOverrides: Overrides = existsSync(overridesUrl)
  ? decodeJson(LiveOverrides, overridesUrl)
  : { schema: 1, traces: {} }

const normalize = (value: unknown, ignored: ReadonlySet<string>): unknown => {
  if (typeof value === "bigint") return value.toString()
  if (value instanceof Uint8Array) return decoder.decode(value)
  if (value instanceof Map) {
    return Array.from(value, ([key, item]) => [normalize(key, ignored), normalize(item, ignored)])
  }
  if (Array.isArray(value)) return value.map((item) => normalize(item, ignored))
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !ignored.has(key))
        .map(([key, item]) => [key, normalize(item, ignored)])
    )
  }
  return value
}

const observe = <A, E>(
  effect: Effect.Effect<A, E>,
  success: (value: A) => unknown
): Effect.Effect<unknown> =>
  Effect.map(Effect.exit(effect), (exit) =>
    Exit.isSuccess(exit)
      ? { ok: success(exit.value) }
      : { error: Result.getOrThrow(Exit.findError(exit)) })

const observeStep = (
  service: JetStream["Service"],
  op: FixtureOp
): Effect.Effect<unknown> => {
  switch (op.op) {
    case "createStream":
      return observe(service.createStream(op.config), () => ({ _tag: "unit" }))
    case "getStream":
      return observe(service.getStream(op.name), (config) => ({ _tag: "config", config }))
    case "deleteStream":
      return observe(service.deleteStream(op.name), () => ({ _tag: "unit" }))
    case "publish":
      return observe(
        service.publish(op.stream, op.subject, utf8.encode(op.payload), {
          headers: new Map(op.headers),
          ...(op.expectedLastSubjectSequence === null
            ? {}
            : { expectedLastSubjectSequence: BigInt(op.expectedLastSubjectSequence) })
        }),
        (sequence) => ({ _tag: "sequence", sequence })
      )
    case "lastMessageForSubject":
      return observe(
        service.lastMessageForSubject(op.stream, op.subject),
        (message) => ({ _tag: "message", message })
      )
  }
}

const viewPayloads = (
  service: JetStream["Service"],
  streamSubjects: ReadonlyMap<string, ReadonlyArray<string>>,
  stream: string,
  subject: string | null
): Effect.Effect<ReadonlyArray<string>, unknown> => {
  const filters = subject === null ? streamSubjects.get(stream) : [subject]
  if (filters === undefined) {
    return Effect.die(new Error(`No successful createStream recorded for ${stream}`))
  }
  return service.consume(stream, {
    filters,
    start: { _tag: "AllHistory" },
    buffer: roomy
  }).pipe(
    Stream.takeWhile((event) => event._tag !== "CaughtUp"),
    Stream.runCollect,
    Effect.map((events) =>
      events.map((event) => event._tag === "Entry" ? decoder.decode(event.message.payload) : "")
    )
  )
}

export type WithService = <A, E>(
  f: (service: JetStream["Service"]) => Effect.Effect<A, E>
) => Effect.Effect<A, unknown>

/** Replay the kernel-checked sequential fixture through an interpreter's approved service seam. */
export const leanTraceReplay = (
  label: string,
  withService: WithService,
  timeout = 20_000
): void => {
  const ignored = new Set(fixture.compare.ignore)

  for (const trace of fixture.traces) {
    it.effect(
      `${label}: lean trace ${trace.name} (mirrors ${trace.mirrors.join(", ")})`,
      () =>
        withService((service) =>
          Effect.gen(function*() {
            const streamSubjects = new Map<string, ReadonlyArray<string>>()
            const overrides = label === "live" ? liveOverrides.traces[trace.name] : undefined

            for (let index = 0; index < trace.steps.length; index++) {
              const step = trace.steps[index]
              if (step === undefined) continue
              const actual = yield* observeStep(service, step.op)
              const override = overrides?.steps[String(index)]
              if (override !== undefined) {
                expect(override.finding, `${trace.name} step ${index} finding link`)
                  .toBe(findingsDocument)
              }
              const expected = override?.outcome ?? step.expect
              const context = override === undefined
                ? `${trace.name} step ${index}`
                : `${trace.name} step ${index}; recorded in ${override.finding}`
              expect(normalize(actual, ignored), context).toEqual(normalize(expected, ignored))

              if (step.op.op === "createStream" && "ok" in expected) {
                streamSubjects.set(step.op.config.name, step.op.config.subjects)
              }
            }

            for (let index = 0; index < trace.views.length; index++) {
              const view = trace.views[index]
              if (view === undefined) continue
              const actual = yield* viewPayloads(
                service,
                streamSubjects,
                view.stream,
                view.subject
              )
              const override = overrides?.views[String(index)]
              if (override !== undefined) {
                expect(override.finding, `${trace.name} view ${index} finding link`)
                  .toBe(findingsDocument)
              }
              const expected = override?.payloads ?? view.payloads
              const context = override === undefined
                ? `${trace.name} view ${index}`
                : `${trace.name} view ${index}; recorded in ${override.finding}`
              expect(actual, context).toEqual(expected)
            }
          })
        ),
      timeout
    )
  }
}
