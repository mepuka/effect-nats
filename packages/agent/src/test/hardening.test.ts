import { describe, expect, test } from "bun:test"
import { Effect, Layer, Queue, Result, Stream } from "effect"
import { TestClock } from "effect/testing"
import { AgentHost, layerHost, type AgentHostConfig } from "../protocol/host.js"
import { classify } from "../protocol/prompt-engine.js"
import {
  ACK,
  TERMINATOR,
  chunkFrame,
  decodeChunk,
  errorFrame,
  wellFormed,
  type Frame,
} from "../protocol/protocol/chunks.js"
import {
  encodeRequest,
  isRequestError,
  parseRequest,
  type RequestEnvelope,
} from "../protocol/protocol/envelope.js"
import {
  literalPattern,
  matchSubject,
  type Pattern,
} from "../protocol/protocol/subjects.js"
import { Wire } from "../protocol/wire.js"
import { makeLoopbackBus, msg, type LoopbackBus } from "./kit.js"

const encoder = new TextEncoder()
const enc = (text: string): Uint8Array => encoder.encode(text)
const forkNow = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.forkChild(effect, { startImmediately: true })

const PROMPT_SUBJECT = "agents.prompt.hard.review.round2"

const hostConfig = (overrides?: Partial<AgentHostConfig>): AgentHostConfig => ({
  address: { agent: "hard", owner: "review", name: "round2" },
  instanceId: "hardening-instance",
  heartbeatIntervalS: 30,
  handler: () => Stream.make({ _tag: "Text", text: "ok" } as const),
  ...overrides,
})

const withHost = <A, E>(
  config: AgentHostConfig,
  body: (bus: LoopbackBus) => Effect.Effect<A, E, AgentHost | Wire>,
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bus = yield* makeLoopbackBus()
      const wireLayer = Layer.succeed(Wire)(bus.wire)
      const layer = Layer.mergeAll(
        layerHost(config).pipe(Layer.provide(wireLayer)),
        wireLayer,
      )
      return yield* Effect.gen(function* () {
        yield* TestClock.adjust(1)
        return yield* body(bus)
      }).pipe(Effect.provide(layer))
    }).pipe(Effect.provide(TestClock.layer())),
  )

const framesOn = (bus: LoopbackBus, subject: string): Frame[] =>
  bus.log
    .filter((entry) => entry.subject === subject)
    .map((entry) =>
      classify(
        msg(entry.payload, {
          errorCode: entry.options?.errorCode,
          errorMessage: entry.options?.errorMessage,
        }),
      ),
    )
    .filter(Result.isSuccess)
    .map((result) => result.success)

describe("request envelope hardening", () => {
  test("a UTF-8 BOM forces the plain-text discrimination branch", () => {
    const json = enc('{"prompt":"json"}')
    const payload = new Uint8Array(json.length + 3)
    payload.set([0xef, 0xbb, 0xbf])
    payload.set(json, 3)
    const parsed = parseRequest(payload)
    expect(isRequestError(parsed)).toBe(false)
    if (!isRequestError(parsed)) expect(parsed.prompt).toBe('{"prompt":"json"}')
  })

  test("escaped lone surrogates remain data and round-trip", () => {
    const parsed = parseRequest(enc('{"prompt":"\\ud800"}'))
    expect(isRequestError(parsed)).toBe(false)
    if (isRequestError(parsed)) return
    expect(parsed.prompt.charCodeAt(0)).toBe(0xd800)
    expect(parseRequest(encodeRequest(parsed))).toEqual(parsed)
  })

  test("attachment extras cannot replace filename or content", () => {
    const envelope: RequestEnvelope = {
      prompt: "go",
      attachments: [{
        filename: "real.txt",
        content: "YQ==",
        extra: {
          filename: "shadow.txt",
          content: "not-base64",
          mime: "text/plain",
        },
      }],
      extra: {},
    }
    const parsed = parseRequest(encodeRequest(envelope))
    expect(isRequestError(parsed)).toBe(false)
    if (!isRequestError(parsed)) {
      expect(parsed.attachments).toEqual([{
        filename: "real.txt",
        content: "YQ==",
        extra: { mime: "text/plain" },
      }])
    }
  })

  test("large whitespace prefixes still select JSON", () => {
    const parsed = parseRequest(enc(`${" ".repeat(256 * 1024)}{"prompt":"ok"}`))
    expect(isRequestError(parsed)).toBe(false)
    if (!isRequestError(parsed)) expect(parsed.prompt).toBe("ok")
  })

  test("non-object attachment entries are rejected without throwing", () => {
    for (const attachment of [null, false, 0, "x", []]) {
      expect(parseRequest(enc(JSON.stringify({ prompt: "p", attachments: [attachment] })))).toEqual({
        _tag: "InvalidAttachment",
        filename: "<non-object>",
      })
    }
  })
})

describe("chunk attachment hardening", () => {
  test("response and query attachments require padded standard base64", () => {
    for (const content of ["a-b_", "Y Q==", "YQ"]) {
      const response = decodeChunk({
        type: "response",
        data: { text: "x", attachments: [{ filename: "f", content }] },
      })
      const query = decodeChunk({
        type: "query",
        data: {
          id: "q",
          reply_subject: "_INBOX.q",
          prompt: "x",
          attachments: [{ filename: "f", content }],
        },
      })
      expect(Result.isFailure(response)).toBe(true)
      expect(Result.isFailure(query)).toBe(true)
    }
  })
})

describe("subject matcher hardening", () => {
  test("empty subject and literal tokens are never matchable", () => {
    const malformed: Pattern = {
      toks: [
        { _tag: "lit", value: "a" },
        { _tag: "lit", value: "" },
        { _tag: "lit", value: "b" },
      ],
      gt: false,
    }
    expect(matchSubject(["a", "", "b"], malformed)).toBe(false)
    expect(matchSubject(["a", "", "b"], literalPattern("a..b"))).toBe(false)
  })

  test("> in the middle cannot be laundered as a literal token", () => {
    expect(matchSubject(["a", ">", "b"], literalPattern("a.>.b"))).toBe(false)
  })
})

describe("reply DFA hardening", () => {
  const ack = chunkFrame(ACK)
  const response = chunkFrame({
    _tag: "Response",
    text: "x",
    attachments: [],
    extra: {},
  })
  const error = errorFrame(400, "bad")

  test("content cannot precede the initial acknowledgement", () => {
    expect(wellFormed([response, TERMINATOR])).toBe(false)
    expect(wellFormed([ack, TERMINATOR])).toBe(true)
  })

  test("illegal terminal and error sequences never recover", () => {
    expect(wellFormed([response, ack, TERMINATOR])).toBe(false)
    expect(wellFormed([ack, error, error, TERMINATOR])).toBe(false)
    expect(wellFormed([ack, TERMINATOR, TERMINATOR])).toBe(false)
    expect(wellFormed([ack, error, TERMINATOR, response])).toBe(false)
  })
})

describe("prompt classification hardening", () => {
  test("an unparseable service error code is a protocol violation", () => {
    const result = classify(msg('{"error":"bad"}', {
      errorCode: Number("not-a-code"),
      errorMessage: "bad code",
    }))
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure._tag).toBe("ProtocolViolation")
  })

  test("an error JSON body without message falls back to its header", () => {
    const result = classify(msg('{"error":"internal","detail":"x"}', {
      errorCode: 500,
      errorMessage: "header fallback",
    }))
    expect(Result.isSuccess(result)).toBe(true)
    if (Result.isSuccess(result)) {
      expect(result.success).toEqual(errorFrame(500, "header fallback"))
    }
  })
})

describe("loopback queue groups", () => {
  test("delivers once per queue group and to each ordinary subscriber", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const bus = yield* makeLoopbackBus()
          const groupA = yield* bus.wire.subscribe("events.*", { queueGroup: "workers" })
          const groupB = yield* bus.wire.subscribe("events.*", { queueGroup: "workers" })
          const otherGroup = yield* bus.wire.subscribe("events.*", { queueGroup: "audit" })
          const ordinary = yield* bus.wire.subscribe("events.*")
          const observed = yield* Queue.bounded<string>(8)

          yield* forkNow(groupA.pipe(Stream.runForEach(() => Queue.offer(observed, "workers-a"))))
          yield* forkNow(groupB.pipe(Stream.runForEach(() => Queue.offer(observed, "workers-b"))))
          yield* forkNow(otherGroup.pipe(Stream.runForEach(() => Queue.offer(observed, "audit"))))
          yield* forkNow(ordinary.pipe(Stream.runForEach(() => Queue.offer(observed, "ordinary"))))
          yield* bus.wire.publish("events.one", enc("x"))

          const labels = [
            yield* Queue.take(observed),
            yield* Queue.take(observed),
            yield* Queue.take(observed),
          ]
          expect(labels.filter((label) => label.startsWith("workers-"))).toHaveLength(1)
          expect(labels).toContain("audit")
          expect(labels).toContain("ordinary")
        }),
      ),
    )
  })
})

describe("host reply hardening", () => {
  test("a handler defect emits 500 before the sole terminator", async () => {
    await withHost(hostConfig({ handler: () => Stream.die("handler defect") }), (bus) =>
      Effect.gen(function* () {
        yield* bus.wire.publish(PROMPT_SUBJECT, enc("go"), { replyTo: "_inbox.defect" })
        yield* TestClock.adjust(10)
        const frames = framesOn(bus, "_inbox.defect")
        expect(frames).toHaveLength(3)
        expect(frames[0]).toEqual(chunkFrame(ACK))
        expect(frames[1]?._tag).toBe("ErrorFrame")
        expect(frames[1]?._tag === "ErrorFrame" && frames[1].code).toBe(500)
        expect(frames[2]).toEqual(TERMINATOR)
        expect(wellFormed(frames)).toBe(true)
      }),
    )
  })
})
