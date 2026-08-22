/**
 * P0 wire-contract model tests — each test cites its upstream spec section.
 * Protocol: Synadia Agent Protocol for NATS v0.3.0; local contract: docs/01-SPEC.md.
 */
import { describe, expect, test } from "bun:test"
import {
  HEARTBEAT_PATTERN,
  HEARTBEAT_WILDCARD,
  heartbeatSubject,
  hostDefaultSubject,
  isConventionalToken,
  isToken,
  literalPattern,
  matchSubject,
  parseAgentSubject,
} from "../agents/protocol/subjects.js"
import {
  encodeRequest,
  isRequestError,
  parseRequest,
  type RequestEnvelope,
} from "../agents/protocol/envelope.js"
import {
  ACK,
  ERROR_CODES,
  TERMINATOR,
  chunkFrame,
  decodeChunk,
  errorFrame,
  wellFormed,
} from "../agents/protocol/chunks.js"
import { parseMaxPayload, validateLocal } from "../agents/protocol/validation.js"
import { Result } from "effect"

const enc = (s: string): Uint8Array => new TextEncoder().encode(s)

// -- §2 subject hierarchy -----------------------------------------------------

describe("subjects (Protocol §2)", () => {
  test("parses verb-first identity tree", () => {
    expect(parseAgentSubject("agents.prompt.claude-code.aconnolly.synadia-com-2")).toEqual({
      verb: "prompt",
      agent: "claude-code",
      owner: "aconnolly",
      name: "synadia-com-2",
    })
    expect(parseAgentSubject("agents.hb.cc.aconnolly.x")?.verb).toBe("hb")
  })

  test("rejects wrong shape and unknown verbs", () => {
    expect(parseAgentSubject("agents.prompt.only-three")).toBeNull()
    expect(parseAgentSubject("agents.frobnicate.a.o.n")).toBeNull()
    expect(parseAgentSubject("x.agents.a.o.n")).toBeNull()
  })

  test("token rules: $ prefix, whitespace, control chars forbidden (§2.2)", () => {
    expect(isToken("$srv")).toBe(false)
    expect(isToken("claude-code_2")).toBe(true)
    expect(isToken("a b")).toBe(false)
    expect(isToken("a\tb")).toBe(false)
    expect(isToken("a\nb")).toBe(false)
    expect(isToken("")).toBe(false)
    expect(isToken("*")).toBe(false)
    expect(isToken(">")).toBe(false)
    expect(isToken("a.b")).toBe(false)
    expect(isConventionalToken("ClaudeCode")).toBe(false) // SHOULD charset
  })

  test("validated construction rejects malformed identity tokens", () => {
    expect(heartbeatSubject("cc", "o", "n")).toBe("agents.hb.cc.o.n")
    expect(heartbeatSubject("c c", "o", "n")).toBeNull()
    expect(hostDefaultSubject("prompt", "cc", "$o", "n")).toBeNull()
    expect(hostDefaultSubject("prompt", "cc", "o", "a.b")).toBeNull()
  })

  test("> matches one-or-more past the pattern", () => {
    const p = { toks: [{ _tag: "lit" as const, value: "agents" }], gt: true }
    expect(matchSubject(["agents", "prompt", "cc", "o", "n"], p)).toBe(true)
    expect(matchSubject(["agents"], p)).toBe(false)
    expect(matchSubject([], p)).toBe(false)
  })

  test("heartbeat wildcard partitions from prompts (§2.4)", () => {
    expect(matchSubject(heartbeatSubject("cc", "o", "n")!.split("."), HEARTBEAT_PATTERN)).toBe(true)
    expect(
      matchSubject(hostDefaultSubject("prompt", "cc", "o", "n")!.split("."), HEARTBEAT_PATTERN),
    ).toBe(false)
    expect(HEARTBEAT_WILDCARD).toBe("agents.hb.*.*.*")
  })

  test("exact literal patterns are singletons", () => {
    const p = literalPattern("agents.prompt.cc.o.n")
    expect(matchSubject(["agents", "prompt", "cc", "o", "n"], p)).toBe(true)
    expect(matchSubject(["agents", "prompt", "cc", "o", "m"], p)).toBe(false)
    expect(matchSubject(["agents", "prompt", "cc", "o", "n", "x"], p)).toBe(false)
    expect(matchSubject(["agents", "prompt", "cc", "o"], p)).toBe(false)
  })
})

// -- §5 request envelope --------------------------------------------------------

describe("request envelope (Protocol §5)", () => {
  test("B.1 plain-text shorthand promotes to prompt", () => {
    const r = parseRequest(enc("summarize the attached report"))
    expect(r).toEqual({ prompt: "summarize the attached report", attachments: [], extra: {} })
  })

  test("§5.3 plain text is promoted VERBATIM — no trimming", () => {
    const r = parseRequest(enc("  keep my whitespace  \n"))
    expect(!isRequestError(r) && r.prompt).toBe("  keep my whitespace  \n")
  })

  test("B.3 JSON envelope with attachment parses", () => {
    const r = parseRequest(
      enc(
        JSON.stringify({ prompt: "go", attachments: [{ filename: "a.txt", content: "aGVsbG8=" }] }),
      ),
    )
    expect(isRequestError(r)).toBe(false)
    if (!isRequestError(r)) expect(r.attachments[0]?.filename).toBe("a.txt")
  })

  test("§5.3 leading whitespace then { still discriminates as JSON", () => {
    const r = parseRequest(enc('  \n\t {"prompt":"hi"}'))
    expect(isRequestError(r)).toBe(false)
  })

  test("§5.1 zero-byte payload invalid", () => {
    expect(parseRequest(new Uint8Array(0))).toEqual({ _tag: "EmptyPayload" })
  })

  test("blank prompts map to EmptyPrompt in both encodings", () => {
    expect(parseRequest(enc("   \n\t "))).toEqual({ _tag: "EmptyPrompt" })
    expect(parseRequest(enc('{"prompt":""}'))).toEqual({ _tag: "EmptyPrompt" })
    expect(parseRequest(enc('{"prompt":"  "}'))).toEqual({ _tag: "EmptyPrompt" })
  })

  test("§5.3 malformed JSON after { → MalformedJson", () => {
    expect(parseRequest(enc("{nope"))).toEqual({ _tag: "MalformedJson" })
  })

  test("§5.3 object without prompt string → MissingPrompt", () => {
    expect(parseRequest(enc('{"other":1}'))).toEqual({ _tag: "MissingPrompt" })
  })

  test("§5.2 non-standard base64 rejected", () => {
    const r = parseRequest(
      enc(JSON.stringify({ prompt: "p", attachments: [{ filename: "f", content: "a-b_c" }] })),
    )
    expect(r).toEqual({ _tag: "InvalidAttachment", filename: "f" })
  })

  test("§5.6 unknown top-level fields preserved", () => {
    const r = parseRequest(enc(JSON.stringify({ prompt: "p", trace_id: "abc" })))
    expect(!isRequestError(r) && r.extra["trace_id"]).toBe("abc")
  })

  test("§5.6 unknown fields INSIDE attachments preserved", () => {
    const r = parseRequest(
      enc(
        JSON.stringify({
          prompt: "p",
          attachments: [{ filename: "f", content: "YQ==", mime: "text/plain" }],
        }),
      ),
    )
    expect(!isRequestError(r) && r.attachments[0]?.extra["mime"]).toBe("text/plain")
  })

  test("encodeRequest ∘ parseRequest is identity on well-formed envelopes", () => {
    const envelope: RequestEnvelope = {
      prompt: "relay me",
      attachments: [{ filename: "f", content: "YQ==", extra: { mime: "text/plain" } }],
      extra: { trace_id: "t-1" },
    }
    expect(parseRequest(encodeRequest(envelope))).toEqual(envelope)
  })
})

// -- §5.4 local validation ------------------------------------------------------

describe("local validation (Protocol §5.4)", () => {
  const env: RequestEnvelope = {
    prompt: "x",
    attachments: [{ filename: "f", content: "YQ==", extra: {} }],
    extra: {},
  }
  const plainEnv: RequestEnvelope = { prompt: "x", attachments: [], extra: {} }

  test("attachments when attachments_ok=false fails locally", () => {
    expect(
      validateLocal(env, 100, { maxPayload: 1024, brokerMaxPayload: 1024, attachmentsOk: false }),
    ).toEqual({ _tag: "AttachmentsNotSupported" })
  })

  test("oversize fails with actual > limit", () => {
    expect(
      validateLocal(plainEnv, 2048, { maxPayload: 1024, brokerMaxPayload: 4096, attachmentsOk: true }),
    ).toEqual({ _tag: "PayloadTooLarge", actual: 2048, limit: 1024 })
  })

  test("smaller broker cap binds over advertised limit", () => {
    expect(
      validateLocal(plainEnv, 2048, { maxPayload: 4096, brokerMaxPayload: 1024, attachmentsOk: true }),
    ).toEqual({ _tag: "PayloadTooLarge", actual: 2048, limit: 1024 })
  })

  test("max_payload format (§2.1)", () => {
    expect(parseMaxPayload("512KB")).toBe(512 * 1024)
    expect(parseMaxPayload("1MB")).toBe(1024 * 1024)
    expect(parseMaxPayload("1TB")).toBeNull()
    expect(parseMaxPayload("-5MB")).toBeNull()
  })
})

// -- §6 streaming model ---------------------------------------------------------

describe("stream model (Protocol §6, §9)", () => {
  const response = chunkFrame({ _tag: "Response", text: "hi", attachments: [], extra: {} })
  const ack = chunkFrame(ACK)

  test("accepted shape: ack · chunk* · terminator", () => {
    expect(wellFormed([ack, TERMINATOR])).toBe(true)
    expect(wellFormed([ack, response, response, TERMINATOR])).toBe(true)
  })

  test("§9.3 error after acceptance: ack · chunk* · error · terminator", () => {
    expect(wellFormed([ack, response, errorFrame(500, "boom"), TERMINATOR])).toBe(true)
  })

  test("§6.4 rejection before acceptance: error · terminator, no ack", () => {
    expect(wellFormed([errorFrame(400, "bad"), TERMINATOR])).toBe(true)
  })

  test("violations: no ack first, repeated ack, empty, post-terminator writes", () => {
    expect(wellFormed([])).toBe(false)
    expect(wellFormed([response, TERMINATOR])).toBe(false)
    expect(wellFormed([ack, ack, TERMINATOR])).toBe(false)
    expect(wellFormed([ack])).toBe(false)
    expect(wellFormed([TERMINATOR])).toBe(false)
    expect(wellFormed([errorFrame(500, "x")])).toBe(false)
    expect(wellFormed([ack, TERMINATOR, response])).toBe(false)
    expect(wellFormed([ack, errorFrame(500, "x"), response, TERMINATOR])).toBe(false)
  })

  test("§6.6 unknown chunk types decode as Unknown, not errors", () => {
    const r = decodeChunk({ type: "telemetry", data: { x: 1 } })
    expect(Result.isSuccess(r) && r.success._tag === "Unknown").toBe(true)
  })

  test("known chunk type violating its schema is a decode failure", () => {
    const r = decodeChunk({ type: "response", data: 42 })
    expect(Result.isFailure(r)).toBe(true)
  })

  test("§5.6 attachment extras survive chunk decode", () => {
    const r = decodeChunk({
      type: "response",
      data: { text: "t", attachments: [{ filename: "f", content: "YQ==", mime: "x/y" }] },
    })
    expect(
      Result.isSuccess(r) &&
        r.success._tag === "Response" &&
        r.success.attachments[0]?.extra["mime"],
    ).toBe("x/y")
  })

  test("error taxonomy covers Protocol §9.2 codes", () => {
    expect(ERROR_CODES.badRequest).toBe(400)
    expect(ERROR_CODES.rateLimited).toBe(429)
    expect(ERROR_CODES.internal).toBe(500)
  })
})
