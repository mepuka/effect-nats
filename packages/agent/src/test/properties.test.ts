/**
 * FastCheck property gates for the P0 laws (docs/CODEX review conformance
 * gate items 2–3). Reproducible: every run prints its seed on failure.
 */
import { describe, test } from "bun:test"
import { FastCheck as fc } from "effect/testing"
import {
  encodeRequest,
  isRequestError,
  parseRequest,
  type RequestEnvelope,
} from "../protocol/protocol/envelope.js"
import { isToken, matchSubject, type Pattern } from "../protocol/protocol/subjects.js"
import {
  ACK,
  PUMP_START,
  chunkFrame,
  errorFrame,
  stepWellFormed,
  TERMINATOR,
  wellFormed,
  type Frame,
} from "../protocol/protocol/chunks.js"

const PARAMS = { numRuns: 300 }

describe("parser totality (§5)", () => {
  test("parseRequest never throws and always returns a tagged value", () => {
    fc.assert(
      fc.property(fc.uint8Array({ maxLength: 512 }), (bytes) => {
        const r = parseRequest(bytes)
        return isRequestError(r) ? typeof r._tag === "string" : typeof r.prompt === "string"
      }),
      PARAMS,
    )
  })

  test("plain text round-trips verbatim (no trimming)", () => {
    fc.assert(
      fc.property(
        fc
          .string({ minLength: 1, maxLength: 200 })
          .filter((s) => {
            const t = s.replace(/^[\t\n\r ]+/, "")
            return t.length > 0 && !t.startsWith("{") && !s.includes("\0")
          }),
        (text) => {
          const r = parseRequest(new TextEncoder().encode(text))
          if (isRequestError(r)) return false
          const reencoded = new TextDecoder().decode(new TextEncoder().encode(text))
          return r.prompt === reencoded
        },
      ),
      PARAMS,
    )
  })

  const jsonValue = fc.oneof(fc.string({ maxLength: 8 }), fc.boolean(), fc.integer())
  const extraRecord = fc.dictionary(
    fc.string({ minLength: 1, maxLength: 8 }).filter((k) => k !== "prompt" && k !== "attachments" && k !== "__proto__"),
    jsonValue,
    { maxKeys: 4 },
  )
  const base64 = fc
    .uint8Array({ maxLength: 24 })
    .map((b) => Buffer.from(b).toString("base64"))
  const envelopeArb: fc.Arbitrary<RequestEnvelope> = fc.record({
    prompt: fc.string({ minLength: 1, maxLength: 60 }).filter((s) => s.trim().length > 0),
    attachments: fc.array(
      fc.record({
        filename: fc.string({ minLength: 1, maxLength: 20 }),
        content: base64,
        extra: extraRecord.filter((r) => !("filename" in r) && !("content" in r)),
      }),
      { maxLength: 3 },
    ),
    extra: extraRecord,
  })

  test("§5.6 encode∘parse identity: unknown fields survive at every level", () => {
    fc.assert(
      fc.property(envelopeArb, (envelope) => {
        const r = parseRequest(encodeRequest(envelope))
        return !isRequestError(r) && Bun.deepEquals(r, envelope, true)
      }),
      PARAMS,
    )
  })
})

describe("subject matcher laws (§2)", () => {
  const token = fc.stringMatching(/^[a-z0-9_-]{1,8}$/)
  const tokens = fc.array(token, { minLength: 1, maxLength: 6 })

  test("exact literal patterns are singletons", () => {
    fc.assert(
      fc.property(tokens, tokens, (subject, other) => {
        const p: Pattern = {
          toks: subject.map((value) => ({ _tag: "lit", value })),
          gt: false,
        }
        const same = subject.length === other.length && subject.every((t, i) => t === other[i])
        return matchSubject(subject, p) && matchSubject(other, p) === same
      }),
      PARAMS,
    )
  })

  test("* arity is exactly one", () => {
    fc.assert(
      fc.property(tokens, fc.nat({ max: 5 }), (subject, starCount) => {
        const p: Pattern = {
          toks: Array.from({ length: starCount }, () => ({ _tag: "star" as const })),
          gt: false,
        }
        return matchSubject(subject, p) === (subject.length === starCount)
      }),
      PARAMS,
    )
  })

  test("terminal > requires one-or-more past the prefix", () => {
    fc.assert(
      fc.property(tokens, tokens, (prefix, rest) => {
        const p: Pattern = {
          toks: prefix.map((value) => ({ _tag: "lit", value })),
          gt: true,
        }
        return (
          matchSubject([...prefix, ...rest], p) === rest.length > 0 &&
          matchSubject(prefix, p) === false
        )
      }),
      PARAMS,
    )
  })

  test("accepted tokens never contain dot, wildcard, whitespace, or $ prefix", () => {
    fc.assert(
      fc.property(fc.string({ maxLength: 12 }), (t) => {
        if (!isToken(t)) return true
        return (
          t.length > 0 &&
          !t.startsWith("$") &&
          !t.includes(".") &&
          t !== "*" &&
          t !== ">" &&
          !/\s/.test(t)
        )
      }),
      PARAMS,
    )
  })
})

describe("reply DFA model check (§6)", () => {
  const frameArb: fc.Arbitrary<Frame> = fc.oneof(
    fc.constant(chunkFrame(ACK)),
    fc.constant(chunkFrame({ _tag: "Response", text: "x", attachments: [], extra: {} })),
    fc.constant(chunkFrame({ _tag: "Status", status: "thinking" })),
    fc.constant(errorFrame(500, "e")),
    fc.constant(TERMINATOR),
  )

  const symbolOf = (f: Frame): string =>
    f._tag === "Terminator"
      ? "T"
      : f._tag === "ErrorFrame"
        ? "E"
        : f.chunk._tag === "Status" && f.chunk.status === "ack"
          ? "A"
          : "C"

  const REFERENCE = /^(A[C]*E?T|ET)$/

  test("checker agrees with the reference regular language", () => {
    fc.assert(
      fc.property(fc.array(frameArb, { maxLength: 8 }), (frames) => {
        const word = frames.map(symbolOf).join("")
        return wellFormed(frames) === REFERENCE.test(word)
      }),
      { numRuns: 2000 },
    )
  })

  test("step function is prefix-monotone: illegal prefixes never recover", () => {
    fc.assert(
      fc.property(fc.array(frameArb, { maxLength: 8 }), (frames) => {
        let state: ReturnType<typeof stepWellFormed> = PUMP_START
        for (const f of frames) {
          state = stepWellFormed(state, f)
          if (state === null) return !wellFormed(frames)
        }
        return true
      }),
      { numRuns: 2000 },
    )
  })
})
