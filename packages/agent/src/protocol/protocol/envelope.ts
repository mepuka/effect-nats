/**
 * Request envelope — Protocol §5. Pure byte-level discrimination rule (§5.3):
 * skip whitespace, `{` ⇒ JSON envelope, otherwise plain-text shorthand.
 * Zero-byte payload is invalid. Unknown fields tolerated AND preserved (§5.6)
 * at BOTH the top level and inside each attachment object.
 *
 * Relay losslessness (§5.3): a plain-text payload is promoted VERBATIM to
 * `{"prompt": <payload>}` — no trimming. A prompt is rejected as empty only
 * when it is blank (zero-length or whitespace-only), the one explicit local
 * mapping of upstream's non-empty-prompt requirement.
 *
 * `parseRequest` is TOTAL over Uint8Array: every input maps to an envelope or
 * a typed RequestError; it never throws. `encodeRequest` is its inverse on
 * well-formed envelopes — the client publishes through the SAME contract the
 * host parses (round-trip property-tested).
 */

export interface Attachment {
  readonly filename: string
  /** RFC 4648 §4 standard base64, padded (Protocol §5.2). */
  readonly content: string
  /** §5.6: unknown fields inside the attachment object, preserved for relay. */
  readonly extra: Readonly<Record<string, unknown>>
}

export interface RequestEnvelope {
  readonly prompt: string
  readonly attachments: readonly Attachment[]
  /** §5.6: unknown top-level fields, preserved for relaying. */
  readonly extra: Readonly<Record<string, unknown>>
}

export type RequestError =
  | { readonly _tag: "EmptyPayload" }
  | { readonly _tag: "EmptyPrompt" }
  | { readonly _tag: "MalformedJson" }
  | { readonly _tag: "MalformedUtf8" }
  | { readonly _tag: "MissingPrompt" }
  | { readonly _tag: "InvalidAttachment"; readonly filename: string }
  | { readonly _tag: "NotAnObject" }

export type ParsedRequest = RequestEnvelope | RequestError

export const isRequestError = (r: ParsedRequest): r is RequestError =>
  "_tag" in r

const WHITESPACE = new Set([0x09, 0x0a, 0x0d, 0x20])
const decoder = new TextDecoder("utf-8", { fatal: true })
const encoder = new TextEncoder()

/** RFC 4648 §4 standard alphabet, padded, no whitespace (Protocol §5.2). */
export const isStandardBase64 = (s: string): boolean =>
  s.length % 4 === 0 && (s.length === 0 || /^[A-Za-z0-9+/]+={0,2}$/.test(s))

const isBlank = (s: string): boolean => {
  for (let i = 0; i < s.length; i++) {
    if (!WHITESPACE.has(s.charCodeAt(i))) return false
  }
  return true
}

export const parseRequest = (bytes: Uint8Array): ParsedRequest => {
  // §5.3: zero-byte payload is invalid
  if (bytes.length === 0) return { _tag: "EmptyPayload" }

  let start = 0
  while (start < bytes.length && WHITESPACE.has(bytes[start] ?? -1)) start++
  const isJson = bytes[start] === 0x7b // "{"

  // §5.1/§5.3: the payload is UTF-8 text; invalid encodings are a 400, not
  // silently replacement-decoded content the agent never received
  let text: string
  try {
    text = decoder.decode(bytes)
  } catch {
    return { _tag: "MalformedUtf8" }
  }

  if (!isJson) {
    // §5.3 step 3: promote plain text VERBATIM to {"prompt": <payload>}
    if (isBlank(text)) return { _tag: "EmptyPrompt" }
    return { prompt: text, attachments: [], extra: {} }
  }

  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return { _tag: "MalformedJson" }
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { _tag: "NotAnObject" }
  }
  const obj = parsed as Record<string, unknown>
  const { prompt, attachments, ...extra } = obj
  if (typeof prompt !== "string") return { _tag: "MissingPrompt" }
  if (isBlank(prompt)) return { _tag: "EmptyPrompt" }

  const atts: Attachment[] = []
  if (attachments !== undefined) {
    if (!Array.isArray(attachments)) return { _tag: "InvalidAttachment", filename: "<not-array>" }
    for (const a of attachments) {
      if (typeof a !== "object" || a === null || Array.isArray(a)) {
        return { _tag: "InvalidAttachment", filename: "<non-object>" }
      }
      const { filename, content, ...attExtra } = a as Record<string, unknown>
      if (typeof filename !== "string" || typeof content !== "string") {
        return { _tag: "InvalidAttachment", filename: String(filename ?? "<missing>") }
      }
      if (!isStandardBase64(content)) {
        return { _tag: "InvalidAttachment", filename }
      }
      atts.push({ filename, content, extra: attExtra })
    }
  }

  return { prompt, attachments: atts, extra }
}

/**
 * Encode an envelope to wire bytes through the same §5 contract the host
 * parses. Unknown fields re-enter their original positions; `prompt` and
 * `attachments` always win over identically-named extras.
 */
export const encodeRequest = (envelope: RequestEnvelope): Uint8Array => {
  const body: Record<string, unknown> = { ...envelope.extra, prompt: envelope.prompt }
  if (envelope.attachments.length > 0) {
    body["attachments"] = envelope.attachments.map((a) => ({
      ...a.extra,
      filename: a.filename,
      content: a.content,
    }))
  } else {
    delete body["attachments"]
  }
  return encoder.encode(JSON.stringify(body))
}
