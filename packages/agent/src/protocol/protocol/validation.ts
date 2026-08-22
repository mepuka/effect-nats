/**
 * Local validation — Protocol §5.4: callers MUST enforce advertised capability
 * metadata BEFORE publishing. Fails locally, no wire traffic.
 */

import type { RequestEnvelope } from "./envelope.js"

const UNIT_BYTES: Record<string, number> = {
  B: 1,
  KB: 1024,
  MB: 1024 ** 2,
  GB: 1024 ** 3,
}

/**
 * §2.1 max_payload format: positive integer + B | KB | MB | GB. Byte counts
 * that lose integer precision are rejected — a limit that cannot be compared
 * exactly is not a limit.
 */
export const parseMaxPayload = (s: string): number | null => {
  const m = /^(\d+)(B|KB|MB|GB)$/.exec(s)
  if (m === null || m[1] === undefined || m[2] === undefined) return null
  const unit = UNIT_BYTES[m[2]]
  if (unit === undefined) return null
  const n = Number(m[1])
  if (!Number.isSafeInteger(n) || n <= 0) return null
  const bytes = n * unit
  return Number.isSafeInteger(bytes) ? bytes : null
}

export type LocalViolation =
  | { readonly _tag: "AttachmentsNotSupported" }
  | { readonly _tag: "PayloadTooLarge"; readonly actual: number; readonly limit: number }

/**
 * §5.4: the binding limit is min(advertised max_payload, our own broker's
 * max payload) — a caller behind a smaller-cap broker fails fast.
 */
export const validateLocal = (
  envelope: RequestEnvelope,
  encodedSize: number,
  limits: {
    readonly maxPayload: number
    readonly brokerMaxPayload: number
    readonly attachmentsOk: boolean
  },
): LocalViolation | null => {
  if (envelope.attachments.length > 0 && !limits.attachmentsOk) {
    return { _tag: "AttachmentsNotSupported" }
  }
  const limit = Math.min(limits.maxPayload, limits.brokerMaxPayload)
  if (encodedSize > limit) {
    return { _tag: "PayloadTooLarge", actual: encodedSize, limit }
  }
  return null
}
