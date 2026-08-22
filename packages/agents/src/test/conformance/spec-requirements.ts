export type RequirementKind =
  | "normative"
  | "wire-table"
  | "procedure"
  | "checklist"
  | "exactness"

export interface ExtractedRequirement {
  readonly id: string
  readonly section: string
  readonly line: number
  readonly kinds: readonly [RequirementKind, ...Array<RequirementKind>]
  readonly text: string
}

const normative = /\b(MUST(?: NOT)?|SHOULD(?: NOT)?|MAY)\b/
const exactness =
  /\b(exactly|zero-byte|headerless|no NATS headers|first message|last message|publication order|not accepted|fixed|stable subjects?|before|after the terminator|inactivity timeout|unknown fields?|unknown chunk|without publishing|positive integer|standard-alphabet|MAJOR\.MINOR)\b/i
const tableSeparator = /^\|(?:\s*:?-+:?\s*\|)+$/

const isTableSeparator = (line: string): boolean => tableSeparator.test(line.trim())

/**
 * Deliberately over-approximates protocol obligations. Never loosen this to make
 * coverage pass; classify a catalog entry as informative instead.
 */
export const extractProtocolRequirements = (source: string): ReadonlyArray<ExtractedRequirement> => {
  const lines = source.replace(/\r\n/g, "\n").split("\n")
  const requirements: Array<ExtractedRequirement> = []
  let section = ""
  let fenced = false

  for (let index = 0; index < lines.length; index += 1) {
    const trimmed = (lines[index] ?? "").trim()

    if (trimmed.startsWith("```")) {
      fenced = !fenced
      continue
    }
    if (fenced || trimmed.length === 0) continue

    const heading = /^(#{2,4})\s+(.+)$/.exec(trimmed)
    if (heading !== null) {
      section = heading[2] ?? ""
      continue
    }
    if (trimmed.startsWith("Normative keywords")) continue

    const next = (lines[index + 1] ?? "").trim()
    const tableHeader = trimmed.startsWith("|") && isTableSeparator(next)
    if (tableHeader) continue
    const tableData =
      trimmed.startsWith("|") && !isTableSeparator(trimmed) && !isTableSeparator(next)
    const numberedProcedure = /^\d+\.\s/.test(trimmed)
    const checklist = section.startsWith("12.") && /^-\s/.test(trimmed)
    const hasExactnessMarker = exactness.test(trimmed)
    const hasNormativeKeyword = normative.test(trimmed)

    const kinds: Array<RequirementKind> = []
    if (hasNormativeKeyword) kinds.push("normative")
    if (tableData) kinds.push("wire-table")
    if (numberedProcedure) kinds.push("procedure")
    if (checklist) kinds.push("checklist")
    if (hasExactnessMarker) kinds.push("exactness")
    const firstKind = kinds[0]
    if (firstKind === undefined) continue

    requirements.push({
      id: `AP03-L${String(index + 1).padStart(4, "0")}`,
      section,
      line: index + 1,
      kinds: [firstKind, ...kinds.slice(1)],
      text: trimmed,
    })
  }

  return requirements
}
