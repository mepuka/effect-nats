import { describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import {
  extractProtocolRequirements,
  type ExtractedRequirement,
  type RequirementKind,
} from "./spec-requirements.js"

interface CatalogRequirement extends ExtractedRequirement {
  readonly status: "unmapped" | "covered" | "deviation" | "informative"
  readonly tests: ReadonlyArray<string>
}

interface Catalog {
  readonly source: string
  readonly sourceCommit: string
  readonly sourceSha256: string
  readonly extractionVersion: number
  readonly requirements: ReadonlyArray<CatalogRequirement>
}

const isRequirementKind = (value: unknown): value is RequirementKind =>
  value === "normative" ||
  value === "wire-table" ||
  value === "procedure" ||
  value === "checklist" ||
  value === "exactness"

const isStatus = (value: unknown): value is CatalogRequirement["status"] =>
  value === "unmapped" ||
  value === "covered" ||
  value === "deviation" ||
  value === "informative"

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null

const isCatalogRequirement = (value: unknown): value is CatalogRequirement => {
  if (!isRecord(value)) return false
  const kinds = value["kinds"]
  const tests = value["tests"]
  const status = value["status"]
  return (
    typeof value["id"] === "string" &&
    typeof value["section"] === "string" &&
    typeof value["line"] === "number" &&
    Array.isArray(kinds) &&
    kinds.length > 0 &&
    kinds.every(isRequirementKind) &&
    typeof value["text"] === "string" &&
    isStatus(status) &&
    Array.isArray(tests) &&
    tests.every((entry) => typeof entry === "string")
  )
}

const isCatalog = (value: unknown): value is Catalog =>
  isRecord(value) &&
  typeof value["source"] === "string" &&
  typeof value["sourceCommit"] === "string" &&
  typeof value["sourceSha256"] === "string" &&
  typeof value["extractionVersion"] === "number" &&
  Array.isArray(value["requirements"]) &&
  value["requirements"].every(isCatalogRequirement)

const root = resolve(import.meta.dir, "../../..")
const protocolPath = resolve(root, "docs/upstream/core-protocol-v0.3.0.md")
const catalogPath = resolve(root, "docs/conformance/protocol-requirements.json")
const protocolBytes = readFileSync(protocolPath)
const protocolText = protocolBytes.toString("utf8")
const catalogJson: unknown = JSON.parse(readFileSync(catalogPath, "utf8"))
if (!isCatalog(catalogJson)) throw new Error("invalid protocol conformance catalog")
const catalog = catalogJson

const sourceProjection = (requirement: ExtractedRequirement) => ({
  id: requirement.id,
  section: requirement.section,
  line: requirement.line,
  kinds: [...requirement.kinds],
  text: requirement.text,
})

describe("vendored Agent Protocol v0.3 contract", () => {
  test("is byte-exact at the pinned upstream SHA-256", () => {
    expect(createHash("sha256").update(protocolBytes).digest("hex")).toBe(
      "4c03389e0c2157d961d9db4399f422dbe2d1664d35e7362dccef5fa52e671a85",
    )
    expect(catalog.sourceCommit).toBe("d907f5ca080528ee5e9e1385f0591c42f7fdb9b8")
    expect(catalog.sourceSha256).toBe(
      "4c03389e0c2157d961d9db4399f422dbe2d1664d35e7362dccef5fa52e671a85",
    )
  })

  test("catalog contains every extracted stipulation without rewriting it", () => {
    const extracted = extractProtocolRequirements(protocolText).map(sourceProjection)
    const recorded = catalog.requirements.map(sourceProjection)

    expect(recorded).toEqual(extracted)
    expect(new Set(recorded.map(({ id }) => id)).size).toBe(recorded.length)
  })

  test("strict conformance gate has executable evidence for every stipulation", () => {
    const missing = catalog.requirements
      .filter(({ status, tests }) =>
        status === "unmapped" || (status === "covered" && tests.length === 0),
      )
      .map(({ id }) => id)

    if (missing.length > 0) {
      throw new Error(
        `${missing.length} protocol requirements lack executable evidence: ${missing
          .slice(0, 20)
          .join(", ")}${missing.length > 20 ? ", …" : ""}`,
      )
    }
  })
})

describe("explicit unmapped protocol obligations", () => {
  for (const requirement of catalog.requirements) {
    if (requirement.status !== "unmapped") continue
    test.todo(`[${requirement.id}] ${requirement.section}: ${requirement.text}`, () => {})
  }
})
