import { expect, it } from "@effect/vitest"
import { subjectMatches } from "../src/internal/jetstream/subject.ts"

// NATS subject matching: "*" matches exactly one token, ">" matches one or more
// trailing tokens; both act only as whole tokens.

it("matches literal subjects exactly", () => {
  expect(subjectMatches("$KV.b.k", "$KV.b.k")).toBe(true)
  expect(subjectMatches("$KV.b.k", "$KV.b.other")).toBe(false)
  expect(subjectMatches("$KV.b.k", "$KV.b.k.x")).toBe(false)
  expect(subjectMatches("$KV.b.k.x", "$KV.b.k")).toBe(false)
})

it("matches * against exactly one token", () => {
  expect(subjectMatches("$KV.b.*", "$KV.b.k")).toBe(true)
  expect(subjectMatches("$KV.b.*", "$KV.b.k.x")).toBe(false)
  expect(subjectMatches("$KV.b.*", "$KV.b")).toBe(false)
  expect(subjectMatches("$KV.*.k", "$KV.b.k")).toBe(true)
})

it("matches > against one or more trailing tokens", () => {
  expect(subjectMatches("$KV.b.>", "$KV.b.k")).toBe(true)
  expect(subjectMatches("$KV.b.>", "$KV.b.a.b.c")).toBe(true)
  expect(subjectMatches("$KV.b.>", "$KV.b")).toBe(false)
  expect(subjectMatches(">", "anything.at.all")).toBe(true)
})

it("treats wildcard characters inside literal tokens as literals", () => {
  expect(subjectMatches("$KV.b.foo>", "$KV.b.k")).toBe(false)
  expect(subjectMatches("$KV.b.foo>", "$KV.b.foo>")).toBe(true)
  expect(subjectMatches("$KV.b.a*b", "$KV.b.axb")).toBe(false)
})
