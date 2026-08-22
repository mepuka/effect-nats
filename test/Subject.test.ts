import { expect, it } from "@effect/vitest"
import * as Subject from "../src/internal/Subject.ts"

// NATS subject matching: "*" matches exactly one token, ">" matches one or more
// trailing tokens; both act only as whole tokens.

it("matches literal subjects exactly", () => {
  expect(Subject.matches("$KV.b.k", "$KV.b.k")).toBe(true)
  expect(Subject.matches("$KV.b.k", "$KV.b.other")).toBe(false)
  expect(Subject.matches("$KV.b.k", "$KV.b.k.x")).toBe(false)
  expect(Subject.matches("$KV.b.k.x", "$KV.b.k")).toBe(false)
})

it("matches * against exactly one token", () => {
  expect(Subject.matches("$KV.b.*", "$KV.b.k")).toBe(true)
  expect(Subject.matches("$KV.b.*", "$KV.b.k.x")).toBe(false)
  expect(Subject.matches("$KV.b.*", "$KV.b")).toBe(false)
  expect(Subject.matches("$KV.*.k", "$KV.b.k")).toBe(true)
})

it("matches > against one or more trailing tokens", () => {
  expect(Subject.matches("$KV.b.>", "$KV.b.k")).toBe(true)
  expect(Subject.matches("$KV.b.>", "$KV.b.a.b.c")).toBe(true)
  expect(Subject.matches("$KV.b.>", "$KV.b")).toBe(false)
  expect(Subject.matches(">", "anything.at.all")).toBe(true)
})

it("treats wildcard characters inside literal tokens as literals", () => {
  expect(Subject.matches("$KV.b.foo>", "$KV.b.k")).toBe(false)
  expect(Subject.matches("$KV.b.foo>", "$KV.b.foo>")).toBe(true)
  expect(Subject.matches("$KV.b.a*b", "$KV.b.axb")).toBe(false)
})
