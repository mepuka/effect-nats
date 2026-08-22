/**
 * Bridge tests — the deployment obligations from notes/0003/0005.
 * Each test names the theorem it mirrors in the proved companion package
 * (github.com/mepuka/effect-nats-lean); agreement here is evidence for
 * (not proof of) TS ⟷ Lean conformance.
 */
import { describe, expect, test } from "bun:test"
import { TestSchema } from "effect/testing"
import { UserCreated, DeviceEvent, type UserCreated as User } from "../schema/messages.js"
import { encodeWire, decodeWire } from "../schema/wire.js"
import { matchSubject, mkSubject } from "../core/subjects.js"
import { meet } from "../nats/grade.js"
import { Registry, issue } from "../core/registry.js"

// -- rt_ed: encode then decode recovers the value (Lean: rt_ed) --------------

describe("rt_ed round trip (TestSchema property)", () => {
  test("UserCreated", async () => {
    await new TestSchema.Asserts(UserCreated).verifyLosslessTransformation({
      params: { numRuns: 200 },
    })
  })

  test("DeviceEvent", async () => {
    await new TestSchema.Asserts(DeviceEvent).verifyLosslessTransformation({
      params: { numRuns: 200 },
    })
  })

  test("optional field is wire-absent, not null", () => {
    const user: User = { name: "ann" }
    expect(JSON.parse(encodeWire(UserCreated)(user))).toEqual({ name: "ann" })
  })

  test("tagged union discriminates on _tag", () => {
    const wire = encodeWire(DeviceEvent)({ _tag: "Move", dx: -2 })
    expect(JSON.parse(wire)).toEqual({ _tag: "Move", dx: -2 })
  })

  test("decode failure is an error-channel entry", () => {
    const bad = JSON.stringify({ name: 42 })
    try {
      decodeWire(UserCreated)(bad)
      throw new Error("should have thrown")
    } catch (e) {
      expect(e).toBeInstanceOf(Error)
      expect((e as Error).message).not.toBe("should have thrown")
    }
  })
})

// -- subject algebra (Subject.lean: gt/star/exact_singleton witnesses) -------

describe("subjects", () => {
  test("wildcards unrepresentable in publishable subjects", () => {
    expect(mkSubject(["orders", "*"])).toBeNull()
    expect(mkSubject(["orders", ">"])).toBeNull()
    expect(mkSubject(["orders", "eu", "created"])).not.toBeNull()
    expect(mkSubject(["orders", "eu.created"])).toBeNull()
  })

  test("> needs one token past the prefix", () => {
    const p = { toks: [], gt: true }
    expect(matchSubject([], p)).toBe(false)
    expect(matchSubject(["a"], p)).toBe(true)
    expect(matchSubject(["a", "b"], p)).toBe(true)
  })

  test("* arity is strict", () => {
    const star = { toks: [{ _tag: "star" as const }], gt: false }
    expect(matchSubject(["a"], star)).toBe(true)
    expect(matchSubject([], star)).toBe(false)
    expect(matchSubject(["a", "b"], star)).toBe(false)
  })

  test("exact patterns are singletons (exact_singleton)", () => {
    const exact = mkSubject(["orders", "created"])
    if (exact === null) throw new Error("unreachable")
    const pattern = {
      toks: exact.tokens.map((t) => ({ _tag: "lit" as const, value: t })),
      gt: false,
    }
    expect(matchSubject(["orders", "created"], pattern)).toBe(true)
    expect(matchSubject(["orders", "rejected"], pattern)).toBe(false)
    expect(matchSubject(["orders", "created", "extra"], pattern)).toBe(false)
  })
})

// -- registry (closed world; collisions rejected at registration) ------------

describe("registry", () => {
  test("issues typed subjects and rejects collisions", () => {
    const registry = new Registry()
    const ok = issue(registry, ["orders", "created"], UserCreated)
    if ("_tag" in ok) throw new Error(`expected success: ${JSON.stringify(ok)}`)
    expect(ok.name).toBe("orders.created")

    const collision = issue(registry, ["orders", "created"], DeviceEvent)
    expect("_tag" in collision && collision._tag).toBe("collision")
  })
})

// -- grade ladder (Guarantee.lean: meet laws' key instances) -----------------

describe("grades", () => {
  test("meet is absorbing at atMostOnce", () => {
    expect(meet("atMostOnce", "pure")).toBe("atMostOnce")
    expect(meet("dedupWindow", "atLeastOnce")).toBe("atLeastOnce")
    expect(meet("pure", "atLeastOnce")).toBe("atLeastOnce")
  })
})
