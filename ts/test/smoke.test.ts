import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"

describe("smoke", () => {
  it.effect("effect v4 runtime boots", () =>
    Effect.gen(function* () {
      const n = yield* Effect.succeed(1)
      assert.strictEqual(n, 1)
    }))
})
