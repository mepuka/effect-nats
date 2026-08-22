import { expect, it } from "@effect/vitest"
import { Effect } from "effect"

it.effect("runs on the pinned Effect v4 runtime", () =>
  Effect.sync(() => {
    expect(true).toBe(true)
  }))
