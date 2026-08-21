import { Effect } from "effect"

const program = Effect.log("clean slate")

Effect.runPromise(program)
