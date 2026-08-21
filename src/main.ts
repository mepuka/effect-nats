/**
 * Demo: schema → encode to wire text → publish (deduped) → tap → decode back.
 * The round trip exercised here is exactly the Lean `rt_ed` scenario.
 */
import { Effect, Fiber, Option, Stream } from "effect"
import { UserCreated, type UserCreated as User } from "./schema/messages.js"
import { encodeWire, decodeWire } from "./schema/wire.js"
import { issue, Registry } from "./core/registry.js"
import { renderSubject } from "./core/subjects.js"
import { Nats } from "./nats/service.js"

const registry = new Registry()
const subject = issue(registry, ["orders", "eu", "created"], UserCreated)
if ("_tag" in subject) {
  throw new Error(`registration failed: ${subject._tag}`)
}

const user: User = { name: "ann", age: 7 }

const program = Effect.gen(function* () {
  const nats = yield* Nats
  const wire =  encodeWire(UserCreated)(user)

  // tap the stream before publishing — the subscriber sees it land
  const reader = yield* Effect.forkChild(
    nats.tap().pipe(Stream.take(1), Stream.runHead),
  )

  const first = yield* nats.jsPublishDedup("user-1", subject, wire)
  const second = yield* nats.jsPublishDedup("user-1", subject, wire)
  const tapped = Option.getOrUndefined(yield* Fiber.join(reader))
  const model = yield* nats.model

  return {
    subject: renderSubject(subject),
    wire,
    first,
    second,
    tapped: tapped?.payload ?? null,
    decoded: decodeWire(UserCreated)(wire),
    logLength: model.log.length,
  }
})

Effect.runPromise(Effect.provide(program, Nats.layer)).then(console.log)
