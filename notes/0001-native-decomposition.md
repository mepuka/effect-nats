# 0001 — What a native NATS+JetStream layer means in Effect's base abstractions

Grounded in a firsthand read of the eight Foldable effect briefs (core-representation,
fiber-machine, r-channel, extension-patterns, stream-channel, reified-dsls-algebra,
schema-deep, schema-variants-encoding), all against `effect-foldable` @ `648f566dd`
(4.0.0-rc.111). Facts below cite the brief that establishes them.

"Native" means: the protocol is spoken by this layer in Effect's own pattern language, with
no nats.js at runtime and no runtime API (node/bun/browser/deno) in the core — runtimes enter
only as transport Layers, exactly the `packages/platform-*` split. nats.js stays vendored
reference material.

## The two structural facts everything follows from

**The Effect kernel is open and non-enumerable by design.** There is no switch: dispatch is a
virtual call on the string slot `~effect/Effect/evaluate`, the op tag is never read by the
interpreter, and `Effectable.Prototype` is the public door for minting primitives — the
stdlib itself mints `Statement`, `Command`, `Config`, `Service`, `Prompt`, `Activity` this
way (core-representation §1, §3). Two string slots make a foreign object both guard-visible
and runnable (`~effect/Effect` for `isEffect`, `~effect/Effect/evaluate` for the run loop);
the run loop alone needs only the second (core-representation §5). Consequence: our
operations can *be* Effects — plain objects the run loop steps directly, no adapter, no
wrapper — while the kernel remains unforked.

**Schema is the library's spine, and it is the counter-model to the kernel.** Where the
kernel is an open set of self-stepping objects, Schema is a *closed* 21-node discriminated
AST with derived interpreters — parser (via per-node `getParser`, memoized by AST identity),
encoder (decode of the `flip`ped AST), arbitrary, equivalence, formatter, JSON Schema,
canonical-JSON codec, serializable Representation (schema-deep §2–4, reified-dsls §1). Every
high-level subsystem consumes a Schema value or AST — HttpApi, Rpc, AI tools, CLI, workflow,
cluster, persistence, Config, SQL, ChannelSchema. The two deliberate exceptions sit *below*
schema: `ConfigProvider` (raw nodes) and `RpcSerialization` (bytes over already
schema-encoded envelopes) (schema-deep §6). That is the layering law: framing services below,
Schema in the middle, schema-carrying descriptors above. A native NATS layer that respects it
is "in the library"; one that invents its own validation/serialization story is a wrapper.

So the design thesis: **build the closed algebra the kernel deliberately isn't, the way
Schema already did** — a closed frame/instruction AST wearing the open Effectable protocol at
the boundary, with multiple derived interpreters (live connection, in-memory server model as
conformance oracle, trace recorder, Lean transcription). Schema is the existence proof that
this shape thrives inside the open kernel.

## The stack, each layer named by its in-tree precedent

| Layer | Pattern (extension-patterns §8 taxonomy) | Precedent | NATS instantiation |
|---|---|---|---|
| Wire grammar | encoding-module shape | `unstable/encoding/Sse` + `Msgpack` | Local stateful incremental parser (`makeParser(onParse)` with `feed`/`reset`), `decode`/`encode` Channel transducers over byte chunks with cross-chunk partial-frame state, `duplex` wrappers, `NatsError = Data.TaggedError(...)<{kind: "Pack" \| "Unpack"}>`. parser.go is the reference machine. |
| Frame model | tagged Schema envelope + runtime/encoded pair | `RpcMessage`, cluster `Envelope` | Closed Schema union of INFO/CONNECT/PING/PONG/PUB/HPUB/SUB/UNSUB/MSG/HMSG/OK/ERR; subjects as checked branded strings (checks carry validation — `brand` alone adds none, schema-deep §2). |
| Transport | serializer-strategy service + Layer factory | `RpcSerialization`, `unstable/socket` (note `SendQueueCapacity=16` Reference) | `NatsTransport` Context.Service = duplex byte channel; Layers per runtime from the platform packages. Core never imports a runtime API. |
| Connection | scoped resource + machine-state fields + Reference config | `FiberImpl` field discipline (fiber-machine §1), the 48 `Context.Reference` configs | Subscription table, sid counter, server pool, parser position, pending pings as mutable fields each with enumerated write sites; read-loop fiber; reconnect/ping/inbox-prefix knobs as `Context.Reference` with defaults (TestClock-compatible). Drain follows the `acquireRelease` mask protocol (fiber-machine §7). |
| Client kernel ops | Effectable primitive, schema-only payload | `Statement`, `Command`, `IndexedDbQuery.*` (13 scoped sites, all through `Effectable`) | `Pub`/`Sub`/`Unsub`/`Ping` as descriptors that are Effects; `evaluate` reads the connection from `fiber.context` (the `Context.Service` evaluate shape). Payloads schema-only, so they land on the durable side of the §3 boundary law — unlike Statement, which captures closures. |
| Derived ops | derivation, as in the kernel's 9 derivable ops | `Suspend ≈ flatMap(sync)` (core-representation Surprises) | request/reply = SUB inbox + PUB reply-to + first + UNSUB; mux inbox an optimization; drain = UNSUB* + PING barrier; flush = PING barrier. Provable derivations — Lean obligations. |
| Subscription delivery | queue strategy choice | `Channel.asyncQueue` (`suspend`/`dropping`/`sliding`, omitted capacity = unbounded) | The capacity/strategy choice IS slow-consumer policy, surfaced in the API, never defaulted to unbounded. Correctness obligation: ordinary failure drains buffered messages, interruption drops them (stream-channel §7) — `Done` vs interrupt must not be conflated. |
| JetStream API | schema-carried descriptor + VariantSchema projection | the six schema-carried families; `Model`'s select/insert/update variants | `$JS.API.>` request/reply with Schema-typed bodies and error union; stream/consumer config as variant schemas (create/update/info are projections of one declaration, exactly Model's shape). |
| Consumer flow | Pull protocol + Take | `Pull` = `Effect<A, E \| Cause.Done<Done>, R>`, `Take` = `NE<A> \| Exit<Done,E>` (stream-channel §1, §4) | Pull-consumer fetch loop is a `Pull`; a fetch batch is literally a `Take`; acks are derived publishes to the `$JS.ACK.…` reply subject (subject grammar as TemplateLiteral schema); AckWait/backoff on the Clock service. |
| KV / Object | derived state machine over streams | eventlog's MessagePack payload schemas | KV over JetStream only; watch is a Stream; CAS via `Nats-Expected-*` headers; revisions are sequences. |
| Guarantee grading | phantom R entries | verdict: viable, all four properties hold (r-channel §6) | Ladder rungs (at-most-once core, at-least-once JS publish, window-bounded dedup) as phantom services discharged by the operations that upgrade them. Namespace the key strings; avoid the two scanned prefixes. |

## Details that would otherwise be relearned the hard way

- The Sse decoder surfaces the `retry:` control directive through the Channel *error* type,
  after buffered events (schema-variants §12). The same move separates NATS control-plane
  signals from the MSG element stream without a wrapper union.
- Sse enforces `maxEventSize` (default 10 MiB) in the parser; the NATS parser gets the same
  bound from negotiated INFO `max_payload`.
- `Async` is the only true async door, resume is single-shot and first-resume-wins
  (fiber-machine §4). The read loop's frame dispatch into queues, and `Deferred.doneUnsafe`'s
  synchronous waiter resumption (nested run-loop reentry, fiber-machine §3), are where
  request/reply correlation must be careful.
- `Cause` gives ordered Fail/Die/Interrupt slots (fiber-machine §8): typed `Fail` for
  no-responders and `$JS.API` `Error{code, err_code}`, `Die` for protocol violations,
  `Cause.Done` for consumer/batch termination.
- v4 has no typeclass layer: `Combiner`/`Reducer`/`Order`/`Equivalence` only, `Combiner.min`/
  `max` are the semilattice constructors, `Cause.combine` is idempotent+associative but NOT
  commutative (ordered union, reified-dsls §3–4). Lattice/monotonicity laws for KV and
  guarantee ladders have no lawful TS home — they live in Lean; TS carries the operations.
- `SchemaRepresentation` is not canonical: identity-keyed memoization, insertion-order
  references, closure loss, order-sensitive Documents (schema-deep §4). Deriving subjects or
  KV keys from schema identity requires the canonicalization quotient first — a Lean target,
  not a TS afternoon.
- Reification stops at the instruction level: composition above our ops is opaque Effect
  structure (no total fold over Effect programs — core-representation §5's negative result).
  If choreography/whole-program folds are wanted later, the in-tree precedent for a reified
  *control* tree is CLI `Prompt` (`Loop`/`OnSuccess`/`Succeed` interpreted by `run(this)`,
  extension-patterns §2). That is a second, separate decision.

## Open questions (the real forks)

1. **Instruction-level vs program-level reification.** Ops-as-Effectable composed by
   ordinary `flatMap` (folds see single ops; programs stay opaque), or a reified program
   layer above (Prompt-style control tree; folds see whole workflows; CALM/choreography
   becomes expressible; cost: we own a second composition story). Start instruction-level,
   revisit after the Lean model exists?
2. **Subscription algebra boundary.** SUB as an op yielding a Stream, or subscription as a
   scoped resource with `next` as the op. Determines the fold's shape and the Lean state
   space.
3. **How far down "native" goes on day one.** Speak the wire immediately (frame codec first,
   the Sse/Msgpack template makes this a bounded task) vs. temporarily borrow nats.js's
   transport under our instruction layer. The decomposition says the codec is the cheapest
   high-value piece and is pure — no connection machinery needed to test it.
4. **Grading mechanism.** Phantom R entries (viable, invisible, compile-time only) vs a
   wrapper type carrying the rung as a type parameter (visible in signatures, heavier). The
   phantom option composes for free; the wrapper option is teachable in error messages.
