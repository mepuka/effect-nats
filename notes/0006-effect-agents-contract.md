# 0006 — Pass A contract: Effect-native wrapper for the Synadia Agent Protocol

Target: `effect-agents/` wrapper around [`@synadia-ai/agents`](https://github.com/synadia-ai/synadia-agents)
(caller) + `@synadia-ai/agent-service` (host), integrated with `effect/ai`
(`LanguageModel`, `Chat`, `Toolkit`) and NATS/JetStream natively. No shortcuts:
wrap at protocol seams, not at promise boundaries.

## 1. Concept map (SDK → Effect)

| Synadia SDK | Effect-native form |
|---|---|
| `connect()` / `nc.close()` | `NatsConnection` Context.Service; scoped Layer via `Effect.acquireRelease`; reconnect defaults folded in |
| `nc.status()` | `Stream<NatsStatus>` on the connection service |
| Host: `new AgentService(...)` + `service.start()/stop()` | `AgentHost` resource in a scoped Layer; start/stop = acquire/finalize |
| Host: `service.onPrompt(async (envelope, response) => …)` | Handler is an **Effect**: `(envelope: PromptEnvelope) => Effect<Stream.Chunk<WireChunk>, E, R>` — a pumping interpreter writes chunks through `response.send`, honors backpressure, emits terminator once |
| Caller: `agent.prompt(...) → PromptStream (async iterable)` | `prompt(): Stream<WireChunk, AgentPromptError>` — async-iterable→Stream bridge with interruption ⇄ `AbortSignal` propagation in BOTH directions |
| Caller: `discover()` / `liveness` / `onHeartbeat` | Typed discovery effect + heartbeat liveness as reactive state (Cell-shaped), supervised background fibers |
| Their error hierarchy (`NatsAgentError`…) | Mirrored `Schema.TaggedError` classes; `instanceof` taxonomy ⇄ typed E channel, no `any` leaks |
| Wire chunks / envelopes | Protocol-compliant on the wire; internally Schema-typed values |

## 2. Integration invariant candidates (Lean-refereeable later)

1. **Terminator exactly-once** per prompt response, even under failure paths.
2. **Chunk order** per response is total and stable.
3. **Cancellation closes both ways**: fiber interruption sets the caller's
   `signal`; upstream `signal` abort fails the Stream; after abort no further
   chunks and no terminator-after-abort.
4. **Per-agent serialization**: default max 1 in-flight prompt per hosted
   agent (configurable bound); enforced by queue, not by hope.
5. **Heartbeat supervision**: the heartbeat loop is a supervised repeat —
   it cannot die silently; death surfaces as status.
6. **Bounded channels**: no unbounded queue between wire I/O and user code;
   a slow consumer applies backpressure to its own pump only.
7. **Task-queue dedup**: job id = deterministic function of intent; commit is
   idempotent (`dedupWindow` rung — effectively-once apply, honestly named).

## 3. Orchestration patterns backlog (the actual fun)

- **Decentralized task queue**: JetStream work-group consumers; jobs are
  Schema-typed; ack/nak/retry with Schedule; quarantine after max attempts;
  workers are ordinary Effects — scale by starting more worker fibers/processes.
- **Scatter-gather**: fan a prompt out to N discovered agents, first-good-wins
  or vote — `Effect.forEach` + racing with bounded concurrency.
- **Mid-stream query round-trips (§7 `.ask`)**: model as `Deferred` — agent asks,
  caller answers while generation keeps streaming.
- **Supervision trees**: agent hosts restart with backoff; status events feed a
  supervisor Cell.
- **Budgets**: per-agent/per-model rate limits as Schedule-based admission —
  overflow defers, never drops.
- **Conversation logs on JetStream**: append-only room log + replay-from-seq
  consumers (the eventlog sync pattern); deltas lossy, commits durable —
  our grade ladder decides which channel each message type may use.

## 4. Slices

1. **S1 — Connection + client read path**: deps installed; `NatsConnection`
   Layer; `AgentClient.discover/prompt` as interruptible Streams; tested
   against `ReferenceAgent` on a local `nats-server`.
2. **S2 — Host path**: `AgentHost` with Effect handlers; echo agent; then an
   `effect/ai` LanguageModel agent (provider from context — OpenRouter works).
3. **S3 — Task queue**: JetStream-backed typed work queue with ack/nak/retry/
   quarantine; the orchestration playground opens here.
4. **S4+**: scatter-gather, §7 queries, supervision, budgets.

## 5. Open questions

- OQ-1: do we depend on their packages at all, or reimplement the protocol
  subjects/wire format ourselves for full control? Default: DEPEND first
  (learn the protocol), fork later if ceilings appear. Record decision.
- OQ-2: single long-lived `nc` shared by host+client roles in one process —
  yes per SDK docs; model as one Layer, two role services over it.
- OQ-3: does the pump own chunk batching (coalesce tiny text chunks) or pass
  through verbatim? Perf-relevant; decide in S2 with measurements.
