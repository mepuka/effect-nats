# 0001 — Specification: Effect-native Agent Protocol runtime

Normative for our implementation. The upstream contract is the Agent Protocol
for NATS v0.3.0 ("the Protocol"); we implement it fully and add extensions
namespaced so vanilla peers are unaffected. RFC 2119 keywords apply.

## 1. Subject algebra (Protocol §2)

- Identity tuple: `agents.{verb}.{agent}.{owner}.{name}`; verbs `prompt | hb |
  status | attachments`; `hb` is protocol-fixed.
- Tokens: MUST NOT start with `$`; SHOULD match `[a-z0-9_-]{1,63}`.
- Endpoint subjects are LEARNED from `$SRV.INFO`, never constructed — except
  `agents.hb.*.*.*` which is fixed by the Protocol.
- Our internal subjects (cancellation, control plane) live under
  `_fx.agents.{...}` — outside the protocol's reserved tree, invisible to it.

Invariant S1 (refereeable): the token well-formedness relation and wildcard
matching (`*` = exactly one, trailing `>` = one-or-more) form a refinement
order; exact literal patterns are singletons. [Subject.lean already proves
this family; port to the protocol's verb-first shape.]

## 2. Request path

- Envelope discrimination (§5.3) implemented as a total function:
  `parseRequest: Uint8Array → Result<RequestEnvelope, RequestError>` with
  `RequestError = EmptyPayload | EmptyPrompt | MalformedJson | MissingPrompt |
  InvalidAttachment | NotAnObject`. All six map to status `400` on the wire
  (upstream §5.3, §9.2); the finer taxonomy exists for local diagnostics only.
- Plain text is promoted VERBATIM (no trimming); a prompt is rejected as empty
  exactly when it is blank (zero-length or whitespace-only) in either encoding.
- Local validation (§5.4) runs BEFORE publish: attachment permission and
  encoded-size against `min(max_payload_advertised, nc.info.max_payload)`.
- Unknown fields tolerated AND preserved (§5.6) — our Schema decoders carry a
  passthrough record.

## 3. Response streaming

Host obligations:

1. Emit `{type:"status",data:"ack"}` as the FIRST message on the reply
   subject, before any latency-inducing work (§6.4).
2. Chunks in publication order; terminator (zero-byte, header-less) LAST;
   nothing after it (§6.5).
3. Errors: error-headered message, THEN terminator (§9.3).

Our pump model: the handler returns a chunk Stream; a single interpreter owns
the reply subject and enforces the reply DFA structurally:

```
accepted ::= ack · chunk* · error? · terminator
rejected ::= error · terminator            (no ack: §6.4 ack confirms ACCEPTANCE,
                                            so 400/429 rejections precede it)
ack-first        :: pump emits ack before pulling the first element
terminator-once  :: single sequential writer; terminator in a guarded finalizer
order            :: Stream is sequential by definition
```

The same DFA (`stepWellFormed`/`wellFormed` in `protocol/chunks.ts`) is the
caller-side checker and the model-based test oracle.

Caller obligations: inactivity timeout (default 60s since last chunk, §6.6);
unknown chunk types skipped silently; error headers checked on every message.

**Extension D1 (durable mode)**: when both peers advertise JetStream support,
response chunks publish to an ad-hoc stream with `Nats-Msg-Id =
{reply-inbox}#{seq}`; caller consumes with an ephemeral ordered consumer;
terminator committed once ⇒ at-least-once delivery of a stream that terminates
exactly once. Reconnect mid-response replays from last-seen seq.

## 4. Cancellation (extension D2)

Protocol §6.7 defines none. We add:

- Caller-side: interrupting the prompt Effect closes the reply subscription
  (protocol behavior, chunks die server-side) AND publishes
  `_fx.agents.cancel.{instance_id}.{correlation_id}`.
- Host-side: the cancel subject is bound to the in-flight handler fiber via
  a registry keyed by correlation id; interruption propagates into
  `LanguageModel` calls (their SDKs take AbortSignal; our bridge maps fiber
  interruption ⇄ signal in both directions).
- Liveness fallback per Protocol §7: hosts SHOULD issue a mid-stream query as
  an abandonment probe for expensive work even without D2 support.

## 5. Liveness

- Heartbeat loop = supervised repeat: `Effect.repeat(Schedule.spaced(interval))`
  under a restart supervisor; failure increments a status Cell, never dies
  silently.
- Status endpoint replies from the SAME builder as periodic beats (§8.7.1).
- Subscribe-before-discover ordering enforced by the client's typestate:
  discovery is unavailable until the heartbeat wildcard is bound.

## 6. Concurrency and backpressure

- Default 1 in-flight prompt per hosted agent identity (queue group `"agents"`
  load-balances across instances; serialization is per-instance). Configurable
  bounded concurrency; overflow responds `429` (§9.2) — never queues forever.
- All wire⇄user boundaries are bounded Queues/PubSubs. A slow prompt reader
  backpressures its own pump only; broker I/O fibers never block on user code.

## 7. Work queue (module `queue/`)

Built strictly from documented NATS mechanics: WorkQueue retention + shared
durable pull consumers + explicit acks (docs: retention-policies,
worker-pool, acknowledgment). Job = Schema-typed payload + deterministic
dedup id (`Nats-Msg-Id`). Semantics: at-least-once delivery, idempotent
apply required; `nak` with delay for retry, `term` to quarantine after
max-deliver; advisories drive quarantine observability. This is
*effectively-once apply*, not exactly-once delivery — named honestly, same
discipline as the Lean grade ladder.

## 8. Error taxonomy

Mirrors §9.2 one-to-one as `Schema.TaggedError` classes:
`BadRequest400 | Unauthorized401 | Forbidden403 | NotFound404 | Conflict409 |
RateLimited429 | Internal500`, plus transport-level errors
(`StreamTimeout`, `TerminatorLost`) that have no protocol code but must be
distinguishable in E.
