# 0001 — Local conformance contract for the Synadia Agent Protocol adapter

This document constrains this repository's independent implementation of the externally owned
Synadia Agent Protocol for NATS v0.3.0. It is not a protocol specification and cannot amend the
vendored upstream contract. When this document and upstream differ, upstream wins and this file
must be corrected.

RFC 2119 keywords below are quotations or direct implementation obligations traced to the pinned
upstream specification. Local engineering policies are labeled **Local policy**.

## 1. Subject handling (Protocol §2)

- The default subject tuple is `agents.{verb}.{agent}.{owner}.{name}` with verbs `prompt | hb |
  status | attachments`; only the heartbeat subject is protocol-fixed.
- Tokens MUST NOT start with `$`; they SHOULD match `[a-z0-9_-]{1,63}`.
- Caller code learns endpoint subjects from `$SRV.INFO` and uses them verbatim. It never constructs
  prompt or status subjects from metadata.
- Agent-host code may build its own default endpoint subjects from validated configuration.

The runtime matcher and subject constructors are protocol-derived implementation machinery, not a
new subject algebra owned by this package.

## 2. Request path (Protocol §5)

- Envelope discrimination is total over bytes and maps malformed input to typed local diagnostics.
  Agent-host rejection maps those diagnostics to protocol status `400`.
- Plain text is promoted verbatim; JSON envelopes require a non-empty string `prompt`.
- Caller-side validation runs before publish and checks attachments plus final encoded bytes against
  both advertised `max_payload` and the broker maximum.
- Unknown envelope and attachment fields are tolerated and preserved by relay-capable codecs.

## 3. Response streaming (Protocol §§6 and 9)

For an accepted prompt, the agent host emits the mandatory `ack` first, preserves publication
order, emits at most one error signal, emits the zero-byte/headerless terminator last, and publishes
nothing afterward. A rejection emits an error signal followed by the terminator without first
claiming acceptance.

The local `stepWellFormed`/`wellFormed` DFA is a checker derived from those upstream rules. It is
used by the agent-host pump, caller read path, and independent model tests; it is not itself the
protocol authority.

The caller applies the upstream inactivity timeout to every observed frame, checks service-error
headers before body interpretation, rejects malformed known chunks, and ignores unknown chunk
types as required.

## 4. Cancellation boundary (Protocol §6.7)

The protocol defines no cancellation signal. Interrupting a local caller Stream must release its
reply subscription exactly once and stop local delivery. It does not prove the agent host observed
cancellation or stopped generation.

Any future cancellation signal is a separate, opt-in local extension protocol and must not be
described as standard v0.3 behavior.

## 5. Liveness (Protocol §8)

- Heartbeat interest is bound before discovery begins.
- Periodic heartbeat and status replies share one payload builder.
- Liveness is keyed by service `instance_id`, not the agent/owner/name tuple or session.
- A service instance is offline after the upstream three-interval threshold.

**Local policy:** remote interval values are validated and clamped for pruning, and the liveness map
has a hard capacity so untrusted heartbeat traffic cannot create unbounded retention.

## 6. Concurrency and resource bounds

**Local policy:** all subscriptions, fibers, queues, pubsubs, and concurrent work are scope-owned and
explicitly bounded. Agent-host prompt admission has a positive finite capacity; overload maps to
protocol status `429`. Broker callbacks must not block on arbitrary user work.

These bounds are implementation behavior. They do not create protocol delivery, fairness,
backpressure, retry, or durability guarantees.

## 7. Conformance and extensions

The vendored upstream file and generated requirement ledger are the only protocol conformance
authority in this repository. Tests for a local extension live outside that ledger.

Non-standard durability, cancellation, work queues, orchestration, and control-plane features must
be separately negotiated, use non-reserved subjects, preserve vanilla-peer fallback, and have an
accepted local ADR before implementation. Current research is isolated in
[`extensions/durable-cancellation-workbench.md`](extensions/durable-cancellation-workbench.md).

## 8. Error taxonomy

Protocol service errors preserve the upstream §9 status codes and headers. Local typed errors may
distinguish validation, discovery, inactivity timeout, malformed peer frames, transport failure,
and host configuration, but they must not invent new wire status semantics.
