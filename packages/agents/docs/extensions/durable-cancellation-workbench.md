# Durable replies and cancellation — non-standard extension workbench

Status: research only; not accepted, not implemented, and not Synadia Agent Protocol conformance.

## Authority boundary

The [Synadia Agent Protocol for NATS v0.3](../upstream/core-protocol-v0.3.0.md) defines Core NATS
reply streaming and explicitly leaves JetStream-backed streaming and cancellation signals out of
scope. This repository does not fill a “gap” in that protocol. It may define an independent,
opt-in extension protocol only after a separate ADR is accepted.

Standard v0.3 remains the default and fallback. A peer that is absent, vanilla, incompatible, or
ambiguous must receive only standard endpoint requests and frames.

## Negotiation requirements

Any future ADR must define all of the following before implementation:

1. A versioned capability identifier carried only in an upstream-permitted unknown metadata field.
2. Mutual, exact-version opt-in before any extension subject is used.
3. Subjects outside the upstream-reserved `agents.>` tree, with ownership and permission guidance.
4. Downgrade behavior that returns to unmodified v0.3 before a request is published.
5. A rule forbidding mid-request migration between standard and extension transports.
6. Cross-version and malicious-capability handling.

A possible metadata key or subject shown during research is not reserved until an ADR accepts it.

## Observable operations that must remain distinct

| Operation | What success can establish | What it cannot establish |
|---|---|---|
| Core publish/enqueue | Client accepted bytes for outbound processing | Server receipt, peer receipt, or durable storage |
| Current-connection flush | Server processed prior protocol traffic on that connection | Application handling or durable commit |
| JetStream publish with PubAck | A stream leader accepted the message under the reported stream/sequence | Consumer processing or business idempotency |
| Consumer acknowledgement | The consumer sent an acknowledgement for a delivery | Exactly-once application or external side effects |
| KV revision/CAS success | The server committed the conditional KV update at a revision | Multi-key atomicity or an Object Store object commit |
| Object Store metadata commit | Object metadata was accepted after chunk publication | Absence of abandoned chunks or reader digest success |

Timeout or transport loss after any request may leave the operation's outcome uncertain. Retrying is
allowed only when the ADR defines an idempotency key, its retention window, and duplicate semantics.

## Durable-reply questions

The durable-reply ADR must specify:

- stream ownership, naming, replicas, storage, retention, max age/messages/bytes, and cleanup;
- correlation and sequence identifiers plus deduplication-window bounds;
- producer ordering, PubAck handling, partial publication, and timeout-after-possible-commit;
- consumer kind, delivery policy, acknowledgement policy, redelivery, resume cursor, and ownership;
- the durable completion record and why it cannot be confused with the standard zero-byte/headerless
  terminator;
- cancellation during chunk publication, completion publication, acknowledgement, and cleanup;
- missing chunks, duplicate chunks, reordered observation, corrupt payloads, and digest/integrity
  behavior; and
- what survives reconnect, process restart, stream leader change, and account boundary changes.

JetStream deduplication does not by itself prove exactly-once delivery, exactly-once application, or
exactly-once stream termination.

## Cancellation questions

The cancellation ADR must define behavior for:

- cancellation before admission, after admission but before work, during work, after completion,
  and concurrently with failure;
- lost, duplicated, delayed, unauthorized, and spoofed cancellation messages;
- correlation lifetime and bounded in-flight registries;
- whether cancellation is advisory or acknowledged and what any acknowledgement proves;
- host interruption, provider/LLM abort propagation, and cleanup ordering; and
- a vanilla host that never observes the extension signal.

Local reply-subscription release remains necessary in every case, but it is not evidence that remote
work stopped.

## Outcome ledger required by the ADR

For each extension operation, tests must cover success, typed rejection, permission denial, timeout
before commit, timeout after possible commit, transport close, cancellation at every state,
duplicate/reordered delivery, reconnect, malformed frames, cleanup failure, and terminal-stream
ordering. Every uncertain outcome must remain represented as uncertain in the public API.

## Test and proof gates before implementation

- A pure transition model for negotiation and each extension lifecycle.
- Deterministic adversarial sequence tests against an independent reference language/model.
- Native `nats-server` tests with JetStream enabled, including leader loss or restart where relevant.
- Exact leak counters and bounded-growth tests for subscriptions, consumers, streams, maps, and
  abandoned chunks.
- Interoperability tests proving vanilla official callers and agent hosts remain on standard v0.3.
- Separate conformance reporting: extension tests cannot satisfy upstream requirement-ledger entries.
