# ADR-0004: Revision and sequence representation

Status: accepted, 2026-08-21; amended and re-accepted 2026-08-21 per the ADR-0006 adjudication
(Amendment 1), which refuted the original wording's `BigInt(number)` losslessness claim.
Discharges open decision 4 of [kv-slice-scoping.md](kv-slice-scoping.md).

## Decision

Stream sequences and KV/Object Store revisions are `bigint` throughout the substrate and domain
layers. JetStream sequences are unsigned 64-bit on the wire, which exceeds
`Number.MAX_SAFE_INTEGER`, so `number` silently corrupts above 2^53. A `bigint` is honest only
where it was parsed from the wire's decimal string. Official nats.js surfaces `number` for
sequences, and `JSON.parse` has already rounded the value before any conversion runs: `BigInt(n)`
is exact for every double and therefore launders an already-wrong value into an
authoritative-looking integer. `BigInt(n)` is not a fidelity remedy and must not appear
unguarded at any wire boundary.

The wrap adapter (ADR-0002) is the single boundary and obeys three rules:

1. String-borne sequences are lossless. Sequences read from a header — `Nats-Sequence` and
   `Nats-Last-Sequence` on a direct get, the ack-subject tokens of a `JsMsg` — are decoded with
   `Schema.BigIntFromString`, whose digit-string guard makes a malformed header a typed decode
   failure. `lastMessageForSubject` must move to `$JS.API.DIRECT.GET` for this reason; the JSON
   `STREAM.MSG.GET` path is not an acceptable revision source.
2. JSON-number sequences are lossy and fail closed. A sequence that reaches the adapter only as
   a JSON number (`PubAck.seq`, stream-state bounds, consumer sequence info) converts only when
   `Number.isSafeInteger` holds; otherwise the operation fails with a typed
   `SequenceNotProvablyExact` carrying the operation and raw response. No unchecked `BigInt(n)`
   ships.
3. Client-to-wire is lossless only when the adapter formats the `bigint` itself: the
   expected-last-subject-sequence header is written as `` `${bigint}` ``; interpolating a
   `number` is forbidden.

The public `Revision`/`ObjectRevision` types are branded `bigint`. The safe-integer gate is an
adapter-boundary property, never a narrowing of a domain type.

## Consequences

- No arithmetic on revisions outside comparison and equality; the substrate never invents
  sequence values.
- The wrap adapter owns the single number-to-bigint decision point and its test; the substrate
  and domain layers never see a `number` sequence.
- Failing closed on a non-exact sequence is necessary, not conservative: two distinct uint64
  sequences above 2^53 decode to the same JavaScript number, so a stale rounded revision can
  equal a different newer exact server revision and make an expected-sequence check pass.
