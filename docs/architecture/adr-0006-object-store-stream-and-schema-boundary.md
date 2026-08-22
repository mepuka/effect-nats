# ADR-0006: Object Store stream and Schema boundary

Status: proposed. This ADR does not authorize a public export or implementation. The owner must
accept it after the open decisions and revision blocker in
[object-store-slice-scoping.md](object-store-slice-scoping.md) are resolved.

## Context

NATS Object Store is a JetStream materialized view. Object bytes are published as chunk messages
under a per-object NUID; a rolled-up metadata message makes that chunk set the current value. The
official architecture specifies a 128 KiB default chunk size and stream subjects/configuration,
while the pinned NATS.js implementation clamps the requested chunk size to the connection's
`max_payload`, awaits each chunk PubAck, computes a whole-object SHA-256 digest, and publishes
metadata last.

The public NATS.js Object Store read surface is not a safe production bridge for this repository's
stream contract. It pumps a push consumer into a W3C `ReadableStream` without consulting
`desiredSize`, while watch pushes into `QueuedIteratorImpl`. Adding an Effect queue outside either
source would bound only the second queue, not the real producer-side buffering.

Effect Schema also does not make an arbitrary encoded value incremental. A normal Schema adapter
must encode or collect a whole value before applying `Schema.encodeEffect` or
`Schema.decodeEffect`; presenting it as an unbounded streaming codec would be false.

## Proposed decision

1. The canonical Object Store body API is raw and streaming:
   `Stream<Uint8Array, ObjectReadError>` for reads and a caller-supplied
   `Stream<Uint8Array, E, R>` for writes. It is implemented over the private public-JetStream
   substrate, not the public `@nats-io/obj` read/watch iterators. The adapter preserves the
   official Object Store wire layout and server-observable semantics.
2. Reads are pull-driven through a finite positive chunk or byte window. No production read or
   watch uses an unbounded async-iterator/ReadableStream bridge. Object watches use a finite event
   queue and terminate with a resumable typed lag error on overflow.
3. The metadata PubAck is the write commit boundary. Chunk PubAcks prove only that chunks landed;
   cleanup of old or orphan chunks is a separate outcome. A post-commit cleanup failure never
   converts a committed write or delete into failure.
4. A timeout or disconnect after metadata publication may have reached the server is
   `ObjectCommitUnknown`. Interruption remains interruption, but records the same recovery evidence
   before release. Neither path purges the new chunk subject until metadata has been reconciled by
   name, revision, NUID, and digest; blindly purging could corrupt a value whose metadata committed
   while its PubAck was lost.
5. Effect Schema integration has two layers:
   - all wire metadata and public domain values are Schema-backed and decoded from `unknown` at
     the adapter boundary;
   - typed object values are an opt-in, explicitly bounded whole-value adapter whose Schema
     encoded side is `Uint8Array`. JSON is one named composition, not an implicit default.
6. Schema encode completes and its byte bound is checked before the first object chunk is
   published. Schema decode begins only after the raw body has been fully consumed and its digest,
   size, and chunk count verified. Generic incremental/framed Schema streams are a separate future
   decision.
7. Object Store configuration uses ADR-0005's four layers. Fixed protocol invariants such as
   subjects, roll-up headers, `allow_direct`, and `discard = new` are not exposed as ordinary
   Object Store settings. Requested and effective chunk sizes are both observable.
8. The ordinary first-slice mutation capability has no unconditional `put`. `create` commits only
   when the object is logically absent, and `replace`, metadata update, and delete require the
   metadata revision the caller observed. The expected-last-subject-sequence header is attached to
   the metadata publish, so the server—not a client-side pre-check—arbitrates the race. A deliberately
   named unconditional parity capability may be proposed separately, but cannot be a default or
   optional omitted condition.
9. Guarded mutation requires an exact revision. While the official NATS.js adapter exposes
   revisions as `number`, the first slice rejects every non-safe-integer revision before it can be
   used as a condition. ADR-0004 must be corrected before this ADR can be accepted. A later
   lossless wire seam may widen `ObjectRevision` to the full server uint64 range.

## Consequences

- The first Object Store slice can depend on official public Core/JetStream packages while owning
  the small Object Store state machine required for honest Effect streams. This is compatible with
  ADR-0002's private substrate seam but is not a thin wrapper over `@nats-io/obj.get` or `watch`.
- A streaming read can emit bytes before final digest verification. Its values are provisional
  until the stream terminates successfully. The bounded Schema/blob adapters provide a verified
  value for callers that must not observe provisional bytes.
- Replacing a value temporarily stores both old and new chunks. Provisioning must include
  replacement and orphan-cleanup headroom; `maxBytes` is not a per-object limit.
- Revision-guarded writes prevent this client from overwriting an unseen concurrent change at its
  commit point. They do not stop a different client with direct publish authority from issuing a
  later unconditional metadata write. Deployments requiring enforced single-writer/CAS policy must
  restrict direct Object Store write authority and route mutations through a controlled writer.
- The safe-integer gate is a temporary compatibility limit, not a claim about JetStream's range.
  Failing closed is necessary: two distinct uint64 sequences above 2^53 can decode to the same
  JavaScript number and invalidate the CAS proof.
- Typed Schema reads and writes are deliberately memory-bounded conveniences, not the primary
  large-object API. Their Effect environment includes the Schema's decoding/encoding services.
- The initial implementation requires pure write/read state machines, a scripted JetStream
  interpreter, real-server conformance tests, and fault tests that lose metadata PubAcks after the
  server accepts them.

## Sources

- NATS Object Store specification: [ADR-20, design and configuration](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-20.md#basic-design).
- Pinned NATS.js upload and commit sequence:
  [`objectstore.ts` lines 442-574](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L442-L574).
- Official per-subject optimistic-concurrency header:
  [`message.go` lines 1541-1575](https://github.com/nats-io/nats.go/blob/main/jetstream/message.go#L1541-L1575).
- Pinned NATS.js read pump and digest check:
  [`objectstore.ts` lines 635-725](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L635-L725).
- Pinned NATS.js watch queue:
  [`objectstore.ts` lines 829-908](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L829-L908).
- Effect v4 Schema effectful boundaries:
  [`Schema.ts` lines 1493-1554](https://github.com/Effect-TS/effect/blob/648f566dd259898e7697c7fcb796183ccbc474ab/packages/effect/src/Schema.ts#L1493-L1554)
  and
  [`Schema.ts` lines 1953-2015](https://github.com/Effect-TS/effect/blob/648f566dd259898e7697c7fcb796183ccbc474ab/packages/effect/src/Schema.ts#L1953-L2015).
