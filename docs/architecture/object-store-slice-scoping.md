# Object Store slice scoping

Status: draft workbench material for proposed
[ADR-0006](adr-0006-object-store-stream-and-schema-boundary.md). Nothing here authorizes a public
export. Facts are pinned to NATS.js commit
[`3a7b736`](https://github.com/nats-io/nats.js/tree/3a7b736b175d9a387130210c88f851d71e1b014f)
and the implemented NATS [Object Store ADR-20](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-20.md).

## Slice boundary

The first slice provides an Effect-native Object Store over the private JetStream substrate:

- open or create an Object Store bucket;
- inspect, list, create, revision-guarded replace, get, revision-guarded delete, and
  revision-guarded metadata update;
- raw streaming bodies with whole-object SHA-256 verification;
- object and bucket links with explicit resolution policy;
- a scoped, bounded metadata watch;
- Schema-backed wire/domain models and a bounded typed-value adapter;
- status, seal, and destroy on a separate administrative capability.

Deferred: unconditional put, rename/move, filesystem conveniences, range/chunk reads, automatic
orphan collection, cross-domain links, streaming JSON/record framing, batched/fast ingest, and a
general raw-Stream configuration escape hatch. Rename is a two-metadata-subject operation and
cannot inherit same-subject CAS atomicity. Batched ingest is experimental and server/API-level
gated in the pinned client; it must not become an implicit performance mode.

## Domain vocabulary and invariants

| Term                  | Meaning and invariant                                                                                                                                                          |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ObjectBucketName`    | Non-empty restricted term used in `OBJ_<bucket>` and `$O.<bucket>...`; parsed by a branded Schema.                                                                             |
| `ObjectName`          | Non-empty string preserved byte-for-byte at the text level; the metadata subject uses padded base64url. No Unicode normalization is invented.                                  |
| `ObjectId`            | NUID identifying exactly one set of chunk messages.                                                                                                                            |
| `ObjectRevision`      | Sequence of the current metadata message, not a chunk sequence. See the revision blocker below.                                                                                |
| `ObjectDigest`        | Currently exactly `SHA-256=<base64url-padded digest>`; unknown algorithms are typed failures.                                                                                  |
| `ObjectMetadataInput` | Caller-controlled name, description, headers, string metadata, and requested chunk size. It cannot set bucket, NUID, size, chunks, digest, revision, deletion, or server time. |
| `ObjectInfo`          | Validated server metadata plus metadata-message revision and canonical modified time. The canonical time source is an explicit conformance decision below.                     |
| `ObjectReader`        | Scoped metadata plus a single-consumer body Stream. A successful terminal proves the declared object was fully received and verified.                                          |
| commit                | The metadata publish receives a JetStream PubAck. Chunk PubAcks alone are not an object commit.                                                                                |
| cleanup               | Purging old or abandoned chunk subjects after or before commit. Cleanup changes storage use, not the already-observed commit fact.                                             |

The flattened wire `ObjectInfo` is decoded into a tagged domain union rather than leaving
inconsistent nullable fields available:

- `DataObjectInfo` — live bytes, non-empty digest, no link;
- `ObjectLinkInfo` — link with bucket and object name;
- `BucketLinkInfo` — link with bucket and no object name;
- `DeletedObjectInfo` — tombstone metadata; ordinary `get` and `info` may hide it, but watch and
  explicit `infoIncludingDeleted` preserve it.

The official specification treats deleted values as absent for ordinary get/info and requires
links-of-links to fail. The Effect API keeps those semantics while retaining a tombstone variant
where the operation explicitly includes deletions.

## Proposed capability shape

Names are illustrative until ADR acceptance and a compile spike against Effect `4.0.0-rc.111`.

```ts
interface ObjectStore {
  readonly open: (
    bucket: ObjectBucketName,
    options?: ObjectOpenOptions
  ) => Effect<ObjectBucket, ObjectOpenError>

  readonly create: (
    bucket: ObjectBucketName,
    config: ObjectStoreBucketConfig
  ) => Effect<ObjectBucket, ObjectCreateError>
}

interface ObjectBucket {
  readonly info: (name: ObjectName) => Effect<Option<ObjectInfo>, ObjectReadError>
  readonly infoIncludingDeleted: (
    name: ObjectName
  ) => Effect<Option<ObjectEntryInfo>, ObjectReadError>
  readonly list: (options?: ObjectListOptions) => Stream<ObjectEntryInfo, ObjectReadError>
  readonly get: (
    name: ObjectName,
    options?: ObjectReadOptions
  ) => Effect<Option<ObjectReader>, ObjectReadError, Scope>

  readonly create: <E, R>(
    metadata: ObjectMetadataInput,
    body: Stream<Uint8Array, E, R>,
    options?: ObjectCreateOptions
  ) => Effect<ObjectWriteResult, E | ObjectWriteError, R>

  readonly replace: <E, R>(
    metadata: ObjectMetadataInput,
    expectedRevision: ObjectRevision,
    body: Stream<Uint8Array, E, R>,
    options?: ObjectReplaceOptions
  ) => Effect<ObjectWriteResult, E | ObjectWriteError, R>

  readonly updateMetadata: (
    name: ObjectName,
    expectedRevision: ObjectRevision,
    metadata: ObjectMetadataPatch
  ) => Effect<ObjectMetadataWriteResult, ObjectWriteError>

  readonly delete: (
    name: ObjectName,
    expectedRevision: ObjectRevision
  ) => Effect<ObjectDeleteResult, ObjectWriteError>

  readonly watch: (options: ObjectWatchOptions) => Stream<ObjectEvent, ObjectWatchError>
}
```

`create` and `replace` use the server's expected-last-subject-sequence header and never implement
CAS as a client-side pre-check. The header value is the global stream sequence of the last message
on this object's metadata subject, not a per-subject ordinal. `create` uses zero for a never-seen
metadata subject; logical recreation after a tombstone first reads the tombstone and then commits
against that exact revision. A concurrent live creation wins and the guarded recreation conflicts.
No ordinary method omits the condition, and no write automatically retries a chunk or metadata
publish whose server acceptance is unknown.

The same rule applies to metadata update and delete. Link creation is create-only at the
destination metadata subject. Rename/move is deferred because destination creation plus source
removal spans two subjects and cannot be represented as one per-subject CAS commit.

`ObjectReader.body` is single-use. Releasing its Scope cancels the backing consumer and terminates
the body exactly once. Taking a prefix, interrupting, or releasing early is cancellation—not a
verified read success.

## Upload state machine

```text
Preparing
  -> Uploading(new ObjectId; 0..n chunk PubAcks)
  -> PublishingMetadata(expected revision?; digest; size; chunks)
     -> Committed(metadata PubAck)
        -> CleanupComplete | CleanupPending        (successful result)
     -> CommitUnknown                              (typed ambiguous result/error)

Preparing | Uploading
  -> InputFailed | Interrupted | ChunkRejected | ChunkOutcomeUnknown
  -> PurgeNewChunks
     -> FailedClean | FailedWithOrphans
```

Rules:

1. Input is pulled on demand. A byte chunker slices each input element and awaits the corresponding
   chunk PubAck; it never accumulates multiple object chunks behind an unbounded queue.
2. A source failure or interruption before metadata publication attempts to purge the new chunk
   subject. Interruption remains interruption; cleanup evidence is emitted through diagnostics or
   a finalizer-safe recovery record rather than swallowed.
3. Chunk timeout is not a committed object. Retrying the same bytes without an idempotent chunk
   protocol could duplicate them, so the initial slice stops and cleans the whole new chunk
   subject.
4. Metadata is published only after input completion, final size/chunk count, and whole-object
   digest are known. `expectedRevision` is attached to this publish.
5. A definite metadata rejection (CAS conflict, sealed bucket, permission, capacity) leaves the
   previous object current and triggers new-chunk cleanup.
6. Once metadata was sent, a timeout/disconnect may mean it committed. `ObjectCommitUnknown`
   carries bucket, name, ObjectId, digest, expected prior revision, and the attempted metadata. It
   does not purge new chunks until reconciliation shows they are not current. Reconciliation has
   at least `CommittedCurrent`, `NotCommitted`, and `SupersededOrIndeterminate` results; seeing a
   different newer value does not prove whether this write briefly committed before replacement.
7. Interruption remains an interrupt cause rather than being caught into `ObjectCommitUnknown`.
   If it arrives after metadata may have been sent, an uninterruptible finalizer persists/emits the
   same recovery evidence and must not purge the new ObjectId. A future operation handle may make
   reconciliation directly awaitable, but an interrupted fiber cannot pretend it returned a typed
   outcome.
8. After metadata PubAck, the new entry is live. Purging the old ObjectId is best effort. Failure
   returns `cleanup: Pending` on a successful `ObjectWriteResult`; it cannot report write failure.

This technique prevents accidental overwrite only at the guarded metadata commit. Chunks uploaded
before a losing CAS are unreachable and cleaned; they never select the current object. Two writers
replacing revision `R` can both upload chunks, but only one metadata publish can observe `R` as the
last sequence for that subject. The loser receives a conflict and purges only its own new ObjectId.
CAS does not prohibit a later unconditional publish by another library or credential.

The proof also assumes `R` is exact. It is not enough to convert the official client's parsed
`number` with `BigInt`: for example, adjacent uint64 values above 2^53 can decode to the same
JavaScript number. A stale rounded revision could equal a different newer exact server revision and
make the expected-sequence header pass. The initial wrapper must therefore reject any revision for
which `Number.isSafeInteger` is false before returning it as mutation authority. Full-range CAS
requires a lossless response/header seam.

The ambiguity rule is intentionally stricter than the pinned NATS.js catch path, which purges the
new chunk subject after any caught metadata-publish failure. If the server accepted metadata but
the PubAck was lost, that purge can remove chunks referenced by the live metadata. The Effect
contract preserves the official success boundary while refusing to strengthen a timeout into
failure.

Two further specification/source deltas remain visible instead of being silently copied:

- ADR-20 says modified time is not stored in JSON and should come from the metadata message's
  server timestamp; the pinned NATS.js put serializes a client-generated `mtime` and `rawInfo`
  returns it. The private substrate can expose server time, so the conformance suite must choose
  the canonical `modifiedAt` and record parity behavior.
- ADR-20 says Object Info is always rolled up per subject. Pinned `put` and `delete` send the
  roll-up header, while pinned `link` and `update` publish metadata without it. The initial slice
  must test cross-client interoperability and decide whether all Effect mutations enforce the
  architecture rule.

## Read state machine and integrity

```text
LookupMetadata
  -> Absent | Deleted
  -> Link (resolve under explicit policy and depth/cycle bound)
  -> DataMetadata
     -> AcquirePullConsumer
     -> StreamAndHash
     -> EndObserved
     -> Verify { digest, total bytes, total chunks }
        -> Success | CorruptObject
```

- The backing consumer is pull-based with one finite positive `PullWindow`. A chunk window derives
  a worst-case local byte bound from the object's effective maximum chunk size; a byte window uses
  the substrate's byte-limited fetch. Refill threshold is lower than capacity.
- The Stream's only success is normal exhaustion after all three integrity checks. Missing chunks,
  extra chunks, wrong total size, malformed/unsupported digest, heartbeat/consumer failure,
  permission loss, and digest mismatch are typed terminal failures.
- Bytes emitted before terminal verification are provisional. This is inherent in a streaming
  whole-object digest. `getVerifiedBytes(maxBytes)` and the Schema adapter collect only to an
  explicit bound and return nothing until verification succeeds.
- The zero-byte representation must be settled by a conformance test: NATS.js emits one empty
  `Uint8Array`, while an idiomatic Effect body might emit zero chunks. Object size and digest are
  identical, but element framing is observable, so the slice may not normalize it silently.
- Object links are resolved only under `followLinks: Never | ObjectLinks { maxDepth }`. Bucket
  links are metadata, not byte bodies. Cross-bucket resolution opens an existing bucket and never
  creates one as a side effect of read.

## First-class Effect Schema integration

### Schema-backed protocol/domain data

Every external JSON boundary decodes from `unknown` with the pinned Effect v4
`Schema.decodeUnknownEffect`. Branded Schemas cover bucket/name/revision/digest/size/count, and
tagged Schemas cover entry variants, watch events, cleanup results, and expected failures.
`ObjectMetadataInput` is a separate Schema so server-computed fields are unrepresentable as input.

Expected failures use repository-style `Schema.TaggedError` classes and preserve upstream causes,
JetStream status/API codes, bucket/name, operation phase, and commit certainty. Minimum distinct
families are:

- validation/metadata decode;
- bucket/object absence where absence is not modeled as `Option`;
- permission, unsupported feature, sealed bucket, and capacity rejection;
- conflict with expected and observed revisions when known;
- chunk write failure and pre-commit cleanup pending;
- metadata `ObjectCommitUnknown`;
- consumer/transport failure and `CorruptObject`;
- `ObjectTooLarge`, Schema encode/decode, and format failure;
- watch lag with last observed metadata revision.

### Typed object values

A generic Schema does not define a byte wire format. The proposed adapter accepts a Schema whose
encoded side is `Uint8Array`; named constructors compose common formats:

```ts
interface ObjectValueSchema<S extends Schema.Top> {
  readonly schema: S // S["Encoded"] is Uint8Array
  readonly maxEncodedBytes: ByteSize
  readonly mediaType: Option<string>
}

declare const bytes: <S extends SchemaWithUint8ArrayEncoded>(
  schema: S,
  options: { readonly maxEncodedBytes: ByteSize; readonly mediaType?: string }
) => ObjectValueSchema<S>

declare const json: <S extends Schema.Top>(
  schema: S,
  options: { readonly maxEncodedBytes: ByteSize; readonly mediaType?: string }
) => ObjectValueSchema<JsonBytesSchema<S>>
```

Typed write runs `Schema.encodeEffect` and enforces `maxEncodedBytes` before publishing any chunk.
Typed read fully consumes and verifies raw bytes within that bound, then runs
`Schema.decodeEffect`. The returned Effect retains `S["EncodingServices"]` or
`S["DecodingServices"]` in its environment. Schema failures are typed expected failures, never
defects.

This deliberately materializes one bounded value. A future `ObjectRecordCodec` may map
`Stream<A>` to an explicitly framed byte stream (for example length-delimited records), but it
requires its own framing, partial-frame, versioning, and terminal-integrity contract. It is not
smuggled into the whole-value Schema adapter.

Object metadata's native `Record<string, string>` remains a string map. Persisting arbitrary typed
metadata would require an explicit reserved key/format/version policy and is deferred; the library
does not hide JSON in user metadata automatically.

## Settings developers should see

### Resource provisioning

| Setting     | Public shape                      | Derived/diagnosed behavior                                                                                                                                      |
| ----------- | --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| storage     | `Memory` or `File`                | Preserve server support; file is the normal durable choice.                                                                                                     |
| replicas    | positive finite count             | Report requested/effective value, placement feasibility, quorum warning, and physical-capacity multiplier. Never describe one replica as HA.                    |
| placement   | cluster/tags option               | Compare with observed topology; warn when replicas can share a failure domain or placement is unsatisfied.                                                      |
| ttl         | `Unlimited` or `Finite<Duration>` | Maps to stream `max_age`, which ages chunks and metadata individually. Warn when TTL is not safely above maximum upload duration plus intended read window.     |
| maxBytes    | `Unlimited` or `Finite<ByteSize>` | Aggregate backing-stream limit, not per object. Include metadata, replacement overlap, and orphan/cleanup headroom. Compare with account/server storage limits. |
| compression | explicit boolean/profile choice   | Maps to S2 at-rest compression only when the server supports it; it is not wire compression.                                                                    |
| metadata    | validated string map              | Reject reserved `_nats` keys at the boundary and version-gate support.                                                                                          |

The following are fixed Object Store invariants and not profile knobs: stream/subject names,
metadata roll-up, `allow_direct = true`, Object Store subjects, and `discard = new`. Expert raw
stream overrides are a different capability because changing them can break Object Store
semantics.

### Per-write and per-read operation settings

| Setting                               | Contract                                                                                                                                                                                        |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| requested chunk size                  | `Default` (128 KiB) or positive finite bytes. Resolve to no more than connection `max_payload` and any observed stream message limit; expose requested and effective values.                    |
| maximum metadata bytes                | Positive finite preflight bound for encoded metadata plus required headers, checked against negotiated/deployment payload and control-line limits before chunks are published.                  |
| per-chunk PubAck timeout              | Applies to each chunk request, not the whole upload and not metadata commit. Timeout is acceptance-unknown for that chunk; no blind retry.                                                      |
| metadata commit timeout               | Separate semantic phase. Ambiguous expiry produces `ObjectCommitUnknown`. If the initial official adapter cannot configure it separately, resolved config reports that both values are coupled. |
| overall deadline / input idle timeout | Effect-level interruption policy, separate from per-request PubAck timeouts. The chosen cleanup/ambiguity state remains observable.                                                             |
| write condition                       | `CreateOnly` or `Replace(ObjectRevision)` on ordinary writes. Never nullable, omitted, or a client-side check. Unconditional overwrite is not in the first ordinary capability.                 |
| maximum object bytes                  | Optional positive client safety limit. Bucket `maxBytes` does not provide this. Unknown-length streams are counted during upload and cleaned if they exceed it.                                 |
| read pull window                      | `Chunks { capacity, refillAt }` or `Bytes { capacity, refillAt }`, finite and positive per ADR-0005. Report the derived worst-case local memory bound.                                          |
| link policy                           | Never follow, or follow object links with a positive maximum depth and cycle detection. Bucket links do not yield bodies.                                                                       |
| verified convenience bound            | Required finite byte limit for `getVerifiedBytes`, `putBytes`, and every whole-value Schema adapter.                                                                                            |
| watch buffer                          | Finite positive metadata-event capacity using `TerminateOnLag`; error includes last observed revision.                                                                                          |

### Operational envelope and capability diagnostics

Ask for or infer expected live bytes, object-size distribution, replacement rate, concurrent
uploads/reads/watches, maximum upload duration, retention horizon, and designed outage. Resolve
against connection `max_payload`, backing stream config, account memory/storage, maximum consumers,
HA asset limits, placement, server version, and API level.

Useful calculations and warnings:

```text
effectiveChunkBytes = min(requestedChunkBytes, maxPayload, observedStreamMessageLimit?)
chunkMessages       = ceil(objectBytes / effectiveChunkBytes)   // zero for empty body
normalPublishes     = chunkMessages + 1 metadata publish
replacementPeak     >= old live bytes + new live bytes + metadata/stream overhead
physicalStorage     ~= logical stored bytes * replicas          // planning estimate, not quota math
```

- Small chunks increase PubAck round trips and metadata count; large chunks increase per-read
  memory and retransmission cost. The normal pinned implementation awaits each chunk PubAck, so
  latency matters.
- A nearly full `maxBytes` bucket can read successfully but fail replacement because old and new
  chunks coexist until metadata commits and old chunks are purged. Surface a replacement-headroom
  warning rather than presenting capacity as live-object bytes.
- Sustained write/read demand beyond the slowest PubAck, storage, network, or consumer stage is not
  fixed by larger buffers. Report overload separately from outage absorption.
- Every read and watch consumes JetStream consumer capacity. Compare maximum concurrent resources
  with account/server limits and prove Scope release deletes/stops consumers.
- Cleanup-pending outcomes increase storage without changing the current logical value. They need
  metrics and an operator-visible reconciliation queue before production readiness.
- Client-side CAS is opt-in at each JetStream publish, not a bucket lock. If all holders of direct
  Object Store write credentials are not trusted to use guarded writes, surface a deployment
  warning and require a controlled writer service/account for enforceable overwrite policy.

## Outcome ledger

| Operation/phase          | Success                                       | Required non-success branches                                                                                                                                                         |
| ------------------------ | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| open/create bucket       | scoped bucket capability                      | invalid name/config, absent, permission, JetStream unavailable, unsupported server/API/crypto, incompatible existing stream, transport                                                |
| metadata lookup          | `Option<ObjectInfo>`                          | malformed JSON/headers/time, invalid name/digest/link, permission, server status, transport, sequence outside adapter-safe range                                                      |
| list                     | finite Stream of validated entries            | per-entry decode, pagination/consumer, cancellation, permission, transport; deleted include/exclude is explicit                                                                       |
| upload input/chunks      | progress to metadata phase                    | source typed failure, interruption, size bound, chunk PubAck success/reject/timeout/disconnect, partial cleanup success/failure                                                       |
| guarded metadata publish | committed write result                        | CAS conflict, sealed/capacity/permission rejection, malformed response, timeout/disconnect with unknown commit, interruption plus recovery evidence, reconciliation current/old/other |
| post-write cleanup       | `Complete` or `Pending` on success            | purge permission/transport/server failure; never retroactive write failure                                                                                                            |
| get                      | `Option<ObjectReader>`                        | absent/deleted, link policy/depth/cycle, malformed metadata/digest, consumer creation/status/permission/transport                                                                     |
| body Stream              | verified terminal success                     | missing/extra chunk, wrong size/count/digest, unsupported digest, heartbeat/consumer deletion, reconnect gap, cancellation/interruption, exactly-once finalization                    |
| typed value              | verified decoded value                        | raw read errors, finite bound exceeded, byte-format error, Schema decode/encode failure with services preserved                                                                       |
| delete                   | `NotFound` or `Deleted(cleanup)`              | metadata rejection/commit unknown, old-chunk cleanup pending; committed tombstone is not undone by purge failure                                                                      |
| metadata update          | committed metadata outcome                    | absent/deleted, CAS conflict, metadata commit unknown, permission/transport; no unguarded patch                                                                                       |
| rename/move              | deferred                                      | two-subject atomicity is unavailable from one expected-last-subject-sequence condition                                                                                                |
| link                     | committed link metadata                       | source missing/deleted/link, destination conflict, bucket link vs object link, CAS/permission/commit unknown                                                                          |
| watch                    | scoped Stream of snapshot/update entry events | bounded lag, decode, consumer deletion, permission, disconnect/reconnect policy, cancellation; no invented snapshot-done data event without official evidence                         |
| seal/destroy             | explicit admin result                         | absent, permission, commit/status unknown, transport; destroy is never on the ordinary data capability                                                                                |

## Stream and lifecycle contract

- Every body reader, list iterator, and watch is Scope-owned. Finalizers are registered before the
  value is exposed, are idempotent, and stop/delete the server consumer before completing locally.
- Read production can suspend because the client owns the pull window. Watch callbacks cannot
  suspend; on the triggering over-capacity event, the adapter records the last admitted revision,
  stops the consumer, and terminates after all admitted events with `ObjectWatchLagged`.
- Parser/consumer delivery is serialized and object chunks retain server order. No concurrent hash
  updates or downstream delivery.
- No public `Stream` is implemented by `runCollect`, and no upstream unbounded iterator is hidden
  behind a bounded downstream queue.
- Interruption is preserved. Expected NATS, integrity, and Schema failures remain typed; causes are
  not swallowed and no cleanup finalizer converts interruption into a success or ordinary error.

## TDD and proof gates

Confirmed seams for the first implementation proposal are the pure upload/read state machines,
the private Object Store JetStream port plus scripted interpreter, the metadata Schemas, and—only
after ADR acceptance—the public capability. Work stays vertical: one failing behavior test, the
minimal implementation, then the next branch.

Minimum deterministic gates:

1. Chunking across empty, exact-boundary, split, coalesced, and oversized source elements; digest,
   size, count, PubAck order, and maximum retained bytes.
2. Source failure/interruption and every chunk failure before metadata; cleanup is attempted once,
   the original cause/interruption is preserved, and orphan status is exact.
3. Metadata accepted by the scripted server but PubAck dropped: result is commit-unknown, live
   chunks are not purged, reconciliation can prove success, old value, or conflicting value.
   Interruption after send preserves interruption, persists the same recovery evidence, and also
   leaves live chunks intact.
4. Metadata PubAck followed by old-chunk purge failure: operation succeeds with `cleanup: Pending`.
   The analogous guarded delete/metadata-update cases receive the same gate.
5. Against each declared real-server version, race two creates and two replacements from the same
   revision: exactly one metadata publish commits, the loser reports conflict, the winner's chunks
   remain, and the loser purges only its own chunks. Repeat for guarded update and delete. Also
   demonstrate that a separately authorized unconditional publisher can overwrite later, so the
   library never claims bucket-enforced CAS.
   The revision-boundary test uses `2^53 - 1`, `2^53`, and an aliasing pair above 2^53; only exact
   safe revisions may reach the expected-sequence header.
6. Pull-window saturation, slow downstream, interruption, consumer deletion, reconnect, missing
   chunks, extra chunks, size/count mismatch, digest mismatch, and exact leak counters.
7. Watch saturation proves triggering-event disposition, admitted-event ordering, terminal lag
   error, last revision, no post-release enqueue, and no latent blocked offer.
8. Schema encode failure produces zero JetStream publishes. Size-bound failure is deterministic.
   Schema decode occurs only after verified completion and retains required services in compile-time
   channel tests.
9. Real `nats-server` tests cover Object Store subjects/config, roll-up, deletion, links, TTL,
   `maxBytes` replacement headroom, replica/placement status, seal, permissions, and server-version
   gates. Fault tests cover leader loss and lost replies.
10. Repeated-suite flake, adversarial metadata/names/digests, live-dependency interruption, and
    bounded-growth checks pass with `it.effect`, `Deferred`, `Queue`, `Latch`, `Ref`, and
    `TestClock`; no sleeps adjudicate correctness.

## Blockers and open decisions

1. **Revision fidelity blocker.** Accepted ADR-0004 currently claims `BigInt(number)` is lossless
   for official NATS.js sequence values. It is not lossless once JSON parsing has rounded a value
   above `Number.MAX_SAFE_INTEGER`; pinned `ObjectInfo.revision` is `number`. Before Object Store
   types ship, ADR-0004 must be corrected. This proposal chooses a branded safe-integer
   compatibility revision with typed failure for the first wrapper; a lossless lower wire boundary
   may later widen it. No unsafe value may authorize CAS, and this draft makes no `bigint` fidelity
   claim through the official numeric surface.
2. Accept or reject the private JetStream implementation of Object Store read/watch required for
   bounded producer-side behavior.
3. Decide whether cleanup-pending is returned in every committed mutation result, emitted only as
   diagnostics plus a maintenance journal, or both. It may not be hidden entirely.
4. Decide the zero-byte Stream element convention via an official-client conformance test.
5. Decide canonical modified time and universal metadata-rollup behavior for the two explicit
   ADR-20 versus pinned-NATS.js deltas above.
6. Decide reconnect policy for an in-progress pull read. Resume must prove chunk position and
   ordering; otherwise the stream fails and the caller restarts from metadata.
7. Decide the first supported Schema byte formats. Recommended initial set: direct bytes and
   UTF-8 JSON only, both with mandatory finite encoded-size bounds.
8. Decide whether an explicitly named unconditional parity capability is ever needed. It is not
   part of the ordinary first-slice API and cannot be introduced as an optional omitted condition.
9. Decide the deployment/permission profile for applications that need CAS policy enforced against
   non-Effect clients with direct Object Store write access.
10. Set profile defaults only after real-server load measurements report requested/effective chunk
    size, PubAck RTT, replacement peak, read memory, and consumer usage. The model supplies formulas,
    not universal numeric defaults.

## Exact source evidence inspected

- NATS architecture: bucket naming, default chunk size, stream layout/config, metadata structures,
  operations, delete/link rules — [ADR-20 lines 235-319 and 430-582](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-20.md#basic-design).
- NATS.js metadata and revision types —
  [`types.ts` lines 48-104](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/types.ts#L48-L104).
- Official subject-level optimistic concurrency rejects a publish whose expected last sequence does
  not match the last stream sequence on that subject —
  [`message.go` lines 1541-1575](https://github.com/nats-io/nats.go/blob/main/jetstream/message.go#L1541-L1575).
- NATS.js obtains an object's revision from the metadata message's stream sequence and sends that
  number as `Nats-Expected-Last-Subject-Sequence` on guarded metadata publish —
  [`objectstore.ts` lines 385-400](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L385-L400)
  and
  [`objectstore.ts` lines 514-534](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L514-L534).
- NATS.js result's body plus separate completion error —
  [`types.ts` lines 195-224](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/types.ts#L195-L224).
- Chunk-size resolution, sequential chunk PubAcks, metadata-last commit, old-chunk cleanup, and
  failure cleanup —
  [`objectstore.ts` lines 442-574](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L442-L574).
- Read/link/digest behavior and eager ReadableStream pump —
  [`objectstore.ts` lines 635-725](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L635-L725).
- Delete, update/rename, and watch queue behavior —
  [`objectstore.ts` lines 770-908](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L770-L908).
- Stream configuration and capability gates —
  [`objectstore.ts` lines 922-983](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L922-L983).
- Effect v4 effectful Schema decode/encode and service channels —
  [`Schema.ts` lines 1493-1554](https://github.com/Effect-TS/effect/blob/648f566dd259898e7697c7fcb796183ccbc474ab/packages/effect/src/Schema.ts#L1493-L1554)
  and
  [`Schema.ts` lines 1953-2015](https://github.com/Effect-TS/effect/blob/648f566dd259898e7697c7fcb796183ccbc474ab/packages/effect/src/Schema.ts#L1953-L2015).
