# JetStream KV slice scoping

Status: draft workbench material for the decision pass. Nothing below is an accepted decision;
implementation stays gated on the ADR list at the end, per the workbench rule that accepted
decisions become numbered ADRs before they constrain code. Facts cite the repository research
maps pinned to nats.js `3a7b736`; bucket and key naming facts live in
[jetstream-kv-bucket-naming.md](../research/jetstream-kv-bucket-naming.md).

## Slice boundary

The slice delivers a typed KV capability over an internal JetStream substrate: open a bucket,
read, write with the full conditional-write family, delete, purge, watch, and iterate keys.

In scope:

- `Kv` service: open (and create/bind) a bucket, returning an immutable `KvBucket` capability.
- Reads: `get` (missing key is `Option.none`), per-key `history`, bucket `status`.
- Writes: unconditional `put`, `create` (fails if present), `update` (compare-and-set on an
  expected revision), soft `delete`, `purge`.
- Watch: scoped `Stream` of KV events with Put/Delete/Purge distinguished, initial-value and
  live phases distinguishable, resumable from a revision.
- Key iteration as a `Stream<KvKey>`, filterable.
- Branded naming types: `KvBucketName`, `KvKey`, `KvKeyFilter` — the naming deep dive defines
  their exact grammars.

Out of scope for the first slice (tracked, not dropped):

- Mirrors/sources and cross-domain buckets — naming interactions are recorded in the research
  doc so the branded types do not preclude them, but no API is exposed.
- Key codecs (subject-transforming), compression, placement/replica management beyond
  pass-through configuration.
- Object Store (separate slice; shares the JetStream substrate).

## Position in the stack

KV is a materialized view over one JetStream stream; the official client keeps it a thin layer
over the JetStream API (research map: KV section). This slice therefore consumes a private
JetStream substrate service — stream management, publish-with-expectations, direct/last-per-subject
reads, and ordered push/pull consumption — and never reaches around it to Core. The substrate
service is an internal seam, testable per the TDD skill's agreed-internal-seam rule, and is the
part of item 5 of the workbench decision list this slice forces into concrete form.

## Operations and outcome ledger

Every operation ships with a bidirectional outcome ledger (stream-discipline applicability gate):
each official response, status, error, and wire branch maps to exactly one typed outcome, and each
typed outcome maps back to the official behaviors that produce it. The ledger skeleton below names
the outcome families; the cells are completed against the pinned official sources during
implementation, operation by operation.

| Operation          | Success           | Typed errors (minimum)                                                       | Notes                                                               |
| ------------------ | ----------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `open`             | `KvBucket`        | `KvOpenError` (bucket invalid/absent/config-mismatch), transport, permission | bind-vs-create is an explicit option, not inferred                  |
| `get`              | `Option<KvEntry>` | `KvReadError`, `DecodeError`                                                 | absent key is success `none`, never an error                        |
| `history`          | `Stream<KvEntry>` | `KvReadError`, `DecodeError`                                                 | terminal: end of recorded history                                   |
| `put`              | `Revision`        | `KvWriteError`, `EncodeError`                                                | no automatic retry; delivery-ambiguity note below                   |
| `create`           | `Revision`        | `KvConflictError`, `KvWriteError`, `EncodeError`                             | conflict is its own error, never collapsed to boolean               |
| `update`           | `Revision`        | `KvConflictError`, `KvWriteError`, `EncodeError`                             | expected revision is the server-side CAS header, not a client check |
| `delete` / `purge` | `void`            | `KvWriteError`                                                               | soft delete and purge stay distinct operations                      |
| `watch`            | `Stream<KvEvent>` | `KvWatchError`, `DecodeError`                                                | phases and terminals below                                          |
| `keys`             | `Stream<KvKey>`   | `KvReadError`                                                                | streaming by default, never a collected array                       |
| `status`           | `KvStatus`        | `KvReadError`                                                                |                                                                     |
| `destroy`          | `void`            | `KvAdminError`                                                               | destructive; separate capability surface decision                   |

Contract rules carried from the architecture research and binding on this slice:

- CAS conflicts are `KvConflictError`; `create` and `update` are never automatically retried,
  because retrying with stale expectations changes their meaning.
- `put` retry is prohibited until an ADR supplies an idempotency argument; a transport failure
  leaves delivery ambiguous.
- Decode failures surface on the typed error channel of the reading operation; they never become
  defects.
- `KvEvent` is a tagged union preserving operation kind, revision, and entry metadata.

## Watch discipline

Watches are the stream-discipline hot spot. Constraints, all from the applicability gate and the
official implementation facts already pinned:

- The official client implements watch as an ephemeral ordered push consumer with heartbeats and
  flow control, no acks. The substrate service must expose enough of that for the watch stream to
  carry all observable facts (entry, revision, operation, phase marker) through one seam.
- Watch queues are bounded with an explicit, contract-stated overflow behavior. `Queue.dropping`
  is not a default for KV watches; silent loss requires an accepted ADR defining a lossy
  observation API.
- "Initial values done" is a distinct, observable stream event, not an inferred condition. This
  is ADR-8-mandated, not invented: "Internal to the watcher you reach this state the first time
  any message has a `Pending==0` ... The signal must always be sent"
  (`nats-architecture-and-design@e23afc3:adr/ADR-8.md:419-422`), including on an empty bucket
  (immediate signal; conformance-tested against nats-server v2.14.5).
- Watch termination distinguishes: consumer deleted, bucket deleted, connection lost
  (reconnect/resume policy is an open decision), and normal scope interruption. Interruption of a
  pending watch completes in bounded time — the iterator-bridge proof gate from the stream skill
  applies to whatever adapter path the wrap-vs-wire decision selects.
- Resumability: watch accepts a start revision so a consumer can resume without gaps; whether
  gap-free resume is guaranteed across reconnect is an open NATS-semantics question and stays in
  the ledger as unproven until tested against a live server.

## Naming domain model

The naming deep dive ([jetstream-kv-bucket-naming.md](../research/jetstream-kv-bucket-naming.md))
carries the verified grammars, the bucket-to-stream and key-to-subject mappings, reserved names,
limits, and the spec-versus-implementation deltas. The slice consumes it as follows:

- `KvBucketName` and `KvKey` are parse-don't-validate branded schemas at the public boundary.
  Internal code never constructs them by cast, and every subject/stream-name derivation goes
  through one total function per direction so the mapping is testable in isolation.
- Validation strictness (exact cross-client canonical grammar versus server-parity leniency) is
  ADR material; the research doc's canonical-rules section is the input to that decision.
- Adversarial naming inputs from the research doc's edge-case inventory become the Phase 4b
  adversarial test corpus for the branded schemas and the mapping functions.

## Test plan

Seams, per the TDD skill: the public `Kv`/`KvBucket` surface once exported, and two agreed
internal seams before that — the naming module (branded schemas plus mapping functions) and the
JetStream substrate interface (scripted interpreter in tests, live adapter in integration).
Deterministic tests run against the scripted substrate with `TestClock`; naming tests are pure.
Live `nats-server` integration covers CAS semantics, watch phases, and the naming edge cases the
server alone adjudicates. The stream-discipline Phase 4 passes (flake loop, adversarial inputs,
memory, witness audit) gate completion of every stream-bearing operation.

## Open decisions this slice feeds into the ADR pass

1. Wrap the official `@nats-io/kv`/`@nats-io/jetstream` packages versus implement over the
   repository's own substrate — the standing wrap-vs-wire decision; this slice's substrate
   interface is written so either adapter can satisfy it.
2. Bucket/key validation strictness and the exact branded grammars (input: research doc
   canonical rules).
3. Watch reconnect/resume behavior across connection loss. (The overflow contract is decided:
   ADR-0005 makes watch overflow a terminal typed `KvWatchLagged` carrying the last observed
   revision, resumable via `fromRevision`.)
4. Revision representation (`bigint` versus `number`) — follows the wire-range facts in the
   research doc; no assumption of `number` until checked.
5. Whether `destroy` (bucket deletion) sits on `KvBucket` or a separate admin capability.
6. Deferred interface idea (ship-first ruling, ADR-0002): an STM-style atomic-update surface —
   a `modify`/transaction combinator built over the CAS primitives — layers on later without
   changing the substrate; the first slice ships plain `create`/`update`/`put`.
