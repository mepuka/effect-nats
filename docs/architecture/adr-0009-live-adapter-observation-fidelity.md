# ADR-0009: Live adapter observation fidelity

Status: accepted, 2026-08-22, ruled by the project owner.

## Context

The Lean sequential-trace live replay exposed two adapter representations that differed from the
declared JetStream seam. NATS Server reports unlimited per-subject history with a negative sentinel,
while the seam and Lean declaration use only `0`. The official client also carries compare-and-set
expectations as NATS message headers, which the server stores even though the seam defines them as
publish options rather than publisher content.

The subscriber-model Verify pass exposed a third difference. The live adapter put a registration-
time `CaughtUp` marker in the same queue as entries. The queue admission guard counted that marker,
so an undrained marker reduced `ConsumeBuffer.messages` by one. The memory interpreter already emits
its registration marker outside the bounded entry queue.

## Decision

The live adapter normalizes `max_msgs_per_subject` when reading stream configuration. An absent or
non-positive server value reports as `maxMessagesPerSubject: 0`; a positive value reports unchanged.
Creation continues sending the seam value. This decision gives no new meaning to a negative caller
value: the memory interpreter still treats it as unlimited, while the Lean model rejects it at its
typed validation boundary.

`StoredMessage.headers` are exactly the publisher-supplied content headers. The live adapter removes
DIRECT.GET metadata and the full `Nats-Expected-*` publish-precondition family on every stored-message
read path. It retains `Nats-Rollup`, including `Nats-Rollup: sub`, and every other publisher header.
Expectation sequences remain options and are compared without inventing a successor sequence.

KV does not expose these expectation headers. `KvEntry` has no arbitrary header field,
`operationOf` reads only `KV-Operation`, and the create and update paths pass compare-and-set values
through `expectedLastSubjectSequence`. The JetStream read normalization therefore removes the option
headers before KV interprets a stored message; no KV change is required.

`ConsumeBuffer.messages` bounds entries. A `CaughtUp` known at registration because
`num_pending === 0` is emitted before the bounded queue for both `PullWindow` and `TerminateOnLag`.
The live adapter keeps the pre-entry `lastDelivered` value at `0n`; it is unobservable for every
valid capacity because the first entry can now be admitted before the registration marker is pulled.

A `CaughtUp` discovered later, when pending replay drains, remains in the live queue. It must follow
the replay entries already admitted there. Until the consumer pulls it, that single marker can occupy
a queue slot. This is the one remaining live-only replay-time effect: live replay may lag or pause a
pull loop where the memory interpreter's snapshot replay does not. Removing it would weaken the
declared entry-before-`CaughtUp` ordering, so ADR-0008's recorded live replay-lag difference remains.

## Consequences

- Shared conformance cases C16-C18 pin configuration normalization, publisher-header fidelity, and
  registration-time marker placement on both interpreters. The `PullWindow(1)` suspension twin is
  memory-only because a live publisher is decoupled from the adapter's pull queue by the server.
- The generated Lean fixture replays on memory and live with an empty override set. This is
  compatibility evidence for those histories, not a refinement proof or a general server claim.
- No memory-interpreter or Lean declaration changes are required. Replay-time ordering and the
  physical live queue remain an explicit interpreter difference rather than a hidden strengthening.
