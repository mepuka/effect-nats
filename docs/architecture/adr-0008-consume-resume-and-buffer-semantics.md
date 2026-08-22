# ADR-0008: Consume resume and buffer semantics

Status: accepted, 2026-08-22. Ruled by the project owner for the subscriber-model
implementation slice; this decision refines ADR-0004 and ADR-0005.

## Context

A lag error must carry a position that can be used without arithmetic. Adding one to a revision
would violate ADR-0004's comparison-and-equality rule and can cross the exact range of the live
client's number-typed start option. The substrate also accepted zero and fractional queue
capacities even though neither has a coherent operational meaning.

The two interpreters have an intentional implementation difference. Live replay enters the same
bounded queue as live delivery and can terminate on lag. Memory replay is emitted directly from
the registration snapshot and cannot overflow that subscription queue. Memory `PullWindow`
backpressure also occurs while `publish` holds one global mutation permit.

## Decision

`StartPosition` has two distinct sequence forms:

- `FromSequence(n)` is inclusive and emits entries whose sequence is greater than or equal to
  `n`.
- `AfterSequence(n)` is exclusive and emits entries whose sequence is greater than `n`.

Lag recovery uses `AfterSequence(lastDeliveredSequence)`. KV exposes the same distinction as
inclusive `fromRevision` and exclusive `afterRevision`; setting both is a programmer error and
dies with `ConflictingWatchResume`. Recovery from `KvWatchLagged` uses
`afterRevision: lastRevision`. Resume positions are compared, never incremented.

The live adapter implements an exclusive start using the server's inclusive
`DeliverPolicy.StartSequence` at the boundary, then drops entries at or below that boundary.
Those dropped entries may establish `CaughtUp`, but never advance the delivered sequence.
`AfterSequence(0)` uses `DeliverPolicy.All`, which is equivalent because stream sequences are
positive and the server rejects a zero start sequence. Both inclusive and exclusive numeric
starts pass the same exactness guard before conversion to the client's number-typed option.

`ConsumerLagged.lastDeliveredSequence` is the sequence of the last entry delivered downstream.
Every entry already buffered is delivered before the terminal lag failure is observed. The live
adapter may lag during replay; the memory interpreter's snapshot replay cannot.

Every `ConsumeBuffer.messages` value is a positive integer. A zero, negative, fractional, or
non-finite capacity dies with `InvalidConsumeBuffer` before an interpreter acquires subscription
resources. KV watch, keys, and history pass through the same substrate validator.

The memory interpreter's `PullWindow` semantics remain deliberately strong and visible: when its
bounded handoff is full, `publish` suspends while holding the global mutation permit. Until the
consumer pulls, unrelated operations on other memory streams can also stall. This is a test
interpreter limitation, not a claim about live JetStream publishers.

## Consequences

- Lag recovery has no off-by-one arithmetic, duplicate delivery, or exactness-boundary overflow.
- Inclusive user-selected replay remains available without being overloaded as a recovery API.
- Invalid capacities are defects rather than additions to every typed consumer error union.
- Tests must exercise resume behavior through the shared memory/live conformance suite and keep
  the memory-only global-stall behavior explicit.
- A future memory interpreter may replace the global permit with narrower coordination, but that
  would be a deliberate observable-model change with new tests and an ADR update.
