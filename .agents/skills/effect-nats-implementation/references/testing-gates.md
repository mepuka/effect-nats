# Testing gates

Use the smallest layer that can prove the claim. A passing unit test does not establish a server
compatibility claim, and an integration test does not replace deterministic lifecycle tests.

## Universal gates

Every changed operation covers:

- success and its exact result semantics;
- each expected typed failure introduced by the change;
- interruption while blocked;
- finalizer behavior after success, failure, and interruption;
- buffer saturation or in-flight limit when the operation crosses a producer/consumer boundary;
- observability redaction for credentials or secrets when relevant.

Use `it.effect`. Coordinate fibers with `Deferred`, `Queue`, `Latch`, or another observable state
transition. Advance scheduled behavior with `TestClock`. A wall-clock timeout may guard a whole
test from hanging, but it is not proof of ordering or readiness.

## Pure protocol and codec tests

Cover:

- valid frames split at every meaningful chunk boundary;
- multiple frames in one chunk;
- malformed control lines, headers, lengths, and payloads;
- maximum control-line, header, payload, and buffered-byte limits;
- encoder/parser round trips where the protocol permits them;
- absence versus empty values and ASCII/UTF-8 edge cases;
- parser recovery or terminal failure exactly as specified.

Prefer generated inputs or compact fixtures over snapshots that obscure byte meaning.

## Connection and Core tests

Cover:

- connect, INFO, auth, CONNECT, PING/PONG, and first usable state;
- server error before and after readiness;
- request success, timeout, no responders, cancellation, and late reply cleanup;
- subscription ordering, unsubscribe limits, slow consumer policy, and finalization;
- flush as a protocol round trip rather than an alias for local enqueue;
- drain admission stop, bounded completion, timeout, and forced close;
- reconnect attempt scheduling, exhaustion, state restoration, and interruption;
- Core publish uncertainty across disconnect without claiming acknowledgement.

## KV tests

Cover:

- create, put, get, delete, purge, and missing key behavior;
- compare-and-set success and revision conflict without weakening the expected revision;
- tombstone and purge-marker interpretation;
- watch initial synchronization, updates, deletion, cancellation, and reconnect;
- history and TTL behavior against declared server versions;
- bucket naming/config decoding and JetStream API failures.

The first-party Effect `KeyValueStore` is only a service/error-shape analogue. It cannot prove NATS
KV watch, revision, history, or tombstone semantics.

## Object Store tests

Cover:

- empty, single-chunk, multi-chunk, and boundary-sized objects;
- metadata publication only after the chunk contract completes;
- digest computation and verification;
- missing chunks, wrong digest, partial upload, interruption, and cleanup;
- replacement, deletion, links, metadata update, and watch behavior if exposed;
- reconnect and retry at each permitted boundary without duplicating committed metadata;
- streaming without assembling the whole object in memory.

## Integration matrix receipt

For every server integration run, record:

- exact NATS Server version or image digest;
- enabled JetStream, storage, auth/TLS, clustering, and resource-limit configuration;
- client runtime and transport;
- test resource isolation and cleanup path;
- skipped scenarios and why.

Use at least the repository's declared minimum and current target server versions before making a
compatibility claim. Add clustered, constrained-resource, TLS/auth, and reconnect scenarios when
the changed behavior depends on them.
