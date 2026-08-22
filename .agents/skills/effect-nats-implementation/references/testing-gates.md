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
- request timeout/cancellation stopping only the wait, plus duplicate responder invocation risk on
  retry;
- subscription ordering, unsubscribe limits, slow consumer policy, and finalization;
- one `SUB` per stream run and exactly one `UNSUB` after `take`, failure, interruption, overflow,
  and unsubscribe/interruption races;
- no post-release delivery, no latent blocked offer, and the documented ordering between admitted
  messages and `SlowConsumerError`;
- flush as a current-connection PING/PONG barrier for commands sent before its PING, without
  claiming subscriber processing or reconnect recovery;
- drain rejecting new subscriptions, draining existing subscriptions, then rejecting publishes,
  flushing pending outbound commands, and closing;
- explicit wrapper timeout/forced-close behavior around drain;
- reconnect attempt scheduling, exhaustion, fresh handshake, subscription restoration, in-flight
  request failure, Core message-loss exposure, and interruption;
- Core publish uncertainty across disconnect without claiming acknowledgement.

## KV tests

Cover:

- create, put, get, delete, purge, true absence, and empty payload behavior;
- compare-and-set success, revision conflict, and timeout-after-possible-commit uncertainty without
  weakening the expected revision;
- returned `DEL` and `PURGE` entry interpretation;
- last-value, all-history, and updates-only watch modes;
- `isUpdate`, resume revision, cancellation, reconnect, and an empty bucket producing no synthetic
  initialization event;
- history and TTL removal of values or markers without requiring another watch notification,
  observed against declared server versions;
- bucket naming/config decoding and JetStream API failures.

The first-party Effect `KeyValueStore` is only a service/error-shape analogue. It cannot prove NATS
KV watch, revision, history, or tombstone semantics.

## Object Store tests

Cover:

- empty, single-chunk, multi-chunk, and boundary-sized objects;
- PubAcked chunks followed by pre-metadata failure/interruption and attempted chunk cleanup;
- metadata publication only after full input consumption and digest computation;
- metadata commit making the replacement live even if old-chunk purge subsequently fails;
- digest computation and verification;
- missing chunks, wrong digest, partial upload, interruption, cleanup, and early read cancellation
  without verified-success reporting;
- replacement, deletion, links, metadata update, and watch behavior if exposed;
- reconnect and retry at each permitted boundary, including reconciliation or previous-revision
  protection after uncertain metadata commit;
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
