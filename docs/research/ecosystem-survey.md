# TypeScript NATS ecosystem survey

Status: comparison evidence, not protocol authority\
Snapshot date: 2026-08-21\
Scope: seven open-source TypeScript libraries/applications plus official NATS.js as the baseline

## Executive result

The ecosystem has several useful pieces, but no third-party project is a complete blueprint for an
Effect v4-native Core + KV + Object Store client.

The strongest patterns to adapt are:

- a scoped connection service with a bounded drain and close fallback;
- subscriptions and watches represented as scoped `Stream` values, with iterator cleanup attached
  to stream lifetime;
- central settlement (`ack`, `nak`, `term`, delayed retry) instead of making every handler remember
  the protocol rules;
- distinct manager/client/bucket services and separate Node and WebSocket transport layers;
- raw-byte APIs underneath typed codecs, with decode failures in the typed error channel;
- `Option` for genuinely missing KV/object values, typed CAS failures, and streaming Object Store
  upload/download with digest verification;
- testcontainers-based server integration tests in addition to pure and fake-transport tests.

The most important gaps to avoid are equally consistent:

- `Stream.fromAsyncIterable` alone does **not** make the upstream NATS iterator bounded. The
  official queued iterator uses a growable in-memory array, so our bridge must specify capacity,
  overflow behavior, pending metrics, and slow-consumer handling.
- automatic JetStream publish retry is unsafe without a stable message ID: a lost PubAck leaves an
  ambiguous outcome and retry can duplicate the message;
- global promise/singleton connections are not a substitute for Effect scope and can retain a
  rejected initial connection forever;
- returning a stream from an `acquireRelease` effect is unsafe unless the resource lifetime is
  attached to consuming the stream;
- shutdown timeouts must actually force `close`; logging that a drain timed out is not enough;
- KV tombstones and Object Store stream/digest errors must not be collapsed into generic `null` or
  catch-all errors.

## Authority baseline: official NATS.js v3

The implementation target should follow official NATS.js, not the projects below. This survey pins
NATS.js at commit
[`3a7b736b175d9a387130210c88f851d71e1b014f`](https://github.com/nats-io/nats.js/commit/3a7b736b175d9a387130210c88f851d71e1b014f),
whose modular Core, JetStream, KV, Object Store, and Node transport packages are all version `3.4.0`
and Apache-2.0 licensed
([Core manifest](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/package.json),
[KV manifest](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/package.json),
[Object Store manifest](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/package.json)).

Four upstream details materially constrain our design:

1. A Core subscription is an async iterator, while Core `publish` returns `void`; `flush` and
   connection status are distinct operations. Status includes disconnect/reconnect, errors, stale
   connections, and slow-consumer events
   ([Core API](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/core.ts)).
2. The queued iterator stores pending values in a growable `yields` array and only exposes counts;
   converting it to an Effect stream does not push back into the NATS protocol
   ([iterator implementation](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/queued_iterator.ts)).
3. KV `get` may return an entry marked `DEL` or `PURGE`, watches/history are queued iterators, CAS
   uses revisions, and custom key and payload codecs affect subject compatibility and stored bytes
   ([KV types](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/types.ts)).
4. Object Store `get` returns both a `ReadableStream` and a separate completion-error promise; puts
   are chunked streams and objects carry a whole-object digest. A wrapper must observe the stream
   and completion error together
   ([Object Store types](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/types.ts)).

## Projects reviewed

| Project                            | Pinned revision and maintenance signal                                                                                                               | Runtime/package signal                                                                                    | Licence signal                                                                    | Why it matters                                                                                         |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `spiko-tech/effect-messaging`      | [`cd97cbd`](https://github.com/spiko-tech/effect-messaging/commit/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b), committed 2026-06-03; repo not archived | Published-style `@effect-messaging/nats` 0.7.6; Effect 3.19+; modular NATS 3.4; Node and WebSocket layers | MIT                                                                               | Best mature Effect 3 precedent for layers, streams, tracing, retries, and settlement                   |
| `erikshestopal/effect-nats`        | [`e4fc256`](https://github.com/erikshestopal/effect-nats/commit/e4fc2565128f088566a70a8f9ec416bd98950f78), committed 2026-07-08; repo not archived   | Private 0.0.0 design project; Effect 4 beta.93; Bun; modular NATS 3.4                                     | No repository licence detected                                                    | Closest API sketch to our exact Core + KV + Object Store scope, but not reusable code                  |
| `jkonowitch/hex-effect`            | [`9e95924`](https://github.com/jkonowitch/hex-effect/commit/9e959248c3e23465eb2135470ca30167bb5581a1), committed 2024-10-18                          | `@hex-effect/infra-libsql-nats` 0.2.3; Effect >=3.9; prerelease NATS v3 packages                          | No package/repository licence detected                                            | Useful durable-consumer supervision and settlement policy precedent; stale and error-erasing in places |
| `zetsuchan/angstromscd-monorepo`   | [`464be04`](https://github.com/zetsuchan/angstromscd-monorepo/commit/464be0466e5737ff45a46ebe07669ac03710ab59), committed 2025-12-26                 | Private Bun app; Effect 3.19; legacy monolithic `nats` 2.26                                               | MIT                                                                               | Compact negative control for scope and singleton lifecycle mistakes                                    |
| `lithqube/nuxt-nats`               | [`b5616b7`](https://github.com/lithqube/nuxt-nats/commit/b5616b7946129df3e76792f5ce1056eebbc7c55a), committed 2026-08-08                             | 0.1.0-beta.1; Node >=20; modular NATS 3.4; Nuxt/Nitro                                                     | MIT                                                                               | Broad Core, JetStream, KV, and Object Store integration with real container tests                      |
| `HorizonRepublic/nestjs-jetstream` | [`2c53eba`](https://github.com/HorizonRepublic/nestjs-jetstream/commit/2c53eba348dd1c043de6deea454942a0295f50b5), committed 2026-08-12               | Published package 2.13.0; Node >=22; modular NATS 3.4; large unit/integration suite                       | Manifest says MIT; checked-in licence still contains placeholder copyright fields | Strong operational precedent for connection recovery, settlement, concurrency, shutdown, and tests     |
| `KLogicHQ/nats-console`            | [`77a88a6`](https://github.com/KLogicHQ/nats-console/commit/77a88a64df87c4efe41dbe15ab9358297342b4cc), committed 2025-12-24                          | Private Node >=20 app; legacy monolithic `nats` 2.28                                                      | Apache-2.0                                                                        | Useful multi-cluster and health-check application patterns; weak error boundaries                      |

Dates above are commit timestamps of the pinned revisions, not promises of ongoing maintenance.
Projects without a clear licence are evidence only; do not copy their code.

## 1. `@effect-messaging/nats`

This is the best maintained Effect precedent. It exposes `NATSConnection` as a `Context` service,
constructs it with `Layer.scoped`, and drains the raw connection in a finalizer. Core subscriptions,
request-many responses, connection status, and NATS queued iterators become Effect `Stream` values
([connection](https://github.com/spiko-tech/effect-messaging/blob/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b/packages/nats/src/NATSConnection.ts),
[subscription](https://github.com/spiko-tech/effect-messaging/blob/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b/packages/nats/src/NATSSubscription.ts),
[queued iterator](https://github.com/spiko-tech/effect-messaging/blob/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b/packages/nats/src/NATSQueuedIterator.ts)).

Worth adapting:

- transport-specific Node and WebSocket layers around one service contract;
- connection and subscription metrics (`pending`, `processed`, `received`) retained alongside the
  stream;
- Effect `Schedule` accepted by publishers, with the default set to `Schedule.stop` rather than an
  implicit retry;
- OpenTelemetry producer/consumer spans and header propagation;
- JetStream handlers return an explicit settlement value, and integration tests cover `ack`,
  delayed `nak`, `term`, failure redelivery, handler timeout, and interruption
  ([publisher](https://github.com/spiko-tech/effect-messaging/blob/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b/packages/nats/src/NATSPublisher.ts),
  [JetStream subscriber tests](https://github.com/spiko-tech/effect-messaging/blob/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b/packages/nats/test/JetStreamSubscriber.test.ts)).

Do not adopt unchanged:

- the finalizer awaits `drain()` with no visible timeout/close fallback;
- wrapping `QueuedIterator` directly as `Stream.fromAsyncIterable` leaves the upstream growable
  queue unbounded;
- most failures are resource-level wrappers containing `reason` plus `unknown` cause, which is too
  coarse for permission, timeout, closed, draining, stale revision, and digest failures;
- the pinned package has no KV or Object Store surface
  ([manifest](https://github.com/spiko-tech/effect-messaging/blob/cd97cbd9b2a03b7f4040f094be0c4ffc6bb49c7b/packages/nats/package.json)).

## 2. `erikshestopal/effect-nats`

This is the closest independent design to the requested product. It uses Effect v4-style
`Context.Service`, `Layer`, typed tagged errors, `Config.Wrap`, redacted credentials, scoped
connections, scoped subscription iterators, and separate transport connectors. Connection release
tries `drain`, bounds it to five seconds, falls back to `close`, and independently bounds
subscription drain
([client](https://github.com/erikshestopal/effect-nats/blob/e4fc2565128f088566a70a8f9ec416bd98950f78/src/NatsClient.ts),
[iterator bridge](https://github.com/erikshestopal/effect-nats/blob/e4fc2565128f088566a70a8f9ec416bd98950f78/src/internal/iterator.ts)).

Worth adapting conceptually:

- keep raw Core publish/subscribe/request operations small and expose honest, operation-specific
  error unions;
- attach iterator acquisition/release to `Stream.scoped`, not to an effect that merely returns a
  stream;
- model request-many termination (`count`, `stall`, `sentinel`, timer) in the Effect API;
- expose KV as a native bucket service and optionally adapt it to Effect's `KeyValueStore` only at
  a higher layer;
- represent missing values with `Option`, preserve revision/CAS errors, and expose watch/history as
  streams;
- expose Object Store data as Effect streams, surface digest mismatch, and test multi-chunk transfer,
  failed input streams, watch, delete, seal, and missing buckets
  ([KV tests](https://github.com/erikshestopal/effect-nats/blob/e4fc2565128f088566a70a8f9ec416bd98950f78/test/NatsKv.test.ts),
  [Object Store tests](https://github.com/erikshestopal/effect-nats/blob/e4fc2565128f088566a70a8f9ec416bd98950f78/test/NatsObjectStore.test.ts)).

Limits:

- the package is private, version `0.0.0`, tied to one Effect beta and Bun toolchain, and has no
  detected licence
  ([manifest](https://github.com/erikshestopal/effect-nats/blob/e4fc2565128f088566a70a8f9ec416bd98950f78/package.json));
- its declarations are design evidence, not upstream compatibility evidence;
- direct async-iterator conversion still needs an explicit bounded queue policy in our project.

## 3. `@hex-effect/infra-libsql-nats`

The library acquires the NATS connection in a scoped layer, publishes with a stable event message ID,
creates durable consumers, supervises consumer fibers, interrupts them in a finalizer, and uses
`acquireUseRelease` to settle every handled message
([messaging source](https://github.com/jkonowitch/hex-effect/blob/9e959248c3e23465eb2135470ca30167bb5581a1/packages/%40hex-effect/infra-libsql-nats/src/messaging.ts)).

Useful ideas:

- a supervisor for all long-lived consumer fibers;
- stable event IDs passed as JetStream `msgID`;
- message settlement in release logic, so defects and interruption cannot skip settlement;
- durable consumer upsert and tests against a real container
  ([tests](https://github.com/jkonowitch/hex-effect/blob/9e959248c3e23465eb2135470ca30167bb5581a1/packages/%40hex-effect/infra-libsql-nats/src/test/messaging.test.ts)).

Avoid its policy defaults:

- typed handler failures are terminated while defects/interruption are negatively acknowledged;
  that is a domain policy, not a safe universal default;
- several infrastructure failures are converted to defects with `Effect.orDie`, losing recoverable
  operational errors;
- connection drain has no timeout fallback;
- the code predates stable NATS v3 packages and has no clear licence
  ([manifest](https://github.com/jkonowitch/hex-effect/blob/9e959248c3e23465eb2135470ca30167bb5581a1/packages/%40hex-effect/infra-libsql-nats/package.json)).

## 4. AngstromSCD NATS service

This application wraps JetStream operations in an Effect service and adds logging, spans, and a
test layer, but the underlying connection is a module-global cached promise
([Effect service](https://github.com/zetsuchan/angstromscd-monorepo/blob/464be0466e5737ff45a46ebe07669ac03710ab59/apps/api/src/effect/services/nats-service.ts),
[connection singleton](https://github.com/zetsuchan/angstromscd-monorepo/blob/464be0466e5737ff45a46ebe07669ac03710ab59/apps/api/src/lib/messaging/nats-client.ts)).

This is primarily a negative control:

- a rejected initial promise remains cached until an explicit shutdown;
- connection lifetime is controlled by `beforeExit`, not an Effect `Scope`;
- subscription acquisition uses `acquireRelease`, then returns a separately consumed stream; the
  stream itself does not own the subscription scope;
- the subscription maps payloads but exposes no settlement contract;
- all errors collapse into one operation-labelled `NatsError`.

Its infrastructure script is still useful evidence that stream retention, storage, replicas,
per-subject limits, consumer durability, ack policy, delivery policy, and subject filters must be
configuration-visible
([bootstrap](https://github.com/zetsuchan/angstromscd-monorepo/blob/464be0466e5737ff45a46ebe07669ac03710ab59/infra/scripts/bootstrap-nats.ts)).

## 5. `nuxt-nats`

This is the broadest recent non-Effect integration in the sample. It deliberately uses modular
NATS.js v3, separates Core from JetStream publish, caches KV/Object Store bucket handles, provisions
streams, monitors connection status, and tests Core/JetStream/KV/Object Store against a NATS
testcontainer
([SDK ADR](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/docs/adr/001-modular-nats-sdk.md),
[manifest](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/package.json)).

Worth adapting:

- teardown order: stop long-lived consumers before draining, then clear the connection and
  JetStream client/manager handles;
- status-loop visibility for disconnect, reconnect, and authorization errors;
- clear separation between Core fire-and-forget publish and JetStream PubAck publish;
- stable message ID, trace, correlation, and custom headers with precedence rules;
- serial pull consumption (`max_messages: 1`), idle heartbeat, in-progress ack extension for slow
  handlers, delayed `nak`, and DLQ routing;
- KV CAS and tombstone tests, plus chunked Object Store integration tests
  ([plugin lifecycle](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/src/runtime/server/plugins/nats.ts),
  [consumer](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/src/runtime/server/utils/consumer.ts),
  [KV tests](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/test/integration/kv.test.ts),
  [Object Store tests](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/test/integration/obj.test.ts)).

Avoid or tighten:

- JetStream publish retries three times by default even when no `msgId` is supplied, so an
  ambiguous PubAck timeout may duplicate messages
  ([publish helper](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/src/runtime/server/utils/publish.ts));
- consumer and KV/Object Store caches are process globals rather than scope-owned services;
- a successful handler must remember to acknowledge the message itself; returning a typed
  settlement value would make this obligation visible;
- the consumer's internal retry loop and heartbeat use raw promises/timers rather than supervised
  fibers and schedules;
- JSON decoding falls back to a string on parse failure, which hides schema errors;
- cached bucket creation options only apply on the first call, an important behavior that should be
  explicit in a service boundary, and those caches are not invalidated by the shown connection
  cleanup
  ([KV cache](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/src/runtime/server/utils/useKV.ts),
  [Object Store cache](https://github.com/lithqube/nuxt-nats/blob/b5616b7946129df3e76792f5ce1056eebbc7c55a/src/runtime/server/utils/useObj.ts)).

## 6. `@horizon-republic/nestjs-jetstream`

This is the deepest operational implementation in the sample. It prevents connection stampedes
with a cached in-flight promise, clears the promise after failure, invalidates JetStream clients on
reconnect, drains on shutdown with a close fallback, and exhaustively switches over upstream status
tags
([connection provider](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/src/connection/connection.provider.ts)).

Worth adapting:

- central message settlement and one place to calculate delayed `nak`/final-delivery behavior;
- explicit concurrency gates, ack extension while work waits locally, and `nak` of parked work on
  disposal so rolling shutdown does not wait out `ack_wait`;
- idempotent, two-phase shutdown: stop intake everywhere, then drain connections in parallel;
- pluggable codecs and end-to-end observability hooks;
- broad unit and testcontainer integration coverage, including retry timing with both lower and
  upper bounds and validation of effective server consumer configuration
  ([settlement](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/src/server/routing/settlement.ts),
  [concurrency gate](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/src/server/routing/concurrency-gate.ts),
  [retry/DLQ integration tests](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/test/integration/retry-and-dlq-defaults.spec.ts)).

Avoid or correct:

- the concurrency gate's FIFO backlog is unbounded; warning at 1,000 items is observability, not
  backpressure;
- the shutdown manager races drain with a timeout and logs that it is “closing anyway,” but its
  timeout branch does not call force-close. Our timeout path must perform the state change it claims
  ([shutdown manager](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/src/shutdown/shutdown.manager.ts));
- JSON codec output is `unknown` and empty bytes become `undefined`; our typed layer should use
  `Schema` and distinguish empty payload policy from decode failure
  ([JSON codec](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/src/codec/json.codec.ts));
- the package declares MIT, but the checked-in licence text retains placeholder copyright fields
  ([manifest](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/package.json),
  [licence](https://github.com/HorizonRepublic/nestjs-jetstream/blob/2c53eba348dd1c043de6deea454942a0295f50b5/LICENSE.md)).

## 7. NATS Console

The application keeps per-cluster connection, JetStream manager, and client maps; drains before
removing a cluster; can drain all clusters in parallel; and uses flush/RTT plus server and account
information for health reporting. It also exposes management operations and discovers KV/Object
Store buckets from their backing stream prefixes
([NATS module](https://github.com/KLogicHQ/nats-console/blob/77a88a64df87c4efe41dbe15ab9358297342b4cc/apps/api/src/lib/nats.ts)).

Useful application-level lessons:

- connection identity belongs in observability and should be explicit for multi-cluster use;
- the Core connection, JetStream data client, and JetStream manager have different jobs and should
  not be conflated;
- health should include round-trip/flush, active server information, JetStream account limits, and
  byte/message counters.

Avoid its wrapper shape:

- a single very large module owns lifecycle, management, publishing, health, KV, and Object Store;
- many broad `catch` blocks return `null`, empty results, or “not connected,” losing the distinction
  between not-found, permission, timeout, disconnect, and invalid configuration;
- mutable process-global maps have no capacity, eviction, or scope;
- it uses legacy `nats` 2.28 rather than modular NATS.js v3
  ([API manifest](https://github.com/KLogicHQ/nats-console/blob/77a88a64df87c4efe41dbe15ab9358297342b4cc/apps/api/package.json)).

Its load generator is useful only as a smoke-test pattern: it records errors and p50/p95/p99 PubAck
latency under steady and burst traffic, but it is not a controlled benchmark or backpressure proof
([load test](https://github.com/KLogicHQ/nats-console/blob/77a88a64df87c4efe41dbe15ab9358297342b4cc/examples/src/load-test.ts)).

## Cross-project design implications

### Connection lifecycle

Adopt one raw connection per scoped service instance, with separate Node and WebSocket connector
layers. Acquisition failure remains typed. Finalization should:

1. stop new subscription/consumer intake;
2. interrupt or settle owned fibers;
3. attempt `drain` under a configured duration;
4. call `close` if drain fails or times out;
5. await `closed` and clear derived manager/bucket handles.

Expose connection status as a stream and preserve upstream tags. A reconnect invalidates derived
handles only where the official SDK requires it; this needs an integration test rather than an
assumption.

### Subscriptions and backpressure

The public Core subscription should be a `Stream`, but the bridge must not imply backpressure that
NATS does not provide. Specify:

- buffer capacity in messages and preferably bytes;
- overflow behavior (`fail`, `dropNewest`, `dropOldest`, or disconnect) with a safe default;
- queue-group and auto-unsubscribe semantics;
- pending/received/processed counters and slow-consumer status;
- finalization behavior (`drain`, `unsubscribe`, iterator `stop`);
- whether concurrent handlers preserve ordering.

JetStream pull consumers should prefer server-side pull limits and `max_ack_pending` over a large
client backlog. If local concurrency exists, use a bounded queue and extend or settle parked
messages deliberately.

### Codecs and schemas

Raw bytes are the interoperability floor. Build typed codecs above them:

- encode/decode are effects with distinct `EncodeError` and `DecodeError`;
- schema decoding never silently falls back from malformed JSON to a string;
- headers, reply subject, message ID, and trace context remain available with decoded payloads;
- KV key codecs preserve subject-token semantics;
- payload transforms such as compression/encryption declare compatibility and byte limits.

### Retry truthfulness

Use no implicit retry for Core publish or request. Connection reconnect is an upstream lifecycle
policy, not an operation retry. JetStream publish retry should require either a stable deduplication
ID or an explicit opt-in that acknowledges duplicates. KV CAS, KV create, object chunk upload,
watch recovery, and request/reply each need separate retry policies; a single package-wide schedule
would be misleading.

### KV

Provide separate create/open operations and a scoped bucket service. Preserve revision and operation
metadata in the low-level entry type. An ergonomic `Option`-returning read may treat tombstones as
missing, but the raw/historical API must keep `DEL` and `PURGE`. Expose create/update CAS failures as
typed errors, watch/history/keys as scoped streams, and resume-from-revision explicitly. Put an
Effect `KeyValueStore` adapter above this API rather than making the generic adapter the canonical
NATS contract.

### Object Store

Use `Stream<Uint8Array, ObjectStoreError>` for upload and download, with chunk size and size limits
visible. Scope owns the upstream `ReadableStream` reader and cancellation. Completion must validate
the official result's separate error/digest outcome; early consumer cancellation is not success.
Keep metadata, revision, digest, links, watch, seal, and destructive operations distinct. Bulk blob
helpers may collect bytes only under an explicit maximum.

### Testing

The strongest projects combine small unit tests with real-server integration tests. The proposed
test pyramid is:

- pure tests for subject validation, headers, codecs, option translation, retry classification, and
  protocol/error mapping;
- Effect tests with `TestClock`, bounded queues, deferred/latches, fake connectors, interruption,
  acquisition failure, and finalizer order;
- testcontainer integration tests for Core request/subscription, queue groups, reconnect, slow
  consumers, KV CAS/tombstones/watch resume, Object Store multi-chunk/digest/cancellation, and
  permission errors;
- a declared NATS server version matrix for feature-gated behavior;
- load/fault tests that report offered load, accepted load, pending messages/bytes, overflow,
  duplicate rate, redelivery, and latency rather than throughput alone.

## Recommended first decisions

Before implementation, freeze ADRs for:

1. supported runtimes and the transport seam;
2. service/layer ownership and bounded shutdown behavior;
3. Core subscription buffer capacity and overflow policy;
4. raw and schema codec contracts;
5. typed error taxonomy and defect/interruption rules;
6. retry policy per operation, especially ambiguous JetStream publish;
7. KV tombstone, CAS, and watch semantics;
8. Object Store streaming, cancellation, and digest semantics;
9. server-version integration matrix and testcontainer strategy.

Third-party code should remain comparison evidence. Official NATS.js source and server observations
must decide compatibility questions when these projects disagree.

## Method and limitations

This was a lightweight source survey, not a package security audit or benchmark. Evidence came from
the repositories' pinned source, manifests, tests, and licence files; no secondary articles or
marketing comparisons were used. “Maintained” signals are limited to archive state and recent
commit/package evidence at the snapshot date. The projects were not installed or executed, so
behavior inferred from code is marked as a design lesson rather than a conformance result.
