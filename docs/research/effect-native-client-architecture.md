# Effect-native architecture for NATS Core, KV, and Object Store

Status: research proposal; no public API is frozen by this document.

## Scope and source basis

This note proposes the Effect-facing architecture for a TypeScript client covering NATS Core,
Key/Value, and Object Store. It does not settle the NATS protocol mapping, server compatibility,
or the choice of an underlying TypeScript transport. Those require the companion NATS and
third-party-client reviews.

The primary Effect source inspected for this note is the read-only checkout at
`C:\Users\kokok\Dev\Foldable\effect-foldable`, commit
`648f566dd259898e7697c7fcb796183ccbc474ab`, whose `effect` package identifies itself as
`4.0.0-rc.111` (`effect@648f566d:packages/effect/package.json:2-4`). Citations of the form
`effect@648f566d:path:line` refer to that checkout. Repository policy says the pinned tree is the
ground truth, so this proposal uses the v4 names actually present there. In particular, the
schema-backed error constructor at this pin is `Schema.TaggedError`, not
`Schema.TaggedErrorClass` (`effect@648f566d:packages/effect/src/Schema.ts:14745-14784`).

This document deliberately separates:

- **Verified Effect fact**: directly supported by the pinned source.
- **Design recommendation**: the proposed NATS Effect API or implementation policy. It remains
  subject to protocol and adapter validation.

## Primary recommendation

Build one scoped connection owner and three Effect-native capability services over it:

```text
       Config / concrete options
                 |
                 v
    NatsConnection (one Scope owner)
                 |
   +-------------+--------------+
   |                            |
   v                            v
NatsCore                 JetStreamContext
                                |
                      +---------+---------+
                      |                   |
                      v                   v
                    NatsKv          NatsObjectStore
```

The public API should expose `Effect` for one-shot operations, `Stream` for subscriptions,
watches, status changes, and object content, `Layer` for acquisition, `Schema` at untrusted
boundaries, and tagged errors for expected failures. The underlying NATS library or wire client
should live behind an internal `Transport` seam and must not appear in public types.

The connection layer should own connection acquisition, protocol-reader fibers, status
broadcast, draining, and close. Core, KV, and Object Store layers should be shallow views over
that owner. This prevents three independently acquired connections and gives cancellation one
truthful lifetime boundary.

## Verified Effect v4 facts

### Schema boundaries

- `Schema.Struct` constructs record schemas
  (`effect@648f566d:packages/effect/src/Schema.ts:3567-3572`).
- `Schema.TaggedUnion` builds `_tag`-discriminated unions and supplies cases, guards, and
  exhaustive matching helpers (`effect@648f566d:packages/effect/src/Schema.ts:6370-6409`).
- `Schema.brand` makes structurally identical output types nominally distinct, but adds no
  runtime check of its own. Runtime constraints must precede the brand
  (`effect@648f566d:packages/effect/src/Schema.ts:5212-5233`).
- `Schema.decodeUnknownEffect` validates unknown input and reports `SchemaError` in the typed
  error channel (`effect@648f566d:packages/effect/src/Schema.ts:1489-1515`).
- `Schema.encodeEffect` accepts a schema-typed value and returns the encoded representation in
  an `Effect` (`effect@648f566d:packages/effect/src/Schema.ts:1988-2015`).
- `schema.makeEffect` keeps constructor validation failures in the Effect error channel
  (`effect@648f566d:packages/effect/src/Schema.ts:242-258`).
- Effect has a `Schema.Uint8Array`; its default JSON representation is Base64, while the runtime
  type remains `Uint8Array` (`effect@648f566d:packages/effect/src/Schema.ts:13521-13544`). This
  does not mean NATS payloads should be Base64 encoded: Core NATS transports raw bytes.
- The schema-backed expected-error constructor in this pin is `Schema.TaggedError`; its values
  are yieldable errors with `_tag` discrimination
  (`effect@648f566d:packages/effect/src/Schema.ts:14745-14784`).

### Services, layers, and scope

- A class-style `Context.Service<Self, Shape>()("Id")` is both a context key and a typed service
  constructor (`effect@648f566d:packages/effect/src/Context.ts:98-112` and
  `effect@648f566d:packages/effect/src/Context.ts:201-224`).
- `Layer.effect` builds one service from an effect and discharges the construction scope from
  the resulting layer requirement (`effect@648f566d:packages/effect/src/Layer.ts:1014-1033`).
  `Layer.effectContext` is the corresponding constructor for multiple services
  (`effect@648f566d:packages/effect/src/Layer.ts:1068-1075`).
- `Effect.acquireRelease` registers release against the surrounding `Scope`; the release action
  receives the exit and cannot have a typed failure
  (`effect@648f566d:packages/effect/src/Effect.ts:6573-6583`).
- A scope runs registered finalizers when it closes
  (`effect@648f566d:packages/effect/src/Scope.ts:21-24` and
  `effect@648f566d:packages/effect/src/Scope.ts:51-67`).
- `Effect.forkScoped` attaches a fiber to a scope, and its own example states that the fiber is
  interrupted when that scope closes (`effect@648f566d:packages/effect/src/Effect.ts:8595-8626`).
- `Effect.ensuring` runs a non-failing finalizer around an individual operation
  (`effect@648f566d:packages/effect/src/Effect.ts:6792-6813`).

### Streams and buffering

- `Stream<A, E, R>` is pull-based and backpressured downstream
  (`effect@648f566d:packages/effect/src/Stream.ts:95-103`).
- `Stream.callback` adapts a callback producer through a queue and accepts an explicit buffer
  size and `sliding`, `dropping`, or `suspend` strategy. Its setup effect may use `Scope`
  (`effect@648f566d:packages/effect/src/Stream.ts:660-700`).
- `Stream.fromQueue` exposes a queue's read side as a stream, while `Stream.fromPubSub` broadcasts
  a PubSub to stream subscribers (`effect@648f566d:packages/effect/src/Stream.ts:1129-1166`).
- `Stream.buffer` adds another queue boundary. A finite buffer can suspend, drop new chunks, or
  slide old chunks (`effect@648f566d:packages/effect/src/Stream.ts:4570-4601`).
- `Stream.mapEffect` supports bounded concurrent processing and an explicit unordered mode
  (`effect@648f566d:packages/effect/src/Stream.ts:1821-1847`).
- `Queue.bounded` suspends Effect producers at capacity. `Queue.dropping` rejects new values and
  `Queue.sliding` evicts the oldest values
  (`effect@648f566d:packages/effect/src/Queue.ts:470-500` and
  `effect@648f566d:packages/effect/src/Queue.ts:503-574`).
- `PubSub.bounded` backpressures on full subscriptions, while the dropping and sliding
  constructors have intentionally lossy semantics
  (`effect@648f566d:packages/effect/src/PubSub.ts:331-343` and
  `effect@648f566d:packages/effect/src/PubSub.ts:345-435`). PubSub subscriptions are scoped and
  register unsubscription finalizers (`effect@648f566d:packages/effect/src/PubSub.ts:1079-1093`).
- `Stream.scoped` closes stream resources when consumption ends
  (`effect@648f566d:packages/effect/src/Stream.ts:1637-1668`).

### Config, retry, and testing

- `Config<T>` is a yieldable effectful recipe that resolves the current `ConfigProvider`
  (`effect@648f566d:packages/effect/src/Config.ts:87-110`). `Config.schema` validates a structured
  configuration with a Schema (`effect@648f566d:packages/effect/src/Config.ts:871-899`).
- `Config.withDefault` only supplies a value for absent input
  (`effect@648f566d:packages/effect/src/Config.ts:510-537`), and `Config.redacted` hides secrets
  from string and log output (`effect@648f566d:packages/effect/src/Config.ts:1462-1499`).
- `Config.unwrap` turns a `Config.Wrap<T>` tree into `Config<T>`, which supports a library-style
  `layerConfig` constructor (`effect@648f566d:packages/effect/src/Config.ts:635-656`).
- The default config provider reads the environment, and `ConfigProvider.layer` can replace it;
  `fromUnknown` provides deterministic object-backed configuration for tests
  (`effect@648f566d:packages/effect/src/ConfigProvider.ts:307-343`,
  `effect@648f566d:packages/effect/src/ConfigProvider.ts:640-669`, and
  `effect@648f566d:packages/effect/src/ConfigProvider.ts:750-784`).
- `Effect.retry` retries typed failures, propagates the last failure when the policy stops, and
  does not retry defects or interruptions (`effect@648f566d:packages/effect/src/Effect.ts:4017-4038`).
  `Schedule.exponential`, `Schedule.jittered`, and `Schedule.upTo` provide exponential delay,
  randomization, and a finite bound
  (`effect@648f566d:packages/effect/src/Schedule.ts:830-859`,
  `effect@648f566d:packages/effect/src/Schedule.ts:1076-1100`, and
  `effect@648f566d:packages/effect/src/Schedule.ts:1270-1320`).
- `Effect.timeout` interrupts the source when the timeout wins and adds a typed `TimeoutError`
  (`effect@648f566d:packages/effect/src/Effect.ts:4530-4549`).
- `TestClock.adjust` advances virtual time and runs due actions
  (`effect@648f566d:packages/effect/src/testing/TestClock.ts:473-508`). `Deferred` suspends waiters
  until one result becomes available and its completion is single-assignment
  (`effect@648f566d:packages/effect/src/Deferred.ts:188-223` and
  `effect@648f566d:packages/effect/src/Deferred.ts:760-786`).

## Recommended public capability model

The snippets below are **API sketches**, not compile-checked declarations. Exact generic order and
names should be verified in the first API spike.

### 1. Boundary values and codecs

Define schema-backed nominal values for protocol names only after their actual NATS grammar is
verified:

```ts
export const Subject = Schema.String.pipe(
  /* verified NATS subject constraint */,
  Schema.brand("NatsSubject"),
)
export interface Subject extends Schema.Schema.Type<typeof Subject> {}

export const QueueGroup = Schema.String.pipe(
  /* verified queue-group constraint */,
  Schema.brand("NatsQueueGroup"),
)
export interface QueueGroup extends Schema.Schema.Type<typeof QueueGroup> {}
```

At minimum, model `Subject`, `QueueGroup`, `Inbox`, `KvBucketName`, `KvKey`, `ObjectName`, and
`Revision` separately. Do not cast strings into these types. The exact revision representation
must follow the selected NATS API and wire range; do not assume JavaScript `number` until checked.

Keep raw bytes as the foundational public payload. Typed payload support should be an explicit
value-level codec rather than a second connection service:

```ts
export interface PayloadCodec<A> {
  readonly encode: (value: A) => Effect.Effect<Uint8Array, EncodeError>
  readonly decode: (bytes: Uint8Array) => Effect.Effect<A, DecodeError>
}

export const json = <A>(schema: Schema.Codec<A, unknown>): PayloadCodec<A> => /* ... */
```

The codec implementation should use `Schema.encodeEffect` and `Schema.decodeUnknownEffect` and
map `SchemaError` at the codec boundary. Encoding must finish before the transport publish begins.
Decoding failures belong on the subscription, KV-read, or object-metadata error channel; they
must not become defects.

Do not put methods such as `respond` inside persisted `Schema.Struct` messages. Use a schema for
the data record and a normal interface for the live capability:

```ts
export interface Message<A> {
  readonly subject: Subject
  readonly replyTo: Option.Option<Subject>
  readonly headers: Headers
  readonly data: A
  readonly respond: (
    data: Uint8Array,
    options?: PublishOptions
  ) => Effect.Effect<boolean, PublishError>
}
```

### 2. Connection owner

`NatsConnection` is the only service allowed to own the underlying connection and long-lived
reader/status fibers.

```ts
export interface Interface {
  readonly serverInfo: Effect.Effect<ServerInfo, ConnectionClosedError>
  readonly status: Stream.Stream<ConnectionStatus>
  readonly flush: Effect.Effect<void, FlushError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@nats-effect/NatsConnection"
) {}

export const layer: (options: ConnectionOptions) => Layer.Layer<Service, ConnectError>

export const layerConfig: (
  options: Config.Wrap<ConnectionOptions>
) => Layer.Layer<Service, Config.ConfigError | ConnectError>
```

The real layer should acquire with `Effect.acquireRelease`. On normal scope closure it should
attempt a bounded drain, then close. On interruption or failed acquisition it should close
promptly. Because an Effect finalizer cannot expose a typed error, failed drain/close operations
must be logged and measured; a caller that needs a reported drain result should get an explicit
`drain` operation before scope exit, while finalization remains the safety net.

The status stream should be backed by a small scoped PubSub. A sliding status buffer is acceptable
because connection status is observational and the newest state is normally more useful than an
old transition. Message/data streams must not inherit that policy.

### 3. Core NATS

```ts
export interface Interface {
  readonly publish: (
    subject: Subject,
    data: Uint8Array,
    options?: PublishOptions
  ) => Effect.Effect<void, PublishError>

  readonly request: (
    subject: Subject,
    data: Uint8Array,
    options?: RequestOptions
  ) => Effect.Effect<Message<Uint8Array>, RequestError>

  readonly subscribe: (
    subject: Subject,
    options?: SubscribeOptions
  ) => Stream.Stream<Message<Uint8Array>, SubscriptionError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@nats-effect/NatsCore"
) {}
```

Add typed combinators as pure module functions over `NatsCore.Service` and a `PayloadCodec<A>`:

```ts
Core.publishWith(codec)(subject, value)
Core.requestWith(requestCodec, responseCodec)(subject, value)
Core.subscribeWith(codec)(subject, options)
```

This keeps one small raw capability contract, makes the encoding policy visible at each call,
and avoids `unknown` or unchecked generic payloads.

Do not call Core `publish` durable or acknowledged. Its completion contract must be specified
from the selected transport and NATS protocol review. `flush` is a separate operation. Automatic
publish retries are prohibited unless a higher layer supplies an idempotency protocol, because a
transport failure can leave delivery ambiguous.

Each `subscribe` call should construct a new scoped stream. When the stream terminates, fails, or
is interrupted, its finalizer must unsubscribe/drain the underlying NATS subscription and remove
all callback listeners. NATS queue groups stay an explicit `SubscribeOptions.queueGroup`; an
Effect `Queue` must not be presented as equivalent to broker queue-group semantics.

### 4. Key/Value

Use a service to open buckets and return immutable bucket capabilities. Bucket acquisition itself
does not need to be a Layer unless the selected adapter allocates independent resources.

```ts
export interface Kv {
  readonly open: <A>(
    bucket: KvBucketName,
    codec: PayloadCodec<A>,
    options?: OpenBucketOptions
  ) => Effect.Effect<KvBucket<A>, KvOpenError>
}

export interface KvBucket<A> {
  readonly get: (key: KvKey) => Effect.Effect<Option.Option<KvEntry<A>>, KvReadError | DecodeError>
  readonly put: (key: KvKey, value: A) => Effect.Effect<Revision, KvWriteError | EncodeError>
  readonly create: (
    key: KvKey,
    value: A
  ) => Effect.Effect<Revision, KvConflictError | KvWriteError | EncodeError>
  readonly update: (
    key: KvKey,
    value: A,
    expected: Revision
  ) => Effect.Effect<Revision, KvConflictError | KvWriteError | EncodeError>
  readonly delete: (key: KvKey, options?: DeleteOptions) => Effect.Effect<void, KvWriteError>
  readonly watch: (
    selector?: KvSelector,
    options?: WatchOptions
  ) => Stream.Stream<KvEvent<A>, KvWatchError | DecodeError>
}
```

Design rules:

- Missing keys should be `Option.none`; connection, permission, timeout, and decode failures stay
  errors.
- `KvEvent` should be a `Schema.TaggedUnion` that preserves Put/Delete/Purge distinctions and
  revision metadata.
- Compare-and-set conflicts deserve `KvConflictError`; never collapse them into not-found or a
  Boolean.
- Never automatically retry `create` or revision-conditional `update`. Retrying with stale
  expected state changes their meaning.
- Watchers are scoped streams. Initial values, live values, deletes, purges, and watcher
  completion must remain distinguishable.
- Key iteration should be a `Stream<KvKey, ...>`, not a collected array by default.

Whether `put` can be retried safely, and whether a watch can resume without gaps, are NATS
semantics questions and remain open until the protocol/adapter mapping is complete.

### 5. Object Store

Object bodies must remain streaming. Metadata can be schema-backed; object content should not be
decoded into one in-memory object by default.

```ts
export interface ObjectStore {
  readonly open: (
    bucket: ObjectBucketName,
    options?: OpenObjectStoreOptions
  ) => Effect.Effect<ObjectBucket, ObjectStoreOpenError>
}

export interface ObjectBucket {
  readonly info: (name: ObjectName) => Effect.Effect<Option.Option<ObjectInfo>, ObjectReadError>
  readonly get: (name: ObjectName) => Effect.Effect<Option.Option<ObjectReader>, ObjectReadError>
  readonly put: (
    metadata: ObjectMetadata,
    content: Stream.Stream<Uint8Array, ObjectSourceError>
  ) => Effect.Effect<ObjectInfo, ObjectWriteError | ObjectSourceError>
  readonly delete: (name: ObjectName) => Effect.Effect<void, ObjectWriteError>
  readonly watch: (options?: ObjectWatchOptions) => Stream.Stream<ObjectEvent, ObjectWatchError>
}

export interface ObjectReader {
  readonly info: ObjectInfo
  readonly content: Stream.Stream<Uint8Array, ObjectReadError>
}
```

Design rules:

- Never use `Stream.runCollect` in the production `get` or `put` implementation.
- A `put` is interruptible. Interruption must stop reading input, stop publishing chunks, and
  attempt cleanup of partial state according to the verified Object Store protocol.
- The successful result is returned only after all chunks and terminal metadata/checksum state
  have reached the guarantee exposed by the underlying API.
- Cleanup failure during interruption cannot be returned from a scope finalizer; log and measure
  it, and expose orphan reconciliation as a separate administrative operation if the protocol
  requires one.
- Metadata replacement, links, chunk size, checksum behavior, and overwrite/CAS rules stay open
  until verified against the official implementation.

## Backpressure model

There is no single "NATS buffer." Specify and observe three different boundaries:

| Boundary                          | Owner                   | Proposed control                                                          | Overflow policy                                                    |
| --------------------------------- | ----------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Socket/client pending writes      | selected NATS transport | explicit pending-byte/message limits                                      | fail publish or disconnect according to verified adapter semantics |
| NATS callback/iterator to Effect  | this adapter            | finite per-subscription capacity                                          | default fail-and-unsubscribe; lossy modes must be explicit         |
| Effect stream to application work | application pipeline    | `Stream.mapEffect(..., { concurrency })`, optional finite `Stream.buffer` | suspend by default                                                 |

**Important implication:** downstream `Stream` backpressure does not by itself stop a socket or
synchronous callback producer. `Stream.callback` gives the adapter a finite queue, but the adapter
must still decide what happens when its external producer outruns that queue. Therefore:

1. Never default message subscriptions, KV watches, or object chunks to an unbounded queue.
2. Default data overflow to a typed `SlowConsumerError`, terminate/unsubscribe the subscription,
   and include capacity plus observed pending counts in the error.
3. Allow `dropNewest` or `dropOldest` only as explicit options for observational streams. Return
   loss counters or status events; do not silently discard.
4. Use a small sliding PubSub only for connection status/metrics where current state supersedes
   history.
5. Do not add `Stream.buffer` inside every public method. It would create a second hidden queue.
6. Expose diagnostics for client pending bytes/messages, adapter queue depth, dropped messages,
   active subscriptions, and downstream in-flight handlers separately.

The actual defaults should be chosen only after the selected NATS client and server limits have
been measured. The public option names should nevertheless make every buffer visible:

```ts
interface SubscriptionBufferOptions {
  readonly capacity: number
  readonly overflow: "fail" | "dropNewest" | "dropOldest"
}
```

`"suspend"` is meaningful only where the producer can itself return an Effect and wait. For a
non-blocking external callback, naming the policy `suspend` would overstate the guarantee unless
the underlying subscription can actually pause delivery.

## Typed error model

Use schema-backed tagged errors for expected public failures. Do not expose the underlying
library's exception classes as the stable contract.

Recommended families:

```text
Configuration: Config.ConfigError (preserve Effect's native error)
Connection:    ConnectError | ConnectionClosedError | AuthorizationError | ProtocolError
Core:          PublishError | RequestError | NoRespondersError | SubscriptionError
Pressure:      SlowConsumerError | PendingLimitError
Codec:         EncodeError | DecodeError
KV:            KvOpenError | KvReadError | KvWriteError | KvConflictError | KvWatchError
Object Store:  ObjectStoreOpenError | ObjectReadError | ObjectWriteError | ObjectWatchError
```

Each adapter-boundary error should contain:

- a stable `_tag`;
- a stable operation label such as `"Core.request"`;
- relevant safe identifiers such as subject or bucket, with secrets excluded;
- a `retryable` classification only when the boundary can support it truthfully;
- a schema-safe defect/cause representation for diagnostics.

Keep timeout, no-responders, authorization, decode failure, conflict, slow-consumer overflow, and
interruption semantically distinct. Interruption is not a typed error and must not be caught and
repackaged. Broad cause handlers, if any, belong only at a connection-reader supervision boundary
and must preserve interrupt causes.

An illustrative pinned-v4 error shape is:

```ts
export class SlowConsumerError extends Schema.TaggedError<SlowConsumerError>(
  "@nats-effect/SlowConsumerError"
)(
  "SlowConsumerError",
  {
    operation: Schema.String,
    capacity: Schema.Number,
    pending: Schema.Number,
    subject: Subject
  }
) {}
```

The precise `Schema.TaggedError` overload and identifier/tag convention should be compile-checked
in the API spike. Error construction must not rely on `as any` or unchecked casts.

## Configuration and layer acquisition

Offer both a concrete and a Config-backed constructor:

```ts
NatsConnection.layer(options)
NatsConnection.layerConfig(config: Config.Wrap<ConnectionOptions>)
```

The concrete options record is the primary library API and should also be schema-validated at the
layer boundary. `layerConfig` should use `Config.unwrap`. Application logic must not read
`process.env` directly.

Initial configuration groups:

```text
connection
  servers, connectionName, connectTimeout, pingInterval, maxPingOut
authentication
  user/password, token, nkey/seed, credentials (all secret material redacted)
reconnect
  enabled, maxAttempts, baseDelay, maxDelay, jitter
pending writes
  maxMessages, maxBytes
subscriptions
  defaultCapacity, defaultOverflow
request/reply
  timeout, maxConcurrentRequests
shutdown
  drainTimeout, closeTimeout
observability
  logLevel/annotations, metrics namespace, tracing enablement
```

Field names and defaults remain provisional until mapped to the chosen client and official NATS
configuration. Validate positive capacities/durations and non-empty server lists. Missing optional
configuration can have a documented default; malformed values must fail layer construction.

Test configuration should use `ConfigProvider.fromUnknown` or pass concrete options. Secrets
should use `Config.redacted`, be unwrapped only at the transport boundary, and never enter error
payloads or log annotations.

## Retry and reconnection policy

Separate **connection reconnection** from **operation retry**. The selected NATS transport will
usually own reconnect and subscription restoration; layering an independent generic Effect retry
around every method can multiply attempts and produce duplicates.

Recommended initial policy:

| Operation                      |                            Automatic Effect retry? | Reason                                                                |
| ------------------------------ | -------------------------------------------------: | --------------------------------------------------------------------- |
| Initial connection acquisition |                                        bounded yes | no operation has been accepted yet                                    |
| Transport reconnect loop       |                           selected adapter owns it | must coordinate subscriptions and pending writes                      |
| Core publish                   |                                                 no | failure may be ambiguous and publish has no durable acknowledgement   |
| Request/reply                  |                                      no by default | responder may have performed the request before the reply was lost    |
| KV get/info                    |                       bounded retry may be offered | read-only, after classifying transient failures                       |
| KV create/update               |                                                 no | replay changes CAS meaning or duplicates a successful ambiguous write |
| KV put/delete/purge            |                                  no until verified | server acceptance may be ambiguous                                    |
| Object get/info                | bounded retry may be offered before content starts | restart semantics must be explicit after bytes are emitted            |
| Object put/delete              |                                  no until verified | partial chunk state and terminal metadata make replay non-trivial     |

For the few retryable boundaries, use a finite exponential schedule with jitter and a total
attempt/duration bound. Retry only the tagged transient errors admitted by the operation. Report
the exhausted error; do not turn it into success. Use `TestClock` to verify exact attempt counts
and delay behavior.

## Cancellation, interruption, and finalizers

Required cancellation contracts:

- Interrupting `request` removes its inbox subscription and timeout state.
- Ending or interrupting a Core subscription, KV watch, or object watch unregisters the underlying
  subscription before the stream scope closes.
- Interrupting object upload stops input consumption and network publication, then attempts
  protocol-defined cleanup.
- Closing the connection layer interrupts its scoped status/reader fibers, drains within a bounded
  deadline on orderly shutdown, and closes even if drain times out.
- No public `start` method and no globally detached fiber. Layer acquisition must finish after
  forking long-lived work with `Effect.forkScoped`.
- Method-local listeners and timers use `Effect.acquireRelease`, `Effect.ensuring`, or a scoped
  stream, not ad hoc `finally` blocks.

Finalizers should be idempotent because explicit drain/close and scope interruption may race.
Typed cleanup failures must be handled before the release effect; finalizers themselves remain
non-failing and observable through logs/metrics.

## Recommended modules and package surface

Start with one package. Splitting Core, KV, and Object Store into independently versioned packages
would add release and compatibility burden before the seams are proven.

```text
src/
  index.ts                     intentional package exports only
  NatsConnection.ts            service, layers, concrete/config options
  NatsCore.ts                  raw Core capability and typed codec combinators
  NatsKv.ts                    KV service, bucket capability, watch events
  NatsObjectStore.ts           object metadata and streaming body API
  NatsSubject.ts               subject/name/key schemas and constructors
  NatsPayload.ts               PayloadCodec and Schema-backed codecs
  NatsError.ts                 stable shared error schemas
  NatsStatus.ts                status/event schemas
  internal/
    Transport.ts               adapter-neutral transport seam
    SubscriptionBridge.ts      finite queue and scoped callback/iterator bridge
    RequestMux.ts              inbox/request lifecycle
    JetStream.ts               minimum internals required by KV/Object Store
    Diagnostics.ts             metrics/log annotations
test/
  ...                          behavior and lifecycle tests
typetest/
  ...                          public inference and error-channel assertions
docs/
  architecture/
  research/
```

Use PascalCase module files and import module namespaces, matching Effect's own root barrel style;
the pinned `effect` barrel exports `Config`, `Context`, `Effect`, `Layer`, `PubSub`, `Queue`,
`Schedule`, `Schema`, and `Stream` as ES module namespaces
(`effect@648f566d:packages/effect/src/index.ts:97-112`,
`effect@648f566d:packages/effect/src/index.ts:152-157`,
`effect@648f566d:packages/effect/src/index.ts:292-302`, and
`effect@648f566d:packages/effect/src/index.ts:422-432`,
`effect@648f566d:packages/effect/src/index.ts:512-587`).

Use ESM, `.js` specifiers in TypeScript source, and `sideEffects: false`/an empty side-effects list
once every module is import-pure. The pinned Effect package is ESM, declares no package side
effects, and exports individual modules (`effect@648f566d:packages/effect/package.json:2-4` and
`effect@648f566d:packages/effect/package.json:28-51`).

Keep implementation constructors and raw adapter handles internal. The root package should export
the four capability modules, boundary schemas/codecs, and public errors. Avoid a catch-all client
object with dozens of methods.

Use `Context.Service` classes for service tags. Name public and non-trivial operations with
`Effect.fn("NatsCore.publish")`, except measured hot-path internals where `Effect.fnUntraced` is an
intentional performance choice. Do not use `async`/`await` or `try`/`catch` inside Effect workflows;
adapt promises and callbacks at the `Transport` boundary.

## Testing architecture

Create a first-class internal `Transport` test layer and a public-service test layer. The same
stateful fake object can be installed under both the production service tag and a test-control tag
with `Layer.effectContext`.

Use:

- `TestClock` for reconnect delays, request timeouts, drain deadlines, retry schedules, ping
  timers, and KV/object watch heartbeats;
- `Deferred` for connection-ready, subscription-installed, publish-observed, and finalizer-ran
  synchronization;
- bounded `Queue` for test-controlled messages and captured writes;
- `Ref` for pending counts, handler state, and deterministic observations;
- finite `Stream.take(n)` plus collection only in tests;
- `@effect/vitest` `it.effect` for Effect tests and Tstyche for public type tests, matching the
  pinned Effect repository's own test conventions
  (`effect@648f566d:.patterns/testing.md:5-18` and
  `effect@648f566d:.patterns/testing.md:35-44`).

Minimum behavior matrix before the first public release:

1. connection acquisition success/failure and release on every exit;
2. interruption during connect, request, subscription, KV watch, object get, and object put;
3. request timeout removes the inbox subscription exactly once;
4. finite subscription overflow fails visibly and reports the right counters;
5. lossy overflow modes drop exactly the documented side and increment metrics;
6. malformed subject/key/name is rejected before transport use;
7. payload encode failure performs no publish;
8. payload decode failure is typed and does not kill unrelated subscriptions;
9. no implicit retry after ambiguous Core publish/request failure;
10. bounded connect/read retry has exact attempts and virtual delays;
11. KV not-found versus tombstone versus purge versus decode failure;
12. KV CAS conflict is preserved and never automatically retried;
13. object content is chunk-streamed without whole-object collection;
14. interrupted object put stops input and attempts cleanup exactly once;
15. layer close interrupts all owned fibers and leaves no subscription/listener behind;
16. type tests preserve payload inference and enumerate public error unions.

Integration suites should later cover a real NATS server matrix, disconnect/reconnect, slow
consumer behavior, authentication failure, server restart, JetStream unavailability, and KV/Object
Store compatibility. Those are not substitutable with unit fakes.

## Decisions to freeze after the NATS mapping

The Effect shape can be adopted now, but these details must remain open until verified:

1. underlying implementation: wrap an official TypeScript client, fork it, or implement the wire
   protocol;
2. exact meaning of Core publish completion, flush, drain, and reconnect;
3. async-iterator versus callback subscription boundary and whether delivery can actually pause;
4. pending-message and pending-byte controls available from the chosen transport;
5. subject, queue, bucket, key, and object-name grammar;
6. headers representation and duplicate-header behavior;
7. request no-responders and timeout mapping;
8. KV revision range, tombstone/purge events, watch resume, and CAS failure mapping;
9. Object Store chunk, digest, link, overwrite, resume, and cleanup semantics;
10. Node-only versus Node/Bun/Deno/browser runtime and transport package split;
11. tracing/metrics interface and OpenTelemetry dependency policy;
12. public package name and NATS-project governance.

## Concise developer implications

- Users acquire NATS with a Layer; they do not manually remember to close it.
- One-shot operations return `Effect`; ongoing values and large bodies return `Stream`.
- Raw bytes remain first-class; typed payloads cross explicit Schema codecs.
- Every expected failure is catchable by a stable tag. Interruption remains interruption.
- Every buffer is finite and named. Data loss is never an invisible default.
- Retry is operation-specific, bounded, and absent where acceptance is ambiguous.
- KV conflicts and Object Store partial writes are domain states, not generic network errors.
- Deterministic tests use `TestClock` and synchronization primitives, not wall-clock sleeps.
- The transport adapter can change without changing public domain types.
