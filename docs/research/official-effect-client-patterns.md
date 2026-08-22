# Official Effect client patterns applicable to NATS

Status: pinned-source pattern map; implementation guidance, not a frozen NATS API.

## Source basis

This report inspects the official read-only Effect checkout at
`C:\Users\kokok\Dev\Foldable\effect-foldable`, commit
`648f566dd259898e7697c7fcb796183ccbc474ab`. The package at that pin is Effect
`4.0.0-rc.111` (`effect@648f566d:packages/effect/package.json:2-4`). Citations of the form
`effect@648f566d:path:line` refer to that checkout.

The most useful result is not one client to imitate wholesale. An Effect-native NATS client should
combine five narrower first-party patterns:

1. Socket and NodeSocket for scoped transport acquisition and typed read/write/open/close failure;
2. RPC Client for a transport seam, request correlation, interruption cleanup, and bounded streamed
   responses;
3. Persistence KeyValueStore for a raw byte/string store plus a schema-aware view;
4. FileSystem and HttpClientResponse for large payloads represented as `Stream<Uint8Array>` and
   `Sink`, not collected buffers;
5. WorkerError and its tests for schema-backed reason unions that preserve adapter failures as
   typed errors.

Several first-party internals deliberately optimize for their own protocols and must **not** be
copied into NATS: unbounded queues, handler-per-frame concurrency, unlimited retry schedules,
`Effect.orDie` at transport/schema boundaries, process-local request counters, and get-then-set
"modify" on distributed KV.

## Pattern map

| Effect exemplar                      | Closest NATS responsibility             | Copy this pattern                                                                   | Do not copy this pattern                                                           |
| ------------------------------------ | --------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `unstable/socket/Socket`             | TCP/WebSocket transport                 | scoped writer, typed reason union, explicit run lifecycle                           | its unbounded channel queue                                                        |
| `platform/node-shared/NodeSocket`    | Node TCP adapter                        | register close/listener finalizers before connection use                            | concurrent effect handler for each data event                                      |
| `unstable/rpc/RpcClient`             | request/reply and subscription dispatch | protocol service seam, correlation map, interruption cleanup, bounded stream queues | counter IDs, unbounded default reconnect, `orDie` on encode/write                  |
| `unstable/persistence/KeyValueStore` | raw KV plus typed values                | raw primitive capability with a Schema-derived view and `Option` absence            | generic get-then-set `modify` as if atomic                                         |
| `FileSystem` / `HttpClientResponse`  | Object Store bodies                     | lazy byte streams, scoped acquisition, optional Sink upload surface                 | whole-body accessors as the default                                                |
| `unstable/workers/WorkerError`       | stable NATS error family                | schema reason variants plus wrapper and roundtrip tests                             | constructor casts and live transport objects in error schemas                      |
| `HttpClient`                         | public client façade                    | small service, pure combinators, operation-specific retry classifier                | HTTP-style universal preprocess/postprocess hooks before NATS invariants are known |

## 1. Service and Layer design

### Verified first-party patterns

Effect's Socket API exposes one service value with a read loop and a scoped writer. The writer
requires `Scope.Scope`, which makes its lifetime visible in the type
(`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:47-90`). The platform adapter
constructs the socket with `Socket.make` and turns the resulting acquisition effect into a layer
with `Layer.effect(Socket.Socket)`
(`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:223-236` and
`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:259-269`).

RPC separates the logical client from its transport. `RpcClient.Protocol` is a `Context.Service`
whose contract is only a receive loop, send operation, and capability flags; HTTP, Socket, and
worker implementations are layered behind that service
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:836-867` and
`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:982-1001`).

Persistence likewise separates the typed service from its raw backing service. Both are
`Context.Service` classes, and store acquisition requires Scope
(`effect@648f566d:packages/effect/src/unstable/persistence/Persistence.ts:51-63` and
`effect@648f566d:packages/effect/src/unstable/persistence/Persistence.ts:99-107`). Its implementation
layer captures the layer scope for background cleanup without exposing Scope as a public service
member (`effect@648f566d:packages/effect/src/unstable/persistence/Persistence.ts:133-150` and
`effect@648f566d:packages/effect/src/unstable/persistence/Persistence.ts:182-197`).

HttpClient shows a small effectful client interface whose error and environment remain generic,
with a single service tag plus module-level accessors
(`effect@648f566d:packages/effect/src/unstable/http/HttpClient.ts:70-116` and
`effect@648f566d:packages/effect/src/unstable/http/HttpClient.ts:139-168`).

### Copy this pattern

- Define one internal `NatsTransport` service: ordered inbound bytes, serialized outbound bytes,
  close/drain, and transport status.
- Define one scoped `NatsConnection` owner over it. Core, KV, and Object Store are capability
  services/views that close over this owner; they do not each acquire a connection.
- Use `Layer.effect(Service, acquire)` for each real service. Capture the layer Scope internally
  when methods need to fork connection-lifetime work.
- Keep platform selection behind transport layers such as `layerNodeTcp`, `layerWebSocket`, and a
  fake transport layer. Do not leak Node `net.Socket`, WebSocket, or a third-party client type.
- Keep module-level helpers as thin accessors or pure transformations over the service. The raw
  service contract should remain small.

### Do not copy this pattern

- Do not make the transport a `Context.Reference` with a default. A connection is authority and a
  resource, so it must be provided explicitly.
- Do not expose the captured Scope or an underlying socket through the public service.
- Do not copy HttpClient's universal preprocess/postprocess transformation surface yet. NATS has
  protocol state, subscription identity, and ambiguous publish outcomes; unconstrained transforms
  could invalidate invariants. Add narrow combinators only when a real use case is proven.
- Do not provide one broad client object containing connection, Core, KV, Object Store,
  administration, testing controls, and adapter handles.

## 2. Scoped socket and resource ownership

### Verified first-party patterns

`NodeSocket.makeNet` registers a finalizer before it creates the Node connection. The finalizer
closes or destroys the socket if connection setup succeeded, so interruption during open cannot
leak it (`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:45-98`).

`NodeSocket.fromDuplex` scopes listener installation. Its finalizer removes every registered
listener, and open timeout is mapped into a typed `SocketOpenError`
(`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:119-160`). The writer is itself an
`Effect.acquireRelease` resource and ends the current socket when its scope closes
(`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:203-236`).

The generic WebSocket implementation also brackets creation with `Effect.acquireRelease`, closing
the WebSocket on release (`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:573-596`).
It waits for readiness with `Deferred`, applies an explicit open timeout, removes listeners on
error/close, and resets its mutable connection latch in an `ensuring`
(`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:628-735`).

The production test closes a socket-server Scope even when an accepted socket has not started its
run loop, proving that acquisition—not a later `run` call—owns cleanup
(`effect@648f566d:packages/platform/node/test/NodeSocket.test.ts:23-43`). A Layer test separately
shows an `Effect.acquireRelease` service is acquired and released once even when the same layer is
merged with itself (`effect@648f566d:packages/effect/test/Layer.test.ts:32-39` and
`effect@648f566d:packages/effect/test/Layer.test.ts:609-624`).

### Copy this pattern

- Register cleanup before beginning connect/open.
- Treat connect, protocol reader, subscription dispatcher, reconnect supervision, and status
  publisher as children of one connection Scope.
- Remove all socket/client listeners on every exit, not only successful drain.
- Use a readiness `Deferred` or `Latch` for deterministic transition from acquiring to ready.
- Make explicit `drain` an operation with a typed result; retain close in the non-failing finalizer
  as a safety net.
- Make finalization idempotent because explicit drain, remote close, connection failure, and parent
  interruption may race.

### Do not copy this pattern

- Do not delay ownership until the first subscription or read-loop invocation. A connected but
  unused NATS client must still close with its layer.
- Do not make callers manually invoke `close` for correctness.
- Do not return finalizer failures as typed errors: finalizers cannot do so. Log/measure close
  failures and expose explicit drain when the caller needs an observable result.
- Do not use a detached fiber for reconnect or reader loops.

## 3. Ordered protocol input and duplex bridges

### Verified first-party patterns

Socket exposes binary/string read loops separately and a scoped writer, while `Socket.make`
derives the alternate text/binary views from a raw implementation
(`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:60-124` and
`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:125-149`). It also provides a
bidirectional `Channel` adapter, forks its writer into the channel Scope, closes the writer Scope,
and joins socket errors into the inbound queue
(`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:379-435`).

However, that adapter uses `Queue.make` without a capacity and enqueues inbound values with
`Queue.offerUnsafe` (`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:396-430`);
`Queue.make` is unbounded when capacity is omitted
(`effect@648f566d:packages/effect/src/Queue.ts:406-453`). The Node adapter invokes each effectful
data handler via `FiberSet.runtime`, so handler effects may run concurrently
(`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:153-169`). These are valid choices
for that generic socket abstraction but not sufficient NATS protocol guarantees.

### Copy this pattern

- Isolate transport framing from protocol parsing. Let `NatsTransport` exchange raw ordered byte
  chunks; let a single protocol engine own parser state and dispatch complete messages.
- Keep the channel/queue bridge internal. Public users see `Effect` operations and per-subscription
  `Stream`s, not a bidirectional protocol Channel.
- Acquire the writer in Scope and serialize writes through one ordered path.
- Map read, parse, write, open, and close failures into distinct typed reasons before they leave
  the adapter boundary.

### Do not copy this pattern

- Do not use the generic `Socket.toChannel` unchanged: its inbound queue is unbounded.
- Do not fork an independent effect for each TCP `data` event. NATS parser state and byte ordering
  require serialized processing; TCP chunk boundaries are not message boundaries.
- Do not parse one chunk as one protocol message.
- Do not call `Queue.offerUnsafe` into a finite queue and assume it backpressures a synchronous
  external callback. If the producer cannot suspend, overflow needs an explicit fail/drop/close
  policy.

## 4. Request correlation, subscriptions, and bounded queues

### Verified first-party patterns

RPC Client keeps a request-id map whose entries are either one-shot callbacks or streaming queues.
It registers a parent-scope finalizer that interrupts or fails every outstanding entry on shutdown
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:268-305`). One-shot request
interruption deletes the correlation entry, interrupts the send fiber, and emits a protocol
interrupt (`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:349-424`).

Streaming requests register an exit finalizer, allocate `Queue.bounded(streamBufferSize)`, and
expose it as `Stream.fromQueue`; the public default in this path is 16
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:307-346` and
`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:428-499`). Incoming streamed chunks
are admitted with `Queue.offerAll`; a protocol acknowledgement is sent only after that offer
completes. Terminal exits remove the entry and end or fail the queue
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:560-590`).

Effect's callback-stream tests prove a finite `bufferSize: 2` suspends Effect-owned offers until
pulls release capacity (`effect@648f566d:packages/effect/test/Stream.test.ts:111-135`). Queue tests
show the distinct suspend, dropping, and sliding behavior, and prove interruption removes a
blocked offer rather than leaving latent data
(`effect@648f566d:packages/effect/test/Queue.test.ts:50-70` and
`effect@648f566d:packages/effect/test/Queue.test.ts:92-131`).

### Copy this pattern

- Keep one connection-owned correlation/dispatcher table.
- Register removal before sending a request or creating a subscription.
- On request interruption: remove the inbox/correlation entry, cancel its timeout, and unsubscribe
  the inbox exactly once.
- Give every subscription/watch a finite private queue and expose only its dequeue as a Stream.
- End or fail that queue when the server, connection, subscription, or parent Scope terminates.
- If a protocol acknowledgement is under this client's control, acknowledge only after admission
  to the bounded queue. Document that Core subscription delivery has no such acknowledgement.
- Test each overflow strategy and interruption of blocked offers directly.

### Do not copy this pattern

- Do not use RPC Client's process-local monotonic request counter for NATS inboxes. Generate
  collision-resistant inbox subjects according to official NATS rules.
- Do not expose the queue's enqueue side or mutable correlation map.
- Do not default Core subscription, KV watch, or object-chunk queues to unbounded.
- Do not call a synchronous callback bridge "backpressured" unless the selected NATS transport can
  actually pause input. Otherwise fail and unsubscribe on overflow by default.
- Do not use dropping or sliding for durable data unless the caller explicitly selects lossy
  observation and receives loss diagnostics.

## 5. Raw values plus Schema-aware views

### Verified first-party patterns

Effect's persistence KeyValueStore starts with primitive string and `Uint8Array` operations behind
one service (`effect@648f566d:packages/effect/src/unstable/persistence/KeyValueStore.ts:30-95` and
`effect@648f566d:packages/effect/src/unstable/persistence/KeyValueStore.ts:197-225`). The
schema-aware view is a separate adapter. It carries Schema decoding/encoding services through the
Effect environment, returns `Option` for absence, decodes on get, and encodes before set
(`effect@648f566d:packages/effect/src/unstable/persistence/KeyValueStore.ts:699-760` and
`effect@648f566d:packages/effect/src/unstable/persistence/KeyValueStore.ts:762-810`).

The same module's default `modify` is implemented as get, pure transform, then set
(`effect@648f566d:packages/effect/src/unstable/persistence/KeyValueStore.ts:224-255`). That is useful
for local backends but is not an atomic distributed compare-and-set.

### Copy this pattern

- Make Core payloads, KV values, and Object Store chunks raw `Uint8Array` at the foundation.
- Add value-level Schema codecs/views over raw capabilities instead of parameterizing the
  connection service itself.
- Preserve `Schema.SchemaError` or map it once into stable `EncodeError`/`DecodeError`; never defect
  on invalid remote bytes.
- Preserve Schema encoding and decoding service requirements in the `R` channel.
- Return `Option.none` only for verified KV absence. Tombstone, purge, conflict, permission,
  timeout, and decode failure remain distinct.
- Encode fully before Core/KV transport publication begins.

### Do not copy this pattern

- Do not implement NATS KV `modify` as get-then-set and call it atomic. Expose revision-aware
  `create`/`update(expectedRevision)` and map server conflict explicitly.
- Do not reuse plain `string` for bucket, key, subject, queue group, object name, and revision.
- Do not Base64-encode raw Core payloads merely because the JSON schema view does so.
- Do not force one schema codec on an entire connection; codecs belong to individual operations,
  typed topics, buckets, or object metadata.

## 6. Object Store content as Stream and Sink

### Verified first-party patterns

FileSystem exposes large reads as `Stream<Uint8Array, PlatformError>` and writes as a byte `Sink`,
with chunk and internal buffer sizes explicit (`effect@648f566d:packages/effect/src/FileSystem.ts:292-328`). Its
default stream opens a scoped file, pulls bounded chunks lazily, and stops at EOF without
collecting the file (`effect@648f566d:packages/effect/src/FileSystem.ts:713-747`). File handles are
typed as scoped resources that automatically close with Scope
(`effect@648f566d:packages/effect/src/FileSystem.ts:220-228`).

HttpClientResponse similarly exposes body bytes as a Stream and lifts an effectful response into a
stream with `Stream.unwrap`
(`effect@648f566d:packages/effect/src/unstable/http/HttpClientResponse.ts:140-148`). The Web response
implementation converts the platform `ReadableStream` into an Effect Stream and maps read failures
into a typed decode reason (`effect@648f566d:packages/effect/src/unstable/http/HttpClientResponse.ts:296-317`).

### Copy this pattern

- Expose Object Store reads as lazy `Stream<Uint8Array, ObjectReadError>`.
- Consider a paired `sink(metadata): Sink<ObjectInfo, Uint8Array, ...>` as the low-level upload
  surface, with `put(metadata, contentStream)` as a convenience wrapper.
- Acquire chunk subscriptions/readers inside the stream Scope so early consumer termination
  unsubscribes and releases resources.
- Make chunk size and local queue capacity explicit independently of the server's object chunk
  size.
- Return success only after the verified terminal metadata/digest operation completes.

### Do not copy this pattern

- Do not make `arrayBuffer`, `Uint8Array`, or `runCollect` the primary Object Store API.
- Do not let stream cancellation report successful object completion.
- Do not hide partial-upload cleanup in a fallible finalizer. Attempt cleanup on the operation path
  and keep finalization as the non-failing safety net.
- Do not assume a platform stream's backpressure propagates through JetStream and the server.

## 7. Typed Schema error families

### Verified first-party patterns

Socket models read, write, open, and close as separate `Schema.Error` reason classes, combines them
with `Schema.Union`, and wraps them in `Schema.TaggedError`
(`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:217-321` and
`effect@648f566d:packages/effect/src/unstable/socket/Socket.ts:323-345`). This allows code to match
the outer client family and then the precise reason.

WorkerError uses the same basic shape: distinct schema-backed reason classes, a schema union, and
an outer wrapper (`effect@648f566d:packages/effect/src/unstable/workers/WorkerError.ts:31-116` and
`effect@648f566d:packages/effect/src/unstable/workers/WorkerError.ts:118-147`). Its tests encode and
decode individual reasons, the union, and the wrapper, then verify a thrown adapter `postMessage`
failure appears as a typed send reason with no defect
(`effect@648f566d:packages/effect/test/unstable/workers/WorkerError.test.ts:54-89` and
`effect@648f566d:packages/effect/test/unstable/workers/WorkerError.test.ts:92-128`).

Socket tests likewise distinguish open timeout, close code/reason, and send failure at runtime
(`effect@648f566d:packages/platform/node/test/NodeSocket.test.ts:82-92` and
`effect@648f566d:packages/platform/node/test/NodeSocket.test.ts:138-199`).

### Copy this pattern

- Define schema-backed reason families at adapter boundaries: Connect, Read, Parse, Write,
  Authorization, Close, SlowConsumer, Timeout, NoResponders, KVConflict, and ObjectPartialWrite.
- Wrap reasons only where callers benefit from one catch tag such as `NatsError`, while preserving
  the reason `_tag` for policy and diagnostics.
- Store stable operation labels and safe protocol identifiers. Use `Schema.Defect()` for external
  causes; never include credentials or live socket/request objects.
- Roundtrip every public error schema and separately prove adapter exceptions enter the typed
  failure channel rather than the defect channel.
- Preserve interruption. It is not a NATS error reason.

### Do not copy this pattern

- Do not copy the first-party wrapper constructors' `as any` cause-propagation casts into new code.
  Prefer a simple schema shape that does not require constructor override.
- Do not use `Data.TaggedError` when the public error must be encoded, persisted, or schema-tested.
- Do not collapse all NATS failures into `TransportError` or a message string.
- Do not attach third-party exception classes as the public contract.

## 8. Retry and scheduling

### Verified first-party patterns

HttpClient implements retry as an immutable client transform and allows retry policy to distinguish
typed errors from responses
(`effect@648f566d:packages/effect/src/unstable/http/HttpClient.ts:822-879` and
`effect@648f566d:packages/effect/src/unstable/http/HttpClient.ts:881-985`). This is a useful
classification pattern: retry is not an unconditional loop around every operation.

RPC Socket protocol retries only after it classifies a `SocketOpenError` as transient and invokes a
retry hook; its reader/reconnect loop is scoped
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:1108-1144`). Its default schedule,
however, is `min(exponential, spaced)` with no finite bound
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:1163-1166`). RPC also turns some
schema and transport failures into defects with `Effect.orDie`
(`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:682-693` and
`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:719-739`, plus
`effect@648f566d:packages/effect/src/unstable/rpc/RpcClient.ts:1146-1160`).

Persistence demonstrates a scheduled maintenance pass: repeat while a full cleanup batch is
returned, then repeat the pass on a spaced schedule in a scoped fiber
(`effect@648f566d:packages/effect/src/unstable/persistence/Persistence.ts:676-690`). Stream retry
tests show that resources are finalized before restart and use `TestClock.adjust` to verify exact
exponential delays and schedule reset after a successful pull
(`effect@648f566d:packages/effect/test/Stream.test.ts:4511-4557` and
`effect@648f566d:packages/effect/test/Stream.test.ts:4559-4594`).

### Copy this pattern

- Classify retry eligibility by typed reason at the narrowest boundary.
- Own reconnect as one scoped connection supervisor, not as a wrapper around every public method.
- Use bounded Schedule policies with jitter and an attempt or total-duration cap.
- Test exact virtual delays, finalization before restart, and schedule reset behavior.
- Keep exhausted failures visible and annotate retry attempts with operation/reason/server.

### Do not copy this pattern

- Do not copy RPC's unbounded default reconnect schedule. NATS may support continuous reconnect as
  a deliberate option, but it must be configured explicitly and remain observable.
- Do not `orDie` on payload encode/decode, socket write, authentication, slow-consumer, KV, or
  Object Store failures.
- Do not automatically retry Core publish/request or revision-conditional KV/Object writes when
  server acceptance can be ambiguous.
- Do not stack an Effect retry loop over a third-party client's reconnect loop without one owner
  and one combined attempt budget.

## 9. Deterministic lifecycle and conformance tests

### Verified first-party patterns

A callback Stream test registers a finalizer, interrupts the consumer fiber, and asserts cleanup
ran (`effect@648f566d:packages/effect/test/Stream.test.ts:56-75`). Deferred tests show that
interrupting one waiter removes it while a live waiter still completes
(`effect@648f566d:packages/effect/test/Deferred.test.ts:102-119`). TestClock tests fork the delayed
effect first and advance virtual time without wall-clock sleeping
(`effect@648f566d:packages/effect/test/TestClock.test.ts:18-29`).

Persistence provides a reusable KeyValueStore conformance suite parameterized by a Layer, so the
same set/get/binary/remove/clear contract runs against multiple implementations
(`effect@648f566d:packages/effect/test/unstable/persistence/KeyValueStoreTest.ts:1-34` and
`effect@648f566d:packages/effect/test/unstable/persistence/KeyValueStoreTest.ts:36-84`).

### Copy this pattern

- Build reusable `CoreContract`, `KvContract`, and `ObjectStoreContract` suites parameterized by a
  Layer. Run them against the in-memory fake, wrapped third-party adapter, and real NATS server.
- Use `Deferred` for connected, subscription-installed, first-byte, publish-observed, and
  finalizer-ran gates.
- Use `TestClock` for connect timeout, request timeout, reconnect, ping, drain deadline, and retry.
- Assert cleanup after success, typed failure, defect, interruption, timeout, remote close, and
  parent-scope close.
- Test bounded queue fill, overflow, recovery after consumer progress, and interruption of blocked
  producers.
- Type-test error/environment channels and Schema encoding/decoding service propagation.

### Do not copy this pattern

- Do not use real sleeps to coordinate fake transport tests.
- Do not make only happy-path fake tests. Real-server suites must cover server restart,
  disconnect/reconnect, slow consumer, permission errors, JetStream unavailable, KV tombstones and
  conflicts, and interrupted object transfer.
- Do not share one mutable test connection across cases unless its Scope and state are reset by the
  test Layer.
- Do not treat integration coverage as a replacement for deterministic unit and lifecycle tests.

## Repo-local implementation rules

These are suitable for inclusion in a future repository skill or `AGENTS.md` after design review.

### Copy these patterns

1. **One Scope owns one connection.** Acquire transport with `Effect.acquireRelease`; reader,
   reconnect, dispatcher, status, and timers are scoped children.
2. **One protocol seam.** Public Core/KV/Object services depend on internal `NatsTransport`, never
   a concrete Node/WebSocket/third-party type.
3. **One serialized parser.** TCP chunks enter one ordered protocol state machine.
4. **Finite queues only.** Every subscription/watch/request stream declares capacity and overflow
   behavior; users receive a Stream, never the enqueue side.
5. **Correlation cleanup first.** Register request/subscription cleanup before send; remove it on
   reply, timeout, interruption, remote close, or connection close.
6. **Raw then typed.** Raw bytes are foundational; Schema codecs are explicit adapters and preserve
   typed failures/services.
7. **Stream large bodies.** Object data is Stream/Sink based and cancellation releases the
   underlying subscription/upload.
8. **Reasoned errors.** Public expected errors are schema-backed reason unions; adapter throws are
   mapped once; interruption is preserved.
9. **Retry by proof.** Only typed transient reasons at idempotent boundaries get a bounded Schedule.
10. **Contract suites.** Every implementation must pass the same Core/KV/Object behavior and
    lifecycle suites.

### Do not copy these patterns

1. No unbounded message/reconnect/pending-write buffers.
2. No handler fiber per TCP chunk and no assumption that one chunk is one NATS message.
3. No detached connection fibers and no cleanup dependent on a later `start` call.
4. No `Effect.orDie`, unchecked cast, or thrown exception to erase public protocol failures.
5. No automatic retry for ambiguous publish/request/CAS/object mutations.
6. No get-then-set "atomic" KV modify.
7. No process counter as a network-global inbox or operation identifier.
8. No silent dropping/sliding for durable data streams.
9. No whole-object collection in production Object Store operations.
10. No public adapter handles, live socket objects, credentials, or unstable transport exceptions in
    schemas/errors.

## Recommended first implementation slice

Implement and test these seams before Core request/reply, KV, or Object Store:

```text
NatsTransport
  acquire / close
  ordered inbound byte Stream or run loop
  serialized write(bytes)
  status Stream

ProtocolEngine
  incremental INFO / CONNECT / PING / PONG / +OK / -ERR / MSG / HMSG parser
  one serialized input loop
  bounded pending-write admission
  scoped dispatcher registry

NatsConnection Layer
  acquisition and readiness Deferred
  scoped reader/reconnect/ping fibers
  explicit flush and drain
  final close safety net

Core subset
  publish raw bytes
  subscribe raw bytes with finite queue
  unsubscribe on Stream finalization
```

Acceptance for that slice should prove: ordered parsing across arbitrary TCP chunk splits, no
resource leaks on every exit, visible slow-consumer overflow, no post-interruption delivery,
serialized writes, typed adapter errors, and deterministic timeout/reconnect timing. The later KV
and Object Store layers should then be built over the verified JetStream primitives rather than
inventing independent transport/lifecycle behavior.
