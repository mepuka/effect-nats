# Official NATS.js reference map

## Scope and pin

This is a read-only map of the official [`nats-io/nats.js`](https://github.com/nats-io/nats.js) repository. The observations below are pinned to commit [`3a7b736b175d9a387130210c88f851d71e1b014f`](https://github.com/nats-io/nats.js/tree/3a7b736b175d9a387130210c88f851d71e1b014f) (upstream `main` when surveyed, commit date 2026-06-24). “Fact” means the pinned source says or implements it. “Recommendation” is an inference for this Effect-native project.

## Primary result

**Fact.** NATS.js v3 is a modular, multi-runtime monorepo. Runtime-neutral Core implements protocol behavior; a transport supplies sockets/DNS; JetStream depends on Core; KV and Object Store are JetStream-backed materialized views. The public module graph is deliberately smaller than the implementation graph. The project describes its official core as thin, stable, performance-oriented, and kept in cross-client parity; opinionated JavaScript abstractions belong above that layer. [Repository overview, lines 25–74](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/README.md#L25-L74), [Core-versus-Orbit policy, lines 108–155](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/README.md#L108-L155)

**Recommendation.** Build the Effect-native client as a strict semantic layer over official public `@nats-io/*` contracts first. Keep transport/protocol parity beneath it and Effect services, typed errors, schemas, scopes, schedules, and streams above it. If full wire reimplementation remains a goal, make that a separately tested refinement target rather than coupling it to the first public API.

## Repository and package layout

| Package/directory | Upstream responsibility | Dependency direction |
|---|---|---|
| `core` / `@nats-io/nats-core` | Runtime-neutral Core NATS connection, protocol, parser, auth, messages, headers, subscriptions, requests, reconnects | Foundation; only `nkeys` and `nuid` runtime dependencies at this pin. [package, lines 1–43](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/package.json#L1-L43) |
| `transport-node` | Node/Bun TCP/TLS/DNS transport; re-exports Core | Depends on Core. [README, lines 13–16 and 38–42](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/transport-node/README.md#L13-L42) |
| `transport-deno` | Deno TCP/DNS transport | Depends on Core’s internal transport seam. [connect.ts, lines 15–46](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/transport-deno/src/connect.ts#L15-L46) |
| `core` WebSocket transport | W3C WebSocket implementation and `wsconnect` | Lives in Core because several runtimes expose W3C WebSocket. [transport contract, lines 36–59](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/ws_transport.ts#L36-L59), [`wsconnect`, lines 351–369](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/ws_transport.ts#L351-L369) |
| `jetstream` / `@nats-io/jetstream` | JetStream management, publish, stream and consumer APIs | Depends on Core. [package, lines 1–37](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/package.json#L1-L37) |
| `kv` / `@nats-io/kv` | KV API materialized over a JetStream stream | Depends on JetStream and Core. [README, lines 12–18](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/README.md#L12-L18), [package, lines 34–38](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/package.json#L34-L38) |
| `obj` / `@nats-io/obj` | Chunked, checksummed Object Store API materialized over JetStream | Depends on JetStream, Core, and `js-sha256`. [README, lines 12–18](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/README.md#L12-L18), [package, lines 34–39](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/package.json#L34-L39) |
| `services` | NATS service framework | Separate optional module; not needed for Core/KV/Object Store. [root README, lines 40–48](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/README.md#L40-L48) |
| `nst` | Internal/unsupported real-server and fake-server test utilities | Published for workspace tests, but explicitly not stable public API. [nst README, lines 1–21](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/nst/README.md#L1-L21) |

**Fact.** The root is both an npm workspace (`core`, `jetstream`, `kv`, `obj`, `services`, `nst`, `transport-node`) and a Deno workspace that additionally includes `transport-deno`. [package.json, lines 1–10](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/package.json#L1-L10), [deno.json, lines 124–148](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/deno.json#L124-L148)

**Fact.** Each functional package publishes a narrow `.` entry point and a separate `./internal` entry point. Public `mod.ts` files explicitly re-export values and types; internal modules expose implementation classes and transport hooks. [Core package exports, lines 9–13](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/package.json#L9-L13), [Core public exports, lines 16–113](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/mod.ts#L16-L113), [Core internal exports, lines 16–84](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/internal_mod.ts#L16-L84)

**Recommendation.** Mirror the domain seams (`core`, `jetstream` substrate, `kv`, `obj`) but do not expose an `internal` path as an accidental compatibility promise. Internal Effect interpreters can live in package-private modules. Depend only on upstream public paths unless implementing an isolated transport adapter that necessarily uses the documented internal seam.

## Core NATS: protocol and runtime seams

### Transport boundary

**Fact.** Core injects a single `TransportFactory`; a `Transport` is an `AsyncIterable<Uint8Array>` with connect, send, close, disconnect, encryption, and lifecycle methods. The factory can also provide default-port, URL parsing, and DNS resolution. [transport.ts, lines 27–91](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/transport.ts#L27-L91)

**Fact.** Node’s `connect()` installs `NodeTransport` and its DNS resolver, then delegates to `NatsConnectionImpl.connect`; Deno does the same with its own transport. [Node connect.ts, lines 36–54](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/transport-node/src/connect.ts#L36-L54), [Deno connect.ts, lines 30–46](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/transport-deno/src/connect.ts#L30-L46)

**Recommendation.** Represent transport acquisition as an Effect `Layer` and connection ownership as a `Scope`. Keep TCP/TLS/DNS/WebSocket capabilities below the protocol service. Avoid a global mutable transport registration in the Effect API; inject the transport service explicitly so parallel tests and multiple runtimes cannot race over process-wide state.

### Wire protocol and connection engine

**Fact.** The parser is a byte-oriented state machine emitting `OK`, `ERR`, `MSG`/header-message, `INFO`, `PING`, and `PONG` events. It accepts split frames and enforces a 64 MiB parser numeric bound. [parser.ts, lines 20–100](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/parser.ts#L20-L100), [parser states, lines 687–721](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/parser.ts#L687-L721)

**Fact.** `ProtocolHandler` owns server discovery, subscriptions, request multiplexing, parser dispatch, heartbeats, feature detection, reconnect policy, metrics, and an outbound `DataBuffer`. It reads transport chunks into the parser, restores subscriptions after reconnect, and sends `CONNECT` followed by `PING` during the initial handshake. [protocol.ts, lines 392–469](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L392-L469), [dial/restore, lines 575–601](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L575-L601), [INFO/CONNECT handshake, lines 853–882](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L853-L882)

**Fact.** Core publish is a synchronous enqueue operation, not an acknowledgement. Commands are microtask-flushed, with an internal 32 KiB flush threshold; `flush()` appends `PING` and resolves on matching `PONG`. Payloads are rejected locally when larger than the server-advertised `max_payload`. [flush threshold, lines 60–65](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L60-L65), [command buffering, lines 928–945](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L928-L945), [publish encoding and max payload, lines 948–1004](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L948-L1004), [flush, lines 1066–1074](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L1066-L1074)

**Recommendation.** Preserve three distinct operations and name them honestly: Core `publish` means accepted into the client path, Core `flush` means a server round trip, and JetStream `publish` means a JetStream `PubAck`. Do not model any of these as subscriber processing or external-effect completion.

**Fact.** Reconnect policy includes server rotation, delay/jitter, maximum attempts, an optional server-choice hook, subscription restoration, and discriminated connection status events. Forced reconnect can lose in-flight inbound/outbound messages and rejects in-flight requests. [protocol.ts, lines 637–740](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/protocol.ts#L637-L740), [NatsConnection reconnect caveats, lines 524–542](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/core.ts#L524-L542)

### Core public API conventions

**Fact.** The central interface exposes `publish`, `subscribe`, single/multi request, `flush`, ordered `drain`, `close`, status/metrics, RTT, server pool control, and `AsyncDisposable`. Subscriptions and status feeds are async iterables; subscriptions also expose explicit stop/drain/lifecycle inspection. [NatsConnection, lines 377–480](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/core.ts#L377-L480), [Subscription, lines 647–705](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/core.ts#L647-L705)

**Fact.** Messages remain bytes (`Uint8Array`) with explicit `string()` and `json<T>()` convenience decoders. Status is a discriminated union. Callback handlers are intentionally typed `void | Promise<never>`; asynchronous handling is expected through async iteration rather than awaited callbacks. [Msg, lines 576–619](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/core.ts#L576-L619), [Status and callback, lines 62–88](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/core.ts#L62-L88)

**Fact.** Errors are specific classes (`InvalidSubjectError`, `ConnectionError`, `ProtocolError`, `RequestError`, `TimeoutError`, `NoRespondersError`, permission/auth variants), with causes used to retain underlying failures. [argument/operation errors, lines 23–71](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/errors.ts#L23-L71), [connection/request errors, lines 164–235](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/errors.ts#L164-L235)

**Recommendation.** Map async iterables to scoped Effect `Stream`s and callbacks to explicit low-level escape hatches. Keep payloads as bytes at the transport boundary; add `Schema` codecs as opt-in transformations. Use a closed tagged error algebra that preserves the upstream error as `cause`, rather than collapsing failures to `unknown` or using error-message matching.

**Fact.** The internal queued iterator tracks received, processed, pending, and in-flight counts, stops itself when iteration exits, and rejects concurrent iteration or callback/iterator mixing. [iterator state, lines 21–59](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/queued_iterator.ts#L21-L59), [iteration/finalization, lines 87–165](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/queued_iterator.ts#L87-L165)

**Recommendation.** Make stream buffering explicit and bounded in the Effect layer. Cancellation/finalization must stop or drain the underlying subscription/consumer. Expose buffer, overflow, concurrency, and acknowledgement policy as data rather than hiding unbounded queues behind a `Stream` constructor.

## JetStream substrate

**Fact.** JetStream is documented as at-least-once persistence. `jetstream(nc)` returns the data client synchronously, while `jetstreamManager(nc)` returns an administrative client after optionally checking account/API availability. The client separates `streams`, `consumers`, and reliable `publish`; the manager separates stream, consumer, and direct administration APIs. [JetStream README, lines 12–22](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/README.md#L12-L22), [jsclient.ts, lines 190–242](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/src/jsclient.ts#L190-L242)

**Fact.** JetStream publish is implemented as Core request/reply with timeout/retry/backoff, parses the response into `PubAck`, and reports stream, sequence, and duplicate status. [jsclient.ts, lines 387–456](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/src/jsclient.ts#L387-L456), [PubAck type, lines 96–116](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/src/types.ts#L96-L116)

**Fact.** Consumer APIs are deliberately shaped around buffering strategy: next one, fetch a bounded batch, or maintain a replenished buffer. The official guidance warns that clients must not buffer more than they can process and acknowledge within `AckWait`, because missed acknowledgements cause redelivery and slow clients may be disconnected. [JetStream README, lines 243–301](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/README.md#L243-L301)

**Recommendation.** Even if this project initially advertises only Core, KV, and Object Store, retain a private JetStream service boundary. KV and Object Store should consume that service rather than reaching through Core ad hoc. Model JetStream delivery as at least once; acknowledgements should be Effect finalizers or explicit operations, never an unconditional action performed when a message is merely pulled.

## KV

**Fact.** KV is a view over a JetStream stream. Its public surface exports a manager (`Kvm`), bucket implementation, read-only and mutable interfaces, codecs, options, status, entries, and watch types. [KV README, lines 12–18](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/README.md#L12-L18), [KV public module, lines 1–27](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/mod.ts#L1-L27)

**Fact.** Entries carry bucket, key, raw bytes, time, revision, operation (`PUT | DEL | PURGE`), raw key, and parse conveniences. `get`, history, watch, keys, and status form the read-only API. Mutation distinguishes create-if-absent, compare-and-set update, unconditional/conditional put, soft delete, purge, and bucket destruction. [KvEntry, lines 24–50](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/types.ts#L24-L50), [RoKV/KV, lines 342–451](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/types.ts#L342-L451)

**Fact.** Compare-and-set is not a client-side check: the implementation sends the expected last subject sequence header and returns the JetStream sequence. Watches use ephemeral push consumers with last-per-subject/all/new delivery policies, no acknowledgements, flow control, and idle heartbeats. [kv.ts, lines 590–657](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/kv.ts#L590-L657), [watch consumer config, lines 801–822](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/kv.ts#L801-L822)

**Fact.** KV supports subject-preserving key codecs, payload codecs, bounded per-key history, storage/replica/placement settings, mirrors/sources, compression, bucket/per-key TTL mechanics, direct reads, and resumable watch revisions. Direct reads explicitly trade speed for possible inconsistency. [KV codecs, lines 52–167](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/types.ts#L52-L167), [limits/options, lines 169–296](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/types.ts#L169-L296), [watch options, lines 298–340](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/src/types.ts#L298-L340)

**Recommendation.** Give revisions a branded type and make create/update/put distinct Effect operations. Represent tombstones in the value algebra instead of silently returning absence everywhere. A watch should expose snapshot-versus-update phase and resume revision, and its contract must state that TTL may remove values/markers without an additional watch notification. Make direct-read consistency an explicit option in the API type.

## Object Store

**Fact.** Object Store is also a JetStream materialized view. Its API includes info/list, streaming and blob get/put, metadata updates, delete, object/store links, watch, seal, status, and destroy. Object payloads use W3C `ReadableStream<Uint8Array>`; metadata includes chunk count, size, SHA-256 digest, revision, and deletion state. [metadata types, lines 51–96](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/types.ts#L51-L96), [result/API types, lines 203–344](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/types.ts#L203-L344), [public module, lines 1–27](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/mod.ts#L1-L27)

**Fact.** Puts clamp chunk size to the server `max_payload`, stream chunks to per-object subjects, compute SHA-256 over the full value, and then publish metadata; a failed put attempts to purge landed chunks. Gets stream chunks through a consumer, recompute the digest, and fail the stream if it does not match. The result has both a data stream and a separate completion/error promise. [chunk setup, lines 468–525](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L468-L525), [metadata/cleanup, lines 523–608](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L523-L608), [get/digest verification, lines 676–772](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L676-L772), [ObjectResult, lines 203–221](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/types.ts#L203-L221)

**Fact.** Deletion writes roll-up metadata marked deleted and then purges chunk subjects. Links reject links-of-links. Opening/creating checks server feature support and requires a SHA-256 backend. [objectstore.ts, lines 97–180](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L97-L180), [link/delete, lines 775–839](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/src/objectstore.ts#L775-L839)

**Recommendation.** Map object data to `Stream<Uint8Array, ObjectStoreError>` so checksum/consumer failure has one completion channel. Scope every reader and consumer. Keep `putBlob/getBlob` as bounded convenience APIs with explicit size limits; the main API should stream. Preserve previous-revision checks for metadata replacement and make “published chunks but metadata failed” a named failure/recovery case in tests and telemetry.

## Compatibility and evolution

**Fact.** NATS.js targets Node/Bun, Deno, and browser runtimes; npm carries Node-compatible bundles while JSR carries ESM for Deno/browser/Node. Identical `@nats-io/<module>` names are intended to keep imports stable across registries. [runtimes.md, lines 1–20](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/runtimes.md#L1-L20)

**Fact.** Runtime capability is checked from server `INFO` version. A central feature table records minimum server versions for KV, Object Store, direct reads, multiple filters, compression, priority groups, consumer reset, and other features. JetStream API requests can also carry a required API level so a server rejects unsupported config instead of silently dropping fields. [feature names, lines 40–60](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/semver.ts#L40-L60), [minimum versions, lines 98–145](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/src/semver.ts#L98-L145), [JetStream options, lines 80–94](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/src/types.ts#L80-L94), [request header, lines 127–153](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/src/jsbaseclient_api.ts#L127-L153)

**Fact.** JetStream errors retain both HTTP-like status and NATS API error code; the source defines typed not-found and feature-disabled variants. [jserrors.ts, lines 193–258](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/src/jserrors.ts#L193-L258)

**Recommendation.** Treat server version/API level as negotiated capabilities, not package-version guesses. Feature-dependent Effect constructors should fail early with typed `UnsupportedFeature` errors containing required and observed versions. The compatibility suite should test at least the minimum supported server, current stable, and current prerelease when available.

## Tests and verification style

**Fact.** Upstream has parser/buffer/unit tests plus integration suites for connection, reconnect, TLS/auth, JetStream consumers/streams/clusters, KV CRUD/CAS/history/watch/TTL/replicas/cross-account behavior, and Object Store chunks/digests/links/watch/revisions. Examples include split-frame and oversized parser cases, reconnection policy cases, consumer throttling, KV previous-sequence operations, and corrupt-object detection. [split-frame parser tests, lines 96–247](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/tests/parser_test.ts#L96-L247), [oversized parser test, lines 571–580](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/tests/parser_test.ts#L571-L580), [consumer throttling tests, lines 686–787](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/jetstream/tests/consume_test.ts#L686-L787), [KV CAS tests, lines 1838–1857](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/kv/tests/kv_test.ts#L1838-L1857), [Object Store digest test, lines 1322–1358](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/obj/tests/objectstore_test.ts#L1322-L1358)

**Fact.** `nst` starts real single servers, clusters, and leaf nodes, builds JetStream/WebSocket configs, exposes monitoring endpoints, and also supplies a fake TCP NATS server for protocol tests. CI installs `nats-server`, runs Deno module tests in parallel, builds/tests the Node workspace, uploads coverage, and separately performs format, lint, bundle-version, and dependency-version checks. [nst capabilities, lines 9–21](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/nst/README.md#L9-L21), [fixtures/monitoring, lines 69–91](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/nst/README.md#L69-L91), [Deno CI, lines 7–50](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/.github/workflows/deno_checks.yml#L7-L50), [Node CI, lines 7–53](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/.github/workflows/node_checks.yml#L7-L53), [consistency checks, lines 7–42](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/.github/workflows/consistency_checks.yml#L7-L42)

**Recommendation.** Adopt the same test pyramid:

1. Pure parser, subject, header, codec, schema, error-mapping, and state-machine tests.
2. Fake-transport tests for partial frames, write failure, cancellation, reconnect, buffering, and lifecycle races.
3. Real `nats-server` integration tests for Core, JetStream substrate, KV, and Object Store.
4. Multi-node/fault tests for reconnect, cluster gossip, JetStream quorum, leader changes, and watch recovery.
5. Compile-time API tests for Effect environment, error, and success channels.

Every resource test should prove finalizers ran: connection drained/closed, subscription stopped, consumer deleted or stopped as appropriate, temporary store removed, and no Fiber remained live.

## Build and release conventions

**Fact.** Source packages are strict Deno/TypeScript modules. JSR exports source `mod.ts`; npm packages preprocess TypeScript imports then compile with `tsc` using `nodenext`, declarations, and source maps. [core deno.json, lines 1–39](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/deno.json#L1-L39), [core package scripts, lines 20–28](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/package.json#L20-L28), [core tsconfig, lines 1–15](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/core/tsconfig.json#L1-L15)

**Fact.** Releases are triggered from GitHub releases, dry-run both registries, use OIDC/provenance, publish JSR, build all npm bundles, then publish packages individually with `next` for prereleases and `latest` otherwise. [release setup and dry run, lines 1–54](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/.github/workflows/npm.yml#L1-L54), [package publishing, lines 56–96](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/.github/workflows/npm.yml#L56-L96)

**Fact.** Upstream contribution policy asks for an issue before functional changes, regression/feature tests, minimal dependencies, DCO sign-off, and consideration of Orbit for utilities. [change/test/dependency policy, lines 22–62](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/CONTRIBUTING.md#L22-L62), [DCO sign-off, lines 64–76](https://github.com/nats-io/nats.js/blob/3a7b736b175d9a387130210c88f851d71e1b014f/CONTRIBUTING.md#L64-L76)

**Recommendation.** Our first release can target Node/TypeScript and pnpm, but public docs must not imply Deno/Bun/browser support until CI exercises those runtimes. Keep Effect and Schema as peer dependencies where feasible, publish provenance, generate API docs, and make package exports explicit. If “official NATS client” means NATS-project ownership or endorsement, treat that as governance work requiring an upstream proposal; technical compatibility alone does not confer official status.

## Reusable design patterns for the Effect-native draft

1. **Ports below, domain services above.** Runtime transport → Core protocol → JetStream substrate → KV/Object Store. This matches upstream dependency direction and keeps Node APIs out of portable domain code.
2. **One explicit public facade per package.** Export services, constructors, models, and tagged errors deliberately; keep interpreters/internal state private.
3. **Scoped resource ownership.** `NatsConnection`, subscriptions, consumer streams, watches, and object readers are acquired in `Scope` and finalize deterministically.
4. **Semantic operation names.** Distinguish enqueue, flush round-trip, JetStream PubAck, consumer ack, and application completion.
5. **Typed capability negotiation.** Record server version, API level, headers, max payload, JetStream availability, and feature gates on the connection service.
6. **Bytes first, schema second.** Core carries `Uint8Array`; typed codecs are explicit and failure-producing.
7. **Backpressure is configuration.** Every conversion from NATS delivery to Effect `Stream` declares capacity, overflow, pull batch, concurrency, and cancellation/ack behavior.
8. **Revision-aware views.** KV CAS and Object Store previous-revision operations retain server sequence numbers as branded domain values.
9. **Single failure channel.** Adapt callback errors, iterator termination, status errors, and Object Store’s secondary error promise into the Effect error channel while preserving original causes and metadata.
10. **Real-server acceptance.** A green unit suite is insufficient; each protocol/resource behavior needs tests against pinned `nats-server` versions, with cluster/fault cases for reconnect and JetStream-backed resources.

## Decisions still needed before API mapping

- Whether Core is an Effect adapter over `@nats-io/transport-node` or an independent wire implementation. The former is the recommended first milestone.
- Initial runtime promise: Node-only, Node+Bun, or all upstream runtimes.
- Package names and whether JetStream is public or an internal dependency for KV/Object Store.
- Buffer/overflow defaults, reconnect queue limits, and shutdown semantics.
- Error taxonomy and which upstream metadata remains public.
- Schema/codec policy for subjects, headers, payloads, KV values, and object metadata.
- Supported `nats-server` version floor and the feature-gate matrix.
- Governance path for any claim of official NATS status.
