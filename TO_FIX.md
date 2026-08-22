# To fix

Reviewed baseline: `origin/main` at `262b77a` plus the working-tree basis recorded by the prior heartbeat
Reviewed HEAD: local `f506b48`; `origin/main` remains three commits ahead at `262b77a`, and the local
working tree additionally changes `docs/architecture/README.md` and adds
`docs/architecture/adr-0001-reconnect-limits.md`, `docs/architecture/adr-0002-wrap-official-client.md`,
`docs/architecture/adr-0003-kv-naming-strictness.md`,
`docs/architecture/adr-0004-revision-representation.md`, `docs/architecture/kv-slice-scoping.md`,
`docs/research/jetstream-kv-bucket-naming.md`, `src/internal/kv/naming.ts`, and
`test/kv-naming.test.ts`.
`TO_FIX.md` itself was excluded from the reviewed diff.

## P0 — correctness, wire incompatibility, or data-loss blockers

- [ ] `origin/main:src/internal/NatsTransport.ts:43` and
      `origin/main:test/internal/natsTransport.test.ts:4,10,20,31,49` — the pushed transport slice
      does not pass its repository gates: production exports `layerTransport`, while every transport
      test imports the nonexistent `layerFromOfficial`.
  - Official behavior and source: the official 3.4.0 connection surface supplies `flush`, `drain`,
    and `close` (`nats.js@95e76e7:core/src/core.ts:377-480`); the repository adapter must compile
    before it can claim those semantics. GitHub Actions run `32545839657` already failed
    `pnpm format:check` on the missing space in `layerTransport=`, before typecheck or tests ran.
  - Why the proposal/implementation fails: `origin/main` is red on formatting, and once formatted
    the test import still has no matching export. None of the new lifecycle, retry, iterator, or
    overflow tests has passed CI at the reviewed merge SHA. This violates the repository completion
    workflow and means the pushed slice is not usable from its own agreed test seam.
  - Required correction: choose one internal constructor name, update implementation, tests and
    comments atomically, format the file, and retain the structural `Connection` rename only if its
    callers agree.
  - Proof/test gate: on the exact merged tree run `pnpm format:check`, `pnpm lint`, `pnpm check`, and
    `pnpm test`; all must pass in CI. Add a compile-time import test if the constructor remains a
    named internal seam.

- [ ] `docs/architecture/adr-0004-revision-representation.md:8-14,18-20` — converting the official
      client's `number` sequences with `BigInt(n)` is claimed to be lossless, but precision has
      already been irreversibly lost above `Number.MAX_SAFE_INTEGER`.
  - Official behavior and source: pinned NATS.js types KV revisions and CAS versions as `number`
    (`nats.js@3a7b736:kv/src/types.ts:24-31,351,396-424`) and PubAck sequence as `number`
    (`jetstream/src/types.ts:96-116`). JetStream replies are decoded by ordinary `JSON.parse`
    (`jetstream/src/jsbaseclient_api.ts:43-57`) before the wrapper sees them; KV then returns
    `pa.seq` and stringifies numeric expected revisions (`kv/src/kv.ts:624-657`). JavaScript rounds
    integers above 2^53 during that parse/conversion.
  - Why the proposal/implementation fails: `BigInt(n)` preserves the already-rounded number, not
    the server's original uint64. The reverse path cannot pass an arbitrary bigint through the
    official KV `version: number` API without the same loss. Revisions can therefore alias, target
    the wrong CAS precondition, fetch the wrong sequence, or report a corrupt revision while the ADR
    promises end-to-end uint64 fidelity. This is a data-integrity blocker and contradicts ADR-0002's
    public-wrapper constraint.
  - Required correction: either specify a branded safe-integer compatibility limit with typed
    failure before unsafe values are used, or introduce a proven lossless protocol seam that parses
    sequence JSON/ack tokens as decimal strings and emits CAS headers from bigint directly. Do not
    claim bigint fidelity through the official numeric KV surface.
  - Proof/test gate: exercise 2^53−1, 2^53, 2^53+1, and 2^64−1 through raw PubAck/API JSON, watch
    metadata, direct get, returned revisions, and expected-revision headers. Every value must either
    round-trip exactly or fail typed before it can drive a read or mutation.

- [ ] `origin/main:src/internal/NodeOfficialConnect.ts:65-70` and
      `origin/main:src/internal/NatsTransport.ts:120-147` — interruption can leak a connection that
      completes after the Effect consumer stops.
  - Official behavior and source: pinned Effect `tryPromise` aborts its signal on interruption, but
    the underlying operation stops only if it observes that signal
    (`effect@648f566d:packages/effect/src/Effect.ts:900-973`). Official NATS
    `connect(options)` exposes no `AbortSignal`; Node transport dials and resolves asynchronously
    (`nats.js@95e76e7:transport-node/src/node_transport.ts:58-118`). The pinned NodeSocket analogue
    registers its close/destroy finalizer before calling `Net.createConnection`
    (`effect@648f566d:packages/platform/node-shared/src/NodeSocket.ts:59-98`).
  - Why the proposal/implementation fails: `nodeOfficialConnect.connect` discards the supplied
    abort signal, and `layerConnecting` brackets the connection only after the Promise has resolved.
    If acquisition is interrupted while the Promise is pending, a later successful official
    connection has no owner or finalizer. The current interruption test covers backoff only, not a
    pending or late-successful live acquisition. This violates Scope ownership and finalizer-before-
    exposure, with an observable socket/fiber leak.
  - Required correction: make the adapter cancellation-aware. Coordinate interruption and Promise
    completion so any connection produced after cancellation is closed exactly once; register
    ownership before a successful connection can escape. Do not make interruption wait forever for
    an uncancellable dial.
  - Proof/test gate: use a controlled `Deferred`/Promise seam to interrupt a pending connect, then
    resolve it successfully and assert exactly one close, no exposed service, and no live fiber or
    socket. Repeat against the real Node adapter with bounded wall-clock guarding and an exact leak
    counter.

- [ ] `origin/main:src/internal/NatsTransport.ts:38-73,94-102,147` — the production finalizer is
      neither bounded nor observable despite claiming a bounded drain followed by forced close.
  - Official behavior and source: official `drain()` waits for all subscriptions, flushes, and then
    closes (`nats.js@95e76e7:core/src/protocol.ts:1126-1140`); it can therefore remain pending while
    user delivery is stuck. Repository style requires modeled drain deadlines and observable
    exhaustion/finalizer failure (`docs/STYLE.md:31-38,53-63`).
  - Why the proposal/implementation fails: `drainTimeout` is optional and `layerConnecting` calls
    `transportFromOfficial(connection)` without one, so its production release may wait forever.
    Both drain and close failures are erased with `Effect.ignore`; close has no deadline at all.
    The only timeout test exercises the explicit-option path through the currently missing export.
    This violates total, bounded release and can hang layer shutdown or silently leak the transport.
  - Required correction: obtain drain and forced-close policies from an accepted lifecycle ADR;
    make the production path use them, bound every potentially hanging phase, and log/measure exact
    drain/close failure and forced-close outcomes without turning finalization into a typed failure.
  - Proof/test gate: deterministically cover drain success/failure/hang, close success/failure/hang,
    parent interruption, concurrent explicit close and scope close, and exactly-once release. Assert
    terminal observations and zero live resources after every branch.

- [ ] `origin/main:src/internal/SubscriptionBridge.ts:4-17,27-70` and
      `origin/main:src/internal/Errors.ts:3-18` — overflow terminal ordering contradicts its own
      contract and hard-codes an unaccepted data-disposition policy.
  - Official behavior and source: pinned Effect `Queue.failCauseUnsafe` enters `Closing` while
    buffered messages remain and finalizes only after they drain
    (`effect@648f566d:packages/effect/src/Queue.ts:910-960`; graceful completion semantics at
    `Queue.ts:963-969`). Official NATS callbacks return `void | Promise<never>` and cannot be
    suspended for user work (`nats.js@3a7b736:core/src/core.ts:62-88`).
  - Why the proposal/implementation fails: the bridge says admitted values are dropped on overflow,
    but `failCauseUnsafe` delivers them before the `SlowConsumerError`; the triggering value is the
    one rejected. `Stream.runCollect` losing its result on failure does not prove that no buffered
    value was delivered. Capacity is also unvalidated, and no accepted subscription ADR authorizes
    fail-and-terminate, triggering-item disposition, or drain-before-error. This violates the first
    stream gate and can process messages after callers believe overflow discarded them.
  - Required correction: first accept the subscription capacity/unit/overflow/terminal-ordering
    contract. Implement that exact ordering, validate a positive finite integral message capacity,
    and expose explicit loss diagnostics including the triggering item and admitted count.
  - Proof/test gate: record side effects with `Stream.tap` and prove the exact sequence of admitted
    values and terminal error at capacity, capacity + 1, concurrent end/overflow, repeated offer,
    interruption, and release. Include zero, negative, fractional, `NaN`, and infinite capacities.

## P1 — missing outcome or Effect/stream contract violations

- [ ] `docs/architecture/adr-0001-reconnect-limits.md:20-40,65-87`,
      `docs/architecture/adr-0002-wrap-official-client.md:7-25`, and
      `origin/main:src/internal/NatsTransport.ts:94-131` — reconnect ownership and policy are
      contradictory across the accepted ADRs and the pushed implementation.
  - Official behavior and source: official NATS owns server rotation, delay, dial attempts and
    termination inside `ProtocolHandler.dodialLoop`
    (`nats.js@3a7b736:core/src/protocol.ts:650-739`) and restores subscriptions after reconnect
    (`protocol.ts:575-601`). Defaults are 10 attempts per server, 2000 ms plus additive jitter, and
    reconnect enabled (`core/src/options.ts:28-53,127-138`).
  - Why the proposal/implementation fails: ADR-0002 now resolves the single owner to the wrapped
    official client and explicitly prohibits an Effect retry layer on top. The pushed code still
    wraps initial `connect()` in exponential 200 ms retry with generic jitter and a 30-second
    schedule window while official reconnect remains enabled. It cannot enforce ADR-0001's
    per-server budget, 2000 ms additive-jitter policy, status sequence, or server rotation. The
    reconnect ADR also cites nonexistent pinned `core/src/core.ts:1167-1310` lines and says two
    _identical_ auth errors abort, while source checks two consecutive auth-family errors
    (`protocol.ts:814-840`). The old pushed proposed ADR remains separately numbered ADR-0001 until
    the merged tree removes or supersedes it.
  - Required correction: remove the Effect retry wrapper, map the accepted typed policy into the
    official client's reconnect options and status iterator, and model first-connect policy
    separately without stacking a second reconnect owner. Fix the bad citation/auth wording and
    ensure only the accepted numbered ADR set remains after synchronization.
  - Proof/test gate: with two servers and TLS/non-TLS paths, prove exact per-server attempt counts,
    additive jitter bounds, first-connect policy, consecutive auth behavior, terminal status,
    subscription restoration, in-flight request failure, Core message-loss exposure, interruption,
    and absence of a second retry loop.

- [ ] `origin/main:src/internal/NodeOfficialConnect.ts:13-57` — the connect outcome ledger defaults
      every unknown/TLS failure to transient and copies raw error messages into public-shaped error
      detail without redaction.
  - Official behavior and source: Node transport wraps only `ECONNREFUSED`; all other dial, DNS,
    INFO, and TLS failures are rethrown in their original shapes
    (`nats.js@95e76e7:transport-node/src/node_transport.ts:58-118,147-177`). Official errors also
    distinguish authorization, invalid arguments, protocol, timeout, closed/draining, permission,
    and connection failures (`nats.js@95e76e7:core/src/errors.ts:23-261`).
  - Why the proposal/implementation fails: `systemCodeOf` looks only at a nested `cause.code`, so a
    direct TLS certificate/configuration error, unknown callback defect, or unsupported official
    branch becomes retryable `unavailable`. Bounded retry then delays and misreports fatal failure;
    `messageOf` stores unsanitized adapter text, which can contain server URLs or credential-bearing
    details. This violates typed-boundary, truthful-retry, source-preservation, and secret-redaction
    rules.
  - Required correction: build an exhaustive classified cause decoder for the exact supported
    official version, preserve safe structured cause metadata, map TLS/config/auth/protocol branches
    explicitly, and surface unknown shapes as an unclassified terminal error until evidence grants
    retry. Redact credentials and server userinfo before error construction.
  - Proof/test gate: table-test every official connect rejection plus direct and nested Node codes,
    TLS certificate failures, DNS temporary/permanent outcomes, non-Error rejection, thrown mapper,
    and messages containing tokens/userinfo. Fatal and unclassified branches must make one attempt;
    logs and schemas must contain no secret.

- [ ] `origin/main:test/internal/kit.ts:60-102` — the fake connection does not model official close
      or status termination, so future lifecycle/status tests can pass against impossible behavior
      or hang.
  - Official behavior and source: official `closed()` resolves only when close completes
    (`nats.js@95e76e7:core/src/core.ts:383-396`), and protocol close stops status iterators and waits
    for their closure (`core/src/protocol.ts:1091-1119`).
  - Why the proposal/implementation fails: the fake's `closed()` resolves immediately even while
    open; `endStatus` flips a boolean but never wakes the currently blocked queue take; and
    `deliverStatus` silently discards overflow regardless of `offerUnsafe` result. The fake cannot
    faithfully prove remote close, terminal status, bounded status buffering, or leak freedom.
  - Required correction: drive close and status with end-capable bounded queues/Deferreds that share
    one state machine with `isClosed`, wake all pending waits, and report overflow according to the
    agreed status contract.
  - Proof/test gate: run the same lifecycle contract against fake and official adapter: open
    `closed()` remains pending, close resolves it once, status emits admitted items then terminates,
    blocked pulls wake, overflow is observable, and all waiter/queue counts return to baseline.

- [ ] `.agents/skills/effect4-stream-discipline/references/effect4-api-notes.md:107,114-117`, its
      Claude mirror, and `origin/main:src/internal/NatsTransport.ts:94-102` — `Schedule.upTo` is still
      called a hard total-duration bound and the sanctioned skill example still swallows exhaustion.
  - Official behavior and source: `Schedule.upTo` filters on schedule-step elapsed metadata
    (`effect@648f566d:packages/effect/src/Schedule.ts:262-271,1294-1320`); it is not an attempt
    timeout. `Effect.catch` recovers the typed error (`Effect.ts:2637-2673`). The new test itself
    expects a nominal 10-second schedule to grant an attempt at 15 seconds
    (`origin/main:test/internal/connectLifecycle.test.ts:73-92`).
  - Why the proposal/implementation fails: an initial or stuck attempt can exceed the stated bound,
    and the skill example ends successfully after logging instead of implementing its promised
    status/error surface. The pushed runtime preserves `ConnectExhaustedError`, but its comment
    strengthens the actual timing guarantee. The carried TestClock gate remains incomplete because
    CI never reached tests and no schedule-reset, blocked-attempt, or successful-finalization test
    exists.
  - Required correction: call `upTo` a retry-window bound, model hard attempt/operation deadlines
    separately, and keep exhaustion in an observable typed/status surface.
  - Proof/test gate: after the build is green, prove transient timing, fatal non-retry, exact
    exhaustion, reset after success, blocked-attempt behavior, interruption, and finalization under
    `TestClock`/`Deferred`.

- [ ] `.agents/skills/effect4-stream-discipline/SKILL.md:217-228`, its Claude mirror, and
      `origin/main:test/internal/iteratorBridgeEvidence.test.ts:14-103` — official iterator evidence
      is improved but the real-subscription release proof remains incomplete.
  - Official behavior and source: official `QueuedIteratorImpl` buffers in a growable array, awaits
    its signal, and only `stop()` resolves pending pulls
    (`nats.js@3a7b736:core/src/queued_iterator.ts:62-165`); Effect awaits `iterator.return()` during
    stream cleanup (`effect@648f566d:packages/effect/src/Channel.ts:1867-1883`).
  - Why the proposal/implementation fails: the new tests prove those two mechanisms separately via
    an internal iterator class and a synthetic iterable. They do not exercise an official
    subscription, exactly-once unsubscribe, finalizer order, callback removal, post-release
    delivery, or a live dependency. The carried proof gate therefore cannot be marked resolved.
  - Required correction: retain callback-to-bounded-Queue for official subscriptions and add the
    actual scoped adapter before claiming the lifecycle proof.
  - Proof/test gate: against official 3.4.0 and the native NATS server, interrupt an idle pending
    subscription pull and prove bounded completion, exactly one unsubscribe/return, no post-release
    callback or delivery, and no hidden growable upstream buffer.

- [ ] `.agents/skills/effect4-stream-discipline/SKILL.md:89-92` and
      `.agents/skills/tdd/SKILL.md:20-24` — applicable skills still disagree on approved internal
      seams even though the pushed slice now relies on them.
  - Official behavior and source: the NATS parser is internal
    (`nats.js@3a7b736:core/src/parser.ts:20-100,687-721`); repository style keeps parser/transport
    internal while requiring their tests (`docs/STYLE.md:7-11,75-78`).
  - Why the proposal/implementation fails: TDD permits task/ADR-approved internal module seams, but
    stream Phase 2 still says "public seam" and TDD still asks for fresh user confirmation even when
    the task/ADR already approved it. The pushed internal tests demonstrate why package-public-only
    is impossible, but do not remove the contradictory instruction or preserve a witnessed red step.
  - Required correction: use "agreed observable seam" everywhere and treat explicit task/accepted
    ADR approval as confirmation; ask only when the governing decision is absent.
  - Proof/test gate: preserve a red/green receipt for an internal split-frame or transport defect
    while `src/index.ts` stays empty and no private-function assertion is introduced.

- [ ] `docs/architecture/kv-slice-scoping.md:75-93` — the proposed watch contract invents a distinct
      “initial values done” event and presents it as a pinned official fact.
  - Official behavior and source: official watch selects LastPerSubject, All, or New delivery
    (`nats.js@3a7b736:kv/src/kv.ts:789-822`), computes `isUpdate` from `num_pending`, and emits only
    entries (`kv.ts:895-949`). An empty bucket sets update mode but emits no synthetic initialization
    value. Repository testing gates explicitly require that empty-bucket behavior.
  - Why the proposal/implementation fails: line 86 requires a separate observable event without an
    ADR authorizing wrapper synthesis and without defining its ordering relative to the last initial
    entry, heartbeat, reconnect, or empty bucket. An implementation could therefore claim upstream
    semantics that NATS never emitted and break watch-resume reasoning.
  - Required correction: preserve official entry-level `isUpdate`/revision facts in the substrate.
    Keep any synthetic phase-complete signal as an explicit open API decision with exact empty-bucket
    and reconnect ordering, not as a pinned fact.
  - Proof/test gate: against JetStream-enabled NATS 2.14.5, cover empty, one-key, multi-key,
    all-history, updates-only and resume watches; record the exact entries/phase flags and prove no
    synthetic event unless an accepted ADR deliberately adds one.

- [ ] `docs/architecture/adr-0002-wrap-official-client.md:7-9,33-36` — the accepted wrapper decision
      names Core, JetStream, and KV but omits Object Store from both its binding scope and claimed
      discharged workbench decisions.
  - Official behavior and source: Object Store is a separate official public package,
    `@nats-io/obj`, depending on Core and JetStream (`nats.js@3a7b736:obj/package.json:1-39`) with a
    deliberate public module (`obj/src/mod.ts:1-27`). The repository and user scope require Core,
    KV, and Object Store.
  - Why the proposal/implementation fails: “wrap the official client first” does not authorize
    whether Object Store wraps `@nats-io/obj`, rebuilds over the internal JetStream substrate, or is
    deferred. Yet the ADR claims the adapter decision is discharged. Object work would therefore
    begin without the accepted adapter/lifecycle authority required by the design gate.
  - Required correction: amend ADR-0002 to include `@nats-io/obj` with its streaming/digest cleanup
    guardrails, or explicitly defer Object Store and leave the workbench item open for that slice.
  - Proof/test gate: before any Object Store implementation, trace every public operation to the
    selected official package/substrate and approve the chunk, metadata-commit, cleanup,
    interruption, digest, and replacement semantics in an Object Store ADR.

- [ ] `test/kv-naming.test.ts:24-160` and `src/internal/kv/naming.ts:14-106` — the approved internal
      seam has a broad green test batch but no preserved one-test-red/minimal-green TDD receipt.
  - Official behavior and source: the narrow seam is authorized by ADR-0003 lines 34-38; its
    expected behavior maps to pinned JS validators (`nats.js@3a7b736:kv/src/kv.ts:156-174`) and
    ADR-8 mappings (`nats-architecture-and-design@e23afc3:adr/ADR-8.md:234-240,307-318`).
  - Why the proposal/implementation fails: eleven naming/mapping tests and the complete module
    appeared in one uncommitted slice. No red output, intermediate test-only state, or mutation
    witness demonstrates that one behavior test failed at the confirmed seam before the minimal
    implementation. This violates the repository-local TDD and heartbeat gates against speculative
    horizontal test batches.
  - Required correction: retain the agreed module seam, but preserve a red/green receipt for one
    canonical behavior at a time and identify which exact test kills each semantic mutation.
  - Proof/test gate: show a witnessed red for one grammar or mapping behavior against the pre-fix
    seam, the minimal green change, then mutation-check `..`, `KV_`, glued wildcard, prefix mismatch,
    and invalid wire-key branches without expanding the package-public surface.

## P2 — maintainability or fidelity gaps

- [ ] `docs/research/jetstream-kv-bucket-naming.md:5-11,45-57,64-82,127-139,191-203` — the new
      cross-client rulings are not reproducible because two claimed reference implementations are
      neither pinned nor cited to exact source.
  - Official behavior and source: the pinned JS validators accept `a..b` for stored keys but reject
    it for search keys (`nats.js@3a7b736:kv/src/kv.ts:156-174`); pinned Go rejects empty tokens for
    both stored and search keys (`nats.go@db1375f:kv.go:389-393,595-613`). Current Python and Rust
    sources exist at `nats.py@f934c5b` and `nats.rs@97ebb23`, but neither commit, path, nor line range
    appears in the document's pin list or individual claims.
  - Why the proposal/implementation fails: the document repeatedly says “all four reference
    clients” and “every reference implementation”, then uses Python/Rust behavior to rule filter,
    mirror, and prefix semantics. An agent cannot tell which source versions were reviewed or
    reproduce those rulings after either default branch moves. This violates the repository's exact
    source and conformance-evidence gate.
  - Required correction: pin every named client, cite an opened path and exact line range for each
    cross-client claim, and separate source facts from live observations. Remove “every” where the
    sweep did not inspect every official implementation.
  - Proof/test gate: re-open each pinned validator/mapping implementation, record its exact accepted
    and rejected corpus, and run one shared adversarial table against every implementation before a
    naming ADR consumes the synthesized grammar.

- [ ] `docs/research/jetstream-kv-bucket-naming.md:136-139` — Object Store names are described as
      arbitrary bytes, which is not the pinned official client's API or metadata contract.
  - Official behavior and source: pinned NATS.js defines `ObjectStoreMeta.name` and link names as
    `string` (`nats.js@3a7b736:obj/src/types.ts:35-57`), and `_metaSubject` base64url-encodes that
    string (`obj/src/objectstore.ts:968-977`). ADR-6 specifies `os-object-name = (any-character)+`,
    not a byte sequence (`nats-architecture-and-design@e23afc3:adr/ADR-6.md:63-66`).
  - Why the proposal/implementation fails: an “arbitrary bytes” model permits names that cannot be
    represented in official metadata JSON or passed through the official string API, and obscures
    the actual cross-client question: how Unicode strings are converted to bytes before subject
    encoding. That can produce incompatible metadata subjects and link lookups.
  - Required correction: state the public name type as a non-empty string and document the exact
    UTF-8/base64url conversion and cross-client Unicode limits separately; use `Uint8Array` only for
    object payloads or an explicitly evidenced wire helper.
  - Proof/test gate: compare pinned JS, Go, Python, and Rust metadata subjects and round trips for
    ASCII, Latin-1, non-Latin Unicode, combining characters, lone surrogates, and empty names; any
    divergence remains an ADR decision rather than a canonical fact.

- [ ] `docs/research/jetstream-kv-bucket-naming.md:73-77,195-203`,
      `src/internal/kv/naming.ts:47-52`, and `test/kv-naming.test.ts:71,90-96` — glued wildcard tokens
      are repeatedly said to “match nothing”, but the server treats them as literal tokens that can
      match reachable non-conformant keys.
  - Official behavior and source: server subject validation accepts any multi-character token
    without interpreting embedded `*` or `>` (`nats-server@d3bc045:server/sublist.go:1227-1245`),
    and wildcard detection treats those characters as wildcards only when they occupy a whole token
    (`sublist.go:1171-1196`). Pinned JS search validation accepts `foo>` and `a*b`
    (`nats.js@3a7b736:kv/src/kv.ts:156-174`).
  - Why the proposal/implementation fails: a filter such as `foo>` matches no _conformant_ KV key,
    but it can exactly match a foreign/directly published literal key named `foo>`, which the same
    research document identifies as reachable. The absolute wording hides the actual strictness
    tradeoff and misstates what broad watches can encounter before `keyFromSubject` fails typed.
  - Required correction: keep ADR-0003's deliberate rejection, but say these filters silently miss
    conformant wildcard intent while potentially matching non-conformant literal keys. Carry that
    reachable branch into the watch outcome ledger.
  - Proof/test gate: on JetStream-enabled NATS 2.14.5, directly publish a literal `foo>` key, prove an
    exact literal subscription can receive it, prove the strict filter rejects it, and prove a broad
    watch terminates with the agreed typed invalid-key outcome.

- [ ] `docs/architecture/kv-slice-scoping.md:119-129` and
      `docs/architecture/adr-0004-revision-representation.md:1-14` — revision representation is
      simultaneously listed as open and accepted/discharged.
  - Official behavior and source: pinned NATS.js exposes KV revision/CAS values as `number`
    (`nats.js@3a7b736:kv/src/types.ts:24-31,351,396-424`); ADR-0004 attempts to rule the repository
    representation as bigint while the workbench still says no representation is assumed.
  - Why the proposal/implementation fails: agents reading the scoping document can legitimately
    stop for the open decision or implement the accepted ADR, producing inconsistent types and test
    fixtures. This violates the architecture index's rule that accepted ADRs constrain subsequent
    implementation.
  - Required correction: after fixing ADR-0004's precision error, replace open decision 4 with a
    resolved link and restate the exact safe/lossless adapter boundary in the scoping ledger.
  - Proof/test gate: an architecture consistency check must find no accepted ADR still listed as an
    open decision, and the substrate signature snapshot must use the corrected representation at
    every sequence/revision field.

## Watch next

- [ ] `origin/main:package.json:14-15` and
      `origin/main:test/internal/iteratorBridgeEvidence.test.ts:10` — runtime dependencies use
      caret ranges while evidence imports unsupported internal NATS APIs. A lock update that resolves
      above 3.4.0 without a new source/behavior receipt promotes this to a reproducibility defect.
- [ ] `.agents/skills/effect4-stream-discipline/` and `.claude/skills/effect4-stream-discipline/`
      remain manually mirrored; any behavioral difference promotes this to an agent-consistency
      defect.
- [ ] `docs/architecture/kv-slice-scoping.md:51-73` is only an outcome-family skeleton. Any KV code
      added before every official status/API/transport/timeout-after-possible-commit/tombstone branch
      is filled in and accepted promotes the missing branch to a confirmed finding.

## Outcome coverage

- Wrapped connect/acquire: inspected official success, refusal, unavailable, timeout, auth,
  protocol, config, unknown/direct Node error, interruption during backoff, retry exhaustion and
  successful release. Missing tests/behavior: pending-connect interruption, late successful result,
  TLS/DNS classification matrix, secret redaction, server rotation, official inner reconnect, and
  readiness/status terminal branches.
- Connection finalization: inspected flush closed/success, drain success/hang, forced close and
  idempotent guard. Missing: drain failure, close failure/hang, production default deadline,
  concurrent close/release, remote close, parent interruption, and exact finalizer observations.
- Subscription bridge: inspected ordered success, overflow, repeated offer, end, buffered drain,
  blocked-consumer interruption and stream failure. Missing: accepted disposition contract,
  capacity validation, admitted-item terminal ordering with side effects, concurrent end/overflow,
  callback unregister/UNSUB, post-release delivery, exact queue termination and real producer path.
- Iterator evidence: inspected NATS push/pending/stop and Effect return/finalization branches;
  official subscription release and live-server branches remain uninspected.
- Reconnect ADR: mapped official defaults, server rotation, wait-on-first-connect, auth abort and
  exhaustion. Restoration, Core loss, in-flight requests, status sequence and single-owner proof are
  not implemented or tested.
- KV draft: inspected watch delivery modes, `isUpdate`, resume and empty bucket. JetStream API
  errors, CAS uncertainty, DEL/PURGE, TTL, reconnect and every live watch branch remain untested.
- KV naming research: inspected ADR-6 bucket/key/Object-name grammar, ADR-8 mappings and watch-filter
  language, pinned JS/Go validators, server subject/name validation and the 255-byte stream-name
  limit. Missing: pinned Python/Rust citations, practical control-line ceiling, Windows case
  collision, non-conformant-key watch behavior, and all declared live probes.
- KV naming implementation: modeled and tested valid/invalid bucket, key and filter parsing;
  252-character ASCII bucket limit; `KV_` rejection; key-to-subject and bucket-to-stream mapping;
  typed prefix/invalid-key reverse failures; and property-based key round trips. Missing:
  encode-boundary checks, bucket round-trip property, raw-subject redaction decision, server-observed
  mapping/filter cases, control-line failure, foreign non-conformant keys, and preserved red/green
  evidence. Transport, cancellation, reconnect and terminal-stream branches do not apply to these
  pure functions; they remain open for the future KV watch adapter.
- Explicitly not inspected: parser/wire frame branches, publish/request, JetStream PubAck, Object
  Store operations beyond name representation, TLS server integration, cluster/fault topology, and
  any public API; this slice adds none of those implementations.

## Verification

- No new commit appeared: local HEAD remains `f506b48` and `origin/main` remains `262b77a`. New
  non-`TO_FIX.md` changes are accepted ADR-0002/0003, `src/internal/kv/naming.ts`,
  `test/kv-naming.test.ts`, and the architecture index update.
- `pnpm format:check` — failed with exit 20 on `src/internal/kv/naming.ts:65-75` and
  `test/kv-naming.test.ts:52-53`.
- `pnpm lint` and `pnpm check` — passed independently.
- `pnpm test` — passed independently: 2 files, 12 tests. These results cover the local naming slice,
  not the three pushed transport commits.
- `git status --short --branch` — local `main` is behind `origin/main` by three commits; reviewed
  pushed SHAs are `6b17149`, `4f48db2`, and merge `262b77a` plus the listed local architecture files.
- GitHub Actions run `32545839657` for `262b77a` — failed `pnpm format:check` at
  `src/internal/NatsTransport.ts:43`; later lint/check/test steps did not run.
- `git diff --check` on the local working tree — passed.
- Read/invoked: `AGENTS.md`, current `TO_FIX.md`, all three repository-local skills and their
  applicable testing/API/case-study references, `docs/STYLE.md`, architecture workbench and new
  ADRs/draft, the KV naming research and implementation/tests, official Effect client patterns, and
  NATS.js reference map.
- Exact official sources inspected: Effect `648f566d` `Effect.ts:900-973,2637-2673`,
  `Schedule.ts:262-271,1294-1320`, `Queue.ts:910-969`, `Channel.ts:1867-1883`, and
  `NodeSocket.ts:59-98`; NATS.js pinned `3a7b736` `options.ts:28-53,127-138`,
  `protocol.ts:575-601,650-739,814-840`, `queued_iterator.ts:62-165`,
  `kv/src/kv.ts:156-174,789-822,895-949`, `obj/src/types.ts:35-57`, and
  `obj/src/objectstore.ts:968-977`; NATS.go `db1375f` `kv.go:389-393,595-613`; NATS architecture
  `e23afc3` `ADR-6.md:26-66` and `ADR-8.md:110-160,238-239,311,415-420`; NATS Server `d3bc045`
  `sublist.go:1199-1267`, `jetstream.go:2780-2792`, and `jetstream_api.go:354-363`;
  installed-version source tag `v3.4.0` commit `95e76e7`
  `core.ts:377-480`, `errors.ts:23-261`, `protocol.ts:1091-1140`, and Node transport
  `node_transport.ts:58-177`.
- Native `nats-server` v2.14.5 is currently listening at `127.0.0.1:24999`, but its command line has
  no JetStream flag. No Core or JetStream integration test was run in this review.
- Untested/unavailable: pushed dependencies are not installed in the local checkout; `origin/main`
  could not be executed without mutating/synchronizing the working tree; no accepted subscription,
  drain, restoration, KV naming, or KV watch ADR exists.
