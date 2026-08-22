# Engineering check-in — 2026-08-21

Status: **review only; do not hand work over**. Liveness was observed continuously from
15:03–15:08 America/Chicago: the active `opencode` process accumulated CPU and created or
rewrote `src/agents/wire.ts`, `src/agents/prompt-engine.ts`, and the protocol modules during
the audit. No implementation files were changed by this review.

Baseline: uncommitted work above `HEAD` `3bbf8f70`. Sources cross-checked:

- local normative `docs/01-SPEC.md` and P0 boundary in `docs/00-PLAN.md`;
- upstream Agent Protocol v0.3.0 `core-protocol.md`, especially §§2, 5, 6, 8, 9, 12;
- installed `effect@4.0.0-rc.111` source in `node_modules/effect/src`;
- Foldable's pinned Effect Stream/Channel brief and NATS subject corpus.

At the first complete snapshot, `bun test` passed 30/30 in about 180 ms. That is a useful
regression signal, not conformance evidence. `bun run typecheck` failed while the prompt
engine was being written; re-run both after the current edit settles.

Moving snapshot at 15:10: the new Schema/client files were still mid-edit. `bun test` then
failed during module load because `Schema.Union` was called variadically, and typecheck
reported the other rc.111 API-shape mismatches listed below (`Result` Success/Failure,
`Schema.Record(key, value)`, `Stream.unwrap`, timeout fallback thunk). Treat this as live
feedback, not a stable verdict on unfinished work.

## Immediate correctness blockers

1. **Request relay is not lossless.** `parseRequest` trims the original plain-text payload,
   although upstream §5.3 promotes the original payload, and it drops unknown fields inside
   attachment objects although upstream §5.6 requires preserving them. JSON and plain-text
   empty-prompt behavior also needs one explicit mapping to the local `RequestError` contract.
   The local spec currently names exactly three parser errors while upstream requires invalid
   attachments and empty prompts to become 400s; resolve that spec/API mismatch deliberately
   rather than growing an ad-hoc union.

2. **Subject construction is too permissive.** `agentSubject` constructs learned endpoints
   from unchecked identity strings, despite the caller rule that only the fixed heartbeat
   subject may be constructed. `isToken` also accepts whitespace, which NATS forbids. Split
   fixed heartbeat construction from validated host-default construction; callers should
   carry an opaque discovered endpoint subject. The duplicated recursive matcher in
   `core/subjects.ts` and `agents/protocol/subjects.ts` repeatedly slices arrays, making it
   allocation-heavy/O(n²). One shared index-based matcher is both smaller and linear.

3. **The stream model does not encode the protocol state machine.** `wellFormed` can accept
   an empty successful stream, repeated ack, an error without ack, and cannot represent a
   headerless terminator or reject post-terminator messages. Model frames explicitly:
   `Chunk | ServiceError | Terminator`, then make the single-writer host pump construct only
   `Ack · Chunk* · (Error)? · Terminator`. Clarify the acceptance boundary for malformed
   requests: upstream §6.4 says ack confirms acceptance, while the local spec currently reads
   as if every error path is ack-first.

4. **The wire seam cannot recognize termination exactly.** `ReplyMessage` exposes only the
   two service-error headers. A zero-byte message with some other NATS header is not a
   terminator under §6.5, but the current classifier cannot distinguish it from a headerless
   message. Preserve header presence (and preferably the raw service-error strings) at the
   seam and inspect headers before body classification.

5. **Errors are values in several paths.** `Option/Either<PromptError, ...>`-style
   classification makes stream failures easy to drop or accidentally emit. Decode one reply
   with `Effect<Frame, PromptError>` and let `Stream.mapEffect` preserve the E channel. Unknown
   chunk types are a `Skip` frame; a terminator is `End`; neither is an error.

6. **Closed-world registry typing is unsound for dynamic arrays.** In `core/registry.ts`,
   `AllWf<readonly string[]>` can select the success type even when runtime registration
   returns `RegistryError`, which is then cast to `RegisteredSubject`. Dynamic input must
   return an honest runtime result; reserve the refined success type for validated literals.

7. **The native implementation boundary was crossed.** `nats-connection.ts` imports
   `withAgentReconnectDefaults` from missing `@synadia-ai/agents`. The plan explicitly says
   the Synadia SDK is reference material, not a dependency. Re-express the small reconnect
   policy locally from `@nats-io` options and keep its defaults covered by a unit test.

8. **The client claims TestClock behavior but reads `Date.now()`.** Discovery and liveness
   must use Effect's `Clock`/`TestClock`, not ambient wall time. The manual `Stream.toPull`
   loop also repeats the same mistaken Option/Either completion model. Prefer a native Stream
   quiet-period pipeline and an overall Effect deadline; if low-level pulling is unavoidable,
   use the pinned `Pull` completion protocol deliberately.

9. **Layer and Stream requirements are being erased in signatures.** The live connection
   layer can fail with `ConnectionLost`, the wire layer requires `NatsConnection`, and the
   client-created prompt stream still requires `Wire`. Let Layer types infer those channels,
   and capture/provide the acquired `Wire` service to streams returned by `AgentClient` so
   its public method truthfully has `R = never`.

## Effect-native route for the pinned version

The installed source is authoritative. This release exports `Schema.TaggedError`, **not**
the later `Schema.TaggedErrorClass` name. It also has:

- `Schema.StructWithRest(Struct, [Schema.Record(Schema.String, Schema.Unknown)])` for
  lossless top-level and nested attachment relay;
- `Schema.fromJsonString`, `Schema.decodeUnknownResult`, `Schema.TaggedUnion`, and
  `Schema.toTaggedUnion` for total boundary decoding;
- `Stream.unwrap` (which eliminates a `Scope.Scope` acquisition requirement), not
  `Stream.unwrapScoped`;
- `Stream.timeoutOrElse({ duration, orElse: () => Stream.fail(error) })`, whose timeout is
  per pull and therefore already implements the required idle-gap reset;
- `Stream.mapEffect`, `takeWhile`, and `filterMap`, sufficient for
  `Emit | Skip | End` without manually driving `Stream.toPull`;
- `Result.succeed` / `Result.fail` (not `left` / `right`) and `Schema.Union([members])` /
  `Schema.Record(key, value)` in this exact release.

Prefer `Schema.TaggedError` classes for `StreamTimeout`, transport, protocol, and status-code
failures. Prefer a `Data.TaggedEnum` for the internal `Emit | Skip | End` decision. Keep the
`Wire` service and scoped `Stream` seam; those are good boundaries. Implement the real service
methods with `Effect.fn` and let `Stream.unwrap` own acquisition/finalization. Avoid `as any`,
unchecked casts, hand-written pull loops, and duplicated JSON decoders.

## Required invariants

| Area | Invariant |
|---|---|
| Request | Parsing is total over every `Uint8Array`; plain text is not trimmed; prompt is non-empty; unknown envelope and attachment fields survive decode→encode. |
| Subject | Concrete tokens contain no dot, wildcard, whitespace, or `$` prefix; exact patterns are singletons; `*` is exactly one; terminal `>` is one-or-more. |
| Discovery | Caller publishes only to the endpoint subject obtained from `$SRV.INFO`; heartbeat is the sole fixed construction path. |
| Host stream | Exactly one ack is first after acceptance; chunks retain publication order; optional error is immediately before exactly one headerless terminator; no later writes. |
| Caller stream | Every message checks headers first; unknown types/status values are skipped; each observed frame resets the gap timer; interruption closes the subscription exactly once. |
| Backpressure | Broker ingestion never waits on user work; every wire↔user queue is bounded with an explicit suspend/drop/slide policy. |

## Conformance test gate

Example tests should remain, then add these as release gates:

1. `effect/testing/TestSchema.Asserts` for every wire codec: generation plus
   lossless encode/decode. Use `Schema.StructWithRest` fixtures to prove unknown nested fields
   survive relaying.
2. `effect/testing/FastCheck` properties for parser totality over arbitrary bytes, request
   discrimination, exact subject singleton, wildcard arity, and P0 refinement laws. Property
   tests should use reproducible seeds in failure output.
3. A small model-based DFA test for arbitrary frame sequences. The checker and host pump must
   agree on ack-first, one terminator, error position, and rejection of post-terminator frames.
4. Scripted `Wire` tests under `TestClock`: timeout immediately before/after the boundary,
   reset after every frame (including unknown chunks), error-header-before-empty-body,
   zero-byte-with-headers not terminating, interruption/finalizer exactly once, and no real
   sleeps.
5. Byte-for-byte fixtures from upstream Appendix B plus pinned fixtures captured from the
   official TypeScript SDK. Run both caller and host directions; round-trip tests alone cannot
   detect two implementations sharing the same mistake.
6. P1 integration tests against a local `nats-server`: subscribe-before-publish, no responders,
   lost terminator timeout, queue group behavior, slow-reader bounds, and disconnect/reconnect.

## Standards axis

Hard findings: incorrect pinned Effect APIs/manual pull protocol while the prompt engine was
in progress; hand-rolled or non-passthrough boundaries where the plan requires Schema
end-to-end; an under-specified stream checker; unrestricted endpoint construction; the
registry cast described above; and missing `Effect.fn` on non-trivial public Effect operations.

Judgement calls: duplicate subject algebra is duplicated code; the KV/Cell rewrite is a
divergent change relative to the active P0 slice; the current tests are deterministic but
example-only. Keep the protocol kernel pure and small, and defer unrelated refactoring unless
it directly supports the slice.

## Spec axis

Hard findings: plain-text mutation and nested unknown-field loss; unsafe endpoint subject
construction; no complete terminator/ack representation; and no property/model/differential
conformance suite for the P0 laws. P1 inactivity/error decoding and P2 host/liveness work are
future slices and are not counted as P0 omissions.

Summary: standards axis has six hard findings plus three design smells; spec axis has four
P0 blockers. The worst standards issue is using APIs/protocols not present in the pinned Effect
source; the worst spec issue is that the current types cannot represent and therefore cannot
enforce the wire termination state machine.

## Monitor update — 15:18 America/Chicago

Worker remains live: OpenCode CPU advanced by 2 seconds during a 12-second sample and
`src/agents/wire.ts` was rewritten at 15:17. No implementation takeover.

New actionable findings from the client/liveness slice:

- `AgentClient.prompt` bypasses the request envelope codec it claims performs validation.
  It currently accepts an empty prompt and invalid base64, and treats missing required
  `max_payload` / `attachments_ok` endpoint metadata as permissive defaults. Build and encode
  through the same Schema contract used by the host; reject or exclude non-compliant discovery
  records before any publish.
- `discover({ deadlineMs })` ignores `deadlineMs`. The quiet-period timeout is useful but is
  not an overall deadline, so a steady reply stream can keep discovery open forever. Compose
  the per-pull quiet timeout with a separate Effect/Stream deadline, both driven by Clock.
- One cold `heartbeatStream` wraps one NATS subscription, is consumed by the tracker fiber,
  and is also returned to arbitrary callers. Those consumers can compete for a single
  async iterator. Terminate the wire subscription into one layer-owned bounded PubSub (or
  SubscriptionRef for state plus PubSub for events) and expose independent subscriber streams.
- The comment says the heartbeat tracker is supervised/rebound, but `catchAll(...).forkScoped`
  merely swallows one failure and exits; normal stream completion also exits. Reacquire the
  subscription inside a named, paced retry/repeat policy and keep interruption observable.
- `toIdentity` converts missing required `metadata.agent` / `metadata.owner` into empty strings
  and does not enforce protocol-version compatibility. Decode service info into a compliant
  record with required metadata, preserve unknown metadata for relay, and filter/reject by the
  §11 compatibility rules instead of manufacturing an invalid identity.
- `NatsConnection.status` catches `ConnectionLost` to `Stream.empty`, contradicting its own
  promise that terminal close is observable and recreating the silent-death failure the design
  rejects. Preserve the typed failure or emit an explicit terminal status for supervision.

Build snapshot remains red while edits continue: protocol tests abort at module load on the
already-noted variadic `Schema.Union`, and typecheck still reports the pinned API mismatches.

## Monitor update — 15:45 America/Chicago

Worker liveness is concrete: OpenCode is responsive with advancing CPU, and it added/updated
`properties.test.ts`, `prompt-engine.test.ts`, and `host.test.ts` through 15:44. No
implementation takeover.

The upstream v0.3.0 protocol is now vendored byte-for-byte at commit
`d907f5ca080528ee5e9e1385f0591c42f7fdb9b8` with a SHA-256 integrity test. The generated
traceability ledger finds 169 normative/table/procedure/exactness obligations. Its extraction
and integrity checks pass; the strict gate deliberately remains red because none of those IDs
yet names executable evidence. As the new tests stabilize, map each one to the complete IDs it
proves; do not bulk-mark checklist duplicates or count `test.todo` as evidence.

New source-level ambiguity to resolve explicitly: §2 and §3 say the instance name is the 5th
token of `agents.{verb}.{agent}.{owner}.{name}`, while §4.3 step 3 says “4th token.” The grammar,
examples, and metadata discussion make 5th-token behavior the coherent interpretation. Lock
that into conformance tests and record the upstream editorial defect beside `AP03-L0228` rather
than implementing both interpretations. Several other paragraphs still say “v0.1” inside the
v0.3 document; those labels must not downgrade `metadata.protocol_version = "0.3"` or restore
the superseded subject shape.

Gate snapshot after the latest worker edit: `bun run typecheck` is now green. `bun test` has
73 passing, 2 failing, and 169 explicit conformance TODOs. One failure is the intentional
unmapped-requirements gate; the other is a real virtual-time boundary failure in
`prompt-engine.test.ts` (“just under the window” returns two chunks instead of three). Do not
paper over that boundary with a larger sleep—make the gap-timer state transition deterministic
under `TestClock` and test exact boundary/reset semantics.

## Monitor update — 15:52 America/Chicago

Worker remains live: OpenCode stayed responsive and accumulated CPU during a 12-second sample;
`client.ts`, `prompt-engine.ts`, `host.ts`, their tests, and `docs/01-SPEC.md` changed within the
last five minutes. No implementation takeover. Concrete progress: typecheck is green; the
timeout boundary regression is fixed; the heartbeat feed now uses one bounded sliding PubSub;
discovery has an overall Clock-driven deadline and filters invalid/incompatible metadata. The
suite is 74 passing with only the intentional 169-requirement traceability gate failing.

New actionable findings in the P1/P2 slice:

- The newly documented/tested `rejected ::= error · terminator` DFA weakens upstream §6.4,
  which says every response stream emits exactly one `ack` as its first message and describes
  it as receipt, not post-validation acceptance. `docs/01-SPEC.md`, `chunks.ts`, `host.ts`, and
  the malformed/overflow tests currently encode the no-ack interpretation. Do not override the
  higher-authority protocol locally: make rejection streams `ack · error · terminator`, or pin
  an upstream clarification before recording a deviation (`AP03-L0378`, `AP03-L0660`).
- `AgentClient.discover` requests `$SRV.INFO.agents` directly. Protocol §4 defines general
  enumeration through `$SRV.PING.agents`, followed by per-instance
  `$SRV.INFO.agents.{instance_id}`; §12 explicitly restricts discovery to that flow. Add a
  differential/integration test with multiple real services and make the two-phase sequence
  observable (`AP03-L0194`, `AP03-L0198`, `AP03-L0199`, `AP03-L0669`).
- The host forks one scoped fiber for every inbound prompt before admission is observed. The
  semaphore bounds handler permits but not queued/runnable fibers, so a burst can allocate
  without a configured bound. Use a bounded non-blocking admission boundary/FiberSet and prove
  burst memory/concurrency plus immediate `429`; broker ingestion must remain non-blocking.
- `heartbeatLoop` is started before status and `$SRV.PING`/`$SRV.INFO` responders, so its first
  immediate beat can precede discoverability, contrary to §8.3's registration-before-heartbeat
  recommendation. Start all service responders first, synchronize readiness, then start the
  shared-builder heartbeat loop; cover the first-event ordering deterministically.
- `AgentFailure.code` accepts any number although §9.2 requires the fixed taxonomy, and handler
  defects bypass the typed `AgentFailure` branch: the finalizer attempts a terminator but no
  `500` error frame. Refine codes at construction and sandbox handler causes so non-interrupt
  defects produce `ack · 500 · terminator` while interruption remains interruption.

Also remove the new non-null assertions in `client.ts`, `host.ts`, and `host.test.ts`; they are
forbidden by the repository contract and hide branches that should be made explicit.

## Monitor update — 16:00 America/Chicago

Worker liveness remains concrete: source changed through 15:56, a 25 KB conformance-evidence
suite appeared at 15:59, and OpenCode CPU advanced during sampling. No implementation takeover.
Typecheck remains green; tests advanced to 110 passing, with only the deliberate 169-ID
traceability gate failing. New useful coverage includes host-side payload/attachment rejection,
numeric overflow, session propagation, malformed-heartbeat survival, and status/error shapes.

Do not count the new test volume as conformance yet:

- `conformance-evidence.test.ts` now explicitly asserts “discovery uses `$SRV.INFO.agents`
  only,” and the robustness tests explicitly assert rejected requests have no ack. Those are
  the two protocol conflicts identified above, now encoded as passing expectations. Invert them
  before mapping any related IDs; a test that freezes the wrong wire contract is negative
  evidence, not coverage.
- `HostIdentity.agent` is used both as the subject token and `metadata.agent`. The protocol
  permits an abbreviated third subject token (`cc`) but requires canonical metadata
  (`claude-code`); current service-info tests incorrectly require `metadata.agent === "cc"`.
  Split canonical identity from subject abbreviation, require session metadata for known
  session-aware harnesses, and test discovery across the two representations.
- `Heartbeat` still decodes `ts` as any string and `interval_s` as any number; the client then
  filters non-positive intervals after decoding. Put UTC ISO-8601 and finite-positive cadence
  constraints in the shared Schema so periodic heartbeat and status reply enforce the exact
  boundary identically. Add `TestSchema`/FastCheck rejection and round-trip properties.
- All new service, queue-group, discovery, and status evidence runs against the loopback bus.
  It proves the fake agrees with the implementation, not that `nats-wire.ts` interoperates with
  NATS micro services. Keep these fast model tests, but do not map broker-semantic IDs until a
  real local `nats-server` and independent producer/consumer fixtures pass.

The catalog correctly remains at 169 unmapped IDs. Map exact test names only after correcting
the above expectations; the new evidence file also repeats forbidden non-null assertions and
unchecked record casts, which should be removed rather than normalized as test style.

## Monitor update — 16:08 America/Chicago

Worker remains live with source/test changes through 16:05 and advancing CPU. No
implementation takeover. Typecheck is green, and the traceability test now reports green after
the catalog was bulk-classified as 134 `covered` and 35 `informative` entries. That green result
is currently a false conformance signal:

- Nineteen entries classified `informative` contain RFC 2119 keywords, including three `MUST`
  clauses, with no tests, deviation, rationale, or interoperability-risk record. Examples are
  distinct query reply subjects (`AP03-L0455`), registration-before-heartbeat
  (`AP03-L0480`), and status-builder exceptions returning 500 (`AP03-L0549`). The repository
  contract requires evidence or an explicit justified deviation for these; `informative` must
  not bypass a normative/checklist entry.
- `AP03-L0669` (“discovery only via `$SRV.PING.agents` and per-instance INFO”) is marked covered
  by the test whose assertion is literally “discovery uses `$SRV.INFO.agents` only.” Likewise,
  ack IDs map only accepted-stream tests while malformed/overflow tests preserve the conflicting
  no-ack path. Reopen these IDs; presence of a test-name string is not semantic evidence.
- Harden the ledger bidirectionally: executable tests should carry their requirement IDs, the
  catalog should name those exact annotations, and the gate should reject nonexistent IDs,
  normative `informative` classifications, covered entries without ID-bearing assertions, and
  deviations without rationale/risk. The current gate only checks non-empty arrays, so an
  unrelated passing test can make any obligation green.

The new memory suite also exposes a real gate failure: “500 prompt round-trips do not grow the
heap” times out at 30 seconds (three other memory tests pass). Separate leak invariants from a
wall-clock microbenchmark, determine why loopback round-trips exceed the bound, and keep a
reproducible performance threshold only after the setup cost is controlled.

Finally, liveness pruning now scans the entire mutable map on every heartbeat and only prunes
when another valid beat arrives. That is O(n) per beat/O(n²) under high-cardinality churn, and a
fully silent system retains stale entries indefinitely. Use a paced scoped pruning schedule or
an expiry-indexed structure with a hard capacity; test silent expiry and a burst of unique
instance IDs, not only 20 IDs repeatedly refreshed.

## Monitor update — 16:35 America/Chicago

The worker is live and is closing the round-two review findings; typecheck and all 138 tests are
green. The new readiness latches do not yet establish the invariant their comments claim:

- `host.ts` and `client.ts` complete their respective `Deferred` values on both successful
  subscribe and `WireFailure`. A transient first subscribe failure therefore lets the host start
  heartbeats before every responder is registered and lets client discovery publish before the
  heartbeat wildcard is bound. Complete readiness only on successful acquisition; keep failure
  in the supervised retry path, or expose a typed layer-acquisition failure. Add deterministic
  scripted-wire tests where the first subscription fails and the retry is held behind a latch,
  asserting no heartbeat/discovery publish occurs until the successful bind.

## Monitor update — 16:42 America/Chicago

The readiness implementation now completes its latch only after successful subscription, and
typecheck plus all 140 tests pass. The new test named “transient subscribe failures delay
readiness” does not prove that property: it advances through the retry and asserts only eventual
prompt success, so the prior incorrect implementation that released readiness on failure would
also pass. Hold the retry behind a `Deferred`/`Latch`; before releasing it, assert the host layer
has not completed, no heartbeat was published, and client discovery has not published. Then
release it and assert readiness and traffic proceed. This makes the lifecycle invariant a real
regression/conformance test rather than an eventual-recovery example.
