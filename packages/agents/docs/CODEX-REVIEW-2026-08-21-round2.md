# Codex adversarial review — round 2 (2026-08-21)

Status: archival review receipt. Names and terminology below describe the code as it existed at the
reviewed baseline; current terminology is defined by `../CONTEXT.md`.

## Scope and method

Reviewed the requested protocol kernel, transport/client/host modules, and `src/test/*.ts` against `docs/upstream/core-protocol-v0.3.0.md`, `docs/01-SPEC.md`, the repository engineering contract, and the pinned `effect@4.0.0-rc.111` source. The review exercised public boundaries with the existing scripted wire and loopback bus. I did not modify `docs/conformance/protocol-requirements.json`, `src/test/conformance-evidence.test.ts`, `docs/upstream/*`, or `src/test/conformance/*`.

Final finding count: **4 high, 14 medium, 5 low**. Four implementation findings were fixed surgically and are covered by `src/test/hardening.test.ts`; the remaining findings are report-only.

## High findings

### H1 — Caller does not enforce the reply-stream DFA (open)

- Severity: **High**
- Citation: `src/agents/prompt-engine.ts:92`
- Violated clauses:
  - Upstream §6.4: “Agents MUST emit exactly one `ack` status chunk as the first message on the reply subject.”
  - Upstream §6.5: “Every response stream MUST end with a zero-byte body message carrying no NATS headers.”
  - Upstream §9.3: “An error mid-stream is an error-headered message ... followed by the empty terminator.”
- Evidence: the pipeline classifies messages, applies a timeout, drops the terminator with `takeWhile`, and maps frames, but never applies `stepWellFormed`. A bare terminator succeeds with `[]`; response-before-ack and duplicate ack sequences are also accepted. An `ErrorFrame` fails immediately without consuming or verifying the required following terminator.
- Minimal reproduction:

  ```ts
  const scripted = yield* makeScriptedWire([
    at(0, terminator()),
  ])
  const out = yield* Stream.runCollect(promptStream(request))
  expect(out).toEqual([]) // current success; should be ProtocolViolation
  ```

### H2 — Handler defects were framed as successful completion (fixed)

- Severity: **High**
- Citation: `src/agents/host.ts:270`
- Violated clauses:
  - Upstream §9.2: “500 | Internal error.”
  - Upstream §9.3: “An error mid-stream is an error-headered message ... followed by the empty terminator. This applies whether the error occurs before any content or partway through.”
- Evidence before fix: `handler: () => Stream.die("boom")` produced `ack · terminator` with no error header.
- Fix: `Cause.hasInterruptsOnly` preserves pure interruption; every other unhandled cause publishes a 500 error before the existing single `ensuring` terminator (`src/agents/host.ts:272-279`).
- Minimal reproduction: `src/test/hardening.test.ts:252`.

### H3 — `$SRV.PING` omits the required service version (open)

- Severity: **High**
- Citation: `src/agents/host.ts:199`
- Violated clause: Upstream §3.1, “Required service fields,” includes `version`; §3 also says, “Every agent MUST register as a NATS micro service using `@nats-io/services` or equivalent.”
- Evidence: the PING payload contains only `type`, `name`, and `id`.
- Minimal reproduction:

  ```ts
  const ping = JSON.parse(new TextDecoder().decode(firstPingReply.payload))
  expect(ping.version).toBe(config.version ?? "0.0.0") // currently undefined
  ```

### H4 — Host layer can return before prompt interest is bound (open race)

- Severity: **High**
- Citation: `src/agents/host.ts:313`
- Violated clause: Upstream §3: “Every agent MUST register as a NATS micro service using `@nats-io/services` or equivalent.” Returning the host service before its prompt subscription is bound exposes an unregistered service as ready.
- Evidence: the prompt loop is forked without a `Deferred`/latch acknowledging successful subscription. One full-suite audit run lost the first prompt and timed out; the final full-suite run passed, confirming a scheduling race rather than a deterministic functional failure.
- Minimal reproduction: use a scripted `Wire.subscribe` that blocks on a `Deferred`, acquire `layerHost`, publish immediately, then release the deferred. The publish occurs before interest exists.

## Medium findings

### M1 — Rejected streams omit ack despite the higher-authority MUST (open contract conflict)

- Severity: **Medium**
- Citation: `src/agents/host.ts:232`, `src/agents/protocol/chunks.ts:178`
- Violated clauses:
  - Upstream §6.4: “Agents MUST emit exactly one `ack` status chunk as the first message on the reply subject.”
  - Upstream §12: ack is the “first message on every response stream.”
- Evidence: malformed, oversize, attachment-disabled, and 429 paths produce `error · terminator`. `docs/01-SPEC.md:49` and existing tests explicitly define rejection without ack, but that lower source weakens the upstream MUST and conflicts with the repository authority order.
- Minimal reproduction: `bun test src/test/host.test.ts -t "malformed request"`; the test currently asserts “400 error · terminator, NO ack.”
- Disposition: report-only because a conforming change requires coordinated updates to the additive grammar and existing tests, which this review was forbidden to edit.

### M2 — `$SRV.INFO` may omit required `description` (open)

- Severity: **Medium**
- Citation: `src/agents/host.ts:52`, `src/agents/host.ts:168`
- Violated clause: Upstream §3.1 lists `description` under “Required service fields.”
- Evidence: `AgentHostConfig.description` is optional and the field is conditionally omitted.
- Minimal reproduction:

  ```ts
  const info = await requestInfo(hostConfig({ description: undefined }))
  expect(info.description).toEqual(expect.any(String)) // currently absent
  ```

### M3 — Status-builder defects are not returned as 500 (open)

- Severity: **Medium**
- Citation: `src/agents/host.ts:205`, `src/agents/host.ts:295`
- Violated clause: Upstream §8.7.1: “An exception thrown while constructing the payload MUST be returned as a `Nats-Service-Error-Code: 500` response.”
- Evidence: `heartbeatBody` can defect in `toISOString`; the generic reply loop propagates/restarts instead of responding with 500.
- Minimal reproduction: provide a test `Clock` returning `NaN`, request the status endpoint, and expect an error header with code 500. The current requester receives no reply.

### M4 — Malformed UTF-8 is silently replacement-decoded (open)

- Severity: **Medium**
- Citation: `src/agents/protocol/envelope.ts:47`, `src/agents/protocol/envelope.ts:69`
- Violated clauses:
  - Upstream §5.1 permits “Plain UTF-8 text.”
  - Upstream §5.3 says the caller “treats the original payload as UTF-8 text.”
- Evidence: the decoder uses `{ fatal: false }`; invalid input is altered to U+FFFD rather than rejected in the typed channel.
- Minimal reproduction:

  ```ts
  expect(isRequestError(parseRequest(Uint8Array.of(0xff)))).toBe(true)
  // current: { prompt: "�", attachments: [], extra: {} }
  ```

### M5 — Query reply subjects are not validated (open)

- Severity: **Medium**
- Citation: `src/agents/protocol/chunks.ts:63`, `src/agents/client.ts:296`
- Violated clauses:
  - Upstream §7.1: `reply_subject` is “A fresh NATS subject.”
  - Upstream §2.2: “Subject tokens MUST conform to NATS subject naming rules.”
- Evidence: `reply_subject` is a plain string and `answerQuery` publishes it verbatim. Empty, wildcard, whitespace, and empty-token subjects decode successfully.
- Minimal reproduction:

  ```ts
  expect(Result.isFailure(decodeChunk({
    type: "query",
    data: { id: "q", reply_subject: "a.*", prompt: "p" },
  }))).toBe(true) // currently false
  ```

### M6 — Response/query attachment base64 was not validated (fixed)

- Severity: **Medium**
- Citation: `src/agents/protocol/chunks.ts:120`
- Violated clauses:
  - Upstream §5.2: “Standard-alphabet, padded base64 (RFC 4648 §4). MUST NOT use URL-safe encoding and MUST NOT contain whitespace.”
  - Upstream §6.3 says response attachments have the “shape per §5.2”; §7.1 says query attachments use the “same shape as §5.2.”
- Evidence before fix: response/query attachment contents `a-b_`, `Y Q==`, and `YQ` decoded successfully.
- Fix: reuse the request-envelope base64 predicate and return `MalformedKnown` for malformed response/query attachments (`src/agents/protocol/chunks.ts:19`, `src/agents/protocol/chunks.ts:120-126`).
- Minimal reproduction: `src/test/hardening.test.ts:135`.

### M7 — Unparseable service error codes crossed the typed seam as `NaN` (fixed)

- Severity: **Medium**
- Citation: `src/agents/prompt-engine.ts:25`
- Violated clause: Upstream §9.1 defines `Nats-Service-Error-Code` as a “numeric status code as a string.”
- Evidence before fix: `classify` accepted `Number("not-a-code")` and constructed an `ErrorFrame` whose code was `NaN`.
- Fix: non-integer values now return `ProtocolViolation` (`src/agents/prompt-engine.ts:27-29`).
- Minimal reproduction: `src/test/hardening.test.ts:201`.

### M8 — Client discovery is exposed before heartbeat subscription readiness (open)

- Severity: **Medium**
- Citation: `src/agents/client.ts:168`
- Violated clauses:
  - `docs/01-SPEC.md:90`: “Subscribe-before-discover ordering enforced by the client's typestate: discovery is unavailable until the heartbeat wildcard is bound.”
  - Upstream §8.5: callers “SHOULD subscribe to ... `agents.hb.*.*.*` ... before sending their first `$SRV.PING.agents`.”
- Evidence: the tracker is forked, with no readiness latch before `AgentClient` returns.
- Minimal reproduction: block the heartbeat `subscribe` on a `Deferred`, acquire the client, call `discover`, and observe the discovery publish before releasing the subscription.

### M9 — `answerQuery` skips final broker byte-limit validation (open)

- Severity: **Medium**
- Citation: `src/agents/client.ts:296`
- Violated clause: Upstream §5.4: callers “MUST compute the final encoded payload byte size and fail locally if it exceeds `max_payload`.”
- Evidence: query-answer shape is validated, but encoded `payload.byteLength` is never compared with `wire.brokerMaxPayload` before publish.
- Minimal reproduction: use a loopback bus with `brokerMaxPayload: 8`, answer with a longer encoded payload, and assert no publish. A publish is currently logged.

### M10 — Heartbeat boundary schema accepts non-positive/fractional intervals (open)

- Severity: **Medium**
- Citation: `src/agents/protocol/chunks.ts:214`
- Violated clause: Upstream §8.3 requires `interval_s` to be “> 0; recommended ≥ 1.”
- Evidence: the schema uses plain `Schema.Number`; the client later filters invalid values, but the untrusted boundary itself succeeds.
- Minimal reproduction:

  ```ts
  expect(Result.isFailure(decodeHeartbeat({ ...valid, interval_s: 0 }))).toBe(true)
  ```

### M11 — Pre-admission host fiber fan-out is unbounded (open)

- Severity: **Medium**
- Citation: `src/agents/host.ts:286`
- Violated engineering contract: “All queues and concurrent work have explicit bounds, overflow behavior, and scoped ownership.”
- Evidence: every inbound broker message is forked before the semaphore decides admission; a burst can create arbitrarily many waiting/validation fibers even though handler concurrency is bounded.
- Minimal reproduction: publish 100,000 messages while the only permit is held and instrument fiber creation; the count grows with the burst rather than the configured admission bound.

### M12 — Liveness tracker cardinality is not hard-bounded (open)

- Severity: **Medium**
- Citation: `src/agents/client.ts:164`
- Violated engineering contract: “Correctness fixes must not introduce ... unbounded retention.”
- Evidence: pruning happens only on a later valid beat, and the remote `interval_s` controls expiry. Unique instance IDs with very large intervals can remain indefinitely; no hard capacity/eviction policy exists.
- Minimal reproduction: publish beats for ever-new `instance_id` values with `interval_s: Number.MAX_VALUE`, then inspect tracker size after clock advancement.

### M13 — Unknown extras can make encoding defect outside the typed error channel (open)

- Severity: **Medium**
- Citation: `src/agents/protocol/envelope.ts:117`, `src/agents/client.ts:265`
- Violated engineering contract: “Invalid boundary values fail in the typed error channel.”
- Evidence: `extra` is `Record<string, unknown>` and synchronous `JSON.stringify` can throw for BigInt or cycles.
- Minimal reproduction:

  ```ts
  client.prompt(agent, "p", { extra: { value: 1n } })
  // current: defect/throw; expected LocalViolation
  ```

### M14 — Loopback subscriber queues are unbounded and not shut down (open)

- Severity: **Medium**
- Citation: `src/test/kit.ts:153`
- Violated engineering contract: “All queues and concurrent work have explicit bounds, overflow behavior, and scoped ownership.”
- Evidence: `Queue.unbounded` is used and release only removes the subscription from a `Set`.
- Minimal reproduction: subscribe without consuming, publish in a loop, and observe retained queue size grow without a bound; closing the scope does not explicitly shut down the queue.

## Low findings

### L1 — Protocol matcher accepted impossible literal/subject tokens (runtime fixed; type-model residual)

- Severity: **Low**
- Citation: `src/agents/protocol/subjects.ts:22`
- Violated clauses:
  - Upstream §2.2: “Subject tokens MUST conform to NATS subject naming rules.”
  - Upstream §2.4 permits `>` only as the trailing wildcard form.
- Evidence before fix: empty tokens and a mid-pattern literal `>` matched themselves.
- Fix: the protocol wrapper now rejects invalid concrete and literal tokens before delegating to the linear core matcher (`src/agents/protocol/subjects.ts:22-33`).
- Residual: the re-exported structural `Pattern` type can still represent invalid literals, `literalPattern` can construct one, and `kit.ts` imports the unguarded core matcher. The fix prevents matching but does not make the invalid state unrepresentable.
- Minimal reproduction: `src/test/hardening.test.ts:159`.

### L2 — Heartbeats start before responder registration completes (open SHOULD)

- Severity: **Low**
- Citation: `src/agents/host.ts:313`
- Violated clause: Upstream §8.2: “Agents SHOULD begin publishing heartbeats only after service registration is complete.”
- Evidence: heartbeat is forked before status and every PING/INFO responder, with no readiness handshake.
- Minimal reproduction: block status subscription in a scripted wire and observe the first heartbeat publish before the block is released.

### L3 — Loopback queue-group selection permanently starves later members (open harness fidelity)

- Severity: **Low**
- Citation: `src/test/kit.ts:142`
- Violated clauses:
  - Upstream §3.3 says queue groups enable “load-balancing across same-subject instances.”
  - Upstream §3.4 says NATS will “load-balance requests across them.”
- Evidence: repeated publishes always select the first insertion for a group. The essential one-copy-per-group rule is correct, but distribution is not modeled.
- Minimal reproduction: subscribe `a` then `b` in the same group, publish ten messages, and assert both receive at least one; currently only `a` receives.

### L4 — Forbidden assertion/cast sites remain (open standards hygiene)

- Severity: **Low**
- Citations: `src/agents/protocol/subjects.ts:74`, `src/agents/protocol/envelope.ts:66`, `src/agents/protocol/validation.ts:25`, `src/agents/host.ts:303`, `src/agents/nats-wire.ts:34`
- Violated engineering contract: “Do not use ... unchecked assertion chains, non-null assertions.”
- Evidence: the cited sites contain `!` or redundant casts where ordinary narrowing/local binding is sufficient.
- Minimal reproduction: `rg -n "!| as Uint8Array" src/agents/protocol/subjects.ts src/agents/protocol/envelope.ts src/agents/protocol/validation.ts src/agents/host.ts src/agents/nats-wire.ts`.

### L5 — Codecs are allocated in message hot paths (open performance hygiene)

- Severity: **Low**
- Citations: `src/agents/prompt-engine.ts:14`, `src/agents/prompt-engine.ts:44`, `src/agents/client.ts:315`, `src/agents/host.ts:89`
- Violated engineering contract: “Hoist Schemas, encoders, decoders, and constant byte sequences out of hot paths.”
- Evidence: new `TextDecoder`/`TextEncoder` instances are constructed per message/publish. `envelope.ts:47-48` demonstrates the correct hoisted shape.
- Minimal reproduction: `rg -n "new Text(Encoder|Decoder)" src/agents`.

## Checks that passed

- PASS — Request discrimination checks only the four §5.3 whitespace bytes before `{`; BOM, 256 KiB whitespace, lone-surrogate JSON, and non-object attachments were exercised in `hardening.test.ts`.
- PASS — Envelope top-level and attachment unknown fields survive parse/encode; collision order keeps prompt/attachments/filename/content authoritative (`envelope.ts:117-128`).
- PASS — Standard padded base64 is now enforced on request, response, and query attachments (`envelope.ts:51-52`, `chunks.ts:120-126`).
- PASS — The protocol matcher rejects empty and mid-pattern wildcard literals while retaining the O(tokens) index-based core matcher (`subjects.ts:22-33`).
- PASS — The pure reply DFA rejects content-before-ack, repeated ack/error, and post-terminal writes; property/model and hardening tests exercise the transitions.
- PASS — Invalid numeric error codes produce `ProtocolViolation`, while valid error JSON without `message` falls back to `Nats-Service-Error` (`prompt-engine.ts:25-34`).
- PASS — Pinned rc.111 `Stream.timeoutOrElse({ duration, orElse })` times each pull (`node_modules/effect/src/Stream.ts:3310`); TestClock gap-reset tests pass.
- PASS — NATS request setup binds the inbox before publish (`nats-wire.ts:96-104`) and scoped release unsubscribes (`nats-wire.ts:47-65`); interruption and normal completion each finalize once.
- PASS — Accepted-handler concurrency uses a bounded semaphore and permit exhaustion produces 429 (`host.ts:217`, `host.ts:258-263`).
- PASS — Typed handler failures use taxonomy codes, unknown codes degrade to 500, defects now emit 500, and one `ensuring` emits the final terminator (`host.ts:265-280`).
- PASS — Periodic and status heartbeat payloads share `heartbeatBody`; cadence/status tests pass (`host.ts:205-215`, `host.ts:309-315`).
- PASS — Heartbeat PubSub is bounded/sliding and independent subscribers both receive beats (`client.ts:166`).
- PASS — Discovery filters incompatible records and the client publishes to the advertised endpoint subject verbatim; focused round-trip tests pass.
- PASS — Prompt send-path validation computes final encoded byte length and binds the lower of advertised/broker limits before publish (`client.ts:265-285`).
- PASS — Loopback delivery sends one copy per queue group and one to each ordinary subscriber (`kit.ts:142-149`).
- PASS — Unknown chunk types remain accepted as `Unknown`; known malformed chunks fail; response/query unknown `data` fields are preserved in `extra`.
- PASS — BOM takes the plain-text branch rather than the JSON-envelope branch; BOM code-point retention is ambiguous and was not changed.
- PASS — JSON-escaped lone surrogate data round-trips; the upstream spec does not impose an additional Unicode-scalar restriction.
- PASS — `bun run typecheck`, the 14-test hardening file, the required 61-test focused suite, and the 129-test full suite all exited 0 in final runs.

## Implementation changes

- `src/agents/protocol/chunks.ts`: validate response/query attachment content with the shared standard padded-base64 predicate.
- `src/agents/protocol/subjects.ts`: reject invalid concrete/literal tokens at the protocol matcher boundary.
- `src/agents/prompt-engine.ts`: reject non-integer parsed service error codes as `ProtocolViolation`.
- `src/agents/host.ts`: preserve interruption and translate unhandled handler defects to a 500 error frame before the existing sole terminator.
- `src/test/hardening.test.ts`: 14 adversarial tests covering every requested envelope, matcher, DFA, classifier, and loopback case, plus chunk base64 and handler-defect framing.

## Required command output

### `bun run typecheck`

```text
$ tsc --noEmit
```

### `bun test src/test/protocol.test.ts src/test/properties.test.ts src/test/prompt-engine.test.ts src/test/host.test.ts`

```text
bun test v1.3.14 (0d9b296a)

src\test\host.test.ts:
(pass) host pump (Protocol §6, §9) > accepted prompt produces ack · chunks · terminator — DFA-valid [18.23ms]
(pass) host pump (Protocol §6, §9) > §6.4 malformed request: 400 error · terminator, NO ack [2.61ms]
(pass) host pump (Protocol §6, §9) > §9.3 handler failure: ack · error · terminator with the declared code [4.27ms]
(pass) host pump (Protocol §6, §9) > handler-emitted ack is dropped — the pump owns the single ack [5.38ms]
(pass) host pump (Protocol §6, §9) > §6 overflow: second concurrent prompt gets 429 · terminator [2.65ms]
(pass) host pump (Protocol §6, §9) > §8 heartbeats publish on schedule from the shared builder [2.29ms]
(pass) host pump (Protocol §6, §9) > §8.7.1 status endpoint replies from the same builder [1.49ms]
(pass) client ⇄ host round trip > discover finds the host with required metadata, then prompt streams [10.65ms]
(pass) client ⇄ host round trip > liveness flips off after 3× the declared interval (§8.2) [2.19ms]
(pass) client ⇄ host round trip > independent heartbeat subscribers both observe beats [3.13ms]
(pass) client ⇄ host round trip > §5.4 local validation blocks the wire on oversize payloads [3.68ms]
(pass) client ⇄ host round trip > prompt against an endpoint without required metadata fails locally [1.41ms]

src\test\prompt-engine.test.ts:
(pass) prompt engine (Protocol §6) > ack + chunks + terminator yields the chunk sequence and ends [2.70ms]
(pass) prompt engine (Protocol §6) > §6.6 gap just under the window passes; just over times out [1.69ms]
(pass) prompt engine (Protocol §6) > §6.6 every observed frame resets the gap timer — unknown chunks included [0.75ms]
(pass) prompt engine (Protocol §6) > §9 error headers fail the stream typed, checked before body [0.78ms]
(pass) prompt engine (Protocol §6) > §6.5 zero-byte WITH headers is not a terminator [0.82ms]
(pass) prompt engine (Protocol §6) > reply stream ending without terminator is a protocol violation [0.72ms]
(pass) prompt engine (Protocol §6) > non-JSON chunk is a protocol violation [0.65ms]
(pass) prompt engine (Protocol §6) > interruption releases the subscription exactly once [0.55ms]
(pass) prompt engine (Protocol §6) > normal completion also releases exactly once [0.60ms]

src\test\properties.test.ts:
(pass) parser totality (§5) > parseRequest never throws and always returns a tagged value [3.24ms]
(pass) parser totality (§5) > plain text round-trips verbatim (no trimming) [2.27ms]
(pass) parser totality (§5) > §5.6 encode∘parse identity: unknown fields survive at every level [15.91ms]
(pass) subject matcher laws (§2) > exact literal patterns are singletons [5.67ms]
(pass) subject matcher laws (§2) > * arity is exactly one [3.54ms]
(pass) subject matcher laws (§2) > terminal > requires one-or-more past the prefix [4.69ms]
(pass) subject matcher laws (§2) > accepted tokens never contain dot, wildcard, whitespace, or $ prefix [1.23ms]
(pass) reply DFA model check (§6) > checker agrees with the reference regular language [4.60ms]
(pass) reply DFA model check (§6) > step function is prefix-monotone: illegal prefixes never recover [4.54ms]

src\test\protocol.test.ts:
(pass) subjects (Protocol §2) > parses verb-first identity tree [0.12ms]
(pass) subjects (Protocol §2) > rejects wrong shape and unknown verbs [0.03ms]
(pass) subjects (Protocol §2) > token rules: $ prefix, whitespace, control chars forbidden (§2.2) [0.07ms]
(pass) subjects (Protocol §2) > validated construction rejects malformed identity tokens [0.04ms]
(pass) subjects (Protocol §2) > > matches one-or-more past the pattern [0.06ms]
(pass) subjects (Protocol §2) > heartbeat wildcard partitions from prompts (§2.4) [0.03ms]
(pass) subjects (Protocol §2) > exact literal patterns are singletons [0.07ms]
(pass) request envelope (Protocol §5) > B.1 plain-text shorthand promotes to prompt [0.07ms]
(pass) request envelope (Protocol §5) > §5.3 plain text is promoted VERBATIM — no trimming [0.03ms]
(pass) request envelope (Protocol §5) > B.3 JSON envelope with attachment parses [0.08ms]
(pass) request envelope (Protocol §5) > §5.3 leading whitespace then { still discriminates as JSON [0.03ms]
(pass) request envelope (Protocol §5) > §5.1 zero-byte payload invalid [0.02ms]
(pass) request envelope (Protocol §5) > blank prompts map to EmptyPrompt in both encodings [0.04ms]
(pass) request envelope (Protocol §5) > §5.3 malformed JSON after { → MalformedJson [0.06ms]
(pass) request envelope (Protocol §5) > §5.3 object without prompt string → MissingPrompt [0.04ms]
(pass) request envelope (Protocol §5) > §5.2 non-standard base64 rejected [0.03ms]
(pass) request envelope (Protocol §5) > §5.6 unknown top-level fields preserved [0.02ms]
(pass) request envelope (Protocol §5) > §5.6 unknown fields INSIDE attachments preserved [0.03ms]
(pass) request envelope (Protocol §5) > encodeRequest ∘ parseRequest is identity on well-formed envelopes [0.06ms]
(pass) local validation (Protocol §5.4) > attachments when attachments_ok=false fails locally [0.11ms]
(pass) local validation (Protocol §5.4) > oversize fails with actual > limit [0.02ms]
(pass) local validation (Protocol §5.4) > smaller broker cap binds over advertised limit [0.02ms]
(pass) local validation (Protocol §5.4) > max_payload format (§2.1) [0.03ms]
(pass) stream model (Protocol §6, §9) > accepted shape: ack · chunk* · terminator [0.02ms]
(pass) stream model (Protocol §6, §9) > §9.3 error after acceptance: ack · chunk* · error · terminator [0.01ms]
(pass) stream model (Protocol §6, §9) > §6.4 rejection before acceptance: error · terminator, no ack [0.02ms]
(pass) stream model (Protocol §6, §9) > violations: no ack first, repeated ack, empty, post-terminator writes [0.04ms]
(pass) stream model (Protocol §6, §9) > §6.6 unknown chunk types decode as Unknown, not errors [0.08ms]
(pass) stream model (Protocol §6, §9) > known chunk type violating its schema is a decode failure [0.48ms]
(pass) stream model (Protocol §6, §9) > §5.6 attachment extras survive chunk decode [0.17ms]
(pass) stream model (Protocol §6, §9) > error taxonomy covers Protocol §9.2 codes [0.02ms]

 61 pass
 0 fail
 121 expect() calls
Ran 61 tests across 4 files. [297.00ms]
```

---

## Post-review disposition (main session, same day)

Every open finding was addressed after this review completed, except two deliberate declines:

| Finding | Disposition |
|---|---|
| H1 caller DFA | **Fixed** — `promptStream` now threads every frame through `stepWellFormed`; bare terminator, content-before-ack, and duplicate ack fail as `ProtocolViolation` (tests: `robustness.test.ts :: caller-side reply DFA`). An `ErrorFrame` still fails the stream immediately — a failed stream cannot await the trailing terminator; documented in the engine. |
| H3 PING version | **Fixed** — ping replies carry `version` and `metadata` (test: `robustness.test.ts :: $SRV.PING replies carry the service version`). |
| H4 registration race | **Fixed** — every responder loop signals a bind latch released ONLY on a successful subscribe; failed attempts are retried by the supervisor while construction keeps waiting, so readiness is never faked (an early revision released the latch on failure too — caught and corrected). The layer awaits all latches before returning, and the heartbeat loop starts only after (also resolves L2). Tests: `robustness.test.ts :: a prompt published immediately after layer build round-trips` and `:: transient subscribe failures delay readiness instead of faking it`. |
| M1 rejection without ack | **Declined** — §6.5 "Error completion" explicitly permits *zero* chunks before the error-headered message, and §6.4 defines ack as confirming *acceptance*. `error · terminator` for rejected requests is therefore a valid error completion; treating §12's summary phrasing as overriding §6.4/§6.5 would force acknowledging requests the host never accepted. Recorded in docs/01-SPEC.md §3. |
| M2 description | **Fixed** — `description` always present (config or derived default). |
| M3 status 500 | **Fixed** — reply-construction defects answer `Nats-Service-Error-Code: 500`. |
| M4 UTF-8 | **Fixed** — fatal decoder; `MalformedUtf8` → 400. |
| M5 reply_subject | **Fixed** — `decodeChunk` rejects non-publishable `reply_subject`; `answerQuery` re-validates. |
| M8 discover readiness | **Fixed** — `discover` awaits the heartbeat-wildcard bind latch (§8.5). |
| M9 answerQuery size | **Fixed** — broker-cap check before publish. |
| M10 heartbeat schema | **Fixed** — `interval_s` checked `> 0` at the Schema boundary. |
| M11 fan-out bound | **Fixed** — pre-admission responders bounded by `maxInFlight × 8 + 32` permits; overflow answers 429 inline with bounded work. |
| M12 liveness bound | **Fixed** — prune horizon clamps declared intervals to 1 h; hard capacity (default 4096) evicts stalest (test: `memory.test.ts`). |
| M13 extras defects | **Fixed** — encode wrapped; unserializable extras are `LocalViolation`. |
| M14 kit queues | **Fixed** — `Queue.dropping(1024)` (slow-consumer drop, like real NATS) + shutdown on scope release. |
| L1 residual | **Accepted** — runtime guard stands; making invalid `Pattern` literals unrepresentable needs a branded constructor, deferred. |
| L2 heartbeat ordering | **Fixed** with H4. |
| L3 queue-group rotation | **Fixed** — loopback delivery round-robins group members. |
| L4 assertions | **Fixed** at all five cited sites. |
| L5 codec allocation | **Fixed** — module-level `TextEncoder`/`TextDecoder` singletons everywhere on hot paths. |

Full suite after disposition: 139 pass / 0 fail, `tsc --noEmit` clean, strict conformance gate green.
