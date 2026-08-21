# effect-nats-jetstream

An [Effect](https://effect.website)-native implementation of the
[Synadia Agent Protocol for NATS](docs/upstream/core-protocol-v0.3.0.md)
v0.3 — both sides of the wire:

- **Caller** — discovery over `$SRV.INFO.agents`, protocol-version filtering,
  heartbeat liveness tracking, and prompting as an interruptible `Stream` with
  the spec's per-stream inactivity timeout.
- **Host** — a micro-service-compatible agent: ack-first reply pump,
  terminator-exactly-once, error taxonomy on NATS service-error headers,
  supervised heartbeats, `$SRV` responders, and bounded admission (429 on
  overflow).

Everything time- or wire-dependent sits behind two seams (`Clock`, `Wire`), so
the entire runtime is tested deterministically under `TestClock` with zero
servers and zero wall-clock sleeps.

## Correctness stance

- **Vendored, byte-pinned spec.** `docs/upstream/core-protocol-v0.3.0.md` is
  the normative contract (SHA-256-pinned). A requirement extractor pulls every
  normative stipulation out of it (169 at this pin), and a strict gate test
  fails the build unless each one is classified and, where testable, mapped to
  a real test (`docs/conformance/protocol-requirements.json`).
- **Explicit reply state machine.** A reply stream is
  `ack · chunk* · error? · terminator` (or `error · terminator` for rejected
  requests). One DFA (`stepWellFormed`) is simultaneously the host pump's
  structure, the caller's runtime checker, and the model-based test oracle —
  checked against a reference regular language with fast-check.
- **Total boundaries.** Request parsing is total over arbitrary bytes;
  unknown fields survive decode→encode at every level (§5.6); invalid UTF-8,
  malformed base64, and non-publishable reply subjects fail in typed error
  channels, never as defects.
- **Bounded everything.** Admission, pre-admission fan-out, heartbeat fan-out,
  and liveness tracking all have explicit bounds and eviction policies —
  verified by memory tests (subscription-leak accounting, heap-stability under
  10k-message load).
- **Formal companion.** The subject algebra and delivery-guarantee lattice are
  proved in Lean 4 at
  [mepuka/effect-nats-lean](https://github.com/mepuka/effect-nats-lean);
  the TS carriers mirror those structures one-to-one.

## Layout

```
src/agents/protocol/   pure kernel: subjects, envelope, chunks + reply DFA, validation
src/agents/            Wire seam, prompt engine, AgentClient, AgentHost, errors
src/core/              closed-world subject algebra + schema registry
src/test/              property, model-based, TestClock, loopback, memory suites
docs/upstream/         vendored protocol spec (byte-pinned)
docs/conformance/      requirement catalog driving the strict gate
```

## Developing

```bash
bun install
bun test           # full suite, including the conformance gate
bun run typecheck
```

Pinned to `effect@4.0.0-rc.111`; the installed package source is authoritative
for API shapes.

## Credits

The vendored protocol specification in `docs/upstream/` is authored by
Synadia; see the file header for version and provenance. This repository is an
independent implementation.

## License

Apache-2.0. See [LICENSE](LICENSE).
