# Principal client tests and conformance evidence

The root `AGENTS.md` applies. Tests in this subtree are executable evidence for the principal NATS
client; their names and placement must state what they actually establish.

## Evidence tiers

- **Pure/unit:** bytes, parsers, schemas, names, policies, or deterministic state transitions. No
  broker claim.
- **Fake/model:** controlled Effect services or `JetStreamMemory`. Proves local contract behavior,
  failure, interruption, cleanup, and bounds; not server parity by itself.
- **Shared interpreter:** `JetStreamConformance.ts` runs the same observable seam cases against the
  memory and live interpreters. Put a claim here only when both implementations must satisfy it.
- **Live server:** `*Live.test.ts`, `*ConformanceLive.test.ts`, and `TestServer.ts` exercise a real
  declared `nats-server`. These establish only the versions and features actually run.
- **Performance/leak:** dedicated measured suites establish the stated workload and threshold, not
  general asymptotic or production guarantees.

## Test construction

- Mirror `src/internal/<Module>.ts` with `<Module>.test.ts`; keep shared harnesses in named support
  modules and out of production exports.
- Use `it.effect`, scopes, `TestClock`, `Deferred`, bounded `Queue`, or `Latch`. Do not use wall-time
  sleeps to prove readiness, cadence, ordering, or cleanup.
- Exercise success, boundary, malformed input, typed failure, interruption, and exactly-once
  cleanup as applicable. Assert queue/fiber/waiter bounds where retention is part of the contract.
- Start broker fixtures through scoped `TestServer`; isolate resources and declare required server
  features/version in the test or its evidence record.

## Mapping specifications to evidence

Derive obligations from every affected clause, equation, law, and required observation in the
accepted semantic declaration. Record the exact test file and test name in the owning specification
or ledger, and keep missing obligations visible. Example counts, imports, comments, and test-name
strings are not traceability.

A memory-only case cannot close a broker-semantic obligation. A live-only case should not be added
to the shared suite unless the memory interpreter is intended to model that observable behavior.
The existing Lean package has two internal kernel-checked worked traces but no TypeScript JSONL
replay harness. Until that bridge lands, report Lean theorem acceptance and TypeScript test results
as separate evidence. When a formal model shares external vectors or traces with TypeScript, test
the encoder/decoder and relation explicitly; agreement with generated examples is not a refinement
proof.
