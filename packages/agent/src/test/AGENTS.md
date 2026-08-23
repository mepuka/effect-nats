# Agent Protocol test evidence

The root, `packages/agent/AGENTS.md`, and `packages/agent/src/AGENTS.md` files apply. This subtree
contains conformance, property, state-machine, lifecycle, memory, robustness, and live-broker
evidence.

## Requirement mapping

`packages/agent/docs/conformance/protocol-requirements.json` is the traceability ledger derived from the vendored
protocol. For every affected ID:

1. name exact executable test files and test names in the ledger;
2. jointly cover the whole obligation, including each clause in bundled source lines;
3. cover success, boundaries, malformed input, failure, interruption, and cleanup where relevant;
4. keep the entry `unmapped` when executable evidence is incomplete; and
5. record a `SHOULD` deviation with interoperability risk and test both sides of an implemented
   `MAY`.

Do not satisfy traceability by checking that a test-name string exists. Assertions against a
duplicate reference model alone are not implementation evidence.

## Evidence selection

- Use `effect/testing/FastCheck` or `TestSchema` for codecs/parsers and unknown-field preservation.
- Use DFA/model tests for reply ordering and terminal states.
- Use `TestClock` and finalizer observations for cadence, inactivity, interruption, and cleanup.
- Use bounded-admission and retention observations for concurrency/memory contracts.
- Use a real local `nats-server` plus independent SDK peers for broker and interoperability claims.

Memory/loopback tests are fast local evidence, not broker conformance. Tests for Effect AI bridges
or local extension protocols must point to their own accepted specification and remain outside the
upstream requirement ledger.
