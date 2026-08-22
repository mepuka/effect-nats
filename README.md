# effect-nats

[![CI](https://github.com/mepuka/effect-nats/actions/workflows/ci.yml/badge.svg)](https://github.com/mepuka/effect-nats/actions/workflows/ci.yml)
[![NATS upstream canary](https://github.com/mepuka/effect-nats/actions/workflows/nats-upstream-canary.yml/badge.svg)](https://github.com/mepuka/effect-nats/actions/workflows/nats-upstream-canary.yml)

An Effect v4 and Bun monorepo for NATS infrastructure and agent workflows.

> **Status:** active pre-release implementation. Neither package has a released public API, and
> this project is not endorsed by the NATS or Synadia projects.

## Workspace

- **`effect-nats` (repository root, principal package):** an Effect-native client layer over the official
  `@nats-io/*` packages, covering Core lifecycle and subscriptions plus the JetStream primitives
  required by Key/Value and Object Store.
- **[`effect-nats-agents`](packages/agents/README.md) subsystem:** a typed caller and host implementation of
  Synadia Agent Protocol v0.3, with a vendored protocol, machine-checked requirement ledger,
  deterministic state-machine tests, and native-broker integration.

Both packages pin `effect@4.0.0-rc.111`, install through one root `bun.lock`, and run from the same
CI toolchain.

## NATS client progress

- **Connection lifecycle — internal implementation.** Scope-owned official connection, typed
  initial-connect failures and retry supervision, reconnect-option compilation, observable drain,
  and forced close.
- **Core subscriptions — internal implementation with live proof.** Official callbacks cross one
  bounded Effect queue. Overflow is typed, admitted messages drain before failure, and scope
  release prevents later delivery without constructing the official async iterator.
- **Reconnect — partial native conformance.** Two independent native servers are exercised over
  plain TCP and TLS. Tests prove status ordering, subscription restoration, configured attempt
  budgets, and terminal close. First-connect waiting, auth-family abort, non-zero jitter, requests,
  and ambiguous publishes remain open.
- **JetStream substrate — memory and live implementations.** Stream create/get/delete, publish
  acknowledgement and per-subject CAS, lossless direct get, history/roll-up, filtered replay, start
  positions, pull windows, caught-up signaling, deletion, and typed lag termination.
- **Key/Value — internal implementation.** Strict Schema-backed naming plus get, put, create,
  update, delete, purge, history, watch, keys, status, and destroy over the JetStream seam. The
  behavioral suite currently runs on the memory substrate; dedicated live KV conformance is still
  required.
- **Object Store — proposed contract.** The stream layout, replacement protocol, integrity model,
  cancellation behavior, and first-class Effect Schema boundary are specified in a proposed ADR.
  No implementation or public API is authorized yet.
- **Public package — closed.** `src/index.ts` remains empty by design; internal progress is not yet
  a compatibility promise.

The accepted decisions and active workbenches are indexed in the
[architecture workbench](docs/architecture/README.md). Confirmed review findings and proof gaps are
kept in [TO_FIX.md](TO_FIX.md).

## Agent Protocol progress

The package under `packages/agents` implements both sides of the v0.3 wire contract:

- discovery, protocol-version filtering, heartbeat liveness, query replies, and interruptible
  streamed prompts for callers;
- ack-first response streams, exactly-one terminators, typed service errors, bounded admission,
  queue-group hosting, heartbeats, and `$SRV` responders for hosts;
- total request and chunk decoding with unknown extension preservation; and
- a strict conformance catalog in which every extracted normative stipulation is classified and
  backed by executable evidence or marked informative.

Its Bun suite includes property tests, reply-DFA model checks, virtual-time lifecycle tests,
bounded-memory checks, and native-server cases for round trips, 400/429/503 outcomes, inactivity
timeouts, heartbeat liveness, cancellation, and queue-group balancing.

## Client conformance progress

Conformance is layered so a passing fake cannot substitute for server-observable behavior:

- **Deterministic model tests:** connection policies, lifecycle finalization, bounded bridges,
  naming, KV behavior, and the in-memory JetStream interpreter run without timing sleeps.
- **Shared interpreter contract:** the same JetStream behavior suite runs against both the memory
  interpreter and a fresh native JetStream-enabled `nats-server`.
- **Core lifecycle proof:** a real callback subscription demonstrates witnessed registration,
  bounded release of an idle pull, and no delivery after release; deterministic adapter tests also
  prove exactly one unsubscribe.
- **Two-server reconnect proof:** plain TCP and TLS suites stop the active server, observe the
  official reconnect statuses, restore a subscription on the second server, and verify exact
  exhaustion when both endpoints are unavailable. See the
  [ADR-0001 conformance receipt](docs/architecture/adr-0001-conformance-status.md).
- **Sequence fidelity proof:** JSON-number sequences outside JavaScript's safe range fail typed,
  while decimal sequences from direct-get headers and JetStream acknowledgement subjects remain
  exact `bigint` values.
- **Release drift:** required CI uses a pinned native server and locked NATS.js dependencies;
  pull requests also run against the latest stable releases. A daily canary and grouped Dependabot
  updates keep upstream changes visible. See
  [upstream compatibility CI](docs/UPSTREAM_COMPATIBILITY.md).

Still outside the current proof envelope: authentication and permission matrices, DNS and
WebSocket transports, Core request cancellation and late replies, reconnect-time publish
ambiguity, clustered JetStream leader failure, full live KV behavior, and all Object Store
operations. A green suite is compatibility evidence for the listed scenarios, not a general
delivery or durability guarantee.

## Development

```text
bun install
bun run format:check
bun run lint
bun run check
bun run test
```

`bun run check` and `bun run test` cover both workspace packages. Package-specific commands are
available as `check:client`, `check:agents`, `test:client`, and `test:agents`. Live integration
tests spawn a native `nats-server`; Docker is not required.

Start with [`AGENTS.md`](AGENTS.md) before changing contracts or implementation, then read the
nearest package instructions. Repository-local skills define the required Effect style,
source-fidelity receipt, and testing gates.
