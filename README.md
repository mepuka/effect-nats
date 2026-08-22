# effect-nats

[![CI](https://github.com/mepuka/effect-nats/actions/workflows/ci.yml/badge.svg)](https://github.com/mepuka/effect-nats/actions/workflows/ci.yml)
[![NATS upstream canary](https://github.com/mepuka/effect-nats/actions/workflows/nats-upstream-canary.yml/badge.svg)](https://github.com/mepuka/effect-nats/actions/workflows/nats-upstream-canary.yml)

An Effect v4-native TypeScript client for NATS Core, Key/Value, and Object Store, built over the
official `@nats-io/*` packages behind adapter-neutral internal seams.

> **Status:** active pre-release implementation. Core lifecycle, subscriptions, reconnect, the
> JetStream substrate, and KV are implemented internally. Object Store is in design. The package
> root deliberately exports nothing until the contracts and conformance gates are ready for public
> review.

This project is not yet released or endorsed by the NATS project.

## Progress

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

## Conformance progress

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
pnpm install
pnpm check
pnpm test
pnpm lint
pnpm format:check
```

Live integration tests spawn a native `nats-server`; Docker is not required. Start with
[`AGENTS.md`](AGENTS.md) before changing contracts or implementation. The repository-local
implementation and testing skills define the required Effect style, source-fidelity receipt, and
test gates.
