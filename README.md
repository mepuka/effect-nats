# effect-nats

An Effect-native TypeScript client for NATS Core, Key/Value, and Object Store.

This repository is intentionally minimal while the public contract is designed. Before the first
implementation milestone, we will specify:

- supported runtimes and transport boundaries;
- NATS protocol and server-version compatibility;
- Core NATS plus the JetStream primitives required by KV and Object Store;
- durability, acknowledgement, retry, cancellation, and backpressure semantics;
- API stability, testing, benchmarking, and conformance requirements;
- package layout, build tooling, release process, and upstream-governance path.

The repository is not yet a released or NATS-project-endorsed client.

## Development

```text
pnpm install
pnpm check
pnpm test
pnpm lint
pnpm format:check
```

Start with `AGENTS.md`. Research and unresolved architecture decisions live under `docs/`; public
API implementation waits for the initial contract review.
