# effect-nats-jetstream

A native Effect v4 integration for NATS and JetStream, with a Lean 4 model of its semantics
alongside. Experimental.

## Layout

- `ts/` — the Effect TypeScript project (bun)
- `lean/` — the Lean 4 model (Lake)
- `repos/nats.js/` — vendored official NATS TypeScript client, read-only reference
- `notes/` — numbered design notes

## Setup

```sh
cd ts && bun install
cd lean && lake build
```

Rules for working in this repo are in [AGENTS.md](AGENTS.md).
