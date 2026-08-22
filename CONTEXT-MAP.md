# Context Map

## Contexts

- **NATS client** (repository root): the principal Effect-native client for NATS Core and the
  JetStream primitives used by Key/Value and Object Store.
- [Synadia Agent Protocol adapter](packages/agents/CONTEXT.md): an independent Effect
  implementation of the externally owned Synadia Agent Protocol for NATS.

## Relationships

- **Agent Protocol adapter → NATS client**: the adapter uses NATS Core transport semantics. It does
  not redefine Core publish, subscription, request/reply, reconnect, or flush guarantees.
- **NATS client → Agent Protocol adapter**: JetStream capabilities may support explicitly
  negotiated, non-standard extensions, but those extensions are not Synadia Agent Protocol
  conformance and cannot change the standard caller/agent-host wire contract.
