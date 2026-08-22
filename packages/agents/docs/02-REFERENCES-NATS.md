# 0002 — NATS references for local extension research

Status: non-normative research ledger.

The Synadia Agent Protocol conformance source is only the vendored upstream protocol. The NATS
sources below constrain possible local extensions and underlying transport claims; they do not
amend the protocol or satisfy its conformance ledger.

## Core NATS

- [Publish-subscribe](https://docs.nats.io/nats-concepts/core-nats/pubsub): Core delivery is
  at-most-once; publishing with no matching subscribers discards the message. This explains the
  standard protocol's inactivity timeout and why local durable replies require a separate contract.
- [Queue groups](https://docs.nats.io/nats-concepts/core-nats/queue): queue membership distributes
  messages among members; it does not establish application fairness, durable redelivery, or
  exactly-once work.
- [Request-reply](https://docs.nats.io/nats-concepts/core-nats/reqreply): request/reply is built on
  publish/subscribe and inbox subjects. Dropping reply interest ends caller-side observation but is
  not a remote cancellation acknowledgement.

## JetStream

- [JetStream model deep dive](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive):
  stream storage, acknowledgements, redelivery, and deduplication are distinct mechanisms.
- [Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers): delivery policy,
  acknowledgement policy, acknowledgement wait, redelivery, and consumer durability must be chosen
  explicitly.
- [Key/Value](https://docs.nats.io/nats-concepts/jetstream/key-value-store): KV revisions and
  conditional writes provide key-level concurrency controls, not arbitrary multi-resource
  transactions.
- [Object Store](https://docs.nats.io/nats-concepts/jetstream/obj_store): Object Store chunks and
  metadata have an operation-specific commit/read-integrity model that must not be collapsed into
  generic “durable publish” language.
- [NATS architecture and design ADRs](https://github.com/nats-io/nats-architecture-and-design/tree/main/adr):
  rationale and compatibility context; pinned implementation source remains authoritative when an
  ADR, documentation, and observed behavior differ.

## Where these references apply

The concrete proof burden for durable replies and cancellation is maintained in
[`extensions/durable-cancellation-workbench.md`](extensions/durable-cancellation-workbench.md).
Root client ADRs own Core, JetStream, KV, and Object Store public semantics. The Agents subsystem
may consume those capabilities only through an explicitly negotiated, non-standard extension.
