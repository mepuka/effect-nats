# ADR-0004: Revision and sequence representation

Status: accepted, 2026-08-21. Discharges open decision 4 of
[kv-slice-scoping.md](kv-slice-scoping.md).

## Decision

Stream sequences and KV revisions are `bigint` throughout the substrate and domain layers.
JetStream sequences are unsigned 64-bit on the wire, which exceeds `Number.MAX_SAFE_INTEGER`,
so `number` silently corrupts above 2^53. Official nats.js surfaces `number` for sequences;
the wrap adapter converts at its boundary and that conversion is lossless in the
client-to-wire direction (a `bigint` the adapter sends came from the server) while
wire-to-client values pass through `BigInt(n)` unchanged. The public `Revision` type, when it
ships, is a branded `bigint`.

## Consequences

- No arithmetic on revisions outside comparison and equality; the substrate never invents
  sequence values.
- The wrap adapter owns the single `number`-to-`bigint` conversion point and its test.
