# Principal NATS client internals

The root `AGENTS.md` applies. This subtree is experimental internal implementation until an
accepted ADR promotes a public surface.

## Authority routing

Before any change here, read `TO_FIX.md` and `docs/STYLE.md`. Then follow the branch that matches
the edited seam:

- **Transport, reconnect, subscription, or lifecycle:** read
  `docs/architecture/adr-0001-reconnect-limits.md`,
  `docs/architecture/adr-0002-wrap-official-client.md`,
  `docs/architecture/adr-0005-configuration-layers.md`,
  `docs/architecture/adr-0007-connection-lifecycle-policy.md`, and
  `docs/research/nats-js-reference.md`.
- **Subject parsing or matching:** read `docs/research/nats-js-reference.md` and preserve the exact
  NATS token/wildcard grammar at the raw boundary.
- **JetStream or KV:** read `docs/architecture/adr-0003-kv-naming-strictness.md`,
  `docs/architecture/adr-0004-revision-representation.md`,
  `docs/architecture/adr-0005-configuration-layers.md`,
  `docs/research/jetstream-kv-bucket-naming.md`, `docs/research/nats-js-reference.md`, and
  `test/JetStreamConformance.ts`. `docs/architecture/kv-slice-scoping.md` is draft scoping, not an
  accepted contract.
- **Object Store:** `docs/architecture/adr-0006-object-store-stream-and-schema-boundary.md` and
  `docs/architecture/object-store-slice-scoping.md` are currently proposed/draft. Keep experiments
  internal until the decision registry marks the required contract accepted.

## Declaration-first implementation

Configuration and operation specifications are the model source. Locate the accepted semantic
declaration before changing a service operation, transition, error, configuration constructor, or
elaboration rule. If the declaration is absent or underspecified, keep the code experimental and
complete the decision workflow rather than inferring semantics from comments or current behavior.

The TypeScript contract and its memory/live implementations must preserve the declaration's named
domain, operation signature, preconditions, observations, typed failures, equations, and laws.
Comments should point back to the declaration and explain representation or implementation choices
only.

## Contracts and interpreters

- Define the observable contract in the seam module before changing an interpreter. Errors,
  events, start positions, buffering, interruption, and finalization are part of that contract.
- `JetStreamMemory.ts` is a deterministic executable model for the exposed seam; it is not a NATS
  server replica. `JetStreamLive.ts` is the server adapter. Behavior claimed for both belongs in
  `test/JetStreamConformance.ts` and must run unchanged against both.
- Keep server-only mechanics behind the live adapter and model-only controls out of the production
  seam. A server semantic claim needs a live-server test or pinned upstream evidence.
- For a new primitive, specify its state, transitions, observable trace, failure modes, ownership,
  and bounds before implementation. Preserve one canonical representation at each boundary.

## Configuration subtypes and elaboration

A new configuration subtype, ordering, merge, defaulting, or elaboration rule requires an accepted
semantic declaration that names:

1. raw input syntax and validation;
2. the semantic configuration carrier and any order/refinement relation;
3. defaulting/normalization and the effective runtime configuration;
4. elaboration failure and observability; and
5. the coherence laws to test or prove.

Until then, keep the experiment internal and do not advertise semantic subtyping. Tests must
separate raw decoding, validation, normalization/elaboration, and runtime behavior. Where the seam
is shared, equal effective configurations must have the same observable memory/live contract.

## Existing Lean declaration

`../Foldable/formal/effect_nats_substrate/` mechanizes the sequential JetStream memory-core slice
against effect-nats pin `d06223f`. Its frozen surface includes raw/validated configuration,
order-sensitive `ConfigEq`, subject matching, stream state, five non-streaming operations,
reachability/invariants, T1 create semantics, T2 sequence discipline, and two kernel-checked traces.

Before changing any covered declaration or interpreter:

1. read `../Foldable/formal/effect_nats_substrate/AGENTS.md`,
   `../Foldable/formal/effect_nats_substrate/README.md`,
   `../Foldable/formal/effect_nats_substrate/docs/signature-snapshot.md`, and
   `../Foldable/research/2026-08-22-first-slice-jetstream-memory-lean-model.md`;
2. compare the affected TypeScript seam with the formalization pin;
3. change the governing semantic declaration before its Lean encoding and interpreters;
4. update the signature snapshot when a frozen Lean declaration changes; and
5. run `lake build` in the formal package plus the applicable TypeScript evidence gates.

The current TypeScript-to-Lean link is reviewed transliteration at a pinned commit, with
kernel-checked worked traces inside Lean. JSONL trace replay/refinement is deferred. Subscribers,
`consume`, buffer policy, Effect fibers/scopes, replication, and NATS-server semantics remain
outside this model until their own slices define relations and obligations.
