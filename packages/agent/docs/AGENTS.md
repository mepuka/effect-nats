# Agent Protocol documents and ledgers

The root and `packages/agent/AGENTS.md` files apply. This subtree contains artifacts with different
authority; file location alone does not make a document normative.

## Artifact classes

- **Vendored upstream (`packages/agent/docs/upstream/**`):** immutable protocol input. Update only as a deliberate pin
  migration with provenance/integrity review and a conformance-ledger migration.
- **Conformance (`packages/agent/docs/conformance/**`):** derived traceability artifacts. Follow
  `packages/agent/docs/conformance/README.md`; never delete or weaken an obligation merely to make
  the gate pass.
- **Local contract (`packages/agent/docs/01-SPEC.md`):** constrains this implementation but cannot
  amend upstream.
- **Development specifications (`packages/agent/docs/00-PLAN.md`, numbered feature documents):** each must state
  active/proposed/accepted/superseded status. Proposed interfaces do not authorize exports.
- **Extensions (`packages/agent/docs/extensions/**`):** research-only unless an accepted local ADR explicitly promotes
  the design. Extension evidence stays outside upstream conformance.
- **Review reports (`packages/agent/docs/CODEX-*.md`):** historical observations, not standing instructions or proof
  that a later tree still has the same status.

## Specification workflow

A new or revised development specification must name its authority boundary, semantic mismatch or
assumption ledger, affected public/runtime promises, lifecycle and bounds, error/retry behavior,
compatibility/fallback, executable evidence, and acceptance status. Use terms from
`packages/agent/CONTEXT.md`.

When accepted, link the document from the owning plan/index and add a trigger pointer to the
nearest implementation `AGENTS.md`. Keep protocol-derived obligations, local adapter policy,
Effect-AI integration behavior, and non-standard extension behavior in separate ledgers.

For an upstream refresh, review line-ID movement as a contract migration, regenerate deterministic
artifacts, reopen affected mappings, and rerun integrity plus conformance gates. Do not overwrite
the vendored copy as an ordinary prose edit.
