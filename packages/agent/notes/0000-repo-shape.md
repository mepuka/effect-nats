# 0000 — Repo shape

Decisions made at repo creation, 2026-08-21.

## Two projects, one repo

`ts/` (Effect v4) and `lean/` (Lean 4) live side by side because the Lean model referees the
TS layer: every guarantee the integration claims (delivery, ordering, idempotency, lifecycle)
should have a corresponding formal statement. Keeping them in one repo keeps claims and code
in one review surface.

## Vendoring: subtree for nats.js, pointers for the rest

`repos/nats.js` is vendored with `git subtree --squash` (the pattern from the Effect blog post
"The one weird git trick that makes coding agents more effect-ive"): agents work better from
real source than from documentation, and the official TypeScript client is the primary
reference for protocol framing, JetStream request/reply shapes, and consumer mechanics.

effect, nats-server, the docs mirror, and the ADRs are not duplicated here. The sibling repo
`../Foldable` already holds them as pinned read-only clones with a verified research corpus on
top. Duplicating them would add hundreds of megabytes and a second set of pins to keep honest.

Rejected alternatives: git submodules (initialization friction, indirection); vendoring
effect itself (already pinned next door at the exact npm version installed here).

## Version pins

- `effect` 4.0.0-rc.111 — equals Foldable's `effect-foldable` clone @ `648f566dd`, so source
  citations and installed behavior agree.
- `@nats-io/*` 3.4.0 — current v3 client line (nats.js monorepo).
- Lean toolchain: pinned per `lean/lean-toolchain` (managed by the bootstrap skill).

## Toolchain

bun for install/run (no pnpm on this machine), vitest + @effect/vitest for tests, oxlint for
linting, tsc for typechecking. tsgo/oxlint enforcement layers from the bridge program can come
later; nothing here precludes them.
