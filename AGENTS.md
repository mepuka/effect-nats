# effect-nats-jetstream — rules of the repo

This repo is the experiment ground for a native Effect v4 + NATS/JetStream integration: an
Effect-first client layer over the NATS protocol and JetStream, paired with a Lean 4 model of
its compositional semantics. Product decisions made here are informed by the evidence corpus
in the sibling repo `../Foldable`; this repo holds the code, Foldable holds the proof.

## Map

| Path | What it is |
|---|---|
| `ts/` | The Effect TypeScript project. `effect` pinned to `4.0.0-rc.111`, `@nats-io/*` to `3.4.0`. |
| `lean/` | The Lean 4 model (Lake package). Formalizes semantics the TS layer claims to have. |
| `repos/nats.js/` | Vendored `nats-io/nats.js` @ `3a7b736b` (git subtree, squashed) — the official TypeScript client monorepo. Read-only reference. |
| `notes/` | Design notes and decisions, numbered, append-only. |

## Sibling evidence corpus

`C:\Users\kokok\Dev\Foldable` holds pinned read-only clones and verified research:

- `effect-foldable/` @ `648f566dd` — effect source at exactly the version installed here (4.0.0-rc.111)
- `nats-server-foldable/` @ `8e2b49f` — server source, VERSION 2.15.0-dev
- `nats-docs-foldable/` — 859-page Markdown mirror of docs.nats.io. Never crawl it: grep the
  per-page index at `research/logs/nats-docs-manifest.txt` for keywords, then open only the
  exact pages it names.
- `nats-adr-foldable/` @ `e23afc3` — 53 NATS architecture decision records
- `research/` — surveyed and verified documents; `research/README.md` is the index
- `skills/` — the six-package Lean workflow skill suite; use it for all Lean work

## Laws

1. **Vendored source is read-only reference material.** Never edit, build into, or import from
   `repos/` or any Foldable clone. Read it to learn real patterns; prefer patterns found in
   source over guesses.
2. **Versions are pinned to the evidence.** The `effect` npm pin matches the surveyed clone,
   so a claim about behavior can be checked against source. Bump pins deliberately, updating
   the subtree and this file in the same commit.
3. **Citations are verified, never estimated.** A `path:line` citation may only be written
   after opening that file at that line.
4. **Lean work goes through the skill suite.** Bootstrap, strategy, invariants, algebraic
   systems, proof loop, assurance review — in Foldable's `skills/CATALOG.md` order.
5. **Design decisions land in `notes/`** as numbered entries, with the alternatives that were
   rejected and why.
