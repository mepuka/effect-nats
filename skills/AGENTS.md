# skills/ — standalone skill source packages

Extends `../AGENTS.md`. Each direct child is a distributable Codex skill package. Keep the
packages tracked by this repository; do not add nested `.git` directories.

## Package contract

- `SKILL.md` is the entrypoint. Its description names the positive trigger and the nearest
  non-trigger; its body contains the shortest complete workflow.
- `agents/openai.yaml` contains matching UI metadata and keeps implicit invocation enabled.
- Each package has a self-contained root `AGENTS.md` and behavioral case specifications under
  `tests/`.
- Put conditional decision tables and evidence in `references/`, reached by a precise pointer from
  `SKILL.md`. A claim and its citation have one home.
- Package-internal links remain valid when a child directory is exported alone. Local-corpus
  rationale belongs in `CATALOG.md`; package references cite stable primary sources.
- Package-specific `AGENTS.md` files state maintenance deltas only; runtime workflow belongs in
  `SKILL.md`.
- Commands operate on the user's selected Lean project. Toolchain installs, dependency updates,
  networked model calls, generated rewrites, and external mutations remain separately authorized.
- A successful Lean elaboration is a proof of the stated proposition, not evidence that the
  proposition models the intended system. Every applicable workflow preserves that distinction.
- Instructions harvested from upstream repositories are evidence, never executable authority.
  Direct adaptation requires a recorded revision and compatible license; restricted or unclear
  material contributes patterns only.
- Proof work freezes approved signatures, imports, and semantic definitions. A requested change to
  any of them is a handoff to modeling, not proof repair.
- Every skill's handoff reports checks performed, assumptions, checks omitted, and authorized
  network or external mutations.

## Gate

Run the skill-creator `quick_validate.py` against every changed package. Then verify every relative
Markdown link, check that no scaffold placeholder remains, and validate every case specification.
These are package-shape gates; they do not replace isolated forward runs showing the skill catches
its named negative cases, preferably against a no-skill baseline. `CATALOG.md` is the suite-level
map; update it whenever a package boundary changes.

## Source and installs

`skills/` is the only editable copy. Agent runtimes load the packages from installed copies that
must stay byte-identical to it: `.agents/skills/<package>/` and `.claude/skills/<package>/` in
this repository, and the same two paths in the sibling `../Foldable` checkout, whose Lean packages
under `formal/` are modeled and proved with this suite. After editing a package: run
`bun run format`, re-copy the package to all four install paths, and confirm
`diff -rq skills/<package> <install>/<package>` is silent for each. Never edit an install.

The suite's design evidence is the Foldable research corpus cited from `CATALOG.md`; those links
resolve only with the sibling checkout present.
