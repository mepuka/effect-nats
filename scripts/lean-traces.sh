#!/usr/bin/env sh
set -eu

# From the effect-nats repository root: `sh scripts/lean-traces.sh`.
#
# lake is run from inside the package so that elan resolves the package's pinned
# toolchain (`lean-toolchain`, leanprover/lean4:v4.33.0). `lake -d <pkg>` from this
# tree would resolve elan's default instead, because effect-nats has no
# `lean-toolchain`; the 2026-08-23 fixtures were built that way under 4.33.1 and
# verified byte-identical to a 4.33.0 build, but the pin is the contract.
package="../Foldable/formal/effect_nats_substrate"
foldable_commit="$(git -C ../Foldable rev-parse HEAD)"
out="$(pwd)/test/fixtures/lean"
(
  cd "$package"
  lake exe effect_nats_traces -- --foldable-commit "$foldable_commit" \
    > "$out/sequential-traces.json"
  lake exe effect_nats_traces -- --subscriber --foldable-commit "$foldable_commit" \
    > "$out/subscriber-traces.json"
)
