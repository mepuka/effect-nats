#!/usr/bin/env sh
set -eu

# From the effect-nats repository root: `sh scripts/lean-traces.sh`.
foldable_commit="$(git -C ../Foldable rev-parse HEAD)"
lake -d ../Foldable/formal/effect_nats_substrate exe effect_nats_traces -- \
  --foldable-commit "$foldable_commit" \
  > test/fixtures/lean/sequential-traces.json
