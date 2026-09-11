#!/usr/bin/env bash
set -euo pipefail

# Run a fixed number of retained closed-loop trials. The driver writes JSONL
# histories; its stderr summary is captured separately. This is a smoke and
# development runner, not the pre-registered open-loop final evaluation.
target=${1:?client target required, e.g. 127.0.0.1:8080}
consistency=${2:?strong, causal, or eventual required}
policy_version=${3:?policy version required}
out_dir=${4:-results/$(date -u +%Y%m%dT%H%M%SZ)-${consistency}}
mkdir -p "$out_dir"

for trial in 1 2 3 4 5; do
  go run ./cmd/meridian-load \
    -target "$target" \
    -consistency "$consistency" \
    -policy-version "$policy_version" \
    -operations 10000 \
    -concurrency 16 \
    -raw "$out_dir/trial-${trial}.jsonl" \
    2>"$out_dir/trial-${trial}.summary.json"
done
