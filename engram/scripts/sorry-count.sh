#!/usr/bin/env bash
# Fails when the number of `sorry` placeholders in formal/lean/ exceeds formal/lean/SORRY_BASELINE (PLAN.md section 7.6:
# the baseline is 3, so a new unfinished proof is a PR-visible event and the count only goes down). Only `sorry` in code
# counts: line comments (`-- ...`) and block comments (`/- ... -/`, doc comments included) are stripped first.
set -euo pipefail
export LC_ALL=C.UTF-8
cd "$(git rev-parse --show-toplevel)"

baseline_file=formal/lean/SORRY_BASELINE
baseline=$(grep -m1 -E '^[0-9]+$' "$baseline_file") || { echo "sorry-count: no number in $baseline_file" >&2; exit 2; }

count=0
while IFS= read -r -d '' f; do
  n=$(perl -0777 -pe 's{/-.*?-/}{}gs; s{--[^\n]*}{}g' "$f" | { grep -o -w 'sorry' || true; } | wc -l)
  [ "$n" -eq 0 ] || echo "sorry-count: $f: $n"
  count=$((count + n))
done < <(find formal/lean -name '*.lean' -not -path '*/.lake/*' -print0 | sort -z)

echo "sorry-count: $count (baseline $baseline)"
if [ "$count" -gt "$baseline" ]; then
  echo "sorry-count: $count sorry placeholders exceed the baseline $baseline in $baseline_file" >&2
  exit 1
fi
