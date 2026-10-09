#!/usr/bin/env bash
# Fails when any source line in the repository exceeds 120 characters (counted as UTF-8 characters, not bytes).
# golangci-lint `lll` covers Go and `buf format` does not check length, so this script covers every other source kind
# the orchestrator prompt names (SQL, TLA+, TLC configs, Lean, YAML, proto, Makefile, shell) plus repository Markdown.
# docs/plan/ is the frozen plan copy and is exempt; gen/ is generated.
set -euo pipefail
export LC_ALL=C.UTF-8
cd "$(git rev-parse --show-toplevel)"
rc=0
while IFS= read -r -d '' f; do
  if ! grep -q -n -E '^.{121,}' "$f"; then continue; fi
  echo "line longer than 120 characters in $f:" >&2
  grep -n -E '^.{121,}' "$f" | cut -d: -f1 | sed "s|^|  $f:|" >&2
  rc=1
done < <(git ls-files -z -- '*.sql' '*.tla' '*.cfg' '*.lean' '*.yaml' '*.yml' '*.proto' '*.sh' '*.md' 'Makefile' \
  ':!docs/plan/**' ':!gen/**')
exit $rc
