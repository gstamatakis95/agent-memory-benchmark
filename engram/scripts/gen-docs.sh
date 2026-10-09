#!/usr/bin/env bash
# make gen-docs (PLAN.md N139, N177, N187): regenerates the documents the plan says are generated and runs the checks
# that go with them (cmd/gendocs).
#
#   scripts/gen-docs.sh            write docs/generated/ and internal/gen/constants/constants_gen.go
#   scripts/gen-docs.sh --check    write nothing; exit 1 when a generated file differs from what the sources produce
#                                  (a hand edit, or a source that changed without a regeneration)
#
# Both fail, and write nothing, when the sources disagree: an enum concept spelled differently in two places, a register
# constant that the plan does not state, a prompt file edited under its pin, an error detail whose gRPC code differs
# between the proto comment, internal/errs and section 4.1.6, a Test name used outside section 8 of the plan that
# section 8 does not define (cmd/gendocs lists every one).
#
# CI regenerates and then runs `git diff --exit-code -- docs/generated internal/gen` (ci.yml), so a hand edit that is
# committed fails the build.
#
# Environment: GUCS_YAML = the Postgres settings table (default deploy/postgres/gucs.yaml; a missing file is an error).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

args=()
if [ -n "${GUCS_YAML:-}" ]; then args+=(-gucs "$GUCS_YAML"); fi
case ${1:-} in
  "") ;;
  --check) args+=(-check) ;;
  *) echo "usage: scripts/gen-docs.sh [--check]" >&2; exit 2 ;;
esac
exec go run ./cmd/gendocs ${args[@]+"${args[@]}"}
