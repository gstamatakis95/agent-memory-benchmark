#!/usr/bin/env bash
# Keeps formal/MANIFEST.md, formal/tla/ and the Go tests in step (PLAN.md sections 7.4 and 7.6; register N46, N141,
# N156).
#
#   scripts/formal-manifest-check.sh [BASE]
#
# Fails when
#   1. a configuration formal/tla/*.cfg has no manifest row (or a row has no configuration, or EXPECT disagrees with the
#      row's Expected column, or the first comment block of a must-fail configuration does not name its invariant);
#   2. a .tla or .lean file has no row in the specification table of the manifest;
#   3. a row is `pending(M..)` and PROGRESS.md marks that milestone `done` (the twin was due), or a row is `paired` and
#      no Go test function of that name exists;
#   4. an existing specification file, or a configuration of it, changed against BASE (default: $FORMAL_BASE, else
#      origin/main, else main; the working tree is compared) and no Go test file matching its row in the manifest
#      changed with it. A waiver is a non-empty FORMAL_NO_TEST_CHANGE (CI sets it from the `formal-no-test-change`
#      label and the justification in the pull request); the justification is printed.
# Exit status 0 on success, 1 on a violation, 2 on a setup error.
set -euo pipefail
export LC_ALL=C.UTF-8
cd "$(git rev-parse --show-toplevel)"

MANIFEST=formal/MANIFEST.md
EXPECT=formal/tla/EXPECT
fail=0
err() { echo "formal-manifest-check: $*" >&2; fail=1; }
[ -f "$MANIFEST" ] || { echo "formal-manifest-check: $MANIFEST missing" >&2; exit 2; }

# ---- parse the manifest -------------------------------------------------------------------------------------------
# rows: config|expected|test|status ; maps: spec file|patterns ; nightly: names
rows=$(awk '/^## 1\./ {on = 1; next} /^## 2\./ {on = 0}
  on && /^\|[A-Za-z0-9_]+\|/ && $0 !~ /^\|Config\|/ { print }' "$MANIFEST")
maps=$(awk '/^## 2\./ {on = 1; next} /^## 3\./ {on = 0} on && /^\|formal\// { print }' "$MANIFEST")
nightly=$(awk '/^### Nightly-only/ {on = 1; next} /^## / {on = 0} on && /^- / { sub(/^- /, ""); print }' "$MANIFEST" |
  grep -v -x '(none)' || true)

# ---- 1. configurations ---------------------------------------------------------------------------------------------
cfgs=$(cd formal/tla && ls *.cfg | sed 's/\.cfg$//' | sort)
row_names=$(echo "$rows" | cut -d'|' -f2 | sort)
dups=$(echo "$row_names" | uniq -d)
[ -z "$dups" ] || err "configurations with more than one manifest row: $(echo $dups)"
for c in $cfgs; do
  grep -qx -- "$c" <<<"$row_names" || err "configuration $c has no row in $MANIFEST"
done
for c in $row_names; do
  grep -qx -- "$c" <<<"$cfgs" || err "manifest row $c has no formal/tla/$c.cfg"
done
exp_names=$(grep -v -E '^(#|$)' "$EXPECT" | cut -f1 | sed 's/\.cfg$//' | sort)
for c in $cfgs; do grep -qx -- "$c" <<<"$exp_names" || err "configuration $c is not listed in $EXPECT"; done
for c in $exp_names; do
  grep -qx -- "$c" <<<"$cfgs" || err "$EXPECT lists $c but formal/tla/$c.cfg does not exist"
done

while IFS='|' read -r _ cfg expected test status _; do
  [ -n "$cfg" ] || continue
  want=$(awk -F'\t' -v n="$cfg.cfg" '$1 == n { print $2 }' "$EXPECT")
  [ -n "$want" ] || continue
  if [ "$want" = pass ]; then
    [ "$expected" = pass ] || err "$cfg: EXPECT says pass, the manifest says $expected"
  else
    inv=${want#violates }
    [ "$expected" = "$inv" ] || err "$cfg: EXPECT says $want, the manifest says $expected"
    [ -f "formal/tla/$cfg.cfg" ] && {
      block=$(awk '/^\\\*/ { print; next } { exit }' "formal/tla/$cfg.cfg")
      grep -q -w -- "$inv" <<<"$block" || err "$cfg: the first comment block of the configuration does not name $inv"
    }
  fi
  case $test in Test*) ;; *) err "$cfg: twin test '$test' is not a Go test name" ;; esac
  if [[ $status =~ ^pending\((M[0-9]+\.[0-9]+)\)$ ]]; then
    ms=${BASH_REMATCH[1]}
    # the milestone must be one of PLAN.md section 10; PROGRESS.md (which tracks Phases 0 to 2) says whether it is done
    if ! grep -q -E "^\| \**$ms[ *(]" docs/plan/PLAN.md; then
      err "$cfg: pending($ms) names a milestone that PLAN.md section 10 does not define"
    fi
    state=$(awk -F'|' -v m="$ms" '{ split($2, a, " ")
      if (a[1] == m) { gsub(/^ +| +$/, "", $3); print $3; exit } }' PROGRESS.md)
    if [ "$state" = done ]; then
      err "$cfg: twin $test is pending($ms) but PROGRESS.md marks $ms done; build the twin and mark the row paired"
    fi
  elif [[ $status =~ ^paired\((.+)\)$ ]]; then
    file=${BASH_REMATCH[1]}
    if [ ! -f "$file" ] || ! grep -q -E "^func $test\(" "$file"; then
      err "$cfg: paired($file) but that file has no func $test"
    fi
  else
    err "$cfg: status '$status' is neither pending(Mx.y) nor paired(file)"
  fi
done <<<"$rows"

for n in $nightly; do
  grep -qx -- "$n" <<<"$cfgs" || err "nightly-only list names $n, which is not a configuration"
done

# ---- 2. specification files ----------------------------------------------------------------------------------------
for f in formal/tla/*.tla formal/lean/Engram/*.lean; do
  awk -F'|' -v f="$f" '$2 == f { found = 1 } END { exit !found }' <<<"$maps" ||
    err "$f has no row in the specification table of $MANIFEST"
done

# ---- 4. change coupling --------------------------------------------------------------------------------------------
base=${1:-${FORMAL_BASE:-}}
if [ -z "$base" ]; then
  for cand in origin/main main; do
    if git rev-parse --verify -q "$cand" >/dev/null; then base=$cand; break; fi
  done
fi
if [ -z "$base" ] || ! git rev-parse --verify -q "$base" >/dev/null; then
  echo "formal-manifest-check: no base revision (set FORMAL_BASE); skipping the change-coupling check" >&2
else
  # a file added by the change is not a change of an existing specification: its rows are checked above
  changed=$( { git diff --name-only --diff-filter=MDRT "$base" --; } | sort -u)
  added=$( { git diff --name-only --diff-filter=A "$base" --; git ls-files --others --exclude-standard; } | sort -u)
  # a test file the change adds counts as a test change, like a modified one
  touched=$( { echo "$changed"; echo "$added"; } | sort -u)
  # patterns of a specification: the second column of every row of its .tla file
  patterns_of() { echo "$maps" | awk -F'|' -v f="$1" '$2 == f { print $3 }' | tr '\n' ' '; }
  matches_any() { # FILE PATTERNS
    local f=$1 p
    for p in $2; do
      # shellcheck disable=SC2053
      [[ $f == $p ]] && return 0
    done
    return 1
  }
  specs_changed=""
  while IFS= read -r f; do
    case $f in
      formal/tla/trace/*) ;; # generated TraceNext modules and fixtures, not specifications
      formal/tla/*.tla) specs_changed="$specs_changed $f" ;;
      formal/lean/Engram/*.lean) specs_changed="$specs_changed $f" ;;
      formal/tla/*.cfg) b=$(basename "$f" .cfg); specs_changed="$specs_changed formal/tla/${b%%_*}.tla" ;;
    esac
  done <<<"$changed"
  for spec in $(echo $specs_changed | tr ' ' '\n' | sort -u); do
    pats=$(patterns_of "$spec")
    found=""
    while IFS= read -r f; do
      if [ -n "$f" ] && matches_any "$f" "$pats"; then found=$f; break; fi
    done <<<"$touched"
    if [ -n "$found" ]; then continue; fi
    if [ -n "${FORMAL_NO_TEST_CHANGE:-}" ]; then
      echo "formal-manifest-check: $spec changed without a test change; waived" \
        "(formal-no-test-change): $FORMAL_NO_TEST_CHANGE"
    else
      err "$spec (or one of its configurations) changed against $base but no file matching [${pats% }] changed"
    fi
  done
fi

# ---- the PR job runs when a mapped package changes ----------------------------------------------------------------
# Every directory a specification row maps (PLAN.md section 8.1: tier F is required on PRs touching the packages a
# spec covers) must be in the path filter of formal-quick.yml (internal/formal/** covers the invariant packages).
set -f
for pat in $(cut -d'|' -f3 <<<"$maps"); do
  dir=${pat%/*_test.go}
  case $pat in
    */*_test.go) want="\"$dir/**\"" ;;
    *) want="\"$pat\"" ;;
  esac
  case $pat in
    cmd/engramctl/restore*) want='"cmd/engramctl/restore*"' ;;
    internal/formal/*) want='"internal/formal/**"' ;;
  esac
  grep -q -F -- "$want" .github/workflows/formal-quick.yml ||
    err "formal-quick.yml has no path filter $want for $pat"
done
set +f

# The formal-changed matrix gives each specification a timeout above 30 minutes per nightly-tier configuration (each
# runs under the 30-minute cap), plus ten minutes of set-up.
for spec in $(cut -d_ -f1 <<<"$cfgs" | sort -u); do
  n=0
  for c in $nightly; do
    case $c in "$spec" | "$spec"_*) n=$((n + 1)) ;; esac
  done
  [ "$n" -gt 0 ] || continue
  have=$(sed -n "s/.*{spec: $spec, timeout: \([0-9]*\)}.*/\1/p" .github/workflows/formal-quick.yml)
  if [ -z "$have" ] || [ "$have" -lt $((30 * n + 10)) ]; then
    err "formal-quick.yml: $spec has $n nightly-tier configurations and needs a timeout of at least" \
      "$((30 * n + 10)) minutes (has ${have:-none})"
  fi
done

if [ "$fail" = 0 ]; then
  echo "formal-manifest-check: ok ($(echo "$rows" | grep -c .) configurations," \
    "$(echo "$maps" | grep -c .) specification rows)"
fi
exit $fail
