#!/usr/bin/env bash
# Runs the TLC model checks of formal/tla/ and compares every outcome with formal/tla/EXPECT (PLAN.md section 7.6,
# register N46, N141, N188).
#
#   scripts/formal-run.sh quick [CONFIG...]   SANY parse of every spec and every configuration of the quick tier of
#                                             formal/MANIFEST.md (the PR job; < 5 min on four cores)
#   scripts/formal-run.sh full  [CONFIG...]   every configuration (or the named ones), 30-minute cap each (nightly)
#   scripts/formal-run.sh changed [BASE]      the nightly-tier configurations of the specifications that a change
#                                             against BASE (default origin/main) touches, 30-minute cap each (PR job)
#   scripts/formal-run.sh sany                SANY parse of every spec only
#   scripts/formal-run.sh report              regenerate formal/tla/results/RESULTS.md from the logs and exit
#
# A CONFIG is a name with or without the .cfg suffix. Each configuration writes formal/tla/results/<name>.log (the raw
# TLC output plus one trailer line "@@ engram-formal ...") and the table formal/tla/results/RESULTS.md is regenerated
# from the logs. Outcomes: a design configuration must end with "No error has been found"; a must-fail configuration
# with exactly its named invariant violated; a timeout is INCOMPLETE (neither pass nor fail, reported with the states
# and depth reached). Exit status: 0 when every run configuration matched EXPECT (INCOMPLETE ones are listed and do
# not fail the run unless FORMAL_STRICT=1), 1 on any mismatch or tool error, 2 on a setup error.
#
# Environment: TLA2TOOLS_JAR (explicit jar, must match the pinned hash), FORMAL_CACHE_DIR (download cache),
# FORMAL_WORKERS (default 4), FORMAL_HEAP (default 6g), FORMAL_CAP_SECONDS (default 1800), FORMAL_WORK_DIR (TLC state
# directories, default a mktemp directory), FORMAL_SKIP_EXISTING=1 (skip configurations whose log already has a
# trailer), FORMAL_JOBS and FORMAL_FAST_SECONDS (the parallel fast group of quick), FORMAL_ONLY_SPEC (changed: one
# specification), FORMAL_DRY_RUN=1 (print the configurations and stop), FORMAL_STRICT=1.
set -euo pipefail
export LC_ALL=C.UTF-8
cd "$(git rev-parse --show-toplevel)"

TLA_DIR=formal/tla
RESULTS_DIR=$TLA_DIR/results
EXPECT_FILE=$TLA_DIR/EXPECT
WORKERS=${FORMAL_WORKERS:-4}
HEAP=${FORMAL_HEAP:-6g}
CAP=${FORMAL_CAP_SECONDS:-1800}

# The nightly-only tier is the list under "Nightly-only configurations" in formal/MANIFEST.md (PLAN.md section 7.6: the
# design configurations that take longer than two minutes, plus the must-fail configurations that do not fit the five
# minutes of formal-quick on a four-core machine; see docs/briefs/reports/M0.7-report.md).
MANIFEST=formal/MANIFEST.md
nightly_only() {
  awk '/^### Nightly-only/ {on = 1; next} /^## / {on = 0} on && /^- / { sub(/^- /, ""); print }' "$MANIFEST" |
    grep -v -x '(none)' || true
}

die() { echo "formal-run: $*" >&2; exit 2; }

# shellcheck source=formal-jar.sh
. "$(dirname "${BASH_SOURCE[0]}")/formal-jar.sh"

# expect_of NAME prints "pass" or "violates INVARIANT" from EXPECT.
expect_of() {
  awk -F'\t' -v n="$1.cfg" '$1 == n { print $2; found = 1 } END { exit !found }' "$EXPECT_FILE"
}

all_configs() { grep -v -E '^(#|$)' "$EXPECT_FILE" | cut -f1 | sed 's/\.cfg$//'; }

in_list() { case " $2 " in *" $1 "*) return 0 ;; esac; return 1; }

quick_configs() {
  local c slow
  slow=$(nightly_only | tr '\n' ' ')
  for c in $(all_configs); do
    if ! in_list "$c" "$slow"; then echo "$c"; fi
  done
}

# classify LOG TIMED_OUT prints the outcome: pass | violates:A[,B] | liveness | incomplete | error:<first line>.
classify() {
  local log=$1 timed_out=$2 inv
  if [ "$timed_out" = 1 ]; then echo incomplete; return; fi
  if grep -q '^Model checking completed. No error has been found' "$log"; then echo pass; return; fi
  inv=$(sed -n 's/^Error: Invariant \(.*\) is violated\.$/\1/p' "$log" | sort -u | paste -sd, -)
  if [ -n "$inv" ]; then echo "violates:$inv"; return; fi
  if grep -q '^Error: Temporal properties were violated' "$log"; then echo liveness; return; fi
  echo "error:$(grep -m1 -E '^(Error|Fatal|TLC threw)' "$log" | cut -c1-80 || true)"
}

# verdict EXPECT OUTCOME prints OK | INCOMPLETE | MISMATCH.
verdict() {
  local want=$1 got=$2
  case $got in
    incomplete) echo INCOMPLETE ;;
    pass) [ "$want" = pass ] && echo OK || echo MISMATCH ;;
    violates:*) [ "$want" = "violates ${got#violates:}" ] && echo OK || echo MISMATCH ;;
    *) echo MISMATCH ;;
  esac
}

run_sany() {
  local jar=$1 f out rc=0
  for f in "$TLA_DIR"/*.tla; do
    out=$(cd "$TLA_DIR" && java -cp "$jar" tla2sany.SANY "$(basename "$f")" 2>&1) || true
    if echo "$out" | grep -q -E 'Fatal errors|Semantic errors|Parse Error|\*\*\* Errors'; then
      echo "SANY FAIL $f" >&2; echo "$out" | grep -v '^Picked up' >&2; rc=1
    else
      echo "SANY ok   $(basename "$f")"
    fi
  done
  return $rc
}

# run_config JAR NAME runs TLC on one configuration and writes its log; the verdict is printed.
run_config() {
  local jar=$1 name=$2 spec=${2%%_*} log want got v start end timed_out=0 rc=0 work
  log=$RESULTS_DIR/$name.log
  want=$(expect_of "$name") || die "configuration $name is not listed in $EXPECT_FILE"
  [ -f "$TLA_DIR/$name.cfg" ] || die "missing $TLA_DIR/$name.cfg"
  if [ "${FORMAL_SKIP_EXISTING:-0}" = 1 ] && grep -q '^@@ engram-formal' "$log" 2>/dev/null; then
    echo "skip      $name (log exists)"; return 0
  fi
  work=$WORK_DIR/$name
  # TLC 2.18 extracts the standard modules (Naturals.tla, TLC.tla, ...) into java.io.tmpdir; processes running in
  # parallel must not share it
  rm -rf "$work"; mkdir -p "$work/tmp" "$RESULTS_DIR"
  start=$(date +%s)
  (cd "$TLA_DIR" && timeout --signal=TERM --kill-after=30 "$CAP" \
    java -XX:+UseParallelGC "-Xmx$HEAP" "-Djava.io.tmpdir=$work/tmp" -cp "$jar" tlc2.TLC \
    -workers "${CFG_WORKERS:-$WORKERS}" -metadir "$work" -config "$name.cfg" "$spec.tla") >"$log" 2>&1 || rc=$?
  end=$(date +%s)
  rm -rf "$work"
  # A timeout is the cap reached: a TLC killed earlier (the OOM killer, a signal) is an error, not INCOMPLETE.
  if { [ "$rc" = 124 ] || [ "$rc" = 137 ] || [ "$rc" = 143 ]; } && [ $((end - start)) -ge "$CAP" ]; then timed_out=1; fi
  got=$(classify "$log" "$timed_out")
  v=$(verdict "$want" "$got")
  echo "@@ engram-formal config=$name expect=\"$want\" outcome=$got verdict=$v wall_seconds=$((end - start))" \
    "workers=${CFG_WORKERS:-$WORKERS} heap=$HEAP cap_seconds=$CAP jar=$JAR_SHA256" >>"$log"
  printf '%-10s %-44s expect=%-34s outcome=%-34s %ss\n' "$v" "$name" "$want" "$got" "$((end - start))"
  [ "$v" != MISMATCH ]
}

# ---- RESULTS.md ------------------------------------------------------------------------------------------------

# last_count LOG FIELD prints the generated (1) or distinct (2) counter of the last summary or progress line.
last_count() {
  local log=$1 field=$2 line
  line=$(grep -E '^[0-9,]+ states generated, [0-9,]+ distinct states found' "$log" | tail -1 || true)
  if [ -n "$line" ]; then
    echo "$line" | sed -E "s/^([0-9,]+) states generated, ([0-9,]+) distinct states found.*/\\$field/"
    return
  fi
  line=$(grep -E '^Progress\(' "$log" | tail -1 || true)
  [ -n "$line" ] || { echo "-"; return; }
  if [ "$field" = 1 ]; then
    echo "$line" | sed -E 's/^Progress\([0-9]+\) at [0-9-]+ [0-9:]+: ([0-9,]+) states generated.*/\1/'
  else
    echo "$line" | sed -E 's/.* ([0-9,]+) distinct states found.*/\1/'
  fi
}

commas() { echo "$1" | sed -E ':a;s/([0-9])([0-9]{3})($|,)/\1,\2\3/;ta'; }

depth_of() {
  local log=$1 d
  d=$(sed -n 's/^The depth of the complete state graph search is \([0-9]*\)\./\1/p' "$log" | tail -1)
  if [ -z "$d" ]; then d=$(grep -E '^Progress\(' "$log" | tail -1 | sed -E 's/^Progress\(([0-9]+)\).*/\1/'); fi
  echo "${d:--}"
}

time_of() {
  local log=$1 t
  t=$(sed -n 's/^Finished in \(.*\) at (.*/\1/p' "$log" | tail -1)
  if [ -z "$t" ]; then t=$(sed -n 's/.* wall_seconds=\([0-9]*\) .*/\1s wall/p' "$log" | tail -1); fi
  echo "${t:--}"
}

# jar_census describes which jar wrote the logs: those whose trailer carries jar=<pin> and the earlier ones.
jar_census() {
  local c n=0 old=0 log
  for c in $(all_configs); do
    log=$RESULTS_DIR/$c.log
    grep -q '^@@ engram-formal' "$log" 2>/dev/null || continue
    if grep -q "jar=$JAR_SHA256" "$log"; then n=$((n + 1)); else old=$((old + 1)); fi
  done
  echo "$n logs were written with that jar."
  if [ "$old" -gt 0 ]; then
    echo "$old logs were written before the pin moved from the GitHub release asset tla2tools.jar (a master build of" \
      "2026-10-06, sha256 7beec0f0...9ea78d, CONFLICTS.md #26) to Maven Central; the design counts are identical."
  fi
}

write_results() {
  local out=$RESULTS_DIR/RESULTS.md c log want got v row_res row_exp gen dist
  mkdir -p "$RESULTS_DIR"
  {
    echo "# TLC results"
    echo
    {
      printf '%s ' "Generated by \`scripts/formal-run.sh\` from the logs in this directory; do not edit by hand."
      printf '%s ' "The jar is \`de.hhu.stups:tlatools:$JAR_VERSION\` from Maven Central, TLC 2.18, sha256"
      printf '%s ' "\`$JAR_SHA256\`, one configuration at a time."
      printf '%s ' "$(jar_census | tr '\n' ' ')"
      printf '%s ' "\`expected\` is what \`../EXPECT\` states: \`pass\` (\"No error has been found\") or the one"
      printf '%s ' "invariant that must be violated. \`generated\`, \`distinct\` and \`depth\` are TLC's own counters"
      printf '%s ' "(for a violation, the work done before the counterexample was found; for a timeout, the last"
      printf '%s ' "progress line); \`time\` is TLC's \`Finished in\`. A configuration without a log reads \`not run\`."
      printf '%s ' "INCOMPLETE (the cap was reached) is neither pass nor fail. \`FAIL (expected)\` is a must-fail"
      printf '%s' "configuration that violated exactly its named invariant."
    } | fold -s -w 120 | sed 's/ *$//'
    echo
    echo "| config | expected | result | generated | distinct | depth | time |"
    echo "|---|---|---|---|---|---|---|"
    for c in $(all_configs); do
      log=$RESULTS_DIR/$c.log
      want=$(expect_of "$c")
      row_exp=$([ "$want" = pass ] && echo pass || echo "${want#violates }")
      if ! grep -q '^@@ engram-formal' "$log" 2>/dev/null; then
        echo "| $c | $row_exp | not run | - | - | - | - |"; continue
      fi
      got=$(sed -n 's/^@@ engram-formal .* outcome=\([^ ]*\) verdict=.*/\1/p' "$log" | tail -1)
      v=$(sed -n 's/^@@ engram-formal .* verdict=\([A-Z]*\) .*/\1/p' "$log" | tail -1)
      case $v in
        OK) row_res=$([ "$want" = pass ] && echo PASS || echo "FAIL (expected)") ;;
        INCOMPLETE) row_res=INCOMPLETE ;;
        *) row_res="MISMATCH: $got" ;;
      esac
      gen=$(commas "$(last_count "$log" 1)"); dist=$(commas "$(last_count "$log" 2)")
      echo "| $c | $row_exp | $row_res | $gen | $dist | $(depth_of "$log") | $(time_of "$log") |"
    done
  } >"$out"
}

# ---- main ------------------------------------------------------------------------------------------------------

mode=${1:-}
[ -n "$mode" ] || die "usage: formal-run.sh quick|full|sany|report [CONFIG...]"
shift || true

if [ "$mode" = report ]; then write_results; echo "wrote $RESULTS_DIR/RESULTS.md"; exit 0; fi

command -v java >/dev/null 2>&1 || die "java not found"
JAR=$(jar_path)
echo "formal-run: $JAR (sha256 verified), workers=$WORKERS heap=$HEAP cap=${CAP}s"

case $mode in
  sany) run_sany "$JAR"; exit $? ;;
  quick|full|changed) ;;
  *) die "unknown mode $mode" ;;
esac

if [ -n "${FORMAL_WORK_DIR:-}" ]; then WORK_DIR=$FORMAL_WORK_DIR; mkdir -p "$WORK_DIR"; else WORK_DIR=$(mktemp -d); fi
trap '[ -n "${FORMAL_WORK_DIR:-}" ] || rm -rf "$WORK_DIR"' EXIT

# changed_configs BASE prints the nightly-tier configurations of the specifications a change touches: a file of the
# specification (its .tla, one of its configurations) or a Go test file mapped to it in the manifest (PLAN.md 7.6: "run
# nightly and on PRs that touch their spec or its mapped Go files").
changed_configs() {
  local base=$1 files spec pats f p hit c
  files=$(git diff --name-only "$base" -- && git ls-files --others --exclude-standard)
  for spec in $(all_configs | cut -d_ -f1 | sort -u); do
    [ -z "${FORMAL_ONLY_SPEC:-}" ] || [ "$spec" = "$FORMAL_ONLY_SPEC" ] || continue
    pats=$(awk -F'|' -v f="formal/tla/$spec.tla" '$2 == f { print $3 }' "$MANIFEST" | tr '\n' ' ')
    hit=0
    for f in $files; do
      case $f in "formal/tla/$spec.tla" | "formal/tla/$spec"_*.cfg) hit=1 ;; esac
      for p in $pats; do
        # shellcheck disable=SC2053
        [[ $f == $p ]] && hit=1
      done
    done
    [ "$hit" = 1 ] || continue
    for c in $(nightly_only); do
      case $c in "$spec" | "$spec"_*) echo "$c" ;; esac
    done
  done
}

configs=""
if [ "$mode" = changed ]; then
  configs=$(changed_configs "${1:-${FORMAL_BASE:-origin/main}}")
  echo "formal-run: nightly-tier configurations of the changed specifications: ${configs:-none}"
else
  for a in "$@"; do configs="$configs ${a%.cfg}"; done
  if [ -z "$configs" ]; then
    if [ "$mode" = quick ]; then configs=$(quick_configs); else configs=$(all_configs); fi
  fi
fi

if [ "${FORMAL_DRY_RUN:-0}" = 1 ]; then echo "formal-run: dry run, would run:" $configs; exit 0; fi

# last_wall NAME prints the wall seconds of the last logged run of a configuration (empty when it has none).
last_wall() {
  sed -n 's/^@@ engram-formal .* wall_seconds=\([0-9]*\) .*/\1/p' "$RESULTS_DIR/$1.log" 2>/dev/null | tail -1
}

# The quick tier runs its fast configurations (last logged wall time <= FORMAL_FAST_SECONDS, default 4: JVM start-up
# dominates them) as FORMAL_JOBS (default 4) parallel one-worker TLC processes. A breadth-first search with one worker
# is deterministic, every configuration still has its own log and is compared with EXPECT on its own; the others run
# one at a time with all workers. The nightly full run is always sequential.
run_parallel() {
  local c running=0
  mkdir -p "$WORK_DIR/rc"
  for c in "$@"; do
    (
      if CFG_WORKERS=1 run_config "$JAR" "$c"; then echo 0 >"$WORK_DIR/rc/$c"; else echo 1 >"$WORK_DIR/rc/$c"; fi
    ) &
    running=$((running + 1))
    if [ "$running" -ge "${FORMAL_JOBS:-4}" ]; then wait -n; running=$((running - 1)); fi
  done
  wait
  for c in "$@"; do [ "$(cat "$WORK_DIR/rc/$c" 2>/dev/null || echo 1)" = 0 ] || return 1; done
}

status=0
t0=$(date +%s)
if [ "$mode" = quick ]; then run_sany "$JAR" || status=1; fi
fast=""
slow=""
for c in $configs; do
  w=$(last_wall "$c")
  if [ "$mode" = quick ] && [ -n "$w" ] && [ "$w" -le "${FORMAL_FAST_SECONDS:-4}" ]; then
    fast="$fast $c"
  else
    slow="$slow $c"
  fi
done
if [ -n "$fast" ]; then run_parallel $fast || status=1; fi
for c in $slow; do
  run_config "$JAR" "$c" || status=1
done
incomplete=0
for c in $configs; do
  if grep -q 'verdict=INCOMPLETE' "$RESULTS_DIR/$c.log" 2>/dev/null; then incomplete=$((incomplete + 1)); fi
done
write_results
echo "formal-run: $mode finished in $(($(date +%s) - t0))s; mismatches: $([ $status = 0 ] && echo none || echo YES);" \
  "incomplete: $incomplete"
if [ "$incomplete" -gt 0 ] && [ "${FORMAL_STRICT:-0}" = 1 ]; then status=3; fi
exit $status
