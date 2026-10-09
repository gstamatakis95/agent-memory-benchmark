#!/usr/bin/env bash
# Trace validation (PLAN.md section 7.4(b), M0.7): replays a JSON-lines action log through the TraceNext module that
# `engramctl formal trace-to-tla` generates for its specification, with TLC.
#
#   scripts/formal-trace-check.sh run SPEC CONFIG LOG.jsonl [violates INVARIANT | accepted]
#       converts LOG.jsonl into a TraceNext module of SPEC (constants and invariants of CONFIG, e.g. Outbox_NoWatch)
#       in a temporary directory (FORMAL_TRACE_OUT=dir keeps it there) and checks it with TLC (one worker). Default
#       expectation: accepted, i.e. the log is a behaviour of the specification and every invariant of CONFIG held along
#       it (TLC ends with the generated invariant TraceNotDone violated: some execution consumed the whole log).
#   scripts/formal-trace-check.sh proof [--regenerate]
#       the converter proof of the milestone. For one must-fail configuration of each of the six specifications, the
#       fixtures formal/tla/trace/<SPEC>.trace.jsonl (action-level) and <SPEC>.states.jsonl (state-level), TLC error
#       traces converted to JSON lines, are converted to TLA+ and replayed: TLC must report the invariant EXPECT names
#       violated after events+1 states, and the trace without its last event must be accepted with every invariant of
#       the configuration holding on the way. The
#       committed modules are not touched. With --regenerate the state-level fixtures are rewritten first from the
#       counterexample in formal/tla/results/<CONFIG>.log (a configuration with SYMMETRY is re-run without it, one
#       worker, so the trace is deterministic); commit the result with the logs it came from.
#   scripts/formal-trace-check.sh proof-all [CONFIG...]
#       the same replay for every (or the named) must-fail configuration that has a TLC log with an error trace, in a
#       temporary directory. A configuration with SYMMETRY is first re-run without it (see proof_one).
#
# Environment: TLA2TOOLS_JAR (see formal-jar.sh), FORMAL_HEAP (default 2g).
set -euo pipefail
export LC_ALL=C.UTF-8
cd "$(git rev-parse --show-toplevel)"

TLA_DIR=$PWD/formal/tla
TRACE_DIR=$TLA_DIR/trace
RESULTS_DIR=$TLA_DIR/results
HEAP=${FORMAL_HEAP:-2g}
PROOF="Outbox:Outbox_NoWatch Consolidation:Consolidation_NonAtomicKey Storage:Storage_AutoRepair \
Derivation:Derivation_RestoreByVisibleTwin Durability:Durability_AckBeforeIntent ShardMove:ShardMove_ReadyBeforeIndex"

die() { echo "formal-trace-check: $*" >&2; exit 2; }

# shellcheck source=formal-jar.sh
. "$(dirname "${BASH_SOURCE[0]}")/formal-jar.sh"

build_ctl() {
  mkdir -p "$WORK"
  go build -o "$WORK/engramctl" ./cmd/engramctl
}

# check_trace SPEC CONFIG LOG OUTDIR EXPECT [INVARIANT] runs TLC on the generated module; prints "ok ..." or "FAIL ...".
check_trace() {
  local spec=$1 cfg=$2 log=$3 out=$4 want=$5 inv=${6:-} tlc rc=0 states
  "$WORK/engramctl" formal trace-to-tla --tla-dir "$TLA_DIR" --cfg "$TLA_DIR/$cfg.cfg" --out "$out" "$spec" "$log"
  tlc=$WORK/$spec.$$.tlc
  (cd "$out" && java -XX:+UseParallelGC "-Xmx$HEAP" "-DTLA-Library=$TLA_DIR" -cp "$JAR" tlc2.TLC -workers 1 \
    -metadir "$WORK/states" -config "${spec}Trace.cfg" "${spec}Trace.tla") >"$tlc" 2>&1 || rc=$?
  rm -rf "$WORK/states"
  states=$(grep -c '^State [0-9]*:' "$tlc" || true)
  if [ "$want" = violates ]; then
    if grep -q "^Error: Invariant $inv is violated\." "$tlc"; then
      echo "ok   $cfg: replay violates $inv after $states states"; echo "$states" >"$WORK/last-states"; return 0
    fi
    echo "FAIL $cfg: expected $inv violated; $(grep -m1 -E '^(Error|Fatal)|No error' "$tlc" || echo 'no verdict')"
    return 1
  fi
  if grep -q '^Error: Invariant TraceNotDone is violated\.' "$tlc"; then
    echo "ok   $cfg: replay accepted after $states states, every invariant held"; echo "$states" >"$WORK/last-states"
    return 0
  fi
  echo "FAIL $cfg: expected the replay accepted; $(grep -m1 -E '^(Error|Fatal)|No error' "$tlc" || echo 'no verdict')"
  return 1
}

# replay_log SPEC CONFIG JSONL OUTDIR replays a log: the invariant EXPECT names is violated after events+1 states,
# and the log without its last event is accepted with every invariant of the configuration holding on the way.
replay_log() {
  local spec=$1 cfg=$2 jsonl=$3 out=$4 events inv rc=0
  inv=$(awk -F'\t' -v n="$cfg.cfg" '$1 == n { sub(/^violates /, "", $2); print $2 }' "$TLA_DIR/EXPECT")
  mkdir -p "$out"
  events=$(wc -l <"$jsonl")
  check_trace "$spec" "$cfg" "$jsonl" "$out" violates "$inv" || rc=1
  if [ "$rc" = 0 ] && [ "$(cat "$WORK/last-states")" != "$((events + 1))" ]; then
    echo "FAIL $cfg: $events events but the replay has $(cat "$WORK/last-states") states (want $((events + 1)))"; rc=1
  fi
  if [ "$rc" = 0 ] && [ "$events" -gt 1 ]; then
    head -n $((events - 1)) "$jsonl" >"$WORK/$cfg.prefix.jsonl"
    mkdir -p "$WORK/$cfg.prefix"
    check_trace "$spec" "$cfg" "$WORK/$cfg.prefix.jsonl" "$WORK/$cfg.prefix" accepted || rc=1
    if [ "$rc" = 0 ] && [ "$(cat "$WORK/last-states")" != "$((events + 1))" ]; then
      echo "FAIL $cfg: the prefix has $((events - 1)) events but the replay has $(cat "$WORK/last-states") states"; rc=1
    fi
  fi
  return $rc
}

# tlc_trace_log CONFIG prints the path of a TLC log with an error trace of CONFIG that replays literally: the committed
# log, or for a configuration with SYMMETRY a fresh one without it (a counterexample found under SYMMETRY is a behaviour
# modulo symmetry: the names of interchangeable constants may differ between its steps). One worker makes the
# breadth-first search, and so the trace, deterministic.
tlc_trace_log() {
  local cfg=$1 log=$RESULTS_DIR/$1.log
  grep -q '^State 2: <' "$log" 2>/dev/null || return 1
  if grep -q '^SYMMETRY' "$TLA_DIR/$cfg.cfg"; then
    log=$WORK/$cfg.nosym.log
    grep -v '^SYMMETRY' "$TLA_DIR/$cfg.cfg" >"$WORK/$cfg.cfg"
    (cd "$TLA_DIR" && java -XX:+UseParallelGC -Xmx6g "-Djava.io.tmpdir=$WORK" -cp "$JAR" tlc2.TLC -workers 1 \
      -metadir "$WORK/nosym-states-$cfg" -config "$WORK/$cfg.cfg" "${cfg%%_*}.tla") >"$log" 2>&1 || true
    rm -rf "$WORK/nosym-states-$cfg"
  fi
  echo "$log"
}

# proof_one SPEC CONFIG MODE. MODE: replay (the committed fixtures <SPEC>.trace.jsonl, an action-level log whose steps
# carry their parameters, and <SPEC>.states.jsonl, a state-level log from a TLC 2.18 trace; the committed modules are
# not touched), regenerate (rewrite <SPEC>.states.jsonl from the log of CONFIG, then replay it) or log (convert the log
# of CONFIG in a temporary directory and replay it, commit nothing). The action-level fixture comes from a TLC build
# whose trace headers print the parameters; TLC 2.18 does not, so only the state-level one can be regenerated.
proof_one() {
  local spec=$1 cfg=$2 mode=$3 rc=0 tlclog jsonl
  if [ "$mode" = replay ]; then
    for jsonl in "$TRACE_DIR/$spec.trace.jsonl" "$TRACE_DIR/$spec.states.jsonl"; do
      [ -f "$jsonl" ] || { echo "FAIL $cfg: no fixture $jsonl"; rc=1; continue; }
      replay_log "$spec" "$cfg" "$jsonl" "$WORK/$cfg.$(basename "$jsonl")" || rc=1
    done
    return $rc
  fi
  [ -f "$RESULTS_DIR/$cfg.log" ] || { echo "skip $cfg: no log"; return 0; }
  tlclog=$(tlc_trace_log "$cfg") || { echo "skip $cfg: no error trace in its log"; return 0; }
  jsonl=$WORK/$cfg.jsonl
  "$WORK/engramctl" formal tlc-to-trace -o "$jsonl" "$spec" "$tlclog"
  if [ "$mode" = regenerate ]; then cp "$jsonl" "$TRACE_DIR/$spec.states.jsonl"; fi
  replay_log "$spec" "$cfg" "$jsonl" "$WORK/$cfg"
}

mode=${1:-}
JAR=$(jar_path)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
build_ctl
case $mode in
  run)
    [ $# -ge 4 ] || die "usage: formal-trace-check.sh run SPEC CONFIG LOG.jsonl [violates INVARIANT | accepted]"
    spec=$2 cfg=$3 log=$4
    outdir=${FORMAL_TRACE_OUT:-$WORK/run}
    mkdir -p "$outdir"
    if [ "${5:-accepted}" = violates ]; then
      check_trace "$spec" "$cfg" "$log" "$outdir" violates "${6:?violates needs an invariant}"
    else
      check_trace "$spec" "$cfg" "$log" "$outdir" accepted
    fi ;;
  proof)
    status=0
    how=replay
    if [ "${2:-}" = --regenerate ]; then how=regenerate; fi
    for p in $PROOF; do proof_one "${p%%:*}" "${p##*:}" "$how" || status=1; done
    exit $status ;;
  proof-all)
    status=0
    configs=${*:2}
    if [ -z "$configs" ]; then
      configs=$(awk -F'\t' '!/^#/ && $2 ~ /^violates/ { sub(/\.cfg$/, "", $1); print $1 }' "$TLA_DIR/EXPECT")
    fi
    for cfg in $configs; do
      proof_one "${cfg%%_*}" "$cfg" log || status=1
    done
    exit $status ;;
  *) die "usage: formal-trace-check.sh run|proof [--regenerate]|proof-all" ;;
esac
