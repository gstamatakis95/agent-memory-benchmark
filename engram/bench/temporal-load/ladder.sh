#!/usr/bin/env bash
# One clean measurement of the P-9 load test (PLAN.md section 9.1, M0.4): reset the Temporal stack, run a ladder of
# offered workflow-start rates against it and write the table to $OUT (default: stdout only).
#
#   bench/temporal-load/ladder.sh [-- extra flags for `temporal-load run`]
#   LADDER=2,4,6 STEP=60s SYNC_COMMIT=on|off FSYNC=on|off CODEC=true|false HISTORY_CPUS=2 OUT=table.md
#
# Every call starts from an empty Temporal database (a ladder step that overloads the cluster leaves a backlog that
# would poison the next measurement). The Postgres of the cell keeps its durable settings unless SYNC_COMMIT / FSYNC say
# otherwise; a run with them off is an upper bound for a rig with a slow disk, never the recorded result.
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"
dir=deploy/compose
export COMPOSE_PROFILES=core
export TEMPORAL_PG_SYNC_COMMIT="${SYNC_COMMIT:-on}" TEMPORAL_PG_FSYNC="${FSYNC:-on}"
export TEMPORAL_HISTORY_CPUS="${HISTORY_CPUS:-2}"
out="${OUT:-$(mktemp -u /tmp/temporal-load-XXXXXX.md)}"
export REPO1_TYPE=posix REPO1_PATH=/repo
# shellcheck disable=SC1091
. "$root/$dir/pull-images.sh"
pull_images TEMPORAL_SERVER_IMAGE TEMPORAL_ADMIN_IMAGE PG_PLAIN_BASE
dc() { docker compose -f "$root/$dir/compose.yml" "$@"; }
trap 'dc down -v --remove-orphans > /dev/null 2>&1 || true' EXIT
dc down -v --remove-orphans > /dev/null 2>&1 || true
"$root/$dir/render.sh"
(cd "$root" && go build -o bin/temporal-load ./bench/temporal-load)
dc up -d --build --wait temporal-postgres > /dev/null
# WAL archiving as in production: a first boot runs with archive_mode=off, so create the stanza and restart (review m14;
# the first recorded ladders ran with archiving off)
"$root/$dir/pgbackrest-bootstrap.sh" temporal-postgres temporal > /dev/null
dc run --rm --no-deps -T temporal-schema > /dev/null 2>&1
dc up -d --no-deps --wait temporal-history temporal-matching temporal-frontend temporal-worker > /dev/null
dc run --rm --no-deps -T temporal-namespace > /dev/null
echo "uptime: $(uptime)"
archiving="$(dc exec -T temporal-postgres psql -h 127.0.0.1 -p 15521 -U temporal -d temporal -Atc 'SHOW archive_mode')"
echo "wal archiving: ${archiving}, synchronous_commit=${TEMPORAL_PG_SYNC_COMMIT}, fsync=${TEMPORAL_PG_FSYNC}"
"$root/bin/temporal-load" run -ladder "${LADDER:-2,4,6}" -step "${STEP:-60s}" -codec="${CODEC:-true}" \
  -history-cpus "${HISTORY_CPUS:-2}" -out "$out.raw" "$@" 2> /dev/null | tee "$out"
echo "uptime: $(uptime)"
