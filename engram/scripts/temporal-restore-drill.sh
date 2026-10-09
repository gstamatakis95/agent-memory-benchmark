#!/usr/bin/env bash
# The temporal-postgres restore drill (PLAN.md sections 9.1 and 9.3, M0.4): run workflows through the payload codec,
# take a pgBackRest full backup, run more workflows (their only trace is archived WAL), destroy the database
# container and its volume, restore from the repository, restart Temporal and prove that every workflow's history is
# back with its payloads intact (drill-verify decodes the sentinel of each through the codec). Exits 0 only if all of
# them are.
#
#   REPO=s3|posix   s3 (default): the MinIO repository of the compose stack; posix: a local volume, for hosts that
#                   cannot run MinIO (the sandbox of the M0.4 report). KEEP=1 leaves the stack running afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."
root="$PWD"
dir=deploy/compose
repo="${REPO:-s3}"
if [ "$repo" = posix ]; then export REPO1_TYPE=posix REPO1_PATH=/repo; fi
dc() { docker compose -f "$root/$dir/compose.yml" "$@"; }
log() { printf '[drill %3ds] %s\n' "$SECONDS" "$*"; }
work="$(mktemp -d)"
cleanup() {
  local rc=$?
  rm -rf "$work"
  if [ "${KEEP:-0}" != 1 ]; then dc down -v --remove-orphans > /dev/null 2>&1 || true; fi
  exit $rc
}
trap cleanup EXIT

# shellcheck disable=SC1091
. "$root/$dir/pull-images.sh"
log "pulling images, building the driver and starting the stack (repository: $repo)"
export COMPOSE_PROFILES=core
pull_images TEMPORAL_SERVER_IMAGE TEMPORAL_ADMIN_IMAGE PG_PLAIN_BASE
if [ "$repo" = s3 ]; then pull_images MINIO_IMAGE; fi
"$root/$dir/render.sh"
go build -o "$root/bin/temporal-load" ./bench/temporal-load
services=(temporal-postgres)
if [ "$repo" = s3 ]; then services+=(minio); fi
dc up -d --build --wait "${services[@]}" > /dev/null
if [ "$repo" = s3 ]; then "$root/$dir/create-bucket.sh"; fi
"$root/$dir/pgbackrest-bootstrap.sh" temporal-postgres temporal > /dev/null
# one-shot jobs run with --rm and the services start with --no-deps: a stopped one-shot container costs a full image
# copy on storage drivers without layer sharing (the sandbox of the M0.4 report)
dc run --rm --no-deps -T temporal-schema > /dev/null 2>&1
dc up -d --no-deps --wait temporal-history temporal-matching temporal-frontend temporal-worker > /dev/null
dc run --rm --no-deps -T temporal-namespace > /dev/null

tl() { "$root/bin/temporal-load" "$@" -keys "$root/$dir/secrets/temporal_codec" -wrapped "$work/wrapped"; }
log "seeding 60 workflows, then a full backup"
tl drill-seed -n 60 -tag pre -out "$work/seed.json"
dc exec -T -u postgres temporal-postgres pgbackrest --stanza=temporal --type=full backup > /dev/null
log "seeding 40 more workflows after the backup (recoverable only from archived WAL)"
tl drill-seed -n 40 -tag post -out "$work/seed.json"
dc exec -T -u postgres temporal-postgres psql -h 127.0.0.1 -p 15521 -U temporal -d temporal -Atc \
  'SELECT pg_switch_wal()' > /dev/null
dc exec -T -u postgres temporal-postgres pgbackrest --stanza=temporal check > /dev/null # waits for the segment

log "destroying temporal-postgres and its volume"
dc stop temporal-history temporal-matching temporal-frontend temporal-worker temporal-postgres > /dev/null
dc rm -sfv temporal-history temporal-matching temporal-frontend temporal-worker temporal-postgres > /dev/null 2>&1
docker volume rm engram-dev_temporal-pg-data > /dev/null
if docker volume inspect engram-dev_temporal-pg-data > /dev/null 2>&1; then echo "volume still exists" >&2; exit 1; fi

log "restoring from the pgBackRest repository into a fresh volume"
dc run --rm --no-deps -T --entrypoint /bin/sh -u root temporal-postgres -c \
  'chown postgres:postgres /var/lib/postgresql/data && chmod 700 /var/lib/postgresql/data &&
   exec gosu postgres pgbackrest --stanza=temporal restore' > /dev/null
dc up -d --wait temporal-postgres > /dev/null
dc up -d --no-deps --wait temporal-history temporal-matching temporal-frontend temporal-worker > /dev/null
log "verifying every workflow history"
tl drill-verify -in "$work/seed.json"
log "PASS: the histories of 100 workflows are back after the restore"
