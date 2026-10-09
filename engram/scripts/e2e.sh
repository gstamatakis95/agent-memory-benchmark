#!/usr/bin/env bash
# make e2e - the T4 smoke of the dev stack (PLAN.md section 8.1 T4, M0.4): cold start of deploy/compose, a health check
# of every service, the Temporal namespace and history-shard count, the payload codec against the live Temporal
# (TestIso_Temporal_PayloadsEncrypted: no sentinel in the persistence database), the shard schema through `engramctl
# migrate` and the pgBackRest stanzas of the shard, the catalog and temporal-postgres. One Retain/Recall round trip
# through Envoy follows when engram-api exists (M1.x); until then Envoy is proven by its listener and by the gRPC
# status it answers with no upstream.
#
# Environment: E2E_SKIP_SHARD=1 leaves out the ParadeDB shard (disk-starved sandboxes, see the M0.4 report), E2E_KEEP=1
# keeps the stack running afterwards, E2E_MIRROR (default mirror.gcr.io) is tried for docker.io images when the
# registry refuses the pull (rate limit); the same digest is pulled, so the pin holds.
set -euo pipefail
cd "$(dirname "$0")/.."
root="$PWD"
compose_dir=deploy/compose
dc() { docker compose -f "$root/$compose_dir/compose.yml" "$@"; }
log() { printf '[e2e %3ds] %s\n' "$((SECONDS))" "$*"; }
fail() { printf '[e2e %3ds] FAIL: %s\n' "$((SECONDS))" "$*" >&2; exit 1; }

# --- 0. what this machine can do ------------------------------------------------------------------------------------
for tool in docker curl go openssl; do
  command -v "$tool" > /dev/null || fail "$tool is required"
done
if ! docker compose version > /dev/null 2>&1; then
  cat >&2 << 'MSG'
e2e: `docker compose` (v2) is not available, so the dev stack cannot start here.
The compose files are correct for CI (Linux with host networking); this environment only runs the unit tiers.
This is the situation CONFLICTS.md #3 describes; see docs/briefs/reports/M0.4-report.md.
MSG
  exit 2
fi
if [ "$(uname -s)" != Linux ]; then
  echo "e2e: the stack uses network_mode: host, which is only real on Linux (Docker Desktop's host mode is a VM)." >&2
  exit 2
fi
if [ -n "$(docker ps -aq --filter label=com.docker.compose.project=engram-dev)" ] && [ "${E2E_REUSE:-0}" != 1 ]; then
  log "removing a previous engram-dev stack"
  dc down -v --remove-orphans > /dev/null 2>&1 || true
fi

# --- 1. images: pinned digests, with a mirror fallback for rate-limited registries ----------------------------------
# shellcheck disable=SC1091
. "$root/$compose_dir/pull-images.sh"
log "pulling the pinned images"
pull_images TEMPORAL_SERVER_IMAGE TEMPORAL_ADMIN_IMAGE ENVOY_IMAGE MINIO_IMAGE PG_PLAIN_BASE || fail "image pull"
if [ "${E2E_SKIP_SHARD:-0}" != 1 ]; then pull_images SHARD_PG_BASE || fail "image pull"; fi
profiles=core,catalog,edge
if [ "${E2E_SKIP_SHARD:-0}" != 1 ]; then profiles=$profiles,shard; fi
export COMPOSE_PROFILES="$profiles"

teardown() {
  local rc=$?
  if [ "${E2E_KEEP:-0}" != 1 ]; then
    log "tearing the stack down"
    dc down -v --remove-orphans > /dev/null 2>&1 || true
  fi
  exit $rc
}
trap teardown EXIT

# --- 2. cold start ---------------------------------------------------------------------------------------------------
log "rendering secrets and settings"
"$root/$compose_dir/render.sh"
export FAILOVER_AFTER=8 # the failover agent test below pauses the primary; production uses 30
log "starting MinIO and the Postgres servers"
first=(minio temporal-postgres catalog-primary)
if [ "${E2E_SKIP_SHARD:-0}" != 1 ]; then first+=(shard-1-postgres); fi
dc up -d --build --wait "${first[@]}" > /dev/null
"$root/$compose_dir/create-bucket.sh"
log "pgBackRest: stanzas for temporal, catalog and the shard"
"$root/$compose_dir/pgbackrest-bootstrap.sh" temporal-postgres temporal > /dev/null
"$root/$compose_dir/pgbackrest-bootstrap.sh" catalog-primary catalog > /dev/null
if [ "${E2E_SKIP_SHARD:-0}" != 1 ]; then
  "$root/$compose_dir/pgbackrest-bootstrap.sh" shard-1-postgres shard-1 > /dev/null
fi
log "starting the standby, the failover agent, Temporal and Envoy"
# one-shot jobs run with --rm and the services start with --no-deps: a stopped one-shot container costs a full image
# copy on storage drivers without layer sharing (the sandbox of the M0.4 report)
dc run --rm --no-deps -T temporal-schema > /dev/null 2>&1
second=(catalog-standby catalog-failover-agent temporal-history temporal-matching temporal-frontend temporal-worker
  envoy)
dc up -d --no-deps --wait "${second[@]}" > /dev/null
dc run --rm --no-deps -T temporal-namespace 2>&1 | grep -E "namespace engram|registered" || true

# --- 3. health of every service ---------------------------------------------------------------------------------------
log "health"
unhealthy="$(dc ps --format '{{.Name}} {{.State}} {{.Health}}' | grep -v 'temporal-schema\|temporal-namespace' |
  awk '$2 != "running" || ($3 != "healthy" && $3 != "")' || true)"
[ -z "$unhealthy" ] || fail "services not healthy: $unhealthy"
for svc in minio; do
  [ "$(dc ps -q "$svc" | wc -l)" = 1 ] || fail "$svc is not running"
done
curl -skf --noproxy '*' -o /dev/null https://127.0.0.1:19000/minio/health/live || fail "minio health"
for port in 18001 18002 18003 18004; do
  curl -sf --noproxy '*' -o /dev/null "http://127.0.0.1:$port/metrics" || fail "temporal metrics on :$port"
done
curl -sf --noproxy '*' http://127.0.0.1:18080/__envoy/ready | grep -q 'envoy listener up' || fail "envoy listener"
grpc_status="$(curl -s --noproxy '*' -D - -o /dev/null --http2-prior-knowledge -H 'content-type: application/grpc' \
  -X POST http://127.0.0.1:18080/memory.v1.MemoryService/Recall | tr -d '\r' | awk -F': ' '/^grpc-status/ {print $2}')"
[ "$grpc_status" = 14 ] || fail "envoy: grpc-status '$grpc_status' for a Recall without engram-api, want 14"
log "envoy answers UNAVAILABLE for gRPC until engram-api exists (expected before M1.x)"

# catalog: one asynchronous streaming standby, no synchronous standby (N163)
state="$(docker exec "$(dc ps -q catalog-primary)" psql -h 127.0.0.1 -p 15511 -U engram -d engram_catalog -Atc \
  "SELECT state || ',' || sync_state FROM pg_stat_replication")"
[ "$state" = streaming,async ] || fail "catalog standby replication is '$state', want streaming,async"
# the settings table reached every server (review m6, m7): the standby archives to the catalog stanza after a promotion
# and the primary caps the WAL its replication slot may pin
psql_in() { # service port user db sql
  docker exec "$(dc ps -q "$1")" psql -h 127.0.0.1 -p "$2" -U "$3" -d "$4" -Atc "$5"
}
[ "$(psql_in catalog-standby 15512 engram engram_catalog 'SHOW archive_mode')" = on ] || fail "standby archive_mode"
psql_in catalog-standby 15512 engram engram_catalog 'SHOW archive_command' | grep -q -- '--stanza=catalog' ||
  fail "standby archive_command"
[ "$(psql_in catalog-primary 15511 engram engram_catalog 'SHOW max_slot_wal_keep_size')" = 2GB ] || fail "slot WAL cap"
[ "$(psql_in catalog-standby 15512 engram engram_catalog 'SHOW tcp_keepalives_idle')" = 10 ] || fail "standby settings"
if [ "${E2E_SKIP_SHARD:-0}" != 1 ]; then
  [ "$(psql_in shard-1-postgres 15501 engram engram 'SHOW synchronous_commit')" = local ] || fail "shard commit mode"
  [ "$(psql_in shard-1-postgres 15501 engram engram 'SHOW autovacuum_freeze_max_age')" = 1000000000 ] || fail "XID age"
fi
[ "$(psql_in temporal-postgres 15521 temporal temporal 'SHOW max_connections')" = 400 ] || fail "temporal connections"
# pgBackRest has archived WAL and can take a backup on every stanza
for pair in "temporal-postgres temporal" "catalog-primary catalog"; do
  set -- $pair
  dc exec -T -u postgres "$1" pgbackrest --stanza="$2" check > /dev/null || fail "pgbackrest check $2"
done

# --- 4. the shard schema through engramctl --------------------------------------------------------------------------
if [ "${E2E_SKIP_SHARD:-0}" != 1 ]; then
  log "shard-1: engramctl migrate"
  go build -o "$root/bin/engramctl" ./cmd/engramctl
  shard_pw="$(cat "$root/$compose_dir/secrets/shard_1_pw")"
  shard_dsn="postgres://engram:${shard_pw}@127.0.0.1:15501/engram?sslmode=disable"
  "$root/bin/engramctl" migrate --dsn "$shard_dsn" > /dev/null
  [ -z "$("$root/bin/engramctl" migrate --dsn "$shard_dsn" --check-rls)" ] || fail "migrate --check-rls reports rows"
  dc exec -T -u postgres shard-1-postgres pgbackrest --stanza=shard-1 check > /dev/null ||
    fail "pgbackrest check shard-1"
fi

# --- 5. Temporal: namespace, history shards, payload codec -----------------------------------------------------------
log "Temporal and the payload codec"
export ENGRAM_TEST_TEMPORAL_ADDR=127.0.0.1:17233
export ENGRAM_TEST_CODEC_KEYS="$root/$compose_dir/secrets/temporal_codec"
temporal_pw="$(cat "$root/$compose_dir/secrets/temporal_pg_pw")"
export ENGRAM_TEST_TEMPORAL_PG_DSN="postgres://temporal:${temporal_pw}@127.0.0.1:15521/temporal?sslmode=disable"
gotest_log="$(mktemp)"
go test -tags integration -count=1 -run 'TestStack_Temporal|TestIso_Temporal_PayloadsEncrypted' ./e2e/ \
  > "$gotest_log" 2>&1 || { tail -40 "$gotest_log" >&2; fail "go test ./e2e"; }
rm -f "$gotest_log"

# --- 6. the failover agent alerts and never promotes (review M2) -------------------------------------------
log "failover agent: a paused primary raises an alert and the standby stays read-only"
primary="$(dc ps -q catalog-primary)"
agent="$(dc ps -q catalog-failover-agent)"
docker pause "$primary" > /dev/null
for _ in $(seq 1 40); do
  if docker logs "$agent" 2>&1 | grep -q 'ALERT CatalogPrimaryDown'; then break; fi
  sleep 1
done
if ! docker logs "$agent" 2>&1 | grep -q 'ALERT CatalogPrimaryDown'; then
  docker unpause "$primary"
  fail "no alert for a silent primary"
fi
[ "$(psql_in catalog-standby 15512 engram engram_catalog 'SELECT pg_is_in_recovery()')" = t ] ||
  { docker unpause "$primary"; fail "the standby left recovery while the primary was only paused"; }
docker unpause "$primary" > /dev/null
for _ in $(seq 1 30); do
  n="$(psql_in catalog-primary 15511 engram engram_catalog 'SELECT count(*) FROM pg_stat_replication' 2> /dev/null)"
  if [ "$n" = 1 ]; then break; fi
  sleep 1
done
[ "$(psql_in catalog-standby 15512 engram engram_catalog 'SELECT pg_is_in_recovery()')" = t ] || fail "standby promoted"
[ "$(psql_in catalog-primary 15511 engram engram_catalog 'SELECT pg_is_in_recovery()')" = f ] ||
  fail "primary in recovery"
if psql_in catalog-standby 15512 engram engram_catalog 'CREATE TABLE split_brain_probe (x int)' 2> /dev/null; then
  fail "the standby accepted a write"
fi
for _ in $(seq 1 20); do
  if docker logs "$agent" 2>&1 | grep -q 'answers again'; then break; fi
  sleep 1
done
docker logs "$agent" 2>&1 | grep -q 'answers again' || fail "the agent did not notice the primary coming back"

log "PASS in ${SECONDS}s"
[ "$SECONDS" -lt 300 ] || fail "the smoke took ${SECONDS}s, the budget is 300s from cold"
