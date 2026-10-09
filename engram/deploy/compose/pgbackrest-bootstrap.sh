#!/usr/bin/env bash
# Once per Postgres service after its first start (PLAN.md section 9.5 step 3): create the pgBackRest stanza, restart
# the server so the table's archive_mode=on takes effect (the first boot runs with archiving off, see
# engram-postgres.sh), wait until it is healthy and prove that WAL archiving works with `pgbackrest check`.
#   usage: pgbackrest-bootstrap.sh <compose service> <stanza> [<bucket>]   (the bucket must exist: see create-bucket.sh)
set -euo pipefail
cd "$(dirname "$0")"
svc="$1"
stanza="$2"
dc() { docker compose -f compose.yml "$@"; }
if [ "${REPO1_TYPE:-s3}" = posix ]; then # the repository volume is created root-owned
  dc exec -T -u root "$svc" chown postgres:postgres "${REPO1_PATH:-/repo}"
fi
dc exec -T -u postgres "$svc" pgbackrest --stanza="$stanza" stanza-create
dc restart "$svc"
for _ in $(seq 1 60); do
  if [ "$(docker inspect -f '{{.State.Health.Status}}' "$(dc ps -q "$svc")")" = healthy ]; then break; fi
  sleep 1
done
dc exec -T -u postgres "$svc" pgbackrest --stanza="$stanza" check
