#!/bin/sh
# Turns the settings file generated from gucs.yaml (deploy/postgres/gucsgen) into `postgres -c name=value` arguments and
# hands over to the image entrypoint. PLAN.md section 9.1 writes `postgres -c include_dir=...`, but include_dir is a
# postgresql.conf directive and not a parameter (`postgres -c include_dir=...` fails with "unrecognized configuration
# parameter"), so the list of -c arguments is built from the generated file instead; the table stays the only source.
set -eu
conf="${ENGRAM_GUC_FILE:-/etc/postgresql/engram.conf}"
# order of precedence, lowest first: the table, then the arguments the service passes (its `command:` in compose), so
# a service can override one setting (temporal-postgres raises max_connections) without editing the shared table
set -- "$@" --end-of-service-args
while IFS= read -r line; do
  case "$line" in "" | "#"*) continue ;; esac
  name="${line%% = *}"
  val="${line#* = }"
  val="${val#\'}"
  val="${val%\'}"
  set -- "$@" -c "${name}=${val}"
done < "$conf"
while [ "$1" != --end-of-service-args ]; do
  first="$1"
  shift
  set -- "$@" "$first"
done
shift
set -- postgres "$@"
# The first boot runs initdb and the entrypoint's temporary server, whose shutdown waits for the archiver, and the
# archiver cannot succeed before `pgbackrest stanza-create` has run against the running server. So the first boot runs
# with archive_mode=off; deploy/compose/pgbackrest-bootstrap.sh creates the stanza, restarts the server (now with the
# table's archive_mode=on) and checks archiving. A later boot (PG_VERSION exists) uses the table unchanged.
if [ ! -s "${PGDATA:-/var/lib/postgresql/data}/PG_VERSION" ]; then
  set -- "$@" -c archive_mode=off
fi
exec docker-entrypoint.sh "$@"
