#!/bin/sh
# catalog-standby: clone the primary with pg_basebackup on first start (the same step the failover agent re-runs to
# rebuild a standby after a promotion, N163), then run as an asynchronous streaming hot standby through the same
# wrapper, settings table (kind standby) and stanza configuration as the primary, so that a promoted standby archives
# to the catalog stanza and the replication slot's WAL is capped like the primary's.
set -eu
export PGDATA=/var/lib/postgresql/data
umask 077
pgpass=/var/lib/postgresql/pgpass
printf '127.0.0.1:%s:*:replicator:%s\n' "$PRIMARY_PORT" "$(cat "$REPLICATOR_PASSWORD_FILE")" > "$pgpass"
chown postgres:postgres "$pgpass"
export PGPASSFILE="$pgpass"
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA"
  chown postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  until gosu postgres pg_basebackup -h 127.0.0.1 -p "$PRIMARY_PORT" -U replicator -D "$PGDATA" -R -X stream \
    -C -S catalog_standby; do
    echo "catalog-standby: waiting for the primary"
    sleep 2
  done
fi
exec /usr/local/bin/engram-postgres -c "port=$PGPORT" -c listen_addresses=127.0.0.1
