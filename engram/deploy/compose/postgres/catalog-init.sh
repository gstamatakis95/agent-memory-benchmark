#!/bin/sh
# initdb hook of catalog-primary: the replication role of the asynchronous standby (N163) and its pg_hba entry.
set -eu
pw="$(cat "$REPLICATOR_PASSWORD_FILE")"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "CREATE ROLE replicator REPLICATION LOGIN PASSWORD '${pw}'"
echo "host replication replicator 127.0.0.1/32 scram-sha-256" >> "$PGDATA/pg_hba.conf"
