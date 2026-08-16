#!/usr/bin/env bash
# Runs inside the postgres container on FIRST boot (docker-entrypoint-initdb.d,
# mounted by docker-compose.yml). temporalio/auto-setup needs its own
# databases alongside the application's `app` database.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE temporal OWNER app;
    CREATE DATABASE temporal_visibility OWNER app;
EOSQL

echo "init-temporal-dbs: created temporal + temporal_visibility"
