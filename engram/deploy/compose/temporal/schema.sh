#!/bin/sh
# One-shot: create the visibility database and apply the Temporal schemas to the cell's Postgres (idempotent: an
# already set-up database is left alone and update-schema only applies what is missing).
set -eu
# the password reaches the tool through its environment (SQL_PASSWORD), not through argv, which any process can read
SQL_PASSWORD="$(cat "$SQL_PASSWORD_FILE")"
export SQL_PASSWORD
dir=/etc/temporal/schema/postgresql/v12
tool() { temporal-sql-tool --plugin postgres12 --ep 127.0.0.1 -p "$SQL_PORT" -u "$SQL_USER" "$@"; }
tool --db temporal_visibility create-database || echo "temporal_visibility exists"
tool --db temporal setup-schema -v 0.0 || echo "temporal schema already set up"
tool --db temporal update-schema -d "$dir/temporal/versioned"
tool --db temporal_visibility setup-schema -v 0.0 || echo "visibility schema already set up"
tool --db temporal_visibility update-schema -d "$dir/visibility/versioned"
