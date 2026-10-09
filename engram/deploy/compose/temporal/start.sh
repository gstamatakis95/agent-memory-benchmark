#!/bin/sh
# Renders the Temporal static config (the database password comes from /run/secrets, never from the environment or the
# image; the rendered file is readable by its owner only) and starts ONE service of the split deployment: SERVICE is
# frontend, history, matching or worker.
set -eu
umask 077
pw="$(cat /run/secrets/temporal_pg_pw)"
sed -e "s|@PG_PASSWORD@|${pw}|g" -e "s|@METRICS_PORT@|${METRICS_PORT}|g" -e "s|@MAX_CONNS@|${MAX_CONNS}|g" \
  -e "s|@VIS_CONNS@|${VIS_CONNS}|g" /etc/temporal/config.yaml.tmpl > /tmp/config.yaml
exec temporal-server --config-file /tmp/config.yaml --allow-no-auth start --service="${SERVICE}"
