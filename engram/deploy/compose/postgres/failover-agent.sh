#!/bin/sh
# catalog-failover-agent (dev): polls the primary every 2 s and ALERTS after FAILOVER_AFTER seconds without an answer
# (CatalogPrimaryDown in the logs and in /tmp/agent-alert). It never promotes. PLAN.md section 9.3 and N163 require a
# promotion to fence the old primary first (stop it, detach its volume) and to run `engramctl catalog reconcile
# --from-shards` before the alias flips; a promote on silence alone makes two writable primaries as soon as the silent
# one was only paused or partitioned (review M2: a 18 s `docker pause` did exactly that). M1.7 implements fence,
# reconcile, promotion and the standby rebuild; until then FAILOVER_PROMOTE=1 is refused at start-up.
set -eu
if [ "${FAILOVER_PROMOTE:-0}" != 0 ]; then
  echo "failover-agent: FAILOVER_PROMOTE is refused: promotion needs fencing and the catalog reconcile (M1.7)" >&2
  exit 2
fi
last_ok="$(date +%s)"
alerted=0
log() { echo "failover-agent: $*"; }
while :; do
  now="$(date +%s)"
  if pg_isready -q -h 127.0.0.1 -p "$PRIMARY_PORT" -t 2; then
    if [ "$alerted" = 1 ]; then log "primary on :$PRIMARY_PORT answers again"; fi
    last_ok="$now"
    alerted=0
    rm -f /tmp/agent-alert
  fi
  echo "$now" > /tmp/agent-heartbeat
  if [ "$alerted" = 0 ] && [ $((now - last_ok)) -ge "$FAILOVER_AFTER" ]; then
    log "ALERT CatalogPrimaryDown: :$PRIMARY_PORT silent for $((now - last_ok)) s; not promoting (no fence/reconcile)"
    echo "$now" > /tmp/agent-alert
    alerted=1
  fi
  sleep 2
done
