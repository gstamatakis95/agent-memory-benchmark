#!/usr/bin/env bash
# Local prerequisites check: run before ./run.sh or ./scripts/run-snapshot.sh.
# Exits non-zero if something required is missing; prints WARN for optional
# pieces (the real embedder) that only Tier-4 real-embedding runs need.
set -uo pipefail

fail=0
ok()   { printf '  ok    %s\n' "$*"; }
warn() { printf '  WARN  %s\n' "$*"; }
bad()  { printf '  FAIL  %s\n' "$*"; fail=1; }

echo "==> tools"
if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then ok "docker daemon reachable ($(docker version --format '{{.Server.Version}}' 2>/dev/null))"
  else bad "docker is installed but the daemon is not reachable (start Docker Desktop / dockerd)"; fi
else bad "docker not found (https://docs.docker.com/get-docker/)"; fi
if docker compose version >/dev/null 2>&1; then ok "docker compose $(docker compose version --short 2>/dev/null)"
else bad "docker compose plugin not found"; fi
if command -v go >/dev/null 2>&1; then
  v="$(go version | awk '{print $3}')"
  case "$v" in go1.2[5-9]*|go1.[3-9][0-9]*) ok "$v (only needed for make test / editing; images build Go inside Docker)";;
  *) warn "$v found; go.mod wants go1.25+ for local tests (Docker builds are unaffected)";; esac
else warn "go not found: make test/unit tests unavailable; ./run.sh still works (builds inside Docker)"; fi
command -v protoc >/dev/null 2>&1 && ok "protoc (only for make proto)" || warn "protoc not found (only needed for make proto)"

echo "==> ports (compose publishes these on localhost)"
for p in 5432:postgres 9000:minio 9001:minio-console 7233:temporal 8080:temporal-ui 8081:server 9100:embedder-mock 9101:embedder-nomic; do
  port="${p%%:*}"; name="${p#*:}"
  if (command -v lsof >/dev/null 2>&1 && lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1) \
     || (command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$port\$"); then
    warn "port $port ($name) is already in use; compose will fail to publish it"
  else ok "port $port free ($name)"; fi
done

echo "==> disk / memory"
avail_kb=$(df -Pk . | awk 'NR==2{print $4}')
if [[ "${avail_kb:-0}" -lt 5000000 ]]; then warn "less than 5 GB free here; images + datasets + snapshots need a few GB"; else ok "$((avail_kb/1024/1024)) GB free"; fi

echo "==> real embedder (optional; Tier 4 only)"
if curl -fsS --max-time 2 http://localhost:12434/engines/v1/models >/dev/null 2>&1; then
  ok "Docker Model Runner answering on :12434 (EMBEDDER_ADDR=embedder-nomic:9100 usable)"
else
  warn "Docker Model Runner not reachable on :12434; the mock embedder (default) still works. See README 'Real datasets'."
fi

echo "==> datasets"
[[ -f testdata/fixtures.json ]] && ok "testdata/fixtures.json (built in)" || bad "testdata/fixtures.json missing"
for f in datasets/*.json; do [[ -e "$f" ]] && ok "custom/real dataset: $f"; done 2>/dev/null || true

if [[ $fail -ne 0 ]]; then echo; echo "preflight: FAILED (fix the FAIL lines above)"; exit 1; fi
echo; echo "preflight: OK — next: ./scripts/run-snapshot.sh --fixtures   (or ./run.sh --fixtures)"
