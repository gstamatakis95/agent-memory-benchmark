#!/usr/bin/env bash
# Snapshot-serving counterpart of ../run.sh (docs/07-snapshot-serving.md).
#
# Steps 1-5 are identical to run.sh: bring up infra, run migrations, fetch
# the dataset, start the server (which embeds both the enrichment worker AND
# the snapshot build worker + serving runtime), ingest, and wait for 100%
# enrichment at --version. Steps 6-7 replace run.sh's single eval step with
# build-snapshot then eval --engine snapshot.
#
# IMPORTANT (see AGENTS.md "compose-run env trap"): that trap is about the
# EMBEDDER_ADDR/PG_DSN env a one-off `docker compose run` container does NOT
# inherit from the long-lived `server` service. It does NOT apply to the
# commands this script adds: build-snapshot, search, and eval --engine
# snapshot are just gRPC clients that ask the long-lived `server` container
# to do the work (build the snapshot, publish it to MinIO, load it, run the
# query) — none of it happens in the one-off container, so none of it needs
# extra env passed to `docker compose run`. Only a real-embedder run of the
# ORIGINAL in-process eval engine needs that -e EMBEDDER_ADDR=... flag.
set -euo pipefail

DATASET="fixtures"
RETRIEVAL="hybrid"
VERSION="${ENRICHMENT_VERSION:-1}"
KEEP_UP=false

usage() {
  cat <<EOF
Usage: ./scripts/run-snapshot.sh [options]
  --fixtures                 tiny built-in corpus, asserts R@5 == 1.0   (default)
  --dataset locomo           LoCoMo (10 conversations)
  --dataset longmemeval_s    LongMemEval-S (500 questions)
  --dataset datasets/x.json  your own corpus (docs/08-custom-datasets.md)
  --retrieval bm25|dense|hybrid   ablation mode (default: hybrid)
  --version N                enrichment version to target (default: 1)
  --keep-up                  leave the stack running after eval
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fixtures)  DATASET="fixtures"; shift ;;
    --dataset)   DATASET="$2"; shift 2 ;;
    --retrieval) RETRIEVAL="$2"; shift 2 ;;
    --version)   VERSION="$2"; shift 2 ;;
    --keep-up)   KEEP_UP=true; shift ;;
    *) usage ;;
  esac
done

cleanup() { $KEEP_UP || docker compose down -v; }
trap cleanup EXIT

echo "==> [1/7] starting infrastructure"
docker compose up -d postgres minio mc-bootstrap temporal temporal-ui embedder-mock embedder-nomic
docker compose up -d --wait postgres temporal minio

echo "==> [2/7] running migrations"
docker compose run --rm server /app/migrate up

echo "==> [3/7] fetching dataset: $DATASET"
if [[ "$DATASET" == "fixtures" ]]; then
  echo "    using built-in fixtures (testdata/fixtures.json)"
elif [[ "$DATASET" == *.json || "$DATASET" == */* ]]; then
  # Custom dataset (docs/08-custom-datasets.md): must live under ./datasets so
  # the read-only /app/datasets mount makes it visible inside the container.
  if [[ ! -f "$DATASET" ]]; then
    echo "custom dataset file not found: $DATASET" >&2; exit 1
  fi
  case "$(cd "$(dirname "$DATASET")" && pwd)" in
    "$(pwd)/datasets"*) ;;
    *) echo "custom dataset must be under ./datasets/ (mounted into the container); got $DATASET" >&2; exit 1 ;;
  esac
  DATASET="datasets/$(basename "$DATASET")"
  echo "    using custom dataset $DATASET"
else
  ./scripts/download-dataset.sh "$DATASET"
fi

echo "==> [4/7] starting server + Temporal worker (schedule created on boot)"
docker compose up -d --wait server
echo "    Temporal UI: http://localhost:8080"
echo "    MinIO console: http://localhost:9001 (app/appsecret)"

echo "==> [5/7] ingesting + waiting for enrichment at version $VERSION"
docker compose run --rm server /app/client ingest \
  --dataset "$DATASET" --version "$VERSION"

# Trigger the schedule immediately rather than waiting up to 60s for the tick.
docker compose run --rm server /app/client trigger-sweep

# Poll the version-pinned progress until remaining == 0. Fail loudly on dead rows.
docker compose run --rm server /app/client wait-enriched \
  --version "$VERSION" --timeout "${WAIT_TIMEOUT:-15m}" --fail-on-dead

echo "==> [6/7] building + publishing snapshot at version $VERSION"
docker compose run --rm server /app/client build-snapshot --version "$VERSION"

echo "==> [7/7] running retrieval eval against the snapshot engine (mode=$RETRIEVAL)"
docker compose run --rm server /app/client eval \
  --dataset "$DATASET" --version "$VERSION" --retrieval "$RETRIEVAL" --engine snapshot
