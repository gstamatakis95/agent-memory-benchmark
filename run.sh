#!/usr/bin/env bash
set -euo pipefail

DATASET="fixtures"
RETRIEVAL="hybrid"
VERSION="${ENRICHMENT_VERSION:-1}"
KEEP_UP=false

usage() {
  cat <<EOF
Usage: ./run.sh [options]
  --fixtures                 tiny built-in corpus, asserts R@5 == 1.0   (default)
  --dataset locomo           LoCoMo (10 conversations)
  --dataset longmemeval_s    LongMemEval-S (500 questions)
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

echo "==> [1/6] starting infrastructure"
docker compose up -d postgres minio mc-bootstrap temporal temporal-ui embedder-mock
docker compose up -d --wait postgres temporal minio

echo "==> [2/6] running migrations"
docker compose run --rm server /app/migrate up

echo "==> [3/6] fetching dataset: $DATASET"
if [[ "$DATASET" != "fixtures" ]]; then
  ./scripts/download-dataset.sh "$DATASET"
else
  echo "    using built-in fixtures (testdata/fixtures.json)"
fi

echo "==> [4/6] starting server + Temporal worker (schedule created on boot)"
docker compose up -d --wait server
echo "    Temporal UI: http://localhost:8080"
echo "    MinIO console: http://localhost:9001 (app/appsecret)"

echo "==> [5/6] ingesting + waiting for enrichment at version $VERSION"
docker compose run --rm server /app/client ingest \
  --dataset "$DATASET" --version "$VERSION"

# Trigger the schedule immediately rather than waiting up to 60s for the tick.
docker compose run --rm server /app/client trigger-sweep

# Poll the version-pinned progress until remaining == 0. Fail loudly on dead rows.
docker compose run --rm server /app/client wait-enriched \
  --version "$VERSION" --timeout 15m --fail-on-dead

echo "==> [6/6] running retrieval eval (mode=$RETRIEVAL)"
docker compose run --rm server /app/client eval \
  --dataset "$DATASET" --version "$VERSION" --retrieval "$RETRIEVAL"
