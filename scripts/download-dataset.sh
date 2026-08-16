#!/usr/bin/env bash
# Download a real benchmark dataset into ./datasets/.
#
# usage: ./scripts/download-dataset.sh locomo|longmemeval_s
#
# NOTE (image visibility): run.sh mounts no volumes into the server image, so
# the client reads real datasets from /app/datasets, which is baked into the
# image AT BUILD TIME. The server image is built before this script runs in
# run.sh's sequence, so for locomo/longmemeval runs download first and force
# a rebuild once:
#     ./scripts/download-dataset.sh locomo
#     docker compose build server
#     ./run.sh --dataset locomo
# Fixtures need none of this (testdata/fixtures.json is always baked in).
set -euo pipefail

DATASET="${1:-}"
DIR="$(cd "$(dirname "$0")/.." && pwd)/datasets"
mkdir -p "$DIR"

case "$DATASET" in
  locomo)
    # Official release: snap-research/locomo, data/locomo10.json (10 conversations).
    URL="https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json"
    OUT="$DIR/locomo10.json"
    if [[ -s "$OUT" ]]; then
      echo "locomo: $OUT already present ($(wc -c <"$OUT") bytes)"
      exit 0
    fi
    echo "locomo: downloading $URL"
    if ! curl -fsSL --retry 3 -o "$OUT.tmp" "$URL"; then
      echo "locomo: download failed. Fetch it manually from" >&2
      echo "  https://github.com/snap-research/locomo (data/locomo10.json)" >&2
      echo "and place it at $OUT" >&2
      exit 1
    fi
    mv "$OUT.tmp" "$OUT"
    echo "locomo: saved $OUT ($(wc -c <"$OUT") bytes)"
    ;;

  longmemeval_s)
    OUT="$DIR/longmemeval_s.json"
    if [[ -s "$OUT" ]]; then
      echo "longmemeval_s: $OUT already present ($(wc -c <"$OUT") bytes)"
      exit 0
    fi
    # Official distribution is the xiaowu0162/longmemeval HF dataset repo
    # (also mirrored via a Google Drive link in the GitHub README). The HF
    # download may require `huggingface-cli login` first.
    if command -v huggingface-cli >/dev/null 2>&1; then
      echo "longmemeval_s: downloading via huggingface-cli"
      if huggingface-cli download xiaowu0162/longmemeval longmemeval_s.json \
           --repo-type dataset --local-dir "$DIR"; then
        echo "longmemeval_s: saved $OUT"
        exit 0
      fi
      echo "longmemeval_s: huggingface-cli download failed (login/permissions?)" >&2
    else
      echo "longmemeval_s: huggingface-cli not installed" >&2
    fi
    cat >&2 <<EOF
longmemeval_s: could not download automatically. To fetch it:
  pip install -U "huggingface_hub[cli]"
  huggingface-cli login            # if the repo requires auth
  huggingface-cli download xiaowu0162/longmemeval longmemeval_s.json \\
      --repo-type dataset --local-dir "$DIR"
(or use the Google Drive link in https://github.com/xiaowu0162/LongMemEval)
Then re-run. Expected path: $OUT
EOF
    exit 1
    ;;

  *)
    echo "usage: $0 locomo|longmemeval_s" >&2
    exit 2
    ;;
esac
