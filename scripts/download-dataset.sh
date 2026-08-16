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
    # The client expects datasets/longmemeval_s.json (cmd/client/dataset.go),
    # but the HF dataset repo names the file exactly "longmemeval_s" — no
    # .json extension — so the download is renamed into place below.
    OUT="$DIR/longmemeval_s.json"
    RAW="$DIR/longmemeval_s"
    if [[ -s "$OUT" ]]; then
      echo "longmemeval_s: $OUT already present ($(wc -c <"$OUT") bytes)"
      exit 0
    fi
    if [[ -s "$RAW" ]]; then
      echo "longmemeval_s: found already-downloaded $RAW; renaming to $OUT"
      mv "$RAW" "$OUT"
      echo "longmemeval_s: saved $OUT ($(wc -c <"$OUT") bytes)"
      exit 0
    fi
    # Official distribution is the xiaowu0162/longmemeval HF dataset repo
    # (also mirrored via a Google Drive link in the GitHub README). The
    # modern CLI binary is `hf`; huggingface-cli is the legacy name kept as
    # a fallback. The download may require a prior `hf auth login`.
    HF_CLI=""
    if command -v hf >/dev/null 2>&1; then
      HF_CLI="hf"
    elif command -v huggingface-cli >/dev/null 2>&1; then
      HF_CLI="huggingface-cli"
    fi
    if [[ -n "$HF_CLI" ]]; then
      echo "longmemeval_s: downloading via $HF_CLI"
      if "$HF_CLI" download xiaowu0162/longmemeval longmemeval_s \
           --repo-type dataset --local-dir "$DIR" && [[ -s "$RAW" ]]; then
        mv "$RAW" "$OUT"
        echo "longmemeval_s: saved $OUT ($(wc -c <"$OUT") bytes)"
        exit 0
      fi
      echo "longmemeval_s: $HF_CLI download failed (login/permissions?)" >&2
    else
      echo "longmemeval_s: neither hf nor huggingface-cli is installed" >&2
    fi
    cat >&2 <<EOF
longmemeval_s: could not download automatically. To fetch it:
  pip install -U "huggingface_hub[cli]"
  hf auth login                    # if the repo requires auth
  hf download xiaowu0162/longmemeval longmemeval_s \\
      --repo-type dataset --local-dir "$DIR"
  mv "$DIR/longmemeval_s" "$OUT"
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
