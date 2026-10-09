#!/usr/bin/env bash
# Creates the dev bucket on MinIO (self-signed TLS, hence -k) with a SigV4-signed PUT (the image has no shell, no mc).
set -euo pipefail
cd "$(dirname "$0")"
bucket="${1:-engram-dev}"
user="$(cat secrets/minio_root_user)"
pass="$(cat secrets/minio_root_password)"
for _ in $(seq 1 60); do
  code="$(curl -sk -o /dev/null -w '%{http_code}' --noproxy '*' -X PUT --aws-sigv4 'aws:amz:local:s3' \
    --user "${user}:${pass}" "https://127.0.0.1:19000/${bucket}" || true)"
  case "$code" in 200 | 409) exit 0 ;; esac # 409: the bucket already exists and is ours
  sleep 1
done
echo "could not create bucket ${bucket} (last HTTP status ${code})" >&2
exit 1
