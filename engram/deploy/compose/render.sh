#!/usr/bin/env bash
# Generates what the dev compose mounts and the repository must not contain: the dev secrets (random, created once,
# world-readable because bind-mounted file secrets keep their host owner and the Postgres users inside the containers
# are not root) and the settings files rendered from deploy/postgres/gucs.yaml by gucsgen. Safe to run repeatedly.
set -euo pipefail
cd "$(dirname "$0")"
root="$(git rev-parse --show-toplevel)"
mkdir -p secrets/temporal_codec gucs
chmod 755 secrets secrets/temporal_codec

rand() { head -c 24 /dev/urandom | base64 | tr -d '/+=\n'; }
secret() { # name [value]: create secrets/<name> once
  if [ ! -s "secrets/$1" ]; then
    printf '%s' "${2:-$(rand)}" > "secrets/$1"
  fi
  chmod 644 "secrets/$1"
}

for s in shard_1_pw catalog_pw catalog_replicator_pw temporal_pg_pw minio_root_user minio_root_password; do
  secret "$s"
done
secret minio_root_user engram-minio # a fixed, readable access key; the secret key stays random
# pgBackRest reads its repository credentials and cipher pass from an include file per stanza
for stanza in shard_1 catalog temporal; do
  if [ ! -s "secrets/pgbackrest_${stanza}.conf" ]; then
    {
      echo "[global]"
      echo "repo1-cipher-pass=$(rand)"
      echo "repo1-s3-key=$(cat secrets/minio_root_user)"
      echo "repo1-s3-key-secret=$(cat secrets/minio_root_password)"
    } > "secrets/pgbackrest_${stanza}.conf"
  fi
  chmod 644 "secrets/pgbackrest_${stanza}.conf"
done
# MinIO speaks TLS (pgBackRest only talks to an S3 endpoint over TLS); a self-signed certificate for 127.0.0.1
mkdir -p secrets/minio_certs
chmod 755 secrets/minio_certs
if [ ! -s secrets/minio_certs/public.crt ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1" \
    -keyout secrets/minio_certs/private.key -out secrets/minio_certs/public.crt 2> /dev/null
fi
chmod 644 secrets/minio_certs/*
# the Temporal payload codec (N59): one 32-byte key per shard (s<shard>-<version>), a cell key for the cell-wide
# workflows and, for the isolation test, the key of a shard the dev stack does not host (a worker holding only that
# one must fail)
for kid in s1-k1 s2-k1 cell-k1; do
  if [ ! -s "secrets/temporal_codec/$kid.key" ]; then
    head -c 32 /dev/urandom > "secrets/temporal_codec/$kid.key"
  fi
  chmod 644 "secrets/temporal_codec/$kid.key"
done

# the postgres settings, rendered from the table (dev profile; the stanza lands in archive_command)
mkdir -p "$root/bin"
(cd "$root" && go build -o bin/gucsgen ./deploy/postgres/gucsgen)
gen() { "$root/bin/gucsgen" -in "$root/deploy/postgres/gucs.yaml" -profile dev "$@"; }
gen -kind shard -stanza shard-1 > gucs/shard-1.conf
gen -kind catalog -stanza catalog > gucs/catalog.conf
gen -kind standby -stanza catalog > gucs/standby.conf
gen -kind temporal -stanza temporal > gucs/temporal.conf
gen -limits > gucs/limits.yml # cpus, mem_limit and shm_size of the shard container
