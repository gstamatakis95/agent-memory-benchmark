# The shard image (PLAN.md section 9.1, N131): the pinned ParadeDB image plus pgBackRest (>= 2.46, block incremental
# needs it), so `archive_command` works and `pgbackrest` runs in the same container as Postgres. The same file builds
# the plain-PostgreSQL images of the catalog and of temporal-postgres by overriding BASE_IMAGE (compose does). The
# pgBackRest package version is a required build argument (deploy/compose/.env pins one per base distribution: the
# Debian build of the shard and the Alpine build of the catalog and temporal images are the same 2.5x series but not the
# same patch release). The settings wrapper (engram-postgres.sh, which turns the generated settings file into
# `postgres -c` arguments) is mounted by compose, not baked in.
#
# Only ONE instruction below changes the filesystem. On a storage driver that copies a whole layer per instruction (the
# vfs driver of the sandbox this was developed in), every extra RUN or COPY would add another full copy of the base.
ARG BASE_IMAGE=paradedb/paradedb@sha256:7aba4dbac45cfcaaaddce50feb7081b3fc907061834c68b52a23aa7b66442b76
FROM ${BASE_IMAGE}
# Alpine bases only: the scheme of the package repositories. A build behind a TLS-intercepting proxy passes `http`
# (apk still verifies every package signature).
ARG APK_SCHEME=https
ARG PGBACKREST_VERSION
USER root
RUN set -eux; \
    test -n "${PGBACKREST_VERSION}" || { echo "PGBACKREST_VERSION is a required build argument" >&2; exit 1; }; \
    if command -v apt-get >/dev/null; then \
        apt-get update; \
        apt-get install -y --no-install-recommends "pgbackrest=${PGBACKREST_VERSION}"; \
        rm -rf /var/lib/apt/lists/*; \
    else \
        sed -i "s|^https\?://|${APK_SCHEME}://|" /etc/apk/repositories; \
        apk add --no-cache "pgbackrest=${PGBACKREST_VERSION}"; \
    fi; \
    v="$(pgbackrest version | sed -n 's/^pgBackRest \([0-9]*\)\.\([0-9]*\).*/\1 \2/p')"; \
    set -- ${v}; \
    if [ "$1" -lt 2 ] || { [ "$1" -eq 2 ] && [ "$2" -lt 46 ]; }; then \
        echo "pgBackRest $1.$2 is older than 2.46 (block incremental, N131)" >&2; exit 1; \
    fi; \
    mkdir -p /etc/pgbackrest /var/log/pgbackrest /var/spool/pgbackrest; \
    chown -R postgres:postgres /var/log/pgbackrest /var/spool/pgbackrest
LABEL org.opencontainers.image.title="engram-shard-pg" \
      org.opencontainers.image.description="PostgreSQL with pgBackRest (PLAN.md 9.1, N131)"
