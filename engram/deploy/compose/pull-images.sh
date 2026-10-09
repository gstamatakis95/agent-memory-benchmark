#!/usr/bin/env bash
# Sourced by scripts/e2e.sh and scripts/temporal-restore-drill.sh: loads deploy/compose/.env (the image pins) and
# defines pull_images VAR..., which makes each pinned image available locally. A registry that refuses the pull (Docker
# Hub rate limit) is retried through E2E_MIRROR (default mirror.gcr.io) with the same digest, so the pin holds; the
# variable is then exported with the mirror's reference and compose picks that up.
_pi_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# the pins of .env, without overriding what the caller already set (a mirror reference, APK_SCHEME=http, ...)
while IFS='=' read -r key value; do
  case "$key" in "" | "#"*) continue ;; esac
  if [ -z "${!key+x}" ]; then export "$key=$value"; fi
done < "$_pi_dir/.env"
pull_images() {
  local var ref mirror="${E2E_MIRROR:-mirror.gcr.io}" path
  for var in "$@"; do
    ref="${!var}"
    if docker image inspect "$ref" > /dev/null 2>&1 || docker pull -q "$ref" > /dev/null 2>&1; then continue; fi
    case "$ref" in cgr.dev/* | */*/*) echo "cannot pull $ref" >&2; return 1 ;; esac
    path="$ref"
    [[ "$path" == */* ]] || path="library/$path"
    if docker image inspect "$mirror/$path" > /dev/null 2>&1 || docker pull -q "$mirror/$path" > /dev/null 2>&1; then
      export "$var=$mirror/$path"
      echo "pulled $ref through $mirror" >&2
    else
      echo "cannot pull $ref (rate limited? set E2E_MIRROR)" >&2
      return 1
    fi
  done
}
# behind a TLS-intercepting proxy the Alpine CDN's certificate is not trusted inside the build: use http (apk still
# verifies every package signature)
if [ -n "${HTTPS_PROXY:-}${https_proxy:-}" ] && [ "${APK_SCHEME:-https}" = https ]; then export APK_SCHEME=http; fi
