#!/usr/bin/env bash
# Sourced by formal-run.sh and formal-trace-check.sh: the TLC jar and its pin (PLAN.md section 7, GUARDRAILS: TLC 2.18,
# the jar pinned by SHA-256, never floating).
#
# The pin is `de.hhu.stups:tlatools:1.1.0` from Maven Central, which is TLC 2.18 ("TLC2 Version 2.18 of 20 March 2023"):
# a Maven Central artifact is immutable, unlike the GitHub release asset `v1.8.0/tla2tools.jar`, which is republished
# from master every night (CONFLICTS.md #26). It reproduces the shipped design state counts exactly (Outbox.cfg
# 17,243,719 generated / 5,557,863 distinct / depth 39; Storage_Gens.cfg 270,826 / 54,596 / 35). The jar holds TLC and
# SANY.
#
# jar_path prints the verified jar: $TLA2TOOLS_JAR when set (it must match the pin too), else the cache copy, downloaded
# from Maven Central when absent. A hash mismatch is refused (exit 2).
JAR_VERSION=1.1.0
JAR_SHA256=fc0a7b69b35076b4aeef54228a62fce9ca035f81a19619f5ed3f023f78c803b3
JAR_URL=https://repo1.maven.org/maven2/de/hhu/stups/tlatools/${JAR_VERSION}/tlatools-${JAR_VERSION}.jar

jar_sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

jar_path() {
  local cache=${FORMAL_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/engram/tla}
  local jar sum try
  if [ -n "${TLA2TOOLS_JAR:-}" ]; then
    jar=$TLA2TOOLS_JAR
    [ -f "$jar" ] || die "TLA2TOOLS_JAR=$jar does not exist"
  else
    jar=$cache/tlatools-$JAR_VERSION.jar
    if [ ! -f "$jar" ]; then
      mkdir -p "$cache"
      echo "formal: downloading $JAR_URL" >&2
      for try in 1 2 3 4 5; do
        curl -fsSL -o "$jar.part" "$JAR_URL" && break
        rm -f "$jar.part"
        [ "$try" -lt 5 ] || die "download of tlatools-$JAR_VERSION.jar failed"
        sleep $((try * 10))
      done
      mv "$jar.part" "$jar"
    fi
  fi
  sum=$(jar_sha256_of "$jar")
  if [ "$sum" != "$JAR_SHA256" ]; then
    die "refusing $jar: sha256 $sum, expected $JAR_SHA256 (de.hhu.stups:tlatools:$JAR_VERSION, TLC 2.18)"
  fi
  echo "$jar"
}
