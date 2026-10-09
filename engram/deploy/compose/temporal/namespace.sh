#!/bin/sh
# One-shot: the Temporal namespace `engram` with 7 days of retention (PLAN.md section 9.1), created once.
set -eu
if temporal operator namespace describe --namespace engram > /dev/null 2>&1; then
  echo "namespace engram exists"
else
  temporal operator namespace create --namespace engram --retention 7d --description "Engram dev cell"
fi
