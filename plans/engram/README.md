# Engram implementation plan

A decision-oriented implementation plan for a Go + PostgreSQL long-term memory service for AI agents,
modelled on Hindsight (MIT). The plan is standalone and unrelated to the benchmark project in this repository.

| Path | What it is |
|---|---|
| `PLAN.md` | The assembled plan (sections 1–12 plus the decision register as Appendix A). |
| `00-decision-register.md` | The binding cross-cutting decisions every section follows. |
| `sections/` | The individual sections that `PLAN.md` is assembled from. |
| `proto/` | The protobuf contracts (`memory.v1`, `memory.admin.v1`, internal workflow and event schemas) with `buf.yaml`. |
| `sql/` | DDL for the control-plane catalog and for one shard. |
| `formal/tla/` | TLA+ specifications and TLC configs; `formal/tla/results/` holds the TLC run logs behind the results table in §7. `formal/lean/` Lean 4 developments (not type-checked here: Lean was unavailable). |
| `reference/hindsight-notes.md` | Research notes on Hindsight used as the reference; quoted prompt text is MIT-licensed, Copyright (c) 2025 Vectorize AI, Inc. |

## Validating the artifacts

```bash
# protobuf contracts: lint and compile (buf v1.57+)
cd plans/engram/proto && buf lint && buf build

# DDL: parse with libpg_query and apply to a scratch Postgres 16 (pgvector >= 0.7; pg_search stubbed if absent)
python3 -m pip install pglast
psql -f plans/engram/sql/catalog_schema.sql
psql -f plans/engram/sql/shard_schema.sql

# TLA+: model-check a spec (tla2tools.jar)
cd plans/engram/formal/tla && java -jar tla2tools.jar -config ShardMove.cfg ShardMove.tla -workers auto
```
