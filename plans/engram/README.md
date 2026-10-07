# Engram implementation plan

A decision-oriented implementation plan for a Go + PostgreSQL long-term memory service for AI agents,
modelled on Hindsight (MIT). The plan is standalone and unrelated to the benchmark project in this repository.
It has been through three adversarial design reviews (`reviews/round-1.md` to `reviews/round-3.md`); the plan text is
the post-third-review version. Round 3 produced a redesign of storage, deletion visibility, delete durability and
shard moves (advice R1 to R5), applied through register block D22 (N111 to N132). **The TLA+ specifications lag the
design:** `Storage`, `Derivation`, `Durability` and a rewritten `ShardMove` are specified in round 3 but not yet
written, and the `*_Gate*.cfg` configurations are written but unrun (section 7.1 states what was checked).

| Path | What it is |
|---|---|
| `PLAN.md` | The assembled plan (sections 1-12 plus the decision register as Appendix A). |
| `00-decision-register.md` | The binding cross-cutting decisions every section follows; D20 holds the corrections adopted from round 1 (N50-N77), D21 those of round 2 (N79-N108), D22 the round-3 redesign (N111-N132). Rows the redesign superseded were rewritten in place and keep their ids. |
| `reviews/` | The three adversarial reviews, one file per round: `round-1.md` (45 findings F-1...F-45), `round-2.md` (G-1...G-30), `round-3.md` (the correctness, Postgres and API/numbers reviews H-*, P-*, A-*, and the redesign advice R1 to R5). Each ends with a Disposition table that maps every finding to a register row and the section where it was applied. Read them to understand *why* D2, D5, D8, D9 and D22 read the way they do. |
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
