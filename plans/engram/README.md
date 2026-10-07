# Engram implementation plan

A decision-oriented implementation plan for a Go + PostgreSQL long-term memory service for AI agents,
modelled on Hindsight (MIT). The plan is standalone and unrelated to the benchmark project in this repository.
It has been through two adversarial design reviews (`REVIEW.md`, then `REVIEW-2.md`); the plan text is the
post-second-review version. The second review (findings G-1 to G-30) is applied through register block D21
(N79 to N108). **The TLA+ specifications lag the design until work items W-1 to W-12 of section 7.6 land:**
no D20 or D21 mechanism is in any `.tla` file, the `ShardMove` PASS checks the pre-D20 move protocol, the
`*_Gate*.cfg` configurations are written but unrun, and the TLC queue was stopped on purpose until the specs
are updated.

| Path | What it is |
|---|---|
| `PLAN.md` | The assembled plan (sections 1–12 plus the decision register as Appendix A). |
| `00-decision-register.md` | The binding cross-cutting decisions every section follows; D20 holds the corrections adopted from the first adversarial review (N50–N77); D21 holds those of the second (N79–N108). |
| `REVIEW-2.md` | The second adversarial review (findings G-1…G-30) of the plan as amended by the first, with its Disposition table mapping every finding to a D21 row (N79–N108) and the section where it was applied. |
| `REVIEW.md` | The adversarial design review (45 findings, F-1…F-45, ranked by severity) and, at its end, the **Disposition** table that maps every finding to the register row and the section where it was applied. Read it to understand *why* D2, D4, D9, D17 and N41 read the way they do. |
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
