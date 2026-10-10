# Engram implementation plan

A decision-oriented implementation plan for a Go + PostgreSQL long-term memory service for AI agents, modelled on
Hindsight (MIT). The plan is standalone and unrelated to the benchmark project in this repository. It does not change
that project's constraints (see the repository `AGENTS.md`).

## Layout

| Path | What it is |
|---|---|
| `PLAN.md` | The single source: sections 1-12 plus the binding decision register as Appendix A. |
| `IMPLEMENTATION_PROMPT.md` | The orchestrator, worker and reviewer prompts for building the plan, and the open decisions. |
| `proto/` | The protobuf contracts (`memory.v1`, `memory.admin.v1`, internal workflow and event schemas), a buf workspace. |
| `sql/` | DDL for the control-plane catalog (`catalog_schema.sql`) and for one shard (`shard_schema.sql`). |
| `formal/tla/` | TLA+ specifications, TLC configurations, the `EXPECT` manifest and `results/` (the TLC results summary). |
| `formal/lean/` | Lean 4 developments (RRF, packing, tag matching, temporal windows); not type-checked, Lean was unavailable. |
| `reference/hindsight-notes.md` | Research notes on Hindsight; quoted prompt text is MIT-licensed, Copyright (c) 2025 Vectorize AI, Inc. |

## Validating the artifacts

```bash
# protobuf contracts: lint, compile, formatting (buf v1.57+)
cd plans/engram/proto && buf lint && buf build && buf format -d --exit-code

# DDL: parse with libpg_query and apply to a scratch PostgreSQL 16 (pg_search stubbed if absent)
python3 -m pip install pglast
psql -f plans/engram/sql/catalog_schema.sql
psql -f plans/engram/sql/shard_schema.sql

# TLA+: parse a spec, then model-check a configuration (tla2tools.jar)
cd plans/engram/formal/tla
java -cp tla2tools.jar tla2sany.SANY ShardMove.tla
java -jar tla2tools.jar -config ShardMove.cfg ShardMove.tla -workers auto
```

`formal/tla/EXPECT` is the machine-readable manifest: one line per configuration, either `pass` or
`violates <Invariant>`. A design configuration must end with "No error has been found"; a must-fail configuration must
end with exactly the named invariant violated. `formal/tla/results/RESULTS.md` records a full run of all 79
configurations. Raw TLC logs are not kept; run TLC to regenerate them.

## Status

The plan has been through eight adversarial design reviews. The review files are not shipped; their findings survive as
the register rows (decisions D1-D27 plus N189, the model-checking corrections to D27, in PLAN.md Appendix A) and as the regression index in
PLAN.md section 8.4.7. D27 (round 8: the WAL-floor seal, restore closes the move, `DeleteTenant` acknowledged after
fencing) was applied but not re-reviewed adversarially; the shard-move protocol is the highest-risk area. The decisions
that only a human may take are listed in section 4 of `IMPLEMENTATION_PROMPT.md`.
