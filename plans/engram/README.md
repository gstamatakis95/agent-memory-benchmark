# Engram implementation plan

A decision-oriented implementation plan for a Go + PostgreSQL long-term memory service for AI agents,
modelled on Hindsight (MIT). The plan is standalone and unrelated to the benchmark project in this repository.
It has been through seven adversarial design reviews (`reviews/round-1.md` to `reviews/round-7.md`); the plan text is
the post-seventh-review version (v1.6). Round 3 produced a redesign of storage, deletion visibility, delete durability and
shard moves (D22, N111 to N134); round 4 corrected the mechanisms that acted outside its assumptions (D23, N135 to N143); round 5 found no blocker (D24, N144 to N159); round 6 replaced the shard-move protocol with
freeze-then-copy (D25, N160 to N168); round 7 removed the recovery paths around it: a move backup before the commit point and one replicated-ack rule for the catalog (D26, N169 to N178). Rewritten rows are marked "rev. D23" to "rev. D26". **The six TLA+ specifications are written and
model-checked** (`Outbox`, `Consolidation`, `Storage`, `Derivation`, `Durability`, `ShardMove`) with their must-fail
configurations and logs; section 7.1 states what each checks and what it omits. The Lean 4 developments are not
type-checked here.

| Path | What it is |
|---|---|
| `PLAN.md` | The assembled plan (sections 1-12 plus the decision register as Appendix A). |
| `00-decision-register.md` | The binding cross-cutting decisions every section follows; D20 holds the corrections adopted from round 1 (N50-N77), D21 those of round 2 (N79-N108), D22 the round-3 redesign (N111-N134), D23 the round-4 decisions (N135-N143), D24 the round-5 decisions (N144-N159), D25 the round-6 decisions (N160-N168), D26 the round-7 decisions (N169-N178). Rows a review superseded were rewritten in place ("rev. D23" to "rev. D26") and keep their ids. |
| `reviews/` | The seven adversarial reviews, one file per round: `round-1.md` (45 findings F-1...F-45), `round-2.md` (G-1...G-30), `round-3.md` (the correctness, Postgres and API/numbers reviews H-*, P-*, A-*, and the redesign advice R1 to R5), `round-4.md` (C-1...C-23, P-1...P-15, A-1...A-25 and the closing advice), `round-5.md` (C-1...C-14, P-1...P-15, A-1...A-18), `round-6.md` (C-1...C-16, P-1...P-12, A-1...A-11), `round-7.md` (44 findings: PG7-1...18, C7-1...11, T7-1...3, A7-1...12; ids restart each round). Each ends with a Disposition table that maps every finding to a register row and the section where it was applied. Read them to understand *why* D2, D5, D8, D9, D22 to D26 read the way they do. |
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
