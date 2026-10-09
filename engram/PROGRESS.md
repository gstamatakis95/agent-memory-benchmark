# PROGRESS.md

Per milestone: state ∈ {not started, in progress, in review, blocked, done}, branch, last green tier, next concrete
open review findings. The orchestrator updates this file on every state change; a worker's first action on resume is
`git log --oneline -20`, `cat PROGRESS.md`, then its brief (orchestrator prompt, "USAGE LIMITS AND RESTARTS").

## Environment (this build session)

- Local repository `/home/user/engram` (`main` + one worktree per milestone branch under `/home/user/engram-wt/`).
- Remote: none yet (CONFLICTS.md #2). Mirror of the tree: `engram/` on branch `claude/engram-implementation-wvfg47` of
  `gstamatakis95/agent-memory-benchmark`.
- Toolchain present: Go 1.25.7, buf 1.57.2, protoc-gen-go/go-grpc/connect-go, goose, govulncheck, golangci-lint 2.5.0,
  Java 21 + tla2tools.jar v1.8.0 (TLC 2.18) at `/root/tools/tla2tools.jar`
  (sha256 7beec0f04818732a62fa193731711a99aa4f11279499b2360a7d156c519ea78d), PostgreSQL 16.15 local cluster on :5432.
- Missing: Docker daemon, pgvector ≥ 0.8, pg_search, Lean/lake (CONFLICTS.md #3; `lake build` is non-gating anyway).

## Phase 0

| Milestone | State | Branch | Last green tier | Next step / findings |
|---|---|---|---|---|
| M0.1 Repo, buf, stubs, CI | not started | m0.1-protos | — | E3 worker: brief docs/briefs/M0.1.md |
| M0.2 Shard schema v1 | not started | m0.2-shard-schema | — | E1 worker; T3 verification blocked (CONFLICTS #3) |
| M0.3 Catalog, Resolver, authz | not started | m0.3-catalog-authz | — | after M0.1 |
| M0.4 Compose, Temporal, codec | not started | m0.4-compose | — | after M0.2; needs Docker (CONFLICTS #3) |
| M0.5 Recall measurements | not started | m0.5-measure-recall | — | after M0.3 (E3) |
| M0.6 Storage measurements | not started | m0.6-measure-storage | — | after M0.3 (E1) |
| M0.7 Formal conformance | not started | m0.7-formal | — | E2 worker: brief docs/briefs/M0.7.md |
| M0.8 Migrations and generators | not started | m0.8-gen | — | after M0.6 (E1) and M0.7 (E2) |

## Phase 1

| Milestone | State | Branch | Last green tier | Next step / findings |
|---|---|---|---|---|
| M1.1 Retain pipeline | not started | m1.1-retain | — | after M0.8 (E2) |
| M1.2 Recall | not started | m1.2-recall | — | after M0.5 (E3) |
| M1.3 Delete and visibility | not started | m1.3-delete | — | after M1.1, M1.8 |
| M1.4 Outbox relay | not started | m1.4-outbox | — | after M1.8 |
| M1.5 Moves behind flag + twins | not started | m1.5-move | — | after M1.4; two review rounds |
| M1.6 Adapters and engramctl | not started | m1.6-adapters | — | after M1.2 |
| M1.7 Compose failover, dashboards | not started | m1.7-ops | — | after M1.10 (E1), M1.9 (E3) |
| M1.8 Fence and state machine | not started | m1.8-fence | — | after M0.8 |
| M1.9 Intent replay floor + put | not started | m1.9-intent | — | after M1.5 (E1), M1.6 (E3); two review rounds |
| M1.10 Paced purge, index runner, materialize | not started | m1.10-expunge | — | after M1.9 (E1), M1.3 (E2) |

## Phase 2

| Milestone | State | Branch | Last green tier | Next step / findings |
|---|---|---|---|---|
| M2.1 Consolidation + Derivation twins | not started | m2.1-consolidate | — | after M1.10 (E2) |
| M2.2 Reflect and H-matched run | not started | m2.2-reflect | — | after M1.7 (E3) |
| M2.3 as_of over observations | not started | m2.3-asof | — | after M2.1 |
| M2.4 Quotas and metering | not started | m2.4-quota | — | after M1.7 (E1) |
| M2.5 RetainBackfill | not started | m2.5-backfill | — | after M2.4 |

## Log

- 2026-10-09 skeleton: `docs/plan/` frozen copy (source commit in `docs/plan/SOURCE_COMMIT`), shared `Makefile` with the
  §8.1 target names, `go.mod`, `.golangci.yml` (lll 120), `scripts/check-line-length.sh`, `cmd/engramctl` dispatcher,
  CONFLICTS.md #1–#5.
