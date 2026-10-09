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
| M0.1 Repo, buf, stubs, CI | done | m0.1-protos (squash-merged) | S + T0 (15 s warm, 47 s cold lint) | review-2
merge-ready; carry-over to M0.3-E3: methodcount lint must load with build tags and walk local types (review-2 N1) |
Opus review round 1 running; worker
report docs/briefs/reports/M0.1-report.md on the branch |
| M0.2 Shard schema v1 | done | m0.2-shard-schema (squash-merged) | T3 (119 s) | review-2 merge-ready; open:
CONFLICTS #12 (BM25 p95 at budget), #15 (bootstrap role), #17; reviewer nit N2 (shared test role collision) carried
into M0.4 | review round 1: 1 Blocker (F1 N175 readiness), 1
Major (F2 BM25 p95 not gated), 9 Minor; fix round 1 sent incl. merge of main; re-review next | Opus review round 1
running; Q18/Q9 → CONFLICTS #12/#13; DDL defects #14; bootstrap role #15 |
| M0.3 Catalog, Resolver, authz | in progress (E3 half) | m0.3-catalog-authz | — | E3 worker running on
docs/briefs/M0.3-E3.md; E1 half after M0.4 |
| M0.4 Compose, Temporal, codec | done (Temporal-peak clause pending ruling #27) | m0.4-compose (squash-merged) | T4
smoke (95 s cold), restore drill, S + T0 | review-3 merge-ready; nit n9 (schedule-name suffix) → M1.x; quiet-host
Temporal re-run needs dedicated hardware | review round 1: 1 Blocker
(failure payloads under the cell key survive Shred), 2 Major (no per-shard key; failover agent promotes unfenced), 16
Minor; fix round sent | Opus review round 1
running; Temporal peak ≈ 300 events/s → CONFLICTS #27 (escalation); §9.1 defects #28 |
| M0.5 Recall measurements | in progress | m0.5-measure-recall | — | E3 worker on docs/briefs/M0.5.md; reranker
clause blocked on gateway access (human) |
| M0.6 Storage measurements | not started | m0.6-measure-storage | — | after M0.3 (E1) |
| M0.7 Formal conformance | done | m0.7-formal (squash-merged) | F (quick 228 s, TLC 2.18; sweep 77/79, #23 re-run
pending) | review-2 merge-ready; minors N1 (fast group listed in MANIFEST), N2 (state replay ignores action names),
N3 (nightly log upload) → carry-over to M0.8-E2; #22 awaits ruling | fix round 1 done
(jar pinned to Maven Central TLC 2.18); Opus review round 2 running | review round 1: 0 Blocker, 5
Major (changed-job timeout, twin milestones per twin, jar source, liveness in quick tier, Served pin), 11 Minor; fix
round sent incl. merge of main | Opus review
round 1 running; CONFLICTS #22 (tiering) needs a ruling; #23 re-run planned |
| M0.8 Migrations and generators | E2 half done (squash-merged); E1 half after M0.6 | m0.8-gen-e2 | S + T0 (55 s cold
lint), gen-docs idempotent, gendocs T3 | review-3 merge-ready; minors N1 (duplicate §8 name detection), N2 (golden
per version) → M1.1 carry-over; CONFLICTS #30 ruling pending | S + T0 (20 s), gen-docs
check | review round 1: 0 Blocker, 5 Major (constant values unchecked, ownership statements not executable,
render_hash contradiction, Reflect delimiters, GUC-derived constants), 10 Minor; fix round sent | S + T0 (20 s),
gen-docs
check | Opus review round 1 running; CONFLICTS #30 (N177 ratchet) needs a ruling; #31 noted | E2 worker on
docs/briefs/M0.8-E2.md; E1
half after M0.6 |

## Phase 1

| Milestone | State | Branch | Last green tier | Next step / findings |
|---|---|---|---|---|
| M1.1 Retain pipeline | in progress | m1.1-retain | — | E2 worker on docs/briefs/M1.1.md; store repositories
pending E1 (M1.8 brief carries the request list) |
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
- 2026-10-09 M0.1 worker finished (5 commits, 45 packages, 149 interfaces ≤ 5 methods); orchestrator verified
  `make lint test-unit` green in 8 s; CONFLICTS #5 withdrawn, #7–#11 added from the worker report; review round 1
started.
- 2026-10-09 M0.1 review round 1 (docs/briefs/reports/M0.1-review-1.md on the branch): 3 Major, 8 Minor, 5 Nit, 0
Blocker.
  Fix round 1 sent. F2 asserts a strict reading of N184(1)/(5) with the four #11 columns as a visible exception.
- 2026-10-09 M0.2 worker finished (2 commits, 4 migrations, harness, 7 named tests + extras); orchestrator verified
  `make test-integration` green in 73 s with ENGRAM_TEST_DOCKER_HOST_NETWORK=1; CONFLICTS #12–#15 added; review
round 1 started.
- 2026-10-09 M0.1 merged (squash) after review round 2 (docs/briefs/reports/M0.1-review-{1,2}.md): 0 Blocker, 0 Major.
  Reviewer minor N1 (methodcount build tags/local types) carried into M0.3-E3; CONFLICTS #16 added, #10 amended.
- 2026-10-09 M0.2 review round 1 (docs/briefs/reports/M0.2-review-1.md): 1 Blocker, 1 Major, 9 Minor, 4 Nit. Fix round
  sent; CONFLICTS #17 (N175 reading) and #18 (reference-DDL defects) added.
- 2026-10-09 M0.2 merged (squash) after review round 2: 0 Blocker, 0 Major. main: lint+unit 74 s (cold lint), T3 119
s green.
  Makefile test-prop fixed to target only rapid packages. M0.3-E3 worker finished; review round 1 started. M0.4 started.
- 2026-10-09 M0.3-E3 review round 1 (docs/briefs/reports/M0.3-E3-review-1.md): 0 Blocker, 4 Major, 18 Minor, 3 Nit.
  Fix round sent; CONFLICTS #19 (catalog DDL move CHECKs), #20 (ns_group storage), #21 (DeadlineGuard wording) added.
- 2026-10-09 12:30 UTC: all three Sonnet workers (M0.3-E3 fix round, M0.4, M0.7) were terminated by the session rate
limit;
  resumed at 12:45 UTC from their worktrees (committed state + uncommitted files preserved). dockerd and the local
  cluster were restarted.
- 2026-10-09 M0.7 worker finished (manifest 79 rows, scripts, invariant code, converter + proofs, full sweep);
orchestrator
  verified go test and the trace proof; CONFLICTS #22–#24 added; review round 1 started.
- 2026-10-09 M0.3-E3 merged (squash) after review round 2: 0 Blocker, 0 Major. T3 latency gates made opt-in
  (ENGRAM_T3_LATENCY_GATES=1) after the shipped BM25 arm missed 60 ms at load 2.1 on main (CONFLICTS #12 addendum).
  Docker daemon moved to overlay2 (disk 94 % → 52 %). M0.5 (E3) started.
- 2026-10-09 M0.7 review round 1 (docs/briefs/reports/M0.7-review-1.md): 0 Blocker, 5 Major, 11 Minor, 2 Nit. Fix round
  sent; CONFLICTS #22 addendum, #24 items 9–10, #26 (jar source) added.
- 2026-10-09 M0.4 worker finished (compose with host networking, split Temporal, codec, e2e, restore drill, load test);
  orchestrator verified make e2e 89 s cold and lint+unit; CONFLICTS #27 (Temporal peak, escalation) and #28 added.
- 2026-10-09 M0.4 review round 1 (docs/briefs/reports/M0.4-review-1.md): 1 Blocker, 2 Major, 16 Minor, 6 Nit; fix round
  sent; CONFLICTS #29 and #28 item 9 added. M0.7 fix round 1 verified (lint/unit, proof, manifest); review round 2
started.
- 2026-10-09 M0.7 merged (squash) after review round 2: 0 Blocker, 0 Major. main: lint+unit 64 s, manifest check ok,
  trace proof 24 ok. M0.8-E2 started.
- 2026-10-09 M0.4 merged (squash) after review round 3: 0 Blocker, 0 Major. main: lint+unit 61 s, make e2e 95 s cold.
  M0.3-E1 started.
- 2026-10-09 17:40 UTC: the session rate limit cut the M0.3-E1, M0.5 and M0.8-E2 workers; resumed 21:05 UTC from their
  worktrees after restarting dockerd (overlay2) and the local cluster.
- 2026-10-09 M0.8-E2 worker finished (prompts + goldens, internal/prompts, cmd/gendocs, docs/generated, M0.7
carry-over);
  orchestrator verified lint/unit and gen-docs --check; CONFLICTS #30–#31 added; review round 1 started.
- 2026-10-09 M0.8-E2 review round 1: 0 Blocker, 5 Major, 10 Minor, 2 Nit. Fix round sent; CONFLICTS #32 added.
- 2026-10-09 M0.3-E1 worker finished (reconcile, promotion, replicated ack, standby harness, config lint); orchestrator
  verified the five TestCatalog_* and the fault twin; CONFLICTS #33 added; review round 1 (move lens) started.
- 2026-10-09 M0.8-E2 merged (squash) after review round 3: 0 Blocker, 0 Major. main: lint+unit 55 s, gen-docs check ok.
