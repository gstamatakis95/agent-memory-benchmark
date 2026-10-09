# CONFLICTS.md

Every entry records a conflict between the plan and reality (plan text vs register vs reference file vs environment),
both readings, the proposed resolution and the register rows involved. An entry is resolved only by a human-approved
line; a resolved entry then counts as a register row for this repository (orchestrator prompt, "ON CONFLICT"). The
orchestrator never changes a register constant on its own.

States: `open` (awaiting a human ruling), `resolved` (ruling recorded below the entry), `noted` (no ruling needed; the
divergence is recorded so it stays visible).

---

## #1 Go module path — resolved

- What: the reference proto README and older plan text predate the module path decision.
- Readings: placeholder module paths in older text vs register N190.
- Resolution: the module path is `github.com/gstamatakis95/engram` (register N190, human decision after D27). `go.mod`,
  `buf.gen.yaml` and every import use it.
- Register rows: N190.
- Ruling: resolved by N190 itself (a human decision recorded in the plan). No further ruling needed.

## #2 Target repository `gstamatakis95/engram` does not exist and cannot be created from this session — open

- What: the task says to build in the new repository `gstamatakis95/engram`. `add_repo` reports that it does not exist
  (or that the session's credential has no access) and `create_repository` through the GitHub integration returns
  `403 Resource not accessible by integration`.
- Readings: (a) the human creates the repository and grants the Claude GitHub App access; (b) the orchestrator builds
  in a local git repository and mirrors its tree into the designated branch of `agent-memory-benchmark` until (a).
- Proposed resolution: (b) now, (a) as soon as the human acts. The local repository at `/home/user/engram` keeps the
  real per-milestone history; its working tree is mirrored under `engram/` on branch
  `claude/engram-implementation-wvfg47` of `gstamatakis95/agent-memory-benchmark` after every merge so the work survives
  a session restart. When the repository exists the orchestrator pushes `main` with its full history and stops
  mirroring.
- Needs from the human: create `gstamatakis95/engram` (empty, no README) and install the Claude GitHub App on it (or
  give the session push access through `add_repo`).
- Register rows: none (environment).

## #3 Docker in the build environment: no bridge network, so testcontainers port mapping does not work — noted

- What: PLAN.md §8.1 runs T3 on `testcontainers-go` with `paradedb/paradedb:latest-pg16`, T4/T5 on `docker compose`.
  The session container had no running daemon; `dockerd` was started by the orchestrator with `--bridge=none
  --iptables=false --storage-driver=vfs` because the container cannot create bridge networks. Containers therefore run
  with `--network host` on distinct ports; the default testcontainers port mapping is unavailable here. The image
  `paradedb/paradedb:latest-pg16` (digest `sha256:7aba4dbac45cfcaaaddce50feb7081b3fc907061834c68b52a23aa7b66442b76`,
  PostgreSQL 16.15, pg_search 0.26.0, pgvector 0.8.6) is pulled and runs.
- Resolution (harness detail, no plan change): the T3 harness keeps testcontainers with the pinned digest as the CI
  default and adds two developer knobs that change no test: `ENGRAM_TEST_DOCKER_HOST_NETWORK=1` (start the container in
  host network mode on a free port) and `ENGRAM_TEST_PG_DSN` (use an already running server; `engramctl migrate --dsn`
  applies the migrations, exactly as §8.1 says). `docker compose` for T4/T5 is re-evaluated in M0.4 (compose needs a
  bridge network for its service DNS; if it cannot run here, M0.4 is written and verified only in CI and the entry is
  reopened).
- Register rows: §8.1 T3 row, §9.1, N131, N138.

## #4 Plan copy location: `docs/plan/` (task statement) vs `docs/PLAN.md` (orchestrator prompt) — noted

- What: the task statement says to copy PLAN.md, proto/, sql/ and formal/ under `docs/plan/`; the orchestrator prompt's
  REPOSITORY block names `docs/PLAN.md`.
- Resolution: one frozen copy under `docs/plan/` (`PLAN.md`, `proto/`, `sql/`, `formal/`, `IMPLEMENTATION_PROMPT.md`,
  `SOURCE_COMMIT`). Every brief and script that the prompt writes as `docs/PLAN.md` reads `docs/plan/PLAN.md`. The
  working copies the plan names (`proto/`, `migrations/`, `formal/`) are created from `docs/plan/` by M0.1, M0.2/M0.8
  and M0.7. The task statement is the later instruction and wins; no plan content changes.

## #5 Reference protos contain lines longer than 120 characters — withdrawn

- What: the first measurement counted bytes, not characters; the 16 "long" proto lines are over 120 bytes because of
  non-ASCII characters and all are ≤ 120 characters. `scripts/check-line-length.sh` counts characters and passes every
  reference proto unchanged. M0.1 reflowed no proto comment and touched no field; only `proto/README.md` tables were
  rewritten as bullets.

## #6 PROGRESS.md and CONFLICTS.md are orchestrator-owned files — noted

- What: the orchestrator prompt has workers update PROGRESS.md and write CONFLICTS.md entries. Three workers run
  concurrently on three branches; concurrent edits of one file would conflict at every merge.
- Resolution: a worker writes its status, exit checklist and any conflict entries into
  `docs/briefs/reports/M<x.y>-report.md` on its branch; the orchestrator folds them into PROGRESS.md and CONFLICTS.md on
  `main` at every state change. The information the prompt requires is still recorded after every green step; only the
  file it lands in differs until the merge.

## #7 Go's `internal` import rule forbids the generated path `gen/go/engram/internal/...` — open (proceeding under the
proposed resolution)

- What: D14 and §2.1 put the generated code of `engram.internal.{errors,events,workflow}.v1` under
  `gen/go/engram/internal/...`. Go allows an import path containing the element `internal` only from packages rooted at
  the parent of that element, so `internal/errs`, `internal/store` and `internal/workflows` cannot import it; the stub
  module does not compile as the plan writes it.
- Readings: (a) the plan's directory is binding and the three packages must live under `gen/go/engram/` (impossible:
  they are leaves and services); (b) only the Go package path changes.
- Proposed resolution (applied in M0.1 so the module compiles; cheap to rename if the ruling differs): the three
  `go_package` options become `github.com/gstamatakis95/engram/gen/go/engram/private/{errors,events,workflow}/v1`
  with the aliases `errorsv1`, `eventsv1`, `workflowv1`. Proto package names (`engram.internal.*`), proto directories,
  message names and field numbers are unchanged; "never crosses the wire" (N128) is a property of the servers, not of
  the Go path. Amend D14 and the §2.1 layout line `gen/go/...` accordingly.
- Register rows: D14, N128, N140.

## #8 `govulncheck` cannot be green on the Go 1.25 pin — open

- What: the toolchain pins Go 1.25 (plan, task statement). On Go 1.25.7 `govulncheck` reports 19 standard-library
  findings; on 1.25.14 (the last 1.25 patch) 7 remain, fixed only in Go 1.26.9 and `golang.org/x/net` v0.60.0, which
  requires go 1.26. Go 1.25 left the two-release support window when Go 1.27 shipped.
- Readings: (a) keep the 1.25 pin and run `govulncheck` as advisory (`continue-on-error`) until a ruling; (b) move the
  pin to Go 1.26 (latest patch) and make `govulncheck` gating as §8.1 tier S requires.
- Proposed resolution: (b). Nothing in the plan depends on a 1.25-only behaviour; the pin is a floor, and tier S names
  `govulncheck` as a required gate. Until the ruling the CI step is advisory, as the M0.1 branch does.
- Register rows: §8.1 tier S; the TOOLCHAIN block of the orchestrator prompt; N190 (module path only).

## #9 "`make lint test-unit` < 30 s" is a warm-cache figure — noted

- What: on this 4-core machine the target runs in 8–11 s with warm Go and golangci-lint caches and ≈ 118 s cold (the
  cold cost is compiling the Temporal SDK, gRPC, protobuf and ≈ 34 k lines of generated code).
- Resolution: the criterion is read as the CI steady state (CI caches the module and build caches between runs, as the
  M0.1 workflow does). If the human reads it as cold, the first lever is dropping the Temporal SDK from the stub
  module (it is needed only for the `workflow.Context` and `converter.DataConverter` signatures).
- Register rows: §10.2 M0.1.

## #10 Small signature-level deviations recorded by M0.1 — noted (reviewer to confirm)

- `pg.LSN`: the plan names a package D14 does not list; M0.1 declares `catalog.LSN` and `store.LSN` instead of adding a
  leaf package. A shared type would need a new leaf or a §2.1 exception.
- `ledger → store`: §2.2.24's ledger implementation needs the store; the edge is not in the §2.1 infrastructure edge
  list and depguard allows exactly that edge.
- `internal/errs` imports `google.golang.org/genproto/googleapis/rpc/{status,errdetails}`: §2.4 attaches the typed
  details to `google.rpc.Status`, which lives in that module; the §2.1 leaf-permitted list names `grpc/status`, which
  depends on it, so it is treated as covered.
- `workflow.proto`: three messages the §2.2.17 prose names (`RetainDocumentResult`, `MarkProgressInput`,
  `MarkOperationInput`) were missing from the reference file and were added (prose wins over file; no existing field
  changed).
- `fsm.MoveState` includes `lost` (DDL and N183).

## #11 Four `namespace_moves` columns have no writer among the §2 methods — open

- What: `TestDeps_EveryRPCHasPath`'s column-coverage half (N184, A8-2) finds `terminated_workflows`, `lost_at`,
  `lost_restore_id`, `recovered_from_move_id` without a writer method. N184 fixes `MoveStamps` at three methods, so a
  writer cannot be added without a register change. The columns are listed in a named exception set in the test.
- Proposed resolution: decide in M1.5's design review which method (the mover's `lost` edge, N183, or the catalog
  reconcile) writes them; this is a move-protocol question and belongs to the two-round review of M1.5.
- Register rows: N183, N184, A8-2.

## #12 Q18: pg_search BM25 stays the MVP lexical arm, but Top-K pushdown under RLS holds in one query shape — open

- What (M0.2 measurement, `bench/results/phase0/m0.2-q18-lexical.md`; ParadeDB digest 7aba4d…, pg_search 0.26.0, 16
  partitions, 50 k- and 1 k-fact namespaces, 150 queries per cell, machine under TLC load): Top-K pushdown under RLS
  holds only when the inner query's namespace predicate is textually the RLS qual
  `namespace_id = current_setting('engram.namespace_id')::uuid`; with a bound `namespace_id = $1` the policy qual
becomes
  a `Result` node under the Top-K and pg_search fails with "Unsupported query shape". `mentioned_at <= 'infinity'` fails
  ("json cannot be converted to term"), so an unset `as_of` must omit the predicate. The RLS-shaped form plans all 16
  partitions (40–80 ms planning): p50 39–44 ms, p95 50–61 ms against the 60 ms budget. Behind a `SECURITY DEFINER`
  function taking the namespace as a parameter: p50 15–18 ms, p95 23–25 ms at 50 k. `TsvectorIndex` is unusable
under
  RLS without a definer function (`@@`/`ts_rank_cd` not leakproof, 217–373 ms seq scan) and behind one reaches p95
80 ms
  (mixed) / 162 ms (common terms). BM25 scores are per partition; ranking within a namespace is deterministic.
- Proposed ruling: pg_search in the RLS-shaped form is the MVP arm; `TsvectorIndex` is not declared. If M0.6 on idle
  hardware confirms the p95 at or over budget, add the definer function `engram_lexical_facts` by an expand-only
  migration in M1.2 (needs a register row; not in the M0.2 migrations). `engramlint sql` must accept the RLS-shaped
  predicate as the explicit `namespace_id` predicate for this arm.
- Needs from the human: a register row for the definer function, or acceptance of a 50–60 ms BM25 p95 inside the N164
  arm budgets.
- Register rows: D7, N19, N76, N131, N164.

## #13 Q9: pgx `QueryExecModeExec` behind pgbouncer transaction pooling — resolved by measurement (noted)

- What (`bench/results/phase0/m0.2-q9-pgx.md`; pgbouncer 1.26.0, five pgx modes, direct and pooled targets with
  `max_prepared_statements` 0 and 100): the default `CacheStatement` fails behind pgbouncer without
  `max_prepared_statements` ("prepared statement already exists"); every other mode works; latency differences are
  0.1–0.4 ms on small statements and nil on arm-sized ones; prepared statements cannot save planning because the arms
  need `force_custom_plan` (N112) and the BM25 arm plans per call.
- Resolution: `QueryExecModeExec` for the pgbouncer pools, pgx's default for direct connections, no pgbouncer version
  floor; `config lint` rejects `CacheStatement` on a pooled DSN unless `max_prepared_statements > 0`.
- Register rows: §11.2 Q9, N112.

## #14 Defects in the reference DDL and plan text found while splitting the migrations — noted (reviewer to confirm)

1. `deletion_log` is insert-only (N137) but the reference DDL gives it no `fillfactor = 100`; migration 0001 adds it and
   0004 gains a whole-class self-check.
2. Self-checks 2 and 14 fail on a database created from the ParadeDB image's `template1` (PostGIS adds
   `spatial_ref_sys`); 0004 and `CheckRLS` ignore extension-owned relations.
3. PLAN.md §9.2 names the second migration `0002_partitions.go`; it is SQL because the reference file builds
   partitions and policies with `DO` loops.
4. PLAN.md §8.3 lists 3 tables without `namespace_id`; §3.3.8 and the DDL have 5 (plus `ownership_transitions` and
   `engram_seq_log`). The check uses 5.
5. pg_search 0.26 deprecates `key_field` (one warning per bm25 index, no effect); the DDL targets 0.25 and is kept
   verbatim. Open: bump the syntax or pin the image to 0.25 (the pinned digest ships 0.26.0).
6. `documents_tags_gin` is unusable by `engram_app` under RLS (array operators are not leakproof); tag resolution reads
   the namespace's documents through the namespace key. Open for M1.2: drop it, keep it for a definer resolver, or make
   tag resolution a definer function.
7. `engram_admin` cannot `ANALYZE`/`VACUUM` (PostgreSQL 16 requires ownership); the plan gives `VACUUM` to the index
   runner, so no change.

## #15 Migrations run as a bootstrap superuser — open (low urgency)

- What: migration 0001 creates roles and extensions, then `SET LOCAL ROLE engram_migrate`, so `engramctl migrate` needs
  a superuser DSN. The alternative splits 0001 into a provisioning script (`engramctl shard add`, §9.5) that creates
  roles and extensions and hands `engramctl migrate` an `engram_migrate` DSN.
- Proposed resolution: decide in M0.8 (which finalises `sql/ → migrations/`) together with §9.5 provisioning;
until then
  the superuser bootstrap stands.
- Register rows: §9.2, §9.5, N133e.
