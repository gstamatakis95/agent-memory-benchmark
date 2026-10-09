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

## #5 Reference protos contain lines longer than 120 characters — noted

- What: the 120-column rule covers proto files; `buf format` does not enforce length. `workflow.proto` (11 lines),
  `memory.proto` (2), `admin.proto` (2), `page.proto` (1) exceed it in comments.
- Resolution: M0.1 reflows those comment lines when it copies `docs/plan/proto/` to `proto/`;
  `scripts/check-line-length.sh` covers `*.proto` from then on. Field numbers and names are untouched.

## #6 PROGRESS.md and CONFLICTS.md are orchestrator-owned files — noted

- What: the orchestrator prompt has workers update PROGRESS.md and write CONFLICTS.md entries. Three workers run
  concurrently on three branches; concurrent edits of one file would conflict at every merge.
- Resolution: a worker writes its status, exit checklist and any conflict entries into
  `docs/briefs/reports/M<x.y>-report.md` on its branch; the orchestrator folds them into PROGRESS.md and CONFLICTS.md on
  `main` at every state change. The information the prompt requires is still recorded after every green step; only the
  file it lands in differs until the merge.
