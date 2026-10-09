# ENVIRONMENT (this build session; read with your brief)

- You work ONLY inside your worktree `/home/user/engram-wt/<branch>` on branch `<branch>`. `main` lives in
  `/home/user/engram`; never commit to it, never touch another worktree, never run `git push` (there is no remote yet,
  CONFLICTS.md #2). Commit after every green step with messages `M<x.y>: <what> (<register rows>)` and end every commit
  message with these two trailer lines, verbatim:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_015oJfa7yW7cidsXu8Ljecvb`.
  Use `git -c user.name=Claude -c user.email=noreply@anthropic.com commit`.
- The plan is `docs/plan/PLAN.md` (cite it as PLAN.md §N); the register is its Appendix A (rows D1–D27, N1–N190).
  Reference files: `docs/plan/proto/`, `docs/plan/sql/`, `docs/plan/formal/`. `docs/plan/` is frozen: never edit it.
- You do not edit `PROGRESS.md` or `CONFLICTS.md` (CONFLICTS.md #6). Your status, exit checklist, conflicts and open
  questions go into `docs/briefs/reports/M<x.y>-report.md` on your branch; keep it current after every green step and
  make it your final message too. There is no GitHub PR: the "PR" is your branch diff against `main`, and the report
file
  is the PR description.
- Shared files: `Makefile` target names are frozen; change only the recipes your brief assigns to you.
`go.mod`/`go.sum`:
  add what you need (the orchestrator reconciles at merge). `.golangci.yml`: only the owner named in your brief.
- Tools on this machine: Go 1.25.7 (`go`), buf 1.57.2 + protoc-gen-go/go-grpc/connect-go, goose, govulncheck in
  `/root/go/bin` (add to PATH: `export PATH=$PATH:/root/go/bin`), golangci-lint 2.5.0, psql 16, Java 21,
  `/root/tools/tla2tools.jar` (v1.8.0, TLC 2.18, sha256
7beec0f04818732a62fa193731711a99aa4f11279499b2360a7d156c519ea78d).
  4 CPU cores, 15 GB RAM. Lean/lake is NOT installed (`lake build` is wired non-gating only). Network: Go module proxy,
  GitHub downloads, Docker Hub and apt work.
- Docker: a daemon runs with `--bridge=none` (CONFLICTS.md #3). Containers must use `--network host` and distinct ports;
  testcontainers' default port mapping does not work here, `docker compose` service networking will not either. The
  image `paradedb/paradedb:latest-pg16` is pulled; pin digest
  `sha256:7aba4dbac45cfcaaaddce50feb7081b3fc907061834c68b52a23aa7b66442b76` (PostgreSQL 16.15, pg_search 0.26.0,
  pgvector 0.8.6, pg_trgm, btree_gin, btree_gist; `shared_preload_libraries` already includes `pg_search`). Start one
with
  e.g. `docker run -d --name <pkg> --network host -e POSTGRES_PASSWORD=pw -e PGPORT=<port> <image> -c port=<port>`.
- A local PostgreSQL 16.15 cluster (apt) runs on :5432 (`su postgres -c psql`) with pgvector 0.8.7 but WITHOUT
  pg_search; use it only where pg_search is irrelevant.
- Before every commit: `make lint test-unit` must be green in your worktree (if the lint recipe cannot run yet because a
  directory your brief does not own is missing, say so in your report and run the parts that can). Never leave the
  branch red at a commit boundary.
- Long runs (TLC full sweeps, measurements) go to the background with output written to files under your worktree so a
  restart loses nothing. Assume your session can end at any moment: your first action on resume is
  `git log --oneline -20`, then your report file, then this brief.
