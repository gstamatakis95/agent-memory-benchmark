# GUARDRAILS (copied from the orchestrator prompt; a violation is a Blocker in review; the plan forbids each)

Sources of truth and precedence (highest first): (1) PLAN.md Appendix A, the register; inside one row a D27 clause beats
D26 beats D25 beats older text. (2) PLAN.md §1–§12; §10 is authoritative for schedule and exit criteria. (3) The
reference files under `docs/plan/` (proto, sql, formal); where prose and file disagree, prose wins and the file is fixed
in the same change with the register row cited in the commit. The TLA+ specifications are the ORACLE for protocol
behaviour: when Go behaviour and a spec invariant disagree, the Go code is wrong unless a register row says otherwise.
On any conflict (register vs section, section vs file, spec vs register, or an exit criterion that cannot be met as
written): STOP that item, write the conflict into your report file (what, where, both readings, your proposed
resolution, register rows involved), continue on independent work, and report. Never silently diverge, never
"simplify", never reopen a §12 non-goal.

- §12 non-goals are closed. In particular: no Kubernetes (NG2), no other database (NG3), no provider SDK (NG4), no
  in-process models (NG5), no read replicas on recall (NG8), no cross-shard query (NG9), no per-request model choice
  (NG25), no recall result cache (NG28), no bulk re-enqueue UPDATE (NG42), no dirty-copy move (NG47), no synchronous
  shard replication (NG34).
- §2 signature rules: every interface ≤ 5 methods (lint counts them); one typed id per entity (`id.FactID` etc., never
  bare strings/uuids); versions typed per entity; `context.Context` first, `error` last; options structs, no variadic
  option funcs; errors are `internal/errs` values; services take `id.Scope` + `id.Caller` + `store.Store`, never
  `authz.RequestScope` or `*router.ShardHandle`.
- Dependency rule of §2.1 enforced by depguard; the import graph stays acyclic; adapters import only `gen/go`, `authz`
  scope names and `errs`.
- Insert-only tables (PLAN.md §3.1 class tag `insert-only`): no `UPDATE`, no `DELETE` outside the expunge role;
  `engramlint sql` rejects it; `TestContent_InsertOnly` trigger stays green. No generated columns (N113). No
  `retired_at`/`live`/`invalidated_at` columns on content tables.
- Visibility is a read-time predicate in every arm and surface (N116, N117); nothing is computed at delete time; derived
  predicates fail closed.
- Every write transaction runs the fence prelude (shared try-lock, never waiting, then the ownership read at the
  caller's epoch); exclusive takers make one 35 s attempt (N82); reads accept `active` and `frozen/move` only.
- Outbox append is the last statement of its transaction (A-F1); the move never reads the outbox.
- Intent `put` after the marker commits and before the ack; the ack re-reads the marker (N122, N150).
- No `namespace`/`tenant` metric label (N62); no tenant content at `info` log level; secrets only from `/run/secrets`.
- nomic prefixes `search_document: ` / `search_query: ` and L2 normalisation on every vector (D15).
- Prompts adapted from Hindsight keep the MIT attribution (§6).
- No LLM call anywhere on the Recall path except the reranker and the query embedding (D10).
- `make gen-docs` output is never hand-edited; register constants (106 ms, 236 ms, θ, pool sizes, caps) come from one
  generated table.
- Every must-fail TLC configuration has a `faultinject` Go twin by the milestone §8.4.6 assigns it; a spec change
  without its manifest row and test change fails `scripts/formal-manifest-check.sh`.
- Line length: every source line (Go, proto, SQL, TLA+, Lean, YAML, Markdown in the repo) is at most 120 characters,
  and comments and doc strings FILL lines up to that limit rather than wrapping early (no 80-column habits). Enforced
  by golangci-lint `lll` (`line-length: 120`, tabs counted as 4) plus `gofmt`, `buf format` for protos, and
  `scripts/check-line-length.sh`. A review may flag an early-wrapped comment block as a Nit.
- Toolchain pins (never float): Go 1.25; buf ≥ 1.57 (`buf lint` STANDARD, `buf build`, `buf format -d --exit-code`,
  `buf generate` → `gen/go`, generated code committed; `buf breaking` is NOT run before v1.0.0, N190); PostgreSQL 16
  image `paradedb/paradedb:latest-pg16` pinned by digest; goose migrations applied only by `engramctl migrate` (§9.2:
  StatementBegin/End around plpgsql bodies, no CIC on a partitioned parent, no column rename in place); Temporal Go SDK;
  ConnectRPC for JSON, gRPC for the core; testcontainers-go for T3; `pgregory.net/rapid` for T1; TLC 2.18
  (`tla2tools.jar` pinned by SHA-256).
- Pre-1.0 protos: breaking changes are allowed and removed fields are not reserved (N190). Field numbers of kept fields
  never change.
