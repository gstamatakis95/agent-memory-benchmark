# Round 8 review: API, Go module design, numbers and cross-file parity (after D26)

Lens: `proto/` (lint, build, breaking FILE and WIRE_JSON), the §4 embeds, the §2 Go API, the move-step parity between
§5.5, §3/SQL and the §9 runbook, the numbers repeated across files, test-name definitions, and stale references to
withdrawn mechanisms. Baseline for "previous round" is commit `f0afa7d` (round-7 disposition, before the D26 edits).

## What was executed

- `buf lint` and `buf build` on `proto/`: both exit 0.
- `buf breaking` (FILE, as configured) against `f0afa7d`: 7 lines, all expected. The 3 deletions are reserved
  (`Move` 16/26, `MoveInput` 16). FILE reports them anyway because `FIELD_NO_DELETE` ignores reservations. The 4
  other lines are the recorded pre-1.0 rename of `Move` 17/25 to `move_backup_*`.
- WIRE against `f0afa7d`: exit 0.
- WIRE_JSON against `f0afa7d`: only the 17/25 rename.
- WIRE_JSON against **every historical tree that touched `proto/`** (a loop over `git log -- plans/engram/proto`):
  - no unreserved name remains apart from one, `MoveCheckpoint.cutover_step` (field 9). See A8-10.
  - `DocumentDeleted` 2–6 and 8–15 now reserve both numbers and names, and the names match the first tree's.
  - every other line is a rename listed in §4.5's table (round 4 `*_bytes`, round 5 `expires_at`, round 7 `move_backup_*`).
- §4 embeds: `common.proto` (lines 334–657) and `memory.proto` (lines 787–1516) are byte-identical to `proto/`. Every
  `sections/*.md` file is verbatim inside `PLAN.md`; the register is verbatim apart from its H1.
- §2: 121 interfaces parsed from the Go blocks, including one-line interfaces, with embedded interfaces resolved.
  None has more than 5 methods:
  - `Copier` has 3 and `Readier` has 3.
  - `Moves` has 5, `MoveBackups` 2, `Replication` 2, `Closer` 3 and `Orchestrator` 4.
- Test names: every `Test[A-Z]…_…` token in `sections/`, the register, `proto/`, `sql/` and `formal/` is defined in
  §8's body. The only exception is in the change tables. The 11 retired names the register cites are all on §8's
  retired-names line.
- Stale terms grepped: `Rerun`, `rerun_move`, `reconcile_in`, `ReconcileIn`, `PreVerify`, `CleanupOptions`,
  `target_backup_*`, `replay_floor_mirror`. They appear only in:
  - change tables and reserved lines;
  - N161's marked historical text;
  - `ShardMove.tla`'s "rejected repair" comments.
- Scratch DB `r8api_cat` (catalog DDL applied unmodified): replayed a catalog restore that the reconcile must move
  forward (A8-3).
- Recomputed:
  - critical path: 2+25+60+30+1+90+3+5 = 216, +20 = 236 ms, headroom 64 ms;
  - Erlang-C at c = 32 and A = 24 with h ≈ 70 ms per arm: per arm p95 4.4 ms, max over six arms p95 19.9 ms;
  - connections 32+8+2P+2+4+1+1+1+2+2 = 61 at P = 4;
  - W_est per 1 M facts: 1000 s copy + 408 s build (1.225 M vectors) + 125 s backup + 60 s verify = 26.6 min;
    10 min ≈ 377 k facts; 8 h / 1.5 ≈ 12.05 M facts;
  - shard counts: 1 B / 5.5 M = 182 shards, / 32 = 5.7 cells, / 4 = 45.5 → 46 shard hosts;
  - roadmap: Phase 1 = 40.5 + 7.25 + 2.5 = 50.25; committed 85.25; total 124.5;
  - Gantt: E1 200 days, E2 201, E3 201; MVP on day 165 (week 24), Phase 2 exit on day 201 (week 29).

Everything in that list agrees across `sections/`, `README.md`, the register and `PLAN.md`. The findings below are
the exceptions.

## Findings (ranked)

### A8-1 — major [regression] — `MoveBackup` has no checkpoint term: as specified it can take most of a `checkpoint_timeout` before it reads a block, which breaks the unattended-move deadline

**Location.**
- N169(1)
- N173(1)
- §5.5.1 step 7
- §9.2 backup table (`checkpoint_timeout = 30 min`, `checkpoint_completion_target = 0.9`, §9.1 GUC table)
- `Readier.MoveBackup`

**Claim.**
- "a pgBackRest **incremental** backup of the target … ≈ 25 KB per fact at ≈ 200 MB/s, ≈ 2 min per 1 M facts,
  inside `W_est`"
- "The rebalancer starts a move unattended only at `W_est ≤ 10 min`"
- the deadline is "`frozen_at + max(1.5 × W_est, W_est + 10 min)`"

**Evidence.**
- A pgBackRest backup starts with `pg_backup_start(label, fast)`. pgBackRest passes `fast = start-fast`, whose default
  is `n`.
- With `fast = false` PostgreSQL performs a **spread** checkpoint. The PG 16 docs, "Making a Base Backup Using the Low
  Level API", say "the I/O required for the checkpoint will be spread out over a significant period of time … see
  checkpoint_completion_target". pgBackRest's own description of `start-fast=n` is "the backup will start after the
  next regular checkpoint".
- With the plan's own settings (30 min, 0.9), that spread checkpoint can last up to ≈ 27 min.
- The plan never sets `start-fast`, and `W_est`'s backup term is `bytes / R_backup` only.
- Example, an unattended 350 k-fact move:
  - W_est ≈ 9.3 min, so the deadline is ≈ 19.3 min after the freeze;
  - copy + build + verify take ≈ 9 min;
  - `MoveBackup` then waits for a checkpoint of up to ≈ 27 min;
  - the deadline fires before (b′), `MoveWindowExceeded` rolls the move back, and the rebalancer re-plans the same
    move with the same estimate.
- The `bytes / R_backup` term also counts only the namespace's bytes. An incremental of the target **shard** also
  reads every block changed on that shard since its last backup. The 32 GB / 6 h watcher bounds that delta, but it
  is not counted.
- For large moves the 1.5× margin absorbs both effects. For the unattended class, the `+10 min` margin does not.

**Recommendation (N173 / N169 amendment).**
- `MoveBackup` runs with `start-fast=y` (an immediate checkpoint on the target; the SLO may degrade, as during the rest
  of the window).
- `W_est` gains a checkpoint term (the dirty-buffer flush at the measured write rate) and a shard-delta term
  (`engram_pg_wal_since_backup_bytes`-derived changed bytes / `R_backup`).
- M1.5 measures both.
- `TestMove_CutWaitsForMoveBackup` asserts that `MoveBackup` begins within seconds of being called.

### A8-2 — minor [regression] — `catalog.MoveBackups.RecordTimeline` writes a column that does not exist, and no catalog method writes the replicated and step stamps the CHECKs require

**Location.**
- §2 `catalog.MoveBackups` and `catalog.Moves`
- `catalog_schema.sql` `namespace_moves`

**Claim.**
- "`RecordTimeline(ctx, m, systemID uint64, timelineID int32)` — the target's system_identifier/timeline at the backup
  (N123)"
- "`Moves` … five methods; the rest is MoveBackups, Replication"

**Evidence.**
- `namespace_moves` has `source_system_id` and `source_timeline_id` only. No target system id or timeline column
  exists, and N169 does not mention one.
- `RecordTimeline` is new in D26: the round-7 `MoveBackups` had `RecordTargetBackup` and `CleanupGate`.
- Conversely, these columns have no writer in any §2 catalog interface:
  - `committed_replicated_at` and `rolled_back_replicated_at` (N171(2));
  - `moved_out_at`, `activated_at`, `ready_at`, `w_final` and `finished_at`.
  `Advance` is documented to stamp `frozen_at`/`freeze_deadline` only, and `Replication` is read-only.
- Several of these are CHECK inputs:
  - `moved_out_at` needs `committed_replicated_at`;
  - `cleaning` needs `moved_out_at`;
  - the cleanup gate needs `activated_at`;
  - `ready_at` needs `w_final`.
- As typed, the catalog API cannot take a move to `cleaning`.

**Recommendation.**
- Replace `RecordTimeline` with a `Stamp(ctx, m, StampKind, time.Time)` (or `RecordReplicated` and `RecordStep`) on
  `MoveBackups`, which stays ≤ 5 methods.
- Either add `target_system_id`/`target_timeline_id` columns, if the backup's timeline is meant to be checked, or drop
  the method.
- `TestDeps_EveryRPCHasPath` should also assert that every non-derived `namespace_moves` column has a writer.

### A8-3 — minor [regression] — the reconcile cannot re-derive `committed` after a catalog restore from backup without fabricating `move_backup_at` and `committed_replicated_at`

**Location.**
- `catalog_schema.sql`: `CHECK (state NOT IN ('cutover','committed','cleaning','done') OR move_backup_at IS NOT NULL)`
  and `CHECK (moved_out_at IS NULL OR committed_replicated_at IS NOT NULL)`
- N163(2) and N172 ("a source `moved_out` ⇒ the move is at least `committed`")
- `Reconciler.FromShards`

**Evidence (executed on `r8api_cat`).**
- Setup: a move row at `copied` with `move_backup_at` NULL. This is the state a backup taken between `MoveBackup` and
  `RecordMoveBackup`, or a 60 s RPO loss, leaves behind.
- `UPDATE … SET state = 'cutover'` fails with `namespace_moves_check11`.
- Setting `committed`, `committed_at` and `moved_out_at` fails with `namespace_moves_check12`.
- The row reaches `committed` only when the reconcile **invents** `move_backup_at` and `committed_replicated_at`.
- `OwnershipObservation` carries neither value. pgBackRest's `info` for the target does carry the backup time, but no
  step reads it.
- A "no catalog move ⇒ `done`" derivation hits the same CHECKs on INSERT.
- The declared lossy path ("repaired from the shards by the reconcile", N171(3)) therefore stops at a CHECK violation
  for exactly the moves it exists to repair.

**Recommendation (N163(2) amendment).**
- Either the reconcile reads `move_backup_at` from the target's pgBackRest `info` (label prefix `move-{move_id}`) and
  stamps `committed_replicated_at = now()` after its own `catalog_replicated` wait,
- or the CHECKs exempt rows with `reconciled = true` (a new column).
- Add a `TestCatalog_ReconcileDerivesRouting` case: catalog restored to before `RecordMoveBackup`, with source
  `moved_out`.

### A8-4 — minor [regression] — `DeleteNamespace` is `NAMESPACE_BUSY` for ≥ 24 h after activation, yet the test asserts that it succeeds seconds after (b″)

**Location.** §5.4.3 step 1, `TestDelete_BusyDuringMove` (§8.4.1), N177.

**Claim.**
- "a move in progress → `ABORTED` + `OperationConflict{NAMESPACE_BUSY}` with `RetryInfo 30 s` (**the catalog's
  partial unique index is the check**; … `move_id` clears at (b″))"
- The test expects: "`move_id` clears at activation, so the delete succeeds seconds after (b″)".

**Evidence.**
- `namespace_moves_live_uq` is `WHERE state NOT IN ('done', 'rolled_back')`, so a move is "live" through `committed`
  and `cleaning`.
- `committed → cleaning` is gated at `activated_at + 24 h`, and `done` comes after the source batches.
- With the catalog index as the check, the delete is refused for ≥ 24 h plus the cleanup time.
- `DeleteTenant` re-resolves and retries for the same span.
- Only the shard-side `move_id` check clears at (b″).

**Recommendation.**
- The API check is "a live move in `planned … cutover`", i.e. the catalog `namespaces.state ∈ {moving, frozen}`.
- Alternatively, state that deletes wait for `done` and change the test.

### A8-5 — minor [regression] — the commit-then-`UNAVAILABLE` ack rule is not idempotent for lifecycle writes sent without `request_id`

**Location.**
- N171(3)
- §4.1.6 `UNAVAILABLE` row ("Retry; the retry finds the row (idempotent by key)")
- §4.1.3 (`request_id` "Optional on writes … documented, not rejected")

**Evidence.**
- Under N171(3) a `CreateNamespace`, `CreateTenant`, `UpdateNamespace` or `UpdateTenant*` call commits and then
  answers `UNAVAILABLE` whenever the standby lags past the wait. During a standby rebuild (minutes, N171(4)) that is
  every such call.
- A client that omitted `request_id` (allowed) follows the documented action, "Retry", and gets the wrong answer for a
  write that succeeded:
  - `CreateNamespace` → `ALREADY_EXISTS{IDEMPOTENCY_KEY_REUSED}` (name taken);
  - `UpdateNamespace`/`UpdateTenant` with `etag` → `PreconditionFailed{ETAG_MISMATCH}`.
- The interceptor cannot tell this apart from a real conflict.

**Recommendation.** Make `request_id` **required** on the N171(3) methods (`INVALID_ARGUMENT` without it; the CLI and
SDKs mint one), and state this in §4.1.3 and `RequestMeta`'s comment.

### A8-6 — minor [regression] — the N177 WIRE_JSON gate does not do what `proto/README.md` says it does

**Location.**
- `proto/README.md`: "CI also runs `buf breaking --config WIRE_JSON` against the first tagged tree (**renames with
  reserved names pass**; N177)"
- §4.5's last paragraph

**Evidence (executed).**
- WIRE_JSON reports `FIELD_SAME_NAME`/`FIELD_SAME_JSON_NAME` regardless of reservations. Against `f0afa7d` it fails
  on `move_backup_*` (exit 100) although `target_backup_*` are reserved.
- §4.5 says the opposite ("it reports a rename … as the break this table lists").
- §4.5 also says "No release tag exists before `v1.0.0`". So "the first tagged tree" does not exist during the period
  this check was added for, which is exactly the pre-1.0 period where A7-7 occurred. After v1.0.0, renames are
  forbidden anyway.

**Recommendation.**
- Run WIRE_JSON against the plan branch's merge base (the same ref as FILE), non-gating, with the §4.5 table as its
  allow-list. A generated `buf.breaking.ignore` of the listed renames is enough.
- Fix the README sentence.

### A8-7 — minor — "If week 26 is a hard date, the cut is M1.5's 1 M-fact measurement (−0.5)" cannot reach week 26

**Location.** §10.2 Totals paragraph.

**Evidence.**
- The chains are E1 28.25, E2 28.5 and E3 28.5 ew against 26.0 each. Week 26 needs cuts of 2.25, 2.5 and 2.5 ew
  (7.25 in total).
- The only cut named is −0.5 on E1's chain. E2 and E3, which set day 201, are untouched, so the Phase 2 exit stays in
  week 29.
- In round 7 the list also named M3.8, which was already outside the committed scope.
- Not previously reported.

**Recommendation.** Either name ≈ 2.5 ew of cuts on each of E2's and E3's chains, or replace the sentence with "week 26
is not reachable by cuts; the committed date is week 29".

### A8-8 — minor (unverified) — `CleanupMove` is one RPC with two transitions, and nothing says how the second knows the batches ended

**Location.**
- `admin.proto` `CleanupMove`
- `Orchestrator.Cleanup`
- `Closer.Cleanup`
- §5.5.1 step 10

**Claim.**
- "takes `committed -> cleaning` under the 24 h gate … and records `done` after the last batch"
- `Closer.Cleanup` "… then the CleanupMove RPC records done"

**Evidence.**
- The same idempotent RPC must take `committed → cleaning` on one call and `cleaning → done` on a later one.
- The operator scope (`engram.operator`) may also call it. An operator call during `cleaning` either records `done`
  while source rows remain or must know the batch state.
- The batch state is not in the catalog (no column) and not in `CleanupMoveRequest`. The `done` CHECK requires only
  `finished_at`.

**Recommendation.** `CleanupMove` only takes `committed → cleaning`. The cleanup activity records `done` through
`catalog.Moves.Advance` after `engram_cleanup_namespace` returns 0 rows. The RPC on a `cleaning` row returns the
current `Move`.

### A8-9 — nit [regression] — N155 still has a connection sum that disagrees with its own parenthesis

**Location.** N155 (rev. D26).

**Claim.** "… direct `engram_subject` 2 per API process, admin 2, move 4, relay 1, migrate 1, runner 1 ≈ **57** of
`max_connections` 100 (**61** at P = 4 API processes per shard: 53 + 2P)"

**Evidence.**
- 32+8+8+2+4+1+1+1 = 57 at P = 4, not at some other P. The missing 4 are exporter 2 and `engramctl` 2, which §9.1 and
  N176 count.
- N155 also keeps "per-arm `pool wait` ≤ 8 ms p95", where N176 and §9.1 say ≈ 4.5 ms.
- N164's "32 + 8 + 4 + … ≈ 53 is replaced by 53 + 2P" equates two different 53s: the old one includes subject 4, the
  new one includes exporter and `engramctl` instead.

**Recommendation.** Restate N155 with the §9.1 list.

### A8-10 — nit — `MoveCheckpoint.cutover_step` (field 9) is the one withdrawn field whose name is not reserved

**Location.** `workflow.proto` `MoveCheckpoint`.

**Evidence.**
- WIRE_JSON against three historical trees: "field 9 … deleted without reserving the name `cutover_step`".
- The name moved to field 13 with an enum type (string `"a"…"d"` → `CutoverStep`). This is the same protojson hazard
  A-9 fixed for events.
- It is relevant because Temporal's default Go converter encodes proto payloads as protojson.
- It is not listed in §4.5's round-3 row.

**Recommendation.** Rename field 13 to `cutover_sub_step`, reserve `cutover_step`, and add a §4.5 row.

### A8-11 — nit — the footprint table does not sum to its stated total, and 225/248 GB at 10 M disagree without an unstated overhead

**Location.** §3.7 footprint table, N176.

**Evidence.**
- The rows sum to ≈ 181.5 GB, the D25 figure, while the stated total is "≈ 201 GB per 10 M".
- `fact_links` still shows PK 21 GB + reverse 20 GB, which is 137 B per link, not N176's 224 B.
- "≈ 225 GB per 10 M facts" and "≈ 248 GB at the 10 M hard cap" are the same quantity unless a ≈ 10 % term (WAL,
  bloat) is meant. 138 GB at 5.5 M is 225 × 0.55 × 1.115.

**Recommendation.** Regenerate the table rows from N165's fill (`make gen-docs`) and name the 10 % term.

### A8-12 — nit — further small parity slips

- **§9 W_est formula.** §9's "Window estimate" adds "+ blob delta". N173, §5.5.1, `admin.proto`, `workflow.proto` and
  the DDL comment do not.
- **RTO arithmetic.** "≤ 75 min while a move-in is landing" gives 5–10 + 1.2 × 47 + ≤ 10 = 71–76 min. The upper end is
  76.
- **proto README.** The `workflow.proto` row omits `MoveBackupResult`. §4.2 lists it.
- **N177 vs README.** N177 says "`proto/README.md` lists rounds 3 to 6". The README now (correctly) says 3 to 7.
- **§4.5 row order.** The changelog puts the three round-7 rows between the round-6 rows.
- **`NAMESPACE_BUSY` comment.** It still reads "a delete or move **cutover** in progress". The busy span is now the
  whole open move.
- **RollbackMove ack list.** `RollbackMove` acknowledges a replicated catalog outcome (its comment says it waits for
  the standby). It is missing from N171(3)'s list, from `catalog.AckAfterReplay`'s list and from the §4.1.6
  `UNAVAILABLE` row. It also names the internal `MoveFenced` as its failure.
- **Move `cutover_step`.** The comment "Set while state is CUTOVER" is wrong: steps (c) and (b″) run in `COMMITTED`.
  `Move.cutover_at` maps to no catalog column. `finished_at`, which the 28-day blob GC derives from, is not exposed.

## Counts

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 1 | A8-1 [regression] |
| minor | 7 | A8-2, A8-3, A8-4, A8-5, A8-6 [regression]; A8-7; A8-8 (unverified) |
| nit | 4 | A8-9 [regression], A8-10, A8-11, A8-12 |

## Verdict

**The API and contract surface after D26 is sound.**
- `buf` is clean, and the breaks are exactly the recorded pre-1.0 rename.
- Every withdrawn number is reserved, and every withdrawn name is too, except one.
- The embeds are identical, every §2 interface has ≤ 5 methods, and every cited test exists or is listed as retired.
- The headline numbers agree in every file.

**The remaining problems sit at the seams D26 added.**
- The move backup needs a checkpoint term, without which unattended moves miss their deadline (A8-1).
- The catalog Go API is missing writers for the stamps its own CHECKs require (A8-2).
- The reconcile cannot satisfy those CHECKs after a catalog restore (A8-3).
- The delete-during-move check contradicts its own test (A8-4).
- The new commit-then-`UNAVAILABLE` ack is only idempotent with a key (A8-5).

A8-1 needs a register amendment to N169/N173. Everything else is a fold.

## Not verified

- The checkpoint timing of `MoveBackup` on the real image. A8-1 rests on the PostgreSQL and pgBackRest documented
  defaults and the plan's GUCs. It was not measured.
- Whether the shard-delta term in A8-1 matters at the measured daily change rate.
- TLC was not re-run; `formal/tla/results` was not compared with `EXPECT` in this lens.
- The shard DDL's ownership edges (`freeze_restore` from `incoming`/`ready`) were not exercised in this lens.
- Lean was not built.
