# Review round 8 (after D26): findings and disposition

Three reviews were run against the D26 plan (move backup before the commit point, N169–N178). This file is the
condensed fold of `round-8-correctness.md`, `round-8-postgres.md` and `round-8-api.md` (now deleted).
**Totals: 0 blockers, 10 majors, 20 minors, 11 nits** (41 findings; four majors are the same two defects seen from
two lenses: C8-1 = PG8-2, C8-4 = PG8-1; A8-1 ⊂ PG8-3; C8-3 ≈ PG8-8).

**What was executed.** Correctness lens: `catalog_replicated` as written, on a scratch PG 16 primary with a live
streaming standby, owned by a non-superuser; the N169 restore path `plan_target → freeze_restore(incoming) →
restore_done` then `freeze_delete`/`start_move` on the result; TLC on a scratch `ShardMove.tla` with a lossless
promotion, N172's routing write and the §5.5.1 "(c) finds the catalog at the target ⇒ done" rule. Postgres lens: both
DDL files on PG 16.15 + pgvector 0.8.6, a real standby (`pg_basebackup -R -X stream`), the helper under three owners
and a `pg_temp` spoof, the `namespace_moves` CHECKs and trigger, `c_order` against `pg_constraint`,
`pg_backup_start(fast => false|true)` timed. API lens: `buf lint/build/breaking` (FILE, WIRE, WIRE_JSON) against
`f0afa7d` and every historical tree that touched `proto/`; §4 embeds diffed; 121 §2 interfaces parsed; every
`Test*` token checked against §8; every headline number recomputed; a catalog restore replayed on `r8api_cat`.

**Pattern.** The freeze-then-copy core, the replicated wait and curation held again. Every major sits on the paths
D26 added around the move backup: the backup's own duration (A8-1, PG8-3), its identification (PG8-7), its time-based
floor (C8-9), the failover it does not bound (C8-3, PG8-8), the restore edges that bypass activation's side effects
(C8-1, PG8-2, C8-10), the routing re-derivation that strands a `ready` target (C8-2), the helper every wait depends
on (C8-4, PG8-1, PG8-5), the recovery path the catalog's own index blocks (PG8-4), and one incomplete PG7-4 fix
(C8-5). D27 replaces the move backup by a WAL floor (fewer moving parts) rather than adding terms to it.

## Findings (one line each)

| Id | Sev. | Claim | Evidence |
|---|---|---|---|
| C8-1 | major [regression] | The N169 "ordinary restore" of a target restored before (b″) keeps `move_id` (no restore edge has `move_effect = close`) and never stamps `activated_at`; the namespace can never be deleted (`55006` forever) or moved again, and the catalog row stays `committed` (no `cleaning` without `activated_at`). | Executed on `r8c_shard`: `incoming → frozen/restore → active/3` with `move_id` set; `freeze_delete` 55006; `start_move` refused. = PG8-2. |
| C8-2 | major [regression] | After (c) commits but before Temporal records it, a lossless catalog promotion's reconcile writes `namespaces = (target, e+1, active)` (N172); the retried (c) sees it and ends `done` without (b″): every request routes to a `ready` row forever and `CutoverInProgress` cannot fire. | TLC on a scratch spec with `CatalogPromote`, N172's routing write and the §5.5.1 shortcut: `OneOwner` violated in 16 states (`own = [tgt ↦ ready, src ↦ moved_out]`, `cm = mp = done`). |
| C8-3 | major [regression] | `engramctl shard failover` promotes an asynchronous standby at whatever LSN it replayed; a standby lagging past build + backup + cutover carries a partial copy and no index, and the N169 path activates it; cleanup then deletes the only complete copy. | N169(5) guards `restore --target` only; `RestoreCore` reverts to `bak[s]`, never older than the backup, so the spec cannot express it. ≈ PG8-8. |
| C8-4 | major [fix wrong] | `catalog_replicated` returns false forever: `pg_stat_get_wal_senders()` checks `pg_read_all_stats` on the definer owner, which `catalog_migrate` lacks; no move passes (a″), every lifecycle ack is `UNAVAILABLE`. | Executed with a live walsender: owner non-superuser → `f`; after `GRANT pg_read_all_stats` → `t`. = PG8-1. |
| C8-5 | major | An acknowledged `DeleteTenant` (catalog `deleting` row + tenant intent, namespaces fenced asynchronously) is undone by a catalog restore from backup inside the fencing window: the reconcile reads no intents and finds no `frozen/delete` row; a later shard restore skips the intents because the row is not `deleting`. | N171(3) "repaired from the shards" is false for this write; `Durability_CatalogLossNoBlobFloor` protects only the floor. |
| C8-6 | minor (unverified) | The restore's "catalog first" epoch write is un-awaited; a promotion between it and the shard rewrite leaves the catalog at `e` while the shard reaches `e+1`: ≈ 120 namespaces unroutable until a manual reconcile. The reconcile's rule for an unreadable (restoring) shard is unstated; the spec's `~cdirty` guard on `RestoreCore` hides the overlap. | Interleaving in the review; §7.2.2 "a shard's own reconcile never waits on the catalog's" vs the guard. |
| C8-7 | minor | `restore cleanup-moved-out` covers `done` only; a source restored after the last batch but before `CleanupMove` records `done` resurrects the source copy permanently (`return_move` refuses while rows exist); a catalog restore reverting `done` to `committed` leaves a row no actor drives. | N170(2), §5.5.1 step 10. |
| C8-8 | minor | The reconcile's "unique owner" is undefined for a torn snapshot (source `frozen@e`, target `active@e+1` between `ReadShard`s); (d)'s "already `(target, e+1)`" success is false after a restore at `e+2`. | `ApplyReconcile`'s `CHOOSE`; `02-modules.md` `Cutover` vs N169(4). |
| C8-9 | minor | The PITR floor compares a catalog clock (`move_backup_at = now()`) with shard WAL commit timestamps; skew can admit a target point before the copy's last commit. | N169(5). |
| C8-10 | minor (unverified) | The restore path never sets `moved_in_at`, so N147(3)'s 10-minute watermark guard is skipped for a target activated by `restore_done`. | `shard_schema.sql` trigger sets it on `ready → active` only. |
| C8-11 | minor [regression, doc] | §3.2 still says "epochs rise to the maximum over the shard rows" and "(c) waits for the standby's `replay_lsn`". | `03-data-model.md:81` vs N171/N172. |
| C8-12 | nit | `Derivation.tla`'s `LazyTwinRead` reads `itag[f]` of the hidden fact; the design reads the subject's last `curation_log` row; equal only by tag reuse, and the model lacks the "all live facts purged, log still names the tag" state. | Spec reading. |
| PG8-1 | major [regression] | = C8-4: `pg_stat_get_wal_senders()` masks rows unless the *owner* holds `pg_read_all_stats`; the lint checks the opposite direction; the §8 test passes only because testcontainers apply the DDL as `postgres`. | Executed: three owners × `catalog_replicated('0/0')`. |
| PG8-2 | major [regression] | = C8-1; also `moved_in_at` stays NULL on the restore edges. | Executed as `engram_move`/`engram_admin`/`engram_app`. |
| PG8-3 | major [regression] | The move backup's duration model omits four terms: (1) a spread checkpoint at `pg_backup_start(fast => false)` (pgBackRest default), up to ≈ 27 min at 30 min × 0.9; (2) the archive drain of the HNSW build burst (≈ 1.7 GB per 1 M vectors) against pgBackRest's unset 60 s `archive-timeout`, which fails the backup; (3) the stanza lock held by the 32 GB watcher (fires every ≈ 21 min during a copy), the daily differential and the weekly full; (4) per-file read volume of every 1 GB segment changed on the shard (30–60 % of the footprint). Term (1) alone exceeds the unattended margin. | Measured: 107 s non-fast vs 0.36 s fast on a 5-min-timeout scratch server; the rest from pgBackRest's documented behaviour. ⊃ A8-1. |
| PG8-4 | major [regression] | `--lose-move-ins` cannot start the N123 recovery move: the original row at `committed` blocks `namespace_moves_live_uq`, `committed → rolled_back` is illegal, `committed → cleaning` needs `activated_at` (NULL when (b″) never ran), and where it is set the only exit deletes the one live copy; the runbook never removes the restored partial `incoming` row; the floor also over-refuses move-ins still before (a″). | Executed on the catalog DDL. |
| PG8-5 | minor | `SET search_path = pg_catalog` without `pg_temp` resolves `pg_stat_replication` through the caller's temp schema first: a `TEMP VIEW` as `catalog_admin` made `catalog_replicated('FF/0')` return true. | Executed. |
| PG8-6 | minor [regression] | `NAMESPACE_BUSY` lasts ≥ 24 h after activation (the live index covers `committed`/`cleaning`), not "seconds after (b″)". | `namespace_moves_live_uq` vs N170's gate. = A8-4. |
| PG8-7 | minor | `move_backup_started_at`/`move_backup_at` set two days before `frozen_at` are accepted and the row reaches `committed`; a resumed activity can adopt a watcher differential that started mid-copy. | Executed; no identity ties the backup to the copy. |
| PG8-8 | minor (unverified) | ≈ C8-3: the `ha`-profile failover has no move-backup floor. | §9.6 vs N169(5). |
| PG8-9 | minor | A 24 GB move-target build (DSM, charged to the 112 GB cgroup with the 32 GB `shared_buffers`) leaves ≈ 56–72 GB of cache for a ≈ 74 GB hot set: a miss-rate rise past the 50 k IOPS budget for the ≈ 1 h of a 10 M build, on every namespace of the target. | Arithmetic on N173(2), §9.1. |
| PG8-10 | minor (unverified) | "RTO ≤ 75 min while a move-in lands" assumes a ≈ 10 min differential; a watcher differential reads every file changed since the weekly full (≈ 12 min at 138 GB, ≈ 21 min at 248 GB) while the copy adds 25 MB/s: ≈ 63 GB → ≈ 76 min of replay before base restore and reconcile. | Arithmetic on N166(3), N176. |
| PG8-11 | minor | The §3.7 rows sum to 181.45 GB (× 1.12 = 203, the old figure) at 90 % fill and 137 B/link; "225 GB per 10 M" and "248 GB at the 10 M cap" cannot both hold; recomputed: ≈ 225 GB before the 12 % `ins_seq` term, ≈ 252 GB after. | Recomputation. = A8-11. |
| PG8-12 | nit | §3.7 says "daily full"; §9.3 and N23 say weekly full + daily differential. | Text comparison. |
| PG8-13 | nit | The listed composition is 49 + 2P = 57 at P = 4, not 53 + 2P = 61; the runner's second session (PG7-17), the pgBackRest connection and the exporter are unlisted. | Arithmetic. = A8-9. |
| PG8-14 | nit | `deletion_log.invalidation_op` has no `CHECK (invalidation_op IS NULL OR kind = 'invalidate')`. | DDL. |
| PG8-15 | nit | §8's `TestCatalog_ReplicatedHelperAsAdmin` names role `catalog_api`; the role is `catalog_app`. | Text. |
| PG8-16 | nit | §3.3 and the DDL header describe `shm_size` as a per-build setting; it is fixed at container creation (compose sets 32g). | Text. |
| PG8-17 | nit | The cleanup gate reads `OLD.activated_at`: one statement setting `activated_at` and `cleaning` together is refused; the reconcile must stamp activation in a separate same-state update first; the same two statements let `catalog_admin` backdate the gate (acceptable under admin trust, unstated). | Executed. |
| A8-1 | major [regression] | ⊂ PG8-3 term (1): no `start-fast`, so `MoveBackup` can wait up to ≈ 27 min for a spread checkpoint before reading a block; an unattended 350 k-fact move (deadline ≈ 19 min) rolls back and is re-planned with the same estimate; the shard-delta read volume is also uncounted. | PG 16 low-level backup docs; pgBackRest `start-fast` default `n`. |
| A8-2 | minor [regression] | `catalog.MoveBackups.RecordTimeline` writes a column that does not exist; no §2 catalog method writes `committed_replicated_at`, `rolled_back_replicated_at`, `moved_out_at`, `activated_at`, `ready_at`, `w_final` or `finished_at`, several of which are CHECK inputs: as typed the catalog API cannot take a move to `cleaning`. | §2 vs DDL. |
| A8-3 | minor [regression] | The reconcile cannot re-derive `committed` after a catalog restore to a point before `RecordMoveBackup` without inventing `move_backup_at` and `committed_replicated_at` (CHECKs 11 and 12); `OwnershipObservation` carries neither. | Executed on `r8api_cat`. |
| A8-4 | minor [regression] | = PG8-6; `TestDelete_BusyDuringMove` asserts success seconds after (b″) while the stated check (the catalog's live index) refuses for ≥ 24 h. | §5.4.3 vs §8.4.1. |
| A8-5 | minor [regression] | The commit-then-`UNAVAILABLE` ack is not idempotent without `request_id` (optional on writes): the documented "Retry" of a succeeded `CreateNamespace`/`UpdateTenant` returns `ALREADY_EXISTS`/`ETAG_MISMATCH`. | §4.1.3, §4.1.6. |
| A8-6 | minor [regression] | WIRE_JSON reports `FIELD_SAME_NAME` regardless of reservations (exit 100 on `move_backup_*` against `f0afa7d`), so the N177 gate fails on every recorded rename; "the first tagged tree" does not exist before `v1.0.0`. | Executed. |
| A8-7 | minor | "If week 26 is a hard date, the cut is M1.5's 1 M-fact measurement (−0.5)": the chains are 28.25/28.5/28.5 ew against 26; E2 and E3 set day 201 and are untouched. | Arithmetic on §10.2. |
| A8-8 | minor (unverified) | `CleanupMove` is one idempotent RPC with two transitions; nothing records the batch state, so an operator call during `cleaning` either records `done` with source rows left or must know it. | `admin.proto`, `Closer.Cleanup`, §5.5.1 step 10. |
| A8-9 | nit [regression] | N155's sum "≈ 57 … (61 at P = 4: 53 + 2P)" and its "≤ 8 ms per arm" disagree with §9.1/N176; N164 equates two different 53s. | Text. = PG8-13. |
| A8-10 | nit | `MoveCheckpoint.cutover_step` (field 9, string) was replaced by field 13 of enum type under the same name without reserving it: the protojson hazard A-9 fixed for events, relevant because Temporal encodes proto payloads as protojson. | WIRE_JSON against three historical trees. |
| A8-11 | nit | = PG8-11; 138 GB at 5.5 M is 225 × 0.55 × 1.115, so a ≈ 10 % term is implied but unnamed. | Arithmetic. |
| A8-12 | nit | Parity slips: §9's `W_est` adds "+ blob delta"; RTO arithmetic gives 71–76 min; `proto/README.md` omits `MoveBackupResult`; N177 says the README lists rounds 3–6; §4.5's round-7 rows sit between round-6 rows; the `NAMESPACE_BUSY` comment says "cutover"; `RollbackMove` is missing from the N171(3)/`AckAfterReplay`/§4.1.6 lists and names the internal `MoveFenced`; `Move.cutover_step`'s comment says `CUTOVER` (steps (c)/(b″) run in `COMMITTED`); `Move.cutover_at` maps to no column; `finished_at` is not exposed. | Reading. |

## Disposition (D27, `00-decision-register.md`)

Every round-8 finding maps to a register row. "Fixed by" names the D27 row (N179 to N188) or the row rewritten in
place "(rev. D27)"; "Withdrawn" names the D26 mechanism the decision retires. Round 8 again deletes rather than
patches: the move backup and its four duration terms, its identification columns and its time floor go (N179); the
restore path gains the activation it was missing instead of a special case (N180).

| Finding | Severity | Disposition | Fixed by | Spec / test |
|---|---|---|---|---|
| C8-1 / PG8-2 restore edges keep `move_id`, no `activated_at` | major | **Accepted.** `restore_done` from a row with `move_id` set closes the move (`move_effect = close`) and stamps `moved_in_at`; the restore tool stamps the catalog `activated_at` (same-state update) so the row reaches `cleaning`/`done` through the ordinary gate; `TestMove_TargetRestoredAfterSeal` deletes and re-moves the namespace afterwards. | N180; N169(2), N147(3), N177 rev. | `MoveClosedWhenFinal`; `_RestoreKeepsMove` must fail it |
| C8-2 `ready` target stranded by the routing write | major | **Accepted.** (b″) is unconditional on `target = ready ∧ move = committed`; the mover decides "done" from the target row, never from `namespaces`; the lossless promotion and N172's routing write are modelled; `MoveTargetNotActivated` pages at 60 s. | N180; N169(4), N172 rev. | `CatalogPromote`; `_EndOnRouting` must fail `OneOwner`; `TestMove_ActivateAfterCatalogPromotion` |
| C8-3 / PG8-8 lagging failover below the copy | major | **Accepted; mechanism replaced.** The floor is the LSN `copy_end_lsn`; `SealCopy` requires the target standby's replay ≥ it before (b′); `engramctl shard failover` refuses a standby below the floor of any move-in not `rolled_back`/`lost` and lets it replay the archive first. | N179; N169(2)(5), D5 rev. | `StandbyReplay`/`Failover`; `_LaggingFailover` must fail `NoLossNoDup`; `TestMove_FailoverBelowFloorRefused` |
| C8-4 / PG8-1 helper owner lacks `pg_read_all_stats` | major | **Accepted.** Both helpers are owned by a dedicated `*_stats_reader` role that is a member of `pg_read_all_stats` only; `config lint` asserts the owner's membership and a true result against the live standby; the test runs with the production owner. | N181; N171(1) rev. | `TestCatalog_ReplicatedHelperAsAdmin` restated |
| C8-5 acknowledged `DeleteTenant` undone by a catalog restore | major | **Accepted (ack after fencing).** `DeleteTenant`'s operation reports the delete acknowledged only once every namespace of the tenant is `frozen/delete` (shard truth the reconcile already reads); the lossy catalog-only set is declared. | N182; N171(3), N122, D16 rev. | `Durability_CatalogRestoreInFenceWindow` must fail `AckedDeleteSurvives`; `TestDelete_TenantAckAfterFence` |
| PG8-3 / A8-1 move-backup duration terms | major | **Accepted; mechanism replaced.** No backup inside the window: `SealCopy` = `pg_switch_wal`, archive through `copy_end_lsn` (`pgbackrest check`), standby replay ≥ it; `W_est`'s term is the archive/redo drain of the build WAL (≈ 150 s per 1 M); checkpoint, stanza lock and changed-segment reads vanish. | N179; N173(1) rev. | `TestMove_CutWaitsForSealedCopy`; M1.5 measures `R_archive`, `R_redo` |
| PG8-4 `--lose-move-ins` cannot start the recovery move | major | **Accepted.** Terminal `lost` state (admin, from `committed`/`cleaning`, excluded from the live index, records the restore id); the runbook removes the restored partial row before `Plan`; the floor refuses only move-ins at `cutover` or later. | N183; N123, N169(5) rev. | `TestMove_TargetPITRBelowFloor` |
| C8-6 un-awaited restore epoch write | minor | **Accepted.** The restore's catalog writes wait for `catalog_replicated`; `restore_done` re-asserts the catalog epoch by CAS to `max`; the reconcile skips an unreadable shard and pages `ReconcileIncomplete`; the spec drops `~cdirty` on shard restore actions. | N185; N163(2) rev. | `TestRestore_CatalogWritesReplicated` |
| C8-7 cleanup `done` without the last batch | minor | **Accepted.** The activity that records `done` first re-runs `engram_cleanup_namespace` expecting 0; `restore cleanup-moved-out` covers `cleaning` and `done`. | N184; N170(2) rev. | `TestMove_CleanupGateAtEntry` restated |
| C8-8 torn reconcile snapshot; (d) success | minor | **Accepted.** The reconcile prefers the higher-epoch owner and re-reads on disagreement; (d) succeeds on `shard = target ∧ epoch ≥ e+1`. | N180; N163(2) rev. | — |
| C8-9 time floor vs shard clock | minor | **Accepted; mechanism replaced.** The floor is `(copy_end_timeline, copy_end_lsn)`. | N179 | — |
| C8-10 `moved_in_at` unset | minor | **Accepted** with C8-1. | N180 | — |
| C8-11 §3.2 stale rules | minor | **Accepted.** Regenerated from N163(2)/N171/N172. | N187 | `make gen-docs` |
| C8-12 lazy-twin tag source | nit | **Accepted.** `Derivation.tla` gains `clog[subj]` (the subject's last action); `LazyTwinRead` reads it. | N188 | manifest row |
| PG8-5 `pg_temp` spoof | minor | **Accepted.** `SET search_path = pg_catalog, pg_temp`, schema-qualified view; the spoof is in the test. | N181 | `TestCatalog_ReplicatedHelperAsAdmin` |
| PG8-6 / A8-4 busy for ≥ 24 h | minor | **Accepted.** The API check is "a move in `planned..cutover`, or `committed` with `activated_at IS NULL`"; the shard's 55006 is the authority; the test stands. | N184; N177 rev. | `TestDelete_BusyDuringMove` |
| PG8-7 backup not tied to the copy | minor | **Accepted; columns deleted.** `copy_end_lsn` is read after the last `BuildIndexes` commit; `CHECK (copy_sealed_at > frozen_at)`. | N179 | — |
| PG8-9 build memory vs cache | minor | **Accepted.** `Plan` prefers targets with `hot set + mwm ≤ usable cache`; otherwise caps `mwm` at `usable − hot set` (≥ 5 GB) and the IOPS degradation is stated. | N186; N173(2) rev. | — |
| PG8-10 / A8-12 RTO during a move-in | minor | **Accepted.** ≤ 80 min at the target, ≤ 95 min at the cap while a move-in is landing. | N186; N166(3), N176 rev. | §9.3 |
| PG8-11 / A8-11 footprint rows | minor | **Accepted.** ≈ 250 GB per 10 M (rows regenerated at 224 B/link, × 1.7 B-trees, 12 % `ins_seq` term named); ≈ 138 GB at 5.5 M; ≈ 250 GB at the cap. | N186; D3, N165(3), N176 rev. | `make gen-docs` |
| PG8-12 backup cadence | nit | **Accepted.** Weekly full + daily differential everywhere. | N187 | — |
| PG8-13 / A8-9 connection sum | nit | **Accepted.** 55 + 2P = 63 at P = 4 from the one §9.1 list (runner 2, pgBackRest 1, exporter 2, `engramctl` 2). | N186; N155, N164(5) rev. | — |
| PG8-14 `invalidation_op` CHECK | nit | **Accepted.** | N187 | DDL |
| PG8-15 role name | nit | **Accepted.** `catalog_app`. | N187 | §8 |
| PG8-16 `shm_size` wording | nit | **Accepted.** Fixed at container creation, 32g on shard images. | N187 | — |
| PG8-17 gate reads `OLD.activated_at` | nit | **Accepted.** Activation is a separate same-state stamp, stated; admin backdating is under admin trust, stated. | N180 | — |
| A8-2 catalog API lacks writers | minor | **Accepted.** `catalog.MoveBackups` → `catalog.MoveStamps{Stamp, RecordFloor, RecordReplicated}`; `RecordTimeline` deleted; `TestDeps_EveryRPCHasPath` asserts a writer per non-derived column. | N184; N167 rev. | `TestDeps_EveryRPCHasPath` |
| A8-3 reconcile vs the CHECKs | minor | **Accepted.** The reconcile reads the floor from the target's `floor_lsn` and stamps `committed_replicated_at = reconciled_at` (new column); an unreadable target is skipped and paged. | N184, N185; N163(2) rev. | `TestCatalog_ReconcileDerivesRouting` case |
| A8-5 ack without `request_id` | minor | **Accepted.** `request_id` is required on every replicated-ack method (`INVALID_ARGUMENT` without; CLI and SDKs mint one). | N184; N171(3) rev. | — |
| A8-6 WIRE_JSON gate | minor | **Accepted.** WIRE_JSON runs against the merge base, non-gating, with a generated ignore list from §4.5; README corrected. | N187; N177 rev. | CI |
| A8-7 week 26 unreachable | minor | **Accepted.** The sentence is replaced: week 26 is not reachable by cuts; the committed Phase 2 exit is week 30 after N188 (week 29 with one of N188's two named 0.5 ew cuts). | N186 | §10 |
| A8-8 `CleanupMove` two transitions | minor | **Accepted.** `CleanupMove` takes `committed → cleaning` only; the activity records `done` through `Moves.Advance`; a call on `cleaning` returns the `Move`. | N184; N170(1) rev. | — |
| A8-10 `cutover_step` name reuse | nit | **Accepted.** Field 13 → `cutover_sub_step`, name `cutover_step` reserved, §4.5 row. | N187 | WIRE_JSON |
| A8-12 parity slips | nit | **Accepted.** "+ blob delta" deleted; README rows; rounds "3 to 8"; §4.5 order; `NAMESPACE_BUSY` comment; `RollbackMove` in every ack list, failing `PreconditionFailed{MOVE_PAST_COMMIT}`; `cutover_step` comment; `Move.cutover_at` reserved, `finished_at` exposed. | N187, N184 | — |

**Counts after disposition:** 10 majors closed by decision (six by replacing the move backup with the WAL floor and
closing the restore path's activation, one by a terminal state, one by a role, one by an ack point, one by a
precondition); 20 minors and 11 nits folded. No finding is deferred. The round-9 lens should be `SealCopy` (N179)
against `ShardMove.tla`'s `StandbyReplay`/`Failover` (N188), the restore edges that now close a move (N180), and
the `DeleteTenant` operation's acknowledged point (N182) against §4's lifecycle RPCs.
