# Round 7 review: PostgreSQL and operations (D25: N160 to N168)

Lens: the freeze-then-copy move schema, the curation subject, the filtered and graph arms, sizing, WAL, backup and RTO, the asynchronous catalog standby, and the purge-pause key.

**Method.** `sql/catalog_schema.sql` and `sql/shard_schema.sql` were applied unmodified to my own databases `r7pg_cat` and `r7pg_shard` on the scratch PG 16.15 with pgvector 0.8.6 (both parse with pglast and apply cleanly). On those databases I ran the catalog move state machine, the ownership edges as `engram_move` and `engram_admin`, `engram_seq_advance`, `engram_move_indexes_valid` and `engram_cleanup_namespace`. I also ran:

- a 60 k-vector partial HNSW on the real `fact_vectors` partition, with the real arm SQL under custom and generic plans;
- the exact-path CTE;
- a 300 k-row `fact_links` load through the TEMP-table upsert, with WAL measured;
- the VerifyFrozen hash over that load;
- the `UNION ALL` hop, before and after `VACUUM`;
- `pg_stat_replication` read as `catalog_admin`, with a real walsender from `pg_receivewal`.

I recomputed the sizing, the WAL figures and the Erlang figures. The prior dispositions in reviews/round-1…6 were grepped, and nothing below repeats one.

---

## Major

### PG7-1 (major, [regression]): the re-run path conflicts with the restore's epoch step, so a moved-in namespace restored to before its activation gets stuck

- **Where.** §5.5.5 step 1 (re-run bullet) and step 2; §9.3 step 4; N161(2); the `reconcile_in` and `rerun_move` edges.
- **Claim.** Step 2: "For every namespace the catalog lists on S **except** those whose open move is `committed` … a catalog transaction sets `epoch = e + 1, state = 'restoring'` … the shard's `namespace_ownership` is then rewritten to the catalog value and brought up `frozen/restore`." N161(2): re-run, then "`reconcile_in: incoming → ready → active` (admin edges, allowed only while the catalog move is `committed`/`cleaning`)".
- **Evidence.** §5.5.1 step 9 sets `namespace_moves.state = 'cleaning'` at `Restart`, seconds after (b″) and (d). The re-run case is "activation happened within the last minute", so the move is normally already `cleaning`, and step 2 does not exclude `cleaning`. Step 2 therefore:
  - writes catalog epoch e+2 / `restoring` first, then
  - tries to bring the shard row (`incoming` or `ready`, epoch e+1) to `frozen/restore`.

  Executed on `r7pg_shard` as `engram_admin`:
  - `ready → frozen(restore)` raises `illegal ownership transition ready(<NULL>)/2 -> frozen(restore)/2`.
  - Bumping the epoch of a `ready` row raises `illegal ownership transition ready/2 -> ready/3`. No edge out of `incoming` or `ready` changes the epoch; `reconcile_in` is `same`.

  The result is a catalog at e+2 and a shard row that re-activates at e+1. Every request carrying the catalog epoch then fails the fence with `WrongShardOrEpoch`, `restore_done` (`frozen/restore → active`) never applies to the row, and the namespace stays unavailable.

  Separately, workflows that `Restart` launched at e+1 on the lost timeline are never fenced, because the re-run keeps e+1. This is the purpose step 2 serves for every other namespace: "every execution that started before the restore carries epoch e and fails its next fence".
- **Recommendation (decision level).**
  - Step 2 and §9.3 step 4 should exclude a namespace whose target row is `incoming`/`ready` under a `committed`/`cleaning` move. These namespaces are handled by the re-run.
  - Give the re-run its own epoch bump. The second `reconcile_in` edge (`ready → active`) should use rule `greater`, with the catalog epoch written first (catalog e+2, then the shard row e+2 on activation). Lost-timeline executions are then fenced exactly as in step 2.
  - Add `ShardMove_TgtRestore` coverage with the move at `cleaning`.

### PG7-2 (major, [regression]): the replicated-LSN wait reads columns that `catalog_admin` cannot see, so step (c) never runs

- **Where.** N163(3); §5.5.1 step 8; the `catalog_schema.sql` header and grants (lines 482–494).
- **Claim.** "the mover … waits until the standby's `replay_lsn ≥` the CAS's commit LSN (`pg_stat_replication`…)".
- **Evidence.** `catalog_admin`, the role the MoveService uses, has no `pg_read_all_stats` or `pg_monitor` membership, and nothing in the schema or in §9 grants it. PostgreSQL nulls every `pg_stat_replication` column except `pid` for roles without `pg_read_all_stats`.

  Executed with a live walsender (`pg_receivewal` against port 55432):
  - as superuser: `state = streaming`;
  - as `catalog_admin`: `pid` present, `state`, `flush_lsn` and `replay_lsn` all NULL.

  `replay_lsn >= $commit_lsn` is therefore NULL forever. Every move stalls at `committed` and `CatalogStandbyDown` pages, or the code treats NULL as down: either way no move ever completes. `TestMove_CutWaitsForReplicatedCommit` would catch it only if run under the production role.
- **Recommendation.**
  - Add `GRANT pg_read_all_stats TO catalog_admin`, or better a `SECURITY DEFINER` function `catalog_standby_replayed(lsn) RETURNS boolean` owned by `catalog_migrate`, granted to `catalog_admin` only. It should return false when no streaming standby exists.
  - Add the grant to `config lint`.
  - Separately, state how the commit LSN is obtained. PG 16 has no "commit LSN of my transaction" function; `pg_current_wal_insert_lsn()` read after `COMMIT` is a valid upper bound, so say so.

### PG7-3 (major, [regression in exposure]): reconcile raises the catalog epoch to the target's `incoming` epoch, which breaks routing and the CAS of any open move

- **Where.** N163(2), §5.5.5 step 0, §3.x: "every namespace's catalog epoch is raised to `max(epoch over its shard rows)`".
- **Evidence.**
  - During a move the target holds `incoming` (or `ready`) at e+1, while the owner is the source at e (`active` at Plan, `frozen/move` under the freeze).
  - The rule takes the max over all rows, so the catalog becomes `(source, e+1, frozen)`.
  - The API then sends epoch e+1 to the source, whose fence requires `epoch = $epoch`. Reads and writes fail `WrongShardOrEpoch`, and the 5 s re-resolve loop returns the same answer.
  - The CAS (a″) "requires the catalog `namespaces` row to still be `(source, e, frozen)`", so it fails with `MoveFenced`. The rollback then thaws the source at e under a catalog at e+1, so the namespace stays unroutable.
  - (d) `WHERE epoch = e` also never matches.

  The rule dates from N146(2), where it ran only after a rare catalog restore. N163 runs it on every promotion, and D25's moves stay `frozen` for up to 4 h with an `incoming` target the whole time. A promotion during any large move is now the expected case, not an edge case.
- **Recommendation.**
  - Raise the epoch only from owner rows: `active`, `frozen/*`, and a `moved_out` row's `target_epoch`, never an `incoming`/`ready` row.
  - When the reconcile re-derives `committed` from a source `moved_out`, set `(target, target_epoch)` exactly as (d) would.
  - Add `ShardMove_CatalogLossDuringFreeze.cfg`: promotion while `frozen`, then `OneOwner` and routing liveness.

### PG7-4 (major, [regression]): an acknowledged `DeleteTenant` can be lost on an asynchronous catalog promotion, and the reconcile has no rule to bring it back

- **Where.** N163(1)(3), R44, §5.4.4 (`DeleteTenant`), §9 ND-15.
- **Claim.** "deletes, epoch bumps and lifecycle writes do not wait (their shard-side records — `frozen/delete`, the intent object, the ownership epoch — are what the reconcile re-derives them from)". R44: "Everything the catalog arbitrates is also on the shards."
- **Evidence.**
  - `DeleteTenant` acks after the catalog commit `tenants.state = 'deleting'` (the read barrier) and one tenant-level intent object.
  - The per-namespace `freeze_delete` runs later, asynchronously in `TenantDelete`.
  - A promotion inside the replication lag reverts the tenant to `active`. The N163(2) rule list only derives `deleting` from a shard's `frozen/delete`, and nothing reads tenant-level intents.
  - Until the workflow reaches each namespace, the acknowledged-deleted tenant serves reads and writes again, and `CreateNamespace` under it succeeds. A namespace created in that gap is not in a list the workflow took before it existed.
  - The workflow's final `tenants.state = 'deleted'` also violates `CHECK ((state IN ('deleting','deleted')) = (delete_operation_id IS NOT NULL))` on the reverted row.

  Smaller cases of the same false premise:
  - `CreateTenant`, `UpdateTenant(Limits)` and `CreateNamespace` metadata (name, model, config) exist only in the catalog. A reverted `CreateNamespace` leaves an `active` ownership row that the reconcile cannot turn back into a full `namespaces` row.
  - The catalog-level `idempotency_keys` row is lost too, so a retry mints a second namespace id.
- **Recommendation (decision level).** Either:
  - make the reconcile list `_control/deletes/{tenant}/` tenant-level intents and re-apply `deleting` together with the operation id and timestamps recorded in the object; or
  - give the delete acks (`DeleteTenant`, and `DeleteNamespace`'s catalog write) the same bounded replicated-LSN wait as (c), failing `UNAVAILABLE` on timeout.

  Either way, R44's "everything is on the shards" should be restated as "routing, moves and deletes are", and catalog-only metadata should get its own RPO statement.

### PG7-5 (major, [regression]): a second `Invalidate` of an already-hidden subject splits the tag, so `Restore` is not exact

- **Where.** N162(2); §5.4.5 `Invalidate`; §3.x `CommitChunk` re-application (line 1153); `Derivation.tla` `Invalidate`.
- **Claim.** "`Restore` is exact by construction: the set it removes is the set the invalidation (and its lazy twins) wrote."
- **Evidence.**
  1. `Invalidate(f)`, operation I1, hides f and twin t, stamped I1, and writes `curation_log(operation_id = I1, action = invalidate)`.
  2. A second `Invalidate(f)` is "not silent". It writes its own `deletion_log` row and intent, and its own `curation_log` row with operation_id I2 (§3.x: "curation_log rows with the same `operation_id`", meaning the marker's). The `fact_hidden` inserts conflict and keep I1.
  3. A re-extraction creates twin u. `CommitChunk` re-applies "the LAST action per (document_id, content_hash)" stamped with "the ORIGINAL invalidation's operation_id" taken from `c.operation_id` of that last row, so u is stamped I2.
  4. `Restore(f)`: `engram_restore_resolve` yields I = I1 and deletes {f, t}.
  5. u stays hidden under I2. The `curation_log(restore)` row then only stops future re-application.

  The second conjunct of `RestoreExact` is violated: u is hidden, f is not, and they belong to one group. The model cannot see this because `Invalidate(f)` is guarded by `f ∉ hidden`, so the repeated call is not modelled. The SQL's "last action" source and the spec's `itag[f]` source also differ.
- **Recommendation.**
  - When the subject already has `fact_hidden(invalidate)` rows, the new `Invalidate` reuses their `invalidation_op` for its `curation_log` row and for any newly hidden twin. Equivalently, the lazy re-application takes the tag from the subject's existing `fact_hidden` rows rather than from the last `curation_log` row.
  - Model the repeated `Invalidate`: drop the `f ∉ hidden` guard and keep the no-visibility-change semantics. Add a must-fail `Derivation_LazyTagFromLastLog.cfg`.

### PG7-6 (major, [regression]): W_est assumes linear build time, and namespaces above about 2.1 M vectors per table cannot be moved inside the 4 h window

- **Where.** N160(1)(5), §5.5.1 step 1, §9.5 window estimate, §9.2 build table, §9.5 capacity signals.
- **Claim.** "`R_build` ≈ 3,000 vectors/s … ≈ 25 min per 1 M live facts"; "the default maximum window is 4 h"; "one namespace > … 2 M facts (the single-build memory bound) → candidate for a move".
- **Evidence.** The plan itself says builds take `maintenance_work_mem = 2.4 KB × vectors`, capped at 5 GB: in memory up to about 2.1 M vectors, then "≈ 3 ms per on-disk insert". My build of 60 k vectors with 3 workers on a 4-core box ran at about 1.7 k/s, consistent with the in-memory rate.

  For the 5.5 M-fact namespace the brief asks about:

  | Term | Value |
  |---|---|
  | W_est = 5,500 s copy + (5.5 M + 0.55 M chunk + 0.28 M observation vectors) / 3,000 ≈ 2,100 s | ≈ 2.1 h + verify |
  | Deadline = 1.5 × W_est | ≈ 3.2 h |
  | Actual `fact_vectors` build: 2.1 M × 0.33 ms + 3.4 M × 3 ms | ≈ 700 s + 10,200 s ≈ **3.0 h** |
  | Actual window: 1.5 h copy + 3.0 h + other builds | **≈ 4.6 h** |

  The actual window exceeds both the deadline and the 4 h ceiling, so every attempt ends in `MoveWindowExceeded`. Two related problems:
  - The "> 2 M facts → candidate for a move" signal and the "hard cap → forced moves of the largest namespaces" rule pick exactly the namespaces that cannot be moved. Moving a namespace also does not reduce its build size.
  - Before D25 the HNSW was built before the freeze (`AwaitIndexes`), so a slow build did not extend the read-only window. N160 moved it inside the window.

  For namespaces ≤ 2 M facts, W_est is credible:
  - 2 M facts gives W_est ≈ 50 min. The build table (2 M ≈ 11–12 min) matches R_build.
  - My TEMP-table upsert of 300 k `fact_links` rows wrote 168 MB of WAL (≈ 16.8 KB per fact at 30 links per fact). With the other tables that suggests ≈ 25 KB per fact, so the 25 MB/s pacing caps R_copy at about 1,000 facts/s. The planning value is at the limit, not conservative, and §9's "1 M-fact move ≈ 18–20 GB WAL" is probably low by 25–40 %.
- **Recommendation.**
  - Make W_est piecewise: in memory up to `5 GB / 2.4 KB`, 3 ms per vector beyond.
  - `Plan` refuses (`MovePrecondition{BUILD_EXCEEDS_WINDOW}`) when W_est exceeds the 4 h cap.
  - Either lift the `maintenance_work_mem` cap for move-target builds (13 GB for 5.5 M on a 128 GB instance, with `shm_size` raised to match), or state that namespaces above the in-memory bound move only by a dedicated-shard procedure such as a backup-restore of a dedicated shard.
  - Remove "> 2 M facts" from the move-candidate signals.

### PG7-7 (major, [regression]): the cleanup gate is not enforced on the destructive step, and re-run is allowed while the source is being deleted

- **Where.** `catalog_schema.sql` `namespace_moves` CHECKs and transition trigger; §5.5.1 steps 9 and 10; N161(1)(2); §5.5.5 re-run bullet.
- **Claim.** N161(2): the source is "static and intact: cleanup is impossible before a post-activation backup"; "`reconcile_in` … allowed only while the catalog move is `committed` or `cleaning`". §9.5: "gate (`CleanupMove`, the catalog `done` CHECK)".
- **Evidence.**
  - Executed on `r7pg_cat`: `committed → cleaning` is accepted with `activated_at`, `committed_replicated_at`, `target_backup_started_at` and `target_backup_at` all NULL. The gate CHECK applies only to `done`.
  - §5.5.1 step 10 runs the source deletions (`engram_cleanup_namespace` batches) *before* `CleanupMove` "checks the gate and records `done`". So the CHECK gates the bookkeeping row after the irreversible step.
  - `engram_cleanup_namespace` on the source checks only `moved_out`.
  - `cleaning` covers both "waiting for the gate" (from `Restart` onward) and "deleting the source". A target PITR to before activation while cleanup batches run therefore re-runs from a half-deleted source. `VerifyFrozen` compares the copy against that same partial source and passes. The namespace activates with rows missing, a silent loss.
- **Recommendation.**
  - Add a state `gated`, or move the gate CHECK from `done` to `cleaning`, so that a row reaches `cleaning` only through the gate. The source batches start only after reading `cleaning`.
  - Restrict `reconcile_in` and the re-run to `committed` plus the new pre-gate state.
  - Make `engram_cleanup_namespace` on a source require a shard-local token written by the gated step, for example `namespace_ownership.cleanup_allowed_at` set by an admin edge. A buggy or manual call then cannot delete before the gate.
  - Add a must-fail `ShardMove_RerunDuringCleanup.cfg`.

---

## Minor

### PG7-8 (minor): VerifyFrozen cannot finish within the 30 s statement timeout of `engram_move`/`engram_admin` above about 0.8 M facts

- **Claim (§5.5.1 step 5, §9).** `bit_xor(hashtextextended(pk::text, 0))` over the whole namespace in one pass, with "no row cap". §9: "30 s and 30 s for `engram_admin` and `engram_move` (raised per session only inside `engramctl`)".
- **Evidence.**
  - Measured: 300 k `fact_links` PK rows in 370–410 ms, about 1.25 µs per row, mostly the row-to-text cast and the hash.
  - A 2 M-fact namespace has 60 M links at the sizing's 30 per fact, which is about 75 s per side in one statement. The activity times out, retries, times out again, then rolls back.
  - The limit is about 24 M links per statement, roughly 0.8 M facts.
- **Recommendation.**
  - Compute the hash per PK range and xor the partial results (`bit_xor` is associative), or allow the verify activity a per-session timeout and state it next to the engramctl exception.
  - A PK hash also cannot detect a stale row, while `VerifyFrozen` in the spec compares content (`store[tgt] = store[src]`, `CopyFault` = "dropped or stale"). Either hash `ROW(t.*)` or narrow the spec's fault to "dropped".

### PG7-9 (minor): `engram_move_indexes_valid` is vacuously true, and `reconcile_in` activates a target the shard knows nothing about

- **Executed.**
  - With zero `vector_indexes` rows the function returns `t`. `vector_indexes` is in the excluded class, and `engram_cleanup_namespace` deletes its rows on the re-run path.
  - `ready_target` and the admin's `reconcile_in` therefore pass for a namespace with any number of copied vectors and no HNSW. The first recall is then the whole-namespace exact scan that N160(5) exists to prevent.
  - `engram_admin` also took an `incoming` row with no committed move through `incoming → ready → active`. "Only while the catalog move is committed/cleaning" is prose that the shard cannot check.
- **Recommendation.**
  - The function should also require a request row for every `(vector table, current model)` whose copied row count is ≥ 2,000, counted with an index-only count on the PK.
  - `rerun_move` should set a shard-local `rerun_of` marker that the `reconcile_in` edges require.

### PG7-10 (minor, [regression]): the footprint figures do not apply N165's own measured B-tree fill

- **Evidence.**
  - The §3.7 table re-adds to 181.45 GB per 10 M, and × 1.12 gives 203 / 112 GB, so the arithmetic holds.
  - But the table's link indexes are 21 + 20 = 41 GB for 300 M links (≈ 137 B per link). N165 says 101 + 123 = **224 B per link**, which is 67 GB per 10 M facts and is what the hot set's 37 GB at 5.5 M uses.
  - The other B-trees in the table are at about 90 % fill (facts PK 0.5 GB per 10 M ≈ 50 B per entry), not N165's "× 1.7".
  - Re-sized with the measured fill: ≈ 220–225 GB per 10 M before `ins_seq`, which gives ≈ 135–140 GB at 5.5 M (not 112) and ≈ 245–250 GB at the cap (not 203).
- **Effect.** The 600 GB volume still fits, so no decision changes. But "≈ 112 GB at target" (D3, N165, `catalog_schema.sql` comments) and every derived figure (base-restore time, backup sizes, move-target headroom) inherit the error.
- **Recommendation.** Restate the footprint with the same fill as the hot set.

### PG7-11 (minor): the window CHECKs allow zero slack and an unbounded deadline

- **Executed.** `w_est_seconds = window_seconds = 14400` is accepted, and so is `freeze_deadline = frozen_at + 30 h` under a 4 h window.
- **Why it matters.** With window = W_est the deadline is `min(max(1.5 W_est, W_est + 10 min), window) = W_est`, so any underestimate rolls back.
- **Recommendation.**
  - `CHECK (window_seconds >= greatest(1.5 * w_est_seconds, w_est_seconds + 600))`; `StartMove` refuses below it.
  - `CHECK (freeze_deadline <= frozen_at + window_seconds * interval '1 second')`.
  - `CHECK (moved_out_at IS NULL OR committed_replicated_at IS NOT NULL)`, so that N163(3) is a schema fact.

### PG7-12 (minor): which tables are copied contradicts itself, and one reading breaks BuildIndexes

- **The conflict.**
  - N160(3) and §5.5.1 step 4: "**every table of all three classes** is copied".
  - The same row, §5.5 intro and §9.5: "the third is re-derived … `vector_indexes`, `namespace_stats`, caches — excluded".
- **The consequence.** If `vector_indexes` were copied, the source's `ready` rows would land on the target. The runner never builds a `ready` row, and the mover's requests would conflict with them on the PK, so `engram_move_indexes_valid` stays false and the move rolls back.
- **Recommendation.** Fix the register and step 4 text to say "classes 1 and 2, plus `token_usage_events` once".

### PG7-13 (minor): when the expunge resumes on the target is stated three different ways

- **The three versions.**
  - SQL: `purgeable_namespaces` resumes at `activate_target`, which clears `move_id`.
  - §5.5.1 step 9: resumes at `cleaning`.
  - §9.5 ("paused from Plan to done"), the `shard_schema.sql` header line 31, and `TestExpunge_FencedAndPaused` / `TestMove_ExpungeStaysPaused` ("no purge runs from Plan to done"): resumes at `done`.
- **Why it matters.** `done` is at least 24 h plus a full backup after activation. Implementing what the tests say pauses acknowledged-delete expunge on the target for a day or more.
- **Recommendation.** The SQL behaviour (resume at activation) is the sound one, so align the prose and the tests to it.

### PG7-14 (minor, unverified): "RTO ≤ 60 min" does not hold while a move is landing on the shard

- **Evidence.** The paced copy writes 25 MB/s (90 GB/h). The N166 watcher starts a differential at 32 GB, and WAL keeps accumulating while that differential runs. A restore replays from the start of the last *completed* backup, so the worst case is 32 GB plus 25 MB/s × the differential's duration. A 10 min differential gives ≈ 47 GB, which is ≈ 56 min of replay at 1.2 min/GB before the base restore and the reconcile. N161 removed the move's re-copy term from RTO, but the move's WAL volume reintroduces one.
- **Recommendation.** Either pace the copy so that WAL since the last completed backup stays ≤ 32 GB, which means pausing the copy while a differential runs, or state the RTO as "≤ 60 min, ≤ 75 min during a move-in".

### PG7-15 (minor, unverified): pool wait per recall versus per arm

- **Evidence.** Erlang-C with c = 32, A = 24 and holding time 425/6 ≈ 71 ms gives P(wait) = 0.083 and a per-arm p95 wait of ≈ 4.5 ms, so "≤ 8 ms p95" holds per arm. A recall waits for its six arms, though. If they are acquired in parallel, the relevant figure is the maximum over six, whose p95 is ≈ 20 ms; at A = 27 it is ≈ 49 ms.
- **Recommendation.** Restate the critical-path term at the recall level (≈ 224 → ≈ 236 ms p95, still under 300 ms) and keep A ≤ 24 as the alert line.

---

## Nits

- **PG7-16: the hop's "Heap Fetches: 0" holds only after VACUUM.** Executed: after inserting 800 links that touch the frontier, the PK branch showed `Heap Fetches: 800`. With `autovacuum_vacuum_insert_scale_factor = 0.05` on 10 M-link partitions, up to about 5 % of each partition (≈ 0.8 GB of link heap at 5.5 M, recency-skewed) is not all-visible. Pin M0.6's EXPLAIN on an unvacuumed tail, and add that tail to the hot set.
- **PG7-17: lock key spaces and the purge-pause key.**
  - "Disjoint lock key spaces" is not accurate for the one-argument keys. `hashtextextended(…, 0..3)` all land in one bigint space; the seed does not separate them. Collisions are negligible at this scale, and the comment already says a collision only over-serialises, so say "independent hashes".
  - The purge-pause key must be held by a session other than the one running `VACUUM`, because the session cannot unlock while VACUUM runs. Holding it until `VACUUM` returns is simpler and only more conservative.
- **PG7-18: `engram_seq_advance` return value.** It returns `last_value` *after* its own `engram_seq_sample()` `nextval` when it does advance, but the pre-sample value when it does not. Callers must not compare the two. Forward-only behaviour was verified: `advance(5)` after `advance(1,000,000)` left `last_value` at 1,010,001.

---

## Verified as stated (executed or recomputed)

- **Catalog move trigger.** It refuses `rolled_back → frozen` and every non-listed edge, and the `done` CHECK refuses `done` without the gate columns.
- **`ready_target`.** As `engram_move` it requires `w_final` and `last_value > w_final`, and `GRANT SET ON PARAMETER session_replication_role` is present.
- **HNSW path with `document_id = ANY(const)` under `force_custom_plan`.**
  - It plans as `Index Scan using fv_… Order By … Filter: (document_id = ANY …)`, with no semi-join and no `Join Filter` on `document_id`.
  - Planning took 1–3 ms at 1,000 documents; ≈ 13 k buffers for 150 rows on 60 k vectors.
  - The generic plan degrades to an `ins_seq` index scan plus sort (1.08 s, with JIT 0.2 s), which confirms `force_custom_plan` is load-bearing.
  - Exact-path costs stay below `jit_above_cost` up to 4θ (cost ≈ 32 k at 20 k rows).
- **Graph hop.** The `UNION ALL` hop gives two `Index Only Scan`s after VACUUM.
- **Fleet arithmetic.**
  - 182 shards, 46 hosts and 6 cells check out (182 / 32 = 5.69).
  - Fill takes 67 days (1 B / (182 × 82 k)).
  - The hot set sums to 74.2 GB, and × 6.5/5.5 = 87.7 GB.
- **WAL arithmetic.**
  - Average fill: 15 GB/day at 82 k facts/day is 183 KB per fact.
  - Backfill: 20.8 k inserts per 30 min × 30 touches over 1.4 M pages gives a Poisson distinct ≈ 0.50 M pages, ≈ 3.4 GB per interval, ≈ 160 GB/day, which matches "≈ 150".
  - Crash recovery: 16 GB × 1.2 min/GB = 19 min.
- **Small-move estimate.** W_est ≈ 25 min per 1 M (1,000 s + 383 s + verify) and ≈ 6–10 min at 250 k.

---

## Counts

| Severity | Count |
|---|---|
| Blocker | 0 |
| Major | 7 (PG7-1 to PG7-7; six marked [regression], one [regression in exposure]) |
| Minor | 8 (PG7-8 to PG7-15) |
| Nit | 3 (PG7-16 to PG7-18) |

## Verdict

**Not ready; needs a small D26.** The freeze-then-copy core is sound on a static source, and the SQL state machines behave as written. The majors are:

- The new post-commit paths, re-run and cleanup, do not fit the restore epoch step or the cleanup gate (PG7-1, PG7-7).
- The asynchronous catalog misses three things: the grant its LSN wait needs (PG7-2), a correct epoch rule for open moves (PG7-3), and a story for catalog-only acknowledged writes (PG7-4).
- The tag rule fails on a repeated `Invalidate` (PG7-5).
- W_est is wrong above the in-memory build bound (PG7-6).

Each has a local fix. None needs the move protocol redesigned again.

## Not verified

- Real `REINDEX` and `CREATE INDEX CONCURRENTLY` WAL bursts.
- pgBackRest differential duration (PG7-14).
- On-disk HNSW build rate past `maintenance_work_mem`; I used the plan's own 3 ms figure.
- Topic-correlated filtered-arm buffers.
- pg_search arms (stubbed locally).
- TLC runs of the changed specs; I read `Derivation.tla`'s `Invalidate`/`Restore` and did not run them.
- Whether the six recall arms acquire connections in parallel (PG7-15).
