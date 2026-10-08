# Review round 8 — PostgreSQL and operations lens (D26, N169–N178)

**Executed.** Both DDL files were applied unmodified to scratch PG 16.15 + pgvector 0.8.6 in my own databases (`r8pg_cat`,
`r8pg_shard`). I also ran a real streaming standby (`pg_basebackup -R -X stream`, port 55439) to test `catalog_replicated`
against a live walsender. On the catalog I tested: `catalog_replicated` with the function owned by a non-superuser and by a
superuser, as `catalog_admin`, with a temp-schema spoof; the replication feedback latency (commit, then
`pg_current_wal_lsn()`, then polling the helper); the `namespace_moves` CHECKs and the move trigger (backup CHECK, cleanup
gate, live-move unique index, the edges out of `committed`). On the shard I tested: the N169(2) restore path
`incoming → frozen/restore → active` as `engram_move`/`engram_admin`, then `freeze_delete` and `start_move` on the result;
`purgeable_namespaces`; `engram_cleanup_namespace`'s `c_order` against `pg_constraint`. I also timed
`pg_backup_start(fast => false|true)` on the scratch server. Everything else is recomputed from the plan's own numbers.
The standby, the databases and the helper role were removed afterwards.

**Holds (checked, no finding):**
- `c_order` is a valid topological order of every FK. The only "violation" is `entities`' self-FK, which the function clears first.
- `freeze_delete` with `move_id` set raises 55006.
- The `freeze_restore` edges from `incoming`/`ready` exist and accept the restore epoch rule.
- `engram_memory_subject`'s `curation_log` fallback is correct and served by the PK.
- The window CHECKs (`≥ max(1.5 W, W + 600)`, `freeze_deadline ≤ frozen_at + window`, cap 28 800) behave as N173 states.
- The gate refuses `committed → cleaning` with `activated_at` NULL or under 24 h.
- `catalog_replicated` returns true promptly (≈ 40 ms after commit, 6/6 runs) when its owner can read the stats. Replies
  from the standby's walreceiver are not throttled to `wal_receiver_status_interval`, so the ack wait adds no latency.
- With no standby it returns false, as designed.
- Arithmetic: the 8 h cap admits ≈ 12.3 M facts at 26 min/M with the 1.5× margin. 32 GB `shared_buffers` + a 24 GB DSM
  build fit the 112 GB container without OOM.

## Findings (ranked)

### PG8-1 — major [regression]: `catalog_replicated` is false forever unless its owner is a superuser or holds `pg_read_all_stats`

*Location:* `catalog_schema.sql:490–495`; N171(1); §9.1 config lint; §8 `TestCatalog_ReplicatedHelperAsAdmin`.

*Claim:* "`SECURITY DEFINER`, owned by `catalog_migrate` … `config lint` checks the grant and that `catalog_admin` holds no
`pg_read_all_stats`."

*Evidence (executed, live walsender).* `pg_stat_get_wal_senders()` checks `has_privs_of_role(GetUserId(),
pg_read_all_stats)`. Inside a `SECURITY DEFINER` function that user is the owner. Results:

| Owner of the function | `catalog_replicated('0/0')` called as `catalog_admin` |
|---|---|
| non-superuser (`r8_catalog_migrate`) | `false`, at any LSN: `replay_lsn` is NULL |
| superuser | `true` |
| non-superuser after `GRANT pg_read_all_stats` | `true` |

Nothing in the plan grants `pg_read_all_stats` to `catalog_migrate`, and the lint checks only the opposite direction.

If `catalog_migrate` is a non-superuser owner, as an owner/migrate role should be:
- every client-acknowledged catalog write (`CreateTenant`, `CreateNamespace`, …) returns `UNAVAILABLE`;
- every move stalls at `committed`;
- every rollback stalls at `rolled_back`.

This is the PG7-2 failure again, one level down. The §8 test would pass in a testcontainer where the DDL is applied as
`postgres`, which is how `validate_sql.py` applies it.

*Recommendation:*
- `GRANT pg_read_all_stats TO catalog_migrate` belongs in the catalog bootstrap. Alternatively, make the helper's owner a
  dedicated `catalog_replication_reader` role that holds only that membership.
- `config lint` asserts the owner's membership with `pg_has_role(owner, 'pg_read_all_stats', 'USAGE')`.
- The test runs with a non-superuser owner.

### PG8-2 — major [regression]: the N169(2) restore path leaves `move_id` set, so the namespace can never be deleted or moved again

*Location:* `shard_schema.sql:646–648` (`freeze_restore` from `incoming`/`ready`, `move_effect 'none'`) and `restore_done`
(`'none'`); N169(2); N177; §5.4.3.

*Claim:* "`freeze_restore` gains the `incoming` and `ready` sources" and "`move_id` clears at activation, so a moved namespace
is deletable seconds after (b″)".

*Evidence (executed):*
1. `plan_target` inserts `incoming` with `move_id`.
2. `freeze_restore` to `frozen/restore`, then `restore_done` at epoch 3. The row is `active|3|<move_id>`.
3. As `engram_app`, `freeze_delete` raises `55006 namespace … has an open move`.
4. As `engram_move`, `start_move` is refused: `open` requires `o.move_id IS NULL`.

No step of §9.3 or §5.5.5 clears `move_id` on this path. Only the admin `abort_move` edge (`active → active, close`) would,
and nothing invokes it. Consequences:
- `DeleteNamespace` answers retryable `NAMESPACE_BUSY` forever.
- `TenantDelete` re-resolves and retries forever, which breaks the 28-day deletion SLA.
- The namespace is unmovable.

The same edges also leave `moved_in_at` NULL: the trigger sets it only on `ready → active`. So N147's 10-minute
watermark/`BeginSnapshot` wait is skipped for a target restored to a point before activation (consequence unverified).

*Recommendation:*
- `freeze_restore` from `incoming`/`ready` takes `move_effect = 'close'`. Alternatively, `restore_done` closes a set
  `move_id`.
- The trigger stamps `moved_in_at` on `restore_done` of a row that was a move target.
- Add both to `TestMove_TargetRestoredAfterMoveBackup`: delete and re-move after the restore.

### PG8-3 — major [regression]: `MoveBackup`'s duration model (`bytes / 200 MB/s`, "≈ 2 min per 1 M facts") omits four terms, each of which can exceed the 10-minute margin of an unattended move

*Location:* N169(1), N173(1), §5.5.1 step 7, §9.3 backup table, R46.

*Claim:* "an incremental backup … reads the copied data once, ≈ 25 KB per fact at ≈ 200 MB/s, ≈ 2 min per 1 M facts, inside
`W_est`".

*Evidence:*

1. **Checkpoint at backup start (executed).** No `start-fast` is configured anywhere. pgBackRest's default is `n`, so
   `pg_backup_start(fast => false)` runs a *spread* checkpoint paced to `checkpoint_completion_target × checkpoint_timeout`.
   - Measured on the scratch server (`checkpoint_timeout` 5 min): **107 s** non-fast versus **0.36 s** fast, the fast run
     after a 400 k-row UPDATE.
   - With the plan's 30 min × 0.9, this is up to **≈ 27 min** right after a copy that dirtied shared buffers. It can be
     longer if a checkpoint is already in progress, because the request then waits for the next checkpoint.
2. **Archive backlog.** "Complete, with its last WAL segment archived" needs the archiver to reach the stop segment, and the
   archiver works in order.
   - The HNSW build just before writes its whole index as WAL in one unpaced burst (§9.2: ≈ 1.7 GB per 1 M vectors, so
     ≈ 17 GB at 10 M). At the planned ≥ 50 MB/s that is ≈ 6 min of backlog.
   - pgBackRest's `archive-timeout` defaults to 60 s and is not set, so the backup *fails* and the activity re-runs the
     whole backup.
3. **Stanza lock.** One pgBackRest backup runs per stanza at a time. These hold the target's stanza:
   - the 32 GB / 6 h WAL watcher, which fires every ≈ 21 min during a 25 MB/s copy;
   - the 02:00 daily differential;
   - the Sunday full, ≈ 21 min at the cap.

   `MoveBackup` waits behind whichever is running. Its policy is `P-db`, 12 h.
4. **Read volume.** pgBackRest detects changes per file (size/mtime) and `repo-block` deduplicates only what it *stores*.
   Every 1 GB segment modified since the last backup is read in full. With 16 hash partitions shared by ≈ 120 namespaces
   and per-namespace HNSW files that ingest touches throughout, this volume is a property of the shard (§9.3 itself: "HNSW
   insertion touches most 1 GB segments"), not 25 KB × the moved facts. My estimate is 30–60 % of the footprint, i.e.
   3–7 min at 200 MB/s on a 138 GB target (unverified magnitude).

For an unattended move (`W_est ≤ 10 min`, deadline `W_est + 10 min`), term 1 alone exceeds the margin, and the move rolls
back with `MoveWindowExceeded`. Large moves absorb it in the 1.5× margin. The rebalancer's path is the one that fails.

*Recommendation (decision on N169/N173):*
- `MoveBackup` runs with `start-fast=y`. Accept the immediate-checkpoint I/O burst as part of the degraded window.
- `pg_switch_wal()`, then wait for `pg_stat_archiver.last_archived_wal` to pass that segment *before* starting the backup.
  Set `archive-timeout` ≥ the remaining window.
- The mover owns the target's stanza for the move-in: the watcher and the scheduled differentials defer to it, and the move
  backup counts as the "differential after every move".
- `W_est`'s backup term becomes:
  - changed-segment bytes, measured at `Plan` from relation files modified since the last backup;
  - plus the archive drain of the build WAL;
  - plus a fast-checkpoint flush term.
- Recompute the unattended threshold.

### PG8-4 — major [regression]: `--lose-move-ins` cannot start the recovery move for a committed move-in

*Location:* N169(5), §9.3 step 2, `catalog_schema.sql:259–290`.

*Claim:* "`--lose-move-ins` … records those namespaces for N123's recovery move: an ordinary move …"

*Evidence (executed on the catalog).* With the original move at `committed`:
- Inserting the recovery move for the same namespace fails on `namespace_moves_live_uq`.
- `committed → rolled_back` is illegal.
- `committed → cleaning` is refused while `activated_at` is NULL. It *is* NULL when (b″) never ran before the target was
  lost, so the original row can never become terminal.
- Where `activated_at` is set, the only exit (`cleaning`) runs the source cleanup, i.e. deletes the one live static copy, in
  order to register a move whose source is a scratch restore of an older backup.

The runbook also does not remove the restored target's partial `incoming` row and its data before the recovery move's
`Plan`. `plan_target` cannot insert over it; `engram_cleanup_namespace` plus admin `rollback_target` would be needed first.

`--lose-move-ins` also over-refuses: for a move-in still before (a″) the reconcile's rollback is already lossless, yet the
plain restore refuses it.

*Recommendation:*
- Add a terminal catalog state, e.g. `lost`: admin-only, from `committed`, recorded with the restore id. The live unique
  index excludes it, and the recovery move references it.
- The runbook gains "cleanup + `rollback_target` of the restored partial row" before `Plan`.
- Restrict the floor refusal to move-ins at `cutover` or later.

### PG8-5 — minor: the `SECURITY DEFINER` helper resolves `pg_stat_replication` through the caller's temp schema

*Location:* `catalog_schema.sql:491`.

*Evidence (executed).* `SET search_path = pg_catalog` without `pg_temp` searches `pg_temp` *first* for relations. As
`catalog_admin`:
```sql
CREATE TEMP VIEW pg_stat_replication AS
  SELECT 'streaming'::text AS state, 'FFFF/0'::pg_lsn AS replay_lsn;
GRANT SELECT ON pg_stat_replication TO PUBLIC;
```
After that, `catalog_replicated('FF/0')` returned **true** with no standby having replayed it. Without the grant the call
errors. A buggy or compromised service role can therefore forge "replicated" and void RPO 0.

*Recommendation:* `SET search_path = pg_catalog, pg_temp` and `FROM pg_catalog.pg_stat_replication`. Add the spoof to the
test.

### PG8-6 — minor [regression]: `NAMESPACE_BUSY` lasts ≥ 24 h after activation, not "seconds after (b″)"

*Location:* §5.4.3 step 1, N177, N170.

§5.4.3 says "the catalog's partial unique index is the check". That index covers every state except `done` and
`rolled_back`, and N170's gate keeps a move in `committed` for ≥ 24 h after activation plus the cleanup batches. So
`DeleteNamespace` and `TenantDelete` are refused for at least a day after each move, while the shard (`move_id` cleared at
(b″)) would accept.

*Recommendation:* the catalog check is "a move of the namespace in `planned..cutover`, or `committed` with `activated_at`
NULL". The shard's 55006 stays the authority.

### PG8-7 — minor: the move-backup columns are not tied to the copy

*Location:* `catalog_schema.sql:240–254`, §8 (MoveBackup "a started backup is checked, else restarted").

*Evidence (executed):* `move_backup_started_at/at` set two days *before* `frozen_at` are accepted, and the row reaches
`committed`. The resumed activity has no identity for "its" backup: both times are recorded only at the end. It can
therefore adopt a watcher differential that started mid-copy, which voids N169(2)'s premise.

*Recommendation:*
- Record `indexes_valid_at` and add `CHECK (move_backup_started_at > indexes_valid_at)`. At minimum, require
  `> frozen_at`.
- Take the backup with `--annotation=move_id=…` and match on it when resuming.

### PG8-8 — minor (unverified): `ha`-profile shard failover has no move-backup floor

*Location:* §9.6 "A shard down (c)", N169(2)/(5).

The floor is enforced only by `engramctl restore --target`. `engramctl shard failover` promotes an asynchronous standby at
whatever LSN it replayed. A standby lagging behind the backup stop LSN at (a″) would be restored onto an incomplete copy
through the "ordinary restore" path. This is the `_NoMoveBackup` violation. It needs a standby lag longer than
build + backup + consumer wait, so it is rare.

*Recommendation:* either (b′) waits for the target standby's `replay_lsn` ≥ the backup stop LSN (record it), or the
failover refuses a standby below the maximum stop LSN of non-terminal move-ins and falls back to an archive restore.

### PG8-9 — minor: a 24 GB move-target build takes the target's cache below its hot set

*Location:* N173(2), §9.1.

| | Unreclaimable | Usable cache |
|---|---|---|
| Normal | `shared_buffers` 32 GB | ≈ 80–96 GB |
| During a 24 GB move-target build | `shared_buffers` 32 GB + `/dev/shm` DSM 24 GB, both charged to the 112 GB cgroup | ≈ 56–72 GB |

A target near 5.5 M facts has a ≈ 74 GB hot set. At 10k page touches per recall, a few points of extra miss rate pushes
past the 50 k IOPS budget for the ≈ 1 h of a 10 M build. That is not an OOM, but it is not the "page cache shrinks" of
N173 either. It also happens to whichever ≈ 120 other namespaces share the target.

*Recommendation:* rebalancer and `Plan` choose targets with `hot set + mwm ≤ usable cache`, or state the IOPS consequence
as part of the degraded window.

### PG8-10 — minor (unverified): "RTO ≤ 75 min while a move-in lands" assumes a ≈ 10 min differential

*Location:* N176, §9.3.

The watcher's differential is relative to Sunday's full and reads every file changed since then: ≈ 12 min at 138 GB and
≈ 21 min at 248 GB, at 200 MB/s. During it the copy adds 25 MB/s, so 32 + 31 ≈ 63 GB → ≈ 76 min of replay at 1.2 min/GB
before the base restore and the reconcile. The unpaced build-WAL burst (≈ 1.7 GB per 1 M vectors) lands after the copy
and before the move backup.

*Recommendation:* defer watcher differentials during a move-in copy (the move backup follows, PG8-3), or restate the bound
as ≈ 95 min at the cap.

### PG8-11 — minor: footprint figures are inconsistent with each other and with the §3.7 table

*Location:* §3.7, N176, `catalog_schema.sql:129–130`.

- The table rows sum to **181.45 GB**; × 1.12 for the `ins_seq` indexes gives **203 GB**, the old figure.
- The rows still use 90 % B-tree fill and 137 B/link (`fact_links` indexes 41 GB).
- The text says ≈ 225 GB per 10 M, but 138 GB at 5.5 M and 248 GB at 10 M both imply ≈ 25 GB/M, i.e. ≈ 250 GB per 10 M.
  "225 per 10 M" and "248 at the 10 M cap" cannot both hold.

Recomputed:

| Correction | Effect |
|---|---|
| Links at 224 B/link | +26 GB |
| Other B-trees × 1.7 | +18 GB |
| Total before the 12 % `ins_seq` term | ≈ 225 GB (this is the 225) |
| Total after it | **≈ 252 GB** |

*Recommendation:* state ≈ 250 GB per 10 M and regenerate the rows. `W_est`'s 25 KB/fact is already consistent with 250.

### PG8-12 — nit: backup cadence
§3.7 says "Backups: daily full"; §9.3 and N23 say a weekly full with daily differentials.

### PG8-13 — nit: connection arithmetic
The listed composition (32 + 8 + 2P + admin 2 + move 4 + relay 1 + migrate 1 + runner 1) is **49 + 2P = 57** at P = 4, not
53 + 2P = 61. The runner's second session (PG7-17), the pgBackRest backup connection and the exporter are not listed. The
total is still well under 100.

### PG8-14 — nit: `deletion_log.invalidation_op` has no CHECK
Unlike `fact_hidden`, nothing ties it to the kind. Add `CHECK (invalidation_op IS NULL OR kind = 'invalidate')`.

### PG8-15 — nit: test row role name
§8's `TestCatalog_ReplicatedHelperAsAdmin` names role `catalog_api`; the role is `catalog_app`.

### PG8-16 — nit: `shm_size` described per build
§3.3 and the shard DDL header describe "`shm_size = 8g`, a move target … `32g`" as a per-build setting. `shm_size` is fixed
at container creation; compose sets 32g permanently. Reword.

### PG8-17 — nit: the cleanup gate reads `OLD.activated_at`
- A single statement that sets `activated_at` and `cleaning` together is refused (executed). The catalog reconcile must
  write the shard's `activated_at` in a separate same-state update first. State this.
- The same two-statement backdating passes the gate as `catalog_admin` (executed). This is acceptable under the stated
  admin trust, but say so.

## Counts

**0 blockers, 4 majors (PG8-1 to PG8-4, all [regression]), 7 minors (PG8-5 to PG8-11), 6 nits (PG8-12 to PG8-17).**

## Verdict

The D26 core holds where it is pure SQL: the edges, CHECKs, cleanup order and gate behave as written. The four majors sit
where D26 meets the server and the tools:
- the replicated-ack helper fails closed without a privilege nobody grants;
- the new restore edges leave a stale `move_id`;
- the move backup's time model ignores checkpoint, archive, lock and file-granularity behaviour;
- the `--lose-move-ins` runbook is blocked by the catalog's own unique index and trigger.

PG8-1 and PG8-2 are one-line DDL fixes. PG8-3 and PG8-4 need register decisions (N169/N173 backup procedure and term; a
terminal state for lost move-ins). Not ready to freeze until those four are dispositioned.

## Could not verify

- pgBackRest itself is not installed here. Per-file read-whole behaviour under `repo-block`, the `archive-timeout` default
  and the stanza-lock error are from pgBackRest's documented design, not executed.
- The magnitude of changed-segment bytes on a real shard.
- The HNSW build rate at 10 M vectors in a 24 GB DSM, and whether it stays near 3,000/s.
- The cache-miss and IOPS effect of PG8-9.
- Replay speed of FPI-dominated build WAL versus the 1.2 min/GB average.
- Async-standby lag distributions for PG8-8.
- The TLA+ side of N178.
