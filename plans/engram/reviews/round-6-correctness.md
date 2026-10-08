# Review round 6: correctness and concurrency (C-1 to C-16)

Lens: the round-5 decisions (D24, N144 to N159) and the mechanisms they touched. These are the `commit_key` idempotency
check before the base CAS, with `$expected` for every writer; `fact_hidden` without its FK and `materialized_at`; the
invalidation twins; the synchronous catalog standby, the blob-mirrored replay floor and the shard-truth repair; the
per-subject lock with help-previous; recorded epochs and settled skips; per-shard `ins_seq` with `engram_seq_advance`;
PreVerify after catch-up; `ReconcileIn`; the N159 cleanup gate; and index readiness before cutover. I checked the prose
(§3, §5, §7, §9, register D24) against `sql/*.sql` and `formal/tla/*`, built interleavings, and executed the
following.

- **SQL.** `shard_schema.sql` was applied to my own database `r6c_shard` (PG 16, pgvector, pg_search stubbed). The
  scripts are in `scratchpad/r6c/`:
  - `twin.sql` checks `engram_invalidation_twins` at `Invalidate` time and at `Restore` time (C-4).
  - `resurrect.sql` checks the visibility of rows re-inserted after a purge (C-1).
  - `fk.sql` runs `engram_verify_fk` on a target state after the catch-up (C-5).
  - Two sessions race `setval` against `nextval` (C-16).
  - A last check covers `engram_holds_derivation_exclusive` with negative keys (it holds).
- **TLC.**
  - `scratchpad/r6c/tla/SMDel.tla` is `ShardMove.tla` plus one action, `TgtDelete` (a legitimate delete of a moved row
    on the active target), and one invariant, `NoResurrect`. Under `ShardMove_TgtRestore.cfg`'s bounds (catalog restore
    off), TLC finds a 15-state counterexample (C-1).
  - I also re-ran `ShardMove_TgtRestore.cfg` with the four invariants its invariant list omits (`NoRouteToTargetBeforeC`,
    `NoWriteToTargetBeforeC`, `ZombieCannotCutOver`, `RestoreReconciles`). It passes: 9,688,960 distinct states, depth
    29, 10 min.
- **Specs read against the prose.** Four findings hold because a spec models something kinder than what the prose or
  SQL specifies (C-1, C-3, C-4, C-7). None of them is listed in §7.1's column "prose mechanisms this spec omits". C-4 is
  listed there as "tested, not modelled", but the SQL that the test would exercise is wrong.

Findings already dispositioned in rounds 1 to 5 are not repeated. [regression: N…] marks a finding that a round-5
change caused. [fix incomplete: N…] marks a round-5 fix that does not achieve its stated effect.

## Findings (ranked)

### C-1: blocker [regression: N159(7), N149]: the cleanup-time `ReconcileIn` re-inserts rows that the target legitimately deleted after activation. Deleted documents, purged REPLACE chunks and acknowledged `Restore`s are undone on every move.

**Where.**
- §5.5.1 step 8.1: "**`ReconcileIn` at cleanup time** (§5.5.5), always, whether or not a timeline change was recorded:
  from the intact source it re-copies what the target lacks as `INSERT … ON CONFLICT DO NOTHING`".
- §5.5.5 step 1 defines `ReconcileIn`: "re-copies the insert-only rows with `ins_seq ≥ F_pre` … and the whole mutable
  class, both as `INSERT … ON CONFLICT DO NOTHING` (a target row that exists is either the copy or a post-activation
  write and is kept; **a missing one is restored from the source**)".
- `engram_doc_tomb`: "Purged tombstones are left out: their rows are gone". The visibility predicate is marker-only.

**The defect.** "Missing on the target" has a third cause that the rule ignores: the target deleted the row after
activation, and that delete was correct. The source is frozen at the freeze instant, so it still holds everything the
target has deleted since then. The cleanup runs ≥ 24 h after activation, so the cleanup-time `ReconcileIn` runs on
every move, with no fault needed.

**Interleaving (document delete).**
1. Namespace N moves from S to T. Document D's `documents` and `document_versions` rows (mutable class) and its
   recently inserted chunks and facts (`ins_seq ≥ F_pre`) are on both shards.
2. After activation, `DeleteDocument(D)` on T is acknowledged. The expunge purges D's rows. **Finish** marks the
   tombstone `purged` and deletes the `documents` row (no version remains). The tombstone row goes 24 h later.
3. At cleanup, `ReconcileIn` inserts S's `documents(D, state = 'active', context, metadata, tags)`, the
   `document_versions` rows, and the insert-only rows of D with `ins_seq ≥ F_pre`. Each insert uses
   `ON CONFLICT DO NOTHING`, and nothing conflicts.
4. D is active again: `GetDocument` and `ListDocuments` return its metadata, and recall returns its facts and chunks.
   Executed in `resurrect.sql`: with D's tombstone in state `purged`, a re-inserted fact and chunk of D are returned by
   `engram_visible_facts` and `engram_visible_chunks`. No intent replay runs at cleanup time. If one did, it would skip
   D's intent, because its `deletion_log` row (`intent_key`) is still present.

**Other effects of the same rule.**
- `Restore(f)` on T deletes `fact_hidden(f, invalidate)` and its `derived_hidden` rows. Both tables are mutable, so
  `ReconcileIn` re-inserts both from S, and the acknowledged `Restore` is undone.
- A REPLACE on T retires chunks. The 1 h retire purge deletes the chunks, and the `chunk_tombstones` rows cascade with
  them. Chunks with `ins_seq ≥ F_pre` come back without a tombstone, so the replaced text is served next to the new
  version.
- `REEXTRACTED_FACTS` purges come back the same way: old-key facts reappear without their `reextract` marker.
- The 24 h `idempotency_keys` sweep is undone: stale keys come back.
- An un-retire flap deletes a `chunk_tombstones` row. The row comes back, so the current chunk is hidden.

**TLC.** `ShardMove.tla` has no action that deletes a moved row on the target, so `store[Tgt] ∪ store[Src]` is harmless
there. `SMDel.tla` adds `TgtDelete(r)`, which is enabled when `mp ∈ {tactive, done}`, the target is active and the
cleanup has not run. TLC then violates `NoResurrect` in 15 states: move → `TgtRestore` → reconcile → `RestoreDone` →
`TgtDelete` → `ReconcileIn`. The spec's own `ReconcileIn` needs a timeline change first (`tlc[tgt] > 0`). The prose's
version runs "always", so the failure needs no fault at all.

**Recommendation (decision level).**
- `ReconcileIn` must never resurrect a row the target deleted after activation.
- **Cleanup time:** drop `ReconcileIn` when no timeline change of the target was recorded. A target without a timeline
  change cannot have lost moved rows.
- **After a timeline change**, restrict the repair to rows that were provably on the target at activation and are
  missing because of the timeline change. One way: re-copy only rows with `ins_seq < w_final` that are absent from the
  target **and** not covered by any target marker. "Covered" here means a tombstone of any state, kept until the
  source cleanup; a `deletion_log` entry of the subject newer than activation; or a purge record. Mutable rows should be
  overwritten or inserted only when the target row is older than the restore point (`updated_at`).
- The sturdier alternative is to make every post-activation target delete leave a durable "deleted since activation"
  record per moved row. Purges would write `(table, pk)` into a move-scoped table until cleanup.
- Add `TgtDelete` to `ShardMove.tla` and a must-fail configuration in which `ReconcileIn` is the union.

### C-2: major [regression: N159(7)]: the cleanup content check can never pass for a namespace whose target purged any moved insert-only row, so the source copy is never deleted. Deleted documents survive on the source indefinitely.

**Where.** §5.5.1 step 8.3: "the source's insert-only rows with `ins_seq < w_final`, less the documents the target has
tombstoned since activation …, are counted and hashed … on the static source and on the target; the two must be equal
… A mismatch repeats from step 1 once, then pages (`MoveCleanupContentMismatch`) and cleanup stays refused". The
catalog `done` CHECK requires `target_content_rows = source_content_rows AND target_content_hash = source_content_hash`.

**The defect.** The only exclusion is "documents the target has tombstoned since activation". The target legitimately
deletes moved insert-only rows in several other ways during the ≥ 24 h before the check:
- **Retire purges.** `CHUNK_TOMBSTONES` after any REPLACE or APPEND re-extraction of a moved document, with a 1 h
  grace. It deletes `chunks`, `facts`, `fact_vectors`, `chunk_vectors`, `fact_links` and `entity_mentions`, all
  insert-only. `REEXTRACTED_FACTS` deletes old-key facts.
- **DerivedPurge for a document delete.** It stubs `observation_versions` and `page_versions`, deleting rows and
  inserting stubs. These rows are not "document rows", so the exclusion misses them.
- **Pending deletes the move carried.** Pending tombstones move with the namespace and their expunge resumes on T at
  Restart. They were tombstoned *before* activation, so "since activation" excludes nothing for them.
- **H = 90-day version compaction** (N157) deletes superseded versions.

Any of these makes the counts differ permanently, because the source is static and the target has diverged. Two
consequences follow:
- **Capacity.** The source never frees the namespace, which defeats the purpose of rebalancing.
- **Erasure.** The source rows of documents deleted on the target are never physically erased: they remain in the
  source's tables and every later source backup. The task requires a hard delete that cascades.

N149 itself rejected a whole-namespace hash before cleanup because "the target legitimately diverges after
activation". N159(7) re-introduced exactly that check. `ShardMove.tla` cannot see the problem: its `Cleanup` guard
checks `frozenSet ⊆ bak[tgt].store`, and the spec has no target-side delete (see C-1).

**Recommendation.** Do not compare a diverging target with the source. After a timeline change, verify the target's
**backup** instead: the C-1 repair ran, and the backup started after it. When no timeline change was recorded, the
backup-after-activation gate of N143(5) is sufficient. If a content check is kept, compare per row "present on T, or
covered by a T marker or purge record" (C-1's record), never raw counts.

### C-3: major [regression: N149]: after a target restore to a point in the copy phase, `ReconcileIn` re-copies only `ins_seq ≥ F_pre` and keeps stale mutable rows (`DO NOTHING`), and the target is activated with missing rows and stale state. The spec re-copies the whole source.

**Where.** §5.5.5 step 1: for a target of a `committed` or `cleaning` move, "a backup taken during the copy phase holds
rows above its then-lower sequence, and a restore to it brings the low sequence back together with the rows". So the
prose expects restore points in the copy phase. It then runs `ReconcileIn` with "insert-only rows with `ins_seq ≥
F_pre` … and the whole mutable class, both as `INSERT … ON CONFLICT DO NOTHING`", then `reconcile_in: incoming → ready
→ active`.

`ShardMove.tla` `ReconcileDesign(s = tgt)` with `cm = committed` does `store[Tgt] ∪ store[Src]`: all rows, with no
floor. A2 in §7.1 states that "the source rows … are the copy the target is repaired from" without the floor.

**Failure.** Suppose the target's restore point lies inside the bulk copy, which runs for up to 12 h, with a daily full
backup plus differentials.
- **Insert-only rows.** Every row bulk-copied after that point and below `F_pre` is absent. These are the hours of
  `facts`, `chunks`, `fact_vectors` and so on copied before PreVerify. `ReconcileIn` does not copy them.
- **Mutable rows.** Rows present at the restore point keep their copy-phase values (`DO NOTHING`):
  - `documents.current_version` and `state`, `document_versions.status`, `observations.current_version` and
    `pages.current_version`, `consolidation_state`, `fact_hidden.materialized_at`, `document_tombstones.expunge_state`
    stay as they were.
  - A newer `document_versions` row whose status is `active` hits the unique `document_versions_active_uq`, and
    `ON CONFLICT DO NOTHING` silently drops it.
- The intent replay restores deletes and invalidations only. The namespace is activated with lost and stale content
  that the intact source still holds, which is not an accepted RPO loss.
- The C-2 content check would then refuse cleanup forever, but the target is already serving.

**Recommendation.**
- Choose the repair by the restore point. If the restored target timeline starts before `ready_at`, run a full
  `Reconcile` from the frozen or `moved_out` source: whole-namespace anti-join for the insert-only class, and the
  freeze's merge-diff for the mutable class (no post-activation writes exist to protect).
- Use the C-1-restricted repair only for restore points after activation, with mutable rows overwritten when
  `updated_at < activated_at`.
- State the floor-free re-copy in §7.1 or remove it from the spec. As written, the spec checks a stronger repair than
  the prose builds.

### C-4: major [fix incomplete: N145(3)]: `Restore` does not undo the twin half of an `Invalidate`. The visible copy of a restored fact stays hidden forever, and its derived versions stay hidden.

**Where.**
- §5.4.5: `Restore` "mirrors the twin rule: `DELETE FROM fact_hidden WHERE memory_id = ANY($f_and_twin)` … `DELETE
  FROM derived_hidden WHERE cause_kind = 'invalidation' AND cause_id = $1`".
- §3 states the same thing as "`$ids of the last Invalidate entry`".
- `engram_invalidation_twins(p_ns, p_memory)` ends with `AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE … h.memory_id =
  t.memory_id)`.
- Materialize writes `derived_hidden(invalidation, cause_id = <the victim>)` for each unstamped `fact_hidden` row,
  twin included.

**Evidence (executed, `twin.sql`).** Fact f1 is the old-key fact with a `reextract` row, and f2 is its re-extraction
twin with the same `content_hash`.
1. `engram_invalidation_twins(ns, f1)` returns f2 at `Invalidate` time.
2. After the `Invalidate` writes `fact_hidden(f1)` and `fact_hidden(f2)`, the same function returns **0 rows** at
   `Restore` time, because f2 now has a `fact_hidden` row.

**Interleavings.**
- **Via the function (§5.4.5).** `Restore(f1)` deletes only f1's row. f2, the copy the user sees, stays hidden. f1 is
  still `reextract`-hidden, so nothing is visible, and `Restore` acknowledged success.
- **Via the effect ids (§3).** f2's `fact_hidden` row is deleted, but `derived_hidden(invalidation, f2)` survives,
  because the delete filters `cause_id = $1` (f1). Every observation or page version citing f2 stays hidden. Nothing
  else ever deletes an invalidation-cause row, and `Restore(f2)` now returns `NOT_INVALIDATED`. `RestoreExact` is
  violated permanently.
- **Lazy twin.** `Invalidate(f1)` runs, and later a re-extraction's `CommitChunk` re-applies the `curation_log` action to
  the new twin f2 (`fact_hidden(f2, invalidate)`). `Restore(f1)` then finds f2 neither through the function (hidden)
  nor in the `Invalidate` entry's ids (it did not exist then). f2 stays hidden forever.

  This path is the common one: invalidate a fact, the document is re-extracted (a prompt-version bump or an APPEND),
  then restore. `CommitChunk` re-applies only to `$new_fact_ids`, so a later re-extraction does not fix it either.

`Derivation.tla`'s `Invalidate(f)` hides only `{f}` and `Restore(f)` un-hides only `{f}` (§7.1: "the visible-twin
resolution … tested, not modelled"). The T3 test would exercise the SQL above.

**Recommendation.**
- Make the twin relation symmetric and state-independent. `Restore` should resolve `{f} ∪ {g : same document, same
  content_hash, same life, fact_hidden(g, invalidate) exists}`, a new `engram_restore_twins` without the `NOT EXISTS`
  filter.
- Delete `derived_hidden` rows with `cause_id = ANY(those ids)`.
- Record the resolved set in the `Restore` intent.
- Add the twin rule to `Derivation.tla` (`Invalidate` hides `{f, Twin[f]}`, `Restore` must un-hide both), with a
  must-fail configuration in which `Restore` un-hides only `{f}` against `RestoreExact`.

### C-5: major: PreVerify's `VerifyFK` runs on the whole namespace before the mutable class has been caught up, so any namespace that creates a document, version, entity or observation during the bulk copy fails it and rolls back deterministically

**Where.**
- §5.5.1 2b(c): "the whole-namespace work against the insert-only rows with `ins_seq < F_c` on both sides: per table
  `count(*)` and `bit_xor` …, `VerifyFK`, and the blob existence checks". A mismatch runs one more round (catch-up,
  check) and then rolls back.
- The catch-up copies **insert-only tables only**. The mutable class is merge-diffed only under the freeze (step 4).
- `engram_verify_fk(p_ns)` has **no range parameter**. It counts orphans for every FK of every namespace-scoped table,
  including the insert-only → mutable FKs: `chunks`/`facts`/`document_versions → documents`,
  `document_version_chunks → document_versions`, `entity_mentions → entities`,
  `observation_versions → observations`, `page_versions → pages`. Its own header says "run by the mover on the target
  **after the reconcile**".

**Evidence (executed, `fk.sql`).** A chunk of a document `dnew` was loaded under `session_replication_role = replica`
(N91), which disables the FK triggers, and the document's `documents` row is absent. `engram_verify_fk` then reports
`chunks_namespace_id_document_id_fkey | chunks | 1`.

**Failure.** The bulk copy takes `documents`, `document_versions` and `entities` first, then the large insert-only tables
over hours. Any retain during the copy creates a `documents` or `document_versions` row and `entities` rows after those
tables were copied. It also creates chunks, facts and mentions, which arrive through the bulk copy or the catch-up.
Their parents arrive only with the freeze-time merge-diff. A second round cannot help, because it does not copy mutable
rows either.

So every namespace that ingests during its move rolls back at PreVerify. This is round-5 C-5's outcome, reached through
`VerifyFK` instead of the counts. `ShardMove.tla`'s rows and mutable keys have no FK between them, so
`MoveTerminatesActive` cannot see it.

**Recommendation.**
- In PreVerify, run `VerifyFK` only for FKs whose parent is insert-only, or let the catch-up also upsert the mutable
  parents of the rows it copied.
- Run the full `VerifyFK` after the freeze-time merge-diff, as its header says.
- Give `engram_verify_fk` the `ins_seq` range parameter that §3 and §5.5.1 step 4 already describe ("restricted to the
  same range"). The function as written has none.

### C-6: major [regression: N153]: `AwaitIndexes` sits after the last catch-up, so freeze work grows with the index wait, and the trigger's 0.99 test uses the current count rather than the verified one, so `ready` is refused for active namespaces

**Where.** §5.5.1 2c: indexes are requested after PreVerify, before Freeze, waiting up to the 12 h `StartToClose`.
`IndexRunnerStarved` fires only after 30 min of refusals, so long waits are expected. Step 4: "re-copy the rows with
`ins_seq ≥ F_pre = engram_seq_floor(T_pre)`", with `T_pre` taken at PreVerify. Step 4 also says: "Freeze work = the rows
inserted since `T_pre − 10 min` … < 30 s for ≤ 1 M-fact namespaces". The register (N153) and §5.5.1 say `rows_at_build
≥ 0.99 ×` **the verified count**. The SQL `engram_move_indexes_ready`, evaluated by the ownership trigger at
`ready_target`, compares `i.rows_at_build < 0.99 * n.c`, where `n.c` is the **current** count, which includes the rows
re-copied under the freeze.

**Failure.**
- **Freeze volume.** The freeze re-copy now covers PreVerify's checks plus the whole index wait. That is tens of minutes
  at normal runner load and up to 12 h. An active namespace writing 5 facts/s for 30 min re-copies ≈ 9 k facts, their
  vectors and ≈ 180 k `fact_links` rows under the freeze, and each vector is inserted into a built HNSW (≈ 1–3 ms
  each). That does not fit a `max(120 s, 60 s + 1 s per 10 k facts)` watchdog for most namespaces. For a multi-hour
  wait the watchdog rolls back every time.
- **Readiness test.**
  - HNSW maintains rows inserted after the build, so `rows_at_build` falling behind the current count says nothing
    about coverage.
  - The trigger nevertheless refuses `ready` when the re-copied vectors exceed 1 % of the namespace.
  - Example: a 50 k-vector namespace that gains 500 vectors between the catch-up and the freeze (≈ 4 per second over a
    2 min window) fails (b′). The retry repeats the same window.
  - Moves are aimed at hot shards, so this hits the namespaces that are actually moved.
- **Spec.** `ShardMove.tla` has no index state (assumption A3), so `MoveTerminatesActive` cannot see either effect.

**Recommendation.**
- After `AwaitIndexes`, run one more catch-up and take `T_pre`/`W_pre` at its start, so `F_pre` is fixed after the
  wait: PreVerify catch-up → `AwaitIndexes` → final catch-up → Freeze.
- Make the readiness predicate `state = 'ready'` with `rows_at_build ≥ 0.99 ×` the **verified** count passed in from
  PreVerify, as the prose states. The trigger cannot compute that count itself.
- Restate A3 so that it states the effect of the wait on freeze work.

### C-7: minor [fix incomplete: N146]: the synchronous catalog standby does not make observed commits RPO 0 when a commit's sync wait is cancelled, and the prose's restore reconcile lacks the shard-truth step that the spec applies on every reconcile

**Where.** N146 says "If the standby is down, catalog writes block … Promotion of the standby is RPO 0". The shard-truth
repair runs only "after every catalog restore" (N159(4)), not after a promotion. In §5.5.5 step 1, the arbiter is the
catalog row ("CASes `cutover → rolled_back`"). `ShardMove.tla` `ReconcileDesign` applies `ShardTruth` (`cm = open ∧
DerivedCommitted ⇒ committed`) on **every** shard reconcile.

**Failure.** PostgreSQL commits locally before the synchronous wait. A cancelled wait completes with "WARNING: canceling
wait for synchronous replication … The transaction has already committed locally, but might not have been replicated
to the standby". That cancel comes from a pgx context deadline or a `statement_timeout`, and the commit then becomes
visible.
1. The standby stalls, and the mover's `CommitMove` context expires.
2. The retried activity reads `committed`, runs (c) and (b″), and the target acknowledges writes.
3. The primary fails, and the agent promotes the standby, which lacks `committed`. No repair runs, because this was a
   promotion and not a restore.
4. A later reconcile CASes `cutover → rolled_back` and rolls back from the target side while the source is
   `moved_out`.

This needs a double fault (standby stall plus primary loss). It contradicts A1 in §7.1 nonetheless.

**Recommendation.**
- Run `catalog reconcile --from-shards` after every promotion too; it is cheap.
- Add the spec's shard-truth step to §5.5.5 step 1: never take the rollback CAS while a source row is `moved_out` or a
  target row is `active` at the move's epoch.
- Have the failover agent refuse to promote a standby that was not `sync_state = 'sync'` at failure without operator
  confirmation.
- Treat a commit whose sync wait was cancelled as unknown and re-read it after the standby is back.

### C-8: minor: a twin `Invalidate` changes two subjects but writes one chain entry, so replay can order it after a later `Restore` of the twin

**Interleaving.**
1. `Invalidate(f1)` (subject f1) hides f1 and the twin f2.
2. The user, seeing f2 through `GetMemory` with `invalidated_at` set, calls `Restore(f2)` (subject f2). It has its own
   chain, `prev = none`, and its twin lookup excludes f1 (C-4). f2 becomes visible, and the call is acknowledged.
3. A shard restore to a point before step 1, with replay.
4. The two subjects' chains are independent ("order across subjects is irrelevant"). If `Restore(f2)` replays first, it
   is a no-op. `Invalidate(f1)` then hides f2.

f2 ends hidden although the last acknowledged request on it un-hid it. `Durability.tla` has one fact subject and no twin.

**Recommendation.** A marker that resolves several memory ids takes the subject lock of each id, in sorted order, and
writes one `deletion_log` row per subject, each chained to that subject's predecessor. Alternatively, make the subject
`(document_id, content_hash)` for curation.

### C-9: minor: the chain predecessor is read by clock (`deleted_at DESC`) and by a subject id with no class. The spec reads application order.

- `deletion_log_subject_idx (namespace_id, subject_id, deleted_at DESC)`. The DeleteDocument SQL uses `ORDER BY
  l.deleted_at DESC LIMIT 1`. `deleted_at` is `now()`, the transaction start time.
- `Durability.tla` `TxnBegin` reads `LastOn("x")`, the last entry in **application order**.
- On one server, under a lock taken before `BEGIN`, the two agree only while the clock is monotone. They diverge after
  an NTP step back, or after a failover to a host whose clock is behind the last entry. That is exactly the clock
  dependence that N122 rejected and that `Durability_ClockOrder` must fail.
- N150 says the subject is `(class, id)`, but neither the column nor the index carries a class. A client-chosen
  `document_id` equal to a fact's UUID text shares the fact's chain when the lookup is "either kind".

**Recommendation.** Read the predecessor as the chain tip: the entry of the subject that no other entry names as
`prev_operation_id`, or `ORDER BY ins_seq DESC`. `ins_seq` is per shard and advanced across moves. Add `subject_class`
to the column, the key and the index.

### C-10: minor: the session-level subject lock survives a vanished API host for about 2 h 11 min, because no TCP keepalive is configured

N159 holds `pg_advisory_lock(engram_subject_lock_key)` on a direct connection until the intent put, and says "A crash
drops the connection and so the lock". That is true for a process crash, where the kernel closes the socket. It is not
true for a host loss or a network partition. An idle backend notices a dead peer only through TCP keepalive, and
`tcp_keepalives_idle = 0` means the OS default of 7,200 s plus probes. `client_connection_check_interval` runs only
during a query. The plan sets no keepalive (none appears in §9.1).

Until the backend notices, every marker on that subject fails `UNAVAILABLE` after 3 s: `DeleteDocument` of the
document, `Invalidate`/`Restore` of the fact, or the namespace delete. Help-previous for the subject also cannot run.
The relay election lock (N15) has the same exposure. That is outside this lens, but it would stall the outbox for
about 2 h.

**Recommendation.** Set `tcp_keepalives_idle/interval/count`, about 10 s/5 s/3 on the server, and `tcp_user_timeout` on
the direct connections. State the resulting bound for both locks.

### C-11: minor: the replay floor is described with two incompatible sources; `replay_floor_effective` is catalog-only

- §9.3 and §5.5.5 step 4: the replay reads "`min(catalog.shards.replay_floor, min over those objects`" in
  `_control/restores/{shard}/`). That is a blob listing, and it is correct.
- `catalog_schema.sql`: "replay uses `replay_floor_effective`, so a catalog restore that lost the lowered replay_floor
  cannot raise the floor". §3 says the same: "from `replay_floor_effective − 10 min`".
- But `replay_floor_effective = least(replay_floor, replay_floor_mirror)` is a generated column **in the same catalog
  row**. A catalog restore reverts both columns, so the floor read from it can rise, which is `Durability_CatalogLossNoBlobFloor`.

**Recommendation.** Say once that the replay floor is computed by listing the blob markers. Drop the claim from the
column comment, or remove the mirror column.

### C-12: minor: `fact_hidden(invalidate)` rows, including their free-text `reason`, survive the purge of a deleted document, and `Restore` of such a fact cannot write its `curation_log` row

- N145: the invalidate row "dies only with the namespace or with an explicit `Restore`". §5.4.2 Purge: "`fact_hidden`
  has no foreign key, so an `invalidate` row stays".
- `reason` is caller free text (≤ 1,024 bytes), and it survives the hard delete of the document it curated. That is an
  erasure gap, even if a small one.
- `Restore` of such a fact passes the `NOT_INVALIDATED` check. It must then `INSERT curation_log(content_hash, document_id,
  document_version)`, all `NOT NULL`. The fact row and the document's `curation_log` rows were both purged, so there
  is no source for these values, and the statement fails.
- The invalidation is useless once the document's tombstone hides everything through permanent `derived_hidden(document)`
  rows.

**Recommendation.** The document purge deletes the `invalidate` rows of its victims, which is safe because the document
cause hides the derived versions permanently. `Restore` of a fact whose document version is covered by a tombstone
returns `NOT_FOUND`.

### C-13: minor: the shard-truth repair rules do not cover a catalog backup that predates the move's (d) or a completed rollback, and the spec's `CatalogRestore` never reverts the `namespaces` row

The rules in §5.5.5 step 0 re-derive `namespace_moves.state` and raise epochs. They have gaps:
- **Routing row.** They never re-derive `namespaces.shard_id`. A catalog restored to before (d), or to before `Plan`,
  keeps naming the source. API traffic still works through the `moved_out` hint, but every catalog-driven procedure
  sees the namespace on S:
  - a later restore of T does not freeze or bump it (§5.5.5 step 2 iterates "every namespace the catalog lists on S");
  - the rebalancer plans from S.
- **Rollbacks.** No rule maps "source active, target row gone" to `rolled_back`. A lost rollback leaves the move
  `cutover` and the namespace `frozen` in the catalog.
- **A rule that can never fire.** "A target `active` with `move_epoch`" can never hold: `move_epoch` is set only on
  the source, and the CHECK requires `move_id`, which `activate_target` clears.
- **Spec.** `ShardMove.tla` `CatalogRestore` reverts only `cm`, never `cat`, so none of this is modelled.

**Recommendation.** Derive `namespaces.(shard_id, epoch, state)` from the unique `active`/`frozen` ownership row. Derive
`rolled_back` from "no target row and no `moved_out` source". Let `CatalogRestore` revert `cat` as well.

### C-14: minor: the stated RTO omits `ReconcileIn` and its index wait, and the whole shard waits for one moved-in namespace

§9.3 bounds the RTO at ≤ 60 min. §5.5.5 step 1 runs `ReconcileIn`, including "waits for the namespace's partial indexes
(step 2c)", and "Only then does the shard continue". For a target restored within a move's `committed`/`cleaning`
window, which rebalancing makes common, about 120 namespaces stay `frozen/restore` during:
- the re-copy of the whole mutable class;
- HNSW builds when the backup predates them (5–6 min per vector table per 1 M, bounded at 12 h).

**Recommendation.** Settle the moved-in namespace separately: reopen the others, and keep only that one `frozen` until
its `ReconcileIn` and indexes complete. Add the term to the RTO.

### C-15: minor: smaller seams in the round-5 commit and Materialize changes

- **A CAS lost to the commit's own zombie triggers a needless rebuild.** A timed-out attempt A and its retry B of one
  `CommitPageVersion` run concurrently with the same `commit_key`. Both miss the lookup. A wins the CAS, B blocks and
  then loses, and B rolls back and "re-run[s] `FullRebuild`". The blob guard is safe, because B's `NOT EXISTS` runs
  after A committed. But the activity Temporal waits on reports a loss for a version its own execution committed, and
  pays for an extra LLM rebuild and version. `RetryCommit` models only a sequential retry. Fix: after a lost CAS, run
  `engram_derivation_commit_seen` again in a fresh statement before declaring the loss.
- **Materialize stamp granularity (unverified).** §5.4.2 stamps `materialized_at` "in the batch that wrote the
  `derived_hidden(invalidation, f)` rows". A victim cited by versions across several batches would then be stamped by
  the first, and a run that dies after it leaves the remaining rows owed with no unstamped marker to find them.
  `Derivation.tla` stamps at `MatEnd`, after all batches. The prose should say "the last batch covering f", or the
  stamp should happen at run end, as the spec does.

### C-16: nit

- **`engram_seq_advance` can move the sequence backward.**
  - `setval(…, greatest((SELECT last_value …), p_to) + 1)` is not atomic with concurrent `nextval`.
  - Executed: with `last_value` = 1000 read, five concurrent draws 1001–1005, then `setval(1001)` makes the next draws
    1002–1006, which are duplicates.
  - Sequences cannot be `LOCK`ed (executed: "This operation is not supported for sequences").
  - The 10 min floor margin absorbs it and nothing requires uniqueness, but "it only moves forward" is false.
  - Fix: call `setval` only when `p_to > last_value`, with slack.
- **`ShardMove_NoTimelineCheck` is cited for the wrong thing.** It is a passing experiment about the mover's session
  timeline. Its state count equals the design's (36,807,699), so the knob is vacuous at these bounds. §5.5.1 step 8
  cites it as a cleanup-gate configuration, and §8.4.6 lists it in the must-fail column. The N159 cleanup "timeline
  check" has no must-fail configuration.
- **The sequence precondition of (b′) is not enforced.** §5.5.1 says it is "re-checked on the rows", but the trigger
  checks only the indexes. `CHECK (ready_at IS NULL OR w_final IS NOT NULL)` checks that `w_final` was recorded, not
  that the advance ran.
- **Subject-lock pool.** It has two direct connections per API process per shard. A blocked `pg_advisory_lock` (up to
  3 s) occupies one of them, so other subjects' marker writers queue behind it. Use `pg_try_advisory_lock` with
  polling.

## Checked and holding

- **The idempotency lookup before the CAS (N144).** The sequential retry returns the committed version and touches
  nothing. With concurrent attempts, the loser's blob-deletion guard runs after the winner committed, because the CAS
  row lock serialises them, so a committed key is never deleted. `NoPhantomVersion` holds under the order lookup → CAS
  → insert. `$expected` with `NULL` refused removes the blind root write.
- **`engram_holds_derivation_exclusive` (executed).** It identifies the exclusive xact advisory lock, including
  negative 64-bit keys, so the stamp guard is enforceable. The mover's `replica` role mode bypasses the guard on
  mutable-class merges, as required.
- **Help-previous.** Only the latest entry of a subject can lack its intent, because a successor cannot commit while
  the predecessor holds the lock and has not put. So helping the latest entry closes every hole. This matches
  `Durability_Chain` and `_NoHelpPrev`.
- **The recorded-epoch guard.** A late-put intent of a lost epoch is skipped after any applied entry of the newer
  epoch, whichever of two chain roots replays first. `Invalidate(e); Restore(e)` replays both. A row settled as
  `skipped` is re-evaluated by a later restore whose point precedes it, and is correctly final otherwise.
- **PreVerify's range argument (N148) for the insert-only class.** Rows below `F_copy` were visible to the bulk copy.
  Rows in `[F_copy, F_c)` were committed before `t_c` and are caught up. C-5 concerns `VerifyFK`, not this argument.
- **`engram_seq_advance` placement.** It runs at Plan and at (b′) before `ready`, and is re-run on a target restore.
  The 10 min export and watermark deferral keyed on `moved_in_at` closes the ring gap.
- **`ShardMove_TgtRestore.cfg` with all four omitted invariants added** (`NoRouteToTargetBeforeC`,
  `NoWriteToTargetBeforeC`, `ZombieCannotCutOver`, `RestoreReconciles`). It passes: 9,688,960 distinct states, depth
  29.

## Counts

- blocker 1 (C-1)
- major 5 (C-2 to C-6)
- minor 9 (C-7 to C-15)
- nit 1 (C-16, four items)

Regressions or incomplete fixes of round 5: C-1 (N159(7), N149), C-2 (N159(7)), C-3 (N149), C-4 (N145(3)), C-6 (N153)
and C-7 (N146).

## Verdict

The round-5 core holds where its specs model it: the commit idempotency, recorded epochs, help-previous with the subject
lock, and the PreVerify range. The new defects again sit where the prose or SQL goes beyond the model.

The most serious one is the cleanup gate that N159(7) rewrote. Its unconditional `ReconcileIn` treats every row
missing from the target as lost. It therefore re-inserts what the target deleted after activation, so an acknowledged
document delete, an acknowledged `Restore` and REPLACE retirements are undone on every move (C-1, executed in TLC and
SQL). Its content check compares a target that legitimately diverges with a static source, so cleanup never completes
and the source copy of erased documents is never removed (C-2). Both follow from one missing concept: the move does
not know which target deletions happened after activation. `ShardMove.tla` has no action that would show it.

Besides that:
- the target-restore repair is weaker than the spec's (C-3);
- `Restore` cannot undo the twin half of an `Invalidate` (C-4, executed);
- PreVerify's `VerifyFK` rolls back every move of an ingesting namespace (C-5, executed);
- the N153 index wait widens the freeze, and its trigger test refuses `ready` for active namespaces (C-6).

Until C-1 and C-2 are fixed, neither "an acknowledged delete survives" nor "no data loss across a move" holds for moved
namespaces, and moves cannot complete their cleanup. Until C-5 and C-6 are fixed, moves of active namespaces do not
terminate as written. Each fix is local: a post-activation deletion record or a restricted repair, a backup-based gate,
the full re-copy for copy-phase restore points, a symmetric twin set, a parent-aware PreVerify `VerifyFK`, and a final
catch-up after the index wait. Each also needs the spec action that would have caught it (`TgtDelete`, the twin rule,
FK edges in the move model).

## What I could not verify

- **The TLC design configurations.** I did not re-run them; their logs report a pass. My runs were the extended
  `ShardMove_TgtRestore` and `SMDel` (a reduced bound with the catalog restore off).
- **The `ReconcileIn` statements.** None exist in SQL. C-1 and C-3 rest on the prose definition ("insert-only rows with
  `ins_seq ≥ F_pre` and the whole mutable class, `ON CONFLICT DO NOTHING`") and on the DDL's visibility functions.
- **Cancellation of the sync wait by a pgx context or `statement_timeout`.** I cite PostgreSQL's documented
  behaviour; I did not reproduce it on a replicated pair.
- **Materialize's exact batch boundaries (C-15).** Neither the prose nor the SQL fixes them.
- **Whether the outbox relay's election lock (N15) has a keepalive elsewhere** (C-10 mentions it only in passing).
