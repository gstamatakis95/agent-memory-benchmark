# Review round 6

Three parallel reviews (correctness, Postgres/operations, API/numbers/parity).

---

## Review round 6: correctness and concurrency (C-1 to C-16)

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

---

## Review round 6: Postgres, Temporal and operations after round 5 (P-1 to P-12)

**Scope.** This review covers the D24 storage and operations design as it would run on PostgreSQL 16 with pgvector,
pgbouncer and pgBackRest. Topics:

- the cost-based semantic plan (N151): θ = 50 k, a scan bound of at least 20 k, the exact fallback up to 4 θ,
  `enable_bitmapscan`/`enable_sort` off, the `MATERIALIZED` CTE, and the 500-document `$allowed_docs` rule;
- partition-level hygiene with `REINDEX INDEX CONCURRENTLY` (N152);
- index readiness gating cutover (N153);
- the covering link indexes and the 6.5 M / 10 M shard on 128 GB (N154);
- pools 40/20 and the arm semaphore (N155);
- the session-level subject lock (N159);
- the catalog's synchronous standby (N146);
- WAL pacing, restore and failover runbooks, and move cost.

Findings dispositioned in rounds 1 to 5 are not repeated unless the fix is wrong or incomplete. `[regression: X]`
marks a defect introduced by decision X. Three findings overlap reviews written in parallel this round: P-4 with
`round-6-correctness.md` C-6, P-5 with C-7 and C-10, and P-7 with `round-6-api.md` A-1 and A-2. They are kept here
because this review adds executed evidence or a failure the others do not cover, and each says what it adds.

## How the evidence was produced

**Server.** The shared scratch PostgreSQL 16.15 with pgvector 0.8.6 runs on 4 vCPUs and 15 GB of RAM, with
`shared_buffers` at 128 MB (unchanged).
- Buffer counts (hits plus reads) are the primary metric.
- Timings are OS-cache-warm and come from a slower, smaller machine than the 8 vCPU / 32 GB `shared_buffers` shard.
  Treat them as relative unless a finding says otherwise.

**Schema.** Database `r6pg_a` (mine, dropped at the end) received `sql/shard_schema.sql` unchanged, except that
`pg_search` and the `USING bm25` indexes were stripped.

**Data.** Rows were inserted through the real DDL, with FK triggers bypassed by `session_replication_role = replica`:
- **Namespace A:** 1,000,000 facts and vectors, 10,000 documents of 100 facts, 10 chunks per document spread over
  time.
- **Namespace B:** 100,000 facts on the same partition `fact_vectors_p00`.
- **Namespace G:** 200,000 facts with 6.0 M `fact_links` rows (10 temporal, 5 semantic and 15 entity links inserted
  per fact, `src` the older endpoint).
- **Embeddings:** `l2_normalize(centroid[doc mod 64] + two scaled noise vectors)` as `halfvec(768)`, so a document's
  facts share a topic.
- **Indexes:** the partial HNSWs come from `engram_hnsw_ddl`.

**Queries.** The §3.8 SQL was run verbatim with the N151 `SET LOCAL`s (`force_custom_plan`, `enable_seqscan = off`,
`max_parallel_workers_per_gather = 0`, and on the HNSW path `enable_bitmapscan = off, enable_sort = off`,
`ef_search = 150`, `relaxed_order`).
- **Off-topic filters** admit the documents of topics other than the query's.
- **E** is the exact eligible count.

**Catalog.** A throw-away PostgreSQL 16 cluster on port 55501 (created and deleted by me) was configured with the
plan's synchronous-replication settings.

---

## Findings (ranked)

### P-1: major [regression: N151(4)]: on the HNSW path, `document_id IN (SELECT unnest($1))` becomes a nested-loop semi-join that scans the whole allowed-document list for every visited tuple. Filtered arms take 1.4 s at 1,254 documents and 5.3 s at 5,000, past the 5 s read `statement_timeout`

**Where.** N151(4): "`$allowed_docs` is passed as one array constant only up to **500** documents; above that the arm
uses `document_id IN (SELECT unnest($1))` (no per-element estimation …)". The same rule appears in §3.3.4, §3.8 and
§9.1 (`allowed_docs_array_max: 500`). N151(3) adds "the HNSW path adds `SET LOCAL enable_bitmapscan = off,
enable_sort = off`".

**Evidence (executed on A, query topic 5, documents of topics 10 to 17 admitted, E = 125,400, `max_scan_tuples` =
20,000).**

| `$allowed_docs` form | Documents | Plan | Execution | Planning |
|---|---|---|---|---|
| `IN (SELECT unnest($1))` (the plan) | 1,254 | `Nested Loop Semi Join`, inner `Materialize` of the unnest, **20,708,001 rows removed by join filter** | **1,447 ms** | 2.4 ms |
| `IN (SELECT unnest($1))` | 5,000 (1,254 real) | same | **5,314 ms** | 2.2 ms |
| `= ANY ($1)` as a custom-plan constant (hashed `ScalarArrayOp`) | 1,254 | filter inside the HNSW scan | **232 ms** | 4.8 ms |
| `= ANY ($1)` | 5,000 | same | **219 ms** | 12.8 ms |
| `= ANY ((SELECT $1)::text[])` (InitPlan, linear search) | 1,254 | filter inside the scan | 577 ms | 1.8 ms |

All rows touch the same ≈ 53.6 k buffers. The difference is CPU in the join filter.

**Why it happens.** `ORDER BY embedding <=> q LIMIT 150` needs ordered output.
- A hash semi-join does not keep the outer order, so the planner does not consider it order-preserving.
- With `enable_sort = off`, the only cheap order-preserving plan is a nested loop. Its inner side is the materialised
  list, scanned in full for every candidate the iterative scan visits.
- The cost is O(visited × |docs|).

**Why it is the common case, not an edge.** The HNSW path runs only at E ≥ θ = 50 k.
- With at most 100 facts per document, that already means at least 500 documents.
- At §3.7's sizing (10 M facts in 1 M documents, ≈ 10 facts per document) it means at least **5,000** documents.
- So nearly every HNSW-path filtered arm takes the semi-join form. Near 5 k documents it fails at the 5 s read
  timeout.
- The exact path is not affected. Its `IN (SELECT unnest)` becomes `HashAggregate(unnest)` → nested loop into
  `facts_doc_idx`, which is correct (see "Checked and holding").

**Recommendation (decision level).**
- On the HNSW path, always pass `$allowed_docs` as `= ANY ($1)` with the array as a custom-plan constant, which the
  executor evaluates as a hashed lookup. Accept the planning cost of 4.8 ms at 1.3 k documents and 12.8 ms at 5 k.
  Keep `IN (SELECT unnest)` for the exact path only.
- Alternatively, route arms with more than N documents to the exact path when E allows, and refuse larger sets in
  the planner.
- Add to M0.6's `EXPLAIN` pins: the HNSW shape must contain no `Nested Loop Semi Join` and no `Join Filter` on
  `document_id`.
- Run `TestRecall_FilteredArm` at 5,000 allowed documents.

### P-2: major [fix incomplete: r5 P-1, N151(6), N155]: the filtered-arm budget is 7 to 10 times too low. An arm at θ costs ≈ 200 k buffers and 0.3 s, not "≈ 20–30 k buffers". Arms below θ are not budgeted at all, and the stated filtered share overruns the CPU and connection budgets

**Where.** N151(6) and §9.1 IOPS row: "a filtered vector arm at θ costs ≈ 20–30 k buffers … the budget assumes ≤ 20 %
of recalls carry a filter with `E ≥ θ`". N155: "four 60 ms arms … ≈ 305 connection-ms". §9.1 CPU row: "recall ≤ 4"
vCPU.

**Evidence (executed on A, all fact types, warm OS cache).**

| Path | E | Buffers | Time |
|---|---|---|---|
| exact (§3.8 SQL, `MATERIALIZED` CTE) | 47,100 (just below θ) | **197.8 k** (4.2 per row; 22 k reads with 128 MB `shared_buffers`) | **311–364 ms** |
| exact | 62,800 | 263.7 k | 340 ms |
| exact (fallback at ≈ 2.5 θ) | 125,400 | 526.6 k | 839 ms |
| exact (fallback at ≈ 4 θ) | 187,800 | 788.6 k | **1,111 ms** |
| HNSW, off-topic, `max_scan_tuples` 20 k, `= ANY` form | 125,400 | 53.9 k | 232 ms |
| HNSW, off-topic, 100 k | 62,800 | 74.8 k | (989 ms with the P-1 form) |
| HNSW, filter includes the query's topic | 62,800 to 125,500 | 3.7 k | 10–25 ms |

**What this shows.**
- **The stated figure is the cold-read count, not buffers.** "20–30 k" matches the cold reads of an exact arm at θ
  (≈ 0.5 per row; 22 k measured). The CPU and buffer-mapping cost is ≈ 200 k buffer touches.
- **Arms with 10 k ≤ E < θ are in no budget.** They run the exact path at 4.2 buffers per row: 10 k eligible ≈ 42 k
  buffers, four MID recalls' worth. Only recalls with `E ≥ θ` are counted as "filtered".
- **At 4 θ the fallback is a 1.1 s exact pass on top of the HNSW attempt**, ≈ 1.3 s on one connection.
- **CPU.** 20 % of 50 QPS is 10 filtered recalls/s. With 2 to 3 vector arms each (facts, chunks, observations) at
  0.25–0.35 s, that is **5 to 10 vCPU-seconds per second**, against "recall ≤ 4" of the 8 vCPUs. The CPU here is
  slower than production, but not 2.5×.
- **Connections.** The same mix adds ≈ 7 to 10 Erlangs to N155's 15. That gives ≈ 22 to 25 Erlangs on two 16-slot
  semaphores (ρ ≈ 0.7 to 0.8 per process). The "≤ 4 ms pool wait" term and `TestReadSession_ConcurrentArms`'s
  "≤ 320 connection-ms" do not hold for filtered recalls.

**Recommendation (decision level).**
- Restate N114 and N155 with per-arm cost, not per-buffer cost: exact ≈ 4 buffers and ≈ 6 µs CPU per eligible row,
  and HNSW under correlation ≈ 50 k buffers.
- Pick one of these:
  - (a) Lower θ to what fits the 60 ms arm budget (≈ 10 k eligible rows), with fallback up to 4 θ. Accept and state
    a separate filtered-recall SLO, for example p95 ≤ 1 s.
  - (b) Keep θ and give filtered arms their own semaphore and SLO, so they cannot consume the unfiltered recall's
    connections and CPU.
- Either way, M0.6 must report the share of recalls with `E ≥ 10 k` (not only `E ≥ θ`) on the benchmark traces.

### P-3: major [fix incomplete: r5 P-6 and P-14, N154]: the hot set at 6.5 M is ≈ 88 GB with an index-only hop and ≈ 107 GB with the hop as written, not 73 GB. The 90 % B-tree fill assumption is false, and the graph hop reads the `fact_links` heap

**Where.**
- §3.7: "B-tree leaf fill 90 %"; "Hot set ≈ 73 GB at target … links ≈ 29 GB".
- N154(1): "both indexes cover the hop … so a hop reads two index ranges and never the heap".
- §3.8 graph SQL: `(l.src_memory_id = ANY (h.frontier) OR l.dst_memory_id = ANY (h.frontier))`.
- N154(3): "if the measured hot set at 8 M is ≤ 80 GB, the target returns to 8 M".

**Evidence 1: B-tree fill (executed).**
- **Link indexes.** 6.0 M link rows were inserted in commit order, four namespaces interleaved in one table, each
  fact's links inserted with it. The PK `(ns, src, dst, type) INCLUDE (weight)` measured **101 B per row** (random
  `src`, ≈ 69 % fill). `fact_links_reverse_idx` measured **123 B per row**.
- **Why the reverse index is worse.** Its `dst` ascends within each namespace, but every namespace's insertion
  point except the last one's is mid-index. PostgreSQL splits those pages 50/50, so the left halves stay half full.
- **Plain PK, 2 M rows.**
  - A two-uuid PK filled by one namespace in ascending order: **49.7 B per entry** (≈ 90 %).
  - The same rows with **8 namespaces interleaved**, as on a shard partition of ≈ 7.5 namespaces: **85.0 B**
    (≈ 52 %).
- **Sized versus measured.** §3.7 sizes the link indexes at 70 and 67 B per row (21 and 20 GB per 300 M). At the
  6.5 M target (195 M links) the measured rates give **19.7 + 24.0 = 43.7 GB**, not ≈ 29 GB.
- **Same effect elsewhere.** It applies to every per-namespace ascending key in a shared partition:
  - `facts` and `fact_vectors` PKs (UUIDv7);
  - `fact_vectors_model_idx`;
  - `facts_mentioned_idx`;
  - the `ins_seq` indexes (round 5 measured 53 B there).
  The "B-trees 5 GB" term is therefore also understated, by roughly 1.7×.
- **Move-ins inherit it.** A namespace moved in by PK ranges inserts mid-index into the target's partitions in the
  same way.

**Evidence 2: the hop reads the heap (executed `EXPLAIN (ANALYZE, BUFFERS)` of the §3.8 recursive CTE on G).**
- The `OR` across `src` and `dst` is planned as `BitmapOr` → **`Bitmap Heap Scan on fact_links_p14`**. An index-only
  scan cannot serve an `OR` across two indexes.
- **Measured per hop:**

| Family (20 seeds) | Heap blocks | Buffers | Time |
|---|---|---|---|
| temporal MID | 49 | 1.8 k | 3.9 ms |
| entity+semantic MID | **498** | 3.6 k | 8–11 ms |

- **Resident set.** 4,000 seeds, which is 200 MID recalls (four seconds of one shard's 50 QPS), touched **47,533 of
  74,059 heap blocks (64 %)** of namespace G's link heap.
- **The link heap is therefore resident.** At 96.5 B per row measured (§3.7: 100 B) it is **≈ 18.8 GB at 6.5 M**.
- **The fix works.** Rewritten as `UNION ALL` of a `src = ANY` and a `dst = ANY` branch, the same hop is two
  `Index Only Scan`s with `Heap Fetches: 0` and 444 buffers.

**Arithmetic.**
- Hop rewritten, index fill corrected: 73 − 29 + 43.7 ≈ **88 GB**.
- Hop as written: + 18.8 ≈ **107 GB**.
- Usable cache is "≈ 80 to 96 GB". The 6.5 M target is in the regime N154 used to reject 8 M.
- Scaling linearly, the hot set reaches 80 GB at ≈ 5.8 M facts with the index-only hop and ≈ 4.9 M with the hop as
  written.

**Recommendation (decision level).**
1. Rewrite the hop as `UNION ALL` of two index-only branches (`causal` forward only) and pin the plan in M0.6.
2. Decide the B-tree fill:
   - either run periodic `REINDEX INDEX CONCURRENTLY` of the link and per-namespace PK indexes per partition through
     the index runner, with WAL paced, transient space and the schedule stated;
   - or size at ≈ 52 % for ascending per-namespace keys and ≈ 69 % for random keys.
3. Re-derive D3. Either lower the target to ≈ 5.5–5.8 M, or adopt now the 192 GB instance that N154 records as the
   fallback.
4. Make M0.6 build its indexes in production insertion order (interleaved namespaces, per-commit), not by bulk load
   plus `CREATE INDEX`, which yields 90 % fill and would hide this.

### P-4: major [regression: N153; corroborates `round-6-correctness.md` C-6 with execution]: the readiness trigger refuses a valid index that holds every row, and the freeze re-copy into built graphs is expensive

**Where.** `engram_move_indexes_ready`: `i.rows_at_build < 0.99 * n.c`, with `n.c` the **current** count, called by
`engram_check_ownership` on `ready_target` and `reconcile_in`. §5.5.1 step 4: "Freeze work = the rows inserted since
`T_pre − 10 min` … < 30 s". §3.7: "`CommitChunk` ≈ 10 inserts into a ≤ 1 M-element index ≈ 2 to 5 ms of HNSW".

**Evidence (executed).**
1. A had a `ready` row with `rows_at_build` = 1,000,000. 16,000 vectors were then inserted into A, and the valid
   index maintained them (`indisvalid = t`, `indisready = t`).
   `SELECT engram_move_indexes_ready(A)` returned **false** (1.6 % above `rows_at_build`), and took **377 ms** inside
   what would be the frozen (b′) transaction. Any namespace whose post-build inserts exceed 1 % is therefore refused
   at (b′), exactly as C-6 describes.
2. **The cost of the rows the freeze re-copies into a built graph** (inserts into A's 1 M-element HNSW, OS-cache-warm,
   128 MB `shared_buffers`):
   - **≈ 15 ms per insert** (5,000 inserts in 74 s);
   - **28 WAL records** per insert;
   - **≈ 120–143 KB of WAL per inserted vector** with `wal_compression = zstd` (18 to 22 full-page images per insert
     after a checkpoint).
   The latency is cache-bound on this machine and is unverified for a 32 GB `shared_buffers` shard. Even at the plan's
   ≈ 0.3 ms, a re-copy that spans the index wait is not "< 30 s" for an active namespace.

**Recommendation.** Adopt C-6's ordering: PreVerify catch-up → `AwaitIndexes` → a final catch-up that fixes `T_pre`
→ Freeze.

For the readiness predicate, use "an index named by `engram_hnsw_ddl` exists with `indisvalid ∧ indisready` for every
`(vector table, current model)` with ≥ 2,000 vectors". A valid index contains every row by construction, so the 0.99
ratio guards nothing. The predicate also drops the 377 ms count from the freeze. Keep the verified-count check in
`AwaitIndexes` only.

### P-5: major [fix incomplete: N146; extends `round-6-correctness.md` C-7]: the catalog's synchronous standby, as configured, does not start; a cancelled commit becomes visible unreplicated; and after the agent's automatic promotion every catalog write blocks, including the "catalog first" epoch write of a shard failover

**Where.**
- N146 and §9.1: `synchronous_standby_names = 'FIRST 1 (catalog-standby)'`.
- N146: "If the standby is down, catalog writes block … Promotion of the standby is RPO 0. **Guarantee (decided): a
  catalog commit that a move, a delete ack or an epoch bump observed survives a catalog failover**".
- §9.6: "Primary down → the agent promotes the standby after 30 s unreachable and flips the alias".
- §9.3 failover: "write the new epoch (catalog first …)".

**Evidence (executed on a throw-away PostgreSQL 16).**
1. **The setting is a syntax error.** With `synchronous_standby_names = 'FIRST 1 (catalog-standby)'` the server
   logs `invalid value for parameter "synchronous_standby_names" … syntax error at or near "-"` followed by
   **`FATAL: configuration file … contains errors`**, and does not start. A standby name with a hyphen must be
   double-quoted: `'FIRST 1 ("catalog-standby")'`. The `config lint` that N146 relies on would generate the failing
   value.
2. **`statement_timeout` does not end the synchronous wait.** `SET statement_timeout = '2s'; BEGIN; UPDATE
   namespace_moves SET state = 'committed' …; COMMIT;` with no standby connected waited **130 s** in `SyncRep`.
   Other sessions still read `cutover`. This corrects C-7's "or a `statement_timeout`".
3. **A cancel does end it, and the commit then succeeds.** `pg_cancel_backend(pid)`, which pgx sends when a context
   deadline expires, produced `WARNING: canceling wait for synchronous replication due to user request … already
   committed locally` and **`COMMIT`**. Every other session then read **`committed`**, which no standby has. So:
   - "writes block" holds only for callers without a deadline;
   - every Go caller with a deadline (activities, the 40 s delete-class RPCs) turns a standby stall into silent
     asynchronous commits;
   - this is C-7's interleaving, now executed.
4. **After the promotion the new primary has no synchronous standby** (two-node set), so every catalog write waits
   until a standby is re-seeded or an operator runs `catalog degrade --async`. Item 3 shows what a deadline does to
   those waiting writes. That blocks:
   - the shard failover and restore paths, which write the epoch "catalog first";
   - the move CAS;
   - namespace and tenant lifecycle, including the catalog `deleting` write behind a `DeleteNamespace` ack.

   A single fault (catalog primary loss) therefore disables shard failover until an operator acts. The runbook
   lists "standby streaming" only as a verification step.

**Recommendation (decision level).**
- Quote the names: `'FIRST 1 ("catalog-standby")'`. Better, run **two** standbys with `ANY 1 ("catalog-s1",
  "catalog-s2")`, so a promotion leaves a synchronous peer and RPO 0 survives one failure.
- Catalog writers commit on a non-cancellable context: `context.WithoutCancel` for `COMMIT`, with pgx cancel
  disabled on that call. A commit that returns the `canceling wait for synchronous replication` notice is an
  unknown outcome, and no protocol step may act on the row until the standby confirms it (`pg_stat_replication`
  `replay_lsn` ≥ the commit LSN).
- Run `catalog reconcile --from-shards` after every promotion (as C-7 asks), and make the failover agent refuse to
  promote a standby that was not `sync_state = 'sync'`.
- Add `TestCatalog_CancelledCommitNotObserved` and a config-lint test that starts PostgreSQL with the generated
  value.

### P-6: major: HNSW insertion WAL is dominated by full-page images, ≈ 120–140 KB per inserted vector after a checkpoint; the ingest WAL budget of "≈ 13 KB per fact" and the backup arithmetic built on it are about 10× low

**Where.**
- §9.1 Disk: "ingest WAL ≈ 13 KB per fact ≈ 13 GB/day at 1 M facts/day".
- §9.3: "WAL replay ≈ 1.2 min per GB (15 min for the 13 GB of a nominal day)"; "≈ 13 GB/day of ingest WAL";
  "retention 4 full sets + WAL between".
- §9.1 GUCs: `checkpoint_timeout` 15 min, `max_wal_size` 8 GB.

**Evidence (executed, `wal_compression = zstd`, three batches of 1,000 inserts into A's 1 M-element graph right after
`CHECKPOINT`, `pg_stat_wal` deltas, no checkpoint during the run).**

| Batch | FPIs | WAL per vector | Records per vector |
|---|---|---|---|
| 1 | 21,865 | 143 KB | 28 |
| 2 | 19,855 | 130 KB | 28 |
| 3 | 18,203 | 120 KB | 28 |

- **What each insert touches.** It writes 28 generic-WAL records (the new element and the neighbour lists it
  updates). The first touch of a graph page after a checkpoint is an ≈ 6.5 KB compressed FPI; HNSW pages are
  float-dense and compress poorly.
- **FPI share falls as expected.** It declines with the share of the graph already imaged, as a Poisson model of
  28 random page touches per insert over ≈ 250 k pages predicts.
- **At the plan's own "1 M facts/day":** ≈ 11.5 k vector inserts per 15 min checkpoint is ≈ 320 k page touches. Over
  1–1.6 M graph pages that is ≈ 180–290 k FPIs, ≈ **1.2–1.9 GB per checkpoint, ≈ 110–180 GB/day**, against
  13 GB.
- **At D3's average fill rate** (≈ 81 k facts/day/shard): ≈ **15 GB/day**, against ≈ 1 GB.

These per-day figures are modelled from the measured per-insert behaviour and depend on the checkpoint spacing.

**Consequences.**
- **Backups.** The 13 GB WAL watcher (N157) fires several times a day per shard at the example rate. Each
  differential re-reads most graph segments.
- **Repository.** WAL kept for the 28-day window grows ≈ 10× (≈ 3–5 TB per shard at the example rate).
- **RTO.** The "nominal day" term is a few hours, not a day.
- **Checkpoints.** A move-in, a backfill or a hygiene burst (P-10) that pushes WAL past `max_wal_size` forces
  requested checkpoints. That shortens the interval and raises the FPI share further.

**Recommendation.**
- Restate the WAL budget per inserted vector as FPI-dominated: WAL ≈ min(page touches, graph pages) × FPI size per
  checkpoint interval, plus ≈ 5–10 KB per insert in records.
- Decide `checkpoint_timeout` and `max_wal_size` for it (30–60 min and ≥ 32 GB cut FPIs by the same factor, at a
  longer crash recovery).
- Re-derive the backup cadence, the repository size and the RTO term.
- M1.7 measures WAL per fact on the real image with production `shared_buffers` and checkpoint settings.

### P-7: minor [extends `round-6-api.md` A-1]: the arm semaphore and per-process pool are sized for exactly two healthy API processes; while one drains or restarts, the survivor carries the shard's load on 16 slots

**Where.** N155: "per API process pool **20** (two processes) … at most **16 concurrent arm transactions per API
process per shard** … queueing at ρ ≈ 0.47 on 32 servers is < 2 ms p95". §9.1: `engram-api` ×2; Envoy drains for up
to 340 s on rollout.

**Evidence (Erlang-C, 61 ms per arm transaction from N155's 305 connection-ms over five transactions).**

| Load on one process | Slots | P(wait) | p95 wait per arm transaction |
|---|---|---|---|
| 7.5 Erlangs (two healthy processes) | 16 | 0.005 | 0 |
| 15 Erlangs (one process drained or down) | 16 | **0.73** | **164 ms** |
| 22–25 Erlangs (P-2's filtered mix, one process) | 16 | unstable queue | — |

At least two arm transactions are in series on the critical path, so recall p95 leaves the 300 ms SLO for the whole
of every rollout and restart.

**Recommendation.** Make the shard-wide bound a pgbouncer fact, as A-1 also proposes (a worker alias, recall
pool 32). Size the per-process semaphore so that N − 1 processes carry the load (32 per process), or run three API
processes, and state the degraded mode.

### P-8: minor [fix incomplete: N152]: nothing stops purges during the partition rebuild set, so the closing `VACUUM (INDEX_CLEANUP ON)` repairs graphs; `Storage.tla` hides this by making `Rebuild` and `Vacuum` atomic

**Where.** N152(2): "a partition is never vacuumed with index cleanup while an un-rebuilt touched graph sits on it".
`Storage.tla`: `Vacuum` requires `VacuumReady` in the same step, and `Purge` removes the namespace from `rebuilt`.

**Evidence (from the documented behaviour and the plan's text).**
- The runner selects the set (`purged_since_build > 0`) and then runs one `REINDEX INDEX CONCURRENTLY` after another.
  At this target that is minutes per partition: 1 M vectors measured 547 s here, 5–6 min in §9.2. Then it vacuums.
- The expunge's `PurgeBatch` keeps deleting in the meantime. No lock, pause or re-check appears in §5.4.2, §9.2 or
  the SQL.
- Three kinds of tuple deleted in that window are dead TIDs in some graph:
  - a tuple deleted after a rebuild's snapshot, which is still in the rebuilt graph;
  - a tuple of a namespace not in the selected set;
  - a tuple deleted before the `VACUUM` starts.
- `VACUUM (INDEX_CLEANUP ON)` collects them and runs `ambulkdelete` on every HNSW with dead TIDs. That is the repair
  path N152 exists to avoid, and its cost grows with the dead count.
- In the spec, a purge after `Rebuild` disables `Vacuum` until another rebuild. The implementation checks once, at
  selection.

**Recommendation.**
- Take a per-partition advisory key: purge batches take it shared (try-lock, skip the partition while hygiene holds
  it), and the runner holds it exclusively from selection until `VACUUM` has started.
- Alternatively, have the runner re-read `purged_since_build` immediately before `VACUUM` and rebuild again or skip.
- Model `Rebuild` as snapshot then publish, and `Vacuum` as start then collect, in `Storage.tla`.

### P-9: minor [regression: N152]: the partition unit has unbounded amplification. One due small graph rebuilds every touched neighbour, however slightly touched, and no rate budget exists

**Where.** N152(2): rebuild "**every** HNSW on that partition with `purged_since_build > 0`". §3.7: "hygiene is
≈ 0.35 ms per vector" (per rebuilt vector, no rate).

**Evidence.**
- Rebuilt vectors per purged vector = Σ touched graphs on the partition ÷ the due graph's threshold.
- **Example.** A 20 k-vector namespace reaches its 2 k floor while a 1 M-vector neighbour has one purged row. The
  event rebuilds 1.02 M vectors: 547 s and 1.76 GB of WAL measured here (≈ 6 min in §9.2). That is ≈ 510 rebuilt
  vectors per purged vector, against 10 under a per-graph rule.
- **Frequency.** `REPLACE` and re-extraction purge continuously (the "retire purge"), so large namespaces are almost
  always touched. Every hygiene event on a partition rebuilds ≈ all of its graphs: ≈ 400 k fact vectors at target,
  plus chunk and observation graphs. Under churn this happens daily per partition.
- **The rejected alternative is not equivalent.** N152 rejected deferring the vacuum, but a graph with a handful of
  dead elements costs far less to repair than to rebuild. Round 5's "≈ 1× at 0.2 %" is at 600 dead elements; the
  repair scales with dead × in-degree.

**Recommendation.**
- Rebuild a neighbour only above a small fraction (for example `purged_since_build ≥ 0.05 % × rows_at_build`) and
  let the partition vacuum repair the rest.
- Or state the rate model: rebuild-seconds per day per partition, as a function of churn, against the shard's single
  build slot.
- Extend `TestIndex_Hygiene` with a 1 M neighbour touched once.

### P-10: minor [fix wrong: r5 P-10]: rebuild WAL cannot be "paced by the purge budget": a build emits its WAL as one burst at its end

**Where.** N152(4): "Rebuild WAL (≈ 1.5–1.8 KB per vector) is paced by the purge budget (N119's 25 MB/s, shared)".
§9.6: "Rebuild WAL … is paced by the shared purge budget".

**Evidence (executed: `REINDEX INDEX CONCURRENTLY` of B's 100 k graph, 22.7 s, with `pg_current_wal_insert_lsn` sampled
every second).**
- For the first 20 s: 0.0 MB per second.
- Then **164 MB in one second** and 21 MB in the next.
- The fresh 1 M build wrote 1.76 GB in the same way: pgvector writes the finished graph with `log_newpage_range`.
- One statement cannot be paced from outside. At 1 M vectors the burst is ≈ 1.5–1.8 GB at hundreds of MB/s into
  `pg_wal` and the archive queue: ≈ 35 s of lag at the assumed 50 MB/s archive rate.

**Recommendation.**
- Account rebuild WAL as debt: the next purge batches wait WAL ÷ 25 MB/s.
- Start a rebuild only while archive lag < 30 s and `pg_wal` has room for the burst.
- State the burst in §9.1.

### P-11: minor: the graph wave cannot be "cut at its sub-budget" with the nodes kept, and the visibility join runs on every candidate link before the per-hop `LIMIT`

**Where.** §1 latency table: "graph wave 2 … 30 ms | the wave is cut at its sub-budget; nodes visited so far are
kept". §3.8: the graph arm is one recursive CTE statement.

**Evidence (executed on G).**
- **The join comes first.** In the hop's plan, the `facts` PK join and the marker anti-join run on every candidate
  link. `GROUP BY … ORDER BY max(weight) … LIMIT` comes afterwards.
- **Cost at HIGH.** The entity+semantic family at budget 1,000 and 2 hops joined 33.8 k candidates: **163 k buffers,
  796 ms**. MID (300) took 8–11 ms.
- **No partial result.** One statement cannot return "nodes visited so far". Cutting it means cancelling it and
  losing the wave.

**Recommendation.**
- Run one statement per hop from Go, so the cut keeps the completed hops.
- Order and limit candidate links per frontier node, from the index-only branches of P-3, before the visibility join.
- Budget the HIGH graph arm explicitly.

### P-12: nit

- **`allowed_docs` threshold drift.** §9.1's configuration has both `visibility.allowed_docs_join_above: 8000` and
  `recall.allowed_docs_array_max: 500`. The first is the pre-N151 value.
- **Connection totals disagree.** §9.1 says "≈ 53 of 100"; §2.2.7 says "≈ 57" with the subject leases (also
  `round-6-api.md` A-2). The subject-lock keepalive gap is `round-6-correctness.md` C-10 and is not repeated here.
- **`relaxed_order` ordering.** With `hnsw.iterative_scan = relaxed_order` and no outer re-sort, the 150 rows can be
  slightly out of distance order. pgvector documents a re-sort in a `MATERIALIZED` CTE. State that RRF tolerates it,
  or add the re-sort.

---

## Checked and holding

- **The exact path above 500 documents** plans as `HashAggregate(unnest)` → `facts_doc_idx` → `fact_vectors` PK, with
  the distance in the `MATERIALIZED` CTE and a top-N heapsort. It costs 4.2 buffers per eligible row and returns
  `min(cap, E)`.
- **The HNSW path with `enable_bitmapscan`/`enable_sort` off** chose the namespace's partial HNSW in every run. No
  `ins_seq` bitmap scan appeared (r5 P-7 is fixed).
- **The `fact_hidden` anti-join** with 20 k rows is a parameterised `Index Only Scan` per candidate, not a
  materialised scan.
- **The 20 k floor on `max_scan_tuples`.** Off-topic filters at 6–19 % of A returned 150 rows. This data does not
  reproduce round 5's zero-row truncation (r5 P-1 is fixed as far as this data can show). The fallback path was not
  exercised by the synthetic topics.
- **Builds.**
  - 100 k vectors: 24 s and 176 MB of WAL.
  - 1 M vectors: 547 s on 4 vCPU (≈ 0.55 ms per vector; the plan's 0.28–0.37 ms at 8 vCPU is plausible), 1.76 GB of
    WAL, a 1,946 MB index (≈ 2 KB per element, as N114 says).
  - `REINDEX INDEX CONCURRENTLY` of 100 k: 22.7 s, the same as a fresh build.
- **Link heap row:** 96.5 B (§3.7: 100 B).
- **`statement_timeout` does not release a synchronous-replication wait.** "Writes block" holds for callers without
  a cancel (P-5 covers the cancel case).

## Counts

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 6 | P-1 to P-6 |
| minor | 5 | P-7 to P-11 |
| nit | 1 | P-12 |

## Verdict

The round-5 storage fixes hold at the level they were aimed at:
- the scan bound no longer truncates;
- the HNSW and exact plans are pinned;
- hygiene uses `REINDEX CONCURRENTLY`;
- moves wait for indexes.

The six majors are in what the fixes assumed:
- the document-set rule for large filters is the slowest form PostgreSQL offers on the ordered path (P-1);
- filtered arms cost an order of magnitude more than budgeted (P-2);
- the 6.5 M hot set rests on 90 % B-tree fill and a hop that is not index-only (P-3);
- the readiness gate refuses valid indexes and puts graph inserts under the freeze (P-4, with C-6);
- the catalog standby as configured does not start, leaks cancelled commits, and blocks the shard failover path
  after its own promotion (P-5, with C-7);
- HNSW insertion WAL is about 10× the ingest budget (P-6).

Each has a decision-level fix inside the current architecture:
- `= ANY` on the HNSW path;
- a filtered-recall budget and SLO;
- the `UNION ALL` hop plus a re-derived target or the 192 GB fallback;
- an index-validity predicate and a final catch-up;
- two quoted synchronous standbys with non-cancellable commits;
- checkpoint spacing and a restated backup budget.

**Ready:** the recall layer can be staffed once P-1 and P-2 are decided, which they must be before M1.2.

**Not yet:**
- freezing the shard size (P-3);
- enabling moves (P-4, C-6);
- the catalog HA runbook (P-5);
- the backup and WAL budget (P-6).

## What I could not verify

- **Not installable here.**
  - **`pg_search`:** BM25 arm cost and WAL are not included.
  - **pgbouncer:** the pool and queue behaviour is Erlang-C arithmetic, not a load test.
- **This machine's settings.**
  - **`shared_buffers` = 128 MB.** HNSW insert latency (15 ms) and the hit/read split are cache-bound here. Buffer
    totals, plan shapes, WAL bytes, FPI counts and B-tree sizes do not depend on it.
  - **Checkpoint spacing.** The per-day WAL figures in P-6 are modelled from measured per-insert FPI behaviour, not
    observed over a production checkpoint interval.
- **Data.**
  - **Real embeddings and tags.** Topic correlation is synthetic (64 centroids). Off-topic HNSW arms found 150 rows
    within 20 k tuples, so the exact-fallback path's frequency in production is unknown. Its cost is the measured
    exact-path cost.
  - **Scale.** One 1 M namespace and a 200 k-fact graph, not a full 6.5 M shard. Hot-set totals scale the measured
    per-row sizes linearly.
- **Not tested end to end.**
  - **Catalog standby.** Only the "no standby connected" case of synchronous replication was executed. The automatic
    failover agent, `remote_apply` latency with a live standby, and hot-standby conflicts were not.
  - **Temporal.** Activity timeouts around `AwaitIndexes` and cutover retries were read from the plan, not executed.

---

## Review round 6: API contract, Go API, numbers, completeness, Hindsight parity

Lens: proto and §4 (evolution policy and its PR baseline, RPC → interface coverage, MCP tool set,
`tenant.admin` vs `engram.operator`), §2 (five-method rule, import graph vs depguard, `ReadSession` and the
arm semaphore, typed ids, patterns), the derived numbers (6.5 M / 154 shards / 5 cells, hot set, 220 ms
path, connection budget with the N159 subject locks, §6.8 cost, fill, §10 effort), completeness against the
brief, Hindsight parity, and consistency of sections, register, proto, SQL and §7/§8 test names. Findings
already dispositioned in rounds 1 to 5 are not repeated unless the fix is incomplete or wrong.

## How the evidence was produced

- **buf 1.57.0**, executed. `buf lint`, `buf build` and `buf format -d --exit-code` in `proto/` all exit 0.
  `buf breaking` was run twice from the repository root. Against the round-5 tree (`.git#ref=7288fa5`) it exits 100
  with exactly the two lines of the `expired_at → expires_at` rename. Against the PR baseline §4.5 prescribes
  (`.git#branch=main,subdir=plans/engram/proto`) it fails (A-7).
- **Five-method rule.** A script parsed every `type X interface { … }` in the Go fences of §1 to §12, including
  one-liners, `;`-separated bodies and embedded interfaces. That is 116 interfaces, plus 4 one-liners written
  with aligned spacing (`Chatter`, `Fuser`, `Packer`, `Planner`). The largest has 5 methods. **The rule holds.**
- **Import graph.** A script collected every `pkg.Ident` reference inside each `package` block of §2 and
  compared the edges with the depguard table and the "edges are exactly" list. It found no cycle. The table
  gaps are in A-8.
- **RPC coverage.** The 54 RPCs in `proto/` (36 `memory.v1`, 18 `memory.admin.v1`) were mapped by hand to
  `api.Deps` interface methods (A-3).
- **Embedded protos.** The §4 "full text" blocks of `common.proto` and `memory.proto` were diffed against the
  files. They are identical.
- **Test names.** A script collected 308 distinct `Test*` names cited across the sections and the register,
  expanding the `_Suffix` shorthand. Every real name is defined in §7 or §8; the residue was shorthand that
  points to TLA+ configs. A second script checked every `Spec_Config` citation against `formal/tla/*.cfg`:
  one dangling citation (A-11).
- **Numbers.** Recomputed in Python, including Erlang-C for the arm semaphore.

## Findings

### A-1: minor [fix incomplete: r5 A-1 / N155]: the recall pool share "≤ 32 of 40" is not enforceable, because the workers draw on the same pgbouncer pool, and the 305 connection-ms per recall omits the observation arms

**Where.** N155; D3 cell row; §9.1 "Connections" row and the pgbouncer excerpt (`DEFAULT_POOL_SIZE: "40",
RESERVE_POOL_SIZE: "0"`); §9 hosts ("`shard-N-pgbouncer` (api and worker)"); §2.2.7 `ReadSession`
(`TestReadSession_ConcurrentArms` "≤ 320 connection-ms per MID recall"); §3 table row 8 ("observation arms
(semantic + lexical …)"); `common.proto` `ArmRank.arm` (lists `observation_semantic` and `observation_lexical`);
§2.2.13 `Arm.Name()` (`semantic | lexical | graph | temporal | chunks`).

**Claim.** "`engram_app` pgbouncer pool **40** (recall ≤ 32, ack/commit and Reflect ≤ 8) … a MID recall holds
≈ 305 connection-ms (four 60 ms arms, two graph waves, visibility) … pool wait ≤ 4 ms p95."

**Evidence.**
- **The 32/8 split has no mechanism.** API and worker both connect as `engram_app` to the same
  `shard-N-pgbouncer`, and pgbouncer has one pool per (database, user). The arm semaphore (16 per API process,
  × 2 = 32) bounds recall from the API side. Nothing bounds the workers' share: no worker client pool size is
  stated anywhere, and `RESERVE_POOL_SIZE` is 0.
  - The worker-side database load (`BuildLinks` semantic kNN per fact, `ResolveEntities` fuzzy lookups,
    consolidation `FindCandidates`, `CommitChunk`) scales with chunk rate.
  - Online that is ≈ 0.09 chunks/s per shard, which is negligible.
  - At the `RetainBackfill` peak that §9 sizes Temporal for (580 to 5,800 events/s per cell, ≈ 1 to 9 chunks/s
    per shard), ≈ 10 kNN probes per chunk at arm-like cost is several connections. That is before consolidation's
    candidate searches. Whether it passes 8 is unmeasured, and nothing stops it if it does.
- **The connection-ms figure counts four arms.** A recall with `include_observations` and `include_chunks` set
  runs semantic, lexical, temporal, chunks, plus the two observation arms.
  - That is ≈ 6 × 60 + 2 × 30 + 5 ≈ 425 connection-ms if the observation arms are their own transactions, as §2.1
    says every concurrently running arm is.
  - This is the configuration the IOPS budget sizes ("3 vector arms", facts, chunks and observations).
  - At 50 QPS that is ≈ 21 Erlangs, i.e. 10.6 per API process on 16 semaphore slots.
  - Erlang-C with a 60 ms hold gives P(wait) ≈ 0.09, wait p95 ≈ 6 ms and p99 ≈ 24 ms per arm transaction. At least
    two transactions are in series on the critical path (semantic, then graph wave 2).
  - So the "≤ 4 ms" term and `TestReadSession_ConcurrentArms`'s 320 connection-ms bound do not hold for that
    configuration.
  - The p95 < 300 ms SLO still holds, with ≈ +10 ms.
  - Unverified: whether the observation searches are separate transactions. `Arm.Name()` omits them; §3 and
    `ArmRank` name them as arms.

**Recommendation.**
- Give the workers their own pgbouncer database alias (for example `engram_worker`, `pool_size = 8`) so that
  "recall ≤ 32" is a pool fact, not an intent.
- State the arm set the budget is sized for, in N155, §2.1 and `Arm.Name()`. Either count the observation arms
  (≈ 425 connection-ms, semaphore 20 per process, pgbouncer 48) or fold them into the semantic and lexical arm
  transactions and say so.

### A-2: minor [regression: N159]: the session-level subject-lock connections are missing from the connection budget and have no credential

**Where.** §2.2.7 `SubjectLocker` ("a small pool: 2 per API process per shard, so ≈ 57 of max_connections 100
with N155's ≈ 53"); N155 and D3 ("≈ 53"); §9.1 Connections row (lists admin, move, relay, migrate, runner,
exporter and `engramctl`: "≈ 53"); §9.1 secrets table (`engram_app` DSN is "(pgbouncer)" only).

**Evidence.**
- **Budget.** The lease runs on a **direct** connection, because session locks do not survive transaction
  pooling. That is 2 × 2 API processes = 4 more server connections per shard. §2 counts them (57); N155, D3 and
  the §9 budget table that operators provision from do not (53).
- **Credential.** Only `engram_move` and `engram_relay` have direct DSNs. The subject lock needs `engram_app`
  (or another role) straight to `shard-N-primary`, and no such secret or `pg_hba` entry exists.
- **Pool exhaustion.** The lease is held across the marker transaction. For `DeleteDocument` that includes the
  exclusive document lock (one 35 s attempt); for `Restore`, the 40 s derivation lock. So two slow deletes on one
  shard occupy both slots of a process. What a third Invalidate/Restore/Delete does is unspecified: block on the
  pool, or fail like the 3 s `lock_timeout`.

**Recommendation.**
- Put "subject leases 2 per API process, direct" into N155, D3 and §9 (≈ 57 of 100).
- Add the direct `engram_app` DSN, or a dedicated `engram_subject` role with only `pg_advisory_lock`, to the
  secrets table.
- Specify pool exhaustion as an immediate retryable `UNAVAILABLE`, like the lock refusal.

### A-3: minor [fix incomplete: r5 A-6]: `MoveService.CleanupMove` has no interface method behind it, so the generated (RPC → method) coverage check fails as specified

**Where.** §2.2.1 `Deps` and the coverage-table comment; §2.2.19 `move.Orchestrator { Start, Status, Abort }`
and `Closer.Cleanup`; §2.2.3 `catalog.Moves`; §2.2.7 `AdminTx`; `admin.proto` `CleanupMove`; §5.5 step 8
("the last activity calls the `MoveService.CleanupMove` admin RPC, which has the index runner drop the
namespace's partial indexes … runs `engram_cleanup_namespace` in batches"); §8 `TestDeps_EveryRPCHasPath`
("every public and admin RPC maps to an interface method").

**Evidence.**
- **CleanupMove.** Every other admin RPC maps to a method:

  | RPC | Method |
  |---|---|
  | `DrainShard` | `Registry.SetShardState` |
  | `ReleaseNamespace` | `Namespaces.SetState` |
  | `ResolveNamespace` | `Namespaces.Resolve` |
  | `RollbackMove` | `Orchestrator.Abort` |
  | `GetMove` / `ListMoves` | `MoveReader` |

  `CleanupMove` does not:
  - `Orchestrator` has no cleanup method.
  - `catalog.Moves.Advance` can only set `done`.
  - No `AdminTx` accessor drops a moved-out namespace's indexes or runs `engram_cleanup_namespace`. `Purger` covers
    the expunge targets only.
  - The only `Cleanup` is `move.Closer.Cleanup`, a worker activity, and that activity is the RPC's *caller*. The
    call path is circular: worker activity → admin RPC on `engram-api` → "the index runner", which lives in the
    worker.
- **Weaker gaps of the same kind.** `GetSnapshotManifest(version ≠ 0)` has only `Streamer.Latest` (or a filtered
  `SnapshotLister.List`). `GetPage(name)` has only `pages.Reader.Get(id.PageID)`.

**Recommendation.**
- Add `Cleanup(ctx, m id.MoveID, o CleanupOptions) (*CleanupResult, error)` to `move.Orchestrator` (four methods).
  Its implementation checks the N159 gate on the catalog row and returns `CLEANING` while batches remain.
- Have the activity drive the batches itself (it already holds both shard handles), so the RPC only records
  `done`.
- Add `ByName` and `Manifest(v)` selectors to the reader interfaces, or state the `List` mapping in the
  coverage table.

### A-4: minor: workers call `engram.operator` admin RPCs, but no worker credential exists, and the only scope that would work is fleet-wide

**Where.** D4 ("every other catalog write a workflow needs … goes through an admin RPC on `engram-api`"); N36;
§5.4 (`ReleaseNamespace`), §5.5 step 8 (`CleanupMove`), §9 ("Restart … through the admin RPC"); §4.1.9
(`ShardService`, `MoveService` are `engram.operator`, "fleet-wide, own audience, separate Envoy admin route");
§9.1 secrets table; NG14 ("Engram verifies, it does not mint").

**Evidence.**
- **Missing secret.** The secrets table lists no JWT or mTLS identity for `engram-worker` to reach the admin
  route.
- **Scope.** The scope model offers only `engram.operator`, which also covers `StartMove`, `UpdateTenant`
  quotas and isolation, and `RegisterShard` across every cell. A compromised worker, or a bug in it, would hold
  fleet-wide operator rights, where it needs `ReleaseNamespace` and `CleanupMove` for its own cell.

**Recommendation.**
- Add a service identity: a narrow scope `engram.worker`, valid on exactly `ReleaseNamespace` and `CleanupMove`,
  bound to the worker's cell by an mTLS SAN or a JWT `cell` claim checked against the shard's cell.
- Add its secret and rotation to §9.1.

### A-5: minor [fix incomplete: r5 A-17]: the generated MCP surface does not close: two RPCs are neither allowed nor omitted, one tool name collides, and the core client lacks two services

**Where.** §4.6 (the ten hand-tuned tools, the generated table, the `omit:` list, "the build fails for an RPC
that is neither allowed nor listed under `omit:`"); NG46; §2.2.25 `CoreClient { Memory, Document, Page,
Operation }`.

**Evidence.**
- **Unaccounted RPCs.** Counting the 36 `memory.v1` RPCs:
  - 10 are hand-tuned;
  - 20 are in the generated table;
  - 4 are omitted (`Create/Update/DeleteNamespace`, `StreamSnapshot`);
  - that leaves `OperationService.GetOperation` and `NamespaceService.ListNamespaces` in neither list. By the
    stated rule, the build fails.
- **Name collision.** The hand-tuned `get_operation` maps to **`WaitOperation`**. A generated tool for
  `GetOperation` would take the same snake-case name.
- **Missing clients.** `CoreClient` has no `Namespace()` or `Export()` client. The generated `get_namespace`,
  `get_effective_config`, `create_snapshot`, `get_snapshot_manifest` and `list_snapshots` therefore have no client
  to call through.

**Recommendation.**
- Put `GetOperation` (as `get_operation_status`, or fold it into `get_operation` and list it under `omit:` with
  that reason) and `ListNamespaces` (omit: a per-namespace endpoint has no use for it) into the lists.
- Add `Namespace()` and `Export()` to `CoreClient`. That makes six methods, so split it, for example
  `DataClients` and `AdminClients`.

### A-6: minor [fix incomplete: r5 A-12]: the per-path split of `UpdateTenant` cannot be expressed by the interceptor's `MethodPolicy`, and operators cannot read or delete a tenant

**Where.** §2.2.2 `MethodPolicy { Scope; Target; Bucket; AllowDeleting }`, `Policy map[string]MethodPolicy`
(keyed by method); §4.1.1, §4.1.9 (`UpdateTenant`: `display_name`/`config` under `tenant.admin`;
`quotas`/`isolation`/`state` under `engram.operator`; served on both listeners); D13 ("the interceptor is the only
enforcement point").

**Evidence.**
- **Path split.** One method maps to one `Scope`, so the path-dependent rule must be coded in the `UpdateTenant`
  handler. That makes a second enforcement point, which D13 rules out, and the §8 table test
  `(claims, method, catalog entry) → code` cannot see it.
- **Operator lifecycle.** `GetTenant`, `DeleteTenant` and `GetTenantOperation` are tenant-bound `tenant.admin`
  only. An operator token carries no tenant, so it fails `token.tenant_id == request.tenant_id`. The control plane
  then cannot delete a tenant (offboarding, a legal erasure request) without a tenant-bound token, and NG14 says
  Engram mints none.

**Recommendation.**
- Split the RPC: `UpdateTenant` (tenant paths) and `UpdateTenantLimits` (quotas, isolation, state;
  `engram.operator`). Each stays one-method/one-scope, and the change is additive (the reduced `UpdateTenant` is a
  pre-1.0 semantic tightening, logged in §4.5).
- Let `engram.operator` (Target Operator) call `GetTenant`, `DeleteTenant` and `GetTenantOperation` on the admin
  route.

### A-7: minor [fix wrong: r5 A-4]: the PR baseline that §4.5 and `proto/README.md` prescribe fails to run

**Where.** §4.5 ("a pull request is compared with the merge target …
`buf breaking plans/engram/proto --against '.git#branch=main,subdir=plans/engram/proto'`"); `proto/README.md`
CI commands.

**Evidence (executed, buf 1.57.0, repository root).**

```text
$ buf breaking plans/engram/proto --against '.git#branch=main,subdir=plans/engram/proto'
Failure: Module "path: "plans/engram/proto"" had no .proto files
$ echo $?
1
```

`main` has no `plans/engram/` at all; the plan lives on `claude/engram-implementation-plan`. Every PR that
touches the protos therefore fails the gate, and nothing on this branch was ever gated against `main`. The real
repository hits the same case on the PR that first introduces `proto/`.

Holding: against the round-5 tree (`ref=7288fa5`), `buf breaking` reports exactly the two `expires_at` lines, as
§4.5 states.

**Recommendation.**
- Add a bootstrap rule: when the baseline subdir does not exist on the merge target, run lint and build only, and
  record "no baseline" in the job summary.
- Or compare plan PRs against the plan branch's own merge base (`.git#ref=$(git merge-base …)`).

### A-8: minor: the depguard table still rejects edges the design needs: `pages → recall`, and the leaves for every non-leaf row

**Where.** §2.1 dependency table and its rows: services, `internal/api`, `internal/workflows`; §5.3 step 2
(`GatherEvidence`: "`recall.Planner` over `source_query`"); §2.2.16 `pages.Writer.Refresh`.

**Evidence.**
- **`pages → recall`.** A page refresh gathers its evidence with `recall.Planner`. The services row allows
  `recall` only for `reflectagent`, and `pages.Writer.Refresh` takes no evidence argument, so `pages` must import
  `recall`.
- **Leaves.** The services and `api` rows list `errs` but not `id`, `pipeline`, `fsm` or `txn`, yet every service
  signature uses `id.Scope`, and `recall` uses `pipeline.Step`. Only the infrastructure row says "leaves". A
  depguard config generated from the table as written fails M0.1's vet.
- No cycle results from either fix, because `recall` imports neither `pages` nor any service.

**Recommendation.** Add `pages: recall` to the services row (no cycle), and "leaves" to every row.

### A-9: minor: the delete subject drops its class, contrary to N150(2) and the typed-id rule; a client-chosen `document_id` can alias a fact's or namespace's subject

**Where.** N150(2) ("The subject is `(class ∈ {document, memory, namespace, tenant}, id)`"); §2.2.7
`SubjectLocker.Lock(ctx, ns id.NamespaceID, subject string)`; `shard_schema.sql`
`engram_subject_lock_key(ns uuid, subject text)` (`hashtextextended(ns || ':' || subject, 2)`),
`deletion_log.subject_id text` ("document_id | memory_id::text | namespace_id::text | tenant_id"),
`deletion_log_subject_idx (namespace_id, subject_id, deleted_at DESC)`; §2 preamble ("one typed id per entity
… never a bare `string`").

**Evidence.**
- **Collision.** `document_id` is client-chosen (≤ 256 bytes), so a client may name a document
  `"0192…"`, the text form of one of its own facts' `memory_id`. Then:
  - `DeleteDocument("0192…")` and `Invalidate(0192…)` share one advisory key;
  - `SubjectLease.Latest` returns the other class's last entry;
  - help-previous re-puts that entry's intent;
  - `prev_operation_id` chains across the two subjects, which N150 and the `Durability` spec model as
    independent chains.
- **Impact.** No cross-tenant effect (the key includes `ns`). Within a namespace it causes spurious
  serialisation, and the replay's per-subject grouping and epoch guard compare entries of different classes. That
  case is outside what TLC checked.

**Recommendation.**
- Make the subject a typed value `id.Subject{Class, ID}`, encoded `class || ':' || id`, in the lock key, in
  `deletion_log` (a `subject_class` column, or the encoded `subject_id`) and in the index.
- Add the aliasing case to `TestIntent_ConcurrentCurationOneChain`.

### A-10: nit: register drift

- **D17** still reads "Phase 0 … 13.5 ew; Phase 1 MVP 50 … Phase 2 … 21 … committed 84.5 … (rev. D24: figures
  regenerated from §10, N156)". §10 and N158 give 12.5 / 50.5 / 21.25 / **84.25**. N158 supersedes D17, but D17's
  own "rev. D24, regenerated from §10" label is false, and D17 is what the roadmap's first line cites.
- The **Status** paragraph says D24 is "N144 to N157"; N158 and N159 exist.
- **D13**'s `engram.operator` coverage ("`ShardService`, `MoveService`, `ListTenants`") omits `CreateTenant` and
  the operator paths of `UpdateTenant` (N157).
- **D14**'s proto list omits `proto/engram/internal/errors/v1`.

### A-11: nit: stale citations and small inconsistencies

- **Dangling TLC citation.** §5.5 step 6 cites TLC `ShardMove_D5Order` for the (c)-before-(d) order. That
  configuration was removed in round 3 (`1c25dbe`), and no `formal/tla/*.cfg` of that name exists.
- **Calendar text.** §10.3: "E2's Phase 2 work starts on day 127 … while E1 is still in M1.5". By the Gantt,
  E1's M1.5 runs from day 68 to day 126.
- **Sink.** §2.2.13 calls `Sink` (`Batch`, `Stats`) a one-method interface; it has two methods.
- **Documents per fact.** §3.7 assumes 1 M documents per 10 M facts. Table 6.8-B fixes 25 documents per 1 k facts
  (250 k per 10 M; 163 k per 6.5 M-fact shard). Small bytes, but two figures for one assumption.
- **Link indexes.** The §3.7 table gives link indexes of 41 GB per 10 M facts. That is 32.8 GB at 8 M and 26.7 GB at
  6.5 M, not the ≈ 36 and ≈ 29 GB the hot set uses. The error is on the safe side.
- **Expiry reason.** `SnapshotManifest` exposes `expired` and `expires_at` but not `expired_reason` (`ttl`,
  `document_delete`, `namespace_delete`). The comment says listed expired versions let "a client learn why its
  chain is broken".

## Checked and holding

- **buf.** `buf lint`, `buf build` and `buf format` are clean. `buf breaking` against the round-5 tree reports
  exactly the documented rename (the internal module included). The rename is in the §4.5 changelog, and
  `reserved "expired_at"` is present.
- **Five-method rule.** Holds for all 120 interfaces (largest = 5). `DocumentTags` embeds `TagLister` plus
  `Update` (2).
- **Import graph.** The signature-derived edges match the named infrastructure edges
  (`router → {store, index, blob, catalog, telemetry}`, `blob → store`, `quota → {txn, gateway}`,
  `authz → {catalog, quota, config}`, `catalog → {fsm, config}`, `index`/`outbox → store`), with no cycle. Services
  take `id.Scope` and `id.Caller` (A-8 of round 5 fixed), apart from A-8 above.
- **RPC coverage.** All 54 RPCs have a method except `CleanupMove` (A-3). That includes the round-5 A-6 list
  (`NamespaceAdmin`, `OperationReader`, `FactLister`, `DocumentReader.Version`/`Bodies`, `DocumentTags`,
  `pages.Admin`, `SnapshotLister`, `Orchestrator`).
- **Scope table.** §4.1.9 agrees with the MCP gates and the Connect route table. Operator-only methods are
  refused for tenant tokens by scope, not only by route.
- **SQL against proto.** The `operations.kind` and `state` `CHECK` lists agree with `OperationKind` and
  `OperationState` (`DELETE_TENANT` derived from the catalog, not stored). `cancel_reason` matches
  `CancelReason`. `superseded_by` is a version number in both.
- **Embedded protos.** The §4 copies of `common.proto` and `memory.proto` are identical to the files.
- **Sizing.**
  - 1 B / 6.5 M = 153.8 → 154 shards; / 32 = 4.8 → 5 cells.
  - Footprint 181 × 0.65 × 1.12 = 131.8 GB, and 181 × 1.12 = 202.7 GB.
  - Hot set 0.8125 × 90 = 73.1 GB, with 7 to 23 GB of headroom against 80 to 96 GB of cache.
  - IOPS 10 k × 50 × 5 % = 25 k, plus the filtered arms at 20 % ≈ 12.5 k, so under 50 k.
- **Critical path.** 2 + 25 + 60 + 30 + 1 + 90 + 3 + 5 = 216, + 4 = 220 ms. The rerank reserve is
  90 + 3 + 5 + 8 = 106; the skip threshold at a 300 ms deadline is 194 ms; the pre-rerank path is ≈ 122 ms.
- **Connections (four-arm recall).** 305 connection-ms → 15.25 mean, ≈ 24.4 p99 (Poisson). Erlang-C on two
  16-slot semaphores at 7.6 Erlangs each gives P(wait) ≈ 0.006, so the wait is ≈ 0 at p95.
- **Cost (§6.8).** All recomputed and consistent:
  - $0.157 per 1 k facts;
  - $157 k per 1 B ($124 k with the batch API);
  - $1,020 per 6.5 M-fact shard;
  - LME-S $0.236 per haystack, $118 per run ($93 with the batch API), 7.6 h at 600 RPM;
  - Reflect $0.114 typical and $0.34 worst case (cumulative billing);
  - `calls_per_chunk` 3.475 to 3.625.
- **Fill.** 100 M chunks / (5 × 600 / 210) = 79.8 days. At A-F = 4: 250 M / 23.8 ≈ 121 days. (Per-cell vs
  per-organisation RPM is as unverified as in rounds 3 to 5.)
- **Effort (§10).**
  - Milestone sums: Phase 0 12.5, Phase 1 50.5, Phase 2 21.25 → 84.25 committed; Phase 3 25.25; F/B 14; total 123.5.
  - Per engineer: E1 28.0, E2 28.25, E3 28.0.
  - Gantt chains: 198 / 199 / 197 days; MVP on day 163 (week 24); Phase 2 exit on day 199 (week 29); overrun 6.25.
- **Cross-references.** Every `N…` id cited in the sections, protos and SQL exists in the register. Every
  `Test*` name cited in §2, §5, §9 to §11 and the register is defined in §7 or §8. Every TLC configuration on
  disk is cited.
- **Completeness and parity.** Every deliverable of the brief has a home. The Hindsight features that are
  absent (memory defense, webhooks, import/clone, memory history, `retry_operation`, `clear_memories`, prompt
  preview, entity results, bank aliases) each have a non-goal row (NG7, NG10, NG12, NG36 to NG40, NG43).

## Counts

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 0 | — |
| minor | 9 | A-1 to A-9 |
| nit | 2 | A-10, A-11 |

## Verdict

**The API contract, the Go API and the derived numbers hold after round 5.** The protos lint, build and pass
`buf breaking` against the round-5 tree with only the documented rename. The five-method rule and the acyclic
import graph hold mechanically. Every sizing, latency, cost, fill and effort figure recomputes.

What remains is local:
- three fixes of round-5 findings that stop one step short:
  - `CleanupMove` coverage (A-3);
  - MCP list closure (A-5);
  - the `UpdateTenant` split (A-6);
- the PR baseline command that does not run (A-7);
- the connection budget, where the worker pool is unbounded and the subject leases are uncounted (A-1, A-2);
- small typing and depguard gaps (A-8, A-9).

None needs a new mechanism. The plan is ready for implementation once these rows are folded.

## What I could not verify

- **`buf breaking` in the real repository.** I could not run it under the real repository layout (`proto/` at
  the root, with a release tag as baseline).
- **Worker database time.** No measurement exists of worker-side database connection-time per chunk at the
  `RetainBackfill` peak (A-1).
- **Observation arms.** Whether the observation arms run as their own transactions; §2 and §3 disagree (A-1).
- **Gateway rate limit.** Whether the 600 RPM cap is per cell or per organisation (carried from rounds 3 to 5).
- **Hindsight live.** No side-by-side against a running Hindsight instance; parity was judged from
  `reference/hindsight-notes.md` only.
