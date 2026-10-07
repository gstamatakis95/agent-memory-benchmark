# Engram plan: third adversarial review (correctness and concurrency)

**Lens.** Correctness and concurrency only. The review checks whether the guarantees the plan
states for delete invisibility, `as_of`, moves, the outbox, consolidation and the fences hold
against the artifacts as written. Those artifacts are `sql/shard_schema.sql`, `proto/**`,
§3 and §5, plus the §9.3/§9.6 runbooks where §5 points to them. The register is read up to
N110. Findings already accepted in `REVIEW.md` (F-n) or `REVIEW-2.md` (G-n) are not repeated
unless their fix is wrong. `[regression]` marks a defect that a D20/D21 fix introduced or
exposed.

**Method.** Every finding below has an interleaving written against the real DDL. Where
**Executed** appears, the interleaving ran on PostgreSQL 16.
- **Shard schema:** applied to a scratch database (`rev3_*`) with only the pg_search
  `bm25` indexes stubbed out.
- **Concurrency:** reproduced with two `psycopg2` sessions, as roles `engram_app` and
  `engram_move` under RLS where it matters.
- **Synchronous-commit behaviour:** run on a private throw-away instance whose
  `synchronous_standby_names` named a standby that does not exist. That instance is stopped
  and deleted.
- **Scripts:** kept in the session scratchpad (`rev3/t_*.py`), not in the repo.

**Severity scale (same as REVIEW-2).**
- **blocker:** breaks a non-negotiable guarantee, or cannot run as written.
- **major:** a stated invariant does not hold without a decision-level change.
- **minor:** real but local.
- **nit:** small.

**Counts:** 5 blockers, 12 majors, 8 minors (25 findings).

---

## Findings

### H-1: blocker [regression]: the N79 lineage closure is not closed under a concurrent `ApplyBatch`, so a version derived from deleted content is written unflagged and served

**Where.**
- `engram_observation_inputs_after_delete` → `engram_flag_lineage` (`shard_schema.sql`).
- §5.4.1 step 7.
- §5.2.2 step 6.4 (candidate re-verification `FOR SHARE`).
- §5.2.5, row "A candidate version shown to the prompt is flagged…".

**Claim.** "The delete cascade's lineage closure (§5.4.1 step 7) flagged every already-written
descendant"; T1 invariant "any version reachable by lineage from a flagged version is flagged".

**Interleaving (Executed, `t_lineage_race.py`).** Setup: O1v1 has input `fv` (document D). O2v1
was written with O1v1's text shown, so it has the edge (O2,1)→(O1,1) and input `fe`.
1. `ApplyBatch` (A) for a new O3v1 shows candidate O2v1. Step 6.4 locks O2v1 `FOR SHARE`;
   O2v1 is still unflagged, so A proceeds. A inserts O3v1 and the edge (O3,1)→(O2,1), but has
   not committed yet.
2. The cascade (B) for D runs. It retires `fv` and deletes its `observation_inputs` rows. The
   trigger flags O1v1 directly. `engram_flag_lineage` then runs its walk `SELECT`, which does
   **not** see A's uncommitted edge. B's `UPDATE` of O2v1 blocks on A's share lock.
3. A commits. B resumes and flags O2v1, using the walk it already computed.
4. Result:

   ```
   O1v1 derived_from_deleted=t live=f
   O2v1 derived_from_deleted=t live=f
   O3v1 derived_from_deleted=f live=t   ← has a lineage edge to flagged O2v1
   ```

Nothing serialises the two transactions:
- Both take the namespace fence shared.
- `ApplyBatch` takes no document lock.
- The candidate row lock only orders A *before* the flag. It does not make the walk see A's child.

The window runs from the walk snapshot to the moment B locks O2v1. Any descendant that is not yet
locked is exposed, not only the first hop. This is G-1 again, so D16 fails in the same way.

**Recommendation (decision level).**
- Make the closure a fixpoint. After every flag `UPDATE`, re-walk from the rows it just
  flagged, with a fresh statement snapshot. Loop until a pass flags nothing. The row locks the
  cascade now holds guarantee that any apply which used a newly flagged version as candidate
  has committed and is visible to the next pass.
- Alternatively, serialise lineage writers against flaggers with a per-namespace
  "observation lineage" advisory key: `ApplyBatch` takes it shared, the cascade, purge and
  `Invalidate` take it exclusive for the flag phase only.
- Add the H-1 interleaving to `DocLifecycle.tla` (W-2). The current spec flags atomically.

### H-2: blocker: the depth-64 bound does not "fail closed" for `as_of`. Superseded descendants beyond depth 64 stay servable, and so does the frontier version itself.

**Where.** `engram_flag_lineage` and `engram_adjust_invalidation` (frontier handling); N79
"if the frontier is non-empty at depth 64 the cascade fails closed by setting `stale_delete` on
each observation still on the frontier (over-hiding is safe)".

**Why it is wrong.** `stale_delete` hides only the **current** version. The `live` predicate is
`NOT (stale_delete AND superseded_at IS NULL)`. Two kinds of version escape it:
- Superseded versions at depth ≥ 65.
- The depth-65 frontier version itself, which is never flagged.

Both are therefore served to `Recall(as_of)` in their `[effective_at, superseded_at)` ranges.
Every consolidation `update` adds one hop, because the edge (o, v)→(o, v−1) is mandatory. A
long-lived observation, such as one per entity or user preference, therefore passes 65 versions
in about 65 rounds; at the 5-minute debounce that is hours of steady ingest.

**Executed (`t_depth.py`).** One observation has 70 versions; v1's input is deleted.

```
counters lineage_flagged=64 lineage_frontier=1
v64 derived=t live=f | v65 derived=t live=f
v66 derived=f live=t | v67 … v69 derived=f live=t      ← superseded, servable under as_of
v70 current: stale_delete=t live=f
```

The `Invalidate` path has the same hole (`depth <= 64` only).

**Recommendation.**
- Remove the depth bound from the correctness path. Lineage is a DAG with at most one edge per
  shown candidate, so the closure is linear in the number of edges and terminates without a
  bound.
- If a bound is kept for latency, failing closed must set `derived_from_deleted` (or a
  per-observation "hide all versions" flag that `as_of` honours) on **every** version of every
  frontier observation and of all their descendants. A flag on the current version only is not
  failing closed.
- Restate the T1 invariant without the "or its observation is `stale_delete`" escape.

### H-3: blocker [regression]: N84 invalidation computes the lineage walk inside the `UPDATE`'s own snapshot. A concurrent apply leaks the invalidated content, and `Restore` is then permanently refused by the `CHECK`.

**Where.** `engram_adjust_invalidation` (`UPDATE observation_versions … FROM (SELECT DISTINCT …
engram_lineage_walk(…)) s`); §5.4.5 "decrements … **exactly the same set** (stable, because no
new version may have a hidden version as candidate)".

**Interleaving (Executed, `t_inval_race.py`).**
1. `ApplyBatch` (A) for O2v1 shows candidate O1v1, whose inputs contain `f`. O1v1's ≤ 5
   rendered quotes did not include `f`, so `f ∉ inputs(O2v1)` and A takes no lock on `f`. A
   locks O1v1 `FOR SHARE`, inserts O2v1 with the edge to O1v1, and does not commit yet.
2. `Invalidate(f)` (B) runs. Its trigger's `UPDATE … FROM walk` takes its snapshot. The walk
   cannot see the uncommitted edge. The `UPDATE` of O1v1 blocks on A.
3. A commits. B re-checks only the O1v1 row (EvalPlanQual) and increments it.
4. Result:

   ```
   O1v1 hidden=1 live=f
   O2v1 hidden=0 live=t       ← derived from O1v1's text, which saw f
   Restore(f): CheckViolation observation_versions_hidden_by_invalidation_check
   ```

`Restore` re-walks in a fresh snapshot, finds O2v1 and decrements it from 0 to −1. The `CHECK`
rejects this, so `Restore(f)` fails on every attempt from then on. The "same set" stability
argument assumed that apply re-verification orders every child after the increment. It orders
only the candidate row.

**Recommendation.**
- Use the same fixpoint as H-1, with walks in separate statements after the row locks are held.
- Make the counter set explicit: record the exact `(observation, version)` set incremented per
  `(fact, invalidation)` in a small table. `Restore` then decrements exactly the recorded rows,
  plus any rows added by later fixpoint passes. It must never recompute the walk.
- Make the decrement saturating-safe by asserting membership in the recorded set.

### H-4: blocker [regression]: N92's `remote_apply` does not return `UNAVAILABLE`. It blocks `COMMIT` indefinitely and invisibly, which breaks A-F1, the relay's 60 s gap horizon and every exclusive fence taker.

**Where.**
- N92.
- §5.4.1 "if no standby is available the delete returns `UNAVAILABLE` (retryable)".
- §9.3 "Acknowledged deletes survive failover".
- D6 A-F1: "a drawn `seq` is committed or aborted within one timeout of being drawn".
- `shard_schema.sql` role defaults (no `synchronous_commit` default).

**Executed (private PG 16 instance, `synchronous_standby_names = 'ANY 1 (nosuchstandby)'`).**
- The session sets `statement_timeout = 3s` and `SET LOCAL synchronous_commit = remote_apply`,
  then runs `INSERT; COMMIT`. `COMMIT` was still in `wait_event = SyncRep` after **2 min 27 s**.
  `statement_timeout` does not cover the sync-rep wait, because `finish_xact_command` disables
  it before commit.
- Another session saw **0 rows** throughout.
- Killing the client did not end the wait.
- A plain `INSERT` under the default `synchronous_commit = on` hung the same way.

Consequences:
1. **No `UNAVAILABLE`.** Postgres has no "no standby → error" path. The delete hangs until
   the API's own deadline. If the API then cancels, the backend logs "transaction has already
   committed locally" and the delete becomes visible later. The client was told it failed, and
   a failover can still undo it.
2. **A-F1 is false for delete-class transactions.** The `seq` drawn by `DocumentDeleted` and
   `FactInvalidated` stays invisible for as long as the standby lags (unbounded; also
   `remote_apply` waits on standby *replay*, which recovery conflicts can pause for
   `max_standby_streaming_delay`). After 60 s the relay declares the gap aborted and advances
   (§5.6). The event is then **never delivered** to:
   - the `deletion-log` consumer, which feeds the catalog `deletion_log` that restore replays;
   - the external index;
   - Kafka.
3. **Lock hold.** The waiting transaction keeps the namespace shared fence and the
   **exclusive document lock**. The document is `DocumentBusy` for every retain. Every
   exclusive fence taker (freeze, copy barrier, delete freeze) times out every 5 s, so a move
   cannot freeze and its watchdog rolls it back.
4. **Not delete-only.** With `synchronous_standby_names` set on every shard and no
   `synchronous_commit = local` role default, **every** commit (`CommitChunk`, acks) waits for
   the standby. That contradicts "RPO ≤ 60 s for retains" and the write latency budgets, and a
   standby outage stalls all writes on the shard.

**Recommendation.**
- Set `ALTER ROLE … SET synchronous_commit = local` for the app and worker roles, and keep
  `remote_apply` only for delete-class transactions.
- Before a delete-class `COMMIT`, check `pg_stat_replication` for a streaming sync standby
  whose apply lag is under a bound. Otherwise return `UNAVAILABLE` *before* drawing a `seq`.
- Run the delete-class `COMMIT` under a client-side deadline. On expiry, report the outcome
  as **unknown**, not failed, and rely on the idempotency key.
- Make the relay's gap rule specific to commit-wait: a gap is abandoned only when the gap
  `seq`'s writer is no longer running, which needs a writer registry, or the outbox row must
  be written by a transaction whose commit cannot wait. Alternatively move the delete-class
  outbox write to an `AFTER COMMIT` sweeper keyed on `deletion_log`.
- Restate A-F1 with the commit-wait term and re-run `Outbox.tla` with an unbounded-commit
  writer. It will fail.

### H-5: blocker [regression]: rollback "after (b), before (c)" (N98) cannot execute against the ownership state machine

**Where.** N98; §5.5.1 step 7 and step 10 ("a failure before (c), including a target outage
after (b), rolls back by `ReleaseNamespace(target, e + 1)`"); `ownership_transitions`;
`engram_cleanup_namespace`.

**Executed (`t_own.py`).** On the target after `activate_target` (b):

```
DELETE active row as engram_admin   → 42501 "may not be deleted by engram_admin"
DELETE active row as engram_move    → 42501
UPDATE active → incoming            → 23514 "illegal ownership transition"
engram_cleanup_namespace(ns)        → 55006 "refusing to clean up … in ownership state active"
```

There is no edge out of `active` except `freeze_*` and `epoch_bump`, and `active` rows "are
never deleted" by design (N93). The only rollback the plan defines after (b) is therefore
unexecutable. The watchdog fires, `Rollback` fails, and the source stays `frozen` until a
human intervenes. Writes are unavailable for the whole namespace. The model `ShardMove.tla`
(W-12) cannot have caught this, because it does not run against the DDL.

**Recommendation.**
- Add an explicit `unactivate_target` edge: `active → incoming`, role `engram_move`, allowed
  only while `move_id` is still set. That requires `activate_target` to keep `move_id` until
  the post-(c) settle step (see H-6).
- Alternatively make (b) a new state, `ready` (`incoming → ready → active`), that no writer,
  reader or scheduler accepts, and promote it to `active` only after (c).
- Either way, `TestIso_Ownership_Transitions` must drive the real `Rollback` activity after
  (b).

### H-6: major [regression]: from (b) to (c) the target is `active`, so the target's shard schedulers run the namespace before the point of no return

**Where.** Views `schedulable_namespaces` and `purgeable_namespaces` (`state = 'active'`;
`activate_target` clears `move_id`/`move_epoch`); N97 "acts only on `active`"; N98 "nothing
routes to the target before (c)".

**Executed.** After (b), the target namespace appears in both `schedulable_namespaces` and
`purgeable_namespaces`.

**Interleaving.**
1. (b) commits.
2. (c) is delayed, for example because the source is slow or briefly unreachable. The
   watchdog allows up to 15 min.
3. In that window the target's DEFERRED resumer, op-sweeper (copied `PENDING` rows with
   `workflow_started_at IS NULL`), purge-sweep and consolidate-sweep start workflows at
   e + 1 on `shard-{target}`. Their fenced writes pass: the target is `active` at e + 1.

So the target accepts writes while the source is still the rollback-able owner. With H-5
fixed, a rollback discards those writes. The terminated source workflows are restarted at the
source (step 10). Their target twins have the same workflow ids, in one Temporal cluster.
§5.5.4 does not mention them, and `Restart`'s reconcile loop runs only on the forward path.
"Nothing routes" covers the API, not the schedulers.

**Recommendation.** Schedulers must require `state = 'active' AND move_id IS NULL`, and (b)
must leave `move_id` set until a post-(c) "settle" edge clears it. Or use the `ready` state of
H-5.

### H-7: major [regression, partial]: N81 replay is upsert-only and still not total. Row deletions and cascade side effects are not replayed, so moves deterministically fail `Verify` or silently diverge.

**Where.** §5.5.1 step 3 table; `events.proto` `replay:` annotations (the generator source,
N81); §5.4.1 steps 7–8; §5.2.2 step 6.5; `RowsPurged`.

**Uncovered writes:**
1. **Delete cascade.**
   - It deletes `observation_sources` and `observation_inputs` rows (step 7) and
     `page_sources` rows (step 8).
   - Its triggers set `observations.retired_at`, `observations.stale_delete`,
     `observation_versions.retired_at` and `observation_versions.stale_delete`. Step 8 sets
     `pages.stale_delete`.
   - The cascade emits only `DocumentDeleted`, `VersionsFlagged`, `SnapshotsExpired`,
     `BlobTombstoned`, `OperationTransitioned` and `IdempotencyKeyStored` (step 11). The
     annotation for `DocumentDeleted` is "documents, document_versions, facts, chunks (retired_at),
     operations", which covers neither observations nor pages. The ids travel in
     `DocumentDeleted.observations_retired` and `pages_marked_stale`, but nothing maps them to rows.
2. **`ApplyBatch` update.** It deletes the shown-and-dropped `observation_sources` rows (N79).
   The `delete` op deletes all of the observation's sources. `ObservationUpserted`/`ObservationRetired`
   replay *upserts* the rows that exist at the source, so a row that is now absent is never
   deleted on the target.
3. **`VersionsFlagged{LINEAGE_FRONTIER}` and `ObservationsMarkedStale`.** The annotations say
   "observations" or "flag columns", while §5.5 says `observation_versions`. The
   `observation_versions.stale_delete` mirror is not replayed, so a frontier-hidden current
   version (H-2) is `live` on the target.
4. **`RowsPurged`.** Its replay is
   `DELETE FROM <table> WHERE namespace_id AND document_id = $d AND key ∈ [min,max]`.
   - `fact_links`, `entity_mentions`, `observation_sources`, `observation_inputs` and
     `observation_version_sources` have **no `document_id` column**, so the statement cannot be
     generated.
   - Under N91's replica mode the FK `ON DELETE CASCADE`s do not fire either.
   - For `facts`, a `ctid`-ordered purge batch's key range spans live facts of the same
     document: kept chunks of a `REPLACE` keep their v1 ids, which interleave with the retired
     ones. The blind range delete removes **live** rows on the target.

**Consequence.** On the target:
- the row counts of `observation_sources`, `observation_inputs` and `page_sources` exceed the
  source's. The N90 `Verify` (a) and (b) count checks fail, giving one extra round and then
  `Rollback`. **Any namespace with a delete or a consolidation `update` during catch-up can
  never be moved.**
- flag-only drift such as `pages.stale_delete` or the version `stale_delete` mirror is caught
  only if the 4,096-row content sample hits it. Otherwise it passes cutover. A page that cites
  deleted content then never refreshes on the target.

N88's "skew between ranges is repaired by the replay (N81 is total)" is false for the same
reason: a range copied before a source-side delete keeps the deleted rows.

**Recommendation.** Give replay **set semantics per parent key**. For each
`(event, parent key)` (an observation, an observation version, a page, a document's purge set),
the target first deletes the child rows of that parent that the source no longer has, then
upserts. In practice this is an anti-join against the source rows read in the same replay
transaction. Make the cascade and trigger side effects explicit rows in the mapping
(observations, observation versions and pages from `DocumentDeleted`). Replace `RowsPurged`
replay by the same "delete what the source no longer has" rule over the document's rows of
each table, joining through `facts` for tables without `document_id`. Add a T3 test: delete,
consolidation update and purge during catch-up, then `Verify` must pass.

### H-8: major: blobs written after the copy listing are never caught up. Acknowledged large retains, version bodies, page markdown and export files are lost after cleanup.

**Where.** §5.5.1 step 2 (blob prefix copy once, "skipping keys whose target etag/size already
match"), step 6(a) (blob listing diff pre-freeze only), step 6(b) (no blob check); the N81
mapping has no blob entries; §5.1.1 step 3 (`ledger/{sha}` written to the *resolved* shard's
prefix before the ack); N104 `ver/{sha}`.

**Interleaving.**
1. After `Verify (a)`, during the last catch-up round, a client retains a 200 KiB item. The
   API puts `{src}/…/ledger/{h}` and acks at the source (`active`).
2. `DocumentVersionStarted` replays the `ingest_ledger` row to the target. The blob is not
   copied.
3. `Drain` terminates `RetainDocument`. `Restart` runs it on the target.
4. `LoadItem` reads `{dst}/…/ledger/{h}`, which is missing. The activity fails `P-db`, and the
   operation ends `FAILED`.
5. After 24 h, `CleanupMove` deletes the source prefix. The acknowledged input is gone.

The same applies to:
- `ver/` bodies: every later `APPEND` breaks;
- `pages/{id}/v{n}.md`: `page_versions` rows point to nothing;
- `export/v{n}/`: `StreamSnapshot` fails;
- `consolidate/{batch_key}.json`.

**Recommendation.**
- Events that create durable blobs carry their keys, and replay copies the key before
  upserting the row (blob first, idempotent).
- Alternatively, `Verify (b)` performs an exact listing diff of the durable classes (`ledger/`,
  `ver/`, `pages/`, `export/`) under the freeze, and cutover is refused on any difference.
- `CleanupMove` must refuse while any target row references a key that is missing on the target.

### H-9: major: namespace and tenant delete acknowledge before any shard-side fence, so D16 depends on catalog cache invalidation. The restored-shard read fence admits pre-replay data.

**Where.**
- §5.4.3: the catalog tx and `ExecuteWorkflow`, then the ack; `FenceForDelete` is step 1
  *inside* the workflow.
- §3.3: the read fence accepts `frozen/restore`.
- §5.0: "Read activities … accept `active` and `frozen`". This contradicts §3.3, which rejects
  `frozen/delete`.
- §9.3: the restore steps.

**Interleavings.**
1. A client calls `DeleteNamespace` through API instance 1, which acks after the catalog
   `UPDATE` plus `NOTIFY`. The very next `Recall`, load-balanced per request to instance 2,
   arrives before instance 2 has processed `NOTIFY`. `NOTIFY` delivery is asynchronous, and
   the stated catalog-outage mode is "every cached namespace keeps working". The shard row is
   still `active`, because `FenceForDelete` has not run, so the deleted namespace is served,
   and a retain into it is acknowledged. "From then on nothing … is returned" (D16) is violated.
2. Restore. After PITR, the shard's ownership rows are `active`. §9.3 never sets
   `frozen/restore` on the shard, and even `frozen/restore` passes the read fence. Before
   step 4, the deletion replay, any request routed by a cache that did not see
   `state = 'restoring'` reads the **resurrected** documents. The read fence ignores the
   epoch, so the epoch bump does not help.

**Recommendation.**
- `DeleteNamespace` and `DeleteTenant` run `FenceForDelete` (shard tx, `frozen/delete`)
  synchronously before the ack, as the document delete already does.
- The read fence rejects `frozen/delete` and `frozen/restore`, so only `frozen/move` is readable.
- The restored database comes up with `listen_addresses` restricted, and before reopening it
  sets every row to `frozen/restore`.
- Fix the §5.0 text.

### H-10: major: an export whose `WriteFiles` snapshot predates a delete becomes `ready` after the delete and serves the deleted document

**Where.** §5.4.1 step 10 (expires `state = 'ready'` snapshots only); §5.7 steps 2–4
(`WriteFiles` is an up-to-2 h `REPEATABLE READ` snapshot; `RecordSnapshot` later inserts
`state = 'ready'` with no check); §5.4.8 ack text "nothing from the document is visible, in
any export either".

**Interleaving.**
1. `WriteFiles` takes its snapshot S, which contains document D.
2. The `DeleteDocument(D)` cascade commits and acks. No `export_snapshots` row exists yet,
   because it is inserted in step 4, so nothing is expired.
3. `RecordSnapshot` inserts version n as `ready`.
4. `StreamSnapshot(n)` serves D's facts and chunks after the ack.

The `building` value of the state enum exists but is never written.

**Recommendation.**
- `BeginSnapshot` inserts the row as `building`, and the cascade expires `building` and `ready`
  alike.
- `RecordSnapshot` refuses to promote an expired row, and it scans
  `DocumentDeleted`/`NamespacePurged` events with `seq > to_seq OR seq = ANY(open_gaps)`
  (anti-join) and expires the snapshot when any intersect.

### H-11: major [regression]: the N82 export cut cannot identify in-flight `seq`s under RLS, so "a late committer is never missed" is false

**Where.** N82; §5.7 step 2 ("every `seq < to_seq` that is *not* visible … is recorded in
`open_gaps[]`"); §5.5.1 step 3, which states the counter-argument: "under RLS the
namespace-confined role sees only this namespace's rows, so a hole … is indistinguishable from
another namespace's seq".

**Why it is wrong.** The export runs as `engram_app` under `ns_isolation`. Every other
namespace's `seq` in `(from_seq, to_seq)` is invisible to it in the same way an in-flight writer's
`seq` is. This leaves two options:
- `open_gaps` lists every other namespace's `seq`. That is up to the shard's whole event
  volume since the base, on the order of a million entries per day, in a manifest and in an
  `= ANY(...)` predicate.
- `open_gaps` lists nothing, and a late committer below `to_seq` is silently missing from
  every later delta.

`pg_current_snapshot()` records xids, not `seq`s, and D6 rejected xmin tricks. The local copy
then misses acknowledged facts until the next `FULL`.

**Recommendation.** Use the move's mechanism. The export is a cursor-style consumer with its
own anti-join ledger (`export_applied(namespace_id, seq)`), or it waits, as the copy barrier
does, for one try-lock-free instant: take the namespace exclusive key for milliseconds, which
N82 forbade because of the pool. A third option is to read the gap set from the relay's
shard-level watchlist through a `SECURITY DEFINER` function that returns only the gap `seq`s
(no payloads).

### H-12: major [regression]: N73's `search_pages` puts deleted content back into Reflect. `page_versions` have neither per-version deletion flags nor lineage, which is F-1 for pages.

**Where.** N73; §4 `PageService.SearchPages` "BM25 ∪ HNSW over `page_versions`" and the
Reflect tool; §5.3.1 "`stale_delete` … the page may say something it must not"; D16 (Reflect
is in the list); D9 "Pages use the same rule"; the `page_versions` DDL (no
`derived_from_deleted`, no `hidden_by_invalidation`).

**Interleaving.**
1. Page P v3 was written from facts of document D. The cascade sets `pages.stale_delete` and
   deletes `page_sources`.
2. Until the refresh lands:
   - Reflect's `search_pages` ranks P v3 and quotes its snippet;
   - `GetPage` returns it with a flag;
   - an export's `pages/{id}.md` contains it.
3. After the refresh, P v3 is superseded but still selected by `GetPage(as_of)` and
   `SearchPages(as_of)` in its range, because nothing marks it as derived from deleted content.
4. Pages derived from observation versions flagged by H-1/H-2 lineage are never flagged at all.

**Recommendation.** Apply the observation machinery to pages:
- `page_version_inputs`, the facts and observation versions rendered;
- per-version `derived_from_deleted` and `hidden_by_invalidation`, flagged by the cascade, by
  the lineage closure through observation versions, and by `Invalidate`;
- `live` excludes `stale_delete` on the current version;
- `SearchPages`, `GetPage` and the export filter on `live`.

Until then, `search_pages` must not be a Reflect tool.

### H-13: major: restore and moves are not reconciled. A restore lands a namespace in two `active` owners, or loses a moved-in namespace, and §9.3 cannot bump the epoch it prescribes.

**Where.** §9.3 steps 1–8; §5.5 "Epoch bump on restore-from-backup"; N64/N101 edges; N92.

**Interleavings.**
1. **Source shard S lost within 60 s after cutover (c), restored to the latest WAL.** The WAL
   with (c) is gone (RPO 60 s), so the restored S has the namespace `frozen/move` at e, or
   `active` at e with `move_epoch` set if the freeze was lost too. The catalog already says T at
   e + 1. Step 3 "the move rolled back" now *thaws* a namespace that T owns at e + 1. The
   restore then bumps S to e + 1, which is the same epoch as T, and step 6 restarts the restored
   `RUNNING` operations "with the new epoch and the same `operation_id`s". On S they pass the
   fence (`active`, e + 1), and their workflow ids are the ones running on T (one Temporal
   cluster, `TERMINATE_IF_RUNNING`). Retains commit on S, which no reader is routed to, so
   acknowledged work disappears from view. Two shards are writable at the same epoch.
2. **`--target-time` before a move-in.** The restored T lacks the namespace. Its data exists
   only on the old source, and only until the 24 h `CleanupMove`. Backups are kept 28 days, so
   most of the PITR window loses the namespace entirely. The restore check "ownership rows =
   catalog" fails with no defined repair.
3. **`--target-time` before a move-out.** The restored S has the moved-out namespace `active`
   at the old epoch. The catalog no longer lists it on S, so steps 1, 4 and 5 skip it.
   `schedulable_namespaces` picks it up (op-sweeper, consolidate-sweep, purge). The read fence
   ignores the epoch, so stale routes read pre-deletion data.
4. **Executed (`t_own.py`).** §9.3 step 5 ("`namespace_ownership.epoch = e + 1` (`state` still
   `frozen`)") is refused: `23514 illegal ownership transition frozen(restore)/1 →
   frozen(restore)/2`. §5.5 says the bump is the `active → active` edge, the DDL has
   `restore_done` (`frozen/restore → active`, +1), and §9.3 never sets `frozen/restore`. Three
   texts give three procedures.

**Recommendation.**
- **Shards are the truth for ownership, but only together.** A restore must reconcile against
  `namespace_moves` and the peer shards' rows. It may reactivate a namespace only if no other
  shard holds `active` or `incoming` for it at an epoch ≥ the restored one; otherwise it
  converts the restored row to `moved_out` with the peer's hint.
- `CleanupMove`'s grace must be at least the backup PITR window, or a move must force a fresh
  base backup of the target before cleanup.
- Write one restore procedure, generated from the transition table (N103), and execute it in
  `TestIso_Ownership_Transitions`.

### H-14: major: a failover during an open move cannot bump epochs, and the mover's and relay's direct sessions keep talking to the old primary

**Where.** N63 ("a promotion bumps the epoch of every namespace on the shard"), N92, §9.6 "A
shard down"; the CHECK `move_epoch IS NULL OR (… move_epoch > epoch)`; `ownership_transitions`
(no `frozen → frozen` edge).

**Executed (`t_own.py`).**
- `epoch_bump` on an `active` row with an open move: `23514 … namespace_ownership_check5`
  (`move_epoch > epoch` would become equal).
- `epoch_bump` on `frozen/move`: `23514 illegal ownership transition`.
- A shard-wide `UPDATE … SET epoch = epoch + 1` therefore aborts entirely. A per-row bump
  leaves exactly the namespaces under move un-fenced.

**Interleaving.**
1. The old primary is not verifiably stopped (the condition under which N63 bumps). Promotion
   flips the VIP, but **existing TCP sessions** to the old primary survive. These include:
   - the mover's direct source session (N15, the session-level barrier lock);
   - the relay's direct session.
2. The mover keeps reading the outbox from, and freezing, a zombie, while the new primary keeps
   accepting writes at e, because the bump failed for this namespace.
3. Cutover (c) commits on the zombie, and (d) flips the catalog.
4. The writes acknowledged on the new primary between promotion and (c) are never replayed,
   and the new primary still holds `active` at e.

**Recommendation.**
- Failover first aborts every open move on the shard. That needs a defined edge, for example
  an admin `abort_move` from `frozen/move`, and it calls `ReleaseNamespace` on the targets.
- The bump edge clears `move_id`/`move_epoch` atomically.
- The mover verifies `pg_control_system()`/`system_identifier` and the timeline on every
  activity, and the relay on every batch.
- Kill direct sessions at promotion (`pg_terminate_backend` on the old primary is not
  possible), so they must check `pg_is_in_recovery()` plus a shard generation counter in
  `shard_meta` that the promotion increments.

### H-15: major [regression]: `fact_consolidation` failed stamps break at both ends. The 5.2.2 insert violates the `CHECK`, and a retried fact can never be marked done, so it is reconsolidated every round.

**Where.** N95, N110(b); §5.2.2 step 3.3 (`INSERT fact_consolidation(…, batch_key, …, note =
'failed') ON CONFLICT DO NOTHING`); step 1 ("retry facts whose only stamp is a `failed` note
older than 7 days"); step 6 (`INSERT … ON CONFLICT DO NOTHING`); DDL
`CHECK ((note = 'done') = (batch_key IS NOT NULL))`, `PRIMARY KEY (namespace_id, memory_id)`;
`engram_pending_facts`.

**Executed (`t_fc.py`).**

```
5.2.2 StampFailed with batch_key           → 23514 fact_consolidation_check
pending after an 8-day-old failed stamp     → engram_pending_facts returns 0
ApplyBatch done-stamp after the retry       → 0 rows inserted (PK conflict); stamps = ['failed']
```

**Consequence.**
- As written, `StampFailed` fails on every attempt. The bisect leaf cannot terminate, and its
  retries burn `P-db`.
- With the `CHECK` respected, the "only stamps are `failed`" wording assumes several stamps per
  fact, but the primary key allows one.
- After a successful retry, the `done` row is swallowed by `ON CONFLICT DO NOTHING`. The fact
  still qualifies as "only failed, older than 7 days", so it is re-selected and re-applied in
  every round. That is a new batch key each time, so `consolidation_applied` does not dedup:
  N43's exactly-once holds per batch, but each fact is consolidated an unbounded number of
  times.
- The watermark also passes failed-stamped facts, because they are not pending per
  `engram_pending_facts`.

**Recommendation.** Make the stamp history append-only with the attempt in the key:
`PRIMARY KEY (namespace_id, memory_id, stamped_at)`. Define "consolidated" as
`EXISTS note = 'done'`, and "retryable" as "latest stamp is `failed` and older than 7 days".
Teach `engram_pending_facts` and the watermark both rules, and keep the watermark below the
smallest retryable fact. Add a T3 test for fail → 7 days → succeed → never selected again.

### H-16: major [regression]: the retain ack cannot create the version row (`body_key`/`body_hash NOT NULL`), and APPEND bases are read from a blob that may not exist yet

**Where.** §5.1.1 step 4.4 (`INSERT document_versions(…)` without a body); N104 ("stored by
`LoadItem` **before** the version row is created"); N110(d)/PD-29 ("sets `body_key`/`body_hash` on
the already-created version row `WHERE body_key IS NULL`"); DDL `body_hash bytea NOT NULL`,
`body_key text NOT NULL`, `CHECK (body_key = 'ver/' || …)`; §5.1.2 step 1 uses
`append_base_version`, which **does not exist** in the DDL.

**Executed (`t_ack.py`).** The ack's insert fails with `23502 null value in column "body_hash"`,
so every retain fails.

**Ordering hole (N56 + N104).** Appends A (v1) and B (v2) are acknowledged 1 s apart.
1. B's `LoadItem` may run before A's (independent workflows on a shared queue). It assigns base
   v1 and must read `ver/{body(v1)}`.
2. A's `LoadItem` has not written that blob yet. A's body hash is not even known, because the
   row has no body until A's `LoadItem` runs.
3. The plan specifies no wait, so B either fails or falls back.

A fallback to "the highest version with a body" re-creates the F-10 lost append.

**Recommendation.**
- Make `body_key`/`body_hash` nullable, with `CHECK (body_key IS NULL OR …)`, and add
  `append_base_version`.
- `LoadItem(v)` for APPEND must wait for, or itself materialise, `body(base)` recursively from
  the ledger chain up to the nearest version that has a body.
- Alternatively, the ack stores a *delta* pointer and `LoadItem` composes the body under the
  `documents` row lock in version order.
- Add a T3 test: two appends whose `LoadItem`s run in reverse order.

### H-17: major (Async index mode): `DocumentDeleted{ids_elided}` cannot be honoured by the external index, because the victims are purged before a lagging consumer reads them "by `document_id`"

**Where.** N80 ("the rows live until `PurgeDocument` plus grace, so this needs no blob
write"); §5.4.1 step 6 (`purge_after = now()`) and `PurgeDocument` grace 0 for deletes; N4
("Events carry every field a consumer needs … the relay never reads another table").

**Interleaving.**
1. A 100 k-fact transcript is deleted. The event goes out with `ids_elided`.
2. The `index` consumer lags (an engine outage, which §5.6 alerts on at 15 min).
3. `PurgeDocument` runs immediately and removes the rows.
4. The consumer reads `facts WHERE document_id = $d` and gets nothing, so the external engine
   keeps every document of the transcript's text.

N44's read-time join hides the hits from Recall, but the brief's cascade "search index"
requirement and the erasure SLA fail for exactly the largest, most compliance-relevant deletes.

**Recommendation.** For `ids_elided`, the index consumer deletes by query on an indexed
`document_id` (plus `namespace_id`) field. Alternatively, the purge of a document waits until
every registered consumer cursor has passed the delete's `seq`.

### H-18: minor: exclusive takers' `lock_timeout = 5 s` is shorter than a legal writer transaction, so starvation-freedom holds only while every writer finishes in under 5 s

**Where.** N82 "a continuous stream of writers cannot starve it"; §3.3 lock table; the cascade
SLO (≈ 5 s per 100 k facts, ≤ 30 s), H-4 commit waits.

**Executed (`t_trylock.py`).**
- C's try-lock is correctly refused while B's exclusive request is queued, which **confirms
  `TestFence_TryLockRefusedBehindWaiter`**.
- After B's 5 s timeout, a new writer D gets the shared lock while A still holds it.
- Overlapping writers that each run longer than 5 s, such as large cascades or a lagging sync
  standby, therefore reset the queue on every retry, and `Freeze` and the barrier can be starved
  until the watchdog rolls the move back.

**Recommendation.** Exclusive takers wait `statement_timeout + 5 s` in one attempt. Writers
are refused rather than blocked, so the long wait costs no pooled connection. Alternatively,
the taker first sets a "freeze pending" bit that the fence reads.

### H-19: minor: the document lock key collides with the namespace fence key when `hashtext(document_id) = 0`

**Where.** `engram_ns_lock_keys` → `(hashtext(ns), 0)`; `engram_doc_lock_keys` → `(hashtext(ns),
hashtext(doc))`; §3.3 "a hash collision … only over-serialises … never under-fences".

**Why it is wrong.** `document_id` is client-chosen, so a tenant can find a string with
`hashtext = 0` offline (2³² trials). Then:
- `FinalizeVersion`, the cascade and `PurgeDocument` for that document take the **namespace
  fence** exclusively. Every writer of the namespace is refused for up to 5 s per attempt.
- Two such finalizes deadlock: each holds the shared fence and requests the exclusive one on
  the same key.

This is a self-DoS, but it also means the over-serialisation argument does not cover
cross-kind collisions.

**Recommendation.** Separate the key spaces. Use the one-argument `bigint` form for the
namespace fence (a different lock tag), or reserve `k2 = 0` with `hashtext(doc) | 1`.

### H-20: minor: admin-transaction schedulers write `operations` and the outbox without the shared fence, which breaks the `p0` exactness argument and permits writes after the final pass

**Where.** §5.0 ("every *write* activity opens `WithNamespaceTx(write)`"; admin tx "used only by
the namespace purge and the per-shard sweepers"); N81 (`OperationTransitioned` on every
`operations` transition); §5.5 step 2 ("every `seq ≤ p0` is committed or aborted at snapshot
time").

**Interleaving.**
1. The DEFERRED resumer, or the op-sweeper setting `workflow_started_at`, reads
   `schedulable_namespaces` (`active`).
2. It updates `operations` and inserts `OperationTransitioned` with no namespace fence.
3. If it draws `seq ≤ p0` and commits after the barrier snapshot, the row is in neither the
   copy nor the replay.
4. If its read preceded `Freeze` and its commit follows the final anti-join pass, the event is
   lost and the source row diverges.

The impact is small (`Restart` re-reads operations), but the proof's premise, "every outbox
writer holds the shared fence", is false.

**Recommendation.** Per-namespace writes by sweepers run through `WithNamespaceTx(write)`
(fence prelude, `active` at the row's epoch), and `engramlint sql` checks that every outbox
insert is preceded by the fence prelude.

### H-21: minor: `engram_move` cannot delete its own trim-pinning cursor, and it can rewrite every consumer's cursor

**Where.** Grants `SELECT, INSERT, UPDATE ON outbox_cursors TO engram_move` (no `DELETE`);
§5.6 "the mover's cursor row is removed at cleanup or rollback"; §5.5 step 10 ("delete the move
cursor"); `outbox_cursors` has no `namespace_id`, so no RLS applies.

**Executed.** `has_table_privilege('engram_move','outbox_cursors','DELETE') = false`.

**Consequences.**
- A rolled-back move leaves `move:{ns}` at `p0` forever. `outbox-trim` deletes only
  `seq <= min(last_seq)`, so **the whole shard's outbox stops trimming**.
- Conversely, `engram_move` may `UPDATE` the `relay`, `index`, `kafka` and `deletion-log` rows,
  so one mover bug can skip deletion events for every namespace. N50's "`engram_move` stays
  namespace-confined" is false for this table.

**Recommendation.** Use a `SECURITY DEFINER` pair, `engram_move_cursor_open(ns, p0)` and
`engram_move_cursor_close(ns)`, restricted to `consumer = 'move:' || ns`, and revoke direct DML.

### H-22: minor: rolling back a return move destroys the permanent `moved_out` fence value

**Where.** N93 ("a `moved_out` row is a permanent fence value"); §5.5 step 10 ("its ownership
row (or reset a `moved_out` one)"); `ownership_transitions`.

**Executed.** `incoming → moved_out` is refused with `23514`. The only exit from `incoming` is
`rollback_target` (DELETE), so after rolling back a move back to a former shard, that shard
has no row at all. Stale routes then get "no row" instead of the target hint. That is safe, but
it is not what N93 promises, and the "reset" step is unexecutable.

**Recommendation.** Add a `return_abort` edge, `incoming → moved_out` (`engram_move`), which
restores `target_shard_id`/`target_epoch` from `namespace_moves`.

### H-23: minor: the consolidation watermark skips facts that become live again below it

**Where.** N95/N110(c) (the watermark advances "to just below the smallest pending **live**
fact id"); §5.1.2 step 5.4 (`CommitChunk` un-retires facts with their original ids); §5.4.5
`Restore`.

**Interleaving.**
1. A fact f is committed.
2. A quick `REPLACE` retires it before any round runs.
3. The sweep advances the watermark past f, because f is not live and therefore not pending.
4. The document flaps back, and `CommitChunk` un-retires f.
5. f is live, has no `fact_consolidation` row, and sits below the watermark. It is never
   consolidated.

A fact invalidated before its first round and restored later goes the same way.

**Recommendation.** The watermark must stay below the smallest *unconsolidated* fact, live or
retired-in-grace. Alternatively, un-retire and `Restore` must insert a "re-pending" marker
that `SelectRound` unions in.

### H-24: minor: candidate quotes may include `REPLACE`-retired sources in grace, so apply re-verification discards and re-queues in a loop that pays for an LLM call each time

**Where.** §6 (the 5 most recent quotes by `mentioned_at`, with no `live` filter); N42
(retired facts keep their `observation_sources` during the 1 h grace); §5.2.2 step 6.4
(`input_fact_ids … AND live FOR SHARE`, fewer rows → discard and re-queue at the front);
§5.4.5 (only `invalidated_at` is filtered for candidate sources).

**Interleaving.**
1. A `REPLACE` retires one of O's five newest sources, which marks O `stale_write`.
2. The next round's `FindCandidates` renders it as a quote.
3. `ApplyBatch` finds an input that is not live, discards the proposal, and re-queues the
   batch at the front of its group.
4. `FindCandidates` returns the same quote, and the loop continues until the purge, at least
   1 h later.

The loop has no attempt bound.

**Recommendation.** Render only `live` sources as quotes, everywhere, and bound re-queues per
batch, for example 3, before stamping `failed`.

### H-25: minor (partially verified): Kafka and the external index see a namespace's events from two shards with no ordering across cutover

**Where.** D6 (topic per shard, key `namespace_id`); §5.5 (the target outbox is not written
during copy; post-cutover events originate on T).

**Interleaving.**
1. The S relay lags and still holds S's tail, for example a late `ChunkCommitted` of document D.
2. After cutover, a client deletes D on T. T's relay emits `DocumentDeleted(D)` first.
3. A consumer of both topics applies the delete, then the late upsert, which re-indexes deleted
   content.
4. The index consumer's "read the current row by id" goes to S, which is `moved_out`, and the
   outcome there is unspecified.

**Recommendation.** Cutover (c) waits until every S consumer cursor has passed the
namespace's last source `seq` (relay drain). Alternatively, events carry `(epoch, seq)` and
consumers drop an event whose epoch is older than one they have already applied for the
namespace.

---

## Checked and holding

- **N82 lock-manager premise.** A shared try-lock is refused behind a queued exclusive request
  and granted to an existing holder. Executed (`t_trylock.py`). The starvation caveat is H-18.
- **Delete cascade versus `ApplyBatch` when the victim is in the batch's own inputs.** The
  victim's row lock serialises them, and the trigger's volatile-function statements take fresh
  snapshots, so the later `DELETE … observation_inputs` sees the committed apply. Correct in
  READ COMMITTED. It becomes wrong only through lineage (H-1).
- **N43.** The proposal is write-once (`UPDATE` trigger). The discard is gated by the
  `RESTRICT` FK from `consolidation_applied`, and keys and effects commit in one transaction.
  Correct per batch; the per-fact repetition is H-15.
- **N84 idempotence of a repeated `Invalidate`.** The `WHEN` clause fires only on NULL ↔ set.
  Correct.
- **N93/N101 forward path.** `start_move → freeze_move → cutover_c → return_move` is accepted
  by the trigger with the stated epoch, move and target rules. Executed. Only the rollback and
  failover edges are missing (H-5, H-14, H-22).
- **(c) before (d) and the `moved_out` hint (N98).** Sound for the API path. The schedulers'
  path is H-6.

## Verdict

**Not ready to staff the move, delete or consolidation code.** The two review rounds fixed
the earlier mechanisms. Four of the new ones fail on the real DDL:
- **Lineage flagging (H-1, H-2).** The cascade misses versions written concurrently with it,
  and the depth bound leaves superseded versions servable under `as_of`.
- **Invalidation counters (H-3).** The same race applies, and afterwards `Restore` is
  permanently refused by the `CHECK`.
- **Synchronous delete durability (H-4).** `remote_apply` blocks `COMMIT` indefinitely instead
  of returning `UNAVAILABLE`, and that silently undermines the outbox gap horizon.
- **Rollback after cutover step (b) (H-5).** The state machine cannot execute it.

None of these is visible to the TLA⁺ specs as they stand, because they model flagging and
ownership transitions as atomic. Each one needs a decision, not an edit:
1. Fixpoint lineage closure or a lineage lock, and no depth bound in the correctness path.
2. A recorded invalidation set.
3. A precise synchronous-commit policy with role defaults and a pre-commit standby check.
4. An `unactivate` or `ready` state for the target.
5. Set-semantics replay.
6. A restore procedure that reconciles against peer shards.

The schema-level contradictions (H-15, H-16) would fail the first retain and the first
consolidation failure in M1 tests. They are cheap to fix but show that §5 and the DDL were not
executed together after D21.

Recommended gate before Phase 1 move or consolidation code:
- every interleaving in H-1, H-3, H-5, H-7 and H-13 as a T3 two-session test against the real
  DDL;
- `DocLifecycle.tla` gains non-atomic flagging (walk and update as separate steps, with a
  concurrent apply);
- `ShardMove.tla` gains failover during a move and rollback after (b).

## What I could not verify

- **TLC.** I did not run it. I read no `.tla` beyond what the register states, so whether W-7
  to W-12 already model non-atomic flagging is unverified (the register says they are open).
- **pg_search behaviour.** The `bm25` indexes were stubbed out, so N76 is untouched.
- **Synchronous replication with a real standby.** I could not test lag, `remote_apply` under
  recovery conflicts, or the API driver's handling of the "committed locally" warning. H-4 rests
  on the no-standby execution plus documented `SyncRepWaitForLSN` behaviour.
- **Temporal semantics.** `TERMINATE_IF_RUNNING` across task queues and memo-based
  `AlreadyStarted` checks (H-6, H-13) are reasoned from the documentation, not executed.
- **The catalog side.** `namespace_moves` and catalog `deletion_log` were not exercised; H-9
  and H-13 assume the catalog behaves as §3.2 states.
- **A brute-force `hashtext(doc) = 0` string (H-19).** Not computed. The argument depends only
  on `document_id` being client-chosen and the key layout.
