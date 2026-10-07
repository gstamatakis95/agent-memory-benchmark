# Review round 3

Three parallel reviews (correctness, Postgres/operations, API/numbers/parity) and the architect's redesign advice that answered them.

---

## Engram plan: third adversarial review (correctness and concurrency)

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

---

## Engram: adversarial review 4 (Postgres, Temporal and operational reality)

Scope: whether the DDL, indexes, triggers, RLS policies, roles, GUCs and functions in
`sql/shard_schema.sql` and `sql/catalog_schema.sql` behave and perform as §3, §5 and §9 claim
at 10 M facts per shard and 32 shards per cell. This review also covers Temporal, migrations,
backups, connection budgets, metrics and the Compose topology. Findings accepted in `REVIEW.md` or
`REVIEW-2.md` are not repeated unless the applied fix is wrong. Where a finding overlaps a
concurrent review (`REVIEW-3.md` H-*, `REVIEW-5.md` A-*), that is stated, and this review adds
only its measurements.

## How the evidence was produced

- **Server.** A scratch PostgreSQL 16.15 with pgvector 0.8.6, pg_trgm, btree_gin and btree_gist on a 4-vCPU, 15 GB VM.
  - Settings: `shared_buffers = 2GB`, `maintenance_work_mem = 2GB`, `max_parallel_maintenance_workers = 3`.
  - Every timing below is **warm**: the data fits in `shared_buffers`. Cold-cache figures were not measured, so the I/O terms are computed.
- **Schema.** Databases `rev4_shard` and `rev4_cat` received the two DDL files unmodified. The only exception is the `-- pg_search:begin/end` blocks, which were stripped because pg_search is not installable here.
  - Both files apply cleanly.
  - All self-checks pass.
- **Synthetic shard.**
  - Eight namespaces, all hashing into `facts_p07` (chosen with `satisfies_hash_partition`), of 100 k, 40 k, 20 k, 15 k, 10 k, 6 k, 5 k and 4 k facts. That is 200 k facts and 20 k chunks.
  - The rows are inserted interleaved across namespaces, as concurrent ingest would leave them.
  - Embeddings are `halfvec(768)`, clustered: 40 topic centroids per namespace plus noise, L2-normalised.
  - HNSW uses `m = 16, ef_construction = 128` as in the DDL.
  - Measured sizes: the `facts_p07` heap is 521 MB (≈ 2.6 KB per row, matching §3.7's 2.4 KB) and its HNSW is 391 MB (≈ 1.95 KB per vector).
- **Caveats.**
  - Clustered synthetic vectors make the *namespace* filter almost free for an in-namespace query vector, because each namespace occupies its own region. Real tenants share topic space. Filter costs are therefore measured with the tag and `as_of` filters, which do reject neighbours.
  - The production partition is ≈ 3× larger (625 k rows), so HNSW insert, search and vacuum costs below are lower bounds at target scale.

Raw scripts are reproducible from the statements quoted in each finding.

---

## Findings (ranked)

### P-1: blocker [regression: N94 + N41/N84 `live`]: every mutation of a vectored row is a full HNSW insertion. The synchronous delete cascade is ≈ 50× over its SLO, and documents above ≈ 12 k facts cannot be deleted inside the 30 s `statement_timeout`

**Where.**
- §3.8 "Delete cascade … SLO … p95 ≤ 50 ms per 1 k retired facts … a 100 k-fact document ≈ 5 s … inside the 30 s writer `statement_timeout`".
- §9.4, SLO table (same figure).
- §3.7 bloat plan: "a retire … not HOT, because both columns are in partial-index predicates, so each retire adds index entries to **the two partial indexes**".
- §3.7 vacuum table: `observations`/… "fillfactor = 80 … HOT updates stay on the page"; facts "fillfactor = 90".
- §5.1.6 "`CommitChunk` ≈ 15 ms".
- §9.5 move copy ≈ 1,000 facts/s per stream (A-O17′).

**Claim.** "≈ 6 index probes and ≈ 3 row updates per fact and no 300 M-row table, so the SLO is stated per 1 k retired facts: p95 ≤ 50 ms."

**Evidence.**

Retire of 2,000 facts: 20 documents of the 100 k namespace, using the cascade's own `UPDATE facts SET retired_at = coalesce(retired_at, now()), purge_after = now() WHERE namespace_id = $1 AND document_id = ANY(…)`.

| Variant | Time | Per fact | WAL |
|---|---|---|---|
| as specified (HNSW present) | **4,872 ms** | **2.44 ms** | 3.8 MB |
| same statement, `facts_embedding_hnsw` dropped inside the transaction | 92 ms | 0.046 ms | 3.4 MB |
| chunk retire, 200 chunks (`chunks` HNSW) | 248 ms | 1.24 ms | |
| `INSERT` of 5,000 facts (HNSW present / dropped) | 9,260 / 257 ms | 1.85 / 0.05 ms | 13 KB / 3 KB per fact |

The non-HOT update creates a new heap tuple. Every non-partial index gets an entry for it, **including the shared HNSW and BM25 indexes**. The partial `WHERE live` indexes do *not* get one, because the new version is not live, so §3.7 has the mechanism backwards.

HOT is unreachable even for columns that are in no index. `UPDATE facts SET invalidation_reason = 'x'` on 1,000 rows gave `n_tup_hot_upd` +400 / `n_tup_upd` +1,000 and took 1.49 s. The reason is page space: a 2–2.4 KB inline row (N94 `STORAGE MAIN`) at `fillfactor = 90` leaves 819 B free per 8 KB page, which is less than one row version. Every update of a fact row therefore re-inserts its vector into HNSW whatever column it touches:
- `invalidation_reason`;
- tag re-sync;
- un-retire in `CommitChunk`;
- the cascade's `coalesce(retired_at, now())` rewrite of rows already retired in grace.

**Consequences.**
- **Cascade cost.** ≈ 2.4 s per 1 k facts plus ≈ 0.12 s per 1 k facts for chunks (one chunk per 10 facts), against the 50 ms SLO.
  - A 12 k-fact document reaches the 30 s `statement_timeout` on the facts `UPDATE` alone.
  - The 100 k-fact transcript the SLO cites needs ≈ 250 s. It can never be deleted. It times out and rolls back on every retry, while holding the exclusive per-document lock and the shared fence (P-12).
  - These figures come from a 200 k-element graph. At 625 k elements HNSW insertion is slower still (log-depth search plus more cache misses).
- **Other costs on the same path.**
  - `FinalizeVersion` of a `REPLACE`, the cascade's chunk retire, and every `Invalidate` and `Restore` pay the same per-row HNSW cost.
  - Each retire leaves a dead HNSW entry that P-4's vacuum must repair.
- **`CommitChunk` ≈ 15 ms** holds only for small chunks. Ten facts plus one chunk are ≈ 20 ms of HNSW insertion alone, before BM25, ≈ 300 `fact_links` rows, GiST and GIN.
- **Move copy rate.** It is bounded by ≈ 1.85 ms per fact per stream (≈ 540 facts/s with HNSW only). That is below A-O17′'s assumed 1,000 facts/s before BM25 and B-trees.

**Recommendation (decision level).** Take mutable visibility state off the rows that carry vectors.
- **Option (a), preferred.** Split each vectored table into an **insert-only** vector table `(namespace_id, id, embedding)` and a narrow mutable row.
  - The HNSW (and the per-namespace partial) index lives on the vector table and is touched only at insert and at purge.
  - The arms already fetch the base row per candidate to test `live`/`as_of`/tags, so the join adds no heap fetches. P-6 shows that fetch is the dominant cost anyway.
- **Option (b), less disruptive.**
  - Remove `live`, `retired_at` and `purge_after` from the BM25 field lists and from every index predicate. Use one partial index `WHERE retired_at IS NOT NULL` for the purge sweep and test liveness as a heap filter.
  - Make HNSW partial `WHERE live` (pgvector supports partial HNSW), so a retire inserts nothing.
  - Set `fillfactor ≤ 60` on vectored partitions so a second 2.4 KB version fits on the page.
- **Delete cascade, either way.**
  - Make the synchronous visibility cut O(1) per document: one `deleted_documents (namespace_id, document_id, deleted_at)` row that every arm anti-joins (a per-namespace set bounded by the purge grace; small, fits an `InitPlan` array).
  - Move row retirement into `PurgeDocument`, batched.
  - Restate the SLO only after `TestDelete_CascadeScales` runs at 625 k rows per partition.

### P-2: blocker [regression: N79]: the depth-64 frontier does not fail closed for `as_of`. Superseded descendants beyond depth 64 stay servable. Reproduced; also REVIEW-3 H-2

**Where.**
- `engram_flag_lineage`, `engram_adjust_invalidation`.
- Comment: "fails closed on the frontier (over-hiding is safe)".
- §3.3.6 visibility predicate `… AND NOT (stale_delete AND superseded_at IS NULL)`.

**Evidence.** One observation with 70 versions chained `(o, v) → (o, v−1)`, version 1 having input fact F. The test retires F and deletes its `observation_inputs` row, as the cascade does.
- `engram.lineage_flagged = 64`, `lineage_frontier = 1`.
- Versions 1–65 are `derived_from_deleted`.
- The observation is `stale_delete`, so version 70, the current one, is hidden.
- **Versions 66–69 remain `live = true`.** `as_of = day 68.5` returns `obs v68`, written transitively from F.

The frontier flag hides only the *current* version, because the version mirror of `stale_delete` is ignored once `superseded_at` is set. An observation updated more than 65 times is ordinary for a long-lived preference or belief. Each further version extends the leak.

**Recommendation.**
- Lineage within one observation is a chain. Close it in O(1) with `UPDATE observation_versions SET derived_from_deleted = true WHERE (namespace_id, observation_id) = … AND version >= v_min` per affected observation.
- Walk only *cross-observation* edges.
- On a frontier hit, flag **every version of the frontier observation from the frontier version up**, not only the observation row.
- Add the 70-version chain as a T1 regression test with an `as_of` probe.

### P-3: major [regression: N79/N84 × N94]: lineage flagging percolates through the candidate graph. One `Invalidate` takes 5.9 s and one fact delete hides two thirds of a namespace's observations, nearly all of the time going into HNSW re-insertion

**Where.**
- `engram_adjust_invalidation` (the walk executed three times), `engram_flag_lineage` (twice).
- §3.7 lineage sizing "≤ 11 candidate versions".
- §9.4 SLO "99 % of document deletes flag ≤ 100 observation versions".
- `consolidate.max_rebuilds_per_round` = 50.

**Evidence.** Namespace with 2,000 observations × 10 versions (20 k versions). Each version has an edge to its predecessor and to 10 random earlier candidate versions (≈ 190 k lineage rows), which is the §3.7 shape.

| Operation on one fact that version (o, 1) of one early observation used | Result |
|---|---|
| `engram_lineage_walk` from that version | 9,886 (version, depth) rows, 5,749 distinct versions, max depth 18, 73 ms |
| `UPDATE facts SET invalidated_at = now()` (trigger N84) | **5,891 ms**; 5,749 versions `hidden_by_invalidation > 0`; 1,324 observations `stale_write` |
| same with `observation_versions_embedding_hnsw` dropped in the transaction | 421 ms (HNSW = 93 % of the cost) |
| delete path (retire F, delete its inputs row) | **7,033 ms**; `lineage_flagged = 5748`; **1,324 of 2,000 observations `stale_delete`** (hidden) |

**Why.**
- A version shown as a candidate taints every later version that saw it, transitively. With about 10 candidates per prompt, the descendant set of an early version is a large fraction of the namespace.
- `live` is a stored generated column over `derived_from_deleted`, `hidden_by_invalidation`, `stale_delete` and `superseded_at`, so every flag flip is a non-HOT update. Each one re-inserts a 768-d vector into the partition HNSW (and into BM25, which also indexes `superseded_at` and `live`).
- The ≤ 100-versions-per-delete SLO cannot hold.
- At 50 rebuilds per round, 1,324 hidden observations need 27 consolidation rounds of LLM spend. Until then they are invisible.
- A `Restore` costs the same as the `Invalidate`.

**Recommendation.**
- Keep flags in a side table `observation_version_flags (ns, obs, ver, derived_from_deleted, hidden_count)`, not on the vectored row. The arms then filter on it through an anti-join on a small set, and flag flips stop being HNSW inserts (P-1 (a)).
- Bound taint semantically rather than by depth. Record lineage only for candidate versions whose **text was cited or quoted in the output**, not for every candidate shown. Alternatively, rebuild descendants from live sources eagerly in a bounded background job, and hide only the directly flagged versions synchronously.
- Compute the walk once per call into a temp array. It is currently recomputed two or three times.

### P-4: major: pgvector HNSW vacuum ("repair graph") is the shard's throughput ceiling for retire, purge, move cleanup and namespace delete. The bloat plan's "20 k rows/s per shard" is off by about three orders of magnitude

**Where.**
- §3.7: "purge throughput is deliberately capped at 20 k rows/s per shard to keep autovacuum ahead"; "`REINDEX INDEX CONCURRENTLY` … when dead fraction > 30 %"; "a whole-namespace purge of a 1 M-fact namespace takes ≈ 10 min and one vacuum cycle".
- Per-partition `autovacuum_vacuum_scale_factor = 0.02`, `threshold = 10000`, `cost_delay = 2`, `cost_limit = 1000`.
- `autovacuum_max_workers` is not set anywhere (default 3).

**Evidence.** `VACUUM (VERBOSE)` of `facts_p07`, ≈ 190–200 k rows, warm:

| Dead index tuples (share of partition) | Vacuum elapsed | Note |
|---|---|---|
| 1,000 (0.5 %), `INDEX_CLEANUP ON` | 51 s | parallel index worker |
| 2,000 (1 %) | 79 s | |
| 20,000 (≈ 10 %): 10 k facts retired, then purged | **231 s, 228 s user CPU** | B-trees took milliseconds; the HNSW took the rest |
| 1,000 (0.5 %, < 2 % of heap pages) | 0.09 s | PG 14+ index-vacuum **bypass**, so the HNSW was not touched |

**Why.**
- pgvector's `ambulkdelete` must find a new neighbour list for every element that pointed to a deleted one.
- With `m = 16` (32 layer-0 links), a dead fraction f affects ≈ 1 − (1 − f)³² of the graph: 15 % at f = 0.5 %, 69 % at 3.6 %.
- At the autovacuum trigger (2 % of 625 k plus 10 k, i.e. 22.5 k dead tuples, which is ≈ 11 k purged facts because retire and purge each leave one) a vacuum repairs ≈ 430 k elements. At the measured ≈ 1.2 ms per repaired element that is ≈ 9 min of CPU, before autovacuum's cost-delay throttling.
- A single autovacuum worker therefore clears ≈ 11 k facts per ≈ 10 min per vector index. The `chunks` and `observation_versions` HNSW indexes queue behind it.
- With 3 workers and 48 vectored partitions (+ 32 non-vectored), sustained retire-plus-purge capacity is in the tens of facts per second per shard, not 20 k/s.
- The 30 % `REINDEX` trigger never fires, because autovacuum repairs at 2–4 %. Yet rebuilding is cheaper than repairing:
  - a full build of this 200 k partition took **67 s** with 4 parallel workers;
  - repair at 10 % took 231 s.
- Namespace delete, tenant delete and move cleanup produce dead fractions of 10–100 % of a partition.

**Recommendation.**
- **Prefer rebuild over repair for mass deletes.** For namespace delete, tenant delete, move cleanup and purges above ~2 % of a partition:
  1. set `vacuum_index_cleanup = off` on the partition for the duration;
  2. `REINDEX INDEX CONCURRENTLY` the partition's HNSW;
  3. run one `VACUUM (INDEX_CLEANUP ON)`, which finds no dead elements in the fresh graph and is cheap.
- Avoid retire-time dead entries altogether (P-1 (a) or (b)).
- Set `autovacuum_max_workers` (≥ 6) and per-partition `autovacuum_vacuum_cost_limit` for vectored partitions explicitly.
- Size "purge throughput" from a measured repair rate at 625 k rows.
- Add an alert on autovacuum runtime per partition (P-14).

### P-5: major: under RLS the planner may not use a non-leakproof operator as an index qual. The entity-resolution trigram lookup degrades from an index scan to a scan of the shard's entities, and the "verified plans" were not run as `engram_app`

**Where.**
- §3.8 #12 "entity resolution … `entities_trgm_idx` (GIN, uuid + trigram)"; the tags GIN `facts_tags_gin` and `observations_tags_gin`.
- §3.8 BM25 arm ("the whole WHERE runs inside the index scan").
- N19 query registry with `EXPLAIN` assertions.
- `ns_isolation` is `FORCE`d and `engram_app` is `NOBYPASSRLS`.

**Evidence.** The §3.8 query `SELECT … similarity(canonical_norm, $2) … WHERE namespace_id = $1 AND merged_into IS NULL AND canonical_norm % $2 ORDER BY s DESC LIMIT 5`, with 85 k entities on the shard (50 k in the probed namespace):

| Run as | Plan | Time |
|---|---|---|
| superuser | Bitmap Index Scan on `entities_trgm_idx`, `Index Cond: namespace_id = … AND canonical_norm % …` | **6.7 ms**, 275 buffers |
| `engram_app` (`SET ROLE`, `engram.namespace_id` set) | **Seq Scan on entities**, `Filter: … canonical_norm % …`, 84,999 rows removed | **308 ms**, 2,576 buffers |

`pg_proc.proleakproof` is false for `similarity_op`, `arrayoverlap` and `arraycontains`. The planner (`restriction_is_securely_promotable`) refuses such a qual as an index qual when an RLS qual sits at a lower security level. At §3.7's 0.5 M entities per shard, a CommitChunk resolving ~10 entities therefore costs seconds, or at best a scan of the namespace's entire entity set per lookup.

The same rule keeps `tags && $q` and `tags @> $q` off the tags GIN indexes. pg_search's `|||` and `@@@` go through a custom scan whose RLS behaviour could not be checked here (pg_search unavailable). If its operator functions are not leakproof and the custom scan honours security levels, the BM25 arms lose Top-K pushdown entirely.

**Recommendation.**
- Audit and mark `LEAKPROOF` (superuser migration) the operator functions the arms rely on, *or* route these lookups through narrow `SECURITY DEFINER` SQL functions that take `namespace_id` from `current_setting` and re-check it.
- Make every N19 `EXPLAIN` assertion run **as `engram_app` with the scope GUCs set**, never as the owner or superuser.
- Add an M0 gate: the BM25 arm's plan under RLS on the real ParadeDB image.

### P-6: major: both semantic-arm plans exceed the per-arm budget at the band edges. The shared HNSW at 2–3 % selectivity costs 67–111 ms and 26–27 k buffers; the exact path at 20 k costs 41 ms and 8,000 heap pages; the heap is not in the hot set

**Where.**
- §3.3.4 band rule (exact below 20 k; shared HNSW at ≥ 2 % of the partition; the partial band "EMPTY at the 10 M target").
- §3.8 #2: the exact path "cost to be MEASURED cold in M0.6".
- §3.7 hot set (40 GB, no facts heap). REVIEW-5 A-3 raises the hot-set omission; this finding adds measurements.

**Evidence.** Run as `engram_app` under RLS, warm. Plans were forced with `enable_sort = off` (HNSW) or `enable_indexscan = off` (exact). `hnsw.ef_search = 150`, `iterative_scan = relaxed_order`, `max_scan_tuples = 20000`.

| Case | Plan | Rows removed by filter | Buffers | Time |
|---|---|---|---|---|
| 6 k namespace (3 % of partition) + tag filter `ANY` | HNSW | 3,588 | 26,056 | **87–111 ms** |
| 4 k namespace (2 %) + `as_of` cutting about half | HNSW | 3,959 | 27,257 | **67 ms** |
| same namespaces, no extra filter (clustered data: neighbours are in-namespace) | HNSW | 0 | ≈ 3,000 | 6.5 ms |
| 20 k namespace, exact path (bitmap on `(namespace_id, tags)` GIN + top-N sort) | exact | — | 8,007 (= 8,000 heap pages, **64 MB**) | **41 ms** |
| 6 k namespace, planner's own choice | `facts_mentioned_idx` + sort | — | 2,418 | 10 ms |

**What the measurements show.**
- With real data a tenant's neighbours are mostly *other* tenants' facts in the same partition. Cost then scales with 1/selectivity: ≈ 4.7 k visited tuples at 3.2 % (a 20 k namespace in a 625 k partition), each costing a heap fetch for the RLS, `namespace_id`, `live`, `as_of` and tag filter.
- That is ≈ 100 ms per arm warm. There are three such arms (facts, chunks, observations) against a 60 ms arm stage.
- The exact path is 2–10× cheaper than HNSW up to tens of thousands of rows here. The 20 k switch point is therefore on the wrong side.
- The exact path reads 8,000 pages for 20 k interleaved rows. Cold, that is 64 MB of random reads per arm call.
- Both paths read the facts heap (25 GB at target), which §3.7's hot set omits.

**Recommendation.**
- Set the exact-versus-HNSW switch from a measured crossover at 625 k rows with *shared-topic* vectors, not at 20 k.
- For the exact path, keep the vector side table of P-1 (a) **clustered by namespace** (`CLUSTER`, or a BRIN-friendly key order), which turns 8,000 random pages into ≈ 1,000 sequential ones.
- Budget the heap of every vectored table into the hot set, or state the cold p95.

### P-7: major [regression: N92]: `remote_apply` ties delete latency to standby *replay* of HNSW-heavy WAL; `synchronous_commit = on` already gives RPO 0; the topology has no shard standbys. Complements REVIEW-3 H-4

**Where.** N92, §9.3 "Acknowledged deletes survive failover", §9.1 Compose (no standby services, no `synchronous_standby_names`), R32.

REVIEW-3 H-4 shows that COMMIT blocks instead of returning `UNAVAILABLE` and that every commit becomes synchronous without a `local` role default. This review adds three points.

1. **`remote_apply` is the wrong level for the stated goal.** RPO 0 across promotion needs the commit record *flushed* on the standby: `synchronous_commit = on`, i.e. remote flush. `remote_apply` additionally waits until the standby has *replayed* everything up to the commit LSN, including other transactions' WAL.
   - Measured WAL is ≈ 13 KB per inserted fact with HNSW and ≈ 1.9 KB per retired fact.
   - Partition HNSW repair vacuums (P-4) and `REINDEX CONCURRENTLY` of a ≈ 1.1 GB partition index produce GBs of WAL, which recovery replays single-threaded.
   - Delete latency then equals the standby's replay lag, which during backfill or index maintenance is seconds to minutes. Nothing reads from the standby, so `remote_apply` buys nothing.
2. **The topology does not contain the standbys N92 requires.** §9.1 lists one `shard-N-postgres` per shard, and its `command` has no `synchronous_standby_names`. A synchronous standby per shard doubles the cell's Postgres footprint: 32 more 64 GB / 8-CPU instances and NVMe volumes per cell. It must sit on a *different host* to be worth anything, which settles Q5/A-O1 (multi-host cells) by necessity. Neither cost appears in §9 or §10.
3. **No standby observability.** No metric or alert reports `pg_stat_replication` sync state, `flush_lag` or `replay_lag` (P-14).

**Recommendation.**
- Use `synchronous_commit = on` for delete-class transactions and set the role default to `local`.
- Add the standby services, `synchronous_standby_names = 'ANY 1 (shard_N_standby)'` and the extra hosts to §9.1 and the cost model.
- Alert on sync-standby absence and on `flush_lag`.

### P-8: major: the move loader as written cannot run, and its copy rate is bounded by HNSW insertion

**Where.**
- §3.8 "Move loader … `CREATE TEMP TABLE tmp (LIKE <table>) ON COMMIT DROP; COPY tmp FROM STDIN BINARY; INSERT INTO <table> SELECT * FROM tmp ON CONFLICT …`" (§5.0 has "SELECT … FROM tmp").
- §9.5 "≈ 1,000 facts/s per stream".

**Evidence.**
- `CREATE TEMP TABLE tmp (LIKE facts); INSERT INTO facts SELECT * FROM tmp;` fails with `ERROR: cannot insert a non-DEFAULT value into column "fact_type_code" … is a generated column`. This applies equally to `live` and `tag_count` on `facts`, `chunks` and `observation_versions`, and the error is raised even for an empty `tmp`.
- `LIKE` copies generated columns as plain columns.
- The source side cannot `COPY facts TO STDOUT` either: `ERROR: cannot copy from partitioned table "facts"` (it must be `COPY (SELECT <non-generated columns> …) TO`).
- The positional `BINARY` stream must therefore list columns explicitly on both ends. That also makes §9.2's "COPY streams map columns positionally" guard insufficient: equal `schema_version` does not imply equal column *order* after a dropped and re-added column.
- Rate: a 5,000-fact `INSERT … SELECT` took 1.85 ms per fact with HNSW live versus 0.05 ms without. A stream is therefore ≈ 540 facts/s before BM25, GiST, GIN and B-trees, at a third of the target partition size.

**Recommendation.**
- Generate the loader's column lists from `pg_attribute` (`attgenerated = ''`, ordered by name), identical on both shards.
- Copy into the vector side table (P-1 (a)) with its HNSW **dropped on the target partition set** for the incoming namespace's bulk phase, then rebuild. This is legal because the namespace is `incoming` and unreadable.
- Re-derive A-O17′ from that.

### P-9: major: Temporal is a single fleet-wide `auto-setup` cluster whose history-shard count is never chosen and cannot be changed later; it is the fleet's write-path SPOF and throughput ceiling

**Where.**
- §9.1 `temporal: image temporalio/auto-setup:1.28.0`, 2 CPU / 4 GB; `temporal-postgres` with default settings, no volume tuning, no backup, no replica.
- N71 "ONE Temporal cluster per FLEET".
- R22/Q14 ("history shard count … cannot be changed after history accumulates"; decision deferred to Phase 1 exit).

**Why it matters now.**
- **History shards.** `numHistoryShards` is fixed when the cluster's persistence is created. `auto-setup` defaults to a handful (4), so a dev cluster cannot be grown into the production one. Q14's deferral turns into a cluster migration with every in-flight workflow drained.
- **Event rate.** Every retain chunk is ≈ 5 activities (`ExtractChunk`, `EmbedChunk`, `ResolveEntities`, `BuildLinks`, `CommitChunk`), each ≈ 3 history events plus workflow tasks. That is ≈ 20 events per chunk.
  - At the plan's own fill pace (§5.1.6: ≈ 29 days for 1 B facts across 4 cells, ≈ 40 chunks/s fleet-wide online), that is ≈ 800 events/s of steady persistence load before consolidation, purge, moves and the per-shard schedules.
  - `RetainBackfill` children "run at Postgres speed", which is one to two orders of magnitude more.
  - One 2-CPU frontend/history/matching process on one untuned Postgres is not sized for that, and nothing in §9 or §10 measures it.
- **Blast radius.** Cells are the failure domain (D3), but every cell's retain, consolidation, purge and move stops when the one fleet cluster or its database stops. `temporal-postgres` has no pgBackRest stanza, so a disk loss also loses every in-flight operation fleet-wide. Only the per-shard `operations` rows and the op-sweeper survive it.

**Recommendation.**
- Decide the production Temporal layout in M0, not at Phase 1 exit:
  - split services;
  - `numHistoryShards` sized for the backfill peak (e.g. 512–2,048);
  - a dedicated HA Postgres with backups;
  - load-tested with the retain activity shape.
- Re-examine N71. A cluster per cell, with cross-cell moves using the operations-table restart path the plan already has (N97), keeps D3's failure domain.

### P-10: major: migration rules that PostgreSQL 16 cannot honour on this schema: no `CREATE INDEX CONCURRENTLY` on partitioned tables, and stored generated `live` columns that can change only by rewriting the table

**Where.**
- §9.2 "index creation is always `CREATE INDEX CONCURRENTLY` … a migration never renames a column or changes a type in place".
- Generated `live` on `chunks`, `facts` and `observation_versions` (N41, N84 already changed the `observation_versions` definition once).
- §3.3.4 "indexes created on the parent".

**Evidence.**
- `CREATE INDEX CONCURRENTLY facts_test_cic ON facts (…)` fails with `ERROR: cannot create index on partitioned table "facts" concurrently`.
- PG 16 has no `ALTER COLUMN … SET EXPRESSION` (a syntax error here; it arrived in PG 17).
- `ALTER TABLE … ADD COLUMN … GENERATED ALWAYS AS (…) STORED` rewrote the test table (the relfilenode changed) under `ACCESS EXCLUSIVE`.

**Consequences.**
- Every index change on the five big tables needs the per-partition procedure: `CREATE INDEX CONCURRENTLY` on each partition, `CREATE INDEX … ON ONLY` the parent, then `ATTACH PARTITION`. §9.2 and `engramctl` do not describe it.
- Any change to the visibility predicate means dropping and re-adding `live` on 10 M-row partitions under an exclusive lock. That also drops and rebuilds every BM25 and partial index that references it: hours of downtime per shard, ×100 shards.

**Recommendation.**
- Replace generated `live` with a plain boolean maintained by a `BEFORE INSERT OR UPDATE` trigger function. A predicate change then becomes a `CREATE OR REPLACE FUNCTION` plus a batched backfill, or better, P-1's side table.
- Write the partitioned-index procedure into §9.2 and `engramctl`, with its HNSW build time per partition (measured: 67 s for 200 k rows with 4 workers, ≈ 3.5–4 min per 625 k partition).

### P-11: major: the transaction-ID arithmetic is wrong by about 8×, so anti-wraparound vacuums, including HNSW repair, come about monthly on every partition, and nothing monitors XID age

**Where.** §3.7: "Transaction-id age is not a concern at these rates (`autovacuum_freeze_max_age` default, ≈ 2 × 10⁸ transactions per shard-year at 50 writes/s)".

**Recomputation.**
- 50 writes/s × 3.15 × 10⁷ s ≈ **1.6 × 10⁹ XIDs per year**, not 2 × 10⁸.
- The relay and consumers update `outbox_cursors` "every 100 ms" (§3.7 vacuum table). With ≈ 4 consumer rows, that alone is ≈ 40 XIDs/s.
- At ≈ 90 XIDs/s the default `autovacuum_freeze_max_age = 2 × 10⁸` is reached every ≈ 26 days. Every one of the ≈ 80 partitioned heaps then gets a non-cancellable aggressive vacuum.
  - That includes the 71 GB of `fact_links`.
  - Any dead tuples present trigger the HNSW repair of P-4.
- Nothing in §9.4 exports `age(datfrozenxid)` or `age(relfrozenxid)`.

**Recommendation.**
- Batch cursor advances (one transaction per second, not per 100 ms).
- Set `vacuum_freeze_min_age` low on insert-mostly partitions, so the insert-triggered vacuums freeze as they go.
- Export XID age with a page-level alert (P-14), and correct §3.7.

### P-12: major: exclusive fence takers starve behind legal writers that last tens of seconds, and every failed 5 s attempt is a 5 s write brownout for the namespace. Measured; REVIEW-3 H-18 states the shape

**Where.** Header of `shard_schema.sql` (fence protocol), §9.5 "exclusive takers … `lock_timeout = 5 s` per attempt and retry with jitter".

**Evidence.**
- With a shared holder present, a queued `pg_advisory_xact_lock` makes `pg_try_advisory_xact_lock_shared` return **false** (verified), so writers are refused while the exclusive request waits.
- The exclusive request then timed out (`canceling statement due to lock timeout`) behind a 6 s shared holder.
- P-1 measured that one delete-cascade or `FinalizeVersion` transaction on a 2 k-fact document holds the shared fence for ≈ 5 s, and on a 10 k-fact document for ≈ 25–30 s. `PurgeBatch` (1,000 facts) holds it for ≈ 0.3 s plus HNSW work.
- So every move freeze, delete freeze or restore fence attempted during such a transaction fails. Each attempt first refuses every writer of the namespace for 5 s. The retry loop turns into a periodic write outage plus a move watchdog rollback.

**Recommendation.**
- Bound *writer* duration, not only the exclusive wait. P-1's O(1) visibility cut and batched retire transactions under 1 s are the structural fix.
- Have the exclusive taker first announce the freeze through a namespace-level `freeze_pending` flag that writers check *before* starting new long transactions, then acquire.
- Report `engram_namespace_frozen_retries_total` per cause.

### P-13: minor: backup sizing assumes file-level differentials stay small; HNSW and retire churn touch almost every segment; the Postgres image probably cannot run `archive_command`

**Where.**
- §9.3 "Differential daily … ≈ 10–30 GB/day at 10 chunks/s".
- §9.1 `archive_command=pgbackrest … archive-push` inside `paradedb/paradedb`.
- The `pgbackrest-conf: {}` volume, mounted read-only.

**Points.**
- **Differential size.** pgBackRest differentials copy every 1 GB relation segment that changed.
  - HNSW insertion rewrites neighbour pages throughout the graph.
  - Retire updates land on old heap pages.
  - Vacuum repair (P-4) touches most of the index.
  - Most segments of the facts, chunks and observation HNSW indexes, heaps and B-trees therefore change daily, and a differential approaches a full backup (≈ 150–300 GB). Block incremental (`repo-block=y`, pgBackRest ≥ 2.46) is the fix and should be the stated setting.
- **WAL volume.** WAL is ≈ 13 KB per inserted fact (P-1). 1 M facts per day per shard is ≈ 13 GB of WAL before vacuum and `REINDEX`. The "≤ 15 min WAL replay" RTO term should be computed from that.
- **Image and config (not verified here).** The ParadeDB image is built on the official Postgres image, which does not ship pgBackRest. If `archive-push` is absent, archiving fails, WAL accumulates in `pg_wal` until the volume fills, and the shard stops. `pgbackrest-conf` is an empty named volume, so no stanza or repo config exists either.

**Recommendation.**
- Build a shard image that includes pgBackRest.
- Bind-mount generated configs.
- Enable block incremental.
- Add `pg_wal` size and archive-failure alerts.

### P-14: minor: observability lacks the Postgres signals this design depends on, and the `ShardDown` alert reads a metric nothing exports

**Where.** §9.4 metrics and alerts; the §9.1 Compose has no `postgres_exporter`.

**Missing signals.**
- `pg_up` is used by `ShardDown` and `CatalogDown`, but no exporter service exists. The only PostgreSQL metrics are the application's own and pgBackRest's.
- Not exported at all:
  - autovacuum runtime and progress per partition (P-4);
  - `n_dead_tup` per vectored partition;
  - `age(datfrozenxid)` (P-11);
  - sync-standby state and `flush_lag`/`replay_lag` (P-7);
  - `pg_stat_progress_create_index` for `engramctl index` builds;
  - `pg_wal` size;
  - long-running transactions holding the namespace fence (P-12).

**Recommendation.** Add `postgres_exporter` per shard with custom queries for the above. Add alerts on:
- XID age above 50 % of `autovacuum_freeze_max_age`;
- any autovacuum longer than 30 min;
- no sync standby;
- the oldest transaction holding an `advisory` share lock older than 10 s.

### P-15: minor: Compose and role configuration drift from the text

**Where.** §9.1 and §9.5, `shard_schema.sql` role block.

| Item | Text says | Artifact does |
|---|---|---|
| HNSW parallel build | "parallelise with `max_parallel_maintenance_workers=4`" | not in the `postgres -c` list, so the default of 2 applies |
| `autovacuum_max_workers` | relied on by the bloat plan | not set (default 3; see P-4) |
| `ef_search` | §3.8 "≥ the arm cap (150 at MID, 400 at HIGH)" | `engram.yaml` `hnsw: { ef_search: 100 }` |
| lock timeouts | §9.5 "`ALTER ROLE engram_app, engram_worker SET lock_timeout`" | `engram_worker` does not exist in the SQL; `ALTER ROLE` takes one role |
| per-document exclusive lock in the cascade | "`lock_timeout = 5 s`" | `engram_app`'s role default is 2 s; the cascade must `SET LOCAL lock_timeout = '5s'` and does not say so |
| sync standby | N92 | no `synchronous_standby_names`, no standby services (P-7) |

**Recommendation.** Generate the `postgres -c` list and the role GUCs from one table checked by `engramctl config lint`.

### P-16: minor: the catalog's `namespaces_count` only ever increments, so placement drifts

**Where.** `catalog_schema.sql` `pick_shard` (`UPDATE shards SET namespaces_count = namespaces_count + 1`). Nothing decrements it on delete or move-out, and nothing increments it on a move target.

**Evidence.** `pick_shard('t1')` run as `catalog_app` works and increments the count (verified). No trigger, function or §5/§9 step adjusts it afterwards.

**Consequence.** Over a shard's life `namespaces_count < max_namespaces` (150) blocks placement on shards that have been emptied by moves or deletes. Targets also accept namespaces beyond the cap.

**Recommendation.** Derive the count from `namespaces` (`count(*) WHERE shard_id = … AND state <> 'deleted'`) in `pick_shard`. Alternatively, maintain it with a trigger on `namespaces` that covers `shard_id` and `state` changes.

### P-17: minor: a 32-shard cell is not a "one host or a few hosts" Compose project

**Where.** §9.1 "one host or a small fixed set of hosts (A-O1)"; `deploy.resources.limits` 8 CPU / 64 GB per shard; R20/Q5 open.

**Arithmetic.**
- 32 × (8 CPU, 64 GB) is 256 CPUs and 2 TB of RAM for primaries alone, plus api, workers, Envoy and pgbouncer.
- N92 standbys double that and must sit on other hosts (P-7).
- The memory cgroup limit counts the container's page cache, so `effective_cache_size = 48GB` is the *whole* cache available to a shard. That is less than the ≈ 40 GB hot set plus the 25 GB facts heap (P-6).

**Recommendation.**
- State hosts per cell (e.g. 4 shards per 512 GB host, 8 primary plus 8 standby hosts) and the overlay or static-endpoint decision now. Every sizing number in §3.7 and §9 depends on it.
- Size `effective_cache_size` and the container limit from the measured hot set.

### P-18: minor: deletion replay on restore re-applies `Invalidate` but not `Restore`, and the cascade it re-runs needs an `active` fence the restoring shard does not have

**Where.** §9.3 step 4: `deletion_log` kinds include `memory` (= Invalidate); "re-applies the D8 synchronous cascade". The cascade is "one fenced write transaction as `engram_app`", which requires ownership `active` at the epoch, while the shard is `frozen/restore` during step 4.

**Consequences.**
- A fact invalidated before T′ (T < T′) and restored after it is re-invalidated by the replay. Curation regresses silently.
- The replay cannot run on the code path the text names. It needs an admin variant that bypasses the fence under the restore freeze, and the D16 tests must cover that variant.

**Recommendation.**
- Log `Restore` (kind `memory_restore`) and replay the *last* state per subject.
- Specify the admin replay path and include it in the T3 restore drill.

### P-19: nit: `engram_cleanup_namespace` probes all 16 partitions per deleted row

**Where.** `DELETE FROM %I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I WHERE namespace_id = $1 LIMIT $2)`.

**Evidence.** `EXPLAIN` on `fact_links` shows a Nested Loop with an Append of **16 Tid Scans** (`TID Cond: ctid = ANY_subquery.ctid`, `Filter: tableoid = …`). The outer `DELETE` has no `namespace_id` predicate, so it is not pruned.

**Recommendation.** Add `AND namespace_id = $1` to the outer `DELETE`. That gives one Tid Scan per row and also stops a ctid on another partition from being probed at all.

### P-20: nit: the lineage walk's `UNION` keeps depth in the row, so diamonds are explored once per distinct path length; callers recompute it two or three times

**Where.** `engram_lineage_walk`, `engram_flag_lineage`, `engram_adjust_invalidation`.

**Evidence.** In the P-3 DAG, 5,749 distinct versions produced 9,886 walk rows (73 ms per call), and the invalidation path runs the walk three times.

**Recommendation.** Use a `CYCLE`/`SEARCH`-free formulation that carries a visited array, or a plpgsql BFS with a temp table keyed on `(obs, ver)` that keeps `min(depth)`. Call it once per trigger.

---

## Checked and found sound (no finding)

- **Statement-level transition-table triggers under FK cascades** fire once per statement, not once per parent row. A `DELETE` of 1,000 facts showed 7 RI triggers × 1,000 calls, but `observation_inputs_lost` and `observation_sources_orphans` each fired with `calls=1`; a counting trigger saw a single 1,000-row transition table and a single 9,945-row `fact_links` set. Purge of 1,000 facts with ≈ 10 k links took 308 ms.
- **Touch triggers** (`engram_touch_updated_at`) are negligible beside the index costs above.
- **Fence semantics.** `pg_try_advisory_xact_lock_shared` refuses while an exclusive request is queued (verified). This is the N82 behaviour; its cost is P-12.
- **RLS pruning.** With `namespace_id` bound as a literal, RLS folds into a one-time filter and the other 15 partitions are pruned.
- **Storage.** `STORAGE MAIN` is inherited by all 16 partitions of each vectored table.
- **DDL.** Both DDL files apply cleanly on PG 16 with pgvector 0.8.6, and every self-check passes.
- **`pick_shard` as `catalog_app`.** The column-level `UPDATE` grant suffices for `FOR UPDATE SKIP LOCKED`, and identity columns need no sequence grant.
- **HNSW size and build.** Index size matches §3.7 (≈ 1.95 KB per vector). The partition build rate (67 s per 200 k rows, 4 workers) supports "rebuilds in minutes".

## Verdict

**Not ready to staff the storage layer as specified.** The schema is carefully reasoned for correctness, but it was not costed against how pgvector and PostgreSQL behave.

The central problem is one decision: inline vectors (N94) on rows whose visibility is mutable state in a generated column referenced by indexes (N41, N79, N84). That makes every retire, invalidate, restore, flag flip and supersede a full HNSW insertion now (P-1, P-3) and an HNSW graph repair later (P-4). The delete path is about 50× over its SLO and cannot complete for large documents; lineage flagging costs seconds per fact; vacuum, not ingest, bounds purge and move cleanup.

Separating vectors from mutable state, and making the synchronous delete cut O(1), fixes P-1, P-3, P-4, P-8 and most of P-12 together. Four further points are independent and needed regardless:
- the `as_of` frontier fix (P-2);
- RLS-aware plans (P-5);
- a decided Temporal production layout (P-9);
- PG-16-legal migration mechanics (P-10).

With those decisions the design is buildable. The remaining items are sizing and operations hygiene.

## What I could not verify

- **pg_search.** It is not installable here, so nothing about BM25 was executed:
  - its cost under non-HOT updates;
  - its plan and Top-K pushdown under RLS as `engram_app` (P-5);
  - segment merge and vacuum behaviour;
  - its WAL volume.
- **Cold-cache latencies.** These would have required restarting a shared server and dropping the OS cache; the I/O terms in P-6 are computed. Everything else ran at 200 k facts per partition, not 625 k, so HNSW insert, search and vacuum numbers are lower bounds.
- **Real embeddings.** Clustered synthetic vectors understate cross-tenant filter rejection, so the shared-topic HNSW costs in P-6 are extrapolated from the tag and `as_of` filter runs.
- **Untested components.** pgbouncer (1.21+ prepared statements, `MAX_PREPARED_STATEMENTS`), Temporal throughput and history-shard behaviour, pgBackRest differential sizes, and whether the ParadeDB image ships pgBackRest (P-13).
- **Synchronous replication.** Not run; REVIEW-3 H-4 executed the blocking behaviour.

---

## Engram: adversarial review 5 (API contract, numbers, completeness, Hindsight parity)

**Reviewed:** `plans/engram/` at `fce0cd4` (v1.2, after D20 and D21), including `proto/`, `sql/`, `formal/`, the sections and `reference/hindsight-notes.md`, against the task brief.
**Lens:** (1) the API contract: protos, §4, MCP and Connect mappings, the evolution policy and `buf breaking`; (2) the numbers: D3, N54/N106, N109, §3.7, §6.8, §10/N105 and the places that cite them; (3) completeness against the brief; (4) Hindsight parity.
**Not repeated:** findings accepted in `REVIEW.md` (F-1…F-45) and `REVIEW-2.md` (G-1…G-30). I raise one of those again only where its fix is missing or wrong, and I say so at that finding.

**Tool runs (from the repository root, because the `.git#` form fails inside `proto/`):**

```text
$ buf lint / buf build / buf format -d --exit-code     (in plans/engram/proto)  → all exit 0
$ buf breaking plans/engram/proto --against '.git#ref=50304db,subdir=plans/engram/proto'
  25 breaks, all in buf.build/engram/internal, none in memory.v1 / memory.admin.v1:
  23 × events.proto  Field "N" … changed type from "string" to "bytes"   (ChunkCommitted, ChunksRetired,
       DocumentDeleted, FactInvalidated, FactRestored, Observation*, Entity*, Page*)
   1 × events.proto  Previously present field "2" with name "ids" on message "RowsPurged" was deleted
   1 × workflow.proto Previously present field "9" with name "item_index" on message "ChunkWork" was deleted
```

My judgement of these breaks is A-11.

Severity scale (same as the earlier reviews): **blocker** means a stated guarantee is false or a required path cannot run; **major** means a wrong number or contract that changes a decision; **minor** means drift or a gap with a local fix; **nit** means cosmetic.

---

## Findings

### A-1: blocker: deleted content is still served through pages. Page versions have no deletion hiding, so D16 is false for Reflect, `GetPage` and `SearchPages`

**Where.** `proto/memory/v1/page.proto` header and `GetPage`/`SearchPages`; §3.3.6 `page_versions` DDL; §5.4.6 table (row "Explicit delete", column Pages: "`stale_delete` in the cascade"); §2.2.16; D16; §4.4 ("no … page version derived from content mentioned after T is returned").

**Claim.** D16: "from then on nothing from the document is returned by Recall, Reflect, GetMemory or Export." page.proto: "a delete-stale page may be citing content that must not be shown, while a write-stale page is merely incomplete."

**Evidence.**
- The delete cascade does only two things to pages. It deletes `page_sources` and sets `pages.stale_delete = true` (§5.4.1 step 8; §5.4.6).
- `page_versions(… markdown_blob_key, effective_at, superseded_at, evidence_hash …)` has no `derived_from_deleted`, no `hidden_by_invalidation` and no inputs table. Nothing in §2.2.16, §5.3 or `GetPage` withholds a `stale_delete` page.
- So after an acknowledged delete, all of the following still return markdown written from the deleted facts or observations until a refresh lands:
  - `GetPage` (current version, or any older version through `version`/`as_of`);
  - `SearchPages` (`include_content`, and `snippet` ≤ 512 B);
  - Reflect's `get_page` and `search_pages` tools;
  - the MCP `get_page`/`search_pages` tools.
- The refresh is asynchronous. If `refresh_policy.on_delete` is false, it waits for cron. Old versions are never fixed: a refresh writes a new version and leaves the old ones servable by `version`/`as_of`. This is the F-1/G-1 bug again, one level up the derivation chain. N41/N79/N84 were applied to observation versions only.
- `Invalidate` also marks pages `stale_delete` (§5.4.6), with the same consequence.

**Recommendation (decision level).** Treat pages exactly like observations:
- `page_version_inputs(page_id, version, source_kind, source_id)` records every fact and observation version shown to the refresh prompt.
- The cascade sets `page_versions.derived_from_deleted` (permanent) and the reversible `hidden_by_invalidation` counter. It also handles transitive lineage, because the previous page text is in the prompt.
- `GetPage`, `SearchPages` and Reflect skip flagged versions, and a `stale_delete` current version is hidden until rewritten. `GetPage` returns `NOT_FOUND{PAGE version}` or a typed `PreconditionFailed{PAGE_STALE_DELETE}`.
- Add pages to `DocLifecycle.tla`'s `NoDeletedContentRecalled` and to the §8.5 leak canary.

### A-2: blocker: move rollback after cutover (b) cannot execute. `ReleaseNamespace` has a different contract, and the ownership state machine has no edge out of the target's `active` row

**Where.** N98; §5.5.1 step 10 and the §5.5 failure table ("a target outage or watchdog expiry here rolls back by `ReleaseNamespace(target, e + 1)` then thawing the source"); `proto/memory/admin/v1/admin.proto` `ReleaseNamespace`; `sql/shard_schema.sql` `ownership_transitions` (lines 709–726) and `engram_cleanup_namespace` (line 1690).

**Claim.** N98: "a failure before (c), including a target outage after (b), rolls back by `ReleaseNamespace(target, e + 1)` then thawing the source." §5.5.1: "(admin RPC: deletes the target's rows and ownership)".

**Evidence.**
1. admin.proto defines `ReleaseNamespace` as "after the PurgeNamespace workflow removed the last rows it marks the catalog row DELETED". Its request carries `epoch`, and the call is "FAILED_PRECONDITION if the catalog disagrees". During this rollback the catalog still says epoch `e` (step (d) never ran), so the call as contracted fails. If an implementer "fixes" the precondition, the RPC marks the namespace DELETED in the catalog.
2. After (b), the target row is `active @ e+1`. The immutable transition table admits `rollback_target` only `FROM incoming`. The only edges out of `active` are `start_move`, `abort_move`, `freeze_*` and `epoch_bump`. No role may delete or demote an `active` target row, and `engram_cleanup_namespace` deletes data only for `moved_out`/`incoming`.
3. The watchdog-driven rollback therefore fails on every retry. The source stays `frozen` (writes refused with `NamespaceFrozen`) with no automatic exit. This is exactly the window N98 promised to make safe.

**Recommendation.** Make the target revert a first-class edge: `revert_target` = `active → incoming` at the same epoch, role `engram_move`, allowed only while the catalog and the source show the move non-terminal and the source row is `frozen/move`. After it, the existing `rollback_target` delete applies. Give it its own admin RPC (`MoveService.RevertTarget`) instead of overloading `ReleaseNamespace`, whose delete semantics must stay as they are. Add the "target outage after (b)" interleaving to W-12 as a must-pass configuration.

### A-3: major: the hot working set leaves out the facts heap that N94 made 2.4× larger. The "IOPS sizing in 3.7" does not exist

**Where.** §3.7 (facts row; hot-working-set paragraph "≈ 40 GB"); D3 footprint row; §3.3.4 ("heap fetches per recall × QPS enter the IOPS sizing in 3.7"); §2.2.8 (`PlanPartitionHNSW`); N94, N109; A-1 (NVMe ≥ 10 k IOPS).

**Claim.** The hot set is "facts HNSW 18 GB, BM25 4 GB, facts B-trees 3.4 GB, chunk and observation HNSW 3 GB, … ≈ 10 GB → ≈ 40 GB".

**Evidence.**
- **The heap is not in the hot set.**
  - pgvector's HNSW stores only the vector. Every visited tuple is a heap fetch, which applies `namespace_id`, `live`, `mentioned_at` and tags.
  - At the 10 M-fact target, N109 says the partial-index band is empty, so every namespace ≥ 20 k facts uses the shared partition index.
  - With 150 namespaces in 16 partitions, a typical namespace is ≈ 10–11 % of its partition. A MID query (cap 150) then visits ≈ 1,400 tuples, and HIGH (400) ≈ 3,700, each a random heap page.
  - Namespaces < 20 k use the exact plan, which is itself a heap scan of ≤ 20 k × 2.4 KB = 48 MB.
  - So the facts heap is on the read path of every recall.
- **The heap is bigger than stated.** With `STORAGE MAIN` the heap is not 12 GB but "≈ 25 GB". The table's own "3 rows per 8 KB page" gives 10 M / 3 × 8 KiB = **27.3 GB**.
- **The hot set exceeds RAM.**
  - Realistic hot set: ≈ 40 + 27 (facts heap) + ≈ 5.6 (chunks heap, same argument for the chunk arm) ≈ **73 GB**.
  - Available: a 64 GB instance with `shared_buffers = 16 GB`, `work_mem = 64 MB` × connections, and pg_search and autovacuum memory.
- **IOPS.** At 50 QPS × ≈ 1,400 page touches ≈ 70 k page reads/s for the fact arm alone, a miss rate of 15 % saturates the A-1 10 k IOPS.
- **No IOPS sizing exists.** §3.3.4 and §2.2.8 defer to "the IOPS sizing in 3.7", and §3.7 contains none.

**Recommendation.**
- Decide one of:
  - (a) revert facts to `STORAGE EXTERNAL`/TOAST, and accept the TOAST probes only on the exact plan; or
  - (b) size the shard at 128 GB RAM; or
  - (c) lower the soft cap to ≈ 6 M facts.
- Write the IOPS table that §3.3.4 promises: page touches per recall by plan and budget, times QPS, times assumed miss rate. Gate M0.6 on it.
- Restate D3's "≈ 40 GB hot" from the result.

### A-4: major: time-to-fill is about 2.7× too optimistic, and the A-F = 4 sensitivity goes in the wrong direction. This is the F-34/N74 fix, and its arithmetic is wrong

**Where.** D3 "Retain throughput"; §1.4, §5.1.6; §6.8 Table 6.8-B rows "Initial fill of 1 B facts" and "Wall time of an LME-S run"; R30; executive summary ("≈ 29 days and ≈ $160 k"); M3.7.

**Claim.** "≈ 29 days of continuous ingest at 10 chunks/s × 4 cells … At A-F = 4: ≈ $90 k, ≈ 12 days." §5.1.6: "5–10 chunks/s/worker ≈ 15–30 k chunks/h ≈ 60–120 k facts/h."

**Evidence.**
- **Every gateway call per chunk shares the cap.** The same table states that for LME-S every stage shares the 600 RPM cap ("the earlier '≈ 6 h' counted extraction only"). Per chunk at A-F = 10 that is 1 extract + 1.25 consolidation batches + 0.1–0.25 summaries + 0.375 dedup ≈ **2.7 calls**.
  - Sustained ingest at 600 RPM ≈ 10 / 2.7 ≈ 3.7 chunks/s per cell.
  - 100 M chunks / (4 × 3.7) ≈ **78 days**, not 29.
  - The "29 days" figure counts extraction only, which is the error the LME-S row says was fixed.
- **A-F = 4 for 1 B facts needs more chunks, not fewer.**
  - 1 B facts at A-F = 4 is 250 M chunks: ≥ 72 days on extraction alone, ≈ 190 days with every stage sharing the cap.
  - The table's own "Per 1 k facts" row says ≈ $0.21 at A-F = 4, i.e. ≈ **$210 k** for 1 B facts, not $90 k.
  - "12 days" equals 29 × 4/10. It follows from neither reading.
- **Facts per hour.** 5–10 chunks/s = 18–36 k chunks/h = 180–360 k facts/h at A-F = 10. "60–120 k facts/h" is the A-F = 4 figure, which contradicts N74's single-assumption rule.
- **Document count.** The fill row says ≈ 10 M documents, but the per-1 k-facts row (25 documents per 1 k facts) implies 25 M.

**Recommendation.**
- Restate throughput as gateway calls per chunk, and give the D3 formula as `min(32/L, RPM_cap / (60 × calls_per_chunk))`. Note that consolidation can use a different model and therefore a different RPM cap.
- Re-derive the fill row for both A-F values with a fixed fact count.
- Decide explicitly whether `RetainBackfill` (now on the uncommitted track) is a launch prerequisite. At ≈ 78 days per online fill it probably is.

### A-5: major: the public `Operation` cannot express what the pipelines do. The read-barrier promise is false for superseded retains, and tenant deletes have no operation kind

**Where.** `proto/memory/v1/operation.proto` (`OperationResult`, `OperationKind`, file header); N49; §5.1.2 step 7.4; N70; §4.1.1 ("so the `DELETE_NAMESPACE`/`DELETE_TENANT` operations … can be awaited"); D16.

**Claim.**
- N49: "ends `SUCCEEDED` with `superseded_by = <version>` in the operation result".
- operation.proto: "after WaitOperation returns SUCCEEDED for a retain, Recall on the same namespace observes every fact of that document version."

**Evidence.**
- **`superseded_by` is missing.** `OperationResult` has no such field; it exists only in the internal `FinalizeVersionResult`. §5.1.2 also sets `document_version = c` (the *current* version, not the operation's own).
- **Clients cannot see a superseded `REPLACE`.** A client cannot tell that its version never became active. The barrier sentence is then false: superseded chunks are retired by the newer finalise.
- **Retain racing a delete.** This ends `CANCELLED{document_deleted}` (§5.1.2 step 7.3), but `OperationError`/`CANCELLED` carry no reason field for a non-FAILED state.
- **`DELETE_TENANT` is missing.** `OperationKind` has no `DELETE_TENANT`, yet N70 and §4.1.1 promise to await it. `DeleteTenantResponse` returns per-namespace operations only.
- **`MOVE_NAMESPACE` has no backing workflow.** It is a public kind, but the move workflow is `move/{ns}/{epoch}`, not `ns/{ns}/op/{operation_id}`. That contradicts the header ("every … Operation [is] backed by exactly one Temporal workflow id `ns/{namespace_id}/op/{operation_id}`"), and nothing says on which shard's `operations` table it lives.

**Recommendation.**
- Add `OperationResult.document_version` semantics ("this operation's version"), `superseded_by`, and a `cancel_reason` enum.
- Restate the barrier as "…observes every fact of that version unless `superseded_by` is set".
- Either add `OPERATION_KIND_DELETE_TENANT` with a tenant-scoped `GetOperation` (admin API), or delete the promise.
- Define where the MOVE operation row lives, or drop the kind from the public enum.

### A-6: major: idempotency keys at the edges collide or are not deterministic

**Where.** §4.6 MCP table (`retain`: "`request_id` = MCP request id"); §4.1.3; `RetainItem.document_id` comment ("Empty: the server mints a UUIDv7"); `RetainRequest.operation_id` ("per-document ids are derived as UUIDv5(operation_id, document_id)").

**Evidence.**
- **MCP request ids are per-session counters.** JSON-RPC ids in MCP are unique only within a session (typically `1, 2, 3…`). `request_id` is scoped to (tenant, namespace, method) for 24 h. Every reconnecting agent (or two agents mounting the same namespace) reuses `id = 1…`.
  - The first `retain` with different content fails `ALREADY_EXISTS{IDEMPOTENCY_KEY_REUSED}`, and the §4.6 error text tells the agent to "use a fresh id", which it does not control.
  - Identical content is silently answered with the other session's stored response.
- **Minted document ids break retry idempotency.** For items without `document_id`, the minted id is a fresh UUIDv7 per attempt. A retry that carries `operation_id` but no `request_id` (or comes after 24 h) derives different per-document operation ids, so it creates duplicate documents and duplicate facts.

**Recommendation.**
- MCP: derive `request_id = sha256(mcp-session-id ‖ jsonrpc-id ‖ tool)`, or omit it and use `operation_id = UUIDv5(session, id)`.
- Minted documents: derive the document id deterministically, `UUIDv5(operation_id, "item/" ‖ index)`, whenever an `operation_id` is present. Without one, document that such retains are not replay-safe.

### A-7: major: a per-namespace `models.embed` or `EmbedDims` override silently corrupts semantic recall

**Where.** §2.2.22 `Models{… Embed …; EmbedDims int /* 768; 512 with Matryoshka truncation */}`; D12 keys `models.{…,embed,…}` (also in the namespace.proto comment); D15; `sql/shard_schema.sql` (`embedding halfvec(768)` and `embedding_model text NOT NULL` on facts, chunks and observation versions).

**Evidence.**
- **Mixed vector spaces.** A tenant `memory.admin` can set `models.embed` on a namespace. From then on, queries are embedded with the new model and new facts get new-model vectors, but every existing vector stays.
  - Delta retain keeps unchanged chunks with "no embedding".
  - `extraction_key` does not include the embed model.
  - `embedding_model` is written and **never read**: no arm filters on it, and no workflow re-embeds on change. (grep: the column appears only in DDL and INSERT lists.)
- **512-d does not fit.** `EmbedDims = 512` cannot be stored in `halfvec(768)` at all.

**Recommendation.**
- Make `models.embed` and `EmbedDims` system-level only, or fixed at namespace creation.
- If a change is allowed, it is a versioned migration: a `ReembedNamespace` workflow, dual vectors or a shadow column during the switch, and the arms filtering `embedding_model = current`.
- Reject the override in the config allow-list until then.

### A-8: major: quota deferral covers only retain extraction, about 40 % of LLM spend. Consolidation, Reflect and page refresh are unmetered at admission, and the share rule starves new namespaces

**Where.** §5.1.2 step 5 (`CheckQuota` before each retain wave); §5.2 (no quota step); §2.4 limits table (Reflect: "≤ 4 concurrent reflects per namespace"); D13; §6.8 ("consolidation is ≈ 53 % of all-in ingest cost"); PD-4.

**Evidence.**
- **Only retain checks the quota.** `CheckQuota` appears only in the retain step table. Consolidation (53 % of ingest cost), Reflect (strong model, $0.04 typical and $0.45 worst case per call) and page refresh (strong model) have no deferral or admission check on `llm_tokens_per_day`.
- **Reflect has only a concurrency cap.** 4 concurrent × 300 s sessions allow ≈ 48 worst-case reflects per hour per namespace (≈ $21/h), unbounded by the daily quota.
- **The share rule penalises ingesting namespaces.** The per-namespace share is `tenant_limit × max(live_facts, 1000) / Σ`, recomputed daily. It is proportional to facts already stored, not to ingest demand. A tenant with one 1 M-fact namespace and a new one gives the new namespace ≈ 0.1 % of the tenant's tokens, so its first document defers at once. That is the opposite of the workload.

**Recommendation.**
- Put one `quota.Reserve(tokens_estimate)` gate in front of every gateway call class: extract, consolidate batch, refresh, and each Reflect iteration. Reflect gets `RESOURCE_EXHAUSTED` because it is synchronous; workflows defer.
- Compute shares from trailing usage plus a floor, and allow borrowing of unused share at the next daily recompute.
- State the task's "quota enforcement with deferral" per call class in D13.

### A-9: major: `PageService`'s refresh contract cannot express what §5.3 reads, and §5.1.6 cites a `Retain` field that does not exist

**Where.** `page.proto` `RefreshPolicy{RefreshTrigger trigger; Duration interval; Duration debounce}`; §5.3 trigger table (`refresh_policy ? 'cron'`, `refresh_policy.cron`, `refresh_policy.force_on_cron`, `refresh_policy.on_delete = immediate`); §2.2.16 `Page{… TagFilter []string; RefreshPolicy string /* "after_consolidation" | "cron:<expr>" | "manual" */}`; §5.1.6 ("started by `engramctl backfill` or by `Retain{priority = BATCH}`").

**Evidence.**
- **`RefreshPolicy` mismatch.** The proto has a periodic `interval` (1 h…30 d) and no cron, no `force_on_cron` and no `on_delete`. The pipeline evaluates a cron expression and an `on_delete` flag that no client can set.
- **Go `Page` mismatch.** The Go `Page` type uses a bare tag list where the proto has a `TagFilter` with a mode.
- **No `priority` field.** `RetainRequest` has fields 1–4 only. N105 moved `RetainBackfill` off the committed track, but the trigger path through `Retain` is still described.

**Recommendation.**
- Choose one `RefreshPolicy` (add `cron`, `on_delete`, `force_on_cron` to the proto, or rewrite §5.3 to interval semantics) and generate the §2 type from the proto.
- Either add `RetainRequest.priority` (enum `INTERACTIVE|BATCH`) or remove the sentence.
- Extend the N103 `gen-docs` check to proto-field references in §5.

### A-10: major: the propagation pass that N103 accepted was not done. Binding register rows contradict N53, N54, N106 and N82, and the rerank-skip threshold has three values

**Where.** D2 rationale cell; D3 recall row; D5 steps 2 and 6 plus its safety argument; D10 Rerank row; D15; D17; §9.3 config `rerank_min_remaining: 120ms`; executive summary; §11 Q16.

**Claim.** N103: "The propagation pass rewrites D2, D3, D5 and D15 in place (D20/D21 become history)… (8) D3's recall row follows N53/N54 (216 ms p95, `bge-reranker-base` in D15)."

**Evidence.** As of `fce0cd4`:
- **D3** still reads: "cross-encoder on 150 pairs ≤ 120 ms p95 … → ≈ 215 ms", "5 arms in parallel ≤ 60 ms".
- **D15** still names `bge-reranker-v2-m3` as the rerank default.
- **D5** step 2 still takes "`SELECT … FOR UPDATE` on the source ownership row (it waits for every in-flight writer, which holds `FOR SHARE`…)", and its safety argument still relies on "`FOR SHARE` on an ownership row".
- **D2** rationale: "the ownership `FOR SHARE` row is the fencing token for shard moves."
- **D10** (not in G-25's list) still has rerank on the "top 50/150/300" and skip when "remaining deadline < 150 ms". The §4 proto says 0/50/150 and 106 ms (N106), and the production config in §9.3 says `rerank_min_remaining: 120ms`. That is three thresholds, one of them in a binding row and one in the deployed config.
- **Executive summary** still says "a partial HNSW for large ones", which N109 contradicts (the band is empty at the target).
- **D17** still says 112/74, while N105 says "total ≈ 118 ew … unchanged", although the earlier total was 112.
- **§11 Q16** still debates `stale_delete` for invalidation, which N84 removed.

This is G-25 items 1 and 8: the fix was accepted but not applied.

**Recommendation.** Do the fold now; it is mechanical. Make `rerank_min_remaining` a derived constant (`rerank_p95 + pack + stream + 8 ms`) with one source, and lint D-rows against N-rows in `make gen-docs`.

### A-11: major: the internal schema changes since `50304db` break the plan's own evolution policy. They are intended but not recorded as exceptions, and the in-place retype decodes silently wrong

**Where.** `buf breaking` output above; §4.5 ("The internal module follows the same `buf breaking FILE` gate as the public one … a running Temporal history *is* a wire client"; "Change a field's number, type … → Add a new field, deprecate the old one"; "A field whose interpretation changes bumps [`schema_version`]"); events.proto header ("Changes are additive; removed field numbers are reserved"); `proto/README.md` (CI compares against `main`, "the only override is a `v2` package").

**Judgement.**
- **The breaks are intended.** All 25 trace to documented decisions: N80 (ids as 16-byte `bytes`, `RowsPurged.ids` removed) and N86 (`item_index` → `item_indexes`). The two deletions are correctly `reserved` by number and name. Nothing in `memory.v1` or `memory.admin.v1` broke.
- **They are not documented as policy exceptions.** No changelog entry or exception note exists, and `Event.schema_version` and `RetainDocumentInput.schema_version` stay `1`.
- **The CI gate would block the change.** It would fail the very PR that made it, with no sanctioned override.
- **The retype is unsafe.** `string → bytes` on the *same* field numbers is wire-compatible, so a consumer built at the new commit decodes a 36-byte ASCII UUID from a pre-change event (7-day outbox retention, Kafka retention, open Temporal histories) as a 36-byte "id" without an error. That is exactly the silent misinterpretation §4.5 forbids.

**Recommendation.**
- Declare in §4.5 a pre-1.0 exception for both modules, with the list of breaks, and bump both `schema_version`s to 2. Better still, renumber: new `bytes` fields and reserve the old `string` ones.
- Add a CI `buf breaking` baseline tag (`proto/v1.0.0`) so the gate compares against a release, not `main`.

### A-12: minor: internal-only error details live in the public package and are therefore frozen forever

**Where.** `proto/memory/v1/errors.proto` (`WrongShardOrEpoch.target_shard_id/next_epoch`, `DocumentBusy`, `InputBlobMissing`, `FREEZE_REASON_FENCE_BUSY`); admin.proto header ("This is the ONLY place where shard ids and epochs appear in an API"); §4.5 ("Nothing is ever removed from `memory.v1` itself").

**Evidence.**
- §4.1.6 says these "never reach public callers" and that `target_shard_id` is "stripped before the error leaves the API". Stripping is a runtime discipline, not a contract: one missed strip leaks shard ids.
- Under the plan's own rule, the messages can never be removed from `memory.v1`.

**Recommendation.** Move them to `engram.internal.errors.v1` (or `memory.admin.v1`), and keep a public `WrongShardOrEpoch` without shard fields.

### A-13: minor: proto comments, the stated source of truth, contradict the decisions

**Where.** `memory.proto` `Invalidate` ("Observations keep the source row but are marked stale_delete and hidden from Recall until reconsolidated"); `Restore` (no decrement, no `remote_apply`); `namespace.proto` `config_overrides` (lists only the D12 keys and rejects unknown keys); `export.proto` header (delta "derived from the outbox" and present only "when … its outbox range has not been pruned"); §4.5 deprecation text ("the three removals … `wait_for_visibility`, `min_score`, `include_untagged`").

**Evidence.**
- N84: invalidation marks the observation `stale_write` (visible) and uses the per-version counter; the observation-level `stale_delete` was removed.
- N66 says the `config_overrides` comment is generated from `config.Keys`; it is not. `Keys` itself lacks N79's `consolidate.max_rebuilds_per_round`, so the resolver would reject the knob N79 introduced.
- D12/N30 make deltas always producible by diffing snapshots.
- There are four reserved removals, not three: `RetainItem.mentioned_at` (rc5) is missing from the list.

**Recommendation.** Fix the comments. Add `consolidate.max_rebuilds_per_round` to `Keys`. Make the N66 generation and the M3.4 exit ("`buf lint` fails if the proto comment and the Go table disagree") an M0.8 check instead of a Phase 3 one.

### A-14: major: "incremental export versions" degrade to full re-syncs after any delete, and synced local copies keep deleted content indefinitely

**Where.** `export.proto` `SnapshotManifest.expired` ("the next CreateSnapshot must be a full one"); N59; D12 Export; M3.2.

**Evidence.**
- **Every delete forces a full re-download.** Every document delete expires every snapshot that contains the document. In a namespace with routine deletes (chat retention, user data requests), almost every snapshot expires before the next one is taken, so every sync is a full re-download. That defeats the brief's "incremental versions".
- **The local copy is the bigger leak.** A thin client that synced v{n} keeps the deleted facts on the agent's disk until it next polls `ListSnapshots` and notices `expired`. Nothing pushes or forces the deletion, and the sync client has no stated obligation to purge.
- **The delta format already handles deletes.** It carries "ordered upsert/delete records". A delta from an expired base to the new version is exactly what removes the content locally. Expiry only needs to stop *serving the old bytes*; it does not prevent computing a delta from them.

**Recommendation.**
- Separate "base no longer servable" from "no delta from base". Always emit `delta-v{n-1}-v{n}` with delete records, even when v{n-1} expired.
- Add `deleted_ids` (or a tombstone file) to the manifest.
- Make `engram-sync` apply deletes before any read and refuse to serve a local copy whose manifest is expired. State this local-copy obligation in D16.

### A-15: major: entity names bypass `as_of` and the delete cascade

**Where.** `common.proto` `EntityRef{canonical_name, mention}` on every `Memory`; §3.3.5 `entities(canonical_name …)`, `entity_aliases`; §5.1.2 entity upsert; §5.4.1 (entities pruned only when `mention_count = 0` at purge); `EntitiesMerged` event; graph arm entity links.

**Evidence.**
- **`as_of` leak.** `canonical_name` and the alias set are namespace-wide current state, built from every mention including those after T. Under `as_of = T`, a pre-T fact is returned with a canonical name, or merge results, learned after T (for example "Bob" merged into "Robert Smith, CFO of X"). A post-T merge also creates entity-link paths between pre-T facts, so post-T knowledge shapes graph-arm ranking.
- **Delete leak.** If the canonical name or an alias came from a deleted document but the entity still has other mentions, it survives the cascade and the purge (pruning needs `mention_count = 0`). It keeps being returned on other facts' `EntityRef`.
- **Not covered elsewhere.** Neither leak is covered by the §8.5 canary or by `AsOf.tla`.

**Recommendation.**
- Under `as_of`, return only `mention` (the surface form inside the fact's own chunk) and suppress `canonical_name`. Restrict graph entity hops to mentions with `mentioned_at ≤ T`.
- On delete, recompute `canonical_name` and aliases from the remaining mentions in the purge.
- Add both cases to the leak canary.

### A-16: minor: `Invalidate` is lost on re-extraction, although §4.4 says curation "is meant to stick"

**Where.** N26, N58, N87 (new `extraction_key` → re-extract → old facts retired); §5.4.5; §4.4 edge rules ("curation is not time-travelled; a curator's decision is meant to stick"); hindsight-notes §1 ("Reprocessing a document resets any curation applied to extracted facts").

**Evidence.**
- Any re-retain after a prompt or model bump, a `retain.mission` edit, an entity-hint change or a summary/heading change (all in `render_hash`, N87) re-extracts the chunk.
- The re-extraction produces a fresh, un-invalidated twin of the curated-away fact, and N58 retires the invalidated original.
- Nothing carries `invalidated_at` across, and nothing documents the reset. Hindsight at least documents its reset.
- §6.7 names `Invalidate` as the remedy for injected content, which is exactly the content a prompt bump brings back.

**Recommendation.** Keep a `curation(namespace_id, document_id, content_hash, fact_text_hash, action)` table that is applied at `CommitChunk` to the new facts of the same chunk hash. Otherwise, state the reset in the `Invalidate` contract and return a warning in `RetainResponse` when curated chunks were re-extracted.

### A-17: major: rerank depth is cut 6× against the brief and Hindsight without a quality gate

**Where.** N53 (`rerank_top` 0/50/150); task brief ("Rerank the top ~300 candidates"); hindsight-notes §3 (`RERANKER_MAX_CANDIDATES=300`, rerank at every budget); M0.5 (measures throughput and latency only); §8.6 ablations.

**Evidence.**
- MID reranks 50 and LOW reranks nothing. The decision was driven by reranker throughput (F-6), which is legitimate.
- The plan never measures what depth 50 costs in recall. No ablation, exit criterion or risk row compares R@5 or accuracy at depth 50, 150 and 300.
- The Phase 1 exit "LME-S R@5 ≥ 0.93" does not say at which depth. Engram's §1.9 marks the row "same idea".

**Recommendation.** Add `--rerank-top {0,50,150,300}` to the B.1 ablation, required in M1.2's exit. Write the depth/latency/quality trade-off into N53 as measured, and record a departure from the brief in §11 if depth stays at 50.

### A-18: minor: Hindsight capabilities dropped without a non-goal row (part of F-33, never dispositioned)

**Where.** hindsight-notes §9 (`GET /memories/{mid}/history`, `transfer/import`, `clone`, directives CRUD with `priority`/`is_active`/`tags`, `GET/PATCH config` returning resolved and raw config, document `original_text`, `retry_operation`, `PATCH memory`, `sync_retain`); `namespace.proto` (`repeated string directives`); `memory.proto`; §12.

**Evidence.**
- **Observation history.** Engram versions observations, which §1.9 offers as its differentiator, but no RPC lists the versions of an observation. `GetMemory` has neither `version` nor `as_of`.
- **Import and clone.** There is no import or clone, so an export cannot be restored, merged or used to seed a namespace.
- **Directives.** Directives are untyped strings, losing tag-scoped and prioritised directives ("tagged directives apply only when the reflect request carries matching tags").
- **Resolved config.** No RPC shows the effective resolved config, so a tenant cannot see which model a namespace will use.
- **Document body.** `Document` does not return the body.
- **F-33 items (c) and (f).** Entity-level results, and operation retry or memory edit, were accepted in F-33 but are not in N73 or §12.

**Recommendation.** For each item, either add an RPC (cheap ones: `GetMemory.version/as_of`, `ListObservationVersions`, `GetEffectiveConfig`, `Directive{text, priority, active, tags}`) or add an NG row with a reason.

### A-19: minor: Reflect's typical cost is understated about 2×

**Where.** §6.8 prompt table (`reflect/v1`: "≈ 6 000 per iteration of tool results, cumulative; typical session 6 iterations ≈ 40 k input total") and Table 6.8-A ($0.04).

**Evidence.**
- **Billed input.** If tool results accumulate in the context, as the "cumulative" wording and the 100 k context cap imply, the six calls bill Σ (1.2 k + 6 k·i) for i = 1…6 ≈ **133 k** input tokens. Forty thousand is the *final context*, not the billed total.
- **Cost.** At 90 % cache hits that is 13.3 k × $2.50 + 120 k × $0.25 + 2.1 k × $10 per M ≈ **$0.085**.
- **Inconsistent with the worst case.** The worst-case row ($0.45) is computed cumulatively.

**Recommendation.** Use one method for both rows, and re-derive the Phase 2 cost gate and §8.8 from it.

### A-20: minor: the rationale for streaming Recall is numerically wrong, and the Connect "bookmarkable GET" cannot work

**Where.** §4.1.7 Recall row ("Unary with `repeated` results (… 4 MiB message concerns at HIGH budget)"); D10 ("flushed in groups of 10 after the last stage that fits the deadline"); §4.7 ("makes reads cacheable and bookmarkable").

**Evidence.**
- **Message size.** A HIGH response is 16 k tokens ≈ 64 KiB of text. Even 500 results with full metadata are ≈ 1 MiB, far below 4 MiB.
- **No early results.** Results stream only after packing completes, so the stream delivers no early results. A deadline cannot fall mid-stream except during the few milliseconds of serialisation.
- **What streaming costs.** Connect streaming forces POST with the binary envelope, which plain-JSON clients and curl `-d` cannot use.
- **Bookmarkable GET.** GET requires the mandatory `Connect-Timeout-Ms` and `Authorization` headers, so a bookmarked URL is rejected (`INVALID_ARGUMENT`/`UNAUTHENTICATED`). Caching responses that carry `Authorization` needs explicit `Cache-Control`, which the plan does not state.

**Recommendation.**
- Keep streaming because the brief asks for it, but state the honest rationale: the stats trailer and future progressive arms. Add unary `RecallUnary` (or a Connect-only `unary` alias) for JSON clients.
- Drop "bookmarkable", or allow a default deadline for GET only.

### A-21: minor: the delete SLO's outbox arithmetic contradicts N80's own elision rule

**Where.** N80 ("a 100 k-fact document: ≈ 5 s and ≈ 400 rows"; "When a set exceeds 4,096 ids … `ids_elided = true` and counts only").

**Evidence.** 100 k fact ids plus about 10 k chunk ids exceed 4,096, so `DocumentDeleted` is a single elided event. "≈ 400 rows" is the un-elided page count.

**Recommendation.** Restate: "1 elided `DocumentDeleted` + ≤ 16 pages for every list under the elision threshold". Size `TestDelete_CascadeScales` on that.

### A-22: minor: recall connection and CPU sizing contradict the deployed pool

**Where.** §2.4 limits ("5 arms, each one pooled conn; ≤ 50 QPS/shard → ≤ 250 concurrent arm queries per shard ≤ pool headroom"); §9.1 (`DEFAULT_POOL_SIZE: "24"`, `max_connections=100`); §4.1.2 ("the per-instance pool is only 32 (D3)") vs §2.4 and §1.8 (16 per process per shard); D3 (8 vCPU).

**Evidence.**
- **Pool size.** 250 concurrent queries cannot fit a 24-connection pgbouncer pool. The sentence confuses per-second with concurrent: concurrency ≈ 50 × 6 statements × 40 ms ≈ 12. That fits, but only with the cascade, ack and Reflect traffic sharing the same 24.
- **CPU.** No CPU budget exists for 50 QPS × 6 statements (two HNSW scans with ≈ 1.4 k heap rechecks, two BM25 Top-K, graph, temporal) plus `CommitChunk` HNSW inserts and pg_search merges on 8 vCPU.
- **Pool figure.** The "32" in §4.1.2 matches no other number.

**Recommendation.** Write one connection and CPU budget per shard (statements × mean duration × QPS, plus writes), fix §4.1.2 to 16, and add recall-under-ingest CPU to M0.5.

### A-23: minor: the schedule text disagrees with its own Gantt, and M0.8's scope overlaps work the commit already claims

**Where.** §10.2 ("*Phase 1 exit (the MVP, week 20)*" vs "Phase 1 — MVP (45 ew, weeks 4–24)" and the Gantt, where Phase 1 exit follows E1's chain 3+1+2+2+2+2+10+2 = week 24); N105 ("total ≈ 118 ew … unchanged"; it was 112); M0.6 (A-F measured "with E2's prompt" in week 4, while `extract/v1` is built in M1.1, weeks 4–13); M0.8 ("decisions N79 to N108 land in §1 to §12, `sql/` and `proto/`").

**Evidence.**
- **Totals.** The milestone sums are correct: Phase 0 = 15, Phase 1 = 45, Phase 2 = 17, Phase 3 = 27, F/B = 14, committed 77, separate 41, total 118. Per engineer: E1 25.5, E2 26.0, E3 25.5.
- **Two dates and the word "unchanged" are wrong.** The MVP exit is in week 24, not week 20, and the total rose from 112 to 118.
- **M0.8 duplicates work.** Commit `fce0cd4` says D21 was already applied "to DDL, protos and all sections". M0.8's 3.5 ew therefore either duplicates work or is really "make the four must-fail TLA+ configurations exist". None of `DocLifecycle_NoLineage`, `AsOf_InvalidateSuperseded`, `ShardMove_DeleteDuringCatchup` or `ShardMove_PerTableNewSnapshot` is in `formal/tla/`.

**Recommendation.** Fix the dates. Re-scope M0.8 to what is actually missing (the configurations, `gen-docs`, and A-1/A-2/A-9/A-13 of this review). Note that E2 at zero slack is the Phase 2 critical path.

### A-24: minor: formal conformance artefacts claimed in §7 are missing or stale

**Where.** §7.6 ("`formal/lean/` is a Lake project (`lakefile.lean`, `lean-toolchain` …) … `formal/lean/SORRY_BASELINE` (currently 3)"); `formal/lean/` contents; §7.4(c) refinement mapping; §7.5 ("the version row's `FOR SHARE`"); the brief ("Protobuf is also the schema for Temporal payloads").

**Evidence.**
- **Lean.** `formal/lean/` contains only `Engram/*.lean`: no lakefile, no toolchain pin, no baseline, so `lake build` cannot run. `RRF.score_mono`, the monotonicity property the brief names, is one of the three `sorry`s.
- **Refinement mapping.** The mapping still maps `obs[o].st` to an observation-level state, with no per-version `derived_from_deleted`, lineage or `hidden_by_invalidation`. §7.5 still assumes the `FOR SHARE` version-row lock that N83 removed.
- **Workflow payloads.** `TenantDelete` (N36), `RetainBackfill` (N31) and the per-shard sweeper schedules have no message in `workflow.proto`, although "every top-level workflow input … carries `schema_version`".

**Recommendation.** Add the Lake skeleton and baseline, prove `score_mono` in Phase 0 (it is a routine list induction), update §7.4(c) and §7.5 to D21, and add the missing workflow inputs.

### A-25: nit: the MCP adapter does no OAuth resource-server work

**Where.** §4.6 ("the adapter never inspects claims except to decide which tools to list"; `Authorization` forwarded unchanged; 401/403 "before any MCP framing").

**Evidence.**
- Current MCP authorization requires the server to validate that the token was issued for it, and to advertise Protected Resource Metadata (RFC 9728) and a `WWW-Authenticate` challenge so clients can obtain tokens.
- `aud: engram` is shared by the core, so forwarding is acceptable. But the adapter must still validate the token before listing tools, and it publishes no discovery metadata, so standard MCP clients cannot complete an OAuth flow against it.

**Recommendation.** Have the adapter call the same `TokenVerifier`, and serve `/.well-known/oauth-protected-resource` per endpoint.

### A-26: nit: packing can return nothing where Hindsight returns the top result

**Where.** D10 packing ("an item that does not fit is skipped, not truncated"); `RecallRequest.max_tokens` (minimum 256); hindsight-notes §3 ("the top-ranked result is always returned whole").

**Evidence.** With `max_tokens = 256` and a 300-token top chunk, Engram can return zero results. The Lean `Packer` proves the budget bound, not this parity property.

**Recommendation.** Always emit rank 1, and count its overflow in `tokens_used`, or document the divergence in §1.9.

---

## Verdict

**Not ready to staff Phase 1 as written. The blocking set is small and specific.**
- **A-1** (pages leak deleted content) breaks D16 and the brief's delete invariant on three public surfaces. Its fix mirrors N41/N79/N84, so it belongs in Phase 0″, not M3.1.
- **A-2** makes the move's safety valve unexecutable in exactly the window N98 introduced. It must be fixed in the transition table and the admin contract before M1.5.
- **A-3** and **A-4** change D3. Either the shard shrinks or its RAM doubles, and the 1 B-fact fill takes about 2.5 months online, not one. Both are decisions, not edits.
- The API contract is otherwise unusually careful: buf lint, build and format are clean, and pagination, field masks, deadlines and error details are coherent. Its remaining defects are drift between the protos and §5 (A-5, A-9, A-13) and idempotency at the edges (A-6).
- **A-7** and **A-8** are cheap to fix now and expensive after tenants have data.
- The second review's register fold (A-10) was accepted and not done. Until it is, the "binding" register contradicts the sections on the rerank, fence and move rows.

## What I could not verify

- **Runtime behaviour** (no Postgres or pgvector here): whether an HNSW scan on PG 16 with pgvector 0.8 fetches the heap for every visited tuple under `iterative_scan = relaxed_order` with non-indexed filters. A-3 assumes yes, per pgvector's documented filtering. I also could not check actual page-cache hit rates or CPU per arm.
- **Gateway facts:** whether the 600 RPM cap is per model, per cell or per organisation. A-4 assumes per model per cell, as D3 does; per organisation would make it 4× worse. I also could not check whether consolidation's model shares extraction's cap.
- **Proofs and specs:** Lean files were not type-checked (no toolchain). TLA+ was not re-run; I relied on the committed logs and §7's own statements.
- **MCP client behaviour:** that specific clients reuse JSON-RPC ids across sessions (A-6) is per the JSON-RPC and MCP specs, not tested against a client.
- **Hindsight parity:** taken from `reference/hindsight-notes.md` only. Items it marks UNVERIFIED (for example graph hop counts) were not used.

---

## ADVICE-R3 — redesign of storage, deletion visibility, delete durability and moves

Scope: the areas where REVIEW-3 (H-*), REVIEW-4 (P-*) and REVIEW-5 (A-*) found regressions twice
(D20 → D21). This is a redesign, not a third patch. Product guidance that binds it: **deletes are
rare**; the synchronous part of a delete is an **O(1) soft-delete marker** that every read path
honours at ack; all physical work is an **asynchronous, throttled expunge**; SLOs may degrade for a
namespace while a delete is being expunged; simple and correct beats cheap-at-delete-time.

The five decisions below share one principle: **rows that carry vectors or BM25 text are immutable;
mutable state lives in narrow side tables; visibility is a read-time predicate over small marker
sets, never a flag walked and stamped at delete time.** Everything in REVIEW-4's cost table and most
of REVIEW-3's races follow from violating that principle.

---

## R1. Storage layout: immutable content rows, marker tables, per-namespace HNSW

**Closes** P-1, P-3, P-4, P-6, P-8, P-10, A-3, A-7, P-12 (structurally), H-19.

### Mechanism

1. **Content tables are insert-only and purge-only.** `facts`, `chunks`, `observation_versions`,
   `page_versions`, `fact_links`, `entity_mentions`, `observation_inputs`,
   `observation_version_sources` get no `UPDATE` path at all: no `retired_at`, `purge_after`,
   `invalidated_at`, `invalidation_reason`, `live`, `stale_delete`, `derived_from_deleted`,
   `hidden_by_invalidation`, `superseded_at`, `tags` columns. `fillfactor = 100`. The only DML after
   insert is `DELETE` by the expunge worker. Every generated column is dropped (`fact_type_code`,
   `tag_count`, `live`): P-10's migration problem disappears with them.
2. **Vectors move to insert-only side tables** keyed by content id and embedding generation:
   `fact_vectors(namespace_id, memory_id, embedding_model, embedding halfvec(768)) PK (namespace_id,
   memory_id, embedding_model)`, same for `chunk_vectors(…, chunk_id, …)` and
   `observation_version_vectors(…, ov_id, …)`; hash-partitioned by `namespace_id` like their parents.
   `ReembedChunk` and an embedding-model change insert new rows; the arms read
   `embedding_model = <namespace's current model>`. That is the versioned re-embed A-7 asks for; the
   `models.embed` override becomes a `ReembedNamespace` workflow, never a config flip.
3. **No shared HNSW.** Each vector table carries **one partial HNSW per namespace per partition**,
   `CREATE INDEX CONCURRENTLY fv_<ns8> ON fact_vectors_pNN USING hnsw (embedding halfvec_cosine_ops)
   WHERE namespace_id = '…' AND embedding_model = '…'`, created by the stats sweeper when a namespace
   crosses **2,000 live vectors** (dropped below 1,000; `engramctl index`, owner role; CIC is legal on
   a partition, P-10). Below 2,000 the arm does an exact scan of ≤ 2,000 rows (≤ 3.2 MB). ≈ 150
   namespaces × 3 tables = ≤ 450 small indexes per shard, which PostgreSQL handles without issue.
   Queries that must match a partial-index predicate run with `plan_cache_mode = force_custom_plan`.
   Consequences: a query visits only its own namespace's graph (P-6's 1/selectivity term is gone,
   visited tuples ≈ `ef_search`); a namespace delete or move cleanup is `DROP INDEX` + batched
   `DELETE`, with **no HNSW graph repair** (P-4); a document purge dirties only one small index, and
   the expunge worker rebuilds it (`REINDEX INDEX CONCURRENTLY` on a partition index is legal) when
   its dead fraction exceeds 5 %, instead of letting autovacuum repair it.
4. **Mutable state lives in narrow tables** (`fillfactor 50–70`, HOT forever):
   - `documents` keeps `state`, `current_version`, **`tags`** (tags are item-level in the API; the
     per-fact copy is dropped; the tag filter is resolved once per recall into an allowed-document
     set, see R2.4), `metadata`, `context`.
   - `fact_state` is **gone**; retirement is expressed by markers (R2.1).
   - `observations` keeps `current_version`, `proof_count`, `stale_write`, `stale_delete`, `tags`,
     `retired_at`. `observation_version_meta(namespace_id, observation_id, version, superseded_at)`
     holds the one write-once value a later version adds to an earlier one (D9/N33). `pages` and
     `page_version_meta` likewise.
5. **Lock key spaces are separated** (H-19): namespace fence = one-argument
   `pg_*_advisory_*lock(hashtextextended(ns::text, 0))`; derivation lock (R2) = one-argument
   `hashtextextended(ns::text, 1)`; document lock = two-argument `(hashtext(ns), hashtext(doc))`.
   One- and two-argument advisory locks are distinct lock tags, so no cross-kind collision exists.

### Shard re-sizing (from REVIEW-4's measurements)

| Item | Value | Basis |
|---|---|---|
| RAM | **128 GB**, `shared_buffers = 32 GB`, container limit 112 GB, `effective_cache_size = 96 GB` | hot set below; A-3 option (b) |
| Hot set at 10 M facts | HNSW 18 + vector heaps 16 (every HNSW visit fetches its heap tuple; unavoidable on pgvector) + facts content heap 8 (0.8 KB rows, 10/page) + BM25 4 + B-trees 3.4 + links PK half 10 + chunk/obs vectors and HNSW 6 ≈ **65 GB** | P-6, A-3 |
| Page touches per MID recall | 3 vector arms × ≈ 150 visited × 2 pages + BM25 ≈ 200 + graph ≈ 300 ≈ **1,400**; at 50 QPS and a 5 % miss rate ≈ **3.5 k IOPS**, within A-1's 10 k | per-namespace index: visited ≈ `ef_search` |
| Semantic-arm plan | exact ≤ 2,000 vectors; per-namespace HNSW above; `ef_search` = arm cap (150 MID, 400 HIGH); no shared index, no "band" | P-6 crossover |
| Write cost | `CommitChunk` ≈ 10 inserts into a ≤ 1 M-element index ≈ 2–5 ms HNSW; retire/invalidate/flag = 0 HNSW work | P-1 |
| Move copy rate | bounded by B-tree/BM25 inserts only: the target has no HNSW for an `incoming` namespace until the mover builds it after the bulk copy (R4) | P-8 |
| XID budget | cursor advances batched to 1/s per consumer; `vacuum_freeze_min_age = 10 M` on content partitions; `age(relfrozenxid)` exported and alerted at 50 % | P-11 |

**Rejected:** option (b) of P-1 (keep flags on the vectored row, partial `WHERE live` HNSW,
`fillfactor 60`): a retire still rewrites a 2.4 KB tuple, BM25 still re-indexes it, and every later
visibility change we have not yet thought of lands on the same row again. A shared partition HNSW
with per-namespace partials only "in the band" (N94): its cost is 1/selectivity and its vacuum repair
is shard-wide; the band was a patch on top of it.

**TLA+ (new `Storage.tla`, small):** `ContentImmutable` (no action changes a content row after
insert except Purge, which requires an expunge marker); `VectorGenerationConsistent` (an arm reads
only vectors of the namespace's current generation); `IndexConvergence` (liveness: after Purge, the
namespace index eventually contains exactly the live vectors).

---

## R2. Deletion and invalidation visibility: markers + read-time derivation check + async expunge

**Closes** H-1, H-2, H-3, H-12, H-23, H-24, P-2, P-3, A-1, A-15, A-16, and the blast-radius problem.

### R2.1 Markers (the only synchronous writes of a delete or invalidation)

```sql
CREATE TABLE document_tombstones (namespace_id uuid, tenant_id text, document_id text,
  deleted_at timestamptz NOT NULL, operation_id uuid, expunge_state text NOT NULL DEFAULT 'pending'
    CHECK (expunge_state IN ('pending','materialized','purged')), PRIMARY KEY (namespace_id, document_id));
CREATE TABLE chunk_tombstones    (namespace_id uuid, chunk_id uuid, retired_at timestamptz NOT NULL,
  reason text CHECK (reason IN ('replace','reextract')), PRIMARY KEY (namespace_id, chunk_id));
CREATE TABLE fact_hidden         (namespace_id uuid, memory_id uuid, hidden_at timestamptz NOT NULL,
  reason text NOT NULL, PRIMARY KEY (namespace_id, memory_id));          -- Invalidate; Restore deletes the row
CREATE TABLE curation_log        (namespace_id uuid, memory_id uuid, content_hash bytea, document_id text,
  action text, at timestamptz, reason text);                              -- A-16: re-applied at CommitChunk by content_hash
```

- `DeleteDocument` = document lock (exclusive), `documents.state = 'deleting'`, **one
  `document_tombstones` row**, `deletion_log`, outbox `DocumentDeleted`, commit. Milliseconds for any
  size. Nothing else is touched synchronously.
- `FinalizeVersion(REPLACE)` inserts `chunk_tombstones` for chunks not in the new membership (one
  statement, ≤ chunk count); un-retire on a flap is `DELETE FROM chunk_tombstones`. N58
  re-extraction retires the old facts of a kept chunk through `fact_hidden(reason = 'reextract')`.
- `Invalidate(f)` = `INSERT fact_hidden`; `Restore(f)` = `DELETE fact_hidden` (+ the materialized
  rows of R2.3 with `cause = ('invalidation', f)`). No counter, no `CHECK`, no walk: H-3 is gone.

### R2.2 Visibility is a read-time predicate

Facts and chunks: `visible(f) ≡ f.document_id ∉ DocTomb ∧ f.chunk_id ∉ ChunkTomb ∧ f.memory_id ∉
FactHidden`, where the three sets are the namespace's marker rows. The recall layer loads them **once
per request** (three indexed selects; sizes are bounded by expunge lag and alerted above 16 k) and
passes them as array parameters (`<> ALL($n)`) to every arm, including the external-index join of
N44; above 16 k entries an arm falls back to an SQL anti-join. `as_of` stays `mentioned_at <= T` on
the immutable row.

Derived versions use **evidence segments** instead of lineage:

- `observation_versions.root_version` (immutable): `= version` for a **root rebuild** (written from
  live sources only, no previous text shown), else `root_version(v−1)`. The derivation set of
  `(O, v)` is **`inputs(O, w)` for all `root_version(v) ≤ w ≤ v`**: everything its writer could have
  seen (the batch facts attached to O and the text of the previous version, which was itself written
  from the segment's earlier inputs). `observation_inputs` gains `document_id` (denormalised) and
  indexes `(namespace_id, document_id)` and `(namespace_id, fact_id)`.
- **Visibility of `(O, v)`:** `NOT EXISTS (SELECT 1 FROM observation_inputs i WHERE i.namespace_id =
  $ns AND i.observation_id = O AND i.version BETWEEN v.root_version AND v.version AND (i.document_id
  = ANY($doc_tomb) OR i.fact_id = ANY($fact_hidden)))` **and** `NOT EXISTS (derived_hidden …)` of
  R2.3. Plus the `as_of` range from `observation_version_meta`, plus `NOT (observations.stale_delete
  AND v.version = current_version)` is **removed**: a current version whose inputs are intact stays
  visible; one with a victim in its segment is hidden by the predicate itself.
- **Pages:** `page_versions.root_version`, `page_version_inputs(namespace_id, page_id, version, kind
  ∈ {fact, observation}, source_id, source_version, document_id)`. A page version is hidden if any
  fact input in its segment is tombstoned/hidden **or any observation-version input `(O, w)` in its
  segment is hidden by the observation rule**. The derivation graph has fixed depth two (fact →
  observation → page; pages never feed observations, Reflect output is never stored as a recall
  surface), so this is two `EXISTS`, not a walk. `GetPage`, `SearchPages`, Reflect's `get_page` /
  `search_pages`, the MCP tools and the export apply the same predicate; a hidden current page
  returns `PreconditionFailed{PAGE_HIDDEN}` until the refresh lands (A-1, H-12).
- **Entities (A-15):** under `as_of`, `EntityRef` carries only `mention`; `canonical_name` and alias
  merges are suppressed and graph entity hops use `entity_mentions.mentioned_at <= T`.
  `entity_aliases` gets `document_id`; the expunge deletes the victim's aliases and recomputes
  `canonical_name` from the remaining mentions.

Why this closes the races: nothing is computed at delete time, so there is no snapshot to be stale
(H-1) and no recorded set to drift (H-3). A version that commits after the marker and names the
victim is still hidden, because the predicate is evaluated over the committed `observation_inputs`
at read time. There is no depth bound because there is no walk (H-2, P-2). The blast radius is
exactly "versions whose own segment saw the victim" (R2.5 makes that set small).

### R2.3 Expunge: one throttled workflow per namespace, four phases

`Expunge` (Temporal, id `ns/{ns}/expunge`, `SignalWithStart`, one at a time per namespace, every
activity fenced `active` at epoch and skipped while a move is non-terminal):

1. **Materialize** (one short transaction per affected observation batch, under the **derivation
   lock** taken exclusive; see R2.4): for each pending marker, insert
   `derived_hidden(namespace_id, kind ∈ {observation, page}, id, root_version, from_version,
   cause_kind ∈ {document, invalidation}, cause_id)` with `from_version = min(version)` of the inputs
   naming a victim in that segment. Mark observations `stale_delete` (rewrite needed) and pages
   `stale_delete`; nudge `Consolidate` (root rebuilds, `max_rebuilds_per_round`) and `PageRefresh`.
   Set `document_tombstones.expunge_state = 'materialized'`. From here the read predicate's
   `observation_inputs` lookup is redundant for that marker and is skipped (the recall layer passes
   only `pending` tombstones in `$doc_tomb`; `derived_hidden` is a PK-prefix lookup per candidate).
2. **Purge rows** in batches of 1,000 with 50 ms pauses: `facts`, `fact_vectors`, `fact_links`,
   `entity_mentions`, evidence rows (FK cascade), chunks, versions, ledger rows of an explicit delete;
   then `blob_tombstones` for `xcache`, `ver/`, `ledger/`, proposal blobs.
3. **Index hygiene:** per touched partition index of the namespace, `REINDEX INDEX CONCURRENTLY`
   when dead fraction > 5 % (`pgstattuple`), else leave to autovacuum.
4. **Finish:** `document_tombstones.expunge_state = 'purged'`; the row is deleted 24 h later
   (`DocumentDeleted{purged}` event; the operation ends `SUCCEEDED`). `derived_hidden` rows for
   document causes are **permanent** (an older version written with the victim in view must never
   resurface at any `as_of`); their versions are physically deleted only when the observation or
   page itself is retired or the namespace is deleted.

**Degraded mode while `pending` markers exist** (accepted per product guidance): the observation and
page arms pay the `observation_inputs` lookup per candidate (measured budget: ≤ 150 candidates × ≤
60 input rows, index-only, ≈ 10–20 ms per arm); `Consolidate` for the namespace runs only root
rebuilds until phase 1 finishes; the rerank-skip SLO is suspended for the namespace. An alert fires
when a marker stays `pending` > 15 min.

**Guarantees stated once (replace D16's delete paragraph):**
- *At ack:* no fact, chunk, observation version, page version, entity alias or export part derived
  from the document is returned by Recall, Reflect, GetMemory, GetPage, SearchPages, ListMemories or
  StreamSnapshot; exports whose snapshot predates the ack are marked `expired` and refused.
- *After expunge (SLA: materialize ≤ 15 min, rows and blobs purged ≤ 24 h, index rebuilt ≤ 48 h):*
  no row, vector, index entry or blob of the document exists on the shard; Temporal histories expire
  with retention (N99).
- *Invalidate/Restore:* visibility flips at commit of the marker row; `Restore` is exact because it
  deletes the marker and the rows that carry its cause, and nothing else encodes the invalidation.

### R2.4 Concurrency: one derivation lock, used only by writers of derived versions

- `ApplyBatch` (stage 2 of R2.5), `PageRefresh` commit and `Expunge.Materialize` take
  `pg_try_advisory_xact_lock_shared(hashtextextended(ns, 1))` (writers, refused → retry) or
  `pg_advisory_xact_lock(…)` (materialize, exclusive, `lock_timeout = 35 s` in one attempt so a legal
  30 s writer cannot starve it; H-18/P-12). Markers take **no** lock: they are correct without one
  because visibility is read-time.
- The lock exists for exactly one reason: Materialize must see every version whose inputs name the
  victim, and a writer whose re-verification predates the marker but whose commit postdates it holds
  the shared lock across both, so Materialize waits for it. Apply re-verification is the R2.2
  predicate over `$fact_ids` in a fresh statement (no `FOR SHARE` on facts: facts are immutable and
  cannot be locked meaningfully).
- Tag filter: the recall layer resolves `tag_match` once against `documents.tags` (GIN on
  `(namespace_id, tags)`; EXACT/strict modes scan the namespace's documents, ≤ 10 k rows) into
  `$allowed_docs`, passed to every arm; above 8 k allowed documents the arm joins `documents` instead.
  This also removes `tags && $q` from under RLS (P-5); the trigram lookup goes through a
  `SECURITY DEFINER` function that re-checks `namespace_id`, and every N19 `EXPLAIN` runs as
  `engram_app` with the scope GUCs set.

### R2.5 Consolidation becomes two-stage so segments stay small

Cross-observation lineage (N79) made one early version the ancestor of two thirds of a namespace
(P-3). The redesign removes cross-observation text flow from the **prompt protocol**:

- **Stage 1, routing** (one call per batch of 8 facts): the model sees the batch facts and the
  candidate observations' texts and quotes, and returns only **decisions**: `attach(fact → O)`,
  `create(O_new ← {facts})`, `merge(O_a ← O_b)`, `drop_source(O, fact)`. Nothing textual from stage 1
  is persisted, so candidate text cannot flow into stored content.
- **Stage 2, writing** (one call per touched observation): the model sees O's previous text, O's live
  sources (quotes) and the newly attached facts, and returns the new text. `inputs(O, v)` = the facts
  shown, all of them O's own sources. A `merge` is written as a **root rebuild** of the survivor from
  the union of live sources (no previous texts shown). `effective_at` keeps the D9 clamp.
- Cost: ≈ 2× consolidation calls per chunk (routing + one write per touched observation), similar
  total tokens; A-4's table is re-derived with `calls_per_chunk ≈ 3.5`. Accepted: this is the price of
  a bounded, honest derivation set. H-24: only visible sources are rendered as quotes; a batch is
  re-queued at most 3 times before `failed`.

**Rejected:** lineage closure made a fixpoint under a lineage lock (H-1 option 1): correct, but keeps
the P-3 blast radius and the 27-round rebuild backlog. Per-version document bitmaps / roaring sets:
closures under "shown = tainted" are most of the namespace, so the representation does not help.
Copy-on-write evidence sets: same closure, more machinery.

**TLA+ (`Derivation.tla`, replaces `DocLifecycle.tla` and `AsOf.tla`; one shared `Deriv` operator):**
`NoDeletedDerivationServed` (after `DeleteDocument` is acked, no served fact/chunk/observation/page
version has a document fact in `Deriv`), `NoInvalidatedDerivationServed`, `RestoreExact` (hidden set
after Invalidate;Restore equals the set before), `NoOverHiding` (a version with no victim in `Deriv`
is served — the precision property that guards the blast radius), `AsOfNoLeak` (served at T ⇒
`effective_at ≤ T` with `effective_at` over the segment's inputs), `MaterializeComplete` (every
version with a victim in `Deriv` committed before Materialize ends is in `derived_hidden`), and a
configuration without the derivation lock that must FAIL. Bounds: 2 documents × 2 facts,
2 observations × 3 versions (one root rebuild), 1 page × 2 versions, non-atomic Materialize.

---

## R3. Delete durability: a strongly consistent intent log in blob storage, local commits only

**Closes** H-4, P-7, P-18, H-9.

- **No synchronous replication anywhere.** `ALTER ROLE engram_app, engram_move, engram_admin SET
  synchronous_commit = local`; N92's `remote_apply`, the per-shard synchronous standby and the
  `UNAVAILABLE` path are deleted. A-F1 holds again: commits cannot hang, the relay's 60 s gap horizon
  is sound (H-4 item 2).
- **Delete intents are durable before the shard commit.** `DeleteDocument`, `DeleteNamespace`,
  `DeleteTenant`, `Invalidate` and `Restore` first `put` an intent object
  `_control/deletes/{tenant}/{ns}/{deleted_at_rfc3339}-{operation_id}.json` (kind, subject, request
  hash, epoch) to the blob store (strongly consistent per the brief; shard-independent key so it
  follows the namespace across moves), then run the marker transaction, then ack. The put costs
  ≈ 10–50 ms. The catalog `deletion_log` table and the `deletion-log` outbox consumer are deleted;
  the shard `deletion_log` stays as the local "already applied" record (idempotency key = intent
  object name).
- **RPO promised:** retains ≤ 60 s (`archive_timeout`, unchanged); **acknowledged deletes and
  invalidations 0** — an intent exists for every ack. A delete whose intent was written but whose
  client saw a transport error may still take effect (documented; it is the request the client made).
- **Restore and failover re-apply intents:** the restored or promoted shard comes up with
  `listen_addresses` restricted, every ownership row `frozen/restore`; `engramctl restore replay`
  lists each hosted namespace's intents with `deleted_at ≥ restore_point − 10 min`, applies them in
  name order through the **admin variant of the marker transaction** (same statements, fence bypassed
  while `frozen/restore`; P-18), last state per subject wins (so `Restore` after `Invalidate` is
  honoured); only then `restore_done`. The read fence rejects `frozen/delete` and `frozen/restore`
  (only `frozen/move` is readable), and `DeleteNamespace`/`DeleteTenant` run `freeze_delete` on the
  shard **before** the ack (H-9).
- Intent objects are retained 35 days (> the 28-day backup window) and trimmed by `engramctl`.

**Rejected:** `synchronous_commit = on` with a sync standby (P-7): doubles the Postgres footprint,
still blocks commits when the standby is down, and needs a pre-commit standby probe that is itself
racy. The catalog as the intent store: it has the same HA question, and D4 keeps workers off it.

**TLA+ (`Durability.tla`, small):** `AckImpliesIntent`, `RestoreReplaysIntents` (after restore from
any point, every acked delete is applied before reads reopen), `IntentOrderLastWins` for
Invalidate/Restore pairs. The outbox spec is re-run with the commit-wait term removed (it passes
because there is no wait).

---

## R4. Move protocol: copy dirty, freeze, reconcile by set difference, switch

**Closes** H-5, H-6, H-7, H-8, H-13, H-14, H-21, H-22, H-25, A-2, P-8, P-12, H-20. Most of D5/N50/
N78/N81/N88–N91/N97/N98 is deleted.

### Why the replay machinery can go

With R1, every large table is immutable and only ever loses rows through the expunge, which is
paused for the namespace from `Plan` to `done`. Therefore for a big table the difference between
source and target after a dirty copy is **exactly the rows inserted after the copy began**, and for
the small mutable tables a full per-row content diff fits in the freeze window. No outbox replay,
no event→row mapping, no `move_applied`, no catch-up rounds, no copy barrier, no `p0`.

### Steps

1. **Plan.** As today (catalog `namespace_moves`, target row `incoming` with `move_id`, source
   `start_move` sets `move_epoch`, which pauses `Expunge` and the schedulers for the namespace).
   Record in `namespace_moves` the source's `pg_control_system().system_identifier` and
   `pg_control_checkpoint().timeline_id`; every later activity on the source re-reads them on its
   session and fails `MoveFenced` on mismatch (H-14). Record `T_copy = now()` on the source.
2. **BulkCopy** (no snapshot, no barrier; `READ COMMITTED` ranges of ≤ 100 k rows ordered by PK,
   restartable at `(table, last_key)`): `COPY (SELECT <cols> FROM t WHERE namespace_id = $1 AND <pk>
   > $k ORDER BY <pk> LIMIT n) TO STDOUT BINARY` → target session `TEMP` table → `INSERT … SELECT
   <cols> ON CONFLICT (<pk>) DO UPDATE SET <mutable cols>` under the `incoming` fence and
   `session_replication_role = replica`. `<cols>` is generated on both sides from `pg_attribute
   WHERE attgenerated = '' AND NOT attisdropped ORDER BY attname`, and Plan refuses unless the two
   column-list hashes match per table (P-8). The target has **no HNSW** for the namespace; after
   the bulk copy the mover builds the namespace's partial indexes (`CREATE INDEX CONCURRENTLY` on
   each partition). Blobs: copy every key referenced by a copied row (`ingest_ledger.body_blob_key`,
   `document_versions.body_key`, `page_versions.markdown_blob_key`, export manifests and parts);
   caches (`xcache`, `ecache`, `staging`, `consolidate/`) are not copied (N100 recomputes).
3. **Freeze.** Exclusive fence on the source, `freeze_move`, single attempt with `lock_timeout =
   35 s` (no 5 s retry storm, P-12). After it no source writer exists. Watchdog unchanged
   (`max(120 s, 60 s + 1 s / 10 k facts)`, ≤ 15 min, armed until (c)).
4. **Reconcile** (under freeze; the whole correctness argument lives here):
   - *Immutable tables* (facts, vectors, chunks, links, mentions, inputs, evidence, versions, ledger,
     proposals, applied, idempotency keys, token events): re-copy rows with `created_at ≥ T_copy −
     10 min` (every immutable table gets `created_at` and an index `(namespace_id, created_at)`;
     `memory_id`/`chunk_id` are UUIDv7 and may be used instead). The margin exceeds the maximum
     writer lifetime (`statement_timeout + idle_in_transaction_session_timeout = 60 s`), so every row
     missing from the dirty copy is in the re-copied range. Then `count(*)` on both sides must match
     and, for tables with ≤ 2 M rows of the namespace, `bit_xor(hashtextextended(pk::text, 0))` too.
   - *Mutable tables* (documents, document_versions, observations, meta tables, pages, operations,
     markers, `derived_hidden`, `namespace_stats`, `quota_counters`, `batch_jobs`, `consolidation_state`,
     export_snapshots): stream `(pk, md5(row minus updated_at))` from both sides ordered by pk,
     merge: upsert rows that differ or are missing on the target, **delete target rows absent on the
     source**. This is H-7's "set semantics" applied uniformly, with no per-event mapping.
   - *Blobs:* existence check of every referenced key on the target (parallel 64); copy the missing;
     refuse cutover while any is missing (H-8).
   - *Relay drain:* wait until every source consumer cursor ≥ the namespace's `max(seq)` (final,
     since frozen), bounded 60 s, else rollback (H-25).
   - `VerifyFK` on the target. Any residual mismatch → `Rollback`.
   Freeze window ≈ (rows written in the last 10 min) + (mutable rows of the namespace, ≤ ≈ 100 k)
   + blob checks: < 30 s for ≤ 1 M-fact namespaces, measured in M1.5.
5. **Drain** workflows from the source `operations` table and terminate them (N97 kept as is).
6. **Cutover** with a `ready` state (H-5/H-6/A-2): (b′) target `incoming → ready` (edge
   `ready_target`, `engram_move`; nothing routes to `ready`, writers and readers get retryable
   `NamespaceNotReady`); (c) source `frozen/move → moved_out` with the target hint — **point of no
   return**; (b″) target `ready → active` (edge `activate_target`, clears `move_id`); (d) catalog
   flip, retried forever. Between (c) and (b″) there is no owner at all, which is the simplest way
   to make "at most one writable owner" true; (b″) is sub-second and retried indefinitely.
   Schedulers keep `state = 'active'` as their predicate: the target is `active` only after (c).
7. **Restart** on the target (N97 unchanged). **Cleanup** after 24 h: `DROP` the namespace's partial
   indexes on the source, `engram_cleanup_namespace` batches, blob prefix; the `moved_out` row stays.
8. **Rollback** is defined at every step before (c): before Freeze → `abort_move` on the source,
   delete target rows (`incoming` fence) and the target row; after Freeze → `thaw_move`, then the
   same; after (b′) → `unready_target` (`ready → incoming`, new edge), then the same. After a
   rollback onto a shard that had a `moved_out` row, the edge `return_abort` (`incoming →
   moved_out`, restoring the hint from `namespace_moves`) keeps the permanent fence value (H-22).
   `outbox_cursors` is not touched by moves at all (H-21 disappears).

### Restore and failover reconcile against the catalog (H-13, H-14)

- A restore or promotion first **aborts or completes every open move** that names the shard, using
  `namespace_moves` as the arbiter: if the catalog shows (c) done, the restored source row becomes
  `moved_out(target, e+1)` (new admin edge `reconcile_out`, from any state); otherwise the move is
  rolled back from the target side and the restored row is thawed. Only then does the shard set
  `frozen/restore` and run R3's intent replay.
- Epochs: `restore_done` and `epoch_bump` use rule `greater`, with the new epoch = `catalog epoch +
  1` written to the catalog first; the `move_epoch > epoch` CHECK is dropped (the move is already
  aborted at that point).
- Promotion writes the new `(system_identifier, timeline_id)` into `catalog.shards` **before** the
  virtual endpoint flips; the mover compares its session's timeline with the catalog at Freeze and at
  (c), and the relay every 10 s; a zombie primary therefore cannot be frozen or cut over.
- PITR to a point before a move-in: the namespace is recovered by running an ordinary move whose
  source is a scratch instance restored from the **old source's** backup at cutover time (same
  Copy/Reconcile code, source = scratch). `CleanupMove` stays at 24 h; no 28-day retention on the
  source.

**Rejected:** dual-write during the move (a second write path to fence); keeping outbox replay with
set semantics per parent key (H-7's suggestion): correct in principle, but it keeps `replay_map.go`,
the N81 event zoo, `move_applied` on both shards and the trimmer pin, and it was the mechanism that
regressed twice.

**TLA+ (`ShardMove.tla`, rewritten):** `SingleWriter` (never two shards with `active`; `ready`
accepts nothing), `NoLossNoDup` (row set at source freeze = row set at target `active`, under a
nondeterministic partial bulk copy, bounded writer lifetime, the time-margin re-copy and the mutable
set diff), `RollbackPossibleBeforeC` (from every state before (c) a rollback action is enabled and
restores the source `active`), `NoRouteToTargetBeforeC`, `ZombieCannotCutOver` (a source session with
a stale timeline cannot execute Freeze or (c)), `RestoreReconciles` (a restored shard never ends
`active` for a namespace another shard holds at a ≥ epoch), liveness: a started move completes or
rolls back. Bounds: 2 shards + scratch, 3 rows per table, writers with lifetime ≤ 2 ticks, 1 failover.

---

## R5. Remaining blockers and majors, decided

| Finding | Decision |
|---|---|
| H-10 | `BeginSnapshot` inserts `export_snapshots` as `building`; the marker transaction expires `building` and `ready` alike; `RecordSnapshot` refuses to promote an expired row and re-checks `document_tombstones.deleted_at > snapshot_started_at`. |
| H-11 | The export's outbox-based cut (`open_gaps`) is deleted; deltas are diffs of consecutive snapshots (D12 already says so). `WriteFiles` keeps one `REPEATABLE READ` snapshot. |
| A-14 | Always emit `delta-v{n−1}-v{n}` with delete records even when the base expired; manifest carries `deleted_ids`; `engram-sync` applies deletes before serving and refuses an expired local copy. |
| H-15 | `fact_consolidation PRIMARY KEY (namespace_id, memory_id, stamped_at)`, append-only; consolidated = `EXISTS note = 'done'`; retryable = latest stamp `failed` and older than 7 days; the watermark stays below the smallest unconsolidated fact, live or marker-hidden (H-23). |
| H-16 | `document_versions.body_key/body_hash` nullable with `CHECK (body_key IS NULL OR …)`; add `append_base_version`; `LoadItem(APPEND)` materialises the base body from the ledger chain up to the nearest version with a body, under the document lock. |
| H-17 | Async index consumer deletes by indexed `(namespace_id, document_id)` query for `ids_elided` events; the expunge's row purge waits until every registered consumer cursor has passed the delete's `seq`. |
| H-20 | Sweeper writes to `operations`/outbox go through `WithNamespaceTx(write)`; `engramlint sql` checks the fence prelude before every outbox insert. |
| P-5 | Mark `similarity_op`/trigram functions as used only through `SECURITY DEFINER` wrappers; tags no longer filtered under RLS (R2.4); `EXPLAIN` assertions run as `engram_app`; M0 gate on the BM25 plan under RLS on the real image. |
| P-9 | Temporal: one cluster **per cell** (N71 reversed; cross-cell moves use the operations-table restart path), split services, `numHistoryShards = 512`, dedicated HA Postgres with pgBackRest, load-tested with the retain activity shape in M0. |
| P-10 | No generated columns remain; the partitioned-index procedure (CIC per partition, `CREATE INDEX … ON ONLY` parent, `ATTACH`) goes into §9.2 and `engramctl index`. |
| P-13/P-14/P-15/P-16 | Shard image with pgBackRest and `repo-block=y`; `postgres_exporter` per shard with XID age, dead tuples, autovacuum runtime, `pg_wal` size, fence-holder age; generated `postgres -c` list; `namespaces_count` derived in `pick_shard`. |
| A-4 | D3 throughput = `min(32/L, RPM_cap / (60 × calls_per_chunk))` with `calls_per_chunk ≈ 3.5` after R2.5; fill time re-derived (≈ 100 days online at 4 cells); `RetainBackfill` returns to the committed scope as a launch prerequisite. |
| A-5/A-6/A-8 | `OperationResult.superseded_by`, `cancel_reason`, `DELETE_TENANT` kind with tenant-scoped `GetOperation`; MCP `request_id = sha256(session ‖ jsonrpc id ‖ tool)`, minted document ids `UUIDv5(operation_id, "item/"‖index)`; `quota.Reserve` before every gateway call class, shares from trailing usage with a floor. |

---

## Simplifications (delete from the plan)

- `observation_version_lineage`, `engram_lineage_walk`, `engram_flag_lineage`,
  `engram_adjust_invalidation`, `engram_observation_inputs_after_delete`, the depth-64 frontier,
  `derived_from_deleted`, `hidden_by_invalidation`, `stale_delete` mirrors on versions, `live`
  generated columns, `VersionsFlagged` events, `deletion_log.details` lineage counters.
- `facts.retired_at/purge_after/invalidated_at/invalidation_reason/tags/tag_count`, the partial
  `WHERE live` indexes, `facts_purge_idx`, the shared partition HNSW and the N94 "band".
- `remote_apply`, synchronous standbys, the catalog `deletion_log` and `deletion-log` consumer.
- The move's copy barrier, `p0`, catch-up rounds, `move_applied` (both copies), `replay_map.go` and
  the `replay:` annotations, every N81-only event (`ProposalStored`, `ProposalDiscarded`,
  `BatchApplied`, `IdempotencyKeyStored`, `OperationTransitioned`, `BlobTombstoned`,
  `SnapshotsExpired`, `RowsPurged`), the frozen re-copy class, `move:{ns}` outbox cursors and the
  trimmer pin, `engram.replay` touch-trigger mode, the `store.Queries` "covering event" CI rule, the
  N90 two-stage verify.
- The synchronous cascade's steps 5–10 (victim selection, retire updates, evidence deletes, page
  flags, snapshot expiry inside the delete transaction); the per-1 k-facts cascade SLO.
- Export `open_gaps`; `pg_current_snapshot()` in exports.
- `FOR SHARE` re-verification on facts (immutable rows), the N84 counter `CHECK`, N48.

## Register rows to supersede

D2 row 3 (isolation: partial indexes per namespace, lock key forms), D3 (sizing, plan, throughput),
D5 (entire move protocol), D8 (delete, replace retire, invalidate), D9 (observation and page
derivation: segments), D10 (tag filter resolution, visibility sets), D12 (consolidation two-stage),
D16 (delete guarantees as stated in R2.3), N21, N23 (RPO split), N33, N41, N42, N45, N47, N48, N50,
N51, N55, N57 (order no longer matters), N61, N63 (promotion registers timeline), N78, N79, N80
(events shrink: no per-id lists needed for replay), N81, N82 (exclusive takers single 35 s attempt;
marker transactions lockless), N84, N88, N89, N90, N91, N92, N93 (add `ready`, `unready_target`,
`return_abort`, `reconcile_out` edges), N94, N95 (H-15 shape), N97 (unchanged in substance, restated),
N98, N101 (edge table regenerated from R4), N103 (fold again), N109, N110(b)(c).

Kept unchanged: D1, D4, D6 (outbox and relay; the move no longer reads it), D7 (BM25), D11, D13,
D14, N40, N43, N56, N58 (via `fact_hidden`), N59, N60, N83, N85 (evidence per version), N86, N87,
N99, N100, N104 (with H-16), N106.
