# Round 7 review: correctness of protocols and invariants (D25: N160 to N168)

Lens: the freeze-then-copy move (§5.5, §3.3.1, `sql/`), re-run-or-nothing after the commit point (N161), the
replicated-LSN wait and the asynchronous catalog standby with reconcile-from-shards (N163), the curation subject
with `invalidation_op` stamps and `Restore` (N162), delete intents and replay, the expunge, and how these
interact with shard restore (PITR) and catalog promotion. I also checked whether `ShardMove.tla`, `Derivation.tla`
and `Durability.tla` model the D25 protocol faithfully.

**Method.**
- I read N160–N163 in full, together with §5.4.1–§5.4.5, §5.5.1–§5.5.5, §3's marker and `CommitChunk` SQL, the
  ownership machine and `engram_check_ownership`, `engram_cleanup_namespace`, `engram_invalidation_twins`,
  `engram_restore_resolve` and `engram_purge_document_invalidations` in `sql/shard_schema.sql`, the
  `namespace_moves` machine in `sql/catalog_schema.sql`, and the three TLA+ specs with `EXPECT`.
- I executed the `CommitChunk`-versus-curation races (C7-4) on the scratch PG 16 server. I used my own database
  `r7c_race`, created and dropped, with a reduced schema that has the plan's statement shapes. I did not run TLC
  (another agent is running it), and I edited no other file.

**Not repeated.** PG7-1 to PG7-15 and A7-1 to A7-12 are not repeated. C7-3 extends PG7-7 with a worse variant,
as the brief allows.

---

## Major

### C7-1 (major, [regression]): the rollback acts on an unreplicated `rolled_back`, so a promoted catalog can commit a move whose source has already been thawed

- **Where.**
  - N163(3): "The one step that needs the commit to have survived is (c) … deletes, epoch bumps and lifecycle
    writes do not wait".
  - §5.5.1 step 11 ("It first CASes `cutover → rolled_back` … **after Freeze** → `thaw_move` … first").
  - §5.5.5 step 1 ("takes `cutover → rolled_back` … then the move is rolled back from the target side and the
    restored source row is thawed").
  - §5.5.1 step 8 (a″) ("A rollback and the restore or failover reconcile take the same row with
    `cutover → rolled_back`; **exactly one CAS wins**").
- **Claim.** Exactly one of commit and rollback wins, and after (a″) the move only completes.
- **Evidence.** The arbiter is a catalog CAS, and the losing side's shard action is irreversible in both
  directions:
  - (c) turns the source into `moved_out`;
  - `thaw_move` makes the source writable at epoch `e`.

  D25 guards only the first. Concrete interleaving with the restore reconcile of the target as the rollback actor
  (the operator's `engramctl move abort` works the same way):
  1. The move is at `cutover`, the source is `frozen/move` at `e`, and the target is `ready`. The target fails
     over. Its reconcile reads both rows (no `moved_out`, no `active`) and wins `cutover → rolled_back` on catalog
     primary P1.
  2. The mover's `CommitMove` gets 0 rows (`MoveFenced`) and, per step 8, "stops … and re-verifies".
  3. P1 dies before the standby replays the rollback. P2 is promoted. `reconcile --from-shards` sees the source
     still `frozen/move`, because the reconcile has not yet thawed it, and the target `ready`. No rule fires, so
     the move row stays `cutover` and the namespace stays `(S, e, frozen)`.
  4. The mover re-verifies against P2: `cutover`, source `frozen/move`, target `ready`, namespace
     `(S, e, frozen)`. All of the (a″) predicates hold, so the CAS wins: **`committed`**.
  5. The reconcile, still acting on its step-1 win, runs `thaw_move`. The source is `active` at `e` and acks
     writes.
  6. The mover waits for P2's standby. Right after a promotion P2 has none until one is rebuilt (C7-6), so this
     window lasts minutes to hours. Then (c) `frozen/move → moved_out` matches 0 rows.

  The move is `committed`, so it can never roll back, yet its source is active with writes that are not on the
  target. There are two outcomes:
  - It is stuck: a committed move whose (c) can never apply, and a source that is not frozen while the plan says
    it is.
  - A later restore or failover reconcile "completes (c), (b″) and (d) itself (`reconcile_out`, **from any
    state**)". `reconcile_out` has an edge from `active` (shard DDL line 657). This discards every write acked
    after the thaw, which is loss of acknowledged writes outside any RPO.

  The precondition is a stall of at least the catalog failover time (30 s plus the reconcile) between the
  rollback's CAS and its thaw: a paused `engramctl`, a retrying worker, or a slow source.

  The model cannot find this:
  - `RollbackA` is one atomic step (CAS, thaw, target drop).
  - `CatalogLoss` is enabled only when `cm = "committed"`, so a lost `rolled_back` is never explored.
- **Recommendation (decision level).** Make the replicated-LSN wait symmetric. Every shard action taken because of
  a catalog arbiter outcome waits until the standby has replayed that outcome: (c) after `committed`, and
  `thaw_move`/`unready_target`/`rollback_target` after `rolled_back`. Alternatively, the rollback re-reads the
  catalog row after the wait and before the thaw. Then:
  - restate N163(3) as "the two steps that act on the arbiter";
  - split `RollbackA` in `ShardMove.tla` into `AbortCAS` and `Thaw`;
  - let `CatalogLoss` revert an unreplicated `rolled_back` to `open`;
  - add the must-fail `ShardMove_ThawOnUnreplicatedAbort.cfg` (expected to violate `NoLossNoDup` or
    `OneOwner`).

### C7-2 (major, [regression]): the re-run activates the target before the intent replay, which reopens with acknowledged deletes visible (the `Durability_ReopenEarly` bug)

- **Where.** N161(2); §5.5.5 step 1, re-run bullet; `MoveService.Rerun` in §2 (line 1286); edge `reconcile_in
  ready → active`.
- **Claim.**
  - "… then `reconcile_in` (`incoming → ready → active` …), **then the intent replay**."
  - §5.4: deletes and invalidations are "RPO 0", "honoured by every read path at ack".
- **Evidence.**
  - The re-run case is a target restored to a point before activation. Every `DeleteDocument`, `Invalidate` and
    `Restore` acknowledged on the target after activation lost its marker with the WAL tail. Its only record is
    the intent object.
  - The re-run re-copies from the source, which holds the pre-freeze state, so those documents and facts are
    visible in the copy. The order activates first and replays second.
  - The order is forced by the mechanism. The admin replay is "fence bypassed while `frozen/restore`", but the
    moved-in namespace never passes through `frozen/restore` (its row goes `incoming → ready → active`), so the
    replay can only run as a live transaction on an `active` row.
  - Between `reconcile_in` and the end of the replay, Recall, `GetDocument` and exports serve acknowledged-deleted
    content. Live writers also run before replay finishes: a curation call on a subject whose lost tip is still
    unreplayed reads the stale chain tip by `ins_seq` and forks the chain the replay relies on.
  - This is the interleaving that `Durability_ReopenEarly.cfg` exists to reject ("reads reopen before replay
    finishes → `AckedDeleteSurvives`"). `ShardMove.tla`'s `Rerun` is atomic and applies `\ gone` in the same
    step, so it cannot see the gap.
- **Recommendation.**
  - Replay into the re-copied namespace before it can serve. Either add an admin edge `incoming → frozen/restore`
    for the re-run (replay, then `restore_done` to `active` with the epoch step PG7-1 asks for), or allow the
    admin replay variant on `incoming`/`ready` rows and order it before `reconcile_in ready → active`.
  - Split `Rerun` in the spec into copy, replay and activate steps, so that `NoResurrect`/`AckedDeleteSurvives`
    can see the order.

### C7-3 (major, [regression]; worse variant of PG7-7): the re-run trusts whatever the source holds now; a source restore during `cleaning` wipes it, a partial cleanup resurrects deletes, and equality on an empty set passes

- **Where.**
  - N161(2): the source is "static and intact: cleanup is impossible before a post-activation backup".
  - N161(3), §5.5.5 step 1: "a source restored to a point before a completed cleanup re-runs
    `engram_cleanup_namespace` for every `moved_out` row whose move is past `committed`".
  - §8 `TestRestore_SourceCleansMovedOut`: "whose move is `done` **or `cleaning`**".
  - `engram_cleanup_namespace` order (shard DDL line 2409).
- **Evidence.** The re-run's `VerifyFrozen` compares the new copy with the source **as it is now**. Nothing
  compares it with what was frozen and verified at the first copy. Three paths make "now" differ from "frozen":
  1. **Source restore before the gate.** `cleaning` starts at `Restart`, seconds after activation (PG7-7). Any
     restore of the source in the 24 h before the gate holds runs `restore cleanup-moved-out` for this move, and
     the test asserts exactly that. If the target is then restored to before activation (crash within the archive
     lag, or a deliberate PITR), the re-run copies an empty namespace. Per-table `count = 0` and
     `bit_xor = 0` on both sides pass, `VerifyFK` finds no orphan, and the namespace activates **empty**. That is
     silent loss of every row, while `ShardMove.tla` models the cleanup-on-restore only `IF … cleaned`, not at
     `cleaning`.
  2. **Partial cleanup (PG7-7) resurrects as well as loses.** `c_order` deletes `curation_log`, `fact_hidden`,
     `chunk_tombstones` and `document_tombstones` before `facts`, `chunks` and `documents`, and each call empties
     the first non-empty table. A source interrupted mid-cleanup therefore holds facts without their markers.
     `engram_visible_facts` reads the tombstones and `fact_hidden`, so a re-run from that source serves
     acknowledged-deleted documents and invalidated facts. This is resurrection, not only the loss PG7-7 reports.
  3. **Source PITR within the window, to a point before the freeze** (an operator restoring the source shard for
     another namespace). Shard truth makes the row `moved_out` (`reconcile_out` from `active`), but its data is
     older than the frozen set:
     - writes acknowledged between the source's restore point and the freeze are lost by a later target re-run;
     - deletes acknowledged on the source in that range resurrect, because the target's replay floor is its own
       restore point and never reaches source-era intents.

  The model excludes all three: `RestoreCore` allows faults on one shard only (`\A o : tlc[o] = 0 \/ o = s`),
  `Cleanup` is atomic, and `Rerun` reads `store[src]`, which nothing but an atomic `Cleanup` changes after the
  freeze.
- **Recommendation (decision level).**
  - Record the first `VerifyFrozen` fingerprint (per table count and PK `bit_xor`, plus `w_final`) in
    `namespace_moves`. The re-run verifies the new copy **against the record**, not against the live source.
    Any mismatch refuses the re-run and falls back to N123's recovery move from the source's backup at cutover
    time.
  - Make `restore cleanup-moved-out` apply only to moves at `done`, never `cleaning`. This also needs PG7-7's
    pre-gate state.
  - Reorder `engram_cleanup_namespace` so that markers go **last** (content first, then `fact_hidden`,
    `curation_log` and the tombstones). An interrupted cleanup then never leaves unmarked content.
  - Add a two-fault `ShardMove_SourceRestoreThenRerun.cfg` (source restore during `cleaning`, then a target
    restore before activation), expected to violate `NoLossNoDup` without the recorded fingerprint.

### C7-4 (major; N162's exactness depends on it): `CommitChunk`'s lazy re-application races `Invalidate` and `Restore`, so an acknowledged invalidation leaves a visible twin and an acknowledged restore leaves a hidden one (executed)

- **Where.**
  - §3 `CommitChunk` SQL (data model line 1153): `INSERT fact_hidden … FROM facts f JOIN curation_log c … WHERE
    c.action = 'invalidate' AND f.memory_id = ANY ($new_fact_ids)`.
  - §5.4.5 and N162(2): "A twin created later … gets its `fact_hidden` row from the lazy `curation_log`
    re-application … so a re-extraction of the same content stays invalidated and `Restore` still undoes it";
    "`Restore` is exact by construction".
- **Evidence.**
  - Locks:
    - `CommitChunk` holds the shared fence and the shared **document** try-lock, and nothing else.
    - `Invalidate` holds the subject lock and the shared fence, with no document lock and no derivation lock.
    - `Restore` adds the exclusive **derivation** lock, which `CommitChunk` never takes (§2 line 1685: shared only
      by writers of derived versions).

    No lock is shared between them, and under `READ COMMITTED` each statement sees only what had committed when
    it started.
  - Executed on `r7c_race` with two concurrent sessions, using the plan's statement shapes:
    - **Race 1.** `CommitChunk` inserts twin `u` and runs its re-application statement, which finds no
      `invalidate` row yet. `Invalidate` then hides `{f, t}` stamped `I1` (it cannot see the uncommitted `u`),
      writes `curation_log(invalidate)` and commits. `CommitChunk` commits.

      Result: `f → I1`, `t → I1`, **`u → (none)`**.

      The acknowledged invalidation leaves a live, visible twin with the same content. Nothing ever re-applies it:
      re-application runs only for `$new_fact_ids`, and the expunge's work list is the marker rows.
    - **Race 2.** `CommitChunk` inserts twin `v` and stamps it `I1` (the last action is still `invalidate`).
      `Restore` deletes `WHERE invalidation_op = 'I1'` (it does not see `v`'s uncommitted row) and writes
      `curation_log(restore)`. Then `CommitChunk` commits.

      Result: `f`, `t` and `u` visible, **`v → I1`**. The restore was acknowledged, yet one twin of the subject
      stays hidden, which violates `RestoreExact`'s second conjunct.
  - The window is from the re-application statement to `CommitChunk`'s commit (entity, link and mention inserts,
    tens of ms) per chunk. A prompt-version bump re-extracts whole namespaces for hours, so curation during a
    re-extraction will hit it.
  - Not found by the model:
    - `Derivation.tla`'s `LazyTwin` and `Invalidate` are atomic actions.
    - `Reextract` is guarded by `f ∉ hidden`, so the "read not-invalidated, commit after the invalidation"
      history is excluded by construction.
  - The §3 premise "markers are correct without [a lock] because visibility is read-time" does not hold for the
    re-application, which computes visibility at write time.
- **Recommendation.**
  - Serialise the lazy re-application with the subject's curation: `Invalidate` and `Restore` take the
    **exclusive document lock** of the subject's `document_id` (one 35 s attempt, as `DeleteDocument` does). That
    lock already waits for in-flight `CommitChunk`s, which hold it shared.
  - Equivalently, `CommitChunk` takes the subject locks of its new facts' `(document_id, content_hash)` in sorted
    order before the re-application statement. This one is costlier.
  - Split `LazyTwin` into read and commit steps in `Derivation.tla`, add a must-fail
    `Derivation_LazyTwinNoLock.cfg` (`RestoreExact` / `NoGhostInvalidatedServed`), and add
    `TestInvalidate_ConcurrentReextract`.

---

## Minor

### C7-5 (minor): the specs do not model the D25 steps whose atomicity the guarantees depend on

- `ShardMove.tla`:
  - `RollbackA` and `Rerun` (wipe, copy, verify, activate and replay) are single actions. `Rerun` requires
    `~cleaned`, and cleanup is atomic. C7-1, C7-2 and C7-3 are the gaps this leaves.
  - `CatalogLoss` sets no `cdirty`, so the modelled promotion runs **no** reconcile. That is not the designed
    system (N163(1): the reconcile is the first step of every promotion).
  - `CatalogReconcile` is enabled only after `CatalogRestore` and is atomic with respect to mover steps. The
    designed path, a promotion whose per-shard reads interleave with (c), (b″) or a thaw, is never explored. That
    is step 3 of C7-1.
  - `ShardMove_CutOnUnreplicatedCommit.cfg` fails only because the model's promotion lacks the reconcile. Its
    result does not show that the designed system needs the wait.
- `Derivation.tla`: `LazyTwin` is atomic (C7-4), and a repeated `Invalidate` is excluded (PG7-5).
- **Recommendation.** For each atomic action above, state in §7 why the atomicity is sound, or split the action.
  Model the promotion as `CatalogLoss` followed by a non-atomic `CatalogReconcile` that reads shard rows one at a
  time.

### C7-6 (minor, [regression]): after any catalog promotion every committed move stalls until a new standby is built, and the freeze is then unbounded

- **Where.** N163(3); §5.5.1 step 2 (deadline "armed … until the catalog CAS (a″) commits"); D5's "bounded
  read-only window".
- **Evidence.**
  - A promoted two-node set has no standby, so `replay_lsn ≥ commit_lsn` cannot hold until an operator rebuilds
    one.
  - The deadline timer was disarmed at the CAS, so the namespace stays `frozen` (writes refused) past
    `frozen_until_estimate` with no automatic exit.
  - "remains rollback-able by the arbiter rule" is not an exit either: that rule runs only inside a shard restore
    reconcile, and it completes a committed move rather than rolling it back.
- **Recommendation.** State the bound as "standby rebuild time". Alternatively, define the wait as "replayed by a
  standby, **or** the reconcile-from-shards of the current primary's promotion ran after this commit" (the
  commit then lives on the primary that shard truth already reconciled), and keep a deadline armed after (a″) that
  pages.

### C7-7 (minor): the re-run rule names only `incoming`/`ready`; a restored target row `none` or `moved_out` (a move back) is unhandled

N161(2) branches on the restored target row: `active` gives an ordinary restore, and `incoming` or `ready` gives a
re-run. Two other rows are possible:
- A target restored to a point **before Plan** has no row, or, if the namespace once lived there, its permanent
  `moved_out` row, which by shard truth would be read as a *source*.
- In the move-back case, both shards then hold `moved_out` and the namespace has no owner.

`ShardMove.tla`'s `Rerun` comment notes the gap ("also none … which N161(2) does not list"), but `MaxMoves = 1` in
`ShardMove_TgtRestore.cfg` hides the `moved_out` variant.

**Recommendation.** Define the rule on "not `active` at `move_epoch`" (as `ReconcileDesign` does). A `moved_out`
row whose `target_epoch` is below the current move's epoch goes through `return_move` and then the re-run.

### C7-8 (minor): namespace and tenant deletes are not excluded from an open move

- **Where.** §5.4.3 step 1 (the move check is a catalog read, then `freeze_delete` on the shard), §5.4.4
  (`TenantDelete` has no busy rule), and the ownership machine (`freeze_delete` is `move_effect none`, allowed
  while `move_id` is set; `abort_move` and `thaw_move` have no `frozen/delete` source).
- **Evidence.**
  - A `Plan` committing between `DeleteNamespace`'s step 1 and step 2 leaves a source in `frozen/delete` with
    `move_id` set (time-of-check to time-of-use). A `TenantDelete` reaching a `planned` namespace does the same.
  - `Freeze` then fails, and the rollback's `abort_move` has no edge, so the move retries forever.
  - The target's `incoming` row and its pre-warmed owner-keyed blobs (`ver/`, `ledger/`) under the target prefix
    are never purged, because `PurgeBlobs` lists only the source prefix.
  - For a namespace already `frozen/move`, `freeze_delete … WHERE state = 'active'` matches 0 rows, and the plan
    does not say whether `TenantDelete` retries, re-resolves after the cutover, or counts it as done.
- **Recommendation.**
  - `freeze_delete` requires `move_id IS NULL`; a namespace with an open move answers `NAMESPACE_BUSY` (retryable
    for `TenantDelete`, which re-resolves the shard each attempt).
  - Alternatively, a delete aborts a pre-freeze move first.

### C7-9 (minor, unverified): `Restore` of a purged fact id: §5.4.5 and the SQL disagree

§5.4.5 resolves the subject "from `curation_log` when the fact row was purged". `engram_memory_subject` reads only
`facts` and returns NULL. `engram_restore_resolve` would answer `ok`, because the `invalidate` row outlives REPLACE
purges (N145).

If the code follows the SQL, `Restore(f)` of the id the user invalidated returns `NOT_FOUND` after the 1 h retire
grace, while its REPLACE twin stays hidden under `f`'s stamp.

**Recommendation.** Give `engram_memory_subject` the `curation_log` fallback, and add
`TestRestore_AfterReplacePurge`.

### C7-10 (minor, unverified): replaying an `Invalidate` intent "verbatim, recorded `memory_ids`" may not write `curation_log`

If the admin replay inserts only `fact_hidden` rows for the recorded ids, a restore that lost the marker
transaction also loses `curation_log(invalidate)`. A later re-extraction's twin then comes up visible, so the
acknowledged invalidation does not survive re-extraction after a restore. The same holds for a replayed
`Restore`'s `curation_log(restore)`, without which a lazy twin is re-hidden.

**Recommendation.** State that the replay variant writes the same `curation_log` row with the recorded
`invalidation_op`.

### C7-11 (minor, unverified): an in-flight `PurgeBlobs` can leave pre-warmed copies of purged blobs on the target

`start_move` pauses only the *discovery* of expunge work (`purgeable_namespaces`). An expunge activity already
deleting blobs keeps running until `Drain`, after the pre-warm listed the prefix. The copies under the target
prefix are referenced by no copied row, and the target's tombstone is already `purged`, so only
`blob gc --reconcile` (if it covers them) removes them.

**Recommendation.** Pre-warm after `Drain` has terminated the expunge, or delete unreferenced target-prefix keys
in `VerifyFrozen`.

---

## Checked and found consistent

- The (a″) CAS predicate (`namespaces` still `(source, e, frozen)`) protects against a lost rollback **when the
  thaw precedes the promotion reconcile**: the reconcile derives `active` and the CAS fails. C7-1 needs the thaw
  after it.
- The freeze makes the source static, provided `PurgeBatch` and the other write activities take the fence.
  (`P-frozen` lists them, and the exclusive fence waits 35 s for 30 s writers.)
- (c) before (d), and the no-owner gap between (c) and (b″), satisfy `SingleWriter` for every interleaving I tried.
- Delete intents acknowledged on the source before the freeze are carried by the copy (markers are copied rows),
  and replay floors per target restore cover target-era intents.
- The recovery move's floor (`cutover_at − margin`) is within the 35-day intent retention for every PITR point the
  28-day backup window allows.
- `Restore` versus `Materialize` is serialised by the derivation lock, and `Restore` versus `Invalidate` of the
  same subject by the subject lock.

## Counts

| Severity | Count | Findings |
|---|---|---|
| Blocker | 0 | — |
| Major | 4 | C7-1, C7-2 and C7-3 are regressions; C7-4 predates D25 but breaks N162's exactness claim |
| Minor | 7 | C7-5 to C7-11, three of them unverified |
| Nit | 0 | — |

## Verdict

The freeze-then-copy core holds: a static source, equality verification and a catalog CAS arbiter. The D25
repairs around it do not yet keep their stated guarantees, and in each case the reason is that the spec models
the step atomically.

- **Arbiter outcomes.** Only `committed` waits for replication; `rolled_back` does not (C7-1).
- **Re-run order.** The re-run activates before it replays (C7-2).
- **Re-run source.** The re-run trusts a source that the plan's own restore step may have wiped (C7-3).
- **Lazy twins.** The lazy-twin path has no lock against curation (C7-4).

All four have local fixes:
- a symmetric replicated wait;
- replaying under `frozen/restore` before activation;
- a recorded verification fingerprint, plus cleanup on restore only at `done`;
- the exclusive document lock for `Invalidate` and `Restore`.

Each fix needs a split action in the spec. No blocker: each failure needs a fault (catalog or shard failover,
PITR) or a concurrency window, and none breaks the steady-state path.

**Not verified.**
- I ran no TLC (by instruction). The model observations come from reading `ShardMove.tla` and `Derivation.tla`.
- C7-1 assumes the rollback actor does not re-read the catalog between its CAS and the thaw; the plan's text says
  it does not.
- C7-4 was executed on a reduced schema with the plan's statement shapes, not on the full DDL with its triggers
  and RLS.
- Whether a shard **failover** (as opposed to a restore) runs `restore cleanup-moved-out` (C7-3, path 1) is
  ambiguous: §5.5.5's preamble says yes, and step 2 names `epoch_bump` for failovers.
- C7-9 to C7-11 depend on code the plan describes only in prose.
