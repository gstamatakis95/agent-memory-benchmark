## 7. Formal verification

Scope: the six TLA+ specifications under `formal/tla/` and the four Lean 4 modules under `formal/lean/Engram/`;
what TLC reported on them after the round-6 amendment (D25: N160 to N163, N166(6), N168; `ShardMove.tla` is rewritten
for freeze-then-copy; N161 and N163 also leave assumption lines in 7.1); how the Go code is kept faithful to them; what is not formalised; how it is wired into CI. Every number below is copied from a log in `formal/tla/results/` (summary
in `RESULTS.md`, which also holds the full table).

Tooling: TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap per
configuration. Lean 4 is not available in the planning environment; the Lean files and the Lake skeleton are
**not type-checked**.

### 7.1 What is modelled, and the convention

| Spec | Register | Question the model answers | Prose mechanisms this spec omits (C-21) |
|---|---|---|---|
| `Derivation.tla` | N113, N115 to N121, N133, N135, N136, N144, N145 | Can a deleted, invalidated or superseded fact reach a reader through a fact, an observation version or a page version, at any `as_of`, with the markers as the only synchronous write, and is nothing else hidden? Does a retried commit leave `current_version` naming a version that exists, does an acknowledged invalidation outlive the purge of its fact, is the owed Materialize found from the markers rather than from a signal, and does `Restore` undo exactly the set an invalidation (and its lazy twins) wrote? | LLM text (a version's text is its evidence set plus its base's); `proof_count` and retirement; the marker-set size alerts; `stale_write`/`stale_delete` flags; the pacing of Materialize and Purge; one fact per chunk; the consolidation scheduler; the attempt-unique markdown blob key and its `NOT EXISTS` deletion rule (N144) and the export `hidden_overlay` (N145(4)) are tested, not modelled |
| `ShardMove.tla` | D5, N123 to N125, N137, N160 to N163 | Freeze, copy of a static source, verify by equality, the index bit, the catalog CAS and the replicated-LSN gate on (c), cleanup, restore and failover of either shard during and after a move, a promoted or restored catalog, a target restored to before its activation (re-run) or after it, deletes on the active target, a second move: one owner, no loss, no duplicate, no resurrected delete, a started move completes. | One namespace; blob pre-warm and copy, `return_abort`; the window estimate and the copy's parallel streams and key ranges; which indexes are requested (A3); reads at `frozen/move`; drain of workflows; the relay's cursor wait; the 24 h timer (cleanup is an action); the timeline check is a guard, not a zombie primary; PITR to before a move-in (C-22) |
| `Durability.tla` | N122, N134, N146, N150 | Is every acknowledged delete or invalidation still in force when reads reopen after a restore or failover (also after the catalog was restored), and is no unacknowledged effect applied over a later acknowledged write, with overlapping requests on one fact? | Tenant and namespace intents (one subject kind per config); the catalog `deleting` edge; the 35-day retention; intent content is a set of versions, not `up_to_version` arithmetic |
| `Storage.tla` | N111 to N113, N138, N152, N166(6) | Content rows never change after insert, a model flip never exposes a row without a vector, dead index entries leave only through a rebuild and the hygiene unit is the partition (a vacuum waits until every touched graph on it is rebuilt; a rebuild is snapshot-then-publish and a vacuum start-then-collect, and purges skip the partition from the selection of the rebuild set until the vacuum has started), the index converges. | HNSW internals and recall; the index runner's lease; a partition is a set of per-namespace graphs |
| `Outbox.tla` | D6, N80 | No committed event is lost through sequence gaps; per-namespace order; the 2 x timeout watch horizon. | Kafka, consumer lag, batching |
| `Consolidation.tla` | N43, N121 | A round's decisions are applied exactly once under at-least-once activities and crashes. | The persisted-proposal `attempt` key and the all-skip stamp (C-11) are tested, not modelled; `Derivation.tla` carries the base check |

Each spec has *design* configurations (every knob at the register's value) that must end with "No error has been
found", and *must-fail* configurations in which one knob is set to the plausible simplification or to a reviewer's
variant; each must end with exactly the invariant named in its first comment line violated, and lists only that
invariant. A design that passes only because the simplification was never tried is not evidence, so the must-fail
configurations are part of the deliverable and run in CI (N141). Bounds were shrunk until every design configuration
finished in under 30 minutes (the longest: `Durability.cfg` 27.5 minutes, `Durability_Chain.cfg` 13, `ShardMove.cfg` 7; all
others under 7; the reductions are listed in 7.2.6).

**Assumptions of the D25 models** (what the specs take as given; each is a sentence a reader can falsify):

- **A1 (N146, N163), the catalog.** A catalog commit that the one asynchronous standby has replayed (`rep` in
  `ShardMove.tla`) survives a *promotion*; one that has not is lost by it (`CatalogLoss`), which is why (c) is the only
  step that waits for the flag. A catalog **restore from backup** keeps nothing: `Durability.tla` and `ShardMove.tla`
  revert the floor and `cm`/`cat` (`CatalogRestore`), and the design recovers from state outside the catalog: the
  replay reads `min(fl, blobFloor)` (the blob-store record is written before each replay and only lowered) and
  `engramctl catalog reconcile --from-shards` re-derives routing and the move row from the ownership rows before any
  shard fault (the model never interleaves a shard restore with a dirty catalog). The blob store's strong consistency
  (7.5) is what makes the floor record lossless.
- **A2 (N161), the target after the CAS.** A pre-activation target is re-copied from the static source (wipe, copy,
  verify, index, then the intent replay of the acknowledged deletes); a post-activation target is never repaired from
  it, and nothing is merged. The source rows are intact until the cleanup, which needs a full target backup that
  started after activation. The target's acknowledged deletes (`gone`) are durable (intents) and re-applied by every
  restore and by the re-run; deletes without an intent (sweeps, purges) are re-derived by their schedulers and are not
  modelled. Repeated faults hit one shard; a fault of **both** copies of a row before it is re-replicated is an RPO
  loss outside the model.
- **A3 (N160(5)), readiness at cutover.** `ShardMove.tla` has one index bit per shard: `BuildIndex` sets it under the
  freeze, `MakeReady` requires it, a ready or active shard must have it (`ServedFromIndex`). The bit stands for "every
  requested partial index is `indisvalid and indisready`" (`engram_move_indexes_valid`); which indexes are requested and
  the one retry of a failed build are covered by `TestMove_IndexesValidBeforeReady` and the index runner's tests.

### 7.2 The specifications

#### 7.2.1 `Derivation.tla`: visibility of derived data

*Model.* Facts `f1 = (d1, v1)` (with a re-extraction twin `f4`), `f2 = (d1, v2)` (the id re-used right after the
delete), `f3 = (d2, v1)`. State: tombstones `tomb[d]`, `fact_hidden` split by cause (`hidden` = invalidate, `hidRe` =
reextract), `chunk_tombstones`, observation and page versions with `root_version`, evidence rows (no foreign key to
facts, N135) and status `live | stub | absent`, cause-tagged `derived_hidden` rows, the exclusive derivation lock,
persisted proposals with `base_version`, and two writers. Actions: `Ingest`, `DeleteDocument`, `Invalidate`,
`Restore` (exclusive lock, deletes its own cause-tagged rows), `Replace`, `ChunkPurge`, `Reextract`,
`ReextractPurge`; writers `Propose` (stage 2, stored), `Pick`, `Verify` (shared lock; every rendered input and the
base re-checked; on failure the proposal is discarded), `Commit` (base compare-and-set), crash `AbortW`; expunge
`MatBegin`, per-node `MatScan`/`MatWrite` batches (each under the exclusive lock, each re-reading `fact_hidden`),
`MatEnd`, `Purge`, `DerivedPurge` (stubs). D24 (N144, N145): `cv[n]` is `current_version`, separate from the version
rows; `Commit` is the compare-and-set `cv + 1` (an update compares its base, a root rebuild the `cv` it read at
`LoadPage`) and records the attempt's `commit_key` on the row, and may be followed by a crash before the result is
recorded; `RetryCommit` then re-executes it with the same rendered result and, in the design, looks the `commit_key` up
first. `mat` is `fact_hidden.materialized_at`: `MatBegin` is enabled by an open tombstone or by an unstamped invalidation
with rows owed (the markers; an unstamped one with nothing owed is stamped at once, under the same lock rule,
`StampTrivial`), the run stamps what it read at its start, `Restore` deletes the stamp with the row. `ginv` is the ghost set
of acknowledged invalidations; `CascadeHidden` restores the old foreign key (purges drop `fact_hidden`). `Served(T)` is the SQL rule of N117: the version current at `T`, served
only if visible. A ghost copy of each version's real derivation (its inputs plus its base's) states the truth the
invariants are checked against. D25 (N162): `f1` and its re-extraction twin `f4` are one subject; `itag[f]` is
`fact_hidden.invalidation_op`; `Invalidate(f)` hides the fact and its live twins in one step under one id, `LazyTwin(f)`
is a re-extraction of an invalidated fact (the twin is born hidden, with the id), and `Restore(g)` removes every row
that carries the id of `g` (and those facts' `derived_hidden` rows) and nothing else.

*Invariants.* `NoDeletedDerivationServed`, `NoInvalidatedDerivationServed`, `RestoreExact`, `NoOverHiding`,
`AsOfNoLeak`, `MaterializeComplete`, and the round-4 additions `EvidenceOutlivesFacts`, `BaseCurrentAtCommit`,
`NoLostRebuild`, `NoVictimTextAfterPurge`, `FailClosed`, `ReextractKeepsDerivedVisible`, and the round-5 additions
`NoPhantomVersion` (`current_version` names an existing row) and `NoGhostInvalidatedServed`; `RestoreExact` now also
says that an invalidation is atomic over its subject (a live fact of the subject is hidden by it or by none of it, so
no twin survives a `Restore`); `MaterializeComplete` now
also states that a stamped invalidation is covered by `derived_hidden` rows on every node and that an unstamped one
with rows owed is never stranded (while one exists and no run is in progress, Materialize is enabled); liveness
`ExpungeCompletes`.

*Configurations.* Design: `Derivation.cfg` (1 observation x 3 versions, 2 writers, 3 proposals, REPLACE or
re-extraction once, invalidate and restore, one delete), `Derivation_Page.cfg` (adds a page x 2 versions;
REPLACE and re-extraction are left to the first), `Derivation_Live.cfg`. Must-fail, one per round-4 bug:

| Config | Knob | Violates | Finding |
|---|---|---|---|
| `_CascadeEvidence` | evidence rows die with their fact | `NoDeletedDerivationServed` | C-1: REPLACE, chunk purge, delete: the derived version survives |
| `_PageNoVerify` | `CommitPageVersion` re-verifies nothing | `NoDeletedDerivationServed` | C-2 |
| `_StaleProposal` | stored update applied without its base | `NoDeletedDerivationServed` | C-3 |
| `_ReextractHides` | derived versions hidden by cause `reextract` | `ReextractKeepsDerivedVisible` | C-6 |
| `_RestoreNoLock` | `Restore` without the exclusive lock | `RestoreExact` | C-7: scan, Restore, write leaves a permanent row |
| `_MatOnce` | Materialize reads `fact_hidden` once | `RestoreExact` | C-7: the per-batch re-read is load-bearing |
| `_PurgeDropsStub` | `DerivedPurge` deletes the row | `FailClosed` | N136: the version number is reused |
| `_NoLock`, `_EffCited`, `_TombByDocId` | no lock; `effective_at` of cited facts only; tombstone by document id | `NoDeletedDerivationServed`, `AsOfNoLeak`, `NoOverHiding` | kept from D22 |
| `_CasBeforeIdem` | the base compare-and-set runs before the `commit_key` check, and a root rebuild is blind (`RootExpected = FALSE`) | `NoPhantomVersion` | r5 C-1 (N144): a retried `page_full/v1` commit advances `current_version` past the last row |
| `_CascadeHidden` | purges also drop `fact_hidden(invalidate)` (the old foreign key) | `NoGhostInvalidatedServed` | r5 C-2 (N145): invalidate, REPLACE, chunk purge before Materialize: the hidden text is served again |
| `_MatSignalOnly` | Materialize is enabled only by the post-ack signal, which is lost | `MaterializeComplete` | r5 C-2: an unstamped invalidation is never found |
| `_RestoreOnlySelf` | `Restore(g)` un-hides only `g` | `RestoreExact` | r6 C-4 (N162): the twin hidden with it stays hidden |
| `_RestoreByVisibleTwin` | `Restore` resolves the twins by current state | `RestoreExact` | r6 C-4: a twin also hidden by re-extraction keeps its invalidate row |

*What the model changed or showed.*

1. **The cause-tagged design is the design, and the `Restore` lock is load-bearing** (C-7). The D22 conclusion
   that the lock is "a candidate for removal" came from a spec in which Materialize wrote cause-less rows; with
   cause-tagged rows and batches `_RestoreNoLock` and `_MatOnce` fail.
2. **Evidence must outlive facts** (C-1). With the old foreign key `_CascadeEvidence` reaches a served derived
   version in ten steps; REPLACE keeps the derived version visible by design, so only the evidence can tell a
   later delete about it.
3. **The derivation lock is needed because the read predicate looks only at pending tombstones** (N117). Evidence
   now survives `Purge`, so a late writer's version stays hidden while the tombstone is `pending` and is exposed
   exactly when it becomes `materialized`; `_NoLock` and `_PageNoVerify` fail at that edge.
4. **Shared-lock holders do not serialise each other, so the base check must be a compare-and-set inside the
   insert** (found by the design run). With the base verified in `Verify` only, two writers that picked proposals
   against version 1 both verify and both commit; the second update lands on a base that a first commit has
   already superseded (`BaseCurrentAtCommit` failed on the design configuration until `Commit` re-evaluated the
   base). N120(3) must be written as `UPDATE observations SET current_version = v + 1 WHERE current_version =
   base_version` (zero rows: discard), not as a read before the lock.
5. Two checks that came out of building the model, not of a counterexample: a writer whose proposal was applied by
   another writer must release the lock (the unique key refuses it), and a stub keeps `FailClosed` only if
   version numbers are never reused.
6. **Idempotency before the CAS is what N144 adds, and it matters through the blind root CAS** (r5 C-1). With
   `$expected` on every writer a CAS-first order cannot advance the pointer on a retry (the CAS fails, the commit is
   treated as lost and the run is repeated), so the phantom row of `page_full/v1` needs the D23 blind branch
   (`engram_derivation_base_cas(p_base IS NULL)`) as well as the wrong order; `_CasBeforeIdem` sets both
   (`CasFirst`, no `RootExpected`). What the order alone costs, the deletion of the committed version's markdown by
   the "lost" branch, is below the model (the blob key rule is tested). Both halves of N144 therefore stay.
7. **The owed Materialize needs a durable marker, and the invalidation row must outlive the fact** (r5 C-2).
   `_CascadeHidden` reproduces the review's nine-state counterexample; with the marker independent of the fact row the
   served text stays hidden whether or not Materialize has run, so `derived_hidden(invalidation)` rows matter for the
   export overlay and for `Restore`, not for the read predicate. `MaterializeComplete` is therefore stated as "no
   stranded work" (an unstamped invalidation with rows owed and no run in progress implies `MatBegin` is enabled) plus
   "a stamp implies coverage" (the first draft of the stamp, written without the derivation lock for invalidations
   that nothing cites, was refuted by the model: a writer that had verified before the `Invalidate` committed a version
   citing the fact after the stamp, so the stamp waits for the shared holders like any batch); `_MatSignalOnly` fails the first half in eight steps (a committed version, Invalidate, LoseSignal).
8. **`Restore` must be exact by tag** (r6 C-4, N162). With the invalidation id on every row it wrote, the lazy twin
   included, `Restore` needs no look-up by current hidden state; both variants that resolve the twin set at `Restore`
   time (`_RestoreOnlySelf`, `_RestoreByVisibleTwin`) fail on the first twin.
9. **`NoPhantomVersion` says "an existing row", not "a non-stub row".** `DerivedPurge` legitimately turns the current
   version into a stub (the document it was derived from was deleted); N144's wording "existing, non-stub row of the
   same root" cannot hold for that state and should read "existing row" (the stub is hidden by `FailClosed`).

#### 7.2.2 `ShardMove.tla`: freeze, copy, verify, index, cut over, restore

*Model.* Source `s1` and target `s2` (a second move swaps the roles). Per shard: insert-only rows (each with an
`ins_seq` from the shard's sequence `sq[s]`), mutable keys (swept by an active source), a counter for the expiring class
and **one index bit** `idx[s]` (A3). The catalog carries the move row `cm`, the namespace `(shard, epoch)` and the flag
`rep` (the standby has replayed the commit). Mover: `Plan`; `Freeze` (the source is static from here; also the one
`engram_seq_advance`); `CopyStep(r)`, `CopyEx` (only in `frozen`: a set difference, no floor, catch-up or merge);
`CopyFault(r)` (a dropped row or a stale key); `VerifyFrozen` (equal row and key sets, `ex[Tgt] <= ex[Src]`; a mismatch
re-copies once, a second one rolls back); `BuildIndex`; `MakeReady` (b', needs the index); `CommitCAS` (a'', retried when
the commit is lost); `Replicated`; `Cut` (c, needs `committed` and `rep`); `Activate` (b''); `CatFlip` (d); `Cleanup`
(needs a full target backup that started after activation); `Rollback`, `WindowTimeout` (rollback from `frozen`,
`copied` or `built`); `Rerun`. Environment: writers before the freeze and after activation (never under it),
`SrcDelete`, `SweepEx`, `TgtDelete(r)` (a legitimate delete on the active target; the row joins `gone`), `Backup`,
`Restore` and `TgtRestore` (to the last backup, which may predate activation, or lossless failover; the acknowledged
deletes in `gone` are re-applied, as the intent replay does), `CatalogLoss` (the standby is promoted and lacks an
unreplicated commit), `CatalogRestore` (reverts `cm` and `cat`; `CatalogReconcile` re-derives them from the ownership
rows before the next shard fault), `Reconcile_` (first re-derives `cm` from the ownership rows; `open`: abort CAS and roll
back; `committed`: complete (c), (b''), (d), or mark a re-run when the target's restored row is not `active` at the move's
epoch; no move: owner bump), `RestoreDone`.

*Invariants.* `SingleWriter`, `NoLossNoDup` (the target is activated with exactly the rows frozen at the source; after the
cleanup an active target holds every frozen row it did not delete; once settled every acknowledged row that is not an
accepted RPO loss and not deleted on the target is on an active owner), `RollbackPossibleBeforeC`,
`NoWriteToTargetBeforeC`, `NoRouteToTargetBeforeC`, `ZombieCannotCutOver`, `RestoreReconciles`, `OneOwner`,
`CatalogNamesOwnerAfterDone`, `CleanupSafe`, `CopiedBelowTargetSeq`, and new: `NoResurrect` (a row deleted on an active
target is never again in an active store), `ServedFromIndex` (`ready` or `active` implies the index bit),
`SourceStaticUnderFreeze`. Liveness: `MoveTerminates` and `FrozenBounded` (a `frozen` move reaches the commit point or
rolls back); `MoveTerminatesActive` is gone: writers cannot run under the freeze and the only deterministic rollbacks left
are the window deadline and a second `VerifyFrozen` mismatch.

*Configurations.* Design: `ShardMove.cfg` (2 rows, 1 client, writer lifetime 1, 2 ticks, freeze window 1 tick, 1 restore or
failover of either shard, 1 extra backup, 1 copy fault, 1 catalog loss or restore; 28.2 M distinct states, 7 min 18 s),
`_Live` (2 clients, both liveness properties; 2.3 M, 4 min 35 s), `_ActiveWriters` (2 clients, writers before and after, no
fault; 23 k), `_Twice` (one row, two moves, the second from the first target after its cleanup; 28 k) and `_TgtRestore`
(one row, one restore or promotion of the target after the commit point, one catalog restore, 2 backups; 1.6 M, 28 s).
Must-fail, kept: `_UnfencedSteps`, `_NoReady`, `_RestoreNoReconcile`, `_NoVerify` (a copy fault nothing detects),
`_StampAfterCut`, `_SweepNotPaused` (now `SourceStaticUnderFreeze`), `_CleanupNoBackup`, `_NoSeqAdvance`,
`_CatalogLossNoShardTruth`; new:

| Config | Knob | Violates | Finding |
|---|---|---|---|
| `_UnionRepair` | a restored, serving target is repaired as `store[Tgt] u store[Src]` (the D24 `ReconcileIn`) | `NoResurrect` | N161: the target's own deletes look like missing rows; a deleted row comes back (17 states) |
| `_RerunMerges` | a target restored to before activation is repaired by a union, not wipe-and-copy | `NoResurrect` | N161(2): the same after a restore to the pre-activation backup (19 states) |
| `_CleanupBackupBeforeActivate` | the cleanup gate accepts a backup that started before activation | `NoLossNoDup` | N161(1): a backup taken during the copy is not a copy; a restore to it after the cleanup leaves a partial target |
| `_CopyBeforeFreeze` | the copy runs from the unfrozen source, with no catch-up | `NoLossNoDup` | N160: rows committed after the copy are missing at cutover |
| `_ReadyBeforeIndex` | `MakeReady` does not wait for the index | `ServedFromIndex` | N160(5): the first recall after activation would not use the HNSW |
| `_CutOnUnreplicatedCommit` | (c) runs before the standby has the commit | `NoLossNoDup` | N163(3): the promotion loses the commit, the source is restored, the reconcile rolls back after the cut and wipes the target's copy |

*What the model showed.*

1. **Freeze-then-copy removes the dirty-copy failures, not the restore ones.** The seven D24 configurations about a
   re-copy key, a merge, a verify order, a margin, a timeline gate or the sequence ring are gone with their machinery;
   the move completes (`MoveTerminates`, `FrozenBounded`). What remains is what a restored shard does to a committed move.
2. **The catalog CAS before (c) and the row-state guard on every mover step stay load-bearing** (`_StampAfterCut`,
   `_UnfencedSteps`, `_RestoreNoReconcile`). `_StampAfterCut` fails only with `ShardTruth = FALSE`; with the re-derivation
   of `cm` from the shards the stamp order is repaired too.
3. **(c) is the one step that needs the commit to have survived** (`_CutOnUnreplicatedCommit`). After (c) the only
   evidence of the move may be the source's `moved_out` row; a source restored to before (c) with a catalog that lost the
   commit rolls back and wipes the target's complete copy. With the gate a lost commit is one no shard has acted on.
   Accounting: rows a source restore loses are an RPO loss unless the commit was replicated or (c) has run; a catalog
   *restore* withdraws that promise (`rep := FALSE`), since it restores from backup, not from the standby.
4. **A catalog restore must be repaired from the shards before any shard fault** (`_CatalogLossNoShardTruth`): after (c)
   the reconcile finds `committed` in the source's `moved_out` or the target's `active` row; without it the shard
   reconcile trusts `open` and rolls back a cut move.
5. **N161(2) is short by two cases.** A target whose restored row is not `active` at the move's epoch needs the re-run:
   `incoming`, `ready`, and also `none`, a restore point that predates `Plan` (a small namespace moves within the 60 s
   archive window); without it the reconcile activates an empty target. The re-run may also find the source still
   `frozen` (the target was restored between (a'') and (c)); the mover's (c) needs a `ready` target and would wait
   forever (`MoveTerminates` fails if `Rerun` demands a `moved_out` source), so `Rerun` takes (c) itself.
6. **Nothing is merged** (`_UnionRepair`, `_RerunMerges`): a target that has served writes has legitimately deleted
   rows. Wipe, copy and replay of the acknowledged deletes is exact.
7. **The cleanup gate is "a full backup that started after activation", nothing more** (`_CleanupNoBackup`,
   `_CleanupBackupBeforeActivate`); the time, content and timeline conditions of D24 are gone and the property holds,
   because such a backup contains the complete copy. One advance under the freeze is enough (`_NoSeqAdvance`, `_Twice`).
8. **A source that sweeps under the freeze is not static** (`_SweepNotPaused`), a copy from an unfrozen source without a
   catch-up loses rows (`_CopyBeforeFreeze`), and `VerifyFrozen` is load-bearing against a copy fault (`_NoVerify`).

#### 7.2.3 `Durability.tla`: acknowledged deletes survive restore and failover

*Model.* Operations come from a fixed shape: on a document, delete, a duplicate delete and a retain (which revives
the document); on a fact, invalidate, restore, invalidate. The write path is `Commit` (the marker, a local commit
that a restore may lose) then `PutIntent` (after the commit; a duplicate that finds the committed marker re-puts
that marker's own intent) then `Ack` (after re-reading the marker); failures: `Abort`, `CrashBeforePut`,
`LostAck`. The intent records the marker's effect (the set of versions a delete covers), `prev_operation_id` and the
epoch. `RestoreTo(p)` keeps commits up to `p`, lowers the replay floor (held in the catalog, outside the restorable
state, never raised) and the blob-store floor record (written before the replay, only lowered), bumps the epoch;
`Replay` applies in-window intents verbatim in chain order, the window starting at `min(fl, blobFloor)`, and stamps
each entry with the intent's **recorded** epoch; an intent whose recorded epoch is older than an applied entry of its
subject is skipped and counts as settled; `Reopen` waits for replay. Hosts' clocks may skew the recorded `deleted_at`
by one tick. D24: `CatalogRestore` puts the catalog's floor back to its initial value (the worst case, since the
window only narrows as the floor rises); requests on one fact may overlap, and a marker transaction is `TxnBegin`
(reads the subject's previous entry, the `prev_operation_id`, under the subject lock) then `Commit`; the lock is held
until the transaction's own intent is put, and under it the transaction first puts the intent of the subject's latest
entry if that is missing (`HelpPrev`).

*Invariants.* `AckImpliesIntent`, `AckedDeleteSurvives`, `IntentOrderLastWins` (the state after a restore is that of
the operation that committed last, whatever the issue order of overlapping requests), `NoUnackedEffectOnLaterAck`.

*Configurations.* Design: `Durability.cfg` (document subject, `MaxT = 7`, 2 restores, 1 catalog restore, margin 1,
latency 1), `Durability_Chain.cfg` (fact subject, overlapping requests with the subject lock, clocks skewed by one
tick, `MaxT = 4`, 2 restores, the lock held through the intent put and the help step). Must-fail: `_IntentBeforeCommit` (C-5), `_RaiseOnReopen` (C-4), `_ClockOrder`
(C-16), `_DupNoReput`, `_AckNoRecheck`, `_NoEpochGuard`, from D22 `_RetargetRestore`, `_ReopenEarly`,
`_AckBeforeIntent`, `_NarrowWindow`, `_UnorderedReplay`, and the round-5 ones: `_CatalogLossNoBlobFloor`
(`AckedDeleteSurvives`: the catalog is restored, its floor is back at its highest value and the replay reads it
alone), `_ReplayStampsCurrentEpoch` (`IntentOrderLastWins`: a replayed marker takes the bumped epoch and the guard
skips the second intent of the subject), `_NoSubjectLock` (`IntentOrderLastWins`: two overlapping requests read the
same `prev_operation_id`, the chain forks and replay applies the children in an unspecified order) and
`_NoHelpPrev` (`IntentOrderLastWins`: the lock without the help step, see item 9; not in the register).

*What the model changed or showed.*

1. **C-4 and C-5 reproduced and fixed as the register says**: the floor lives in the catalog and is only lowered
   (`_RaiseOnReopen`, `_RetargetRestore`); the intent follows the commit (`_IntentBeforeCommit`: an orphan intent
   or a concurrent duplicate is replayed over a later acknowledged write); a retry after a crash between commit
   and put must re-put the marker's intent (`_DupNoReput`, the register's second point beyond the advice;
   Go twin `TestIntent_DuplicateAttempt`).
2. **The ack must re-read the marker after the put** (`_AckNoRecheck`, a flaw of N122 as written). A restore can
   land between the commit and the put; replay then runs, the shard reopens, the late put succeeds and the delete
   is acknowledged although no replay will ever see its intent. One indexed read of the marker after the put (ack
   only if it is still committed; else error, the client retries) closes it; the register and §5.4.1 step 4 gain
   that sentence.
3. **Replay must compare epochs, and `Invalidate`/`Restore` must always write their marker** (`_NoEpochGuard`,
   found by the design run). An unacknowledged `Restore` whose commit a restore lost can have its intent put after
   the reopen. A later acknowledged `Invalidate` that finds the fact already hidden returns success with no marker
   of its own (A-10's no-op); a second restore then replays the older `Restore` intent over it. The design:
   intents carry the namespace epoch, replay skips an intent whose epoch is older than an applied entry of its
   subject, and a double `Invalidate` (or `Restore`) still inserts its `deletion_log` row and puts its own intent
   (`fact_hidden` stays a no-op). Duplicate document deletes keep the D23 rule (re-put the observed marker's
   intent), since their effect is a fixed set of versions.
4. **The margin must exceed the longest commit latency plus the clock skew** (`_NarrowWindow`); chain order
   removes the need for synchronised clocks (`_ClockOrder`, `_UnorderedReplay`).
5. **The replay floor must not live only in the catalog** (r5 C-3, N146). `_CatalogLossNoBlobFloor` is
   `_RaiseOnReopen` reached through the catalog: after a catalog restore the next restore's window starts too late and
   the deletes the first replay re-applied are lost. The blob-store record, written before each replay and only
   lowered, makes the window `min(fl, blobFloor)`.
6. **A skipped intent must count as settled** (a fix of the D23 model, found while adding `_ReplayStampsCurrentEpoch`).
   In D23 the epoch guard disabled `Replay` for a skipped intent but the intent stayed in `Pending`, so `Reopen`
   waited for it forever and every safety invariant was vacuous on that branch. Replay must record a skipped intent
   as handled (§9.3 step 5 says "skips"; the implementation needs a stored outcome per intent or a count of the
   skipped ones, or reads never reopen).
7. **Recorded epochs on both sides of the guard** (r5 C-7, N150). If replay stamps the bumped epoch, the first replayed
   intent outranks the second one of its subject. `_ReplayStampsCurrentEpoch` fails in the shortest trace of three
   operations (Invalidate, Restore, restore to before the Invalidate).
8. **Overlapping requests need the subject lock, and the order is the commit order** (r5 C-8, N150). Without the lock
   `_NoSubjectLock` forks the chain; `IntentOrderLastWins` is now stated against the operation that committed last,
   because with overlapping requests the issue order is not the serialisation order.
9. **N150's lock is not enough: the chain has holes, and the lock must cover the intent put** (a design flaw the model
   exposed; `_NoHelpPrev`). Two things break the chain once requests on a fact overlap. (a) The advisory lock was a
   transaction-level lock: a successor could commit and put its intent while the predecessor's put was still in
   flight, and a restore in that window replays the successor first (the first design run of `Durability_Chain`
   failed this way in fifteen steps). The lock has to be a session-level lock held until the intent is put. (b) A
   marker whose writer crashed between commit and put (`CrashBeforePut`) stays in `deletion_log` with no intent; its
   successor records it as `prev_operation_id`, the replay finds that intent absent, treats the link as satisfied and
   replays the successor before older intents (Invalidate, a crashed Invalidate, Restore: the replay ends hidden
   although the last acknowledged request was the Restore). The fix modelled is a help step: under the lock the next
   transaction puts the missing intent of the subject's latest entry (put-if-absent, one more blob request per
   marker); the alternative is a per-subject sequence number assigned under the lock and replay in sequence order,
   which tolerates gaps. N150 and §5.4.5 need one of the two.

#### 7.2.4 `Storage.tla`: insert-only content, vector generations, index hygiene

Content rows are never rewritten and are purged only under a marker; a model flip waits for the next generation's
vectors; the index follows the vector table. New in D23 (N138): the purge counts the dead entries it leaves
(`pc = purged_since_build`), a rebuild at the threshold is the only thing that removes them
(`vacuum_index_cleanup = off`), and `AutoRepair` models autovacuum cleaning the graph in place. Invariants
`ContentImmutable`, `PurgeNeedsMarker`, `VectorGenerationConsistent`, `RebuildBeforeRepair`, `DeadCounted`,
liveness `IndexConvergence`. Must-fail: `Storage_Update`, `_FlipEarly`, `_PurgeUnmarked`, and `_AutoRepair`
(`RebuildBeforeRepair`).

D24 (N152): the hygiene unit is the **partition**. A partition holds the graphs of several namespaces (`Ns`, here one
with threshold 1 and one with threshold 2), each with its own purge counter; the hygiene rebuilds every touched graph
once one is due, and the partition `Vacuum`, which reads every graph, runs only when each touched graph is rebuilt.
`Storage_PerIndexHygiene` rebuilds only the due graph and vacuums afterwards, which repairs the other graph's dead
entries in place (`RebuildBeforeRepair`, fifteen states). `IndexConvergence`: every due graph is rebuilt and the
current generation's vectors are indexed; a graph below its threshold may keep a few dead entries (`DeadCounted`).

New in D25 (N166(6), P-8): a rebuild and a vacuum are not atomic. `RebuildStart(n)` takes the live vectors of the
graph and `RebuildPublish(n)` replaces the graph by exactly that snapshot and resets `pc[n]`, so a row purged in
between is a dead entry of the published graph that nothing counts; `VacuumStart` records the dead entries at that
moment and `VacuumCollect` removes exactly those (a repair, if there were any). The design takes the partition key
`lk` from the selection of the rebuild set until `VacuumStart`, and purge batches (`Purge`, `ExpungeOld`) skip the
partition while it is held. `Storage_PurgeDuringRebuildSet` (purges ignore the key) fails `RebuildBeforeRepair` in
thirteen states (`RebuildStart`, `Purge`, `RebuildPublish`, `VacuumStart`, `VacuumCollect`): in the design the key makes
the dead set empty at `VacuumStart`, and a purge after it is counted by `pc` and waits for the next round. `Storage.cfg`
(2 generations, 139 k distinct states, 47 s) and `Storage_Gens.cfg` (3 generations, 55 k) pass.

#### 7.2.5 `Outbox.tla` and `Consolidation.tla`

Unchanged by D23 and re-run. `Outbox.tla` (D6): writers draw `seq` in their transaction and commit out of order; a
relay with a persisted cursor and a gap watchlist declares a missing `seq` aborted after `Watch` ticks
(invariants `NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered`, `IdempotentConsumerState`, liveness
`NoLossLive`); `Outbox_NoWatch` and `Outbox_Watch1x` (horizon 1 x timeout) violate `NoLossSafety`.
`Consolidation.tla` (D12, N43, N121): stage 1's decisions are persisted write-once under `batch_key`, each op's
effect and its `op_key` row are one transaction, the worker may crash anywhere; invariants `ExactlyOnceEffect`,
`ObservationHasSources`, liveness `RoundTerminates`; `_VolatileProposal` and `_NonAtomicKey` violate
`ExactlyOnceEffect`. The proposal lifecycle of C-11 (`(batch_key, attempt)` keys, no `DELETE`, batch `applied`
state) is below the model's abstraction and is covered by `TestConsolidation_PersistedProposal` and `TestConsolidation_AtomicKey` (8.4.6).

#### 7.2.6 Boundaries of the results

1. **Abstractions that matter.** One namespace, one fact per chunk, two to three writers, version caps of one to
   three; restores are "revert to the last backup" or lossless failover, not PITR; the mutable and expiring classes are
   sets and a counter; the promotion and the catalog restore are one event each, not a sequence of replica states;
   repeated restores hit one shard (assumption A2); the freeze window is a tick count, not a clock. A bound bump is a
   spec change reviewed as such.
2. **`Derivation_Page.cfg` leaves REPLACE and re-extraction out** (the product with a page ran past 30 minutes);
   `Derivation.cfg` covers both on observations only, and the page path is the same `Commit`. The retry of N144 is
   in the design configurations that have no REPLACE (`Derivation.cfg`, `_Retire`, `_Page`, `_Live`); the must-fail
   `_CasBeforeIdem` uses one observation and one proposal. The twin rule of N162 is exercised where a re-extraction is
   possible (`_Retire` and the two `Restore` must-fail configurations).
3. **Liveness runs forbid writer crashes** (`AllowAbortW = FALSE`): with crashes a writer can starve Materialize
   by re-taking the shared lock forever, which the 35 s single attempt and the retry make a latency issue, not a
   safety one.
4. **Bounds reduced to keep every design configuration under 30 minutes** (the figures are in `results/RESULTS.md`).
   D24: REPLACE, re-extraction and the two purges moved out of `Derivation.cfg` into `Derivation_Retire.cfg`;
   `Durability_Chain.cfg` has no catalog restore. **D25: `Storage.cfg` has two embedding generations** (the three
   generation product with snapshot-then-publish and start-then-collect ran 30 minutes to 2.8 M distinct states at
   depth 22 with the queue still growing and was stopped), and the third generation is covered by `Storage_Gens.cfg`
   with one row in each namespace. `ShardMove.cfg` needed no reduction (7 min 18 s: with a static source a move has
   far fewer interleavings than the D24 dirty copy), `_Twice` keeps one row, and `Durability.cfg` keeps its D23
   bounds (27.5 minutes, the closest to the cap).

### 7.3 Lean 4 theorems

Files under `formal/lean/Engram/`, Lean 4 core only (no Mathlib), **not type-checked**. `lakefile.lean`,
`lean-toolchain` and `SORRY_BASELINE` (value 3) form the Lake skeleton (N142). `TagMatch.lean`: the six tag modes
of D10, decidability, `anyStrict_imp_any`, `allStrict_imp_all`, `exact_imp_allStrict`, monotonicity of the strict
modes, non-monotonicity of ANY and ALL (`decide`). `RRF.lean`: the fused score is invariant under arm reordering,
per-arm monotonicity, `contribNat_antitone` (needs `0 < k`, which is why D10 fixes k = 60); `score_mono` and
`score_le_bound` are `sorry`. `Packer.lean`: `pack_total_le`, `pack_sublist`, `keep_count`, `keep_append`,
`keep_skip_oversize`; `kept_fits` is `sorry`. `TemporalWindow.lean`: overlap symmetry, containment order,
`distanceTo` non-negativity, monotonicity, 1-Lipschitz. Three `sorry` in total, the checked-in baseline.

### 7.4 Conformance: how the Go code stays faithful

A spec that is not tied to a test is documentation. Three mechanisms for every spec.

**(a) Model tests.** Each spec has an in-memory Go model of its state and actions and a `pgregory.net/rapid` state
machine that runs generated action sequences against the model and against the real implementation on a
`testcontainers` Postgres, checking the spec's invariants after every step with the TLC constants as generator
bounds. Invariant code is written once in `internal/formal/<spec>/invariants.go`. Section 8.4.6 lists the Go twin
of every must-fail configuration (the fix disabled by a `faultinject` knob reproduces the flaw; with the fix on it
is absent).

| Spec | Go package and twin tests |
|---|---|
| `Derivation` | `internal/recall`, `internal/expunge`, `internal/consolidate`, `internal/pages`: `TestVisibility_AllSurfaces`, `TestVisibility_SegmentHiding`, `TestInvalidate_RestoreExact`, `TestAsOf_*`, `TestExpunge_DerivationLock`, `TestDelete_ReuseDocumentID`, `TestVisibility_AllSurfaces` (C-1), `TestDerivation_CommitRule` (C-2), `TestApply_BaseVersionCAS` (C-3), `TestReextract_Rebuilds` (C-6), `TestMaterialize_StampUnderLock` (C-7), `TestPageRefresh_CommitRetry` (r5 C-1), `TestInvalidate_SurvivesChunkPurge`, `TestInvalidate_RestoreUndoesTwinSet`, `TestInvalidate_LazyTwinRestored` (N162), `TestExpunge_SweeperFindsUnmaterialized`, `TestExport_OverlayBeforeMaterialize` (r5 C-2) |
| `ShardMove` | `internal/move`: `TestMove_RollbackEveryStep`, `TestMove_ReadyState`, `TestMove_FrozenCopy`, `TestMove_VerifyFrozenCatchesFault`, `TestMove_WindowDeadlineRollback`, `TestMove_IndexesValidBeforeReady` (N160), `TestMove_CatalogCAS`, `TestMove_ZombieFenced`, `TestRestore_OpenMoves`, `TestMove_TwiceAndExport`, `TestMove_CleanupGateAtEntry`, `TestMove_TargetRestoredAfterMoveBackup`, `TestMove_TargetRestoredAfterActivation`, `TestRestore_SourceCleansMovedOut` (N161), `TestCatalog_PromotionRunsReconcile`, `TestMove_CutWaitsForReplicatedCommit`, `TestCatalog_ReconcileDerivesRouting` (N163); mover killed at every persisted state |
| `Durability` | `internal/intent`, `cmd/engramctl restore replay`: `TestIntent_AckImpliesIntent`, `TestIntent_DuplicateAttempt` (re-put of the committed marker's intent), `TestIntent_AckRereadsMarker`, `TestIntent_EpochGuard`, `TestRestore_ReplaysIntents`, `TestFailover_ReplaysIntents`, a double-restore case, `TestReplay_FloorFromBlob` (r5 C-3), `TestIntent_ReplayTwoOfSameSubject`, `TestIntent_ConcurrentCurationOneChain` (r5 C-7, C-8) |
| `Storage` | `internal/store`, `internal/index`: `TestContent_InsertOnly`, `TestVectors_ModelGeneration`, `TestHNSW_PerNamespace`, `TestIndex_Hygiene`, `TestIndex_Hygiene` (N152: a partition with three namespaces purged at 0.2 %, 1 % and 0 %; N166(6): a neighbour touched once is not rebuilt and a purge during the rebuild set is skipped) |
| `Outbox` | `internal/outbox`: relay with writers delayed by `pg_sleep`, `TestOutbox_Watch1x` |
| `Consolidation` | `internal/consolidate`: `TestConsolidation_PersistedProposal`, `_AtomicKey`, `_TwoStage` |
| Lean modules | `internal/recall` table and `rapid` tests (tags, fusion, packing, temporal window) |

**(b) Trace validation.** Store, relay, expunge, intent and move code paths emit JSON-line decision logs in tests
(`internal/formal/trace.Logger`, written after the transaction commits). `engramctl formal trace-to-tla <spec>
<log.jsonl>` emits a module that `EXTENDS` the spec with the trace as a constant sequence; TLC checks that the logged
behaviour is a behaviour of the spec and that every invariant held along it. M0.7 delivers the converter.

**(c) Refinement mapping.**

| Spec variable | Go / Postgres state |
|---|---|
| `Derivation.tomb[d]`, `ms[d]` | `document_tombstones(up_to_version, expunge_state)` |
| `hidden`, `hidRe`, `ctomb`, `itag` | `fact_hidden(cause = 'invalidate' / 'reextract')`, `fact_hidden.invalidation_op`; `chunk_tombstones` |
| `born`, `gone` | `facts` rows; rows removed by the purges |
| `vers[n][v]` | `observation_versions` / `page_versions` (`root_version`, stub flag); `observation_inputs`, `page_version_inputs` (no FK to `facts`); `effective_at` |
| `dh` | `derived_hidden(root_version, from_version, cause_kind, cause_id)` |
| `cv[n]`, `ck` (row), `mat` | `pages.current_version` / `observations.current_version`; `page_versions.commit_key` / `observation_versions.commit_key`; `fact_hidden.materialized_at` |
| `props` | `consolidation_proposals` with `base_version` |
| `lockX`, writer `verified` | derivation advisory lock, exclusive and shared |
| `ShardMove.cat`, `cm`, `mp` | catalog `namespaces(shard_id, epoch)`; `namespace_moves.state`; the move workflow |
| `own[s]` | `namespace_ownership(state, epoch)` on shard `s` |
| `store`, `mk`, `ex`, `gone` | insert-only tables (`ins_seq`); mutable-class tables; expiring-class tables; rows deleted on the active target (intents, expunge) |
| `idx[s]`, `rep`, `cdn`/`rc` | `engram_move_indexes_valid(ns)` (`indisvalid and indisready` of the requested indexes); the standby's `replay_lsn >=` the CAS's commit LSN; the copy's per-table progress and the one re-copy of `VerifyFrozen` |
| `sq[s]` | `engram_ins_seq` of shard `s`, raised once by `engram_seq_advance(W_final)` under the freeze |
| `tlc[s]` | the shard's timeline id (a restore or promotion starts a new one) |
| `tl`, `mtl`, `bak` | `catalog.shards.timeline_id`; the mover session's timeline; pgBackRest backup (`target_backup_started_at`) |
| `Durability.intents`, `dbq`, `fl`, `bf`, `ep` | `_control/deletes/...` objects; the shard `deletion_log` (its `epoch` is the recorded one); `catalog.shards.replay_floor`; the blob-store floor record `_control/replay_floor`; the namespace epoch |
| `Storage.vec`, `cur`, `idx`, `pc[n]`, `rebuilt`, `lk`, `rsnap`, `dsn` | `fact_vectors`; the namespace's current model; the per-namespace partial HNSW; `vector_indexes.purged_since_build`; the partition hygiene round's list of rebuilt graphs; the partition advisory key; the rebuild's snapshot; the dead entries at `VACUUM` start |
| `Outbox.*`, `Consolidation.*` | visible `outbox` rows, `outbox_cursors`; `consolidation_batches`, `consolidation_applied` |

### 7.5 What is not formalised, and why

- **LLM outputs** (extraction, routing, writing, reflect): non-deterministic external input, modelled as
  unconstrained choices. Quality is measured in section 8, not proved.
- **Ranking quality and HNSW recall**; **index engines behind `index.Searcher`** (candidates are filtered by the
  same N116 predicate).
- **Temporal's guarantees**, **Postgres semantics** (snapshot isolation, advisory try-lock, `synchronous_commit =
  local`), **the blob store's strong consistency**: assumed as documented.
- **Zombie primaries serving client writes**, instance-level failover and the relay's 10 s timeline check:
  outside `ShardMove.tla`, which has the session timeline only as a guard (`ZombieCannotCutOver`); D25 removed the
  `_NoTimelineCheck` experiment with the cleanup-time timeline check, so there is no must-fail configuration for it.
- **Multi-namespace interference in a move**, **reads during a move**, **blob copy**, **`return_abort`**:
  tested (`TestIso_Move_Epoch`, misroute suites), not modelled.
- **Which indexes a move requests and when a build counts as failed (N160(5))**, **the attempt-unique markdown blob key
  (N144)**, **the export overlay (N145)**: below the abstraction of the specs (the first is one bit);
  `TestMove_IndexesValidBeforeReady`, `TestPageRefresh_CommitRetry` and `TestExport_OverlayBeforeMaterialize` carry them.
- **Export snapshot expiry (N126), page refresh policy, Reflect, quotas, config inheritance, JWT/authz, Kafka
  beyond per-partition order**: no interleaving worth a model.

### 7.6 CI integration

- **Nightly `formal-tlc`** (`.github/workflows/formal-nightly.yml`): pins `tla2tools.jar` by SHA-256, runs every
  `*.cfg` under `formal/tla/` with `-workers auto` and a 30-minute cap per configuration. A design configuration
  must end with "No error has been found"; a must-fail configuration must end with "Invariant ... is violated" on
  the invariant named in its first comment line (or, for a liveness property, "Temporal properties were
  violated"); anything else fails the job. `formal/tla/EXPECT` lists the expected outcome of every configuration;
  no configuration is an experiment any more (D25 deleted `ShardMove_ZeroMargin` and `_NoTimelineCheck`). A **timeout is
  neither a failure nor a pass**: it is reported INCOMPLETE with the states and depth reached. A bound bump that
  pushes a design configuration over 30 minutes is a spec change reviewed as such.
- **PR job `formal-quick`**: parses every spec with SANY and runs every must-fail configuration and every design
  configuration that finishes in under two minutes; the others (`Derivation.cfg`, `_Retire`, `_Page`, `Durability.cfg`,
  `Durability_Chain.cfg`, `ShardMove.cfg`, `ShardMove_Live.cfg`) run nightly and on PRs that touch their spec or its
  mapped Go files.
- **Lean**: `formal/lean/` is a Lake project (`lakefile.lean`, `lean-toolchain`); the PR job runs `lake build` and
  `scripts/sorry-count.sh`, which fails if the `sorry` count exceeds `formal/lean/SORRY_BASELINE` (3).
- **Spec and test coupling**: `formal/MANIFEST.md` maps each `.tla` and `.lean` file to its Go test files (7.4);
  `scripts/formal-manifest-check.sh` fails a PR that changes a spec without a change in at least one mapped test file,
  unless it carries the `formal-no-test-change` label with a justification.
- **Trace validation** runs on the Postgres and chaos tests' traces in the integration job (`-workers 1`).

All results of this section, as run after the D25 amendment (4 workers, 10 GB heap, one configuration at a time; `time` is TLC's own wall-clock; for a violation,
`generated` and `distinct` are the work done before the counterexample and `depth` is its length):

| config | expected | result | generated | distinct | depth | time |
|---|---|---|---|---|---|---|
| `Outbox.cfg` | all hold | PASS | 17,243,719 | 5,557,863 | 39 | 01min 40s |
| `Outbox_Live.cfg` | all hold | PASS | 1,342,866 | 486,937 | 31 | 41s |
| `Outbox_NoWatch.cfg` | violates `NoLossSafety` | FAIL: NoLossSafety (as intended) | 9,574 | 6,046 | 7 | 00s |
| `Outbox_Watch1x.cfg` | violates `NoLossSafety` | FAIL: NoLossSafety (as intended) | 196,197 | 100,073 | 9 | 01s |
| `Consolidation.cfg` | all hold | PASS | 10,864,684 | 2,426,544 | 18 | 59s |
| `Consolidation_Live.cfg` | all hold | PASS | 65,555 | 20,404 | 12 | 02s |
| `Consolidation_VolatileProposal.cfg` | violates `ExactlyOnceEffect` | FAIL: ExactlyOnceEffect (as intended) | 7,535 | 3,970 | 6 | 01s |
| `Consolidation_NonAtomicKey.cfg` | violates `ExactlyOnceEffect` | FAIL: ExactlyOnceEffect (as intended) | 415 | 344 | 5 | 00s |
| `Storage.cfg` | all hold | PASS | 673,635 | 139,225 | 30 | 47s |
| `Storage_Gens.cfg` | all hold | PASS | 270,826 | 54,596 | 35 | 15s |
| `Storage_Update.cfg` | violates `ContentImmutable` | FAIL: ContentImmutable (as intended) | 12 | 12 | 3 | 00s |
| `Storage_FlipEarly.cfg` | violates `VectorGenerationConsistent` | FAIL: VectorGenerationConsistent (as intended) | 39 | 33 | 3 | 00s |
| `Storage_PurgeUnmarked.cfg` | violates `PurgeNeedsMarker` | FAIL: PurgeNeedsMarker (as intended) | 23 | 21 | 3 | 00s |
| `Storage_AutoRepair.cfg` | violates `RebuildBeforeRepair` | FAIL: RebuildBeforeRepair (as intended) | 2,835 | 1,045 | 7 | 00s |
| `Storage_PerIndexHygiene.cfg` | violates `RebuildBeforeRepair` | FAIL: RebuildBeforeRepair (as intended) | 136,244 | 32,732 | 15 | 01s |
| `Storage_PurgeDuringRebuildSet.cfg` | violates `RebuildBeforeRepair` | FAIL: RebuildBeforeRepair (as intended) | 85,592 | 20,307 | 13 | 01s |
| `Derivation.cfg` | all hold | PASS | 23,840,070 | 4,094,217 | 34 | 03min 49s |
| `Derivation_Page.cfg` | all hold | PASS | 41,597,641 | 7,251,558 | 34 | 06min 39s |
| `Derivation_Retire.cfg` | all hold | PASS | 22,613,082 | 3,773,679 | 30 | 02min 45s |
| `Derivation_Live.cfg` | all hold | PASS | 390,901 | 117,292 | 31 | 17s |
| `Derivation_NoLock.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 4,531 | 1,489 | 12 | 01s |
| `Derivation_CascadeEvidence.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 6,481 | 2,580 | 9 | 01s |
| `Derivation_PageNoVerify.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 49,274 | 13,275 | 13 | 02s |
| `Derivation_StaleProposal.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 84,133 | 23,152 | 16 | 03s |
| `Derivation_ReextractHides.cfg` | violates `ReextractKeepsDerivedVisible` | FAIL: ReextractKeepsDerivedVisible (as intended) | 490 | 215 | 7 | 00s |
| `Derivation_RestoreNoLock.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,296 | 415 | 11 | 00s |
| `Derivation_MatOnce.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,264 | 409 | 11 | 00s |
| `Derivation_PurgeDropsStub.cfg` | violates `FailClosed` | FAIL: FailClosed (as intended) | 9,163 | 2,797 | 14 | 01s |
| `Derivation_EffCited.cfg` | violates `AsOfNoLeak` | FAIL: AsOfNoLeak (as intended) | 135 | 47 | 7 | 00s |
| `Derivation_TombByDocId.cfg` | violates `NoOverHiding` | FAIL: NoOverHiding (as intended) | 58 | 39 | 4 | 00s |
| `Derivation_CasBeforeIdem.cfg` | violates `NoPhantomVersion` | FAIL: NoPhantomVersion (as intended) | 82 | 50 | 8 | 00s |
| `Derivation_CascadeHidden.cfg` | violates `NoGhostInvalidatedServed` | FAIL: NoGhostInvalidatedServed (as intended) | 2,610 | 823 | 9 | 01s |
| `Derivation_MatSignalOnly.cfg` | violates `MaterializeComplete` | FAIL: MaterializeComplete (as intended) | 740 | 302 | 8 | 00s |
| `Derivation_RestoreOnlySelf.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 501 | 252 | 6 | 00s |
| `Derivation_RestoreByVisibleTwin.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 401 | 204 | 5 | 00s |
| `Durability.cfg` | all hold | PASS | 555,786,086 | 138,507,972 | 27 | 27min 32s |
| `Durability_Chain.cfg` | all hold | PASS | 237,637,520 | 63,006,042 | 28 | 13min 25s |
| `Durability_IntentBeforeCommit.cfg` | violates `NoUnackedEffectOnLaterAck` | FAIL: NoUnackedEffectOnLaterAck (as intended) | 771 | 409 | 8 | 00s |
| `Durability_RaiseOnReopen.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 7,166,940 | 2,185,262 | 13 | 15s |
| `Durability_RetargetRestore.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 387,625 | 161,986 | 10 | 02s |
| `Durability_DupNoReput.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 41,626 | 17,788 | 11 | 01s |
| `Durability_AckNoRecheck.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 15,401 | 6,854 | 8 | 00s |
| `Durability_ReopenEarly.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 15,988 | 6,780 | 10 | 00s |
| `Durability_NarrowWindow.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 71,542 | 29,544 | 11 | 01s |
| `Durability_AckBeforeIntent.cfg` | violates `AckImpliesIntent` | FAIL: AckImpliesIntent (as intended) | 329 | 216 | 4 | 00s |
| `Durability_ClockOrder.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 1,109,362 | 453,238 | 16 | 05s |
| `Durability_UnorderedReplay.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 273,853 | 107,330 | 14 | 02s |
| `Durability_NoEpochGuard.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 12,764,837 | 4,708,469 | 18 | 34s |
| `Durability_CatalogLossNoBlobFloor.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 47,609 | 20,905 | 9 | 01s |
| `Durability_ReplayStampsCurrentEpoch.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 6,120,798 | 1,911,045 | 14 | 14s |
| `Durability_NoSubjectLock.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 30,215,391 | 9,242,180 | 16 | 01min 07s |
| `Durability_NoHelpPrev.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 1,908,812 | 770,234 | 19 | 07s |
| `ShardMove.cfg` | all hold | PASS | 112,413,371 | 28,152,715 | 39 | 07min 18s |
| `ShardMove_Live.cfg` | all hold | PASS | 12,888,037 | 2,267,141 | 38 | 04min 35s |
| `ShardMove_ActiveWriters.cfg` | all hold | PASS | 125,927 | 23,174 | 29 | 03s |
| `ShardMove_Twice.cfg` | all hold | PASS | 96,873 | 27,923 | 37 | 04s |
| `ShardMove_TgtRestore.cfg` | all hold | PASS | 6,309,063 | 1,576,137 | 30 | 28s |
| `ShardMove_UnfencedSteps.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 1,914,200 | 424,613 | 13 | 09s |
| `ShardMove_NoReady.cfg` | violates `RollbackPossibleBeforeC` | FAIL: RollbackPossibleBeforeC (as intended) | 375,681 | 86,201 | 11 | 03s |
| `ShardMove_RestoreNoReconcile.cfg` | violates `RestoreReconciles` | FAIL: RestoreReconciles (as intended) | 201,764 | 45,875 | 10 | 02s |
| `ShardMove_NoVerify.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 18,768 | 6,750 | 13 | 01s |
| `ShardMove_StampAfterCut.cfg` | violates `OneOwner` | FAIL: OneOwner (as intended) | 342,707 | 78,293 | 11 | 03s |
| `ShardMove_SweepNotPaused.cfg` | violates `SourceStaticUnderFreeze` | FAIL: SourceStaticUnderFreeze (as intended) | 1,360 | 391 | 6 | 01s |
| `ShardMove_CleanupNoBackup.cfg` | violates `CleanupSafe` | FAIL: CleanupSafe (as intended) | 86,349 | 22,748 | 19 | 01s |
| `ShardMove_NoSeqAdvance.cfg` | violates `CopiedBelowTargetSeq` | FAIL: CopiedBelowTargetSeq (as intended) | 6,799 | 2,319 | 10 | 01s |
| `ShardMove_CatalogLossNoShardTruth.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 51,684 | 14,554 | 17 | 01s |
| `ShardMove_UnionRepair.cfg` | violates `NoResurrect` | FAIL: NoResurrect (as intended) | 45,845 | 12,596 | 17 | 02s |
| `ShardMove_CleanupBackupBeforeActivate.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 532,683 | 147,136 | 20 | 04s |
| `ShardMove_CopyBeforeFreeze.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 15,311 | 5,222 | 11 | 01s |
| `ShardMove_RerunMerges.cfg` | violates `NoResurrect` | FAIL: NoResurrect (as intended) | 51,838 | 14,252 | 19 | 01s |
| `ShardMove_ReadyBeforeIndex.cfg` | violates `ServedFromIndex` | FAIL: ServedFromIndex (as intended) | 232 | 97 | 6 | 00s |
| `ShardMove_CutOnUnreplicatedCommit.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 53,969 | 15,137 | 17 | 01s |
