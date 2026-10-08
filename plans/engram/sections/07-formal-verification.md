## 7. Formal verification

Scope: the six TLA+ specifications under `formal/tla/` and the four Lean 4 modules under `formal/lean/Engram/`;
what TLC reported on them after the round-8 amendment (D27: N179 to N188; `ShardMove.tla` gains the seal, the standby, the failover and the move bit; `Durability.tla` gains the tenant delete; `Derivation.tla` gains the curation log); how the Go code is kept faithful to them; what is not formalised; how it is wired into CI. Every number below is copied from a log in `formal/tla/results/` (summary
in `RESULTS.md`, which also holds the full table).

Tooling: TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap per
configuration. Lean 4 is not available in the planning environment; the Lean files and the Lake skeleton are
**not type-checked**.

### 7.1 What is modelled, and the convention

| Spec | Register | Question the model answers | Prose mechanisms this spec omits (C-21) |
|---|---|---|---|
| `Derivation.tla` | N113, N115 to N121, N133, N135, N136, N144, N145, N162, N174 | Can a deleted, invalidated or superseded fact reach a reader through a fact, an observation version or a page version, at any `as_of`, with the markers as the only synchronous write, and is nothing else hidden? Does a retried commit leave `current_version` naming a version that exists, does an acknowledged invalidation outlive the purge of its fact, is the owed Materialize found from the markers rather than from a signal, and does `Restore` undo exactly the set an invalidation (and its lazy twins, tagged from the curation log) wrote? | LLM text (a version's text is its evidence set plus its base's); `proof_count` and retirement; the marker-set size alerts; `stale_write`/`stale_delete` flags; the pacing of Materialize and Purge; one fact per chunk; the consolidation scheduler; the attempt-unique markdown blob key and its `NOT EXISTS` deletion rule (N144) and the export `hidden_overlay` (N145(4)) are tested, not modelled |
| `ShardMove.tla` | D5, N123 to N125, N137, N160 to N163, N169 to N172, N175, N179 to N185, N188 | Freeze, copy of a static source, verify by equality, the index bit, the seal (the target's standby has replayed the copy and its index; the floor), the catalog CAS and the replicated-LSN gate on (c) and on the thaw, cleanup, restore and failover of either shard during and after a move (a target restored after the commit point is an ordinary restore that closes the move; a restore or failover below the floor is refused), a promoted or restored catalog and the reconcile's reads interleaved with mover steps and with a shard restore, deletes on the active target, a second move: one owner, no loss, no duplicate, no resurrected delete, no move bit left on a finished move, a started move completes. | One namespace; blob pre-warm and copy, `return_abort`; the window estimate and the copy's parallel streams and key ranges; which indexes are requested (A3); reads at `frozen/move`; drain of workflows; the relay's cursor wait; the 24 h timer (cleanup is an action); the timeline check is a guard, not a zombie primary; a standby's replay is a snapshot, not a WAL position, and a restore at or past the floor is a snapshot that covers the copy; `--lose-move-ins`, the `lost` state and the recovery move (N183); T7-3 is accounted as RPO (N171(5)) |
| `Durability.tla` | N122, N134, N146, N150, N182 | Is every acknowledged delete or invalidation still in force when reads reopen after a restore or failover (also after the catalog was restored), is no unacknowledged effect applied over a later acknowledged write, with overlapping requests on one fact, and does an acknowledged tenant delete survive a catalog restore? | Namespace-level intents and the per-namespace replay of a tenant delete (the model has the catalog row, two fences and the ack; one subject kind per config); the 35-day retention; intent content is a set of versions, not `up_to_version` arithmetic |
| `Storage.tla` | N111 to N113, N138, N152, N166(6) | Content rows never change after insert, a model flip never exposes a row without a vector, dead index entries leave only through a rebuild and the hygiene unit is the partition (a vacuum waits until every touched graph on it is rebuilt; a rebuild is snapshot-then-publish and a vacuum start-then-collect, and purges skip the partition from the selection of the rebuild set until the vacuum has started), the index converges. | HNSW internals and recall; the index runner's lease; a partition is a set of per-namespace graphs |
| `Outbox.tla` | D6, N80 | No committed event is lost through sequence gaps; per-namespace order; the 2 x timeout watch horizon. | Kafka, consumer lag, batching |
| `Consolidation.tla` | N43, N121 | A round's decisions are applied exactly once under at-least-once activities and crashes. | The persisted-proposal `attempt` key and the all-skip stamp (C-11) are tested, not modelled; `Derivation.tla` carries the base check |

Each spec has *design* configurations (every knob at the register's value) that must end with "No error has been
found", and *must-fail* configurations in which one knob is set to the plausible simplification or to a reviewer's
variant; each must end with exactly the invariant named in its first comment line violated, and lists only that
invariant. A design that passes only because the simplification was never tried is not evidence, so the must-fail
configurations are part of the deliverable and run in CI (N141). Bounds were shrunk until every design configuration
finished in under 30 minutes (the longest: `ShardMove_TgtRestore2.cfg` 23 min 51 s, `Durability.cfg` 16 min 44 s, `Durability_Chain.cfg` 13; all
others under 8; the reductions are listed in 7.2.6).

**Assumptions of the D27 models** (what the specs take as given; each is a sentence a reader can falsify):

- **A1 (N146, N163, N171, N185), the catalog.** A catalog outcome (`committed` or `rolled_back`) that the one asynchronous
  standby has replayed (`rep` in `ShardMove.tla`) survives a *promotion*; one that has not is lost by it
  (`CatalogLoss`); `CatalogPromote` loses nothing. Each starts the reconcile (`cdirty`). Every shard action on an
  arbiter outcome ((c), the thaw, the restore reconcile's completion) waits for `rep` and re-reads the row. The reconcile
  is the only catalog access while it runs; its reads (`ReadShard`) and its derivation (`ApplyReconcile`) are separate
  steps, so mover steps and a shard restore that began earlier interleave. A catalog **restore from backup** keeps
  nothing (`CatalogRestore` also resets `rep`: T7-3 is an RPO loss, N171(5)); the reconcile re-derives routing and the
  move row from the ownership rows. A restore to an older point starts only when no reconcile is running (7.2.2, item 5).
- **A2 (N179, N180), the target after the commit point.** The target's copy is sealed before (b'): `SealCopy` needs the
  standby's snapshot to cover the copy and its index and sets the floor `flr`, so a restore or failover of the target
  lands at or past a complete copy (below it is refused; a restore at the floor is a snapshot that holds the copy).
  Afterwards it is an ordinary restore: the reconcile completes (c) and (d), `RestoreDone` activates the target at a new
  epoch, closes its move bit and re-asserts the catalog epoch, and the acknowledged deletes (`gone`, durable intents)
  are re-applied. Nothing is merged and the source is never read again. The cleanup gate is the seal. Deletes without an
  intent (sweeps, purges) are re-derived by their schedulers. Repeated faults hit one shard; a fault of **both** copies
  of a row before it is re-replicated is an RPO loss outside the model.
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
`ReextractPurge`; writers `Propose`, `Pick`, `Verify` (shared lock), `Commit` (base compare-and-set), crash `AbortW`;
expunge `MatBegin`, per-node `MatScan`/`MatWrite` batches (each under the exclusive lock, each re-reading
`fact_hidden`), `MatEnd`, `Purge`, `DerivedPurge` (stubs). `cv[n]` is
`current_version`, separate from the version rows; `Commit` is the compare-and-set `cv + 1` and records the attempt's
`commit_key`, and a crash may follow it; `RetryCommit` re-executes it and, in the design, looks the `commit_key` up
first (N144). `mat` is `fact_hidden.materialized_at`: `MatBegin` is enabled by an open tombstone or by an unstamped
invalidation with rows owed (the markers, not a signal). `Served(T)` is the SQL rule of N117; a ghost copy of each
version's real derivation states the truth the invariants are checked against. N162: `f1` and its
twin `f4` are one subject; `itag[f]` is `fact_hidden.invalidation_op`; `Invalidate(f)` hides the fact and its live twins
in one step under one id (the existing rows' tag, else a fresh one), and `Restore(g)` removes every row that carries the
id of `g`. N174: the lazy twin of a re-extraction is `LazyTwinRead` (a `CommitChunk` holding the document lock shared
reads the subject's tag) then `LazyTwinCommit` (the twin is born hidden, with that tag); with `DocLock`, `Invalidate` and
`Restore` wait in between. **D27 (C8-12):** `clog[subj]` is the subject's last `curation_log` action and tag, written by
`Invalidate` and `Restore`; `LazyTwinRead` reads `clog[subj].tag` (it replaces `lastOp`, which was one global variable).

*Invariants.* `NoDeletedDerivationServed`, `NoInvalidatedDerivationServed`, `RestoreExact` (also: an invalidation is
atomic over its subject, so no twin survives a `Restore`), `NoOverHiding`, `AsOfNoLeak`, `MaterializeComplete` (a stamped
invalidation is covered by `derived_hidden` rows on every node; an unstamped one with rows owed is never stranded),
`EvidenceOutlivesFacts`, `BaseCurrentAtCommit`, `NoLostRebuild`, `NoVictimTextAfterPurge`, `FailClosed`,
`ReextractKeepsDerivedVisible`, `NoPhantomVersion` (`current_version` names an existing row), `NoGhostInvalidatedServed`,
and the new `TagFromLog` (`clog[subj].tag` is the tag of every hidden fact of the subject, so the lazy twin resolves with
the subject's `Restore`); liveness `ExpungeCompletes`.

*Configurations.* Design: `Derivation.cfg` (1 observation x 3 versions, 2 writers, 3 proposals, REPLACE or
re-extraction once, invalidate and restore, one delete), `_Page` (adds a page x 2 versions; REPLACE and re-extraction are
left to the first), `_Retire`, `_Live`. Must-fail, one per bug:

| Config | Knob | Violates | Finding |
|---|---|---|---|
| `_CascadeEvidence` | evidence rows die with their fact | `NoDeletedDerivationServed` | C-1: REPLACE, chunk purge, delete: the derived version survives |
| `_PageNoVerify`, `_StaleProposal` | a page commit re-verifies nothing; a stored update is applied without its base | `NoDeletedDerivationServed` | C-2, C-3 |
| `_ReextractHides` | derived versions hidden by cause `reextract` | `ReextractKeepsDerivedVisible` | C-6 |
| `_RestoreNoLock`, `_MatOnce` | `Restore` without the exclusive lock; Materialize reads `fact_hidden` once | `RestoreExact` | C-7: scan, Restore, write leaves a permanent row |
| `_PurgeDropsStub` | `DerivedPurge` deletes the row | `FailClosed` | N136: the version number is reused |
| `_NoLock`, `_EffCited`, `_TombByDocId` | no lock; `effective_at` of cited facts only; tombstone by document id | `NoDeletedDerivationServed`, `AsOfNoLeak`, `NoOverHiding` | kept from D22 |
| `_CasBeforeIdem` | the base compare-and-set before the `commit_key` check, and a blind root rebuild | `NoPhantomVersion` | r5 C-1 (N144) |
| `_CascadeHidden`, `_MatSignalOnly` | purges drop `fact_hidden(invalidate)`; Materialize waits for a lost signal | `NoGhostInvalidatedServed`, `MaterializeComplete` | r5 C-2 (N145) |
| `_RestoreOnlySelf`, `_RestoreByVisibleTwin` | `Restore(g)` un-hides only `g`; twins resolved by current state | `RestoreExact` | r6 C-4 (N162) |
| `_LazyTwinNoLock` | `DocLock = FALSE` | `RestoreExact` | N174(1): `LazyTwinRead`, `Restore`, `LazyTwinCommit` leaves a hidden twin under a removed tag |
| `_LazyTagFromLastLog` | `TagFromLastLog`: a repeated `Invalidate` logs a fresh id although its rows keep the old one | `RestoreExact` | N174(2): the twin carries the log's tag, not the one `Restore` resolves |

*What the model showed.*

1. **The cause-tagged design is the design, and the `Restore` lock is load-bearing** (C-7): with cause-tagged rows and
   batches `_RestoreNoLock` and `_MatOnce` fail. **Evidence must outlive facts** (C-1: `_CascadeEvidence` reaches a served
   derived version in ten steps).
2. **The derivation lock is needed because the read predicate looks only at pending tombstones** (N117): a late
   writer's version stays hidden while the tombstone is `pending` and is exposed when it becomes `materialized`
   (`_NoLock`, `_PageNoVerify`). **The base check is a compare-and-set inside the insert** (N120(3): `UPDATE ... SET
   current_version = v + 1 WHERE current_version = base_version`; zero rows: discard); idempotency comes before the
   CAS (`_CasBeforeIdem`).
3. **The owed Materialize needs a durable marker, and the invalidation row must outlive the fact** (r5 C-2:
   `_CascadeHidden`, `_MatSignalOnly`). `NoPhantomVersion` says "an existing row", not "a non-stub row": `DerivedPurge`
   turns the current version into a stub, which `FailClosed` hides and which keeps its number.
4. **`Restore` must be exact by tag** (r6 C-4, N162): with the invalidation id on every row it wrote, the lazy twin
   included, `Restore` needs no look-up by current hidden state (`_RestoreOnlySelf`, `_RestoreByVisibleTwin`). The tag
   must be the subject's one and the log must record it (`_LazyTagFromLastLog`, `TagFromLog`); the lazy twin's
   read-to-commit span excludes the markers (`_LazyTwinNoLock`).

#### 7.2.2 `ShardMove.tla`: freeze, copy, verify, index, seal, cut over, restore

*Model.* Source `s1` and target `s2` (a second move swaps the roles). Per shard: insert-only rows (each with an
`ins_seq` from the shard's sequence `sq[s]`), mutable keys (swept by an active source), a counter for the expiring class,
**one index bit** `idx[s]` (A3), the ownership row's move bit `mv[s]` (`move_id` set) and the target's floor bit `flr[s]`;
the target's standby holds a replayed image `sb[s]` (rows, keys, index bit). The catalog carries the move row `cm`, the
namespace `(shard, epoch)` and `rep` (the standby has replayed the outcome). Mover: `Plan` (sets the target's `mv`);
`Freeze` (the source is static from here; sets its `mv`; the one `engram_seq_advance`); `CopyStep(r)`, `CopyEx`;
`CopyFault(r)` (a dropped row, N175); `VerifyFrozen` (equal row and key sets, `ex[Tgt] <= ex[Src]`; a mismatch
re-copies once, a second one aborts); `BuildIndex`; **`SealCopy`** (`built` to `sealed`: under `FloorBeforeCut` the
standby's image `sb[Tgt]` must cover the copy and its index; sets `flr[Tgt]`); `MakeReady` (b', needs the index and
`sealed`); `CommitCAS` (a''); `Replicated`; `Cut` (c: source `frozen`, `cm = committed`, `rep`); `Activate` (b'': target
row `ready` and `cm = committed`, whatever the routing row says; clears `mv`); `CatFlip` (d); `Cleanup` (gate: the seal).
Abort: `AbortCAS`, `AbortReplicated`, `Thaw` (needs `rolled_back` replicated; clears `mv` and `flr`). Environment:
writers before the freeze and after activation, `SrcDelete`, `SweepEx`, `TgtDelete(r)`, `Backup`; **`StandbyReplay(Tgt)`**
(the standby replays to now, or is rebuilt from the base backup and starts again from there); `Restore` and `TgtRestore`
(to the last backup, or lossless failover) and **`Failover(Tgt)`** (to `sb`), both refused below the floor (`BelowFloor`:
`flr[Tgt]` and the image lacks the copy; `Failover` only under `FailoverChecksFloor`); `CatalogLoss`, **`CatalogPromote`**
(a lossless promotion), `CatalogRestore` (each sets `cdirty`); `ReadShard(s)`, `ApplyReconcile` (owner = the
highest-epoch `active`/`frozen`/`restoring` row; a re-derived `committed` writes the routing row `(Tgt, e_t)` only);
`Reconcile_` (`open`: `AbortCAS`, then the thaw once replicated; `committed`: a source completes (c), (b'') and (d); a
target takes the ordinary restore path); **`RestoreDone`** (ends a restored target's move, clears `mv` under
`RestoreCloses`, re-asserts the catalog epoch `max(catalog, shard)`).

*Invariants.* `SingleWriter`, `NoLossNoDup` (the target is activated with the frozen rows, no more; a committed move
whose source left `frozen` has a ready or active target holding them; after the cleanup an active target holds every
frozen row it did not delete; once settled every acknowledged row that is not an accepted RPO loss and not deleted on
the target is on an active owner), `RollbackPossibleBeforeC`, `NoWriteToTargetBeforeC`, `NoRouteToTargetBeforeC`,
`ZombieCannotCutOver`, `RestoreReconciles`, `OneOwner`, `CatalogNamesOwnerAfterDone`, `CleanupSafe`,
`CopiedBelowTargetSeq`, `NoResurrect`, `ServedFromIndex`, `SourceStaticUnderFreeze`, and the new
**`MoveClosedWhenFinal`** (a `done` or `rolled_back` move leaves no `mv` bit) and **`OwnerHasNoStaleMove`**. Liveness:
`MoveTerminates` and `FrozenBounded`.

*Configurations.* Design: `ShardMove.cfg` (2 rows, 1 client, writer lifetime 1, 2 ticks, freeze window 1 tick, 1 restore, failover or lossless
failover of either shard, no extra backup, 1 copy fault, 1 catalog loss, promotion or restore; 25.9 M distinct states,
07min 18s), `_Live` (1 row, 2 clients, both liveness properties; 1.8 M, 03min 25s), `_ActiveWriters` (2 clients, writers
before and after, no fault; 44 k), `_Twice` (one row, two moves; 309 k), `_TgtRestore` (one row, one restore, failover
or promotion of the target after the commit point, one catalog event, 2 backups; 15.8 M, 04min 41s) and `_TgtRestore2`
(as `_TgtRestore` with two moves, so the restored row can be a previous life's `moved_out`; 73.1 M, 23min 51s). Must-fail, kept: `_UnfencedSteps`, `_NoReady`,
`_RestoreNoReconcile`, `_NoVerify`, `_StampAfterCut`, `_SweepNotPaused`, `_NoSeqAdvance`, `_CatalogLossNoShardTruth`,
`_UnionRepair`, `_CopyBeforeFreeze`, `_ReadyBeforeIndex`, `_CutOnUnreplicatedCommit`, `_ThawOnUnreplicatedAbort`,
`_CatalogLossDuringFreeze`; renamed and new:

| Config | Knob | Violates | Finding |
|---|---|---|---|
| `_NoFloor` | `FloorBeforeCut = FALSE` | `NoLossNoDup` | N179: without the seal a target restored after the commit point to a pre-copy backup is activated without the moved rows |
| `_LaggingFailover` | `FailoverChecksFloor = FALSE` | `NoLossNoDup` | N179(3), C8-3: the target's standby is rebuilt after the seal and promoted after the commit; the restore path activates it without the copy (and without its index) |
| `_RestoreKeepsMove` | `RestoreCloses = FALSE` | `MoveClosedWhenFinal` | N180(1), C8-10: the restored target is activated by `restore_done`, the move ends `done`, `move_id` stays set |
| `_EndOnRouting` | `EndFromTargetRow = FALSE`, `MaxCat = 1` | `OneOwner` | N180(3), C8-2: (c), a catalog promotion, its reconcile writes `(Tgt, e_t)`, the mover ends `done` on the routing row; the target is `ready`, no shard is active (17 states) |

*What the model showed.*

1. **The WAL floor replaces the backup, and it must bind the standby as well as the restore.** With the seal before (b')
   every restore or failover of the target after the commit lands at or past a complete copy (`_TgtRestore`,
   `_TgtRestore2`); `_NoFloor` shows the seal is load-bearing and `_LaggingFailover` that a rebuilt standby needs the floor
   check. The floor is read after the last index build, so the standby's image carries the index too (the first model
   of the floor covered the rows only and `_TgtRestore2` promoted a standby without the HNSW: `ServedFromIndex`; a spec
   gap, not a design flaw).
2. **The mover's end test must read the target row** (`_EndOnRouting`, C8-2), the exact trace of the round-8 review:
   with the routing row as the test, a promotion after (c) makes the reconcile write `(Tgt, e_t)` while the target is
   still `ready`. With the row as the test, (b'') runs and the design holds, including with the promotion.
3. **`restore_done` must close the move** (`_RestoreKeepsMove`, C8-10); `RestoreDone` also re-asserts the catalog epoch.
4. **Kept from D25 and D26** : every shard
   action on an arbiter outcome needs the replicated outcome (`_CutOnUnreplicatedCommit`, `_ThawOnUnreplicatedAbort`); the
   reconcile reads shard rows, not epochs of unowned rows (`_CatalogLossDuringFreeze`); the catalog CAS before (c) and
   the row-state guard on every mover step (`_StampAfterCut`, `_UnfencedSteps`, `_RestoreNoReconcile`); nothing is merged
   (`_UnionRepair`); a static source (`_SweepNotPaused`, `_CopyBeforeFreeze`), `VerifyFrozen` (`_NoVerify`) and one
   sequence advance (`_NoSeqAdvance`, `_Twice`) are load-bearing.
5. **Two design findings of the first design run, both fixed in the model by a rule the register does not yet state**
   (N188 and N180(4) take `frozen/restore` as an owner and drop the `~cdirty` guard of a restore; the register needs
   one sentence each). (a) *A restoring target of an uncommitted move is not an owner.* With "owner = highest-epoch
   `active`/`frozen/*` row, `frozen/restore` included", a target restored during the freeze window (`incoming`, epoch
   `e + 1`) outranks the frozen source (`e`); a promotion in that window routes the namespace to a target that holds
   nothing (`NoRouteToTargetBeforeC`, 8 states). The model counts a restoring target only once
   the source row reads `moved_out`; the real rows carry it as `move_id` set and `moved_in_at` null. (b) *A restore to an
   older point must not start while a lossy catalog event is being reconciled.* A catalog restore after (c) followed by
   a source restore to a backup that predates the move, both inside the reconcile, leaves no witness of the commit: the move rolls
   back and the committed rows exist only on the discarded target (`RollbackPossibleBeforeC`, 21 states). The restore's
   first write is the catalog epoch, which waits for the reconcile (N185), so the model enables a lossy restore or
   failover only when `cdirty` is false; a restore that began earlier, and a lossless failover, still interleave with the
   reconcile's reads. If the failover agent may promote a shard standby while the catalog reconcile runs, this
   double fault needs a rule (for example: the restore refuses while `catalog reconcile` is running).
6. **Modelling choices (not design changes):** `NoLossNoDup` is stated on the moved rows; a restored shard reads as no
   owner in its own reconcile's snapshot; the floor covers the rows the target held at the seal, minus rows it deleted
   itself (`gone`); a restore at or past the floor is a restore to a snapshot that covers them; only the target's standby is
   tracked (the source's lagging promotion is a restore to an older backup).

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
by one tick. `CatalogRestore` puts the catalog's floor back to its initial value (the worst case); requests on one fact
may overlap, and a marker transaction is `TxnBegin` (reads the subject's previous entry under the subject lock) then
`Commit`; the lock is held until the transaction's own intent is put, and under it the transaction first puts the
intent of the subject's latest entry if that is missing (`HelpPrev`). **D27 (N182):** the tenant-level delete is a
phase `tdp`: 1 = the catalog `deleting` row and the tenant intent , 2 and 3 = `freeze_delete` on the
first and second of the tenant's two namespaces , then the acknowledgement `ta` (after the last fence; with
`TenantAckBeforeFence` after phase 1); one `Tick` advances the delete by one phase. `CatalogRestore` also reverts the `deleting` row while no
namespace is fenced (`tdp = 4`, `FAILED{CATALOG_RESTORED}`); once one is, the reconcile re-derives it (N163(2)).

*Invariants.* `AckImpliesIntent`, `AckedDeleteSurvives` (a served shard has the marker every acknowledged delete relies
on; and an acknowledged tenant delete is never reverted), `IntentOrderLastWins` (the state after a restore is that of
the operation that committed last), `NoUnackedEffectOnLaterAck`.

*Configurations.* Design: `Durability.cfg` (document subject, the tenant delete, `MaxT = 6`, 2 restores, 1 catalog restore, margin 1, latency 1), `Durability_Chain.cfg`
(fact subject, overlapping requests with the subject lock, clocks skewed by one tick, `MaxT = 4`, 2 restores). Must-fail:
`_IntentBeforeCommit` (C-5), `_RaiseOnReopen` (C-4), `_ClockOrder` (C-16), `_DupNoReput`, `_AckNoRecheck`,
`_NoEpochGuard`, `_RetargetRestore`, `_ReopenEarly`, `_AckBeforeIntent`, `_NarrowWindow`, `_UnorderedReplay`,
`_CatalogLossNoBlobFloor` (the catalog is restored, its floor is back at its highest value and the replay reads it
alone), `_ReplayStampsCurrentEpoch` (a replayed marker takes the bumped epoch and the guard skips the subject's second
intent), `_NoSubjectLock` (two overlapping requests read the same `prev_operation_id`, the chain forks), `_NoHelpPrev`
(the lock without the help step), and new **`_CatalogRestoreInFenceWindow`** (`AckedDeleteSurvives`, N182: the
tenant delete is acknowledged before the fences and a catalog restore inside the window reverts the `deleting` row).

*What the model showed.*

1. **The floor lives in the catalog and is only lowered; the intent follows the commit** (C-4, C-5: `_RaiseOnReopen`,
   `_RetargetRestore`, `_IntentBeforeCommit`); a retry after a crash between commit and put re-puts the marker's
   intent (`_DupNoReput`). **The ack re-reads the marker after the put** (`_AckNoRecheck`): a restore can land between the
   commit and the put, and a late put would be acknowledged although no replay will see it.
2. **Replay compares epochs, and `Invalidate`/`Restore` always write their marker** (`_NoEpochGuard`): intents carry the
   namespace epoch, replay skips an intent older than an applied entry of its subject (a skipped intent counts as
   settled, or `Reopen` waits forever, §9.3 step 5) and stamps the **recorded** epoch (`_ReplayStampsCurrentEpoch`).
3. **The margin must exceed the longest commit latency plus the clock skew** (`_NarrowWindow`); chain order removes the
   need for synchronised clocks (`_ClockOrder`, `_UnorderedReplay`); the replay floor `min(fl, blobFloor)` must not live
   only in the catalog (`_CatalogLossNoBlobFloor`).
4. **Overlapping requests need a session-level subject lock held through the intent put, and a help step** (N150;
   `_NoSubjectLock`, `_NoHelpPrev`). A transaction-level lock lets a successor commit and put its intent while the
   predecessor's put is in flight, and a restore then replays the successor first; a marker whose writer crashed
   between commit and put leaves a hole that the replay treats as satisfied. The fix modelled: under the lock the next
   transaction puts the missing intent of the subject's latest entry (put-if-absent); the alternative is a per-subject
   sequence under the lock with replay in sequence order. `IntentOrderLastWins` is stated against the commit order.
5. **A tenant delete is acknowledged only once every namespace is fenced** (N182, C8-5): after the `deleting` row and
   the intent a catalog restore can still revert the row, and a client that was told "deleted" finds the tenant
   active (`_CatalogRestoreInFenceWindow`, 3 states); after the last fence the shard rows re-derive it.

#### 7.2.4 `Storage.tla`: insert-only content, vector generations, index hygiene

Content rows are never rewritten and are purged only under a marker; a model flip waits for the next generation's
vectors; the index follows the vector table. The purge counts the dead entries it leaves (`pc = purged_since_build`), a
rebuild at the threshold is the only thing that removes them (`vacuum_index_cleanup = off`), and `AutoRepair` models
autovacuum cleaning the graph in place. Invariants `ContentImmutable`, `PurgeNeedsMarker`,
`VectorGenerationConsistent`, `RebuildBeforeRepair`, `DeadCounted`, liveness `IndexConvergence`. Must-fail:
`Storage_Update`, `_FlipEarly`, `_PurgeUnmarked`, `_AutoRepair`.

N152: the hygiene unit is the **partition**. It holds the graphs of several namespaces (one with threshold 1, one with 2),
each with its own purge counter; the hygiene rebuilds every touched graph once one is due, and the partition `Vacuum`,
which reads every graph, runs only when each touched graph is rebuilt. `Storage_PerIndexHygiene` (rebuild only the due
graph) fails `RebuildBeforeRepair` in fifteen states. `IndexConvergence`: every due graph is rebuilt and the current
generation's vectors are indexed. N166(6): a rebuild (`RebuildStart`, `RebuildPublish`) and a vacuum (`VacuumStart`,
`VacuumCollect`) are not atomic; the design takes the partition key `lk` from the selection of the rebuild set until
`VacuumStart`, and purge batches skip the partition while it is held; `Storage_PurgeDuringRebuildSet` fails in thirteen
states. `Storage.cfg` (2 generations, 139 k distinct states, 47 s) and `Storage_Gens.cfg` (3 generations, 55 k) pass.

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
   three; restores are "revert to the last backup", "promote the standby's snapshot" or lossless failover, not PITR; the
   mutable and expiring classes are sets and a counter; a promotion and a catalog restore are one event each, not a
   sequence of replica states; repeated restores hit one shard (A2); the freeze window is a tick count, not a clock; the
   reconcile's snapshot reads are per shard, and nobody else writes the catalog meanwhile (A1). A bound bump is a spec
   change reviewed as such.
2. **`Derivation_Page.cfg` leaves REPLACE and re-extraction out** (the product with a page ran past 30 minutes);
   `Derivation.cfg` covers the retry of N144 without them, and the twin rule and the document lock are exercised where a
   re-extraction is possible (`_Retire` and the `Restore` and `LazyTwin` must-fail configurations).
3. **Liveness runs forbid writer crashes** (`AllowAbortW = FALSE`): with crashes a writer can starve Materialize by
   re-taking the shared lock forever, which the 35 s single attempt and the retry make a latency issue, not a safety one.
4. **Bounds** (figures in `results/RESULTS.md`): REPLACE, re-extraction and the two purges live in `Derivation_Retire.cfg`;
   `Durability_Chain.cfg` has no catalog restore; `Storage.cfg` has two embedding generations and `Storage_Gens.cfg` keeps
   the third with one row per namespace; `ShardMove_Twice` keeps one row. **D27 deviations from the D26 bounds:** with the standby, the failover and the promotion, `ShardMove.cfg`
   with its D26 bounds ran the 30-minute cap out at 112 M distinct states (32 M queued), so **`MaxBak` went from 1 to 0**
   (backups are exercised by `_TgtRestore` and `_TgtRestore2`, which keep their bounds and are the closest to the cap: 23 min
   51 s); `_Live` ran out at 13.7 M (3.1 M queued), so **`NRows` went from 2 to 1**; `Durability.cfg` with the tenant delete
   ran out at 168 M (18 M queued) at `MaxT = 7`, so **`MaxT` went from 7 to 6** (the tenant delete advances one phase per
   tick, which keeps `Durability_Chain.cfg` and the must-fail configurations at their D26 state counts). The standby image is
   (rows, keys, index bit) and only the target's is tracked: a full shard image ran `_TgtRestore` past 77 M states.

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
| `Derivation` | `internal/recall`, `internal/expunge`, `internal/consolidate`, `internal/pages`: `TestVisibility_AllSurfaces`, `TestVisibility_SegmentHiding`, `TestInvalidate_RestoreExact`, `TestInvalidate_ConcurrentReextract`, `TestInvalidate_RepeatedReusesTag`, `TestAsOf_*`, `TestExpunge_DerivationLock`, `TestDelete_ReuseDocumentID`, `TestVisibility_AllSurfaces` (C-1), `TestDerivation_CommitRule` (C-2), `TestApply_BaseVersionCAS` (C-3), `TestReextract_Rebuilds` (C-6), `TestMaterialize_StampUnderLock` (C-7), `TestPageRefresh_CommitRetry` (r5 C-1), `TestInvalidate_SurvivesChunkPurge`, `TestInvalidate_RestoreUndoesTwinSet`, `TestInvalidate_LazyTwinRestored` (N162), `TestExpunge_SweeperFindsUnmaterialized`, `TestExport_OverlayBeforeMaterialize` (r5 C-2) |
| `ShardMove` | `internal/move`: `TestMove_RollbackEveryStep`, `TestMove_ReadyState`, `TestMove_FrozenCopy`, `TestMove_VerifyFrozenCatchesFault`, `TestMove_WindowDeadlineRollback`, `TestMove_IndexesValidBeforeReady` (N160), `TestMove_CatalogCAS`, `TestMove_ZombieFenced`, `TestRestore_OpenMoves`, `TestMove_TwiceAndExport`, `TestMove_CleanupGateAtEntry`, `TestMove_TargetRestoredAfterSeal`, `TestMove_TargetPITRBelowFloor`, `TestMove_CutWaitsForSealedCopy`, `TestMove_FailoverBelowFloorRefused`, `TestMove_ActivateAfterCatalogPromotion`, `TestMove_RollbackWaitsForReplicatedAbort`, `TestCatalog_ReconcileIgnoresIncomingEpoch`, `TestMove_VerifyPerRange`, `TestRestore_SourceCleansMovedOut` (N161), `TestCatalog_PromotionRunsReconcile`, `TestMove_CutWaitsForReplicatedCommit`, `TestCatalog_ReconcileDerivesRouting` (N163), `TestRestore_CatalogWritesReplicated` (N185); mover killed at every persisted state |
| `Durability` | `internal/intent`, `cmd/engramctl restore replay`: `TestIntent_AckImpliesIntent`, `TestIntent_DuplicateAttempt` (re-put of the committed marker's intent), `TestIntent_AckRereadsMarker`, `TestIntent_EpochGuard`, `TestRestore_ReplaysIntents`, `TestFailover_ReplaysIntents`, a double-restore case, `TestReplay_FloorFromBlob` (r5 C-3), `TestIntent_ReplayTwoOfSameSubject`, `TestIntent_ConcurrentCurationOneChain` (r5 C-7, C-8), `TestDelete_TenantAckAfterFence` (N182) |
| `Storage` | `internal/store`, `internal/index`: `TestContent_InsertOnly`, `TestVectors_ModelGeneration`, `TestHNSW_PerNamespace`, `TestIndex_Hygiene` (N152: a partition with three namespaces purged at 0.2 %, 1 % and 0 %; N166(6): a neighbour touched once is not rebuilt and a purge during the rebuild set is skipped) |
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
| `hidden`, `hidRe`, `ctomb`, `itag`, `clog` | `fact_hidden(cause = 'invalidate' / 'reextract')`, `fact_hidden.invalidation_op`; `chunk_tombstones`; the subject's last `curation_log` action and operation id |
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
| `tl`, `mtl`, `bak`, `sb`, `flr` | `catalog.shards.timeline_id`; the mover session's timeline; the latest pgBackRest backup; the standby's replayed position (`engram_standby_replayed`); `namespace_moves.copy_end_lsn`, `copy_end_timeline`, `copy_sealed_at` and the target row's `floor_lsn` |
| `mv[s]` | `namespace_ownership.move_id` (set by `Plan` and `start_move`, cleared by `activate_target`, `thaw_move`, `restore_done`) |
| `Durability.tdp`, `ta` | catalog `tenants` `deleting` row and the tenant intent; `freeze_delete` on each namespace; `TenantOperation.acknowledged_at` |
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
  outside `ShardMove.tla`, which has the session timeline only as a guard (`ZombieCannotCutOver`); there is no
  must-fail configuration for it.
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
  no configuration is an experiment. A **timeout is
  neither a failure nor a pass**: it is reported INCOMPLETE with the states and depth reached. A bound bump that
  pushes a design configuration over 30 minutes is a spec change reviewed as such.
- **PR job `formal-quick`**: parses every spec with SANY and runs every must-fail configuration and every design
  configuration that finishes in under two minutes; the others (`Derivation.cfg`, `_Retire`, `_Page`, `Durability.cfg`,
  `Durability_Chain.cfg`, `ShardMove.cfg`, `ShardMove_Live.cfg`) run nightly and on PRs that touch their spec or its
  mapped Go files.
- **Manifest** (`EXPECT`, N188): 79 configurations: `Outbox` 4, `Consolidation` 4, `Storage` 8 (both `Storage.cfg` and
  `Storage_Gens.cfg`), `Derivation` 21, `Durability` 18, `ShardMove` 24 (design: `ShardMove`, `_Live`, `_ActiveWriters`,
  `_Twice`, `_TgtRestore`, `_TgtRestore2`; the other 18 must fail).
- **Lean**: `formal/lean/` is a Lake project (`lakefile.lean`, `lean-toolchain`); the PR job runs `lake build` and
  `scripts/sorry-count.sh`, which fails if the `sorry` count exceeds `formal/lean/SORRY_BASELINE` (3).
- **Spec and test coupling**: `formal/MANIFEST.md` maps each `.tla` and `.lean` file to its Go test files (7.4);
  `scripts/formal-manifest-check.sh` fails a PR that changes a spec without a change in at least one mapped test file,
  unless it carries the `formal-no-test-change` label with a justification.
- **Trace validation** runs on the Postgres and chaos tests' traces in the integration job (`-workers 1`).

All results of this section, as run after the D27 amendment (4 workers, 10 GB heap, one configuration at a time; `time` is TLC's own wall-clock; for a violation,
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
| `Derivation.cfg` | all hold | PASS | 29,459,388 | 5,014,417 | 34 | 04min 25s |
| `Derivation_Page.cfg` | all hold | PASS | 41,597,641 | 7,251,558 | 34 | 06min 43s |
| `Derivation_Retire.cfg` | all hold | PASS | 29,274,470 | 4,837,592 | 31 | 03min 55s |
| `Derivation_Live.cfg` | all hold | PASS | 390,901 | 117,292 | 31 | 16s |
| `Derivation_NoLock.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 5,373 | 1,739 | 11 | 01s |
| `Derivation_CascadeEvidence.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 3,535 | 1,515 | 10 | 01s |
| `Derivation_PageNoVerify.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 42,590 | 11,698 | 14 | 02s |
| `Derivation_StaleProposal.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 94,025 | 25,477 | 16 | 02s |
| `Derivation_ReextractHides.cfg` | violates `ReextractKeepsDerivedVisible` | FAIL: ReextractKeepsDerivedVisible (as intended) | 697 | 292 | 7 | 00s |
| `Derivation_RestoreNoLock.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,863 | 573 | 11 | 01s |
| `Derivation_MatOnce.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,746 | 550 | 11 | 01s |
| `Derivation_PurgeDropsStub.cfg` | violates `FailClosed` | FAIL: FailClosed (as intended) | 8,939 | 2,736 | 13 | 01s |
| `Derivation_EffCited.cfg` | violates `AsOfNoLeak` | FAIL: AsOfNoLeak (as intended) | 135 | 47 | 7 | 00s |
| `Derivation_TombByDocId.cfg` | violates `NoOverHiding` | FAIL: NoOverHiding (as intended) | 74 | 48 | 4 | 00s |
| `Derivation_CasBeforeIdem.cfg` | violates `NoPhantomVersion` | FAIL: NoPhantomVersion (as intended) | 74 | 45 | 7 | 00s |
| `Derivation_CascadeHidden.cfg` | violates `NoGhostInvalidatedServed` | FAIL: NoGhostInvalidatedServed (as intended) | 2,091 | 706 | 9 | 01s |
| `Derivation_MatSignalOnly.cfg` | violates `MaterializeComplete` | FAIL: MaterializeComplete (as intended) | 757 | 300 | 8 | 00s |
| `Derivation_RestoreOnlySelf.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 614 | 320 | 6 | 00s |
| `Derivation_RestoreByVisibleTwin.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 258 | 156 | 5 | 00s |
| `Derivation_LazyTwinNoLock.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,727 | 604 | 7 | 01s |
| `Derivation_LazyTagFromLastLog.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,328 | 586 | 6 | 00s |
| `Durability.cfg` | all hold | PASS | 400,878,956 | 100,607,628 | 26 | 16min 44s |
| `Durability_Chain.cfg` | all hold | PASS | 237,637,520 | 63,006,042 | 28 | 12min 58s |
| `Durability_IntentBeforeCommit.cfg` | violates `NoUnackedEffectOnLaterAck` | FAIL: NoUnackedEffectOnLaterAck (as intended) | 909 | 453 | 8 | 00s |
| `Durability_RaiseOnReopen.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 7,163,191 | 2,174,548 | 16 | 14s |
| `Durability_RetargetRestore.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 297,963 | 125,009 | 11 | 02s |
| `Durability_DupNoReput.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 32,064 | 14,157 | 9 | 01s |
| `Durability_AckNoRecheck.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 14,344 | 6,489 | 8 | 01s |
| `Durability_ReopenEarly.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 18,578 | 8,161 | 8 | 01s |
| `Durability_NarrowWindow.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 78,420 | 31,498 | 11 | 01s |
| `Durability_AckBeforeIntent.cfg` | violates `AckImpliesIntent` | FAIL: AckImpliesIntent (as intended) | 311 | 208 | 4 | 00s |
| `Durability_ClockOrder.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 1,003,791 | 413,781 | 14 | 04s |
| `Durability_UnorderedReplay.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 239,589 | 93,706 | 14 | 02s |
| `Durability_NoEpochGuard.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 12,588,365 | 4,656,165 | 18 | 33s |
| `Durability_CatalogLossNoBlobFloor.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 78,673 | 32,507 | 11 | 01s |
| `Durability_ReplayStampsCurrentEpoch.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 5,309,765 | 1,664,213 | 14 | 13s |
| `Durability_NoSubjectLock.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 26,223,957 | 8,094,411 | 15 | 57s |
| `Durability_NoHelpPrev.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 2,236,861 | 893,425 | 20 | 08s |
| `Durability_CatalogRestoreInFenceWindow.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 26 | 26 | 3 | 00s |
| `ShardMove.cfg` | all hold | PASS | 97,077,157 | 25,930,382 | 44 | 07min 18s |
| `ShardMove_Live.cfg` | all hold | PASS | 8,340,698 | 1,758,738 | 38 | 03min 25s |
| `ShardMove_ActiveWriters.cfg` | all hold | PASS | 239,107 | 43,634 | 32 | 05s |
| `ShardMove_Twice.cfg` | all hold | PASS | 1,053,217 | 308,843 | 42 | 26s |
| `ShardMove_TgtRestore.cfg` | all hold | PASS | 58,194,803 | 15,835,227 | 35 | 04min 41s |
| `ShardMove_TgtRestore2.cfg` | all hold | PASS | 312,103,720 | 73,051,538 | 47 | 23min 51s |
| `ShardMove_UnfencedSteps.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 12,124,664 | 2,530,465 | 16 | 52s |
| `ShardMove_NoReady.cfg` | violates `RollbackPossibleBeforeC` | FAIL: RollbackPossibleBeforeC (as intended) | 314,469 | 62,558 | 11 | 03s |
| `ShardMove_RestoreNoReconcile.cfg` | violates `RestoreReconciles` | FAIL: RestoreReconciles (as intended) | 1,149,588 | 232,587 | 12 | 07s |
| `ShardMove_NoVerify.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 63,514 | 21,226 | 16 | 02s |
| `ShardMove_StampAfterCut.cfg` | violates `OneOwner` | FAIL: OneOwner (as intended) | 7,218,450 | 1,460,791 | 15 | 33s |
| `ShardMove_SweepNotPaused.cfg` | violates `SourceStaticUnderFreeze` | FAIL: SourceStaticUnderFreeze (as intended) | 1,307 | 367 | 6 | 00s |
| `ShardMove_NoSeqAdvance.cfg` | violates `CopiedBelowTargetSeq` | FAIL: CopiedBelowTargetSeq (as intended) | 23,118 | 7,257 | 12 | 01s |
| `ShardMove_CatalogLossNoShardTruth.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 453,804 | 121,044 | 21 | 03s |
| `ShardMove_UnionRepair.cfg` | violates `NoResurrect` | FAIL: NoResurrect (as intended) | 172,121 | 47,535 | 19 | 02s |
| `ShardMove_CopyBeforeFreeze.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 36,071 | 12,731 | 12 | 01s |
| `ShardMove_ReadyBeforeIndex.cfg` | violates `ServedFromIndex` | FAIL: ServedFromIndex (as intended) | 260 | 106 | 6 | 00s |
| `ShardMove_CutOnUnreplicatedCommit.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 1,538,874 | 455,186 | 23 | 09s |
| `ShardMove_NoFloor.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 48,930 | 15,250 | 14 | 02s |
| `ShardMove_LaggingFailover.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 814,799 | 243,502 | 18 | 05s |
| `ShardMove_RestoreKeepsMove.cfg` | violates `MoveClosedWhenFinal` | FAIL: MoveClosedWhenFinal (as intended) | 147,816 | 45,925 | 14 | 02s |
| `ShardMove_EndOnRouting.cfg` | violates `OneOwner` | FAIL: OneOwner (as intended) | 16,790 | 6,030 | 17 | 01s |
| `ShardMove_ThawOnUnreplicatedAbort.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 831,411 | 265,252 | 16 | 06s |
| `ShardMove_CatalogLossDuringFreeze.cfg` | violates `CatalogNamesOwnerAfterDone` | FAIL: CatalogNamesOwnerAfterDone (as intended) | 4,132 | 1,615 | 11 | 01s |

### Round-8 changes

| Finding | Change |
|---|---|
| C8-3, PG8-8 | `MoveBackup` is `SealCopy`; `BackupBeforeCut` is `FloorBeforeCut`, `backed` is `sealed`; the standby image `sb`, the floor `flr`, `StandbyReplay`, `Failover` (`FailoverChecksFloor`); `_NoMoveBackup` is `_NoFloor`, new `_LaggingFailover` |
| C8-2 | `CatalogPromote` and the end test from the target row (`EndFromTargetRow`, `EndOnRouting`); new `_EndOnRouting` reproduces the review's trace |
| C8-10 | the move bit `mv`, `RestoreCloses`, `RestoreDone` closes it; `MoveClosedWhenFinal`, `OwnerHasNoStaleMove`; new `_RestoreKeepsMove` |
| C8-6, A8-3 | a restore that began before a promotion is read by its reconcile; `frozen/restore` counts as an owner; `RestoreDone` re-asserts the epoch |
| C8-5 | `Durability.tla`: the tenant delete (`tdp`, `ta`, `TenantAckBeforeFence`); new `_CatalogRestoreInFenceWindow` |
| C8-12 | `Derivation.tla`: `clog[subj]` replaces `lastOp`; `TagFromLog` |
| TLC | two rules the register lacks (7.2.2, item 5), found by `_TgtRestore`; the floor covers the index (`_TgtRestore2`) |
| Bounds | `ShardMove.cfg` `MaxBak` 0, `_Live` `NRows` 1, `Durability.cfg` `MaxT` 6 (7.2.6) |
| Manifest | `EXPECT` 79 configurations (75 + 4 new; `_NoMoveBackup` renamed `_NoFloor`, its log deleted) |
| Tests | 7.4 cites the seven new or renamed names of §8.4 (`TestMove_TargetRestoredAfterSeal` to `TestRestore_CatalogWritesReplicated`) |
