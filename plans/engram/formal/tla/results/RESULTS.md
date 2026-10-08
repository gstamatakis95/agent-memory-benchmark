# TLC results

Real runs of every configuration under `formal/tla/` after the round-8 (D27, N179 to N188) amendment of the specs,
TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap, one configuration at a time.
`ShardMove.tla` gained `SealCopy`, the standby image, `Failover`, `CatalogPromote`, the move bit and `RestoreDone`; `Durability.tla`
the tenant delete; `Derivation.tla` the curation log. Their configurations were all re-run; `Outbox`, `Consolidation` and `Storage` keep their logs. Raw logs are the
`<config>.log` files next to this file. `expected` is what the configuration is for: a design configuration must end with "No error
has been found"; a must-fail configuration must end with exactly the named invariant violated, and lists only that one;
`generated`/`distinct`/`depth` are TLC's own counters (for a violation, the work done before the counterexample was found, and `depth`
is the number of states in the counterexample; otherwise the depth of the complete state graph); `time` is TLC's `Finished in`.
Section 7 of the plan interprets the results; `EXPECT` is the machine-readable form.

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

Bounds, one line each (the constants are in the `.cfg` files):

- `Derivation.cfg`: 4 facts (`f1 = (d1,v1)` with re-extraction twin `f4`, `f2 = (d1,v2)`, `f3 = (d2,v1)`), 1 observation x 3 versions, 2 writers, 3 stored proposals, 1 delete, invalidate and restore (2 curation operations), a commit that may be retried (N144), symmetry on writers. **Reduced in D24:** REPLACE, re-extraction and the chunk and re-extraction purges (`MaxRetire`) moved to `Derivation_Retire.cfg` (1 observation x 2 versions, 2 proposals, 2 curation operations, 1 delete, 1 REPLACE or re-extraction): with `MaxRetire = 1` the D23 bounds reached 26 M distinct states in 18 minutes with the queue still growing and were stopped (the stamp `mat`, the pointer `cv` and the proposal's `exp` multiplied the product), and with 3 versions x 3 proposals even one curation operation did not finish in three minutes at 3 M states.
- `Derivation_Page.cfg`: 1 observation x 1 version and 1 page x 2 versions, 2 writers, 3 proposals, 1 delete, 1 curation operation, a commit that may be retried, no REPLACE or re-extraction (with them the product ran past 30 minutes and was stopped).
- `Derivation_Live.cfg`: 1 observation x 2 versions, 1 writer that never crashes (a commit may still be retried), 2 proposals, 2 deletes, 1 curation operation; safety plus `ExpungeCompletes`.
- `Derivation_CasBeforeIdem.cfg`, `_CascadeHidden.cfg`, `_MatSignalOnly.cfg` (D24): 1 observation x 2 versions and 1 proposal (`_CasBeforeIdem`, with `CasFirst` and a blind root rebuild), or 1 version, 1 proposal and 1 curation operation (the other two); each fails in at most nine states.
- `Derivation_RestoreOnlySelf.cfg`, `_RestoreByVisibleTwin.cfg` (D25): 1 observation x 1 version, 1 proposal, 2 curation operations, 1 re-extraction (`MaxRetire = 1`), no delete; each fails in at most six states. The design configurations `Derivation.cfg`, `_Retire`, `_Live` and `_Page` are unchanged in bounds; the twin rule (`itag`, `LazyTwin`, the atomic `RestoreExact`) raised `Derivation.cfg` from 3.1 M to 4.1 M distinct states (2 min 18 s to 3 min 49 s) and `_Retire` from 2.8 M to 3.8 M (1 min 57 s to 2 min 45 s); `_Live` and `_Page` are unchanged. `Derivation_LazyTwinNoLock.cfg` and `_LazyTagFromLastLog.cfg` (D26) use the same bounds with `MaxRetire = 1` (fail in 6 and 7 states). The design configurations gained `DocLock` and `TagFromLastLog` and the repeated `Invalidate`: `Derivation.cfg` 4.1 M to 4.9 M distinct states (3 min 49 s to 4 min 09 s), `_Retire` 3.8 M to 4.7 M (2 min 45 s to 3 min 31 s); `_Page` and `_Live` unchanged in bounds.
- `ShardMove.cfg` (D27): 2 rows (each a ledger row, an idempotency key and an event), 1 client, writer lifetime 1, 2 ticks, freeze window 1 tick, epochs <= 4, 1 restore (to the initial backup), failover or lossless failover of either shard, **no extra backup (`MaxBak = 0`, was 1)**, 1 copy fault, 1 catalog loss, promotion or restore; the shard sequences start at 2 (`s1`) and 0 (`s2`). With `MaxBak = 1` it hit the cap at 112 M distinct states (32 M queued); now 25.9 M, 7 min 18 s. `_Live`: **1 row (was 2)**, 2 clients, `MoveTerminates` and `FrozenBounded` (2 rows: cap at 13.7 M; now 1.8 M, 3 min 25 s). `_ActiveWriters`: 2 clients, 3 ticks, no fault. `_TgtRestore`: 1 row, 2 backups, 1 restore, failover or promotion of the target after the commit point, 1 catalog event (15.8 M, 4 min 41 s). `_TgtRestore2`: as `_TgtRestore` with `MaxMoves = 2`, `MaxEp = 6`, 4 ticks, no catalog event (73.1 M, 23 min 51 s). The standby image is (rows, keys, index bit), the target's only (a full shard image ran `_TgtRestore` past 77 M states). The must-fail configurations use 1 or 2 rows and 1 or 2 clients (constants in the `.cfg` files).
- `ShardMove_Twice.cfg` and `_NoSeqAdvance`: **1 row**, 1 client, 4 ticks, 2 moves, no restore, 1 backup (the second move waits for the cleanup, which needs the backup), no copy fault. With 2 rows the D24 product passed a million states at depth 14 within a minute and the second move is about 37 steps deep.
- `Durability.cfg`: shape `del, del, ret` on one document and the tenant delete (two namespaces, one phase per tick), **`MaxT = 6` (was 7)**, commit latency 1, margin 1, 2 restores, 1 catalog restore (also reverts the `deleting` row before the first fence). With `MaxT = 7` the run hit the cap at 168 M distinct states (18 M queued); now 100.6 M, 16 min 44 s. `Durability_Chain.cfg`: shape `inv, res, inv` on one fact, overlapping requests, clock skew 1, margin 2, `MaxT = 4`, 2 restores, no catalog restore; its state count is unchanged. `Durability_CatalogRestoreInFenceWindow.cfg`: `MaxT = 2`, no shard restore, 1 catalog restore, `TenantAckBeforeFence = TRUE`.
- `Storage.cfg` (**reduced in D25**): 3 rows in two namespaces on one partition (thresholds 1 and 2), **2** embedding generations, rebuild as snapshot-then-publish, vacuum as start-then-collect, the partition key. With 3 generations the product ran the 30-minute cap out at 2.76 M distinct states, depth 22, 0.73 M states on the queue and was stopped; `Storage_Gens.cfg` keeps the third generation with one row in each namespace (rows 1 and 3). The must-fail `_PerIndexHygiene`, `_AutoRepair` and `_PurgeDuringRebuildSet` use 2 generations.
- `Consolidation.cfg`: 3 facts, 1 observation, 2 crashes, 1 delete (the 4-fact and 2-observation variants were stopped after about ten minutes with the queue still growing).
- `Outbox.cfg`: 3 writers, 2 namespaces, 2 consumers, 5 seqs, timeout 2, watch 4, 1 relay crash.
