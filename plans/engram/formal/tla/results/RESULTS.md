# TLC results

Real runs of every configuration under `formal/tla/` after the round-6 (D25, N160 to N163, N166(6) and N168) amendment of the specs,
TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap, one configuration at a time.
`ShardMove.tla` was rewritten for freeze-then-copy, `Derivation.tla` gained the twin rule of N162 and `Storage.tla` the rebuild and
vacuum phases of N166(6); every design configuration was re-run (also those of the unchanged `Outbox`, `Consolidation` and `Durability`),
and so was every must-fail configuration of the three changed specs. The must-fail configurations of `Outbox`, `Consolidation`
and `Durability` are unchanged and keep their D24 logs. Raw logs are the
`<config>.log` files next to this file. `expected` is what the configuration is for: a design configuration must end with
"No error has been found"; a must-fail configuration must end with exactly the named invariant violated, and lists only that one;
`generated`/`distinct`/`depth` are TLC's own counters (for a violation, the work done before the counterexample was
found, and `depth` is the number of states in the counterexample; otherwise the depth of the complete state graph);
`time` is TLC's `Finished in` (or the wall-clock of the run for a short violation). Section 7 of the plan interprets
the results.

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

Bounds, one line each (the constants are in the `.cfg` files):

- `Derivation.cfg`: 4 facts (`f1 = (d1,v1)` with re-extraction twin `f4`, `f2 = (d1,v2)`, `f3 = (d2,v1)`), 1 observation x 3 versions, 2 writers, 3 stored proposals, 1 delete, invalidate and restore (2 curation operations), a commit that may be retried (N144), symmetry on writers. **Reduced in D24:** REPLACE, re-extraction and the chunk and re-extraction purges (`MaxRetire`) moved to `Derivation_Retire.cfg` (1 observation x 2 versions, 2 proposals, 2 curation operations, 1 delete, 1 REPLACE or re-extraction): with `MaxRetire = 1` the D23 bounds reached 26 M distinct states in 18 minutes with the queue still growing and were stopped (the stamp `mat`, the pointer `cv` and the proposal's `exp` multiplied the product), and with 3 versions x 3 proposals even one curation operation did not finish in three minutes at 3 M states.
- `Derivation_Page.cfg`: 1 observation x 1 version and 1 page x 2 versions, 2 writers, 3 proposals, 1 delete, 1 curation operation, a commit that may be retried, no REPLACE or re-extraction (with them the product ran past 30 minutes and was stopped).
- `Derivation_Live.cfg`: 1 observation x 2 versions, 1 writer that never crashes (a commit may still be retried), 2 proposals, 2 deletes, 1 curation operation; safety plus `ExpungeCompletes`.
- `Derivation_CasBeforeIdem.cfg`, `_CascadeHidden.cfg`, `_MatSignalOnly.cfg` (D24): 1 observation x 2 versions and 1 proposal (`_CasBeforeIdem`, with `CasFirst` and a blind root rebuild), or 1 version, 1 proposal and 1 curation operation (the other two); each fails in at most nine states.
- `Derivation_RestoreOnlySelf.cfg`, `_RestoreByVisibleTwin.cfg` (D25): 1 observation x 1 version, 1 proposal, 2 curation operations, 1 re-extraction (`MaxRetire = 1`), no delete; each fails in at most six states. The design configurations `Derivation.cfg`, `_Retire`, `_Live` and `_Page` are unchanged in bounds; the twin rule (`itag`, `LazyTwin`, the atomic `RestoreExact`) raised `Derivation.cfg` from 3.1 M to 4.1 M distinct states (2 min 18 s to 3 min 49 s) and `_Retire` from 2.8 M to 3.8 M (1 min 57 s to 2 min 45 s); `_Live` and `_Page` are unchanged.
- `ShardMove.cfg` (D25): 2 rows (each a ledger row, an idempotency key and an event), 1 client, writer lifetime 1, 2 ticks, freeze window 1 tick, epochs <= 4, 1 restore or failover of either shard (lossy or lossless; the target after the commit point through `TgtRestore`), 1 extra backup, 1 copy fault, 1 catalog loss or catalog restore; the shard sequences start at 2 (`s1`) and 0 (`s2`). No reduction was needed (the D24 dirty-copy model reached 36.8 M distinct states with the same shape of bounds; with a static source the product is 28.2 M). `_Live`: 2 clients, no extra backup, `MoveTerminates` and `FrozenBounded`. `_ActiveWriters`: 2 clients, 3 ticks, no restore, no backup, no catalog or copy fault, no voluntary abort. `_TgtRestore`: 1 row, 2 backups, 1 restore or promotion of the target after the commit point, 1 catalog restore, no voluntary abort or copy fault. The must-fail configurations use 1 or 2 rows and 1 or 2 clients, whichever reaches the counterexample (the constants are in the `.cfg` files).
- `ShardMove_Twice.cfg` and `_NoSeqAdvance`: **1 row**, 1 client, 4 ticks, 2 moves, no restore, 1 backup (the second move waits for the cleanup, which needs the backup), no copy fault. With 2 rows the D24 product passed a million states at depth 14 within a minute and the second move is about 37 steps deep.
- `Durability.cfg`: shape `del, del, ret` on one document, 8 ticks (`MaxT = 7`), commit latency 1, margin 1, 2 restores, 1 catalog restore (D24: the floor goes back to its highest value). `Durability_Chain.cfg`: shape `inv, res, inv` on one fact, requests that may overlap (subject lock held through the intent put), clock skew 1, margin 2, `MaxT = 4`, 2 restores, no catalog restore (it changes nothing for a fact subject once the blob floor is in place). A `MaxT = 7` run of the chain shape was stopped at 47 M distinct states with the queue still growing.
- `Storage.cfg` (**reduced in D25**): 3 rows in two namespaces on one partition (thresholds 1 and 2), **2** embedding generations, rebuild as snapshot-then-publish, vacuum as start-then-collect, the partition key. With 3 generations the product ran the 30-minute cap out at 2.76 M distinct states, depth 22, 0.73 M states on the queue and was stopped; `Storage_Gens.cfg` keeps the third generation with one row in each namespace (rows 1 and 3). The must-fail `_PerIndexHygiene`, `_AutoRepair` and `_PurgeDuringRebuildSet` use 2 generations.
- `Consolidation.cfg`: 3 facts, 1 observation, 2 crashes, 1 delete (the 4-fact and 2-observation variants were stopped after about ten minutes with the queue still growing).
- `Outbox.cfg`: 3 writers, 2 namespaces, 2 consumers, 5 seqs, timeout 2, watch 4, 1 relay crash.
