# TLC results

Real runs of every configuration under `formal/tla/` after the round-5 (D24, N144 to N150 and N152) amendment of the specs, TLC 2.18
(`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap, one configuration at a time. Raw logs are the
`<config>.log` files next to this file. `expected` is what the configuration is for: a design configuration (and
a passing experiment) must end with "No error has been found"; a must-fail configuration must end with exactly the
named invariant (or, for `MoveTerminatesActive`, a temporal property) violated, and lists only that one;
`generated`/`distinct`/`depth` are TLC's own counters (for a violation, the work done before the counterexample was
found, and `depth` is the number of states in the counterexample; otherwise the depth of the complete state graph);
`time` is TLC's `Finished in` (or the wall-clock of the run for a short violation). Section 7 of the plan interprets
the results.

| config | expected | result | generated | distinct | depth | time |
|---|---|---|---|---|---|---|
| `Outbox.cfg` | all hold | PASS | 17,243,719 | 5,557,863 | 39 | 01min 34s |
| `Outbox_Live.cfg` | all hold | PASS | 1,342,866 | 486,937 | 31 | 39s |
| `Outbox_NoWatch.cfg` | violates `NoLossSafety` | FAIL: NoLossSafety (as intended) | 9,574 | 6,046 | 7 | 00s |
| `Outbox_Watch1x.cfg` | violates `NoLossSafety` | FAIL: NoLossSafety (as intended) | 196,197 | 100,073 | 9 | 01s |
| `Consolidation.cfg` | all hold | PASS | 10,864,684 | 2,426,544 | 18 | 57s |
| `Consolidation_Live.cfg` | all hold | PASS | 65,555 | 20,404 | 12 | 03s |
| `Consolidation_VolatileProposal.cfg` | violates `ExactlyOnceEffect` | FAIL: ExactlyOnceEffect (as intended) | 7,535 | 3,970 | 6 | 01s |
| `Consolidation_NonAtomicKey.cfg` | violates `ExactlyOnceEffect` | FAIL: ExactlyOnceEffect (as intended) | 415 | 344 | 5 | 00s |
| `Storage.cfg` | all hold | PASS | 15,167,809 | 1,651,471 | 33 | 22min 07s |
| `Storage_Update.cfg` | violates `ContentImmutable` | FAIL: ContentImmutable (as intended) | 12 | 12 | 4 | 00s |
| `Storage_FlipEarly.cfg` | violates `VectorGenerationConsistent` | FAIL: VectorGenerationConsistent (as intended) | 39 | 33 | 4 | 00s |
| `Storage_PurgeUnmarked.cfg` | violates `PurgeNeedsMarker` | FAIL: PurgeNeedsMarker (as intended) | 23 | 21 | 4 | 00s |
| `Storage_AutoRepair.cfg` | violates `RebuildBeforeRepair` | FAIL: RebuildBeforeRepair (as intended) | 1,323 | 563 | 7 | 00s |
| `Storage_PerIndexHygiene.cfg` | violates `RebuildBeforeRepair` | FAIL: RebuildBeforeRepair (as intended) | 76,288 | 17,097 | 13 | 01s |
| `Derivation.cfg` | all hold | PASS | 18,252,454 | 3,077,073 | 34 | 02min 18s |
| `Derivation_Page.cfg` | all hold | PASS | 41,597,641 | 7,251,558 | 34 | 06min 12s |
| `Derivation_Retire.cfg` | all hold | PASS | 17,365,950 | 2,845,427 | 30 | 01min 57s |
| `Derivation_Live.cfg` | all hold | PASS | 390,901 | 117,292 | 31 | 16s |
| `Derivation_NoLock.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 4,539 | 1,494 | 12 | 01s |
| `Derivation_CascadeEvidence.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 5,171 | 2,080 | 9 | 01s |
| `Derivation_PageNoVerify.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 48,214 | 13,056 | 14 | 02s |
| `Derivation_StaleProposal.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 94,314 | 25,556 | 18 | 02s |
| `Derivation_ReextractHides.cfg` | violates `ReextractKeepsDerivedVisible` | FAIL: ReextractKeepsDerivedVisible (as intended) | 401 | 187 | 7 | 00s |
| `Derivation_RestoreNoLock.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,190 | 364 | 12 | 00s |
| `Derivation_MatOnce.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 1,169 | 364 | 13 | 00s |
| `Derivation_PurgeDropsStub.cfg` | violates `FailClosed` | FAIL: FailClosed (as intended) | 8,417 | 2,590 | 13 | 01s |
| `Derivation_EffCited.cfg` | violates `AsOfNoLeak` | FAIL: AsOfNoLeak (as intended) | 135 | 47 | 9 | 00s |
| `Derivation_TombByDocId.cfg` | violates `NoOverHiding` | FAIL: NoOverHiding (as intended) | 83 | 51 | 6 | 00s |
| `Derivation_CasBeforeIdem.cfg` | violates `NoPhantomVersion` | FAIL: NoPhantomVersion (as intended) | 76 | 45 | 8 | 00s |
| `Derivation_CascadeHidden.cfg` | violates `NoGhostInvalidatedServed` | FAIL: NoGhostInvalidatedServed (as intended) | 1,700 | 604 | 9 | 00s |
| `Derivation_MatSignalOnly.cfg` | violates `MaterializeComplete` | FAIL: MaterializeComplete (as intended) | 974 | 361 | 9 | 00s |
| `Durability.cfg` | all hold | PASS | 555,786,086 | 138,507,972 | 27 | 25min 37s |
| `Durability_Chain.cfg` | all hold | PASS | 237,637,520 | 63,006,042 | 28 | 13min 57s |
| `Durability_IntentBeforeCommit.cfg` | violates `NoUnackedEffectOnLaterAck` | FAIL: NoUnackedEffectOnLaterAck (as intended) | 771 | 409 | 8 | 00s |
| `Durability_RaiseOnReopen.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 7,166,940 | 2,185,262 | 16 | 15s |
| `Durability_RetargetRestore.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 387,625 | 161,986 | 13 | 02s |
| `Durability_DupNoReput.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 41,626 | 17,788 | 11 | 01s |
| `Durability_AckNoRecheck.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 15,401 | 6,854 | 11 | 00s |
| `Durability_ReopenEarly.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 15,988 | 6,780 | 11 | 00s |
| `Durability_NarrowWindow.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 71,542 | 29,544 | 11 | 01s |
| `Durability_AckBeforeIntent.cfg` | violates `AckImpliesIntent` | FAIL: AckImpliesIntent (as intended) | 329 | 216 | 7 | 00s |
| `Durability_ClockOrder.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 1,109,362 | 453,238 | 16 | 05s |
| `Durability_UnorderedReplay.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 273,853 | 107,330 | 16 | 02s |
| `Durability_NoEpochGuard.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 12,764,837 | 4,708,469 | 20 | 34s |
| `Durability_CatalogLossNoBlobFloor.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 47,609 | 20,905 | 10 | 01s |
| `Durability_ReplayStampsCurrentEpoch.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 6,120,798 | 1,911,045 | 15 | 14s |
| `Durability_NoSubjectLock.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 30,215,391 | 9,242,180 | 16 | 01min 07s |
| `Durability_NoHelpPrev.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 1,908,812 | 770,234 | 19 | 07s |
| `ShardMove.cfg` | all hold | PASS | 145,121,412 | 36,807,699 | 30 | 09min 42s |
| `ShardMove_Live.cfg` | all hold | PASS | 12,028,465 | 2,150,879 | 27 | 03min 41s |
| `ShardMove_ActiveWriters.cfg` | all hold | PASS | 1,713,087 | 312,230 | 25 | 33s |
| `ShardMove_Twice.cfg` | all hold | PASS | 636,014 | 202,783 | 35 | 16s |
| `ShardMove_TgtRestore.cfg` | all hold | PASS | 36,274,924 | 9,688,960 | 29 | 02min 21s |
| `ShardMove_ZeroMargin.cfg` | all hold | PASS | 12,317,204 | 2,204,243 | 28 | 03min 41s |
| `ShardMove_UnfencedSteps.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 10,546,663 | 2,134,704 | 13 | 39s |
| `ShardMove_NoTimelineCheck.cfg` | all hold | PASS | 147,041,839 | 36,807,699 | 30 | 09min 19s |
| `ShardMove_NoReady.cfg` | violates `RollbackPossibleBeforeC` | FAIL: RollbackPossibleBeforeC (as intended) | 3,106,409 | 782,532 | 11 | 14s |
| `ShardMove_RestoreNoReconcile.cfg` | violates `RestoreReconciles` | FAIL: RestoreReconciles (as intended) | 980,235 | 267,794 | 10 | 05s |
| `ShardMove_NoVerify.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 28,568,880 | 6,434,921 | 13 | 01min 42s |
| `ShardMove_StampAfterCut.cfg` | violates `OneOwner` | FAIL: OneOwner (as intended) | 686,390 | 184,513 | 11 | 05s |
| `ShardMove_IdKeyedRecopy.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 4,651,781 | 866,924 | 19 | 01min 07s |
| `ShardMove_MergeNoDeletes.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 1,788,977 | 329,000 | 20 | 26s |
| `ShardMove_SweepNotPaused.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 3,447,603 | 572,242 | 20 | 53s |
| `ShardMove_CleanupNoBackup.cfg` | violates `CleanupSafe` | FAIL: CleanupSafe (as intended) | 420,803 | 111,397 | 17 | 03s |
| `ShardMove_NoSeqAdvance.cfg` | violates `CopiedBelowTargetSeq` | FAIL: CopiedBelowTargetSeq (as intended) | 3,686 | 1,467 | 9 | 01s |
| `ShardMove_NoSeqAdvanceTwice.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 411,501 | 126,134 | 27 | 09s |
| `ShardMove_VerifyBeforeCatchUp.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 73,641 | 20,469 | 10 | 03s |
| `ShardMove_CatalogLossNoShardTruth.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 127,749 | 34,056 | 15 | 02s |
| `ShardMove_CleanupTimeGate.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 2,962,786 | 861,096 | 18 | 12s |

Bounds, one line each (the constants are in the `.cfg` files):

- `Derivation.cfg`: 4 facts (`f1 = (d1,v1)` with re-extraction twin `f4`, `f2 = (d1,v2)`, `f3 = (d2,v1)`), 1 observation x 3 versions, 2 writers, 3 stored proposals, 1 delete, invalidate and restore (2 curation operations), a commit that may be retried (N144), symmetry on writers. **Reduced in D24:** REPLACE, re-extraction and the chunk and re-extraction purges (`MaxRetire`) moved to `Derivation_Retire.cfg` (1 observation x 2 versions, 2 proposals, 2 curation operations, 1 delete, 1 REPLACE or re-extraction): with `MaxRetire = 1` the D23 bounds reached 26 M distinct states in 18 minutes with the queue still growing and were stopped (the stamp `mat`, the pointer `cv` and the proposal's `exp` multiplied the product), and with 3 versions x 3 proposals even one curation operation did not finish in three minutes at 3 M states.
- `Derivation_Page.cfg`: 1 observation x 1 version and 1 page x 2 versions, 2 writers, 3 proposals, 1 delete, 1 curation operation, a commit that may be retried, no REPLACE or re-extraction (with them the product ran past 30 minutes and was stopped).
- `Derivation_Live.cfg`: 1 observation x 2 versions, 1 writer that never crashes (a commit may still be retried), 2 proposals, 2 deletes, 1 curation operation; safety plus `ExpungeCompletes`.
- `Derivation_CasBeforeIdem.cfg`, `_CascadeHidden.cfg`, `_MatSignalOnly.cfg` (D24): 1 observation x 2 versions and 1 proposal (`_CasBeforeIdem`, with `CasFirst` and a blind root rebuild), or 1 version, 1 proposal and 1 curation operation (the other two); each fails in at most nine states.
- `ShardMove.cfg`: 2 rows (each a ledger row, an idempotency key and an event), **1 client** (D24; `_Live` and `_ActiveWriters` keep 2), writer lifetime 1, margin 1, 2 ticks, epochs <= 4, 1 restore or failover (lossy or lossless), 1 extra backup; the shard sequences start at 2 (`s1`) and 0 (`s2`). **Reduced in D24:** with 2 clients and 1 extra backup the D24 model passed 22 M distinct states at depth 19 in 9 minutes with the queue still growing (the D23 design had 23 M in total) and was stopped; the catalog restore and the promotion of the target (`TgtRestore`) are in `ShardMove_TgtRestore.cfg`. `_NoTimelineCheck` (an experiment with the same bounds) also has 1 client. `_Live`, `_ZeroMargin`: no extra backup. `_ActiveWriters`: no restore, no voluntary abort, no backup. `_CleanupNoBackup`: 1 row, 1 client. The first design runs with 3 ticks and 2 extra backups were stopped after ten minutes with the queue still growing.
- `ShardMove_Twice.cfg` and the second-move must-fail configurations (`_NoSeqAdvance`, `_NoSeqAdvanceTwice`): **1 row**, 1 client, 4 ticks, 2 moves, no restore, 1 backup (the second move waits for the cleanup, which needs the backup). With 2 rows the product passed a million states at depth 14 within a minute and was stopped (the second move is about 40 steps deep). `_TgtRestore`, `_CleanupTimeGate`: 1 row, 1 client, 3 ticks, 1 promotion of the target that loses 1 row, 2 (design) or 1 backups; `_TgtRestore` also has 1 catalog restore. `_CatalogLossNoShardTruth`: 1 row, 1 client, 1 catalog restore.
- `Durability.cfg`: shape `del, del, ret` on one document, 8 ticks (`MaxT = 7`), commit latency 1, margin 1, 2 restores, 1 catalog restore (D24: the floor goes back to its highest value). `Durability_Chain.cfg`: shape `inv, res, inv` on one fact, requests that may overlap (subject lock held through the intent put), clock skew 1, margin 2, `MaxT = 4`, 2 restores, no catalog restore (it changes nothing for a fact subject once the blob floor is in place). A `MaxT = 7` run of the chain shape was stopped at 47 M distinct states with the queue still growing.
- `Storage.cfg`: 3 rows in two namespaces on one partition (thresholds 1 and 2), 3 embedding generations. The must-fail `_PerIndexHygiene` uses 2 generations.
- `Consolidation.cfg`: 3 facts, 1 observation, 2 crashes, 1 delete (the 4-fact and 2-observation variants were stopped after about ten minutes with the queue still growing).
- `Outbox.cfg`: 3 writers, 2 namespaces, 2 consumers, 5 seqs, timeout 2, watch 4, 1 relay crash.
