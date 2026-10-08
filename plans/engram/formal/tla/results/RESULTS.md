# TLC results

Real runs of every configuration under `formal/tla/` after the round-4 (D23, N141) rewrite of the specs, TLC 2.18
(`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap. Raw logs are the
`<config>.log` files next to this file. `expected` is what the configuration is for: a design configuration (and
a passing experiment) must end with "No error has been found"; a must-fail configuration must end with exactly the
named invariant (or, for `MoveTerminatesActive`, a temporal property) violated, and lists only that one;
`generated`/`distinct`/`depth` are TLC's own counters (for a violation, the work done before the counterexample was
found, and `depth` is the number of states in the counterexample; otherwise the depth of the complete state graph);
`time` is TLC's `Finished in` (or the wall-clock of the run for a short violation). Section 7 of the plan interprets
the results.

| config | expected | result | generated | distinct | depth | time |
|---|---|---|---|---|---|---|
| `Outbox.cfg` | all hold | PASS | 17,243,719 | 5,557,863 | 39 | 01min 18s |
| `Outbox_Live.cfg` | all hold | PASS | 1,342,866 | 486,937 | 31 | 32s |
| `Outbox_NoWatch.cfg` | violates `NoLossSafety` | FAIL: NoLossSafety (as intended) | 9,199 | 5,945 | 7 | 00s |
| `Outbox_Watch1x.cfg` | violates `NoLossSafety` | FAIL: NoLossSafety (as intended) | 195,431 | 97,616 | 9 | 01s |
| `Consolidation.cfg` | all hold | PASS | 10,864,684 | 2,426,544 | 18 | 47s |
| `Consolidation_Live.cfg` | all hold | PASS | 65,555 | 20,404 | 12 | 02s |
| `Consolidation_VolatileProposal.cfg` | violates `ExactlyOnceEffect` | FAIL: ExactlyOnceEffect (as intended) | 7,594 | 3,978 | 6 | 01s |
| `Consolidation_NonAtomicKey.cfg` | violates `ExactlyOnceEffect` | FAIL: ExactlyOnceEffect (as intended) | 1,136 | 719 | 5 | 00s |
| `Storage.cfg` | all hold | PASS | 9,687,211 | 1,211,703 | 33 | 05min 22s |
| `Storage_Update.cfg` | violates `ContentImmutable` | FAIL: ContentImmutable (as intended) | 12 | 12 | 3 | 00s |
| `Storage_FlipEarly.cfg` | violates `VectorGenerationConsistent` | FAIL: VectorGenerationConsistent (as intended) | 44 | 37 | 3 | 00s |
| `Storage_PurgeUnmarked.cfg` | violates `PurgeNeedsMarker` | FAIL: PurgeNeedsMarker (as intended) | 18 | 18 | 3 | 00s |
| `Storage_AutoRepair.cfg` | violates `RebuildBeforeRepair` | FAIL: RebuildBeforeRepair (as intended) | 1,080 | 492 | 7 | 00s |
| `Derivation.cfg` | all hold | PASS | 59,706,072 | 10,887,594 | 36 | 06min 00s |
| `Derivation_Page.cfg` | all hold | PASS | 14,364,460 | 2,900,212 | 34 | 01min 41s |
| `Derivation_Live.cfg` | all hold | PASS | 151,274 | 52,805 | 31 | 06s |
| `Derivation_NoLock.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 4,780 | 1,525 | 12 | 01s |
| `Derivation_CascadeEvidence.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 6,826 | 2,675 | 9 | 01s |
| `Derivation_PageNoVerify.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 40,841 | 11,067 | 13 | 02s |
| `Derivation_StaleProposal.cfg` | violates `NoDeletedDerivationServed` | FAIL: NoDeletedDerivationServed (as intended) | 60,556 | 16,259 | 16 | 01s |
| `Derivation_ReextractHides.cfg` | violates `ReextractKeepsDerivedVisible` | FAIL: ReextractKeepsDerivedVisible (as intended) | 885 | 343 | 7 | 00s |
| `Derivation_RestoreNoLock.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 800 | 261 | 11 | 00s |
| `Derivation_MatOnce.cfg` | violates `RestoreExact` | FAIL: RestoreExact (as intended) | 737 | 246 | 11 | 00s |
| `Derivation_PurgeDropsStub.cfg` | violates `FailClosed` | FAIL: FailClosed (as intended) | 9,013 | 2,657 | 13 | 01s |
| `Derivation_EffCited.cfg` | violates `AsOfNoLeak` | FAIL: AsOfNoLeak (as intended) | 135 | 47 | 7 | 00s |
| `Derivation_TombByDocId.cfg` | violates `NoOverHiding` | FAIL: NoOverHiding (as intended) | 69 | 46 | 4 | 00s |
| `Durability.cfg` | all hold | PASS | 148,847,658 | 32,742,996 | 25 | 04min 56s |
| `Durability_Chain.cfg` | all hold | PASS | 28,312,605 | 10,352,781 | 25 | 01min 24s |
| `Durability_IntentBeforeCommit.cfg` | violates `NoUnackedEffectOnLaterAck` | FAIL: NoUnackedEffectOnLaterAck (as intended) | 877 | 467 | 6 | 00s |
| `Durability_RaiseOnReopen.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 4,444,259 | 1,175,159 | 13 | 08s |
| `Durability_RetargetRestore.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 280,846 | 99,618 | 12 | 01s |
| `Durability_DupNoReput.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 36,424 | 15,883 | 10 | 01s |
| `Durability_AckNoRecheck.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 13,140 | 5,751 | 10 | 00s |
| `Durability_ReopenEarly.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 18,776 | 8,211 | 9 | 00s |
| `Durability_NarrowWindow.cfg` | violates `AckedDeleteSurvives` | FAIL: AckedDeleteSurvives (as intended) | 66,162 | 27,251 | 9 | 01s |
| `Durability_AckBeforeIntent.cfg` | violates `AckImpliesIntent` | FAIL: AckImpliesIntent (as intended) | 346 | 213 | 4 | 00s |
| `Durability_ClockOrder.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 1,274,796 | 519,169 | 14 | 04s |
| `Durability_UnorderedReplay.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 255,857 | 100,249 | 14 | 02s |
| `Durability_NoEpochGuard.cfg` | violates `IntentOrderLastWins` | FAIL: IntentOrderLastWins (as intended) | 12,495,475 | 4,612,704 | 18 | 27s |
| `ShardMove.cfg` | all hold | PASS | 126,216,549 | 23,255,160 | 30 | 05min 39s |
| `ShardMove_Live.cfg` | all hold | PASS | 7,653,478 | 1,355,212 | 26 | 01min 48s |
| `ShardMove_ActiveWriters.cfg` | all hold | PASS | 1,549,719 | 255,952 | 24 | 20s |
| `ShardMove_ZeroMargin.cfg` | all hold | PASS | 7,140,428 | 1,275,040 | 26 | 01min 21s |
| `ShardMove_UnfencedSteps.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 3,516,336 | 751,626 | 12 | 10s |
| `ShardMove_NoTimelineCheck.cfg` | all hold | PASS | 128,404,582 | 23,255,160 | 30 | 05min 40s |
| `ShardMove_NoReady.cfg` | violates `RollbackPossibleBeforeC` | FAIL: RollbackPossibleBeforeC (as intended) | 667,880 | 174,096 | 10 | 03s |
| `ShardMove_RestoreNoReconcile.cfg` | violates `RestoreReconciles` | FAIL: RestoreReconciles (as intended) | 173,887 | 47,688 | 9 | 01s |
| `ShardMove_NoVerify.cfg` | violates `NoLossNoDup` | FAIL: NoLossNoDup (as intended) | 8,121,260 | 1,963,744 | 12 | 20s |
| `ShardMove_StampAfterCut.cfg` | violates `OneOwner` | FAIL: OneOwner (as intended) | 954,961 | 246,474 | 10 | 04s |
| `ShardMove_IdKeyedRecopy.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 147,092 | 35,602 | 11 | 03s |
| `ShardMove_MergeNoDeletes.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 1,590,935 | 265,158 | 19 | 18s |
| `ShardMove_SweepNotPaused.cfg` | violates `MoveTerminatesActive` | FAIL: temporal property (as intended) | 2,796,561 | 418,928 | 19 | 28s |
| `ShardMove_CleanupNoBackup.cfg` | violates `CleanupSafe` | FAIL: CleanupSafe (as intended) | 154,998 | 38,161 | 15 | 01s |

Bounds, one line each (the constants are in the `.cfg` files):

- `Derivation.cfg`: 4 facts (`f1 = (d1,v1)` with re-extraction twin `f4`, `f2 = (d1,v2)`, `f3 = (d2,v1)`), 1 observation x 3 versions, 2 writers, 3 stored proposals, 1 delete, invalidate and restore (2 curation operations), a commit that may be retried (N144), symmetry on writers. **Reduced in D24:** REPLACE, re-extraction and the chunk and re-extraction purges (`MaxRetire`) moved to `Derivation_Retire.cfg` (1 observation x 2 versions, 2 proposals, 2 curation operations, 1 delete, 1 REPLACE or re-extraction): with `MaxRetire = 1` the D23 bounds reached 26 M distinct states in 18 minutes with the queue still growing and were stopped (the stamp `mat`, the pointer `cv` and the proposal's `exp` multiplied the product), and with 3 versions x 3 proposals even one curation operation did not finish in three minutes at 3 M states.
- `Derivation_Page.cfg`: 1 observation x 1 version and 1 page x 2 versions, 2 writers, 3 proposals, 1 delete, 1 curation operation, a commit that may be retried, no REPLACE or re-extraction (with them the product ran past 30 minutes and was stopped).
- `Derivation_Live.cfg`: 1 observation x 2 versions, 1 writer that never crashes (a commit may still be retried), 2 proposals, 2 deletes, 1 curation operation; safety plus `ExpungeCompletes`.
- `Derivation_CasBeforeIdem.cfg`, `_CascadeHidden.cfg`, `_MatSignalOnly.cfg` (D24): 1 observation x 2 versions and 1 proposal (`_CasBeforeIdem`, with `CasFirst` and a blind root rebuild), or 1 version, 1 proposal and 1 curation operation (the other two); each fails within 14 states.
- `ShardMove.cfg`: 2 rows (each a ledger row, an idempotency key and an event), **1 client** (D24; `_Live` and `_ActiveWriters` keep 2), writer lifetime 1, margin 1, 2 ticks, epochs <= 4, 1 restore or failover (lossy or lossless), 1 extra backup; the shard sequences start at 2 (`s1`) and 0 (`s2`). **Reduced in D24:** with 2 clients and 1 extra backup the D24 model passed 22 M distinct states at depth 19 in 9 minutes with the queue still growing (the D23 design had 23 M in total) and was stopped; the catalog restore and the promotion of the target (`TgtRestore`) are in `ShardMove_TgtRestore.cfg`. `_NoTimelineCheck` (an experiment with the same bounds) also has 1 client. `_Live`, `_ZeroMargin`: no extra backup. `_ActiveWriters`: no restore, no voluntary abort, no backup. `_CleanupNoBackup`: 1 row, 1 client. The first design runs with 3 ticks and 2 extra backups were stopped after ten minutes with the queue still growing.
- `ShardMove_Twice.cfg` and the second-move must-fail configurations (`_NoSeqAdvance`, `_NoSeqAdvanceTwice`): **1 row**, 1 client, 4 ticks, 2 moves, no restore, 1 backup (the second move waits for the cleanup, which needs the backup). With 2 rows the product passed a million states at depth 14 within a minute and was stopped (the second move is about 40 steps deep). `_TgtRestore`, `_CleanupTimeGate`: 1 row, 1 client, 3 ticks, 1 promotion of the target that loses 1 row, 2 (design) or 1 backups; `_TgtRestore` also has 1 catalog restore. `_CatalogLossNoShardTruth`: 1 row, 1 client, 1 catalog restore.
- `Durability.cfg`: shape `del, del, ret` on one document, 8 ticks (`MaxT = 7`), commit latency 1, margin 1, 2 restores, 1 catalog restore (D24: the floor goes back to its highest value). `Durability_Chain.cfg`: shape `inv, res, inv` on one fact, requests that may overlap (subject lock held through the intent put), clock skew 1, margin 2, `MaxT = 4`, 2 restores, no catalog restore (it changes nothing for a fact subject once the blob floor is in place). A `MaxT = 7` run of the chain shape was stopped at 47 M distinct states with the queue still growing.
- `Storage.cfg`: 3 rows in two namespaces on one partition (thresholds 1 and 2), 3 embedding generations. The must-fail `_PerIndexHygiene` uses 2 generations.
- `Consolidation.cfg`: 3 facts, 1 observation, 2 crashes, 1 delete (the 4-fact and 2-observation variants were stopped after about ten minutes with the queue still growing).
- `Outbox.cfg`: 3 writers, 2 namespaces, 2 consumers, 5 seqs, timeout 2, watch 4, 1 relay crash.
