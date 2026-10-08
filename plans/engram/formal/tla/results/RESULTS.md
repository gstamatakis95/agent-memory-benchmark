# TLC results

Real runs of every configuration under `formal/tla/`, TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21,
4 workers, 10 GB heap, 4 cores, 30-minute cap. Raw logs are the `<config>.log` files next to this file.
`expected` is what the configuration is for: a design configuration must end with "No error has been found";
a must-fail configuration must end with the named invariant violated; `generated`/`distinct`/`depth` are TLC's
own counters (for a violation, the work done before the counterexample was found; `depth` is then the number of states in the
counterexample, otherwise the depth of the complete state graph). Section 7 of the plan interprets the results.

| config | expected | result | generated | distinct | depth | time |
|---|---|---|---|---|---|---|
| `Outbox.cfg` | all hold | PASS | 17,243,719 | 5,557,863 | 39 | 01min 35s |
| `Outbox_Live.cfg` | all hold | PASS | 1,342,866 | 486,937 | 31 | 38s |
| `Outbox_NoWatch.cfg` | violates `NoLossSafety` | FAIL as intended: NoLossSafety | 5,526 | 3,721 | 7 | 00s |
| `Outbox_Watch1x.cfg` | violates `NoLossSafety` | FAIL as intended: NoLossSafety | 233,325 | 113,111 | 9 | 01s |
| `Consolidation.cfg` | all hold | PASS | 10,864,684 | 2,426,544 | 18 | 56s |
| `Consolidation_Live.cfg` | all hold | PASS | 65,555 | 20,404 | 12 | 02s |
| `Consolidation_VolatileProposal.cfg` | violates `ExactlyOnceEffect` | FAIL as intended: ExactlyOnceEffect | 8,238 | 4,351 | 6 | 01s |
| `Consolidation_NonAtomicKey.cfg` | violates `ExactlyOnceEffect` | FAIL as intended: ExactlyOnceEffect | 523 | 424 | 5 | 00s |
| `Storage.cfg` | all hold | PASS | 12,872,199 | 1,478,807 | 36 | 06min 40s |
| `Storage_Update.cfg` | violates `ContentImmutable` | FAIL as intended: ContentImmutable | 16 | 16 | 3 | 00s |
| `Storage_FlipEarly.cfg` | violates `VectorGenerationConsistent` | FAIL as intended: VectorGenerationConsistent | 36 | 30 | 3 | 00s |
| `Storage_PurgeUnmarked.cfg` | violates `PurgeNeedsMarker` | FAIL as intended: PurgeNeedsMarker | 23 | 21 | 3 | 00s |
| `Derivation.cfg` | all hold | PASS | 7,659,307 | 2,133,161 | 29 | 01min 35s |
| `Derivation_Page.cfg` | all hold | PASS | 81,273,512 | 22,215,959 | 28 | 16min 53s |
| `Derivation_Live.cfg` | all hold | PASS | 314,063 | 87,830 | 24 | 11s |
| `Derivation_RestoreNoLock.cfg` | all hold | PASS | 8,115,973 | 2,159,112 | 29 | 01min 34s |
| `Derivation_NoLock.cfg` | violates `NoDeletedDerivationServed` | FAIL as intended: NoDeletedDerivationServed | 2,595 | 1,125 | 9 | 01s |
| `Derivation_MatInvalid.cfg` | violates `RestoreExact` | FAIL as intended: RestoreExact | 9,669 | 3,905 | 11 | 01s |
| `Derivation_EffCited.cfg` | violates `AsOfNoLeak` | FAIL as intended: AsOfNoLeak | 904 | 449 | 6 | 00s |
| `Derivation_TombByDocId.cfg` | violates `NoOverHiding` | FAIL as intended: NoOverHiding | 106 | 74 | 4 | 00s |
| `Durability.cfg` | all hold | PASS | 117,776,042 | 26,113,068 | 25 | 04min 26s |
| `Durability_AckBeforeIntent.cfg` | violates `AckImpliesIntent` | FAIL as intended: AckImpliesIntent | 410 | 308 | 4 | 00s |
| `Durability_ReopenEarly.cfg` | violates `AckedDeleteSurvives` | FAIL as intended: AckedDeleteSurvives | 37,706 | 14,919 | 9 | 01s |
| `Durability_UnorderedReplay.cfg` | violates `IntentOrderLastWins` | FAIL as intended: IntentOrderLastWins | 3,750,201 | 1,131,357 | 14 | 11s |
| `Durability_NarrowWindow.cfg` | violates `AckedDeleteSurvives` | FAIL as intended: AckedDeleteSurvives | 167,915 | 59,829 | 11 | 02s |
| `Durability_RetargetRestore.cfg` | violates `AckedDeleteSurvives` | FAIL as intended: AckedDeleteSurvives | 663,981 | 225,321 | 11 | 04s |
| `ShardMove.cfg` | all hold | PASS | 88,022,416 | 16,472,880 | 28 | 03min 57s |
| `ShardMove_Live.cfg` | all hold | PASS | 2,760,289 | 578,804 | 25 | 31s |
| `ShardMove_ZeroMargin.cfg` | all hold | PASS | 2,780,467 | 582,960 | 25 | 30s |
| `ShardMove_UnfencedSteps.cfg` | all hold | PASS | 142,521,064 | 21,636,679 | 28 | 05min 49s |
| `ShardMove_NoTimelineCheck.cfg` | all hold | PASS | 90,507,928 | 16,472,880 | 28 | 03min 55s |
| `ShardMove_NoReady.cfg` | violates `RollbackPossibleBeforeC` | FAIL as intended: RollbackPossibleBeforeC | 29,389 | 8,505 | 9 | 01s |
| `ShardMove_RestoreNoReconcile.cfg` | violates `RestoreReconciles` | FAIL as intended: RestoreReconciles | 26,869 | 7,923 | 8 | 01s |
| `ShardMove_NoVerify.cfg` | violates `NoLossNoDup` | FAIL as intended: NoLossNoDup | 587,653 | 159,846 | 10 | 03s |

Bounds, in one line each (the constants are in the `.cfg` files):

- `Derivation.cfg`: 3 facts (`f1 = (d1,v1)`, `f2 = (d1,v2)`, `f3 = (d2,v1)`), 2 observations x 3 versions, 2 deletes, 2 curation operations, 5 routed writes, 1 writer, symmetry on observations.
- `Derivation_Page.cfg`: 1 observation x 2 versions, 1 page x 2 versions, 2 deletes, 2 curation operations, 5 routed writes.
- `Derivation_Live.cfg`: 1 observation x 2 versions, 4 routed writes, safety plus `ExpungeCompletes`.
- `ShardMove.cfg`: 3 rows, 2 clients, writer lifetime 1, margin 1, 3 ticks, epochs <= 4, 1 restore/failover, 1 extra backup.
- `Durability.cfg`: 3 operations, 5 ticks, `Lat` 2, `Margin` 2, 2 restores.
- `Storage.cfg`: 3 rows, 3 embedding generations.
- `Consolidation.cfg`: 3 facts, 1 observation, 2 crashes, 1 delete (the 4-fact and 2-observation variants were stopped after about ten minutes with the queue still growing).
- `Outbox.cfg`: 3 writers, 2 namespaces, 2 consumers, 5 seqs, timeout 2, watch 4, 1 relay crash.
