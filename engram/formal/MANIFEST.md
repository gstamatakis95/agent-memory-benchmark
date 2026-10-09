# formal/MANIFEST.md

The must-fail manifest of the formal specifications (PLAN.md sections 7.4, 7.6, 8.4.6; register N46, N141, N156, N178,
N188). `scripts/formal-manifest-check.sh` reads this file; keep the table layout. Three parts: one row per TLC
configuration (79: `Outbox` 4, `Consolidation` 4, `Storage` 8, `Derivation` 21, `Durability` 18, `ShardMove` 24), the
mapping of every `.tla` and `.lean` file to its Go test files, and the first comment line of every configuration.

## 1. Configurations

`Expected` is the outcome `formal/tla/EXPECT` states: `pass` (a design configuration: "No error has been found") or
the one invariant a must-fail configuration violates. `Twin test` is the Go test of PLAN.md section 8.4.6 that will
pair the configuration: for a must-fail configuration the `faultinject` knob that disables the fix reproduces the
flaw and the fix proves its absence; for a design configuration it is the spec's model test (the plan does not pair
every row one to one; the choice is recorded in the report). `Status` is `pending(M1.x)`, the milestone whose
section 10.2 exit cell names the test (where section 10.2 is silent, the milestone that builds its subject, per
section 8.4.x), until that test exists; then `paired(path/to/file_test.go)`. The check fails a `pending` row whose
milestone is not in PROGRESS.md or is `done` there, and a `paired` row whose file lacks `func <Twin test>(`.

### Outbox (4)

|Config|Expected|Twin test|Status|
|---|---|---|---|
|Outbox|pass|TestOutbox_Watch1x|pending(M1.4)|
|Outbox_Live|pass|TestOutbox_Watch1x|pending(M1.4)|
|Outbox_NoWatch|NoLossSafety|TestOutbox_Watch1x|pending(M1.4)|
|Outbox_Watch1x|NoLossSafety|TestOutbox_Watch1x|pending(M1.4)|

### Consolidation (4)

|Config|Expected|Twin test|Status|
|---|---|---|---|
|Consolidation|pass|TestConsolidation_PersistedProposal|pending(M2.1)|
|Consolidation_Live|pass|TestConsolidation_TwoStage|pending(M2.1)|
|Consolidation_VolatileProposal|ExactlyOnceEffect|TestConsolidation_PersistedProposal|pending(M2.1)|
|Consolidation_NonAtomicKey|ExactlyOnceEffect|TestConsolidation_AtomicKey|pending(M2.1)|

### Storage (8)

|Config|Expected|Twin test|Status|
|---|---|---|---|
|Storage|pass|TestIndex_Hygiene|pending(M3.1)|
|Storage_Gens|pass|TestVectors_ModelGeneration|pending(M3.1)|
|Storage_Update|ContentImmutable|TestContent_InsertOnly|paired(internal/store/content_integration_test.go)|
|Storage_FlipEarly|VectorGenerationConsistent|TestVectors_ModelGeneration|pending(M3.1)|
|Storage_PurgeUnmarked|PurgeNeedsMarker|TestExpunge_Stages|pending(M1.10)|
|Storage_AutoRepair|RebuildBeforeRepair|TestIndex_Hygiene|pending(M3.1)|
|Storage_PerIndexHygiene|RebuildBeforeRepair|TestIndex_Hygiene|pending(M3.1)|
|Storage_PurgeDuringRebuildSet|RebuildBeforeRepair|TestIndex_Hygiene|pending(M3.1)|

### Derivation (21)

|Config|Expected|Twin test|Status|
|---|---|---|---|
|Derivation|pass|TestVisibility_AllSurfaces|pending(M1.3)|
|Derivation_Page|pass|TestPageRefresh_DeleteMidCall|pending(M2.1)|
|Derivation_Retire|pass|TestVisibility_SegmentHiding|pending(M1.3)|
|Derivation_Live|pass|TestExpunge_SweeperFindsUnmaterialized|pending(M1.3)|
|Derivation_NoLock|NoDeletedDerivationServed|TestExpunge_DerivationLock|pending(M1.10)|
|Derivation_CascadeEvidence|NoDeletedDerivationServed|TestVisibility_AllSurfaces|pending(M1.3)|
|Derivation_PageNoVerify|NoDeletedDerivationServed|TestPageRefresh_DeleteMidCall|pending(M2.1)|
|Derivation_StaleProposal|NoDeletedDerivationServed|TestApply_BaseVersionCAS|pending(M2.1)|
|Derivation_ReextractHides|ReextractKeepsDerivedVisible|TestReextract_Rebuilds|pending(M1.1)|
|Derivation_RestoreNoLock|RestoreExact|TestInvalidate_RestoreExact|pending(M1.3)|
|Derivation_MatOnce|RestoreExact|TestMaterialize_StampUnderLock|pending(M1.10)|
|Derivation_PurgeDropsStub|FailClosed|TestExpunge_Stages|pending(M1.10)|
|Derivation_EffCited|AsOfNoLeak|TestAsOf_ObservationVersions|pending(M2.3)|
|Derivation_TombByDocId|NoOverHiding|TestDelete_ReuseDocumentID|pending(M1.3)|
|Derivation_CasBeforeIdem|NoPhantomVersion|TestPageRefresh_CommitRetry|pending(M3.1)|
|Derivation_CascadeHidden|NoGhostInvalidatedServed|TestInvalidate_SurvivesChunkPurge|pending(M1.3)|
|Derivation_MatSignalOnly|MaterializeComplete|TestExpunge_SweeperFindsUnmaterialized|pending(M1.3)|
|Derivation_RestoreOnlySelf|RestoreExact|TestInvalidate_RestoreUndoesTwinSet|pending(M1.3)|
|Derivation_RestoreByVisibleTwin|RestoreExact|TestInvalidate_RestoreUndoesTwinSet|pending(M1.3)|
|Derivation_LazyTwinNoLock|RestoreExact|TestInvalidate_ConcurrentReextract|pending(M1.3)|
|Derivation_LazyTagFromLastLog|RestoreExact|TestInvalidate_RepeatedReusesTag|pending(M1.3)|

### Durability (18)

|Config|Expected|Twin test|Status|
|---|---|---|---|
|Durability|pass|TestRestore_ReplaysIntents|pending(M1.9)|
|Durability_Chain|pass|TestIntent_ConcurrentCurationOneChain|pending(M1.3)|
|Durability_IntentBeforeCommit|NoUnackedEffectOnLaterAck|TestRestore_NoUnackedEffect|pending(M1.9)|
|Durability_RaiseOnReopen|AckedDeleteSurvives|TestRestore_FloorInCatalog|pending(M1.9)|
|Durability_RetargetRestore|AckedDeleteSurvives|TestRestore_FloorInCatalog|pending(M1.9)|
|Durability_DupNoReput|AckedDeleteSurvives|TestIntent_DuplicateAttempt|pending(M1.9)|
|Durability_AckNoRecheck|AckedDeleteSurvives|TestIntent_AckRereadsMarker|pending(M1.9)|
|Durability_ReopenEarly|AckedDeleteSurvives|TestRestore_ReplaysIntents|pending(M1.9)|
|Durability_NarrowWindow|AckedDeleteSurvives|TestRestore_ReplaysIntents|pending(M1.9)|
|Durability_AckBeforeIntent|AckImpliesIntent|TestIntent_AckImpliesIntent|pending(M1.9)|
|Durability_ClockOrder|IntentOrderLastWins|TestRestore_ChainOrder|pending(M1.9)|
|Durability_UnorderedReplay|IntentOrderLastWins|TestRestore_ChainOrder|pending(M1.9)|
|Durability_NoEpochGuard|IntentOrderLastWins|TestIntent_EpochGuard|pending(M1.9)|
|Durability_CatalogLossNoBlobFloor|AckedDeleteSurvives|TestReplay_FloorFromBlob|pending(M1.9)|
|Durability_ReplayStampsCurrentEpoch|IntentOrderLastWins|TestIntent_EpochGuard|pending(M1.9)|
|Durability_NoSubjectLock|IntentOrderLastWins|TestIntent_ConcurrentCurationOneChain|pending(M1.3)|
|Durability_NoHelpPrev|IntentOrderLastWins|TestIntent_HelpPrev|pending(M1.9)|
|Durability_CatalogRestoreInFenceWindow|AckedDeleteSurvives|TestDelete_TenantAckAfterFence|pending(M1.3)|

### ShardMove (24)

|Config|Expected|Twin test|Status|
|---|---|---|---|
|ShardMove|pass|TestMove_RollbackEveryStep|pending(M1.5)|
|ShardMove_Live|pass|TestMove_WindowDeadlineRollback|pending(M1.5)|
|ShardMove_ActiveWriters|pass|TestMove_ActiveWriters|pending(M1.5)|
|ShardMove_Twice|pass|TestMove_TwiceAndExport|pending(M1.5)|
|ShardMove_TgtRestore|pass|TestMove_TargetRestoredAfterSeal|pending(M1.5)|
|ShardMove_TgtRestore2|pass|TestMove_TargetRestoredAfterSeal|pending(M1.5)|
|ShardMove_UnfencedSteps|NoLossNoDup|TestMove_CatalogCAS|pending(M1.5)|
|ShardMove_NoReady|RollbackPossibleBeforeC|TestMove_ReadyState|pending(M1.5)|
|ShardMove_RestoreNoReconcile|RestoreReconciles|TestRestore_OpenMoves|pending(M1.9)|
|ShardMove_NoVerify|NoLossNoDup|TestMove_VerifyFrozenCatchesFault|pending(M1.5)|
|ShardMove_StampAfterCut|OneOwner|TestMove_CatalogCAS|pending(M1.5)|
|ShardMove_SweepNotPaused|SourceStaticUnderFreeze|TestMove_FrozenCopy|pending(M1.5)|
|ShardMove_NoSeqAdvance|CopiedBelowTargetSeq|TestMove_TwiceAndExport|pending(M1.5)|
|ShardMove_CatalogLossNoShardTruth|NoLossNoDup|TestCatalog_ReconcileAfterEveryRestore|pending(M1.9)|
|ShardMove_UnionRepair|NoResurrect|TestMove_TargetRestoredAfterSeal|pending(M1.5)|
|ShardMove_CopyBeforeFreeze|NoLossNoDup|TestMove_FrozenCopy|pending(M1.5)|
|ShardMove_ReadyBeforeIndex|ServedFromIndex|TestMove_IndexesValidBeforeReady|pending(M1.5)|
|ShardMove_CutOnUnreplicatedCommit|NoLossNoDup|TestMove_CutWaitsForReplicatedCommit|pending(M1.5)|
|ShardMove_NoFloor|NoLossNoDup|TestMove_CutWaitsForSealedCopy|pending(M1.5)|
|ShardMove_LaggingFailover|NoLossNoDup|TestMove_FailoverBelowFloorRefused|pending(M1.7)|
|ShardMove_RestoreKeepsMove|MoveClosedWhenFinal|TestMove_TargetRestoredAfterSeal|pending(M1.5)|
|ShardMove_EndOnRouting|OneOwner|TestMove_ActivateAfterCatalogPromotion|pending(M1.5)|
|ShardMove_ThawOnUnreplicatedAbort|NoLossNoDup|TestMove_RollbackWaitsForReplicatedAbort|pending(M1.5)|
|ShardMove_CatalogLossDuringFreeze|CatalogNamesOwnerAfterDone|TestCatalog_ReconcileIgnoresIncomingEpoch|pending(M0.3)|

### Nightly-only configurations

`make formal-quick` (the PR job) runs the SANY parse of every specification and every configuration that is not listed
here. The configurations below run in `make formal` (nightly, 30 minutes each) and in the PR job when the PR touches
their specification or its mapped test files. The tiering is the outcome of the measurement in
`docs/briefs/reports/M0.7-report.md`; a configuration is listed with `- ` and its name, one per line.

- Derivation
- Derivation_Page
- Derivation_Retire
- Durability
- Durability_Chain
- ShardMove
- ShardMove_Live
- ShardMove_TgtRestore
- ShardMove_TgtRestore2
- Consolidation
- Outbox
- Storage
- ShardMove_Twice
- Durability_NoSubjectLock
- ShardMove_UnfencedSteps
- Durability_NoEpochGuard
- ShardMove_StampAfterCut
- Durability_ReplayStampsCurrentEpoch
- Durability_RaiseOnReopen

### Fast group of the quick tier

`scripts/formal-run.sh quick` runs the configurations below as up to four parallel one-worker TLC processes (JVM
start-up dominates them, each took at most 4 s in the M0.7 measurement) and every other quick-tier configuration one at
a time with all workers. A breadth-first search with one worker is deterministic, so a fast-group configuration always
writes the same counts and counterexample; the group is listed here, not derived from the wall time of the previous run,
so the worker count of a configuration does not flip with machine load. Every configuration still has its own log and
its own comparison with `EXPECT`. A name is listed with `- `, one per line, and must be a configuration that is not in
the nightly-only list (`scripts/formal-manifest-check.sh` checks both).

- Consolidation_Live
- Consolidation_NonAtomicKey
- Derivation_CasBeforeIdem
- Derivation_CascadeEvidence
- Derivation_CascadeHidden
- Derivation_EffCited
- Derivation_LazyTagFromLastLog
- Derivation_LazyTwinNoLock
- Derivation_MatOnce
- Derivation_MatSignalOnly
- Derivation_NoLock
- Derivation_PageNoVerify
- Derivation_ReextractHides
- Derivation_RestoreByVisibleTwin
- Derivation_RestoreNoLock
- Derivation_RestoreOnlySelf
- Derivation_TombByDocId
- Durability_AckBeforeIntent
- Durability_AckNoRecheck
- Durability_CatalogLossNoBlobFloor
- Durability_CatalogRestoreInFenceWindow
- Durability_DupNoReput
- Durability_IntentBeforeCommit
- Durability_NarrowWindow
- Durability_ReopenEarly
- Durability_RetargetRestore
- Durability_UnorderedReplay
- Outbox_NoWatch
- Outbox_Watch1x
- ShardMove_CatalogLossDuringFreeze
- ShardMove_CopyBeforeFreeze
- ShardMove_EndOnRouting
- ShardMove_NoSeqAdvance
- ShardMove_NoVerify
- ShardMove_ReadyBeforeIndex
- ShardMove_RestoreKeepsMove
- ShardMove_SweepNotPaused
- ShardMove_UnionRepair
- Storage_AutoRepair
- Storage_FlipEarly
- Storage_PerIndexHygiene
- Storage_PurgeDuringRebuildSet
- Storage_PurgeUnmarked
- Storage_Update

## 2. Specifications and their Go test files (PLAN.md section 7.4)

`scripts/formal-manifest-check.sh` fails a change to a file in the first column, or to a configuration of that
specification, unless at least one file matching a pattern in the second column changed too, or the change carries the
`formal-no-test-change` label with a justification (`FORMAL_NO_TEST_CHANGE` in the check). The first pattern is the
invariant code and its unit tests (M0.7); the others are the packages that build the twins.

|Specification|Test file patterns|
|---|---|
|formal/tla/Outbox.tla|internal/formal/outbox/*_test.go internal/outbox/*_test.go|
|formal/tla/Consolidation.tla|internal/formal/consolidation/*_test.go internal/consolidate/*_test.go|
|formal/tla/Storage.tla|internal/formal/storage/*_test.go internal/store/*_test.go|
|formal/tla/Storage.tla|internal/index/*_test.go|
|formal/tla/Derivation.tla|internal/formal/derivation/*_test.go internal/recall/*_test.go|
|formal/tla/Derivation.tla|internal/expunge/*_test.go internal/consolidate/*_test.go|
|formal/tla/Derivation.tla|internal/pages/*_test.go|
|formal/tla/Durability.tla|internal/formal/durability/*_test.go internal/intent/*_test.go|
|formal/tla/Durability.tla|cmd/engramctl/restore*_test.go|
|formal/tla/ShardMove.tla|internal/formal/shardmove/*_test.go internal/move/*_test.go|
|formal/tla/ShardMove.tla|internal/catalog/*_test.go|
|formal/lean/Engram/RRF.lean|internal/recall/*_test.go|
|formal/lean/Engram/Packer.lean|internal/recall/*_test.go|
|formal/lean/Engram/TagMatch.lean|internal/recall/*_test.go|
|formal/lean/Engram/TemporalWindow.lean|internal/recall/*_test.go|

## 3. First comment line of each configuration

The comment block that opens each `.cfg` (joined), as the check compares it with `EXPECT`: for a must-fail
configuration the invariant that `EXPECT` names must appear in it.

### Outbox

- Outbox: Design configuration, safety at full bounds (symmetry-reduced): Watch = 2 x Timeout. Expected: all
  invariants hold.  Liveness is checked in Outbox_Live.cfg.
- Outbox_Live: Design configuration, safety + liveness at reduced bounds, no symmetry (TLC's symmetry reduction is
  unsound for liveness). Expected: all hold.
- Outbox_NoWatch: Counterexample configuration: no watchlist (a gap is declared aborted on first sight). Expected:
  NoLossSafety is violated.
- Outbox_Watch1x: Counterexample configuration: horizon = 1 x Timeout (no margin). Expected: NoLossSafety is violated
  (writer commits on the last allowed tick).

### Consolidation

- Consolidation: Design configuration, safety (3 facts, 1 observation, 2 crashes, 1 delete). Expected: all hold.
  Liveness is in Consolidation_Live.cfg.
- Consolidation_Live: Design configuration, minimal bounds, no symmetry: safety + RoundTerminates (liveness).
- Consolidation_VolatileProposal: Counterexample: proposal kept only in the attempt's memory; op_key = (batch_key,
  index). Expected: ExactlyOnceEffect violated after a crash between two ops.
- Consolidation_NonAtomicKey: Counterexample: effect and consolidation_applied insert in separate transactions.
  Expected: ExactlyOnceEffect violated (an op applied twice).

### Storage

- Storage: Design configuration (safety + liveness), two namespaces on one partition (hygiene unit = partition, N152),
  rebuild as snapshot-then-publish, vacuum as start-then-collect, purges skip the partition while the runner holds its
  key (N166(6)); 3 rows, 2 embedding generations (D25: the 3-generation product with the rebuild and vacuum phases ran
  past 30 minutes, see Storage_Gens.cfg). Expected: all hold.
- Storage_Gens: Design configuration (safety + liveness): the D24 three embedding generations (two successive
  re-embeds), with one row in each of the two namespaces of the partition (thresholds 1 and 2), rebuild and vacuum
  phases, partition key. Expected: all hold.
- Storage_Update: Must fail: a content row may be rewritten in place -> ContentImmutable.
- Storage_FlipEarly: Must fail: Flip does not wait for the next-generation vectors -> VectorGenerationConsistent.
- Storage_PurgeUnmarked: Must fail: Purge needs no expunge marker -> PurgeNeedsMarker.
- Storage_AutoRepair: Must fail (N138): autovacuum repairs the graph in place before any rebuild (vacuum_index_cleanup
  left on) -> RebuildBeforeRepair.
- Storage_PerIndexHygiene: Must fail (N152): the hygiene unit is the index -- only the due graph is rebuilt, then the
  partition vacuum repairs the other graph in place -> RebuildBeforeRepair.
- Storage_PurgeDuringRebuildSet: Must fail (N166(6), P-8): purge batches ignore the partition key, so a row purged
  between the snapshot and the publish of a rebuild is a dead entry of the published graph that nothing counts, and
  the vacuum that follows repairs it in place -> RebuildBeforeRepair.

### Derivation

- Derivation: Design configuration, 1 observation x 3 versions, 2 writers, 3 proposals, one delete, invalidate and
  restore, a retried commit; REPLACE, re-extraction and the chunk purge are in Derivation_Retire.cfg. Expected: all
  invariants hold.
- Derivation_Page: Design configuration, 1 observation x 1 version and 1 page x 2 versions, 2 writers (REPLACE and
  re-extraction are in Derivation.cfg). Expected: all invariants hold.
- Derivation_Retire: Design configuration, 1 observation x 2 versions, 2 writers, 2 proposals, one delete, invalidate
  and restore, one REPLACE or re-extraction with the chunk and re-extraction purges, a retried commit (the product
  with 3 proposals ran past 30 minutes). Expected: all invariants hold.
- Derivation_Live: Design configuration (liveness): a pending tombstone is eventually done -- 1 observation x 2
  versions.
- Derivation_NoLock: Must fail: no derivation lock -- a writer that re-verified before the marker commits after
  Materialize -> NoDeletedDerivationServed.
- Derivation_CascadeEvidence: Must fail (C-1): evidence rows die with their fact (FK cascade); REPLACE, chunk purge,
  then DeleteDocument misses the derived version -> NoDeletedDerivationServed.
- Derivation_PageNoVerify: Must fail (C-2): CommitPageVersion re-verifies nothing; a refresh spanning a delete and its
  Materialize commits a visible page built from deleted content -> NoDeletedDerivationServed.
- Derivation_StaleProposal: Must fail (C-3): a stored update is applied without checking its base_version; after a
  root rebuild it takes the new root and carries hidden text into a visible segment -> NoDeletedDerivationServed.
- Derivation_ReextractHides: Must fail (C-6): derived versions are hidden by fact_hidden(reextract) ->
  ReextractKeepsDerivedVisible.
- Derivation_RestoreNoLock: Must fail (C-7): Restore without the exclusive derivation lock; a Materialize batch's
  scan, Restore, then the batch's write leaves a permanent row -> RestoreExact.
- Derivation_MatOnce: Must fail (C-7): Materialize reads fact_hidden once instead of in every batch under the lock ->
  RestoreExact.
- Derivation_PurgeDropsStub: Must fail (N136): DerivedPurge deletes the version row instead of leaving a stub; the
  version number is reused -> FailClosed.
- Derivation_EffCited: Must fail: effective_at counts only cited facts, not all shown facts -> AsOfNoLeak.
- Derivation_TombByDocId: Must fail: a tombstone hides the whole document id, so the re-used id (f2) is hidden by the
  old tombstone -> NoOverHiding.
- Derivation_CasBeforeIdem: Must fail (N144, C-1): the commit runs the base compare-and-set before its idempotency
  check and a root rebuild is blind; a retried commit advances current_version to a version that does not exist ->
  NoPhantomVersion.
- Derivation_CascadeHidden: Must fail (N145, C-2): purging a retired fact also drops its fact_hidden(invalidate) row
  (the old foreign key); the derived text it hid is served again -> NoGhostInvalidatedServed.
- Derivation_MatSignalOnly: Must fail (N145, C-2): Materialize is enabled only by the post-ack signal, which can be
  lost; an unstamped invalidation is then never found -> MaterializeComplete.
- Derivation_RestoreOnlySelf: Must fail (N162(2), C-4): Restore(g) un-hides only g; the twin that Invalidate hid with
  it (same document, same content hash) stays hidden -> RestoreExact.
- Derivation_RestoreByVisibleTwin: Must fail (N162(2), C-4): Restore resolves the twin set by current state (the twins
  not hidden for another cause) instead of by the invalidation id; the twin that is also hidden by re-extraction keeps
  its invalidate row -> RestoreExact.
- Derivation_LazyTwinNoLock: Must fail (N174(1), C7-4): Invalidate and Restore take no exclusive document lock, so
  they interleave with a lazy twin's read and commit: the twin is born hidden under a tag that Restore already removed
  (or a later Invalidate replaced) -> RestoreExact.
- Derivation_LazyTagFromLastLog: Must fail (N174(2), PG7-5): the lazy twin takes the tag of the curation_log's last
  invalidate action and a repeated Invalidate logs a fresh operation id although its rows keep the old one; the twin
  then carries a tag that Restore of the subject does not resolve -> RestoreExact.

### Durability

- Durability: Design configuration, document subject (del, duplicate del, ret) and a tenant delete (N182: the ack
  follows the last fence; one tenant, two namespaces). Expected: all invariants hold.
- Durability_Chain: Design configuration, fact subject (inv, res, inv) with host clocks skewed by one tick. Expected:
  all invariants hold.
- Durability_IntentBeforeCommit: Must fail (C-5): the intent is put before the marker commits, so an orphan intent
  (failed attempt, concurrent duplicate) is replayed against later state -> NoUnackedEffectOnLaterAck.
- Durability_RaiseOnReopen: Must fail (C-4): the replay floor is raised when a replay completes (N134 as first
  written); a second restore loses the replayed commit and skips the intent -> AckedDeleteSurvives.
- Durability_RetargetRestore: Must fail: each restore sets the floor to its own target; a second restore while the
  first replay is pending skips lost intents -> AckedDeleteSurvives.
- Durability_DupNoReput: Must fail: a retry that finds the committed marker acks without re-putting its intent (crash
  between commit and put) -> AckedDeleteSurvives.
- Durability_AckNoRecheck: Experiment (expected to FAIL): the ack path does not re-read the marker after the intent
  put; a restore between commit and put, replay, and a late put ack a delete that is not in force ->
  AckedDeleteSurvives (design flaw the model exposed).
- Durability_ReopenEarly: Must fail: reads reopen before replay finishes -> AckedDeleteSurvives.
- Durability_NarrowWindow: Must fail: replay margin 0 < commit latency 2 -> AckedDeleteSurvives.
- Durability_AckBeforeIntent: Must fail: the ack may precede the intent put -> AckImpliesIntent.
- Durability_ClockOrder: Must fail (C-16): replay in (deleted_at, id) order with host clocks skewed by one tick ->
  IntentOrderLastWins.
- Durability_UnorderedReplay: Must fail: replay in arbitrary order -> IntentOrderLastWins (a later Invalidate
  overtaken by an older Restore).
- Durability_NoEpochGuard: Must fail: replay ignores the epoch recorded in the intent; the late intent of an op whose
  commit a restore lost is replayed over a later acknowledged write -> IntentOrderLastWins (design flaw the model
  exposed).
- Durability_CatalogLossNoBlobFloor: Must fail (N146, C-3): the catalog is restored from a backup and its replay floor
  goes back up; replay reads the catalog floor only, so a second restore misses the deletes the first replay
  re-applied -> AckedDeleteSurvives.
- Durability_ReplayStampsCurrentEpoch: Must fail (N150, C-7): a replayed marker is stamped with the bumped epoch, so
  the epoch guard skips the second intent of the subject (the Restore after an Invalidate) -> IntentOrderLastWins.
- Durability_NoSubjectLock: Must fail (N150, C-8): overlapping requests on one fact read the same prev_operation_id
  (no subject lock); the chain forks and replay applies the two children in an unspecified order ->
  IntentOrderLastWins.
- Durability_NoHelpPrev: Must fail (N150, found by the model): overlapping requests with the subject lock but no help
  for a missing predecessor intent -- a marker whose writer crashed between commit and put is a hole in the chain, and
  its successor replays before older intents -> IntentOrderLastWins.
- Durability_CatalogRestoreInFenceWindow: Must fail (N182, C8-5): DeleteTenant is acknowledged after the catalog
  `deleting` row and the tenant intent but before the namespaces are fenced; a catalog restore inside that window
  reverts the row, the tenant is active again and the acknowledged delete is lost -> AckedDeleteSurvives.

### ShardMove

- ShardMove: Design configuration (safety, one client): freeze-then-copy with a copy fault, VerifyFrozen, BuildIndex,
  the seal, the replicated-LSN gate, a restore (to the initial backup), a failover or a lossless failover of either
  shard during a move, a catalog loss, promotion or restore, deletes on the active target, cleanup gate. Expected: all
  invariants hold.
- ShardMove_Live: Design configuration, reduced bounds, with liveness: a started move completes or rolls back, and a
  frozen move reaches the commit point or rolls back. Expected: all hold.
- ShardMove_ActiveWriters: Design configuration (liveness): writers run before the freeze and after activation (never
  under it), with source-side key sweeps, no restore or voluntary abort; every started move completes or rolls back
  (the window may expire) and no deleted row is resurrected. Expected: all hold.
- ShardMove_Twice: Design configuration (safety + liveness), two moves of the namespace (one row) (the second from the
  first target, after its cleanup and a backup), young target shard: every started move completes and every ready or
  active shard's sequence is above its rows (N147). Expected: all hold.
- ShardMove_TgtRestore: Design configuration: one restore, failover or promotion of the target after the commit point
  (to a backup or the standby's image at or past the floor: an ordinary restore at a new epoch, N179, N180), one
  catalog event (loss, promotion or restore), deletes on the active target, the cleanup gate is the seal. Expected:
  all invariants hold.
- ShardMove_TgtRestore2: Design configuration (N178): as _TgtRestore with two moves of the namespace, so the restored
  target's row can be a previous life's `moved_out`; the second move runs from the first target after its cleanup.
  Expected: all invariants hold.
- ShardMove_UnfencedSteps: Must fail: mover steps do not check the source and target rows they act on; a source
  restored after Freeze lets the mover finish the copy from reverted data, and recovery completes the committed move
  -> NoLossNoDup.
- ShardMove_NoReady: Must fail: no ready state (old order: target active before the CAS, catalog may flip first): a
  write lands at the target while Rollback is still possible -> RollbackPossibleBeforeC.
- ShardMove_RestoreNoReconcile: Must fail: restore trusts its backup row and bumps the epoch during an open move ->
  RestoreReconciles.
- ShardMove_NoVerify: Must fail: VerifyFrozen compares nothing and the copy drops a row (CopyFault undetected) ->
  NoLossNoDup.
- ShardMove_StampAfterCut: Must fail (C-10): the catalog stamp comes after (c) and the restore reconcile does not
  re-derive the move from the ownership rows (ShardTruth = FALSE) -> OneOwner.
- ShardMove_SweepNotPaused: Must fail (C-8a): the expiring-class sweep keeps running during a move, also under the
  freeze, so the source changes while it is copied -> SourceStaticUnderFreeze.
- ShardMove_NoSeqAdvance: Must fail (N147, C-4): the target's sequence is not advanced at the freeze; the moved rows
  are above it -> CopiedBelowTargetSeq.
- ShardMove_CatalogLossNoShardTruth: Must fail (N146, C-3, N163): the catalog is restored from backup after (c) and
  the restore reconcile trusts the catalog (open) instead of re-deriving it from the ownership rows; the move is
  rolled back after the source was cut -> NoLossNoDup.
- ShardMove_UnionRepair: Must fail (N161): any repair of a restored, serving target as store[Tgt] u store[Src] (the
  D24 ReconcileIn) re-adds a row the target deleted -> NoResurrect.
- ShardMove_CopyBeforeFreeze: Must fail (N160): the copy runs from the unfrozen source with no catch-up and no
  verification under the freeze; rows committed after the copy are lost -> NoLossNoDup.
- ShardMove_ReadyBeforeIndex: Must fail (N160(5)): the target becomes ready without its index (readiness is not the
  index) -> ServedFromIndex.
- ShardMove_CutOnUnreplicatedCommit: Must fail (N163(3)): (c) runs before the standby has the catalog commit; the
  promotion loses the commit and the source is then restored, the restore rolls the move back after the source was cut
  and the target copy is wiped -> NoLossNoDup.
- ShardMove_NoFloor: Must fail (N179, N169(1)): (b') without the seal (no floor, no wait for the standby); the target
  is restored after the commit point to a backup that predates the copy, and the restore path activates it without the
  moved rows -> NoLossNoDup.
- ShardMove_LaggingFailover: Must fail (N179(3), C8-3): the target's standby is rebuilt after the seal and promoted
  after the commit point without the floor check; it lacks the copy, and the restore path activates the target without
  the moved rows -> NoLossNoDup.
- ShardMove_RestoreKeepsMove: Must fail (N180(1), C8-10): restore_done does not close the move bit of a restored
  target; the move ends `done` with a move_id still set on the ownership row -> MoveClosedWhenFinal.
- ShardMove_EndOnRouting: Must fail (N180(3), C8-2): the mover ends `done` as soon as the routing row names the
  target; a catalog promotion after (c) makes its reconcile write that row while the target row is still `ready`, so
  no shard is active -> OneOwner.
- ShardMove_ThawOnUnreplicatedAbort: Must fail (N171(2), C7-1): the thaw does not wait for the replicated
  `rolled_back`; the promotion loses the abort, the move is open again and a later restore or reconcile acts on it ->
  NoLossNoDup.
- ShardMove_CatalogLossDuringFreeze: Must fail (N172, PG7-3): a catalog promotion while the source is frozen; the
  reconcile takes the epoch from every ownership row, including the target's incoming/ready row at e+1, so the catalog
  names the owner at an epoch it does not hold and (d) never applies -> CatalogNamesOwnerAfterDone.
