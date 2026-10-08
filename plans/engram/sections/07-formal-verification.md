## 7. Formal verification

Scope: the six TLA+ specifications under `formal/tla/` and the four Lean 4 modules under `formal/lean/Engram/`;
what TLC reported on them after the round-4 rewrite (D23, N141); how the Go code is kept faithful to them; what is
not formalised; how it is wired into CI. Every number below is copied from a log in `formal/tla/results/` (summary
in `RESULTS.md`, which also holds the full table).

Tooling: TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores, 30-minute cap per
configuration. Lean 4 is not available in the planning environment; the Lean files and the Lake skeleton are
**not type-checked**.

### 7.1 What is modelled, and the convention

| Spec | Register | Question the model answers | Prose mechanisms this spec omits (C-21) |
|---|---|---|---|
| `Derivation.tla` | N113, N115 to N121, N133, N135, N136 | Can a deleted, invalidated or superseded fact reach a reader through a fact, an observation version or a page version, at any `as_of`, with the markers as the only synchronous write, and is nothing else hidden? | LLM text (a version's text is its evidence set plus its base's); `proof_count` and retirement; the marker-set size alerts; `stale_write`/`stale_delete` flags; the pacing of Materialize and Purge; one fact per chunk; the consolidation scheduler |
| `ShardMove.tla` | D5, N123 to N125, N137 | Dirty copy, freeze, reconcile by insertion sequence, the catalog CAS as point of no return, cleanup, restore and failover during a move: one owner, no loss, no duplicate, a started move completes. | One namespace; blob copy and `return_abort`; reads at `frozen/move`; drain of workflows; the relay's cursor wait; the 24 h timer (cleanup is an action); the timeline check is a guard, not a model of a zombie primary; PITR to before a move-in (C-22) |
| `Durability.tla` | N122, N134 | Is every acknowledged delete or invalidation still in force when reads reopen after a restore or failover, and is no unacknowledged effect applied over a later acknowledged write? | Tenant and namespace intents (one subject kind per config); the catalog `deleting` edge; the 35-day retention; intent content is a set of versions, not `up_to_version` arithmetic |
| `Storage.tla` | N111 to N113, N138 | Content rows never change after insert, a model flip never exposes a row without a vector, dead index entries leave only through a rebuild, the index converges. | HNSW internals and recall; the index runner's lease; partition layout |
| `Outbox.tla` | D6, N80 | No committed event is lost through sequence gaps; per-namespace order; the 2 x timeout watch horizon. | Kafka, consumer lag, batching |
| `Consolidation.tla` | N43, N121 | A round's decisions are applied exactly once under at-least-once activities and crashes. | The persisted-proposal `attempt` key and the all-skip stamp (C-11) are tested, not modelled; `Derivation.tla` carries the base check |

Each spec has *design* configurations (every knob at the register's value) that must end with "No error has been
found", and *must-fail* configurations in which one knob is set to the plausible simplification or to the round-4
reviewer's variant; each must end with exactly the invariant named in its first comment line violated, and lists
only that invariant. A design that passes only because the simplification was never tried is not evidence, so the
must-fail configurations are part of the deliverable and run in CI (N141). Bounds were shrunk until every design
configuration finished in under 30 minutes (the longest, `Derivation.cfg`, `ShardMove.cfg` and `Storage.cfg`, take between five and six minutes).

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
`MatEnd`, `Purge`, `DerivedPurge` (stubs). `Served(T)` is the SQL rule of N117: the version current at `T`, served
only if visible. A ghost copy of each version's real derivation (its inputs plus its base's) states the truth the
invariants are checked against.

*Invariants.* `NoDeletedDerivationServed`, `NoInvalidatedDerivationServed`, `RestoreExact`, `NoOverHiding`,
`AsOfNoLeak`, `MaterializeComplete`, and the round-4 additions `EvidenceOutlivesFacts`, `BaseCurrentAtCommit`,
`NoLostRebuild`, `NoVictimTextAfterPurge`, `FailClosed`, `ReextractKeepsDerivedVisible`; liveness
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

#### 7.2.2 `ShardMove.tla`: dirty copy, freeze, reconcile, cutover, restore

*Model.* Source `s1` and target `s2`; per shard a set of insert-only rows (each carries an `ins_seq` drawn at
`Begin` and, for `old` rows, a created-at/entity key from before the copy), a set of mutable keys (idempotency
keys, swept by the source at any time) and a counter for the expiring class (its sweep pauses while a move is
open). The catalog carries the move row `cm` (`open`, `committed`, `rolled_back`, `done`) and the namespace
`(shard, epoch)`. Mover: `Plan`, `CopyRange`/`CopyMk`/`CopyEx` (any part, any order), `EndCopy` (pre-freeze
verification, `T_pre`), `Freeze`, `Reconcile` (re-copy insert-only rows with `ins_seq >= T_pre - Margin`,
merge-diff the mutable class including deletes, `count <=` for the expiring class, then the count/hash
`Verified`), `MakeReady` (b'), `CommitCAS` (the catalog CAS, a''), `Cut` (c, only after `committed`), `Activate`
(b''), `CatFlip` (d, `WHERE epoch = e`), `Cleanup` (needs a post-activation backup of the target), `Rollback`
(abort CAS `open -> rolled_back`), `SrcDelete` and `SweepEx`, writers with a bounded lifetime. Recovery: `Restore`
(to the last backup, or lossless failover), `Reconcile_` (open: abort CAS and roll back; `committed`: complete
(c), (b''), (d) and skip the epoch bump; no move: owner bump), `RestoreDone`.

*Invariants.* `SingleWriter`, `NoLossNoDup`, `RollbackPossibleBeforeC`, `NoWriteToTargetBeforeC`,
`NoRouteToTargetBeforeC`, `ZombieCannotCutOver`, `RestoreReconciles`, and the round-4 additions `OneOwner`,
`CatalogNamesOwnerAfterDone`, `CleanupSafe`; liveness `MoveTerminates` (done or rolled back) and
`MoveTerminatesActive` (done, with writers, sweeps and old-id inserts but no restore and no voluntary abort).

*Configurations.* Design: `ShardMove.cfg` (2 rows, 2 clients, writer lifetime 1, margin 1, 2 ticks, 1 restore or
failover, 1 extra backup), `_Live`, and **`_ActiveWriters`** (every started move completes). Experiments that must
pass: `_ZeroMargin` (margin 0 with the verify can only cost a rollback) and `_NoTimelineCheck`.
Must-fail: `_NoReady`, `_RestoreNoReconcile`, `_NoVerify` (kept), `_UnfencedSteps` (now fails, item 4), and

| Config | Knob | Violates | Finding |
|---|---|---|---|
| `_IdKeyedRecopy` | re-copy by created_at / entity id | `MoveTerminatesActive` | C-8, P-1: rows inserted under old ids are missed, the verify rolls the move back every time |
| `_MergeNoDeletes` | mutable class merged by upserts | `MoveTerminatesActive` | C-8a: a swept key stays on the target |
| `_SweepNotPaused` | expiring-class sweep runs during a move | `MoveTerminatesActive` | C-8a |
| `_StampAfterCut` | (c) first, the catalog stamped afterwards | `OneOwner` | C-10: a failover between them rolls back a target whose source is already `moved_out`: no owner |
| `_CleanupNoBackup` | cleanup without a post-activation backup of the target | `CleanupSafe` | C-21: after a restore of the target to an older backup the moved rows exist only on the source; cleanup then deletes the last copy |

*What the model showed.*

1. **Safety does not depend on the re-copy key; completion does.** `_IdKeyedRecopy`, `_MergeNoDeletes` and
   `_SweepNotPaused` never lose a row (the count/hash verify blocks (b')); they roll back deterministically, which
   `MoveTerminates` hides and `MoveTerminatesActive` exposes. The ins_seq key with a margin of at least the writer
   lifetime is what makes the active-writer move complete (`_ActiveWriters`).
2. **The catalog CAS before (c) is what makes a restore during cutover decidable.** With the stamp after (c)
   (`_StampAfterCut`) the restore reads `open` for a source that is already `moved_out` and has no legal edge back.
   With the CAS the reconcile either aborts (`open -> rolled_back`) or completes the move itself (`committed`),
   without the epoch bump that would leave the catalog naming the source.
3. **Cleanup is only safe after a backup that contains the moved rows** (`_CleanupNoBackup`; N125 and §5.5.1 say
   "24 h after done"; they must also say "and after a differential backup of the target taken after activation").
4. **The row-state guard on every mover step became load-bearing** (a change from D22, where `_UnfencedSteps`
   passed). With the catalog CAS completing a `committed` move on restore, a mover that ignores the ownership rows it
   acts on can finish its copy from a source that was restored after Freeze and lost a row; recovery then completes
   the move with that smaller set (`NoLossNoDup`). Every step, the CAS included, must be a compare-and-set on the
   source and target rows it verified (`WHERE state = expected`). The count/hash verify stays load-bearing
   (`_NoVerify` loses a row; `_ZeroMargin` only rolls back); `_NoTimelineCheck` still changes no result, so the
   timeline comparison stays as the register decided and the case it covers is outside the model.

#### 7.2.3 `Durability.tla`: acknowledged deletes survive restore and failover

*Model.* Operations come from a fixed shape: on a document, delete, a duplicate delete and a retain (which revives
the document); on a fact, invalidate, restore, invalidate. The write path is `Commit` (the marker, a local commit
that a restore may lose) then `PutIntent` (after the commit; a duplicate that finds the committed marker re-puts
that marker's own intent) then `Ack` (after re-reading the marker); failures: `Abort`, `CrashBeforePut`,
`LostAck`. The intent records the marker's effect (the set of versions a delete covers), `prev_operation_id` and the
epoch. `RestoreTo(p)` keeps commits up to `p`, lowers the replay floor (held in the catalog, outside the restorable
state, never raised), bumps the epoch; `Replay` applies in-window intents verbatim in chain order; `Reopen` waits
for replay. Hosts' clocks may skew the recorded `deleted_at` by one tick.

*Invariants.* `AckImpliesIntent`, `AckedDeleteSurvives`, `IntentOrderLastWins`, `NoUnackedEffectOnLaterAck`.

*Configurations.* Design: `Durability.cfg` (document subject, `MaxT = 7`, 2 restores, margin 1, latency 1),
`Durability_Chain.cfg` (fact subject, clocks skewed by one tick, `MaxT = 4`). Must-fail: `_IntentBeforeCommit`
(C-5), `_RaiseOnReopen` (C-4), `_ClockOrder` (C-16), `_DupNoReput`, `_AckNoRecheck`, `_NoEpochGuard`, and from D22
`_RetargetRestore`, `_ReopenEarly`, `_AckBeforeIntent`, `_NarrowWindow`, `_UnorderedReplay`.

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

#### 7.2.4 `Storage.tla`: insert-only content, vector generations, index hygiene

Content rows are never rewritten and are purged only under a marker; a model flip waits for the next generation's
vectors; the index follows the vector table. New in D23 (N138): the purge counts the dead entries it leaves
(`pc = purged_since_build`), a rebuild at the threshold is the only thing that removes them
(`vacuum_index_cleanup = off`), and `AutoRepair` models autovacuum cleaning the graph in place. Invariants
`ContentImmutable`, `PurgeNeedsMarker`, `VectorGenerationConsistent`, `RebuildBeforeRepair`, `DeadCounted`,
liveness `IndexConvergence`. Must-fail: `Storage_Update`, `_FlipEarly`, `_PurgeUnmarked`, and `_AutoRepair`
(`RebuildBeforeRepair`).

#### 7.2.5 `Outbox.tla` and `Consolidation.tla`

Unchanged by D23 and re-run. `Outbox.tla` (D6): writers draw `seq` in their transaction and commit out of order; a
relay with a persisted cursor and a gap watchlist declares a missing `seq` aborted after `Watch` ticks
(invariants `NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered`, `IdempotentConsumerState`, liveness
`NoLossLive`); `Outbox_NoWatch` and `Outbox_Watch1x` (horizon 1 x timeout) violate `NoLossSafety`.
`Consolidation.tla` (D12, N43, N121): stage 1's decisions are persisted write-once under `batch_key`, each op's
effect and its `op_key` row are one transaction, the worker may crash anywhere; invariants `ExactlyOnceEffect`,
`ObservationHasSources`, liveness `RoundTerminates`; `_VolatileProposal` and `_NonAtomicKey` violate
`ExactlyOnceEffect`. The proposal lifecycle of C-11 (`(batch_key, attempt)` keys, no `DELETE`, batch `applied`
state) is below the model's abstraction and is covered by `TestConsolidation_*` (8.4.6).

#### 7.2.6 Boundaries of the results

1. **Abstractions that matter.** One namespace, one fact per chunk, two to three writers, version caps of one to
   three; restores are "revert to the last backup" or lossless failover, not PITR; the mutable and expiring
   classes are sets and a counter. A bound bump is a spec change reviewed as such.
2. **Experiments that pass by design** (`_ZeroMargin`, `_NoTimelineCheck`) are not gates for the property they name:
   they document that the verify, not a larger margin or the timeline check, carries the move's safety in this
   abstraction.
3. **`Derivation_Page.cfg` leaves REPLACE and re-extraction out** (the product with a page ran past 30 minutes);
   `Derivation.cfg` covers both on observations only, and the page path is the same `Commit`.
4. **Liveness runs forbid writer crashes** (`AllowAbortW = FALSE`): with crashes a writer can starve Materialize
   by re-taking the shared lock forever, which the 35 s single attempt and the retry make a latency issue, not a
   safety one.

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
| `Derivation` | `internal/recall`, `internal/expunge`, `internal/consolidate`, `internal/pages`: `TestVisibility_AllSurfaces`, `TestVisibility_SegmentHiding`, `TestInvalidate_RestoreExact`, `TestAsOf_*`, `TestExpunge_DerivationLock`, `TestDelete_ReuseDocumentID`, `TestReplace_ThenDelete` (C-1), `TestPage_CommitReverifies` (C-2), `TestApply_BaseVersionCAS` (C-3), `TestReextract_DerivedStaysVisible` (C-6), `TestMaterialize_BatchRereadsFactHidden` (C-7) |
| `ShardMove` | `internal/move`: `TestMove_RollbackEveryStep`, `TestMove_ReadyState`, `TestMove_ActiveBacklog`, `TestMove_InsSeqReconcile`, `TestMove_CatalogCAS`, `TestMove_ZombieFenced`, `TestRestore_OpenMoves`; mover killed at every persisted state |
| `Durability` | `internal/intent`, `cmd/engramctl restore replay`: `TestIntent_AckImpliesIntent`, `TestIntent_DuplicateAttempt` (re-put of the committed marker's intent), `TestIntent_AckRereadsMarker`, `TestIntent_EpochGuard`, `TestRestore_ReplaysIntents`, `TestFailover_ReplaysIntents`, a double-restore case |
| `Storage` | `internal/store`, `internal/index`: `TestContent_InsertOnly`, `TestVectors_ModelGeneration`, `TestHNSW_PerNamespace`, `TestHNSW_RebuildOnly` |
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
| `hidden`, `hidRe`, `ctomb` | `fact_hidden(cause = 'invalidate' / 'reextract')`; `chunk_tombstones` |
| `born`, `gone` | `facts` rows; rows removed by the purges |
| `vers[n][v]` | `observation_versions` / `page_versions` (`root_version`, stub flag); `observation_inputs`, `page_version_inputs` (no FK to `facts`); `effective_at` |
| `dh` | `derived_hidden(root_version, from_version, cause_kind, cause_id)` |
| `props` | `consolidation_proposals` with `base_version` |
| `lockX`, writer `verified` | derivation advisory lock, exclusive and shared |
| `ShardMove.cat`, `cm`, `mp` | catalog `namespaces(shard_id, epoch)`; `namespace_moves.state`; the move workflow |
| `own[s]` | `namespace_ownership(state, epoch)` on shard `s` |
| `store`, `mk`, `ex` | insert-only tables (`ins_seq`); mutable-class tables; expiring-class tables |
| `tl`, `mtl`, `bak` | `catalog.shards.timeline_id`; the mover session's timeline; pgBackRest backup or standby |
| `Durability.intents`, `dbq`, `fl`, `ep` | `_control/deletes/...` objects; the shard `deletion_log`; `catalog.shards.replay_floor`; the namespace epoch |
| `Storage.vec`, `cur`, `idx`, `pc` | `fact_vectors`; the namespace's current model; the per-namespace partial HNSW; `vector_indexes.purged_since_build` |
| `Outbox.*`, `Consolidation.*` | visible `outbox` rows, `outbox_cursors`; `consolidation_batches`, `consolidation_applied` |

### 7.5 What is not formalised, and why

- **LLM outputs** (extraction, routing, writing, reflect): non-deterministic external input, modelled as
  unconstrained choices. Quality is measured in section 8, not proved.
- **Ranking quality and HNSW recall**; **index engines behind `index.Searcher`** (candidates are filtered by the
  same N116 predicate).
- **Temporal's guarantees**, **Postgres semantics** (snapshot isolation, advisory try-lock, `synchronous_commit =
  local`), **the blob store's strong consistency**: assumed as documented.
- **Zombie primaries serving client writes**, instance-level failover and the relay's 10 s timeline check:
  outside `ShardMove.tla`; `_NoTimelineCheck` passes, so there is no must-fail configuration for
  `ZombieCannotCutOver`.
- **Multi-namespace interference in a move**, **reads during a move**, **blob copy**, **`return_abort`**:
  tested (`TestIso_Move_Epoch`, misroute suites), not modelled.
- **Export snapshot expiry (N126), page refresh policy, Reflect, quotas, config inheritance, JWT/authz, Kafka
  beyond per-partition order**: no interleaving worth a model.

### 7.6 CI integration

- **Nightly `formal-tlc`** (`.github/workflows/formal-nightly.yml`): pins `tla2tools.jar` by SHA-256, runs every
  `*.cfg` under `formal/tla/` with `-workers auto` and a 30-minute cap per configuration. A design configuration
  must end with "No error has been found"; a must-fail configuration must end with "Invariant ... is violated" on
  the invariant named in its first comment line (or, for a liveness property, "Temporal properties were
  violated"); anything else fails the job. `formal/tla/EXPECT` lists the expected outcome of every configuration;
  the experiments `ShardMove_ZeroMargin` and `_NoTimelineCheck` are expected to pass. A **timeout is
  neither a failure nor a pass**: it is reported INCOMPLETE with the states and depth reached. A bound bump that
  pushes a design configuration over 30 minutes is a spec change reviewed as such.
- **PR job `formal-quick`**: parses every spec with SANY and runs every must-fail configuration and every design
  configuration that finishes in under two minutes; the longer ones (`Derivation.cfg`, `Durability.cfg`,
  `Durability_Chain.cfg`, `ShardMove.cfg`, `Storage.cfg`, `Consolidation.cfg`) run nightly and on PRs that touch their
  spec or its mapped Go files.
- **Lean**: `formal/lean/` is a Lake project (`lakefile.lean`, `lean-toolchain`); the PR job runs `lake build` and
  `scripts/sorry-count.sh`, which fails if the `sorry` count exceeds `formal/lean/SORRY_BASELINE` (3).
- **Spec and test coupling**: `formal/MANIFEST.md` maps each `.tla` and `.lean` file to its Go test files (7.4);
  `scripts/formal-manifest-check.sh` fails a PR that changes a spec without a change in at least one mapped test file,
  unless it carries the `formal-no-test-change` label with a justification.
- **Trace validation** runs on the Postgres and chaos tests' traces in the integration job (`-workers 1`).

All results of this section, as run (4 workers, 10 GB heap; `time` is TLC's own wall-clock; for a violation,
`generated` and `distinct` are the work done before the counterexample and `depth` is its length):

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
