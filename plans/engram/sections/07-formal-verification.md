## 7. Formal verification

Scope: the six TLA+ specifications under `formal/tla/` and the four Lean 4 modules under
`formal/lean/Engram/`; what TLC actually reported on them; how the Go code is kept faithful to
them; what is not formalised; how it is wired into CI. Four specs are new in the round-3
redesign and model its protocols (`Derivation`, `ShardMove`, `Durability`, `Storage`); two
survive from before because D22 left their protocols intact (`Outbox`, `Consolidation`). Every
number below is copied from a log in `formal/tla/results/` (summary in `RESULTS.md`).

Tooling: TLC 2.18 (`de.hhu.stups:tlatools:1.1.0`), OpenJDK 21, 4 workers, 10 GB heap, 4 cores,
30-minute cap per configuration. Lean 4 is not available in the planning environment; the Lean
files are marked "not type-checked".

### 7.1 What is modelled, and the convention

| Spec | Register | Question the model answers |
|---|---|---|
| `Derivation.tla` | N113, N115 to N118, N120, N133, D9 | Can a deleted or invalidated fact reach a reader through a fact, an observation version or a page version, at any `as_of`, with the markers as the only synchronous write, and is nothing else hidden? |
| `ShardMove.tla` | D5, N123 to N125 | Dirty copy, freeze, reconcile by set difference, cutover through `ready`, rollback before (c), restore and failover during a move: one writer, no loss, no duplicate, a started move ends. |
| `Durability.tla` | N122 | Is every acknowledged delete or invalidation still in force when reads reopen after a restore or failover? |
| `Storage.tla` | N111 to N113 | Content rows never change after insert, an embedding-model flip never exposes a row without a vector, the index converges after a purge. |
| `Outbox.tla` | D6, N80 | No committed event is lost through sequence gaps; per-namespace order; the 2 x timeout watch horizon. |
| `Consolidation.tla` | N43, N121 | A round's decisions are applied exactly once under at-least-once activities and crashes. |

Each spec has a *design* configuration (every knob at the register's value) that must end with
"No error has been found", and *must-fail* configurations in which one knob is set to the
plausible simplification; each must end with the invariant named in the configuration's first
comment line violated. A design that passes only because the simplification was never tried is
not evidence, so the must-fail configurations are part of the deliverable and run in CI. A spec
whose bounds do not let TLC finish is not a gate: the bounds below were sized until every design
configuration completes in under 20 minutes.

### 7.2 The specifications

#### 7.2.1 `Derivation.tla`: visibility of derived data

*Model.* Facts are `f1 = (d1, v1)`, `f2 = (d1, v2)` (the document id re-used right after the
delete, N133) and `f3 = (d2, v1)`, mentioned at 1, 3, 2. State: `tomb[d]` (the tombstone's
`up_to_version`; a fact is deleted iff `version <= tomb[d]`, and a document is re-ingested at
`tomb[d] + 1`), `hidden` (`fact_hidden`), versions of observations and pages with
`root_version` and inputs (fact ids; one observation version for pages), `derived_hidden` rows
`(node, root_version, from_version)`, the derivation lock, and one writer. A writer is the
two-stage path: *Route* (stage 1, no lock) picks a target and candidate inputs; *Verify* takes the
shared derivation lock and keeps only visible inputs (an update also needs a visible previous
version); *Commit* inserts the version and releases the lock. Markers (`DeleteDocument`,
`Invalidate`) take no lock and may land between any two writer steps; `Restore` takes the lock
exclusively. The expunge is `MatStart` (exclusive lock, snapshot of the versions whose segment names
a victim), `MatWrite` (insert `derived_hidden`, document `materialized`, release) and `Purge`
(delete the victims' facts **and the evidence rows that name them**). Visibility is exactly N117:
`(n, v)` is visible iff no fact of its segment's real evidence is deleted or hidden, no
`derived_hidden` row covers it, and (pages) every cited observation version is visible. A *ghost*
copy of the evidence survives `Purge` so the invariants can say what is true after the real rows are
gone. Recall at `T` serves, per node, the latest visible version with `effective_at <= T` (the
model's reading of D9 plus N117).

*Invariants.* `NoDeletedDerivationServed`, `NoInvalidatedDerivationServed` (no served fact or
version has a deleted or hidden fact anywhere in its derivation, ghost included);
`RestoreExact` (a version with no currently hidden fact is visible exactly as if no invalidation
had happened); `NoOverHiding` (a fact or version with no victim is visible: the blast-radius
guard, and the re-used document id is not hidden by the old tombstone); `AsOfNoLeak` (served at
`T` implies every fact in the derivation was mentioned at or before `T`); `MaterializeComplete` (once
a document is `materialized`, `derived_hidden` covers every committed version whose derivation
names one of its victims); liveness `ExpungeCompletes` (a pending tombstone is eventually purged,
under weak fairness of the writer and expunge steps).

*Bounds.* Version-sequence products dominate the state space, so two design configurations cover
the two levels. `Derivation.cfg`: observations only, 2 observations x up to 3 versions, 2 deletes,
2 curation operations (Invalidate or Restore), 5 routed writes, symmetry on observations.
`Derivation_Page.cfg`: 1 observation x 2 versions and 1 page x 2 versions (a page cites at most one
observation version), 2 deletes, 2 curation operations, 5 routed writes. `Derivation_Live.cfg`:
liveness at 1 observation x 2 versions, 4 routed writes, no symmetry. One writer, one fact per
version (two in `Derivation_EffCited.cfg`, which needs a shown-but-uncited fact).

*Results.* `Derivation.cfg`: 7,659,307 generated / 2,133,161 distinct, depth 29, 01 min 35s. `Derivation_Page.cfg`: 81,273,512 generated / 22,215,959 distinct, depth 28, 16 min 53s.
`Derivation_Live.cfg` (safety and `ExpungeCompletes`): 314,063 generated / 87,830 distinct, depth 24, 11s.

*Must-fail.*
`Derivation_NoLock.cfg` (writers and Materialize ignore each other) violates
`NoDeletedDerivationServed` at depth 9: a writer verifies `f3` visible, the delete
marker commits, Materialize runs and finds no version to cover, the writer commits a version citing
`f3`, `Purge` deletes the evidence row, and the version is served. This is the race N120 exists
for; with the lock the writer holds the shared lock across its commit so Materialize waits and
sees the version. `Derivation_MatInvalid.cfg` (Materialize also writes permanent rows for
invalidated facts) violates `RestoreExact` at depth 11: after `Restore` the
version stays hidden. `Derivation_EffCited.cfg` (`effective_at` over cited facts only) violates
`AsOfNoLeak` at depth 6. `Derivation_TombByDocId.cfg` (a tombstone hides the
whole document id, the pre-N133 rule) violates `NoOverHiding` at depth 4:
the re-ingested `f2` is invisible. `Derivation_RestoreNoLock.cfg` removes the exclusive lock from
`Restore` and is **expected to pass**, and does: 8,115,973 generated / 2,159,112 distinct, depth 29, 01 min 34s. The lock on `Restore` (N133)
is therefore not needed for any property stated here; it is kept as the register decided and is
a candidate for removal if it ever shows up as contention.

#### 7.2.2 `ShardMove.tla`: dirty copy, freeze, reconcile, cutover, rollback, restore

*Model.* One namespace, two shards, a catalog row `(shard, epoch)`, `namespace_moves.state` (`mp`),
and an ownership row per shard (`none`, `incoming`, `ready`, `active`, `frozen`, `moved_out`,
`restoring`, `replaying`). Clients cache `(shard, epoch)`; a write takes the shared fence at an
`active` shard at its cached epoch, holds it for at most `Life` ticks, inserts an immutable row
and sets one mutable cell. The mover: `Plan` (target `incoming` at epoch + 1), `CopyRange` (any
non-empty part of what the source has and the target lacks, concurrent with writes), `CopyMut`
(possibly stale), `Freeze` (exclusive fence: no holder), `Reconcile` (re-copy rows with
`created_at + Margin >= T_copy`, merge the mutable cell in full), verify (the source and target row
sets and cells must match, else only `Rollback` stays enabled), (b') `incoming -> ready`, (c)
source `frozen -> moved_out` (point of no return), (b'') `ready -> active`, (d) catalog flip.
`Rollback` is enabled in every state before (c). Restore and failover (`Restore`, `Reconcile_`,
`RestoreDone`): the shard reverts to its backup (rows after it are an RPO loss, recorded in a ghost
set), comes up `restoring`, reconciles the open move against `namespace_moves` (before (c): roll
back, thaw the source; after (c): the source becomes `moved_out`, a restored target is re-filled
from the source that still holds the data), bumps the catalog epoch when it is the owner, then
`replaying` (Durability.tla), then its final row. A failover also bumps the source timeline; the
mover compares its session timeline at Freeze and (c). Every mover step is conditional on the
rows it observed (`UPDATE ... WHERE state = expected`); `ShardMove_UnfencedSteps.cfg` drops that.

*Invariants.* `SingleWriter` (at most one shard `active`, and a write holder only at an `active`
shard, so `ready`, `incoming`, `frozen`, `moved_out` and `restoring` accept nothing);
`NoLossNoDup` (the row set and the cell the target is activated with equal what was frozen at the
source; the PK makes duplicates impossible, so this is the loss check); `RollbackPossibleBeforeC`
(before (c) the source is the owner and holds every committed row, so rollback loses nothing);
`NoWriteToTargetBeforeC`; `NoRouteToTargetBeforeC` (the catalog never names the target before (c)
and (b'')); `ZombieCannotCutOver` (a session on a stale timeline never executed Freeze or (c));
`RestoreReconciles` (a restored shard never ends `active` while another shard is `active` or `ready`
for the namespace, and never at an epoch below the catalog's); liveness `MoveTerminates` (a started
move ends `done` or `rolled_back`, with fairness on the forward steps, recovery and client commits, and on
rollback only when the verify failed).

*Bounds.* `ShardMove.cfg`: 3 rows, 2 clients (symmetry), writer lifetime 1 tick, margin 1,
3 ticks, epochs up to 4, 1 restore or failover of either shard, 1 extra backup.
`ShardMove_Live.cfg`: 2 rows, 2 ticks, no symmetry, liveness.

*Results.* `ShardMove.cfg`: 88,022,416 generated / 16,472,880 distinct, depth 28, 03 min 57s. `ShardMove_Live.cfg` (safety and `MoveTerminates`):
2,760,289 generated / 578,804 distinct, depth 25, 31s. `ShardMove_ZeroMargin.cfg` (margin 0 **with** the verify: a short margin
may cost a rollback, never a row; safety and `MoveTerminates`): 2,780,467 generated / 582,960 distinct, depth 25, 30s.

*Must-fail.* `ShardMove_NoReady.cfg` (target goes straight to `active` before (c), the catalog
may flip first) violates `RollbackPossibleBeforeC` at depth 9: the target accepts a
write, and the only rollback left would discard it. `ShardMove_RestoreNoReconcile.cfg` (restore
trusts its backup row, bumps the epoch and ignores the open move: the old "epoch bump during a move") violates
`RestoreReconciles` at depth 8: the source returns `active` at epoch e + 1
while the target is `ready` at e + 1. `ShardMove_NoVerify.cfg` (margin 0 and no count/hash verify)
violates `NoLossNoDup` at depth 10: a row begun one tick before `T_copy` and committed
after it is neither in the dirty copy nor in the re-copy. There is no must-fail configuration
for `ZombieCannotCutOver`: `ShardMove_NoTimelineCheck.cfg` (no timeline comparison) and
`ShardMove_UnfencedSteps.cfg` (no row compare-and-set in the mover) are experiments that are expected to pass
and do (7.2.6 items 3 and 5): 90,507,928 generated / 16,472,880 distinct, depth 28, 03 min 55s; 142,521,064 generated / 21,636,679 distinct, depth 28, 05 min 49s.

#### 7.2.3 `Durability.tla`: acknowledged deletes survive restore and failover

*Model.* Operations on two subjects: `inv` and `res` on a fact (`fact_hidden`, last state wins) and `del`
on a document. One subject has at most one operation in flight; an operation that errors closes its
subject (its intent may still take effect, as documented). Steps: `PutIntent` (durable blob object),
`Commit` (the marker transaction, a local commit, refused while the shard is not `active`, abandoned
after `Lat` ticks), `Ack`. `dbq` is the sequence of marker transactions the shard has applied.
`Restore(p)` keeps only commits with commit time <= p and comes up `restoring`; `Replay` applies, in name
order, each intent with `deleted_at + Margin >= floor` that is not applied (replayed commits get a new
commit time); `Reopen` returns to `active`. Up to two restores, so a restore can hit before, during or after
a replay.

*Invariants.* `AckImpliesIntent`; `AckedDeleteSurvives` (while the shard serves, a document whose last
operation was acked is tombstoned); `IntentOrderLastWins` (the same for the `inv`/`res` fact: an acked
Restore is not undone by an older Invalidate); `RestoreReplaysIntents` (when reads reopen after a restore,
every acked in-window intent is applied).

*Bounds.* 3 operations, 5 ticks, `Lat` 2, `Margin` 2, 2 restores.

*Results.* `Durability.cfg`: 117,776,042 generated / 26,113,068 distinct, depth 25, 04 min 26s.

*Must-fail.* `Durability_AckBeforeIntent.cfg` (the ack may precede the intent put) violates
`AckImpliesIntent` at depth 4. `Durability_ReopenEarly.cfg` (reads reopen before the replay
finishes) violates `AckedDeleteSurvives` at depth 9. `Durability_UnorderedReplay.cfg` (replay in any order)
violates `IntentOrderLastWins` at depth 14 (an acked Restore and then an acked
Invalidate, both lost, replayed Invalidate first: the Restore is applied last and the fact comes back). `Durability_NarrowWindow.cfg` (`Margin` 0 below `Lat` 2) violates `AckedDeleteSurvives`
at depth 11. `Durability_RetargetRestore.cfg` (each restore recomputes the replay floor
from its own target) violates `AckedDeleteSurvives` at depth 11; this one is a flaw in the register as
written, 7.2.6 item 1.

#### 7.2.4 `Storage.tla`: insert-only content, vector generations, index convergence

*Model.* Content rows are inserted once; `Purge` is the only later change and needs an expunge marker.
Vectors are insert-only rows `(row, generation)`; the namespace's current generation flips only
when every live row has a next-generation vector and the index holds it (`ReembedNamespace`); old
generations are expunged afterwards; the index follows the vector table asynchronously.
*Invariants.* `ContentImmutable`, `PurgeNeedsMarker`, `VectorGenerationConsistent` (every row live at the
last flip and still live has a current-generation vector), liveness `IndexConvergence` (the index's
current-generation entries eventually equal the vector table's). Bounds: 3 rows, 3 generations.
*Results.* `Storage.cfg`: 12,872,199 generated / 1,478,807 distinct, depth 36, 06 min 40s. Must-fail: `Storage_Update.cfg` (in-place rewrite) violates
`ContentImmutable` (depth 3); `Storage_FlipEarly.cfg` (flip without waiting) violates
`VectorGenerationConsistent` (depth 3); `Storage_PurgeUnmarked.cfg` violates
`PurgeNeedsMarker` (depth 3).

#### 7.2.5 `Outbox.tla` and `Consolidation.tla`

D22 changed neither protocol's state machine, so both models are kept; what changed is the text
around them. `Outbox.tla` (D6): writers draw `seq` inside their transaction, commit out of order,
and a relay with a persisted cursor and a gap watchlist declares a missing `seq` aborted after
`Watch` ticks; assumption A-F1 now holds without a caveat because every role runs
`synchronous_commit = local` (N122), so a commit cannot hang on a standby, and moves no longer replay
the outbox (N124). Invariants `NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered`,
`IdempotentConsumerState`, liveness `NoLossLive`. `Outbox.cfg`: 3 writers, 2 namespaces, 2 consumers, 5 seqs,
timeout 2, watch 4, 1 relay crash: 17,243,719 generated / 5,557,863 distinct, depth 39, 01 min 35s. `Outbox_Live.cfg`: 1,342,866 generated / 486,937 distinct, depth 31, 38s. Must-fail:
`Outbox_NoWatch.cfg` (no watchlist) and `Outbox_Watch1x.cfg` (horizon = 1 x timeout) violate `NoLossSafety`
at depth 7 and depth 9.

`Consolidation.tla` (D12, N43, N121): a round's batch is bisected on failure; stage 1's decisions are
persisted write-once under `batch_key`; each op's effect and its `op_key` row are one transaction;
the worker may crash anywhere and Temporal re-runs the activity. The model abstracts the text-writing
call (stage 2); it checks that a persisted decision list yields one effect per `op_key` and that the
version written cites only facts of the round. D22 changed one thing here: a delete is a marker and
no longer rewrites observation sources (no trigger), so the old "observation never cites a retired
fact" invariant is replaced by `ObservationHasSources` (a written version cites at least one fact of
the round) and the delete-versus-write race moved to `Derivation.tla`. Invariants `ExactlyOnceEffect`,
`ObservationHasSources`, liveness `RoundTerminates`. `Consolidation.cfg` (3 facts, 1 observation,
2 crashes, 1 delete; the 4-fact and the 2-observation variants were stopped after about ten minutes with the queue still growing, and are not gates): 10,864,684 generated / 2,426,544 distinct, depth 18, 56s. `Consolidation_Live.cfg`: 65,555 generated / 20,404 distinct, depth 12, 02s.
Must-fail: `Consolidation_VolatileProposal.cfg` (the proposal is not persisted before apply) and
`Consolidation_NonAtomicKey.cfg` (effect and key in separate transactions) violate `ExactlyOnceEffect` at
depth 6 and depth 5.

#### 7.2.6 What the models found

Item 1 is a change to the register (N122); the others confirm, bound or relax a register claim.

1. **The replay window of N122 loses acknowledged deletes on a second restore.** "Intents with
   `deleted_at >= restore_point - 10 min`" is anchored at the latest restore's target. A second
   restore or failover before the first replay has finished (an operator retries or retargets a failed
   restore, or the promoted standby fails again) moves the anchor later and skips intents the first
   restore lost; replayed markers also carry new commit times, so a restore after a partial replay
   loses them again. The shortest counterexample (`Durability_RetargetRestore.cfg`) is the retry: an
   acked Invalidate is lost, the shard is restored again to a later point, and reopens without it.
   With a **monotone floor** (the minimum over every restore since the oldest retained backup, persisted
   next to `restore_done`; replay is idempotent through the shard `deletion_log`, and intents are kept
   35 days, so the window is bounded) `Durability.cfg` passes.
2. **What the margin must exceed.** `Durability_NarrowWindow.cfg` loses an acked delete when the replay
   margin is below the longest time between the intent put and the marker commit. The register's
   10 minutes against a 30 s statement timeout is a wide factor; the model fixes what the number has to
   stay above (statement and idle timeouts plus pool wait), which is a constraint on any later change
   to either.
3. **The count/hash verify is load-bearing; the row-state compare-and-set is not.** The copy margin
   must be at least the writer lifetime: with margin 0 and no verify a row begun before `T_copy` and
   committed after it is lost (`ShardMove_NoVerify.cfg`); with the verify the same margin can only
   cost a rollback (`ShardMove_ZeroMargin.cfg`, safety and `MoveTerminates` hold). Conversely, letting
   mover steps ignore the ownership rows they act on (`ShardMove_UnfencedSteps.cfg`) changes no result
   while the verify is there (the unfenced model admits more interleavings and still passes). The `WHERE state = expected` guard on each step stays, since it removes
   interleavings the recovery path would otherwise have to undo, but the safety argument rests on the verify.
4. **A restore of a non-owner during a move needs the catalog arbiter too.** Restoring the target
   before (c) must roll the move back and thaw the source; after (c) it must re-fill the target from
   the source that still holds the data (the `moved_out` row's data survives until cleanup). Without
   `namespace_moves` as arbiter (`ShardMove_RestoreNoReconcile.cfg`) a restored source returns
   `active` next to a `ready` target at the same epoch. N123 says this; the model shows both
   directions are needed.
5. **Two guards are not load-bearing in this abstraction.** Removing the timeline comparison at
   Freeze and (c) (`ShardMove_NoTimelineCheck.cfg`) changes no result: a restored or promoted shard
   rejects Freeze and (c) through its ownership row (`restoring`) until recovery has run, and
   `namespace_moves` arbitrates. Removing the exclusive derivation lock from `Restore`
   (`Derivation_RestoreNoLock.cfg`) changes no result either. Both stay as the register decided; the
   case the timeline check covers (a zombie primary still serving a mover session and client writes)
   is outside the model (7.5), so there is no must-fail configuration for `ZombieCannotCutOver`.
6. **The derivation lock is needed exactly because `Purge` deletes the evidence.** The lock-free
   configuration fails only after `Purge` (`Derivation_NoLock.cfg`); before it the read-time check still
   sees the victim, and `MaterializeComplete` already fails at the late commit. The model confirms the
   lock is sufficient (`Derivation.cfg`, `Derivation_Page.cfg`).

### 7.3 Lean 4 theorems

Files under `formal/lean/Engram/`, Lean 4 core only (no Mathlib). None is type-checked; the
statuses are honest about what is written out and what is `sorry`. Each file ends with
`example ... := by decide` checks that double as the table test of its Go counterpart.
`TagMatch.lean`: the six tag modes of D10, decidability, `anyStrict_imp_any`, `allStrict_imp_all`,
`exact_imp_allStrict`, monotonicity of the strict modes (written), non-monotonicity of ANY and ALL
(`decide` witness). `RRF.lean`: the fused score is invariant under arm reordering (`score_perm`,
written), per-arm monotonicity (`contribOf_le_of_improves`, written), `score_mono` and
`score_le_bound` (`sorry`), `contribNat_antitone` (needs `0 < k`, which is why D10 fixes k = 60,
written). `Packer.lean`: `pack_total_le`, `pack_sublist`, `keep_count`, `keep_append`,
`keep_skip_oversize` (written), `kept_fits` (`sorry`). `TemporalWindow.lean`: overlap symmetry,
containment order, `distanceTo` non-negativity, monotonicity and 1-Lipschitz (written; overlap
non-transitivity by `decide`). Three `sorry` in total (the checked-in baseline).

### 7.4 Conformance: how the Go code stays faithful

A spec that is not tied to a test is documentation. Three mechanisms for every spec.

**(a) Model tests.** For each spec there is an in-memory Go model of its state and actions and a
`pgregory.net/rapid` state machine that runs generated action sequences against the model and
against the real implementation on a `testcontainers` Postgres, checking the spec's invariants after every
step with the TLC constants as generator bounds. Invariant code is written once in
`internal/formal/<spec>/invariants.go`. Section 8.4.6 lists the Go twin of every must-fail configuration
(the fix disabled by a `faultinject` knob reproduces the flaw; with the fix on it is absent).

| Spec | Go package and twin tests |
|---|---|
| `Derivation` | `internal/recall` (visibility predicate), `internal/expunge` and `internal/consolidate` (derivation lock): `TestVisibility_AllSurfaces`, `TestVisibility_SegmentHiding`, `TestInvalidate_RestoreExact`, `TestAsOf_*`, `TestExpunge_DerivationLock`, `TestDelete_ReuseDocumentID` |
| `ShardMove` | `internal/move`: `TestMove_RollbackEveryStep`, `TestMove_ReadyState`, `TestMove_DirtyCopyReconcile`, `TestMove_ZombieFenced`, `TestRestore_OpenMoves`; mover killed at every persisted state |
| `Durability` | `internal/intent`, `cmd/engramctl restore replay`: `TestIntent_AckImpliesIntent`, `TestRestore_ReplaysIntents`, `TestFailover_ReplaysIntents` (plus a double-restore case for item 1 of 7.2.6) |
| `Storage` | `internal/store`, `internal/index`: `TestContent_InsertOnly`, `TestVectors_ModelGeneration`, `TestHNSW_PerNamespace` |
| `Outbox` | `internal/outbox`: relay with writers delayed by `pg_sleep`, `TestOutbox_Watch1x` |
| `Consolidation` | `internal/consolidate`: `TestConsolidation_PersistedProposal`, `_AtomicKey`, `_TwoStage` |
| Lean modules | `internal/recall` table and `rapid` tests (tags, fusion, packing, temporal window) |

**(b) Trace validation.** Store, relay, expunge, intent and move code paths emit JSON-line decision logs in
tests (`internal/formal/trace.Logger`, written after the transaction commits). `engramctl formal trace-to-tla
<spec> <log.jsonl>` emits a module that `EXTENDS` the spec with the trace as a constant sequence; TLC
checks that the logged behaviour is a behaviour of the spec and that every invariant held along it.
The Postgres tests of (a) and the chaos suites of section 8 produce the logs.

**(c) Refinement mapping.** The contract for each model test's state extraction:

| Spec variable | Go / Postgres state |
|---|---|
| `Derivation.tomb[d]`, `ms[d]` | `document_tombstones(up_to_version, expunge_state)` |
| `hidden` | `fact_hidden` rows |
| `born`, `gone` | `facts` rows (`document_version`); rows removed by the purge |
| `vers[n][v].root`, `.finp`, `.oinp`, `.eff` | `observation_versions.root_version`; `observation_inputs`; `page_version_inputs(kind='observation')`; `effective_at` |
| `dh` | `derived_hidden(root_version, from_version)` |
| `lockX`, writer `verified` | derivation advisory lock `hashtextextended(ns, 1)`, exclusive and shared |
| `ShardMove.cat`, `mp` | catalog `namespaces(shard_id, epoch)`; `namespace_moves.state` |
| `own[s]` | `namespace_ownership(state, epoch)` on shard `s` (`restoring`, `replaying` = `frozen/restore`) |
| `store[s]`, `mut[s]` | the immutable tables and the mutable-table rows of the namespace on `s` |
| `tl`, `mtl` | `catalog.shards.timeline_id`; the mover session's timeline |
| `bak[s]` | pgBackRest backup or standby of `s` |
| `Durability.intents`, `dbq` | `_control/deletes/...` objects; markers applied in order, recorded in the shard `deletion_log` |
| `rp` (floor) | the restore floor persisted with `restore_done` |
| `Storage.vec`, `cur`, `idx` | `fact_vectors`; the namespace's current embedding model; the per-namespace partial HNSW fed through `index.Applier` |
| `Outbox.committed`, `cursor`, `watch` | visible `outbox` rows; `outbox_cursors.last_seq`; the relay's in-memory gaps |
| `Consolidation.stored`, `applied`, `effectOf` | `consolidation_batches`; `consolidation_applied`; the effect rows |

### 7.5 What is not formalised, and why

- **LLM outputs** (extraction, routing, writing, reflect): non-deterministic external input, modelled as
  unconstrained choices from a small menu. Quality is measured in section 8, not proved.
- **Ranking quality and HNSW recall**: empirical properties of the index parameters.
- **Index engines behind `index.Searcher`**: assumed to return candidates that the recall path then filters with
  the same N116 predicate; engine-side deletion latency is covered only by `Storage.IndexConvergence`.
- **Temporal's guarantees** (at-least-once activities, determinism, signals): assumed as documented.
- **Postgres semantics**: snapshot isolation of `REPEATABLE READ`, try-lock behaviour of advisory locks
  (`TestFence_TryLockRefusedBehindWaiter`), `synchronous_commit = local`, and that a committed transaction is
  visible to later snapshots. pgbouncer transaction pooling preserves per-transaction affinity.
- **The blob store's strong consistency** (the intent `put` is visible to a later list, N122): assumed from
  the brief; `Durability.tla` takes the put as atomic and durable.
- **Zombie primaries serving client writes**, **instance-level failover** (two instances of one shard) and the
  relay's 10 s timeline check: the model has one instance per shard and treats the timeline check as a guard
  (7.2.6 item 5).
- **Multi-namespace interference in a move** (RLS, the shared outbox, other namespaces' writers): one namespace
  per model; isolation is tested by the suites of section 8.
- **Reads during a move**: reads at `frozen/move` and the stale-read window after (c) are not an invariant of
  `ShardMove.tla`; they are tested by `TestIso_Move_Epoch` and the misroute suites.
- **Blob copy in a move**, the `return_abort` path of moving back onto a `moved_out` shard, the 24 h cleanup
  and the expunge pause: tested, not modelled.
- **Export snapshot expiry (N126), page refresh policy, Reflect, quotas, config inheritance, JWT/authz,
  Kafka beyond per-partition order**: no interleaving worth a model, or covered by table tests.
- **Cursor-pass before purge** (N119 step 2): `Purge` is modelled as enabled once `materialized`; the extra
  precondition only delays it.

### 7.6 CI integration

- **Nightly `formal-tlc`** (`.github/workflows/formal-nightly.yml`): pins `tla2tools.jar` by SHA-256, runs every
  `*.cfg` under `formal/tla/` with `-workers auto` and a 30-minute cap per configuration. A design
  configuration must end with "No error has been found"; a must-fail configuration must end with
  "Invariant ... is violated" on the invariant named in its first comment line (a different invariant, a parse
  error or no violation fails the job); `Derivation_RestoreNoLock.cfg`, `ShardMove_ZeroMargin.cfg`, `ShardMove_NoTimelineCheck.cfg` and
  `ShardMove_UnfencedSteps.cfg` are experiments whose expected outcome is a pass; `formal/tla/EXPECT` lists the
  expected outcome of every configuration. A **timeout is neither a
  failure nor a pass**: it is reported INCOMPLETE with the states and depth reached and shown in its own colour.
  Measured: every design configuration below finishes in under 20 minutes on 4 cores (the longest is
  `Derivation_Page.cfg`); a bound bump that pushes one over 30 minutes is a spec change reviewed as such.
- **PR job `formal-quick`**: parses every spec with SANY and runs every must-fail configuration and every
  design configuration that finishes in under two minutes; the longer ones (`Derivation_Page.cfg`,
  `Durability.cfg`, `ShardMove.cfg`, `Storage.cfg`, `Consolidation.cfg` and the two long ShardMove
  experiments) run nightly and on PRs that touch their spec or its mapped Go files.
- **Lean**: `formal/lean/` is a Lake project; the PR job runs `lake build` and `scripts/sorry-count.sh`, which fails if
  the `sorry` count exceeds the baseline `formal/lean/SORRY_BASELINE` (currently 3).
- **Spec and test coupling**: `formal/MANIFEST.md` maps each `.tla` and `.lean` file to its Go test files (the
  table in 7.4); `scripts/formal-manifest-check.sh` fails a PR that changes a spec without a change in at least one
  mapped test file, unless it carries the `formal-no-test-change` label with a justification.
- **Trace validation** runs on the Postgres and chaos tests' traces in the integration job
  (`-workers 1`, seconds each). The converter and the harnesses are Track F work (section 10).

All results of this section, as run (4 workers, 10 GB heap; times are TLC's own wall-clock):

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
