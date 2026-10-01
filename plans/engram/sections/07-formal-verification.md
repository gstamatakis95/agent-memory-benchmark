## 7. Formal verification plan

Scope of this section: the five TLA+ specifications under `formal/tla/`, the four Lean 4
modules under `formal/lean/Engram/`, what TLC actually reported when run on them, how the Go
code is kept faithful to them, what is deliberately not formalised, and how all of it is wired
into CI. Everything here follows the decision register; the few places where modelling forced
a sharper decision than the register states are collected under "New decisions" at the end
and are marked `ND-n` in the text.

Tooling actually used for the results below: TLC **2.18** (the `de.hhu.stups:tlatools:1.1.0`
repackaging of `tla2tools.jar` from Maven Central, run as `java -cp tla2tools.jar tlc2.TLC`),
OpenJDK 21, 4 workers, 6 GB heap, 560 s wall cap per configuration. Lean 4 was **not**
available in the planning environment: every Lean file is marked "not type-checked" and the
theorems table says which proofs are written out and which are `sorry`.

### 7.1 What is formalised and why

The register's protocols are all "small concurrent state machines over three stores"
(catalog, source shard, target shard; store, index, outbox; facts, versions, observations).
Each of them has at least one interleaving that a code review will not reliably catch and
that production will reliably hit. Those are the ones modelled. Each spec has a *design*
configuration (all knobs set to the register's design) and one or more *counterexample*
configurations in which one knob is flipped to the obvious simplification; TLC must pass the
former and fail the latter. A design that passes only because the simplification was never
tried is not evidence of anything, so the counterexample configurations are part of the
deliverable and run in CI.

| Spec | Protocol (register) | Properties | Model size | TLC bounds (cfg) | Run result |
|---|---|---|---|---|---|
| `Outbox.tla` | D6 outbox relay: sequence-drawn `seq`, out-of-order commit, statement timeout, cursor + gap watchlist, fan-out to index/kafka | `NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered`, `IdempotentConsumerState`, liveness `NoLossLive` | 3 writers, 2 namespaces, 2 consumers, 5 seqs, Timeout 2, Watch 4, 1 relay crash | `Outbox.cfg` (safety, symmetry); `Outbox_Live.cfg` (2 writers, 4 seqs, no symmetry) | **PASS** 17,243,719 generated / 5,557,863 distinct, depth 39, 1 min 38 s. Liveness: **PASS** 1,342,866 / 486,937, depth 31, 40 s |
| `Outbox.tla` | same, watchlist removed (Watch 0) | `NoLossSafety` | same | `Outbox_NoWatch.cfg` | **FAIL as intended**, counterexample at depth 7 (7.2.3) |
| `Outbox.tla` | same, horizon = 1 × timeout (Watch 2) | `NoLossSafety` | same | `Outbox_Watch1x.cfg` | **FAIL as intended**, depth 9, 106,588 distinct states before the violation, 3 s |
| `Consolidation.tla` | D12 consolidation round: at-least-once activities, crashes, `batch_key`/`op_key`, bisect 4→2→1, concurrent fact delete, source-count trigger | `ExactlyOnceEffect`, `ObservationHasSources`, liveness `RoundTerminates` | 4 facts, 2 observations, proposals of ≤ 2 ops, 2 crashes, 1 delete | `Consolidation.cfg` (safety, symmetry on Obs); `Consolidation_Live.cfg` (1 observation, 1 crash) | PENDING_CONS_SAFETY / PENDING_CONS_LIVE |
| `Consolidation.tla` | proposal not persisted before apply | `ExactlyOnceEffect` | same | `Consolidation_VolatileProposal.cfg` | **FAIL as intended**, depth 6, 2 s |
| `Consolidation.tla` | effect and `consolidation_applied` insert in separate transactions | `ExactlyOnceEffect` | same | `Consolidation_NonAtomicKey.cfg` | **FAIL as intended**, depth 5, 2 s |
| `AsOf.tla` | D9 `as_of`: `mentioned_at`, observation versions, `effective_at`, recall(T) | `NoLeak`, `EffectiveCoversCited`, action property `OlderVersionStable` | 3 facts, 2 observations, times 1..3, 2 versions each | `AsOf.cfg` (symmetry) | **PASS** 1,687,878 / 332,776, depth 8, 1 min 22 s (run concurrently with another check) |
| `AsOf.tla` | `effective_at` from cited sources only (D9 as first written) | `NoLeak` | same | `AsOf_CitedOnly.cfg` | **FAIL as intended**, depth 4, 1 s (7.2.4) |
| `DocLifecycle.tla` | D8/D11/D12/D7: replace/delete vs per-chunk commits, consolidation read/apply, purge, transactional and async index | `NoOrphanLinks`, `NoObservationCitesDeletedAfterAck`, `NoDeletedContentRecalled`, `ObservationHasSources`, `OneActiveVersion`, `VersionsConsistent`, liveness `IndexConverges` | 2 documents, 3 hashes, 2 versions, 3 observations, 3 consolidations | `DocLifecycle.cfg` (async index, symmetry); `DocLifecycle_Tx.cfg` (transactional index); `DocLifecycle_Live.cfg` (2 hashes, 1 observation, no symmetry) | PENDING_DOC_ASYNC / PENDING_DOC_TX / PENDING_DOC_LIVE |
| `DocLifecycle.tla` | five single-knob simplifications (7.2.1) | the invariant each one breaks | 2 documents, 2 hashes, 2 observations | `DocLifecycle_NoCommitCheck/_NoFinalizeCheck/_CitedOnly/_NoApplyCheck/_UnfilteredIndex.cfg` | **all FAIL as intended**, depths 5/7/8/6/6, ≤ 3 s each |
| `ShardMove.tla` | D5 + N2 move: plan, barrier copy, catch-up, freeze, drain, four-step cutover, rollback, mover crash/restart, stale caches, pinned workflows, concurrent retain/consolidate/delete writes | `SingleWritableOwner`, `WritesOnlyAtOwner`, `NoLossNoDup`, `NoDupAnywhere`, `ReadsFresh`, liveness `MoveTerminates` | 2 shards, 2 namespaces (1 movable), 2 API clients, 1 workflow, 3 writes, epochs ≤ 3, 2 move attempts, 1 crash | `ShardMove.cfg` (symmetry on clients/writes); `ShardMove_Live.cfg` (1 namespace, 1 client, 2 writes, epochs ≤ 2) | PENDING_MOVE_SAFETY / PENDING_MOVE_LIVE |
| `ShardMove.tla` | copy snapshot without barrier (D5 step 2 as first written) | `NoLossNoDup` | 1 namespace, 2 writes | `ShardMove_NoBarrier.cfg` | **FAIL as intended**, depth 12, 1 s (7.2.5) |
| `ShardMove.tla` | catalog switched before source `moved_out` (D5 step 6 order) | `ReadsFresh` | same | `ShardMove_D5Order.cfg` | **FAIL as intended**, depth 8, 1,523 distinct states, 3 s (7.2.5) |

Reading the table: every design configuration that finished passed, every counterexample
configuration failed on exactly the invariant it was built to break, and each failure maps
to a concrete rule in the Go code (7.4). Bounds were chosen as the smallest that exercise
every race of interest twice (two documents so that links and consolidation batches cross a
delete boundary; three writers so that a gap can sit between two committed seqs; two move
attempts so that an epoch can be reused after a rollback). Liveness configurations are
smaller because TLC's symmetry reduction is unsound for liveness and the liveness check is
roughly 10× slower per state.

### 7.2 The specifications

Each subsection gives the state variables, the actions, the invariants and the liveness
property in prose, followed by the load-bearing excerpt of the TLA+. Full modules are in
`formal/tla/`.

#### 7.2.1 `DocLifecycle.tla` — replace/delete vs retain, consolidation, index sync

**State.** `fstate[f] ∈ {absent, live, retired}` per fact `f = (document, hash)` (purged rows
are `absent` again); `vstate[d][v]` ∈ {none, ingesting, active, superseded, deleted} with
`vcontent` (the version's hash set) and `vpending` (hashes not yet committed); `deleted`,
the set of facts whose delete was acknowledged and that have not been re-retained since;
`links`, unordered pairs of facts; `obs[o]` with status ∈ {absent, live, stale, retired},
`src` (sources), `deriv` (every fact the LLM saw when the text was written) and the in-flight
`snap`; for the async index, `index` and `dirty` (facts with an outbox event not yet applied).

**Actions.** `StartRetain(d, v, C)` allocates versions in order and may run while an older
version is still ingesting. `CommitChunk(d, v, h)` is one transaction: insert (new hash),
un-retire (retired but not purged) or keep; a new fact is linked to every live fact (the
abstraction of entity/semantic/temporal linking); the transaction requires
`document_versions.status = 'ingesting'` (`CommitChecksVersion`). `AbandonVersion` is what
happens to the workflow when that check fails. `FinalizeVersion(d, v)` marks `v` active,
supersedes older versions and retires the document's live facts that `v` does not contain —
unless a newer version has already been started, in which case `v` is marked superseded and
retires nothing (`FinalizeChecksNewer`). `Delete(d)` is the synchronous cascade of D8: all
ingesting/active versions `deleted`, facts retired, links to the document removed, observation
sources pruned, observations with no sources retired, observations that lost a source hidden as
`stale`. `Purge(f)` is the asynchronous physical delete. `ConsolidateRead(o)` snapshots the live
facts (the batch); `ConsolidateApply(o)` is the apply transaction, which re-verifies the batch
(`ApplyCheck = "batch"`). `Relay(f)` syncs one dirty fact into the async index.

**Invariants.** `NoOrphanLinks`: every link endpoint is an existing row and no link touches
acknowledged-deleted content. `NoObservationCitesDeletedAfterAck`: `src ∩ deleted = ∅` for
every observation. `NoDeletedContentRecalled`: neither the fact arm, the graph arm (one hop
over `links`, both endpoints live) nor any live observation whose `deriv` intersects
`deleted` is returned; recall over the async index is `index ∩ Live`. `ObservationHasSources`
(D12's trigger). `OneActiveVersion` and `VersionsConsistent` (an active version's chunks are
live; every live fact belongs to a non-deleted version; once no version of a document is
ingesting, its live facts are exactly the active version's chunks). **Liveness**
`IndexConverges ≡ ◇□(Index = Live)` under weak fairness of `Relay`; every other action is
bounded by the constants, so the system quiesces and the property is meaningful.

```tla
CommitChunk(d, v, h) ==
  /\ h \in vpending[d][v]
  /\ CommitChecksVersion => vstate[d][v] = "ingesting"
  /\ LET f == <<d, h>> IN
     /\ fstate' = [fstate EXCEPT ![f] = "live"]
     /\ links' = IF fstate[f] = "absent" THEN links \cup {{f, g} : g \in Live} ELSE links
     /\ deleted' = deleted \ {f}
     /\ dirty' = IF Async /\ fstate[f] /= "live" THEN dirty \cup {f} ELSE dirty
  /\ vpending' = [vpending EXCEPT ![d][v] = @ \ {h}]

FinalizeVersion(d, v) ==
  /\ vstate[d][v] = "ingesting" /\ vpending[d][v] = {}
  /\ IF FinalizeChecksNewer /\ NewerStarted(d, v)
       THEN vstate' = [vstate EXCEPT ![d][v] = "superseded"] /\ UNCHANGED <<fstate, dirty>>
       ELSE LET R == {f \in FactsOf(d) : fstate[f] = "live" /\ f[2] \notin vcontent[d][v]} IN
            /\ vstate' = [vstate EXCEPT ![d] = [u \in Versions |->
                 IF u = v THEN "active"
                 ELSE IF u < v /\ vstate[d][u] \in {"active", "ingesting"} THEN "superseded"
                 ELSE vstate[d][u]]]
            /\ fstate' = [f \in Facts |-> IF f \in R THEN "retired" ELSE fstate[f]]
            /\ dirty' = IF Async THEN dirty \cup R ELSE dirty

ConsolidateApply(o) ==
  /\ obs[o].busy
  /\ LET snap == obs[o].snap
         commit(S) == [obs EXCEPT ![o] = [st |-> "live", src |-> S, deriv |-> snap, snap |-> {}, busy |-> FALSE]]
         abort     == [obs EXCEPT ![o].snap = {}, ![o].busy = FALSE]
     IN CASE ApplyCheck = "batch" -> obs' = IF snap \subseteq Live THEN commit(snap) ELSE abort
          [] ApplyCheck = "cited" -> obs' = IF snap \cap Live /= {} THEN commit(snap \cap Live) ELSE abort
          [] ApplyCheck = "none"  -> obs' = commit(snap)

RecallFacts == IF FilterIndexByStore THEN Index \cap Live ELSE Index
NoDeletedContentRecalled ==
  /\ (RecallFacts \cup GraphArm) \cap deleted = {}
  /\ \A o \in RecallObs : obs[o].deriv \cap deleted = {}
```

**What the counterexamples say.** Each is a four-to-eight step trace TLC printed:

1. `CommitChecksVersion = FALSE` (depth 5): `StartRetain(d1,1)`, `Delete(d1)` acked,
   `CommitChunk(d1,1,h)` — the in-flight retain resurrects the deleted document; the fact is
   live and belongs only to a deleted version, and the next recall returns it. **Rule:**
   `CommitChunk` locks the `document_versions` row `FOR SHARE` and requires
   `status = 'ingesting'`; `Delete` updates that row in its cascade transaction, so the two
   serialise and the late commit fails with `FAILED_PRECONDITION/VersionSuperseded` (ND-2).
2. `FinalizeChecksNewer = FALSE` (depth 7): v1 commits h1; v2 starts and commits h2;
   `FinalizeVersion(v1)` retires h2 because v1 does not contain it; `FinalizeVersion(v2)`
   then activates a version whose chunk is retired — the newest content is invisible until
   the next retain. **Rule:** `FinalizeVersion(u)` first reads `documents.current_version`
   and the set of started versions `FOR UPDATE`; if a newer version exists it only marks `u`
   superseded; the retire set is computed by the newest version only (ND-2).
3. `ApplyCheck = "cited"` (depth 8): consolidation reads a batch spanning d1 and d2; d1 is
   deleted and acked; the apply transaction drops the deleted sources but keeps the text,
   which was written with d1's facts in the prompt. `NoDeletedContentRecalled` fails on the
   observation. `ApplyCheck = "none"` (depth 6) fails one step earlier on
   `NoObservationCitesDeletedAfterAck`. **Rule:** the apply transaction re-reads every fact
   of the batch and every source fact of the candidate observations `FOR SHARE` with
   `retired_at IS NULL`; if any is missing the whole proposal is discarded and the batch is
   re-queued; D8's `observation_sources` cascade runs on `observation_inputs` (every fact in
   the prompt) and an observation that loses an *input* is hidden (`stale_delete`) until
   reconsolidated (ND-3). This is the one place the model changed the register's behaviour:
   D8 as written keeps a stale observation visible, which contradicts D16's "nothing from the
   document is returned after the ack".
4. `FilterIndexByStore = FALSE` (depth 6): `CommitChunk`, `Relay` (index has f), `Delete`
   acked — the store retired f, the index has not caught up, recall returns f. **Rule:**
   results from an `Async` index are joined with `facts.retired_at IS NULL AND
   invalidated_at IS NULL` before ranking (ND-9); the transactional MVP index does not need
   it but the `index.Index` contract requires it so the external-engine adapter cannot forget.

A note on the async-index abstraction: `Relay(f)` sets the index entry for `f` to the store's
current state instead of replaying `f`'s events one by one. That is sound for the safety
properties because the read-time join makes any intermediate index state invisible (an
entry can only be an extra candidate that the join removes, or a missing one), and the per-fact
delivery order that would make the exact replay well-defined is proved separately in
`Outbox.tla`. The counterexample without the join needs only one stale entry, which the
abstraction has.

#### 7.2.2 `Consolidation.tla` — exactly-once effect under at-least-once execution

**State.** `live` facts; `queue` of batches (sets of fact ids — `batch_key` is the sorted id
list, so the set *is* the key); `stored`, the durable proposal store keyed by batch; `mem`,
the volatile proposal of the current attempt; `applied` (recorded `op_key`s), `applyCount`
and `effectOf` per key (the auditing variables), `pending` (effect applied, key not yet
recorded, only when `AtomicKeyRecord = FALSE`), `done`/`failed` batches, `finalProp` (the
proposal in force when a batch completed), `obs[o]` with status and sources, crash and
delete counters.

**Actions.** `ProposeOk(B, P)`: the LLM returned proposal `P` (any list of one or two
create/update/delete ops over the observation ids); it is recorded once (`ON CONFLICT DO
NOTHING`). `ProposeFail(B)`: bisect into halves by sorted id, or mark a singleton failed.
`ApplyAtomic(B)`: apply the next unapplied op of the stored proposal and record its key in the
same transaction; the effect re-reads the batch's live facts (`S = B ∩ live`) and drops the
op if none remain, if an `update`/`delete` targets a non-live observation or a `create`
targets an existing one — but always records the key. `MarkDone(B)` when every key is
recorded. `Crash` loses `mem` and `pending`; Temporal's re-execution is simply the actions
remaining enabled. `DeleteFact(f)` is the concurrent cascade plus the source-count trigger.

**Invariants.** `ExactlyOnceEffect`: `applyCount[k] ≤ 1` for every key, and for every
completed batch every op of its final proposal has a recorded key, count 1, and the effect
recorded under that key *is that op*. `ObservationHasSources`: a live observation cites at
least one fact and only live ones. **Liveness** `RoundTerminates ≡ ◇(queue = ∅)` under weak
fairness of the worker (bisection makes progress on every failure, so the LLM may fail
forever and the round still ends with every singleton done or failed).

```tla
ProposeOk(B, P) ==
  /\ B \in queue /\ B \notin done /\ ~HasProp(B)
  /\ IF DurableProposal THEN stored' = stored @@ (B :> P) /\ UNCHANGED mem
                        ELSE mem' = mem @@ (B :> P) /\ UNCHANGED stored

ApplyAtomic(B) ==
  /\ AtomicKeyRecord
  /\ B \in queue /\ HasProp(B) /\ Unapplied(B) /\ pending = {}
  /\ LET i == NextIndex(B) IN LET op == PropStore[B][i] IN LET k == <<B, i>> IN
     /\ obs' = Effect(op, B)
     /\ applied' = applied \cup {k}
     /\ applyCount' = [applyCount EXCEPT ![k] = @ + 1]
     /\ effectOf' = [effectOf EXCEPT ![k] = op]

Crash ==
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1 /\ mem' = << >> /\ pending' = {}

ExactlyOnceEffect ==
  /\ \A k \in Keys : applyCount[k] <= 1
  /\ \A B \in done : \A i \in 1..Len(finalProp[B]) :
       /\ <<B, i>> \in applied
       /\ applyCount[<<B, i>>] = 1
       /\ effectOf[<<B, i>>] = finalProp[B][i]
```

**Counterexamples.** `DurableProposal = FALSE`: `ProposeOk([create o1])`,
`ApplyAtomic` (key (B,1) = create o1), `Crash`, `ProposeOk([update o1])` — the re-run LLM
call returns a different list — `MarkDone`: key (B,1) exists so the apply loop skips it and
the batch completes with `update o1` never applied and `create o1` recorded under its key.
`AtomicKeyRecord = FALSE`: effect, crash before the key insert, effect again; `applyCount =
2`. **Rules (ND-8):** the proposal is persisted write-once in `consolidation_batches
(batch_key, ops, observation ids for creates)` before any op is applied; the apply activity
reads the stored proposal, never the LLM; `op_key = sha256(batch_key ‖ op_index)` therefore
names a fixed op; each op's effect and its `consolidation_applied` row are one transaction.
D12's text is compatible with this but did not say it; without it the keys are decorative.

#### 7.2.3 `Outbox.tla` — ordering and no-loss through sequence gaps

**State.** `nextSeq`; writers `wr[w]` with `st ∈ {idle, holding}`, their `seq`, namespace and
`age` in ticks since the draw; `committed` and `aborted` seq sets; `nsOf`; the relay's persisted
`cursor`, its in-memory `watch` (seq ↦ age since first seen as a gap), `declared` (seqs it
declared aborted) and `log[c]` per consumer (duplicates allowed); a crash counter.

**Actions.** `Draw(w, n)` = `nextval()` inside the transaction; `Commit(w)` only while
`age ≤ Timeout`; `Abort(w)`; `Tick` ages holders and watch entries and cancels any holder at
`age = Timeout` (the database's `statement_timeout`/`idle_in_transaction_session_timeout`);
`DeliverPersist` delivers `cursor + 1` to every consumer and persists the cursor;
`DeliverCrash` delivers and crashes before persisting (the watchlist is lost, timers restart,
the seq is re-delivered after restart); `WatchGap` starts a timer the first time
`cursor + 1` is missing while a larger seq is visible; `DeclareAborted` moves the cursor past a
watched seq whose timer reached `Watch` while it is still invisible. The relay is strictly
prefix-ordered: it never delivers past an unresolved seq (ND-1).

**Invariants.** `NoLossSafety`: `declared ∩ committed = ∅` and every committed seq below the
cursor is in every consumer's log. `PerNamespaceOrder`: the first delivery of each seq is in
seq order per consumer, hence per namespace (duplicates from `DeliverCrash` are deduplicated
by the consumer on `seq`). `OnlyCommittedDelivered`. `IdempotentConsumerState`: the
consumer's effective state — a set — is the committed prefix regardless of duplicates.
**Liveness** `NoLossLive ≡ ∀ s: (s ∈ committed) ⇝ (s delivered everywhere)` under weak
fairness of the relay, of `Tick` and of each writer's commit-or-abort.

```tla
Tick ==
  /\ wr' = [w \in Writers |->
              IF w \in Overdue THEN [wr[w] EXCEPT !.st = "idle"]
              ELSE IF wr[w].st = "holding" THEN [wr[w] EXCEPT !.age = @ + 1]
              ELSE wr[w]]
  /\ aborted' = aborted \cup {wr[w].seq : w \in Overdue}
  /\ watch' = [s \in DOMAIN watch |-> IF watch[s] < Watch THEN watch[s] + 1 ELSE watch[s]]

GapObserved == Head1 \notin committed /\ \E t \in committed : t > Head1

WatchGap ==
  /\ GapObserved /\ Head1 \notin DOMAIN watch
  /\ watch' = watch @@ (Head1 :> 0)

DeclareAborted ==
  /\ Head1 \notin committed /\ Head1 \in DOMAIN watch /\ watch[Head1] >= Watch
  /\ declared' = declared \cup {Head1}
  /\ cursor' = Head1
  /\ watch' = RemoveWatch(Head1)

NoLossSafety ==
  /\ declared \cap committed = {}
  /\ \A s \in committed : s <= cursor => \A c \in Consumers : s \in Range(log[c])
```

**The counterexample without the watchlist** (`Watch = 0`, depth 7, found in under a
second): w1 draws seq 1 for namespace a; w2 draws seq 2 for a and commits; the relay reads,
sees 2 but not 1, and — with no horizon — declares 1 aborted and moves its cursor to 1; w1
then commits seq 1 inside its timeout. Seq 1 is committed, below the cursor, and in no
consumer's log: the index never learns about that fact and the move consumer (`move:<ns>`)
never replays it. With `Watch = 2 = Timeout` the same shape appears two ticks later: the gap
is first seen at writer age 0, the timer reaches 2 at the same tick the writer reaches age 2,
and the writer is still allowed to commit — the horizon must exceed the writer bound by a
margin. The register's 2× horizon gives a margin of one full timeout for clock skew between
the relay's clock and the database's, and for the commit itself. Assumption **A-F1** (ND-1):
the outbox `INSERT` is the last statement of every write transaction and writers run with
`statement_timeout = idle_in_transaction_session_timeout = 30 s`, so draw-to-commit is
bounded by 30 s plus commit latency; the watchlist horizon is 60 s measured from the relay's
first observation of the gap, which is never earlier than the draw. The price is head-of-line
blocking of up to 60 s behind each aborted transaction; see ND-1 for the optional
`pg_current_snapshot()`-based acceleration that keeps the bound as the fallback.

#### 7.2.4 `AsOf.tla` — leak-free recall at T

**State.** `mentioned[f] ∈ 0..MaxTime` (0 = not retained); `versions[o]`, a sequence of
records `[cited, deriv, eff]` where `deriv` is every fact the LLM saw (the batch plus,
transitively, the `deriv` of every candidate observation version in the prompt) and `eff` is
`effective_at`.

**Actions.** `InsertFact(f, t)` in any order relative to `t` (backfills). `Consolidate(o,
batch, cands, cited)` appends a version with `eff = max mentioned_at over deriv`
(`EffectiveFromInputs = TRUE`, ND-4) or over `cited` only (D9's wording).

**Properties.** `NoLeak`: for every T and observation, the version `RetV(o, T)` = the latest
with `eff ≤ T` has `deriv ⊆ {f : mentioned[f] ≤ T}`; facts trivially. `EffectiveCoversCited`:
`eff ≥` the newest cited source (D9's definition is a lower bound of the design's).
`OlderVersionStable` (an action property): appending a version with `eff > T` does not change
`RetV(o, T)` — the older version is what recall(T) keeps returning, which is the "update after
T" scenario the task asked to check.

```tla
Consolidate(o, batch, cands, cited) ==
  /\ batch /= {} /\ batch \subseteq Live
  /\ cited /= {} /\ cited \subseteq batch
  /\ cands \subseteq (Obs \ {o}) /\ \A c \in cands : HasVersion(c)
  /\ Len(versions[o]) < MaxVersions
  /\ LET deriv == batch \cup UNION {Latest(c).deriv : c \in cands}
         effIn == Max({mentioned[f] : f \in deriv})
         effCi == Max({mentioned[f] : f \in cited})
         eff   == IF EffectiveFromInputs THEN effIn ELSE effCi
     IN versions' = [versions EXCEPT ![o] = Append(@, [cited |-> cited, deriv |-> deriv, eff |-> eff])]

RetV(vs, o, T) ==
  LET ok == {i \in 1..Len(vs[o]) : vs[o][i].eff <= T}
  IN IF ok = {} THEN 0 ELSE Max(ok)

NoLeak ==
  \A T \in Times :
    /\ \A f \in RecallFacts(T) : mentioned[f] <= T
    /\ \A o \in Obs : LET i == RetV(versions, o, T) IN
         i /= 0 => \A f \in versions[o][i].deriv : mentioned[f] <= T

OlderVersionStable ==
  [][\A o \in Obs : \A T \in Times :
       (Len(versions'[o]) > Len(versions[o]) /\ versions'[o][Len(versions'[o])].eff > T)
         => RetV(versions', o, T) = RetV(versions, o, T)]_vars
```

**The counterexample** (`AsOf_CitedOnly.cfg`, depth 4): f1 mentioned at 1, f2 at 2;
`Consolidate(o1, batch {f1, f2}, cited {f1})` gets `effective_at = 1`; recall with
`as_of = 1` serves a text written with f2 in the prompt. The fix is ND-4: `effective_at` is
the maximum over `observation_inputs` and over the `effective_at` of every observation
version in the prompt; cited sources are a subset of inputs so D9's rule still holds as a
bound. Pages get the same rule through `page_sources` (inputs, not citations).

#### 7.2.5 `ShardMove.tla` — fencing, no loss/dup, termination

**State.** `cat[n] = [shard, epoch, state]` (catalog truth); `own[s][n] = [epoch, state]`
(the ownership row on each shard); `cache[a][n] = [shard, epoch]` for API clients (refreshed
by `LISTEN`/TTL or after a `FAILED_PRECONDITION`) and workflows (pinned at start, re-pinned only
by the mover); `store[s][n]` as a bag `Writes → ℕ` so a duplicate shows as a count of 2;
per-write `wst`/`winfo` (shard, namespace, outbox seq, actor); `nextSeq[s]`; `accepted[n]`
(acked writes); the mover record `mv` (persisted state machine, `p0`, applied seqs); `moverUp`;
counters; `readViolation`.

**Actions.** `BeginWrite(a, n, w)`: at the cached route the fence either admits the
transaction (`own[s][n] = (active, e)` — it now holds `FOR SHARE` and has drawn a seq) or
rejects it; a rejected client refreshes its cache. `CommitWrite(w)` applies `ON CONFLICT DO
NOTHING` and acks; `AbortWrite`. `Read(a, n)` is accepted at `active` or `frozen` rows at the
caller's epoch and records whether it was stale or misrouted. Mover: `Plan` (target row
`incoming` at e+1, catalog `moving`), `Copy` (snapshot; with `CopyBarrier` it waits for the
source fence to be free), `Replay` (next committed seq above `p0` once every smaller seq is
resolved — the watchlist's guarantee), `Freeze` (waits for every `FOR SHARE` holder, exactly
as the `UPDATE` of the ownership row does), `Drained`, then the four cutover sub-steps
`CutTarget` → `CutSource` → `CutCatalog` → `RestartWorkflows` in the "safe" order (ND-6) or
`CutTarget` → `CutCatalog` → `CutSource` in D5's order, `Rollback` from any pre-cutover state,
`MoverCrash`/`MoverRestart` (persisted state survives, the step re-executes).

**Invariants.** `SingleWritableOwner`: at most one `active` row per namespace across shards.
`WritesOnlyAtOwner`: a transaction holding the fence is at the catalog's shard for the
namespace at the catalog's epoch (so every accepted write was). `NoLossNoDup`: at every
instant the catalog's shard holds exactly the accepted writes, once each — during the move
(owner = source) and after it (owner = target). `NoDupAnywhere`. `ReadsFresh`: an accepted
read sees every accepted write of the namespace and happened at the catalog's shard and
epoch (D16's read barrier across the move). **Liveness** `MoveTerminates` under weak fairness
of the mover, of every writer's commit-or-abort and of client refreshes.

```tla
FenceOK(s, n, e) == own[s][n].state = "active" /\ own[s][n].epoch = e
Holding(s, n) == {w \in Writes : wst[w] = "holding" /\ winfo[w].shard = s /\ winfo[w].ns = n}

Copy ==
  /\ moverUp /\ mv.st = "planned"
  /\ CopyBarrier => Holding(mv.src, mv.ns) = {}
  /\ store' = [store EXCEPT ![mv.dst][mv.ns] = store[mv.src][mv.ns]]
  /\ mv' = [mv EXCEPT !.st = "catching_up", !.p0 = MaxOr0(CommittedSeqs(mv.src, mv.ns)), !.applied = {}]

Replay ==
  /\ moverUp /\ mv.st \in {"catching_up", "drained"}
  /\ \E w \in NextReplay :
       /\ Resolved(mv.src, mv.ns, winfo[w].seq)
       /\ store' = [store EXCEPT ![mv.dst][mv.ns][w] = @ + 1]
       /\ mv' = [mv EXCEPT !.applied = @ \cup {winfo[w].seq}]

Freeze ==
  /\ moverUp /\ mv.st = "catching_up" /\ Lag <= 1
  /\ Holding(mv.src, mv.ns) = {}
  /\ own' = [own EXCEPT ![mv.src][mv.ns].state = "frozen"]
  /\ cat' = [cat EXCEPT ![mv.ns].state = "frozen"]
  /\ mv' = [mv EXCEPT !.st = "frozen"]

NoLossNoDup ==
  \A n \in NS : \A w \in Writes :
    store[cat[n].shard][n][w] = IF w \in accepted[n] THEN 1 ELSE 0
```

**Counterexample 1 — `ShardMove_NoBarrier.cfg`, depth 12.** x1 begins at the source (seq 1);
x2 begins (seq 2) and commits; `Plan`; `Copy` takes its `REPEATABLE READ` snapshot: it sees
x2, not x1, and records `p0 = 2`; x1 commits — legally, the source is still `active` at the
same epoch; `Freeze` (no holders left), `Drained` (nothing with seq > 2), `CutTarget`,
`CutSource`, `CutCatalog`: the target is the owner and lacks x1. D5 step 2 as written loses
exactly the writes whose transactions straddle the snapshot. **Rule (ND-5):** the copy opens
with a *barrier*: the mover takes `SELECT … FOR UPDATE` on the source ownership row (which
waits for every in-flight `FOR SHARE` holder and blocks new writers for the few milliseconds
it is held), opens the `REPEATABLE READ` snapshot on a second connection, reads `p0 =
max(seq)` for the namespace inside that snapshot, then releases the lock. With no holder at
snapshot time every drawn seq ≤ p0 is committed or aborted, so copy + replay of `seq > p0`
is exactly once.

**Counterexample 2 — `ShardMove_D5Order.cfg`, depth 8.** D5 step 6 lists the catalog
switch, the target row and the source row as "one catalog transaction"; they live in three
databases. TLC's trace needs no write at all: `Plan`, `Copy`, `Freeze`, `Drained`,
`CutTarget`, `CutCatalog` (the catalog now names the target at epoch 2 while the source row
is still `frozen` at epoch 1), then `Read` by a client whose cache still says (source, 1):
the source accepts it (reads are allowed at `frozen`), so a read for N was served by a shard
that is not `shard(N)` at the current epoch — the task's first safety requirement. One
`CommitWrite` at the target by a refreshed client turns the same trace into a stale read that
misses an acknowledged write, which is D16's read barrier breaking across a move. **Rule
(ND-6):** persist `cutover` in `namespace_moves` (the point of no return), set the target row
`active`, set the source row `moved_out`, *then* switch the catalog and `NOTIFY`, then
restart the recorded workflows on the target queue. In the window between the source row and
the catalog switch every request for the namespace fails with
`FAILED_PRECONDITION/WrongShardOrEpoch`, which the API already retries (D5 step 4); nothing
can be stale because nothing is served.

### 7.3 Lean 4 theorems

Files under `formal/lean/Engram/`, Lean 4 core only (no Mathlib, no Batteries). None of them
has been type-checked; the "proof" column is honest about what is written out and what is
`sorry` with a comment. Each file ends with `example … := by decide` checks that double as
the table test of its Go counterpart.

| File | Definition | Theorem | Proof status |
|---|---|---|---|
| `TagMatch.lean` | `Mode` (unfiltered, any, anyStrict, all, allStrict, exact); `subset`, `inter`; `Matches m Q I` exactly as D10; `matches := decide` | `Decidable (Matches m Q I)` for all modes (`List.decidableBAll`/`decidableBEx`) | written (`cases m <;> infer_instance`) |
| | | `anyStrict_imp_any`, `allStrict_imp_all` | written (one-liners) |
| | | `exact_imp_allStrict` (needs `Q ≠ []`), `allStrict_imp_anyStrict` (needs `Q ≠ []`), `exact_symm` | written |
| | | `anyStrict_mono`, `allStrict_mono` (monotone in I), `allStrict_anti` (antitone in Q) | written |
| | | ANY and ALL are *not* monotone in I (untagged item passes, tagged one may not) | `decide` on the witness |
| `RRF.lean` | `CommAdd` (comm/assoc/zero), `sumList`, arms as `Doc → Option Nat`, `contribOf`, `score contrib arms d = Σ contribOf` | `sumList_perm` (sum invariant under `List.Perm`) | written (induction on `Perm`) |
| | | `score_perm`, `order_perm`: reordering arms changes no score, hence no fused order | written |
| | `OrderedCommAdd`, `Antitone contrib`, `Improves a a' d`, `setArm` | `contribOf_le_of_improves` (per-arm monotonicity) | written (case split) |
| | | `score_mono`: improving `d` in one arm never lowers its fused score | `sorry` (routine induction over `List.set`) |
| | `contribNat D k r = D / (k + r)` (exact Nat scaling) | `contribNat_antitone` (needs `0 < k`: with k = 0, `D/0 = 0` breaks antitonicity, which is exactly why D10 fixes k = 60), `contribNat_le`, `contribNat_pos` | written (Nat division lemmas) |
| | | `score_le_bound`: `score ≤ |arms| · D/(k+1)` | `sorry` (list induction) |
| `Packer.lean` | `keep`/`skipped`/`pack` (greedy, skip-not-truncate), `total` | `pack_total_le`: kept total ≤ budget | written (induction, `omega`) |
| | | `pack_sublist`: output order = input order (`List.Sublist`) | written |
| | | `keep_count`: kept + skipped = length | written |
| | | `keep_append`: prefix-closed selection (later items never change earlier decisions; the scan is a left fold) | written (induction, `Nat.sub_sub`) |
| | | `keep_skip_oversize`: an oversize item is skipped and the scan continues | written (`simp`) |
| | | `kept_fits`: a kept item fitted the budget remaining when it was reached | `sorry` |
| `TemporalWindow.lean` | `Window` (lo ≤ hi), `overlaps`, `contains`, `distanceTo` | `overlaps_symm`, `overlaps_refl`, `overlaps_iff_exists` (an instant lies in both) | written |
| | | overlap is not transitive (witness) | `decide` |
| | | `contains_refl`, `contains_trans`, `contains_antisymm`, `contains_imp_overlaps`, `overlaps_of_contains` | written |
| | | `distanceTo_nonneg`, `distanceTo_eq_zero_iff` (= 0 ⇔ anchor inside), `distanceTo_mono` (widening never increases distance), `distanceTo_lipschitz` (1-Lipschitz in the anchor), `distance_total` | written (`omega` after `split`) |

Why these four and not more: they are the pure functions on the recall path whose bugs are
silent (a packer that truncates, a fusion that depends on arm order, a tag mode off by an
empty-list case, an overlap test that is not symmetric) and whose statements are short
enough that a `sorry` count of zero is a realistic target in phase 2.

### 7.4 Conformance: how the Go code stays faithful

Three mechanisms, all three for every spec. A spec that is not tied to a test is
documentation, not verification.

**(a) Property-based model tests with `pgregory.net/rapid`.** For each spec there is an
in-memory Go model of exactly the TLA+ state and actions, and a `rapid` state machine test
that generates the same action sequences, runs them against (i) the model and (ii) the real
implementation on a `testcontainers` Postgres, and checks the TLA+ invariants after every
step. The generators use the TLC bounds (same constants), so the test explores the same
state space statistically that TLC explored exhaustively; the point of (ii) is that the real
SQL, locks and triggers are under test, not a reimplementation.

| Spec | Go package / files | What the real side is |
|---|---|---|
| `DocLifecycle` | `internal/store/doclifecycle_model_test.go` (model + rapid machine), `internal/store/doclifecycle_pg_test.go` | `store.CommitChunk`, `FinalizeVersion`, `DeleteDocument`, `Purge`, `consolidate.Apply` against one Postgres; the async index is `index.Async` backed by a fake engine fed by the real relay |
| `Consolidation` | `internal/consolidate/apply_model_test.go`, `internal/consolidate/apply_pg_test.go`, `internal/workflows/consolidate_test.go` (Temporal `testsuite` with activity retries and a `hooks.CrashAfterOp(i)` panic hook) | `consolidation_batches`/`consolidation_applied` tables, the apply transaction, the source-count trigger |
| `Outbox` | `internal/outbox/relay_model_test.go`, `internal/outbox/relay_pg_test.go` (writers delayed with `pg_sleep` before `COMMIT`, `statement_timeout = 2s`, watch 4 s, relay process killed between deliver and cursor persist) | `outbox.Relay`, `outbox_cursors`, consumer fakes that record first-delivery order |
| `AsOf` | `internal/recall/asof_model_test.go`, `internal/recall/asof_pg_test.go` | `observation_versions.effective_at` computation in `consolidate.Apply`, the `as_of` predicates of every arm |
| `ShardMove` | `internal/move/move_model_test.go`, `internal/move/move_pg_test.go` (two Postgres containers + one catalog container; `catalog.Resolver` with invalidation disabled to force staleness; mover killed at every persisted state) | `move.Executor`, `namespace_ownership`, the fence in `store.Tx`, the catalog transaction |
| Lean modules | `internal/recall/tags_test.go` (exhaustive over the `decide` universe), `fuse_test.go`, `pack_test.go`, `temporal_test.go` (rapid) | the pure functions |

The invariant code is written once per spec in `internal/formal/<spec>/invariants.go` and
imported by both the model test and the Postgres test, so the two cannot drift.

**(b) Trace validation.** Every store, relay, consolidation and move code path emits a
structured decision log in tests: `internal/formal/trace.Logger` writes JSON lines
`{"action":"CommitChunk","args":{"d":"d1","v":1,"h":"h2"},"seq":17}` from the exact points
where the TLA+ action is considered to have happened (after the transaction commits). The
converter `engramctl formal trace-to-tla <spec> <log.jsonl>` emits a module
`<Spec>Trace.tla` that `EXTENDS` the spec, declares the trace as a constant sequence, and
defines `TraceNext ≡ Next ∧ (the enabled action at index i is the logged one with the logged
arguments)` with a `TraceInit` that maps the logged initial state onto `Init`. TLC then checks
that the logged behaviour is a behaviour of the spec (`TraceNext` reaches the end of the
sequence) and that every invariant held along it. This is the standard "TraceCheck" pattern;
a trace that the spec rejects is either a bug in the code or a spec that is too strict, and
either is worth a failing test. Trace validation runs on the Postgres tests of (a) (they
already produce the logs) and on the fault-injection suites of section 8 (stale catalog,
misrouted requests, crash mid-move), which is how a chaos run becomes a checked artefact
rather than a green log.

**(c) Refinement mapping.** The table the reviewer uses to check that a Go change is still
"the same state machine". The mapping is also the contract for the model test's state
extraction function.

| Spec variable | Go / Postgres state |
|---|---|
| `DocLifecycle.fstate[f]` | `facts` row for `(namespace_id, document_id, content_hash)`: absent / `retired_at IS NULL` / `retired_at NOT NULL` |
| `vstate[d][v]`, `vcontent`, `vpending` | `document_versions.status`; the version's chunk-hash set from `chunks`; the Temporal workflow's per-chunk completion state |
| `deleted` | facts whose document delete operation is `SUCCEEDED` and that have no later `CommitChunk` |
| `links` | `fact_links` rows (both endpoints) |
| `obs[o].src` / `.deriv` / `.st` | `observation_sources` / `observation_inputs` / `observations.state` ∈ {active, stale_delete, retired} |
| `index`, `dirty` | the external engine's document set / outbox rows for the `index` consumer above its cursor |
| `Consolidation.stored[B]` | `consolidation_batches(batch_key, ops)` |
| `applied`, `effectOf` | `consolidation_applied(op_key, op_json)` |
| `Outbox.committed` / `cursor` / `watch` | visible `outbox` rows / `outbox_cursors.last_seq` / `relay.gaps` in memory |
| `AsOf.versions[o][i].eff` / `.deriv` | `observation_versions.effective_at` / `observation_inputs` for that version |
| `ShardMove.cat[n]` | `engram_catalog.namespaces(shard_id, epoch, state)` |
| `own[s][n]` | `namespace_ownership` on shard `s` |
| `cache[a][n]` | `catalog.Resolver` entry on API instance `a`; workflow input `(shard_id, epoch)` for workflows |
| `mv` | `namespace_moves` row + `move_applied_seq` on the target |
| `store[s][n]` as a bag | row counts per logical id across `facts`, `fact_links`, `observations` on shard `s` for namespace `n` (a count of 2 is a primary-key violation, which is why the bag models `ON CONFLICT DO NOTHING` separately from replay) |

### 7.5 What is not formalised, and why

- **LLM outputs** (extraction, consolidation ops, reflect answers): non-deterministic
  external input. The specs model them as unconstrained nondeterministic choices from a small
  menu, which is the right abstraction for safety properties and useless for quality
  properties. Quality is measured in section 8 (LongMemEval, LoCoMo), not proved.
- **Ranking quality and HNSW recall**: approximate nearest-neighbour recall is an empirical
  property of the index parameters (D3); no model would tell us anything the ablation does
  not.
- **Temporal's own guarantees** (at-least-once activity execution, workflow determinism,
  signal delivery): assumed as documented; the specs model only their observable consequence
  (an activity may run again after any crash). Workflow replay determinism is enforced by
  Temporal's replay tests, not by us.
- **pgbouncer transaction pooling**: assumed to preserve per-transaction connection affinity
  (which is what `SET LOCAL` and `FOR SHARE` need). A `pgbouncer` bug is out of scope.
- **JWT verification, JWKS rotation, the authz interceptor** (D13, N5): single-threaded
  predicate logic with no interesting interleavings; covered by table tests and the
  isolation suites of section 8.
- **The cross-encoder and the gateway**: external services; only their latency budget is
  relevant (D3) and that is measured.
- **Kafka broker semantics beyond per-partition order**: the `kafka` consumer in `Outbox.tla`
  is a log; Kafka's own durability, rebalancing and exactly-once modes are not modelled. The
  only property we rely on is that the key (`namespace_id`) fixes the partition, so
  per-namespace order survives.
- **Backup/restore and the epoch bump on restore** (D1): a restore is modelled as nothing
  more than "a new epoch", which the fence already handles; the restore procedure itself is
  operational.
- **Quota deferral, metering, config inheritance**: no concurrency worth a model.
- **Postgres MVCC itself**: the specs assume snapshot isolation semantics for
  `REPEATABLE READ`, lock conflicts between `FOR SHARE` and `UPDATE`/`FOR UPDATE`, and that a
  committed transaction is visible to snapshots taken after its commit. These are the
  documented semantics; the Postgres tests of 7.4(a) are where the assumption meets reality.

### 7.6 CI integration

- **Nightly `formal-tlc` job** (`.github/workflows/formal-nightly.yml`): pins
  `tla2tools.jar` by SHA-256 (the `de.hhu.stups:tlatools:1.1.0` jar used here, TLC 2.18, or
  the upstream `tla2tools` release once the egress policy allows GitHub downloads), runs every
  `*.cfg` under `formal/tla/` with `-workers auto`, 8 GB heap and `timeout 900` per
  configuration; a design configuration must end with "No error has been found" and a
  counterexample configuration (`*_No*.cfg`, `*_CitedOnly.cfg`, `*_Watch1x.cfg`,
  `*_VolatileProposal.cfg`, `*_NonAtomicKey.cfg`, `*_D5Order.cfg`, `*_UnfilteredIndex.cfg`)
  must end with "Invariant … is violated" on the invariant named in the cfg's first comment
  line; anything else (timeout, parse error, a *different* invariant failing) fails the job.
  Logs and `-dump dot` state graphs of the counterexamples are uploaded as artefacts. The
  runtime budget from the measured runs is about 25 minutes on 4 cores.
- **PR job `formal-quick`**: parses every spec (`tlc2.TLC -parse` equivalent via SANY),
  runs the counterexample configurations and the `_Live` configurations (all under two
  minutes in the measured runs) so a spec edit that breaks parsing or silently weakens an
  invariant is caught before merge.
- **Lean**: `formal/lean/` is a Lake project (`lakefile.lean`, `lean-toolchain` pinned to a
  specific `leanprover/lean4:v4.x`); the PR job runs `lake build` (no Mathlib, so the build is
  seconds) and `scripts/sorry-count.sh`, which fails if the number of `sorry` occurrences
  exceeds the checked-in baseline `formal/lean/SORRY_BASELINE` (currently 3, the three rows
  marked `sorry` in 7.3). Lowering the baseline is the only way to touch that file.
- **Spec ↔ test coupling**: `formal/MANIFEST.md` maps each `.tla` and `.lean` file to its Go
  test files (the table in 7.4). `scripts/formal-manifest-check.sh` runs on every PR and
  fails when a spec file changed without a change in at least one of its mapped test files,
  unless the PR carries the `formal-no-test-change` label with a one-line justification in
  the description (used for comment-only edits). `CODEOWNERS` routes `formal/**` to the
  formal-methods owner and the owner of the mapped Go package.
- **Trace validation in CI**: the Postgres tests of 7.4(a) write their JSON-line traces to
  `$TEST_ARTIFACTS/traces/`; a final step of the integration job converts and checks every
  trace with TLC (`-workers 1`, these are linear behaviours and take seconds each). A chaos
  run from section 8 that produces an unexplainable trace fails here, not in a dashboard.

### New decisions (beyond the register; to be folded into D18)

| Id | Decision | Replaces / clarifies | Rejected alternative |
|---|---|---|---|
| ND-1 | The outbox relay is strict-prefix: its cursor never passes an unresolved `seq`; a gap is watched for 60 s = 2 × the writer bound from the relay's first observation, then declared aborted. Assumption A-F1: writers set `statement_timeout = idle_in_transaction_session_timeout = 30 s` and the outbox `INSERT` is the last statement before `COMMIT`. Optional acceleration (phase 2): record `pg_current_snapshot()` with each poll and resolve a gap as soon as no transaction that was in progress at first observation is still in progress (xid8, wraparound-safe); the 60 s timer remains the fallback bound. | D6 "skipped seq re-checked" | Delivering past a gap (breaks per-namespace order for the move consumer); serialising writers on a lock. |
| ND-2 | `CommitChunk` runs inside a transaction that locks the `document_versions` row `FOR SHARE` and requires `status = 'ingesting'`; `Delete` and `FinalizeVersion` update that row, so a late commit fails `FAILED_PRECONDITION/VersionSuperseded` and the operation ends `FAILED{reason: superseded}`. `FinalizeVersion(u)` marks `u` superseded without retiring anything if a newer version of the document has been started; only the newest version computes the retire set. | D8 row 3, D11 | Rejecting a second retain while one is in flight (breaks the "replace is the upsert" promise under bursty clients). |
| ND-3 | `observation_inputs(observation_id, version, fact_id)` records every fact in the consolidation prompt; the apply transaction re-reads all inputs and the source facts of every candidate observation `FOR SHARE … retired_at IS NULL` and discards the whole proposal if any is missing (batch re-queued). D8's delete cascade runs on inputs ∪ sources; an observation that loses an input gets `state = stale_delete` and is excluded from recall until reconsolidated; `stale_write` (new evidence) stays visible. | D8 row 4, D12 | Keeping stale observations visible (leaks deleted text until the next consolidation, contradicting D16). |
| ND-4 | `observation_versions.effective_at = max(mentioned_at over observation_inputs ∪ effective_at of every observation version in the prompt)`; cited sources are a subset of inputs. Same for `page_versions` via `page_sources` recorded as inputs. | D9 row 2 | max over cited sources only (TLC leak in 7.2.4). |
| ND-5 | Copy barrier: D5 step 2 starts with `SELECT … FROM namespace_ownership WHERE namespace_id = $1 FOR UPDATE` on the source (held for milliseconds), opens the `REPEATABLE READ` snapshot on a second connection, reads `p0 = max(seq)` for the namespace in that snapshot, releases. | D5 step 2 | `p0` = the relay's safe cursor (correct but couples move latency to relay lag). |
| ND-6 | Cutover is four idempotent sub-steps across three databases, in this order: persist `cutover` in `namespace_moves`; target ownership `active`; source ownership `moved_out`; catalog switch + `NOTIFY`; then restart recorded workflows on the target queue. No rollback after the first sub-step. | D5 step 6 "one catalog transaction" | catalog switch before source `moved_out` (TLC `ReadsFresh` violation). |
| ND-7 | Catch-up stops waiting for `lag < 100` after 10 rounds and freezes anyway; drain then runs longer but the move always reaches `frozen`. | D5 step 3 | unbounded catch-up (liveness fails under sustained writes). |
| ND-8 | Consolidation proposals are persisted write-once in `consolidation_batches(batch_key, ops, created_observation_ids)` before any op is applied; the apply activity reads the stored proposal; `op_key = sha256(batch_key ‖ op_index)` over the stored list; each op's effect and its `consolidation_applied` row are one transaction. Failed LLM calls store nothing (bisect/retry). | D12 row 1 | keys over the live LLM output (TLC counterexample in 7.2.2). |
| ND-9 | Every `index.Index` implementation's query results are joined with `facts.retired_at IS NULL AND invalidated_at IS NULL` (and the `as_of` predicate) before ranking; `Transactional` implementations may skip the join by contract only if the test suite proves it is redundant. | D7 | trusting the index. |
| ND-10 | Formal artefacts are part of the definition of done: a PR that changes a modelled protocol changes the spec and the mapped test in the same PR (`formal/MANIFEST.md`, label escape hatch). | — | nightly-only checking. |
