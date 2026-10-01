--------------------------- MODULE DocLifecycle ---------------------------
(***************************************************************************)
(* Engram D8/D11/D12/D7: document replace and delete racing with           *)
(* per-chunk retain commits, consolidation, purge and index sync.          *)
(*                                                                         *)
(* Facts are identified by (document, chunk content hash).  A document has *)
(* numbered versions; a retain of version v commits one chunk per          *)
(* transaction (CommitChunk) and then FinalizeVersion retires the chunks   *)
(* the new version no longer contains.  Delete(document) retires all its   *)
(* facts, deletes its links and observation_sources rows synchronously     *)
(* and acks; physical purge is asynchronous.  Consolidation reads a batch  *)
(* of live facts, calls the LLM (outside any transaction) and applies the  *)
(* result in a transaction.  The search index is either transactional      *)
(* (IndexMode = "tx": same transaction as the facts) or asynchronous       *)
(* (IndexMode = "async": fed by the outbox relay, whose per-fact order is   *)
(* established by Outbox.tla, so it is abstracted here as "each dirty fact  *)
(* is eventually re-synced to the store").                                  *)
(*                                                                         *)
(* Design knobs (TRUE / "batch" is the design; the others are the bugs TLC *)
(* finds):                                                                 *)
(*   CommitChecksVersion : CommitChunk requires document_versions.status = *)
(*       'ingesting' in the same transaction (a deleted or superseded      *)
(*       version cannot resurrect content).                                *)
(*   FinalizeChecksNewer : FinalizeVersion(u) retires nothing and marks u  *)
(*       superseded when a newer version has already been started.         *)
(*   (ND-11) REPLACE-retired facts keep their observation_sources rows until  *)
(*       purge; the design run of this spec is what exposed the gap.         *)
(*   ApplyCheck : what the consolidation apply transaction re-verifies     *)
(*       with FOR SHARE: "batch" = every fact the LLM saw is still live    *)
(*       (else the proposal is discarded); "cited" = only cited facts are  *)
(*       filtered (text derived from deleted facts survives); "none".      *)
(*   FilterIndexByStore : async index hits are joined with                 *)
(*       facts.retired_at IS NULL at read time.                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Docs,                \* <= 2 documents
          Hashes,              \* <= 3 chunk content hashes per document
          MaxVersion,          \* <= 2 versions per document
          Obs,                 \* <= 3 observations
          MaxConsolidations,   \* consolidation apply attempts per behaviour
          IndexMode,           \* "tx" | "async"
          FilterIndexByStore,  \* BOOLEAN
          CommitChecksVersion, \* BOOLEAN
          FinalizeChecksNewer, \* BOOLEAN
          ApplyCheck           \* "batch" | "cited" | "none"

ASSUME IndexMode \in {"tx", "async"} /\ ApplyCheck \in {"batch", "cited", "none"}

Facts == Docs \X Hashes
Versions == 1..MaxVersion
FactsOf(d) == {<<d, h>> : h \in Hashes}
VStates == {"none", "ingesting", "active", "superseded", "deleted"}

VARIABLES
  fstate,    \* [Facts -> {"absent","live","retired"}]  (purged = absent)
  vstate,    \* [Docs -> [Versions -> VStates]]
  vcontent,  \* [Docs -> [Versions -> SUBSET Hashes]]  chunk set of the version
  vpending,  \* [Docs -> [Versions -> SUBSET Hashes]]  chunks not yet committed
  deleted,   \* SUBSET Facts: content of acknowledged deletes, not re-retained since
  links,     \* SUBSET (SUBSET Facts): unordered pairs {f, g}
  obs,       \* [Obs -> [st, src, deriv, snap, busy]]
  index,     \* SUBSET Facts: the async index's notion of live facts
  dirty,     \* SUBSET Facts: facts with an outbox event not yet applied to the index
  ncons      \* consolidation attempts so far

vars == <<fstate, vstate, vcontent, vpending, deleted, links, obs, index, dirty, ncons>>

ObsRec == [st : {"absent", "live", "stale", "retired"},
           src : SUBSET Facts, deriv : SUBSET Facts, snap : SUBSET Facts, busy : BOOLEAN]

TypeOK ==
  /\ fstate \in [Facts -> {"absent", "live", "retired"}]
  /\ vstate \in [Docs -> [Versions -> VStates]]
  /\ vcontent \in [Docs -> [Versions -> SUBSET Hashes]]
  /\ vpending \in [Docs -> [Versions -> SUBSET Hashes]]
  /\ deleted \subseteq Facts
  /\ \A l \in links : l \subseteq Facts /\ Cardinality(l) = 2
  /\ obs \in [Obs -> ObsRec]
  /\ index \subseteq Facts /\ dirty \subseteq Facts
  /\ ncons \in 0..MaxConsolidations

Live == {f \in Facts : fstate[f] = "live"}
Async == IndexMode = "async"

Init ==
  /\ fstate = [f \in Facts |-> "absent"]
  /\ vstate = [d \in Docs |-> [v \in Versions |-> "none"]]
  /\ vcontent = [d \in Docs |-> [v \in Versions |-> {}]]
  /\ vpending = [d \in Docs |-> [v \in Versions |-> {}]]
  /\ deleted = {}
  /\ links = {}
  /\ obs = [o \in Obs |-> [st |-> "absent", src |-> {}, deriv |-> {}, snap |-> {}, busy |-> FALSE]]
  /\ index = {}
  /\ dirty = {}
  /\ ncons = 0

-----------------------------------------------------------------------------
(* Retain: versions are numbered in submission order; several may be in     *)
(* flight for the same document.                                            *)

StartRetain(d, v, C) ==
  /\ vstate[d][v] = "none"
  /\ \A u \in 1..(v - 1) : vstate[d][u] /= "none"
  /\ C /= {}
  /\ vstate' = [vstate EXCEPT ![d][v] = "ingesting"]
  /\ vcontent' = [vcontent EXCEPT ![d][v] = C]
  /\ vpending' = [vpending EXCEPT ![d][v] = C]
  /\ UNCHANGED <<fstate, deleted, links, obs, index, dirty, ncons>>

\* One CommitChunk transaction: insert (new hash), un-retire (retired but not
\* purged) or keep (already live).  A new fact is linked to every live fact
\* (abstraction of entity/semantic/temporal link building).
CommitChunk(d, v, h) ==
  /\ h \in vpending[d][v]
  /\ CommitChecksVersion => vstate[d][v] = "ingesting"
  /\ LET f == <<d, h>> IN
     /\ fstate' = [fstate EXCEPT ![f] = "live"]
     /\ links' = IF fstate[f] = "absent" THEN links \cup {{f, g} : g \in Live} ELSE links
     /\ deleted' = deleted \ {f}
     /\ dirty' = IF Async /\ fstate[f] /= "live" THEN dirty \cup {f} ELSE dirty
  /\ vpending' = [vpending EXCEPT ![d][v] = @ \ {h}]
  /\ UNCHANGED <<vstate, vcontent, obs, index, ncons>>

\* The version was deleted or superseded under the workflow: its remaining
\* CommitChunk transactions fail the status check and the operation ends.
AbandonVersion(d, v) ==
  /\ CommitChecksVersion
  /\ vstate[d][v] \in {"superseded", "deleted"}
  /\ vpending[d][v] /= {}
  /\ vpending' = [vpending EXCEPT ![d][v] = {}]
  /\ UNCHANGED <<fstate, vstate, vcontent, deleted, links, obs, index, dirty, ncons>>

NewerStarted(d, v) == \E u \in Versions : u > v /\ vstate[d][u] /= "none"

\* FinalizeVersion: mark active, supersede older versions, retire the
\* document's live facts that the new version does not contain.
FinalizeVersion(d, v) ==
  /\ vstate[d][v] = "ingesting"
  /\ vpending[d][v] = {}
  /\ IF FinalizeChecksNewer /\ NewerStarted(d, v)
       THEN /\ vstate' = [vstate EXCEPT ![d][v] = "superseded"]
            /\ UNCHANGED <<fstate, dirty>>
       ELSE LET R == {f \in FactsOf(d) : fstate[f] = "live" /\ f[2] \notin vcontent[d][v]} IN
            /\ vstate' = [vstate EXCEPT ![d] =
                 [u \in Versions |-> IF u = v THEN "active"
                                     ELSE IF u < v /\ vstate[d][u] \in {"active", "ingesting"} THEN "superseded"
                                     ELSE vstate[d][u]]]
            /\ fstate' = [f \in Facts |-> IF f \in R THEN "retired" ELSE fstate[f]]
            /\ dirty' = IF Async THEN dirty \cup R ELSE dirty
  /\ UNCHANGED <<vcontent, vpending, deleted, links, obs, index, ncons>>

-----------------------------------------------------------------------------
(* Delete and purge *)

\* Synchronous cascade, then ack.  Observation sources referencing the
\* document are removed; an observation with no sources left is retired,
\* one that lost some is hidden as stale until reconsolidated.
Delete(d) ==
  /\ \E v \in Versions : vstate[d][v] \in {"ingesting", "active"}
  /\ LET D == FactsOf(d) IN
     /\ vstate' = [vstate EXCEPT ![d] = [v \in Versions |->
                     IF vstate[d][v] \in {"ingesting", "active"} THEN "deleted" ELSE vstate[d][v]]]
     /\ fstate' = [f \in Facts |-> IF f \in D /\ fstate[f] = "live" THEN "retired" ELSE fstate[f]]
     /\ deleted' = deleted \cup {f \in D : fstate[f] /= "absent"}
     /\ links' = {l \in links : l \cap D = {}}
     /\ obs' = [o \in Obs |->
                 IF obs[o].st \in {"live", "stale"}
                   THEN LET s == obs[o].src \ D IN
                        IF s = {} THEN [obs[o] EXCEPT !.st = "retired", !.src = {}]
                        ELSE IF s /= obs[o].src THEN [obs[o] EXCEPT !.st = "stale", !.src = s]
                        ELSE obs[o]
                   ELSE obs[o]]
     /\ dirty' = IF Async THEN dirty \cup {f \in D : fstate[f] = "live"} ELSE dirty
  /\ UNCHANGED <<vcontent, vpending, index, ncons>>

\* Asynchronous physical purge of a retired fact (after the grace period).
\* A fact retired by REPLACE keeps its observation_sources rows while it can
\* still be un-retired; the purge removes them and the source-count trigger
\* retires observations left with none (ND-11).  A fact retired by Delete
\* lost its source rows in the delete cascade, so this is a no-op for it.
Purge(f) ==
  /\ fstate[f] = "retired"
  /\ fstate' = [fstate EXCEPT ![f] = "absent"]
  /\ links' = {l \in links : f \notin l}
  /\ obs' = [o \in Obs |->
              IF obs[o].st \in {"live", "stale"} /\ f \in obs[o].src
                THEN IF obs[o].src \ {f} = {} THEN [obs[o] EXCEPT !.st = "retired", !.src = {}]
                                              ELSE [obs[o] EXCEPT !.src = @ \ {f}]
                ELSE obs[o]]
  /\ UNCHANGED <<vstate, vcontent, vpending, deleted, index, dirty, ncons>>

-----------------------------------------------------------------------------
(* Consolidation: read batch, LLM call (no transaction), apply transaction *)

ConsolidateRead(o) ==
  /\ ~obs[o].busy
  /\ obs[o].st \in {"absent", "live", "stale"}
  /\ ncons < MaxConsolidations
  /\ Live /= {}
  /\ obs' = [obs EXCEPT ![o].snap = Live, ![o].busy = TRUE]
  /\ UNCHANGED <<fstate, vstate, vcontent, vpending, deleted, links, index, dirty, ncons>>

ConsolidateApply(o) ==
  /\ obs[o].busy
  /\ ncons' = ncons + 1
  /\ LET snap == obs[o].snap
         commit(S) == [obs EXCEPT ![o] = [st |-> "live", src |-> S, deriv |-> snap, snap |-> {}, busy |-> FALSE]]
         abort     == [obs EXCEPT ![o].snap = {}, ![o].busy = FALSE]
     IN CASE ApplyCheck = "batch" -> obs' = IF snap \subseteq Live THEN commit(snap) ELSE abort
          [] ApplyCheck = "cited" -> obs' = IF snap \cap Live /= {} THEN commit(snap \cap Live) ELSE abort
          [] ApplyCheck = "none"  -> obs' = commit(snap)
  /\ UNCHANGED <<fstate, vstate, vcontent, vpending, deleted, links, index, dirty>>

-----------------------------------------------------------------------------
(* Async index consumer *)

Relay(f) ==
  /\ Async /\ f \in dirty
  /\ index' = IF f \in Live THEN index \cup {f} ELSE index \ {f}
  /\ dirty' = dirty \ {f}
  /\ UNCHANGED <<fstate, vstate, vcontent, vpending, deleted, links, obs, ncons>>

-----------------------------------------------------------------------------

Next ==
  \/ \E d \in Docs, v \in Versions, C \in SUBSET Hashes : StartRetain(d, v, C)
  \/ \E d \in Docs, v \in Versions, h \in Hashes : CommitChunk(d, v, h)
  \/ \E d \in Docs, v \in Versions : AbandonVersion(d, v) \/ FinalizeVersion(d, v)
  \/ \E d \in Docs : Delete(d)
  \/ \E f \in Facts : Purge(f) \/ Relay(f)
  \/ \E o \in Obs : ConsolidateRead(o) \/ ConsolidateApply(o)
  \/ UNCHANGED vars        \* quiescence (every other action is bounded)

Spec == Init /\ [][Next]_vars /\ WF_vars(\E f \in Facts : Relay(f))

Symm == Permutations(Docs) \cup Permutations(Hashes) \cup Permutations(Obs)

-----------------------------------------------------------------------------
(* Recall, as the store and the index see it *)

Index == IF Async THEN index ELSE Live
RecallFacts == IF FilterIndexByStore THEN Index \cap Live ELSE Index
GraphArm == {g \in Live : \E f \in RecallFacts : f /= g /\ {f, g} \in links}
RecallObs == {o \in Obs : obs[o].st = "live"}

-----------------------------------------------------------------------------
(* Invariants *)

\* Every link endpoint is an existing fact row, and no link touches
\* acknowledged-deleted content.
NoOrphanLinks ==
  /\ \A l \in links : \A f \in l : fstate[f] /= "absent"
  /\ \A l \in links : l \cap deleted = {}

NoObservationCitesDeletedAfterAck ==
  \A o \in Obs : obs[o].src \cap deleted = {}

\* Neither a fact, a graph-expansion neighbour nor an observation whose text
\* was derived from deleted content is ever returned after the ack.
NoDeletedContentRecalled ==
  /\ (RecallFacts \cup GraphArm) \cap deleted = {}
  /\ \A o \in RecallObs : obs[o].deriv \cap deleted = {}

\* A live observation cites at least one source and every source row still
\* exists (live, or retired by REPLACE and inside its grace period -- the
\* observation is then stale_write and still visible, D16).  Sources of
\* acknowledged deletes are excluded by NoObservationCitesDeletedAfterAck.
ObservationHasSources ==
  \A o \in Obs : obs[o].st = "live" =>
    /\ obs[o].src /= {}
    /\ \A f \in obs[o].src : fstate[f] /= "absent"

OneActiveVersion ==
  \A d \in Docs : Cardinality({v \in Versions : vstate[d][v] = "active"}) <= 1

\* An active version's chunks are all live; every live fact belongs to a
\* version that exists and is not deleted; and once no version of a document
\* is ingesting, its live facts are exactly the active version's chunks
\* (chunks of a superseded version are visible only while a newer version is
\* still ingesting -- the same transient as any per-chunk ingest, D16).
VersionsConsistent ==
  /\ \A d \in Docs, v \in Versions :
       vstate[d][v] = "active" => \A h \in vcontent[d][v] : fstate[<<d, h>>] = "live"
  /\ \A f \in Live : \E v \in Versions :
       vstate[f[1]][v] \notin {"none", "deleted"} /\ f[2] \in vcontent[f[1]][v]
  /\ \A d \in Docs :
       (\A v \in Versions : vstate[d][v] /= "ingesting") =>
         \A f \in Live \cap FactsOf(d) : \E v \in Versions :
           vstate[d][v] = "active" /\ f[2] \in vcontent[d][v]

\* Liveness: once the writers quiesce the index equals the store.
IndexConverges == <>[](Index = Live)

=============================================================================
