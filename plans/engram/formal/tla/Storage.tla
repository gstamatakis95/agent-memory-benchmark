----------------------------- MODULE Storage -----------------------------
(***************************************************************************)
(* Engram D23 (N111-N113, N138): insert-only content, vector side tables keyed   *)
(* by embedding model, per-namespace index, ReembedNamespace.              *)
(*                                                                         *)
(*   content   rows of facts/chunks; immutable after insert; the only      *)
(*             later DML is Purge, which needs an expunge marker           *)
(*   vec       fact_vectors rows <<row, gen>>, insert-only; gen is the     *)
(*             embedding model generation                                  *)
(*   cur       the namespace's current embedding model generation          *)
(*   idx       entries of the namespace's partial HNSW (async sync)        *)
(*                                                                         *)
(* ReembedNamespace: insert vectors of generation cur+1 for every live     *)
(* row, build the index, Flip (cur := cur+1), expunge the old vectors.     *)
(* An arm reads exactly the vectors of generation cur from the index.      *)
(*                                                                         *)
(* Index hygiene (N138): a purge leaves dead entries in the HNSW graph; the *)
(* purge counts them (pc = purged_since_build) and a rebuild at the        *)
(* threshold is the only thing that removes them -- vector partitions have *)
(* vacuum_index_cleanup = off, so no autovacuum repair (5-6x a rebuild).   *)
(*                                                                         *)
(* Knobs (design: all FALSE):                                              *)
(*   AllowUpdate  a content row may be rewritten in place                  *)
(*   FlipEarly    Flip does not wait for every live row to have a vector   *)
(*                of the next generation                                   *)
(*   PurgeUnmarked Purge needs no expunge marker                            *)
(*   AutoRepair    autovacuum repairs the graph in place (drops dead entries)*)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Rows, MaxGen, AllowUpdate, FlipEarly, PurgeUnmarked, AutoRepair, Thr

VARIABLES content, payload, marked, vec, cur, idx, ingested, snap, pc, repairs
vars == <<content, payload, marked, vec, cur, idx, ingested, snap, pc, repairs>>

Gens == 1..MaxGen

TypeOK ==
  /\ content \subseteq Rows /\ marked \subseteq Rows /\ ingested \subseteq Rows
  /\ vec \subseteq Rows \X Gens /\ idx \subseteq Rows \X Gens /\ cur \in Gens
  /\ payload \in [Rows -> 0..1] /\ snap \subseteq Rows /\ pc \in 0..(Cardinality(Rows) * MaxGen) /\ repairs \in 0..(Cardinality(Rows) * MaxGen)

Init ==
  /\ content = {} /\ payload = [r \in Rows |-> 0] /\ marked = {} /\ vec = {} /\ cur = 1
  /\ idx = {} /\ ingested = {} /\ snap = {} /\ pc = 0 /\ repairs = 0

\* A row is inserted once; its vectors for the current generation (and the next, if a re-embed
\* has begun) are inserted by the pipeline.
Insert(r) ==
  /\ r \notin ingested
  /\ content' = content \cup {r} /\ ingested' = ingested \cup {r}
  /\ UNCHANGED <<payload, marked, vec, cur, idx, snap, pc, repairs>>

EmbedRow(r, g) ==
  /\ r \in content /\ g \in {cur, cur + 1} /\ g <= MaxGen /\ <<r, g>> \notin vec
  /\ vec' = vec \cup {<<r, g>>}
  /\ UNCHANGED <<content, payload, marked, cur, idx, ingested, snap, pc, repairs>>

Update(r) ==
  /\ AllowUpdate /\ r \in content /\ payload[r] = 0
  /\ payload' = [payload EXCEPT ![r] = 1]
  /\ UNCHANGED <<content, marked, vec, cur, idx, ingested, snap, pc, repairs>>

Mark(r) ==                      \* the delete's marker (document tombstone); reads hide the row
  /\ r \in content /\ r \notin marked
  /\ marked' = marked \cup {r}
  /\ UNCHANGED <<content, payload, vec, cur, idx, ingested, snap, pc, repairs>>

Purge(r) ==                     \* batched DELETE by the expunge: content, vectors, then index entries
  /\ r \in content /\ (PurgeUnmarked \/ r \in marked)
  /\ content' = content \ {r}
  /\ vec' = {x \in vec : x[1] # r}
  /\ pc' = pc + Cardinality({x \in idx : x[1] = r /\ x \in vec})
  /\ UNCHANGED <<payload, marked, cur, idx, ingested, snap, repairs>>

Live == content \ marked

\* The index follows the vector table asynchronously: add what is missing, drop what is gone.
IndexAdd(x) == /\ x \in vec /\ x \notin idx /\ idx' = idx \cup {x}
               /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, pc, repairs>>
\* autovacuum index cleanup (off in the design): drops a dead entry in place
AutoRepairDrop(x) == /\ AutoRepair /\ x \in idx /\ x \notin vec /\ idx' = idx \ {x} /\ repairs' = repairs + 1
                     /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, pc>>
\* The hygiene rebuild: once the purge counter reaches the threshold the graph is rebuilt from the live vectors.
Rebuild == /\ pc >= Thr /\ idx' = vec /\ pc' = 0
           /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, repairs>>

\* Flip the namespace's current model: every live row has a next-generation vector (and the index has it).
Flip ==
  /\ cur < MaxGen
  /\ FlipEarly \/ \A r \in Live : <<r, cur + 1>> \in vec /\ <<r, cur + 1>> \in idx
  /\ cur' = cur + 1 /\ snap' = Live
  /\ UNCHANGED <<content, payload, marked, vec, idx, ingested, pc, repairs>>

ExpungeOld(x) ==
  /\ x \in vec /\ x[2] < cur
  /\ vec' = vec \ {x} /\ pc' = IF x \in idx THEN pc + 1 ELSE pc
  /\ UNCHANGED <<content, payload, marked, cur, idx, ingested, snap, repairs>>

Next == (\E r \in Rows : Insert(r) \/ Update(r) \/ Mark(r) \/ Purge(r) \/ (\E g \in Gens : EmbedRow(r, g)))
        \/ (\E x \in Rows \X Gens : IndexAdd(x) \/ AutoRepairDrop(x) \/ ExpungeOld(x)) \/ Flip \/ Rebuild

Spec == Init /\ [][Next]_vars
        /\ \A x \in Rows \X Gens : WF_vars(IndexAdd(x)) /\ WF_vars(ExpungeOld(x))
        /\ \A r \in Rows : WF_vars(Purge(r))
        /\ WF_vars(Rebuild)

-----------------------------------------------------------------------------
\* No action changes a content row after insert; Purge needs a marker.
ContentImmutable == \A r \in content : payload[r] = 0
PurgeNeedsMarker == \A r \in ingested \ content : r \in marked

\* An arm reads exactly the generation-cur vectors; every row that was live at the last Flip and is
\* still live has one (an arm never misses a row because its new vector was not there yet).
VectorGenerationConsistent == \A r \in snap \cap Live : <<r, cur>> \in vec

\* After Purge, the index eventually contains exactly the live vectors of the current generation.
IndexConvergence ==
  <>[]( {x \in idx : x[2] = cur} = {x \in vec : x[2] = cur} )

\* Only a rebuild removes dead entries: no in-place repair, ever (N138).
RebuildBeforeRepair == repairs = 0

\* The purge counter accounts for every dead entry, so the hygiene trigger cannot miss one.
DeadCounted == Cardinality(idx \ vec) <= pc

=============================================================================
