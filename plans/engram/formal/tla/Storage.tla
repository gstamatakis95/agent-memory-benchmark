----------------------------- MODULE Storage -----------------------------
(***************************************************************************)
(* Engram D22 (N111-N113): insert-only content, vector side tables keyed   *)
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
(* Knobs (design: all FALSE):                                              *)
(*   AllowUpdate  a content row may be rewritten in place                  *)
(*   FlipEarly    Flip does not wait for every live row to have a vector   *)
(*                of the next generation                                   *)
(*   PurgeUnmarked Purge needs no expunge marker                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Rows, MaxGen, AllowUpdate, FlipEarly, PurgeUnmarked

VARIABLES content, payload, marked, vec, cur, idx, ingested, snap
vars == <<content, payload, marked, vec, cur, idx, ingested, snap>>

Gens == 1..MaxGen

TypeOK ==
  /\ content \subseteq Rows /\ marked \subseteq Rows /\ ingested \subseteq Rows
  /\ vec \subseteq Rows \X Gens /\ idx \subseteq Rows \X Gens /\ cur \in Gens
  /\ payload \in [Rows -> 0..1] /\ snap \subseteq Rows

Init ==
  /\ content = {} /\ payload = [r \in Rows |-> 0] /\ marked = {} /\ vec = {} /\ cur = 1
  /\ idx = {} /\ ingested = {} /\ snap = {}

\* A row is inserted once; its vectors for the current generation (and the next, if a re-embed
\* has begun) are inserted by the pipeline.
Insert(r) ==
  /\ r \notin ingested
  /\ content' = content \cup {r} /\ ingested' = ingested \cup {r}
  /\ UNCHANGED <<payload, marked, vec, cur, idx, snap>>

EmbedRow(r, g) ==
  /\ r \in content /\ g \in {cur, cur + 1} /\ g <= MaxGen /\ <<r, g>> \notin vec
  /\ vec' = vec \cup {<<r, g>>}
  /\ UNCHANGED <<content, payload, marked, cur, idx, ingested, snap>>

Update(r) ==
  /\ AllowUpdate /\ r \in content /\ payload[r] = 0
  /\ payload' = [payload EXCEPT ![r] = 1]
  /\ UNCHANGED <<content, marked, vec, cur, idx, ingested, snap>>

Mark(r) ==                      \* the delete's marker (document tombstone); reads hide the row
  /\ r \in content /\ r \notin marked
  /\ marked' = marked \cup {r}
  /\ UNCHANGED <<content, payload, vec, cur, idx, ingested, snap>>

Purge(r) ==                     \* batched DELETE by the expunge: content, vectors, then index entries
  /\ r \in content /\ (PurgeUnmarked \/ r \in marked)
  /\ content' = content \ {r}
  /\ vec' = {x \in vec : x[1] # r}
  /\ UNCHANGED <<payload, marked, cur, idx, ingested, snap>>

Live == content \ marked

\* The index follows the vector table asynchronously: add what is missing, drop what is gone.
IndexAdd(x) == /\ x \in vec /\ x \notin idx /\ idx' = idx \cup {x}
               /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap>>
IndexDrop(x) == /\ x \in idx /\ x \notin vec /\ idx' = idx \ {x}
                /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap>>

\* Flip the namespace's current model: every live row has a next-generation vector (and the index has it).
Flip ==
  /\ cur < MaxGen
  /\ FlipEarly \/ \A r \in Live : <<r, cur + 1>> \in vec /\ <<r, cur + 1>> \in idx
  /\ cur' = cur + 1 /\ snap' = Live
  /\ UNCHANGED <<content, payload, marked, vec, idx, ingested>>

ExpungeOld(x) ==
  /\ x \in vec /\ x[2] < cur
  /\ vec' = vec \ {x}
  /\ UNCHANGED <<content, payload, marked, cur, idx, ingested, snap>>

Next == (\E r \in Rows : Insert(r) \/ Update(r) \/ Mark(r) \/ Purge(r) \/ (\E g \in Gens : EmbedRow(r, g)))
        \/ (\E x \in Rows \X Gens : IndexAdd(x) \/ IndexDrop(x) \/ ExpungeOld(x)) \/ Flip

Spec == Init /\ [][Next]_vars
        /\ \A x \in Rows \X Gens : WF_vars(IndexAdd(x)) /\ WF_vars(IndexDrop(x)) /\ WF_vars(ExpungeOld(x))
        /\ \A r \in Rows : WF_vars(Purge(r))

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

=============================================================================
