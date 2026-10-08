----------------------------- MODULE Storage -----------------------------
(***************************************************************************)
(* Engram D25 (N111-N113, N138, N152, N166(6)): insert-only content, vector*)
(* side tables keyed by embedding model, per-namespace index,              *)
(* ReembedNamespace.                                                       *)
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
(*                                                                         *)
(* D24 (N152): the hygiene unit is the PARTITION.  A partition holds the    *)
(* partial HNSW of several namespaces (Ns), each with its own purge counter *)
(* pc[n] and threshold Thr[n]; a vacuum of the partition reads EVERY graph  *)
(* on it, so it may run only when every touched graph has been rebuilt      *)
(* (Vacuum(p) requires \A i \in idx[p] : dead[i] = 0 \/ rebuilt[i]).  When  *)
(* one graph is due, the hygiene rebuilds all touched graphs of the         *)
(* partition, then vacuums once.  Knob PartitionUnit=FALSE (must-fail):     *)
(* rebuild only the due graph and vacuum afterwards; the vacuum then        *)
(* repairs the other graphs' dead entries in place (the 5-6x repair).       *)
(*                                                                         *)
(* D25 (N166(6), P-8): a rebuild and a vacuum are not atomic.  Rebuild is   *)
(* snapshot-then-publish: RebuildStart(n) takes the live vectors of n,      *)
(* RebuildPublish(n) replaces the graph by exactly that snapshot and        *)
(* resets pc[n]; a row purged in between is then a dead entry of the        *)
(* published graph that nothing counts.  Vacuum is start-then-collect:      *)
(* VacuumStart records the dead entries of the graphs at that moment,       *)
(* VacuumCollect removes exactly those (a repair, if there were any).  The  *)
(* design takes the partition key (lk) exclusively from the selection of    *)
(* the rebuild set until VacuumStart; purge batches (Purge, ExpungeOld)     *)
(* skip the partition while it is held.  PurgeLock = FALSE (must-fail):     *)
(* purges ignore the key.                                                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Rows, MaxGen, AllowUpdate, FlipEarly, PurgeUnmarked, AutoRepair, PartitionUnit, PurgeLock,
          Ns, NsOf, Thr

VARIABLES content, payload, marked, vec, cur, idx, ingested, snap, pc, repairs, rebuilt,
          lk, rbusy, rsnap, vst, dsn
HygV == <<lk, rbusy, rsnap, vst, dsn>>
vars == <<content, payload, marked, vec, cur, idx, ingested, snap, pc, repairs, rebuilt, HygV>>

\* Design instance (cfg: NsOf <- NsOfDef, Thr <- ThrDef): rows 1 and 2 in namespace a (threshold 1: due after one purge),
\* row 3 in namespace b (threshold 2: one purge leaves it touched but not due).
NsOfDef == <<"a", "a", "b">>
ThrDef == [n \in {"a", "b"} |-> IF n = "a" THEN 1 ELSE 2]

Gens == 1..MaxGen
NsOfX(x) == NsOf[x[1]]                      \* namespace of a vector entry <<row, gen>>
EntriesOf(S, n) == {x \in S : NsOfX(x) = n}

TypeOK ==
  /\ content \subseteq Rows /\ marked \subseteq Rows /\ ingested \subseteq Rows
  /\ vec \subseteq Rows \X Gens /\ idx \subseteq Rows \X Gens /\ cur \in Gens
  /\ payload \in [Rows -> 0..1] /\ snap \subseteq Rows /\ pc \in [Ns -> 0..(Cardinality(Rows) * MaxGen)] /\ rebuilt \subseteq Ns /\ repairs \in 0..(Cardinality(Rows) * MaxGen)
  /\ lk \in BOOLEAN /\ rbusy \subseteq Ns /\ vst \in BOOLEAN /\ dsn \subseteq Rows \X Gens

Init ==
  /\ content = {} /\ payload = [r \in Rows |-> 0] /\ marked = {} /\ vec = {} /\ cur = 1
  /\ idx = {} /\ ingested = {} /\ snap = {} /\ pc = [n \in Ns |-> 0] /\ repairs = 0 /\ rebuilt = {}
  /\ lk = FALSE /\ rbusy = {} /\ rsnap = [n \in Ns |-> {}] /\ vst = FALSE /\ dsn = {}

\* A row is inserted once; its vectors for the current generation (and the next, if a re-embed
\* has begun) are inserted by the pipeline.
Insert(r) ==
  /\ r \notin ingested
  /\ content' = content \cup {r} /\ ingested' = ingested \cup {r}
  /\ UNCHANGED <<payload, marked, vec, cur, idx, snap, pc, repairs, rebuilt, HygV>>

EmbedRow(r, g) ==
  /\ r \in content /\ g \in {cur, cur + 1} /\ g <= MaxGen /\ <<r, g>> \notin vec
  /\ vec' = vec \cup {<<r, g>>}
  /\ UNCHANGED <<content, payload, marked, cur, idx, ingested, snap, pc, repairs, rebuilt, HygV>>

Update(r) ==
  /\ AllowUpdate /\ r \in content /\ payload[r] = 0
  /\ payload' = [payload EXCEPT ![r] = 1]
  /\ UNCHANGED <<content, marked, vec, cur, idx, ingested, snap, pc, repairs, rebuilt, HygV>>

Mark(r) ==                      \* the delete's marker (document tombstone); reads hide the row
  /\ r \in content /\ r \notin marked
  /\ marked' = marked \cup {r}
  /\ UNCHANGED <<content, payload, vec, cur, idx, ingested, snap, pc, repairs, rebuilt, HygV>>

Purge(r) ==                     \* batched DELETE by the expunge: content, vectors, then index entries
  /\ r \in content /\ (PurgeUnmarked \/ r \in marked)
  /\ ~(PurgeLock /\ lk)                      \* the purge batch skips a partition whose key the runner holds
  /\ content' = content \ {r}
  /\ vec' = {x \in vec : x[1] # r}
  /\ LET k == Cardinality({x \in idx : x[1] = r /\ x \in vec}) IN
     /\ pc' = [pc EXCEPT ![NsOf[r]] = @ + k]
     /\ rebuilt' = IF k > 0 THEN rebuilt \ {NsOf[r]} ELSE rebuilt
  /\ UNCHANGED <<payload, marked, cur, idx, ingested, snap, repairs, HygV>>

Live == content \ marked

\* The index follows the vector table asynchronously: add what is missing, drop what is gone.
IndexAdd(x) == /\ x \in vec /\ x \notin idx /\ idx' = idx \cup {x}
               /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, pc, repairs, rebuilt, HygV>>
\* autovacuum index cleanup (off in the design): drops a dead entry in place
AutoRepairDrop(x) == /\ AutoRepair /\ x \in idx /\ x \notin vec /\ idx' = idx \ {x} /\ repairs' = repairs + 1
                     /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, pc, rebuilt, HygV>>

Due(n) == pc[n] >= Thr[n]
Touched(n) == pc[n] > 0

\* The hygiene rebuild of one namespace's graph from the live vectors, snapshot-then-publish.  In the design
\* (PartitionUnit) the hygiene of a partition with a due graph rebuilds every touched graph; per-index hygiene
\* rebuilds only the due one.  The first RebuildStart takes the partition key (selection of the rebuild set).
RebuildStart(n) ==
  /\ n \notin rbusy /\ n \notin rebuilt /\ ~vst
  /\ Due(n) \/ (PartitionUnit /\ Touched(n) /\ ((\E m \in Ns : Due(m)) \/ rebuilt # {} \/ lk))
  /\ lk' = TRUE /\ rbusy' = rbusy \cup {n} /\ rsnap' = [rsnap EXCEPT ![n] = EntriesOf(vec, n)]
  /\ UNCHANGED <<content, payload, marked, vec, cur, idx, ingested, snap, pc, repairs, rebuilt, vst, dsn>>

RebuildPublish(n) ==
  /\ n \in rbusy
  /\ idx' = (idx \ EntriesOf(idx, n)) \cup rsnap[n]
  /\ pc' = [pc EXCEPT ![n] = 0] /\ rebuilt' = rebuilt \cup {n}
  /\ rbusy' = rbusy \ {n} /\ rsnap' = [rsnap EXCEPT ![n] = {}]
  /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, repairs, lk, vst, dsn>>

\* Vacuum of the partition (heap and indexes), start-then-collect: it reads every HNSW on the partition, and a graph
\* that still holds dead entries is repaired in place.  Design: starts only when every touched graph has been
\* rebuilt (dead[i] = 0 \/ rebuilt[i]) and releases the partition key; per-index hygiene: after the due graph was
\* rebuilt, when no graph is due.
VacuumReady == IF PartitionUnit THEN \A n \in Ns : EntriesOf(idx, n) \ vec = {} \/ n \in rebuilt
                                 ELSE rebuilt # {} /\ \A n \in Ns : ~Due(n)
VacuumStart ==
  /\ ~vst /\ rbusy = {}
  /\ (idx \ vec) # {} \/ \E n \in Ns : n \in rebuilt
  /\ VacuumReady
  /\ vst' = TRUE /\ dsn' = idx \ vec /\ lk' = FALSE
  /\ UNCHANGED <<content, payload, marked, vec, cur, idx, ingested, snap, pc, repairs, rebuilt, rbusy, rsnap>>
VacuumCollect ==
  /\ vst
  /\ idx' = idx \ dsn /\ repairs' = repairs + Cardinality(dsn) /\ rebuilt' = {}
  /\ vst' = FALSE /\ dsn' = {}
  /\ UNCHANGED <<content, payload, marked, vec, cur, ingested, snap, pc, lk, rbusy, rsnap>>

\* Flip the namespace's current model: every live row has a next-generation vector (and the index has it).
Flip ==
  /\ cur < MaxGen
  /\ FlipEarly \/ \A r \in Live : <<r, cur + 1>> \in vec /\ <<r, cur + 1>> \in idx
  /\ cur' = cur + 1 /\ snap' = Live
  /\ UNCHANGED <<content, payload, marked, vec, idx, ingested, pc, repairs, rebuilt, HygV>>

ExpungeOld(x) ==
  /\ x \in vec /\ x[2] < cur
  /\ ~(PurgeLock /\ lk)
  /\ vec' = vec \ {x}
  /\ pc' = IF x \in idx THEN [pc EXCEPT ![NsOfX(x)] = @ + 1] ELSE pc
  /\ rebuilt' = IF x \in idx THEN rebuilt \ {NsOfX(x)} ELSE rebuilt
  /\ UNCHANGED <<content, payload, marked, cur, idx, ingested, snap, repairs, HygV>>

Next == (\E r \in Rows : Insert(r) \/ Update(r) \/ Mark(r) \/ Purge(r) \/ (\E g \in Gens : EmbedRow(r, g)))
        \/ (\E x \in Rows \X Gens : IndexAdd(x) \/ AutoRepairDrop(x) \/ ExpungeOld(x)) \/ Flip
        \/ (\E n \in Ns : RebuildStart(n) \/ RebuildPublish(n)) \/ VacuumStart \/ VacuumCollect

Spec == Init /\ [][Next]_vars
        /\ \A x \in Rows \X Gens : WF_vars(IndexAdd(x)) /\ WF_vars(ExpungeOld(x))
        /\ \A r \in Rows : WF_vars(Purge(r))
        /\ \A n \in Ns : WF_vars(RebuildStart(n)) /\ WF_vars(RebuildPublish(n))
        /\ WF_vars(VacuumStart) /\ WF_vars(VacuumCollect)

-----------------------------------------------------------------------------
\* No action changes a content row after insert; Purge needs a marker.
ContentImmutable == \A r \in content : payload[r] = 0
PurgeNeedsMarker == \A r \in ingested \ content : r \in marked

\* An arm reads exactly the generation-cur vectors; every row that was live at the last Flip and is
\* still live has one (an arm never misses a row because its new vector was not there yet).
VectorGenerationConsistent == \A r \in snap \cap Live : <<r, cur>> \in vec

\* After Purge, every due graph is eventually rebuilt and the index holds the current generation's live vectors
\* (a graph below its threshold may keep a few dead entries; DeadCounted bounds them).
IndexConvergence ==
  <>[]( (\A n \in Ns : ~Due(n)) /\ {x \in vec : x[2] = cur} \subseteq idx )

\* Only a rebuild removes dead entries: no in-place repair, ever (N138).
RebuildBeforeRepair == repairs = 0

\* The purge counter accounts for every dead entry, so the hygiene trigger cannot miss one.
DeadCounted == \A n \in Ns : Cardinality(EntriesOf(idx, n) \ vec) <= pc[n]

=============================================================================
