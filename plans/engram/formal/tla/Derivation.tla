--------------------------- MODULE Derivation ---------------------------
(***************************************************************************)
(* Engram D23 (N113, N115-N121, N133, N135, N136): visibility of derived   *)
(* data.  Mutable state is a handful of markers; everything else is a      *)
(* read-time predicate over insert-only evidence.                          *)
(*   tomb[d]   document_tombstones: versions 1..tomb[d] of d are deleted;  *)
(*             a re-used document id starts at tomb[d] + 1                 *)
(*   hidden    fact_hidden(cause = invalidate); Restore deletes it         *)
(*   hidRe     fact_hidden(cause = reextract): a stale-key fact; the fact  *)
(*             and chunk arms skip it, derived versions do NOT (N135)      *)
(*   ctomb     chunk_tombstones (REPLACE retires the fact; not a deletion) *)
(*   vers      observation_versions / page_versions: root_version, the     *)
(*             evidence rows (fact ids; observation versions for pages;    *)
(*             no foreign key to facts, N135), st live | stub | absent     *)
(*   dh        derived_hidden rows <<node, root_version, from_version,     *)
(*             cause>>, cause <<"doc", 0>> or <<"inv", fact id>>             *)
(*   props     persisted stage-2 proposals (write-once) with base_version  *)
(*   lockX     the derivation lock, exclusive: Materialize batch, Restore  *)
(*             (writers hold it shared from Verify to Commit, N120)        *)
(* Segment of (n, v) = versions root_version(v)..v; Deriv(n, v) = union of *)
(* the segment's evidence (two levels: fact -> observation -> page).       *)
(*                                                                         *)
(* Facts: f1 = (d1, v1) with a re-extraction twin f4, f2 = (d1, v2: the id *)
(* re-used right after the delete), f3 = (d2, v1).                         *)
(*                                                                         *)
(* Writers (two): a proposal is stored (stage 2 rendered against           *)
(* base_version = the version it edits); a writer applies it with Verify   *)
(* (shared lock; every rendered input and the base re-checked; on failure  *)
(* the proposal is discarded) and Commit.  Pages use the same path.        *)
(*                                                                         *)
(* Expunge of a deleted document: Materialize (batches; each batch scans   *)
(* and writes under the exclusive lock and re-reads fact_hidden), Purge    *)
(* (the victims' facts), DerivedPurge (versions covered by a document-cause*)
(* row become content-free stubs).  REPLACE and re-extraction retire facts *)
(* (ChunkPurge, ReextractPurge delete them later); evidence stays.         *)
(*                                                                         *)
(* Ghost state: gfinp/goinp keep what a version's text was really derived  *)
(* from (the inputs plus the base version's); the implementation has none. *)
(*                                                                         *)
(* Knobs (design: UseLock, RestoreLock, TombByVersion, EffAllShown,        *)
(* PageVerify, BaseCheck TRUE; the others FALSE):                          *)
(*   UseLock=FALSE        writers and Materialize ignore each other        *)
(*   RestoreLock=FALSE    Restore does not take the derivation lock         *)
(*   TombByVersion=FALSE  a tombstone hides the whole document id           *)
(*   EffAllShown=FALSE    effective_at counts only cited facts              *)
(*   CascadeEvidence      evidence rows die with their fact (old FK)        *)
(*   ReextractHides       derived versions are hidden by cause reextract    *)
(*   PageVerify=FALSE     CommitPageVersion re-verifies nothing             *)
(*   BaseCheck=FALSE      an update is applied without checking its base    *)
(*   MatOnce              Materialize reads fact_hidden once, not per batch *)
(*   DropStub             DerivedPurge deletes the version row              *)
(*   AllowAbortW=FALSE    writers never crash (liveness runs)               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Docs, Facts, DocOf, FVer, Mentioned, Twin,
          Obs, Pages, MaxVerO, MaxVerP, Writers, Budget, MaxF,
          MaxT, MaxDeletes, MaxCuration, MaxRetire,
          UseLock, RestoreLock, TombByVersion, EffAllShown, CascadeEvidence, ReextractHides,
          PageVerify, BaseCheck, MatOnce, DropStub, AllowAbortW,
          CascadeHidden, MatSignalOnly, CasFirst, RootExpected, AllowRetry

\* Design instance (cfg: DocOf <- DocOfDef, FVer <- FVerDef, Mentioned <- MentionedDef, Twin <- TwinDef).
DocOfDef == <<"d1", "d1", "d2", "d1">>
FVerDef == <<1, 2, 1, 1>>
MentionedDef == <<1, 3, 2, 1>>
TwinDef == <<4, 0, 0, 0>>

Nodes == Obs \cup Pages
IsPage(n) == n \in Pages
MaxVer(n) == IF n \in Pages THEN MaxVerP ELSE MaxVerO
Pairs == {<<o, v>> : o \in Obs, v \in 1..MaxVerO}
Twins == {f \in Facts : \E g \in Facts : Twin[g] = f}

VARIABLES tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX,
          matPhase, matDocs, matV0, matTodo, matSnap, wr, props, budget, nDel, nCur, nRet,
          cv, ginv, mat, matOwe, sig
NewV == <<cv, ginv, mat, matOwe, sig>>
vars == <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX,
          matPhase, matDocs, matV0, matTodo, matSnap, wr, props, budget, nDel, nCur, nRet, NewV>>

Max2(a, b) == IF a >= b THEN a ELSE b
MaxSet(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : x <= m
SmallSubsets(S, k) == {s \in SUBSET S : Cardinality(s) <= k}
FactsOf(d) == {f \in Facts : DocOf[f] = d}
Victims(d) == {f \in FactsOf(d) : FVer[f] <= tomb[d]}     \* facts covered by d's tombstone
CurVer(d) == MaxSet({FVer[f] : f \in born \cap FactsOf(d)})

-----------------------------------------------------------------------------
(* Derivation and visibility (all pure functions of the state)             *)

Exists(x) == x[2] <= Len(vers[x[1]])
Versions == UNION {{<<n, v>> : v \in 1..Len(vers[n])} : n \in Nodes}
Seg(n, v) == vers[n][v].root..v
DerivF(n, v)  == UNION {vers[n][w].finp  : w \in Seg(n, v)}      \* real evidence rows
DerivO(n, v)  == UNION {vers[n][w].oinp  : w \in Seg(n, v)}      \* observation versions a page cites
GDerivF(n, v) == UNION {vers[n][w].gfinp : w \in Seg(n, v)}      \* ghost: never removed
GDerivO(n, v) == UNION {vers[n][w].goinp : w \in Seg(n, v)}
GDeriv(n, v)  == GDerivF(n, v) \cup UNION {GDerivF(x[1], x[2]) : x \in {y \in GDerivO(n, v) : Exists(y)}}

TombTrue(f) == FVer[f] <= tomb[DocOf[f]]                           \* the truth: deleted
TombImpl(f) == IF TombByVersion THEN TombTrue(f) ELSE tomb[DocOf[f]] > 0   \* what the predicate checks
Victim(f)  == TombTrue(f) \/ f \in hidden                          \* ghost: deleted or invalidated
VictimD(f) == (TombImpl(f) /\ ms[DocOf[f]] = "pending") \/ f \in hidden \/ (ReextractHides /\ f \in hidRe)   \* derived-version predicate: pending tombstones only (N117)
VictimF(f) == TombImpl(f) \/ f \in hidden \/ f \in hidRe \/ f \in ctomb        \* fact and chunk arms

RowCovered(n, v) == \E r \in dh : r[1] = n /\ r[2] = vers[n][v].root /\ r[3] <= v
Perm(n, v) == RowCovered(n, v) \/ \E x \in DerivO(n, v) : Exists(x) /\ RowCovered(x[1], x[2])

\* N117: a version row exists and is live (fail closed: a stub or missing row means hidden), no victim in the
\* segment's evidence, no derived_hidden row covers it.
VisBase(n, v) == /\ v <= Len(vers[n]) /\ vers[n][v].st = "live"
                 /\ ~(\E f \in DerivF(n, v) : VictimD(f)) /\ ~RowCovered(n, v)
\* A page version is also hidden when a cited observation version is hidden (or missing).
VisN(n, v) == VisBase(n, v) /\ \A x \in DerivO(n, v) : Exists(x) /\ VisBase(x[1], x[2])

VisFacts == {f \in born \ gone : ~VictimF(f)}
VisPairs == {x \in Versions : x[1] \in Obs /\ VisN(x[1], x[2])}

\* Recall at time T (C-14, the SQL rule): per node, the version current at T (the latest with
\* effective_at <= T); served only if visible, else nothing for that node.
Served(T) == {x \in Versions :
                /\ vers[x[1]][x[2]].eff <= T
                /\ \A u \in (x[2] + 1)..Len(vers[x[1]]) : vers[x[1]][u].eff > T
                /\ VisN(x[1], x[2])}
ServedFacts(T) == {f \in VisFacts : Mentioned[f] <= T}

-----------------------------------------------------------------------------
Prop == [n : Nodes, mode : {"root", "update"}, cf : SUBSET Facts, co : SUBSET Pairs, base : 0..3, exp : 0..3]
\* ck: the commit_key of the attempt that wrote the row (N144): the stored proposal it rendered.
Rec == [root : 1..4, finp : SUBSET Facts, gfinp : SUBSET Facts, oinp : SUBSET Pairs, goinp : SUBSET Pairs,
        eff : 0..MaxT, base : 0..3, st : {"live", "stub", "absent"}, ck : Prop]
IdleW == [ph |-> "idle", p |-> [n |-> CHOOSE n \in Nodes : TRUE, mode |-> "root", cf |-> {}, co |-> {}, base |-> 0, exp |-> 0]]

TypeOK ==
  /\ tomb \in [Docs -> 0..2] /\ hidden \subseteq Facts /\ hidRe \subseteq Facts /\ ctomb \subseteq Facts
  /\ born \subseteq Facts /\ gone \subseteq born
  /\ ms \in [Docs -> {"none", "pending", "materialized", "purged", "done"}]
  /\ \A n \in Nodes : \A i \in 1..Len(vers[n]) : vers[n][i] \in Rec
  /\ \A n \in Nodes : Len(vers[n]) <= MaxVer(n)
  /\ cv \in [Nodes -> 0..(MaxVerP + 2)] /\ ginv \subseteq Facts /\ mat \subseteq Facts /\ matOwe \subseteq Facts /\ sig \in BOOLEAN
  /\ lockX \in BOOLEAN /\ matPhase \in {"idle", "running", "scanned"}
  /\ props \subseteq Prop
  /\ budget \in 0..Budget /\ nDel \in 0..MaxDeletes /\ nCur \in 0..MaxCuration /\ nRet \in 0..MaxRetire

Init ==
  /\ tomb = [d \in Docs |-> 0] /\ ms = [d \in Docs |-> "none"] /\ hidden = {} /\ hidRe = {} /\ ctomb = {}
  /\ born = {} /\ gone = {} /\ vers = [n \in Nodes |-> << >>] /\ hw = [n \in Nodes |-> 0] /\ dh = {}
  /\ lockX = FALSE /\ matPhase = "idle" /\ matDocs = {} /\ matV0 = {} /\ matTodo = {} /\ matSnap = {}
  /\ wr = [w \in Writers |-> IdleW] /\ props = {}
  /\ budget = Budget /\ nDel = 0 /\ nCur = 0 /\ nRet = 0
  /\ cv = [n \in Nodes |-> 0] /\ ginv = {} /\ mat = {} /\ matOwe = {} /\ sig = FALSE

-----------------------------------------------------------------------------
(* Ingest, and the markers: the only synchronous writes of a delete, an    *)
(* invalidation, a REPLACE or a re-extraction (N115, N135)                 *)

Ingest(f) ==
  /\ f \notin born /\ f \notin Twins /\ FVer[f] = tomb[DocOf[f]] + 1
  /\ born' = born \cup {f}
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nCur, nRet, NewV>>

DeleteDocument(d) ==
  /\ nDel < MaxDeletes /\ CurVer(d) > tomb[d] /\ ms[d] \in {"none", "done"}
  /\ tomb' = [tomb EXCEPT ![d] = CurVer(d)] /\ ms' = [ms EXCEPT ![d] = "pending"] /\ nDel' = nDel + 1
  /\ UNCHANGED <<hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nCur, nRet, NewV>>

Invalidate(f) ==
  /\ f \in born \ gone /\ f \notin hidden /\ nCur < MaxCuration
  /\ hidden' = hidden \cup {f} /\ nCur' = nCur + 1
  /\ ginv' = ginv \cup {f} /\ sig' = MatSignalOnly            \* the lossy signal after the ack (MatSignalOnly only)
  /\ UNCHANGED <<tomb, ms, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nRet, cv, mat, matOwe>>

\* N133: Restore takes the derivation lock exclusively (waits for shared holders, excludes a Materialize
\* batch) and deletes the cause-tagged derived_hidden rows.
Restore(f) ==
  /\ f \in hidden /\ nCur < MaxCuration
  /\ (RestoreLock => ~lockX /\ \A w \in Writers : wr[w].ph # "verified")
  /\ hidden' = hidden \ {f} /\ dh' = {r \in dh : r[4] # <<"inv", f>>} /\ nCur' = nCur + 1
  /\ ginv' = ginv \ {f} /\ mat' = mat \ {f} /\ matOwe' = matOwe \ {f}      \* the stamp dies with the marker row
  /\ UNCHANGED <<tomb, ms, hidRe, ctomb, born, gone, vers, hw, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nRet, cv, sig>>

\* REPLACE retires a fact's chunk (chunk_tombstones); observations and pages stay visible.
Replace(f) ==
  /\ f \in born \ gone /\ f \notin ctomb /\ ~TombTrue(f) /\ nRet < MaxRetire
  /\ ctomb' = ctomb \cup {f} /\ nRet' = nRet + 1
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nCur, NewV>>

\* Re-extraction is a write: the new-key twin is ingested, the old-key fact gets fact_hidden(reextract).
Reextract(f) ==
  /\ f \in born \ gone /\ f \notin hidRe /\ Twin[f] # 0 /\ Twin[f] \notin born /\ ~TombTrue(f) /\ nRet < MaxRetire
  /\ hidRe' = hidRe \cup {f} /\ born' = born \cup {Twin[f]} /\ nRet' = nRet + 1
  /\ UNCHANGED <<tomb, ms, hidden, ctomb, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nCur, NewV>>

\* The FK of the old schema: evidence rows of observation versions die with their fact.
\* N145: the invalidate row is independent of the fact row; CascadeHidden models the old foreign key (purges drop it).
DropH(S) == IF CascadeHidden THEN S ELSE {}
Strip(S) == [n \in Nodes |-> [i \in 1..Len(vers[n]) |->
               IF CascadeEvidence /\ n \in Obs THEN [vers[n][i] EXCEPT !.finp = @ \ S] ELSE vers[n][i]]]

ChunkPurge(f) ==                    \* grace elapsed: the retired chunk's fact is deleted
  /\ f \in ctomb /\ f \notin gone
  /\ gone' = gone \cup {f} /\ vers' = Strip({f})
  /\ hidden' = hidden \ DropH({f}) /\ mat' = mat \ DropH({f}) /\ matOwe' = matOwe \ DropH({f})
  /\ UNCHANGED <<tomb, ms, hidRe, ctomb, born, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nCur, nRet, cv, ginv, sig>>

ReextractPurge(f) ==                \* REEXTRACTED_FACTS: the old-key fact is deleted after 1 h
  /\ f \in hidRe /\ f \notin gone
  /\ gone' = gone \cup {f} /\ vers' = Strip({f})
  /\ hidden' = hidden \ DropH({f}) /\ mat' = mat \ DropH({f}) /\ matOwe' = matOwe \ DropH({f})
  /\ UNCHANGED <<tomb, ms, hidRe, ctomb, born, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nCur, nRet, cv, ginv, sig>>

-----------------------------------------------------------------------------
(* Writers of derived versions (N120, N121): Propose = stage 2 stored      *)
(* (write-once, with base_version); a writer applies it with Verify (shared*)
(* lock, every rendered input and the base re-checked) and Commit.  A      *)
(* writer may crash (AbortW); the stored proposal outlives it.             *)

Propose ==
  /\ budget > 0
  /\ \E n \in Nodes : \E mode \in {"root", "update"} : \E cf \in SmallSubsets(VisFacts, MaxF) :
     \E co \in SmallSubsets(IF IsPage(n) THEN VisPairs ELSE {}, 1) :
       /\ (cf # {} \/ co # {})
       /\ (mode = "update" => cv[n] > 0 /\ cv[n] <= Len(vers[n]) /\ VisN(n, cv[n]))
       \* base: the version an update was rendered from; exp: current_version read at LoadPage by a root rebuild (N144)
       /\ props' = props \cup {[n |-> n, mode |-> mode, cf |-> cf, co |-> co,
                                base |-> IF mode = "update" THEN cv[n] ELSE 0,
                                exp |-> IF mode = "root" /\ RootExpected THEN cv[n] ELSE 0]}
  /\ budget' = budget - 1
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo,
                 matSnap, wr, nDel, nCur, nRet, NewV>>

Pick(w) ==
  /\ wr[w].ph = "idle" /\ props # {}
  /\ \E p \in props : wr' = [wr EXCEPT ![w] = [ph |-> "picked", p |-> p]]
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo,
                 matSnap, props, budget, nDel, nCur, nRet, NewV>>

\* Everything the rendered result depends on is still fine: inputs visible, base current and visible.
InputsOK(p) == (IsPage(p.n) /\ ~PageVerify) \/ (p.cf \subseteq VisFacts /\ p.co \subseteq VisPairs)
\* The base compare-and-set (UPDATE ... SET current_version = v+1 WHERE current_version = base): an update compares
\* its base, and since N144 a root rebuild compares the version it read at LoadPage (RootExpected).
BaseOK(p) ==
  /\ p.mode = "update" =>
       IF BaseCheck THEN cv[p.n] = p.base /\ p.base <= Len(vers[p.n]) /\ VisN(p.n, p.base)
                    ELSE Len(vers[p.n]) > 0 /\ VisN(p.n, Len(vers[p.n]))
  /\ (p.mode = "root" /\ RootExpected) => cv[p.n] = p.exp
CheckOK(p) == InputsOK(p) /\ BaseOK(p)

Verify(w) ==
  /\ wr[w].ph = "picked" /\ wr[w].p \in props
  /\ ~(UseLock /\ lockX)                       \* try-lock refused while a Materialize batch runs
  /\ CheckOK(wr[w].p)
  /\ wr' = [wr EXCEPT ![w].ph = "verified"]
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo,
                 matSnap, props, budget, nDel, nCur, nRet, NewV>>

\* Verification failed: ROLLBACK and discard the stored proposal (never repair it).
Discard(w) ==
  /\ wr[w].ph # "idle"
  /\ \/ wr[w].p \notin props                              \* another writer applied it: the unique key refuses
     \/ wr[w].p \in props /\ wr[w].ph = "picked" /\ ~(UseLock /\ lockX) /\ ~CheckOK(wr[w].p)
     \/ wr[w].p \in props /\ wr[w].ph = "verified" /\ ~BaseOK(wr[w].p)   \* the base compare-and-set failed
     \/ wr[w].p \in props /\ wr[w].ph = "verified" /\ Len(vers[wr[w].p.n]) >= MaxVer(wr[w].p.n)   \* bound of the model
  /\ props' = props \ {wr[w].p} /\ wr' = [wr EXCEPT ![w] = IdleW]
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo,
                 matSnap, budget, nDel, nCur, nRet, NewV>>

\* The insert is a compare-and-set on the base (UPDATE observations SET current_version = v+1 WHERE
\* current_version = base): shared-lock holders do not exclude each other, so the base is re-evaluated here.
Commit(w) ==
  /\ wr[w].ph = "verified" /\ wr[w].p \in props /\ BaseOK(wr[w].p)
  /\ LET p == wr[w].p IN LET n == p.n IN LET L == Len(vers[n]) IN
     /\ L < MaxVer(n)
     /\ \E cit \in SUBSET p.cf :
          /\ (EffAllShown => cit = p.cf)
          /\ (~EffAllShown => (p.cf # {} => cit # {}))
          /\ LET e == Max2(MaxSet({Mentioned[f] : f \in cit} \cup {vers[x[1]][x[2]].eff : x \in p.co}),
                           IF L = 0 THEN 0 ELSE vers[n][L].eff) IN
             LET upd == p.mode = "update" /\ L > 0 IN
             vers' = [vers EXCEPT ![n] = Append(@,
                        [root |-> IF ~upd THEN L + 1 ELSE vers[n][L].root,
                         finp |-> p.cf, oinp |-> p.co,
                         gfinp |-> p.cf \cup (IF upd THEN vers[n][p.base].gfinp ELSE {}),
                         goinp |-> p.co \cup (IF upd THEN vers[n][p.base].goinp ELSE {}),
                         eff |-> e, base |-> IF upd THEN p.base ELSE 0, st |-> "live", ck |-> p])]
     /\ hw' = [hw EXCEPT ![n] = L + 1]
     /\ cv' = [cv EXCEPT ![n] = cv[n] + 1]                \* the CAS: current_version + 1
     /\ props' = props \ {p}
  \* The transaction committed; the worker may crash before the result is recorded (Temporal retries the activity).
  /\ \E lost \in (IF AllowRetry THEN BOOLEAN ELSE {FALSE}) :
       wr' = [wr EXCEPT ![w] = IF lost THEN [ph |-> "lost", p |-> wr[w].p] ELSE IdleW]
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 budget, nDel, nCur, nRet, ginv, mat, matOwe, sig>>

\* N144: the re-execution of a committed Commit with the same rendered result (same commit_key).  Design: the
\* commit transaction first looks for a row with its commit_key; a hit returns that version and touches nothing.
\* CasFirst (the D23 order): the compare-and-set runs first; a root rebuild that was blind (not RootExpected)
\* advances current_version, and the insert then conflicts on commit_key, so the pointer names no row.
RetryCommit(w) ==
  /\ AllowRetry /\ wr[w].ph = "lost"
  /\ LET p == wr[w].p IN LET n == p.n IN
     LET hit == \E i \in 1..Len(vers[n]) : vers[n][i].ck = p IN
     LET casOk == IF p.mode = "update" THEN cv[n] = p.base ELSE (RootExpected => cv[n] = p.exp) IN
     cv' = IF CasFirst /\ casOk /\ hit THEN [cv EXCEPT ![n] = cv[n] + 1] ELSE cv
  /\ wr' = [wr EXCEPT ![w] = IdleW]
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo,
                 matSnap, props, budget, nDel, nCur, nRet, ginv, mat, matOwe, sig>>

AbortW(w) ==
  /\ AllowAbortW /\ wr[w].ph # "idle"
  /\ wr' = [wr EXCEPT ![w] = IdleW]
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo,
                 matSnap, props, budget, nDel, nCur, nRet, NewV>>

-----------------------------------------------------------------------------
(* Expunge (N119, N136): Materialize in batches, Purge, DerivedPurge       *)

\* The derived_hidden rows of one node for victim set V (DV: the tombstoned part of V, cause "doc").
Cause(f, DV) == IF f \in DV THEN <<"doc", 0>> ELSE <<"inv", f>>
HitF(n, w, V) == V \cap (vers[n][w].finp \cup
                         UNION {DerivF(x[1], x[2]) : x \in {y \in vers[n][w].oinp : Exists(y)}})
RowsNode(n, V, DV) == UNION {{<<n, vers[n][w].root, w, Cause(f, DV)>> : f \in HitF(n, w, V)} : w \in 1..Len(vers[n])}
\* N145(2): Materialize discovers its work from the markers: a pending tombstone, or an invalidation row whose
\* materialized_at is unset (hidden \ mat).  MatSignalOnly: the invalidation is found only through the signal
\* sent after the ack, which can be lost (LoseSignal).
MatWork == (\E d \in Docs : ms[d] = "pending") \/ (IF MatSignalOnly THEN sig ELSE hidden \ mat # {})

MatBegin ==
  /\ matPhase = "idle" /\ ~lockX
  /\ MatWork
  /\ LET D == {d \in Docs : ms[d] = "pending"} IN
     /\ matDocs' = D /\ matV0' = UNION {Victims(d) : d \in D} \cup hidden
  /\ matTodo' = Nodes /\ matPhase' = "running"
  /\ matOwe' = hidden \ mat /\ sig' = FALSE                \* the run owes the unstamped invalidations it read
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matSnap, wr, props, budget, nDel, nCur, nRet,
                 cv, ginv, mat>>

\* One batch (one node): the scan under the exclusive lock re-reads the open tombstones and fact_hidden.
MatScan(n) ==
  /\ matPhase = "running" /\ n \in matTodo /\ ~lockX
  /\ (UseLock => \A w \in Writers : wr[w].ph # "verified")      \* waits for shared holders
  /\ LET DV == UNION {Victims(d) : d \in matDocs} IN
     LET V == IF MatOnce THEN matV0 ELSE DV \cup hidden IN
     matSnap' = RowsNode(n, V, DV)
  /\ matTodo' = matTodo \ {n} /\ matPhase' = "scanned" /\ lockX' = TRUE
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, matDocs, matV0, wr, props, budget, nDel, nCur, nRet, NewV>>

MatWrite ==
  /\ matPhase = "scanned"
  /\ dh' = dh \cup matSnap
  /\ matPhase' = "running" /\ lockX' = FALSE /\ matSnap' = {}
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, matDocs, matV0, matTodo, wr, props, budget, nDel, nCur, nRet, NewV>>

MatEnd ==
  /\ matPhase = "running" /\ matTodo = {}
  /\ ms' = [d \in Docs |-> IF d \in matDocs THEN "materialized" ELSE ms[d]]
  /\ matPhase' = "idle" /\ matDocs' = {} /\ matV0' = {}
  /\ mat' = mat \cup matOwe /\ matOwe' = {}                 \* materialized_at stamped for what the run covered
  /\ UNCHANGED <<tomb, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matTodo, matSnap, wr, props, budget, nDel, nCur, nRet,
                 cv, ginv, sig>>

LoseSignal ==                       \* the SignalWithStart after the ack is lost (API death, a move terminating the singleton)
  /\ MatSignalOnly /\ sig /\ sig' = FALSE
  /\ UNCHANGED <<tomb, ms, hidden, hidRe, ctomb, born, gone, vers, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap,
                 wr, props, budget, nDel, nCur, nRet, cv, ginv, mat, matOwe>>

Purge(d) ==                         \* the victims' facts; evidence rows stay (no FK)
  /\ ms[d] = "materialized"
  /\ ms' = [ms EXCEPT ![d] = "purged"]
  /\ gone' = gone \cup (Victims(d) \cap born) /\ vers' = Strip(Victims(d))
  /\ hidden' = hidden \ DropH(Victims(d)) /\ mat' = mat \ DropH(Victims(d)) /\ matOwe' = matOwe \ DropH(Victims(d))
  /\ UNCHANGED <<tomb, hidRe, ctomb, born, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap, wr, props,
                 budget, nDel, nCur, nRet, cv, ginv, sig>>

RECURSIVE Trim(_)
Trim(s) == IF s # << >> /\ s[Len(s)].st = "absent" THEN Trim(SubSeq(s, 1, Len(s) - 1)) ELSE s

DocCovered(n, i) == \E r \in dh : r[4] = <<"doc", 0>> /\ r[1] = n /\ r[2] = vers[n][i].root /\ r[3] <= i

\* Versions covered by a document-cause row become content-free stubs (evidence and text gone, the row stays);
\* DropStub deletes the row instead, and the next version number is reused.
DerivedPurge(d) ==
  /\ ms[d] = "purged"
  /\ ms' = [ms EXCEPT ![d] = "done"]
  /\ vers' = [n \in Nodes |->
               LET s == [i \in 1..Len(vers[n]) |->
                           IF DocCovered(n, i) /\ vers[n][i].st = "live"
                             THEN [vers[n][i] EXCEPT !.finp = {}, !.oinp = {}, !.st = IF DropStub THEN "absent" ELSE "stub"]
                             ELSE vers[n][i]]
               IN IF DropStub THEN Trim(s) ELSE s]
  /\ UNCHANGED <<tomb, hidden, hidRe, ctomb, born, gone, hw, dh, lockX, matPhase, matDocs, matV0, matTodo, matSnap, wr,
                 props, budget, nDel, nCur, nRet, NewV>>

-----------------------------------------------------------------------------
WriterStep == \E w \in Writers : Pick(w) \/ Verify(w) \/ Discard(w) \/ Commit(w) \/ RetryCommit(w) \/ AbortW(w)
WriterProgress == \E w \in Writers : Verify(w) \/ Discard(w) \/ Commit(w) \/ RetryCommit(w)
Expunge == MatBegin \/ (\E n \in Nodes : MatScan(n)) \/ MatWrite \/ MatEnd \/ (\E d \in Docs : Purge(d) \/ DerivedPurge(d))
Next == Propose \/ WriterStep \/ Expunge \/ LoseSignal
        \/ (\E d \in Docs : DeleteDocument(d))
        \/ (\E f \in Facts : Ingest(f) \/ Invalidate(f) \/ Restore(f) \/ Replace(f) \/ Reextract(f)
                             \/ ChunkPurge(f) \/ ReextractPurge(f))

Spec == Init /\ [][Next]_vars /\ WF_vars(WriterProgress) /\ WF_vars(Expunge)

Symm == Permutations(Writers)

-----------------------------------------------------------------------------
(* Invariants *)

\* After DeleteDocument is acked, no served fact or derivation has a deleted fact.
NoDeletedDerivationServed ==
  \A T \in 1..MaxT :
    /\ \A f \in ServedFacts(T) : ~TombTrue(f)
    /\ \A x \in Served(T) : \A f \in GDeriv(x[1], x[2]) : ~TombTrue(f)

NoInvalidatedDerivationServed ==
  \A T \in 1..MaxT : \A x \in Served(T) : \A f \in GDeriv(x[1], x[2]) : f \notin hidden

\* Invalidate;Restore leaves no trace: a version with no currently hidden fact in its derivation
\* is visible exactly as if invalidation had never existed.
RestoreExact ==
  \A x \in Versions : vers[x[1]][x[2]].st = "live" =>
    ((\A f \in GDeriv(x[1], x[2]) : f \notin hidden) =>
      (VisN(x[1], x[2]) <=> \A f \in GDeriv(x[1], x[2]) : ~TombTrue(f)))

\* Precision: a version with no victim in its derivation is visible (the blast radius guard, the re-used
\* document id is not hidden by the old tombstone); REPLACE and re-extraction hide only the fact arms.
NoOverHiding ==
  /\ \A f \in born \ gone : (~Victim(f) /\ f \notin ctomb /\ f \notin hidRe) => f \in VisFacts
  /\ \A x \in Versions : vers[x[1]][x[2]].st = "live" =>
       ((\A f \in GDeriv(x[1], x[2]) : ~Victim(f)) => VisN(x[1], x[2]))

\* Served at T => nothing in the derivation was mentioned after T.
AsOfNoLeak ==
  \A T \in 1..MaxT : \A x \in Served(T) : \A f \in GDeriv(x[1], x[2]) : Mentioned[f] <= T

\* Once a document is materialized, derived_hidden covers every committed version
\* with a victim of its tombstone in the derivation.
MaterializeComplete ==
  /\ \A d \in Docs : ms[d] \in {"materialized", "purged", "done"} =>
       \A x \in Versions : (GDeriv(x[1], x[2]) \cap Victims(d) # {}) => Perm(x[1], x[2])
  \* N145(2): a stamped invalidation (materialized_at set) has its derived_hidden rows on every node ...
  /\ \A f \in mat : \A n \in Nodes : RowsNode(n, {f}, {}) \subseteq dh
  \* ... and an unstamped one is never stranded: while one exists and no run is in progress, Materialize
  \* is enabled (found from the marker, not from a signal that may be lost).
  /\ (hidden \ mat # {} /\ matPhase = "idle") => MatWork

\* N145: an acknowledged invalidation stays in force until Restore: no served version has an invalidated
\* fact in its derivation, whatever happened to the fact row (ginv is the ghost of the acknowledged set).
NoGhostInvalidatedServed ==
  \A T \in 1..MaxT : \A x \in Served(T) : GDeriv(x[1], x[2]) \cap ginv = {}

\* N144: current_version always names an existing row of the node (it is never ahead of the rows).
NoPhantomVersion ==
  \A n \in Nodes : cv[n] = 0 \/ (cv[n] <= Len(vers[n]) /\ vers[n][cv[n]].st # "absent" /\ vers[n][cv[n]].root <= cv[n])

\* N135(1): evidence outlives the facts it names -- a live version's evidence is everything its text
\* was derived from.
EvidenceOutlivesFacts ==
  \A x \in Versions :
    (\A w \in Seg(x[1], x[2]) : vers[x[1]][w].st = "live") =>
      (GDerivF(x[1], x[2]) \subseteq DerivF(x[1], x[2]))

\* N120(3): an update extends the version it was rendered from, which is the version it follows.
BaseCurrentAtCommit == \A x \in Versions : vers[x[1]][x[2]].base \in {0, x[2] - 1}
\* ... so no root rebuild lies between an update and its base: stale text never overwrites a rebuild.
NoLostRebuild ==
  \A x \in Versions : vers[x[1]][x[2]].base > 0 =>
    \A u \in (vers[x[1]][x[2]].base + 1)..(x[2] - 1) : vers[x[1]][u].root # u

\* N136: after DerivedPurge no live version holds text derived from the deleted document.
NoVictimTextAfterPurge ==
  \A d \in Docs : ms[d] = "done" =>
    \A x \in Versions : vers[x[1]][x[2]].st = "live" => GDeriv(x[1], x[2]) \cap Victims(d) = {}

\* N135(4), N136: version rows are never deleted (version numbers are not reused), and a stub or missing
\* row is never served.
FailClosed ==
  /\ \A n \in Nodes : Len(vers[n]) = hw[n]
  /\ \A x \in Versions : vers[x[1]][x[2]].st # "live" => ~VisN(x[1], x[2])

\* N135(2)-(3): a derived version whose derivation holds only re-extracted (reextract-hidden) facts as
\* hidden ones stays visible.
ReextractKeepsDerivedVisible ==
  \A x \in Versions : vers[x[1]][x[2]].st = "live" =>
    ((GDeriv(x[1], x[2]) \cap hidRe # {} /\ \A f \in GDeriv(x[1], x[2]) : ~Victim(f) /\ ~RowCovered(x[1], x[2]))
       => VisN(x[1], x[2]))

\* Liveness: a pending tombstone is eventually done.
ExpungeCompletes == \A d \in Docs : (ms[d] = "pending") ~> (ms[d] = "done")

=============================================================================
