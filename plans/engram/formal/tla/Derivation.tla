--------------------------- MODULE Derivation ---------------------------
(***************************************************************************)
(* Engram D22 (N113, N115-N121): visibility of derived data.               *)
(*                                                                         *)
(* Mutable state is a handful of markers; everything else is a read-time   *)
(* predicate over insert-only evidence.                                    *)
(*   tomb      document_tombstones (DeleteDocument; expunge_state in ms)   *)
(*   hidden    fact_hidden (Invalidate inserts, Restore deletes)           *)
(*   vers      observation_versions / page_versions with root_version and  *)
(*             inputs (fact ids; observation versions for pages)           *)
(*   dh        derived_hidden rows <<node, root_version, from_version>>    *)
(*   lockX     the derivation lock held exclusively by Expunge.Materialize;*)
(*             writers of derived versions hold it shared from the         *)
(*             re-verification to the commit (N120)                        *)
(* Segment of (n, v) = versions root_version(v)..v; Deriv(n, v) = union of *)
(* the segment's inputs (two levels: fact -> observation -> page).         *)
(*                                                                         *)
(* Expunge: Materialize (MatStart snapshots the covered versions, MatWrite *)
(* inserts derived_hidden and marks the documents materialized), then     *)
(* Purge deletes the victims' facts and the evidence rows naming them.     *)
(* After Purge only derived_hidden still hides a derived version, which is *)
(* why Materialize must see every committed version.                       *)
(*                                                                         *)
(* Ghost state: gfinp keeps the evidence that Purge deletes, so the        *)
(* invariants can state the truth about derivation after the real rows are *)
(* gone.  The implementation has no ghost.                                 *)
(*                                                                         *)
(* Knobs (design: UseLock, ~MatInvalid, EffAllShown):                      *)
(*   UseLock = FALSE   writers and Materialize ignore each other           *)
(*   MatInvalid = TRUE Materialize also covers invalidated facts           *)
(*   EffAllShown = FALSE effective_at counts only cited facts, not shown   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Docs, Facts, DocOf, Mentioned,   \* facts are 1..4; tuples <<doc,..>> and <<time,..>>
          Obs, Pages, MaxVerO, MaxVerP,    \* max versions per observation / page
          Writers, Budget,                 \* Route actions available in total
          MaxF,                            \* max fact inputs per version
          MaxT, MaxDeletes, MaxCuration,   \* time horizon, DeleteDocument count, Invalidate+Restore count
          UseLock, MatInvalid, EffAllShown

\* Design instance (cfg: DocOf <- DocOfDef, Mentioned <- MentionedDef): f1,f2 in d1; f3,f4 in d2.
DocOfDef == <<"d1", "d1", "d2", "d2">>
MentionedDef == <<1, 2, 2, 3>>

Nodes == Obs \cup Pages
IsPage(n) == n \in Pages
MaxVer(n) == IF n \in Pages THEN MaxVerP ELSE MaxVerO
Pairs == {<<o, v>> : o \in Obs, v \in 1..MaxVerO}

VARIABLES tomb, ms, hidden, vers, dh, lockX, matPhase, matDocs, matSnap,
          wr, budget, nDel, nCur
vars == <<tomb, ms, hidden, vers, dh, lockX, matPhase, matDocs, matSnap, wr, budget, nDel, nCur>>

Max2(a, b) == IF a >= b THEN a ELSE b
MaxSet(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : x <= m
SmallSubsets(S, k) == {s \in SUBSET S : Cardinality(s) <= k}
FactsOf(d) == {f \in Facts : DocOf[f] = d}

-----------------------------------------------------------------------------
(* Derivation and visibility (all pure functions of the state)             *)

Versions == UNION {{<<n, v>> : v \in 1..Len(vers[n])} : n \in Nodes}
Seg(n, v) == vers[n][v].root..v
DerivF(n, v)  == UNION {vers[n][w].finp  : w \in Seg(n, v)}      \* real rows
GDerivF(n, v) == UNION {vers[n][w].gfinp : w \in Seg(n, v)}      \* ghost: never purged
DerivO(n, v)  == UNION {vers[n][w].oinp  : w \in Seg(n, v)}      \* observation versions a page cites
GDeriv(n, v)  == GDerivF(n, v) \cup UNION {GDerivF(x[1], x[2]) : x \in DerivO(n, v)}

TombF(f) == DocOf[f] \in tomb
Victim(f) == TombF(f) \/ f \in hidden

RowCovered(n, v) == \E r \in dh : r[1] = n /\ r[2] = vers[n][v].root /\ r[3] <= v
Perm(n, v) == RowCovered(n, v) \/ \E x \in DerivO(n, v) : RowCovered(x[1], x[2])

\* N117: no victim fact in the segment's real evidence, and no derived_hidden row.
VisBase(n, v) == ~(\E f \in DerivF(n, v) : Victim(f)) /\ ~RowCovered(n, v)
\* A page version is also hidden when a cited observation version is hidden.
VisN(n, v) == VisBase(n, v) /\ \A x \in DerivO(n, v) : VisBase(x[1], x[2])

VisFacts == {f \in Facts : ~Victim(f)}
VisPairs == {x \in Versions : x[1] \in Obs /\ VisN(x[1], x[2])}

\* Recall at time T: per node, the latest visible version with effective_at <= T.
Served(T) == {x \in Versions :
                /\ VisN(x[1], x[2]) /\ vers[x[1]][x[2]].eff <= T
                /\ \A u \in (x[2] + 1)..Len(vers[x[1]]) : ~(VisN(x[1], u) /\ vers[x[1]][u].eff <= T)}
ServedFacts(T) == {f \in VisFacts : Mentioned[f] <= T}

-----------------------------------------------------------------------------
Rec == [root : 1..3, finp : SUBSET Facts, gfinp : SUBSET Facts, oinp : SUBSET Pairs, eff : 0..MaxT]
IdleW == [ph |-> "idle", n |-> CHOOSE n \in Nodes : TRUE, mode |-> "root",
          cf |-> {}, co |-> {}, sf |-> {}, so |-> {}]

TypeOK ==
  /\ tomb \subseteq Docs /\ hidden \subseteq Facts
  /\ ms \in [Docs -> {"none", "pending", "materialized", "purged"}]
  /\ \A n \in Nodes : \A i \in 1..Len(vers[n]) : vers[n][i] \in Rec
  /\ \A n \in Nodes : Len(vers[n]) <= MaxVer(n)
  /\ lockX \in BOOLEAN /\ matPhase \in {"idle", "scanned"}
  /\ budget \in 0..Budget /\ nDel \in 0..MaxDeletes /\ nCur \in 0..MaxCuration

Init ==
  /\ tomb = {} /\ ms = [d \in Docs |-> "none"] /\ hidden = {}
  /\ vers = [n \in Nodes |-> << >>] /\ dh = {}
  /\ lockX = FALSE /\ matPhase = "idle" /\ matDocs = {} /\ matSnap = {}
  /\ wr = [w \in Writers |-> IdleW]
  /\ budget = Budget /\ nDel = 0 /\ nCur = 0

-----------------------------------------------------------------------------
(* Markers: the only synchronous writes of a delete or invalidation (N115) *)

DeleteDocument(d) ==
  /\ d \notin tomb /\ nDel < MaxDeletes
  /\ tomb' = tomb \cup {d} /\ ms' = [ms EXCEPT ![d] = "pending"] /\ nDel' = nDel + 1
  /\ UNCHANGED <<hidden, vers, dh, lockX, matPhase, matDocs, matSnap, wr, budget, nCur>>

Invalidate(f) ==
  /\ f \notin hidden /\ ms[DocOf[f]] # "purged" /\ nCur < MaxCuration
  /\ hidden' = hidden \cup {f} /\ nCur' = nCur + 1
  /\ UNCHANGED <<tomb, ms, vers, dh, lockX, matPhase, matDocs, matSnap, wr, budget, nDel>>

Restore(f) ==
  /\ f \in hidden /\ nCur < MaxCuration
  /\ hidden' = hidden \ {f} /\ nCur' = nCur + 1
  /\ UNCHANGED <<tomb, ms, vers, dh, lockX, matPhase, matDocs, matSnap, wr, budget, nDel>>

-----------------------------------------------------------------------------
(* Writers of derived versions: two-stage consolidation / PageRefresh      *)
(* Route = stage 1 (decisions only, no lock); Verify = stage 2 start       *)
(* (shared derivation lock, re-verify visibility); Commit inserts the      *)
(* version and releases the lock.  Markers may land between any two steps. *)

Route(w) ==
  /\ wr[w].ph = "idle" /\ budget > 0
  /\ \E n \in Nodes : \E mode \in {"root", "update"} : \E cf \in SmallSubsets(VisFacts, MaxF) :
     \E co \in SmallSubsets(IF IsPage(n) THEN VisPairs ELSE {}, 1) :
       /\ (cf # {} \/ co # {})
       /\ (mode = "update" => Len(vers[n]) > 0)
       /\ wr' = [wr EXCEPT ![w] = [ph |-> "routed", n |-> n, mode |-> mode, cf |-> cf, co |-> co,
                                    sf |-> {}, so |-> {}]]
  /\ budget' = budget - 1
  /\ UNCHANGED <<tomb, ms, hidden, vers, dh, lockX, matPhase, matDocs, matSnap, nDel, nCur>>

Verify(w) ==
  /\ wr[w].ph = "routed"
  /\ ~(UseLock /\ lockX)                       \* try-lock refused while Materialize runs
  /\ LET r == wr[w] IN LET sf == r.cf \cap VisFacts IN LET so == r.co \cap VisPairs IN
     /\ (sf # {} \/ so # {})
     /\ (r.mode = "update" => Len(vers[r.n]) > 0 /\ VisN(r.n, Len(vers[r.n])))
     /\ wr' = [wr EXCEPT ![w].ph = "verified", ![w].sf = sf, ![w].so = so]
  /\ UNCHANGED <<tomb, ms, hidden, vers, dh, lockX, matPhase, matDocs, matSnap, budget, nDel, nCur>>

Commit(w) ==
  /\ wr[w].ph = "verified"
  /\ LET r == wr[w] IN LET n == r.n IN LET L == Len(vers[n]) IN
     /\ L < MaxVer(n)
     /\ \A f \in r.sf : ms[DocOf[f]] # "purged"            \* FK: evidence cannot name a purged fact
     /\ \E cit \in SUBSET r.sf :
          /\ (EffAllShown => cit = r.sf)
          /\ (~EffAllShown => (r.sf # {} => cit # {}))
          /\ LET e == Max2(MaxSet({Mentioned[f] : f \in cit} \cup {vers[x[1]][x[2]].eff : x \in r.so}),
                           IF L = 0 THEN 0 ELSE vers[n][L].eff) IN
             vers' = [vers EXCEPT ![n] = Append(@,
                        [root |-> IF r.mode = "root" \/ L = 0 THEN L + 1 ELSE vers[n][L].root,
                         finp |-> r.sf, gfinp |-> r.sf, oinp |-> r.so, eff |-> e])]
  /\ wr' = [wr EXCEPT ![w] = IdleW]
  /\ UNCHANGED <<tomb, ms, hidden, dh, lockX, matPhase, matDocs, matSnap, budget, nDel, nCur>>

AbortW(w) ==
  /\ wr[w].ph # "idle"
  /\ wr' = [wr EXCEPT ![w] = IdleW]
  /\ UNCHANGED <<tomb, ms, hidden, vers, dh, lockX, matPhase, matDocs, matSnap, budget, nDel, nCur>>

-----------------------------------------------------------------------------
(* Expunge (N119): Materialize under the exclusive lock, then Purge        *)

MatStart ==
  /\ matPhase = "idle" /\ ~lockX
  /\ \E d \in Docs : ms[d] = "pending"
  /\ (UseLock => \A w \in Writers : wr[w].ph # "verified")      \* waits for shared holders
  /\ LET D == {d \in Docs : ms[d] = "pending"} IN
     LET V == UNION {FactsOf(d) : d \in D} \cup (IF MatInvalid THEN hidden ELSE {}) IN
     /\ matDocs' = D
     /\ matSnap' = UNION {{<<x[1], vers[x[1]][x[2]].root, w>> :
                         w \in {u \in Seg(x[1], x[2]) : vers[x[1]][u].finp \cap V # {}}} : x \in Versions}
  /\ matPhase' = "scanned" /\ lockX' = TRUE
  /\ UNCHANGED <<tomb, ms, hidden, vers, dh, wr, budget, nDel, nCur>>

MatWrite ==
  /\ matPhase = "scanned"
  /\ dh' = dh \cup matSnap
  /\ ms' = [d \in Docs |-> IF d \in matDocs THEN "materialized" ELSE ms[d]]
  /\ matPhase' = "idle" /\ lockX' = FALSE /\ matDocs' = {} /\ matSnap' = {}
  /\ UNCHANGED <<tomb, hidden, vers, wr, budget, nDel, nCur>>

Purge(d) ==
  /\ ms[d] = "materialized"
  /\ ms' = [ms EXCEPT ![d] = "purged"]
  /\ vers' = [n \in Nodes |-> [i \in 1..Len(vers[n]) |->
                 [vers[n][i] EXCEPT !.finp = @ \ FactsOf(d)]]]
  /\ UNCHANGED <<tomb, hidden, dh, lockX, matPhase, matDocs, matSnap, wr, budget, nDel, nCur>>

-----------------------------------------------------------------------------
WriterStep == \E w \in Writers : Verify(w) \/ Commit(w) \/ AbortW(w)
Expunge == MatStart \/ MatWrite \/ \E d \in Docs : Purge(d)
Next == (\E w \in Writers : Route(w)) \/ WriterStep \/ Expunge
        \/ (\E d \in Docs : DeleteDocument(d))
        \/ (\E f \in Facts : Invalidate(f) \/ Restore(f))

Spec == Init /\ [][Next]_vars /\ WF_vars(WriterStep) /\ WF_vars(MatStart) /\ WF_vars(MatWrite)
        /\ WF_vars(\E d \in Docs : Purge(d))

-----------------------------------------------------------------------------
(* Invariants *)

\* After DeleteDocument is acked, no served fact or derivation has a fact of the document.
NoDeletedDerivationServed ==
  \A T \in 1..MaxT :
    /\ \A f \in ServedFacts(T) : ~TombF(f)
    /\ \A x \in Served(T) : \A f \in GDeriv(x[1], x[2]) : ~TombF(f)

NoInvalidatedDerivationServed ==
  \A T \in 1..MaxT : \A x \in Served(T) : \A f \in GDeriv(x[1], x[2]) : f \notin hidden

\* Invalidate;Restore leaves no trace: a version with no currently hidden fact in its
\* derivation is visible exactly as if invalidation had never existed.
RestoreExact ==
  \A x \in Versions : (\A f \in GDeriv(x[1], x[2]) : f \notin hidden) =>
      (VisN(x[1], x[2]) <=> \A f \in GDeriv(x[1], x[2]) : ~TombF(f))

\* Precision: a version with no victim in its derivation is visible (the blast radius guard).
NoOverHiding ==
  \A x \in Versions : (\A f \in GDeriv(x[1], x[2]) : ~Victim(f)) => VisN(x[1], x[2])

\* Served at T => nothing in the derivation was mentioned after T.
AsOfNoLeak ==
  \A T \in 1..MaxT : \A x \in Served(T) : \A f \in GDeriv(x[1], x[2]) : Mentioned[f] <= T

\* Once a document is materialized, derived_hidden covers every committed version
\* with one of its facts in the derivation.
MaterializeComplete ==
  \A d \in Docs : ms[d] \in {"materialized", "purged"} =>
    \A x \in Versions : (GDeriv(x[1], x[2]) \cap FactsOf(d) # {}) => Perm(x[1], x[2])

\* Liveness: every tombstone is eventually purged.
ExpungeCompletes == \A d \in Docs : (d \in tomb) ~> (ms[d] = "purged")

=============================================================================
