------------------------------ MODULE AsOf ------------------------------
(***************************************************************************)
(* Engram D9: `as_of` isolation.                                           *)
(*                                                                         *)
(* Facts carry `mentioned_at` (when the source said it).  Observations are *)
(* versioned; each version is produced by a consolidation call whose       *)
(* prompt contains a batch of facts and the current text of some candidate *)
(* observations, and cites a subset of the batch as sources.               *)
(*                                                                         *)
(* recall(T) returns facts with mentioned_at <= T and, per observation,   *)
(* the latest version with effective_at <= T.  The property to protect is  *)
(* that nothing recall(T) returns was derived from content with            *)
(* mentioned_at > T -- "derived" meaning *anything the LLM saw*, not only  *)
(* what it chose to cite.                                                  *)
(*                                                                         *)
(* Knob EffectiveFromInputs:                                               *)
(*   TRUE  (design, ND-4 in section 7): effective_at = max over every      *)
(*         fact in the prompt and the effective_at of every candidate      *)
(*         observation version in the prompt.                              *)
(*   FALSE (D9 as first written): effective_at = max over *cited* sources  *)
(*         only.  TLC finds a leak: a version written from a batch that    *)
(*         contains a newer fact, citing only older facts, is served at a  *)
(*         T before the newer fact was mentioned.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Facts,               \* e.g. {f1, f2, f3}
          Obs,                 \* e.g. {o1, o2}
          MaxTime,             \* mentioned_at values range over 1..MaxTime
          MaxVersions,         \* versions per observation
          EffectiveFromInputs  \* BOOLEAN, see above

Times == 1..MaxTime
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

VARIABLES
  mentioned,   \* [Facts -> 0..MaxTime]   0 = fact not (yet) retained
  versions     \* [Obs -> Seq([cited : SUBSET Facts, deriv : SUBSET Facts, eff : Nat])]

vars == <<mentioned, versions>>

Version == [cited : SUBSET Facts, deriv : SUBSET Facts, eff : 0..MaxTime]

TypeOK ==
  /\ mentioned \in [Facts -> 0..MaxTime]
  /\ versions \in [Obs -> Seq(Version)]
  /\ \A o \in Obs : Len(versions[o]) <= MaxVersions

Live == {f \in Facts : mentioned[f] > 0}
Latest(o) == versions[o][Len(versions[o])]
HasVersion(o) == Len(versions[o]) > 0

Init ==
  /\ mentioned = [f \in Facts |-> 0]
  /\ versions = [o \in Obs |-> << >>]

\* A fact is retained with its mentioned_at.  Retain order is unrelated to
\* mentioned_at order (backfills, late documents).
InsertFact(f, t) ==
  /\ mentioned[f] = 0
  /\ mentioned' = [mentioned EXCEPT ![f] = t]
  /\ UNCHANGED versions

\* One consolidation op (create = first version, update = later version) for
\* observation o.  The prompt held `batch` (facts) and the latest text of the
\* candidate observations `cands`; the op cites `cited` \subseteq batch.
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
  /\ UNCHANGED mentioned

Next ==
  \/ \E f \in Facts, t \in Times : InsertFact(f, t)
  \/ \E o \in Obs, batch \in SUBSET Facts, cands \in SUBSET Obs, cited \in SUBSET Facts :
       Consolidate(o, batch, cands, cited)
  \/ UNCHANGED vars      \* quiescence

Spec == Init /\ [][Next]_vars

Symm == Permutations(Facts) \cup Permutations(Obs)

-----------------------------------------------------------------------------
(* Recall *)

\* The version index of o that recall(T) serves (0 = none).
RetV(vs, o, T) ==
  LET ok == {i \in 1..Len(vs[o]) : vs[o][i].eff <= T}
  IN IF ok = {} THEN 0 ELSE Max(ok)

RecallFacts(T) == {f \in Facts : mentioned[f] \in 1..T}

-----------------------------------------------------------------------------
(* Properties *)

\* Nothing served at T was derived from content mentioned after T.
NoLeak ==
  \A T \in Times :
    /\ \A f \in RecallFacts(T) : mentioned[f] <= T
    /\ \A o \in Obs : LET i == RetV(versions, o, T) IN
         i /= 0 => \A f \in versions[o][i].deriv : mentioned[f] <= T

\* effective_at is never below the newest cited source (D9's definition is
\* a lower bound of the design's).
EffectiveCoversCited ==
  \A o \in Obs : \A i \in 1..Len(versions[o]) :
    \A f \in versions[o][i].cited : mentioned[f] <= versions[o][i].eff

\* An update whose effective_at is after T does not change what recall(T)
\* serves for that observation: the older version stays pinned.
OlderVersionStable ==
  [][\A o \in Obs : \A T \in Times :
       (Len(versions'[o]) > Len(versions[o]) /\ versions'[o][Len(versions'[o])].eff > T)
         => RetV(versions', o, T) = RetV(versions, o, T)]_vars

=============================================================================
