--------------------------- MODULE Durability ---------------------------
(***************************************************************************)
(* Engram D23 (N122, N123, N134): acknowledged deletes survive restore and *)
(* failover.                                                               *)
(*                                                                         *)
(* Write path (design): the marker transaction commits locally (a commit   *)
(* that a restore or failover may lose); only then is the intent object    *)
(* put into the blob store (strongly consistent, shard independent); then  *)
(* the ack, after a re-read of the marker.  The intent records the         *)
(* marker's exact effect (for a document delete: the versions it covers)   *)
(* and the subject's previous deletion_log entry (prev_operation_id).      *)
(* A duplicate attempt (concurrent, or a retry after a crash between       *)
(* commit and put) finds the subject already deleting/hidden: it writes no *)
(* marker and puts the committed marker's own intent (put-if-absent), so   *)
(* an ack always implies an intent for the marker the client relies on.    *)
(*                                                                         *)
(* Restore / failover to point p: the shard keeps only commits with        *)
(* commit time <= p and comes up `restoring` (reads and writes rejected).  *)
(* The replay floor `fl` lives in the catalog, OUTSIDE the restorable      *)
(* state, and is lowered (min) by every restore, never raised (N134).      *)
(* Replay applies, per subject in prev_operation_id chain order, every     *)
(* intent with deleted_at + Margin >= fl that is not yet applied, with the *)
(* recorded effect, verbatim; only then do reads reopen.  A second restore *)
(* may hit before or after the first replay.                               *)
(*                                                                         *)
(* Ops come from a fixed Shape (op i has subject and kind Shape[i]):       *)
(*   y: del (document delete), ret (retain; revives the document, no       *)
(*      intent, RPO 60 s).  Version 0 exists from the start.               *)
(*   x: inv / res (fact_hidden: last state wins).                          *)
(* A failed op (API crash, transport error) may or may not have committed; *)
(* the client's retry is a new op (a duplicate if the marker is there).    *)
(* The recorded time `at` (deleted_at) of an op is its issue time minus a  *)
(* host clock skew in 0..Skew.                                             *)
(*                                                                         *)
(* Knobs (design: all TRUE except Recompute/ClockOrder/AnyOrder variants): *)
(*   IntentAfterCommit FALSE  the intent is put before the marker and has  *)
(*                     no recorded effect: replay recomputes it (old N122; *)
(*                     orphan intents, C-5)                                *)
(*   AckNeedsIntent    FALSE  the ack may precede the intent               *)
(*   AckRecheck        FALSE  no re-read of the marker before the ack      *)
(*   DupReput          FALSE  a duplicate that found the marker puts nothing *)
(*   ReplayFirst       FALSE  reads reopen before replay finishes          *)
(*   FloorMode         "min" lower only (N134); "raise" Reopen resets the  *)
(*                     floor (C-4); "target" each restore sets its target  *)
(*   OrderMode         "chain" prev_operation_id; "clock" (deleted_at,id); *)
(*                     "any"                                               *)
(* A Margin below Lat + Skew loses commits outside the replay window.      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Shape, MaxT, Lat, Skew, Margin, MaxRestores,
          IntentAfterCommit, AckNeedsIntent, AckRecheck, DupReput, ReplayFirst,
          FloorMode, OrderMode

ShapeDoc  == <<[s |-> "y", k |-> "del"], [s |-> "y", k |-> "del"], [s |-> "y", k |-> "ret"]>>
ShapeFact == <<[s |-> "x", k |-> "inv"], [s |-> "x", k |-> "res"], [s |-> "x", k |-> "inv"]>>

N == Len(Shape)
OpIds == 1..N
S(i) == Shape[i].s
K(i) == Shape[i].k

VARIABLES t, ops, intents, dbq, sst, fl, nIssued, nRestore, cn
vars == <<t, ops, intents, dbq, sst, fl, nIssued, nRestore, cn>>

\* dbq: the marker rows the shard has applied, in application order: [op, eff].
\* eff: for a document delete, the versions it covers (the tombstone's up_to_version).
db == {dbq[i].op : i \in DOMAIN dbq}
Entries == {dbq[i] : i \in DOMAIN dbq}

NoOp == [ph |-> "none", it |-> 0, at |-> 0, ct |-> 0, cn0 |-> 0, eff |-> {}, prev |-> 0, obs |-> 0]
Phases == {"none", "new", "committed", "acked", "failed"}

TypeOK ==
  /\ t \in 0..MaxT /\ intents \subseteq OpIds /\ db \subseteq OpIds /\ Len(dbq) <= 2 * N
  /\ sst \in {"active", "restoring"} /\ fl \in 0..(MaxT + 1)
  /\ \A i \in OpIds : ops[i].ph \in Phases

Init ==
  /\ t = 0 /\ ops = [i \in OpIds |-> NoOp] /\ intents = {} /\ dbq = << >>
  /\ sst = "active" /\ fl = MaxT + 1 /\ nIssued = 0 /\ nRestore = 0 /\ cn = 0

-----------------------------------------------------------------------------
(* What the marker tables show *)

RetOps == {e.op : e \in {e \in Entries : K(e.op) = "ret"}}
AllRet == {0} \cup RetOps                                 \* document versions that exist
DelCov == UNION {e.eff : e \in {e \in Entries : K(e.op) = "del"}}
Vis == AllRet \ DelCov                                    \* versions of the document that are served

IdxOn(s) == {i \in DOMAIN dbq : S(dbq[i].op) = s /\ K(dbq[i].op) # "ret"}
LastOn(s) == IF IdxOn(s) = {} THEN 0 ELSE dbq[CHOOSE m \in IdxOn(s) : \A j \in IdxOn(s) : j <= m].op
HiddenX == LastOn("x") # 0 /\ K(LastOn("x")) = "inv"

Issued == {i \in OpIds : ops[i].ph # "none"}
Live(i) == ops[i].ph \in {"new", "committed"}

-----------------------------------------------------------------------------
Issue ==
  /\ nIssued < N /\ sst = "active"
  /\ LET i == nIssued + 1 IN
     /\ (S(i) = "x" => \A j \in Issued : ~Live(j))          \* x: one request at a time
     /\ \E sk \in 0..Skew :
          /\ sk <= t
          /\ ops' = [ops EXCEPT ![i] = [NoOp EXCEPT !.ph = "new", !.it = t, !.at = t - sk]]
  /\ nIssued' = nIssued + 1
  /\ UNCHANGED <<t, intents, dbq, sst, fl, nRestore, cn>>

\* The marker transaction: a local commit, refused by the namespace fence while restoring.
\* A duplicate (subject already deleting / hidden) writes no marker and observes the existing one.
Commit(o) ==
  /\ ops[o].ph = "new" /\ sst = "active" /\ t - ops[o].it <= Lat
  /\ (IntentAfterCommit \/ o \in intents)                  \* old order: intent first
  /\ LET k == K(o) IN LET s == S(o) IN
     LET isDup == (k = "del" /\ Vis = {}) \/ (k = "inv" /\ HiddenX) IN
     /\ (k = "res" => HiddenX)
     /\ IF isDup
          THEN /\ ops' = [ops EXCEPT ![o] = [@ EXCEPT !.ph = "committed", !.ct = t, !.obs = LastOn(s)]]
               /\ UNCHANGED <<dbq, cn>>
          ELSE LET eff == IF k = "del" THEN AllRet ELSE {} IN
               /\ dbq' = Append(dbq, [op |-> o, eff |-> eff])
               /\ cn' = cn + 1
               /\ ops' = [ops EXCEPT ![o] = [@ EXCEPT !.ph = "committed", !.ct = t, !.cn0 = cn + 1,
                            !.eff = eff, !.obs = o, !.prev = IF k = "ret" THEN 0 ELSE LastOn(s)]]
  /\ UNCHANGED <<t, intents, sst, fl, nIssued, nRestore>>

\* Design: after the commit, put the intent of the marker this attempt relies on (its own, or the
\* one a duplicate observed).  Old order: before the commit, its own.
PutIntent(o) ==
  /\ K(o) # "ret"
  /\ IF IntentAfterCommit
       THEN /\ ops[o].ph = "committed" /\ ops[o].obs \notin intents
            /\ (ops[o].obs = o \/ DupReput)
            /\ intents' = intents \cup {ops[o].obs}
       ELSE /\ ops[o].ph = "new" /\ o \notin intents
            /\ intents' = intents \cup {o}
  /\ UNCHANGED <<t, ops, dbq, sst, fl, nIssued, nRestore, cn>>

IntentOK(o) == ops[o].obs \in intents \/ ~AckNeedsIntent \/ (ops[o].obs # o /\ ~DupReput)

Ack(o) ==
  /\ ops[o].ph = "committed"
  /\ K(o) = "ret" \/ (IntentOK(o) /\ (AckRecheck => ops[o].obs \in db))
  /\ ops' = [ops EXCEPT ![o].ph = "acked"]
  /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn>>

\* The client sees an error: before the commit (Abort), or after it with the intent not yet put
\* (CrashBeforePut), or with the intent put (LostAck).
Abort(o) == /\ ops[o].ph = "new" /\ K(o) # "ret"
            /\ ops' = [ops EXCEPT ![o].ph = "failed"]
            /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn>>
CrashBeforePut(o) == /\ ops[o].ph = "committed" /\ K(o) # "ret" /\ ops[o].obs \notin intents
                     /\ ops' = [ops EXCEPT ![o].ph = "failed"]
                     /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn>>
LostAck(o) == /\ ops[o].ph = "committed" /\ K(o) # "ret" /\ ops[o].obs \in intents
              /\ ops' = [ops EXCEPT ![o].ph = "failed"]
              /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn>>

Tick ==
  /\ t < MaxT /\ t' = t + 1
  /\ UNCHANGED <<ops, intents, dbq, sst, fl, nIssued, nRestore, cn>>

\* Restore or failover to point p: commits after p are lost; the floor (catalog) is not.
RestoreTo(p) ==
  /\ nRestore < MaxRestores /\ p <= t
  /\ dbq' = SelectSeq(dbq, LAMBDA e : ops[e.op].ct <= p)
  /\ sst' = "restoring" /\ nRestore' = nRestore + 1
  /\ fl' = IF FloorMode = "target" THEN p ELSE IF fl < p THEN fl ELSE p
  /\ UNCHANGED <<t, ops, intents, nIssued, cn>>

InWindow(o) == o \in intents /\ ops[o].at + Margin >= fl
Pending == {o \in intents : InWindow(o) /\ o \notin db}

Before(o, o2) == ops[o].at < ops[o2].at \/ (ops[o].at = ops[o2].at /\ o <= o2)

\* Admin variant of the marker transaction: same effect, fence bypassed.
Replay(o) ==
  /\ sst = "restoring" /\ o \in Pending
  /\ CASE OrderMode = "chain" -> ops[o].prev = 0 \/ ops[o].prev \notin Pending
       [] OrderMode = "clock" -> \A o2 \in Pending : S(o2) = S(o) => Before(o, o2)
       [] OTHER -> TRUE
  /\ LET eff == IF K(o) = "del" /\ ~IntentAfterCommit THEN AllRet ELSE ops[o].eff IN
     dbq' = Append(dbq, [op |-> o, eff |-> eff])
  /\ ops' = [ops EXCEPT ![o].ct = t]
  /\ UNCHANGED <<t, intents, sst, fl, nIssued, nRestore, cn>>

Reopen ==
  /\ sst = "restoring" /\ (ReplayFirst => Pending = {})
  /\ sst' = "active"
  /\ fl' = IF FloorMode = "raise" THEN MaxT + 1 ELSE fl
  /\ UNCHANGED <<t, ops, intents, dbq, nIssued, nRestore, cn>>

Next == Tick \/ Reopen \/ Issue \/ (\E p \in 0..MaxT : RestoreTo(p))
        \/ (\E o \in OpIds : PutIntent(o) \/ Commit(o) \/ Ack(o) \/ Abort(o) \/ CrashBeforePut(o)
                             \/ LostAck(o) \/ Replay(o))

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
LastIssued(s) == LET L == {i \in Issued : S(i) = s} IN IF L = {} THEN 0 ELSE CHOOSE m \in L : \A j \in L : j <= m

\* Every acknowledged delete or invalidation has a durable intent.
AckImpliesIntent == \A o \in OpIds : (ops[o].ph = "acked" /\ K(o) # "ret") => ops[o].obs \in intents

\* The marker an acknowledged delete relies on is in force whenever the shard serves.
AckedDeleteSurvives == sst = "active" => \A o \in OpIds : (ops[o].ph = "acked" /\ K(o) = "del") => ops[o].obs \in db

\* Invalidate/Restore: per-subject chain order makes the last state win, whatever the clocks say.
IntentOrderLastWins ==
  sst = "active" =>
    LET l == LastIssued("x") IN
    (l # 0 /\ ops[l].ph = "acked") => (HiddenX <=> K(l) = "inv")

\* An effect that reaches a later acknowledged write is not allowed: every applied marker was
\* written by a committed operation, and covers only versions committed before it.
NoUnackedEffectOnLaterAck ==
  \A e \in Entries : K(e.op) # "ret" =>
    /\ ops[e.op].cn0 > 0
    /\ \A r \in e.eff \ {0} : ops[r].cn0 < ops[e.op].cn0

=============================================================================
