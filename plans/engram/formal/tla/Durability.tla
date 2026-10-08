--------------------------- MODULE Durability ---------------------------
(***************************************************************************)
(* Engram D22 (N122, N123): acknowledged deletes survive restore/failover. *)
(*                                                                         *)
(* Write path: put the intent object in the blob store (strongly           *)
(* consistent, shard independent), run the marker transaction (a local     *)
(* commit that a restore or failover may lose), then ack.                  *)
(*                                                                         *)
(* Restore / failover to point p: the shard keeps only commits with        *)
(* commit time <= p and comes up `frozen/restore` (writes and reads        *)
(* rejected).  Replay applies, in name order, every intent whose           *)
(* deleted_at >= p - Margin and that is not yet applied; only then reads   *)
(* reopen.  A second restore may hit before or after the first replay.     *)
(*                                                                         *)
(* Ops: kind inv/res on subject "x" (fact_hidden: last state wins), del on *)
(* subject "y" (document tombstone).  Ops on one subject are sequential:   *)
(* the next starts after the previous was acked; a failed op (transport    *)
(* error, intent maybe written) closes the subject.  An op that has not    *)
(* committed within Lat ticks of its intent time is abandoned.             *)
(*                                                                         *)
(* Knobs (design: all TRUE):                                               *)
(*   IntentBeforeAck  FALSE: the ack may precede the intent put            *)
(*   ReplayFirst      FALSE: reads reopen before replay finishes           *)
(*   OrderedReplay    FALSE: replay applies intents in any order           *)
(*   MonotoneFloor    FALSE: each restore recomputes the replay floor from its *)
(*                    own target; a second restore (or failover) during or *)
(*                    after a replay then skips intents the first restore  *)
(*                    lost and the second loses again.  TRUE: the floor    *)
(*                    never moves later (min over all restores, kept while *)
(*                    intents are retained).                               *)
(* A Margin below the longest intent-to-commit latency (Margin 0, Lat 2 in *)
(* Durability_NarrowWindow.cfg) loses commits outside the replay window.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS MaxOps, MaxT, Lat, Margin, MaxRestores,
          IntentBeforeAck, ReplayFirst, OrderedReplay, MonotoneFloor

Subjects == {"x", "y"}
Allowed(s, k) == (s = "x" /\ k \in {"inv", "res"}) \/ (s = "y" /\ k = "del")
OpIds == 1..MaxOps

VARIABLES t, ops, intents, dbq, sst, rp, nIssued, nRestore
vars == <<t, ops, intents, dbq, sst, rp, nIssued, nRestore>>

\* dbq: the marker transactions the shard has applied, in application order.
db == {dbq[i] : i \in DOMAIN dbq}

NoOp == [subj |-> "x", kind |-> "inv", at |-> 0, ph |-> "none", ct |-> 0]
Phases == {"none", "new", "committed", "acked", "failed"}

TypeOK ==
  /\ t \in 0..MaxT /\ intents \subseteq OpIds /\ db \subseteq OpIds /\ Len(dbq) <= MaxOps
  /\ sst \in {"active", "restoring"} /\ rp \in 0..MaxT
  /\ \A i \in OpIds : ops[i].ph \in Phases

Init ==
  /\ t = 0 /\ ops = [i \in OpIds |-> NoOp] /\ intents = {} /\ dbq = << >>
  /\ sst = "active" /\ rp = 0 /\ nIssued = 0 /\ nRestore = 0

Issued == {i \in OpIds : ops[i].ph # "none"}
OnSubj(s) == {i \in Issued : ops[i].subj = s}

Issue(s, k) ==
  /\ nIssued < MaxOps /\ Allowed(s, k) /\ sst = "active"
  /\ \A i \in OnSubj(s) : ops[i].ph = "acked"          \* sequential; a failed op closes the subject
  /\ (s = "y" => OnSubj(s) = {})
  /\ LET i == nIssued + 1 IN
     ops' = [ops EXCEPT ![i] = [subj |-> s, kind |-> k, at |-> t, ph |-> "new", ct |-> 0]]
  /\ nIssued' = nIssued + 1
  /\ UNCHANGED <<t, intents, dbq, sst, rp, nRestore>>

PutIntent(o) ==
  /\ o \notin intents /\ ops[o].ph \in (IF IntentBeforeAck THEN {"new"} ELSE {"new", "committed", "acked"})
  /\ intents' = intents \cup {o}
  /\ UNCHANGED <<t, ops, dbq, sst, rp, nIssued, nRestore>>

\* The marker transaction: a local commit, refused by the namespace fence while restoring.
Commit(o) ==
  /\ ops[o].ph = "new" /\ o \notin db /\ sst = "active" /\ t - ops[o].at <= Lat
  /\ (IntentBeforeAck => o \in intents)
  /\ dbq' = Append(dbq, o)
  /\ ops' = [ops EXCEPT ![o].ph = "committed", ![o].ct = t]
  /\ UNCHANGED <<t, intents, sst, rp, nIssued, nRestore>>

Ack(o) ==
  /\ ops[o].ph = "committed" /\ o \in db
  /\ (IntentBeforeAck => o \in intents)
  /\ ops' = [ops EXCEPT ![o].ph = "acked"]
  /\ UNCHANGED <<t, intents, dbq, sst, rp, nIssued, nRestore>>

Fail(o) ==
  /\ ops[o].ph \in {"new", "committed"}
  /\ ops' = [ops EXCEPT ![o].ph = "failed"]
  /\ UNCHANGED <<t, intents, dbq, sst, rp, nIssued, nRestore>>

Tick ==
  /\ t < MaxT /\ t' = t + 1
  /\ UNCHANGED <<ops, intents, dbq, sst, rp, nIssued, nRestore>>

\* Restore or failover to point p: commits after p are lost.
Restore(p) ==
  /\ nRestore < MaxRestores /\ p <= t
  /\ dbq' = SelectSeq(dbq, LAMBDA o : ops[o].ct <= p)
  /\ sst' = "restoring" /\ nRestore' = nRestore + 1
  /\ rp' = IF MonotoneFloor /\ nRestore > 0 /\ rp < p THEN rp ELSE p
  /\ UNCHANGED <<t, ops, intents, nIssued>>

InWindow(o) == o \in intents /\ ops[o].at + Margin >= rp
Pending == {o \in intents : InWindow(o) /\ o \notin db}

\* Admin variant of the marker transaction: same effect, fence bypassed.
Replay(o) ==
  /\ sst = "restoring" /\ o \in Pending
  /\ (OrderedReplay => \A o2 \in Pending : o <= o2)           \* name order = (deleted_at, operation id)
  /\ dbq' = Append(dbq, o)
  /\ ops' = [ops EXCEPT ![o].ct = t]
  /\ UNCHANGED <<t, intents, sst, rp, nIssued, nRestore>>

Reopen ==
  /\ sst = "restoring" /\ (ReplayFirst => Pending = {})
  /\ sst' = "active"
  /\ UNCHANGED <<t, ops, intents, dbq, rp, nIssued, nRestore>>

Next == Tick \/ Reopen \/ (\E p \in 0..MaxT : Restore(p)) \/ (\E o \in OpIds : PutIntent(o) \/ Commit(o) \/ Ack(o) \/ Fail(o) \/ Replay(o))
        \/ (\E s \in Subjects, k \in {"inv", "res", "del"} : Issue(s, k))

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* State of a subject as the shard's marker tables show it: the last APPLIED op wins
\* (Invalidate inserts fact_hidden, Restore deletes it, DeleteDocument inserts a tombstone).
Hidden(s) ==
  LET idx == {i \in DOMAIN dbq : ops[dbq[i]].subj = s} IN
  idx # {} /\ ops[dbq[CHOOSE m \in idx : \A j \in idx : j <= m]].kind # "res"

\* Every acknowledged op has a durable intent.
AckImpliesIntent == \A o \in OpIds : ops[o].ph = "acked" => o \in intents

\* A subject whose last op was acked shows exactly that op's result while the shard serves.
LastAckedShown(s) ==
  LET L == {i \in OnSubj(s) : \A j \in OnSubj(s) : j <= i} IN
  (L # {} /\ ops[CHOOSE i \in L : TRUE].ph = "acked") =>
    (Hidden(s) <=> ops[CHOOSE i \in L : TRUE].kind # "res")

\* Acknowledged deletes survive restore and failover.
AckedDeleteSurvives == sst = "active" => LastAckedShown("y")

\* Invalidate/Restore pairs: replay in name order makes the last state win, so an acked Restore is
\* not undone by an older Invalidate and an acked Invalidate is not undone by an older Restore.
IntentOrderLastWins == sst = "active" => LastAckedShown("x")

\* When reads reopen after a restore, every acked in-window intent is applied.
RestoreReplaysIntents ==
  (sst = "active" /\ nRestore > 0) => \A o \in OpIds : (ops[o].ph = "acked" /\ InWindow(o)) => o \in db

=============================================================================
