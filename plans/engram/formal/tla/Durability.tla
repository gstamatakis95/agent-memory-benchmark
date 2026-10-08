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
(* A duplicate document delete (concurrent, or a retry after a crash       *)
(* between commit and put) finds the subject already deleting: it writes no*)
(* marker and puts the committed marker's own intent (put-if-absent), so   *)
(* an ack always implies an intent for the marker the client relies on.    *)
(* Invalidate/Restore always write their own marker row and intent, even   *)
(* when the fact is already in the requested state (a pure no-op success   *)
(* would let a replayed older intent override the acknowledged request).   *)
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
(* Replay also skips an intent whose recorded epoch is older than an      *)
(* applied entry of its subject (the restore bumps the epoch, so a write   *)
(* after the reopen outranks any intent of the lost epoch).               *)
(* Knobs (design: all TRUE, FloorMode min, OrderMode chain):              *)
(*   IntentAfterCommit FALSE  the intent is put before the marker and has  *)
(*                     no recorded effect: replay recomputes it (old N122; *)
(*                     orphan intents, C-5)                                *)
(*   AckNeedsIntent    FALSE  the ack may precede the intent               *)
(*   AckRecheck        FALSE  no re-read of the marker before the ack      *)
(*   DupReput          FALSE  a duplicate that found the marker puts nothing *)
(*   ReplayFirst       FALSE  reads reopen before replay finishes          *)
(*   FloorMode         "min" lower only (N134); "raise" Reopen resets the  *)
(*                     floor (C-4); "target" each restore sets its target  *)
(*   EpochGuard        FALSE  replay ignores the namespace epoch recorded in the *)
(*                     intent: an older intent put after a later write replays  *)
(*                     over it                                              *)
(*   OrderMode         "chain" prev_operation_id; "clock" (deleted_at,id); *)
(*                     "any"                                               *)
(* A Margin below Lat + Skew loses commits outside the replay window.      *)
(*                                                                         *)
(* D24 additions (N146, N150):                                             *)
(*   BlobFloor        FALSE  replay reads the catalog floor only; TRUE     *)
(*                     (design) it reads min(fl, blobFloor): a record in   *)
(*                     the blob store, written before each replay and      *)
(*                     lowered only (N146).  CatalogRestore (up to         *)
(*                     MaxCatLoss times) reverts the catalog's fl to an    *)
(*                     earlier, higher value.                              *)
(*   ReplayCurrentEpoch TRUE  a replayed entry is stamped with the bumped  *)
(*                     epoch instead of the intent's recorded one (the     *)
(*                     guard then skips the second intent of a subject)    *)
(*   ConcurrentX      TRUE   requests on one fact subject may overlap: the *)
(*                     marker transaction is TxnBegin (reads the subject's *)
(*                     previous entry = prev_operation_id) and Commit.     *)
(*   HelpPrev         TRUE   (design, with ConcurrentX) TxnBegin puts the   *)
(*                     intent of the subject's latest entry if missing: a  *)
(*                     marker whose writer crashed before the put would    *)
(*                     otherwise break the chain and its successors replay *)
(*                     before older intents.                               *)
(*   SubjectLock      TRUE   (design) TxnBegin waits while another         *)
(*                     transaction of the subject is open or has not yet   *)
(*                     put its intent (advisory lock on the fact, held     *)
(*                     through the intent put); FALSE: both read the same  *)
(*                     prev, the chain forks and replay order is           *)
(*                     unspecified.                                        *)
(*                                                                         *)
(* D27 addition (N182): the tenant-level delete.  DeleteTenant writes the  *)
(* catalog `deleting` row and a tenant intent (tdp = 1), TenantDelete then *)
(* takes freeze_delete on each of the tenant's two namespaces (tdp = 2, 3),*)
(* and the operation is acknowledged (ta) only once both are fenced.  A    *)
(* catalog restore (the existing CatalogRestore, RPO 60 s) reverts the     *)
(* `deleting` row when no namespace is fenced yet (tdp = 4: the operation  *)
(* is FAILED{CATALOG_RESTORED}); once a namespace is frozen/delete the     *)
(* reconcile re-derives `deleting` from the shard rows (N163(2)).          *)
(*   TenantAckBeforeFence TRUE  the ack follows the `deleting` row and the *)
(*                     intent, before the fences (the rejected order)      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS Shape, MaxT, Lat, Skew, Margin, MaxRestores,
          IntentAfterCommit, AckNeedsIntent, AckRecheck, DupReput, ReplayFirst,
          FloorMode, OrderMode, EpochGuard,
          BlobFloor, MaxCatLoss, ReplayCurrentEpoch, ConcurrentX, SubjectLock, HelpPrev, TenantAckBeforeFence

ShapeDoc  == <<[s |-> "y", k |-> "del"], [s |-> "y", k |-> "del"], [s |-> "y", k |-> "ret"]>>
ShapeFact == <<[s |-> "x", k |-> "inv"], [s |-> "x", k |-> "res"], [s |-> "x", k |-> "inv"]>>

N == Len(Shape)
OpIds == 1..N
S(i) == Shape[i].s
K(i) == Shape[i].k

VARIABLES t, ops, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta
vars == <<t, ops, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

\* dbq: the marker rows the shard has applied, in application order: [op, eff, ep] (ep: the epoch the row records).
\* eff: for a document delete, the versions it covers (the tombstone's up_to_version).
db == {dbq[i].op : i \in DOMAIN dbq}
Entries == {dbq[i] : i \in DOMAIN dbq}

NoOp == [ph |-> "none", ep |-> 0, it |-> 0, at |-> 0, ct |-> 0, cn0 |-> 0, eff |-> {}, prev |-> 0, obs |-> 0, rd |-> -1, be |-> 0]
Phases == {"none", "new", "committed", "acked", "failed"}

TypeOK ==
  /\ t \in 0..MaxT /\ intents \subseteq OpIds /\ db \subseteq OpIds /\ Len(dbq) <= 2 * N
  /\ sst \in {"active", "restoring"} /\ fl \in 0..(MaxT + 1) /\ bf \in 0..(MaxT + 1)
  /\ \A i \in OpIds : ops[i].ph \in Phases
  /\ tdp \in 0..4 /\ ta \in BOOLEAN

Init ==
  /\ t = 0 /\ ops = [i \in OpIds |-> NoOp] /\ intents = {} /\ dbq = << >>
  /\ sst = "active" /\ fl = MaxT + 1 /\ nIssued = 0 /\ nRestore = 0 /\ cn = 0 /\ ep = 1 /\ bf = MaxT + 1 /\ nCat = 0 /\ tdp = 0 /\ ta = FALSE

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
\* The subject lock is held from TxnBegin through the put of the transaction's own intent (a session-level advisory
\* lock, not a transaction-level one): a successor must not commit, and so must not put its intent, while the
\* predecessor's intent is still missing -- replay would apply the successor first (found by Durability_Chain).
LockHeld(s) == \E j \in OpIds : S(j) = s /\ ((ops[j].ph = "new" /\ ops[j].rd >= 0)
                                         \/ (ops[j].ph = "committed" /\ ops[j].obs = j /\ j \notin intents))

Issue ==
  /\ nIssued < N /\ sst = "active"
  /\ LET i == nIssued + 1 IN
     /\ (S(i) = "x" /\ ~ConcurrentX => \A j \in Issued : ~Live(j))   \* x: one request at a time unless ConcurrentX
     /\ \E sk \in 0..Skew :
          /\ sk <= t
          /\ ops' = [ops EXCEPT ![i] = [NoOp EXCEPT !.ph = "new", !.it = t, !.at = t - sk]]
  /\ nIssued' = nIssued + 1
  /\ UNCHANGED <<t, intents, dbq, sst, fl, nRestore, cn, ep, bf, nCat, tdp, ta>>

\* ConcurrentX: the marker transaction of a fact subject begins by reading the subject's previous entry
\* (prev_operation_id).  With the subject lock (N150) it waits while another transaction of the subject is open.
TxnBegin(o) ==
  /\ ConcurrentX /\ S(o) = "x" /\ ops[o].ph = "new" /\ ops[o].rd < 0 /\ sst = "active"
  /\ (SubjectLock => ~LockHeld("x"))
  /\ ops' = [ops EXCEPT ![o].rd = LastOn("x"), ![o].be = ep]
  \* HelpPrev: under the lock the transaction first puts the intent of the subject's latest entry if it is missing
  \* (a marker whose writer crashed between commit and put); otherwise that entry is a hole in the chain.
  /\ intents' = IF HelpPrev /\ LastOn("x") # 0 THEN intents \cup {LastOn("x")} ELSE intents
  /\ UNCHANGED <<t, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

\* The marker transaction: a local commit, refused by the namespace fence while restoring.
\* A duplicate (subject already deleting / hidden) writes no marker and observes the existing one.
Commit(o) ==
  /\ ops[o].ph = "new" /\ sst = "active" /\ t - ops[o].it <= Lat
  /\ (IntentAfterCommit \/ o \in intents)                  \* old order: intent first
  /\ (ConcurrentX /\ S(o) = "x" => ops[o].rd >= 0 /\ ops[o].be = ep)   \* the transaction began in this epoch
  /\ LET k == K(o) IN LET s == S(o) IN
     LET isDup == k = "del" /\ Vis = {} IN
     /\ (k = "res" => HiddenX)
     /\ IF isDup
          THEN /\ ops' = [ops EXCEPT ![o] = [@ EXCEPT !.ph = "committed", !.ct = t, !.obs = LastOn(s)]]
               /\ UNCHANGED <<dbq, cn, bf, nCat>>
          ELSE LET eff == IF k = "del" THEN AllRet ELSE {} IN
               /\ dbq' = Append(dbq, [op |-> o, eff |-> eff, ep |-> ep])
               /\ cn' = cn + 1
               /\ ops' = [ops EXCEPT ![o] = [@ EXCEPT !.ph = "committed", !.ct = t, !.cn0 = cn + 1, !.ep = ep,
                            !.eff = eff, !.obs = o,
                            !.prev = IF k = "ret" THEN 0 ELSE IF ConcurrentX /\ s = "x" THEN ops[o].rd ELSE LastOn(s)]]
  /\ UNCHANGED <<t, intents, sst, fl, nIssued, nRestore, ep, bf, nCat, tdp, ta>>

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
  /\ UNCHANGED <<t, ops, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

IntentOK(o) == ops[o].obs \in intents \/ ~AckNeedsIntent \/ (ops[o].obs # o /\ ~DupReput)

Ack(o) ==
  /\ ops[o].ph = "committed"
  /\ K(o) = "ret" \/ (IntentOK(o) /\ (AckRecheck => ops[o].obs \in db))
  /\ ops' = [ops EXCEPT ![o].ph = "acked"]
  /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

\* The client sees an error: before the commit (Abort), or after it with the intent not yet put
\* (CrashBeforePut), or with the intent put (LostAck).
Abort(o) == /\ ops[o].ph = "new" /\ K(o) # "ret"
            /\ ops' = [ops EXCEPT ![o].ph = "failed"]
            /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>
CrashBeforePut(o) == /\ ops[o].ph = "committed" /\ K(o) # "ret" /\ ops[o].obs \notin intents
                     /\ ops' = [ops EXCEPT ![o].ph = "failed"]
                     /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>
LostAck(o) == /\ ops[o].ph = "committed" /\ K(o) # "ret" /\ ops[o].obs \in intents
              /\ ops' = [ops EXCEPT ![o].ph = "failed"]
              /\ UNCHANGED <<t, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

\* One tick advances the tenant delete by one step (the model fixes the pace so that the tenant machine does not multiply the
\* state space): tdp 0 -> 1 the catalog `deleting` row and the tenant intent, 1 -> 2 -> 3 `freeze_delete` on each of the two
\* namespaces.  The operation is acknowledged (ta) after the last fence, or, under TenantAckBeforeFence, after the intent.
TenantStep == IF tdp \in 0..2 THEN tdp + 1 ELSE tdp
Tick ==
  /\ t < MaxT /\ t' = t + 1
  /\ tdp' = TenantStep
  /\ ta' = (ta \/ (IF TenantAckBeforeFence THEN TenantStep = 1 ELSE TenantStep = 3))
  /\ UNCHANGED <<ops, intents, dbq, sst, fl, nIssued, nRestore, cn, ep, bf, nCat>>

\* Restore or failover to point p: commits after p are lost; the floor (catalog) is not.
RestoreTo(p) ==
  /\ nRestore < MaxRestores /\ p <= t
  /\ dbq' = SelectSeq(dbq, LAMBDA e : ops[e.op].ct <= p)
  /\ sst' = "restoring" /\ nRestore' = nRestore + 1
  /\ fl' = IF FloorMode = "target" THEN p ELSE IF fl < p THEN fl ELSE p
  /\ bf' = IF bf < p THEN bf ELSE p                      \* the blob-store floor record, written before the replay (N146)
  /\ ep' = ep + 1
  /\ UNCHANGED <<t, ops, intents, nIssued, cn, nCat, tdp, ta>>

\* CatalogRestore: the catalog is restored from a backup (or fails over asynchronously) and its floor goes back to
\* its initial (highest) value; the blob-store record is not affected.
CatalogRestore ==
  /\ nCat < MaxCatLoss /\ (fl < MaxT + 1 \/ tdp = 1)
  /\ fl' = MaxT + 1              \* the oldest value is the worst case: the replay window only narrows as the floor rises
  /\ nCat' = nCat + 1
  /\ tdp' = IF tdp = 1 THEN 4 ELSE tdp          \* before any fence the `deleting` row is gone; after one the reconcile re-derives it
  /\ UNCHANGED <<t, ops, intents, dbq, sst, nIssued, nRestore, cn, ep, bf, ta>>

RFloor == IF BlobFloor THEN (IF fl < bf THEN fl ELSE bf) ELSE fl
InWindow(o) == o \in intents /\ ops[o].at + Margin >= RFloor
\* The epoch guard: an intent whose recorded epoch is older than that of an applied entry of its subject is
\* skipped (it counts as settled; replay never applies it).
Skipped(o) == EpochGuard /\ \E e \in Entries : (S(e.op) = S(o) /\ K(e.op) # "ret") /\ e.ep > ops[o].ep
Pending == {o \in intents : InWindow(o) /\ o \notin db /\ ~Skipped(o)}

Before(o, o2) == ops[o].at < ops[o2].at \/ (ops[o].at = ops[o2].at /\ o <= o2)

\* Admin variant of the marker transaction: same effect, fence bypassed.
Replay(o) ==
  /\ sst = "restoring" /\ o \in Pending
  /\ CASE OrderMode = "chain" -> ops[o].prev = 0 \/ ops[o].prev \notin Pending
       [] OrderMode = "clock" -> \A o2 \in Pending : S(o2) = S(o) => Before(o, o2)
       [] OTHER -> TRUE
  /\ LET eff == IF K(o) = "del" /\ ~IntentAfterCommit THEN AllRet ELSE ops[o].eff IN
     dbq' = Append(dbq, [op |-> o, eff |-> eff, ep |-> IF ReplayCurrentEpoch THEN ep ELSE ops[o].ep])
  /\ ops' = [ops EXCEPT ![o].ct = t]
  /\ UNCHANGED <<t, intents, sst, fl, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

Reopen ==
  /\ sst = "restoring" /\ (ReplayFirst => Pending = {})
  /\ sst' = "active"
  /\ fl' = IF FloorMode = "raise" THEN MaxT + 1 ELSE fl
  /\ UNCHANGED <<t, ops, intents, dbq, nIssued, nRestore, cn, ep, bf, nCat, tdp, ta>>

Next == Tick \/ Reopen \/ Issue \/ (\E p \in 0..MaxT : RestoreTo(p)) \/ CatalogRestore
        \/ (\E o \in OpIds : TxnBegin(o) \/ PutIntent(o) \/ Commit(o) \/ Ack(o) \/ Abort(o) \/ CrashBeforePut(o)
                             \/ LostAck(o) \/ Replay(o))

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* The operation of subject s that committed last (by commit order, whatever the issue order of overlapping requests).
LastCommitted(s) ==
  LET L == {i \in OpIds : S(i) = s /\ ops[i].cn0 > 0} IN IF L = {} THEN 0 ELSE CHOOSE m \in L : \A j \in L : ops[j].cn0 <= ops[m].cn0

\* Every acknowledged delete or invalidation has a durable intent.
AckImpliesIntent == \A o \in OpIds : (ops[o].ph = "acked" /\ K(o) # "ret") => ops[o].obs \in intents

\* The marker an acknowledged delete relies on is in force whenever the shard serves.
AckedDeleteSurvives ==
  /\ sst = "active" => \A o \in OpIds : (ops[o].ph = "acked" /\ K(o) = "del") => ops[o].obs \in db
  /\ ta => tdp # 4                                       \* an acknowledged tenant delete is never reverted (N182)

\* Invalidate/Restore: per-subject chain order makes the last state win, whatever the clocks say.
IntentOrderLastWins ==
  sst = "active" =>
    LET l == LastCommitted("x") IN
    (l # 0 /\ ops[l].ph = "acked") => (HiddenX <=> K(l) = "inv")

\* An effect that reaches a later acknowledged write is not allowed: every applied marker was
\* written by a committed operation, and covers only versions committed before it.
NoUnackedEffectOnLaterAck ==
  \A e \in Entries : K(e.op) # "ret" =>
    /\ ops[e.op].cn0 > 0
    /\ \A r \in e.eff \ {0} : ops[r].cn0 < ops[e.op].cn0

=============================================================================
