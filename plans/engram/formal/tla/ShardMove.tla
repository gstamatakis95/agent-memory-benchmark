---------------------------- MODULE ShardMove ----------------------------
(***************************************************************************)
(* Engram D22 (D5, N123-N125): moving one namespace from Src to Tgt.       *)
(*                                                                         *)
(* Protocol (namespace_moves state `mp`, the catalog arbiter):             *)
(*   Plan -> dirty range copy (partial, concurrent with writes) -> Freeze  *)
(*   (exclusive fence, no holders) -> Reconcile under freeze (re-copy rows *)
(*   with created_at >= Tc - Margin, mutable cell merged) -> verify        *)
(*   (count/hash must match, else Rollback) -> (b') Tgt incoming->ready -> *)
(*   (c) Src frozen->moved_out = POINT OF NO RETURN -> (b'') Tgt           *)
(*   ready->active -> (d) catalog flip.                                    *)
(* Rollback is available at every step before (c).                         *)
(*                                                                         *)
(* Restore / failover of either shard (N123): the shard reverts to its     *)
(* last backup, comes up `restoring` (rejects everything), reconciles the  *)
(* open move against the catalog (before (c): roll back; after (c): Src   *)
(* becomes moved_out, Tgt is re-filled from Src), bumps the catalog epoch  *)
(* when it is the owner, replays intents (Durability.tla), and only then  *)
(* takes its final row.  A failover also bumps the source timeline; the    *)
(* mover compares its session timeline at Freeze and (c).                  *)
(*                                                                         *)
(* Writers hold the shared fence for at most Life ticks; Freeze needs no   *)
(* holder (one attempt).  A write inserts an immutable row and sets one    *)
(* mutable cell (`mut`: id of the last row written).                       *)
(*                                                                         *)
(* Knobs (design: UseReady, ReconcileOnRestore, TimelineCheck,             *)
(* ReconcileVerify all TRUE, Margin >= Life):                              *)
(*   UseReady=FALSE          Tgt goes active before (c); catalog may flip  *)
(*   ReconcileOnRestore=FALSE restore trusts its backup row, bumps epoch,  *)
(*                           ignores the open move                         *)
(*   TimelineCheck=FALSE     Freeze/(c) on a stale session are allowed     *)
(*   ReconcileVerify=FALSE   no count/hash verify (needs Margin < Life)    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS NRows, Clients, Life, Margin, MaxT, MaxEp, MaxRestore, MaxBak,
          UseReady, ReconcileOnRestore, TimelineCheck, ReconcileVerify

Src == "s1"
Tgt == "s2"
Shards == {Src, Tgt}
Rows == 1..NRows
PreC == {"copying", "frozen", "reconciled", "ready", "early", "early_flipped"}

VARIABLES cat, mp, own, fin, store, mut, bak, used, ca, committed, lost,
          wr, cc, now, Tc, mtl, tl, nRestore, nBak,
          frozenSet, frozenMut, actSet, actMut, zcut
vars == <<cat, mp, own, fin, store, mut, bak, used, ca, committed, lost,
          wr, cc, now, Tc, mtl, tl, nRestore, nBak, frozenSet, frozenMut, actSet, actMut, zcut>>

Row(st, ep) == [st |-> st, ep |-> ep]
IdleW == [ph |-> "idle", r |-> 0, sh |-> Src, age |-> 0]
Holders(s) == {c \in Clients : wr[c].ph = "hold" /\ wr[c].sh = s}
Verified == ~ReconcileVerify \/ (store[Src] = store[Tgt] /\ mut[Src] = mut[Tgt])

TypeOK ==
  /\ cat.sh \in Shards /\ cat.ep \in 1..MaxEp
  /\ mp \in {"none", "copying", "frozen", "reconciled", "ready", "moved", "tactive", "done",
             "rolled_back", "early", "early_flipped"}
  /\ \A s \in Shards : store[s] \subseteq Rows /\ mut[s] \in 0..NRows
  /\ now \in 0..MaxT

Init ==
  /\ cat = [sh |-> Src, ep |-> 1] /\ mp = "none"
  /\ own = [s \in Shards |-> IF s = Src THEN Row("active", 1) ELSE Row("none", 0)]
  /\ fin = own
  /\ store = [s \in Shards |-> {}] /\ mut = [s \in Shards |-> 0]
  /\ bak = [s \in Shards |-> [own |-> own[s], store |-> {}, mut |-> 0]]
  /\ used = {} /\ ca = [r \in Rows |-> 0] /\ committed = {} /\ lost = {}
  /\ wr = [c \in Clients |-> IdleW] /\ cc = [c \in Clients |-> cat]
  /\ now = 0 /\ Tc = 0 /\ mtl = 0 /\ tl = 0 /\ nRestore = 0 /\ nBak = 0
  /\ frozenSet = {} /\ frozenMut = 0 /\ actSet = {} /\ actMut = 0 /\ zcut = FALSE

-----------------------------------------------------------------------------
(* Clients and writers *)

Refresh(c) == /\ cc' = [cc EXCEPT ![c] = cat]
              /\ UNCHANGED <<cat, mp, own, fin, store, mut, bak, used, ca, committed, lost, wr, now, Tc,
                             mtl, tl, nRestore, nBak, frozenSet, frozenMut, actSet, actMut, zcut>>

\* A write transaction takes the shared fence and checks `active` at its epoch.
Begin(c, r) ==
  /\ wr[c].ph = "idle" /\ r \notin used
  /\ LET s == cc[c].sh IN own[s] = Row("active", cc[c].ep)
  /\ wr' = [wr EXCEPT ![c] = [ph |-> "hold", r |-> r, sh |-> cc[c].sh, age |-> 0]]
  /\ used' = used \cup {r} /\ ca' = [ca EXCEPT ![r] = now]
  /\ UNCHANGED <<cat, mp, own, fin, store, mut, bak, committed, lost, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

Commit(c) ==
  /\ wr[c].ph = "hold"
  /\ LET s == wr[c].sh IN LET r == wr[c].r IN
     /\ own[s].st = "active"
     /\ store' = [store EXCEPT ![s] = @ \cup {r}]
     /\ mut' = [mut EXCEPT ![s] = r]
  /\ committed' = committed \cup {wr[c].r}
  /\ wr' = [wr EXCEPT ![c] = IdleW]
  /\ UNCHANGED <<cat, mp, own, fin, bak, used, ca, lost, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

Tick ==
  /\ now < MaxT /\ \A c \in Clients : wr[c].ph = "idle" \/ wr[c].age < Life
  /\ now' = now + 1
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" THEN [wr[c] EXCEPT !.age = @ + 1] ELSE wr[c]]
  /\ UNCHANGED <<cat, mp, own, fin, store, mut, bak, used, ca, committed, lost, cc, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

-----------------------------------------------------------------------------
(* The mover *)

Plan ==
  /\ mp = "none" /\ cat.sh = Src /\ own[Src] = Row("active", cat.ep) /\ cat.ep < MaxEp
  /\ own[Tgt].st = "none"
  /\ mp' = "copying" /\ own' = [own EXCEPT ![Tgt] = Row("incoming", cat.ep + 1)]
  /\ Tc' = now /\ mtl' = tl
  /\ UNCHANGED <<cat, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

\* Dirty range copy: any non-empty part of what Src has and Tgt lacks (READ COMMITTED ranges).
CopyRange ==
  /\ mp = "copying" /\ own[Src].st \in {"active", "frozen"} /\ own[Tgt].st = "incoming"
  /\ \E X \in SUBSET (store[Src] \ store[Tgt]) :
       /\ X # {} /\ store' = [store EXCEPT ![Tgt] = @ \cup X]
  /\ UNCHANGED <<cat, mp, own, fin, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

CopyMut ==
  /\ mp = "copying" /\ own[Src].st \in {"active", "frozen"} /\ own[Tgt].st = "incoming" /\ mut' = [mut EXCEPT ![Tgt] = mut[Src]]
  /\ UNCHANGED <<cat, mp, own, fin, store, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

SessionOk == ~TimelineCheck \/ mtl = tl

Freeze ==
  /\ mp = "copying" /\ own[Src].st = "active" /\ Holders(Src) = {} /\ SessionOk
  /\ own' = [own EXCEPT ![Src].st = "frozen"] /\ mp' = "frozen"
  /\ frozenSet' = store[Src] /\ frozenMut' = mut[Src]
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 actSet, actMut>>

\* Under freeze: re-copy immutable rows created since Tc - Margin, merge the mutable cell in full.
Reconcile ==
  /\ mp = "frozen" /\ own[Src].st = "frozen" /\ own[Tgt].st = "incoming"   \* every mover step is fenced on both rows
  /\ store' = [store EXCEPT ![Tgt] = @ \cup {r \in store[Src] : ca[r] + Margin >= Tc}]
  /\ mut' = [mut EXCEPT ![Tgt] = mut[Src]]
  /\ mp' = "reconciled"
  /\ UNCHANGED <<cat, own, fin, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

MakeReady ==
  /\ mp = "reconciled" /\ own[Src].st = "frozen" /\ own[Tgt].st = "incoming" /\ Verified
  /\ IF UseReady
       THEN /\ own' = [own EXCEPT ![Tgt].st = "ready"] /\ mp' = "ready"
            /\ UNCHANGED <<actSet, actMut>>
       ELSE /\ own' = [own EXCEPT ![Tgt].st = "active"] /\ mp' = "early"      \* old order: no ready state
            /\ actSet' = store[Tgt] /\ actMut' = mut[Tgt]
  /\ UNCHANGED <<cat, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, zcut>>

\* (c): the point of no return.
Cut ==
  /\ mp \in {"ready", "early", "early_flipped"} /\ own[Src].st = "frozen" /\ SessionOk
  /\ own' = [own EXCEPT ![Src] = Row("moved_out", own[Tgt].ep)]
  /\ mp' = IF mp = "ready" THEN "moved" ELSE IF mp = "early" THEN "tactive" ELSE "done"
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut>>

Activate ==
  /\ mp = "moved" /\ own[Tgt].st = "ready"
  /\ own' = [own EXCEPT ![Tgt].st = "active"] /\ mp' = "tactive"
  /\ actSet' = store[Tgt] /\ actMut' = mut[Tgt]
  /\ UNCHANGED <<cat, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, zcut>>

CatFlip ==
  /\ mp \in {"tactive", "early"}
  /\ cat' = [sh |-> Tgt, ep |-> own[Tgt].ep]
  /\ mp' = IF mp = "tactive" THEN "done" ELSE "early_flipped"
  /\ UNCHANGED <<own, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

\* Rollback at any step before (c): thaw the source, drop the target, undo a premature flip.
Rollback ==
  /\ mp \in PreC /\ Holders(Tgt) = {}
  /\ mp' = "rolled_back"
  /\ own' = [own EXCEPT ![Src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @,
                        ![Tgt] = Row("none", 0)]
  /\ store' = [store EXCEPT ![Tgt] = {}] /\ mut' = [mut EXCEPT ![Tgt] = 0]
  /\ cat' = IF mp = "early_flipped" THEN [sh |-> Src, ep |-> own[Src].ep] ELSE cat
  /\ UNCHANGED <<fin, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

\* The mover restarts (Temporal) and re-reads the shard's timeline.
Reconnect ==
  /\ mp \in PreC /\ mtl # tl /\ mtl' = tl
  /\ UNCHANGED <<cat, mp, own, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

-----------------------------------------------------------------------------
(* Backups, restore and failover (N123) *)

Backup(s) ==
  /\ nBak < MaxBak /\ own[s].st \notin {"restoring", "replaying"}
  /\ bak' = [bak EXCEPT ![s] = [own |-> own[s], store |-> store[s], mut |-> mut[s]]]
  /\ nBak' = nBak + 1
  /\ UNCHANGED <<cat, mp, own, fin, store, mut, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

\* Restore to the last backup, or failover to a replica that lags (RPO loss): the shard rejects
\* everything until Reconcile and RestoreDone.
Restore(s) ==
  /\ nRestore < MaxRestore /\ own[s].st \notin {"restoring", "replaying"} /\ cat.ep < MaxEp
  /\ lost' = lost \cup (store[s] \ bak[s].store)
  /\ store' = [store EXCEPT ![s] = bak[s].store] /\ mut' = [mut EXCEPT ![s] = bak[s].mut]
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" /\ wr[c].sh = s THEN IdleW ELSE wr[c]]
  /\ own' = [own EXCEPT ![s] = Row("restoring", own[s].ep)]
  /\ fin' = [fin EXCEPT ![s] = bak[s].own]
  /\ tl' = IF s = Src THEN tl + 1 ELSE tl
  /\ nRestore' = nRestore + 1
  /\ UNCHANGED <<cat, mp, bak, used, ca, committed, cc, now, Tc, mtl, nBak, frozenSet, frozenMut, actSet, actMut, zcut>>

ReconcileDesign(s) ==
  /\ own[s].st = "restoring"
  /\ IF mp \in PreC /\ s \in Shards                       \* open move, before (c): roll it back
       THEN /\ mp' = "rolled_back"
            /\ IF s = Src
                 THEN /\ cat' = [sh |-> Src, ep |-> cat.ep + 1]               \* owner restored: epoch bump
                      /\ fin' = [fin EXCEPT ![Src] = Row("active", cat.ep + 1)]
                      /\ own' = [own EXCEPT ![Src].st = "replaying", ![Tgt] = Row("none", 0)]
                 ELSE /\ cat' = IF mp = "early_flipped" THEN [sh |-> Src, ep |-> own[Src].ep] ELSE cat
                      /\ fin' = fin
                      /\ own' = [own EXCEPT ![Tgt] = Row("none", 0),
                                            ![Src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @]
            /\ store' = [store EXCEPT ![Tgt] = {}] /\ mut' = [mut EXCEPT ![Tgt] = 0]
       ELSE IF mp \in {"moved", "tactive"}                 \* after (c): complete the move
       THEN /\ cat' = cat
            /\ IF s = Src
                 THEN /\ fin' = [fin EXCEPT ![Src] = Row("moved_out", cat.ep + 1)]
                      /\ own' = [own EXCEPT ![Src].st = "replaying"]
                      /\ mp' = mp /\ UNCHANGED <<store, mut>>
                 ELSE /\ store' = [store EXCEPT ![Tgt] = store[Src]]           \* Src still holds the data
                      /\ mut' = [mut EXCEPT ![Tgt] = mut[Src]]
                      /\ own' = [own EXCEPT ![Tgt] = Row("ready", cat.ep + 1)]
                      /\ mp' = "moved" /\ fin' = fin
       ELSE IF cat.sh = s                                  \* no open move: owner, new epoch
       THEN /\ cat' = [sh |-> s, ep |-> cat.ep + 1]
            /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
            /\ own' = [own EXCEPT ![s].st = "replaying"] /\ mp' = mp /\ UNCHANGED <<store, mut>>
       ELSE /\ fin' = [fin EXCEPT ![s] = IF s = Src THEN Row("moved_out", cat.ep) ELSE Row("none", 0)]
            /\ own' = [own EXCEPT ![s].st = "replaying"] /\ mp' = mp /\ cat' = cat /\ UNCHANGED <<store, mut>>
  /\ UNCHANGED <<bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

\* Bug variant: trust the backup row, bump the epoch if it says active, ignore the open move.
ReconcileNaive(s) ==
  /\ own[s].st = "restoring"
  /\ IF fin[s].st = "active"
       THEN /\ cat' = [sh |-> cat.sh, ep |-> cat.ep + 1]
            /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
       ELSE /\ cat' = cat /\ fin' = fin
  /\ own' = [own EXCEPT ![s].st = "replaying"]
  /\ UNCHANGED <<mp, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

Reconcile_(s) == IF ReconcileOnRestore THEN ReconcileDesign(s) ELSE ReconcileNaive(s)

RestoreDone(s) ==
  /\ own[s].st = "replaying"
  /\ own' = [own EXCEPT ![s] = fin[s]]
  /\ UNCHANGED <<cat, mp, fin, store, mut, bak, used, ca, committed, lost, wr, cc, now, Tc, mtl, tl, nRestore, nBak,
                 frozenSet, frozenMut, actSet, actMut, zcut>>

-----------------------------------------------------------------------------
Forward == Freeze \/ Reconcile \/ MakeReady \/ Cut \/ Activate \/ CatFlip
Recovery == \E s \in Shards : Reconcile_(s) \/ RestoreDone(s)
StuckRollback == mp = "reconciled" /\ ~Verified /\ Rollback
Clientstep == \E c \in Clients : Commit(c)

Next == Plan \/ CopyRange \/ CopyMut \/ Forward \/ Rollback \/ Reconnect \/ Tick
        \/ (\E c \in Clients : Refresh(c) \/ Commit(c) \/ (\E r \in Rows : Begin(c, r)))
        \/ (\E s \in Shards : Backup(s) \/ Restore(s)) \/ Recovery

Spec == Init /\ [][Next]_vars /\ WF_vars(Forward) /\ WF_vars(Recovery) /\ WF_vars(Clientstep)
        /\ WF_vars(StuckRollback)

-----------------------------------------------------------------------------
(* Invariants *)

\* Never two writable owners; a writer only holds the fence at an active shard
\* (so `ready`, `incoming`, `frozen`, `moved_out`, `restoring` accept nothing).
SingleWriter ==
  /\ Cardinality({s \in Shards : own[s].st = "active"}) <= 1
  /\ \A c \in Clients : wr[c].ph = "hold" => own[wr[c].sh].st = "active"

\* The target is activated with exactly the row set and mutable cell that were frozen at the source.
NoLossNoDup ==
  mp \in {"tactive", "done", "early", "early_flipped"} =>
    /\ actSet = frozenSet /\ actMut = frozenMut

\* Before (c) the source is still the owner and holds every committed row (bar restore RPO loss),
\* so Rollback loses nothing.
RollbackPossibleBeforeC ==
  mp \in PreC =>
    /\ own[Src].st \in {"active", "frozen", "restoring", "replaying"}
    /\ (committed \ lost) \subseteq store[Src]

\* `incoming` and `ready` accept nothing: the target is never writable before (c).
NoWriteToTargetBeforeC == mp \in PreC => own[Tgt].st # "active"

NoRouteToTargetBeforeC == cat.sh = Tgt => mp \in {"done", "tactive", "moved"}

\* A stale-session mover never executed Freeze or (c).
ZombieCannotCutOver == ~zcut

\* A restored shard never ends active while another shard holds the namespace at a >= epoch.
RestoreReconciles ==
  \A s \in Shards : own[s].st = "active" =>
    /\ own[s].ep >= cat.ep
    /\ \A t \in Shards \ {s} :
         /\ own[t].st \notin {"active", "ready"}
         /\ own[t].st = "moved_out" => own[t].ep <= own[s].ep

\* Liveness: a started move completes or rolls back.
Symm == Permutations(Clients)

MoveTerminates == (mp # "none") ~> (mp \in {"done", "rolled_back"})

=============================================================================
