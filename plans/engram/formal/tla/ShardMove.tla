---------------------------- MODULE ShardMove ----------------------------
(***************************************************************************)
(* Engram D23 (D5, N123-N125, N137): moving one namespace from Src to Tgt. *)
(*                                                                         *)
(* Protocol.  The catalog row of the move, cm, is the arbiter:             *)
(*   open --(a'') CAS--> committed --> done        (point of no return)    *)
(*   open --abort CAS--> rolled_back               (mover or restore)      *)
(* Mover (mp): Plan -> dirty copy -> EndCopy (pre-freeze verification and  *)
(* catch-up) -> Freeze (exclusive fence) -> Reconcile under the freeze ->  *)
(* (b') Tgt incoming->ready -> (a'') CAS open->committed -> (c) Src        *)
(* frozen->moved_out (only after the mover read `committed`) -> (b'')      *)
(* Tgt ready->active -> (d) catalog flip -> Cleanup (after 24 h).          *)
(*                                                                         *)
(* Three table classes (N137).  A write transaction inserts, at Begin,     *)
(*   an insert-only row r (ledger: keyed for the re-copy by ins_seq, which *)
(*     is drawn at Begin; `old` rows carry an OLD entity id / created_at,  *)
(*     e.g. a stamp for an old fact),                                      *)
(*   a mutable key r (idempotency key: swept by the source at any time;    *)
(*     merge-diffed under the freeze),                                     *)
(*   an event of the expiring class (token usage: its sweep is paused      *)
(*     while a move is open; copied once, verified `count <=`).            *)
(*                                                                         *)
(* Restore / failover of either shard (N123): the shard reverts to its     *)
(* last backup (or, lossless failover, keeps its state), comes up          *)
(* `restoring`, reconciles the open move against the catalog by CAS        *)
(* (open: roll back; committed: complete (c), (b''), (d) itself), bumps    *)
(* the catalog epoch only when it owns the namespace with no committed     *)
(* move, replays intents (Durability.tla) and takes its final row.         *)
(*                                                                         *)
(* Writers hold the shared fence for at most Life ticks; Freeze needs no   *)
(* holder (one attempt).                                                   *)
(*                                                                         *)
(* Knobs (design: all TRUE except StampAfterCut/IdKeyed/AllowAbort):       *)
(*   UseReady=FALSE          Tgt goes active before (c); catalog may flip  *)
(*   ReconcileOnRestore=FALSE restore trusts its backup row, bumps epoch   *)
(*   TimelineCheck=FALSE     Freeze/(a'')/(c) on a stale session allowed   *)
(*   ReconcileVerify=FALSE   no count/hash verify (needs Margin < Life)    *)
(*   FencedSteps=FALSE       mover steps do not check the rows they saw    *)
(*   IdKeyed=TRUE            re-copy by created_at / entity id, not ins_seq*)
(*   MergeDeletes=FALSE      the mutable merge only upserts                *)
(*   SweepPause=FALSE        the expiring-class sweep runs during a move   *)
(*   StampAfterCut=TRUE      (c) first, the catalog stamp afterwards       *)
(*   CleanupNeedsBackup=FALSE cleanup without a post-activation backup     *)
(*   AllowAbort              the mover may roll back at any pre-commit step*)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS NRows, Clients, Life, Margin, MaxT, MaxEp, MaxRestore, MaxBak, AllowAbort,
          UseReady, ReconcileOnRestore, TimelineCheck, ReconcileVerify, FencedSteps,
          IdKeyed, MergeDeletes, SweepPause, StampAfterCut, CleanupNeedsBackup

Src == "s1"
Tgt == "s2"
Shards == {Src, Tgt}
Rows == 1..NRows
PreC == {"copying", "copied", "frozen", "reconciled", "ready", "early", "early_flipped"}
Final == {"none", "rolled_back", "done"}

VARIABLES cat, cm, mp, own, fin, store, mk, ex, bak, used, sqt, ky, committed, lost,
          wr, cc, now, Tc, mtl, tl, nRestore, nBak,
          frozenSet, frozenMk, actSet, actMk, zcut, cleaned
CatV  == <<cat, cm>>
MovV  == <<mp, Tc, mtl>>
OwnV  == <<own, fin>>
DatV  == <<store, mk, ex>>
HistV == <<used, sqt, ky, committed, lost>>
CliV  == <<wr, cc>>
EnvV  == <<now, tl, nRestore, nBak, bak>>
FzV   == <<frozenSet, frozenMk, actSet, actMk, zcut, cleaned>>
vars == <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV>>

Row(st, ep) == [st |-> st, ep |-> ep]
IdleW == [ph |-> "idle", r |-> 0, sh |-> Src, age |-> 0, old |-> FALSE]
Holders(s) == {c \in Clients : wr[c].ph = "hold" /\ wr[c].sh = s}
\* Every mover step is a compare-and-set on the rows it observed (source, target, catalog move).
Fenced == cm = "open" /\ (~FencedSteps \/ (own[Src].st \in {"active", "frozen"} /\ own[Tgt].st = "incoming"))
SessionOk == ~TimelineCheck \/ mtl = tl

\* The re-copy key: ins_seq (drawn at Begin) in the design, created_at / entity id in the old design.
KeyOf(r) == IF IdKeyed THEN ky[r] ELSE sqt[r]
Verified == ~ReconcileVerify \/ (store[Src] = store[Tgt] /\ mk[Src] = mk[Tgt] /\ ex[Tgt] <= ex[Src])

TypeOK ==
  /\ cat.sh \in Shards /\ cat.ep \in 1..MaxEp
  /\ cm \in {"none", "open", "committed", "rolled_back", "done"}
  /\ mp \in {"none", "copying", "copied", "frozen", "reconciled", "ready", "committed", "cut", "tactive",
             "done", "rolled_back", "early", "early_flipped"}
  /\ \A s \in Shards : store[s] \subseteq Rows /\ mk[s] \subseteq Rows /\ ex[s] \in 0..NRows
  /\ now \in 0..MaxT

Init ==
  /\ cat = [sh |-> Src, ep |-> 1] /\ cm = "none" /\ mp = "none"
  /\ own = [s \in Shards |-> IF s = Src THEN Row("active", 1) ELSE Row("none", 0)]
  /\ fin = own
  /\ store = [s \in Shards |-> {}] /\ mk = [s \in Shards |-> {}] /\ ex = [s \in Shards |-> 0]
  /\ bak = [s \in Shards |-> [own |-> own[s], store |-> {}, mk |-> {}, ex |-> 0]]
  /\ used = {} /\ sqt = [r \in Rows |-> 0] /\ ky = [r \in Rows |-> 0] /\ committed = {} /\ lost = {}
  /\ wr = [c \in Clients |-> IdleW] /\ cc = [c \in Clients |-> cat]
  /\ now = 0 /\ Tc = 0 /\ mtl = 0 /\ tl = 0 /\ nRestore = 0 /\ nBak = 0
  /\ frozenSet = {} /\ frozenMk = {} /\ actSet = {} /\ actMk = {} /\ zcut = FALSE /\ cleaned = FALSE

-----------------------------------------------------------------------------
(* Clients, writers and the source's own sweeps *)

Refresh(c) == /\ cc' = [cc EXCEPT ![c] = cat]
              /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, wr, EnvV, FzV>>

\* A write transaction takes the shared fence and checks `active` at its epoch.  Its insert-only
\* row has ins_seq = now; it may carry an old entity id (a stamp for an old fact).
Begin(c, r) ==
  /\ wr[c].ph = "idle" /\ r \notin used
  /\ LET s == cc[c].sh IN own[s] = Row("active", cc[c].ep)
  /\ \E old \in BOOLEAN :
       /\ wr' = [wr EXCEPT ![c] = [ph |-> "hold", r |-> r, sh |-> cc[c].sh, age |-> 0, old |-> old]]
       /\ ky' = [ky EXCEPT ![r] = IF old THEN 0 ELSE now]
  /\ used' = used \cup {r} /\ sqt' = [sqt EXCEPT ![r] = now]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, committed, lost, cc, EnvV, FzV>>

Commit(c) ==
  /\ wr[c].ph = "hold"
  /\ LET s == wr[c].sh IN LET r == wr[c].r IN
     /\ own[s].st = "active"
     /\ store' = [store EXCEPT ![s] = @ \cup {r}]
     /\ mk' = [mk EXCEPT ![s] = @ \cup {r}]
     /\ ex' = [ex EXCEPT ![s] = IF @ < NRows THEN @ + 1 ELSE @]
  /\ committed' = committed \cup {wr[c].r}
  /\ wr' = [wr EXCEPT ![c] = IdleW]
  /\ UNCHANGED <<CatV, MovV, OwnV, used, sqt, ky, lost, cc, EnvV, FzV>>

\* Source-side deletes outside the expunge: the 24 h sweep of idempotency keys (mutable class), and the
\* retention sweep of the expiring class, which joins purgeable_namespaces and so pauses during a move.
SrcDelete(r) ==
  /\ own[Src].st = "active" /\ r \in mk[Src]
  /\ mk' = [mk EXCEPT ![Src] = @ \ {r}]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, ex, HistV, CliV, EnvV, FzV>>

SweepEx ==
  /\ own[Src].st = "active" /\ ex[Src] > 0 /\ (~SweepPause \/ cm \notin {"open", "committed"})
  /\ ex' = [ex EXCEPT ![Src] = @ - 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV>>

Tick ==
  /\ now < MaxT /\ \A c \in Clients : wr[c].ph = "idle" \/ wr[c].age < Life
  /\ now' = now + 1
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" THEN [wr[c] EXCEPT !.age = @ + 1] ELSE wr[c]]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, cc, tl, nRestore, nBak, bak, FzV>>

-----------------------------------------------------------------------------
(* The mover *)

Plan ==
  /\ mp = "none" /\ cat.sh = Src /\ own[Src] = Row("active", cat.ep) /\ cat.ep < MaxEp
  /\ own[Tgt].st = "none"
  /\ mp' = "copying" /\ cm' = "open" /\ own' = [own EXCEPT ![Tgt] = Row("incoming", cat.ep + 1)]
  /\ Tc' = now /\ mtl' = tl
  /\ UNCHANGED <<cat, fin, DatV, HistV, CliV, EnvV, FzV>>

\* Dirty range copy: any non-empty part of what Src has and Tgt lacks (READ COMMITTED ranges), the
\* mutable class (upserts only) and the expiring class (once).
CopyRange ==
  /\ mp = "copying" /\ Fenced
  /\ \E X \in SUBSET (store[Src] \ store[Tgt]) :
       /\ X # {} /\ store' = [store EXCEPT ![Tgt] = @ \cup X]
  /\ UNCHANGED <<CatV, MovV, OwnV, mk, ex, HistV, CliV, EnvV, FzV>>

CopyMk ==
  /\ mp = "copying" /\ Fenced /\ mk' = [mk EXCEPT ![Tgt] = @ \cup mk[Src]]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, ex, HistV, CliV, EnvV, FzV>>

CopyEx ==
  /\ mp = "copying" /\ Fenced /\ ex' = [ex EXCEPT ![Tgt] = ex[Src]]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV>>

\* Pre-freeze verification (namespace still active): every insert-only row older than the sampled floor
\* is on the target; T_pre = now bounds the re-copy that follows.
EndCopy ==
  /\ mp = "copying" /\ Fenced
  /\ \A r \in store[Src] : KeyOf(r) + Margin < now => r \in store[Tgt]
  /\ mp' = "copied" /\ Tc' = now
  /\ UNCHANGED <<CatV, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV>>

Freeze ==
  /\ mp = "copied" /\ Fenced /\ own[Src].st = "active" /\ Holders(Src) = {} /\ SessionOk
  /\ own' = [own EXCEPT ![Src].st = "frozen"] /\ mp' = "frozen"
  /\ frozenSet' = store[Src] /\ frozenMk' = mk[Src]
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<CatV, Tc, mtl, fin, DatV, HistV, CliV, EnvV, actSet, actMk, cleaned>>

\* Under freeze: re-copy insert-only rows with key >= T_pre - Margin; merge-diff the mutable class.
Reconcile ==
  /\ mp = "frozen" /\ Fenced /\ (own[Src].st = "frozen" \/ ~FencedSteps)
  /\ store' = [store EXCEPT ![Tgt] = @ \cup {r \in store[Src] : KeyOf(r) + Margin >= Tc}]
  /\ mk' = [mk EXCEPT ![Tgt] = IF MergeDeletes THEN mk[Src] ELSE @ \cup mk[Src]]
  /\ mp' = "reconciled"
  /\ UNCHANGED <<CatV, Tc, mtl, OwnV, ex, HistV, CliV, EnvV, FzV>>

MakeReady ==                                                 \* (b')
  /\ mp = "reconciled" /\ Fenced /\ (own[Src].st = "frozen" \/ ~FencedSteps) /\ Verified
  /\ IF UseReady
       THEN /\ own' = [own EXCEPT ![Tgt].st = "ready"] /\ mp' = "ready"
            /\ UNCHANGED <<actSet, actMk>>
       ELSE /\ own' = [own EXCEPT ![Tgt].st = "active"] /\ mp' = "early"      \* old order: no ready state
            /\ actSet' = store[Tgt] /\ actMk' = mk[Tgt]
  /\ UNCHANGED <<CatV, Tc, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, zcut, cleaned>>

\* (a''): the catalog CAS, the point of no return; it also compares the session's timeline.
CommitCAS ==
  /\ mp = "ready" /\ ~StampAfterCut /\ cm = "open" /\ SessionOk
  /\ (~FencedSteps \/ (own[Src].st = "frozen" /\ own[Tgt].st = "ready"))   \* the rows the mover verified
  /\ cm' = "committed" /\ mp' = "committed"
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, Tc, mtl, OwnV, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, actSet, actMk, cleaned>>

\* (c): Src frozen -> moved_out, only after the mover read `committed` (design).
Cut ==
  /\ own[Src].st = "frozen" /\ SessionOk
  /\ \/ mp = "committed" /\ cm = "committed"
     \/ mp \in {"early", "early_flipped"}
     \/ StampAfterCut /\ mp = "ready"
  /\ own' = [own EXCEPT ![Src] = Row("moved_out", own[Tgt].ep)]
  /\ mp' = CASE mp = "committed" -> "cut" [] mp = "ready" -> "cut" [] mp = "early" -> "tactive" [] OTHER -> "done"
  /\ cm' = IF mp = "early" THEN "committed" ELSE IF mp = "early_flipped" THEN "done" ELSE cm
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, Tc, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, actSet, actMk, cleaned>>

\* Old-order bookkeeping: the stamp that records (c) in the catalog after the fact.
Stamp ==
  /\ StampAfterCut /\ mp \in {"cut", "tactive"} /\ cm = "open"
  /\ cm' = "committed"
  /\ UNCHANGED <<cat, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV>>

Activate ==                                                  \* (b'')
  /\ mp = "cut" /\ own[Tgt].st = "ready"
  /\ own' = [own EXCEPT ![Tgt].st = "active"] /\ mp' = "tactive"
  /\ actSet' = store[Tgt] /\ actMk' = mk[Tgt]
  /\ UNCHANGED <<CatV, Tc, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, zcut, cleaned>>

\* (d): catalog flip WHERE epoch = e AND state = frozen; early order flips before (c).
CatFlip ==
  /\ \/ mp = "tactive" /\ cat = [sh |-> Src, ep |-> own[Tgt].ep - 1]
     \/ mp = "early"
  /\ cat' = [sh |-> Tgt, ep |-> own[Tgt].ep]
  /\ mp' = IF mp = "tactive" THEN "done" ELSE "early_flipped"
  /\ cm' = IF mp = "tactive" THEN "done" ELSE cm
  /\ UNCHANGED <<Tc, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV>>

\* Cleanup (24 h after done): the source's data rows go; the moved_out ownership row stays.
Cleanup ==
  /\ mp = "done" /\ ~cleaned /\ own[Tgt].st = "active" /\ own[Src].st = "moved_out"
  /\ (~CleanupNeedsBackup \/ (bak[Tgt].own = own[Tgt] /\ frozenSet \subseteq bak[Tgt].store))
  /\ store' = [store EXCEPT ![Src] = {}] /\ mk' = [mk EXCEPT ![Src] = {}] /\ ex' = [ex EXCEPT ![Src] = 0]
  /\ cleaned' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, frozenSet, frozenMk, actSet, actMk, zcut>>

\* Rollback at any step before the point of no return: abort CAS open -> rolled_back, thaw the source,
\* drop the target, undo a premature flip.
RollbackA ==
  /\ mp \in PreC /\ cm = "open" /\ Holders(Tgt) = {}
  /\ mp' = "rolled_back" /\ cm' = "rolled_back"
  /\ own' = [own EXCEPT ![Src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @,
                        ![Tgt] = Row("none", 0)]
  /\ store' = [store EXCEPT ![Tgt] = {}] /\ mk' = [mk EXCEPT ![Tgt] = {}] /\ ex' = [ex EXCEPT ![Tgt] = 0]
  /\ cat' = IF mp = "early_flipped" THEN [sh |-> Src, ep |-> own[Src].ep] ELSE cat
  /\ UNCHANGED <<Tc, mtl, fin, HistV, CliV, EnvV, FzV>>
AbortAny == AllowAbort /\ RollbackA
StuckRollback == mp = "reconciled" /\ ~Verified /\ RollbackA
Rollback == AbortAny \/ StuckRollback

\* The mover restarts (Temporal) and re-reads the shard's timeline.
Reconnect ==
  /\ mp \in PreC /\ mtl # tl /\ mtl' = tl
  /\ UNCHANGED <<CatV, mp, Tc, OwnV, DatV, HistV, CliV, now, tl, nRestore, nBak, bak, FzV>>

-----------------------------------------------------------------------------
(* Backups, restore and failover (N123) *)

Backup(s) ==
  /\ nBak < MaxBak /\ own[s].st \notin {"restoring", "replaying"}
  /\ bak' = [bak EXCEPT ![s] = [own |-> own[s], store |-> store[s], mk |-> mk[s], ex |-> ex[s]]]
  /\ nBak' = nBak + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, now, tl, nRestore, FzV>>

\* Restore to the last backup, or failover to a replica (which may or may not lag): the shard rejects
\* everything until Reconcile and RestoreDone.  A lossless failover keeps the state and the row.
Restore(s) ==
  /\ nRestore < MaxRestore /\ own[s].st \notin {"restoring", "replaying"} /\ cat.ep < MaxEp
  /\ \E ll \in BOOLEAN :
       /\ IF ll
            THEN /\ UNCHANGED <<DatV, lost>> /\ fin' = [fin EXCEPT ![s] = own[s]]
            ELSE /\ lost' = lost \cup (store[s] \ bak[s].store)
                 /\ store' = [store EXCEPT ![s] = bak[s].store] /\ mk' = [mk EXCEPT ![s] = bak[s].mk]
                 /\ ex' = [ex EXCEPT ![s] = bak[s].ex]
                 /\ fin' = [fin EXCEPT ![s] = bak[s].own]
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" /\ wr[c].sh = s THEN IdleW ELSE wr[c]]
  /\ own' = [own EXCEPT ![s] = Row("restoring", own[s].ep)]
  /\ tl' = IF s = Src THEN tl + 1 ELSE tl
  /\ nRestore' = nRestore + 1
  /\ UNCHANGED <<CatV, MovV, used, sqt, ky, committed, cc, now, nBak, bak, FzV>>

\* Settle the open move against the catalog, then take the final row.
ReconcileDesign(s) ==
  /\ own[s].st = "restoring"
  /\ IF cm = "open"                                        \* before the point of no return: abort CAS, roll back
       THEN /\ cm' = "rolled_back" /\ mp' = "rolled_back"
            /\ store' = [store EXCEPT ![Tgt] = {}] /\ mk' = [mk EXCEPT ![Tgt] = {}] /\ ex' = [ex EXCEPT ![Tgt] = 0]
            /\ IF s = Src
                 THEN /\ cat' = [sh |-> Src, ep |-> cat.ep + 1]               \* owner restored: epoch bump
                      /\ fin' = [fin EXCEPT ![Src] = Row("active", cat.ep + 1)]
                      /\ own' = [own EXCEPT ![Src].st = "replaying", ![Tgt] = Row("none", 0)]
                 ELSE /\ cat' = IF mp = "early_flipped" THEN [sh |-> Src, ep |-> own[Src].ep] ELSE cat
                      /\ fin' = [fin EXCEPT ![Tgt] = Row("none", 0)]
                      /\ own' = [own EXCEPT ![Tgt].st = "replaying",
                                            ![Src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @]
            /\ UNCHANGED <<actSet, actMk>>
       ELSE IF cm = "committed"                             \* committed: complete (c), (b''), (d) here; no epoch bump
       THEN LET e1 == own[Tgt].ep IN
            /\ cat' = [sh |-> Tgt, ep |-> e1] /\ cm' = "done" /\ mp' = "done"
            /\ IF s = Tgt
                 THEN /\ IF mp = "tactive"
                           THEN UNCHANGED <<store, mk, ex, actSet, actMk>>
                           ELSE /\ store' = [store EXCEPT ![Tgt] = @ \cup store[Src]]    \* Src still holds the data
                                /\ mk' = [mk EXCEPT ![Tgt] = mk[Src]] /\ ex' = [ex EXCEPT ![Tgt] = ex[Src]]
                                /\ actSet' = store[Tgt] \cup store[Src] /\ actMk' = mk[Src]
                      /\ fin' = [fin EXCEPT ![Tgt] = Row("active", e1)]
                      /\ own' = [own EXCEPT ![Tgt].st = "replaying", ![Src] = Row("moved_out", e1)]
                 ELSE /\ fin' = [fin EXCEPT ![Src] = Row("moved_out", e1)]
                      /\ own' = [own EXCEPT ![Src].st = "replaying",
                                            ![Tgt] = IF @.st = "active" THEN @ ELSE Row("active", e1)]
                      /\ IF mp = "tactive"
                           THEN UNCHANGED <<store, mk, ex, actSet, actMk>>
                           ELSE /\ actSet' = store[Tgt] /\ actMk' = mk[Tgt] /\ UNCHANGED DatV
       ELSE /\ mp' = mp /\ cm' = cm                         \* no open move
            /\ IF cat.sh = s                                \* owner: new epoch
                 THEN /\ cat' = [sh |-> s, ep |-> cat.ep + 1]
                      /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
                 ELSE /\ cat' = cat
                      /\ fin' = [fin EXCEPT ![s] = IF s = Src THEN Row("moved_out", cat.ep) ELSE Row("none", 0)]
            /\ own' = [own EXCEPT ![s].st = "replaying"]
            /\ UNCHANGED <<DatV, actSet, actMk>>
  /\ UNCHANGED <<Tc, mtl, HistV, CliV, EnvV, frozenSet, frozenMk, zcut, cleaned>>

\* Bug variant: trust the backup row, bump the epoch if it says active, ignore the open move.
ReconcileNaive(s) ==
  /\ own[s].st = "restoring"
  /\ IF fin[s].st = "active"
       THEN /\ cat' = [sh |-> cat.sh, ep |-> cat.ep + 1]
            /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
       ELSE /\ cat' = cat /\ fin' = fin
  /\ own' = [own EXCEPT ![s].st = "replaying"]
  /\ UNCHANGED <<cm, MovV, DatV, HistV, CliV, EnvV, FzV>>

Reconcile_(s) == IF ReconcileOnRestore THEN ReconcileDesign(s) ELSE ReconcileNaive(s)

RestoreDone(s) ==
  /\ own[s].st = "replaying"
  /\ own' = [own EXCEPT ![s] = fin[s]]
  /\ UNCHANGED <<CatV, MovV, fin, DatV, HistV, CliV, EnvV, FzV>>

-----------------------------------------------------------------------------
Forward == Freeze \/ EndCopy \/ Reconcile \/ MakeReady \/ CommitCAS \/ Cut \/ Stamp \/ Activate \/ CatFlip
Recovery == \E s \in Shards : Reconcile_(s) \/ RestoreDone(s)
Clientstep == \E c \in Clients : Commit(c)
Copying == CopyRange \/ CopyMk \/ CopyEx

Next == Plan \/ Copying \/ Forward \/ Rollback \/ Reconnect \/ Tick \/ Cleanup \/ SweepEx
        \/ (\E c \in Clients : Refresh(c) \/ Commit(c) \/ (\E r \in Rows : Begin(c, r)))
        \/ (\E r \in Rows : SrcDelete(r))
        \/ (\E s \in Shards : Backup(s) \/ Restore(s)) \/ Recovery

Spec == Init /\ [][Next]_vars /\ WF_vars(Forward) /\ WF_vars(Recovery) /\ WF_vars(Clientstep)
        /\ WF_vars(StuckRollback) /\ WF_vars(Copying)

-----------------------------------------------------------------------------
(* Invariants *)

\* Never two writable owners; a writer only holds the fence at an active shard
\* (so `ready`, `incoming`, `frozen`, `moved_out`, `restoring` accept nothing).
SingleWriter ==
  /\ Cardinality({s \in Shards : own[s].st = "active"}) <= 1
  /\ \A c \in Clients : wr[c].ph = "hold" => own[wr[c].sh].st = "active"

\* The target is activated with exactly the insert-only rows and mutable keys that were frozen at the source.
NoLossNoDup ==
  mp \in {"tactive", "done", "early", "early_flipped"} =>
    /\ actSet = frozenSet /\ actMk = frozenMk

\* Before the point of no return the source is still the owner and holds every committed row (bar restore
\* RPO loss), so Rollback loses nothing.
RollbackPossibleBeforeC ==
  mp \in PreC =>
    /\ own[Src].st \in {"active", "frozen", "restoring", "replaying"}
    /\ (committed \ lost) \subseteq store[Src]

\* `incoming` and `ready` accept nothing: the target is never writable before the CAS.
NoWriteToTargetBeforeC == mp \in PreC => own[Tgt].st # "active"

NoRouteToTargetBeforeC == cat.sh = Tgt => cm \in {"committed", "done"}

\* A stale-session mover never executed Freeze, the CAS or (c).
ZombieCannotCutOver == ~zcut

\* A restored shard never ends active while another shard holds the namespace at a >= epoch.
RestoreReconciles ==
  \A s \in Shards : own[s].st = "active" =>
    /\ own[s].ep >= cat.ep
    /\ \A t \in Shards \ {s} :
         /\ own[t].st \notin {"active", "ready"}
         /\ own[t].st = "moved_out" => own[t].ep <= own[s].ep

Settled == \A s \in Shards : own[s].st \notin {"restoring", "replaying"}

\* Whenever no recovery is running and no move is mid-flight, exactly one shard is the writable owner.
OneOwner ==
  (Settled /\ mp \in Final) => Cardinality({s \in Shards : own[s].st = "active"}) = 1

\* ... and the catalog names it at its epoch; while (d) is pending the CAS can still succeed.
CatalogNamesOwnerAfterDone ==
  Settled =>
    /\ mp \in Final => (own[cat.sh].st = "active" /\ own[cat.sh].ep = cat.ep)
    /\ mp = "tactive" => cat = [sh |-> Src, ep |-> own[Tgt].ep - 1]

\* Cleanup never removes the only copy: once the source rows are gone, an active target holds what was frozen.
CleanupSafe == (cleaned /\ own[Tgt].st = "active") => frozenSet \subseteq store[Tgt]

\* Liveness: a started move completes or rolls back.
Symm == Permutations(Clients)

MoveTerminates == (mp # "none") ~> (mp \in {"done", "rolled_back"})

\* Liveness with active writers (no restore, no voluntary abort): every started move completes; a
\* deterministic rollback (missed rows, deleted keys, a sweep during the move) violates it.
MoveTerminatesActive == (mp # "none") ~> (mp = "done")

=============================================================================
