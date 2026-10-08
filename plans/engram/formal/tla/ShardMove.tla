---------------------------- MODULE ShardMove ----------------------------
(***************************************************************************)
(* Engram D27 (N160 to N163, N168 to N188): moving one namespace from Src  *)
(* to Tgt by freeze, copy, verify, index, SEAL, cut over.                  *)
(*                                                                         *)
(* The catalog row of the move, cm, is the arbiter:                        *)
(*   open --(a'') CAS--> committed --> done                                *)
(*   open --AbortCAS--> rolled_back   (mover, window, restore reconcile)   *)
(* Mover (mp): Plan (planned) -> Freeze (the source is static from here to *)
(* thaw or cleanup) -> CopyStep* (frozen) -> VerifyFrozen (copied) ->      *)
(* BuildIndex (built) -> SealCopy (sealed: the target's WAL is archived    *)
(* through copy_end_lsn and its standby has replayed the copy; flr) ->     *)
(* (b') Tgt incoming->ready -> (a'') CAS -> Replicated (the standby has the*)
(* commit) -> (c) Src frozen->moved_out (source row and replicated         *)
(* `committed` only) -> (b'') Tgt ready->active -> (d) catalog flip ->     *)
(* Cleanup (the gate is the seal).  An abort is AbortCAS (mp = aborting),  *)
(* AbortReplicated, then Thaw (needs the replicated `rolled_back`).        *)
(*                                                                         *)
(* No dirty copy, no catch-up, no merge, no re-run: the source does not    *)
(* change while it is copied.  Three table classes (N137) are copied alike:*)
(* an insert-only row r (ledger), a mutable key r (idempotency key; the    *)
(* source may sweep it before the freeze) and a counter of the expiring    *)
(* class (copied once, verified `<=`).  TgtDelete is a legitimate delete on*)
(* the active target; `gone` is the set of such rows, which are durable:   *)
(* the intent replay (Durability.tla) re-applies them after every restore. *)
(*                                                                         *)
(* Restore / failover of either shard (N123): the shard reverts to its last*)
(* backup (Restore), to its standby's replayed snapshot sb (Failover), or, *)
(* lossless, keeps its state; it comes up `restoring` (= frozen/restore),  *)
(* reconciles the move against the catalog (open: AbortCAS, then the thaw  *)
(* once `rolled_back` is replicated; committed: complete (c), (b''), (d)   *)
(* itself).  A target restored after the commit point is an ordinary       *)
(* restore: it completes (c)/(d), then RestoreDone activates it at a new   *)
(* epoch and closes the move bit mv (N180).  Once the copy is sealed, flr  *)
(* is the LSN floor of the target: a restore or failover that would land   *)
(* below it is refused (N179(3)); a restore at or past it carries the copy,*)
(* which the model states as "the snapshot covers the frozen rows".        *)
(*                                                                         *)
(* Catalog (N163, N171, N172, N185): CatalogLoss is a promotion of the     *)
(* standby that loses an arbiter outcome not replicated yet; CatalogPromote*)
(* is a lossless promotion; CatalogRestore is a restore from backup (RPO   *)
(* 60 s, T7-3: rep is reset); every one sets cdirty and the reconcile (no  *)
(* catalog access by anyone else while cdirty) re-derives cat/cm from the  *)
(* ownership rows: ReadShard(s) snapshots one row, ApplyReconcile derives  *)
(* from the snapshots (owner = highest-epoch active/frozen/restoring row), *)
(* so mover steps and shard restores interleave with the reads.            *)
(*                                                                         *)
(* Knobs (design: all TRUE except StampAfterCut, CopyBeforeFreeze,         *)
(* ReadyBeforeIndex, UnionRepair, AllowAbort, EpochFromAnyRow):            *)
(*   UseReady=FALSE           Tgt goes active before (c); catalog may flip *)
(*   ReconcileOnRestore=FALSE restore trusts its backup row, bumps epoch   *)
(*   ReconcileVerify=FALSE    VerifyFrozen compares nothing (CopyFault ok) *)
(*   FencedSteps=FALSE        mover steps do not check the rows they saw   *)
(*   SweepPause=FALSE         the expiring-class sweep runs under a freeze *)
(*   StampAfterCut=TRUE       (c) first, the catalog stamp afterwards      *)
(*   SeqAdvance=FALSE         Freeze does not raise sq[Tgt] (N147)         *)
(*   ShardTruth=FALSE         no catalog reconcile; shards trust the cm    *)
(*   CopyBeforeFreeze=TRUE    copy from the unfrozen source, no catch-up   *)
(*   ReadyBeforeIndex=TRUE    Tgt may become ready without its index       *)
(*   CutNeedsReplicated=FALSE (c) and the shard reconcile act on an        *)
(*                            unreplicated commit                          *)
(*   UnionRepair=TRUE         a restored, serving target is repaired by    *)
(*                            store[Tgt] u store[Src]                      *)
(*   FloorBeforeCut=FALSE     (b') without the seal: no floor, no wait for *)
(*                            the standby (N179)                           *)
(*   FailoverChecksFloor=FALSE a failover of the target may land below the *)
(*                            floor (N179(3))                              *)
(*   RestoreCloses=FALSE      restore_done leaves the move bit set (N180)  *)
(*   EndFromTargetRow=FALSE   the mover ends `done` when the catalog routes*)
(*                            to the target, whatever its row says (N180(3))*)
(*   ThawNeedsReplicated=FALSE thaw before `rolled_back` is replicated     *)
(*   EpochFromAnyRow=TRUE     the reconcile's epoch counts incoming/ready  *)
(*                            rows (N172)                                  *)
(*   AllowAbort               the mover may abort at any pre-commit step   *)
(*   MaxCat, MaxMoves, MaxFault, SqSkew, Window: catalog losses, promotions*)
(*   and restores, moves of the namespace, copy faults, initial sq[s1], the*)
(*   freeze window in ticks                                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS NRows, Clients, Life, MaxT, Window, MaxEp, MaxRestore, MaxBak, AllowAbort,
          UseReady, ReconcileOnRestore, ReconcileVerify, FencedSteps,
          SweepPause, StampAfterCut, SeqAdvance, ShardTruth, MaxCat, MaxMoves, MaxFault, SqSkew,
          CopyBeforeFreeze, ReadyBeforeIndex, CutNeedsReplicated, UnionRepair,
          FloorBeforeCut, FailoverChecksFloor, RestoreCloses, EndFromTargetRow, ThawNeedsReplicated, EpochFromAnyRow

Shards == {"s1", "s2"}
Other(s) == IF s = "s1" THEN "s2" ELSE "s1"
Rows == 1..NRows
PreC == {"planned", "frozen", "copied", "built", "sealed", "ready", "early", "early_flipped", "aborting"}
PostC == {"committed", "cut", "tactive", "done"}
Final == {"none", "rolled_back", "done"}

VARIABLES cat, cm, mp, own, fin, store, mk, ex, bak, sb, flr, mv, used, sqt, committed, lost, gone,
          wr, cc, now, mtl, tl, nRestore, nBak,
          frozenSet, frozenMk, frozenEx, actSet, actMk, zcut, cleaned,
          src, tgt, nMoves, me, sq, tlc, nCat, idx, rep, cdirty, rview, abt, abp,
          cdn, cex, rc, nFault, fzt
CatV  == <<cat, cm>>
MovV  == <<mp, mtl>>
OwnV  == <<own, fin>>
DatV  == <<store, mk, ex>>
HistV == <<used, sqt, committed, lost, gone>>
CliV  == <<wr, cc>>
EnvV  == <<now, tl, nRestore, nBak, bak>>
FzV   == <<frozenSet, frozenMk, frozenEx, actSet, actMk, zcut, cleaned>>
RolV  == <<src, tgt, nMoves, me>>             \* roles of the current move (a second move swaps them); me = target epoch
SqV   == <<sq>>
IdxV  == <<idx>>
FlagV == <<rep, cdirty, rview, abt, abp>>
FlrV  == <<sb, flr, mv>>                    \* sb: standby snapshot; flr: the target's WAL floor is set; mv: the ownership row's move bit
CpV   == <<cdn, cex, rc, nFault, fzt>>
TlV   == <<tlc, nCat>>
vars == <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

Row(st, ep) == [st |-> st, ep |-> ep]
NoRow == Row("unread", 0)
IdleW == [ph |-> "idle", r |-> 0, sh |-> "s1", age |-> 0]
Snap(s) == [own |-> own[s], store |-> store[s], mk |-> mk[s], ex |-> ex[s], sq |-> sq[s], idx |-> idx[s]]
Holders(s) == {c \in Clients : wr[c].ph = "hold" /\ wr[c].sh = s}
\* Every mover step is a compare-and-set on the rows it observed (source, target, catalog move).
Fenced == cm = "open" /\ (~FencedSteps \/ (own[src].st \in {"active", "frozen"} /\ own[tgt].st = "incoming"))
SrcFrozen == ~FencedSteps \/ own[src].st = "frozen"
SessionOk == mtl = tl
Settled == \A s \in Shards : own[s].st \notin {"restoring", "replaying"}
Max2(a, b) == IF a >= b THEN a ELSE b

\* A snapshot (a backup, a standby's replay) carries the whole copy of the move: every frozen row it did not delete itself.
Covers(B) == (frozenSet \ gone) \subseteq B.store /\ (frozenMk \ gone) \subseteq B.mk
\* N179(3): once the copy is sealed, a restore or failover of the target to B would land below the floor if B lacks the copy.
BelowFloor(s, B) == s = tgt /\ flr[tgt] /\ ~Covers(B)

\* The copy of the current move is complete: every row of the (static) source has been copied once, and the counter.
CopyDone == (store[src] \cup mk[src]) \subseteq cdn /\ cex
\* VerifyFrozen: per table equality (count and key hash) of the insert-only and mutable classes, `<=` for the counter.
Verified == ~ReconcileVerify \/ (store[src] = store[tgt] /\ mk[src] = mk[tgt] /\ ex[tgt] <= ex[src])
CopyPhase == IF CopyBeforeFreeze THEN mp = "planned" ELSE (mp = "frozen" /\ SrcFrozen)

\* Rows that the move preserves although the shard that held them loses its tail (not an RPO loss): the target's copy
\* of the source rows is repairable from the intact source; the source's frozen rows live on the target copy once the
\* commit is durable (replicated, or acted on by (c)).  A commit that is neither is not a promise (N163(3)).
Recoverable(s) ==
  IF cleaned THEN {}
  ELSE IF s = tgt /\ flr[tgt] /\ mp \in {"sealed", "ready", "committed", "cut", "tactive", "done"}
       THEN frozenSet
  ELSE IF s = src /\ ((cm = "committed" /\ rep) \/ cm = "done" \/ mp \in {"cut", "tactive", "done"}) THEN frozenSet
  ELSE {}

TypeOK ==
  /\ cat.sh \in Shards /\ cat.ep \in 1..MaxEp
  /\ cm \in {"none", "open", "committed", "rolled_back", "done"}
  /\ mp \in {"none", "planned", "frozen", "copied", "built", "sealed", "ready", "committed", "cut",
             "tactive", "done", "aborting", "rolled_back", "early", "early_flipped"}
  /\ \A s \in Shards : store[s] \subseteq Rows /\ mk[s] \subseteq Rows /\ ex[s] \in 0..NRows
  /\ now \in 0..MaxT /\ src \in Shards /\ tgt = Other(src) /\ nMoves \in 0..MaxMoves
  /\ rc \in 0..1 /\ nFault \in 0..MaxFault /\ cdn \subseteq Rows
  /\ flr \in [Shards -> BOOLEAN] /\ mv \in [Shards -> BOOLEAN]

Init ==
  /\ cat = [sh |-> "s1", ep |-> 1] /\ cm = "none" /\ mp = "none"
  /\ own = [s \in Shards |-> IF s = "s1" THEN Row("active", 1) ELSE Row("none", 0)]
  /\ fin = own
  /\ store = [s \in Shards |-> {}] /\ mk = [s \in Shards |-> {}] /\ ex = [s \in Shards |-> 0]
  /\ sq = [s \in Shards |-> IF s = "s1" THEN SqSkew ELSE 0]
  /\ idx = [s \in Shards |-> s = "s1"]
  /\ bak = [s \in Shards |-> [own |-> own[s], store |-> {}, mk |-> {}, ex |-> 0, sq |-> sq[s], idx |-> idx[s]]]
  /\ sb = bak /\ flr = [s \in Shards |-> FALSE] /\ mv = [s \in Shards |-> FALSE]
  /\ used = {} /\ sqt = [r \in Rows |-> 0]
  /\ committed = {} /\ lost = {} /\ gone = {}
  /\ wr = [c \in Clients |-> IdleW] /\ cc = [c \in Clients |-> cat]
  /\ now = 0 /\ mtl = 0 /\ tl = 0 /\ nRestore = 0 /\ nBak = 0
  /\ frozenSet = {} /\ frozenMk = {} /\ frozenEx = 0 /\ actSet = {} /\ actMk = {} /\ zcut = FALSE /\ cleaned = FALSE
  /\ src = "s1" /\ tgt = "s2" /\ nMoves = 0 /\ me = 1
  /\ tlc = [s \in Shards |-> 0] /\ nCat = 0
  /\ rep = FALSE /\ cdirty = FALSE /\ rview = [s \in Shards |-> NoRow] /\ abt = FALSE /\ abp = "none"
  /\ cdn = {} /\ cex = FALSE /\ rc = 0 /\ nFault = 0 /\ fzt = 0

-----------------------------------------------------------------------------
(* Clients, writers and the source's own sweeps *)

Refresh(c) == /\ ~cdirty
              /\ cc' = [cc EXCEPT ![c] = cat]
              /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, wr, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* A write transaction takes the shared fence and checks `active` at its epoch.  Its insert-only
\* row has ins_seq = nextval of the shard's sequence.
Begin(c, r) ==
  /\ wr[c].ph = "idle" /\ r \notin used
  /\ LET s == cc[c].sh IN own[s] = Row("active", cc[c].ep)
  /\ wr' = [wr EXCEPT ![c] = [ph |-> "hold", r |-> r, sh |-> cc[c].sh, age |-> 0]]
  /\ used' = used \cup {r}
  /\ sqt' = [sqt EXCEPT ![r] = sq[cc[c].sh]]
  /\ sq' = [sq EXCEPT ![cc[c].sh] = @ + 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, committed, lost, gone, cc, EnvV, FzV, RolV, IdxV, FlagV, CpV, TlV, FlrV>>

Commit(c) ==
  /\ wr[c].ph = "hold"
  /\ LET s == wr[c].sh IN LET r == wr[c].r IN
     /\ own[s].st = "active"
     /\ store' = [store EXCEPT ![s] = @ \cup {r}]
     /\ mk' = [mk EXCEPT ![s] = @ \cup {r}]
     /\ ex' = [ex EXCEPT ![s] = IF @ < NRows THEN @ + 1 ELSE @]
  /\ committed' = committed \cup {wr[c].r}
  /\ wr' = [wr EXCEPT ![c] = IdleW]
  /\ UNCHANGED <<CatV, MovV, OwnV, used, sqt, lost, gone, cc, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* Source-side deletes of the mutable class (the 24 h sweep of idempotency keys).  An active source only: under
\* the freeze nothing is deleted (N160).
SrcDelete(r) ==
  /\ own[src].st = "active" /\ r \in mk[src]
  /\ mk' = [mk EXCEPT ![src] = @ \ {r}]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* A legitimate delete on the active target (document expunge, retire purge, key sweep, compaction; C-1).  The
\* row stays on the source until its cleanup; the delete is durable (intent), see the header.
TgtDelete(r) ==
  /\ own[tgt].st = "active" /\ mp \in {"tactive", "done"} /\ r \in store[tgt]
  /\ store' = [store EXCEPT ![tgt] = @ \ {r}]
  /\ mk' = [mk EXCEPT ![tgt] = @ \ {r}]
  /\ gone' = gone \cup {r}
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, used, sqt, committed, lost, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* The retention sweep of the expiring class.  It joins purgeable_namespaces and so pauses during a move; with
\* SweepPause = FALSE it also runs under the freeze (a source that is not static).
SweepEx ==
  /\ ex[src] > 0
  /\ \/ own[src].st = "active" /\ (~SweepPause \/ cm \notin {"open", "committed"})
     \/ ~SweepPause /\ own[src].st = "frozen"
  /\ ex' = [ex EXCEPT ![src] = @ - 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

Tick ==
  /\ now < MaxT /\ \A c \in Clients : wr[c].ph = "idle" \/ wr[c].age < Life
  /\ now' = now + 1
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" THEN [wr[c] EXCEPT !.age = @ + 1] ELSE wr[c]]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, cc, tl, nRestore, nBak, bak, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

-----------------------------------------------------------------------------
(* The mover *)

Plan ==
  /\ mp \in {"none", "done"} /\ nMoves < MaxMoves /\ (nMoves = 0 \/ cleaned) /\ ~cdirty
  /\ LET s == cat.sh IN LET t == Other(s) IN
     /\ own[s] = Row("active", cat.ep) /\ cat.ep < MaxEp
     /\ own[t].st \in {"none", "moved_out"} /\ store[t] = {} /\ mk[t] = {} /\ ex[t] = 0
     /\ src' = s /\ tgt' = t
     /\ own' = [own EXCEPT ![t] = Row("incoming", cat.ep + 1)]
     /\ me' = cat.ep + 1
     /\ idx' = [idx EXCEPT ![t] = FALSE]
     /\ mv' = [mv EXCEPT ![t] = TRUE]
  /\ flr' = [s \in Shards |-> FALSE]
  /\ nMoves' = nMoves + 1
  /\ mp' = "planned" /\ cm' = "open"
  /\ mtl' = tl
  /\ frozenSet' = {} /\ frozenMk' = {} /\ frozenEx' = 0 /\ actSet' = {} /\ actMk' = {} /\ cleaned' = FALSE
  /\ rep' = FALSE
  /\ cdn' = {} /\ cex' = FALSE /\ rc' = 0 /\ fzt' = 0
  /\ UNCHANGED <<cat, fin, DatV, HistV, CliV, EnvV, zcut, SqV, cdirty, rview, abt, abp, nFault, TlV, sb>>

\* Freeze (exclusive fence, one attempt, no holder).  The source is static from here.  The one engram_seq_advance
\* call of the move follows directly (no source write exists in between, N147).
Freeze ==
  /\ mp = "planned" /\ Fenced /\ own[src].st = "active" /\ Holders(src) = {} /\ SessionOk
  /\ own' = [own EXCEPT ![src].st = "frozen"]
  /\ mp' = IF CopyBeforeFreeze THEN "copied" ELSE "frozen"
  /\ frozenSet' = store[src] /\ frozenMk' = mk[src] /\ frozenEx' = ex[src]
  /\ sq' = IF SeqAdvance THEN [sq EXCEPT ![tgt] = Max2(sq[tgt], sq[src])] ELSE sq
  /\ fzt' = now
  /\ mv' = [mv EXCEPT ![src] = TRUE]
  /\ UNCHANGED <<CatV, mtl, fin, DatV, HistV, CliV, EnvV, actSet, actMk, zcut, cleaned, RolV, IdxV, FlagV, cdn, cex, rc, nFault, TlV, sb, flr>>

\* Copy one row of the static source (COPY into a temp table, upsert under the incoming fence).  CopyBeforeFreeze
\* is the rejected variant: the same copy from an active source.
CopyStep(r) ==
  /\ Fenced /\ CopyPhase
  /\ r \in (store[src] \cup mk[src]) \ cdn
  /\ store' = IF r \in store[src] THEN [store EXCEPT ![tgt] = @ \cup {r}] ELSE store
  /\ mk' = IF r \in mk[src] THEN [mk EXCEPT ![tgt] = @ \cup {r}] ELSE mk
  /\ cdn' = cdn \cup {r}
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, cex, rc, nFault, fzt, TlV, FlrV>>

CopyEx ==
  /\ Fenced /\ CopyPhase /\ ~cex
  /\ ex' = [ex EXCEPT ![tgt] = ex[src]]
  /\ cex' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, cdn, rc, nFault, fzt, TlV, FlrV>>

\* A fault of the copy, to give VerifyFrozen something to catch: a dropped row (the target holds no row before the
\* copy, so a stale row cannot exist, N175).
CopyFault(r) ==
  /\ Fenced /\ CopyPhase /\ nFault < MaxFault
  /\ r \in store[tgt] \cup mk[tgt]
  /\ store' = [store EXCEPT ![tgt] = @ \ {r}] /\ mk' = [mk EXCEPT ![tgt] = @ \ {r}]
  /\ nFault' = nFault + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, cdn, cex, rc, fzt, TlV, FlrV>>

\* VerifyFrozen on the complete copy.  A mismatch re-copies once (Recopy), a second mismatch rolls back (VerifyAbort).
VerifyFrozen ==
  /\ mp = "frozen" /\ Fenced /\ SrcFrozen /\ CopyDone /\ Verified
  /\ mp' = "copied"
  /\ UNCHANGED <<CatV, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>
VerifyFails == mp = "frozen" /\ Fenced /\ SrcFrozen /\ CopyDone /\ ~Verified
Recopy ==
  /\ VerifyFails /\ rc = 0
  /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
  /\ cdn' = {} /\ cex' = FALSE /\ rc' = 1
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, nFault, fzt, TlV, FlrV>>

\* BuildIndexes under the freeze: the index runner builds the HNSW of every vector table; readiness is the index.
BuildIndex ==
  /\ mp = "copied" /\ Fenced /\ SrcFrozen
  /\ idx' = [idx EXCEPT ![tgt] = TRUE] /\ mp' = "built"
  /\ UNCHANGED <<CatV, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlagV, CpV, TlV, FlrV>>

\* SealCopy (N179(1)): after the last index build, the target's WAL is archived through copy_end_lsn and its standby has
\* replayed it (sb covers the copy); the floor flr is then recorded.  A restore or failover of the target lands at or past it.
SealCopy ==
  /\ mp = "built" /\ Fenced /\ SrcFrozen
  /\ FloorBeforeCut => Covers(sb[tgt])
  /\ flr' = [flr EXCEPT ![tgt] = TRUE]
  /\ mp' = "sealed"
  /\ UNCHANGED <<CatV, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, sb, mv>>

MakeReady ==                                                 \* (b')
  /\ mp \in (IF ReadyBeforeIndex THEN {"copied"} ELSE IF FloorBeforeCut THEN {"sealed"} ELSE {"built", "sealed"})
  /\ Fenced /\ SrcFrozen
  /\ IF UseReady
       THEN /\ own' = [own EXCEPT ![tgt].st = "ready"] /\ mp' = "ready"
            /\ UNCHANGED <<actSet, actMk>>
       ELSE /\ own' = [own EXCEPT ![tgt].st = "active"] /\ mp' = "early"      \* old order: no ready state
            /\ actSet' = store[tgt] /\ actMk' = mk[tgt]
  /\ UNCHANGED <<CatV, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* (a''): the catalog CAS, the point of no return.  Retried when a catalog loss reverted it.
CommitCAS ==
  /\ mp \in {"ready", "committed"} /\ ~StampAfterCut /\ cm = "open" /\ SessionOk /\ ~cdirty
  /\ (~FencedSteps \/ (own[src].st = "frozen" /\ own[tgt].st = "ready"))   \* the rows the mover verified
  /\ cm' = "committed" /\ mp' = "committed" /\ rep' = FALSE
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, mtl, OwnV, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, actSet, actMk, cleaned, RolV, SqV, IdxV, cdirty, rview, abt, abp, CpV, TlV, FlrV>>

\* The asynchronous standby has replayed the commit (N163(3)).
Replicated ==
  /\ cm = "committed" /\ ~rep
  /\ rep' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, cdirty, rview, abt, abp, CpV, TlV, FlrV>>

\* (c) (N169(4), N171): Src frozen -> moved_out, fenced on the source row and the replicated `committed` only; the
\* target row is not a conjunct (after the commit point it is the restore path's business).
Cut ==
  /\ own[src].st = "frozen" /\ SessionOk /\ ~cdirty
  /\ \/ mp = "committed" /\ cm = "committed" /\ (rep \/ ~CutNeedsReplicated)
     \/ mp \in {"early", "early_flipped"}
     \/ StampAfterCut /\ mp = "ready"
  /\ own' = [own EXCEPT ![src] = Row("moved_out", me)]
  /\ mp' = CASE mp = "committed" -> "cut" [] mp = "ready" -> "cut" [] mp = "early" -> "tactive" [] OTHER -> "done"
  /\ cm' = IF mp = "early" THEN "committed" ELSE IF mp = "early_flipped" THEN "done" ELSE cm
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, actSet, actMk, cleaned, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* Old-order bookkeeping: the stamp that records (c) in the catalog after the fact.
Stamp ==
  /\ StampAfterCut /\ mp \in {"cut", "tactive"} /\ cm = "open"
  /\ cm' = "committed"
  /\ UNCHANGED <<cat, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* (b'') (N180(2)): whenever the target row reads `ready` and the move reads `committed`, whatever the routing row says.
Activate ==
  /\ mp = "cut" /\ own[tgt].st = "ready" /\ cm = "committed"
  /\ own' = [own EXCEPT ![tgt].st = "active"] /\ mp' = "tactive"
  /\ actSet' = store[tgt] /\ actMk' = mk[tgt]
  /\ mv' = [s \in Shards |-> FALSE]
  /\ UNCHANGED <<CatV, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, SqV, IdxV, FlagV, CpV, TlV, sb, flr>>

\* The rejected end test (EndFromTargetRow = FALSE, C8-2): after (c) the mover ends `done` as soon as the routing row names
\* the target, without reading the target's row; a reconcile after (c) writes that routing row while the target is `ready`.
EndOnRouting ==
  /\ ~EndFromTargetRow /\ mp = "cut" /\ ~cdirty /\ cat.sh = tgt /\ cat.ep >= me
  /\ mp' = "done" /\ cm' = "done"
  /\ UNCHANGED <<cat, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* (d): catalog flip WHERE epoch = e AND state = frozen; early order flips before (c).  After a catalog
\* reconcile the flip has already been derived from the shards.
CatFlip ==
  /\ ~cdirty
  /\ \/ mp = "tactive" /\ cat = [sh |-> src, ep |-> me - 1]
     \/ mp = "tactive" /\ cat = [sh |-> tgt, ep |-> own[tgt].ep]
     \/ mp = "early"
  /\ cat' = [sh |-> tgt, ep |-> own[tgt].ep]
  /\ mp' = IF mp = "tactive" THEN "done" ELSE "early_flipped"
  /\ cm' = IF mp = "tactive" THEN "done" ELSE cm
  /\ UNCHANGED <<mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* The rejected D24 repair: a cleanup-time ReconcileIn against a serving target that was restored or promoted.
UnionRepairStep ==
  /\ UnionRepair /\ mp = "done" /\ ~cleaned /\ tlc[tgt] > 0 /\ Settled
  /\ own[tgt].st = "active" /\ own[src].st = "moved_out" /\ ~(store[src] \subseteq store[tgt])
  /\ store' = [store EXCEPT ![tgt] = @ \cup store[src]]
  /\ mk' = [mk EXCEPT ![tgt] = @ \cup mk[src]]
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* Cleanup (N161(1), N170): the source's data rows go; the moved_out ownership row stays.  The gate is the seal (already
\* taken, N179): the target's restore point is never below the floor, so never earlier than a complete copy.
Cleanup ==
  /\ mp = "done" /\ ~cleaned /\ own[tgt].st = "active" /\ own[src].st = "moved_out"
  /\ store' = [store EXCEPT ![src] = {}] /\ mk' = [mk EXCEPT ![src] = {}] /\ ex' = [ex EXCEPT ![src] = 0]
  /\ idx' = [idx EXCEPT ![src] = FALSE]
  /\ cleaned' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, actSet, actMk, zcut, RolV, SqV, FlagV, CpV, TlV, FlrV>>

\* Abort before the point of no return (N171(2)): the abort CAS open -> rolled_back; the shard actions follow only
\* once it is replicated.  A catalog loss may revert it, then the mover (or a restore reconcile) takes it again.
AbortCAS ==
  /\ mp \in PreC /\ cm = "open" /\ Holders(tgt) = {} /\ ~cdirty
  /\ cm' = "rolled_back" /\ mp' = "aborting" /\ rep' = FALSE /\ abt' = TRUE /\ abp' = mp
  /\ UNCHANGED <<cat, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, cdirty, rview, CpV, TlV, FlrV>>

AbortReplicated ==
  /\ cm = "rolled_back" /\ ~rep
  /\ rep' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, cdirty, rview, abt, abp, CpV, TlV, FlrV>>

\* Thaw the source, drop the target rows and indexes, undo a premature flip.  It re-reads `rolled_back` and (design)
\* waits for its replication.
Thaw ==
  /\ abt /\ Settled /\ ~cdirty
  /\ IF ThawNeedsReplicated THEN cm = "rolled_back" /\ rep ELSE TRUE   \* without it the actor acts on the outcome it read
  /\ mp' = IF mp = "aborting" THEN "rolled_back" ELSE mp
  /\ abt' = FALSE
  /\ own' = [own EXCEPT ![src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @,
                        ![tgt] = Row("none", 0)]
  /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
  /\ idx' = [idx EXCEPT ![tgt] = FALSE]
  /\ cat' = IF cat.sh = tgt THEN [sh |-> src, ep |-> own[src].ep] ELSE cat
  /\ mv' = [s \in Shards |-> FALSE] /\ flr' = [s \in Shards |-> FALSE]            \* return_move clears the floor and the move bit
  /\ UNCHANGED <<cm, mtl, fin, HistV, CliV, EnvV, FzV, RolV, SqV, rep, cdirty, rview, abp, CpV, TlV, sb>>
AbortAny == AllowAbort /\ AbortCAS
AbortRetry == mp = "aborting" /\ AbortCAS
VerifyAbort == VerifyFails /\ rc = 1 /\ AbortCAS                         \* the second mismatch: MoveVerifyFailed
WindowTimeout == mp \in {"frozen", "copied", "built", "sealed"} /\ now >= fzt + Window /\ AbortCAS   \* MoveWindowExceeded
Rollback == AbortAny \/ AbortRetry \/ VerifyAbort \/ WindowTimeout

\* The mover restarts (Temporal) and re-reads the shard's timeline.
Reconnect ==
  /\ mp \in PreC /\ mtl # tl /\ mtl' = tl
  /\ UNCHANGED <<CatV, mp, OwnV, DatV, HistV, CliV, now, tl, nRestore, nBak, bak, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

-----------------------------------------------------------------------------
(* Backups, restore and failover (N123, N161) *)

Backup(s) ==
  /\ nBak < MaxBak /\ own[s].st \notin {"restoring", "replaying"}
  /\ bak' = [bak EXCEPT ![s] = [own |-> own[s], store |-> store[s], mk |-> mk[s], ex |-> ex[s], sq |-> sq[s], idx |-> idx[s]]]
  /\ nBak' = nBak + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, now, tl, nRestore, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

\* Restore to the last backup, or failover to a replica that keeps everything: the shard rejects everything
\* until Reconcile and RestoreDone.  A lossy restore re-applies the acknowledged deletes (intent replay).  Faults are
\* not combined with a dirty catalog: the catalog reconcile runs first (N163(1)).
\* B is the image the shard comes back to: its last backup (Restore), its standby's replayed snapshot (Failover); lossy = FALSE
\* keeps the state (a failover to a replica that has everything).  The shard's standby is rebuilt from the result.
RestoreCore(s, B, lossy) ==
  /\ nRestore < MaxRestore /\ own[s].st \notin {"restoring", "replaying"} /\ cat.ep < MaxEp
  /\ \A o \in Shards : tlc[o] = 0 \/ o = s            \* repeated faults hit one shard (a fault of both copies is outside the model)
  /\ IF ~lossy
       THEN /\ UNCHANGED <<DatV, lost, sq, idx>> /\ fin' = [fin EXCEPT ![s] = own[s]]
            /\ sb' = [sb EXCEPT ![s] = Snap(s)]
       ELSE /\ lost' = lost \cup ((store[s] \ B.store) \ Recoverable(s))
            /\ store' = [store EXCEPT ![s] = B.store \ gone]
            /\ mk' = [mk EXCEPT ![s] = B.mk \ gone]
            /\ ex' = [ex EXCEPT ![s] = B.ex]
            /\ sq' = [sq EXCEPT ![s] = B.sq]
            /\ idx' = [idx EXCEPT ![s] = B.idx]
            /\ fin' = [fin EXCEPT ![s] = B.own]
            /\ sb' = [sb EXCEPT ![s] = [B EXCEPT !.store = B.store \ gone, !.mk = B.mk \ gone]]
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" /\ wr[c].sh = s THEN IdleW ELSE wr[c]]
  /\ own' = [own EXCEPT ![s] = Row("restoring", own[s].ep)]
  /\ tl' = IF s = src THEN tl + 1 ELSE tl
  /\ tlc' = [tlc EXCEPT ![s] = @ + 1]
  /\ nRestore' = nRestore + 1
  /\ UNCHANGED <<CatV, MovV, used, sqt, committed, gone, cc, now, nBak, bak, FzV, RolV, FlagV, CpV, nCat, flr, mv>>

TgtPhase == cm \in {"committed", "done"}
\* A restore to B, refused below the floor (N179(3)); the shard's own catalog reconcile follows.  There is no ~cdirty guard
\* (N185): a promotion's reads interleave with a shard restore.
RestoreTo(s, B) == RestoreCore(s, B, FALSE) \/ (~BelowFloor(s, B) /\ RestoreCore(s, B, TRUE))
Restore(s) == ~(s = tgt /\ TgtPhase) /\ RestoreTo(s, bak[s])
\* The target restored or promoted after the commit point, to bak[tgt]: an ordinary restore at a new epoch (N169).
TgtRestore == TgtPhase /\ me < MaxEp /\ RestoreTo(tgt, bak[tgt])
\* The standby of s is promoted with what it has replayed; refused when that is below the target's floor (N179(3)).
Failover(s) ==
  /\ (s = tgt /\ TgtPhase) => me < MaxEp
  /\ FailoverChecksFloor => ~BelowFloor(s, sb[s])
  /\ RestoreCore(s, sb[s], TRUE)

\* The standby replays the primary up to now, or is rebuilt from the base backup and starts again from there (N179(3)).  Enabled
\* while a failover can still happen, and for the seal's wait.
StandbyReplay(s) ==
  /\ own[s].st \notin {"restoring", "replaying"}
  /\ \/ /\ sb[s] # Snap(s) /\ (nRestore < MaxRestore \/ (s = tgt /\ mp = "built"))
        /\ sb' = [sb EXCEPT ![s] = Snap(s)]
     \/ /\ nRestore < MaxRestore /\ sb[s] # bak[s]                     \* the standby is rebuilt from the base backup (PG8-8)
        /\ sb' = [sb EXCEPT ![s] = bak[s]]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, flr, mv>>

\* N171(2), N163(3): the catalog promotion loses an arbiter outcome that the standby has not replayed; the promotion
\* runs the reconcile (cdirty).
CatalogLoss ==
  /\ nCat < MaxCat /\ cm \in {"committed", "rolled_back"} /\ ~rep
  /\ cm' = "open" /\ nCat' = nCat + 1
  /\ cdirty' = ShardTruth /\ rview' = [s \in Shards |-> NoRow]
  \* a lost abort: the mover never learned of it and resumes from its pre-abort step; the aborting actor's thaw is
  \* pending only if it does not wait (ThawNeedsReplicated = FALSE)
  /\ mp' = (IF mp = "aborting" THEN abp ELSE mp)
  /\ abt' = (abt /\ ~ThawNeedsReplicated) /\ abp' = abp
  /\ UNCHANGED <<cat, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rep, CpV, tlc, FlrV>>

\* N146, N163(5), T7-3: a catalog restore from backup (RPO 60 s) reverts the move row and the routing row.
CatalogRestore ==
  /\ nCat < MaxCat /\ cm \in {"committed", "done"} /\ Settled
  /\ cm' = "open"
  /\ cat' = IF cat.sh = tgt THEN [sh |-> src, ep |-> me - 1] ELSE cat
  /\ cdirty' = ShardTruth /\ rep' = FALSE /\ rview' = [s \in Shards |-> NoRow]   \* the commit is gone from the catalog, so it is no promise any more
  /\ nCat' = nCat + 1
  /\ UNCHANGED <<MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, abt, abp, CpV, tlc, FlrV>>

\* N188, N172: a lossless promotion of the catalog standby: nothing is reverted, but the promotion runs the reconcile, which
\* re-derives the routing row from the ownership rows (so it runs after (c), C8-2).
CatalogPromote ==
  /\ nCat < MaxCat /\ ShardTruth /\ ~cdirty
  /\ cdirty' = TRUE /\ rview' = [s \in Shards |-> NoRow] /\ nCat' = nCat + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rep, abt, abp, CpV, tlc, FlrV>>

\* engramctl catalog reconcile --from-shards (N163(2), N172, C7-5): ReadShard snapshots one ownership row, ApplyReconcile
\* derives the routing row from the unique owner (its epoch from owner rows only; EpochFromAnyRow counts every row) and the
\* move row from the source's moved_out or the target's active row.
ReadShard(s) ==
  /\ ShardTruth /\ cdirty /\ rview[s] = NoRow            \* a restoring shard reads as no owner
  /\ rview' = [rview EXCEPT ![s] = own[s]]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rep, cdirty, abt, abp, CpV, TlV, FlrV>>

\* The owner is the highest-epoch active/frozen/restoring row (frozen/restore counts, N180(4)); at one epoch an active or
\* frozen row outranks a restoring one.  A re-derived `committed` (the source row moved_out) writes the routing row and
\* nothing else: it never activates a shard row (N172).
ApplyReconcile ==
  /\ ShardTruth /\ cdirty /\ \A s \in Shards : rview[s] # NoRow
  /\ LET owners == {s \in Shards : rview[s].st \in {"active", "frozen", "restoring", "replaying"}} IN
     LET rank(s) == 2 * rview[s].ep + (IF rview[s].st \in {"active", "frozen"} THEN 1 ELSE 0) IN
     LET anyEp == Max2(rview["s1"].ep, rview["s2"].ep) IN
       cat' = IF owners # {}
                THEN LET o == CHOOSE s \in owners : \A p \in owners : rank(s) >= rank(p)
                     IN [sh |-> o, ep |-> IF EpochFromAnyRow THEN anyEp ELSE rview[o].ep]
              ELSE IF mp # "none" /\ rview[src] = Row("moved_out", me) THEN [sh |-> tgt, ep |-> me]
              ELSE cat
  /\ cm' = IF cm = "open" /\ (rview[src].st = "moved_out" \/ rview[tgt].st = "active")
             THEN (IF mp = "done" THEN "done" ELSE "committed") ELSE cm
  /\ cdirty' = FALSE /\ rview' = [s \in Shards |-> NoRow]
  /\ UNCHANGED <<MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rep, abt, abp, CpV, TlV, FlrV>>

\* What the ownership rows say about the catalog's move (N146): the source row moved_out, or the target row active at
\* the move's epoch or later, can only exist after the CAS committed.
DerivedCommitted(s) ==
  IF s = tgt THEN own[src].st = "moved_out" \/ (fin[tgt].st = "active" /\ fin[tgt].ep >= me)
  ELSE own[tgt].st = "active" \/ fin[src] = Row("moved_out", me)

\* Settle the move against the catalog, then take the final row.  With ShardTruth the catalog row is first re-derived
\* from the ownership rows.  Every act on an arbiter outcome waits for its replication (N171(2)): the abort CAS
\* of the reconcile is its own step, the thaw follows once `rolled_back` is replicated.
ReconcileDesign(s) ==
  /\ own[s].st = "restoring" /\ ~cdirty
  /\ LET cmE == IF ShardTruth /\ cm = "open" /\ DerivedCommitted(s) THEN "committed" ELSE cm IN
     LET seen == cm # "committed" \/ rep \/ ~CutNeedsReplicated IN            \* a read `committed` is acted on once replicated
     IF cmE = "open"                                       \* the rows show no commit (N161(4)): abort CAS
       THEN /\ cm' = "rolled_back" /\ mp' = "aborting" /\ rep' = FALSE /\ abp' = mp /\ abt' = FALSE
            /\ UNCHANGED <<own, cat, fin, DatV, idx, actSet, actMk, flr, mv>>
     ELSE IF cm = "rolled_back" /\ mp = "aborting"         \* the thaw, once `rolled_back` is replicated
       THEN /\ rep \/ ~ThawNeedsReplicated
            /\ cm' = cm /\ mp' = "rolled_back" /\ rep' = rep
            /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
            /\ idx' = [idx EXCEPT ![tgt] = FALSE]
            /\ IF s = src
                 THEN /\ cat' = [sh |-> src, ep |-> cat.ep + 1]               \* owner restored: epoch bump
                      /\ fin' = [fin EXCEPT ![src] = Row("active", cat.ep + 1)]
                      /\ own' = [own EXCEPT ![src].st = "replaying", ![tgt] = Row("none", 0)]
                 ELSE /\ cat' = IF cat.sh = tgt THEN [sh |-> src, ep |-> own[src].ep] ELSE cat
                      /\ fin' = [fin EXCEPT ![tgt] = Row("none", 0)]
                      /\ own' = [own EXCEPT ![tgt].st = "replaying",
                                            ![src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @]
            /\ mv' = [t2 \in Shards |-> FALSE] /\ flr' = [t2 \in Shards |-> FALSE]
            /\ UNCHANGED <<actSet, actMk, abt, abp>>
     ELSE IF s = tgt /\ cmE \in {"committed", "done"}      \* N169: an ordinary restore after (c)/(d), at a new epoch
       THEN /\ seen
            /\ LET ne == IF cat.sh = tgt THEN cat.ep + 1 ELSE me + 1 IN
               /\ cat' = [sh |-> tgt, ep |-> ne] /\ cm' = "done" /\ mp' = mp          \* RestoreDone ends the move
               /\ fin' = [fin EXCEPT ![tgt] = Row("active", ne)]
            /\ own' = [own EXCEPT ![tgt].st = "replaying", ![src] = Row("moved_out", me)]
            /\ actSet' = store[tgt] \cap frozenSet /\ actMk' = mk[tgt] \cap frozenMk   \* the moved rows it holds (post-activation writes are not moved rows)
            /\ UNCHANGED <<rep, DatV, idx, abt, abp, flr, mv>>
     ELSE IF cmE = "committed"                             \* the source: complete (c) here; no epoch bump
       THEN /\ seen
            /\ LET tgtAct == own[tgt].st \in {"active", "ready"} IN
               /\ cat' = IF tgtAct THEN [sh |-> tgt, ep |-> me] ELSE cat
               /\ cm' = IF tgtAct THEN "done" ELSE "committed"
               /\ mp' = IF tgtAct THEN "done" ELSE "cut"
               /\ fin' = [fin EXCEPT ![src] = Row("moved_out", me)]
               /\ own' = [own EXCEPT ![src].st = "replaying",
                                     ![tgt] = IF @.st = "ready" THEN Row("active", me) ELSE @]
               /\ actSet' = IF own[tgt].st = "ready" THEN store[tgt] ELSE actSet
               /\ actMk' = IF own[tgt].st = "ready" THEN mk[tgt] ELSE actMk
               /\ mv' = IF own[tgt].st = "ready" THEN [t2 \in Shards |-> FALSE] ELSE mv
            /\ UNCHANGED <<rep, DatV, idx, abt, abp, flr>>
     ELSE /\ mp' = mp /\ cm' = cm                         \* no open move
          /\ IF cat.sh = s                                 \* owner: new epoch
               THEN /\ cat' = [sh |-> s, ep |-> cat.ep + 1]
                    /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
               ELSE /\ cat' = cat
                    /\ fin' = [fin EXCEPT ![s] = IF s = src THEN Row("moved_out", cat.ep) ELSE Row("none", 0)]
          /\ own' = [own EXCEPT ![s].st = "replaying"]
          \* a source restored to a point before its cleanup re-runs the cleanup (restore cleanup-moved-out)
          /\ IF s = src /\ cleaned /\ cat.sh = tgt
               THEN store' = [store EXCEPT ![src] = {}] /\ mk' = [mk EXCEPT ![src] = {}] /\ ex' = [ex EXCEPT ![src] = 0]
               ELSE UNCHANGED DatV
          /\ UNCHANGED <<rep, idx, actSet, actMk, abt, abp, flr, mv>>
  /\ UNCHANGED <<mtl, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, SqV, cdirty, rview, CpV, TlV, sb>>

\* Bug variant: trust the backup row, bump the epoch if it says active, ignore the open move.
ReconcileNaive(s) ==
  /\ own[s].st = "restoring"
  /\ IF fin[s].st = "active"
       THEN /\ cat' = [sh |-> cat.sh, ep |-> cat.ep + 1]
            /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
       ELSE /\ cat' = cat /\ fin' = fin
  /\ own' = [own EXCEPT ![s].st = "replaying"]
  /\ UNCHANGED <<cm, MovV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, FlrV>>

Reconcile_(s) == IF ReconcileOnRestore THEN ReconcileDesign(s) ELSE ReconcileNaive(s)

\* restore_done (frozen/restore -> active): ends the move of a restored target (conditional `close`, N180(1)) and re-asserts the
\* catalog epoch, max(catalog, shard), once the catalog accepts writes (N185).
RestoreDone(s) ==
  /\ own[s].st = "replaying" /\ ~cdirty
  /\ LET ends == s = tgt /\ cm = "done" /\ mp \in {"committed", "cut", "tactive"} IN
     /\ mp' = IF ends THEN "done" ELSE mp
     /\ mv' = IF ends /\ RestoreCloses THEN [t2 \in Shards |-> FALSE] ELSE mv
  /\ own' = [own EXCEPT ![s] = fin[s]]
  /\ cat' = IF cat.sh = s /\ fin[s].st = "active" THEN [cat EXCEPT !.ep = Max2(cat.ep, fin[s].ep)] ELSE cat
  /\ UNCHANGED <<cm, mtl, fin, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV, sb, flr>>

-----------------------------------------------------------------------------
Copying == (\E r \in Rows : CopyStep(r)) \/ CopyEx
Forward == Freeze \/ Copying \/ VerifyFrozen \/ Recopy \/ BuildIndex \/ SealCopy \/ MakeReady \/ CommitCAS
           \/ Replicated \/ AbortReplicated \/ Thaw \/ Cut \/ Stamp \/ Activate \/ EndOnRouting \/ CatFlip
           \/ (\E s \in Shards : ReadShard(s)) \/ ApplyReconcile
Recovery == \E s \in Shards : Reconcile_(s) \/ RestoreDone(s)
Clientstep == \E c \in Clients : Commit(c)
Forced == VerifyAbort \/ WindowTimeout \/ AbortRetry
Faults == \E r \in Rows : CopyFault(r)

Next == Plan \/ Forward \/ Rollback \/ Reconnect \/ Tick \/ Cleanup \/ SweepEx \/ Faults
        \/ (\E c \in Clients : Refresh(c) \/ Commit(c) \/ (\E r \in Rows : Begin(c, r)))
        \/ (\E r \in Rows : SrcDelete(r) \/ TgtDelete(r))
        \/ (\E s \in Shards : Backup(s) \/ Restore(s) \/ Failover(s) \/ StandbyReplay(s)) \/ TgtRestore
        \/ CatalogRestore \/ CatalogLoss \/ CatalogPromote
        \/ Recovery \/ UnionRepairStep

Spec == Init /\ [][Next]_vars /\ WF_vars(Forward) /\ WF_vars(Recovery) /\ WF_vars(Clientstep)
        /\ WF_vars(Forced)

-----------------------------------------------------------------------------
(* Invariants *)

\* Never two writable owners; a writer only holds the fence at an active shard
\* (so `ready`, `incoming`, `frozen`, `moved_out`, `restoring` accept nothing).
SingleWriter ==
  /\ Cardinality({s \in Shards : own[s].st = "active"}) <= 1
  /\ \A c \in Clients : wr[c].ph = "hold" => own[wr[c].sh].st = "active"

\* The active stores.  NoLossNoDup: the target is activated with exactly the rows that were frozen at the source (no
\* loss, no duplicate); once the source is cleaned an active target holds every frozen row that it did not delete
\* itself (also after a restore behind it); and once the namespace is settled every acknowledged row that was not an
\* accepted RPO loss and not deleted on the target is on an active owner (N146).
ActiveStores == UNION {store[s] : s \in {s2 \in Shards : own[s2].st = "active"}}
NoLossNoDup ==
  /\ mp \in {"tactive", "done", "early", "early_flipped"} =>
       ((frozenSet \ gone) \subseteq actSet /\ actSet \subseteq frozenSet /\ (frozenMk \ gone) \subseteq actMk /\ actMk \subseteq frozenMk)
  \* a committed move whose source no longer holds the namespace frozen has a ready or active target with the rows
  /\ (Settled /\ ~cleaned /\ cm \in {"committed", "done"} /\ own[src].st \in {"active", "moved_out"}) =>
       (own[tgt].st \in {"ready", "active"} /\ ((frozenSet \ gone) \ lost) \subseteq store[tgt])
  /\ (cleaned /\ own[tgt].st = "active") => (frozenSet \ gone) \subseteq store[tgt]
  /\ (Settled /\ mp \in Final) => ((committed \ lost) \ gone) \subseteq ActiveStores

\* A row deleted on the active target is never again in any active store: no repair re-adds it (N161).
NoResurrect == gone \cap ActiveStores = {}

\* A target that is ready or active has its index (the first recall after activation uses the HNSW, N160(5)).
ServedFromIndex == \A s \in Shards : own[s].st \in {"ready", "active"} => idx[s]

\* Under the freeze the source's rows do not change (N160(2)).
SourceStaticUnderFreeze ==
  (own[src].st = "frozen" /\ mp \in {"frozen", "copied", "built", "sealed", "ready", "committed"}) =>
    (store[src] = frozenSet /\ mk[src] = frozenMk /\ ex[src] = frozenEx)

\* Before the point of no return the source is still the owner and holds every committed row (bar restore
\* RPO loss and rows deleted on the active target of an earlier move), so Rollback loses nothing.
RollbackPossibleBeforeC ==
  mp \in PreC =>
    /\ own[src].st \in {"active", "frozen", "restoring", "replaying"}
    /\ ((committed \ lost) \ gone) \subseteq store[src]

\* `incoming` and `ready` accept nothing: the target is never writable before the CAS.
NoWriteToTargetBeforeC == mp \in PreC => own[tgt].st # "active"

NoRouteToTargetBeforeC == (mp # "none" /\ cat.sh = tgt) => cm \in {"committed", "done"}

\* A stale-session mover never executed Freeze, the CAS or (c).
ZombieCannotCutOver == ~zcut

\* A restored shard never ends active while another shard holds the namespace at a >= epoch.
RestoreReconciles ==
  \A s \in Shards : own[s].st = "active" =>
    /\ own[s].ep >= cat.ep
    /\ \A t \in Shards \ {s} :
         /\ own[t].st \notin {"active", "ready"}
         /\ own[t].st = "moved_out" => own[t].ep <= own[s].ep

\* Whenever no recovery is running, no move is mid-flight, exactly one shard is the writable owner.
OneOwner ==
  (Settled /\ mp \in Final) => Cardinality({s \in Shards : own[s].st = "active"}) = 1

\* ... and the catalog names it at its epoch (once reconciled); while (d) is pending the CAS can still succeed.
CatalogNamesOwnerAfterDone ==
  (Settled /\ ~cdirty) =>
    /\ mp \in Final => (own[cat.sh].st = "active" /\ own[cat.sh].ep = cat.ep)
    /\ mp = "tactive" => cat \in {[sh |-> src, ep |-> me - 1], [sh |-> tgt, ep |-> own[tgt].ep]}

\* Cleanup never removes the only copy: once the source rows are gone, an active target holds what was frozen.
CleanupSafe == (cleaned /\ own[tgt].st = "active") => (frozenSet \ gone) \subseteq store[tgt]

\* N147: every row a ready or active shard holds has an ins_seq below the shard's sequence, so floors computed on
\* the shard (exports, the consolidation watermark, a second move) see the moved rows as old.
CopiedBelowTargetSeq ==
  \A s \in Shards : own[s].st \in {"ready", "active"} => \A r \in store[s] : sqt[r] < sq[s]

\* N180, C8-10: a move that has ended (done, rolled back) leaves no move bit on either ownership row.
MoveClosedWhenFinal == mp \in {"done", "rolled_back"} => \A s \in Shards : ~mv[s]

\* ... and an active owner carries no move bit once the move is over.
OwnerHasNoStaleMove == \A s \in Shards : (own[s].st = "active" /\ mv[s]) => mp \notin Final

Symm == Permutations(Clients)

\* Liveness: a started move completes or rolls back.
MoveTerminates == (mp # "none") ~> (mp \in {"done", "rolled_back"})

\* A frozen move reaches the commit point or rolls back (the window deadline, N160(2)).
FrozenBounded == (mp = "frozen") ~> (mp \in PostC \cup {"rolled_back"})

=============================================================================
