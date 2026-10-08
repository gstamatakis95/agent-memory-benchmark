---------------------------- MODULE ShardMove ----------------------------
(***************************************************************************)
(* Engram D25 (N160 to N163, N168): moving one namespace from Src to Tgt   *)
(* by freeze, then copy, then verify, then index, then cut over.           *)
(*                                                                         *)
(* Protocol.  The catalog row of the move, cm, is the arbiter:             *)
(*   open --(a'') CAS--> committed --> done        (point of no return)    *)
(*   open --abort CAS--> rolled_back               (mover, window, restore)*)
(* Mover (mp): Plan (planned) -> Freeze (exclusive fence; the source is    *)
(* static from here to thaw or cleanup) -> CopyStep* / CopyEx (frozen) ->  *)
(* VerifyFrozen (copied) -> BuildIndex (built) -> (b') Tgt incoming->ready *)
(* -> (a'') CAS open->committed -> Replicated (the standby has the commit) *)
(* -> (c) Src frozen->moved_out (only after the mover read `committed`    *)
(* and the commit is replicated) -> (b'') Tgt ready->active -> (d) catalog *)
(* flip -> Cleanup (after a full target backup started after activation).  *)
(*                                                                         *)
(* There is no dirty copy, no catch-up, no floor and no merge: the source  *)
(* does not change while it is copied, so "what is missing" is a set       *)
(* difference.  Three table classes (N137) are all copied alike under the  *)
(* freeze: an insert-only row r (ledger), a mutable key r (idempotency     *)
(* key; the source may sweep it before the freeze) and a counter of the    *)
(* expiring class (token usage, copied once, verified `<=`).               *)
(*                                                                         *)
(* After the commit point a target is repaired by re-running the copy from *)
(* the retained source (Rerun) or not at all; nothing is merged (N161).    *)
(* TgtDelete is a legitimate delete on the active target (expunge, purge,  *)
(* key sweep, C-1); `gone` is the set of such rows, which are durable: the *)
(* intent replay (Durability.tla) re-applies them after every restore, so  *)
(* Restore and Rerun produce `... \ gone`.  NoResurrect says no repair     *)
(* ever puts a gone row back into an active store.                         *)
(*                                                                         *)
(* Restore / failover of either shard (N123): the shard reverts to its     *)
(* last backup (or, lossless failover, keeps its state), comes up          *)
(* `restoring`, reconciles the open move against the catalog by CAS        *)
(* (open: roll back; committed: complete (c), (b''), (d) itself, or, for a *)
(* target whose restored row is none/incoming/ready, mark a Rerun), bumps  *)
(* the catalog epoch only when it owns the namespace with no committed     *)
(* move, replays intents (Durability.tla) and takes its final row.         *)
(*                                                                         *)
(* Catalog (N163): CatalogLoss is a promotion of the asynchronous standby  *)
(* (loses the commit while it is not replicated); CatalogRestore is a      *)
(* restore from backup (loses cm and cat); CatalogReconcile re-derives     *)
(* them from the ownership rows before any shard fault.                    *)
(*                                                                         *)
(* Knobs (design: all TRUE except StampAfterCut, CopyBeforeFreeze,         *)
(* ReadyBeforeIndex, UnionRepair, RerunMerges, AllowAbort):                *)
(*   UseReady=FALSE           Tgt goes active before (c); catalog may flip *)
(*   ReconcileOnRestore=FALSE restore trusts its backup row, bumps epoch   *)
(*   ReconcileVerify=FALSE    VerifyFrozen compares nothing (CopyFault ok) *)
(*   FencedSteps=FALSE        mover steps do not check the rows they saw   *)
(*   SweepPause=FALSE         the expiring-class sweep runs under a freeze *)
(*   StampAfterCut=TRUE       (c) first, the catalog stamp afterwards      *)
(*   CleanupNeedsBackup=FALSE cleanup without a backup of the target       *)
(*   GateAfterActivation=FALSE the gate takes a backup started before      *)
(*                            activation                                   *)
(*   SeqAdvance=FALSE         Freeze does not raise sq[Tgt] (N147)         *)
(*   ShardTruth=FALSE         no catalog reconcile; the shard reconcile    *)
(*                            trusts the catalog's cm                      *)
(*   CopyBeforeFreeze=TRUE    copy from the unfrozen source, no catch-up   *)
(*   ReadyBeforeIndex=TRUE    Tgt may become ready without its index       *)
(*   CutNeedsReplicated=FALSE (c) before the commit is replicated          *)
(*   UnionRepair=TRUE         a restored, serving target is repaired by    *)
(*                            store[Tgt] u store[Src]                      *)
(*   RerunMerges=TRUE         the re-run is a union, not wipe-and-copy     *)
(*   AllowAbort               the mover may roll back at any pre-commit step*)
(*   MaxCat, MaxMoves, MaxFault, SqSkew, Window: catalog losses and        *)
(*   restores, moves of the namespace, copy faults, initial sq[s1], the    *)
(*   freeze window in ticks                                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS NRows, Clients, Life, MaxT, Window, MaxEp, MaxRestore, MaxBak, AllowAbort,
          UseReady, ReconcileOnRestore, ReconcileVerify, FencedSteps,
          SweepPause, StampAfterCut, CleanupNeedsBackup, GateAfterActivation,
          SeqAdvance, ShardTruth, MaxCat, MaxMoves, MaxFault, SqSkew,
          CopyBeforeFreeze, ReadyBeforeIndex, CutNeedsReplicated, UnionRepair, RerunMerges

Shards == {"s1", "s2"}
Other(s) == IF s = "s1" THEN "s2" ELSE "s1"
Rows == 1..NRows
PreC == {"planned", "frozen", "copied", "built", "ready", "early", "early_flipped"}
PostC == {"committed", "cut", "tactive", "done"}
Final == {"none", "rolled_back", "done"}

VARIABLES cat, cm, mp, own, fin, store, mk, ex, bak, used, sqt, committed, lost, gone,
          wr, cc, now, mtl, tl, nRestore, nBak,
          frozenSet, frozenMk, frozenEx, actSet, actMk, zcut, cleaned,
          src, tgt, nMoves, me, sq, tlc, nCat, idx, rep, rr, cdirty,
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
FlagV == <<rep, rr, cdirty>>
CpV   == <<cdn, cex, rc, nFault, fzt>>
TlV   == <<tlc, nCat>>
vars == <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

Row(st, ep) == [st |-> st, ep |-> ep]
IdleW == [ph |-> "idle", r |-> 0, sh |-> "s1", age |-> 0]
Holders(s) == {c \in Clients : wr[c].ph = "hold" /\ wr[c].sh = s}
\* Every mover step is a compare-and-set on the rows it observed (source, target, catalog move).
Fenced == cm = "open" /\ (~FencedSteps \/ (own[src].st \in {"active", "frozen"} /\ own[tgt].st = "incoming"))
SrcFrozen == ~FencedSteps \/ own[src].st = "frozen"
SessionOk == mtl = tl
Settled == \A s \in Shards : own[s].st \notin {"restoring", "replaying"}
Max2(a, b) == IF a >= b THEN a ELSE b

\* The copy of the current move is complete: every row of the (static) source has been copied once, and the counter.
CopyDone == (store[src] \cup mk[src]) \subseteq cdn /\ cex
\* VerifyFrozen: per table equality (count and key hash) of the insert-only and mutable classes, `<=` for the counter.
Verified == ~ReconcileVerify \/ (store[src] = store[tgt] /\ mk[src] = mk[tgt] /\ ex[tgt] <= ex[src])
CopyPhase == IF CopyBeforeFreeze THEN mp = "planned" ELSE (mp = "frozen" /\ SrcFrozen)

\* Rows that the intact source can still supply (not an RPO loss).
Recoverable(s) ==
  IF cleaned THEN {}
  ELSE IF s = tgt /\ cm \in {"committed", "done"} THEN frozenSet
  ELSE IF s = src /\ (cm \in {"committed", "done"} \/ mp \in {"cut", "tactive", "done"}) THEN frozenSet
  ELSE {}

TypeOK ==
  /\ cat.sh \in Shards /\ cat.ep \in 1..MaxEp
  /\ cm \in {"none", "open", "committed", "rolled_back", "done"}
  /\ mp \in {"none", "planned", "frozen", "copied", "built", "ready", "committed", "cut",
             "tactive", "done", "rolled_back", "early", "early_flipped"}
  /\ \A s \in Shards : store[s] \subseteq Rows /\ mk[s] \subseteq Rows /\ ex[s] \in 0..NRows
  /\ now \in 0..MaxT /\ src \in Shards /\ tgt = Other(src) /\ nMoves \in 0..MaxMoves
  /\ rc \in 0..1 /\ nFault \in 0..MaxFault /\ cdn \subseteq Rows

Init ==
  /\ cat = [sh |-> "s1", ep |-> 1] /\ cm = "none" /\ mp = "none"
  /\ own = [s \in Shards |-> IF s = "s1" THEN Row("active", 1) ELSE Row("none", 0)]
  /\ fin = own
  /\ store = [s \in Shards |-> {}] /\ mk = [s \in Shards |-> {}] /\ ex = [s \in Shards |-> 0]
  /\ sq = [s \in Shards |-> IF s = "s1" THEN SqSkew ELSE 0]
  /\ idx = [s \in Shards |-> s = "s1"]
  /\ bak = [s \in Shards |-> [own |-> own[s], store |-> {}, mk |-> {}, ex |-> 0, sq |-> sq[s], idx |-> idx[s]]]
  /\ used = {} /\ sqt = [r \in Rows |-> 0]
  /\ committed = {} /\ lost = {} /\ gone = {}
  /\ wr = [c \in Clients |-> IdleW] /\ cc = [c \in Clients |-> cat]
  /\ now = 0 /\ mtl = 0 /\ tl = 0 /\ nRestore = 0 /\ nBak = 0
  /\ frozenSet = {} /\ frozenMk = {} /\ frozenEx = 0 /\ actSet = {} /\ actMk = {} /\ zcut = FALSE /\ cleaned = FALSE
  /\ src = "s1" /\ tgt = "s2" /\ nMoves = 0 /\ me = 1
  /\ tlc = [s \in Shards |-> 0] /\ nCat = 0
  /\ rep = FALSE /\ rr = FALSE /\ cdirty = FALSE
  /\ cdn = {} /\ cex = FALSE /\ rc = 0 /\ nFault = 0 /\ fzt = 0

-----------------------------------------------------------------------------
(* Clients, writers and the source's own sweeps *)

Refresh(c) == /\ ~cdirty
              /\ cc' = [cc EXCEPT ![c] = cat]
              /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, wr, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* A write transaction takes the shared fence and checks `active` at its epoch.  Its insert-only
\* row has ins_seq = nextval of the shard's sequence.
Begin(c, r) ==
  /\ wr[c].ph = "idle" /\ r \notin used
  /\ LET s == cc[c].sh IN own[s] = Row("active", cc[c].ep)
  /\ wr' = [wr EXCEPT ![c] = [ph |-> "hold", r |-> r, sh |-> cc[c].sh, age |-> 0]]
  /\ used' = used \cup {r}
  /\ sqt' = [sqt EXCEPT ![r] = sq[cc[c].sh]]
  /\ sq' = [sq EXCEPT ![cc[c].sh] = @ + 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, committed, lost, gone, cc, EnvV, FzV, RolV, IdxV, FlagV, CpV, TlV>>

Commit(c) ==
  /\ wr[c].ph = "hold"
  /\ LET s == wr[c].sh IN LET r == wr[c].r IN
     /\ own[s].st = "active"
     /\ store' = [store EXCEPT ![s] = @ \cup {r}]
     /\ mk' = [mk EXCEPT ![s] = @ \cup {r}]
     /\ ex' = [ex EXCEPT ![s] = IF @ < NRows THEN @ + 1 ELSE @]
  /\ committed' = committed \cup {wr[c].r}
  /\ wr' = [wr EXCEPT ![c] = IdleW]
  /\ UNCHANGED <<CatV, MovV, OwnV, used, sqt, lost, gone, cc, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* Source-side deletes of the mutable class (the 24 h sweep of idempotency keys).  An active source only: under
\* the freeze nothing is deleted (N160).
SrcDelete(r) ==
  /\ own[src].st = "active" /\ r \in mk[src]
  /\ mk' = [mk EXCEPT ![src] = @ \ {r}]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* A legitimate delete on the active target (document expunge, retire purge, key sweep, compaction; C-1).  The
\* row stays on the source until its cleanup; the delete is durable (intent), see the header.
TgtDelete(r) ==
  /\ own[tgt].st = "active" /\ mp \in {"tactive", "done"} /\ r \in store[tgt]
  /\ store' = [store EXCEPT ![tgt] = @ \ {r}]
  /\ mk' = [mk EXCEPT ![tgt] = @ \ {r}]
  /\ gone' = gone \cup {r}
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, used, sqt, committed, lost, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* The retention sweep of the expiring class.  It joins purgeable_namespaces and so pauses during a move; with
\* SweepPause = FALSE it also runs under the freeze (a source that is not static).
SweepEx ==
  /\ ex[src] > 0
  /\ \/ own[src].st = "active" /\ (~SweepPause \/ cm \notin {"open", "committed"})
     \/ ~SweepPause /\ own[src].st = "frozen"
  /\ ex' = [ex EXCEPT ![src] = @ - 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

Tick ==
  /\ now < MaxT /\ \A c \in Clients : wr[c].ph = "idle" \/ wr[c].age < Life
  /\ now' = now + 1
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" THEN [wr[c] EXCEPT !.age = @ + 1] ELSE wr[c]]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, cc, tl, nRestore, nBak, bak, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

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
  /\ nMoves' = nMoves + 1
  /\ mp' = "planned" /\ cm' = "open"
  /\ mtl' = tl
  /\ frozenSet' = {} /\ frozenMk' = {} /\ frozenEx' = 0 /\ actSet' = {} /\ actMk' = {} /\ cleaned' = FALSE
  /\ rep' = FALSE /\ rr' = FALSE
  /\ cdn' = {} /\ cex' = FALSE /\ rc' = 0 /\ fzt' = 0
  /\ UNCHANGED <<cat, fin, DatV, HistV, CliV, EnvV, zcut, SqV, cdirty, nFault, TlV>>

\* Freeze (exclusive fence, one attempt, no holder).  The source is static from here.  The one engram_seq_advance
\* call of the move follows directly (no source write exists in between, N147).
Freeze ==
  /\ mp = "planned" /\ Fenced /\ own[src].st = "active" /\ Holders(src) = {} /\ SessionOk
  /\ own' = [own EXCEPT ![src].st = "frozen"]
  /\ mp' = IF CopyBeforeFreeze THEN "copied" ELSE "frozen"
  /\ frozenSet' = store[src] /\ frozenMk' = mk[src] /\ frozenEx' = ex[src]
  /\ sq' = IF SeqAdvance THEN [sq EXCEPT ![tgt] = Max2(sq[tgt], sq[src])] ELSE sq
  /\ fzt' = now
  /\ UNCHANGED <<CatV, mtl, fin, DatV, HistV, CliV, EnvV, actSet, actMk, zcut, cleaned, RolV, IdxV, FlagV, cdn, cex, rc, nFault, TlV>>

\* Copy one row of the static source (COPY into a temp table, upsert under the incoming fence).  CopyBeforeFreeze
\* is the rejected variant: the same copy from an active source.
CopyStep(r) ==
  /\ Fenced /\ CopyPhase
  /\ r \in (store[src] \cup mk[src]) \ cdn
  /\ store' = IF r \in store[src] THEN [store EXCEPT ![tgt] = @ \cup {r}] ELSE store
  /\ mk' = IF r \in mk[src] THEN [mk EXCEPT ![tgt] = @ \cup {r}] ELSE mk
  /\ cdn' = cdn \cup {r}
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, cex, rc, nFault, fzt, TlV>>

CopyEx ==
  /\ Fenced /\ CopyPhase /\ ~cex
  /\ ex' = [ex EXCEPT ![tgt] = ex[src]]
  /\ cex' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, cdn, rc, nFault, fzt, TlV>>

\* A fault of the copy, to give VerifyFrozen something to catch: a dropped row, or a stale mutable key.
CopyFault(r) ==
  /\ Fenced /\ CopyPhase /\ nFault < MaxFault
  /\ \/ /\ r \in store[tgt] \cup mk[tgt]
        /\ store' = [store EXCEPT ![tgt] = @ \ {r}] /\ mk' = [mk EXCEPT ![tgt] = @ \ {r}]
     \/ /\ r \notin mk[src] /\ r \notin mk[tgt]
        /\ mk' = [mk EXCEPT ![tgt] = @ \cup {r}] /\ store' = store
  /\ nFault' = nFault + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, cdn, cex, rc, fzt, TlV>>

\* VerifyFrozen on the complete copy.  A mismatch re-copies once (Recopy), a second mismatch rolls back (VerifyAbort).
VerifyFrozen ==
  /\ mp = "frozen" /\ Fenced /\ SrcFrozen /\ CopyDone /\ Verified
  /\ mp' = "copied"
  /\ UNCHANGED <<CatV, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>
VerifyFails == mp = "frozen" /\ Fenced /\ SrcFrozen /\ CopyDone /\ ~Verified
Recopy ==
  /\ VerifyFails /\ rc = 0
  /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
  /\ cdn' = {} /\ cex' = FALSE /\ rc' = 1
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, nFault, fzt, TlV>>

\* BuildIndexes under the freeze: the index runner builds the HNSW of every vector table; readiness is the index.
BuildIndex ==
  /\ mp = "copied" /\ Fenced /\ SrcFrozen
  /\ idx' = [idx EXCEPT ![tgt] = TRUE] /\ mp' = "built"
  /\ UNCHANGED <<CatV, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlagV, CpV, TlV>>

MakeReady ==                                                 \* (b')
  /\ mp = (IF ReadyBeforeIndex THEN "copied" ELSE "built") /\ Fenced /\ SrcFrozen
  /\ IF UseReady
       THEN /\ own' = [own EXCEPT ![tgt].st = "ready"] /\ mp' = "ready"
            /\ UNCHANGED <<actSet, actMk>>
       ELSE /\ own' = [own EXCEPT ![tgt].st = "active"] /\ mp' = "early"      \* old order: no ready state
            /\ actSet' = store[tgt] /\ actMk' = mk[tgt]
  /\ UNCHANGED <<CatV, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* (a''): the catalog CAS, the point of no return.  Retried when a catalog loss reverted it.
CommitCAS ==
  /\ mp \in {"ready", "committed"} /\ ~StampAfterCut /\ cm = "open" /\ SessionOk /\ ~rr
  /\ (~FencedSteps \/ (own[src].st = "frozen" /\ own[tgt].st = "ready"))   \* the rows the mover verified
  /\ cm' = "committed" /\ mp' = "committed" /\ rep' = FALSE
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, mtl, OwnV, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, actSet, actMk, cleaned, RolV, SqV, IdxV, rr, cdirty, CpV, TlV>>

\* The asynchronous standby has replayed the commit (N163(3)).
Replicated ==
  /\ cm = "committed" /\ ~rep
  /\ rep' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rr, cdirty, CpV, TlV>>

\* (c): Src frozen -> moved_out, only after the mover read `committed` and (design) the commit is replicated.
Cut ==
  /\ own[src].st = "frozen" /\ SessionOk /\ ~rr
  /\ (~FencedSteps \/ own[tgt].st \in {"ready", "active"})
  /\ \/ mp = "committed" /\ cm = "committed" /\ (rep \/ ~CutNeedsReplicated)
     \/ mp \in {"early", "early_flipped"}
     \/ StampAfterCut /\ mp = "ready"
  /\ own' = [own EXCEPT ![src] = Row("moved_out", me)]
  /\ mp' = CASE mp = "committed" -> "cut" [] mp = "ready" -> "cut" [] mp = "early" -> "tactive" [] OTHER -> "done"
  /\ cm' = IF mp = "early" THEN "committed" ELSE IF mp = "early_flipped" THEN "done" ELSE cm
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, actSet, actMk, cleaned, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* Old-order bookkeeping: the stamp that records (c) in the catalog after the fact.
Stamp ==
  /\ StampAfterCut /\ mp \in {"cut", "tactive"} /\ cm = "open"
  /\ cm' = "committed"
  /\ UNCHANGED <<cat, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

Activate ==                                                  \* (b'')
  /\ mp = "cut" /\ own[tgt].st = "ready" /\ ~rr
  /\ own' = [own EXCEPT ![tgt].st = "active"] /\ mp' = "tactive"
  /\ actSet' = store[tgt] /\ actMk' = mk[tgt]
  /\ UNCHANGED <<CatV, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* (d): catalog flip WHERE epoch = e AND state = frozen; early order flips before (c).  After a catalog
\* reconcile the flip has already been derived from the shards.
CatFlip ==
  /\ ~rr
  /\ \/ mp = "tactive" /\ cat = [sh |-> src, ep |-> me - 1]
     \/ mp = "tactive" /\ cat = [sh |-> tgt, ep |-> own[tgt].ep]
     \/ mp = "early"
  /\ cat' = [sh |-> tgt, ep |-> own[tgt].ep]
  /\ mp' = IF mp = "tactive" THEN "done" ELSE "early_flipped"
  /\ cm' = IF mp = "tactive" THEN "done" ELSE cm
  /\ UNCHANGED <<mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* Re-run (N161(2)): the restore reconcile found a target whose restored row is none, incoming or ready although the
\* catalog move is committed.  Wipe, FrozenCopy from the retained (static) source, VerifyFrozen, BuildIndexes,
\* reconcile_in to active, then the intent replay of the acknowledged deletes (`\ gone`).  RerunMerges is the
\* rejected variant: a union on top of the replayed target, which re-adds the rows the target deleted.
Rerun ==
  /\ rr /\ Settled /\ ~cleaned
  /\ own[tgt].st \in {"none", "incoming", "ready"} /\ own[src].st \in {"frozen", "moved_out"}
  /\ LET cp == IF RerunMerges THEN store[tgt] \cup store[src] ELSE store[src] IN
     /\ store' = [store EXCEPT ![tgt] = IF RerunMerges THEN cp ELSE cp \ gone]
     /\ mk' = [mk EXCEPT ![tgt] = IF RerunMerges THEN mk[tgt] \cup mk[src] ELSE mk[src] \ gone]
     /\ actSet' = cp /\ actMk' = IF RerunMerges THEN mk[tgt] \cup mk[src] ELSE mk[src]
  /\ ex' = [ex EXCEPT ![tgt] = Max2(ex[tgt], ex[src])]
  /\ idx' = [idx EXCEPT ![tgt] = TRUE]
  /\ sq' = IF SeqAdvance THEN [sq EXCEPT ![tgt] = Max2(sq[tgt], sq[src])] ELSE sq
  /\ own' = [own EXCEPT ![tgt] = Row("active", me), ![src] = Row("moved_out", me)]
  /\ fin' = [fin EXCEPT ![tgt] = Row("active", me), ![src] = Row("moved_out", me)]
  /\ cat' = [sh |-> tgt, ep |-> me] /\ cm' = "done" /\ mp' = "done" /\ rr' = FALSE
  /\ UNCHANGED <<mtl, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, rep, cdirty, CpV, TlV>>

\* The rejected D24 repair: a cleanup-time ReconcileIn against a serving target that was restored or promoted.
UnionRepairStep ==
  /\ UnionRepair /\ mp = "done" /\ ~cleaned /\ tlc[tgt] > 0 /\ Settled
  /\ own[tgt].st = "active" /\ own[src].st = "moved_out" /\ ~(store[src] \subseteq store[tgt])
  /\ store' = [store EXCEPT ![tgt] = @ \cup store[src]]
  /\ mk' = [mk EXCEPT ![tgt] = @ \cup mk[src]]
  /\ UNCHANGED <<CatV, MovV, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* Cleanup (N161(1)): the source's data rows go; the moved_out ownership row stays.  The gate: a full backup of the
\* target that started after activation (the 24 h timer is an action here).  GateAfterActivation = FALSE accepts a
\* backup that started before activation (incoming or ready).
Cleanup ==
  /\ mp = "done" /\ ~cleaned /\ own[tgt].st = "active" /\ own[src].st = "moved_out"
  /\ (~CleanupNeedsBackup \/ (bak[tgt].own.ep = own[tgt].ep
                              /\ (bak[tgt].own.st = "active"
                                  \/ (~GateAfterActivation /\ bak[tgt].own.st \in {"incoming", "ready"}))))
  /\ store' = [store EXCEPT ![src] = {}] /\ mk' = [mk EXCEPT ![src] = {}] /\ ex' = [ex EXCEPT ![src] = 0]
  /\ idx' = [idx EXCEPT ![src] = FALSE]
  /\ cleaned' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, actSet, actMk, zcut, RolV, SqV, FlagV, CpV, TlV>>

\* Rollback at any step before the point of no return: abort CAS open -> rolled_back, thaw the source,
\* drop the target and its indexes, undo a premature flip.
RollbackA ==
  /\ mp \in PreC /\ cm = "open" /\ Holders(tgt) = {}
  /\ mp' = "rolled_back" /\ cm' = "rolled_back"
  /\ own' = [own EXCEPT ![src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @,
                        ![tgt] = Row("none", 0)]
  /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
  /\ idx' = [idx EXCEPT ![tgt] = FALSE]
  /\ cat' = IF mp = "early_flipped" THEN [sh |-> src, ep |-> own[src].ep] ELSE cat
  /\ UNCHANGED <<mtl, fin, HistV, CliV, EnvV, FzV, RolV, SqV, FlagV, CpV, TlV>>
AbortAny == AllowAbort /\ RollbackA
VerifyAbort == VerifyFails /\ rc = 1 /\ RollbackA                         \* the second mismatch: MoveVerifyFailed
WindowTimeout == mp \in {"frozen", "copied", "built"} /\ now >= fzt + Window /\ RollbackA   \* MoveWindowExceeded
Rollback == AbortAny \/ VerifyAbort \/ WindowTimeout

\* The mover restarts (Temporal) and re-reads the shard's timeline.
Reconnect ==
  /\ mp \in PreC /\ mtl # tl /\ mtl' = tl
  /\ UNCHANGED <<CatV, mp, OwnV, DatV, HistV, CliV, now, tl, nRestore, nBak, bak, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

-----------------------------------------------------------------------------
(* Backups, restore and failover (N123, N161) *)

Backup(s) ==
  /\ nBak < MaxBak /\ own[s].st \notin {"restoring", "replaying"}
  /\ bak' = [bak EXCEPT ![s] = [own |-> own[s], store |-> store[s], mk |-> mk[s], ex |-> ex[s], sq |-> sq[s], idx |-> idx[s]]]
  /\ nBak' = nBak + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, now, tl, nRestore, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

\* Restore to the last backup, or failover to a replica that keeps everything: the shard rejects everything
\* until Reconcile and RestoreDone.  A lossy restore re-applies the acknowledged deletes (intent replay).  Faults are
\* not combined with a dirty catalog: the catalog reconcile runs first (N163(1)).
RestoreCore(s) ==
  /\ nRestore < MaxRestore /\ own[s].st \notin {"restoring", "replaying"} /\ cat.ep < MaxEp /\ ~cdirty
  /\ \A o \in Shards : tlc[o] = 0 \/ o = s            \* repeated faults hit one shard (a fault of both copies is outside the model)
  /\ \E ll \in BOOLEAN :
       IF ll
         THEN /\ UNCHANGED <<DatV, lost, sq, idx>> /\ fin' = [fin EXCEPT ![s] = own[s]]
         ELSE /\ lost' = lost \cup ((store[s] \ bak[s].store) \ Recoverable(s))
              /\ store' = [store EXCEPT ![s] = bak[s].store \ gone]
              /\ mk' = [mk EXCEPT ![s] = bak[s].mk \ gone]
              /\ ex' = [ex EXCEPT ![s] = bak[s].ex]
              /\ sq' = [sq EXCEPT ![s] = bak[s].sq]
              /\ idx' = [idx EXCEPT ![s] = bak[s].idx]
              /\ fin' = [fin EXCEPT ![s] = bak[s].own]
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" /\ wr[c].sh = s THEN IdleW ELSE wr[c]]
  /\ own' = [own EXCEPT ![s] = Row("restoring", own[s].ep)]
  /\ tl' = IF s = src THEN tl + 1 ELSE tl
  /\ tlc' = [tlc EXCEPT ![s] = @ + 1]
  /\ nRestore' = nRestore + 1
  /\ UNCHANGED <<CatV, MovV, used, sqt, committed, gone, cc, now, nBak, bak, FzV, RolV, FlagV, CpV, nCat>>

TgtPhase == cm \in {"committed", "done"} /\ own[tgt].st \in {"ready", "active"}
Restore(s) == ~(s = tgt /\ TgtPhase) /\ RestoreCore(s)
\* The target restored or promoted after the commit point, to bak[tgt], which may predate activation (N161(2)).
TgtRestore == TgtPhase /\ RestoreCore(tgt)

\* N163(3): the catalog promotion loses the commit while it is not yet replicated.
CatalogLoss ==
  /\ nCat < MaxCat /\ cm = "committed" /\ ~rep
  /\ cm' = "open" /\ nCat' = nCat + 1
  /\ UNCHANGED <<cat, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, tlc>>

\* N146, N163(5): a catalog restore from backup (RPO 60 s) reverts the move row and the routing row.
CatalogRestore ==
  /\ nCat < MaxCat /\ cm \in {"committed", "done"} /\ Settled
  /\ cm' = "open"
  /\ cat' = IF cat.sh = tgt THEN [sh |-> src, ep |-> me - 1] ELSE cat
  /\ cdirty' = ShardTruth
  /\ nCat' = nCat + 1
  /\ UNCHANGED <<MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rep, rr, CpV, tlc>>

\* engramctl catalog reconcile --from-shards: the routing row from the unique owner, the move row from the source's
\* moved_out or the target's active row.
CatalogReconcile ==
  /\ ShardTruth /\ cdirty /\ Settled
  /\ LET owners == {s \in Shards : own[s].st \in {"active", "frozen"}} IN
       cat' = IF owners = {} THEN cat ELSE LET o == CHOOSE s \in owners : TRUE IN [sh |-> o, ep |-> own[o].ep]
  /\ cm' = IF cm = "open" /\ (own[src].st = "moved_out" \/ own[tgt].st = "active")
             THEN (IF mp = "done" THEN "done" ELSE "committed") ELSE cm
  /\ cdirty' = FALSE
  /\ UNCHANGED <<MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, rep, rr, CpV, TlV>>

\* What the ownership rows say about the catalog's move (N146): the source row moved_out, or the target row active at
\* the move's epoch, can only exist after the CAS committed.
DerivedCommitted(s) ==
  IF s = tgt THEN own[src].st = "moved_out" \/ fin[tgt] = Row("active", me)
  ELSE own[tgt].st = "active" \/ fin[src] = Row("moved_out", me)

\* Settle the open move against the catalog, then take the final row.  With ShardTruth the catalog row is first
\* re-derived from the ownership rows.
ReconcileDesign(s) ==
  /\ own[s].st = "restoring"
  /\ LET cmE == IF ShardTruth /\ cm = "open" /\ DerivedCommitted(s) THEN "committed" ELSE cm IN
     LET needRerun == s = tgt /\ ~cleaned /\ cmE \in {"committed", "done"} /\ fin[tgt].st \in {"none", "incoming", "ready"} IN
     IF needRerun                                          \* committed, but the target came back before activation: re-run
       THEN /\ cm' = cmE /\ rr' = TRUE
            /\ own' = [own EXCEPT ![s].st = "replaying"]
            /\ UNCHANGED <<mp, cat, fin, store, mk, ex, idx, actSet, actMk>>
     ELSE IF cmE = "open"                                  \* before the point of no return: abort CAS, roll back
       THEN /\ cm' = "rolled_back" /\ mp' = "rolled_back" /\ rr' = FALSE
            /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
            /\ idx' = [idx EXCEPT ![tgt] = FALSE]
            /\ IF s = src
                 THEN /\ cat' = [sh |-> src, ep |-> cat.ep + 1]               \* owner restored: epoch bump
                      /\ fin' = [fin EXCEPT ![src] = Row("active", cat.ep + 1)]
                      /\ own' = [own EXCEPT ![src].st = "replaying", ![tgt] = Row("none", 0)]
                 ELSE /\ cat' = IF mp = "early_flipped" THEN [sh |-> src, ep |-> own[src].ep] ELSE cat
                      /\ fin' = [fin EXCEPT ![tgt] = Row("none", 0)]
                      /\ own' = [own EXCEPT ![tgt].st = "replaying",
                                            ![src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @]
            /\ UNCHANGED <<actSet, actMk>>
     ELSE IF cmE = "committed"                             \* committed: complete (c), (b''), (d) here; no epoch bump
       THEN /\ rr' = rr
            /\ IF s = tgt                                  \* restored active: ordinary, the data is the backup
                 THEN /\ cat' = [sh |-> tgt, ep |-> me] /\ cm' = "done" /\ mp' = "done"
                      /\ fin' = [fin EXCEPT ![tgt] = Row("active", me)]
                      /\ own' = [own EXCEPT ![tgt].st = "replaying", ![src] = Row("moved_out", me)]
                      /\ UNCHANGED <<DatV, idx, actSet, actMk>>
                 ELSE LET tgtAct == own[tgt].st \in {"active", "ready"} IN
                      /\ cat' = IF tgtAct THEN [sh |-> tgt, ep |-> me] ELSE cat
                      /\ cm' = IF tgtAct THEN "done" ELSE "committed"
                      /\ mp' = IF tgtAct THEN "done" ELSE "cut"
                      /\ fin' = [fin EXCEPT ![src] = Row("moved_out", me)]
                      /\ own' = [own EXCEPT ![src].st = "replaying",
                                            ![tgt] = IF @.st = "ready" THEN Row("active", me) ELSE @]
                      /\ actSet' = IF own[tgt].st = "ready" THEN store[tgt] ELSE actSet
                      /\ actMk' = IF own[tgt].st = "ready" THEN mk[tgt] ELSE actMk
                      /\ UNCHANGED <<DatV, idx>>
     ELSE /\ mp' = mp /\ cm' = cm /\ rr' = rr              \* no open move
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
          /\ UNCHANGED <<idx, actSet, actMk>>
  /\ UNCHANGED <<mtl, HistV, CliV, EnvV, frozenSet, frozenMk, frozenEx, zcut, cleaned, RolV, SqV, cdirty, CpV, TlV>>

\* Bug variant: trust the backup row, bump the epoch if it says active, ignore the open move.
ReconcileNaive(s) ==
  /\ own[s].st = "restoring"
  /\ IF fin[s].st = "active"
       THEN /\ cat' = [sh |-> cat.sh, ep |-> cat.ep + 1]
            /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
       ELSE /\ cat' = cat /\ fin' = fin
  /\ own' = [own EXCEPT ![s].st = "replaying"]
  /\ UNCHANGED <<cm, MovV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

Reconcile_(s) == IF ReconcileOnRestore THEN ReconcileDesign(s) ELSE ReconcileNaive(s)

RestoreDone(s) ==
  /\ own[s].st = "replaying"
  /\ own' = [own EXCEPT ![s] = fin[s]]
  /\ UNCHANGED <<CatV, MovV, fin, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, IdxV, FlagV, CpV, TlV>>

-----------------------------------------------------------------------------
Copying == (\E r \in Rows : CopyStep(r)) \/ CopyEx
Forward == Freeze \/ Copying \/ VerifyFrozen \/ Recopy \/ BuildIndex \/ MakeReady \/ CommitCAS \/ Replicated
           \/ Cut \/ Stamp \/ Activate \/ CatFlip \/ Rerun \/ CatalogReconcile
Recovery == \E s \in Shards : Reconcile_(s) \/ RestoreDone(s)
Clientstep == \E c \in Clients : Commit(c)
Forced == VerifyAbort \/ WindowTimeout
Faults == \E r \in Rows : CopyFault(r)

Next == Plan \/ Forward \/ Rollback \/ Reconnect \/ Tick \/ Cleanup \/ SweepEx \/ Faults
        \/ (\E c \in Clients : Refresh(c) \/ Commit(c) \/ (\E r \in Rows : Begin(c, r)))
        \/ (\E r \in Rows : SrcDelete(r) \/ TgtDelete(r))
        \/ (\E s \in Shards : Backup(s) \/ Restore(s)) \/ TgtRestore \/ CatalogRestore \/ CatalogLoss
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
  /\ mp \in {"tactive", "done", "early", "early_flipped"} => (actSet = frozenSet /\ actMk = frozenMk)
  /\ (cleaned /\ own[tgt].st = "active") => (frozenSet \ gone) \subseteq store[tgt]
  /\ (Settled /\ mp \in Final /\ ~rr) => ((committed \ lost) \ gone) \subseteq ActiveStores

\* A row deleted on the active target is never again in any active store: no repair re-adds it (N161).
NoResurrect == gone \cap ActiveStores = {}

\* A target that is ready or active has its index (the first recall after activation uses the HNSW, N160(5)).
ServedFromIndex == \A s \in Shards : own[s].st \in {"ready", "active"} => idx[s]

\* Under the freeze the source's rows do not change (N160(2)).
SourceStaticUnderFreeze ==
  (own[src].st = "frozen" /\ mp \in {"frozen", "copied", "built", "ready", "committed"}) =>
    (store[src] = frozenSet /\ mk[src] = frozenMk /\ ex[src] = frozenEx)

\* Before the point of no return the source is still the owner and holds every committed row (bar restore
\* RPO loss), so Rollback loses nothing.
RollbackPossibleBeforeC ==
  mp \in PreC =>
    /\ own[src].st \in {"active", "frozen", "restoring", "replaying"}
    /\ (committed \ lost) \subseteq store[src]

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

\* Whenever no recovery is running, no re-run is pending and no move is mid-flight, exactly one shard is the writable owner.
OneOwner ==
  (Settled /\ mp \in Final /\ ~rr) => Cardinality({s \in Shards : own[s].st = "active"}) = 1

\* ... and the catalog names it at its epoch (once reconciled); while (d) is pending the CAS can still succeed.
CatalogNamesOwnerAfterDone ==
  (Settled /\ ~cdirty /\ ~rr) =>
    /\ mp \in Final => (own[cat.sh].st = "active" /\ own[cat.sh].ep = cat.ep)
    /\ mp = "tactive" => cat \in {[sh |-> src, ep |-> me - 1], [sh |-> tgt, ep |-> own[tgt].ep]}

\* Cleanup never removes the only copy: once the source rows are gone, an active target holds what was frozen.
CleanupSafe == (cleaned /\ own[tgt].st = "active") => (frozenSet \ gone) \subseteq store[tgt]

\* N147: every row a ready or active shard holds has an ins_seq below the shard's sequence, so floors computed on
\* the shard (exports, the consolidation watermark, a second move) see the moved rows as old.
CopiedBelowTargetSeq ==
  \A s \in Shards : own[s].st \in {"ready", "active"} => \A r \in store[s] : sqt[r] < sq[s]

Symm == Permutations(Clients)

\* Liveness: a started move completes or rolls back.
MoveTerminates == (mp # "none") ~> (mp \in {"done", "rolled_back"})

\* A frozen move reaches the commit point or rolls back (the window deadline, N160(2)).
FrozenBounded == (mp = "frozen") ~> (mp \in PostC \cup {"rolled_back"})

=============================================================================
