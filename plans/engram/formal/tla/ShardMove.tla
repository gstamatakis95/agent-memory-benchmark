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
(*                                                                         *)
(* D24 knobs (design: SeqAdvance, ShardTruth, TimelineGate TRUE; VerifyFirst*)
(* FALSE):                                                                 *)
(*   SeqAdvance=FALSE   Plan and (b') do not raise sq[Tgt] (N147)          *)
(*   VerifyFirst=TRUE   the check runs before the catch-up (N148)          *)
(*   ShardTruth=FALSE   the restore reconcile trusts the catalog's cm      *)
(*   TimelineGate=FALSE cleanup gated on any post-activation backup, no    *)
(*                      ReconcileIn (the D23 gate, N149)                   *)
(*   MaxCat, MaxMoves, Lag, SqSkew: catalog restores, moves of the         *)
(*   namespace, rows a promoted target may lack, initial sq[s1]            *)
(*                                                                         *)
(* D24 (N146-N149; round-5 C-3 to C-6).                                    *)
(*  - Sequences (N147).  ins_seq comes from a PER-SHARD sequence sq[s]      *)
(*    (the old global clock is gone).  A shard's ring hist[s][t] is its sq  *)
(*    at the start of tick t; every floor (engram_seq_floor) is computed on *)
(*    the SOURCE and passed as a number.  Plan and (b') set                 *)
(*    sq[Tgt] := max(sq[Tgt], sq[Src]) (SeqAdvance); CopiedBelowTargetSeq   *)
(*    says every row a ready or active shard holds has a key below its sq.  *)
(*    A second move (MaxMoves = 2) runs from the first target; without the *)
(*    advance its floors are below every moved row, the freeze re-copies    *)
(*    more than the margin window (a ghost: dt[r] + Margin < Tct) and the   *)
(*    watchdog rolls it back (FreezeTimeout).                               *)
(*  - Pre-freeze verification (N148).  The bulk copy guarantees only the    *)
(*    rows below the floor Fb taken at Plan.  CatchUp copies the rows the   *)
(*    floor selects (key >= Fb) and takes the floor Tc of its own start;    *)
(*    Verify is a check-then-abort over keys < Tc.  VerifyFirst (the old    *)
(*    order) checks before the catch-up: a row inserted under an old id     *)
(*    into an already-copied range fails the check, deterministically.      *)
(*  - Catalog loss (N146).  CatalogRestore reverts cm committed -> open (up *)
(*    to MaxCat times).  ShardTruth: ReconcileDesign first re-derives cm    *)
(*    from the ownership rows (the source moved_out, or the target active   *)
(*    at the move's epoch, mean the CAS had committed).                     *)
(*  - Target restore (N149).  TgtRestore: an asynchronous standby is        *)
(*    promoted after the CAS and loses up to Lag recent rows (never rows    *)
(*    of its latest backup).  A restore or promotion starts a new timeline  *)
(*    (tlc[s]); a backup of an older timeline cannot be restored onto it.   *)
(*    Reconcile completes a committed move with store[Tgt] u store[Src]    *)
(*    (ReconcileIn; the source is intact); after (d) the cleanup workflow   *)
(*    runs ReconcileIn itself when the target's timeline changed, and       *)
(*    Cleanup waits for a backup of Tgt that contains frozenSet and was     *)
(*    taken after that change (TimelineGate).  TimelineGate = FALSE is the  *)
(*    D23 gate: any backup taken after activation, no ReconcileIn.          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS NRows, Clients, Life, Margin, MaxT, MaxEp, MaxRestore, MaxBak, AllowAbort,
          UseReady, ReconcileOnRestore, TimelineCheck, ReconcileVerify, FencedSteps,
          IdKeyed, MergeDeletes, SweepPause, StampAfterCut, CleanupNeedsBackup,
          SeqAdvance, VerifyFirst, ShardTruth, TimelineGate, MaxCat, MaxMoves, Lag, SqSkew

Shards == {"s1", "s2"}
Other(s) == IF s = "s1" THEN "s2" ELSE "s1"
Rows == 1..NRows
PreC == {"copying", "caught", "verified", "copied", "frozen", "reconciled", "ready", "early", "early_flipped"}
Final == {"none", "rolled_back", "done"}

VARIABLES cat, cm, mp, own, fin, store, mk, ex, bak, used, sqt, ky, committed, lost,
          wr, cc, now, Tc, mtl, tl, nRestore, nBak,
          frozenSet, frozenMk, actSet, actMk, zcut, cleaned,
          src, tgt, nMoves, Fb, Tct, sq, hist, ta, dt, tlc, nCat
CatV  == <<cat, cm>>
MovV  == <<mp, Tc, mtl>>
OwnV  == <<own, fin>>
DatV  == <<store, mk, ex>>
HistV == <<used, sqt, ky, committed, lost, dt>>
CliV  == <<wr, cc>>
EnvV  == <<now, tl, nRestore, nBak, bak>>
FzV   == <<frozenSet, frozenMk, actSet, actMk, zcut, cleaned>>
RolV  == <<src, tgt, nMoves>>                 \* roles of the current move (a second move swaps them)
SqV   == <<sq, hist, ta>>                     \* per-shard sequences, their rings, tick of the last advance
FlV   == <<Fb, Tct>>                          \* floors of the current move (numbers computed on the source)
TlV   == <<tlc, nCat>>                        \* shard timelines, catalog restores
vars == <<CatV, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

Row(st, ep) == [st |-> st, ep |-> ep]
IdleW == [ph |-> "idle", r |-> 0, sh |-> "s1", age |-> 0, old |-> FALSE]
Holders(s) == {c \in Clients : wr[c].ph = "hold" /\ wr[c].sh = s}
\* Every mover step is a compare-and-set on the rows it observed (source, target, catalog move).
Fenced == cm = "open" /\ (~FencedSteps \/ (own[src].st \in {"active", "frozen"} /\ own[tgt].st = "incoming"))
SessionOk == ~TimelineCheck \/ mtl = tl

\* The re-copy key: ins_seq (drawn at Begin from the shard's sequence) in the design, created_at / entity id in the
\* old design.  Floors are numbers in the same key space, computed on the source (N147): the sequence the shard
\* had at the start of tick T (its ring), or T itself for the time-keyed old design.
KeyOf(r) == IF IdKeyed THEN ky[r] ELSE sqt[r]
FloorAt(s, T) == IF T < 0 THEN 0 ELSE IF IdKeyed THEN T ELSE hist[s][T]
CutF(s) == FloorAt(s, now - Margin)                    \* "rows older than the writer lifetime are below it"
Verified == ~ReconcileVerify \/ (store[src] = store[tgt] /\ mk[src] = mk[tgt] /\ ex[tgt] <= ex[src])
Settled == \A s \in Shards : own[s].st \notin {"restoring", "replaying"}
Max2(a, b) == IF a >= b THEN a ELSE b

\* The latest backup of s was taken after the shard's last timeline change (restore or promotion).
BakCur(s) == bak[s].tl = tlc[s]
\* Rows of the move that the intact source can still supply (not an RPO loss).
Recoverable(s) == IF s = tgt /\ cm \in {"committed", "done"} /\ ~cleaned THEN frozenSet ELSE {}

TypeOK ==
  /\ cat.sh \in Shards /\ cat.ep \in 1..MaxEp
  /\ cm \in {"none", "open", "committed", "rolled_back", "done"}
  /\ mp \in {"none", "copying", "caught", "verified", "copied", "frozen", "reconciled", "ready", "committed", "cut",
             "tactive", "done", "rolled_back", "early", "early_flipped"}
  /\ \A s \in Shards : store[s] \subseteq Rows /\ mk[s] \subseteq Rows /\ ex[s] \in 0..NRows
  /\ now \in 0..MaxT /\ src \in Shards /\ tgt = Other(src) /\ nMoves \in 0..MaxMoves

Init ==
  /\ cat = [sh |-> "s1", ep |-> 1] /\ cm = "none" /\ mp = "none"
  /\ own = [s \in Shards |-> IF s = "s1" THEN Row("active", 1) ELSE Row("none", 0)]
  /\ fin = own
  /\ store = [s \in Shards |-> {}] /\ mk = [s \in Shards |-> {}] /\ ex = [s \in Shards |-> 0]
  /\ sq = [s \in Shards |-> IF s = "s1" THEN SqSkew ELSE 0]
  /\ hist = [s \in Shards |-> [t \in 0..MaxT |-> IF s = "s1" THEN SqSkew ELSE 0]]
  /\ bak = [s \in Shards |-> [own |-> own[s], store |-> {}, mk |-> {}, ex |-> 0, tl |-> 0, sq |-> sq[s]]]
  /\ used = {} /\ sqt = [r \in Rows |-> 0] /\ ky = [r \in Rows |-> 0] /\ dt = [r \in Rows |-> 0]
  /\ committed = {} /\ lost = {}
  /\ wr = [c \in Clients |-> IdleW] /\ cc = [c \in Clients |-> cat]
  /\ now = 0 /\ Tc = 0 /\ mtl = 0 /\ tl = 0 /\ nRestore = 0 /\ nBak = 0
  /\ frozenSet = {} /\ frozenMk = {} /\ actSet = {} /\ actMk = {} /\ zcut = FALSE /\ cleaned = FALSE
  /\ src = "s1" /\ tgt = "s2" /\ nMoves = 0 /\ Fb = 0 /\ Tct = 0 /\ ta = 0
  /\ tlc = [s \in Shards |-> 0] /\ nCat = 0

-----------------------------------------------------------------------------
(* Clients, writers and the source's own sweeps *)

Refresh(c) == /\ cc' = [cc EXCEPT ![c] = cat]
              /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, wr, EnvV, FzV, RolV, SqV, FlV, TlV>>

\* A write transaction takes the shared fence and checks `active` at its epoch.  Its insert-only
\* row has ins_seq = nextval of the shard's sequence; it may carry an old entity id (a stamp for an old fact).
Begin(c, r) ==
  /\ wr[c].ph = "idle" /\ r \notin used
  /\ LET s == cc[c].sh IN own[s] = Row("active", cc[c].ep)
  /\ \E old \in (IF IdKeyed THEN BOOLEAN ELSE {FALSE}) :      \* the old-id key only matters to the old design
       /\ wr' = [wr EXCEPT ![c] = [ph |-> "hold", r |-> r, sh |-> cc[c].sh, age |-> 0, old |-> old]]
       /\ ky' = [ky EXCEPT ![r] = IF old \/ ~IdKeyed THEN 0 ELSE now]
  /\ used' = used \cup {r}
  /\ sqt' = [sqt EXCEPT ![r] = sq[cc[c].sh]]
  /\ dt' = [dt EXCEPT ![r] = IF MaxMoves > 1 THEN now ELSE 0]     \* ghost for the freeze watchdog (second moves only)
  /\ sq' = [sq EXCEPT ![cc[c].sh] = @ + 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, committed, lost, cc, EnvV, FzV, RolV, hist, ta, FlV, TlV>>

Commit(c) ==
  /\ wr[c].ph = "hold"
  /\ LET s == wr[c].sh IN LET r == wr[c].r IN
     /\ own[s].st = "active"
     /\ store' = [store EXCEPT ![s] = @ \cup {r}]
     /\ mk' = [mk EXCEPT ![s] = @ \cup {r}]
     /\ ex' = [ex EXCEPT ![s] = IF @ < NRows THEN @ + 1 ELSE @]
  /\ committed' = committed \cup {wr[c].r}
  /\ wr' = [wr EXCEPT ![c] = IdleW]
  /\ UNCHANGED <<CatV, MovV, OwnV, used, sqt, ky, dt, lost, cc, EnvV, FzV, RolV, SqV, FlV, TlV>>

\* Source-side deletes outside the expunge: the 24 h sweep of idempotency keys (mutable class), and the
\* retention sweep of the expiring class, which joins purgeable_namespaces and so pauses during a move.
SrcDelete(r) ==
  /\ own[src].st = "active" /\ r \in mk[src]
  /\ mk' = [mk EXCEPT ![src] = @ \ {r}]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, ex, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

SweepEx ==
  /\ own[src].st = "active" /\ ex[src] > 0 /\ (~SweepPause \/ cm \notin {"open", "committed"})
  /\ ex' = [ex EXCEPT ![src] = @ - 1]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

Tick ==
  /\ now < MaxT /\ \A c \in Clients : wr[c].ph = "idle" \/ wr[c].age < Life
  /\ now' = now + 1
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" THEN [wr[c] EXCEPT !.age = @ + 1] ELSE wr[c]]
  /\ hist' = [s \in Shards |-> [hist[s] EXCEPT ![now + 1] = sq[s]]]
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, cc, tl, nRestore, nBak, bak, FzV, RolV, sq, ta, FlV, TlV>>

-----------------------------------------------------------------------------
(* The mover *)

\* sq[t] := max(sq[t], sq[s]) and a ring sample right afterwards (N147: engram_seq_advance at Plan and (b')).
Advance(s, t) == /\ sq' = IF SeqAdvance THEN [sq EXCEPT ![t] = Max2(sq[t], sq[s])] ELSE sq
                 /\ hist' = IF SeqAdvance THEN [hist EXCEPT ![t][now] = Max2(sq[t], sq[s])] ELSE hist

Plan ==
  /\ mp \in {"none", "done"} /\ nMoves < MaxMoves /\ (nMoves = 0 \/ (cleaned /\ now > ta + Margin))
  /\ LET s == cat.sh IN LET t == Other(s) IN
     /\ own[s] = Row("active", cat.ep) /\ cat.ep < MaxEp
     /\ own[t].st \in {"none", "moved_out"} /\ store[t] = {} /\ mk[t] = {} /\ ex[t] = 0
     /\ src' = s /\ tgt' = t
     /\ own' = [own EXCEPT ![t] = Row("incoming", cat.ep + 1)]
     /\ Advance(s, t)
     /\ Fb' = CutF(s)                                  \* floor at the start of the bulk copy, computed on the source
  /\ nMoves' = nMoves + 1
  /\ mp' = "copying" /\ cm' = "open"
  /\ Tc' = 0 /\ mtl' = tl /\ Tct' = now
  /\ frozenSet' = {} /\ frozenMk' = {} /\ actSet' = {} /\ actMk' = {} /\ cleaned' = FALSE
  /\ UNCHANGED <<cat, fin, DatV, HistV, CliV, EnvV, zcut, ta, TlV>>

\* Dirty range copy: any non-empty part of what Src has and Tgt lacks (READ COMMITTED ranges), the
\* mutable class (upserts only) and the expiring class (once).
CopyRange ==
  /\ mp = "copying" /\ Fenced
  /\ \E X \in SUBSET (store[src] \ store[tgt]) :
       /\ X # {} /\ store' = [store EXCEPT ![tgt] = @ \cup X]
  /\ UNCHANGED <<CatV, MovV, OwnV, mk, ex, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

CopyMk ==
  /\ mp = "copying" /\ Fenced /\ mk' = [mk EXCEPT ![tgt] = @ \cup mk[src]]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, ex, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

CopyEx ==
  /\ mp = "copying" /\ Fenced /\ ex' = [ex EXCEPT ![tgt] = ex[src]]
  /\ UNCHANGED <<CatV, MovV, OwnV, store, mk, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

\* What the bulk copy guarantees when it has finished: every row below the floor taken at its start is on the target
\* (rows inserted later under old ids into ranges already copied are not).
BulkDone == \A r \in store[src] : KeyOf(r) < Fb => r \in store[tgt]
\* The pre-freeze check: every insert-only row below the floor F is on the target.
Check(F) == \A r \in store[src] : KeyOf(r) < F => r \in store[tgt]

\* Catch-up (namespace still active): copy the rows the floor selects (key >= Fb); Tc is the floor of the catch-up's
\* own start, bounding the re-copy under the freeze.
CatchUp ==
  /\ mp = (IF VerifyFirst THEN "verified" ELSE "copying") /\ Fenced /\ BulkDone
  /\ store' = [store EXCEPT ![tgt] = @ \cup {r \in store[src] : KeyOf(r) >= Fb}]
  /\ Tc' = CutF(src) /\ Tct' = now
  /\ mp' = IF VerifyFirst THEN "copied" ELSE "caught"
  /\ UNCHANGED <<CatV, mtl, OwnV, mk, ex, HistV, CliV, EnvV, FzV, RolV, SqV, Fb, TlV>>

\* Verify is a check-then-abort: it passes, or the move rolls back (VerifyAbort).  Design: after the catch-up, over keys
\* below the catch-up's floor.  VerifyFirst: before it, over keys below the floor of now.
VerifyOK ==
  IF VerifyFirst THEN mp = "copying" /\ BulkDone /\ Check(CutF(src)) ELSE mp = "caught" /\ Check(Tc)
Verify ==
  /\ Fenced /\ VerifyOK
  /\ mp' = IF VerifyFirst THEN "verified" ELSE "copied"
  /\ UNCHANGED <<CatV, Tc, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>
VerifyFails ==
  IF VerifyFirst THEN mp = "copying" /\ BulkDone /\ ~Check(CutF(src)) ELSE mp = "caught" /\ ~Check(Tc)

Freeze ==
  /\ mp = "copied" /\ Fenced /\ own[src].st = "active" /\ Holders(src) = {} /\ SessionOk
  /\ own' = [own EXCEPT ![src].st = "frozen"] /\ mp' = "frozen"
  /\ frozenSet' = store[src] /\ frozenMk' = mk[src]
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<CatV, Tc, mtl, fin, DatV, HistV, CliV, EnvV, actSet, actMk, cleaned, RolV, SqV, FlV, TlV>>

\* Under freeze: re-copy insert-only rows with key >= Tc; merge-diff the mutable class.  The work is bounded by the
\* watchdog: a selection that reaches back beyond the margin window (the whole namespace, when the source's own
\* floors are below the moved rows) rolls the move back (FreezeTimeout).
Work == {r \in store[src] : KeyOf(r) >= Tc}
Excess == MaxMoves > 1 /\ \E r \in Work : dt[r] + Margin < Tct      \* dt is tracked only when a second move is possible
Reconcile ==
  /\ mp = "frozen" /\ Fenced /\ (own[src].st = "frozen" \/ ~FencedSteps) /\ ~Excess
  /\ store' = [store EXCEPT ![tgt] = @ \cup Work]
  /\ mk' = [mk EXCEPT ![tgt] = IF MergeDeletes THEN mk[src] ELSE @ \cup mk[src]]
  /\ mp' = "reconciled"
  /\ UNCHANGED <<CatV, Tc, mtl, OwnV, ex, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

MakeReady ==                                                 \* (b')
  /\ mp = "reconciled" /\ Fenced /\ (own[src].st = "frozen" \/ ~FencedSteps) /\ Verified
  /\ Advance(src, tgt) /\ ta' = now                          \* the target's sequence is raised before it is readable
  /\ IF UseReady
       THEN /\ own' = [own EXCEPT ![tgt].st = "ready"] /\ mp' = "ready"
            /\ UNCHANGED <<actSet, actMk>>
       ELSE /\ own' = [own EXCEPT ![tgt].st = "active"] /\ mp' = "early"      \* old order: no ready state
            /\ actSet' = store[tgt] /\ actMk' = mk[tgt]
  /\ UNCHANGED <<CatV, Tc, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, zcut, cleaned, RolV, FlV, TlV>>

\* (a''): the catalog CAS, the point of no return; it also compares the session's timeline.
CommitCAS ==
  /\ mp = "ready" /\ ~StampAfterCut /\ cm = "open" /\ SessionOk
  /\ (~FencedSteps \/ (own[src].st = "frozen" /\ own[tgt].st = "ready"))   \* the rows the mover verified
  /\ cm' = "committed" /\ mp' = "committed"
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, Tc, mtl, OwnV, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, actSet, actMk, cleaned, RolV, SqV, FlV, TlV>>

\* (c): Src frozen -> moved_out, only after the mover read `committed` (design).
Cut ==
  /\ own[src].st = "frozen" /\ SessionOk
  /\ \/ mp = "committed" /\ cm = "committed"
     \/ mp \in {"early", "early_flipped"}
     \/ StampAfterCut /\ mp = "ready"
  /\ own' = [own EXCEPT ![src] = Row("moved_out", own[tgt].ep)]
  /\ mp' = CASE mp = "committed" -> "cut" [] mp = "ready" -> "cut" [] mp = "early" -> "tactive" [] OTHER -> "done"
  /\ cm' = IF mp = "early" THEN "committed" ELSE IF mp = "early_flipped" THEN "done" ELSE cm
  /\ zcut' = (zcut \/ mtl # tl)
  /\ UNCHANGED <<cat, Tc, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, actSet, actMk, cleaned, RolV, SqV, FlV, TlV>>

\* Old-order bookkeeping: the stamp that records (c) in the catalog after the fact.
Stamp ==
  /\ StampAfterCut /\ mp \in {"cut", "tactive"} /\ cm = "open"
  /\ cm' = "committed"
  /\ UNCHANGED <<cat, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

Activate ==                                                  \* (b'')
  /\ mp = "cut" /\ own[tgt].st = "ready"
  /\ own' = [own EXCEPT ![tgt].st = "active"] /\ mp' = "tactive"
  /\ actSet' = store[tgt] /\ actMk' = mk[tgt]
  /\ UNCHANGED <<CatV, Tc, mtl, fin, DatV, HistV, CliV, EnvV, frozenSet, frozenMk, zcut, cleaned, RolV, SqV, FlV, TlV>>

\* (d): catalog flip WHERE epoch = e AND state = frozen; early order flips before (c).
CatFlip ==
  /\ \/ mp = "tactive" /\ cat = [sh |-> src, ep |-> own[tgt].ep - 1]
     \/ mp = "early"
  /\ cat' = [sh |-> tgt, ep |-> own[tgt].ep]
  /\ mp' = IF mp = "tactive" THEN "done" ELSE "early_flipped"
  /\ cm' = IF mp = "tactive" THEN "done" ELSE cm
  /\ UNCHANGED <<Tc, mtl, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

\* ReconcileIn before the cleanup (N149): when the target's timeline changed after the move was done, the cleanup
\* workflow first re-copies from the source (still intact) what the promoted target lacks, then takes the backup.
\* (TimelineGate = FALSE: the D23 prose, which has no such step.)
ReconcileIn ==
  /\ TimelineGate /\ mp = "done" /\ ~cleaned /\ tlc[tgt] > 0 /\ Settled
  /\ own[tgt].st = "active" /\ own[src].st = "moved_out" /\ ~(frozenSet \subseteq store[tgt])
  /\ store' = [store EXCEPT ![tgt] = @ \cup store[src]]
  /\ mk' = [mk EXCEPT ![tgt] = @ \cup mk[src]]
  /\ ex' = [ex EXCEPT ![tgt] = IF ex[tgt] >= ex[src] THEN @ ELSE ex[src]]
  /\ Advance(src, tgt)
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, FzV, RolV, ta, FlV, TlV>>

\* Cleanup (24 h after done): the source's data rows go; the moved_out ownership row stays.  The gate (N143(5),
\* N149): a backup of the target that contains the moved rows and was taken after activation and after the target's
\* last timeline change.  TimelineGate = FALSE: any backup taken after activation (the D23 gate).
Cleanup ==
  /\ mp = "done" /\ ~cleaned /\ own[tgt].st = "active" /\ own[src].st = "moved_out"
  /\ (~CleanupNeedsBackup \/ (bak[tgt].own = own[tgt]
                              /\ (~TimelineGate \/ (frozenSet \subseteq bak[tgt].store /\ BakCur(tgt)))))
  /\ store' = [store EXCEPT ![src] = {}] /\ mk' = [mk EXCEPT ![src] = {}] /\ ex' = [ex EXCEPT ![src] = 0]
  /\ cleaned' = TRUE
  /\ UNCHANGED <<CatV, MovV, OwnV, HistV, CliV, EnvV, frozenSet, frozenMk, actSet, actMk, zcut, RolV, SqV, FlV, TlV>>

\* Rollback at any step before the point of no return: abort CAS open -> rolled_back, thaw the source,
\* drop the target, undo a premature flip.
RollbackA ==
  /\ mp \in PreC /\ cm = "open" /\ Holders(tgt) = {}
  /\ mp' = "rolled_back" /\ cm' = "rolled_back"
  /\ own' = [own EXCEPT ![src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @,
                        ![tgt] = Row("none", 0)]
  /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
  /\ cat' = IF mp = "early_flipped" THEN [sh |-> src, ep |-> own[src].ep] ELSE cat
  /\ UNCHANGED <<Tc, mtl, fin, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>
AbortAny == AllowAbort /\ RollbackA
StuckRollback == mp = "reconciled" /\ ~Verified /\ RollbackA
VerifyAbort == VerifyFails /\ RollbackA                      \* the check failed: roll back
FreezeTimeout == mp = "frozen" /\ Excess /\ RollbackA        \* the freeze work exceeds the watchdog
Rollback == AbortAny \/ StuckRollback \/ VerifyAbort \/ FreezeTimeout

\* The mover restarts (Temporal) and re-reads the shard's timeline.
Reconnect ==
  /\ mp \in PreC /\ mtl # tl /\ mtl' = tl
  /\ UNCHANGED <<CatV, mp, Tc, OwnV, DatV, HistV, CliV, now, tl, nRestore, nBak, bak, FzV, RolV, SqV, FlV, TlV>>

-----------------------------------------------------------------------------
(* Backups, restore and failover (N123) *)

Backup(s) ==
  /\ nBak < MaxBak /\ own[s].st \notin {"restoring", "replaying"}
  /\ bak' = [bak EXCEPT ![s] = [own |-> own[s], store |-> store[s], mk |-> mk[s], ex |-> ex[s], tl |-> tlc[s], sq |-> sq[s]]]
  /\ nBak' = nBak + 1
  /\ UNCHANGED <<CatV, MovV, OwnV, DatV, HistV, CliV, now, tl, nRestore, FzV, RolV, SqV, FlV, TlV>>

\* Restore to the last backup, or failover to a replica (which may or may not lag): the shard rejects everything
\* until Reconcile and RestoreDone.  A lossless failover keeps the state and the row.  Every restore or promotion
\* is a timeline change of the shard (tlc), which a backup taken before it does not reflect.
Restore(s) ==
  /\ nRestore < MaxRestore /\ own[s].st \notin {"restoring", "replaying"} /\ cat.ep < MaxEp
  /\ \A o \in Shards : tlc[o] = 0 \/ o = s            \* repeated faults hit one shard (a fault of both copies is outside the model)
  /\ \E ll \in BOOLEAN :
       /\ IF ll
            THEN /\ UNCHANGED <<DatV, lost, sq>> /\ fin' = [fin EXCEPT ![s] = own[s]]
            ELSE /\ lost' = lost \cup ((store[s] \ bak[s].store) \ Recoverable(s))
                 /\ store' = [store EXCEPT ![s] = bak[s].store] /\ mk' = [mk EXCEPT ![s] = bak[s].mk]
                 /\ ex' = [ex EXCEPT ![s] = bak[s].ex]
                 /\ sq' = [sq EXCEPT ![s] = bak[s].sq]
                 /\ fin' = [fin EXCEPT ![s] = bak[s].own]
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" /\ wr[c].sh = s THEN IdleW ELSE wr[c]]
  /\ own' = [own EXCEPT ![s] = Row("restoring", own[s].ep)]
  /\ tl' = IF s = src THEN tl + 1 ELSE tl
  /\ tlc' = [tlc EXCEPT ![s] = @ + 1]
  /\ nRestore' = nRestore + 1
  /\ UNCHANGED <<CatV, MovV, used, sqt, ky, dt, committed, cc, now, nBak, bak, FzV, RolV, hist, ta, FlV, nCat>>

\* N149: an asynchronous standby of the target is promoted after the catalog CAS.  It lacks up to Lag of the newest
\* rows (moved rows included while the 24 h grace has not elapsed, i.e. before the cleanup) and may still hold the
\* ownership row `ready`; the promotion is a timeline change.
TgtRestore ==
  /\ Lag > 0 /\ nRestore < MaxRestore /\ cat.ep < MaxEp
  /\ \A o \in Shards : tlc[o] = 0 \/ o = tgt
  /\ cm \in {"committed", "done"} /\ own[tgt].st \in {"ready", "active"}
  /\ \E X \in SUBSET (store[tgt] \ (IF cleaned THEN frozenSet ELSE {})) :
       /\ Cardinality(X) <= Lag
       /\ store' = [store EXCEPT ![tgt] = @ \ X] /\ mk' = [mk EXCEPT ![tgt] = @ \ X]
       /\ lost' = lost \cup (X \ Recoverable(tgt))
  /\ \E row \in {own[tgt], Row("ready", own[tgt].ep)} : fin' = [fin EXCEPT ![tgt] = row]
  /\ wr' = [c \in Clients |-> IF wr[c].ph = "hold" /\ wr[c].sh = tgt THEN IdleW ELSE wr[c]]
  /\ own' = [own EXCEPT ![tgt] = Row("restoring", own[tgt].ep)]
  /\ tlc' = [tlc EXCEPT ![tgt] = @ + 1]
  /\ nRestore' = nRestore + 1
  /\ UNCHANGED <<CatV, MovV, ex, used, sqt, ky, dt, committed, cc, now, tl, nBak, bak, FzV, RolV, SqV, FlV, nCat>>

\* N146: the catalog loses the committed CAS (restore from backup).
CatalogRestore ==
  /\ nCat < MaxCat /\ cm = "committed"
  /\ cm' = "open" /\ nCat' = nCat + 1
  /\ UNCHANGED <<cat, MovV, OwnV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, tlc>>

\* What the ownership rows say about the catalog's move (N146): the source row moved_out, or the target row active at
\* the move's epoch, can only exist after the CAS committed.
DerivedCommitted(s) ==
  IF s = tgt THEN own[src].st = "moved_out" \/ fin[tgt] = Row("active", own[tgt].ep)
  ELSE own[tgt].st = "active" \/ fin[src] = Row("moved_out", own[tgt].ep)

\* Settle the open move against the catalog, then take the final row.  With ShardTruth the catalog row is first
\* re-derived from the ownership rows (a catalog restored from backup is repaired from the shards).
ReconcileDesign(s) ==
  /\ own[s].st = "restoring"
  /\ LET cmE == IF ShardTruth /\ cm = "open" /\ DerivedCommitted(s) THEN "committed" ELSE cm IN
     IF cmE = "open"                                       \* before the point of no return: abort CAS, roll back
       THEN /\ cm' = "rolled_back" /\ mp' = "rolled_back"
            /\ store' = [store EXCEPT ![tgt] = {}] /\ mk' = [mk EXCEPT ![tgt] = {}] /\ ex' = [ex EXCEPT ![tgt] = 0]
            /\ IF s = src
                 THEN /\ cat' = [sh |-> src, ep |-> cat.ep + 1]               \* owner restored: epoch bump
                      /\ fin' = [fin EXCEPT ![src] = Row("active", cat.ep + 1)]
                      /\ own' = [own EXCEPT ![src].st = "replaying", ![tgt] = Row("none", 0)]
                 ELSE /\ cat' = IF mp = "early_flipped" THEN [sh |-> src, ep |-> own[src].ep] ELSE cat
                      /\ fin' = [fin EXCEPT ![tgt] = Row("none", 0)]
                      /\ own' = [own EXCEPT ![tgt].st = "replaying",
                                            ![src] = IF @.st = "frozen" THEN Row("active", @.ep) ELSE @]
            /\ UNCHANGED <<actSet, actMk, sq, hist>>
       ELSE IF cmE = "committed"                            \* committed: complete (c), (b''), (d) here; no epoch bump
       THEN LET e1 == own[tgt].ep IN
            /\ cat' = [sh |-> tgt, ep |-> e1] /\ cm' = "done" /\ mp' = "done"
            /\ IF s = tgt
                 THEN \* ReconcileIn: the source is intact; re-copy what the restored target lacks, raise its sequence
                      /\ store' = [store EXCEPT ![tgt] = @ \cup store[src]]
                      /\ mk' = [mk EXCEPT ![tgt] = IF mp = "tactive" THEN @ \cup mk[src] ELSE mk[src]]
                      /\ ex' = [ex EXCEPT ![tgt] = IF mp = "tactive" /\ ex[tgt] >= ex[src] THEN @ ELSE ex[src]]
                      /\ Advance(src, tgt)
                      /\ IF mp = "tactive"
                           THEN UNCHANGED <<actSet, actMk>>
                           ELSE /\ actSet' = store[tgt] \cup store[src] /\ actMk' = mk[src]
                      /\ fin' = [fin EXCEPT ![tgt] = Row("active", e1)]
                      /\ own' = [own EXCEPT ![tgt].st = "replaying", ![src] = Row("moved_out", e1)]
                 ELSE /\ fin' = [fin EXCEPT ![src] = Row("moved_out", e1)]
                      /\ own' = [own EXCEPT ![src].st = "replaying",
                                            ![tgt] = IF @.st = "active" THEN @ ELSE Row("active", e1)]
                      /\ UNCHANGED <<sq, hist>>
                      /\ IF mp = "tactive"
                           THEN UNCHANGED <<DatV, actSet, actMk>>
                           ELSE /\ actSet' = store[tgt] /\ actMk' = mk[tgt] /\ UNCHANGED DatV
       ELSE /\ mp' = mp /\ cm' = cm                         \* no open move
            /\ IF cat.sh = s                                \* owner: new epoch
                 THEN /\ cat' = [sh |-> s, ep |-> cat.ep + 1]
                      /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
                 ELSE /\ cat' = cat
                      /\ fin' = [fin EXCEPT ![s] = IF s = src THEN Row("moved_out", cat.ep) ELSE Row("none", 0)]
            /\ own' = [own EXCEPT ![s].st = "replaying"]
            \* a target restored to a backup of the copy phase has rows above its sequence: re-run the advance (N147)
            /\ IF s = tgt /\ cm = "done" /\ ~cleaned THEN Advance(src, tgt) ELSE UNCHANGED <<sq, hist>>
            /\ UNCHANGED <<DatV, actSet, actMk>>
  /\ UNCHANGED <<Tc, mtl, HistV, CliV, EnvV, frozenSet, frozenMk, zcut, cleaned, RolV, ta, FlV, TlV>>

\* Bug variant: trust the backup row, bump the epoch if it says active, ignore the open move.
ReconcileNaive(s) ==
  /\ own[s].st = "restoring"
  /\ IF fin[s].st = "active"
       THEN /\ cat' = [sh |-> cat.sh, ep |-> cat.ep + 1]
            /\ fin' = [fin EXCEPT ![s] = Row("active", cat.ep + 1)]
       ELSE /\ cat' = cat /\ fin' = fin
  /\ own' = [own EXCEPT ![s].st = "replaying"]
  /\ UNCHANGED <<cm, MovV, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

Reconcile_(s) == IF ReconcileOnRestore THEN ReconcileDesign(s) ELSE ReconcileNaive(s)

RestoreDone(s) ==
  /\ own[s].st = "replaying"
  /\ own' = [own EXCEPT ![s] = fin[s]]
  /\ UNCHANGED <<CatV, MovV, fin, DatV, HistV, CliV, EnvV, FzV, RolV, SqV, FlV, TlV>>

-----------------------------------------------------------------------------
Forward == ReconcileIn \/ Freeze \/ CatchUp \/ Verify \/ Reconcile \/ MakeReady \/ CommitCAS \/ Cut \/ Stamp \/ Activate \/ CatFlip
Recovery == \E s \in Shards : Reconcile_(s) \/ RestoreDone(s)
Clientstep == \E c \in Clients : Commit(c)
Copying == CopyRange \/ CopyMk \/ CopyEx
Forced == StuckRollback \/ VerifyAbort \/ FreezeTimeout

Next == Plan \/ Copying \/ Forward \/ Rollback \/ Reconnect \/ Tick \/ Cleanup \/ SweepEx
        \/ (\E c \in Clients : Refresh(c) \/ Commit(c) \/ (\E r \in Rows : Begin(c, r)))
        \/ (\E r \in Rows : SrcDelete(r))
        \/ (\E s \in Shards : Backup(s) \/ Restore(s)) \/ TgtRestore \/ CatalogRestore \/ Recovery

Spec == Init /\ [][Next]_vars /\ WF_vars(Forward) /\ WF_vars(Recovery) /\ WF_vars(Clientstep)
        /\ WF_vars(Forced) /\ WF_vars(Copying)

-----------------------------------------------------------------------------
(* Invariants *)

\* Never two writable owners; a writer only holds the fence at an active shard
\* (so `ready`, `incoming`, `frozen`, `moved_out`, `restoring` accept nothing).
SingleWriter ==
  /\ Cardinality({s \in Shards : own[s].st = "active"}) <= 1
  /\ \A c \in Clients : wr[c].ph = "hold" => own[wr[c].sh].st = "active"

\* A done move whose source is still intact: what a promoted target lacks is re-copied by the cleanup workflow.
Repairable == mp = "done" /\ ~cleaned /\ own[src].st = "moved_out" /\ own[tgt].st = "active"

\* The target is activated with exactly the insert-only rows and mutable keys that were frozen at the source; an
\* source is cleaned an active target holds every frozen row (also after a promotion behind it, N149); and once the
\* namespace is settled every acknowledged row that was not an accepted RPO loss is on an active owner (N146).
ActiveStores == UNION {store[s] : s \in {s2 \in Shards : own[s2].st = "active"}}
NoLossNoDup ==
  /\ mp \in {"tactive", "done", "early", "early_flipped"} => (actSet = frozenSet /\ actMk = frozenMk)
  /\ (cleaned /\ own[tgt].st = "active") => frozenSet \subseteq store[tgt]
  /\ (Settled /\ mp \in Final) =>
       ((committed \ lost) \ (IF Repairable THEN frozenSet ELSE {})) \subseteq ActiveStores

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

\* Whenever no recovery is running and no move is mid-flight, exactly one shard is the writable owner.
OneOwner ==
  (Settled /\ mp \in Final) => Cardinality({s \in Shards : own[s].st = "active"}) = 1

\* ... and the catalog names it at its epoch; while (d) is pending the CAS can still succeed.
CatalogNamesOwnerAfterDone ==
  Settled =>
    /\ mp \in Final => (own[cat.sh].st = "active" /\ own[cat.sh].ep = cat.ep)
    /\ mp = "tactive" => cat = [sh |-> src, ep |-> own[tgt].ep - 1]

\* Cleanup never removes the only copy: once the source rows are gone, an active target holds what was frozen.
CleanupSafe == (cleaned /\ own[tgt].st = "active") => frozenSet \subseteq store[tgt]

\* N147: every row a ready or active shard holds has an ins_seq below the shard's sequence, so floors computed on
\* the shard (exports, the consolidation watermark, a second move) see the moved rows as old.
CopiedBelowTargetSeq ==
  \A s \in Shards : own[s].st \in {"ready", "active"} => \A r \in store[s] : sqt[r] < sq[s]

\* Liveness: a started move completes or rolls back.
Symm == Permutations(Clients)

MoveTerminates == (mp # "none") ~> (mp \in {"done", "rolled_back"})

\* Liveness with active writers (no restore, no voluntary abort): every started move completes; a
\* deterministic rollback (missed rows, deleted keys, a sweep during the move, a check before the catch-up,
\* a freeze that re-copies the whole namespace) violates it.
MoveTerminatesActive == (mp # "none") ~> (mp = "done")

=============================================================================
