---------------------------- MODULE ShardMove ----------------------------
(***************************************************************************)
(* Engram D2/D4/D5/N2: moving a namespace between shards with per-namespace *)
(* epoch fencing.                                                          *)
(*                                                                         *)
(* Three stores take part: the catalog (truth for routing), the source     *)
(* shard and the target shard (each with its own namespace_ownership row,  *)
(* the fence that every write transaction locks FOR SHARE).  Clients cache *)
(* (shard, epoch) and may be stale; workflows pin (shard, epoch) at start   *)
(* and are re-pinned only when the mover restarts them.  The mover (a      *)
(* Temporal workflow on the target's queue, N2) persists its state in       *)
(* namespace_moves and may crash and restart at any step; it may roll back *)
(* at any step before cutover.                                             *)
(*                                                                         *)
(* Writes commit out of order with respect to their outbox seq, exactly as *)
(* in Outbox.tla; the move consumer is assumed to see a seq only once it   *)
(* is resolved (committed, or declared aborted by the watchlist).          *)
(*                                                                         *)
(* Design knobs:                                                           *)
(*   CopyBarrier = TRUE  : the copy snapshot is taken while no write       *)
(*       transaction holds the source fence (mover takes FOR UPDATE on the *)
(*       ownership row, opens the REPEATABLE READ snapshot, releases).     *)
(*       FALSE = D5 step 2 as first written (p0 = max visible seq):       *)
(*       a write that drew a smaller seq and commits after the snapshot    *)
(*       is neither copied nor replayed (NoLossNoDup violated).           *)
(*   CutoverOrder = "safe" : target active -> source moved_out -> catalog   *)
(*       switch -> workflows restarted.  "d5" = target active -> catalog   *)
(*       switch -> source moved_out: a stale reader at the frozen source   *)
(*       misses writes already accepted at the target (ReadsFresh).        *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Shards,        \* {s1, s2}
          NS,            \* namespaces, e.g. {n1, n2}
          Movable,       \* namespaces the mover may move (subset of NS)
          Clients,       \* API instances with a catalog cache, e.g. {c1, c2}
          Workflows,     \* Temporal workflows with pinned routing, e.g. {w1}
          Writes,        \* pool of write ids (facts/links/observations), e.g. {x1, x2, x3}
          MaxEpoch,      \* epochs are 1..MaxEpoch
          MaxMoves,      \* move attempts per behaviour
          MaxCrashes,    \* mover crashes per behaviour
          CopyBarrier,   \* BOOLEAN
          CutoverOrder,  \* "safe" | "d5"
          NoActor        \* model value: actor of a write that has not begun

ASSUME Movable \subseteq NS /\ CutoverOrder \in {"safe", "d5"} /\ NoActor \notin Clients \cup Workflows

Actors == Clients \cup Workflows
MoveStates == {"none", "planned", "copying", "catching_up", "frozen", "drained",
               "cut_target", "cut_catalog", "cut_source", "done", "rolled_back"}
OwnStates == {"none", "incoming", "active", "frozen", "moved_out"}

VARIABLES
  cat,       \* [NS -> [shard, epoch, state]]           catalog truth
  own,       \* [Shards -> [NS -> [epoch, state]]]      shard-side ownership rows
  cache,     \* [Actors -> [NS -> [shard, epoch]]]      cached / pinned routing
  store,     \* [Shards -> [NS -> [Writes -> Nat]]]     bag of applied writes
  wst,       \* [Writes -> {"idle","holding","committed"}]
  winfo,     \* [Writes -> [shard, ns, seq, actor]]
  nextSeq,   \* [Shards -> Nat]                          outbox sequence per shard
  accepted,  \* [NS -> SUBSET Writes]                    writes acked to clients
  mv,        \* mover record: [st, ns, src, dst, p0, applied, epoch]
  moverUp,   \* BOOLEAN
  moves, crashes,
  readViolation  \* BOOLEAN, set by an accepted read that was stale or misrouted (monotone)

vars == <<cat, own, cache, store, wst, winfo, nextSeq, accepted, mv, moverUp, moves, crashes, readViolation>>

Other(s) == CHOOSE t \in Shards : t /= s
Home(n) == cat[n].shard
Bag0 == [w \in Writes |-> 0]

TypeOK ==
  /\ cat \in [NS -> [shard : Shards, epoch : 1..MaxEpoch, state : {"active", "moving", "frozen"}]]
  /\ own \in [Shards -> [NS -> [epoch : 0..MaxEpoch, state : OwnStates]]]
  /\ cache \in [Actors -> [NS -> [shard : Shards, epoch : 1..MaxEpoch]]]
  /\ store \in [Shards -> [NS -> [Writes -> 0..2]]]
  /\ wst \in [Writes -> {"idle", "holding", "committed"}]
  /\ mv.st \in MoveStates
  /\ moverUp \in BOOLEAN

Init ==
  /\ cat = [n \in NS |-> [shard |-> CHOOSE s \in Shards : TRUE, epoch |-> 1, state |-> "active"]]
  /\ own = [s \in Shards |-> [n \in NS |-> IF s = cat[n].shard THEN [epoch |-> 1, state |-> "active"]
                                                               ELSE [epoch |-> 0, state |-> "none"]]]
  /\ cache = [a \in Actors |-> [n \in NS |-> [shard |-> cat[n].shard, epoch |-> 1]]]
  /\ store = [s \in Shards |-> [n \in NS |-> Bag0]]
  /\ wst = [w \in Writes |-> "idle"]
  /\ winfo = [w \in Writes |-> [shard |-> CHOOSE s \in Shards : TRUE, ns |-> CHOOSE n \in NS : TRUE, seq |-> 0, actor |-> NoActor]]
  /\ nextSeq = [s \in Shards |-> 1]
  /\ accepted = [n \in NS |-> {}]
  /\ mv = [st |-> "none", ns |-> CHOOSE n \in NS : TRUE, src |-> CHOOSE s \in Shards : TRUE,
           dst |-> CHOOSE s \in Shards : TRUE, p0 |-> 0, applied |-> {}, epoch |-> 1]
  /\ moverUp = TRUE
  /\ moves = 0
  /\ crashes = 0
  /\ readViolation = FALSE

-----------------------------------------------------------------------------
(* Routing and the shard-side fence *)

\* The write transaction's first statement: lock the ownership row FOR SHARE
\* with state = 'active' and the caller's epoch.
FenceOK(s, n, e) == own[s][n].state = "active" /\ own[s][n].epoch = e

Holding(s, n) == {w \in Writes : wst[w] = "holding" /\ winfo[w].shard = s /\ winfo[w].ns = n}

\* Begin a write at the cached route: the fence either admits it (the
\* transaction now holds FOR SHARE and has drawn its outbox seq) or rejects
\* it with FAILED_PRECONDITION; a rejected API client refreshes its cache,
\* a rejected workflow just retries later with its pinned route.
BeginWrite(a, n, w) ==
  /\ wst[w] = "idle"
  /\ LET r == cache[a][n] IN
     IF FenceOK(r.shard, n, r.epoch)
       THEN /\ wst' = [wst EXCEPT ![w] = "holding"]
            /\ winfo' = [winfo EXCEPT ![w] = [shard |-> r.shard, ns |-> n, seq |-> nextSeq[r.shard], actor |-> a]]
            /\ nextSeq' = [nextSeq EXCEPT ![r.shard] = @ + 1]
            /\ UNCHANGED cache
       ELSE /\ cache' = IF a \in Clients THEN [cache EXCEPT ![a][n] = [shard |-> cat[n].shard, epoch |-> cat[n].epoch]] ELSE cache
            /\ UNCHANGED <<wst, winfo, nextSeq>>
  /\ UNCHANGED <<cat, own, store, accepted, mv, moverUp, moves, crashes, readViolation>>

\* COMMIT: the write is applied at its shard (ON CONFLICT DO NOTHING on its
\* id) and acknowledged.
CommitWrite(w) ==
  /\ wst[w] = "holding"
  /\ LET s == winfo[w].shard IN LET n == winfo[w].ns IN
     /\ store' = [store EXCEPT ![s][n][w] = IF @ = 0 THEN 1 ELSE @]
     /\ accepted' = [accepted EXCEPT ![n] = @ \cup {w}]
  /\ wst' = [wst EXCEPT ![w] = "committed"]
  /\ UNCHANGED <<cat, own, cache, winfo, nextSeq, mv, moverUp, moves, crashes, readViolation>>

AbortWrite(w) ==
  /\ wst[w] = "holding"
  /\ wst' = [wst EXCEPT ![w] = "idle"]
  /\ UNCHANGED <<cat, own, cache, store, winfo, nextSeq, accepted, mv, moverUp, moves, crashes, readViolation>>

\* A read at the cached route is accepted when the row is active or frozen
\* at the caller's epoch; what it returns is the shard's store.
Read(a, n) ==
  /\ LET r == cache[a][n] IN
     /\ own[r.shard][n].state \in {"active", "frozen"} /\ own[r.shard][n].epoch = r.epoch
     /\ readViolation' = (readViolation
                            \/ ~(accepted[n] \subseteq {w \in Writes : store[r.shard][n][w] > 0})
                            \/ r.shard /= cat[n].shard \/ r.epoch /= cat[n].epoch)
  /\ UNCHANGED <<cat, own, cache, store, wst, winfo, nextSeq, accepted, mv, moverUp, moves, crashes>>

\* Cache refresh (LISTEN catalog_changes or TTL): clients only; workflows
\* keep their pinned route until the mover restarts them.
Refresh(c, n) ==
  /\ c \in Clients
  /\ cache' = [cache EXCEPT ![c][n] = [shard |-> cat[n].shard, epoch |-> cat[n].epoch]]
  /\ UNCHANGED <<cat, own, store, wst, winfo, nextSeq, accepted, mv, moverUp, moves, crashes, readViolation>>

-----------------------------------------------------------------------------
(* The mover.  Every step is idempotent and keyed on mv.st (persisted).      *)

MvUnch == UNCHANGED <<cache, wst, winfo, nextSeq, accepted, readViolation>>

Plan(n) ==
  /\ moverUp /\ mv.st \in {"none", "done", "rolled_back"}
  /\ moves < MaxMoves /\ cat[n].epoch < MaxEpoch /\ cat[n].state = "active"
  /\ LET s == cat[n].shard IN LET t == Other(s) IN LET e == cat[n].epoch IN
     /\ own' = [own EXCEPT ![t][n] = [epoch |-> e + 1, state |-> "incoming"]]
     /\ cat' = [cat EXCEPT ![n].state = "moving"]
     /\ mv' = [st |-> "planned", ns |-> n, src |-> s, dst |-> t, p0 |-> 0, applied |-> {}, epoch |-> e]
     /\ store' = [store EXCEPT ![t][n] = Bag0]
  /\ moves' = moves + 1
  /\ UNCHANGED <<moverUp, crashes>> /\ MvUnch

CommittedSeqs(s, n) == {winfo[w].seq : w \in {w \in Writes : wst[w] = "committed" /\ winfo[w].shard = s /\ winfo[w].ns = n}}
MaxOr0(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x

\* Copy: one REPEATABLE READ snapshot of the source, p0 = max visible seq.
Copy ==
  /\ moverUp /\ mv.st = "planned"
  /\ CopyBarrier => Holding(mv.src, mv.ns) = {}
  /\ LET n == mv.ns IN
     /\ store' = [store EXCEPT ![mv.dst][n] = store[mv.src][n]]
     /\ mv' = [mv EXCEPT !.st = "catching_up", !.p0 = MaxOr0(CommittedSeqs(mv.src, n)), !.applied = {}]
  /\ UNCHANGED <<cat, own, moverUp, moves, crashes>> /\ MvUnch

\* The next outbox seq the move consumer may apply: it is committed, above
\* p0, not yet applied, and every smaller seq is resolved (no holding write
\* with a smaller seq -- the watchlist of Outbox.tla provides this).
Resolved(s, n, k) == \A w \in Holding(s, n) : winfo[w].seq > k
NextReplay ==
  LET cands == {w \in Writes : wst[w] = "committed" /\ winfo[w].shard = mv.src /\ winfo[w].ns = mv.ns
                               /\ winfo[w].seq > mv.p0 /\ winfo[w].seq \notin mv.applied}
  IN {w \in cands : \A v \in cands : winfo[w].seq <= winfo[v].seq}

Replay ==
  /\ moverUp /\ mv.st \in {"catching_up", "drained"}
  /\ \E w \in NextReplay :
       /\ Resolved(mv.src, mv.ns, winfo[w].seq)
       /\ store' = [store EXCEPT ![mv.dst][mv.ns][w] = @ + 1]
       /\ mv' = [mv EXCEPT !.applied = @ \cup {winfo[w].seq}]
  /\ UNCHANGED <<cat, own, moverUp, moves, crashes>> /\ MvUnch

Lag == Cardinality(NextReplay)

\* Freeze: UPDATE namespace_ownership SET state='frozen' on the source waits
\* for every FOR SHARE holder (no holding write), so after it no source
\* write for the namespace is in flight.
Freeze ==
  /\ moverUp /\ mv.st = "catching_up" /\ Lag <= 1
  /\ Holding(mv.src, mv.ns) = {}
  /\ own' = [own EXCEPT ![mv.src][mv.ns].state = "frozen"]
  /\ cat' = [cat EXCEPT ![mv.ns].state = "frozen"]
  /\ mv' = [mv EXCEPT !.st = "frozen"]
  /\ UNCHANGED <<store, moverUp, moves, crashes>> /\ MvUnch

\* Drain: the source is frozen, so the set of committed seqs is final.
Drained ==
  /\ moverUp /\ mv.st = "frozen" /\ NextReplay = {}
  /\ mv' = [mv EXCEPT !.st = "drained"]
  /\ UNCHANGED <<cat, own, store, moverUp, moves, crashes>> /\ MvUnch

\* Cutover substeps across three databases.
CutTarget ==
  /\ moverUp /\ mv.st = "drained" /\ NextReplay = {}
  /\ own' = [own EXCEPT ![mv.dst][mv.ns].state = "active"]
  /\ mv' = [mv EXCEPT !.st = "cut_target"]
  /\ UNCHANGED <<cat, store, moverUp, moves, crashes>> /\ MvUnch

CutCatalog ==
  /\ moverUp
  /\ mv.st = IF CutoverOrder = "safe" THEN "cut_source" ELSE "cut_target"
  /\ cat' = [cat EXCEPT ![mv.ns] = [shard |-> mv.dst, epoch |-> mv.epoch + 1, state |-> "active"]]
  /\ mv' = [mv EXCEPT !.st = "cut_catalog"]
  /\ UNCHANGED <<own, store, moverUp, moves, crashes>> /\ MvUnch

CutSource ==
  /\ moverUp
  /\ mv.st = IF CutoverOrder = "safe" THEN "cut_target" ELSE "cut_catalog"
  /\ own' = [own EXCEPT ![mv.src][mv.ns].state = "moved_out"]
  /\ mv' = [mv EXCEPT !.st = "cut_source"]
  /\ UNCHANGED <<cat, store, moverUp, moves, crashes>> /\ MvUnch

\* Restart the recorded workflows on the target queue with the new route.
RestartWorkflows ==
  /\ moverUp
  /\ mv.st = IF CutoverOrder = "safe" THEN "cut_catalog" ELSE "cut_source"
  /\ cache' = [a \in Actors |-> IF a \in Workflows
                                  THEN [cache[a] EXCEPT ![mv.ns] = [shard |-> mv.dst, epoch |-> mv.epoch + 1]]
                                  ELSE cache[a]]
  /\ mv' = [mv EXCEPT !.st = "done"]
  /\ UNCHANGED <<cat, own, store, wst, winfo, nextSeq, accepted, moverUp, moves, crashes, readViolation>>

\* Rollback before cutover: source back to active (same epoch), target row
\* and partial copy dropped.
Rollback ==
  /\ moverUp /\ mv.st \in {"planned", "catching_up", "frozen", "drained"}
  /\ own' = [own EXCEPT ![mv.src][mv.ns].state = "active", ![mv.dst][mv.ns] = [epoch |-> 0, state |-> "none"]]
  /\ cat' = [cat EXCEPT ![mv.ns].state = "active"]
  /\ store' = [store EXCEPT ![mv.dst][mv.ns] = Bag0]
  /\ mv' = [mv EXCEPT !.st = "rolled_back"]
  /\ UNCHANGED <<moverUp, moves, crashes>> /\ MvUnch

\* Crash and restart: persisted state (mv, own, cat, store) survives; the
\* step in progress is simply re-executed.
MoverCrash ==
  /\ moverUp /\ crashes < MaxCrashes
  /\ moverUp' = FALSE /\ crashes' = crashes + 1
  /\ UNCHANGED <<cat, own, store, mv, moves>> /\ MvUnch

MoverRestart ==
  /\ ~moverUp
  /\ moverUp' = TRUE
  /\ UNCHANGED <<cat, own, store, mv, moves, crashes>> /\ MvUnch

Mover == Copy \/ Replay \/ Freeze \/ Drained \/ CutTarget \/ CutCatalog \/ CutSource \/ RestartWorkflows \/ MoverRestart

-----------------------------------------------------------------------------

Next ==
  \/ \E a \in Actors, n \in NS, w \in Writes : BeginWrite(a, n, w)
  \/ \E w \in Writes : CommitWrite(w) \/ AbortWrite(w)
  \/ \E a \in Actors, n \in NS : Read(a, n) \/ Refresh(a, n)
  \/ \E n \in Movable : Plan(n)
  \/ Mover \/ Rollback \/ MoverCrash
  \/ UNCHANGED vars

Fairness ==
  /\ WF_vars(Mover)
  /\ \A w \in Writes : WF_vars(CommitWrite(w) \/ AbortWrite(w))
  /\ \A c \in Clients, n \in NS : WF_vars(Refresh(c, n))

Spec == Init /\ [][Next]_vars /\ Fairness

Symm == Permutations(Clients) \cup Permutations(Writes)

-----------------------------------------------------------------------------
(* Invariants *)

SingleWritableOwner ==
  \A n \in NS : Cardinality({s \in Shards : own[s][n].state = "active"}) <= 1

\* A write that holds the fence is at the catalog's shard for its namespace,
\* at the catalog's current epoch (so every accepted write was too).
WritesOnlyAtOwner ==
  \A w \in Writes : wst[w] = "holding" =>
    /\ own[winfo[w].shard][winfo[w].ns].state = "active"
    /\ cat[winfo[w].ns].shard = winfo[w].shard
    /\ cat[winfo[w].ns].epoch = own[winfo[w].shard][winfo[w].ns].epoch

\* The catalog's shard holds exactly the accepted writes, once each -- during
\* the move (owner = source) and after it (owner = target).
NoLossNoDup ==
  \A n \in NS : \A w \in Writes :
    store[cat[n].shard][n][w] = IF w \in accepted[n] THEN 1 ELSE 0

\* The target never holds a write twice (replay of an already-copied event).
NoDupAnywhere ==
  \A s \in Shards, n \in NS, w \in Writes : store[s][n][w] <= 1

\* An accepted read sees every accepted write of its namespace (D16's read
\* barrier survives the move) and happens at the catalog's shard and epoch.
ReadsFresh == ~readViolation

\* Liveness: a started move completes or rolls back.
MoveTerminates == (mv.st \notin {"none", "done", "rolled_back"}) ~> (mv.st \in {"done", "rolled_back"})

=============================================================================
