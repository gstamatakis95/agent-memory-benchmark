---------------------------------------------------- MODULE Outbox -----------------------------------------------------
(**********************************************************************************************************************)
(* Engram D6: per-shard transactional outbox.                                                                         *)
(*                                                                                                                    *)
(* Writers draw `seq` from a Postgres sequence inside their transaction, so a later seq may become visible before an  *)
(* earlier one (or the earlier one may never become visible because its transaction aborted). A single relay per      *)
(* shard scans in seq order with a persisted cursor and an in-memory gap watchlist: a seq that is missing while a     *)
(* larger seq is visible is watched for `Watch` ticks (register: 2 x statement_timeout) and then declared aborted.    *)
(* Delivered events fan out to idempotent consumers (index, kafka). The relay may crash after delivering and before   *)
(* persisting its cursor, which re-delivers (at-least-once).                                                          *)
(*                                                                                                                    *)
(* Timing model (discrete ticks). A writer that drew a seq must commit within `Timeout` ticks of the draw; on the     *)
(* tick after that the database cancels it (statement_timeout / idle_in_transaction_session_timeout; assumption A-F1: *)
(* the outbox INSERT is the last statement of every write transaction, and no commit waits on a standby because every *)
(* role runs synchronous_commit = local, N122). Moves no longer replay the outbox (N124), so the relay is the only    *)
(* reader of the gap watchlist.                                                                                       *)
(*                                                                                                                    *)
(* Design run:      Timeout = 2, Watch = 4  (2 x timeout)     -> all pass                                             *)
(* Counterexamples: Watch = 0 (no watchlist), Watch = 2 (1 x timeout)                                                 *)
(*                  -> NoLossSafety violated (a declared-aborted seq later                                            *)
(*                     commits and is never delivered).                                                               *)
(**********************************************************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Writers,     \* concurrent write transactions, e.g. {w1, w2, w3}
          Namespaces,  \* namespaces hosted by the shard, e.g. {a, b}
          Consumers,   \* e.g. {"index", "kafka"}
          MaxSeq,      \* number of seq values drawn per behaviour
          Timeout,     \* draw-to-commit bound in ticks
          Watch,       \* watchlist horizon in ticks (design: 2 * Timeout)
          MaxCrashes,  \* relay crashes per behaviour (bounds duplicate delivery)
          NoNs         \* model value: "no namespace" for an undrawn seq

ASSUME Timeout >= 1 /\ MaxSeq >= 1 /\ Watch >= 0

Seqs == 1..MaxSeq
ASSUME NoNs \notin Namespaces

VARIABLES
  nextSeq,    \* next value the sequence hands out
  wr,         \* writer state: [Writers -> [st, seq, ns, age]]
  committed,  \* seqs whose transaction committed (visible to the relay)
  aborted,    \* seqs whose transaction aborted (never visible)
  nsOf,       \* [Seqs -> Namespaces \cup {NoNs}]  namespace of a drawn seq
  cursor,     \* persisted relay cursor: every seq <= cursor is resolved
  watch,      \* in-memory watchlist: seq |-> age in ticks since first seen
  declared,   \* seqs the relay declared aborted (cursor moved past them)
  log,        \* [Consumers -> Seq(Seqs)]  delivery log (duplicates possible)
  crashes     \* relay crash counter

vars == <<nextSeq, wr, committed, aborted, nsOf, cursor, watch, declared, log, crashes>>

WriterState == [st : {"idle", "holding"}, seq : 0..MaxSeq, ns : Namespaces \cup {NoNs}, age : 0..Timeout]

TypeOK ==
  /\ nextSeq \in 1..(MaxSeq + 1)
  /\ wr \in [Writers -> WriterState]
  /\ committed \subseteq Seqs
  /\ aborted \subseteq Seqs
  /\ committed \cap aborted = {}
  /\ nsOf \in [Seqs -> Namespaces \cup {NoNs}]
  /\ cursor \in 0..MaxSeq
  /\ DOMAIN watch \subseteq Seqs
  /\ \A s \in DOMAIN watch : watch[s] \in 0..Watch
  /\ declared \subseteq Seqs
  /\ log \in [Consumers -> Seq(Seqs)]
  /\ crashes \in 0..MaxCrashes

Init ==
  /\ nextSeq = 1
  /\ wr = [w \in Writers |-> [st |-> "idle", seq |-> 0, ns |-> NoNs, age |-> 0]]
  /\ committed = {}
  /\ aborted = {}
  /\ nsOf = [s \in Seqs |-> NoNs]
  /\ cursor = 0
  /\ watch = << >>          \* the empty function (DOMAIN = {})
  /\ declared = {}
  /\ log = [c \in Consumers |-> << >>]
  /\ crashes = 0

-----------------------------------------------------------------------------
(* Writers *)

\* nextval() inside the transaction: the seq is allocated now, visibility comes at commit.
Draw(w, n) ==
  /\ wr[w].st = "idle"
  /\ nextSeq <= MaxSeq
  /\ wr' = [wr EXCEPT ![w] = [st |-> "holding", seq |-> nextSeq, ns |-> n, age |-> 0]]
  /\ nsOf' = [nsOf EXCEPT ![nextSeq] = n]
  /\ nextSeq' = nextSeq + 1
  /\ UNCHANGED <<committed, aborted, cursor, watch, declared, log, crashes>>

\* COMMIT: allowed only while the draw-to-commit bound has not elapsed.
Commit(w) ==
  /\ wr[w].st = "holding"
  /\ wr[w].age <= Timeout
  /\ committed' = committed \cup {wr[w].seq}
  /\ wr' = [wr EXCEPT ![w].st = "idle"]
  /\ UNCHANGED <<nextSeq, aborted, nsOf, cursor, watch, declared, log, crashes>>

\* Voluntary abort (e.g. FAILED_PRECONDITION on a frozen namespace, app error).
Abort(w) ==
  /\ wr[w].st = "holding"
  /\ aborted' = aborted \cup {wr[w].seq}
  /\ wr' = [wr EXCEPT ![w].st = "idle"]
  /\ UNCHANGED <<nextSeq, committed, nsOf, cursor, watch, declared, log, crashes>>

-----------------------------------------------------------------------------
(* Time *)

Overdue == {w \in Writers : wr[w].st = "holding" /\ wr[w].age = Timeout}

\* One tick: holding writers age; a writer that has been holding for Timeout ticks is cancelled by the database;
\* watchlist entries age (capped at Watch).
Tick ==
  /\ wr' = [w \in Writers |->
              IF w \in Overdue THEN [wr[w] EXCEPT !.st = "idle"]
              ELSE IF wr[w].st = "holding" THEN [wr[w] EXCEPT !.age = @ + 1]
              ELSE wr[w]]
  /\ aborted' = aborted \cup {wr[w].seq : w \in Overdue}
  /\ watch' = [s \in DOMAIN watch |-> IF watch[s] < Watch THEN watch[s] + 1 ELSE watch[s]]
  /\ UNCHANGED <<nextSeq, committed, nsOf, cursor, declared, log, crashes>>

-----------------------------------------------------------------------------
(* Relay *)

Head1 == cursor + 1

\* A gap: the next expected seq is not visible but a larger seq is.
GapObserved == Head1 \notin committed /\ \E t \in committed : t > Head1

RemoveWatch(s) == [t \in (DOMAIN watch) \ {s} |-> watch[t]]

\* Deliver the next seq to every consumer and persist the cursor.
DeliverPersist ==
  /\ Head1 \in committed
  /\ log' = [c \in Consumers |-> Append(log[c], Head1)]
  /\ cursor' = Head1
  /\ watch' = RemoveWatch(Head1)
  /\ UNCHANGED <<nextSeq, wr, committed, aborted, nsOf, declared, crashes>>

\* Deliver, then crash before the cursor is persisted: the in-memory watchlist is lost (timers restart, which is
\* conservative) and the same seq will be delivered again after restart.
DeliverCrash ==
  /\ Head1 \in committed
  /\ crashes < MaxCrashes
  /\ log' = [c \in Consumers |-> Append(log[c], Head1)]
  /\ watch' = << >>
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<nextSeq, wr, committed, aborted, nsOf, cursor, declared>>

\* First observation of a gap: start its timer.
WatchGap ==
  /\ GapObserved
  /\ Head1 \notin DOMAIN watch
  /\ watch' = watch @@ (Head1 :> 0)
  /\ UNCHANGED <<nextSeq, wr, committed, aborted, nsOf, cursor, declared, log, crashes>>

\* The horizon elapsed and the seq is still invisible: declare it aborted and move the cursor past it. (If it became
\* visible meanwhile, DeliverPersist is enabled instead and this action is not.)
DeclareAborted ==
  /\ Head1 \notin committed
  /\ Head1 \in DOMAIN watch
  /\ watch[Head1] >= Watch
  /\ declared' = declared \cup {Head1}
  /\ cursor' = Head1
  /\ watch' = RemoveWatch(Head1)
  /\ UNCHANGED <<nextSeq, wr, committed, aborted, nsOf, log, crashes>>

Relay == DeliverPersist \/ DeliverCrash \/ WatchGap \/ DeclareAborted

-----------------------------------------------------------------------------

Next ==
  \/ \E w \in Writers : \E n \in Namespaces : Draw(w, n)
  \/ \E w \in Writers : Commit(w) \/ Abort(w)
  \/ Tick
  \/ Relay

Fairness == WF_vars(Relay) /\ WF_vars(Tick) /\ \A w \in Writers : WF_vars(Commit(w) \/ Abort(w))

Spec == Init /\ [][Next]_vars /\ Fairness

Symm == Permutations(Writers) \cup Permutations(Namespaces) \cup Permutations(Consumers)

-----------------------------------------------------------------------------
(* Properties *)

Range(s) == {s[i] : i \in DOMAIN s}

RECURSIVE DedupFrom(_, _)
DedupFrom(s, seen) ==
  IF s = << >> THEN << >>
  ELSE IF Head(s) \in seen THEN DedupFrom(Tail(s), seen)
  ELSE <<Head(s)>> \o DedupFrom(Tail(s), seen \cup {Head(s)})

\* First-delivery order (consumers deduplicate by seq).
Dedup(s) == DedupFrom(s, {})

Increasing(s) == \A i \in 1..(Len(s) - 1) : s[i] < s[i + 1]

\* NoLoss (safety form): the cursor never passes a committed seq without delivering it, and a seq the relay declared
\* aborted never turns out to be committed. `committed` only grows, so this is a state invariant.
NoLossSafety ==
  /\ declared \cap committed = {}
  /\ \A s \in committed : s <= cursor => \A c \in Consumers : s \in Range(log[c])

\* Per-namespace order: the first delivery of each seq happens in seq order; hence the per-namespace subsequence is in
\* seq order too.
PerNamespaceOrder ==
  \A c \in Consumers : \A n \in Namespaces :
    Increasing(SelectSeq(Dedup(log[c]), LAMBDA s : nsOf[s] = n))

\* Only committed rows are ever delivered.
OnlyCommittedDelivered == \A c \in Consumers : Range(log[c]) \subseteq committed

\* At-least-once with idempotent consumers: the consumer's *effective* state (a set) equals the committed prefix below
\* the persisted cursor, no matter how many duplicates the log holds.
IdempotentConsumerState ==
  \A c \in Consumers : {s \in committed : s <= cursor} \subseteq Range(log[c])

\* Liveness form of NoLoss: every committed seq is eventually delivered.
NoLossLive == \A s \in Seqs : (s \in committed) ~> (\A c \in Consumers : s \in Range(log[c]))

========================================================================================================================
