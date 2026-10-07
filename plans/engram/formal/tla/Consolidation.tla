------------------------- MODULE Consolidation -------------------------
(***************************************************************************)
(* Engram D12: consolidation round with at-least-once activity execution.  *)
(*                                                                         *)
(* A round takes a set of facts, calls the LLM on a batch (8 facts in      *)
(* production, 4 here), and applies the returned ops (create/update/delete *)
(* of observations, each citing source facts).  A failed LLM call bisects  *)
(* the batch (8 -> 4 -> 2 -> 1); a failed singleton is skipped.           *)
(*                                                                         *)
(* Idempotency: batch_key = sha256(sorted fact ids || prompt || model) is  *)
(* modelled as the fact set itself; op_key = (batch_key, op_index).        *)
(* `consolidation_applied(op_key)` is inserted in the same transaction as  *)
(* the op's effect.  The worker may crash anywhere and Temporal re-runs    *)
(* the activity (at-least-once).                                           *)
(*                                                                         *)
(* Two design knobs expose the two ways this goes wrong:                   *)
(*   DurableProposal = FALSE : the apply step uses the LLM output of the   *)
(*       current attempt instead of a proposal persisted under batch_key.  *)
(*       After a crash the LLM returns a different op list and op_index-   *)
(*       based keys silently skip ops that were never applied             *)
(*       (ExactlyOnceEffect violated).                                     *)
(*   AtomicKeyRecord = FALSE : effect and op_key insert are separate       *)
(*       transactions; a crash between them double-applies the op.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS NFacts,           \* facts of the round are 1..NFacts (4 here, 8 in production)
          Obs,              \* observation ids the LLM may reference, e.g. {o1, o2}
          NoObs,            \* model value used by the "no effect" marker
          MaxCrashes,       \* worker crashes per behaviour
          MaxDeletes,       \* concurrent fact deletes per behaviour
          DurableProposal,  \* BOOLEAN: proposal persisted under batch_key before apply
          AtomicKeyRecord   \* BOOLEAN: effect + op_key insert in one transaction

ASSUME NFacts >= 1 /\ NoObs \notin Obs

Facts == 1..NFacts
FactOrder == [i \in 1..NFacts |-> i]      \* the bisect order (sorted fact ids)
Kinds == {"create", "update", "delete"}
Op == [kind : Kinds, o : Obs]
NoOp == [kind |-> "none", o |-> NoObs]
Proposals == {<<a>> : a \in Op} \cup {<<a, b>> : a \in Op, b \in Op}
Batches == (SUBSET Facts) \ {{}}
Keys == Batches \X (1..2)

Range(s) == {s[i] : i \in DOMAIN s}
Sorted(B) == SelectSeq(FactOrder, LAMBDA f : f \in B)
Halves(B) == LET s == Sorted(B) IN LET n == Len(s) IN
             {Range(SubSeq(s, 1, n \div 2)), Range(SubSeq(s, (n \div 2) + 1, n))}

VARIABLES
  live,        \* live (not retired) facts
  queue,       \* batches still to be processed
  stored,      \* durable proposals: batch |-> proposal (DOMAIN = batches with one)
  mem,         \* volatile proposals of the current attempt (lost on crash)
  applied,     \* op keys recorded in consolidation_applied
  applyCount,  \* [Keys -> Nat]  how many times each op's effect was applied
  effectOf,    \* [Keys -> Op \cup {NoOp}]  the op whose effect was applied under a key
  pending,     \* {} or {key}: effect applied, key not yet recorded (AtomicKeyRecord = FALSE)
  done,        \* completed batches
  finalProp,   \* batch |-> proposal in force when the batch completed
  failed,      \* singleton batches whose LLM call failed (skipped this round)
  obs,         \* [Obs -> [st : {"absent","live","retired"}, src : SUBSET Facts]]
  crashes,
  deletes

vars == <<live, queue, stored, mem, applied, applyCount, effectOf, pending, done, finalProp, failed, obs, crashes, deletes>>

PropStore == IF DurableProposal THEN stored ELSE mem
HasProp(B) == B \in DOMAIN PropStore

TypeOK ==
  /\ live \subseteq Facts
  /\ queue \subseteq Batches
  /\ DOMAIN stored \subseteq Batches /\ \A B \in DOMAIN stored : stored[B] \in Proposals
  /\ DOMAIN mem \subseteq Batches /\ \A B \in DOMAIN mem : mem[B] \in Proposals
  /\ applied \subseteq Keys
  /\ applyCount \in [Keys -> 0..2]
  /\ effectOf \in [Keys -> Op \cup {NoOp}]
  /\ pending \subseteq Keys /\ Cardinality(pending) <= 1
  /\ done \subseteq Batches
  /\ DOMAIN finalProp = done
  /\ failed \subseteq Batches
  /\ obs \in [Obs -> [st : {"absent", "live", "retired"}, src : SUBSET Facts]]

Init ==
  /\ live = Facts
  /\ queue = {Facts}
  /\ stored = << >>
  /\ mem = << >>
  /\ applied = {}
  /\ applyCount = [k \in Keys |-> 0]
  /\ effectOf = [k \in Keys |-> NoOp]
  /\ pending = {}
  /\ done = {}
  /\ finalProp = << >>
  /\ failed = {}
  /\ obs = [o \in Obs |-> [st |-> "absent", src |-> {}]]
  /\ crashes = 0
  /\ deletes = 0

-----------------------------------------------------------------------------
(* Propose activity: one LLM call per batch.                                *)

\* Success: the proposal is recorded (durably, keyed by batch_key, or only in
\* the attempt's memory).  A durable record is write-once (ON CONFLICT DO NOTHING).
ProposeOk(B, P) ==
  /\ B \in queue /\ B \notin done /\ ~HasProp(B)
  /\ IF DurableProposal THEN stored' = stored @@ (B :> P) /\ UNCHANGED mem
                        ELSE mem' = mem @@ (B :> P) /\ UNCHANGED stored
  /\ UNCHANGED <<live, queue, applied, applyCount, effectOf, pending, done, finalProp, failed, obs, crashes, deletes>>

\* Failure (gateway error, schema violation, timeout): bisect, or skip a singleton.
ProposeFail(B) ==
  /\ B \in queue /\ B \notin done /\ ~HasProp(B)
  /\ IF Cardinality(B) >= 2
       THEN queue' = (queue \ {B}) \cup Halves(B) /\ UNCHANGED failed
       ELSE queue' = queue \ {B} /\ failed' = failed \cup {B}
  /\ UNCHANGED <<live, stored, mem, applied, applyCount, effectOf, pending, done, finalProp, obs, crashes, deletes>>

-----------------------------------------------------------------------------
(* Apply activity: ops of the proposal in index order, each guarded by its  *)
(* op_key.  Re-execution after a crash skips recorded keys.                 *)

\* Effect of an op on the observation table.  Sources are the batch facts
\* that are still live (checked FOR SHARE in the same transaction); an op
\* whose sources vanished, or that does not apply (update of a retired
\* observation, create of an existing one), is dropped but its key is still
\* recorded so it is never retried.
Effect(op, B) ==
  LET S == B \cap live IN
  IF S = {} THEN obs
  ELSE CASE op.kind = "create" -> IF obs[op.o].st = "absent" THEN [obs EXCEPT ![op.o] = [st |-> "live", src |-> S]] ELSE obs
         [] op.kind = "update" -> IF obs[op.o].st = "live"   THEN [obs EXCEPT ![op.o].src = S] ELSE obs
         [] op.kind = "delete" -> IF obs[op.o].st = "live"   THEN [obs EXCEPT ![op.o] = [st |-> "retired", src |-> {}]] ELSE obs

NextIndex(B) == LET P == PropStore[B] IN
  CHOOSE i \in 1..Len(P) : <<B, i>> \notin applied /\ \A j \in 1..(i - 1) : <<B, j>> \in applied

Unapplied(B) == \E i \in 1..Len(PropStore[B]) : <<B, i>> \notin applied

\* Apply the next op of B: effect + key in one transaction.
ApplyAtomic(B) ==
  /\ AtomicKeyRecord
  /\ B \in queue /\ HasProp(B) /\ Unapplied(B) /\ pending = {}
  /\ LET i == NextIndex(B) IN LET op == PropStore[B][i] IN LET k == <<B, i>> IN
     /\ obs' = Effect(op, B)
     /\ applied' = applied \cup {k}
     /\ applyCount' = [applyCount EXCEPT ![k] = @ + 1]
     /\ effectOf' = [effectOf EXCEPT ![k] = op]
  /\ UNCHANGED <<live, queue, stored, mem, pending, done, finalProp, failed, crashes, deletes>>

\* Non-atomic variant: effect first ...
ApplyEffectOnly(B) ==
  /\ ~AtomicKeyRecord
  /\ B \in queue /\ HasProp(B) /\ Unapplied(B) /\ pending = {}
  /\ LET i == NextIndex(B) IN LET op == PropStore[B][i] IN LET k == <<B, i>> IN
     /\ obs' = Effect(op, B)
     /\ pending' = {k}
     /\ applyCount' = [applyCount EXCEPT ![k] = @ + 1]
     /\ effectOf' = [effectOf EXCEPT ![k] = op]
  /\ UNCHANGED <<live, queue, stored, mem, applied, done, finalProp, failed, crashes, deletes>>

\* ... then the key in a second transaction.
RecordKey ==
  /\ pending /= {}
  /\ applied' = applied \cup pending
  /\ pending' = {}
  /\ UNCHANGED <<live, queue, stored, mem, applyCount, effectOf, done, finalProp, failed, obs, crashes, deletes>>

\* The batch completes once every op of its proposal has a recorded key.
MarkDone(B) ==
  /\ B \in queue /\ HasProp(B) /\ ~Unapplied(B) /\ pending = {}
  /\ done' = done \cup {B}
  /\ finalProp' = finalProp @@ (B :> PropStore[B])
  /\ queue' = queue \ {B}
  /\ UNCHANGED <<live, stored, mem, applied, applyCount, effectOf, pending, failed, obs, crashes, deletes>>

-----------------------------------------------------------------------------
(* Faults and concurrency *)

\* Worker crash: everything volatile is lost; durable state survives.
\* Temporal re-runs the activity, which is modelled by the actions above
\* simply remaining enabled.
Crash ==
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ mem' = << >>
  /\ pending' = {}
  /\ UNCHANGED <<live, queue, stored, applied, applyCount, effectOf, done, finalProp, failed, obs, deletes>>

\* Concurrent document delete retires a fact; the DB trigger removes it from
\* observation_sources and retires observations whose source count hits 0.
DeleteFact(f) ==
  /\ f \in live /\ deletes < MaxDeletes
  /\ deletes' = deletes + 1
  /\ live' = live \ {f}
  /\ obs' = [o \in Obs |->
              IF obs[o].st = "live"
                THEN IF obs[o].src \ {f} = {} THEN [st |-> "retired", src |-> {}]
                                              ELSE [obs[o] EXCEPT !.src = @ \ {f}]
                ELSE obs[o]]
  /\ UNCHANGED <<queue, stored, mem, applied, applyCount, effectOf, pending, done, finalProp, failed, crashes>>

-----------------------------------------------------------------------------

Worker ==
  \/ \E B \in Batches : \E P \in Proposals : ProposeOk(B, P)
  \/ \E B \in Batches : ProposeFail(B) \/ ApplyAtomic(B) \/ ApplyEffectOnly(B) \/ MarkDone(B)
  \/ RecordKey

\* The round is over: allow infinite stuttering so TLC does not report a deadlock.
Terminating == queue = {} /\ UNCHANGED vars

Next == Worker \/ Crash \/ (\E f \in Facts : DeleteFact(f)) \/ Terminating

Spec == Init /\ [][Next]_vars /\ WF_vars(Worker)

Symm == Permutations(Obs)

-----------------------------------------------------------------------------
(* Properties *)

\* Each op's effect is applied at most once, and every op of a completed
\* batch was applied exactly as the batch's (final) proposal states it.
ExactlyOnceEffect ==
  /\ \A k \in Keys : applyCount[k] <= 1
  /\ \A B \in done : \A i \in 1..Len(finalProp[B]) :
       /\ <<B, i>> \in applied
       /\ applyCount[<<B, i>>] = 1
       /\ effectOf[<<B, i>>] = finalProp[B][i]

\* Observations never outlive their sources, and never cite a retired fact.
ObservationHasSources ==
  \A o \in Obs : obs[o].st = "live" => obs[o].src /= {} /\ obs[o].src \subseteq live

\* Every batch of the round is eventually done, bisected into done/failed children.
RoundTerminates == <>(queue = {})

=============================================================================
