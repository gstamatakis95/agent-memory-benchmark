// Package shardmove is the invariant code of formal/tla/ShardMove.tla (PLAN.md section 7.2.2, D5, N123 to N125, N137,
// N160 to N163, N169 to N172, N175, N179 to N185, N188): moving one namespace from a source to a target shard by
// freeze, copy, verify, index, seal and cut over. The functions read a plain Go value model of the specification's
// variables; the move tests build a State from the ownership rows, the catalog and the move row after every mover step
// and call Check with the TLC constants as generator bounds.
package shardmove

import "github.com/gstamatakis95/engram/internal/formal/tla"

// The two shards of the model (a second move swaps the roles of source and target).
const (
	S1 = "s1"
	S2 = "s2"
)

// Shards lists both shards.
var Shards = []string{S1, S2}

// Row is an ownership row <<st, ep>>: the state ("none", "unread", "incoming", "ready", "active", "frozen",
// "moved_out", "restoring", "replaying") and the epoch.
type Row struct {
	St string
	Ep int
}

// Cat is the catalog's routing row <<shard, epoch>>.
type Cat struct {
	Sh string
	Ep int
}

// Writer is one client's write transaction: Ph is "idle" or "hold", Sh the shard it holds the fence on.
type Writer struct {
	Ph string
	Sh string
}

// State mirrors the variables of ShardMove.tla that the invariants read. Rows are 1..NRows.
type State struct {
	Cat       Cat
	Cm        string // the catalog's move row: none, open, committed, rolled_back, done
	Mp        string // the mover's progress: none, planned, frozen, copied, built, sealed, ready, ...
	Own       map[string]Row
	Store     map[string]tla.Set[int] // insert-only rows per shard
	Mk        map[string]tla.Set[int] // mutable-class keys per shard
	Ex        map[string]int          // the expiring-class counter per shard
	Idx       map[string]bool         // the shard's index bit
	Sq        map[string]int          // the shard's sequence
	Sqt       map[int]int             // ins_seq of each row
	Mv        map[string]bool         // the ownership row's move bit
	Committed tla.Set[int]            // acknowledged rows
	Lost      tla.Set[int]            // accepted RPO losses
	Gone      tla.Set[int]            // rows deleted on the active target (durable intents)
	Wr        []Writer
	FrozenSet tla.Set[int]
	FrozenMk  tla.Set[int]
	FrozenEx  int
	ActSet    tla.Set[int]
	ActMk     tla.Set[int]
	Zcut      bool
	Cleaned   bool
	Src, Tgt  string
	Me        int // the target epoch of the current move
	Cdirty    bool
}

// Names of the invariants, as they appear in formal/tla/ShardMove.tla and formal/tla/EXPECT.
const (
	SingleWriterName               = "SingleWriter"
	NoLossNoDupName                = "NoLossNoDup"
	NoResurrectName                = "NoResurrect"
	ServedFromIndexName            = "ServedFromIndex"
	SourceStaticUnderFreezeName    = "SourceStaticUnderFreeze"
	RollbackPossibleBeforeCName    = "RollbackPossibleBeforeC"
	NoWriteToTargetBeforeCName     = "NoWriteToTargetBeforeC"
	NoRouteToTargetBeforeCName     = "NoRouteToTargetBeforeC"
	ZombieCannotCutOverName        = "ZombieCannotCutOver"
	RestoreReconcilesName          = "RestoreReconciles"
	OneOwnerName                   = "OneOwner"
	CatalogNamesOwnerAfterDoneName = "CatalogNamesOwnerAfterDone"
	CleanupSafeName                = "CleanupSafe"
	CopiedBelowTargetSeqName       = "CopiedBelowTargetSeq"
	MoveClosedWhenFinalName        = "MoveClosedWhenFinal"
	OwnerHasNoStaleMoveName        = "OwnerHasNoStaleMove"
)

// PreC is the mover states before the point of no return (the catalog CAS (a”)).
var PreC = tla.NewSet("planned", "frozen", "copied", "built", "sealed", "ready", "early", "early_flipped", "aborting")

// PostC is the mover states after it.
var PostC = tla.NewSet("committed", "cut", "tactive", "done")

// Final is the mover states in which no move is in flight.
var Final = tla.NewSet("none", "rolled_back", "done")

// Settled: no shard is restoring or replaying.
func (s State) Settled() bool {
	for _, sh := range Shards {
		if st := s.Own[sh].St; st == "restoring" || st == "replaying" {
			return false
		}
	}
	return true
}

// ActiveStores is the union of the stores of the active shards.
func (s State) ActiveStores() tla.Set[int] {
	out := tla.NewSet[int]()
	for _, sh := range Shards {
		if s.Own[sh].St == "active" {
			out = out.Union(s.Store[sh])
		}
	}
	return out
}

func (s State) activeCount() int {
	n := 0
	for _, sh := range Shards {
		if s.Own[sh].St == "active" {
			n++
		}
	}
	return n
}

func other(sh string) string {
	if sh == S1 {
		return S2
	}
	return S1
}

// SingleWriter: never two writable owners; a writer only holds the fence at an active shard.
func SingleWriter(s State) bool {
	if s.activeCount() > 1 {
		return false
	}
	for _, w := range s.Wr {
		if w.Ph == "hold" && s.Own[w.Sh].St != "active" {
			return false
		}
	}
	return true
}

// NoLossNoDup: the target is activated with exactly the rows frozen at the source; a committed move whose source left
// frozen has a ready or active target holding them; after the cleanup an active target holds every frozen row it did
// not delete; once settled every acknowledged row that is not an accepted RPO loss and not deleted is on an active
// owner.
func NoLossNoDup(s State) bool {
	if tla.NewSet("tactive", "done", "early", "early_flipped").Has(s.Mp) {
		if !s.FrozenSet.Minus(s.Gone).SubsetOf(s.ActSet) || !s.ActSet.SubsetOf(s.FrozenSet) ||
			!s.FrozenMk.Minus(s.Gone).SubsetOf(s.ActMk) || !s.ActMk.SubsetOf(s.FrozenMk) {
			return false
		}
	}
	srcSt := s.Own[s.Src].St
	committed := s.Cm == "committed" || s.Cm == "done"
	if s.Settled() && !s.Cleaned && committed && (srcSt == "active" || srcSt == "moved_out") {
		tgtSt := s.Own[s.Tgt].St
		held := s.FrozenSet.Minus(s.Gone).Minus(s.Lost).SubsetOf(s.Store[s.Tgt])
		if (tgtSt != "ready" && tgtSt != "active") || !held {
			return false
		}
	}
	if s.Cleaned && s.Own[s.Tgt].St == "active" && !s.FrozenSet.Minus(s.Gone).SubsetOf(s.Store[s.Tgt]) {
		return false
	}
	if s.Settled() && Final.Has(s.Mp) && !s.Committed.Minus(s.Lost).Minus(s.Gone).SubsetOf(s.ActiveStores()) {
		return false
	}
	return true
}

// NoResurrect: a row deleted on the active target is never again in any active store; no repair re-adds it (N161).
func NoResurrect(s State) bool { return !s.Gone.Intersects(s.ActiveStores()) }

// ServedFromIndex: a target that is ready or active has its index (the first recall after activation uses the HNSW,
// N160(5)).
func ServedFromIndex(s State) bool {
	for _, sh := range Shards {
		if st := s.Own[sh].St; (st == "ready" || st == "active") && !s.Idx[sh] {
			return false
		}
	}
	return true
}

// SourceStaticUnderFreeze: under the freeze the source's rows do not change (N160(2)).
func SourceStaticUnderFreeze(s State) bool {
	frozenStates := tla.NewSet("frozen", "copied", "built", "sealed", "ready", "committed")
	if s.Own[s.Src].St != "frozen" || !frozenStates.Has(s.Mp) {
		return true
	}
	return s.Store[s.Src].Equal(s.FrozenSet) && s.Mk[s.Src].Equal(s.FrozenMk) && s.Ex[s.Src] == s.FrozenEx
}

// RollbackPossibleBeforeC: before the point of no return the source is still the owner and holds every committed row
// (bar restore RPO loss and rows deleted on the active target of an earlier move), so a rollback loses nothing.
func RollbackPossibleBeforeC(s State) bool {
	if !PreC.Has(s.Mp) {
		return true
	}
	switch s.Own[s.Src].St {
	case "active", "frozen", "restoring", "replaying":
	default:
		return false
	}
	return s.Committed.Minus(s.Lost).Minus(s.Gone).SubsetOf(s.Store[s.Src])
}

// NoWriteToTargetBeforeC: `incoming` and `ready` accept nothing; the target is never writable before the CAS.
func NoWriteToTargetBeforeC(s State) bool { return !PreC.Has(s.Mp) || s.Own[s.Tgt].St != "active" }

// NoRouteToTargetBeforeC: the catalog routes to the target only once the move is committed.
func NoRouteToTargetBeforeC(s State) bool {
	return s.Mp == "none" || s.Cat.Sh != s.Tgt || s.Cm == "committed" || s.Cm == "done"
}

// ZombieCannotCutOver: a stale-session mover never executed Freeze, the CAS or (c).
func ZombieCannotCutOver(s State) bool { return !s.Zcut }

// RestoreReconciles: a restored shard never ends active while another shard holds the namespace at a >= epoch.
func RestoreReconciles(s State) bool {
	for _, sh := range Shards {
		if s.Own[sh].St != "active" {
			continue
		}
		if s.Own[sh].Ep < s.Cat.Ep {
			return false
		}
		t := other(sh)
		if st := s.Own[t].St; st == "active" || st == "ready" {
			return false
		}
		if s.Own[t].St == "moved_out" && s.Own[t].Ep > s.Own[sh].Ep {
			return false
		}
	}
	return true
}

// OneOwner: whenever no recovery is running and no move is mid-flight, exactly one shard is the writable owner.
func OneOwner(s State) bool { return !s.Settled() || !Final.Has(s.Mp) || s.activeCount() == 1 }

// CatalogNamesOwnerAfterDone: once reconciled, the catalog names the owner at its epoch; while (d) is pending the CAS
// can still succeed.
func CatalogNamesOwnerAfterDone(s State) bool {
	if !s.Settled() || s.Cdirty {
		return true
	}
	if Final.Has(s.Mp) {
		if o := s.Own[s.Cat.Sh]; o.St != "active" || o.Ep != s.Cat.Ep {
			return false
		}
	}
	if s.Mp == "tactive" {
		a := Cat{Sh: s.Src, Ep: s.Me - 1}
		b := Cat{Sh: s.Tgt, Ep: s.Own[s.Tgt].Ep}
		if s.Cat != a && s.Cat != b {
			return false
		}
	}
	return true
}

// CleanupSafe: cleanup never removes the only copy; once the source rows are gone an active target holds what was
// frozen.
func CleanupSafe(s State) bool {
	return !s.Cleaned || s.Own[s.Tgt].St != "active" || s.FrozenSet.Minus(s.Gone).SubsetOf(s.Store[s.Tgt])
}

// CopiedBelowTargetSeq (N147): every row a ready or active shard holds has an ins_seq below the shard's sequence, so
// floors computed on the shard see the moved rows as old.
func CopiedBelowTargetSeq(s State) bool {
	for _, sh := range Shards {
		if st := s.Own[sh].St; st != "ready" && st != "active" {
			continue
		}
		for r := range s.Store[sh] {
			if s.Sqt[r] >= s.Sq[sh] {
				return false
			}
		}
	}
	return true
}

// MoveClosedWhenFinal (N180, C8-10): a move that has ended leaves no move bit on either ownership row.
func MoveClosedWhenFinal(s State) bool {
	if s.Mp != "done" && s.Mp != "rolled_back" {
		return true
	}
	return !s.Mv[S1] && !s.Mv[S2]
}

// OwnerHasNoStaleMove: an active owner carries no move bit once the move is over.
func OwnerHasNoStaleMove(s State) bool {
	for _, sh := range Shards {
		if s.Own[sh].St == "active" && s.Mv[sh] && Final.Has(s.Mp) {
			return false
		}
	}
	return true
}

// Invariants maps each safety invariant name to its predicate.
var Invariants = map[string]func(State) bool{
	SingleWriterName:               SingleWriter,
	NoLossNoDupName:                NoLossNoDup,
	NoResurrectName:                NoResurrect,
	ServedFromIndexName:            ServedFromIndex,
	SourceStaticUnderFreezeName:    SourceStaticUnderFreeze,
	RollbackPossibleBeforeCName:    RollbackPossibleBeforeC,
	NoWriteToTargetBeforeCName:     NoWriteToTargetBeforeC,
	NoRouteToTargetBeforeCName:     NoRouteToTargetBeforeC,
	ZombieCannotCutOverName:        ZombieCannotCutOver,
	RestoreReconcilesName:          RestoreReconciles,
	OneOwnerName:                   OneOwner,
	CatalogNamesOwnerAfterDoneName: CatalogNamesOwnerAfterDone,
	CleanupSafeName:                CleanupSafe,
	CopiedBelowTargetSeqName:       CopiedBelowTargetSeq,
	MoveClosedWhenFinalName:        MoveClosedWhenFinal,
	OwnerHasNoStaleMoveName:        OwnerHasNoStaleMove,
}

// Check returns the sorted names of the invariants that do not hold in s.
func Check(s State) []string { return tla.Failed(s, Invariants) }

// MoveTerminatesGoal is the right-hand side of the liveness property MoveTerminates (a started move completes or rolls
// back); FrozenBoundedGoal is the right-hand side of FrozenBounded (a frozen move reaches the commit point or rolls
// back). Liveness tests assert them on quiescent states.
func MoveTerminatesGoal(s State) bool { return s.Mp == "done" || s.Mp == "rolled_back" }

// FrozenBoundedGoal: the mover left `frozen` for a post-commit state or rolled back.
func FrozenBoundedGoal(s State) bool { return PostC.Has(s.Mp) || s.Mp == "rolled_back" }
