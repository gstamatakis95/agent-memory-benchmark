package shardmove

import (
	"reflect"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

func set(xs ...int) tla.Set[int] { return tla.NewSet(xs...) }

// initial is the Init state of the specification: s1 owns the namespace at epoch 1, s2 holds nothing.
func initial() State {
	return State{
		Cat:       Cat{S1, 1},
		Cm:        "none",
		Mp:        "none",
		Own:       map[string]Row{S1: {"active", 1}, S2: {"none", 0}},
		Store:     map[string]tla.Set[int]{S1: set(), S2: set()},
		Mk:        map[string]tla.Set[int]{S1: set(), S2: set()},
		Ex:        map[string]int{S1: 0, S2: 0},
		Idx:       map[string]bool{S1: true, S2: false},
		Sq:        map[string]int{S1: 2, S2: 0},
		Sqt:       map[int]int{},
		Mv:        map[string]bool{S1: false, S2: false},
		Committed: set(), Lost: set(), Gone: set(),
		Wr:        []Writer{{Ph: "idle", Sh: S1}},
		FrozenSet: set(), FrozenMk: set(), ActSet: set(), ActMk: set(),
		Src: S1, Tgt: S2, Me: 1,
	}
}

// moved is the state after a clean move of row 1: the source is moved_out, the target is active with the row.
func moved() State {
	s := initial()
	s.Cat = Cat{S2, 2}
	s.Cm, s.Mp, s.Me = "done", "done", 2
	s.Own = map[string]Row{S1: {"moved_out", 2}, S2: {"active", 2}}
	s.Store = map[string]tla.Set[int]{S1: set(1), S2: set(1)}
	s.Idx = map[string]bool{S1: true, S2: true}
	s.Sq = map[string]int{S1: 3, S2: 3}
	s.Sqt = map[int]int{1: 2}
	s.Committed = set(1)
	s.FrozenSet, s.ActSet = set(1), set(1)
	return s
}

func check(t *testing.T, s State, want ...string) {
	t.Helper()
	got := Check(s)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

func has(t *testing.T, s State, name string) {
	t.Helper()
	for _, g := range Check(s) {
		if g == name {
			return
		}
	}
	t.Fatalf("%s holds in a state that must violate it (violations: %v)", name, Check(s))
}

func TestInvariants_HoldOnInitialAndMovedStates(t *testing.T) {
	check(t, initial())
	check(t, moved())
}

// ShardMove_ReadyBeforeIndex.cfg (6 states): the target became ready without its index.
func TestInvariants_ReadyBeforeIndexCounterexample(t *testing.T) {
	s := initial()
	s.Mp = "ready"
	s.Cm = "open"
	s.Own = map[string]Row{S1: {"frozen", 1}, S2: {"ready", 2}}
	s.Mv = map[string]bool{S1: true, S2: true}
	s.Me = 2
	s.Store[S1], s.Store[S2] = set(1), set(1)
	s.FrozenSet = set(1)
	s.Committed = set(1)
	s.Sqt = map[int]int{1: 2}
	s.Sq[S2] = 3
	check(t, s, ServedFromIndexName)
	s.Idx[S2] = true
	check(t, s)
}

// ShardMove_SweepNotPaused.cfg (6 states): the expiring-class sweep ran under the freeze, so the source changed.
func TestInvariants_SweepNotPausedCounterexample(t *testing.T) {
	s := initial()
	s.Mp, s.Cm, s.Me = "copied", "open", 2
	s.Own = map[string]Row{S1: {"frozen", 1}, S2: {"incoming", 2}}
	s.Mv = map[string]bool{S1: true, S2: true}
	s.Ex[S1], s.FrozenEx = 0, 1
	check(t, s, SourceStaticUnderFreezeName)
	s.Ex[S1] = 1
	check(t, s)
}

// ShardMove_NoSeqAdvance.cfg (12 states): the target's sequence was not raised, so a moved row sorts above it.
func TestInvariants_NoSeqAdvanceCounterexample(t *testing.T) {
	s := moved()
	s.Sq[S2] = 0
	s.Sqt = map[int]int{1: 2}
	check(t, s, CopiedBelowTargetSeqName)
}

// ShardMove_UnionRepair.cfg (19 states): a repair re-added a row the target had deleted.
func TestInvariants_UnionRepairCounterexample(t *testing.T) {
	s := moved()
	s.Gone = set(1)
	has(t, s, NoResurrectName)
}

// ShardMove_RestoreKeepsMove.cfg (14 states): restore_done left the move bit set on a finished move.
func TestInvariants_RestoreKeepsMoveCounterexample(t *testing.T) {
	s := moved()
	s.Mv[S2] = true
	has(t, s, MoveClosedWhenFinalName)
	has(t, s, OwnerHasNoStaleMoveName)
	s.Mv[S2] = false
	check(t, s)
}

// ShardMove_EndOnRouting.cfg and _StampAfterCut.cfg: the mover ended `done` (or a stamp came after (c)) while no shard
// is active; the target is still `ready`.
func TestInvariants_NoActiveOwnerCounterexample(t *testing.T) {
	s := moved()
	s.Own[S2] = Row{"ready", 2}
	has(t, s, OneOwnerName)
}

// ShardMove_CatalogLossDuringFreeze.cfg (11 states): the reconcile took the epoch from the target's incoming row, so
// the catalog names the source at an epoch it does not hold while (d) is pending.
func TestInvariants_CatalogLossDuringFreezeCounterexample(t *testing.T) {
	s := moved()
	s.Mp = "tactive"
	s.Cm = "committed"
	s.Cat = Cat{S1, 2}
	has(t, s, CatalogNamesOwnerAfterDoneName)
	s.Cat = Cat{S1, 1}
	check(t, s)
	s.Cat = Cat{S2, 2}
	check(t, s)
}

// ShardMove_NoReady.cfg (11 states): no ready state, the target is active before the CAS while rollback is still
// possible.
func TestInvariants_NoReadyCounterexample(t *testing.T) {
	s := moved()
	s.Mp, s.Cm = "early", "open"
	s.Own = map[string]Row{S1: {"frozen", 1}, S2: {"active", 2}}
	s.Cat = Cat{S1, 1}
	has(t, s, NoWriteToTargetBeforeCName)
}

// ShardMove_RestoreNoReconcile.cfg (12 states): a restored source trusted its backup row; the catalog epoch is ahead.
func TestInvariants_RestoreNoReconcileCounterexample(t *testing.T) {
	s := initial()
	s.Cat = Cat{S1, 2}
	has(t, s, RestoreReconcilesName)
}

// ShardMove_NoVerify, _CopyBeforeFreeze, _UnfencedSteps, _CatalogLossNoShardTruth, _CutOnUnreplicatedCommit, _NoFloor,
// _LaggingFailover and _ThawOnUnreplicatedAbort all end with an activated target that lacks an acknowledged row.
func TestInvariants_RowLostCounterexample(t *testing.T) {
	s := moved()
	s.Store[S2] = set()
	s.ActSet = set()
	has(t, s, NoLossNoDupName)
	s.Store[S2] = set(1)
	s.ActSet = set(1)
	check(t, s)
	// the row is an accepted RPO loss, or was deleted on the target: not a violation
	s.Store[S2], s.ActSet = set(), set()
	s.Lost = set(1)
	s.FrozenSet = set(1)
	s.Gone = set(1)
	check(t, s)
}

// An unfenced second writer, a writer on a frozen shard, a zombie mover and a route to the target before (c).
func TestInvariants_SingleWriterAndRouting(t *testing.T) {
	s := moved()
	s.Own[S1] = Row{"active", 2}
	has(t, s, SingleWriterName)
	s = initial()
	s.Own[S1] = Row{"frozen", 1}
	s.Wr = []Writer{{Ph: "hold", Sh: S1}}
	has(t, s, SingleWriterName)
	s = initial()
	s.Mp, s.Cm, s.Cat = "ready", "open", Cat{S2, 2}
	has(t, s, NoRouteToTargetBeforeCName)
	s = initial()
	s.Zcut = true
	has(t, s, ZombieCannotCutOverName)
}

func TestInvariants_RollbackNeedsSourceRows(t *testing.T) {
	s := initial()
	s.Mp, s.Cm, s.Me = "frozen", "open", 2
	s.Own = map[string]Row{S1: {"frozen", 1}, S2: {"incoming", 2}}
	s.Committed = set(1)
	has(t, s, RollbackPossibleBeforeCName)
	s.Store[S1] = set(1)
	s.FrozenSet = set(1)
	check(t, s)
	s.Own[S1] = Row{"moved_out", 1}
	has(t, s, RollbackPossibleBeforeCName)
}

func TestInvariants_CleanupNeverRemovesTheOnlyCopy(t *testing.T) {
	s := moved()
	s.Cleaned = true
	s.Store[S1] = set()
	check(t, s)
	s.Store[S2] = set()
	has(t, s, CleanupSafeName)
}

func TestLivenessGoals(t *testing.T) {
	s := initial()
	s.Mp = "cut"
	if MoveTerminatesGoal(s) || !FrozenBoundedGoal(s) {
		t.Fatal("cut is past the commit point but not final")
	}
	s.Mp = "done"
	if !MoveTerminatesGoal(s) {
		t.Fatal("done terminates")
	}
}

// NoLossNoDup, third conjunct: after the cleanup an active target holds every frozen row it did not delete itself, even
// when the mover state is neither tactive/done (first conjunct) nor settled-final (fourth).
func TestNoLossNoDup_CleanedActiveTargetHoldsFrozenRows(t *testing.T) {
	s := moved()
	s.Mp = "cut"
	s.Cleaned = true
	s.Store[S2] = set()
	s.ActSet = set()
	has(t, s, NoLossNoDupName)
	s.Store[S2] = set(1)
	if !NoLossNoDup(s) {
		t.Fatal("the target holds the frozen row")
	}
}

// MoveClosedWhenFinal looks at both bits: a bit left on the source alone violates it.
func TestMoveClosedWhenFinal_SourceBitAlone(t *testing.T) {
	s := moved()
	s.Mv[S1] = true
	has(t, s, MoveClosedWhenFinalName)
	s.Mv[S1], s.Mv[S2] = false, true
	has(t, s, MoveClosedWhenFinalName)
	s.Mv[S2] = false
	s.Mp = "frozen"
	s.Mv[S1] = true
	if !MoveClosedWhenFinal(s) {
		t.Fatal("a move in flight holds its bits")
	}
}

// RestoreReconciles: a moved_out row at an epoch above the active shard's means the active one is stale.
func TestRestoreReconciles_MovedOutEpoch(t *testing.T) {
	s := initial()
	s.Own = map[string]Row{S1: {"moved_out", 3}, S2: {"active", 2}}
	s.Cat = Cat{S2, 2}
	has(t, s, RestoreReconcilesName)
	s.Own[S1] = Row{"moved_out", 2}
	if !RestoreReconciles(s) {
		t.Fatal("a moved_out row at an epoch not above the owner's is fine")
	}
}
