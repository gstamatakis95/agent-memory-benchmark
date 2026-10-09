package durability

import (
	"reflect"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

func doc() Params  { return Params{Shape: ShapeDoc} }
func fact() Params { return Params{Shape: ShapeFact} }

func fresh(n int) State {
	ops := map[int]Op{}
	for i := 1; i <= n; i++ {
		ops[i] = Op{Ph: "none", Eff: tla.NewSet[int](), Rd: -1}
	}
	return State{Ops: ops, Intents: tla.NewSet[int](), Sst: "active"}
}

func check(t *testing.T, p Params, s State, want ...string) {
	t.Helper()
	got := Check(p, s)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

func TestInvariants_HoldOnInitialState(t *testing.T) { check(t, doc(), fresh(3)) }

// A committed, intent-backed, acknowledged delete that the shard still applies violates nothing.
func TestInvariants_AckedDeleteInForce(t *testing.T) {
	s := fresh(3)
	s.Ops[1] = Op{Ph: "acked", Cn0: 1, Obs: 1, Eff: tla.NewSet(0), Rd: -1}
	s.Intents.Add(1)
	s.Dbq = []Entry{{Op: 1, Eff: tla.NewSet(0), Ep: 1}}
	check(t, doc(), s)
}

// Durability_AckBeforeIntent.cfg (4 states): Issue, Commit, Ack with no intent put.
func TestInvariants_AckBeforeIntentCounterexample(t *testing.T) {
	s := fresh(3)
	s.Ops[1] = Op{Ph: "acked", Cn0: 1, Obs: 1, Eff: tla.NewSet(0), Rd: -1}
	s.Dbq = []Entry{{Op: 1, Eff: tla.NewSet(0), Ep: 1}}
	check(t, doc(), s, AckImpliesIntentName)
}

// Durability_CatalogRestoreInFenceWindow.cfg (3 states): the tenant delete is acknowledged after the `deleting` row
// (tdp 1), then a catalog restore reverts the row (tdp 4).
func TestInvariants_CatalogRestoreInFenceWindowCounterexample(t *testing.T) {
	s := fresh(3)
	s.Ta, s.Tdp = true, 4
	check(t, doc(), s, AckedDeleteSurvivesName)
	s.Ta = false // the operation was not yet acknowledged: FAILED{CATALOG_RESTORED}
	check(t, doc(), s)
}

// Durability_RaiseOnReopen.cfg, _RetargetRestore, _DupNoReput, _AckNoRecheck, _ReopenEarly, _NarrowWindow and
// _CatalogLossNoBlobFloor all end the same way: the shard serves again and the marker of an acknowledged delete is
// gone.
func TestInvariants_AckedDeleteLostByRestoreCounterexample(t *testing.T) {
	s := fresh(3)
	s.Ops[1] = Op{Ph: "acked", Cn0: 1, Obs: 1, Eff: tla.NewSet(0), Rd: -1}
	s.Intents.Add(1)
	s.Sst = "active"
	check(t, doc(), s, AckedDeleteSurvivesName)
	s.Sst = "restoring" // reads are rejected until the replay finished
	check(t, doc(), s)
}

// Durability_IntentBeforeCommit.cfg (8 states): the intent of an attempt that never committed is replayed.
func TestInvariants_OrphanIntentReplayedCounterexample(t *testing.T) {
	s := fresh(3)
	s.Ops[1] = Op{Ph: "failed", Cn0: 0, Eff: tla.NewSet[int](), Rd: -1}
	s.Intents.Add(1)
	s.Dbq = []Entry{{Op: 1, Eff: tla.NewSet(0), Ep: 1}}
	check(t, doc(), s, NoUnackedEffectOnLaterAckName)
}

// A delete that covers a version committed after it (a replay over a later write).
func TestInvariants_EffectCoversLaterVersion(t *testing.T) {
	s := fresh(3)
	s.Ops[1] = Op{Ph: "committed", Cn0: 1, Obs: 1, Eff: tla.NewSet(0, 3), Rd: -1}
	s.Ops[3] = Op{Ph: "acked", Cn0: 2, Obs: 3, Eff: tla.NewSet[int](), Rd: -1}
	s.Dbq = []Entry{{Op: 1, Eff: tla.NewSet(0, 3), Ep: 1}}
	check(t, doc(), s, NoUnackedEffectOnLaterAckName)
}

// Durability_ClockOrder.cfg, _UnorderedReplay, _NoEpochGuard, _ReplayStampsCurrentEpoch, _NoSubjectLock and
// _NoHelpPrev: an Invalidate (op 1) is replayed after the Restore (op 2) that committed after it, so the fact ends
// hidden although the acknowledged request was the Restore.
func TestInvariants_ReplayOutOfChainOrderCounterexample(t *testing.T) {
	s := fresh(3)
	s.Ops[1] = Op{Ph: "acked", Cn0: 1, Obs: 1, Eff: tla.NewSet[int](), Rd: -1}
	s.Ops[2] = Op{Ph: "acked", Cn0: 2, Obs: 2, Eff: tla.NewSet[int](), Prev: 1, Rd: -1}
	s.Intents.Add(1, 2)
	s.Dbq = []Entry{{Op: 2, Eff: tla.NewSet[int](), Ep: 2}, {Op: 1, Eff: tla.NewSet[int](), Ep: 1}}
	check(t, fact(), s, IntentOrderLastWinsName)
	s.Dbq = []Entry{s.Dbq[1], s.Dbq[0]} // chain order: Invalidate, then Restore
	check(t, fact(), s)
}

func TestHelpers(t *testing.T) {
	p := fact()
	s := fresh(3)
	if p.LastOn(s, "x") != 0 || p.HiddenX(s) || p.LastCommitted(s, "x") != 0 {
		t.Fatal("empty state has no last entry")
	}
	s.Dbq = []Entry{{Op: 1, Eff: tla.NewSet[int]()}, {Op: 3, Eff: tla.NewSet[int]()}}
	if p.LastOn(s, "x") != 3 || !p.HiddenX(s) {
		t.Fatalf("LastOn = %d", p.LastOn(s, "x"))
	}
	if !s.DB().Equal(tla.NewSet(1, 3)) {
		t.Fatal("DB")
	}
}
