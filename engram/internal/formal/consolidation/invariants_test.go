package consolidation

import (
	"reflect"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

var create = Op{Kind: "create", O: "o1"}

func applied() State {
	k := Key{Batch: "1,2", Index: 1}
	return State{
		Facts:      tla.NewSet(1, 2),
		ApplyCount: map[Key]int{k: 1},
		Applied:    tla.NewSet(k),
		EffectOf:   map[Key]Op{k: create},
		Done:       tla.NewSet("1,2"),
		FinalProp:  map[string][]Op{"1,2": {create}},
		Obs:        map[string]ObsRow{"o1": {State: "live", Src: tla.NewSet(1, 2)}},
	}
}

func TestInvariants_HoldOnAppliedBatch(t *testing.T) {
	if got := Check(applied()); len(got) != 0 {
		t.Fatalf("violations = %v", got)
	}
}

// Consolidation_NonAtomicKey.cfg (5 states): the effect was applied, the worker crashed before the key was recorded and
// the activity re-ran the op: applyCount is 2.
func TestInvariants_NonAtomicKeyCounterexample(t *testing.T) {
	s := applied()
	s.ApplyCount[Key{Batch: "1,2", Index: 1}] = 2
	if got := Check(s); !reflect.DeepEqual(got, []string{ExactlyOnceEffectName}) {
		t.Fatalf("violations = %v", got)
	}
}

// Consolidation_VolatileProposal.cfg (6 states): the proposal lived in the attempt's memory; after a crash the model
// returned a different op list, an index-based key skipped an op that was never applied, and the batch completed with a
// final proposal whose second op has no recorded effect.
func TestInvariants_VolatileProposalCounterexample(t *testing.T) {
	s := applied()
	del := Op{Kind: "delete", O: "o1"}
	s.FinalProp["1,2"] = []Op{create, del}
	s.Applied.Add(Key{Batch: "1,2", Index: 2})
	if got := Check(s); !reflect.DeepEqual(got, []string{ExactlyOnceEffectName}) {
		t.Fatalf("violations = %v", got)
	}
}

func TestInvariants_LiveObservationNeedsSources(t *testing.T) {
	s := applied()
	s.Obs["o1"] = ObsRow{State: "live", Src: tla.NewSet[int]()}
	if got := Check(s); !reflect.DeepEqual(got, []string{ObservationHasSourcesName}) {
		t.Fatalf("violations = %v", got)
	}
	s.Obs["o1"] = ObsRow{State: "retired"}
	if got := Check(s); len(got) != 0 {
		t.Fatalf("retired observation flagged: %v", got)
	}
}

func TestBatchKey_Canonical(t *testing.T) {
	if got := BatchKey(tla.NewSet(3, 1, 2)); got != "1,2,3" {
		t.Fatalf("BatchKey = %q", got)
	}
}
