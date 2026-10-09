package consolidation

import (
	"sort"
	"strconv"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/crosscheck"
	"github.com/gstamatakis95/engram/internal/formal/tla"
	"github.com/gstamatakis95/engram/internal/formal/tlcval"
	"github.com/gstamatakis95/engram/internal/formal/trace"
)

func key(v any) Key {
	t := tlcval.Items(v)
	return Key{Batch: BatchKey(tlcval.IntSet(t[0])), Index: tlcval.Int(t[1])}
}

func op(v any) Op {
	r := tlcval.Rec(v)
	if tlcval.Str(r["kind"]) == "none" {
		return Op{}
	}
	return Op{Kind: tlcval.Str(r["kind"]), O: tlcval.Str(r["o"])}
}

// decode rebuilds the Go value model from a state of a TLC error trace of Consolidation.tla.
func decode(nFacts int, v map[string]any) State {
	s := State{
		Facts:      tla.NewSet[int](),
		ApplyCount: map[Key]int{},
		Applied:    tla.NewSet[Key](),
		EffectOf:   map[Key]Op{},
		Done:       tla.NewSet[string](),
		FinalProp:  map[string][]Op{},
		Obs:        map[string]ObsRow{},
	}
	for i := 1; i <= nFacts; i++ {
		s.Facts.Add(i)
	}
	for _, e := range tlcval.Fn(v["applyCount"]) {
		s.ApplyCount[key(e.K)] = tlcval.Int(e.V)
	}
	for _, k := range tlcval.Items(v["applied"]) {
		s.Applied.Add(key(k))
	}
	for _, e := range tlcval.Fn(v["effectOf"]) {
		s.EffectOf[key(e.K)] = op(e.V)
	}
	for _, b := range tlcval.Items(v["done"]) {
		s.Done.Add(BatchKey(tlcval.IntSet(b)))
	}
	for _, e := range tlcval.Fn(v["finalProp"]) {
		var ops []Op
		for _, o := range tlcval.Items(e.V) {
			ops = append(ops, op(o))
		}
		s.FinalProp[BatchKey(tlcval.IntSet(e.K))] = ops
	}
	for _, e := range tlcval.Fn(v["obs"]) {
		r := tlcval.Rec(e.V)
		s.Obs[tlcval.Str(e.K)] = ObsRow{State: tlcval.Str(r["st"]), Src: tlcval.IntSet(r["src"])}
	}
	return s
}

// TestInvariants_AgreeWithTLC: the Go invariants hold in every state of the counterexamples TLC printed for
// Consolidation_VolatileProposal and Consolidation_NonAtomicKey but the last, which violates ExactlyOnceEffect.
func TestInvariants_AgreeWithTLC(t *testing.T) {
	names := make([]string, 0, len(Invariants))
	for n := range Invariants {
		names = append(names, n)
	}
	sort.Strings(names)
	crosscheck.Verify(t, "Consolidation", names, func(tr crosscheck.Trace, s trace.State) []string {
		n, err := strconv.Atoi(tr.Constants["NFacts"])
		if err != nil {
			t.Fatalf("%s: NFacts: %v", tr.Config, err)
		}
		return Check(decode(n, s.Vars))
	})
}
