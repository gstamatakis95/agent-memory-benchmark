package storage

import (
	"sort"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/crosscheck"
	"github.com/gstamatakis95/engram/internal/formal/tla"
	"github.com/gstamatakis95/engram/internal/formal/tlcval"
	"github.com/gstamatakis95/engram/internal/formal/trace"
)

func entries(v any) tla.Set[Entry] {
	out := tla.NewSet[Entry]()
	for _, e := range tlcval.Items(v) {
		t := tlcval.Items(e)
		out.Add(Entry{Row: tlcval.Int(t[0]), Gen: tlcval.Int(t[1])})
	}
	return out
}

// decode rebuilds the Go value model from a state of a TLC error trace of Storage.tla.
func decode(v map[string]any) State {
	return State{
		Content:  tlcval.IntSet(v["content"]),
		Payload:  tlcval.FnInt(v["payload"], tlcval.Int),
		Marked:   tlcval.IntSet(v["marked"]),
		Ingested: tlcval.IntSet(v["ingested"]),
		Vec:      entries(v["vec"]),
		Idx:      entries(v["idx"]),
		Cur:      tlcval.Int(v["cur"]),
		Snap:     tlcval.IntSet(v["snap"]),
		Pc:       tlcval.FnStr(v["pc"], tlcval.Int),
		Repairs:  tlcval.Int(v["repairs"]),
	}
}

// TestInvariants_AgreeWithTLC: the Go invariants hold in every state of the counterexamples TLC printed for the
// must-fail configurations of Storage.tla but the last, where the invariant EXPECT names fails. Every shipped
// configuration uses the design instance (NsOfDef, ThrDef).
func TestInvariants_AgreeWithTLC(t *testing.T) {
	names := make([]string, 0, len(Invariants))
	for n := range Invariants {
		names = append(names, n)
	}
	sort.Strings(names)
	crosscheck.Verify(t, "Storage", names, func(_ crosscheck.Trace, s trace.State) []string {
		return Check(Design(), decode(s.Vars))
	})
}
