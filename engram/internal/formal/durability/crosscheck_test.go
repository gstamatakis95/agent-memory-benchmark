package durability

import (
	"sort"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/crosscheck"
	"github.com/gstamatakis95/engram/internal/formal/tla"
	"github.com/gstamatakis95/engram/internal/formal/tlcval"
	"github.com/gstamatakis95/engram/internal/formal/trace"
)

func params(tr crosscheck.Trace) Params {
	switch shape := tr.Constants["Shape"]; {
	case strings.Contains(shape, "ShapeFact"):
		return Params{Shape: ShapeFact}
	case strings.Contains(shape, "ShapeDoc"):
		return Params{Shape: ShapeDoc}
	default:
		panic("durability: unknown Shape " + shape)
	}
}

// decode rebuilds the Go value model from a state of a TLC error trace of Durability.tla.
func decode(v map[string]any) State {
	ops := map[int]Op{}
	for i, o := range tlcval.Items(v["ops"]) {
		r := tlcval.Rec(o)
		ops[i+1] = Op{
			Ph: tlcval.Str(r["ph"]), Ep: tlcval.Int(r["ep"]), It: tlcval.Int(r["it"]), At: tlcval.Int(r["at"]),
			Ct: tlcval.Int(r["ct"]), Cn0: tlcval.Int(r["cn0"]), Eff: tlcval.IntSet(r["eff"]),
			Prev: tlcval.Int(r["prev"]), Obs: tlcval.Int(r["obs"]), Rd: tlcval.Int(r["rd"]), Be: tlcval.Int(r["be"]),
		}
	}
	var dbq []Entry
	for _, e := range tlcval.Items(v["dbq"]) {
		r := tlcval.Rec(e)
		dbq = append(dbq, Entry{Op: tlcval.Int(r["op"]), Eff: tlcval.IntSet(r["eff"]), Ep: tlcval.Int(r["ep"])})
	}
	return State{
		Ops: ops, Intents: tla.NewSet(tlcval.Ints(v["intents"])...), Dbq: dbq, Sst: tlcval.Str(v["sst"]),
		Ta: tlcval.Bool(v["ta"]), Tdp: tlcval.Int(v["tdp"]),
	}
}

// TestInvariants_AgreeWithTLC: the Go invariants hold in every state of the counterexamples TLC printed for the
// must-fail configurations of Durability.tla but the last, where the invariant EXPECT names fails.
func TestInvariants_AgreeWithTLC(t *testing.T) {
	names := make([]string, 0, len(Invariants))
	for n := range Invariants {
		names = append(names, n)
	}
	sort.Strings(names)
	crosscheck.Verify(t, "Durability", names, func(tr crosscheck.Trace, s trace.State) []string {
		return Check(params(tr), decode(s.Vars))
	})
}
