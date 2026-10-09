package derivation

import (
	"sort"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/crosscheck"
	"github.com/gstamatakis95/engram/internal/formal/tla"
	"github.com/gstamatakis95/engram/internal/formal/tlcval"
	"github.com/gstamatakis95/engram/internal/formal/trace"
)

// params builds the constants of a configuration: the design instance (DocOfDef and friends are replaced in every
// shipped configuration) plus the knobs the read-time predicates look at.
func params(tr crosscheck.Trace) Params {
	p := Design()
	p.Pages = nil
	if strings.Contains(tr.Constants["Pages"], "p1") {
		p.Pages = []string{"p1"}
	}
	p.TombByVersion = tr.Constants["TombByVersion"] == "TRUE"
	p.ReextractHides = tr.Constants["ReextractHides"] == "TRUE"
	p.MatSignalOnly = tr.Constants["MatSignalOnly"] == "TRUE"
	return p
}

func pair(v any) Pair {
	t := tlcval.Items(v)
	return Pair{Node: tlcval.Str(t[0]), V: tlcval.Int(t[1])}
}

func pairs(v any) tla.Set[Pair] {
	out := tla.NewSet[Pair]()
	for _, e := range tlcval.Items(v) {
		out.Add(pair(e))
	}
	return out
}

// decode rebuilds the Go value model from a state of a TLC error trace of Derivation.tla.
func decode(v map[string]any) State {
	s := State{
		Tomb:     tlcval.FnStr(v["tomb"], tlcval.Int),
		Ms:       tlcval.FnStr(v["ms"], tlcval.Str),
		Hidden:   tlcval.IntSet(v["hidden"]),
		HidRe:    tlcval.IntSet(v["hidRe"]),
		Ctomb:    tlcval.IntSet(v["ctomb"]),
		Born:     tlcval.IntSet(v["born"]),
		Gone:     tlcval.IntSet(v["gone"]),
		Vers:     map[string][]Rec{},
		Hw:       tlcval.FnStr(v["hw"], tlcval.Int),
		Cv:       tlcval.FnStr(v["cv"], tlcval.Int),
		Ginv:     tlcval.IntSet(v["ginv"]),
		Mat:      tlcval.IntSet(v["mat"]),
		MatOwe:   tlcval.IntSet(v["matOwe"]),
		Sig:      tlcval.Bool(v["sig"]),
		MatPhase: tlcval.Str(v["matPhase"]),
		Itag:     tlcval.FnInt(v["itag"], tlcval.Int),
		Clog:     map[int]Clog{},
	}
	for _, e := range tlcval.Fn(v["vers"]) {
		var recs []Rec
		for _, r := range tlcval.Items(e.V) {
			m := tlcval.Rec(r)
			recs = append(recs, Rec{
				Root: tlcval.Int(m["root"]), Finp: tlcval.IntSet(m["finp"]), Gfinp: tlcval.IntSet(m["gfinp"]),
				Oinp: pairs(m["oinp"]), Goinp: pairs(m["goinp"]), Eff: tlcval.Int(m["eff"]),
				Base: tlcval.Int(m["base"]), St: tlcval.Str(m["st"]),
			})
		}
		s.Vers[tlcval.Str(e.K)] = recs
	}
	for _, d := range tlcval.Items(v["dh"]) {
		t := tlcval.Items(d)
		cause := tlcval.Items(t[3])
		s.Dh = append(s.Dh, DhRow{Node: tlcval.Str(t[0]), Root: tlcval.Int(t[1]), From: tlcval.Int(t[2]),
			CauseKind: tlcval.Str(cause[0]), CauseFact: tlcval.Int(cause[1])})
	}
	for _, e := range tlcval.Fn(v["clog"]) {
		r := tlcval.Rec(e.V)
		s.Clog[tlcval.Int(e.K)] = Clog{Act: tlcval.Str(r["act"]), Tag: tlcval.Int(r["tag"])}
	}
	return s
}

// TestInvariants_AgreeWithTLC: the Go invariants hold in every state of the counterexamples TLC printed for the
// must-fail configurations of Derivation.tla but the last, where the invariant EXPECT names fails. This pins the Go
// Served (the SQL rule of N117) and every predicate under it to the specification.
func TestInvariants_AgreeWithTLC(t *testing.T) {
	names := make([]string, 0, len(Invariants))
	for n := range Invariants {
		names = append(names, n)
	}
	sort.Strings(names)
	crosscheck.Verify(t, "Derivation", names, func(tr crosscheck.Trace, s trace.State) []string {
		return Check(params(tr), decode(s.Vars))
	})
}
