package shardmove

import (
	"sort"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/crosscheck"
	"github.com/gstamatakis95/engram/internal/formal/tla"
	"github.com/gstamatakis95/engram/internal/formal/tlcval"
	"github.com/gstamatakis95/engram/internal/formal/trace"
)

func sets(v any) map[string]tla.Set[int] { return tlcval.FnStr(v, tlcval.IntSet) }

// decode rebuilds the Go value model from a state of a TLC error trace of ShardMove.tla.
func decode(v map[string]any) State {
	cat := tlcval.Rec(v["cat"])
	own := map[string]Row{}
	for k, r := range tlcval.FnStr(v["own"], tlcval.Rec) {
		own[k] = Row{St: tlcval.Str(r["st"]), Ep: tlcval.Int(r["ep"])}
	}
	var wr []Writer
	for _, e := range tlcval.Fn(v["wr"]) {
		r := tlcval.Rec(e.V)
		wr = append(wr, Writer{Ph: tlcval.Str(r["ph"]), Sh: tlcval.Str(r["sh"])})
	}
	return State{
		Cat: Cat{Sh: tlcval.Str(cat["sh"]), Ep: tlcval.Int(cat["ep"])}, Cm: tlcval.Str(v["cm"]),
		Mp: tlcval.Str(v["mp"]), Own: own, Store: sets(v["store"]), Mk: sets(v["mk"]),
		Ex: tlcval.FnStr(v["ex"], tlcval.Int), Idx: tlcval.FnStr(v["idx"], tlcval.Bool),
		Sq: tlcval.FnStr(v["sq"], tlcval.Int), Sqt: tlcval.FnInt(v["sqt"], tlcval.Int),
		Mv:        tlcval.FnStr(v["mv"], tlcval.Bool),
		Committed: tlcval.IntSet(v["committed"]), Lost: tlcval.IntSet(v["lost"]), Gone: tlcval.IntSet(v["gone"]),
		Wr: wr, FrozenSet: tlcval.IntSet(v["frozenSet"]), FrozenMk: tlcval.IntSet(v["frozenMk"]),
		FrozenEx: tlcval.Int(v["frozenEx"]), ActSet: tlcval.IntSet(v["actSet"]), ActMk: tlcval.IntSet(v["actMk"]),
		Zcut: tlcval.Bool(v["zcut"]), Cleaned: tlcval.Bool(v["cleaned"]), Src: tlcval.Str(v["src"]),
		Tgt: tlcval.Str(v["tgt"]), Me: tlcval.Int(v["me"]), Cdirty: tlcval.Bool(v["cdirty"]),
	}
}

// TestInvariants_AgreeWithTLC: the Go invariants hold in every state of the counterexamples TLC printed for the
// must-fail configurations of ShardMove.tla but the last, where the invariant EXPECT names fails.
func TestInvariants_AgreeWithTLC(t *testing.T) {
	names := make([]string, 0, len(Invariants))
	for n := range Invariants {
		names = append(names, n)
	}
	sort.Strings(names)
	crosscheck.Verify(t, "ShardMove", names, func(_ crosscheck.Trace, s trace.State) []string {
		return Check(decode(s.Vars))
	})
}
