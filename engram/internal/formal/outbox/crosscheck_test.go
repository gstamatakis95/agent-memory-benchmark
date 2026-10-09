package outbox

import (
	"sort"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/crosscheck"
	"github.com/gstamatakis95/engram/internal/formal/tlcval"
	"github.com/gstamatakis95/engram/internal/formal/trace"
)

// params reads the consumers of the configuration (Consumers = {index, kafka}).
func params(tr crosscheck.Trace) Params {
	raw := strings.Trim(tr.Constants["Consumers"], "{}")
	var cs []string
	for _, c := range strings.Split(raw, ",") {
		cs = append(cs, strings.TrimSpace(c))
	}
	return Params{Consumers: cs}
}

// decode rebuilds the Go value model from a state of a TLC error trace of Outbox.tla.
func decode(v map[string]any) State {
	nsOf := map[int]string{}
	for _, e := range tlcval.Fn(v["nsOf"]) {
		if ns := tlcval.Str(e.V); ns != "NoNs" {
			nsOf[tlcval.Int(e.K)] = ns
		}
	}
	return State{
		Committed: tlcval.IntSet(v["committed"]),
		Declared:  tlcval.IntSet(v["declared"]),
		NsOf:      nsOf,
		Cursor:    tlcval.Int(v["cursor"]),
		Log:       tlcval.FnStr(v["log"], tlcval.Ints),
	}
}

// TestInvariants_AgreeWithTLC: every state of the counterexamples TLC printed for Outbox_NoWatch and Outbox_Watch1x
// satisfies the configuration's invariants, but the last, which violates NoLossSafety (formal/tla/results).
func TestInvariants_AgreeWithTLC(t *testing.T) {
	names := make([]string, 0, len(Invariants))
	for n := range Invariants {
		names = append(names, n)
	}
	sort.Strings(names)
	crosscheck.Verify(t, "Outbox", names, func(tr crosscheck.Trace, s trace.State) []string {
		return Check(params(tr), decode(s.Vars))
	})
}
