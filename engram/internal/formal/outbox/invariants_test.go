package outbox

import (
	"reflect"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

func good() State {
	return State{
		Committed: tla.NewSet(1, 2),
		Declared:  tla.NewSet[int](),
		NsOf:      map[int]string{1: "b", 2: "a"},
		Cursor:    2,
		Log:       map[string][]int{"index": {1, 2}, "kafka": {1, 2, 2}},
	}
}

func TestInvariants_HoldOnConsistentState(t *testing.T) {
	if got := Check(Design(), good()); len(got) != 0 {
		t.Fatalf("consistent state violates %v", got)
	}
}

// The last state of the counterexample of Outbox_NoWatch.cfg (7 states): seq 1 was declared aborted by the relay on
// first sight (Watch = 0), then its writer committed. The declared seq is committed: NoLossSafety fails (the
// configuration checks only that invariant; IdempotentConsumerState, which it does not check, fails with it because the
// cursor is past an undelivered commit).
func TestInvariants_NoWatchCounterexample(t *testing.T) {
	s := State{
		Committed: tla.NewSet(1, 2),
		Declared:  tla.NewSet(1),
		NsOf:      map[int]string{1: "b", 2: "a"},
		Cursor:    1,
		Log:       map[string][]int{"index": {}, "kafka": {}},
	}
	got, want := Check(Design(), s), []string{IdempotentConsumerStateName, NoLossSafetyName}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

// Outbox_Watch1x.cfg ends the same way (a horizon of 1 x Timeout): the writer commits on its last allowed tick after
// the relay gave up on the seq.
func TestInvariants_Watch1xCounterexample(t *testing.T) {
	s := good()
	s.Declared = tla.NewSet(2)
	s.Cursor = 2
	if got := Check(Design(), s); !reflect.DeepEqual(got, []string{NoLossSafetyName}) {
		t.Fatalf("violations = %v", got)
	}
}

func TestInvariants_CursorPastUndeliveredCommit(t *testing.T) {
	s := good()
	s.Log = map[string][]int{"index": {1}, "kafka": {1, 2}}
	got := Check(Design(), s)
	want := []string{IdempotentConsumerStateName, NoLossSafetyName}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

func TestInvariants_DeliveryOrderPerNamespace(t *testing.T) {
	s := good()
	s.NsOf = map[int]string{1: "a", 2: "a"}
	s.Log = map[string][]int{"index": {2, 1}, "kafka": {1, 2}}
	if got := Check(Design(), s); !reflect.DeepEqual(got, []string{PerNamespaceOrderName}) {
		t.Fatalf("violations = %v", got)
	}
	// duplicates after a relay crash are not an order violation: the first delivery counts
	s.Log = map[string][]int{"index": {1, 1, 2}, "kafka": {1, 2, 1}}
	if got := Check(Design(), s); len(got) != 0 {
		t.Fatalf("duplicate delivery flagged: %v", got)
	}
}

func TestInvariants_UncommittedNeverDelivered(t *testing.T) {
	s := good()
	s.Log = map[string][]int{"index": {1, 2, 3}, "kafka": {1, 2}}
	if got := Check(Design(), s); !reflect.DeepEqual(got, []string{OnlyCommittedDeliveredName}) {
		t.Fatalf("violations = %v", got)
	}
}

// TLA+ quantifies over the constant Consumers, not over the consumers that happen to appear in the state: a consumer
// that has received nothing while the cursor passed a committed seq violates NoLossSafety and IdempotentConsumerState.
func TestInvariants_ConsumerWithoutDeliveries(t *testing.T) {
	p := Params{Consumers: []string{"c1", "c2"}}
	s := State{
		Committed: tla.NewSet(1),
		Declared:  tla.NewSet[int](),
		NsOf:      map[int]string{1: "a"},
		Cursor:    1,
		Log:       map[string][]int{"c1": {1}},
	}
	got, want := Check(p, s), []string{IdempotentConsumerStateName, NoLossSafetyName}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
	s.Log["c2"] = []int{1}
	if got := Check(p, s); len(got) != 0 {
		t.Fatalf("violations = %v", got)
	}
}
