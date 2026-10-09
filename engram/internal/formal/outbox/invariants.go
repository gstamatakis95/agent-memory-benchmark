// Package outbox is the invariant code of formal/tla/Outbox.tla (PLAN.md section 7.2.5, D6, N80): the safety properties
// of the per-shard transactional outbox as pure functions over a plain Go value model of the specification's variables.
// The relay and the store tests build a State from what they observe and call Check after every step.
package outbox

import "github.com/gstamatakis95/engram/internal/formal/tla"

// Params are the constants of an Outbox.tla configuration that the invariants quantify over: the consumers, which the
// state does not necessarily mention (a consumer with no delivery yet is still checked).
type Params struct {
	Consumers []string
}

// Design returns the consumers of every shipped configuration.
func Design() Params { return Params{Consumers: []string{"index", "kafka"}} }

// State mirrors the variables of Outbox.tla that the invariants read. Seqs are 1..MaxSeq; NsOf maps a drawn seq to its
// namespace ("" for an undrawn one, the spec's NoNs); Log is the delivery log per consumer (duplicates possible).
type State struct {
	Committed tla.Set[int]
	Declared  tla.Set[int]
	NsOf      map[int]string
	Cursor    int
	Log       map[string][]int
}

// Names of the invariants, as they appear in formal/tla/Outbox.tla and formal/tla/EXPECT.
const (
	NoLossSafetyName            = "NoLossSafety"
	PerNamespaceOrderName       = "PerNamespaceOrder"
	OnlyCommittedDeliveredName  = "OnlyCommittedDelivered"
	IdempotentConsumerStateName = "IdempotentConsumerState"
)

func contains(xs []int, x int) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// NoLossSafety: the cursor never passes a committed seq without delivering it, and a seq the relay declared aborted
// never turns out to be committed.
func NoLossSafety(p Params, s State) bool {
	if s.Declared.Intersects(s.Committed) {
		return false
	}
	for q := range s.Committed {
		if q <= s.Cursor {
			for _, c := range p.Consumers {
				if !contains(s.Log[c], q) {
					return false
				}
			}
		}
	}
	return true
}

// dedup keeps the first delivery of each seq (consumers deduplicate by seq).
func dedup(log []int) []int {
	seen := make(map[int]bool, len(log))
	out := make([]int, 0, len(log))
	for _, q := range log {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	return out
}

// PerNamespaceOrder: the first delivery of each seq happens in seq order within every namespace of every consumer.
func PerNamespaceOrder(p Params, s State) bool {
	for _, c := range p.Consumers {
		last := map[string]int{}
		for _, q := range dedup(s.Log[c]) {
			ns := s.NsOf[q]
			if prev, ok := last[ns]; ok && prev >= q {
				return false
			}
			last[ns] = q
		}
	}
	return true
}

// OnlyCommittedDelivered: every delivered seq is committed.
func OnlyCommittedDelivered(p Params, s State) bool {
	for _, c := range p.Consumers {
		for _, q := range s.Log[c] {
			if !s.Committed.Has(q) {
				return false
			}
		}
	}
	return true
}

// IdempotentConsumerState: the committed seqs at or below the persisted cursor are in every consumer's log.
func IdempotentConsumerState(p Params, s State) bool {
	for q := range s.Committed {
		if q <= s.Cursor {
			for _, c := range p.Consumers {
				if !contains(s.Log[c], q) {
					return false
				}
			}
		}
	}
	return true
}

// Invariants maps each invariant name to its predicate.
var Invariants = map[string]func(Params, State) bool{
	NoLossSafetyName:            NoLossSafety,
	PerNamespaceOrderName:       PerNamespaceOrder,
	OnlyCommittedDeliveredName:  OnlyCommittedDelivered,
	IdempotentConsumerStateName: IdempotentConsumerState,
}

// Check returns the sorted names of the invariants that do not hold in s.
func Check(p Params, s State) []string {
	bound := make(map[string]func(State) bool, len(Invariants))
	for name, f := range Invariants {
		bound[name] = func(st State) bool { return f(p, st) }
	}
	return tla.Failed(s, bound)
}
