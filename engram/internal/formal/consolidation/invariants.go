// Package consolidation is the invariant code of formal/tla/Consolidation.tla (PLAN.md section 7.2.5, D12, N43, N121):
// a round's decisions are applied exactly once under at-least-once activities and crashes. The functions read a plain
// Go value model of the specification's variables.
package consolidation

import (
	"strconv"
	"strings"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

// Op is one decision of a persisted proposal (the spec's [kind, o]); the zero value is the spec's NoOp.
type Op struct {
	Kind string // "create", "update", "delete" or "" (no effect)
	O    string // the observation id the decision references
}

// Key is an op key (batch_key, op_index) with a 1-based index; the batch key is the sorted fact-id set rendered by
// BatchKey.
type Key struct {
	Batch string
	Index int
}

// BatchKey renders a batch (a set of fact ids) as its canonical key: the sorted ids joined by commas. The specification
// models batch_key = sha256(sorted fact ids || prompt || model) as the fact set itself.
func BatchKey(facts tla.Set[int]) string {
	ids := tla.Sorted(facts)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return strings.Join(parts, ",")
}

// ObsRow is one entry of the observation table: State is "absent", "live" or "retired".
type ObsRow struct {
	State string
	Src   tla.Set[int]
}

// State mirrors the variables of Consolidation.tla that the invariants read.
type State struct {
	Facts      tla.Set[int]      // the facts of the round, 1..NFacts
	ApplyCount map[Key]int       // how many times each op's effect was applied
	Applied    tla.Set[Key]      // op keys recorded in consolidation_applied
	EffectOf   map[Key]Op        // the op whose effect was applied under a key
	Done       tla.Set[string]   // completed batches
	FinalProp  map[string][]Op   // batch -> proposal in force when the batch completed
	Obs        map[string]ObsRow // observation id -> row
}

// Names of the invariants, as they appear in formal/tla/Consolidation.tla and formal/tla/EXPECT.
const (
	ExactlyOnceEffectName     = "ExactlyOnceEffect"
	ObservationHasSourcesName = "ObservationHasSources"
)

// ExactlyOnceEffect: each op's effect is applied at most once, and every op of a completed batch was applied exactly as
// the batch's final proposal states it.
func ExactlyOnceEffect(s State) bool {
	for _, n := range s.ApplyCount {
		if n > 1 {
			return false
		}
	}
	for b := range s.Done {
		for i, op := range s.FinalProp[b] {
			k := Key{Batch: b, Index: i + 1}
			if !s.Applied.Has(k) || s.ApplyCount[k] != 1 || s.EffectOf[k] != op {
				return false
			}
		}
	}
	return true
}

// ObservationHasSources: every live observation cites at least one fact of the round.
func ObservationHasSources(s State) bool {
	for _, o := range s.Obs {
		if o.State == "live" && (o.Src.Empty() || !o.Src.SubsetOf(s.Facts)) {
			return false
		}
	}
	return true
}

// Invariants maps each invariant name to its predicate.
var Invariants = map[string]func(State) bool{
	ExactlyOnceEffectName:     ExactlyOnceEffect,
	ObservationHasSourcesName: ObservationHasSources,
}

// Check returns the sorted names of the invariants that do not hold in s.
func Check(s State) []string { return tla.Failed(s, Invariants) }
