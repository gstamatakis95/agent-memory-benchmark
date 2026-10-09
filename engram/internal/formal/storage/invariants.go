// Package storage is the invariant code of formal/tla/Storage.tla (PLAN.md section 7.2.4, N111 to N113, N138, N152,
// N166(6)): insert-only content, vector generations and index hygiene as pure functions over a plain Go value model of
// the specification's variables.
package storage

import "github.com/gstamatakis95/engram/internal/formal/tla"

// Entry is one vector or index entry <<row, gen>>: a row's vector of an embedding model generation.
type Entry struct {
	Row, Gen int
}

// Params are the constants of a Storage.tla configuration that the invariants read: the namespace of each row (NsOf),
// the namespaces on the partition and each namespace's purge threshold (Thr).
type Params struct {
	NsOf map[int]string
	Ns   []string
	Thr  map[string]int
}

// Design returns the design instance of the specification (NsOfDef, ThrDef): rows 1 and 2 in namespace a (threshold 1),
// row 3 in namespace b (threshold 2).
func Design() Params {
	return Params{
		NsOf: map[int]string{1: "a", 2: "a", 3: "b"},
		Ns:   []string{"a", "b"},
		Thr:  map[string]int{"a": 1, "b": 2},
	}
}

// State mirrors the variables of Storage.tla that the invariants read.
type State struct {
	Content  tla.Set[int]   // rows of facts/chunks
	Payload  map[int]int    // 0 = as inserted, 1 = rewritten in place
	Marked   tla.Set[int]   // rows with an expunge marker
	Ingested tla.Set[int]   // rows ever inserted
	Vec      tla.Set[Entry] // fact_vectors rows
	Idx      tla.Set[Entry] // entries of the partition's partial HNSW graphs
	Cur      int            // the namespace's current embedding model generation
	Snap     tla.Set[int]   // live rows at the last Flip
	Pc       map[string]int // purged_since_build per namespace
	Repairs  int            // dead entries removed in place
}

// Names of the invariants, as they appear in formal/tla/Storage.tla and formal/tla/EXPECT.
const (
	ContentImmutableName           = "ContentImmutable"
	PurgeNeedsMarkerName           = "PurgeNeedsMarker"
	VectorGenerationConsistentName = "VectorGenerationConsistent"
	RebuildBeforeRepairName        = "RebuildBeforeRepair"
	DeadCountedName                = "DeadCounted"
)

// Live is content \ marked.
func (s State) Live() tla.Set[int] { return s.Content.Minus(s.Marked) }

// EntriesOf is the entries of S that belong to namespace n.
func (p Params) EntriesOf(set tla.Set[Entry], n string) tla.Set[Entry] {
	out := tla.NewSet[Entry]()
	for e := range set {
		if p.NsOf[e.Row] == n {
			out.Add(e)
		}
	}
	return out
}

// Due is pc[n] >= Thr[n]: the hygiene rebuild of n's graph is owed.
func (p Params) Due(s State, n string) bool { return s.Pc[n] >= p.Thr[n] }

// ContentImmutable: no content row is rewritten after insert.
func ContentImmutable(_ Params, s State) bool {
	for r := range s.Content {
		if s.Payload[r] != 0 {
			return false
		}
	}
	return true
}

// PurgeNeedsMarker: a row that left the content table carried an expunge marker.
func PurgeNeedsMarker(_ Params, s State) bool {
	return s.Ingested.Minus(s.Content).SubsetOf(s.Marked)
}

// VectorGenerationConsistent: every row that was live at the last Flip and still is has a vector of the current
// generation, so an arm never misses a row because its new vector was not there yet.
func VectorGenerationConsistent(_ Params, s State) bool {
	for r := range s.Snap.Inter(s.Live()) {
		if !s.Vec.Has(Entry{Row: r, Gen: s.Cur}) {
			return false
		}
	}
	return true
}

// RebuildBeforeRepair: only a rebuild removes dead entries, never an in-place repair (N138).
func RebuildBeforeRepair(_ Params, s State) bool { return s.Repairs == 0 }

// DeadCounted: the purge counter accounts for every dead index entry, so the hygiene trigger cannot miss one.
func DeadCounted(p Params, s State) bool {
	for _, n := range p.Ns {
		if p.EntriesOf(s.Idx, n).Minus(s.Vec).Len() > s.Pc[n] {
			return false
		}
	}
	return true
}

// Converged is the body of IndexConvergence (a liveness property, so it is a state predicate for a quiescent state): no
// graph is due and the index holds the current generation's vectors.
func Converged(p Params, s State) bool {
	for _, n := range p.Ns {
		if p.Due(s, n) {
			return false
		}
	}
	for e := range s.Vec {
		if e.Gen == s.Cur && !s.Idx.Has(e) {
			return false
		}
	}
	return true
}

// Invariants maps each safety invariant name to its predicate.
var Invariants = map[string]func(Params, State) bool{
	ContentImmutableName:           ContentImmutable,
	PurgeNeedsMarkerName:           PurgeNeedsMarker,
	VectorGenerationConsistentName: VectorGenerationConsistent,
	RebuildBeforeRepairName:        RebuildBeforeRepair,
	DeadCountedName:                DeadCounted,
}

// Check returns the sorted names of the invariants that do not hold in s.
func Check(p Params, s State) []string {
	return tla.Failed(s, bind(p))
}

func bind(p Params) map[string]func(State) bool {
	out := make(map[string]func(State) bool, len(Invariants))
	for name, f := range Invariants {
		out[name] = func(s State) bool { return f(p, s) }
	}
	return out
}
