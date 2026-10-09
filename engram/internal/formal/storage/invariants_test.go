package storage

import (
	"reflect"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

func empty() State {
	return State{
		Content:  tla.NewSet[int](),
		Payload:  map[int]int{},
		Marked:   tla.NewSet[int](),
		Ingested: tla.NewSet[int](),
		Vec:      tla.NewSet[Entry](),
		Idx:      tla.NewSet[Entry](),
		Cur:      1,
		Snap:     tla.NewSet[int](),
		Pc:       map[string]int{"a": 0, "b": 0},
	}
}

func check(t *testing.T, s State, want ...string) {
	t.Helper()
	got := Check(Design(), s)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

func TestInvariants_HoldOnInitialState(t *testing.T) { check(t, empty()) }

// Storage_Update.cfg (3 states): Insert(1), then Update(1) rewrites the row in place.
func TestInvariants_UpdateCounterexample(t *testing.T) {
	s := empty()
	s.Content.Add(1)
	s.Ingested.Add(1)
	s.Payload[1] = 1
	check(t, s, ContentImmutableName)
}

// Storage_FlipEarly.cfg (3 states): Insert(1), Flip with no next-generation vector, so cur = 2 and the live row has no
// generation-2 vector.
func TestInvariants_FlipEarlyCounterexample(t *testing.T) {
	s := empty()
	s.Content.Add(1)
	s.Ingested.Add(1)
	s.Cur = 2
	s.Snap = tla.NewSet(1)
	check(t, s, VectorGenerationConsistentName)
	s.Vec.Add(Entry{Row: 1, Gen: 2})
	check(t, s)
}

// Storage_PurgeUnmarked.cfg (3 states): Insert(1), Purge(1) with no marker.
func TestInvariants_PurgeUnmarkedCounterexample(t *testing.T) {
	s := empty()
	s.Ingested.Add(1)
	check(t, s, PurgeNeedsMarkerName)
	s.Marked.Add(1)
	check(t, s)
}

// Storage_AutoRepair.cfg (7 states): a purge leaves a dead entry in the graph and autovacuum drops it in place.
func TestInvariants_AutoRepairCounterexample(t *testing.T) {
	s := empty()
	s.Ingested.Add(1)
	s.Marked.Add(1)
	s.Repairs = 1
	check(t, s, RebuildBeforeRepairName)
}

func TestInvariants_DeadEntriesMustBeCounted(t *testing.T) {
	s := empty()
	s.Ingested.Add(1)
	s.Marked.Add(1)
	s.Idx.Add(Entry{Row: 1, Gen: 1}) // the row and its vector are purged; the graph still holds the entry
	check(t, s, DeadCountedName)
	s.Pc["a"] = 1
	check(t, s)
}

func TestConverged(t *testing.T) {
	p := Design()
	s := empty()
	if !Converged(p, s) {
		t.Fatal("an empty partition has converged")
	}
	s.Pc["a"] = 1 // threshold 1: a rebuild is owed
	if Converged(p, s) {
		t.Fatal("a due graph is not converged")
	}
	s.Pc["a"] = 0
	s.Vec.Add(Entry{Row: 1, Gen: 1})
	if Converged(p, s) {
		t.Fatal("a current-generation vector missing from the index is not converged")
	}
}
