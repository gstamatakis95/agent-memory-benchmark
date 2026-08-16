package retrieve

import (
	"math"
	"testing"
)

// Golden test from docs/06-testing.md Tier 0: RRF(d) = Σ 1/(k+rank) with
// k=60; a doc at rank 3 in BM25 and rank 7 in dense scores 1/63 + 1/67.
func TestRRF(t *testing.T) {
	bm25 := []string{"a", "b", "c", "d", "e", "f", "g"} // c is rank 3 (1-indexed)
	dense := []string{"x", "y", "z", "p", "q", "r", "c"} // c is rank 7
	got := RRF(60, bm25, dense)
	want := 1.0/63.0 + 1.0/67.0
	if math.Abs(got["c"]-want) > 1e-9 {
		t.Fatalf("rrf(c)=%v want %v", got["c"], want)
	}
	// Docs absent from a list contribute 0, not 1/k.
	if got["a"] != 1.0/61.0 {
		t.Fatalf("rank-1-only doc wrong: %v", got["a"])
	}
}

func TestRRFConfigurableK(t *testing.T) {
	got := RRF(10, []string{"a"}, []string{"b", "a"})
	want := 1.0/11.0 + 1.0/12.0
	if math.Abs(got["a"]-want) > 1e-9 {
		t.Fatalf("rrf k=10: got %v want %v", got["a"], want)
	}
	if math.Abs(got["b"]-1.0/11.0) > 1e-9 {
		t.Fatalf("rrf k=10 single-list doc: got %v", got["b"])
	}
	if _, ok := got["missing"]; ok {
		t.Fatal("absent doc must not appear in the fused map")
	}
}
