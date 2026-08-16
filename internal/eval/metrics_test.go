package eval

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Hand-computed goldens: retrieved [a b c d e], gold {b, e}.
func TestMetricsGolden(t *testing.T) {
	retrieved := []string{"a", "b", "c", "d", "e"}
	gold := []string{"b", "e"}

	assert.InDelta(t, 0.0, RecallAtK(retrieved, gold, 1), 1e-9)
	assert.InDelta(t, 0.5, RecallAtK(retrieved, gold, 3), 1e-9)
	assert.InDelta(t, 1.0, RecallAtK(retrieved, gold, 5), 1e-9)

	assert.InDelta(t, 0.5, MRR(retrieved, gold), 1e-9, "first hit at rank 2")

	// DCG@5 = 1/log2(3) + 1/log2(6)  (hits at positions 2 and 5)
	// IDCG@5 = 1/log2(2) + 1/log2(3) (two golds, ideally at the top)
	wantNDCG5 := (1/math.Log2(3) + 1/math.Log2(6)) / (1 + 1/math.Log2(3))
	assert.InDelta(t, wantNDCG5, NDCGAtK(retrieved, gold, 5), 1e-9)

	// At k=2 only the rank-2 hit counts; ideal still has 2 slots.
	wantNDCG2 := (1 / math.Log2(3)) / (1 + 1/math.Log2(3))
	assert.InDelta(t, wantNDCG2, NDCGAtK(retrieved, gold, 2), 1e-9)
}

func TestMetricsEdgeCases(t *testing.T) {
	// Perfect single-item retrieval.
	assert.InDelta(t, 1.0, RecallAtK([]string{"b"}, []string{"b"}, 1), 1e-9)
	assert.InDelta(t, 1.0, NDCGAtK([]string{"b"}, []string{"b"}, 1), 1e-9)
	assert.InDelta(t, 1.0, MRR([]string{"b"}, []string{"b"}), 1e-9)

	// Duplicated retrieved ids count once.
	assert.InDelta(t, 0.5, RecallAtK([]string{"b", "b"}, []string{"b", "e"}, 2), 1e-9)
	assert.InDelta(t, 1.0, NDCGAtK([]string{"b", "b"}, []string{"b"}, 2), 1e-9)

	// No gold retrieved.
	assert.Zero(t, MRR([]string{"x", "y"}, []string{"b"}))
	assert.Zero(t, RecallAtK([]string{"x"}, []string{"b"}, 1))
	assert.Zero(t, NDCGAtK([]string{"x"}, []string{"b"}, 1))

	// Empty gold scores 0 (Evaluate skips these entirely).
	assert.Zero(t, RecallAtK([]string{"x"}, nil, 1))
	assert.Zero(t, NDCGAtK([]string{"x"}, nil, 1))
	assert.Zero(t, MRR([]string{"x"}, nil))
}

func TestEvaluateBreakdownAndSkip(t *testing.T) {
	results := []QueryResult{
		{ID: "q1", Group: "single-hop", Retrieved: []string{"a", "b"}, Gold: []string{"a"}},
		{ID: "q2", Group: "single-hop", Retrieved: []string{"x", "a"}, Gold: []string{"a"}},
		{ID: "q3_abs", Group: "temporal", Retrieved: []string{"x"}, Gold: nil}, // abstention: skipped
	}
	rep := Evaluate(results, []int{1, 2})

	assert.Equal(t, 1, rep.Skipped)
	require.Equal(t, 2, rep.Overall.N)
	assert.InDelta(t, 0.5, rep.Overall.Recall[1], 1e-9)
	assert.InDelta(t, 1.0, rep.Overall.Recall[2], 1e-9)
	assert.InDelta(t, 0.75, rep.Overall.MRR, 1e-9)

	require.Contains(t, rep.ByGroup, "single-hop")
	assert.NotContains(t, rep.ByGroup, "temporal", "skipped-only groups must not appear")
	g := rep.ByGroup["single-hop"]
	assert.Equal(t, 2, g.N)
	assert.InDelta(t, 0.75, g.MRR, 1e-9)
}
