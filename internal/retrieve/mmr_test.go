package retrieve

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Golden test from docs/06-testing.md Tier 0: with lambda=0.7, doc B
// nearly identical to doc A and doc C dissimilar but slightly less
// relevant, the selection order must be A, C, B.
func TestMMROrdering(t *testing.T) {
	ids := []string{"A", "B", "C"}
	rel := []float64{0.90, 0.89, 0.85}
	simMat := map[string]float64{
		"AB": 0.95, "BA": 0.95, // B ~ A
		"AC": 0.10, "CA": 0.10,
		"BC": 0.10, "CB": 0.10,
	}
	sim := func(i, j int) float64 { return simMat[ids[i]+ids[j]] }

	got := MMR(ids, rel, sim, nil, 3, MMROptions{Lambda: 0.7})
	require.Len(t, got, 3)
	order := []string{got[0].ID, got[1].ID, got[2].ID}
	assert.Equal(t, []string{"A", "C", "B"}, order)
}

// Golden test from docs/06-testing.md Tier 0: 10 candidates from one
// session with maxPerSession=2 must yield exactly 2 results.
func TestMMRPerSessionCap(t *testing.T) {
	n := 10
	ids := make([]string, n)
	rel := make([]float64, n)
	sessions := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("d%02d", i)
		rel[i] = 1 - float64(i)/100
		sessions[i] = "session_1"
	}
	sim := func(i, j int) float64 { return 0 }

	got := MMR(ids, rel, sim, sessions, 10, MMROptions{Lambda: 0.7, MaxPerSession: 2})
	require.Len(t, got, 2)
	assert.Equal(t, "d00", got[0].ID)
	assert.Equal(t, "d01", got[1].ID)
}

func TestMMRCapAcrossSessions(t *testing.T) {
	ids := []string{"a1", "a2", "a3", "b1", "b2"}
	rel := []float64{0.9, 0.8, 0.7, 0.6, 0.5}
	sessions := []string{"a", "a", "a", "b", "b"}
	sim := func(i, j int) float64 { return 0 }

	got := MMR(ids, rel, sim, sessions, 5, MMROptions{MaxPerSession: 2})
	require.Len(t, got, 4)
	assert.Equal(t, []Scored{
		{ID: "a1", Score: 0.9}, {ID: "a2", Score: 0.8},
		{ID: "b1", Score: 0.6}, {ID: "b2", Score: 0.5},
	}, got)
}
