package snapshot

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"example.com/agentmem/internal/retrieve"
)

// Bleve's BM25 scorer (search/scorer/scorer_term.go) is
//
//	idf * (sqrt(tf) * k1) / (sqrt(tf) + k1 * (1 - b + b*len/avgLen))
//
// with idf = ln(1 + (N-n+0.5)/(n+0.5)) — the same IDF and k1/b as the
// hand-rolled Okapi in internal/retrieve, but a sqrt-saturated tf and k1
// (not k1+1) in the numerator. What we control is avgLen: without the
// manifest's LexemeCount fed back as pre-search data Bleve derives it from
// the field's DISTINCT term count and it collapses to 1 (see
// bm25PreSearchData). This test pins avgLen == true average (20/4 = 5) by
// recomputing every score from the formula above; with avgLen 1 all three
// numbers change.
func TestBleveBM25UsesTrueAverageLength(t *testing.T) {
	docs := [][]string{
		{"appl"}, // len 1
		{"appl", "appl", "appl", "appl", "appl", "appl", "appl", "x"}, // len 8, tf 7
		{"pear", "appl", "kiwi"},                                   // len 3
		{"pear", "pear", "fig", "fig", "fig", "fig", "fig", "fig"}, // len 8
	} // 20 lexemes / 4 docs = avgLen 5 exactly (Bleve ceils the average)
	dir := t.TempDir()
	b, err := NewBuilder(dir, Manifest{VectorDim: 2})
	require.NoError(t, err)
	for i, d := range docs {
		require.NoError(t, b.Add(Doc{MemoryID: int64(i + 1), ConversationID: "c"}, d, []float32{1, 0}))
	}
	m, err := b.Close()
	require.NoError(t, err)
	require.EqualValues(t, 20, m.LexemeCount)

	s, err := Open(dir)
	require.NoError(t, err)
	defer s.Close()

	const k1, b_ = retrieve.DefaultK1, retrieve.DefaultB
	idf := math.Log(1 + (4-3+0.5)/(3+0.5)) // "appl" in 3 of 4 docs
	bleveScore := func(tf, length float64) float64 {
		stf := math.Sqrt(tf)
		return idf * stf * k1 / (stf + k1*(1-b_+b_*length/5))
	}
	want := map[int64]float64{1: bleveScore(1, 1), 2: bleveScore(7, 8), 3: bleveScore(1, 3)}

	got, err := s.LexicalTopN(context.Background(), []string{"appl"}, 0, nil)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, h := range got {
		require.InDelta(t, want[h.MemoryID], h.Score, 1e-6, "memory %d", h.MemoryID)
	}
	// And the IDF really is the shared Okapi one: a term in 1 of 4 docs
	// outscores a term in 3 of 4 at equal tf and length.
	rare, err := s.LexicalTopN(context.Background(), []string{"kiwi"}, 0, nil)
	require.NoError(t, err)
	require.Len(t, rare, 1)
	require.Greater(t, rare[0].Score, want[3])
}
