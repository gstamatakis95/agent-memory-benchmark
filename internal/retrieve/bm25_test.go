package retrieve

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBM25HigherTFScoresHigher(t *testing.T) {
	// Equal document lengths, so length normalization cancels: the doc
	// with tf=2 for the query term must outscore the doc with tf=1.
	idx := NewBM25(
		[]string{"d1", "d2"},
		[][]string{
			{"appl", "banana"},
			{"appl", "appl"},
		},
	)
	top := idx.TopN([]string{"appl"}, 0)
	require.Len(t, top, 2)
	assert.Equal(t, "d2", top[0].ID)
	assert.Greater(t, top[0].Score, top[1].Score)
}

func TestBM25IDFOrdering(t *testing.T) {
	// "rare" appears in 1 of 3 docs, "common" in all 3. With equal tf and
	// equal doc lengths, the rare term must contribute a higher score.
	idx := NewBM25(
		[]string{"d1", "d2", "d3"},
		[][]string{
			{"rare", "common"},
			{"common", "pad"},
			{"common", "pad"},
		},
	)
	rare := idx.TopN([]string{"rare"}, 0)
	require.Len(t, rare, 1)
	require.Equal(t, "d1", rare[0].ID)

	common := idx.TopN([]string{"common"}, 0)
	require.Len(t, common, 3)
	var d1common float64
	for _, s := range common {
		if s.ID == "d1" {
			d1common = s.Score
		}
	}
	assert.Greater(t, rare[0].Score, d1common, "idf(rare) must beat idf(common)")
}

func TestBM25UnknownTermAndTopN(t *testing.T) {
	idx := NewBM25([]string{"d1"}, [][]string{{"x"}})
	assert.Empty(t, idx.TopN([]string{"nope"}, 5))

	idx = NewBM25([]string{"d1", "d2", "d3"}, [][]string{{"x"}, {"x"}, {"x"}})
	assert.Len(t, idx.TopN([]string{"x"}, 2), 2, "TopN must truncate")
}
