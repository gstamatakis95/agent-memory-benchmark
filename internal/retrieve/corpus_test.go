package retrieve

import (
	"math"
	"testing"
	"time"

	"example.com/agentmem/internal/embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vec768 builds a 768-dim vector from (index, value) pairs.
func vec768(pairs map[int]float32) []float32 {
	v := make([]float32, embed.Dims)
	for i, x := range pairs {
		v[i] = x
	}
	return v
}

// Golden test from docs/06-testing.md Tier 0: on L2-normalized inputs
// cosine must equal dot, and the norm after normalization must be 1.
func TestCosineEqualsDotOnNormalized(t *testing.T) {
	a := embed.L2Normalize([]float32{3, 4, 0, 1})
	b := embed.L2Normalize([]float32{1, 2, 2, 0.5})

	var norm float64
	for _, x := range a {
		norm += float64(x) * float64(x)
	}
	norm = math.Sqrt(norm)
	require.Less(t, math.Abs(norm-1), 1e-6, "L2Normalize must produce a unit vector")

	require.Less(t, math.Abs(Cosine(a, b)-Dot(a, b)), 1e-6,
		"cosine(a,b) must equal dot(a,b) for normalized inputs")
}

func TestCorpusArenaAndDenseTopN(t *testing.T) {
	v1 := vec768(map[int]float32{0: 1})
	v2 := embed.L2Normalize(vec768(map[int]float32{0: 0.6, 1: 0.8}))
	rows := []Row{
		{ID: "d1", SessionID: "s1", EmbeddingBytes: embed.PackVector(v1), Timestamp: time.Unix(1000, 0)},
		{ID: "d2", SessionID: "s2", EmbeddingBytes: embed.PackVector(v2)},
		{ID: "d3", SessionID: "s2"}, // no embedding: zero vector
	}
	c, err := NewCorpus(rows)
	require.NoError(t, err)
	require.Equal(t, 3, c.Len())

	// The arena is one contiguous backing slice with stride 768.
	require.Len(t, c.arena, 3*embed.Dims)
	assert.InDelta(t, 1.0, float64(c.Vector(0)[0]), 1e-6)
	assert.InDelta(t, 0.8, float64(c.Vector(1)[1]), 1e-6)

	query := vec768(map[int]float32{0: 1})
	top := c.DenseTopN(query, 2)
	require.Len(t, top, 2)
	assert.Equal(t, "d1", top[0].ID)
	assert.InDelta(t, 1.0, top[0].Score, 1e-6)
	assert.Equal(t, "d2", top[1].ID)
	assert.InDelta(t, 0.6, top[1].Score, 1e-6)

	// Metadata lookups.
	if ts, ok := c.TimeOf("d1"); assert.True(t, ok) {
		assert.Equal(t, time.Unix(1000, 0), ts)
	}
	_, ok := c.TimeOf("d2") // zero timestamp -> not known
	assert.False(t, ok)
	i, ok := c.IndexOf("d3")
	require.True(t, ok)
	assert.Equal(t, "s2", c.SessionOf(i))
	assert.InDelta(t, 0.0, Dot(c.Vector(i), query), 1e-9, "nil embedding scores 0")
}

func TestNewCorpusRejectsBadRows(t *testing.T) {
	_, err := NewCorpus([]Row{{ID: ""}})
	assert.Error(t, err, "empty id")

	_, err = NewCorpus([]Row{{ID: "a"}, {ID: "a"}})
	assert.Error(t, err, "duplicate id")

	_, err = NewCorpus([]Row{{ID: "a", EmbeddingBytes: embed.PackVector([]float32{1, 2})}})
	assert.Error(t, err, "wrong dimensionality")

	_, err = NewCorpus([]Row{{ID: "a", EmbeddingBytes: []byte{1, 2, 3}}})
	assert.Error(t, err, "truncated packed vector")
}
