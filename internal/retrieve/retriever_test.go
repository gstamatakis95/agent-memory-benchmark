package retrieve

import (
	"context"
	"testing"
	"time"

	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeQueryEmbedder returns a fixed vector and counts calls; it stands in
// for *embed.Client (the search_query: path) in Tier 0 tests.
type fakeQueryEmbedder struct {
	vec   []float32
	calls int
}

func (f *fakeQueryEmbedder) EmbedQuery(ctx context.Context, raw string) ([]float32, error) {
	f.calls++
	return f.vec, nil
}

func parisCorpus(t *testing.T) *Corpus {
	t.Helper()
	rows := []Row{
		{
			ID: "d_paris", SessionID: "s1",
			Lexemes:        pipeline.Tokenize("Alice visited Paris in the spring"),
			EmbeddingBytes: embed.PackVector(vec768(map[int]float32{0: 1})),
		},
		{
			ID: "d_cheese", SessionID: "s2",
			Lexemes:        pipeline.Tokenize("Bob likes cheese and wine"),
			EmbeddingBytes: embed.PackVector(vec768(map[int]float32{1: 1})),
		},
		{
			ID: "d_car", SessionID: "s1",
			Lexemes:        pipeline.Tokenize("Alice bought a new car"),
			EmbeddingBytes: embed.PackVector(vec768(map[int]float32{0: 0.6, 2: 0.8})),
		},
	}
	c, err := NewCorpus(rows)
	require.NoError(t, err)
	return c
}

func TestRetrieverModes(t *testing.T) {
	c := parisCorpus(t)
	query := "Where did Alice visit Paris?"

	// bm25 mode needs no embedder at all.
	r, err := NewRetriever(c, nil, Options{Mode: ModeBM25})
	require.NoError(t, err)
	got, err := r.Search(context.Background(), query, time.Time{})
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "d_paris", got[0].ID)

	// dense mode ranks by cosine (dot on normalized vectors).
	emb := &fakeQueryEmbedder{vec: vec768(map[int]float32{0: 1})}
	r, err = NewRetriever(c, emb, Options{Mode: ModeDense})
	require.NoError(t, err)
	got, err = r.Search(context.Background(), query, time.Time{})
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "d_paris", got[0].ID)
	assert.InDelta(t, 1.0, got[0].Score, 1e-6)

	// hybrid fuses both lists with RRF; the doc found by both wins.
	emb = &fakeQueryEmbedder{vec: vec768(map[int]float32{0: 1})}
	r, err = NewRetriever(c, emb, Options{Mode: ModeHybrid, RRFK: 60})
	require.NoError(t, err)
	got, err = r.Search(context.Background(), query, time.Time{})
	require.NoError(t, err)
	require.NotEmpty(t, got)
	assert.Equal(t, "d_paris", got[0].ID)
	assert.Equal(t, 1, emb.calls, "hybrid embeds the query exactly once")
}

func TestRetrieverBM25ModeNeverEmbeds(t *testing.T) {
	c := parisCorpus(t)
	emb := &fakeQueryEmbedder{vec: vec768(map[int]float32{0: 1})}
	r, err := NewRetriever(c, emb, Options{Mode: ModeBM25})
	require.NoError(t, err)
	_, err = r.Search(context.Background(), "Alice?", time.Time{})
	require.NoError(t, err)
	assert.Zero(t, emb.calls, "bm25 ablation must not touch the embedder")
}

func TestNewRetrieverValidation(t *testing.T) {
	c := parisCorpus(t)
	_, err := NewRetriever(c, nil, Options{Mode: ModeDense})
	assert.Error(t, err, "dense mode without an embedder")
	_, err = NewRetriever(c, nil, Options{Mode: Mode("fancy")})
	assert.Error(t, err, "unknown mode")
	_, err = ParseMode("hybrid")
	assert.NoError(t, err)
	_, err = ParseMode("llm")
	assert.Error(t, err)
}

func TestRetrieverPerSessionCap(t *testing.T) {
	c := parisCorpus(t)
	r, err := NewRetriever(c, nil, Options{Mode: ModeBM25, MMR: MMROptions{MaxPerSession: 1}})
	require.NoError(t, err)
	// "Alice" matches d_paris and d_car, both in session s1.
	got, err := r.Search(context.Background(), "Alice", time.Time{})
	require.NoError(t, err)
	assert.Len(t, got, 1, "both matches share a session; cap 1 keeps one")
}

func TestRetrieverTemporalBoost(t *testing.T) {
	base := time.Date(2023, 6, 10, 12, 0, 0, 0, time.UTC)
	lex := pipeline.Tokenize("alice said hello")
	v := embed.PackVector(vec768(map[int]float32{0: 1}))
	rows := []Row{
		// Identical text and vectors: without a temporal signal the
		// deterministic tie-break (ascending id) puts a_far first.
		{ID: "a_far", SessionID: "s1", Lexemes: lex, Timestamp: base.Add(-100 * 24 * time.Hour), EmbeddingBytes: v},
		{ID: "z_near", SessionID: "s2", Lexemes: lex, Timestamp: base.Add(-24 * time.Hour), EmbeddingBytes: v},
	}
	c, err := NewCorpus(rows)
	require.NoError(t, err)
	emb := &fakeQueryEmbedder{vec: vec768(map[int]float32{0: 1})}
	r, err := NewRetriever(c, emb, Options{Mode: ModeHybrid})
	require.NoError(t, err)

	// Dated query: "yesterday" resolves against questionDate via the
	// pipeline's rule-based extraction; the near-date doc must win.
	got, err := r.Search(context.Background(), "What did Alice say yesterday?", base)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "z_near", got[0].ID)

	// Undated query (zero base, no absolute date): ranking unchanged.
	got, err = r.Search(context.Background(), "What did Alice say?", time.Time{})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "a_far", got[0].ID)
}
