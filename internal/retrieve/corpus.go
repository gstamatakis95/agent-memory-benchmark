// Package retrieve implements the client-side retrieval stack of
// docs/01-retrieval.md section 4.3 / 4.7: brute-force cosine over a
// contiguous []float32 arena, a hand-rolled in-memory BM25 over the
// pipeline's lexemes, Reciprocal Rank Fusion, rule-based temporal
// boosting with an unfiltered fallback, and MMR diversification with
// per-session caps. Classical IR only — no LLMs, no rerankers, no
// pgvector; ranking happens in-process over vectors unpacked from the
// BYTEA layout shared with internal/embed.
package retrieve

import (
	"fmt"
	"math"
	"sort"
	"time"

	"example.com/agentmem/internal/embed"
)

// Row is one enriched memory as fetched from the store: identity, the
// BM25 lexemes produced by pipeline.Tokenize at enrichment time, the
// session timestamp, and the packed little-endian float32 embedding
// (already L2-normalized by the enrichment path).
type Row struct {
	ID             string
	ConversationID string
	SessionID      string
	TurnID         string
	Text           string
	Lexemes        []string
	Timestamp      time.Time // zero means unknown
	EmbeddingBytes []byte    // packed float32 LE (embed.PackVector layout); nil allowed for lexical-only rows
}

// Scored is a ranked (id, score) pair.
type Scored struct {
	ID    string
	Score float64
}

// Corpus holds all rows of one conversation's enriched memories in RAM:
// a single contiguous []float32 arena (row-major, stride embed.Dims) for
// cache-friendly dense scans, plus parallel metadata slices
// (docs/01-retrieval.md section 4.7).
type Corpus struct {
	dims     int
	arena    []float32 // len == Len()*dims, one backing slice
	ids      []string
	sessions []string
	times    []time.Time
	lexemes  [][]string
	byID     map[string]int
}

// NewCorpus unpacks rows into a Corpus. Every non-nil embedding must be
// exactly embed.Dims floats; rows with a nil embedding get a zero vector
// (they participate in BM25 but score 0 on the dense path). IDs must be
// non-empty and unique.
func NewCorpus(rows []Row) (*Corpus, error) {
	c := &Corpus{
		dims:     embed.Dims,
		arena:    make([]float32, len(rows)*embed.Dims),
		ids:      make([]string, len(rows)),
		sessions: make([]string, len(rows)),
		times:    make([]time.Time, len(rows)),
		lexemes:  make([][]string, len(rows)),
		byID:     make(map[string]int, len(rows)),
	}
	for i, r := range rows {
		if r.ID == "" {
			return nil, fmt.Errorf("retrieve: row %d has empty id", i)
		}
		if _, dup := c.byID[r.ID]; dup {
			return nil, fmt.Errorf("retrieve: duplicate row id %q", r.ID)
		}
		if r.EmbeddingBytes != nil {
			v, err := embed.UnpackVector(r.EmbeddingBytes)
			if err != nil {
				return nil, fmt.Errorf("retrieve: row %q: %w", r.ID, err)
			}
			if len(v) != c.dims {
				return nil, fmt.Errorf("retrieve: row %q has %d dims, want %d", r.ID, len(v), c.dims)
			}
			copy(c.arena[i*c.dims:(i+1)*c.dims], v)
		}
		c.ids[i] = r.ID
		c.sessions[i] = r.SessionID
		c.times[i] = r.Timestamp
		c.lexemes[i] = r.Lexemes
		c.byID[r.ID] = i
	}
	return c, nil
}

// Len is the number of rows.
func (c *Corpus) Len() int { return len(c.ids) }

// Vector returns row i's embedding as a view into the arena.
func (c *Corpus) Vector(i int) []float32 {
	return c.arena[i*c.dims : (i+1)*c.dims]
}

// IndexOf maps an id to its row index.
func (c *Corpus) IndexOf(id string) (int, bool) {
	i, ok := c.byID[id]
	return i, ok
}

// ID returns row i's id.
func (c *Corpus) ID(i int) string { return c.ids[i] }

// SessionOf returns row i's session id.
func (c *Corpus) SessionOf(i int) string { return c.sessions[i] }

// TimeOf returns the timestamp of the row with the given id; ok is false
// for unknown ids and for rows without a timestamp. The signature matches
// the lookup TemporalAdjust takes.
func (c *Corpus) TimeOf(id string) (time.Time, bool) {
	i, ok := c.byID[id]
	if !ok || c.times[i].IsZero() {
		return time.Time{}, false
	}
	return c.times[i], true
}

// Dot is the inner product. On L2-normalized vectors it IS the cosine
// similarity (docs/01-retrieval.md section 4.1), which is why the dense
// scan below never divides by norms.
func Dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

// Cosine is the full cosine similarity (dot over the product of norms).
// It exists so tests can pin Cosine == Dot for normalized inputs; the
// hot path uses Dot. Zero-norm inputs score 0.
func Cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// DenseTopN brute-force scans the arena with dot products against the
// (L2-normalized) query vector and returns the top n rows, score-descending.
// n <= 0 or n > Len() returns all rows ranked.
func (c *Corpus) DenseTopN(query []float32, n int) []Scored {
	if len(query) != c.dims {
		return nil
	}
	out := make([]Scored, c.Len())
	for i := range c.ids {
		out[i] = Scored{ID: c.ids[i], Score: Dot(c.Vector(i), query)}
	}
	sortScored(out)
	if n > 0 && n < len(out) {
		out = out[:n]
	}
	return out
}

// sortScored orders score-descending, breaking ties by ascending id so
// rankings are deterministic.
func sortScored(s []Scored) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Score != s[j].Score {
			return s[i].Score > s[j].Score
		}
		return s[i].ID < s[j].ID
	})
}
