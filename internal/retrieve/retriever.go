package retrieve

import (
	"context"
	"fmt"
	"time"

	"example.com/agentmem/internal/pipeline"
)

// Mode is the retrieval ablation flag (docs/06-testing.md Tier 4: the
// bm25/dense/hybrid comparison is the system's main correctness gate).
type Mode string

const (
	ModeBM25   Mode = "bm25"
	ModeDense  Mode = "dense"
	ModeHybrid Mode = "hybrid"
)

// ParseMode validates a --retrieval flag value.
func ParseMode(s string) (Mode, error) {
	switch Mode(s) {
	case ModeBM25, ModeDense, ModeHybrid:
		return Mode(s), nil
	}
	return "", fmt.Errorf("retrieve: unknown mode %q (want bm25|dense|hybrid)", s)
}

// QueryEmbedder embeds queries through the search_query path.
// *embed.Client satisfies it; there is deliberately no way to hand this
// package a raw Embedder, so a query can never be embedded without its
// nomic task prefix.
type QueryEmbedder interface {
	EmbedQuery(ctx context.Context, raw string) ([]float32, error)
}

// Defaults for Options (docs/01-retrieval.md section 4.3).
const (
	// DefaultTopK is the fixed result depth (step 7).
	DefaultTopK = 10
	// DefaultCandidateN is the per-list depth fed into fusion
	// (step 2/3: N ~ 100-200).
	DefaultCandidateN = 100
)

// Options configures a Retriever. Zero values select the defaults.
type Options struct {
	Mode       Mode // bm25 | dense | hybrid; default hybrid
	TopK       int  // final result depth; default DefaultTopK
	CandidateN int  // per-list depth before fusion; default DefaultCandidateN
	RRFK       int  // RRF constant; default DefaultRRFK (60)
	Temporal   TemporalOptions
	MMR        MMROptions
}

// Retriever runs the per-query pipeline of docs/01 section 4.3 over an
// in-RAM Corpus: BM25 and/or dense candidate generation, RRF fusion
// (hybrid), rule-based temporal boost, then MMR with per-session caps.
type Retriever struct {
	corpus *Corpus
	bm25   *BM25
	emb    QueryEmbedder
	opts   Options
}

// NewRetriever builds a Retriever. emb may be nil only for ModeBM25.
func NewRetriever(c *Corpus, emb QueryEmbedder, opts Options) (*Retriever, error) {
	if opts.Mode == "" {
		opts.Mode = ModeHybrid
	}
	if _, err := ParseMode(string(opts.Mode)); err != nil {
		return nil, err
	}
	if opts.Mode != ModeBM25 && emb == nil {
		return nil, fmt.Errorf("retrieve: mode %q needs a query embedder", opts.Mode)
	}
	if opts.TopK <= 0 {
		opts.TopK = DefaultTopK
	}
	if opts.CandidateN <= 0 {
		opts.CandidateN = DefaultCandidateN
	}
	if opts.RRFK <= 0 {
		opts.RRFK = DefaultRRFK
	}
	r := &Retriever{corpus: c, emb: emb, opts: opts}
	if opts.Mode != ModeDense {
		r.bm25 = NewBM25FromCorpus(c)
	}
	return r, nil
}

// Search runs one query and returns the top-K memory/turn ids with
// scores (fused, post-temporal-boost), in final MMR order. questionDate
// is the base for relative date expressions (LongMemEval question_date);
// pass the zero time when unknown, in which case only absolute dates in
// the question anchor the temporal boost.
func (r *Retriever) Search(ctx context.Context, question string, questionDate time.Time) ([]Scored, error) {
	var bm25List, denseList []Scored
	if r.opts.Mode != ModeDense {
		bm25List = r.bm25.TopN(pipeline.Tokenize(question), r.opts.CandidateN)
	}
	if r.opts.Mode != ModeBM25 {
		qvec, err := r.emb.EmbedQuery(ctx, question)
		if err != nil {
			return nil, fmt.Errorf("retrieve: embed query: %w", err)
		}
		if len(qvec) != r.corpus.dims {
			return nil, fmt.Errorf("retrieve: query vector has %d dims, want %d", len(qvec), r.corpus.dims)
		}
		denseList = r.corpus.DenseTopN(qvec, r.opts.CandidateN)
	}

	// Fuse. Single-list modes keep their native scores so the ablation
	// compares each ranker as-is; hybrid fuses ranks via RRF.
	var fused []Scored
	switch r.opts.Mode {
	case ModeBM25:
		fused = bm25List
	case ModeDense:
		fused = denseList
	default: // hybrid
		m := RRF(r.opts.RRFK, idsOf(bm25List), idsOf(denseList))
		fused = make([]Scored, 0, len(m))
		for id, s := range m {
			fused = append(fused, Scored{ID: id, Score: s})
		}
		sortScored(fused)
	}
	if len(fused) == 0 {
		return nil, nil
	}

	// Temporal boost from rule-based query date extraction, with the
	// mandatory unfiltered fallback inside TemporalAdjust.
	qt, found := extractQueryTime(question, questionDate)
	fused = TemporalAdjust(fused, r.corpus.TimeOf, qt, found, r.opts.Temporal)

	// MMR over the fused candidates. Relevance is max-normalized so it is
	// on the same [0,1]-ish scale as the dot-product similarities
	// (hybrid's raw RRF scores are ~1/k and would otherwise let the
	// diversity term dominate).
	ids := make([]string, 0, len(fused))
	rel := make([]float64, 0, len(fused))
	sessions := make([]string, 0, len(fused))
	rows := make([]int, 0, len(fused))
	maxScore := fused[0].Score
	for _, c := range fused {
		if c.Score > maxScore {
			maxScore = c.Score
		}
	}
	scoreByID := make(map[string]float64, len(fused))
	for _, c := range fused {
		i, ok := r.corpus.IndexOf(c.ID)
		if !ok {
			continue // cannot happen: candidates come from the corpus
		}
		nrel := c.Score
		if maxScore > 0 {
			nrel = c.Score / maxScore
		}
		ids = append(ids, c.ID)
		rel = append(rel, nrel)
		sessions = append(sessions, r.corpus.SessionOf(i))
		rows = append(rows, i)
		scoreByID[c.ID] = c.Score
	}
	sim := func(i, j int) float64 {
		return Dot(r.corpus.Vector(rows[i]), r.corpus.Vector(rows[j]))
	}
	out := MMR(ids, rel, sim, sessions, r.opts.TopK, r.opts.MMR)

	// Report the fused (post-temporal) scores, not the normalized rel.
	for i := range out {
		out[i].Score = scoreByID[out[i].ID]
	}
	return out, nil
}

// extractQueryTime wraps the pipeline's rule-based extraction. With no
// base date, relative expressions ("last month") have nothing to resolve
// against, so only absolute timestamps are tried.
func extractQueryTime(question string, base time.Time) (time.Time, bool) {
	if base.IsZero() {
		t, err := pipeline.ParseTimestamp(question)
		return t, err == nil
	}
	return pipeline.ExtractQueryTime(question, base)
}

func idsOf(s []Scored) []string {
	ids := make([]string, len(s))
	for i, c := range s {
		ids[i] = c.ID
	}
	return ids
}
