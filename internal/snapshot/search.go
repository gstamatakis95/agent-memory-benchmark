package snapshot

// Query-time side of the artifact: BM25 via Bleve, brute-force cosine over
// the flat vector file, RRF fusion, and the recency/abstention rules of
// docs/01-retrieval.md section 4 — the same classical-IR building blocks as
// internal/retrieve, just laid out for a read-only, pre-built snapshot
// instead of an in-memory Corpus.

import (
	"container/heap"
	"context"
	"math"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	"golang.org/x/sync/errgroup"
)

// LexicalTopN runs a BM25 disjunction of the query lexemes against
// index.bleve. See the Snapshot.LexicalTopN doc comment in snapshot.go for
// the contract (repeated lexemes intentionally repeat the term query — that
// is what gives query-term-frequency weighting, matching the hand-rolled
// BM25 in internal/retrieve/bm25.go).
func (s *snap) LexicalTopN(ctx context.Context, lexemes []string, n int, f *Filter) ([]Hit, error) {
	if len(lexemes) == 0 {
		return nil, nil
	}

	terms := make([]query.Query, 0, len(lexemes))
	for _, lx := range lexemes {
		tq := bleve.NewTermQuery(lx)
		tq.SetField("lexemes")
		terms = append(terms, tq)
	}
	var q query.Query = bleve.NewDisjunctionQuery(terms...)
	if f != nil {
		cq := bleve.NewTermQuery(f.ConversationID)
		cq.SetField("conversation_id")
		q = bleve.NewConjunctionQuery(cq, q)
	}

	size := n
	if size <= 0 {
		size = s.Len()
	}
	if size == 0 {
		return nil, nil
	}

	req := bleve.NewSearchRequestOptions(q, size, 0, false)
	req.SortBy([]string{"-_score", "_id"})
	req.PreSearchData = s.bm25PreSearchData()
	res, err := s.idx.SearchInContext(ctx, req)
	if err != nil {
		return nil, err
	}

	hits := make([]Hit, 0, len(res.Hits))
	for _, h := range res.Hits {
		memID, err := strconv.ParseInt(h.ID, 10, 64)
		if err != nil {
			return nil, errorf("snapshot: bleve returned unparseable doc id %q: %w", h.ID, err)
		}
		ord, ok := s.ordByID[memID]
		if !ok {
			continue
		}
		hits = append(hits, Hit{Ordinal: ord, MemoryID: memID, Score: h.Score})
	}
	return hits, nil
}

// bm25PreSearchData supplies the corpus statistics Bleve's BM25 scorer
// needs for length normalization. Bleve computes avgFieldLength as
// ceil(FieldCardinality[field] / DocCount); left to itself it fills
// FieldCardinality with the field's DISTINCT term count, which on any real
// corpus is a small fraction of the doc count and rounds the average
// length up to 1 — every document then looks "longer than average" in
// proportion to its length and short rows win regardless of term
// evidence. Passing the true total lexeme count restores the standard
// Okapi norm 1 - b + b*len/avgLen (up to the ceil), which is what the
// in-process BM25 in internal/retrieve uses, so both engines rank alike.
// Every field the query touches must be present or Bleve errors; the
// conversation_id filter term gets a per-doc length of 1.
func (s *snap) bm25PreSearchData() map[string]interface{} {
	n := s.Len()
	return map[string]interface{}{
		search.BM25PreSearchDataKey: &search.BM25Stats{
			DocCount: float64(n),
			FieldCardinality: map[string]int{
				"lexemes":         int(s.m.LexemeCount),
				"conversation_id": n,
			},
		},
	}
}

// hitBetter is the tiebreak contract every ranked list in this package
// shares: score descending, then ascending memory id (RRF/recency re-sort
// with it too, so the final order stays deterministic through both).
func hitBetter(a, b Hit) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return a.MemoryID < b.MemoryID
}

func sortHits(hits []Hit) {
	sort.Slice(hits, func(i, j int) bool { return hitBetter(hits[i], hits[j]) })
}

// vecHeap is a bounded min-heap of Hits ordered so the worst candidate
// (by hitBetter) is always the root — i.e. the one a fixed-size top-n scan
// evicts first.
type vecHeap []Hit

func (h vecHeap) Len() int            { return len(h) }
func (h vecHeap) Less(i, j int) bool  { return hitBetter(h[j], h[i]) } // root = worst
func (h vecHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *vecHeap) Push(x interface{}) { *h = append(*h, x.(Hit)) }
func (h *vecHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// VectorTopN brute-force scans the vector rows in parallel row ranges. See
// the Snapshot.VectorTopN doc comment in snapshot.go for the contract.
func (s *snap) VectorTopN(ctx context.Context, q []float32, n int, f *Filter) ([]Hit, error) {
	if len(q) != s.dim {
		return nil, errorf("snapshot: query vector has dim %d, want %d", len(q), s.dim)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rows := s.Len()
	if n <= 0 {
		n = rows
	}
	if rows == 0 || n == 0 {
		return nil, nil
	}

	var filterBM *roaring.Bitmap
	if f != nil {
		filterBM = f.Ordinals
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > rows {
		workers = rows
	}
	if workers < 1 {
		workers = 1
	}
	chunk := (rows + workers - 1) / workers

	partials := make([][]Hit, workers)
	var g errgroup.Group
	for w := 0; w < workers; w++ {
		lo := w * chunk
		hi := lo + chunk
		if hi > rows {
			hi = rows
		}
		if lo >= hi {
			continue
		}
		g.Go(func() error {
			partials[w] = vectorScanRange(s.vectors, s.dim, s.ordinals, lo, hi, q, n, filterBM)
			return nil
		})
	}
	_ = g.Wait() // workers never return an error

	var merged []Hit
	for _, p := range partials {
		merged = append(merged, p...)
	}
	sortHits(merged)
	if len(merged) > n {
		merged = merged[:n]
	}
	return merged, nil
}

// vectorScanRange computes dot products for rows [lo,hi), skipping rows
// outside filter (tested before the dot product, per contract), and keeps
// the top n via a bounded min-heap.
func vectorScanRange(vectors []float32, dim int, ordinals []int64, lo, hi int, q []float32, n int, filter *roaring.Bitmap) []Hit {
	h := make(vecHeap, 0, n)
	for i := lo; i < hi; i++ {
		if filter != nil && !filter.Contains(uint32(i)) {
			continue
		}
		row := vectors[i*dim : (i+1)*dim]
		var dot float32
		for j, v := range row {
			dot += v * q[j]
		}
		hit := Hit{Ordinal: i, MemoryID: ordinals[i], Score: float64(dot)}
		if len(h) < n {
			heap.Push(&h, hit)
		} else if hitBetter(hit, h[0]) {
			heap.Pop(&h)
			heap.Push(&h, hit)
		}
	}
	return h
}

// Search runs the query path of docs/01-retrieval.md section 4: the arms
// selected by opts.Mode run concurrently at depth opts.Depth, RRF fuses
// ranks (single-arm modes keep native scores), then recency boost and
// abstention apply, then truncation to TopK.
func (s *snap) Search(ctx context.Context, lexemes []string, qvec []float32, f *Filter, opts SearchOptions) (Result, error) {
	mode, err := ParseMode(string(opts.Mode))
	if err != nil {
		return Result{}, err
	}
	if opts.TopK <= 0 {
		opts.TopK = DefaultTopK
	}
	if opts.Depth <= 0 {
		opts.Depth = DefaultDepth
	}
	if opts.RRFK <= 0 {
		opts.RRFK = DefaultRRFK
	}
	if opts.HalfLifeDays <= 0 {
		opts.HalfLifeDays = DefaultHalfLifeDays
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}

	runLexical := mode == ModeBM25 || mode == ModeHybrid
	runDense := mode == ModeDense || mode == ModeHybrid
	lexicalOnly := false
	if runDense && qvec == nil {
		if mode == ModeDense {
			return Result{}, errorf("snapshot: dense mode requires a query vector")
		}
		runDense = false
		lexicalOnly = true
	}

	var lexHits, denseHits []Hit
	g, gctx := errgroup.WithContext(ctx)
	if runLexical {
		g.Go(func() error {
			hits, err := s.LexicalTopN(gctx, lexemes, opts.Depth, f)
			if err != nil {
				return err
			}
			lexHits = hits
			return nil
		})
	}
	if runDense {
		g.Go(func() error {
			hits, err := s.VectorTopN(gctx, qvec, opts.Depth, f)
			if err != nil {
				return err
			}
			denseHits = hits
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return Result{}, err
	}

	var hits []Hit
	switch {
	case mode == ModeBM25:
		hits = lexHits
	case mode == ModeDense:
		hits = denseHits
	case lexicalOnly: // hybrid degraded to lexical-only
		hits = lexHits
	default: // hybrid, both arms ran
		scores := RRF(opts.RRFK, lexHits, denseHits)
		hits = make([]Hit, 0, len(scores))
		for ord, sc := range scores {
			hits = append(hits, Hit{Ordinal: ord, MemoryID: s.ordinals[ord], Score: sc})
		}
		sortHits(hits)
	}

	if opts.RecencyBoost > 0 {
		for i := range hits {
			ts := s.docs[hits[i].Ordinal].TSUnix
			if ts == 0 {
				continue
			}
			ageDays := opts.Now.Sub(time.Unix(ts, 0)).Hours() / 24
			if ageDays < 0 {
				ageDays = 0
			}
			hits[i].Score *= 1 + opts.RecencyBoost*math.Exp(-math.Ln2/opts.HalfLifeDays*ageDays)
		}
		sortHits(hits)
	}

	abstained := opts.AbstainThreshold > 0 && (len(hits) == 0 || hits[0].Score < opts.AbstainThreshold)

	if len(hits) > opts.TopK {
		hits = hits[:opts.TopK]
	}

	return Result{Hits: hits, Abstained: abstained, LexicalOnly: lexicalOnly}, nil
}
