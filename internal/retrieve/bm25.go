package retrieve

import "math"

// Hand-rolled BM25 (Okapi) over the pipeline's lexemes — the "compact
// custom BM25" option of docs/01-retrieval.md section 4.3 step 3 / 4.7.
// docs/01 does not pin k1/b, so the standard defaults are used.
const (
	// DefaultK1 is the standard Okapi BM25 term-frequency saturation.
	DefaultK1 = 1.2
	// DefaultB is the standard Okapi BM25 length-normalization strength.
	DefaultB = 0.75
)

type posting struct {
	doc int // row index
	tf  int
}

// BM25 is an in-memory inverted index with Okapi BM25 scoring.
type BM25 struct {
	k1, b    float64
	ids      []string
	docLen   []int
	avgLen   float64
	postings map[string][]posting
}

// NewBM25 indexes docs (parallel to ids; each doc is its lexeme list as
// produced by pipeline.Tokenize) with the default k1/b.
func NewBM25(ids []string, docs [][]string) *BM25 {
	return NewBM25Params(ids, docs, DefaultK1, DefaultB)
}

// NewBM25Params is NewBM25 with explicit k1/b.
func NewBM25Params(ids []string, docs [][]string, k1, b float64) *BM25 {
	x := &BM25{
		k1:       k1,
		b:        b,
		ids:      ids,
		docLen:   make([]int, len(docs)),
		postings: make(map[string][]posting),
	}
	var total int
	for i, doc := range docs {
		x.docLen[i] = len(doc)
		total += len(doc)
		tf := make(map[string]int, len(doc))
		for _, term := range doc {
			tf[term]++
		}
		for term, n := range tf {
			x.postings[term] = append(x.postings[term], posting{doc: i, tf: n})
		}
	}
	if len(docs) > 0 {
		x.avgLen = float64(total) / float64(len(docs))
	}
	return x
}

// NewBM25FromCorpus indexes a Corpus's stored lexemes.
func NewBM25FromCorpus(c *Corpus) *BM25 {
	return NewBM25(c.ids, c.lexemes)
}

// idf is the non-negative Robertson/Sparck-Jones IDF used by Lucene:
// ln(1 + (N - df + 0.5)/(df + 0.5)).
func (x *BM25) idf(df int) float64 {
	n := float64(len(x.ids))
	return math.Log(1 + (n-float64(df)+0.5)/(float64(df)+0.5))
}

// TopN scores the query lexemes (already tokenized/stemmed by
// pipeline.Tokenize) against the index and returns the top n matching
// docs, score-descending. Only docs matching at least one query term are
// returned. n <= 0 returns all matches ranked. Repeated query terms
// contribute once per occurrence (query term frequency weighting).
func (x *BM25) TopN(query []string, n int) []Scored {
	acc := make(map[int]float64)
	for _, term := range query {
		plist, ok := x.postings[term]
		if !ok {
			continue
		}
		idf := x.idf(len(plist))
		for _, p := range plist {
			tf := float64(p.tf)
			norm := 1 - x.b + x.b*float64(x.docLen[p.doc])/x.avgLen
			acc[p.doc] += idf * tf * (x.k1 + 1) / (tf + x.k1*norm)
		}
	}
	out := make([]Scored, 0, len(acc))
	for doc, score := range acc {
		out = append(out, Scored{ID: x.ids[doc], Score: score})
	}
	sortScored(out)
	if n > 0 && n < len(out) {
		out = out[:n]
	}
	return out
}
