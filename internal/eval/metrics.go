// Package eval computes retrieval metrics (Recall@k, NDCG@k, MRR) in Go
// directly from the benchmarks' evidence annotations (docs/01-retrieval.md
// section 4.8), broken out by LoCoMo category and LongMemEval
// question_type, and hosts the JSON loaders for both benchmark formats
// plus the tiny local fixtures format. These are retrieval metrics, not
// official QA scores.
package eval

import "math"

// RecallAtK is |gold ∩ retrieved[:k]| / |gold| with both sides deduped.
// Empty gold returns 0; callers should skip unanswerable items (see
// Evaluate).
func RecallAtK(retrieved, gold []string, k int) float64 {
	goldSet := toSet(gold)
	if len(goldSet) == 0 {
		return 0
	}
	hits := 0
	seen := make(map[string]bool, k)
	for i, id := range retrieved {
		if k > 0 && i >= k {
			break
		}
		if goldSet[id] && !seen[id] {
			seen[id] = true
			hits++
		}
	}
	return float64(hits) / float64(len(goldSet))
}

// NDCGAtK is binary-relevance NDCG: DCG = Σ 1/log2(pos+1) over gold hits
// at 1-indexed positions ≤ k, normalized by the ideal DCG of
// min(k, |gold|) hits at the top. Duplicate retrieved ids gain only once.
func NDCGAtK(retrieved, gold []string, k int) float64 {
	goldSet := toSet(gold)
	if len(goldSet) == 0 {
		return 0
	}
	var dcg float64
	seen := make(map[string]bool, k)
	for i, id := range retrieved {
		if k > 0 && i >= k {
			break
		}
		if goldSet[id] && !seen[id] {
			seen[id] = true
			dcg += 1 / math.Log2(float64(i)+2)
		}
	}
	ideal := len(goldSet)
	if k > 0 && k < ideal {
		ideal = k
	}
	var idcg float64
	for i := 0; i < ideal; i++ {
		idcg += 1 / math.Log2(float64(i)+2)
	}
	return dcg / idcg
}

// MRR is 1/rank of the first gold id in retrieved (1-indexed), 0 when no
// gold id is retrieved.
func MRR(retrieved, gold []string) float64 {
	goldSet := toSet(gold)
	for i, id := range retrieved {
		if goldSet[id] {
			return 1 / float64(i+1)
		}
	}
	return 0
}

func toSet(ids []string) map[string]bool {
	s := make(map[string]bool, len(ids))
	for _, id := range ids {
		s[id] = true
	}
	return s
}

// QueryResult is one scored query: the ranked retrieved ids, the gold
// evidence ids, and the breakdown group (LoCoMo category or LongMemEval
// question_type).
type QueryResult struct {
	ID        string
	Group     string
	Retrieved []string
	Gold      []string
}

// Summary is the mean metrics over a set of queries.
type Summary struct {
	N      int
	Recall map[int]float64 // k -> mean Recall@k
	NDCG   map[int]float64 // k -> mean NDCG@k
	MRR    float64
}

// Report is the evaluation output: overall means plus per-group
// breakdowns. Skipped counts queries with no gold evidence (e.g.
// LongMemEval _abs abstention questions, LoCoMo adversarial), which are
// excluded from every mean per docs/01 section 4.8.
type Report struct {
	Overall Summary
	ByGroup map[string]Summary
	Skipped int
}

type accum struct {
	n      int
	recall map[int]float64
	ndcg   map[int]float64
	mrr    float64
}

func newAccum(ks []int) *accum {
	a := &accum{recall: make(map[int]float64), ndcg: make(map[int]float64)}
	for _, k := range ks {
		a.recall[k] = 0
		a.ndcg[k] = 0
	}
	return a
}

func (a *accum) add(r QueryResult, ks []int) {
	a.n++
	for _, k := range ks {
		a.recall[k] += RecallAtK(r.Retrieved, r.Gold, k)
		a.ndcg[k] += NDCGAtK(r.Retrieved, r.Gold, k)
	}
	a.mrr += MRR(r.Retrieved, r.Gold)
}

func (a *accum) summary(ks []int) Summary {
	s := Summary{N: a.n, Recall: make(map[int]float64, len(ks)), NDCG: make(map[int]float64, len(ks))}
	if a.n == 0 {
		return s
	}
	for _, k := range ks {
		s.Recall[k] = a.recall[k] / float64(a.n)
		s.NDCG[k] = a.ndcg[k] / float64(a.n)
	}
	s.MRR = a.mrr / float64(a.n)
	return s
}

// Evaluate aggregates per-query results into overall and per-group mean
// metrics at each cutoff in ks (default {5, 10}). Queries with empty
// Gold are skipped, not scored as zero.
func Evaluate(results []QueryResult, ks []int) Report {
	if len(ks) == 0 {
		ks = []int{5, 10}
	}
	overall := newAccum(ks)
	groups := make(map[string]*accum)
	skipped := 0
	for _, r := range results {
		if len(r.Gold) == 0 {
			skipped++
			continue
		}
		overall.add(r, ks)
		g, ok := groups[r.Group]
		if !ok {
			g = newAccum(ks)
			groups[r.Group] = g
		}
		g.add(r, ks)
	}
	rep := Report{Overall: overall.summary(ks), ByGroup: make(map[string]Summary, len(groups)), Skipped: skipped}
	for name, a := range groups {
		rep.ByGroup[name] = a.summary(ks)
	}
	return rep
}
