package retrieve

// DefaultRRFK is the TREC-scale Reciprocal Rank Fusion constant
// (docs/01-retrieval.md section 4.3 step 4). For the ~50-session
// LongMemEval haystacks the doc suggests trying k=10-30, so k is a
// first-class parameter everywhere it is used.
const DefaultRRFK = 60

// RRF fuses ranked id lists (rank 1 = first element):
//
//	RRF(d) = sum over lists r of 1/(k + rank_r(d))
//
// (Cormack, Clarke & Buettcher, SIGIR 2009). A document absent from a
// list contributes 0 for that list — never 1/k. Worked example from
// docs/06-testing.md Tier 0: with k=60, a doc at rank 3 in BM25 and
// rank 7 in dense scores 1/63 + 1/67.
func RRF(k int, lists ...[]string) map[string]float64 {
	scores := make(map[string]float64)
	for _, list := range lists {
		for i, id := range list {
			scores[id] += 1 / float64(k+i+1)
		}
	}
	return scores
}
