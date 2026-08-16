package retrieve

import "math"

// DefaultMMRLambda is the relevance/diversity tradeoff of docs/01
// section 4.3 step 6 (Carbonell & Goldstein 1998).
const DefaultMMRLambda = 0.7

// MMROptions configures MMR. Zero values select the defaults.
type MMROptions struct {
	// Lambda weighs relevance against diversity:
	// lambda*rel(d) - (1-lambda)*max_{d' in S} sim(d, d').
	// <= 0 selects DefaultMMRLambda.
	Lambda float64
	// MaxPerSession caps how many results any one session may contribute,
	// so multi-session evidence is not crowded out by one dominant
	// session. 0 means no cap.
	MaxPerSession int
}

func (o MMROptions) lambda() float64 {
	if o.Lambda > 0 {
		return o.Lambda
	}
	return DefaultMMRLambda
}

// MMR greedily selects up to k of the candidates maximizing the Maximal
// Marginal Relevance objective. ids and rel are parallel (rel should be
// on a scale comparable to sim, e.g. cosine similarities or max-
// normalized fused scores); sim(i, j) is the pairwise candidate
// similarity (dot product of normalized doc vectors); sessions is
// parallel to ids and may be nil when no per-session cap applies.
//
// The returned slice is in selection order and carries each candidate's
// INPUT rel as its Score (the MMR objective only orders; it is not a
// calibrated score). Fewer than k results are returned when candidates
// run out or every remaining candidate's session is at its cap.
func MMR(ids []string, rel []float64, sim func(i, j int) float64, sessions []string, k int, opt MMROptions) []Scored {
	n := len(ids)
	if len(rel) != n || (sessions != nil && len(sessions) != n) {
		panic("retrieve: MMR slice lengths differ")
	}
	if k <= 0 || k > n {
		k = n
	}
	lambda := opt.lambda()

	used := make([]bool, n)
	perSession := make(map[string]int)
	selected := make([]int, 0, k)
	for len(selected) < k {
		best, bestScore := -1, math.Inf(-1)
		for i := 0; i < n; i++ {
			if used[i] {
				continue
			}
			if opt.MaxPerSession > 0 && sessions != nil && perSession[sessions[i]] >= opt.MaxPerSession {
				continue
			}
			maxSim := 0.0 // empty selected set contributes nothing
			if len(selected) > 0 {
				maxSim = math.Inf(-1)
				for _, j := range selected {
					if s := sim(i, j); s > maxSim {
						maxSim = s
					}
				}
			}
			score := lambda*rel[i] - (1-lambda)*maxSim
			if score > bestScore {
				best, bestScore = i, score
			}
		}
		if best < 0 {
			break // exhausted or fully capped
		}
		used[best] = true
		if sessions != nil {
			perSession[sessions[best]]++
		}
		selected = append(selected, best)
	}

	out := make([]Scored, len(selected))
	for i, idx := range selected {
		out[i] = Scored{ID: ids[idx], Score: rel[idx]}
	}
	return out
}
