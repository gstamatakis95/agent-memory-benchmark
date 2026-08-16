package retrieve

import "time"

// Rule-based temporal boosting (docs/01-retrieval.md section 4.3 step 5).
// The query time comes from the pipeline's rule-based date extraction —
// never from a model — and the adjustment ALWAYS has an unfiltered
// fallback: no extracted date, or a hard filter that would empty the
// candidate set, leaves the ranking unchanged (the doc's guard against
// the weak-extractor recall regressions reported in the LongMemEval
// paper).
const (
	// DefaultTemporalBoost is the in-window score multiplier (docs/01
	// section 4.3 suggests x1.3).
	DefaultTemporalBoost = 1.3
	// DefaultTemporalWindow is the half-width of the "near the referenced
	// date" window. docs/01 does not pin a width; 14 days is generous
	// enough that session-granular timestamps around the referenced date
	// stay in range.
	DefaultTemporalWindow = 14 * 24 * time.Hour
)

// TemporalOptions configures TemporalAdjust. Zero values select the
// defaults above.
type TemporalOptions struct {
	Window time.Duration // half-width of the window around the query time
	Boost  float64       // in-window multiplier (ignored when HardFilter)
	// HardFilter drops out-of-window candidates instead of boosting
	// in-window ones; if that would empty the set, the input is returned
	// unchanged (unfiltered fallback).
	HardFilter bool
}

func (o TemporalOptions) window() time.Duration {
	if o.Window > 0 {
		return o.Window
	}
	return DefaultTemporalWindow
}

func (o TemporalOptions) boost() float64 {
	if o.Boost > 0 {
		return o.Boost
	}
	return DefaultTemporalBoost
}

// TemporalAdjust re-ranks candidates around a query-referenced time.
// timeOf reports a candidate's timestamp (Corpus.TimeOf satisfies it);
// candidates with no known timestamp are never boosted and never
// filtered out by the hard filter's fallback logic — they simply count
// as out-of-window. found=false (no date extracted from the query)
// returns cands unchanged. The result is a new slice, score-descending.
func TemporalAdjust(cands []Scored, timeOf func(id string) (time.Time, bool), queryTime time.Time, found bool, opt TemporalOptions) []Scored {
	if !found || len(cands) == 0 {
		return cands
	}
	w := opt.window()
	inWindow := func(id string) bool {
		t, ok := timeOf(id)
		if !ok {
			return false
		}
		d := t.Sub(queryTime)
		if d < 0 {
			d = -d
		}
		return d <= w
	}

	if opt.HardFilter {
		kept := make([]Scored, 0, len(cands))
		for _, c := range cands {
			if inWindow(c.ID) {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			return cands // unfiltered fallback
		}
		sortScored(kept)
		return kept
	}

	boost := opt.boost()
	out := make([]Scored, len(cands))
	copy(out, cands)
	for i := range out {
		if inWindow(out[i].ID) {
			out[i].Score *= boost
		}
	}
	sortScored(out)
	return out
}
