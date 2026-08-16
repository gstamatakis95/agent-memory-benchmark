package retrieve

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func temporalFixture() ([]Scored, func(id string) (time.Time, bool), time.Time) {
	qt := time.Date(2023, 5, 8, 12, 0, 0, 0, time.UTC)
	times := map[string]time.Time{
		"near": qt.Add(-24 * time.Hour),
		"far":  qt.Add(-60 * 24 * time.Hour),
		// "undated" has no timestamp at all
	}
	timeOf := func(id string) (time.Time, bool) {
		t, ok := times[id]
		return t, ok
	}
	cands := []Scored{{ID: "far", Score: 1.05}, {ID: "near", Score: 1.0}, {ID: "undated", Score: 0.9}}
	return cands, timeOf, qt
}

func TestTemporalBoostPrefersNearDate(t *testing.T) {
	cands, timeOf, qt := temporalFixture()
	got := TemporalAdjust(cands, timeOf, qt, true, TemporalOptions{})
	require.Len(t, got, 3)
	assert.Equal(t, "near", got[0].ID, "in-window doc must be boosted past the slightly-higher far doc")
	assert.InDelta(t, 1.0*DefaultTemporalBoost, got[0].Score, 1e-9)
	assert.Equal(t, "far", got[1].ID)
	assert.InDelta(t, 1.05, got[1].Score, 1e-9, "out-of-window score unchanged")
	assert.InDelta(t, 0.9, got[2].Score, 1e-9, "undated doc never boosted")
}

func TestTemporalUndatedQueryUnchanged(t *testing.T) {
	cands, timeOf, _ := temporalFixture()
	got := TemporalAdjust(cands, timeOf, time.Time{}, false, TemporalOptions{})
	assert.Equal(t, cands, got, "no extracted date must leave the ranking untouched")
}

func TestTemporalHardFilter(t *testing.T) {
	cands, timeOf, qt := temporalFixture()

	got := TemporalAdjust(cands, timeOf, qt, true, TemporalOptions{HardFilter: true, Window: 48 * time.Hour})
	require.Len(t, got, 1)
	assert.Equal(t, "near", got[0].ID)

	// Empty-after-filter must fall back to the unfiltered candidates.
	got = TemporalAdjust(cands, timeOf, qt, true, TemporalOptions{HardFilter: true, Window: time.Minute})
	assert.Equal(t, cands, got, "hard filter that empties the set must fall back unfiltered")
}
