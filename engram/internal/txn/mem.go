package txn

import (
	"context"
	"sync"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
)

// MemTx is an in-memory Tx for tests of quota and of anything else that meters usage: it keeps the exactly-once rule of
// token_usage_events (one row per usage_key) and rolls usage up by UTC day. The facts counter of Day is set with
// SetFacts, because facts are written by store, not by metering.
type MemTx struct {
	Sc id.Scope

	mu     sync.Mutex
	events map[UsageKey]UsageEvent
	facts  map[string]int64
}

// NewMemTx returns an empty MemTx for sc.
func NewMemTx(sc id.Scope) *MemTx {
	return &MemTx{Sc: sc, events: map[UsageKey]UsageEvent{}, facts: map[string]int64{}}
}

// Scope implements Tx.
func (m *MemTx) Scope() id.Scope { return m.Sc }

// Usage implements Tx.
func (m *MemTx) Usage() Usage { return m }

// SetFacts sets the facts-written counter reported by Day for the UTC day of d.
func (m *MemTx) SetFacts(d time.Time, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.facts[dayKey(d)] = n
}

// Events returns the number of distinct usage keys recorded.
func (m *MemTx) Events() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

// Record implements Usage.
func (m *MemTx) Record(ctx context.Context, key UsageKey, e UsageEvent) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.events[key]; dup {
		return false, nil
	}
	m.events[key] = e
	return true, nil
}

// Day implements Usage.
func (m *MemTx) Day(ctx context.Context, day time.Time) (tokens, facts int64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	want := dayKey(day)
	for _, e := range m.events {
		if dayKey(e.Day) == want {
			tokens += e.PromptTokens + e.CompletionTokens
		}
	}
	return tokens, m.facts[want], nil
}

func dayKey(t time.Time) string { return t.UTC().Format(time.DateOnly) }

var _ Tx = (*MemTx)(nil)
