// Package outbox is the transactional outbox relay, cursors and sinks (PLAN.md section 2.2.18; register D6, N4, N15,
// N80, N114, N124, A-F1). Pattern: transactional outbox: store.OutboxWriter.Append is the last statement of every write
// transaction, so a consumer sees exactly the committed changes, with no dual write. The writer half lives in store
// (store.OutboxWriter, N140): store does not import outbox, and this package imports store. One relay per shard,
// elected with pg_try_advisory_lock on a dedicated direct connection, reads `outbox` in seq order in batches of 500,
// drives named sinks with independent cursors and delivers a strict prefix. Moves are not consumers (N124). M0.1
// declares the signatures only.
package outbox

import (
	"context"
	"time"

	eventsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/events/v1"
	"github.com/gstamatakis95/engram/internal/store"
)

// Event is the outbox envelope (engram.internal.events.v1.Event).
type Event = eventsv1.Event

// Sink consumes events.
type Sink interface {
	Name() string                                    // "index" | "kafka"
	Apply(ctx context.Context, events []Event) error // idempotent by (namespace_id, seq)
	Filter() func(Event) bool
}

// RelayOptions configure a relay: 500 rows per read, cursor advances at most once per second per consumer (N114), the
// gap watchlist expiring after 2 x statement_timeout = 60 s.
type RelayOptions struct {
	BatchSize    int
	AdvanceEvery time.Duration
	GapExpiry    time.Duration
	LockKey      int64
}

// Relay reads the outbox and drives the sinks.
type Relay struct{}

// NewRelay takes a store.Store and uses s.Direct for the election lock.
func NewRelay(s store.Store, sinks []Sink, o RelayOptions) *Relay { panic("stub") }

// Run acquires the lock and returns when it is lost.
func (r *Relay) Run(ctx context.Context) error { panic("stub") }

// AddSink registers a sink whose cursor starts at fromSeq.
func (r *Relay) AddSink(ctx context.Context, s Sink, fromSeq int64) error { panic("stub") }

// GapWatch is the gap watchlist, persisted in outbox_cursors.gaps (at most 1 000 entries).
type GapWatch interface {
	Note(seq int64, seenAt time.Time)
	Due(now time.Time) []int64
	Resolve(seq int64)
}

// Group reassembles a paged event group (N80): complete only when every page was seen; an ids_elided event is complete
// alone.
type Group interface {
	Add(e Event) (complete bool)
	Ids() [][]byte
	Elided() bool
}
