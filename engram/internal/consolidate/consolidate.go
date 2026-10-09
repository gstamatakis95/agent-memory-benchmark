// Package consolidate turns facts into observations in two stages (PLAN.md section 2.2.14; register N43, N57, N95,
// N120, N121, N135, N143, N144, C-13, C-19). Pattern: Pipeline of five single-purpose strategies (route, write, dedup,
// store, apply) so each LLM call sees exactly what a delete may later need to hide (N121). Stage 1 routes a batch of at
// most 8 facts against candidate observations and returns decisions only; stage 2 writes one version per touched
// observation from that observation's own text and visible sources; a `merge` is a root rebuild. M0.1 declares the
// signatures only.
package consolidate

import (
	"context"

	"github.com/gstamatakis95/engram/internal/gateway"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// BatchKey identifies a routing batch; proposals are keyed (batch_key, attempt).
type BatchKey [32]byte

// Batch is at most 8 facts routed in one stage-1 call.
type Batch struct {
	Key   BatchKey
	Facts []store.Fact
}

// Candidate is an existing observation considered for a fact.
type Candidate struct {
	Observation id.ObservationID
	Text        string
	Score       float64
}

// Placement is a stage-1 decision for one fact: attach | create | skip.
type Placement struct {
	Fact        id.FactID
	Action      string
	Observation id.ObservationID
}

// Routing is the stage-1 result: placements, merges and drop_sources; nothing textual is persisted.
type Routing struct {
	Placements  []Placement
	Merges      [][]id.ObservationID
	DropSources map[id.ObservationID][]id.FactID
}

// WriteInput is the stage-2 input: the target observation's own text and its VISIBLE sources only.
type WriteInput struct {
	Observation id.ObservationID
	Mode        string // update | create | rebuild
	Base        id.ObsVersion
	PrevText    string
	Facts       []store.Fact
}

// ObservationWrite is the stage-2 result: the new text of one observation version.
type ObservationWrite struct {
	Observation id.ObservationID
	Text        string
	Retire      bool
}

// Proposal is the stored routing for (batch_key, attempt); every update and merge op records base_version.
type Proposal struct {
	Key     BatchKey
	Attempt int
	Routing Routing
	Writes  []ObservationWrite
	Base    map[id.ObservationID]id.ObsVersion
}

// RetryWhy says why a proposal was discarded: a stale base, a hidden input or a capacity retry (prompt_variant =
// 'capacity').
type RetryWhy string

// The retry reasons.
const (
	RetryStale    RetryWhy = "stale"
	RetryHidden   RetryWhy = "hidden"
	RetryCapacity RetryWhy = "capacity"
)

// Applied reports what Apply did; op_key = sha256(batch_key || attempt || op_index) over the STORED list.
type Applied struct {
	Ops      int
	Skipped  int
	Versions []id.ObsVersion
}

// Router is stage 1: consolidate_route/v1.
type Router interface {
	// Route returns placements{attach|create|skip}, merges, drop_sources; nothing textual is persisted.
	Route(ctx context.Context, b Batch, cands []Candidate) (Routing, *gateway.Usage, error)
}

// Writer is stage 2: consolidate_write/v1, one call per touched observation.
type Writer interface {
	// Write: mode update | create | rebuild; only VISIBLE sources are rendered.
	Write(ctx context.Context, in WriteInput) (ObservationWrite, *gateway.Usage, error)
}

// Adjudicator is dedup_adjudicate/v1: a decision, never text.
type Adjudicator interface {
	Adjudicate(ctx context.Context, a, b string) (merge bool, u *gateway.Usage, err error)
}

// ProposalStore is insert-only: nothing is updated or deleted (engram_app holds SELECT, INSERT), N121.
type ProposalStore interface {
	// Store is keyed (batch_key, attempt); ON CONFLICT DO NOTHING, the stored list wins (N43).
	Store(ctx context.Context, tx store.Tx, p Proposal) (stored Proposal, err error)
	Load(ctx context.Context, tx store.Tx, key BatchKey, attempt int) (Proposal, error)
	// Retry opens attempt+1 after a discard or a capacity retry; the old list is dead by key.
	Retry(ctx context.Context, tx store.Tx, key BatchKey, why RetryWhy) (next int, err error)
}

// Applier runs in one fenced derivation transaction and obeys the commit rule of N120: shared derivation lock,
// Observations().Verify in a fresh statement, then the compare-and-set at commit; any failure rolls back and the WHOLE
// proposal is discarded to the next attempt, never repaired.
type Applier interface {
	Apply(ctx context.Context, tx store.Tx, p Proposal) (Applied, error)
}
