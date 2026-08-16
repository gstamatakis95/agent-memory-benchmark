package enrich

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
	"golang.org/x/sync/errgroup"

	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/store"
)

// Store is the slice of the append-only ledger the activities need. All
// writes are INSERTs (docs/04-append-only.md); there is deliberately no
// update/delete surface here.
type Store interface {
	// CountPending returns the number of claimable memories at version
	// (no done event, not dead, past backoff).
	CountPending(ctx context.Context, version int16) (int64, error)
	// PendingIDs returns up to limit claimable memory ids at version,
	// ordered ascending (deterministic for range planning).
	PendingIDs(ctx context.Context, version int16, limit int) ([]int64, error)
	// InsertDone appends a success event (idempotent via the
	// uq_enrich_success ON CONFLICT ... DO NOTHING arbiter).
	InsertDone(ctx context.Context, e store.DoneEvent) error
	// InsertFailed appends a failure event (never deduped).
	InsertFailed(ctx context.Context, e store.FailedEvent) error
}

// PGStore adapts internal/store's insert-only API to the Store interface.
type PGStore struct {
	DB          store.DB
	MaxAttempts int
	BackoffBase time.Duration
}

// NewPGStore wraps a pgx pool/tx with the production retry-policy defaults.
func NewPGStore(db store.DB) *PGStore {
	return &PGStore{DB: db, MaxAttempts: store.DefaultMaxAttempts, BackoffBase: store.DefaultBackoffBase}
}

func (s *PGStore) CountPending(ctx context.Context, version int16) (int64, error) {
	return store.CountPending(ctx, s.DB, version, s.MaxAttempts, s.BackoffBase)
}

func (s *PGStore) PendingIDs(ctx context.Context, version int16, limit int) ([]int64, error) {
	return store.PendingMemoryIDs(ctx, s.DB, version, s.MaxAttempts, s.BackoffBase, limit)
}

func (s *PGStore) InsertDone(ctx context.Context, e store.DoneEvent) error {
	return store.InsertEnrichmentDone(ctx, s.DB, e)
}

func (s *PGStore) InsertFailed(ctx context.Context, e store.FailedEvent) error {
	return store.InsertEnrichmentFailed(ctx, s.DB, e)
}

// MemoryText is the enrichment-ready content of one memory: blob fetched from
// S3 and passed through the pipeline (normalize/tokenize/date-parse). Text is
// RAW (unprefixed) — the embed.Client owns the nomic prefix.
type MemoryText struct {
	MemoryID int64
	Text     string
	Lexemes  []string
	TS       *time.Time
}

// TextSource resolves memory ids to their enrichment-ready texts. The server
// (Phase 6) implements it over S3 + internal/pipeline; tests use fakes.
// Implementations must return one entry per requested id, in the same order.
type TextSource interface {
	FetchTexts(ctx context.Context, ids []int64) ([]MemoryText, error)
}

// DocumentEmbedder is the corpus-side embedding surface; *embed.Client
// satisfies it (prefixing, embedding_cache, dims validation, L2 norm).
type DocumentEmbedder interface {
	EmbedDocument(ctx context.Context, raw string) ([]float32, error)
}

// Activities carries the sweep activities' dependencies. Register one
// instance on the worker (see NewWorker).
type Activities struct {
	Store    Store
	Source   TextSource
	Embedder DocumentEmbedder
	// Version is the fallback enrichment version when an input carries 0.
	Version int16
	// EmbedConcurrency bounds the errgroup inside ProcessBatch
	// (0 => DefaultEmbedConcurrency = 32).
	EmbedConcurrency int
}

func (a *Activities) version(v int16) int16 {
	if v != 0 {
		return v
	}
	if a.Version != 0 {
		return a.Version
	}
	return DefaultVersion
}

// CountBacklog returns the number of claimable memories at version, using the
// exact pending/backoff predicate the claim uses (avoids livelock,
// docs/04-append-only.md section 4).
func (a *Activities) CountBacklog(ctx context.Context, version int16) (int, error) {
	n, err := a.Store.CountPending(ctx, a.version(version))
	if err != nil {
		return 0, fmt.Errorf("enrich: count backlog: %w", err)
	}
	return int(n), nil
}

// BatchInput is one ProcessBatch assignment: a contiguous page of the
// ordered pending-id snapshot. Pages from one PlanRanges call are disjoint,
// so sibling activities in a wave never claim overlapping rows. Only ids
// cross workflow history — never texts or vectors (payload limits,
// docs/03-temporal.md).
type BatchInput struct {
	Version int16
	IDs     []int64
}

// BatchResult is counts only, never vectors (docs/03-temporal.md).
type BatchResult struct{ Done, Failed, Permanent int }

// PlanRanges deterministically claims work for one wave: it reads the
// ordered pending-id snapshot at version and splits it into contiguous
// pages of batchSize ids, at most maxBatches pages.
func (a *Activities) PlanRanges(ctx context.Context, version int16, batchSize, maxBatches int) ([]BatchInput, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if maxBatches <= 0 {
		maxBatches = DefaultFanOut
	}
	v := a.version(version)
	ids, err := a.Store.PendingIDs(ctx, v, batchSize*maxBatches)
	if err != nil {
		return nil, fmt.Errorf("enrich: plan ranges: %w", err)
	}
	var batches []BatchInput
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		batches = append(batches, BatchInput{Version: v, IDs: ids[start:end]})
	}
	return batches, nil
}

// progress tracks the contiguous terminally-processed prefix of a batch.
// Only terminal outcomes (done / permanent dead-letter) advance the
// watermark: a retried activity must re-attempt transiently failed items but
// may skip terminal ones. Successes past a stuck watermark are re-run
// harmlessly (idempotent ON CONFLICT done-insert + embedding_cache).
type progress struct {
	mu        sync.Mutex
	terminal  []bool
	watermark int
}

func (p *progress) mark(i int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.terminal[i] = true
	for p.watermark < len(p.terminal) && p.terminal[p.watermark] {
		p.watermark++
	}
	return p.watermark
}

// current returns the watermark without advancing it — used to keep the
// activity's heartbeat alive on transient-failure outcomes (which are NOT
// terminal and must not move the resume point).
func (p *progress) current() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.watermark
}

// ProcessBatch enriches one page of memory ids: fetch texts, embed with a
// bounded errgroup over the unary embedder, append done/failed events.
// Heartbeats carry the terminally-processed prefix length so a retry after a
// crash resumes past completed work (docs/03-temporal.md section 3).
//
// Failure semantics:
//   - permanent embed error (wrong dims, double prefix): failed event with
//     permanent=true — dead-lettered, never retried;
//   - transient embed error: failed event with permanent=false — the row
//     re-enters pending after its backoff window; the batch itself still
//     succeeds unless NOTHING succeeded;
//   - store/source errors: the activity fails and Temporal retries it.
func (a *Activities) ProcessBatch(ctx context.Context, in BatchInput) (BatchResult, error) {
	version := a.version(in.Version)

	// Resume point if this is a retry after a heartbeat timeout.
	startIdx := 0
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &startIdx); err != nil {
			startIdx = 0
		}
		if startIdx < 0 || startIdx > len(in.IDs) {
			startIdx = 0
		}
	}
	if startIdx >= len(in.IDs) {
		return BatchResult{}, nil
	}

	items, err := a.Source.FetchTexts(ctx, in.IDs[startIdx:])
	if err != nil {
		return BatchResult{}, fmt.Errorf("enrich: fetch texts: %w", err)
	}

	attempt := int(activity.GetInfo(ctx).Attempt)
	prog := &progress{terminal: make([]bool, len(in.IDs)), watermark: startIdx}
	for i := 0; i < startIdx; i++ {
		prog.terminal[i] = true
	}

	concurrency := a.EmbedConcurrency
	if concurrency <= 0 {
		concurrency = DefaultEmbedConcurrency
	}
	var done, failed, permanent atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for off, item := range items {
		i, item := startIdx+off, item
		g.Go(func() error {
			vec, err := a.Embedder.EmbedDocument(gctx, item.Text)
			if err != nil {
				perm := embed.IsPermanent(err)
				if ierr := a.Store.InsertFailed(ctx, store.FailedEvent{
					MemoryID:     item.MemoryID,
					Version:      version,
					Attempt:      attempt,
					Permanent:    perm,
					ErrorMessage: err.Error(),
				}); ierr != nil {
					return fmt.Errorf("enrich: append failed event for memory %d: %w", item.MemoryID, ierr)
				}
				if perm {
					permanent.Add(1)
					activity.RecordHeartbeat(ctx, prog.mark(i)) // dead-lettered = terminal
				} else {
					failed.Add(1) // NOT terminal: a retry re-attempts it
					// Still heartbeat (with the UNADVANCED watermark) so a
					// sustained all-transient outage cannot starve the 15s
					// HeartbeatTimeout and burn retry attempts faster than
					// the outage clears. The resume point is unchanged: a
					// retry re-attempts every non-terminal item.
					activity.RecordHeartbeat(ctx, prog.current())
				}
				return nil
			}
			if ierr := a.Store.InsertDone(ctx, store.DoneEvent{
				MemoryID:       item.MemoryID,
				Version:        version,
				Attempt:        attempt,
				NormalizedText: item.Text,
				Lexemes:        item.Lexemes,
				TS:             item.TS,
				Embedding:      embed.PackVector(vec),
			}); ierr != nil {
				return fmt.Errorf("enrich: append done event for memory %d: %w", item.MemoryID, ierr)
			}
			done.Add(1)
			activity.RecordHeartbeat(ctx, prog.mark(i))
			return nil
		})
	}
	res := BatchResult{}
	err = g.Wait()
	res.Done, res.Failed, res.Permanent = int(done.Load()), int(failed.Load()), int(permanent.Load())
	if err != nil {
		return res, err
	}
	// Everything that could succeed failed transiently: fail the activity so
	// Temporal retries it with backoff (the heartbeat watermark stops before
	// the first non-terminal item, so the retry re-attempts exactly those).
	if res.Failed > 0 && res.Done == 0 {
		return res, fmt.Errorf("enrich: all %d retryable items in batch failed transiently", res.Failed)
	}
	return res, nil
}
