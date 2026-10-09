package store

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
)

// Sweepers enumerates the work of the shard-wide schedulers (admin role, documented exceptions to the namespace-leading
// rule): namespaces with pending facts, stale observations, pending tombstones, deferred operations.
type Sweepers interface {
	NamespacesWithPendingFacts(ctx context.Context, limit int) ([]id.NamespaceID, error)
	NamespacesWithStaleObservations(ctx context.Context, limit int) ([]id.NamespaceID, error)
	NamespacesWithPendingTombstones(ctx context.Context, limit int) ([]id.NamespaceID, error)
	DeferredOperations(ctx context.Context, due time.Time, limit int) ([]OperationRow, error)
}

// RestoreRepo applies a delete intent verbatim through the admin variant of the marker transaction (N122), idempotently
// through the shard deletion_log; a skipped intent is recorded as settled (N159).
type RestoreRepo interface {
	Apply(ctx context.Context, in ReplayIntent) (ReplayOutcome, error)
}

// IndexRepo is vector_indexes for the index runner: requested -> building -> ready | failed, lease, purged_since_build
// (N138).
type IndexRepo interface {
	Request(ctx context.Context, r IndexRequest) error
	Claim(ctx context.Context, lease time.Duration) (*IndexRequest, error)
	Record(ctx context.Context, r IndexRequest, st IndexState) error
}

// CursorRepo is outbox_cursors: guarded UPDATE, gap watchlist (D6, A-F1).
type CursorRepo interface {
	Get(ctx context.Context, consumer string) (Cursor, error)
	// Advance is a guarded UPDATE; it moves the cursor and the persisted gaps (at most 1 000) in one statement.
	Advance(ctx context.Context, consumer string, to int64, gaps []int64) error
}
