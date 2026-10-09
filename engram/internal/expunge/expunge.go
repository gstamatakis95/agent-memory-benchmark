// Package expunge is the asynchronous half of a delete (PLAN.md section 2.2.29; register N119, N120, N135, N136, N138,
// N145, N152, N166). Pattern: a forward-only workflow (not a saga: nothing is compensated). Materialize, Purge,
// DerivedPurge, Index and Finish are idempotent, resumable from `expunge_progress`, paced by the WAL they write (at
// most 25 MB/s, measured per batch, not a fixed pause), run at most two per shard and are paused while a move is open;
// there is no compensation because the marker already made the delete effective. SLA: materialize 15 min, purge 24 h,
// index 48 h. M0.1 declares the signatures only.
package expunge

import (
	"context"

	workflowv1 "github.com/gstamatakis95/engram/gen/go/engram/private/workflow/v1"
)

// Expunger is the activity set of the Expunge workflow.
type Expunger interface {
	// Materialize takes the exclusive derivation lock, one 35 s attempt; EVERY batch re-reads fact_hidden and the open
	// tombstones under it (N120); the work list is the marker set (fact_hidden with materialized_at IS NULL, open
	// tombstones), stamped in the same batch, never the signal payload (N145).
	Materialize(ctx context.Context, in *workflowv1.MaterializeInput) (*workflowv1.MaterializeResult, error)
	// PurgeBatch waits for every consumer cursor past the tombstone's event_seq; targets MARKERS, CHUNK_TOMBSTONES,
	// REEXTRACTED_FACTS, OLD_EMBEDDING_MODEL; never deletes evidence rows (N135).
	PurgeBatch(ctx context.Context, in *workflowv1.PurgeBatchInput) (*workflowv1.PurgeBatchResult, error)
	// DerivedPurge turns covered observation and page versions into content-free stubs; page markdown and Reflect
	// transcripts are deleted (N136).
	DerivedPurge(ctx context.Context, in *workflowv1.DerivedPurgeInput) (*workflowv1.DerivedPurgeResult, error)
	// PurgeBlobs deletes owner-keyed blobs with their rows, and caches after xcache_grace.
	PurgeBlobs(ctx context.Context, in *workflowv1.ExpungeInput) (int64, error)
	// Finish sets expunge_state 'purged'; the tombstone is dropped 24 h later; the documents row only if no higher
	// tombstone is open and no version row remains (C-12).
	Finish(ctx context.Context, in *workflowv1.ExpungeInput) error
}

// Hygiene requests index rebuilds after a purge.
type Hygiene interface {
	// RequestRebuild marks every touched HNSW of a due partition (max(1 % x rows_at_build, 2 k) purged on any index) as
	// `requested` when at least 0.05 % of its rows were purged since the build (50 elements or more), else the
	// partition vacuum repairs it; the index runner rebuilds with REINDEX INDEX CONCURRENTLY, then vacuums the
	// partition once (N138, N152, N166).
	RequestRebuild(ctx context.Context, in *workflowv1.ExpungeInput) (int, error)
}
