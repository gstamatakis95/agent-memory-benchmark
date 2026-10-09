// Package export builds snapshots for local agentic search (PLAN.md section 2.2.20, phase 3; register N126, N145, N147,
// N157, P-10). `building` then `ready` snapshots under {shard}/{tenant}/{ns}/export/v{n}/ (manifest, facts,
// observations, chunks, pages, the always-emitted delta with `deleted_ids`) and 1 MiB part streaming. Pattern: Builder
// with a commit point, the manifest is written last. M0.1 declares the signatures only.
package export

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/blob"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Shard is the pair of resources a snapshot needs; the API builds it from the shard handle, so export imports neither
// router nor authz (N157).
type Shard struct {
	Store store.Store
	Blob  blob.Store
}

// BuildOptions are the options of a snapshot build.
type BuildOptions struct {
	Operation id.OperationID
	System    bool // started by the expunge's Materialize, not by a memory.write caller
}

// Manifest describes a snapshot; ExpiresAt and ExpiredReason come from the export_snapshots row.
type Manifest struct {
	Version       id.SnapshotVersion
	State         string // building | ready | expired
	AsOf          time.Time
	ExpiresAt     *time.Time
	ExpiredReason string
	Files         []string
}

// Part is one 1 MiB part of a streamed file.
type Part struct {
	Path   string
	Offset int64
	Data   []byte
	Last   bool
}

// Overlay is the live hidden_overlay of GetSnapshotManifest.
type Overlay struct {
	Hidden      []id.FactID
	Restored    []id.FactID
	RootVersion int64
	Next        string
}

// SnapshotQuery is the paging of ListSnapshots.
type SnapshotQuery struct {
	PageSize  int32
	PageToken string
}

// Builder builds a snapshot.
type Builder interface {
	// Begin inserts the row as 'building' and records the ins_seq watermark; deferred ten minutes on a namespace moved
	// in that recently (N147).
	Begin(ctx context.Context, tx store.Tx, o BuildOptions) (id.SnapshotVersion, error)
	// WriteFiles uses short READ COMMITTED ranges bounded by the watermark, marker sets read once; NO long snapshot (it
	// would block every index build and pin the vacuum horizon, P-10).
	WriteFiles(ctx context.Context, sh Shard, sc id.Scope, v id.SnapshotVersion) (*Manifest, error)
	// Record refuses to promote an expired row and re-checks tombstones newer than the start.
	Record(ctx context.Context, tx store.Tx, m *Manifest) error
}

// Streamer streams snapshot files.
type Streamer interface {
	// Stream is resumable by (path, offset); it refuses expired versions; the stored bytes are NOT filtered on read.
	Stream(ctx context.Context, sh Shard, sc id.Scope, v id.SnapshotVersion, path string, offset int64,
		part func(ctx context.Context, p Part) error) error
	Latest(ctx context.Context, sh Shard, sc id.Scope) (*Manifest, error)
	// Overlay is GetSnapshotManifest's live hidden_overlay, computed with the READ PREDICATE (segment test over
	// fact_hidden(invalidate) and open tombstones), markers newer than the manifest as_of plus Restores, with
	// root_version (N126, N145).
	Overlay(ctx context.Context, sh Shard, sc id.Scope, page string) (*Overlay, error)
}

// SnapshotLister lists and reads manifests.
type SnapshotLister interface {
	// List is ExportService.ListSnapshots.
	List(ctx context.Context, sh Shard, sc id.Scope, q SnapshotQuery) ([]Manifest, string, error)
	// Manifest is GetSnapshotManifest(version != 0), with expired_reason (A-3, A-11).
	Manifest(ctx context.Context, sh Shard, sc id.Scope, v id.SnapshotVersion) (*Manifest, error)
}
