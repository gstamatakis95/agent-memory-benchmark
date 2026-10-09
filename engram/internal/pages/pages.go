// Package pages is mental models / knowledge pages (PLAN.md section 2.2.16, phase 3; register N73, N117, N120, N128,
// N139, N143, N144, N157). Source query + tag filter, versioned markdown in blob, evidence segments per version, one
// RefreshPolicy{trigger, interval, debounce}, delta refresh by `page/v1` and root rebuild by `page_full/v1`. Pattern:
// Repository, split into a read and a write capability. Page.RefreshPolicy is the generated memoryv1.RefreshPolicy;
// there is no on_delete, cron or string policy. M0.1 declares the signatures only.
package pages

import (
	"context"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Page is the page resource without its markdown.
type Page struct {
	ID            id.PageID
	Name          string
	Current       id.PageVersion
	SourceQuery   string
	TagFilter     *memoryv1.TagFilter
	RefreshPolicy *memoryv1.RefreshPolicy
	Stale         bool
	Etag          string
	UpdatedAt     time.Time
}

// VersionSelector picks the version to read: the current one, a number, or the one current at AsOf.
type VersionSelector struct {
	Version id.PageVersion
	AsOf    *time.Time
}

// ListQuery, SearchQuery and PageHit are the list and search values.
type (
	ListQuery struct {
		PageSize  int32
		PageToken string
	}
	SearchQuery struct {
		Text     string
		Vector   []float32
		PageSize int32
	}
	PageHit struct {
		Page    Page
		Score   float64
		Snippet string
	}
)

// PageDraft is the input of Writer.Create.
type PageDraft struct {
	Name          string
	SourceQuery   string
	TagFilter     *memoryv1.TagFilter
	RefreshPolicy *memoryv1.RefreshPolicy
	Markdown      string
}

// PagePatch is the input of Admin.Update; changing source_query or tag_filter sets stale_write.
type PagePatch struct {
	Mask          []string
	Name          *string
	SourceQuery   *string
	TagFilter     *memoryv1.TagFilter
	RefreshPolicy *memoryv1.RefreshPolicy
}

// RefreshReason says why a refresh runs: scheduled, delete-driven or manual.
type RefreshReason string

// The refresh reasons.
const (
	RefreshScheduled RefreshReason = "scheduled"
	RefreshDelete    RefreshReason = "delete"
	RefreshManual    RefreshReason = "manual"
)

// RefreshResult reports a refresh.
type RefreshResult struct {
	Version id.PageVersion
	Rebuilt bool
}

// Reader reads pages through store.ReadTx.Derived() (N157).
type Reader interface {
	// Get returns PAGE_HIDDEN when the version is hidden.
	Get(ctx context.Context, sc id.Scope, c id.Caller, p id.PageID,
		sel VersionSelector) (*Page, string /*markdown*/, error)
	// ByName is GetPage by name (A-3).
	ByName(ctx context.Context, sc id.Scope, c id.Caller, name string,
		sel VersionSelector) (*Page, string /*markdown*/, error)
	List(ctx context.Context, sc id.Scope, c id.Caller, q ListQuery) ([]Page, string, error)
	// Search is BM25 over page_versions.text union HNSW over page_version_vectors, visible versions, RRF, no LLM (N73,
	// N139).
	Search(ctx context.Context, sc id.Scope, c id.Caller, q SearchQuery) ([]PageHit, string, error)
}

// Writer creates, deletes and refreshes pages.
type Writer interface {
	Create(ctx context.Context, sc id.Scope, c id.Caller, p PageDraft) (*Page, error)
	Delete(ctx context.Context, sc id.Scope, c id.Caller, p id.PageID) error
	// Refresh takes a store.Store, never a ShardHandle (N157). It runs as the per-page singleton
	// ns/{ns}/page/{page_id}.
	Refresh(ctx context.Context, s store.Store, sc id.Scope, p id.PageID, why RefreshReason,
		op id.OperationID) (*RefreshResult, error)
}

// Admin is PageService.UpdatePage.
type Admin interface {
	Update(ctx context.Context, sc id.Scope, c id.Caller, p id.PageID, patch PagePatch, etag string) (*Page, error)
}
