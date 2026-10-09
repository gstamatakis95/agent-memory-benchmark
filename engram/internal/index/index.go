// Package index is the search index abstraction of PLAN.md section 2.2.8 (register N44, N54, N67, N73, N85, N112, N116,
// N138, N139, N151). Pattern: Strategy; recall arms see only Searcher. The index lives in the shard's Postgres
// (Transactional) or in an external engine fed by the outbox (Async). The D7 name index.Index denotes a Searcher plus,
// for an Async engine, the Applier that the `index` outbox sink drives. M0.1 declares the signatures only.
package index

import (
	"context"
	"time"

	eventsv1 "github.com/gstamatakis95/engram/gen/go/engram/private/events/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Filter is the predicate every arm applies inside its scan.
type Filter struct {
	// AsOf: mentioned_at <= AsOf inside the predicate; the chunk arm also embedding_effective_at <= AsOf (N85).
	AsOf      *time.Time
	FactTypes []memoryv1.FactType // copied onto fact_vectors, so the type filter runs inside the scan (N138)
	// AllowedDocs are the tag and metadata filters, resolved once at document level (N116, N139).
	AllowedDocs []id.DocumentID
	// Markers are the doc_tomb (document to up_to_version) and chunk_tomb arrays, applied to EVERY hit, also an
	// external engine's (N44, N116); fact_hidden is an anti-join per candidate, never an array.
	Markers store.MarkerSets
}

// Hit is one arm result.
type Hit struct {
	Ref         id.MemoryRef
	Score       float64 // arm-local; normalised per query by the planner (N67)
	MentionedAt time.Time
}

// SemanticQuery, LexicalQuery, ChunkQuery and PageQuery are the inputs of the four arms.
type (
	SemanticQuery struct {
		Vector []float32
		Model  string
		Filter Filter
		Plan   SemanticPlan
		Cap    int
	}
	LexicalQuery struct {
		Text   string
		Filter Filter
		Cap    int
	}
	ChunkQuery struct {
		Vector []float32
		Model  string
		Filter Filter
		Cap    int
	}
	PageQuery struct {
		Text   string
		Vector []float32
		Cap    int
	}
)

// PlanKind selects the semantic path chosen in Go before the arm runs (N138, N151).
type PlanKind uint8

// The semantic plans: PlanExact below theta eligible rows, PlanHNSW at or above it.
const (
	PlanExact PlanKind = iota + 1
	PlanHNSW
)

// SemanticPlan is cost-based and chosen in Go before the arm runs (N112, N138, N151): PlanExact |
// PlanHNSW{MaxScanTuples, ExactFallback}.
type SemanticPlan struct {
	Kind          PlanKind
	MaxScanTuples int  // PlanHNSW: hnsw.max_scan_tuples = max(20 000, min(4 x ef_search / s, 100 000))
	ExactFallback bool // PlanHNSW: re-run the exact path in the same transaction when the scan exhausts (E <= 4 theta)
	Eligible      int64
}

// Searcher is the Strategy of the recall arms. Every method takes the ReadSession and opens its own short
// transaction(s) with s.Tx: arms run concurrently (N140).
type Searcher interface {
	SearchSemantic(ctx context.Context, s store.ReadSession, q SemanticQuery) ([]Hit, error)
	SearchLexical(ctx context.Context, s store.ReadSession, q LexicalQuery) ([]Hit, error)
	SearchChunks(ctx context.Context, s store.ReadSession, q ChunkQuery) ([]Hit, error)
	// SearchPages is BM25 over page_versions.text union HNSW over page_version_vectors, visible versions only (N73,
	// N139).
	SearchPages(ctx context.Context, s store.ReadSession, q PageQuery) ([]Hit, error)
	// Plan is the cost-based estimate and plan, chosen in Go (N138, N151).
	Plan(ctx context.Context, s store.ReadSession, f Filter) (SemanticPlan, error)
}

// Applier is for Async engines only; Apply is idempotent by (namespace_id, seq).
type Applier interface {
	Apply(ctx context.Context, events []*eventsv1.Event) error
}
