// Package recall is the whole D10 read path with zero generative calls (PLAN.md section 2.2.13; register D10, N53, N54,
// N67, N106, N129, N138, N140, N164, N165). Patterns: Strategy (Arm) for what is searched, a small DAG scheduler for
// the retrieval stage (nodes declare Needs(), so lexical and temporal overlap the embedding round-trip, N54: a purely
// sequential Pipeline cannot express that) and Pipeline (pipeline.Step[*State]) for the stages after it; the planner is
// the composition and nothing else. M0.1 declares the signatures only.
//
// The planner is a small DAG scheduler followed by sequential pipeline steps (N140): Retrieve = DAG{ visibility (marker
// sets, allowed documents, plan estimate: N116, N138), embed, arms by Needs() } then Fuse, Rerank, Boost, Pack, Stream
// (each of the last five is a pipeline.Step[*State]).
package recall

import (
	"context"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/index"
	"github.com/gstamatakis95/engram/internal/pipeline"
	"github.com/gstamatakis95/engram/internal/store"
)

// Query is the planner input. Filter, Plan and the marker sets arrive as values computed once (N140).
type Query struct {
	Scope          id.Scope
	Text           string
	Vector         []float32 // nil: dense arms skipped
	Budget         memoryv1.Budget
	Filter         index.Filter
	Plan           index.SemanticPlan
	QueryTimestamp *time.Time
	Window         *memoryv1.TemporalWindow
	MaxTokens      int
	Deadline       time.Time
}

// Candidate is one arm result.
type Candidate struct {
	Ref         id.MemoryRef
	Arm         string
	Rank        int
	ArmScore    float64
	MentionedAt time.Time
	Provenance  *memoryv1.Provenance
}

// Needs says what an arm waits for before it can start.
type Needs uint8

// The arm dependencies: NeedsNone arms start as soon as `visibility` is done and overlap the embedding round-trip,
// NeedsVector arms start when the embedding arrives, NeedsSeeds is the graph arm, which runs two seeded waves (N54).
const (
	NeedsNone Needs = iota + 1
	NeedsVector
	NeedsSeeds
)

// Arm is the Strategy: one retrieval method. The planner schedules by Needs(). Each Run opens its own short read
// transaction(s) on the session; Filter, Plan and the marker sets arrive in q as values computed once (N140).
type Arm interface {
	Name() string // semantic | lexical | graph | temporal | chunks
	Needs() Needs // NeedsNone | NeedsVector | NeedsSeeds
	Run(ctx context.Context, s store.ReadSession, q *Query, seeds []Candidate, capN int) ([]Candidate, error)
}

// Fused is a candidate after reciprocal rank fusion; the score is an exact rational rendered as float64 for transport.
type Fused struct {
	Candidate
	Score float64
	Arms  []string
}

// Ranked is a fused candidate after the optional rerank.
type Ranked struct {
	Fused
	RerankScore *float64
}

// PackResult is what Pack returns: the packed items in rank order and the skip count.
type PackResult struct {
	Items        []Ranked
	TokensUsed   int
	SkippedCount int
}

// Fuser is RRF with k = 60 in math/big.Rat: permutation-invariant exactly (N67).
type Fuser interface {
	Fuse(lists [][]Candidate, k int) []Fused
}

// Reranker reranks the top 0/50/150 (N53); the default model is bge-reranker-base.
type Reranker interface {
	Rerank(ctx context.Context, query string, in []Fused, top int) ([]Ranked, error)
}

// Packer is greedy: skip, never truncate; rank 1 always whole (N129).
type Packer interface {
	Pack(in []Ranked, maxTokens int) PackResult
}

// Booster applies the recency (at most +10 %), temporal (+10 %) and proof (+5 %) boosts, clamped to [0.75, 1.25].
type Booster interface {
	Boost(in []Ranked, q *Query) []Ranked
}

// Sink receives the stream: batches of 10, then the trailing stats.
type Sink interface {
	Batch(ctx context.Context, results []*memoryv1.RecallResult) error
	Stats(ctx context.Context, st *memoryv1.RecallStats) error
}

// QueryEmbedder embeds the query text with the `search_query: ` prefix behind an LRU of 10,000 entries per process,
// keyed (namespace_id, sha256("search_query: " + text)); a repeat query skips the 25 ms hop (section 2.5).
type QueryEmbedder interface {
	Embed(ctx context.Context, ns id.NamespaceID, text string) ([]float32, error)
}

// Planner is the composition: it runs a recall on a ReadSession and streams to the sink.
type Planner interface {
	Recall(ctx context.Context, q *Query, sink Sink) error
}

// State is the shared state of the pipeline stages after Retrieve (each stage is a pipeline.Step[*State]).
type State struct {
	Query   *Query
	Lists   [][]Candidate
	Fused   []Fused
	Ranked  []Ranked
	Packed  PackResult
	Timings []pipeline.StageTiming
}
