// Package extract is structured extraction with prompt versioning and a cache (PLAN.md section 2.2.10; register D9,
// D11, N87, N110). One structured LLM call per chunk, document summarisation, and a content-addressed per-namespace
// cache in blob. It owns the JSON schema and the prompt versions (`extract/v1`, `summarize/v1`, section 6). Pattern:
// Decorator; Cached wraps any Extractor, and cache errors are logged, never returned. M0.1 declares the signatures
// only.
package extract

import (
	"context"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/gateway"
)

// EntityMention is an entity named by an extracted fact.
type EntityMention struct {
	Name string
	Type string
}

// CausalRef points at the fact (by index within the chunk's extraction) that causes this one; target_index < i.
type CausalRef struct {
	TargetIndex int
	Relation    string
}

// Fact is one extracted fact.
type Fact struct {
	Text          string
	Type          memoryv1.FactType
	Who           string
	What          string
	When          string
	Where         string
	Why           string
	OccurredStart *time.Time
	OccurredEnd   *time.Time
	// MentionedAt is ALWAYS the chunk's mentioned_at, set by the activity: the model has no say in the as_of key (D9).
	MentionedAt time.Time
	SaidAt      *time.Time // display and ranking only
	Entities    []EntityMention
	Causes      []CausalRef
	Confidence  float32
}

// Extraction is the validated result for one chunk.
type Extraction struct {
	Facts         []Fact
	PromptVersion string
	Model         string
}

// ChunkInput is everything an extraction depends on.
type ChunkInput struct {
	Header        string
	Text          string
	ContentHash   [32]byte
	ItemTimestamp time.Time
	Context       string
	Metadata      map[string]any
	EntityHints   []string
	Tags          []string
	Mission       string
	HeaderHash    [32]byte
}

// CacheKey is the five inputs of the cache key: chunk hash, prompt version, model, schema version and render hash.
type CacheKey struct {
	ChunkHash     [32]byte
	PromptVersion string
	Model         string
	SchemaVersion string
	RenderHash    [32]byte
}

// RenderHash is sha256(day(mentioned_at) || context || canonical(metadata) || sorted(entity_hints) || retain.mission ||
// heading path): N87, N110(a); NOT the document summary.
func RenderHash(in ChunkInput) [32]byte { panic("stub") }

// Extractor extracts facts and summarises documents.
type Extractor interface {
	Extract(ctx context.Context, in ChunkInput) (*Extraction, error)
	Summarize(ctx context.Context, content string) (string, *gateway.Usage, error)
}

// Cache is the content-addressed extraction cache: "{prefix}xcache/{sha256(chunk_hash||prompt||model||schema||
// render_hash)}.json" is the extraction_key.
type Cache interface {
	Get(ctx context.Context, k CacheKey) (*Extraction, bool, error)
	Put(ctx context.Context, k CacheKey, x *Extraction) error
}

// Cached wraps an Extractor with a Cache.
func Cached(e Extractor, c Cache) Extractor { panic("stub") }
