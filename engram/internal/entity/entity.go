// Package entity is per-namespace fuzzy entity resolution (PLAN.md section 2.2.11; register N69, N118, N131). It maps
// extracted mentions to `entities` using trigram similarity (reached only through the SECURITY DEFINER function), an
// alias table, mandatory type agreement and hints. Pattern: Strategy (MergePolicy). Creates and merges are applied by
// EntityWriter.UpsertSorted in canonical_norm order in one statement, so concurrent commits cannot deadlock. M0.1
// declares the signatures only.
package entity

import (
	"context"

	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store"
)

// Mention is an extracted entity mention to resolve.
type Mention struct {
	Name string
	Type string
	Fact id.FactID
}

// Candidate is an existing entity considered for a mention.
type Candidate struct {
	Entity id.EntityID
	Name   string
	Type   string
	Score  float32
}

// Resolution is the outcome for one mention.
type Resolution struct {
	Mention Mention
	Entity  id.EntityID
	Created bool
	Method  string // exact | alias | trigram | hint | new
	Score   float32
}

// Options are the resolver options.
type Options struct {
	Threshold     float32 // 0.6; 0.85 when a type is unknown
	MaxCandidates int
	TypeStrict    bool
}

// Resolver resolves mentions.
type Resolver interface {
	Resolve(ctx context.Context, tx store.ReadTx, ms []Mention, hints []string, o Options) ([]Resolution, error)
}

// MergePolicy decides whether a mention merges into a candidate.
type MergePolicy interface {
	ShouldMerge(m Mention, c Candidate) bool
}
