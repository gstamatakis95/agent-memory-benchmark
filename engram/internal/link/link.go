// Package link builds fact links under a budget (PLAN.md section 2.2.12). For each new fact it builds `fact_links` of
// four kinds: entity, temporal (+-24 h, at most 20), semantic (kNN k = 10, cosine >= 0.75 over fact_vectors of the
// current model) and causal; at most 60 per fact; links to invisible facts are never built. Deterministic for equal
// input (ties by weight then id). Pattern: Strategy (BudgetLinker). M0.1 declares the signatures only.
package link

import (
	"context"

	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/index"
	"github.com/gstamatakis95/engram/internal/store"
)

// BuildInput is the batch of new facts to link.
type BuildInput struct {
	Scope id.Scope
	Facts []store.Fact
}

// BuildResult is the set of links to insert.
type BuildResult struct {
	Links []store.Link
}

// LinkBudget bounds the links per fact.
type LinkBudget struct {
	TemporalPerFact, SemanticK, EntityPerFact, MaxPerFact int
	SemanticMinCosine                                     float32
}

// Linker builds links.
type Linker interface {
	Build(ctx context.Context, tx store.ReadTx, idx index.Searcher, in BuildInput, b LinkBudget) (*BuildResult, error)
}
