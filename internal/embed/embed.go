// Package embed owns everything about turning text into vectors: the unary
// Embedder transport abstraction, the nomic task-prefixing layer (the ONLY
// sanctioned way to embed), the gRPC adapter with retry/keepalive config
// (docs/02-storage.md section E.3), the embedding_cache keyed on the
// SHA-256 of the PREFIXED text, and the []float32 <-> BYTEA packing helpers
// shared with retrieval.
package embed

import "context"

// nomic-embed-text-v1.5 task prefixes (docs/01-retrieval.md section 4.1).
// Mandatory on every embedded text; omitting or doubling them is the single
// most expensive bug in the system.
const (
	// DocumentPrefix is prepended to every corpus text.
	DocumentPrefix = "search_document: "
	// QueryPrefix is prepended to every query text.
	QueryPrefix = "search_query: "
)

// Dims is the native nomic-embed-text-v1.5 dimensionality. Any response
// that is not exactly this many floats is a permanent error.
const Dims = 768

// Model is the embedding model identity recorded in embedding_cache rows.
const Model = "nomic-embed-text-v1.5"

// Embedder is the raw transport: ONE text per call, mirroring the unary-only
// embed.v1.Embedder RPC (no batching — throughput comes from bounded
// goroutine concurrency in the enrichment worker, not from batch calls).
// The text passed here must already carry its nomic task prefix; callers
// other than Client must not use this interface directly.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}
