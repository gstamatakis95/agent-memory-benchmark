package embed

import (
	"context"
	"strings"
)

// Client is the prefixing layer and the ONLY way callers embed text. It owns
// the nomic task prefixes, the embedding_cache lookup/insert (keyed on the
// SHA-256 of the prefixed text), dimension validation, and L2 normalization.
// Callers hand it RAW text; handing it text that already carries a task
// prefix is a permanent error (the double-prefix guard).
type Client struct {
	transport Embedder
	db        DB // nil disables the persistent embedding_cache
	model     string
}

// NewClient wraps a transport Embedder. db may be nil (no persistent cache —
// used by tests and pure query-side callers).
func NewClient(transport Embedder, db DB) *Client {
	return &Client{transport: transport, db: db, model: Model}
}

// EmbedDocument embeds a corpus text: prepends "search_document: " exactly
// once, consults the cache, and returns a 768-dim L2-normalized vector.
func (c *Client) EmbedDocument(ctx context.Context, raw string) ([]float32, error) {
	return c.embed(ctx, raw, DocumentPrefix)
}

// EmbedQuery embeds a query text: prepends "search_query: " exactly once,
// consults the cache, and returns a 768-dim L2-normalized vector.
func (c *Client) EmbedQuery(ctx context.Context, raw string) ([]float32, error) {
	return c.embed(ctx, raw, QueryPrefix)
}

func (c *Client) embed(ctx context.Context, raw, prefix string) ([]float32, error) {
	// Double-prefix guard: raw must be raw. Retrying cannot fix a caller
	// bug, so this is permanent.
	if strings.HasPrefix(raw, DocumentPrefix) || strings.HasPrefix(raw, QueryPrefix) {
		return nil, Permanentf("embed: text already carries a nomic task prefix; pass raw text: %.40q", raw)
	}
	prefixed := prefix + raw
	key := CacheKey(prefixed) // sha256 of the PREFIXED text, nothing else

	if c.db != nil {
		if vec, hit, err := CacheGet(ctx, c.db, key, c.model, prefix); err != nil {
			return nil, err
		} else if hit {
			return vec, nil
		}
	}

	vec, err := c.transport.Embed(ctx, prefixed)
	if err != nil {
		return nil, err
	}
	// Wrong dimensionality can never be fixed by a retry: typed permanent
	// so enrichment dead-letters it (docs/02-storage.md E.7 item 7).
	if len(vec) != Dims {
		return nil, Permanentf("embed: embedder returned %d dims, want %d", len(vec), Dims)
	}
	L2Normalize(vec)

	if c.db != nil {
		// Best-effort insert: ON CONFLICT DO NOTHING makes races harmless,
		// and a failed cache write must not fail a successful embed.
		_ = CachePut(ctx, c.db, key, c.model, prefix, vec)
	}
	return vec, nil
}
