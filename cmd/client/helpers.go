package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/agentmem/internal/blob"
	"example.com/agentmem/internal/embed"
)

// blobHash aliases the shared content-hash so eval-side identity mapping and
// the server's ingest write use the one definition.
func blobHash(raw []byte) []byte { return blob.Hash(raw) }

// newPGPool opens a small pgx pool for the read-only cache statistics.
func newPGPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// dimsRetryEmbedder mirrors the server-side wrapper (cmd/server/main.go): a
// bounded re-ask on wrong-dims responses so the chaos tier's randomly
// injected 512-dim replies (MOCK_BAD_DIMS_RATE) cannot permanently fail a
// query embed, while a deterministically broken embedder still surfaces the
// typed PermanentError.
type dimsRetryEmbedder struct {
	inner   embed.Embedder
	retries int
}

func (d *dimsRetryEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	var lastErr error
	for i := 0; i <= d.retries; i++ {
		vec, err := d.inner.Embed(ctx, text)
		if err == nil || !embed.IsPermanent(err) {
			return vec, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// transientRetryEmbedder retries transient (non-permanent) embed failures
// — e.g. the embedder briefly unreachable mid-eval — with a short linear
// backoff, so a blip cannot kill a multi-thousand-question run. Permanent
// errors (typed wrong-dims after the dims re-ask below it in the chain)
// pass through untouched: retrying a deterministic failure only wastes
// the eval's clock.
type transientRetryEmbedder struct {
	inner   embed.Embedder
	retries int
	backoff time.Duration
}

func (t *transientRetryEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	var lastErr error
	for i := 0; i <= t.retries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(i) * t.backoff):
			}
		}
		vec, err := t.inner.Embed(ctx, text)
		if err == nil || embed.IsPermanent(err) {
			return vec, err
		}
		lastErr = err
	}
	return nil, lastErr
}
