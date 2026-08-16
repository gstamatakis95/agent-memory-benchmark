package embed

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is the minimal Postgres surface the cache needs (satisfied by
// pgxpool.Pool and pgx.Tx). Kept local so embed does not depend on
// internal/store.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CacheKey is THE cache key function: SHA-256 of the exact PREFIXED text
// (docs/02-storage.md E.2 embedding_cache: "sha256 of the exact prefixed
// text"). Never the raw text, and never any timestamp — the key is a pure
// function of the string so identical text always hits, on any run, at any
// time.
func CacheKey(prefixedText string) []byte {
	h := sha256.Sum256([]byte(prefixedText))
	return h[:]
}

const cacheGetSQL = `
SELECT vector FROM embedding_cache
WHERE  content_hash = $1 AND model = $2 AND task_prefix = $3`

// Insert-only, append-only friendly: concurrent workers embedding the same
// text race harmlessly (docs/02-storage.md E.5).
const cachePutSQL = `
INSERT INTO embedding_cache (content_hash, model, task_prefix, dims, vector)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (content_hash, model, task_prefix) DO NOTHING`

// CacheGet looks up a previously stored vector by (key, model, taskPrefix).
// The second return is false on a miss.
func CacheGet(ctx context.Context, db DB, key []byte, model, taskPrefix string) ([]float32, bool, error) {
	var packed []byte
	err := db.QueryRow(ctx, cacheGetSQL, key, model, taskPrefix).Scan(&packed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("embed: cache get: %w", err)
	}
	vec, err := UnpackVector(packed)
	if err != nil {
		return nil, false, err
	}
	return vec, true, nil
}

// CachePut inserts a vector; ON CONFLICT DO NOTHING keeps it idempotent.
func CachePut(ctx context.Context, db DB, key []byte, model, taskPrefix string, vec []float32) error {
	_, err := db.Exec(ctx, cachePutSQL, key, model, taskPrefix, int16(len(vec)), PackVector(vec))
	if err != nil {
		return fmt.Errorf("embed: cache put: %w", err)
	}
	return nil
}
