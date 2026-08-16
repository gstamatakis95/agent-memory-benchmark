// Package store implements the append-only Postgres ledger described in
// docs/04-append-only.md. Every write to memories and
// memory_enrichment_events is an INSERT; current state (latest enrichment,
// pending, dead-letter, progress) is derived by views, functions and the
// anti-join + backoff predicate. There are NO UPDATE or DELETE statements in
// this package, by design.
package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Production defaults for the retry policy (docs/04-append-only.md section 3:
// exponential backoff base * 2^n_fail, docs/02-storage.md E.4: 1-second base,
// max_attempts 5). Tests pass their own values; later phases may override.
var (
	DefaultMaxAttempts = 5
	DefaultBackoffBase = time.Second
)

// DB is the subset of pgxpool.Pool / pgx.Tx the store queries need.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CopyDB additionally supports the CopyFrom bulk path (pgxpool.Pool, pgx.Tx).
type CopyDB interface {
	DB
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// NewPool builds a pgx connection pool from a DSN.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
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
