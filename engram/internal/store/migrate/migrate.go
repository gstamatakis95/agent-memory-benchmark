// Package migrate is the goose runner behind `engramctl migrate` and the T3 harness (internal/store/pgtest): both apply
// the embedded migration files through Up, so a test database is built by exactly the code path an operator runs
// (PLAN.md section 8.1 T3 row, section 9.2). It also holds the static RLS check of section 8.3 (CheckRLS).
//
// The shard migrations must run on a SUPERUSER connection: 0001 creates the roles and extensions and then switches to
// engram_migrate (SET LOCAL ROLE) for the DDL, so engram_migrate owns every object (N133e).
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/gstamatakis95/engram/migrations"
)

// Set names one of the two independent migration sets of section 9.2.
type Set string

// The migration sets. The catalog set arrives with M0.3.
const (
	Shard   Set = "shard"
	Catalog Set = "catalog"
)

const (
	// lockTimeout is the lock_timeout every migration session runs with (section 9.2: DDL that takes strong locks sets
	// lock_timeout = '5s' and is retried by engramctl).
	lockTimeout = "5s"
	// maxAttempts is the retry budget for a lock_not_available (55P03) failure (section 9.2: up to 20 times).
	maxAttempts = 20
	retryPause  = time.Second
)

// Options select the database and the target of one run.
type Options struct {
	// DSN is a libpq URL or key/value string of a superuser connection to the database to migrate.
	DSN string
	// Set is the migration set; the zero value means Shard.
	Set Set
	// To is the target version: Up migrates to it (0 = latest), DownTo migrates down to it (0 = everything).
	To int64
	// RetryPause overrides the pause between lock_timeout retries (tests); zero means one second.
	RetryPause time.Duration
}

// Status is one migration file and whether it is applied.
type Status struct {
	Version int64
	Source  string
	Applied bool
}

// FS returns the embedded files of a set as a file system rooted at the migration directory.
func FS(set Set) (fs.FS, error) {
	switch set {
	case "", Shard:
		return fs.Sub(migrations.Shard, "shard")
	case Catalog:
		return nil, errors.New("migrate: the catalog migration set arrives with M0.3")
	default:
		return nil, fmt.Errorf("migrate: unknown migration set %q", set)
	}
}

func openDB(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("migrate: parse dsn: %w", err)
	}
	if _, ok := cfg.RuntimeParams["lock_timeout"]; !ok {
		cfg.RuntimeParams["lock_timeout"] = lockTimeout
	}
	return stdlib.OpenDB(*cfg), nil
}

func provider(db *sql.DB, set Set) (*goose.Provider, error) {
	fsys, err := FS(set)
	if err != nil {
		return nil, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, fsys)
	if err != nil {
		return nil, fmt.Errorf("migrate: open provider: %w", err)
	}
	return p, nil
}

// retry runs fn until it succeeds, fails with something other than lock_not_available, or the budget is spent.
func retry(ctx context.Context, pause time.Duration, fn func() error) error {
	if pause == 0 {
		pause = retryPause
	}
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
	}
	return fmt.Errorf("migrate: lock_timeout persisted after %d attempts: %w", maxAttempts, err)
}

// Up applies every pending migration up to o.To (0 = latest) and returns the versions it applied.
func Up(ctx context.Context, o Options) (applied []int64, err error) {
	db, err := openDB(o.DSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	p, err := provider(db, o.Set)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, syncSchemaVersion(ctx, db, p, o.Set)) }()
	err = retry(ctx, o.RetryPause, func() error {
		var rs []*goose.MigrationResult
		var e error
		if o.To == 0 {
			rs, e = p.Up(ctx)
		} else {
			rs, e = p.UpTo(ctx, o.To)
		}
		for _, r := range rs {
			applied = append(applied, r.Source.Version)
		}
		return e
	})
	return applied, err
}

// DownTo reverts migrations until version o.To is the current one (0 = revert everything).
func DownTo(ctx context.Context, o Options) (reverted []int64, err error) {
	db, err := openDB(o.DSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	p, err := provider(db, o.Set)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, syncSchemaVersion(ctx, db, p, o.Set)) }()
	err = retry(ctx, o.RetryPause, func() error {
		rs, e := p.DownTo(ctx, o.To)
		for _, r := range rs {
			reverted = append(reverted, r.Source.Version)
		}
		return e
	})
	return reverted, err
}

// List returns every embedded migration with its applied flag.
func List(ctx context.Context, o Options) ([]Status, error) {
	db, err := openDB(o.DSN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	p, err := provider(db, o.Set)
	if err != nil {
		return nil, err
	}
	sts, err := p.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: status: %w", err)
	}
	out := make([]Status, 0, len(sts))
	for _, s := range sts {
		out = append(out, Status{Version: s.Source.Version, Source: s.Source.Path,
			Applied: s.State == goose.StateApplied})
	}
	return out, nil
}

// syncSchemaVersion keeps shard_meta.schema_version equal to the goose version after every run (PLAN.md 9.2: the router
// marks a shard unavailable when the version it reads is outside the binary's range, so it must follow the migrations).
// Provisioning inserts the single shard_meta row; before that, or after a full Down, there is nothing to update. The
// catalog set has no shard_meta.
func syncSchemaVersion(ctx context.Context, db *sql.DB, p *goose.Provider, set Set) error {
	if set != "" && set != Shard {
		return nil
	}
	v, err := p.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("migrate: read the goose version: %w", err)
	}
	if v < 1 {
		return nil
	}
	var exists bool
	const probe = `SELECT to_regclass('public.shard_meta') IS NOT NULL`
	if err := db.QueryRowContext(ctx, probe).Scan(&exists); err != nil || !exists {
		return err
	}
	if _, err := db.ExecContext(ctx, `UPDATE shard_meta SET schema_version = $1, applied_at = now()
		 WHERE schema_version IS DISTINCT FROM $1`, v); err != nil {
		return fmt.Errorf("migrate: set shard_meta.schema_version: %w", err)
	}
	return nil
}
