package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"example.com/agentmem/migrations"
)

// goose keeps package-level state (base FS, dialect); serialize access so
// concurrent callers (parallel tests) cannot interleave configuration.
var gooseMu sync.Mutex

// ApplyMigrations runs all embedded goose migrations against the pool's
// database. Tests and the migrate binary share the same embedded FS, so the
// schema under test is byte-for-byte the deployed schema.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDB(*pool.Config().ConnConfig)
	defer db.Close()
	return RunGoose(ctx, db, "up")
}

// RunGoose executes a goose command ("up", "down", "status") over the
// embedded migrations. Used by ApplyMigrations and cmd/migrate.
func RunGoose(ctx context.Context, db *sql.DB, command string) error {
	gooseMu.Lock()
	defer gooseMu.Unlock()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	switch command {
	case "up":
		return goose.UpContext(ctx, db, ".")
	case "down":
		return goose.DownContext(ctx, db, ".")
	case "status":
		return goose.StatusContext(ctx, db, ".")
	default:
		return fmt.Errorf("unknown goose command %q (want up|down|status)", command)
	}
}
