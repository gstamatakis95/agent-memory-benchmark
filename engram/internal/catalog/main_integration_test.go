//go:build integration

package catalog_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/migrate"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// TestMain starts the one PostgreSQL server of this package (T3: one container per package, internal/store/pgtest);
// each test gets a database of its own with the catalog schema, built by the code path of `engramctl migrate`.
func TestMain(m *testing.M) { os.Exit(pgtest.MainServerOnly(m)) }

// rolePassword is the password the tests give catalog_app and catalog_admin.
const rolePassword = "pw"

var dbs sync.Map // superuser DSN -> pgtest.Database

// newCatalogDB returns the superuser DSN of a fresh database with the catalog migrations applied.
func newCatalogDB(t testing.TB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d := pgtest.NewDatabase(t, false)
	dsn := d.DSN(pgtest.Super)
	if _, err := migrate.Up(ctx, migrate.Options{DSN: dsn, Set: migrate.Catalog}); err != nil {
		t.Fatalf("catalog migrations: %v", err)
	}
	dbs.Store(dsn, d)
	return dsn
}

// roleDSN is the DSN of a catalog login (catalog_app, catalog_admin) on the database behind dsn.
func roleDSN(dsn, user string) string {
	d, _ := dbs.Load(dsn)
	return d.(pgtest.Database).RoleDSN(user, rolePassword)
}

// connect opens one connection and closes it when the test ends.
func connect(t testing.TB, dsn string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}
