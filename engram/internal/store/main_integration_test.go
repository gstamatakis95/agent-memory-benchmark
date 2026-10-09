//go:build integration

package store_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// Fixture namespaces. A1 and Z1 share the shard (the case that matters, PLAN.md section 8.3); the empty one owns no
// rows but is a real namespace.
const (
	nsA     = "0190c000-0000-7000-8000-00000000000a"
	nsB     = "0190c000-0000-7000-8000-00000000000b"
	nsEmpty = "0190c000-0000-7000-8000-00000000000e"

	tenantAcme = "acme"
	tenantZeta = "zeta"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m)) }

var seedOnce sync.Once

// seeded makes sure the two fixture namespaces hold the complete minimal graph (every table) and the empty one only
// its ownership rows.
func seeded(t testing.TB) {
	t.Helper()
	seedOnce.Do(func() {
		ctx := context.Background()
		pgtest.SeedNamespace(ctx, t, nsA, tenantAcme)
		pgtest.SeedNamespace(ctx, t, nsB, tenantZeta)
		pgtest.SeedOwnership(ctx, t, nsEmpty, tenantAcme)
	})
}

func scope(ns, tenant string) pgtest.Scope {
	return pgtest.Scope{Namespace: ns, Tenant: tenant, Epoch: 1}
}

// connect opens a connection as role and registers its closing.
func connect(t testing.TB, role pgtest.Role) *pgx.Conn {
	t.Helper()
	c, err := pgtest.Connect(context.Background(), role)
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// scopedTx opens a pooled transaction as role with the scope GUCs set and rolls it back when the test ends. Loops use
// readTx instead, which gives the connection back after every iteration.
func scopedTx(t testing.TB, role pgtest.Role, sc pgtest.Scope) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := pgtest.BeginScoped(ctx, pgtest.Pool(t, role), sc)
	if err != nil {
		t.Fatalf("begin as %s: %v", role, err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return tx
}

// readTx runs fn in a scoped transaction that is rolled back (and its connection released) when fn returns.
func readTx(t testing.TB, role pgtest.Role, sc pgtest.Scope, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := pgtest.BeginScoped(ctx, pgtest.Pool(t, role), sc)
	if err != nil {
		t.Fatalf("begin as %s: %v", role, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	fn(tx)
}
