//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// TestFence_ExclusiveHelperTimeouts pins N82 and PLAN.md section 5: an exclusive taker makes ONE attempt that ends by
// lock_timeout = 35 s (SQLSTATE 55P03), longer than any legal 30 s writer. The role default statement_timeout of
// engram_app, engram_move and engram_admin is 30 s and a function cannot re-arm the timer of the statement that calls
// it, so the caller runs `SET LOCAL statement_timeout = '36s'` first; without it the 30 s statement timeout (57014)
// ends the attempt before the lock timeout does. A superuser transaction holds the shared fence and the shared
// derivation lock of one namespace for the whole test (35 s of wall time; the three attempts run concurrently).
func TestFence_ExclusiveHelperTimeouts(t *testing.T) {
	if testing.Short() {
		t.Skip("takes 36 s")
	}
	ctx := context.Background()
	const ns = "0190c000-0000-7000-8000-0000000000a9"

	holder := connect(t, pgtest.Super)
	htx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = htx.Rollback(context.Background()) })
	var fence, deriv bool
	if err := htx.QueryRow(ctx, `SELECT engram_try_ns_fence($1::uuid), engram_try_derivation_lock($1::uuid)`, ns).
		Scan(&fence, &deriv); err != nil || !fence || !deriv {
		t.Fatalf("the holder could not take the shared locks: %v %v (err %v)", fence, deriv, err)
	}

	type result struct {
		code    string
		elapsed time.Duration
	}
	attempt := func(role pgtest.Role, setStatementTimeout bool, call string) result {
		c, err := pgx.Connect(ctx, pgtest.DSN(role))
		if err != nil {
			t.Error(err)
			return result{}
		}
		defer func() { _ = c.Close(context.Background()) }()
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Error(err)
			return result{}
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if setStatementTimeout {
			if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '36s'"); err != nil {
				t.Error(err)
				return result{}
			}
		}
		start := time.Now()
		_, err = tx.Exec(ctx, "SELECT "+call+"($1::uuid)", ns)
		return result{sqlState(err), time.Since(start)}
	}

	var wg sync.WaitGroup
	var withSet, withoutSet, derivation result
	wg.Add(3)
	go func() { defer wg.Done(); withSet = attempt(pgtest.App, true, "engram_ns_fence_exclusive") }()
	go func() { defer wg.Done(); withoutSet = attempt(pgtest.Move, false, "engram_ns_fence_exclusive") }()
	go func() { defer wg.Done(); derivation = attempt(pgtest.Admin, true, "engram_derivation_lock_exclusive") }()
	wg.Wait()

	within := func(name string, r result, code string, lo, hi time.Duration) {
		t.Helper()
		if r.code != code || r.elapsed < lo || r.elapsed > hi {
			t.Errorf("%s: SQLSTATE %q after %v, want %s between %v and %v", name, r.code, r.elapsed, code, lo, hi)
		}
	}
	within("fence, caller sets statement_timeout 36 s", withSet, "55P03", 34*time.Second, 36*time.Second)
	within("derivation lock, caller sets statement_timeout 36 s", derivation, "55P03", 34*time.Second, 36*time.Second)
	within("fence, no SET LOCAL: the role's 30 s statement_timeout ends it", withoutSet, "57014", 29*time.Second,
		32*time.Second)
}
