//go:build integration

package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// TestResolver_PausedCatalogServesEveryCachedNamespace is the third clause of the M0.3 exit criterion: "a catalog
// paused 30 min serves every cached namespace". The catalog container is frozen with `docker pause` (a partition:
// connections hang, they are not refused), a fake clock is advanced 30 minutes (no 30-minute sleep), and every
// namespace resolved before the pause is still served, with the age gauge near 30 minutes, while a namespace never
// resolved is UNAVAILABLE with RetryInfo 2 s. Unpausing heals it: the next probe succeeds and the gauge returns to 0.
func TestResolver_PausedCatalogServesEveryCachedNamespace(t *testing.T) {
	dsn := newCatalogDB(t)
	admin := connect(t, dsn)
	seed(t, admin)
	app := openApp(t, dsn, nil)
	ctx := context.Background()

	const n = 200
	nss := make([]id.NamespaceID, n)
	for i := range nss {
		e, err := app.Create(ctx, catalog.CreateParams{Tenant: "acme", Name: "ns" + itoa(i), Shard: id.ShardID(i % 2)})
		if err != nil {
			t.Fatal(err)
		}
		nss[i] = e.Namespace
	}
	clk := newClock()
	r := catalog.NewResolver(app, nil, catalog.ResolverOptions{Clock: clk.Now, LoadTimeout: time.Second})
	for _, ns := range nss {
		if _, err := r.Resolve(ctx, ns); err != nil {
			t.Fatal(err)
		}
	}

	if err := pgtest.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	paused := true
	t.Cleanup(func() {
		if paused {
			_ = pgtest.Unpause(context.Background())
		}
	})

	clk.Advance(30 * time.Minute) // every TTL (60 s) is long expired
	start := time.Now()
	for i, ns := range nss {
		e, err := r.Resolve(ctx, ns)
		if err != nil || e.Namespace != ns || e.State != catalog.StateActive {
			t.Fatalf("namespace %d with the catalog paused 30 min: %+v, %v", i, e, err)
		}
	}
	// No caller waits for the hung catalog (F5): every expired entry is served at once and refreshed in the background.
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("serving %d expired namespaces took %v; a hung catalog must cost the callers nothing", n, took)
	}
	r.WaitRefreshes() // the background probe times out (1 s) and marks the catalog down
	for i, ns := range nss {
		if e, err := r.Resolve(ctx, ns); err != nil || e.Namespace != ns {
			t.Fatalf("namespace %d, catalog known down: %+v, %v", i, e, err)
		}
	}
	if age := r.StaleAge(); age < 29*time.Minute || age > 31*time.Minute {
		t.Errorf("age gauge = %v; want about 30 min", age)
	}
	st := r.Stats()
	if st.StaleServed < n {
		t.Errorf("stale served = %d; want at least %d", st.StaleServed, n)
	}
	_, err := r.Resolve(ctx, id.NewNamespaceID())
	if !errs.Is(err, errs.KindUnavailable) || errs.ToStatus(err).Code().String() != "Unavailable" {
		t.Errorf("a miss = %v; want UNAVAILABLE", err)
	}
	if e, ok := err.(*errs.Error); !ok || e.Retry != 2*time.Second { //nolint:errorlint // exact type from the resolver
		t.Errorf("a miss carries RetryInfo 2 s: %+v", err)
	}

	if err := pgtest.Unpause(ctx); err != nil {
		t.Fatal(err)
	}
	paused = false
	clk.Advance(2 * time.Second) // the next probe is due
	waitFor(t, func() bool {
		clk.Advance(2 * time.Second)
		_, err := r.Resolve(ctx, nss[0])
		return err == nil && r.StaleAge() == 0
	})
}
