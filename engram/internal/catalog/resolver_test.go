package catalog_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// fakeClock is a settable clock; the Resolver never sleeps on it, so a test advances 30 minutes in one call.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fixture is a MemoryCatalog with one tenant, two shards and n active namespaces.
type fixture struct {
	cat *catalog.MemoryCatalog
	clk *fakeClock
	res *catalog.CachedResolver
	nss []id.NamespaceID
}

func newFixture(t testing.TB, n int, o catalog.ResolverOptions) *fixture {
	t.Helper()
	f := &fixture{cat: catalog.NewMemoryCatalog(nil), clk: newClock()}
	f.cat.AddTenant(catalog.TenantEntry{Tenant: "acme"})
	f.cat.AddShard(0, "")
	f.cat.AddShard(1, "")
	for i := range n {
		e, err := f.cat.Create(context.Background(), catalog.CreateParams{Tenant: "acme", Name: "ns" + itoa(i),
			Shard: id.ShardID(i % 2)})
		if err != nil {
			t.Fatal(err)
		}
		f.nss = append(f.nss, e.Namespace)
	}
	o.Clock = f.clk.Now
	f.res = catalog.NewResolver(f.cat, f.cat, o)
	return f
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

func TestResolver_HitDoesNotTouchTheCatalog(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	ctx := context.Background()
	e1, err := f.res.Resolve(ctx, f.nss[0])
	if err != nil || e1.State != catalog.StateActive || e1.Epoch != 1 {
		t.Fatalf("first resolve = %+v, %v", e1, err)
	}
	f.cat.SetDown(true) // a hit must not need the catalog
	for range 100 {
		e, err := f.res.Resolve(ctx, f.nss[0])
		if err != nil || e != e1 {
			t.Fatalf("hit = %v, %v", e, err)
		}
	}
	if s := f.res.Stats(); s.Misses != 1 || s.Hits != 100 {
		t.Errorf("stats = %+v; want 1 miss and 100 hits", s)
	}
}

func TestResolver_TTLExpiryReloadsWhileReachable(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	ctx := context.Background()
	_, _ = f.res.Resolve(ctx, f.nss[0])
	if _, err := f.cat.BumpEpoch(ctx, f.nss[0], 1, "failover"); err != nil {
		t.Fatal(err)
	}
	f.cat.DropNotifications(true) // the change is silent: only the TTL repairs it
	f.cat.Notify(f.nss[0])
	e, _ := f.res.Resolve(ctx, f.nss[0])
	if e.Epoch != 1 {
		t.Fatalf("inside the TTL the cached epoch 1 is served, got %d", e.Epoch)
	}
	f.clk.Advance(59 * time.Second)
	if e, _ := f.res.Resolve(ctx, f.nss[0]); e.Epoch != 1 {
		t.Fatalf("at 59 s the cached entry is still fresh, got epoch %d", e.Epoch)
	}
	f.clk.Advance(2 * time.Second)
	// D4: the expired entry is served as it is and refreshed in the background, no caller waits for the catalog.
	if e, err := f.res.Resolve(ctx, f.nss[0]); err != nil || e.Epoch != 1 {
		t.Fatalf("an expired entry is served while it refreshes: %+v, %v", e, err)
	}
	f.res.WaitRefreshes()
	if e, err := f.res.Resolve(ctx, f.nss[0]); err != nil || e.Epoch != 2 {
		t.Fatalf("after the refresh the entry is new: %+v, %v", e, err)
	}
}

func TestResolver_NegativeEntryIsServedFiveSecondsWhenReachable(t *testing.T) {
	f := newFixture(t, 0, catalog.ResolverOptions{})
	ctx := context.Background()
	ghost := id.NewNamespaceID()
	if _, err := f.res.Resolve(ctx, ghost); !errs.Is(err, errs.KindNotFound) {
		t.Fatalf("unknown namespace: %v", err)
	}
	created, err := f.cat.Create(ctx, catalog.CreateParams{Namespace: ghost, Tenant: "acme", Name: "late", Shard: 0})
	if err != nil {
		t.Fatal(err)
	}
	f.cat.DropNotifications(true)
	if _, err := f.res.Resolve(ctx, ghost); !errs.Is(err, errs.KindNotFound) {
		t.Fatal("a negative entry is served inside its 5 s")
	}
	f.clk.Advance(5 * time.Second)
	if e, err := f.res.Resolve(ctx, ghost); err != nil || e.Namespace != created.Namespace {
		t.Fatalf("after 5 s the negative entry expired: %v, %v", e, err)
	}
}

func TestResolver_StaleEntriesAreServedForTheWholeOutage(t *testing.T) {
	f := newFixture(t, 3, catalog.ResolverOptions{})
	ctx := context.Background()
	for _, ns := range f.nss {
		if _, err := f.res.Resolve(ctx, ns); err != nil {
			t.Fatal(err)
		}
	}
	f.cat.SetDown(true)
	f.clk.Advance(30 * time.Minute)
	for range 2 { // the first pass starts the refreshes, which fail and mark the catalog down
		for _, ns := range f.nss {
			e, err := f.res.Resolve(ctx, ns)
			if err != nil || e.Namespace != ns {
				t.Fatalf("existing namespace after 30 min down: %v, %v", e, err)
			}
		}
		f.res.WaitRefreshes()
	}
	if age := f.res.StaleAge(); age < 29*time.Minute {
		t.Errorf("stale age gauge = %v; want about 30 min", age)
	}
	// Only a miss is UNAVAILABLE, with RetryInfo 2 s.
	_, err := f.res.Resolve(ctx, id.NewNamespaceID())
	if !errs.Is(err, errs.KindUnavailable) {
		t.Fatalf("a miss while down = %v; want UNAVAILABLE", err)
	}
	if st := errs.ToStatus(err); st.Code().String() != "Unavailable" {
		t.Errorf("code = %v", st.Code())
	}
	f.clk.Advance(24 * time.Hour)
	if _, err := f.res.Resolve(ctx, f.nss[0]); err != nil {
		t.Fatalf("an entry stays served for as long as the outage lasts (StaleMax 0): %v", err)
	}
	f.cat.SetDown(false)
	f.clk.Advance(2 * time.Second) // the next probe
	if _, err := f.res.Resolve(ctx, f.nss[0]); err != nil {
		t.Fatal(err)
	}
	f.res.WaitRefreshes()
	if f.res.StaleAge() != 0 || f.res.Stats().Down {
		t.Error("the gauge returns to 0 once the catalog answers")
	}
}

func TestResolver_StaleMaxBoundsTheOutage(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{StaleMax: time.Hour})
	ctx := context.Background()
	_, _ = f.res.Resolve(ctx, f.nss[0])
	f.cat.SetDown(true)
	f.clk.Advance(30 * time.Minute)
	if _, err := f.res.Resolve(ctx, f.nss[0]); err != nil {
		t.Fatal(err)
	}
	f.clk.Advance(31 * time.Minute)
	if _, err := f.res.Resolve(ctx, f.nss[0]); !errs.Is(err, errs.KindUnavailable) {
		t.Fatalf("past StaleMax the entry is a miss: %v", err)
	}
}

func TestResolver_NegativeEntriesLiveTenMinutesWhileDown(t *testing.T) {
	f := newFixture(t, 0, catalog.ResolverOptions{})
	ctx := context.Background()
	ghost := id.NewNamespaceID()
	_, _ = f.res.Resolve(ctx, ghost) // negative entry
	f.cat.SetDown(true)
	f.clk.Advance(6 * time.Second) // reachable it would have expired; the load fails and marks the catalog down
	if _, err := f.res.Resolve(ctx, ghost); !errs.Is(err, errs.KindNotFound) {
		t.Fatalf("the answer for one id must not flip at the onset of an outage (F7): %v", err)
	}
	f.clk.Advance(5 * time.Minute)
	if _, err := f.res.Resolve(ctx, ghost); !errs.Is(err, errs.KindNotFound) {
		t.Fatalf("a negative entry is served while down inside 10 min: %v", err)
	}
	f.clk.Advance(6 * time.Minute)
	if _, err := f.res.Resolve(ctx, ghost); !errs.Is(err, errs.KindUnavailable) {
		t.Fatalf("a negative entry is never served past 10 min unreachable: %v", err)
	}
}

func TestResolver_DownCatalogIsProbedOncePerInterval(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	ctx := context.Background()
	_, _ = f.res.Resolve(ctx, f.nss[0])
	f.cat.SetDown(true)
	f.clk.Advance(2 * time.Minute)
	_, _ = f.res.Resolve(ctx, f.nss[0]) // first expired resolve: a background refresh, which fails
	f.res.WaitRefreshes()
	before := f.res.Stats().Misses
	for range 50 {
		if _, err := f.res.Resolve(ctx, f.nss[0]); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.res.Stats().Misses; got != before {
		t.Errorf("%d catalog loads inside one probe interval; want 0", got-before)
	}
	f.clk.Advance(1100 * time.Millisecond)
	_, _ = f.res.Resolve(ctx, f.nss[0])
	f.res.WaitRefreshes()
	if got := f.res.Stats().Misses; got != before+1 {
		t.Errorf("loads after one interval = %d; want exactly one probe", got-before)
	}
}

func TestResolver_NotificationDropsOnlyTheNamedEntry(t *testing.T) {
	f := newFixture(t, 2, catalog.ResolverOptions{})
	ctx := context.Background()
	a, b := f.nss[0], f.nss[1]
	_, _ = f.res.Resolve(ctx, a)
	_, _ = f.res.Resolve(ctx, b)
	if _, err := f.cat.BumpEpoch(ctx, a, 1, "move"); err != nil {
		t.Fatal(err)
	}
	f.res.HandleNotification([]byte(`{"event_id":1,"kind":"namespace","namespace_id":"` + a.String() +
		`","tenant_id":"acme","shard_id":0,"epoch":2,"state":"active"}`))
	if e, _ := f.res.Resolve(ctx, a); e.Epoch != 2 {
		t.Errorf("the notified entry is reloaded, epoch = %d", e.Epoch)
	}
	if s := f.res.Stats(); s.Entries != 2 || s.Invalidations != 1 {
		t.Errorf("stats = %+v; want 2 entries and exactly 1 invalidation", s)
	}
}

func TestResolver_TenantAndShardNotifications(t *testing.T) {
	f := newFixture(t, 4, catalog.ResolverOptions{})
	ctx := context.Background()
	for _, ns := range f.nss {
		_, _ = f.res.Resolve(ctx, ns)
	}
	f.res.HandleNotification([]byte(`{"kind":"shard","shard_id":1}`))
	if got := f.res.Stats().Entries; got != 2 {
		t.Errorf("a shard event drops the entries of that shard: %d left, want 2", got)
	}
	f.res.HandleNotification([]byte(`{"kind":"tenant","tenant_id":"acme","state":"active"}`))
	if got := f.res.Stats().Entries; got != 0 {
		t.Errorf("a tenant event drops every entry of the tenant: %d left", got)
	}
}

func TestResolver_UnreadablePayloadFlushesEverything(t *testing.T) {
	f := newFixture(t, 2, catalog.ResolverOptions{})
	ctx := context.Background()
	for _, ns := range f.nss {
		_, _ = f.res.Resolve(ctx, ns)
	}
	for _, bad := range []string{`not json`, `{"kind":"namespace"}`, `{"kind":"wat","namespace_id":"x"}`, ``,
		`{"kind":"namespace","namespace_id":"00000000-0000-0000-0000-000000000000"}`} {
		for _, ns := range f.nss {
			_, _ = f.res.Resolve(ctx, ns)
		}
		f.res.HandleNotification([]byte(bad))
		if got := f.res.Stats().Entries; got != 0 {
			t.Errorf("payload %q left %d entries; an unreadable change must flush", bad, got)
		}
	}
	if f.res.Stats().BadPayloads != 5 {
		t.Errorf("bad payload counter = %d", f.res.Stats().BadPayloads)
	}
}

func TestResolver_LRUEvictsTheLeastRecentlyUsed(t *testing.T) {
	f := newFixture(t, 4, catalog.ResolverOptions{MaxEntries: 3})
	ctx := context.Background()
	for _, ns := range f.nss[:3] {
		_, _ = f.res.Resolve(ctx, ns)
	}
	_, _ = f.res.Resolve(ctx, f.nss[0]) // 0 is now the most recently used; 1 the least
	_, _ = f.res.Resolve(ctx, f.nss[3]) // evicts 1
	f.cat.SetDown(true)
	if _, err := f.res.Resolve(ctx, f.nss[1]); !errs.Is(err, errs.KindUnavailable) {
		t.Errorf("namespace 1 should have been evicted (a miss while down): %v", err)
	}
	for _, i := range []int{0, 2, 3} {
		if _, err := f.res.Resolve(ctx, f.nss[i]); err != nil {
			t.Errorf("namespace %d should still be cached: %v", i, err)
		}
	}
	if f.res.Stats().Entries != 3 {
		t.Errorf("entries = %d", f.res.Stats().Entries)
	}
}

// slowSource counts loads and blocks them until released.
type slowSource struct {
	catalog.Namespaces
	loads   atomic.Int32
	release chan struct{}
}

func (s *slowSource) Resolve(ctx context.Context, ns id.NamespaceID) (*catalog.Entry, error) {
	s.loads.Add(1)
	<-s.release
	return s.Namespaces.Resolve(ctx, ns)
}

func TestResolver_SingleFlight(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	src := &slowSource{Namespaces: f.cat, release: make(chan struct{})}
	r := catalog.NewResolver(src, nil, catalog.ResolverOptions{Clock: f.clk.Now})
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Resolve(context.Background(), f.nss[0]); err != nil {
				t.Error(err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(src.release)
	wg.Wait()
	if n := src.loads.Load(); n != 1 {
		t.Errorf("%d catalog loads for 20 concurrent resolves of one namespace; want 1", n)
	}
}

func TestResolver_InvalidateDuringLoadKeepsTheOldAnswerOutOfTheCache(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	src := &slowSource{Namespaces: f.cat, release: make(chan struct{})}
	r := catalog.NewResolver(src, nil, catalog.ResolverOptions{Clock: f.clk.Now})
	done := make(chan *catalog.Entry)
	go func() { e, _ := r.Resolve(context.Background(), f.nss[0]); done <- e }()
	for src.loads.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	r.Invalidate(f.nss[0]) // a notification arrives while the load reads the old row
	close(src.release)
	if e := <-done; e == nil || e.Epoch != 1 {
		t.Fatalf("the caller still gets its answer: %v", e)
	}
	if r.Stats().Entries != 0 {
		t.Error("an answer that raced an invalidation must not be cached")
	}
}

func TestResolver_ResolveFreshBypassesTheCacheAndNeverServesStale(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	ctx := context.Background()
	_, _ = f.res.Resolve(ctx, f.nss[0])
	f.cat.DropNotifications(true)
	if _, err := f.cat.BumpEpoch(ctx, f.nss[0], 1, "failover"); err != nil {
		t.Fatal(err)
	}
	if e, _ := f.res.Resolve(ctx, f.nss[0]); e.Epoch != 1 {
		t.Fatal("setup: the cache should be stale")
	}
	if e, err := f.res.ResolveFresh(ctx, f.nss[0]); err != nil || e.Epoch != 2 {
		t.Fatalf("ResolveFresh = %v, %v", e, err)
	}
	if e, _ := f.res.Resolve(ctx, f.nss[0]); e.Epoch != 2 {
		t.Error("ResolveFresh refreshes the cache")
	}
	f.cat.SetDown(true)
	if _, err := f.res.ResolveFresh(ctx, f.nss[0]); !errs.Is(err, errs.KindUnavailable) {
		t.Errorf("ResolveFresh while down = %v; want UNAVAILABLE, not the cached entry", err)
	}
	// ...and it did not evict the entry for everyone else (F8): Resolve keeps serving it.
	f.clk.Advance(5 * time.Minute)
	if e, err := f.res.Resolve(ctx, f.nss[0]); err != nil || e.Epoch != 2 {
		t.Errorf("Resolve after a failed ResolveFresh = %+v, %v; want the cached entry", e, err)
	}
}

func TestResolver_RunFlushesOnEveryConnectAndReconnects(t *testing.T) {
	f := newFixture(t, 2, catalog.ResolverOptions{ReconnectMin: 5 * time.Millisecond,
		ReconnectMax: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.res.Run(ctx) }()
	waitFor(t, func() bool { return f.res.Stats().Flushes >= 1 }) // the first subscription flushes an empty cache
	_, _ = f.res.Resolve(ctx, f.nss[0])

	// A notification reaches the resolver through the listener.
	if _, err := f.cat.BumpEpoch(ctx, f.nss[0], 1, "move"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { e, _ := f.res.Resolve(ctx, f.nss[0]); return e.Epoch == 2 })

	// A lost connection: the change made while disconnected is not announced to anyone; reconnecting flushes.
	f.cat.DropNotifications(true)
	f.cat.DisconnectListeners()
	if _, err := f.cat.BumpEpoch(ctx, f.nss[0], 2, "move"); err != nil {
		t.Fatal(err)
	}
	flushes := f.res.Stats().Flushes
	waitFor(t, func() bool { return f.res.Stats().Flushes > flushes })
	if e, _ := f.res.Resolve(ctx, f.nss[0]); e.Epoch != 3 {
		t.Errorf("after the reconnect flush the epoch is %d; want 3", e.Epoch)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run returned %v on a clean stop", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not reached in 5 s")
}

func TestParseNotification(t *testing.T) {
	ns := id.NewNamespaceID()
	n, err := catalog.ParseNotification([]byte(`{"event_id": 7, "kind": "namespace", "namespace_id": "` + ns.String() +
		`", "tenant_id": "acme", "shard_id": 3, "epoch": 9, "state": "frozen"}`))
	if err != nil || n.Namespace != ns || n.Tenant != "acme" || *n.Shard != 3 || *n.Epoch != 9 || n.State != "frozen" ||
		n.EventID != 7 {
		t.Fatalf("parse = %+v, %v", n, err)
	}
	for _, bad := range []string{`{"kind":"move"}`, `{"kind":"tenant","tenant_id":"UPPER"}`, `{"kind":"shard"}`, `[]`} {
		if _, err := catalog.ParseNotification([]byte(bad)); err == nil {
			t.Errorf("%s must be rejected", bad)
		}
	}
}

func TestStaticResolver(t *testing.T) {
	ns := id.NewNamespaceID()
	truth := &catalog.Entry{Namespace: ns, Tenant: "acme", Shard: 1, Epoch: 2, State: catalog.StateActive}
	stale := &catalog.Entry{Namespace: ns, Tenant: "acme", Shard: 0, Epoch: 1, State: catalog.StateActive}
	s := catalog.NewStaticResolver(truth)
	s.Stale(stale)
	ctx := context.Background()
	if e, _ := s.Resolve(ctx, ns); e != stale {
		t.Error("Resolve serves the injected stale entry")
	}
	if e, _ := s.ResolveFresh(ctx, ns); e != truth {
		t.Error("ResolveFresh returns the truth")
	}
	s.Stale(stale)
	s.Invalidate(ns)
	if e, _ := s.Resolve(ctx, ns); e != truth {
		t.Error("Invalidate repairs the served layer")
	}
	s.Fail(catalog.ErrUnavailable())
	if _, err := s.Resolve(ctx, ns); !errs.Is(err, errs.KindUnavailable) {
		t.Error("Fail simulates an outage")
	}
	s.Fail(nil)
	if _, err := s.Resolve(ctx, id.NewNamespaceID()); !errs.Is(err, errs.KindNotFound) {
		t.Error("unknown namespace is NOT_FOUND")
	}
}

func TestMemoryCatalog_RestoreToRevertsACommittedChange(t *testing.T) {
	f := newFixture(t, 1, catalog.ResolverOptions{})
	ctx := context.Background()
	snap := f.cat.Snapshot()
	if _, err := f.cat.BumpEpoch(ctx, f.nss[0], 1, "failover"); err != nil {
		t.Fatal(err)
	}
	if err := f.cat.SetState(ctx, f.nss[0], catalog.StateActive, catalog.StateDeleting); err != nil {
		t.Fatal(err)
	}
	f.cat.RestoreTo(snap) // a lossy catalog restore reverts the epoch bump and the deleting marker (N163)
	e, _ := f.cat.Resolve(ctx, f.nss[0])
	if e.Epoch != 1 || e.State != catalog.StateActive {
		t.Errorf("after RestoreTo: epoch %d state %s", e.Epoch, e.State)
	}
}

func TestTenantStates_CacheStaleAndInvalidate(t *testing.T) {
	cat := catalog.NewMemoryCatalog(nil)
	cat.AddTenant(catalog.TenantEntry{Tenant: "acme"})
	clk := newClock()
	ts := catalog.NewTenantStates(cat, 0, clk.Now)
	ctx := context.Background()
	if st, err := ts.State(ctx, "acme"); err != nil || st != "active" {
		t.Fatalf("state = %q, %v", st, err)
	}
	cat.SetTenantState("acme", "deleting")
	if st, _ := ts.State(ctx, "acme"); st != "active" {
		t.Error("inside the TTL the cached state is served")
	}
	ts.Invalidate("acme")
	if st, _ := ts.State(ctx, "acme"); st != "deleting" {
		t.Error("an invalidation reaches the next read")
	}
	cat.SetDown(true)
	clk.Advance(time.Hour)
	if st, err := ts.State(ctx, "acme"); err != nil || st != "deleting" {
		t.Errorf("a down catalog serves the last known state: %q, %v", st, err)
	}
	if _, err := ts.State(ctx, "unknown"); !errs.Is(err, errs.KindUnavailable) {
		t.Errorf("an unread tenant while down = %v; want UNAVAILABLE", err)
	}
	cat.SetDown(false)
	if _, err := ts.State(ctx, "unknown"); !errs.Is(err, errs.KindNotFound) {
		t.Errorf("an unknown tenant = %v; want NOT_FOUND", err)
	}
}

func TestResolver_OnTenantFollowsNotificationsAndFlushes(t *testing.T) {
	var got []id.TenantID
	f := newFixture(t, 1, catalog.ResolverOptions{OnTenant: func(t id.TenantID) { got = append(got, t) }})
	f.res.HandleNotification([]byte(`{"kind":"tenant","tenant_id":"acme","state":"deleting"}`))
	f.res.Flush()
	if len(got) != 2 || got[0] != "acme" || got[1] != "" {
		t.Errorf("OnTenant calls = %q; want [acme, \"\" (everything)]", got)
	}
}
