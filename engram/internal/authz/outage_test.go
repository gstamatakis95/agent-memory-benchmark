package authz_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/quota"
)

// TestAuthz_OutageIsNoExistenceOracle is review F9: while the catalog is down the Resolver answers for the entries it
// holds, and a cached entry of ANOTHER tenant must answer exactly like a miss (UNAVAILABLE), not NOT_FOUND, or the
// outage tells a caller which ids exist in other tenants.
func TestAuthz_OutageIsNoExistenceOracle(t *testing.T) {
	cat := catalog.NewMemoryCatalog(nil)
	cat.AddTenant(catalog.TenantEntry{Tenant: acme})
	cat.AddTenant(catalog.TenantEntry{Tenant: globex})
	cat.AddShard(0, "")
	ctx := context.Background()
	foreign, err := cat.Create(ctx, catalog.CreateParams{Tenant: globex, Name: "theirs", Shard: 0})
	if err != nil {
		t.Fatal(err)
	}
	own, err := cat.Create(ctx, catalog.CreateParams{Tenant: acme, Name: "mine", Shard: 0})
	if err != nil {
		t.Fatal(err)
	}
	clk := &fakeNow{t: now0}
	res := catalog.NewResolver(cat, nil, catalog.ResolverOptions{Clock: clk.Now, LoadTimeout: time.Second})
	for _, ns := range []id.NamespaceID{foreign.Namespace, own.Namespace} { // the cache holds both tenants' entries
		if _, err := res.Resolve(ctx, ns); err != nil {
			t.Fatal(err)
		}
	}
	c := claims(acme, authz.ScopeMemoryRead)
	c.AllNamespaces = true
	ic := authz.NewInterceptor(mapVerifier{claims: map[string]*authz.Claims{"t": c}}, res, nil, authz.DefaultPolicy())
	call := func(ns id.NamespaceID) error {
		md := metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer t"))
		_, err := ic.Unary()(md, getMemory(acme, ns), &grpc.UnaryServerInfo{FullMethod: mGetMemory},
			func(context.Context, any) (any, error) { return nil, nil })
		return err
	}

	if status.Code(call(foreign.Namespace)) != codes.NotFound {
		t.Fatal("setup: with the catalog up, another tenant's namespace is NOT_FOUND")
	}
	cat.SetDown(true)
	clk.Advance(2 * time.Minute)
	_ = call(own.Namespace) // starts the background refresh that finds the catalog down
	res.WaitRefreshes()
	if !res.Down() {
		t.Fatal("setup: the Resolver should know the catalog is down")
	}
	foreignErr, missErr := call(foreign.Namespace), call(id.NewNamespaceID())
	if status.Code(foreignErr) != codes.Unavailable || status.Code(missErr) != codes.Unavailable {
		t.Fatalf("foreign cached = %v, never seen = %v; both must be UNAVAILABLE", foreignErr, missErr)
	}
	if foreignErr.Error() != missErr.Error() {
		t.Errorf("the two answers differ: %q vs %q", foreignErr, missErr)
	}
	if err := call(own.Namespace); err != nil {
		t.Errorf("the caller's own cached namespace is still served during the outage: %v", err)
	}
}

// recordingLimiter remembers the keys it was asked about.
type recordingLimiter struct {
	mu   sync.Mutex
	keys []quota.Key
}

func (r *recordingLimiter) Allow(_ context.Context, k quota.Key, _ int64) (quota.Decision, error) {
	r.mu.Lock()
	r.keys = append(r.keys, k)
	r.mu.Unlock()
	return quota.Decision{Allowed: true}, nil
}

// TestAuthz_RateBucketIsKeyedByTenantAndNamespace is review F18 (D13): a move or a restore changes shard and epoch and
// must not reset the namespace's bucket, nor may a stale cache entry key it differently.
func TestAuthz_RateBucketIsKeyedByTenantAndNamespace(t *testing.T) {
	res := fixtureResolver()
	lim := &recordingLimiter{}
	c := claims(acme, authz.ScopeMemoryRead)
	c.AllNamespaces = true
	ic := authz.NewInterceptor(mapVerifier{claims: map[string]*authz.Claims{"t": c}}, res, lim, authz.DefaultPolicy())
	call := func() {
		md := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer t"))
		if _, err := ic.Unary()(md, recall(acme, a1), &grpc.UnaryServerInfo{FullMethod: mRecall},
			func(context.Context, any) (any, error) { return nil, nil }); err != nil {
			t.Fatal(err)
		}
	}
	call()
	moved := entry(a1, acme, catalog.StateActive, "eng") // the namespace moved: another shard, another epoch
	moved.Shard, moved.Epoch = 9, 8
	res.Put(moved)
	call()
	if len(lim.keys) != 2 || lim.keys[0] != lim.keys[1] {
		t.Fatalf("keys = %+v; a move must not change the bucket", lim.keys)
	}
	if k := lim.keys[0]; k.Scope != (id.Scope{Tenant: acme, Namespace: a1}) || k.Bucket != quota.BucketRecall {
		t.Errorf("key = %+v; want (tenant, namespace, recalls_per_min) only", k)
	}
}

// TestJWKS_ColdStartWithTheIssuerDown is review F19: with no key set yet, an issuer that is down sees one request per
// MinRefresh, not one per token, and the callers get UNAVAILABLE in between.
func TestJWKS_ColdStartWithTheIssuerDown(t *testing.T) {
	ed := newEd(t, "k1")
	srv := newJWKS(t, ed)
	srv.down.Store(true)
	clk := &fakeNow{t: now0}
	v := verifierFor(t, srv, clk)
	tok := ed.token(t, good(nil))
	for range 50 {
		if _, err := v.Verify(context.Background(), tok); !errs.Is(err, errs.KindUnavailable) {
			t.Fatalf("err = %v; want UNAVAILABLE", err)
		}
	}
	if n := srv.fetches.Load(); n != 1 {
		t.Errorf("%d fetches for 50 tokens; want 1", n)
	}
	srv.down.Store(false)
	clk.Advance(11 * time.Second)
	if _, err := v.Verify(context.Background(), tok); err != nil {
		t.Fatalf("after MinRefresh the issuer is asked again and recovers: %v", err)
	}
}
