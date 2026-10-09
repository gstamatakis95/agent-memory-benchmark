package catalog_test

import (
	"context"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// TestResolver_InvalidationModelProp is the invalidation model of PLAN.md section 8.2 (internal/catalog row), over the
// operations resolve, notify(ns) (a catalog change announced to the listener), expire (time passes), catalog_down and
// reconnect (the catalog is reachable again and the LISTEN reconnect flushes the cache). Properties:
//
//	P1  after notify(ns) while the catalog is reachable, the next resolve(ns) returns the new epoch;
//	P2  while the catalog is unreachable, an entry the resolver holds is served for as long as the outage lasts, and
//	    only a miss is UNAVAILABLE;
//	P3  a negative entry is never served longer than 5 s reachable, or 10 min unreachable;
//	P4  an answer never goes back in time (epochs are monotone per namespace) and is never invented;
//	P5  after a reconnect, the next resolve of any namespace returns the catalog's current value.
func TestResolver_InvalidationModelProp(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		clk := newClock()
		cat := catalog.NewMemoryCatalog(nil)
		cat.AddTenant(catalog.TenantEntry{Tenant: "acme"})
		cat.AddShard(0, "")
		res := catalog.NewResolver(cat, cat, catalog.ResolverOptions{Clock: clk.Now})
		ctx := context.Background()

		const n = 4
		var nss [n]id.NamespaceID
		for i := range nss {
			nss[i] = id.NewNamespaceID()
		}
		type model struct {
			exists   bool
			truth    id.Epoch  // catalog epoch
			seen     id.Epoch  // highest epoch ever returned
			cached   bool      // the resolver holds a positive entry that no flush or notification has dropped
			negAt    time.Time // when a NotFound was cached; zero when none is
			mustLoad bool      // P1/P5: the next resolve must reflect the catalog
		}
		var m [n]model
		up := true

		// admin runs a catalog change as an operator on the catalog's side of the partition: the resolver cannot reach
		// it (up == false), the catalog itself keeps working.
		admin := func(f func()) {
			cat.SetDown(false)
			f()
			cat.SetDown(!up)
		}
		create := func(i int) {
			if _, err := cat.Create(ctx, catalog.CreateParams{Namespace: nss[i], Tenant: "acme",
				Name: "n" + itoa(i), Shard: 0}); err != nil {
				rt.Fatalf("create: %v", err)
			}
			m[i].exists, m[i].truth = true, 1
		}

		rt.Repeat(map[string]func(*rapid.T){
			"resolve": func(rt *rapid.T) {
				i := rapid.IntRange(0, n-1).Draw(rt, "ns")
				e, err := res.Resolve(ctx, nss[i])
				res.WaitRefreshes() // background refreshes finish before the next operation, to keep the model exact
				now := clk.Now()
				x := &m[i]
				switch {
				case err == nil:
					if !x.exists {
						rt.Fatalf("ns %d does not exist but resolved: %+v", i, e)
					}
					if e.Epoch < x.seen {
						rt.Fatalf("P4: ns %d went back in time: %d after %d", i, e.Epoch, x.seen)
					}
					if e.Epoch > x.truth {
						rt.Fatalf("P4: ns %d epoch %d was never published (truth %d)", i, e.Epoch, x.truth)
					}
					if x.mustLoad && e.Epoch != x.truth {
						rt.Fatalf("P1/P5: ns %d resolved epoch %d after a notification/reconnect; truth is %d", i,
							e.Epoch, x.truth)
					}
					if x.mustLoad && !up {
						rt.Fatalf("ns %d: a dropped entry was served while the catalog is down", i)
					}
					x.seen, x.cached = e.Epoch, true
					if e.Epoch == x.truth {
						x.mustLoad = false
					}
					x.negAt = time.Time{}
				case errs.Is(err, errs.KindNotFound):
					if x.exists && up && (x.negAt.IsZero() || now.Sub(x.negAt) >= 5*time.Second) {
						rt.Fatalf("P3: ns %d exists and the catalog is reachable but NOT_FOUND was served (neg age %v)",
							i, now.Sub(x.negAt))
					}
					if !up && !x.negAt.IsZero() && now.Sub(x.negAt) > 10*time.Minute {
						rt.Fatalf("P3: a negative entry served after %v unreachable", now.Sub(x.negAt))
					}
					if x.negAt.IsZero() {
						x.negAt = now
					}
					x.cached = false
				case errs.Is(err, errs.KindUnavailable):
					if up {
						rt.Fatalf("UNAVAILABLE while the catalog is reachable: %v", err)
					}
					if x.cached {
						rt.Fatalf("P2: ns %d is cached and the catalog is down, but the resolve failed", i)
					}
					x.negAt = time.Time{}
				default:
					rt.Fatalf("unexpected error %v", err)
				}
			},
			"notify": func(rt *rapid.T) {
				i := rapid.IntRange(0, n-1).Draw(rt, "ns")
				x := &m[i]
				if !x.exists {
					admin(func() { create(i) })
				} else {
					admin(func() {
						if _, err := cat.BumpEpoch(ctx, nss[i], x.truth, "failover"); err != nil {
							rt.Fatalf("bump: %v", err)
						}
					})
					x.truth++
				}
				if up { // delivered: the listener drops the entry before the next resolve (the resolver is synchronous)
					waitNotified(res, cat, nss[i])
					x.mustLoad, x.cached, x.negAt = true, false, time.Time{}
				}
			},
			"expire": func(rt *rapid.T) {
				clk.Advance(time.Duration(rapid.Int64Range(1, int64(3*time.Minute)).Draw(rt, "d")))
			},
			"catalog_down": func(rt *rapid.T) { cat.SetDown(true); up = false },
			"reconnect": func(rt *rapid.T) {
				cat.SetDown(false)
				up = true
				res.Flush()
				for i := range m {
					m[i].mustLoad, m[i].cached, m[i].negAt = true, false, time.Time{}
				}
			},
		})
	})
}

// waitNotified delivers the namespace event to res synchronously (the property test does not run the LISTEN goroutine,
// so that every step is deterministic; TestResolver_RunFlushesOnEveryConnectAndReconnects covers the goroutine).
func waitNotified(res *catalog.CachedResolver, _ *catalog.MemoryCatalog, ns id.NamespaceID) {
	res.HandleNotification([]byte(`{"kind":"namespace","namespace_id":"` + ns.String() + `"}`))
}
