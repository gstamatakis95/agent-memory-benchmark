package catalog_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/id"
)

// genSource answers every namespace with a synthetic entry; it makes a 100 k-entry cache cheap to fill.
type genSource struct{ catalog.Namespaces }

func (genSource) Resolve(_ context.Context, ns id.NamespaceID) (*catalog.Entry, error) {
	return &catalog.Entry{Namespace: ns, Tenant: "acme", Name: "n", Shard: 3, Epoch: 7, State: catalog.StateActive,
		EmbeddingModel: catalog.DefaultEmbeddingModel, EmbeddingDims: catalog.DefaultEmbeddingDims}, nil
}

func filled(tb testing.TB, n int) (*catalog.CachedResolver, []id.NamespaceID) {
	tb.Helper()
	r := catalog.NewResolver(genSource{}, nil, catalog.ResolverOptions{})
	nss := make([]id.NamespaceID, n)
	for i := range nss {
		nss[i] = id.NewNamespaceID()
		if _, err := r.Resolve(context.Background(), nss[i]); err != nil {
			tb.Fatal(err)
		}
	}
	return r, nss
}

// TestResolver_HitLatency is the T0 half of the exit criterion "resolve p99 < 2 ms on hit": 8 goroutines resolve cached
// namespaces of a full 100 k-entry cache and every call is timed. The bound is two orders of magnitude above what the
// code does (microseconds), so a loaded CI machine does not flake it, but a hit that waits on a lock held across a
// catalog read, or allocates per call, would fail it.
func TestResolver_HitLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("latency assertion: runs without -short (make test-prop, go test ./...), not in the T0 pass")
	}
	r, nss := filled(t, catalog.DefaultMaxEntries)
	const workers, perWorker = 8, 10_000
	lat := make([][]time.Duration, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			lat[w] = make([]time.Duration, perWorker)
			for i := range perWorker {
				ns := nss[(w*7919+i*104729)%len(nss)]
				t0 := time.Now()
				if _, err := r.Resolve(ctx, ns); err != nil {
					t.Error(err)
					return
				}
				lat[w][i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p50, p99, p999 := all[len(all)/2], all[len(all)*99/100], all[len(all)*999/1000]
	t.Logf("resolve hit over %d calls on %d workers, 100k entries: p50=%v p99=%v p99.9=%v max=%v", len(all), workers,
		p50, p99, p999, all[len(all)-1])
	if p99 >= 2*time.Millisecond {
		t.Errorf("resolve hit p99 = %v; the exit criterion is < 2 ms", p99)
	}
	if s := r.Stats(); s.Misses != uint64(len(nss)) {
		t.Errorf("hits went to the catalog: %d loads for %d namespaces", s.Misses, len(nss))
	}
}

// BenchmarkResolveHit is the benchmark half: the cost of one cached resolve, single goroutine and parallel.
func BenchmarkResolveHit(b *testing.B) {
	r, nss := filled(b, 10_000)
	ctx := context.Background()
	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = r.Resolve(ctx, nss[i%len(nss)])
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				_, _ = r.Resolve(ctx, nss[i%len(nss)])
				i++
			}
		})
	})
}
