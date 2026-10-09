package catalog

import (
	"context"
	"sync"

	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// StaticResolver is the Resolver double (PLAN.md section 8.1): a fixed table of entries and no catalog. It exists for
// the tests of everything that consumes a Resolver (authz, the router, the API). The table has two layers, so a test
// can reproduce a stale cache: Put sets the truth, Stale sets what Resolve returns instead until the next Invalidate
// or ResolveFresh.
type StaticResolver struct {
	mu     sync.Mutex
	truth  map[id.NamespaceID]*Entry
	served map[id.NamespaceID]*Entry
	err    error // returned by every call while set (a down catalog)
	calls  int
}

var _ Resolver = (*StaticResolver)(nil)

// NewStaticResolver returns a table holding entries.
func NewStaticResolver(entries ...*Entry) *StaticResolver {
	s := &StaticResolver{truth: map[id.NamespaceID]*Entry{}, served: map[id.NamespaceID]*Entry{}}
	for _, e := range entries {
		s.Put(e)
	}
	return s
}

// Put sets the entry of e.Namespace in both layers.
func (s *StaticResolver) Put(e *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truth[e.Namespace] = e
	s.served[e.Namespace] = e
}

// Stale makes Resolve return e (a stale cache entry) while the truth stays what Put set.
func (s *StaticResolver) Stale(e *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.served[e.Namespace] = e
}

// Fail makes every call return err until Fail(nil); Fail(errs.Unavailable(...)) simulates an outage with no cache.
func (s *StaticResolver) Fail(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

// Calls returns the number of Resolve and ResolveFresh calls so far.
func (s *StaticResolver) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Resolve returns the served layer.
func (s *StaticResolver) Resolve(_ context.Context, ns id.NamespaceID) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	if e, ok := s.served[ns]; ok {
		return e, nil
	}
	return nil, nsNotFound(ns)
}

// ResolveFresh returns the truth and makes it the served layer.
func (s *StaticResolver) ResolveFresh(_ context.Context, ns id.NamespaceID) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	e, ok := s.truth[ns]
	if !ok {
		delete(s.served, ns)
		return nil, nsNotFound(ns)
	}
	s.served[ns] = e
	return e, nil
}

// Invalidate makes the served layer the truth for ns.
func (s *StaticResolver) Invalidate(ns id.NamespaceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.truth[ns]; ok {
		s.served[ns] = e
	} else {
		delete(s.served, ns)
	}
}

// Run blocks until ctx ends.
func (s *StaticResolver) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// ErrUnavailable is the error a test passes to StaticResolver.Fail for a catalog outage.
func ErrUnavailable() error { return errs.Unavailable("catalog unavailable", unavailableRetry) }
