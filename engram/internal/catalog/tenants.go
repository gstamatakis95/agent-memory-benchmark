package catalog

import (
	"context"
	"sync"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// TenantStateReader reads the state of one tenant (active | suspended | deleting | deleted).
type TenantStateReader interface {
	TenantState(ctx context.Context, t id.TenantID) (string, error)
}

// DefaultTenantStateTTL is how long a tenant state is trusted: short, because `deleting` is a read barrier (N122), but
// the shard's own fence is the authority, so a few seconds of lag only delays the refusal.
const DefaultTenantStateTTL = 5 * time.Second

type tenantItem struct {
	state  string
	loaded time.Time
}

// TenantStates is a small read-through cache of tenant states for the authorization of tenant-level methods (GetTenant,
// UpdateTenant, DeleteTenant, CreateNamespace, ListNamespaces refuse a `deleting` tenant, N5, N122). It follows the
// Resolver's invalidation through ResolverOptions.OnTenant (Invalidate), keeps serving the last state it knows while
// the catalog is unreachable, and answers UNAVAILABLE only for a tenant it has never read.
type TenantStates struct {
	src   TenantStateReader
	ttl   time.Duration
	clock func() time.Time
	mu    sync.Mutex
	items map[id.TenantID]tenantItem
}

// NewTenantStates builds the cache; ttl 0 means DefaultTenantStateTTL, clock nil means time.Now.
func NewTenantStates(src TenantStateReader, ttl time.Duration, clock func() time.Time) *TenantStates {
	if ttl <= 0 {
		ttl = DefaultTenantStateTTL
	}
	if clock == nil {
		clock = time.Now
	}
	return &TenantStates{src: src, ttl: ttl, clock: clock, items: map[id.TenantID]tenantItem{}}
}

// State returns the state of t, NOT_FOUND{TENANT} for an unknown tenant.
func (s *TenantStates) State(ctx context.Context, t id.TenantID) (string, error) {
	now := s.clock()
	s.mu.Lock()
	it, ok := s.items[t]
	s.mu.Unlock()
	if ok && now.Sub(it.loaded) < s.ttl {
		return it.state, nil
	}
	st, err := s.src.TenantState(ctx, t)
	switch {
	case err == nil:
		s.mu.Lock()
		s.items[t] = tenantItem{st, now}
		s.mu.Unlock()
		return st, nil
	case errs.Is(err, errs.KindNotFound):
		return "", err
	case ok:
		return it.state, nil // the catalog is unreachable: the last known state
	}
	return "", err
}

// Invalidate drops the cached state of t; the empty tenant drops them all.
func (s *TenantStates) Invalidate(t id.TenantID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t == "" {
		s.items = map[id.TenantID]tenantItem{}
		return
	}
	delete(s.items, t)
}

func tenantMissing(t id.TenantID) error {
	return errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_TENANT, t)
}
