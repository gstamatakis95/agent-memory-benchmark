package catalog

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// The documented defaults of ResolverOptions (D4 as amended, PLAN.md section 2.5).
const (
	DefaultMaxEntries      = 100_000
	DefaultTTL             = 60 * time.Second
	DefaultNegativeTTL     = 5 * time.Second
	DefaultNegativeStale   = 10 * time.Minute // a negative entry is served this long while the catalog is unreachable
	DefaultLoadTimeout     = 2 * time.Second
	DefaultProbeInterval   = time.Second // while the catalog is down, at most one load attempt per interval
	DefaultReconnectMin    = time.Second // LISTEN reconnect backoff, 1 s to 30 s (section 2.5)
	DefaultReconnectMax    = 30 * time.Second
	unavailableRetry       = 2 * time.Second // RetryInfo of an UNAVAILABLE on a miss (D4)
	namespaceKindForErrors = memoryv1.ResourceKind_RESOURCE_KIND_NAMESPACE
)

// ResolverClock is the time source of the Resolver; tests inject a fake one to advance 30 minutes without waiting.
type ResolverClock func() time.Time

// ResolverStats is a snapshot of the Resolver's counters and gauges.
type ResolverStats struct {
	Entries       int
	Hits          uint64
	Misses        uint64 // loads that went to the catalog
	StaleServed   uint64 // expired entries of existing namespaces served while the catalog was unreachable
	NegativeHits  uint64
	Invalidations uint64 // entries dropped by a notification or Invalidate
	Flushes       uint64 // full flushes (reconnects, unreadable notifications)
	BadPayloads   uint64
	Down          bool
	StaleAge      time.Duration // time since the last successful catalog contact while down; 0 otherwise
}

type item struct {
	key    id.NamespaceID
	entry  *Entry // nil: a negative entry
	loaded time.Time
	elem   *list.Element
}

type call struct {
	done  chan struct{}
	entry *Entry
	err   error
	dirty bool // an Invalidate or a flush happened while the load was in flight: the result must not be cached
}

// CachedResolver is the read-through cache of PLAN.md section 2.2.3 over a catalog.Namespaces source: LRU of
// MaxEntries, TTL, a short negative TTL, single-flight loads, LISTEN-driven invalidation with a full flush on every
// reconnect, and, while the catalog is unreachable, the entries of existing namespaces served for as long as the
// outage lasts (only a miss is UNAVAILABLE). Entries it returns are shared and must be treated as immutable.
type CachedResolver struct {
	src  Namespaces
	lis  Listener
	opts ResolverOptions

	mu        sync.Mutex
	items     map[id.NamespaceID]*item
	lru       *list.List // front = most recently used
	inflight  map[id.NamespaceID]*call
	down      bool
	lastOK    time.Time // last successful catalog contact
	nextProbe time.Time

	hits, misses, stale, negHits, invalidations, flushes, bad atomic.Uint64
	bg                                                        sync.WaitGroup // background refreshes
}

var _ Resolver = (*CachedResolver)(nil)

// NewResolver builds the Resolver. A zero field of o takes its documented default (MaxEntries 100 000, TTL 60 s,
// NegativeTTL 5 s, StaleMax 0 = unbounded). lis may be nil: Run then only waits for ctx (invalidation by Invalidate
// and HandleNotification alone).
func NewResolver(src Namespaces, lis Listener, o ResolverOptions) *CachedResolver {
	if o.MaxEntries <= 0 {
		o.MaxEntries = DefaultMaxEntries
	}
	if o.TTL <= 0 {
		o.TTL = DefaultTTL
	}
	if o.NegativeTTL <= 0 {
		o.NegativeTTL = DefaultNegativeTTL
	}
	if o.NegativeStaleTTL <= 0 {
		o.NegativeStaleTTL = DefaultNegativeStale
	}
	if o.LoadTimeout <= 0 {
		o.LoadTimeout = DefaultLoadTimeout
	}
	if o.ProbeInterval <= 0 {
		o.ProbeInterval = DefaultProbeInterval
	}
	if o.ReconnectMin <= 0 {
		o.ReconnectMin = DefaultReconnectMin
	}
	if o.ReconnectMax <= 0 {
		o.ReconnectMax = DefaultReconnectMax
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return &CachedResolver{src: src, lis: lis, opts: o, items: make(map[id.NamespaceID]*item),
		lru: list.New(), inflight: make(map[id.NamespaceID]*call), lastOK: o.Clock()}
}

func notFound(ns id.NamespaceID) error { return errs.NotFound(namespaceKindForErrors, ns) }

func unavailable(cause error) error {
	e := errs.Unavailable("catalog unavailable", unavailableRetry)
	e.Cause = cause
	return e
}

// Resolve returns the entry of ns.
//
//   - A fresh cached entry never touches the catalog.
//   - An expired entry of an existing namespace is served as it is and refreshed in the background (D4: stale entries
//     of existing namespaces are served; the TTL starts a refresh, it does not make a blocking miss, and no caller ever
//     waits for the catalog on behalf of an entry it already holds). While the catalog is known to be down, at most
//     one refresh per ProbeInterval is attempted, by a goroutine of the Resolver, never by a caller.
//   - A miss (nothing cached, a negative entry past its life, an entry past StaleMax) reads the catalog; if the catalog
//     cannot be reached the answer is UNAVAILABLE{RetryInfo 2 s}, except that a negative entry younger than
//     NegativeStaleTTL is still NOT_FOUND (D4: negative entries live 10 min while the catalog is unreachable), so the
//     answer for one id does not flip at the moment the outage starts.
func (r *CachedResolver) Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error) {
	now := r.opts.Clock()
	r.mu.Lock()
	it := r.items[ns]
	var negAge time.Duration
	negative := false
	if it != nil {
		r.lru.MoveToFront(it.elem)
		age := now.Sub(it.loaded)
		if cur := it.entry; cur != nil { // read under the lock: store() replaces item.entry in place
			if age < r.opts.TTL {
				r.mu.Unlock()
				r.hits.Add(1)
				return cur, nil
			}
			if r.opts.StaleMax == 0 || age <= r.opts.StaleMax {
				down := r.down
				r.refreshLocked(ns, now)
				r.mu.Unlock()
				if down {
					r.stale.Add(1)
				} else {
					r.hits.Add(1)
				}
				return cur, nil
			}
		} else {
			negative, negAge = true, age
			limit := r.opts.NegativeTTL
			if r.down {
				limit = r.opts.NegativeStaleTTL
			}
			if age < limit {
				r.mu.Unlock()
				r.negHits.Add(1)
				return nil, notFound(ns)
			}
		}
	}
	if r.down && now.Before(r.nextProbe) { // do not hammer a catalog that is known to be down
		r.mu.Unlock()
		return r.missWhileDown(ns, negative, negAge, nil)
	}
	if r.down {
		r.nextProbe = now.Add(r.opts.ProbeInterval)
	}
	r.mu.Unlock()

	e, err := r.load(ctx, ns)
	if err != nil && !errs.Is(err, errs.KindNotFound) {
		return r.missWhileDown(ns, negative, negAge, err)
	}
	return e, err
}

// missWhileDown answers a miss that could not be served by the catalog.
func (r *CachedResolver) missWhileDown(ns id.NamespaceID, negative bool, negAge time.Duration,
	cause error) (*Entry, error) {
	if negative && negAge < r.opts.NegativeStaleTTL {
		r.negHits.Add(1)
		return nil, notFound(ns)
	}
	if cause != nil && !errs.Is(cause, errs.KindUnavailable) {
		return nil, cause
	}
	return nil, unavailable(cause)
}

// refreshLocked starts a background load of ns unless one is in flight or the catalog is down and not yet due for a
// probe. mu is held.
func (r *CachedResolver) refreshLocked(ns id.NamespaceID, now time.Time) {
	if _, ok := r.inflight[ns]; ok {
		return
	}
	if r.down {
		if now.Before(r.nextProbe) {
			return
		}
		r.nextProbe = now.Add(r.opts.ProbeInterval)
	}
	c := &call{done: make(chan struct{})}
	r.inflight[ns] = c
	r.bg.Add(1)
	go func() {
		defer r.bg.Done()
		r.run(c, ns)
	}()
}

// WaitRefreshes blocks until every background refresh started so far has finished (tests).
func (r *CachedResolver) WaitRefreshes() { r.bg.Wait() }

// Down reports whether the last read of the catalog failed (and no read or reconnect has succeeded since). The authz
// interceptor uses it to keep a cached entry of another tenant from answering differently from a miss during an outage.
func (r *CachedResolver) Down() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.down
}

// ResolveFresh bypasses the cache (after WrongShardOrEpoch): the catalog is read first, and only its answer replaces
// the cached entry. A failure is the catalog's error to this caller alone; the cached entry stays for everyone else (it
// is what Resolve keeps serving during the outage), and this caller never gets a stale one.
func (r *CachedResolver) ResolveFresh(ctx context.Context, ns id.NamespaceID) (*Entry, error) {
	r.mu.Lock()
	if c, ok := r.inflight[ns]; ok { // its answer may predate the change the caller just learned of
		c.dirty = true
		delete(r.inflight, ns)
	}
	r.mu.Unlock()
	e, err := r.load(ctx, ns)
	if err != nil && !errs.Is(err, errs.KindNotFound) && !errs.Is(err, errs.KindUnavailable) {
		return nil, err
	}
	if err != nil && !errs.Is(err, errs.KindNotFound) {
		return nil, unavailable(err)
	}
	return e, err
}

// load reads ns from the catalog through a single-flight call and caches the outcome unless an invalidation raced it.
func (r *CachedResolver) load(ctx context.Context, ns id.NamespaceID) (*Entry, error) {
	r.mu.Lock()
	c, joined := r.inflight[ns]
	if !joined {
		c = &call{done: make(chan struct{})}
		r.inflight[ns] = c
	}
	r.mu.Unlock()
	if !joined {
		go r.run(c, ns)
	}
	select {
	case <-c.done:
		return c.entry, c.err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, errs.DeadlineExceeded("catalog resolve", ctx.Err())
		}
		return nil, ctx.Err()
	}
}

func (r *CachedResolver) run(c *call, ns id.NamespaceID) {
	// The load is detached from the leader's context: a joined caller must not inherit another caller's cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), r.opts.LoadTimeout)
	defer cancel()
	e, err := r.src.Resolve(ctx, ns)
	now := r.opts.Clock()

	r.misses.Add(1)
	r.mu.Lock()
	switch {
	case err == nil:
		r.down = false
		r.lastOK = now
		if !c.dirty {
			r.store(ns, e, now)
		}
	case errs.Is(err, errs.KindNotFound):
		r.down = false // the catalog answered
		r.lastOK = now
		err = notFound(ns)
		if !c.dirty {
			r.store(ns, nil, now)
		}
	default:
		if !r.down {
			r.down = true
			r.nextProbe = now.Add(r.opts.ProbeInterval)
		}
	}
	if r.inflight[ns] == c {
		delete(r.inflight, ns)
	}
	c.entry, c.err = e, err
	r.mu.Unlock()
	close(c.done)
}

// store inserts or replaces the item of ns and evicts the least recently used entry above MaxEntries. mu is held.
func (r *CachedResolver) store(ns id.NamespaceID, e *Entry, now time.Time) {
	if it, ok := r.items[ns]; ok {
		it.entry, it.loaded = e, now
		r.lru.MoveToFront(it.elem)
		return
	}
	it := &item{key: ns, entry: e, loaded: now}
	it.elem = r.lru.PushFront(it)
	r.items[ns] = it
	for r.lru.Len() > r.opts.MaxEntries {
		oldest := r.lru.Back()
		r.drop(oldest.Value.(*item))
	}
}

// drop removes it from the cache. mu is held.
func (r *CachedResolver) drop(it *item) {
	r.lru.Remove(it.elem)
	delete(r.items, it.key)
}

// Invalidate drops the entry of ns, positive or negative, and keeps a load that is in flight from caching its (possibly
// older) answer.
func (r *CachedResolver) Invalidate(ns id.NamespaceID) {
	r.mu.Lock()
	if it, ok := r.items[ns]; ok {
		r.drop(it)
		r.invalidations.Add(1)
	}
	if c, ok := r.inflight[ns]; ok {
		c.dirty = true
		delete(r.inflight, ns)
	}
	r.mu.Unlock()
}

// InvalidateTenant drops every cached namespace of t (a tenant config or state change reaches every namespace).
func (r *CachedResolver) InvalidateTenant(t id.TenantID) {
	if r.opts.OnTenant != nil {
		r.opts.OnTenant(t)
	}
	r.dropWhere(func(it *item) bool { return it.entry != nil && it.entry.Tenant == t })
}

// InvalidateShard drops every cached namespace placed on s.
func (r *CachedResolver) InvalidateShard(s id.ShardID) {
	r.dropWhere(func(it *item) bool { return it.entry != nil && it.entry.Shard == s })
}

func (r *CachedResolver) dropWhere(pred func(*item) bool) {
	r.mu.Lock()
	for _, it := range r.items {
		if pred(it) {
			r.drop(it)
			r.invalidations.Add(1)
		}
	}
	for ns, c := range r.inflight { // an in-flight load may carry a pre-change answer
		c.dirty = true
		delete(r.inflight, ns)
	}
	r.mu.Unlock()
}

// Flush drops every entry. Run calls it each time the LISTEN subscription is (re)established, which also proves the
// catalog reachable, so the down state ends with it: the next resolve loads instead of failing fast.
func (r *CachedResolver) Flush() {
	r.mu.Lock()
	r.down = false
	r.lastOK = r.opts.Clock()
	r.items = make(map[id.NamespaceID]*item)
	r.lru.Init()
	for ns, c := range r.inflight {
		c.dirty = true
		delete(r.inflight, ns)
	}
	r.mu.Unlock()
	r.flushes.Add(1)
	if r.opts.OnTenant != nil {
		r.opts.OnTenant("") // every tenant
	}
}

// HandleNotification applies one catalog_changes payload: it drops the entry the payload names (a tenant event drops
// the tenant's entries, a shard event the shard's). A payload that cannot be read flushes the whole cache.
func (r *CachedResolver) HandleNotification(payload []byte) {
	n, err := ParseNotification(payload)
	if err != nil {
		r.bad.Add(1)
		r.Flush()
		return
	}
	switch n.Kind {
	case KindNamespace, KindMove:
		r.Invalidate(n.Namespace)
	case KindTenant:
		r.InvalidateTenant(n.Tenant)
	case KindShard:
		r.InvalidateShard(*n.Shard)
	}
}

// Run is the LISTEN loop. It returns nil when ctx ends. Each time the subscription is established, the cache is
// flushed once; when the connection is lost it reconnects with a backoff of 1 s doubling to 30 s. While it is
// reconnecting, cached entries keep being served (they were valid when loaded and the shard fence is the authority).
func (r *CachedResolver) Run(ctx context.Context) error {
	if r.lis == nil {
		<-ctx.Done()
		return nil
	}
	backoff := r.opts.ReconnectMin
	for {
		ready := func() {
			r.Flush()
			backoff = r.opts.ReconnectMin
		}
		err := r.lis.Listen(ctx, ready, r.HandleNotification)
		if ctx.Err() != nil {
			return nil
		}
		_ = err // the loss is reported through the reconnect, not as a failure of Run
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, r.opts.ReconnectMax)
	}
}

// StaleAge is the age gauge of D4: how long the catalog has been unreachable (the time since the last successful
// contact, or since the Resolver was built if there was none) while the Resolver serves from its cache, 0 when the
// catalog is reachable.
func (r *CachedResolver) StaleAge() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.down {
		return 0
	}
	return r.opts.Clock().Sub(r.lastOK)
}

// Stats returns the counters and gauges.
func (r *CachedResolver) Stats() ResolverStats {
	r.mu.Lock()
	n, down := len(r.items), r.down
	r.mu.Unlock()
	return ResolverStats{Entries: n, Hits: r.hits.Load(), Misses: r.misses.Load(), StaleServed: r.stale.Load(),
		NegativeHits: r.negHits.Load(), Invalidations: r.invalidations.Load(), Flushes: r.flushes.Load(),
		BadPayloads: r.bad.Load(), Down: down, StaleAge: r.StaleAge()}
}
