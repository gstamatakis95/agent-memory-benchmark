package catalog

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// memShard is the placement state of one shard in the MemoryCatalog.
type memShard struct {
	Shard
	FactsEstimate int64
	SoftCapFacts  int64
	MaxNamespaces int
	Dedicated     id.TenantID
}

// MemoryCatalog is the in-memory catalog double (PLAN.md section 8.1): the Namespaces CAS semantics of the Postgres
// catalog without a database, plus the knobs a test needs: SetDown (every call fails UNAVAILABLE), DropNotifications
// (changes are made but not announced), Snapshot and RestoreTo (a lossy catalog restore, N163), and Notify. It is also
// a Listener, so a CachedResolver can run against it. The T3 twin (catalog_twin_integration_test.go) holds it to the
// Postgres catalog on random operation sequences.
type MemoryCatalog struct {
	mu       sync.Mutex
	tenants  map[id.TenantID]*TenantEntry
	shards   map[id.ShardID]*memShard
	nss      map[id.NamespaceID]*Entry
	creating map[id.NamespaceID]time.Time
	down     bool
	drop     bool
	subs     map[int]chan []byte
	nextSub  int
	boot     ShardBootstrapper
	events   int64
}

var (
	_ Namespaces = (*MemoryCatalog)(nil)
	_ Listener   = (*MemoryCatalog)(nil)
)

// NewMemoryCatalog returns an empty catalog. boot may be nil.
func NewMemoryCatalog(boot ShardBootstrapper) *MemoryCatalog {
	return &MemoryCatalog{tenants: map[id.TenantID]*TenantEntry{}, shards: map[id.ShardID]*memShard{},
		nss: map[id.NamespaceID]*Entry{}, creating: map[id.NamespaceID]time.Time{}, subs: map[int]chan []byte{},
		boot: boot}
}

// AddTenant registers a tenant (state "active" and isolation "shared" when empty).
func (m *MemoryCatalog) AddTenant(t TenantEntry) {
	if t.State == "" {
		t.State = "active"
	}
	if t.Isolation == "" {
		t.Isolation = "shared"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tenants[t.Tenant] = &t
}

// SetTenantState changes a tenant's state and announces it, as the tenants trigger does.
func (m *MemoryCatalog) SetTenantState(t id.TenantID, state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if te, ok := m.tenants[t]; ok {
		te.State = state
		m.emit(map[string]any{"kind": KindTenant, "tenant_id": t, "state": state})
	}
}

// AddShard registers a shard with the reference defaults (120 namespaces, soft cap 5.5 M facts) and state active.
func (m *MemoryCatalog) AddShard(s id.ShardID, dedicated id.TenantID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[s] = &memShard{Shard: Shard{ID: s, State: ShardActive}, SoftCapFacts: 5_500_000, MaxNamespaces: 120,
		Dedicated: dedicated}
}

// SetMaxNamespaces sets a shard's namespace cap (max_namespaces, default 120).
func (m *MemoryCatalog) SetMaxNamespaces(s id.ShardID, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sh, ok := m.shards[s]; ok {
		sh.MaxNamespaces = n
	}
}

// Reset removes every namespace (the tenants and shards stay); the T3 twin test calls it between iterations.
func (m *MemoryCatalog) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nss = map[id.NamespaceID]*Entry{}
}

// SetFacts sets a shard's facts_estimate (the placement ratio).
func (m *MemoryCatalog) SetFacts(s id.ShardID, facts int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sh, ok := m.shards[s]; ok {
		sh.FactsEstimate = facts
	}
}

// SetDown makes every call fail with UNAVAILABLE (a catalog outage) until it is called with false.
func (m *MemoryCatalog) SetDown(down bool) {
	m.mu.Lock()
	m.down = down
	m.mu.Unlock()
}

// DropNotifications makes changes silent: the catalog changes but no notification reaches the listeners.
func (m *MemoryCatalog) DropNotifications(drop bool) {
	m.mu.Lock()
	m.drop = drop
	m.mu.Unlock()
}

// Snapshot is a frozen copy of the catalog's namespaces and tenants, the input of RestoreTo.
type Snapshot struct {
	nss     map[id.NamespaceID]Entry
	tenants map[id.TenantID]TenantEntry
}

// Snapshot copies the current state.
func (m *MemoryCatalog) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Snapshot{nss: map[id.NamespaceID]Entry{}, tenants: map[id.TenantID]TenantEntry{}}
	for k, e := range m.nss {
		s.nss[k] = *e
	}
	for k, t := range m.tenants {
		s.tenants[k] = *t
	}
	return s
}

// RestoreTo reverts the catalog to an earlier snapshot, as a restore from backup does (RPO 60 s, N163): committed
// changes, deleting markers and epoch bumps made since are gone. No notification is sent (nothing announces a restore);
// the reconcile and the Resolver's reconnect flush repair the caches.
func (m *MemoryCatalog) RestoreTo(s Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nss = map[id.NamespaceID]*Entry{}
	for k, e := range s.nss {
		e := e
		m.nss[k] = &e
	}
	m.tenants = map[id.TenantID]*TenantEntry{}
	for k, t := range s.tenants {
		t := t
		m.tenants[k] = &t
	}
}

// Notify announces a change of ns without making one (the notification a trigger would send).
func (m *MemoryCatalog) Notify(ns id.NamespaceID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.nss[ns]; ok {
		m.announce(e)
	}
}

// announce emits the namespace event of e. mu is held.
func (m *MemoryCatalog) announce(e *Entry) {
	m.emit(map[string]any{"kind": KindNamespace, "namespace_id": e.Namespace, "tenant_id": e.Tenant,
		"shard_id": e.Shard, "epoch": e.Epoch, "state": string(e.State)})
}

// emit delivers a payload to every listener unless notifications are dropped. mu is held.
func (m *MemoryCatalog) emit(v map[string]any) {
	m.events++
	if m.drop {
		return
	}
	v["event_id"] = m.events
	b, _ := json.Marshal(v)
	for _, ch := range m.subs {
		select {
		case ch <- b:
		default: // a listener that cannot keep up is a lost connection: it reconnects and flushes
		}
	}
}

// DisconnectListeners ends every Listen call with an error, as a lost LISTEN connection does.
func (m *MemoryCatalog) DisconnectListeners() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, ch := range m.subs {
		close(ch)
		delete(m.subs, k)
	}
}

// Listen implements Listener.
func (m *MemoryCatalog) Listen(ctx context.Context, ready func(), fn func(payload []byte)) error {
	m.mu.Lock()
	if m.down {
		m.mu.Unlock()
		return errs.Unavailable("catalog down", 0)
	}
	ch := make(chan []byte, 1024)
	k := m.nextSub
	m.nextSub++
	m.subs[k] = ch
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.subs, k)
		m.mu.Unlock()
	}()
	ready()
	for {
		select {
		case <-ctx.Done():
			return nil
		case p, ok := <-ch:
			if !ok {
				return errs.Unavailable("listener disconnected", 0)
			}
			fn(p)
		}
	}
}

func (m *MemoryCatalog) check() error {
	if m.down {
		return errs.Unavailable("catalog down", unavailableRetry)
	}
	return nil
}

func (m *MemoryCatalog) view(e *Entry) *Entry {
	c := *e
	if t, ok := m.tenants[e.Tenant]; ok {
		tc := *t
		c.TenantEntry = &tc
	}
	return &c
}

// TenantState implements TenantStateReader.
func (m *MemoryCatalog) TenantState(_ context.Context, t id.TenantID) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return "", err
	}
	te, ok := m.tenants[t]
	if !ok {
		return "", tenantMissing(t)
	}
	return te.State, nil
}

// Resolve implements Namespaces.
func (m *MemoryCatalog) Resolve(_ context.Context, ns id.NamespaceID) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	e, ok := m.nss[ns]
	if !ok {
		return nil, nsNotFound(ns)
	}
	return m.view(e), nil
}

// ResolveByName implements Namespaces: the live (not deleted) namespace named name of tenant t.
func (m *MemoryCatalog) ResolveByName(_ context.Context, t id.TenantID, name string) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return nil, err
	}
	for _, e := range m.nss {
		if e.Tenant == t && e.Name == name && e.State != StateDeleted {
			return m.view(e), nil
		}
	}
	return nil, errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_NAMESPACE, stringer(name))
}

// pick mirrors pick_shard: least loaded by facts ratio, then namespace count, then id. mu is held.
func (m *MemoryCatalog) pick(t *TenantEntry) (id.ShardID, error) {
	counts := map[id.ShardID]int{}
	for _, e := range m.nss {
		if e.State != StateDeleted {
			counts[e.Shard]++
		}
	}
	var best *memShard
	for _, s := range m.shards {
		if s.State != ShardActive || counts[s.ID] >= s.MaxNamespaces || s.FactsEstimate >= s.SoftCapFacts {
			continue
		}
		if (t.Isolation == "dedicated") != (s.Dedicated != "") || (s.Dedicated != "" && s.Dedicated != t.Tenant) {
			continue
		}
		if best == nil || lessLoaded(s, counts[s.ID], best, counts[best.ID]) {
			best = s
		}
	}
	if best == nil {
		return 0, errs.QuotaExceeded("namespace_placement", memoryv1.QuotaScope_QUOTA_SCOPE_TENANT, 0, 0, 0)
	}
	return best.ID, nil
}

func lessLoaded(a *memShard, an int, b *memShard, bn int) bool {
	// FactsEstimate / SoftCapFacts compared by cross-multiplication (exact, like numeric in pick_shard).
	l, r := a.FactsEstimate*b.SoftCapFacts, b.FactsEstimate*a.SoftCapFacts
	if l != r {
		return l < r
	}
	if an != bn {
		return an < bn
	}
	return a.ID < b.ID
}

// Create implements Namespaces: phase 1 inserts the row in state creating (placement by pick_shard when p.Shard is
// AutoShard), the ShardBootstrapper runs phase 2, and the row is then activated (creating -> active).
func (m *MemoryCatalog) Create(ctx context.Context, p CreateParams) (*Entry, error) {
	if err := validateCreate(&p); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if err := m.check(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	t, ok := m.tenants[p.Tenant]
	if !ok {
		m.mu.Unlock()
		return nil, errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_TENANT, p.Tenant)
	}
	if t.State != "active" {
		m.mu.Unlock()
		return nil, tenantNotActive(p.Tenant, t.State)
	}
	shard := p.Shard
	if shard == AutoShard {
		var err error
		if shard, err = m.pick(t); err != nil {
			m.mu.Unlock()
			return nil, err
		}
	}
	// A taken name or id is refused before an unknown shard: that is the order the Postgres INSERT reports them in (the
	// unique index is checked at insert, the foreign key at the end of the statement).
	for _, e := range m.nss {
		if e.Tenant == p.Tenant && e.Name == p.Name && e.State != StateDeleted {
			m.mu.Unlock()
			return nil, nameTaken(p.Name)
		}
	}
	ns := p.Namespace
	if ns.IsZero() {
		ns = id.NewNamespaceID()
	} else if _, dup := m.nss[ns]; dup {
		m.mu.Unlock()
		return nil, nameTaken(p.Name)
	}
	if _, ok := m.shards[shard]; !ok {
		m.mu.Unlock()
		return nil, errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_SHARD, shard)
	}
	e := &Entry{Namespace: ns, Tenant: p.Tenant, Name: p.Name, Shard: shard, Epoch: 1, State: StateCreating,
		EmbeddingModel: p.EmbeddingModel, EmbeddingDims: p.EmbeddingDims, Config: p.Config, Group: p.Group}
	m.nss[ns] = e
	m.announce(e)
	created := m.view(e)
	m.mu.Unlock()

	if m.boot != nil {
		if err := m.boot.Bootstrap(ctx, created); err != nil {
			return nil, err // the row stays in state creating: the op-sweeper completes or deletes it (N72)
		}
	}
	if err := m.SetState(ctx, ns, StateCreating, StateActive); err != nil {
		return nil, err
	}
	return m.Resolve(ctx, ns)
}

// SetState implements Namespaces: a CAS from -> to on the namespace's state.
func (m *MemoryCatalog) SetState(_ context.Context, ns id.NamespaceID, from, to NamespaceState) error {
	if err := validateSetState(from, to); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return err
	}
	e, ok := m.nss[ns]
	if !ok {
		return nsNotFound(ns)
	}
	if e.State != from {
		return stateMismatch(ns, from, e.State)
	}
	if from == to {
		return nil // a same-state CAS changes nothing and announces nothing (the trigger skips it)
	}
	e.State = to
	m.announce(e)
	return nil
}

// BumpEpoch implements Namespaces: epoch = epoch + 1 where epoch = expected.
func (m *MemoryCatalog) BumpEpoch(_ context.Context, ns id.NamespaceID, expected id.Epoch,
	why EpochReason) (id.Epoch, error) {
	if why == "" {
		return 0, errs.Validation("reason", "required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(); err != nil {
		return 0, err
	}
	e, ok := m.nss[ns]
	if !ok {
		return 0, nsNotFound(ns)
	}
	if e.Epoch != expected {
		return 0, epochMismatch(ns, expected, e.Epoch)
	}
	e.Epoch++
	m.announce(e)
	return e.Epoch, nil
}

// Namespaces lists the namespace ids currently held, sorted (test helper).
func (m *MemoryCatalog) Namespaces() []id.NamespaceID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]id.NamespaceID, 0, len(m.nss))
	for k := range m.nss {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
