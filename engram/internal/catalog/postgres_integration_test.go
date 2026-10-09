//go:build integration

package catalog_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// openApp opens the Postgres catalog as catalog_app, the role engram-api runs with, so every test exercises the grants.
func openApp(t testing.TB, dsn string, boot catalog.ShardBootstrapper) *catalog.Postgres {
	t.Helper()
	admin := connect(t, dsn)
	mustExec(t, admin, `ALTER ROLE catalog_app PASSWORD 'pw'`)
	p, err := catalog.OpenPostgres(context.Background(), roleDSN(dsn, "catalog_app"),
		catalog.PostgresOptions{Bootstrapper: boot})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

type recordingBootstrapper struct {
	got []catalog.Entry
	err error
}

func (r *recordingBootstrapper) Bootstrap(_ context.Context, e *catalog.Entry) error {
	r.got = append(r.got, *e)
	return r.err
}

func TestPostgres_CreateResolveCAS(t *testing.T) {
	dsn := newCatalogDB(t)
	admin := connect(t, dsn)
	seed(t, admin)
	boot := &recordingBootstrapper{}
	p := openApp(t, dsn, boot)
	ctx := context.Background()

	e, err := p.Create(ctx, catalog.CreateParams{Tenant: "acme", Name: "main", Shard: catalog.AutoShard})
	if err != nil {
		t.Fatal(err)
	}
	if e.State != catalog.StateActive || e.Epoch != 1 || e.EmbeddingModel != "nomic-embed-text-v1.5" ||
		e.EmbeddingDims != 768 || e.TenantEntry == nil || e.TenantEntry.State != "active" {
		t.Fatalf("created = %+v", e)
	}
	if len(boot.got) != 1 || boot.got[0].State != catalog.StateCreating || boot.got[0].Shard != e.Shard {
		t.Errorf("the bootstrapper runs once, on the creating row: %+v", boot.got)
	}
	if got, err := p.Resolve(ctx, e.Namespace); err != nil || got.Name != "main" {
		t.Fatalf("resolve = %+v, %v", got, err)
	}
	if got, err := p.ResolveByName(ctx, "acme", "main"); err != nil || got.Namespace != e.Namespace {
		t.Fatalf("resolve by name = %+v, %v", got, err)
	}
	if _, err := p.ResolveByName(ctx, "globex", "main"); !errs.Is(err, errs.KindNotFound) {
		t.Errorf("names are tenant-scoped: %v", err)
	}
	if _, err := p.Create(ctx, catalog.CreateParams{Tenant: "acme", Name: "main", Shard: 0}); !errs.Is(err,
		errs.KindOperationConflict) {
		t.Errorf("a taken name = %v; want ALREADY_EXISTS", err)
	}

	// SetState and BumpEpoch are CAS.
	err = p.SetState(ctx, e.Namespace, catalog.StateFrozen, catalog.StateActive)
	if !errs.Is(err, errs.KindPreconditionFailed) {
		t.Errorf("CAS from the wrong state = %v", err)
	}
	if err := p.SetState(ctx, e.Namespace, catalog.StateActive, catalog.StateMoving); err != nil {
		t.Fatal(err)
	}
	if _, err := p.BumpEpoch(ctx, e.Namespace, 5, "failover"); !errs.Is(err, errs.KindPreconditionFailed) {
		t.Errorf("stale expected epoch = %v", err)
	}
	if ep, err := p.BumpEpoch(ctx, e.Namespace, 1, "failover"); err != nil || ep != 2 {
		t.Errorf("bump = %d, %v", ep, err)
	}
	// deleted is a tombstone: it needs deleted_at (DDL CHECK), frees the name, and never leaves.
	if err := p.SetState(ctx, e.Namespace, catalog.StateMoving, catalog.StateDeleted); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ResolveByName(ctx, "acme", "main"); !errs.Is(err, errs.KindNotFound) {
		t.Errorf("a deleted namespace has no live name: %v", err)
	}
	err = p.SetState(ctx, e.Namespace, catalog.StateDeleted, catalog.StateActive)
	if !errs.Is(err, errs.KindPreconditionFailed) {
		t.Errorf("leaving deleted = %v", err)
	}
	if _, err := p.Create(ctx, catalog.CreateParams{Tenant: "acme", Name: "main", Shard: 1}); err != nil {
		t.Errorf("the name is reusable after delete: %v", err)
	}
}

func TestPostgres_FailedBootstrapLeavesACreatingRow(t *testing.T) {
	dsn := newCatalogDB(t)
	seed(t, connect(t, dsn))
	boot := &recordingBootstrapper{err: errs.Unavailable("shard down", time.Second)}
	p := openApp(t, dsn, boot)
	ns := id.NewNamespaceID()
	_, err := p.Create(context.Background(),
		catalog.CreateParams{Namespace: ns, Tenant: "acme", Name: "half", Shard: 0})
	if !errs.Is(err, errs.KindUnavailable) {
		t.Fatalf("create = %v", err)
	}
	e, err := p.Resolve(context.Background(), ns)
	if err != nil || e.State != catalog.StateCreating {
		t.Fatalf("the row stays creating for the op-sweeper (PLAN.md 3.2): %+v, %v", e, err)
	}
	// The op-sweeper (or the caller's retry) completes the row: creating -> active.
	boot.err = nil
	if err := p.SetState(context.Background(), ns, catalog.StateCreating, catalog.StateActive); err != nil {
		t.Fatal(err)
	}
}

func TestPostgres_ListenDeliversNotifications(t *testing.T) {
	dsn := newCatalogDB(t)
	admin := connect(t, dsn)
	seed(t, admin)
	p := openApp(t, dsn, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := catalog.NewResolver(p, p, catalog.ResolverOptions{ReconnectMin: 50 * time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitFor(t, func() bool { return r.Stats().Flushes >= 1 })

	e, err := p.Create(ctx, catalog.CreateParams{Tenant: "acme", Name: "live", Shard: 0})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.Resolve(ctx, e.Namespace); err != nil || got.Epoch != 1 {
		t.Fatalf("resolve = %+v, %v", got, err)
	}
	if _, err := p.BumpEpoch(ctx, e.Namespace, 1, "move"); err != nil {
		t.Fatal(err)
	}
	// LISTEN/NOTIFY invalidation: the cached entry (TTL 60 s) is dropped by the payload, not by time.
	waitFor(t, func() bool { got, _ := r.Resolve(ctx, e.Namespace); return got != nil && got.Epoch == 2 })

	// Kill the LISTEN backend: the resolver reconnects and flushes once; a change made while disconnected is picked up.
	flushes := r.Stats().Flushes
	mustExec(t, admin, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE query LIKE 'LISTEN catalog_changes%'`)
	if _, err := p.BumpEpoch(ctx, e.Namespace, 2, "move"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.Stats().Flushes > flushes })
	waitFor(t, func() bool { got, _ := r.Resolve(ctx, e.Namespace); return got != nil && got.Epoch == 3 })
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run = %v", err)
	}
}

// blackholeProxy forwards TCP to target until Blackhole: then it keeps both sockets open and drops every byte, which is
// how a catalog that failed over without a reset looks from the other side (no RST, no FIN, no answer).
type blackholeProxy struct {
	ln   net.Listener
	dead atomic.Bool
}

func newBlackholeProxy(t testing.TB, target string) *blackholeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &blackholeProxy{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			t.Cleanup(func() { _ = c.Close(); _ = up.Close() })
			go p.pipe(c, up)
			go p.pipe(up, c)
		}
	}()
	return p
}

func (p *blackholeProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.dead.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// TestPostgres_ListenDetectsADeadConnection is review F6: a LISTEN connection whose peer vanished without a reset is
// noticed by the periodic ping within seconds (here 0.2 s + 0.3 s), Listen returns an error, and the Resolver's loop
// reconnects and flushes, instead of waiting for the kernel's keepalive.
func TestPostgres_ListenDetectsADeadConnection(t *testing.T) {
	dsn := newCatalogDB(t)
	seed(t, connect(t, dsn))
	cfg, err := pgx.ParseConfig(roleDSNWithPassword(t, dsn, "catalog_app"))
	if err != nil {
		t.Fatal(err)
	}
	proxy := newBlackholeProxy(t, net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))))
	cfg.Host, cfg.Port = "127.0.0.1", uint16(proxy.ln.Addr().(*net.TCPAddr).Port) //nolint:gosec // port range
	viaProxy := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", cfg.User, cfg.Password, proxy.ln.Addr(),
		cfg.Database)
	p, err := catalog.OpenPostgres(context.Background(), viaProxy, catalog.PostgresOptions{
		ListenPingEvery: 200 * time.Millisecond, ListenPingTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- p.Listen(ctx, func() { ready <- struct{}{} }, func([]byte) {}) }()
	<-ready
	time.Sleep(600 * time.Millisecond) // healthy pings pass
	select {
	case err := <-done:
		t.Fatalf("Listen ended while the connection was healthy: %v", err)
	default:
	}
	proxy.dead.Store(true)
	start := time.Now()
	select {
	case err := <-done:
		if err == nil || !errs.Is(err, errs.KindUnavailable) {
			t.Fatalf("Listen = %v; want an UNAVAILABLE loss", err)
		}
		if took := time.Since(start); took > 3*time.Second {
			t.Errorf("the dead connection took %v to notice", took)
		}
	case <-ctx.Done():
		t.Fatal("a dead LISTEN connection was never noticed")
	}
}
