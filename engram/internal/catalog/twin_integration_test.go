//go:build integration

package catalog_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// observe renders an outcome in a form both implementations must agree on: the entry's observable fields, or the error
// kind and gRPC code (messages and ids of non-entities are not part of the contract).
func observe(e *catalog.Entry, err error) string {
	if err != nil {
		return fmt.Sprintf("err kind=%v code=%v retryable=%v", errs.KindOf(err), errs.ToStatus(err).Code(),
			errs.IsRetryable(err))
	}
	return fmt.Sprintf("ok %s %s %s shard=%d epoch=%d state=%s model=%s dims=%d tenant-state=%s group=%q", e.Namespace,
		e.Tenant, e.Name, e.Shard, e.Epoch, e.State, e.EmbeddingModel, e.EmbeddingDims, e.TenantEntry.State, e.Group)
}

func observeEpoch(ep id.Epoch, err error) string {
	if err != nil {
		return observe(nil, err)
	}
	return fmt.Sprintf("ok epoch=%d", ep)
}

// TestCatalog_MemoryEqualsPostgres is the T3 twin of PLAN.md section 8.2 (internal/catalog, CAS row): the MemoryCatalog
// and the Postgres catalog (as catalog_app, through the real grants, triggers and pick_shard) give identical results
// for a random sequence of Create, SetState, BumpEpoch, Resolve and ResolveByName, including every error.
func TestCatalog_MemoryEqualsPostgres(t *testing.T) {
	dsn := newCatalogDB(t)
	admin := connect(t, dsn)
	seed(t, admin) // cells c1; tenants acme, globex; shards 0, 1 (shared)
	mustExec(t, admin, `INSERT INTO tenants (tenant_id, state) VALUES ('sleepy', 'suspended')`)
	mustExec(t, admin, `INSERT INTO tenants (tenant_id, isolation) VALUES ('ded', 'dedicated')`)
	mustExec(t, admin, `INSERT INTO shards (shard_id, cell_id, state, pgbouncer_addr, direct_addr, dsn_secret_ref,
		blob_prefix, blob_cred_secret_ref, task_queue, kafka_topic, dedicated_tenant_id, max_namespaces)
		VALUES (2, 'c1', 'active', 'pb:6432', 'pg:5432', 'sec', '2', 'cred', 'shard-2', 'engram.events.shard-2',
		'ded', 2)`)
	mustExec(t, admin, `UPDATE shards SET max_namespaces = 3 WHERE shard_id IN (0, 1)`)
	pg := openApp(t, dsn, nil)

	mem := catalog.NewMemoryCatalog(nil)
	for _, te := range []catalog.TenantEntry{{Tenant: "acme"}, {Tenant: "globex"},
		{Tenant: "sleepy", State: "suspended"}, {Tenant: "ded", Isolation: "dedicated"}} {
		mem.AddTenant(te)
	}
	mem.AddShard(0, "")
	mem.AddShard(1, "")
	mem.AddShard(2, "ded")
	// The twin's shards must share the placement caps (max_namespaces) of the Postgres side.
	mem.SetMaxNamespaces(0, 3)
	mem.SetMaxNamespaces(1, 3)
	mem.SetMaxNamespaces(2, 2)

	ctx := context.Background()
	tenants := []id.TenantID{"acme", "globex", "sleepy", "ded", "nobody"}
	names := []string{"a", "b", "c", "bad name!", ""}
	shards := []id.ShardID{catalog.AutoShard, 0, 1, 2, 9}
	states := []catalog.NamespaceState{catalog.StateCreating, catalog.StateActive, catalog.StateMoving,
		catalog.StateFrozen, catalog.StateRestoring, catalog.StateDeleting, catalog.StateDeleted, "bogus"}
	pool := make([]id.NamespaceID, 8)
	for i := range pool {
		pool[i] = id.NewNamespaceID()
	}

	outcomes := map[string]int{} // coverage: how often each operation ended each way
	defer func() { t.Logf("twin outcomes: %v", outcomes) }()
	rapid.Check(t, func(rt *rapid.T) {
		// Each rapid iteration starts from an empty namespaces table on both sides.
		mustExec(t, admin, `TRUNCATE namespaces, namespace_moves, catalog_events RESTART IDENTITY CASCADE`)
		mustExec(t, admin, `UPDATE shards SET namespaces_count = 0`)
		mem.Reset()

		both := func(op string, f func(c catalog.Namespaces) string) {
			a, b := f(mem), f(pg)
			if a != b {
				rt.Fatalf("%s:\n  memory:   %s\n  postgres: %s", op, a, b)
			}
			word, _, _ := strings.Cut(op, " ")
			outcomes[word+" -> "+strings.Join(strings.Fields(a)[:min(2, len(strings.Fields(a)))], " ")]++
		}
		rt.Repeat(map[string]func(*rapid.T){
			"create": func(rt *rapid.T) {
				p := catalog.CreateParams{Namespace: rapid.SampledFrom(pool).Draw(rt, "id"),
					Tenant: rapid.SampledFrom(tenants).Draw(rt, "tenant"),
					Name:   rapid.SampledFrom(names).Draw(rt, "name"),
					Group:  rapid.SampledFrom([]string{"", "eng", "sales"}).Draw(rt, "group"),
					Shard:  rapid.SampledFrom(shards).Draw(rt, "shard")}
				both(fmt.Sprintf("create %+v", p), func(c catalog.Namespaces) string {
					e, err := c.Create(ctx, p)
					return observe(e, err)
				})
			},
			"set_state": func(rt *rapid.T) {
				ns := rapid.SampledFrom(pool).Draw(rt, "ns")
				from, to := rapid.SampledFrom(states).Draw(rt, "from"), rapid.SampledFrom(states).Draw(rt, "to")
				both(fmt.Sprintf("set_state %s %s->%s", ns, from, to), func(c catalog.Namespaces) string {
					if err := c.SetState(ctx, ns, from, to); err != nil {
						return observe(nil, err)
					}
					e, err := c.Resolve(ctx, ns)
					return "ok " + observe(e, err)
				})
			},
			"bump": func(rt *rapid.T) {
				ns := rapid.SampledFrom(pool).Draw(rt, "ns")
				exp := id.Epoch(rapid.IntRange(0, 3).Draw(rt, "expected"))
				why := catalog.EpochReason(rapid.SampledFrom([]string{"failover", "restore", ""}).Draw(rt, "why"))
				both(fmt.Sprintf("bump %s %d %q", ns, exp, why), func(c catalog.Namespaces) string {
					ep, err := c.BumpEpoch(ctx, ns, exp, why)
					return observeEpoch(ep, err)
				})
			},
			"resolve": func(rt *rapid.T) {
				ns := rapid.SampledFrom(pool).Draw(rt, "ns")
				both("resolve "+ns.String(), func(c catalog.Namespaces) string {
					e, err := c.Resolve(ctx, ns)
					return observe(e, err)
				})
			},
			"resolve_by_name": func(rt *rapid.T) {
				tn, name := rapid.SampledFrom(tenants).Draw(rt, "tenant"), rapid.SampledFrom(names).Draw(rt, "name")
				both("resolve_by_name "+string(tn)+"/"+name, func(c catalog.Namespaces) string {
					e, err := c.ResolveByName(ctx, tn, name)
					return observe(e, err)
				})
			},
		})
	})
}
