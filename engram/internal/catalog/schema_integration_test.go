//go:build integration

package catalog_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/store/migrate"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

const (
	nsA = "018f0000-0000-7000-8000-00000000000a"
	nsB = "018f0000-0000-7000-8000-00000000000b"
)

func mustExec(t testing.TB, c *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := c.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func seed(t testing.TB, c *pgx.Conn) {
	t.Helper()
	mustExec(t, c, `INSERT INTO cells (cell_id, grpc_addr) VALUES ('c1', 'envoy:9000')`)
	mustExec(t, c, `INSERT INTO tenants (tenant_id) VALUES ('acme'), ('globex')`)
	for _, id := range []int{0, 1} {
		mustExec(t, c, `INSERT INTO shards (shard_id, cell_id, state, pgbouncer_addr, direct_addr, dsn_secret_ref,
			blob_prefix, blob_cred_secret_ref, task_queue, kafka_topic)
			VALUES ($1::int, 'c1', 'active', 'pb:6432', 'pg:5432', 'sec', $1::int::text, 'cred',
			'shard-' || $1::int::text, 'engram.events.shard-' || $1::int::text)`, id)
	}
}

func TestSchema_MigrateIsIdempotentAndVersioned(t *testing.T) {
	dsn := newCatalogDB(t)
	applied, err := migrate.Up(context.Background(), migrate.Options{DSN: dsn, Set: migrate.Catalog})
	if err != nil || len(applied) != 0 {
		t.Fatalf("second Up must be a no-op: applied %v, %v", applied, err)
	}
	sts, err := migrate.List(context.Background(), migrate.Options{DSN: dsn, Set: migrate.Catalog})
	if err != nil || len(sts) != 1 || !sts[0].Applied || sts[0].Version != 1 {
		t.Fatalf("status = %+v, %v; want migration 1 applied", sts, err)
	}
	c := connect(t, dsn)
	var n int
	if err := c.QueryRow(context.Background(), `SELECT count(*) FROM pg_tables WHERE schemaname = 'public'
		AND tablename IN ('cells', 'tenants', 'shards', 'namespaces', 'namespace_moves', 'idempotency_keys',
		'tenant_usage_daily', 'catalog_events')`).Scan(&n); err != nil || n != 8 {
		t.Fatalf("catalog tables = %d, %v; want 8", n, err)
	}
	// Every object is owned by catalog_migrate, except the N181 helper.
	var wrong []string
	rows, err := c.Query(context.Background(), `SELECT c.relname FROM pg_class c
		WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'v', 'S')
		AND pg_get_userbyid(c.relowner) <> 'catalog_migrate'
		AND c.relname NOT LIKE 'goose_db_version%' AND NOT EXISTS (SELECT 1 FROM pg_depend d
		WHERE d.objid = c.oid AND d.deptype = 'e')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		wrong = append(wrong, s)
	}
	rows.Close()
	if len(wrong) > 0 {
		t.Errorf("objects not owned by catalog_migrate: %v", wrong)
	}
}

func TestSchema_DownThenUp(t *testing.T) {
	dsn := newCatalogDB(t)
	ctx := context.Background()
	o := migrate.Options{DSN: dsn, Set: migrate.Catalog}
	if _, err := migrate.DownTo(ctx, o); err != nil {
		t.Fatalf("down: %v", err)
	}
	c := connect(t, dsn)
	var n int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace
		AND relkind IN ('r', 'p', 'v', 'S') AND relname NOT LIKE 'goose%' AND NOT EXISTS (SELECT 1 FROM pg_depend d
		WHERE d.objid = pg_class.oid AND d.deptype = 'e')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("objects left after down: %d, %v", n, err)
	}
	if _, err := migrate.Up(ctx, o); err != nil {
		t.Fatalf("up after down: %v", err)
	}
}

func TestSchema_NotifyCarriesNamespaceEvents(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	seed(t, c)
	l := connect(t, dsn)
	mustExec(t, l, `LISTEN catalog_changes`)

	mustExec(t, c, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id)
		VALUES ($1, 'acme', 'main', 0)`, nsA)
	got := waitNotify(t, l)
	if got["namespace_id"] != nsA || got["state"] != "creating" || got["kind"] != "namespace" ||
		got["tenant_id"] != "acme" || got["shard_id"] != float64(0) || got["epoch"] != float64(1) {
		t.Fatalf("payload = %v", got)
	}
	mustExec(t, c, `UPDATE namespaces SET state = 'active' WHERE namespace_id = $1`, nsA)
	if got := waitNotify(t, l); got["state"] != "active" {
		t.Fatalf("payload = %v", got)
	}
	// Stats-only updates do not invalidate caches (reference DDL, catalog_log_namespace_change).
	mustExec(t, c, `UPDATE namespaces SET facts_estimate = 42, bytes_estimate = 7, stats_updated_at = now()
		WHERE namespace_id = $1`, nsA)
	expectQuiet(t, l)
	// A tenant config change names the tenant; the Resolver drops every cached namespace of it.
	mustExec(t, c, `UPDATE tenants SET config = '{"recall": {"default_budget": "HIGH"}}' WHERE tenant_id = 'acme'`)
	if got := waitNotify(t, l); got["kind"] != "tenant" || got["tenant_id"] != "acme" || got["namespace_id"] != nil {
		t.Fatalf("tenant payload = %v", got)
	}
}

func waitNotify(t testing.TB, l *pgx.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := l.WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("no notification: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(n.Payload), &m); err != nil {
		t.Fatalf("payload %q: %v", n.Payload, err)
	}
	return m
}

func expectQuiet(t testing.TB, l *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if n, err := l.WaitForNotification(ctx); err == nil {
		t.Fatalf("unexpected notification %q", n.Payload)
	}
}

func TestSchema_MoveTransitionTrigger(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	seed(t, c)
	mustExec(t, c, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, state) VALUES ($1, 'acme', 'main',
		0, 'active')`, nsA)
	mustExec(t, c, `INSERT INTO namespace_moves (move_id, namespace_id, tenant_id, source_shard_id, target_shard_id,
		from_epoch, to_epoch, w_est_seconds, window_seconds, created_by) VALUES
		('018f0000-0000-7000-8000-0000000000f1', $1, 'acme', 0, 1, 1, 2, 60, 14400, 'op')`, nsA)
	const move = "018f0000-0000-7000-8000-0000000000f1"
	_, err := c.Exec(context.Background(), `UPDATE namespace_moves SET state = 'committed' WHERE move_id = $1`, move)
	var pe *pgconn.PgError
	if err == nil || !asPg(err, &pe) || pe.Code != "23514" {
		t.Fatalf("planned -> committed must be an illegal transition (23514), got %v", err)
	}
	mustExec(t, c, `UPDATE namespace_moves SET state = 'frozen', frozen_at = now(),
		freeze_deadline = now() + interval '1 hour' WHERE move_id = $1`, move)
	// The state-change event is logged and notified through catalog_events.
	var n int
	err = c.QueryRow(context.Background(), `SELECT count(*) FROM catalog_events WHERE kind = 'move'`).Scan(&n)
	if err != nil || n != 2 {
		t.Fatalf("move events = %d, %v; want 2 (insert, planned -> frozen)", n, err)
	}
}

func asPg(err error, target **pgconn.PgError) bool {
	for err != nil {
		if pe, ok := err.(*pgconn.PgError); ok { //nolint:errorlint // pgx wraps by value
			*target = pe
			return true
		}
		u, ok := err.(interface{ Unwrap() error }) //nolint:errorlint // loop below unwraps
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestSchema_PickShardBalancesAndRespectsCaps(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	seed(t, c)
	mustExec(t, c, `UPDATE shards SET facts_estimate = 1000 WHERE shard_id = 0`)
	var shard int
	if err := c.QueryRow(context.Background(), `SELECT pick_shard('acme')`).Scan(&shard); err != nil || shard != 1 {
		t.Fatalf("pick_shard = %d, %v; the less loaded shard 1 expected", shard, err)
	}
	// A dedicated tenant lands only on shards flagged for it; none is, so placement is refused (53400).
	mustExec(t, c, `UPDATE tenants SET isolation = 'dedicated' WHERE tenant_id = 'globex'`)
	err := c.QueryRow(context.Background(), `SELECT pick_shard('globex')`).Scan(&shard)
	var pe *pgconn.PgError
	if err == nil || !asPg(err, &pe) || pe.Code != "53400" {
		t.Fatalf("dedicated tenant without a shard: %v", err)
	}
	_, err = c.Exec(context.Background(), `SELECT pick_shard('nobody')`)
	if err == nil || !strings.Contains(err.Error(), "P0002") {
		t.Fatalf("unknown tenant: %v", err)
	}
}

func TestSchema_ReplicatedHelperIsOwnedByTheStatsReader(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	var owner string
	var member bool
	if err := c.QueryRow(context.Background(), `SELECT pg_get_userbyid(proowner),
		pg_has_role(proowner, 'pg_read_all_stats', 'USAGE') FROM pg_proc WHERE proname = 'catalog_replicated'`).
		Scan(&owner, &member); err != nil || owner != "catalog_stats_reader" || !member {
		t.Fatalf("owner = %q member = %v, %v", owner, member, err)
	}
	// Callable as catalog_app and catalog_admin; with no standby the answer is false.
	mustExec(t, c, `ALTER ROLE catalog_app PASSWORD 'pw'`)
	app := connect(t, roleDSN(dsn, "catalog_app"))
	var ok bool
	err := app.QueryRow(context.Background(), `SELECT catalog_replicated(pg_current_wal_lsn())`).Scan(&ok)
	if err != nil || ok {
		t.Fatalf("catalog_replicated as catalog_app = %v, %v; false (no standby) expected", ok, err)
	}
}

func TestSchema_GrantsMatchTheReference(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	seed(t, c)
	mustExec(t, c, `ALTER ROLE catalog_app PASSWORD 'pw'`)
	app := connect(t, roleDSN(dsn, "catalog_app"))
	ctx := context.Background()
	if _, err := app.Exec(ctx, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id)
		VALUES ($1, 'acme', 'n', 0)`, nsA); err != nil {
		t.Fatalf("catalog_app may insert namespaces: %v", err)
	}
	if _, err := app.Exec(ctx, `DELETE FROM namespaces`); err == nil {
		t.Error("catalog_app must not delete namespaces")
	}
	if _, err := app.Exec(ctx, `INSERT INTO tenants (tenant_id) VALUES ('evil')`); err == nil {
		t.Error("catalog_app must not insert tenants")
	}
	if _, err := app.Exec(ctx, `UPDATE shards SET facts_estimate = 5 WHERE shard_id = 0`); err != nil {
		t.Errorf("catalog_app may update the shard counters: %v", err)
	}
	if _, err := app.Exec(ctx, `UPDATE shards SET state = 'retired' WHERE shard_id = 0`); err == nil {
		t.Error("catalog_app must not change a shard state")
	}
}

// TestSchema_CreateSerialisesWithTenantDelete is the two-session trace of review F1 (N122, N182): a DeleteTenant ack
// (`state = 'deleting'`, uncommitted) is open when a Create starts; the Create must wait on the tenant row and then see
// `deleting`, instead of inserting a namespace after the delete has enumerated the tenant's namespaces.
func TestSchema_CreateSerialisesWithTenantDelete(t *testing.T) {
	dsn := newCatalogDB(t)
	admin := connect(t, dsn)
	seed(t, admin)
	app := openApp(t, dsn, nil)
	ctx := context.Background()

	del := connect(t, dsn)
	mustExec(t, del, `BEGIN`)
	mustExec(t, del, `UPDATE tenants SET state = 'deleting', delete_operation_id = gen_random_uuid(),
		delete_requested_at = now() WHERE tenant_id = 'acme'`)
	res := make(chan error, 1)
	go func() {
		_, err := app.Create(ctx, catalog.CreateParams{Tenant: "acme", Name: "racer", Shard: 0})
		res <- err
	}()
	select {
	case err := <-res:
		t.Fatalf("Create returned while the delete was open (no lock on the tenant row): %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	mustExec(t, del, `COMMIT`)
	select {
	case err := <-res:
		var pe *errs.Error
		if !errors.As(err, &pe) || pe.Kind != errs.KindPreconditionFailed {
			t.Fatalf("Create after the delete committed = %v; want PreconditionFailed{TENANT_DELETING}", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Create did not resume after the delete committed")
	}
	var n int
	err := admin.QueryRow(ctx, `SELECT count(*) FROM namespaces WHERE tenant_id = 'acme'`).Scan(&n)
	if err != nil || n != 0 {
		t.Fatalf("namespaces of the deleting tenant = %d, %v; want 0", n, err)
	}
}

// TestSchema_DeleteWaitsForAnOpenCreate is the other order: a Create holds the tenant lock; the delete ack waits for it
// and so enumerates the new namespace.
func TestSchema_DeleteWaitsForAnOpenCreate(t *testing.T) {
	dsn := newCatalogDB(t)
	seed(t, connect(t, dsn))
	app := connect(t, roleDSNWithPassword(t, dsn, "catalog_app"))
	mustExec(t, app, `BEGIN`)
	var st string
	err := app.QueryRow(context.Background(), `SELECT catalog_lock_tenant('acme')`).Scan(&st)
	if err != nil || st != "active" {
		t.Fatalf("lock = %q, %v", st, err)
	}
	del := connect(t, dsn)
	done := make(chan error, 1)
	go func() {
		_, err := del.Exec(context.Background(), `UPDATE tenants SET state = 'deleting', delete_operation_id =
			gen_random_uuid(), delete_requested_at = now() WHERE tenant_id = 'acme'`)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the delete ack did not wait for the create's tenant lock: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	mustExec(t, app, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id)
		VALUES ($1, 'acme', 'first', 0)`, nsA)
	mustExec(t, app, `COMMIT`)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var n int
	if err := connect(t, dsn).QueryRow(context.Background(), `SELECT count(*) FROM namespaces
		WHERE tenant_id = 'acme'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the delete must see the namespace the create committed: %d, %v", n, err)
	}
}

func roleDSNWithPassword(t testing.TB, dsn, role string) string {
	t.Helper()
	mustExec(t, connect(t, dsn), `ALTER ROLE `+role+` PASSWORD '`+rolePassword+`'`)
	return roleDSN(dsn, role)
}

// TestSchema_MoveRowRules covers review F20: the floor columns move together and precede the seal (N179), one outcome
// per row (N125, N171), and no INSERT can enter a later state, `cleaning` past the 24 h gate included (N170).
func TestSchema_MoveRowRules(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	seed(t, c)
	mustExec(t, c, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, state)
		VALUES ($1, 'acme', 'main', 0, 'active')`, nsA)
	ctx := context.Background()
	exec := func(cols, vals string) error {
		_, err := c.Exec(ctx, `INSERT INTO namespace_moves (move_id, namespace_id, tenant_id, source_shard_id,
			target_shard_id, from_epoch, to_epoch, w_est_seconds, window_seconds, created_by, `+cols+`)
			VALUES (gen_random_uuid(), $1, 'acme', 0, 1, 1, 2, 60, 14400, 'op', `+vals+`)`, nsA)
		return err
	}
	bad := map[string][2]string{
		"a seal without a floor (N179)": {"state, frozen_at, freeze_deadline, copy_sealed_at, reconciled_at",
			`'cutover', now() - interval '2 h', now() + interval '1 h', now() - interval '1 h', now()`},
		"a floor without a timeline (N179)": {"copy_end_lsn", `'0/16B3748'`},
		"a rolled_back row that committed (N125)": {"state, committed_at, reconciled_at",
			`'rolled_back', now(), now()`},
		"a rollback stamp on a planned row (N171)": {"rolled_back_replicated_at", `now()`},
		"an INSERT in cleaning (N170)": {"state, frozen_at, freeze_deadline, committed_at, moved_out_at, " +
			"committed_replicated_at, copy_sealed_at, copy_end_lsn, copy_end_timeline, activated_at, reconciled_at",
			`'cleaning', now() - interval '3 h', now() - interval '1 h', now() - interval '2 h', now() - interval '2 h',
			now() - interval '2 h', now() - interval '1 h' , '0/1', 1, now() - interval '1 h', now()`},
		"an INSERT in committed without the reconcile (N184)": {"state, frozen_at, freeze_deadline, committed_at",
			`'committed', now(), now() + interval '1 h', now()`},
	}
	for name, v := range bad {
		var pe *pgconn.PgError
		if err := exec(v[0], v[1]); err == nil || !asPg(err, &pe) || pe.Code != "23514" {
			t.Errorf("%s: err = %v; want a check violation (23514)", name, err)
		}
	}
	if err := exec("copy_end_lsn, copy_end_timeline", `'0/16B3748', 1`); err == nil {
		t.Error("a floor without the seal must be refused")
	}
	if err := exec("frozen_at, freeze_deadline", `now(), now() + interval '1 h'`); err != nil {
		t.Errorf("a planned move still inserts: %v", err)
	}
}

// TestSchema_PickShardIsBounded: with 100 live namespaces per shard and a pile of tombstones, the placement of a new
// namespace touches a number of buffers that does not grow with the tombstones (review F21).
func TestSchema_PickShardIsBounded(t *testing.T) {
	dsn := newCatalogDB(t)
	c := connect(t, dsn)
	seed(t, c)
	mustExec(t, c, `INSERT INTO shards (shard_id, cell_id, state, pgbouncer_addr, direct_addr, dsn_secret_ref,
		blob_prefix, blob_cred_secret_ref, task_queue, kafka_topic)
		SELECT g, 'c1', 'active', 'pb', 'pg', 's', g::text, 'c', 'shard-' || g, 'engram.events.shard-' || g
		FROM generate_series(2, 99) g`)
	mustExec(t, c, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, state, deleted_at)
		SELECT gen_random_uuid(), 'acme', 'n' || g, g % 100,
		CASE WHEN g % 10 = 0 THEN 'active' ELSE 'deleted' END::namespace_state,
		CASE WHEN g % 10 = 0 THEN NULL ELSE now() END FROM generate_series(1, 30000) g`)
	mustExec(t, c, `ANALYZE namespaces`)
	var plan string
	rows, err := c.Query(context.Background(), `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT pick_shard('acme')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var l string
		_ = rows.Scan(&l)
		plan += l + "\n"
	}
	rows.Close()
	t.Logf("pick_shard over 100 shards, 3000 live and 27000 deleted namespaces:\n%s", plan)
	m := regexp.MustCompile(`shared hit=(\d+)`).FindStringSubmatch(plan)
	if m == nil {
		t.Fatalf("no buffer figures in the plan: %s", plan)
	}
	// 100 shards x (3000/100 live entries + the index descents) is about 2 000 hits; walking the 27 000 tombstones too
	// (the reference's index on (shard_id, state)) costs ten times that.
	if hits, _ := strconv.Atoi(m[1]); hits > 6000 {
		t.Errorf("pick_shard touched %d buffers; the tombstones must not be walked", hits)
	}
}

// TestSchema_GroupIsAString: profile.group is what Entry.Group reads (N65, CONFLICTS.md #20), so it must be a string.
func TestSchema_GroupIsAString(t *testing.T) {
	c := connect(t, newCatalogDB(t))
	seed(t, c)
	_, err := c.Exec(context.Background(), `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, profile)
		VALUES ($1, 'acme', 'g', 0, '{"group": 5}')`, nsA)
	var pe *pgconn.PgError
	if err == nil || !asPg(err, &pe) || pe.Code != "23514" {
		t.Fatalf("a numeric group must be refused: %v", err)
	}
}

// TestSchema_AppliesAsANonSuperuserBootstrapRole is review F15 and what N181's TestCatalog_ReplicatedHelperAsAdmin
// needs: migration 0001 does not need a superuser. The recipe: a LOGIN role with CREATEROLE, ADMIN OPTION on
// pg_read_all_stats, ownership of the database, and createrole_self_grant = 'set, inherit' (so that the roles it
// creates are usable by it).
func TestSchema_AppliesAsANonSuperuserBootstrapRole(t *testing.T) {
	d := pgtest.NewDatabase(t, false)
	super := connect(t, d.DSN(pgtest.Super))
	ctx := context.Background()
	// Roles are cluster-wide and earlier tests (run by the superuser) created the catalog roles: start from a cluster
	// that has none, as a fresh catalog server does, so that the bootstrap role is the one that creates them.
	for _, r := range []string{"catalog_app", "catalog_admin", "catalog_migrate", "catalog_stats_reader"} {
		mustExec(t, super, `DROP ROLE IF EXISTS `+r)
	}
	for _, q := range []string{
		`DROP ROLE IF EXISTS boot`,
		`CREATE ROLE boot LOGIN CREATEROLE PASSWORD 'pw'`,
		`GRANT pg_read_all_stats TO boot WITH ADMIN OPTION`,
		`ALTER ROLE boot SET createrole_self_grant = 'set, inherit'`,
		`ALTER DATABASE ` + d.Name + ` OWNER TO boot`,
	} {
		mustExec(t, super, q)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), d.DSN(pgtest.Super))
		if err == nil {
			defer c.Close(context.Background()) //nolint:errcheck // cleanup
			_, _ = c.Exec(context.Background(), `DROP OWNED BY boot CASCADE`)
		}
	})
	boot := d.RoleDSN("boot", "pw")
	if _, err := migrate.Up(ctx, migrate.Options{DSN: boot, Set: migrate.Catalog}); err != nil {
		t.Fatalf("0001 as the non-superuser bootstrap role: %v", err)
	}
	var owner string
	var inherited bool
	if err := super.QueryRow(ctx, `SELECT pg_get_userbyid(proowner), pg_has_role(proowner, 'pg_read_all_stats', 'USAGE')
		FROM pg_proc WHERE proname = 'catalog_replicated'`).Scan(&owner, &inherited); err != nil ||
		owner != "catalog_stats_reader" || !inherited {
		t.Fatalf("helper owner = %q (pg_read_all_stats: %v), %v", owner, inherited, err)
	}
}
