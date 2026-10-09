//go:build integration

package store_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/migrate"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// TestMigrate_UpDownUp: goose up, down and up again on an empty database (the M0.2 exit test), plus the partial
// reversions and the refusal of a destructive Down while data exists.
func TestMigrate_UpDownUp(t *testing.T) {
	ctx := context.Background()
	db := pgtest.NewDatabase(t, false)
	opts := migrate.Options{DSN: db.DSN(pgtest.Super)}

	up := func(want int) {
		t.Helper()
		applied, err := migrate.Up(ctx, opts)
		if err != nil || len(applied) != want {
			t.Fatalf("up applied %v (err %v), want %d migrations", applied, err, want)
		}
	}
	down := func(to int64, want int) {
		t.Helper()
		o := opts
		o.To = to
		reverted, err := migrate.DownTo(ctx, o)
		if err != nil || len(reverted) != want {
			t.Fatalf("down to %d reverted %v (err %v), want %d", to, reverted, err, want)
		}
	}
	objects := func() int {
		c, err := pgx.Connect(ctx, db.DSN(pgtest.Super))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(ctx) }()
		var n int
		if err := c.QueryRow(ctx, `SELECT (SELECT count(*) FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace
			   AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i'))
			   AND c.relname NOT LIKE 'goose%' AND c.relname NOT IN ('spatial_ref_sys', 'geometry_columns',
			     'geography_columns'))
			  + (SELECT count(*) FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace
			     AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'))`).
			Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	up(4)
	full := objects()
	if full < 100 {
		t.Fatalf("only %d objects after up", full)
	}
	down(3, 1) // markers only
	down(1, 2) // partitions, indexes
	up(3)
	down(0, 4)
	if n := objects(); n != 0 {
		t.Fatalf("%d schema objects survive a full down", n)
	}
	up(4)
	if n := objects(); n != full {
		t.Fatalf("objects after up, down, up = %d, first time %d", n, full)
	}
	sts, err := migrate.List(ctx, opts)
	if err != nil || len(sts) != 4 {
		t.Fatalf("status: %v (err %v)", sts, err)
	}
	for _, s := range sts {
		if !s.Applied {
			t.Errorf("migration %04d is not applied", s.Version)
		}
	}

	// the self-checks pass on a template1 database too (the ParadeDB template carries PostGIS tables)
	// and a Down refuses to destroy data
	c, err := pgx.Connect(ctx, db.DSN(pgtest.Super))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	if _, err := c.Exec(ctx, "INSERT INTO shard_meta (shard_id, schema_version) VALUES (7, 4)"); err != nil {
		t.Fatal(err)
	}
	o := opts
	o.To = 0
	if _, err := migrate.DownTo(ctx, o); err == nil || !strings.Contains(err.Error(), "Down refused") {
		t.Fatalf("a Down over a provisioned shard must be refused, got %v", err)
	}
	// the guard sits at the top of every Down (M0.2 review F6): the very first one, 0004's, refuses, nothing was
	// reverted and the shard is as serviceable as before (grants and indexes intact)
	sts, err = migrate.List(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sts {
		if !s.Applied {
			t.Errorf("after the refused Down migration %04d is not applied", s.Version)
		}
	}
	if n := objects(); n != full {
		t.Fatalf("a refused Down changed the schema: %d objects, was %d", n, full)
	}
	var priv bool
	err = c.QueryRow(ctx, `SELECT has_table_privilege('engram_app', 'facts', 'INSERT')
		AND has_parameter_privilege('engram_move', 'session_replication_role', 'SET')`).Scan(&priv)
	if err != nil || !priv {
		t.Fatalf("grants after a refused Down: %v (err %v)", priv, err)
	}
	// a Down on an unprovisioned database leaves the cluster-wide parameter grant alone
	if _, err := c.Exec(ctx, "DELETE FROM shard_meta"); err != nil {
		t.Fatal(err)
	}
	down(0, 4)
	up(4)
	if err := c.QueryRow(ctx, `SELECT has_parameter_privilege('engram_move', 'session_replication_role', 'SET')`).
		Scan(&priv); err != nil || !priv {
		t.Fatalf("SET ON PARAMETER session_replication_role lost by a Down: %v (err %v)", priv, err)
	}
}

// TestMigrate_Template1: the shard database may be created from template1, which in the ParadeDB image carries PostGIS
// tables (spatial_ref_sys); the self-checks and the RLS check ignore extension-owned relations.
func TestMigrate_Template1(t *testing.T) {
	ctx := context.Background()
	sup, err := pgx.Connect(ctx, pgtest.DSN(pgtest.Super))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sup.Close(ctx) }()
	name := "engram_t3_template1_check"
	if _, err := sup.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sup.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE template1"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { // a fresh connection: the deferred Close above has already run (M0.2 review F13)
		c, err := pgx.Connect(context.Background(), pgtest.DSN(pgtest.Super))
		if err != nil {
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	dsn := strings.Replace(pgtest.DSN(pgtest.Super), "/"+pgtest.DatabaseName(), "/"+name, 1)
	if _, err := migrate.Up(ctx, migrate.Options{DSN: dsn}); err != nil {
		t.Fatalf("migrate on a template1 database: %v", err)
	}
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	vs, err := migrate.CheckRLS(ctx, c)
	if err != nil || len(vs) != 0 {
		t.Fatalf("rls check on a template1 database: %v (err %v)", vs, err)
	}
}

// TestMigrate_OwnersAndRoleDefaults: engram_migrate owns every object, the SECURITY DEFINER functions run as it, the
// standby helper belongs to engram_stats_reader, and each role carries exactly the defaults of N122/N133e.
func TestMigrate_OwnersAndRoleDefaults(t *testing.T) {
	ctx := context.Background()
	c := connect(t, pgtest.Super)

	var other string
	if err := c.QueryRow(ctx, `SELECT coalesce(string_agg(c.relname || '=' || r.rolname, ', '), '') FROM pg_class c
		 JOIN pg_roles r ON r.oid = c.relowner
		WHERE c.relnamespace = 'public'::regnamespace AND r.rolname <> 'engram_migrate'
		  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i'))
		  AND c.relname NOT LIKE 'goose%' AND c.relkind IN ('r', 'p', 'v', 'S', 'i', 'I')`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	if other != "" {
		t.Errorf("objects not owned by engram_migrate: %s", other)
	}
	rows, err := c.Query(ctx,
		`SELECT p.proname, r.rolname, p.prosecdef FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
		 WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE 'engram\_%'
		   AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	definers := map[string]bool{}
	for rows.Next() {
		var fn, owner string
		var def bool
		if err := rows.Scan(&fn, &owner, &def); err != nil {
			t.Fatal(err)
		}
		want := "engram_migrate"
		if fn == "engram_standby_replayed" {
			want = "engram_stats_reader"
		}
		if owner != want {
			t.Errorf("function %s is owned by %s, want %s", fn, owner, want)
		}
		if def {
			definers[fn] = true
		}
	}
	rows.Close()
	wantDefiners := []string{"engram_cleanup_namespace", "engram_entity_fuzzy", "engram_move_indexes_valid",
		"engram_seq_advance", "engram_standby_replayed"}
	var got []string
	for f := range definers {
		got = append(got, f)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(wantDefiners, ",") {
		t.Errorf("SECURITY DEFINER functions = %v, want %v", got, wantDefiners)
	}

	// effective defaults per role, as the role itself sees them
	for role, want := range map[pgtest.Role]map[string]string{
		pgtest.App: {"lock_timeout": "2s", "statement_timeout": "30s",
			"idle_in_transaction_session_timeout": "30s", "synchronous_commit": "local"},
		pgtest.Move: {"lock_timeout": "10s", "statement_timeout": "30s",
			"idle_in_transaction_session_timeout": "30s", "synchronous_commit": "local"},
		pgtest.Admin: {"lock_timeout": "10s", "statement_timeout": "30s",
			"idle_in_transaction_session_timeout": "30s", "synchronous_commit": "local"},
		pgtest.Relay: {"statement_timeout": "0", "idle_in_transaction_session_timeout": "0",
			"synchronous_commit": "local"},
		pgtest.Migrate: {"statement_timeout": "0", "idle_in_transaction_session_timeout": "0"},
	} {
		rc := connect(t, role)
		for gucName, v := range want {
			var got string
			if err := rc.QueryRow(ctx, "SELECT current_setting($1)", gucName).Scan(&got); err != nil || got != v {
				t.Errorf("%s: %s = %q (err %v), want %q", role, gucName, got, err, v)
			}
		}
	}
	// the mover may switch replica mode for its own session; the app may not
	if _, err := connect(t, pgtest.Move).Exec(ctx, "SET session_replication_role = replica"); err != nil {
		t.Errorf("engram_move SET session_replication_role: %v", err)
	}
	if _, err := connect(t, pgtest.App).Exec(ctx, "SET session_replication_role = replica"); sqlState(err) != "42501" {
		t.Errorf("engram_app SET session_replication_role: want 42501, got %v", err)
	}
	// role attributes
	var attrs string
	if err := c.QueryRow(ctx,
		`SELECT string_agg(rolname || ':' || rolcanlogin || ':' || rolsuper || ':' || rolcreaterole
		 || ':' || rolcreatedb || ':' || rolbypassrls, ' ' ORDER BY rolname) FROM pg_roles WHERE rolname LIKE
		   'engram\_%'`).Scan(&attrs); err != nil {
		t.Fatal(err)
	}
	const wantAttrs = "engram_admin:true:false:false:false:true engram_app:true:false:false:false:false " +
		"engram_migrate:true:false:false:false:true engram_move:true:false:false:false:false " +
		"engram_relay:true:false:false:false:false engram_stats_reader:false:false:false:false:false"
	if attrs != wantAttrs {
		t.Errorf("role attributes:\n got %s\nwant %s", attrs, wantAttrs)
	}
}

// TestMigrations_MatchPlanDDL proves the migrations are the reference file split, not a rewrite: a database built by
// the four migrations and a database built by running docs/plan/sql/shard_schema.sql as one script have the same
// catalog (tables, columns, constraints, indexes, policies, triggers, function bodies, views, grants). The only
// differences allowed are the ones the report lists: deletion_log's fillfactor (the reference file omits it; N137 and
// TestContent_InsertOnly require it) and the objects of goose.
func TestMigrations_MatchPlanDDL(t *testing.T) {
	ctx := context.Background()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "plan", "sql", "shard_schema.sql"))
	if err != nil {
		t.Fatalf("reference DDL: %v", err)
	}
	ref := pgtest.NewDatabase(t, false)
	rc, err := pgx.Connect(ctx, ref.DSN(pgtest.Super))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close(ctx) }()
	if _, err := rc.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply the reference DDL: %v", err)
	}
	mine := pgtest.NewDatabase(t, true)
	mc, err := pgx.Connect(ctx, mine.DSN(pgtest.Super))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mc.Close(ctx) }()

	a, b := fingerprint(t, rc), fingerprint(t, mc)
	var diff []string
	for k := range a {
		if _, ok := b[k]; !ok {
			diff = append(diff, "only in the reference: "+k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			diff = append(diff, "only in the migrations: "+k)
		}
	}
	// The functions the review rounds corrected on purpose (each cites its finding in the migration): the readiness
	// check (F1), the ownership trigger (F5), the exclusive-taker comments (F4), the cleanup (F15) and the new shared
	// table list (F5). Everything else must be byte-identical.
	corrected := []string{"function engram_move_indexes_valid(", "function engram_check_ownership(",
		"function engram_cleanup_namespace(", "function engram_ns_fence_exclusive(",
		"function engram_derivation_lock_exclusive(", "function engram_cleanup_order(", "fnacl engram_cleanup_order("}
	var unexpected []string
	for _, d := range diff {
		skip := false
		for _, c := range corrected {
			if strings.Contains(d, ": "+c) {
				skip = true
			}
		}
		if !skip {
			unexpected = append(unexpected, d)
		}
	}
	diff = unexpected
	sort.Strings(diff)
	allowed := []string{
		"only in the migrations: schema public|engram_migrate CREATE",
		"only in the migrations: table deletion_log|reloptions={fillfactor=100}",
		"only in the reference: table deletion_log|reloptions=<null>",
	}
	if strings.Join(diff, "\n") != strings.Join(allowed, "\n") {
		t.Fatalf("catalog differences between the migrations and the reference DDL:\n%s\nwant exactly:\n%s",
			strings.Join(diff, "\n"), strings.Join(allowed, "\n"))
	}
	t.Logf("%d catalog facts compared", len(a))
}

// own selects the relations that belong to the schema (not goose's, not an extension's, not owned by a column).
const own = `c.relnamespace = 'public'::regnamespace AND c.relname NOT LIKE 'goose%'
	AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i'))`

// fingerprint renders the catalog of schema public as a set of "kind key|value" lines. Owners and grantors are left out
// (the reference file is applied by a superuser, the migrations hand ownership to engram_migrate); everything else a
// consumer can observe is in.
func fingerprint(t *testing.T, c *pgx.Conn) map[string]bool {
	t.Helper()
	ctx := context.Background()
	out := map[string]bool{}
	add := func(format string, args ...any) { out[fmt.Sprintf(format, args...)] = true }
	q := func(sql string, each func(r pgx.Rows)) {
		t.Helper()
		rows, err := c.Query(ctx, sql)
		if err != nil {
			t.Fatalf("fingerprint query: %v\n%s", err, sql)
		}
		defer rows.Close()
		for rows.Next() {
			each(rows)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	scan := func(r pgx.Rows, dest ...any) {
		if err := r.Scan(dest...); err != nil {
			t.Fatal(err)
		}
	}
	q(`SELECT c.relname, c.relkind::text, c.relispartition, c.relrowsecurity, c.relforcerowsecurity,
		coalesce(c.reloptions::text, '<null>'), coalesce(obj_description(c.oid, 'pg_class'), '<null>'),
		coalesce(pg_get_expr(c.relpartbound, c.oid), '<none>')
		FROM pg_class c WHERE `+own+` AND c.relkind IN ('r', 'p', 'v', 'S')`, func(r pgx.Rows) {
		var name, kind, opts, comment, bound string
		var part, rls, force bool
		scan(r, &name, &kind, &part, &rls, &force, &opts, &comment, &bound)
		add("table %s|kind=%s partition=%v rls=%v force=%v bound=%s", name, kind, part, rls, force, bound)
		add("table %s|reloptions=%s", name, opts)
		add("table %s|comment=%s", name, comment)
	})
	q(`SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.atthasdef,
		a.attgenerated::text, coalesce(pg_get_expr(d.adbin, d.adrelid), ''), a.attstorage::text
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE `+own+` AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped`, func(r pgx.Rows) {
		var tbl, col, typ, gen, def, storage string
		var nn, hasdef bool
		scan(r, &tbl, &col, &typ, &nn, &hasdef, &gen, &def, &storage)
		add("column %s.%s|%s notnull=%v default=%s generated=%q storage=%s", tbl, col, typ, nn, def, gen, storage)
	})
	q(`SELECT c.relname, k.conname, pg_get_constraintdef(k.oid), k.convalidated, k.condeferrable, k.condeferred
		FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid WHERE `+own, func(r pgx.Rows) {
		var tbl, name, def string
		var valid, defer1, defer2 bool
		scan(r, &tbl, &name, &def, &valid, &defer1, &defer2)
		add("constraint %s.%s|%s valid=%v deferrable=%v deferred=%v", tbl, name, def, valid, defer1, defer2)
	})
	q(`SELECT indexname, indexdef FROM pg_indexes
		WHERE schemaname = 'public' AND tablename NOT LIKE 'goose%'`, func(r pgx.Rows) {
		var name, def string
		scan(r, &name, &def)
		add("index %s|%s", name, def)
	})
	q(`SELECT tablename, policyname, permissive, roles::text, cmd, coalesce(qual, ''), coalesce(with_check, '')
		FROM pg_policies WHERE schemaname = 'public'`, func(r pgx.Rows) {
		var tbl, name, perm, roles, cmd, qual, chk string
		scan(r, &tbl, &name, &perm, &roles, &cmd, &qual, &chk)
		add("policy %s.%s|%s %s %s using=%s check=%s", tbl, name, perm, roles, cmd, qual, chk)
	})
	q(`SELECT c.relname, g.tgname, pg_get_triggerdef(g.oid), g.tgenabled::text
		FROM pg_trigger g JOIN pg_class c ON c.oid = g.tgrelid WHERE `+own+` AND NOT g.tgisinternal`, func(r pgx.Rows) {
		var tbl, name, def, en string
		scan(r, &tbl, &name, &def, &en)
		add("trigger %s.%s|%s enabled=%s", tbl, name, def, en)
	})
	q(`SELECT p.oid::regprocedure::text, pg_get_functiondef(p.oid), p.provolatile::text, p.prosecdef,
		coalesce(p.proconfig::text, '')
		FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace
		AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')`, func(r pgx.Rows) {
		var sig, def, vol, cfg string
		var secdef bool
		scan(r, &sig, &def, &vol, &secdef, &cfg)
		add("function %s|%s vol=%s secdef=%v config=%s", sig, def, vol, secdef, cfg)
	})
	q(`SELECT c.relname, pg_get_viewdef(c.oid), coalesce(c.reloptions::text, '')
		FROM pg_class c WHERE `+own+` AND c.relkind = 'v'`, func(r pgx.Rows) {
		var name, def, opts string
		scan(r, &name, &def, &opts)
		add("view %s|%s options=%s", name, def, opts)
	})
	q(`SELECT t.typname, string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
		FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid
		WHERE t.typnamespace = 'public'::regnamespace GROUP BY 1`, func(r pgx.Rows) {
		var name, labels string
		scan(r, &name, &labels)
		add("enum %s|%s", name, labels)
	})
	q(`SELECT c.relname, format_type(s.seqtypid, NULL), s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache,
		s.seqcycle FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid WHERE `+own, func(r pgx.Rows) {
		var name, typ string
		var start, inc, max, min, cache int64
		var cycle bool
		scan(r, &name, &typ, &start, &inc, &max, &min, &cache, &cycle)
		add("sequence %s|%s start=%d increment=%d max=%d min=%d cache=%d cycle=%v",
			name, typ, start, inc, max, min, cache, cycle)
	})
	q(`SELECT coalesce(r.rolname, 'PUBLIC'), a.privilege_type
		FROM pg_namespace n, LATERAL aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
		LEFT JOIN pg_roles r ON r.oid = a.grantee
		WHERE n.nspname = 'public' AND a.grantee <> n.nspowner`, func(r pgx.Rows) {
		var grantee, priv string
		scan(r, &grantee, &priv)
		add("schema public|%s %s", grantee, priv)
	})
	// privileges per grantee (the grantor differs: owner vs superuser)
	q(`SELECT c.relname, c.relkind::text, coalesce(r.rolname, 'PUBLIC'),
		string_agg(DISTINCT a.privilege_type, ',' ORDER BY a.privilege_type)
		FROM pg_class c, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles r ON r.oid = a.grantee
		WHERE `+own+` AND c.relkind IN ('r', 'p', 'v', 'S') AND a.grantee <> c.relowner
		GROUP BY 1, 2, 3`, func(r pgx.Rows) {
		var name, kind, grantee, privs string
		scan(r, &name, &kind, &grantee, &privs)
		add("acl %s|%s %s", name, grantee, privs)
	})
	q(`SELECT c.relname, a.attname, coalesce(r.rolname, 'PUBLIC'),
		string_agg(DISTINCT x.privilege_type, ',' ORDER BY x.privilege_type)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid,
		LATERAL aclexplode(a.attacl) x LEFT JOIN pg_roles r ON r.oid = x.grantee
		WHERE `+own+` AND a.attacl IS NOT NULL AND x.grantee <> c.relowner
		GROUP BY 1, 2, 3`, func(r pgx.Rows) {
		var tbl, col, grantee, privs string
		scan(r, &tbl, &col, &grantee, &privs)
		add("colacl %s.%s|%s %s", tbl, col, grantee, privs)
	})
	q(`SELECT p.oid::regprocedure::text, coalesce(r.rolname, 'PUBLIC'), x.privilege_type
		FROM pg_proc p, LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
		LEFT JOIN pg_roles r ON r.oid = x.grantee
		WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE 'engram\_%'
		AND x.grantee <> p.proowner`, func(r pgx.Rows) {
		var sig, grantee, priv string
		scan(r, &sig, &grantee, &priv)
		add("fnacl %s|%s %s", sig, grantee, priv)
	})
	return out
}
