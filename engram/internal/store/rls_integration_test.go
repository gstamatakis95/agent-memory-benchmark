//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gstamatakis95/engram/internal/store/migrate"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

func sqlState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// TestEveryTableHasRLS is the static check of PLAN.md section 8.3 (also `engramctl migrate --check-rls`): zero rows on
// the migrated database, rows on a database that has been tampered with (so the check is known to bite), and the
// application roles cannot bypass RLS.
func TestEveryTableHasRLS(t *testing.T) {
	ctx := context.Background()
	conn := connect(t, pgtest.Admin)
	vs, err := migrate.CheckRLS(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 0 {
		t.Fatalf("--check-rls must return zero rows, got %v", vs)
	}

	// rolbypassrls = false for the three application roles, none is a member of a BYPASSRLS role, and the two
	// BYPASSRLS roles are exactly engram_admin and the owner engram_migrate.
	rows, err := conn.Query(ctx, `SELECT rolname, rolbypassrls, rolsuper FROM pg_roles WHERE rolname LIKE 'engram\_%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	bypass := map[string]bool{}
	for rows.Next() {
		var name string
		var by, super bool
		if err := rows.Scan(&name, &by, &super); err != nil {
			t.Fatal(err)
		}
		bypass[name] = by
		if super {
			t.Errorf("role %s is a superuser", name)
		}
	}
	rows.Close()
	for _, r := range []string{"engram_app", "engram_relay", "engram_move", "engram_stats_reader"} {
		if v, ok := bypass[r]; !ok || v {
			t.Errorf("role %s: exists=%v bypassrls=%v, want exists and NOBYPASSRLS", r, ok, v)
		}
	}
	for _, r := range []string{"engram_admin", "engram_migrate"} {
		if !bypass[r] {
			t.Errorf("role %s must be BYPASSRLS (N133e)", r)
		}
	}
	for r := range bypass {
		switch r {
		case "engram_app", "engram_relay", "engram_move", "engram_stats_reader", "engram_admin", "engram_migrate":
		default:
			t.Errorf("unexpected role %s (N133e: no worker or expunge role)", r)
		}
	}
	// the policy renders as the string the static check compares against
	var qual, check string
	if err := conn.QueryRow(ctx, `SELECT qual, with_check FROM pg_policies
		 WHERE tablename = 'facts' AND policyname = 'ns_isolation'`).Scan(&qual, &check); err != nil {
		t.Fatal(err)
	}
	const want = "(namespace_id = (current_setting('engram.namespace_id'::text))::uuid)"
	if qual != want || check != want {
		t.Errorf("ns_isolation renders as %q / %q, want %q", qual, check, want)
	}

	// The check bites: tamper with a throwaway database and expect exactly the planted problems.
	db := pgtest.NewDatabase(t, true)
	sup, err := pgx.Connect(ctx, db.DSN(pgtest.Super))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sup.Close(ctx) }()
	// roles are cluster-wide, so the helper role is unique per run (parallel packages share one server) and the
	// BYPASSRLS tamper is undone before anything else can run against engram_app; the undo uses a fresh connection
	// because sup is already closed when t.Cleanup runs
	// Known limit (M0.4 review n4): unique names remove the CREATE ROLE collision, but this test still grants its
	// helper role to the shared engram_relay (and sets engram_app BYPASSRLS) for the length of the test. A concurrent
	// migrate.Up on the SAME server (one ENGRAM_TEST_PG_DSN shared by packages run with -p 4) can fail its self-check
	// 4 inside that window. With one container per package, the default, that cannot happen; on a shared server run
	// these two packages with -p 1.
	mid := pgtest.RoleName("engram_test_mid")
	revert := func() error {
		fresh, err := pgx.Connect(context.Background(), db.DSN(pgtest.Super))
		if err != nil {
			return err
		}
		defer func() { _ = fresh.Close(context.Background()) }()
		for _, q := range []string{`ALTER ROLE engram_app NOBYPASSRLS`, `REVOKE ` + mid + ` FROM engram_relay`,
			`DROP ROLE IF EXISTS ` + mid} {
			if _, err := fresh.Exec(context.Background(), q); err != nil {
				return err
			}
		}
		return nil
	}
	t.Cleanup(func() { _ = revert() })
	for _, q := range []string{
		`ALTER TABLE documents NO FORCE ROW LEVEL SECURITY`,
		`ALTER TABLE tag_counts DISABLE ROW LEVEL SECURITY`,
		`DROP POLICY ns_isolation ON entity_aliases`,
		`CREATE POLICY sneaky ON facts_p03 USING (true)`,
		`CREATE TABLE stray_without_namespace (x int)`,
		`ALTER ROLE engram_app BYPASSRLS`,
		// F3: transitive membership of a BYPASSRLS role (relay -> mid -> admin) and a definer view past RLS
		`CREATE ROLE ` + mid + ` NOLOGIN`,
		`GRANT engram_admin TO ` + mid,
		`GRANT ` + mid + ` TO engram_relay`,
		`CREATE VIEW leak2 AS SELECT * FROM ingest_ledger`,
		`ALTER VIEW leak2 OWNER TO engram_migrate`,
		`GRANT SELECT ON leak2 TO engram_app`,
	} {
		if _, err := sup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	vs, err = migrate.CheckRLS(ctx, sup)
	if rerr := revert(); rerr != nil {
		t.Fatal(rerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, v := range vs {
		got[v.String()] = true
	}
	for _, w := range []string{
		"documents\trow level security is not forced",
		"tag_counts\trow level security is not enabled",
		"entity_aliases\tpolicy ns_isolation is missing or its qualifier differs",
		"facts_p03\tunexpected policy sneaky",
		"stray_without_namespace\thas no namespace_id column and is not on the shard-level allowlist",
		"engram_app\trole may bypass row level security",
		"engram_relay\trole is a member (directly or transitively) of the BYPASSRLS role engram_admin",
		"leak2\tview readable by engram_app without security_invoker (reads past RLS)",
	} {
		if !got[w] {
			t.Errorf("tampered database: expected violation %q, got %v", w, vs)
		}
	}
}

// TestRLSCanary: a second namespace's rows are invisible to engram_app on every namespace-scoped table (PLAN.md section
// 8.3). For each relation: reads in a scope that owns nothing return zero rows while the owning scope returns rows (the
// control that proves the query would have returned something), writes naming another namespace are refused by WITH
// CHECK, UPDATE/DELETE touch zero rows, and an unset or empty scope errors instead of leaking.
func TestRLSCanary(t *testing.T) {
	seeded(t)
	ctx := context.Background()
	admin := connect(t, pgtest.Admin)
	rows, err := admin.Query(ctx, `
		SELECT c.relname::text, c.relkind::text, c.relispartition,
		       has_table_privilege('engram_app', c.oid, 'SELECT'), has_table_privilege('engram_app', c.oid, 'INSERT'),
		       has_table_privilege('engram_app', c.oid, 'UPDATE'), has_table_privilege('engram_app', c.oid, 'DELETE')
		  FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT
		    a.attisdropped
		 WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	type rel struct {
		name               string
		partition          bool
		sel, ins, upd, del bool
	}
	var rels []rel
	for rows.Next() {
		var r rel
		var kind string
		if err := rows.Scan(&r.name, &kind, &r.partition, &r.sel, &r.ins, &r.upd, &r.del); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r)
	}
	rows.Close()
	if len(rels) < 60 {
		t.Fatalf("only %d namespace-scoped relations found; the canary would be vacuous", len(rels))
	}
	tables := 0
	for _, r := range rels {
		q := pgx.Identifier{r.name}.Sanitize()
		t.Run(r.name, func(t *testing.T) {
			if r.partition {
				// partitions stay ungranted: a direct reference is refused before RLS is consulted
				tx := scopedTx(t, pgtest.App, scope(nsA, tenantAcme))
				var n int
				err := tx.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n)
				if sqlState(err) != "42501" {
					t.Fatalf("direct partition read as engram_app: want 42501, got %v", err)
				}
				return
			}
			tables++
			if !r.sel {
				tx := scopedTx(t, pgtest.App, scope(nsA, tenantAcme))
				var n int
				if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n); sqlState(err) != "42501" {
					t.Fatalf("engram_app has no SELECT here: want 42501, got %v", err)
				}
				return
			}
			count := func(sc pgtest.Scope, where string) int {
				tx := scopedTx(t, pgtest.App, sc)
				var n int
				if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+q+where).Scan(&n); err != nil {
					t.Fatalf("count under %s: %v", sc.Namespace, err)
				}
				return n
			}
			ownA := count(scope(nsA, tenantAcme), "")
			if ownA == 0 {
				t.Fatalf("the owning namespace sees no rows in %s: the canary would prove nothing", r.name)
			}
			// the empty namespace is real: it has its ownership, stats and models rows and nothing else
			wantEmpty := map[string]int{"namespace_ownership": 1, "namespace_stats": 1, "namespace_models": 1}[r.name]
			if n := count(scope(nsEmpty, tenantAcme), ""); n != wantEmpty {
				t.Errorf("namespace that owns nothing sees %d rows, want %d", n, wantEmpty)
			}
			// the same query with an explicit predicate for another namespace is still empty (RLS, not the WHERE)
			if n := count(scope(nsB, tenantZeta), fmt.Sprintf(" WHERE namespace_id = '%s'", nsA)); n != 0 {
				t.Errorf("namespace B sees %d rows of namespace A", n)
			}
			if n := count(scope(nsB, tenantZeta), ""); n == 0 {
				t.Errorf("namespace B sees none of its own rows (A sees %d)", ownA)
			}
			if r.ins {
				var js string
				if err := admin.QueryRow(ctx,
					"SELECT row_to_json(t)::text FROM "+q+" t WHERE namespace_id = $1 LIMIT 1",
					nsA).Scan(&js); err != nil {
					t.Fatalf("fetch a row: %v", err)
				}
				ins := "INSERT INTO " + q + " SELECT * FROM json_populate_record(NULL::" + q + ", $1::json)"
				tx := scopedTx(t, pgtest.App, scope(nsEmpty, tenantAcme))
				_, err := tx.Exec(ctx, ins, js)
				if sqlState(err) != "42501" || !strings.Contains(fmt.Sprint(err), "row-level security") {
					t.Errorf("insert of a namespace-A row under scope %s: want an RLS violation, got %v", nsEmpty, err)
				}
				// control: under the owning scope the same row passes RLS and stops at the duplicate key
				tx = scopedTx(t, pgtest.App, scope(nsA, tenantAcme))
				if _, err := tx.Exec(ctx, ins, js); sqlState(err) != "23505" {
					t.Errorf("insert of the same row under the owning scope: want 23505, got %v", err)
				}
			}
			if r.upd {
				tx := scopedTx(t, pgtest.App, scope(nsEmpty, tenantAcme))
				tag, err := tx.Exec(ctx, "UPDATE "+q+" SET tenant_id = tenant_id WHERE namespace_id = $1", nsA)
				if err != nil || tag.RowsAffected() != 0 {
					t.Errorf("update across namespaces: %v rows, err %v", tag.RowsAffected(), err)
				}
				tx = scopedTx(t, pgtest.App, scope(nsA, tenantAcme))
				if tag, err := tx.Exec(ctx,
					"UPDATE "+q+" SET tenant_id = tenant_id"); err != nil || tag.RowsAffected() == 0 {
					t.Errorf("control update under the owning scope: %v rows, err %v", tag.RowsAffected(), err)
				}
			}
			if r.del {
				tx := scopedTx(t, pgtest.App, scope(nsEmpty, tenantAcme))
				tag, err := tx.Exec(ctx, "DELETE FROM "+q+" WHERE namespace_id = $1", nsA)
				if err != nil || tag.RowsAffected() != 0 {
					t.Errorf("delete across namespaces: %v rows, err %v", tag.RowsAffected(), err)
				}
			}
		})
	}
	if tables < 45 {
		t.Errorf("only %d tables were canaried", tables)
	}

	// fail closed: an unset scope is 42704 (unrecognized configuration parameter), an expired one 22P02.
	c := connect(t, pgtest.App)
	if err := c.QueryRow(ctx, "SELECT count(*) FROM facts").Scan(new(int)); sqlState(err) != "42704" {
		t.Errorf("unset scope: want 42704, got %v", err)
	}
	tx := scopedTx(t, pgtest.App, scope("", tenantAcme))
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM facts").Scan(new(int)); sqlState(err) != "22P02" {
		t.Errorf("empty scope: want 22P02, got %v", err)
	}
}
