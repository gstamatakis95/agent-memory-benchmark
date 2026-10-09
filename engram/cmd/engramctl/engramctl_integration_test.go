//go:build integration

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// TestEngramctlMigrate drives `engramctl migrate` end to end on an empty database: status, dry-run, up, the static RLS
// check (which must print zero rows), down and up again.
func TestEngramctlMigrate(t *testing.T) {
	ctx := context.Background()
	db := pgtest.NewDatabase(t, false)
	dsn := db.DSN(pgtest.Super)
	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := migrateMain(ctx, append([]string{"--dsn", dsn}, args...), &out)
		return out.String(), err
	}

	out, err := run("status")
	if err != nil || strings.Count(out, "pending") != 4 {
		t.Fatalf("status of an empty database: %q (err %v)", out, err)
	}
	out, err = run("--dry-run")
	if err != nil || strings.Count(out, "would apply") != 4 {
		t.Fatalf("dry run: %q (err %v)", out, err)
	}
	if out, err = run("status"); err != nil || strings.Count(out, "pending") != 4 {
		t.Fatalf("a dry run must change nothing: %q (err %v)", out, err)
	}
	out, err = run()
	if err != nil || strings.Count(out, "applied 000") != 4 {
		t.Fatalf("up: %q (err %v)", out, err)
	}
	if out, err = run("--check-rls"); err != nil || out != "" {
		t.Fatalf("--check-rls must return zero rows, got %q (err %v)", out, err)
	}
	if _, err = run("down"); err == nil {
		t.Fatal("down without --to must be refused")
	}
	if out, err = run("--to", "0", "down"); err != nil || strings.Count(out, "reverted") != 4 {
		t.Fatalf("down: %q (err %v)", out, err)
	}
	if out, err = run(); err != nil || strings.Count(out, "applied 000") != 4 {
		t.Fatalf("second up: %q (err %v)", out, err)
	}

	// tamper: the check prints the offending relation and fails
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	// shard_meta.schema_version follows the goose version (PLAN.md 9.2; M0.2 review F10): provisioning wrote a stale 1
	if _, err := c.Exec(ctx, "INSERT INTO shard_meta (shard_id, schema_version) VALUES (3, 1)"); err != nil {
		t.Fatal(err)
	}
	if _, err = run(); err != nil {
		t.Fatal(err)
	}
	var sv int
	if err := c.QueryRow(ctx, "SELECT schema_version FROM shard_meta").Scan(&sv); err != nil || sv != 4 {
		t.Fatalf("shard_meta.schema_version = %d (err %v), want 4", sv, err)
	}
	// roles are cluster-wide: undo the membership plant on a fresh connection (c is closed by its defer before
	// t.Cleanup runs) before anything else uses the server
	// Known limit (M0.4 review n4): unique names remove the CREATE ROLE collision, but this test still grants its
	// helper role to the shared engram_relay for the length of the test. A concurrent migrate.Up on the SAME server
	// (one ENGRAM_TEST_PG_DSN shared by packages run with -p 4) can fail its self-check 4 inside that window. With one
	// container per package, the default, that cannot happen; on a shared server run these two packages with -p 1.
	mid := pgtest.RoleName("engram_test_mid")
	t.Cleanup(func() {
		fresh, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Errorf("revert: %v", err)
			return
		}
		defer func() { _ = fresh.Close(context.Background()) }()
		for _, q := range []string{"REVOKE " + mid + " FROM engram_relay", "DROP ROLE IF EXISTS " + mid} {
			if _, err := fresh.Exec(context.Background(), q); err != nil {
				t.Errorf("revert %s: %v", q, err)
			}
		}
	})
	for _, q := range []string{
		"ALTER TABLE operations NO FORCE ROW LEVEL SECURITY",
		// transitive membership of a BYPASSRLS role, and a view owned by the BYPASSRLS owner that app can read
		"CREATE ROLE " + mid + " NOLOGIN", "GRANT engram_admin TO " + mid,
		"GRANT " + mid + " TO engram_relay",
		"CREATE VIEW leak2 AS SELECT * FROM ingest_ledger", "ALTER VIEW leak2 OWNER TO engram_migrate",
		"GRANT SELECT ON leak2 TO engram_app",
	} {
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	out, err = run("--check-rls")
	for _, want := range []string{
		"operations\trow level security is not forced",
		"engram_relay\trole is a member (directly or transitively) of the BYPASSRLS role engram_admin",
		"leak2\tview readable by engram_app without security_invoker (reads past RLS)",
	} {
		if err == nil || !strings.Contains(out, want) {
			t.Errorf("tampered --check-rls: missing %q in %q (err %v)", want, out, err)
		}
	}
}

// TestEngramctlIndex builds an index on a partitioned table through the CLI and lists the invalid ones.
func TestEngramctlIndex(t *testing.T) {
	ctx := context.Background()
	db := pgtest.NewDatabase(t, true)
	dsn := db.DSN(pgtest.Migrate)
	var out bytes.Buffer
	err := indexMain(ctx, []string{"build", "--dsn", dsn, "--parent", "entity_mentions", "--name",
		"entity_mentions_cli_idx",
		"--columns", "namespace_id, mentioned_at"}, &out)
	if err != nil || !strings.Contains(out.String(), "built and attached") {
		t.Fatalf("index build: %q (err %v)", out.String(), err)
	}
	c, err := pgx.Connect(ctx, db.DSN(pgtest.Super))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(ctx) }()
	var n int
	const attached = `SELECT count(*) FROM pg_inherits WHERE inhparent = 'entity_mentions_cli_idx'::regclass`
	if err := c.QueryRow(ctx, attached).Scan(&n); err != nil || n != 16 {
		t.Fatalf("attached partition indexes = %d (err %v), want 16", n, err)
	}
	out.Reset()
	if err := indexMain(ctx, []string{"status", "--dsn", dsn}, &out); err != nil || out.Len() != 0 {
		t.Fatalf("index status: %q (err %v)", out.String(), err)
	}
	out.Reset()
	if err := indexMain(ctx, []string{"hnsw", "--dsn", dsn, "--table", "fact_vectors", "--namespace",
		"0190c000-0000-7000-8000-0000000000aa", "--model", pgtest.SeedModel}, &out); err != nil ||
		!strings.Contains(out.String(), "USING hnsw") {
		t.Fatalf("index hnsw: %q (err %v)", out.String(), err)
	}
	if err := indexMain(ctx, []string{"drop", "--dsn", dsn, "--name", "entity_mentions_cli_idx"}, &out); err != nil {
		t.Fatal(err)
	}
	if err := indexMain(ctx, []string{"bogus", "--dsn", dsn}, &out); err == nil {
		t.Fatal("unknown subcommand must fail")
	}
}
