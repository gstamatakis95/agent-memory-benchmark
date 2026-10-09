//go:build integration

package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/store/pgindex"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// TestIndex_PartitionedProcedure (PLAN.md 3.3.4, 9.2, N138): an index added to a 16-partition table through the
// procedure that `engramctl index build` runs: CREATE INDEX ... ON ONLY the parent (invalid until the last partition
// attaches), CREATE INDEX CONCURRENTLY on each partition, ALTER INDEX ... ATTACH PARTITION; resumable, an INVALID
// leftover of an interrupted build is dropped first; and the per-namespace partial HNSW statements of
// engram_hnsw_ddl are executed the way the index runner would (never IF NOT EXISTS).
func TestIndex_PartitionedProcedure(t *testing.T) {
	seeded(t)
	ctx := context.Background()
	r := &pgindex.Runner{DSN: pgtest.DSN(pgtest.Migrate)}
	admin := connect(t, pgtest.Admin)
	mig := connect(t, pgtest.Migrate)
	spec := pgindex.Spec{Parent: "fact_links", Name: "fact_links_test_weight_idx", Columns: "namespace_id, weight"}

	state := func() (parentValid bool, attached, valid int) {
		t.Helper()
		if err := admin.QueryRow(ctx,
			`SELECT coalesce((SELECT indisvalid FROM pg_index WHERE indexrelid = $1::regclass), false)`,
			spec.Name).Scan(&parentValid); err != nil {
			t.Fatalf("parent index state: %v", err)
		}
		if err := admin.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE x.indisvalid) FROM pg_inherits i
			 JOIN pg_index x ON x.indexrelid = i.inhrelid WHERE i.inhparent = $1::regclass`, spec.Name).Scan(&attached,
			&valid); err != nil {
			t.Fatalf("partition index state: %v", err)
		}
		return
	}

	t.Run("statements_come_in_the_prescribed_order", func(t *testing.T) {
		stmts, err := r.Statements(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		if len(stmts) != 1+2*16 {
			t.Fatalf("%d statements, want 33 (ON ONLY + 16 x (CIC + ATTACH))", len(stmts))
		}
		if !strings.HasPrefix(stmts[0], "CREATE INDEX fact_links_test_weight_idx ON ONLY fact_links ") {
			t.Errorf("first statement %q", stmts[0])
		}
		for i := 0; i < 16; i++ {
			if !strings.HasPrefix(stmts[1+2*i], "CREATE INDEX CONCURRENTLY ") || !strings.HasPrefix(stmts[2+2*i],
				"ALTER INDEX ") ||
				!strings.Contains(stmts[2+2*i], "ATTACH PARTITION") {
				t.Errorf("statements %d/%d: %q / %q", 1+2*i, 2+2*i, stmts[1+2*i], stmts[2+2*i])
			}
		}
	})

	t.Run("concurrently_on_the_parent_is_refused", func(t *testing.T) {
		_, err := mig.Exec(ctx,
			"CREATE INDEX CONCURRENTLY fact_links_parent_cic_idx ON fact_links (namespace_id, weight)")
		if sqlState(err) != "0A000" {
			t.Fatalf("CIC on a partitioned parent: want 0A000, got %v", err)
		}
	})

	t.Run("build_attaches_every_partition", func(t *testing.T) {
		if err := r.BuildPartitioned(ctx, spec); err != nil {
			t.Fatal(err)
		}
		if ok, attached, valid := state(); !ok || attached != 16 || valid != 16 {
			t.Fatalf("parent valid=%v attached=%d valid partition indexes=%d, want true/16/16", ok, attached, valid)
		}
		if inv, err := r.Status(ctx); err != nil || len(inv) != 0 {
			t.Fatalf("invalid indexes after a clean build: %v (err %v)", inv, err)
		}
	})

	t.Run("rerun_is_a_no_op", func(t *testing.T) {
		if err := r.BuildPartitioned(ctx, spec); err != nil {
			t.Fatal(err)
		}
		if ok, attached, valid := state(); !ok || attached != 16 || valid != 16 {
			t.Fatalf("after a re-run: %v/%d/%d", ok, attached, valid)
		}
	})

	t.Run("invalid_leftover_is_dropped_and_rebuilt", func(t *testing.T) {
		if err := r.DropPartitioned(ctx, spec.Name); err != nil {
			t.Fatal(err)
		}
		// a failed CREATE UNIQUE INDEX CONCURRENTLY leaves an INVALID index under the very name the procedure will
		// want: two links of the fixtures share a weight, so the unique build fails
		if _, err := admin.Exec(ctx,
			`INSERT INTO fact_links (namespace_id, tenant_id, src_memory_id, dst_memory_id, link_type)
			 VALUES ($1::uuid, $2, md5($1::text || 'f1')::uuid, md5($1::text || 'f3')::uuid, 'causal')`, nsA,
			tenantAcme); err != nil {
			t.Fatalf("seed a second link of the same weight: %v", err)
		}
		t.Cleanup(func() { // the fixtures are shared: leave namespace A as it was
			_, _ = admin.Exec(context.Background(), `DELETE FROM fact_links WHERE namespace_id = $1::uuid
				AND dst_memory_id = md5($1::text || 'f3')::uuid`, nsA)
		})
		var part string
		if err := admin.QueryRow(ctx,
			`SELECT tableoid::regclass::text FROM fact_links WHERE namespace_id = $1 LIMIT 1`,
			nsA).Scan(&part); err != nil {
			t.Fatal(err)
		}
		bad := spec.Name + "_" + part
		_, err := mig.Exec(ctx, "CREATE UNIQUE INDEX CONCURRENTLY "+bad+" ON "+part+" (weight)")
		if err == nil {
			t.Fatalf("the unique build was expected to fail on duplicate weights")
		}
		inv, err := r.Status(ctx)
		if err != nil || len(inv) != 1 || inv[0].Index != bad {
			t.Fatalf("invalid indexes = %v (err %v), want just %s", inv, err, bad)
		}
		if err := r.BuildPartitioned(ctx, spec); err != nil {
			t.Fatalf("rebuild over an invalid leftover: %v", err)
		}
		if ok, attached, valid := state(); !ok || attached != 16 || valid != 16 {
			t.Fatalf("after the rebuild: %v/%d/%d", ok, attached, valid)
		}
		if inv, _ := r.Status(ctx); len(inv) != 0 {
			t.Fatalf("invalid indexes remain: %v", inv)
		}
		// the new index really is a unique-free ordinary index on the weight column
		var def string
		if err := admin.QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE indexname = $1`,
			bad).Scan(&def); err != nil ||
			strings.Contains(def, "UNIQUE") {
			t.Fatalf("partition index definition %q (err %v)", def, err)
		}
		if err := r.DropPartitioned(ctx, spec.Name); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("only_the_owner_issues_index_ddl", func(t *testing.T) {
		// engram_admin cannot create, reindex or drop an index (N133e, N138): it does not own the tables
		_, err := admin.Exec(ctx, "CREATE INDEX admin_forbidden_idx ON fact_links_p00 (weight)")
		if sqlState(err) != "42501" {
			t.Errorf("CREATE INDEX as engram_admin: want 42501, got %v", err)
		}
		app := connect(t, pgtest.App)
		if _, err := app.Exec(ctx,
			"CREATE INDEX app_forbidden_idx ON fact_links_p00 (weight)"); sqlState(err) != "42501" {
			t.Errorf("CREATE INDEX as engram_app: want 42501, got %v", err)
		}
	})

	t.Run("hnsw_per_namespace_lifecycle", func(t *testing.T) {
		stateOf := func() string {
			var st string
			if err := admin.QueryRow(ctx,
				`SELECT index_state FROM engram_hnsw_ddl('fact_vectors', $1::uuid, $2, 'create')`,
				nsA, pgtest.SeedModel).Scan(&st); err != nil {
				t.Fatal(err)
			}
			return st
		}
		if st := stateOf(); st != "absent" {
			t.Fatalf("index state before the build: %s", st)
		}
		ran, err := r.HNSW(ctx, "fact_vectors", nsA, pgtest.SeedModel, "create")
		if err != nil || len(ran) != 1 || !strings.HasPrefix(ran[0], "CREATE INDEX CONCURRENTLY fv_") ||
			!strings.Contains(ran[0], "USING hnsw (embedding halfvec_cosine_ops)") || !strings.Contains(ran[0],
			"WHERE namespace_id = '"+nsA+"'") {
			t.Fatalf("create ran %v (err %v)", ran, err)
		}
		if st := stateOf(); st != "valid" {
			t.Fatalf("index state after the build: %s", st)
		}
		if ran, err := r.HNSW(ctx, "fact_vectors", nsA, pgtest.SeedModel, "create"); err != nil || len(ran) != 0 {
			t.Fatalf("a second create must be a no-op, ran %v (err %v)", ran, err)
		}
		// deterministic name: rebuild is REINDEX INDEX CONCURRENTLY, drop is DROP INDEX CONCURRENTLY
		if ran, err := r.HNSW(ctx, "fact_vectors", nsA, pgtest.SeedModel, "rebuild"); err != nil || len(ran) != 1 ||
			!strings.HasPrefix(ran[0], "REINDEX INDEX CONCURRENTLY fv_") {
			t.Fatalf("rebuild ran %v (err %v)", ran, err)
		}
		if ran, err := r.HNSW(ctx, "fact_vectors", nsA, pgtest.SeedModel, "drop"); err != nil || len(ran) != 1 ||
			!strings.HasPrefix(ran[0], "DROP INDEX CONCURRENTLY IF EXISTS fv_") {
			t.Fatalf("drop ran %v (err %v)", ran, err)
		}
		if st := stateOf(); st != "absent" {
			t.Fatalf("index state after the drop: %s", st)
		}
		if _, err := r.HNSW(ctx, "facts", nsA, pgtest.SeedModel,
			"create"); err == nil || !strings.Contains(err.Error(), "not a vector table") {
			t.Fatalf("a non-vector table must be refused: %v", err)
		}
	})
}
