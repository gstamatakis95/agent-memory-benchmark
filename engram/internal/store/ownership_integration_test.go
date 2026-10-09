//go:build integration

package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgindex"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

const planTarget = `INSERT INTO namespace_ownership (namespace_id, tenant_id, shard_id, epoch, state, move_id)
	VALUES ($1, $2, $3, 2, 'incoming', $4)`

// moverFor opens a session of role engram_move whose scope GUCs name ns (the move executor works under RLS with the
// namespace in scope), on the database dsn.
func moverFor(t *testing.T, dsn, ns string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	if _, err := c.Exec(ctx, `SELECT set_config('engram.namespace_id', $1, false),
		set_config('engram.tenant_id', 'acme', false), set_config('engram.epoch', '2', false)`, ns); err != nil {
		t.Fatal(err)
	}
	return c
}

const toReady = `UPDATE namespace_ownership SET state = 'ready', w_final = $2, floor_lsn = '0/1'
	WHERE namespace_id = $1`

// TestOwnership_ReadyState: the `ready` state of N125 exists and is guarded by the trigger (the full edge matrix is
// TestIso_Ownership_Transitions of the move milestones; this pins what M0.2 ships). The two traces of the M0.2 review
// (F1) pin N175's readiness clause: a namespace with no vector table at >= 2,000 vectors needs no index and reaches
// ready; a table at >= 2,000 vectors with no request does not, however many other tables are indexed. The last case
// pins the return_move guard over every table the cleanup deletes (F5).
func TestOwnership_ReadyState(t *testing.T) {
	ctx := context.Background()

	t.Run("guards", func(t *testing.T) {
		const ns = "0190c000-0000-7000-8000-0000000000d1"
		const move = "0190c000-0000-7000-8000-0000000000d2"
		admin := connect(t, pgtest.Admin)
		t.Cleanup(func() { // the rollback_target edge of engram_admin deletes an incoming row
			_, _ = admin.Exec(context.Background(), `DELETE FROM namespace_ownership WHERE namespace_id = $1`, ns)
		})
		// engram_app may create only the first active row; a target in `incoming` is the mover's edge
		tx := scopedTx(t, pgtest.App, scope(ns, tenantAcme))
		if _, err := tx.Exec(ctx, planTarget, ns, tenantAcme, pgtest.ShardID, move); sqlState(err) != "42501" {
			t.Errorf("engram_app planning a target: want 42501, got %v", err)
		}
		mv := moverFor(t, pgtest.DSN(pgtest.Move), ns)
		if _, err := mv.Exec(ctx, planTarget, ns, tenantAcme, pgtest.ShardID, move); err != nil {
			t.Fatalf("plan_target: %v", err)
		}
		// incoming without a move id is refused by the table's CHECK
		const other = "0190c000-0000-7000-8000-0000000000d3"
		if _, err := mv.Exec(ctx, `SELECT set_config('engram.namespace_id', $1, false)`, other); err != nil {
			t.Fatal(err)
		}
		if _, err := mv.Exec(ctx, `INSERT INTO namespace_ownership (namespace_id, tenant_id, shard_id, epoch, state)
			VALUES ($1, $2, $3, 2, 'incoming')`, other, tenantAcme, pgtest.ShardID); sqlState(err) != "23514" {
			t.Errorf("incoming without move_id: want 23514, got %v", err)
		}
		if _, err := mv.Exec(ctx, `SELECT set_config('engram.namespace_id', $1, false)`, ns); err != nil {
			t.Fatal(err)
		}
		// ready without w_final and floor_lsn: CHECK
		const bare = `UPDATE namespace_ownership SET state = 'ready' WHERE namespace_id = $1`
		if _, err := mv.Exec(ctx, bare, ns); sqlState(err) != "23514" {
			t.Errorf("ready without w_final: want 23514, got %v", err)
		}
		// w_final not yet passed by this shard's sequence (N147): refused
		if _, err := mv.Exec(ctx, toReady, ns, int64(1)<<60); sqlState(err) != "55006" {
			t.Errorf("ready before the sequence passed w_final: want 55006, got %v", err)
		}
		// the permanent fence value: engram_app cannot delete an ownership row
		tx = scopedTx(t, pgtest.App, scope(ns, tenantAcme))
		_, err := tx.Exec(ctx, `DELETE FROM namespace_ownership WHERE namespace_id = $1`, ns)
		if sqlState(err) != "42501" {
			t.Errorf("engram_app deleting an ownership row: want 42501, got %v", err)
		}
	})

	t.Run("trace_A_a_small_namespace_without_vector_indexes_reaches_ready", func(t *testing.T) {
		const ns = "0190c000-0000-7000-8000-0000000000e2"
		const move = "0190c000-0000-7000-8000-0000000000e3"
		admin := connect(t, pgtest.Admin)
		t.Cleanup(func() {
			_, _ = admin.Exec(context.Background(), `DELETE FROM namespace_ownership WHERE namespace_id = $1`, ns)
		})
		mv := moverFor(t, pgtest.DSN(pgtest.Move), ns)
		if _, err := mv.Exec(ctx, planTarget, ns, tenantAcme, pgtest.ShardID, move); err != nil {
			t.Fatal(err)
		}
		var ok bool
		if err := admin.QueryRow(ctx, `SELECT engram_move_indexes_valid($1::uuid)`, ns).Scan(&ok); err != nil || !ok {
			t.Fatalf("engram_move_indexes_valid of a namespace without vectors = %v (err %v), want true", ok, err)
		}
		if _, err := mv.Exec(ctx, toReady, ns, int64(0)); err != nil {
			t.Fatalf("ready_target of a small namespace: %v", err)
		}
		var state string
		err := admin.QueryRow(ctx, `SELECT state::text FROM namespace_ownership WHERE namespace_id = $1`,
			ns).Scan(&state)
		if err != nil || state != "ready" {
			t.Fatalf("state = %q (err %v), want ready", state, err)
		}
	})

	t.Run("trace_B_a_large_table_without_a_request_blocks_ready", func(t *testing.T) {
		db := pgtest.NewDatabase(t, true)
		const ns = "0190c000-0000-7000-8000-0000000000f1"
		const move = "0190c000-0000-7000-8000-0000000000f2"
		mv := moverFor(t, db.DSN(pgtest.Move), ns)
		if _, err := mv.Exec(ctx, planTarget, ns, tenantAcme, pgtest.ShardID, move); err != nil {
			t.Fatal(err)
		}
		admin, err := pgx.Connect(ctx, db.DSN(pgtest.Admin))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = admin.Close(ctx) }()
		if _, err := admin.Exec(ctx, `INSERT INTO namespace_models (namespace_id, tenant_id, embedding_model)
			VALUES ($1, $2, $3)`, ns, tenantAcme, pgtest.SeedModel); err != nil {
			t.Fatal(err)
		}
		// 2,500 fact_vectors and 2,500 chunk_vectors (one chunk per document, one fact per chunk)
		pgtest.SeedBulk(ctx, t, pgtest.BulkFacts{Namespace: ns, Tenant: tenantAcme, Documents: 2500, PerDoc: 1,
			Vectors: true, ChunkVectors: true, DB: &db})
		valid := func() bool {
			t.Helper()
			var ok bool
			if err := admin.QueryRow(ctx, `SELECT engram_move_indexes_valid($1::uuid)`, ns).Scan(&ok); err != nil {
				t.Fatal(err)
			}
			return ok
		}
		ready := func() error {
			_, err := mv.Exec(ctx, toReady, ns, int64(0))
			return err
		}
		runner := &pgindex.Runner{DSN: db.DSN(pgtest.Migrate)}
		request := func(table string) {
			t.Helper()
			_, err := admin.Exec(ctx, `INSERT INTO vector_indexes (namespace_id, tenant_id, vector_table,
				embedding_model, state) VALUES ($1, $2, $3, $4, 'ready')`, ns, tenantAcme, table, pgtest.SeedModel)
			if err != nil {
				t.Fatal(err)
			}
		}
		// no request at all: both tables are required and neither is requested
		if valid() {
			t.Fatal("engram_move_indexes_valid is true with two unrequested tables at 2,500 vectors")
		}
		// only fact_vectors requested and built: chunk_vectors (2,500) still has no request
		request("fact_vectors")
		if _, err := runner.HNSW(ctx, "fact_vectors", ns, pgtest.SeedModel, "create"); err != nil {
			t.Fatal(err)
		}
		if valid() {
			t.Fatal("engram_move_indexes_valid is true while chunk_vectors (2,500 vectors) has no request")
		}
		err = ready()
		if sqlState(err) != "55006" || !strings.Contains(err.Error(), "vector indexes") {
			t.Fatalf("ready with an unrequested large table: want 55006 (vector indexes), got %v", err)
		}
		// a request whose index was never built is not enough either
		request("chunk_vectors")
		if valid() {
			t.Fatal("engram_move_indexes_valid is true with a request whose index is absent")
		}
		if _, err := runner.HNSW(ctx, "chunk_vectors", ns, pgtest.SeedModel, "create"); err != nil {
			t.Fatal(err)
		}
		// a request being dropped does not count
		if _, err := admin.Exec(ctx, `UPDATE vector_indexes SET state = 'dropping' WHERE namespace_id = $1
			AND vector_table = 'fact_vectors'`, ns); err != nil {
			t.Fatal(err)
		}
		if valid() {
			t.Fatal("engram_move_indexes_valid is true while the fact_vectors request is dropping")
		}
		if _, err := admin.Exec(ctx, `UPDATE vector_indexes SET state = 'ready' WHERE namespace_id = $1
			AND vector_table = 'fact_vectors'`, ns); err != nil {
			t.Fatal(err)
		}
		if !valid() {
			t.Fatal("engram_move_indexes_valid is false with both tables requested and built")
		}
		if err := ready(); err != nil {
			t.Fatalf("ready_target with both indexes valid: %v", err)
		}
	})

	t.Run("return_move_guard_covers_every_table_the_cleanup_deletes", func(t *testing.T) {
		// N125: return_move is refused 55006 while data rows remain; N170(3): the cleanup empties content before
		// markers, so an interrupted cleanup leaves markers. The guard must see them (M0.2 review F5).
		db := pgtest.NewDatabase(t, true)
		const ns = "0190c000-0000-7000-8000-0000000000f5"
		admin, err := pgx.Connect(ctx, db.DSN(pgtest.Admin))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = admin.Close(ctx) }()
		// an active row, then the restore/failover edge reconcile_out makes it moved_out(target shard 2, epoch 2)
		for _, q := range []string{
			`INSERT INTO namespace_ownership (namespace_id, tenant_id, shard_id, epoch, state)
			 VALUES ('` + ns + `', 'acme', 1, 1, 'active')`,
			`UPDATE namespace_ownership SET state = 'moved_out', target_shard_id = 2, target_epoch = 2
			 WHERE namespace_id = '` + ns + `'`,
			// the interrupted cleanup left only markers behind
			`INSERT INTO fact_hidden (namespace_id, tenant_id, memory_id, cause, invalidation_op)
			 VALUES ('` + ns + `', 'acme', md5('x')::uuid, 'invalidate', md5('op')::uuid)`,
			`INSERT INTO document_tombstones (namespace_id, tenant_id, document_id, up_to_version, deleted_at,
			 event_seq)
			 VALUES ('` + ns + `', 'acme', 'd9', 1, now(), 1)`,
		} {
			if _, err := admin.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		mv := moverFor(t, db.DSN(pgtest.Move), ns)
		const back = `UPDATE namespace_ownership SET state = 'incoming', epoch = 3,
			move_id = '0190c000-0000-7000-8000-0000000000f6', move_epoch = NULL, target_shard_id = NULL,
			target_epoch = NULL WHERE namespace_id = $1`
		_, err = mv.Exec(ctx, back, ns)
		if sqlState(err) != "55006" || !strings.Contains(err.Error(), "fact_hidden") {
			t.Fatalf("return_move over stale markers: want 55006 naming fact_hidden, got %v", err)
		}
		if _, err := admin.Exec(ctx, `DELETE FROM fact_hidden WHERE namespace_id = $1`, ns); err != nil {
			t.Fatal(err)
		}
		_, err = mv.Exec(ctx, back, ns)
		if sqlState(err) != "55006" || !strings.Contains(err.Error(), "document_tombstones") {
			t.Fatalf("return_move over a stale tombstone: want 55006 naming document_tombstones, got %v", err)
		}
		if _, err := admin.Exec(ctx, `DELETE FROM document_tombstones WHERE namespace_id = $1`, ns); err != nil {
			t.Fatal(err)
		}
		if _, err := mv.Exec(ctx, back, ns); err != nil {
			t.Fatalf("return_move over an empty shard: %v", err)
		}
	})
}
