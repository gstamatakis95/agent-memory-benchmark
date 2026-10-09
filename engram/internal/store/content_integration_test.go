//go:build integration

package store_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// insertOnlyTables is the insert-only class of PLAN.md N137, in the order the register lists it.
var insertOnlyTables = []string{
	"ingest_ledger", "document_version_chunks", "chunks", "facts", "fact_links", "entity_mentions",
	"fact_vectors", "chunk_vectors", "observation_version_vectors", "page_version_vectors",
	"observation_versions", "observation_inputs", "observation_version_sources", "observation_version_meta",
	"page_versions", "page_version_inputs", "page_version_meta",
	"fact_consolidation", "consolidation_proposals", "consolidation_applied", "deletion_log", "curation_log",
}

// TestContent_InsertOnly (N113, N137): the class tags are the one list; every insert-only table refuses UPDATE for
// every role (the BEFORE UPDATE trigger is the second wall: it stops even the owner and engram_admin) and refuses
// DELETE for every role but engram_admin (the expunge purge); the structure the class promises is present (ins_seq with
// its (namespace_id, ins_seq) index, fillfactor 100, no visibility or generated column, evidence without a foreign key
// to facts).
func TestContent_InsertOnly(t *testing.T) {
	seeded(t)
	ctx := context.Background()
	admin := connect(t, pgtest.Admin)

	// the class tags: exactly the register list, every table has exactly one tag
	rows, err := admin.Query(ctx, `SELECT c.relname::text, coalesce(obj_description(c.oid, 'pg_class'), '')
		 FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT
		   c.relispartition
		   AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')`)
	if err != nil {
		t.Fatal(err)
	}
	var tagged []string
	for rows.Next() {
		var name, tag string
		if err := rows.Scan(&name, &tag); err != nil {
			t.Fatal(err)
		}
		switch tag {
		case "class: insert-only":
			tagged = append(tagged, name)
		case "class: mutable", "class: expiring", "class: shard-local":
		default:
			if name != "goose_db_version" {
				t.Errorf("table %s has no class tag (%q)", name, tag)
			}
		}
	}
	rows.Close()
	want := append([]string(nil), insertOnlyTables...)
	sort.Strings(want)
	sort.Strings(tagged)
	if strings.Join(tagged, ",") != strings.Join(want, ",") {
		t.Errorf("insert-only tags = %v, want the N137 list %v", tagged, want)
	}

	// structure: ins_seq NOT NULL DEFAULT nextval + (namespace_id, ins_seq) index; fillfactor 100; no generated column
	for _, tbl := range insertOnlyTables {
		var hasSeq, hasIdx bool
		err := admin.QueryRow(ctx, `SELECT
			EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = $1::regclass AND a.attname = 'ins_seq'
			          AND a.attnotnull AND a.atthasdef AND NOT a.attisdropped),
			EXISTS (SELECT 1 FROM pg_index x WHERE x.indrelid = $1::regclass
			          AND (SELECT attname FROM pg_attribute WHERE attrelid = x.indrelid AND attnum = x.indkey[0]) =
			            'namespace_id'
			          AND (SELECT attname FROM pg_attribute WHERE attrelid = x.indrelid AND attnum = x.indkey[1]) =
			            'ins_seq')`,
			tbl).Scan(&hasSeq, &hasIdx)
		if err != nil || !hasSeq || !hasIdx {
			t.Errorf("%s: ins_seq=%v (namespace_id, ins_seq) index=%v err=%v", tbl, hasSeq, hasIdx, err)
		}
		// fillfactor 100 on the table itself or, for a partitioned table, on every partition
		var bad int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM (
			SELECT r.oid FROM pg_class r WHERE r.oid = $1::regclass AND r.relkind = 'r'
			UNION ALL SELECT i.inhrelid FROM pg_inherits i WHERE i.inhparent = $1::regclass) x
			WHERE NOT coalesce('fillfactor=100' = ANY (SELECT unnest(reloptions) FROM pg_class WHERE oid = x.oid),
			  false)`,
			tbl).Scan(&bad); err != nil || bad != 0 {
			t.Errorf("%s: %d relations without fillfactor=100 (err %v)", tbl, bad, err)
		}
	}
	var gen string
	if err := admin.QueryRow(ctx, `SELECT coalesce(string_agg(c.relname || '.' || a.attname, ', '), '')
		 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND a.attnum > 0
		  AND NOT a.attisdropped AND a.attgenerated <> ''`).Scan(&gen); err != nil || gen != "" {
		t.Errorf("generated columns exist (N113): %q (err %v)", gen, err)
	}
	var vis string
	if err := admin.QueryRow(ctx, `SELECT coalesce(string_agg(c.relname || '.' || a.attname, ', '), '')
		 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT
		  a.attisdropped
		  AND c.relname = ANY ($1) AND c.relname NOT IN ('ingest_ledger', 'observation_version_meta',
		    'page_version_meta')
		  AND a.attname IN ('live', 'retired_at', 'invalidated_at', 'stale_write', 'stale_delete', 'superseded_at',
		                    'tags', 'derived_from_deleted', 'hidden_by_invalidation')`,
		insertOnlyTables).Scan(&vis); err != nil || vis != "" {
		t.Errorf("visibility columns on content tables (N113): %q (err %v)", vis, err)
	}
	var fk string
	if err := admin.QueryRow(ctx, `SELECT coalesce(string_agg(conrelid::regclass::text, ', '), '') FROM pg_constraint
		 WHERE contype = 'f' AND conparentid = 0 AND confrelid = 'facts'::regclass
		   AND conrelid::regclass::text IN ('observation_inputs', 'observation_version_sources', 'page_version_inputs',
		                                    'fact_hidden', 'curation_log')`).Scan(&fk); err != nil || fk != "" {
		t.Errorf("evidence tables reference facts (N135): %q (err %v)", fk, err)
	}

	// behaviour, per table
	const any = "namespace_id = '" + nsA + "'"
	for _, tbl := range append(append([]string(nil), insertOnlyTables...), "token_usage_events") {
		q := tbl
		t.Run(tbl, func(t *testing.T) {
			update := "UPDATE " + q + " SET namespace_id = namespace_id WHERE " + any
			// UPDATE: refused for everyone. engram_admin and the owner pass the privilege and meet the trigger.
			for _, role := range []pgtest.Role{pgtest.Admin, pgtest.Migrate} {
				tx := scopedTx(t, role, scope(nsA, tenantAcme))
				_, err := tx.Exec(ctx, update)
				if sqlState(err) != "42501" || !strings.Contains(err.Error(), "-only") {
					t.Errorf("UPDATE as %s: want the insert-only trigger (42501), got %v", role, err)
				}
			}
			for _, role := range []pgtest.Role{pgtest.App, pgtest.Move, pgtest.Relay} {
				tx := scopedTx(t, role, scope(nsA, tenantAcme))
				if _, err := tx.Exec(ctx, update); sqlState(err) != "42501" {
					t.Errorf("UPDATE as %s: want 42501, got %v", role, err)
				}
			}
			// DELETE: only the expunge role. For the others the privilege is missing (42501).
			for _, role := range []pgtest.Role{pgtest.App, pgtest.Move, pgtest.Relay} {
				tx := scopedTx(t, role, scope(nsA, tenantAcme))
				if _, err := tx.Exec(ctx, "DELETE FROM "+q+" WHERE "+any); sqlState(err) != "42501" {
					t.Errorf("DELETE as %s: want 42501, got %v", role, err)
				}
			}
			tx := scopedTx(t, pgtest.Admin, scope(nsA, tenantAcme))
			if _, err := tx.Exec(ctx, "DELETE FROM "+q+" WHERE "+any); err != nil && sqlState(err) != "23503" {
				t.Errorf("DELETE as engram_admin (the purge): want success or a foreign-key stop, got %v", err)
			}
			// The owner can delete content too (N93 cleanup path: engram_migrate owns the tables and is BYPASSRLS), so
			// the claim is "exactly engram_migrate and engram_admin", pinned here (M0.2 review F11).
			_ = tx.Rollback(ctx) // release the admin's row locks first
			tx = scopedTx(t, pgtest.Migrate, scope(nsA, tenantAcme))
			if _, err := tx.Exec(ctx, "DELETE FROM "+q+" WHERE "+any); err != nil && sqlState(err) != "23503" {
				t.Errorf("DELETE as engram_migrate (the owner): want success or a foreign-key stop, got %v", err)
			}
			var holders string
			if err := admin.QueryRow(ctx, `SELECT string_agg(r.rolname, ', ' ORDER BY r.rolname) FROM pg_roles r
				WHERE r.rolname LIKE 'engram\_%' AND has_table_privilege(r.oid, $1::regclass, 'DELETE')`,
				q).Scan(&holders); err != nil || holders != "engram_admin, engram_migrate" {
				t.Errorf("roles holding DELETE on %s: %q (err %v), want engram_admin, engram_migrate only", q, holders,
					err)
			}
		})
	}
}
