//go:build integration

package store_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// lexicalArm is the lexical arm of PLAN.md section 3.8 in the form that keeps the pg_search Top-K pushdown under RLS
// (TestLexical_TopKPushdown, bench/results/phase0/m0.2-q18-lexical.md): the inner query carries the namespace predicate
// in the SAME shape as the RLS policy, `namespace_id = current_setting('engram.namespace_id')::uuid`, so the planner
// merges the two and the policy qual no longer sits in a Result node between the Top-K scan and its ORDER BY. $1 the
// query text, $2 the DocTomb jsonb, $3 the ChunkTomb array, $4 the over-fetch.
const lexicalArm = `SELECT c.memory_id, c.score FROM (
  SELECT memory_id, document_id, document_version, chunk_id, mentioned_at, fact_type, pdb.score(memory_id) AS score
    FROM facts
   WHERE namespace_id = current_setting('engram.namespace_id')::uuid AND text ||| $1
   ORDER BY pdb.score(memory_id) DESC, memory_id ASC
   LIMIT $4::int) c
 WHERE NOT engram_doc_hidden($2::jsonb, c.document_id, c.document_version) AND c.chunk_id <> ALL ($3::uuid[])
   AND NOT EXISTS (SELECT 1 FROM fact_hidden h
                    WHERE h.namespace_id = current_setting('engram.namespace_id')::uuid AND h.memory_id = c.memory_id)
 ORDER BY c.score DESC, c.memory_id ASC
 LIMIT 150`

// lexicalArmAsOf adds the as_of bound. as_of unset means NO predicate: pg_search 0.26 cannot turn 'infinity' into a
// term ("json cannot be converted to term"), so the arm never binds an infinite bound.
const lexicalArmAsOf = `SELECT memory_id, pdb.score(memory_id) AS score FROM facts
   WHERE namespace_id = current_setting('engram.namespace_id')::uuid AND text ||| $1 AND mentioned_at <= $2
   ORDER BY pdb.score(memory_id) DESC, memory_id ASC LIMIT 150`

// assertLexicalPushdown checks the plan of the lexical arm run as engram_app under RLS: the pg_search custom scan with
// the Top-K (score and tiebreak) pushed down, the composite-PK key field accepted, one partition kept of sixteen.
func assertLexicalPushdown(t testing.TB, tx pgx.Tx, query string) {
	t.Helper()
	plan := explain(t, tx, true, lexicalArm, query, `{"bulk-1": 1}`, "{}", 300)
	mustHave(t, plan,
		"Custom Scan (ParadeDB Base Scan) on facts_p",
		"Exec Method: TopKScanExecState",
		"Scores: true",
		"TopK Order By: pdb.score() desc, memory_id asc",
		"TopK Limit: 300",
		"Subplans Removed: 15",
		"memory_id_text_namespace_id_mentioned_at_idx",
	)
	mustNot(t, plan, "Seq Scan on facts", "Sort Method", "Unsupported")
}

// TestLexical_TopKPushdown (PLAN.md 8.4.4, Q18, N76): on the real image, as engram_app with the scope GUCs set, the
// BM25 arm keeps Top-K pushdown under RLS: the pg_search custom scan, the score+tiebreak ORDER BY inside the scan, one
// hash partition, the composite-PK key field accepted. Always asserted: every plan subtest. Latency gates, each in its
// own subtest and skipped (visibly) when the host is overloaded: (1) the SHIPPED RLS-shaped arm, p95 < 60 ms on a
// 50 k-fact namespace (the 8.4.4 clause); (2) the definer-wrapper CANDIDATE of Q18, p95 < 60 ms. The measurements
// of the decision pg_search vs TsvectorIndex are in bench/results/phase0/m0.2-q18-lexical.md.
func TestLexical_TopKPushdown(t *testing.T) {
	planFixtures(t)
	ctx := context.Background()

	t.Run("plan", func(t *testing.T) {
		tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
		assertLexicalPushdown(t, tx, "w5 w17 w300")
	})

	t.Run("index_definition", func(t *testing.T) {
		// bm25 on the hash-partitioned parent propagated to all 16 partitions, text tokenised as unicode words, the
		// non-partition half of the composite PK as the first (key) column
		c := connect(t, pgtest.Admin)
		var n int
		if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'
			 AND tablename LIKE 'facts\_p%' AND indexdef LIKE
			   '%USING bm25 (memory_id, ((text)::pdb.unicode_words)%'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 16 {
			t.Errorf("bm25 indexes on facts partitions = %d, want 16", n)
		}
	})

	t.Run("ordering_is_deterministic", func(t *testing.T) {
		run := func() []string {
			tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
			rows, err := tx.Query(ctx, lexicalArm, "w3 w44 w512", "{}", "{}", 150)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var ids []string
			for rows.Next() {
				var id string
				var score float32
				if err := rows.Scan(&id, &score); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			return ids
		}
		a, b := run(), run()
		if len(a) != 150 || strings.Join(a, ",") != strings.Join(b, ",") {
			t.Errorf("two runs differ or are short: %d / %d rows", len(a), len(b))
		}
	})

	t.Run("visibility_filter_after_top_k", func(t *testing.T) {
		// bulk-1 is tombstoned: with its tombstone in the DocTomb parameter its facts leave the Top-K, so the result is
		// shorter than the inner limit (the over-fetch rule of the arm); with an empty set they stay.
		const arm = `SELECT c.document_id FROM (
		  SELECT document_id, document_version, chunk_id, pdb.score(memory_id) AS score FROM facts
		   WHERE namespace_id = current_setting('engram.namespace_id')::uuid AND text ||| $1
		   ORDER BY pdb.score(memory_id) DESC, memory_id ASC LIMIT 300) c
		 WHERE NOT engram_doc_hidden($2::jsonb, c.document_id, c.document_version)`
		docsOf := func(tomb string) (total, hidden int) {
			tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
			rows, err := tx.Query(ctx, arm, "w1 w2 w3", tomb)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var d string
				if err := rows.Scan(&d); err != nil {
					t.Fatal(err)
				}
				total++
				if d == "bulk-1" {
					hidden++
				}
			}
			return total, hidden
		}
		_, shown := docsOf("{}")
		total, gone := docsOf(`{"bulk-1": 1}`)
		if shown == 0 {
			t.Fatal("the fixture query returns no fact of bulk-1; the filter would be vacuous")
		}
		if gone != 0 || total >= 300 {
			t.Errorf("with the tombstone: %d rows, %d from bulk-1; want fewer than 300 and none from bulk-1", total,
				gone)
		}
	})

	t.Run("as_of_bound_is_pushed_down_and_infinity_is_not_bound", func(t *testing.T) {
		tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
		plan := explain(t, tx, true, lexicalArmAsOf, "w5 w17", time.Date(2026, 1, 1, 5, 0, 0, 0, time.UTC))
		mustHave(t, plan, "TopKScanExecState", `"range"`)
		tx2 := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
		_, err := tx2.Exec(ctx, lexicalArmAsOf, "w5 w17", "infinity")
		if err == nil || !strings.Contains(err.Error(), "cannot be converted to term") {
			t.Logf("pg_search now accepts an infinite as_of bound (%v): the arm may bind it", err)
		}
	})

	t.Run("literal_namespace_predicate_defeats_the_pushdown", func(t *testing.T) {
		// Recorded behaviour of pg_search 0.26 (Q18): with a literal predicate next to the RLS qual the policy becomes
		// a One-Time Filter above the scan and the score cannot be computed ("Unsupported query shape"). The arm must
		// use the RLS-shaped predicate; engramlint sql accepts both forms as an explicit namespace_id predicate.
		tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
		_, err := tx.Exec(ctx, `SELECT memory_id, pdb.score(memory_id) FROM facts WHERE namespace_id = $1::uuid
			 AND text ||| 'w5 w17' ORDER BY pdb.score(memory_id) DESC LIMIT 150`, nsBig)
		t.Logf("literal predicate form: %v", err)
	})

	t.Run("latency_rls_shaped_form", func(t *testing.T) {
		// The shipped arm and the 8.4.4 clause (p95 < 60 ms). The planner visits all 16 partitions (the namespace is
		// only known at executor start), ~2.5 ms of pg_search planning each, which is why this gate is close to the
		// budget.
		d := lexicalLatency(t, 120, func(tx pgx.Tx, q string) error {
			rows, err := tx.Query(ctx, lexicalArm, q, "{}", "{}", 150)
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			rows.Close()
			return rows.Err()
		})
		p95 := percentile(d, 0.95)
		t.Logf("lexical arm, RLS-shaped predicate, 50k-fact namespace: p50 %v p95 %v (%s)", percentile(d, 0.5), p95,
			loadNote())
		if p95 >= 60*time.Millisecond {
			latencyFailure(t, "shipped RLS-shaped lexical arm p95 %v >= 60 ms", p95)
		}
	})

	t.Run("definer_wrapper_candidate", func(t *testing.T) {
		// The candidate of the Q18 decision: the same Top-K inside a SECURITY DEFINER function that takes the namespace
		// as a parameter (like engram_entity_fuzzy, N131). The owner bypasses RLS, so there is no policy qual between
		// the scan and its ORDER BY, the literal namespace prunes at plan time (one partition planned instead of
		// sixteen) and the p95 budget of the arm holds. The function is created here, as engram_migrate, and dropped
		// afterwards: it is a proposal in CONFLICTS.md, not part of the migrations.
		sup := connect(t, pgtest.Super)
		const create = `CREATE FUNCTION engram_lexical_facts_candidate(p_ns uuid, p_query text, p_limit integer)
		RETURNS TABLE (memory_id uuid, document_id text, document_version integer, chunk_id uuid, score real)
		LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
		BEGIN
		  IF p_ns IS DISTINCT FROM nullif(current_setting('engram.namespace_id', true), '')::uuid THEN
		    RAISE EXCEPTION 'namespace % is not the namespace in scope', p_ns USING ERRCODE = '42501';
		  END IF;
		  RETURN QUERY EXECUTE
		    'SELECT f.memory_id, f.document_id, f.document_version, f.chunk_id, pdb.score(f.memory_id)
		    FROM facts f WHERE f.namespace_id = $1 AND f.text ||| $2
		    ORDER BY pdb.score(f.memory_id) DESC, f.memory_id ASC LIMIT $3' USING p_ns, p_query, p_limit;
		END $$`
		for _, q := range []string{"SET ROLE engram_migrate", create, "RESET ROLE",
			"GRANT EXECUTE ON FUNCTION engram_lexical_facts_candidate(uuid, text, integer) TO engram_app"} {
			if _, err := sup.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		t.Cleanup(func() {
			_, _ = sup.Exec(context.Background(),
				"DROP FUNCTION IF EXISTS engram_lexical_facts_candidate(uuid, text, integer)")
		})
		const call = `SELECT memory_id, score FROM engram_lexical_facts_candidate($1::uuid, $2, 150)`
		// same ranking as the RLS-shaped arm
		ids := func(sql string, args ...any) (out []string) {
			readTx(t, pgtest.App, scope(nsBig, tenantAcme), func(tx pgx.Tx) {
				rows, err := tx.Query(ctx, sql, args...)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
					var id string
					var sc float32
					if err := rows.Scan(&id, &sc); err != nil {
						t.Fatal(err)
					}
					out = append(out, id)
				}
			})
			return out
		}
		a := ids(lexicalArm, "w3 w44 w512", "{}", "{}", 150)
		b := ids(call, nsBig, "w3 w44 w512")
		if len(a) != 150 || strings.Join(a, ",") != strings.Join(b, ",") {
			t.Errorf("wrapper and RLS-shaped arm disagree: %d vs %d rows", len(a), len(b))
		}
		// another namespace is refused
		tx := scopedTx(t, pgtest.App, scope(nsSmall, tenantAcme))
		if _, err := tx.Exec(ctx, call, nsBig, "w1"); sqlState(err) != "42501" {
			t.Errorf("wrapper called for another namespace: want 42501, got %v", err)
		}
		d := lexicalLatency(t, 120, func(tx pgx.Tx, q string) error {
			rows, err := tx.Query(ctx, call, nsBig, q)
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			rows.Close()
			return rows.Err()
		})
		p50, p95 := percentile(d, 0.5), percentile(d, 0.95)
		t.Logf("lexical Top-K through the definer wrapper, 50k-fact namespace: p50 %v p95 %v (%s)", p50, p95,
			loadNote())
		if p95 >= 60*time.Millisecond {
			latencyFailure(t, "wrapper p95 %v >= 60 ms", p95)
		}
	})
}

// lexicalLatency runs n lexical queries of a realistic mix (one frequent word, two mid-frequency words) in read
// transactions and returns the durations after a warm-up.
func lexicalLatency(t *testing.T, n int, run func(tx pgx.Tx, q string) error) []time.Duration {
	t.Helper()
	rng := rand.New(rand.NewSource(18))
	var d []time.Duration
	for i := 0; i < n; i++ {
		q := fmt.Sprintf("w%d w%d w%d", 1+rng.Intn(60), 100+rng.Intn(1900), 100+rng.Intn(1900))
		readTx(t, pgtest.App, scope(nsBig, tenantAcme), func(tx pgx.Tx) {
			start := time.Now()
			if err := run(tx, q); err != nil {
				t.Fatal(err)
			}
			if i >= 10 {
				d = append(d, time.Since(start))
			}
		})
	}
	return d
}

// loadNote and latencyFailure make the latency gates honest on a shared machine: a run on a host whose one-minute load
// average exceeds its CPU count measures the neighbours as much as the database, so a missed budget SKIPS the
// subtest (visible as SKIP in CI, never a silent pass; CI runs on an idle runner and fails it). Structural assertions
// (plans) are separate subtests and never relaxed. Call it as the last statement of a subtest or from a defer.
func loadNote() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "load unknown"
	}
	return fmt.Sprintf("load average %s on %d CPUs", strings.Fields(string(b))[0], runtime.NumCPU())
}

func latencyFailure(t *testing.T, format string, args ...any) {
	t.Helper()
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		var l float64
		if _, err := fmt.Sscanf(string(b), "%f", &l); err == nil && l > float64(runtime.NumCPU()) {
			t.Skipf("latency gate not enforced ("+loadNote()+"): "+format, args...)
			return
		}
	}
	t.Errorf(format, args...)
}

// TestRecall_SmallNamespaceExact: a namespace below 2,000 vectors is scanned exactly (PLAN.md 3.3.4, N112): 150
// candidates in under 5 ms warm, through fact_vectors_model_idx, with no HNSW node, and the index plan says so.
func TestRecall_SmallNamespaceExact(t *testing.T) {
	planFixtures(t)
	ctx := context.Background()
	q := vecLiteral(7)

	t.Run("index_plan_says_exact_below_2000", func(t *testing.T) {
		tx := scopedTx(t, pgtest.Admin, scope(nsSmall, tenantAcme))
		rows, err := tx.Query(ctx, `SELECT vector_table, vectors, action FROM engram_vector_index_plan($1::uuid)`,
			nsSmall)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, action string
			var n int64
			if err := rows.Scan(&tbl, &n, &action); err != nil {
				t.Fatal(err)
			}
			if tbl == "fact_vectors" && (n != smallDocs*smallPerDoc || action != "none") {
				t.Errorf("fact_vectors: %d vectors action %s, want %d / none", n, action, smallDocs*smallPerDoc)
			}
		}
	})

	t.Run("model_idx_scan_plan", func(t *testing.T) {
		tx := scopedTx(t, pgtest.App, scope(nsSmall, tenantAcme))
		if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			t.Fatal(err)
		}
		plan := explain(t, tx, false, `SELECT memory_id, embedding <=> $1::halfvec(768) AS distance FROM fact_vectors
			 WHERE namespace_id = $2::uuid AND embedding_model = $3 ORDER BY embedding <=> $1::halfvec(768) LIMIT 150`,
			q, nsSmall, pgtest.SeedModel)
		// PLAN.md 3.3.4 names fact_vectors_model_idx; the planner may equally take the (namespace_id, ins_seq) index of
		// the insert-only class (both lead with the namespace), so the pin is: a namespace-keyed index scan, a sort, no
		// partition scan and no HNSW node
		mustHave(t, plan, "Index Cond: (namespace_id =", "Sort")
		mustNot(t, plan, "fv_", "Order By: (embedding", "Seq Scan")
	})

	t.Run("exact_path_150_candidates_under_5ms", func(t *testing.T) {
		const exact = `WITH e AS MATERIALIZED (
		  SELECT v.memory_id, v.embedding <=> $1::halfvec(768) AS distance
		    FROM facts f JOIN fact_vectors v ON v.namespace_id = $2::uuid AND v.memory_id = f.memory_id AND
		      v.embedding_model = $3
		   WHERE f.namespace_id = $2::uuid AND f.document_id = ANY ($4::text[]) AND f.fact_type = ANY ($5::fact_type[])
		     AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $2::uuid AND h.memory_id = f.memory_id))
		SELECT memory_id, distance FROM e ORDER BY distance LIMIT 150`
		docs := make([]string, 0, smallDocs)
		for i := 1; i <= smallDocs; i++ {
			docs = append(docs, fmt.Sprintf("bulk-%d", i))
		}
		const simple = `SELECT memory_id, embedding <=> $1::halfvec(768) AS distance FROM fact_vectors
		 WHERE namespace_id = $2::uuid AND embedding_model = $3
		   AND NOT engram_doc_hidden('{}'::jsonb, document_id, document_version) AND chunk_id <> ALL ('{}'::uuid[])
		   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $2::uuid AND h.memory_id =
		     fact_vectors.memory_id)
		 ORDER BY embedding <=> $1::halfvec(768) LIMIT 150`
		var d []time.Duration
		for i := 0; i < 60; i++ {
			readTx(t, pgtest.App, scope(nsSmall, tenantAcme), func(tx pgx.Tx) {
				start := time.Now()
				rows, err := tx.Query(ctx, simple, q, nsSmall, pgtest.SeedModel)
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for rows.Next() {
					n++
				}
				rows.Close()
				if n != 150 {
					t.Fatalf("exact path returned %d rows, want 150", n)
				}
				if i >= 10 {
					d = append(d, time.Since(start))
				}
			})
		}
		p50, p95 := percentile(d, 0.5), percentile(d, 0.95)
		t.Logf("exact scan over %d vectors: p50 %v p95 %v", smallDocs*smallPerDoc, p50, p95)
		defer func() { // the structural assertions below always run; the gate may skip the subtest
			if p50 >= 5*time.Millisecond {
				latencyFailure(t, "warm p50 %v >= 5 ms", p50)
			}
		}()
		tx := scopedTx(t, pgtest.App, scope(nsSmall, tenantAcme))
		if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			t.Fatal(err)
		}
		plan := explain(t, tx, false, simple, q, nsSmall, pgtest.SeedModel)
		mustNot(t, plan, "fv_", "hnsw", "Seq Scan")
		// the join form of PLAN.md 3.8 (driven from facts_doc_idx) is measured for the record
		var dj []time.Duration
		for i := 0; i < 30; i++ {
			readTx(t, pgtest.App, scope(nsSmall, tenantAcme), func(tx pgx.Tx) {
				start := time.Now()
				rows, err := tx.Query(ctx, exact, q, nsSmall, pgtest.SeedModel, docs, []string{"world", "experience"})
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
				}
				rows.Close()
				if i >= 5 {
					dj = append(dj, time.Since(start))
				}
			})
		}
		t.Logf("join form over %d vectors: p50 %v p95 %v", smallDocs*smallPerDoc, percentile(dj, 0.5), percentile(dj,
			0.95))
	})
}

// TestPlans_AsEngramApp (N131, N19, PLAN.md 8.4.4): every plan assertion of the Postgres side, executed as engram_app
// with the scope GUCs set on the real ParadeDB image (never as owner or superuser).
func TestPlans_AsEngramApp(t *testing.T) {
	planFixtures(t)
	ctx := context.Background()
	armGUCs := "SET LOCAL enable_seqscan = off; SET LOCAL max_parallel_workers_per_gather = 0"
	withGUCs := func(t *testing.T, ns string) pgx.Tx {
		tx := scopedTx(t, pgtest.App, scope(ns, tenantAcme))
		for _, s := range strings.Split(armGUCs, "; ") {
			if _, err := tx.Exec(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		return tx
	}

	t.Run("fence_read_is_a_primary_key_lookup", func(t *testing.T) {
		tx := withGUCs(t, nsBig)
		plan := explain(t, tx, false,
			`SELECT state, epoch, freeze_reason FROM namespace_ownership WHERE namespace_id = $1::uuid`, nsBig)
		mustHave(t, plan, "Index Scan using namespace_ownership_", "Index Cond: (namespace_id =")
	})

	t.Run("marker_sets_use_the_namespace_keys", func(t *testing.T) {
		tx := withGUCs(t, nsBig)
		mustHave(t, explain(t, tx, false, `SELECT t.document_id, max(t.up_to_version) FROM document_tombstones t
			 WHERE t.namespace_id = $1::uuid AND t.expunge_state <> 'purged' GROUP BY t.document_id`, nsBig),
			"document_tombstones_")
		mustHave(t, explain(t, tx, false, `SELECT chunk_id FROM chunk_tombstones WHERE namespace_id = $1::uuid`, nsBig),
			"Index Cond: (namespace_id =")
		var tomb string
		if err := tx.QueryRow(ctx, `SELECT engram_doc_tomb($1::uuid)::text`, nsBig).Scan(&tomb); err != nil {
			t.Fatal(err)
		}
		if tomb != `{"bulk-1": 1}` {
			t.Errorf("engram_doc_tomb = %s", tomb)
		}
	})

	t.Run("fact_hidden_is_a_primary_key_anti_join", func(t *testing.T) {
		tx := withGUCs(t, nsBig)
		plan := explain(t, tx, false, `SELECT f.memory_id FROM facts f
			 WHERE f.namespace_id = $1::uuid AND f.document_id = 'bulk-7'
			   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1::uuid AND h.memory_id =
			     f.memory_id)`, nsBig)
		mustHave(t, plan, "Anti Join", "fact_hidden_pkey", "namespace_id_document_id_document_version_idx")
	})

	t.Run("bm25_arm_keeps_top_k_under_rls", func(t *testing.T) {
		tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
		assertLexicalPushdown(t, tx, "w2 w9 w81")
	})

	t.Run("temporal_arm_probes_facts_occurred_idx", func(t *testing.T) {
		tx := withGUCs(t, nsBig)
		plan := explain(t, tx, false, `SELECT memory_id, occurred_start FROM facts
			 WHERE namespace_id = $1::uuid AND occurred_start IS NOT NULL AND occurred_start <= $2 AND occurred_start
			   >= $3
			 ORDER BY occurred_start DESC LIMIT 150`, nsBig, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		mustHave(t, plan, "Index Scan Backward using facts_p10_namespace_id_occurred_start_idx")
		mustNot(t, plan, "Seq Scan")
	})

	t.Run("entity_trigram_lookup_goes_through_the_definer_wrapper", func(t *testing.T) {
		// wrapped: an index scan of entities_trgm_idx (proved by the index's own scan counter), <= 10 ms warm
		admin := connect(t, pgtest.Admin)
		scans := func() int64 {
			var n int64
			if err := admin.QueryRow(ctx, `SELECT coalesce(sum(idx_scan), 0)::bigint FROM pg_stat_user_indexes
				 WHERE indexrelname LIKE 'entities\_%trgm\_idx'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		var wrapped []time.Duration
		before := scans()
		for i := 0; i < 40; i++ {
			pool := pgtest.Pool(t, pgtest.App)
			tx, err := pgtest.BeginScoped(ctx, pool, scope(nsBig, tenantAcme))
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			var name string
			if err := tx.QueryRow(ctx, `SELECT canonical_name FROM engram_entity_fuzzy($1::uuid, 'kestrel corp')`,
				nsBig).Scan(&name); err != nil {
				t.Fatal(err)
			}
			if i > 0 {
				wrapped = append(wrapped, time.Since(start))
			}
			if name != "Kestrel Corp" {
				t.Fatalf("wrapper returned %q", name)
			}
			// make the session's statistics visible to the counter read below
			if _, err := tx.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if got := scans() - before; got < 39 {
			t.Errorf("entities_trgm_idx scanned %d times by 40 wrapped lookups, want every one", got)
		}
		p50, p95 := percentile(wrapped, 0.5), percentile(wrapped, 0.95)
		t.Logf("entity_fuzzy over %d entities, trigram-selective query: p50 %v p95 %v", entityCount, p50, p95)
		defer func() { // the structural assertions below always run; the gate may skip the subtest
			if p50 > 10*time.Millisecond {
				latencyFailure(t, "wrapped lookup p50 %v > 10 ms", p50)
			}
		}()
		// Scope of the gate (M0.2 review F9): 'kestrel corp' shares its trigrams with no other entity name, so the GIN
		// scan is selective. A query whose trigrams occur in most names goes through the same index and rechecks nearly
		// every row (~360 ms for 85 k entities); the gate proves the plan shape, not a latency for arbitrary names.
		// Realistic names are measured in M0.6. Recorded here, not gated:
		{
			tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
			start := time.Now()
			rows, err := tx.Query(ctx, `SELECT canonical_name FROM engram_entity_fuzzy($1::uuid, 'entity 5a1')`, nsBig)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for rows.Next() {
				n++
			}
			rows.Close()
			t.Logf("non-selective trigram query through the wrapper ('entity 5a1', %d rows): %v", n, time.Since(start))
		}
		// unwrapped: the trigram operator is not leakproof, so under RLS the planner seq-scans the whole table
		tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
		start := time.Now()
		plan := explain(t, tx, true, `SELECT entity_id, similarity(canonical_norm, 'kestrel corp') FROM entities
			 WHERE namespace_id = $1::uuid AND merged_into IS NULL AND canonical_norm % 'kestrel corp'`, nsBig)
		unwrapped := time.Since(start)
		mustHave(t, plan, "Seq Scan on entities")
		mustNot(t, plan, "trgm_idx")
		t.Logf("unwrapped lookup as engram_app: %v\n%s", unwrapped, plan)
		if unwrapped < 5*p50 {
			t.Errorf("unwrapped %v is not clearly slower than wrapped p50 %v", unwrapped, p50)
		}
		// the wrapper re-checks the namespace in scope
		tx = scopedTx(t, pgtest.App, scope(nsSmall, tenantAcme))
		if _, err := tx.Exec(ctx, `SELECT * FROM engram_entity_fuzzy($1::uuid, 'kestrel corp')`,
			nsBig); sqlState(err) != "42501" {
			t.Errorf("wrapper called for another namespace: want 42501, got %v", err)
		}
		// and neither the relay nor the mover may call it (P-9)
		for _, r := range []pgtest.Role{pgtest.Relay, pgtest.Move} {
			tx := scopedTx(t, r, scope(nsBig, tenantAcme))
			if _, err := tx.Exec(ctx, `SELECT * FROM engram_entity_fuzzy($1::uuid, 'kestrel corp')`,
				nsBig); sqlState(err) != "42501" {
				t.Errorf("%s may execute engram_entity_fuzzy: %v", r, err)
			}
		}
	})

	t.Run("semantic_arm_uses_the_partial_hnsw_of_its_namespace", func(t *testing.T) {
		tx := scopedTx(t, pgtest.App, scope(nsHNSW, tenantAcme))
		for _, s := range []string{
			"SET LOCAL plan_cache_mode = force_custom_plan", "SET LOCAL enable_seqscan = off",
			"SET LOCAL max_parallel_workers_per_gather = 0", "SET LOCAL enable_bitmapscan = off",
			"SET LOCAL enable_sort = off",
			"SET LOCAL hnsw.iterative_scan = relaxed_order", "SET LOCAL hnsw.ef_search = 150",
			"SET LOCAL hnsw.max_scan_tuples = 20000",
		} {
			if _, err := tx.Exec(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		// bound namespace and model become constants: the partial-index predicate is provable
		arm := fmt.Sprintf(`SELECT memory_id, document_id, chunk_id, mentioned_at,
		       embedding <=> '%s'::halfvec(768) AS distance
		  FROM fact_vectors
		 WHERE namespace_id = '%s' AND embedding_model = '%s'
		   AND NOT engram_doc_hidden('{}'::jsonb, document_id, document_version)
		   AND chunk_id <> ALL ('{}'::uuid[])
		   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = '%s' AND h.memory_id =
		     fact_vectors.memory_id)
		   AND fact_type = ANY ('{world,experience}'::fact_type[])
		 ORDER BY embedding <=> '%s'::halfvec(768) LIMIT 150`, vecLiteral(3), nsHNSW, pgtest.SeedModel, nsHNSW,
			vecLiteral(3))
		plan := explain(t, tx, true, arm)
		mustHave(t, plan, "Index Scan using fv_", "Order By: (embedding <=>", "rows=150")
		mustNot(t, plan, "Seq Scan", "Sort Method")
		if n := strings.Count(plan, " on fact_vectors_p"); n != 1 {
			t.Errorf("expected exactly one vector partition in the plan, found %d:\n%s", n, plan)
		}
	})

	t.Run("no_arm_table_carries_a_tag_column_and_resolution_is_a_separate_statement", func(t *testing.T) {
		// Structural (M0.2 review F8): tags live on documents only (N113), so no predicate on a tag can run in an arm.
		// The one exception is observation_version_vectors.obs_tags, the immutable copy of observations.tags that the
		// observation arm tests inside its scan (N151).
		admin := connect(t, pgtest.Admin)
		var cols string
		if err := admin.QueryRow(ctx, `SELECT coalesce(string_agg(c.relname || '.' || a.attname, ', ' ORDER BY 1), '')
			 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
			WHERE c.relnamespace = 'public'::regnamespace AND NOT c.relispartition AND a.attnum > 0
			  AND NOT a.attisdropped
			  AND c.relname IN ('facts', 'chunks', 'fact_vectors', 'chunk_vectors', 'observation_versions',
			                    'observation_version_vectors', 'page_versions', 'page_version_vectors', 'fact_links',
			                    'entity_mentions')
			  AND a.attname ~ 'tag' AND (c.relname, a.attname) <> ('observation_version_vectors', 'obs_tags')`).
			Scan(&cols); err != nil || cols != "" {
			t.Fatalf("arm tables with a tag column: %q (err %v)", cols, err)
		}
		// The resolution is its own statement against documents; the arm then filters by document_id = ANY($allowed).
		tx := withGUCs(t, nsBig)
		var docs []string
		rows, err := tx.Query(ctx,
			`SELECT document_id FROM documents WHERE namespace_id = $1::uuid AND tags && ARRAY['bulk']`, nsBig)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var d string
			if err := rows.Scan(&d); err != nil {
				t.Fatal(err)
			}
			docs = append(docs, d)
		}
		rows.Close()
		if len(docs) != bigDocs {
			t.Fatalf("resolved %d documents, want %d", len(docs), bigDocs)
		}
		arm := explain(t, tx, false, `SELECT f.memory_id FROM facts f WHERE f.namespace_id = $1::uuid
			 AND f.document_id = ANY ($2::text[]) AND f.mentioned_at <= $3 ORDER BY f.mentioned_at DESC LIMIT 150`,
			nsBig, docs, time.Now())
		mustHave(t, arm, "document_id = ANY")
		mustNot(t, arm, "tags", "&&")
		// What runs under RLS and what does not: the resolution statement itself evaluates `tags && ...` under the
		// policy. arrayoverlap is not leakproof, so engram_app cannot use documents_tags_gin and reads the namespace's
		// documents through the namespace key (<= 10 k rows by design). "Tags resolve outside RLS" therefore holds for
		// the ARMS only; the resolution under RLS is open (CONFLICTS #14.6), pinned here so a change is noticed.
		var leakproof bool
		const leakQ = `SELECT bool_and(proleakproof) FROM pg_proc WHERE proname = 'arrayoverlap'`
		if err := admin.QueryRow(ctx, leakQ).Scan(&leakproof); err != nil ||
			leakproof {
			t.Errorf("arrayoverlap leakproof = %v (err %v): the premise of this assertion changed", leakproof, err)
		}
		res := explain(t, tx, false,
			`SELECT document_id FROM documents WHERE namespace_id = $1::uuid AND tags && ARRAY['bulk']`, nsBig)
		mustHave(t, res, "Filter: (tags &&")
		mustNot(t, res, "documents_tags_gin")
	})
}
