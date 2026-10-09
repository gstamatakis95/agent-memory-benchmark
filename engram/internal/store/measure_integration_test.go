//go:build integration

package store_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// The measurements behind the M0.2 decisions Q18 (pg_search vs TsvectorIndex) and Q9 (pgx exec mode behind pgbouncer).
// They are not part of the gate: set ENGRAM_M02_MEASURE=<output directory> to run them. Each writes a markdown table
// that bench/results/phase0/m0.2-q18-lexical.md and m0.2-q9-pgx.md quote.
//
//	ENGRAM_M02_MEASURE=/tmp/m02 ENGRAM_TEST_DOCKER_HOST_NETWORK=1 \
//	  go test -tags integration -count=1 -timeout 30m -run 'TestMeasure_' -v ./internal/store/
func measureDir(t *testing.T) string {
	t.Helper()
	d := os.Getenv("ENGRAM_M02_MEASURE")
	if d == "" {
		t.Skip("set ENGRAM_M02_MEASURE=<dir> to run the M0.2 measurements")
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	return d
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1f", float64(d.Microseconds())/1000) }

// mix draws the terms of one query class from the corpus vocabulary w1..w2000 (the corpus draws words with
// power(random(), 2.5), so low ranks are frequent): rare = two terms of rank 500-2000, mixed = one frequent term (rank
// 1-60) and two of rank 100-2000, common = three frequent terms.
func mix(name string, rng *rand.Rand) string {
	pick := func(lo, hi int) int { return lo + rng.Intn(hi-lo+1) }
	switch name {
	case "rare":
		return fmt.Sprintf("w%d w%d", pick(500, 2000), pick(500, 2000))
	case "mixed":
		return fmt.Sprintf("w%d w%d w%d", pick(1, 60), pick(100, 2000), pick(100, 2000))
	default:
		return fmt.Sprintf("w%d w%d w%d", pick(1, 60), pick(1, 60), pick(1, 60))
	}
}

// TestMeasure_Q18_Lexical compares the lexical arm candidates on the real image, as engram_app under RLS: pg_search
// with the RLS-shaped predicate (what the schema supports today), pg_search behind a SECURITY DEFINER function, and the
// TsvectorIndex fallback (GIN over an expression, no generated column) behind the same kind of function.
func TestMeasure_Q18_Lexical(t *testing.T) {
	dir := measureDir(t)
	planFixtures(t)
	ctx := context.Background()
	sup := connect(t, pgtest.Super)
	mustExec := func(q string) {
		t.Helper()
		if _, err := sup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// candidates: created as the owner, dropped afterwards
	mustExec("SET ROLE engram_migrate")
	mustExec(`CREATE FUNCTION m02_lex_bm25(p_ns uuid, p_query text, p_limit integer)
	RETURNS TABLE (memory_id uuid, score real) LANGUAGE plpgsql STABLE SECURITY DEFINER
	SET search_path = public, pg_temp AS $$
	BEGIN
	  IF p_ns IS DISTINCT FROM nullif(current_setting('engram.namespace_id', true), '')::uuid THEN
	    RAISE EXCEPTION 'not in scope' USING ERRCODE = '42501';
	  END IF;
	  RETURN QUERY EXECUTE 'SELECT f.memory_id, pdb.score(f.memory_id) FROM facts f
	    WHERE f.namespace_id = $1 AND f.text ||| $2
	    ORDER BY pdb.score(f.memory_id) DESC, f.memory_id ASC LIMIT $3' USING p_ns, p_query, p_limit;
	END $$`)
	mustExec(`CREATE FUNCTION m02_lex_tsv(p_ns uuid, p_query text, p_limit integer)
	RETURNS TABLE (memory_id uuid, score real) LANGUAGE plpgsql STABLE SECURITY DEFINER
	SET search_path = public, pg_temp AS $$
	BEGIN
	  IF p_ns IS DISTINCT FROM nullif(current_setting('engram.namespace_id', true), '')::uuid THEN
	    RAISE EXCEPTION 'not in scope' USING ERRCODE = '42501';
	  END IF;
	  RETURN QUERY SELECT f.memory_id, ts_rank_cd(to_tsvector('simple', f.text), q.q)
	    FROM facts f, to_tsquery('simple', replace(p_query, ' ', ' | ')) q
	   WHERE f.namespace_id = p_ns AND to_tsvector('simple', f.text) @@ q.q
	   ORDER BY 2 DESC, f.memory_id LIMIT p_limit;
	END $$`)
	mustExec("CREATE INDEX m02_facts_tsv_idx ON facts USING gin (namespace_id, to_tsvector('simple', text))")
	mustExec("RESET ROLE")
	mustExec("GRANT EXECUTE ON FUNCTION m02_lex_bm25(uuid, text, integer), " +
		"m02_lex_tsv(uuid, text, integer) TO engram_app")
	mustExec("ANALYZE facts")
	t.Cleanup(func() {
		for _, q := range []string{"DROP FUNCTION IF EXISTS m02_lex_bm25(uuid, text, integer)",
			"DROP FUNCTION IF EXISTS m02_lex_tsv(uuid, text, integer)", "DROP INDEX IF EXISTS m02_facts_tsv_idx"} {
			_, _ = sup.Exec(context.Background(), q)
		}
	})

	type method struct {
		name string
		run  func(tx pgx.Tx, ns, q string) (int, error)
	}
	count := func(rows pgx.Rows, err error) (int, error) {
		if err != nil {
			return 0, err
		}
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		return n, rows.Err()
	}
	methods := []method{
		{"pg_search, RLS-shaped predicate (arm of 3.8 as supported today)", func(tx pgx.Tx, ns, q string) (int, error) {
			return count(tx.Query(ctx, lexicalArm, q, "{}", "{}", 150))
		}},
		{"pg_search behind a SECURITY DEFINER function", func(tx pgx.Tx, ns, q string) (int, error) {
			return count(tx.Query(ctx, "SELECT * FROM m02_lex_bm25($1::uuid, $2, 150)", ns, q))
		}},
		{"TsvectorIndex behind a SECURITY DEFINER function", func(tx pgx.Tx, ns, q string) (int, error) {
			return count(tx.Query(ctx, "SELECT * FROM m02_lex_tsv($1::uuid, $2, 150)", ns, q))
		}},
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| namespace | query class | method | n | p50 ms | p95 ms | p99 ms | rows |\n")
	fmt.Fprintf(&b, "|---|---|---|---|---|---|---|---|\n")
	for _, nsz := range []struct {
		ns   string
		name string
	}{{nsBig, "50,000 facts"}, {nsSmall, "1,000 facts"}} {
		for _, class := range []string{"rare", "mixed", "common"} {
			for _, m := range methods {
				rng := rand.New(rand.NewSource(1818))
				var d []time.Duration
				rowsSeen := 0
				for i := 0; i < 160; i++ {
					q := mix(class, rng)
					readTx(t, pgtest.App, scope(nsz.ns, tenantAcme), func(tx pgx.Tx) {
						start := time.Now()
						n, err := m.run(tx, nsz.ns, q)
						if err != nil {
							t.Fatalf("%s: %v", m.name, err)
						}
						if i >= 10 {
							d = append(d, time.Since(start))
							rowsSeen += n
						}
					})
				}
				fmt.Fprintf(&b, "| %s | %s | %s | %d | %s | %s | %s | %.0f |\n", nsz.name, class, m.name, len(d),
					ms(percentile(d, 0.5)), ms(percentile(d, 0.95)), ms(percentile(d, 0.99)),
					float64(rowsSeen)/float64(len(d)))
			}
		}
	}
	// tsvector straight under RLS (no function): the plan the fallback gets without a wrapper
	tx := scopedTx(t, pgtest.App, scope(nsBig, tenantAcme))
	plan := explain(t, tx, true, `SELECT memory_id, ts_rank_cd(to_tsvector('simple', text), q) AS score
		FROM facts, to_tsquery('simple', 'w5 | w17 | w300') q
		WHERE namespace_id = current_setting('engram.namespace_id')::uuid AND to_tsvector('simple', text) @@ q
		ORDER BY score DESC, memory_id LIMIT 150`)
	fmt.Fprintf(&b, "\nTsvectorIndex straight under RLS (no function), 50,000 facts:\n\n```\n%s```\n", plan)
	fmt.Fprintf(&b, "\nMeasured %s; %s; image %s.\n", time.Now().UTC().Format(time.RFC3339), loadNote(), pgtest.Image)
	if err := os.WriteFile(filepath.Join(dir, "q18-lexical.md"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + b.String())
}

// pgbouncerImage runs the pgbouncer 1.26 used for the Q9 measurement (transaction pooling, the production mode).
const pgbouncerImage = "edoburu/pgbouncer@sha256:9c78945868a6a142c7fc40ccd843bbe5a606df163c7ffce4de70e0d628d696a2"

// startPgbouncer runs pgbouncer in transaction pooling mode in front of the package server on a free port and returns
// the DSN of engram_app through it. maxPrepared is pgbouncer's max_prepared_statements (0 = protocol-level prepared
// statements unsupported).
func startPgbouncer(t *testing.T, maxPrepared int) string {
	t.Helper()
	host, port := pgtest.Addr()
	dir := t.TempDir()
	listen := 0
	{
		l, err := freeListen()
		if err != nil {
			t.Fatal(err)
		}
		listen = l
	}
	ini := fmt.Sprintf(`[databases]
* = host=%s port=%s

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = %d
auth_type = scram-sha-256
auth_file = /cfg/userlist.txt
pool_mode = transaction
max_client_conn = 200
default_pool_size = 20
max_prepared_statements = %d
ignore_startup_parameters = extra_float_digits
`, host, port, listen, maxPrepared)
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	userlist := fmt.Sprintf("\"engram_app\" \"%s\"\n", pgtest.Password)
	for name, body := range map[string]string{"pgbouncer.ini": ini, "userlist.txt": userlist} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	name := fmt.Sprintf("engram-pgbouncer-%d-%d", os.Getpid(), listen)
	out, err := exec.Command("docker", "run", "-d", "--rm", "--name", name, "--network", "host", "-v", dir+":/cfg:ro",
		"--entrypoint", "pgbouncer", pgbouncerImage, "/cfg/pgbouncer.ini").CombinedOutput()
	if err != nil {
		t.Skipf("cannot start pgbouncer (%v): %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	dsn := (&url.URL{Scheme: "postgres", User: url.UserPassword("engram_app", pgtest.Password),
		Host: fmt.Sprintf("127.0.0.1:%d", listen), Path: "/" + pgtest.DatabaseName(),
		RawQuery: "sslmode=disable"}).String()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			_ = c.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("pgbouncer did not come up on %d", listen)
	return ""
}

// TestMeasure_Q9_PgxExecMode compares pgx's query execution modes directly and behind pgbouncer in transaction pooling
// mode with and without protocol-level prepared statement support (max_prepared_statements), on the statement shapes of
// the arms. Every unit is one read transaction like an arm: BEGIN, set_config of the scope, the statement, COMMIT.
func TestMeasure_Q9_PgxExecMode(t *testing.T) {
	dir := measureDir(t)
	planFixtures(t)
	ctx := context.Background()
	targets := []struct{ name, dsn string }{
		{"direct", pgtest.DSN(pgtest.App)},
		{"pgbouncer 1.26, max_prepared_statements=0", startPgbouncer(t, 0)},
		{"pgbouncer 1.26, max_prepared_statements=100", startPgbouncer(t, 100)},
	}
	modes := []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"CacheStatement (default)", pgx.QueryExecModeCacheStatement},
		{"CacheDescribe", pgx.QueryExecModeCacheDescribe},
		{"DescribeExec", pgx.QueryExecModeDescribeExec},
		{"Exec", pgx.QueryExecModeExec},
		{"SimpleProtocol", pgx.QueryExecModeSimpleProtocol},
	}
	vec := vecLiteral(5)
	docs := make([]string, 0, 50)
	for i := 1; i <= 50; i++ {
		docs = append(docs, fmt.Sprintf("bulk-%d", i))
	}
	stmts := []struct {
		name string
		ns   string
		sql  string
		args func(i int) []any
	}{
		{"fence read (uuid param)", nsBig, `SELECT state, epoch, freeze_reason FROM namespace_ownership
			WHERE namespace_id = $1::uuid`,
			func(int) []any { return []any{nsBig} }},
		{"fact by key (2 uuid params)", nsBig, `SELECT memory_id, text FROM facts WHERE namespace_id = $1::uuid
			AND memory_id = md5($1::text || 'bf1-1')::uuid`, func(int) []any { return []any{nsBig} }},
		{"allowed docs (text[] param), 50 docs", nsBig, `SELECT memory_id FROM facts WHERE namespace_id = $1::uuid
			AND document_id = ANY ($2::text[]) AND mentioned_at <= $3 ORDER BY mentioned_at DESC LIMIT 150`,
			func(int) []any { return []any{nsBig, docs, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)} }},
		{"exact semantic (768-d halfvec param), 1k vectors", nsSmall, `SELECT memory_id,
			embedding <=> $1::halfvec(768) AS d
			FROM fact_vectors WHERE namespace_id = $2::uuid AND embedding_model = $3 ORDER BY 2 LIMIT 150`,
			func(int) []any { return []any{vec, nsSmall, pgtest.SeedModel} }},
		{"lexical Top-K (RLS-shaped, text param), 50k facts", nsBig, lexicalArm, func(i int) []any {
			return []any{fmt.Sprintf("w%d w%d w%d", 1+i%60, 100+(i*7)%1900, 100+(i*13)%1900), "{}", "{}", 150}
		}},
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| statement | pgx mode | target | n | p50 ms | p95 ms | result |\n|---|---|---|---|---|---|---|\n")
	for _, st := range stmts {
		for _, tg := range targets {
			for _, m := range modes {
				cfg, err := pgx.ParseConfig(tg.dsn)
				if err != nil {
					t.Fatal(err)
				}
				cfg.DefaultQueryExecMode = m.mode
				conn, err := pgx.ConnectConfig(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				var d []time.Duration
				var failure string
				const n = 120
				for i := 0; i < n && failure == ""; i++ {
					start := time.Now()
					err := func() error {
						tx, err := conn.Begin(ctx)
						if err != nil {
							return err
						}
						defer func() { _ = tx.Rollback(ctx) }()
						if err := pgtest.SetScope(ctx, tx, scope(st.ns, tenantAcme)); err != nil {
							return err
						}
						rows, err := tx.Query(ctx, st.sql, st.args(i)...)
						if err != nil {
							return err
						}
						for rows.Next() {
						}
						rows.Close()
						if err := rows.Err(); err != nil {
							return err
						}
						return tx.Commit(ctx)
					}()
					if err != nil {
						failure = strings.SplitN(err.Error(), "\n", 2)[0]
						if len(failure) > 90 {
							failure = failure[:90]
						}
						break
					}
					if i >= 20 {
						d = append(d, time.Since(start))
					}
				}
				_ = conn.Close(ctx)
				res := "ok"
				p50, p95 := "-", "-"
				if failure != "" {
					res = "FAILS: " + failure
				} else {
					p50, p95 = ms(percentile(d, 0.5)), ms(percentile(d, 0.95))
				}
				fmt.Fprintf(&b, "| %s | %s | %s | %d | %s | %s | %s |\n", st.name, m.name, tg.name, len(d), p50, p95,
					res)
			}
		}
	}
	fmt.Fprintf(&b, "\nMeasured %s; %s; image %s; pgbouncer %s.\n", time.Now().UTC().Format(time.RFC3339), loadNote(),
		pgtest.Image, pgbouncerImage)
	if err := os.WriteFile(filepath.Join(dir, "q9-pgx.md"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + b.String())
}
