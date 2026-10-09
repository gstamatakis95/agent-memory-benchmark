//go:build integration

package store_test

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgindex"
	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

// Namespaces of the plan fixtures (PLAN.md section 8.4.4: a 16-partition fixture with namespaces of 1 k and 50 k
// facts).
const (
	nsBig   = "0190c000-0000-7000-8000-0000000000b1" // 50,000 facts, 85,001 entities, no vectors
	nsSmall = "0190c000-0000-7000-8000-0000000000a1" // 1,000 facts with vectors: below the 2,000-vector crossover
	nsHNSW  = "0190c000-0000-7000-8000-0000000000c1" // 2,500 facts with vectors and a per-namespace partial HNSW

	bigDocs, bigPerDoc     = 50, 1000
	smallDocs, smallPerDoc = 10, 100
	hnswDocs, hnswPerDoc   = 25, 100
	entityCount            = 85000
)

var planOnce sync.Once

// planFixtures seeds the three plan namespaces once per package run, builds the HNSW index of the third through the
// index procedure (the way the index runner would) and analyzes the touched tables so the planner sees real sizes.
func planFixtures(t testing.TB) {
	t.Helper()
	planOnce.Do(func() {
		ctx := context.Background()
		for _, f := range []struct {
			ns   string
			b    pgtest.BulkFacts
			vecs bool
		}{
			{nsBig, pgtest.BulkFacts{Documents: bigDocs, PerDoc: bigPerDoc}, false},
			{nsSmall, pgtest.BulkFacts{Documents: smallDocs, PerDoc: smallPerDoc, Vectors: true}, true},
			{nsHNSW, pgtest.BulkFacts{Documents: hnswDocs, PerDoc: hnswPerDoc, Vectors: true}, true},
		} {
			pgtest.SeedOwnership(ctx, t, f.ns, tenantAcme)
			f.b.Namespace, f.b.Tenant = f.ns, tenantAcme
			pgtest.SeedBulk(ctx, t, f.b)
		}
		admin, err := pgtest.Connect(ctx, pgtest.Admin)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = admin.Close(ctx) }()
		if _, err := admin.Exec(ctx,
			`INSERT INTO entities (namespace_id, tenant_id, entity_id, canonical_name, canonical_norm)
			SELECT $1::uuid, 'acme', md5('e' || g)::uuid, 'Entity ' || g, 'entity ' || md5(g::text)
			  FROM generate_series(1, $2::int) g`, nsBig, entityCount); err != nil {
			t.Fatalf("entities: %v", err)
		}
		if _, err := admin.Exec(ctx,
			`INSERT INTO entities (namespace_id, tenant_id, entity_id, canonical_name, canonical_norm)
			VALUES ($1, 'acme', md5('kestrel')::uuid, 'Kestrel Corp', 'kestrel corp')`, nsBig); err != nil {
			t.Fatalf("entities: %v", err)
		}
		// a document tombstone in the big namespace: the lexical arm's visibility filter has something to remove
		if _, err := admin.Exec(ctx,
			`INSERT INTO document_tombstones (namespace_id, tenant_id, document_id, up_to_version,
			deleted_at, operation_id, intent_key, event_seq) VALUES ($1, 'acme', 'bulk-1', 1, now(), md5('del')::uuid,
			  'ik', 1)`,
			nsBig); err != nil {
			t.Fatalf("tombstone: %v", err)
		}
		runner := &pgindex.Runner{DSN: pgtest.DSN(pgtest.Migrate)}
		if _, err := runner.HNSW(ctx, "fact_vectors", nsHNSW, pgtest.SeedModel, "create"); err != nil {
			t.Fatalf("hnsw build: %v", err)
		}
		sup, err := pgtest.Connect(ctx, pgtest.Super)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sup.Close(ctx) }()
		for _, tbl := range []string{"entities", "fact_vectors", "document_tombstones"} {
			if _, err := sup.Exec(ctx, "ANALYZE "+tbl); err != nil {
				t.Fatalf("analyze %s: %v", tbl, err)
			}
		}
	})
}

// explain returns the EXPLAIN text of a statement run in tx; with analyze it also executes it.
func explain(t testing.TB, tx pgx.Tx, analyze bool, sql string, args ...any) string {
	t.Helper()
	opts := "COSTS OFF"
	if analyze {
		opts = "ANALYZE, BUFFERS, COSTS OFF"
	}
	rows, err := tx.Query(context.Background(), "EXPLAIN ("+opts+") "+sql, args...)
	if err != nil {
		t.Fatalf("explain: %v\n%s", err, sql)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("explain: %v\n%s", err, sql)
	}
	return b.String()
}

// mustHave fails the test for every needle missing from the plan; mustNot for every needle present.
func mustHave(t testing.TB, plan string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if !strings.Contains(plan, n) {
			t.Errorf("plan lacks %q:\n%s", n, plan)
		}
	}
}

func mustNot(t testing.TB, plan string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if strings.Contains(plan, n) {
			t.Errorf("plan must not contain %q:\n%s", n, plan)
		}
	}
}

// percentile returns the p-th percentile (0..1) of durations.
func percentile(d []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(float64(len(s)-1) * p)
	return s[i]
}

// vecLiteral renders a deterministic 768-dimension halfvec literal.
func vecLiteral(seed int) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < 768; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%.4f", float64((i*31+seed*17)%97)/97.0)
	}
	b.WriteByte(']')
	return b.String()
}

// freeListen returns a free TCP port on the loopback interface.
func freeListen() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
