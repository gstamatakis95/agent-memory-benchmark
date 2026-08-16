package embed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeCacheDB implements the DB surface (Exec/QueryRow) over a map, mimicking
// embedding_cache semantics including ON CONFLICT DO NOTHING.
type fakeCacheDB struct {
	rows map[string][]byte
	puts int
	gets int
}

func newFakeCacheDB() *fakeCacheDB { return &fakeCacheDB{rows: map[string][]byte{}} }

func cacheMapKey(args []any) string {
	return string(args[0].([]byte)) + "|" + args[1].(string) + "|" + args[2].(string)
}

func (f *fakeCacheDB) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	f.puts++
	k := cacheMapKey(args)
	if _, exists := f.rows[k]; !exists { // ON CONFLICT DO NOTHING
		f.rows[k] = args[4].([]byte)
	}
	return pgconn.CommandTag{}, nil
}

type fakeRow struct {
	vec []byte
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*[]byte)) = r.vec
	return nil
}

func (f *fakeCacheDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	f.gets++
	if vec, ok := f.rows[cacheMapKey(args)]; ok {
		return fakeRow{vec: vec}
	}
	return fakeRow{err: pgx.ErrNoRows}
}

// TestCacheKey is the docs/06-testing.md Tier 0 cache-key test: the key is
// the SHA-256 of the PREFIXED text — never the raw text, never a timestamp.
func TestCacheKey(t *testing.T) {
	raw := "I visited Paris in May."
	prefixed := DocumentPrefix + raw

	key := CacheKey(prefixed)
	wantSum := sha256.Sum256([]byte(prefixed))
	if !bytes.Equal(key, wantSum[:]) {
		t.Fatalf("CacheKey != sha256(prefixed text)")
	}

	// Hashing the raw text would be the classic bug: assert it differs.
	rawSum := sha256.Sum256([]byte(raw))
	if bytes.Equal(key, rawSum[:]) {
		t.Fatal("cache key equals sha256(raw text) — key must hash the PREFIXED text")
	}

	// Document and query prefixes must never collide in the cache.
	if bytes.Equal(CacheKey(DocumentPrefix+raw), CacheKey(QueryPrefix+raw)) {
		t.Fatal("document and query cache keys collide")
	}

	// No timestamp anywhere in the key: identical text yields the identical
	// key across calls at different times.
	k1 := CacheKey(prefixed)
	time.Sleep(5 * time.Millisecond)
	k2 := CacheKey(prefixed)
	if !bytes.Equal(k1, k2) {
		t.Fatal("cache key changed across calls — a timestamp leaked into the key")
	}
}

// TestClientUsesCache: a second EmbedDocument of the same raw text is served
// from embedding_cache, with no second transport RPC; the cached vector is
// bit-identical.
func TestClientUsesCache(t *testing.T) {
	ctx := context.Background()
	mock := &recordingEmbedder{}
	db := newFakeCacheDB()
	c := NewClient(mock, db)

	v1, err := c.EmbedDocument(ctx, "same text")
	if err != nil {
		t.Fatal(err)
	}
	if len(mock.texts) != 1 {
		t.Fatalf("first embed: want 1 transport call, got %d", len(mock.texts))
	}

	v2, err := c.EmbedDocument(ctx, "same text")
	if err != nil {
		t.Fatal(err)
	}
	if len(mock.texts) != 1 {
		t.Fatalf("cache miss on identical text: %d transport calls", len(mock.texts))
	}
	if !bytes.Equal(PackVector(v1), PackVector(v2)) {
		t.Fatal("cached vector differs from original")
	}

	// Same raw text via the QUERY path must NOT hit the document cache
	// entry (different prefix -> different key and task_prefix).
	if _, err := c.EmbedQuery(ctx, "same text"); err != nil {
		t.Fatal(err)
	}
	if len(mock.texts) != 2 {
		t.Fatalf("query path served from document cache: %d transport calls", len(mock.texts))
	}
}

// TestCachePutIdempotent: double insert leaves one row (ON CONFLICT DO
// NOTHING semantics at the fake level; the SQL itself is pinned below).
func TestCachePutIdempotent(t *testing.T) {
	ctx := context.Background()
	db := newFakeCacheDB()
	key := CacheKey(DocumentPrefix + "x")
	vec := L2Normalize([]float32{1, 2, 3})
	if err := CachePut(ctx, db, key, Model, DocumentPrefix, vec); err != nil {
		t.Fatal(err)
	}
	if err := CachePut(ctx, db, key, Model, DocumentPrefix, vec); err != nil {
		t.Fatal(err)
	}
	if len(db.rows) != 1 {
		t.Fatalf("want 1 cache row, got %d", len(db.rows))
	}

	got, hit, err := CacheGet(ctx, db, key, Model, DocumentPrefix)
	if err != nil || !hit {
		t.Fatalf("CacheGet: hit=%v err=%v", hit, err)
	}
	if !bytes.Equal(PackVector(got), PackVector(vec)) {
		t.Fatal("CacheGet returned different vector")
	}

	if _, hit, err := CacheGet(ctx, db, CacheKey("missing"), Model, DocumentPrefix); err != nil || hit {
		t.Fatalf("miss: hit=%v err=%v", hit, err)
	}
}

// TestCachePutSQLIsInsertOnly pins the append-only-friendly shape of the
// insert: INSERT ... ON CONFLICT DO NOTHING, no UPDATE/DELETE/UPSERT-SET.
func TestCachePutSQLIsInsertOnly(t *testing.T) {
	for _, forbidden := range []string{"UPDATE", "DELETE", "DO UPDATE", "SET"} {
		if bytes.Contains([]byte(cachePutSQL), []byte(forbidden)) {
			t.Fatalf("cachePutSQL contains %q", forbidden)
		}
	}
	if !bytes.Contains([]byte(cachePutSQL), []byte("ON CONFLICT (content_hash, model, task_prefix) DO NOTHING")) {
		t.Fatal("cachePutSQL missing ON CONFLICT ... DO NOTHING")
	}
}
