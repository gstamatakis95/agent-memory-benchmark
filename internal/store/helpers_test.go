package store_test

// Thin wrappers that give append_only_test.go (the frozen spec) its bare
// helper identifiers by delegating to the real production API in
// internal/store. No SQL lives here except a read-only attempt count.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	store "example.com/agentmem/internal/store"
)

// Retry-policy knobs used by the invariant tests. The backoff base is larger
// than production's 1s default so "a just-failed memory is not pending" can
// never flake on a slow CI machine; the exponential formula is unchanged.
const (
	testMaxAttempts = 5
	testBackoffBase = time.Minute
)

func applyMigrations(ctx context.Context, db *pgxpool.Pool) error {
	return store.ApplyMigrations(ctx, db)
}

func insertMemory(t *testing.T, db *pgxpool.Pool, conv, sess, turn string) int64 {
	t.Helper()
	id, err := store.InsertMemory(context.Background(), db, store.Memory{
		ConversationID: conv,
		SessionID:      sess,
		TurnID:         turn,
		S3Bucket:       "test-bucket",
		S3Key:          "sha256/" + conv + "-" + sess + "-" + turn,
		ByteSize:       1,
		ContentHash:    []byte(conv + "/" + sess + "/" + turn),
	})
	require.NoError(t, err)
	return id
}

func insertDone(ctx context.Context, db *pgxpool.Pool, id int64, version int16, attempt int) error {
	ts := time.Now()
	return store.InsertEnrichmentDone(ctx, db, store.DoneEvent{
		MemoryID:       id,
		Version:        version,
		Attempt:        attempt,
		NormalizedText: "normalized text",
		Lexemes:        []string{"normalized", "text"},
		TS:             &ts,
		Embedding:      make([]byte, 3072),
	})
}

func insertFailed(ctx context.Context, db *pgxpool.Pool, id int64, version int16, attempt int, permanent bool, msg string) error {
	return store.InsertEnrichmentFailed(ctx, db, store.FailedEvent{
		MemoryID:     id,
		Version:      version,
		Attempt:      attempt,
		Permanent:    permanent,
		ErrorMessage: msg,
	})
}

func isPending(t *testing.T, db *pgxpool.Pool, id int64, version int16) bool {
	t.Helper()
	ok, err := store.IsPending(context.Background(), db, id, version, testMaxAttempts, testBackoffBase)
	require.NoError(t, err)
	return ok
}

func isDead(t *testing.T, db *pgxpool.Pool, id int64, version int16) bool {
	t.Helper()
	ok, err := store.IsDead(context.Background(), db, id, version, testMaxAttempts)
	require.NoError(t, err)
	return ok
}

func fetchAtVersion(t *testing.T, db *pgxpool.Pool, conv string, version int16) []int64 {
	t.Helper()
	rows, err := store.FetchAtVersion(context.Background(), db, conv, version)
	require.NoError(t, err)
	ids := make([]int64, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.MemoryID)
	}
	return ids
}

// backdateViaFixtureTable simulates an aged failure WITHOUT updating any row:
// it appends a NEW failed attempt whose created_at lies `offset` in the past
// (offset is negative). Because the backoff predicate gates on the created_at
// of the latest attempt, the aged row makes the memory claimable again.
func backdateViaFixtureTable(t *testing.T, db *pgxpool.Pool, id int64, offset time.Duration) {
	t.Helper()
	ctx := context.Background()

	var nFail int
	require.NoError(t, db.QueryRow(ctx,
		`SELECT count(*) FROM memory_enrichment_events
		 WHERE memory_id=$1 AND enrichment_version=$2 AND status='failed'`,
		id, currentVersion).Scan(&nFail))

	at := time.Now().Add(offset)
	require.NoError(t, store.InsertEnrichmentFailed(ctx, db, store.FailedEvent{
		MemoryID:     id,
		Version:      currentVersion,
		Attempt:      nFail + 1,
		Permanent:    false,
		ErrorMessage: "backdated fixture failure",
		CreatedAt:    &at,
	}))
}
