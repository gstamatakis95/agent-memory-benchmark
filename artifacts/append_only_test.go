package store_test

// Integration tests for the immutable enrichment ledger. These run against a
// real Postgres via testcontainers because every invariant here is enforced by
// the database (partial unique index, anti-join derivation), not by Go code.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const currentVersion = int16(1)

func setupPG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("app"),
		postgres.WithUsername("app"),
		postgres.WithPassword("app"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, applyMigrations(ctx, pool))
	require.NoError(t, installImmutabilityGuard(ctx, pool))
	return pool
}

// installImmutabilityGuard is TEST-ONLY. Any UPDATE or DELETE against the
// ledger — from anywhere in the codebase — becomes a loud failure. This is the
// single highest-value guard for the append-only requirement, because an
// accidental in-place update would otherwise pass every other test.
func installImmutabilityGuard(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
		CREATE OR REPLACE FUNCTION forbid_mutation() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'immutability violated: % on %', TG_OP, TG_TABLE_NAME;
		END $$;

		CREATE TRIGGER no_mutate_events
			BEFORE UPDATE OR DELETE ON memory_enrichment_events
			FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

		CREATE TRIGGER no_mutate_memories
			BEFORE UPDATE OR DELETE ON memories
			FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
	`)
	return err
}

// --- Invariant 1: at most one success per (memory, version) ------------------

func TestSuccessIsIdempotent(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	id := insertMemory(t, db, "conv1", "sess1", "turn1")

	for i := 0; i < 3; i++ {
		require.NoError(t, insertDone(ctx, db, id, currentVersion, i+1))
	}

	var n int
	require.NoError(t, db.QueryRow(ctx,
		`SELECT count(*) FROM memory_enrichment_events
		 WHERE memory_id=$1 AND enrichment_version=$2 AND status='done'`,
		id, currentVersion).Scan(&n))
	require.Equal(t, 1, n, "partial unique index must collapse duplicate successes")
}

// Pins the ON CONFLICT arbiter-inference gotcha: omitting the index predicate
// makes Postgres fail to find the arbiter. If someone "simplifies" the insert
// by dropping the WHERE clause, this test explains why they must not.
func TestOnConflictRequiresIndexPredicate(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	id := insertMemory(t, db, "conv1", "sess1", "turn1")

	_, err := db.Exec(ctx, `
		INSERT INTO memory_enrichment_events
			(memory_id, enrichment_version, attempt, status, normalized_text, embedding)
		VALUES ($1,$2,1,'done','x',$3)
		ON CONFLICT (memory_id, enrichment_version) DO NOTHING`, // no WHERE -> no arbiter
		id, currentVersion, make([]byte, 3072))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no unique or exclusion constraint")
}

// --- Invariant 2: failures are distinct historical facts ---------------------

func TestFailuresAreNotDeduped(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	id := insertMemory(t, db, "conv1", "sess1", "turn1")

	for i := 1; i <= 3; i++ {
		require.NoError(t, insertFailed(ctx, db, id, currentVersion, i, false, "boom"))
	}

	var n int
	require.NoError(t, db.QueryRow(ctx,
		`SELECT count(*) FROM memory_enrichment_events
		 WHERE memory_id=$1 AND status='failed'`, id).Scan(&n))
	require.Equal(t, 3, n)
}

// --- Invariant 3: pending is derived by absence (the free version bump) ------

func TestPendingDerivationAcrossVersions(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	id := insertMemory(t, db, "conv1", "sess1", "turn1")

	require.True(t, isPending(t, db, id, 1), "new memory must be pending at v1")

	require.NoError(t, insertDone(ctx, db, id, 1, 1))
	require.False(t, isPending(t, db, id, 1), "done at v1 -> not pending at v1")

	// The whole point of the append-only design: bumping the version constant
	// re-derives every memory as pending with zero writes.
	require.True(t, isPending(t, db, id, 2), "version bump -> pending again at v2")
}

// --- Invariant 4: backoff gating --------------------------------------------

func TestBackoffGatesRetry(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	id := insertMemory(t, db, "conv1", "sess1", "turn1")

	require.NoError(t, insertFailed(ctx, db, id, currentVersion, 1, false, "transient"))
	require.False(t, isPending(t, db, id, currentVersion),
		"a just-failed memory must wait out its backoff window")

	// Backdate the failure past the window. Note we insert a *new* row rather
	// than updating the old one — even test setup respects immutability.
	backdateViaFixtureTable(t, db, id, -1*time.Hour)
	require.True(t, isPending(t, db, id, currentVersion),
		"after the backoff window the memory becomes claimable again")
}

// --- Invariant 5: dead-letter derivation ------------------------------------

func TestPermanentFailureIsDeadImmediately(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	id := insertMemory(t, db, "conv1", "sess1", "turn1")

	require.NoError(t, insertFailed(ctx, db, id, currentVersion, 1, true, "512 dims"))

	require.False(t, isPending(t, db, id, currentVersion))
	require.True(t, isDead(t, db, id, currentVersion),
		"permanent failures must not be retried until max_attempts")
}

// --- Invariant 6: version-pinned fetch never mixes versions -----------------

func TestFetchIsVersionHomogeneous(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()
	a := insertMemory(t, db, "conv1", "sess1", "turn1")
	b := insertMemory(t, db, "conv1", "sess1", "turn2")

	require.NoError(t, insertDone(ctx, db, a, 1, 1))
	require.NoError(t, insertDone(ctx, db, a, 2, 1))
	require.NoError(t, insertDone(ctx, db, b, 1, 1)) // b lags at v1

	got := fetchAtVersion(t, db, "conv1", 2)
	require.Equal(t, []int64{a}, got,
		"a v2-pinned fetch must never fall back to v1 rows for lagging memories")
}
