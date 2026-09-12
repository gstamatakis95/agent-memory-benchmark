//go:build integration

package store_test

// Integration tests for the snapshot publishing ledger
// (migrations/00004_snapshots.sql): real Postgres via testcontainers, same
// harness as append_only_test.go (setupPG applies every embedded migration,
// so snapshot_version_seq / snapshot_events / current_snapshot are exactly
// the deployed schema).

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	store "example.com/agentmem/internal/store"
)

func TestAllocateSnapshotVersionIsMonotonic(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()

	v1, err := store.AllocateSnapshotVersion(ctx, db)
	require.NoError(t, err)
	v2, err := store.AllocateSnapshotVersion(ctx, db)
	require.NoError(t, err)

	require.Greater(t, v2, v1, "snapshot versions must be strictly increasing")
}

func TestCurrentSnapshotReturnsLatestPublished(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()

	version, err := store.AllocateSnapshotVersion(ctx, db)
	require.NoError(t, err)

	// Nothing published yet.
	_, ok, err := store.CurrentSnapshot(ctx, db)
	require.NoError(t, err)
	require.False(t, ok, "no snapshot has been published")

	manifest := []byte(`{"version":` + strconv.FormatInt(version, 10) + `,"doc_count":2}`)
	require.NoError(t, store.InsertSnapshotEvent(ctx, db, store.SnapshotEvent{
		SnapshotVersion:   version,
		EnrichmentVersion: currentVersion,
		Event:             "built",
		DocCount:          2,
		Manifest:          manifest,
	}))

	// A 'built' event alone is not "current" — only 'published' is.
	_, ok, err = store.CurrentSnapshot(ctx, db)
	require.NoError(t, err)
	require.False(t, ok, "a built (not yet published) snapshot must not read as current")

	require.NoError(t, store.InsertSnapshotEvent(ctx, db, store.SnapshotEvent{
		SnapshotVersion:   version,
		EnrichmentVersion: currentVersion,
		Event:             "published",
		S3Key:             "snapshots/snapshot-v1.tar.zst",
		DocCount:          2,
		Manifest:          manifest,
	}))

	cur, ok, err := store.CurrentSnapshot(ctx, db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, version, cur.SnapshotVersion)
	require.Equal(t, currentVersion, cur.EnrichmentVersion)
	require.Equal(t, "snapshots/snapshot-v1.tar.zst", cur.S3Key)
	require.Equal(t, int64(2), cur.DocCount)
	require.NotEmpty(t, cur.Manifest)

	// A second, later publish supersedes the first — current_snapshot never
	// flips a row, it just derives from the newest 'published' event.
	version2, err := store.AllocateSnapshotVersion(ctx, db)
	require.NoError(t, err)
	require.NoError(t, store.InsertSnapshotEvent(ctx, db, store.SnapshotEvent{
		SnapshotVersion:   version2,
		EnrichmentVersion: currentVersion,
		Event:             "published",
		S3Key:             "snapshots/snapshot-v2.tar.zst",
		DocCount:          3,
	}))

	cur2, ok, err := store.CurrentSnapshot(ctx, db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, version2, cur2.SnapshotVersion)
	require.Equal(t, "snapshots/snapshot-v2.tar.zst", cur2.S3Key)
}

func TestCurrentSnapshotRecordsFailure(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()

	version, err := store.AllocateSnapshotVersion(ctx, db)
	require.NoError(t, err)
	require.NoError(t, store.InsertSnapshotEvent(ctx, db, store.SnapshotEvent{
		SnapshotVersion:   version,
		EnrichmentVersion: currentVersion,
		Event:             "failed",
		ErrorMessage:      "boom: embed dims mismatch",
	}))

	_, ok, err := store.CurrentSnapshot(ctx, db)
	require.NoError(t, err)
	require.False(t, ok, "a failed build must never read as current")
}

func TestStreamAtVersionAllReturnsOnlyEnrichedRowsInOrder(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()

	// Three memories; only two get a 'done' enrichment at currentVersion.
	id1 := insertMemory(t, db, "conv1", "sess1", "t1")
	id2 := insertMemory(t, db, "conv1", "sess1", "t2")
	id3 := insertMemory(t, db, "conv1", "sess1", "t3") // left pending, never enriched

	ts := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, store.InsertEnrichmentDone(ctx, db, store.DoneEvent{
		MemoryID:       id2,
		Version:        currentVersion,
		Attempt:        1,
		NormalizedText: "second",
		Lexemes:        []string{"second"},
		TS:             &ts,
		Embedding:      make([]byte, 3072),
	}))
	require.NoError(t, store.InsertEnrichmentDone(ctx, db, store.DoneEvent{
		MemoryID:       id1,
		Version:        currentVersion,
		Attempt:        1,
		NormalizedText: "first",
		Lexemes:        []string{"first"},
		TS:             nil,
		Embedding:      make([]byte, 3072),
	}))

	var got []store.EnrichedRow
	err := store.StreamAtVersionAll(ctx, db, currentVersion, func(r store.EnrichedRow) error {
		got = append(got, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 2, "id3 was never enriched and must be absent")

	// ORDER BY e.memory_id ascending; id1 < id2 by insertion order.
	require.Equal(t, id1, got[0].MemoryID)
	require.Equal(t, id2, got[1].MemoryID)

	require.Equal(t, "conv1", got[0].ConversationID)
	require.Equal(t, "sess1", got[0].SessionID)
	require.Equal(t, []string{"first"}, got[0].Lexemes)
	require.Nil(t, got[0].TS)
	require.Len(t, got[0].Embedding, 3072)
	require.NotEmpty(t, got[0].ContentHash)
	require.NotEmpty(t, got[0].S3Key)

	require.Equal(t, []string{"second"}, got[1].Lexemes)
	require.NotNil(t, got[1].TS)
	require.WithinDuration(t, ts, *got[1].TS, time.Second)

	_ = id3 // referenced only to document why it's excluded
}

func TestStreamAtVersionAllStopsOnCallbackError(t *testing.T) {
	db := setupPG(t)
	ctx := context.Background()

	id1 := insertMemory(t, db, "conv1", "sess1", "t1")
	id2 := insertMemory(t, db, "conv1", "sess1", "t2")
	require.NoError(t, store.InsertEnrichmentDone(ctx, db, store.DoneEvent{
		MemoryID: id1, Version: currentVersion, Attempt: 1,
		NormalizedText: "x", Lexemes: []string{"x"}, Embedding: make([]byte, 3072),
	}))
	require.NoError(t, store.InsertEnrichmentDone(ctx, db, store.DoneEvent{
		MemoryID: id2, Version: currentVersion, Attempt: 1,
		NormalizedText: "y", Lexemes: []string{"y"}, Embedding: make([]byte, 3072),
	}))

	sentinel := context.Canceled // any distinguishable error
	calls := 0
	err := store.StreamAtVersionAll(ctx, db, currentVersion, func(r store.EnrichedRow) error {
		calls++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, calls, "iteration must stop at the first callback error")
}
