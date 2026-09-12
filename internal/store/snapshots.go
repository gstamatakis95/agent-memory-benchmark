package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// AllocateSnapshotVersion draws the next monotonic snapshot version from
// snapshot_version_seq (migrations/00004_snapshots.sql). A version is never
// reused, even if the build that allocated it fails.
func AllocateSnapshotVersion(ctx context.Context, db DB) (int64, error) {
	var v int64
	err := db.QueryRow(ctx, `SELECT nextval('snapshot_version_seq')`).Scan(&v)
	return v, err
}

// SnapshotEvent is one append-only fact in snapshot_events: a build
// completing ('built'), a build's artifact going live ('published'), or a
// build failing ('failed'). "Current" is derived from the latest
// 'published' event (current_snapshot view) — there is no status flip.
type SnapshotEvent struct {
	SnapshotVersion   int64
	EnrichmentVersion int16
	Event             string // "built" | "published" | "failed"
	S3Key             string // set on "published"
	DocCount          int64  // set on "built"/"published"
	Manifest          []byte // JSON (snapshot.Manifest); may be nil
	ErrorMessage      string // set on "failed"
}

const insertSnapshotEventSQL = `
INSERT INTO snapshot_events
    (snapshot_version, enrichment_version, event, s3_key, doc_count, manifest, error_message)
VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, 0), $6, NULLIF($7, ''))`

// InsertSnapshotEvent appends one snapshot_events row. There is no
// UPDATE/DELETE surface for this table, by design.
func InsertSnapshotEvent(ctx context.Context, db DB, e SnapshotEvent) error {
	var manifest any
	if len(e.Manifest) > 0 {
		manifest = e.Manifest
	}
	_, err := db.Exec(ctx, insertSnapshotEventSQL,
		e.SnapshotVersion, e.EnrichmentVersion, e.Event, e.S3Key, e.DocCount, manifest, e.ErrorMessage)
	return err
}

// CurrentSnapshot reads the current_snapshot view: the latest 'published'
// event. ok is false when nothing has ever been published.
func CurrentSnapshot(ctx context.Context, db DB) (SnapshotEvent, bool, error) {
	var e SnapshotEvent
	var manifest []byte
	err := db.QueryRow(ctx, `
		SELECT snapshot_version, enrichment_version, s3_key, doc_count, manifest
		FROM   current_snapshot`).
		Scan(&e.SnapshotVersion, &e.EnrichmentVersion, &e.S3Key, &e.DocCount, &manifest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SnapshotEvent{}, false, nil
		}
		return SnapshotEvent{}, false, err
	}
	e.Event = "published"
	e.Manifest = manifest
	return e, true, nil
}

// EnrichedRow is one version-pinned enriched memory joined with its
// memories row, as consumed by the snapshot builder (internal/snapshotflow):
// it needs the S3 key and content hash (for Doc) alongside the enrichment
// payload, but NOT normalized_text (the builder re-derives lexemes are
// already stored; the raw text itself never leaves Postgres/S3 into a
// snapshot).
type EnrichedRow struct {
	MemoryID       int64
	ConversationID string
	SessionID      string
	Lexemes        []string
	TS             *time.Time
	Embedding      []byte
	ContentHash    []byte
	S3Key          string
}

const streamAtVersionAllSQL = `
SELECT e.memory_id, m.conversation_id, m.session_id, e.lexemes, e.ts, e.embedding,
       m.content_hash, m.s3_key
FROM   enrichment_at_version e
JOIN   memories m ON m.id = e.memory_id
WHERE  e.enrichment_version = $1
ORDER  BY e.memory_id`

// StreamAtVersionAll iterates the entire version-pinned enriched corpus
// (every conversation, unlike store.FetchAtVersion which is one
// conversation) row by row via pgx's native cursor, calling fn for each —
// the whole corpus is never materialized in memory, which matters at
// LongMemEval-S / full-corpus scale (docs/07 section 2). Iteration stops
// (and fn's error is returned) as soon as fn returns an error.
func StreamAtVersionAll(ctx context.Context, db DB, version int16, fn func(EnrichedRow) error) error {
	rows, err := db.Query(ctx, streamAtVersionAllSQL, version)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var r EnrichedRow
		if err := rows.Scan(&r.MemoryID, &r.ConversationID, &r.SessionID, &r.Lexemes, &r.TS,
			&r.Embedding, &r.ContentHash, &r.S3Key); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}
