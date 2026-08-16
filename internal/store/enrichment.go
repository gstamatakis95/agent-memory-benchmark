package store

import (
	"context"
	"fmt"
	"time"
)

// DoneEvent is a successful enrichment fact (docs/04-append-only.md section 3,
// completion insert).
type DoneEvent struct {
	MemoryID       int64
	Version        int16
	Attempt        int
	NormalizedText string
	Lexemes        []string
	TS             *time.Time // content-derived timestamp; nil if none parsed
	Embedding      []byte     // 3072-byte L2-normalized little-endian float32
}

// FailedEvent is a failed enrichment attempt fact. CreatedAt is nil in
// production (the column defaults to now()); tests set it to append a
// pre-aged failure row instead of mutating history.
type FailedEvent struct {
	MemoryID     int64
	Version      int16
	Attempt      int
	Permanent    bool
	ErrorMessage string
	CreatedAt    *time.Time // TEST FIXTURE ONLY: explicit created_at
}

// Completion insert (docs/04-append-only.md section 3). The ON CONFLICT
// predicate MUST repeat the uq_enrich_success index predicate verbatim or
// Postgres cannot infer the arbiter. Never UPDATE: duplicate successes from
// zombie/overlapping workers collapse into at most one row per
// (memory, version).
const insertDoneSQL = `
INSERT INTO memory_enrichment_events
    (memory_id, enrichment_version, attempt, status,
     normalized_text, lexemes, ts, embedding)
VALUES ($1,$2,$3,'done',$4,$5,$6,$7)
ON CONFLICT (memory_id, enrichment_version) WHERE status='done'
DO NOTHING`

// InsertEnrichmentDone appends a success event; idempotent under the partial
// unique index uq_enrich_success.
func InsertEnrichmentDone(ctx context.Context, db DB, e DoneEvent) error {
	_, err := db.Exec(ctx, insertDoneSQL,
		e.MemoryID, e.Version, e.Attempt,
		e.NormalizedText, e.Lexemes, e.TS, e.Embedding)
	return err
}

// Failure insert (docs/04-append-only.md section 3): no ON CONFLICT —
// failures are distinct historical facts and are never deduped. created_at
// defaults to now() unless a test supplies an explicit fixture timestamp.
const insertFailedSQL = `
INSERT INTO memory_enrichment_events
    (memory_id, enrichment_version, attempt, status, permanent, error_message, created_at)
VALUES ($1,$2,$3,'failed',$4,$5, COALESCE($6::timestamptz, now()))`

// InsertEnrichmentFailed appends a failure event.
func InsertEnrichmentFailed(ctx context.Context, db DB, e FailedEvent) error {
	_, err := db.Exec(ctx, insertFailedSQL,
		e.MemoryID, e.Version, e.Attempt, e.Permanent, e.ErrorMessage, e.CreatedAt)
	return err
}

// pendingCoreSQL is the pending/claimable derivation from
// docs/04-append-only.md section 3: no success at version V (anti-join over
// uq_enrich_success), not permanently failed, under max_attempts, and past
// the exponential backoff window of the LATEST failed attempt.
//
// Deviation from the doc's sketch: the doc aggregates last_fail as
// max(created_at), but "latest failure" must mean the most recent ATTEMPT
// (highest attempt/id), not the max timestamp — a backdated fixture row (and,
// in general, any explicitly timestamped append) would otherwise never be able
// to age a memory back into the pending set without mutating history.
//
// Parameters: $1 version, $2 max_attempts, $3 backoff base interval.
const pendingCoreSQL = `
WITH fail AS (
    SELECT memory_id,
           count(*) AS n_fail,
           (array_agg(created_at ORDER BY attempt DESC, id DESC))[1] AS last_fail,
           bool_or(permanent) AS has_permanent
    FROM   memory_enrichment_events
    WHERE  enrichment_version = $1
      AND  status = 'failed'
    GROUP  BY memory_id
)
SELECT m.id
FROM   memories m
LEFT   JOIN fail f ON f.memory_id = m.id
WHERE  NOT EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id
            AND e.enrichment_version = $1
            AND e.status = 'done')
  AND  COALESCE(f.has_permanent, false) = false
  AND  COALESCE(f.n_fail, 0) < $2
  AND  (f.last_fail IS NULL
        OR f.last_fail < now() - ($3::interval * power(2, LEAST(f.n_fail, 6))))`

var (
	pendingIDsSQL   = pendingCoreSQL + "\nORDER BY m.id\nLIMIT $4"
	countPendingSQL = "SELECT count(*) FROM (" + pendingCoreSQL + ") p"
	isPendingSQL    = "SELECT EXISTS (SELECT 1 FROM (" + pendingCoreSQL + ") p WHERE p.id = $4)"
)

// pgInterval renders a Go duration as a Postgres interval literal for the
// $::interval backoff parameter.
func pgInterval(d time.Duration) string {
	return fmt.Sprintf("%d microseconds", d.Microseconds())
}

// PendingMemoryIDs returns up to limit memory ids that are claimable at the
// given version: not yet done, not dead, and past their backoff window.
func PendingMemoryIDs(ctx context.Context, db DB, version int16, maxAttempts int, backoffBase time.Duration, limit int) ([]int64, error) {
	rows, err := db.Query(ctx, pendingIDsSQL, version, maxAttempts, pgInterval(backoffBase), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// CountPending is the CountBacklog query: it uses the exact same predicate as
// the claim to avoid livelock (docs/04-append-only.md section 4).
func CountPending(ctx context.Context, db DB, version int16, maxAttempts int, backoffBase time.Duration) (int64, error) {
	var n int64
	err := db.QueryRow(ctx, countPendingSQL, version, maxAttempts, pgInterval(backoffBase)).Scan(&n)
	return n, err
}

// IsPending reports whether one memory is currently claimable at the version.
func IsPending(ctx context.Context, db DB, memoryID int64, version int16, maxAttempts int, backoffBase time.Duration) (bool, error) {
	var ok bool
	err := db.QueryRow(ctx, isPendingSQL, version, maxAttempts, pgInterval(backoffBase), memoryID).Scan(&ok)
	return ok, err
}

// DeadLetterIDs returns memories whose failures are permanent or exhausted at
// the version, via the dead_letter SQL function.
func DeadLetterIDs(ctx context.Context, db DB, version int16, maxAttempts int) ([]int64, error) {
	rows, err := db.Query(ctx,
		`SELECT memory_id FROM dead_letter($1::smallint, $2::int) ORDER BY memory_id`,
		version, maxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// IsDead reports whether one memory is dead-lettered at the version.
func IsDead(ctx context.Context, db DB, memoryID int64, version int16, maxAttempts int) (bool, error) {
	var ok bool
	err := db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM dead_letter($1::smallint, $2::int) d WHERE d.memory_id = $3)`,
		version, maxAttempts, memoryID).Scan(&ok)
	return ok, err
}

// Progress is the version-scoped enrichment progress for one conversation.
type Progress struct {
	Total     int64
	Done      int64
	Remaining int64
	Dead      int64
}

// GetProgress calls the enrichment_progress SQL function.
func GetProgress(ctx context.Context, db DB, conversationID string, version int16) (Progress, error) {
	var p Progress
	err := db.QueryRow(ctx,
		`SELECT total, done, remaining, dead FROM enrichment_progress($1, $2::smallint)`,
		conversationID, version).Scan(&p.Total, &p.Done, &p.Remaining, &p.Dead)
	return p, err
}

// EnrichedMemory is one row of a version-pinned fetch.
type EnrichedMemory struct {
	MemoryID       int64
	NormalizedText string
	Lexemes        []string
	TS             *time.Time
	Embedding      []byte
	ContentHash    []byte
	S3Key          string
}

// Version-pinned fetch (docs/04-append-only.md section 4 / 5): never mixes
// enrichment versions — lagging memories are simply absent.
const fetchAtVersionSQL = `
SELECT e.memory_id, e.normalized_text, e.lexemes, e.ts, e.embedding,
       m.content_hash, m.s3_key
FROM   enrichment_at_version e
JOIN   memories m ON m.id = e.memory_id
WHERE  m.conversation_id = $1 AND e.enrichment_version = $2
ORDER  BY e.memory_id`

// FetchAtVersion returns the version-homogeneous enriched corpus for a
// conversation.
func FetchAtVersion(ctx context.Context, db DB, conversationID string, version int16) ([]EnrichedMemory, error) {
	rows, err := db.Query(ctx, fetchAtVersionSQL, conversationID, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EnrichedMemory
	for rows.Next() {
		var em EnrichedMemory
		if err := rows.Scan(&em.MemoryID, &em.NormalizedText, &em.Lexemes,
			&em.TS, &em.Embedding, &em.ContentHash, &em.S3Key); err != nil {
			return nil, err
		}
		out = append(out, em)
	}
	return out, rows.Err()
}
