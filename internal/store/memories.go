package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Memory is one raw-memory reference row (docs/02-storage.md E.2). S3 holds
// the bytes; this row is the durable outbox entry — pending enrichment is
// derived from the absence of a done event, so inserting the row is the only
// write ingest performs.
type Memory struct {
	ConversationID string
	SessionID      string
	TurnID         string // empty means NULL (session-granular memory)
	S3Bucket       string
	S3Key          string
	ByteSize       int32
	ContentHash    []byte
}

const insertMemorySQL = `
INSERT INTO memories
    (conversation_id, session_id, turn_id, s3_bucket, s3_key, byte_size, content_hash)
VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7)
RETURNING id`

// InsertMemory appends one memory row and returns its generated id.
func InsertMemory(ctx context.Context, db DB, m Memory) (int64, error) {
	var id int64
	err := db.QueryRow(ctx, insertMemorySQL,
		m.ConversationID, m.SessionID, m.TurnID,
		m.S3Bucket, m.S3Key, m.ByteSize, m.ContentHash).Scan(&id)
	return id, err
}

var memoryCopyColumns = []string{
	"conversation_id", "session_id", "turn_id",
	"s3_bucket", "s3_key", "byte_size", "content_hash",
}

// CopyMemories bulk-inserts memories via the Postgres COPY protocol. It is
// the ingest fast path for later phases; ids are not returned (fetch them by
// the (conversation_id, session_id, turn_id) natural key if needed).
func CopyMemories(ctx context.Context, db CopyDB, ms []Memory) (int64, error) {
	rows := make([][]any, len(ms))
	for i, m := range ms {
		var turnID any
		if m.TurnID != "" {
			turnID = m.TurnID
		}
		rows[i] = []any{
			m.ConversationID, m.SessionID, turnID,
			m.S3Bucket, m.S3Key, m.ByteSize, m.ContentHash,
		}
	}
	return db.CopyFrom(ctx, pgx.Identifier{"memories"}, memoryCopyColumns, pgx.CopyFromRows(rows))
}
