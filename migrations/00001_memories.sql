-- +goose Up
-- Raw memory reference. S3 is source of truth for bytes. (docs/02-storage.md E.2)
CREATE TABLE memories (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id TEXT NOT NULL,          -- LoCoMo conv / LongMemEval question id
    session_id     TEXT NOT NULL,
    turn_id        TEXT,                     -- LoCoMo dia_id or round id; NULL for session-granular
    s3_bucket      TEXT NOT NULL,
    s3_key         TEXT NOT NULL,            -- content-addressed: sha256 of blob
    byte_size      INTEGER NOT NULL,
    content_hash   BYTEA NOT NULL,           -- sha256 of the *raw blob*
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, session_id, turn_id)
);
CREATE INDEX idx_memories_conv ON memories (conversation_id);

-- Global, deterministic embedding cache (benchmark-legal: pure fn of text).
CREATE TABLE embedding_cache (
    content_hash BYTEA NOT NULL,   -- sha256 of the exact prefixed text
    model        TEXT  NOT NULL,   -- 'nomic-embed-text-v1.5'
    task_prefix  TEXT  NOT NULL,   -- 'search_document: '
    dims         SMALLINT NOT NULL DEFAULT 768,
    vector       BYTEA NOT NULL,   -- L2-normalized float32
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_hash, model, task_prefix)
);

-- +goose Down
DROP TABLE embedding_cache;
DROP TABLE memories;
