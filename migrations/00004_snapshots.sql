-- +goose Up
-- Snapshot publishing ledger (docs/07-snapshot-serving.md section 3),
-- append-only like memory_enrichment_events (docs/04-append-only.md): a
-- snapshot's "current" state is DERIVED from the latest 'published' event,
-- never a stored status flip.

-- Monotonic snapshot versions. Allocated once per build attempt; a failed
-- build simply burns its version rather than reusing one (never mutated).
CREATE SEQUENCE snapshot_version_seq;

CREATE TABLE snapshot_events (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_version   BIGINT      NOT NULL,
    enrichment_version SMALLINT    NOT NULL,
    event              TEXT        NOT NULL
                          CHECK (event IN ('built', 'published', 'failed')),
    s3_key             TEXT,                 -- set on 'published'; NULL for 'built'/'failed'
    doc_count          BIGINT,               -- set on 'built'/'published'
    manifest           JSONB,                -- the snapshot.Manifest JSON, set on 'built'/'published'
    error_message      TEXT,                 -- set on 'failed'
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_snapshot_events_version ON snapshot_events (snapshot_version, id);

-- "Current" published snapshot is DERIVED: the latest 'published' event,
-- ordered by (snapshot_version, id) so a concurrent-insert race still
-- resolves to the highest version/most-recent row. No UPDATE ever touches
-- this table (docs/04-append-only.md): a superseding publish is a new row.
CREATE VIEW current_snapshot AS
SELECT snapshot_version, enrichment_version, s3_key, doc_count, manifest, created_at
FROM   snapshot_events
WHERE  event = 'published'
ORDER  BY snapshot_version DESC, id DESC
LIMIT  1;

-- +goose Down
DROP VIEW current_snapshot;
DROP INDEX ix_snapshot_events_version;
DROP TABLE snapshot_events;
DROP SEQUENCE snapshot_version_seq;
