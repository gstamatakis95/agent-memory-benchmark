-- +goose Up
-- Immutable enrichment ledger. INSERT-only. No UPDATE/DELETE on data rows.
-- DDL copied verbatim from docs/04-append-only.md section 2.
CREATE TABLE memory_enrichment_events (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    memory_id         BIGINT      NOT NULL REFERENCES memories(id),
    enrichment_version SMALLINT   NOT NULL,
    attempt           INT         NOT NULL,          -- 1-based per (memory,version)
    status            TEXT        NOT NULL
                        CHECK (status IN ('done','failed')),
    permanent         BOOLEAN     NOT NULL DEFAULT false,  -- 'dead' derived: failed AND permanent
    error_message     TEXT,                          -- NULL for done
    normalized_text   TEXT,                          -- NULL unless done
    lexemes           TEXT[],                        -- NULL unless done
    ts                TIMESTAMPTZ,                   -- content-derived timestamp, NULL unless done
    embedding         BYTEA,                         -- 3072-byte L2-normalized f32, NULL unless done
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 100);   -- insert-only: keep pages fully packed (this is the default)

-- Store the incompressible 3072-byte vector out-of-line, skip the futile compression pass.
ALTER TABLE memory_enrichment_events
    ALTER COLUMN embedding SET STORAGE EXTERNAL;

-- LINCHPIN INVARIANT: at most one success per (memory, version).
CREATE UNIQUE INDEX uq_enrich_success
    ON memory_enrichment_events (memory_id, enrichment_version)
    WHERE status = 'done';

-- Backoff support: locate failed attempts fast by (memory, version, recency).
CREATE INDEX ix_enrich_attempts
    ON memory_enrichment_events (memory_id, enrichment_version, created_at)
    WHERE status = 'failed';

-- Note: uq_enrich_success already serves the "is there a done row at V?" existence probe
-- as an index-only scan, so no separate anti-join support index is needed for successes.

-- Insert-only autovacuum tuning (freezing + visibility map health for index-only scans).
-- PG16 defaults: insert_threshold=1000, insert_scale_factor=0.2. Tighten so the VM stays current.
ALTER TABLE memory_enrichment_events SET (
    autovacuum_vacuum_insert_threshold = 2000,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

-- +goose Down
DROP TABLE memory_enrichment_events;
