-- +goose Up
-- Derived "current state" views and functions, from docs/04-append-only.md section 2.

-- Current enrichment per memory: latest success at the highest version that has a success.
-- DISTINCT ON is the fastest single-row-per-group form and uses uq_enrich_success ordering.
CREATE VIEW latest_enrichment AS
SELECT DISTINCT ON (memory_id)
       memory_id, enrichment_version, normalized_text, lexemes, ts, embedding
FROM   memory_enrichment_events
WHERE  status = 'done'
ORDER  BY memory_id, enrichment_version DESC, id DESC;

-- Version-pinned enrichment for reproducible eval (client passes V):
CREATE VIEW enrichment_at_version AS
SELECT memory_id, enrichment_version, normalized_text, lexemes, ts, embedding
FROM   memory_enrichment_events
WHERE  status = 'done';   -- caller adds: AND enrichment_version = V

-- Version-scoped progress: total / done / remaining / dead AT a version.
-- Parameterized as a function to keep it version-pinned for benchmark comparability.
-- NOTE: p_conversation is TEXT (the doc sketch says BIGINT, but memories.conversation_id
-- is TEXT per docs/02-storage.md E.2 -- a BIGINT parameter would not even type-check).
-- +goose StatementBegin
CREATE FUNCTION enrichment_progress(p_conversation TEXT, p_version SMALLINT)
RETURNS TABLE(total BIGINT, done BIGINT, remaining BIGINT, dead BIGINT)
LANGUAGE sql STABLE AS $$
    SELECT
      count(*) AS total,
      count(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'done')) AS done,
      count(*) FILTER (WHERE NOT EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'done')) AS remaining,
      count(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'failed' AND e.permanent)) AS dead
    FROM memories m
    WHERE m.conversation_id = p_conversation;
$$;
-- +goose StatementEnd

-- Dead-letter view: memories whose failures are permanent or exhausted at version V.
-- +goose StatementBegin
CREATE FUNCTION dead_letter(p_version SMALLINT, p_max_attempts INT)
RETURNS TABLE(memory_id BIGINT) LANGUAGE sql STABLE AS $$
    SELECT m.id
    FROM memories m
    WHERE NOT EXISTS (
        SELECT 1 FROM memory_enrichment_events e
        WHERE e.memory_id = m.id AND e.enrichment_version = p_version
          AND e.status = 'done')
      AND (
        EXISTS (SELECT 1 FROM memory_enrichment_events e
                WHERE e.memory_id = m.id AND e.enrichment_version = p_version
                  AND e.status='failed' AND e.permanent)
        OR
        (SELECT count(*) FROM memory_enrichment_events e
         WHERE e.memory_id = m.id AND e.enrichment_version = p_version
           AND e.status='failed') >= p_max_attempts
      );
$$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION dead_letter(SMALLINT, INT);
DROP FUNCTION enrichment_progress(TEXT, SMALLINT);
DROP VIEW enrichment_at_version;
DROP VIEW latest_enrichment;
