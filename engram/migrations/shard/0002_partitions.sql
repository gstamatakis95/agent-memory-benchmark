-- +goose Up
-- Shard schema v1, migration 2 of 4: the 16 hash partitions of the eight big tables, the table class tags, the
-- insert-only triggers and Row-Level Security (policies on parents AND partitions, ENABLEd and FORCEd). PLAN.md 9.2
-- lists this step as "16 hash partitions + RLS policies per table"; it is plain SQL here because the reference file
-- builds both with DO loops that need no Go. Everything runs as engram_migrate: one owner for every object.
SET LOCAL search_path = public;
SET LOCAL ROLE engram_migrate;

-- =============================================================================
-- Partitions: 16 hash partitions for the eight big tables (facts, chunks, links, mentions, observation versions and the
-- three partitioned vector tables). Storage parameters must be set per partition (a partitioned parent cannot carry
-- them). Content is insert-only, so fillfactor = 100 everywhere; the freeze age is lowered (N114: vacuum_freeze_min_age
-- = 10 M on content partitions) and insert-triggered autovacuum keeps the visibility map and freezing current; the
-- delete-oriented scale factors serve the expunge's purge batches. Vector partitions carry vacuum_index_cleanup = off
-- PERMANENTLY (N138): a touched graph is rebuilt by the index runner, never repaired by autovacuum.
-- =============================================================================
-- +goose StatementBegin
DO $$
DECLARE
  t text;
  i integer;
BEGIN
  FOREACH t IN ARRAY ARRAY['chunks', 'facts', 'fact_links', 'entity_mentions', 'observation_versions',
                           'fact_vectors', 'chunk_vectors', 'observation_version_vectors'] LOOP
    FOR i IN 0..15 LOOP
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES WITH (MODULUS 16, REMAINDER %s) '
        'WITH (fillfactor = 100, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 10000, '
        'autovacuum_vacuum_insert_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.01, '
        'autovacuum_freeze_min_age = 10000000, '
        'autovacuum_vacuum_cost_delay = 2, autovacuum_vacuum_cost_limit = 1000' ||
        CASE WHEN t LIKE '%vectors' THEN ', vacuum_index_cleanup = off' ELSE '' END || ')',
        t || '_p' || lpad(i::text, 2, '0'), t, i);
    END LOOP;
  END LOOP;
END $$;
-- +goose StatementEnd

-- =============================================================================
-- Table classes (N113, N137). The ONE list: COMMENT ON TABLE 'class: <tag>' on every table (the parent of a partitioned
-- table). engramlint sql checks it, the self-check at the end of this file enforces it, the (namespace_id, ins_seq)
-- indexes below are created from it, and N113, N124 and section 3 render from it.
-- =============================================================================
-- +goose StatementBegin
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'ingest_ledger', 'document_version_chunks', 'chunks', 'facts', 'fact_links', 'entity_mentions',
    'fact_vectors', 'chunk_vectors', 'observation_version_vectors', 'page_version_vectors',
    'observation_versions', 'observation_inputs', 'observation_version_sources', 'observation_version_meta',
    'page_versions', 'page_version_inputs', 'page_version_meta',
    'fact_consolidation', 'consolidation_proposals', 'consolidation_applied', 'deletion_log', 'curation_log'] LOOP
    EXECUTE format('COMMENT ON TABLE %I IS %L', t, 'class: insert-only');
  END LOOP;
  FOREACH t IN ARRAY ARRAY[
    'documents', 'document_versions', 'entities', 'entity_aliases', 'observations', 'observation_sources',
    'pages', 'page_sources', 'operations', 'document_tombstones', 'chunk_tombstones', 'fact_hidden',
    'derived_hidden', 'expunge_progress', 'quota_counters', 'token_usage', 'batch_jobs', 'consolidation_batches',
        'tag_counts',
    'consolidation_state', 'export_snapshots', 'idempotency_keys', 'blob_tombstones', 'namespace_models'] LOOP
    EXECUTE format('COMMENT ON TABLE %I IS %L', t, 'class: mutable');
  END LOOP;
  FOREACH t IN ARRAY ARRAY['token_usage_events', 'vector_indexes', 'namespace_stats'] LOOP
    EXECUTE format('COMMENT ON TABLE %I IS %L', t, 'class: expiring');
  END LOOP;
  FOREACH t IN ARRAY ARRAY[
    'shard_meta', 'outbox_cursors', 'ownership_transitions', 'namespace_ownership', 'outbox', 'outbox_skipped',
    'engram_seq_log'] LOOP
    EXECUTE format('COMMENT ON TABLE %I IS %L', t, 'class: shard-local');
  END LOOP;
END $$;
-- +goose StatementEnd

-- =============================================================================
-- Insert-only enforcement (N113): BEFORE UPDATE is refused on every content table. The grants below give no UPDATE
-- privilege to the application roles either; this is the second wall.
-- =============================================================================
-- +goose StatementBegin
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'document_version_chunks', 'chunks', 'facts', 'fact_links', 'entity_mentions',
    'observation_versions', 'observation_inputs', 'observation_version_sources', 'observation_version_meta',
    'page_versions', 'page_version_meta', 'page_version_inputs',
    'fact_vectors', 'chunk_vectors', 'observation_version_vectors', 'page_version_vectors',
    'fact_consolidation', 'consolidation_applied', 'token_usage_events', 'curation_log', 'deletion_log'] LOOP
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION engram_forbid_update()',
                   t || '_insert_only', t);
  END LOOP;
END $$;
-- +goose StatementEnd

-- =============================================================================
-- Row-Level Security (D2): policy ns_isolation on every table that has a namespace_id column, parents and partitions
-- alike, ENABLEd and FORCEd (the section 8 --check-rls test asserts both). Partitions carry the policy too, so a direct
-- partition reference (which no non-admin role is granted) is still confined. No tag predicate runs under RLS (N116).
-- =============================================================================
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT c.oid::regclass AS rel
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
     WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
  LOOP
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', r.rel);
    EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', r.rel);
    EXECUTE format(
      'CREATE POLICY ns_isolation ON %s '
      'USING (namespace_id = current_setting(''engram.namespace_id'')::uuid) '
      'WITH CHECK (namespace_id = current_setting(''engram.namespace_id'')::uuid)', r.rel);
  END LOOP;
END $$;
-- +goose StatementEnd

-- engram_move writes (INSERT/UPDATE/DELETE) only into a namespace whose ownership row is 'incoming', i.e. on the TARGET
-- before (b'). RESTRICTIVE policies are ANDed with ns_isolation and apply to engram_move only. namespace_ownership has
-- its own trigger/state machine.
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT c.oid::regclass AS rel
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
     WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
       AND c.relname <> 'namespace_ownership'
  LOOP
    EXECUTE format('CREATE POLICY move_target_ins ON %s AS RESTRICTIVE FOR INSERT TO engram_move '
                   'WITH CHECK ((SELECT engram_ns_is_incoming()))', r.rel);
    EXECUTE format('CREATE POLICY move_target_upd ON %s AS RESTRICTIVE FOR UPDATE TO engram_move '
                   'USING ((SELECT engram_ns_is_incoming())) WITH CHECK ((SELECT engram_ns_is_incoming()))', r.rel);
    EXECUTE format('CREATE POLICY move_target_del ON %s AS RESTRICTIVE FOR DELETE TO engram_move '
                   'USING ((SELECT engram_ns_is_incoming()))', r.rel);
  END LOOP;
END $$;
-- +goose StatementEnd

-- Relay role: all-namespace SELECT on outbox only (D6/N4).
CREATE POLICY relay_read_all ON outbox FOR SELECT TO engram_relay USING (true);

RESET ROLE;

-- +goose Down
-- Honest Down: drops the policies, triggers, class tags and the partitions. REFUSES while any partitioned table holds
-- rows (a Down must never silently destroy data; data-losing rollbacks are done by restore, section 9.3).
-- +goose StatementBegin
DO $$
DECLARE
  r record;
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['chunks', 'facts', 'fact_links', 'entity_mentions', 'observation_versions',
                           'fact_vectors', 'chunk_vectors', 'observation_version_vectors'] LOOP
    IF EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = t::regclass AND c.relkind = 'p')
       AND (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = t::regclass) > 0 THEN
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I)', t) INTO STRICT r;
      IF r.exists THEN
        RAISE EXCEPTION '0002 Down refused: % holds rows (restore from backup instead)', t USING ERRCODE = '55006';
      END IF;
    END IF;
  END LOOP;
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
           WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
  LOOP
    EXECUTE format('ALTER TABLE %s NO FORCE ROW LEVEL SECURITY', r.rel);
    EXECUTE format('ALTER TABLE %s DISABLE ROW LEVEL SECURITY', r.rel);
    EXECUTE format('DROP POLICY IF EXISTS ns_isolation ON %s', r.rel);
    EXECUTE format('DROP POLICY IF EXISTS move_target_ins ON %s', r.rel);
    EXECUTE format('DROP POLICY IF EXISTS move_target_upd ON %s', r.rel);
    EXECUTE format('DROP POLICY IF EXISTS move_target_del ON %s', r.rel);
    EXECUTE format('DROP POLICY IF EXISTS relay_read_all ON %s', r.rel);
  END LOOP;
  FOR r IN SELECT g.tgrelid::regclass AS rel, g.tgname FROM pg_trigger g
            WHERE g.tgname LIKE '%\_insert\_only' AND g.tgparentid = 0 AND NOT g.tgisinternal
  LOOP
    EXECUTE format('DROP TRIGGER %I ON %s', r.tgname, r.rel);
  END LOOP;
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
              AND obj_description(c.oid, 'pg_class') LIKE 'class: %'
  LOOP
    EXECUTE format('COMMENT ON TABLE %s IS NULL', r.rel);
  END LOOP;
  -- a partition referenced by a foreign key cannot be dropped while attached: detach it first, then drop it
  FOR r IN SELECT i.inhrelid::regclass AS part, i.inhparent::regclass AS parent FROM pg_inherits i
            WHERE i.inhparent = ANY (ARRAY['chunks', 'facts', 'fact_links', 'entity_mentions', 'observation_versions',
                                           'fact_vectors', 'chunk_vectors', 'observation_version_vectors']::regclass[])
  LOOP
    EXECUTE format('ALTER TABLE %s DETACH PARTITION %s', r.parent, r.part);
    EXECUTE format('DROP TABLE %s', r.part);
  END LOOP;
END $$;
-- +goose StatementEnd
