-- +goose Up
-- Shard schema v1, migration 3 of 4: the index procedures of N112/N138 (engram_hnsw_ddl, engram_vector_index_plan,
-- engram_move_indexes_valid, engram_partitioned_index_ddl and the index-runner views), then the indexes of the
-- partitioned tables (created on the parent, propagated to the 16 partitions; PLAN.md 9.2 forbids CREATE INDEX
-- CONCURRENTLY on a parent, so a LATER index on these tables goes through `engramctl index`) and the
-- (namespace_id, ins_seq) index of every insert-only table, created from the class tags of 0002. Runs as
-- engram_migrate.
SET LOCAL search_path = public;
SET LOCAL ROLE engram_migrate;

-- =============================================================================
-- Index procedures (N112, N138, N78/P-10). Index DDL on partitioned tables cannot be written once as static DDL: CREATE
-- INDEX CONCURRENTLY cannot run in a transaction (so not inside a function) and cannot be issued on a partitioned
-- parent. These functions GENERATE the statements. The INDEX RUNNER (`engramctl index`, control host, role
-- engram_migrate: the one owner of index DDL, N138) executes them one at a time, each as its own top-level statement,
-- serialised per shard with maintenance_work_mem = 2.4 KB x vectors (<= 5 GB; shm_size is 32g on the shard image),
-- refuses to start a build while any backend_xmin is older than 5 min (engram_old_snapshots), and records progress in
-- vector_indexes.
-- =============================================================================

-- 1. Per-namespace partial HNSW. A hash-partitioned table keeps all rows of a namespace in exactly ONE partition
-- (page_version_vectors is not partitioned and is indexed on the table itself), so a namespace owns one index per
-- (vector table, embedding model): 120 namespaces per shard (N142) give ~360 expected small indexes per shard, cap 450,
-- alert at 400. Queries that must match the partial-index predicate carry the literal namespace id and model
-- (plan_cache_mode = force_custom_plan, so the bound parameters are constants at plan time).
-- p_action = 'create' | 'drop' | 'rebuild' (N152: REINDEX INDEX CONCURRENTLY, preceded by one DROP INDEX CONCURRENTLY
-- row (index_state 'leftover') per <name>_ccnew* leftover of a crashed rebuild). Names are deterministic, so
-- rollback_target and namespace delete drop by name and orphans cannot hide. 'create' is a plain CREATE INDEX
-- CONCURRENTLY, NEVER IF NOT EXISTS: the runner reads index_state first and acts on it ('invalid' -> run the drop
-- statement, then create; 'valid' -> nothing to build), because a retry that merely skips an existing INVALID index
-- leaves the namespace on the exact path indefinitely (P-8).
-- +goose StatementBegin
CREATE FUNCTION engram_hnsw_ddl(p_table text, p_ns uuid, p_model text, p_action text DEFAULT 'create')
RETURNS TABLE (partition_name text, index_name text, index_state text, statement text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_parent regclass;
  v_kind   "char";
  v_abbr   text;
  v_rem    integer;
  v_part   text;
  v_left   text;
BEGIN
  IF p_table NOT IN ('fact_vectors', 'chunk_vectors', 'observation_version_vectors', 'page_version_vectors') THEN
    RAISE EXCEPTION 'not a vector table: %', p_table USING ERRCODE = '22023';
  END IF;
  IF p_action NOT IN ('create', 'drop', 'rebuild') THEN
    RAISE EXCEPTION 'unknown action %', p_action USING ERRCODE = '22023';
  END IF;
  v_parent := p_table::regclass;
  SELECT c.relkind INTO v_kind FROM pg_class c WHERE c.oid = v_parent;
  v_abbr := CASE p_table WHEN 'fact_vectors' THEN 'fv' WHEN 'chunk_vectors' THEN 'cv'
                         WHEN 'page_version_vectors' THEN 'pv' ELSE 'ov' END;
  FOR v_part, v_rem IN
    SELECT c.relname::text, (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'remainder (\d+)'))[1]::integer
      FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = v_parent
    UNION ALL
    SELECT p_table, 0 WHERE v_kind = 'r'
  LOOP
    IF v_kind = 'r' OR satisfies_hash_partition(v_parent::oid, 16, v_rem, p_ns) THEN
      partition_name := v_part;
      index_name := format('%s_%s_%s', v_abbr, left(md5(p_ns::text), 12), left(md5(p_model), 6));
      index_state := coalesce((SELECT CASE WHEN x.indisvalid THEN 'valid' ELSE 'invalid' END
                                 FROM pg_index x JOIN pg_class ic ON ic.oid = x.indexrelid
                                WHERE ic.relname = index_name AND ic.relnamespace = 'public'::regnamespace), 'absent');
      IF p_action = 'rebuild' THEN
        FOR v_left IN SELECT ic.relname::text FROM pg_class ic
                       WHERE ic.relnamespace = 'public'::regnamespace AND ic.relname LIKE index_name || '\_ccnew%'
        LOOP
          index_state := 'leftover';
          statement := format('DROP INDEX CONCURRENTLY IF EXISTS %I', v_left);
          RETURN NEXT;
        END LOOP;
        index_state := coalesce((SELECT CASE WHEN x.indisvalid THEN 'valid' ELSE 'invalid' END
                                   FROM pg_index x JOIN pg_class ic ON ic.oid = x.indexrelid
                                  WHERE ic.relname = index_name AND ic.relnamespace = 'public'::regnamespace),
                                      'absent');
      END IF;
      statement := CASE p_action
        WHEN 'rebuild' THEN format('REINDEX INDEX CONCURRENTLY %I', index_name)
        WHEN 'create' THEN format('CREATE INDEX CONCURRENTLY %I ON %I USING hnsw (embedding halfvec_cosine_ops) '
                                  'WITH (m = 16, ef_construction = 128) WHERE namespace_id = %L AND embedding_model = '
                                  '%L',
                                  index_name, v_part, p_ns, p_model)
        ELSE format('DROP INDEX CONCURRENTLY IF EXISTS %I', index_name) END;
      RETURN NEXT;
    END IF;
  END LOOP;
END $$;
-- +goose StatementEnd

-- What the stats sweeper should do for one namespace: request an index at p_create_at vectors of the namespace's
-- CURRENT model (default 2,000; below it the arm does an exact scan of <= 2,000 rows, ~3.2 MB) when no row exists or
-- the last build FAILED, drop below p_drop_below (hysteresis). A requested/building row is the runner's: its lease, not
-- this plan, decides a retry. Run as engram_admin.
-- +goose StatementBegin
CREATE FUNCTION engram_vector_index_plan(p_ns uuid, p_create_at integer DEFAULT 2000, p_drop_below integer DEFAULT 1000)
RETURNS TABLE (vector_table text, embedding_model text, vectors bigint, indexed boolean, action text)
LANGUAGE sql STABLE AS $$
  WITH cur AS (SELECT m.embedding_model FROM namespace_models m WHERE m.namespace_id = p_ns),
       n AS (
         SELECT 'fact_vectors'::text AS t, count(*) AS c
           FROM fact_vectors v, cur WHERE v.namespace_id = p_ns AND v.embedding_model = cur.embedding_model
         UNION ALL
         SELECT 'chunk_vectors', count(*)
           FROM chunk_vectors v, cur WHERE v.namespace_id = p_ns AND v.embedding_model = cur.embedding_model
         UNION ALL
         SELECT 'observation_version_vectors', count(*)
           FROM observation_version_vectors v, cur WHERE v.namespace_id = p_ns
               AND v.embedding_model = cur.embedding_model
         UNION ALL
         SELECT 'page_version_vectors', count(*)
           FROM page_version_vectors v, cur WHERE v.namespace_id = p_ns AND v.embedding_model = cur.embedding_model)
  SELECT n.t, cur.embedding_model, n.c,
         coalesce(i.state = 'ready', false),
         CASE WHEN n.c >= p_create_at AND (i.vector_table IS NULL OR i.state = 'failed') THEN 'create'
              WHEN n.c <  p_drop_below AND i.state IN ('ready', 'failed') THEN 'drop'
              ELSE 'none' END
    FROM n CROSS JOIN cur
    LEFT JOIN vector_indexes i ON i.namespace_id = p_ns AND i.vector_table = n.t
        AND i.embedding_model = cur.embedding_model;
$$;
-- +goose StatementEnd

-- N175 readiness: true iff, for every (vector table, current model) with >= 2,000 copied vectors (index-only count on
-- the primary key), a vector_indexes request exists (not 'dropping') whose index, by the name of engram_hnsw_ddl, is
-- indisvalid AND indisready. Precondition of ready_target. SECURITY DEFINER.
-- "Zero rows is false" guards the vacuous bool_and: a REQUIRED (table, model) without a request row is false. It does
-- not make a namespace below 2,000 vectors unmovable: with no (table, model) at >= 2,000 vectors the answer is true
-- (such a namespace is served by the exact scan, N111/N112; CONFLICTS.md #17 records the reading). The reference
-- file's body ignored the counts, which refused ready for every small namespace and accepted it for an unrequested
-- large table (M0.2 review F1). With no namespace_models row there is no current model: any vector row then fails
-- closed.
-- +goose StatementBegin
CREATE FUNCTION engram_move_indexes_valid(p_ns uuid) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  v_model text;
  v_tbl   text;
  v_big   boolean;
BEGIN
  SELECT m.embedding_model INTO v_model FROM namespace_models m WHERE m.namespace_id = p_ns;
  FOREACH v_tbl IN ARRAY ARRAY['fact_vectors', 'chunk_vectors', 'observation_version_vectors',
                               'page_version_vectors'] LOOP
    IF v_model IS NULL THEN
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE namespace_id = $1)', v_tbl) INTO v_big USING p_ns;
      IF v_big THEN
        RETURN false;
      END IF;
      CONTINUE;
    END IF;
    -- at least 2,000 rows? stop counting at 2,000 (an index-only scan of the primary key's prefix)
    EXECUTE format('SELECT count(*) >= 2000 FROM (SELECT 1 FROM %I WHERE namespace_id = $1 AND embedding_model = $2 '
                   'LIMIT 2000) c', v_tbl) INTO v_big USING p_ns, v_model;
    IF NOT v_big THEN
      CONTINUE;
    END IF;
    IF NOT EXISTS (
      SELECT 1
        FROM vector_indexes i
       WHERE i.namespace_id = p_ns AND i.vector_table = v_tbl AND i.embedding_model = v_model AND i.state <> 'dropping'
         AND EXISTS (SELECT 1 FROM engram_hnsw_ddl(v_tbl, p_ns, v_model, 'create') d
                       JOIN pg_class ic ON ic.relname = d.index_name AND ic.relnamespace = 'public'::regnamespace
                       JOIN pg_index x ON x.indexrelid = ic.oid
                      WHERE x.indisvalid AND x.indisready)) THEN
      RETURN false;
    END IF;
  END LOOP;
  RETURN true;
END $$;
-- +goose StatementEnd


-- The index runner's work queue: requested builds, requested drops, and builds whose lease expired.
CREATE VIEW engram_index_runner_queue AS
  SELECT namespace_id, vector_table, embedding_model, state, requested_at, lease_until
    FROM vector_indexes
   WHERE state IN ('requested', 'dropping') OR (state = 'building' AND lease_until < now());

-- Hygiene (N138, N152): the unit is the PARTITION. A touched HNSW is DUE at purged_since_build >= max(1 % of
-- rows_at_build, 2 000) (the 2 k floor binds below 200 k rows). When any index on a vector partition is due, the runner
-- rebuilds EVERY touched index on that partition (engram_hnsw_ddl 'rebuild' = REINDEX INDEX CONCURRENTLY, after
-- dropping any <name>_ccnew* leftover; a crash mid-rebuild is repaired the same way) and only then runs one VACUUM
-- (INDEX_CLEANUP ON) of the partition: a partition is never vacuumed with index cleanup while an un-rebuilt touched
-- graph sits on it. Vector partitions carry vacuum_index_cleanup = off, so autovacuum never repairs a graph.
CREATE VIEW engram_index_hygiene_due AS
  SELECT namespace_id, vector_table, embedding_model, rows_at_build, purged_since_build
    FROM vector_indexes
   WHERE state = 'ready'
     AND purged_since_build >= greatest(2000, ceil(rows_at_build * 0.01));

-- The rebuild set: every touched ready index on a partition that has at least one DUE index.
CREATE VIEW engram_index_partition_hygiene AS
  WITH idx AS (
    SELECT i.namespace_id, i.vector_table, i.embedding_model, i.rows_at_build, i.purged_since_build, d.partition_name,
        d.index_name
      FROM vector_indexes i
      CROSS JOIN LATERAL engram_hnsw_ddl(i.vector_table, i.namespace_id, i.embedding_model, 'create') d
     WHERE i.state = 'ready'),
  due AS (SELECT DISTINCT partition_name FROM idx
           WHERE purged_since_build >= greatest(2000, ceil(rows_at_build * 0.01)))
  SELECT idx.* FROM idx JOIN due USING (partition_name) WHERE idx.purged_since_build > 0;

-- A build must not start while any backend holds an old snapshot (the index build waits for them all and blocks every
-- other build behind it): the runner refuses while this view is non-empty. Needs pg_read_all_stats for the runner's
-- role to see other roles' backends.
CREATE VIEW engram_old_snapshots AS
  SELECT pid, usename, backend_xmin, xact_start
    FROM pg_stat_activity
   WHERE backend_xmin IS NOT NULL AND coalesce(xact_start, query_start) < now() - interval '5 minutes'
     -- N152: CREATE INDEX CONCURRENTLY does not wait for autovacuum workers or walsenders
     AND backend_type = 'client backend'
     AND query !~* '^\s*(auto)?vacuum'                          -- ... nor for a VACUUM command
     AND pid <> pg_backend_pid();

-- 2. Ordinary index added to a partitioned table later (P-10): CREATE INDEX ... ON ONLY the parent (created INVALID,
-- nothing is built), then per partition CREATE INDEX CONCURRENTLY and ATTACH it; the parent becomes valid when the last
-- partition is attached. Returns the statements in order.
-- +goose StatementBegin
CREATE FUNCTION engram_partitioned_index_ddl(p_parent regclass, p_index text, p_definition text,
                                             p_using text DEFAULT 'btree', p_where text DEFAULT NULL)
RETURNS TABLE (step integer, statement text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_part record;
  v_n    integer := 1;
  v_tail text := format('USING %s (%s)%s', p_using, p_definition,
                        CASE WHEN p_where IS NULL THEN '' ELSE ' WHERE ' || p_where END);
BEGIN
  step := v_n; v_n := v_n + 1;
  statement := format('CREATE INDEX %I ON ONLY %s %s', p_index, p_parent, v_tail);
  RETURN NEXT;
  FOR v_part IN
    SELECT c.oid::regclass::text AS rel, c.relname::text AS relname
      FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = p_parent ORDER BY c.relname
  LOOP
    step := v_n; v_n := v_n + 1;
    statement := format('CREATE INDEX CONCURRENTLY %I ON %s %s', p_index || '_' || v_part.relname, v_part.rel, v_tail);
    RETURN NEXT;
    step := v_n; v_n := v_n + 1;
    statement := format('ALTER INDEX %I ATTACH PARTITION %I', p_index, p_index || '_' || v_part.relname);
    RETURN NEXT;
  END LOOP;
END $$;
-- +goose StatementEnd

-- A failed CREATE INDEX CONCURRENTLY leaves an INVALID index; engramctl index drops and rebuilds it.
CREATE VIEW engram_invalid_indexes AS
  SELECT c.relname AS index_name, t.relname AS table_name
    FROM pg_index x
    JOIN pg_class c ON c.oid = x.indexrelid
    JOIN pg_class t ON t.oid = x.indrelid
   WHERE NOT x.indisvalid AND c.relnamespace = 'public'::regnamespace;

-- =============================================================================
-- Indexes on the partitioned tables (created on the parent, propagated to every partition; later additions follow
-- engram_partitioned_index_ddl). There is NO shared vector index and no partial WHERE live index: vector indexes are
-- per namespace (engram_hnsw_ddl) and created by the stats sweeper at 2,000 vectors; below that the arms scan exactly
-- through the *_model_idx btrees.
-- =============================================================================

-- chunks
CREATE INDEX chunks_doc_idx       ON chunks (namespace_id, document_id, document_version);
CREATE INDEX chunks_mentioned_idx ON chunks (namespace_id, mentioned_at);
-- pg_search:begin
CREATE INDEX chunks_bm25 ON chunks
  USING bm25 (chunk_id, (text::pdb.unicode_words), namespace_id, mentioned_at)
  WITH (key_field = 'chunk_id');
-- pg_search:end

-- facts
CREATE INDEX facts_doc_idx          ON facts (namespace_id, document_id, document_version);
CREATE INDEX facts_mentioned_idx    ON facts (namespace_id, mentioned_at DESC);
-- temporal arm: two-sided probe around query_timestamp (N68)
CREATE INDEX facts_occurred_idx     ON facts (namespace_id, occurred_start)
  WHERE occurred_start IS NOT NULL;
-- explicit occurrence-window filters (lists, Recall filters), not the arm
CREATE INDEX facts_occurred_gist    ON facts
  USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
  WHERE occurred_start IS NOT NULL;
-- BM25 over the immutable text: no live/tag columns, so the index is never updated in place. Hidden hits (markers) are
-- removed by the arm's visibility join, not by the index; the arm over-fetches. pg_search:begin
CREATE INDEX facts_bm25 ON facts
  USING bm25 (memory_id, (text::pdb.unicode_words), namespace_id, mentioned_at)
  WITH (key_field = 'memory_id');
-- pg_search:end

-- vector side tables: exact-scan path (a namespace below 2,000 vectors reads <= 2,000 rows through this btree and sorts
-- by distance). The expunge deletes vectors through the FK cascade from facts.
CREATE INDEX fact_vectors_model_idx  ON fact_vectors  (namespace_id, embedding_model, memory_id);
CREATE INDEX chunk_vectors_model_idx ON chunk_vectors (namespace_id, embedding_model, chunk_id);
CREATE INDEX observation_version_vectors_model_idx
  ON observation_version_vectors (namespace_id, embedding_model, observation_id, version);
CREATE INDEX page_version_vectors_model_idx ON page_version_vectors (namespace_id, embedding_model, pv_id);
-- No (namespace_id, document_id) index on any vector table (N138): the exact path is driven from facts_doc_idx /
-- facts_mentioned_idx and joins the vector table by primary key.

-- fact_links: the PK serves forward expansion; the reverse index serves the other direction and the FK cascade of the
-- purge (N154: BOTH indexes are covering and both are in the hot set)
-- N154: both link indexes cover the hop (two index ranges, no heap)
CREATE INDEX fact_links_reverse_idx ON fact_links (namespace_id, dst_memory_id, src_memory_id, link_type)
    INCLUDE (weight);

-- entity_mentions
CREATE INDEX entity_mentions_entity_idx ON entity_mentions (namespace_id, entity_id, mentioned_at);

-- observation_versions: hash-partitioned like facts; ~1/20 of facts
CREATE INDEX observation_versions_effective_idx ON observation_versions (namespace_id, effective_at);
-- pg_search:begin
CREATE INDEX observation_versions_bm25 ON observation_versions
  USING bm25 (ov_id, (text::pdb.unicode_words), namespace_id, observation_id, effective_at)
  WITH (key_field = 'ov_id');
-- pg_search:end

-- pages: BM25 over the page text (a purged version is a stub with text = '') pg_search:begin
CREATE INDEX page_versions_bm25 ON page_versions
  USING bm25 (pv_id, (text::pdb.unicode_words), namespace_id, page_id, effective_at)
  WITH (key_field = 'pv_id');
-- pg_search:end

-- The sequence key of the stats sweeper, the export watermark and the consolidation watermark (N137): every insert-only
-- table has (namespace_id, ins_seq) (about 12 % of the footprint, N154; only their tail is hot; B-tree fill ~52 %,
-- N165). Created from the class tags, so a new insert-only table cannot miss it.
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT c.oid::regclass AS rel, c.relname
      FROM pg_class c
     WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
       AND obj_description(c.oid, 'pg_class') = 'class: insert-only'
  LOOP
    EXECUTE format('CREATE INDEX %I ON %s (namespace_id, ins_seq)', r.relname || '_ins_seq_idx', r.rel);
  END LOOP;
END $$;
-- +goose StatementEnd

RESET ROLE;

-- +goose Down
-- Honest Down: drops the indexes and index procedures of this migration (indexes hold no data of their own). The
-- per-namespace HNSW indexes are not migrations (the index runner owns them) and are left alone. REFUSES on a
-- provisioned shard before anything is reverted: dropping the BM25 and secondary indexes under live data would leave
-- it unserviceable (M0.2 review F6).
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM shard_meta) OR EXISTS (SELECT 1 FROM namespace_ownership) THEN
    RAISE EXCEPTION '0003 Down refused: shard_meta or namespace_ownership holds rows (restore from backup instead)'
      USING ERRCODE = '55006';
  END IF;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT c.oid::regclass AS idx FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('i', 'I') AND NOT c.relispartition
              AND c.relname = ANY (ARRAY[
    'chunk_vectors_model_idx', 'chunks_bm25', 'chunks_doc_idx', 'chunks_mentioned_idx',
    'entity_mentions_entity_idx', 'fact_links_reverse_idx', 'fact_vectors_model_idx', 'facts_bm25',
    'facts_doc_idx', 'facts_mentioned_idx', 'facts_occurred_gist', 'facts_occurred_idx',
    'observation_version_vectors_model_idx', 'observation_versions_bm25', 'observation_versions_effective_idx',
    'page_version_vectors_model_idx', 'page_versions_bm25'])
               OR (c.relnamespace = 'public'::regnamespace AND c.relkind IN ('i', 'I') AND NOT c.relispartition
                   AND c.relname LIKE '%\_ins\_seq\_idx')
  LOOP
    EXECUTE format('DROP INDEX %s', r.idx);
  END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'v' AND c.relname = ANY (ARRAY[
    'engram_index_hygiene_due', 'engram_index_partition_hygiene', 'engram_index_runner_queue',
    'engram_invalid_indexes', 'engram_old_snapshots'])
  LOOP
    EXECUTE format('DROP VIEW %s', r.rel);
  END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p
            WHERE p.pronamespace = 'public'::regnamespace AND p.proname = ANY (ARRAY[
    'engram_hnsw_ddl', 'engram_move_indexes_valid', 'engram_partitioned_index_ddl', 'engram_vector_index_plan'])
  LOOP
    EXECUTE format('DROP FUNCTION %s CASCADE', r.sig);
  END LOOP;
END $$;
-- +goose StatementEnd
