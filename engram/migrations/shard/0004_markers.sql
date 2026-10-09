-- +goose Up
-- Shard schema v1, migration 4 of 4: the marker, visibility and derivation-rule functions (N115 to N117, N120, N135,
-- N144, N162), consolidation bookkeeping, the move helpers (engram_cleanup_namespace, engram_verify_fk), the grants of
-- every role and the self-checks that fail the migration when an invariant of the schema is violated. The functions are
-- created as engram_migrate (so the SECURITY DEFINER ones run with its BYPASSRLS); the standby helper is handed to
-- engram_stats_reader (N181); the grants and the checks run on the superuser connection, which GRANT treats as the
-- owner of each object. SQL bodies are validated at CREATE time, which is why this migration comes last.
SET LOCAL search_path = public;
SET LOCAL ROLE engram_migrate;

-- =============================================================================
-- Functions that need the tables (SQL bodies are validated at CREATE time)
-- =============================================================================

-- ---- Marker sets (N116) -----------------------------------------------------
-- The recall layer loads TWO sets once per request and passes them to every arm as parameters:
-- doc_tomb (engram_doc_hidden($doc_tomb, f.document_id, f.document_version)) and chunk_tomb (f.chunk_id <>
-- ALL($chunk_tomb)), both bounded by expunge lag (alert above 16 k entries; above it an arm switches to the NOT EXISTS
-- anti-join form). fact_hidden is NEVER an array (N116, N135): the 'invalidate' rows are permanent by design, so every
-- arm and predicate tests them per candidate by primary-key anti-join. These functions are the same selects, for tests,
-- engramctl and the SQL fallback.
--
-- DocTomb (N133c) is a jsonb object {document_id: up_to_version}: per document the GREATEST up_to_version over its open
-- tombstones (a document can be deleted, re-used and deleted again while the first marker still exists). A row is
-- hidden by it iff document_version <= that number, so the versions of a re-used document_id above it stay visible.
-- Purged tombstones are left out: their rows are gone.
-- +goose StatementBegin
CREATE FUNCTION engram_doc_tomb(p_ns uuid, p_pending_only boolean DEFAULT false) RETURNS jsonb
LANGUAGE sql STABLE AS $$
  SELECT coalesce(jsonb_object_agg(x.document_id, x.up_to), '{}'::jsonb)
    FROM (SELECT t.document_id, max(t.up_to_version) AS up_to
            FROM document_tombstones t
           WHERE t.namespace_id = p_ns AND t.expunge_state <> 'purged'
             AND (NOT p_pending_only OR t.expunge_state = 'pending')
           GROUP BY t.document_id) x;
$$;
-- +goose StatementEnd

-- The document half of the visibility predicate: is version p_ver of document p_doc covered by a tombstone in the set
-- p_tomb? Single-expression SQL, so the planner inlines it.
-- +goose StatementBegin
CREATE FUNCTION engram_doc_hidden(p_tomb jsonb, p_doc text, p_ver integer) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT p_ver <= coalesce((p_tomb ->> p_doc)::integer, 0);
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_chunk_tomb(p_ns uuid) RETURNS uuid[]
LANGUAGE sql STABLE AS $$
  SELECT coalesce(array_agg(t.chunk_id), '{}'::uuid[]) FROM chunk_tombstones t WHERE t.namespace_id = p_ns;
$$;
-- +goose StatementEnd

-- ---- The visibility predicate (N116, N117, N135): the one SQL every arm applies ------------------
-- Facts and chunks: visible(f) = (f.document_id, f.document_version) NOT COVERED BY DocTomb AND f.chunk_id NOT IN
-- ChunkTomb AND NO fact_hidden row of ANY cause, plus as_of = mentioned_at <= T on the immutable row. "Covered" is
-- document_version <= up_to_version (N133c).
-- +goose StatementBegin
CREATE FUNCTION engram_visible_facts(p_ns uuid, p_as_of timestamptz DEFAULT 'infinity') RETURNS TABLE (memory_id uuid)
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns) AS d, engram_chunk_tomb(p_ns) AS c)
  SELECT f.memory_id
    FROM facts f, m
   WHERE f.namespace_id = p_ns AND f.mentioned_at <= p_as_of
     AND NOT engram_doc_hidden(m.d, f.document_id, f.document_version)
     AND f.chunk_id <> ALL (m.c)
     AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = f.namespace_id AND h.memory_id = f.memory_id);
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_visible_chunks(p_ns uuid, p_as_of timestamptz DEFAULT 'infinity') RETURNS TABLE (chunk_id uuid)
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns) AS d, engram_chunk_tomb(p_ns) AS c)
  SELECT k.chunk_id
    FROM chunks k, m
   WHERE k.namespace_id = p_ns AND k.mentioned_at <= p_as_of
     AND NOT engram_doc_hidden(m.d, k.document_id, k.document_version) AND k.chunk_id <> ALL (m.c);
$$;
-- +goose StatementEnd

-- Observation version (O, v) is HIDDEN iff (0) no version row exists (FAIL CLOSED, C-18: DerivedPurge keeps a stub, so
-- the row exists for every purged version), or it is a stub, or (a) an input of its SEGMENT (root_version(v) <= w <= v)
-- names a document version covered by a tombstone in p_doc_tomb or a fact with a fact_hidden(INVALIDATE) row (cause
-- 'reextract' hides nothing derived, N135), or (b) a derived_hidden row covers it. The read path passes the PENDING
-- tombstones (engram_doc_tomb(ns, true)): once Materialize has finished a marker, (b) answers for it. A WRITER's commit
-- re-verification passes ALL open tombstones (engram_doc_tomb(ns, false), N120). Nothing here walks: a version that
-- commits after the marker and names the victim is hidden because inputs are read at query time.
-- +goose StatementBegin
CREATE FUNCTION engram_obs_version_hidden(p_ns uuid, p_obs uuid, p_ver integer, p_doc_tomb jsonb) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT NOT EXISTS (SELECT 1 FROM observation_versions v
                      WHERE v.namespace_id = p_ns AND v.observation_id = p_obs AND v.version = p_ver)
      OR EXISTS (
    SELECT 1
      FROM observation_versions v
     WHERE v.namespace_id = p_ns AND v.observation_id = p_obs AND v.version = p_ver
       AND (v.stub
         OR EXISTS (SELECT 1 FROM observation_inputs i
                     WHERE i.namespace_id = p_ns AND i.observation_id = p_obs
                       AND i.version BETWEEN v.root_version AND v.version
                       AND (engram_doc_hidden(p_doc_tomb, i.document_id, i.document_version)
                            OR EXISTS (SELECT 1 FROM fact_hidden h
                                        WHERE h.namespace_id = p_ns AND h.memory_id = i.fact_id
                                            AND h.cause = 'invalidate')))
         OR EXISTS (SELECT 1 FROM derived_hidden h
                     WHERE h.namespace_id = p_ns AND h.kind = 'observation' AND h.id = p_obs
                       AND h.root_version = v.root_version AND v.version >= h.from_version)));
$$;
-- +goose StatementEnd

-- Page version: the same rule with depth two (fact -> observation -> page; pages never feed observations): hidden iff
-- no row / a stub, or a fact input of its segment is tombstoned or invalidated, OR an observation-version input of its
-- segment is hidden by the observation rule (which fails closed on a purged version too), OR a derived_hidden row
-- covers it.
-- +goose StatementBegin
CREATE FUNCTION engram_page_version_hidden(p_ns uuid, p_page uuid, p_ver integer, p_doc_tomb jsonb) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT NOT EXISTS (SELECT 1 FROM page_versions pv
                      WHERE pv.namespace_id = p_ns AND pv.page_id = p_page AND pv.version = p_ver)
      OR EXISTS (
    SELECT 1
      FROM page_versions pv
     WHERE pv.namespace_id = p_ns AND pv.page_id = p_page AND pv.version = p_ver
       AND (pv.stub
         OR EXISTS (SELECT 1 FROM page_version_inputs i
                     WHERE i.namespace_id = p_ns AND i.page_id = p_page
                       AND i.version BETWEEN pv.root_version AND pv.version
                       AND i.kind = 'fact'
                       AND (engram_doc_hidden(p_doc_tomb, i.document_id, i.document_version)
                            OR EXISTS (SELECT 1 FROM fact_hidden h
                                        WHERE h.namespace_id = p_ns AND h.memory_id = i.source_id
                                            AND h.cause = 'invalidate')))
         OR EXISTS (SELECT 1 FROM page_version_inputs i
                     WHERE i.namespace_id = p_ns AND i.page_id = p_page
                       AND i.version BETWEEN pv.root_version AND pv.version
                       AND i.kind = 'observation'
                       AND engram_obs_version_hidden(p_ns, i.source_id, i.source_version, p_doc_tomb))
         OR EXISTS (SELECT 1 FROM derived_hidden h
                     WHERE h.namespace_id = p_ns AND h.kind = 'page' AND h.id = p_page
                       AND h.root_version = pv.root_version AND pv.version >= h.from_version)));
$$;
-- +goose StatementEnd

-- Observation versions an arm may return. Served(T) is the version CURRENT at T (effective_at <= T AND no meta row or
-- superseded_at > T); when it is hidden NOTHING is served for that observation at T (C-14: never an older version).
-- Retirement is filtered by its own time (retired_at IS NULL OR retired_at > T). The default T = infinity serves the
-- CURRENT version (the one without a meta row). A version with no visible source (proof_count = 0: every cited source
-- tombstoned or invalidated) is not served (A-19).
-- +goose StatementBegin
CREATE FUNCTION engram_visible_observation_versions(p_ns uuid, p_as_of timestamptz DEFAULT 'infinity')
RETURNS TABLE (ov_id uuid, observation_id uuid, version integer)
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns, true) AS dp, engram_doc_tomb(p_ns, false) AS da)
  SELECT v.ov_id, v.observation_id, v.version
    FROM observation_versions v
    JOIN observations o ON o.namespace_id = v.namespace_id AND o.observation_id = v.observation_id
    LEFT JOIN observation_version_meta s
           ON s.namespace_id = v.namespace_id AND s.observation_id = v.observation_id AND s.version = v.version
    CROSS JOIN m
   WHERE v.namespace_id = p_ns AND NOT v.stub
     AND (o.retired_at IS NULL OR o.retired_at > p_as_of)
     AND v.effective_at <= p_as_of
     AND (s.superseded_at IS NULL OR s.superseded_at > p_as_of)
     AND NOT engram_obs_version_hidden(p_ns, v.observation_id, v.version, m.dp)
     AND EXISTS (SELECT 1 FROM observation_version_sources src
                  WHERE src.namespace_id = v.namespace_id AND src.observation_id = v.observation_id
                      AND src.version = v.version
                    AND NOT engram_doc_hidden(m.da, src.document_id, src.document_version)
                    AND NOT EXISTS (SELECT 1 FROM fact_hidden h
                                     WHERE h.namespace_id = src.namespace_id AND h.memory_id = src.memory_id
                                         AND h.cause = 'invalidate'));
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_visible_page_versions(p_ns uuid, p_as_of timestamptz DEFAULT 'infinity')
RETURNS TABLE (page_id uuid, version integer)
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns, true) AS dp)
  SELECT pv.page_id, pv.version
    FROM page_versions pv
    JOIN pages p ON p.namespace_id = pv.namespace_id AND p.page_id = pv.page_id
    LEFT JOIN page_version_meta s
           ON s.namespace_id = pv.namespace_id AND s.page_id = pv.page_id AND s.version = pv.version
    CROSS JOIN m
   WHERE pv.namespace_id = p_ns AND NOT pv.stub
     AND (p.retired_at IS NULL OR p.retired_at > p_as_of)
     AND pv.effective_at <= p_as_of
     AND (s.superseded_at IS NULL OR s.superseded_at > p_as_of)
     AND NOT engram_page_version_hidden(p_ns, pv.page_id, pv.version, m.dp);
$$;
-- +goose StatementEnd

-- ---- The derivation commit rule (N120): the re-verification a writer runs under the SHARED derivation lock -----
-- Every writer of an observation or page version (ApplyBatch stage 2, degraded-mode root rebuilds and merges,
-- CommitPageVersion for page/v1 and page_full/v1) commits in one transaction that (1) try-locks the derivation lock
-- shared, (2) re-verifies EVERY rendered input in a fresh statement, (3) checks its base, (4) on any failure ROLLBACKs
-- and re-derives from current evidence; the lock is never held across the LLM call.
--   (2) fact inputs: every fact exists and is visible against the FULL marker sets (all open tombstones, chunk_tomb,
--       fact_hidden of both causes); observation-version inputs: NOT engram_obs_version_hidden(ns, o, v,
--       engram_doc_tomb(ns, false)), against all open tombstones, not only pending ones.
-- +goose StatementBegin
CREATE FUNCTION engram_facts_all_visible(p_ns uuid, p_fact_ids uuid[]) RETURNS boolean
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns, false) AS d, engram_chunk_tomb(p_ns) AS c),
       want AS (SELECT DISTINCT x AS memory_id FROM unnest(p_fact_ids) AS x)
  SELECT NOT EXISTS (
    SELECT 1
      FROM want w CROSS JOIN m
      LEFT JOIN facts f ON f.namespace_id = p_ns AND f.memory_id = w.memory_id
     WHERE f.memory_id IS NULL
        OR engram_doc_hidden(m.d, f.document_id, f.document_version)
        OR f.chunk_id = ANY (m.c)
        OR EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = p_ns AND h.memory_id = f.memory_id));
$$;
-- +goose StatementEnd

--   (0) FIRST, before anything else, the commit transaction asks whether THIS execution already committed (N144):
--       engram_derivation_commit_seen(ns, kind, id, commit_key) returns the version a previous attempt committed, or
--       NULL. A hit means "return that version and touch nothing" (no CAS, no blob delete, no re-render). Only a miss
--       runs the compare-and-set below. The CAS-first order of N143 turned a retry of a committed commit into "lost the
--       CAS": it deleted the markdown of the version it had committed (delta) or advanced current_version to a version
--       that did not exist (root rebuild) (C-1).
-- +goose StatementBegin
CREATE FUNCTION engram_derivation_commit_seen(p_ns uuid, p_kind text, p_id uuid, p_commit_key bytea) RETURNS integer
LANGUAGE sql STABLE AS $$
  SELECT CASE p_kind
           WHEN 'observation' THEN (SELECT v.version FROM observation_versions v
                                     WHERE v.namespace_id = p_ns AND v.observation_id = p_id
                                         AND v.commit_key = p_commit_key)
           WHEN 'page'        THEN (SELECT v.version FROM page_versions v
                                     WHERE v.namespace_id = p_ns AND v.page_id = p_id AND v.commit_key = p_commit_key)
         END;
$$;
-- +goose StatementEnd

--   (3) the base, as a COMPARE-AND-SET at commit (N143, N144, BaseCurrentAtCommit): the version the writer rendered
--       from is still CURRENT, and the same statement advances current_version, so two writers that rendered from the
--       same base cannot both commit (the SHARED derivation lock does not serialise them; the row lock of this UPDATE
--       does). It is the last check before the version rows are inserted:
--         UPDATE observations SET current_version = $expected + 1 WHERE ... AND current_version = $expected
--       EVERY writer passes the version it READ (root rebuilds included; $expected = the current_version at LoadPage /
--       the read of the batch; 0 for a root that has no version yet): there is no blind write and no "advance whatever
--       it holds" branch. A NULL expectation is refused, and NULL comes back only when no row advanced (the compare
--       lost, or the root row does not exist). NoPhantomVersion is "the current version row exists" (a stub counts,
--       N136); the order idempotency lookup -> CAS -> version insert preserves it (N144, N159). A delta or merge
--       (p_check_visible) additionally requires the base version to be VISIBLE; a root rebuild is written from live
--       sources only and may start from a hidden current version. NULL (zero rows) means the writer lost: ROLLBACK,
--       discard the rendered result, and delete only a blob this execution minted that no page_versions row names.
--       Inheriting root(base) and requiring only a visible base is rejected. VOLATILE: this is a write.
-- +goose StatementBegin
CREATE FUNCTION engram_derivation_base_cas(p_ns uuid, p_kind text, p_id uuid, p_expected integer,
                                           p_check_visible boolean DEFAULT true) RETURNS integer
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  v integer;
BEGIN
  IF p_expected IS NULL THEN
    RETURN NULL;                                  -- no blind writes (N144)
  END IF;
  IF p_kind = 'observation' THEN
    UPDATE observations o SET current_version = o.current_version + 1
     WHERE o.namespace_id = p_ns AND o.observation_id = p_id
       AND o.current_version = p_expected
       AND (NOT p_check_visible OR p_expected = 0
            OR NOT engram_obs_version_hidden(p_ns, p_id, p_expected, engram_doc_tomb(p_ns, false)))
    RETURNING o.current_version INTO v;
  ELSIF p_kind = 'page' THEN
    UPDATE pages g SET current_version = g.current_version + 1
     WHERE g.namespace_id = p_ns AND g.page_id = p_id
       AND g.current_version = p_expected
       AND (NOT p_check_visible OR p_expected = 0
            OR NOT engram_page_version_hidden(p_ns, p_id, p_expected, engram_doc_tomb(p_ns, false)))
    RETURNING g.current_version INTO v;
  END IF;
  RETURN v;   -- NULL: zero rows updated (lost the compare-and-set, or an unknown kind/id) -> discard
END $$;
-- +goose StatementEnd

-- The tombstone view of a DELETING document (N115, N136): the only thing GetDocument and
-- ListDocuments(include_deleting) return from the ack on. security_invoker: the caller's RLS applies.
CREATE VIEW document_tombstone_view WITH (security_invoker = true) AS
  SELECT d.namespace_id, d.document_id, d.state, d.deleted_at, t.up_to_version, t.operation_id
    FROM documents d
    CROSS JOIN LATERAL (SELECT x.up_to_version, x.operation_id
                          FROM document_tombstones x
                         WHERE x.namespace_id = d.namespace_id AND x.document_id = d.document_id
                         ORDER BY x.up_to_version DESC LIMIT 1) t
   WHERE d.state = 'deleting';

-- ---- Entity lookup through a SECURITY DEFINER wrapper (N131, P-5) ---------------------------------
-- pg_trgm's operators are not leakproof, so under RLS the planner refuses to push them into the GIN index scan. The
-- wrapper runs as the owner (BYPASSRLS), re-checks that p_ns IS the namespace in scope and filters on namespace_id
-- itself, so the index is used and the isolation stays exact. EXECUTE is granted to engram_app only through the general
-- function grant; the check below is the guard.
-- +goose StatementBegin
CREATE FUNCTION engram_entity_fuzzy(p_ns uuid, p_norm text, p_limit integer DEFAULT 5, p_min real DEFAULT 0.4)
RETURNS TABLE (entity_id uuid, canonical_name text, sim real)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
BEGIN
  IF p_ns IS DISTINCT FROM nullif(current_setting('engram.namespace_id', true), '')::uuid THEN
    RAISE EXCEPTION 'namespace % is not the namespace in scope', p_ns USING ERRCODE = '42501';
  END IF;
  RETURN QUERY
    SELECT e.entity_id, e.canonical_name, similarity(e.canonical_norm, p_norm)
      FROM entities e
     WHERE e.namespace_id = p_ns AND e.merged_into IS NULL
       AND e.canonical_norm % p_norm AND similarity(e.canonical_norm, p_norm) >= p_min
     ORDER BY similarity(e.canonical_norm, p_norm) DESC, e.entity_id
     LIMIT p_limit;
END $$;
-- +goose StatementEnd

-- ---- Consolidation bookkeeping (N95, H-15, H-23) ---------------------------------------------------
-- Pending facts of a namespace: VISIBLE facts above the watermark with no 'done' stamp, except those whose latest stamp
-- is 'failed' and younger than 7 days, oldest first.
-- +goose StatementBegin
CREATE FUNCTION engram_pending_facts(p_ns uuid, p_limit integer DEFAULT 200) RETURNS SETOF uuid
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns) AS d, engram_chunk_tomb(p_ns) AS c,
                    coalesce((SELECT s.watermark_memory_id FROM consolidation_state s WHERE s.namespace_id = p_ns),
                             '00000000-0000-0000-0000-000000000000'::uuid) AS wm)
  SELECT f.memory_id
    FROM facts f, m
   WHERE f.namespace_id = p_ns AND f.memory_id > m.wm
     AND NOT engram_doc_hidden(m.d, f.document_id, f.document_version)
     AND f.chunk_id <> ALL (m.c)
     AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = f.namespace_id AND h.memory_id = f.memory_id)
     AND NOT EXISTS (SELECT 1 FROM fact_consolidation c
                      WHERE c.namespace_id = f.namespace_id AND c.memory_id = f.memory_id AND c.note = 'done')
     AND NOT EXISTS (SELECT 1 FROM fact_consolidation l
                      WHERE l.namespace_id = f.namespace_id AND l.memory_id = f.memory_id
                        AND l.note = 'failed' AND l.stamped_at > now() - interval '7 days'
                        AND l.stamped_at = (SELECT max(x.stamped_at) FROM fact_consolidation x
                                             WHERE x.namespace_id = l.namespace_id AND x.memory_id = l.memory_id))
   ORDER BY f.memory_id
   LIMIT p_limit;
$$;
-- +goose StatementEnd

-- The watermark the consolidate sweep may advance to: the largest fact id strictly below the smallest UNCONSOLIDATED
-- fact (no visibility filter: a marker-hidden fact that Restore may reveal still counts, H-23), and never past a fact
-- whose ins_seq is above engram_seq_floor(now()) (C-19: the guard is the insertion sequence, which orders rows by
-- INSERT, not an id timestamp that a long transaction or a retry reusing minted ids can fall behind; ids are minted per
-- attempt inside the transaction). NULL = do not advance. A fact of a re-extracted chunk stops pinning the watermark
-- when the purge removes it (1 h).
-- +goose StatementBegin
CREATE FUNCTION engram_consolidation_watermark(p_ns uuid) RETURNS uuid
LANGUAGE sql STABLE AS $$
  WITH w AS (SELECT coalesce((SELECT s.watermark_memory_id FROM consolidation_state s WHERE s.namespace_id = p_ns),
                             '00000000-0000-0000-0000-000000000000'::uuid) AS wm),
       u AS (SELECT coalesce((SELECT f.memory_id
                                FROM facts f, w
                               WHERE f.namespace_id = p_ns AND f.memory_id > w.wm
                                 AND NOT EXISTS (SELECT 1 FROM fact_consolidation c
                                                  WHERE c.namespace_id = f.namespace_id AND c.memory_id = f.memory_id
                                                      AND c.note = 'done')
                               ORDER BY f.memory_id LIMIT 1),
                             'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid) AS first_open)
  SELECT f.memory_id
    FROM facts f, w, u
   WHERE f.namespace_id = p_ns AND f.memory_id > w.wm
     AND f.memory_id < u.first_open
     AND f.ins_seq < engram_seq_floor(now())
     -- N147: a moved-in namespace's ring has no sample below W_final for 10 min
     AND NOT EXISTS (SELECT 1 FROM namespace_ownership o
                      WHERE o.namespace_id = p_ns AND o.moved_in_at > now() - interval '10 minutes')
   ORDER BY f.memory_id DESC
   LIMIT 1;
$$;
-- +goose StatementEnd

-- Purge gate (H-17, N119, P-15): the row purge of a document starts only after every REGISTERED index/Kafka consumer
-- has passed the DocumentDeleted event of its tombstone. The tombstone stores that event's seq (the marker transaction
-- drew it), so the gate is one comparison with the cursors. Unregistered consumers (Kafka off) have no cursor row.
-- +goose StatementBegin
CREATE FUNCTION engram_consumers_passed(p_event_seq bigint) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT p_event_seq IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM outbox_cursors c
     WHERE c.consumer IN ('index', 'kafka') AND c.last_seq < p_event_seq);
$$;
-- +goose StatementEnd

-- ---- Move helpers (P-8, N160) ------------------------------------------------------------------------
-- <cols> of the frozen copy: generated and dropped columns are skipped, order is by name so both shards produce the
-- same list; no generated column exists in this schema, which is what the CI check pins. Plan refuses unless
-- engram_column_hash(table) is equal on source and target.
-- +goose StatementBegin
CREATE FUNCTION engram_copy_columns(p_table regclass) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attname)
    FROM pg_attribute a
   WHERE a.attrelid = p_table AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '';
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_column_hash(p_table regclass) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT md5(string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod), ',' ORDER BY a.attname))
    FROM pg_attribute a
   WHERE a.attrelid = p_table AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '';
$$;
-- +goose StatementEnd

-- Move cleanup (N93, N91): deletes the data rows of ONE namespace on this shard in bounded batches and NEVER touches
-- the namespace_ownership row (a moved_out row is a permanent fence value). SECURITY DEFINER, owned by engram_migrate:
-- engram_move has no DELETE on source data and cannot bypass RLS, so this is the only way a move frees the source.
-- EXECUTE is granted to engram_move and engram_admin only. It refuses unless the ownership row is moved_out (source
-- cleanup after cutover (c)) or incoming (target rollback). It is called by the cleanup activity while the catalog row
-- reads 'cleaning' (and once more, expecting 0, before 'done'), and by `restore cleanup-moved-out` for 'cleaning' and
-- 'done' moves (N170, N184(2)). Each call deletes at most p_batch rows from the first non-empty table in FK order and
-- returns the count; 0 means done. The outer DELETE carries namespace_id (P-19). Run engram_hnsw_ddl(..., 'drop')
-- FIRST: the namespace's partial indexes are dropped with DROP INDEX CONCURRENTLY, so no HNSW graph is repaired row by
-- row (N112). blob_tombstones, outbox, outbox_skipped and deletion_log are not touched (worklists and history). The
-- *_version_meta rows are deleted before their versions (their FK does not cascade, N136).
-- +goose StatementBegin
CREATE FUNCTION engram_cleanup_namespace(p_ns uuid, p_batch integer DEFAULT 10000) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  v_state ownership_state;
  v_tbl   text;
  v_n     bigint;
  -- content before markers; the list lives in engram_cleanup_order() because the return_move guard of the ownership
  -- trigger needs the very same tables (M0.2 review F5)
  c_order constant text[] := engram_cleanup_order();
BEGIN
  -- no set_config here (M0.2 review F15): the owner bypasses RLS, and a transaction-local scope GUC would outlive
  -- the call and re-scope the caller's transaction to p_ns
  SELECT o.state INTO v_state FROM namespace_ownership o WHERE o.namespace_id = p_ns;
  IF v_state IS NULL OR v_state NOT IN ('moved_out', 'incoming') THEN
    RAISE EXCEPTION 'refusing to clean up namespace % in ownership state %', p_ns, coalesce(v_state::text, '(none)')
      USING ERRCODE = '55006';
  END IF;
  FOREACH v_tbl IN ARRAY c_order LOOP
    IF v_tbl = 'entities' THEN
      UPDATE entities SET merged_into = NULL WHERE namespace_id = p_ns AND merged_into IS NOT NULL;
    END IF;
    EXECUTE format('DELETE FROM %I WHERE namespace_id = $1 AND (tableoid, ctid) IN '
                   '(SELECT tableoid, ctid FROM %I WHERE namespace_id = $1 LIMIT $2)',
                   v_tbl, v_tbl) USING p_ns, p_batch;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n > 0 THEN
      RETURN v_n;
    END IF;
  END LOOP;
  RETURN 0;
END $$;
-- +goose StatementEnd

-- VerifyFK (N88): orphan count per foreign key of the namespace-scoped tables, run by the mover on the target on the
-- COMPLETE copy (whole namespace, no range: an orphan is a copy defect, N160(4)). Every row must be 0 before cutover.
-- Generated from pg_constraint, so a new foreign key is covered without editing the mover. Run with the namespace in
-- scope.
-- +goose StatementBegin
CREATE FUNCTION engram_verify_fk(p_ns uuid) RETURNS TABLE (constraint_name text, child_table text, orphans bigint)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  c       record;
  v_join  text;
  v_nn    text;
  v_n     bigint;
BEGIN
  FOR c IN
    SELECT k.conname::text AS conname, k.conrelid, k.confrelid, k.conkey, k.confkey
      FROM pg_constraint k
     WHERE k.contype = 'f' AND k.conparentid = 0 AND k.connamespace = 'public'::regnamespace
       AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = k.conrelid AND a.attname = 'namespace_id' AND
                   NOT a.attisdropped)
     ORDER BY k.conrelid::regclass::text, k.conname
  LOOP
    SELECT string_agg(format('p.%I = ch.%I', pa.attname, ca.attname), ' AND ' ORDER BY u.ord),
           string_agg(format('ch.%I IS NOT NULL', ca.attname), ' AND ' ORDER BY u.ord)
      INTO v_join, v_nn
      FROM unnest(c.conkey, c.confkey) WITH ORDINALITY AS u (ck, pk, ord)
      JOIN pg_attribute ca ON ca.attrelid = c.conrelid  AND ca.attnum = u.ck
      JOIN pg_attribute pa ON pa.attrelid = c.confrelid AND pa.attnum = u.pk;
    EXECUTE format('SELECT count(*) FROM %s ch WHERE ch.namespace_id = $1 AND %s AND NOT EXISTS (SELECT 1 FROM %s p '
                   'WHERE %s)',
                   c.conrelid::regclass, v_nn, c.confrelid::regclass, v_join)
       INTO v_n USING p_ns;
    constraint_name := c.conname;
    child_table := c.conrelid::regclass::text;
    orphans := v_n;
    RETURN NEXT;
  END LOOP;
END $$;
-- +goose StatementEnd

-- N151: the eligible-rows estimate E of the cost-based semantic plan: the sum over the CURRENT versions of p_docs of
-- the counts of the requested fact types (p_types NULL = all), less the namespace's hidden fraction. The caller passes
-- the allowed-document set as ONE array; no per-element estimation happens in the planner. (The ARM SQL differs by
-- path, N164: `document_id = ANY ($1)` on the HNSW path always; `IN (SELECT unnest($1))` only on the exact path.)
-- +goose StatementBegin
CREATE FUNCTION engram_eligible_facts(p_ns uuid, p_docs text[], p_types text[] DEFAULT NULL) RETURNS bigint
LANGUAGE sql STABLE AS $$
  SELECT floor(coalesce(sum(CASE WHEN p_types IS NULL THEN v.fact_count
                                 ELSE (SELECT coalesce(sum((v.fact_count_by_type ->> t)::bigint), 0)
                                       FROM unnest(p_types) t) END), 0)
               * (1 - coalesce((SELECT s.hidden_fraction FROM namespace_stats s WHERE s.namespace_id = p_ns),
                               0)))::bigint
    FROM documents d
    JOIN document_versions v ON v.namespace_id = d.namespace_id AND v.document_id = d.document_id
        AND v.version = d.current_version
   WHERE d.namespace_id = p_ns AND d.state = 'active' AND d.document_id IN (SELECT unnest(p_docs));
$$;
-- +goose StatementEnd

-- N162: the subject's other members. The marker transaction of Invalidate(f) hides {f} plus every row returned here,
-- all stamped with one invalidation_op: the same-document, same-content_hash LIVE facts of the subject (the
-- re-extraction twins), found through facts, or through curation_log when the named fact was already purged. Unlike
-- N145 the rows already hidden (a reextract-hidden old-key twin, an earlier invalidation) are NOT filtered out: the
-- subject is the set, and the insert is ON CONFLICT DO NOTHING per (memory_id, cause). A twin created LATER is not
-- here: it gets its row at CommitChunk.
-- +goose StatementBegin
CREATE FUNCTION engram_invalidation_twins(p_ns uuid, p_memory uuid) RETURNS SETOF uuid
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns) AS d, engram_chunk_tomb(p_ns) AS c),
       src AS (SELECT f.document_id, f.content_hash FROM facts f WHERE f.namespace_id = p_ns AND f.memory_id = p_memory
               UNION ALL
               SELECT c.document_id, c.content_hash FROM curation_log c WHERE c.namespace_id = p_ns
                   AND c.memory_id = p_memory
               LIMIT 1)
  SELECT t.memory_id
    FROM src
    JOIN facts t ON t.namespace_id = p_ns AND t.document_id = src.document_id AND t.content_hash = src.content_hash
    CROSS JOIN m
   WHERE t.memory_id <> p_memory
     AND t.document_version >= coalesce((SELECT d.life_start FROM documents d WHERE d.namespace_id = p_ns
                                         AND d.document_id = src.document_id), 1)
     AND NOT engram_doc_hidden(m.d, t.document_id, t.document_version)
     AND t.chunk_id <> ALL (m.c);
$$;
-- +goose StatementEnd

-- N174(3): the curation subject of a fact, from facts, else the latest curation_log row of the memory_id; NULL if
-- neither.
-- +goose StatementBegin
CREATE FUNCTION engram_memory_subject(p_ns uuid, p_memory uuid) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT engram_subject_id('memory', s.document_id || ':' || encode(s.content_hash, 'hex'))
    FROM (SELECT f.document_id, f.content_hash, 1 AS pr FROM facts f
           WHERE f.namespace_id = p_ns AND f.memory_id = p_memory
          UNION ALL
          (SELECT c.document_id, c.content_hash, 2 FROM curation_log c
            WHERE c.namespace_id = p_ns AND c.memory_id = p_memory ORDER BY c.ins_seq DESC LIMIT 1)) s
   ORDER BY s.pr LIMIT 1;
$$;
-- +goose StatementEnd

-- N162: what Restore(g) does, resolved exactly by tag. status:
--   'ok'               fact_hidden(invalidate) row of g found: invalidation_op and the memory_ids stamped with it
--   'document_deleted' g's fact exists but its document version is covered by a tombstone (NOT_FOUND{DOCUMENT_DELETED})
--   'not_invalidated'  g's fact is live and has no invalidate row (NOT_INVALIDATED)
--   'not_found'        neither a fact nor an invalidate row of g exists (a purged document took both, C-12)
-- The caller deletes WHERE invalidation_op = I and derived_hidden(invalidation, cause_id = ANY (memory_ids::text))
-- under the exclusive derivation lock (N133(b)); this function only resolves the set.
-- +goose StatementBegin
CREATE FUNCTION engram_restore_resolve(p_ns uuid, p_memory uuid)
RETURNS TABLE (status text, invalidation_op uuid, memory_ids uuid[])
LANGUAGE sql STABLE AS $$
  WITH h AS (SELECT x.invalidation_op FROM fact_hidden x
              WHERE x.namespace_id = p_ns AND x.memory_id = p_memory AND x.cause = 'invalidate'),
       f AS (SELECT engram_doc_hidden(engram_doc_tomb(p_ns), f.document_id, f.document_version) AS doc_hidden
               FROM facts f WHERE f.namespace_id = p_ns AND f.memory_id = p_memory)
  SELECT CASE WHEN EXISTS (SELECT 1 FROM f WHERE doc_hidden) THEN 'document_deleted'
              WHEN EXISTS (SELECT 1 FROM h)                   THEN 'ok'
              WHEN EXISTS (SELECT 1 FROM f)                   THEN 'not_invalidated'
              ELSE 'not_found' END,
         (SELECT invalidation_op FROM h),
         coalesce((SELECT array_agg(y.memory_id) FROM fact_hidden y
                    WHERE y.namespace_id = p_ns AND y.cause = 'invalidate'
                      AND y.invalidation_op = (SELECT invalidation_op FROM h)), '{}'::uuid[]);
$$;
-- +goose StatementEnd

-- N162(3): the document purge deletes the invalidate rows (and their free-text reason) of the facts the tombstone
-- covers, BEFORE the fact rows are deleted (the join needs them) and with the curation_log as the second source for
-- facts a REPLACE purge already removed. One batch; returns the rows deleted, 0 = done. Run by the expunge as
-- engram_admin. The document cause already hides every derived version permanently (derived_hidden 'document'), so
-- nothing is lost.
-- +goose StatementBegin
CREATE FUNCTION engram_purge_document_invalidations(p_ns uuid, p_doc text, p_up_to integer, p_batch integer
                                                    DEFAULT 1000)
RETURNS bigint
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  v_n bigint;
BEGIN
  DELETE FROM fact_hidden h
   WHERE h.namespace_id = p_ns AND h.cause = 'invalidate'
     AND (h.namespace_id, h.memory_id) IN (
           SELECT p_ns, k.memory_id FROM (
             SELECT f.memory_id FROM facts f
              WHERE f.namespace_id = p_ns AND f.document_id = p_doc AND f.document_version <= p_up_to
             UNION
             SELECT c.memory_id FROM curation_log c
              WHERE c.namespace_id = p_ns AND c.document_id = p_doc AND c.document_version <= p_up_to) k
           JOIN fact_hidden z ON z.namespace_id = p_ns AND z.memory_id = k.memory_id AND z.cause = 'invalidate'
           LIMIT p_batch);
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END $$;
-- +goose StatementEnd

-- N179(1), N181: true iff a streaming standby replayed p_lsn (false with none; the shard twin of catalog_replicated).
-- The owner must be a member of pg_read_all_stats (pg_stat_get_wal_senders masks the rows otherwise); `config lint`
-- asserts it.
-- +goose StatementBegin
CREATE FUNCTION engram_standby_replayed(p_lsn pg_lsn) RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
  SELECT coalesce(bool_or(replay_lsn >= p_lsn), false) FROM pg_catalog.pg_stat_replication WHERE state = 'streaming'
$$;
-- +goose StatementEnd

RESET ROLE;
ALTER FUNCTION engram_standby_replayed(pg_lsn) OWNER TO engram_stats_reader;

-- =============================================================================
-- Grants. Only parent tables are granted: partitions stay ungranted, so a direct partition reference by
-- engram_app/engram_move fails with permission denied (a query through the parent checks parent privileges only).
-- =============================================================================
GRANT USAGE ON SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
-- ins_seq is a column default
GRANT USAGE ON SEQUENCE outbox_seq, engram_ins_seq TO engram_app, engram_move, engram_admin;
-- N125 (b'): the ready trigger reads last_value (w_final check)
GRANT SELECT ON SEQUENCE engram_ins_seq TO engram_move, engram_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
-- N93: the SECURITY DEFINER cleanup is callable by the move executor and admin only; the index and expunge helpers that
-- read shard-wide state are admin-only too.
REVOKE EXECUTE ON FUNCTION engram_cleanup_namespace(uuid, integer) FROM PUBLIC, engram_app, engram_relay;
REVOKE EXECUTE ON FUNCTION engram_standby_replayed(pg_lsn) FROM PUBLIC, engram_app, engram_relay;
GRANT EXECUTE ON FUNCTION pg_switch_wal() TO engram_move;                    -- N179(1): SealCopy (bootstrap, superuser)
REVOKE EXECUTE ON FUNCTION engram_consumers_passed(bigint) FROM PUBLIC, engram_app, engram_relay, engram_move;
-- P-9: the relay and the move executor must not reach the definer entity lookup (it would read any namespace's entity
-- names by setting the scope GUC); the sequence ring and the scheduler samplers are admin-only too.
REVOKE EXECUTE ON FUNCTION engram_entity_fuzzy(uuid, text, integer, real) FROM PUBLIC, engram_relay, engram_move;
REVOKE EXECUTE ON FUNCTION engram_seq_sample() FROM PUBLIC, engram_app, engram_relay, engram_move;
-- N147, N160: the sequence advance and the index-validity probe are for the mover and admin only.
REVOKE EXECUTE ON FUNCTION engram_seq_advance(bigint), engram_move_indexes_valid(uuid) FROM PUBLIC, engram_app,
    engram_relay;
GRANT SELECT ON shard_meta, ownership_transitions TO engram_app, engram_relay, engram_move, engram_admin;

-- engram_app: ordinary namespace transactions. Content: SELECT + INSERT only (no UPDATE, no DELETE; the purge belongs
-- to engram_admin). Marker tables: the verbs the marker transactions use.
GRANT SELECT, INSERT ON
  ingest_ledger, document_version_chunks, chunks, facts, fact_links, entity_mentions,
  observation_versions, observation_inputs, observation_version_sources, observation_version_meta,
  page_versions, page_version_meta, page_version_inputs,
  fact_vectors, chunk_vectors, observation_version_vectors, page_version_vectors,
  fact_consolidation, consolidation_proposals, consolidation_applied, token_usage_events, curation_log,
  deletion_log, outbox
  TO engram_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  documents, document_versions, entities, entity_aliases, tag_counts,
  observations, observation_sources, consolidation_batches, consolidation_state, batch_jobs,
  pages, page_sources, operations, idempotency_keys, token_usage, quota_counters,
  blob_tombstones, export_snapshots
  TO engram_app;
GRANT SELECT, INSERT, UPDATE ON namespace_stats, namespace_models TO engram_app;
-- Delete(document); the Expunge's state changes are engram_admin's
GRANT SELECT, INSERT ON document_tombstones TO engram_app;
-- N157: the marker transaction's final statement stamps the outbox seq it drew
GRANT UPDATE (event_seq) ON document_tombstones TO engram_app;
-- REPLACE / un-retire; Invalidate / Restore (cause 'invalidate' only; FinalizeVersion inserts 'reextract')
GRANT SELECT, INSERT, DELETE ON chunk_tombstones, fact_hidden TO engram_app;
-- the tombstone view of a DELETING document (N136)
GRANT SELECT ON document_tombstone_view TO engram_app;
-- Restore removes the rows with cause (invalidation, f)
GRANT SELECT, DELETE ON derived_hidden TO engram_app;
GRANT SELECT ON expunge_progress, vector_indexes TO engram_app;
-- fence read: plain SELECT under the shared advisory lock (no row lock)
GRANT SELECT ON namespace_ownership TO engram_app;
-- CreateNamespace's first 'active'/epoch-1 row (the trigger admits nothing else from this role)
GRANT INSERT ON namespace_ownership TO engram_app;
-- the delete freeze (active -> frozen/delete) is the only edge the trigger admits for this role
GRANT UPDATE (state, freeze_reason) ON namespace_ownership TO engram_app;

-- engram_relay: the outbox stream and its own cursor rows; nothing else (D6)
GRANT SELECT ON outbox TO engram_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox_cursors TO engram_relay;

-- engram_move: DML grants are table-level; the TARGET-ONLY restriction is the RESTRICTIVE policies move_target_*.
-- Content tables: INSERT only (immutable rows are loaded ON CONFLICT DO NOTHING and the reconcile re-copies missing
-- rows; it never deletes content, rollback uses engram_cleanup_namespace). Mutable tables and markers: full DML (the
-- reconcile merges and deletes). The mover READS outbox and outbox_cursors (relay drain, N124) and writes neither.
GRANT SELECT, INSERT ON
  ingest_ledger, document_version_chunks, chunks, facts, fact_links, entity_mentions,
  observation_versions, observation_inputs, observation_version_sources, observation_version_meta,
  page_versions, page_version_meta, page_version_inputs,
  fact_vectors, chunk_vectors, observation_version_vectors, page_version_vectors,
  fact_consolidation, consolidation_proposals, consolidation_applied, token_usage_events, curation_log,
  deletion_log
  -- outbox_skipped is shard-local (its seqs mean nothing on the target) and is not copied
  TO engram_move;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  namespace_ownership, namespace_stats, namespace_models, vector_indexes,
  documents, document_versions, entities, entity_aliases, tag_counts,
  observations, observation_sources, consolidation_batches, consolidation_state, batch_jobs,
  pages, page_sources, operations, idempotency_keys, token_usage, quota_counters,
  blob_tombstones, export_snapshots,
  document_tombstones, chunk_tombstones, fact_hidden, derived_hidden, expunge_progress
  TO engram_move;
-- the consumer-cursor wait (N160(6)); engram_seq_advance is SECURITY DEFINER, no floor is a move input
GRANT SELECT ON outbox, outbox_cursors TO engram_move;
-- N91: the loader switches off FK, insert-only and touch triggers for its own session only. The ownership trigger is
-- ENABLE ALWAYS and RLS is unaffected by replica mode.
GRANT SET ON PARAMETER session_replication_role TO engram_move;
GRANT EXECUTE ON FUNCTION engram_cleanup_namespace(uuid, integer) TO engram_move, engram_admin;

-- engram_admin: engramctl, Expunge purge, schedulers, retention. The only role that DELETEs content.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO engram_admin;
-- the machine is data nobody edits at run time
REVOKE INSERT, UPDATE, DELETE ON ownership_transitions FROM engram_admin;
GRANT SELECT ON schedulable_namespaces, purgeable_namespaces, engram_invalid_indexes,
  engram_index_runner_queue, engram_index_hygiene_due, engram_index_partition_hygiene, engram_old_snapshots,
  document_tombstone_view TO engram_admin;
GRANT EXECUTE ON FUNCTION engram_consumers_passed(bigint), engram_seq_sample() TO engram_admin;

-- goose's own version table is not part of the schema's privilege surface: no application role touches it.
REVOKE ALL ON goose_db_version FROM engram_admin;

-- =============================================================================
-- Self-checks: fail the migration if an invariant of this file is violated.
-- =============================================================================
-- +goose StatementBegin
DO $$
DECLARE
  missing text;
  c_content constant text[] := ARRAY[
    'ingest_ledger', 'document_version_chunks', 'chunks', 'facts', 'fact_links', 'entity_mentions',
    'observation_versions', 'observation_inputs', 'observation_version_sources', 'observation_version_meta',
    'page_versions', 'page_version_meta', 'page_version_inputs',
    'fact_vectors', 'chunk_vectors', 'observation_version_vectors', 'page_version_vectors'];
BEGIN
  -- 1. every table with a namespace_id column has RLS enabled + forced and the ns_isolation policy
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND (NOT c.relrowsecurity OR NOT c.relforcerowsecurity
          OR NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname = 'ns_isolation'));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without namespace RLS: %', missing;
  END IF;

  -- 2. tables WITHOUT namespace_id are exactly the allowlisted shard-level ones
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND NOT EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'namespace_id')
     -- extension-owned tables (PostGIS's spatial_ref_sys in the ParadeDB template database) are not ours
     AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
     AND c.relname NOT IN ('shard_meta', 'outbox_cursors', 'ownership_transitions', 'engram_seq_log',
                           'goose_db_version');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'unexpected tables without namespace_id: %', missing;
  END IF;

  -- 3. every table with a namespace_id column also carries tenant_id (denormalised on every row)
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND NOT EXISTS (SELECT 1 FROM pg_attribute b WHERE b.attrelid = c.oid AND b.attname = 'tenant_id');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without tenant_id: %', missing;
  END IF;

  -- 4. no non-admin role holds a privilege on any partition
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_inherits i ON i.inhrelid = c.oid
   WHERE n.nspname = 'public'
     AND (has_table_privilege('engram_app',   c.oid, 'SELECT, INSERT, UPDATE, DELETE')
       OR has_table_privilege('engram_move',  c.oid, 'SELECT, INSERT, UPDATE, DELETE')
       OR has_table_privilege('engram_relay', c.oid, 'SELECT, INSERT, UPDATE, DELETE'));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'non-admin role has direct privileges on partitions: %', missing;
  END IF;

  -- 5. shard_id appears only on namespace_ownership and the shard_meta identity row
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'shard_id' AND NOT a.attisdropped
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND c.relname NOT IN ('namespace_ownership', 'shard_meta');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'shard_id must only exist on namespace_ownership/shard_meta, found on: %', missing;
  END IF;

  -- 6. only engram_admin (and the owner) may bypass RLS
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('engram_app', 'engram_relay', 'engram_move') AND rolbypassrls)
      THEN
    RAISE EXCEPTION 'engram_app/engram_relay/engram_move must be NOBYPASSRLS';
  END IF;

  -- 7. the move executor (NOBYPASSRLS, see 6): may set session_replication_role, holds no
  --    DELETE on content, and the ownership trigger is ENABLE ALWAYS so replica mode cannot switch it off
  IF NOT has_parameter_privilege('engram_move', 'session_replication_role', 'SET') THEN
    RAISE EXCEPTION 'engram_move must be allowed to SET session_replication_role (N91)';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgrelid = 'namespace_ownership'::regclass
                   AND t.tgname = 'namespace_ownership_check' AND t.tgenabled = 'A') THEN
    RAISE EXCEPTION 'namespace_ownership_check must be ENABLE ALWAYS (N91)';
  END IF;

  -- 8. every table engram_move writes carries the three RESTRICTIVE target-only policies
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND c.relname <> 'namespace_ownership'
     AND (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname LIKE 'move_target_%' AND
          NOT p.polpermissive) <> 3;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without the move_target_* restrictive policies: %', missing;
  END IF;

  -- 9. the cleanup and entity-lookup functions are SECURITY DEFINER; cleanup is not executable by app/relay
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'engram_cleanup_namespace' AND prosecdef)
     OR has_function_privilege('engram_app', 'engram_cleanup_namespace(uuid, integer)', 'EXECUTE')
     OR has_function_privilege('engram_relay', 'engram_cleanup_namespace(uuid, integer)', 'EXECUTE') THEN
    RAISE EXCEPTION 'engram_cleanup_namespace must be SECURITY DEFINER and executable by engram_move/engram_admin only '
                    '(N93)';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'engram_entity_fuzzy' AND prosecdef) THEN
    RAISE EXCEPTION 'engram_entity_fuzzy must be SECURITY DEFINER (N131)';
  END IF;

  -- 10. the ownership state machine is loaded and carries the ready state and the rollback edges
  IF NOT EXISTS (SELECT 1 FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid
                  WHERE t.typname = 'ownership_state' AND e.enumlabel = 'ready') THEN
    RAISE EXCEPTION 'ownership_state lacks ready (N125)';
  END IF;
  IF (SELECT count(DISTINCT edge) FROM ownership_transitions
       WHERE edge IN ('ready_target', 'unready_target', 'activate_target', 'return_abort', 'reconcile_out',
                      'abort_move', 'thaw_move', 'cutover_c', 'restore_delete', 'restore_done',
                          'rollback_target')) <> 11 THEN
    RAISE EXCEPTION 'ownership_transitions lacks cutover or rollback edges (N125)';
  END IF;

  -- 11. content tables are insert-only: no UPDATE/DELETE for the app roles, a BEFORE UPDATE guard, fillfactor 100
  SELECT string_agg(x.t, ', ') INTO missing
    FROM unnest(c_content) AS x(t)
   WHERE has_table_privilege('engram_app',  x.t, 'UPDATE') OR has_table_privilege('engram_app',  x.t, 'DELETE')
      OR has_table_privilege('engram_move', x.t, 'UPDATE') OR has_table_privilege('engram_move', x.t, 'DELETE')
      OR has_table_privilege('engram_relay', x.t, 'UPDATE') OR has_table_privilege('engram_relay', x.t, 'DELETE')
      OR NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = x.t::regclass
                     AND (g.tgname = x.t || '_insert_only' OR g.tgname = 'ingest_ledger_append_only'))
      OR coalesce((SELECT 'fillfactor=100' = ANY (r.reloptions) FROM pg_class r WHERE r.oid = x.t::regclass),
                  true) = false;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'content tables must be insert-only for non-admin roles: %', missing;
  END IF;

  -- 11b. every insert-only table (class tag) has fillfactor 100, its partitions included (N113, N137)
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p', 'm')
     AND ((NOT c.relispartition AND c.relkind = 'r' AND obj_description(c.oid, 'pg_class') = 'class: insert-only')
          OR EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
                      WHERE i.inhrelid = c.oid AND obj_description(p.oid, 'pg_class') = 'class: insert-only'))
     AND NOT coalesce('fillfactor=100' = ANY (c.reloptions), false);
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'insert-only tables without fillfactor = 100: %', missing;
  END IF;

  -- 12. no generated column anywhere and no visibility column on a content table (N113); the ledger keeps the
  --     request's tags and the *_meta tables ARE the write-once superseded_at
  SELECT string_agg(c.relname || '.' || a.attname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND (a.attgenerated <> ''
          OR (c.relname = ANY (c_content) AND c.relname NOT IN ('ingest_ledger', 'observation_version_meta',
                                                                'page_version_meta')
              AND a.attname IN ('live', 'retired_at', 'purge_after', 'invalidated_at',
              'invalidation_reason', 'superseded_at', 'tags', 'tag_count', 'stale_write', 'stale_delete',
              'derived_from_deleted', 'hidden_by_invalidation')));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'generated or visibility columns on content tables: %', missing;
  END IF;

  -- 13. the vector tables and their parents are hash-partitioned
  IF (SELECT count(*) FROM pg_class WHERE relname IN ('facts', 'chunks', 'fact_links', 'entity_mentions',
                                                      'observation_versions',
        'fact_vectors', 'chunk_vectors', 'observation_version_vectors') AND relkind = 'p'
            AND relnamespace = 'public'::regnamespace) <> 8 THEN
    RAISE EXCEPTION 'the eight big tables must be hash-partitioned';
  END IF;

  -- 14. every table carries exactly one class tag (N137); only insert-only tables carry ins_seq, each with its
  --     (namespace_id, ins_seq) index; the content tables are all insert-only
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
     AND c.relname <> 'goose_db_version'
     AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
     AND coalesce(obj_description(c.oid, 'pg_class'), '') NOT IN
         ('class: insert-only', 'class: mutable', 'class: expiring', 'class: shard-local');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without a class tag: %', missing;
  END IF;
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
     AND ((obj_description(c.oid, 'pg_class') = 'class: insert-only')
          <> EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'ins_seq' AND
                     NOT a.attisdropped AND a.attnotnull AND a.atthasdef)
          OR (obj_description(c.oid, 'pg_class') = 'class: insert-only'
              AND NOT EXISTS (SELECT 1 FROM pg_index x
                               WHERE x.indrelid = c.oid
                                 AND (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.oid
                                      AND a.attnum = x.indkey[0]) = 'namespace_id'
                                 AND (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.oid
                                      AND a.attnum = x.indkey[1]) = 'ins_seq')));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'ins_seq and its (namespace_id, ins_seq) index must exist exactly on insert-only tables: %',
        missing;
  END IF;
  SELECT string_agg(x.t, ', ') INTO missing
    FROM unnest(c_content) AS x(t) WHERE obj_description(x.t::regclass, 'pg_class') <> 'class: insert-only';
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'content tables must be tagged insert-only: %', missing;
  END IF;

  -- 15. evidence outlives facts (N135): no foreign key from an evidence table to facts; the evidence tables carry
  --     their own document columns
  SELECT string_agg(k.conrelid::regclass::text, ', ') INTO missing
    FROM pg_constraint k
   WHERE k.contype = 'f' AND k.conparentid = 0 AND k.confrelid = 'facts'::regclass
     AND k.conrelid::regclass::text IN ('observation_inputs', 'observation_version_sources', 'page_version_inputs');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'evidence tables must not reference facts (N135): %', missing;
  END IF;

  -- 16. fact_hidden is keyed by cause (N135); proposals and applied ops are never updated or deleted by the app role
  -- (N43)
  IF NOT EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_attribute a ON a.attrelid = k.conrelid
                 AND a.attnum = ANY (k.conkey)
                  WHERE k.conrelid = 'fact_hidden'::regclass AND k.contype = 'p' AND a.attname = 'cause') THEN
    RAISE EXCEPTION 'fact_hidden primary key must include cause (N135)';
  END IF;
  -- 16b. an invalidation outlives its fact (N145): no foreign key from fact_hidden to facts
  IF EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = 'fact_hidden'::regclass AND k.contype = 'f') THEN
    RAISE EXCEPTION 'fact_hidden must not reference facts (N145)';
  END IF;
  -- 16c. an invalidation is tagged (N162): invalidate rows carry invalidation_op; the chain subject is class-encoded
  IF NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = 'fact_hidden'::regclass AND k.contype = 'c'
                  AND pg_get_constraintdef(k.oid) LIKE '%invalidation_op%')
     OR NOT EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = 'deletion_log'::regclass
                    AND a.attname = 'subject_class' AND NOT a.attisdropped) THEN
    RAISE EXCEPTION 'fact_hidden.invalidation_op CHECK or deletion_log.subject_class is missing (N162)';
  END IF;
  IF has_table_privilege('engram_app', 'consolidation_proposals', 'DELETE')
     OR has_table_privilege('engram_app', 'consolidation_proposals', 'UPDATE')
     OR has_table_privilege('engram_app', 'consolidation_applied', 'DELETE') THEN
    RAISE EXCEPTION 'engram_app must not UPDATE or DELETE consolidation proposals or applied ops (N43)';
  END IF;

  -- 17. only app and admin reach the definer entity lookup (P-9); admin and move are bounded like app (N133e)
  IF has_function_privilege('engram_relay', 'engram_entity_fuzzy(uuid, text, integer, real)', 'EXECUTE')
     OR has_function_privilege('engram_move', 'engram_entity_fuzzy(uuid, text, integer, real)', 'EXECUTE') THEN
    RAISE EXCEPTION 'engram_entity_fuzzy must not be executable by engram_relay or engram_move (P-9)';
  END IF;
  IF (SELECT count(*) FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole
       WHERE r.rolname IN ('engram_app', 'engram_move', 'engram_admin')
         AND 'statement_timeout=30s' = ANY (s.setconfig)
             AND 'idle_in_transaction_session_timeout=30s' = ANY (s.setconfig)) <> 3 THEN
    RAISE EXCEPTION 'engram_app, engram_move and engram_admin need 30 s statement and idle timeouts (N133e)';
  END IF;

  -- 18. every role commits locally (N122): no synchronous standby wait anywhere
  IF (SELECT count(*) FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole
       WHERE r.rolname IN ('engram_app', 'engram_move', 'engram_admin', 'engram_relay')
         AND 'synchronous_commit=local' = ANY (s.setconfig)) <> 4 THEN
    RAISE EXCEPTION 'every engram role must have synchronous_commit = local (N122)';
  END IF;
END $$;
-- +goose StatementEnd


-- +goose Down
-- Honest Down: drops the functions and the view of this migration and revokes every table, sequence and function
-- grant of the four roles (the tables are untouched). REFUSES on a provisioned shard, before anything is reverted: a
-- Down that stripped the grants and the visibility functions of a shard holding data would leave it unserviceable
-- (M0.2 review F6; data-losing rollbacks are done by restore, section 9.3). The cluster-wide privilege `SET ON
-- PARAMETER session_replication_role` of engram_move is left alone, like the roles: other databases use it.
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM shard_meta) OR EXISTS (SELECT 1 FROM namespace_ownership) THEN
    RAISE EXCEPTION '0004 Down refused: shard_meta or namespace_ownership holds rows (restore from backup instead)'
      USING ERRCODE = '55006';
  END IF;
END $$;
-- +goose StatementEnd
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM engram_app, engram_relay, engram_move, engram_admin;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM engram_app, engram_relay, engram_move, engram_admin;
REVOKE EXECUTE ON FUNCTION pg_switch_wal() FROM engram_move;
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'v' AND c.relname = ANY (ARRAY[
    'document_tombstone_view'])
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
    'engram_chunk_tomb', 'engram_cleanup_namespace', 'engram_column_hash', 'engram_consolidation_watermark',
    'engram_consumers_passed', 'engram_copy_columns', 'engram_derivation_base_cas',
    'engram_derivation_commit_seen', 'engram_doc_hidden', 'engram_doc_tomb', 'engram_eligible_facts',
    'engram_entity_fuzzy', 'engram_facts_all_visible', 'engram_invalidation_twins', 'engram_memory_subject',
    'engram_obs_version_hidden', 'engram_page_version_hidden', 'engram_pending_facts',
    'engram_purge_document_invalidations', 'engram_restore_resolve', 'engram_verify_fk', 'engram_visible_chunks',
    'engram_visible_facts', 'engram_visible_observation_versions', 'engram_visible_page_versions',
    'engram_standby_replayed'])
  LOOP
    EXECUTE format('DROP FUNCTION %s CASCADE', r.sig);
  END LOOP;
END $$;
-- +goose StatementEnd
