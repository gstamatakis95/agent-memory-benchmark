-- +goose Up
-- Shard schema v1, migration 1 of 4 (PLAN.md section 9.2): extensions, roles and role defaults, enumerations, helper
-- functions and every table with its inline constraints, triggers and non-partitioned indexes. Derived from
-- docs/plan/sql/shard_schema.sql (the reference file) by splitting it, adding goose annotations and the owner switch
-- below; the 16 hash partitions, the class tags, the insert-only triggers and RLS follow in 0002, the indexes and the
-- index procedures in 0003, the visibility and marker functions, the grants and the self-checks in 0004.
--
-- Who runs this: a SUPERUSER connection (the shard container's bootstrap role). The statements that need it (roles with
-- BYPASSRLS, extensions, GRANT ... ON PARAMETER, GRANT EXECUTE ON pg_switch_wal) run first; then SET LOCAL ROLE
-- engram_migrate makes engram_migrate (BYPASSRLS, N133e) the OWNER of every object created here, which is what lets the
-- SECURITY DEFINER functions bypass RLS and lets the index runner (`engramctl index`, role engram_migrate, N138) issue
-- index DDL. RESET ROLE at the end hands the connection back to goose, whose version-table insert needs it.
SET LOCAL search_path = public;

CREATE EXTENSION IF NOT EXISTS vector;
-- pg_search:begin
CREATE EXTENSION IF NOT EXISTS pg_search;
-- pg_search:end
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Roles (N133e): exactly engram_app, engram_relay, engram_move, engram_admin, engram_migrate (plus the NOLOGIN
-- engram_stats_reader of N181). The worker connects as engram_app and the Expunge purge as engram_admin: there is no
-- worker role and no expunge role. Passwords are set by provisioning (section 9), never here.
-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_migrate') THEN
    CREATE ROLE engram_migrate LOGIN BYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN
    CREATE ROLE engram_app LOGIN NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_relay') THEN
    CREATE ROLE engram_relay LOGIN NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_move') THEN
    CREATE ROLE engram_move LOGIN NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_admin') THEN
    CREATE ROLE engram_admin LOGIN BYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_stats_reader') THEN
    CREATE ROLE engram_stats_reader NOLOGIN;       -- N181: owns engram_standby_replayed and nothing else
  END IF;
END $$;
-- +goose StatementEnd
GRANT pg_read_all_stats TO engram_stats_reader;
GRANT CREATE ON SCHEMA public TO engram_migrate;

-- Role defaults (N82, N122). Row and advisory waits end after lock_timeout; exclusive takers raise it to 35 s with SET
-- LOCAL for their single attempt. statement_timeout stays the outer bound. synchronous_commit = local on every role: no
-- synchronous standby exists on a shard (N122) or in the catalog (N163). engram_migrate carries no timeout: index
-- builds and migrations legitimately run for minutes (the lock_timeout of 5 s and the retries are engramctl's, 9.2).
ALTER ROLE engram_app   SET lock_timeout = '2s';
ALTER ROLE engram_app   SET statement_timeout = '30s';
ALTER ROLE engram_app   SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_app   SET synchronous_commit = 'local';
ALTER ROLE engram_move  SET lock_timeout = '10s';
-- N133e/C-19: every outbox-writing role is bounded
ALTER ROLE engram_move  SET statement_timeout = '30s';
ALTER ROLE engram_move  SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_move  SET synchronous_commit = 'local';
ALTER ROLE engram_admin SET lock_timeout = '10s';
ALTER ROLE engram_admin SET statement_timeout = '30s';
ALTER ROLE engram_admin SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_admin SET synchronous_commit = 'local';
ALTER ROLE engram_relay SET synchronous_commit = 'local';

SET LOCAL ROLE engram_migrate;

-- -----------------------------------------------------------------------------
-- Enumerations (closed sets; extended with ALTER TYPE ... ADD VALUE in a migration)
-- -----------------------------------------------------------------------------
CREATE TYPE fact_type        AS ENUM ('world', 'experience');
CREATE TYPE link_type        AS ENUM ('entity', 'temporal', 'semantic', 'causal');
CREATE TYPE ownership_state  AS ENUM ('incoming', 'ready', 'active', 'frozen', 'moved_out');
CREATE TYPE document_state   AS ENUM ('active', 'deleting', 'deleted');
CREATE TYPE version_status   AS ENUM ('ingesting', 'active', 'superseded', 'deleted');
CREATE TYPE update_mode      AS ENUM ('replace', 'append');
CREATE TYPE operation_state  AS ENUM ('PENDING', 'RUNNING', 'DEFERRED', 'SUCCEEDED',
                                      'FAILED', 'CANCELLED');

-- -----------------------------------------------------------------------------
-- Helper functions (prefixed engram_ because they live in public)
-- -----------------------------------------------------------------------------

-- UUIDv7 for ops scripts and tests only; the application mints ids (google/uuid v7).
-- +goose StatementBegin
CREATE FUNCTION engram_uuid_v7() RETURNS uuid
LANGUAGE sql VOLATILE PARALLEL SAFE AS $$
  SELECT encode(
           set_bit(
             set_bit(
               overlay(uuid_send(gen_random_uuid())
                       PLACING substring(int8send((extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3)
                       FROM 1 FOR 6),
               52, 1),
             53, 1),
           'hex')::uuid;
$$;
-- +goose StatementEnd

-- The smallest UUIDv7 at or after a timestamp: a key lower bound for ops scripts and tests. No schema function and no
-- move uses it any more: the move re-copies by ins_seq (N137) and the consolidation watermark is bounded by
-- engram_seq_floor. Byte order of a uuid = timestamp, version nibble 7, variant bits 10.
-- +goose StatementBegin
CREATE FUNCTION engram_uuid_v7_floor(ts timestamptz) RETURNS uuid
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT encode(substring(int8send((extract(epoch FROM ts) * 1000)::bigint) FROM 3)
                || '\x7000'::bytea || '\x8000000000000000'::bytea, 'hex')::uuid;
$$;
-- +goose StatementEnd

-- Insertion sequence of the insert-only class (N137). nextval is drawn at INSERT, not at commit, so ins_seq is
-- commit-ordered only within the writer lifetime (30 s statements, 30 s idle gaps).
CREATE SEQUENCE engram_ins_seq AS bigint CACHE 1;

-- A stored consolidation proposal records the version each update/merge op was rendered from (N121): ApplyBatch applies
-- an op only while the observation is still at its base_version (N120).
-- +goose StatementBegin
CREATE FUNCTION engram_ops_have_base(ops jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NOT EXISTS (SELECT 1 FROM jsonb_array_elements(ops) AS e
                      WHERE e ->> 'kind' IN ('update', 'merge') AND jsonb_typeof(e -> 'base_version') IS DISTINCT
                          FROM 'number');
$$;
-- +goose StatementEnd

-- Advisory-lock keys (N113): independent hashes in one key space. IMMUTABLE. A hash collision only over-serialises two
-- namespaces or documents, never under-fences one.
-- +goose StatementBegin
CREATE FUNCTION engram_ns_fence_key(ns uuid) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtextextended(ns::text, 0) $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_ns_derivation_key(ns uuid) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtextextended(ns::text, 1) $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_doc_lock_keys(ns uuid, doc text) RETURNS TABLE (k1 integer, k2 integer)
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtext(ns::text), hashtext(doc) $$;
-- +goose StatementEnd

-- N150, N159, N162: the per-SUBJECT lock key of the marker writers. The subject is the encoded `class:key`
-- (engram_subject_id): a document is `document:<document_id>`, a memory is `memory:<document_id>:<content_hash hex>`
-- (twins are ONE subject), a namespace `namespace:<id>`, a tenant `tenant:<id>`; a client-chosen document_id can never
-- alias a fact's subject. The handler takes pg_advisory_lock(engram_subject_lock_key(ns, subject)) (session level,
-- direct connection, lock_timeout 3 s, one attempt) before the marker transaction and releases it after the intent put
-- and the marker re-read; under it the handler first puts the missing intent of the subject's latest deletion_log entry
-- (help-previous). A crash drops the connection and so the lock.
-- +goose StatementBegin
CREATE FUNCTION engram_subject_lock_key(ns uuid, subject text) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtextextended(ns::text || ':' || subject, 2) $$;
-- +goose StatementEnd

-- N162: the encoded subject id, `class:key` (class in document | memory | namespace | tenant).
-- +goose StatementBegin
CREATE FUNCTION engram_subject_id(p_class text, p_key text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT p_class || ':' || p_key $$;
-- +goose StatementEnd

-- N166: the per-partition purge-pause key. The index runner takes pg_advisory_lock(key) (session level, direct
-- connection) from the selection of a partition's rebuild set until VACUUM has started; every purge batch takes the
-- shared TRY-lock below and SKIPS that partition (it is simply not purged this round) when the runner holds it.
-- +goose StatementBegin
CREATE FUNCTION engram_partition_purge_key(p_partition text) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtextextended('partition:' || p_partition, 3) $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_try_partition_purge_shared(p_partition text) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$ SELECT pg_try_advisory_xact_lock_shared(engram_partition_purge_key(p_partition)) $$;
-- +goose StatementEnd

-- N159: does THIS backend hold the EXCLUSIVE derivation lock of ns
-- (pg_advisory_xact_lock(engram_ns_derivation_key(ns)))? Used by the Materialize stamp guard below.
-- +goose StatementBegin
CREATE FUNCTION engram_holds_derivation_exclusive(ns uuid) RETURNS boolean
LANGUAGE sql STABLE STRICT AS $$
  SELECT EXISTS (SELECT 1 FROM pg_locks l
                  WHERE l.locktype = 'advisory' AND l.granted AND l.pid = pg_backend_pid() AND l.mode = 'ExclusiveLock'
                    AND l.objsubid = 1
                    AND ((l.classid::bigint << 32) | l.objid::bigint) = engram_ns_derivation_key(ns))
$$;
-- +goose StatementEnd

-- Writers' fence acquisition (N82): never waits. false -> NamespaceFrozen{retry_after = 200 ms}.
-- +goose StatementBegin
CREATE FUNCTION engram_try_ns_fence(ns uuid) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$ SELECT pg_try_advisory_xact_lock_shared(engram_ns_fence_key(ns)) $$;
-- +goose StatementEnd

-- Exclusive takers (freeze, delete freeze, restore): ONE attempt, 35 s (N82). Raises 55P03 on timeout. The role default
-- statement_timeout of engram_app, engram_move and engram_admin is 30 s, shorter than the lock timeout, and a function
-- cannot re-arm the timer of the statement that calls it: THE CALLER runs `SET LOCAL statement_timeout = '36s'` before
-- the statement that calls this function (PLAN.md section 5, N82), so the lock timeout and not the statement timeout
-- ends the wait. TestFence_ExclusiveHelperTimeouts pins 55P03 after 35 s with it and 57014 after 30 s without it.
-- +goose StatementBegin
CREATE FUNCTION engram_ns_fence_exclusive(ns uuid) RETURNS void
LANGUAGE plpgsql VOLATILE STRICT AS $$
BEGIN
  PERFORM set_config('lock_timeout', '35s', true);
  PERFORM pg_advisory_xact_lock(engram_ns_fence_key(ns));
END $$;
-- +goose StatementEnd

-- Derivation lock (N120): writers of derived versions (ApplyBatch stage 2, PageRefresh commit) try-lock it shared and
-- retry on refusal; Expunge.Materialize takes it exclusive, one 35 s attempt (the caller sets `SET LOCAL
-- statement_timeout = '36s'`, as for engram_ns_fence_exclusive).
-- +goose StatementBegin
CREATE FUNCTION engram_try_derivation_lock(ns uuid) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$ SELECT pg_try_advisory_xact_lock_shared(engram_ns_derivation_key(ns)) $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION engram_derivation_lock_exclusive(ns uuid) RETURNS void
LANGUAGE plpgsql VOLATILE STRICT AS $$
BEGIN
  PERFORM set_config('lock_timeout', '35s', true);
  PERFORM pg_advisory_xact_lock(engram_ns_derivation_key(ns));
END $$;
-- +goose StatementEnd

-- CommitChunk's per-document lock (N83): shared try-lock; false -> retryable DocumentBusy (100 ms).
-- +goose StatementBegin
CREATE FUNCTION engram_try_doc_lock_shared(ns uuid, doc text) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$
  SELECT pg_try_advisory_xact_lock_shared(k.k1, k.k2) FROM engram_doc_lock_keys(ns, doc) AS k;
$$;
-- +goose StatementEnd

-- Tag grammar: <= 32 tags, each ^[a-z0-9][a-z0-9._:/-]{0,63}$, strictly ascending in "C" collation (= sorted and
-- deduplicated bytewise, exactly what the Go normaliser emits). Tags live on documents (and observations' consolidation
-- scope) only: the recall layer resolves the tag mode ONCE against documents.tags into an allowed-document set; no tag
-- predicate runs under RLS (N116, P-5).
-- +goose StatementBegin
CREATE FUNCTION engram_tags_valid(t text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT cardinality(t) <= 32
     AND NOT EXISTS (SELECT 1 FROM unnest(t) AS x WHERE x !~ '^[a-z0-9][a-z0-9._:/-]{0,63}$')
     AND coalesce((SELECT bool_and(t[i] COLLATE "C" < t[i + 1] COLLATE "C")
                     FROM generate_series(1, cardinality(t) - 1) AS i), true);
$$;
-- +goose StatementEnd

-- SQL twin of the Lean-verified tag decision procedure (section 7). Q = query tags, I = item tags.
--   ANY: I = {} or I ∩ Q ≠ {}   ANY_STRICT: I ∩ Q ≠ {}   ALL: I = {} or Q ⊆ I
--   ALL_STRICT: Q ⊆ I and I ≠ {}   EXACT: I = Q      unset mode (NULL/'') matches everything.
-- +goose StatementBegin
CREATE FUNCTION engram_tag_match(mode text, q text[], i text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE coalesce(mode, '')
           WHEN ''           THEN true
           WHEN 'ANY'        THEN cardinality(i) = 0 OR i && q
           WHEN 'ANY_STRICT' THEN i && q
           WHEN 'ALL'        THEN cardinality(i) = 0 OR i @> q
           WHEN 'ALL_STRICT' THEN i @> q AND cardinality(i) > 0
           WHEN 'EXACT'      THEN i @> q AND i <@ q
         END;
$$;
-- +goose StatementEnd

-- jsonb form used by pages.tag_filter: {"mode": "ALL", "tags": ["a","b"]}
-- +goose StatementBegin
CREATE FUNCTION engram_tag_match(filter jsonb, i text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT engram_tag_match(filter ->> 'mode',
                          coalesce((SELECT array_agg(x ORDER BY x COLLATE "C")
                                      FROM jsonb_array_elements_text(filter -> 'tags') AS x), '{}'::text[]),
                          i);
$$;
-- +goose StatementEnd

-- Mutable tables only (N113): stamps updated_at on every UPDATE. The move loader runs in session_replication_role =
-- replica, where this trigger does not fire, so copied rows keep the source's updated_at.
-- +goose StatementBegin
CREATE FUNCTION engram_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;
-- +goose StatementEnd

-- Insert-only content tables (N113): UPDATE is refused for every role. The grants already give engram_app and
-- engram_move no UPDATE privilege on them; this trigger is the second wall (it also stops the owner and engram_admin).
-- DELETE stays possible for the purge only (grants).
-- +goose StatementBegin
CREATE FUNCTION engram_forbid_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is insert-only (UPDATE by % refused)', TG_TABLE_NAME, current_user
    USING ERRCODE = '42501';
END $$;
-- +goose StatementEnd

-- ingest_ledger is append-only. UPDATE is never allowed. DELETE is allowed only to engram_admin (explicit document,
-- namespace or tenant delete expunge; never the retire of superseded versions, N104) and to the table owner, which is
-- the SECURITY DEFINER cleanup function engram_cleanup_namespace after a move. Every other role gets 42501.
-- +goose StatementBegin
CREATE FUNCTION engram_forbid_ledger_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' AND (current_user = 'engram_admin'
       OR current_user = (SELECT pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.oid = TG_RELID)) THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'ingest_ledger is append-only (% by % refused)', TG_OP, current_user
    USING ERRCODE = '42501';
END $$;
-- +goose StatementEnd

-- The tables engram_cleanup_namespace deletes, in a deletion order that is a topological order of the foreign keys
-- (`engramlint sql` checks it against pg_constraint; chunk_tombstones references chunks, so it goes immediately before
-- chunks): content before the markers that mark it (N170(3)), the *_version_meta rows before their versions (their FK
-- does not cascade, N136). It is a function so that the cleanup and the return_move guard of the ownership trigger
-- share one list.
-- +goose StatementBegin
CREATE FUNCTION engram_cleanup_order() RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ARRAY[
    'observation_version_vectors', 'fact_vectors', 'chunk_vectors', 'page_version_vectors',
    'expunge_progress',
    'page_version_inputs', 'page_version_meta', 'page_sources', 'page_versions', 'pages',
    'observation_version_sources', 'observation_inputs', 'observation_version_meta', 'observation_sources',
    'fact_consolidation', 'consolidation_state', 'consolidation_applied', 'consolidation_proposals',
    'consolidation_batches', 'observation_versions', 'observations',
    'entity_mentions', 'fact_links', 'facts', 'entity_aliases', 'entities',
    'document_version_chunks', 'chunk_tombstones', 'chunks', 'document_versions', 'ingest_ledger', 'documents',
    -- markers after the content they mark (N170(3))
    'derived_hidden', 'curation_log', 'fact_hidden', 'document_tombstones',
    'export_snapshots', 'token_usage_events', 'token_usage', 'quota_counters', 'batch_jobs', 'tag_counts',
    'idempotency_keys', 'operations', 'vector_indexes', 'namespace_models', 'namespace_stats'];
$$;
-- +goose StatementEnd

-- namespace_ownership rows must carry this database's shard id, epochs never decrease, and only the edges of
-- ownership_transitions are accepted (N101, N125):
--   * freeze_reason, epoch, move and target columns change only in an explicit edge, each with its role;
--   * moved_out and active rows are never deleted by an ordinary role (the fence value a late writer
--     or stale reader must still hit, WrongShardOrEpoch{MOVED_OUT}); cleanup never removes them;
--   * moved_out -> incoming additionally requires that no data rows remain (the cleanup ran).
-- Everything else is 23514 / 42501. The trigger is ENABLE ALWAYS (N91): session_replication_role = replica, which the
-- move loader sets, must not switch it off.
-- +goose StatementBegin
CREATE FUNCTION engram_check_ownership() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  v_shard integer;
  t       record;
  v_ok    boolean := false;
  v_tbl   text;
  v_rows  boolean;
BEGIN
  SELECT shard_id INTO v_shard FROM shard_meta;
  IF v_shard IS NULL THEN
    RAISE EXCEPTION 'shard_meta is empty; provision the shard first' USING ERRCODE = '55000';
  END IF;

  IF TG_OP = 'DELETE' THEN
    PERFORM 1 FROM ownership_transitions x
      WHERE x.from_state = OLD.state AND x.from_reason IS NOT DISTINCT FROM OLD.freeze_reason
        AND x.to_state IS NULL AND x.role_name = current_user;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'ownership row % in state %(%) may not be deleted by %',
        OLD.namespace_id, OLD.state, OLD.freeze_reason, current_user USING ERRCODE = '42501';
    END IF;
    RETURN OLD;
  END IF;

  IF NEW.shard_id <> v_shard THEN
    RAISE EXCEPTION 'ownership row for % names shard % but this database is shard %',
      NEW.namespace_id, NEW.shard_id, v_shard USING ERRCODE = '23514';
  END IF;

  IF TG_OP = 'INSERT' THEN
    SELECT * INTO t FROM ownership_transitions x
     WHERE x.from_state IS NULL AND x.to_state = NEW.state AND x.role_name = current_user;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'role % may not insert an ownership row in state % (%)',
        current_user, NEW.state, NEW.namespace_id USING ERRCODE = '42501';
    END IF;
    IF t.epoch_rule = 'one' AND NEW.epoch <> 1 THEN
      RAISE EXCEPTION 'a new ownership row must start at epoch 1 (got %)', NEW.epoch USING ERRCODE = '23514';
    END IF;
    IF NEW.floor_lsn IS NOT NULL THEN
      RAISE EXCEPTION 'floor_lsn is stamped by ready_target only (N179)' USING ERRCODE = '23514';
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
  END IF;

  -- UPDATE
  IF NEW.namespace_id <> OLD.namespace_id OR NEW.tenant_id <> OLD.tenant_id OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION 'namespace_id, tenant_id and created_at of an ownership row are immutable' USING ERRCODE = '23514';
  END IF;
  IF NEW.epoch < OLD.epoch THEN
    RAISE EXCEPTION 'epoch must not decrease for namespace %', NEW.namespace_id USING ERRCODE = '23514';
  END IF;

  FOR t IN
    SELECT * FROM ownership_transitions x
     WHERE x.from_state = OLD.state AND x.from_reason IS NOT DISTINCT FROM OLD.freeze_reason
       AND x.to_state = NEW.state   AND x.to_reason   IS NOT DISTINCT FROM NEW.freeze_reason
       AND x.role_name = current_user
  LOOP
    IF engram_edge_accepts(t.epoch_rule, t.move_effect, t.target_effect, OLD, NEW) THEN
      v_ok := true;
      EXIT;
    END IF;
  END LOOP;
  IF NOT v_ok THEN
    IF EXISTS (SELECT 1 FROM ownership_transitions x
                WHERE x.from_state = OLD.state AND x.from_reason IS NOT DISTINCT FROM OLD.freeze_reason
                  AND x.to_state = NEW.state   AND x.to_reason   IS NOT DISTINCT FROM NEW.freeze_reason
                  AND x.role_name = current_user) THEN
      -- the edge exists for this role but the epoch rule or the move/target column effects are wrong
      RAISE EXCEPTION 'ownership transition %(%)/% -> %(%)/% by % violates the epoch or move/target rules of its edge '
                      '(namespace %)',
        OLD.state, OLD.freeze_reason, OLD.epoch, NEW.state, NEW.freeze_reason, NEW.epoch, current_user, NEW.namespace_id
        USING ERRCODE = '23514';
    ELSIF EXISTS (SELECT 1 FROM ownership_transitions x
                   WHERE x.from_state = OLD.state AND x.from_reason IS NOT DISTINCT FROM OLD.freeze_reason
                     AND x.to_state = NEW.state   AND x.to_reason   IS NOT DISTINCT FROM NEW.freeze_reason) THEN
      -- the state pair exists, but only for other roles
      RAISE EXCEPTION 'ownership transition %(%)/% -> %(%)/% refused for role % (namespace %)',
        OLD.state, OLD.freeze_reason, OLD.epoch, NEW.state, NEW.freeze_reason, NEW.epoch, current_user, NEW.namespace_id
        USING ERRCODE = '42501';
    END IF;
    RAISE EXCEPTION 'illegal ownership transition %(%)/% -> %(%)/% for namespace %',
      OLD.state, OLD.freeze_reason, OLD.epoch, NEW.state, NEW.freeze_reason, NEW.epoch, NEW.namespace_id
      USING ERRCODE = '23514';
  END IF;

  -- N179(1): ready_target stamps floor_lsn, return_move clears it, every other edge keeps it.
  IF (t.edge = 'ready_target' AND NEW.floor_lsn IS NULL)
     OR (t.edge = 'return_move' AND NEW.floor_lsn IS NOT NULL)
     OR (t.edge NOT IN ('ready_target', 'return_move') AND NEW.floor_lsn IS DISTINCT FROM OLD.floor_lsn) THEN
    RAISE EXCEPTION 'edge % of namespace % violates the floor_lsn rule (N179)', t.edge, NEW.namespace_id
        USING ERRCODE = '23514';
  END IF;
  -- N177: a namespace with an open move answers NAMESPACE_BUSY
  IF t.edge = 'freeze_delete' AND OLD.move_id IS NOT NULL THEN
    RAISE EXCEPTION 'namespace % has an open move', NEW.namespace_id
      USING ERRCODE = '55006';
  END IF;
  -- N125 "refused 55006 while data rows remain", N170(3): the cleanup deletes content before markers, so an interrupted
  -- cleanup leaves stale markers behind; the guard therefore checks EVERY table the cleanup deletes
  -- (engram_cleanup_order(), the one list), not only the first three it empties. The plan does not say which tables;
  -- the all-tables reading is the one that cannot pass a half-cleaned shard (M0.2 review F5, CONFLICTS #18).
  IF t.edge = 'return_move' THEN
    FOREACH v_tbl IN ARRAY engram_cleanup_order() LOOP
      EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE namespace_id = $1)', v_tbl)
        INTO v_rows USING NEW.namespace_id;
      IF v_rows THEN
        RAISE EXCEPTION 'namespace % still has rows in % on this shard; run engram_cleanup_namespace first',
            NEW.namespace_id, v_tbl
          USING ERRCODE = '55006';
      END IF;
    END LOOP;
  END IF;
  -- N160(5), N125 (b'): nothing becomes 'ready' before (1) every requested partial HNSW index of the namespace is
  -- indisvalid AND indisready (the first recall after activation must not be a whole-namespace exact scan) and (2) this
  -- shard's engram_ins_seq has passed the source's final value w_final (N147).
  IF NEW.state = 'ready' AND t.edge = 'ready_target' THEN
    IF NOT engram_move_indexes_valid(NEW.namespace_id) THEN
      RAISE EXCEPTION 'namespace % has vector indexes that are not valid and ready (N160)', NEW.namespace_id
          USING ERRCODE = '55006';
    END IF;
    IF NEW.w_final IS NULL OR (SELECT last_value FROM engram_ins_seq) <= NEW.w_final THEN
      RAISE EXCEPTION 'namespace %: the target sequence has not passed w_final (N147)', NEW.namespace_id
          USING ERRCODE = '55006';
    END IF;
  END IF;
  IF (OLD.state = 'ready' AND NEW.state = 'active') OR (t.edge = 'restore_done' AND OLD.move_id IS NOT NULL) THEN
    NEW.moved_in_at := now();
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;
-- +goose StatementEnd

-- shard_meta holds exactly one row.
-- +goose StatementBegin
CREATE FUNCTION engram_shard_meta_singleton() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT count(*) FROM shard_meta) > 0 THEN
    RAISE EXCEPTION 'shard_meta already has a row; UPDATE it' USING ERRCODE = '23505';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

-- =============================================================================
-- Shard-level tables (no namespace_id; allowlisted by the section 8 RLS check)
-- =============================================================================

-- shard_meta: the database's identity and schema version (one row; section 9 ND-10).
CREATE TABLE shard_meta (
  shard_id            integer NOT NULL CHECK (shard_id >= 0),
  schema_version      integer NOT NULL CHECK (schema_version >= 1),
  applied_at          timestamptz NOT NULL DEFAULT now(),
  engram_min_version  text NOT NULL DEFAULT '',
  engram_max_version  text NOT NULL DEFAULT ''
  -- no replay floor here (N134): it lives in catalog.shards.replay_floor, outside the restorable shard state, so
  -- neither a PITR nor a stale promotion can lose or raise it
);

CREATE TRIGGER shard_meta_singleton BEFORE INSERT ON shard_meta
  FOR EACH ROW EXECUTE FUNCTION engram_shard_meta_singleton();

-- outbox_cursors: one row per consumer plus the 'relay' row, whose gaps column is the persisted gap watchlist (D6). A
-- seq missing for more than 1 s at the relay's read point is recorded as {"seq": n, "deadline": ts}; it is removed when
-- the row appears or when the deadline (first_seen + 2 x statement_timeout) passes. The relay stops advancing when the
-- list holds 1000 entries. Cursor advances are batched to 1/s per consumer (XID budget, N114). Moves never read or
-- write this table (N124).
CREATE TABLE outbox_cursors (
  consumer    text PRIMARY KEY CHECK (consumer ~ '^(relay|index|kafka)$'),
  last_seq    bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  gaps        jsonb NOT NULL DEFAULT '[]'::jsonb
              CHECK (jsonb_typeof(gaps) = 'array' AND jsonb_array_length(gaps) <= 1000),
  updated_at  timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);

-- engram_seq_log (N137): the ring behind engram_seq_floor. engram_seq_sample() inserts (now(),
-- nextval('engram_ins_seq')) once a minute (admin scheduler) and trims samples older than 7 days. Shard-local: a move
-- never copies it and takes no floor from it (N160).
CREATE TABLE engram_seq_log (
  sampled_at  timestamptz PRIMARY KEY,
  seq         bigint NOT NULL
);

-- +goose StatementBegin
CREATE FUNCTION engram_seq_sample() RETURNS bigint
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  v bigint := nextval('engram_ins_seq');
BEGIN
  INSERT INTO engram_seq_log (sampled_at, seq) VALUES (clock_timestamp(), v);
  DELETE FROM engram_seq_log WHERE sampled_at < now() - interval '7 days';
  RETURN v;
END $$;
-- +goose StatementEnd

-- The lower bound on ins_seq for "rows inserted since ts": the sample at or before ts - 10 min, which exceeds the
-- longest time between drawing an ins_seq and committing the row. 0 on a new shard (everything is "recent"). Evaluated
-- only by the stats sweeper, BeginSnapshot and the consolidation watermark, each on its own shard; a move takes no
-- floor (N160, N147).
-- +goose StatementBegin
CREATE FUNCTION engram_seq_floor(ts timestamptz) RETURNS bigint
LANGUAGE sql STABLE AS $$
  SELECT coalesce((SELECT l.seq FROM engram_seq_log l
                    WHERE l.sampled_at <= ts - interval '10 minutes'
                    ORDER BY l.sampled_at DESC LIMIT 1), 0);
$$;
-- +goose StatementEnd

-- ins_seq is PER SHARD and copied verbatim by a move, so the target's sequence must be advanced past the source's or
-- every row the target inserts after cutover sorts BELOW the copied ones and the export and a second move miss them
-- (N147). engram_seq_advance(p_to) moves forward only, and by one call: the mover calls it on the target ONCE, at the
-- start of FrozenCopy, with the source's nextval under the freeze (W_final); setval runs only when p_to + 10 000
-- exceeds last_value, to p_to + 10 000 (the slack absorbs concurrent nextvals, N167: the stated property is "monotone
-- within the 10 min floor margin"). SECURITY DEFINER (setval needs UPDATE on the sequence); engram_move and
-- engram_admin only.
-- +goose StatementBegin
CREATE FUNCTION engram_seq_advance(p_to bigint) RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  v bigint;
BEGIN
  SELECT last_value INTO v FROM engram_ins_seq;
  IF p_to + 10000 > v THEN
    PERFORM setval('engram_ins_seq', p_to + 10000);
    PERFORM engram_seq_sample();
  END IF;
  SELECT last_value INTO v FROM engram_ins_seq;   -- N177: after the sample, in both branches
  RETURN v;
END $$;
-- +goose StatementEnd

-- =============================================================================
-- Namespace-keyed tables (the big ones are partitioned further down)
-- =============================================================================

-- namespace_ownership: the fence VALUE (D2 row 4, D5; the fence LOCK is the advisory lock engram_ns_fence_key, see the
-- header). Mirrors the catalog for the namespaces this shard hosts. The ONLY table with a shard_id column. The state
-- machine (states x roles x statements) is the data table ownership_transitions, enforced by engram_check_ownership
-- (N101, N125).
-- State 'ready' (N125): the target after the verified copy and the index builds, before the point of no return; nothing
-- routes to it and callers get the retryable NamespaceNotReady. w_final (N147) is the source's final sequence value
-- under the freeze, set by the mover on the update that makes the row 'ready'; the trigger refuses 'ready' unless this
-- shard's engram_ins_seq has passed it.
-- move_id / move_epoch: set on the SOURCE by start_move (move_epoch = the target epoch e + 1) and cleared on
-- abort/thaw; Expunge and the schedulers skip a namespace while move_epoch IS NOT NULL (views schedulable_namespaces /
-- purgeable_namespaces). On the target move_id is set by the incoming row. target_shard_id / target_epoch: carried by a
-- moved_out row so the API can route without the catalog; a moved_out row is a PERMANENT fence value and is never
-- deleted. After a return_abort the row's epoch may exceed target_epoch, so no CHECK relates the two.
CREATE TABLE namespace_ownership (
  namespace_id      uuid PRIMARY KEY,
  tenant_id         text NOT NULL CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  shard_id          integer NOT NULL,
  epoch             bigint NOT NULL CHECK (epoch >= 1),
  state             ownership_state NOT NULL,
  freeze_reason     text CHECK (freeze_reason IN ('move', 'delete', 'restore')),   -- why frozen (D2)
  move_id           uuid,
  move_epoch        bigint,
  target_shard_id   integer,
  target_epoch      bigint,
  -- N147, N125 (b'): the source's final engram_ins_seq; required on 'ready'
  w_final           bigint,
  -- N179(1): the target's copy_end_lsn, stamped by ready_target, kept by every later edge of the row, cleared by
  -- return_move
  floor_lsn         pg_lsn,
  -- N147, N180(1): set by the trigger on ready -> active and on restore_done of a row with move_id; BeginSnapshot and
  -- the consolidation watermark wait 10 min after it (the target's seq ring has no sample below W_final until then)
  moved_in_at       timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),                -- FK target for the tenant_id denormalisation
  CHECK (state NOT IN ('incoming', 'ready') OR move_id IS NOT NULL),
  CHECK (state <> 'ready' OR w_final IS NOT NULL),
  CHECK (state <> 'ready' OR floor_lsn IS NOT NULL),
  CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL)),   -- a thaw and cutover (c) clear the reason
  CHECK ((state = 'moved_out') = (target_shard_id IS NOT NULL)),
  CHECK ((target_shard_id IS NULL) = (target_epoch IS NULL)),
  CHECK (target_shard_id IS NULL OR target_shard_id <> shard_id),
  CHECK (move_epoch IS NULL OR move_id IS NOT NULL)
);

-- The ownership state machine as DATA (N101, N125, section 3.3.1): one row per (edge, role, from state). The trigger
-- engram_check_ownership accepts an INSERT, UPDATE or DELETE of a namespace_ownership row only if it matches a row here
-- for current_user, and then checks the epoch rule and the move/target column effects of that edge. Any (state pair,
-- role) not listed is refused: that is the whole negative test surface of TestIso_Ownership_Transitions. NULL
-- from_state = INSERT, NULL to_state = DELETE.
--   epoch_rule: one = epoch 1, any, same, plus1 = exactly +1, greater = strictly greater (restore
--     and failover use the catalog epoch + 1, written to the catalog first).
--   move_effect on (move_id, move_epoch): none = unchanged, open = start_move (move_id set from
--     NULL, move_epoch = epoch + 1), close = both NULL (whether or not they were set: restore_done relies on it),
--     retarget = a NEW move_id and move_epoch NULL (moved_out -> incoming).
--   target_effect on (target_shard_id, target_epoch): none, set = cutover (c) / reconcile_out
--     (target shard differs, target_epoch = epoch + 1), clear, restore = return_abort (the hint of the permanent fence
--     value comes back from namespace_moves).
-- Rollback edges before (a''): abort_move (before freeze), thaw_move (after freeze), unready_target (after (b')),
-- return_abort (onto a shard that had a moved_out row). After (a'') a restored target is an ordinary restore (N169(2)).
CREATE TABLE ownership_transitions (
  edge           text NOT NULL,
  role_name      text NOT NULL,
  from_state     ownership_state,
  from_reason    text,
  to_state       ownership_state,
  to_reason      text,
  epoch_rule     text NOT NULL CHECK (epoch_rule IN ('one', 'any', 'same', 'plus1', 'greater')),
  move_effect    text NOT NULL DEFAULT 'none' CHECK (move_effect IN ('none', 'open', 'close', 'retarget')),
  target_effect  text NOT NULL DEFAULT 'none' CHECK (target_effect IN ('none', 'set', 'clear', 'restore')),
  note           text NOT NULL DEFAULT '',
  CHECK (from_state IS NOT NULL OR to_state IS NOT NULL),
  CHECK ((from_state = 'frozen') = (from_reason IS NOT NULL)),
  CHECK ((to_state = 'frozen') = (to_reason IS NOT NULL)),
  UNIQUE NULLS NOT DISTINCT (edge, role_name, from_state, from_reason)
);

INSERT INTO ownership_transitions (edge, role_name, from_state, from_reason, to_state, to_reason, epoch_rule,
                                   move_effect, target_effect, note) VALUES
  ('create',          'engram_app',   NULL,       NULL,      'active',    NULL,      'one',     'none',     'none',
   'CreateNamespace: first row, epoch 1'),
  ('create',          'engram_admin', NULL,       NULL,      'active',    NULL,      'one',     'none',     'none',
   'provisioning and tests'),
  ('plan_target',     'engram_move',  NULL,       NULL,      'incoming',  NULL,      'any',     'none',     'none',
   'move plan on the target; move_id mandatory (CHECK)'),
  ('start_move',      'engram_move',  'active',   NULL,      'active',    NULL,      'same',    'open',     'none',
   'Plan on the source: sets move_id and move_epoch, pauses Expunge and schedulers'),
  ('abort_move',      'engram_move',  'active',   NULL,      'active',    NULL,      'same',    'close',    'none',
   'rollback before the freeze'),
  ('abort_move',      'engram_admin', 'active',   NULL,      'active',    NULL,      'same',    'close',    'none',
   'restore/failover reconcile (N123)'),
  ('freeze_move',     'engram_move',  'active',   NULL,      'frozen',    'move',    'same',    'none',     'none',
   'D5 freeze: reads continue'),
  ('freeze_delete',   'engram_app',   'active',   NULL,      'frozen',    'delete',  'same',    'none',     'none',
   'namespace delete, before the ack (N122); needs move_id IS NULL (activate_target clears it, N177); no outgoing edge '
   'except deletion'),
  ('freeze_restore',  'engram_admin', 'active',   NULL,      'frozen',    'restore', 'same',    'none',     'none',
   'restore or failover (N123)'),
  ('freeze_restore',  'engram_admin', 'incoming', NULL,      'frozen',    'restore', 'same',    'none',     'none',
   'N169(2): restored move target'),
  ('freeze_restore',  'engram_admin', 'ready',    NULL,      'frozen',    'restore', 'same',    'none',     'none',
   'N169(2): as above'),
  ('restore_delete',  'engram_admin', 'frozen',   'restore', 'frozen',    'delete',  'same',    'none',     'none',
   'replay of a namespace/tenant delete intent onto a restored shard (N122, C-13); no pass through active'),
  ('thaw_move',       'engram_move',  'frozen',   'move',    'active',    NULL,      'same',    'close',    'none',
   'rollback after the freeze'),
  ('thaw_move',       'engram_admin', 'frozen',   'move',    'active',    NULL,      'same',    'close',    'none',
   'restore/failover reconcile: the move is rolled back (N123)'),
  ('ready_target',    'engram_move',  'incoming', NULL,      'ready',     NULL,      'same',    'none',     'none',
   'cutover (b''): nothing routes to ready; stamps floor_lsn'),
  ('unready_target',  'engram_move',  'ready',    NULL,      'incoming',  NULL,      'same',    'none',     'none',
   'rollback after (b'')'),
  ('cutover_c',       'engram_move',  'frozen',   'move',    'moved_out', NULL,      'same',    'none',     'set',
   'cutover (c), after (a'''') was read and replicated (N171); clears freeze_reason'),
  ('activate_target', 'engram_move',  'ready',    NULL,      'active',    NULL,      'same',    'close',    'none',
   'cutover (b''''): the row already carries e + 1'),
  ('return_move',     'engram_move',  'moved_out', NULL,     'incoming',  NULL,      'greater', 'retarget', 'clear',
   'a later move back to this shard; data rows must be gone'),
  ('return_abort',    'engram_move',  'incoming', NULL,      'moved_out', NULL,      'any',     'close',    'restore',
   'rollback of a move back: the permanent fence value returns (H-22)'),
  ('reconcile_out',   'engram_admin', 'active',   NULL,      'moved_out', NULL,      'same',    'close',    'set',
   'restore/failover: (c) was done, the restored source row becomes moved_out(target, e+1)'),
  ('reconcile_out',   'engram_admin', 'frozen',   'move',    'moved_out', NULL,      'same',    'close',    'set',
   'same, from a restored frozen/move row'),
  ('reconcile_out',   'engram_admin', 'frozen',   'restore', 'moved_out', NULL,      'same',    'close',    'set',
   'same, from a restored frozen/restore row'),
  ('epoch_bump',      'engram_admin', 'active',   NULL,      'active',    NULL,      'greater', 'none',     'none',
   'failover bump: new epoch = catalog epoch + 1'),
  ('restore_done',    'engram_admin', 'frozen',   'restore', 'active',    NULL,      'greater', 'close',    'none',
   'restore completion after intent replay: catalog epoch + 1; closes a set move_id (N180(1))'),
  ('rollback_target', 'engram_move',  'incoming', NULL,      NULL,        NULL,      'any',     'none',     'none',
   'DELETE of the target row on rollback'),
  ('rollback_target', 'engram_admin', 'incoming', NULL,      NULL,        NULL,      'any',     'none',     'none',
   'operator cleanup'),
  ('rollback_target', 'engram_admin', 'ready',    NULL,      NULL,        NULL,      'any',     'none',     'none',
   'operator cleanup after a restore of the target'),
  ('rollback_target', 'engram_admin', 'frozen',   'restore', NULL,        NULL,      'any',     'none',     'none',
   'N183: a restored active row below its floor, after freeze_restore'),
  ('purge_deleted',   'engram_admin', 'frozen',   'delete',  NULL,        NULL,      'any',     'none',     'none',
   'DELETE after NamespacePurged');

-- The machine is immutable once loaded (a change is a migration that drops this trigger first).
-- +goose StatementBegin
CREATE FUNCTION engram_forbid_transition_edit() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ownership_transitions is immutable (% refused)', TG_OP USING ERRCODE = '42501';
END $$;
-- +goose StatementEnd

CREATE TRIGGER ownership_transitions_immutable BEFORE INSERT OR UPDATE OR DELETE ON ownership_transitions
  FOR EACH STATEMENT EXECUTE FUNCTION engram_forbid_transition_edit();

-- Epoch rule and column effects of one candidate edge for an UPDATE (o = OLD, n = NEW); the three rule names are the
-- columns of the matched ownership_transitions row.
-- +goose StatementBegin
CREATE FUNCTION engram_edge_accepts(p_epoch_rule text, p_move_effect text, p_target_effect text,
                                    o namespace_ownership, n namespace_ownership)
RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_epoch_rule
           WHEN 'same'    THEN n.epoch = o.epoch
           WHEN 'plus1'   THEN n.epoch = o.epoch + 1
           WHEN 'greater' THEN n.epoch > o.epoch
           ELSE true
         END
     AND CASE p_move_effect
           WHEN 'none'     THEN (n.move_id, n.move_epoch) IS NOT DISTINCT FROM (o.move_id, o.move_epoch)
           WHEN 'open'     THEN o.move_id IS NULL AND n.move_id IS NOT NULL AND n.move_epoch = o.epoch + 1
           WHEN 'close'    THEN n.move_id IS NULL AND n.move_epoch IS NULL
           WHEN 'retarget' THEN n.move_id IS NOT NULL AND n.move_id IS DISTINCT FROM o.move_id
                                AND n.move_epoch IS NULL AND n.epoch > coalesce(o.target_epoch, o.epoch)
         END
     AND CASE p_target_effect
           WHEN 'none'    THEN (n.target_shard_id, n.target_epoch) IS NOT DISTINCT
               FROM (o.target_shard_id, o.target_epoch)
           WHEN 'set'     THEN n.target_shard_id IS NOT NULL AND n.target_shard_id <> n.shard_id
               AND n.target_epoch = o.epoch + 1
           WHEN 'clear'   THEN n.target_shard_id IS NULL AND n.target_epoch IS NULL
           WHEN 'restore' THEN o.target_shard_id IS NULL AND n.target_shard_id IS NOT NULL
                               AND n.target_shard_id <> n.shard_id AND n.target_epoch IS NOT NULL
         END;
$$;
-- +goose StatementEnd

CREATE TRIGGER namespace_ownership_check BEFORE INSERT OR UPDATE OR DELETE ON namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram_check_ownership();
-- N91: the move loader runs with session_replication_role = replica; this trigger must still fire.
ALTER TABLE namespace_ownership ENABLE ALWAYS TRIGGER namespace_ownership_check;

-- Shard-wide schedulers read these views (admin role only), never the table (N97): the op-sweeper, DEFERRED resumer,
-- consolidate-sweep, page-cron, outbox-trim and stats sweeper join schedulable_namespaces (state = 'active': the target
-- of a move is 'active' only after (c)); Expunge and the index sweeper join purgeable_namespaces, which excludes a
-- namespace with an open move (Plan to done, N124).
CREATE VIEW schedulable_namespaces AS
  SELECT namespace_id, tenant_id, epoch, move_epoch
    FROM namespace_ownership
   WHERE state = 'active';

CREATE VIEW purgeable_namespaces AS
  SELECT namespace_id, tenant_id, epoch
    FROM namespace_ownership
   WHERE state = 'active' AND move_epoch IS NULL;

-- namespace_stats: DERIVED counters (N69): refreshed per namespace by the stats sweeper (admin role), never updated by
-- CommitChunk or FinalizeVersion. live_facts counts VISIBLE facts (markers applied). `large` (N112) flips when the
-- namespace crosses 2,000 vectors of its current model (cleared below 1,000) and drives the per-namespace partial HNSW
-- indexes (vector_indexes).
CREATE TABLE namespace_stats (
  namespace_id        uuid PRIMARY KEY,
  tenant_id           text NOT NULL,
  large               boolean NOT NULL DEFAULT false,   -- >= 2,000 vectors: per-namespace partial HNSW (N112)
  large_since         timestamptz,
  live_facts          bigint NOT NULL DEFAULT 0 CHECK (live_facts >= 0),
  total_facts         bigint NOT NULL DEFAULT 0 CHECK (total_facts >= 0),
  live_chunks         bigint NOT NULL DEFAULT 0 CHECK (live_chunks >= 0),
  live_documents      bigint NOT NULL DEFAULT 0 CHECK (live_documents >= 0),
  live_observations   bigint NOT NULL DEFAULT 0 CHECK (live_observations >= 0),
  pending_markers     bigint NOT NULL DEFAULT 0 CHECK (pending_markers >= 0),   -- alert above 16 k (N116)
  -- relation bytes, hidden and unpurged rows included: the capacity signal (N114), not live_facts
  bytes_estimate      bigint NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  -- {"2026-01": facts, ...}: the as_of selectivity estimate (N138)
  mentioned_histogram jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(mentioned_histogram) = 'object'),
  -- {"tag": current observation versions}: the observation arm's own estimate (N151)
  observations_by_tag jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(observations_by_tag) = 'object'),
  -- hidden / total facts: subtracted from the eligible estimate (N151)
  hidden_fraction     real NOT NULL DEFAULT 0 CHECK (hidden_fraction >= 0 AND hidden_fraction <= 1),
  recalls_1h          bigint NOT NULL DEFAULT 0,
  retains_1h          bigint NOT NULL DEFAULT 0,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- namespace_models (N111): the namespace's CURRENT embedding model, fixed at creation. Every vector arm reads
-- embedding_model = this value. ReembedNamespace inserts vectors under the next model, builds the next index, then
-- flips this row (the one UPDATE of the workflow) and expunges the old.
CREATE TABLE namespace_models (
  namespace_id     uuid PRIMARY KEY,
  tenant_id        text NOT NULL,
  embedding_model  text NOT NULL CHECK (octet_length(embedding_model) BETWEEN 1 AND 128),
  embedding_dims   integer NOT NULL DEFAULT 768 CHECK (embedding_dims = 768),   -- the vector column is halfvec(768)
  -- set while ReembedNamespace runs
  next_model       text CHECK (next_model IS NULL OR octet_length(next_model) BETWEEN 1 AND 128),
  changed_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE TRIGGER namespace_models_touch BEFORE UPDATE ON namespace_models
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- vector_indexes (N112, N138): which per-namespace partial HNSW indexes exist, as a machine requested -> building ->
-- ready, or failed (and dropping). The stats sweeper (engram_admin) INSERTs a 'requested' row; the INDEX RUNNER
-- (`engramctl index`, role engram_migrate, the only process that issues index DDL) leases it (state 'building',
-- lease_until), checks pg_index.indisvalid and drops an invalid index BEFORE every build (never CREATE ... IF NOT
-- EXISTS), then marks it 'ready' with rows_at_build; a build whose lease expired is re-leased. purged_since_build is
-- the expunge's own bookkeeping (one writer, engram_admin): a touched index is DUE at max(1 % of rows_at_build, 2 k)
-- purged elements (view engram_index_hygiene_due, N152); when any index on a partition is due the runner rebuilds every
-- touched index on that partition (view engram_index_partition_hygiene) with REINDEX INDEX CONCURRENTLY and only then
-- vacuums the partition.
CREATE TABLE vector_indexes (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  vector_table     text NOT NULL CHECK (vector_table IN ('fact_vectors', 'chunk_vectors', 'observation_version_vectors',
                                                         'page_version_vectors')),
  embedding_model  text NOT NULL,
  state            text NOT NULL DEFAULT 'requested' CHECK (state
                                                            IN ('requested', 'building', 'ready', 'failed',
                                                                'dropping')),
  requested_at     timestamptz NOT NULL DEFAULT now(),
  lease_owner      text,
  lease_until      timestamptz,
  rows_at_build    bigint NOT NULL DEFAULT 0 CHECK (rows_at_build >= 0),
  purged_since_build bigint NOT NULL DEFAULT 0 CHECK (purged_since_build >= 0),
  built_at         timestamptz,
  last_error       text,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, vector_table, embedding_model),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (state <> 'building' OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL))
) WITH (fillfactor = 70);

CREATE TRIGGER vector_indexes_touch BEFORE UPDATE ON vector_indexes
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- outbox: transactional outbox and per-shard change log (D6). PK is the global seq (the relay reads in seq order);
-- (namespace_id, seq) serves export deltas and the purge's consumer check. namespace_id, tenant_id and epoch are
-- stamped from the transaction scope, never passed by the application. Events are thin (N12, N80): ids, versions and
-- flags; never text or vectors; a delete is ONE O(1) marker event (DocumentDeleted); ids are 16-byte `bytes`, an event
-- carries at most 256 ids, larger sets are paged, above 4,096 ids the event carries counts only (ids_elided; consumers
-- delete by the indexed (namespace_id, document_id, document_version <= up_to_version) query, N119, N133c). Moves do
-- not read the outbox (N124). The CHECK is the backstop, not the mechanism.
CREATE SEQUENCE outbox_seq AS bigint CACHE 1;

CREATE TABLE outbox (
  seq           bigint PRIMARY KEY DEFAULT nextval('outbox_seq'),
  namespace_id  uuid   NOT NULL DEFAULT current_setting('engram.namespace_id')::uuid,
  tenant_id     text   NOT NULL DEFAULT current_setting('engram.tenant_id'),
  epoch         bigint NOT NULL DEFAULT current_setting('engram.epoch')::bigint,
  event_type    text   NOT NULL CHECK (event_type ~ '^[A-Z][A-Za-z0-9]{2,63}$'),  -- Event oneof case name
  payload       bytea  NOT NULL CHECK (octet_length(payload) <= 16384),          -- engram.internal.events.v1.Event
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_ns_seq_idx ON outbox (namespace_id, seq);

-- outbox_skipped: poison events an operator skipped for one consumer (engramctl outbox skip).
CREATE TABLE outbox_skipped (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  consumer      text NOT NULL,
  seq           bigint NOT NULL,
  reason        text NOT NULL,
  skipped_by    text NOT NULL,
  skipped_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, consumer, seq)
);

-- deletion_log (N122): the shard-local "already applied" record of every marker transaction (delete, invalidate,
-- restore), written IN the marker transaction. intent_key is the NAME of the intent object
-- `_control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json`, which the API puts AFTER the marker commits and
-- BEFORE the ack; the name is derived from this row, so it is the idempotency key and `engramctl restore replay`
-- re-applies each intent at most once. prev_operation_id is the subject's previous entry read under the SUBJECT lock
-- (N150, N159): replay applies a subject's intents in chain order, never by clock. effect is the marker's exact effect,
-- so a duplicate attempt that finds the subject already deleted re-puts the committed marker's own intent
-- (put-if-absent). Insert-only.
-- epoch (N143): the namespace epoch the marker committed under, copied into the intent object. Replay applies a
-- subject's intents in chain order and SKIPS an intent whose epoch is older than that of an entry of the same subject
-- already applied (here or by an earlier replay step), so a stale intent can never override a later one.
-- N174: Invalidate reuses the invalidation_op of the subject's fact_hidden(invalidate) rows; this row records it. A
-- second Invalidate or Restore of the same fact is NOT a silent no-op: it writes its own row (fresh operation_id,
-- prev_operation_id = the subject's last entry) and its own intent, although visibility does not change.
-- N150: epoch is ALWAYS the intent's recorded epoch (a replay keeps it; the guard compares recorded epochs only);
-- applied_epoch is the epoch a replay committed under (informational). The subject is (class, id) independent of the
-- action kind (document | memory = invalidate and restore | namespace | tenant), so the chain of a fact reads the last
-- entry of either kind. N162: subject_class + subject_id hold the ENCODED subject `class:key`; for a memory the key is
-- `document_id ':' content_hash(hex)` of the fact the caller named, so a fact and its re-extraction twins (same
-- document, same hash) are ONE subject with ONE chain, ONE lock and ONE intent, and a client-chosen document_id cannot
-- alias it. The chain TIP is the subject's row with the greatest ins_seq (insert-only, monotone across moves, N147),
-- never deleted_at; every marker writer on a subject first takes the SESSION-level subject lock
-- (engram_subject_lock_key(ns, subject_id)) and HOLDS it until its intent is put and the marker re-read (N159), so
-- concurrent calls form one chain and a successor cannot commit or put while its predecessor's put is in flight. Under
-- the lock the writer first puts the intent of the subject's latest entry if that object is absent (help-previous):
-- effect, epoch, operation_id, prev_operation_id and deleted_at of this row are exactly the intent's body, so a crash
-- between a commit and its put never leaves a hole in the prev_operation_id chain (Durability_NoHelpPrev).
-- replay_outcome (N159): a replay inserts a row for EVERY intent it settles, 'applied' (effect applied, applied_epoch
-- set) or 'skipped' (older recorded epoch than an applied entry of its subject, or a namespace/tenant intent whose
-- catalog row is not deleting|deleted; nothing applied, applied_epoch NULL). `restore replay` is complete, and the
-- shard reopens, when every in-window intent has a row, so a skipped intent is never waited on. NULL = a live marker
-- transaction.
CREATE TABLE deletion_log (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  intent_key    text NOT NULL CHECK (octet_length(intent_key) BETWEEN 1 AND 512),
  kind          text NOT NULL CHECK (kind IN ('document', 'invalidate', 'restore', 'namespace', 'tenant')),
  subject_class text NOT NULL CHECK (subject_class IN ('document', 'memory', 'namespace', 'tenant')),   -- N162
  -- N162: engram_subject_id(class, key): document:<id> | memory:<document_id>:<hash hex> | namespace:<id> | tenant:<id>
  subject_id    text NOT NULL,
  epoch         bigint NOT NULL,                   -- the intent's RECORDED epoch (N150), never the shard's current one
  -- N150: informational, set by a replay
  applied_epoch bigint CHECK (applied_epoch IS NULL OR applied_epoch >= epoch),
  -- N159: settled by a replay
  replay_outcome text CHECK (replay_outcome IS NULL OR replay_outcome IN ('applied', 'skipped')),
  operation_id  uuid,
  prev_operation_id uuid,
  invalidation_op uuid,                            -- N174: the tag an Invalidate used
  effect        jsonb NOT NULL CHECK (jsonb_typeof(effect) = 'object'),   -- {up_to_version} | {memory_ids}
  deleted_at    timestamptz NOT NULL DEFAULT now(),
  ins_seq       bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, intent_key),
  CHECK (replay_outcome IS DISTINCT FROM 'skipped' OR applied_epoch IS NULL),
  CHECK (replay_outcome IS DISTINCT FROM 'applied' OR applied_epoch IS NOT NULL),
  -- N162: the id is the encoded form of its class
  CHECK (left(subject_id, length(subject_class) + 1) = subject_class || ':'),
  CHECK (subject_class = CASE kind WHEN 'invalidate' THEN 'memory' WHEN 'restore' THEN 'memory' ELSE kind END),
  CHECK (invalidation_op IS NULL OR kind = 'invalidate')                                   -- PG8-14
-- insert-only class (N137): fillfactor 100 like every other table of the class. The reference file omits it here; the
-- self-check of 0004 and TestContent_InsertOnly pin it.
) WITH (fillfactor = 100);

-- N150, N162: the chain tip of a subject (either kind) by ins_seq
CREATE INDEX deletion_log_subject_idx ON deletion_log (namespace_id, subject_class, subject_id, ins_seq DESC);

-- =============================================================================
-- Namespace-scoped tables. The class of each table (insert-only, mutable, expiring, shard-local; N113, N137) is the
-- COMMENT ON TABLE set in the "Table classes" block after the partitions.
-- =============================================================================

-- ingest_ledger: append-only raw inputs. Body inline when <= 64 KiB; larger bodies live in the OWNER-KEYED blob
-- ledger/{ledger_id} (N104): ledger_id is minted before the put, the put precedes this row, and the blob dies with this
-- row in the purge, so no reference check or adoption race exists. Rows are never purged by REPLACE/APPEND retirement,
-- only by an explicit delete expunge.
CREATE TABLE ingest_ledger (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  ledger_id       uuid NOT NULL,
  document_id     text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  operation_id    uuid NOT NULL,
  request_id      text CHECK (octet_length(request_id) <= 128),
  update_mode     update_mode NOT NULL,
  item_timestamp  timestamptz NOT NULL,
  context         text NOT NULL DEFAULT '' CHECK (octet_length(context) <= 4096),
  tags            text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  metadata        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  entity_hints    jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(entity_hints) = 'array'),
  content_hash    bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(body)
  content_bytes   integer NOT NULL CHECK (content_bytes BETWEEN 0 AND 1048576),
  body            text,                            -- inline iff content_bytes <= 65536
  body_blob_key   text,                            -- ledger/{ledger_id} iff content_bytes > 65536
  received_at     timestamptz NOT NULL DEFAULT now(),
  ins_seq         bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, ledger_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((content_bytes <= 65536) = (body IS NOT NULL)),
  CHECK ((content_bytes >  65536) = (body_blob_key IS NOT NULL)),
  CHECK (body_blob_key IS NULL OR body_blob_key = 'ledger/' || ledger_id::text)
) WITH (fillfactor = 100);

CREATE INDEX ingest_ledger_doc_idx ON ingest_ledger (namespace_id, document_id, received_at DESC);
CREATE INDEX ingest_ledger_op_idx  ON ingest_ledger (namespace_id, operation_id);

CREATE TRIGGER ingest_ledger_append_only BEFORE UPDATE OR DELETE ON ingest_ledger
  FOR EACH ROW EXECUTE FUNCTION engram_forbid_ledger_mutation();

-- documents (D8): MUTABLE (state, current_version, life_start, tags, metadata, context; tags are item-level and live
-- HERE only, N113). state 'deleting' is set in the same transaction as the tombstone (N115). A Retain of a 'deleting'
-- document REVIVES it (N133c) in the ack transaction:
-- state 'active', deleted_at NULL, current_version 0 (nothing finalised yet) and life_start = the new version, which is
-- greatest(max(document_versions.version), max(document_tombstones.up_to_version))
-- + 1, so a version number never falls at or below a tombstone's up_to_version. life_start is the first version of the
-- document's current life (1 for a new document): chunk identity is (document_id, content_hash, life_start), so a
-- revived document never re-uses a chunk row that a tombstone covers. The marker transaction clears the content-bearing
-- columns in the same row update (summary_blob_key, summary_hash, document_hash, context, metadata, tags; the summary
-- blob goes to blob_tombstones) and the CHECK below pins it, so GetDocument/ListDocuments return the tombstone view
-- (document_tombstone_view) only (N115, N136). The Expunge deletes this row only when no open tombstone with a higher
-- up_to_version exists and no document_versions row remains (C-12).
CREATE TABLE documents (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  document_id       text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  life_start        integer NOT NULL DEFAULT 1 CHECK (life_start >= 1),
  state             document_state NOT NULL DEFAULT 'active',
  item_timestamp    timestamptz,
  context           text NOT NULL DEFAULT '',
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  -- N157/A-13: bumped by every tag change; the export delta's documents part is keyed (document_id, tag_generation)
  tag_generation    integer NOT NULL DEFAULT 0 CHECK (tag_generation >= 0),
  metadata          jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  -- hash of the chunk-hash list
  document_hash     bytea CHECK (document_hash IS NULL OR octet_length(document_hash) = 32),
  summary_blob_key  text,                         -- docsum/{sha256(document_hash || prompt_version || model)}.json
  summary_hash      bytea CHECK (summary_hash IS NULL OR octet_length(summary_hash) = 32),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  PRIMARY KEY (namespace_id, document_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((state = 'active') = (deleted_at IS NULL)),
  CHECK (state = 'active' OR (summary_blob_key IS NULL AND summary_hash IS NULL AND document_hash IS NULL
                              AND context = '' AND metadata = '{}'::jsonb AND tags = '{}'))
) WITH (fillfactor = 70);

CREATE INDEX documents_updated_idx  ON documents (namespace_id, updated_at DESC);

-- tag_counts (N157/A-13): documents per tag, maintained in the SAME transactions that change documents.tags (ack,
-- UpdateDocumentTags, the delete marker); ListTags reads it instead of unnesting every document.
CREATE TABLE tag_counts (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  tag           text NOT NULL,
  doc_count     integer NOT NULL CHECK (doc_count >= 0),
  PRIMARY KEY (namespace_id, tag),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);
CREATE INDEX documents_deleting_idx ON documents (namespace_id, deleted_at) WHERE state = 'deleting';
-- the tag filter is resolved once per recall against this index into an allowed-document set (N116)
CREATE INDEX documents_tags_gin     ON documents USING gin (namespace_id, tags);
-- metadata filters resolve at document level into $allowed_docs like tags (N139, A-13)
CREATE INDEX documents_metadata_gin ON documents USING gin (namespace_id, metadata jsonb_path_ops);

CREATE TRIGGER documents_touch BEFORE UPDATE ON documents
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- document_versions: MUTABLE (status, chunks_done, body_key set once). H-16 / N104: body_key and body_hash are
-- NULLABLE. LoadItem creates the row without a body, then stores the reconstructed body as the OWNER-KEYED blob
-- ver/{document_id}/v{version} (N104: one owner row, dies with it in the purge; cross-document dedup of bodies is given
-- up) and sets both WHERE body_key IS NULL; LoadItem(APPEND) materialises the base body from the ledger chain up to the
-- nearest version that has one (append_base_version), under the document lock.
CREATE TABLE document_versions (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  document_id     text NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  content_hash    bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- = ingest_ledger.content_hash
  status          version_status NOT NULL DEFAULT 'ingesting',
  update_mode     update_mode NOT NULL,
  append_base_version integer CHECK (append_base_version IS NULL OR append_base_version < version),
  operation_id    uuid NOT NULL,
  ledger_id       uuid NOT NULL,
  chunk_count     integer CHECK (chunk_count >= 0),
  -- written once by FinalizeVersion: the eligible-rows estimate of the selectivity-aware plan sums it over the current
  -- versions (N138)
  fact_count      integer CHECK (fact_count >= 0),
  -- {"world": n, "experience": m}, written with fact_count (N151): the estimate honours the requested fact types
  fact_count_by_type jsonb CHECK (fact_count_by_type IS NULL OR jsonb_typeof(fact_count_by_type) = 'object'),
  -- written per wave by the workflow (N69), never per chunk
  chunks_done     integer NOT NULL DEFAULT 0 CHECK (chunks_done >= 0),
  -- sha256 of the FULL reconstructed body; NULL until stored
  body_hash       bytea CHECK (body_hash IS NULL OR octet_length(body_hash) = 32),
  -- ver/{document_id}/v{version} (owner-keyed blob, N104)
  body_key        text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  activated_at    timestamptz,
  finished_at     timestamptz,
  PRIMARY KEY (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  FOREIGN KEY (namespace_id, ledger_id)   REFERENCES ingest_ledger (namespace_id, ledger_id),
  CHECK ((fact_count IS NULL) = (fact_count_by_type IS NULL)),
  CHECK (body_key IS NULL OR body_hash IS NOT NULL),
  CHECK (body_key IS NULL OR body_key = 'ver/' || document_id || '/v' || version::text),
  CHECK ((update_mode = 'append') = (append_base_version IS NOT NULL))
) WITH (fillfactor = 70);

-- at most one active version per document
CREATE UNIQUE INDEX document_versions_active_uq ON document_versions (namespace_id, document_id)
  WHERE status = 'active';
CREATE INDEX document_versions_op_idx ON document_versions (namespace_id, operation_id);
-- Lock protocol (N40 as amended by N83; model-checked in section 7). CommitChunk(v) takes
-- engram_try_doc_lock_shared(ns, doc) (failure -> retryable DocumentBusy, 100 ms), reads the version row and
-- documents.current_version as PLAIN reads and proceeds only when status = 'ingesting'. FinalizeVersion and the delete
-- marker transaction take the same key EXCLUSIVE, so they wait for every in-flight commit of v, and a commit that
-- starts afterwards sees the terminal status and stops. The retain ack's version assignment keeps only the documents
-- row lock (FOR UPDATE) and takes no advisory lock, so ack latency is never behind ingest commits.

-- chunks (hash-partitioned), INSERT-ONLY. Identity within a document = content hash of the text (N6: the contextual
-- header is hashed separately). Text <= 4,000 characters, checked as <= 16 KiB of UTF-8 (D8). Position in a version
-- lives in document_version_chunks.ordinal; the vector in chunk_vectors; retirement in chunk_tombstones. The extraction
-- key is NOT here: a prompt or model bump re-extracts a kept chunk and the new facts carry the new key (N58).
-- document_version is the version whose CommitChunk inserted the row (the version a document tombstone's up_to_version
-- is compared with, N133c). life_start is documents.life_start at insert: identity within a document is (content hash,
-- life), so the same text in a revived document is a NEW row and the covered one is purged by the Expunge without
-- touching the new content.
CREATE TABLE chunks (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  chunk_id         uuid NOT NULL,
  document_id      text NOT NULL,
  document_version integer NOT NULL CHECK (document_version >= 1),
  life_start       integer NOT NULL CHECK (life_start >= 1),
  content_hash     bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(text)
  header_hash      bytea NOT NULL CHECK (octet_length(header_hash) = 32),    -- sha256(header) (N6)
  heading_path     text NOT NULL DEFAULT '' CHECK (octet_length(heading_path) <= 1024),
  -- "[doc summary] > [heading path]" as first embedded
  header           text NOT NULL DEFAULT '' CHECK (octet_length(header) <= 1024),
  text             text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 16384),
  -- N86: max(timestamp of every item whose bytes the chunk covers); facts inherit it
  mentioned_at     timestamptz NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  ins_seq          bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, chunk_id),
  UNIQUE (namespace_id, document_id, content_hash, life_start),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  CHECK (life_start <= document_version)
) PARTITION BY HASH (namespace_id);

-- membership and position of chunks in document versions (REPLACE tombstones chunks not in the new set)
CREATE TABLE document_version_chunks (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  document_id    text NOT NULL,
  version        integer NOT NULL,
  content_hash   bytea NOT NULL,
  chunk_id       uuid NOT NULL,
  ordinal        integer NOT NULL CHECK (ordinal >= 0),
  created_at     timestamptz NOT NULL DEFAULT now(),
  ins_seq        bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, document_id, version, content_hash),
  FOREIGN KEY (namespace_id, document_id, version) REFERENCES document_versions (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, chunk_id) REFERENCES chunks (namespace_id, chunk_id) ON DELETE CASCADE
) WITH (fillfactor = 100);

CREATE INDEX document_version_chunks_chunk_idx ON document_version_chunks (namespace_id, chunk_id);

-- facts (hash-partitioned), INSERT-ONLY. memory_id is the public id of a fact (D1, UUIDv7). No tags (item-level tags
-- live on documents), no retired_at / invalidated_at / live (markers, N115), no vector (fact_vectors), no generated
-- column.
CREATE TABLE facts (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  memory_id            uuid NOT NULL,
  document_id          text NOT NULL,
  -- version whose CommitChunk inserted the fact (N133c)
  document_version     integer NOT NULL CHECK (document_version >= 1),
  chunk_id             uuid NOT NULL,
  ordinal              smallint NOT NULL CHECK (ordinal >= 0),               -- position in the chunk's extraction
  -- sha256(normalised text); the curation_log key (N115)
  content_hash         bytea NOT NULL CHECK (octet_length(content_hash) = 32),
  -- N87 key = xcache key; a re-extraction of a kept chunk inserts facts under a new key and marks the old ones
  -- fact_hidden(reextract): a write, not a hide of derived content (N58, N135)
  extraction_key       bytea NOT NULL CHECK (octet_length(extraction_key) = 32),
  text                 text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
  fact_type            fact_type NOT NULL,
  -- {who:[],what,when,where,why}
  w5                   jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(w5) = 'object'),
  occurred_start       timestamptz,
  occurred_end         timestamptz,
  -- = the item's timestamp, SERVER-SET; the only as_of key (D9); never written by the extractor or a per-item override
  mentioned_at         timestamptz NOT NULL,
  -- the model's judgement of when the source said it: display and ranking only, never an as_of key
  said_at              timestamptz,
  metadata             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  extraction_version   integer NOT NULL CHECK (extraction_version >= 1),    -- extraction schema version
  prompt_version       text NOT NULL,                                        -- e.g. extract/v1
  model                text NOT NULL,                                        -- models.extract used
  created_at           timestamptz NOT NULL DEFAULT now(),
  ins_seq              bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, memory_id),
  UNIQUE (namespace_id, chunk_id, extraction_key, content_hash),            -- CommitChunk idempotency (no epoch, D11)
  FOREIGN KEY (namespace_id, chunk_id)    REFERENCES chunks (namespace_id, chunk_id),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  CHECK (occurred_start IS NULL OR occurred_end IS NULL OR occurred_end >= occurred_start)
) PARTITION BY HASH (namespace_id);

-- Vector side tables (N111): INSERT-ONLY, hash-partitioned by namespace_id like their parents, one row per (content id,
-- embedding model). The arms read embedding_model = the namespace's CURRENT model (namespace_models); ReembedChunk and
-- a model change INSERT rows, ReembedNamespace flips the current model and the expunge removes the old rows. The
-- columns after the key are immutable COPIES of attributes of the content row, so every visibility and as_of predicate
-- can run INSIDE the (iterative) index scan without a join: document_id, document_version and chunk_id for the marker
-- sets (N116, N133c), mentioned_at / effective_at for as_of, fact_type for the type filter (N138). STORAGE MAIN keeps
-- the vector inline so the exact scan is a heap scan.
CREATE TABLE fact_vectors (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  memory_id        uuid NOT NULL,
  embedding_model  text NOT NULL,
  document_id      text NOT NULL,
  document_version integer NOT NULL CHECK (document_version >= 1),
  chunk_id         uuid NOT NULL,
  -- immutable copy (N138): the type filter runs inside the scan
  fact_type        fact_type NOT NULL,
  mentioned_at     timestamptz NOT NULL,
  embedding        halfvec(768) STORAGE MAIN NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  ins_seq          bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, memory_id, embedding_model),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);

-- chunk_vectors: a summary refresh re-embeds a chunk without re-extracting (N110a): the new row has a newer
-- embedding_effective_at (N85: mentioned_at of the newest item covered by the summary in the embedded header). That
-- column is therefore part of the key (a documented extension of N111's key): under as_of = T the chunk arm admits only
-- rows with embedding_effective_at <= T and mentioned_at <= T, and takes the best admitted row per chunk.
CREATE TABLE chunk_vectors (
  namespace_id           uuid NOT NULL,
  tenant_id              text NOT NULL,
  chunk_id               uuid NOT NULL,
  embedding_model        text NOT NULL,
  embedding_effective_at timestamptz NOT NULL,
  document_id            text NOT NULL,
  document_version       integer NOT NULL CHECK (document_version >= 1),
  mentioned_at           timestamptz NOT NULL,
  embedding              halfvec(768) STORAGE MAIN NOT NULL,
  created_at             timestamptz NOT NULL DEFAULT now(),
  ins_seq                bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, chunk_id, embedding_model, embedding_effective_at),
  FOREIGN KEY (namespace_id, chunk_id) REFERENCES chunks (namespace_id, chunk_id) ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);

-- fact_links (hash-partitioned), INSERT-ONLY. One row per edge. entity/temporal/semantic edges are undirected and
-- stored once in canonical order (src < dst); causal edges are directed (src = cause, dst = effect). Per-fact caps
-- (temporal <= 20, semantic <= 10, entity <= 10 per shared entity) are enforced by the linker (section 5). A link is
-- inserted in the CommitChunk of its newer endpoint. The graph arm joins the endpoints and requires BOTH to be visible
-- (N116); the expunge deletes the rows through the FK cascade from facts.
CREATE TABLE fact_links (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  src_memory_id  uuid NOT NULL,
  dst_memory_id  uuid NOT NULL,
  link_type      link_type NOT NULL,
  weight         real NOT NULL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),
  ins_seq        bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  -- N154: covering, so a hop never reads the heap
  PRIMARY KEY (namespace_id, src_memory_id, dst_memory_id, link_type) INCLUDE (weight),
  FOREIGN KEY (namespace_id, src_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, dst_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  CHECK (src_memory_id <> dst_memory_id),
  CHECK (link_type = 'causal' OR src_memory_id < dst_memory_id)
) PARTITION BY HASH (namespace_id);

-- entities / entity_aliases / entity_mentions (N118) entities is MUTABLE (mention_count, last_seen_at, merged_into; the
-- expunge recomputes canonical_name from the remaining mentions). Under as_of an EntityRef carries only `mention`:
-- canonical_name and alias merges are suppressed and entity hops use entity_mentions.mentioned_at <= T.
CREATE TABLE entities (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  entity_id       uuid NOT NULL,
  canonical_name  text NOT NULL CHECK (octet_length(canonical_name) BETWEEN 1 AND 256),
  -- lower/trim/unaccent, computed in Go
  canonical_norm  text NOT NULL CHECK (octet_length(canonical_norm) BETWEEN 1 AND 256),
  entity_type     text NOT NULL DEFAULT 'other'
                  CHECK (entity_type IN ('person', 'organization', 'location', 'product', 'event', 'concept', 'other')),
  mention_count   integer NOT NULL DEFAULT 0 CHECK (mention_count >= 0),
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at    timestamptz NOT NULL DEFAULT now(),
  merged_into     uuid,                           -- set by EntitiesMerged; the row stays so aliases still resolve
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, entity_id),
  FOREIGN KEY (namespace_id, tenant_id)   REFERENCES namespace_ownership (namespace_id, tenant_id),
  FOREIGN KEY (namespace_id, merged_into) REFERENCES entities (namespace_id, entity_id),
  CHECK (merged_into IS NULL OR merged_into <> entity_id)
) WITH (fillfactor = 70);

CREATE UNIQUE INDEX entities_canonical_uq ON entities (namespace_id, canonical_norm)
  WHERE merged_into IS NULL;
-- fuzzy resolution: multi-column GIN (btree_gin for the uuid) so the index leads with namespace_id. Reached ONLY
-- through engram_entity_fuzzy (SECURITY DEFINER, N131): the trigram operators are not leakproof, so under RLS the
-- planner would not push them into the index scan (P-5).
CREATE INDEX entities_trgm_idx ON entities USING gin (namespace_id, canonical_norm gin_trgm_ops);

CREATE TRIGGER entities_touch BEFORE UPDATE ON entities
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

CREATE TABLE entity_aliases (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  entity_id     uuid NOT NULL,
  alias         text NOT NULL CHECK (octet_length(alias) BETWEEN 1 AND 256),
  alias_norm    text NOT NULL CHECK (octet_length(alias_norm) BETWEEN 1 AND 256),
  source        text NOT NULL CHECK (source IN ('extracted', 'hint', 'merge')),
  -- N118: the document whose item produced the alias; the expunge deletes the victim's aliases
  document_id   text,
  -- N133c: the expunge deletes only document_version <= up_to_version
  document_version integer CHECK (document_version >= 1),
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, alias_norm),
  FOREIGN KEY (namespace_id, entity_id) REFERENCES entities (namespace_id, entity_id) ON DELETE CASCADE,
  CHECK ((document_id IS NULL) = (document_version IS NULL))
);

CREATE INDEX entity_aliases_entity_idx ON entity_aliases (namespace_id, entity_id);
CREATE INDEX entity_aliases_document_idx ON entity_aliases (namespace_id, document_id, document_version)
    WHERE document_id IS NOT NULL;

-- entity_mentions (hash-partitioned), INSERT-ONLY. mentioned_at is the fact's, copied (N118).
CREATE TABLE entity_mentions (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  memory_id     uuid NOT NULL,
  entity_id     uuid NOT NULL,
  role          text NOT NULL DEFAULT 'other' CHECK (role IN ('who', 'what', 'where', 'when', 'why', 'other')),
  confidence    real NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
  mentioned_at  timestamptz NOT NULL,
  ins_seq       bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, memory_id, entity_id),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, entity_id) REFERENCES entities (namespace_id, entity_id)
) PARTITION BY HASH (namespace_id);

-- observations (D9, D12): MUTABLE narrow state of an observation. Version content is insert-only
-- (observation_versions). stale_write / stale_delete mean "needs a rewrite" and never hide anything by themselves
-- (N117: hiding is the read predicate).
CREATE TABLE observations (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  observation_id    uuid NOT NULL,
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  proof_count       integer NOT NULL DEFAULT 0 CHECK (proof_count >= 0),
  -- evidence changed under it (replace-retire, restore, re-extraction): rewrite wanted; FinalizeVersion sets it in its
  -- own transaction (N135)
  stale_write       boolean NOT NULL DEFAULT false,
  -- Materialize found a victim in its segment: root rebuild wanted (N119)
  stale_delete      boolean NOT NULL DEFAULT false,
  stale_since       timestamptz,
  -- N144: monotone, bumped by every writer that sets a stale flag; a rewrite captures it and clears the flags ONLY if
  -- unchanged (compare-and-clear, as pages, N37)
  stale_seq         bigint NOT NULL DEFAULT 0 CHECK (stale_seq >= 0),
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),   -- consolidation scope tags
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  PRIMARY KEY (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((stale_write OR stale_delete) = (stale_since IS NOT NULL))
) WITH (fillfactor = 70);

CREATE INDEX observations_stale_idx ON observations (namespace_id, stale_delete DESC, stale_since)
  WHERE (stale_write OR stale_delete) AND retired_at IS NULL;      -- delete-driven rebuilds first (section 5.2)
CREATE INDEX observations_tags_gin ON observations USING gin (namespace_id, tags) WHERE retired_at IS NULL;

CREATE TRIGGER observations_touch BEFORE UPDATE ON observations
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- observation_versions (hash-partitioned), INSERT-ONLY (N117). root_version = version for a ROOT REBUILD (written from
-- live sources only, no previous text shown), else root_version(v - 1): the derivation set of (O, v) is
-- observation_inputs(O, w) for root_version(v) <= w <= v. effective_at(v) = max(mentioned_at of every fact shown to v's
-- writer, effective_at(v - 1)) (D9), monotone. The one value a later version adds to an earlier one, superseded_at,
-- lives in observation_version_meta. UNIQUE (namespace_id, ov_id): ov_id is the single-column BM25 key_field.
-- stub (N136): DerivedPurge replaces a covered version by a CONTENT-FREE stub in one admin transaction (delete the
-- version row, which cascades its inputs, sources, vector row and BM25 entry; insert the same (ov_id, version,
-- root_version, effective_at) with text = '' and stub = true). The stub keeps the D9 range arithmetic and the
-- *_version_meta rows valid, is what the fail-closed predicate finds, and stays covered by the permanent derived_hidden
-- row. It is an insert, never an UPDATE.
CREATE TABLE observation_versions (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  ov_id             uuid NOT NULL,
  observation_id    uuid NOT NULL,
  version           integer NOT NULL CHECK (version >= 1),
  root_version      integer NOT NULL,
  text              text NOT NULL,
  effective_at      timestamptz NOT NULL,
  source_count      integer NOT NULL,
  prompt_version    text NOT NULL,
  model             text NOT NULL,
  stub              boolean NOT NULL DEFAULT false,
  -- N144: sha256(id || base_version || evidence_hash || prompt_version || attempt_nonce); NULL only on a stub
  commit_key        bytea CHECK (commit_key IS NULL OR octet_length(commit_key) = 32),
  created_at        timestamptz NOT NULL DEFAULT now(),
  ins_seq           bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, observation_id, version),
  UNIQUE (namespace_id, ov_id),
  -- N144: the commit transaction asks for it FIRST; a hit means a previous attempt committed
  UNIQUE (namespace_id, observation_id, commit_key),
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  CHECK (root_version BETWEEN 1 AND version),
  CHECK (CASE WHEN stub THEN text = '' AND source_count = 0
              ELSE octet_length(text) BETWEEN 1 AND 8192 AND source_count >= 1 END),
  CHECK (stub OR commit_key IS NOT NULL)
) PARTITION BY HASH (namespace_id);

CREATE TABLE observation_version_vectors (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  ov_id            uuid NOT NULL,
  embedding_model  text NOT NULL,
  observation_id   uuid NOT NULL,
  version          integer NOT NULL,
  effective_at     timestamptz NOT NULL,
  -- immutable copy of observations.tags at insert (N151): the tag test runs inside the scan (not named 'tags': check 12
  -- keeps content tables tag-free)
  obs_tags         text[] NOT NULL DEFAULT '{}',
  embedding        halfvec(768) STORAGE MAIN NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  ins_seq          bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, ov_id, embedding_model),
  FOREIGN KEY (namespace_id, ov_id) REFERENCES observation_versions (namespace_id, ov_id) ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);

-- observation_version_meta (N33): write-once. When version v + 1 is inserted, ApplyBatch inserts (O, v, superseded_at =
-- effective_at(v + 1)) in the same transaction; the current version has NO row. An as_of query is then a plain range
-- filter: effective_at <= T AND (no meta row OR superseded_at > T). The FK is DEFERRED and does not cascade (N136):
-- DerivedPurge deletes a version row and inserts its stub in one transaction, and the meta row must survive; purges of
-- a whole observation delete the meta rows first.
CREATE TABLE observation_version_meta (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  superseded_at   timestamptz NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  ins_seq         bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, observation_id, version),
  FOREIGN KEY (namespace_id, observation_id, version)
    REFERENCES observation_versions (namespace_id, observation_id, version) DEFERRABLE INITIALLY DEFERRED
) WITH (fillfactor = 100);

-- observation_sources is the mutable WORKING SET of the CURRENT version (evidence the consolidator may still extend),
-- rewritten only by ApplyBatch under the derivation lock (N120). The evidence of each version is frozen in
-- observation_version_sources.
CREATE TABLE observation_sources (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  memory_id       uuid NOT NULL,
  quote           text NOT NULL DEFAULT '' CHECK (octet_length(quote) <= 2048),
  added_version   integer NOT NULL CHECK (added_version >= 1),
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, memory_id),
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, memory_id)      REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
) WITH (fillfactor = 70);

CREATE INDEX observation_sources_memory_idx ON observation_sources (namespace_id, memory_id);

-- observation_inputs (N41, N117), INSERT-ONLY: every fact the stage-2 writer was SHOWN when it wrote version v (the
-- batch facts attached to O; at most 5 quoted older sources; all of them O's own sources, N121). document_id and
-- document_version are denormalised (immutable copies of the fact's) so the read predicate and the expunge reach a
-- victim document version without joining facts. NO foreign key to facts (N135): evidence outlives the facts it names
-- (REPLACE then a chunk purge must not erase what a later DeleteDocument needs to find); the row dies with its own
-- version only.
CREATE TABLE observation_inputs (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  fact_id         uuid NOT NULL,                       -- = facts.memory_id
  document_id     text NOT NULL,                       -- = facts.document_id (immutable copy)
  document_version integer NOT NULL CHECK (document_version >= 1),   -- = facts.document_version (immutable copy, N133c)
  created_at      timestamptz NOT NULL DEFAULT now(),
  ins_seq         bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, observation_id, version, fact_id),
  FOREIGN KEY (namespace_id, observation_id, version)
    REFERENCES observation_versions (namespace_id, observation_id, version) ON DELETE CASCADE
) WITH (fillfactor = 100);

CREATE INDEX observation_inputs_document_idx ON observation_inputs (namespace_id, document_id, document_version);
-- FinalizeVersion's stale_write update for re-extraction (N135)
CREATE INDEX observation_inputs_fact_idx     ON observation_inputs (namespace_id, fact_id);

-- observation_version_sources (N85), INSERT-ONLY: the cited evidence of EACH version, copied from
-- consolidation_proposals.ops (quote <= 500 chars) when the version is applied. A source is visible iff its own
-- document_version is not covered by an open tombstone and its fact has no fact_hidden(invalidate) row (the document
-- columns are copies, like observation_inputs', so no join to facts is needed and a purged fact does not matter);
-- proof_count counts visible sources and a version with none is not served (N117, N135). NO foreign key to facts.
CREATE TABLE observation_version_sources (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  memory_id       uuid NOT NULL,
  document_id     text NOT NULL,                       -- = facts.document_id (immutable copy, N135)
  document_version integer NOT NULL CHECK (document_version >= 1),   -- = facts.document_version (immutable copy, N133c)
  quote           text NOT NULL DEFAULT '' CHECK (char_length(quote) <= 500),
  created_at      timestamptz NOT NULL DEFAULT now(),
  ins_seq         bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, observation_id, version, memory_id),
  FOREIGN KEY (namespace_id, observation_id, version)
    REFERENCES observation_versions (namespace_id, observation_id, version) ON DELETE CASCADE
) WITH (fillfactor = 100);

CREATE INDEX observation_version_sources_memory_idx ON observation_version_sources (namespace_id, memory_id);

-- consolidation_batches: MUTABLE exactly-once effect (D12, N121). attempt is the CURRENT attempt (a batch is re-queued
-- at most 3 times, so 1..4); state: routed = stage 1 decided; stored = the proposal of `attempt` is persisted; applied
-- = its ops took effect (written in the SAME transaction as the 'done' stamps, and by an all-skip batch that applies
-- zero ops); discarded = the stored proposal went stale (a base_version no longer current) and the batch is re-routed
-- under a new attempt, or is terminal after attempt 4; capacity = the overflow rerun (new attempt, prompt_variant
-- 'capacity') is pending. 'Already applied' is state = 'applied'. Nothing here is ever deleted by engram_app.
CREATE TABLE consolidation_batches (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  -- sha256(sorted memory ids || prompt_version || model)
  batch_key         bytea NOT NULL CHECK (octet_length(batch_key) = 32),
  round_id          uuid NOT NULL,
  memory_ids        uuid[] NOT NULL CHECK (cardinality(memory_ids) BETWEEN 1 AND 8),
  state             text NOT NULL DEFAULT 'routed'
                    CHECK (state IN ('routed', 'stored', 'applied', 'discarded', 'capacity')),
  attempt           integer NOT NULL DEFAULT 1 CHECK (attempt BETWEEN 1 AND 4),
  model             text NOT NULL,
  prompt_version    text NOT NULL,
  result_blob_key   text,                                      -- consolidate/{hex(batch_key)}.json
  error             text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  started_at        timestamptz,
  finished_at       timestamptz,
  PRIMARY KEY (namespace_id, batch_key),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE INDEX consolidation_batches_round_idx ON consolidation_batches (namespace_id, round_id);

-- consolidation_proposals (N43, N121), INSERT-ONLY and write-once, keyed (batch_key, attempt): the validated,
-- deduplicated op list of stage 2, persisted BEFORE any op is applied. op_key = sha256(batch_key || attempt ||
-- op_index) is computed over THIS stored list. ops is a JSON array in op_index order: {kind, observation_id (pre-minted
-- for creates), text, source_fact_ids[], quotes[], reason, base_version}; every update and merge op records the
-- base_version it was rendered from (CHECK). input_fact_ids = the facts rendered to the writer = the observation_inputs
-- rows of every version the batch creates. A discard or a capacity retry writes a NEW attempt; the old list is dead by
-- key. engram_app holds SELECT, INSERT only: no UPDATE, no DELETE, ever. Stage-1 routing decisions are not persisted.
CREATE TABLE consolidation_proposals (
  namespace_id        uuid NOT NULL,
  tenant_id           text NOT NULL,
  batch_key           bytea NOT NULL CHECK (octet_length(batch_key) = 32),
  attempt             integer NOT NULL CHECK (attempt BETWEEN 1 AND 4),
  prompt_variant      text NOT NULL DEFAULT 'default' CHECK (prompt_variant IN ('default', 'capacity')),
  ops                 jsonb NOT NULL CHECK (jsonb_typeof(ops) = 'array' AND engram_ops_have_base(ops)),
  op_count            integer NOT NULL CHECK (op_count BETWEEN 0 AND 16),
  input_fact_ids      uuid[] NOT NULL CHECK (cardinality(input_fact_ids) >= 1),
  prompt_version      text NOT NULL,
  model               text NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  ins_seq             bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, batch_key, attempt),
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
) WITH (fillfactor = 100);

CREATE TRIGGER consolidation_proposals_write_once BEFORE UPDATE ON consolidation_proposals
  FOR EACH ROW EXECUTE FUNCTION engram_forbid_update();

CREATE TABLE consolidation_applied (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  op_key          bytea NOT NULL CHECK (octet_length(op_key) = 32),     -- sha256(batch_key || attempt || op_index)
  batch_key       bytea NOT NULL,
  attempt         integer NOT NULL,
  op_index        integer NOT NULL CHECK (op_index >= 0),
  op_kind         text NOT NULL CHECK (op_kind IN ('create', 'update', 'merge', 'delete')),
  observation_id  uuid NOT NULL,
  version         integer,
  applied_at      timestamptz NOT NULL DEFAULT now(),
  ins_seq         bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, op_key),
  -- RESTRICT: the proposal of an applied op is never removed
  FOREIGN KEY (namespace_id, batch_key, attempt) REFERENCES consolidation_proposals (namespace_id, batch_key, attempt)
) WITH (fillfactor = 100);

CREATE INDEX consolidation_applied_batch_idx ON consolidation_applied (namespace_id, batch_key, attempt, op_index);

-- fact_consolidation (N95, H-15), INSERT-ONLY: append-only stamps, never updated. A fact is consolidated iff a 'done'
-- stamp exists; it is retryable iff its latest stamp is 'failed' and older than 7 days (a 'capacity' stamp as latest
-- means "not yet", also pending); batch_key is required exactly for 'done'.
CREATE TABLE fact_consolidation (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  memory_id     uuid NOT NULL,
  stamped_at    timestamptz NOT NULL DEFAULT now(),
  note          text NOT NULL CHECK (note IN ('done', 'failed', 'capacity')),
  batch_key     bytea CHECK (octet_length(batch_key) = 32),
  ins_seq       bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  CHECK ((note = 'done') = (batch_key IS NOT NULL)),
  PRIMARY KEY (namespace_id, memory_id, stamped_at),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
) WITH (fillfactor = 100);

CREATE INDEX fact_consolidation_batch_idx ON fact_consolidation (namespace_id, batch_key) WHERE batch_key IS NOT NULL;

-- consolidation_state (N95): ONE row per namespace, MUTABLE. The watermark advances only to just below the smallest
-- UNCONSOLIDATED fact, visible or marker-hidden (H-23: a hidden fact that Restore later reveals must still be pending),
-- and never past engram_uuid_v7_floor(now() - 2 x statement_timeout) (see engram_consolidation_watermark below).
CREATE TABLE consolidation_state (
  namespace_id         uuid PRIMARY KEY,
  tenant_id            text NOT NULL,
  watermark_memory_id  uuid,
  updated_at           timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- batch_jobs: gateway batch API jobs for backfills (D3), idempotent by batch_key
CREATE TABLE batch_jobs (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  batch_key      bytea NOT NULL CHECK (octet_length(batch_key) = 32),     -- sha256(sorted xcache keys)
  kind           text NOT NULL CHECK (kind IN ('extract', 'summarize', 'embed', 'consolidate')),
  gateway_job_id text,
  state          text NOT NULL DEFAULT 'submitted' CHECK (state IN ('submitted', 'running', 'done', 'failed')),
  item_count     integer NOT NULL CHECK (item_count >= 1),
  operation_id   uuid,
  error          text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  finished_at    timestamptz,
  PRIMARY KEY (namespace_id, batch_key),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE INDEX batch_jobs_open_idx ON batch_jobs (namespace_id, updated_at) WHERE state IN ('submitted', 'running');

CREATE TRIGGER batch_jobs_touch BEFORE UPDATE ON batch_jobs
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- pages (D12, phase 3): MUTABLE narrow state. stale_seq is a monotone counter: a refresh captures it and clears the
-- flags only if it is unchanged (section 5.3). A hidden CURRENT page version is served as
-- PreconditionFailed{PAGE_HIDDEN} until the refresh lands (N117).
CREATE TABLE pages (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  page_id           uuid NOT NULL,
  name              text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 256),
  source_query      text NOT NULL,
  tag_filter        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(tag_filter) = 'object'),  -- {mode, tags}
  refresh_policy    jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(refresh_policy) = 'object'),
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  stale_write       boolean NOT NULL DEFAULT false,
  stale_delete      boolean NOT NULL DEFAULT false,
  stale_seq         bigint NOT NULL DEFAULT 0,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  PRIMARY KEY (namespace_id, page_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE UNIQUE INDEX pages_name_uq ON pages (namespace_id, name) WHERE retired_at IS NULL;
CREATE INDEX pages_stale_idx ON pages (namespace_id, updated_at)
  WHERE (stale_write OR stale_delete) AND retired_at IS NULL;

CREATE TRIGGER pages_touch BEFORE UPDATE ON pages
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- page_versions: INSERT-ONLY. root_version as for observation versions (N117); superseded_at in page_version_meta. text
-- is the page's searchable markdown (N139, A-14): BM25 index plus page_version_vectors under the N111/N112 rules, so
-- pages share the visibility and purge path of observations. pv_id is the single-column BM25 key_field. A purged
-- version becomes a content-free stub (text = '', stub = true, the .md blob deleted), exactly like an observation
-- version (N136).
CREATE TABLE page_versions (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  page_id            uuid NOT NULL,
  version            integer NOT NULL CHECK (version >= 1),
  pv_id              uuid NOT NULL,
  root_version       integer NOT NULL,
  text               text NOT NULL,
  -- pages/{page_id}/v{version}-{sha256(markdown)[:16]}.md: ATTEMPT-UNIQUE (N144); '_stub' on a stub (never
  -- dereferenced)
  markdown_blob_key  text NOT NULL,
  effective_at       timestamptz NOT NULL,           -- D9 rule, same as observation_versions
  evidence_hash      bytea NOT NULL CHECK (octet_length(evidence_hash) = 32),   -- a stub carries 32 zero bytes
  stub               boolean NOT NULL DEFAULT false,
  -- N144: see observation_versions; NULL only on a stub
  commit_key         bytea CHECK (commit_key IS NULL OR octet_length(commit_key) = 32),
  created_at         timestamptz NOT NULL DEFAULT now(),
  ins_seq            bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, page_id, version),
  UNIQUE (namespace_id, pv_id),
  UNIQUE (namespace_id, page_id, commit_key),                -- N144: the first statement of the commit transaction
  FOREIGN KEY (namespace_id, page_id) REFERENCES pages (namespace_id, page_id),
  CHECK (root_version BETWEEN 1 AND version),
  CHECK (CASE WHEN stub THEN text = '' ELSE octet_length(text) BETWEEN 1 AND 262144 END),
  CHECK (stub OR commit_key IS NOT NULL),
  CHECK (stub = (markdown_blob_key = '_stub'))
) WITH (fillfactor = 100);

-- page_version_vectors: INSERT-ONLY, keyed (pv_id, embedding_model) like the other vector tables (N111). Pages are few,
-- so the table is not partitioned; engram_hnsw_ddl builds the same per-namespace partial HNSW on the table itself
-- (N112).
CREATE TABLE page_version_vectors (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  pv_id            uuid NOT NULL,
  embedding_model  text NOT NULL,
  page_id          uuid NOT NULL,
  version          integer NOT NULL,
  effective_at     timestamptz NOT NULL,
  embedding        halfvec(768) STORAGE MAIN NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  ins_seq          bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, pv_id, embedding_model),
  FOREIGN KEY (namespace_id, pv_id) REFERENCES page_versions (namespace_id, pv_id) ON DELETE CASCADE
) WITH (fillfactor = 100, vacuum_index_cleanup = off);

-- The FK is DEFERRED and does not cascade, like observation_version_meta (N136).
CREATE TABLE page_version_meta (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  page_id        uuid NOT NULL,
  version        integer NOT NULL CHECK (version >= 1),
  superseded_at  timestamptz NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  ins_seq        bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, page_id, version),
  FOREIGN KEY (namespace_id, page_id, version) REFERENCES page_versions (namespace_id, page_id, version) DEFERRABLE
      INITIALLY DEFERRED
) WITH (fillfactor = 100);

-- page_version_inputs (N117), INSERT-ONLY: what the page writer was shown for each version. The derivation depth is
-- fixed at two (fact -> observation -> page), so a page version is hidden by two EXISTS (a fact input of its segment is
-- tombstoned/hidden, or an observation-version input of its segment is hidden). source_version is 0 for facts.
-- document_id and document_version are the fact's (NULL for observation inputs); no FK to the source because it is
-- polymorphic and evidence outlives its facts (N135), so DerivedPurge and the retire purge delete by page version.
CREATE TABLE page_version_inputs (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  page_id         uuid NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  kind            text NOT NULL CHECK (kind IN ('fact', 'observation')),
  source_id       uuid NOT NULL,                      -- memory_id | observation_id
  source_version  integer NOT NULL DEFAULT 0 CHECK (source_version >= 0),
  document_id     text,
  document_version integer CHECK (document_version >= 1),
  created_at      timestamptz NOT NULL DEFAULT now(),
  ins_seq         bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, page_id, version, kind, source_id, source_version),
  FOREIGN KEY (namespace_id, page_id, version) REFERENCES page_versions (namespace_id, page_id, version)
      ON DELETE CASCADE,
  CHECK ((kind = 'fact') = (document_id IS NOT NULL)),
  CHECK ((kind = 'fact') = (document_version IS NOT NULL)),
  CHECK ((kind = 'fact') = (source_version = 0))
) WITH (fillfactor = 100);

CREATE INDEX page_version_inputs_document_idx ON page_version_inputs (namespace_id, document_id, document_version)
    WHERE document_id IS NOT NULL;
CREATE INDEX page_version_inputs_source_idx   ON page_version_inputs (namespace_id, kind, source_id);

-- page_sources: MUTABLE working set of the current page's sources (rewritten by PageRefresh)
CREATE TABLE page_sources (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  page_id        uuid NOT NULL,
  source_kind    text NOT NULL CHECK (source_kind IN ('fact', 'observation')),
  source_id      uuid NOT NULL,                      -- memory_id or observation_id (polymorphic: no FK)
  version_added  integer NOT NULL CHECK (version_added >= 1),
  PRIMARY KEY (namespace_id, page_id, source_kind, source_id),
  FOREIGN KEY (namespace_id, page_id) REFERENCES pages (namespace_id, page_id) ON DELETE CASCADE
) WITH (fillfactor = 70);

CREATE INDEX page_sources_source_idx ON page_sources (namespace_id, source_id);

-- -----------------------------------------------------------------------------
-- Deletion and invalidation markers (N115): the ONLY synchronous writes of a delete or an invalidation. Tiny,
-- namespace-keyed, loaded into the recall request three selects at a time (N116).
-- -----------------------------------------------------------------------------

-- document_tombstones: one row per delete of a document, covering (document_id, up_to_version) (N133c): it hides
-- exactly the rows with document_version <= up_to_version, i.e. every version the document had when it was deleted
-- (up_to_version = max(document_versions.version) read under the document lock, in-flight versions included). The same
-- document_id can be retained again at once: the new versions start at up_to_version + 1 and stay visible while the
-- covered rows are hidden and then purged. up_to_version is part of the key because a re-used id can be deleted again
-- while the first tombstone still exists. expunge_state is the Expunge's progress (pending -> materialized -> purged,
-- N119); the row is deleted 24 h after 'purged'. Recall passes the rows in 'pending' and 'materialized' (purged ones
-- have no rows left to hide). intent_key names the blob-storage intent object (deterministic from deleted_at and
-- operation_id): it is put AFTER this row commits and before the ack (N122). event_seq is the seq of the
-- DocumentDeleted event written in the same transaction: the tombstone is inserted with event_seq NULL and the FINAL
-- statement is WITH s AS (INSERT INTO outbox ... RETURNING seq) UPDATE document_tombstones SET event_seq = s.seq, so
-- the seq is drawn (N157/A-10) inside the last statement and the 60 s gap horizon holds. A committed row always has it;
-- the purge gate engram_consumers_passed(NULL) is false (P-15).
CREATE TABLE document_tombstones (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  document_id    text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  up_to_version  integer NOT NULL CHECK (up_to_version >= 1),
  deleted_at     timestamptz NOT NULL,
  operation_id   uuid,
  intent_key     text,
  event_seq      bigint,                                 -- NULL only inside the marker transaction (N157)
  expunge_state  text NOT NULL DEFAULT 'pending' CHECK (expunge_state IN ('pending', 'materialized', 'purged')),
  materialized_at timestamptz,
  purged_at      timestamptz,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, document_id, up_to_version),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((expunge_state <> 'pending') = (materialized_at IS NOT NULL)),
  CHECK ((expunge_state = 'purged') = (purged_at IS NOT NULL))
) WITH (fillfactor = 50);

CREATE INDEX document_tombstones_open_idx ON document_tombstones (namespace_id, deleted_at)
  WHERE expunge_state <> 'purged';

-- chunk_tombstones: REPLACE / re-extraction retired a chunk. Un-retire on a flap is a DELETE of the row. The expunge
-- purges the chunk (and deletes the row) after the 1 h grace (target CHUNK_TOMBSTONES). FK cascade: the purge of the
-- chunk removes its marker. The purge never removes evidence rows (N135).
CREATE TABLE chunk_tombstones (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  chunk_id      uuid NOT NULL,
  retired_at    timestamptz NOT NULL DEFAULT now(),
  reason        text NOT NULL CHECK (reason IN ('replace', 'reextract')),
  PRIMARY KEY (namespace_id, chunk_id),
  FOREIGN KEY (namespace_id, chunk_id) REFERENCES chunks (namespace_id, chunk_id) ON DELETE CASCADE
) WITH (fillfactor = 50);

CREATE INDEX chunk_tombstones_retired_idx ON chunk_tombstones (namespace_id, retired_at);

-- fact_hidden (N115, N135): keyed (memory_id, cause). Invalidate inserts cause 'invalidate'; Restore deletes ONLY that
-- row, so Restore is exact and cannot resurrect a stale extraction next to its re-extracted twin. cause 'reextract'
-- (N58) is written by FinalizeVersion for facts of a kept chunk whose extraction key is stale: the fact and chunk arms
-- hide a fact while ANY row exists, but derived-version predicates read cause = 'invalidate' only (re-extraction is a
-- write: dependents are flagged stale_write, not hidden), and the old-key facts are purged after 1 h (target
-- REEXTRACTED_FACTS), which bounds the marker sets. The 'invalidate' set is permanent by design and is tested by
-- primary-key anti-join, never passed as an array. reason is the caller's free text.
-- NO foreign key to facts (N145, C-2): an acknowledged invalidation outlives the fact row it names. The 'invalidate'
-- row dies only with the namespace or an explicit Restore; the 'reextract' row is deleted EXPLICITLY by the
-- REEXTRACTED_FACTS and CHUNK_TOMBSTONES purges of the facts they remove. materialized_at is the durable "Materialize
-- still owed" state of an invalidation: Materialize (and the per-shard purge-sweep) finds its work from the partial
-- index below and stamps it in the batch that wrote the derived_hidden(invalidation) rows, never from a signal payload.
-- N162: invalidation_op is the operation_id of the Invalidate marker that wrote the row, and a twin that re-extraction
-- adds LATER gets its row from the lazy curation_log re-application stamped with the SAME id. Restore(g) for any g of
-- the subject resolves I = fact_hidden(g).invalidation_op and deletes every row WHERE invalidation_op = I
-- (engram_restore_resolve): exact by construction, nothing is looked up by current hidden state. The row (and its
-- free-text reason) is deleted by the purge of its fact's deleted document (engram_purge_document_invalidations):
-- Restore of such a fact is NOT_FOUND{DOCUMENT_DELETED}.
CREATE TABLE fact_hidden (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  memory_id     uuid NOT NULL,
  cause         text NOT NULL DEFAULT 'invalidate' CHECK (cause IN ('invalidate', 'reextract')),
  hidden_at     timestamptz NOT NULL DEFAULT now(),
  reason        text NOT NULL DEFAULT '' CHECK (octet_length(reason) <= 1024),
  intent_key    text,
  -- N162: the Invalidate operation_id (a lazy twin carries the original's); NULL for 'reextract'
  invalidation_op uuid,
  -- N145: stamped by Materialize (engram_admin); NULL = derived hiding still owed
  materialized_at timestamptz,
  PRIMARY KEY (namespace_id, memory_id, cause),
  CHECK (cause = 'invalidate' OR materialized_at IS NULL),
  CHECK ((cause = 'invalidate') = (invalidation_op IS NOT NULL))
) WITH (fillfactor = 50);

-- N162: Restore deletes by tag
CREATE INDEX fact_hidden_op_idx ON fact_hidden (namespace_id, invalidation_op) WHERE cause = 'invalidate';

CREATE INDEX fact_hidden_unmaterialized_idx ON fact_hidden (namespace_id, hidden_at)
  -- the sweeper's and Materialize's worklist (N145)
  WHERE cause = 'invalidate' AND materialized_at IS NULL;

-- N159: the Materialize STAMP (fact_hidden.materialized_at, document_tombstones.materialized_at) is written only by a
-- transaction that holds the EXCLUSIVE derivation lock. A stamp written without it (an invalidation that nothing cites,
-- a batch that did not wait for the shared holders) lets a writer that verified before the Invalidate commit a version
-- citing the fact after the stamp, and the owed derived hiding is then never done (MaterializeComplete).
-- +goose StatementBegin
CREATE FUNCTION engram_materialize_stamp_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.materialized_at IS NULL AND NEW.materialized_at IS NOT NULL
     AND NOT engram_holds_derivation_exclusive(NEW.namespace_id) THEN
    RAISE EXCEPTION 'materialize stamp on %.% requires the exclusive derivation lock (N159)', TG_TABLE_NAME,
        NEW.namespace_id
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TRIGGER fact_hidden_stamp_guard BEFORE UPDATE OF materialized_at ON fact_hidden
  FOR EACH ROW EXECUTE FUNCTION engram_materialize_stamp_guard();
CREATE TRIGGER document_tombstones_stamp_guard BEFORE UPDATE OF materialized_at ON document_tombstones
  FOR EACH ROW EXECUTE FUNCTION engram_materialize_stamp_guard();

-- curation_log (A-16), INSERT-ONLY: every Invalidate / Restore with the fact's content_hash and document. CommitChunk
-- re-applies the LAST action per (document_id, content_hash) to the new facts of a re-extracted twin (of the document's
-- current life: document_version >= documents.life_start), so curation sticks. No FK: it must outlive the purge of the
-- old fact; the expunge deletes the rows of a deleted document explicitly (document_version <= up_to_version, N133c).
-- N162: (document_id, content_hash) IS the curation subject; operation_id is the marker's, and the lazy re-application
-- of the last 'invalidate' action stamps a re-extraction twin's fact_hidden row with THAT operation_id, so Restore
-- removes the twin with the rest of the set. A 'restore' row stops a later CommitChunk from re-applying the restored
-- invalidation.
CREATE TABLE curation_log (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  operation_id  uuid NOT NULL,
  memory_id     uuid NOT NULL,
  content_hash  bytea NOT NULL CHECK (octet_length(content_hash) = 32),
  document_id   text NOT NULL,
  document_version integer NOT NULL CHECK (document_version >= 1),   -- the fact's
  action        text NOT NULL CHECK (action IN ('invalidate', 'restore')),
  at            timestamptz NOT NULL DEFAULT now(),
  reason        text NOT NULL DEFAULT '' CHECK (octet_length(reason) <= 1024),
  ins_seq       bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, memory_id, at)
) WITH (fillfactor = 100);

-- the twin lookup filters document_version >= life_start
CREATE INDEX curation_log_twin_idx ON curation_log (namespace_id, document_id, content_hash, at DESC);

-- -----------------------------------------------------------------------------
-- Expunge stage tables (N119)
-- -----------------------------------------------------------------------------

-- derived_hidden: the MATERIALISATION of the read predicate, written by Expunge.Materialize under the exclusive
-- derivation lock (N120). One row says "versions of <kind> <id> in root segment root_version from from_version on are
-- hidden because of <cause>", where from_version = min(version) of the inputs of that segment that name the victim.
-- Rows with cause_kind 'document' are PERMANENT (an older version written with the victim in view must never resurface
-- at any as_of); rows with cause_kind 'invalidation' are deleted by Restore (under the exclusive derivation lock).
-- Removed only with the observation/page itself or the namespace. DerivedPurge (N136) leaves a stub for every version
-- such a row covers, and the row keeps covering it.
CREATE TABLE derived_hidden (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  kind          text NOT NULL CHECK (kind IN ('observation', 'page')),
  id            uuid NOT NULL,                       -- observation_id | page_id
  root_version  integer NOT NULL CHECK (root_version >= 1),
  from_version  integer NOT NULL,
  cause_kind    text NOT NULL CHECK (cause_kind IN ('document', 'invalidation')),
  -- document_id | memory_id::text (a document cause covers the tombstone's document_version <= up_to_version inputs
  -- only, N133c)
  cause_id      text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, kind, id, root_version, cause_kind, cause_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (from_version >= root_version)
) WITH (fillfactor = 70);

CREATE INDEX derived_hidden_cause_idx ON derived_hidden (namespace_id, cause_kind, cause_id);

-- expunge_progress: restartable purge position. unit = a document_id, or '*chunks' (replaced chunks past the 1 h
-- grace), '*reextract' (old-key facts past the grace), or '*namespace'; one row per (unit, table). The purge deletes in
-- batches of 1,000 ordered by primary key, PACED BY WAL (each batch's pg_current_wal_insert_lsn delta is measured; <=
-- 25 MB/s per shard, at most two expunges per shard, N119), and records last_key after each batch.
CREATE TABLE expunge_progress (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  unit          text NOT NULL,
  table_name    text NOT NULL,
  phase         text NOT NULL DEFAULT 'purge' CHECK (phase IN ('materialize', 'purge', 'derived_purge', 'hygiene',
                                                               'finish')),
  last_key      text,
  rows_purged   bigint NOT NULL DEFAULT 0 CHECK (rows_purged >= 0),
  done          boolean NOT NULL DEFAULT false,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, unit, table_name),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 50);

CREATE TRIGGER expunge_progress_touch BEFORE UPDATE ON expunge_progress
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- operations: async work visible through OperationService (D13: DEFERRED on quota exhaustion). workflow_started_at IS
-- NULL marks "no workflow yet" for the op-sweeper (N3). The CHECK lists below are the proto enums (OperationKind,
-- CancelReason) lower-cased and are generated from them (N139, A-8). MOVE_NAMESPACE lives on the catalog
-- namespace_moves row and DELETE_TENANT is derived in the catalog (N127, N133d), so neither has a row here. Workflow
-- mapping (N136): retain, export and refresh run at ns/{ns}/op/{op}; delete_document is the expunge singleton plus the
-- tombstone's operation_id; consolidate is the singleton. DELETE_* operations are not cancellable.
CREATE TABLE operations (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  operation_id         uuid NOT NULL,
  kind                 text NOT NULL CHECK (kind IN ('retain_document', 'delete_document', 'delete_namespace',
                                                     -- no 'reflect': Reflect is synchronous
                                                     'consolidate', 'refresh_page', 'create_snapshot')),
  state                operation_state NOT NULL DEFAULT 'PENDING',
  request_id           text CHECK (octet_length(request_id) <= 128),
  target_id            text,                           -- document_id / page_id / snapshot version, per kind
  -- N127/A-8: the newer document VERSION number (proto int64), not an operation id
  superseded_by        bigint CHECK (superseded_by >= 1),
  cancel_reason        text CHECK (cancel_reason IN ('client_request', 'document_deleted', 'namespace_deleting')),
  -- ns/{namespace_id}/op/{operation_id}, or the expunge/consolidate singleton (N136)
  workflow_id          text NOT NULL,
  -- shard-{shard_id} at submission; restarted on the target after a move
  task_queue           text NOT NULL,
  submitted_epoch      bigint NOT NULL CHECK (submitted_epoch >= 1),   -- audit only; never part of an identity (D11)
  workflow_started_at  timestamptz,
  -- written once per wave by the workflow (N69), never per chunk
  progress             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),
  deferred_until       timestamptz,
  deferred_reason      text,                           -- quota key, e.g. llm_tokens_per_day
  error                jsonb CHECK (error IS NULL OR jsonb_typeof(error) = 'object'),   -- google.rpc.Status as JSON
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  started_at           timestamptz,
  finished_at          timestamptz,
  PRIMARY KEY (namespace_id, operation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (state <> 'DEFERRED' OR deferred_until IS NOT NULL),
  CHECK ((state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')) = (finished_at IS NOT NULL))
) WITH (fillfactor = 70);

CREATE INDEX operations_created_idx ON operations (namespace_id, created_at DESC);
CREATE INDEX operations_active_idx  ON operations (namespace_id, state, deferred_until)
  WHERE state IN ('PENDING', 'RUNNING', 'DEFERRED');
-- shard-wide schedulers (documented exceptions to the namespace-leading rule; admin role):
CREATE INDEX operations_sweeper_idx  ON operations (created_at)     WHERE state = 'PENDING' AND workflow_started_at
    IS NULL;
CREATE INDEX operations_deferred_idx ON operations (deferred_until) WHERE state = 'DEFERRED';

CREATE TRIGGER operations_touch BEFORE UPDATE ON operations
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- idempotency_keys: request_id scoped to (tenant, namespace, method), 24 h (D1)
CREATE TABLE idempotency_keys (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  method         text NOT NULL,                       -- full gRPC method name
  request_id     text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 128),
  request_hash   bytea NOT NULL CHECK (octet_length(request_hash) = 32),
  operation_id   uuid,
  response       bytea,                               -- serialised response for replay
  created_at     timestamptz NOT NULL DEFAULT now(),
  expires_at     timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (namespace_id, method, request_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (response IS NULL OR octet_length(response) <= 65536)
);

-- shard-wide expiry sweep (documented exception to the namespace-leading rule; admin role)
CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys (expires_at);

-- token_usage_events: one row per gateway call, idempotent by usage_key (section 5 N5-1); 30-day retention.
-- token_usage: the daily aggregate the API reports (D13).
CREATE TABLE token_usage_events (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  -- sha256(operation_id || activity || item key)
  usage_key          bytea NOT NULL CHECK (octet_length(usage_key) = 32),
  day                date NOT NULL,
  op                 text NOT NULL CHECK (op IN ('extract', 'summarize', 'embed', 'consolidate', 'adjudicate',
                                                 'reflect', 'page', 'rerank')),
  model              text NOT NULL,
  price_version      text NOT NULL,                  -- cost is computed at write time with this price list (N20)
  operation_id       uuid,
  prompt_tokens      bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
  completion_tokens  bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
  cost_micros        bigint NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
  cached             boolean NOT NULL DEFAULT false,
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, usage_key),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 100);

CREATE INDEX token_usage_events_op_idx  ON token_usage_events (namespace_id, operation_id);
CREATE INDEX token_usage_events_day_idx ON token_usage_events (namespace_id, day);

CREATE TABLE token_usage (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  day                date NOT NULL,
  op                 text NOT NULL,
  model              text NOT NULL,
  price_version      text NOT NULL,
  calls              bigint NOT NULL DEFAULT 0 CHECK (calls >= 0),
  prompt_tokens      bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
  completion_tokens  bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
  cost_micros        bigint NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, day, op, model, price_version),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- quota_counters: per-namespace windows (llm_tokens_per_day, recalls/retains per minute for auditing the API's token
-- buckets); max_facts reads namespace_stats.live_facts.
CREATE TABLE quota_counters (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  quota_key      text NOT NULL CHECK (quota_key IN ('llm_tokens', 'recalls', 'retains')),
  window_start   timestamptz NOT NULL,
  used           bigint NOT NULL DEFAULT 0 CHECK (used >= 0),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, quota_key, window_start),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- blob_tombstones: asynchronous blob deletion queue (rows are deleted once the blob is gone)
CREATE TABLE blob_tombstones (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  tombstone_id   uuid NOT NULL,
  blob_key       text NOT NULL,                       -- key relative to {shard}/{tenant}/{namespace}/
  reason         text NOT NULL CHECK (reason IN ('document_delete', 'version_supersede', 'namespace_delete',
                                                  'move_cleanup', 'export_expire', 'page_retire', 'xcache_gc',
                                                  -- derived_purge: pages/{id}/v{n}.md of a stubbed page version;
                                                  -- transcript_purge: reflect/{op}.jsonl (N136)
                                                  'derived_purge', 'transcript_purge')),
  -- N100: the sweeper deletes only when not_before <= now(); xcache_gc rows carry now() + xcache_grace (24 h) and are
  -- re-checked against live chunks first
  not_before     timestamptz NOT NULL DEFAULT now(),
  operation_id   uuid,
  attempts       integer NOT NULL DEFAULT 0,
  last_error     text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, tombstone_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE INDEX blob_tombstones_created_idx ON blob_tombstones (namespace_id, not_before);

-- export_snapshots (D12, N126): one row per snapshot version, MUTABLE. BeginSnapshot inserts it as 'building' with
-- snapshot_started_at; the marker transaction of a delete expires 'building' and 'ready' rows alike; RecordSnapshot
-- refuses to promote an expired row and re-checks document_tombstones.deleted_at > snapshot_started_at. There is NO
-- outbox cut: a delta is the diff of two consecutive snapshots (base_version), always emitted with delete records
-- (deleted_ids in the manifest) even when the base expired. expires_at = now() hands the blobs to the tombstone
-- sweeper.
CREATE TABLE export_snapshots (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  version              integer NOT NULL CHECK (version >= 1),
  state                text NOT NULL DEFAULT 'building' CHECK (state IN ('building', 'ready', 'expired', 'failed')),
  expired_reason       text CHECK (expired_reason IN ('ttl', 'document_delete', 'namespace_delete')),
  manifest_key         text NOT NULL,                  -- export/v{version}/manifest.json
  base_version         integer CHECK (base_version IS NULL OR base_version < version),   -- delta-v{base}-v{version}
  snapshot_started_at  timestamptz NOT NULL DEFAULT now(),
  fact_count           bigint NOT NULL DEFAULT 0,
  observation_count    bigint NOT NULL DEFAULT 0,
  chunk_count          bigint NOT NULL DEFAULT 0,
  page_count           bigint NOT NULL DEFAULT 0,
  bytes                bigint NOT NULL DEFAULT 0,
  operation_id         uuid,
  created_at           timestamptz NOT NULL DEFAULT now(),
  completed_at         timestamptz,
  expires_at           timestamptz,
  PRIMARY KEY (namespace_id, version),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((state = 'expired') = (expired_reason IS NOT NULL))
) WITH (fillfactor = 70);

-- Predicate of the RESTRICTIVE move policies below: is the namespace in scope the target of a move right now? Evaluated
-- once per statement as an InitPlan.
-- +goose StatementBegin
CREATE FUNCTION engram_ns_is_incoming() RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM namespace_ownership o
                  WHERE o.namespace_id = current_setting('engram.namespace_id')::uuid AND o.state = 'incoming');
$$;
-- +goose StatementEnd

RESET ROLE;

-- +goose Down
-- Honest Down: drops every object of this schema (all shard data with it), so it REFUSES when any namespace or shard
-- identity row exists; data-losing rollbacks are done by restore (section 9.3). Roles and extensions are cluster and
-- database prerequisites other databases may share, so they stay.
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  IF EXISTS (SELECT 1 FROM shard_meta) OR EXISTS (SELECT 1 FROM namespace_ownership) THEN
    RAISE EXCEPTION '0001 Down refused: shard_meta or namespace_ownership holds rows (restore from backup instead)'
      USING ERRCODE = '55006';
  END IF;
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'v'
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
  LOOP
    EXECUTE format('DROP VIEW %s CASCADE', r.rel);
  END LOOP;
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
              AND c.relname <> 'goose_db_version'
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
  LOOP
    EXECUTE format('DROP TABLE %s CASCADE', r.rel);
  END LOOP;
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'S'
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i'))
  LOOP
    EXECUTE format('DROP SEQUENCE %s CASCADE', r.rel);
  END LOOP;
  FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p
            WHERE p.pronamespace = 'public'::regnamespace
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')
  LOOP
    EXECUTE format('DROP FUNCTION %s CASCADE', r.sig);
  END LOOP;
  FOR r IN SELECT t.oid::regtype AS typ FROM pg_type t
            WHERE t.typnamespace = 'public'::regnamespace AND t.typtype = 'e'
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e')
  LOOP
    EXECUTE format('DROP TYPE %s CASCADE', r.typ);
  END LOOP;
END $$;
-- +goose StatementEnd
