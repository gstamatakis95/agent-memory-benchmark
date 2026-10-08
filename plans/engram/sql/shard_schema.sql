-- =============================================================================
-- Engram shard schema (identical on every shard)
-- Target: PostgreSQL 16, image paradedb/paradedb:latest-pg16
--   pgvector >= 0.8 (halfvec, hnsw.iterative_scan), pg_search >= 0.25 (USING bm25 is the
--   backwards-compatible alias of USING paradedb; pdb.score / ||| / &&& / === operators),
--   pg_trgm, btree_gin, btree_gist. pg_search needs shared_preload_libraries = 'pg_search'.
-- Plain SQL (no goose annotations; section 9 wraps it as migrations/shard/0001_init.sql).
-- Everything lives in schema public, which is what the section 8 RLS checks and the
-- section 9 tooling (goose_db_version, shard_meta) assume. Apply as engram_migrate (owner).
--
-- Design rule (D22, D23; N113): rows that carry vectors or BM25 text are IMMUTABLE. Content tables
-- (facts, chunks, observation_versions, page_versions, fact_links, entity_mentions,
-- observation_inputs, observation_version_sources, page_version_inputs, the *_vectors side
-- tables) are insert-only and purge-only: no UPDATE path, no visibility column, no generated
-- column, fillfactor 100; the only DML after the insert is a DELETE by the expunge (engram_admin)
-- and the insert of a content-free stub for a purged derived version (N136).
-- Evidence outlives the facts it names (N135): observation_inputs, observation_version_sources and
-- page_version_inputs have NO foreign key to facts; they die with their own version row only.
-- Visibility is a READ-TIME predicate over small marker tables (document_tombstones,
-- chunk_tombstones, fact_hidden, derived_hidden; N115-N117), never a flag stamped at delete time.
-- A document tombstone covers (document_id, up_to_version) (N133c): every row that carries a
-- document_id also carries the document_version that inserted it, and the predicate hides only
-- document_version <= up_to_version, so a deleted document_id can be re-used at once.
-- Mutable state lives in narrow tables (fillfactor 50-70, HOT): documents, observations, pages,
-- markers, namespace_stats, vector_indexes.
--
-- Table classes (N113, N137): every table carries one COMMENT ON TABLE tag 'class: ...', set from one
-- list in the "Table classes" block below and checked by the self-check and by `engramlint sql`;
-- N113, N124 and section 3 render from it, no hand-written class list exists.
--   insert-only   re-copied by a move through ins_seq (nextval('engram_ins_seq') at INSERT, index
--                 (namespace_id, ins_seq)); only the expunge deletes from it, paused from Plan to done
--   mutable       merge-diffed by a move: (pk, md5(row minus updated_at))
--   expiring      excluded from the move and re-derived on the target (token_usage_events is copied
--                 once and verified count <=)
--   shard-local   not namespace data: identity, outbox, the sequence ring
-- ins_seq is commit-ordered only within the writer lifetime, so engram_seq_log (sampled every minute)
-- and engram_seq_floor(ts) give the key lower bound a move re-copies from (the analogue of a UUIDv7 floor).
--
-- Shard identity: shard_meta holds exactly one row (shard_id, schema_version). Provisioning
-- inserts it; the store verifies it against the catalog at pool open (section 2); the
-- namespace_ownership trigger refuses rows whose shard_id disagrees with it. No other table
-- carries a shard_id: inside a shard database the shard is implied by the database.
--
-- Per-transaction scope (set by the store with set_config(name, value, true) = SET LOCAL):
--   engram.namespace_id  uuid    engram.tenant_id  text    engram.epoch  bigint
-- RLS policies read engram.namespace_id: unset -> ERROR 42704, empty -> ERROR 22P02
-- (fail closed, verified in section 8's RLS canary).
--
-- Write-transaction protocol (D6 invariant A-F1, model-checked in section 7): every role that writes
-- the outbox (engram_app, engram_move, engram_admin; engramctl raises the timeouts per session only)
-- runs with statement_timeout = idle_in_transaction_session_timeout = 30 s and the outbox INSERT is
-- the LAST statement before COMMIT, so a drawn outbox.seq is committed or aborted within one
-- timeout of being drawn; the relay's 60 s gap watchlist relies on it. Every role runs
-- synchronous_commit = local (N122): a commit never waits for a standby, so the invariant holds.
--
-- Roles (LOGIN; passwords are set by provisioning from the shard secret, section 9):
--   engram_migrate  owner of every object; DDL only (BYPASSRLS so data migrations and the
--                   SECURITY DEFINER functions engram_cleanup_namespace / engram_entity_fuzzy run).
--                   The index runner (`engramctl index`, control host, N138) is the ONE process that
--                   connects as this role at run time: index DDL has one owner
--   engram_app      API + worker per-namespace transactions; NOBYPASSRLS; SELECT/INSERT on content,
--                   full DML on mutable tables, no UPDATE on markers, NO UPDATE or DELETE on content
--                   rows and none on consolidation_proposals (a discard is a new attempt, N43)
--   engram_relay    outbox relay; NOBYPASSRLS; one extra policy grants all-namespace SELECT on
--                   outbox only (D6/N4); owns outbox_cursors; reads nothing else
--   engram_move     move executor (N91, N124); NOBYPASSRLS, 30 s statement/idle timeouts, confined by ns_isolation to the
--                   namespace in scope. On the TARGET it writes through the RESTRICTIVE policies
--                   move_target_* (a write passes only while the ownership row is 'incoming'):
--                   INSERT on content tables, full DML on mutable tables (the reconcile merges and
--                   deletes). On the SOURCE it has SELECT and the ownership transitions of the state
--                   machine (freeze, cutover (c)); it never DELETEs source data (cleanup runs through
--                   engram_cleanup_namespace). Bulk load: COPY each range into a session TEMP table,
--                   then INSERT ... SELECT <cols> ON CONFLICT under RLS with SET LOCAL
--                   session_replication_role = replica (GRANT SET ON PARAMETER, PG 15+).
--                   <cols> comes from engram_copy_columns(), which skips generated and dropped
--                   columns (P-8); Plan refuses unless engram_column_hash() matches on both shards.
--   engram_admin    engramctl, Expunge purge activities, shard-wide schedulers; BYPASSRLS; the ONLY
--                   role that DELETEs content rows (purge-only discipline)
--
-- Ownership fence (D2 row 3, N82, N113, N125; the state machine is DATA here: the table
-- ownership_transitions, section 3.3.1):
--   the namespace_ownership ROW is the fence VALUE, a heavyweight advisory lock is the fence
--   LOCK. Writers take pg_try_advisory_xact_lock_shared(engram_ns_fence_key(ns)); a refused
--   try-lock ends the statement at once with the retryable NamespaceFrozen{retry_after 200 ms}.
--   Then a plain SELECT state, epoch FROM namespace_ownership (no row lock); proceed only when
--   state = 'active' AND epoch = $epoch. Exclusive takers (freeze, delete freeze, restore) make ONE
--   attempt with lock_timeout = 35 s, longer than any legal 30 s writer. Marker transactions
--   (delete, invalidate, restore) take no lock beyond the shared fence. Readers take no lock and
--   accept 'active' and 'frozen'/'move' only. Three disjoint lock key spaces (N113):
--     namespace fence      one-argument  pg_*_advisory_*lock(hashtextextended(ns::text, 0))
--     derivation lock      one-argument  pg_*_advisory_*lock(hashtextextended(ns::text, 1))   (N120)
--     document lock        two-argument  pg_*_advisory_*lock(hashtext(ns::text), hashtext(doc))
--   One- and two-argument advisory locks are different lock tags (objsubid 1 vs 2), so no
--   cross-kind collision exists. FOR SHARE is used on no row (immutable facts cannot be locked
--   meaningfully; compatible row lockers churn multixacts).
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS vector;
-- pg_search:begin
CREATE EXTENSION IF NOT EXISTS pg_search;
-- pg_search:end
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS btree_gist;

SET search_path = public;

DO $$
BEGIN
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
END $$;

-- Role defaults (N82, N122). Row and advisory waits end after lock_timeout; exclusive takers raise it
-- to 35 s with SET LOCAL for their single attempt. statement_timeout stays the outer bound.
-- synchronous_commit = local on every role: no synchronous standby exists (N122).
ALTER ROLE engram_app   SET lock_timeout = '2s';
ALTER ROLE engram_app   SET statement_timeout = '30s';
ALTER ROLE engram_app   SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_app   SET synchronous_commit = 'local';
ALTER ROLE engram_move  SET lock_timeout = '10s';
ALTER ROLE engram_move  SET statement_timeout = '30s';                     -- N133e/C-19: every outbox-writing role is bounded
ALTER ROLE engram_move  SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_move  SET synchronous_commit = 'local';
ALTER ROLE engram_admin SET lock_timeout = '10s';
ALTER ROLE engram_admin SET statement_timeout = '30s';
ALTER ROLE engram_admin SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_admin SET synchronous_commit = 'local';
ALTER ROLE engram_relay SET synchronous_commit = 'local';

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

-- The smallest UUIDv7 at or after a timestamp: a key lower bound for ops scripts and tests. No schema
-- function and no move uses it any more: the move re-copies by ins_seq (N137) and the consolidation
-- watermark is bounded by engram_seq_floor. Byte order of a uuid = timestamp, version nibble 7, variant bits 10.
CREATE FUNCTION engram_uuid_v7_floor(ts timestamptz) RETURNS uuid
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT encode(substring(int8send((extract(epoch FROM ts) * 1000)::bigint) FROM 3)
                || '\x7000'::bytea || '\x8000000000000000'::bytea, 'hex')::uuid;
$$;

-- Insertion sequence of the insert-only class (N137). nextval is drawn at INSERT, not at commit, so
-- ins_seq is commit-ordered only within the writer lifetime (30 s statements, 30 s idle gaps).
CREATE SEQUENCE engram_ins_seq AS bigint CACHE 1;

-- A stored consolidation proposal records the version each update/merge op was rendered from (N121):
-- ApplyBatch applies an op only while the observation is still at its base_version (N120).
CREATE FUNCTION engram_ops_have_base(ops jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NOT EXISTS (SELECT 1 FROM jsonb_array_elements(ops) AS e
                      WHERE e ->> 'kind' IN ('update', 'merge') AND jsonb_typeof(e -> 'base_version') IS DISTINCT FROM 'number');
$$;

-- Advisory-lock keys (N113): three disjoint key spaces. IMMUTABLE so calls are inlined. A hash
-- collision only over-serialises two namespaces or documents, never under-fences one.
CREATE FUNCTION engram_ns_fence_key(ns uuid) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtextextended(ns::text, 0) $$;

CREATE FUNCTION engram_ns_derivation_key(ns uuid) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtextextended(ns::text, 1) $$;

CREATE FUNCTION engram_doc_lock_keys(ns uuid, doc text) RETURNS TABLE (k1 integer, k2 integer)
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT hashtext(ns::text), hashtext(doc) $$;

-- Writers' fence acquisition (N82): never waits. false -> NamespaceFrozen{retry_after = 200 ms}.
CREATE FUNCTION engram_try_ns_fence(ns uuid) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$ SELECT pg_try_advisory_xact_lock_shared(engram_ns_fence_key(ns)) $$;

-- Exclusive takers (freeze, delete freeze, restore): ONE attempt, 35 s (N82). Raises 55P03 on timeout.
CREATE FUNCTION engram_ns_fence_exclusive(ns uuid) RETURNS void
LANGUAGE plpgsql VOLATILE STRICT AS $$
BEGIN
  PERFORM set_config('lock_timeout', '35s', true);
  PERFORM pg_advisory_xact_lock(engram_ns_fence_key(ns));
END $$;

-- Derivation lock (N120): writers of derived versions (ApplyBatch stage 2, PageRefresh commit)
-- try-lock it shared and retry on refusal; Expunge.Materialize takes it exclusive, one 35 s attempt.
CREATE FUNCTION engram_try_derivation_lock(ns uuid) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$ SELECT pg_try_advisory_xact_lock_shared(engram_ns_derivation_key(ns)) $$;

CREATE FUNCTION engram_derivation_lock_exclusive(ns uuid) RETURNS void
LANGUAGE plpgsql VOLATILE STRICT AS $$
BEGIN
  PERFORM set_config('lock_timeout', '35s', true);
  PERFORM pg_advisory_xact_lock(engram_ns_derivation_key(ns));
END $$;

-- CommitChunk's per-document lock (N83): shared try-lock; false -> retryable DocumentBusy (100 ms).
CREATE FUNCTION engram_try_doc_lock_shared(ns uuid, doc text) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$
  SELECT pg_try_advisory_xact_lock_shared(k.k1, k.k2) FROM engram_doc_lock_keys(ns, doc) AS k;
$$;

-- Tag grammar: <= 32 tags, each ^[a-z0-9][a-z0-9._:/-]{0,63}$, strictly ascending in "C"
-- collation (= sorted and deduplicated bytewise, exactly what the Go normaliser emits).
-- Tags live on documents (and observations' consolidation scope) only: the recall layer resolves
-- the tag mode ONCE against documents.tags into an allowed-document set; no tag predicate runs
-- under RLS (N116, P-5).
CREATE FUNCTION engram_tags_valid(t text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT cardinality(t) <= 32
     AND NOT EXISTS (SELECT 1 FROM unnest(t) AS x WHERE x !~ '^[a-z0-9][a-z0-9._:/-]{0,63}$')
     AND coalesce((SELECT bool_and(t[i] COLLATE "C" < t[i + 1] COLLATE "C")
                     FROM generate_series(1, cardinality(t) - 1) AS i), true);
$$;

-- SQL twin of the Lean-verified tag decision procedure (section 7). Q = query tags, I = item tags.
--   ANY: I = {} or I ∩ Q ≠ {}   ANY_STRICT: I ∩ Q ≠ {}   ALL: I = {} or Q ⊆ I
--   ALL_STRICT: Q ⊆ I and I ≠ {}   EXACT: I = Q      unset mode (NULL/'') matches everything.
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

-- jsonb form used by pages.tag_filter: {"mode": "ALL", "tags": ["a","b"]}
CREATE FUNCTION engram_tag_match(filter jsonb, i text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT engram_tag_match(filter ->> 'mode',
                          coalesce((SELECT array_agg(x ORDER BY x COLLATE "C")
                                      FROM jsonb_array_elements_text(filter -> 'tags') AS x), '{}'::text[]),
                          i);
$$;

-- Mutable tables only (N113): stamps updated_at on every UPDATE. The move loader runs in
-- session_replication_role = replica, where this trigger does not fire, so copied rows keep the
-- source's updated_at.
CREATE FUNCTION engram_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- Insert-only content tables (N113): UPDATE is refused for every role. The grants already give
-- engram_app and engram_move no UPDATE privilege on them; this trigger is the second wall (it also
-- stops the owner and engram_admin). DELETE stays possible for the purge only (grants).
CREATE FUNCTION engram_forbid_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is insert-only (UPDATE by % refused)', TG_TABLE_NAME, current_user
    USING ERRCODE = '42501';
END $$;

-- ingest_ledger is append-only. UPDATE is never allowed. DELETE is allowed only to engram_admin
-- (explicit document, namespace or tenant delete expunge; never the retire of superseded
-- versions, N104) and to the table owner, which is the SECURITY DEFINER cleanup function
-- engram_cleanup_namespace after a move. Every other role gets 42501.
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

-- namespace_ownership rows must carry this database's shard id, epochs never decrease, and only
-- the edges of ownership_transitions are accepted (N101, N125):
--   * freeze_reason, epoch, move and target columns change only in an explicit edge, each with its role;
--   * moved_out and active rows are never deleted by an ordinary role (the fence value a late writer
--     or stale reader must still hit, WrongShardOrEpoch{MOVED_OUT}); cleanup never removes them;
--   * moved_out -> incoming additionally requires that no data rows remain (the cleanup ran).
-- Everything else is 23514 / 42501. The trigger is ENABLE ALWAYS (N91): session_replication_role =
-- replica, which the move loader sets, must not switch it off.
CREATE FUNCTION engram_check_ownership() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  v_shard integer;
  t       record;
  v_ok    boolean := false;
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
      RAISE EXCEPTION 'ownership transition %(%)/% -> %(%)/% by % violates the epoch or move/target rules of its edge (namespace %)',
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

  IF t.edge = 'return_move' AND (EXISTS (SELECT 1 FROM ingest_ledger l WHERE l.namespace_id = NEW.namespace_id)
                                 OR EXISTS (SELECT 1 FROM documents d WHERE d.namespace_id = NEW.namespace_id)
                                 OR EXISTS (SELECT 1 FROM facts f WHERE f.namespace_id = NEW.namespace_id)) THEN
    RAISE EXCEPTION 'namespace % still has data rows on this shard; run engram_cleanup_namespace first', NEW.namespace_id
      USING ERRCODE = '55006';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- shard_meta holds exactly one row.
CREATE FUNCTION engram_shard_meta_singleton() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT count(*) FROM shard_meta) > 0 THEN
    RAISE EXCEPTION 'shard_meta already has a row; UPDATE it' USING ERRCODE = '23505';
  END IF;
  RETURN NEW;
END $$;

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
  -- no replay floor here (N134): it lives in catalog.shards.replay_floor, outside the restorable
  -- shard state, so neither a PITR nor a stale promotion can lose or raise it
);

CREATE TRIGGER shard_meta_singleton BEFORE INSERT ON shard_meta
  FOR EACH ROW EXECUTE FUNCTION engram_shard_meta_singleton();

-- outbox_cursors: one row per consumer plus the 'relay' row, whose gaps column is the persisted
-- gap watchlist (D6). A seq missing for more than 1 s at the relay's read point is recorded as
-- {"seq": n, "deadline": ts}; it is removed when the row appears or when the deadline
-- (first_seen + 2 x statement_timeout) passes. The relay stops advancing when the list holds
-- 1000 entries. Cursor advances are batched to 1/s per consumer (XID budget, N114). Moves never
-- read or write this table (N124).
CREATE TABLE outbox_cursors (
  consumer    text PRIMARY KEY CHECK (consumer ~ '^(relay|index|kafka)$'),
  last_seq    bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  gaps        jsonb NOT NULL DEFAULT '[]'::jsonb
              CHECK (jsonb_typeof(gaps) = 'array' AND jsonb_array_length(gaps) <= 1000),
  updated_at  timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);

-- engram_seq_log (N137): the ring behind engram_seq_floor. engram_seq_sample() inserts
-- (now(), nextval('engram_ins_seq')) once a minute (admin scheduler) and trims samples older than
-- 7 days, longer than any copy. Shard-local: a move never copies it.
CREATE TABLE engram_seq_log (
  sampled_at  timestamptz PRIMARY KEY,
  seq         bigint NOT NULL
);

CREATE FUNCTION engram_seq_sample() RETURNS bigint
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  v bigint := nextval('engram_ins_seq');
BEGIN
  INSERT INTO engram_seq_log (sampled_at, seq) VALUES (clock_timestamp(), v);
  DELETE FROM engram_seq_log WHERE sampled_at < now() - interval '7 days';
  RETURN v;
END $$;

-- The lower bound on ins_seq for "rows inserted since ts": the sample at or before ts - 10 min, which
-- exceeds the longest time between drawing an ins_seq and committing the row. 0 on a new shard
-- (everything is "recent"). A move re-copies insert-only rows WHERE ins_seq >= engram_seq_floor(T).
CREATE FUNCTION engram_seq_floor(ts timestamptz) RETURNS bigint
LANGUAGE sql STABLE AS $$
  SELECT coalesce((SELECT l.seq FROM engram_seq_log l
                    WHERE l.sampled_at <= ts - interval '10 minutes'
                    ORDER BY l.sampled_at DESC LIMIT 1), 0);
$$;

-- =============================================================================
-- Namespace-keyed tables (the big ones are partitioned further down)
-- =============================================================================

-- namespace_ownership: the fence VALUE (D2 row 4, D5; the fence LOCK is the advisory lock
-- engram_ns_fence_key, see the header). Mirrors the catalog for the namespaces this shard hosts.
-- The ONLY table with a shard_id column. The state machine (states x roles x statements) is the
-- data table ownership_transitions, enforced by engram_check_ownership (N101, N125).
-- State 'ready' (N125): the target after the bulk copy and reconcile, before the point of no
-- return; nothing routes to it and callers get the retryable NamespaceNotReady.
-- move_id / move_epoch: set on the SOURCE by start_move (move_epoch = the target epoch e + 1) and
-- cleared on abort/thaw; Expunge and the schedulers skip a namespace while move_epoch IS NOT NULL
-- (views schedulable_namespaces / purgeable_namespaces). On the target move_id is set by the
-- incoming row. target_shard_id / target_epoch: carried by a moved_out row so the API can route
-- without the catalog; a moved_out row is a PERMANENT fence value and is never deleted. After a
-- return_abort the row's epoch may exceed target_epoch, so no CHECK relates the two.
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
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),                -- FK target for the tenant_id denormalisation
  CHECK (state NOT IN ('incoming', 'ready') OR move_id IS NOT NULL),
  CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL)),   -- a thaw and cutover (c) clear the reason
  CHECK ((state = 'moved_out') = (target_shard_id IS NOT NULL)),
  CHECK ((target_shard_id IS NULL) = (target_epoch IS NULL)),
  CHECK (target_shard_id IS NULL OR target_shard_id <> shard_id),
  CHECK (move_epoch IS NULL OR move_id IS NOT NULL)
);

-- The ownership state machine as DATA (N101, N125, section 3.3.1): one row per (edge, role, from
-- state). The trigger engram_check_ownership accepts an INSERT, UPDATE or DELETE of a
-- namespace_ownership row only if it matches a row here for current_user, and then checks the
-- epoch rule and the move/target column effects of that edge. Any (state pair, role) not listed is
-- refused: that is the whole negative test surface of TestIso_Ownership_Transitions. NULL
-- from_state = INSERT, NULL to_state = DELETE.
--   epoch_rule: one = epoch 1, any, same, plus1 = exactly +1, greater = strictly greater (restore
--     and failover use the catalog epoch + 1, written to the catalog first).
--   move_effect on (move_id, move_epoch): none = unchanged, open = start_move (move_id set from
--     NULL, move_epoch = epoch + 1), close = both NULL, retarget = a NEW move_id and move_epoch NULL
--     (moved_out -> incoming).
--   target_effect on (target_shard_id, target_epoch): none, set = cutover (c) / reconcile_out
--     (target shard differs, target_epoch = epoch + 1), clear, restore = return_abort (the hint of
--     the permanent fence value comes back from namespace_moves).
-- Rollback edges before the point of no return: abort_move (before freeze), thaw_move (after
-- freeze), unready_target (after (b')), return_abort (onto a shard that had a moved_out row).
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

INSERT INTO ownership_transitions (edge, role_name, from_state, from_reason, to_state, to_reason, epoch_rule, move_effect, target_effect, note) VALUES
  ('create',          'engram_app',   NULL,       NULL,      'active',    NULL,      'one',     'none',     'none',  'CreateNamespace: first row, epoch 1'),
  ('create',          'engram_admin', NULL,       NULL,      'active',    NULL,      'one',     'none',     'none',  'provisioning and tests'),
  ('plan_target',     'engram_move',  NULL,       NULL,      'incoming',  NULL,      'any',     'none',     'none',  'move plan on the target; move_id mandatory (CHECK)'),
  ('start_move',      'engram_move',  'active',   NULL,      'active',    NULL,      'same',    'open',     'none',  'Plan on the source: sets move_id and move_epoch, pauses Expunge and schedulers'),
  ('abort_move',      'engram_move',  'active',   NULL,      'active',    NULL,      'same',    'close',    'none',  'rollback before the freeze'),
  ('abort_move',      'engram_admin', 'active',   NULL,      'active',    NULL,      'same',    'close',    'none',  'restore/failover reconcile (N123)'),
  ('freeze_move',     'engram_move',  'active',   NULL,      'frozen',    'move',    'same',    'none',     'none',  'D5 freeze: reads continue'),
  ('freeze_delete',   'engram_app',   'active',   NULL,      'frozen',    'delete',  'same',    'none',     'none',  'namespace delete, before the ack (N122); no outgoing edge except deletion'),
  ('freeze_restore',  'engram_admin', 'active',   NULL,      'frozen',    'restore', 'same',    'none',     'none',  'restore or failover (N123)'),
  ('restore_delete',  'engram_admin', 'frozen',   'restore', 'frozen',    'delete',  'same',    'none',     'none',  'replay of a namespace/tenant delete intent onto a restored shard (N122, C-13); no pass through active'),
  ('thaw_move',       'engram_move',  'frozen',   'move',    'active',    NULL,      'same',    'close',    'none',  'rollback after the freeze'),
  ('thaw_move',       'engram_admin', 'frozen',   'move',    'active',    NULL,      'same',    'close',    'none',  'restore/failover reconcile: the move is rolled back (N123)'),
  ('ready_target',    'engram_move',  'incoming', NULL,      'ready',     NULL,      'same',    'none',     'none',  'cutover (b''): nothing routes to ready'),
  ('unready_target',  'engram_move',  'ready',    NULL,      'incoming',  NULL,      'same',    'none',     'none',  'rollback after (b'')'),
  ('cutover_c',       'engram_move',  'frozen',   'move',    'moved_out', NULL,      'same',    'none',     'set',   'cutover (c), the point of no return; clears freeze_reason'),
  ('activate_target', 'engram_move',  'ready',    NULL,      'active',    NULL,      'same',    'close',    'none',  'cutover (b''''): the row already carries e + 1'),
  ('return_move',     'engram_move',  'moved_out', NULL,     'incoming',  NULL,      'greater', 'retarget', 'clear', 'a later move back to this shard; data rows must be gone'),
  ('return_abort',    'engram_move',  'incoming', NULL,      'moved_out', NULL,      'any',     'close',    'restore', 'rollback of a move back: the permanent fence value returns (H-22)'),
  ('reconcile_out',   'engram_admin', 'active',   NULL,      'moved_out', NULL,      'same',    'close',    'set',   'restore/failover: (c) was done, the restored source row becomes moved_out(target, e+1)'),
  ('reconcile_out',   'engram_admin', 'frozen',   'move',    'moved_out', NULL,      'same',    'close',    'set',   'same, from a restored frozen/move row'),
  ('reconcile_out',   'engram_admin', 'frozen',   'restore', 'moved_out', NULL,      'same',    'close',    'set',   'same, from a restored frozen/restore row'),
  ('epoch_bump',      'engram_admin', 'active',   NULL,      'active',    NULL,      'greater', 'none',     'none',  'failover bump: new epoch = catalog epoch + 1'),
  ('restore_done',    'engram_admin', 'frozen',   'restore', 'active',    NULL,      'greater', 'none',     'none',  'restore completion after intent replay: catalog epoch + 1'),
  ('rollback_target', 'engram_move',  'incoming', NULL,      NULL,        NULL,      'any',     'none',     'none',  'DELETE of the target row on rollback'),
  ('rollback_target', 'engram_admin', 'incoming', NULL,      NULL,        NULL,      'any',     'none',     'none',  'operator cleanup'),
  ('rollback_target', 'engram_admin', 'ready',    NULL,      NULL,        NULL,      'any',     'none',     'none',  'operator cleanup after a restore of the target'),
  ('purge_deleted',   'engram_admin', 'frozen',   'delete',  NULL,        NULL,      'any',     'none',     'none',  'DELETE after NamespacePurged');

-- The machine is immutable once loaded (a change is a migration that drops this trigger first).
CREATE FUNCTION engram_forbid_transition_edit() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ownership_transitions is immutable (% refused)', TG_OP USING ERRCODE = '42501';
END $$;

CREATE TRIGGER ownership_transitions_immutable BEFORE INSERT OR UPDATE OR DELETE ON ownership_transitions
  FOR EACH STATEMENT EXECUTE FUNCTION engram_forbid_transition_edit();

-- Epoch rule and column effects of one candidate edge for an UPDATE (o = OLD, n = NEW); the three
-- rule names are the columns of the matched ownership_transitions row.
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
           WHEN 'none'    THEN (n.target_shard_id, n.target_epoch) IS NOT DISTINCT FROM (o.target_shard_id, o.target_epoch)
           WHEN 'set'     THEN n.target_shard_id IS NOT NULL AND n.target_shard_id <> n.shard_id AND n.target_epoch = o.epoch + 1
           WHEN 'clear'   THEN n.target_shard_id IS NULL AND n.target_epoch IS NULL
           WHEN 'restore' THEN o.target_shard_id IS NULL AND n.target_shard_id IS NOT NULL
                               AND n.target_shard_id <> n.shard_id AND n.target_epoch IS NOT NULL
         END;
$$;

CREATE TRIGGER namespace_ownership_check BEFORE INSERT OR UPDATE OR DELETE ON namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram_check_ownership();
-- N91: the move loader runs with session_replication_role = replica; this trigger must still fire.
ALTER TABLE namespace_ownership ENABLE ALWAYS TRIGGER namespace_ownership_check;

-- Shard-wide schedulers read these views (admin role only), never the table (N97): the
-- op-sweeper, DEFERRED resumer, consolidate-sweep, page-cron, outbox-trim and stats sweeper join
-- schedulable_namespaces (state = 'active': the target of a move is 'active' only after (c));
-- Expunge and the index sweeper join purgeable_namespaces, which excludes a namespace with an open
-- move (Plan to done, N124).
CREATE VIEW schedulable_namespaces AS
  SELECT namespace_id, tenant_id, epoch, move_epoch
    FROM namespace_ownership
   WHERE state = 'active';

CREATE VIEW purgeable_namespaces AS
  SELECT namespace_id, tenant_id, epoch
    FROM namespace_ownership
   WHERE state = 'active' AND move_epoch IS NULL;

-- namespace_stats: DERIVED counters (N69): refreshed per namespace by the stats sweeper (admin
-- role), never updated by CommitChunk or FinalizeVersion. live_facts counts VISIBLE facts (markers
-- applied). `large` (N112) flips when the namespace crosses 2,000 vectors of its current model
-- (cleared below 1,000) and drives the per-namespace partial HNSW indexes (vector_indexes).
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
  bytes_estimate      bigint NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),   -- relation bytes, hidden and unpurged rows included: the capacity signal (N114), not live_facts
  mentioned_histogram jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(mentioned_histogram) = 'object'),   -- {"2026-01": facts, ...}: the as_of selectivity estimate (N138)
  recalls_1h          bigint NOT NULL DEFAULT 0,
  retains_1h          bigint NOT NULL DEFAULT 0,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- namespace_models (N111): the namespace's CURRENT embedding model, fixed at creation. Every vector
-- arm reads embedding_model = this value. ReembedNamespace inserts vectors under the next model,
-- builds the next index, then flips this row (the one UPDATE of the workflow) and expunges the old.
CREATE TABLE namespace_models (
  namespace_id     uuid PRIMARY KEY,
  tenant_id        text NOT NULL,
  embedding_model  text NOT NULL CHECK (octet_length(embedding_model) BETWEEN 1 AND 128),
  embedding_dims   integer NOT NULL DEFAULT 768 CHECK (embedding_dims = 768),   -- the vector column is halfvec(768)
  next_model       text CHECK (next_model IS NULL OR octet_length(next_model) BETWEEN 1 AND 128),   -- set while ReembedNamespace runs
  changed_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE TRIGGER namespace_models_touch BEFORE UPDATE ON namespace_models
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- vector_indexes (N112, N138): which per-namespace partial HNSW indexes exist, as a machine
-- requested -> building -> ready, or failed (and dropping). The stats sweeper (engram_admin) INSERTs a
-- 'requested' row; the INDEX RUNNER (`engramctl index`, role engram_migrate, the only process that
-- issues index DDL) leases it (state 'building', lease_until), checks pg_index.indisvalid and drops an
-- invalid index BEFORE every build (never CREATE ... IF NOT EXISTS), then marks it 'ready' with
-- rows_at_build; a build whose lease expired is re-leased. purged_since_build is the expunge's own
-- bookkeeping (one writer, engram_admin): hygiene rebuilds a touched index at 1 % of rows_at_build or
-- 2 k elements, whichever comes first (view engram_index_hygiene_due), then VACUUM (INDEX_CLEANUP ON).
CREATE TABLE vector_indexes (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  vector_table     text NOT NULL CHECK (vector_table IN ('fact_vectors', 'chunk_vectors', 'observation_version_vectors', 'page_version_vectors')),
  embedding_model  text NOT NULL,
  state            text NOT NULL DEFAULT 'requested' CHECK (state IN ('requested', 'building', 'ready', 'failed', 'dropping')),
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

-- outbox: transactional outbox and per-shard change log (D6). PK is the global seq (the relay
-- reads in seq order); (namespace_id, seq) serves export deltas and the purge's consumer check.
-- namespace_id, tenant_id and epoch are stamped from the transaction scope, never passed by the
-- application. Events are thin (N12, N80): ids, versions and flags; never text or vectors; a
-- delete is ONE O(1) marker event (DocumentDeleted); ids are 16-byte `bytes`, an event carries at
-- most 256 ids, larger sets are paged, above 4,096 ids the event carries counts only
-- (ids_elided; consumers delete by the indexed (namespace_id, document_id, document_version <=
-- up_to_version) query, N119, N133c). Moves do
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

-- deletion_log (N122): the shard-local "already applied" record of every marker transaction
-- (delete, invalidate, restore), written IN the marker transaction. intent_key is the NAME of the intent
-- object `_control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json`, which the API puts AFTER the
-- marker commits and BEFORE the ack; the name is derived from this row, so it is the idempotency key and
-- `engramctl restore replay` re-applies each intent at most once. prev_operation_id is the subject's
-- previous entry read under the document lock: replay applies a subject's intents in chain order, never
-- by clock. effect is the marker's exact effect, so a duplicate attempt that finds the subject already
-- deleted re-puts the committed marker's own intent (put-if-absent). Insert-only.
CREATE TABLE deletion_log (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  intent_key    text NOT NULL CHECK (octet_length(intent_key) BETWEEN 1 AND 512),
  kind          text NOT NULL CHECK (kind IN ('document', 'invalidate', 'restore', 'namespace', 'tenant')),
  subject_id    text NOT NULL,                     -- document_id | memory_id::text | namespace_id::text | tenant_id
  epoch         bigint NOT NULL,
  operation_id  uuid,
  prev_operation_id uuid,
  effect        jsonb NOT NULL CHECK (jsonb_typeof(effect) = 'object'),   -- {up_to_version} | {memory_ids}
  deleted_at    timestamptz NOT NULL DEFAULT now(),
  ins_seq       bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, intent_key)
);

CREATE INDEX deletion_log_subject_idx ON deletion_log (namespace_id, kind, subject_id, deleted_at DESC);

-- =============================================================================
-- Namespace-scoped tables. The class of each table (insert-only, mutable, expiring, shard-local; N113,
-- N137) is the COMMENT ON TABLE set in the "Table classes" block after the partitions.
-- =============================================================================

-- ingest_ledger: append-only raw inputs. Body inline when <= 64 KiB; larger bodies live in the
-- OWNER-KEYED blob ledger/{ledger_id} (N104): ledger_id is minted before the put, the put precedes this
-- row, and the blob dies with this row in the purge, so no reference check or adoption race exists.
-- Rows are never purged by REPLACE/APPEND retirement, only by an explicit delete expunge.
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

-- documents (D8): MUTABLE (state, current_version, life_start, tags, metadata, context; tags are
-- item-level and live HERE only, N113). state 'deleting' is set in the same transaction as the
-- tombstone (N115). A Retain of a 'deleting' document REVIVES it (N133c) in the ack transaction:
-- state 'active', deleted_at NULL, current_version 0 (nothing finalised yet) and life_start = the
-- new version, which is greatest(max(document_versions.version), max(document_tombstones.up_to_version))
-- + 1, so a version number never falls at or below a tombstone's up_to_version. life_start is the
-- first version of the document's current life (1 for a new document): chunk identity is
-- (document_id, content_hash, life_start), so a revived document never re-uses a chunk row that a
-- tombstone covers. The marker transaction clears the content-bearing columns in the same row update
-- (summary_blob_key, summary_hash, document_hash, context, metadata, tags; the summary blob goes to
-- blob_tombstones) and the CHECK below pins it, so GetDocument/ListDocuments return the tombstone view
-- (document_tombstone_view) only (N115, N136). The Expunge deletes this row only when no open tombstone
-- with a higher up_to_version exists and no document_versions row remains (C-12).
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
  metadata          jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  document_hash     bytea CHECK (document_hash IS NULL OR octet_length(document_hash) = 32),   -- hash of the chunk-hash list
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
CREATE INDEX documents_deleting_idx ON documents (namespace_id, deleted_at) WHERE state = 'deleting';
-- the tag filter is resolved once per recall against this index into an allowed-document set (N116)
CREATE INDEX documents_tags_gin     ON documents USING gin (namespace_id, tags);
-- metadata filters resolve at document level into $allowed_docs like tags (N139, A-13)
CREATE INDEX documents_metadata_gin ON documents USING gin (namespace_id, metadata jsonb_path_ops);

CREATE TRIGGER documents_touch BEFORE UPDATE ON documents
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- document_versions: MUTABLE (status, chunks_done, body_key set once). H-16 / N104: body_key and
-- body_hash are NULLABLE. LoadItem creates the row without a body, then stores the reconstructed
-- body as the OWNER-KEYED blob ver/{document_id}/v{version} (N104: one owner row, dies with it in the
-- purge; cross-document dedup of bodies is given up) and sets both WHERE body_key IS NULL;
-- LoadItem(APPEND) materialises the base body from the ledger chain up to the nearest version that
-- has one (append_base_version), under the document lock.
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
  fact_count      integer CHECK (fact_count >= 0),                       -- written once by FinalizeVersion: the eligible-rows estimate of the selectivity-aware plan sums it over the current versions (N138)
  chunks_done     integer NOT NULL DEFAULT 0 CHECK (chunks_done >= 0),   -- written per wave by the workflow (N69), never per chunk
  body_hash       bytea CHECK (body_hash IS NULL OR octet_length(body_hash) = 32),   -- sha256 of the FULL reconstructed body; NULL until stored
  body_key        text,                                                   -- ver/{document_id}/v{version} (owner-keyed blob, N104)
  created_at      timestamptz NOT NULL DEFAULT now(),
  activated_at    timestamptz,
  finished_at     timestamptz,
  PRIMARY KEY (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  FOREIGN KEY (namespace_id, ledger_id)   REFERENCES ingest_ledger (namespace_id, ledger_id),
  CHECK (body_key IS NULL OR body_hash IS NOT NULL),
  CHECK (body_key IS NULL OR body_key = 'ver/' || document_id || '/v' || version::text),
  CHECK ((update_mode = 'append') = (append_base_version IS NOT NULL))
) WITH (fillfactor = 70);

-- at most one active version per document
CREATE UNIQUE INDEX document_versions_active_uq ON document_versions (namespace_id, document_id)
  WHERE status = 'active';
CREATE INDEX document_versions_op_idx ON document_versions (namespace_id, operation_id);
-- Lock protocol (N40 as amended by N83; model-checked in section 7). CommitChunk(v) takes
-- engram_try_doc_lock_shared(ns, doc) (failure -> retryable DocumentBusy, 100 ms), reads the
-- version row and documents.current_version as PLAIN reads and proceeds only when status =
-- 'ingesting'. FinalizeVersion and the delete marker transaction take the same key EXCLUSIVE,
-- so they wait for every in-flight commit of v, and a commit that starts afterwards sees the
-- terminal status and stops. The retain ack's version assignment keeps only the documents row
-- lock (FOR UPDATE) and takes no advisory lock, so ack latency is never behind ingest commits.

-- chunks (hash-partitioned), INSERT-ONLY. Identity within a document = content hash of the text
-- (N6: the contextual header is hashed separately). Text <= 4,000 characters, checked as <= 16 KiB
-- of UTF-8 (D8). Position in a version lives in document_version_chunks.ordinal; the vector in
-- chunk_vectors; retirement in chunk_tombstones. The extraction key is NOT here: a prompt or model
-- bump re-extracts a kept chunk and the new facts carry the new key (N58).
-- document_version is the version whose CommitChunk inserted the row (the version a document
-- tombstone's up_to_version is compared with, N133c). life_start is documents.life_start at insert:
-- identity within a document is (content hash, life), so the same text in a revived document is a
-- NEW row and the covered one is purged by the Expunge without touching the new content.
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
  header           text NOT NULL DEFAULT '' CHECK (octet_length(header) <= 1024),  -- "[doc summary] > [heading path]" as first embedded
  text             text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 16384),
  mentioned_at     timestamptz NOT NULL,                                     -- N86: max(timestamp of every item whose bytes the chunk covers); facts inherit it
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

-- facts (hash-partitioned), INSERT-ONLY. memory_id is the public id of a fact (D1, UUIDv7).
-- No tags (item-level tags live on documents), no retired_at / invalidated_at / live (markers,
-- N115), no vector (fact_vectors), no generated column.
CREATE TABLE facts (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  memory_id            uuid NOT NULL,
  document_id          text NOT NULL,
  document_version     integer NOT NULL CHECK (document_version >= 1),       -- version whose CommitChunk inserted the fact (N133c)
  chunk_id             uuid NOT NULL,
  ordinal              smallint NOT NULL CHECK (ordinal >= 0),               -- position in the chunk's extraction
  content_hash         bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(normalised text); the curation_log key (N115)
  extraction_key       bytea NOT NULL CHECK (octet_length(extraction_key) = 32), -- N87 key = xcache key; a re-extraction of a kept chunk inserts facts under a new key and marks the old ones fact_hidden(reextract): a write, not a hide of derived content (N58, N135)
  text                 text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
  fact_type            fact_type NOT NULL,
  w5                   jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(w5) = 'object'),  -- {who:[],what,when,where,why}
  occurred_start       timestamptz,
  occurred_end         timestamptz,
  mentioned_at         timestamptz NOT NULL,            -- = the item's timestamp, SERVER-SET; the only as_of key (D9); never written by the extractor or a per-item override
  said_at              timestamptz,                     -- the model's judgement of when the source said it: display and ranking only, never an as_of key
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

-- Vector side tables (N111): INSERT-ONLY, hash-partitioned by namespace_id like their parents, one row
-- per (content id, embedding model). The arms read embedding_model = the namespace's CURRENT model
-- (namespace_models); ReembedChunk and a model change INSERT rows, ReembedNamespace flips the
-- current model and the expunge removes the old rows. The columns after the key are immutable
-- COPIES of attributes of the content row, so every visibility and as_of predicate can run INSIDE
-- the (iterative) index scan without a join: document_id, document_version and chunk_id for the marker
-- sets (N116, N133c), mentioned_at / effective_at for as_of, fact_type for the type filter (N138).
-- STORAGE MAIN keeps the vector inline so the exact scan is a heap scan.
CREATE TABLE fact_vectors (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  memory_id        uuid NOT NULL,
  embedding_model  text NOT NULL,
  document_id      text NOT NULL,
  document_version integer NOT NULL CHECK (document_version >= 1),
  chunk_id         uuid NOT NULL,
  fact_type        fact_type NOT NULL,                         -- immutable copy (N138): the type filter runs inside the scan
  mentioned_at     timestamptz NOT NULL,
  embedding        halfvec(768) STORAGE MAIN NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  ins_seq          bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, memory_id, embedding_model),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);

-- chunk_vectors: a summary refresh re-embeds a chunk without re-extracting (N110a): the new row has a
-- newer embedding_effective_at (N85: mentioned_at of the newest item covered by the summary in the
-- embedded header). That column is therefore part of the key (a documented extension of N111's key):
-- under as_of = T the chunk arm admits only rows with embedding_effective_at <= T and mentioned_at <= T,
-- and takes the best admitted row per chunk.
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

-- fact_links (hash-partitioned), INSERT-ONLY. One row per edge. entity/temporal/semantic edges are
-- undirected and stored once in canonical order (src < dst); causal edges are directed (src = cause,
-- dst = effect). Per-fact caps (temporal <= 20, semantic <= 10, entity <= 10 per shared entity) are
-- enforced by the linker (section 5). A link is inserted in the CommitChunk of its newer endpoint.
-- The graph arm joins the endpoints and requires BOTH to be visible (N116); the expunge deletes the
-- rows through the FK cascade from facts.
CREATE TABLE fact_links (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  src_memory_id  uuid NOT NULL,
  dst_memory_id  uuid NOT NULL,
  link_type      link_type NOT NULL,
  weight         real NOT NULL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),
  ins_seq        bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, src_memory_id, dst_memory_id, link_type),
  FOREIGN KEY (namespace_id, src_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, dst_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  CHECK (src_memory_id <> dst_memory_id),
  CHECK (link_type = 'causal' OR src_memory_id < dst_memory_id)
) PARTITION BY HASH (namespace_id);

-- entities / entity_aliases / entity_mentions (N118)
-- entities is MUTABLE (mention_count, last_seen_at, merged_into; the expunge recomputes canonical_name
-- from the remaining mentions). Under as_of an EntityRef carries only `mention`: canonical_name and
-- alias merges are suppressed and entity hops use entity_mentions.mentioned_at <= T.
CREATE TABLE entities (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  entity_id       uuid NOT NULL,
  canonical_name  text NOT NULL CHECK (octet_length(canonical_name) BETWEEN 1 AND 256),
  canonical_norm  text NOT NULL CHECK (octet_length(canonical_norm) BETWEEN 1 AND 256),   -- lower/trim/unaccent, computed in Go
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
-- fuzzy resolution: multi-column GIN (btree_gin for the uuid) so the index leads with namespace_id.
-- Reached ONLY through engram_entity_fuzzy (SECURITY DEFINER, N131): the trigram operators are not
-- leakproof, so under RLS the planner would not push them into the index scan (P-5).
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
  document_id   text,                              -- N118: the document whose item produced the alias; the expunge deletes the victim's aliases
  document_version integer CHECK (document_version >= 1),   -- N133c: the expunge deletes only document_version <= up_to_version
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, alias_norm),
  FOREIGN KEY (namespace_id, entity_id) REFERENCES entities (namespace_id, entity_id) ON DELETE CASCADE,
  CHECK ((document_id IS NULL) = (document_version IS NULL))
);

CREATE INDEX entity_aliases_entity_idx ON entity_aliases (namespace_id, entity_id);
CREATE INDEX entity_aliases_document_idx ON entity_aliases (namespace_id, document_id, document_version) WHERE document_id IS NOT NULL;

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
-- (observation_versions). stale_write / stale_delete mean "needs a rewrite" and never hide
-- anything by themselves (N117: hiding is the read predicate).
CREATE TABLE observations (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  observation_id    uuid NOT NULL,
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  proof_count       integer NOT NULL DEFAULT 0 CHECK (proof_count >= 0),
  stale_write       boolean NOT NULL DEFAULT false,      -- evidence changed under it (replace-retire, restore, re-extraction): rewrite wanted; FinalizeVersion sets it in its own transaction (N135)
  stale_delete      boolean NOT NULL DEFAULT false,      -- Materialize found a victim in its segment: root rebuild wanted (N119)
  stale_since       timestamptz,
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

-- observation_versions (hash-partitioned), INSERT-ONLY (N117). root_version = version for a ROOT
-- REBUILD (written from live sources only, no previous text shown), else root_version(v - 1): the
-- derivation set of (O, v) is observation_inputs(O, w) for root_version(v) <= w <= v.
-- effective_at(v) = max(mentioned_at of every fact shown to v's writer, effective_at(v - 1)) (D9),
-- monotone. The one value a later version adds to an earlier one, superseded_at, lives in
-- observation_version_meta. UNIQUE (namespace_id, ov_id): ov_id is the single-column BM25 key_field.
-- stub (N136): DerivedPurge replaces a covered version by a CONTENT-FREE stub in one admin transaction
-- (delete the version row, which cascades its inputs, sources, vector row and BM25 entry; insert the same
-- (ov_id, version, root_version, effective_at) with text = '' and stub = true). The stub keeps the D9 range
-- arithmetic and the *_version_meta rows valid, is what the fail-closed predicate finds, and stays covered by
-- the permanent derived_hidden row. It is an insert, never an UPDATE.
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
  created_at        timestamptz NOT NULL DEFAULT now(),
  ins_seq           bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, observation_id, version),
  UNIQUE (namespace_id, ov_id),
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  CHECK (root_version BETWEEN 1 AND version),
  CHECK (CASE WHEN stub THEN text = '' AND source_count = 0
              ELSE octet_length(text) BETWEEN 1 AND 8192 AND source_count >= 1 END)
) PARTITION BY HASH (namespace_id);

CREATE TABLE observation_version_vectors (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  ov_id            uuid NOT NULL,
  embedding_model  text NOT NULL,
  observation_id   uuid NOT NULL,
  version          integer NOT NULL,
  effective_at     timestamptz NOT NULL,
  embedding        halfvec(768) STORAGE MAIN NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  ins_seq          bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, ov_id, embedding_model),
  FOREIGN KEY (namespace_id, ov_id) REFERENCES observation_versions (namespace_id, ov_id) ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);

-- observation_version_meta (N33): write-once. When version v + 1 is inserted, ApplyBatch inserts
-- (O, v, superseded_at = effective_at(v + 1)) in the same transaction; the current version has NO
-- row. An as_of query is then a plain range filter: effective_at <= T AND (no meta row OR
-- superseded_at > T). The FK is DEFERRED and does not cascade (N136): DerivedPurge deletes a version row
-- and inserts its stub in one transaction, and the meta row must survive; purges of a whole observation
-- delete the meta rows first.
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

-- observation_sources is the mutable WORKING SET of the CURRENT version (evidence the consolidator
-- may still extend), rewritten only by ApplyBatch under the derivation lock (N120). The evidence of
-- each version is frozen in observation_version_sources.
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

-- observation_inputs (N41, N117), INSERT-ONLY: every fact the stage-2 writer was SHOWN when it
-- wrote version v (the batch facts attached to O; at most 5 quoted older sources; all of them O's
-- own sources, N121). document_id and document_version are denormalised (immutable copies of the
-- fact's) so the read predicate and the expunge reach a victim document version without joining
-- facts. NO foreign key to facts (N135): evidence outlives the facts it names (REPLACE then a chunk purge
-- must not erase what a later DeleteDocument needs to find); the row dies with its own version only.
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
CREATE INDEX observation_inputs_fact_idx     ON observation_inputs (namespace_id, fact_id);   -- FinalizeVersion's stale_write update for re-extraction (N135)

-- observation_version_sources (N85), INSERT-ONLY: the cited evidence of EACH version, copied from
-- consolidation_proposals.ops (quote <= 500 chars) when the version is applied. A source is visible iff its
-- own document_version is not covered by an open tombstone and its fact has no fact_hidden(invalidate) row
-- (the document columns are copies, like observation_inputs', so no join to facts is needed and a purged
-- fact does not matter); proof_count counts visible sources and a version with none is not served (N117,
-- N135). NO foreign key to facts.
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

-- consolidation_batches: MUTABLE exactly-once effect (D12, N121). attempt is the CURRENT attempt (a batch is
-- re-queued at most 3 times, so 1..4); state: routed = stage 1 decided; stored = the proposal of `attempt`
-- is persisted; applied = its ops took effect (written in the SAME transaction as the 'done' stamps, and by
-- an all-skip batch that applies zero ops); discarded = the stored proposal went stale (a base_version no
-- longer current) and the batch is re-routed under a new attempt, or is terminal after attempt 4; capacity =
-- the overflow rerun (new attempt, prompt_variant 'capacity') is pending. 'Already applied' is
-- state = 'applied'. Nothing here is ever deleted by engram_app.
CREATE TABLE consolidation_batches (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  batch_key         bytea NOT NULL CHECK (octet_length(batch_key) = 32),   -- sha256(sorted memory ids || prompt_version || model)
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
-- op_index) is computed over THIS stored list. ops is a JSON array in op_index order: {kind, observation_id
-- (pre-minted for creates), text, source_fact_ids[], quotes[], reason, base_version}; every update and merge
-- op records the base_version it was rendered from (CHECK). input_fact_ids = the facts rendered to the
-- writer = the observation_inputs rows of every version the batch creates. A discard or a capacity retry
-- writes a NEW attempt; the old list is dead by key. engram_app holds SELECT, INSERT only: no UPDATE, no
-- DELETE, ever. Stage-1 routing decisions are not persisted.
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
  FOREIGN KEY (namespace_id, batch_key, attempt) REFERENCES consolidation_proposals (namespace_id, batch_key, attempt)   -- RESTRICT: the proposal of an applied op is never removed
) WITH (fillfactor = 100);

CREATE INDEX consolidation_applied_batch_idx ON consolidation_applied (namespace_id, batch_key, attempt, op_index);

-- fact_consolidation (N95, H-15), INSERT-ONLY: append-only stamps, never updated. A fact is
-- consolidated iff a 'done' stamp exists; it is retryable iff its latest stamp is 'failed' and older
-- than 7 days (a 'capacity' stamp as latest means "not yet", also pending); batch_key is required
-- exactly for 'done'.
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

-- consolidation_state (N95): ONE row per namespace, MUTABLE. The watermark advances only to just
-- below the smallest UNCONSOLIDATED fact, visible or marker-hidden (H-23: a hidden fact that Restore
-- later reveals must still be pending), and never past engram_uuid_v7_floor(now() - 2 x
-- statement_timeout) (see engram_consolidation_watermark below).
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

-- pages (D12, phase 3): MUTABLE narrow state. stale_seq is a monotone counter: a refresh captures
-- it and clears the flags only if it is unchanged (section 5.3). A hidden CURRENT page version is
-- served as PreconditionFailed{PAGE_HIDDEN} until the refresh lands (N117).
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

-- page_versions: INSERT-ONLY. root_version as for observation versions (N117); superseded_at in
-- page_version_meta. text is the page's searchable markdown (N139, A-14): BM25 index plus
-- page_version_vectors under the N111/N112 rules, so pages share the visibility and purge path of
-- observations. pv_id is the single-column BM25 key_field. A purged version becomes a content-free stub
-- (text = '', stub = true, the .md blob deleted), exactly like an observation version (N136).
CREATE TABLE page_versions (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  page_id            uuid NOT NULL,
  version            integer NOT NULL CHECK (version >= 1),
  pv_id              uuid NOT NULL,
  root_version       integer NOT NULL,
  text               text NOT NULL,
  markdown_blob_key  text NOT NULL,                  -- pages/{page_id}/v{version}.md
  effective_at       timestamptz NOT NULL,           -- D9 rule, same as observation_versions
  evidence_hash      bytea NOT NULL CHECK (octet_length(evidence_hash) = 32),
  stub               boolean NOT NULL DEFAULT false,
  created_at         timestamptz NOT NULL DEFAULT now(),
  ins_seq            bigint NOT NULL DEFAULT nextval('engram_ins_seq'),
  PRIMARY KEY (namespace_id, page_id, version),
  UNIQUE (namespace_id, pv_id),
  FOREIGN KEY (namespace_id, page_id) REFERENCES pages (namespace_id, page_id),
  CHECK (root_version BETWEEN 1 AND version),
  CHECK (CASE WHEN stub THEN text = '' ELSE octet_length(text) BETWEEN 1 AND 262144 END)
) WITH (fillfactor = 100);

-- page_version_vectors: INSERT-ONLY, keyed (pv_id, embedding_model) like the other vector tables (N111).
-- Pages are few, so the table is not partitioned; engram_hnsw_ddl builds the same per-namespace partial
-- HNSW on the table itself (N112).
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
  FOREIGN KEY (namespace_id, page_id, version) REFERENCES page_versions (namespace_id, page_id, version) DEFERRABLE INITIALLY DEFERRED
) WITH (fillfactor = 100);

-- page_version_inputs (N117), INSERT-ONLY: what the page writer was shown for each version. The
-- derivation depth is fixed at two (fact -> observation -> page), so a page version is hidden by two
-- EXISTS (a fact input of its segment is tombstoned/hidden, or an observation-version input of its
-- segment is hidden). source_version is 0 for facts. document_id and document_version are the
-- fact's (NULL for observation inputs); no FK to the source because it is polymorphic and evidence
-- outlives its facts (N135), so DerivedPurge and the retire purge delete by page version.
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
  FOREIGN KEY (namespace_id, page_id, version) REFERENCES page_versions (namespace_id, page_id, version) ON DELETE CASCADE,
  CHECK ((kind = 'fact') = (document_id IS NOT NULL)),
  CHECK ((kind = 'fact') = (document_version IS NOT NULL)),
  CHECK ((kind = 'fact') = (source_version = 0))
) WITH (fillfactor = 100);

CREATE INDEX page_version_inputs_document_idx ON page_version_inputs (namespace_id, document_id, document_version) WHERE document_id IS NOT NULL;
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
-- Deletion and invalidation markers (N115): the ONLY synchronous writes of a delete or an
-- invalidation. Tiny, namespace-keyed, loaded into the recall request three selects at a time (N116).
-- -----------------------------------------------------------------------------

-- document_tombstones: one row per delete of a document, covering (document_id, up_to_version)
-- (N133c): it hides exactly the rows with document_version <= up_to_version, i.e. every version
-- the document had when it was deleted (up_to_version = max(document_versions.version) read under
-- the document lock, in-flight versions included). The same document_id can be retained again at
-- once: the new versions start at up_to_version + 1 and stay visible while the covered rows are
-- hidden and then purged. up_to_version is part of the key because a re-used id can be deleted
-- again while the first tombstone still exists. expunge_state is the Expunge's progress
-- (pending -> materialized -> purged, N119); the row is deleted 24 h after 'purged'. Recall
-- passes the rows in 'pending' and 'materialized' (purged ones have no rows left to hide).
-- intent_key names the blob-storage intent object (deterministic from deleted_at and operation_id): it is
-- put AFTER this row commits and before the ack (N122). event_seq is the seq of the DocumentDeleted event
-- written in the same transaction (the transaction draws it first and inserts the outbox row last), so the
-- purge gate engram_consumers_passed is a comparison with the cursors, not a scan of the outbox (P-15).
CREATE TABLE document_tombstones (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  document_id    text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  up_to_version  integer NOT NULL CHECK (up_to_version >= 1),
  deleted_at     timestamptz NOT NULL,
  operation_id   uuid,
  intent_key     text,
  event_seq      bigint NOT NULL,
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

-- chunk_tombstones: REPLACE / re-extraction retired a chunk. Un-retire on a flap is a DELETE of the
-- row. The expunge purges the chunk (and deletes the row) after the 1 h grace (target CHUNK_TOMBSTONES).
-- FK cascade: the purge of the chunk removes its marker. The purge never removes evidence rows (N135).
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

-- fact_hidden (N115, N135): keyed (memory_id, cause). Invalidate inserts cause 'invalidate'; Restore deletes
-- ONLY that row, so Restore is exact and cannot resurrect a stale extraction next to its re-extracted twin.
-- cause 'reextract' (N58) is written by FinalizeVersion for facts of a kept chunk whose extraction key is
-- stale: the fact and chunk arms hide a fact while ANY row exists, but derived-version predicates read
-- cause = 'invalidate' only (re-extraction is a write: dependents are flagged stale_write, not hidden), and
-- the old-key facts are purged after 1 h (target REEXTRACTED_FACTS), which bounds the marker sets. The
-- 'invalidate' set is permanent by design and is tested by primary-key anti-join, never passed as an
-- array. reason is the caller's free text. FK cascade removes the markers when the fact is purged.
CREATE TABLE fact_hidden (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  memory_id     uuid NOT NULL,
  cause         text NOT NULL DEFAULT 'invalidate' CHECK (cause IN ('invalidate', 'reextract')),
  hidden_at     timestamptz NOT NULL DEFAULT now(),
  reason        text NOT NULL DEFAULT '' CHECK (octet_length(reason) <= 1024),
  intent_key    text,
  PRIMARY KEY (namespace_id, memory_id, cause),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
) WITH (fillfactor = 50);

-- curation_log (A-16), INSERT-ONLY: every Invalidate / Restore with the fact's content_hash and
-- document. CommitChunk re-applies the LAST action per (document_id, content_hash) to the new facts
-- of a re-extracted twin (of the document's current life: document_version >= documents.life_start),
-- so curation sticks. No FK: it must outlive the purge of the old fact; the expunge deletes the
-- rows of a deleted document explicitly (document_version <= up_to_version, N133c).
CREATE TABLE curation_log (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
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

CREATE INDEX curation_log_twin_idx ON curation_log (namespace_id, document_id, content_hash, at DESC);   -- the twin lookup filters document_version >= life_start

-- -----------------------------------------------------------------------------
-- Expunge stage tables (N119)
-- -----------------------------------------------------------------------------

-- derived_hidden: the MATERIALISATION of the read predicate, written by Expunge.Materialize under the
-- exclusive derivation lock (N120). One row says "versions of <kind> <id> in root segment root_version
-- from from_version on are hidden because of <cause>", where from_version = min(version) of the
-- inputs of that segment that name the victim. Rows with cause_kind 'document' are PERMANENT (an
-- older version written with the victim in view must never resurface at any as_of); rows with
-- cause_kind 'invalidation' are deleted by Restore (under the exclusive derivation lock). Removed only with
-- the observation/page itself or the namespace. DerivedPurge (N136) leaves a stub for every version such a
-- row covers, and the row keeps covering it.
CREATE TABLE derived_hidden (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  kind          text NOT NULL CHECK (kind IN ('observation', 'page')),
  id            uuid NOT NULL,                       -- observation_id | page_id
  root_version  integer NOT NULL CHECK (root_version >= 1),
  from_version  integer NOT NULL,
  cause_kind    text NOT NULL CHECK (cause_kind IN ('document', 'invalidation')),
  cause_id      text NOT NULL,                       -- document_id | memory_id::text (a document cause covers the tombstone's document_version <= up_to_version inputs only, N133c)
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, kind, id, root_version, cause_kind, cause_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (from_version >= root_version)
) WITH (fillfactor = 70);

CREATE INDEX derived_hidden_cause_idx ON derived_hidden (namespace_id, cause_kind, cause_id);

-- expunge_progress: restartable purge position. unit = a document_id, or '*chunks' (replaced chunks
-- past the 1 h grace), '*reextract' (old-key facts past the grace), or '*namespace'; one row per
-- (unit, table). The purge deletes in batches of 1,000 ordered by primary key, PACED BY WAL (each batch's
-- pg_current_wal_insert_lsn delta is measured; <= 25 MB/s per shard, at most two expunges per shard, N119),
-- and records last_key after each batch.
CREATE TABLE expunge_progress (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  unit          text NOT NULL,
  table_name    text NOT NULL,
  phase         text NOT NULL DEFAULT 'purge' CHECK (phase IN ('materialize', 'purge', 'derived_purge', 'hygiene', 'finish')),
  last_key      text,
  rows_purged   bigint NOT NULL DEFAULT 0 CHECK (rows_purged >= 0),
  done          boolean NOT NULL DEFAULT false,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, unit, table_name),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 50);

CREATE TRIGGER expunge_progress_touch BEFORE UPDATE ON expunge_progress
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- operations: async work visible through OperationService (D13: DEFERRED on quota exhaustion).
-- workflow_started_at IS NULL marks "no workflow yet" for the op-sweeper (N3). The CHECK lists below are the
-- proto enums (OperationKind, CancelReason) lower-cased and are generated from them (N139, A-8).
-- MOVE_NAMESPACE lives on the catalog namespace_moves row and DELETE_TENANT is derived in the catalog
-- (N127, N133d), so neither has a row here. Workflow mapping (N136): retain, export and refresh run at
-- ns/{ns}/op/{op}; delete_document is the expunge singleton plus the tombstone's operation_id; consolidate is
-- the singleton. DELETE_* operations are not cancellable.
CREATE TABLE operations (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  operation_id         uuid NOT NULL,
  kind                 text NOT NULL CHECK (kind IN ('retain_document', 'delete_document', 'delete_namespace',
                                                     'consolidate', 'refresh_page', 'create_snapshot')),   -- no 'reflect': Reflect is synchronous
  state                operation_state NOT NULL DEFAULT 'PENDING',
  request_id           text CHECK (octet_length(request_id) <= 128),
  target_id            text,                           -- document_id / page_id / snapshot version, per kind
  superseded_by        bigint CHECK (superseded_by >= 1),   -- N127/A-8: the newer document VERSION number (proto int64), not an operation id
  cancel_reason        text CHECK (cancel_reason IN ('client_request', 'document_deleted', 'namespace_deleting')),
  workflow_id          text NOT NULL,                  -- ns/{namespace_id}/op/{operation_id}, or the expunge/consolidate singleton (N136)
  task_queue           text NOT NULL,                  -- shard-{shard_id} at submission; restarted on the target after a move
  submitted_epoch      bigint NOT NULL CHECK (submitted_epoch >= 1),   -- audit only; never part of an identity (D11)
  workflow_started_at  timestamptz,
  progress             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),  -- written once per wave by the workflow (N69), never per chunk
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
CREATE INDEX operations_sweeper_idx  ON operations (created_at)     WHERE state = 'PENDING' AND workflow_started_at IS NULL;
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

-- token_usage_events: one row per gateway call, idempotent by usage_key (section 5 N5-1);
-- 30-day retention. token_usage: the daily aggregate the API reports (D13).
CREATE TABLE token_usage_events (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  usage_key          bytea NOT NULL CHECK (octet_length(usage_key) = 32),  -- sha256(operation_id || activity || item key)
  day                date NOT NULL,
  op                 text NOT NULL CHECK (op IN ('extract', 'summarize', 'embed', 'consolidate', 'adjudicate', 'reflect', 'page', 'rerank')),
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

-- quota_counters: per-namespace windows (llm_tokens_per_day, recalls/retains per minute for
-- auditing the API's token buckets); max_facts reads namespace_stats.live_facts.
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
                                                  'derived_purge', 'transcript_purge')),   -- derived_purge: pages/{id}/v{n}.md of a stubbed page version; transcript_purge: reflect/{op}.jsonl (N136)
  not_before     timestamptz NOT NULL DEFAULT now(),  -- N100: the sweeper deletes only when not_before <= now(); xcache_gc rows carry now() + xcache_grace (24 h) and are re-checked against live chunks first
  operation_id   uuid,
  attempts       integer NOT NULL DEFAULT 0,
  last_error     text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, tombstone_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

CREATE INDEX blob_tombstones_created_idx ON blob_tombstones (namespace_id, not_before);

-- export_snapshots (D12, N126): one row per snapshot version, MUTABLE. BeginSnapshot inserts it as
-- 'building' with snapshot_started_at; the marker transaction of a delete expires 'building' and
-- 'ready' rows alike; RecordSnapshot refuses to promote an expired row and re-checks
-- document_tombstones.deleted_at > snapshot_started_at. There is NO outbox cut: a delta is the diff
-- of two consecutive snapshots (base_version), always emitted with delete records (deleted_ids in
-- the manifest) even when the base expired. expires_at = now() hands the blobs to the tombstone sweeper.
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

-- =============================================================================
-- Functions that need the tables (SQL bodies are validated at CREATE time)
-- =============================================================================

-- ---- Marker sets (N116) -----------------------------------------------------
-- The recall layer loads TWO sets once per request and passes them to every arm as parameters:
-- doc_tomb (engram_doc_hidden($doc_tomb, f.document_id, f.document_version)) and chunk_tomb
-- (f.chunk_id <> ALL($chunk_tomb)), both bounded by expunge lag (alert above 16 k entries; above it an arm
-- switches to the NOT EXISTS anti-join form). fact_hidden is NEVER an array (N116, N135): the 'invalidate'
-- rows are permanent by design, so every arm and predicate tests them per candidate by primary-key
-- anti-join. These functions are the same selects, for tests, engramctl and the SQL fallback.
--
-- DocTomb (N133c) is a jsonb object {document_id: up_to_version}: per document the GREATEST
-- up_to_version over its open tombstones (a document can be deleted, re-used and deleted again while
-- the first marker still exists). A row is hidden by it iff document_version <= that number, so the
-- versions of a re-used document_id above it stay visible. Purged tombstones are left out: their
-- rows are gone.
CREATE FUNCTION engram_doc_tomb(p_ns uuid, p_pending_only boolean DEFAULT false) RETURNS jsonb
LANGUAGE sql STABLE AS $$
  SELECT coalesce(jsonb_object_agg(x.document_id, x.up_to), '{}'::jsonb)
    FROM (SELECT t.document_id, max(t.up_to_version) AS up_to
            FROM document_tombstones t
           WHERE t.namespace_id = p_ns AND t.expunge_state <> 'purged'
             AND (NOT p_pending_only OR t.expunge_state = 'pending')
           GROUP BY t.document_id) x;
$$;

-- The document half of the visibility predicate: is version p_ver of document p_doc covered by a
-- tombstone in the set p_tomb? Single-expression SQL, so the planner inlines it.
CREATE FUNCTION engram_doc_hidden(p_tomb jsonb, p_doc text, p_ver integer) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT p_ver <= coalesce((p_tomb ->> p_doc)::integer, 0);
$$;

CREATE FUNCTION engram_chunk_tomb(p_ns uuid) RETURNS uuid[]
LANGUAGE sql STABLE AS $$
  SELECT coalesce(array_agg(t.chunk_id), '{}'::uuid[]) FROM chunk_tombstones t WHERE t.namespace_id = p_ns;
$$;

-- ---- The visibility predicate (N116, N117, N135): the one SQL every arm applies ------------------
-- Facts and chunks: visible(f) = (f.document_id, f.document_version) NOT COVERED BY DocTomb AND
-- f.chunk_id NOT IN ChunkTomb AND NO fact_hidden row of ANY cause, plus as_of = mentioned_at <= T on the
-- immutable row. "Covered" is document_version <= up_to_version (N133c).
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

CREATE FUNCTION engram_visible_chunks(p_ns uuid, p_as_of timestamptz DEFAULT 'infinity') RETURNS TABLE (chunk_id uuid)
LANGUAGE sql STABLE AS $$
  WITH m AS (SELECT engram_doc_tomb(p_ns) AS d, engram_chunk_tomb(p_ns) AS c)
  SELECT k.chunk_id
    FROM chunks k, m
   WHERE k.namespace_id = p_ns AND k.mentioned_at <= p_as_of
     AND NOT engram_doc_hidden(m.d, k.document_id, k.document_version) AND k.chunk_id <> ALL (m.c);
$$;

-- Observation version (O, v) is HIDDEN iff (0) no version row exists (FAIL CLOSED, C-18: DerivedPurge keeps
-- a stub, so the row exists for every purged version), or it is a stub, or (a) an input of its SEGMENT
-- (root_version(v) <= w <= v) names a document version covered by a tombstone in p_doc_tomb or a fact with
-- a fact_hidden(INVALIDATE) row (cause 'reextract' hides nothing derived, N135), or (b) a derived_hidden
-- row covers it. The read path passes the PENDING tombstones (engram_doc_tomb(ns, true)): once Materialize
-- has finished a marker, (b) answers for it. A WRITER's commit re-verification passes ALL open tombstones
-- (engram_doc_tomb(ns, false), N120). Nothing here walks: a version that commits after the marker and
-- names the victim is hidden because inputs are read at query time.
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
                                        WHERE h.namespace_id = p_ns AND h.memory_id = i.fact_id AND h.cause = 'invalidate')))
         OR EXISTS (SELECT 1 FROM derived_hidden h
                     WHERE h.namespace_id = p_ns AND h.kind = 'observation' AND h.id = p_obs
                       AND h.root_version = v.root_version AND v.version >= h.from_version)));
$$;

-- Page version: the same rule with depth two (fact -> observation -> page; pages never feed
-- observations): hidden iff no row / a stub, or a fact input of its segment is tombstoned or invalidated, OR
-- an observation-version input of its segment is hidden by the observation rule (which fails closed on a
-- purged version too), OR a derived_hidden row covers it.
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
                                        WHERE h.namespace_id = p_ns AND h.memory_id = i.source_id AND h.cause = 'invalidate')))
         OR EXISTS (SELECT 1 FROM page_version_inputs i
                     WHERE i.namespace_id = p_ns AND i.page_id = p_page
                       AND i.version BETWEEN pv.root_version AND pv.version
                       AND i.kind = 'observation'
                       AND engram_obs_version_hidden(p_ns, i.source_id, i.source_version, p_doc_tomb))
         OR EXISTS (SELECT 1 FROM derived_hidden h
                     WHERE h.namespace_id = p_ns AND h.kind = 'page' AND h.id = p_page
                       AND h.root_version = pv.root_version AND pv.version >= h.from_version)));
$$;

-- Observation versions an arm may return. Served(T) is the version CURRENT at T (effective_at <= T AND
-- no meta row or superseded_at > T); when it is hidden NOTHING is served for that observation at T (C-14:
-- never an older version). Retirement is filtered by its own time (retired_at IS NULL OR retired_at > T).
-- The default T = infinity serves the CURRENT version (the one without a meta row). A version with no
-- visible source (proof_count = 0: every cited source tombstoned or invalidated) is not served (A-19).
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
                  WHERE src.namespace_id = v.namespace_id AND src.observation_id = v.observation_id AND src.version = v.version
                    AND NOT engram_doc_hidden(m.da, src.document_id, src.document_version)
                    AND NOT EXISTS (SELECT 1 FROM fact_hidden h
                                     WHERE h.namespace_id = src.namespace_id AND h.memory_id = src.memory_id AND h.cause = 'invalidate'));
$$;

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

-- ---- The derivation commit rule (N120): the re-verification a writer runs under the SHARED derivation lock -----
-- Every writer of an observation or page version (ApplyBatch stage 2, degraded-mode root rebuilds and merges,
-- CommitPageVersion for page/v1 and page_full/v1) commits in one transaction that (1) try-locks the derivation
-- lock shared, (2) re-verifies EVERY rendered input in a fresh statement, (3) checks its base, (4) on any failure
-- ROLLBACKs and re-derives from current evidence; the lock is never held across the LLM call.
--   (2) fact inputs: every fact exists and is visible against the FULL marker sets (all open tombstones, chunk_tomb,
--       fact_hidden of both causes); observation-version inputs: NOT engram_obs_version_hidden(ns, o, v,
--       engram_doc_tomb(ns, false)), against all open tombstones, not only pending ones.
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

--   (3) the base: the version the writer rendered from is still CURRENT and VISIBLE. A root rebuild has no base
--       (p_base IS NULL) and skips the check. Inheriting root(base) and requiring only a visible base is rejected:
--       a visible but superseded base still lets stale text overwrite a rebuild.
CREATE FUNCTION engram_derivation_base_ok(p_ns uuid, p_kind text, p_id uuid, p_base integer) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT CASE
           WHEN p_base IS NULL THEN true
           WHEN p_kind = 'observation' THEN
             coalesce((SELECT o.current_version = p_base
                              AND NOT engram_obs_version_hidden(p_ns, p_id, p_base, engram_doc_tomb(p_ns, false))
                         FROM observations o WHERE o.namespace_id = p_ns AND o.observation_id = p_id), false)
           WHEN p_kind = 'page' THEN
             coalesce((SELECT g.current_version = p_base
                              AND NOT engram_page_version_hidden(p_ns, p_id, p_base, engram_doc_tomb(p_ns, false))
                         FROM pages g WHERE g.namespace_id = p_ns AND g.page_id = p_id), false)
           ELSE false END;
$$;

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
-- pg_trgm's operators are not leakproof, so under RLS the planner refuses to push them into the GIN
-- index scan. The wrapper runs as the owner (BYPASSRLS), re-checks that p_ns IS the namespace in
-- scope and filters on namespace_id itself, so the index is used and the isolation stays exact.
-- EXECUTE is granted to engram_app only through the general function grant; the check below is the guard.
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

-- ---- Consolidation bookkeeping (N95, H-15, H-23) ---------------------------------------------------
-- Pending facts of a namespace: VISIBLE facts above the watermark with no 'done' stamp, except those
-- whose latest stamp is 'failed' and younger than 7 days, oldest first.
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

-- The watermark the consolidate sweep may advance to: the largest fact id strictly below the smallest
-- UNCONSOLIDATED fact (no visibility filter: a marker-hidden fact that Restore may reveal still
-- counts, H-23), and never past a fact whose ins_seq is above engram_seq_floor(now()) (C-19: the guard is
-- the insertion sequence, which orders rows by INSERT, not an id timestamp that a long transaction or a
-- retry reusing minted ids can fall behind; ids are minted per attempt inside the transaction). NULL = do
-- not advance. A fact of a re-extracted chunk stops pinning the watermark when the purge removes it (1 h).
CREATE FUNCTION engram_consolidation_watermark(p_ns uuid) RETURNS uuid
LANGUAGE sql STABLE AS $$
  WITH w AS (SELECT coalesce((SELECT s.watermark_memory_id FROM consolidation_state s WHERE s.namespace_id = p_ns),
                             '00000000-0000-0000-0000-000000000000'::uuid) AS wm),
       u AS (SELECT coalesce((SELECT f.memory_id
                                FROM facts f, w
                               WHERE f.namespace_id = p_ns AND f.memory_id > w.wm
                                 AND NOT EXISTS (SELECT 1 FROM fact_consolidation c
                                                  WHERE c.namespace_id = f.namespace_id AND c.memory_id = f.memory_id AND c.note = 'done')
                               ORDER BY f.memory_id LIMIT 1),
                             'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid) AS first_open)
  SELECT f.memory_id
    FROM facts f, w, u
   WHERE f.namespace_id = p_ns AND f.memory_id > w.wm
     AND f.memory_id < u.first_open
     AND f.ins_seq < engram_seq_floor(now())
   ORDER BY f.memory_id DESC
   LIMIT 1;
$$;

-- Purge gate (H-17, N119, P-15): the row purge of a document starts only after every REGISTERED index/Kafka
-- consumer has passed the DocumentDeleted event of its tombstone. The tombstone stores that event's seq
-- (the marker transaction drew it), so the gate is one comparison with the cursors. Unregistered consumers
-- (Kafka off) have no cursor row.
CREATE FUNCTION engram_consumers_passed(p_event_seq bigint) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT NOT EXISTS (
    SELECT 1 FROM outbox_cursors c
     WHERE c.consumer IN ('index', 'kafka') AND c.last_seq < p_event_seq);
$$;

-- ---- Move helpers (P-8, N124) ------------------------------------------------------------------------
-- <cols> of the bulk copy and the reconcile: generated and dropped columns are skipped, order is by name
-- so both shards produce the same list; no generated column exists in this schema, which is what the
-- CI check pins. Plan refuses unless engram_column_hash(table) is equal on source and target.
CREATE FUNCTION engram_copy_columns(p_table regclass) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attname)
    FROM pg_attribute a
   WHERE a.attrelid = p_table AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '';
$$;

CREATE FUNCTION engram_column_hash(p_table regclass) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT md5(string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod), ',' ORDER BY a.attname))
    FROM pg_attribute a
   WHERE a.attrelid = p_table AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '';
$$;

-- Move cleanup (N93, N91): deletes the data rows of ONE namespace on this shard in bounded batches
-- and NEVER touches the namespace_ownership row (a moved_out row is a permanent fence value).
-- SECURITY DEFINER, owned by engram_migrate: engram_move has no DELETE on source data and cannot
-- bypass RLS, so this is the only way a move frees the source. EXECUTE is granted to engram_move and
-- engram_admin only. It refuses unless the ownership row is moved_out (source cleanup after cutover
-- (c)) or incoming (target rollback). Each call deletes at most p_batch rows from the first
-- non-empty table in FK order and returns the count; 0 means done. The outer DELETE carries
-- namespace_id (P-19). Run engram_hnsw_ddl(..., 'drop') FIRST: the namespace's partial indexes are
-- dropped with DROP INDEX CONCURRENTLY, so no HNSW graph is repaired row by row (N112).
-- blob_tombstones, outbox, outbox_skipped and deletion_log are not touched (worklists and history). The
-- *_version_meta rows are deleted before their versions (their FK does not cascade, N136).
CREATE FUNCTION engram_cleanup_namespace(p_ns uuid, p_batch integer DEFAULT 10000) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  v_state ownership_state;
  v_tbl   text;
  v_n     bigint;
  c_order constant text[] := ARRAY[
    'observation_version_vectors', 'fact_vectors', 'chunk_vectors', 'page_version_vectors',
    'derived_hidden', 'expunge_progress', 'curation_log', 'fact_hidden', 'chunk_tombstones', 'document_tombstones',
    'page_version_inputs', 'page_version_meta', 'page_sources', 'page_versions', 'pages',
    'observation_version_sources', 'observation_inputs', 'observation_version_meta', 'observation_sources',
    'fact_consolidation', 'consolidation_state', 'consolidation_applied', 'consolidation_proposals',
    'consolidation_batches', 'observation_versions', 'observations',
    'entity_mentions', 'fact_links', 'facts', 'entity_aliases', 'entities',
    'document_version_chunks', 'chunks', 'document_versions', 'ingest_ledger', 'documents',
    'export_snapshots', 'token_usage_events', 'token_usage', 'quota_counters', 'batch_jobs',
    'idempotency_keys', 'operations', 'vector_indexes', 'namespace_models', 'namespace_stats'];
BEGIN
  PERFORM set_config('engram.namespace_id', p_ns::text, true);   -- ns_isolation passes even without BYPASSRLS
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

-- VerifyFK (N88): orphan count per foreign key of the namespace-scoped tables, run by the mover on
-- the target after the reconcile. Every row must be 0 before cutover. Generated from pg_constraint,
-- so a new foreign key is covered without editing the mover. Run with the namespace in scope.
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
       AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = k.conrelid AND a.attname = 'namespace_id' AND NOT a.attisdropped)
     ORDER BY k.conrelid::regclass::text, k.conname
  LOOP
    SELECT string_agg(format('p.%I = ch.%I', pa.attname, ca.attname), ' AND ' ORDER BY u.ord),
           string_agg(format('ch.%I IS NOT NULL', ca.attname), ' AND ' ORDER BY u.ord)
      INTO v_join, v_nn
      FROM unnest(c.conkey, c.confkey) WITH ORDINALITY AS u (ck, pk, ord)
      JOIN pg_attribute ca ON ca.attrelid = c.conrelid  AND ca.attnum = u.ck
      JOIN pg_attribute pa ON pa.attrelid = c.confrelid AND pa.attnum = u.pk;
    EXECUTE format('SELECT count(*) FROM %s ch WHERE ch.namespace_id = $1 AND %s AND NOT EXISTS (SELECT 1 FROM %s p WHERE %s)',
                   c.conrelid::regclass, v_nn, c.confrelid::regclass, v_join)
       INTO v_n USING p_ns;
    constraint_name := c.conname;
    child_table := c.conrelid::regclass::text;
    orphans := v_n;
    RETURN NEXT;
  END LOOP;
END $$;

-- Predicate of the RESTRICTIVE move policies below: is the namespace in scope the target of a move
-- right now? Evaluated once per statement as an InitPlan.
CREATE FUNCTION engram_ns_is_incoming() RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM namespace_ownership o
                  WHERE o.namespace_id = current_setting('engram.namespace_id')::uuid AND o.state = 'incoming');
$$;

-- =============================================================================
-- Index procedures (N112, N138, N78/P-10). Index DDL on partitioned tables cannot be written once as static
-- DDL: CREATE INDEX CONCURRENTLY cannot run in a transaction (so not inside a function) and cannot be
-- issued on a partitioned parent. These functions GENERATE the statements. The INDEX RUNNER (`engramctl
-- index`, control host, role engram_migrate: the one owner of index DDL, N138) executes them one at a time,
-- each as its own top-level statement, serialised per shard with maintenance_work_mem = 2.4 KB x vectors
-- (<= 5 GB) and shm_size = 8g, refuses to start a build while any backend_xmin is older than 5 min
-- (engram_old_snapshots), and records progress in vector_indexes.
-- =============================================================================

-- 1. Per-namespace partial HNSW. A hash-partitioned table keeps all rows of a namespace in exactly ONE
-- partition (page_version_vectors is not partitioned and is indexed on the table itself), so a namespace
-- owns one index per (vector table, embedding model): <= 120 namespaces x 4 tables = <= 480 small indexes
-- per shard. Queries that must match the partial-index predicate carry the literal namespace id and model
-- (plan_cache_mode = force_custom_plan, so the bound parameters are constants at plan time).
-- p_action = 'create' | 'drop'. Names are deterministic, so rollback_target and namespace delete drop by
-- name and orphans cannot hide. 'create' is a plain CREATE INDEX CONCURRENTLY, NEVER IF NOT EXISTS: the
-- runner reads index_state first and acts on it ('invalid' -> run the drop statement, then create;
-- 'valid' -> nothing to build), because a retry that merely skips an existing INVALID index leaves the
-- namespace on the exact path indefinitely (P-8).
CREATE FUNCTION engram_hnsw_ddl(p_table text, p_ns uuid, p_model text, p_action text DEFAULT 'create')
RETURNS TABLE (partition_name text, index_name text, index_state text, statement text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_parent regclass;
  v_kind   "char";
  v_abbr   text;
  v_rem    integer;
  v_part   text;
BEGIN
  IF p_table NOT IN ('fact_vectors', 'chunk_vectors', 'observation_version_vectors', 'page_version_vectors') THEN
    RAISE EXCEPTION 'not a vector table: %', p_table USING ERRCODE = '22023';
  END IF;
  IF p_action NOT IN ('create', 'drop') THEN
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
      statement := CASE p_action
        WHEN 'create' THEN format('CREATE INDEX CONCURRENTLY %I ON %I USING hnsw (embedding halfvec_cosine_ops) '
                                  'WITH (m = 16, ef_construction = 128) WHERE namespace_id = %L AND embedding_model = %L',
                                  index_name, v_part, p_ns, p_model)
        ELSE format('DROP INDEX CONCURRENTLY IF EXISTS %I', index_name) END;
      RETURN NEXT;
    END IF;
  END LOOP;
END $$;

-- What the stats sweeper should do for one namespace: request an index at p_create_at vectors of the
-- namespace's CURRENT model (default 2,000; below it the arm does an exact scan of <= 2,000 rows, ~3.2 MB)
-- when no row exists or the last build FAILED, drop below p_drop_below (hysteresis). A requested/building
-- row is the runner's: its lease, not this plan, decides a retry. Run as engram_admin.
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
           FROM observation_version_vectors v, cur WHERE v.namespace_id = p_ns AND v.embedding_model = cur.embedding_model
         UNION ALL
         SELECT 'page_version_vectors', count(*)
           FROM page_version_vectors v, cur WHERE v.namespace_id = p_ns AND v.embedding_model = cur.embedding_model)
  SELECT n.t, cur.embedding_model, n.c,
         coalesce(i.state = 'ready', false),
         CASE WHEN n.c >= p_create_at AND (i.vector_table IS NULL OR i.state = 'failed') THEN 'create'
              WHEN n.c <  p_drop_below AND i.state IN ('ready', 'failed') THEN 'drop'
              ELSE 'none' END
    FROM n CROSS JOIN cur
    LEFT JOIN vector_indexes i ON i.namespace_id = p_ns AND i.vector_table = n.t AND i.embedding_model = cur.embedding_model;
$$;

-- The index runner's work queue: requested builds, requested drops, and builds whose lease expired.
CREATE VIEW engram_index_runner_queue AS
  SELECT namespace_id, vector_table, embedding_model, state, requested_at, lease_until
    FROM vector_indexes
   WHERE state IN ('requested', 'dropping') OR (state = 'building' AND lease_until < now());

-- Hygiene trigger (N138): a touched HNSW is rebuilt when the purge has removed 1 % of the elements it was
-- built over or 2 k elements, whichever comes first, then VACUUM (INDEX_CLEANUP ON). Vector partitions carry
-- vacuum_index_cleanup = off, so autovacuum never repairs a graph (repair costs 5 to 6 times a rebuild).
CREATE VIEW engram_index_hygiene_due AS
  SELECT namespace_id, vector_table, embedding_model, rows_at_build, purged_since_build
    FROM vector_indexes
   WHERE state = 'ready'
     AND purged_since_build >= least(2000, greatest(1, ceil(rows_at_build * 0.01)));

-- A build must not start while any backend holds an old snapshot (the index build waits for them all and
-- blocks every other build behind it): the runner refuses while this view is non-empty. Needs
-- pg_read_all_stats for the runner's role to see other roles' backends.
CREATE VIEW engram_old_snapshots AS
  SELECT pid, usename, backend_xmin, xact_start
    FROM pg_stat_activity
   WHERE backend_xmin IS NOT NULL AND coalesce(xact_start, query_start) < now() - interval '5 minutes'
     AND pid <> pg_backend_pid();

-- 2. Ordinary index added to a partitioned table later (P-10): CREATE INDEX ... ON ONLY the parent
-- (created INVALID, nothing is built), then per partition CREATE INDEX CONCURRENTLY and ATTACH it;
-- the parent becomes valid when the last partition is attached. Returns the statements in order.
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

-- A failed CREATE INDEX CONCURRENTLY leaves an INVALID index; engramctl index drops and rebuilds it.
CREATE VIEW engram_invalid_indexes AS
  SELECT c.relname AS index_name, t.relname AS table_name
    FROM pg_index x
    JOIN pg_class c ON c.oid = x.indexrelid
    JOIN pg_class t ON t.oid = x.indrelid
   WHERE NOT x.indisvalid AND c.relnamespace = 'public'::regnamespace;

-- =============================================================================
-- Partitions: 16 hash partitions for the eight big tables (facts, chunks, links, mentions, observation
-- versions and the three partitioned vector tables). Storage parameters must be set per partition (a
-- partitioned parent cannot carry them). Content is insert-only, so fillfactor = 100 everywhere; the freeze
-- age is lowered (N114: vacuum_freeze_min_age = 10 M on content partitions) and insert-triggered
-- autovacuum keeps the visibility map and freezing current; the delete-oriented scale factors serve
-- the expunge's purge batches. Vector partitions carry vacuum_index_cleanup = off PERMANENTLY (N138): a
-- touched graph is rebuilt by the index runner, never repaired by autovacuum.
-- =============================================================================
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

-- =============================================================================
-- Table classes (N113, N137). The ONE list: COMMENT ON TABLE 'class: <tag>' on every table (the parent of a
-- partitioned table). engramlint sql checks it, the self-check at the end of this file enforces it, the
-- (namespace_id, ins_seq) indexes below are created from it, and N113, N124 and section 3 render from it.
-- =============================================================================
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

-- =============================================================================
-- Indexes on the partitioned tables (created on the parent, propagated to every partition; later
-- additions follow engram_partitioned_index_ddl). There is NO shared vector index and no partial
-- WHERE live index: vector indexes are per namespace (engram_hnsw_ddl) and created by the stats
-- sweeper at 2,000 vectors; below that the arms scan exactly through the *_model_idx btrees.
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
CREATE INDEX facts_occurred_idx     ON facts (namespace_id, occurred_start)   -- temporal arm: two-sided probe around query_timestamp (N68)
  WHERE occurred_start IS NOT NULL;
CREATE INDEX facts_occurred_gist    ON facts                                   -- explicit occurrence-window filters (lists, Recall filters), not the arm
  USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
  WHERE occurred_start IS NOT NULL;
-- BM25 over the immutable text: no live/tag columns, so the index is never updated in place. Hidden
-- hits (markers) are removed by the arm's visibility join, not by the index; the arm over-fetches.
-- pg_search:begin
CREATE INDEX facts_bm25 ON facts
  USING bm25 (memory_id, (text::pdb.unicode_words), namespace_id, mentioned_at)
  WITH (key_field = 'memory_id');
-- pg_search:end

-- vector side tables: exact-scan path (a namespace below 2,000 vectors reads <= 2,000 rows through
-- this btree and sorts by distance). The expunge deletes vectors through the FK cascade from facts.
CREATE INDEX fact_vectors_model_idx  ON fact_vectors  (namespace_id, embedding_model, memory_id);
CREATE INDEX chunk_vectors_model_idx ON chunk_vectors (namespace_id, embedding_model, chunk_id);
CREATE INDEX observation_version_vectors_model_idx
  ON observation_version_vectors (namespace_id, embedding_model, observation_id, version);
CREATE INDEX page_version_vectors_model_idx ON page_version_vectors (namespace_id, embedding_model, pv_id);
-- No (namespace_id, document_id) index on any vector table (N138): the exact path is driven from
-- facts_doc_idx / facts_mentioned_idx and joins the vector table by primary key.

-- fact_links: the PK serves forward expansion; the reverse index serves the other direction and
-- the FK cascade of the purge
CREATE INDEX fact_links_reverse_idx ON fact_links (namespace_id, dst_memory_id, src_memory_id);

-- entity_mentions
CREATE INDEX entity_mentions_entity_idx ON entity_mentions (namespace_id, entity_id, mentioned_at);

-- observation_versions: hash-partitioned like facts; ~1/20 of facts
CREATE INDEX observation_versions_effective_idx ON observation_versions (namespace_id, effective_at);
-- pg_search:begin
CREATE INDEX observation_versions_bm25 ON observation_versions
  USING bm25 (ov_id, (text::pdb.unicode_words), namespace_id, observation_id, effective_at)
  WITH (key_field = 'ov_id');
-- pg_search:end

-- pages: BM25 over the page text (a purged version is a stub with text = '')
-- pg_search:begin
CREATE INDEX page_versions_bm25 ON page_versions
  USING bm25 (pv_id, (text::pdb.unicode_words), namespace_id, page_id, effective_at)
  WITH (key_field = 'pv_id');
-- pg_search:end

-- The re-copy key of the move (N137): every insert-only table has (namespace_id, ins_seq) (about 5 % of the
-- footprint). Created from the class tags, so a new insert-only table cannot miss it.
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

-- =============================================================================
-- Insert-only enforcement (N113): BEFORE UPDATE is refused on every content table. The grants below
-- give no UPDATE privilege to the application roles either; this is the second wall.
-- =============================================================================
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

-- =============================================================================
-- Row-Level Security (D2): policy ns_isolation on every table that has a namespace_id column,
-- parents and partitions alike, ENABLEd and FORCEd (the section 8 --check-rls test asserts
-- both). Partitions carry the policy too, so a direct partition reference (which no
-- non-admin role is granted) is still confined. No tag predicate runs under RLS (N116).
-- =============================================================================
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

-- engram_move writes (INSERT/UPDATE/DELETE) only into a namespace whose ownership row is
-- 'incoming', i.e. on the TARGET before (b'). RESTRICTIVE policies are ANDed with ns_isolation and
-- apply to engram_move only. namespace_ownership has its own trigger/state machine.
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

-- Relay role: all-namespace SELECT on outbox only (D6/N4).
CREATE POLICY relay_read_all ON outbox FOR SELECT TO engram_relay USING (true);

-- =============================================================================
-- Grants. Only parent tables are granted: partitions stay ungranted, so a direct partition
-- reference by engram_app/engram_move fails with permission denied (a query through the
-- parent checks parent privileges only).
-- =============================================================================
GRANT USAGE ON SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
GRANT USAGE ON SEQUENCE outbox_seq, engram_ins_seq TO engram_app, engram_move, engram_admin;   -- ins_seq is a column default
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
-- N93: the SECURITY DEFINER cleanup is callable by the move executor and admin only; the index and
-- expunge helpers that read shard-wide state are admin-only too.
REVOKE EXECUTE ON FUNCTION engram_cleanup_namespace(uuid, integer) FROM PUBLIC, engram_app, engram_relay;
REVOKE EXECUTE ON FUNCTION engram_consumers_passed(bigint) FROM PUBLIC, engram_app, engram_relay, engram_move;
-- P-9: the relay and the move executor must not reach the definer entity lookup (it would read any namespace's
-- entity names by setting the scope GUC); the sequence ring and the scheduler samplers are admin-only too.
REVOKE EXECUTE ON FUNCTION engram_entity_fuzzy(uuid, text, integer, real) FROM PUBLIC, engram_relay, engram_move;
REVOKE EXECUTE ON FUNCTION engram_seq_sample() FROM PUBLIC, engram_app, engram_relay, engram_move;
GRANT SELECT ON shard_meta, ownership_transitions TO engram_app, engram_relay, engram_move, engram_admin;

-- engram_app: ordinary namespace transactions. Content: SELECT + INSERT only (no UPDATE, no DELETE;
-- the purge belongs to engram_admin). Marker tables: the verbs the marker transactions use.
GRANT SELECT, INSERT ON
  ingest_ledger, document_version_chunks, chunks, facts, fact_links, entity_mentions,
  observation_versions, observation_inputs, observation_version_sources, observation_version_meta,
  page_versions, page_version_meta, page_version_inputs,
  fact_vectors, chunk_vectors, observation_version_vectors, page_version_vectors,
  fact_consolidation, consolidation_proposals, consolidation_applied, token_usage_events, curation_log,
  deletion_log, outbox
  TO engram_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  documents, document_versions, entities, entity_aliases,
  observations, observation_sources, consolidation_batches, consolidation_state, batch_jobs,
  pages, page_sources, operations, idempotency_keys, token_usage, quota_counters,
  blob_tombstones, export_snapshots
  TO engram_app;
GRANT SELECT, INSERT, UPDATE ON namespace_stats, namespace_models TO engram_app;
GRANT SELECT, INSERT ON document_tombstones TO engram_app;                 -- Delete(document); the Expunge's state changes are engram_admin's
GRANT SELECT, INSERT, DELETE ON chunk_tombstones, fact_hidden TO engram_app;   -- REPLACE / un-retire; Invalidate / Restore (cause 'invalidate' only; FinalizeVersion inserts 'reextract')
GRANT SELECT ON document_tombstone_view TO engram_app;                    -- the tombstone view of a DELETING document (N136)
GRANT SELECT, DELETE ON derived_hidden TO engram_app;                      -- Restore removes the rows with cause (invalidation, f)
GRANT SELECT ON expunge_progress, vector_indexes TO engram_app;
GRANT SELECT ON namespace_ownership TO engram_app;                         -- fence read: plain SELECT under the shared advisory lock (no row lock)
GRANT INSERT ON namespace_ownership TO engram_app;                         -- CreateNamespace's first 'active'/epoch-1 row (the trigger admits nothing else from this role)
GRANT UPDATE (state, freeze_reason) ON namespace_ownership TO engram_app;  -- the delete freeze (active -> frozen/delete) is the only edge the trigger admits for this role

-- engram_relay: the outbox stream and its own cursor rows; nothing else (D6)
GRANT SELECT ON outbox TO engram_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox_cursors TO engram_relay;

-- engram_move: DML grants are table-level; the TARGET-ONLY restriction is the RESTRICTIVE policies
-- move_target_*. Content tables: INSERT only (immutable rows are loaded ON CONFLICT DO NOTHING and
-- the reconcile re-copies missing rows; it never deletes content, rollback uses
-- engram_cleanup_namespace). Mutable tables and markers: full DML (the reconcile merges and deletes).
-- The mover READS outbox and outbox_cursors (relay drain, N124) and writes neither.
GRANT SELECT, INSERT ON
  ingest_ledger, document_version_chunks, chunks, facts, fact_links, entity_mentions,
  observation_versions, observation_inputs, observation_version_sources, observation_version_meta,
  page_versions, page_version_meta, page_version_inputs,
  fact_vectors, chunk_vectors, observation_version_vectors, page_version_vectors,
  fact_consolidation, consolidation_proposals, consolidation_applied, token_usage_events, curation_log,
  deletion_log
  TO engram_move;                       -- outbox_skipped is shard-local (its seqs mean nothing on the target) and is not copied
GRANT SELECT, INSERT, UPDATE, DELETE ON
  namespace_ownership, namespace_stats, namespace_models, vector_indexes,
  documents, document_versions, entities, entity_aliases,
  observations, observation_sources, consolidation_batches, consolidation_state, batch_jobs,
  pages, page_sources, operations, idempotency_keys, token_usage, quota_counters,
  blob_tombstones, export_snapshots,
  document_tombstones, chunk_tombstones, fact_hidden, derived_hidden, expunge_progress
  TO engram_move;
GRANT SELECT ON outbox, outbox_cursors, engram_seq_log TO engram_move;   -- engram_seq_floor is evaluated as engram_move on the source
-- N91: the loader switches off FK, insert-only and touch triggers for its own session only.
-- The ownership trigger is ENABLE ALWAYS and RLS is unaffected by replica mode.
GRANT SET ON PARAMETER session_replication_role TO engram_move;
GRANT EXECUTE ON FUNCTION engram_cleanup_namespace(uuid, integer) TO engram_move, engram_admin;

-- engram_admin: engramctl, Expunge purge, schedulers, retention. The only role that DELETEs content.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO engram_admin;
REVOKE INSERT, UPDATE, DELETE ON ownership_transitions FROM engram_admin;   -- the machine is data nobody edits at run time
GRANT SELECT ON schedulable_namespaces, purgeable_namespaces, engram_invalid_indexes,
  engram_index_runner_queue, engram_index_hygiene_due, engram_old_snapshots, document_tombstone_view TO engram_admin;
GRANT EXECUTE ON FUNCTION engram_consumers_passed(bigint), engram_seq_sample() TO engram_admin;

-- =============================================================================
-- Self-checks: fail the migration if an invariant of this file is violated.
-- =============================================================================
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
     AND c.relname NOT IN ('shard_meta', 'outbox_cursors', 'ownership_transitions', 'engram_seq_log', 'goose_db_version');
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
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('engram_app', 'engram_relay', 'engram_move') AND rolbypassrls) THEN
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
     AND (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname LIKE 'move_target_%' AND NOT p.polpermissive) <> 3;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without the move_target_* restrictive policies: %', missing;
  END IF;

  -- 9. the cleanup and entity-lookup functions are SECURITY DEFINER; cleanup is not executable by app/relay
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'engram_cleanup_namespace' AND prosecdef)
     OR has_function_privilege('engram_app', 'engram_cleanup_namespace(uuid, integer)', 'EXECUTE')
     OR has_function_privilege('engram_relay', 'engram_cleanup_namespace(uuid, integer)', 'EXECUTE') THEN
    RAISE EXCEPTION 'engram_cleanup_namespace must be SECURITY DEFINER and executable by engram_move/engram_admin only (N93)';
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
                      'abort_move', 'thaw_move', 'cutover_c', 'restore_delete')) <> 9 THEN
    RAISE EXCEPTION 'ownership_transitions lacks cutover or rollback edges (N125)';
  END IF;

  -- 11. content tables are insert-only: no UPDATE/DELETE for the app roles, a BEFORE UPDATE guard, fillfactor 100
  SELECT string_agg(x.t, ', ') INTO missing
    FROM unnest(c_content) AS x(t)
   WHERE has_table_privilege('engram_app',  x.t, 'UPDATE') OR has_table_privilege('engram_app',  x.t, 'DELETE')
      OR has_table_privilege('engram_move', x.t, 'UPDATE') OR has_table_privilege('engram_move', x.t, 'DELETE')
      OR has_table_privilege('engram_relay', x.t, 'UPDATE') OR has_table_privilege('engram_relay', x.t, 'DELETE')
      OR NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = x.t::regclass AND (g.tgname = x.t || '_insert_only' OR g.tgname = 'ingest_ledger_append_only'))
      OR coalesce((SELECT 'fillfactor=100' = ANY (r.reloptions) FROM pg_class r WHERE r.oid = x.t::regclass), true) = false;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'content tables must be insert-only for non-admin roles: %', missing;
  END IF;

  -- 12. no generated column anywhere and no visibility column on a content table (N113); the ledger keeps the
  --     request's tags and the *_meta tables ARE the write-once superseded_at
  SELECT string_agg(c.relname || '.' || a.attname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
     AND (a.attgenerated <> ''
          OR (c.relname = ANY (c_content) AND c.relname NOT IN ('ingest_ledger', 'observation_version_meta', 'page_version_meta')
              AND a.attname IN ('live', 'retired_at', 'purge_after', 'invalidated_at',
              'invalidation_reason', 'superseded_at', 'tags', 'tag_count', 'stale_write', 'stale_delete',
              'derived_from_deleted', 'hidden_by_invalidation')));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'generated or visibility columns on content tables: %', missing;
  END IF;

  -- 13. the vector tables and their parents are hash-partitioned
  IF (SELECT count(*) FROM pg_class WHERE relname IN ('facts', 'chunks', 'fact_links', 'entity_mentions', 'observation_versions',
        'fact_vectors', 'chunk_vectors', 'observation_version_vectors') AND relkind = 'p' AND relnamespace = 'public'::regnamespace) <> 8 THEN
    RAISE EXCEPTION 'the eight big tables must be hash-partitioned';
  END IF;

  -- 14. every table carries exactly one class tag (N137); only insert-only tables carry ins_seq, each with its
  --     (namespace_id, ins_seq) index; the content tables are all insert-only
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
     AND c.relname <> 'goose_db_version'
     AND coalesce(obj_description(c.oid, 'pg_class'), '') NOT IN
         ('class: insert-only', 'class: mutable', 'class: expiring', 'class: shard-local');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without a class tag: %', missing;
  END IF;
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p') AND NOT c.relispartition
     AND ((obj_description(c.oid, 'pg_class') = 'class: insert-only')
          <> EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'ins_seq' AND NOT a.attisdropped AND a.attnotnull AND a.atthasdef)
          OR (obj_description(c.oid, 'pg_class') = 'class: insert-only'
              AND NOT EXISTS (SELECT 1 FROM pg_index x
                               WHERE x.indrelid = c.oid
                                 AND (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum = x.indkey[0]) = 'namespace_id'
                                 AND (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum = x.indkey[1]) = 'ins_seq')));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'ins_seq and its (namespace_id, ins_seq) index must exist exactly on insert-only tables: %', missing;
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

  -- 16. fact_hidden is keyed by cause (N135); proposals and applied ops are never updated or deleted by the app role (N43)
  IF NOT EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = ANY (k.conkey)
                  WHERE k.conrelid = 'fact_hidden'::regclass AND k.contype = 'p' AND a.attname = 'cause') THEN
    RAISE EXCEPTION 'fact_hidden primary key must include cause (N135)';
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
         AND 'statement_timeout=30s' = ANY (s.setconfig) AND 'idle_in_transaction_session_timeout=30s' = ANY (s.setconfig)) <> 3 THEN
    RAISE EXCEPTION 'engram_app, engram_move and engram_admin need 30 s statement and idle timeouts (N133e)';
  END IF;

  -- 18. every role commits locally (N122): no synchronous standby wait anywhere
  IF (SELECT count(*) FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole
       WHERE r.rolname IN ('engram_app', 'engram_move', 'engram_admin', 'engram_relay')
         AND 'synchronous_commit=local' = ANY (s.setconfig)) <> 4 THEN
    RAISE EXCEPTION 'every engram role must have synchronous_commit = local (N122)';
  END IF;
END $$;
