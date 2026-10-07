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
-- Write-transaction protocol (D6 invariant A-F1, model-checked in section 7): writers run with
-- statement_timeout = idle_in_transaction_session_timeout = 30 s and the outbox INSERT is the
-- LAST statement before COMMIT, so a drawn outbox.seq is committed or aborted within one
-- timeout of being drawn; the relay's 60 s gap watchlist and the move's copy barrier
-- (D5 step 2) both rely on it.
--
-- Roles (LOGIN; passwords are set by provisioning from the shard secret, section 9):
--   engram_migrate  owner of every object; DDL only (BYPASSRLS so data migrations and the
--                   SECURITY DEFINER cleanup function engram_cleanup_namespace can run)
--   engram_app      API + worker per-namespace transactions; NOBYPASSRLS; role defaults
--                   lock_timeout = 2 s, statement_timeout = 30 s (N82)
--   engram_relay    outbox relay; NOBYPASSRLS; one extra policy grants all-namespace SELECT on
--                   outbox only (D6/N4; the deletion-log consumer reads nothing but events);
--                   owns outbox_cursors; reads nothing else
--   engram_move     move executor (N2, N91, N103 item 5); NOBYPASSRLS, confined by the ordinary
--                   ns_isolation policy to the namespace in scope. On the TARGET it has DML on the
--                   namespace-scoped tables, enforced by the RESTRICTIVE policies move_target_*:
--                   a write passes only while the namespace's ownership row is 'incoming'. On the
--                   SOURCE it has SELECT, the ownership transitions of the state machine (freeze,
--                   cutover (c)) and INSERT/UPDATE on move_applied and outbox_cursors; it cannot
--                   DELETE source data (cleanup runs through engram_cleanup_namespace, N93).
--                   Bulk load (N91, replaces the BYPASSRLS loader role of N51, which no longer
--                   exists): COPY each range into a session TEMP table, then INSERT ... SELECT ...
--                   ON CONFLICT under RLS, with SET LOCAL session_replication_role = replica
--                   (GRANT SET ON PARAMETER, PG 15+). Role defaults lock_timeout = 10 s,
--                   idle_in_transaction_session_timeout = 10 min (N89)
--   engram_admin    engramctl, purge workflows, shard-wide schedulers; BYPASSRLS;
--                   lock_timeout = 10 s
--
-- Ownership fence (D2 row 3 as amended by F-5, N82, N83 and N103; the state machine is in
-- section 3.3.1 and is DATA here: the table ownership_transitions):
--   the namespace_ownership ROW is the fence VALUE, a heavyweight advisory lock is the fence
--   LOCK. Writers take pg_try_advisory_xact_lock_shared(k1, k2) over engram_ns_lock_keys(ns); a
--   refused try-lock ends the statement at once with the retryable NamespaceFrozen{retry_after
--   200 ms}, so no pooled connection waits behind a queued freeze (N82). Then a plain SELECT
--   state, epoch, freeze_reason FROM namespace_ownership (no row lock); proceed only when
--   state = 'active' AND epoch = $epoch. Exclusive takers (copy barrier, freeze, cutover,
--   restore, delete freeze) use pg_advisory_xact_lock or the session-level variant on a direct
--   connection, under lock_timeout = 5 s per attempt with jittered retry. Exports take NO
--   exclusive fence (REPEATABLE READ + pg_current_snapshot, N82). Readers take no lock and check
--   state only (N64). Per-document work uses engram_doc_lock_keys(ns, doc) (N83): CommitChunk
--   takes it shared with try-lock semantics (failure -> retryable DocumentBusy), FinalizeVersion,
--   the delete cascade and PurgeDocument take it exclusive. FOR SHARE is used on NO row of
--   namespace_ownership, documents or document_versions (compatible row lockers bypass a
--   waiting FOR UPDATE and churn multixacts).
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
  -- N91: the BYPASSRLS loader role engram_move_load of N51 is gone. A cluster that still has it
  -- must drop it (DROP OWNED BY engram_move_load; DROP ROLE engram_move_load) before this file runs.
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_move_load') THEN
    RAISE EXCEPTION 'legacy role engram_move_load exists; N91 removed it (drop it first)';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_admin') THEN
    CREATE ROLE engram_admin LOGIN BYPASSRLS;
  END IF;
END $$;

-- Role defaults (N82, N89). Row and advisory waits end after lock_timeout; statement_timeout
-- stays the outer bound. engram_move's source session is exempt from the 30 s idle limit because
-- the target side of each copy range commits between source reads (N89).
ALTER ROLE engram_app   SET lock_timeout = '2s';
ALTER ROLE engram_app   SET statement_timeout = '30s';
ALTER ROLE engram_app   SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE engram_move  SET lock_timeout = '10s';
ALTER ROLE engram_move  SET idle_in_transaction_session_timeout = '10min';
ALTER ROLE engram_admin SET lock_timeout = '10s';

-- -----------------------------------------------------------------------------
-- Enumerations (closed sets; extended with ALTER TYPE ... ADD VALUE in a migration)
-- -----------------------------------------------------------------------------
CREATE TYPE fact_type        AS ENUM ('world', 'experience');
CREATE TYPE link_type        AS ENUM ('entity', 'temporal', 'semantic', 'causal');
CREATE TYPE ownership_state  AS ENUM ('incoming', 'active', 'frozen', 'moved_out');
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

-- Advisory-lock keys (N103 item 9: ONE key form, two int4 keys, used with the two-argument
-- advisory-lock functions). Every party that fences on a namespace derives the keys from
-- engram_ns_lock_keys so writers (shared try-lock, N82), the move's copy barrier / freeze /
-- cutover, the delete freeze and the restore (exclusive) meet on the same lock; per-document
-- work (N83) uses engram_doc_lock_keys. A hash collision only over-serialises two namespaces or
-- documents, never under-fences one. IMMUTABLE so the call is inlined.
CREATE FUNCTION engram_ns_lock_keys(ns uuid) RETURNS TABLE (k1 integer, k2 integer)
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT hashtext(ns::text), 0;
$$;

CREATE FUNCTION engram_doc_lock_keys(ns uuid, doc text) RETURNS TABLE (k1 integer, k2 integer)
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT hashtext(ns::text), hashtext(doc);
$$;

-- The writers' fence acquisition (N82): never waits. false -> the caller ends the statement with
-- the retryable NamespaceFrozen{retry_after = 200 ms}.
CREATE FUNCTION engram_try_ns_fence(ns uuid) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$
  SELECT pg_try_advisory_xact_lock_shared(k.k1, k.k2) FROM engram_ns_lock_keys(ns) AS k;
$$;

-- CommitChunk's per-document lock (N83): shared try-lock; false -> retryable DocumentBusy (100 ms).
CREATE FUNCTION engram_try_doc_lock_shared(ns uuid, doc text) RETURNS boolean
LANGUAGE sql VOLATILE STRICT AS $$
  SELECT pg_try_advisory_xact_lock_shared(k.k1, k.k2) FROM engram_doc_lock_keys(ns, doc) AS k;
$$;

-- Tag grammar: <= 32 tags, each ^[a-z0-9][a-z0-9._:/-]{0,63}$, strictly ascending in "C"
-- collation (= sorted and deduplicated bytewise, exactly what the Go normaliser emits).
-- The set semantics of the five tag modes (D10) rely on this.
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

-- N81: the move replay writes mutable tables with ON CONFLICT DO UPDATE ... updated_at =
-- EXCLUDED.updated_at, so the trigger must leave the source's timestamp alone. The bypass needs
-- ALL of: role engram_move, SET LOCAL engram.replay = 'on', and a target row in state 'incoming'
-- (a client cannot satisfy the last two: engram_app never sees an incoming namespace).
CREATE FUNCTION engram_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF current_user = 'engram_move'
     AND coalesce(current_setting('engram.replay', true), '') = 'on'
     AND EXISTS (SELECT 1 FROM namespace_ownership o
                  WHERE o.namespace_id = NEW.namespace_id AND o.state = 'incoming') THEN
    RETURN NEW;
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- ingest_ledger is append-only. UPDATE is never allowed. DELETE is allowed only to engram_admin
-- (explicit document, namespace or tenant delete; never the retire-purge of superseded versions,
-- N104) and to the table owner, which is the SECURITY DEFINER cleanup function
-- engram_cleanup_namespace after a move (N93). engram_move can no longer delete ledger rows. Every
-- other role gets 42501.
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
-- the edges of ownership_transitions are accepted (N64, N93, N98, N101):
--   * same-state UPDATEs that change anything but move_applied_seq / updated_at are edges too
--     (start_move, abort_move, epoch_bump); a pure touch is allowed to engram_move/engram_admin;
--   * freeze_reason and epoch change only in an explicit edge, each with its role;
--   * moved_out and active rows are never deleted (the fence value a late writer or stale reader
--     must still hit, WrongShardOrEpoch{MOVED_OUT}, N52); cleanup never removes them (N93);
--   * moved_out -> incoming additionally requires that no data rows remain (the cleanup ran).
-- Everything else is 23514 / 42501. The trigger is ENABLE ALWAYS (N91): session_replication_role
-- = replica, which the move loader sets, must not switch it off.
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

  -- pure touch: nothing but move_applied_seq / updated_at changes
  IF (NEW.state, NEW.freeze_reason, NEW.epoch, NEW.move_id, NEW.move_epoch, NEW.target_shard_id, NEW.target_epoch)
     IS NOT DISTINCT FROM
     (OLD.state, OLD.freeze_reason, OLD.epoch, OLD.move_id, OLD.move_epoch, OLD.target_shard_id, OLD.target_epoch) THEN
    IF current_user NOT IN ('engram_move', 'engram_admin') THEN
      RAISE EXCEPTION 'role % may not touch an ownership row', current_user USING ERRCODE = '42501';
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
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

-- Evidence triggers (D12, N41 as amended by review F-1/F-9/F-11, N57, N61). Both are
-- statement-level AFTER DELETE triggers with a transition table. "Lost evidence" means the row
-- is gone AND the fact it named is no longer live (retired, invalidated or physically purged):
-- the delete cascade retires the facts BEFORE it deletes sources/inputs (section 3.8, order is
-- load-bearing) and the purge removes the fact row itself (FK cascade), so both paths qualify;
-- the consolidation apply path (N57: insert the new rows, then delete the stale ones, whose
-- facts are live) never does, so the triggers are robust to statement order inside an update
-- and never retire or hide an observation that is being rewritten. The application emits the
-- matching outbox events after reading the affected rows back; the triggers are the invariant,
-- not the event source. stale_write (new evidence under a visible observation) is set by
-- FinalizeVersion / consolidation apply, never here.
--
-- Sources: observations never outlive their sources: zero sources left -> retire the
-- observation and every version (unconditional, whatever removed the rows). Otherwise a lost
-- source whose fact is no longer live marks the observation stale_delete ("needs a rewrite")
-- and mirrors the flag onto the CURRENT version, which hides that version until the rewrite
-- lands; superseded versions keep their as_of range.
CREATE FUNCTION engram_observation_sources_after_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE observations o
     SET retired_at = now(), stale_write = false, stale_delete = false, stale_since = NULL, updated_at = now()
   WHERE (o.namespace_id, o.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND o.retired_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM observation_sources s
                      WHERE s.namespace_id = o.namespace_id AND s.observation_id = o.observation_id);
  UPDATE observation_versions v
     SET retired_at = now()
   WHERE (v.namespace_id, v.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND v.retired_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM observation_sources s
                      WHERE s.namespace_id = v.namespace_id AND s.observation_id = v.observation_id);
  UPDATE observations o
     SET stale_delete = true, stale_since = coalesce(o.stale_since, now()), updated_at = now()
   WHERE (o.namespace_id, o.observation_id) IN (
           SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d
            WHERE NOT EXISTS (SELECT 1 FROM facts f
                               WHERE f.namespace_id = d.namespace_id AND f.memory_id = d.memory_id AND f.live))
     AND o.retired_at IS NULL
     AND o.stale_delete = false;
  UPDATE observation_versions v
     SET stale_delete = true
    FROM observations o
   WHERE o.namespace_id = v.namespace_id AND o.observation_id = v.observation_id
     AND o.current_version = v.version
     AND (o.namespace_id, o.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND o.stale_delete AND o.retired_at IS NULL
     AND v.stale_delete = false;
  RETURN NULL;
END $$;

-- Lineage walk (N79). Starting from the given (namespace, observation, version) roots, returns
-- the roots (depth 0) and every version reachable through observation_version_lineage child
-- edges, depth-bounded: rows with depth 1..p_max_depth are descendants to flag; rows with depth
-- p_max_depth + 1 are the FRONTIER, i.e. versions whose ancestry could not be explored further.
-- The callers fail closed on the frontier (over-hiding is safe). UNION (not UNION ALL) removes
-- repeated (version, depth) pairs of diamond-shaped lineage. plpgsql so the function can be
-- created before the lineage table exists.
CREATE FUNCTION engram_lineage_walk(p_ns uuid[], p_obs uuid[], p_ver integer[], p_max_depth integer DEFAULT 64)
RETURNS TABLE (ns uuid, obs uuid, ver integer, depth integer)
LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN QUERY
  WITH RECURSIVE walk (w_ns, w_obs, w_ver, w_depth) AS (
    SELECT r.a, r.b, r.c, 0 FROM unnest(p_ns, p_obs, p_ver) AS r (a, b, c)
    UNION
    SELECT l.namespace_id, l.observation_id, l.version, w.w_depth + 1
      FROM walk w
      JOIN observation_version_lineage l
        ON l.namespace_id = w.w_ns AND l.parent_observation_id = w.w_obs AND l.parent_version = w.w_ver
     WHERE w.w_depth <= p_max_depth
  )
  SELECT walk.w_ns, walk.w_obs, walk.w_ver, walk.w_depth FROM walk;
END $$;

-- Transitive derived_from_deleted (N79). Called with the versions that were just flagged
-- DIRECTLY (their inputs named a victim); flags every lineage descendant up to depth 64 in the
-- same transaction, hides the observations whose CURRENT version became flagged (stale_delete +
-- the version mirror), and fails closed on the frontier: each observation still on the frontier
-- at depth 65 gets stale_delete (its current version is hidden until a root rebuild). Returns the
-- number of frontier observations and adds both counts to the transaction-local counters
-- engram.lineage_flagged / engram.lineage_frontier, which the delete cascade copies into
-- deletion_log.details. T1 invariant: every version reachable by lineage from a flagged version
-- is flagged (or its observation is stale_delete when the walk was cut at depth 64).
CREATE FUNCTION engram_flag_lineage(p_ns uuid[], p_obs uuid[], p_ver integer[]) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  d_ns uuid[]; d_obs uuid[]; d_ver integer[];
  f_ns uuid[]; f_obs uuid[];
  n_ns uuid[]; n_obs uuid[]; n_ver integer[];
  s_ns uuid[]; s_obs uuid[];
  v_frontier integer;
  v_flagged  integer;
BEGIN
  SELECT array_agg(x.ns), array_agg(x.obs), array_agg(x.ver) INTO d_ns, d_obs, d_ver
    FROM (SELECT DISTINCT w.ns, w.obs, w.ver FROM engram_lineage_walk(p_ns, p_obs, p_ver, 64) w
           WHERE w.depth BETWEEN 1 AND 64) x;
  SELECT array_agg(x.ns), array_agg(x.obs) INTO f_ns, f_obs
    FROM (SELECT DISTINCT w.ns, w.obs FROM engram_lineage_walk(p_ns, p_obs, p_ver, 64) w
           WHERE w.depth = 65) x;

  WITH u AS (
    UPDATE observation_versions v SET derived_from_deleted = true
      FROM unnest(d_ns, d_obs, d_ver) AS d (a, b, c)
     WHERE v.namespace_id = d.a AND v.observation_id = d.b AND v.version = d.c
       AND NOT v.derived_from_deleted
     RETURNING v.namespace_id, v.observation_id, v.version)
  SELECT array_agg(u.namespace_id), array_agg(u.observation_id), array_agg(u.version) INTO n_ns, n_obs, n_ver FROM u;

  WITH o AS (
    UPDATE observations ob
       SET stale_delete = true, stale_since = coalesce(ob.stale_since, now()), updated_at = now()
     WHERE ob.retired_at IS NULL AND NOT ob.stale_delete
       AND ((ob.namespace_id, ob.observation_id, ob.current_version) IN (SELECT x.a, x.b, x.c FROM unnest(n_ns, n_obs, n_ver) AS x (a, b, c))
         OR (ob.namespace_id, ob.observation_id) IN (SELECT y.a, y.b FROM unnest(f_ns, f_obs) AS y (a, b)))
     RETURNING ob.namespace_id, ob.observation_id)
  SELECT array_agg(o.namespace_id), array_agg(o.observation_id) INTO s_ns, s_obs FROM o;

  UPDATE observation_versions v SET stale_delete = true
    FROM observations ob, unnest(s_ns, s_obs) AS s (a, b)
   WHERE ob.namespace_id = s.a AND ob.observation_id = s.b
     AND v.namespace_id = ob.namespace_id AND v.observation_id = ob.observation_id
     AND v.version = ob.current_version AND NOT v.stale_delete;

  v_frontier := coalesce(cardinality(f_ns), 0);
  v_flagged  := coalesce(cardinality(n_ns), 0);
  PERFORM set_config('engram.lineage_flagged',
    (coalesce(nullif(current_setting('engram.lineage_flagged', true), ''), '0')::integer + v_flagged)::text, true);
  PERFORM set_config('engram.lineage_frontier',
    (coalesce(nullif(current_setting('engram.lineage_frontier', true), ''), '0')::integer + v_frontier)::text, true);
  RETURN v_frontier;
END $$;

-- Inputs: a version whose observation_inputs named a fact that is no longer live was written
-- with deleted content in view. It is marked derived_from_deleted, PERMANENTLY and PER VERSION
-- (review F-1): it is never served again at any as_of, and reconsolidation never clears the
-- flag, it only writes a new version. Versions written without the victim keep their as_of
-- range. If the observation's CURRENT version is among the flagged ones the observation needs
-- a rewrite (stale_delete, mirrored onto that version as for sources). N79: the flag then
-- propagates along observation_version_lineage to every descendant version (the text of a
-- flagged version was shown to the model that wrote them), depth-bounded, failing closed.
CREATE FUNCTION engram_observation_inputs_after_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  r_ns uuid[]; r_obs uuid[]; r_ver integer[];
BEGIN
  WITH u AS (
    UPDATE observation_versions v
       SET derived_from_deleted = true
     WHERE (v.namespace_id, v.observation_id, v.version) IN (
             SELECT DISTINCT d.namespace_id, d.observation_id, d.version FROM deleted d
              WHERE NOT EXISTS (SELECT 1 FROM facts f
                                 WHERE f.namespace_id = d.namespace_id AND f.memory_id = d.fact_id AND f.live))
       AND v.derived_from_deleted = false
     RETURNING v.namespace_id, v.observation_id, v.version)
  SELECT array_agg(u.namespace_id), array_agg(u.observation_id), array_agg(u.version) INTO r_ns, r_obs, r_ver FROM u;
  UPDATE observations o
     SET stale_delete = true, stale_since = coalesce(o.stale_since, now()), updated_at = now()
    FROM observation_versions v
   WHERE v.namespace_id = o.namespace_id AND v.observation_id = o.observation_id
     AND v.version = o.current_version
     AND (v.namespace_id, v.observation_id, v.version) IN (SELECT DISTINCT d.namespace_id, d.observation_id, d.version FROM deleted d)
     AND v.derived_from_deleted
     AND o.retired_at IS NULL
     AND o.stale_delete = false;
  UPDATE observation_versions v
     SET stale_delete = true
    FROM observations o
   WHERE o.namespace_id = v.namespace_id AND o.observation_id = v.observation_id
     AND o.current_version = v.version
     AND (v.namespace_id, v.observation_id, v.version) IN (SELECT DISTINCT d.namespace_id, d.observation_id, d.version FROM deleted d)
     AND o.stale_delete AND o.retired_at IS NULL
     AND v.stale_delete = false;
  IF r_ns IS NOT NULL THEN
    PERFORM engram_flag_lineage(r_ns, r_obs, r_ver);
  END IF;
  RETURN NULL;
END $$;

-- Reversible per-version invalidation (N84, closes review G-6). Invalidate(f) increments
-- hidden_by_invalidation on every version whose observation_inputs name f and on all lineage
-- descendants of those versions (superseded versions included); Restore(f) decrements exactly
-- the same set (stable: no new version may have a hidden version as a candidate, N79). The
-- observations of the affected versions become stale_write so they are rebuilt from live sources
-- without f. A walk cut at depth 64 fails closed like the delete cascade (stale_delete on the
-- frontier observations; those are cleared only by the rebuild, not by Restore). Returns the
-- number of versions adjusted. It is called only by the facts trigger below, which fires when
-- invalidated_at goes NULL -> set (+1) or set -> NULL (-1), never on a repeat call.
CREATE FUNCTION engram_adjust_invalidation(p_ns uuid, p_fact uuid, p_delta integer) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  r_ns uuid[]; r_obs uuid[]; r_ver integer[];
  f_ns uuid[]; f_obs uuid[];
  s_ns uuid[]; s_obs uuid[];
  v_n integer;
BEGIN
  IF p_delta NOT IN (1, -1) THEN
    RAISE EXCEPTION 'engram_adjust_invalidation: delta must be +1 or -1' USING ERRCODE = '22023';
  END IF;
  SELECT array_agg(i.namespace_id), array_agg(i.observation_id), array_agg(i.version) INTO r_ns, r_obs, r_ver
    FROM observation_inputs i WHERE i.namespace_id = p_ns AND i.fact_id = p_fact;
  IF r_ns IS NULL THEN
    RETURN 0;
  END IF;
  UPDATE observation_versions v
     SET hidden_by_invalidation = v.hidden_by_invalidation + p_delta
    FROM (SELECT DISTINCT w.ns, w.obs, w.ver FROM engram_lineage_walk(r_ns, r_obs, r_ver, 64) w WHERE w.depth <= 64) s
   WHERE v.namespace_id = s.ns AND v.observation_id = s.obs AND v.version = s.ver;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  IF p_delta = 1 THEN
    UPDATE observations o
       SET stale_write = true, stale_since = coalesce(o.stale_since, now()), updated_at = now()
      FROM (SELECT DISTINCT w.ns, w.obs FROM engram_lineage_walk(r_ns, r_obs, r_ver, 64) w WHERE w.depth <= 64) s
     WHERE o.namespace_id = s.ns AND o.observation_id = s.obs AND o.retired_at IS NULL AND NOT o.stale_write;
    SELECT array_agg(x.ns), array_agg(x.obs) INTO f_ns, f_obs
      FROM (SELECT DISTINCT w.ns, w.obs FROM engram_lineage_walk(r_ns, r_obs, r_ver, 64) w WHERE w.depth = 65) x;
    WITH o AS (
      UPDATE observations ob
         SET stale_delete = true, stale_since = coalesce(ob.stale_since, now()), updated_at = now()
        FROM unnest(f_ns, f_obs) AS y (a, b)
       WHERE ob.namespace_id = y.a AND ob.observation_id = y.b AND ob.retired_at IS NULL AND NOT ob.stale_delete
       RETURNING ob.namespace_id, ob.observation_id)
    SELECT array_agg(o.namespace_id), array_agg(o.observation_id) INTO s_ns, s_obs FROM o;
    UPDATE observation_versions v SET stale_delete = true
      FROM observations ob, unnest(s_ns, s_obs) AS s (a, b)
     WHERE ob.namespace_id = s.a AND ob.observation_id = s.b
       AND v.namespace_id = ob.namespace_id AND v.observation_id = ob.observation_id
       AND v.version = ob.current_version AND NOT v.stale_delete;
  END IF;
  RETURN v_n;
END $$;

CREATE FUNCTION engram_fact_invalidation_changed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM engram_adjust_invalidation(NEW.namespace_id, NEW.memory_id,
                                     CASE WHEN NEW.invalidated_at IS NOT NULL THEN 1 ELSE -1 END);
  RETURN NULL;
END $$;

-- consolidation_proposals is write-once (N43): the op list an op_key was computed over must
-- never change. UPDATE is refused for every role; DELETE is left to the FK from
-- consolidation_applied (RESTRICT), which admits it only while no op of the batch has been
-- applied — the discard path of section 5.2 (apply re-verification failed, N41).
CREATE FUNCTION engram_forbid_proposal_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'consolidation_proposals is write-once (UPDATE by % refused)', current_user
    USING ERRCODE = '42501';
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
);

CREATE TRIGGER shard_meta_singleton BEFORE INSERT ON shard_meta
  FOR EACH ROW EXECUTE FUNCTION engram_shard_meta_singleton();

-- outbox_cursors: one row per consumer plus the 'relay' row, whose gaps column is the persisted
-- gap watchlist (D6). A seq missing for more than 1 s at the relay's read point is recorded as
-- {"seq": n, "deadline": ts}; it is removed when the row appears or when the deadline
-- (first_seen + 2 x statement_timeout) passes. The relay stops advancing when the list holds
-- 1000 entries. Persisting it here (not in memory) makes relay failover lossless.
CREATE TABLE outbox_cursors (
  consumer    text PRIMARY KEY CHECK (consumer ~ '^(relay|index|kafka|deletion-log|move:[0-9a-f-]{36})$'),
  last_seq    bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  gaps        jsonb NOT NULL DEFAULT '[]'::jsonb
              CHECK (jsonb_typeof(gaps) = 'array' AND jsonb_array_length(gaps) <= 1000),
  updated_at  timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);

-- =============================================================================
-- Namespace-keyed tables (the five big ones are partitioned further down)
-- =============================================================================

-- namespace_ownership: the fence VALUE (D2 row 4, D5; the fence LOCK is the advisory lock pair
-- engram_ns_lock_keys, see the header). Mirrors the catalog for the namespaces this shard
-- hosts. The ONLY table with a shard_id column. The state machine (states x roles x
-- statements) is written once in section 3.3.1 and lives here as the data table
-- ownership_transitions, enforced by engram_check_ownership (N101).
-- move_applied_seq is the target-side replay PROGRESS of a move (lag reporting only): the
-- replay never reads "seq > move_applied_seq" (review F-3); it anti-joins outbox against
-- move_applied (N50).
-- move_id / move_epoch (N88): set on the SOURCE by StartMove (edge start_move; move_epoch = the
-- target epoch e + 1) and cleared on abort/rollback; the purge sweep skips a namespace while
-- move_epoch IS NOT NULL (view purgeable_namespaces), so no parent row can vanish between the
-- copy ranges. On the target move_id is set by the incoming row.
-- target_shard_id / target_epoch (N93, N98): carried by a moved_out row so the API can route to the
-- target without the catalog; a moved_out row is a PERMANENT fence value and is never deleted.
CREATE TABLE namespace_ownership (
  namespace_id      uuid PRIMARY KEY,
  tenant_id         text NOT NULL CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  shard_id          integer NOT NULL,
  epoch             bigint NOT NULL CHECK (epoch >= 1),
  state             ownership_state NOT NULL,
  freeze_reason     text CHECK (freeze_reason IN ('move', 'delete', 'restore')),   -- why frozen (D2)
  move_id           uuid,
  move_epoch        bigint,
  move_applied_seq  bigint,
  target_shard_id   integer,
  target_epoch      bigint,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),                -- FK target for the tenant_id denormalisation
  CHECK (state <> 'incoming' OR move_id IS NOT NULL),
  CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL)),   -- a thaw and cutover (c) clear the reason (N64, N93)
  CHECK ((state = 'moved_out') = (target_shard_id IS NOT NULL)),
  CHECK ((target_shard_id IS NULL) = (target_epoch IS NULL)),
  CHECK (target_shard_id IS NULL OR (target_shard_id <> shard_id AND target_epoch > epoch)),
  CHECK (move_epoch IS NULL OR (move_id IS NOT NULL AND move_epoch > epoch))
);

-- The ownership state machine as DATA (N101, section 3.3.1): one row per (edge, role). The
-- trigger engram_check_ownership accepts an INSERT, UPDATE or DELETE of a namespace_ownership row
-- only if it matches a row here for current_user, and then checks the epoch rule and the
-- move/target column effects of that edge. Any (state pair, role) not listed is refused: that is
-- the whole negative test surface of TestIso_Ownership_Transitions. NULL from_state = INSERT,
-- NULL to_state = DELETE. epoch_rule: one = epoch 1, any, same, plus1 = exactly +1, greater =
-- strictly greater (and above the stored target_epoch for a move back). move_effect on
-- (move_id, move_epoch): none = unchanged, open = StartMove (move_id set from NULL, move_epoch =
-- epoch + 1), close = both NULL, retarget = a NEW move_id and move_epoch NULL (moved_out ->
-- incoming). target_effect on (target_shard_id, target_epoch): none, set = cutover (c) (target
-- shard differs, target_epoch = epoch + 1), clear.
CREATE TABLE ownership_transitions (
  edge           text NOT NULL,
  role_name      text NOT NULL,
  from_state     ownership_state,
  from_reason    text,
  to_state       ownership_state,
  to_reason      text,
  epoch_rule     text NOT NULL CHECK (epoch_rule IN ('one', 'any', 'same', 'plus1', 'greater')),
  move_effect    text NOT NULL DEFAULT 'none' CHECK (move_effect IN ('none', 'open', 'close', 'retarget')),
  target_effect  text NOT NULL DEFAULT 'none' CHECK (target_effect IN ('none', 'set', 'clear')),
  note           text NOT NULL DEFAULT '',
  PRIMARY KEY (edge, role_name),
  CHECK (from_state IS NOT NULL OR to_state IS NOT NULL),
  CHECK ((from_state = 'frozen') = (from_reason IS NOT NULL)),
  CHECK ((to_state = 'frozen') = (to_reason IS NOT NULL))
);

INSERT INTO ownership_transitions (edge, role_name, from_state, from_reason, to_state, to_reason, epoch_rule, move_effect, target_effect, note) VALUES
  ('create',          'engram_app',   NULL,       NULL,      'active',    NULL,      'one',     'none',     'none',  'CreateNamespace: first row, epoch 1'),
  ('create',          'engram_admin', NULL,       NULL,      'active',    NULL,      'one',     'none',     'none',  'provisioning and tests'),
  ('plan_target',     'engram_move',  NULL,       NULL,      'incoming',  NULL,      'any',     'none',     'none',  'move plan on the target; move_id mandatory (CHECK)'),
  ('start_move',      'engram_move',  'active',   NULL,      'active',    NULL,      'same',    'open',     'none',  'StartMove on the source: sets move_id and move_epoch, pauses purges (N88)'),
  ('abort_move',      'engram_move',  'active',   NULL,      'active',    NULL,      'same',    'close',    'none',  'rollback before the freeze'),
  ('freeze_move',     'engram_move',  'active',   NULL,      'frozen',    'move',    'same',    'none',     'none',  'D5 step 4'),
  ('freeze_delete',   'engram_app',   'active',   NULL,      'frozen',    'delete',  'same',    'none',     'none',  'namespace delete; no outgoing edge except deletion'),
  ('freeze_restore',  'engram_admin', 'active',   NULL,      'frozen',    'restore', 'same',    'none',     'none',  'restore from backup (N23)'),
  ('thaw_move',       'engram_move',  'frozen',   'move',    'active',    NULL,      'same',    'close',    'none',  'move rollback'),
  ('cutover_c',       'engram_move',  'frozen',   'move',    'moved_out', NULL,      'same',    'none',     'set',   'cutover (c), the point of no return (N98); clears freeze_reason (N93)'),
  ('activate_target', 'engram_move',  'incoming', NULL,      'active',    NULL,      'same',    'close',    'none',  'cutover (b); the row already carries e + 1'),
  ('return_move',     'engram_move',  'moved_out', NULL,     'incoming',  NULL,      'greater', 'retarget', 'clear', 'a later move back to this shard (N93); data rows must be gone'),
  ('epoch_bump',      'engram_admin', 'active',   NULL,      'active',    NULL,      'plus1',   'none',     'none',  'failover and restore bump, exactly +1 (N63)'),
  ('restore_done',    'engram_admin', 'frozen',   'restore', 'active',    NULL,      'plus1',   'none',     'none',  'restore completion, exactly +1'),
  ('rollback_target', 'engram_move',  'incoming', NULL,      NULL,        NULL,      'any',     'none',     'none',  'DELETE of the target row on rollback'),
  ('rollback_target', 'engram_admin', 'incoming', NULL,      NULL,        NULL,      'any',     'none',     'none',  'operator cleanup'),
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
           WHEN 'none'  THEN (n.target_shard_id, n.target_epoch) IS NOT DISTINCT FROM (o.target_shard_id, o.target_epoch)
           WHEN 'set'   THEN n.target_shard_id IS NOT NULL AND n.target_shard_id <> n.shard_id AND n.target_epoch = o.epoch + 1
           WHEN 'clear' THEN n.target_shard_id IS NULL AND n.target_epoch IS NULL
         END;
$$;

CREATE TRIGGER namespace_ownership_check BEFORE INSERT OR UPDATE OR DELETE ON namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram_check_ownership();
-- N91: the move loader runs with session_replication_role = replica; this trigger must still fire.
ALTER TABLE namespace_ownership ENABLE ALWAYS TRIGGER namespace_ownership_check;

-- Shard-wide schedulers read these views (admin role only), never the table (N97, N88): the
-- op-sweeper, DEFERRED resumer, consolidate-sweep, page-cron, outbox-trim and stats sweeper join
-- schedulable_namespaces, so the source of a move never starts a copied operation; the
-- purge-sweep joins purgeable_namespaces, which excludes a namespace with an open move.
CREATE VIEW schedulable_namespaces AS
  SELECT namespace_id, tenant_id, epoch, move_epoch
    FROM namespace_ownership
   WHERE state = 'active';

CREATE VIEW purgeable_namespaces AS
  SELECT namespace_id, tenant_id, epoch
    FROM namespace_ownership
   WHERE state = 'active' AND move_epoch IS NULL;

-- move_applied: exactly-once ledger of replayed source events, keyed by (namespace_id, seq)
-- (D5, N50). On the TARGET it is authoritative: INSERT ... ON CONFLICT DO NOTHING RETURNING seq
-- in the same transaction as the event's row-level apply (no row -> already applied). On the
-- SOURCE the mover keeps a lagging copy, inserted after the target apply committed, so that the
-- catch-up/drain read is one namespace-confined anti-join on the source:
--   SELECT ... FROM outbox o WHERE o.namespace_id = $1 AND o.seq > $p0
--     AND NOT EXISTS (SELECT 1 FROM move_applied m WHERE m.namespace_id = o.namespace_id AND m.seq = o.seq)
-- (a crash between the two inserts re-reads an event the target then drops). Never
-- "seq > watermark": an out-of-order lower seq committing late would be skipped (review F-3).
-- Rows are deleted on both sides when the move reaches done/rolled_back.
CREATE TABLE move_applied (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  seq           bigint NOT NULL,
  move_id       uuid NOT NULL,
  applied_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, seq),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

-- namespace_stats: DERIVED counters (N69, review F-36): refreshed per namespace by the stats
-- sweeper (admin role) with count(*) over the live partial indexes, never updated by
-- CommitChunk, FinalizeVersion or the delete cascade (a per-commit UPDATE serialised every
-- commit of a namespace on this row). max_facts and the catalog's facts_estimate tolerate one
-- refresh interval of staleness. `large` (N55, review F-8) flips when live_facts crosses 20 k
-- (cleared below 10 k) and drives the per-namespace partial HNSW index (section 3.3.4).
CREATE TABLE namespace_stats (
  namespace_id        uuid PRIMARY KEY,
  tenant_id           text NOT NULL,
  large               boolean NOT NULL DEFAULT false,   -- >= 20 k live facts: partial HNSW per namespace (N55)
  large_since         timestamptz,
  live_facts          bigint NOT NULL DEFAULT 0 CHECK (live_facts >= 0),
  total_facts         bigint NOT NULL DEFAULT 0 CHECK (total_facts >= 0),
  live_chunks         bigint NOT NULL DEFAULT 0 CHECK (live_chunks >= 0),
  live_documents      bigint NOT NULL DEFAULT 0 CHECK (live_documents >= 0),
  live_observations   bigint NOT NULL DEFAULT 0 CHECK (live_observations >= 0),
  bytes_estimate      bigint NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  recalls_1h          bigint NOT NULL DEFAULT 0,
  retains_1h          bigint NOT NULL DEFAULT 0,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- outbox: transactional outbox and per-shard change log (D6). PK is the global seq (the relay
-- reads in seq order); (namespace_id, seq) serves move consumers and export deltas.
-- namespace_id, tenant_id and epoch are stamped from the transaction scope, never passed by
-- the application. Events are thin (N12): ids, versions and flags; never text or vectors.
-- N80 bounds every event BY CONSTRUCTION: ids are 16-byte `bytes` (about 18 B encoded), an event
-- carries at most 256 ids across all its lists, a larger set is paged as consecutive events of the
-- same type in the same transaction (page, page_count, plus the group key document_id /
-- deleted_at / batch_key), above 4,096 ids (16 pages) the event is sent with ids_elided = true and
-- counts only (consumers read the ids from the store; the rows live until PurgeDocument plus
-- grace), and RowsPurged carries (table, document_id, count, min_key, max_key) with no id list.
-- The CHECK below is the backstop, not the mechanism.
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

-- deletion_log: every synchronous delete, in the same transaction (section 9 ND-8). The
-- deletion-log outbox consumer replicates rows to the catalog so a restore-from-backup can
-- re-apply deletes made after the backup. Idempotent on (namespace_id, kind, subject_id, deleted_at).
CREATE TABLE deletion_log (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  kind          text NOT NULL CHECK (kind IN ('document', 'memory', 'namespace', 'tenant')),
  subject_id    text NOT NULL,                     -- document_id | memory_id::text | namespace_id::text | tenant_id
  epoch         bigint NOT NULL,
  operation_id  uuid,
  deleted_at    timestamptz NOT NULL DEFAULT now(),
  details       jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),   -- N79: {"lineage_flagged": n, "lineage_frontier": m} from engram.lineage_* of the cascade transaction
  PRIMARY KEY (namespace_id, kind, subject_id, deleted_at)
);

-- =============================================================================
-- Namespace-scoped tables
-- =============================================================================

-- ingest_ledger: append-only raw inputs. Body inline when <= 64 KiB; larger bodies live in
-- blob ledger/{sha256 hex} written before this row (N7) and the row stores hash + key.
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
  body_blob_key   text,                            -- ledger/{hex(content_hash)} iff content_bytes > 65536
  received_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, ledger_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((content_bytes <= 65536) = (body IS NOT NULL)),
  CHECK ((content_bytes >  65536) = (body_blob_key IS NOT NULL))
) WITH (fillfactor = 100);

CREATE INDEX ingest_ledger_doc_idx ON ingest_ledger (namespace_id, document_id, received_at DESC);
CREATE INDEX ingest_ledger_op_idx  ON ingest_ledger (namespace_id, operation_id);
CREATE INDEX ingest_ledger_hash_idx ON ingest_ledger (namespace_id, content_hash) WHERE body_blob_key IS NOT NULL;

CREATE TRIGGER ingest_ledger_append_only BEFORE UPDATE OR DELETE ON ingest_ledger
  FOR EACH ROW EXECUTE FUNCTION engram_forbid_ledger_mutation();

-- documents / document_versions / document_version_chunks (D8)
CREATE TABLE documents (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  document_id       text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
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
  CHECK ((state = 'active') = (deleted_at IS NULL))
) WITH (fillfactor = 80);

CREATE INDEX documents_updated_idx  ON documents (namespace_id, updated_at DESC);
CREATE INDEX documents_deleting_idx ON documents (namespace_id, deleted_at) WHERE state = 'deleting';

CREATE TRIGGER documents_touch BEFORE UPDATE ON documents
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

CREATE TABLE document_versions (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  document_id     text NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  content_hash    bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- = ingest_ledger.content_hash
  status          version_status NOT NULL DEFAULT 'ingesting',
  update_mode     update_mode NOT NULL,
  operation_id    uuid NOT NULL,
  ledger_id       uuid NOT NULL,
  chunk_count     integer CHECK (chunk_count >= 0),
  chunks_done     integer NOT NULL DEFAULT 0 CHECK (chunks_done >= 0),   -- written per wave by the workflow (N69), never per chunk (a per-commit counter serialises every commit of v on one row)
  body_hash       bytea NOT NULL CHECK (octet_length(body_hash) = 32),   -- N104: sha256 of the FULL reconstructed body of this version
  body_key        text NOT NULL,                                          -- N104: ver/{hex(body_hash)} (content-addressed blob written by LoadItem BEFORE this row); APPEND reads this one object
  created_at      timestamptz NOT NULL DEFAULT now(),
  activated_at    timestamptz,
  finished_at     timestamptz,
  PRIMARY KEY (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  FOREIGN KEY (namespace_id, ledger_id)   REFERENCES ingest_ledger (namespace_id, ledger_id),
  CHECK (body_key = 'ver/' || encode(body_hash, 'hex'))
) WITH (fillfactor = 80);

-- at most one active version per document
CREATE UNIQUE INDEX document_versions_active_uq ON document_versions (namespace_id, document_id)
  WHERE status = 'active';
-- Lock protocol (N40 as amended by N83, closes review G-5; model-checked in section 7).
-- CommitChunk(v) takes engram_try_doc_lock_shared(ns, doc) (failure -> retryable DocumentBusy,
-- 100 ms) and then reads the version row and documents.current_version as PLAIN reads (no FOR
-- SHARE, no multixact churn) and proceeds only when status = 'ingesting'. FinalizeVersion, the
-- delete cascade and PurgeDocument take the same key EXCLUSIVE under lock_timeout = 5 s, so they
-- wait for every in-flight commit of v, and a commit that starts afterwards sees the terminal
-- status and stops. The retain ack's version assignment keeps only the documents row lock (FOR
-- UPDATE, N56) and takes no advisory lock, so ack latency is never behind ingest commits.
-- FinalizeVersion(v) marks v 'superseded' without retiring anything when a newer version row
-- already exists; only the newest version computes the retire set. The status column is all the
-- schema needs; the self-check below only pins the enum.
CREATE INDEX document_versions_op_idx ON document_versions (namespace_id, operation_id);

-- chunks (hash-partitioned). Identity within a document = content hash of the text (N6: the
-- contextual header is hashed separately). Chunk text <= 4,000 characters, checked as
-- <= 16 KiB of UTF-8 (D8; the chunker enforces the character bound).
CREATE TABLE chunks (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  chunk_id         uuid NOT NULL,
  document_id      text NOT NULL,
  content_hash     bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(text)
  header_hash      bytea NOT NULL CHECK (octet_length(header_hash) = 32),    -- sha256(header) (N6)
  extraction_key   bytea NOT NULL CHECK (octet_length(extraction_key) = 32), -- N87: sha256(content_hash||prompt_version||model||schema_version||render_hash) = xcache key; render_hash = sha256(day(mentioned_at)||context||canonical(metadata)||sorted(entity_hints)||retain.mission||header_hash)
  ordinal          integer NOT NULL CHECK (ordinal >= 0),                    -- position in the latest version
  heading_path     text NOT NULL DEFAULT '' CHECK (octet_length(heading_path) <= 1024),
  header           text NOT NULL DEFAULT '' CHECK (octet_length(header) <= 1024),  -- "[doc summary] > [heading path]"
  text             text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 16384),
  tags             text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count        smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  mentioned_at     timestamptz NOT NULL,                                     -- N86: max(timestamp of every item whose bytes the chunk covers); an APPEND re-chunk takes max(item ts, mentioned_at of every overlapped base chunk); facts inherit it
  embedding_effective_at timestamptz NOT NULL,                               -- N85: mentioned_at of the newest item covered by the summary used in the embedded header; under as_of = T the chunk arm adds embedding_effective_at <= T AND mentioned_at <= T
  embedding        halfvec(768) STORAGE MAIN NOT NULL,                       -- embeds header || text; STORAGE MAIN keeps the vector inline so the exact scan is a heap scan (N94)
  embedding_model  text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  retired_at       timestamptz,
  purge_after      timestamptz,                                              -- retire: +1 h; delete: now()
  live             boolean GENERATED ALWAYS AS (retired_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, chunk_id),
  UNIQUE (namespace_id, document_id, content_hash),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  CHECK ((retired_at IS NULL) = (purge_after IS NULL))
) PARTITION BY HASH (namespace_id);

-- membership of chunks in document versions (REPLACE retires chunks not in the new set)
CREATE TABLE document_version_chunks (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  document_id    text NOT NULL,
  version        integer NOT NULL,
  content_hash   bytea NOT NULL,
  chunk_id       uuid NOT NULL,
  PRIMARY KEY (namespace_id, document_id, version, content_hash),
  FOREIGN KEY (namespace_id, document_id, version) REFERENCES document_versions (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, chunk_id) REFERENCES chunks (namespace_id, chunk_id)
);

CREATE INDEX document_version_chunks_chunk_idx ON document_version_chunks (namespace_id, chunk_id);

-- facts (hash-partitioned). memory_id is the public id of a fact (D1).
CREATE TABLE facts (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  memory_id            uuid NOT NULL,
  document_id          text NOT NULL,
  chunk_id             uuid NOT NULL,
  ordinal              smallint NOT NULL CHECK (ordinal >= 0),               -- position in the chunk's extraction
  content_hash         bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(normalised text)
  extraction_key       bytea NOT NULL CHECK (octet_length(extraction_key) = 32), -- = chunks.extraction_key at insert (N87 key); FinalizeVersion retires live facts whose key <> the chunk's current key (N58)
  text                 text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
  fact_type            fact_type NOT NULL,
  fact_type_code       smallint GENERATED ALWAYS AS (CASE fact_type WHEN 'world' THEN 1 WHEN 'experience' THEN 2 END) STORED,
  w5                   jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(w5) = 'object'),  -- {who:[],what,when,where,why}
  occurred_start       timestamptz,
  occurred_end         timestamptz,
  mentioned_at         timestamptz NOT NULL,            -- = the item's timestamp, SERVER-SET; the only as_of key (D9, review F-2); never written by the extractor or a per-item override
  said_at              timestamptz,                     -- the model's judgement of when the source said it: display and ranking only, never an as_of key (D9)
  tags                 text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count            smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  metadata             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  embedding            halfvec(768) STORAGE MAIN NOT NULL,                   -- inline (N94); rewritten only by ReembedChunk
  extraction_version   integer NOT NULL CHECK (extraction_version >= 1),    -- extraction schema version
  prompt_version       text NOT NULL,                                        -- e.g. extract/v1
  model                text NOT NULL,                                        -- models.extract used
  embedding_model      text NOT NULL,
  created_at           timestamptz NOT NULL DEFAULT now(),
  retired_at           timestamptz,
  purge_after          timestamptz,
  invalidated_at       timestamptz,
  invalidation_reason  text,
  live                 boolean GENERATED ALWAYS AS (retired_at IS NULL AND invalidated_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, memory_id),
  UNIQUE (namespace_id, chunk_id, content_hash),                            -- CommitChunk idempotency (no epoch, D11)
  FOREIGN KEY (namespace_id, chunk_id)    REFERENCES chunks (namespace_id, chunk_id),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  CHECK (occurred_start IS NULL OR occurred_end IS NULL OR occurred_end >= occurred_start),
  CHECK ((retired_at IS NULL) = (purge_after IS NULL))
) PARTITION BY HASH (namespace_id);

-- fact_links (hash-partitioned). One row per edge. entity/temporal/semantic edges are
-- undirected and stored once in canonical order (src < dst); causal edges are directed
-- (src = cause, dst = effect). Per-fact caps (temporal <= 20, semantic <= 10, entity <= 10
-- per shared entity) are enforced by the linker (section 5): a counting trigger would cost
-- one query per inserted row on the largest table. Rows are NOT deleted by the synchronous
-- delete cascade (N61, review F-19): the graph arm joins facts and requires `live` on both
-- endpoints (NoOrphanLinks is a traversal property), so a link to a retired fact is inert;
-- PurgeDocument removes it through the FK cascade from facts.
CREATE TABLE fact_links (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  src_memory_id  uuid NOT NULL,
  dst_memory_id  uuid NOT NULL,
  link_type      link_type NOT NULL,
  weight         real NOT NULL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),
  PRIMARY KEY (namespace_id, src_memory_id, dst_memory_id, link_type),
  FOREIGN KEY (namespace_id, src_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, dst_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  CHECK (src_memory_id <> dst_memory_id),
  CHECK (link_type = 'causal' OR src_memory_id < dst_memory_id)
) PARTITION BY HASH (namespace_id);

-- entities / entity_aliases / entity_mentions
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
) WITH (fillfactor = 80);

CREATE UNIQUE INDEX entities_canonical_uq ON entities (namespace_id, canonical_norm)
  WHERE merged_into IS NULL;
-- fuzzy resolution: multi-column GIN (btree_gin for the uuid) so the index leads with namespace_id
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
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, alias_norm),
  FOREIGN KEY (namespace_id, entity_id) REFERENCES entities (namespace_id, entity_id) ON DELETE CASCADE
);

CREATE INDEX entity_aliases_entity_idx ON entity_aliases (namespace_id, entity_id);

CREATE TABLE entity_mentions (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  memory_id     uuid NOT NULL,
  entity_id     uuid NOT NULL,
  role          text NOT NULL DEFAULT 'other' CHECK (role IN ('who', 'what', 'where', 'when', 'why', 'other')),
  confidence    real NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
  PRIMARY KEY (namespace_id, memory_id, entity_id),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, entity_id) REFERENCES entities (namespace_id, entity_id)
) PARTITION BY HASH (namespace_id);

-- observations / observation_versions / observation_sources (D9, D12)
CREATE TABLE observations (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  observation_id    uuid NOT NULL,
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  proof_count       integer NOT NULL DEFAULT 0 CHECK (proof_count >= 0),   -- sources with retired_at IS NULL (N42)
  stale_write       boolean NOT NULL DEFAULT false,      -- evidence changed under it (replace-retire, restore); still visible (N42)
  stale_delete      boolean NOT NULL DEFAULT false,      -- "needs a rewrite": lost a live source, or its CURRENT version is derived_from_deleted (delete, purge, invalidate); hides the current version until the rewrite lands (N41 as amended)
  stale_since       timestamptz,
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),   -- consolidation scope tags
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  PRIMARY KEY (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((stale_write OR stale_delete) = (stale_since IS NOT NULL))
) WITH (fillfactor = 80);

CREATE INDEX observations_stale_idx ON observations (namespace_id, stale_delete DESC, stale_since)
  WHERE (stale_write OR stale_delete) AND retired_at IS NULL;      -- hidden ones first (section 5.2)
CREATE INDEX observations_tags_gin ON observations USING gin (namespace_id, tags) WHERE retired_at IS NULL;

CREATE TRIGGER observations_touch BEFORE UPDATE ON observations
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- Recall visibility predicate for an observation version (N41 as amended by review F-1):
--   live AND effective_at <= T AND (superseded_at IS NULL OR superseded_at > T)
-- where
--   live = retired_at IS NULL
--          AND NOT derived_from_deleted                        -- per version, permanent; propagated along lineage (N79)
--          AND hidden_by_invalidation = 0                      -- per version, reversible counter (N84)
--          AND NOT (stale_delete AND superseded_at IS NULL)    -- the CURRENT version while the observation awaits a rewrite
-- derived_from_deleted is set by the observation_inputs trigger on exactly the versions whose
-- inputs named a now-deleted fact; nothing ever clears it (a rewrite supersedes the version
-- instead), so an older version written with deleted content in view can never resurface at
-- an earlier as_of. stale_delete on a version mirrors observations.stale_delete for the
-- current version only: set by the evidence triggers and by Invalidate, cleared only by Restore
-- (N48); when the rewrite lands the old version gets superseded_at and is live again in its
-- own as_of range if it was not derived from the victim. superseded_at = min(effective_at)
-- over later versions (maintained when a later version is inserted). This is D9's "latest
-- version with effective_at <= T", precomputed so it is a plain filter for HNSW and BM25 scans.
-- hidden_by_invalidation (N84, closes G-6) counts the invalidated facts a version (or any lineage
-- ancestor of it) was written from; Invalidate increments, Restore decrements the same set, so a
-- superseded version is hidden at every as_of while any such fact is invalidated and resurfaces
-- only when the last one is restored. The observation-level stale_delete for invalidation and
-- the "permanent once rewritten" rule of N48 are gone.
-- tags, retired_at and the flags are denormalised from observations so both observation arms
-- are single-table queries (Top-K pushdown needs that).
-- N94: HASH-partitioned by namespace_id (16 partitions) like facts and chunks, embedding STORAGE
-- MAIN; UNIQUE (namespace_id, ov_id) because a unique constraint of a partitioned table must
-- contain the partition key, and pg_search accepts the non-partition part of a composite key as
-- key_field (as for chunks).
-- effective_at(v) = max(max(mentioned_at) over observation_inputs(v) — every fact rendered to
-- the prompt, not only the cited sources — , max(effective_at) over the candidate observation
-- versions shown, effective_at(v - 1)) (D9 as amended; TLC AsOf_CitedOnly shows the leak
-- with cited sources only).
CREATE TABLE observation_versions (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  ov_id             uuid NOT NULL,                    -- surrogate: the pg_search key_field must be one column
  observation_id    uuid NOT NULL,
  version           integer NOT NULL CHECK (version >= 1),
  text              text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  embedding         halfvec(768) STORAGE MAIN NOT NULL,   -- inline (N94)
  embedding_model   text NOT NULL,
  effective_at      timestamptz NOT NULL,             -- D9 rule above (inputs ∪ candidates ∪ previous version), monotone across versions
  superseded_at     timestamptz,
  source_count      integer NOT NULL CHECK (source_count >= 1),
  stale_delete      boolean NOT NULL DEFAULT false,     -- mirror of observations.stale_delete for the current version (N41); effective only while superseded_at IS NULL
  derived_from_deleted boolean NOT NULL DEFAULT false,  -- inputs named a deleted fact, or a lineage ancestor did (N79): hidden at every as_of, forever (review F-1)
  hidden_by_invalidation integer NOT NULL DEFAULT 0 CHECK (hidden_by_invalidation >= 0),   -- N84: invalidated facts in this version's inputs or lineage; live needs 0
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count         smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  prompt_version    text NOT NULL,
  model             text NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  live              boolean GENERATED ALWAYS AS (retired_at IS NULL AND NOT derived_from_deleted AND hidden_by_invalidation = 0
                                                 AND NOT (stale_delete AND superseded_at IS NULL)) STORED,   -- the recall predicate (N41 as amended, N84)
  PRIMARY KEY (namespace_id, observation_id, version),
  UNIQUE (namespace_id, ov_id),                       -- partition key included; ov_id is the BM25 key_field
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  CHECK (superseded_at IS NULL OR superseded_at >= effective_at)
) PARTITION BY HASH (namespace_id);

CREATE INDEX observation_versions_effective_idx ON observation_versions (namespace_id, effective_at) WHERE live;

-- observation_sources is the mutable WORKING SET of the CURRENT version (evidence the consolidator
-- may still extend). The evidence of each version is frozen in observation_version_sources (N85).
-- N79: the update path deletes only rows the model was shown and dropped, never unshown ones.
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
);

CREATE INDEX observation_sources_memory_idx ON observation_sources (namespace_id, memory_id);

CREATE TRIGGER observation_sources_orphans AFTER DELETE ON observation_sources
  REFERENCING OLD TABLE AS deleted
  FOR EACH STATEMENT EXECUTE FUNCTION engram_observation_sources_after_delete();

-- observation_inputs (N41, N47; review F-9): every fact RENDERED to the consolidation prompt
-- that produced version v of the observation — the 8 batch facts plus the quoted sources of
-- every candidate observation shown, at most 5 per candidate (<= 58 rows per version) —
-- whether or not the version cites it. Cited sources are a subset. Used by (1) the
-- delete/purge cascade: removing a row whose fact is no longer live marks THAT version
-- derived_from_deleted through the trigger below (hidden at every as_of, permanently) and the
-- observation stale_delete when its current version is affected; (2) the apply transaction,
-- which re-verifies every input FOR SHARE (live) before writing and, since N79, every
-- candidate_versions entry FOR SHARE as well (NOT derived_from_deleted AND hidden_by_invalidation
-- = 0 AND retired_at IS NULL, else the proposal is discarded by the N41 path); (3) effective_at (D9).
-- A REPLACE-retired fact keeps its rows during the purge grace (N42). The FK to facts
-- cascades so the physical purge of a fact can never leave a dangling input.
CREATE TABLE observation_inputs (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  fact_id         uuid NOT NULL,                       -- = facts.memory_id
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, version, fact_id),
  FOREIGN KEY (namespace_id, observation_id, version)
    REFERENCES observation_versions (namespace_id, observation_id, version),
  FOREIGN KEY (namespace_id, fact_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
);

CREATE INDEX observation_inputs_fact_idx ON observation_inputs (namespace_id, fact_id);

CREATE TRIGGER observation_inputs_lost AFTER DELETE ON observation_inputs
  REFERENCING OLD TABLE AS deleted
  FOR EACH STATEMENT EXECUTE FUNCTION engram_observation_inputs_after_delete();

-- observation_version_lineage (N79, closes G-1): one row per candidate version whose TEXT the
-- consolidation prompt showed when it wrote (observation_id, version), including the
-- observation's own previous version, which is the edge (o, v) -> (o, v-1). ApplyBatch inserts
-- one row per entry of consolidation_proposals.candidate_versions in the same transaction as the
-- observation_versions row. Direct flagging (observation_inputs) misses the case "the model saw
-- the TEXT of a version that had seen the victim"; this table lets the delete cascade and
-- Invalidate follow that chain (engram_flag_lineage, engram_adjust_invalidation). A flagged
-- version taints every later version of its lineage until the observation is rebuilt as a root
-- version (no edge to a flagged ancestor, only live sources); rebuilds are rate-limited by config
-- consolidate.max_rebuilds_per_round (default 50 per namespace per wave, oldest first). Rows are
-- immutable: replay uses INSERT ... ON CONFLICT DO NOTHING (N81).
CREATE TABLE observation_version_lineage (
  namespace_id           uuid NOT NULL,
  tenant_id              text NOT NULL,
  observation_id         uuid NOT NULL,
  version                integer NOT NULL CHECK (version >= 1),
  parent_observation_id  uuid NOT NULL,
  parent_version         integer NOT NULL CHECK (parent_version >= 1),
  PRIMARY KEY (namespace_id, observation_id, version, parent_observation_id, parent_version),
  FOREIGN KEY (namespace_id, observation_id, version)
    REFERENCES observation_versions (namespace_id, observation_id, version),
  FOREIGN KEY (namespace_id, parent_observation_id, parent_version)
    REFERENCES observation_versions (namespace_id, observation_id, version),
  CHECK (parent_observation_id <> observation_id OR parent_version < version)   -- an edge always points at an older version
);

CREATE INDEX observation_version_lineage_parent_idx
  ON observation_version_lineage (namespace_id, parent_observation_id, parent_version);

-- observation_version_sources (N85, closes G-7): the evidence of EACH version, copied from
-- consolidation_proposals.ops (quote <= 500 chars) when the version is applied. Serving any
-- version (current or as_of) returns that version's rows joined to facts WHERE live AND
-- mentioned_at <= T (T = as_of), and proof_count counts the served rows. PurgeDocument deletes the
-- rows of purged facts (FK cascade).
CREATE TABLE observation_version_sources (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  memory_id       uuid NOT NULL,
  quote           text NOT NULL DEFAULT '' CHECK (char_length(quote) <= 500),
  PRIMARY KEY (namespace_id, observation_id, version, memory_id),
  FOREIGN KEY (namespace_id, observation_id, version)
    REFERENCES observation_versions (namespace_id, observation_id, version),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
);

CREATE INDEX observation_version_sources_memory_idx ON observation_version_sources (namespace_id, memory_id);

-- consolidation_batches / consolidation_applied: exactly-once effect (D12)
CREATE TABLE consolidation_batches (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  batch_key         bytea NOT NULL CHECK (octet_length(batch_key) = 32),   -- sha256(sorted memory ids || prompt_version || model)
  round_id          uuid NOT NULL,
  memory_ids        uuid[] NOT NULL CHECK (cardinality(memory_ids) BETWEEN 1 AND 8),
  state             text NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'running', 'proposed', 'applied', 'discarded', 'bisected', 'failed')),
  attempts          integer NOT NULL DEFAULT 0,
  model             text NOT NULL,
  prompt_version    text NOT NULL,
  result_blob_key   text,                                      -- consolidate/{hex(batch_key)}.json
  error             text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  started_at        timestamptz,
  finished_at       timestamptz,
  PRIMARY KEY (namespace_id, batch_key),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 80);

CREATE INDEX consolidation_batches_round_idx ON consolidation_batches (namespace_id, round_id);

-- consolidation_proposals (N43): the validated, deduplicated op list of a batch, persisted
-- write-once BEFORE any op is applied. op_key = sha256(batch_key || op_index) is computed over
-- THIS stored list (never over the live LLM output), and ApplyBatch reads it from here.
-- ops is a JSON array in op_index order: {kind, observation_id (pre-minted for creates), text,
-- source_fact_ids[], quotes[], reason}. input_fact_ids = batch facts ∪ the quoted sources
-- actually rendered for each candidate shown (<= 5 per candidate, N47); candidate_versions =
-- the (observation_id, version) pairs shown — together they are the observation_inputs rows
-- and the effective_at inputs of every version the batch creates (D9). A retry that finds a
-- row keeps the stored list and discards its own.
CREATE TABLE consolidation_proposals (
  namespace_id        uuid NOT NULL,
  tenant_id           text NOT NULL,
  batch_key           bytea NOT NULL CHECK (octet_length(batch_key) = 32),
  ops                 jsonb NOT NULL CHECK (jsonb_typeof(ops) = 'array'),
  op_count            integer NOT NULL CHECK (op_count BETWEEN 0 AND 16),
  input_fact_ids      uuid[] NOT NULL CHECK (cardinality(input_fact_ids) >= 1),
  candidate_versions  jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(candidate_versions) = 'array'),
  prompt_version      text NOT NULL,
  model               text NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, batch_key),
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
);

CREATE TRIGGER consolidation_proposals_write_once BEFORE UPDATE ON consolidation_proposals
  FOR EACH ROW EXECUTE FUNCTION engram_forbid_proposal_update();

CREATE TABLE consolidation_applied (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  op_key          bytea NOT NULL CHECK (octet_length(op_key) = 32),     -- sha256(batch_key || op_index)
  batch_key       bytea NOT NULL,
  op_index        integer NOT NULL CHECK (op_index >= 0),
  op_kind         text NOT NULL CHECK (op_kind IN ('create', 'update', 'delete')),
  observation_id  uuid NOT NULL,
  version         integer,
  applied_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, op_key),
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key),
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_proposals (namespace_id, batch_key)   -- RESTRICT: a proposal with an applied op can never be discarded (N43)
);

CREATE INDEX consolidation_applied_batch_idx ON consolidation_applied (namespace_id, batch_key, op_index);

-- fact_consolidation (N95, closes G-16): which facts a batch consolidated. Insert-only, one row
-- per consolidated fact, written in ApplyBatch (covered by the BatchApplied event, N81). Replaces
-- facts.consolidated_at, whose index predicate made every stamp a non-HOT update that re-inserted
-- the fact into HNSW and BM25.
CREATE TABLE fact_consolidation (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  memory_id        uuid NOT NULL,
  batch_key        bytea CHECK (octet_length(batch_key) = 32),            -- NULL for 'failed'/'capacity' stamps (N110)
  note             text NOT NULL DEFAULT 'done' CHECK (note IN ('done', 'failed', 'capacity')),
  consolidated_at  timestamptz NOT NULL DEFAULT now(),
  CHECK ((note = 'done') = (batch_key IS NOT NULL)),
  PRIMARY KEY (namespace_id, memory_id),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
);

CREATE INDEX fact_consolidation_batch_idx ON fact_consolidation (namespace_id, batch_key);

-- consolidation_state (N95): ONE row per namespace. The consolidate sweep advances the watermark
-- when every live fact with memory_id <= watermark is consolidated (memory_id is UUIDv7, so id
-- order is creation order). Pending facts = live facts above the watermark with no
-- fact_consolidation row (engram_pending_facts).
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
) WITH (fillfactor = 80);

CREATE INDEX batch_jobs_open_idx ON batch_jobs (namespace_id, updated_at) WHERE state IN ('submitted', 'running');

CREATE TRIGGER batch_jobs_touch BEFORE UPDATE ON batch_jobs
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- pages / page_versions / page_sources (D12, phase 3). stale_seq is a monotone counter:
-- a refresh captures it and clears the flags only if it is unchanged (section 5.3).
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
) WITH (fillfactor = 80);

CREATE UNIQUE INDEX pages_name_uq ON pages (namespace_id, name) WHERE retired_at IS NULL;
CREATE INDEX pages_stale_idx ON pages (namespace_id, updated_at)
  WHERE (stale_write OR stale_delete) AND retired_at IS NULL;

CREATE TRIGGER pages_touch BEFORE UPDATE ON pages
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

CREATE TABLE page_versions (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  page_id            uuid NOT NULL,
  version            integer NOT NULL CHECK (version >= 1),
  markdown_blob_key  text NOT NULL,                  -- pages/{page_id}/v{version}.md
  effective_at       timestamptz NOT NULL,           -- D9 rule, same as observation_versions
  superseded_at      timestamptz,
  evidence_hash      bytea NOT NULL CHECK (octet_length(evidence_hash) = 32),
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, page_id, version),
  FOREIGN KEY (namespace_id, page_id) REFERENCES pages (namespace_id, page_id)
);

CREATE TABLE page_sources (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  page_id        uuid NOT NULL,
  source_kind    text NOT NULL CHECK (source_kind IN ('fact', 'observation')),
  source_id      uuid NOT NULL,                      -- memory_id or observation_id (polymorphic: no FK)
  version_added  integer NOT NULL CHECK (version_added >= 1),
  PRIMARY KEY (namespace_id, page_id, source_kind, source_id),
  FOREIGN KEY (namespace_id, page_id) REFERENCES pages (namespace_id, page_id) ON DELETE CASCADE
);

CREATE INDEX page_sources_source_idx ON page_sources (namespace_id, source_id);

-- operations: async work visible through OperationService (D13: DEFERRED on quota exhaustion).
-- workflow_started_at IS NULL marks "no workflow yet" for the op-sweeper (N3).
CREATE TABLE operations (
  namespace_id         uuid NOT NULL,
  tenant_id            text NOT NULL,
  operation_id         uuid NOT NULL,
  kind                 text NOT NULL CHECK (kind IN ('retain', 'delete_document', 'delete_namespace', 'consolidate',
                                                     'page_refresh', 'export', 'purge', 'move')),   -- no 'reflect': Reflect is synchronous (review F-27)
  state                operation_state NOT NULL DEFAULT 'PENDING',
  request_id           text CHECK (octet_length(request_id) <= 128),
  target_id            text,                           -- document_id / page_id / snapshot version, per kind
  workflow_id          text NOT NULL,                  -- ns/{namespace_id}/op/{operation_id}
  task_queue           text NOT NULL,                  -- shard-{shard_id} at submission; restarted on the target after a move
  submitted_epoch      bigint NOT NULL CHECK (submitted_epoch >= 1),   -- audit only; never part of an identity (D11)
  workflow_started_at  timestamptz,
  progress             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),  -- units_total, units_done, facts_created, consolidation_lag_s; written once per wave by the workflow (N69), never per chunk
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
);

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
                                                  'move_cleanup', 'export_expire', 'page_retire', 'xcache_gc')),
  not_before     timestamptz NOT NULL DEFAULT now(),  -- N100: the sweeper deletes only when not_before <= now(); xcache_gc rows carry now() + xcache_grace (24 h) and are re-checked against live chunks first
  operation_id   uuid,
  attempts       integer NOT NULL DEFAULT 0,
  last_error     text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, tombstone_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

CREATE INDEX blob_tombstones_created_idx ON blob_tombstones (namespace_id, not_before);

-- export_snapshots (D12): one row per snapshot version. The delete cascade marks every ready
-- snapshot that may contain the document 'expired' (N59, review F-13): StreamSnapshot refuses
-- an expired version and the client re-syncs from a new full snapshot; expires_at = now()
-- hands the blobs to the tombstone sweeper.
CREATE TABLE export_snapshots (
  namespace_id        uuid NOT NULL,
  tenant_id           text NOT NULL,
  version             integer NOT NULL CHECK (version >= 1),
  state               text NOT NULL DEFAULT 'building' CHECK (state IN ('building', 'ready', 'expired', 'failed')),
  expired_reason      text CHECK (expired_reason IN ('ttl', 'document_delete', 'namespace_delete')),
  manifest_key        text NOT NULL,                  -- export/v{version}/manifest.json
  from_seq            bigint,                         -- outbox range for delta-v{n-1}-v{n}
  to_seq              bigint NOT NULL,
  fact_count          bigint NOT NULL DEFAULT 0,
  observation_count   bigint NOT NULL DEFAULT 0,
  chunk_count         bigint NOT NULL DEFAULT 0,
  page_count          bigint NOT NULL DEFAULT 0,
  bytes               bigint NOT NULL DEFAULT 0,
  operation_id        uuid,
  created_at          timestamptz NOT NULL DEFAULT now(),
  completed_at        timestamptz,
  expires_at          timestamptz,
  PRIMARY KEY (namespace_id, version),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((state = 'expired') = (expired_reason IS NOT NULL))
);

-- =============================================================================
-- Functions that need the tables (SQL bodies are validated at CREATE time)
-- =============================================================================

-- Pending facts of a namespace (N95): live facts above the consolidation watermark with no
-- fact_consolidation row, oldest first. A plain PK range scan on (namespace_id, memory_id).
CREATE FUNCTION engram_pending_facts(p_ns uuid, p_limit integer DEFAULT 200) RETURNS SETOF uuid
LANGUAGE sql STABLE AS $$
  SELECT f.memory_id
    FROM facts f
   WHERE f.namespace_id = p_ns AND f.live
     AND f.memory_id > coalesce((SELECT s.watermark_memory_id FROM consolidation_state s WHERE s.namespace_id = p_ns),
                                '00000000-0000-0000-0000-000000000000'::uuid)
     AND NOT EXISTS (SELECT 1 FROM fact_consolidation c WHERE c.namespace_id = f.namespace_id AND c.memory_id = f.memory_id)
   ORDER BY f.memory_id
   LIMIT p_limit;
$$;

-- Move cleanup (N93, N91): deletes the data rows of ONE namespace on this shard in bounded
-- batches and NEVER touches the namespace_ownership row (a moved_out row is a permanent fence
-- value). SECURITY DEFINER, owned by engram_migrate: engram_move has no DELETE on source data and
-- cannot bypass RLS, so this is the only way a move frees the source. EXECUTE is granted to
-- engram_move and engram_admin only. It refuses unless the ownership row is moved_out (source
-- cleanup after cutover (c)) or incoming (target rollback). Each call deletes at most p_batch
-- rows from the first non-empty table in FK order and returns the count; 0 means done. blob_tombstones, outbox,
-- outbox_skipped, deletion_log and move_applied are not touched (worklists and history).
CREATE FUNCTION engram_cleanup_namespace(p_ns uuid, p_batch integer DEFAULT 10000) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
  v_state ownership_state;
  v_tbl   text;
  v_n     bigint;
  c_order constant text[] := ARRAY[
    'observation_version_sources', 'observation_version_lineage', 'observation_inputs', 'observation_sources',
    'fact_consolidation', 'consolidation_state', 'consolidation_applied', 'consolidation_proposals',
    'consolidation_batches', 'observation_versions', 'observations',
    'page_sources', 'page_versions', 'pages',
    'entity_mentions', 'fact_links', 'facts', 'entity_aliases', 'entities',
    'document_version_chunks', 'chunks', 'document_versions', 'ingest_ledger', 'documents',
    'export_snapshots', 'token_usage_events', 'token_usage', 'quota_counters', 'batch_jobs',
    'idempotency_keys', 'operations', 'namespace_stats'];
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
    EXECUTE format('DELETE FROM %I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I WHERE namespace_id = $1 LIMIT $2)',
                   v_tbl, v_tbl) USING p_ns, p_batch;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n > 0 THEN
      RETURN v_n;
    END IF;
  END LOOP;
  RETURN 0;
END $$;

-- VerifyFK (N88): orphan count per foreign key of the namespace-scoped tables, run by the mover on
-- the target outside the freeze window and again over the rows touched by the final pass. Every
-- row must be 0 before cutover. Generated from pg_constraint, so a new foreign key is covered
-- without editing the mover. Run with the namespace in scope (RLS confines both sides).
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

-- Predicate of the RESTRICTIVE move policies below (N103 item 5): is the namespace in scope the
-- target of a move right now? Evaluated once per statement as an InitPlan.
CREATE FUNCTION engram_ns_is_incoming() RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM namespace_ownership o
                  WHERE o.namespace_id = current_setting('engram.namespace_id')::uuid AND o.state = 'incoming');
$$;

-- =============================================================================
-- Partitions: 16 hash partitions for the five big tables. Storage parameters must be set per
-- partition (a partitioned parent cannot carry them).
-- =============================================================================
DO $$
DECLARE
  t text;
  i integer;
BEGIN
  FOREACH t IN ARRAY ARRAY['chunks', 'facts', 'fact_links', 'entity_mentions', 'observation_versions'] LOOP
    FOR i IN 0..15 LOOP
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES WITH (MODULUS 16, REMAINDER %s) '
        'WITH (fillfactor = %s, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 10000, '
        'autovacuum_vacuum_insert_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.01, '
        'autovacuum_vacuum_cost_delay = 2, autovacuum_vacuum_cost_limit = 1000)',
        t || '_p' || lpad(i::text, 2, '0'), t, i,
        CASE WHEN t IN ('fact_links', 'entity_mentions') THEN 100 ELSE 90 END);
    END LOOP;
  END LOOP;
END $$;

-- =============================================================================
-- Indexes on the partitioned tables (created on the parent, propagated to every partition)
-- =============================================================================

-- chunks
CREATE INDEX chunks_doc_idx       ON chunks (namespace_id, document_id, ordinal);
CREATE INDEX chunks_mentioned_idx ON chunks (namespace_id, mentioned_at) WHERE live;
CREATE INDEX chunks_purge_idx     ON chunks (namespace_id, purge_after) WHERE purge_after IS NOT NULL;
CREATE INDEX chunks_embedding_hnsw ON chunks
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX chunks_bm25 ON chunks
  USING bm25 (chunk_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, tag_count, (tags::pdb.literal))
  WITH (key_field = 'chunk_id');
-- pg_search:end

-- facts
CREATE INDEX facts_doc_idx          ON facts (namespace_id, document_id);
CREATE INDEX facts_mentioned_idx    ON facts (namespace_id, mentioned_at DESC) WHERE live;
CREATE INDEX facts_occurred_idx     ON facts (namespace_id, occurred_start)   -- temporal arm: two-sided probe around query_timestamp (N68)
  WHERE live AND occurred_start IS NOT NULL;
CREATE INDEX facts_occurred_gist    ON facts                                   -- explicit occurrence-window filters (lists, Recall filters), not the arm
  USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
  WHERE live AND occurred_start IS NOT NULL;
CREATE INDEX facts_tags_gin         ON facts USING gin (namespace_id, tags) WHERE live;
-- (N95: facts_unconsolidated_idx is gone with consolidated_at; pending facts come from fact_consolidation + consolidation_state)
CREATE INDEX facts_purge_idx        ON facts (namespace_id, purge_after) WHERE purge_after IS NOT NULL;
CREATE INDEX facts_embedding_hnsw   ON facts
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- Semantic-arm plan by namespace size, same rule for ALL THREE vector arms (N55, N94): facts,
-- chunks and observation_versions each have a shared HNSW per partition and an exact-scan path.
--   live rows of the namespace < 20,000          -> exact scan (SET LOCAL enable_indexscan = off;
--                                                   bitmap scan on the namespace-leading live index +
--                                                   top-K sort; <= 20,000 rows x ~3 KB heap because
--                                                   embeddings are STORAGE MAIN, i.e. inline; cost to
--                                                   be MEASURED cold in M0.6, the old "30 MB, 5 to 15
--                                                   ms" figure is withdrawn)
--   >= 20,000 and < 2 % of the partition's live rows -> a PARTIAL per-namespace HNSW built by
--                                                   engramctl (owner engram_migrate):
--     CREATE INDEX CONCURRENTLY facts_hnsw_ns_<12 hex of namespace_id> ON facts_pNN
--       USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128)
--       WHERE namespace_id = '<namespace_id>';
--     (same for chunks_pNN and observation_versions_pNN; the arm binds the namespace id as a
--     literal so the predicate is provable; dropped at purge or move cleanup; on a move target
--     only after copy and catch-up, before cutover, N89)
--   >= 2 % of the partition's live rows            -> the shared index serves (the filter passes
--                                                   often enough for relaxed_order to reach 150).
-- The partial index only helps where the shared index fails: a namespace below 2 % of its
-- partition. At the 10 M facts/shard target a partition holds ~625 k live facts, so 2 % is
-- 12.5 k < 20 k and the band is EMPTY; it opens above ~1 M live rows per partition (16 M per
-- shard). hnsw.ef_search >= the arm cap (150 at MID, 400 at HIGH); the observation arm's
-- ef_search / max_scan_tuples come from the M0.6 recall measurement.
-- pg_search:begin
CREATE INDEX facts_bm25 ON facts
  USING bm25 (memory_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, fact_type_code,
              tag_count, (tags::pdb.literal))
  WITH (key_field = 'memory_id');
-- pg_search:end

-- fact_links: the PK serves forward expansion; the reverse index serves the other direction
-- and the purge (links are not touched by the synchronous delete cascade, N61)
CREATE INDEX fact_links_reverse_idx ON fact_links (namespace_id, dst_memory_id, src_memory_id);

-- entity_mentions
CREATE INDEX entity_mentions_entity_idx ON entity_mentions (namespace_id, entity_id, memory_id);

-- observation_versions (N94: hash-partitioned like facts; ~1/20 of facts; indexes created on the
-- parent propagate to the 16 partitions; live also requires hidden_by_invalidation = 0, N84)
CREATE INDEX observation_versions_embedding_hnsw ON observation_versions
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX observation_versions_bm25 ON observation_versions
  USING bm25 (ov_id, (text::pdb.unicode_words), namespace_id, observation_id, live, effective_at, superseded_at,
              tag_count, (tags::pdb.literal))
  WITH (key_field = 'ov_id');
-- pg_search:end

-- N84: the invalidation counter is maintained by the database so no code path can double count:
-- the trigger fires only when invalidated_at goes NULL -> set (Invalidate) or set -> NULL (Restore).
CREATE TRIGGER facts_invalidation_counter AFTER UPDATE OF invalidated_at ON facts
  FOR EACH ROW WHEN ((OLD.invalidated_at IS NULL) <> (NEW.invalidated_at IS NULL))
  EXECUTE FUNCTION engram_fact_invalidation_changed();

-- =============================================================================
-- Row-Level Security (D2): policy ns_isolation on every table that has a namespace_id column,
-- parents and partitions alike, ENABLEd and FORCEd (the section 8 --check-rls test asserts
-- both). Partitions carry the policy too, so a direct partition reference (which no
-- non-admin role is granted) is still confined.
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

-- N103 item 5 / N91: engram_move writes (INSERT/UPDATE/DELETE) only into a namespace whose
-- ownership row is 'incoming', i.e. on the TARGET. RESTRICTIVE policies are ANDed with
-- ns_isolation and apply to engram_move only. namespace_ownership (its own trigger/state machine)
-- and move_applied (written on both shards) are exempt.
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
       AND c.relname NOT IN ('namespace_ownership', 'move_applied')
  LOOP
    EXECUTE format('CREATE POLICY move_target_ins ON %s AS RESTRICTIVE FOR INSERT TO engram_move '
                   'WITH CHECK ((SELECT engram_ns_is_incoming()))', r.rel);
    EXECUTE format('CREATE POLICY move_target_upd ON %s AS RESTRICTIVE FOR UPDATE TO engram_move '
                   'USING ((SELECT engram_ns_is_incoming())) WITH CHECK ((SELECT engram_ns_is_incoming()))', r.rel);
    EXECUTE format('CREATE POLICY move_target_del ON %s AS RESTRICTIVE FOR DELETE TO engram_move '
                   'USING ((SELECT engram_ns_is_incoming()))', r.rel);
  END LOOP;
END $$;

-- Relay role: all-namespace SELECT on outbox only (D6/N4). The deletion-log consumer needs no
-- other table: the DocumentDeleted/NamespaceDeleted events carry the deletion_log key fields.
CREATE POLICY relay_read_all ON outbox FOR SELECT TO engram_relay USING (true);

-- =============================================================================
-- Grants. Only parent tables are granted: partitions stay ungranted, so a direct partition
-- reference by engram_app/engram_move fails with permission denied (a query through the
-- parent checks parent privileges only).
-- =============================================================================
GRANT USAGE ON SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
GRANT USAGE ON SEQUENCE outbox_seq TO engram_app, engram_move, engram_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
-- N93: the SECURITY DEFINER cleanup is callable by the move executor and admin only.
REVOKE EXECUTE ON FUNCTION engram_cleanup_namespace(uuid, integer) FROM PUBLIC, engram_app, engram_relay;
GRANT SELECT ON shard_meta, ownership_transitions TO engram_app, engram_relay, engram_move, engram_admin;

-- engram_app: ordinary namespace transactions
GRANT SELECT, INSERT, UPDATE, DELETE ON
  documents, document_versions, document_version_chunks, chunks, facts, fact_links,
  entities, entity_aliases, entity_mentions,
  observations, observation_versions, observation_sources, observation_inputs,
  observation_version_lineage, observation_version_sources, fact_consolidation, consolidation_state,
  consolidation_batches, consolidation_proposals, consolidation_applied, batch_jobs,
  pages, page_versions, page_sources,
  operations, idempotency_keys, token_usage_events, token_usage, quota_counters,
  blob_tombstones, export_snapshots, namespace_stats
  TO engram_app;
GRANT SELECT, INSERT ON ingest_ledger TO engram_app;          -- append-only
GRANT SELECT, INSERT ON outbox TO engram_app;                 -- never UPDATE/DELETE
GRANT SELECT, INSERT ON deletion_log TO engram_app;
GRANT SELECT ON namespace_ownership TO engram_app;            -- fence read: plain SELECT under the shared advisory lock (no row lock; review F-5)
GRANT INSERT ON namespace_ownership, namespace_stats TO engram_app;   -- CreateNamespace's first 'active'/epoch-1 row (the trigger admits nothing else from this role)
GRANT UPDATE (state, freeze_reason) ON namespace_ownership TO engram_app;   -- N101: the delete freeze (active -> frozen/delete) is the only edge the trigger admits for this role

-- engram_relay: the outbox stream and its own cursor rows; nothing else (D6)
GRANT SELECT ON outbox TO engram_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox_cursors TO engram_relay;

-- engram_move (N103 item 5): DML grants are table-level, the TARGET-ONLY restriction is the
-- RESTRICTIVE policies move_target_* (a write passes only while the namespace is 'incoming'). On the
-- source the role reads, runs the ownership transitions, and writes move_applied / outbox_cursors.
-- ingest_ledger: INSERT only (the ledger is append-only; cleanup runs through
-- engram_cleanup_namespace, N93).
GRANT SELECT, INSERT, UPDATE, DELETE ON
  namespace_ownership, move_applied, namespace_stats,
  documents, document_versions, document_version_chunks, chunks, facts, fact_links,
  entities, entity_aliases, entity_mentions,
  observations, observation_versions, observation_sources, observation_inputs,
  observation_version_lineage, observation_version_sources, fact_consolidation, consolidation_state,
  consolidation_batches, consolidation_proposals, consolidation_applied, batch_jobs,
  pages, page_versions, page_sources,
  operations, idempotency_keys, token_usage_events, token_usage, quota_counters,
  blob_tombstones, export_snapshots, deletion_log, outbox_skipped
  TO engram_move;
GRANT SELECT, INSERT ON ingest_ledger TO engram_move;
GRANT SELECT, INSERT ON outbox TO engram_move;
GRANT SELECT, INSERT, UPDATE ON outbox_cursors TO engram_move;   -- its move:<ns> cursor on the source
-- N91: the loader switches off FK, append-only and touch triggers for its own session only.
-- The ownership trigger is ENABLE ALWAYS and RLS is unaffected by replica mode.
GRANT SET ON PARAMETER session_replication_role TO engram_move;
GRANT EXECUTE ON FUNCTION engram_cleanup_namespace(uuid, integer) TO engram_move, engram_admin;

-- engram_admin: engramctl, purge, schedulers, retention
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO engram_admin;
REVOKE INSERT, UPDATE, DELETE ON ownership_transitions FROM engram_admin;   -- the machine is data nobody edits at run time
GRANT SELECT ON schedulable_namespaces, purgeable_namespaces TO engram_admin;

-- =============================================================================
-- Self-checks: fail the migration if an invariant of this file is violated.
-- =============================================================================
DO $$
DECLARE
  missing text;
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
     AND c.relname NOT IN ('shard_meta', 'outbox_cursors', 'ownership_transitions', 'goose_db_version');
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

  -- 7. the move executor (N91, N103): no BYPASSRLS loader role exists, engram_move may set
  --    session_replication_role, holds no DELETE on the ledger, and the ownership trigger is
  --    ENABLE ALWAYS (tgenabled = 'A') so replica mode cannot switch it off
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_move_load') THEN
    RAISE EXCEPTION 'engram_move_load must not exist (N91)';
  END IF;
  IF NOT has_parameter_privilege('engram_move', 'session_replication_role', 'SET') THEN
    RAISE EXCEPTION 'engram_move must be allowed to SET session_replication_role (N91)';
  END IF;
  IF has_table_privilege('engram_move', 'ingest_ledger', 'DELETE') OR has_table_privilege('engram_move', 'ingest_ledger', 'UPDATE') THEN
    RAISE EXCEPTION 'engram_move must not hold UPDATE/DELETE on ingest_ledger';
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
     AND c.relname NOT IN ('namespace_ownership', 'move_applied')
     AND (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname LIKE 'move_target_%' AND NOT p.polpermissive) <> 3;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without the move_target_* restrictive policies: %', missing;
  END IF;

  -- 9. the cleanup function is SECURITY DEFINER and not executable by the app or relay roles
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'engram_cleanup_namespace' AND prosecdef)
     OR has_function_privilege('engram_app', 'engram_cleanup_namespace(uuid, integer)', 'EXECUTE')
     OR has_function_privilege('engram_relay', 'engram_cleanup_namespace(uuid, integer)', 'EXECUTE') THEN
    RAISE EXCEPTION 'engram_cleanup_namespace must be SECURITY DEFINER and executable by engram_move/engram_admin only (N93)';
  END IF;

  -- 10. the ownership state machine is loaded and the live-flag columns exist (N84, N94)
  IF (SELECT count(*) FROM ownership_transitions) < 15 THEN
    RAISE EXCEPTION 'ownership_transitions is not loaded';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'observation_versions' AND relkind = 'p') THEN
    RAISE EXCEPTION 'observation_versions must be hash-partitioned (N94)';
  END IF;
END $$;
