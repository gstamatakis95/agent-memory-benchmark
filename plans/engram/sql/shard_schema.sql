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
-- Roles (LOGIN; passwords are set by provisioning from the shard secret, section 9):
--   engram_migrate  owner of every object; DDL only (BYPASSRLS so data migrations can run)
--   engram_app      API + worker per-namespace transactions; NOBYPASSRLS
--   engram_relay    outbox relay; NOBYPASSRLS; extra policies grant all-namespace SELECT on
--                   outbox and deletion_log only (D6/N4); owns outbox_cursors; reads nothing else
--   engram_move     move executor (N2); NOBYPASSRLS, confined by the ordinary ns_isolation
--                   policy to the namespace being moved; full DML on namespace-scoped tables
--                   and namespace_ownership; may DELETE from ingest_ledger (move cleanup)
--   engram_admin    engramctl, purge workflows, shard-wide schedulers; BYPASSRLS
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
                                      'SUCCEEDED_WITH_ERRORS', 'FAILED', 'CANCELLED');

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

CREATE FUNCTION engram_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- ingest_ledger is append-only. UPDATE is never allowed. DELETE is allowed only to the purge
-- path (engram_admin: PurgeWorkflow after an acknowledged delete, namespace delete) and to
-- move cleanup (engram_move). Every other role gets 42501.
CREATE FUNCTION engram_forbid_ledger_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' AND current_user IN ('engram_admin', 'engram_move') THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'ingest_ledger is append-only (% by % refused)', TG_OP, current_user
    USING ERRCODE = '42501';
END $$;

-- namespace_ownership rows must carry this database's shard id and epochs never decrease.
CREATE FUNCTION engram_check_ownership() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  v_shard integer;
BEGIN
  SELECT shard_id INTO v_shard FROM shard_meta;
  IF v_shard IS NULL THEN
    RAISE EXCEPTION 'shard_meta is empty; provision the shard first' USING ERRCODE = '55000';
  END IF;
  IF NEW.shard_id <> v_shard THEN
    RAISE EXCEPTION 'ownership row for % names shard % but this database is shard %',
      NEW.namespace_id, NEW.shard_id, v_shard USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.epoch < OLD.epoch THEN
    RAISE EXCEPTION 'epoch must not decrease for namespace %', NEW.namespace_id USING ERRCODE = '23514';
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

-- Observations never outlive their sources (D12): after any deletion of observation_sources
-- rows, observations left with zero sources are retired (and so are their versions);
-- observations that lost >= 1 source are marked stale for reconsolidation. The application
-- emits the matching outbox events after reading the affected rows back; this trigger is
-- the invariant, not the event source. Fires for explicit deletes and for FK cascades alike.
CREATE FUNCTION engram_observation_sources_after_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE observations o
     SET retired_at = now(), stale = false, updated_at = now()
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
     SET stale = true, stale_since = coalesce(o.stale_since, now()), updated_at = now()
   WHERE (o.namespace_id, o.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND o.retired_at IS NULL
     AND o.stale = false;
  RETURN NULL;
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
-- Namespace-keyed tables that are not partitioned
-- =============================================================================

-- namespace_ownership: the fencing token (D2 row 4, D5). Mirrors the catalog for the
-- namespaces this shard hosts. The ONLY table with a shard_id column. move_applied_seq is the
-- target-side replay watermark of a move (D5 step 3): the highest seq such that every
-- source event <= it has been applied (gaps are tracked in the source relay row).
CREATE TABLE namespace_ownership (
  namespace_id      uuid PRIMARY KEY,
  tenant_id         text NOT NULL CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  shard_id          integer NOT NULL,
  epoch             bigint NOT NULL CHECK (epoch >= 1),
  state             ownership_state NOT NULL,
  move_id           uuid,
  move_applied_seq  bigint,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),                -- FK target for the tenant_id denormalisation
  CHECK (state <> 'incoming' OR move_id IS NOT NULL)
);

CREATE TRIGGER namespace_ownership_check BEFORE INSERT OR UPDATE ON namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram_check_ownership();

-- move_applied: target-side exactly-once ledger of replayed source events, keyed by
-- (namespace_id, seq) (D5). Rows are deleted when the move reaches done/rolled_back.
CREATE TABLE move_applied (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  seq           bigint NOT NULL,
  move_id       uuid NOT NULL,
  applied_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, seq),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

-- namespace_stats: live counters (max_facts quota, catalog facts_estimate, hot-namespace
-- detection; section 9 ND-9). Kept apart from namespace_ownership on purpose: every write
-- transaction holds FOR SHARE on the ownership row and an UPDATE there would serialise them.
CREATE TABLE namespace_stats (
  namespace_id        uuid PRIMARY KEY,
  tenant_id           text NOT NULL,
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
  chunks_done     integer NOT NULL DEFAULT 0 CHECK (chunks_done >= 0),
  created_at      timestamptz NOT NULL DEFAULT now(),
  activated_at    timestamptz,
  finished_at     timestamptz,
  PRIMARY KEY (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  FOREIGN KEY (namespace_id, ledger_id)   REFERENCES ingest_ledger (namespace_id, ledger_id)
) WITH (fillfactor = 80);

-- at most one active version per document
CREATE UNIQUE INDEX document_versions_active_uq ON document_versions (namespace_id, document_id)
  WHERE status = 'active';
CREATE INDEX document_versions_op_idx ON document_versions (namespace_id, operation_id);

-- chunks (hash-partitioned). Identity within a document = content hash of the text (N6: the
-- contextual header is hashed separately). Chunk text <= 8 KB (4,000 chars max, multibyte).
CREATE TABLE chunks (
  namespace_id     uuid NOT NULL,
  tenant_id        text NOT NULL,
  chunk_id         uuid NOT NULL,
  document_id      text NOT NULL,
  content_hash     bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(text)
  header_hash      bytea NOT NULL CHECK (octet_length(header_hash) = 32),    -- sha256(header) (N6)
  extraction_key   bytea NOT NULL CHECK (octet_length(extraction_key) = 32), -- sha256(content_hash||prompt_version||model||schema_version) = xcache key
  ordinal          integer NOT NULL CHECK (ordinal >= 0),                    -- position in the latest version
  heading_path     text NOT NULL DEFAULT '' CHECK (octet_length(heading_path) <= 1024),
  header           text NOT NULL DEFAULT '' CHECK (octet_length(header) <= 1024),  -- "[doc summary] > [heading path]"
  text             text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  tags             text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count        smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  mentioned_at     timestamptz NOT NULL,                                     -- = item timestamp (D9)
  embedding        halfvec(768) NOT NULL,                                    -- embeds header || text
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
  extraction_key       bytea NOT NULL CHECK (octet_length(extraction_key) = 32), -- = chunks.extraction_key at insert
  text                 text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
  fact_type            fact_type NOT NULL,
  fact_type_code       smallint GENERATED ALWAYS AS (CASE fact_type WHEN 'world' THEN 1 WHEN 'experience' THEN 2 END) STORED,
  w5                   jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(w5) = 'object'),  -- {who:[],what,when,where,why}
  occurred_start       timestamptz,
  occurred_end         timestamptz,
  mentioned_at         timestamptz NOT NULL,
  tags                 text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count            smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  metadata             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  embedding            halfvec(768) NOT NULL,
  extraction_version   integer NOT NULL CHECK (extraction_version >= 1),    -- extraction schema version
  prompt_version       text NOT NULL,                                        -- e.g. extract/v1
  model                text NOT NULL,                                        -- models.extract used
  embedding_model      text NOT NULL,
  created_at           timestamptz NOT NULL DEFAULT now(),
  consolidated_at      timestamptz,                                          -- consolidation watermark
  consolidation_note   text,
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
-- one query per inserted row on the largest table.
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
  merged_into     uuid,                           -- set by EntityMerged; the row stays so aliases still resolve
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
  proof_count       integer NOT NULL DEFAULT 0 CHECK (proof_count >= 0),
  stale             boolean NOT NULL DEFAULT false,      -- needs reconsolidation (source lost/invalidated)
  stale_since       timestamptz,
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),   -- consolidation scope tags
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  PRIMARY KEY (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (stale = (stale_since IS NOT NULL))
) WITH (fillfactor = 80);

CREATE INDEX observations_stale_idx ON observations (namespace_id, stale_since)
  WHERE stale AND retired_at IS NULL;
CREATE INDEX observations_tags_gin ON observations USING gin (namespace_id, tags) WHERE retired_at IS NULL;

CREATE TRIGGER observations_touch BEFORE UPDATE ON observations
  FOR EACH ROW EXECUTE FUNCTION engram_touch_updated_at();

-- Version v is visible at as_of = T iff effective_at <= T AND (superseded_at IS NULL OR
-- superseded_at > T), where superseded_at = min(effective_at) over later versions
-- (maintained when a later version is inserted). This is D9's "latest version with
-- effective_at <= T", precomputed so it is a plain filter for HNSW and BM25 scans.
-- tags and retired_at are denormalised from observations so both observation arms are
-- single-table queries (Top-K pushdown needs that).
CREATE TABLE observation_versions (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  ov_id             uuid NOT NULL,                    -- surrogate: the pg_search key_field must be one column
  observation_id    uuid NOT NULL,
  version           integer NOT NULL CHECK (version >= 1),
  text              text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  embedding         halfvec(768) NOT NULL,
  embedding_model   text NOT NULL,
  effective_at      timestamptz NOT NULL,             -- max(mentioned_at) over cited sources, clamped >= previous version (section 5)
  superseded_at     timestamptz,
  source_count      integer NOT NULL CHECK (source_count >= 1),
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count         smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  prompt_version    text NOT NULL,
  model             text NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  live              boolean GENERATED ALWAYS AS (retired_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, observation_id, version),
  UNIQUE (ov_id),                                     -- single-column uniqueness required by the BM25 key_field
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  CHECK (superseded_at IS NULL OR superseded_at >= effective_at)
);

CREATE INDEX observation_versions_effective_idx ON observation_versions (namespace_id, effective_at) WHERE live;

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

-- consolidation_batches / consolidation_applied: exactly-once effect (D12)
CREATE TABLE consolidation_batches (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  batch_key         bytea NOT NULL CHECK (octet_length(batch_key) = 32),   -- sha256(sorted memory ids || prompt_version || model)
  round_id          uuid NOT NULL,
  memory_ids        uuid[] NOT NULL CHECK (cardinality(memory_ids) BETWEEN 1 AND 8),
  state             text NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'running', 'applied', 'bisected', 'failed')),
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
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
);

CREATE INDEX consolidation_applied_batch_idx ON consolidation_applied (namespace_id, batch_key, op_index);

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
                                                     'reflect', 'page_refresh', 'export', 'purge', 'move')),
  state                operation_state NOT NULL DEFAULT 'PENDING',
  request_id           text CHECK (octet_length(request_id) <= 128),
  target_id            text,                           -- document_id / page_id / snapshot version, per kind
  workflow_id          text NOT NULL,                  -- ns/{namespace_id}/op/{operation_id}
  task_queue           text NOT NULL,                  -- shard-{shard_id} at submission; restarted on the target after a move
  submitted_epoch      bigint NOT NULL CHECK (submitted_epoch >= 1),   -- audit only; never part of an identity (D11)
  workflow_started_at  timestamptz,
  progress             jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),  -- units_total, units_done, facts_created, consolidation_lag_s
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
  CHECK ((state IN ('SUCCEEDED', 'SUCCEEDED_WITH_ERRORS', 'FAILED', 'CANCELLED')) = (finished_at IS NOT NULL))
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
  calls              bigint NOT NULL DEFAULT 0 CHECK (calls >= 0),
  prompt_tokens      bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
  completion_tokens  bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
  cost_micros        bigint NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, day, op, model),
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
                                                  'move_cleanup', 'export_expire', 'page_retire')),
  operation_id   uuid,
  attempts       integer NOT NULL DEFAULT 0,
  last_error     text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, tombstone_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

CREATE INDEX blob_tombstones_created_idx ON blob_tombstones (namespace_id, created_at);

-- export_snapshots (D12): one row per snapshot version
CREATE TABLE export_snapshots (
  namespace_id        uuid NOT NULL,
  tenant_id           text NOT NULL,
  version             integer NOT NULL CHECK (version >= 1),
  state               text NOT NULL DEFAULT 'building' CHECK (state IN ('building', 'ready', 'expired', 'failed')),
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
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

-- =============================================================================
-- Partitions: 16 hash partitions for the four big tables. Storage parameters must be set per
-- partition (a partitioned parent cannot carry them).
-- =============================================================================
DO $$
DECLARE
  t text;
  i integer;
BEGIN
  FOREACH t IN ARRAY ARRAY['chunks', 'facts', 'fact_links', 'entity_mentions'] LOOP
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
CREATE INDEX facts_occurred_gist    ON facts
  USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
  WHERE live AND occurred_start IS NOT NULL;
CREATE INDEX facts_tags_gin         ON facts USING gin (namespace_id, tags) WHERE live;
CREATE INDEX facts_unconsolidated_idx ON facts (namespace_id, created_at) WHERE live AND consolidated_at IS NULL;
CREATE INDEX facts_purge_idx        ON facts (namespace_id, purge_after) WHERE purge_after IS NOT NULL;
CREATE INDEX facts_embedding_hnsw   ON facts
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX facts_bm25 ON facts
  USING bm25 (memory_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, fact_type_code,
              tag_count, (tags::pdb.literal))
  WITH (key_field = 'memory_id');
-- pg_search:end

-- fact_links: the PK serves forward expansion; the reverse index serves the other direction
-- and the delete cascade
CREATE INDEX fact_links_reverse_idx ON fact_links (namespace_id, dst_memory_id, src_memory_id);

-- entity_mentions
CREATE INDEX entity_mentions_entity_idx ON entity_mentions (namespace_id, entity_id, memory_id);

-- observation_versions (not partitioned; ~1/20 of facts)
CREATE INDEX observation_versions_embedding_hnsw ON observation_versions
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX observation_versions_bm25 ON observation_versions
  USING bm25 (ov_id, (text::pdb.unicode_words), namespace_id, observation_id, live, effective_at, superseded_at,
              tag_count, (tags::pdb.literal))
  WITH (key_field = 'ov_id');
-- pg_search:end

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

-- Relay role: all-namespace SELECT on the two tables it streams (D6/N4, section 8 allowlist).
CREATE POLICY relay_read_all ON outbox       FOR SELECT TO engram_relay USING (true);
CREATE POLICY relay_read_all ON deletion_log FOR SELECT TO engram_relay USING (true);

-- =============================================================================
-- Grants. Only parent tables are granted: partitions stay ungranted, so a direct partition
-- reference by engram_app/engram_move fails with permission denied (a query through the
-- parent checks parent privileges only).
-- =============================================================================
GRANT USAGE ON SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
GRANT USAGE ON SEQUENCE outbox_seq TO engram_app, engram_move, engram_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO engram_app, engram_relay, engram_move, engram_admin;
GRANT SELECT ON shard_meta TO engram_app, engram_relay, engram_move, engram_admin;

-- engram_app: ordinary namespace transactions
GRANT SELECT, INSERT, UPDATE, DELETE ON
  documents, document_versions, document_version_chunks, chunks, facts, fact_links,
  entities, entity_aliases, entity_mentions,
  observations, observation_versions, observation_sources,
  consolidation_batches, consolidation_applied, batch_jobs,
  pages, page_versions, page_sources,
  operations, idempotency_keys, token_usage_events, token_usage, quota_counters,
  blob_tombstones, export_snapshots, namespace_stats
  TO engram_app;
GRANT SELECT, INSERT ON ingest_ledger TO engram_app;          -- append-only
GRANT SELECT, INSERT ON outbox TO engram_app;                 -- never UPDATE/DELETE
GRANT SELECT, INSERT ON deletion_log TO engram_app;
GRANT SELECT ON namespace_ownership TO engram_app;            -- fence read (FOR SHARE) only

-- engram_relay: outbox + deletion_log stream, its own cursor rows; nothing else
GRANT SELECT ON outbox, deletion_log TO engram_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON outbox_cursors TO engram_relay;

-- engram_move: everything a namespace copy/replay/cleanup needs, confined by RLS to the
-- namespace in scope; ownership transitions (incoming/active/frozen/moved_out) included
GRANT SELECT, INSERT, UPDATE, DELETE ON
  namespace_ownership, move_applied, namespace_stats,
  ingest_ledger, documents, document_versions, document_version_chunks, chunks, facts, fact_links,
  entities, entity_aliases, entity_mentions,
  observations, observation_versions, observation_sources,
  consolidation_batches, consolidation_applied, batch_jobs,
  pages, page_versions, page_sources,
  operations, idempotency_keys, token_usage_events, token_usage, quota_counters,
  blob_tombstones, export_snapshots, deletion_log, outbox_skipped
  TO engram_move;
GRANT SELECT, INSERT ON outbox TO engram_move;
GRANT SELECT, INSERT, UPDATE ON outbox_cursors TO engram_move;   -- its move:<ns> cursor on the source

-- engram_admin: engramctl, purge, schedulers, retention
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO engram_admin;

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
     AND c.relname NOT IN ('shard_meta', 'outbox_cursors', 'goose_db_version');
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
END $$;
