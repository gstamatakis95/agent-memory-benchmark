-- =============================================================================
-- Engram shard schema (identical on every shard)
-- Target: PostgreSQL 16, image paradedb/paradedb:latest-pg16
--   pgvector >= 0.8 (halfvec, hnsw.iterative_scan), pg_search >= 0.25 (USING bm25 is an
--   alias of USING paradedb; pdb.score / ||| / === operators), pg_trgm, btree_gin, btree_gist.
--   pg_search requires shared_preload_libraries = 'pg_search' (set in the shard's postgresql.conf).
-- Plain SQL (no goose annotations). Apply as engram_migrate (object owner).
--
-- Shard identity: this DDL is identity-free. Provisioning (engramctl shard provision) runs once:
--   ALTER DATABASE engram SET engram.shard_id = '7';
-- Every store connection verifies SHOW engram.shard_id against the catalog's shard_id (section 2),
-- and the namespace_ownership trigger below refuses rows whose shard_id disagrees with it.
--
-- Per-transaction scope (set by the store with set_config(..., true) = SET LOCAL):
--   engram.namespace_id  uuid   engram.tenant_id  text   engram.epoch  bigint
-- RLS policies read engram.namespace_id; unset -> ERROR 42704, empty -> ERROR 22P02 (fail closed).
--
-- Roles (LOGIN; passwords are set by provisioning from the shard secret):
--   engram_migrate  owner of all objects, DDL only
--   engram_app      API + worker per-namespace transactions, NOBYPASSRLS
--   engram_relay    outbox relay/retention + schedulers, NOBYPASSRLS, extra all-namespace policies
--   engram_admin    move executor, purge, engramctl: BYPASSRLS, may DELETE from ingest_ledger
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS vector;
-- pg_search:begin
CREATE EXTENSION IF NOT EXISTS pg_search;
-- pg_search:end
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA IF NOT EXISTS engram;
SET search_path = engram, public;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_app') THEN
    CREATE ROLE engram_app LOGIN NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_relay') THEN
    CREATE ROLE engram_relay LOGIN NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'engram_admin') THEN
    CREATE ROLE engram_admin LOGIN BYPASSRLS;
  END IF;
END $$;

-- -----------------------------------------------------------------------------
-- Enumerations
-- -----------------------------------------------------------------------------
CREATE TYPE engram.fact_type        AS ENUM ('world', 'experience');
CREATE TYPE engram.link_type        AS ENUM ('entity', 'temporal', 'semantic', 'causal');
CREATE TYPE engram.ownership_state  AS ENUM ('incoming', 'active', 'frozen', 'moved_out');
CREATE TYPE engram.document_state   AS ENUM ('active', 'deleting', 'deleted');
CREATE TYPE engram.version_status   AS ENUM ('ingesting', 'active', 'superseded', 'deleted');
CREATE TYPE engram.update_mode      AS ENUM ('replace', 'append');
CREATE TYPE engram.operation_state  AS ENUM ('PENDING', 'RUNNING', 'DEFERRED', 'SUCCEEDED', 'FAILED', 'CANCELLED');

-- -----------------------------------------------------------------------------
-- Helper functions
-- -----------------------------------------------------------------------------

-- UUIDv7 for ops scripts and tests only; the application mints ids (google/uuid v7).
CREATE FUNCTION engram.uuid_v7() RETURNS uuid
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
-- collation (= sorted and deduplicated, bytewise, exactly what the Go normaliser produces).
-- Set semantics of the five tag modes rely on this (section 3.8).
CREATE FUNCTION engram.tags_valid(t text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT cardinality(t) <= 32
     AND NOT EXISTS (SELECT 1 FROM unnest(t) AS x WHERE x !~ '^[a-z0-9][a-z0-9._:/-]{0,63}$')
     AND coalesce((SELECT bool_and(t[i] COLLATE "C" < t[i + 1] COLLATE "C")
                     FROM generate_series(1, cardinality(t) - 1) AS i), true);
$$;

CREATE FUNCTION engram.touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- ingest_ledger is append-only. UPDATE is never allowed. DELETE is allowed only to
-- engram_admin (PurgeWorkflow after an acknowledged delete, namespace delete, move cleanup).
CREATE FUNCTION engram.forbid_ledger_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' AND current_user = 'engram_admin' THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'ingest_ledger is append-only (% by % refused)', TG_OP, current_user
    USING ERRCODE = '42501';
END $$;

-- namespace_ownership rows must carry this database's shard id.
CREATE FUNCTION engram.check_ownership_shard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.shard_id <> current_setting('engram.shard_id')::integer THEN
    RAISE EXCEPTION 'ownership row for % names shard % but this database is shard %',
      NEW.namespace_id, NEW.shard_id, current_setting('engram.shard_id')
      USING ERRCODE = '23514';
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.epoch < OLD.epoch THEN
    RAISE EXCEPTION 'epoch must not decrease for namespace %', NEW.namespace_id USING ERRCODE = '23514';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- Observations never outlive their sources (D12): after any deletion of observation_sources rows,
-- observations left with zero sources are retired; observations that lost >= 1 source are marked
-- stale for reconsolidation. The application emits the matching outbox events (it reads back the
-- affected observations); this trigger is the invariant, not the event source.
CREATE FUNCTION engram.observation_sources_after_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE engram.observations o
     SET retired_at = now(), stale = false, updated_at = now()
   WHERE (o.namespace_id, o.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND o.retired_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM engram.observation_sources s
                      WHERE s.namespace_id = o.namespace_id AND s.observation_id = o.observation_id);
  UPDATE engram.observation_versions v
     SET retired_at = now()
   WHERE (v.namespace_id, v.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND v.retired_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM engram.observation_sources s
                      WHERE s.namespace_id = v.namespace_id AND s.observation_id = v.observation_id);
  UPDATE engram.observations o
     SET stale = true, updated_at = now()
   WHERE (o.namespace_id, o.observation_id) IN (SELECT DISTINCT d.namespace_id, d.observation_id FROM deleted d)
     AND o.retired_at IS NULL
     AND o.stale = false;
  RETURN NULL;
END $$;

-- =============================================================================
-- Shard-global tables (keyed by namespace, but not partitioned)
-- =============================================================================

-- namespace_ownership: the fencing token (D2 row 4, D5). Mirrors the catalog for hosted namespaces.
-- shard_id is the ONLY place a shard id appears in a shard database (it is otherwise implied by
-- the database itself). move_applied_seq is the target-side replay cursor of a move (D5 step 3),
-- folded in here because it is one value per (namespace, move).
CREATE TABLE engram.namespace_ownership (
  namespace_id      uuid PRIMARY KEY,
  tenant_id         text NOT NULL CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  shard_id          integer NOT NULL,
  epoch             bigint NOT NULL CHECK (epoch >= 1),
  state             engram.ownership_state NOT NULL,
  move_id           uuid,
  move_applied_seq  bigint,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),                -- FK target for tenant_id denormalisation
  CHECK (state <> 'incoming' OR move_id IS NOT NULL)
);

CREATE TRIGGER namespace_ownership_shard BEFORE INSERT OR UPDATE ON engram.namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram.check_ownership_shard();

-- namespace_stats: live counters (max_facts quota, catalog facts_estimate reporting).
-- Separate from namespace_ownership on purpose: every write transaction holds FOR SHARE on the
-- ownership row, and an UPDATE there would serialise against all of them.
CREATE TABLE engram.namespace_stats (
  namespace_id        uuid PRIMARY KEY,
  tenant_id           text NOT NULL,
  facts_live          bigint NOT NULL DEFAULT 0 CHECK (facts_live >= 0),
  facts_total         bigint NOT NULL DEFAULT 0 CHECK (facts_total >= 0),
  chunks_live         bigint NOT NULL DEFAULT 0 CHECK (chunks_live >= 0),
  documents_live      bigint NOT NULL DEFAULT 0 CHECK (documents_live >= 0),
  observations_live   bigint NOT NULL DEFAULT 0 CHECK (observations_live >= 0),
  bytes_estimate      bigint NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- outbox: transactional outbox and per-shard change log (D6). PK is the global seq (the relay reads
-- in seq order); (namespace_id, seq) serves move consumers and export deltas. namespace_id,
-- tenant_id and epoch are stamped from the transaction scope, never passed by the application.
CREATE SEQUENCE engram.outbox_seq AS bigint CACHE 1;

CREATE TABLE engram.outbox (
  seq           bigint PRIMARY KEY DEFAULT nextval('engram.outbox_seq'),
  namespace_id  uuid   NOT NULL DEFAULT current_setting('engram.namespace_id')::uuid,
  tenant_id     text   NOT NULL DEFAULT current_setting('engram.tenant_id'),
  epoch         bigint NOT NULL DEFAULT current_setting('engram.epoch')::bigint,
  event_type    text   NOT NULL CHECK (event_type IN (
                  'FactUpserted', 'FactRetired', 'FactInvalidated', 'FactRestored',
                  'ChunkUpserted', 'ChunkRetired', 'DocumentDeleted',
                  'ObservationUpserted', 'ObservationRetired',
                  'PageStale', 'PageUpserted', 'EntityUpserted', 'EntityMerged', 'OperationFinished')),
  payload       bytea  NOT NULL CHECK (octet_length(payload) <= 65536),  -- engram.internal.events.v1.Event
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_ns_seq_idx ON engram.outbox (namespace_id, seq);

CREATE TABLE engram.outbox_cursors (
  consumer    text PRIMARY KEY CHECK (consumer ~ '^(index|kafka|move:[0-9a-f-]{36})$'),
  last_seq    bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  updated_at  timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);

-- outbox_gaps: persisted gap watchlist of the relay (D6). A seq that is missing for more than 1 s is
-- recorded here so that a relay failover cannot lose it; resolved or expired gaps are deleted.
CREATE TABLE engram.outbox_gaps (
  seq            bigint PRIMARY KEY,
  first_seen_at  timestamptz NOT NULL DEFAULT now(),
  deadline       timestamptz NOT NULL           -- first_seen_at + 2 x statement_timeout (60 s)
);

-- =============================================================================
-- Namespace-scoped tables
-- =============================================================================

-- ingest_ledger: append-only raw inputs. Body inline when <= 64 KiB, otherwise blob only.
CREATE TABLE engram.ingest_ledger (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  ledger_id       uuid NOT NULL,
  document_id     text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  operation_id    uuid NOT NULL,
  request_id      text CHECK (octet_length(request_id) <= 128),
  update_mode     engram.update_mode NOT NULL,
  item_timestamp  timestamptz NOT NULL,
  context         text NOT NULL DEFAULT '' CHECK (octet_length(context) <= 4096),
  tags            text[] NOT NULL DEFAULT '{}' CHECK (engram.tags_valid(tags)),
  metadata        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  entity_hints    jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(entity_hints) = 'array'),
  content_hash    bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(body)
  content_bytes   integer NOT NULL CHECK (content_bytes BETWEEN 0 AND 1048576),
  body            text,
  body_blob_key   text NOT NULL,                  -- ingest/{ledger_id}
  received_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, ledger_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id),
  CHECK ((content_bytes <= 65536) = (body IS NOT NULL))
) WITH (fillfactor = 100);

CREATE INDEX ingest_ledger_doc_idx ON engram.ingest_ledger (namespace_id, document_id, received_at DESC);
CREATE INDEX ingest_ledger_op_idx  ON engram.ingest_ledger (namespace_id, operation_id);

CREATE TRIGGER ingest_ledger_append_only BEFORE UPDATE OR DELETE ON engram.ingest_ledger
  FOR EACH ROW EXECUTE FUNCTION engram.forbid_ledger_mutation();

-- documents / document_versions (D8)
CREATE TABLE engram.documents (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  document_id       text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  state             engram.document_state NOT NULL DEFAULT 'active',
  item_timestamp    timestamptz,
  context           text NOT NULL DEFAULT '',
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram.tags_valid(tags)),
  metadata          jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  summary_blob_key  text,                         -- docsum/{sha256(content_hash || prompt_version || model)}.json
  summary_hash      bytea CHECK (summary_hash IS NULL OR octet_length(summary_hash) = 32),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  PRIMARY KEY (namespace_id, document_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id),
  CHECK ((state = 'active') = (deleted_at IS NULL))
) WITH (fillfactor = 80);

CREATE INDEX documents_updated_idx ON engram.documents (namespace_id, updated_at DESC);
CREATE INDEX documents_deleting_idx ON engram.documents (namespace_id, deleted_at) WHERE state = 'deleting';

CREATE TRIGGER documents_touch BEFORE UPDATE ON engram.documents
  FOR EACH ROW EXECUTE FUNCTION engram.touch_updated_at();

CREATE TABLE engram.document_versions (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  document_id     text NOT NULL,
  version         integer NOT NULL CHECK (version >= 1),
  content_hash    bytea NOT NULL CHECK (octet_length(content_hash) = 32),
  status          engram.version_status NOT NULL DEFAULT 'ingesting',
  update_mode     engram.update_mode NOT NULL,
  operation_id    uuid NOT NULL,
  ledger_id       uuid NOT NULL,
  chunk_count     integer CHECK (chunk_count >= 0),
  chunks_done     integer NOT NULL DEFAULT 0 CHECK (chunks_done >= 0),
  created_at      timestamptz NOT NULL DEFAULT now(),
  activated_at    timestamptz,
  finished_at     timestamptz,
  PRIMARY KEY (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, document_id) REFERENCES engram.documents (namespace_id, document_id),
  FOREIGN KEY (namespace_id, ledger_id)   REFERENCES engram.ingest_ledger (namespace_id, ledger_id)
) WITH (fillfactor = 80);

-- at most one active version per document
CREATE UNIQUE INDEX document_versions_active_uq ON engram.document_versions (namespace_id, document_id)
  WHERE status = 'active';
CREATE INDEX document_versions_op_idx ON engram.document_versions (namespace_id, operation_id);

-- chunks (hash-partitioned). Identity within a document = content hash (D8).
CREATE TABLE engram.chunks (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  chunk_id        uuid NOT NULL,
  document_id     text NOT NULL,
  content_hash    bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(text)
  ordinal         integer NOT NULL CHECK (ordinal >= 0),                    -- position in the latest version
  header          text NOT NULL DEFAULT '' CHECK (octet_length(header) <= 1024),  -- "[doc summary] > [heading path]"
  text            text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  tags            text[] NOT NULL DEFAULT '{}' CHECK (engram.tags_valid(tags)),
  tag_count       smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  mentioned_at    timestamptz NOT NULL,                                     -- = item timestamp (D9)
  embedding       halfvec(768) NOT NULL,                                    -- embeds header || text
  embedding_model text NOT NULL,
  first_version   integer NOT NULL CHECK (first_version >= 1),
  last_version    integer NOT NULL CHECK (last_version >= first_version),
  created_at      timestamptz NOT NULL DEFAULT now(),
  retired_at      timestamptz,
  live            boolean GENERATED ALWAYS AS (retired_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, chunk_id),
  UNIQUE (namespace_id, document_id, content_hash),
  FOREIGN KEY (namespace_id, document_id) REFERENCES engram.documents (namespace_id, document_id)
) PARTITION BY HASH (namespace_id);

-- facts (hash-partitioned). memory_id is the public id of a fact (D1).
CREATE TABLE engram.facts (
  namespace_id        uuid NOT NULL,
  tenant_id           text NOT NULL,
  memory_id           uuid NOT NULL,
  document_id         text NOT NULL,
  chunk_id            uuid NOT NULL,
  ordinal             smallint NOT NULL CHECK (ordinal >= 0),               -- position in the chunk's extraction
  content_hash        bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(normalised text)
  text                text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
  fact_type           engram.fact_type NOT NULL,
  fact_type_code      smallint GENERATED ALWAYS AS (CASE fact_type WHEN 'world' THEN 1 WHEN 'experience' THEN 2 END) STORED,
  w5                  jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(w5) = 'object'),  -- {who:[],what,when,where,why}
  occurred_start      timestamptz,
  occurred_end        timestamptz,
  mentioned_at        timestamptz NOT NULL,
  tags                text[] NOT NULL DEFAULT '{}' CHECK (engram.tags_valid(tags)),
  tag_count           smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  metadata            jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
  embedding           halfvec(768) NOT NULL,
  extraction_version  integer NOT NULL CHECK (extraction_version >= 1),    -- extraction schema version
  prompt_version      text NOT NULL,                                        -- e.g. extract/v1
  model               text NOT NULL,                                        -- models.extract used
  embedding_model     text NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now(),
  retired_at          timestamptz,
  invalidated_at      timestamptz,
  live                boolean GENERATED ALWAYS AS (retired_at IS NULL AND invalidated_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, memory_id),
  UNIQUE (namespace_id, chunk_id, content_hash),                           -- CommitChunk idempotency
  FOREIGN KEY (namespace_id, chunk_id)    REFERENCES engram.chunks (namespace_id, chunk_id),
  FOREIGN KEY (namespace_id, document_id) REFERENCES engram.documents (namespace_id, document_id),
  CHECK (occurred_start IS NULL OR occurred_end IS NULL OR occurred_end >= occurred_start)
) PARTITION BY HASH (namespace_id);

-- fact_links (hash-partitioned). One row per edge. entity/temporal/semantic edges are undirected and
-- stored once in canonical order (src < dst); causal edges are directed (src = cause, dst = effect).
-- Per-fact caps (temporal <= 20, semantic <= 10, entity <= 10 per shared entity) are enforced by the
-- linker (section 5), not by the database: a counting trigger would cost a query per insert.
CREATE TABLE engram.fact_links (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  src_memory_id  uuid NOT NULL,
  dst_memory_id  uuid NOT NULL,
  link_type      engram.link_type NOT NULL,
  weight         real NOT NULL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),
  PRIMARY KEY (namespace_id, src_memory_id, dst_memory_id, link_type),
  FOREIGN KEY (namespace_id, src_memory_id) REFERENCES engram.facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, dst_memory_id) REFERENCES engram.facts (namespace_id, memory_id) ON DELETE CASCADE,
  CHECK (src_memory_id <> dst_memory_id),
  CHECK (link_type = 'causal' OR src_memory_id < dst_memory_id)
) PARTITION BY HASH (namespace_id);

-- entities / entity_aliases / entity_mentions
CREATE TABLE engram.entities (
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
  merged_into     uuid,                           -- set by EntityMerged; row kept so aliases still resolve
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, entity_id),
  FOREIGN KEY (namespace_id, tenant_id)   REFERENCES engram.namespace_ownership (namespace_id, tenant_id),
  FOREIGN KEY (namespace_id, merged_into) REFERENCES engram.entities (namespace_id, entity_id),
  CHECK (merged_into IS NULL OR merged_into <> entity_id)
) WITH (fillfactor = 80);

CREATE UNIQUE INDEX entities_canonical_uq ON engram.entities (namespace_id, canonical_norm)
  WHERE merged_into IS NULL;
-- fuzzy resolution: multi-column GIN (btree_gin for the uuid) so the index leads with namespace_id
CREATE INDEX entities_trgm_idx ON engram.entities USING gin (namespace_id, canonical_norm gin_trgm_ops);

CREATE TRIGGER entities_touch BEFORE UPDATE ON engram.entities
  FOR EACH ROW EXECUTE FUNCTION engram.touch_updated_at();

CREATE TABLE engram.entity_aliases (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  entity_id     uuid NOT NULL,
  alias         text NOT NULL CHECK (octet_length(alias) BETWEEN 1 AND 256),
  alias_norm    text NOT NULL CHECK (octet_length(alias_norm) BETWEEN 1 AND 256),
  source        text NOT NULL CHECK (source IN ('extracted', 'hint', 'merge')),
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, alias_norm),
  FOREIGN KEY (namespace_id, entity_id) REFERENCES engram.entities (namespace_id, entity_id) ON DELETE CASCADE
);

CREATE INDEX entity_aliases_entity_idx ON engram.entity_aliases (namespace_id, entity_id);

CREATE TABLE engram.entity_mentions (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  memory_id     uuid NOT NULL,
  entity_id     uuid NOT NULL,
  role          text NOT NULL DEFAULT 'other' CHECK (role IN ('who', 'what', 'where', 'when', 'why', 'other')),
  confidence    real NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
  PRIMARY KEY (namespace_id, memory_id, entity_id),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES engram.facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, entity_id) REFERENCES engram.entities (namespace_id, entity_id)
) PARTITION BY HASH (namespace_id);

-- observations / observation_versions / observation_sources (D9, D12)
CREATE TABLE engram.observations (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  observation_id    uuid NOT NULL,
  current_version   integer NOT NULL DEFAULT 0 CHECK (current_version >= 0),
  proof_count       integer NOT NULL DEFAULT 0 CHECK (proof_count >= 0),
  stale             boolean NOT NULL DEFAULT false,      -- needs reconsolidation (source lost / invalidated)
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram.tags_valid(tags)),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  PRIMARY KEY (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 80);

CREATE INDEX observations_stale_idx ON engram.observations (namespace_id, updated_at)
  WHERE stale AND retired_at IS NULL;

CREATE TRIGGER observations_touch BEFORE UPDATE ON engram.observations
  FOR EACH ROW EXECUTE FUNCTION engram.touch_updated_at();

-- Version v is visible at as_of = T iff effective_at <= T AND (superseded_at IS NULL OR superseded_at > T),
-- where superseded_at = min(effective_at) over later versions (maintained on version insert).
-- This is exactly D9's "latest version with effective_at <= T", precomputed so it is a plain filter.
-- tags and retired_at are denormalised from observations so the observation arms are single-table.
CREATE TABLE engram.observation_versions (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  ov_id             uuid NOT NULL,                    -- surrogate: pg_search key_field must be one column
  observation_id    uuid NOT NULL,
  version           integer NOT NULL CHECK (version >= 1),
  text              text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  embedding         halfvec(768) NOT NULL,
  embedding_model   text NOT NULL,
  effective_at      timestamptz NOT NULL,             -- max(mentioned_at) over cited source facts
  superseded_at     timestamptz,
  source_count      integer NOT NULL CHECK (source_count >= 1),
  tags              text[] NOT NULL DEFAULT '{}' CHECK (engram.tags_valid(tags)),
  tag_count         smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  prompt_version    text NOT NULL,
  model             text NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  live              boolean GENERATED ALWAYS AS (retired_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, observation_id, version),
  UNIQUE (ov_id),                                     -- single-column uniqueness required by the BM25 key_field
  FOREIGN KEY (namespace_id, observation_id) REFERENCES engram.observations (namespace_id, observation_id)
);

CREATE INDEX observation_versions_effective_idx ON engram.observation_versions (namespace_id, effective_at)
  WHERE live;

CREATE TABLE engram.observation_sources (
  namespace_id    uuid NOT NULL,
  tenant_id       text NOT NULL,
  observation_id  uuid NOT NULL,
  memory_id       uuid NOT NULL,
  quote           text NOT NULL DEFAULT '' CHECK (octet_length(quote) <= 2048),
  added_version   integer NOT NULL CHECK (added_version >= 1),
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, memory_id),
  FOREIGN KEY (namespace_id, observation_id) REFERENCES engram.observations (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, memory_id)      REFERENCES engram.facts (namespace_id, memory_id) ON DELETE CASCADE
);

CREATE INDEX observation_sources_memory_idx ON engram.observation_sources (namespace_id, memory_id);

CREATE TRIGGER observation_sources_orphans AFTER DELETE ON engram.observation_sources
  REFERENCING OLD TABLE AS deleted
  FOR EACH STATEMENT EXECUTE FUNCTION engram.observation_sources_after_delete();

-- consolidation_batches / consolidation_applied: exactly-once effect (D12)
CREATE TABLE engram.consolidation_batches (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  batch_key         bytea NOT NULL CHECK (octet_length(batch_key) = 32),   -- sha256(sorted fact ids || prompt_version || model)
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
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 80);

CREATE INDEX consolidation_batches_round_idx ON engram.consolidation_batches (namespace_id, round_id);

CREATE TABLE engram.consolidation_applied (
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
  FOREIGN KEY (namespace_id, batch_key) REFERENCES engram.consolidation_batches (namespace_id, batch_key)
);

CREATE INDEX consolidation_applied_batch_idx ON engram.consolidation_applied (namespace_id, batch_key, op_index);

-- pages / page_versions / page_sources (D12, phase 3)
CREATE TABLE engram.pages (
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
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  retired_at        timestamptz,
  PRIMARY KEY (namespace_id, page_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 80);

CREATE UNIQUE INDEX pages_name_uq ON engram.pages (namespace_id, name) WHERE retired_at IS NULL;
CREATE INDEX pages_stale_idx ON engram.pages (namespace_id, updated_at)
  WHERE (stale_write OR stale_delete) AND retired_at IS NULL;

CREATE TRIGGER pages_touch BEFORE UPDATE ON engram.pages
  FOR EACH ROW EXECUTE FUNCTION engram.touch_updated_at();

CREATE TABLE engram.page_versions (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  page_id            uuid NOT NULL,
  version            integer NOT NULL CHECK (version >= 1),
  markdown_blob_key  text NOT NULL,                  -- pages/{page_id}/v{version}.md
  effective_at       timestamptz NOT NULL,           -- max(effective_at / mentioned_at) over cited sources (D9)
  superseded_at      timestamptz,
  evidence_hash      bytea NOT NULL CHECK (octet_length(evidence_hash) = 32),
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, page_id, version),
  FOREIGN KEY (namespace_id, page_id) REFERENCES engram.pages (namespace_id, page_id)
);

CREATE TABLE engram.page_sources (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  page_id        uuid NOT NULL,
  source_kind    text NOT NULL CHECK (source_kind IN ('fact', 'observation')),
  source_id      uuid NOT NULL,                      -- memory_id or observation_id (no FK: polymorphic)
  version_added  integer NOT NULL CHECK (version_added >= 1),
  PRIMARY KEY (namespace_id, page_id, source_kind, source_id),
  FOREIGN KEY (namespace_id, page_id) REFERENCES engram.pages (namespace_id, page_id) ON DELETE CASCADE
);

CREATE INDEX page_sources_source_idx ON engram.page_sources (namespace_id, source_id);

-- operations: async work visible through OperationService (D13: DEFERRED on quota exhaustion)
CREATE TABLE engram.operations (
  namespace_id      uuid NOT NULL,
  tenant_id         text NOT NULL,
  operation_id      uuid NOT NULL,
  kind              text NOT NULL CHECK (kind IN ('retain', 'delete_document', 'delete_namespace', 'consolidate',
                                                  'reflect', 'page_refresh', 'export', 'purge', 'move')),
  state             engram.operation_state NOT NULL DEFAULT 'PENDING',
  request_id        text CHECK (octet_length(request_id) <= 128),
  document_id       text,
  workflow_id       text NOT NULL,                   -- ns/{namespace_id}/op/{operation_id}
  task_queue        text NOT NULL,                   -- shard-{shard_id} at submission (restarted on the target after a move)
  submitted_epoch   bigint NOT NULL CHECK (submitted_epoch >= 1),
  progress          jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),  -- chunks_total, chunks_done, facts_created, consolidation_lag_s
  deferred_until    timestamptz,
  deferred_reason   text,                            -- quota key, e.g. llm_tokens_per_day
  error             jsonb CHECK (error IS NULL OR jsonb_typeof(error) = 'object'),   -- google.rpc.Status as JSON
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  started_at        timestamptz,
  finished_at       timestamptz,
  PRIMARY KEY (namespace_id, operation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id),
  CHECK (state <> 'DEFERRED' OR deferred_until IS NOT NULL),
  CHECK ((state IN ('SUCCEEDED', 'FAILED', 'CANCELLED')) = (finished_at IS NOT NULL))
) WITH (fillfactor = 70);

CREATE INDEX operations_created_idx ON engram.operations (namespace_id, created_at DESC);
CREATE INDEX operations_active_idx  ON engram.operations (namespace_id, state, deferred_until)
  WHERE state IN ('PENDING', 'RUNNING', 'DEFERRED');

CREATE TRIGGER operations_touch BEFORE UPDATE ON engram.operations
  FOR EACH ROW EXECUTE FUNCTION engram.touch_updated_at();

-- idempotency_keys: request_id for unary writes, 24 h (D1)
CREATE TABLE engram.idempotency_keys (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  request_id     text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 128),
  method         text NOT NULL,                       -- full gRPC method name
  request_hash   bytea NOT NULL CHECK (octet_length(request_hash) = 32),
  operation_id   uuid,
  response       bytea,                               -- serialised response for replay (<= 64 KiB)
  created_at     timestamptz NOT NULL DEFAULT now(),
  expires_at     timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (namespace_id, request_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id),
  CHECK (response IS NULL OR octet_length(response) <= 65536)
);

-- shard-wide expiry sweep (documented exception to the namespace-leading rule)
CREATE INDEX idempotency_keys_expiry_idx ON engram.idempotency_keys (expires_at);

-- token_usage: per-namespace metering, moves with the namespace (D13)
CREATE TABLE engram.token_usage (
  namespace_id       uuid NOT NULL,
  tenant_id          text NOT NULL,
  day                date NOT NULL,
  op                 text NOT NULL CHECK (op IN ('extract', 'summarize', 'embed', 'consolidate', 'reflect', 'page', 'rerank')),
  model              text NOT NULL,
  calls              bigint NOT NULL DEFAULT 0 CHECK (calls >= 0),
  prompt_tokens      bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
  completion_tokens  bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
  cost_micros        bigint NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, day, op, model),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- quota_counters: per-namespace windows (llm_tokens_per_day, max_facts is read from namespace_stats)
CREATE TABLE engram.quota_counters (
  namespace_id   uuid NOT NULL,
  tenant_id      text NOT NULL,
  quota_key      text NOT NULL CHECK (quota_key IN ('llm_tokens', 'recalls', 'retains')),
  window_start   timestamptz NOT NULL,
  used           bigint NOT NULL DEFAULT 0 CHECK (used >= 0),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, quota_key, window_start),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);

-- blob_tombstones: asynchronous blob deletion queue (rows are deleted once the blob is gone)
CREATE TABLE engram.blob_tombstones (
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
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
);

CREATE INDEX blob_tombstones_created_idx ON engram.blob_tombstones (namespace_id, created_at);

-- export_snapshots (D12): one row per snapshot version
CREATE TABLE engram.export_snapshots (
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
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES engram.namespace_ownership (namespace_id, tenant_id)
);

-- =============================================================================
-- Partitions: 16 hash partitions for the four big tables.
-- Storage parameters must be set per partition (not on the partitioned parent).
-- =============================================================================
DO $$
DECLARE
  t text;
  i integer;
BEGIN
  FOREACH t IN ARRAY ARRAY['chunks', 'facts', 'fact_links', 'entity_mentions'] LOOP
    FOR i IN 0..15 LOOP
      EXECUTE format(
        'CREATE TABLE engram.%I PARTITION OF engram.%I FOR VALUES WITH (MODULUS 16, REMAINDER %s) '
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
CREATE INDEX chunks_doc_idx ON engram.chunks (namespace_id, document_id, ordinal);
CREATE INDEX chunks_mentioned_idx ON engram.chunks (namespace_id, mentioned_at) WHERE live;
CREATE INDEX chunks_retired_idx ON engram.chunks (namespace_id, retired_at) WHERE retired_at IS NOT NULL;
CREATE INDEX chunks_embedding_hnsw ON engram.chunks
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX chunks_bm25 ON engram.chunks
  USING bm25 (chunk_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, tag_count, (tags::pdb.literal))
  WITH (key_field = 'chunk_id');
-- pg_search:end

-- facts
CREATE INDEX facts_doc_idx ON engram.facts (namespace_id, document_id);
CREATE INDEX facts_mentioned_idx ON engram.facts (namespace_id, mentioned_at DESC) WHERE live;
CREATE INDEX facts_occurred_gist ON engram.facts
  USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
  WHERE live AND occurred_start IS NOT NULL;
CREATE INDEX facts_tags_gin ON engram.facts USING gin (namespace_id, tags) WHERE live;
CREATE INDEX facts_retired_idx ON engram.facts (namespace_id, retired_at) WHERE retired_at IS NOT NULL;
CREATE INDEX facts_embedding_hnsw ON engram.facts
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX facts_bm25 ON engram.facts
  USING bm25 (memory_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, fact_type_code,
              tag_count, (tags::pdb.literal))
  WITH (key_field = 'memory_id');
-- pg_search:end

-- fact_links: PK serves forward expansion; the reverse index serves the other direction and the cascade
CREATE INDEX fact_links_reverse_idx ON engram.fact_links (namespace_id, dst_memory_id, src_memory_id);

-- entity_mentions
CREATE INDEX entity_mentions_entity_idx ON engram.entity_mentions (namespace_id, entity_id, memory_id);

-- observation_versions (not partitioned; ~1/20 of facts)
CREATE INDEX observation_versions_embedding_hnsw ON engram.observation_versions
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
-- pg_search:begin
CREATE INDEX observation_versions_bm25 ON engram.observation_versions
  USING bm25 (ov_id, (text::pdb.unicode_words), namespace_id, observation_id, live, effective_at, superseded_at,
              tag_count, (tags::pdb.literal))
  WITH (key_field = 'ov_id');
-- pg_search:end

-- =============================================================================
-- Row-Level Security
-- Policy (D2): namespace_id = current_setting('engram.namespace_id')::uuid, on every namespace-scoped
-- table and on every partition (partitions carry the same policy so a direct partition reference,
-- which engram_app is not granted anyway, is still confined).
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
     WHERE n.nspname = 'engram' AND c.relkind IN ('r', 'p')
  LOOP
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', r.rel);
    EXECUTE format(
      'CREATE POLICY ns_isolation ON %s '
      'USING (namespace_id = current_setting(''engram.namespace_id'')::uuid) '
      'WITH CHECK (namespace_id = current_setting(''engram.namespace_id'')::uuid)', r.rel);
  END LOOP;
END $$;

-- Relay/scheduler role: all-namespace read of the outbox and ownership, retention deletes.
CREATE POLICY relay_read_all   ON engram.outbox              FOR SELECT TO engram_relay USING (true);
CREATE POLICY relay_delete_all ON engram.outbox              FOR DELETE TO engram_relay USING (true);
CREATE POLICY relay_read_all   ON engram.namespace_ownership FOR SELECT TO engram_relay USING (true);
CREATE POLICY relay_read_all   ON engram.namespace_stats     FOR SELECT TO engram_relay USING (true);
CREATE POLICY relay_read_all   ON engram.observations        FOR SELECT TO engram_relay USING (true);  -- consolidation scheduler
CREATE POLICY relay_read_all   ON engram.pages               FOR SELECT TO engram_relay USING (true);  -- page refresh scheduler
CREATE POLICY relay_read_all   ON engram.operations          FOR SELECT TO engram_relay USING (true);  -- DEFERRED resumer
CREATE POLICY relay_read_all   ON engram.idempotency_keys    FOR SELECT TO engram_relay USING (true);
CREATE POLICY relay_delete_all ON engram.idempotency_keys    FOR DELETE TO engram_relay USING (true);

-- =============================================================================
-- Grants. Only parent tables are granted: partitions stay ungranted so a direct partition reference
-- by engram_app fails with permission denied (queries through the parent check parent privileges only).
-- =============================================================================
GRANT USAGE ON SCHEMA engram TO engram_app, engram_relay, engram_admin;
GRANT USAGE ON SEQUENCE engram.outbox_seq TO engram_app, engram_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA engram TO engram_app, engram_relay, engram_admin;

GRANT SELECT, INSERT, UPDATE, DELETE ON
  engram.documents, engram.document_versions, engram.chunks, engram.facts, engram.fact_links,
  engram.entities, engram.entity_aliases, engram.entity_mentions,
  engram.observations, engram.observation_versions, engram.observation_sources,
  engram.consolidation_batches, engram.consolidation_applied,
  engram.pages, engram.page_versions, engram.page_sources,
  engram.operations, engram.idempotency_keys, engram.token_usage, engram.quota_counters,
  engram.blob_tombstones, engram.export_snapshots, engram.namespace_stats
  TO engram_app;
GRANT SELECT, INSERT ON engram.ingest_ledger TO engram_app;          -- append-only
GRANT SELECT, INSERT ON engram.outbox TO engram_app;                 -- never UPDATE/DELETE
GRANT SELECT ON engram.namespace_ownership TO engram_app;            -- fence read (FOR SHARE) only

GRANT SELECT, DELETE ON engram.outbox TO engram_relay;
GRANT SELECT, INSERT, UPDATE, DELETE ON engram.outbox_cursors, engram.outbox_gaps TO engram_relay;
GRANT SELECT ON engram.namespace_ownership, engram.namespace_stats, engram.observations, engram.pages,
                engram.operations TO engram_relay;
GRANT SELECT, DELETE ON engram.idempotency_keys TO engram_relay;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA engram TO engram_admin;

-- =============================================================================
-- Self-checks: fail the migration if an invariant of this file is violated.
-- =============================================================================
DO $$
DECLARE
  missing text;
BEGIN
  -- 1. every table with a namespace_id column has RLS enabled and the ns_isolation policy
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
   WHERE n.nspname = 'engram' AND c.relkind IN ('r', 'p')
     AND (NOT c.relrowsecurity
          OR NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname = 'ns_isolation'));
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without namespace RLS: %', missing;
  END IF;

  -- 2. every table with a namespace_id column also carries tenant_id (denormalised on every row)
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
   WHERE n.nspname = 'engram' AND c.relkind IN ('r', 'p')
     AND NOT EXISTS (SELECT 1 FROM pg_attribute b WHERE b.attrelid = c.oid AND b.attname = 'tenant_id');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'tables without tenant_id: %', missing;
  END IF;

  -- 3. engram_app holds no privilege on any partition
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_inherits i ON i.inhrelid = c.oid
   WHERE n.nspname = 'engram'
     AND has_table_privilege('engram_app', c.oid, 'SELECT, INSERT, UPDATE, DELETE');
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'engram_app has direct privileges on partitions: %', missing;
  END IF;

  -- 4. shard_id appears only in namespace_ownership
  SELECT string_agg(c.relname, ', ') INTO missing
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'shard_id' AND NOT a.attisdropped
   WHERE n.nspname = 'engram' AND c.relkind IN ('r', 'p') AND c.relname <> 'namespace_ownership';
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'shard_id must only exist on namespace_ownership, found on: %', missing;
  END IF;
END $$;
