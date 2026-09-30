-- =============================================================================
-- Engram catalog (control plane) schema
-- Database: engram_catalog (one small PostgreSQL 16 instance + streaming replica)
-- Plain SQL, PostgreSQL 16. No extensions required (gen_random_uuid() is core).
-- Apply as the owner role (catalog_migrate). See plan section 3.2 for rationale.
--
-- Roles
--   catalog_migrate  owner of every object; runs this file and later migrations
--   catalog_app      engram-api (Resolver + NamespaceService + TenantService reads)
--   catalog_admin    engramctl, MoveService, ShardService (full DML)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS catalog;
SET search_path = catalog, public;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_app') THEN
    CREATE ROLE catalog_app LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_admin') THEN
    CREATE ROLE catalog_admin LOGIN;
  END IF;
END $$;

-- -----------------------------------------------------------------------------
-- Enumerations (closed sets; extend with ALTER TYPE ... ADD VALUE in a migration)
-- -----------------------------------------------------------------------------
CREATE TYPE catalog.tenant_state    AS ENUM ('active', 'suspended', 'deleting', 'deleted');
CREATE TYPE catalog.isolation_mode  AS ENUM ('shared', 'dedicated');
CREATE TYPE catalog.shard_state     AS ENUM ('provisioning', 'active', 'draining', 'readonly', 'retired');
CREATE TYPE catalog.namespace_state AS ENUM ('creating', 'active', 'moving', 'frozen', 'deleting', 'deleted');
CREATE TYPE catalog.move_state      AS ENUM ('planned', 'copying', 'catching_up', 'frozen', 'cutover',
                                             'cleaning', 'done', 'rolled_back');

-- -----------------------------------------------------------------------------
-- Helpers
-- -----------------------------------------------------------------------------
CREATE FUNCTION catalog.touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- -----------------------------------------------------------------------------
-- cells: one API + worker stack. A shard belongs to exactly one cell (D3).
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.cells (
  cell_id     text PRIMARY KEY CHECK (cell_id ~ '^[a-z0-9-]{1,32}$'),
  grpc_addr   text NOT NULL,                       -- Envoy front door of the cell, for cross-cell forwarding (phase 3)
  state       text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'draining', 'retired')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- tenants
-- config: JSONB layer of the system < tenant < namespace inheritance (D12).
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.tenants (
  tenant_id     text PRIMARY KEY CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  display_name  text NOT NULL DEFAULT '',
  state         catalog.tenant_state NOT NULL DEFAULT 'active',
  isolation     catalog.isolation_mode NOT NULL DEFAULT 'shared',
  config        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at    timestamptz,
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL))
);

CREATE TRIGGER tenants_touch BEFORE UPDATE ON catalog.tenants
  FOR EACH ROW EXECUTE FUNCTION catalog.touch_updated_at();

-- -----------------------------------------------------------------------------
-- shards: one dedicated PostgreSQL 16 instance each (D2). Dense int32 ids.
-- Placement policy columns: dedicated_tenant_id + capacity counters.
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.shards (
  shard_id              integer PRIMARY KEY CHECK (shard_id >= 0),
  cell_id               text NOT NULL REFERENCES catalog.cells (cell_id),
  state                 catalog.shard_state NOT NULL DEFAULT 'provisioning',
  pgbouncer_addr        text NOT NULL,             -- host:port of the shard's pgbouncer sidecar
  dsn_secret_ref        text NOT NULL,             -- name of the secret holding role passwords; never a DSN in the catalog
  blob_prefix           text NOT NULL,             -- first path segment of every blob key on this shard (= shard_id as text)
  blob_cred_secret_ref  text NOT NULL,             -- secret holding the credential scoped to blob_prefix/*
  task_queue            text NOT NULL,             -- 'shard-{shard_id}' (Temporal)
  kafka_topic           text NOT NULL,             -- 'engram.events.shard-{shard_id}' (only used if Kafka is enabled)
  dedicated_tenant_id   text REFERENCES catalog.tenants (tenant_id),   -- NULL = shared pool
  max_namespaces        integer NOT NULL DEFAULT 150 CHECK (max_namespaces > 0),
  soft_cap_facts        bigint  NOT NULL DEFAULT 10000000,
  hard_cap_facts        bigint  NOT NULL DEFAULT 20000000,
  namespaces_count      integer NOT NULL DEFAULT 0 CHECK (namespaces_count >= 0),
  facts_estimate        bigint  NOT NULL DEFAULT 0 CHECK (facts_estimate >= 0),
  bytes_estimate        bigint  NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  stats_updated_at      timestamptz,
  schema_version        integer NOT NULL DEFAULT 0,  -- last migration applied on the shard (section 9)
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  CHECK (soft_cap_facts <= hard_cap_facts),
  CHECK (blob_prefix = shard_id::text),
  CHECK (task_queue = 'shard-' || shard_id::text),
  CHECK (kafka_topic = 'engram.events.shard-' || shard_id::text)
);

CREATE INDEX shards_placement_idx ON catalog.shards (state, dedicated_tenant_id, facts_estimate)
  WHERE state = 'active';

CREATE TRIGGER shards_touch BEFORE UPDATE ON catalog.shards
  FOR EACH ROW EXECUTE FUNCTION catalog.touch_updated_at();

-- -----------------------------------------------------------------------------
-- namespaces: the routing table. shard_id + epoch are what the resolver caches.
-- profile: reflect mission/directives/disposition (namespace identity, NOT inherited).
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.namespaces (
  namespace_id      uuid PRIMARY KEY,               -- UUIDv7 minted by engram-api
  tenant_id         text NOT NULL REFERENCES catalog.tenants (tenant_id),
  name              text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  shard_id          integer NOT NULL REFERENCES catalog.shards (shard_id),
  epoch             bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
  state             catalog.namespace_state NOT NULL DEFAULT 'creating',
  config            jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
  profile           jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(profile) = 'object'),
  facts_estimate    bigint NOT NULL DEFAULT 0 CHECK (facts_estimate >= 0),
  bytes_estimate    bigint NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  stats_updated_at  timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL))
);

-- tenant-unique name, reusable after delete
CREATE UNIQUE INDEX namespaces_tenant_name_uq ON catalog.namespaces (tenant_id, name)
  WHERE state <> 'deleted';
CREATE INDEX namespaces_shard_idx  ON catalog.namespaces (shard_id, state);
CREATE INDEX namespaces_tenant_idx ON catalog.namespaces (tenant_id, state);

CREATE TRIGGER namespaces_touch BEFORE UPDATE ON catalog.namespaces
  FOR EACH ROW EXECUTE FUNCTION catalog.touch_updated_at();

-- -----------------------------------------------------------------------------
-- namespace_moves: one row per move attempt (D5). At most one live move per namespace.
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.namespace_moves (
  move_id                uuid PRIMARY KEY,
  namespace_id           uuid NOT NULL REFERENCES catalog.namespaces (namespace_id),
  tenant_id              text NOT NULL REFERENCES catalog.tenants (tenant_id),
  source_shard_id        integer NOT NULL REFERENCES catalog.shards (shard_id),
  target_shard_id        integer NOT NULL REFERENCES catalog.shards (shard_id),
  from_epoch             bigint NOT NULL CHECK (from_epoch >= 1),
  to_epoch               bigint NOT NULL,
  state                  catalog.move_state NOT NULL DEFAULT 'planned',
  p0_seq                 bigint,                    -- source outbox high-water mark at the copy snapshot
  applied_seq            bigint,                    -- last replayed source seq (mirror of target ownership.move_applied_seq)
  lag_events             bigint,                    -- max(seq) - applied_seq at the last catch-up iteration
  terminated_operations  uuid[] NOT NULL DEFAULT '{}',  -- workflow operation_ids terminated at drain, restarted at cutover
  error                  text,
  created_by             text NOT NULL,             -- operator principal (engramctl) or 'rebalancer'
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  frozen_at              timestamptz,
  cutover_at             timestamptz,
  finished_at            timestamptz,
  CHECK (source_shard_id <> target_shard_id),
  CHECK (to_epoch = from_epoch + 1)
);

CREATE UNIQUE INDEX namespace_moves_live_uq ON catalog.namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back');
CREATE INDEX namespace_moves_state_idx ON catalog.namespace_moves (state, updated_at);

CREATE TRIGGER namespace_moves_touch BEFORE UPDATE ON catalog.namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog.touch_updated_at();

-- -----------------------------------------------------------------------------
-- tenant_usage_daily: cross-shard rollup for TENANT-level quotas (llm_tokens_per_day).
-- Shards hold per-namespace counters; a tenant may span shards and no query may
-- span shards, so workers push per-minute deltas here (off the hot path).
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.tenant_usage_daily (
  tenant_id   text NOT NULL REFERENCES catalog.tenants (tenant_id),
  day         date NOT NULL,
  quota_key   text NOT NULL CHECK (quota_key IN ('llm_tokens', 'facts_live', 'recalls', 'retains')),
  used        bigint NOT NULL DEFAULT 0 CHECK (used >= 0),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, day, quota_key)
) WITH (fillfactor = 70);

-- -----------------------------------------------------------------------------
-- catalog_events: append-only change log + LISTEN/NOTIFY source (D4).
-- Payload of the notification: {namespace_id, shard_id, epoch, state} (+ kind, tenant_id, event_id).
-- Retention: 30 days (engramctl catalog gc).
-- -----------------------------------------------------------------------------
CREATE TABLE catalog.catalog_events (
  event_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind          text NOT NULL CHECK (kind IN ('namespace', 'tenant', 'shard', 'move')),
  namespace_id  uuid,
  tenant_id     text,
  shard_id      integer,
  epoch         bigint,
  state         text,
  payload       jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX catalog_events_created_idx ON catalog.catalog_events (created_at);
CREATE INDEX catalog_events_ns_idx ON catalog.catalog_events (namespace_id, event_id)
  WHERE namespace_id IS NOT NULL;

CREATE FUNCTION catalog.notify_catalog_event() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify(
    'catalog_changes',
    json_build_object(
      'event_id',     NEW.event_id,
      'kind',         NEW.kind,
      'namespace_id', NEW.namespace_id,
      'tenant_id',    NEW.tenant_id,
      'shard_id',     NEW.shard_id,
      'epoch',        NEW.epoch,
      'state',        NEW.state
    )::text);
  RETURN NULL;
END $$;

CREATE TRIGGER catalog_events_notify AFTER INSERT ON catalog.catalog_events
  FOR EACH ROW EXECUTE FUNCTION catalog.notify_catalog_event();

-- Every routing-relevant namespace change is logged (and thereby notified).
-- Stats-only updates (facts_estimate, bytes_estimate, stats_updated_at) do not invalidate caches.
CREATE FUNCTION catalog.log_namespace_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND NEW.shard_id = OLD.shard_id AND NEW.epoch = OLD.epoch AND NEW.state = OLD.state
     AND NEW.config = OLD.config AND NEW.profile = OLD.profile AND NEW.tenant_id = OLD.tenant_id THEN
    RETURN NULL;
  END IF;
  INSERT INTO catalog.catalog_events (kind, namespace_id, tenant_id, shard_id, epoch, state, payload)
  VALUES ('namespace', NEW.namespace_id, NEW.tenant_id, NEW.shard_id, NEW.epoch, NEW.state::text,
          jsonb_build_object('op', TG_OP,
                             'config_changed', TG_OP = 'INSERT' OR NEW.config IS DISTINCT FROM OLD.config));
  RETURN NULL;
END $$;

CREATE TRIGGER namespaces_log AFTER INSERT OR UPDATE ON catalog.namespaces
  FOR EACH ROW EXECUTE FUNCTION catalog.log_namespace_change();

-- Tenant config/state changes invalidate every cached namespace of the tenant.
CREATE FUNCTION catalog.log_tenant_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.config = OLD.config AND NEW.state = OLD.state
     AND NEW.isolation = OLD.isolation THEN
    RETURN NULL;
  END IF;
  INSERT INTO catalog.catalog_events (kind, tenant_id, state, payload)
  VALUES ('tenant', NEW.tenant_id, NEW.state::text, jsonb_build_object('op', TG_OP));
  RETURN NULL;
END $$;

CREATE TRIGGER tenants_log AFTER INSERT OR UPDATE ON catalog.tenants
  FOR EACH ROW EXECUTE FUNCTION catalog.log_tenant_change();

CREATE FUNCTION catalog.log_move_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.state = OLD.state THEN
    RETURN NULL;
  END IF;
  INSERT INTO catalog.catalog_events (kind, namespace_id, tenant_id, shard_id, epoch, state, payload)
  VALUES ('move', NEW.namespace_id, NEW.tenant_id, NEW.target_shard_id, NEW.to_epoch, NEW.state::text,
          jsonb_build_object('move_id', NEW.move_id, 'source_shard_id', NEW.source_shard_id));
  RETURN NULL;
END $$;

CREATE TRIGGER namespace_moves_log AFTER INSERT OR UPDATE ON catalog.namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog.log_move_change();

-- -----------------------------------------------------------------------------
-- Placement policy: pick_shard(tenant) chooses and reserves a shard slot.
-- Dedicated tenants only land on shards flagged for them; shared tenants only on
-- unflagged active shards below both caps. Least-loaded first (facts, then count).
-- Called inside the CreateNamespace transaction; the row lock serialises placement.
-- -----------------------------------------------------------------------------
CREATE FUNCTION catalog.pick_shard(p_tenant_id text) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_isolation catalog.isolation_mode;
  v_shard     integer;
BEGIN
  SELECT isolation INTO v_isolation
    FROM catalog.tenants
   WHERE tenant_id = p_tenant_id AND state = 'active';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'tenant % is not active', p_tenant_id USING ERRCODE = 'P0002';
  END IF;

  SELECT shard_id INTO v_shard
    FROM catalog.shards
   WHERE state = 'active'
     AND namespaces_count < max_namespaces
     AND facts_estimate < soft_cap_facts
     AND CASE WHEN v_isolation = 'dedicated'
              THEN dedicated_tenant_id = p_tenant_id
              ELSE dedicated_tenant_id IS NULL END
   ORDER BY facts_estimate::numeric / soft_cap_facts::numeric, namespaces_count, shard_id
   LIMIT 1
   FOR UPDATE SKIP LOCKED;

  IF v_shard IS NULL THEN
    RAISE EXCEPTION 'no shard with capacity for tenant % (isolation=%)', p_tenant_id, v_isolation
      USING ERRCODE = '53400';   -- configuration_limit_exceeded -> RESOURCE_EXHAUSTED at the API
  END IF;

  UPDATE catalog.shards SET namespaces_count = namespaces_count + 1 WHERE shard_id = v_shard;
  RETURN v_shard;
END $$;

-- CreateNamespace (called by engram-api in one transaction):
--   SELECT catalog.pick_shard($tenant);
--   INSERT INTO catalog.namespaces (namespace_id, tenant_id, name, shard_id, epoch, state) VALUES (..., 1, 'creating');
--   -- engram-api then inserts namespace_ownership(ns, tenant, shard, 1, 'active') on the shard and
--   -- UPDATEs catalog.namespaces SET state='active' (which NOTIFYs).

-- -----------------------------------------------------------------------------
-- Grants
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA catalog TO catalog_app, catalog_admin;

GRANT SELECT ON ALL TABLES IN SCHEMA catalog TO catalog_app;
GRANT INSERT, UPDATE ON catalog.namespaces TO catalog_app;
GRANT INSERT ON catalog.catalog_events TO catalog_app;          -- via triggers
GRANT INSERT, UPDATE ON catalog.tenant_usage_daily TO catalog_app;
GRANT UPDATE (namespaces_count, facts_estimate, bytes_estimate, stats_updated_at, updated_at)
  ON catalog.shards TO catalog_app;                            -- pick_shard + stats reporter
GRANT EXECUTE ON FUNCTION catalog.pick_shard(text) TO catalog_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA catalog TO catalog_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA catalog TO catalog_admin;

-- The catalog has no RLS: it holds routing metadata only, is reached only by
-- engram-api/engramctl service roles, and never by tenant credentials.
