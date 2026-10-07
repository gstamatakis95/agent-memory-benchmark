-- =============================================================================
-- Engram catalog (control plane) schema
-- Database: engram_catalog (one small PostgreSQL 16 instance + streaming replica, D4)
-- Plain SQL, PostgreSQL 16, schema public (section 9 wraps it as migrations/catalog/0001_init.sql).
-- No extensions required (gen_random_uuid() is core). Apply as the owner role catalog_migrate.
--
-- Roles
--   catalog_migrate  owner of every object; runs this file and later migrations
--   catalog_app      engram-api (Resolver, NamespaceService, TenantService reads, usage rollups)
--   catalog_admin    engramctl, MoveService, ShardService (full DML)
-- The catalog has no RLS: it holds routing metadata only, is reached only by service roles,
-- and never by tenant credentials.
-- =============================================================================

SET search_path = public;

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
CREATE TYPE tenant_state    AS ENUM ('active', 'suspended', 'deleting', 'deleted');
CREATE TYPE isolation_mode  AS ENUM ('shared', 'dedicated');
CREATE TYPE shard_state     AS ENUM ('provisioning', 'active', 'full', 'draining', 'readonly', 'retired');
CREATE TYPE namespace_state AS ENUM ('creating', 'active', 'moving', 'frozen', 'restoring', 'deleting', 'deleted');
-- 'restoring' (N64, review F-23): the shard is being restored from backup; the shard-side
-- ownership rows are frozen with freeze_reason = 'restore' and surface as NamespaceFrozen.
-- A client can therefore tell a restore from a move ('frozen').
CREATE TYPE move_state      AS ENUM ('planned', 'copying', 'catching_up', 'frozen', 'cutover',
                                     'cleaning', 'done', 'rolled_back');

-- -----------------------------------------------------------------------------
-- Helpers
-- -----------------------------------------------------------------------------
CREATE FUNCTION catalog_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- -----------------------------------------------------------------------------
-- cells: one API + worker stack; a shard belongs to exactly one cell (D3)
-- -----------------------------------------------------------------------------
CREATE TABLE cells (
  cell_id     text PRIMARY KEY CHECK (cell_id ~ '^[a-z0-9-]{1,32}$'),
  grpc_addr   text NOT NULL,                       -- the cell's Envoy, for cross-cell forwarding (phase 3)
  state       text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'draining', 'retired')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- tenants. config: the tenant layer of system < tenant < namespace inheritance (D12).
-- -----------------------------------------------------------------------------
CREATE TABLE tenants (
  tenant_id     text PRIMARY KEY CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  display_name  text NOT NULL DEFAULT '',
  state         tenant_state NOT NULL DEFAULT 'active',
  isolation     isolation_mode NOT NULL DEFAULT 'shared',
  config        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  deleted_at    timestamptz,
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL))
);

CREATE TRIGGER tenants_touch BEFORE UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- -----------------------------------------------------------------------------
-- shards: one dedicated PostgreSQL 16 instance each (D2). Dense int32 ids.
-- Placement policy columns: dedicated_tenant_id + capacity counters.
-- -----------------------------------------------------------------------------
CREATE TABLE shards (
  shard_id              integer PRIMARY KEY CHECK (shard_id >= 0),
  cell_id               text NOT NULL REFERENCES cells (cell_id),
  state                 shard_state NOT NULL DEFAULT 'provisioning',
  pgbouncer_addr        text NOT NULL,             -- host:port of the shard's pgbouncer sidecar
  direct_addr           text NOT NULL,             -- host:port of Postgres itself (relay, move, engramctl)
  dsn_secret_ref        text NOT NULL,             -- name of the secret holding role passwords; never a DSN here
  blob_prefix           text NOT NULL,             -- first segment of every blob key on this shard (= shard_id as text)
  blob_cred_secret_ref  text NOT NULL,             -- secret holding the credential scoped to blob_prefix/*
  task_queue            text NOT NULL,             -- 'shard-{shard_id}' (Temporal)
  kafka_topic           text NOT NULL,             -- 'engram.events.shard-{shard_id}' (used only if Kafka is on)
  dedicated_tenant_id   text REFERENCES tenants (tenant_id),   -- NULL = shared pool
  max_namespaces        integer NOT NULL DEFAULT 150 CHECK (max_namespaces > 0),
  soft_cap_facts        bigint  NOT NULL DEFAULT 10000000,
  hard_cap_facts        bigint  NOT NULL DEFAULT 20000000,
  namespaces_count      integer NOT NULL DEFAULT 0 CHECK (namespaces_count >= 0),
  facts_estimate        bigint  NOT NULL DEFAULT 0 CHECK (facts_estimate >= 0),
  bytes_estimate        bigint  NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  stats_updated_at      timestamptz,
  schema_version        integer NOT NULL DEFAULT 0,  -- last migration applied on the shard (mirror of shard_meta)
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  CHECK (soft_cap_facts <= hard_cap_facts),
  CHECK (blob_prefix = shard_id::text),
  CHECK (task_queue = 'shard-' || shard_id::text),
  CHECK (kafka_topic = 'engram.events.shard-' || shard_id::text)
);

CREATE INDEX shards_placement_idx ON shards (state, dedicated_tenant_id, facts_estimate)
  WHERE state = 'active';

CREATE TRIGGER shards_touch BEFORE UPDATE ON shards
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- -----------------------------------------------------------------------------
-- namespaces: the routing table. shard_id + epoch + state are what the resolver caches.
-- profile: reflect mission/directives/disposition (namespace identity, NOT inherited).
-- large: mirror of the shard's namespace_stats.large (N55, review F-8) — the namespace has a
-- per-namespace partial HNSW index on its shard; a stats-only column, no cache invalidation.
-- -----------------------------------------------------------------------------
CREATE TABLE namespaces (
  namespace_id      uuid PRIMARY KEY,               -- UUIDv7 minted by engram-api
  tenant_id         text NOT NULL REFERENCES tenants (tenant_id),
  name              text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  shard_id          integer NOT NULL REFERENCES shards (shard_id),
  epoch             bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
  state             namespace_state NOT NULL DEFAULT 'creating',   -- creating|active|moving|frozen|restoring|deleting|deleted
  config            jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
  profile           jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(profile) = 'object'),
  large             boolean NOT NULL DEFAULT false,
  facts_estimate    bigint NOT NULL DEFAULT 0 CHECK (facts_estimate >= 0),
  bytes_estimate    bigint NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
  stats_updated_at  timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL))
);

-- tenant-unique name, reusable after delete
CREATE UNIQUE INDEX namespaces_tenant_name_uq ON namespaces (tenant_id, name) WHERE state <> 'deleted';
CREATE INDEX namespaces_shard_idx  ON namespaces (shard_id, state);
CREATE INDEX namespaces_tenant_idx ON namespaces (tenant_id, state);
-- 'creating' rows older than 2 min are completed or deleted by the per-shard op-sweeper (N72,
-- review F-45): CreateNamespace is two-phase across the catalog and the shard.
CREATE INDEX namespaces_creating_idx ON namespaces (created_at) WHERE state = 'creating';

CREATE TRIGGER namespaces_touch BEFORE UPDATE ON namespaces
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- -----------------------------------------------------------------------------
-- namespace_moves: one row per move attempt (D5). At most one live move per namespace.
-- Cutover order (N98): (a) CutoverBegin records intent (cutover_at), not a point of no return;
-- (b) the target ownership row goes incoming -> active at epoch e + 1; (c) the source row goes
-- frozen -> moved_out carrying target_shard_id and target_epoch (moved_out_at, the point of no
-- return); (d) the catalog flip, retried indefinitely and idempotently. The API routes from the
-- moved_out row's WrongShardOrEpoch detail, so (d) is on no read path and a catalog failover
-- causes no read outage beyond the bounded re-resolve loop (N52).
-- -----------------------------------------------------------------------------
CREATE TABLE namespace_moves (
  move_id                uuid PRIMARY KEY,
  namespace_id           uuid NOT NULL REFERENCES namespaces (namespace_id),
  tenant_id              text NOT NULL REFERENCES tenants (tenant_id),
  source_shard_id        integer NOT NULL REFERENCES shards (shard_id),
  target_shard_id        integer NOT NULL REFERENCES shards (shard_id),
  from_epoch             bigint NOT NULL CHECK (from_epoch >= 1),
  to_epoch               bigint NOT NULL,
  state                  move_state NOT NULL DEFAULT 'planned',
  p0_seq                 bigint,                    -- source outbox high-water mark at the copy snapshot
  applied_seq            bigint,                    -- last replayed source seq (mirror of target namespace_ownership.move_applied_seq)
  lag_events             bigint,                    -- max(seq) - applied_seq at the last catch-up iteration
  terminated_workflows   text[] NOT NULL DEFAULT '{}',  -- workflow ids terminated at drain, restarted at cutover (D5 step 5)
  error                  text,
  created_by             text NOT NULL,             -- operator principal (engramctl) or 'rebalancer'
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  frozen_at              timestamptz,
  cutover_at             timestamptz,               -- CutoverBegin recorded intent (a): NOT the point of no return (N98)
  moved_out_at           timestamptz,               -- cutover (c) committed on the source: the point of no return (N98); the catalog flip (d) follows, retried indefinitely
  finished_at            timestamptz,
  CHECK (source_shard_id <> target_shard_id),
  CHECK (to_epoch = from_epoch + 1)
);

CREATE UNIQUE INDEX namespace_moves_live_uq ON namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back');
CREATE INDEX namespace_moves_state_idx ON namespace_moves (state, updated_at);

CREATE TRIGGER namespace_moves_touch BEFORE UPDATE ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- -----------------------------------------------------------------------------
-- idempotency_keys: request_id for catalog-level writes (CreateNamespace, tenant/admin
-- methods), scoped to (tenant, method); 24 h (D1). Shard-level writes keep theirs on the shard.
-- -----------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
  tenant_id      text NOT NULL REFERENCES tenants (tenant_id),
  method         text NOT NULL,
  request_id     text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 128),
  request_hash   bytea NOT NULL CHECK (octet_length(request_hash) = 32),
  response       bytea CHECK (response IS NULL OR octet_length(response) <= 65536),
  created_at     timestamptz NOT NULL DEFAULT now(),
  expires_at     timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (tenant_id, method, request_id)
);

CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys (expires_at);

-- -----------------------------------------------------------------------------
-- tenant_usage_daily: cross-shard rollup for TENANT-level quotas (llm_tokens_per_day).
-- Shards hold per-namespace counters; a tenant may span shards and no query may span
-- shards, so engram-api folds per-minute deltas reported by workers into this table.
-- -----------------------------------------------------------------------------
CREATE TABLE tenant_usage_daily (
  tenant_id   text NOT NULL REFERENCES tenants (tenant_id),
  day         date NOT NULL,
  quota_key   text NOT NULL CHECK (quota_key IN ('llm_tokens', 'facts_live', 'recalls', 'retains')),
  used        bigint NOT NULL DEFAULT 0 CHECK (used >= 0),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, day, quota_key)
) WITH (fillfactor = 70);

-- -----------------------------------------------------------------------------
-- deletion_log: replicated from every shard's deletion_log by the deletion-log outbox
-- consumer (section 9 ND-8) so a restore-from-backup re-applies deletes made after the
-- backup point. Idempotent on (namespace_id, kind, subject_id, deleted_at).
-- -----------------------------------------------------------------------------
CREATE TABLE deletion_log (
  namespace_id  uuid NOT NULL,
  tenant_id     text NOT NULL,
  shard_id      integer NOT NULL,
  kind          text NOT NULL CHECK (kind IN ('document', 'memory', 'namespace', 'tenant')),
  subject_id    text NOT NULL,
  epoch         bigint NOT NULL,
  deleted_at    timestamptz NOT NULL,
  details       jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),   -- N79: mirrors the shard row, e.g. {"lineage_flagged": n, "lineage_frontier": m}
  received_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, kind, subject_id, deleted_at)
);

CREATE INDEX deletion_log_shard_idx ON deletion_log (shard_id, deleted_at);

-- -----------------------------------------------------------------------------
-- catalog_events: append-only change log + LISTEN/NOTIFY source (D4).
-- Notification payload: {namespace_id, shard_id, epoch, state} (+ kind, tenant_id, event_id).
-- Retention: 30 days (engramctl catalog gc).
-- -----------------------------------------------------------------------------
CREATE TABLE catalog_events (
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

CREATE INDEX catalog_events_created_idx ON catalog_events (created_at);
CREATE INDEX catalog_events_ns_idx ON catalog_events (namespace_id, event_id) WHERE namespace_id IS NOT NULL;

CREATE FUNCTION catalog_notify_event() RETURNS trigger
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

CREATE TRIGGER catalog_events_notify AFTER INSERT ON catalog_events
  FOR EACH ROW EXECUTE FUNCTION catalog_notify_event();

-- Every routing-relevant namespace change is logged (and thereby notified). Stats-only
-- updates (facts_estimate, bytes_estimate, stats_updated_at) do not invalidate caches.
CREATE FUNCTION catalog_log_namespace_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND NEW.shard_id = OLD.shard_id AND NEW.epoch = OLD.epoch AND NEW.state = OLD.state
     AND NEW.config = OLD.config AND NEW.profile = OLD.profile AND NEW.tenant_id = OLD.tenant_id THEN
    RETURN NULL;
  END IF;
  INSERT INTO catalog_events (kind, namespace_id, tenant_id, shard_id, epoch, state, payload)
  VALUES ('namespace', NEW.namespace_id, NEW.tenant_id, NEW.shard_id, NEW.epoch, NEW.state::text,
          jsonb_build_object('op', TG_OP,
                             'config_changed', TG_OP = 'INSERT' OR NEW.config IS DISTINCT FROM OLD.config));
  RETURN NULL;
END $$;

CREATE TRIGGER namespaces_log AFTER INSERT OR UPDATE ON namespaces
  FOR EACH ROW EXECUTE FUNCTION catalog_log_namespace_change();

-- Tenant config/state changes invalidate every cached namespace of the tenant.
CREATE FUNCTION catalog_log_tenant_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.config = OLD.config AND NEW.state = OLD.state
     AND NEW.isolation = OLD.isolation THEN
    RETURN NULL;
  END IF;
  INSERT INTO catalog_events (kind, tenant_id, state, payload)
  VALUES ('tenant', NEW.tenant_id, NEW.state::text, jsonb_build_object('op', TG_OP));
  RETURN NULL;
END $$;

CREATE TRIGGER tenants_log AFTER INSERT OR UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION catalog_log_tenant_change();

CREATE FUNCTION catalog_log_move_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.state = OLD.state THEN
    RETURN NULL;
  END IF;
  INSERT INTO catalog_events (kind, namespace_id, tenant_id, shard_id, epoch, state, payload)
  VALUES ('move', NEW.namespace_id, NEW.tenant_id, NEW.target_shard_id, NEW.to_epoch, NEW.state::text,
          jsonb_build_object('move_id', NEW.move_id, 'source_shard_id', NEW.source_shard_id));
  RETURN NULL;
END $$;

CREATE TRIGGER namespace_moves_log AFTER INSERT OR UPDATE ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_log_move_change();

-- -----------------------------------------------------------------------------
-- Placement policy: pick_shard(tenant) chooses and reserves a shard slot.
-- Dedicated tenants land only on shards flagged for them; shared tenants only on unflagged
-- active shards below both caps. Least loaded first (facts ratio, then namespace count).
-- Called inside the CreateNamespace transaction; the row lock serialises placement.
-- -----------------------------------------------------------------------------
CREATE FUNCTION pick_shard(p_tenant_id text) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_isolation isolation_mode;
  v_shard     integer;
BEGIN
  SELECT isolation INTO v_isolation
    FROM tenants
   WHERE tenant_id = p_tenant_id AND state = 'active';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'tenant % is not active', p_tenant_id USING ERRCODE = 'P0002';
  END IF;

  SELECT shard_id INTO v_shard
    FROM shards
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

  UPDATE shards SET namespaces_count = namespaces_count + 1 WHERE shard_id = v_shard;
  RETURN v_shard;
END $$;

-- CreateNamespace (engram-api, one catalog transaction):
--   SELECT pick_shard($tenant);
--   INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, epoch, state) VALUES (..., 1, 'creating');
-- then on the shard, as engram_app with the new namespace in scope:
--   INSERT namespace_ownership (ns, tenant, shard, 1, 'active') + namespace_stats
--   (the shard's ownership trigger admits nothing but this active/epoch-1 insert from engram_app);
-- then: UPDATE namespaces SET state = 'active' (which logs + NOTIFYs).
-- A crash between the phases leaves a 'creating' row: the per-shard op-sweeper (N72) completes
-- it when the shard row exists, otherwise deletes both (namespaces_creating_idx); a client
-- retry replays through idempotency_keys exactly like Retain.

-- -----------------------------------------------------------------------------
-- Grants
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO catalog_app, catalog_admin;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO catalog_app;
GRANT INSERT, UPDATE ON namespaces TO catalog_app;
GRANT INSERT ON catalog_events TO catalog_app;                 -- via triggers
GRANT INSERT, UPDATE, DELETE ON idempotency_keys TO catalog_app;
GRANT INSERT, UPDATE ON tenant_usage_daily TO catalog_app;
GRANT INSERT ON deletion_log TO catalog_app;                   -- deletion-log consumer reports through the API
GRANT UPDATE (namespaces_count, facts_estimate, bytes_estimate, stats_updated_at, updated_at)
  ON shards TO catalog_app;                                    -- pick_shard + stats reporter (namespaces.large is set by the same reporter)
GRANT EXECUTE ON FUNCTION pick_shard(text) TO catalog_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO catalog_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO catalog_admin;
