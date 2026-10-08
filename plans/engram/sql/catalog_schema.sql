-- =============================================================================
-- Engram catalog (control plane) schema
-- Database: engram_catalog (one small PostgreSQL 16 instance + ONE SYNCHRONOUS STANDBY, D4, N146)
--   synchronous_commit = remote_apply, synchronous_standby_names = 'FIRST 1 (catalog-standby)': the catalog
--   arbitrates moves and holds the replay floor and the 'deleting' state, so a catalog commit that a protocol step
--   observed must survive a failover (promotion is RPO 0). It is the one place where N92's rejection of a synchronous
--   standby does not apply: writes are off the hot path, so a blocked commit degrades operations, never reads or retains.
--   Standby down: catalog writes block, CatalogStandbyDown pages at 60 s, `engramctl catalog degrade --async` is the
--   operator's explicit, logged switch (never automatic). A catalog RESTORE from backup (RPO 60 s, the only lossy path)
--   is followed by `engramctl catalog reconcile --from-shards` BEFORE it serves writes: the shards' ownership rows are
--   the source of truth (source moved_out => move >= committed; target active with move_epoch => done/cleaning;
--   target ready/incoming with source frozen/move => cutover; shard frozen/delete => deleting) and every namespace's
--   epoch is raised to max(epoch over its shard rows).
-- Plain SQL, PostgreSQL 16, schema public (section 9 wraps it as migrations/catalog/0001_init.sql).
-- No extensions required (gen_random_uuid() is core). Apply as the owner role catalog_migrate.
--
-- Roles
--   catalog_migrate  owner of every object; runs this file and later migrations
--   catalog_app      engram-api (Resolver, NamespaceService, TenantService reads, usage rollups)
--   catalog_admin    engramctl, MoveService, ShardService (full DML)
-- The catalog has no RLS: it holds routing metadata only, is reached only by service roles,
-- and never by tenant credentials. It holds no delete log: delete intents (N122) are objects in blob
-- storage, and the shard's deletion_log records what was applied.
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
-- move_state (D5, N124, N125): planned -> copying (dirty bulk copy, no snapshot) -> frozen -> reconciling
-- (reconcile by insertion sequence under the freeze) -> cutover ((b') target ready) -> committed (a'') the
-- catalog CAS cutover -> committed, the POINT OF NO RETURN -> (c) source moved_out, (b'') target
-- active, (d) catalog flip -> cleaning -> done. rolled_back is reachable from every state up to and
-- including cutover, and from no later one: restore and failover CAS cutover -> rolled_back, and exactly
-- one of the two CASes wins (N123). The trigger catalog_check_move_transition enforces the edges.
CREATE TYPE move_state      AS ENUM ('planned', 'copying', 'frozen', 'reconciling', 'cutover', 'committed',
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
-- DELETE_TENANT (N127, N133d) has no operation row: the catalog derives the operation from this
-- row, as DELETE_NAMESPACE is derived from namespaces.state. delete_operation_id and
-- delete_requested_at are written in the same statement that sets state = 'deleting' (the
-- DeleteTenant ack); deleted_at is written by the last step of the TenantDelete workflow together
-- with state = 'deleted'. See the view tenant_delete_operations.
-- -----------------------------------------------------------------------------
CREATE TABLE tenants (
  tenant_id     text PRIMARY KEY CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  display_name  text NOT NULL DEFAULT '',
  state         tenant_state NOT NULL DEFAULT 'active',
  isolation     isolation_mode NOT NULL DEFAULT 'shared',
  config        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  delete_operation_id  uuid,                        -- the DELETE_TENANT operation id (UUIDv7 minted by engram-api at the ack)
  delete_requested_at  timestamptz,                 -- the ack time = the operation's create_time
  deleted_at    timestamptz,                        -- the operation's finish_time
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL)),
  CHECK ((state IN ('deleting', 'deleted')) = (delete_operation_id IS NOT NULL)),
  CHECK ((state IN ('deleting', 'deleted')) = (delete_requested_at IS NOT NULL))
);

-- DELETE_TENANT operations, derived (N133d): 'deleting' -> RUNNING, 'deleted' -> SUCCEEDED. No other
-- state is derivable: TenantDelete is retried until it completes, so a stuck delete stays RUNNING.
-- TenantService.GetTenantOperation reads this view (admin, tenant-scoped).
CREATE VIEW tenant_delete_operations AS
SELECT t.tenant_id,
       t.delete_operation_id AS operation_id,
       CASE t.state WHEN 'deleting' THEN 'RUNNING' ELSE 'SUCCEEDED' END AS state,
       t.delete_requested_at AS create_time,
       t.deleted_at AS finish_time
  FROM tenants t
 WHERE t.state IN ('deleting', 'deleted');

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
  replay_floor          timestamptz,               -- N134: lower bound for delete-intent replay; lowered with min() by every restore/failover, NEVER raised while intents are retained (35 d); lives here so a PITR or a stale promotion cannot lose it
  replay_floor_mirror   timestamptz,               -- N146: the lowest restore target `engramctl restore replay` wrote to blob storage (_control/restores/{shard}/{target}.json) BEFORE replaying; replay uses replay_floor_effective, so a catalog restore that lost the lowered replay_floor cannot raise the floor
  replay_floor_effective timestamptz GENERATED ALWAYS AS (least(replay_floor, replay_floor_mirror)) STORED,
  system_identifier     bigint,                    -- N123: pg_control_system().system_identifier of the current primary; written on PROMOTION before the virtual endpoint flips
  timeline_id           integer,                   -- N123: pg_control_checkpoint().timeline_id; the mover compares its session's value at Freeze and (c), the relay every 10 s
  dedicated_tenant_id   text REFERENCES tenants (tenant_id),   -- NULL = shared pool
  max_namespaces        integer NOT NULL DEFAULT 120 CHECK (max_namespaces > 0),
  soft_cap_facts        bigint  NOT NULL DEFAULT 6500000,     -- N114/N154/D3: 6.5 M live facts target (hot set about 73 GB with both covering link indexes; was 8 M)
  hard_cap_facts        bigint  NOT NULL DEFAULT 10000000,    -- 10 M hard cap (was 12 M)
  volume_bytes          bigint  NOT NULL DEFAULT 600000000000 CHECK (volume_bytes > 0),   -- 600 GB local NVMe; ShardNearCapacity pages at 70 % of it (relation bytes, N114)
  namespaces_count      integer NOT NULL DEFAULT 0 CHECK (namespaces_count >= 0),
  facts_estimate        bigint  NOT NULL DEFAULT 0 CHECK (facts_estimate >= 0),
  bytes_estimate        bigint  NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),   -- relation bytes of the shard's namespaces, hidden and unpurged rows included (N114)
  stats_updated_at      timestamptz,
  schema_version        integer NOT NULL DEFAULT 0,  -- last migration applied on the shard (mirror of shard_meta)
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  CHECK (soft_cap_facts <= hard_cap_facts),
  CHECK ((system_identifier IS NULL) = (timeline_id IS NULL)),
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
-- embedding_model / embedding_dims (N111): fixed at namespace creation and mirrored by the shard's
-- namespace_models row; a change is the ReembedNamespace workflow (new vectors, new index, flip,
-- expunge of the old), never a config flip.
-- large: mirror of the shard's namespace_stats.large (N112) — the namespace has crossed 2,000
-- vectors and owns per-namespace partial HNSW indexes on its shard; a stats-only column, no cache
-- invalidation.
-- -----------------------------------------------------------------------------
CREATE TABLE namespaces (
  namespace_id      uuid PRIMARY KEY,               -- UUIDv7 minted by engram-api
  tenant_id         text NOT NULL REFERENCES tenants (tenant_id),
  name              text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  shard_id          integer NOT NULL REFERENCES shards (shard_id),
  epoch             bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
  state             namespace_state NOT NULL DEFAULT 'creating',   -- creating|active|moving|frozen|restoring|deleting|deleted
  embedding_model   text NOT NULL DEFAULT 'nomic-embed-text-v1.5' CHECK (octet_length(embedding_model) BETWEEN 1 AND 128),
  embedding_dims    integer NOT NULL DEFAULT 768 CHECK (embedding_dims BETWEEN 8 AND 4000),
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
-- namespace_moves: one row per move attempt (D5, N124, N125). At most one live move per namespace.
-- It is also the ARBITER of restore and failover (N123, N125): the catalog CAS
--   UPDATE ... SET state = 'committed' WHERE move_id = $1 AND state = 'cutover'
-- is the point of no return. A restored or promoted shard CASes cutover -> rolled_back; if it reads
-- 'committed' it completes (c), (b'') and (d) itself (reconcile_out). Exactly one CAS wins and the loser
-- stops. Cutover order: (b') the target row goes incoming -> ready (ready_at); (a'') the CAS above
-- (committed_at), taken only after the mover re-checks its timeline; (c) the source row goes
-- frozen/move -> moved_out carrying target_shard_id / target_epoch (moved_out_at, informational),
-- executed only after the mover READ 'committed'; (b'') the target row goes ready -> active
-- (activated_at); (d) the catalog flip WHERE epoch = e AND state = 'frozen', with "already
-- (target, e + 1)" as the only idempotent success. The API routes from the moved_out row's
-- WrongShardOrEpoch detail, so (d) is on no read path.
-- There is no outbox position here: the move never reads or replays the outbox (N124).
-- Every step, the CAS above included, is a compare-and-set on the exact ownership rows the mover verified (N143): the
-- CAS adds the verified shards, from_epoch, source_system_id / source_timeline_id and requires the namespaces row to
-- still be (source, from_epoch, 'frozen'). Cleanup of the source needs 'done', which needs the N159 gate: a cleanup-time ReconcileIn,
-- a full target backup that STARTED after it, a content check (the source's insert-only rows below W_final, less the documents
-- the target has tombstoned, are all on the target: count and hash equal) taken after that backup completed, and a timeline
-- check (the target's last timeline change is not later than the ReconcileIn). A backup timestamp alone is not the gate.
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
  source_system_id       bigint,                    -- N123: recorded at Plan; every later source activity fails MoveFenced on mismatch
  source_timeline_id     integer,
  t_copy                 timestamptz,               -- N124: source now() when the dirty copy began; the catch-up copy takes ins_seq >= engram_seq_floor(t_copy)
  t_pre                  timestamptz,               -- N124: source now() at the pre-freeze verification; the freeze re-copies ins_seq >= engram_seq_floor(t_pre)
  w_pre                  bigint,                    -- N124: nextval('engram_ins_seq') taken with t_pre
  w_plan                 bigint,                    -- N147: the source's nextval at Plan; the target's engram_ins_seq is advanced past it (engram_seq_advance)
  w_final                bigint,                    -- N147: the source's final value under the freeze; the target is advanced past it BEFORE (b'), recorded here
  terminated_workflows   text[] NOT NULL DEFAULT '{}',  -- workflow ids terminated at drain, restarted on the target (N97)
  error                  text,
  created_by             text NOT NULL,             -- operator principal (engramctl) or 'rebalancer'
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  frozen_at              timestamptz,
  ready_at               timestamptz,               -- (b') target incoming -> ready; rollback still possible (unready_target)
  committed_at           timestamptz,               -- (a'') the catalog CAS cutover -> committed: the point of no return
  moved_out_at           timestamptz,               -- (c) on the source; informational (the arbiter is committed_at)
  activated_at           timestamptz,               -- (b'') target ready -> active
  activated_timeline     integer,                   -- N149: the target's timeline_id when (b'') ran
  target_timeline_changed_at timestamptz,           -- N149: the target's last timeline change (restore or promotion) after activation, NULL if none (catalog.MoveBackups.RecordTimeline)
  reconciled_in_at       timestamptz,               -- N149: ReconcileIn completed (the target was repaired from the source)
  target_backup_started_at timestamptz,             -- N149: start of the FULL target backup that gates cleanup
  target_backup_at       timestamptz,               -- N143: completion of a FULL target backup that STARTED after activated_at and after cleanup_reconciled_at (N159); source cleanup waits for it and for the 24 h grace
  cleanup_reconciled_at  timestamptz,               -- N159: the cleanup-time ReconcileIn (always run, even with no recorded timeline change) completed
  source_content_rows    bigint,                    -- N159: content check, source side: insert-only rows with ins_seq < w_final, less documents the target tombstoned
  source_content_hash    bytea,                     --   bit_xor(hashtextextended(pk::text, 0)) over the same rows
  target_content_rows    bigint,                    -- N159: the same measure on the target; must equal the source's
  target_content_hash    bytea,
  target_content_checked_at timestamptz,            -- N159: taken after target_backup_at; with the timeline check it makes the backup content-complete
  finished_at            timestamptz,
  CHECK (source_shard_id <> target_shard_id),
  CHECK (to_epoch = from_epoch + 1),
  CHECK ((source_system_id IS NULL) = (source_timeline_id IS NULL)),
  CHECK (state NOT IN ('reconciling', 'cutover', 'committed', 'cleaning', 'done') OR frozen_at IS NOT NULL),
  CHECK (state NOT IN ('committed', 'cleaning', 'done') OR committed_at IS NOT NULL),
  CHECK (state NOT IN ('cleaning', 'done') OR moved_out_at IS NOT NULL),
  CHECK (ready_at IS NULL OR w_final IS NOT NULL),                                     -- N147: no 'ready' before the target's sequence passed W_final
  -- N149, N159: the cleanup gate is content check + timeline check + a cleanup-time ReconcileIn, not a backup timestamp alone:
  --   (1) ReconcileIn from the intact source ran at cleanup time; (2) a FULL target backup STARTED after it and after activation;
  --   (3) after that backup completed, the content check found the target holding every source row (count and hash equal);
  --   (4) the target's last timeline change is not later than (1), so no restore or promotion happened between (1) and (3).
  CHECK (state <> 'done' OR (
         activated_at IS NOT NULL AND cleanup_reconciled_at IS NOT NULL
         AND target_backup_started_at IS NOT NULL AND target_backup_at IS NOT NULL
         AND target_backup_started_at >= greatest(activated_at, cleanup_reconciled_at)
         AND target_backup_at >= target_backup_started_at
         AND target_content_checked_at IS NOT NULL AND target_content_checked_at >= target_backup_at
         AND source_content_rows IS NOT NULL AND source_content_hash IS NOT NULL
         AND target_content_rows = source_content_rows AND target_content_hash = source_content_hash
         AND (target_timeline_changed_at IS NULL OR target_timeline_changed_at <= cleanup_reconciled_at)))
);

CREATE UNIQUE INDEX namespace_moves_live_uq ON namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back');
CREATE INDEX namespace_moves_state_idx ON namespace_moves (state, updated_at);

CREATE TRIGGER namespace_moves_touch BEFORE UPDATE ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- The move state machine as a trigger (N125): the only way out of 'committed' is forward, so after
-- the CAS neither a rollback nor a restore reconcile can win. A same-state update is allowed (timestamps).
CREATE FUNCTION catalog_check_move_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state = OLD.state THEN
    RETURN NEW;
  END IF;
  IF (OLD.state, NEW.state) IN (
       ('planned', 'copying'), ('copying', 'frozen'), ('frozen', 'reconciling'), ('reconciling', 'cutover'),
       ('cutover', 'committed'), ('committed', 'cleaning'), ('cleaning', 'done'),
       ('planned', 'rolled_back'), ('copying', 'rolled_back'), ('frozen', 'rolled_back'),
       ('reconciling', 'rolled_back'), ('cutover', 'rolled_back')) THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal move transition % -> % (move %)', OLD.state, NEW.state, OLD.move_id
    USING ERRCODE = '23514';
END $$;

CREATE TRIGGER namespace_moves_transition BEFORE UPDATE OF state ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_check_move_transition();

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

  -- N131 (P-16): namespaces_count is DERIVED here from the namespaces table (a counter that is only
  -- incremented drifts after deletes and moves); the stored column is refreshed from the same count.
  SELECT s.shard_id INTO v_shard
    FROM shards s
   CROSS JOIN LATERAL (SELECT count(*)::integer AS n FROM namespaces x
                        WHERE x.shard_id = s.shard_id AND x.state <> 'deleted') AS c
   WHERE s.state = 'active'
     AND c.n < s.max_namespaces
     AND s.facts_estimate < s.soft_cap_facts
     AND CASE WHEN v_isolation = 'dedicated'
              THEN s.dedicated_tenant_id = p_tenant_id
              ELSE s.dedicated_tenant_id IS NULL END
   ORDER BY s.facts_estimate::numeric / s.soft_cap_facts::numeric, c.n, s.shard_id
   LIMIT 1
   FOR UPDATE OF s SKIP LOCKED;

  IF v_shard IS NULL THEN
    RAISE EXCEPTION 'no shard with capacity for tenant % (isolation=%)', p_tenant_id, v_isolation
      USING ERRCODE = '53400';   -- configuration_limit_exceeded -> RESOURCE_EXHAUSTED at the API
  END IF;

  UPDATE shards SET namespaces_count = (SELECT count(*) FROM namespaces x
                                         WHERE x.shard_id = v_shard AND x.state <> 'deleted') + 1
   WHERE shard_id = v_shard;
  RETURN v_shard;
END $$;

-- CreateNamespace (engram-api, one catalog transaction):
--   SELECT pick_shard($tenant);
--   INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, epoch, state) VALUES (..., 1, 'creating');
-- then on the shard, as engram_app with the new namespace in scope:
--   INSERT namespace_ownership (ns, tenant, shard, 1, 'active') + namespace_stats + namespace_models
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
GRANT UPDATE (namespaces_count, facts_estimate, bytes_estimate, stats_updated_at, updated_at)
  ON shards TO catalog_app;                                    -- pick_shard + stats reporter (namespaces.large is set by the same reporter)
GRANT EXECUTE ON FUNCTION pick_shard(text) TO catalog_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO catalog_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO catalog_admin;
