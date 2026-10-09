-- +goose Up
-- Catalog schema v1 (PLAN.md section 3.2, 9.2), migration 1: the control-plane database engram_catalog. Derived from
-- docs/plan/sql/catalog_schema.sql (the reference file) by adding goose annotations and the owner switch below; the
-- statements are otherwise the reference's, in its order. N181's replicated-ack helper (catalog_replicated) is included
-- verbatim from the reference; catalog.Replication and AckAfterReplay (E1) only call it.
--
-- Who runs this: the catalog's bootstrap role, which need NOT be a superuser (the T3 test
-- TestSchema_AppliesAsANonSuperuserBootstrapRole applies this file as one). Its recipe: LOGIN CREATEROLE;
-- GRANT pg_read_all_stats TO <boot> WITH ADMIN OPTION; owner of the database (so it owns schema public through
-- pg_database_owner); ALTER ROLE <boot> SET createrole_self_grant =
-- 'set, inherit' (so that the roles it creates here are usable by it). The statements that need that authority (roles,
-- GRANT pg_read_all_stats, the schema grants) run first; then SET LOCAL ROLE catalog_migrate makes catalog_migrate the
-- OWNER of every object created here, as the reference file assumes. RESET ROLE at the end hands the connection back to
-- goose, whose version-table insert needs it.
--
-- Roles
--   catalog_migrate      owner of every object; runs this file and later migrations
--   catalog_app          engram-api (Resolver, NamespaceService, TenantService reads, usage rollups)
--   catalog_admin        engramctl, MoveService, ShardService (full DML)
--   catalog_stats_reader NOLOGIN, member of pg_read_all_stats, owns catalog_replicated and nothing else (N181)
-- The catalog has no RLS: it holds routing metadata only, is reached only by service roles, and never by tenant
-- credentials. It holds no delete log: delete intents (N122) are objects in blob storage, and the shard's deletion_log
-- records what was applied. Passwords are set by provisioning (section 9), never here.
SET LOCAL search_path = public;

-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_migrate') THEN
    CREATE ROLE catalog_migrate LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_app') THEN
    CREATE ROLE catalog_app LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_admin') THEN
    CREATE ROLE catalog_admin LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'catalog_stats_reader') THEN
    CREATE ROLE catalog_stats_reader NOLOGIN;      -- N181: owns catalog_replicated and nothing else
  END IF;
END $$;
-- +goose StatementEnd
GRANT pg_read_all_stats TO catalog_stats_reader;
GRANT CREATE ON SCHEMA public TO catalog_migrate;
GRANT USAGE ON SCHEMA public TO catalog_app, catalog_admin;
-- Only for the ALTER FUNCTION ... OWNER at the end of this file; both are revoked again there.
GRANT catalog_stats_reader TO catalog_migrate;
GRANT CREATE ON SCHEMA public TO catalog_stats_reader;

SET LOCAL ROLE catalog_migrate;

-- -----------------------------------------------------------------------------
-- Enumerations (closed sets; extend with ALTER TYPE ... ADD VALUE in a migration)
-- -----------------------------------------------------------------------------
CREATE TYPE tenant_state    AS ENUM ('active', 'suspended', 'deleting', 'deleted');
CREATE TYPE isolation_mode  AS ENUM ('shared', 'dedicated');
CREATE TYPE shard_state     AS ENUM ('provisioning', 'active', 'full', 'draining', 'readonly', 'retired');
CREATE TYPE namespace_state AS ENUM ('creating', 'active', 'moving', 'frozen', 'restoring', 'deleting', 'deleted');
-- 'restoring' (N64): the shard is being restored; its ownership rows are frozen/restore and surface as NamespaceFrozen.
-- move_state (D5, N125, N160): planned -> frozen (source write-fenced; reads continue) -> copied (copy, verify,
-- indexes, move seal, consumer wait done) -> cutover ((b') target ready) -> committed ((a'') the CAS, the point of no
-- return; then (c), (b''), (d)) -> cleaning -> done. rolled_back is reachable up to and including cutover, and from no
-- later state; restore and failover CAS cutover -> rolled_back and exactly one of the two CASes wins (N123, N161(4)).
-- 'lost' (N183) is terminal, admin-only, from committed or cleaning: a restore left the move-in below its floor. The
-- trigger catalog_check_move_transition enforces the edges.
CREATE TYPE move_state      AS ENUM ('planned', 'frozen', 'copied', 'cutover', 'committed',
                                     'cleaning', 'done', 'rolled_back', 'lost');

-- -----------------------------------------------------------------------------
-- Helpers
-- -----------------------------------------------------------------------------
-- +goose StatementBegin
CREATE FUNCTION catalog_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;
-- +goose StatementEnd

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
-- tenants. config: the tenant layer of system < tenant < namespace inheritance (D12). DELETE_TENANT (N127, N133d) has
-- no operation row: the catalog derives the operation from this row, as DELETE_NAMESPACE is derived from
-- namespaces.state. delete_operation_id and delete_requested_at are written in the same statement that sets state =
-- 'deleting' (the DeleteTenant ack); deleted_at is written by the last step of the TenantDelete workflow together with
-- state = 'deleted'. See the view tenant_delete_operations.
-- -----------------------------------------------------------------------------
CREATE TABLE tenants (
  tenant_id     text PRIMARY KEY CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  display_name  text NOT NULL DEFAULT '',
  state         tenant_state NOT NULL DEFAULT 'active',
  isolation     isolation_mode NOT NULL DEFAULT 'shared',
  config        jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(config) = 'object'),
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  -- the DELETE_TENANT operation id (UUIDv7 minted by engram-api at the ack)
  delete_operation_id  uuid,
  delete_requested_at  timestamptz,                 -- the ack time = the operation's create_time
  -- N182: stamped by TenantDelete once every namespace is frozen/delete
  delete_acknowledged_at timestamptz,
  deleted_at    timestamptz,                        -- the operation's finish_time
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL)),
  CHECK ((state IN ('deleting', 'deleted')) = (delete_operation_id IS NOT NULL)),
  CHECK ((state IN ('deleting', 'deleted')) = (delete_requested_at IS NOT NULL)),
  CHECK (delete_acknowledged_at IS NULL OR state IN ('deleting', 'deleted'))
);

-- DELETE_TENANT operations, derived (N133d): 'deleting' -> RUNNING, 'deleted' -> SUCCEEDED. No other state is
-- derivable: TenantDelete is retried until it completes, so a stuck delete stays RUNNING.
-- TenantService.GetTenantOperation reads this view (admin, tenant-scoped).
CREATE VIEW tenant_delete_operations AS
SELECT t.tenant_id,
       t.delete_operation_id AS operation_id,
       CASE t.state WHEN 'deleting' THEN 'RUNNING' ELSE 'SUCCEEDED' END AS state,
       t.delete_requested_at AS create_time,
       t.delete_acknowledged_at AS acknowledged_at,
       t.deleted_at AS finish_time
  FROM tenants t
 WHERE t.state IN ('deleting', 'deleted');

CREATE TRIGGER tenants_touch BEFORE UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- -----------------------------------------------------------------------------
-- shards: one dedicated PostgreSQL 16 instance each (D2). Dense int32 ids. Placement policy columns:
-- dedicated_tenant_id + capacity counters.
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
  -- N134, N163(4): lower bound for delete-intent replay; lowered with min() by every restore/failover and NEVER raised
  -- while intents are retained (35 d). A catalog restore can lose it, so replay computes min(this, the restore targets
  -- listed under _control/restores/{shard}/ in blob storage): the blob listing is the only mirror
  replay_floor          timestamptz,
  -- N123: pg_control_system().system_identifier of the current primary; written on PROMOTION before the virtual
  -- endpoint flips
  system_identifier     bigint,
  -- N123: pg_control_checkpoint().timeline_id; the mover compares its session's value at Freeze and (c), the relay
  -- every 10 s
  timeline_id           integer,
  dedicated_tenant_id   text REFERENCES tenants (tenant_id),   -- NULL = shared pool
  max_namespaces        integer NOT NULL DEFAULT 120 CHECK (max_namespaces > 0),
  -- N114/N165/D3: 5.5 M live facts target (hot set about 74 GB, footprint about 138 GB; 250 GB per 10 M; 6.5 M only if
  -- M0.6 measures <= 80 GB)
  soft_cap_facts        bigint  NOT NULL DEFAULT 5500000,
  hard_cap_facts        bigint  NOT NULL DEFAULT 10000000,    -- 10 M hard cap (about 250 GB)
  -- 600 GB local NVMe; ShardNearCapacity pages at 70 % of it (relation bytes, N114)
  volume_bytes          bigint  NOT NULL DEFAULT 600000000000 CHECK (volume_bytes > 0),
  namespaces_count      integer NOT NULL DEFAULT 0 CHECK (namespaces_count >= 0),
  facts_estimate        bigint  NOT NULL DEFAULT 0 CHECK (facts_estimate >= 0),
  -- relation bytes of the shard's namespaces, hidden and unpurged rows included (N114)
  bytes_estimate        bigint  NOT NULL DEFAULT 0 CHECK (bytes_estimate >= 0),
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
-- embedding_model / embedding_dims (N111): fixed at namespace creation and mirrored by the shard's namespace_models
-- row; a change is the ReembedNamespace workflow (new vectors, new index, flip, expunge of the old), never a config
-- flip.
-- large: mirror of the shard's namespace_stats.large (N112) — the namespace has crossed 2,000 vectors and owns
-- per-namespace partial HNSW indexes on its shard; a stats-only column, no cache invalidation.
-- -----------------------------------------------------------------------------
CREATE TABLE namespaces (
  namespace_id      uuid PRIMARY KEY,               -- UUIDv7 minted by engram-api
  tenant_id         text NOT NULL REFERENCES tenants (tenant_id),
  name              text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  shard_id          integer NOT NULL REFERENCES shards (shard_id),
  epoch             bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
  -- creating|active|moving|frozen|restoring|deleting|deleted
  state             namespace_state NOT NULL DEFAULT 'creating',
  embedding_model   text NOT NULL DEFAULT 'nomic-embed-text-v1.5'
      CHECK (octet_length(embedding_model) BETWEEN 1 AND 128),
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
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL)),
  -- N65: the namespace group the `ns_group` claim is matched against lives in profile.group (CONFLICTS.md #20)
  CHECK (profile -> 'group' IS NULL OR jsonb_typeof(profile -> 'group') = 'string')
);

-- Reconcile epoch rule (N172): epoch = max over a namespace's active and frozen/* shard rows and a moved_out row's
-- target_epoch; incoming and ready rows never contribute (they carry e + 1 for the whole freeze window). tenant-unique
-- name, reusable after delete
CREATE UNIQUE INDEX namespaces_tenant_name_uq ON namespaces (tenant_id, name) WHERE state <> 'deleted';
CREATE INDEX namespaces_shard_idx  ON namespaces (shard_id, state);
-- Live namespaces per shard, for pick_shard's derived count: tombstones (state = 'deleted') accumulate for ever and
-- must not be walked on every CreateNamespace (N131; M0.3 review F21; not in the reference file).
CREATE INDEX namespaces_live_shard_idx ON namespaces (shard_id) WHERE state <> 'deleted';
CREATE INDEX namespaces_tenant_idx ON namespaces (tenant_id, state);
-- 'creating' rows older than 2 min are completed or deleted by the per-shard op-sweeper (N72, review F-45):
-- CreateNamespace is two-phase across the catalog and the shard.
CREATE INDEX namespaces_creating_idx ON namespaces (created_at) WHERE state = 'creating';

CREATE TRIGGER namespaces_touch BEFORE UPDATE ON namespaces
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- -----------------------------------------------------------------------------
-- namespace_moves: one row per move attempt (D5, N125, N160). At most one live move per namespace. It is also the
-- ARBITER of restore and failover (N123): the catalog CAS
--   UPDATE ... SET state = 'committed' WHERE move_id = $1 AND state = 'cutover'
-- is the point of no return (a''). A restored or promoted shard CASes cutover -> rolled_back only after reading both
-- ownership rows and finding neither moved_out on the source nor active at the move's epoch on the target (N161(4)); if
-- it reads 'committed' it completes (c), (b'') and (d) (reconcile_out). Exactly one CAS wins. Every shard action on an
-- arbiter outcome (thaw, unready_target, rollback_target, reconcile_out, (c)) waits catalog_replicated (10 s per
-- attempt) and re-reads the row (N171(2)). Order: (b') target incoming -> ready; (a'') the CAS; (c) source frozen/move
-- -> moved_out, fenced on the source row and the replicated 'committed' only; (b'') target ready -> active (clears
-- move_id); (d) catalog flip WHERE epoch = e AND state = 'frozen', "already (target, e + 1)" its only idempotent
-- success.
-- Window (N173): read-only from frozen_at; freeze_deadline <= frozen_at + window_seconds, window_seconds >= max(1.5 *
-- w_est, w_est + 10 min), cap 8 h; before (a'') the deadline rolls back (MoveWindowExceeded), after it the page
-- MoveFrozenPastDeadline (N171(4)), never a rollback.
-- Cleanup gate (N170): committed -> cleaning needs now() >= activated_at + 24 h, enforced once, in
-- catalog_check_move_transition. activated_at is stamped by a separate same-state UPDATE ((b'') by the mover, or
-- `engramctl restore` for a target the restore path activated, N180(1)) before the committed -> cleaning statement; the
-- trigger reads OLD.activated_at; backdating by catalog_admin is within the admin trust. CleanupMove takes committed ->
-- cleaning only; the cleanup activity records 'done' after one extra engram_cleanup_namespace returning 0 (N184(2)).
-- The seal (N179) precedes 'cutover': CHECKs below.
-- Reconcile (N163, N180(4), N184(5), N185): the owner of a namespace is its highest-epoch active/frozen/* shard row
-- (frozen/restore counts), re-read once on a torn snapshot; a re-derived 'committed' writes routing only and never
-- activates a shard row; its copy_end_lsn comes from the target row's floor_lsn; a shard unreadable within 5 s is
-- skipped (ReconcileIncomplete).
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
  source_system_id       bigint,                    -- N123: a mismatch fails MoveFenced
  source_timeline_id     integer,
  -- N173(1): rows/R_copy + build(vectors) + max(wal_build/R_archive, wal_build/R_redo) + verify; about 27 min per 1 M
  -- facts
  w_est_seconds          integer NOT NULL CHECK (w_est_seconds >= 0),
  -- N173(3): the operator window (cap 8 h, default 4 h)
  window_seconds         integer NOT NULL CHECK (window_seconds > 0 AND window_seconds <= 28800),
  -- N147: the source's nextval under the freeze; the target is advanced past it once at the start of FrozenCopy ((b')
  -- checks last_value > w_final)
  w_final                bigint,
  -- workflow ids terminated at drain, restarted on the target (N97)
  terminated_workflows   text[] NOT NULL DEFAULT '{}',
  error                  text,
  created_by             text NOT NULL,             -- operator principal (engramctl) or 'rebalancer'
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  frozen_at              timestamptz,
  freeze_deadline        timestamptz,               -- N173(3): at most frozen_at + window_seconds
  -- (b') target incoming -> ready; rollback still possible (unready_target)
  ready_at               timestamptz,
  -- (a'') the catalog CAS cutover -> committed: the point of no return
  committed_at           timestamptz,
  -- N171(2): the standby replayed the 'committed' CAS; (c) waits for it
  committed_replicated_at timestamptz,
  -- N171(2): the standby replayed the 'rolled_back' CAS; thaw / unready_target / rollback_target wait for it
  rolled_back_replicated_at timestamptz,
  moved_out_at           timestamptz,               -- (c) on the source; informational (the arbiter is committed_at)
  activated_at           timestamptz,               -- (b'') target ready -> active
  -- N179(1): pg_switch_wal() on the target after the last index commit (the floor of every target restore and failover)
  copy_end_lsn           pg_lsn,
  copy_end_timeline      int4,
  -- N179(1): segment archived (pgbackrest check) and standby replayed (engram_standby_replayed), before (b')
  copy_sealed_at         timestamptz,
  -- N184(5): re-derived from shard truth; the reconcile stamped copy_sealed_at and committed_replicated_at with it
  reconciled_at          timestamptz,
  lost_at                timestamptz,               -- N183
  lost_restore_id        text,                      -- N183: the _control/restores/ marker that caused it
  -- N183: set on the recovery move of a lost move-in
  recovered_from_move_id uuid REFERENCES namespace_moves (move_id),
  finished_at            timestamptz,
  CHECK (source_shard_id <> target_shard_id),
  CHECK (to_epoch = from_epoch + 1),
  CHECK ((source_system_id IS NULL) = (source_timeline_id IS NULL)),
  CHECK (state NOT IN ('frozen', 'copied', 'cutover', 'committed', 'cleaning', 'done', 'lost') OR frozen_at IS
         NOT NULL),
  CHECK ((frozen_at IS NULL) = (freeze_deadline IS NULL)),
  CHECK (state NOT IN ('committed', 'cleaning', 'done', 'lost') OR committed_at IS NOT NULL),
  CHECK (state NOT IN ('cleaning', 'done') OR moved_out_at IS NOT NULL),
  -- N147: no 'ready' before the target's sequence passed W_final
  CHECK (ready_at IS NULL OR w_final IS NOT NULL),
  -- N173(3): StartMove refuses a shorter window
  CHECK (window_seconds >= greatest(1.5 * w_est_seconds, w_est_seconds + 600)),
  CHECK (freeze_deadline IS NULL OR freeze_deadline <= frozen_at + window_seconds * interval '1 second'),
  -- N173: unattended only when W_est <= 10 min
  CHECK (created_by <> 'rebalancer' OR w_est_seconds <= 600),
  -- N179(1): the seal precedes the commit point
  CHECK (state NOT IN ('cutover', 'committed', 'cleaning', 'done', 'lost') OR copy_sealed_at IS NOT NULL),
  CHECK (copy_sealed_at IS NULL OR copy_sealed_at > frozen_at),
  CHECK (state <> 'lost' OR (lost_at IS NOT NULL AND lost_restore_id IS NOT NULL)),    -- N183
  -- N171(2); the reconcile satisfies it with committed_replicated_at = reconciled_at
  CHECK (moved_out_at IS NULL OR committed_replicated_at IS NOT NULL),
  CHECK (state <> 'done' OR finished_at IS NOT NULL),
  -- Added by M0.3 (review F20); the reference file lacks them. N179: the floor is the three columns RecordFloor writes
  -- together, and the seal means the floor exists, so a move cannot reach `cutover`/`committed` with a NULL
  -- copy_end_lsn / copy_end_timeline for a restore guard to read nothing from.
  CHECK ((copy_sealed_at IS NULL) = (copy_end_lsn IS NULL) AND (copy_end_lsn IS NULL) = (copy_end_timeline IS NULL)),
  -- N125, N171(2): one outcome per row. A rolled_back row never committed; only a rolled_back row has a rollback
  -- replication stamp; a committed_replicated_at needs the commit it replicated.
  CHECK (state <> 'rolled_back' OR committed_at IS NULL),
  CHECK (rolled_back_replicated_at IS NULL OR state = 'rolled_back'),
  CHECK (committed_replicated_at IS NULL OR committed_at IS NOT NULL),
  -- N170: the cleanup gate reads activated_at, so a row in cleaning has one.
  CHECK (state <> 'cleaning' OR activated_at IS NOT NULL)
);

CREATE UNIQUE INDEX namespace_moves_live_uq ON namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back', 'lost');
CREATE INDEX namespace_moves_state_idx ON namespace_moves (state, updated_at);

CREATE TRIGGER namespace_moves_touch BEFORE UPDATE ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_touch_updated_at();

-- The move state machine as a trigger (N125): the only way out of 'committed' is forward, so after the CAS neither a
-- rollback nor a restore reconcile can win. A same-state update is allowed (timestamps).
-- +goose StatementBegin
CREATE FUNCTION catalog_check_move_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state = OLD.state THEN
    RETURN NEW;
  END IF;
  IF OLD.state = 'committed' AND NEW.state = 'cleaning'
     AND (OLD.activated_at IS NULL OR now() < OLD.activated_at + interval '24 hours') THEN
    RAISE EXCEPTION 'cleanup gate: move % is not 24 h past activation', OLD.move_id USING ERRCODE = '23514';
  END IF;
  IF (OLD.state, NEW.state) IN (
       ('planned', 'frozen'), ('frozen', 'copied'), ('copied', 'cutover'),
       ('cutover', 'committed'), ('committed', 'cleaning'), ('cleaning', 'done'),
       ('planned', 'rolled_back'), ('frozen', 'rolled_back'), ('copied', 'rolled_back'),
       ('cutover', 'rolled_back'), ('committed', 'lost'), ('cleaning', 'lost')) THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal move transition % -> % (move %)', OLD.state, NEW.state, OLD.move_id
    USING ERRCODE = '23514';
END $$;
-- +goose StatementEnd

CREATE TRIGGER namespace_moves_transition BEFORE UPDATE OF state ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_check_move_transition();

-- The entry rule of the same state machine (N125, N170, N184(5); M0.3 review F20; not in the reference file): a move
-- is planned, and only the reconcile re-derives a row in a later state (N163; it stamps reconciled_at). Without it an
-- INSERT could enter `cleaning` past the transition trigger and the 24 h gate of N170, which `BEFORE UPDATE OF state`
-- never sees.
-- +goose StatementBegin
CREATE FUNCTION catalog_check_move_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state <> 'planned' AND NEW.reconciled_at IS NULL THEN
    RAISE EXCEPTION 'move % may only be inserted as planned (or re-derived by the reconcile)', NEW.move_id
      USING ERRCODE = '23514';
  END IF;
  IF NEW.state = 'cleaning' AND (NEW.activated_at IS NULL OR now() < NEW.activated_at + interval '24 hours') THEN
    RAISE EXCEPTION 'cleanup gate: move % is not 24 h past activation', NEW.move_id USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TRIGGER namespace_moves_entry BEFORE INSERT ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_check_move_insert();

-- -----------------------------------------------------------------------------
-- idempotency_keys: request_id for catalog-level writes (CreateNamespace, tenant/admin methods), scoped to (tenant,
-- method); 24 h (D1). Shard-level writes keep theirs on the shard.
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
-- tenant_usage_daily: cross-shard rollup for TENANT-level quotas (llm_tokens_per_day). Shards hold per-namespace
-- counters; a tenant may span shards and no query may span shards, so engram-api folds per-minute deltas reported by
-- workers into this table.
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

-- +goose StatementBegin
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
-- +goose StatementEnd

CREATE TRIGGER catalog_events_notify AFTER INSERT ON catalog_events
  FOR EACH ROW EXECUTE FUNCTION catalog_notify_event();

-- Every routing-relevant namespace change is logged (and thereby notified). Stats-only updates (facts_estimate,
-- bytes_estimate, stats_updated_at) do not invalidate caches.
-- +goose StatementBegin
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
-- +goose StatementEnd

CREATE TRIGGER namespaces_log AFTER INSERT OR UPDATE ON namespaces
  FOR EACH ROW EXECUTE FUNCTION catalog_log_namespace_change();

-- Tenant config/state changes invalidate every cached namespace of the tenant.
-- +goose StatementBegin
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
-- +goose StatementEnd

CREATE TRIGGER tenants_log AFTER INSERT OR UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION catalog_log_tenant_change();

-- +goose StatementBegin
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
-- +goose StatementEnd

CREATE TRIGGER namespace_moves_log AFTER INSERT OR UPDATE ON namespace_moves
  FOR EACH ROW EXECUTE FUNCTION catalog_log_move_change();

-- -----------------------------------------------------------------------------
-- Placement policy: pick_shard(tenant) chooses and reserves a shard slot. Dedicated tenants land only on shards flagged
-- for them; shared tenants only on unflagged active shards below both caps. Least loaded first (facts ratio, then
-- namespace count). Called inside the CreateNamespace transaction; the row lock serialises placement.
-- -----------------------------------------------------------------------------
-- +goose StatementBegin
CREATE FUNCTION pick_shard(p_tenant_id text) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  v_isolation isolation_mode;
  v_shard     integer;
  v_n         integer;
BEGIN
  SELECT isolation INTO v_isolation
    FROM tenants
   WHERE tenant_id = p_tenant_id AND state = 'active';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'tenant % is not active', p_tenant_id USING ERRCODE = 'P0002';
  END IF;

  -- N131 (P-16): namespaces_count is DERIVED here from the namespaces table (a counter that is only incremented drifts
  -- after deletes and moves); the stored column is refreshed from the same count. The count is bounded by the shard's
  -- own cap (LIMIT max_namespaces: a shard at its cap is excluded whatever the exact figure) and served by the partial
  -- index namespaces_live_shard_idx, so a CreateNamespace costs at most max_namespaces index entries per candidate
  -- shard, not one per namespace of the fleet (M0.3 review F21).
  SELECT s.shard_id, c.n INTO v_shard, v_n
    FROM shards s
   CROSS JOIN LATERAL (SELECT count(*)::integer AS n
                         FROM (SELECT 1 FROM namespaces x
                                WHERE x.shard_id = s.shard_id AND x.state <> 'deleted'
                                LIMIT s.max_namespaces) AS bounded) AS c
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

  UPDATE shards SET namespaces_count = v_n + 1 WHERE shard_id = v_shard;
  RETURN v_shard;
END $$;
-- +goose StatementEnd

-- catalog_lock_tenant(t): takes the tenant row FOR SHARE for the rest of the caller's transaction and returns its state
-- (NULL when there is no such tenant). CreateNamespace calls it before it inserts, so that the DeleteTenant ack
-- (`UPDATE tenants SET state = 'deleting'`, a FOR NO KEY UPDATE lock, which conflicts with FOR SHARE) either commits
-- first and the create sees `deleting`, or waits for the create and enumerates the new namespace (N122, N182; M0.3
-- review F1). A plain SELECT ... FOR SHARE needs an UPDATE privilege that catalog_app deliberately lacks on tenants,
-- and the foreign key's own FOR KEY SHARE does not conflict with a non-key update; this SECURITY DEFINER function
-- (owned by catalog_migrate, nothing but the lock and the state) is the narrowest grant.
-- +goose StatementBegin
CREATE FUNCTION catalog_lock_tenant(p_tenant_id text) RETURNS text
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
  SELECT t.state::text FROM public.tenants t WHERE t.tenant_id = p_tenant_id FOR SHARE
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION catalog_lock_tenant(text) FROM PUBLIC;

-- CreateNamespace (engram-api, one catalog transaction):
--   SELECT catalog_lock_tenant($tenant);  -- refuse unless 'active'
--   SELECT pick_shard($tenant);
--   INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, epoch, state) VALUES (..., 1, 'creating');
-- then on the shard, as engram_app with the new namespace in scope:
--   INSERT namespace_ownership (ns, tenant, shard, 1, 'active') + namespace_stats + namespace_models
--   (the shard's ownership trigger admits nothing but this active/epoch-1 insert from engram_app);
-- then: UPDATE namespaces SET state = 'active' (which logs + NOTIFYs).
-- A crash between the phases leaves a 'creating' row: the per-shard op-sweeper (N72) completes it when the shard row
-- exists, otherwise deletes both (namespaces_creating_idx); a client retry replays through idempotency_keys exactly
-- like Retain.

-- -----------------------------------------------------------------------------
-- Grants
-- -----------------------------------------------------------------------------
-- The reference grants ON ALL TABLES IN SCHEMA public. goose's own version table lives in the same schema and belongs
-- to the bootstrap superuser, so the grants name the catalog's relations; a later migration that adds one grants it.
GRANT SELECT ON cells, tenants, shards, namespaces, namespace_moves, idempotency_keys, tenant_usage_daily,
  catalog_events, tenant_delete_operations TO catalog_app;
GRANT INSERT, UPDATE ON namespaces TO catalog_app;
GRANT INSERT ON catalog_events TO catalog_app;                 -- via triggers
GRANT INSERT, UPDATE, DELETE ON idempotency_keys TO catalog_app;
GRANT INSERT, UPDATE ON tenant_usage_daily TO catalog_app;
GRANT UPDATE (namespaces_count, facts_estimate, bytes_estimate, stats_updated_at, updated_at)
  -- pick_shard + stats reporter (namespaces.large is set by the same reporter)
  ON shards TO catalog_app;
GRANT EXECUTE ON FUNCTION pick_shard(text), catalog_lock_tenant(text) TO catalog_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON cells, tenants, shards, namespaces, namespace_moves, idempotency_keys,
  tenant_usage_daily, catalog_events, tenant_delete_operations TO catalog_admin;
-- ON ALL FUNCTIONS IN SCHEMA public would also name the functions of whatever extension the image preinstalled in
-- public (pg_stat_statements_reset, ...), which the owner cannot grant; the catalog's own functions are named instead.
GRANT EXECUTE ON FUNCTION catalog_touch_updated_at(), catalog_check_move_transition(), catalog_notify_event(),
  catalog_log_namespace_change(), catalog_log_tenant_change(), catalog_log_move_change(), pick_shard(text),
  catalog_lock_tenant(text), catalog_check_move_insert()
  TO catalog_admin;

-- N171(1), N181: true iff a streaming standby replayed p_lsn (false with none). p_lsn = pg_current_wal_lsn() read after
-- COMMIT in the same session (an upper bound). The owner must be a member of pg_read_all_stats (pg_stat_get_wal_senders
-- masks the rows otherwise); `config lint` asserts pg_has_role(owner, 'pg_read_all_stats', 'USAGE') and that no
-- application role holds it.
-- +goose StatementBegin
CREATE FUNCTION catalog_replicated(p_lsn pg_lsn) RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
  SELECT coalesce(bool_or(replay_lsn >= p_lsn), false) FROM pg_catalog.pg_stat_replication WHERE state = 'streaming'
$$;
-- +goose StatementEnd
-- Privileges first, as the owner; they survive the change of owner below.
REVOKE ALL ON FUNCTION catalog_replicated(pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION catalog_replicated(pg_lsn) TO catalog_admin, catalog_app;
ALTER FUNCTION catalog_replicated(pg_lsn) OWNER TO catalog_stats_reader;

RESET ROLE;
REVOKE CREATE ON SCHEMA public FROM catalog_stats_reader;
REVOKE catalog_stats_reader FROM catalog_migrate;

-- Self-check (N181): the helper is owned by the stats reader, which reads pg_stat_replication through
-- pg_read_all_stats; no application role (nor the owner of every other object) holds that membership; the helper is
-- callable by catalog_app and catalog_admin and by nobody else.
-- +goose StatementBegin
DO $$
BEGIN
  IF (SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname = 'catalog_replicated')
     <> 'catalog_stats_reader' THEN
    RAISE EXCEPTION 'catalog_replicated is not owned by catalog_stats_reader';
  END IF;
  IF NOT pg_has_role('catalog_stats_reader', 'pg_read_all_stats', 'USAGE') THEN
    RAISE EXCEPTION 'catalog_stats_reader lacks pg_read_all_stats';
  END IF;
  IF pg_has_role('catalog_app', 'pg_read_all_stats', 'USAGE')
     OR pg_has_role('catalog_admin', 'pg_read_all_stats', 'USAGE')
     OR pg_has_role('catalog_migrate', 'pg_read_all_stats', 'USAGE') THEN
    RAISE EXCEPTION 'an application role or the owner holds pg_read_all_stats';
  END IF;
  IF NOT (has_function_privilege('catalog_app', 'catalog_replicated(pg_lsn)', 'EXECUTE')
          AND has_function_privilege('catalog_admin', 'catalog_replicated(pg_lsn)', 'EXECUTE'))
     OR has_function_privilege('public', 'catalog_replicated(pg_lsn)', 'EXECUTE') THEN
    RAISE EXCEPTION 'catalog_replicated EXECUTE grants are wrong';
  END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
-- The initial migration of an empty database: dropping the objects is the whole rollback (roles are cluster-level and
-- stay; a data-losing rollback of a populated catalog is a restore, section 9.3). Extension-owned relations (PostGIS in
-- the ParadeDB image's template) are left alone.
-- +goose StatementBegin
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'v'
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
  LOOP
    EXECUTE format('DROP VIEW %s CASCADE', r.rel);
  END LOOP;
  FOR r IN SELECT c.oid::regclass AS rel FROM pg_class c
            WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
              AND c.relname <> 'goose_db_version'
              AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
  LOOP
    EXECUTE format('DROP TABLE %s CASCADE', r.rel);
  END LOOP;
  FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace
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
