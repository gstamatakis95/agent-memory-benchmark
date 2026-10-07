## 3. Data model

This section fixes the physical schema of the two kinds of database Engram runs, what lives in
blob storage, and the queries the schema is shaped for. The complete, runnable DDL is in
`plans/engram/sql/catalog_schema.sql` (control plane) and `plans/engram/sql/shard_schema.sql`
(one copy per shard). Both files parse under the PostgreSQL 16 grammar and apply cleanly to a
PostgreSQL 16.15 instance with pgvector 0.8.6, pg_trgm, btree_gin and btree_gist; the `USING
bm25` statements were parser-checked and written against the pg_search 0.25 syntax (the
`paradedb/paradedb:latest-pg16` image), whose rules for `key_field`, cast-based tokenizers and
columnar filter pushdown are quoted where they matter. Excerpts below are abbreviated; the SQL
files are the source of truth and the section 9 migration `0001_init.sql` is generated from them.

Conventions used throughout: ids are `uuid` holding UUIDv7 (D1); every timestamp is
`timestamptz`; epochs and outbox sequence numbers are `bigint`; hashes are `bytea` of exactly 32
bytes (SHA-256); vectors are `halfvec(768)`; enumerations that are closed sets are Postgres enums,
sets that grow (operation kinds, event names, reasons) are `text` with a `CHECK`.

### 3.1 Principles

1. **Keys lead with `namespace_id`.** Every primary key and every B-tree, GIN and GiST index of
   a namespace-scoped table starts with `namespace_id` (D2). A query for one namespace therefore
   touches one contiguous key range, the planner prunes one of the 16 hash partitions, and the
   isolation predicate costs nothing extra. Rationale: the namespace is the unit of every
   request, move, export and delete; nothing ever asks for "all facts on the shard" except
   admin tooling. Rejected: `tenant_id` as the leading column (a tenant spans namespaces, and
   nothing is addressed per tenant on the data path).
2. **Row-Level Security on every namespace-scoped table**, policy `ns_isolation`:
   `namespace_id = current_setting('engram.namespace_id')::uuid` as both `USING` and `WITH
   CHECK`, enabled *and forced*, applied to the 16 partitions as well as to the parents. The
   application role `engram_app` is `NOBYPASSRLS`. A transaction that forgot to set the scope
   does not see zero rows, it errors: an unset GUC raises `42704`, the empty string left behind
   after `SET LOCAL` expires raises `22P02` on the uuid cast (both verified). Rationale: a
   missing `WHERE namespace_id =` becomes an error or an empty result, never a leak. Rejected:
   application predicates only (one forgotten predicate is a cross-tenant leak); per-namespace
   tables (thousands of relations per shard, DDL on namespace create).
3. **Sixteen hash partitions by `namespace_id`** for the four large tables (`facts`,
   `fact_links`, `chunks`, `entity_mentions`). Each HNSW and BM25 index is one sixteenth of the
   shard, which is what makes an HNSW build fit `maintenance_work_mem` and keeps per-partition
   `VACUUM` short; the namespace predicate prunes to one partition at executor start-up (verified
   with `current_setting()` in the plan: "Subplans Removed: 15"). Rejected: one partition per
   namespace (150 to 1,000 relations, DDL per namespace, planner cost per partition); no
   partitioning (a single 18 GB HNSW index that cannot be built in memory and a 300 M-row table
   whose vacuum is measured in hours).
4. **Only `ingest_ledger` is append-only.** A trigger refuses `UPDATE` always and `DELETE` for
   every role except the purge path (`engram_admin`) and move cleanup (`engram_move`). Every
   other table is ordinary mutable state: `retired_at`, `invalidated_at`, `stale_write`/`stale_delete`,
   `derived_from_deleted` (set once, never cleared), version pointers and counters are updated in
   place, and physical rows are purged asynchronously.
   Rationale: the ledger is the audit trail and the replay source; derived tables must be
   cheap to retire and un-retire (D8). Rejected: an append-only `facts` table with status
   rows (every recall would need an anti-join; the benchmark code in this repository does
   exactly that and pays for it on every query).
5. **`tenant_id` is denormalised on every row** of every namespace-scoped table, and the root
   tables carry `FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership`.
   Rationale: blob keys, metering, exports and the deletion log need the tenant without a
   join, and a row whose tenant disagrees with its namespace is rejected at insert on the root
   tables. Cost: ~12 bytes per row, about 4 GB at 300 M `fact_links` rows (3.7). Rejected:
   tenant only in the catalog (every export and every blob key would resolve it).
6. **Epoch appears on `namespace_ownership` and `outbox` only.** The ownership row is the
   fence *value* (`state='active' AND epoch=$e`, read with a plain `SELECT` under the
   namespace's shared advisory lock, which is the fence *lock*; D2 as amended by review F-5,
   3.3); the outbox row is stamped with
   the epoch of the transaction that wrote it so consumers and the move replay can tell which
   owner produced it (D5, D6). Data rows never carry the epoch and no identity or idempotency
   key includes it (D11): a workflow restarted on the target after a move recognises the
   chunks it committed at the previous epoch.
7. **`shard_id` appears nowhere in data rows.** Inside a shard database the shard is implied by
   the database: `shard_meta` holds the one-row identity `(shard_id, schema_version)` and
   `namespace_ownership.shard_id` is checked against it by a trigger. Rationale: a shard id
   on every row would be a constant column, and a wrong value could only mean a mis-provisioned
   database, which the trigger catches at the first ownership insert. A self-check at the end
   of the DDL fails the migration if any other table grows a `shard_id` column.
8. **Everything recall can see is visible through plain filters.** `live` is a stored generated
   column (`retired_at IS NULL AND invalidated_at IS NULL`), `fact_type_code` and `tag_count`
   are stored generated columns, observation visibility is `observation_versions.live`
   (`retired_at IS NULL AND NOT derived_from_deleted AND NOT (stale_delete AND superseded_at IS
   NULL)` — N41 as amended by review F-1: a version whose prompt inputs named a now-deleted fact
   is hidden *permanently and per version*, the observation's *current* version is hidden while
   the observation awaits a rewrite, and one that merely gained or lost-by-replace evidence is
   `stale_write` and visible), and observation visibility at `as_of` is precomputed as
   `effective_at <= T AND (superseded_at IS NULL OR superseded_at > T)`. Rationale: HNSW
   iterative scans and pg_search Top-K pushdown both need their filters to be columnar
   predicates, not joins or subqueries (3.8). Rejected: computing visibility at query time with
   correlated subqueries (kills Top-K pushdown, 10x slower lexical arm).

Mutable versus append-only, per table group:

| Group | Mutability | Physical delete |
|---|---|---|
| `ingest_ledger` | append-only (trigger) | purge after an acknowledged delete, namespace delete, move cleanup; `engram_admin`/`engram_move` only |
| `facts`, `chunks` | `retired_at`, `purge_after`, `invalidated_at`, `consolidated_at`, tags/metadata on re-retain | `PurgeDocument` / `PurgeNamespace` in batches of 1,000 once `purge_after <= now()` |
| `fact_links`, `entity_mentions` | insert/delete only, never updated | purge only (N61, review F-19): the synchronous cascade leaves them; the graph arm never traverses a non-live endpoint, so a dangling link is inert |
| `observation_sources`, `observation_inputs`, `page_sources`, `document_version_chunks` | insert/delete only, never updated | synchronous in the delete cascade (D8, N41) or purge (N42: a replace-retire keeps `observation_sources`/`observation_inputs` until the purge) |
| `documents`, `document_versions`, `observations`, `observation_versions`, `pages`, `page_versions`, `entities`, `operations` | updated in place (state, counters, flags, versions; `observation_versions.derived_from_deleted` is set once and never cleared) | purge / namespace delete |
| `namespace_ownership`, `namespace_stats`, `outbox_cursors`, `token_usage`, `quota_counters` | ownership: state-machine transitions only (3.3.1); stats: refreshed by the sweeper (N69), never on the commit path; others hot updates, `fillfactor` 50 to 70 | ownership: an `incoming` row on move rollback, a `frozen`/`delete` row after `NamespacePurged`; `active` and `moved_out` rows are never deleted (they are the fence value a late writer must still hit) |
| `outbox`, `deletion_log`, `token_usage_events`, `consolidation_applied`, `move_applied` | insert only | retention jobs (7 d, catalog-replicated, 30 d, never, move end) |
| `consolidation_proposals` | write-once (N43): `UPDATE` refused by a trigger | only the §5.2 discard path (apply re-verification failed before any op was applied; the `RESTRICT` FK from `consolidation_applied` refuses it afterwards); namespace delete |

### 3.2 Catalog schema (control-plane database `engram_catalog`)

One small PostgreSQL 16 with a streaming replica (D4), schema `public`, no RLS (only service
roles connect). Tables: `cells`, `tenants`, `shards`, `namespaces`, `namespace_moves`,
`idempotency_keys` (catalog-level writes, D1), `tenant_usage_daily`, `deletion_log` (N21
replica) and `catalog_events` with the `LISTEN/NOTIFY` trigger. Roles: `catalog_migrate`
(owner), `catalog_app` (engram-api), `catalog_admin` (engramctl, MoveService, ShardService).

```sql
CREATE TABLE tenants (
  tenant_id     text PRIMARY KEY CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  display_name  text NOT NULL DEFAULT '',
  state         tenant_state NOT NULL DEFAULT 'active',        -- active|suspended|deleting|deleted
  isolation     isolation_mode NOT NULL DEFAULT 'shared',      -- shared|dedicated
  config        jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(config) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at    timestamptz,
  CHECK ((state = 'deleted') = (deleted_at IS NOT NULL))
);

CREATE TABLE shards (
  shard_id              integer PRIMARY KEY CHECK (shard_id >= 0),       -- dense int32 (D1)
  cell_id               text NOT NULL REFERENCES cells (cell_id),
  state                 shard_state NOT NULL DEFAULT 'provisioning',    -- provisioning|active|full|draining|readonly|retired
  pgbouncer_addr        text NOT NULL,
  direct_addr           text NOT NULL,              -- relay election lock, move, engramctl (N15)
  dsn_secret_ref        text NOT NULL,              -- secret name, never a DSN
  blob_prefix           text NOT NULL,              -- = shard_id::text
  blob_cred_secret_ref  text NOT NULL,              -- credential scoped to blob_prefix/*
  task_queue            text NOT NULL,              -- 'shard-{id}'
  kafka_topic           text NOT NULL,              -- 'engram.events.shard-{id}'
  dedicated_tenant_id   text REFERENCES tenants (tenant_id),            -- NULL = shared pool
  max_namespaces        integer NOT NULL DEFAULT 150,
  soft_cap_facts        bigint NOT NULL DEFAULT 10000000,
  hard_cap_facts        bigint NOT NULL DEFAULT 20000000,
  namespaces_count      integer NOT NULL DEFAULT 0,
  facts_estimate        bigint NOT NULL DEFAULT 0,
  bytes_estimate        bigint NOT NULL DEFAULT 0,
  stats_updated_at      timestamptz,
  schema_version        integer NOT NULL DEFAULT 0,
  ...,
  CHECK (blob_prefix = shard_id::text), CHECK (task_queue = 'shard-' || shard_id::text)
);

CREATE TABLE namespaces (
  namespace_id      uuid PRIMARY KEY,                           -- UUIDv7 minted by engram-api
  tenant_id         text NOT NULL REFERENCES tenants (tenant_id),
  name              text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  shard_id          integer NOT NULL REFERENCES shards (shard_id),
  epoch             bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
  state             namespace_state NOT NULL DEFAULT 'creating', -- creating|active|moving|frozen|restoring|deleting|deleted (N64)
  config            jsonb NOT NULL DEFAULT '{}',                 -- inheritable layer (D12)
  profile           jsonb NOT NULL DEFAULT '{}',                 -- reflect mission/directives/disposition, not inherited
  large             boolean NOT NULL DEFAULT false,              -- mirror of the shard's namespace_stats.large (N55)
  facts_estimate    bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0,
  ...
);
CREATE UNIQUE INDEX namespaces_tenant_name_uq ON namespaces (tenant_id, name) WHERE state <> 'deleted';
CREATE INDEX namespaces_shard_idx ON namespaces (shard_id, state);
CREATE INDEX namespaces_creating_idx ON namespaces (created_at) WHERE state = 'creating';   -- op-sweeper (N72)

CREATE TABLE namespace_moves (
  move_id uuid PRIMARY KEY, namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  source_shard_id integer NOT NULL, target_shard_id integer NOT NULL,
  from_epoch bigint NOT NULL, to_epoch bigint NOT NULL CHECK (to_epoch = from_epoch + 1),
  state move_state NOT NULL DEFAULT 'planned',  -- planned|copying|catching_up|frozen|cutover|cleaning|done|rolled_back
  p0_seq bigint, applied_seq bigint, lag_events bigint,
  terminated_workflows text[] NOT NULL DEFAULT '{}',   -- restarted at cutover with the same ids (D5 step 5)
  error text, created_by text NOT NULL, ..., frozen_at timestamptz, cutover_at timestamptz, finished_at timestamptz
);
CREATE UNIQUE INDEX namespace_moves_live_uq ON namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back');                   -- at most one live move per namespace
```

**Config inheritance.** `tenants.config` and `namespaces.config` are both `jsonb` objects
holding the keys of D12 (`models.*`, `chunk.target_chars`, `recall.default_budget`,
`quota.*`). The database checks only that each is an object; key validation is the Go config
schema's job (see section 2, `internal/config`), and the resolver caches the deep-merged
`system ⊂ tenant ⊂ namespace` result with the catalog entry. `namespaces.profile` is kept
apart from `config` because it is identity, not configuration: it is never inherited and a
tenant default for a reflect mission would be wrong.

**Placement policy.** Columns `dedicated_tenant_id`, `max_namespaces`, `soft_cap_facts`,
`namespaces_count`, `facts_estimate`, `bytes_estimate` drive one function, called inside the
`CreateNamespace` transaction:

```sql
CREATE FUNCTION pick_shard(p_tenant_id text) RETURNS integer LANGUAGE plpgsql AS $$
  ...
  SELECT shard_id INTO v_shard FROM shards
   WHERE state = 'active' AND namespaces_count < max_namespaces AND facts_estimate < soft_cap_facts
     AND CASE WHEN v_isolation = 'dedicated' THEN dedicated_tenant_id = p_tenant_id
              ELSE dedicated_tenant_id IS NULL END
   ORDER BY facts_estimate::numeric / soft_cap_facts::numeric, namespaces_count, shard_id
   LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF v_shard IS NULL THEN RAISE EXCEPTION '...' USING ERRCODE = '53400'; END IF;   -- RESOURCE_EXHAUSTED
  UPDATE shards SET namespaces_count = namespaces_count + 1 WHERE shard_id = v_shard;
  RETURN v_shard;
$$;
```

Least-loaded-first by the facts ratio, then namespace count, then id; `SKIP LOCKED` lets
concurrent creates pick different shards instead of queueing. `facts_estimate` and `large` are
refreshed hourly from each shard's `namespace_stats` by `engramctl stats` (section 9); neither
invalidates the resolver cache (the catalog trigger skips stats-only updates). Rejected: random
placement (hot namespaces pile up); power-of-two choices (needs live load, which the catalog
does not have at creation time).

`CreateNamespace` is two-phase across the catalog and the shard (catalog row `creating` →
shard `namespace_ownership` row `active`/epoch 1, inserted by `engram_app` with the new
namespace in scope → catalog row `active`). A crash between the phases leaves a `creating`
row; the per-shard op-sweeper (N3, N72, review F-45) completes it when the shard row exists and
otherwise deletes both, using `namespaces_creating_idx` for rows older than 2 min, and a client
retry replays through `idempotency_keys` exactly like Retain. `restoring` (N64) is the state a
namespace shows while its shard is restored from backup, so a client can tell a restore
(`NamespaceFrozen` from a `freeze_reason = 'restore'` row) from a move.

**Events and cache invalidation.** `catalog_events` is append-only; `AFTER INSERT` it
`pg_notify('catalog_changes', json)` with `{event_id, kind, namespace_id, tenant_id, shard_id,
epoch, state}` (D4). Triggers on `namespaces`, `tenants` and `namespace_moves` insert an event
for every routing-relevant change and skip stats-only updates, so the resolver's `LISTEN`
never fires for an hourly estimate refresh:

```sql
CREATE FUNCTION catalog_notify_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('catalog_changes', json_build_object(
    'event_id', NEW.event_id, 'kind', NEW.kind, 'namespace_id', NEW.namespace_id,
    'tenant_id', NEW.tenant_id, 'shard_id', NEW.shard_id, 'epoch', NEW.epoch, 'state', NEW.state)::text);
  RETURN NULL;
END $$;
CREATE TRIGGER catalog_events_notify AFTER INSERT ON catalog_events
  FOR EACH ROW EXECUTE FUNCTION catalog_notify_event();
```

Retention of `catalog_events` is 30 days; the table doubles as the audit log of every move and
epoch bump. `idempotency_keys(tenant_id, method, request_id, request_hash, response,
expires_at)` holds the 24 h keys of catalog-level writes (`CreateNamespace`, tenant and admin
methods). `tenant_usage_daily(tenant_id, day, quota_key, used)` is the cross-shard rollup that
tenant-level daily quotas need: a tenant may span shards and no query may span shards, so
workers report per-minute deltas through the API (off the hot path, D4) and the quota package
reads this table. `deletion_log` mirrors every shard's `deletion_log` (N21) so a restore can
re-apply deletes made after the backup point.

### 3.3 Shard schema (identical on every shard)

Schema `public` (what the section 8 `--check-rls` query and the section 9 tooling assume),
PostgreSQL 16, extensions `vector`, `pg_search`, `pg_trgm`, `btree_gin`, `btree_gist`.

**Roles** (all `LOGIN`; passwords from the shard secret, section 9):

| Role | RLS | Grants | Used by |
|---|---|---|---|
| `engram_migrate` | owner | DDL | migrations only |
| `engram_app` | `NOBYPASSRLS`, confined by `ns_isolation` | DML on all namespace-scoped tables; `SELECT, INSERT` on `ingest_ledger`, `outbox`, `deletion_log`; `SELECT` on `namespace_ownership` (the fence is a plain read under the advisory lock — no row lock, hence no `UPDATE` grant; review F-5) plus `INSERT` of the first `active`/epoch-1 row at `CreateNamespace` (the trigger admits nothing else from this role); `SELECT` on `shard_meta` | API and worker per-namespace transactions |
| `engram_relay` | `NOBYPASSRLS` + policy `relay_read_all` on `outbox` (`SELECT`, all namespaces) | `SELECT` on `outbox`; all on `outbox_cursors`; nothing else (D6, N4) | outbox relay, on a direct connection (N15) |
| `engram_move` | `NOBYPASSRLS`, confined by `ns_isolation` to the namespace being moved | DML on every namespace-scoped table and on `namespace_ownership`, `move_applied`; `SELECT, INSERT` on `outbox`; may `DELETE` from `ingest_ledger` | move executor (N2): `COPY … TO STDOUT` out of the source, ownership transitions, replay by anti-join (N50), cleanup |
| `engram_move_load` | `BYPASSRLS` (`COPY FROM` is refused on RLS tables: "COPY FROM not supported with row-level security", verified on PG 16) | `INSERT` only, on the namespace-scoped tables that are copied (not `outbox`, `namespace_ownership`, `move_applied`); `SELECT` on `namespace_ownership` and `shard_meta` for the per-batch fence; self-check 7 pins this shape | the target side of the move copy (N51, review F-4): `COPY … FROM STDIN BINARY` in 10 k-row batches, each its own transaction that takes the shared advisory lock and requires `state = 'incoming' AND epoch = e + 1` |
| `engram_admin` | `BYPASSRLS` | all | engramctl, purge workflows, shard-wide schedulers (op-sweeper, DEFERRED resumer, stats sweeper, retention), delete freeze, restore |

`engram_move` deliberately runs *under* RLS for everything but the bulk load: the move touches
exactly one namespace, so the policy is a second fence for free on the source read, the replay
and the ownership transitions (a bug that mixes namespaces gets zero rows or `42501`, never
another tenant's data; section 8 `TestIso_Move_TenantMix` relies on this). The bulk load into
the target cannot run under RLS (F-4), which is the only reason `engram_move_load` exists; its
fence is the explicit per-batch ownership check under the shared advisory lock, so a cutover
or rollback (exclusive lock) waits for in-flight batches and a batch that starts afterwards
sees a row that is no longer `incoming` and aborts. Partitions receive no grants at all, so a
direct partition reference by any non-admin role is `permission denied` (queries through the
parent check parent privileges only).

**Transaction scope and fence** (D2 row 3 as amended by review F-5; N64). Every store
transaction starts with

```sql
SELECT set_config('engram.namespace_id', $1, true),
       set_config('engram.tenant_id',    $2, true),
       set_config('engram.epoch',        $3, true);           -- SET LOCAL, pgbouncer-safe
-- writes: the advisory lock is the fence LOCK (fair queue), the row is the fence VALUE
SELECT pg_advisory_xact_lock_shared(engram_ns_lock_key($1::uuid));   -- = hashtextextended(namespace_id::text, 0)
SELECT state, epoch, freeze_reason FROM namespace_ownership WHERE namespace_id = $1::uuid;   -- plain SELECT, no row lock
--   proceed iff state = 'active' AND epoch = $3
--   frozen/move, frozen/restore -> NamespaceFrozen{retry_after}; frozen/delete -> PreconditionFailed{NAMESPACE_DELETING}
--   incoming, moved_out, no row, or epoch <> $3 -> WrongShardOrEpoch{namespace_state}
-- reads: no lock, state only, the epoch is not checked (a reader whose cache lags one epoch still reads the frozen source, D5 step 4)
SELECT state, freeze_reason FROM namespace_ownership WHERE namespace_id = $1::uuid;
--   proceed iff state = 'active' OR (state = 'frozen' AND freeze_reason IN ('move', 'restore'))
--   moved_out -> WrongShardOrEpoch{MOVED_OUT} (the API re-resolves in a bounded loop, N52); frozen/delete -> NAMESPACE_DELETING
```

**Lock discipline.** The ownership *row* is never row-locked by anyone: compatible row lockers
(`FOR SHARE`) are granted past a waiting `FOR UPDATE`, so a 32-way `CommitChunk` fan-out plus
consolidation could keep a hot row share-locked indefinitely and starve a freeze, and every
additional share locker allocates a multixact (F-5). Heavyweight advisory locks queue fairly:
once an exclusive request is waiting, later shared requests wait behind it. The parties and
their modes, all on the one key `engram_ns_lock_key(namespace_id)`:

| Party | Lock | Scope | Then |
|---|---|---|---|
| every write transaction (`engram_app`, `engram_move` replay, `engram_move_load` batch) | shared | transaction (`pg_advisory_xact_lock_shared`; pooling-safe) | plain `SELECT` of the ownership row; the predicate above |
| copy barrier (D5 step 2) | exclusive | **session** (`pg_advisory_lock`/`pg_advisory_unlock`) on the mover's direct connection (N15: session locks do not survive transaction pooling) | `BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT max(seq) FROM outbox WHERE namespace_id = $1` (the first statement fixes the snapshot and `p0` while the lock is held), then `pg_advisory_unlock`, then the `COPY … TO STDOUT` statements in the same snapshot. Every writer that drew a `seq` before the barrier has committed or aborted (A-F1), so no `seq ≤ p0` can appear later |
| freeze (D5 step 4), cutover (b)/(c), rollback | exclusive | transaction | the ownership `UPDATE` of 3.3.1; commits in milliseconds; writers queued behind it then read `frozen`/`moved_out` |
| delete freeze, restore (`engram_admin`) | exclusive | transaction | `UPDATE … SET state = 'frozen', freeze_reason = 'delete'` (or `'restore'`) |
| export snapshot (`ExportSnapshot`) | exclusive | session, as the barrier | opens its `REPEATABLE READ` snapshot behind every in-flight writer so the snapshot is a prefix of the outbox |
| reads | none | — | state only |

Why a freeze cannot lose a write: a write commits only inside a transaction that held the
shared lock while it read `active` at its epoch; the freeze takes the exclusive lock, so it
starts after every such transaction has committed or aborted and no writer that read `active`
before the freeze can commit after it (D5 safety argument, TLC `ShardMove`). The lock is
transaction-scoped on writers precisely so that pgbouncer's transaction pooling cannot leak
it; the two session-scoped uses live on direct connections. A 64-bit key collision between
two namespaces only over-serialises them, never under-fences one.

#### 3.3.1 Shard-level tables

```sql
CREATE TABLE shard_meta (                         -- exactly one row (trigger-enforced); N22
  shard_id integer NOT NULL, schema_version integer NOT NULL, applied_at timestamptz NOT NULL DEFAULT now(),
  engram_min_version text NOT NULL DEFAULT '', engram_max_version text NOT NULL DEFAULT ''
);

CREATE TABLE namespace_ownership (                -- the fence VALUE (D2 row 4, D5); mirrors the catalog
  namespace_id      uuid PRIMARY KEY,
  tenant_id         text NOT NULL CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  shard_id          integer NOT NULL,             -- the ONLY shard_id column on data tables; trigger-checked against shard_meta
  epoch             bigint NOT NULL CHECK (epoch >= 1),
  state             ownership_state NOT NULL,     -- incoming|active|frozen|moved_out
  freeze_reason     text CHECK (freeze_reason IN ('move', 'delete', 'restore')),   -- why frozen (D2)
  move_id           uuid,
  move_applied_seq  bigint,                       -- replay PROGRESS of a move (lag reporting); never a read cursor (N50)
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),               -- FK target of every root table
  CHECK (state <> 'incoming' OR move_id IS NOT NULL),
  CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL))   -- a thaw clears the reason
);
CREATE TRIGGER namespace_ownership_check BEFORE INSERT OR UPDATE OR DELETE ON namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram_check_ownership();   -- shard_id = shard_meta.shard_id; epoch never decreases; only the transitions below

CREATE TABLE move_applied (                       -- exactly-once ledger of replayed events (target: authoritative; source: the mover's lagging copy for the anti-join, N50)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, seq bigint NOT NULL, move_id uuid NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, seq), FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

CREATE TABLE namespace_stats (                    -- DERIVED counters (N69): refreshed by the stats sweeper, never written on the commit path
  namespace_id uuid PRIMARY KEY, tenant_id text NOT NULL,
  large boolean NOT NULL DEFAULT false, large_since timestamptz,    -- >= 20 k live facts: per-namespace partial HNSW (N55, 3.3.4)
  live_facts bigint NOT NULL DEFAULT 0, total_facts bigint NOT NULL DEFAULT 0, live_chunks bigint NOT NULL DEFAULT 0,
  live_documents bigint NOT NULL DEFAULT 0, live_observations bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0,
  recalls_1h bigint NOT NULL DEFAULT 0, retains_1h bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);
```

**Replay bookkeeping (N50, review F-3).** The move's catch-up and drain never read "`seq >
watermark`": a writer that drew `seq` 102 in its last statement can commit after `seq` 103 was
read and applied, and under RLS the mover sees only its namespace's rows, so a hole between
them is indistinguishable from another namespace's `seq` and there is no gap to watch. The read
is an **anti-join** on the source against `move_applied`:

```sql
SELECT o.seq, o.event_type, o.payload
  FROM outbox o
 WHERE o.namespace_id = $1 AND o.seq > $p0
   AND NOT EXISTS (SELECT 1 FROM move_applied m WHERE m.namespace_id = o.namespace_id AND m.seq = o.seq)
 ORDER BY o.seq LIMIT 500;
```

`move_applied` therefore exists on both shards. On the target it is authoritative and
exactly-once: `INSERT INTO move_applied … ON CONFLICT DO NOTHING RETURNING seq` runs in the
same transaction as the event's row-level apply (no row → already applied, skip). On the
source the mover inserts the same key *after* the target transaction committed; it is a
lagging copy, so a crash between the two inserts only makes the next round re-read an event
that the target then drops. Catch-up runs the anti-join per round (≤ 10 rounds, D5 step 3);
after the freeze no writer holds a `seq`, `max(seq)` for the namespace is final, and one last
anti-join pass is complete by construction (D5 step 5). `move_applied_seq` is kept only to
report lag. Both copies are deleted when the move reaches `done`/`rolled_back`. Rejected: a
separate `move_state` table (one more row to copy and to reason about); the watermark alone
(the F-3 interleaving skips a late commit); granting `engram_move` the relay's shard-wide
`SELECT` on `outbox` (the ND-5 rejected option — it widens the one role that holds two shard
handles).

**Namespace stats are derived (N69, review F-36).** `CommitChunk`, `FinalizeVersion` and the
delete cascade no longer `UPDATE namespace_stats`: a per-commit counter update serialised
every commit of a namespace on one row and put the (seconds-long) cascade in the same queue.
The stats sweeper (`engram_admin`, every 60 s per namespace, one read per tick) refreshes
`live_facts` with `count(*)` over the partial index `facts_mentioned_idx (… ) WHERE live`,
`live_chunks`/`live_documents`/`live_observations` likewise, and flips `large` when
`live_facts` crosses 20 k (cleared below 10 k, hysteresis). `max_facts` and the catalog's
`facts_estimate` tolerate one refresh interval of staleness. `document_versions.chunks_done`
and `operations.progress` follow the same rule: written once per wave by the workflow, never
per chunk (a per-chunk `UPDATE` of the version row after the N40 `FOR SHARE` would deadlock
two commits of the same version).

**Ownership state machine (N64, review F-23).** Written once here; `engram_check_ownership`
enforces it on the real DDL and section 8's T3 `TestIso_Ownership_Transitions` executes every
row of this table before any move code exists. Every transition that changes `state` runs
under the exclusive advisory lock (3.3) in a transaction of its own.

| From → to | Role | Statement | Notes |
|---|---|---|---|
| (none) → `active`, epoch 1 | `engram_app` (`CreateNamespace`), `engram_admin` | `INSERT … (ns, tenant, shard, 1, 'active')` | the only ownership write `engram_app` may make |
| (none) → `incoming`, epoch e+1 | `engram_move` | `INSERT … (ns, tenant, shard, e+1, 'incoming', move_id)` | move plan, target shard; data load follows under `engram_move_load` |
| `moved_out` → `incoming`, epoch > old | `engram_move` | `UPDATE … SET state = 'incoming', epoch = $e1, move_id = $m WHERE state = 'moved_out'` | the namespace returns to a former shard; the old row is reused |
| `active` → `frozen` (`move`), same epoch | `engram_move` | `UPDATE … SET state = 'frozen', freeze_reason = 'move', move_id = $m WHERE namespace_id = $1 AND epoch = $e AND state = 'active'` | D5 step 4; the `freeze_reason` is mandatory (`CHECK`), review F-23 |
| `active` → `frozen` (`delete`) | `engram_admin` | `… freeze_reason = 'delete'` | namespace delete; surfaces as `NAMESPACE_DELETING`; never thaws |
| `active` → `frozen` (`restore`) | `engram_admin` | `… freeze_reason = 'restore'` on every `active` row of the shard | restore-from-backup (N23); catalog `restoring` |
| `frozen` (`move`) → `active`, same epoch | `engram_move` | `UPDATE … SET state = 'active', freeze_reason = NULL, move_id = NULL` | move rollback |
| `frozen` (`restore`) → `active`, epoch bumped | `engram_admin` | `UPDATE … SET state = 'active', freeze_reason = NULL, epoch = $e1` | restore completion; the bump fences any zombie of the old primary (N63) |
| `frozen` (`move`) → `moved_out` | `engram_move` | `UPDATE … SET state = 'moved_out', freeze_reason = NULL` | cutover (c), after the target became `active` (b) |
| `incoming` → `active`, same epoch | `engram_move` | `UPDATE … SET state = 'active', move_id = NULL WHERE state = 'incoming'` | cutover (b); the row already carries e+1 |
| same → same | `engram_move`, `engram_admin` | `move_applied_seq`, `freeze_reason`, `updated_at` touches | |
| `incoming` → (deleted) | `engram_move` | `DELETE` | move rollback, target side |
| `frozen` (`delete`) → (deleted) | `engram_admin` | `DELETE` after `NamespacePurged` | |
| `active`, `moved_out` → (deleted) | — | refused (`42501`) | the row is the value a late writer or stale reader must still hit (`WrongShardOrEpoch{MOVED_OUT}`, N52) |

Anything else (`active → moved_out`, `frozen/delete → active`, an epoch decrease, a frozen row
without a reason, an `engram_app` insert that is not `active`/epoch 1) is `23514`/`42501`.
The read fence looks at `state` and `freeze_reason` only — never the epoch (N64): a reader
whose catalog cache lags one epoch still reads the frozen source as D5 step 4 promises, and a
reader that hits `moved_out` is told so and re-resolves.

#### 3.3.2 Outbox

```sql
CREATE SEQUENCE outbox_seq AS bigint CACHE 1;     -- CACHE 1 keeps seq close to commit order
CREATE TABLE outbox (
  seq           bigint PRIMARY KEY DEFAULT nextval('outbox_seq'),
  namespace_id  uuid   NOT NULL DEFAULT current_setting('engram.namespace_id')::uuid,   -- stamped from the scope
  tenant_id     text   NOT NULL DEFAULT current_setting('engram.tenant_id'),
  epoch         bigint NOT NULL DEFAULT current_setting('engram.epoch')::bigint,
  event_type    text   NOT NULL CHECK (event_type ~ '^[A-Z][A-Za-z0-9]{2,63}$'),      -- Event oneof case name
  payload       bytea  NOT NULL CHECK (octet_length(payload) <= 16384),               -- thin (N12)
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_ns_seq_idx ON outbox (namespace_id, seq);

CREATE TABLE outbox_cursors (                     -- consumers + the 'relay' row (gap watchlist)
  consumer   text PRIMARY KEY CHECK (consumer ~ '^(relay|index|kafka|deletion-log|move:[0-9a-f-]{36})$'),
  last_seq   bigint NOT NULL DEFAULT 0,
  gaps       jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(gaps) = 'array' AND jsonb_array_length(gaps) <= 1000),
  updated_at timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);
```

`namespace_id`, `tenant_id` and `epoch` on the outbox row are column defaults read from the
transaction scope: the application cannot stamp an event with a namespace other than the one
it is fenced on, and the `WITH CHECK` half of the RLS policy rejects an explicit wrong value
(verified: "new row violates row-level security policy"). The outbox is the only
namespace-tagged table whose primary key does not lead with `namespace_id`: the relay must
read the whole shard in `seq` order (D6), and the secondary index `(namespace_id, seq)` serves
the per-namespace readers (move, export delta). `outbox_skipped(namespace_id, consumer, seq,
reason, skipped_by)` records poison events an operator skipped (section 9 runbook); `deletion_log`
is described with the delete cascade (3.8).

#### 3.3.3 Ingest ledger, documents, versions

```sql
CREATE TABLE ingest_ledger (                      -- append-only (trigger); body inline <= 64 KiB, else blob (N7)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, ledger_id uuid NOT NULL,
  document_id text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  operation_id uuid NOT NULL, request_id text,
  update_mode update_mode NOT NULL,               -- replace|append
  item_timestamp timestamptz NOT NULL, context text NOT NULL DEFAULT '',
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  metadata jsonb NOT NULL DEFAULT '{}', entity_hints jsonb NOT NULL DEFAULT '[]',
  content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),       -- sha256(body)
  content_bytes integer NOT NULL CHECK (content_bytes BETWEEN 0 AND 1048576),
  body text,                                      -- iff content_bytes <= 65536
  body_blob_key text,                             -- ledger/{hex(content_hash)} iff content_bytes > 65536
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, ledger_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((content_bytes <= 65536) = (body IS NOT NULL)),
  CHECK ((content_bytes >  65536) = (body_blob_key IS NOT NULL))
);
CREATE INDEX ingest_ledger_doc_idx ON ingest_ledger (namespace_id, document_id, received_at DESC);
CREATE INDEX ingest_ledger_op_idx  ON ingest_ledger (namespace_id, operation_id);
CREATE TRIGGER ingest_ledger_append_only BEFORE UPDATE OR DELETE ON ingest_ledger
  FOR EACH ROW EXECUTE FUNCTION engram_forbid_ledger_mutation();   -- 42501 unless DELETE by engram_admin/engram_move
```

The 64 KiB inline bound keeps the common case (chat turns, notes, pages of a few KB) to one
Postgres write on the ack path (D16) and keeps the ledger `COPY` of a move self-contained;
oversized bodies are written to the content-addressed blob `ledger/{sha256}` *before* the ledger
transaction (N7), so a crash between the two leaves an orphan blob (swept by the tombstone
reconciler) and never a dangling row. `document_versions` references the ledger row, so a purge
must delete versions before ledger rows (verified: the FK refuses the reverse order even for
the roles the trigger admits). Rejected: body always in blob (a blob round trip on
every ack); body always inline (1 MiB TOAST rows in the hottest write path).

```sql
CREATE TABLE documents (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  document_id text NOT NULL CHECK (octet_length(document_id) BETWEEN 1 AND 256),
  current_version integer NOT NULL DEFAULT 0,
  state document_state NOT NULL DEFAULT 'active', -- active|deleting|deleted
  item_timestamp timestamptz, context text NOT NULL DEFAULT '',
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)), metadata jsonb NOT NULL DEFAULT '{}',
  document_hash bytea, summary_blob_key text, summary_hash bytea,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz,
  PRIMARY KEY (namespace_id, document_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((state = 'active') = (deleted_at IS NULL))
) WITH (fillfactor = 80);

CREATE TABLE document_versions (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL,
  version integer NOT NULL CHECK (version >= 1),
  content_hash bytea NOT NULL, status version_status NOT NULL DEFAULT 'ingesting',  -- ingesting|active|superseded|deleted
  update_mode update_mode NOT NULL, operation_id uuid NOT NULL, ledger_id uuid NOT NULL,
  chunk_count integer, chunks_done integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(), activated_at timestamptz, finished_at timestamptz,
  PRIMARY KEY (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  FOREIGN KEY (namespace_id, ledger_id)   REFERENCES ingest_ledger (namespace_id, ledger_id)
);
CREATE UNIQUE INDEX document_versions_active_uq ON document_versions (namespace_id, document_id) WHERE status = 'active';

CREATE TABLE document_version_chunks (            -- membership: which chunk hashes make up version v
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL, version integer NOT NULL,
  content_hash bytea NOT NULL, chunk_id uuid NOT NULL,
  PRIMARY KEY (namespace_id, document_id, version, content_hash),
  FOREIGN KEY (namespace_id, document_id, version) REFERENCES document_versions (namespace_id, document_id, version),
  FOREIGN KEY (namespace_id, chunk_id) REFERENCES chunks (namespace_id, chunk_id)
);
```

Membership is a table rather than `first_version/last_version` columns on `chunks` because
`APPEND` and `REPLACE` histories are not intervals: a chunk can be present in v1 and v3 but
not v2, and `FinalizeVersion` computes "chunks not in the new set" as a set difference against
this table (section 5).

**The version row is the lock that `CommitChunk` relies on (N40).** `CommitChunk(v)` runs
`SELECT status FROM document_versions WHERE (namespace_id, document_id, version = v) FOR SHARE`
and proceeds only when `status = 'ingesting'`; `FinalizeVersion` and the delete cascade
`UPDATE` that row to `superseded`/`active`/`deleted`, so they wait for every in-flight commit
of `v` and a commit that starts afterwards sees the terminal status and stops. The status
enum already expresses this; no extra column is needed. `FinalizeVersion(v)` marks `v`
`superseded` without retiring anything when a newer version row already exists (the newest
version alone computes the retire set), which is why the primary key `(namespace_id,
document_id, version)` is also the "has a newer version started?" probe. TLC
`DocLifecycle_NoCommitCheck`/`_NoFinalizeCheck` (§7) show the resurrection and the wrong
retire set without these two checks. Because every commit of `v` holds `FOR SHARE` on the
version row, no commit may `UPDATE` that row: `chunks_done` is written once per wave by the
workflow (N69), never `+ 1` per chunk, or two concurrent commits deadlock on it. The retire
set that `FinalizeVersion` computes has two parts (3.8): the chunks not in the new set, and —
on kept chunks — every live fact whose `extraction_key` differs from the chunk's current key,
so a prompt or model bump never leaves two live fact sets on one chunk (N58, review F-12).

#### 3.3.4 Chunks and facts (partitioned)

```sql
CREATE TABLE chunks (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, chunk_id uuid NOT NULL, document_id text NOT NULL,
  content_hash   bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(text), header excluded (N6)
  header_hash    bytea NOT NULL CHECK (octet_length(header_hash) = 32),    -- decides re-embedding without re-extraction
  extraction_key bytea NOT NULL CHECK (octet_length(extraction_key) = 32), -- sha256(content_hash||prompt_version||model||schema_version) = xcache key
  ordinal integer NOT NULL, heading_path text NOT NULL DEFAULT '', header text NOT NULL DEFAULT '',
  text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 16384),      -- <= 4,000 chars (D8)
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  mentioned_at timestamptz NOT NULL,                                       -- = item timestamp (D9)
  embedding halfvec(768) NOT NULL, embedding_model text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz, purge_after timestamptz,
  live boolean GENERATED ALWAYS AS (retired_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, chunk_id),
  UNIQUE (namespace_id, document_id, content_hash),                        -- chunk identity (D8)
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  CHECK ((retired_at IS NULL) = (purge_after IS NULL))
) PARTITION BY HASH (namespace_id);

CREATE TABLE facts (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, memory_id uuid NOT NULL,
  document_id text NOT NULL, chunk_id uuid NOT NULL, ordinal smallint NOT NULL,
  content_hash   bytea NOT NULL CHECK (octet_length(content_hash) = 32),   -- sha256(normalised text)
  extraction_key bytea NOT NULL CHECK (octet_length(extraction_key) = 32),
  text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
  fact_type fact_type NOT NULL,                                            -- world|experience
  fact_type_code smallint GENERATED ALWAYS AS (CASE fact_type WHEN 'world' THEN 1 WHEN 'experience' THEN 2 END) STORED,
  w5 jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(w5) = 'object'),      -- {who:[...], what, when, where, why}
  occurred_start timestamptz, occurred_end timestamptz,
  mentioned_at timestamptz NOT NULL,                                       -- = item timestamp, SERVER-SET: the only as_of key (D9, review F-2)
  said_at timestamptz,                                                     -- the model's "when the source said it": display and ranking only
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),
  tag_count smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  metadata jsonb NOT NULL DEFAULT '{}',
  embedding halfvec(768) NOT NULL,
  extraction_version integer NOT NULL, prompt_version text NOT NULL, model text NOT NULL, embedding_model text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  consolidated_at timestamptz, consolidation_note text,
  retired_at timestamptz, purge_after timestamptz, invalidated_at timestamptz, invalidation_reason text,
  live boolean GENERATED ALWAYS AS (retired_at IS NULL AND invalidated_at IS NULL) STORED,
  PRIMARY KEY (namespace_id, memory_id),
  UNIQUE (namespace_id, chunk_id, content_hash),                           -- CommitChunk idempotency; no epoch (D11)
  FOREIGN KEY (namespace_id, chunk_id)    REFERENCES chunks (namespace_id, chunk_id),
  FOREIGN KEY (namespace_id, document_id) REFERENCES documents (namespace_id, document_id),
  CHECK (occurred_start IS NULL OR occurred_end IS NULL OR occurred_end >= occurred_start),
  CHECK ((retired_at IS NULL) = (purge_after IS NULL))
) PARTITION BY HASH (namespace_id);
```

Decisions inside `facts`:

| Choice | Decision | Rationale | Rejected |
|---|---|---|---|
| who/what/when/where/why | one `w5 jsonb` object | payload for prompts and display, never filtered (entities are filtered through `entity_mentions`); one TOASTable column keeps the hot row narrow | five columns (`who` is multi-valued; five nullable text columns widen every row) |
| vector storage | `halfvec(768)` with pgvector's default `EXTERNAL` storage (TOASTed out of line, uncompressed) | the heap row stays ~0.8 KB so metadata filters and row fetches stay cache-dense; HNSW keeps its own copy of the vector so search never touches TOAST except for the final top-K fetch | `SET STORAGE PLAIN` (2.3 KB rows, 3 per page, same total bytes, worse cache density for everything but vector reads) |
| retire/invalidate | two nullable timestamps plus generated `live` | D8 distinguishes curation from retirement; `live` is the single predicate every arm uses and is pushdown-friendly | a status enum (an update per state change, no "restore keeps retired_at" semantics) |
| per-fact identity | `UNIQUE (namespace_id, chunk_id, content_hash)` | `CommitChunk` re-execution is `ON CONFLICT DO NOTHING`; the key has no epoch (D11) | `(chunk_id, ordinal)` (a re-extraction with the same hash but different order would duplicate) |
| `fact_type_code` | generated `smallint` twin of the enum | pg_search cannot index an enum but pushes `smallint = ANY(...)` into the scan (3.8) | a `text` column instead of the enum (loses the closed set) |
| `mentioned_at` vs `said_at` | `mentioned_at` = the item's `timestamp`, written by the server from the ledger row, never by the extractor or a per-item override; `said_at` nullable, model-supplied | the `as_of` cut-off must be the time the *system learned* the content, or leak-freedom depends on an LLM's dating judgement (D9 as amended, review F-2); chunks carry the same `mentioned_at`, so every arm agrees on "as of T" | one model-adjustable `mentioned_at` (a day-30 session quoting day-3 material surfaced at `as_of = day 5`) |

The sixteen partitions are created once per table by a loop, shown here for `facts`
(the loop in the SQL file also sets per-partition storage parameters, 3.7):

```sql
CREATE TABLE facts_p00 PARTITION OF facts FOR VALUES WITH (MODULUS 16, REMAINDER 0)
  WITH (fillfactor = 90, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_insert_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.01,
        autovacuum_vacuum_cost_delay = 2, autovacuum_vacuum_cost_limit = 1000);
-- ... facts_p01 ... facts_p15 with REMAINDER 1 ... 15; likewise chunks_p*, fact_links_p*, entity_mentions_p*
```

Indexes (created on the parent, propagated to every partition):

```sql
-- facts
CREATE INDEX facts_doc_idx            ON facts (namespace_id, document_id);
CREATE INDEX facts_mentioned_idx      ON facts (namespace_id, mentioned_at DESC) WHERE live;
CREATE INDEX facts_occurred_idx       ON facts (namespace_id, occurred_start) WHERE live AND occurred_start IS NOT NULL;   -- temporal arm: two-sided probe (N68)
CREATE INDEX facts_occurred_gist      ON facts USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
                                      WHERE live AND occurred_start IS NOT NULL;            -- explicit window filters only; btree_gist for the uuid
CREATE INDEX facts_tags_gin           ON facts USING gin (namespace_id, tags) WHERE live;   -- btree_gin for the uuid
CREATE INDEX facts_unconsolidated_idx ON facts (namespace_id, created_at) WHERE live AND consolidated_at IS NULL;
CREATE INDEX facts_purge_idx          ON facts (namespace_id, purge_after) WHERE purge_after IS NOT NULL;
CREATE INDEX facts_embedding_hnsw     ON facts USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
CREATE INDEX facts_bm25               ON facts
  USING bm25 (memory_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, fact_type_code, tag_count, (tags::pdb.literal))
  WITH (key_field = 'memory_id');
-- chunks: chunks_doc_idx (namespace_id, document_id, ordinal), chunks_mentioned_idx, chunks_purge_idx,
--         chunks_embedding_hnsw, chunks_bm25 (chunk_id, (text::pdb.unicode_words), namespace_id, live, mentioned_at, tag_count, (tags::pdb.literal))
```

pg_search rules applied here (quoted from the 0.25 documentation, verified in the extension's
own partitioned-table fixtures): the `key_field` "must have a UNIQUE constraint, usually the
PRIMARY KEY", must be the first column of the list and untokenized; on a partitioned table the
index is created on the parent with the key column being the non-partition part of the
composite primary key; `uuid`, `timestamptz`, `bool` and `smallint` columns are "columnar" by
default, which enables filter pushdown for `=`, range, `IN`/`ANY` and `IS NULL`; `text[]`
columns are indexed one term per element (`tags::pdb.literal` keeps the exact tag); only one
such index per table; `USING bm25` "remains supported as a backwards-compatible alias for
`USING paradedb`". Every filter the lexical arm applies is therefore in the index, which is
the condition for Top-K pushdown (3.8).

**Semantic-arm plan by namespace size (N55, review F-8).** A partition holds ≈ 625 k facts of
≈ 10 namespaces, so for a 1 k-fact namespace the shared HNSW must visit ≈ 94 k tuples to find
150 that pass the `namespace_id` filter — above `max_scan_tuples`, with a heap fetch per
visited tuple. The schema therefore serves two plans:

- **small namespace** (`namespace_stats.large = false`, fewer than 20 k live facts): an exact
  scan. The arm runs `SET LOCAL enable_indexscan = off` for the statement, so the planner
  takes a bitmap scan on `facts_mentioned_idx` (namespace-leading, `WHERE live`) and sorts the
  ≤ 20 k candidates by `embedding <=> $q` — ≤ 30 MB of `halfvec`, 5–15 ms, exact recall;
- **large namespace** (`large = true`): a per-namespace partial HNSW on the partition that
  holds it, created by `engramctl index` as the owner `engram_migrate` (only the owner may
  create an index on PG 16) when the stats sweeper flips the flag, and dropped at purge or move
  cleanup:

```sql
CREATE INDEX CONCURRENTLY facts_hnsw_ns_<12 hex of namespace_id> ON facts_p07
  USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128)
  WHERE namespace_id = '<namespace_id>';
```

  The arm binds the namespace id as a literal in that statement (a custom plan), because a
  partial-index predicate is matched only against a provable constant, not against `$1` or the
  RLS `current_setting()` expression. The shared partition index remains as the fallback while
  the partial one is building. `hnsw.ef_search` is set to at least the arm cap (150 at MID,
  400 at HIGH); heap fetches per recall × QPS enter the IOPS sizing in 3.7. Rejected: one HNSW
  per namespace for everyone (150 to 1,000 indexes per shard and a build per tiny namespace);
  raising `max_scan_tuples` (20 k random heap reads per arm is the cost being avoided).

#### 3.3.5 Links, entities, mentions

```sql
CREATE TABLE fact_links (                         -- one row per edge; symmetric types in canonical order
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  src_memory_id uuid NOT NULL, dst_memory_id uuid NOT NULL,
  link_type link_type NOT NULL,                   -- entity|temporal|semantic|causal
  weight real NOT NULL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),
  PRIMARY KEY (namespace_id, src_memory_id, dst_memory_id, link_type),
  FOREIGN KEY (namespace_id, src_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, dst_memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  CHECK (src_memory_id <> dst_memory_id),
  CHECK (link_type = 'causal' OR src_memory_id < dst_memory_id)
) PARTITION BY HASH (namespace_id);
CREATE INDEX fact_links_reverse_idx ON fact_links (namespace_id, dst_memory_id, src_memory_id);
```

Entity, temporal and semantic edges are undirected and stored once (`src < dst`, enforced);
causal edges are directed (`src` = cause). Expansion scans both the PK and the reverse index
(3.8). Rationale: `fact_links` is the largest table on the shard (3.7) and storing symmetric
edges twice would double it for one index scan saved. Caps (temporal ≤ 20, semantic ≤ 10,
entity ≤ 10 per shared entity) are enforced by the linker in section 5: a counting trigger
would add a query to every one of 300 M inserts, and the caps are soft quality knobs, not
integrity constraints. No `created_at`: links are derived and rebuildable; the facts carry the
time. Links are **not** deleted by the synchronous delete cascade (N61, review F-19): a
100 k-fact document would mean ≈ 3 M link deletes inside the ack transaction, past the 30 s
writer `statement_timeout`. `NoOrphanLinks` is a *traversal* property instead — the graph arm
joins `facts` and requires `live` on every endpoint it steps onto (3.8), so a link to a retired
or invalidated fact is inert the moment the fact is retired — and `PurgeDocument` removes the
rows through the `ON DELETE CASCADE` foreign keys when it deletes the facts.

```sql
CREATE TABLE entities (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, entity_id uuid NOT NULL,
  canonical_name text NOT NULL, canonical_norm text NOT NULL,           -- lower/trim/unaccent computed in Go
  entity_type text NOT NULL DEFAULT 'other' CHECK (entity_type IN ('person','organization','location','product','event','concept','other')),
  mention_count integer NOT NULL DEFAULT 0, first_seen_at timestamptz NOT NULL DEFAULT now(), last_seen_at timestamptz NOT NULL DEFAULT now(),
  merged_into uuid, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, entity_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  FOREIGN KEY (namespace_id, merged_into) REFERENCES entities (namespace_id, entity_id)
);
CREATE UNIQUE INDEX entities_canonical_uq ON entities (namespace_id, canonical_norm) WHERE merged_into IS NULL;
CREATE INDEX entities_trgm_idx ON entities USING gin (namespace_id, canonical_norm gin_trgm_ops);

CREATE TABLE entity_aliases (namespace_id, tenant_id, entity_id, alias, alias_norm, source, created_at,
  PRIMARY KEY (namespace_id, alias_norm), FOREIGN KEY (namespace_id, entity_id) REFERENCES entities ... ON DELETE CASCADE);

CREATE TABLE entity_mentions (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, memory_id uuid NOT NULL, entity_id uuid NOT NULL,
  role text NOT NULL DEFAULT 'other' CHECK (role IN ('who','what','where','when','why','other')),
  confidence real NOT NULL DEFAULT 1.0,
  PRIMARY KEY (namespace_id, memory_id, entity_id),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE,
  FOREIGN KEY (namespace_id, entity_id) REFERENCES entities (namespace_id, entity_id)
) PARTITION BY HASH (namespace_id);
CREATE INDEX entity_mentions_entity_idx ON entity_mentions (namespace_id, entity_id, memory_id);
```

The trigram index is a two-column GIN (`btree_gin` supplies the uuid operator class) so that
even the fuzzy index leads with `namespace_id`; `canonical_norm % $q AND namespace_id = $ns`
is one index scan. A merged entity keeps its row with `merged_into` set, so aliases resolve
to the survivor without rewriting history; `EntitiesMerged` re-points `entity_mentions` rows.

#### 3.3.6 Observations, consolidation, pages

```sql
CREATE TABLE observations (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL,
  current_version integer NOT NULL DEFAULT 0, proof_count integer NOT NULL DEFAULT 0,   -- proof_count = sources with retired_at IS NULL (N42)
  stale_write boolean NOT NULL DEFAULT false,     -- evidence changed under it (replace-retire, restore); still visible (N42)
  stale_delete boolean NOT NULL DEFAULT false,    -- "needs a rewrite": lost a live source, or its CURRENT version is derived_from_deleted; hides the current version until the rewrite lands (N41 as amended)
  stale_since timestamptz,
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),       -- consolidation scope tags
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz,
  PRIMARY KEY (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK ((stale_write OR stale_delete) = (stale_since IS NOT NULL))
);
CREATE INDEX observations_stale_idx ON observations (namespace_id, stale_delete DESC, stale_since)
  WHERE (stale_write OR stale_delete) AND retired_at IS NULL;              -- hidden ones first (section 5.2)

CREATE TABLE observation_versions (               -- D9: versioned beliefs; recalled through semantic + lexical arms (D7, D10)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  ov_id uuid NOT NULL,                            -- surrogate: pg_search key_field must be a single UNIQUE column
  observation_id uuid NOT NULL, version integer NOT NULL CHECK (version >= 1),
  text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  embedding halfvec(768) NOT NULL, embedding_model text NOT NULL,
  effective_at timestamptz NOT NULL,              -- D9: max(mentioned_at over observation_inputs, effective_at over candidates shown, previous version) (section 5)
  superseded_at timestamptz,                      -- min(effective_at) over later versions; NULL for the newest
  source_count integer NOT NULL CHECK (source_count >= 1),
  stale_delete boolean NOT NULL DEFAULT false,    -- mirror of observations.stale_delete for the CURRENT version (N41); inert once superseded_at is set
  derived_from_deleted boolean NOT NULL DEFAULT false,   -- this version's inputs named a now-deleted fact: hidden at every as_of, forever (review F-1)
  tags text[] NOT NULL DEFAULT '{}', tag_count smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  prompt_version text NOT NULL, model text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  retired_at timestamptz,
  live boolean GENERATED ALWAYS AS (retired_at IS NULL AND NOT derived_from_deleted
                                    AND NOT (stale_delete AND superseded_at IS NULL)) STORED,   -- the recall predicate (N41 as amended)
  PRIMARY KEY (namespace_id, observation_id, version),
  UNIQUE (ov_id),
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  CHECK (superseded_at IS NULL OR superseded_at >= effective_at)
);
CREATE INDEX observation_versions_effective_idx    ON observation_versions (namespace_id, effective_at) WHERE live;
CREATE INDEX observation_versions_embedding_hnsw   ON observation_versions USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128);
CREATE INDEX observation_versions_bm25             ON observation_versions
  USING bm25 (ov_id, (text::pdb.unicode_words), namespace_id, observation_id, live, effective_at, superseded_at, tag_count, (tags::pdb.literal))
  WITH (key_field = 'ov_id');

CREATE TABLE observation_sources (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL, memory_id uuid NOT NULL,
  quote text NOT NULL DEFAULT '' CHECK (octet_length(quote) <= 2048), added_version integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, memory_id),
  FOREIGN KEY (namespace_id, observation_id) REFERENCES observations (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, memory_id)      REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
);
CREATE INDEX observation_sources_memory_idx ON observation_sources (namespace_id, memory_id);

CREATE TRIGGER observation_sources_orphans AFTER DELETE ON observation_sources
  REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT
  EXECUTE FUNCTION engram_observation_sources_after_delete();

CREATE TABLE observation_inputs (                 -- N41: every fact shown to the prompt that produced version v (cited sources are a subset)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL,
  version integer NOT NULL CHECK (version >= 1), fact_id uuid NOT NULL,     -- = facts.memory_id
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, version, fact_id),
  FOREIGN KEY (namespace_id, observation_id, version) REFERENCES observation_versions (namespace_id, observation_id, version),
  FOREIGN KEY (namespace_id, fact_id) REFERENCES facts (namespace_id, memory_id) ON DELETE CASCADE
);
CREATE INDEX observation_inputs_fact_idx ON observation_inputs (namespace_id, fact_id);
CREATE TRIGGER observation_inputs_lost AFTER DELETE ON observation_inputs
  REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT
  EXECUTE FUNCTION engram_observation_inputs_after_delete();           -- derived_from_deleted per version (F-1)
```

The two evidence triggers are statement-level with a transition table: one statement that
deletes 10,000 rows runs a handful of `UPDATE`s, not 10,000 row triggers. Both define "lost
evidence" the same way — *the row is gone and the fact it named is no longer `live`* (retired,
invalidated or physically purged). The delete cascade retires the facts **before** it deletes
the source and input rows (3.8; the order is load-bearing) and the purge removes the fact row
itself, so both paths qualify; the consolidation apply path, which inserts the new
`observation_sources`/`observation_inputs` rows first and then deletes the stale ones (N57,
review F-11), removes rows whose facts are live and therefore trips neither trigger — an
update whose new source set is disjoint from the old one no longer retires the belief it is
rewriting, whatever order §5 lists the two statements in.

- **Sources** (`observation_sources`): zero sources left → retire the observation and every
  version, unconditionally (observations never outlive their sources, D12). Otherwise a lost
  source marks the observation `stale_delete` ("needs a rewrite") and mirrors the flag onto
  the *current* version, which hides that version until the rewrite lands; superseded
  versions keep their `as_of` range.
- **Inputs** (`observation_inputs`, the facts *rendered* to the prompt for one version: the 8
  batch facts plus at most 5 quoted sources per candidate shown, N47 — ≤ 58 rows per version,
  cited sources a subset): every version whose inputs named the victim is marked
  `derived_from_deleted`, **per version and permanently** (review F-1). Reconsolidation never
  clears it; it writes a new version, which supersedes the old one. If the observation's
  current version is among the flagged ones the observation is also marked `stale_delete`.

The recall predicate for a version is therefore `live` = `retired_at IS NULL AND NOT
derived_from_deleted AND NOT (stale_delete AND superseded_at IS NULL)` plus the `as_of` range.
Replaying F-1's interleaving on the schema: `o` has v1 (inputs {f1}), v2 ({f1, f5}), v3 ({f1,
f5, f9}); deleting f5's document flags v2 and v3 and marks `o` `stale_delete`; the rewrite
inserts v4, sets `superseded_at` on v3 and clears `o.stale_delete`; `Recall(as_of = t7)` now
finds v1 (live in its range, written without f5) and never v2 — the previous design cleared
one observation-level flag and resurrected v2. Blast radius (review F-9): with ≤ 58 inputs per
version a fact is named by ≈ 4 versions on average rather than ≈ 16, hiding is per version,
and a delete hides only current versions that saw the victim, so a routine 100-fact session
delete hides tens of observations until the hidden-first reconsolidation round, not thousands
for hours. `stale_write` is never set here: a `REPLACE`-retired fact keeps its rows during the
purge grace and `FinalizeVersion` marks the citing observations `stale_write`, which stays
visible (N42); the purge then deletes the rows and cascades exactly like an explicit delete.
`Invalidate` sets `stale_delete` on the observation and the mirror on its current version
without deleting rows, and `Restore` clears both (N48) — the only writer that ever clears a
version flag, and never `derived_from_deleted`. `proof_count` counts only sources whose fact
has `retired_at IS NULL`. The triggers do not write outbox events (a plpgsql trigger cannot
encode the protobuf payload); the cascade reads the affected observations back and emits
`ObservationRetired` for the ones whose `retired_at` is now set and `ObservationsMarkedStale`
for the hidden ones (3.8). Verified on the schema (the scenario above, the N57 order, the
zero-sources retire and the N58 retire statement were executed against the DDL).

`superseded_at` is the only denormalisation that D9 needs: "the latest version with
`effective_at <= T`" equals "`effective_at <= T` and no later version has `effective_at <= T`",
and the latter is a plain filter once `superseded_at = min(effective_at of later versions)` is
stored (maintained by one `UPDATE ... WHERE version < $new` when a version is inserted). Both
HNSW and BM25 scans over `observation_versions` can then apply the `as_of` rule inside the
scan. `tags`, `retired_at` and the `stale_delete` mirror are copied from `observations` for
the same reason (single-table arms): the recall visibility predicate for an observation
version is `live` (above) plus the `as_of` filter, and both are columns of the BM25 and HNSW
scans. Setting `superseded_at` on the previous version when a new one is inserted is also what
releases that version from the current-version hiding, with no flag to clear. `effective_at`
follows the amended D9 rule:
`effective_at(v) = max(max(mentioned_at) over observation_inputs(v), max(effective_at) over
the candidate observation versions shown to the prompt, effective_at(v − 1))` — inputs, not
only cited sources (TLC `AsOf_CitedOnly` shows the leak otherwise), and monotone across
versions.

```sql
CREATE TABLE consolidation_batches (              -- D12 exactly-once
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  batch_key bytea NOT NULL CHECK (octet_length(batch_key) = 32),          -- sha256(sorted memory ids || prompt_version || model)
  round_id uuid NOT NULL, memory_ids uuid[] NOT NULL CHECK (cardinality(memory_ids) BETWEEN 1 AND 8),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','proposed','applied','discarded','bisected','failed')),
  attempts integer NOT NULL DEFAULT 0, model text NOT NULL, prompt_version text NOT NULL,
  result_blob_key text, error text, created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
  PRIMARY KEY (namespace_id, batch_key), FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);
CREATE TABLE consolidation_proposals (            -- N43: the op list persisted write-once BEFORE apply; op_key is computed over THIS list
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  batch_key bytea NOT NULL CHECK (octet_length(batch_key) = 32),
  ops jsonb NOT NULL CHECK (jsonb_typeof(ops) = 'array'),                -- [{kind, observation_id (pre-minted for creates), text, source_fact_ids[], quotes[], reason}] in op_index order
  op_count integer NOT NULL CHECK (op_count BETWEEN 0 AND 16),
  input_fact_ids uuid[] NOT NULL CHECK (cardinality(input_fact_ids) >= 1),   -- batch facts ∪ the quoted sources rendered per candidate (<= 5 each, N47) = observation_inputs of every version it creates
  candidate_versions jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(candidate_versions) = 'array'),   -- [{observation_id, version}] shown (effective_at inputs, D9)
  prompt_version text NOT NULL, model text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, batch_key), FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
);
CREATE TRIGGER consolidation_proposals_write_once BEFORE UPDATE ON consolidation_proposals
  FOR EACH ROW EXECUTE FUNCTION engram_forbid_proposal_update();         -- 42501 for every role
CREATE TABLE consolidation_applied (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  op_key bytea NOT NULL CHECK (octet_length(op_key) = 32),                -- sha256(batch_key || op_index) over the stored proposal
  batch_key bytea NOT NULL, op_index integer NOT NULL, op_kind text NOT NULL CHECK (op_kind IN ('create','update','delete')),
  observation_id uuid NOT NULL, version integer, applied_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, op_key), FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key),
  FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_proposals (namespace_id, batch_key)   -- RESTRICT: an applied proposal can never be discarded
);
CREATE TABLE batch_jobs (namespace_id, tenant_id, batch_key bytea(32) /* sha256(sorted xcache keys) */, kind, gateway_job_id,
  state, item_count, operation_id, error, created_at, updated_at, finished_at, PRIMARY KEY (namespace_id, batch_key));

CREATE TABLE pages (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, page_id uuid NOT NULL,
  name text NOT NULL, source_query text NOT NULL,
  tag_filter jsonb NOT NULL DEFAULT '{}',         -- {"mode": "ALL", "tags": [...]} evaluated by engram_tag_match()
  refresh_policy jsonb NOT NULL DEFAULT '{}', current_version integer NOT NULL DEFAULT 0,
  stale_write boolean NOT NULL DEFAULT false, stale_delete boolean NOT NULL DEFAULT false, stale_seq bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz,
  PRIMARY KEY (namespace_id, page_id), FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);
CREATE UNIQUE INDEX pages_name_uq ON pages (namespace_id, name) WHERE retired_at IS NULL;
CREATE INDEX pages_stale_idx ON pages (namespace_id, updated_at) WHERE (stale_write OR stale_delete) AND retired_at IS NULL;
CREATE TABLE page_versions (namespace_id, tenant_id, page_id, version, markdown_blob_key, effective_at, superseded_at,
  evidence_hash bytea(32), created_at, PRIMARY KEY (namespace_id, page_id, version), FOREIGN KEY (namespace_id, page_id) REFERENCES pages ...);
CREATE TABLE page_sources (namespace_id, tenant_id, page_id, source_kind CHECK IN ('fact','observation'), source_id uuid, version_added,
  PRIMARY KEY (namespace_id, page_id, source_kind, source_id), FOREIGN KEY (namespace_id, page_id) REFERENCES pages ... ON DELETE CASCADE);
CREATE INDEX page_sources_source_idx ON page_sources (namespace_id, source_id);
```

`page_sources.source_id` is polymorphic (`fact_id | observation_id`, D12) and therefore has no
foreign key; the delete cascade and the observation retire path remove the rows explicitly and
mark the page `stale_delete` with `stale_seq + 1`. `stale_seq` is the monotone counter a refresh
captures and compares before clearing the flags (section 5.3), so a mark that lands mid-refresh
is never lost.

`consolidation_proposals` exists because idempotency keys over a *volatile* LLM answer are
decorative (N43; TLC `Consolidation_VolatileProposal` and `_NonAtomicKey`, §7): a retried
`ConsolidateBatch` can return a different op list, so `op_key = sha256(batch_key ‖ op_index)`
is only meaningful over a list that was stored first. The row is inserted `ON CONFLICT DO
NOTHING` after validation and dedup (a retry that finds a row keeps the stored list and
discards its own), `ApplyBatch` reads the ops from it, and every op's effect and its
`consolidation_applied` row commit in one transaction. The `UPDATE` trigger makes the list
immutable; the only `DELETE` is the §5.2 discard path — the apply transaction found an input
that is no longer live (N41) before writing any op — and the `RESTRICT` foreign key from
`consolidation_applied` refuses a delete once any op has been applied, so an `op_key` can never
point at a list that changed under it. `input_fact_ids` and `candidate_versions` are what the
prompt saw; they become the `observation_inputs` rows and the `effective_at` inputs of every
version the batch creates (D9).

#### 3.3.7 Operations, idempotency, metering, tombstones, exports

```sql
CREATE TABLE operations (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, operation_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind IN ('retain','delete_document','delete_namespace','consolidate','page_refresh','export','purge','move')),   -- no 'reflect': Reflect is synchronous (review F-27)
  state operation_state NOT NULL DEFAULT 'PENDING',   -- PENDING|RUNNING|DEFERRED|SUCCEEDED|FAILED|CANCELLED
  request_id text, target_id text,
  workflow_id text NOT NULL,                      -- ns/{namespace_id}/op/{operation_id} (D11)
  task_queue text NOT NULL,                       -- shard-{id} at submission; restarted on the target after a move
  submitted_epoch bigint NOT NULL,                -- audit only, never an identity (D11)
  workflow_started_at timestamptz,                -- NULL = no workflow yet (op-sweeper, N3)
  progress jsonb NOT NULL DEFAULT '{}',           -- units_total, units_done, facts_created, consolidation_lag_s
  deferred_until timestamptz, deferred_reason text,
  error jsonb,                                    -- google.rpc.Status as JSON
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
  PRIMARY KEY (namespace_id, operation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (state <> 'DEFERRED' OR deferred_until IS NOT NULL),
  CHECK ((state IN ('SUCCEEDED','FAILED','CANCELLED')) = (finished_at IS NOT NULL))
) WITH (fillfactor = 70);
CREATE INDEX operations_created_idx  ON operations (namespace_id, created_at DESC);                       -- ListOperations
CREATE INDEX operations_active_idx   ON operations (namespace_id, state, deferred_until) WHERE state IN ('PENDING','RUNNING','DEFERRED');
CREATE INDEX operations_sweeper_idx  ON operations (created_at)     WHERE state = 'PENDING' AND workflow_started_at IS NULL;   -- shard-wide (admin)
CREATE INDEX operations_deferred_idx ON operations (deferred_until) WHERE state = 'DEFERRED';                                  -- shard-wide (admin)

CREATE TABLE idempotency_keys (                   -- (tenant, namespace, method) scope, 24 h (D1)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, method text NOT NULL,
  request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 128),
  request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32), operation_id uuid, response bytea,
  created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (namespace_id, method, request_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);
CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys (expires_at);                   -- shard-wide sweep (admin)

CREATE TABLE token_usage_events (namespace_id, tenant_id, usage_key bytea(32) /* sha256(operation_id||activity||item) */, day, op, model,
  price_version, operation_id, prompt_tokens, completion_tokens, cost_micros, cached, created_at, PRIMARY KEY (namespace_id, usage_key));
CREATE TABLE token_usage (namespace_id, tenant_id, day, op, model, price_version, calls, prompt_tokens, completion_tokens, cost_micros,
  updated_at, PRIMARY KEY (namespace_id, day, op, model, price_version)) WITH (fillfactor = 70);     -- D13, N20
CREATE TABLE quota_counters (namespace_id, tenant_id, quota_key CHECK IN ('llm_tokens','recalls','retains'), window_start, used,
  updated_at, PRIMARY KEY (namespace_id, quota_key, window_start)) WITH (fillfactor = 70);
CREATE TABLE blob_tombstones (namespace_id, tenant_id, tombstone_id, blob_key, reason, operation_id, attempts, last_error, created_at,
  PRIMARY KEY (namespace_id, tombstone_id));
CREATE TABLE export_snapshots (namespace_id, tenant_id, version, state CHECK IN ('building','ready','expired','failed'),
  expired_reason CHECK IN ('ttl','document_delete','namespace_delete'), manifest_key,
  from_seq, to_seq, fact_count, observation_count, chunk_count, page_count, bytes, operation_id, created_at, completed_at, expires_at,
  PRIMARY KEY (namespace_id, version), CHECK ((state = 'expired') = (expired_reason IS NOT NULL)));   -- the delete cascade expires containing snapshots (N59)
CREATE TABLE deletion_log (namespace_id, tenant_id, kind CHECK IN ('document','memory','namespace','tenant'), subject_id text,
  epoch, operation_id, deleted_at, PRIMARY KEY (namespace_id, kind, subject_id, deleted_at));                                 -- N21
```

`token_usage_events` is the per-call record (one row per gateway call, idempotent by
`usage_key`, so a retried activity never double-counts) and `token_usage` the daily aggregate
D13 names; the event table is what "cost per conversation" in section 8 is computed from.
`blob_tombstones` rows are inserted in the same transaction as the logical delete and removed
when the blob is gone, so the tombstone table is also the reconciliation worklist after a crash.
`export_snapshots.state = 'expired'` is reached by TTL and by the delete cascade (N59, review
F-13): every `ready` snapshot taken after the deleted document was created is marked
`expired`/`document_delete` with `expires_at = now()`, `StreamSnapshot` refuses an expired
version, the client re-syncs from a new full snapshot, and the tombstone sweeper removes the
`export/v{n}/` blobs — so an acknowledged delete does not keep serving through an older
snapshot.

#### 3.3.8 RLS and grants

The policy is created by one loop over every table that has a `namespace_id` column, parents
and partitions alike, and a self-check at the end of the file fails the migration if any such
table lacks it, is not `FORCE`d, lacks `tenant_id`, if any non-admin role holds a privilege on
a partition, or if `engram_move_load` is not `BYPASSRLS` or holds anything beyond `INSERT` on
the namespace tables and `SELECT` on `namespace_ownership`/`shard_meta` (self-check 7, N51):

```sql
ALTER TABLE facts ENABLE ROW LEVEL SECURITY;
ALTER TABLE facts FORCE ROW LEVEL SECURITY;
CREATE POLICY ns_isolation ON facts
  USING      (namespace_id = current_setting('engram.namespace_id')::uuid)
  WITH CHECK (namespace_id = current_setting('engram.namespace_id')::uuid);
-- identical on every other namespace-scoped table and partition; plus:
CREATE POLICY relay_read_all ON outbox FOR SELECT TO engram_relay USING (true);
```

`pg_policies.qual` renders this exactly as `(namespace_id =
(current_setting('engram.namespace_id'::text))::uuid)`, which is the string the section 8
static check compares against. Tables without `namespace_id` are exactly `shard_meta`,
`outbox_cursors` and goose's `goose_db_version` (the self-check enumerates them).

### 3.4 Where tenant_id, namespace_id and shard_id appear

`shard_id` appears on no namespace-scoped table. Inside a shard database the shard is implied
by the database: `shard_meta` (one row) is the identity, `namespace_ownership.shard_id` is the
only per-namespace copy and a trigger checks it against `shard_meta`. The catalog is the only
place where `shard_id` is a routing column (`namespaces.shard_id`, `shards.shard_id`,
`namespace_moves.*_shard_id`, `deletion_log.shard_id`).

| Table | PK columns | `namespace_id` | `tenant_id` | `shard_id` | Indexes leading with `namespace_id` | Exceptions |
|---|---|---|---|---|---|---|
| `shard_meta` | (none; singleton) | no | no | yes (identity) | none | shard-level |
| `namespace_ownership` | `namespace_id` | PK | yes (+UNIQUE with ns) | yes (trigger-checked) | PK | the fence row |
| `move_applied` | `namespace_id, seq` | PK | yes | no | PK | |
| `namespace_stats` | `namespace_id` | PK | yes | no | PK | derived counters + `large` flag (N55, N69) |
| `outbox` | `seq` | yes (default from scope) | yes | no | `outbox_ns_seq_idx` | PK is `seq` (relay reads shard-wide in seq order) |
| `outbox_cursors` | `consumer` | no | no | no | none | shard-level; relay-owned |
| `outbox_skipped` | `namespace_id, consumer, seq` | PK | yes | no | PK | |
| `deletion_log` | `namespace_id, kind, subject_id, deleted_at` | PK | yes | no | PK | |
| `ingest_ledger` | `namespace_id, ledger_id` | PK | yes (FK) | no | PK, doc, op, hash | append-only |
| `documents` | `namespace_id, document_id` | PK | yes (FK) | no | PK, updated, deleting | |
| `document_versions` | `namespace_id, document_id, version` | PK | yes | no | PK, active (partial unique), op | |
| `document_version_chunks` | `namespace_id, document_id, version, content_hash` | PK | yes | no | PK, chunk | |
| `chunks` (16 partitions) | `namespace_id, chunk_id` | PK, partition key | yes | no | PK, (doc, hash) unique, doc, mentioned, purge | HNSW and BM25 are single-column access methods: isolation by partition pruning + in-scan `namespace_id` filter + RLS |
| `facts` (16) | `namespace_id, memory_id` | PK, partition key | yes | no | PK, (chunk, hash) unique, doc, mentioned, occurred (btree + GiST), tags (GIN), unconsolidated, purge | HNSW, BM25 as above; plus one partial HNSW `WHERE namespace_id = …` per large namespace (N55) |
| `fact_links` (16) | `namespace_id, src_memory_id, dst_memory_id, link_type` | PK, partition key | yes | no | PK, reverse | |
| `entities` | `namespace_id, entity_id` | PK | yes (FK) | no | PK, canonical (partial unique), trgm (GIN via btree_gin) | |
| `entity_aliases` | `namespace_id, alias_norm` | PK | yes | no | PK, entity | |
| `entity_mentions` (16) | `namespace_id, memory_id, entity_id` | PK, partition key | yes | no | PK, entity | |
| `observations` | `namespace_id, observation_id` | PK | yes (FK) | no | PK, stale, tags (GIN) | |
| `observation_versions` | `namespace_id, observation_id, version` | PK | yes | no | PK, effective | `UNIQUE (ov_id)` exists only because the BM25 `key_field` must be a single unique column; HNSW, BM25 as above |
| `observation_sources` | `namespace_id, observation_id, memory_id` | PK | yes | no | PK, memory | |
| `consolidation_batches` | `namespace_id, batch_key` | PK | yes (FK) | no | PK, round | |
| `consolidation_applied` | `namespace_id, op_key` | PK | yes | no | PK, batch | |
| `batch_jobs` | `namespace_id, batch_key` | PK | yes (FK) | no | PK, open | |
| `pages` | `namespace_id, page_id` | PK | yes (FK) | no | PK, name (partial unique), stale | |
| `page_versions` | `namespace_id, page_id, version` | PK | yes | no | PK | |
| `page_sources` | `namespace_id, page_id, source_kind, source_id` | PK | yes | no | PK, source | |
| `operations` | `namespace_id, operation_id` | PK | yes (FK) | no | PK, created, active | `operations_sweeper_idx (created_at)` and `operations_deferred_idx (deferred_until)` are shard-wide partial indexes for admin schedulers |
| `idempotency_keys` | `namespace_id, method, request_id` | PK | yes (FK) | no | PK | `idempotency_keys_expiry_idx (expires_at)` shard-wide for the 24 h sweep |
| `token_usage_events` | `namespace_id, usage_key` | PK | yes (FK) | no | PK, op, day | |
| `token_usage` | `namespace_id, day, op, model, price_version` | PK | yes (FK) | no | PK | |
| `quota_counters` | `namespace_id, quota_key, window_start` | PK | yes (FK) | no | PK | |
| `blob_tombstones` | `namespace_id, tombstone_id` | PK | yes (FK) | no | PK, created | |
| `export_snapshots` | `namespace_id, version` | PK | yes (FK) | no | PK | |

Catalog: `tenants(tenant_id)`, `shards(shard_id)`, `namespaces(namespace_id)` with indexes on
`(shard_id, state)` and `(tenant_id, state)`, `namespace_moves(move_id)` with the partial
unique index on `namespace_id`, `idempotency_keys(tenant_id, method, request_id)`,
`tenant_usage_daily(tenant_id, day, quota_key)`, `deletion_log(namespace_id, kind, subject_id,
deleted_at)` with `(shard_id, deleted_at)`, `catalog_events(event_id)` with `(namespace_id,
event_id)`.

### 3.5 Outbox design detail

**Columns.** `seq` (PK, from `outbox_seq`), `namespace_id`, `tenant_id`, `epoch` (all three
defaulted from the transaction scope), `event_type` (the `Event` oneof case name, for cheap
filtering and dashboards), `payload` (the serialised `engram.internal.events.v1.Event`, ≤ 16 KiB),
`created_at`.

**Events** (`engram.internal.events.v1.Event`: envelope fields `seq, namespace_id, tenant_id, epoch,
occurred_at, schema_version, event_id, operation_id, shard_id`, then `oneof payload { ... }`; the
oneof case name is what `event_type` stores). Events are thin
(N12): ids, versions, hashes and flags, never text or vectors; a consumer that needs content
reads the rows by id under the recorded namespace scope.

| Event | Emitted by | Payload |
|---|---|---|
| `DocumentVersionStarted` | retain ack | `document_id, version, content_hash, update_mode, chunks_planned` |
| `ChunkCommitted` | `CommitChunk` (re-emitted by `ReembedChunk`) | `document_id, version, chunk_id, content_hash, fact_ids[], entity_ids[], links_written, mentioned_at` |
| `ChunksRetired` | `FinalizeVersion` | `document_id, version, chunk_ids[], fact_ids[], retired_at` |
| `DocumentVersionActivated` | `FinalizeVersion` | `document_id, version, superseded_version, fact_count, chunk_count` |
| `DocumentDeleted` | delete cascade | `document_id, fact_ids[], chunk_ids[], observations_marked_stale[], observations_retired[], pages_marked_stale[], snapshots_expired[], deleted_at` — the `deletion_log` key is derivable (`kind = document`, `subject_id = document_id`, `deleted_at`); links and mentions are purged later (N61) |
| `FactInvalidated`, `FactRestored` | Invalidate/Restore | `fact_id, invalidated_at, reason` / `fact_id` (`deletion_log` kind `memory`) |
| `ObservationUpserted`, `ObservationRetired`, `ObservationsMarkedStale` | consolidation apply; cascade and trigger read-back | `observation_id, version, source_fact_ids[], effective_at, op_key, created` / `observation_id, op_key, zero_sources` / `observation_ids[], cause_fact_id` |
| `EntityUpserted`, `EntitiesMerged` | `CommitChunk`, resolver | `entity_id, canonical_name, type, created` / `survivor_id, merged_ids[]` |
| `PageVersionCreated`, `PageDeleted`, `PagesMarkedStale` | refresh, delete, consolidation/cascade | `page_id, version, markdown_blob_key, effective_at` / `page_id` / `page_ids[], stale_write, stale_delete` |
| `SnapshotCreated` | export | `version, manifest_blob_key, base_version` |
| `RowsPurged` | purge batches (facts, chunks, entities, namespace) | `table, ids[], document_id` |
| `TokenUsageRecorded` | every metered commit | `day, op, model, prompt_tokens, completion_tokens, cost_micros` |
| `NamespacePurged` | namespace purge | `rows_purged, blobs_purged` (`deletion_log` kind `namespace`) |
| `RestoreMarker` | restore-from-backup (§9.3) | `shard_id, restored_to, epoch_bump` — tells every consumer to reconcile from the restore point (N23) |

Field numbers, reserved ranges and the BSR schema reference are in section 4; the oneof case
name is what `event_type` stores, and adding an event is a proto change plus nothing in SQL
(the column is a regex `CHECK`, not an enum, on purpose).

**Cursors.** `outbox_cursors` has one row per consumer (`index`, `kafka`, `deletion-log`,
`move:<ns>`) and one `relay` row. `last_seq` means "every row with `seq <= last_seq` was
delivered to this consumer, except the seqs listed in the relay row's `gaps`". The relay
delivers batches of 500 to each registered consumer in process and advances a consumer's
`last_seq` only after the consumer acknowledged the batch (`index` acknowledges immediately for
the built-in index, after the engine call for an external one; `deletion-log` after the catalog
insert; `kafka` after the producer ack).

**Gap watchlist.** `seq` is assigned at `nextval()` time, so a slow transaction can commit seq
101 after seq 102 was already read. The relay batch `SELECT ... WHERE seq > $hwm ORDER BY seq
LIMIT 500` detects a hole, re-reads it on the next poll (most holes close within milliseconds)
and, if it is still open after 1 s, records `{seq, deadline = first_seen + 60 s}` (2 ×
`statement_timeout`, D6) in the relay row's `gaps`. On every poll the relay `SELECT seq FROM
outbox WHERE seq = ANY($gaps)`; a resolved seq is delivered to every consumer whose `last_seq`
is already past it and removed; a seq past its deadline is declared aborted and removed. The
list is persisted so that relay failover (the advisory lock moves to another worker, N15)
cannot lose a late commit, and it is bounded at 1,000 entries, beyond which the relay stops
advancing `last_seq` (backpressure rather than silent loss). Rejected: an in-memory list (a
failover during a 60 s window would skip a late commit); `pg_current_snapshot()` xmin
watermarks (wraparound-unsafe, D6); a table lock around every writer (throughput).

**Retention.** A daily admin job deletes rows with `seq < min(last_seq) over consumers` and
`created_at < now() - 7 days`, in batches of 10,000 by `seq` range (PK scan, no extra index).
Consequence for exports: `delta-v{n-1}-v{n}` can only be derived from the outbox while the
range is retained, so a snapshot older than 7 days gets a full snapshot instead of a delta
(section 5).

**How a move consumer reads one namespace** (N50, review F-3). The move executor connects as
`engram_move` with the moved namespace in scope and reads, on the index `(namespace_id, seq)`,
`WHERE namespace_id = $ns AND seq > $p0 AND NOT EXISTS (SELECT 1 FROM move_applied m WHERE
m.namespace_id = o.namespace_id AND m.seq = o.seq) ORDER BY seq LIMIT 500` — an anti-join
against the source-side copy of `move_applied` (3.3.1), **never** `seq > $applied`: a writer
that drew `seq` 102 in its last statement may commit after 103 was applied, and under RLS the
mover cannot tell a hole between its namespace's `seq`s from another namespace's `seq`, so a
watermark would skip 102 forever. RLS makes any other namespace invisible to the role even if
the predicate were wrong (section 8 `TestIso_Move_NamespaceOnly`). It registers a `move:<ns>`
cursor on the source so that retention cannot delete rows it has not replayed, applies each
event on the target with `INSERT INTO move_applied ... ON CONFLICT DO NOTHING RETURNING seq`
(exactly-once), then records the same key on the source, and updates
`namespace_ownership.move_applied_seq` for lag reporting only. The replay starts at `p0 =
max(seq)` read *inside* the copy snapshot, which is only safe behind the copy barrier (D5 step
2, N45, as amended by F-5): the mover takes the namespace's **exclusive session-level advisory
lock** (`pg_advisory_lock(engram_ns_lock_key($ns))` on its direct connection, N15), which
queues behind every in-flight writer's shared transaction-level lock and ahead of every new
one, opens a `REPEATABLE READ` transaction whose first statement is `SELECT max(seq) FROM
outbox WHERE namespace_id = $ns` (snapshot and `p0` in one step), releases the lock with
`pg_advisory_unlock`, and streams the `COPY … TO STDOUT` statements in that snapshot. Because
the outbox `INSERT` is the last statement of every writer (A-F1), every `seq ≤ p0` is then
committed or aborted and nothing with a lower `seq` can commit later (TLC
`ShardMove_NoBarrier` loses exactly the writes that straddle an unbarriered snapshot). After
the freeze — the same exclusive lock, transaction-scoped, around the ownership `UPDATE` — no
writer holds a `seq`, `max(seq)` for the namespace is final, and one last anti-join pass is
the complete drain (D5 step 5).

### 3.6 Blob storage layout

Every key is `{shard}/{tenant}/{namespace}/{class}/...` where `{shard}` is the decimal shard
id (`catalog.shards.blob_prefix`), so one prefix listing enumerates a namespace and one prefix
is what a move copies and a purge deletes. Credentials are issued **per shard prefix**
(`{shard}/*`): a cell holds one credential per shard it serves, the move executor holds the
source and the target credential, and `blob.Scoped` (section 2) refuses any key outside the
namespace prefix it was opened for, so a forged key in an export request fails before a request
leaves the process (section 8 `TestIso_Export_CrossTenant`). Rejected: one credential per cell
(a leaked cell credential exposes 32 shards); per-namespace credentials (150 × 100 credentials
to rotate).

| Class | Key | Content | Addressing | Written by | Deleted by |
|---|---|---|---|---|---|
| Ingest raw body | `ledger/{sha256}` | raw item body > 64 KiB (N7) | content-addressed; shared by identical bodies in the namespace | engram-api before the ledger tx | purge, after checking no other live ledger row in the namespace has the hash (`ingest_ledger_hash_idx`) |
| Extraction cache | `xcache/{sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)}.json` | the structured extraction result | content-addressed, immutable, per namespace (D11) | `ExtractChunk` | never on retain/delete (it is a cache); namespace delete / move cleanup |
| Document summary | `docsum/{sha256(document_hash ‖ prompt_version ‖ model)}.json` | ≤ 200-char summary + heading tree | content-addressed, immutable | `SummarizeDocument` | tombstoned on document delete |
| Consolidation result | `consolidate/{hex(batch_key)}.json` | raw LLM ops for the batch (bisect replays read it) | content-addressed by `batch_key` | `ConsolidateBatch` | 30 d sweep |
| Page markdown | `pages/{page_id}/v{n}.md` | one immutable version | versioned, immutable | page refresh | tombstoned on page retire / namespace delete |
| Export snapshot | `export/v{n}/manifest.json`, `facts.jsonl.zst`, `observations.jsonl.zst`, `chunks.jsonl.zst`, `pages/*.md`, `delta-v{n-1}-v{n}.jsonl.zst` | D12 | versioned, immutable | `ExportSnapshot` workflow | `export_snapshots.expires_at` → tombstones |
| Reflect transcript (optional) | `reflect/{operation_id}.jsonl` | tool calls and model turns of one Reflect | per operation, immutable | `Reflect` when `profile.keep_transcripts` | 7 d sweep |
| Temporal staging (N13) | `staging/{operation_id}/{activity}.bin` | embeddings > 512 KiB in flight | per activity | activities | on operation end |

Blobs are immutable: a changed input gets a new key, never an overwrite, which is what makes
the content-addressed classes safe to share within a namespace and makes a move's prefix copy
idempotent (copy-if-absent). Deletion is always asynchronous through `blob_tombstones`
(inserted in the logical delete's transaction) so the ack never waits on the blob store and a
crash leaves a reconcilable worklist, not a leak. The shard's pgBackRest repository lives under
`_backups/shard-{id}/` outside every tenant prefix (N23). Everything in blob storage is derivable
from Postgres except the raw bodies above 64 KiB, which is why those are written first.

### 3.7 Sizing at 10 M facts per shard and the bloat plan

Assumptions: A-3 average fact text 160 bytes, `w5` 200 bytes, 3 tags, `document_id` 24 bytes;
A-4 1 M documents with 1 M live chunks (10 facts per chunk) and 1.5 versions per document;
A-5 0.5 M entities, 2 mentions per fact; A-6 one observation per 20 facts, 1.5 versions each,
one source row per fact with a 150-byte quote; A-7 30 `fact_links` rows per fact (D3).
Tuple header 24 B, line pointer 4 B, index entry header 8 B, B-tree leaf fill 90 %.

| Table | Rows | Heap bytes/row | Heap | Indexes | Total |
|---|---|---|---|---|---|
| `facts` (without vector) | 10 M | ≈ 800 | 9 GB (fillfactor 90) | PK 0.5, (chunk,hash) 0.9, doc 0.6, mentioned 0.4, GiST 0.5, GIN 0.5, unconsolidated 0.3 = 3.7 GB | 12.7 GB |
| `facts.embedding` TOAST | 10 M | 1,544 + chunk header → ≈ 2,000 | 20 GB | | 20 GB |
| `facts` HNSW | 10 M | vector copy 1,544 + 32 neighbours × 6 B + page overhead ≈ 1,800 | | 18 GB | 18 GB |
| `facts` BM25 | 10 M | postings + positions + 6 columnar fields ≈ 400 | | 4 GB | 4 GB |
| `fact_links` | 300 M | 24 + 16 + 12 + 16 + 16 + 4 + 4 = 92 → 96 + 4 | 30 GB | PK (52 B keys → ≈ 71 B/entry) 21 GB; reverse 20 GB | **71 GB** |
| `chunks` + TOAST | 1 M | ≈ 3,600 + vector 2,000 | 5.6 GB | HNSW 1.8, BM25 0.8, B-tree 0.4 | 8.6 GB |
| `entities` + aliases | 0.5 M + 1 M | 250 / 120 | 0.25 GB | trgm GIN 0.3, B-tree 0.2 | 0.75 GB |
| `entity_mentions` | 20 M | 96 + 4 | 2 GB | PK 1.2, entity 1.2 | 4.4 GB |
| `observations` + versions | 0.5 M + 0.75 M | 200 / 700 + vector 2,000 | 2.1 GB | HNSW 1.3, BM25 0.4, B-tree 0.2 | 4 GB |
| `observation_sources` | 10 M | ≈ 250 | 2.5 GB | PK 0.7, memory 0.5 | 3.7 GB |
| `observation_inputs` (N41, N47) | 0.75 M versions × ≤ 58 rendered facts (8 batch facts + ≤ 10 candidates × ≤ 5 quoted sources) ≈ 45 M (uncapped it would be ≈ 210 per version, 160 M rows, ≈ 34 GB — review F-9) | 92 + 4 | 4.5 GB | PK 3.2, fact 2.3 | ≈ 10 GB |
| `ingest_ledger` | 1.5 M | ≈ 3,800 (inline bodies) | 5.7 GB | 0.3 GB | 6 GB |
| `documents`, versions, membership | 1 M + 1.5 M + 1.5 M | 300 / 150 / 100 | 0.6 GB | 0.4 GB | 1 GB |
| `outbox` (7 d) | ≈ 1.2 M steady, 12 M during backfill | ≈ 250 | 0.3 to 3 GB | 0.1 to 0.6 GB | ≤ 3.6 GB |
| `token_usage_events` (30 d), operations, idempotency, stats, tombstones | ≈ 4 M | ≈ 150 | 0.6 GB | 0.3 GB | 1 GB |
| **Total** | | | | | **≈ 168 GB** (≈ 158 GB before `observation_inputs` + ≈ 10 GB for the capped table; **≈ 133 GB at 15 links per fact**) — the figure the register's D3 row carries |

Hot working set (what must stay in the 64 GB of RAM for the p95 budget): facts HNSW 18 GB,
BM25 4 GB, facts B-trees 3.7 GB, chunk and observation HNSW 3 GB, the recently touched half of
the `fact_links` PK ≈ 10 GB, ownership/stats/cursors negligible → ≈ 40 GB, served from
`shared_buffers = 16 GB` plus the OS page cache; the 300 GB volume holds the total with room
for one `REINDEX CONCURRENTLY` of the largest index (3.7 below).

These are the figures the register's D3 row now carries (≈ 71 GB of `fact_links`, ≈ 168 GB
total, ≈ 133 GB at 15 links per fact, ≈ 40 GB hot): `fact_links` cannot be smaller at 300 M
rows of three uuids (71 GB is the floor with the PK and the reverse index; a denser row is
impossible without abandoning UUIDv7 ids, which D1 fixes), and the ledger (6 GB) and the
observation sources are part of the total. The practical lever is the link cap (section 5):
15 links per fact brings the shard to ≈ 133 GB, and "≈ 30/fact" (A-7) is an assumption, not a
measurement.

**Vacuum and bloat plan.**

| Table | Write pattern | Settings (per partition where partitioned) | Why |
|---|---|---|---|
| `facts`, `chunks` | insert-heavy; updates only on retire/un-retire, invalidate, consolidate stamp, tag re-sync | `fillfactor = 90`, `autovacuum_vacuum_scale_factor = 0.02`, `autovacuum_vacuum_insert_scale_factor = 0.05`, `autovacuum_analyze_scale_factor = 0.01`, `autovacuum_vacuum_cost_delay = 2`, `cost_limit = 1000` | the default 20 % threshold would mean 2 M dead tuples per partition before a vacuum; insert-driven vacuums keep the visibility map fresh for index-only scans and for pg_search ("Heap Fetches" in a BM25 plan mean vacuum is overdue) |
| `fact_links`, `entity_mentions` | insert/delete only, never updated | `fillfactor = 100`, same autovacuum factors | no HOT updates to make room for; deletes come in bursts from the cascade |
| `observations`, `documents`, `pages`, `entities`, `batch_jobs` | frequent small updates | `fillfactor = 80` | HOT updates stay on the page |
| `operations`, `token_usage`, `quota_counters`, `namespace_stats` | counter updates | `fillfactor = 70` | same, hotter |
| `outbox_cursors` | a few rows updated every 100 ms | `fillfactor = 50` | one page per row, HOT forever |

Retire + purge pattern: a retire is an in-place update of `retired_at`/`purge_after` (not HOT,
because both columns are in partial-index predicates, so each retire adds index entries to the
two partial indexes and marks the old tuple dead). `PurgeDocument` deletes physical rows in
batches of 1,000 (`WHERE namespace_id = $1 AND purge_after <= now() ... LIMIT 1000` over
`facts_purge_idx`; the FK cascades take `fact_links`, `entity_mentions` and any remaining
sources with them — this is where links and mentions are deleted, N61) with a 50 ms pause
between batches, so a 100 k-fact document purge spreads over ~10 s and never holds a long
transaction. HNSW entries of deleted rows are removed only by `VACUUM` (pgvector repairs the
graph in a second pass), so purge throughput is deliberately capped at 20 k rows/s per shard to
keep autovacuum ahead; `pgstattuple` on each partition's HNSW index runs hourly and `REINDEX
INDEX CONCURRENTLY` is scheduled per partition when dead fraction > 30 % (a partition's HNSW is
≈ 1.1 GB, which rebuilds in minutes inside `maintenance_work_mem = 2 GB` — the §9 value; this
section's earlier 4 GB figure was the inconsistency review F-27 found, and 2 GB is enough for
a 1.1 GB partition index with headroom; an unpartitioned 18 GB index would not fit and would
fall back to pgvector's slower on-disk build). Index DDL (`REINDEX`, the per-namespace partial
HNSW of 3.3.4) runs as the owner `engram_migrate` through `engramctl index`. BM25 segments
merge in the background (`background_layer_sizes`, pg_search default); deleted documents stop
influencing scores after `VACUUM`, which the per-partition thresholds above trigger within
minutes of a large purge. Namespace delete and move cleanup use the same batched path under the
admin role; a whole-namespace purge of a 1 M-fact namespace takes ≈ 10 min and one vacuum cycle.
Transaction-id age is not a concern at these rates (`autovacuum_freeze_max_age` default,
≈ 2 × 10⁸ transactions per shard-year at 50 writes/s).

### 3.8 Query patterns the schema serves

| # | Query | Served by | Role |
|---|---|---|---|
| 1 | scope + ownership fence (every transaction) | shared advisory lock `engram_ns_lock_key(ns)` + `namespace_ownership` PK (plain read) | app |
| 2 | semantic arm over facts (HNSW, as_of, tags, fact types) | exact scan below 20 k live facts; the namespace's partial HNSW when `large`; `facts_embedding_hnsw` with iterative scan as the fallback; partition pruned (N55) | app |
| 3 | lexical arm over facts (BM25 Top-K with pushed-down filters) | `facts_bm25` | app |
| 4 | graph expansion (bounded, from ≤ 20 seeds) | `fact_links` PK + `fact_links_reverse_idx`, `facts` PK for the as_of/live check | app |
| 5 | temporal arm (nearest N occurrences before and after `query_timestamp`, N68) | `facts_occurred_idx` (btree, two-sided probe); `facts_occurred_gist` only for explicit window filters | app |
| 6 | chunk arm (BM25 ∪ HNSW over chunks) | `chunks_bm25`, `chunks_embedding_hnsw` | app |
| 7 | observation arms (semantic + lexical over `observation_versions` with the D9 rule) | `observation_versions_embedding_hnsw`, `observation_versions_bm25` | app |
| 8 | retain upsert path (`CommitChunk`) | `chunks (document_id, content_hash)` unique, `facts (chunk_id, content_hash)` unique, `entities_canonical_uq`, PKs | app |
| 9 | delete cascade (visibility only, N61) | `facts_doc_idx`, `chunks_doc_idx`, `observation_sources_memory_idx`, `observation_inputs_fact_idx`, `page_sources_source_idx`, `export_snapshots` PK | app |
| 10 | outbox relay read / cursor advance / retention | `outbox` PK (`seq`), `outbox_cursors` PK | relay / admin |
| 11 | move consumer read (anti-join, N50), target apply, target bulk load | `outbox_ns_seq_idx` + `move_applied` PK (source), `move_applied` PK (target), `namespace_ownership` PK | move / move_load |
| 12 | entity resolution (fuzzy, per namespace) | `entities_trgm_idx` (GIN, uuid + trigram) | app |
| 13 | consolidation round selection, stale observations, stale pages | `facts_unconsolidated_idx`, `observations_stale_idx`, `pages_stale_idx` | app |
| 14 | `ListMemories`/`ListDocuments`/`ListOperations` keyset pagination, `GetMemory` | PKs and `*_created_idx`/`documents_updated_idx` | app |
| 15 | op-sweeper (incl. `creating` namespaces, N72), DEFERRED resumer, idempotency expiry, purge sweep, stats sweeper (N69) | `operations_sweeper_idx`, `operations_deferred_idx`, `idempotency_keys_expiry_idx`, `facts_purge_idx` per namespace, `facts_mentioned_idx` (`count(*) WHERE live`) | admin |

In the SQL below, `$1`, `$2`, `$3` are the namespace, the query vector and `as_of`; named
placeholders such as `$q`, `$seeds`, `$budget` stand for further bind parameters and are
numbered by the store. Every statement shown was executed against the validated shard schema
(with the BM25 statements parser-checked, since pg_search is not installable outside the
ParadeDB image).

**Tag predicates (D10), identical in every arm.** Tags are stored sorted, deduplicated and
lowercase (`engram_tags_valid`), so array operators are set operators. `Q` is the sorted query
tag array, `n = |Q|`.

| Mode | Definition | Postgres predicate (semantic, temporal, graph, lists) | pg_search form (lexical arms) |
|---|---|---|---|
| unset | matches everything | (none) | (none) |
| `ANY` | I = ∅ ∨ I ∩ Q ≠ ∅ | `(cardinality(tags) = 0 OR tags && $q)` | `(tag_count = 0 OR memory_id @@@ pdb.parse('tags:(a OR b)'))` |
| `ANY_STRICT` | I ∩ Q ≠ ∅ | `tags && $q` | `memory_id @@@ pdb.parse('tags:(a OR b)')` |
| `ALL` | I = ∅ ∨ Q ⊆ I | `(cardinality(tags) = 0 OR tags @> $q)` | `(tag_count = 0 OR memory_id @@@ pdb.parse('tags:a AND tags:b'))` |
| `ALL_STRICT` | Q ⊆ I ∧ I ≠ ∅ | `tags @> $q AND cardinality(tags) > 0` | `memory_id @@@ pdb.parse('tags:a AND tags:b')` |
| `EXACT` | I = Q | `tags @> $q AND tags <@ $q` | `tag_count = n AND memory_id @@@ pdb.parse('tags:a AND tags:b')` |

`engram_tag_match(mode, q, i)` in the DDL is the SQL twin of the Lean decision procedure
(section 7) and is what `pages.tag_filter` evaluation and the property tests call; the arms
inline the predicates above so the planner sees plain operators. The pg_search forms are
equivalent because tags are deduplicated (`Q ⊆ I ∧ |I| = |Q| ⟹ I = Q`) and the tag grammar
excludes every parser metacharacter, so values are embedded verbatim.

**Semantic arm** (mid budget; `$1` namespace, `$2` query vector, `$3` `as_of`, `$4` fact types,
`$q` tags in mode `ANY`):

```sql
SET LOCAL hnsw.ef_search = 150;                          -- >= the arm cap (N55): 150 at MID, 400 at HIGH
SET LOCAL hnsw.iterative_scan = relaxed_order;
SET LOCAL hnsw.max_scan_tuples = 20000;
-- small namespace (namespace_stats.large = false): SET LOCAL enable_indexscan = off  -> exact scan (bitmap on facts_mentioned_idx + top-K sort)
-- large namespace: namespace_id is bound as a literal so the partial HNSW `WHERE namespace_id = '…'` is provable
SELECT memory_id, document_id, chunk_id, mentioned_at, embedding <=> $2::halfvec(768) AS distance
  FROM facts
 WHERE namespace_id = $1
   AND live
   AND mentioned_at <= $3                               -- omitted when as_of is unset (D9); mentioned_at = item timestamp, never said_at
   AND fact_type = ANY ($4::fact_type[])                 -- omitted when unset
   AND (cardinality(tags) = 0 OR tags && $q)             -- mode-specific, table above
 ORDER BY embedding <=> $2::halfvec(768)
 LIMIT 150;
```

Verified plan: `Index Scan using facts_p07_embedding_idx ... Order By: (embedding <=> $2)
Filter: (live AND namespace_id = ... AND mentioned_at <= ... AND tags && ...)` under RLS with
the other 15 partitions removed. With `relaxed_order` the scan keeps walking the graph until
150 rows pass the filter or `max_scan_tuples` is reached, which is what makes a selective
`as_of` or tag filter return a full candidate list instead of a truncated one — for a
namespace large enough that 150 passing rows lie within 20 k visited tuples. Below 20 k live
facts the shared index would visit far more (a 0.16 %-selective namespace needs ≈ 94 k visits
for 150 hits, review F-8), so the arm takes the exact-scan path; above it the per-namespace
partial index makes the filter trivially selective (3.3.4, N55).

**Lexical arm** (pg_search ≥ 0.25 syntax; before 0.25 read `paradedb.score` and
`paradedb.match` for `pdb.score` and `|||`):

```sql
SELECT memory_id, document_id, chunk_id, mentioned_at, pdb.score(memory_id) AS score
  FROM facts
 WHERE namespace_id = $1
   AND text ||| $2                                      -- match disjunction over the query terms
   AND live = true
   AND mentioned_at <= $3
   AND fact_type_code = ANY ($4::smallint[])
   AND (tag_count = 0 OR memory_id @@@ pdb.parse($5))   -- $5 = 'tags:("a" OR "b")', mode ANY
 ORDER BY pdb.score(memory_id) DESC, memory_id ASC      -- deterministic tiebreak, both indexed
 LIMIT 150;
```

Every predicate names a column that is in the BM25 index (`namespace_id`, `live`,
`mentioned_at`, `fact_type_code`, `tag_count` columnar by default; `tags` literal-tokenized), so
the whole `WHERE` runs inside the index scan and `ORDER BY score LIMIT` is pushed down as Top-K
(`TopKScanExecState` in `EXPLAIN`). An unindexed predicate would force a scan of every match
sorted by score; the store's query registry (N19) has an `EXPLAIN` assertion for this plan.
`pdb.score` is used for *ranking only*: the BM25 statistics (IDF) are per index, i.e. per hash
partition shared by ≈ 10 namespaces of several tenants, so a raw score is partition-relative,
changes as neighbours ingest, differs after a move, and would let a tenant probe term rarity
across the partition (review F-28). `Scores.lexical` (and `Scores.semantic`) are therefore
reported as per-query normalised values in [0, 1] — the raw score divided by the arm's top
score — never the raw BM25 value (N67).

**Observation arms** apply the D9 rule as a filter; the lexical form is the same shape as above
on `observation_versions_bm25`:

```sql
SELECT ov_id, observation_id, version, effective_at, embedding <=> $2::halfvec(768) AS distance
  FROM observation_versions
 WHERE namespace_id = $1 AND live                                              -- live = retired_at IS NULL AND NOT derived_from_deleted AND NOT (stale_delete AND superseded_at IS NULL) (N41 as amended)
   AND effective_at <= $3 AND (superseded_at IS NULL OR superseded_at > $3)   -- as_of; unset: superseded_at IS NULL
   AND (cardinality(tags) = 0 OR tags && $q)
 ORDER BY embedding <=> $2::halfvec(768)
 LIMIT 150;
```

**Temporal arm** (N68, review F-32; `$4..$5` query window, `$6` `query_timestamp`, N = the
arm cap). A two-sided probe on the btree `(namespace_id, occurred_start)`: the nearest N
occurrences before `query_timestamp` and the nearest N after it, each an index range scan that
stops after N rows, then one 2N-row sort — instead of collecting every fact whose window
overlaps a possibly year-wide query window and sorting it in memory:

```sql
(SELECT memory_id, occurred_start, occurred_end
   FROM facts
  WHERE namespace_id = $1 AND live AND occurred_start IS NOT NULL
    AND occurred_start <= $6 AND occurred_start >= $4                -- the "before" side, inside the query window
    AND mentioned_at <= $3
  ORDER BY occurred_start DESC
  LIMIT 150)
UNION ALL
(SELECT memory_id, occurred_start, occurred_end
   FROM facts
  WHERE namespace_id = $1 AND live AND occurred_start IS NOT NULL
    AND occurred_start > $6 AND occurred_start <= $5                 -- the "after" side
    AND mentioned_at <= $3
  ORDER BY occurred_start ASC
  LIMIT 150)
ORDER BY abs(extract(epoch FROM (occurred_start - $6)))
LIMIT 150;
```

Each branch is a backward or forward range scan on `facts_occurred_idx` (partial: `live AND
occurred_start IS NOT NULL`, namespace-leading) that terminates after 150 index entries, so
the arm's cost is bounded by 2 × 150 heap fetches whatever the window width; a fact with an
open-ended `occurred_end` participates by its start. The GiST range index stays for explicit
occurrence-window filters (list methods, Recall filters), where overlap semantics are what
the caller asked for.

**Graph expansion** (one query per link family; temporal: `$types = '{temporal}'`, `$hops = 5`,
`$per_node = 10`; the others: `'{entity,semantic,causal}'`, `$hops = 2`; `$budget` = 100/300/1000;
`$seeds` = top-20 of semantic ∪ lexical). The recursive CTE carries the frontier and the global
visited set as arrays, so the node budget is global and every hop is one index-driven query:

```sql
WITH RECURSIVE hops AS (
  SELECT 0 AS hop, $seeds::uuid[] AS frontier, $seeds::uuid[] AS visited
  UNION ALL
  SELECT h.hop + 1, nf.frontier, h.visited || nf.frontier
    FROM hops h
    CROSS JOIN LATERAL (
      SELECT array_agg(nb) AS frontier
        FROM (
          SELECT e.nb
            FROM (
              SELECT CASE WHEN l.src_memory_id = ANY (h.frontier) THEN l.dst_memory_id ELSE l.src_memory_id END AS nb,
                     CASE WHEN l.src_memory_id = ANY (h.frontier) THEN l.src_memory_id ELSE l.dst_memory_id END AS origin,
                     l.weight
                FROM fact_links l
               WHERE l.namespace_id = $1
                 AND l.link_type = ANY ($types::link_type[])
                 AND (l.src_memory_id = ANY (h.frontier) OR l.dst_memory_id = ANY (h.frontier))
                 AND (l.link_type <> 'causal' OR l.src_memory_id = ANY (h.frontier))      -- causal: forward only
            ) e
            JOIN facts f ON f.namespace_id = $1 AND f.memory_id = e.nb
           WHERE NOT (e.nb = ANY (h.visited))
             AND f.live AND f.mentioned_at <= $3                                          -- as_of inside the arm (D9)
           GROUP BY e.nb
           ORDER BY max(e.weight) DESC
           LIMIT GREATEST(0, $budget - cardinality(h.visited))
        ) s
    ) nf
   WHERE h.hop < $hops AND cardinality(h.visited) < $budget AND nf.frontier IS NOT NULL
)
SELECT unnest(deepest.visited) AS memory_id
  FROM (SELECT visited FROM hops ORDER BY hop DESC LIMIT 1) AS deepest;   -- pick the deepest row first, THEN unnest (review F-40)
```

The per-node cap (`$per_node` neighbours per frontier node) is applied by a window
`row_number() OVER (PARTITION BY origin ORDER BY weight DESC) <= $per_node` in the inner
subquery (elided above for width). Verified on a chain graph: budget 7 and 5 hops stop at
exactly 7 visited nodes (the earlier `SELECT unnest(visited) … ORDER BY hop DESC LIMIT 1`
unnested before limiting and returned one id — F-40). `NoOrphanLinks` is enforced here, not by
the delete cascade (N61): the seeds come from arms that filter `live`, every frontier node was
admitted by the `JOIN facts f … WHERE f.live AND f.mentioned_at <= $3` of an earlier hop, and
a neighbour is admitted only through the same join — so a link is traversed only between two
live, `as_of`-visible facts, and a link left dangling by a retire, an invalidate or a
not-yet-purged delete is never followed. Cost per hop is one index range scan per frontier node on the PK and
one on the reverse index, plus a PK lookup per candidate on `facts`; at the mid budget (300
nodes, ≤ 5 hops) that is ≤ 3,000 index probes, ≈ 3 ms warm. Rejected: a Go-driven BFS with one
round trip per hop (5 to 7 round trips through pgbouncer at the p95 budget); a recursive CTE
without the array state (no way to enforce a global budget, exponential blow-up).

**Delete cascade** (D8, N61: *visibility only*; one fenced write transaction as `engram_app`
after the 3.3 prelude; `$1` namespace, `$2` document, `$t`/`$e`/`$op` tenant, epoch,
operation). Statement order is load-bearing: facts are retired **before** their source and
input rows are deleted, which is what lets the evidence triggers tell a delete from a
consolidation update (3.3.6):

```sql
SELECT pg_advisory_xact_lock(hashtextextended($1::text || '/' || $2, 0));      -- one cascade per document at a time (N27)
UPDATE documents SET state = 'deleting', deleted_at = now()
 WHERE namespace_id = $1 AND document_id = $2 AND state = 'active' RETURNING current_version, created_at;   -- 0 rows -> NOT_FOUND; created_at -> $c
WITH f AS (
  UPDATE facts SET retired_at = coalesce(retired_at, now()), purge_after = now()
   WHERE namespace_id = $1 AND document_id = $2 AND (purge_after IS NULL OR purge_after > now())
   RETURNING memory_id)
SELECT array_agg(memory_id) FROM f;                                              -- -> $3 (facts are non-live from here on)
-- fact_links and entity_mentions are NOT touched here (N61): the graph arm never traverses a non-live endpoint;
-- PurgeDocument deletes them through the FK cascade when it deletes the facts
WITH d AS (DELETE FROM observation_sources WHERE namespace_id = $1 AND memory_id = ANY ($3) RETURNING observation_id),
     i AS (DELETE FROM observation_inputs  WHERE namespace_id = $1 AND fact_id   = ANY ($3) RETURNING observation_id)
SELECT array_agg(DISTINCT observation_id) FROM (SELECT observation_id FROM d UNION ALL SELECT observation_id FROM i) u;   -- -> $4; the triggers retire (0 sources), flag the versions that named a victim derived_from_deleted, and mark stale_delete where the current version is affected (N41 as amended)
SELECT observation_id, retired_at IS NOT NULL AS retired, stale_delete
  FROM observations WHERE namespace_id = $1 AND observation_id = ANY ($4);       -- for ObservationRetired / ObservationsMarkedStale events
WITH p AS (DELETE FROM page_sources WHERE namespace_id = $1 AND source_kind = 'fact' AND source_id = ANY ($3) RETURNING page_id)
UPDATE pages SET stale_delete = true, stale_seq = stale_seq + 1
 WHERE namespace_id = $1 AND page_id IN (SELECT page_id FROM p);
UPDATE chunks SET retired_at = coalesce(retired_at, now()), purge_after = now()
 WHERE namespace_id = $1 AND document_id = $2 AND (purge_after IS NULL OR purge_after > now());
UPDATE document_versions SET status = 'deleted', finished_at = coalesce(finished_at, now())
 WHERE namespace_id = $1 AND document_id = $2 AND status <> 'deleted';
UPDATE export_snapshots SET state = 'expired', expired_reason = 'document_delete', expires_at = now()
 WHERE namespace_id = $1 AND state = 'ready' AND created_at >= $c RETURNING version;          -- N59: every snapshot that may contain the document; -> snapshots_expired[]
-- namespace_stats: nothing here — the counters are derived by the stats sweeper (N69)
INSERT INTO deletion_log (namespace_id, tenant_id, kind, subject_id, epoch, operation_id)
VALUES ($1, $t, 'document', $2, $e, $op);
INSERT INTO blob_tombstones (namespace_id, tenant_id, tombstone_id, blob_key, reason, operation_id)
SELECT $1, $t, engram_uuid_v7(), k, 'document_delete', $op FROM unnest($blob_keys) AS k;   -- docsum, ledger/{sha256} if unshared
INSERT INTO operations (namespace_id, tenant_id, operation_id, kind, target_id, workflow_id, task_queue, submitted_epoch)
VALUES ($1, $t, $op, 'purge', $2, 'ns/' || $1 || '/op/' || $op, $queue, $e);
INSERT INTO outbox (event_type, payload) VALUES ('DocumentDeleted', $proto);
COMMIT;
```

After commit nothing from the document is reachable by any arm (`live` is false on every fact
and chunk and on every observation version whose inputs named one of them; citations and
inputs are gone; links and mentions still exist but no arm steps onto a non-live endpoint;
every export snapshot that could contain the document is `expired`), which is the D16 delete
guarantee; `PurgeDocument` removes the physical rows (facts, chunks and, through the FK
cascades, links and mentions) and the blobs afterwards. **Cascade cost and SLO (N61, review
F-19).** The transaction touches, per retired fact, one `facts` row (on `facts_doc_idx`), its
`observation_sources`/`observation_inputs` rows (≈ 1 + 4 on their memory indexes) and the
trigger `UPDATE`s on the affected observations; chunks and versions are per document. That is
≈ 6 index probes and ≈ 3 row updates per fact and no 300 M-row table, so the SLO is stated
**per 1 k retired facts: p95 ≤ 50 ms per 1 k facts** on a shard at its write target (a
10 k-fact document ≈ 0.5 s, a 100 k-fact document ≈ 5 s, inside the 30 s writer
`statement_timeout`; the §9 table carries the same per-1 k figure). Invalidate/Restore are the
one-row variants (`facts.invalidated_at`, observations `stale_delete` / `stale_write` and the
mirror on the current version, pages `stale_delete`) described in section 5.4.5.

**Retain upsert path** (`CommitChunk`, one fenced transaction per chunk, idempotent on re-execution):

```sql
SELECT state, current_version FROM documents WHERE namespace_id = $1 AND document_id = $2 FOR SHARE;   -- state <> 'active' -> aborted
SELECT status FROM document_versions
 WHERE namespace_id = $1 AND document_id = $2 AND version = $v FOR SHARE;    -- N40: anything but 'ingesting' -> stop (superseded / deleted)
INSERT INTO chunks (namespace_id, tenant_id, chunk_id, document_id, content_hash, header_hash, extraction_key, ordinal,
                    heading_path, header, text, tags, mentioned_at, embedding, embedding_model)
VALUES (...)
ON CONFLICT (namespace_id, document_id, content_hash) DO UPDATE
   SET retired_at = NULL, purge_after = NULL, ordinal = EXCLUDED.ordinal, header = EXCLUDED.header,
       header_hash = EXCLUDED.header_hash, embedding = EXCLUDED.embedding, extraction_key = EXCLUDED.extraction_key
RETURNING chunk_id, (created_at = now()) AS inserted;         -- xmax is not readable on a partitioned table
UPDATE facts SET retired_at = NULL, purge_after = NULL
 WHERE namespace_id = $1 AND chunk_id = $chunk AND extraction_key = $xkey AND retired_at IS NOT NULL;   -- un-retire
INSERT INTO facts (..., mentioned_at, said_at, ...) VALUES (..., $item_timestamp, $said_at, ...)   -- only when inserted or no live facts under $xkey; mentioned_at from the ledger row, never from the extractor (D9)
ON CONFLICT (namespace_id, chunk_id, content_hash) DO NOTHING;
INSERT INTO entities (namespace_id, tenant_id, entity_id, canonical_name, canonical_norm, entity_type, mention_count)
SELECT $1, $t, e.id, e.name, e.norm, e.type, e.n
  FROM unnest($ids, $names, $norms, $types, $counts) AS e (id, name, norm, type, n)
 ORDER BY e.norm                                                     -- N69: one statement, rows upserted in canonical_norm order = lock order; no deadlocks between concurrent chunks
ON CONFLICT (namespace_id, canonical_norm) WHERE merged_into IS NULL
DO UPDATE SET mention_count = entities.mention_count + EXCLUDED.mention_count, last_seen_at = now()
RETURNING entity_id, canonical_norm;
INSERT INTO entity_aliases (...) ON CONFLICT DO NOTHING;
INSERT INTO entity_mentions (...) ON CONFLICT DO NOTHING;
INSERT INTO fact_links (...) VALUES (...)                          -- rows pre-sorted by (src, dst, link_type): no deadlocks
ON CONFLICT DO NOTHING;
INSERT INTO document_version_chunks (namespace_id, tenant_id, document_id, version, content_hash, chunk_id)
VALUES (...) ON CONFLICT DO NOTHING;
-- no UPDATE of document_versions.chunks_done here (it would deadlock with the FOR SHARE of a concurrent commit of v) and no
-- UPDATE of namespace_stats (derived, N69): the workflow writes chunks_done and operations.progress once per wave
INSERT INTO token_usage_events (...) ON CONFLICT (namespace_id, usage_key) DO NOTHING;
INSERT INTO token_usage (...) ON CONFLICT (namespace_id, day, op, model, price_version)
DO UPDATE SET calls = token_usage.calls + EXCLUDED.calls, prompt_tokens = token_usage.prompt_tokens + EXCLUDED.prompt_tokens,
              completion_tokens = token_usage.completion_tokens + EXCLUDED.completion_tokens,
              cost_micros = token_usage.cost_micros + EXCLUDED.cost_micros, updated_at = now();
INSERT INTO outbox (event_type, payload) VALUES ('ChunkCommitted', $proto);
COMMIT;
```

The `inserted` flag uses `created_at = now()` because `xmax` is not accessible in `RETURNING`
on a partitioned table (verified: "cannot retrieve a system column in this context"); section 5
should use the same idiom.

**FinalizeVersion retire set** (N58, review F-12; same per-document advisory lock as the
cascade, N27, after the N40 version-row check). Two statements: the chunks that left the
version's set, and the facts of *kept* chunks whose `extraction_key` is no longer the chunk's
current one — the second is what a prompt or model bump needs, since the chunk row is updated
in place by `CommitChunk` and stays in the set:

```sql
UPDATE facts f SET retired_at = now(), purge_after = now() + interval '1 hour'          -- retire grace (D8)
  FROM chunks c
 WHERE f.namespace_id = $1 AND f.document_id = $2 AND f.retired_at IS NULL
   AND c.namespace_id = f.namespace_id AND c.chunk_id = f.chunk_id
   AND f.extraction_key <> c.extraction_key                                            -- old-key facts on a kept chunk
RETURNING f.memory_id;                                                                   -- -> ChunksRetired.fact_ids
```

Retired facts keep their `observation_sources`/`observation_inputs` rows during the grace and
`FinalizeVersion` marks the citing observations `stale_write` (N42). Verified on the schema: a
key bump followed by this statement leaves zero live facts under the old key.

**Consolidation apply order** (N57, review F-11). The `update` op inserts the new version
row, sets `superseded_at` on the previous one, inserts the new `observation_sources` and
`observation_inputs` rows (`ON CONFLICT DO NOTHING`), and only then `DELETE FROM
observation_sources WHERE observation_id = $1 AND memory_id <> ALL ($new)`. The zero-sources
trigger therefore sees a non-empty set, and because the deleted rows name live facts the
"lost evidence" branch does not fire either (3.3.6), so the observation being rewritten is
neither retired nor hidden by its own update — whichever order a future edit lists the two
statements in.

**Outbox relay** (`engram_relay`, direct connection): `SELECT seq, namespace_id, tenant_id,
epoch, event_type, payload FROM outbox WHERE seq > $hwm ORDER BY seq LIMIT 500`; gap probe
`SELECT seq FROM outbox WHERE seq = ANY ($gaps)`; `UPDATE outbox_cursors SET last_seq = $n,
gaps = $j, updated_at = now() WHERE consumer = $c`. Retention (admin): `DELETE FROM outbox WHERE
seq IN (SELECT seq FROM outbox WHERE seq < $min_cursor AND created_at < now() - interval '7 days'
ORDER BY seq LIMIT 10000)`.

**Move consumer** (`engram_move`, N50): source `SELECT o.seq, o.event_type, o.payload FROM
outbox o WHERE o.namespace_id = $1 AND o.seq > $p0 AND NOT EXISTS (SELECT 1 FROM move_applied m
WHERE m.namespace_id = o.namespace_id AND m.seq = o.seq) ORDER BY o.seq LIMIT 500` (anti-join
on `outbox_ns_seq_idx` + the `move_applied` PK; never `seq > $applied`, review F-3); target
`INSERT INTO move_applied (namespace_id, tenant_id, seq, move_id) VALUES (...) ON CONFLICT DO
NOTHING RETURNING seq` (no row → already applied, skip) then the event's row-level apply in
the same transaction, then — after that commit — the same `INSERT … ON CONFLICT DO NOTHING`
into the source's `move_applied`, then `UPDATE namespace_ownership SET move_applied_seq = $seq
WHERE namespace_id = $1 AND state = 'incoming' AND coalesce(move_applied_seq, 0) < $seq` for
lag reporting. **Move loader** (`engram_move_load`, N51), per 10 k-row batch: `BEGIN; SELECT
pg_advisory_xact_lock_shared(engram_ns_lock_key($1)); SELECT state, epoch FROM
namespace_ownership WHERE namespace_id = $1` → abort unless `('incoming', $e + 1)`; `COPY
<table> (...) FROM STDIN BINARY; COMMIT`.

**Entity resolution**: `SELECT entity_id, canonical_name, similarity(canonical_norm, $2) AS s
FROM entities WHERE namespace_id = $1 AND merged_into IS NULL AND canonical_norm % $2 ORDER BY s
DESC LIMIT 5` on `entities_trgm_idx`, after an exact `entity_aliases` PK probe.

**Schedulers** (admin, shard-wide): op-sweeper `SELECT namespace_id, tenant_id, operation_id,
workflow_id, task_queue FROM operations WHERE state = 'PENDING' AND workflow_started_at IS NULL
AND created_at < now() - interval '2 min' ORDER BY created_at LIMIT 100` on
`operations_sweeper_idx`; DEFERRED resumer on `operations_deferred_idx`; idempotency expiry
`DELETE ... WHERE expires_at < now()` in batches; the purge safety net iterates
`namespace_ownership` and probes `facts_purge_idx` per namespace rather than carrying a
shard-wide index on a 10 M-row table. The same sweeper tick completes or deletes catalog
namespaces left `creating` for more than 2 min (N72, through the admin RPC — workers never
write the catalog directly, D4) and the **stats sweeper** refreshes `namespace_stats` per
namespace (`SELECT count(*) FROM facts WHERE namespace_id = $1 AND live` on
`facts_mentioned_idx`, and the chunk/document/observation counts likewise) and flips `large`
at the 20 k / 10 k thresholds (N55, N69).

### 3.9 Rejected alternatives

| Alternative | Why rejected (one line) |
|---|---|
| pgvector `vector(768)` (float32) instead of `halfvec(768)` | doubles vector heap and HNSW (36 GB more per shard) for no measurable recall gain on nomic-embed at 768-d |
| Matryoshka 512-d by default | 33 % smaller index but a known recall drop on LongMemEval; kept as the D15 knob, off |
| One partition per namespace | 150 to 1,000 relations per shard, DDL on namespace create, planner cost per partition, no benefit over hash pruning |
| No partitioning | an 18 GB HNSW that cannot build in `maintenance_work_mem`, a 300 M-row vacuum, no per-partition `REINDEX` |
| Range partitioning by `created_at` | queries are per namespace, never per time slice; `as_of` is on `mentioned_at`, which is not insert-ordered |
| A separate `search_entries` table (text + vector per searchable thing) | a join on every arm, a second copy of 15 GB of vectors, and the index becomes a table to keep consistent instead of an index |
| Chunk text in blob storage | the chunk arm would do a blob round trip per candidate; 3 GB of text in Postgres is cheap and moves with `COPY` |
| `bigserial` ids | not portable across shards during a move (D1); UUIDv7 keeps insert locality anyway |
| Application-only isolation, no RLS | one forgotten predicate is a cross-tenant leak; RLS turns it into an empty result and the canary (N19) proves every query |
| RLS keyed on `tenant_id` | the tenant is not the request unit; a same-tenant cross-namespace leak is still a leak |
| `FORCE` off, policies on parents only | the owner and direct partition access would bypass; the section 8 check requires both |
| Five W columns (`who`, `what`, `when`, `where`, `why`) | `who` is multi-valued; five nullable text columns widen every row for data that is never filtered |
| Storing symmetric links twice | halves nothing but doubles the largest table (71 GB → 140 GB) |
| Adjacency arrays (`neighbours uuid[]` + GIN) | ≈ 15 GB instead of 71, but the delete cascade becomes an `UPDATE` fan-out with GIN maintenance and the no-orphan-link invariant needs a scan; fallback if the volume budget binds |
| Counting trigger for link caps | one extra query per inserted row on a 300 M-row table; the caps are quality knobs, not integrity |
| Rich outbox events (text + vectors) | 3.5 KB per event, 4 GB per shard-week, and Kafka consumers get vectors they must not have; N12 fixes thin events |
| In-memory gap watchlist | lost on relay failover inside the 60 s window; the persisted list costs one row update |
| `move_applied_seq` as the replay cursor (`seq > watermark`) | review F-3: a lower `seq` committing after a higher one was applied is skipped, and under RLS the mover has no gap to watch; the read is an anti-join against `move_applied` on the source (N50) |
| `FOR SHARE` on the ownership row as the fence lock | review F-5: compatible row lockers bypass a waiting `FOR UPDATE`, so writers can starve the freeze indefinitely, and each extra share locker allocates a multixact; a shared advisory lock queues fairly and allocates nothing (D2 as amended) |
| `COPY FROM` into the move target under RLS as `engram_move` | PostgreSQL refuses `COPY FROM` on RLS tables (F-4); `engram_move_load` is `BYPASSRLS` + `INSERT`-only with an explicit per-batch ownership check (N51) |
| Deleting `fact_links`/`entity_mentions` in the synchronous cascade | ≈ 3 M row deletes for a 100 k-fact document, past the writer timeout (F-19); the graph arm already requires `live` on both endpoints, so the purge takes them (N61) |
| Enum for `outbox.event_type` and `operations.kind` | adding an event or kind would be a migration on every shard; the proto is the source of truth |
| `tsvector` + `ts_rank_cd` instead of pg_search | not BM25 and no Top-K pushdown; kept as the documented lower-quality fallback (D7) |
| pg_search JSON `text_fields` configuration | the pre-0.25 syntax; cast-based configuration is what the pinned image documents and is parser-checked |
| `ledger/{ledger_id}` keys for oversized bodies | not content-addressed; identical re-sends would store the body twice (N7 fixes `ledger/{sha256}`) |
| Computing observation visibility at `as_of` with a correlated subquery | correct but kills Top-K pushdown and HNSW in-scan filtering; `superseded_at` is one column |
| Storing `shard_id` on every row | a constant column per database; a wrong value can only mean a mis-provisioned database, which the ownership trigger catches |
| A dedicated `engram` schema | no isolation benefit inside a per-shard database, and every sibling check and tool assumes `public` |
| A single `observations.stale` flag | cannot distinguish "evidence changed" (visible) from "evidence deleted" (must be hidden, N41); two booleans mirror `pages` and `live` folds the flags into the scan predicate |
| Observation-level hiding cleared by reconsolidation | review F-1: clearing one observation-level flag resurrects an older version written with deleted content in view under `as_of`; `derived_from_deleted` is per version and permanent |
| A model-adjustable `mentioned_at` as the `as_of` key | review F-2: leak-freedom would depend on the extractor's dating; `mentioned_at` is the server-set item timestamp, `said_at` carries the model's judgement for display and ranking |
| Per-commit `UPDATE` of `namespace_stats` / `chunks_done` | review F-36: serialises every commit of a namespace (and of a document) on one row and deadlocks with the N40 `FOR SHARE`; counters are derived or written per wave (N69) |
| GiST overlap scan + in-memory sort for the temporal arm | review F-32: a year-wide window sorts up to the whole namespace; the two-sided btree probe is bounded by 2 × the arm cap (N68) |
| Recording only cited sources per observation version | `AsOf_CitedOnly` and `DocLifecycle_CitedOnly` (§7): a version written with a newer or later-deleted fact in view would surface; `observation_inputs` costs ≈ 10 GB per shard (3.7, Q15) and makes both checks one-hop |
| `op_key` over the live LLM answer, proposal kept in Temporal payloads only | a retried `ConsolidateBatch` can answer differently; `Consolidation_VolatileProposal` (§7) applies two lists under one key — the write-once table is the fix (N43) |

### 3.10 Notes for the other sections

Items this section settled and that the neighbouring sections now follow (the divergences found
while reading them were fixed in those sections during editing):

- Section 5's `CommitChunk` uses `RETURNING chunk_id, (created_at = now()) AS inserted` (3.8);
  `xmax` is not readable in `RETURNING` on a partitioned table.
- Section 5 names `export_snapshots`, the `operations.error` JSON column and the admin-role
  ledger delete at purge (there is no `ledger_tombstones` table: ledger rows are deleted by
  the purge path under `engram_admin`, and `deletion_log` is the audit trail, N21).
- Observation scope tags are `observations.tags`, so every arm filters the same column name.
- Section 8's static RLS check allows the `relay_read_all` policy on `outbox` only (N4; the
  deletion-log consumer reads the `deletion_log` key fields from the `DocumentDeleted`,
  `FactInvalidated` and `NamespacePurged` events).
- Section 9's `shard_meta` carries `shard_id` as well as the version columns; the ownership
  trigger depends on it, so provisioning must insert that row before the first namespace.
- The register's D3 row carries this section's sizing (≈ 71 GB of `fact_links`, ≈ 168 GB
  total including the capped `observation_inputs`, ≈ 133 GB at 15 links per fact, ≈ 40 GB
  hot; see 3.7 for the arithmetic).
- Model checking (§7, register D19) added `observation_inputs`, `consolidation_proposals`,
  the `stale_write`/`stale_delete` split on `observations` (with `stale_delete` denormalised
  into `observation_versions.live`), the version-row `FOR SHARE` protocol on
  `document_versions`, and the copy barrier in 3.5. Section 5's cascade, apply, finalize
  and move steps and section 8's tests follow these tables; `observation_inputs` is in the D3
  sizing row at its N47-capped size (3.7).
- Section 9's SLO table states the delete cascade per 1 k retired facts (3.8) and
  `maintenance_work_mem = 2 GB` is the one value both sections use; workers report tenant
  usage deltas to the catalog through the API, never directly (D4; the §3.2 wording stands).

**Review follow-ups applied in this section and the SQL** (`REVIEW.md` finding → register row →
what changed here):

| Finding | Register | Change in §3 / `shard_schema.sql` / `catalog_schema.sql` |
|---|---|---|
| F-1 (blocker) | N41 | `observation_versions.derived_from_deleted`, set per version by the `observation_inputs` trigger (`engram_observation_inputs_after_delete`) for exactly the versions whose inputs named the victim, never cleared; `live` = `retired_at IS NULL AND NOT derived_from_deleted AND NOT (stale_delete AND superseded_at IS NULL)`; version-level `stale_delete` is a mirror for the current version only; recall predicate restated in 3.1, 3.3.6, 3.8; the F-1 interleaving executed against the DDL |
| F-2 (blocker) | D9 | `facts.said_at` (nullable); `mentioned_at` documented as server-set = item timestamp on facts and chunks, the only `as_of` key (3.3.4 decisions table, retain path) |
| F-3 (blocker) | N50 | catch-up/drain read is the anti-join against `move_applied`; `move_applied` kept on both shards (target authoritative, source a lagging copy); `move_applied_seq` is lag reporting only (3.3.1, 3.5, 3.8) |
| F-4 (blocker) | N51 | role `engram_move_load` (`BYPASSRLS`, `INSERT`-only, `SELECT` on `namespace_ownership`/`shard_meta`), per-batch fence under the shared advisory lock, self-check 7 pins the privilege shape; `COPY FROM` under RLS verified refused, under `BYPASSRLS` verified working |
| F-5 (major) | D2 | fence = `pg_advisory_xact_lock_shared(engram_ns_lock_key(ns))` + plain `SELECT`; exclusive lock for barrier (session-level), freeze, cutover, rollback, delete, restore, export; `FOR SHARE` and the `UPDATE (updated_at)` grant removed; lock discipline table in 3.3 |
| F-8 (major) | N55 | `namespace_stats.large` / `catalog.namespaces.large`; exact-scan path below 20 k live facts, partial HNSW per large namespace on its partition (owner-run DDL), `ef_search ≥` arm cap (3.3.4, 3.8) |
| F-9 (major) | N47 | `observation_inputs` = batch facts ∪ rendered quoted sources (≤ 5 per candidate, ≤ 58 per version); sizing row ≈ 45 M rows / ≈ 10 GB; `consolidation_proposals.input_fact_ids` comment |
| F-11 (major) | N57 | apply order insert-first/delete-second stated in 3.8; triggers made order-robust by defining lost evidence as "row gone and fact not live" (3.3.6), verified |
| F-12 (major) | N58 | `FinalizeVersion` retire statement for facts whose `extraction_key <> chunks.extraction_key` (3.3.3, 3.8), verified |
| F-13 (major) | N59 | cascade marks containing `export_snapshots` `expired` (`expired_reason = 'document_delete'`, `expires_at = now()`); `DocumentDeleted.snapshots_expired[]` |
| F-19, F-38 (major, minor) | N61 | cascade = visibility only; `fact_links`/`entity_mentions` deleted by `PurgeDocument` via FK cascade; `NoOrphanLinks` restated as the graph arm's both-endpoints-live join; SLO per 1 k facts (p95 ≤ 50 ms / 1 k) |
| F-23 (major) | N64 | ownership state machine written once in 3.3.1 (states × roles × statements) and enforced by `engram_check_ownership` (transitions, roles, `DELETE`); freeze `UPDATE` sets `freeze_reason`; `CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL))`; read fence = state only; catalog `namespace_state` gains `restoring`; every transition executed against the DDL |
| F-27 (minor) | — | relay policy comment: `outbox` only; `maintenance_work_mem = 2 GB`; D3 total ≈ 168 GB / ≈ 133 GB wording; `operations.kind` without `'reflect'`; workers report usage through the API (kept) |
| F-28 (minor) | N67 | lexical arm note: `pdb.score` ranks only; `Scores.lexical`/`semantic` normalised to [0, 1], raw BM25 never exposed |
| F-32 (minor) | N68 | temporal arm = two-sided btree probe on new `facts_occurred_idx (namespace_id, occurred_start)`; GiST kept for explicit window filters |
| F-36, F-41 (minor, nit) | N69 | `namespace_stats` derived by the stats sweeper; no per-commit `UPDATE` of stats or `chunks_done` (the latter would deadlock with the N40 `FOR SHARE`); `operations.progress` per wave; entity upsert in one statement ordered by `canonical_norm` |
| F-40 (nit) | — | graph-expansion final `SELECT` picks the deepest row first, then `unnest`s (verified: 3 ids, not 1) |
| F-45 (nit) | N72 | `creating` namespaces completed or deleted by the op-sweeper after 2 min; `namespaces_creating_idx`; `CreateNamespace` shard insert runs as `engram_app` with the trigger restricting it to `active`/epoch 1 |
