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
   other table is ordinary mutable state: `retired_at`, `invalidated_at`, `stale`, version
   pointers and counters are updated in place, and physical rows are purged asynchronously.
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
   fence (`FOR SHARE` on `state='active' AND epoch=$e`, D2); the outbox row is stamped with
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
   are stored generated columns, and observation visibility at `as_of` is precomputed as
   `effective_at <= T AND (superseded_at IS NULL OR superseded_at > T)`. Rationale: HNSW
   iterative scans and pg_search Top-K pushdown both need their filters to be columnar
   predicates, not joins or subqueries (3.8). Rejected: computing visibility at query time with
   correlated subqueries (kills Top-K pushdown, 10x slower lexical arm).

Mutable versus append-only, per table group:

| Group | Mutability | Physical delete |
|---|---|---|
| `ingest_ledger` | append-only (trigger) | purge after an acknowledged delete, namespace delete, move cleanup; `engram_admin`/`engram_move` only |
| `facts`, `chunks` | `retired_at`, `purge_after`, `invalidated_at`, `consolidated_at`, tags/metadata on re-retain | `PurgeWorkflow` in batches of 1,000 once `purge_after <= now()` |
| `fact_links`, `entity_mentions`, `observation_sources`, `page_sources`, `document_version_chunks` | insert/delete only, never updated | synchronous in the delete cascade (D8) or purge |
| `documents`, `document_versions`, `observations`, `observation_versions`, `pages`, `page_versions`, `entities`, `operations` | updated in place (state, counters, flags, versions) | purge / namespace delete |
| `namespace_ownership`, `namespace_stats`, `outbox_cursors`, `token_usage`, `quota_counters` | hot updates, `fillfactor` 50 to 70 | never (ownership rows outlive the data for the fence) |
| `outbox`, `deletion_log`, `token_usage_events`, `consolidation_applied`, `move_applied` | insert only | retention jobs (7 d, catalog-replicated, 30 d, never, move end) |

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
  state             namespace_state NOT NULL DEFAULT 'creating', -- creating|active|moving|frozen|deleting|deleted
  config            jsonb NOT NULL DEFAULT '{}',                 -- inheritable layer (D12)
  profile           jsonb NOT NULL DEFAULT '{}',                 -- reflect mission/directives/disposition, not inherited
  facts_estimate    bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0,
  ...
);
CREATE UNIQUE INDEX namespaces_tenant_name_uq ON namespaces (tenant_id, name) WHERE state <> 'deleted';
CREATE INDEX namespaces_shard_idx ON namespaces (shard_id, state);

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
concurrent creates pick different shards instead of queueing. `facts_estimate` is refreshed
hourly from each shard's `namespace_stats` by `engramctl stats` (section 9). Rejected: random
placement (hot namespaces pile up); power-of-two choices (needs live load, which the catalog
does not have at creation time).

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
| `engram_app` | `NOBYPASSRLS`, confined by `ns_isolation` | DML on all namespace-scoped tables; `SELECT, INSERT` on `ingest_ledger`, `outbox`, `deletion_log`; `SELECT` plus column-level `UPDATE (updated_at)` on `namespace_ownership` (a `FOR SHARE` lock needs `UPDATE` on at least one column; verified); `SELECT` on `shard_meta` | API and worker per-namespace transactions |
| `engram_relay` | `NOBYPASSRLS` + policy `relay_read_all` on `outbox` (`SELECT`, all namespaces) | `SELECT` on `outbox`; all on `outbox_cursors`; nothing else (D6, N4) | outbox relay, on a direct connection (N15) |
| `engram_move` | `NOBYPASSRLS`, confined by `ns_isolation` to the namespace being moved | DML on every namespace-scoped table and on `namespace_ownership`, `move_applied`; `SELECT, INSERT` on `outbox`; may `DELETE` from `ingest_ledger` | move executor (N2): `COPY` out of the source, `COPY` into the target, replay, cleanup |
| `engram_admin` | `BYPASSRLS` | all | engramctl, purge workflows, shard-wide schedulers (op-sweeper, DEFERRED resumer, retention) |

`engram_move` deliberately runs *under* RLS: the move touches exactly one namespace, so the
policy is a second fence for free (a bug that mixes namespaces during a copy gets zero rows or
`42501`, never another tenant's data; section 8 `TestIso_Move_TenantMix` relies on this).
Partitions receive no grants at all, so a direct partition reference by any non-admin role is
`permission denied` (queries through the parent check parent privileges only).

**Transaction scope and fence.** Every store transaction starts with

```sql
SELECT set_config('engram.namespace_id', $1, true),
       set_config('engram.tenant_id',    $2, true),
       set_config('engram.epoch',        $3, true);           -- SET LOCAL, pgbouncer-safe
-- writes (D2): waits behind a concurrent freeze, then re-evaluates the predicate
SELECT 1 FROM namespace_ownership
 WHERE namespace_id = $1::uuid AND epoch = $3::bigint AND state = 'active' FOR SHARE;
-- reads:
SELECT 1 FROM namespace_ownership
 WHERE namespace_id = $1::uuid AND epoch = $3::bigint AND state IN ('active', 'frozen');
```

Zero rows means `FAILED_PRECONDITION` + `WrongShardOrEpoch` (or `NamespaceFrozen` when the row
exists with `state='frozen'`). Because `FOR SHARE` conflicts with the `FOR UPDATE` the freeze
step takes, a freeze waits for in-flight writers and no writer that started before the freeze
can commit after it (D5 safety argument).

#### 3.3.1 Shard-level tables

```sql
CREATE TABLE shard_meta (                         -- exactly one row (trigger-enforced); N22
  shard_id integer NOT NULL, schema_version integer NOT NULL, applied_at timestamptz NOT NULL DEFAULT now(),
  engram_min_version text NOT NULL DEFAULT '', engram_max_version text NOT NULL DEFAULT ''
);

CREATE TABLE namespace_ownership (                -- the fence (D2 row 4, D5); mirrors the catalog
  namespace_id      uuid PRIMARY KEY,
  tenant_id         text NOT NULL CHECK (tenant_id ~ '^[a-z0-9-]{1,64}$'),
  shard_id          integer NOT NULL,             -- the ONLY shard_id column on data tables; trigger-checked against shard_meta
  epoch             bigint NOT NULL CHECK (epoch >= 1),
  state             ownership_state NOT NULL,     -- incoming|active|frozen|moved_out
  move_id           uuid,
  move_applied_seq  bigint,                       -- target-side replay watermark (D5 step 3), folded in here
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (namespace_id, tenant_id),               -- FK target of every root table
  CHECK (state <> 'incoming' OR move_id IS NOT NULL)
);
CREATE TRIGGER namespace_ownership_check BEFORE INSERT OR UPDATE ON namespace_ownership
  FOR EACH ROW EXECUTE FUNCTION engram_check_ownership();   -- shard_id = shard_meta.shard_id; epoch never decreases

CREATE TABLE move_applied (                       -- target-side exactly-once ledger of replayed events
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, seq bigint NOT NULL, move_id uuid NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, seq), FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);

CREATE TABLE namespace_stats (                    -- counters; separate from ownership so FOR SHARE never blocks them
  namespace_id uuid PRIMARY KEY, tenant_id text NOT NULL,
  live_facts bigint NOT NULL DEFAULT 0, total_facts bigint NOT NULL DEFAULT 0, live_chunks bigint NOT NULL DEFAULT 0,
  live_documents bigint NOT NULL DEFAULT 0, live_observations bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0,
  recalls_1h bigint NOT NULL DEFAULT 0, retains_1h bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
) WITH (fillfactor = 70);
```

`move_applied_seq` is folded into `namespace_ownership` (one value per namespace and move)
and `move_applied` keeps the per-event ledger, so a replay that arrives out of order (a source
gap that resolves late, 3.5) is applied exactly once by `INSERT ... ON CONFLICT DO NOTHING
RETURNING seq`. Rejected: a separate `move_state` table (one more row to copy and to reason
about); the watermark alone (an out-of-order late event would be skipped).

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
  occurred_start timestamptz, occurred_end timestamptz, mentioned_at timestamptz NOT NULL,
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
CREATE INDEX facts_occurred_gist      ON facts USING gist (namespace_id, tstzrange(occurred_start, occurred_end, '[]'))
                                      WHERE live AND occurred_start IS NOT NULL;            -- btree_gist for the uuid
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
time.

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
to the survivor without rewriting history; `EntityMerged` re-points `entity_mentions` rows.

#### 3.3.6 Observations, consolidation, pages

```sql
CREATE TABLE observations (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL,
  current_version integer NOT NULL DEFAULT 0, proof_count integer NOT NULL DEFAULT 0,
  stale boolean NOT NULL DEFAULT false, stale_since timestamptz,
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),       -- consolidation scope tags
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz,
  PRIMARY KEY (namespace_id, observation_id),
  FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id),
  CHECK (stale = (stale_since IS NOT NULL))
);

CREATE TABLE observation_versions (               -- D9: versioned beliefs; recalled through semantic + lexical arms (D7, D10)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  ov_id uuid NOT NULL,                            -- surrogate: pg_search key_field must be a single UNIQUE column
  observation_id uuid NOT NULL, version integer NOT NULL CHECK (version >= 1),
  text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 8192),
  embedding halfvec(768) NOT NULL, embedding_model text NOT NULL,
  effective_at timestamptz NOT NULL,              -- max(mentioned_at) over cited sources, clamped >= previous version (section 5)
  superseded_at timestamptz,                      -- min(effective_at) over later versions; NULL for the newest
  source_count integer NOT NULL CHECK (source_count >= 1),
  tags text[] NOT NULL DEFAULT '{}', tag_count smallint GENERATED ALWAYS AS ((cardinality(tags))::smallint) STORED,
  prompt_version text NOT NULL, model text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  retired_at timestamptz, live boolean GENERATED ALWAYS AS (retired_at IS NULL) STORED,
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
```

The orphan trigger is statement-level with a transition table: one statement that deletes
10,000 source rows runs three `UPDATE`s, not 10,000 row triggers. It retires every
observation (and its versions) left with zero sources and marks `stale` every observation that
lost at least one source (D8, D12). It fires for explicit deletes and for the FK cascade from
`facts` alike, so the invariant "an observation never outlives its sources" holds whichever
path removed the citation; verified: deleting one source shared by two observations retires
the one left empty and stales the other. The trigger does not write outbox events (a plpgsql
trigger cannot encode the protobuf payload); the cascade reads the affected observations back
and emits `ObservationRetired` for the ones whose `retired_at` is now set (3.8).

`superseded_at` is the only denormalisation that D9 needs: "the latest version with
`effective_at <= T`" equals "`effective_at <= T` and no later version has `effective_at <= T`",
and the latter is a plain filter once `superseded_at = min(effective_at of later versions)` is
stored (maintained by one `UPDATE ... WHERE version < $new` when a version is inserted). Both
HNSW and BM25 scans over `observation_versions` can then apply the `as_of` rule inside the
scan. `tags` and `retired_at` are copied from `observations` for the same reason (single-table
arms).

```sql
CREATE TABLE consolidation_batches (              -- D12 exactly-once
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  batch_key bytea NOT NULL CHECK (octet_length(batch_key) = 32),          -- sha256(sorted memory ids || prompt_version || model)
  round_id uuid NOT NULL, memory_ids uuid[] NOT NULL CHECK (cardinality(memory_ids) BETWEEN 1 AND 8),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','applied','bisected','failed')),
  attempts integer NOT NULL DEFAULT 0, model text NOT NULL, prompt_version text NOT NULL,
  result_blob_key text, error text, created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
  PRIMARY KEY (namespace_id, batch_key), FOREIGN KEY (namespace_id, tenant_id) REFERENCES namespace_ownership (namespace_id, tenant_id)
);
CREATE TABLE consolidation_applied (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  op_key bytea NOT NULL CHECK (octet_length(op_key) = 32),                -- sha256(batch_key || op_index)
  batch_key bytea NOT NULL, op_index integer NOT NULL, op_kind text NOT NULL CHECK (op_kind IN ('create','update','delete')),
  observation_id uuid NOT NULL, version integer, applied_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, op_key), FOREIGN KEY (namespace_id, batch_key) REFERENCES consolidation_batches (namespace_id, batch_key)
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

#### 3.3.7 Operations, idempotency, metering, tombstones, exports

```sql
CREATE TABLE operations (
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, operation_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind IN ('retain','delete_document','delete_namespace','consolidate','reflect','page_refresh','export','purge','move')),
  state operation_state NOT NULL DEFAULT 'PENDING',   -- PENDING|RUNNING|DEFERRED|SUCCEEDED|SUCCEEDED_WITH_ERRORS|FAILED|CANCELLED
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
  CHECK ((state IN ('SUCCEEDED','SUCCEEDED_WITH_ERRORS','FAILED','CANCELLED')) = (finished_at IS NOT NULL))
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
CREATE TABLE export_snapshots (namespace_id, tenant_id, version, state CHECK IN ('building','ready','expired','failed'), manifest_key,
  from_seq, to_seq, fact_count, observation_count, chunk_count, page_count, bytes, operation_id, created_at, completed_at, expires_at,
  PRIMARY KEY (namespace_id, version));
CREATE TABLE deletion_log (namespace_id, tenant_id, kind CHECK IN ('document','memory','namespace','tenant'), subject_id text,
  epoch, operation_id, deleted_at, PRIMARY KEY (namespace_id, kind, subject_id, deleted_at));                                 -- N21
```

`token_usage_events` is the per-call record (one row per gateway call, idempotent by
`usage_key`, so a retried activity never double-counts) and `token_usage` the daily aggregate
D13 names; the event table is what "cost per conversation" in section 8 is computed from.
`blob_tombstones` rows are inserted in the same transaction as the logical delete and removed
when the blob is gone, so the tombstone table is also the reconciliation worklist after a crash.

#### 3.3.8 RLS and grants

The policy is created by one loop over every table that has a `namespace_id` column, parents
and partitions alike, and a self-check at the end of the file fails the migration if any such
table lacks it, is not `FORCE`d, lacks `tenant_id`, or if any non-admin role holds a privilege on
a partition:

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
| `namespace_stats` | `namespace_id` | PK | yes | no | PK | |
| `outbox` | `seq` | yes (default from scope) | yes | no | `outbox_ns_seq_idx` | PK is `seq` (relay reads shard-wide in seq order) |
| `outbox_cursors` | `consumer` | no | no | no | none | shard-level; relay-owned |
| `outbox_skipped` | `namespace_id, consumer, seq` | PK | yes | no | PK | |
| `deletion_log` | `namespace_id, kind, subject_id, deleted_at` | PK | yes | no | PK | |
| `ingest_ledger` | `namespace_id, ledger_id` | PK | yes (FK) | no | PK, doc, op, hash | append-only |
| `documents` | `namespace_id, document_id` | PK | yes (FK) | no | PK, updated, deleting | |
| `document_versions` | `namespace_id, document_id, version` | PK | yes | no | PK, active (partial unique), op | |
| `document_version_chunks` | `namespace_id, document_id, version, content_hash` | PK | yes | no | PK, chunk | |
| `chunks` (16 partitions) | `namespace_id, chunk_id` | PK, partition key | yes | no | PK, (doc, hash) unique, doc, mentioned, purge | HNSW and BM25 are single-column access methods: isolation by partition pruning + in-scan `namespace_id` filter + RLS |
| `facts` (16) | `namespace_id, memory_id` | PK, partition key | yes | no | PK, (chunk, hash) unique, doc, mentioned, occurred (GiST), tags (GIN), unconsolidated, purge | HNSW, BM25 as above |
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

**Events** (`engram.internal.events.v1.Event { Envelope envelope = 1; oneof body { ... } }`,
envelope = `namespace_id, tenant_id, epoch, seq, occurred_at, operation_id`). Events are thin
(N12): ids, versions, hashes and flags, never text or vectors; a consumer that needs content
reads the rows by id under the recorded namespace scope.

| Event | Emitted by | Payload |
|---|---|---|
| `ChunkCommitted` | `CommitChunk` | `document_id, version, chunk_id, content_hash, memory_ids[], link_count, mention_count` |
| `ChunkRetired`, `ChunkReembedded` | `FinalizeVersion` | `document_id, version, chunk_ids[]` |
| `DocumentVersionActivated` | `FinalizeVersion` | `document_id, version, superseded_version` |
| `FactsRetired` | `FinalizeVersion`, delete cascade | `document_id, memory_ids[] (≤ 500 per event, part/parts)` |
| `FactInvalidated`, `FactRestored` | Invalidate/Restore | `memory_id, reason` |
| `FactsConsolidated` | consolidation apply | `batch_key, memory_ids[]` |
| `DocumentDeleted` | delete cascade | `document_id, version, memory_ids[], chunk_ids[], observation_ids_retired[], page_ids[]` plus the `deletion_log` key `(kind, subject_id, deleted_at)` |
| `ObservationCreated`, `ObservationVersionAdded`, `ObservationRetired` | consolidation, cascade | `observation_id, version, effective_at, source_count` |
| `PageStale`, `PageVersionCreated` | consolidation, cascade, refresh | `page_id, stale_seq / version, effective_at` |
| `EntityMerged`, `EntitiesPruned` | resolver, purge | `survivor_id, merged_ids[]` |
| `RowsPurged`, `BlobsDeleted` | purge | counts per table / keys |
| `OperationFinished` | every workflow end | `operation_id, state` |
| `NamespaceDeleted`, `RestoreMarker` | purge, restore | `deletion_log` key / `restored_to, epoch_bump` |

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

**How a move consumer reads one namespace.** The move executor connects as `engram_move`
with the moved namespace in scope and reads `WHERE namespace_id = $ns AND seq > $applied ORDER
BY seq LIMIT 500` on the index `(namespace_id, seq)`; RLS makes any other namespace invisible
to that role even if the predicate were wrong (section 8 `TestIso_Move_NamespaceOnly`). It
registers a `move:<ns>` cursor on the source so that retention cannot delete rows it has not
replayed, applies each event on the target with `INSERT INTO move_applied ... ON CONFLICT DO
NOTHING RETURNING seq` and advances `namespace_ownership.move_applied_seq`. Gaps cannot bite the
drain: the freeze `UPDATE` on the source ownership row waits for every in-flight `FOR SHARE`
writer, so once `frozen` has committed no lower `seq` for that namespace can appear, and
`max(seq)` for the namespace is final.

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
| Export snapshot | `export/v{n}/manifest.json`, `facts.jsonl.zst`, `observations.jsonl.zst`, `chunks.jsonl.zst`, `pages/*.md`, `delta-v{n-1}-v{n}.jsonl.zst` | D12 | versioned, immutable | `ExportWorkflow` | `export_snapshots.expires_at` → tombstones |
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
| `ingest_ledger` | 1.5 M | ≈ 3,800 (inline bodies) | 5.7 GB | 0.3 GB | 6 GB |
| `documents`, versions, membership | 1 M + 1.5 M + 1.5 M | 300 / 150 / 100 | 0.6 GB | 0.4 GB | 1 GB |
| `outbox` (7 d) | ≈ 1.2 M steady, 12 M during backfill | ≈ 250 | 0.3 to 3 GB | 0.1 to 0.6 GB | ≤ 3.6 GB |
| `token_usage_events` (30 d), operations, idempotency, stats, tombstones | ≈ 4 M | ≈ 150 | 0.6 GB | 0.3 GB | 1 GB |
| **Total** | | | | | **≈ 158 GB** (≈ 123 GB at 15 links per fact) |

Hot working set (what must stay in the 64 GB of RAM for the p95 budget): facts HNSW 18 GB,
BM25 4 GB, facts B-trees 3.7 GB, chunk and observation HNSW 3 GB, the recently touched half of
the `fact_links` PK ≈ 10 GB, ownership/stats/cursors negligible → ≈ 40 GB, served from
`shared_buffers = 16 GB` plus the OS page cache; the 300 GB volume holds the total with room
for one `REINDEX CONCURRENTLY` of the largest index (3.7 below).

Two numbers disagree with the register's D3 row and are reported in the closing notes rather
than silently changed: `fact_links` cannot be 25 GB at 300 M rows of three uuids (71 GB is the
floor with the PK and the reverse index; a denser row is impossible without abandoning UUIDv7
ids, which D1 fixes), and the register's 90 GB total omits the ledger (6 GB) and the
observation sources. The practical lever is the link cap (section 5): 15 links per fact brings
the shard to ≈ 123 GB, and the register's own "≈ 30/fact" is an assumption, not a measurement.

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
two partial indexes and marks the old tuple dead). `PurgeWorkflow` deletes physical rows in
batches of 1,000 (`WHERE namespace_id = $1 AND purge_after <= now() ... LIMIT 1000` over
`facts_purge_idx`, FK cascades take links, mentions and sources with them) with a 50 ms pause
between batches, so a 100 k-fact document delete spreads over ~10 s and never holds a long
transaction. HNSW entries of deleted rows are removed only by `VACUUM` (pgvector repairs the
graph in a second pass), so purge throughput is deliberately capped at 20 k rows/s per shard to
keep autovacuum ahead; `pgstattuple` on each partition's HNSW index runs hourly and `REINDEX
INDEX CONCURRENTLY` is scheduled per partition when dead fraction > 30 % (a partition's HNSW is
≈ 1.1 GB, which rebuilds in minutes inside `maintenance_work_mem = 4 GB`; an unpartitioned 18 GB
index would not fit and would fall back to pgvector's slower on-disk build). BM25 segments
merge in the background (`background_layer_sizes`, pg_search default); deleted documents stop
influencing scores after `VACUUM`, which the per-partition thresholds above trigger within
minutes of a large purge. Namespace delete and move cleanup use the same batched path under the
admin role; a whole-namespace purge of a 1 M-fact namespace takes ≈ 10 min and one vacuum cycle.
Transaction-id age is not a concern at these rates (`autovacuum_freeze_max_age` default,
≈ 2 × 10⁸ transactions per shard-year at 50 writes/s).

### 3.8 Query patterns the schema serves

| # | Query | Served by | Role |
|---|---|---|---|
| 1 | scope + ownership fence (every transaction) | `namespace_ownership` PK, `FOR SHARE` | app |
| 2 | semantic arm over facts (HNSW, as_of, tags, fact types) | `facts_embedding_hnsw` with iterative scan; partition pruned | app |
| 3 | lexical arm over facts (BM25 Top-K with pushed-down filters) | `facts_bm25` | app |
| 4 | graph expansion (bounded, from ≤ 20 seeds) | `fact_links` PK + `fact_links_reverse_idx`, `facts` PK for the as_of/live check | app |
| 5 | temporal arm (occurrence window overlap, ordered by distance to `query_timestamp`) | `facts_occurred_gist` | app |
| 6 | chunk arm (BM25 ∪ HNSW over chunks) | `chunks_bm25`, `chunks_embedding_hnsw` | app |
| 7 | observation arms (semantic + lexical over `observation_versions` with the D9 rule) | `observation_versions_embedding_hnsw`, `observation_versions_bm25` | app |
| 8 | retain upsert path (`CommitChunk`) | `chunks (document_id, content_hash)` unique, `facts (chunk_id, content_hash)` unique, `entities_canonical_uq`, PKs | app |
| 9 | delete cascade | `facts_doc_idx`, `fact_links` PK + reverse, `entity_mentions` PK, `observation_sources_memory_idx`, `page_sources_source_idx` | app |
| 10 | outbox relay read / cursor advance / retention | `outbox` PK (`seq`), `outbox_cursors` PK | relay / admin |
| 11 | move consumer read, target apply | `outbox_ns_seq_idx`, `move_applied` PK, `namespace_ownership` PK | move |
| 12 | entity resolution (fuzzy, per namespace) | `entities_trgm_idx` (GIN, uuid + trigram) | app |
| 13 | consolidation round selection, stale observations, stale pages | `facts_unconsolidated_idx`, `observations_stale_idx`, `pages_stale_idx` | app |
| 14 | `ListMemories`/`ListDocuments`/`ListOperations` keyset pagination, `GetMemory` | PKs and `*_created_idx`/`documents_updated_idx` | app |
| 15 | op-sweeper, DEFERRED resumer, idempotency expiry, purge sweep | `operations_sweeper_idx`, `operations_deferred_idx`, `idempotency_keys_expiry_idx`, `facts_purge_idx` per namespace | admin |

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
SET LOCAL hnsw.ef_search = 100;
SET LOCAL hnsw.iterative_scan = relaxed_order;
SET LOCAL hnsw.max_scan_tuples = 20000;
SELECT memory_id, document_id, chunk_id, mentioned_at, embedding <=> $2::halfvec(768) AS distance
  FROM facts
 WHERE namespace_id = $1
   AND live
   AND mentioned_at <= $3                               -- omitted when as_of is unset (D9)
   AND fact_type = ANY ($4::fact_type[])                 -- omitted when unset
   AND (cardinality(tags) = 0 OR tags && $q)             -- mode-specific, table above
 ORDER BY embedding <=> $2::halfvec(768)
 LIMIT 150;
```

Verified plan: `Index Scan using facts_p07_embedding_idx ... Order By: (embedding <=> $2)
Filter: (live AND namespace_id = ... AND mentioned_at <= ... AND tags && ...)` under RLS with
the other 15 partitions removed. With `relaxed_order` the scan keeps walking the graph until
150 rows pass the filter or `max_scan_tuples` is reached, which is what makes a selective
`as_of` or tag filter return a full candidate list instead of a truncated one.

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

**Observation arms** apply the D9 rule as a filter; the lexical form is the same shape as above
on `observation_versions_bm25`:

```sql
SELECT ov_id, observation_id, version, effective_at, embedding <=> $2::halfvec(768) AS distance
  FROM observation_versions
 WHERE namespace_id = $1 AND live
   AND effective_at <= $3 AND (superseded_at IS NULL OR superseded_at > $3)   -- as_of; unset: superseded_at IS NULL
   AND (cardinality(tags) = 0 OR tags && $q)
 ORDER BY embedding <=> $2::halfvec(768)
 LIMIT 150;
```

**Temporal arm** (`$4..$5` query window, `$6` `query_timestamp`):

```sql
SELECT memory_id, occurred_start, occurred_end
  FROM facts
 WHERE namespace_id = $1 AND live AND occurred_start IS NOT NULL
   AND tstzrange(occurred_start, occurred_end, '[]') && tstzrange($4, $5, '[]')
   AND mentioned_at <= $3
 ORDER BY abs(extract(epoch FROM (coalesce(occurred_end, occurred_start) - $6)))
 LIMIT 150;
```

The predicate is the partial GiST index's expression verbatim, so the index applies; an
open-ended `occurred_end` is an unbounded range.

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
SELECT unnest(visited) AS memory_id FROM hops ORDER BY hop DESC LIMIT 1;
```

The per-node cap (`$per_node` neighbours per frontier node) is applied by a window
`row_number() OVER (PARTITION BY origin ORDER BY weight DESC) <= $per_node` in the inner
subquery (elided above for width). Verified on a chain graph: budget 7 and 5 hops stop at
exactly 7 visited nodes. Cost per hop is one index range scan per frontier node on the PK and
one on the reverse index, plus a PK lookup per candidate on `facts`; at the mid budget (300
nodes, ≤ 5 hops) that is ≤ 3,000 index probes, ≈ 3 ms warm. Rejected: a Go-driven BFS with one
round trip per hop (5 to 7 round trips through pgbouncer at the p95 budget); a recursive CTE
without the array state (no way to enforce a global budget, exponential blow-up).

**Delete cascade** (D8; one fenced write transaction as `engram_app`; `$1` namespace, `$2`
document, `$t`/`$e`/`$op` tenant, epoch, operation):

```sql
SELECT pg_advisory_xact_lock(hashtextextended($1::text || '/' || $2, 0));      -- one cascade per document at a time
UPDATE documents SET state = 'deleting', deleted_at = now()
 WHERE namespace_id = $1 AND document_id = $2 AND state = 'active' RETURNING current_version;   -- 0 rows -> NOT_FOUND
WITH f AS (
  UPDATE facts SET retired_at = coalesce(retired_at, now()), purge_after = now()
   WHERE namespace_id = $1 AND document_id = $2 AND (purge_after IS NULL OR purge_after > now())
   RETURNING memory_id)
SELECT array_agg(memory_id) FROM f;                                              -- -> $3
DELETE FROM fact_links WHERE namespace_id = $1 AND (src_memory_id = ANY ($3) OR dst_memory_id = ANY ($3));
DELETE FROM entity_mentions WHERE namespace_id = $1 AND memory_id = ANY ($3);
WITH d AS (DELETE FROM observation_sources WHERE namespace_id = $1 AND memory_id = ANY ($3) RETURNING observation_id)
SELECT array_agg(DISTINCT observation_id) FROM d;                                -- -> $4; the trigger retires or stales them
SELECT observation_id, retired_at IS NOT NULL AS retired
  FROM observations WHERE namespace_id = $1 AND observation_id = ANY ($4);       -- for ObservationRetired events
WITH p AS (DELETE FROM page_sources WHERE namespace_id = $1 AND source_kind = 'fact' AND source_id = ANY ($3) RETURNING page_id)
UPDATE pages SET stale_delete = true, stale_seq = stale_seq + 1
 WHERE namespace_id = $1 AND page_id IN (SELECT page_id FROM p);
UPDATE chunks SET retired_at = coalesce(retired_at, now()), purge_after = now()
 WHERE namespace_id = $1 AND document_id = $2 AND (purge_after IS NULL OR purge_after > now());
UPDATE document_versions SET status = 'deleted', finished_at = coalesce(finished_at, now())
 WHERE namespace_id = $1 AND document_id = $2 AND status <> 'deleted';
UPDATE namespace_stats SET live_facts = live_facts - $n_facts, live_chunks = live_chunks - $n_chunks,
       live_documents = live_documents - 1 WHERE namespace_id = $1;
INSERT INTO deletion_log (namespace_id, tenant_id, kind, subject_id, epoch, operation_id)
VALUES ($1, $t, 'document', $2, $e, $op);
INSERT INTO blob_tombstones (namespace_id, tenant_id, tombstone_id, blob_key, reason, operation_id)
SELECT $1, $t, engram_uuid_v7(), k, 'document_delete', $op FROM unnest($blob_keys) AS k;   -- docsum, ledger/{sha256} if unshared
INSERT INTO operations (namespace_id, tenant_id, operation_id, kind, target_id, workflow_id, task_queue, submitted_epoch)
VALUES ($1, $t, $op, 'purge', $2, 'ns/' || $1 || '/op/' || $op, $queue, $e);
INSERT INTO outbox (event_type, payload) VALUES ('DocumentDeleted', $proto), ('FactsRetired', $proto2);
COMMIT;
```

After commit nothing from the document is reachable by any arm (`live` is false on every fact
and chunk; links, mentions and citations are gone), which is the D16 delete guarantee; the
purge workflow removes the physical rows and blobs afterwards. Invalidate/Restore are the
one-row variants (`facts.invalidated_at`, observations `stale`, pages `stale_delete`) described
in section 5.4.5.

**Retain upsert path** (`CommitChunk`, one fenced transaction per chunk, idempotent on re-execution):

```sql
INSERT INTO chunks (namespace_id, tenant_id, chunk_id, document_id, content_hash, header_hash, extraction_key, ordinal,
                    heading_path, header, text, tags, mentioned_at, embedding, embedding_model)
VALUES (...)
ON CONFLICT (namespace_id, document_id, content_hash) DO UPDATE
   SET retired_at = NULL, purge_after = NULL, ordinal = EXCLUDED.ordinal, header = EXCLUDED.header,
       header_hash = EXCLUDED.header_hash, embedding = EXCLUDED.embedding, extraction_key = EXCLUDED.extraction_key
RETURNING chunk_id, (created_at = now()) AS inserted;         -- xmax is not readable on a partitioned table
UPDATE facts SET retired_at = NULL, purge_after = NULL
 WHERE namespace_id = $1 AND chunk_id = $chunk AND extraction_key = $xkey AND retired_at IS NOT NULL;   -- un-retire
INSERT INTO facts (...) VALUES (...)                                -- only when inserted or no live facts under $xkey
ON CONFLICT (namespace_id, chunk_id, content_hash) DO NOTHING;
INSERT INTO entities (...) VALUES (...)
ON CONFLICT (namespace_id, canonical_norm) WHERE merged_into IS NULL
DO UPDATE SET mention_count = entities.mention_count + EXCLUDED.mention_count, last_seen_at = now()
RETURNING entity_id;
INSERT INTO entity_aliases (...) ON CONFLICT DO NOTHING;
INSERT INTO entity_mentions (...) ON CONFLICT DO NOTHING;
INSERT INTO fact_links (...) VALUES (...)                          -- rows pre-sorted by (src, dst, link_type): no deadlocks
ON CONFLICT DO NOTHING;
INSERT INTO document_version_chunks (namespace_id, tenant_id, document_id, version, content_hash, chunk_id)
VALUES (...) ON CONFLICT DO NOTHING;
UPDATE document_versions SET chunks_done = chunks_done + 1
 WHERE namespace_id = $1 AND document_id = $2 AND version = $v AND status = 'ingesting';
UPDATE namespace_stats SET live_facts = live_facts + $n, live_chunks = live_chunks + $inserted::int WHERE namespace_id = $1;
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

**Outbox relay** (`engram_relay`, direct connection): `SELECT seq, namespace_id, tenant_id,
epoch, event_type, payload FROM outbox WHERE seq > $hwm ORDER BY seq LIMIT 500`; gap probe
`SELECT seq FROM outbox WHERE seq = ANY ($gaps)`; `UPDATE outbox_cursors SET last_seq = $n,
gaps = $j, updated_at = now() WHERE consumer = $c`. Retention (admin): `DELETE FROM outbox WHERE
seq IN (SELECT seq FROM outbox WHERE seq < $min_cursor AND created_at < now() - interval '7 days'
ORDER BY seq LIMIT 10000)`.

**Move consumer** (`engram_move`): source `SELECT seq, event_type, payload FROM outbox WHERE
namespace_id = $1 AND seq > $applied ORDER BY seq LIMIT 500`; target `INSERT INTO move_applied
(namespace_id, tenant_id, seq, move_id) VALUES (...) ON CONFLICT DO NOTHING RETURNING seq` (no
row → already applied, skip) then the event's row-level apply, then `UPDATE namespace_ownership
SET move_applied_seq = $seq WHERE namespace_id = $1 AND state = 'incoming' AND
coalesce(move_applied_seq, 0) < $seq`.

**Entity resolution**: `SELECT entity_id, canonical_name, similarity(canonical_norm, $2) AS s
FROM entities WHERE namespace_id = $1 AND merged_into IS NULL AND canonical_norm % $2 ORDER BY s
DESC LIMIT 5` on `entities_trgm_idx`, after an exact `entity_aliases` PK probe.

**Schedulers** (admin, shard-wide): op-sweeper `SELECT namespace_id, tenant_id, operation_id,
workflow_id, task_queue FROM operations WHERE state = 'PENDING' AND workflow_started_at IS NULL
AND created_at < now() - interval '2 min' ORDER BY created_at LIMIT 100` on
`operations_sweeper_idx`; DEFERRED resumer on `operations_deferred_idx`; idempotency expiry
`DELETE ... WHERE expires_at < now()` in batches; the purge safety net iterates
`namespace_ownership` and probes `facts_purge_idx` per namespace rather than carrying a
shard-wide index on a 10 M-row table.

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
| `move_applied_seq` only (no `move_applied` table) | a late-resolved gap below the watermark would be skipped silently |
| Enum for `outbox.event_type` and `operations.kind` | adding an event or kind would be a migration on every shard; the proto is the source of truth |
| `tsvector` + `ts_rank_cd` instead of pg_search | not BM25 and no Top-K pushdown; kept as the documented lower-quality fallback (D7) |
| pg_search JSON `text_fields` configuration | the pre-0.25 syntax; cast-based configuration is what the pinned image documents and is parser-checked |
| `ledger/{ledger_id}` keys for oversized bodies | not content-addressed; identical re-sends would store the body twice (N7 fixes `ledger/{sha256}`) |
| Computing observation visibility at `as_of` with a correlated subquery | correct but kills Top-K pushdown and HNSW in-scan filtering; `superseded_at` is one column |
| Storing `shard_id` on every row | a constant column per database; a wrong value can only mean a mis-provisioned database, which the ownership trigger catches |
| A dedicated `engram` schema | no isolation benefit inside a per-shard database, and every sibling check and tool assumes `public` |

### 3.10 Notes for the other sections

Items this section settled that neighbouring sections should align to, and divergences found
while reading them:

- Section 5 uses `RETURNING (xmax = 0) AS inserted` on `chunks`; that is an error on a
  partitioned table. Use `RETURNING chunk_id, (created_at = now()) AS inserted` (3.8).
- Section 5 names `export_versions`, `operation_errors` and `ledger_tombstones`; this schema
  has `export_snapshots`, an `operations.error` JSON column, and no ledger tombstones (ledger
  rows are deleted by the purge path under `engram_admin`, so no tombstone is needed).
- Section 5's `scope_tags` on observations is `observations.tags` here, so every arm filters
  the same column name.
- Section 8's static RLS check should allow the `relay_read_all` policy on `outbox` only (D6
  N4 narrowed the relay to `outbox` and `outbox_cursors`; the deletion-log consumer reads the
  `deletion_log` key fields from the `DocumentDeleted`/`NamespaceDeleted` events).
- Section 9's `shard_meta` carries `shard_id` as well as the version columns; the ownership
  trigger depends on it, so provisioning must insert that row before the first namespace.
- D3's `fact_links ≈ 25 GB` and `≈ 90 GB total` are not reachable with the register's own id
  and link-count decisions; see 3.7 for the arithmetic (≈ 71 GB and ≈ 158 GB, or ≈ 123 GB at
  15 links per fact).
