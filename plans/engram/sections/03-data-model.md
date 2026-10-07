## 3. Data model

This section fixes the physical schema of the two kinds of database Engram runs, what lives in
blob storage, and the queries the schema is shaped for. The complete, runnable DDL is in
`plans/engram/sql/catalog_schema.sql` (control plane) and `plans/engram/sql/shard_schema.sql`
(one copy per shard). Both files parse under the PostgreSQL 16 grammar and apply cleanly to a
PostgreSQL 16.15 instance with pgvector 0.8.6, pg_trgm, btree_gin and btree_gist; the `USING
bm25` statements were parser-checked and written against the pg_search 0.25 syntax (the
`paradedb/paradedb:latest-pg16` image). The marker, visibility, ownership, index-procedure and
immutability behaviour described below was executed as `engram_app`, `engram_move` and
`engram_admin` against the applied shard schema (the BM25 arm with a `tsvector` stand-in). Excerpts
below are abbreviated; the SQL files are the source of truth and the section 9 migration
`0001_init.sql` is generated from them.

Conventions used throughout: ids are `uuid` holding UUIDv7 (D1); every timestamp is
`timestamptz`; epochs and outbox sequence numbers are `bigint`; hashes are `bytea` of exactly 32
bytes (SHA-256); vectors are `halfvec(768)`; enumerations that are closed sets are Postgres enums,
sets that grow (operation kinds, event names, reasons) are `text` with a `CHECK`.

### 3.1 Principles

1. **Content rows are immutable; mutable state is narrow; visibility is a read-time predicate
   (D22, N113).** Rows that carry vectors or BM25 text (`facts`, `chunks`, `observation_versions`,
   `page_versions`, the `*_vectors` side tables) and their evidence rows (`fact_links`,
   `entity_mentions`, `observation_inputs`, `observation_version_sources`, `page_version_inputs`)
   are **insert-only and purge-only**: no `UPDATE` path, no `retired_at`, `invalidated_at`,
   `live`, `tags` or `superseded_at` column, no generated column, `fillfactor = 100`. The only
   DML after the insert is a `DELETE` by the expunge (`engram_admin`). Everything that changes
   sits in a narrow table that stays HOT (`fillfactor` 50 to 70): `documents`, `observations`,
   `pages`, the markers, `namespace_stats`. A delete or an invalidation therefore writes one
   small marker row and rewrites nothing that an HNSW or BM25 index points at. Rejected: flags
   on the vectored row with a partial `WHERE live` HNSW (a retire still rewrites a 2.4 KB tuple
   and re-indexes it, and every later visibility change lands on the same row again).
2. **Keys lead with `namespace_id`.** Every primary key and every B-tree, GIN and GiST index of
   a namespace-scoped table starts with `namespace_id` (D2); a query for one namespace touches
   one contiguous key range and the planner prunes one of the 16 hash partitions. The only
   shard-wide indexes are the outbox PK and the scheduler partial indexes on `operations`.
   Rejected: `tenant_id` as the leading column (a tenant spans namespaces and nothing is
   addressed per tenant on the data path).
3. **Row-Level Security on every namespace-scoped table**, policy `ns_isolation`:
   `namespace_id = current_setting('engram.namespace_id')::uuid` as both `USING` and `WITH
   CHECK`, enabled *and forced*, applied to the 16 partitions as well as to the parents.
   `engram_app` is `NOBYPASSRLS`. A transaction that forgot to set the scope errors (an unset GUC
   is `42704`, the empty string left after `SET LOCAL` expires is `22P02`), it never leaks. No
   tag predicate and no non-leakproof operator runs under RLS: tags are resolved once per recall
   against `documents.tags` into an allowed-document set, and the trigram lookup goes through a
   `SECURITY DEFINER` function that re-checks the namespace (N116, N131, P-5). Rejected:
   application predicates only; per-namespace tables.
4. **Sixteen hash partitions by `namespace_id`** for the eight large tables (`facts`, `chunks`,
   `fact_links`, `entity_mentions`, `observation_versions` and the three vector tables). A
   namespace lives in exactly one partition of each, which is what lets **vector indexes be per
   namespace** (principle 5) and keeps per-partition `VACUUM` short. Rejected: one partition per
   namespace (150 to 1,000 relations, DDL per namespace); no partitioning.
5. **Vectors are insert-only side tables with one partial HNSW per namespace (N111, N112).**
   `fact_vectors`, `chunk_vectors` and `observation_version_vectors` are keyed by content id
   and embedding model; the arms read the namespace's current model. There is no shared HNSW:
   below 2,000 vectors an arm scans exactly (at most 2,000 rows, about 3.2 MB), at 2,000 the
   stats sweeper builds the namespace's partial index through `engram_hnsw_ddl` (3.3.4), below
   1,000 it drops it. A query then visits only its own graph, a namespace delete or move cleanup
   is `DROP INDEX` plus batched `DELETE` with no graph repair, and a document purge dirties one
   small index.
6. **Deletion is a marker; physical work is the expunge (N115, N119).** `DeleteDocument` is one
   `document_tombstones` row, `documents.state = 'deleting'`, one `deletion_log` row and one
   outbox event, committed in milliseconds at any size after the intent object is durable
   (N122). `Invalidate` inserts a `fact_hidden` row and `Restore` deletes it. Every read path
   applies the visibility predicate of 3.8 at ack; a throttled per-namespace `Expunge` workflow
   materialises derived hiding, then purges rows in batches, then rebuilds touched indexes. Deletes
   are rare; recall SLOs may degrade for a namespace while markers are `pending` (3.8).
7. **Derived versions carry evidence segments, not lineage (N117).** `root_version` is immutable
   on `observation_versions` and `page_versions`; a version's derivation set is the inputs of
   its segment; hiding is a read-time `EXISTS` over those inputs plus the permanent
   `derived_hidden` materialisation. Nothing is computed at delete time, so no snapshot can be
   stale and no walk has a depth bound to fail open.
8. **`tenant_id` is denormalised on every row** and the root tables carry `FOREIGN KEY
   (namespace_id, tenant_id) REFERENCES namespace_ownership` (blob keys, metering, exports and
   the deletion log need the tenant without a join; about 4 GB at 300 M `fact_links` rows).
   **Epoch appears on `namespace_ownership` and `outbox` only**, and `shard_id` on no data row
   (`shard_meta` is the identity; a self-check fails the migration if another table grows it). No
   identity or idempotency key includes the epoch (D11).
9. **Three disjoint lock key spaces (N113):** namespace fence `hashtextextended(ns, 0)`,
   derivation lock `hashtextextended(ns, 1)` (one-argument form), document lock
   `(hashtext(ns), hashtext(doc))` (two-argument form). Writers never wait for the fence; every
   exclusive taker makes one 35 s attempt (N82).
10. **Durability is local (N122).** Every role runs `synchronous_commit = local`; no synchronous
    standby exists, so a commit cannot hang and the relay's 60 s gap horizon is sound. Acknowledged
    deletes and invalidations are protected by an intent object in blob storage written before
    the marker transaction, not by replication.

Mutability, per table group:

| Group | Class | Physical delete |
|---|---|---|
| `facts`, `chunks`, `fact_links`, `entity_mentions`, `observation_versions`, `page_versions`, `fact_vectors`, `chunk_vectors`, `observation_version_vectors`, `observation_inputs`, `observation_version_sources`, `page_version_inputs`, `document_version_chunks` | insert-only (grants + `BEFORE UPDATE` trigger), `fillfactor = 100` | expunge only, `engram_admin`; FK cascades from `facts` and `chunks` take vectors, links, mentions, inputs and sources |
| `observation_version_meta`, `page_version_meta`, `fact_consolidation`, `consolidation_proposals`, `consolidation_applied`, `curation_log`, `deletion_log`, `token_usage_events`, `ingest_ledger` | insert-only (write-once or append-only) | retention jobs; ledger only by an explicit delete expunge (N104) |
| `documents`, `observations`, `pages`, `entities`, `document_versions`, `consolidation_state`, `consolidation_batches`, `observation_sources`, `page_sources`, `operations`, `export_snapshots`, `batch_jobs` | mutable, narrow, `fillfactor` 70 | expunge / namespace delete |
| `document_tombstones`, `chunk_tombstones`, `fact_hidden`, `derived_hidden`, `expunge_progress`, `vector_indexes`, `namespace_models`, `namespace_stats` | small mutable marker and state tables, `fillfactor` 50 to 70 | markers: `Restore`, un-retire, the expunge's finish step |
| `namespace_ownership`, `ownership_transitions` | state-machine transitions only (3.3.1); the machine is immutable data | `active` and `moved_out` rows are never deleted |
| `outbox`, `outbox_cursors`, `outbox_skipped`, `idempotency_keys`, `token_usage`, `quota_counters`, `blob_tombstones` | log and hot counters | retention jobs |

### 3.2 Catalog schema (control-plane database `engram_catalog`)

One small PostgreSQL 16 with a streaming replica (D4), schema `public`, no RLS (only service
roles connect). Tables: `cells`, `tenants`, `shards`, `namespaces`, `namespace_moves`,
`idempotency_keys` (catalog-level writes, D1), `tenant_usage_daily` and `catalog_events` with the
`LISTEN/NOTIFY` trigger. There is no catalog delete log: delete intents are blob objects (N122).
Roles: `catalog_migrate` (owner), `catalog_app` (engram-api), `catalog_admin` (engramctl,
MoveService, ShardService).

```sql
CREATE TABLE shards (
  shard_id              integer PRIMARY KEY CHECK (shard_id >= 0),       -- dense int32 (D1)
  cell_id               text NOT NULL REFERENCES cells (cell_id),
  state                 shard_state NOT NULL DEFAULT 'provisioning',    -- provisioning|active|full|draining|readonly|retired
  pgbouncer_addr        text NOT NULL,  direct_addr text NOT NULL,       -- direct: relay election lock, move, engramctl
  dsn_secret_ref        text NOT NULL,  blob_prefix text NOT NULL,  blob_cred_secret_ref text NOT NULL,
  task_queue            text NOT NULL,  kafka_topic text NOT NULL,
  system_identifier     bigint, timeline_id integer,   -- N123: written on PROMOTION, before the virtual endpoint flips
  dedicated_tenant_id   text REFERENCES tenants (tenant_id),            -- NULL = shared pool
  max_namespaces integer NOT NULL DEFAULT 150, soft_cap_facts bigint NOT NULL DEFAULT 10000000,
  hard_cap_facts bigint NOT NULL DEFAULT 20000000, namespaces_count integer NOT NULL DEFAULT 0,
  facts_estimate bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0, ...
);

CREATE TABLE namespaces (
  namespace_id      uuid PRIMARY KEY,                           -- UUIDv7 minted by engram-api
  tenant_id         text NOT NULL REFERENCES tenants (tenant_id),
  name              text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  shard_id          integer NOT NULL REFERENCES shards (shard_id),
  epoch             bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
  state             namespace_state NOT NULL DEFAULT 'creating', -- creating|active|moving|frozen|restoring|deleting|deleted
  embedding_model   text NOT NULL DEFAULT 'nomic-embed-text-v1.5', embedding_dims integer NOT NULL DEFAULT 768,   -- N111: fixed at creation; a change is ReembedNamespace
  config jsonb NOT NULL DEFAULT '{}', profile jsonb NOT NULL DEFAULT '{}',
  large             boolean NOT NULL DEFAULT false,              -- mirror of the shard's namespace_stats.large: >= 2,000 vectors, per-namespace HNSW exists (N112)
  facts_estimate bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0, ...
);
CREATE UNIQUE INDEX namespaces_tenant_name_uq ON namespaces (tenant_id, name) WHERE state <> 'deleted';

CREATE TYPE move_state AS ENUM ('planned', 'copying', 'frozen', 'reconciling', 'cutover', 'cleaning', 'done', 'rolled_back');
CREATE TABLE namespace_moves (
  move_id uuid PRIMARY KEY, namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  source_shard_id integer NOT NULL, target_shard_id integer NOT NULL,
  from_epoch bigint NOT NULL, to_epoch bigint NOT NULL CHECK (to_epoch = from_epoch + 1),
  state move_state NOT NULL DEFAULT 'planned',
  source_system_id bigint, source_timeline_id integer,   -- N123: re-read on every source activity; mismatch -> MoveFenced
  t_copy timestamptz,                                    -- N124: the reconcile re-copies rows with created_at >= t_copy - 10 min
  terminated_workflows text[] NOT NULL DEFAULT '{}', error text, created_by text NOT NULL, ...,
  frozen_at timestamptz,
  ready_at timestamptz,        -- (b') target incoming -> ready; rollback still possible
  moved_out_at timestamptz,    -- (c) the point of no return
  activated_at timestamptz,    -- (b'') target ready -> active
  finished_at timestamptz
);
CREATE UNIQUE INDEX namespace_moves_live_uq ON namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back');                   -- at most one live move per namespace
```

`namespace_moves` has no outbox position: a move never reads or replays the outbox (N124).
It is also the arbiter of restore and failover: a restored or promoted shard completes or
aborts every open move that names it from this row before it accepts traffic (N123, 3.3.1).

**Config inheritance.** `tenants.config` and `namespaces.config` are `jsonb` objects holding the
keys of D12 (`models.*`, `chunk.target_chars`, `recall.default_budget`, `quota.*`). The database
checks only that each is an object; key validation is the Go config schema's job, and the
resolver caches the deep-merged `system ⊂ tenant ⊂ namespace` result with the catalog entry.
`namespaces.profile` is identity, not configuration, and is never inherited. The embedding model
is the exception that is *not* a config key: it is a column fixed at creation (N111).

**Placement policy.** `pick_shard(p_tenant_id)` runs inside the `CreateNamespace` transaction.
`namespaces_count` is **derived there** from the `namespaces` table with a lateral `count(*)`
(N131, P-16): a counter that is only incremented drifts after deletes and moves. Least-loaded
first by the facts ratio, then the derived namespace count, then id; `FOR UPDATE SKIP LOCKED`
lets concurrent creates pick different shards. `facts_estimate` and `large` are refreshed hourly
from each shard's `namespace_stats` by `engramctl stats`; neither invalidates the resolver cache
(the catalog trigger skips stats-only updates). Rejected: random placement; power-of-two choices.

`CreateNamespace` is two-phase across the catalog and the shard (catalog row `creating` → shard
`namespace_ownership` row `active`/epoch 1, `namespace_stats` and `namespace_models` rows,
inserted by `engram_app` with the new namespace in scope → catalog row `active`). A crash between
the phases leaves a `creating` row; the per-shard op-sweeper completes it when the shard row
exists and otherwise deletes both (`namespaces_creating_idx`, rows older than 2 min), and a client
retry replays through `idempotency_keys` exactly like Retain.

**Events and cache invalidation.** `catalog_events` is append-only; `AFTER INSERT` it
`pg_notify('catalog_changes', json)` with `{event_id, kind, namespace_id, tenant_id, shard_id,
epoch, state}` (D4). Triggers on `namespaces`, `tenants` and `namespace_moves` insert an event for
every routing-relevant change and skip stats-only updates. Retention is 30 days; the table doubles
as the audit log of every move and epoch bump. `tenant_usage_daily(tenant_id, day, quota_key,
used)` is the cross-shard rollup that tenant-level daily quotas need; workers report per-minute
deltas through the API (off the hot path, D4).

**Cutover does not depend on the catalog (N125).** The order is (b′) the target ownership row goes
`incoming → ready` (nothing routes to it); (c) the source row goes `frozen/move → moved_out` and
carries `target_shard_id` and `target_epoch` (the point of no return, `moved_out_at`); (b″) the
target row goes `ready → active`; (d) the catalog flip, retried indefinitely and idempotently, on
no read path. The API routes a request that hits the `moved_out` row to the target using only the
`WrongShardOrEpoch{MOVED_OUT}` detail and still verifies at the target's fence, so a catalog
failover causes no read outage beyond the bounded re-resolve loop. Between (c) and (b″) there is
no owner at all, the simplest way to make "at most one writable owner" true; (b″) is sub-second
and retried forever. A failure before (c), including a target outage after (b′), rolls back
(3.3.1) and thaws the source; safe because nothing routes to the target before (c).

### 3.3 Shard schema (identical on every shard)

Schema `public` (what the section 8 `--check-rls` query and the section 9 tooling assume),
PostgreSQL 16, extensions `vector`, `pg_search`, `pg_trgm`, `btree_gin`, `btree_gist`.

**Roles** (all `LOGIN`; passwords from the shard secret, section 9). All of them run
`synchronous_commit = local` as a role default (N122).

| Role | RLS | Grants | Used by |
|---|---|---|---|
| `engram_migrate` | owner (`BYPASSRLS`) | DDL; owns the `SECURITY DEFINER` functions `engram_cleanup_namespace` (N93) and `engram_entity_fuzzy` (N131) | migrations, `engramctl index` |
| `engram_app` | `NOBYPASSRLS`, `ns_isolation`; `lock_timeout = 2 s`, `statement_timeout = 30 s` | `SELECT, INSERT` on every content table, `ingest_ledger`, `outbox`, `deletion_log`; DML on mutable tables; `SELECT, INSERT` on `document_tombstones`; `SELECT, INSERT, DELETE` on `chunk_tombstones`, `fact_hidden`; `SELECT, DELETE` on `derived_hidden`; **no `UPDATE` or `DELETE` on any content row**; `SELECT` on `namespace_ownership` (the fence is a plain read), `INSERT` of the first `active`/epoch-1 row, `UPDATE (state, freeze_reason)` for the one edge the trigger admits, the delete freeze | API and worker per-namespace transactions |
| `engram_relay` | `NOBYPASSRLS` + policy `relay_read_all` on `outbox` | `SELECT` on `outbox`; all on `outbox_cursors`; nothing else | outbox relay, direct connection |
| `engram_move` | `NOBYPASSRLS`; restrictive `move_target_*` policies: writes only into an `incoming` namespace | content tables: `SELECT, INSERT`; mutable tables and markers: full DML; the ownership transitions of 3.3.1; `SELECT` on `outbox`, `outbox_cursors` (relay drain); `SET session_replication_role`; `EXECUTE engram_cleanup_namespace`. No `DELETE` on content, no outbox or cursor write | move executor |
| `engram_admin` | `BYPASSRLS` | all tables; the **only** role that `DELETE`s content rows; `engram_consumers_passed` | `engramctl`, Expunge purge activities, shard-wide schedulers, retention |

**Transaction scope and fence** (D2 row 3, N82, N113). Every store transaction starts with

```sql
SELECT set_config('engram.namespace_id', $1, true),
       set_config('engram.tenant_id',    $2, true),
       set_config('engram.epoch',        $3, true);           -- SET LOCAL, pgbouncer-safe
-- writes: the advisory lock is the fence LOCK and NEVER WAITS (N82); the row is the fence VALUE
SELECT engram_try_ns_fence($1::uuid);   -- pg_try_advisory_xact_lock_shared(hashtextextended(ns::text, 0))
--   false -> NamespaceFrozen{retry_after = 200 ms} (retryable)
SELECT state, epoch, freeze_reason FROM namespace_ownership WHERE namespace_id = $1::uuid;   -- plain SELECT, no row lock
--   proceed iff state = 'active' AND epoch = $3
--   frozen/move, frozen/restore -> NamespaceFrozen{retry_after}; frozen/delete -> PreconditionFailed{NAMESPACE_DELETING}
--   incoming, ready -> NamespaceNotReady (retryable); moved_out, no row, epoch <> $3 -> WrongShardOrEpoch (moved_out carries target_shard_id, next_epoch)
-- reads: no lock, state only, the epoch is not checked
--   proceed iff state = 'active' OR (state = 'frozen' AND freeze_reason = 'move')      (N122, N125)
--   frozen/delete and frozen/restore are rejected; moved_out -> WrongShardOrEpoch{MOVED_OUT}, the API re-resolves in a bounded loop
```

**Lock discipline (N82, N113, N120).** No row is ever row-locked for fencing (`FOR SHARE` lets
compatible lockers pass a waiting `FOR UPDATE` and churns multixacts; immutable facts cannot be
locked meaningfully). Writers never wait: a refused `pg_try_advisory_xact_lock_shared` ends the
statement immediately, and the test `TestFence_TryLockRefusedBehindWaiter` pins the lock-manager
behaviour (verified here: while an exclusive request held the namespace key, the writer's try-lock
returned `false` at once and the derivation key was unaffected). Exclusive takers make **one**
attempt under `lock_timeout = 35 s`, longer than any legal 30 s writer, so a writer cannot
starve them and they cannot start a retry storm. The three key spaces are distinct `pg_locks`
tags (verified: one-argument keys have `objsubid = 1` with different `classid/objid`, the document
key `objsubid = 2`).

| Party | Lock | Then |
|---|---|---|
| every write transaction | namespace key, shared try-lock (`engram_try_ns_fence`) | refusal → `NamespaceFrozen`; else the ownership read above |
| **marker transactions** (delete, invalidate, restore) | the shared namespace fence only; document key for `DeleteDocument` | milliseconds; no derivation lock: markers are correct without one because visibility is read-time (N120) |
| freeze (move), delete freeze, restore | namespace key, exclusive, one 35 s attempt (`engram_ns_fence_exclusive`) | the ownership `UPDATE` of 3.3.1 |
| `ApplyBatch` stage 2, `PageRefresh` commit | derivation key, shared try-lock (`engram_try_derivation_lock`) | refusal → retry |
| `Expunge.Materialize` | derivation key, exclusive, one 35 s attempt (`engram_derivation_lock_exclusive`) | waits for writers whose re-verification predates a marker but whose commit follows it |
| `CommitChunk` | document key, shared try-lock | refusal → retryable `DocumentBusy` (100 ms) |
| `FinalizeVersion`, `DeleteDocument`, expunge purge of a document | document key, exclusive | wait for in-flight commits of the document |
| exports, reads | none | `REPEATABLE READ` for exports |

Why a freeze cannot lose a write: a write commits only inside a transaction that held the shared
lock while it read `active` at its epoch; the freeze takes the exclusive lock, so it starts after
every such transaction has finished and no writer that read `active` before the freeze can commit
after it (D5, TLC `ShardMove`). Why the derivation lock exists: Materialize must see every
version whose inputs name the victim; a writer whose re-verification predates the marker but whose
commit postdates it holds the shared lock across both, so Materialize waits for it (N120;
`Derivation.tla` has a configuration without the lock that must fail).

#### 3.3.1 Shard-level tables

`shard_meta` (one row: `shard_id`, `schema_version`, version window), `outbox_cursors` (consumers
`relay`, `index`, `kafka`; moves never touch it), `outbox`, `outbox_skipped`, `deletion_log`,
`namespace_ownership`, `ownership_transitions`, and the per-namespace state tables
`namespace_stats`, `namespace_models`, `vector_indexes` (these three have a `namespace_id` and RLS).

```sql
CREATE TABLE namespace_ownership (
  namespace_id uuid PRIMARY KEY, tenant_id text NOT NULL, shard_id integer NOT NULL,
  epoch bigint NOT NULL CHECK (epoch >= 1),
  state ownership_state NOT NULL,                -- incoming|ready|active|frozen|moved_out
  freeze_reason text CHECK (freeze_reason IN ('move', 'delete', 'restore')),
  move_id uuid, move_epoch bigint,               -- source: set by start_move (pauses expunge and schedulers); target: set by the incoming row
  target_shard_id integer, target_epoch bigint,  -- carried by a moved_out row: a PERMANENT fence value, never deleted
  ..., UNIQUE (namespace_id, tenant_id),
  CHECK (state NOT IN ('incoming', 'ready') OR move_id IS NOT NULL),
  CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL)),
  CHECK ((state = 'moved_out') = (target_shard_id IS NOT NULL)), CHECK ((target_shard_id IS NULL) = (target_epoch IS NULL)),
  CHECK (target_shard_id IS NULL OR target_shard_id <> shard_id), CHECK (move_epoch IS NULL OR move_id IS NOT NULL)
);
```

The `move_epoch > epoch` CHECK of the old design is gone (a restore aborts the move before the
epoch changes), and no CHECK relates `target_epoch` to `epoch`: after `return_abort` the row's
epoch may exceed the restored hint.

**Ownership state machine (N101, N125, N123, N93).** Written once here and loaded as the rows of
the immutable table `ownership_transitions` (`edge, role_name, from_state/reason, to_state/reason,
epoch_rule, move_effect, target_effect`, unique `NULLS NOT DISTINCT` on the first four columns).
`engram_check_ownership` enforces it: an `INSERT`, `UPDATE` or `DELETE` of an ownership row is
accepted only if it matches an edge for `current_user`, then the epoch rule and the move/target
column effects are checked. A state pair that exists for no role is `23514`; a pair that exists
only for other roles is `42501`; an edge of this role whose epoch rule or column effects are
violated is `23514`. T3 `TestIso_Ownership_Transitions` executes every row and has one negative
test per forbidden role × state pair; the statements of §5.5 are generated from this table.

| Edge | From → to | Role | Notes |
|---|---|---|---|
| `create` | ∅ → `active`, epoch 1 | `engram_app`, `engram_admin` | `CreateNamespace` |
| `plan_target` | ∅ → `incoming` (e+1, `move_id`) | `engram_move` | move plan on the target |
| `start_move` / `abort_move` | `active` → `active` (opens / closes `move_id`, `move_epoch = e+1`) | `engram_move` (`abort_move` also `engram_admin`) | pauses Expunge and schedulers; **rollback before the freeze** |
| `freeze_move` / `thaw_move` | `active` ↔ `frozen/move` | `engram_move` (`thaw_move` also `engram_admin`) | reads continue; **rollback after the freeze** |
| `freeze_delete` | `active` → `frozen/delete` | `engram_app` | before the ack of a namespace delete (N122); no outgoing edge except deletion |
| `freeze_restore` / `restore_done` | `active` → `frozen/restore` → `active` (epoch `greater`) | `engram_admin` | new epoch = catalog epoch + 1, written to the catalog first (N123) |
| `epoch_bump` | `active` → `active`, epoch `greater` | `engram_admin` | failover |
| `ready_target` | `incoming` → `ready` | `engram_move` | (b′): nothing routes to `ready` |
| `cutover_c` | `frozen/move` → `moved_out` (`target_shard_id`, `target_epoch = e+1`) | `engram_move` | (c) **point of no return**; clears `freeze_reason` |
| `activate_target` | `ready` → `active` (closes `move_id`) | `engram_move` | (b″) |
| `unready_target` | `ready` → `incoming` | `engram_move` | **rollback after (b′)** |
| `rollback_target` | `incoming` → (deleted); admin also `ready` | `engram_move`, `engram_admin` | data rows first, through `engram_cleanup_namespace` |
| `return_move` | `moved_out` → `incoming`, epoch > stored | `engram_move` | a later move back; refused (`55006`) while data rows remain |
| `return_abort` | `incoming` → `moved_out` (hint restored from `namespace_moves`) | `engram_move` | **rollback onto a shard that had a `moved_out` row**: the permanent fence value returns (H-22) |
| `reconcile_out` | `active`, `frozen/move`, `frozen/restore` → `moved_out(target, e+1)` | `engram_admin` | restore/failover when the catalog shows (c) done (N123) |
| `purge_deleted` | `frozen/delete` → (deleted) | `engram_admin` | after `NamespacePurged` |
| — | `active`, `moved_out` → (deleted); any other edge | no role | refused; the row is the fence value a late writer must still hit |

Executed against the DDL as the three roles (selected): `engram_app` INSERT at epoch 2 refused
("must start at epoch 1"); `engram_move` `start_move` → `freeze_move` legal, `freeze_reason =
'delete'` refused, `engram_app` thaw refused (`42501`), `thaw_move` legal; `active → moved_out`
refused for `engram_move`, then `freeze_move` → `cutover_c` legal, `DELETE` of the `moved_out` row
refused; `return_move` that leaves the epoch unchanged refused, with epoch 3 (above the stored `target_epoch` 2) legal, then
`return_abort` legal; target side `plan_target` → `ready_target` → `DELETE` refused → `unready_target`
→ `rollback_target` legal, `engram_app` `ready → active` refused (no privilege); `engram_admin`
`reconcile_out` legal where `engram_move` is refused.

The loader's column lists are `engram_copy_columns(table)`: attributes with `attgenerated = ''`
and `NOT attisdropped`, ordered by name, so both shards produce the same list (P-8; no generated
column exists in this schema and a self-check keeps it so). `engram_column_hash(table)` hashes the
`(name:type)` list, and Plan refuses unless the hashes of source and target match per table.

**Restore and failover (N123).** The restored or promoted shard comes up with `listen_addresses`
restricted. It first aborts or completes every open move that names it, with `namespace_moves` as
arbiter: if the catalog shows (c) done, the restored source row becomes `moved_out(target, e+1)`
(`reconcile_out`), otherwise the move is rolled back from the target (`rollback_target`) and the
source thawed (`thaw_move`, `abort_move` by admin). Only then does every row go `frozen/restore`,
`engramctl restore replay` apply the delete intents (N122), and `restore_done` bump the epoch.

**Schedulers** read `schedulable_namespaces` (`state = 'active'`, so the target of a move is
schedulable only after (c)); Expunge and the index sweeper read `purgeable_namespaces`, which also
excludes a namespace with an open move (`move_epoch IS NOT NULL`, Plan to done). Their writes to
`operations` and the outbox go through `WithNamespaceTx(write)` and `engramlint sql` checks the
fence prelude before every outbox insert (N131, H-20).

**Namespace stats are derived (N69).** `namespace_stats` is refreshed per namespace by the stats
sweeper (`engram_admin`): `live_facts` counts *visible* facts, `pending_markers` the open
tombstones, and `large` flips at 2,000 vectors of the current model (cleared below 1,000), which
the sweeper acts on through `engram_vector_index_plan` and `engram_hnsw_ddl` (3.3.4).
`namespace_models` holds the namespace's current `embedding_model` (N111) and `vector_indexes` the
per-namespace HNSW indexes that exist (`building|ready|dropping`).

#### 3.3.2 Outbox

```sql
CREATE SEQUENCE outbox_seq AS bigint CACHE 1;
CREATE TABLE outbox (
  seq bigint PRIMARY KEY DEFAULT nextval('outbox_seq'),
  namespace_id uuid NOT NULL DEFAULT current_setting('engram.namespace_id')::uuid,
  tenant_id text NOT NULL DEFAULT current_setting('engram.tenant_id'),
  epoch bigint NOT NULL DEFAULT current_setting('engram.epoch')::bigint,
  event_type text NOT NULL CHECK (event_type ~ '^[A-Z][A-Za-z0-9]{2,63}$'),
  payload bytea NOT NULL CHECK (octet_length(payload) <= 16384),    -- engram.internal.events.v1.Event
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_ns_seq_idx ON outbox (namespace_id, seq);
```

The PK is the global `seq` because the relay reads shard-wide in `seq` order; `namespace_id`,
`tenant_id` and `epoch` are stamped from the transaction scope. **Every event is bounded by
construction (N80):** ids are 16-byte `bytes`, an event carries at most 256 ids, a larger set is
paged as consecutive events with `page`/`page_count`, and above 4,096 ids the event carries counts
only with `ids_elided = true` (consumers then delete by the indexed `(namespace_id, document_id)`
query, N119). The `CHECK` is the backstop. A delete is **one O(1) marker event**
(`DocumentDeleted`), not a per-id list. The outbox `INSERT` is the last statement of every write
transaction (A-F1). `deletion_log(namespace_id, intent_key, kind, subject_id, epoch,
operation_id, deleted_at)` is the shard-local "already applied" record of marker transactions; its
key is the **name of the intent object** (N122), so `engramctl restore replay` applies each intent
at most once.

#### 3.3.3 Ingest ledger, documents, versions

`ingest_ledger` is append-only (trigger: `UPDATE` never; `DELETE` only for `engram_admin` and the
owner's cleanup function). Bodies up to 64 KiB are inline, larger ones live in the content-addressed
blob `ledger/{sha256}` written before the row (N7). Ledger rows are never purged by `REPLACE` or
`APPEND` retirement, only by an explicit delete expunge (N104).

```sql
CREATE TABLE documents (            -- MUTABLE, fillfactor 70
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL,
  current_version integer NOT NULL DEFAULT 0, state document_state NOT NULL DEFAULT 'active',   -- active|deleting|deleted
  item_timestamp timestamptz, context text NOT NULL DEFAULT '',
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),        -- the ONLY place tags live (N113)
  metadata jsonb NOT NULL DEFAULT '{}', document_hash bytea, summary_blob_key text, summary_hash bytea,
  ..., deleted_at timestamptz, PRIMARY KEY (namespace_id, document_id), CHECK ((state = 'active') = (deleted_at IS NULL))
);
CREATE INDEX documents_tags_gin ON documents USING gin (namespace_id, tags);

CREATE TABLE document_versions (    -- MUTABLE: status, chunks_done, body set once
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL, version integer NOT NULL,
  content_hash bytea NOT NULL, status version_status NOT NULL DEFAULT 'ingesting', update_mode update_mode NOT NULL,
  append_base_version integer CHECK (append_base_version IS NULL OR append_base_version < version),
  operation_id uuid NOT NULL, ledger_id uuid NOT NULL, chunk_count integer, chunks_done integer NOT NULL DEFAULT 0,
  body_hash bytea, body_key text,                                              -- H-16 / N104: NULLABLE until the body blob is stored
  ..., PRIMARY KEY (namespace_id, document_id, version),
  CHECK (body_key IS NULL OR body_hash IS NOT NULL),
  CHECK (body_key IS NULL OR body_key = 'ver/' || encode(body_hash, 'hex')),
  CHECK ((update_mode = 'append') = (append_base_version IS NOT NULL))
);
```

`LoadItem` creates the version row without a body, then stores the reconstructed body as the
content-addressed blob `ver/{sha256}` and sets `body_key`/`body_hash` `WHERE body_key IS NULL`;
`LoadItem(APPEND)` materialises the base body from the ledger chain up to the nearest version that
has one (`append_base_version`), under the document lock. At most one `active` version per
document (partial unique index). `document_version_chunks(…, content_hash, chunk_id, ordinal)` is
insert-only: it carries membership **and the chunk's position in that version**, because a chunk
row (immutable) cannot carry an ordinal that changes across versions.

**Delete versus retain.** `documents.state = 'deleting'` is set in the same transaction as the
tombstone; `Retain` of a `deleting` document is refused until the Expunge finishes (the document
row is deleted at the end of the purge, so the id is reusable; tombstones in state `purged` are
not part of any visibility set, which is what lets a re-created document be visible while its
24 h tombstone still exists).

**The per-document advisory lock is what `CommitChunk` relies on** (N40, N83): shared try-lock,
plain reads of the version row and `documents.current_version`, proceed only on `status =
'ingesting'`; `FinalizeVersion` and `DeleteDocument` take the key exclusive, so they wait for
in-flight commits and a later commit sees the terminal status. The retain ack's version assignment
keeps only the `documents` row lock.

#### 3.3.4 Chunks, facts and vectors (partitioned)

```sql
CREATE TABLE chunks (               -- INSERT-ONLY; identity = content hash within the document
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, chunk_id uuid NOT NULL, document_id text NOT NULL,
  content_hash bytea NOT NULL, header_hash bytea NOT NULL,                      -- header excluded from the identity (N6)
  heading_path text NOT NULL DEFAULT '', header text NOT NULL DEFAULT '',
  text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 16384),            -- <= 4,000 chars (D8)
  mentioned_at timestamptz NOT NULL,                                            -- N86: max(timestamp of every item the chunk covers)
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, chunk_id), UNIQUE (namespace_id, document_id, content_hash)
) PARTITION BY HASH (namespace_id);

CREATE TABLE facts (                -- INSERT-ONLY
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, memory_id uuid NOT NULL,
  document_id text NOT NULL, chunk_id uuid NOT NULL, ordinal smallint NOT NULL,
  content_hash bytea NOT NULL,                                                  -- sha256(normalised text); the curation_log key
  extraction_key bytea NOT NULL,                                                -- N87 key; a re-extraction inserts facts under a new key
  text text NOT NULL, fact_type fact_type NOT NULL, w5 jsonb NOT NULL DEFAULT '{}',
  occurred_start timestamptz, occurred_end timestamptz,
  mentioned_at timestamptz NOT NULL,                                            -- = item timestamp, SERVER-SET: the only as_of key (D9)
  said_at timestamptz, metadata jsonb NOT NULL DEFAULT '{}',
  extraction_version integer NOT NULL, prompt_version text NOT NULL, model text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, memory_id),
  UNIQUE (namespace_id, chunk_id, extraction_key, content_hash)                 -- CommitChunk idempotency (no epoch, D11)
) PARTITION BY HASH (namespace_id);

CREATE TABLE fact_vectors (         -- INSERT-ONLY, N111
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, memory_id uuid NOT NULL, embedding_model text NOT NULL,
  document_id text NOT NULL, chunk_id uuid NOT NULL, mentioned_at timestamptz NOT NULL,    -- immutable copies: the arm filters INSIDE the scan
  embedding halfvec(768) STORAGE MAIN NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, memory_id, embedding_model),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);
-- chunk_vectors(…, chunk_id, embedding_model, embedding_effective_at, document_id, mentioned_at, embedding) and
-- observation_version_vectors(…, ov_id, embedding_model, observation_id, version, effective_at, embedding) likewise
```

Decisions:

| Choice | Decision | Rationale | Rejected |
|---|---|---|---|
| vectors | side tables keyed `(content id, embedding_model)`; `ReembedChunk` and a model change insert rows; a model change is the `ReembedNamespace` workflow (insert, index, flip `namespace_models`, expunge the old), never a config flip | the vectored row never changes; versioned re-embed (A-7) | embedding on the content row |
| `chunk_vectors` key | `(chunk_id, embedding_model, embedding_effective_at)` — one extra column beyond N111's key | a summary refresh re-embeds a chunk without re-extracting (N110a); under `as_of = T` the arm admits rows with `embedding_effective_at <= T` and `mentioned_at <= T` and takes the best admitted row per chunk (N85) | replacing the row (not insert-only) |
| immutable copies on vector rows | `document_id`, `chunk_id`, `mentioned_at` / `effective_at` | visibility and `as_of` become in-scan filters, so an iterative HNSW scan refills past hidden rows without a join per candidate | a join to the content table per candidate |
| `extraction_key` on facts only | the chunk row has no key; `FinalizeVersion` inserts `fact_hidden(cause = 'reextract')` for facts of a kept chunk whose key differs from the current one (N58) | a chunk row cannot change; the unique key includes `extraction_key` so the re-extracted twin can coexist | key on `chunks`, updated in place |
| `w5` | one `jsonb` object | payload for prompts and display, never filtered | five columns |
| `fact_type` | enum column, not in the BM25 index | a rare filter, applied after the Top-K | a generated `fact_type_code` (no generated columns, P-10) |

**Per-namespace partial HNSW (N112) and the index-creation procedure.** No vector index is
declared in the static DDL. A hash-partitioned table keeps all rows of a namespace in exactly one
partition, so a namespace owns **one index per (vector table, embedding model)**; with ≈ 150
namespaces and three tables that is ≤ 450 small indexes per shard. Index DDL has two
constraints: `CREATE INDEX CONCURRENTLY` cannot run inside a transaction (so not inside a
function), and it cannot be issued on a partitioned parent. The schema therefore ships functions
that *generate* the statements, and `engramctl index` (owner role `engram_migrate`) executes
them one at a time as top-level statements:

```sql
SELECT * FROM engram_vector_index_plan($ns);        -- per vector table: vectors of the CURRENT model, indexed?, action = create | drop | none
SELECT statement FROM engram_hnsw_ddl('fact_vectors', $ns, $model, 'create');
--  CREATE INDEX CONCURRENTLY IF NOT EXISTS fv_c189d1d65214_2db1c3 ON fact_vectors_p02
--    USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128)
--    WHERE namespace_id = '0190c000-…-000000000001' AND embedding_model = 'nomic-embed-text-v1.5'
SELECT statement FROM engram_hnsw_ddl('fact_vectors', $ns, $model, 'drop');     -- DROP INDEX CONCURRENTLY IF EXISTS fv_…
```

`engram_vector_index_plan` says `create` at ≥ 2,000 vectors of the namespace's current model with
no `ready` row in `vector_indexes`, and `drop` below 1,000 (hysteresis). The sweeper records
`building` before the statement and `ready` after; a crash leaves an INVALID index that
`engram_invalid_indexes` lists and `engramctl index` drops and rebuilds (`CREATE … IF NOT EXISTS`
makes a retry idempotent). Queries that must match the partial-index predicate carry the
namespace id and model as literals: `plan_cache_mode = force_custom_plan` turns the bound
parameters into constants at plan time. On a move target the mover builds the namespace's indexes
only **after** the bulk copy, so the copy is bounded by B-tree and BM25 inserts (P-8). Namespace
delete and move cleanup run the `drop` statements first, then batched `DELETE`s: no graph repair.
The expunge runs `REINDEX INDEX CONCURRENTLY` on a touched partition index when its dead
fraction exceeds 5 % (`pgstattuple`).

**Ordinary indexes on partitioned tables (P-10)** added later follow `engram_partitioned_index_ddl(parent,
index_name, columns, using, where)`: `CREATE INDEX … ON ONLY` the parent (created invalid), then
for each partition `CREATE INDEX CONCURRENTLY` and `ALTER INDEX … ATTACH PARTITION`; the parent
becomes valid at the last attach (executed here for `facts`: 33 statements, parent valid, 16
partitions attached). No generated column exists, so no rewrite-only migration does either.

**Semantic-arm plan by namespace size.** Below 2,000 vectors the arm reads the namespace's rows
through `fact_vectors_model_idx (namespace_id, embedding_model, memory_id)` and sorts by distance;
at 2,000 and above the partial HNSW serves, with `hnsw.ef_search` = the arm cap (150 MID, 400
HIGH). Observed on the applied schema as `engram_app` under RLS, one namespace with 20,003 vectors: the stats
plan said `create`, the generated statement built `fv_c189d1d65214_2db1c3` on `fact_vectors_p02`
(valid), and the semantic arm with `ef_search = 150`, `iterative_scan = relaxed_order` and all three
marker arrays planned `Index Scan using fv_… Order By: (embedding <=> …)` under the RLS one-time
filter, returned 150 rows and removed 2 by the visibility filter inside the scan. At 2,103 vectors
and default costs the planner still preferred the exact scan, which is the crossover behaving as
designed; the partial index becomes the natural choice well above the 2,000 threshold.

#### 3.3.5 Links, entities, mentions

`fact_links` (partitioned, insert-only): one row per edge; entity/temporal/semantic edges are
undirected, stored once with `src < dst`; causal edges are directed. Per-fact caps (temporal ≤ 20,
semantic ≤ 10, entity ≤ 10 per shared entity) are enforced by the linker, not by a trigger. A link
is inserted in the `CommitChunk` of its newer endpoint; the expunge deletes links through the FK
cascade from `facts`. The graph arm joins both endpoints and applies the visibility predicate to
each (3.8), so a link to a hidden fact is inert from the ack of the marker.

`entities` is mutable (`mention_count`, `last_seen_at`, `merged_into`; the expunge recomputes
`canonical_name` from the remaining mentions); `entity_aliases` gains `document_id` (N118) so the
expunge deletes the victim's aliases; `entity_mentions` (partitioned, insert-only) carries the
fact's `mentioned_at`. **Under `as_of`, an `EntityRef` carries only `mention`:** `canonical_name`
and alias merges are suppressed and graph entity hops use `entity_mentions.mentioned_at <= T`.
Fuzzy resolution goes through `engram_entity_fuzzy(ns, norm)`, a `SECURITY DEFINER` function that
re-checks that `ns` is the namespace in scope (`42501` otherwise) and filters on `namespace_id`
itself, because the trigram operators are not leakproof and the planner will not push them into
the GIN scan under RLS (N131, P-5; verified: the function returns the in-scope match and refuses
another namespace).

#### 3.3.6 Observations, consolidation, pages: evidence segments

```sql
CREATE TABLE observations (          -- MUTABLE: current_version, proof_count, stale_write, stale_delete, tags, retired_at (fillfactor 70)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL, ..., PRIMARY KEY (namespace_id, observation_id)
);
CREATE TABLE observation_versions (  -- INSERT-ONLY, partitioned
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, ov_id uuid NOT NULL, observation_id uuid NOT NULL, version integer NOT NULL,
  root_version integer NOT NULL CHECK (root_version BETWEEN 1 AND version),     -- = version for a root rebuild, else root_version(v-1)
  text text NOT NULL, effective_at timestamptz NOT NULL, source_count integer NOT NULL, prompt_version text NOT NULL, model text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, version), UNIQUE (namespace_id, ov_id)    -- ov_id: the single-column BM25 key_field
) PARTITION BY HASH (namespace_id);
CREATE TABLE observation_version_meta (  -- INSERT-ONLY, write-once (N33)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL, version integer NOT NULL,
  superseded_at timestamptz NOT NULL, PRIMARY KEY (namespace_id, observation_id, version)
);
CREATE TABLE observation_inputs (    -- INSERT-ONLY: every fact SHOWN to the stage-2 writer of version v (N41, N121)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL, version integer NOT NULL,
  fact_id uuid NOT NULL, document_id text NOT NULL,                              -- document_id denormalised (N117)
  PRIMARY KEY (namespace_id, observation_id, version, fact_id)                  -- + indexes (namespace_id, document_id) and (namespace_id, fact_id)
);
CREATE TABLE derived_hidden (        -- the materialisation of the read predicate, written by Expunge.Materialize (N119)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, kind text NOT NULL CHECK (kind IN ('observation', 'page')), id uuid NOT NULL,
  root_version integer NOT NULL, from_version integer NOT NULL, cause_kind text NOT NULL CHECK (cause_kind IN ('document', 'invalidation')),
  cause_id text NOT NULL, ..., PRIMARY KEY (namespace_id, kind, id, root_version, cause_kind, cause_id)
);
```

`page_versions` (insert-only, `root_version`), `page_version_meta` (write-once `superseded_at`) and
`page_version_inputs(page_id, version, kind ∈ {fact, observation}, source_id, source_version,
document_id)` follow the same shape; the input rows of a page are polymorphic, so they carry no FK
and the expunge deletes them by `(namespace_id, document_id)` and by page.

**Evidence segments (N117).** The derivation set of `(O, v)` is `observation_inputs(O, w)` for
`root_version(v) ≤ w ≤ v`: everything its writer could have seen (the batch facts attached to `O`
and the text of the previous version, itself written from the segment's earlier inputs). `(O, v)`
is **visible** iff no input in its segment names a tombstoned document or a hidden fact **and** no
`derived_hidden` row covers it (`root_version` equal, `version >= from_version`). The old rule
"current version hidden while `stale_delete`" is gone: a current version whose inputs are intact
stays visible while the observation awaits a rewrite, and one with a victim in its segment is
hidden by the predicate itself. A page version is hidden if a fact input of its segment is
tombstoned or hidden, or an observation-version input of its segment is hidden by the observation
rule; the derivation graph has fixed depth two (fact → observation → page, pages never feed
observations, Reflect output is never stored), so this is two `EXISTS`, not a walk. A hidden
current page returns `PreconditionFailed{PAGE_HIDDEN}` until the refresh lands.

**`derived_hidden` rows for a document cause are permanent**: an older version written with the
victim in view must never resurface at any `as_of` (verified: after the tombstone of `d1` was
materialised, `as_of = 2026-02-15` still returned no version whose segment saw `d1`, and `as_of =
2026-01-15` returned nothing derived from it). Rows with `cause_kind = 'invalidation'` are deleted
by `Restore`. They are removed physically only with the observation or page itself or the
namespace.

**Two-stage consolidation (N121).** Stage 1 (routing, one call per batch of 8 facts) persists
nothing textual; stage 2 (one call per touched observation) writes the text, and `inputs(O, v)`
are the facts shown, all of them `O`'s own sources. A `merge` is a root rebuild of the survivor.
`consolidation_proposals` (write-once, trigger) holds the validated op list of stage 2 with
`input_fact_ids`; there is no `candidate_versions`, because no text flows between observations.
`consolidation_batches.attempts` is capped at 3, then `failed`.

**Consolidation bookkeeping is append-only (N95, H-15, H-23).** `fact_consolidation(namespace_id,
memory_id, stamped_at, note ∈ {done, failed, capacity}, batch_key)` has `PRIMARY KEY (namespace_id,
memory_id, stamped_at)` and is never updated; a fact is consolidated iff a `done` stamp exists,
retryable iff its latest stamp is `failed` and older than 7 days (`engram_pending_facts`).
`consolidation_state.watermark_memory_id` advances only to `engram_consolidation_watermark(ns)`:
just below the smallest unconsolidated fact **whether visible or marker-hidden** (a fact hidden by
`Invalidate` that `Restore` later reveals must still be pending), and never past
`engram_uuid_v7_floor(now() − 60 s)` (2 × `statement_timeout`).

#### 3.3.7 Markers, Expunge state, operations, metering, exports

```sql
CREATE TABLE document_tombstones (   -- Delete(document): ONE row; Expunge progress lives in expunge_state
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL, deleted_at timestamptz NOT NULL,
  operation_id uuid, intent_key text,                                            -- N122: the intent object put BEFORE this row
  expunge_state text NOT NULL DEFAULT 'pending' CHECK (expunge_state IN ('pending', 'materialized', 'purged')),
  materialized_at timestamptz, purged_at timestamptz, ..., PRIMARY KEY (namespace_id, document_id)
);
CREATE TABLE chunk_tombstones (namespace_id uuid, tenant_id text, chunk_id uuid, retired_at timestamptz NOT NULL DEFAULT now(),
  reason text NOT NULL CHECK (reason IN ('replace', 'reextract')), PRIMARY KEY (namespace_id, chunk_id));         -- FK cascade from chunks
CREATE TABLE fact_hidden (namespace_id uuid, tenant_id text, memory_id uuid, hidden_at timestamptz NOT NULL DEFAULT now(),
  cause text NOT NULL DEFAULT 'invalidate' CHECK (cause IN ('invalidate', 'reextract')), reason text NOT NULL DEFAULT '',
  intent_key text, PRIMARY KEY (namespace_id, memory_id));                                                        -- FK cascade from facts
CREATE TABLE curation_log (namespace_id uuid, tenant_id text, memory_id uuid, content_hash bytea NOT NULL, document_id text NOT NULL,
  action text NOT NULL CHECK (action IN ('invalidate', 'restore')), at timestamptz NOT NULL DEFAULT now(), reason text NOT NULL DEFAULT '',
  PRIMARY KEY (namespace_id, memory_id, at));                       -- INSERT-ONLY; no FK: it outlives the purge of the old fact
CREATE TABLE expunge_progress (namespace_id uuid, tenant_id text, unit text, table_name text,        -- unit: a document_id, '*chunks' or '*namespace'
  phase text NOT NULL CHECK (phase IN ('materialize', 'purge', 'hygiene', 'finish')), last_key text,
  rows_purged bigint NOT NULL DEFAULT 0, done boolean NOT NULL DEFAULT false, ..., PRIMARY KEY (namespace_id, unit, table_name));
```

- `DeleteDocument` = (intent object in blob storage) → `engram_try_ns_fence`, exclusive document lock, `documents.state = 'deleting'`,
  one `document_tombstones` row, one `deletion_log` row, one outbox event, commit. Nothing else is touched.
- `Invalidate(f)` = intent → `INSERT fact_hidden` + `curation_log` + `deletion_log` + outbox; `Restore(f)` = intent →
  `DELETE fact_hidden WHERE cause = 'invalidate'`, `DELETE derived_hidden WHERE cause = ('invalidation', f)`, `curation_log` row.
  **Restore is exact because nothing else encodes the invalidation** (verified: the visible set of facts, observation versions
  and page versions before `Invalidate` equals the set after `Restore`, with the materialised rows in between).
- `FinalizeVersion(REPLACE)` inserts `chunk_tombstones(reason = 'replace')` for chunks not in the new membership (one statement);
  an un-retire on a flap is `DELETE FROM chunk_tombstones`. The curation of a re-extracted twin is re-applied at `CommitChunk`
  from `curation_log` by `(document_id, content_hash)`, last action wins (A-16).
- **Expunge (N119)**, one workflow per namespace, every activity fenced `active` at the epoch and skipped while a move is open:
  (1) **Materialize** under the exclusive derivation lock: insert `derived_hidden`, mark observations/pages `stale_delete`, set
  `expunge_state = 'materialized'`; (2) **Purge** in batches of 1,000 with 50 ms pauses — facts (the FK cascade takes vectors, links,
  mentions, inputs, sources, stamps, `fact_hidden`), chunks, aliases, curation rows, page inputs by `document_id`, ledger rows of an
  explicit delete, then `blob_tombstones` — only after `engram_consumers_passed` says every registered index/Kafka cursor passed the
  delete's event; (3) **index hygiene**; (4) **finish**: `purged`, tombstone deleted after 24 h. SLA: materialize ≤ 15 min, purge ≤ 24 h,
  index rebuilt ≤ 48 h. **Degraded mode while a marker is `pending`**: the observation and page arms pay a per-candidate
  `observation_inputs` lookup (≈ 10 to 20 ms per arm), consolidation runs only root rebuilds, the rerank-skip SLO is suspended, and an
  alert fires after 15 min.

`operations` (kinds `retain, delete_document, delete_namespace, delete_tenant, consolidate, page_refresh, export, expunge, reembed`;
`superseded_by`, `cancel_reason`, N127; `MOVE_NAMESPACE` lives on the catalog row), `idempotency_keys` (24 h),
`token_usage_events` (insert-only, 30 d) with the `token_usage` aggregate, `quota_counters`, `batch_jobs`, and `blob_tombstones` are
unchanged in shape. `export_snapshots` loses `from_seq`/`to_seq` (no outbox cut, N126) and gains `snapshot_started_at` and `base_version`:
`BeginSnapshot` inserts it as `building`; the delete marker transaction expires `building` and `ready` rows alike;
`RecordSnapshot` refuses to promote an expired row and re-checks `document_tombstones.deleted_at > snapshot_started_at`; a delta is
the diff of two consecutive snapshots, always emitted with delete records and `deleted_ids` in the manifest.

#### 3.3.8 RLS and grants

The policy is created by one loop over every table that has a `namespace_id` column, parents and
partitions alike. A self-check at the end of the file fails the migration if any such table lacks
it, is not `FORCE`d, lacks `tenant_id`; if any non-admin role holds a privilege on a partition or
could bypass RLS; if `namespace_ownership_check` is not `ENABLE ALWAYS`; if a table lacks the
three `RESTRICTIVE` `move_target_*` policies; if a cleanup or entity function lost `SECURITY
DEFINER` or the cleanup became executable by `engram_app`/`engram_relay`; if the ownership machine
lacks `ready` or a rollback edge; if a content table is writable by `UPDATE`/`DELETE` for a
non-admin role, lacks its insert-only trigger, or has a generated or visibility column; if any of
the eight big tables is not partitioned; and if a role lacks `synchronous_commit = local`:

```sql
CREATE POLICY ns_isolation ON facts
  USING      (namespace_id = current_setting('engram.namespace_id')::uuid)
  WITH CHECK (namespace_id = current_setting('engram.namespace_id')::uuid);        -- identical on every table and partition
CREATE POLICY move_target_ins ON facts AS RESTRICTIVE FOR INSERT TO engram_move WITH CHECK ((SELECT engram_ns_is_incoming()));
-- move_target_upd / move_target_del likewise, on every namespace-scoped table except namespace_ownership
CREATE POLICY relay_read_all ON outbox FOR SELECT TO engram_relay USING (true);
CREATE TRIGGER facts_insert_only BEFORE UPDATE ON facts FOR EACH ROW EXECUTE FUNCTION engram_forbid_update();   -- every content table
```

Executed as the three roles: `engram_app` `UPDATE`/`DELETE` of `facts` and `fact_vectors` →
`permission denied`; `engram_move` `DELETE FROM chunks` → `permission denied`; `engram_admin`
`UPDATE facts` and `UPDATE observation_version_meta` → `facts_p02 is insert-only` / `observation_version_meta is
insert-only` (`42501`); an `INSERT` with a foreign `namespace_id` → `new row violates row-level security policy`; a
query with no scope → `unrecognized configuration parameter`. Tables without `namespace_id` are exactly `shard_meta`,
`outbox_cursors`, `ownership_transitions` and goose's `goose_db_version`. `pg_policies.qual` renders the policy as
`(namespace_id = (current_setting('engram.namespace_id'::text))::uuid)`, the string section 8's static check compares against.

### 3.4 Where tenant_id, namespace_id and shard_id appear

`shard_id` appears on no namespace-scoped table. Inside a shard database the shard is implied by
the database: `shard_meta` (one row) is the identity, `namespace_ownership.shard_id` is the only
per-namespace copy and a trigger checks it against `shard_meta`. The catalog is the only place
where `shard_id` is a routing column (`namespaces.shard_id`, `shards.shard_id`,
`namespace_moves.*_shard_id`).

| Table | PK columns | `namespace_id` | `tenant_id` | Indexes leading with `namespace_id` | Exceptions / notes |
|---|---|---|---|---|---|
| `shard_meta` | (singleton) | no | no | none | shard-level identity, carries `shard_id` |
| `namespace_ownership` | `namespace_id` | PK | yes (+UNIQUE with ns) | PK | the fence row; the only table besides `shard_meta` with `shard_id` |
| `ownership_transitions` | none; unique `(edge, role_name, from_state, from_reason)` | no | no | none | shard-level; the state machine as immutable data |
| `namespace_stats`, `namespace_models`, `vector_indexes` | `namespace_id` (`vector_indexes`: + table, model) | PK | yes | PK | derived counters + `large`; current embedding model; per-namespace HNSW state |
| `outbox` | `seq` | yes (default from scope) | yes | `outbox_ns_seq_idx` | PK is `seq` (relay reads shard-wide in seq order) |
| `outbox_cursors` | `consumer` | no | no | none | shard-level; relay-owned; moves never touch it |
| `outbox_skipped`, `deletion_log` | `(namespace_id, consumer, seq)`; `(namespace_id, intent_key)` | PK | yes | PK | `deletion_log` key = intent object name |
| `ingest_ledger` | `namespace_id, ledger_id` | PK | yes (FK) | PK, doc, op, hash, received | append-only |
| `documents`, `document_versions`, `document_version_chunks` | `(ns, document_id[, version[, content_hash]])` | PK | yes | PK; tags GIN; active (partial unique); op; chunk | `documents.tags` is the only tag store |
| `chunks` (16), `facts` (16) | `(ns, chunk_id)`, `(ns, memory_id)` | PK, partition key | yes | PK; unique `(doc, hash)` / `(chunk, key, hash)`; doc; mentioned; facts: occurred (btree + GiST) | BM25 per partition; no tags, no vector, no flags |
| `fact_vectors` (16), `chunk_vectors` (16), `observation_version_vectors` (16) | `(ns, id, embedding_model[, embedding_effective_at])` | PK, partition key | yes | PK; `*_model_idx` (exact-scan path) | per-namespace partial HNSW created by `engram_hnsw_ddl`, not in the static DDL |
| `fact_links` (16), `entity_mentions` (16) | `(ns, src, dst, type)`, `(ns, memory_id, entity_id)` | PK, partition key | yes | PK; reverse; entity | insert-only |
| `entities`, `entity_aliases` | `(ns, entity_id)`, `(ns, alias_norm)` | PK | yes | PK; canonical (partial unique); trgm GIN (via the definer function); alias by document | |
| `observations`, `pages` | `(ns, observation_id)`, `(ns, page_id)` | PK | yes (FK) | PK; stale; tags GIN / name | mutable state |
| `observation_versions` (16), `page_versions` | `(ns, observation_id, version)`, `(ns, page_id, version)` | PK, partition key (versions) | yes | PK; `(ns, ov_id)` unique; effective | `ov_id` exists only because the BM25 `key_field` must be one column |
| `observation_version_meta`, `page_version_meta` | like their versions | PK | yes | PK | write-once `superseded_at` |
| `observation_inputs`, `page_version_inputs` | `(ns, obs, version, fact_id)`; `(ns, page, version, kind, source_id, source_version)` | PK | yes | PK; `(ns, document_id)`; `(ns, fact_id)` / `(ns, kind, source_id)` | `document_id` denormalised for the read predicate and the expunge |
| `observation_sources`, `observation_version_sources`, `page_sources` | `(ns, …)` | PK | yes | PK, memory/source | working set / frozen evidence |
| `document_tombstones`, `chunk_tombstones`, `fact_hidden`, `curation_log` | `(ns, document_id)`, `(ns, chunk_id)`, `(ns, memory_id)`, `(ns, memory_id, at)` | PK | yes | PK; open tombstones partial | the markers (N115) |
| `derived_hidden`, `expunge_progress` | `(ns, kind, id, root_version, cause_kind, cause_id)`; `(ns, unit, table_name)` | PK | yes | PK; cause | Expunge stage state (N119) |
| `consolidation_batches`, `consolidation_proposals`, `consolidation_applied`, `fact_consolidation`, `consolidation_state`, `batch_jobs` | `(ns, batch_key)` … | PK | yes | PK; round; batch | |
| `operations` | `namespace_id, operation_id` | PK | yes (FK) | PK, created, active | `operations_sweeper_idx (created_at)` and `operations_deferred_idx (deferred_until)` are shard-wide partial indexes for admin schedulers |
| `idempotency_keys`, `token_usage_events`, `token_usage`, `quota_counters`, `blob_tombstones`, `export_snapshots` | `(ns, …)` | PK | yes (FK) | PK, op, day | `idempotency_keys_expiry_idx (expires_at)` shard-wide for the 24 h sweep |

Catalog: `tenants(tenant_id)`, `shards(shard_id)`, `namespaces(namespace_id)` with indexes on
`(shard_id, state)` and `(tenant_id, state)`, `namespace_moves(move_id)` with the partial unique index
on `namespace_id`, `idempotency_keys(tenant_id, method, request_id)`, `tenant_usage_daily(tenant_id, day,
quota_key)`, `catalog_events(event_id)` with `(namespace_id, event_id)`.

### 3.5 Outbox design detail

The outbox is the transactional change feed of the shard (D6) and is deliberately thin: a delete is one marker
event, and moves do not read it.

**Columns.** `seq` (PK, from `outbox_seq`), `namespace_id`, `tenant_id`, `epoch` (all three defaulted
from the transaction scope), `event_type` (the `Event` oneof case name), `payload` (the serialised
`engram.internal.events.v1.Event`, ≤ 16 KiB), `created_at`. Events are thin (N12): ids, versions, hashes
and flags, never text or vectors; ids are 16-byte `bytes`; every event is bounded (≤ 256 ids, paged by
`page`/`page_count`, `ids_elided` above 4,096, N80). "Event" always means an outbox event written in the
same transaction as the change it describes. Field numbers and the BSR reference are in section 4; the
table below is generated from `events.proto` (`make gen-docs`).

| Event | Emitted by | Payload |
|---|---|---|
| `DocumentVersionStarted` | retain ack | `document_id, version, content_hash, update_mode, chunks_planned` |
| `ChunkCommitted` | `CommitChunk` (re-emitted by `ReembedChunk`) | `document_id, version, chunk_id, content_hash, fact_ids[], entity_ids[], links_written, mentioned_at` |
| `ChunksRetired` | `FinalizeVersion` | `document_id, version, page, page_count, chunk_ids[], retired_at` (chunk tombstones, paged) |
| `DocumentVersionActivated` | `FinalizeVersion` | `document_id, version, superseded_version, fact_count, chunk_count` |
| `DocumentDeleted` | the delete marker transaction; again by the Expunge | `document_id, deleted_at, phase` (`MARKED` \| `PURGED`) — **O(1)**, no id list; index consumers delete by the indexed `(namespace_id, document_id)` query |
| `FactInvalidated`, `FactRestored` | Invalidate/Restore | `fact_id, hidden_at, reason` / `fact_id` |
| `ObservationUpserted`, `ObservationRetired` | consolidation apply | `observation_id, version, root_version, source_fact_ids[], effective_at, op_key` / `observation_id, op_key` |
| `EntityUpserted`, `EntitiesMerged` | `CommitChunk`, resolver | `entity_id, canonical_name, type, created` / `survivor_id, merged_ids[]` |
| `PageVersionCreated`, `PageDeleted` | refresh, delete | `page_id, version, root_version, markdown_blob_key, effective_at` / `page_id` |
| `SnapshotCreated` | export | `version, manifest_blob_key, base_version` |
| `TokenUsageRecorded` | every metered commit | `day, op, model, prompt_tokens, completion_tokens, cost_micros` |
| `NamespacePurged` | namespace expunge | `rows_purged, blobs_purged` |
| `RestoreMarker` | restore-from-backup (§9.3) | `shard_id, restored_to, epoch_bump` — tells every consumer to reconcile from the restore point |

**Cursors.** `outbox_cursors` has one row per registered consumer (`index`, `kafka`) and one `relay` row.
`last_seq` means "every row with `seq <= last_seq` was delivered to this consumer, except the seqs
listed in the relay row's `gaps`". The relay delivers batches of 500 and advances a consumer's `last_seq`
only after the consumer acknowledged the batch, **batched to one update per second per consumer**
(XID budget, N114). The Expunge's row purge of a document waits until `engram_consumers_passed` is true,
i.e. every registered cursor passed the `DocumentDeleted{MARKED}` event (H-17).

**Gap watchlist (D6).** `seq` is assigned at `nextval()` time, so a slow transaction can commit seq 101
after seq 102 was read. The relay re-reads a hole on the next poll and, if it is still open after 1 s,
records `{seq, deadline = first_seen + 60 s}` (2 × `statement_timeout`) in the relay row's `gaps`; a
resolved seq is delivered, a seq past its deadline is declared aborted. The list is persisted (relay
failover cannot lose a late commit) and bounded at 1,000 entries (backpressure, not silent loss).
The 60 s horizon is sound because commits cannot hang (`synchronous_commit = local`, N122) and the outbox
`INSERT` is the last statement of every writer (A-F1). Rejected: an in-memory list; `pg_current_snapshot()`
xmin watermarks (wraparound-unsafe); a table lock around every writer.

**Retention.** A daily admin job deletes rows with `seq < min(last_seq)` over consumers and `created_at <
now() - 7 days` in batches of 10,000 by `seq` range. Exports no longer depend on the outbox (a delta is the
diff of two consecutive snapshots, N126), so the 7-day window only bounds consumer lag.

### 3.6 Blob storage layout

Every key is `{shard}/{tenant}/{namespace}/{class}/...` where `{shard}` is the decimal shard id
(`catalog.shards.blob_prefix`), so one prefix listing enumerates a namespace and one prefix is what a move
copies and a purge deletes. Credentials are issued **per shard prefix** (`{shard}/*`): a cell holds one
credential per shard it serves, the move executor holds the source and the target credential, and
`blob.Scoped` (section 2) refuses any key outside the namespace prefix it was opened for. Delete intents
live outside every shard prefix, at `_control/deletes/{tenant}/{ns}/…` (below), so they follow the
namespace across moves. Rejected: one credential per cell; per-namespace credentials.

| Class | Key | Content | Addressing | Written by | Deleted by |
|---|---|---|---|---|---|
| **Delete intent** (N122) | `_control/deletes/{tenant}/{ns}/{deleted_at_rfc3339}-{operation_id}.json` | kind, subject, request hash, epoch | named by time + operation; strongly consistent put | the API **before** the marker transaction of `DeleteDocument`, `DeleteNamespace`, `DeleteTenant`, `Invalidate`, `Restore` | `engramctl` after 35 days (> the 28-day backup window) |
| Ingest raw body | `ledger/{sha256}` | raw item body > 64 KiB (N7) | content-addressed per namespace | engram-api before the ledger tx | expunge, after checking no other live ledger row has the hash |
| Extraction cache | `xcache/{sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version ‖ render_hash)}.json` (N87) | structured extraction result | content-addressed, immutable, per namespace | `ExtractChunk` | expunge only when no live chunk references the hash **and** the blob is older than `xcache_grace = 24 h` (N100), through `xcache_gc` tombstones with `not_before`; idle TTL 30 d; namespace delete. A missing blob at `CommitChunk` is the retryable `InputBlobMissing` |
| Version body | `ver/{sha256}` (N104) | the full reconstructed body of one document version | content-addressed, immutable | `LoadItem` | expunge for superseded versions; document / namespace delete |
| Document summary | `docsum/{sha256(document_hash ‖ prompt_version ‖ model)}.json` | ≤ 200-char summary + heading tree | content-addressed | `SummarizeDocument` | tombstoned on document delete |
| Consolidation result | `consolidate/{hex(batch_key)}.json` | raw LLM ops for the batch | content-addressed by `batch_key` | `ConsolidateBatch` | 30 d sweep |
| Page markdown | `pages/{page_id}/v{n}.md` | one immutable version | versioned | page refresh | tombstoned on page retire / namespace delete |
| Export snapshot | `export/v{n}/manifest.json`, `facts.jsonl.zst`, `observations.jsonl.zst`, `chunks.jsonl.zst`, `pages/*.md`, `delta-v{n-1}-v{n}.jsonl.zst` | D12 | versioned | `ExportSnapshot` | `export_snapshots.expires_at` → tombstones |
| Reflect transcript (optional) | `reflect/{operation_id}.jsonl` | tool calls and model turns | per operation | `Reflect` when `profile.keep_transcripts` | 7 d sweep |
| Temporal staging (N13) | `staging/{operation_id}/{activity}.bin` | embeddings > 512 KiB in flight | per activity | activities | on operation end |

Blobs are immutable: a changed input gets a new key, never an overwrite, which makes the content-addressed
classes safe to share within a namespace and a move's prefix copy idempotent (copy-if-absent). A move copies
every key referenced by a copied row (`ingest_ledger.body_blob_key`, `document_versions.body_key`,
`page_versions.markdown_blob_key`, export manifests and parts) and **checks existence of each on the target
before cutover** (H-8); caches (`xcache`, `ecache`, `staging`, `consolidate/`) are not copied. Deletion is
always asynchronous through `blob_tombstones`, so the ack never waits on the blob store. The shard's pgBackRest
repository lives under `_backups/shard-{id}/` outside every tenant prefix (N23).

### 3.7 Sizing at 10 M facts per shard (N114)

Assumptions: average fact text 160 bytes, `w5` 200 bytes, `document_id` 24 bytes; 1 M documents with 1 M live
chunks (10 facts per chunk) and 1.5 versions per document; 0.5 M entities, 2 mentions per fact; one
observation per 20 facts, 1.5 versions each; 30 `fact_links` rows per fact (D3). Tuple header 24 B, line
pointer 4 B, index entry header 8 B, B-tree leaf fill 90 %. Content tables are insert-only at `fillfactor =
100`, so a `facts` row (≈ 0.8 KB, 10 per page) no longer carries the 1.5 KB vector.

| Table group | Rows | Heap | Indexes | Total |
|---|---|---|---|---|
| `facts` (no vector, no tags, no flags) | 10 M | ≈ 8 GB | PK 0.5, `(chunk, key, hash)` 1.0, doc 0.6, mentioned 0.4, occurred 0.4, GiST 0.5 = 3.4 GB | ≈ 11.4 GB |
| `fact_vectors` (key + 3 immutable copies + inline `halfvec`) | 10 M | ≈ 1.6 KB each ≈ 16 GB | PK 0.7, `model_idx` 0.5 | ≈ 17.2 GB |
| `fact_vectors` HNSW, sum of the per-namespace partial indexes | 10 M | | 18 GB (the same graphs, split by namespace) | 18 GB |
| `facts` BM25 | 10 M | | 4 GB | 4 GB |
| `fact_links` | 300 M | 30 GB | PK 21 GB, reverse 20 GB | **71 GB** |
| `chunks` + `chunk_vectors` + HNSW + BM25 | 1 M | text 3.6 GB + vectors 1.7 GB | HNSW 1.8, BM25 0.8, B-tree 0.6 | ≈ 8.5 GB |
| `entities` + aliases; `entity_mentions` (with `mentioned_at`) | 0.5 M + 1 M; 20 M | 0.25 GB; 2.1 GB | trgm 0.3, B-tree 0.2; PK 1.2, entity 1.4 | ≈ 0.75 GB; ≈ 4.7 GB |
| `observations` + versions + `observation_version_vectors` + meta | 0.5 M + 0.75 M | text 0.6 GB + vectors 1.3 GB | HNSW 1.3, BM25 0.4, B-tree 0.4 | ≈ 4 GB |
| `observation_sources` (working set); `observation_version_sources` | 10 M; ≈ 15 M | 2.5 GB; 3.8 GB | 1.2 GB; 1.9 GB | ≈ 3.7 GB; ≈ 5.7 GB |
| `fact_consolidation` (append-only stamps) | 10 M | ≈ 1.2 GB | PK 0.5, batch 0.6 | ≈ 2.3 GB |
| `observation_inputs` (with `document_id`; ≤ 58 rows per version) | ≈ 45 M | 5.4 GB | PK 3.2, fact 2.3, document 2.1 | ≈ 13 GB |
| `ingest_ledger`; documents, versions, membership | 1.5 M; 4 M | 5.7 GB; 0.6 GB | 0.3 GB; 0.4 GB | 6 GB; 1 GB |
| `outbox` (7 d), `token_usage_events`, operations, markers, `derived_hidden` | | | | ≈ 3.2 GB (markers are bounded by expunge lag) |
| **Total** | | | | **≈ 174 GB** (≈ 139 GB at 15 links per fact), re-measured in M0.6 |

`fact_links` cannot be smaller at 300 M rows of three UUIDs (71 GB is the floor with the PK and the reverse
index; D1 fixes UUIDv7 ids); the practical lever is the link cap (section 5). The 300 GB volume holds the
total with room for one `REINDEX CONCURRENTLY` of the largest per-partition index.

**Instance and hot set.** 8 vCPU, **128 GB RAM**, `shared_buffers = 32 GB`, container limit 112 GB,
`effective_cache_size = 96 GB`, NVMe ≥ 10 k IOPS (A-1). Every HNSW visit fetches its heap tuple (unavoidable
on pgvector), so the hot set is: HNSW 18 + vector heaps 16 + `facts` content heap 8 + BM25 4 + B-trees 3.4 +
`fact_links` PK (recently touched half) 10 + chunk/observation vectors and HNSW 6 ≈ **65 GB**. Rejected:
`STORAGE EXTERNAL` for the vectors (TOAST probes per exact scan) and a 6 M-fact soft cap.

**IOPS budget (MID recall).**

| Component | Page touches |
|---|---|
| 3 vector arms × ≈ 150 visited × 2 pages (index page + vector heap page; per-namespace graph, visited ≈ `ef_search`) | ≈ 900 |
| BM25 arm | ≈ 200 |
| graph arm (≤ 300 nodes) | ≈ 300 |
| **per recall** | **≈ 1,400** |
| 50 QPS at a 5 % miss rate | ≈ 3,500 IOPS, inside A-1's 10 k |

Write cost: `CommitChunk` ≈ 10 inserts into a ≤ 1 M-element index ≈ 2 to 5 ms of HNSW; retire, invalidate and
delete markers cost **0 HNSW work**. Connection and CPU budget per shard: recall concurrency ≈ 12 of the
24-connection pgbouncer pool, shared with ack, expunge and Reflect; per-process pool 16 everywhere;
recall-under-ingest CPU is an M0.5 exit. Fleet: 4 shards per 512 GB host, 8 hosts per 32-shard cell.

**Vacuum, freeze and bloat plan.**

| Table | Write pattern | Settings (per partition where partitioned) | Why |
|---|---|---|---|
| content partitions (`facts`, `chunks`, `fact_links`, `entity_mentions`, `observation_versions`, the three vector tables) | insert-only; `DELETE` only by the expunge | `fillfactor = 100`, `autovacuum_vacuum_insert_scale_factor = 0.05`, `autovacuum_vacuum_scale_factor = 0.02`, `autovacuum_freeze_min_age = 10 M`, `cost_delay = 2`, `cost_limit = 1000` | no updates means no dead tuples except purge batches; insert-driven vacuums keep the visibility map fresh for index-only scans and pg_search; early freezing keeps `age(relfrozenxid)` down |
| `documents`, `observations`, `pages`, `entities`, `document_versions`, `consolidation_*`, `batch_jobs`, `observation_sources` | frequent small updates | `fillfactor = 70` | HOT stays on the page |
| markers, `derived_hidden`, `expunge_progress`, `vector_indexes`, `namespace_*`, `operations`, `token_usage`, `quota_counters` | tiny, hot | `fillfactor = 50` to 70 | HOT forever |
| `outbox_cursors` | a few rows, advances batched to 1/s per consumer | `fillfactor = 50` | one page per row; XID budget (P-11) |

The shard exports `age(relfrozenxid)` per partition and alerts at 50 % of `autovacuum_freeze_max_age`. A
retire writes a marker row, never a 2.4 KB tuple, so the old "retire + purge" non-HOT update cost is gone.
The expunge deletes physical rows in batches of 1,000 with 50 ms pauses (≈ 10 s for a 100 k-fact document),
capped at 20 k rows/s per shard so autovacuum stays ahead; because the per-namespace HNSW is small, the
expunge rebuilds it (`REINDEX INDEX CONCURRENTLY`) when its dead fraction exceeds 5 % instead of waiting for
autovacuum to repair a shard-wide graph. `maintenance_work_mem = 2 GB` (§9) is enough for any single
per-namespace index. BM25 segments merge in the background; deleted documents stop influencing scores after
`VACUUM`.

### 3.8 Query patterns the schema serves

| # | Query | Served by | Role |
|---|---|---|---|
| 1 | scope + ownership fence (every transaction) | shared advisory try-lock (`engram_try_ns_fence`) + `namespace_ownership` PK (plain read) | app |
| 2 | marker sets, once per recall (three selects) | `document_tombstones` / `chunk_tombstones` / `fact_hidden` PKs | app |
| 3 | semantic arm over `fact_vectors` (as_of, visibility, allowed documents) | exact scan below 2,000 vectors (`fact_vectors_model_idx`); the namespace's partial HNSW above; partition pruned (N112) | app |
| 4 | lexical arm over facts (BM25 Top-K, then the visibility join) | `facts_bm25` | app |
| 5 | graph expansion (bounded, from ≤ 20 seeds, both endpoints visible) | `fact_links` PK + `fact_links_reverse_idx`, `facts` PK | app |
| 6 | temporal arm (N68) | `facts_occurred_idx` (two-sided probe) | app |
| 7 | chunk arm (BM25 ∪ HNSW over `chunk_vectors`, `embedding_effective_at <= T`) | `chunks_bm25`, `chunk_vectors` plans as 3 | app |
| 8 | observation arms (semantic + lexical, segment predicate, D9 `as_of`); version evidence; pages | `observation_version_vectors`, `observation_versions_bm25`, `observation_inputs` PK and document index, `derived_hidden` PK prefix | app |
| 9 | `DeleteDocument`, `Invalidate`, `Restore`, `FinalizeVersion(REPLACE)` marker transactions | `document_tombstones`, `fact_hidden`, `chunk_tombstones` PKs | app |
| 10 | retain upsert path (`CommitChunk`) | `chunks (document_id, content_hash)` unique, `facts (chunk_id, key, hash)` unique, `entities_canonical_uq`, PKs | app |
| 11 | Expunge: Materialize, purge batches, hygiene | `observation_inputs_document_idx`, `page_version_inputs_document_idx`, `facts_doc_idx`, `entity_aliases_document_idx` | admin |
| 12 | outbox relay read / cursor advance / retention | `outbox` PK, `outbox_cursors` PK | relay / admin |
| 13 | move: bulk copy range, reconcile (set difference), `VerifyFK`, cleanup | PKs, `engram_uuid_v7_floor`, `engram_column_hash` | move |
| 14 | entity resolution (fuzzy, per namespace) | `engram_entity_fuzzy` → `entities_trgm_idx` | app |
| 15 | consolidation selection, stale observations/pages | `engram_pending_facts`, `engram_consolidation_watermark`, `observations_stale_idx`, `pages_stale_idx` | app |
| 16 | lists, `GetMemory`, keyset pagination | PKs; the visibility predicate | app |
| 17 | schedulers, stats sweeper, index sweeper | `operations_sweeper_idx`, `operations_deferred_idx`, `idempotency_keys_expiry_idx`, the two views, `engram_vector_index_plan` | admin |

In the SQL below `$1` is the namespace, `$2` the query vector, `$3` `as_of` (`'infinity'` when unset),
`$m` the namespace's current embedding model (`namespace_models`). Every statement shown was executed
against the validated shard schema, with the BM25 statements parser-checked, since pg_search is not
installable outside the ParadeDB image.

**The visibility predicate (N116), identical in every arm.** The recall layer loads the marker sets once
per request, in three indexed selects (alert when a set exceeds 16 k entries; above that an arm switches to
the `NOT EXISTS` anti-join form), and passes them as array parameters:

```sql
SELECT engram_doc_tomb($1)            AS doc_tomb,     -- document_tombstones with expunge_state <> 'purged'
       engram_doc_tomb($1, true)      AS doc_pending,  -- only 'pending': the observation_inputs lookup is skipped for materialised ones
       engram_chunk_tomb($1)          AS chunk_tomb,   -- chunk_tombstones
       engram_fact_hidden_ids($1)     AS fact_hidden;  -- fact_hidden
-- facts and chunks:  visible(f) = f.document_id <> ALL($doc_tomb) AND f.chunk_id <> ALL($chunk_tomb) AND f.memory_id <> ALL($fact_hidden)
--                    and, for as_of = T:  f.mentioned_at <= T   (immutable row; never said_at)
```

`engram_visible_facts`, `engram_visible_chunks`, `engram_visible_observation_versions`,
`engram_visible_page_versions` are the same predicates as set-returning SQL functions, used by tests and
`engramctl` and as the reference the arm SQL is compared against. GetMemory, ListMemories, Reflect's
tools, the MCP tools and the export apply the same predicate (verified on the applied schema: with `d1`
tombstoned, the semantic, lexical and observation arm queries and the page list returned nothing derived
from `d1`, before and after Materialize).

**Tag filter, resolved once.** The five modes of D10 are evaluated against `documents.tags` (GIN on
`(namespace_id, tags)`; EXACT and the strict modes scan the namespace's documents, ≤ 10 k rows) into an
allowed-document set `$allowed_docs`:

| Mode | Predicate on `documents.tags` (`Q` = `$q`) |
|---|---|
| unset | no filter (`$allowed_docs` is not applied) |
| `ANY` / `ANY_STRICT` | `(cardinality(tags) = 0 OR tags && $q)` / `tags && $q` |
| `ALL` / `ALL_STRICT` | `(cardinality(tags) = 0 OR tags @> $q)` / `tags @> $q AND cardinality(tags) > 0` |
| `EXACT` | `tags @> $q AND tags <@ $q` |

Every arm adds `document_id = ANY($allowed_docs)`; above 8 k allowed documents the arm joins `documents`
with the predicate instead. `engram_tag_match` is the SQL twin of the Lean decision procedure. No tag
predicate runs under RLS (P-5), and observation arms use `observations.tags` (consolidation scope).

**Semantic arm** (`fact_vectors`, mid budget):

```sql
SET LOCAL plan_cache_mode = force_custom_plan;            -- bound namespace and model become constants: the partial-index predicate is provable
SET LOCAL hnsw.ef_search = 150;                           -- the arm cap: 150 MID, 400 HIGH (50 LOW)
SET LOCAL hnsw.iterative_scan = relaxed_order;
SELECT memory_id, document_id, chunk_id, mentioned_at, embedding <=> $2::halfvec(768) AS distance
  FROM fact_vectors
 WHERE namespace_id = $1 AND embedding_model = $m
   AND mentioned_at <= $3                                  -- as_of, omitted when unset
   AND document_id <> ALL ($doc_tomb) AND chunk_id <> ALL ($chunk_tomb) AND memory_id <> ALL ($fact_hidden)
   AND document_id = ANY ($allowed_docs)                   -- tag filter, when set
 ORDER BY embedding <=> $2::halfvec(768)
 LIMIT 150;
```

The immutable copies on the vector row let every predicate run inside the scan, so the iterative scan
walks the graph until 150 rows pass, instead of joining `facts` per candidate. Fact-type filters need the
content row and join `facts` after the Top-K. **Plan by namespace size:** below 2,000 vectors the planner
reads the namespace through `fact_vectors_model_idx` and sorts (≤ 2,000 rows, ≈ 3.2 MB); at 2,000 and above
the namespace's partial HNSW serves (`fv_<md5(ns)[0:12]>_<md5(model)[0:6]>` on its single partition).
Verified as `engram_app` under RLS with 20,003 vectors in the namespace: the RLS qual appears as a one-time filter
`current_setting('engram.namespace_id')::uuid = '…'`, only partition `fact_vectors_p02` is scanned, and the plan is
`Index Scan using fv_c189d1d65214_2db1c3 … Order By: (embedding <=> …)` with the three marker arrays and `mentioned_at` as
filters inside the scan (150 rows returned, 2 removed); a namespace without an index plans the btree path plus a sort. The
same three plans apply to `chunk_vectors` and `observation_version_vectors`.

**Lexical arm** (pg_search ≥ 0.25 syntax; before 0.25 read `paradedb.score` and `paradedb.match`). BM25
filters on columns of the index (`namespace_id`, `mentioned_at`); the visibility sets are not index columns,
so the Top-K is over-fetched and the visibility is applied to it:

```sql
SELECT c.* FROM (
  SELECT memory_id, document_id, chunk_id, mentioned_at, fact_type, pdb.score(memory_id) AS score
    FROM facts
   WHERE namespace_id = $1 AND text ||| $2 AND mentioned_at <= $3
   ORDER BY pdb.score(memory_id) DESC, memory_id ASC          -- deterministic tiebreak
   LIMIT $over) c                                              -- $over = 150 when all marker sets are empty, else 300, doubled while short (≤ 3 rounds)
 WHERE c.document_id <> ALL ($doc_tomb) AND c.chunk_id <> ALL ($chunk_tomb) AND c.memory_id <> ALL ($fact_hidden)
   AND c.document_id = ANY ($allowed_docs) AND c.fact_type = ANY ($4::fact_type[])
 ORDER BY c.score DESC, c.memory_id ASC
 LIMIT 150;
```

The inner query keeps the `ORDER BY score LIMIT` Top-K pushdown (`TopKScanExecState`; the N19 `EXPLAIN`
assertion runs as `engram_app`, and the M0 gate checks the BM25 plan under RLS on the real image, N131).
If markers hide most of the top hits the arm returns fewer than 150 rows after the third round: accepted
degraded mode. `pdb.score` ranks only (IDF is per partition, so a raw score is partition-relative and would
let a tenant probe term rarity, F-28); `Scores.lexical` and `Scores.semantic` are reported as per-query
normalised values in [0, 1] (N67).

**Observation arms** apply the D9 rule and the segment predicate. The semantic form:

```sql
WITH cand AS (
  SELECT v.ov_id, v.observation_id, v.version, v.embedding <=> $2::halfvec(768) AS distance
    FROM observation_version_vectors v
   WHERE v.namespace_id = $1 AND v.embedding_model = $m AND v.effective_at <= $3
   ORDER BY v.embedding <=> $2::halfvec(768) LIMIT $over)
SELECT c.ov_id, c.observation_id, c.version, c.distance
  FROM cand c
  JOIN observation_versions ov ON ov.namespace_id = $1 AND ov.ov_id = c.ov_id
  JOIN observations o          ON o.namespace_id = $1 AND o.observation_id = c.observation_id AND o.retired_at IS NULL
  LEFT JOIN observation_version_meta s ON s.namespace_id = $1 AND s.observation_id = c.observation_id AND s.version = c.version
 WHERE (s.superseded_at IS NULL OR s.superseded_at > $3)             -- D9: latest version with effective_at <= T; unset as_of: the current version
   AND (cardinality(o.tags) = 0 OR o.tags && $q)                       -- scope tags, mode-specific
   AND NOT EXISTS (SELECT 1 FROM observation_inputs i                  -- (a) the SEGMENT root_version(v) <= w <= v names a pending victim or a hidden fact
                    WHERE i.namespace_id = $1 AND i.observation_id = c.observation_id
                      AND i.version BETWEEN ov.root_version AND ov.version
                      AND (i.document_id = ANY ($doc_pending) OR i.fact_id = ANY ($fact_hidden)))
   AND NOT EXISTS (SELECT 1 FROM derived_hidden h                      -- (b) already materialised: a PK-prefix lookup
                    WHERE h.namespace_id = $1 AND h.kind = 'observation' AND h.id = c.observation_id
                      AND h.root_version = ov.root_version AND ov.version >= h.from_version)
 ORDER BY c.distance LIMIT 150;
```

The lexical form is the same shape over `observation_versions_bm25` (inner Top-K, outer predicate). The
superseded versions are in the index too, so the inner Top-K is over-fetched like the facts arm. **Pages**
use the same two lookups with depth two (`engram_page_version_hidden`: a fact input of the segment is
tombstoned or hidden, or an observation-version input of the segment is hidden, or `derived_hidden` covers
it); `GetPage`, `SearchPages`, Reflect's `get_page`/`search_pages`, the MCP tools and the export apply it, and
a hidden current page returns `PAGE_HIDDEN` until the refresh lands. **Chunk arm:** `chunk_vectors` with
`embedding_effective_at <= $3 AND mentioned_at <= $3 AND document_id <> ALL ($doc_tomb)` and the chunk
tombstones, best admitted row per `chunk_id`. **Evidence of any version** is that version's own rows:

```sql
SELECT s.memory_id, s.quote, f.text
  FROM observation_version_sources s
  JOIN facts f ON f.namespace_id = s.namespace_id AND f.memory_id = s.memory_id
 WHERE s.namespace_id = $1 AND s.observation_id = $o AND s.version = $v
   AND f.mentioned_at <= $3 AND f.document_id <> ALL ($doc_tomb) AND f.chunk_id <> ALL ($chunk_tomb) AND f.memory_id <> ALL ($fact_hidden);
-- proof_count = the number of rows served
```

**Degraded mode (accepted, N119).** While any tombstone is `pending`, the observation and page arms pay
the `observation_inputs` lookup per candidate (≤ 150 candidates × ≤ 60 input rows, index-only, ≈ 10 to 20 ms
per arm), consolidation runs only root rebuilds, and the rerank-skip SLO is suspended for the namespace.

**Temporal arm** (N68; `$4..$5` query window, `$6` `query_timestamp`, N = the arm cap). A two-sided probe on
`facts_occurred_idx (namespace_id, occurred_start)`: the nearest N occurrences before and after
`query_timestamp`, each an index range scan that stops after N rows, then one 2N-row sort; the visibility
sets filter the branches (the over-fetch rule of the lexical arm applies):

```sql
(SELECT memory_id, occurred_start, occurred_end FROM facts
  WHERE namespace_id = $1 AND occurred_start IS NOT NULL AND occurred_start <= $6 AND occurred_start >= $4
    AND mentioned_at <= $3 AND document_id <> ALL ($doc_tomb) AND chunk_id <> ALL ($chunk_tomb) AND memory_id <> ALL ($fact_hidden)
  ORDER BY occurred_start DESC LIMIT 150)
UNION ALL
(SELECT memory_id, occurred_start, occurred_end FROM facts
  WHERE namespace_id = $1 AND occurred_start IS NOT NULL AND occurred_start > $6 AND occurred_start <= $5
    AND mentioned_at <= $3 AND document_id <> ALL ($doc_tomb) AND chunk_id <> ALL ($chunk_tomb) AND memory_id <> ALL ($fact_hidden)
  ORDER BY occurred_start ASC LIMIT 150)
ORDER BY abs(extract(epoch FROM (occurred_start - $6))) LIMIT 150;
```

**Graph expansion** (one query per link family; temporal: `$hops = 5`, `$per_node = 10`; the others `$hops =
2`; `$budget` = 100/300/1000; `$seeds` = top-20 of semantic ∪ lexical). The recursive CTE carries the
frontier and the global visited set as arrays, so the node budget is global and every hop is one
index-driven query. A neighbour is admitted only through the visibility join, so a link is traversed only
between two visible, `as_of`-safe facts and a link left behind by a marker is never followed:

```sql
WITH RECURSIVE hops AS (
  SELECT 0 AS hop, $seeds::uuid[] AS frontier, $seeds::uuid[] AS visited
  UNION ALL
  SELECT h.hop + 1, nf.frontier, h.visited || nf.frontier
    FROM hops h
    CROSS JOIN LATERAL (
      SELECT array_agg(nb) AS frontier FROM (
        SELECT e.nb
          FROM (SELECT CASE WHEN l.src_memory_id = ANY (h.frontier) THEN l.dst_memory_id ELSE l.src_memory_id END AS nb, l.weight
                  FROM fact_links l
                 WHERE l.namespace_id = $1 AND l.link_type = ANY ($types::link_type[])
                   AND (l.src_memory_id = ANY (h.frontier) OR l.dst_memory_id = ANY (h.frontier))
                   AND (l.link_type <> 'causal' OR l.src_memory_id = ANY (h.frontier))) e          -- causal: forward only
          JOIN facts f ON f.namespace_id = $1 AND f.memory_id = e.nb
         WHERE NOT (e.nb = ANY (h.visited)) AND f.mentioned_at <= $3
           AND f.document_id <> ALL ($doc_tomb) AND f.chunk_id <> ALL ($chunk_tomb) AND f.memory_id <> ALL ($fact_hidden)
         GROUP BY e.nb ORDER BY max(e.weight) DESC
         LIMIT GREATEST(0, $budget - cardinality(h.visited))) s) nf
   WHERE h.hop < $hops AND cardinality(h.visited) < $budget AND nf.frontier IS NOT NULL
)
SELECT unnest(deepest.visited) AS memory_id
  FROM (SELECT visited FROM hops ORDER BY hop DESC LIMIT 1) AS deepest;   -- pick the deepest row first, THEN unnest (F-40)
```

The seeds come from arms that already filter by visibility. The per-node cap is a window
`row_number() OVER (PARTITION BY origin ORDER BY weight DESC) <= $per_node` in the inner subquery (elided
for width). Cost per hop is one index range scan per frontier node on the PK and one on the reverse index,
plus a PK lookup per candidate on `facts`; at the mid budget that is ≤ 3,000 index probes, ≈ 3 ms warm.
Rejected: a Go-driven BFS with one round trip per hop; a recursive CTE without the array state.

**Entity names under `as_of` (N118).** `SELECT … FROM entity_mentions WHERE namespace_id = $1 AND memory_id
= ANY ($facts) AND mentioned_at <= $3` supplies the `mention` of each `EntityRef`; `canonical_name` and
alias merges are returned only when `as_of` is unset.

**Marker transactions** (one fenced write transaction as `engram_app` after the scope prelude and the intent
object; `$t/$e/$op` tenant, epoch, operation; `$ik` the intent object name):

```sql
-- DeleteDocument
SELECT pg_advisory_xact_lock(k.k1, k.k2) FROM engram_doc_lock_keys($1, $2) AS k;     -- exclusive document lock: waits for in-flight CommitChunks
UPDATE documents SET state = 'deleting', deleted_at = now() WHERE namespace_id = $1 AND document_id = $2 AND state = 'active' RETURNING created_at;   -- 0 rows -> NOT_FOUND
INSERT INTO document_tombstones (namespace_id, tenant_id, document_id, deleted_at, operation_id, intent_key) VALUES ($1, $t, $2, now(), $op, $ik);
UPDATE export_snapshots SET state = 'expired', expired_reason = 'document_delete', expires_at = now()
 WHERE namespace_id = $1 AND state IN ('building', 'ready') AND created_at >= $c;                   -- N126: building and ready alike
INSERT INTO deletion_log (namespace_id, tenant_id, intent_key, kind, subject_id, epoch, operation_id) VALUES ($1, $t, $ik, 'document', $2, $e, $op);
INSERT INTO operations (…, kind, target_id, …) VALUES (…, 'expunge', $2, …) ON CONFLICT DO NOTHING;   -- SignalWithStart of ns/{ns}/expunge
INSERT INTO outbox (event_type, payload) VALUES ('DocumentDeleted', $proto);                          -- last statement; one row, O(1)
COMMIT;
-- Invalidate(f):  INSERT fact_hidden (cause 'invalidate') + curation_log ('invalidate') + deletion_log + outbox FactInvalidated
-- Restore(f):     DELETE FROM fact_hidden WHERE memory_id = $f AND cause = 'invalidate';
--                 DELETE FROM derived_hidden WHERE cause_kind = 'invalidation' AND cause_id = $f::text;  + curation_log ('restore') + deletion_log + outbox FactRestored
```

Nothing else is touched, so the commit takes milliseconds at any size and the SLO is the marker transaction
(plus the 10 to 50 ms intent put); there is no per-1 k-facts cascade figure. After the commit nothing
derived from the document is returned by any read path (the predicate above), and every export snapshot that
could contain it is `expired`.

**Retain upsert path** (`CommitChunk`, one fenced transaction per chunk, idempotent on re-execution):

```sql
SELECT engram_try_doc_lock_shared($1, $2);        -- false -> retryable DocumentBusy (100 ms)
SELECT state, current_version FROM documents WHERE namespace_id = $1 AND document_id = $2;   -- plain read; state <> 'active' -> aborted
SELECT status FROM document_versions WHERE namespace_id = $1 AND document_id = $2 AND version = $v;   -- anything but 'ingesting' -> stop
INSERT INTO chunks (namespace_id, tenant_id, chunk_id, document_id, content_hash, header_hash, heading_path, header, text, mentioned_at)
VALUES (…) ON CONFLICT (namespace_id, document_id, content_hash) DO NOTHING RETURNING chunk_id, (created_at = now()) AS inserted;
DELETE FROM chunk_tombstones WHERE namespace_id = $1 AND chunk_id = $chunk;                  -- un-retire a flap; no row rewritten
INSERT INTO chunk_vectors (…, embedding_effective_at, …) VALUES (…) ON CONFLICT DO NOTHING;
INSERT INTO facts (…, mentioned_at, said_at, …) VALUES (…)                                       -- mentioned_at from the chunk (N86), never the extractor
ON CONFLICT (namespace_id, chunk_id, extraction_key, content_hash) DO NOTHING;
INSERT INTO fact_vectors (…) SELECT … ON CONFLICT DO NOTHING;
INSERT INTO fact_hidden (…, cause) SELECT f.namespace_id, …, 'invalidate' FROM facts f JOIN curation_log c ON …   -- A-16: the LAST action per (document_id, content_hash) is re-applied
  WHERE c.action = 'invalidate' AND f.memory_id = ANY ($new_fact_ids) ON CONFLICT DO NOTHING;
INSERT INTO entities (…) SELECT … FROM unnest(…) ORDER BY norm ON CONFLICT (namespace_id, canonical_norm) WHERE merged_into IS NULL DO UPDATE SET mention_count = entities.mention_count + EXCLUDED.mention_count, last_seen_at = now();
INSERT INTO entity_aliases (…, document_id) … ON CONFLICT DO NOTHING;   INSERT INTO entity_mentions (…, mentioned_at) … ON CONFLICT DO NOTHING;
INSERT INTO fact_links (…) VALUES (…) ON CONFLICT DO NOTHING;           -- rows pre-sorted by (src, dst, type): no deadlocks
INSERT INTO document_version_chunks (…, ordinal) VALUES (…) ON CONFLICT DO NOTHING;
INSERT INTO token_usage_events (…) ON CONFLICT (namespace_id, usage_key) DO NOTHING;   INSERT INTO token_usage (…) ON CONFLICT (…) DO UPDATE SET …;
INSERT INTO outbox (event_type, payload) VALUES ('ChunkCommitted', $proto);
COMMIT;
```

The `inserted` flag uses `created_at = now()` because `xmax` is not readable in `RETURNING` on a partitioned
table. No `chunks_done` or `namespace_stats` update runs here (written per wave, derived by the sweeper).
`ReembedChunk` inserts a `chunk_vectors` row and nothing else.

**FinalizeVersion retire set** (REPLACE; the per-document exclusive lock, after the version-row check). One
statement per effect, both markers:

```sql
SELECT pg_advisory_xact_lock(k.k1, k.k2) FROM engram_doc_lock_keys($1, $2) AS k;            -- exclusive, lock_timeout 35 s single attempt
INSERT INTO chunk_tombstones (namespace_id, tenant_id, chunk_id, reason)
SELECT c.namespace_id, c.tenant_id, c.chunk_id, 'replace' FROM chunks c
 WHERE c.namespace_id = $1 AND c.document_id = $2
   AND c.chunk_id NOT IN (SELECT chunk_id FROM document_version_chunks WHERE namespace_id = $1 AND document_id = $2 AND version = $3)   -- document-scoped (N107)
ON CONFLICT DO NOTHING RETURNING chunk_id;                                                    -- -> ChunksRetired (paged)
INSERT INTO fact_hidden (namespace_id, tenant_id, memory_id, cause, reason)                   -- N58: old-key facts on a kept chunk
SELECT f.namespace_id, f.tenant_id, f.memory_id, 'reextract', '' FROM facts f
 WHERE f.namespace_id = $1 AND f.document_id = $2 AND f.chunk_id IN (SELECT chunk_id FROM document_version_chunks WHERE namespace_id = $1 AND document_id = $2 AND version = $3)
   AND f.extraction_key <> $current_key ON CONFLICT DO NOTHING;
```

REPLACE is not a deletion: observations and pages derived from a tombstoned chunk stay visible and are marked
`stale_write`; the Expunge purges the retired chunks after the grace period.

**Consolidation apply order** (stage 2, under `engram_try_derivation_lock`, N120/N121). Re-verification is the
visibility predicate over `input_fact_ids` in a fresh statement (no `FOR SHARE`: facts are immutable). The
`update` op inserts the new `observation_versions` row (with `root_version`), the `observation_version_vectors`
row, `observation_version_meta` for the previous version (`superseded_at = effective_at` of the new one), the
`observation_inputs` rows (the facts shown, with `document_id`), the `observation_version_sources` rows, the
`fact_consolidation('done')` stamps, `consolidation_applied`, and rewrites `observation_sources`; a `merge` is a
root rebuild of the survivor. One transaction, ending with `ObservationUpserted`.

**Expunge Materialize** (under `engram_derivation_lock_exclusive`, one short transaction per batch of
observations):

```sql
INSERT INTO derived_hidden (namespace_id, tenant_id, kind, id, root_version, from_version, cause_kind, cause_id)
SELECT i.namespace_id, i.tenant_id, 'observation', i.observation_id, ov.root_version, min(i.version), 'document', i.document_id
  FROM observation_inputs i
  JOIN observation_versions ov ON ov.namespace_id = i.namespace_id AND ov.observation_id = i.observation_id AND ov.version = i.version
 WHERE i.namespace_id = $1 AND i.document_id = $victim
 GROUP BY i.namespace_id, i.tenant_id, i.observation_id, ov.root_version, i.document_id
ON CONFLICT DO NOTHING;                                              -- invalidation causes: the same with i.fact_id = $f and cause ('invalidation', $f)
-- pages: fact inputs by document_id, observation inputs through the rows just written
INSERT INTO derived_hidden (…, kind, id, root_version, from_version, …)
SELECT … 'page', pv.page_id, pv.root_version, min(pi.version), 'document', $victim
  FROM page_version_inputs pi JOIN page_versions pv ON … AND pv.version = pi.version
 WHERE pi.namespace_id = $1 AND (pi.document_id = $victim OR (pi.kind = 'observation' AND EXISTS (
         SELECT 1 FROM derived_hidden h JOIN observation_versions ov ON ov.namespace_id = h.namespace_id AND ov.observation_id = h.id
          WHERE h.kind = 'observation' AND h.id = pi.source_id AND h.cause_id = $victim AND ov.version = pi.source_version
            AND ov.root_version = h.root_version AND pi.source_version >= h.from_version)))
 GROUP BY …;
UPDATE observations SET stale_delete = true, stale_since = coalesce(stale_since, now()) WHERE (namespace_id, observation_id) IN (…);   -- nudge Consolidate (root rebuilds) and PageRefresh
UPDATE document_tombstones SET expunge_state = 'materialized', materialized_at = now() WHERE namespace_id = $1 AND document_id = $victim;
```

The purge then deletes in primary-key batches, recording `expunge_progress.last_key`:

```sql
DELETE FROM facts WHERE namespace_id = $1 AND memory_id IN
  (SELECT memory_id FROM facts WHERE namespace_id = $1 AND document_id = $victim AND memory_id > $last ORDER BY memory_id LIMIT 1000);
-- FK cascades: fact_vectors, fact_links, entity_mentions, observation_inputs, observation_sources, observation_version_sources, fact_consolidation, fact_hidden
-- then chunks (cascade: chunk_vectors, chunk_tombstones, document_version_chunks), page_version_inputs / entity_aliases / curation_log
-- WHERE document_id = $victim, recompute entities.canonical_name from the remaining mentions, ledger rows of an explicit delete,
-- document_versions, the documents row; blob_tombstones for xcache/, ver/, ledger/, proposal blobs
```

**Move queries** (N124). *Bulk copy,* per range of ≤ 100 k rows ordered by primary key, `READ COMMITTED`, no snapshot or
barrier: `COPY (SELECT <cols> FROM t WHERE namespace_id = $1 AND <pk> > $k ORDER BY <pk> LIMIT n) TO STDOUT BINARY` → a
session `TEMP` table on the target → `INSERT INTO t (<cols>) SELECT <cols> FROM tmp ON CONFLICT (<pk>) DO NOTHING` for
insert-only tables and `DO UPDATE SET <mutable cols>` for mutable ones, as `engram_move` under the `incoming` fence with `SET
LOCAL session_replication_role = replica`; `<cols>` is `engram_copy_columns(t)`. *Reconcile under the freeze:*

```sql
-- insert-only tables: re-copy rows newer than the copy began, with a margin above the 60 s maximum writer lifetime
SELECT … FROM facts WHERE namespace_id = $1 AND memory_id >= engram_uuid_v7_floor($t_copy - interval '10 minutes');   -- UUIDv7 key
SELECT … FROM fact_links WHERE namespace_id = $1 AND (src_memory_id >= $floor OR dst_memory_id >= $floor);           -- every link has a newer endpoint
SELECT … FROM observation_inputs WHERE namespace_id = $1 AND created_at >= $t_copy - interval '10 minutes';          -- created_at tables
SELECT count(*), bit_xor(hashtextextended(<pk>::text, 0)) FROM t WHERE namespace_id = $1;                              -- must match on both sides (hash only up to 2 M rows)
-- mutable tables: merge (pk, md5(row minus updated_at)) streams; upsert differing or missing rows, DELETE target rows absent on the source
-- blobs: existence check of every referenced key on the target (parallel 64); copy the missing; refuse cutover while any is missing
-- relay drain: every source consumer cursor >= the namespace's final max(seq), ≤ 60 s, else rollback;  SELECT * FROM engram_verify_fk($1) (every orphans = 0)
```

After cutover, cleanup on the source runs the `drop` statements of `engram_hnsw_ddl`, then `SELECT
engram_cleanup_namespace($1, 10000)` in a loop until it returns 0 (the outer `DELETE` carries `namespace_id`, P-19), as
`engram_move`, once the row is `moved_out`.

**Outbox relay** (`engram_relay`, direct connection): `SELECT seq, namespace_id, tenant_id, epoch, event_type, payload FROM
outbox WHERE seq > $hwm ORDER BY seq LIMIT 500`; gap probe `SELECT seq FROM outbox WHERE seq = ANY ($gaps)`; `UPDATE
outbox_cursors SET last_seq = $n, gaps = $j, updated_at = now() WHERE consumer = $c` (batched to 1/s per consumer). Retention
(admin): `DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox WHERE seq < $min_cursor AND created_at < now() - interval '7
days' ORDER BY seq LIMIT 10000)`.

**Entity resolution**: after an exact `entity_aliases` PK probe, `SELECT * FROM engram_entity_fuzzy($1, $norm)` (3.3.5).

**Schedulers** (admin, shard-wide): op-sweeper `SELECT namespace_id, tenant_id, operation_id, workflow_id, task_queue FROM
operations WHERE state = 'PENDING' AND workflow_started_at IS NULL AND created_at < now() - interval '2 min' ORDER BY created_at
LIMIT 100` on `operations_sweeper_idx`; DEFERRED resumer on `operations_deferred_idx`; idempotency expiry in batches. Every
shard-wide scheduler joins `schedulable_namespaces` (`state = 'active'`); Expunge and the index sweeper join
`purgeable_namespaces` (no open move). The same tick completes or deletes catalog namespaces left `creating` for more than 2 min
(through the admin RPC; workers never write the catalog directly, D4). The **stats sweeper** refreshes `namespace_stats` per
namespace (visible-fact count via the marker anti-join, counts of chunks, documents, observations, open markers) and acts on
`engram_vector_index_plan` through `engram_hnsw_ddl` (3.3.4).

### 3.9 Rejected alternatives

| Alternative | Why rejected (one line) |
|---|---|
| Flags on the vectored row (`retired_at`, `live`, partial `WHERE live` HNSW, `fillfactor 60`) | a retire still rewrites a 2.4 KB tuple, BM25 re-indexes it, and every later visibility change lands on the same row again (N112) |
| A shared partition HNSW with per-namespace partials only "in a band" | cost grows with 1/selectivity and vacuum repair is shard-wide; the band was a patch on top (N112) |
| `STORAGE EXTERNAL` for vectors, or a 6 M-fact soft cap, instead of 128 GB RAM | TOAST probes on every exact scan; a lower cap wastes the instance (N114) |
| Vectors on the content row, replaced by `ReembedChunk` | breaks insert-only; no versioned re-embed (N111) |
| Lineage tables and a lineage fixpoint under a lock | correct but keeps the blast radius (two thirds of a namespace) and a 64-deep walk that fails open (N117) |
| Per-version bitmaps / roaring sets, copy-on-write evidence sets | closures under "shown = tainted" are most of the namespace, so the representation does not help (N117) |
| Hiding a derived version with a stored flag stamped at delete time | needs a snapshot of the victims (stale at commit) and a recorded set that can drift; the read-time predicate has neither (N117) |
| A synchronous cascade at delete (retire facts, delete evidence, flag pages, expire snapshots in one transaction) | cost grows with the document and races with the writers it must stop; the marker is O(1) (N115) |
| `synchronous_commit = on` with a synchronous standby for deletes | doubles the Postgres footprint, still blocks commits when the standby is down, needs a racy pre-commit probe; the intent object in blob storage gives RPO 0 for acknowledged deletes (N122) |
| The catalog as the intent store | same HA question, and workers stay off the catalog (D4) |
| Moving by outbox replay (total event-to-row map, `move_applied`, copy barrier, catch-up rounds) | a second write path to fence and the mechanism that regressed twice; with insert-only content the difference after a dirty copy is exactly the rows inserted since (N124) |
| Dual-write during the move | a second write path to fence (N124) |
| A `BYPASSRLS` loader role | nothing bound the loaded rows to the namespace; load through a `TEMP` table under RLS and the `move_target_*` policies (N91) |
| `FOR SHARE` on facts for apply re-verification, or on the ownership/document rows as the fence | facts are immutable and cannot be locked meaningfully; compatible lockers bypass a waiting `FOR UPDATE` and churn multixacts (N82, N120) |
| A blocking shared fence for writers | a queued shared request holds its pooled connection behind a waiting exclusive one; writers try-lock (N82) |
| Exclusive takers that retry every 5 s | a retry storm; one 35 s attempt, longer than any legal 30 s writer (N82) |
| One lock-key form for namespace fence, derivation lock and document lock | cross-kind collisions over-serialise unrelated work; one- vs two-argument forms are different lock tags (N113) |
| Generated columns (`live`, `fact_type_code`, `tag_count`) | rewrite-only migrations on partitioned tables and move column-list drift; there are none left (P-10) |
| Tags copied onto facts, chunks and observation versions | a tag change would rewrite vectored rows; tags are item-level on `documents`, resolved once per recall (N113, N116) |
| Tag predicates and trigram operators evaluated under RLS | not leakproof, so the planner will not push them into the index scan; resolve outside RLS or use the `SECURITY DEFINER` wrapper (N116, N131) |
| pgvector `vector(768)` (float32) instead of `halfvec(768)` | doubles vector heap and HNSW for no measurable recall gain on nomic-embed at 768-d |
| Matryoshka 512-d by default | a known recall drop on LongMemEval; kept as the D15 knob, off |
| One partition per namespace; no partitioning; range partitioning by `created_at` | relations and DDL per namespace; a single 18 GB index and hours-long vacuums; queries are per namespace, never per time slice |
| A separate `search_entries` table (text + vector per searchable thing) | a join on every arm and a second copy of the vectors |
| Chunk text in blob storage | the chunk arm would do a blob round trip per candidate |
| `bigserial` ids | not portable across shards during a move (D1) |
| Application-only isolation, RLS keyed on `tenant_id`, `FORCE` off | one forgotten predicate is a leak; the tenant is not the request unit; the owner and direct partition access would bypass |
| Five W columns instead of `w5 jsonb` | `who` is multi-valued; five nullable columns widen every row for data that is never filtered |
| Storing symmetric links twice; adjacency arrays; a counting trigger for link caps | doubles the largest table; turns a purge into a GIN fan-out; one query per inserted row on a 300 M-row table |
| Rich outbox events (text + vectors); an in-memory gap watchlist; `pg_current_snapshot()` watermarks | 3.5 KB per event and vectors for Kafka consumers; lost on relay failover; wraparound-unsafe |
| Event payloads in a blob for large documents | a blob write inside a transaction; bounded, paged events with `ids_elided` instead (N80) |
| `tsvector` + `ts_rank_cd` instead of pg_search | not BM25 and no Top-K pushdown; the documented fallback (D7) |
| Per-commit `UPDATE` of `namespace_stats` / `chunks_done` | serialises every commit of a namespace on one row; counters are derived or written per wave (N69) |
| A model-adjustable `mentioned_at` as the `as_of` key | leak-freedom would depend on the extractor's dating (D9) |
| GiST overlap scan + in-memory sort for the temporal arm | a year-wide window sorts up to the whole namespace; the two-sided btree probe is bounded (N68) |
| Recording only cited sources per observation version | a version written with a newer or later-deleted fact in view would surface; `observation_inputs` records every fact shown (N41) |
| `op_key` over the live LLM answer | a retried batch can answer differently; the write-once proposal table is the fix (N43) |
| Deleting the `moved_out` ownership row at cleanup | the row is the fence value a late writer or stale reader must still hit (N93) |
| A dedicated `engram` schema; `shard_id` on every row | no isolation benefit inside a per-shard database; a constant column per database |

### 3.10 Notes for the other sections

- Section 5's `CommitChunk` uses `RETURNING chunk_id, (created_at = now()) AS inserted` (3.8); `xmax` is not
  readable in `RETURNING` on a partitioned table. `ReembedChunk` inserts a `chunk_vectors` row and nothing else.
- Every read path (recall arms, GetMemory, ListMemories, Reflect and MCP tools, GetPage/SearchPages, the
  export) applies the 3.8 visibility predicate; `PAGE_HIDDEN` is the page error; recall passes the four marker
  arrays (`doc_tomb`, `doc_pending`, `chunk_tomb`, `fact_hidden`) to every arm and to the async-index join.
- The fence is the try-lock over `engram_ns_fence_key`; the derivation lock is `engram_ns_derivation_key`; the
  document lock is `engram_doc_lock_keys`; exclusive takers make one 35 s attempt (3.3). Marker transactions
  take no lock beyond the shared fence.
- The ownership SQL of §5.5 is generated from the 3.3.1 table (`ownership_transitions`), including `ready`,
  `unready_target`, `return_abort` and `reconcile_out`; moves do not read the outbox.
- Section 9: the shard image runs `engramctl index` (3.3.4) as the owner; `synchronous_commit = local` is a role
  default; `maintenance_work_mem = 2 GB`; `pick_shard` derives `namespaces_count`; restore and failover follow 3.3.1.
- Intent objects are written by the API before the marker transaction (3.6); `engramctl restore replay` reads
  them; the catalog has no delete log.
- Section 8's static RLS check allows `relay_read_all` on `outbox` only; tables without `namespace_id` are
  exactly `shard_meta`, `outbox_cursors`, `ownership_transitions` and `goose_db_version`.
- Section 9's `shard_meta` carries `shard_id`; provisioning inserts it before the first namespace.

**Round-3 changes** (`reviews/round-3.md` finding → register id; the rows themselves are in D22):

| Finding | Register | Change in §3 and the SQL |
|---|---|---|
| P-1, P-3, P-4, P-6, A-3 | N111, N112, N113, N114 | insert-only content, `*_vectors` side tables, per-namespace partial HNSW through `engram_hnsw_ddl`, no generated or visibility columns, `fillfactor 100`, 128 GB sizing and IOPS table |
| P-10, P-8 | N113, N124 | `engram_partitioned_index_ddl`; `engram_copy_columns` / `engram_column_hash`; no generated column remains |
| H-19 | N113 | three disjoint lock key spaces |
| H-1, H-2, H-12, P-2, A-1 | N115, N116, N117 | `document_tombstones`, `chunk_tombstones`, `fact_hidden`, read-time predicate, `root_version`, `observation_inputs.document_id`, `page_version_inputs`, `PAGE_HIDDEN` |
| H-3, A-16 | N115 | `Invalidate`/`Restore` as one marker row each; `curation_log` re-applied at `CommitChunk` |
| A-15 | N118 | `entity_aliases.document_id`, `entity_mentions.mentioned_at`, mention-only `EntityRef` under `as_of` |
| H-17, H-23, H-24 | N119, N95, N121 | `derived_hidden`, `expunge_progress`, `engram_consumers_passed`; stamps and watermark; two-stage consolidation |
| H-18, P-12 | N120, N82 | derivation lock functions; single 35 s exclusive attempt |
| H-4, H-9, P-7, P-18 | N122 | `synchronous_commit = local`; `intent_key` on tombstones, `fact_hidden`, `deletion_log`; no catalog delete log; read fence rejects `frozen/delete` and `frozen/restore` |
| H-13, H-14 | N123 | catalog `system_identifier`, `timeline_id`; `reconcile_out`; `namespace_moves` as arbiter |
| H-5, H-6, H-22, A-2 | N125, N93 | `ready` state; `ready_target`, `unready_target`, `activate_target`, `return_abort`; `ownership_transitions` table and trigger |
| H-7, H-8, H-21, H-25 | N124 | no `move_applied`, `p0`, catch-up, `move_epoch > epoch` CHECK or cursor writes; reconcile queries; blob existence check |
| H-10, H-11, A-14 | N126 | `export_snapshots` without outbox cut, with `snapshot_started_at`, `base_version` |
| H-15, H-16 | N95, N104 | append-only `fact_consolidation`; nullable `body_key`/`body_hash`, `append_base_version` |
| P-5, P-19, P-16, H-20 | N131, N116 | `engram_entity_fuzzy` (`SECURITY DEFINER`); cleanup `DELETE` carries `namespace_id`; `pick_shard` derives the count; scheduler writes through `WithNamespaceTx(write)` |
| A-5, A-6 | N127 | `operations.kind` adds `delete_tenant`, `expunge`, `reembed`; `superseded_by`, `cancel_reason` |
| A-7 | N111 | `namespace_models`, catalog `embedding_model`, `ReembedNamespace` |
