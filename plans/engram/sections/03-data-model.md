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
   DML after the insert is a `DELETE` by the expunge (`engram_admin`) and the stub insert of
   `DerivedPurge` (N136). **Evidence outlives the facts it names (N135):** the three evidence
   tables have no foreign key to `facts` and die with their own version row only. Every table
   carries one class tag (`COMMENT ON TABLE`; N137, 3.3.1). Everything that changes
   sits in a narrow table that stays HOT (`fillfactor` 50 to 70): `documents`, `observations`,
   `pages`, the markers, `namespace_stats`. A delete or an invalidation therefore writes one
   small marker row and rewrites nothing that an HNSW or BM25 index points at. Rejected: flags
   on the vectored row with a partial `WHERE live` HNSW (a retire still rewrites a 2.4 KB tuple
   and re-indexes it, and every later visibility change lands on the same row again).
2. **Keys lead with `namespace_id`.** Every primary key and every B-tree, GIN and GiST index of a namespace-scoped table starts
   with `namespace_id` (D2), so a query touches one contiguous key range and the planner prunes one of the 16 hash partitions. The only
   shard-wide indexes are the outbox PK and the scheduler partial indexes. Rejected: `tenant_id` as the leading column.
3. **Row-Level Security on every namespace-scoped table**, policy `ns_isolation`:
   `namespace_id = current_setting('engram.namespace_id')::uuid` as both `USING` and `WITH
   CHECK`, enabled *and forced*, applied to the 16 partitions as well as to the parents.
   `engram_app` is `NOBYPASSRLS`. A transaction that forgot to set the scope errors (an unset GUC
   is `42704`, the empty string left after `SET LOCAL` expires is `22P02`), it never leaks. No
   tag predicate and no non-leakproof operator runs under RLS: tags are resolved once per recall
   against `documents.tags` into an allowed-document set, and the trigram lookup goes through a
   `SECURITY DEFINER` function that re-checks the namespace (N116, N131, P-5). Rejected:
   application predicates only; per-namespace tables.
4. **Sixteen hash partitions by `namespace_id`** for the eight large tables (`facts`, `chunks`, `fact_links`, `entity_mentions`,
   `observation_versions` and the three big vector tables). A namespace lives in exactly one partition of each, which is what lets
   **vector indexes be per namespace** and keeps per-partition `VACUUM` short. Rejected: one partition per namespace; no partitioning.
5. **Vectors are insert-only side tables with one partial HNSW per namespace (N111, N112).**
   `fact_vectors`, `chunk_vectors`, `observation_version_vectors` and `page_version_vectors` are keyed by content id
   and embedding model; the arms read the namespace's current model. There is no shared HNSW:
   below 2,000 vectors an arm scans exactly (at most 2,000 rows, about 3.2 MB), at 2,000 the
   stats sweeper builds the namespace's partial index through `engram_hnsw_ddl` (3.3.4), below
   1,000 it drops it. A query then visits only its own graph, a namespace delete or move cleanup
   is `DROP INDEX` plus batched `DELETE` with no graph repair, and a document purge dirties one
   small index. Index DDL has one owner, the index runner (N138).
6. **Deletion is a marker; physical work is the expunge (N115, N119).** `DeleteDocument` is one
   `document_tombstones` row, `documents.state = 'deleting'`, one `deletion_log` row and one
   outbox event, committed in milliseconds at any size after the intent object is durable
   (N122). The tombstone covers `(document_id, up_to_version)` (N133c): it hides the versions the
   document had at the delete, so the same `document_id` can be retained again at once, starting at
   `up_to_version + 1`. `Invalidate` inserts a `fact_hidden(invalidate)` row and `Restore` deletes only that row. Every read
   path applies the visibility predicate of 3.8 at ack; a throttled per-namespace `Expunge` workflow
   materialises derived hiding, then purges rows in batches, then rebuilds touched indexes. Deletes
   are rare; recall SLOs may degrade for a namespace while markers are `pending` (3.8).
7. **Derived versions carry evidence segments, not lineage (N117).** `root_version` is immutable; a version's derivation set is the
   inputs of its segment; hiding is a read-time `EXISTS` over those inputs plus the permanent `derived_hidden` materialisation, and a
   missing row hides (fail closed). Nothing is computed at delete time, so no snapshot can be stale and no walk can fail open.
8. **`tenant_id` is denormalised on every row** and the root tables carry `FOREIGN KEY (namespace_id, tenant_id) REFERENCES
   namespace_ownership` (blob keys, metering, exports and the deletion log need the tenant without a join). **Epoch appears on
   `namespace_ownership` and `outbox` only**, and `shard_id` on no data row (a self-check fails the migration if another table grows
   it). No identity or idempotency key includes the epoch (D11).
9. **Three disjoint lock key spaces (N113):** namespace fence `hashtextextended(ns, 0)`, derivation lock `hashtextextended(ns, 1)`,
   document lock `(hashtext(ns), hashtext(doc))`. Writers never wait for the fence; every exclusive taker makes one 35 s attempt (N82).
10. **Durability is local (N122).** Every role runs `synchronous_commit = local`; no synchronous
    standby exists, so a commit cannot hang and the relay's 60 s gap horizon is sound. The intent
    object is put **after** the marker commits and before the ack, so an *acknowledged* delete or
    invalidation survives restore and failover; a committed-but-unacknowledged one may be lost and
    the client's retry re-applies it. The replay floor lives in `catalog.shards` (N134).

Mutability, per table group:

| Group (class tag, N137) | Rows | Physical delete |
|---|---|---|
| **insert-only** (re-copied by `ins_seq`): `facts`, `chunks`, `fact_links`, `entity_mentions`, `observation_versions`, `page_versions`, the four `*_vectors`, `observation_inputs`, `observation_version_sources`, `page_version_inputs`, `document_version_chunks`, `*_version_meta`, `fact_consolidation`, `consolidation_proposals`, `consolidation_applied`, `curation_log`, `deletion_log`, `ingest_ledger` | grants + `BEFORE UPDATE` trigger, `fillfactor = 100`, `ins_seq` + `(namespace_id, ins_seq)` | expunge only, `engram_admin`; FK cascades from `facts` and `chunks` take vectors, links and mentions, **not evidence** (N135); a purged derived version becomes a stub |
| **mutable** (merge-diffed): `documents`, `observations`, `pages`, `entities`, `entity_aliases`, `document_versions`, `consolidation_state`, `consolidation_batches`, `observation_sources`, `page_sources`, `operations`, `export_snapshots`, `idempotency_keys`, `batch_jobs`, `quota_counters`, `token_usage`, `blob_tombstones`, `namespace_models`, markers (`document_tombstones`, `chunk_tombstones`, `fact_hidden`), `derived_hidden`, `expunge_progress` | `fillfactor` 50 to 70, HOT | expunge / namespace delete; markers: `Restore`, un-retire, the expunge's finish step |
| **expiring or derived** (excluded from a move): `token_usage_events` (30 d), `vector_indexes`, `namespace_stats` | | retention jobs; re-derived on the target |
| **shard-local**: `shard_meta`, `outbox_cursors`, `outbox`, `outbox_skipped`, `engram_seq_log`, `namespace_ownership`, `ownership_transitions` | state machine or log | `active` and `moved_out` ownership rows are never deleted |

### 3.2 Catalog schema (control-plane database `engram_catalog`)

One small PostgreSQL 16 with a streaming replica (D4), schema `public`, no RLS (only service
roles connect). Tables: `cells`, `tenants`, `shards`, `namespaces`, `namespace_moves`,
`idempotency_keys` (catalog-level writes, D1), `tenant_usage_daily` and `catalog_events` with the
`LISTEN/NOTIFY` trigger. There is no catalog delete log: delete intents are blob objects (N122).
Roles: `catalog_migrate` (owner), `catalog_app` (engram-api), `catalog_admin` (engramctl,
MoveService, ShardService).

**Catalog operations are derived, never stored (N127, N133d).** There is no catalog `operations` table: `DELETE_NAMESPACE` is
answered from `namespaces.state` (`deleting` → `RUNNING`, `deleted` → `SUCCEEDED`) and `DELETE_TENANT` from the `tenants` row
(`delete_operation_id`, `delete_requested_at`, `deleted_at`) through the view `tenant_delete_operations`.

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
  replay_floor timestamptz,       -- N134: delete-intent replay floor; lowered with min() by restore/failover, never raised
  max_namespaces integer NOT NULL DEFAULT 120, soft_cap_facts bigint NOT NULL DEFAULT 8000000,   -- N114, N142: 8 M target,
  hard_cap_facts bigint NOT NULL DEFAULT 12000000, volume_bytes bigint NOT NULL DEFAULT 600000000000,   -- 12 M cap, 600 GB
  namespaces_count integer NOT NULL DEFAULT 0, facts_estimate bigint NOT NULL DEFAULT 0, bytes_estimate bigint NOT NULL DEFAULT 0, ...
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

CREATE TYPE move_state AS ENUM ('planned', 'copying', 'frozen', 'reconciling', 'cutover', 'committed', 'cleaning', 'done', 'rolled_back');
CREATE TABLE namespace_moves (
  move_id uuid PRIMARY KEY, namespace_id uuid NOT NULL, tenant_id text NOT NULL,
  source_shard_id integer NOT NULL, target_shard_id integer NOT NULL,
  from_epoch bigint NOT NULL, to_epoch bigint NOT NULL CHECK (to_epoch = from_epoch + 1),
  state move_state NOT NULL DEFAULT 'planned',
  source_system_id bigint, source_timeline_id integer,   -- N123: re-read on every source activity; mismatch -> MoveFenced
  t_copy timestamptz, t_pre timestamptz, w_pre bigint,   -- N124: re-copy ins_seq >= engram_seq_floor(t_copy) / (t_pre)
  terminated_workflows text[] NOT NULL DEFAULT '{}', error text, created_by text NOT NULL, ...,
  frozen_at timestamptz,
  ready_at timestamptz,        -- (b') target incoming -> ready; rollback still possible
  committed_at timestamptz,    -- (a'') the catalog CAS cutover -> committed: the point of no return
  moved_out_at timestamptz,    -- (c) on the source; informational
  activated_at timestamptz,    -- (b'') target ready -> active
  finished_at timestamptz
);
CREATE UNIQUE INDEX namespace_moves_live_uq ON namespace_moves (namespace_id)
  WHERE state NOT IN ('done', 'rolled_back');                   -- at most one live move per namespace
```

`namespace_moves` has no outbox position: a move never reads or replays the outbox (N124).
It is the arbiter of the move, of restore and of failover, through one catalog CAS (N123, N125):
`UPDATE namespace_moves SET state = 'committed' WHERE move_id = $1 AND state = 'cutover'` is the point
of no return; restore and failover CAS `cutover → rolled_back` instead, or, reading `committed`,
complete (c), (b″) and (d) themselves. Exactly one CAS wins and the loser stops. The trigger
`catalog_check_move_transition` encodes the edges, so nothing leaves `committed` backwards.

**Config inheritance.** `tenants.config` and `namespaces.config` are `jsonb` objects holding the keys of D12; the database
checks only that each is an object, key validation is the Go config schema's job, and the resolver caches the deep-merged
`system ⊂ tenant ⊂ namespace` result. `namespaces.profile` is identity, never inherited; the embedding model is a column fixed
at creation, not a config key (N111).

**Placement policy.** `pick_shard(p_tenant_id)` runs inside the `CreateNamespace` transaction and **derives** `namespaces_count`
from the `namespaces` table (N131, P-16). Least-loaded first by the facts ratio, then the derived count, then id;
`FOR UPDATE SKIP LOCKED` lets concurrent creates pick different shards. `facts_estimate` and `large` are refreshed hourly from each
shard's `namespace_stats`; neither invalidates the resolver cache. Rejected: random placement; power-of-two choices.

`CreateNamespace` is two-phase (catalog row `creating` → shard `namespace_ownership` `active`/epoch 1, `namespace_stats` and
`namespace_models` rows inserted by `engram_app` → catalog row `active`). A crash leaves a `creating` row that the per-shard
op-sweeper completes or deletes after 2 min (`namespaces_creating_idx`); a client retry replays through `idempotency_keys`.

**Events and cache invalidation.** `catalog_events` is append-only; `AFTER INSERT` it `pg_notify('catalog_changes', json)` (D4).
Triggers on `namespaces`, `tenants` and `namespace_moves` insert an event for every routing-relevant change and skip stats-only
updates; retention is 30 days, so the table doubles as the audit log of every move and epoch bump. `tenant_usage_daily(tenant_id, day,
quota_key, used)` is the cross-shard rollup for tenant-level daily quotas, fed by per-minute deltas through the API (D4).

**Cutover and the catalog (N125).** The order is (b′) the target ownership row goes `incoming → ready`
(nothing routes to it); **(a″) the catalog CAS `cutover → committed`, the point of no return**, taken
after the mover re-checks its timeline; (c) the source row goes `frozen/move → moved_out` with
`target_shard_id` and `target_epoch`, executed only after the mover *read* `committed`; (b″) the target
row goes `ready → active`; (d) the catalog `namespaces` flip `WHERE epoch = e AND state = 'frozen'`,
retried indefinitely, with "already `(target, e + 1)`" as its only idempotent success. The API routes a
request that hits the `moved_out` row to the target using only the `WrongShardOrEpoch{MOVED_OUT}` detail
and verifies at the target's fence, so (d) is on no read path. Between (c) and (b″) there is no owner at
all, the simplest way to make "at most one writable owner" true. A failure before (a″) rolls back (3.3.1)
and thaws the source; safe because nothing routes to the target before (c).

### 3.3 Shard schema (identical on every shard)

Schema `public` (what the section 8 `--check-rls` query and the section 9 tooling assume),
PostgreSQL 16, extensions `vector`, `pg_search`, `pg_trgm`, `btree_gin`, `btree_gist`.

**Roles** (all `LOGIN`; passwords from the shard secret, section 9). All of them run
`synchronous_commit = local` as a role default (N122).

| Role | RLS | Grants | Used by |
|---|---|---|---|
| `engram_migrate` | owner (`BYPASSRLS`) | DDL; owns the `SECURITY DEFINER` functions `engram_cleanup_namespace` (N93) and `engram_entity_fuzzy` (N131) | migrations and the **index runner** (`engramctl index`, control host): the one process that issues index DDL at run time (N138) |
| `engram_app` | `NOBYPASSRLS`, `ns_isolation`; `lock_timeout = 2 s`, `statement_timeout = idle_in_transaction_session_timeout = 30 s` | `SELECT, INSERT` on every content table, `consolidation_proposals`, `outbox`, `deletion_log`; DML on mutable tables; `SELECT, INSERT` on `document_tombstones`; `SELECT, INSERT, DELETE` on `chunk_tombstones`, `fact_hidden`; `SELECT, DELETE` on `derived_hidden`; **no `UPDATE` or `DELETE` on any content row and none on a proposal (a discard is a new attempt, N43)**; `SELECT` on `namespace_ownership` (the fence is a plain read), `INSERT` of the first `active`/epoch-1 row, `UPDATE (state, freeze_reason)` for the one edge the trigger admits, the delete freeze | API and worker per-namespace transactions |
| `engram_relay` | `NOBYPASSRLS` + policy `relay_read_all` on `outbox`; no `engram_entity_fuzzy` | `SELECT` on `outbox`; all on `outbox_cursors`; nothing else | outbox relay, direct connection |
| `engram_move` | `NOBYPASSRLS`; restrictive `move_target_*` policies: writes only into an `incoming` namespace; 30 s statement and idle timeouts | content tables: `SELECT, INSERT`; mutable tables and markers: full DML; the ownership transitions of 3.3.1; `SELECT` on `outbox`, `outbox_cursors` (relay drain) and `engram_seq_log`; `SET session_replication_role`; `EXECUTE engram_cleanup_namespace`. No `DELETE` on content, no outbox or cursor write, no `engram_entity_fuzzy` | move executor |
| `engram_admin` | `BYPASSRLS`; 30 s statement and idle timeouts (raised per session only in `engramctl`: it holds the shared fence and writes outbox events) | all tables; the **only** role that `DELETE`s content rows; `engram_consumers_passed`, `engram_seq_sample` | `engramctl`, Expunge purge activities, shard-wide schedulers, retention |

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

**Lock discipline (N82, N113, N120).** No row is ever row-locked for fencing (`FOR SHARE` lets compatible lockers pass a waiting
`FOR UPDATE` and churns multixacts). Writers never wait: a refused `pg_try_advisory_xact_lock_shared` ends the statement at
once (`TestFence_TryLockRefusedBehindWaiter`). Exclusive takers make **one** attempt under `lock_timeout = 35 s`, longer than any
legal 30 s writer, so they cannot be starved and cannot start a retry storm. The three key spaces are distinct `pg_locks` tags
(one-argument keys `objsubid = 1`, the document key `objsubid = 2`).

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

Why a freeze cannot lose a write: a write commits only inside a transaction that held the shared lock while it read `active` at its
epoch; the freeze takes the exclusive lock, so no writer that read `active` before it can commit after it (D5, TLC `ShardMove`). Why
the derivation lock exists: Materialize must see every version whose inputs name the victim; a writer whose re-verification predates
the marker but whose commit postdates it holds the shared lock across both (N120; `Derivation.tla` has a configuration without it
that must fail).

#### 3.3.1 Shard-level tables

`shard_meta` (one row: `shard_id`, `schema_version`, version window; no replay floor, N134),
`outbox_cursors` (consumers `relay`, `index`, `kafka`; moves never touch it), `outbox`,
`outbox_skipped`, `engram_seq_log`, `deletion_log`, `namespace_ownership`, `ownership_transitions`,
and the per-namespace state tables
`namespace_stats`, `namespace_models`, `vector_indexes` (these three have a `namespace_id` and RLS).

```sql
CREATE TABLE namespace_ownership (
  namespace_id uuid PRIMARY KEY, tenant_id text NOT NULL, shard_id integer NOT NULL, epoch bigint NOT NULL CHECK (epoch >= 1),
  state ownership_state NOT NULL,                -- incoming|ready|active|frozen|moved_out
  freeze_reason text CHECK (freeze_reason IN ('move', 'delete', 'restore')),
  move_id uuid, move_epoch bigint,               -- source: start_move (pauses expunge and schedulers); target: the incoming row
  target_shard_id integer, target_epoch bigint,  -- on a moved_out row: a PERMANENT fence value, never deleted
  ..., UNIQUE (namespace_id, tenant_id),         -- + CHECKs tying state to move_id, freeze_reason and target_shard_id
);
```

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
| `freeze_restore` / `restore_done` | `active` → `frozen/restore` → `active` (epoch `greater`) | `engram_admin` | new epoch = catalog epoch + 1, written to the catalog first (N123); skipped for namespaces whose open move is `committed` or whose catalog state is `deleting` |
| `restore_delete` | `frozen/restore` → `frozen/delete` | `engram_admin` | replay of a namespace or tenant delete intent without passing through `active` (N122) |
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

Executed against the DDL as the three roles: illegal pairs are refused per role (`engram_app` epoch-2 insert, thaw, `ready → active`;
`engram_move` `freeze_reason = 'delete'`, `active → moved_out`, `DELETE` of a `moved_out` row), every legal edge of the table is accepted,
and `engram_admin` takes `reconcile_out` and `restore_delete` where `engram_move` and `engram_app` are refused.

The loader's column lists are `engram_copy_columns(table)`: attributes with `attgenerated = ''`
and `NOT attisdropped`, ordered by name, so both shards produce the same list (P-8; no generated
column exists in this schema and a self-check keeps it so). `engram_column_hash(table)` hashes the
`(name:type)` list, and Plan refuses unless the hashes of source and target match per table.

**Table classes and the insertion sequence (N137).** The class of every table is a `COMMENT ON TABLE`
tag set from one list in the DDL; a self-check fails the migration if a table has none, if `ins_seq`
exists off the insert-only class, or if an insert-only table lacks `(namespace_id, ins_seq)`. Every
insert-only table has `ins_seq bigint NOT NULL DEFAULT nextval('engram_ins_seq')`. `nextval` is drawn
at insert, not commit, so `ins_seq` is commit-ordered only within the writer lifetime;
`engram_seq_sample()` records `(now(), nextval)` in the ring `engram_seq_log` every minute (7 days
kept) and `engram_seq_floor(ts)` returns the sample at or before `ts − 10 min` (0 on a new shard),
the key lower bound a move re-copies from (N124). The loader copies `ins_seq` verbatim, so counts
filtered by it match on both sides. Rejected: `created_at` or UUIDv7 ids as the re-copy key.

**Restore and failover (N123, N134).** The restored or promoted shard comes up with `listen_addresses`
restricted. It first settles every open move that names it by the catalog CAS (3.2): `cutover →
rolled_back` (rolling back from the target with `rollback_target` and thawing the source), or, if it
reads `committed`, it completes (c), (b″) and (d) itself (`reconcile_out`). Only then does every row go
`frozen/restore` (a namespace with a `committed` move or a catalog `deleting` state is skipped),
`engramctl restore replay` apply the delete intents verbatim from `catalog.shards.replay_floor − 10
min`, per subject in `prev_operation_id` chain order, and `restore_done` bump the epoch. The floor
is outside the restorable state and is never raised while intents are retained (35 days).

**Schedulers** read `schedulable_namespaces` (`state = 'active'`); Expunge and the index jobs read `purgeable_namespaces`, which also
excludes a namespace with an open move (Plan to done). Their writes go through `WithNamespaceTx(write)` and `engramlint sql` checks the
fence prelude before every outbox insert (N131, H-20).

**Namespace stats are derived (N69).** `namespace_stats` is refreshed per namespace by the stats
sweeper (`engram_admin`): `live_facts` counts *visible* facts, `pending_markers` the open
tombstones, and `large` flips at 2,000 vectors of the current model (cleared below 1,000), which
the sweeper acts on through `engram_vector_index_plan` and `engram_hnsw_ddl` (3.3.4). `bytes_estimate`
is relation bytes (hidden and unpurged rows included): the capacity signal, not `live_facts` (N114).
`mentioned_histogram` is the monthly `mentioned_at` histogram behind the `as_of` selectivity estimate
(N138). `namespace_models` holds the current `embedding_model` (N111). `vector_indexes` is the machine
`requested → building → ready | failed` (and `dropping`) with a lease, `rows_at_build` and
`purged_since_build` (3.3.4).

#### 3.3.2 Outbox

```sql
CREATE TABLE outbox (seq bigint PRIMARY KEY DEFAULT nextval('outbox_seq'),            -- outbox_seq AS bigint CACHE 1
  namespace_id uuid NOT NULL DEFAULT current_setting('engram.namespace_id')::uuid, tenant_id text NOT NULL DEFAULT current_setting('engram.tenant_id'),
  epoch bigint NOT NULL DEFAULT current_setting('engram.epoch')::bigint, event_type text NOT NULL CHECK (event_type ~ '^[A-Z][A-Za-z0-9]{2,63}$'),
  payload bytea NOT NULL CHECK (octet_length(payload) <= 16384), created_at timestamptz NOT NULL DEFAULT now());   -- + INDEX (namespace_id, seq)
```

The PK is the global `seq` because the relay reads shard-wide in `seq` order; `namespace_id`,
`tenant_id` and `epoch` are stamped from the transaction scope. **Every event is bounded by
construction (N80):** ids are 16-byte `bytes`, an event carries at most 256 ids, a larger set is
paged as consecutive events with `page`/`page_count`, and above 4,096 ids the event carries counts
only with `ids_elided = true` (consumers then delete by the indexed `(namespace_id, document_id)`
query, N119). The `CHECK` is the backstop. A delete is **one O(1) marker event**
(`DocumentDeleted`), not a per-id list. The outbox `INSERT` is the last statement of every write
transaction (A-F1). `deletion_log(namespace_id, intent_key, kind, subject_id, epoch,
operation_id, prev_operation_id, effect, deleted_at)` is the shard-local "already applied" record of
marker transactions, written in the marker transaction; its key is the **name of the intent object**
(N122), so `engramctl restore replay` applies each intent at most once. `prev_operation_id` is the
subject's previous entry (the replay chain) and `effect` the marker's exact effect, from which a
duplicate attempt re-puts the intent. The intent is put after the commit and before the ack.

#### 3.3.3 Ingest ledger, documents, versions

`ingest_ledger` is append-only (trigger: `UPDATE` never; `DELETE` only for `engram_admin` and the
owner's cleanup function). Bodies up to 64 KiB are inline, larger ones live in the **owner-keyed**
blob `ledger/{ledger_id}` (`ledger_id` is minted before the put; a `CHECK` pins the key, N104, N7).
Ledger rows are never purged by `REPLACE` or `APPEND` retirement, only by an explicit delete expunge.

```sql
CREATE TABLE documents (            -- MUTABLE, fillfactor 70
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL,
  current_version integer NOT NULL DEFAULT 0, state document_state NOT NULL DEFAULT 'active',   -- active|deleting|deleted
  life_start integer NOT NULL DEFAULT 1,                                       -- first version of the current life (N133c); a revive sets it
  item_timestamp timestamptz, context text NOT NULL DEFAULT '',
  tags text[] NOT NULL DEFAULT '{}' CHECK (engram_tags_valid(tags)),        -- the ONLY place tags live (N113)
  metadata jsonb NOT NULL DEFAULT '{}', document_hash bytea, summary_blob_key text, summary_hash bytea,
  ..., deleted_at timestamptz, PRIMARY KEY (namespace_id, document_id), CHECK ((state = 'active') = (deleted_at IS NULL)),
  CHECK (state = 'active' OR (summary_blob_key IS NULL AND summary_hash IS NULL AND document_hash IS NULL
                              AND context = '' AND metadata = '{}' AND tags = '{}'))      -- N115: the marker clears the content columns
);
CREATE INDEX documents_tags_gin ON documents USING gin (namespace_id, tags);
CREATE INDEX documents_metadata_gin ON documents USING gin (namespace_id, metadata jsonb_path_ops);   -- metadata filters resolve into $allowed_docs (N139)

CREATE TABLE document_versions (    -- MUTABLE: status, chunks_done, body set once
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL, version integer NOT NULL,
  content_hash bytea NOT NULL, status version_status NOT NULL DEFAULT 'ingesting', update_mode update_mode NOT NULL,
  append_base_version integer CHECK (append_base_version IS NULL OR append_base_version < version),
  operation_id uuid NOT NULL, ledger_id uuid NOT NULL, chunk_count integer, fact_count integer,   -- fact_count: written once by FinalizeVersion (N138)
  chunks_done integer NOT NULL DEFAULT 0,
  body_hash bytea, body_key text,                                              -- H-16 / N104: NULLABLE until the body blob is stored
  ..., PRIMARY KEY (namespace_id, document_id, version),
  CHECK (body_key IS NULL OR body_hash IS NOT NULL),
  CHECK (body_key IS NULL OR body_key = 'ver/' || document_id || '/v' || version::text),   -- owner-keyed (N104)
  CHECK ((update_mode = 'append') = (append_base_version IS NOT NULL))
);
```

`LoadItem` creates the version row without a body, then stores the reconstructed body as the
owner-keyed blob `ver/{document_id}/v{n}` (cross-document dedup of bodies is given up, N104) and sets `body_key`/`body_hash` `WHERE body_key IS NULL`;
`LoadItem(APPEND)` materialises the base body from the ledger chain up to the nearest version that
has one (`append_base_version`), under the document lock. At most one `active` version per
document (partial unique index). `document_version_chunks(…, content_hash, chunk_id, ordinal)` is
insert-only: it carries membership **and the chunk's position in that version**, because a chunk
row (immutable) cannot carry an ordinal that changes across versions.

**Delete versus retain (N133c).** `documents.state = 'deleting'` is set in the same transaction as the tombstone, which records
`up_to_version = max(document_versions.version)` (under the document lock, in-flight versions included). A `Retain` of a `deleting`
document is **not refused**: the ack transaction *revives* the row (`state = 'active'`, `current_version = 0`, fresh content columns for
the new life) and assigns the version `greatest(max(document_versions.version), max(document_tombstones.up_to_version)) + 1`, which
becomes `life_start`. Every row inserted from then on carries `document_version >= life_start > up_to_version`, so the predicate leaves it
visible while the covered rows are hidden and then purged. Chunk identity is `(document_id, content_hash, life_start)`: the same text
retained again is a new chunk row. An `APPEND` after a delete starts from an empty base. The Expunge deletes the `documents` row only
while it is still `deleting`, no open tombstone with a higher `up_to_version` exists and no `document_versions` row remains (C-12).

**The per-document advisory lock is what `CommitChunk` relies on** (N40, N83): shared try-lock,
plain reads of the version row and `documents.current_version`, proceed only on `status =
'ingesting'`; `FinalizeVersion` and `DeleteDocument` take the key exclusive, so they wait for
in-flight commits and a later commit sees the terminal status. The retain ack's version assignment
keeps only the `documents` row lock.

#### 3.3.4 Chunks, facts and vectors (partitioned)

```sql
CREATE TABLE chunks (               -- INSERT-ONLY; identity = content hash within the document's current life
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, chunk_id uuid NOT NULL, document_id text NOT NULL,
  document_version integer NOT NULL, life_start integer NOT NULL,               -- inserting version (N133c); documents.life_start at insert
  content_hash bytea NOT NULL, header_hash bytea NOT NULL,                      -- header excluded from the identity (N6)
  heading_path text NOT NULL DEFAULT '', header text NOT NULL DEFAULT '',
  text text NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 16384),            -- <= 4,000 chars (D8)
  mentioned_at timestamptz NOT NULL,                                            -- N86: max(timestamp of every item the chunk covers)
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, chunk_id), UNIQUE (namespace_id, document_id, content_hash, life_start)
) PARTITION BY HASH (namespace_id);

CREATE TABLE facts (                -- INSERT-ONLY
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, memory_id uuid NOT NULL,
  document_id text NOT NULL, document_version integer NOT NULL,                 -- the version whose CommitChunk inserted the fact (N133c)
  chunk_id uuid NOT NULL, ordinal smallint NOT NULL,
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
  document_id text NOT NULL, document_version integer NOT NULL, chunk_id uuid NOT NULL, fact_type fact_type NOT NULL,
  mentioned_at timestamptz NOT NULL,                                          -- immutable copies: the arm filters INSIDE the scan (type filter: N138)
  embedding halfvec(768) STORAGE MAIN NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, memory_id, embedding_model),
  FOREIGN KEY (namespace_id, memory_id) REFERENCES facts ON DELETE CASCADE
) PARTITION BY HASH (namespace_id);
-- chunk_vectors(…, chunk_id, embedding_model, embedding_effective_at, document_id, document_version, mentioned_at, embedding),
-- observation_version_vectors(…, ov_id, embedding_model, observation_id, version, effective_at, embedding) and the unpartitioned
-- page_version_vectors(…, pv_id, embedding_model, page_id, version, effective_at, embedding) likewise; every insert-only table also has ins_seq
```

Decisions:

| Choice | Decision | Rationale | Rejected |
|---|---|---|---|
| vectors | side tables keyed `(content id, embedding_model)`; `ReembedChunk` and a model change insert rows; a model change is the `ReembedNamespace` workflow (insert, index, flip `namespace_models`, expunge the old), never a config flip | the vectored row never changes; versioned re-embed (A-7) | embedding on the content row |
| `chunk_vectors` key | `(chunk_id, embedding_model, embedding_effective_at)` — one extra column beyond N111's key | a summary refresh re-embeds a chunk without re-extracting (N110a); under `as_of = T` the arm admits rows with `embedding_effective_at <= T` and `mentioned_at <= T` and takes the best admitted row per chunk (N85) | replacing the row (not insert-only) |
| immutable copies on vector rows | `document_id`, `document_version`, `chunk_id`, `mentioned_at` / `effective_at`, and `fact_type` on `fact_vectors` | visibility, `as_of` and the type filter become in-scan filters, so an iterative HNSW scan refills past hidden rows without a join per candidate | a join to the content table per candidate; post-Top-K filtering (truncation) |
| `extraction_key` on facts only | the chunk row has no key; `FinalizeVersion` inserts `fact_hidden(cause = 'reextract')` for facts of a kept chunk whose key differs from the current one and flags dependent observations `stale_write` (N58, N135) | a chunk row cannot change; the unique key includes `extraction_key` so the re-extracted twin can coexist | key on `chunks`, updated in place |
| `fact_type` | enum column on `facts`, copied onto `fact_vectors`; not in the BM25 index | the vector arm filters it inside the scan (N138); the lexical arm applies it after its over-fetched Top-K | a generated `fact_type_code` (no generated columns, P-10) |

**Per-namespace partial HNSW (N112, N138) and the index runner.** No vector index is declared in the
static DDL. A hash-partitioned table keeps all rows of a namespace in exactly one partition (the
small `page_version_vectors` is unpartitioned and indexed on the table), so a namespace owns **one
index per (vector table, embedding model)**; with 120 namespaces per shard (N142) that is ≈ 360
indexes expected, cap 450, alert at 400. `CREATE INDEX CONCURRENTLY` cannot run inside a transaction
or on a partitioned parent, so the schema ships functions that *generate* the statements and **one
process executes them: the index runner** (`engramctl index` on the control host, role
`engram_migrate`; no other role has the privilege):

```sql
SELECT * FROM engram_vector_index_plan($ns);        -- per vector table: vectors of the CURRENT model, indexed?, action = create | drop | none
SELECT index_state, statement FROM engram_hnsw_ddl('fact_vectors', $ns, $model, 'create');   -- index_state: absent | valid | invalid
--  CREATE INDEX CONCURRENTLY fv_c189d1d65214_2db1c3 ON fact_vectors_p02
--    USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 128)
--    WHERE namespace_id = '0190c000-…-000000000001' AND embedding_model = 'nomic-embed-text-v1.5'
SELECT statement FROM engram_hnsw_ddl('fact_vectors', $ns, $model, 'drop');     -- DROP INDEX CONCURRENTLY IF EXISTS fv_…
```

`vector_indexes` is the machine `requested → building → ready`, or `failed` (and `dropping`). The
stats sweeper inserts `requested` at ≥ 2,000 vectors of the current model (or retries `failed`) and
`drop` below 1,000; the runner takes a lease (`building`, `lease_until`; an expired lease is
re-leased), **reads `index_state` and drops an `invalid` index before every build (never `IF NOT
EXISTS`: it would accept an invalid index and leave the namespace on the exact path)**, builds,
and marks `ready` with `rows_at_build`. Builds are serialised per shard with `maintenance_work_mem =
2.4 KB × vectors` (≤ 5 GB) and `shm_size = 8g`, and the runner refuses to start one while
`engram_old_snapshots` is non-empty (a backend `xmin` older than 5 min). Names are deterministic,
so `rollback_target` and namespace-delete `DropIndexes` drop by name and orphans cannot hide.
Queries that must match the partial-index predicate carry the namespace id and model as literals
(`plan_cache_mode = force_custom_plan`). On a move target the indexes are built only **after** the
bulk copy (P-8).

**Hygiene (N138).** The purge increments `vector_indexes.purged_since_build` (one writer, the
expunge's bookkeeping). A touched index is rebuilt by the runner when `engram_index_hygiene_due`
lists it, at 1 % of `rows_at_build` or 2 k elements, whichever comes first, then `VACUUM
(INDEX_CLEANUP ON)`. Vector partitions carry `vacuum_index_cleanup = off` permanently, so autovacuum
never repairs a graph (repair costs 5 to 6 times a rebuild). Budget ≈ 0.35 ms per vector.

**Ordinary indexes on partitioned tables (P-10)** added later follow `engram_partitioned_index_ddl(parent, index_name, columns, using,
where)`: `CREATE INDEX … ON ONLY` the parent (invalid), then per partition `CREATE INDEX CONCURRENTLY` and `ALTER INDEX … ATTACH
PARTITION`. No generated column exists, so no rewrite-only migration does either.

**Semantic-arm plan (N138).** Go estimates the eligible rows before an arm runs: the sum of
`document_versions.fact_count` over the current versions of `$allowed_docs`, or the monthly
`namespace_stats.mentioned_histogram` for `as_of`. Below θ eligible rows (5 k, fixed by M0.6's
selectivity sweep) the arm runs the **exact path** driven from `facts_doc_idx` or
`facts_mentioned_idx`, joined to the vector table by primary key, `ORDER BY distance LIMIT cap`
(no `(namespace_id, document_id)` index on any vector table). Above θ it runs the HNSW iterative scan
with `hnsw.max_scan_tuples = min(4 × ef_search / s, 100 k)` (`s` = selectivity) and sets
`RecallStats.partial` when the scan is exhausted. Arm transactions `SET LOCAL enable_seqscan = off,
max_parallel_workers_per_gather = 0`, so a partition scan cannot be chosen. A namespace below 2,000
vectors reads `fact_vectors_model_idx` and sorts (≤ 2,000 rows); chunk and observation arms follow
the same plan. Executed earlier on the applied schema as `engram_app` under RLS (20,003 vectors):
the partial index `fv_c189d1d65214_2db1c3` on `fact_vectors_p02` was chosen, `Order By: (embedding
<=> …)`, with the marker filters inside the scan.

#### 3.3.5 Links, entities, mentions

`fact_links` (partitioned, insert-only): one row per edge; entity/temporal/semantic edges are undirected, stored once with `src <
dst`; causal edges are directed. Per-fact caps (temporal ≤ 20, semantic ≤ 10, entity ≤ 10 per shared entity) are enforced by the
linker. A link is inserted in the `CommitChunk` of its newer endpoint; the expunge deletes links through the FK cascade from `facts`
(links are graph structure, not evidence). The graph arm joins both endpoints and applies the visibility predicate to each (3.8).

`entities` is mutable (`mention_count`, `last_seen_at`, `merged_into`; the expunge recomputes
`canonical_name` from the remaining mentions); `entity_aliases` gains `document_id` and `document_version`
(N118, N133c) so the expunge deletes the victim's aliases and no others; `entity_mentions` (partitioned, insert-only) carries the
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
  stub boolean NOT NULL DEFAULT false,                                          -- N136: a purged version is a content-free stub (text '', source_count 0)
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace_id, observation_id, version), UNIQUE (namespace_id, ov_id)    -- ov_id: the single-column BM25 key_field
) PARTITION BY HASH (namespace_id);
CREATE TABLE observation_version_meta (  -- INSERT-ONLY, write-once (N33)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL, version integer NOT NULL,
  superseded_at timestamptz NOT NULL, PRIMARY KEY (namespace_id, observation_id, version)   -- FK to the version: DEFERRED, no cascade (the stub replaces the row)
);
CREATE TABLE observation_inputs (    -- INSERT-ONLY: every fact SHOWN to the stage-2 writer of version v (N41, N121)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, observation_id uuid NOT NULL, version integer NOT NULL,
  fact_id uuid NOT NULL, document_id text NOT NULL, document_version integer NOT NULL,   -- the fact's, denormalised (N117, N133c)
  PRIMARY KEY (namespace_id, observation_id, version, fact_id)                  -- + indexes (namespace_id, document_id, document_version) and (namespace_id, fact_id)
);                                                                              -- NO foreign key to facts (N135); observation_version_sources likewise, with the two document columns
CREATE TABLE derived_hidden (        -- the materialisation of the read predicate, written by Expunge.Materialize (N119)
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, kind text NOT NULL CHECK (kind IN ('observation', 'page')), id uuid NOT NULL,
  root_version integer NOT NULL, from_version integer NOT NULL, cause_kind text NOT NULL CHECK (cause_kind IN ('document', 'invalidation')),
  cause_id text NOT NULL, ..., PRIMARY KEY (namespace_id, kind, id, root_version, cause_kind, cause_id)
);
```

`page_versions` (insert-only, `root_version`, `pv_id`, `text`, `stub`), `page_version_meta`
(write-once `superseded_at`) and `page_version_inputs(page_id, version, kind ∈ {fact, observation},
source_id, source_version, document_id, document_version)` follow the same shape; the input rows
are polymorphic and evidence, so they carry no FK to their source. **Pages are searchable (N139):**
`page_versions.text` has a BM25 index and `page_version_vectors` follows the N111/N112 rules, so
pages share the visibility and purge path of observations.

**Evidence segments (N117, N135).** The derivation set of `(O, v)` is `observation_inputs(O, w)` for
`root_version(v) ≤ w ≤ v`. `(O, v)` is **visible** iff a version row exists (**fail closed**: a missing
row means hidden, and a stub is never served), no input in its segment names a document version
covered by a tombstone or a fact with a `fact_hidden(invalidate)` row, no `derived_hidden` row covers
it, and at least one cited source is visible (`proof_count > 0`, judged from the source's own
document columns, so a purged fact does not matter). **`fact_hidden(reextract)` hides nothing
derived:** re-extraction is a write, `FinalizeVersion` flags dependent observations `stale_write` in
its own transaction and consolidation evolves them; the old-key facts are purged after 1 h. **Served
at `T`** is the version current at `T`; when it is hidden nothing is served for that observation at
`T`, and retirement is filtered by its own time. A page version is hidden by the same rule with depth
two (fact → observation → page; pages never feed observations, Reflect output is never stored): two
`EXISTS`, not a walk. A hidden current page returns `PreconditionFailed{PAGE_HIDDEN}` until the
refresh lands.

**`derived_hidden` rows for a document cause are permanent**: an older version written with the
victim in view must never resurface at any `as_of` (verified again on this schema: after the
materialisation `as_of` 2026-01-02 and 2026-01-05 both return nothing derived from the victim).
Rows with `cause_kind = 'invalidation'` are deleted by `Restore`. They are removed physically only
with the observation or page itself or the namespace. **`DerivedPurge` (N136)** turns every version
a document-cause row covers into a content-free stub in one admin transaction: delete the version
row (its inputs, sources, vector row and BM25 entry cascade; `*_version_meta` survives through its
deferred FK) and insert `(id, version, root_version, effective_at, text = '', stub = true)`; for
pages the `.md` blob goes to `blob_tombstones`. The stub keeps the D9 range arithmetic and the
fail-closed predicate valid; it is an insert, never an `UPDATE`.

**The derivation commit rule (N120).** Every writer of a derived version (`ApplyBatch`, degraded-mode
root rebuilds and merges, `CommitPageVersion` for `page/v1` and `page_full/v1`) commits in one
transaction that (1) try-locks the derivation lock shared, (2) re-verifies every rendered input in a
fresh statement (`engram_facts_all_visible` for fact inputs against all open tombstones, `chunk_tomb`
and `fact_hidden` of both causes; `engram_obs_version_hidden(…, engram_doc_tomb(ns, false))` for
observation-version inputs), (3) checks its base (`engram_derivation_base_ok`: `current_version =
base_version` and visible; a root rebuild has none), (4) on any failure rolls back and re-derives from
current evidence. The lock is never held across an LLM call.

**Two-stage consolidation (N121).** Stage 1 (routing, one call per batch of 8 facts) persists
nothing textual; stage 2 (one call per touched observation) writes the text, and `inputs(O, v)`
are the facts shown. A `merge` is a root rebuild of the survivor. **Proposal lifecycle:**
`consolidation_proposals` is write-once and keyed `(batch_key, attempt)`; every `update` and `merge`
op records its `base_version` (a `CHECK`); `consolidation_batches.state ∈ {routed, stored, applied,
discarded, capacity}` with `attempt` ≤ 4. `ApplyBatch` applies an `update` only while the base is
current, else the **whole** proposal is discarded (`UPDATE` of the batch state, a new attempt, a new
proposal row) and the batch re-routed; a capacity re-run is a new attempt with `prompt_variant =
'capacity'`. `engram_app` holds `SELECT, INSERT` on proposals: nothing is ever deleted. 'Already
applied' is `state = 'applied'`, written with the `done` stamps; an all-skip batch applies zero ops.
`consolidation_applied` references `(batch_key, attempt)` with RESTRICT.

**Consolidation bookkeeping is append-only (N95, H-15, H-23).** `fact_consolidation(namespace_id,
memory_id, stamped_at, note ∈ {done, failed, capacity}, batch_key)` has `PRIMARY KEY (namespace_id,
memory_id, stamped_at)` and is never updated; a fact is consolidated iff a `done` stamp exists,
retryable iff its latest stamp is `failed` and older than 7 days (`engram_pending_facts`).
`consolidation_state.watermark_memory_id` advances only to `engram_consolidation_watermark(ns)`:
just below the smallest unconsolidated fact **whether visible or marker-hidden** (a fact hidden by
`Invalidate` that `Restore` later reveals must still be pending; a re-extracted fact stops pinning it
when the 1 h purge removes it), and never past a fact whose `ins_seq` is above `engram_seq_floor(now())`
(C-19: the guard is the insertion sequence, not an id timestamp; ids are minted per attempt inside the
transaction).

#### 3.3.7 Markers, Expunge state, operations, metering, exports

```sql
CREATE TABLE document_tombstones (   -- Delete(document): ONE row covering (document_id, up_to_version); Expunge progress lives in expunge_state
  namespace_id uuid NOT NULL, tenant_id text NOT NULL, document_id text NOT NULL,
  up_to_version integer NOT NULL CHECK (up_to_version >= 1),                    -- hides document_version <= up_to_version (N133c)
  deleted_at timestamptz NOT NULL,
  operation_id uuid, intent_key text, event_seq bigint NOT NULL,                 -- N122: intent put AFTER the commit; N115: the delete event's seq (P-15)
  expunge_state text NOT NULL DEFAULT 'pending' CHECK (expunge_state IN ('pending', 'materialized', 'purged')),
  materialized_at timestamptz, purged_at timestamptz, ..., PRIMARY KEY (namespace_id, document_id, up_to_version)
);
CREATE TABLE fact_hidden (namespace_id uuid, tenant_id text, memory_id uuid, hidden_at timestamptz NOT NULL DEFAULT now(),
  cause text NOT NULL DEFAULT 'invalidate' CHECK (cause IN ('invalidate', 'reextract')), reason text NOT NULL DEFAULT '',
  intent_key text, PRIMARY KEY (namespace_id, memory_id, cause));                                                 -- N135; FK cascade from facts
-- chunk_tombstones(chunk_id, retired_at, reason ∈ {replace, reextract}; FK cascade from chunks); curation_log (insert-only, no FK: it outlives
-- the purge of the old fact); expunge_progress(unit, table_name, phase ∈ {materialize, purge, derived_purge, hygiene, finish}, last_key, …)
```

- `DeleteDocument` = `engram_try_ns_fence`, exclusive document lock, `expected_version` compared here, `documents.state =
  'deleting'` with the content columns cleared (summary blob to `blob_tombstones`), one `document_tombstones` row
  (`up_to_version`, the delete event's `event_seq`), one `deletion_log` row, one outbox event, commit; **then** the intent put,
  then the ack. A duplicate attempt re-puts the committed marker's own intent. Nothing else is touched.
- `Invalidate(f)` = `INSERT fact_hidden (cause 'invalidate')` + `curation_log` + `deletion_log` + outbox; `Restore(f)` = the
  exclusive derivation lock, `DELETE fact_hidden WHERE cause = 'invalidate'`, `DELETE derived_hidden WHERE cause = ('invalidation', f)`,
  `curation_log` row. **Restore is exact because nothing else encodes the invalidation** (executed: the visible sets before
  `Invalidate` and after `Restore` are equal, and the `reextract` row of the same fact survives it).
- `FinalizeVersion(REPLACE)` inserts `chunk_tombstones(reason = 'replace')` for chunks not in the new membership (one statement),
  `fact_hidden(reextract)` for old-key facts of kept chunks, and `observations.stale_write` for observations whose current segment
  names them; an un-retire on a flap is `DELETE FROM chunk_tombstones`. Curation of a re-extracted twin is re-applied at
  `CommitChunk` from `curation_log` (A-16).
- **Expunge (N119, N136)**, one workflow per namespace, every activity fenced `active` at the epoch and skipped while a move is open,
  at most two per shard: (1) **Materialize** under the exclusive derivation lock, re-reading `fact_hidden` and the open tombstones in
  every batch: insert `derived_hidden`, mark `stale_delete`, set `materialized`, start a system `ExportSnapshot` if it expired the latest
  one; (2) **Purge** in batches of 1,000, **paced by WAL** (≤ 25 MB/s per shard), after `engram_consumers_passed(event_seq)`:
  targets `MARKERS`, `CHUNK_TOMBSTONES` and `REEXTRACTED_FACTS` (1 h grace; facts, vectors, links, mentions, marker; **never
  evidence rows**), `OLD_EMBEDDING_MODEL`, a document's facts, chunks, aliases, curation rows, ledger rows and blobs; (2b)
  **DerivedPurge** and the `reflect/` transcripts; (3) **index hygiene** (3.3.4); (4) **finish**: `purged`, tombstone deleted after
  24 h, the `documents` row only by the C-12 rule. SLA: materialize ≤ 15 min, purge ≤ 24 h, index rebuilt ≤ 48 h. **Degraded mode while a
  marker is `pending`** (accepted): the observation and page arms pay a per-candidate `observation_inputs` lookup (≈ 10 to 20 ms per
  arm), consolidation runs only root rebuilds, the rerank-skip SLO is suspended, an alert fires after 15 min.

`operations` (kinds are the proto `OperationKind` names lower-cased: `retain_document, delete_document, delete_namespace,
consolidate, refresh_page, create_snapshot`; `cancel_reason ∈ {client_request, document_deleted, namespace_deleting}`; `superseded_by` is the
newer document **version**; N139. `MOVE_NAMESPACE` and `DELETE_TENANT` live in the catalog; `delete_document` is the expunge singleton
plus the tombstone's `operation_id`, N136), `idempotency_keys` (24 h),
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
the eight big tables is not partitioned; if a table lacks its class tag or `ins_seq` and its index disagree with the tag; if an evidence table references `facts`; if
`fact_hidden` is not keyed by cause; if `engram_app` can update or delete a proposal; if the relay or mover can execute
`engram_entity_fuzzy`; and if a role lacks `synchronous_commit = local` or (app, move, admin) the 30 s timeouts:

```sql
CREATE POLICY ns_isolation ON facts
  USING      (namespace_id = current_setting('engram.namespace_id')::uuid)
  WITH CHECK (namespace_id = current_setting('engram.namespace_id')::uuid);        -- identical on every table and partition
CREATE POLICY move_target_ins ON facts AS RESTRICTIVE FOR INSERT TO engram_move WITH CHECK ((SELECT engram_ns_is_incoming()));
-- move_target_upd / move_target_del likewise, on every namespace-scoped table except namespace_ownership
CREATE POLICY relay_read_all ON outbox FOR SELECT TO engram_relay USING (true);
CREATE TRIGGER facts_insert_only BEFORE UPDATE ON facts FOR EACH ROW EXECUTE FUNCTION engram_forbid_update();   -- every content table
```

Executed as the three roles: `engram_app` `UPDATE`/`DELETE` of content → `permission denied`; `engram_admin` `UPDATE facts` →
`facts_p02 is insert-only` (`42501`); a foreign `namespace_id` → `new row violates row-level security policy`; no scope →
`unrecognized configuration parameter`. Tables without `namespace_id` are exactly `shard_meta`, `outbox_cursors`,
`ownership_transitions`, `engram_seq_log` and goose's `goose_db_version`; `pg_policies.qual` renders the policy as
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
| `outbox_cursors`, `engram_seq_log` | `consumer`; `sampled_at` | no | no | none | shard-level; relay-owned / the `ins_seq` sample ring; moves never touch them |
| `outbox_skipped`, `deletion_log` | `(namespace_id, consumer, seq)`; `(namespace_id, intent_key)` | PK | yes | PK | `deletion_log` key = intent object name |
| `ingest_ledger` | `namespace_id, ledger_id` | PK | yes (FK) | PK, doc, op, hash, received | append-only |
| `documents`, `document_versions`, `document_version_chunks` | `(ns, document_id[, version[, content_hash]])` | PK | yes | PK; tags GIN; active (partial unique); op; chunk | `documents.tags` is the only tag store |
| `chunks` (16), `facts` (16) | `(ns, chunk_id)`, `(ns, memory_id)` | PK, partition key | yes | PK; unique `(doc, hash)` / `(chunk, key, hash)`; doc; mentioned; facts: occurred (btree + GiST) | BM25 per partition; no tags, no vector, no flags |
| `fact_vectors` (16), `chunk_vectors` (16), `observation_version_vectors` (16), `page_version_vectors` | `(ns, id, embedding_model[, embedding_effective_at])` | PK, partition key | yes | PK; `*_model_idx` (exact-scan path) | per-namespace partial HNSW created by `engram_hnsw_ddl`, not in the static DDL |
| `fact_links` (16), `entity_mentions` (16) | `(ns, src, dst, type)`, `(ns, memory_id, entity_id)` | PK, partition key | yes | PK; reverse; entity | insert-only |
| `entities`, `entity_aliases` | `(ns, entity_id)`, `(ns, alias_norm)` | PK | yes | PK; canonical (partial unique); trgm GIN (via the definer function); alias by document | |
| `observations`, `pages` | `(ns, observation_id)`, `(ns, page_id)` | PK | yes (FK) | PK; stale; tags GIN / name | mutable state |
| `observation_versions` (16), `page_versions` | `(ns, observation_id, version)`, `(ns, page_id, version)` | PK, partition key (versions) | yes | PK; `(ns, ov_id)` unique; effective | `ov_id` exists only because the BM25 `key_field` must be one column |
| `observation_version_meta`, `page_version_meta` | like their versions | PK | yes | PK | write-once `superseded_at`; deferred, non-cascading FK (stubs) |
| every insert-only table | + `ins_seq` | | | `(ns, ins_seq)` | the move's re-copy key (N137) |
| `observation_inputs`, `observation_version_sources`, `page_version_inputs` | `(ns, obs, version, fact_id)`; `(ns, obs, version, memory_id)`; `(ns, page, version, kind, source_id, source_version)` | PK | yes | PK; `(ns, document_id, document_version)`; `(ns, fact_id)` / `(ns, kind, source_id)` | evidence: no FK to `facts` (N135); `document_id` and `document_version` denormalised for the read predicate and the expunge |
| `observation_sources`, `page_sources` | `(ns, …)` | PK | yes | PK, memory/source | mutable working sets |
| `document_tombstones`, `chunk_tombstones`, `fact_hidden`, `curation_log` | `(ns, document_id, up_to_version)`, `(ns, chunk_id)`, `(ns, memory_id, cause)`, `(ns, memory_id, at)` | PK | yes | PK; open tombstones partial | the markers (N115, N133c) |
| `derived_hidden`, `expunge_progress` | `(ns, kind, id, root_version, cause_kind, cause_id)`; `(ns, unit, table_name)` | PK | yes | PK; cause | Expunge stage state (N119) |
| `consolidation_batches`, `consolidation_proposals`, `consolidation_applied`, `fact_consolidation`, `consolidation_state`, `batch_jobs` | `(ns, batch_key[, attempt])` … | PK | yes | PK; round; batch | |
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
| `DocumentDeleted` | the delete marker transaction; again by the Expunge | `document_id, up_to_version, deleted_at, phase` (`MARKED` \| `PURGED`) — **O(1)**, no id list; index consumers delete by the indexed `(namespace_id, document_id)` query restricted to `document_version <= up_to_version` |
| `FactInvalidated`, `FactRestored` | Invalidate/Restore | `fact_id, hidden_at, reason` / `fact_id` |
| `ObservationUpserted`, `ObservationRetired` | consolidation apply | `observation_id, version, root_version, source_fact_ids[], effective_at, op_key` / `observation_id, op_key` |
| `EntityUpserted`, `EntitiesMerged` | `CommitChunk`, resolver | `entity_id, canonical_name, type, created` / `survivor_id, merged_ids[]` |
| `PageVersionCreated`, `PageDeleted` | refresh, delete | `page_id, version, root_version, markdown_blob_key, effective_at` / `page_id` |
| `SnapshotCreated` | export | `version, manifest_blob_key, base_version` |
| `TokenUsageRecorded` | every metered commit | `day, op, model, prompt_tokens, completion_tokens, cost_micros` |
| `NamespacePurged` | namespace expunge | `rows_purged, blobs_purged` |
| `RestoreMarker` | restore-from-backup (§9.3) | `shard_id, restored_to, epoch_bump` — tells every consumer to reconcile from the restore point |

**Cursors.** `outbox_cursors` has one row per registered consumer (`index`, `kafka`) and one `relay` row. `last_seq` means "every
row with `seq <= last_seq` was delivered, except the seqs in the relay row's `gaps`". The relay delivers batches of 500 and advances
a cursor only after the consumer acknowledged, **batched to one update per second per consumer** (XID budget, N114). The Expunge's row
purge waits until `engram_consumers_passed(event_seq)` is true: every registered cursor passed the `DocumentDeleted{MARKED}` event
whose `seq` the tombstone stores (H-17, P-15).

**Gap watchlist (D6).** `seq` is assigned at `nextval()` time, so a slow transaction can commit seq 101 after seq 102 was read. The relay
re-reads a hole and, if it is open after 1 s, records `{seq, deadline = first_seen + 60 s}` in the relay row's `gaps`; a resolved seq is
delivered, one past its deadline is declared aborted. The list is persisted and bounded at 1,000 entries. The 60 s horizon is sound
because commits cannot hang (`synchronous_commit = local`, N122) and every outbox-writing role is bounded by 30 s statement and idle
timeouts with the outbox `INSERT` last (A-F1, N133e). Rejected: an in-memory list; `pg_current_snapshot()` watermarks.

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
| **Delete intent** (N122) | `_control/deletes/{tenant}/{ns}/{deleted_at_rfc3339}-{operation_id}.json` | the marker's exact effect: subject, kind, `up_to_version` or `memory_ids`, `deleted_at`, `operation_id`, `prev_operation_id` | named by time + operation; strongly consistent put | the API **after** the marker transaction commits and before the ack (`DeleteDocument`, `DeleteNamespace`, `DeleteTenant`, `Invalidate`, `Restore`) | `engramctl` after 35 days (> the 28-day backup window) |
| Ingest raw body | `ledger/{ledger_id}` (N104) | raw item body > 64 KiB (N7) | **owner-keyed**: one `ingest_ledger` row, `ledger_id` minted before the put | engram-api before the ledger tx | the purge, with its ledger row (no reference check) |
| Extraction cache | `xcache/{sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version ‖ render_hash)}.json` (N87) | structured extraction result | content-addressed, immutable, per namespace | `ExtractChunk` | expunge only when no live chunk references the hash **and** the blob is older than `xcache_grace = 24 h` (N100), through `xcache_gc` tombstones with `not_before`; idle TTL 30 d; namespace delete. A missing blob at `CommitChunk` is the retryable `InputBlobMissing` |
| Version body | `ver/{document_id}/v{n}` (N104) | the full reconstructed body of one document version | **owner-keyed**, immutable | `LoadItem` | the purge, with its `document_versions` row; document / namespace delete |
| Document summary | `docsum/{sha256(document_hash ‖ prompt_version ‖ model)}.json` | ≤ 200-char summary + heading tree | content-addressed | `SummarizeDocument` | tombstoned on document delete |
| Consolidation result | `consolidate/{hex(batch_key)}.json` | raw LLM ops for the batch | content-addressed by `batch_key` | `ConsolidateBatch` | 30 d sweep |
| Page markdown | `pages/{page_id}/v{n}.md` | one immutable version | versioned | page refresh | tombstoned on page retire / namespace delete, and by `DerivedPurge` when the version becomes a stub |
| Export snapshot | `export/v{n}/manifest.json`, `facts.jsonl.zst`, `observations.jsonl.zst`, `chunks.jsonl.zst`, `pages/*.md`, `delta-v{n-1}-v{n}.jsonl.zst` | D12 | versioned | `ExportSnapshot` | `export_snapshots.expires_at` → tombstones |
| Reflect transcript (optional) | `reflect/{operation_id}.jsonl` | tool calls and model turns | per operation | `Reflect` when `profile.keep_transcripts` | 7 d sweep; the namespace purge deletes every transcript written before `deleted_at` (N136) |
| Temporal staging (N13) | `staging/{operation_id}/{activity}.bin` | embeddings > 512 KiB in flight | per activity | activities | on operation end |

Blobs are immutable: a changed input gets a new key, never an overwrite, which makes a move's prefix copy
idempotent (copy-if-absent). Only the caches are content-addressed (a loss is a recompute); every other blob
has exactly one owner row and dies with it. A move copies every key referenced by a copied row
(`ingest_ledger.body_blob_key`, `document_versions.body_key`, `page_versions.markdown_blob_key`, export manifests
and parts) and **checks existence of each on the target before cutover** (one key per copied row, H-8); caches (`xcache`, `ecache`, `staging`, `consolidate/`) are not copied. Deletion is
always asynchronous through `blob_tombstones`, so the ack never waits on the blob store. The shard's pgBackRest
repository lives under `_backups/shard-{id}/` outside every tenant prefix (N23).

### 3.7 Sizing: measured per 10 M facts, decided at 8 M per shard (D3, N114)

Assumptions: average fact text 160 bytes, `w5` 200 bytes, `document_id` 24 bytes; 1 M documents with 1 M live
chunks (10 facts per chunk) and 1.5 versions per document; 0.5 M entities, 2 mentions per fact; one
observation per 20 facts, 1.5 versions each; 30 `fact_links` rows per fact (D3). Tuple header 24 B, line
pointer 4 B, index entry header 8 B, B-tree leaf fill 90 %. Measured: a vector heap row and an HNSW element are
**2,048 B** each.

| Table group | Rows | Heap | Indexes | Total |
|---|---|---|---|---|
| `facts` (no vector, no tags, no flags) | 10 M | ≈ 8 GB | PK 0.5, `(chunk, key, hash)` 1.0, doc 0.6, mentioned 0.4, occurred 0.4, GiST 0.5 = 3.4 GB | ≈ 11.4 GB |
| `fact_vectors` (key + copies incl. `fact_type` + inline `halfvec`) | 10 M | ≈ 20.5 GB | PK 0.7, `model_idx` 0.5 | ≈ 21.7 GB |
| `fact_vectors` HNSW, sum of the per-namespace partial indexes | 10 M | | ≈ 20.5 GB | 20.5 GB |
| `facts` BM25 | 10 M | | 4 GB | 4 GB |
| `fact_links` | 300 M | 30 GB | PK 21 GB, reverse 20 GB | **71 GB** |
| `chunks` + vectors + HNSW + BM25; `entities` + aliases; `entity_mentions`; `observations` + versions + vectors + meta | 1 M; 1.5 M; 20 M; 1.25 M | ≈ 5.3 GB; 2.4 GB; 2.1 GB; 1.9 GB | ≈ 3.2 GB; 0.5 GB; 2.6 GB; 2.1 GB | ≈ 8.5; 0.75; 4.7; 4 GB |
| `observation_sources`; `observation_version_sources` (with document columns) | 10 M; ≈ 15 M | 2.5 GB; 3.8 GB | 1.2 GB; 1.9 GB | ≈ 3.7 GB; ≈ 5.7 GB |
| `fact_consolidation`; `observation_inputs` (≤ 58 rows per version) | 10 M; ≈ 45 M | 1.2 GB; 5.4 GB | 1.1 GB; 7.6 GB | ≈ 2.3 GB; ≈ 13 GB |
| `ingest_ledger`; documents, versions, membership | 1.5 M; 4 M | 5.7 GB; 0.6 GB | 0.3 GB; 0.4 GB | 6 GB; 1 GB |
| `outbox` (7 d), `token_usage_events`, operations, markers, `derived_hidden`, pages | | | | ≈ 3.2 GB (markers are bounded by expunge lag) |
| **Total before the `ins_seq` indexes** | | | | **≈ 181 GB per 10 M facts** |

The `(namespace_id, ins_seq)` indexes on the insert-only tables add ≈ 5 %. At the **8 M target** the shard holds
≈ **150 GB**; at the **12 M hard cap** ≈ **230 GB**; the **600 GB** volume leaves room for the WAL archive queue, one
`REINDEX CONCURRENTLY` of the largest index, a pgBackRest spool and a move target's copy. `fact_links` cannot be smaller
at 300 M rows of three UUIDs (D1 fixes UUIDv7); the practical lever is the link cap (section 5). Capacity signals use
relation bytes per namespace (hidden and unpurged rows included), not `live_facts`; `ShardNearCapacity` pages at 70 % of
the volume.

**Instance and hot set.** 8 vCPU, **128 GB RAM**, `shared_buffers = 32 GB`, container limit 112 GB,
`effective_cache_size = 96 GB`, **local NVMe ≥ 50 k IOPS**. Every HNSW visit fetches its heap tuple, so the hot set at the
target is ≈ **80 GB**: HNSW 16 + vector heaps 16 + `facts` heap 6.4 + BM25 3.2 + B-trees and `ins_seq` indexes 5 + the
`fact_links` PK and reverse index (≈ 16 GB) + chunk/observation vectors and HNSW 5 + graph working set; usable cache is
≈ 80 to 96 GB of the 112 GB container, so headroom is thin and R37's trigger applies. Rejected: 10 M facts in a 128 GB
container (hot set ≈ 100 GB against ≈ 80 GB of usable cache at the stated miss rate); `STORAGE EXTERNAL` for vectors; a 6 M cap.

**IOPS budget (MID recall; M0.6 re-derives it with the real arm SQL, the graph arm, the exact path, rows for 10 % and 1 % tag
filters and an old `as_of`).**

| Component | Page touches (measured unless noted) |
|---|---|
| 3 vector arms | ≈ 1,250 each ≈ 3,750 |
| graph arm (temporal family, through the links reverse index) | ≈ 3,700 |
| BM25 arm | ≈ 200 (unmeasured) |
| joins, PK lookups, markers | ≈ 2,350 |
| **per recall** | **≈ 10,000** |
| 50 QPS at a 5 % miss rate | **≈ 25,000 IOPS**, hence NVMe ≥ 50 k |

Write cost: `CommitChunk` ≈ 10 inserts into a ≤ 1 M-element index ≈ 2 to 5 ms of HNSW; retire, invalidate and delete markers
cost **0 HNSW work**; the purge's hygiene is ≈ 0.35 ms per vector. One connection and CPU budget per shard: recall concurrency
≈ 12 of a 24-connection pgbouncer pool shared with ack, expunge and Reflect; per-process pool 16 everywhere; recall-under-ingest
CPU is an M0.5 exit. Purge, `DerivedPurge` and move cleanup are paced by WAL to ≤ 25 MB/s per shard (`wal_compression = zstd`);
a differential backup follows every namespace delete and every move. Fleet: 4 shards per 512 GB host, 8 hosts per 32-shard cell,
125 shards for 1 B facts.

**Vacuum, freeze and bloat plan.**

| Table | Write pattern | Settings (per partition where partitioned) | Why |
|---|---|---|---|
| content partitions (`facts`, `chunks`, `fact_links`, `entity_mentions`, `observation_versions`, the vector tables) | insert-only; `DELETE` only by the expunge | `fillfactor = 100`, `autovacuum_vacuum_insert_scale_factor = 0.05`, `autovacuum_vacuum_scale_factor = 0.02`, `autovacuum_freeze_min_age = 10 M`, `cost_delay = 2`, `cost_limit = 1000`; **vector tables also `vacuum_index_cleanup = off`** | no updates means no dead tuples except purge batches; insert-driven vacuums keep the visibility map fresh; early freezing keeps `age(relfrozenxid)` down; a graph is rebuilt, never repaired by autovacuum (N138) |
| `documents`, `observations`, `pages`, `entities`, `document_versions`, `consolidation_*`, `batch_jobs`, `observation_sources` | frequent small updates | `fillfactor = 70` | HOT stays on the page |
| markers, `derived_hidden`, `expunge_progress`, `vector_indexes`, `namespace_*`, `operations`, `token_usage`, `quota_counters` | tiny, hot | `fillfactor = 50` to 70 | HOT forever |
| `outbox_cursors` | a few rows, advances batched to 1/s per consumer | `fillfactor = 50` | one page per row; XID budget (P-11) |

The shard exports `age(relfrozenxid)` per partition and alerts at 50 % of `autovacuum_freeze_max_age`. A retire writes a
marker row, never a 2.4 KB tuple. The expunge deletes in batches of 1,000 paced by WAL; because the per-namespace HNSW is
small, the index runner rebuilds a touched one at 1 % of `rows_at_build` or 2 k elements (3.3.4) instead of waiting for
autovacuum to repair a shard-wide graph. `maintenance_work_mem = 2.4 KB × vectors` per build (≤ 5 GB, serialised, §9.2).
BM25 segments merge in the background; deleted documents stop influencing scores after `VACUUM`.

### 3.8 Query patterns the schema serves

| # | Query | Served by | Role |
|---|---|---|---|
| 1 | scope + ownership fence (every transaction) | shared advisory try-lock (`engram_try_ns_fence`) + `namespace_ownership` PK (plain read) | app |
| 2 | marker sets, once per recall (two selects: `doc_tomb`, `chunk_tomb`); `fact_hidden` is tested per candidate | `document_tombstones` / `chunk_tombstones`; `fact_hidden` PK anti-join | app |
| 3 | semantic arm over `fact_vectors` (as_of, visibility, allowed documents, `fact_type`) | selectivity-aware (N138): exact path below θ eligible rows; above, the namespace's partial HNSW (`max_scan_tuples`); partition pruned | app |
| 4 | lexical arm over facts (BM25 Top-K, then the visibility join) | `facts_bm25` | app |
| 5 | graph expansion (bounded, from ≤ 20 seeds, both endpoints visible) | `fact_links` PK + `fact_links_reverse_idx`, `facts` PK | app |
| 6 | temporal arm (N68) | `facts_occurred_idx` (two-sided probe) | app |
| 7 | chunk arm (BM25 ∪ HNSW over `chunk_vectors`, `embedding_effective_at <= T`) | `chunks_bm25`, `chunk_vectors` plans as 3 | app |
| 8 | observation arms (semantic + lexical, segment predicate, D9 `as_of`); version evidence; pages | `observation_version_vectors`, `observation_versions_bm25`, `observation_inputs` PK and document index, `derived_hidden` PK prefix | app |
| 9 | `DeleteDocument`, `Invalidate`, `Restore`, `FinalizeVersion(REPLACE)` marker transactions | `document_tombstones`, `fact_hidden`, `chunk_tombstones` PKs | app |
| 10 | retain upsert path (`CommitChunk`) | `chunks (document_id, content_hash)` unique, `facts (chunk_id, key, hash)` unique, `entities_canonical_uq`, PKs | app |
| 11 | Expunge: Materialize, purge batches, hygiene | `observation_inputs_document_idx`, `page_version_inputs_document_idx`, `facts_doc_idx`, `entity_aliases_document_idx` | admin |
| 12 | outbox relay read / cursor advance / retention | `outbox` PK, `outbox_cursors` PK | relay / admin |
| 13 | move: bulk copy range, pre-freeze verification, reconcile by `ins_seq`, `VerifyFK`, cleanup | PKs, `(namespace_id, ins_seq)`, `engram_seq_floor`, `engram_column_hash` | move |
| 14 | entity resolution (fuzzy, per namespace) | `engram_entity_fuzzy` → `entities_trgm_idx` | app |
| 15 | consolidation selection, stale observations/pages | `engram_pending_facts`, `engram_consolidation_watermark`, `observations_stale_idx`, `pages_stale_idx` | app |
| 16 | lists, `GetMemory`, keyset pagination | PKs; the visibility predicate | app |
| 17 | schedulers, stats sweeper, index runner | `operations_sweeper_idx`, `operations_deferred_idx`, `idempotency_keys_expiry_idx`, the two views, `engram_vector_index_plan`, `engram_index_runner_queue`, `engram_index_hygiene_due` | admin / index runner |

In the SQL below `$1` is the namespace, `$2` the query vector, `$3` `as_of` (`'infinity'` when unset),
`$m` the namespace's current embedding model (`namespace_models`). Every statement shown was executed
against the validated shard schema, with the BM25 statements parser-checked, since pg_search is not
installable outside the ParadeDB image. A recall is N short read transactions, one per arm (`ReadSession`, N140).

**The visibility predicate (N116, N135), identical in every arm.** The recall layer loads two marker sets once per
request (alert when one exceeds 16 k entries; above that an arm switches to the `NOT EXISTS` anti-join form) and passes
them as parameters; `fact_hidden` is never an array, because its `invalidate` rows are permanent:

```sql
SELECT engram_doc_tomb($1)            AS doc_tomb,     -- jsonb {document_id: up_to_version} over document_tombstones with expunge_state <> 'purged'
       engram_doc_tomb($1, true)      AS doc_pending,  -- only 'pending': the observation_inputs lookup is skipped for materialised ones
       engram_chunk_tomb($1)          AS chunk_tomb;   -- chunk_tombstones
-- facts and chunks:  visible(f) = NOT engram_doc_hidden($doc_tomb, f.document_id, f.document_version)   -- document_version <= up_to_version
--                                   AND f.chunk_id <> ALL($chunk_tomb)
--                                   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = f.memory_id)   -- any cause
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

`ANY`/`ANY_STRICT`: `(cardinality(tags) = 0 OR tags && $q)` / `tags && $q`; `ALL`/`ALL_STRICT`: `(cardinality(tags) = 0 OR tags @> $q)` /
`tags @> $q AND cardinality(tags) > 0`; `EXACT`: `tags @> $q AND tags <@ $q`; unset: no filter. **Metadata filters resolve the same way**
at document level through `documents_metadata_gin` (N139).

Every arm adds `document_id = ANY($allowed_docs)`; above 8 k allowed documents the arm joins `documents`
with the predicate instead. `engram_tag_match` is the SQL twin of the Lean decision procedure. No tag
predicate runs under RLS (P-5), and observation arms use `observations.tags` (consolidation scope).

**Semantic arm** (`fact_vectors`, mid budget):

```sql
SET LOCAL plan_cache_mode = force_custom_plan;            -- bound namespace and model become constants: the partial-index predicate is provable
SET LOCAL enable_seqscan = off; SET LOCAL max_parallel_workers_per_gather = 0;   -- N138: a partition scan cannot be chosen
SET LOCAL hnsw.max_scan_tuples = 20000;                   -- min(4 x ef_search / s, 100 k); RecallStats.partial when exhausted
SET LOCAL hnsw.ef_search = 150;                           -- the arm cap: 150 MID, 400 HIGH (50 LOW)
SET LOCAL hnsw.iterative_scan = relaxed_order;
SELECT memory_id, document_id, chunk_id, mentioned_at, embedding <=> $2::halfvec(768) AS distance
  FROM fact_vectors
 WHERE namespace_id = $1 AND embedding_model = $m
   AND mentioned_at <= $3                                  -- as_of, omitted when unset
   AND NOT engram_doc_hidden($doc_tomb, document_id, document_version)   -- document_version <= up_to_version (N133c)
   AND chunk_id <> ALL ($chunk_tomb)
   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = fact_vectors.memory_id)
   AND document_id = ANY ($allowed_docs)                   -- tag and metadata filters, when set
   AND fact_type = ANY ($4::fact_type[])                   -- N138: inside the scan
 ORDER BY embedding <=> $2::halfvec(768)
 LIMIT 150;
```

The immutable copies on the vector row let every predicate run inside the scan, so the iterative scan
walks the graph until 150 rows pass, instead of joining `facts` per candidate. **Below θ eligible rows**
(Σ `document_versions.fact_count` over `$allowed_docs`, or the `as_of` histogram) the arm instead runs the exact path:

```sql
SELECT v.memory_id, v.embedding <=> $2::halfvec(768) AS distance
  FROM facts f JOIN fact_vectors v ON v.namespace_id = $1 AND v.memory_id = f.memory_id AND v.embedding_model = $m
 WHERE f.namespace_id = $1 AND f.document_id = ANY ($allowed_docs) AND f.mentioned_at <= $3   -- facts_doc_idx / facts_mentioned_idx
   AND f.fact_type = ANY ($4::fact_type[]) AND <the visibility predicate on f>
 ORDER BY v.embedding <=> $2::halfvec(768) LIMIT 150;
```

The RLS qual appears as a one-time filter and only the namespace's partition is scanned. The same plans apply
to `chunk_vectors`, `observation_version_vectors` and `page_version_vectors`.

**Lexical arm** (pg_search ≥ 0.25 syntax; before 0.25 read `paradedb.score` and `paradedb.match`). BM25
filters on columns of the index (`namespace_id`, `mentioned_at`); the visibility sets are not index columns,
so the Top-K is over-fetched and the visibility is applied to it:

```sql
SELECT c.* FROM (
  SELECT memory_id, document_id, document_version, chunk_id, mentioned_at, fact_type, pdb.score(memory_id) AS score
    FROM facts
   WHERE namespace_id = $1 AND text ||| $2 AND mentioned_at <= $3
   ORDER BY pdb.score(memory_id) DESC, memory_id ASC          -- deterministic tiebreak
   LIMIT $over) c                                              -- $over = 150 when all marker sets are empty, else 300, doubled while short (≤ 3 rounds)
 WHERE NOT engram_doc_hidden($doc_tomb, c.document_id, c.document_version) AND c.chunk_id <> ALL ($chunk_tomb)
   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = c.memory_id)
   AND c.document_id = ANY ($allowed_docs) AND c.fact_type = ANY ($4::fact_type[])
 ORDER BY c.score DESC, c.memory_id ASC
 LIMIT 150;
```

The inner query keeps the `ORDER BY score LIMIT` Top-K pushdown (N19 `EXPLAIN` as `engram_app`; the M0 gate checks the BM25 plan under RLS
on the real image, N131). If markers hide most top hits the arm returns fewer than 150 rows after the third round: accepted degraded mode.
`pdb.score` ranks only (IDF is per partition, so a raw score would let a tenant probe term rarity, F-28); `Scores.lexical` and
`Scores.semantic` are reported as per-query normalised values in [0, 1] (N67).

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
  JOIN observations o          ON o.namespace_id = $1 AND o.observation_id = c.observation_id
  LEFT JOIN observation_version_meta s ON s.namespace_id = $1 AND s.observation_id = c.observation_id AND s.version = c.version
 WHERE (s.superseded_at IS NULL OR s.superseded_at > $3)             -- D9: latest version with effective_at <= T; unset as_of: the current version
   AND (o.retired_at IS NULL OR o.retired_at > $3) AND NOT ov.stub      -- retirement by its own time; stubs never served
   AND (cardinality(o.tags) = 0 OR o.tags && $q)                       -- scope tags, mode-specific
   AND NOT EXISTS (SELECT 1 FROM observation_inputs i                  -- (a) the SEGMENT root_version(v) <= w <= v names a pending victim version or an INVALIDATED fact ('reextract' hides nothing derived)
                    WHERE i.namespace_id = $1 AND i.observation_id = c.observation_id
                      AND i.version BETWEEN ov.root_version AND ov.version
                      AND (engram_doc_hidden($doc_pending, i.document_id, i.document_version)
                           OR EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = i.fact_id AND h.cause = 'invalidate')))
   AND NOT EXISTS (SELECT 1 FROM derived_hidden h                      -- (b) already materialised: a PK-prefix lookup
                    WHERE h.namespace_id = $1 AND h.kind = 'observation' AND h.id = c.observation_id
                      AND h.root_version = ov.root_version AND ov.version >= h.from_version)
   AND EXISTS (SELECT 1 FROM observation_version_sources src           -- (c) proof_count > 0: a visible cited source, from the source's own document columns
                WHERE src.namespace_id = $1 AND src.observation_id = c.observation_id AND src.version = c.version
                  AND NOT engram_doc_hidden($doc_tomb, src.document_id, src.document_version)
                  AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = src.memory_id AND h.cause = 'invalidate'))
 ORDER BY c.distance LIMIT 150;
```

The lexical form is the same shape over `observation_versions_bm25` (inner Top-K, outer predicate). The
superseded versions are in the index too, so the inner Top-K is over-fetched like the facts arm. **Pages**
use the same lookups with depth two (`engram_page_version_hidden`: a fact input of the segment is
tombstoned or invalidated, or an observation-version input of the segment is hidden, or `derived_hidden` covers
it; a missing row or a stub is hidden); `SearchPages` runs BM25 ∪ HNSW over `page_versions.text` and `page_version_vectors`; `GetPage`, `SearchPages`, Reflect's `get_page`/`search_pages`, the MCP tools and the export apply it, and
a hidden current page returns `PAGE_HIDDEN` until the refresh lands. **Chunk arm:** `chunk_vectors` with
`embedding_effective_at <= $3 AND mentioned_at <= $3 AND NOT engram_doc_hidden($doc_tomb, document_id, document_version)` and the chunk
tombstones, best admitted row per `chunk_id`. **Evidence of any version** is that version's own rows, judged from their document columns, so a source whose fact was purged
still counts and its `quote` is still served:

```sql
SELECT s.memory_id, s.quote
  FROM observation_version_sources s
 WHERE s.namespace_id = $1 AND s.observation_id = $o AND s.version = $v
   AND NOT engram_doc_hidden($doc_tomb, s.document_id, s.document_version)
   AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = s.memory_id AND h.cause = 'invalidate');
-- proof_count = the number of rows served; a version with none is not served
```

**Temporal arm** (N68; `$4..$5` query window, `$6` `query_timestamp`, N = the arm cap). A two-sided probe on
`facts_occurred_idx (namespace_id, occurred_start)`: the nearest N occurrences before and after
`query_timestamp`, each an index range scan that stops after N rows, then one 2N-row sort; the visibility
sets filter the branches (the over-fetch rule of the lexical arm applies):

```sql
(SELECT memory_id, occurred_start, occurred_end FROM facts
  WHERE namespace_id = $1 AND occurred_start IS NOT NULL AND occurred_start <= $6 AND occurred_start >= $4
    AND mentioned_at <= $3 AND NOT engram_doc_hidden($doc_tomb, document_id, document_version) AND chunk_id <> ALL ($chunk_tomb) AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = facts.memory_id)
  ORDER BY occurred_start DESC LIMIT 150)
UNION ALL
(SELECT memory_id, occurred_start, occurred_end FROM facts
  WHERE namespace_id = $1 AND occurred_start IS NOT NULL AND occurred_start > $6 AND occurred_start <= $5
    AND mentioned_at <= $3 AND NOT engram_doc_hidden($doc_tomb, document_id, document_version) AND chunk_id <> ALL ($chunk_tomb) AND NOT EXISTS (SELECT 1 FROM fact_hidden h WHERE h.namespace_id = $1 AND h.memory_id = facts.memory_id)
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
           AND NOT engram_doc_hidden($doc_tomb, f.document_id, f.document_version) AND f.chunk_id <> ALL ($chunk_tomb) AND NOT EXISTS (SELECT 1 FROM fact_hidden fh WHERE fh.namespace_id = $1 AND fh.memory_id = f.memory_id)
         GROUP BY e.nb ORDER BY max(e.weight) DESC
         LIMIT GREATEST(0, $budget - cardinality(h.visited))) s) nf
   WHERE h.hop < $hops AND cardinality(h.visited) < $budget AND nf.frontier IS NOT NULL
)
SELECT unnest(deepest.visited) AS memory_id
  FROM (SELECT visited FROM hops ORDER BY hop DESC LIMIT 1) AS deepest;   -- pick the deepest row first, THEN unnest (F-40)
```

The seeds come from arms that already filter by visibility; the per-node cap is a window `row_number() OVER (PARTITION BY
origin ORDER BY weight DESC) <= $per_node` in the inner subquery (elided). At the mid budget that is ≤ 3,000 index probes.
Rejected: a Go-driven BFS with one round trip per hop.

**Entity names under `as_of` (N118).** `entity_mentions.mentioned_at <= $3` supplies the `mention` of each `EntityRef`;
`canonical_name` and alias merges are returned only when `as_of` is unset.

**Marker transactions** (one fenced write transaction as `engram_app` after the scope prelude; `$t/$e/$op` tenant, epoch,
operation; `$ik` the intent object name, derived from `deleted_at` and `$op`). The intent is put **after** the commit and before
the ack (N122). This is the one `DELETE_DOCUMENT` marker transaction (§5.4.1):

```sql
-- DeleteDocument
SELECT pg_advisory_xact_lock(k.k1, k.k2) FROM engram_doc_lock_keys($1, $2) AS k;     -- exclusive document lock: waits for in-flight CommitChunks
SELECT current_version FROM documents WHERE namespace_id = $1 AND document_id = $2;  -- expected_version is compared HERE, under the lock (C-17)
SELECT nextval('outbox_seq') AS $seq;                                                 -- the delete event's seq, stored in the tombstone (P-15)
UPDATE documents SET state = 'deleting', deleted_at = now(), summary_blob_key = NULL, summary_hash = NULL, document_hash = NULL,
       context = '', metadata = '{}', tags = '{}'                                    -- N115/N136: content columns cleared (the CHECK pins it)
 WHERE namespace_id = $1 AND document_id = $2 AND state = 'active';                  -- 0 rows -> NOT_FOUND; the summary blob goes to blob_tombstones
INSERT INTO document_tombstones (namespace_id, tenant_id, document_id, up_to_version, deleted_at, operation_id, intent_key, event_seq)
SELECT $1, $t, $2, max(v.version), now(), $op, $ik, $seq FROM document_versions v WHERE v.namespace_id = $1 AND v.document_id = $2;   -- every version assigned so far, in-flight ones included (N133c)
UPDATE export_snapshots SET state = 'expired', expired_reason = 'document_delete', expires_at = now()
 WHERE namespace_id = $1 AND state IN ('building', 'ready') AND created_at >= $c;                   -- N126: building and ready alike
INSERT INTO deletion_log (namespace_id, tenant_id, intent_key, kind, subject_id, epoch, operation_id, prev_operation_id, effect)
VALUES ($1, $t, $ik, 'document', $2, $e, $op, $prev, '{"up_to_version": …}');                      -- $prev: the subject's last entry, read under the lock (replay chain)
-- in-flight retain operations of the document are cancelled with cancel_reason 'document_deleted'; the delete operation row is kind 'delete_document' (N136)
INSERT INTO outbox (seq, event_type, payload) VALUES ($seq, 'DocumentDeleted', $proto);              -- last statement; one row, O(1)
COMMIT;                                                                                               -- then: put the intent object, then ack
-- Invalidate(f):  INSERT fact_hidden (cause 'invalidate') + curation_log ('invalidate') + deletion_log + outbox FactInvalidated      (a double Invalidate is a no-op success)
-- Restore(f):     exclusive derivation lock; DELETE FROM fact_hidden WHERE memory_id = $f AND cause = 'invalidate';
--                 DELETE FROM derived_hidden WHERE cause_kind = 'invalidation' AND cause_id = $f::text;  + curation_log ('restore') + deletion_log + outbox FactRestored
```

Nothing else is touched, so the commit takes milliseconds at any size and the SLO is the marker transaction (plus the 10 to 50 ms
intent put); a crash between commit and put leaves a committed, unacknowledged marker that a restore may lose and the client's retry
re-applies. After the commit nothing derived from the document is returned by any read path, `GetDocument` returns the tombstone view
(`document_tombstone_view`: `{document_id, state, deleted_at, up_to_version, operation_id}`), and every export snapshot that could
contain it is `expired`.

**Retain upsert path** (`CommitChunk`, one fenced transaction per chunk, idempotent on re-execution):

```sql
SELECT engram_try_doc_lock_shared($1, $2);        -- false -> retryable DocumentBusy (100 ms)
SELECT state, current_version, life_start FROM documents WHERE namespace_id = $1 AND document_id = $2;   -- plain read; state <> 'active' -> aborted
SELECT status FROM document_versions WHERE namespace_id = $1 AND document_id = $2 AND version = $v;   -- anything but 'ingesting' -> stop
SELECT 1 FROM document_tombstones WHERE namespace_id = $1 AND document_id = $2 AND up_to_version >= $v;   -- a tombstone covers v (the document was deleted and re-used meanwhile) -> aborted
INSERT INTO chunks (namespace_id, tenant_id, chunk_id, document_id, document_version, life_start, content_hash, header_hash, heading_path, header, text, mentioned_at)
VALUES (…, $v, $life_start, …) ON CONFLICT (namespace_id, document_id, content_hash, life_start) DO NOTHING RETURNING chunk_id, (created_at = now()) AS inserted;   -- N133c: a covered chunk is never the conflict
DELETE FROM chunk_tombstones WHERE namespace_id = $1 AND chunk_id = $chunk;                  -- un-retire a flap; no row rewritten
INSERT INTO chunk_vectors (…, embedding_effective_at, document_version, …) VALUES (…) ON CONFLICT DO NOTHING;
INSERT INTO facts (…, document_version, mentioned_at, said_at, …) VALUES (…, $v, …)           -- document_version = v; mentioned_at from the chunk (N86), never the extractor
ON CONFLICT (namespace_id, chunk_id, extraction_key, content_hash) DO NOTHING;
INSERT INTO fact_vectors (…, document_version, fact_type, …) SELECT … ON CONFLICT DO NOTHING;
INSERT INTO fact_hidden (…, cause) SELECT f.namespace_id, …, 'invalidate' FROM facts f JOIN curation_log c ON …   -- A-16: the LAST action per (document_id, content_hash) of this life (c.document_version >= $life_start) is re-applied
  WHERE c.action = 'invalidate' AND f.memory_id = ANY ($new_fact_ids) ON CONFLICT DO NOTHING;
INSERT INTO entities (…) SELECT … FROM unnest(…) ORDER BY norm ON CONFLICT (namespace_id, canonical_norm) WHERE merged_into IS NULL DO UPDATE SET mention_count = entities.mention_count + EXCLUDED.mention_count, last_seen_at = now();
INSERT INTO entity_aliases (…, document_id, document_version) … ON CONFLICT DO NOTHING;   INSERT INTO entity_mentions (…, mentioned_at) … ON CONFLICT DO NOTHING;
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
 WHERE c.namespace_id = $1 AND c.document_id = $2 AND c.life_start = $life_start       -- the current life only: covered chunks have a tombstone already (N133c)
   AND c.chunk_id NOT IN (SELECT chunk_id FROM document_version_chunks WHERE namespace_id = $1 AND document_id = $2 AND version = $3)   -- document-scoped (N107)
ON CONFLICT DO NOTHING RETURNING chunk_id;                                                    -- -> ChunksRetired (paged)
INSERT INTO fact_hidden (namespace_id, tenant_id, memory_id, cause, reason)                   -- N58: old-key facts on a kept chunk
SELECT f.namespace_id, f.tenant_id, f.memory_id, 'reextract', '' FROM facts f
 WHERE f.namespace_id = $1 AND f.document_id = $2 AND f.chunk_id IN (SELECT chunk_id FROM document_version_chunks WHERE namespace_id = $1 AND document_id = $2 AND version = $3)
   AND f.extraction_key <> $current_key ON CONFLICT DO NOTHING;
```

REPLACE is not a deletion: observations and pages derived from a tombstoned chunk stay visible and are marked
`stale_write`; the Expunge purges the retired chunks after the 1 h grace and **leaves the evidence rows**, so a later
`DeleteDocument` still finds every derived version (N135). The same transaction flags the dependents of re-extracted facts:

```sql
UPDATE observations SET stale_write = true, stale_since = coalesce(stale_since, now())
 WHERE namespace_id = $1 AND observation_id IN (SELECT i.observation_id FROM observation_inputs i
        JOIN observations o ON o.namespace_id = i.namespace_id AND o.observation_id = i.observation_id AND i.version = o.current_version
       WHERE i.namespace_id = $1 AND i.fact_id = ANY ($old_key_facts));         -- observation_inputs_fact_idx
```

**Consolidation apply order** (stage 2, N120/N121). One transaction: `engram_try_derivation_lock` (refused → retry); re-verify every
rendered input in a fresh statement (`engram_facts_all_visible($input_fact_ids)`; no `FOR SHARE`, facts are immutable); for an `update`
or `merge` op check the base (`engram_derivation_base_ok(ns, 'observation', $o, $base_version)`), and on **any** failure roll back,
`UPDATE consolidation_batches SET state = 'discarded'`, and re-route under a new attempt (no `DELETE`, no stored text is trusted past its
base). Otherwise the `update` op inserts the new `observation_versions` row (with `root_version` inherited from the base), the
`observation_version_vectors` row, `observation_version_meta` for the previous version, the `observation_inputs` rows (the facts shown),
the `observation_version_sources` rows (with `document_id`, `document_version`), the `fact_consolidation('done')` stamps,
`consolidation_applied` `(batch_key, attempt, op_index)`, rewrites `observation_sources`, and sets the batch `applied`; a `merge` is a root
rebuild of the survivor. It ends with `ObservationUpserted`. `CommitPageVersion` runs the same four steps with `kind = 'page'`.

**Expunge Materialize** (under `engram_derivation_lock_exclusive`, one short transaction per batch of
observations):

```sql
INSERT INTO derived_hidden (namespace_id, tenant_id, kind, id, root_version, from_version, cause_kind, cause_id)
SELECT i.namespace_id, i.tenant_id, 'observation', i.observation_id, ov.root_version, min(i.version), 'document', i.document_id
  FROM observation_inputs i
  JOIN observation_versions ov ON ov.namespace_id = i.namespace_id AND ov.observation_id = i.observation_id AND ov.version = i.version
 WHERE i.namespace_id = $1 AND i.document_id = $victim AND i.document_version <= $up_to           -- only the covered versions (N133c)
 GROUP BY i.namespace_id, i.tenant_id, i.observation_id, ov.root_version, i.document_id
ON CONFLICT DO NOTHING;                                              -- invalidation causes: the same with i.fact_id = $f and cause ('invalidation', $f)
-- pages: the same INSERT over page_version_inputs: fact inputs by document_id, observation inputs through the rows just written
UPDATE observations SET stale_delete = true, stale_since = coalesce(stale_since, now()) WHERE (namespace_id, observation_id) IN (…);   -- nudge Consolidate (root rebuilds) and PageRefresh
UPDATE document_tombstones SET expunge_state = 'materialized', materialized_at = now() WHERE namespace_id = $1 AND document_id = $victim AND up_to_version = $up_to;
```

The purge then deletes in primary-key batches, recording `expunge_progress.last_key`:

```sql
DELETE FROM facts WHERE namespace_id = $1 AND memory_id IN
  (SELECT memory_id FROM facts WHERE namespace_id = $1 AND document_id = $victim AND document_version <= $up_to AND memory_id > $last ORDER BY memory_id LIMIT 1000);
-- FK cascades: fact_vectors, fact_links, entity_mentions, observation_sources, fact_consolidation, fact_hidden
-- (observation_inputs, observation_version_sources and page_version_inputs are NOT cascaded: evidence outlives its facts, N135)
-- then chunks WHERE document_version <= $up_to (cascade: chunk_vectors, chunk_tombstones, document_version_chunks),
-- entity_aliases / curation_log WHERE document_id = $victim AND document_version <= $up_to, recompute entities.canonical_name from the
-- remaining mentions, ledger rows of the covered versions of an explicit delete (their owner-keyed blobs go to blob_tombstones),
-- document_versions WHERE version <= $up_to, the documents row only when no open tombstone with a higher up_to_version exists and no
-- document_versions row remains (C-12; a revived document keeps it, N133c); each batch adds its row count to vector_indexes.purged_since_build.
-- DerivedPurge (phase 2b), one admin transaction per batch, per version a document-cause derived_hidden row covers:
--   DELETE FROM observation_versions WHERE (ns, observation_id, version) = …;    -- cascades inputs, sources, vector row; *_version_meta survives (deferred FK)
--   INSERT INTO observation_versions (…, ov_id, version, root_version, effective_at, text, source_count, stub) VALUES (…, '', 0, true);   -- pages likewise + .md blob
-- the reflect/ transcripts written before a namespace's deleted_at are deleted by the namespace purge
```

**Move queries** (N124, N137). *Bulk copy,* per range of ≤ 100 k rows ordered by primary key, `READ COMMITTED`, no snapshot or
barrier: `COPY (SELECT <cols> FROM t WHERE namespace_id = $1 AND <pk> > $k ORDER BY <pk> LIMIT n) TO STDOUT BINARY` → a session `TEMP`
table on the target → `INSERT … SELECT <cols> FROM tmp ON CONFLICT (<pk>) DO NOTHING` for insert-only tables and `DO UPDATE SET
<mutable cols>` for mutable ones, as `engram_move` under the `incoming` fence with `SET LOCAL session_replication_role = replica`;
`<cols>` is `engram_copy_columns(t)` (`ins_seq` included, copied verbatim). Expiring tables are excluded. *Before the freeze* (namespace
still active): `W_pre = nextval('engram_ins_seq')`, `T_pre = now()`, then the whole-namespace work only on rows that cannot still be in
flight, plus one catch-up copy:

```sql
SELECT count(*), bit_xor(hashtextextended(<pk>::text, 0)) FROM t WHERE namespace_id = $1 AND ins_seq < engram_seq_floor(now());   -- both sides must match (hash only up to 2 M rows)
SELECT … FROM t WHERE namespace_id = $1 AND ins_seq >= engram_seq_floor($t_copy);                  -- catch-up copy of every insert-only table
SELECT * FROM engram_verify_fk($1);  -- + the blob existence check, one key per copied row
```

*Under the freeze* (no source write transaction exists): only the delta, and the mutable class by merge-diff:

```sql
SELECT … FROM t WHERE namespace_id = $1 AND ins_seq >= engram_seq_floor($t_pre);   -- insert-only: re-copy and verify only rows inserted since T_pre - 10 min
-- mutable tables: merge (pk, md5(row minus updated_at)) streams; upsert differing or missing rows, DELETE target rows absent on the source
-- excluded class (token_usage_events): count on the target <= source; VerifyFK on the target restricted to the same range
-- relay drain: every source consumer cursor >= the namespace's final max(seq), <= 120 s (twice the gap horizon), else rollback
```

Freeze work = rows inserted since `T_pre − 10 min` + the mutable rows (M1.5 measures it on a namespace with a consolidation backlog,
`TestMove_ActiveBacklog`). The expunge is paused from Plan to done. After the catalog CAS and cutover, cleanup on the source runs the
`drop` statements of `engram_hnsw_ddl` (by their deterministic names; rollback does the same on the target), then `SELECT
engram_cleanup_namespace($1, 10000)` in a loop until it returns 0 (the outer `DELETE` carries `namespace_id`, P-19), as `engram_move`,
once the row is `moved_out`. `CleanupMove` deletes the moved-out rows at 24 h and the source blob prefix only after the 28-day backup window.

**Outbox relay** (`engram_relay`, direct connection): `SELECT … FROM outbox WHERE seq > $hwm ORDER BY seq LIMIT 500`; gap probe `WHERE seq = ANY ($gaps)`;
`UPDATE outbox_cursors SET last_seq = $n, gaps = $j WHERE consumer = $c` (batched to 1/s). Retention (admin): `DELETE FROM outbox` in 10,000-row `seq`
batches below the minimum cursor and older than 7 days. **Entity resolution**: an exact `entity_aliases` PK probe, then `engram_entity_fuzzy($1, $norm)` (3.3.5).

**Schedulers** (admin, shard-wide): the op-sweeper reads `operations_sweeper_idx` (PENDING with no workflow, older than 2 min), the DEFERRED resumer
`operations_deferred_idx`, idempotency expiry runs in batches. Every scheduler joins `schedulable_namespaces` (`state = 'active'`); the expunge and the
index jobs join `purgeable_namespaces` (no open move). The same tick completes or deletes catalog namespaces left `creating` for more than 2 min (through the
admin RPC, D4). The **stats sweeper** refreshes `namespace_stats` (visible-fact count via the marker anti-join, counts, open markers, relation bytes, the
`mentioned_at` histogram), inserts `requested` rows from `engram_vector_index_plan` for the index runner (3.3.4) and calls `engram_seq_sample()` every minute.

### 3.9 Rejected alternatives

| Alternative | Why rejected (one line) |
|---|---|
| Flags on the vectored row (`retired_at`, `live`, partial `WHERE live` HNSW, `fillfactor 60`) | a retire still rewrites a 2.4 KB tuple, BM25 re-indexes it, and every later visibility change lands on the same row again (N112) |
| A shared partition HNSW with per-namespace partials only "in a band" | cost grows with 1/selectivity and vacuum repair is shard-wide; the band was a patch on top (N112) |
| `STORAGE EXTERNAL` for vectors, or a 6 M-fact soft cap, instead of 128 GB RAM | TOAST probes on every exact scan; a lower cap wastes the instance (N114) |
| Vectors on the content row, replaced by `ReembedChunk` | breaks insert-only; no versioned re-embed (N111) |
| Lineage tables and a lineage fixpoint under a lock | correct but keeps the blast radius (two thirds of a namespace) and a 64-deep walk that fails open (N117) |
| Per-version bitmaps, copy-on-write evidence sets, a derived-version flag stamped at delete time | closures under "shown = tainted" are most of the namespace; a stamped set drifts; the read-time predicate has neither (N117) |
| A synchronous cascade at delete (retire facts, delete evidence, flag pages, expire snapshots in one transaction) | cost grows with the document and races with the writers it must stop; the marker is O(1) (N115) |
| `synchronous_commit = on` with a synchronous standby for deletes; an intent put before the marker commits | doubles the footprint and still blocks when the standby is down; an orphan intent would delete content acknowledged later. The intent put after the commit gives RPO 0 for acknowledged deletes (N122) |
| A foreign key (and cascade) from evidence rows to `facts`; hiding derived content with `fact_hidden(reextract)`; deleting hidden derived versions outright | REPLACE then the chunk purge erased the evidence a later `DeleteDocument` needs (C-1); a prompt bump would hide every dependent; outright deletion breaks the D9 range rule and the fail-closed predicate. Evidence dies with its version only and a purged version is a stub (N135, N136) |
| Content-addressed ledger and version bodies with reference counting | a reverse index, check-at-delete and an adoption race; owner-keyed blobs have none and cost only cross-document dedup (N104) |
| `created_at` or UUIDv7 ids as the move's re-copy key | not commit-ordered, absent on several tables; `ins_seq` plus the sampled floor (N137) |
| `CREATE INDEX … IF NOT EXISTS` retries; index DDL by several roles | an invalid index is silently accepted and the namespace stays on the exact path; one runner checks `indisvalid` (N138) |
| Autovacuum repair of vector graphs; `pgstattuple` dead fractions | repair costs 5 to 6 times a rebuild; the purge counts and the runner rebuilds (N138) |
| The catalog as the intent store; a `BYPASSRLS` loader role | same HA question and workers stay off the catalog (D4); nothing bound loaded rows to the namespace (N91) |
| Moving by outbox replay or dual write | a second write path to fence and the mechanism that regressed twice; the difference after a dirty copy is exactly the rows inserted since (N124) |
| `FOR SHARE` on facts or the ownership row as the fence; a blocking shared fence for writers | facts cannot be locked meaningfully and compatible lockers churn multixacts; a queued shared request holds its pooled connection (N82, N120) |
| Exclusive takers that retry every 5 s; one lock-key form for all three locks | a retry storm (one 35 s attempt instead); cross-kind collisions over-serialise (N82, N113) |
| Generated columns (`live`, `fact_type_code`, `tag_count`) | rewrite-only migrations on partitioned tables and move column-list drift; there are none left (P-10) |
| Tags copied onto facts, chunks and observation versions | a tag change would rewrite vectored rows; tags are item-level on `documents`, resolved once per recall (N113, N116) |
| Tag predicates and trigram operators evaluated under RLS | not leakproof, so the planner will not push them into the index scan; resolve outside RLS or use the `SECURITY DEFINER` wrapper (N116, N131) |
| pgvector `vector(768)` instead of `halfvec(768)`; Matryoshka 512-d by default | doubles vector heap and HNSW for no measurable recall gain; a known LongMemEval recall drop (kept as the D15 knob, off) |
| One partition per namespace; no partitioning; range partitioning by `created_at` | relations and DDL per namespace; a single 18 GB index and hours-long vacuums; queries are per namespace, never per time slice |
| A separate `search_entries` table; chunk text in blob storage; `bigserial` ids | a join on every arm and a second copy of the vectors; a blob round trip per candidate; not portable across shards during a move (D1) |
| Application-only isolation, RLS keyed on `tenant_id`, `FORCE` off | one forgotten predicate is a leak; the tenant is not the request unit; the owner and direct partition access would bypass |
| Rich outbox events; an in-memory gap watchlist; `pg_current_snapshot()` watermarks; event payloads in a blob | 3.5 KB per event; lost on relay failover; wraparound-unsafe; a blob write inside a transaction (N80) |
| Per-commit `UPDATE` of `namespace_stats` / `chunks_done`; a model-adjustable `mentioned_at` as the `as_of` key | serialises every commit of a namespace on one row (N69); leak-freedom would depend on the extractor's dating (D9) |
| GiST overlap scan for the temporal arm; recording only cited sources per version; `op_key` over the live LLM answer | a year-wide window sorts the namespace (N68); a version written with a later-deleted fact in view would surface (N41); a retried batch can answer differently (N43) |
| Deleting the `moved_out` ownership row at cleanup; a dedicated `engram` schema; `shard_id` on every row | the row is the fence value a late writer must still hit (N93); no isolation benefit inside a per-shard database |
| Post-Top-K filtering of `fact_type` and metadata; a `(namespace_id, document_id)` index on every vector table | truncation (A-1); the exact path needs none (≈ 3 GB saved per 10 M facts) (N138) |

### 3.10 Notes for the other sections

- Section 5's `CommitChunk` uses `RETURNING chunk_id, (created_at = now()) AS inserted` (3.8); `xmax` is not
  readable in `RETURNING` on a partitioned table. `ReembedChunk` inserts a `chunk_vectors` row and nothing else.
- Every read path (recall arms, GetMemory, ListMemories, Reflect and MCP tools, GetPage/SearchPages, DocumentService's tombstone view)
  applies the 3.8 visibility predicate; the export follows N126 (expiry plus `hidden_overlay`); `PAGE_HIDDEN` is the page error; recall passes
  `doc_tomb`, `doc_pending` (jsonb `{document_id: up_to_version}`) and `chunk_tomb` to every arm; `fact_hidden` is a per-candidate anti-join.
- The fence is the try-lock over `engram_ns_fence_key`; the derivation lock is `engram_ns_derivation_key`; the
  document lock is `engram_doc_lock_keys`; exclusive takers make one 35 s attempt (3.3). Marker transactions
  take no lock beyond the shared fence.
- The ownership SQL of §5.5 is generated from the 3.3.1 table (`ownership_transitions`), including `ready`,
  `unready_target`, `return_abort` and `reconcile_out`; moves do not read the outbox.
- Section 9: the shard image runs `engramctl index` (3.3.4) as the owner; `synchronous_commit = local` is a role
  default; `maintenance_work_mem = 2 GB`; `pick_shard` derives `namespaces_count`; restore and failover follow 3.3.1.
- Intent objects are written by the API **after** the marker transaction commits and before the ack (3.6); `engramctl restore
  replay` reads them from `catalog.shards.replay_floor − 10 min`; the catalog has no delete log. DocumentService returns the
  tombstone view of a `DELETING` document (`document_tombstone_view`) and joins `TestVisibility_AllSurfaces`.
- Section 9: the index runner (`engramctl index`) is the one process holding `engram_migrate` at run time; `maintenance_work_mem
  = 2.4 KB × vectors` per build, `shm_size = 8g`; admin and move DSNs, pools and 30 s timeouts are listed in §9.1.
- Section 8's static RLS check allows `relay_read_all` on `outbox` only; tables without `namespace_id` are
  exactly `shard_meta`, `outbox_cursors`, `ownership_transitions`, `engram_seq_log` and `goose_db_version`.
- Section 9's `shard_meta` carries `shard_id`; provisioning inserts it before the first namespace.

**Round-4 changes** (`reviews/round-4.md` finding → register id; the rows are in D23 and the rewritten rows marked "rev. D23"):

| Finding | Register | Change in §3 and the SQL |
|---|---|---|
| C-1, C-6, C-14, C-15, C-18, P-2, A-19, A-20 | N135, N42, N58 | evidence tables without FK to `facts`; `fact_hidden` keyed `(memory_id, cause)`; derived predicates read `invalidate` only and fail closed; `stale_write` on re-extraction; `REEXTRACTED_FACTS` purge |
| C-2, C-3, C-7, C-11 | N120, N121, N43 | `engram_facts_all_visible`, `engram_derivation_base_ok`; proposals keyed `(batch_key, attempt)` with `base_version`; `consolidation_batches.state`/`attempt`; no `DELETE` by `engram_app` |
| A-2, A-3, A-7, C-12 | N136, N115, N119 | `stub` on versions, deferred meta FK, `DerivedPurge`; content columns cleared by the marker, `document_tombstone_view`; operation kinds from the proto enums |
| C-4, C-5, C-13, C-16, P-13 | N122, N134, N101 | intent after commit; `deletion_log.prev_operation_id`, `effect`; replay floor in `catalog.shards`; `restore_delete` edge |
| C-8, C-23, P-1, C-20 | N137, N124 | class tags, `ins_seq` + `engram_seq_log`/`engram_seq_floor`; `namespace_moves.t_pre`, `w_pre` |
| C-10, C-22 | N125, N123 | `move_state` `committed`, `committed_at`, transition trigger (catalog CAS) |
| C-9, A-1, P-3, P-4, P-7, P-8, P-9, P-10 | N104, N138 | owner-keyed `ledger/{ledger_id}`, `ver/{document_id}/v{n}` with `CHECK`s; `fact_type` on `fact_vectors`, `fact_count`, `mentioned_histogram`; `vector_indexes` machine, runner views, `engram_hnsw_ddl` `index_state`; `vacuum_index_cleanup = off` |
| P-5, P-6, P-11, A-16 | N114, D3, N142 | 8 M / 12 M / 600 GB, 120 namespaces, 181 GB per 10 M, IOPS table, WAL pacing |
| C-17, C-19, P-9, P-14, P-15, A-8, A-13, A-14 | N139, N133 | tombstone `event_seq`; 30 s admin/move timeouts; `engram_entity_fuzzy` revoked; `documents_metadata_gin`; `page_versions.text` + `page_version_vectors`; `superseded_by` is a version |
