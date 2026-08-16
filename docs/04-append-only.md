# Immutable, Append-Only Memory Enrichment in Postgres: Technical Design

## TL;DR
- Replace the mutable `memory_enrichment` side table with an **append-only `memory_enrichment_events` ledger**: every attempt/result is an INSERT, never an UPDATE. The "current" enrichment is derived by a `latest_enrichment` view; "pending" is derived by an anti-join. The linchpin invariant is a **partial unique index** `UNIQUE (memory_id, enrichment_version) WHERE status='done'` guaranteeing at-most-one success per (memory, version).
- Recommended claim mechanism is **deterministic id-range partitioning** planned by a small Temporal activity (no locks), with **row-lock claiming (`FOR UPDATE OF memories SKIP LOCKED`)** as the simpler fallback. Both rely on idempotent `INSERT ... ON CONFLICT DO NOTHING` so duplicate embed work from zombie activities is harmless (deterministic embeddings + cache absorb it).
- Three biggest wins: **no reaper/stuck-status machine can exist at all**, **version bumps come for free** (change the version constant → the anti-join re-derives everyone as pending), and **ingest simplifies** (no pending marker row — the `memories` row itself is the outbox). Main costs: anti-join+backoff pending query complexity and unbounded row growth (mitigated by list-partitioning on `enrichment_version` + `DROP PARTITION`).

## Key Findings
- Postgres **partial unique indexes DO work with `ON CONFLICT` inference**, but you must repeat the index predicate verbatim: `ON CONFLICT (memory_id, enrichment_version) WHERE status='done' DO NOTHING`. Omitting the WHERE means the arbiter is not inferred and you get "there is no unique or exclusion constraint matching the ON CONFLICT specification." Additionally, the inserted tuple must itself satisfy the predicate, or Postgres raises "inferred arbiter partial unique index's predicate does not cover tuple proposed for insertion."
- The anti-join pending query is cheap if backed by the right partial index. `NOT EXISTS` is the idiomatic and planner-friendly form (compiles to a Hash/Anti Join or nested-loop anti join); `LEFT JOIN ... IS NULL` is equivalent only when the join column is provably non-null and can mis-estimate cardinality because the planner's IS-NULL selectivity is derived from table-level statistics that do not describe outer-join output.
- **Append-only means fillfactor 100** (the default) — the fillfactor=80 tuning from the mutable design is now actively wrong (wasted space, worse cache/scan density). Autovacuum still matters: per the PostgreSQL 16 official docs (runtime-config-autovacuum), `autovacuum_vacuum_insert_threshold` "Specifies the number of inserted tuples needed to trigger a VACUUM in any one table. The default is 1000 tuples," and `autovacuum_vacuum_insert_scale_factor` default "is 0.2 (20% of unfrozen pages in table)."
- A 768-dim float32 vector is **exactly 3072 bytes**. Per the PostgreSQL 16 docs §73.2 TOAST, "The TOAST management code is triggered only when a row value to be stored in a table is wider than TOAST_TUPLE_THRESHOLD bytes (normally 2 kB)," so the `embedding bytea` will be pushed to TOAST. Out-of-line values are "divided (after compression if used) into chunks of at most TOAST_MAX_CHUNK_SIZE bytes (by default ... about 2000 bytes)." Random float bytes are near-incompressible, so compression wastes CPU for ~0 gain — set the column to `STORAGE EXTERNAL` (out-of-line, no compression attempt).
- **Advisory locks never use fast-path locking** and always consume main shared-lock-table entries (~168 bytes each, a combined LOCK+PROCLOCK figure attributed to Laurenz Albe). Fast-path is reserved for "weak" locks (AccessShareLock, RowShareLock, RowExclusiveLock) with exactly 16 slots per backend in PG16 (per the PostgreSQL source README and PostgresAI's "#PostgresMarathon 2-004"). Per the docs §19.12 the shared table tracks locks on `max_locks_per_transaction * (max_connections + max_prepared_transactions)` objects; with PG16 defaults that is 64 × (100 + 0) = 6,400 slots system-wide, so large per-transaction batches risk "out of shared memory." This makes deterministic partitioning or row-lock claiming preferable to advisory-lock claiming at batch sizes ≥128.

## Details

### 1. Summary of the change

The current design keeps mutable enrichment state (`status pending/done/dead`, `error_message`, `embedded_at`) and UPDATEs it in place. Under strict append-only semantics that is forbidden. The redesign:

- Introduces `memory_enrichment_events`, an insert-only ledger. Each row records the *immutable fact* of one enrichment attempt: what happened (`status`), for which `enrichment_version`, on which `attempt`, and (for successes) the produced `normalized_text`, `lexemes`, `ts`, `embedding`.
- Derives all "current state" via **views and indexes** instead of columns:
    - *Current enrichment* = latest successful row at the max version (via a partial-unique-index-backed `DISTINCT ON` / index scan).
    - *Pending* = `memories` with no `status='done'` event at the target version (anti-join), gated by a backoff+max-attempts predicate.
    - *Dead-letter* = `memories` with ≥ max_attempts failed events (or a permanent failure) at the version (a view, not a status flip).
- **Linchpin invariant**: `CREATE UNIQUE INDEX ... ON memory_enrichment_events (memory_id, enrichment_version) WHERE status='done'`. This makes "latest success" deterministic (at most one success per (memory, version)) and makes completion inserts idempotent under `ON CONFLICT DO NOTHING`. Everything else (harmless duplicate work, crash safety, version bumps) follows from this one invariant.

**Latest-wins semantics, defined precisely.** The current enrichment for a memory is the row with the maximum `enrichment_version` among its `status='done'` rows; within that version the partial unique index guarantees there is exactly one. `id DESC` in the `DISTINCT ON` ordering is a belt-and-suspenders tiebreaker that never actually fires given the invariant. The client always prefers the highest version that has a done row, even if lower versions also have done rows.

**Three biggest wins**
1. *No status machine, no reaper.* There is no `processing` state that can get stuck; a crashed worker either produced a row or it didn't, and the next sweep re-derives pending. There is nothing to reap and no "claimed-but-abandoned" reclamation logic even conceptually.
2. *Version bumps for free.* Bump `CURRENT_ENRICHMENT_VERSION` in Go; the anti-join instantly re-derives every memory as pending at the new version. No bulk `UPDATE ... SET status='pending'` re-enqueue.
3. *Ingest simplification.* No enrichment marker row at ingest. Pending is derived from *absence*, so the `memories` row itself is the durable outbox entry — "never miss a memory" is guaranteed structurally.

**Costs**: the pending query is more complex (anti-join + backoff aggregate); the ledger grows without in-place reuse (bounded by operational partition drops).

### 2. Full DDL

```sql
-- Immutable enrichment ledger. INSERT-only. No UPDATE/DELETE on data rows.
CREATE TABLE memory_enrichment_events (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    memory_id         BIGINT      NOT NULL REFERENCES memories(id),
    enrichment_version SMALLINT   NOT NULL,
    attempt           INT         NOT NULL,          -- 1-based per (memory,version)
    status            TEXT        NOT NULL
                        CHECK (status IN ('done','failed')),
    permanent         BOOLEAN     NOT NULL DEFAULT false,  -- 'dead' derived: failed AND permanent
    error_message     TEXT,                          -- NULL for done
    normalized_text   TEXT,                          -- NULL unless done
    lexemes           TEXT[],                        -- NULL unless done
    ts                TIMESTAMPTZ,                   -- content-derived timestamp, NULL unless done
    embedding         BYTEA,                         -- 3072-byte L2-normalized f32, NULL unless done
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 100);   -- insert-only: keep pages fully packed (this is the default)

-- Store the incompressible 3072-byte vector out-of-line, skip the futile compression pass.
ALTER TABLE memory_enrichment_events
    ALTER COLUMN embedding SET STORAGE EXTERNAL;

-- LINCHPIN INVARIANT: at most one success per (memory, version).
CREATE UNIQUE INDEX uq_enrich_success
    ON memory_enrichment_events (memory_id, enrichment_version)
    WHERE status = 'done';

-- Backoff support: locate failed attempts fast by (memory, version, recency).
CREATE INDEX ix_enrich_attempts
    ON memory_enrichment_events (memory_id, enrichment_version, created_at)
    WHERE status = 'failed';

-- Note: uq_enrich_success already serves the "is there a done row at V?" existence probe
-- as an index-only scan, so no separate anti-join support index is needed for successes.

-- Insert-only autovacuum tuning (freezing + visibility map health for index-only scans).
-- PG16 defaults: insert_threshold=1000, insert_scale_factor=0.2. Tighten so the VM stays current.
ALTER TABLE memory_enrichment_events SET (
    autovacuum_vacuum_insert_threshold = 2000,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
```

**Views**

```sql
-- Current enrichment per memory: latest success at the highest version that has a success.
-- DISTINCT ON is the fastest single-row-per-group form and uses uq_enrich_success ordering.
CREATE VIEW latest_enrichment AS
SELECT DISTINCT ON (memory_id)
       memory_id, enrichment_version, normalized_text, lexemes, ts, embedding
FROM   memory_enrichment_events
WHERE  status = 'done'
ORDER  BY memory_id, enrichment_version DESC, id DESC;

-- Version-pinned enrichment for reproducible eval (client passes V):
CREATE VIEW enrichment_at_version AS
SELECT memory_id, enrichment_version, normalized_text, lexemes, ts, embedding
FROM   memory_enrichment_events
WHERE  status = 'done';   -- caller adds: AND enrichment_version = $V

-- Version-scoped progress: total / done / remaining / dead AT a version.
-- Parameterized as a function to keep it version-pinned for benchmark comparability.
CREATE FUNCTION enrichment_progress(p_conversation BIGINT, p_version SMALLINT)
RETURNS TABLE(total BIGINT, done BIGINT, remaining BIGINT, dead BIGINT)
LANGUAGE sql STABLE AS $$
    SELECT
      count(*) AS total,
      count(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'done')) AS done,
      count(*) FILTER (WHERE NOT EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'done')) AS remaining,
      count(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'failed' AND e.permanent)) AS dead
    FROM memories m
    WHERE m.conversation_id = p_conversation;
$$;

-- Dead-letter view: memories whose failures are permanent or exhausted at version V.
CREATE FUNCTION dead_letter(p_version SMALLINT, p_max_attempts INT)
RETURNS TABLE(memory_id BIGINT) LANGUAGE sql STABLE AS $$
    SELECT m.id
    FROM memories m
    WHERE NOT EXISTS (
        SELECT 1 FROM memory_enrichment_events e
        WHERE e.memory_id = m.id AND e.enrichment_version = p_version
          AND e.status = 'done')
      AND (
        EXISTS (SELECT 1 FROM memory_enrichment_events e
                WHERE e.memory_id = m.id AND e.enrichment_version = p_version
                  AND e.status='failed' AND e.permanent)
        OR
        (SELECT count(*) FROM memory_enrichment_events e
         WHERE e.memory_id = m.id AND e.enrichment_version = p_version
           AND e.status='failed') >= p_max_attempts
      );
$$;
```

### 3. Exact SQL

**Pending query with backoff + max-attempts** (the core CountBacklog / claim predicate):

```sql
-- Pending at version V: no success yet, not dead, and past the backoff window.
-- Backoff = exponential in the number of prior failed attempts.
WITH fail AS (
    SELECT memory_id,
           count(*)          AS n_fail,
           max(created_at)    AS last_fail,
           bool_or(permanent) AS has_permanent
    FROM   memory_enrichment_events
    WHERE  enrichment_version = $1        -- V
      AND  status = 'failed'
    GROUP  BY memory_id
)
SELECT m.id
FROM   memories m
LEFT   JOIN fail f ON f.memory_id = m.id
WHERE  NOT EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id
            AND e.enrichment_version = $1
            AND e.status = 'done')        -- ANTI-JOIN: not done
  AND  COALESCE(f.has_permanent, false) = false
  AND  COALESCE(f.n_fail, 0) < $2         -- max_attempts
  AND  (f.last_fail IS NULL
        OR f.last_fail < now() - ($3::interval * power(2, f.n_fail)))  -- backoff
ORDER  BY m.id
LIMIT  $4;                                 -- batch size N
```

The `NOT EXISTS` probe rides `uq_enrich_success`; the `fail` CTE aggregates over `ix_enrich_attempts`. Both are partial indexes, so they only contain the relevant subset (successes / failures), keeping them small. Using the *same* predicate in CountBacklog and in the claim avoids livelock: a transiently-failed item only re-appears as pending once its exponential backoff window has elapsed.

**Claim option (1): row-lock the immutable parent** (simplest; claim-then-release):

```sql
BEGIN;
SELECT m.id
FROM   memories m
WHERE  NOT EXISTS (SELECT 1 FROM memory_enrichment_events e
                   WHERE e.memory_id=m.id AND e.enrichment_version=$1 AND e.status='done')
  AND  /* backoff predicate as above */
ORDER  BY m.id
FOR    UPDATE OF m SKIP LOCKED
LIMIT  $2;
-- release immediately (COMMIT), then embed outside the tx; results are idempotent inserts.
COMMIT;
```

Locking the immutable `memories` rows is purely a mutex — no columns are written. Because tx-scoped locks release at COMMIT, you either (a) hold the tx open across the gRPC embed calls (bad: long transactions) or (b) claim-then-release and accept rare duplicate work on overlap. Choose (b): the partial unique index + `ON CONFLICT DO NOTHING` dedupe the result, and duplicate embed RPCs are absorbed by `embedding_cache`.

**Claim option (2): advisory lock** (analyzed, not recommended at batch scale — see Pitfalls):

```sql
-- per candidate id:
SELECT pg_try_advisory_xact_lock($memory_id);
```

**Claim option (3): deterministic partition planning** (recommended):

```sql
-- Planning activity: compute k disjoint id ranges over the pending set using NTILE.
WITH pending AS (
    SELECT m.id
    FROM   memories m
    WHERE  NOT EXISTS (SELECT 1 FROM memory_enrichment_events e
                       WHERE e.memory_id=m.id AND e.enrichment_version=$1 AND e.status='done')
      AND  /* backoff predicate */
), bucketed AS (
    SELECT id, ntile($2) OVER (ORDER BY id) AS bucket   -- $2 = fanOut (e.g. 16)
    FROM pending
)
SELECT bucket, min(id) AS lo, max(id) AS hi, count(*) AS n
FROM   bucketed GROUP BY bucket ORDER BY bucket;
-- Each ProcessBatch activity then scans WHERE id BETWEEN lo AND hi with the same predicate.
```

Because sibling `ProcessBatch` activities within a sweep are handed disjoint id ranges, they never overlap **by construction — no locks at all**. Overlap policy Skip prevents overlap across sweeps. The only residual overlap is a zombie activity from a crashed prior sweep; idempotent inserts make that harmless. Keyset ranges (`min`/`max` id per bucket) are preferable to `NTILE`-emitted id lists because the payload is tiny (16 ranges × 2 bigints) and the worker re-runs the cheap predicate on its slice rather than trusting a possibly-stale id list.

**Completion insert (idempotent, never UPDATE):**

```sql
INSERT INTO memory_enrichment_events
    (memory_id, enrichment_version, attempt, status,
     normalized_text, lexemes, ts, embedding)
VALUES ($1,$2,$3,'done',$4,$5,$6,$7)
ON CONFLICT (memory_id, enrichment_version) WHERE status='done'
DO NOTHING;                          -- predicate MUST be repeated for arbiter inference
```

**Failure insert:**

```sql
INSERT INTO memory_enrichment_events
    (memory_id, enrichment_version, attempt, status, permanent, error_message)
VALUES ($1,$2,$3,'failed',$4,$5);    -- no ON CONFLICT: failures are free-form facts
```

### 4. Go / Temporal diffs

**ProcessBatch activity** (claim → embed → append):

```go
func (a *Activities) ProcessBatch(ctx context.Context, in BatchInput) (BatchResult, error) {
    // in.Lo/in.Hi = deterministic id range (option 3), or claim via SKIP LOCKED (option 1)
    rows, err := a.db.Query(ctx, pendingInRangeSQL, in.Version, in.Lo, in.Hi, in.MaxAttempts, a.backoff)
    // ...
    g, gctx := errgroup.WithContext(ctx)
    g.SetLimit(32) // unchanged embedder concurrency
    for _, m := range mem {
        m := m
        g.Go(func() error {
            activity.RecordHeartbeat(gctx, m.ID)                 // unchanged heartbeat
            blob, _ := a.s3.Get(gctx, m.Bucket, m.Key)
            norm := normalize(blob)
            vec, cached, err := a.embedWithCache(gctx, norm)     // embedding_cache unchanged
            if err != nil {
                if permanent(err) {
                    a.insertFailed(gctx, m.ID, in.Version, m.Attempt+1, true, err.Error())
                    return nil                                    // permanent: no retry
                }
                a.insertFailed(gctx, m.ID, in.Version, m.Attempt+1, false, err.Error())
                return nil                                        // transient: reappears after backoff
            }
            // NEVER UPDATE. Idempotent append; ON CONFLICT DO NOTHING dedupes zombie/dup work.
            return a.insertDone(gctx, m.ID, in.Version, m.Attempt+1, norm, lex(norm), tsOf(m), vec)
        })
    }
    return res, g.Wait()
}
```

**Status taxonomy.** Keep `status IN ('done','failed')` plus a `permanent` boolean; "dead" is *derived* (`failed AND permanent`, or `failed count ≥ max_attempts`) rather than a third stored state. This keeps the ledger a record of what physically happened (an embed either succeeded or failed) and pushes policy (is this memory dead?) into views, where it can change without rewriting history.

**CountBacklog activity** now runs the anti-join + backoff pending count — using the *same predicate* as the claim to avoid livelock (a failed item only re-counts as pending after its backoff window elapses):

```go
func (a *Activities) CountBacklog(ctx context.Context, version int16) (int, error) {
    var n int
    err := a.db.QueryRow(ctx, pendingCountSQL, version, a.maxAttempts, a.backoff).Scan(&n)
    return n, err
}
```

**Planning activity** (option 3) returns ranges only (payload trivial — e.g. 16 ranges × 2 int64):

```go
type IDRange struct{ Bucket int; Lo, Hi, N int64 }
func (a *Activities) PlanRanges(ctx context.Context, version int16, fanOut int) ([]IDRange, error)
```

**Re-enrichment version-bump flow.** Bump `CURRENT_ENRICHMENT_VERSION` (a code constant). On the next sweep, `CountBacklog(V+1)` reports the full corpus as pending because no memory has a `done` row at V+1 yet — no re-enqueue write is needed. Old-version rows remain as history. During rollout the corpus is *mixed* (some memories done at V+1, some only at V); the progress function and fetch are **version-scoped** so a benchmark run pins to one version and waits for `enrichment_progress(conv, V) → remaining=0` before reading, guaranteeing a homogeneous corpus.

**Version-pinned fetch / progress RPC signatures** (client eval targets a fixed version to avoid mixed-version corpora):

```protobuf
rpc FetchAllMemories(FetchReq) returns (stream EnrichedMemory);
message FetchReq { int64 conversation_id = 1; int32 enrichment_version = 2; } // version REQUIRED

rpc GetProgress(ProgressReq) returns (Progress);
message ProgressReq { int64 conversation_id = 1; int32 enrichment_version = 2; }
message Progress { int64 total=1; int64 done=2; int64 remaining=3; int64 dead=4; }
```

`FetchAllMemories` streams:

```sql
SELECT e.memory_id, e.normalized_text, e.lexemes, e.ts, e.embedding,
       m.content_hash, m.s3_key
FROM   enrichment_at_version e
JOIN   memories m ON m.id = e.memory_id
WHERE  m.conversation_id = $1 AND e.enrichment_version = $2
ORDER  BY e.memory_id;
```

**Ingest change.** Previously the server inserted a `memories` row plus an enrichment `pending` marker in one tx. Now it inserts only the `memories` row (after the S3-first blob write). Pending is derived by absence, so the `memories` row *is* the outbox entry — no separate marker is needed to guarantee "never miss a memory."

**Schedules / overlap / heartbeats: unchanged.** The 1-minute Schedule with overlap policy Skip, the fan-out topology, and heartbeat semantics are all identical. Skip (the Temporal default) means a new scheduled run is not started while the prior run is still in flight, which continues to prevent cross-sweep overlap; idempotent inserts handle the zombie-activity edge case.

### 5. Comparison table

| Dimension | Mutable state machine (old) | Append-only ledger (new) |
|---|---|---|
| Crash recovery | `processing` rows can get stuck → reaper needed | No `processing` state possible; next sweep re-derives pending. No reaper. |
| Retries / backoff | UPDATE `status`, `error_message`, retry counters in place | Declarative: count/max(created_at) over `failed` rows + backoff predicate |
| Re-enrichment (version bump) | Bulk UPDATE to reset `status='pending'` | Change version constant; anti-join re-derives pending. Zero writes. |
| Bloat / vacuum profile | UPDATE churn → dead tuples, needs fillfactor 80 + aggressive vacuum | Insert-only, fillfactor 100; vacuum only for freeze/VM (insert-threshold) |
| Query complexity | Trivial `WHERE status='pending'` | Anti-join + backoff aggregate (more complex, but index-cheap) |
| Auditability | History destroyed on UPDATE | Full attempt history retained (every failure + success is a durable fact) |
| Duplicate-work safety | UPSERT could clobber | `ON CONFLICT DO NOTHING` on partial unique = at-most-one success |

### 6. Pitfalls

- **`ON CONFLICT` partial-index inference**: you must repeat the exact predicate (`WHERE status='done'`). If omitted, Postgres will not infer the partial index as arbiter. Per the PostgreSQL docs the arbiter is chosen by unique-index inference, and the medium/CyberTec write-ups confirm the fix is "predicates of the index (WHERE …) must be added after the ON CONFLICT clause." Also, the inserted tuple must satisfy the predicate — inserting a `failed` row with `ON CONFLICT (...) WHERE status='done'` raises "inferred arbiter partial unique index's predicate does not cover tuple proposed for insertion." So only *completion* inserts carry the ON CONFLICT clause; failure inserts do not.
- **Anti-join performance cliff**: without a partial index on successes, the `NOT EXISTS` degrades to scanning all events per memory. `uq_enrich_success` makes the existence probe an index-only scan (the visibility map must be current — see autovacuum). Prefer `NOT EXISTS` over `LEFT JOIN ... IS NULL`; the latter can mis-estimate cardinality because, as a Postgres committer thread documents, "the selectivity for 'IS NULL' is estimated using the table-level statistics [which] the LEFT JOIN entirely breaks."
- **Advisory-lock table sizing**: advisory locks bypass fast-path and always consume main shared-lock-table entries (~168 bytes each). With PG16 defaults (`max_locks_per_transaction=64`, `max_connections=100`) the whole table is ~6,400 slots system-wide; a single txn taking 1024 advisory locks uses ~168 KB / ~16% of it, and as few as ~6 concurrent claimers (6400/1024) could exhaust it and trigger "out of shared memory / You might need to increase max_locks_per_transaction." At batch 128 the per-txn cost is ~21 KB but ~50 concurrent claimers still exhaust the default table. This is why deterministic partitioning / row-locks beat advisory locks at batch ≥128 unless you raise `max_locks_per_transaction` to cover `peak_concurrent_claimers × batch_size`. (Raising it is cheap — Tom Lane noted a 1M-slot table is ~160 MB — but it requires a restart and is avoidable here.)
- **Autovacuum on insert-only**: rely on PG13+ insert-triggered vacuum (`autovacuum_vacuum_insert_threshold=1000`, `insert_scale_factor=0.2` in PG16); tighten per-table so the visibility map stays current for index-only scans. Do NOT keep fillfactor 80 — it only benefits HOT updates, which never occur on an insert-only table, and just wastes 20% of every page.
- **TOAST / compression of embeddings**: 3072-byte f32 vectors exceed the ~2KB threshold and are near-incompressible (LZ4's early-abort will bail and pglz needs ≥25% gain to accept); set `STORAGE EXTERNAL` to store them out-of-line and skip the wasted compression pass entirely. LZ4 vs pglz choice is moot for random floats.
- **Mixed-version corpora during rollout**: mid-rollout, some memories are done at V+1 and some only at V. Always pin the eval to a version (RPC arg) and wait for `progress(V)=remaining 0` so the benchmark corpus is frozen and comparable. MVCC additionally gives each query a consistent snapshot, so a single version-pinned `FetchAllMemories` stream sees a coherent point-in-time corpus.
- **View vs materialized view**: plain views are correct at 250k–1M rows because MVCC gives each query a consistent snapshot and the indexes make the anti-join/DISTINCT ON cheap. Only reach for a materialized view (or `pg_cron` refresh) if progress polling becomes a hot loop; `REFRESH MATERIALIZED VIEW` takes an ACCESS EXCLUSIVE lock (or CONCURRENTLY with a unique index) and recomputes the entire result set regardless of how little changed, so it is usually not worth it here.

### 7. Table growth management

At 768-dim f32, each success row carries a 3072-byte embedding plus normalized text and lexemes ≈ ~3.2KB. A full re-enrichment generation over 250k memories is ~800MB–1GB — trivial for a benchmark harness, so **keeping full history is the default**. If many re-enrichment generations accumulate, list-partition the ledger by `enrichment_version` and `DROP PARTITION` (or `DETACH PARTITION CONCURRENTLY`) for superseded versions. Per the PostgreSQL partitioning docs, "Dropping an individual partition using DROP TABLE, or doing ALTER TABLE DETACH PARTITION, is far faster than a bulk operation [and] entirely avoid the VACUUM overhead caused by a bulk DELETE." Present this as an explicit **operator decision**: dropping/detaching a whole partition is a table-lifecycle metadata operation, not a row-level UPDATE/DELETE, so it is compatible with "immutable rows" (no row is ever mutated; whole generations are retired atomically). If strict "never destroy any historical fact" is required, keep all generations and accept linear growth.

### 8. What stays the same
- Retrieval stack (BM25 + cosine → RRF k=60 → temporal boost → MMR), client-side ranking.
- nomic-embed-text-v1.5, 768 dims, mandatory `search_document: `/`search_query: ` prefixes, L2-normalized f32 as bytea. Per the nomic-ai/nomic-embed-text-v1.5 model card, "the text prompt must include a task instruction prefix... you embed your documents as `search_document: <text here>` and embed your user queries as `search_query: <text here>`."
- `embedding_cache(content_hash, model, task_prefix → vector)` — already append-only via `INSERT ... ON CONFLICT DO NOTHING`; unchanged and confirmed compatible.
- S3-first ingest ordering (blob → then Postgres row).
- Temporal Schedule (1-min, overlap Skip), fan-out topology, heartbeats — unchanged.
- docker-compose (postgres:16, minio+mc, temporal auto-setup+ui, embedded worker), one-click run.sh.

## Recommendations
1. **Adopt the ledger + partial-unique-index invariant first.** Ship `memory_enrichment_events`, `uq_enrich_success`, the completion insert with `ON CONFLICT (...) WHERE status='done' DO NOTHING`, and the derived views. This alone delivers immutability, crash-safety, and free version bumps.
2. **Start with row-lock claiming (option 1)** for its simplicity; measure. Move to **deterministic id-range partitioning (option 3)** if you observe claim contention or want to eliminate even the short claim transaction. **Do not use advisory-lock claiming (option 2)** at batch sizes ≥128 unless you raise `max_locks_per_transaction` to cover `peak_concurrent_claimers × batch_size`.
3. **Set fillfactor 100 and `STORAGE EXTERNAL`** on the embedding column from day one; tune insert autovacuum as in the DDL.
4. **Version-pin every client fetch and progress call.** Gate eval runs on `enrichment_progress(conv, V)` reporting `remaining = 0`.
5. **Defer partitioning.** Keep full history until storage crosses a threshold you care about (e.g. >10 generations / tens of GB); then list-partition by `enrichment_version` and `DROP PARTITION` for retired versions.

**Benchmarks that would change these:** if the anti-join pending count exceeds ~50 ms at your scale, add a covering index or materialize progress; if claim contention appears as lock waits in `pg_stat_activity`, switch to option 3; if the ledger's TOAST table dominates storage growth, enable version partitioning earlier; if you ever need advisory locks and see "out of shared memory," that is the signal to raise `max_locks_per_transaction` or shrink batch size.

## Caveats
- Row-lock claiming with immediate release (claim-then-embed-outside-tx) permits a small window of duplicate embed RPCs for the same memory if two sweeps/zombies overlap; this is harmless (deterministic embeddings + cache + `ON CONFLICT DO NOTHING`) but does cost a few redundant RPCs. Holding the tx open across the embed calls would eliminate it at the cost of long transactions during gRPC calls — not recommended.
- The 168-byte-per-lock figure is an order-of-magnitude estimate (architecture-dependent, combined LOCK+PROCLOCK); the nominal lock-table formula is a sizing target with ~10% slop plus a ~100 KB safety margin (per Tom Lane), not a hard ceiling. The PG18 fast-path expansion (arrays sized by `max_locks_per_transaction`) does *not* help advisory locks — do not let it create false reassurance.
- `DROP PARTITION` for superseded versions is an operator policy choice; if strict "never destroy any historical fact" is required, keep all generations and accept linear storage growth.
- Index-only scans on the partial success index require the visibility map to be current, which is why insert-triggered autovacuum tuning matters.
- Postgres 19 is slated to change the default TOAST compression from pglz to LZ4, but this is irrelevant to the embedding column once it is set `STORAGE EXTERNAL` (no compression is attempted regardless of the default).

---

**References:**

1. [INSERT ... ON CONFLICT error messages (postgresql.org)](https://www.postgresql.org/message-id/5548E727.6040201%40iki.fi)
2. [PostgreSQL Documentation: autovacuum_vacuum_insert_threshold parameter (postgresqlco.nf)](https://postgresqlco.nf/doc/en/param/autovacuum_vacuum_insert_threshold/)
3. [PostgreSQL: Documentation: 16: 73.2. TOAST (postgresql.org)](https://www.postgresql.org/docs/16/storage-toast.html)
4. [PostgreSQL: Documentation: 18: 66.2. TOAST (postgresql.org)](https://www.postgresql.org/docs/current/storage-toast.html)
5. [Schedule | Temporal Platform Documentation (docs.temporal.io)](https://docs.temporal.io/schedule)
6. [why postgresqls on conflict cannot find my partial unique index 552327b85e1 (betakuang.medium.com)](https://betakuang.medium.com/why-postgresqls-on-conflict-cannot-find-my-partial-unique-index-552327b85e1)
7. [Monitor PostgreSQL HOT Updates and Fillfactor | Postgres Scripts (postgresscripts.com)](https://www.postgresscripts.com/post/monitor-postgresql-hot-updates-and-fillfactor/)
8. [Refreshing PostgreSQL Materialized Views Without Downtime - DEV Community (dev.to)](https://dev.to/data_with_jelimo/refreshing-postgresql-materialized-views-without-downtime-28n6)
9. [nomic-ai/nomic-embed-text-v1.5 · Hugging Face (huggingface.co)](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5)
