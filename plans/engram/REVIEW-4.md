# Engram: adversarial review 4 (Postgres, Temporal and operational reality)

Scope: whether the DDL, indexes, triggers, RLS policies, roles, GUCs and functions in
`sql/shard_schema.sql` and `sql/catalog_schema.sql` behave and perform as §3, §5 and §9 claim
at 10 M facts per shard and 32 shards per cell. This review also covers Temporal, migrations,
backups, connection budgets, metrics and the Compose topology. Findings accepted in `REVIEW.md` or
`REVIEW-2.md` are not repeated unless the applied fix is wrong. Where a finding overlaps a
concurrent review (`REVIEW-3.md` H-*, `REVIEW-5.md` A-*), that is stated, and this review adds
only its measurements.

## How the evidence was produced

- **Server.** A scratch PostgreSQL 16.15 with pgvector 0.8.6, pg_trgm, btree_gin and btree_gist on a 4-vCPU, 15 GB VM.
  - Settings: `shared_buffers = 2GB`, `maintenance_work_mem = 2GB`, `max_parallel_maintenance_workers = 3`.
  - Every timing below is **warm**: the data fits in `shared_buffers`. Cold-cache figures were not measured, so the I/O terms are computed.
- **Schema.** Databases `rev4_shard` and `rev4_cat` received the two DDL files unmodified. The only exception is the `-- pg_search:begin/end` blocks, which were stripped because pg_search is not installable here.
  - Both files apply cleanly.
  - All self-checks pass.
- **Synthetic shard.**
  - Eight namespaces, all hashing into `facts_p07` (chosen with `satisfies_hash_partition`), of 100 k, 40 k, 20 k, 15 k, 10 k, 6 k, 5 k and 4 k facts. That is 200 k facts and 20 k chunks.
  - The rows are inserted interleaved across namespaces, as concurrent ingest would leave them.
  - Embeddings are `halfvec(768)`, clustered: 40 topic centroids per namespace plus noise, L2-normalised.
  - HNSW uses `m = 16, ef_construction = 128` as in the DDL.
  - Measured sizes: the `facts_p07` heap is 521 MB (≈ 2.6 KB per row, matching §3.7's 2.4 KB) and its HNSW is 391 MB (≈ 1.95 KB per vector).
- **Caveats.**
  - Clustered synthetic vectors make the *namespace* filter almost free for an in-namespace query vector, because each namespace occupies its own region. Real tenants share topic space. Filter costs are therefore measured with the tag and `as_of` filters, which do reject neighbours.
  - The production partition is ≈ 3× larger (625 k rows), so HNSW insert, search and vacuum costs below are lower bounds at target scale.

Raw scripts are reproducible from the statements quoted in each finding.

---

## Findings (ranked)

### P-1: blocker [regression: N94 + N41/N84 `live`]: every mutation of a vectored row is a full HNSW insertion. The synchronous delete cascade is ≈ 50× over its SLO, and documents above ≈ 12 k facts cannot be deleted inside the 30 s `statement_timeout`

**Where.**
- §3.8 "Delete cascade … SLO … p95 ≤ 50 ms per 1 k retired facts … a 100 k-fact document ≈ 5 s … inside the 30 s writer `statement_timeout`".
- §9.4, SLO table (same figure).
- §3.7 bloat plan: "a retire … not HOT, because both columns are in partial-index predicates, so each retire adds index entries to **the two partial indexes**".
- §3.7 vacuum table: `observations`/… "fillfactor = 80 … HOT updates stay on the page"; facts "fillfactor = 90".
- §5.1.6 "`CommitChunk` ≈ 15 ms".
- §9.5 move copy ≈ 1,000 facts/s per stream (A-O17′).

**Claim.** "≈ 6 index probes and ≈ 3 row updates per fact and no 300 M-row table, so the SLO is stated per 1 k retired facts: p95 ≤ 50 ms."

**Evidence.**

Retire of 2,000 facts: 20 documents of the 100 k namespace, using the cascade's own `UPDATE facts SET retired_at = coalesce(retired_at, now()), purge_after = now() WHERE namespace_id = $1 AND document_id = ANY(…)`.

| Variant | Time | Per fact | WAL |
|---|---|---|---|
| as specified (HNSW present) | **4,872 ms** | **2.44 ms** | 3.8 MB |
| same statement, `facts_embedding_hnsw` dropped inside the transaction | 92 ms | 0.046 ms | 3.4 MB |
| chunk retire, 200 chunks (`chunks` HNSW) | 248 ms | 1.24 ms | |
| `INSERT` of 5,000 facts (HNSW present / dropped) | 9,260 / 257 ms | 1.85 / 0.05 ms | 13 KB / 3 KB per fact |

The non-HOT update creates a new heap tuple. Every non-partial index gets an entry for it, **including the shared HNSW and BM25 indexes**. The partial `WHERE live` indexes do *not* get one, because the new version is not live, so §3.7 has the mechanism backwards.

HOT is unreachable even for columns that are in no index. `UPDATE facts SET invalidation_reason = 'x'` on 1,000 rows gave `n_tup_hot_upd` +400 / `n_tup_upd` +1,000 and took 1.49 s. The reason is page space: a 2–2.4 KB inline row (N94 `STORAGE MAIN`) at `fillfactor = 90` leaves 819 B free per 8 KB page, which is less than one row version. Every update of a fact row therefore re-inserts its vector into HNSW whatever column it touches:
- `invalidation_reason`;
- tag re-sync;
- un-retire in `CommitChunk`;
- the cascade's `coalesce(retired_at, now())` rewrite of rows already retired in grace.

**Consequences.**
- **Cascade cost.** ≈ 2.4 s per 1 k facts plus ≈ 0.12 s per 1 k facts for chunks (one chunk per 10 facts), against the 50 ms SLO.
  - A 12 k-fact document reaches the 30 s `statement_timeout` on the facts `UPDATE` alone.
  - The 100 k-fact transcript the SLO cites needs ≈ 250 s. It can never be deleted. It times out and rolls back on every retry, while holding the exclusive per-document lock and the shared fence (P-12).
  - These figures come from a 200 k-element graph. At 625 k elements HNSW insertion is slower still (log-depth search plus more cache misses).
- **Other costs on the same path.**
  - `FinalizeVersion` of a `REPLACE`, the cascade's chunk retire, and every `Invalidate` and `Restore` pay the same per-row HNSW cost.
  - Each retire leaves a dead HNSW entry that P-4's vacuum must repair.
- **`CommitChunk` ≈ 15 ms** holds only for small chunks. Ten facts plus one chunk are ≈ 20 ms of HNSW insertion alone, before BM25, ≈ 300 `fact_links` rows, GiST and GIN.
- **Move copy rate.** It is bounded by ≈ 1.85 ms per fact per stream (≈ 540 facts/s with HNSW only). That is below A-O17′'s assumed 1,000 facts/s before BM25 and B-trees.

**Recommendation (decision level).** Take mutable visibility state off the rows that carry vectors.
- **Option (a), preferred.** Split each vectored table into an **insert-only** vector table `(namespace_id, id, embedding)` and a narrow mutable row.
  - The HNSW (and the per-namespace partial) index lives on the vector table and is touched only at insert and at purge.
  - The arms already fetch the base row per candidate to test `live`/`as_of`/tags, so the join adds no heap fetches. P-6 shows that fetch is the dominant cost anyway.
- **Option (b), less disruptive.**
  - Remove `live`, `retired_at` and `purge_after` from the BM25 field lists and from every index predicate. Use one partial index `WHERE retired_at IS NOT NULL` for the purge sweep and test liveness as a heap filter.
  - Make HNSW partial `WHERE live` (pgvector supports partial HNSW), so a retire inserts nothing.
  - Set `fillfactor ≤ 60` on vectored partitions so a second 2.4 KB version fits on the page.
- **Delete cascade, either way.**
  - Make the synchronous visibility cut O(1) per document: one `deleted_documents (namespace_id, document_id, deleted_at)` row that every arm anti-joins (a per-namespace set bounded by the purge grace; small, fits an `InitPlan` array).
  - Move row retirement into `PurgeDocument`, batched.
  - Restate the SLO only after `TestDelete_CascadeScales` runs at 625 k rows per partition.

### P-2: blocker [regression: N79]: the depth-64 frontier does not fail closed for `as_of`. Superseded descendants beyond depth 64 stay servable. Reproduced; also REVIEW-3 H-2

**Where.**
- `engram_flag_lineage`, `engram_adjust_invalidation`.
- Comment: "fails closed on the frontier (over-hiding is safe)".
- §3.3.6 visibility predicate `… AND NOT (stale_delete AND superseded_at IS NULL)`.

**Evidence.** One observation with 70 versions chained `(o, v) → (o, v−1)`, version 1 having input fact F. The test retires F and deletes its `observation_inputs` row, as the cascade does.
- `engram.lineage_flagged = 64`, `lineage_frontier = 1`.
- Versions 1–65 are `derived_from_deleted`.
- The observation is `stale_delete`, so version 70, the current one, is hidden.
- **Versions 66–69 remain `live = true`.** `as_of = day 68.5` returns `obs v68`, written transitively from F.

The frontier flag hides only the *current* version, because the version mirror of `stale_delete` is ignored once `superseded_at` is set. An observation updated more than 65 times is ordinary for a long-lived preference or belief. Each further version extends the leak.

**Recommendation.**
- Lineage within one observation is a chain. Close it in O(1) with `UPDATE observation_versions SET derived_from_deleted = true WHERE (namespace_id, observation_id) = … AND version >= v_min` per affected observation.
- Walk only *cross-observation* edges.
- On a frontier hit, flag **every version of the frontier observation from the frontier version up**, not only the observation row.
- Add the 70-version chain as a T1 regression test with an `as_of` probe.

### P-3: major [regression: N79/N84 × N94]: lineage flagging percolates through the candidate graph. One `Invalidate` takes 5.9 s and one fact delete hides two thirds of a namespace's observations, nearly all of the time going into HNSW re-insertion

**Where.**
- `engram_adjust_invalidation` (the walk executed three times), `engram_flag_lineage` (twice).
- §3.7 lineage sizing "≤ 11 candidate versions".
- §9.4 SLO "99 % of document deletes flag ≤ 100 observation versions".
- `consolidate.max_rebuilds_per_round` = 50.

**Evidence.** Namespace with 2,000 observations × 10 versions (20 k versions). Each version has an edge to its predecessor and to 10 random earlier candidate versions (≈ 190 k lineage rows), which is the §3.7 shape.

| Operation on one fact that version (o, 1) of one early observation used | Result |
|---|---|
| `engram_lineage_walk` from that version | 9,886 (version, depth) rows, 5,749 distinct versions, max depth 18, 73 ms |
| `UPDATE facts SET invalidated_at = now()` (trigger N84) | **5,891 ms**; 5,749 versions `hidden_by_invalidation > 0`; 1,324 observations `stale_write` |
| same with `observation_versions_embedding_hnsw` dropped in the transaction | 421 ms (HNSW = 93 % of the cost) |
| delete path (retire F, delete its inputs row) | **7,033 ms**; `lineage_flagged = 5748`; **1,324 of 2,000 observations `stale_delete`** (hidden) |

**Why.**
- A version shown as a candidate taints every later version that saw it, transitively. With about 10 candidates per prompt, the descendant set of an early version is a large fraction of the namespace.
- `live` is a stored generated column over `derived_from_deleted`, `hidden_by_invalidation`, `stale_delete` and `superseded_at`, so every flag flip is a non-HOT update. Each one re-inserts a 768-d vector into the partition HNSW (and into BM25, which also indexes `superseded_at` and `live`).
- The ≤ 100-versions-per-delete SLO cannot hold.
- At 50 rebuilds per round, 1,324 hidden observations need 27 consolidation rounds of LLM spend. Until then they are invisible.
- A `Restore` costs the same as the `Invalidate`.

**Recommendation.**
- Keep flags in a side table `observation_version_flags (ns, obs, ver, derived_from_deleted, hidden_count)`, not on the vectored row. The arms then filter on it through an anti-join on a small set, and flag flips stop being HNSW inserts (P-1 (a)).
- Bound taint semantically rather than by depth. Record lineage only for candidate versions whose **text was cited or quoted in the output**, not for every candidate shown. Alternatively, rebuild descendants from live sources eagerly in a bounded background job, and hide only the directly flagged versions synchronously.
- Compute the walk once per call into a temp array. It is currently recomputed two or three times.

### P-4: major: pgvector HNSW vacuum ("repair graph") is the shard's throughput ceiling for retire, purge, move cleanup and namespace delete. The bloat plan's "20 k rows/s per shard" is off by about three orders of magnitude

**Where.**
- §3.7: "purge throughput is deliberately capped at 20 k rows/s per shard to keep autovacuum ahead"; "`REINDEX INDEX CONCURRENTLY` … when dead fraction > 30 %"; "a whole-namespace purge of a 1 M-fact namespace takes ≈ 10 min and one vacuum cycle".
- Per-partition `autovacuum_vacuum_scale_factor = 0.02`, `threshold = 10000`, `cost_delay = 2`, `cost_limit = 1000`.
- `autovacuum_max_workers` is not set anywhere (default 3).

**Evidence.** `VACUUM (VERBOSE)` of `facts_p07`, ≈ 190–200 k rows, warm:

| Dead index tuples (share of partition) | Vacuum elapsed | Note |
|---|---|---|
| 1,000 (0.5 %), `INDEX_CLEANUP ON` | 51 s | parallel index worker |
| 2,000 (1 %) | 79 s | |
| 20,000 (≈ 10 %): 10 k facts retired, then purged | **231 s, 228 s user CPU** | B-trees took milliseconds; the HNSW took the rest |
| 1,000 (0.5 %, < 2 % of heap pages) | 0.09 s | PG 14+ index-vacuum **bypass**, so the HNSW was not touched |

**Why.**
- pgvector's `ambulkdelete` must find a new neighbour list for every element that pointed to a deleted one.
- With `m = 16` (32 layer-0 links), a dead fraction f affects ≈ 1 − (1 − f)³² of the graph: 15 % at f = 0.5 %, 69 % at 3.6 %.
- At the autovacuum trigger (2 % of 625 k plus 10 k, i.e. 22.5 k dead tuples, which is ≈ 11 k purged facts because retire and purge each leave one) a vacuum repairs ≈ 430 k elements. At the measured ≈ 1.2 ms per repaired element that is ≈ 9 min of CPU, before autovacuum's cost-delay throttling.
- A single autovacuum worker therefore clears ≈ 11 k facts per ≈ 10 min per vector index. The `chunks` and `observation_versions` HNSW indexes queue behind it.
- With 3 workers and 48 vectored partitions (+ 32 non-vectored), sustained retire-plus-purge capacity is in the tens of facts per second per shard, not 20 k/s.
- The 30 % `REINDEX` trigger never fires, because autovacuum repairs at 2–4 %. Yet rebuilding is cheaper than repairing:
  - a full build of this 200 k partition took **67 s** with 4 parallel workers;
  - repair at 10 % took 231 s.
- Namespace delete, tenant delete and move cleanup produce dead fractions of 10–100 % of a partition.

**Recommendation.**
- **Prefer rebuild over repair for mass deletes.** For namespace delete, tenant delete, move cleanup and purges above ~2 % of a partition:
  1. set `vacuum_index_cleanup = off` on the partition for the duration;
  2. `REINDEX INDEX CONCURRENTLY` the partition's HNSW;
  3. run one `VACUUM (INDEX_CLEANUP ON)`, which finds no dead elements in the fresh graph and is cheap.
- Avoid retire-time dead entries altogether (P-1 (a) or (b)).
- Set `autovacuum_max_workers` (≥ 6) and per-partition `autovacuum_vacuum_cost_limit` for vectored partitions explicitly.
- Size "purge throughput" from a measured repair rate at 625 k rows.
- Add an alert on autovacuum runtime per partition (P-14).

### P-5: major: under RLS the planner may not use a non-leakproof operator as an index qual. The entity-resolution trigram lookup degrades from an index scan to a scan of the shard's entities, and the "verified plans" were not run as `engram_app`

**Where.**
- §3.8 #12 "entity resolution … `entities_trgm_idx` (GIN, uuid + trigram)"; the tags GIN `facts_tags_gin` and `observations_tags_gin`.
- §3.8 BM25 arm ("the whole WHERE runs inside the index scan").
- N19 query registry with `EXPLAIN` assertions.
- `ns_isolation` is `FORCE`d and `engram_app` is `NOBYPASSRLS`.

**Evidence.** The §3.8 query `SELECT … similarity(canonical_norm, $2) … WHERE namespace_id = $1 AND merged_into IS NULL AND canonical_norm % $2 ORDER BY s DESC LIMIT 5`, with 85 k entities on the shard (50 k in the probed namespace):

| Run as | Plan | Time |
|---|---|---|
| superuser | Bitmap Index Scan on `entities_trgm_idx`, `Index Cond: namespace_id = … AND canonical_norm % …` | **6.7 ms**, 275 buffers |
| `engram_app` (`SET ROLE`, `engram.namespace_id` set) | **Seq Scan on entities**, `Filter: … canonical_norm % …`, 84,999 rows removed | **308 ms**, 2,576 buffers |

`pg_proc.proleakproof` is false for `similarity_op`, `arrayoverlap` and `arraycontains`. The planner (`restriction_is_securely_promotable`) refuses such a qual as an index qual when an RLS qual sits at a lower security level. At §3.7's 0.5 M entities per shard, a CommitChunk resolving ~10 entities therefore costs seconds, or at best a scan of the namespace's entire entity set per lookup.

The same rule keeps `tags && $q` and `tags @> $q` off the tags GIN indexes. pg_search's `|||` and `@@@` go through a custom scan whose RLS behaviour could not be checked here (pg_search unavailable). If its operator functions are not leakproof and the custom scan honours security levels, the BM25 arms lose Top-K pushdown entirely.

**Recommendation.**
- Audit and mark `LEAKPROOF` (superuser migration) the operator functions the arms rely on, *or* route these lookups through narrow `SECURITY DEFINER` SQL functions that take `namespace_id` from `current_setting` and re-check it.
- Make every N19 `EXPLAIN` assertion run **as `engram_app` with the scope GUCs set**, never as the owner or superuser.
- Add an M0 gate: the BM25 arm's plan under RLS on the real ParadeDB image.

### P-6: major: both semantic-arm plans exceed the per-arm budget at the band edges. The shared HNSW at 2–3 % selectivity costs 67–111 ms and 26–27 k buffers; the exact path at 20 k costs 41 ms and 8,000 heap pages; the heap is not in the hot set

**Where.**
- §3.3.4 band rule (exact below 20 k; shared HNSW at ≥ 2 % of the partition; the partial band "EMPTY at the 10 M target").
- §3.8 #2: the exact path "cost to be MEASURED cold in M0.6".
- §3.7 hot set (40 GB, no facts heap). REVIEW-5 A-3 raises the hot-set omission; this finding adds measurements.

**Evidence.** Run as `engram_app` under RLS, warm. Plans were forced with `enable_sort = off` (HNSW) or `enable_indexscan = off` (exact). `hnsw.ef_search = 150`, `iterative_scan = relaxed_order`, `max_scan_tuples = 20000`.

| Case | Plan | Rows removed by filter | Buffers | Time |
|---|---|---|---|---|
| 6 k namespace (3 % of partition) + tag filter `ANY` | HNSW | 3,588 | 26,056 | **87–111 ms** |
| 4 k namespace (2 %) + `as_of` cutting about half | HNSW | 3,959 | 27,257 | **67 ms** |
| same namespaces, no extra filter (clustered data: neighbours are in-namespace) | HNSW | 0 | ≈ 3,000 | 6.5 ms |
| 20 k namespace, exact path (bitmap on `(namespace_id, tags)` GIN + top-N sort) | exact | — | 8,007 (= 8,000 heap pages, **64 MB**) | **41 ms** |
| 6 k namespace, planner's own choice | `facts_mentioned_idx` + sort | — | 2,418 | 10 ms |

**What the measurements show.**
- With real data a tenant's neighbours are mostly *other* tenants' facts in the same partition. Cost then scales with 1/selectivity: ≈ 4.7 k visited tuples at 3.2 % (a 20 k namespace in a 625 k partition), each costing a heap fetch for the RLS, `namespace_id`, `live`, `as_of` and tag filter.
- That is ≈ 100 ms per arm warm. There are three such arms (facts, chunks, observations) against a 60 ms arm stage.
- The exact path is 2–10× cheaper than HNSW up to tens of thousands of rows here. The 20 k switch point is therefore on the wrong side.
- The exact path reads 8,000 pages for 20 k interleaved rows. Cold, that is 64 MB of random reads per arm call.
- Both paths read the facts heap (25 GB at target), which §3.7's hot set omits.

**Recommendation.**
- Set the exact-versus-HNSW switch from a measured crossover at 625 k rows with *shared-topic* vectors, not at 20 k.
- For the exact path, keep the vector side table of P-1 (a) **clustered by namespace** (`CLUSTER`, or a BRIN-friendly key order), which turns 8,000 random pages into ≈ 1,000 sequential ones.
- Budget the heap of every vectored table into the hot set, or state the cold p95.

### P-7: major [regression: N92]: `remote_apply` ties delete latency to standby *replay* of HNSW-heavy WAL; `synchronous_commit = on` already gives RPO 0; the topology has no shard standbys. Complements REVIEW-3 H-4

**Where.** N92, §9.3 "Acknowledged deletes survive failover", §9.1 Compose (no standby services, no `synchronous_standby_names`), R32.

REVIEW-3 H-4 shows that COMMIT blocks instead of returning `UNAVAILABLE` and that every commit becomes synchronous without a `local` role default. This review adds three points.

1. **`remote_apply` is the wrong level for the stated goal.** RPO 0 across promotion needs the commit record *flushed* on the standby: `synchronous_commit = on`, i.e. remote flush. `remote_apply` additionally waits until the standby has *replayed* everything up to the commit LSN, including other transactions' WAL.
   - Measured WAL is ≈ 13 KB per inserted fact with HNSW and ≈ 1.9 KB per retired fact.
   - Partition HNSW repair vacuums (P-4) and `REINDEX CONCURRENTLY` of a ≈ 1.1 GB partition index produce GBs of WAL, which recovery replays single-threaded.
   - Delete latency then equals the standby's replay lag, which during backfill or index maintenance is seconds to minutes. Nothing reads from the standby, so `remote_apply` buys nothing.
2. **The topology does not contain the standbys N92 requires.** §9.1 lists one `shard-N-postgres` per shard, and its `command` has no `synchronous_standby_names`. A synchronous standby per shard doubles the cell's Postgres footprint: 32 more 64 GB / 8-CPU instances and NVMe volumes per cell. It must sit on a *different host* to be worth anything, which settles Q5/A-O1 (multi-host cells) by necessity. Neither cost appears in §9 or §10.
3. **No standby observability.** No metric or alert reports `pg_stat_replication` sync state, `flush_lag` or `replay_lag` (P-14).

**Recommendation.**
- Use `synchronous_commit = on` for delete-class transactions and set the role default to `local`.
- Add the standby services, `synchronous_standby_names = 'ANY 1 (shard_N_standby)'` and the extra hosts to §9.1 and the cost model.
- Alert on sync-standby absence and on `flush_lag`.

### P-8: major: the move loader as written cannot run, and its copy rate is bounded by HNSW insertion

**Where.**
- §3.8 "Move loader … `CREATE TEMP TABLE tmp (LIKE <table>) ON COMMIT DROP; COPY tmp FROM STDIN BINARY; INSERT INTO <table> SELECT * FROM tmp ON CONFLICT …`" (§5.0 has "SELECT … FROM tmp").
- §9.5 "≈ 1,000 facts/s per stream".

**Evidence.**
- `CREATE TEMP TABLE tmp (LIKE facts); INSERT INTO facts SELECT * FROM tmp;` fails with `ERROR: cannot insert a non-DEFAULT value into column "fact_type_code" … is a generated column`. This applies equally to `live` and `tag_count` on `facts`, `chunks` and `observation_versions`, and the error is raised even for an empty `tmp`.
- `LIKE` copies generated columns as plain columns.
- The source side cannot `COPY facts TO STDOUT` either: `ERROR: cannot copy from partitioned table "facts"` (it must be `COPY (SELECT <non-generated columns> …) TO`).
- The positional `BINARY` stream must therefore list columns explicitly on both ends. That also makes §9.2's "COPY streams map columns positionally" guard insufficient: equal `schema_version` does not imply equal column *order* after a dropped and re-added column.
- Rate: a 5,000-fact `INSERT … SELECT` took 1.85 ms per fact with HNSW live versus 0.05 ms without. A stream is therefore ≈ 540 facts/s before BM25, GiST, GIN and B-trees, at a third of the target partition size.

**Recommendation.**
- Generate the loader's column lists from `pg_attribute` (`attgenerated = ''`, ordered by name), identical on both shards.
- Copy into the vector side table (P-1 (a)) with its HNSW **dropped on the target partition set** for the incoming namespace's bulk phase, then rebuild. This is legal because the namespace is `incoming` and unreadable.
- Re-derive A-O17′ from that.

### P-9: major: Temporal is a single fleet-wide `auto-setup` cluster whose history-shard count is never chosen and cannot be changed later; it is the fleet's write-path SPOF and throughput ceiling

**Where.**
- §9.1 `temporal: image temporalio/auto-setup:1.28.0`, 2 CPU / 4 GB; `temporal-postgres` with default settings, no volume tuning, no backup, no replica.
- N71 "ONE Temporal cluster per FLEET".
- R22/Q14 ("history shard count … cannot be changed after history accumulates"; decision deferred to Phase 1 exit).

**Why it matters now.**
- **History shards.** `numHistoryShards` is fixed when the cluster's persistence is created. `auto-setup` defaults to a handful (4), so a dev cluster cannot be grown into the production one. Q14's deferral turns into a cluster migration with every in-flight workflow drained.
- **Event rate.** Every retain chunk is ≈ 5 activities (`ExtractChunk`, `EmbedChunk`, `ResolveEntities`, `BuildLinks`, `CommitChunk`), each ≈ 3 history events plus workflow tasks. That is ≈ 20 events per chunk.
  - At the plan's own fill pace (§5.1.6: ≈ 29 days for 1 B facts across 4 cells, ≈ 40 chunks/s fleet-wide online), that is ≈ 800 events/s of steady persistence load before consolidation, purge, moves and the per-shard schedules.
  - `RetainBackfill` children "run at Postgres speed", which is one to two orders of magnitude more.
  - One 2-CPU frontend/history/matching process on one untuned Postgres is not sized for that, and nothing in §9 or §10 measures it.
- **Blast radius.** Cells are the failure domain (D3), but every cell's retain, consolidation, purge and move stops when the one fleet cluster or its database stops. `temporal-postgres` has no pgBackRest stanza, so a disk loss also loses every in-flight operation fleet-wide. Only the per-shard `operations` rows and the op-sweeper survive it.

**Recommendation.**
- Decide the production Temporal layout in M0, not at Phase 1 exit:
  - split services;
  - `numHistoryShards` sized for the backfill peak (e.g. 512–2,048);
  - a dedicated HA Postgres with backups;
  - load-tested with the retain activity shape.
- Re-examine N71. A cluster per cell, with cross-cell moves using the operations-table restart path the plan already has (N97), keeps D3's failure domain.

### P-10: major: migration rules that PostgreSQL 16 cannot honour on this schema: no `CREATE INDEX CONCURRENTLY` on partitioned tables, and stored generated `live` columns that can change only by rewriting the table

**Where.**
- §9.2 "index creation is always `CREATE INDEX CONCURRENTLY` … a migration never renames a column or changes a type in place".
- Generated `live` on `chunks`, `facts` and `observation_versions` (N41, N84 already changed the `observation_versions` definition once).
- §3.3.4 "indexes created on the parent".

**Evidence.**
- `CREATE INDEX CONCURRENTLY facts_test_cic ON facts (…)` fails with `ERROR: cannot create index on partitioned table "facts" concurrently`.
- PG 16 has no `ALTER COLUMN … SET EXPRESSION` (a syntax error here; it arrived in PG 17).
- `ALTER TABLE … ADD COLUMN … GENERATED ALWAYS AS (…) STORED` rewrote the test table (the relfilenode changed) under `ACCESS EXCLUSIVE`.

**Consequences.**
- Every index change on the five big tables needs the per-partition procedure: `CREATE INDEX CONCURRENTLY` on each partition, `CREATE INDEX … ON ONLY` the parent, then `ATTACH PARTITION`. §9.2 and `engramctl` do not describe it.
- Any change to the visibility predicate means dropping and re-adding `live` on 10 M-row partitions under an exclusive lock. That also drops and rebuilds every BM25 and partial index that references it: hours of downtime per shard, ×100 shards.

**Recommendation.**
- Replace generated `live` with a plain boolean maintained by a `BEFORE INSERT OR UPDATE` trigger function. A predicate change then becomes a `CREATE OR REPLACE FUNCTION` plus a batched backfill, or better, P-1's side table.
- Write the partitioned-index procedure into §9.2 and `engramctl`, with its HNSW build time per partition (measured: 67 s for 200 k rows with 4 workers, ≈ 3.5–4 min per 625 k partition).

### P-11: major: the transaction-ID arithmetic is wrong by about 8×, so anti-wraparound vacuums, including HNSW repair, come about monthly on every partition, and nothing monitors XID age

**Where.** §3.7: "Transaction-id age is not a concern at these rates (`autovacuum_freeze_max_age` default, ≈ 2 × 10⁸ transactions per shard-year at 50 writes/s)".

**Recomputation.**
- 50 writes/s × 3.15 × 10⁷ s ≈ **1.6 × 10⁹ XIDs per year**, not 2 × 10⁸.
- The relay and consumers update `outbox_cursors` "every 100 ms" (§3.7 vacuum table). With ≈ 4 consumer rows, that alone is ≈ 40 XIDs/s.
- At ≈ 90 XIDs/s the default `autovacuum_freeze_max_age = 2 × 10⁸` is reached every ≈ 26 days. Every one of the ≈ 80 partitioned heaps then gets a non-cancellable aggressive vacuum.
  - That includes the 71 GB of `fact_links`.
  - Any dead tuples present trigger the HNSW repair of P-4.
- Nothing in §9.4 exports `age(datfrozenxid)` or `age(relfrozenxid)`.

**Recommendation.**
- Batch cursor advances (one transaction per second, not per 100 ms).
- Set `vacuum_freeze_min_age` low on insert-mostly partitions, so the insert-triggered vacuums freeze as they go.
- Export XID age with a page-level alert (P-14), and correct §3.7.

### P-12: major: exclusive fence takers starve behind legal writers that last tens of seconds, and every failed 5 s attempt is a 5 s write brownout for the namespace. Measured; REVIEW-3 H-18 states the shape

**Where.** Header of `shard_schema.sql` (fence protocol), §9.5 "exclusive takers … `lock_timeout = 5 s` per attempt and retry with jitter".

**Evidence.**
- With a shared holder present, a queued `pg_advisory_xact_lock` makes `pg_try_advisory_xact_lock_shared` return **false** (verified), so writers are refused while the exclusive request waits.
- The exclusive request then timed out (`canceling statement due to lock timeout`) behind a 6 s shared holder.
- P-1 measured that one delete-cascade or `FinalizeVersion` transaction on a 2 k-fact document holds the shared fence for ≈ 5 s, and on a 10 k-fact document for ≈ 25–30 s. `PurgeBatch` (1,000 facts) holds it for ≈ 0.3 s plus HNSW work.
- So every move freeze, delete freeze or restore fence attempted during such a transaction fails. Each attempt first refuses every writer of the namespace for 5 s. The retry loop turns into a periodic write outage plus a move watchdog rollback.

**Recommendation.**
- Bound *writer* duration, not only the exclusive wait. P-1's O(1) visibility cut and batched retire transactions under 1 s are the structural fix.
- Have the exclusive taker first announce the freeze through a namespace-level `freeze_pending` flag that writers check *before* starting new long transactions, then acquire.
- Report `engram_namespace_frozen_retries_total` per cause.

### P-13: minor: backup sizing assumes file-level differentials stay small; HNSW and retire churn touch almost every segment; the Postgres image probably cannot run `archive_command`

**Where.**
- §9.3 "Differential daily … ≈ 10–30 GB/day at 10 chunks/s".
- §9.1 `archive_command=pgbackrest … archive-push` inside `paradedb/paradedb`.
- The `pgbackrest-conf: {}` volume, mounted read-only.

**Points.**
- **Differential size.** pgBackRest differentials copy every 1 GB relation segment that changed.
  - HNSW insertion rewrites neighbour pages throughout the graph.
  - Retire updates land on old heap pages.
  - Vacuum repair (P-4) touches most of the index.
  - Most segments of the facts, chunks and observation HNSW indexes, heaps and B-trees therefore change daily, and a differential approaches a full backup (≈ 150–300 GB). Block incremental (`repo-block=y`, pgBackRest ≥ 2.46) is the fix and should be the stated setting.
- **WAL volume.** WAL is ≈ 13 KB per inserted fact (P-1). 1 M facts per day per shard is ≈ 13 GB of WAL before vacuum and `REINDEX`. The "≤ 15 min WAL replay" RTO term should be computed from that.
- **Image and config (not verified here).** The ParadeDB image is built on the official Postgres image, which does not ship pgBackRest. If `archive-push` is absent, archiving fails, WAL accumulates in `pg_wal` until the volume fills, and the shard stops. `pgbackrest-conf` is an empty named volume, so no stanza or repo config exists either.

**Recommendation.**
- Build a shard image that includes pgBackRest.
- Bind-mount generated configs.
- Enable block incremental.
- Add `pg_wal` size and archive-failure alerts.

### P-14: minor: observability lacks the Postgres signals this design depends on, and the `ShardDown` alert reads a metric nothing exports

**Where.** §9.4 metrics and alerts; the §9.1 Compose has no `postgres_exporter`.

**Missing signals.**
- `pg_up` is used by `ShardDown` and `CatalogDown`, but no exporter service exists. The only PostgreSQL metrics are the application's own and pgBackRest's.
- Not exported at all:
  - autovacuum runtime and progress per partition (P-4);
  - `n_dead_tup` per vectored partition;
  - `age(datfrozenxid)` (P-11);
  - sync-standby state and `flush_lag`/`replay_lag` (P-7);
  - `pg_stat_progress_create_index` for `engramctl index` builds;
  - `pg_wal` size;
  - long-running transactions holding the namespace fence (P-12).

**Recommendation.** Add `postgres_exporter` per shard with custom queries for the above. Add alerts on:
- XID age above 50 % of `autovacuum_freeze_max_age`;
- any autovacuum longer than 30 min;
- no sync standby;
- the oldest transaction holding an `advisory` share lock older than 10 s.

### P-15: minor: Compose and role configuration drift from the text

**Where.** §9.1 and §9.5, `shard_schema.sql` role block.

| Item | Text says | Artifact does |
|---|---|---|
| HNSW parallel build | "parallelise with `max_parallel_maintenance_workers=4`" | not in the `postgres -c` list, so the default of 2 applies |
| `autovacuum_max_workers` | relied on by the bloat plan | not set (default 3; see P-4) |
| `ef_search` | §3.8 "≥ the arm cap (150 at MID, 400 at HIGH)" | `engram.yaml` `hnsw: { ef_search: 100 }` |
| lock timeouts | §9.5 "`ALTER ROLE engram_app, engram_worker SET lock_timeout`" | `engram_worker` does not exist in the SQL; `ALTER ROLE` takes one role |
| per-document exclusive lock in the cascade | "`lock_timeout = 5 s`" | `engram_app`'s role default is 2 s; the cascade must `SET LOCAL lock_timeout = '5s'` and does not say so |
| sync standby | N92 | no `synchronous_standby_names`, no standby services (P-7) |

**Recommendation.** Generate the `postgres -c` list and the role GUCs from one table checked by `engramctl config lint`.

### P-16: minor: the catalog's `namespaces_count` only ever increments, so placement drifts

**Where.** `catalog_schema.sql` `pick_shard` (`UPDATE shards SET namespaces_count = namespaces_count + 1`). Nothing decrements it on delete or move-out, and nothing increments it on a move target.

**Evidence.** `pick_shard('t1')` run as `catalog_app` works and increments the count (verified). No trigger, function or §5/§9 step adjusts it afterwards.

**Consequence.** Over a shard's life `namespaces_count < max_namespaces` (150) blocks placement on shards that have been emptied by moves or deletes. Targets also accept namespaces beyond the cap.

**Recommendation.** Derive the count from `namespaces` (`count(*) WHERE shard_id = … AND state <> 'deleted'`) in `pick_shard`. Alternatively, maintain it with a trigger on `namespaces` that covers `shard_id` and `state` changes.

### P-17: minor: a 32-shard cell is not a "one host or a few hosts" Compose project

**Where.** §9.1 "one host or a small fixed set of hosts (A-O1)"; `deploy.resources.limits` 8 CPU / 64 GB per shard; R20/Q5 open.

**Arithmetic.**
- 32 × (8 CPU, 64 GB) is 256 CPUs and 2 TB of RAM for primaries alone, plus api, workers, Envoy and pgbouncer.
- N92 standbys double that and must sit on other hosts (P-7).
- The memory cgroup limit counts the container's page cache, so `effective_cache_size = 48GB` is the *whole* cache available to a shard. That is less than the ≈ 40 GB hot set plus the 25 GB facts heap (P-6).

**Recommendation.**
- State hosts per cell (e.g. 4 shards per 512 GB host, 8 primary plus 8 standby hosts) and the overlay or static-endpoint decision now. Every sizing number in §3.7 and §9 depends on it.
- Size `effective_cache_size` and the container limit from the measured hot set.

### P-18: minor: deletion replay on restore re-applies `Invalidate` but not `Restore`, and the cascade it re-runs needs an `active` fence the restoring shard does not have

**Where.** §9.3 step 4: `deletion_log` kinds include `memory` (= Invalidate); "re-applies the D8 synchronous cascade". The cascade is "one fenced write transaction as `engram_app`", which requires ownership `active` at the epoch, while the shard is `frozen/restore` during step 4.

**Consequences.**
- A fact invalidated before T′ (T < T′) and restored after it is re-invalidated by the replay. Curation regresses silently.
- The replay cannot run on the code path the text names. It needs an admin variant that bypasses the fence under the restore freeze, and the D16 tests must cover that variant.

**Recommendation.**
- Log `Restore` (kind `memory_restore`) and replay the *last* state per subject.
- Specify the admin replay path and include it in the T3 restore drill.

### P-19: nit: `engram_cleanup_namespace` probes all 16 partitions per deleted row

**Where.** `DELETE FROM %I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I WHERE namespace_id = $1 LIMIT $2)`.

**Evidence.** `EXPLAIN` on `fact_links` shows a Nested Loop with an Append of **16 Tid Scans** (`TID Cond: ctid = ANY_subquery.ctid`, `Filter: tableoid = …`). The outer `DELETE` has no `namespace_id` predicate, so it is not pruned.

**Recommendation.** Add `AND namespace_id = $1` to the outer `DELETE`. That gives one Tid Scan per row and also stops a ctid on another partition from being probed at all.

### P-20: nit: the lineage walk's `UNION` keeps depth in the row, so diamonds are explored once per distinct path length; callers recompute it two or three times

**Where.** `engram_lineage_walk`, `engram_flag_lineage`, `engram_adjust_invalidation`.

**Evidence.** In the P-3 DAG, 5,749 distinct versions produced 9,886 walk rows (73 ms per call), and the invalidation path runs the walk three times.

**Recommendation.** Use a `CYCLE`/`SEARCH`-free formulation that carries a visited array, or a plpgsql BFS with a temp table keyed on `(obs, ver)` that keeps `min(depth)`. Call it once per trigger.

---

## Checked and found sound (no finding)

- **Statement-level transition-table triggers under FK cascades** fire once per statement, not once per parent row. A `DELETE` of 1,000 facts showed 7 RI triggers × 1,000 calls, but `observation_inputs_lost` and `observation_sources_orphans` each fired with `calls=1`; a counting trigger saw a single 1,000-row transition table and a single 9,945-row `fact_links` set. Purge of 1,000 facts with ≈ 10 k links took 308 ms.
- **Touch triggers** (`engram_touch_updated_at`) are negligible beside the index costs above.
- **Fence semantics.** `pg_try_advisory_xact_lock_shared` refuses while an exclusive request is queued (verified). This is the N82 behaviour; its cost is P-12.
- **RLS pruning.** With `namespace_id` bound as a literal, RLS folds into a one-time filter and the other 15 partitions are pruned.
- **Storage.** `STORAGE MAIN` is inherited by all 16 partitions of each vectored table.
- **DDL.** Both DDL files apply cleanly on PG 16 with pgvector 0.8.6, and every self-check passes.
- **`pick_shard` as `catalog_app`.** The column-level `UPDATE` grant suffices for `FOR UPDATE SKIP LOCKED`, and identity columns need no sequence grant.
- **HNSW size and build.** Index size matches §3.7 (≈ 1.95 KB per vector). The partition build rate (67 s per 200 k rows, 4 workers) supports "rebuilds in minutes".

## Verdict

**Not ready to staff the storage layer as specified.** The schema is carefully reasoned for correctness, but it was not costed against how pgvector and PostgreSQL behave.

The central problem is one decision: inline vectors (N94) on rows whose visibility is mutable state in a generated column referenced by indexes (N41, N79, N84). That makes every retire, invalidate, restore, flag flip and supersede a full HNSW insertion now (P-1, P-3) and an HNSW graph repair later (P-4). The delete path is about 50× over its SLO and cannot complete for large documents; lineage flagging costs seconds per fact; vacuum, not ingest, bounds purge and move cleanup.

Separating vectors from mutable state, and making the synchronous delete cut O(1), fixes P-1, P-3, P-4, P-8 and most of P-12 together. Four further points are independent and needed regardless:
- the `as_of` frontier fix (P-2);
- RLS-aware plans (P-5);
- a decided Temporal production layout (P-9);
- PG-16-legal migration mechanics (P-10).

With those decisions the design is buildable. The remaining items are sizing and operations hygiene.

## What I could not verify

- **pg_search.** It is not installable here, so nothing about BM25 was executed:
  - its cost under non-HOT updates;
  - its plan and Top-K pushdown under RLS as `engram_app` (P-5);
  - segment merge and vacuum behaviour;
  - its WAL volume.
- **Cold-cache latencies.** These would have required restarting a shared server and dropping the OS cache; the I/O terms in P-6 are computed. Everything else ran at 200 k facts per partition, not 625 k, so HNSW insert, search and vacuum numbers are lower bounds.
- **Real embeddings.** Clustered synthetic vectors understate cross-tenant filter rejection, so the shared-topic HNSW costs in P-6 are extrapolated from the tag and `as_of` filter runs.
- **Untested components.** pgbouncer (1.21+ prepared statements, `MAX_PREPARED_STATEMENTS`), Temporal throughput and history-shard behaviour, pgBackRest differential sizes, and whether the ParadeDB image ships pgBackRest (P-13).
- **Synchronous replication.** Not run; REVIEW-3 H-4 executed the blocking behaviour.
