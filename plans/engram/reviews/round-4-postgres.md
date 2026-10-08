# Engram round 4: Postgres, Temporal and operational review of the round-3 design

Scope: the D22 storage and operations design as it would run on PostgreSQL 16 with pgvector,
pgbouncer, pgBackRest and one Temporal cluster per cell. The review covers:

- insert-only content rows with vector side tables, and per-namespace partial HNSW indexes (creation, thresholds, count per partition, hygiene, `REINDEX`);
- the exact-scan fallback, the marker sets loaded on every request, and RLS with the `SECURITY DEFINER` lookups;
- the Expunge purge and the vacuum it causes; the 128 GB sizing and the IOPS table;
- `synchronous_commit = local`, the delete-intent store, restore and failover;
- the cost of the move's copy and reconcile, Temporal per cell, migrations, backups, metrics and runbooks.

Findings dispositioned in rounds 1 to 3 are not repeated unless the fix is wrong or missing; where a
finding continues a round-3 item this is stated. Ids are P-1 onwards for this round.

## How the evidence was produced

- **Server.** The shared scratch PostgreSQL 16.15 with pgvector 0.8.6, pg_trgm and pgstattuple. The VM has 4 vCPUs and 15 GB of RAM.
  - `shared_buffers` is 128 MB. It is the shared server's setting, which I did not change.
  - Buffer counts (hits plus reads) are therefore the primary metric. Timings are warm or OS-cached. `maintenance_work_mem` was set per session.
- **Schema.** Database `r4pg_shard` (mine, dropped at the end) received `sql/shard_schema.sql` unmodified, except that the `USING bm25` statements and `CREATE EXTENSION pg_search` were stripped.
- **Synthetic shard.**
  - Ten namespaces in two partitions.
  - `fact_vectors_p07` holds A (100 k facts) and B, C, D, E (40 k each), 260 k rows in all.
  - `fact_vectors_p03` holds namespaces of 2.1 k, 5 k, 10 k, 20 k and 50 k facts.
  - Rows were inserted interleaved across namespaces, as concurrent ingest leaves them.
  - Embeddings are `halfvec(768)`, built as the L2-normalised sum of one of 64 shared centroids and two noise vectors from a 20 k bank. Every namespace shares the same topic space.
  - Namespace A also has 2.83 M `fact_links` rows (about 30 per fact), 200 k `entity_mentions`, consolidation stamps and 5 k plus 230 k `entities` on the shard.
  - Every namespace has its partial HNSW from `engram_hnsw_ddl` (`m = 16, ef_construction = 128`).
- **Roles and plans.**
  - Arm queries ran as `engram_app` under RLS with the scope GUCs set and `plan_cache_mode = force_custom_plan`, using the §3.8 SQL.
  - The purge ran as `engram_admin`, and the reconcile queries as `engram_move`.
  - WAL figures come from `EXPLAIN (ANALYZE, WAL)` around a wrapper function, so they are backend-local and not polluted by other sessions.

---

## Findings (ranked)

### P-1: major [regression: N124]: the reconcile's re-copy keys miss rows written under old ids, so moves of active namespaces roll back deterministically. The whole-namespace checks it runs under the freeze also put the freeze at about twice its target

**Where.**
- §5.5.1 step 4: "re-copy rows with `created_at ≥ T_copy − 10 min` (every immutable table has `created_at` and an index `(namespace_id, created_at)`; UUIDv7 ids may serve as the key)".
- `shard_schema.sql` table-class comment: "tables with a UUIDv7 id re-copy WHERE id >= engram_uuid_v7_floor(t_copy - 10 min); fact_links by the id of either endpoint; the rest by created_at".
- §5.5.1: "Freeze window ≈ (rows written in the last 10 min) + (mutable rows …) + the blob checks: < 30 s for ≤ 1 M-fact namespaces".
- §9.4 Move SLO.

**Evidence.**

1. **Rows are inserted under old ids during the dirty copy.**
   - `start_move` pauses only the expunge and the shard-wide schedulers. The namespace stays `active`, and the `Consolidate` singleton is terminated only at `Drain`, after the freeze. So during the copy:
     - `fact_consolidation (PK namespace_id, memory_id, stamped_at)` receives `done`/`failed` stamps for facts committed long before `T_copy`. These come from a consolidation backlog, a quota `DEFERRED` until the next window, and N95's 7-day retry of failed stamps.
     - `chunk_vectors (PK …, chunk_id, embedding_model, embedding_effective_at)` receives `ReembedChunk` rows for old `chunk_id`s on every summary-refreshing `APPEND` (§5.1.2 step 8).
     - `fact_vectors` and `observation_version_vectors` receive new-model rows for old ids under `ReembedNamespace`.
     - `observation_versions`, `observation_inputs` and `observation_version_sources` receive new versions of old `observation_id`s.
   - The copy reads each table in PK order. A row inserted after the cursor passed its key is not copied, and a re-copy keyed by "UUIDv7 id ≥ floor" does not select it.
   - `count(*)` then differs, and the move rolls back. The retry meets the same writers, so a namespace with a consolidation backlog or `APPEND` traffic cannot be moved. These are exactly the hot namespaces the move exists for.
   - The failure is detected, not silent: the target is a subset of the source and the counts are compared. It is a liveness failure of rebalancing, not a loss.
2. **The indexes the text relies on do not exist.**
   - The DDL has no `(namespace_id, created_at)` index on any immutable table. The only `created_at` indexes are `operations_created_idx` and `ingest_ledger_received_idx`.
   - `fact_links` and `entity_mentions` have no `created_at` column at all.
   - A `created_at`-keyed re-copy is therefore a scan of the namespace's rows per table, inside the freeze. Examples: `observation_inputs` (≈ 4.5 M rows per 1 M facts, unpartitioned), `document_version_chunks` and `observation_version_sources`.
3. **The freeze formula omits work that scales with the whole namespace.** Measured as `engram_move` on namespace A (96 k facts, 2.83 M links), warm, serial:

| Reconcile step under freeze | A (96 k facts) | Linear at 1 M facts |
|---|---|---|
| `count(*)` of `fact_links` (index-only, visibility map set) | 0.85 s | ≈ 9 s |
| `count(*)` + `bit_xor(hashtextextended(pk::text,0))` of `fact_links` | 3.5 s | (links > 2 M: count only) |
| hash of `facts` / `fact_vectors` / `entity_mentions` | 0.08 / 0.11 / 0.22 s | ≈ 4 s |
| `engram_verify_fk(ns)` (56 FKs) | 1.8 s | ≈ 19 s |
| links re-copy probe (`src ≥ floor OR dst ≥ floor`, nothing new: BitmapOr on PK + reverse) | 0.2 ms | — |
| re-copy conflict path (`INSERT … ON CONFLICT DO NOTHING`, rows already present) | 315 k rows/s | — |

- Add the existence check of every referenced blob: ≥ 100 k `ver/` and `ledger/` keys for 1 M facts, at 64 in parallel and 10–20 ms per `HEAD`, takes ≈ 20–30 s.
- The freeze for a 1 M-fact namespace is then ≈ 55–65 s warm, against the "< 30 s".
- The target is worse: right after a bulk copy its visibility map is mostly unset, so the index-only counts become heap fetches.
- The re-copy window is also not "the last 10 min". It is everything inserted since `T_copy − 10 min`, which spans the whole bulk copy.

**Recommendation (decision level).**
- Give every insert-only table a per-namespace insertion key that is monotone in commit order and indexed `(namespace_id, ins_seq)`. A `bigint` from a shard sequence works, or `created_at` with the index. Key every re-copy and every count by it, never by an entity id.
- Before the freeze, run the whole-namespace counts, hashes, `VerifyFK` and blob check against a recorded `ins_seq` watermark, plus one catch-up pass. Under the freeze, verify only the rows above that watermark and the mutable merge-diff.
- Restate the freeze formula and add a T3 test: a move with an active consolidation backlog and `APPEND` re-embeds must complete.

### P-2: major [regression: N115/N116]: marker sets are not bounded by expunge lag. Re-extraction and curation leave permanent `fact_hidden` rows, and re-extracted facts are never purged

**Where.**
- §5.4.1: "sizes are bounded by expunge lag and alerted above 16 k entries".
- §9.6: "Marker sets above 16 k entries mean the expunge is days behind".
- §3.7: "markers are bounded by expunge lag"; R35.
- §5.4.2 "Other targets"; §6 "`engramctl reextract --namespace` … `FinalizeVersion` hides every live fact … by inserting `fact_hidden(reason = 'reextract')`".
- §5.4.6: "Replace retires a chunk … observations and pages derived from them stay visible".

**Evidence.**

1. **Re-extracted facts are never purged.**
   - `FinalizeVersion` inserts `fact_hidden(cause = 'reextract')` for the old-key facts of a *kept* chunk (N58). The chunk keeps its id and gets no `chunk_tombstones` row.
   - The Expunge targets are `MARKERS` (document tombstones), `CHUNK_TOMBSTONES` and `OLD_EMBEDDING_MODEL`, so nothing purges these facts. Their vectors, HNSW and BM25 entries, about 30 `fact_links` rows each, mentions and the `fact_hidden` row stay forever.
   - What triggers it: one `engramctl reextract` of an N-fact namespace, or any `REPLACE` whose `render_hash` changed. `render_hash` covers context, metadata, mission and the heading path, so editing a document's context re-extracts it.
   - The effect is N permanent marker entries and a doubled footprint for that namespace. `live_facts`, which drives `ShardNearCapacity` and placement, counts visible facts only and does not see it.
2. **`fact_hidden(invalidate)` is permanent by design.** It has no purge phase, so curation-heavy namespaces grow the set without bound. Above 16 k entries the namespace stays permanently in the anti-join mode, with the `MarkerSetLarge` ticket open.
3. **Measured cost near the threshold.**
   - Loading 16 k `fact_hidden`, 10 k `chunk_tombstones` and 1 k document tombstones takes 9.0–10.5 ms as `engram_app`, against the stated "≤ 3 ms at 16 k".
   - The semantic arm with 16 k-element `fact_hidden` and `chunk_tomb` arrays plans in **17.8–19.9 ms**, against 1.5 ms with empty sets. Execution is unchanged at 4–6 ms, because the `<> ALL` is hashed.
   - Under `force_custom_plan` every arm replans with the constants on every call, at about 6 arm statements per recall. That is ≈ 110 ms of planner CPU per recall at the threshold, against a recall CPU budget of ≤ 4 vCPU at 50 QPS (§9.1).
4. **Re-extraction hides derived content.** This is outside my lens and flagged for the correctness reviewer; it was checked against the SQL.
   - `engram_obs_version_hidden`, `engram_page_version_hidden` and the observation-arm SQL test `i.fact_id = ANY($fact_hidden)` over **all** causes.
   - So after a re-extraction, every observation and page version whose segment contains a re-extracted fact is hidden until it is rebuilt. That contradicts §5.4.6 and N42 ("a replace is a write").

**Recommendation (decision level).**
- Make re-extraction a purge target. Either:
  - purge old-key facts after the same 1 h grace as `CHUNK_TOMBSTONES`; or
  - express currency as a narrow `chunk_extraction(namespace_id, chunk_id, current_key)` row and test `f.extraction_key = current_key` by join. That is O(1) per candidate and not a growing set.
- Exclude `reextract` from the derived-version predicate.
- Keep `fact_hidden(invalidate)` out of the per-request arrays: always test it with the PK anti-join, which is cheap per candidate.
- Count hidden-but-unpurged rows in the capacity signals, and restate R35.

### P-3: major [regression of N112/N116; round-3 P-6 fixed only for cross-namespace selectivity]: within a namespace, selective tag or `as_of` filters make the planner seq-scan the whole partition

**Where.**
- §3.8 semantic arm: "The immutable copies on the vector row let every predicate run inside the scan, so the iterative scan walks the graph until 150 rows pass".
- §3.1 principle 5: "A query then visits only its own graph".
- The IOPS row "visited ≈ `ef_search`".

**Evidence.** Measured on namespace A (96 k vectors in a 260 k-row partition with 5 partial HNSW indexes), as `engram_app` with `ef_search = 150`, `iterative_scan = relaxed_order` and `LIMIT 150`:

| Filter (fraction of A admitted) | Plan chosen | Buffers | Time |
|---|---|---|---|
| none | A's partial HNSW | 1,882 | 4 ms warm |
| `as_of` (≈ 16 %) | HNSW, 902 rows removed by filter | 4,091 | 33 ms |
| `as_of` (≈ 5 %) | HNSW, 3,113 removed | 19,431 | 101 ms |
| `as_of` (≈ 1 %) | **Parallel Seq Scan of `fact_vectors_p07`**, all namespaces, 253 k rows removed | 65,080 (≈ 508 MB) | — |
| tag filter, 20 allowed documents (2 %) | **Parallel Seq Scan of the partition** | 65,080 | 86.5 ms warm, 3 processes |

- The vector tables carry only the PK, `*_model_idx` and the HNSW indexes. Nothing supports `document_id = ANY($allowed_docs)` or `mentioned_at <= T`, so the cheapest exact plan the planner can find is the partition scan.
- At target size (625 k rows of ≈ 2 KB, ≈ 1.3 GB per partition) with `max_parallel_workers_per_gather = 4` (§9.1), one tag-filtered arm call reads ≈ 1.3 GB with 5 processes.
- Tag filters are a first-class recall feature, `as_of` is the eval contract, and `chunk_vectors` and `observation_version_vectors` behave the same.

**Recommendation (decision level).**
- Choose the filtered path explicitly in Go.
  - Estimate the candidates: allowed documents × facts per document (a per-document fact count), or the `as_of` window count from `facts_mentioned_idx`.
  - At or below ≈ 20 k candidates, run an exact path driven from `facts_doc_idx` or `facts_mentioned_idx` and join `fact_vectors` by PK.
  - Above that, run the HNSW iterative scan with `hnsw.max_scan_tuples` raised to match.
- Make the arm transactions set `max_parallel_workers_per_gather = 0` and `enable_seqscan = off`, so a partition scan can never be chosen.
- Add a selectivity sweep (tags {100, 20, 5, 2, 1 %} and `as_of` deciles) to M1.2's exit, with p95 per arm.

### P-4: major [regression: N112/N119; round-3 P-4 fix incomplete]: index hygiene cannot run as written, and autovacuum repairs the per-namespace graph before any rebuild. Repair costs 5–6× a rebuild at 4 % dead

**Where.**
- §5.4.2 step 3: "for every touched partition index of the namespace, `pgstattuple` dead fraction above 5 % → `REINDEX INDEX CONCURRENTLY`; else autovacuum".
- N112: "the expunge runs `REINDEX INDEX CONCURRENTLY` … above 5 % dead fraction".
- §3.7: "capped at 20 k rows/s per shard so autovacuum stays ahead; … rebuilds it … instead of waiting for autovacuum to repair".
- §3.1: "a document purge dirties one small index".
- Step table: "`ReindexHygiene` | admin role (owner of the index)".

**Evidence.**

1. **`pgstattuple` cannot read HNSW.**
   - `pgstattuple('fv_a3ab6a9c2931_2db1c3')` returns `ERROR: index … (unknown index) is not supported`.
   - `pgstatindex` returns "not a btree index", and `pgstattuple_approx` returns "wrong relation kind".
   - HNSW elements are not marked dead before `VACUUM` anyway, so there is nothing for such a tool to measure. `HNSWDeadFraction` and `engram_hnsw_dead_fraction` therefore have no source.
2. **The purge feeds autovacuum, which repairs first.** Autovacuum fires at 2 % + 10 k dead tuples, and the insert-triggered runs come every 5 % of inserts. Either way, `ambulkdelete` repairs the namespace's graph before the hygiene step could rebuild it.

| Same partition, warm | Elapsed | Buffer accesses | WAL |
|---|---|---|---|
| `VACUUM` after purging 4 % of a 100 k-vector namespace (4,000 facts) | **207 s** | 89 M | **447 MB** (59 k FPIs) |
| `VACUUM` after purging 1 % of a 40 k namespace, no parallel | 18.9 s | 13 M | 67 MB |
| `REINDEX INDEX CONCURRENTLY` of the 38 k index after a 4 % purge, then `VACUUM` | 10.1 s + **0.57 s** | 0.55 M | 1 MB |
| first build of the 100 k index (2 workers) | 36.7 s | — | — |

- Rebuilding is cheaper than repairing from about 1 % dead upwards. The 5 % threshold is too high, and it is evaluated after the repair has already run.
- Autovacuum on the partition runs with cost delay 2 ms and cost limit 1,000, so the same 4 % repair holds a worker for ≈ 7 min.
- The "20 k rows/s" sentence is the round-3 P-4 figure. The measured repair clears ≈ 20 vectors/s for an indexed namespace.
3. `engram_admin` cannot run the `REINDEX` (P-9).

**Recommendation (decision level).**
- Track dead elements per index in `vector_indexes` (`purged_since_build` against `rows_at_build`), counted by the purge itself.
- Set `vacuum_index_cleanup = off` on the vector partitions permanently.
- Let the hygiene step, at a threshold of ≈ 0.5–1 %, rebuild every touched HNSW on a partition and then run `VACUUM (INDEX_CLEANUP ON)` on that partition. The vacuum then only scans.
- Delete the "20 k rows/s" sentence, and budget hygiene at ≈ 0.35 ms per vector with 2–4 workers.

### P-5: major [regression: N114]: the IOPS table undercounts page touches per MID recall by about 7×, and the hot set leaves out the links reverse index that every graph hop reads

**Where.**
- §3.7 IOPS table: "3 vector arms × ≈ 150 visited × 2 pages … ≈ 900; BM25 ≈ 200; graph arm (≤ 300 nodes) ≈ 300; per recall ≈ 1,400; 50 QPS at a 5 % miss rate ≈ 3,500 IOPS, inside A-1's 10 k".
- §9.1 budget row; D3 hot set; R37.
- §3.8 exact path: "≤ 2,000 rows, ≈ 3.2 MB".
- §3.7 `fact_vectors`: "≈ 1.6 KB each ≈ 16 GB".

**Evidence.** Measured buffer accesses per call:

| Component | Measured | Table |
|---|---|---|
| semantic arm, per-namespace HNSW, MID (ef 150, LIMIT 150), namespaces of 2.1 k–100 k | 943–1,882 (first-touch reads 729–1,655) | 300 |
| same at HIGH (ef 400, LIMIT 400) | 2,648–2,773 | — |
| exact path at 2,100 vectors (rows interleaved across namespaces) | 2,060 page reads (≈ 16 MB, not 3.2 MB) | — |
| graph arm, temporal family, 5 hops, budget 300 (warm 7 ms) | 3,702 | 300 (whole arm) |
| graph arm, semantic + entity family, 2 hops | 1,850 | — |

- Vector rows are 2,048 B each effective (`fact_vectors_p07` is 508 MB for 260 k rows: 4 per page). The 10 M-fact vector heap is therefore ≈ 20.5 GB, not 16 GB.
- §3.8 states that each hop reads both the PK and `fact_links_reverse_idx`. The reverse index (≈ 20 GB) is not in the hot set, which counts only "links PK half 10".
- Per MID recall: 3 × ≈ 1,250 + ≈ 5,500 + BM25 200 (not measured) + temporal and observation joins ≈ 10 k touches.
- At the plan's own 50 QPS and 5 % miss rate that is ≈ 25 k IOPS, against the 10 k budget.
- The hot set comes to ≈ 80 GB, against ≈ 80 GB of usable cache in a 112 GB container. R37's trigger is therefore reached at the stated miss rate.

**Recommendation (decision level).**
- Re-derive N114 from measured touches. M0.6 must use the real arm SQL, including the graph arm and the exact path.
- Raise A-1 to ≥ 50 k IOPS (local NVMe meets this), or cut the graph fan-out.
- Put the reverse index and the corrected vector heap into the hot set and re-check 4 shards per host.

### P-6: major [regression: N119]: purge WAL is unbudgeted. At 35–82 KB per purged fact for the deletes plus ≈ 110 KB per fact for the vacuum repair, one large document or namespace delete exceeds the stated daily WAL, the RTO replay term and the archive rate

**Where.**
- §9.1 Disk: "WAL ≈ 13 KB per fact … ≈ 13 GB/day at 1 M facts/day".
- §9.3 RTO: "WAL replay ≤ 15 min (≤ 13 GB)"; differential "≈ 20 to 40 GB/day".
- §3.7: "batches of 1,000 with 50 ms pauses (≈ 10 s for a 100 k-fact document)".
- §5.4.3 `PurgeRows`: "batches of 5,000", with no pause stated.

**Evidence.**
- **One purge batch.** 1,000 facts of A, each with ≈ 30 links, 2 mentions, 1 stamp and 1 vector, run as `engram_admin` with the §3.8 statement:
  - 396–778 ms per batch;
  - **35–82 MB of WAL** (4.3–10 k full-page images, ≈ 45 k records);
  - the 9 FK cascades are index-driven, with `fact_links(src)` taking 489 ms of 778.
- **The vacuum that follows.** It adds 447 MB for 4,000 facts (P-4).
- **A 100 k-fact document** (§5.4.9) therefore means:
  - ≈ 3.5–8 GB of WAL for the rows, plus up to ≈ 11 GB for the repair vacuums;
  - 45–85 s of purge time, not 10 s.
- **Rate at the specified pacing.** Up to ≈ 2,000 facts/s gives ≈ 70–160 MB/s of WAL for as long as the purge runs.
- **Namespace deletes.** A namespace delete of 1 M facts in 5,000-row batches with no pause produces tens of GB.
- **Moves.** The target's WAL is 535 B per link and 2 KB per vector, measured, so a 1 M-fact move writes ≈ 18–20 GB.
- **Compression.** `wal_compression = zstd` cut a vector-heavy batch only from 14.2 to 8.9 MB.
- **Budgets.** None of this is in the disk, WAL, archive or RTO budgets. `WALSizeHigh` pages at 50 GB on a 300 GB volume that holds 174 GB of data.

**Recommendation (decision level).**
- Throttle `PurgeBatch`, `PurgeRows` and move cleanup by WAL bytes per second (the per-batch `pg_current_wal_insert_lsn` delta, e.g. ≤ 25 MB/s per shard), not by a fixed pause.
- Set `wal_compression = zstd` in `gucs.yaml`.
- Restate the RTO as a function of the WAL since the last backup, and take a differential after every namespace delete and every move.
- Budget pgBackRest `archive-push-queue-max` and archive throughput.

### P-7: minor [regression: §3.7/§9.2]: HNSW builds do not fit `maintenance_work_mem` above ≈ 0.95 M vectors, and two concurrent parallel builds exceed `shm_size: 4g`

**Where.**
- §3.7: "`maintenance_work_mem = 2 GB` … is enough for any single per-namespace index".
- §9.2: "100 k about 35 s, 1 M about 6 min".
- §9.1 `shm_size: 4g`; quota `max_facts: 2000000`; "> 3 M facts" as a move candidate.

**Evidence.**
- **Memory per element.** A build at 32 MB logged "hnsw graph no longer fits into maintenance_work_mem after 14894 tuples". That is 2,253 B per element in memory, so 2 GB holds ≈ 0.95 M.
- **Cost past the limit.** That build (15 k elements in memory, 25 k inserted on disk) took 83.5 s serially, ≈ 3 ms per on-disk insert. In memory the rate is 0.28–0.37 ms per vector with 2 workers.
- **Shared memory.** A parallel build at `maintenance_work_mem = 1 GB` held 1.07 GB in `/dev/shm`: the DSM is sized by `maintenance_work_mem`.
  - At 2 GB, two parallel builds overlapping on one shard (sweeper plus hygiene, or a move target build) exceed 4 GB.
  - This was not reproduced to the error itself.

**Recommendation.**
- Have `engramctl index` set `maintenance_work_mem` per build to ≈ 2.4 KB × vectors.
- Serialize HNSW builds per shard, or size `shm_size` as concurrent builds × `maintenance_work_mem` + 2 GB.
- Restate the build table for namespaces of 1 M vectors and more.

### P-8: minor [regression: N112]: the per-namespace index lifecycle has a stuck state, and its retry silently accepts an invalid index

**Where.** §3.3.4: "The sweeper records `building` before the statement and `ready` after; a crash leaves an INVALID index that `engram_invalid_indexes` lists … (`CREATE … IF NOT EXISTS` makes a retry idempotent)". Also `engram_vector_index_plan`, `rollback_target` and namespace-delete `DropIndexes`.

**Evidence.**
1. **A `building` row is never retried.** `engram_vector_index_plan` returns `create` only when no `vector_indexes` row exists. A row stuck in `building` therefore yields `none` forever: for example, a crash after the row is recorded but before the CREATE INDEX is issued leaves no invalid index for anything to list.
2. **The retry accepts an invalid index.** Executed: a cancelled `CREATE INDEX CONCURRENTLY` leaves `indisvalid = f`. Re-running the generated `CREATE INDEX CONCURRENTLY IF NOT EXISTS` returns `CREATE INDEX` after "relation … already exists, skipping", and the index is still invalid.
   - The sweeper marks it `ready`, the planner ignores it, and the namespace is served by the exact path indefinitely. That path costs 2,060 pages at 2,100 rows and grows linearly (P-5).
3. **Rollbacks leave orphan indexes.** `rollback_target` deletes rows through `engram_cleanup_namespace`, which deletes the `vector_indexes` rows but cannot drop the target's HNSW indexes, built after the copy. Orphan indexes remain and their rows are deleted through the graph.
   - Namespace-delete `DropIndexes` enumerates `vector_indexes` and so misses such orphans.

**Recommendation.**
- Use an explicit `requested → building → ready | failed` machine with a lease.
- Before every CREATE INDEX, check `pg_index.indisvalid` and `DROP INDEX CONCURRENTLY` an invalid index; never rely on `IF NOT EXISTS`.
- Drop by the deterministic `engram_hnsw_ddl` names, and run the drop statements in `rollback_target` before the cleanup.

### P-9: minor [regression: N133e, N131]: runtime privileges, pools and timeouts do not match the role model

**Where.**
- Step table: "`ReindexHygiene` … admin role (owner of the index)" and "`DropIndexes` … admin role".
- N133e: "the worker connects as `engram_app` and the Expunge purge runs as `engram_admin`".
- §9.1 config: only `dsn_file` (`engram_app`) and `relay_dsn_file`.
- §9.1: "pgbouncer pool 24 … **shared**: recall ≈ 12, …, expunge ≤ 2".
- SQL header: writers run with "statement_timeout = idle_in_transaction_session_timeout = 30 s … the relay's 60 s gap watchlist relies on it".
- §3.3: "one attempt under `lock_timeout = 35 s`, longer than any legal 30 s writer"; R36.

**Evidence.**
1. **`engram_admin` cannot manage the indexes.** Executed as `engram_admin`:
   - `REINDEX INDEX CONCURRENTLY fv_…` → "must be owner of index";
   - `CREATE INDEX … ON fact_vectors_p05` → "must be owner of table";
   - `DROP INDEX` → "must be owner of index".
   - Hygiene, `DropIndexes`, `CleanupMove` and the sweeper's builds therefore need `engram_migrate` (owner, `BYPASSRLS`, owner of the `SECURITY DEFINER` functions) inside the long-running worker.
2. **Missing credentials and pools.**
   - The worker's configuration provides no admin, move or migrate credentials.
   - pgbouncer pools are per (database, user), so `engram_admin` and `engram_move` get their own pools. "Expunge ≤ 2" is not enforced by anything: there is one Expunge per namespace, and up to ≈ 150 per shard.
3. **No timeouts on two writing roles.** `engram_admin` and `engram_move` have no `statement_timeout` or `idle_in_transaction_session_timeout`.
   - Yet `engram_admin` holds the shared fence (`PurgeBatch`, `Materialize`, `Finish`) and writes outbox events (`DocumentDeleted{MATERIALIZED, PURGED}`, `NamespacePurged`).
   - So neither the 35 s argument nor A-F1's 60 s horizon covers it, and R36's `config lint` check is stated for `engram_app` only.
4. **The relay can read entities.** `GRANT EXECUTE ON ALL FUNCTIONS` reaches `engram_relay`. Through `engram_entity_fuzzy` (definer, `BYPASSRLS`) it can read any namespace's entity names by setting the GUC.

**Recommendation.**
- Run a separate index runner (the `engramctl` service on the control host) that holds `engram_migrate` and is fed by `vector_indexes` requests.
- List the admin and move DSNs and pools with explicit sizes.
- Set 30 s statement and idle timeouts on `engram_admin` and `engram_move`, raised per session only in `engramctl`.
- Revoke `engram_entity_fuzzy` from `engram_relay` and `engram_move`.

### P-10: minor [regression: N126]: two-hour `REPEATABLE READ` export snapshots block every concurrent index build on the shard and pin the vacuum horizon

**Where.**
- §5.7 `WriteFiles`: "one `REPEATABLE READ READ ONLY` transaction, … `StartToClose` 2 h … exports take no fence (writers are never blocked)".
- §9.2: CREATE INDEX CONCURRENTLY "under `lock_timeout = 5 s` … retried by `engramctl` up to 20 times"; "never holds a lock that blocks writers".

**Evidence.**
- **Executed.** With an open `REPEATABLE READ` transaction that had read only `documents`:
  - `CREATE INDEX CONCURRENTLY` on the unrelated `chunks_p05` with `lock_timeout = 5 s` failed after 5.0 s ("canceling statement due to lock timeout") and left an INVALID index;
  - without `lock_timeout` it waited until the snapshot ended (18 s).
- **What that blocks.** Any export blocks every CREATE INDEX CONCURRENTLY and `REINDEX CONCURRENTLY` on the shard for up to 2 h: the sweeper, hygiene, the mover's target build and the migration procedure. Each 5 s attempt leaves an INVALID index.
- **Side effects of the wait.** The waiting build holds `ShareUpdateExclusiveLock` on its partition, so autovacuum skips that partition.
- **The vacuum horizon.** The snapshot pins the shard's horizon: purge dead tuples cannot be removed, and the HOT tables bloat (`outbox_cursors` alone takes 1 update/s per consumer).

**Recommendation.**
- Drop the long snapshot. Content is insert-only, so `WriteFiles` can read short `READ COMMITTED` ranges bounded by the snapshot start (`ins_seq` of P-1) with marker sets read once; the existing `RecordSnapshot` re-check covers deletes.
- Have `engramctl index` refuse to start while any `backend_xmin` is older than a few minutes.

### P-11: minor: the hard cap does not fit the volume, and the capacity signals ignore stored but invisible rows

**Where.**
- D3: "10 M live facts / 20 M live facts; ~150 namespaces; 300 GB volume"; "50 at hard cap".
- §9.5: "`live_facts` > 20 M (hard cap) → page".

**Evidence.**
- **The volume runs out first.** At §3.7's 174 GB per 10 M facts, the hard cap needs ≈ 348 GB, more than the volume. The 240 GB `full` threshold is crossed at ≈ 13.8 M facts.
- **The volume is underestimated.** Vector rows are 2,048 B, not 1.6 KB, which adds ≈ 4.5 GB to `fact_vectors` (P-5).
- **The signal undercounts.** `live_facts` counts visible facts only. It excludes the rows of P-2, tombstoned rows not yet purged, old-model vectors after `ReembedNamespace`, and the WAL bursts of P-6.

**Recommendation.** Use a volume of ≥ 600 GB or a hard cap of ≈ 13 M, drive capacity signals from relation bytes, and re-derive the fleet count.

### P-12: minor [regression: N123]: restoring to a point before a move-in loses that namespace's blobs, and a tenant intent has no key that the per-namespace replay reads

**Where.**
- §9.3 step 8: "`engramctl blob gc --shard N --reconcile` deletes orphans from after `T`".
- N123/§5.5.5: "PITR to a point before a move-in … run an ordinary move whose source is a scratch instance restored from the old source's backup … `CleanupMove` stays at 24 h".
- §9.3: "Blobs are not backed up".
- §5.4.4: "put one tenant intent object", with the key `_control/deletes/{tenant}/{ns}/…`.

**Evidence (from the procedure text).**
- **The blobs are deleted.**
  - The moved-in namespace's blobs live under `{N}/…`, were written after `T`, and have no rows in the restored database, so step 8 deletes them as orphans.
  - The old source prefix `{S}/…` was deleted by `CleanupMove` 24 h after cutover.
  - The recovery move's blob check ("refuse cutover while any is missing") can therefore never pass.
- **The tenant intent is unreachable.** A tenant intent has no `{ns}` key, so the per-namespace listing of step 5 never sees it.

**Recommendation.**
- Exclude the prefixes of namespaces the catalog places on N from `blob gc --reconcile` until their recovery moves finish.
- Keep moved-out source prefixes, or a copy of them, for the 28-day backup window.
- Write one intent per namespace for a tenant delete.

### P-13: minor (unverified frequency) [regression: N122]: replay orders intents by the API replica's wall clock, so a `Restore` that closely follows an `Invalidate` can be undone by a replay

**Where.** N122 and §5.4.1: intents are named `{deleted_at_rfc3339}-{operation_id}` and applied "in name order …, last state per subject wins".

**Evidence (by reasoning).**
- `deleted_at` is set by whichever `engram-api` replica served the call, and Envoy balances per request.
- Suppose `Invalidate(f)` lands on one replica and `Restore(f)` on another within the clock skew between them. The `Restore` intent then sorts first.
- Replay applies the `Restore`, which is a no-op (`NOT_INVALIDATED`), then the `Invalidate`. The restored shard ends with `f` hidden, though the live shard had restored it: an acknowledged `Restore` is lost.
- Whether `Durability.tla`'s `IntentOrderLastWins` models a clock is unknown; the spec is not yet written (R34).

**Recommendation.** Order intents for one subject by a per-subject generation read under the fence, or make a `Restore` intent name the `Invalidate` operation it reverses and replay it only after that one.

### P-14: nit: lock timeouts for the exclusive document lock disagree

**Where.** §5.1.2 steps 2 and 7, §5.4.1 step 3.2 and §5.4.7 say `lock_timeout = 5 s`, while §3.8 `FinalizeVersion` says "35 s single attempt".

**Problem.** At 5 s, a delete that queues behind a legal 30 s `CommitChunk` fails.

**Recommendation.** Pick one value and generate it from the GUC table.

### P-15: nit: `engram_consumers_passed` scans the namespace's later outbox rows on every poll

**Where.** `engram_consumers_passed`: `max(o.seq) … WHERE o.namespace_id = p_ns AND o.created_at <= p_deleted_at` is served by `outbox_ns_seq_idx (namespace_id, seq)` and filters on `created_at`.

**Problem.** Every poll scans all of the namespace's later events, up to 7 days of them.

**Recommendation.** Store the delete event's `seq` in the tombstone; the marker transaction knows it.

---

## Checked and holding

- **Per-namespace partial HNSW: catalog and planner cost.**
  - With 25 partial HNSW indexes on one partition, planning takes 2.0–2.2 ms in a fresh session and 0.15 ms warm.
  - The plan prunes to one partition, folds RLS into a one-time filter, and picks the namespace's index at every size from 2,100 vectors up. (§3.3.4's "exact preferred at 2,103" did not reproduce; that is harmless.)
  - ≤ 450 indexes per shard is not a planner problem.
- **Marker arrays at execution.** `<> ALL` over 16 k constants is hashed: execution is 4.2–6.4 ms against 4 ms with empty sets. Only planning grows (P-2).
- **`engram_entity_fuzzy`.**
  - It is `SECURITY DEFINER`, owned by a `BYPASSRLS` role, with `search_path = public, pg_temp`, so `pg_temp` comes last and cannot shadow tables.
  - With 235 k entities on the shard it takes 17 ms per lookup. The same predicate run directly as `engram_app` is a parallel seq scan at 118 ms, so the P-5 fix of round 3 works.
- **Build rates.** HNSW builds run at 0.28–0.37 ms per vector in memory with 2 workers, which matches §9.2 below ≈ 0.95 M vectors.
- **Move loader rates.**
  - Inserts run at 104 k link rows/s and 55 k vectors/s without HNSW; the conflict path runs at 315 k rows/s.
  - So "≥ 2,000 facts/s per stream" is plausible; `facts` with BM25 was not measured.
- **The purge's FK cascades** through partitioned children are index-driven (9 RI triggers, 1,000 calls each). The links re-copy probe uses a BitmapOr on the PK and the reverse index.
- **`synchronous_commit = local`** on every role with no synchronous standby: no commit waits on replication, which is consistent with the relay's horizon for `engram_app` writers.
- **Temporal per cell** (512 history shards, a dedicated Postgres with a standby, op-sweeper and `operations`-table restart): no new issue inside this lens. Not load-tested.

## Counts and verdict

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 6 | P-1 to P-6 |
| minor | 7 | P-7 to P-13 |
| nit | 2 | P-14, P-15 |

**Verdict.** The round-3 storage model holds up on PostgreSQL. Its parts are immutable vectored rows, narrow mutable state, read-time markers, and per-namespace partial HNSW indexes, which plan cheaply, prune cleanly and stay correct under RLS. The six majors sit in the loop around that model:

- a move reconcile keyed by entity ids instead of insertion order;
- marker sets fed by causes the expunge never purges;
- filtered queries inside a namespace that fall back to scanning the whole partition;
- an index hygiene step that cannot run and comes after autovacuum's repair;
- an IOPS table about 7× low;
- purge WAL outside every budget.

Each has a decision-level fix that keeps the model.

- **Ready:** staffing the storage and recall layers, with P-2 to P-4 decided first.
- **Not yet:** freezing the hardware budgets (P-5, P-6, P-11) and enabling moves (P-1).

## What I could not verify

- **pg_search.** It is not installable here, so the following were not executed:
  - BM25 plans and Top-K pushdown under RLS;
  - segment merge and vacuum under purges;
  - BM25 WAL;
  - the lexical arm's page touches.
- **Scale and cache.**
  - Partitions were 87 k–260 k rows, not 625 k; HNSW repair and search costs grow with size, so the figures above are lower bounds.
  - Cold-cache latencies were not measured. The server is shared and has a 128 MB `shared_buffers`, so buffer counts are reported instead.
- **Real embeddings.** Synthetic vectors use shared topics. Real filter rejection rates may differ.
- **Not populated.** I generated no observation or page data, so the degraded-mode `observation_inputs` lookups (the "≤ 20 ms per arm") are untested.
- **Not reproduced.** The DSM out-of-space error itself (only `/dev/shm` usage was measured), the pgBackRest archive throughput, pgbouncer behaviour, and Temporal throughput.
- **Not executed.** Restore and failover, and the intent replay ordering (P-12 and P-13 are reasoned from the procedure text).
