# Engram — Decision Register (binding for every section of the plan)

This register fixes the cross-cutting decisions that every section of the implementation
plan must agree with. Sections may add detail; they may not contradict a row here without
changing the row. Each decision names the alternative that was rejected.

Project name: **Engram** (an engram is a physical memory trace). Go module `example.com/engram`,
Go 1.25. Reference system: Hindsight (github.com/vectorize-io/hindsight, MIT). This document is
a standalone design and is unrelated to the benchmark code that shares this repository.

## D1. Identifiers and naming

| Thing | Decision | Rejected |
|---|---|---|
| `tenant_id` | Opaque string `[a-z0-9-]{1,64}`, minted by the control plane. | UUID (harder to read in logs/metrics). |
| `namespace_id` | Server-assigned UUIDv7 string, globally unique. Namespaces also carry a tenant-unique `name`. Clients address `tenant_id` + `namespace_id`, never a shard. | Client-chosen ids (collisions across tenants complicate the catalog). |
| `shard_id` | `int32`, dense, assigned by the catalog. Metrics label `shard="7"`. | String names (no ordering, harder to bin-pack). |
| `epoch` | `int64` per namespace, starts at 1, incremented at every shard-move cutover and at every restore-from-backup. | Lease timestamps (clock-dependent). |
| `document_id` | Client-chosen string ≤ 256 bytes, unique per namespace; the upsert key. | Server ids (clients need deterministic replace). |
| `memory_id` (a fact), `chunk_id`, `entity_id`, `observation_id`, `page_id` | UUIDv7 strings (time-ordered, index-friendly). | bigserial (not portable across shards during moves). |
| `operation_id` | UUIDv7; client MAY supply it (idempotent async submit). Also the Temporal workflow id suffix. | Server-only ids (retries create duplicates). |
| `request_id` | Client-supplied idempotency key for unary writes; stored in a per-shard `idempotency_keys` table for 24 h. | None. |
| Binaries | `engram-api` (gRPC + Connect on one port), `engram-worker` (Temporal worker + outbox relay + move executor), `engram-mcp` (MCP adapter), `engramctl` (ops CLI). | One binary with modes (violates "API and worker are separate images"). |
| Proto packages | Public: `memory.v1` (MemoryService, DocumentService, NamespaceService, OperationService, ExportService, PageService). Admin: `memory.admin.v1` (ShardService, MoveService, TenantService). Internal (not served): `engram.internal.workflow.v1` (Temporal payloads), `engram.internal.events.v1` (outbox/Kafka events). | Single package (breaks independent versioning). |

## D2. What a shard is

| Decision | Rationale | Rejected |
|---|---|---|
| A shard is **one dedicated PostgreSQL 16 instance** (own container, own volume, own WAL, own backups), image `paradedb/paradedb:latest-pg16` (bundles pgvector ≥ 0.8, pg_search, pg_trgm), with a **pgbouncer** sidecar in transaction-pooling mode. | Physical isolation of IOPS, buffer cache, WAL, vacuum, backup/restore and migration blast radius. "No query spans shards" is enforced by construction: a connection belongs to exactly one shard. | Schema-per-shard in one cluster (shared buffer pool and WAL, one hot tenant degrades all, backups are not independent). Partition-set-per-shard (same problems, plus cross-partition planner risk). |
| Namespaces of **different tenants MAY share a shard** (default pool). A tenant with `isolation=dedicated` gets shards flagged `dedicated_tenant_id`. | 10,000 tenants cannot each own an instance; bin-packing is the only economic option. | One shard per tenant. |
| Isolation **within** a shard: (1) `namespace_id` is the **leading column of every primary key and every index** of every namespace-scoped table; (2) **Row-Level Security** on every namespace-scoped table, policy `namespace_id = current_setting('engram.namespace_id')::uuid`, application role `engram_app` is `NOBYPASSRLS`; the store sets `SET LOCAL engram.namespace_id`, `engram.tenant_id`, `engram.epoch` at the start of every transaction; (3) every **write** transaction does `SELECT 1 FROM namespace_ownership WHERE namespace_id=$1 AND epoch=$2 AND state='active' FOR SHARE` first and aborts with `FAILED_PRECONDITION` + `WrongShardOrEpoch` detail if it fails; **reads** accept `state IN ('active','frozen')`; (4) big tables are **hash-partitioned by `namespace_id` into 16 partitions** per shard (`facts`, `fact_links`, `chunks`, `entity_mentions`) so each HNSW/BM25 index is smaller and partition-pruned by the namespace predicate. | RLS makes a forgotten `WHERE namespace_id=` a bug that returns zero rows instead of leaking; the ownership `FOR SHARE` row is the fencing token for shard moves. | Per-namespace tables/partitions (DDL per namespace, thousands of relations per shard). Trusting application predicates only. |
| `namespace_ownership(namespace_id, tenant_id, epoch, state)` exists on **every shard** and mirrors the catalog for the namespaces that shard hosts. States: `incoming`, `active`, `frozen`, `moved_out`. | The shard, not the catalog cache, is the source of truth for "may I write here now". | Catalog-only fencing (a stale cache would allow writes to the wrong shard). |

## D3. Sizing (assumptions marked A-n)

| Quantity | Value |
|---|---|
| Shard soft cap / hard cap | 10 M live facts / 20 M live facts; ~150 namespaces; 300 GB volume |
| Shard instance | 8 vCPU, 64 GB RAM, NVMe ≥ 10 k IOPS (A-1) |
| Vectors | `halfvec(768)` in-row (pgvector ≥ 0.7): 1.5 KB/fact; HNSW `m=16, ef_construction=128`, `hnsw.ef_search=100`, `hnsw.iterative_scan=relaxed_order`, `hnsw.max_scan_tuples=20000` |
| Footprint at 10 M facts | facts heap ≈ 12 GB; vectors ≈ 15 GB; HNSW ≈ 18 GB; BM25 ≈ 4 GB; `fact_links` (≈ 30/fact → 300 M rows) ≈ 25 GB incl. indexes; chunks (1 M × 3 KB + vectors + HNSW) ≈ 7 GB; entities/mentions ≈ 5 GB; observations ≈ 2 GB; **≈ 90 GB total, ≈ 35 GB hot working set** |
| Fleet for 1 B facts | 100 shards at soft cap (50 at hard cap) |
| **Cell** = one API + worker stack | serves **≤ 32 shards** (bounded by per-instance pool 32 × 16 = 512 conns through pgbouncer, and 32 Temporal task queues × 2 pollers per worker). 1 B facts = 4 cells. MVP = 1 cell. A request that resolves to a shard in another cell is **forwarded** by `engram-api` over gRPC to that cell's Envoy (same metadata, same deadline), phase 3. |
| Recall target | 50 QPS/shard, p95 < 300 ms at mid budget. Budget: authz+catalog 2 ms, query embedding 25 ms, 5 arms in parallel ≤ 60 ms p95, RRF 1 ms, cross-encoder on 150 pairs ≤ 120 ms p95, boosts+packing 3 ms, streaming overhead 5 ms → ≈ 215 ms p95 (A-2: gateway rerank latency) |
| Retain throughput | per worker: `min(32 / L_extract, R_extract_rpm / 60)` chunks/s with `L_extract` ≈ 3–6 s → 5–10 chunks/s/worker unconstrained; a 600 RPM gateway cap yields 10 chunks/s cell-wide. Backfills go through the gateway **batch** API (no RPM cap, ~50 % cheaper). |

## D4. Catalog (control plane)

| Decision | Rationale | Rejected |
|---|---|---|
| Storage: a small dedicated PostgreSQL `engram_catalog` (streaming replica for HA). Tables: `tenants`, `shards`, `namespaces`, `namespace_moves`, `catalog_events`. | Same operational skill set; transactional epoch bumps. | etcd/Consul (another system; no transactions with tenant config). |
| Cache: in-process `catalog.Resolver`, LRU 100 k entries, TTL 60 s, negative cache 5 s, invalidated by Postgres `LISTEN catalog_changes` (payload `{namespace_id, shard_id, epoch, state}`). | Sub-millisecond resolve on the hot path; near-instant invalidation on moves. | Redis (extra system; the shard-side fence already makes staleness safe). |
| Catalog unavailable: serve cached entries up to `stale_max = 10 min`; a cache miss returns `UNAVAILABLE` with `RetryInfo{2 s}`. Workers never call the catalog on the hot path: workflow inputs carry `(namespace_id, tenant_id, shard_id, epoch)` and the shard's ownership row verifies them. | Correctness never depends on cache freshness (D2 row 4). | Fail-closed on any staleness (needless outage). |

## D5. Move protocol (per-namespace epoch fencing)

States in `namespace_moves.state`: `planned → copying → catching_up → frozen → cutover → cleaning → done`, or `→ rolled_back` from any state before `cutover`.

1. **plan**: choose target; insert `namespace_ownership(ns, tenant, epoch e+1, 'incoming')` on target; catalog `namespaces.state='moving'` (writes still allowed at source, epoch e).
2. **copy**: one `REPEATABLE READ` transaction on source records `p0 = max(outbox.seq)` for the namespace, then streams every namespace-scoped table (namespace-ordered `COPY`) into target; blobs are copied prefix→prefix (`{src}/{tenant}/{ns}/` → `{dst}/{tenant}/{ns}/`).
3. **catch-up**: replay source `outbox` rows for the namespace with `seq > p0` onto target (each event idempotent by `(namespace_id, seq)`; target keeps `move_applied_seq`). Loop until lag < 100 events or < 5 s.
4. **freeze**: catalog `state='frozen'`; source ownership `state='frozen'` (same epoch e). New source writes fail with `FAILED_PRECONDITION/NamespaceFrozen`; the API retries them with backoff for up to 30 s. Reads continue at source.
5. **drain**: replay remaining outbox rows until `move_applied_seq = max(seq)`; terminate in-flight Temporal workflows for the namespace on the source task queue (all are restartable from durable per-chunk state) and record their `operation_id`s.
6. **cutover** (one catalog transaction): `namespaces.shard_id = target, epoch = e+1, state='active'`; target ownership `state='active'`; source ownership `state='moved_out'`; `NOTIFY catalog_changes`. Restart recorded operations on the target task queue with the same `operation_id`s.
7. **cleanup**: after a 24 h grace, `engramctl` deletes the namespace's rows on source (admin role) and the old blob prefix.

Safety argument: a write is accepted only inside a transaction that holds `FOR SHARE` on an ownership row with `state='active'` and the caller's epoch; source is set `frozen` **before** target is set `active`, so at no instant do two shards hold `active` for one namespace. Duplicates are impossible because the target applies outbox rows keyed by `(namespace_id, seq)`; loss is impossible because freeze precedes drain and drain precedes cutover.

## D6. Outbox and Kafka

| Decision | Rationale | Rejected |
|---|---|---|
| Per-shard `outbox(seq bigint, namespace_id, epoch, event_type, payload bytea /*proto*/, created_at)` written in the **same transaction** as every state change. Payload type `engram.internal.events.v1.Event`. | Transactional outbox, never dual writes. It is also the change log a shard move replays. | Logical replication/CDC (Debezium) — another system, and per-namespace filtering is awkward. |
| Relay: one active relay per shard in `engram-worker`, elected with `pg_try_advisory_lock`, reads in `seq` order in batches of 500. Consumers keep cursors in `outbox_cursors(consumer, last_seq)`. Consumers: `index` (no-op for the built-in Postgres index, the external-engine adapter otherwise), `kafka` (optional), `move:<ns>` (temporary, during a move). Rows below every cursor and older than 7 days are deleted daily in batches of 10 k. | | |
| Sequence gaps: `seq` comes from a sequence, so a later `seq` can commit before an earlier one. The relay keeps a **gap watchlist**: a skipped `seq` is re-checked for `2 × statement_timeout` (statement_timeout = 30 s on writers), then declared aborted. | Bounded and provable (TLA+ `Outbox.tla`). | Serializing writers on a table lock (kills throughput); xmin-watermark tricks (wraparound-unsafe). |
| **Kafka is optional and off by default.** When on: topic per shard `engram.events.shard-{id}`, key = `namespace_id`, value = the same `Event` proto, schema published to the buf registry (BSR) and referenced by header `schema=engram.internal.events.v1.Event`. Only justification: external consumers (analytics/CDC, an external search engine) that need replay and fan-out. | Temporal already provides durable orchestration and Postgres provides a per-shard ordered log. | Kafka as the ingest queue (would put a second durable store in the write path). |

## D7. Search index

| Decision | Rationale | Rejected |
|---|---|---|
| MVP index = Postgres indexes on the shard: HNSW (`halfvec_cosine_ops`) on `facts.embedding` and `chunks.embedding`; **BM25 via pg_search** (`USING bm25`) on `facts.text` and `chunks.text`; pg_trgm GIN on `entities.canonical_name`. The `index.Index` interface is `Transactional` here (written in the same tx as facts) and `Async` for an external engine fed by the outbox `index` consumer. | One system, transactional, rebuildable (`REINDEX`). | tsvector + `ts_rank_cd` (not BM25; kept as a fallback implementation when pg_search is unavailable, documented as lower quality). External engine in MVP (extra system). |

## D8. Document versions, delta retain, delete

| Decision |
|---|
| `documents(namespace_id, document_id, current_version, state, …)`, `document_versions(namespace_id, document_id, version, content_hash, status: ingesting/active/superseded/deleted, operation_id)`. |
| Chunk identity within a document is its **content hash**: unique `(namespace_id, document_id, content_hash)`. Chunk text (≤ 8 KB) is stored in Postgres; the raw item body goes to the append-only `ingest_ledger` row plus a blob. |
| Retain (`REPLACE`) of version v: chunk → for each hash: live chunk exists → keep everything (no LLM, no embedding); retired-but-not-purged → un-retire; new → extraction cache lookup → LLM → insert. Chunks not in the new set get `retired_at = now()`, and so do their facts (denormalized `facts.retired_at`, set in the same statement group). `APPEND` only adds chunks. |
| Delete(document) is **synchronous in Postgres** for everything recall can see: facts `retired_at`, `fact_links` rows deleted, `entity_mentions` deleted, `observation_sources` rows deleted, observations left with 0 sources retired, observations that lost ≥ 1 source marked `stale=true` (reconsolidate), `page_sources` rows deleted and pages marked `stale_delete=true`. Blob deletion and physical row purge are asynchronous (Temporal `PurgeWorkflow`, grace 1 h for retire, 0 for explicit delete), but the ack is returned only after the synchronous part commits. |
| Recall filters `retired_at IS NULL` everywhere; nothing else is needed for visibility. |
| Soft curation: `Invalidate(memory_id)` sets `facts.invalidated_at` (recall excludes; observations keep the source but are marked stale); `Restore` clears it. Distinct from `retired_at`. |
| Namespace delete = catalog state `deleting` (reads/writes rejected) → per-shard purge workflow → catalog row `deleted`. Tenant delete = all namespaces. |

## D9. `as_of` semantics

| Decision |
|---|
| Every fact stores `mentioned_at` (when the source *said* it; default = the item's `timestamp`) and `occurred_start`/`occurred_end` (when it *happened*). Chunks store `mentioned_at = item timestamp`. |
| Observations are **versioned**: `observation_versions(observation_id, version, text, effective_at, …)` with `effective_at = max(mentioned_at)` over the source facts cited by that version. Recall with `as_of = T` returns, for each observation, the latest version with `effective_at ≤ T` (none if the first version is later than T). Pages use the same rule (`page_versions.effective_at`). |
| Recall with `as_of = T` filters facts and chunks by `mentioned_at ≤ T` **inside every arm** (semantic, lexical, graph expansion, temporal, chunks), not after fusion, so budgets are not consumed by invisible rows. |

## D10. Recall pipeline (no LLM calls)

| Stage | Decision |
|---|---|
| Arms (parallel, per-arm candidate caps low/mid/high = 50/150/400) | semantic (HNSW over facts), lexical (BM25 over facts), graph (bounded expansion from the top-20 seeds of semantic ∪ lexical over `fact_links`; node budget 100/300/1000; temporal links ≤ 5 hops × 10 neighbours; entity/causal/semantic links ≤ 2 hops), temporal (facts whose occurrence window overlaps the query window, ordered by distance to `query_timestamp`), chunks (BM25 ∪ HNSW over chunks). |
| Fusion | RRF with k = 60 over all arms; observations participate through semantic + lexical arms over `observation_versions`. |
| Rerank | cross-encoder through the gateway (`models.rerank`, default `bge-reranker-v2-m3`) on the top 50/150/300 fused candidates. If the remaining deadline < 150 ms, rerank is skipped and results are marked `stage=FUSED`. |
| Boosts | multiplicative, bounded: recency ≤ +10 %, temporal proximity ≤ +10 %, observation proof count ≤ +5 %; combined factor clamped to [0.75, 1.25]. |
| Tag filter (5 modes, Hindsight-compatible) | `tag_match` ∈ {ANY, ANY_STRICT, ALL, ALL_STRICT, EXACT}; let Q = query tags, I = item tags. ANY: I = ∅ ∨ I ∩ Q ≠ ∅ (untagged items pass); ANY_STRICT: I ∩ Q ≠ ∅; ALL: I = ∅ ∨ Q ⊆ I; ALL_STRICT: Q ⊆ I ∧ I ≠ ∅; EXACT: I = Q. An unset filter matches everything. Tags are filters applied inside every arm, never security. |
| Packing | greedy in rank order into `max_tokens` (default 4 k / 8 k / 16 k by budget); an item that does not fit is **skipped, not truncated**, and counted in `skipped_count`; packing never stops early. Token counting: `cl100k_base` via tiktoken-go. |
| Streaming | `Recall` is server-streaming: batches of 10 results after the last stage that fits the deadline, then a trailing `RecallStats` message with per-stage timings. |

## D11. Retain pipeline (Temporal)

| Decision |
|---|
| Workflow `RetainDocument`, id `ns/{namespace_id}/op/{operation_id}`, task queue `shard-{shard_id}`. Activities: `Chunk` (pure Go), `SummarizeDocument` (1 LLM call per document version, cached by document hash), `ExtractChunk` (1 LLM structured call per chunk, cached), `EmbedChunk` (facts + chunk), `ResolveEntities`, `BuildLinks`, `CommitChunk` (single transaction per chunk: facts, links, mentions, outbox), `FinalizeVersion` (retire missing chunks, mark version active). Per-chunk fan-out ≤ 32. Every activity is idempotent by `(namespace_id, epoch, document_id, version, content_hash)`. |
| Extraction cache: blob key `{shard}/{tenant}/{ns}/xcache/{sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)}.json`. Per-namespace on purpose: a shared cache would let one tenant probe another's content via timing. |
| Chunking: heading-anchored, content-defined boundaries, target 3,000 chars, min 500, max 4,000, no overlap; each chunk gets a header `[doc summary ≤ 200 chars] > [heading path]` prepended for embedding and extraction (not stored in `text`, stored in `header`). |
| Consolidation trigger: `SignalWithStart` of the per-namespace `Consolidate` workflow (id `ns/{namespace_id}/consolidate`), debounced 30 s. |

## D12. Consolidation, Reflect, Pages, Export (phase 2–3)

| Decision |
|---|
| Consolidation: 8 facts per LLM call, ≤ 100 facts per round, candidate observations = top-10 by semantic similarity per batch; ops `create/update/delete` with cited `source_fact_ids`; bisect on failure 8 → 4 → 2 → 1; idempotency `batch_key = sha256(sorted fact ids ‖ prompt_version ‖ model)` and `op_key = sha256(batch_key ‖ op_index)` recorded in `consolidation_applied`. DB trigger retires any observation whose `observation_sources` count reaches 0 (observations never outlive sources). |
| Reflect: forced searches (observations, then facts), then ≤ 10 free iterations, ≤ 100 k context tokens, ≤ 300 s wall, per-tool deadline 10 s; tools `search_memories`, `search_observations`, `get_page`, `expand_fact`; citations are filtered against the set of ids actually returned by tools in the session; optional JSON-schema output validated server-side. Per-namespace `mission`, `directives`, `disposition{skepticism, literalism, empathy ∈ 1..5}`. |
| Pages: `pages(namespace_id, page_id, name, source_query, tag_filter, refresh_policy, current_version, stale_write, stale_delete)`, `page_versions(page_id, version, markdown_blob_key, effective_at)`, `page_sources(page_id, fact_id|observation_id)`. Refresh = LLM delta-edit with previous markdown + added/removed evidence. |
| Export: `{shard}/{tenant}/{ns}/export/v{n}/{manifest.json, facts.jsonl.zst, observations.jsonl.zst, chunks.jsonl.zst, pages/*.md}` plus `delta-v{n-1}-v{n}.jsonl.zst` derived from the outbox range; `ExportService.StreamSnapshot` streams 1 MiB parts. |
| Config inheritance: system (file) ⊂ tenant (catalog JSONB) ⊂ namespace (catalog JSONB); keys `models.{extract,consolidate,reflect,embed,rerank}`, `chunk.target_chars`, `recall.default_budget`, `quota.*`. Resolved and cached with the catalog entry. |

## D13. Auth, quotas, observability

| Decision |
|---|
| JWT (EdDSA/RS256, JWKS cached 5 min) claims: `tenant_id`, `ns` (list of namespace ids or `["*"]`), `scopes ⊆ {memory.read, memory.write, memory.admin, tenant.admin}`. One `authz.Interceptor` (unary + stream) verifies the token, resolves the namespace via the catalog, checks `namespace.tenant_id == token.tenant_id` and allowlist membership, and puts `RequestScope{tenant, namespace, shard, epoch, scopes}` in the context. MCP and Connect adapters forward the same JWT to the core, so the interceptor is the only enforcement point. Tags are filters, never security. |
| Quotas per tenant and per namespace: `recalls_per_min`, `retains_per_min` (enforced in the interceptor, token bucket in the API, `RESOURCE_EXHAUSTED` + `QuotaFailure`), `llm_tokens_per_day` and `max_facts` (enforced in workflows; exhaustion **defers** the operation — state `DEFERRED`, resumes at window reset — rather than failing it). |
| Observability: OpenTelemetry traces with one span per stage; Prometheus metrics labelled `shard` (always) and `tenant` (metering counters only; never `namespace` as a label); per-namespace `token_usage(day, op, model, prompt_tokens, completion_tokens, cost_micros)` lives in the shard DB so it moves with the namespace. |

## D14. Go package layout (fixed)

```
cmd/engram-api  cmd/engram-worker  cmd/engram-mcp  cmd/engramctl
proto/memory/v1  proto/memory/admin/v1  proto/engram/internal/workflow/v1  proto/engram/internal/events/v1
gen/go/...                      (buf generate: protoc-gen-go, protoc-gen-go-grpc, protoc-gen-connect-go)
internal/authz      internal/catalog     internal/router      internal/gateway   internal/blob
internal/store      internal/index       internal/chunk       internal/extract   internal/entity
internal/link       internal/recall      internal/consolidate internal/reflect   internal/pages
internal/workflows  internal/outbox      internal/move        internal/export    internal/quota
internal/config     internal/telemetry   internal/api         internal/ledger
adapters/mcp        adapters/connect
formal/tla/*.tla    formal/lean/Engram/*.lean
```

## D15. Model defaults (through the gateway)

| Operation | Default | Notes |
|---|---|---|
| Embedding | `nomic-embed-text-v1.5`, 768-d, prefixes `search_document: ` / `search_query: `, L2-normalised | Matryoshka truncation to 512-d is a config knob, off by default. |
| Rerank | `bge-reranker-v2-m3` | Gateway rerank endpoint; batch of ≤ 300 pairs per call. |
| Extract / Summarize / Consolidate | `models.extract`, `models.consolidate` — a fast structured-output model class | Per-namespace override. Prompt versions `extract/v1`, `summarize/v1`, `consolidate/v1`. |
| Reflect / Page refresh | `models.reflect` — a stronger model class | Prompt versions `reflect/v1`, `page/v1`. |

## D16. Consistency model (stated once, referenced everywhere)

- **Retain** acks after the ingest-ledger row and the operation row are durable; nothing is visible yet. Facts become visible **per chunk** as each `CommitChunk` transaction commits; the operation reaches `SUCCEEDED` when the version is active. `OperationService.WaitOperation` is the read barrier: after it returns `SUCCEEDED`, a `Recall` on the same namespace observes every fact of that version (reads go to the shard primary; there are no read replicas on the recall path).
- **Observations and pages** lag facts by the consolidation debounce plus processing time; no bound is promised, but `Operation` exposes `consolidation_lag`.
- **Delete** acks after the synchronous cascade commits; from then on nothing from the document is returned by Recall, Reflect, GetMemory or Export. Blob and physical purge complete asynchronously and are observable via the delete operation.
- **Cross-namespace and cross-shard**: no ordering or visibility relationship whatsoever.

## D17. Phasing and effort (used by the roadmap)

Phase 0 foundations ≈ 6 engineer-weeks; Phase 1 MVP ≈ 30; Phase 2 (consolidation, reflect) ≈ 14; Phase 3 (pages, export, multi-cell, per-tenant config) ≈ 14; formal methods and evaluation harness run alongside ≈ 10. Total ≈ 74 engineer-weeks, 3 engineers ≈ 6 months.
