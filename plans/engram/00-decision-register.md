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
| `request_id` | Client-supplied idempotency key for unary writes, scoped to (tenant, namespace, method), kept 24 h in the shard's `idempotency_keys` table; catalog-level writes (namespace/tenant/admin) keep theirs in the catalog DB. Reuse with a different request hash → `ALREADY_EXISTS` + `OperationConflict`. | None. |
| Binaries | `engram-api` (gRPC + Connect on one port), `engram-worker` (Temporal worker + outbox relay + move executor), `engram-mcp` (MCP adapter), `engramctl` (ops CLI). | One binary with modes (violates "API and worker are separate images"). |
| Proto packages | Public: `memory.v1` (MemoryService, DocumentService, NamespaceService, OperationService, ExportService, PageService). Admin: `memory.admin.v1` (ShardService, MoveService, TenantService). Internal (not served): `engram.internal.workflow.v1` (Temporal payloads), `engram.internal.events.v1` (outbox/Kafka events). | Single package (breaks independent versioning). |

## D2. What a shard is

| Decision | Rationale | Rejected |
|---|---|---|
| A shard is **one dedicated PostgreSQL 16 instance** (own container, own volume, own WAL, own backups), image `paradedb/paradedb:latest-pg16` (bundles pgvector ≥ 0.8, pg_search, pg_trgm), with a **pgbouncer** sidecar in transaction-pooling mode. | Physical isolation of IOPS, buffer cache, WAL, vacuum, backup/restore and migration blast radius. "No query spans shards" is enforced by construction: a connection belongs to exactly one shard. | Schema-per-shard in one cluster (shared buffer pool and WAL, one hot tenant degrades all, backups are not independent). Partition-set-per-shard (same problems, plus cross-partition planner risk). |
| Namespaces of **different tenants MAY share a shard** (default pool). A tenant with `isolation=dedicated` gets shards flagged `dedicated_tenant_id`. | 10,000 tenants cannot each own an instance; bin-packing is the only economic option. | One shard per tenant. |
| Isolation **within** a shard: (1) `namespace_id` is the **leading column of every primary key and every B-tree/GIN/GiST index** of every namespace-scoped table (HNSW and BM25 are single-column access methods: there, isolation comes from hash-partition pruning, the in-scan `namespace_id` filter and RLS; the only shard-wide indexes are the outbox PK on `seq` and the scheduler partial indexes on `operations`, listed as explicit exceptions in §3.4); (2) **Row-Level Security** on every namespace-scoped table, policy `namespace_id = current_setting('engram.namespace_id')::uuid`, application role `engram_app` is `NOBYPASSRLS`; the store sets `SET LOCAL engram.namespace_id`, `engram.tenant_id`, `engram.epoch` at the start of every transaction; (3) every **write** transaction does `SELECT 1 FROM namespace_ownership WHERE namespace_id=$1 AND epoch=$2 AND state='active' FOR SHARE` first and aborts with `FAILED_PRECONDITION` + `WrongShardOrEpoch` detail if it fails; **reads** accept `state IN ('active','frozen')`; (4) big tables are **hash-partitioned by `namespace_id` into 16 partitions** per shard (`facts`, `fact_links`, `chunks`, `entity_mentions`) so each HNSW/BM25 index is smaller and partition-pruned by the namespace predicate. | RLS makes a forgotten `WHERE namespace_id=` a bug that returns zero rows instead of leaking; the ownership `FOR SHARE` row is the fencing token for shard moves. | Per-namespace tables/partitions (DDL per namespace, thousands of relations per shard). Trusting application predicates only. |
| `namespace_ownership(namespace_id, tenant_id, epoch, state, freeze_reason)` exists on **every shard** and mirrors the catalog for the namespaces that shard hosts. States: `incoming`, `active`, `frozen`, `moved_out`; `freeze_reason ∈ {move, delete, restore}` says why a namespace is frozen. | The shard, not the catalog cache, is the source of truth for "may I write here now". | Catalog-only fencing (a stale cache would allow writes to the wrong shard). |

## D3. Sizing (assumptions marked A-n)

| Quantity | Value |
|---|---|
| Shard soft cap / hard cap | 10 M live facts / 20 M live facts; ~150 namespaces; 300 GB volume |
| Shard instance | 8 vCPU, 64 GB RAM, NVMe ≥ 10 k IOPS (A-1) |
| Vectors | `halfvec(768)` in-row (pgvector ≥ 0.7): 1.5 KB/fact; HNSW `m=16, ef_construction=128`, `hnsw.ef_search=100`, `hnsw.iterative_scan=relaxed_order`, `hnsw.max_scan_tuples=20000` |
| Footprint at 10 M facts | facts heap ≈ 12 GB; vectors ≈ 15 GB; HNSW ≈ 18 GB; BM25 ≈ 4 GB; `fact_links` (≈ 30/fact → 300 M rows of three UUIDs: heap ≈ 30 GB + PK ≈ 21 GB + reverse index ≈ 20 GB) ≈ 71 GB (≈ 35 GB at 15 links/fact); chunks (1 M × 3 KB + vectors + HNSW) ≈ 7 GB; entities/mentions ≈ 5 GB; observations incl. versions, vectors, HNSW and BM25 ≈ 8 GB; ingest ledger ≈ 6 GB; **≈ 158 GB total (≈ 123 GB at 15 links/fact), ≈ 40 GB hot working set**; inside the 300 GB volume and the 64 GB RAM budget. Detailed per-table sizing in §3.7. |
| Fleet for 1 B facts | 100 shards at soft cap (50 at hard cap) |
| **Cell** = one API + worker stack | serves **≤ 32 shards** (bounded by per-instance pool 32 × 16 = 512 conns through pgbouncer, and 32 Temporal task queues × 2 pollers per worker). 1 B facts = 4 cells. MVP = 1 cell. A request that resolves to a shard in another cell is **forwarded** by `engram-api` over gRPC to that cell's Envoy (same metadata, same deadline), phase 3. |
| Recall target | 50 QPS/shard, p95 < 300 ms at mid budget. Budget: authz+catalog 2 ms, query embedding 25 ms, 5 arms in parallel ≤ 60 ms p95, RRF 1 ms, cross-encoder on 150 pairs ≤ 120 ms p95, boosts+packing 3 ms, streaming overhead 5 ms → ≈ 215 ms p95 (A-2: gateway rerank latency) |
| Retain throughput | per worker: `min(32 / L_extract, R_extract_rpm / 60)` chunks/s with `L_extract` ≈ 3–6 s → 5–10 chunks/s/worker unconstrained; a 600 RPM gateway cap yields 10 chunks/s cell-wide. Backfills go through the gateway **batch** API (no RPM cap, ~50 % cheaper). |

## D4. Catalog (control plane)

| Decision | Rationale | Rejected |
|---|---|---|
| Storage: a small dedicated PostgreSQL `engram_catalog` (streaming replica for HA). Tables: `tenants`, `shards`, `namespaces`, `namespace_moves`, `catalog_events`. | Same operational skill set; transactional epoch bumps. | etcd/Consul (another system; no transactions with tenant config). |
| Cache: in-process `catalog.Resolver`, LRU 100 k entries, TTL 60 s, negative cache 5 s, invalidated by Postgres `LISTEN catalog_changes` (payload `{namespace_id, shard_id, epoch, state}`). | Sub-millisecond resolve on the hot path; near-instant invalidation on moves. | Redis (extra system; the shard-side fence already makes staleness safe). |
| Catalog unavailable: serve cached entries up to `stale_max = 10 min`; a cache miss returns `UNAVAILABLE` with `RetryInfo{2 s}`. Workers never call the catalog on the hot path (only the move executor's Plan/Freeze/Cutover activities do; every other catalog write a workflow needs, such as marking a namespace deleted or releasing a source shard after a move, goes through an admin RPC on `engram-api` or through `engramctl`): workflow inputs carry `(namespace_id, tenant_id, shard_id, epoch)` and the shard's ownership row verifies them. | Correctness never depends on cache freshness (D2 row 4). | Fail-closed on any staleness (needless outage). |

## D5. Move protocol (per-namespace epoch fencing)

States in `namespace_moves.state`: `planned → copying → catching_up → frozen → cutover → cleaning → done`, or `→ rolled_back` from any state before `cutover`.

1. **plan**: choose target; insert `namespace_ownership(ns, tenant, epoch e+1, 'incoming')` on target; catalog `namespaces.state='moving'` (writes still allowed at source, epoch e).
2. **copy**: one `REPEATABLE READ` transaction on source records `p0 = max(outbox.seq)` for the namespace and the safe low watermark `p_low ≤ p0` (the smallest `seq` that could still be held by an in-flight writer: `p0 − relay.gap_watch_window` worth of sequence numbers, bounded by the writers' `statement_timeout`), then streams every namespace-scoped table (namespace-ordered `COPY`) into target; blobs are copied prefix→prefix (`{src}/{tenant}/{ns}/` → `{dst}/{tenant}/{ns}/`).
3. **catch-up**: replay source `outbox` rows for the namespace with `seq ≥ p_low` onto target (each event idempotent by `(namespace_id, seq)` and applied by key, so re-applying an event already covered by the snapshot is a no-op; target keeps `move_applied_seq`). Loop until lag < 100 events or < 5 s.
4. **freeze**: catalog `state='frozen'`; source ownership `state='frozen'` (same epoch e). New source writes fail with `FAILED_PRECONDITION/NamespaceFrozen`; the API retries them with backoff for up to 30 s and otherwise surfaces `NamespaceFrozen{retry_after}` to the client. The drain wait is 15 s by default and 60 s at most; a freeze watchdog rolls the move back after 120 s. Reads continue at source.
5. **drain**: replay remaining outbox rows until `move_applied_seq = max(seq)`; verify row counts (and checksums up to 1 M facts, a 1 % sample above) between the static copies; terminate in-flight Temporal workflows for the namespace on the source task queue (all are restartable from durable per-chunk state) and record their **workflow ids** (operations carry an `operation_id`; the `consolidate` singleton and `page/{page_id}` workflows are restarted by workflow id).
6. **cutover** (one catalog transaction): `namespaces.shard_id = target, epoch = e+1, state='active'`; target ownership `state='active'`; source ownership `state='moved_out'`; `NOTIFY catalog_changes`. Restart the recorded workflows on the target task queue with the same workflow ids (hence the same `operation_id`s).
7. **cleanup**: after a 24 h grace, `engramctl` deletes the namespace's rows on source (admin role) and the old blob prefix.

Safety argument: a write is accepted only inside a transaction that holds `FOR SHARE` on an ownership row with `state='active'` and the caller's epoch; source is set `frozen` **before** target is set `active`, so at no instant do two shards hold `active` for one namespace. Duplicates are impossible because the target applies outbox rows keyed by `(namespace_id, seq)`; loss is impossible because freeze precedes drain and drain precedes cutover.

## D6. Outbox and Kafka

| Decision | Rationale | Rejected |
|---|---|---|
| Per-shard `outbox(seq bigint, namespace_id, epoch, event_type, payload bytea /*proto*/, created_at)` written in the **same transaction** as every state change. Payload type `engram.internal.events.v1.Event`. | Transactional outbox, never dual writes. It is also the change log a shard move replays. | Logical replication/CDC (Debezium) — another system, and per-namespace filtering is awkward. |
| Relay: one active relay per shard in `engram-worker`, elected with `pg_try_advisory_lock`, reads in `seq` order in batches of 500 using the shard-level `engram_relay` role, which bypasses RLS for `SELECT` on `outbox`/`outbox_cursors` only (it must read every namespace in `seq` order; it never reads any other table). Consumers keep cursors in `outbox_cursors(consumer, last_seq)`. Consumers: `index` (no-op for the built-in Postgres index, the external-engine adapter otherwise), `kafka` (optional), `move:<ns>` (temporary, during a move). Rows below every cursor and older than 7 days are deleted daily in batches of 10 k. | | |
| Sequence gaps: `seq` comes from a sequence, so a later `seq` can commit before an earlier one. The relay keeps a **gap watchlist**: a skipped `seq` is re-checked for `2 × statement_timeout` (statement_timeout = 30 s on writers), then declared aborted. | Bounded and provable (TLA+ `Outbox.tla`). | Serializing writers on a table lock (kills throughput); xmin-watermark tricks (wraparound-unsafe). |
| **Kafka is optional and off by default.** When on: topic per shard `engram.events.shard-{id}`, key = `namespace_id`, value = the same `Event` proto, schema published to the buf registry (BSR) and referenced by header `schema=engram.internal.events.v1.Event`. Only justification: external consumers (analytics/CDC, an external search engine) that need replay and fan-out. | Temporal already provides durable orchestration and Postgres provides a per-shard ordered log. | Kafka as the ingest queue (would put a second durable store in the write path). |

## D7. Search index

| Decision | Rationale | Rejected |
|---|---|---|
| MVP index = Postgres indexes on the shard: HNSW (`halfvec_cosine_ops`) on `facts.embedding`, `chunks.embedding` and `observation_versions.embedding`; **BM25 via pg_search** (`USING bm25`) on `facts.text`, `chunks.text` and `observation_versions.text` (observations are recalled through the semantic and lexical arms, D10); pg_trgm GIN on `entities.canonical_name`. The `index.Index` interface is `Transactional` here (written in the same tx as facts) and `Async` for an external engine fed by the outbox `index` consumer. | One system, transactional, rebuildable (`REINDEX`). | tsvector + `ts_rank_cd` (not BM25; kept as a fallback implementation when pg_search is unavailable, documented as lower quality). External engine in MVP (extra system). |

## D8. Document versions, delta retain, delete

| Decision |
|---|
| `documents(namespace_id, document_id, current_version, state, …)`, `document_versions(namespace_id, document_id, version, content_hash, status: ingesting/active/superseded/deleted, operation_id)`. |
| Chunk identity within a document is its **content hash**: unique `(namespace_id, document_id, content_hash)`. Chunk text (≤ 4,000 characters and ≤ 16 KiB of UTF-8; the column `CHECK` is on bytes) is stored in Postgres; the raw item body goes to the append-only `ingest_ledger` row plus a blob. |
| Retain (`REPLACE`) of version v: chunk → for each hash: live chunk exists → keep everything (no LLM, no embedding); retired-but-not-purged → un-retire; new → extraction cache lookup → LLM → insert. Chunks not in the new set get `retired_at = now()`, and so do their facts (denormalized `facts.retired_at`, set in the same statement group). `APPEND` only adds chunks. |
| Delete(document) is **synchronous in Postgres** for everything recall can see: facts `retired_at`, `fact_links` rows deleted, `entity_mentions` deleted, `observation_sources` rows deleted, observations left with 0 sources retired, observations that lost ≥ 1 source marked `stale=true` (reconsolidate), `page_sources` rows deleted and pages marked `stale_delete=true`. Blob deletion and physical row purge are asynchronous (Temporal `PurgeDocument` / `PurgeNamespace`, grace 1 h for retire, 0 for explicit delete), but the ack is returned only after the synchronous part commits. |
| Recall filters `retired_at IS NULL` everywhere; nothing else is needed for visibility. |
| Soft curation: `Invalidate(memory_id)` sets `facts.invalidated_at` (recall excludes; observations keep the source but are marked stale); `Restore` clears it. Distinct from `retired_at`. |
| Namespace delete = catalog state `deleting` (reads/writes rejected) → per-shard purge workflow → catalog row `deleted`. Tenant delete = all namespaces. |

## D9. `as_of` semantics

| Decision |
|---|
| Every fact stores `mentioned_at` (when the source *said* it; default = the item's `timestamp`) and `occurred_start`/`occurred_end` (when it *happened*). Chunks store `mentioned_at = item timestamp`. |
| Observations are **versioned**: `observation_versions(observation_id, version, text, effective_at, …)` with `effective_at(v) = max(max(mentioned_at) over the source facts cited by v, effective_at(v−1))`. The clamp makes `effective_at` monotone across versions: a later version that cites only older facts was still written with knowledge of the previous text, so it must not surface at an earlier `as_of`. Recall with `as_of = T` returns, for each observation, the latest version with `effective_at ≤ T` (none if the first version is later than T). Pages use the same rule (`page_versions.effective_at`). |
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
| Streaming | `Recall` is server-streaming: one result per message, flushed in groups of 10 after the last stage that fits the deadline, then a trailing `RecallStats` message with per-stage timings. A deadline hit mid-stream leaves the delivered results valid and omits the stats. |

## D11. Retain pipeline (Temporal)

| Decision |
|---|
| Workflow `RetainDocument`, id `ns/{namespace_id}/op/{operation_id}`, task queue `shard-{shard_id}`. Activities: `Chunk` (pure Go), `SummarizeDocument` (1 LLM call per document version, cached by document hash), `ExtractChunk` (1 LLM structured call per chunk, cached), `EmbedChunk` (facts + chunk), `ResolveEntities`, `BuildLinks`, `CommitChunk` (single transaction per chunk: facts, links, mentions, outbox), `FinalizeVersion` (retire missing chunks, mark version active). Per-chunk fan-out ≤ 32. Every activity is idempotent by `(namespace_id, document_id, version, content_hash)`; the epoch is a fence checked at commit time, never part of an identity or idempotency key (a restarted workflow after a move must recognise chunks committed at the previous epoch). |
| Extraction cache: blob key `{shard}/{tenant}/{ns}/xcache/{sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)}.json`. Per-namespace on purpose: a shared cache would let one tenant probe another's content via timing. |
| Chunking: heading-anchored, content-defined boundaries, target 3,000 chars, min 500, max 4,000, no overlap; each chunk gets a header `[doc summary ≤ 200 chars] > [heading path]` prepended for embedding and extraction (not stored in `text`, stored in `header`). |
| Consolidation trigger: `SignalWithStart` of the per-namespace `Consolidate` workflow (id `ns/{namespace_id}/consolidate`), debounced 30 s. |

## D12. Consolidation, Reflect, Pages, Export (phase 2–3)

| Decision |
|---|
| Consolidation: 8 facts per LLM call, ≤ 100 facts per round, candidate observations = top-10 by semantic similarity per batch; ops `create/update/delete` with cited `source_fact_ids`; bisect on failure 8 → 4 → 2 → 1; idempotency `batch_key = sha256(sorted fact ids ‖ prompt_version ‖ model)` and `op_key = sha256(batch_key ‖ op_index)` recorded in `consolidation_applied`. DB trigger retires any observation whose `observation_sources` count reaches 0 (observations never outlive sources). |
| Reflect: forced searches (observations, then facts), then ≤ 10 free iterations, ≤ 100 k context tokens, ≤ 300 s wall, per-tool deadline 10 s; tools `search_memories`, `search_observations`, `get_page`, `expand_fact`; citations are filtered against the set of ids actually returned by tools in the session; optional JSON-schema output validated server-side. Per-namespace `mission`, `directives`, `disposition{skepticism, literalism, empathy ∈ 1..5}`. |
| Pages: `pages(namespace_id, page_id, name, source_query, tag_filter, refresh_policy, current_version, stale_write, stale_delete)`, `page_versions(page_id, version, markdown_blob_key, effective_at)`, `page_sources(page_id, fact_id|observation_id)`. Refresh = LLM delta-edit with previous markdown + added/removed evidence. |
| Export: `{shard}/{tenant}/{ns}/export/v{n}/{manifest.json, facts.jsonl.zst, observations.jsonl.zst, chunks.jsonl.zst, pages/*.md}` plus `delta-v{n-1}-v{n}.jsonl.zst` computed by diffing consecutive snapshots by (id, version) — the outbox range is only an optimisation when it is still retained (D6 keeps 7 days), so deltas are always producible; `ExportService.StreamSnapshot` streams 1 MiB parts. |
| Config inheritance: system (file) ⊂ tenant (catalog JSONB) ⊂ namespace (catalog JSONB); keys `models.{extract,consolidate,reflect,embed,rerank}`, `chunk.target_chars`, `recall.default_budget`, `quota.*`. Resolved and cached with the catalog entry. |

## D13. Auth, quotas, observability

| Decision |
|---|
| JWT (EdDSA/RS256, JWKS cached 5 min) claims: `tenant_id`, `ns` (list of namespace ids or `["*"]`), `scopes ⊆ {memory.read, memory.write, memory.admin, tenant.admin}`. One `authz.Interceptor` (unary + stream) verifies the token, resolves the namespace via the catalog, checks `namespace.tenant_id == token.tenant_id` and allowlist membership, and puts `RequestScope{tenant, namespace, shard, epoch, scopes}` in the context. MCP and Connect adapters forward the same JWT to the core, so the interceptor is the only enforcement point. Tags are filters, never security. |
| Quotas per tenant and per namespace: `recalls_per_min`, `retains_per_min` (enforced in the interceptor, token bucket in the API, `RESOURCE_EXHAUSTED` + `memory.v1.QuotaExceeded` detail plus a runtime `google.rpc.RetryInfo`), `llm_tokens_per_day` and `max_facts` (tenant-level counters are rolled up in the catalog table `tenant_usage_daily`, fed by the API off the hot path, because no query may span shards; workflows enforce the namespace's share, which the catalog computes from the tenant limit and places in the workflow's config snapshot; exhaustion **defers** the operation — state `DEFERRED`, resumes at window reset — rather than failing it). |
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
internal/config     internal/telemetry   internal/api         internal/ledger    internal/errs
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

## D18. Decisions added during drafting (binding once listed here)

| Id | Decision | Where it came from |
|---|---|---|
| N1 | Leaf package `internal/errs` holds typed errors and their gRPC/Connect/Temporal mapping; `internal/api` must not be imported by services, store or workflows. | §2 |
| N2 | The move workflow `move/{namespace_id}/{epoch}` runs on the **target** shard's task queue `shard-{target}`; the executing worker opens a second, move-scoped pool to the source using the `engram_move` role. This is the only code path that holds two shard handles, and it is fenced by the move row. | §2, §5 |
| N3 | A per-shard Temporal schedule `shard/{id}/op-sweeper` (every 60 s) starts workflows for `PENDING` operations older than 2 min that have no workflow. The API still returns `UNAVAILABLE` if `StartWorkflow` fails after the ledger commit; the ack is not weakened. | §2 |
| N4 | The outbox relay reads with the `engram_relay` role (RLS bypass for `SELECT` on `outbox` only; `outbox_cursors` has no namespace column). Events carry every field a consumer needs (`DocumentDeleted` carries the deletion-log fields), so the relay never reads another table. | §2, §3 |
| N5 | A namespace of another tenant returns `NOT_FOUND` (no existence oracle); a same-tenant namespace outside the token's allowlist, or a missing scope, returns `PERMISSION_DENIED`; a namespace in state `deleting` returns `FAILED_PRECONDITION`. | §2, §4 |
| N6 | `chunks.content_hash = sha256(text)` excludes the contextual header; the header hash is stored separately and compared at `FinalizeVersion` to decide re-embedding without re-extraction. | §2, §5 |
| N7 | Raw bodies larger than 64 KiB are written to blob `{shard}/{tenant}/{ns}/ledger/{sha256}` before the ledger transaction; the ledger row stores the hash and the key. Smaller bodies are stored inline in the ledger row. | §2, §3 |
| N8 | Streams fix `RequestScope` when opened; a JWT expiring mid-stream does not abort the stream (Reflect may run 300 s). | §2 |
| N9 | Retain groups items by `document_id` and creates one Operation per document; `RetainResponse` returns the aligned list. A caller-supplied `operation_id` is used verbatim for a single document and as the UUIDv5 namespace for several. | §4 |
| N10 | The public `Namespace` message hides both `shard_id` and `epoch`; the epoch appears only in the `WrongShardOrEpoch` error detail and in `memory.admin.v1`. | §4 |
| N11 | Deadlines are mandatory: a missing deadline is `INVALID_ARGUMENT`; per-method caps (Recall 10 s, Retain 30 s, Reflect 330 s, WaitOperation 65 s, StreamSnapshot 600 s, others 30 s) are enforced, not silently clamped. | §4 |
| N12 | Outbox events are thin (ids, versions and flags, never text or vectors); consumers read rows by id at the recorded epoch. | §4 |
| N13 | Embeddings travel inside Temporal payloads as little-endian float32 bytes and spill to a staging blob above 512 KiB. | §4 |
| N14 | `PageService` and `ExportService` are registered from day one and answer `UNIMPLEMENTED` until phase 3, so the contract and the adapters never change shape. | §4 |
| N15 | The outbox relay holds its election lock (`pg_try_advisory_lock`) on a dedicated direct Postgres connection (`shards[].direct`), never through pgbouncer: session-level locks do not survive transaction pooling. `LISTEN` is only ever used against the catalog, which has no pgbouncer. | §8, §9 |
| N16 | Fault-injection knobs compile only under build tags `faultinject` / `testauth`; a lint step fails the build if a release binary exports them. | §8 |
| N17 | Forwarded cross-cell requests carry `engram-forward-hops`; at most one hop, never re-forwarded. | §8 |
| N18 | Benchmark pins: judge `gpt-oss-120b` through the gateway, temperature 0, prompt `judge/v1`; `bench.lock` records gateway-reported model versions; one namespace per LongMemEval question and per LoCoMo conversation under tenant `bench`. Hindsight is measured in two configurations (its defaults and a model-matched one); parity is judged against the matched one. | §8 |
| N19 | Every store query is registered in `store.Queries` so the RLS canary is exhaustive; `engramlint sql` additionally requires an explicit `namespace_id` predicate in every query. | §8 |
| N20 | `token_usage.price_version` is recorded and cost is computed at write time, never re-priced. | §8 |
| N21 | Every shard keeps a `deletion_log(kind, subject_id, namespace_id, deleted_at)` (kinds: document, namespace, tenant, invalidate) written in the delete transaction and replicated to the catalog by a `deletion-log` outbox consumer; a restore replays the deletes recorded after the backup point before the shard is re-enabled. | §9 |
| N22 | `shard_meta(schema_version, engram_min_version, engram_max_version)` on every shard; api/worker mark an out-of-range shard `unavailable` instead of refusing to start; moves refuse on a schema-version mismatch; `migrate --shard` refuses during a non-terminal move. | §9 |
| N23 | Backups: pgBackRest per shard under `_backups/shard-{id}/`, AES-256 repository cipher, `archive_timeout = 60` (RPO ≤ 60 s), four weekly full sets (28 days) which is also the tenant-facing deletion SLA; blobs are not backed up (content-addressed and re-derivable or tombstoned). Restore fences the shard, bumps every hosted namespace's epoch, restarts in-flight operations and resets outbox cursors. | §9 |
| N24 | Envoy retries only a fixed allowlist of read methods and only on transport-level failures; writes and long streams have `num_retries: 0`. Secrets are file-mounted, re-read every 60 s and on `SIGHUP`. | §9 |
| N25 | Token metering is exactly-once through `token_usage_events(usage_key)`; the resolved config travels in the workflow input as `config_snapshot`. | §5 |
| N26 | `chunks.extraction_key = sha256(content_hash ‖ prompt_version ‖ model ‖ schema_version)`; a prompt or model bump is a new key, re-extracted through the ordinary retain path, with the old facts retired at `FinalizeVersion`; nothing is rewritten in place. | §5, §6 |
| N27 | Per-document serialisation: versions are assigned under the `documents` row lock; `FinalizeVersion` and the delete cascade share `pg_advisory_xact_lock(namespace_id, document_id)`; the higher version wins and `CommitChunk` stops with `superseded` when `documents.current_version > v`. | §5 |
| N28 | Per-shard maintenance schedules (`consolidate-sweep`, `page-cron`, `purge-sweep`, `outbox-trim`) run under the admin role with one read per tick; the move's catch-up consumer is pull-based with its own cursor, not a relay sink; the relay's lock lives on a direct connection. | §5 |
| N29 | Observation and page `effective_at` are monotone across versions (D9 clamp); stale observations are reconsolidated in update/delete-only batches. | §5 |
| N30 | Export: a single `WriteFiles` activity per snapshot, the manifest written last as the commit point; the thin client syncs by sorted merge of (id, version). | §5 |
| N31 | `RetainBackfill` pre-warms the extraction cache through the gateway batch API (`batch_jobs` table) and then runs ordinary `RetainDocument` children. | §5 |
| N32 | Both databases use schema `public`; helper functions are prefixed `engram_*` (shard) and `catalog_*` (catalog). Tags are normalised at write time: ≤ 32 per row, each matching `^[a-z0-9][a-z0-9._:/-]{0,63}$`, stored sorted and deduplicated so array operators behave as set operators; `engram_tag_match(mode, q, i)` is the SQL twin of the Lean decision procedure. | §3 |
| N33 | `observation_versions.superseded_at` and `page_versions.superseded_at` precompute the D9 rule (= `effective_at` of the next version), so an `as_of` query is a plain range filter inside the HNSW/BM25 scans. | §3 |
| N34 | `fact_links` stores undirected edges once (`src < dst`), causal edges directed; who/what/when/where/why live in one `w5 jsonb` column. | §3 |
| N35 | Operation states are exactly the proto's: `PENDING, RUNNING, DEFERRED, SUCCEEDED, FAILED, CANCELLED`; a partially failed retain is `SUCCEEDED` with `progress.units_failed > 0`. | §2, §3, §4 |
