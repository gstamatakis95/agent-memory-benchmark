# Engram: implementation plan for a Go + Postgres agent-memory service

**Status:** design plan, v1.3 (2026-10-07, after three adversarial reviews: `reviews/round-1.md` / register D20, `reviews/round-2.md` / D21, `reviews/round-3.md` / D22). **Reference system:** Hindsight (github.com/vectorize-io/hindsight, MIT).
**Scope:** everything needed to build, verify and operate a Hindsight-class long-term memory service in Go,
exposed as gRPC (`memory.v1`), with PostgreSQL 16 as the per-shard system of record, Temporal for
asynchronous work, an AI gateway for every model call and blob storage for large or immutable data.

This document is standalone. It shares a repository with an unrelated benchmark project and does not
change that project's constraints.

**Third review (2026-10-07) and the redesign.** Rounds 1 and 2 were patched; round 3 found the same
areas regressing twice (storage updates, delete visibility, delete durability, shard moves), so it
produced a redesign (`reviews/round-3.md`, advice R1 to R5; register D22, N111 to N132) applied to
every section. One principle and one product guidance drive it: **rows that carry vectors or BM25 text
are immutable, mutable state lives in narrow side tables, and visibility is a read-time predicate over
small marker sets; deletes are rare, so a delete is an O(1) soft marker honoured by every read path at
ack and all physical work is an asynchronous, throttled expunge during which SLOs may degrade.** The
lineage walk, the synchronous delete cascade, the outbox-replay move, `remote_apply` and the shared HNSW
are gone. **The TLA+ specifications lag the design:** `Storage`, `Derivation`, `Durability` and a
rewritten `ShardMove` are specified in the review but not yet written, and the checked set is the one §7.1
lists (the outbox, consolidation exactly-once at 3 facts, `as_of` without deletes, and the pre-D20 move
protocol); the Phase 0 spec work closes the gap (§7.6, M0.7). The schedule absorbed the redesign: the
move simplification pays for the expunge and the delete-intent log, and `RetainBackfill` returned to the
committed scope, so the committed total is **≈ 80 ew against 78 ew of capacity** (a two-engineer-week
overrun, stated in §10).

## How to read this plan

| If you want to | Read |
|---|---|
| The one-page summary of every cross-cutting decision | Appendix A (decision register, binding for all sections) |
| The shape of the system and the request/data flows | §1 |
| Go packages and the interfaces you will implement or mock | §2 |
| The DDL, the catalog, the outbox and the blob layout | §3 and `sql/` |
| The public contract | §4 and `proto/` (run `buf lint` there) |
| How retain, consolidation, delete and shard moves actually run on Temporal | §5 |
| The LLM prompts, their schemas and how they are versioned and evaluated | §6 |
| What is model-checked (TLA+) or proved (Lean 4) and how code stays faithful | §7 and `formal/` |
| Test tiers, isolation and leakage tests, benchmarks | §8 |
| Compose topology, migrations, backups, observability, runbooks | §9 |
| Phases, exit criteria and engineer-weeks | §10 |
| Risks, open questions, rejected alternatives, non-goals | §11 and §12 |

## Executive summary

- **Tenancy is physical.** A namespace lives on exactly one shard, and a shard is a dedicated PostgreSQL
  instance (128 GB RAM, 4 per 512 GB host) with its own pgbouncer, blob prefix, Temporal task queue and
  metrics label. Nothing spans shards. Inside a shard, `namespace_id` leads every key and every index,
  row-level security is on for every namespace-scoped table, and every write transaction tries a
  **shared advisory lock** on the namespace (a refusal is a retryable `NamespaceFrozen`) and reads the
  ownership row at the caller's epoch. That row is the fencing token that makes shard moves safe even when
  the catalog cache is stale.
- **Content is immutable; state is narrow.** Facts, chunks, observation and page versions, links, mentions
  and evidence rows are insert-only and purge-only (`fillfactor 100`, no `UPDATE`, no `live` or
  `retired_at` column). Vectors live in insert-only side tables keyed by embedding model, and the
  semantic arm uses an exact scan below 2,000 vectors and **one small partial HNSW per namespace and
  partition** above, so a query visits only its own graph and a namespace delete or move cleanup is a
  `DROP INDEX` plus batched `DELETE` with no graph repair. Mutable state (`documents`, `observations`,
  `pages`, version meta, markers) sits in narrow HOT tables.
- **Routing is by catalog.** Clients send `tenant_id` + `namespace_id` and a JWT; one interceptor verifies
  the token, resolves the namespace through a cached catalog (existing namespaces are served from cache for
  as long as a catalog outage lasts, because the shard-side fence, not the cache, decides writability),
  checks ownership, and binds the request to a shard.
- **Retain is asynchronous and cheap to repeat.** Chunks are content-addressed, extraction results are cached
  in blob storage per (chunk hash, prompt version, model, render), and unchanged chunks cost nothing. Every
  activity is idempotent; commits are per chunk; visibility is per chunk; `WaitOperation` is the read barrier.
- **Recall makes no LLM calls.** Five arms (semantic, BM25, graph, temporal, raw chunks) run with `as_of`,
  tag and type filters and the **visibility predicate** applied inside each arm; RRF (k = 60, exact
  rational arithmetic), a cross-encoder rerank on 50 pairs at the default budget, bounded boosts and
  token-budget packing follow. The budget is a critical-path sum, 216 ms p95 at MID, and a skipped rerank
  is an SLO breach, not a degradation.
- **Delete is an O(1) soft marker; physical work is asynchronous.** `DeleteDocument` writes an intent
  object to blob storage (strongly consistent, shard-independent key), then one tombstone row in the
  shard, and acks. From the ack, every surface (Recall, Reflect, GetMemory, GetPage, ListMemories, export)
  hides the document, its chunks, and every observation or page version whose **evidence segment** names
  it: visibility is `NOT IN` three small marker sets plus two `EXISTS` over evidence rows, evaluated at
  read time, so there is nothing to walk, stamp or race at delete time. A throttled per-namespace
  **Expunge** workflow then materializes derived hiding (≤ 15 min), purges rows and blobs (≤ 24 h) and
  rebuilds touched indexes (≤ 48 h); while markers are pending the namespace runs in a documented degraded
  mode (extra per-candidate lookups, root rebuilds only, rerank-skip SLO suspended). `Invalidate`/`Restore`
  insert and delete one marker, so `Restore` is exact.
- **Durability without synchronous replication.** Every role commits with `synchronous_commit = local`, so a
  commit cannot hang on a standby. Acknowledged deletes and invalidations have RPO 0 because their intent
  object exists before the ack and a restore or failover replays the intents (`frozen/restore` until the
  replay finishes); retains have RPO ≤ 60 s.
- **Shard moves copy dirty, freeze, and reconcile by set difference.** Because large tables are immutable
  and the expunge is paused for the move, the difference after a dirty copy is exactly the rows inserted
  since the copy began (time-margin re-copy, counts and hashes); small mutable tables are diffed in full
  inside the freeze. There is no replay, no copy barrier and no catch-up loop. Cutover goes through a
  `ready` state with a single point of no return at the source's `moved_out`, and rollback exists at every
  step before it. Restore and failover first reconcile open moves against the catalog.
- **Time travel is exact for facts, observations and their evidence, and for chunks subject to a stated rule (N85).**
  Every fact and chunk carries `mentioned_at` = the item timestamp, set by the server; observations are
  versioned with `effective_at = max(mentioned_at over every fact rendered to the writer, effective_at of
  the previous version)` (D9); recall at `as_of = T` never returns anything derived from content the system
  learned after T. Consolidation is two-stage (a routing call returning decisions only, one write call per
  touched observation) so a version's evidence segment contains only its own sources.
- **Derived state is rebuildable and propagated through a transactional outbox** (index and optional Kafka
  consumers; moves no longer read it). Kafka is optional and off by default.
- **Correctness is checked, not asserted, and the plan says exactly what was checked.** Five TLA+
  specifications and four Lean 4 developments exist (§7); the D22 mechanisms (`Storage`, `Derivation`,
  `Durability`, the rewritten `ShardMove`) are the Phase 0 spec work and are not yet model-checked.
- **Size:** about **118 engineer-weeks** in total; the committed six-month scope for three engineers is
  Phases 0 to 2, **≈ 80 ew** against 78 ew of capacity (foundations and measurements, the MVP with moves
  behind an admin flag, the expunge and delete-intent log, consolidation, Reflect and `RetainBackfill`),
  so the Phase 2 exit lands in week 27; pages, export, multi-cell and most of the formal tooling are a
  separate 38-ew track. One shard holds ~10 M facts, one API + worker stack serves up to 32 shards, and
  1 B facts is 100 shards in 4 cells: **≈ 100 days to fill online** at 3.5 gateway calls per chunk and
  600 RPM per cell, ≈ $160 k at list prices (Table 6.8-B); `RetainBackfill` through the batch API is a
  launch prerequisite.
