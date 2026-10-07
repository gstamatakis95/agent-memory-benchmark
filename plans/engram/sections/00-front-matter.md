# Engram: implementation plan for a Go + Postgres agent-memory service

**Status:** design plan, v1.1 (2026-10-01, post adversarial review — `REVIEW.md`, register D20). **Reference system:** Hindsight (github.com/vectorize-io/hindsight, MIT).
**Scope:** everything needed to build, verify and operate a Hindsight-class long-term memory service in Go,
exposed as gRPC (`memory.v1`), with PostgreSQL 16 as the per-shard system of record, Temporal for
asynchronous work, an AI gateway for every model call and blob storage for large or immutable data.

This document is standalone. It shares a repository with an unrelated benchmark project and does not
change that project's constraints.

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
  instance with its own pgbouncer, blob prefix, Temporal task queue and metrics label. Nothing spans shards.
  Inside a shard, `namespace_id` leads every key and every index, row-level security is on for every
  namespace-scoped table, and every write transaction takes a **shared advisory lock** on the namespace
  and reads the ownership row at the caller's epoch (the row is the fence *value*, the advisory lock the
  fence *lock*, so a move's freeze queues fairly behind writers instead of starving). That row is the
  fencing token that makes shard moves safe even when the catalog cache is stale.
- **Routing is by catalog.** Clients send `tenant_id` + `namespace_id` and a JWT; one interceptor verifies
  the token, resolves the namespace through a cached catalog (LISTEN/NOTIFY invalidation; existing
  namespaces are served from cache for as long as a catalog outage lasts, because the shard-side fence,
  not the cache, decides writability; catalog failover is automatic), checks ownership, and binds the
  request to a shard.
- **Retain is asynchronous and cheap to repeat.** Chunks are content-addressed, extraction results are cached
  in blob storage per (chunk hash, prompt version, model), and unchanged chunks cost nothing. Every activity is
  idempotent; commits are per chunk; visibility is per chunk; `WaitOperation` is the read barrier.
- **Recall makes no LLM calls.** Five arms (semantic, BM25, graph, temporal, raw chunks) run with `as_of`,
  tag and type filters applied inside each arm (the semantic arm is an exact scan for small namespaces and
  a partial HNSW for large ones); RRF (k = 60, exact rational arithmetic), a cross-encoder rerank on 50
  pairs at the default budget, bounded boosts and token-budget packing follow; results stream as soon as
  the last stage that fits the deadline completes. The budget is a critical-path sum, 216 ms p95 at MID,
  and a skipped rerank is an SLO breach, not a degradation.
- **Time travel is exact.** Every fact and chunk carries `mentioned_at` **= the item timestamp, set by the
  server** (the time the system learned the content; the extractor's own date is `said_at`, informational);
  observations are versioned with `effective_at = max(mentioned_at over every fact rendered to the
  consolidation prompt, effective_at of the observations shown, effective_at of the previous version)`
  (D9); recall at `as_of = T` never returns anything derived from content the system learned after T.
  Deletion hides derived text **per version and permanently**: a version written with deleted content in
  the prompt is never served again at any `as_of`, while a version written without it stays servable.
- **Derived state is rebuildable and propagated through a transactional outbox** that doubles as the
  change log for shard moves. Kafka is optional and off by default.
- **Correctness is checked, not asserted — and the plan says exactly what was checked.** Five TLA+
  specifications (document lifecycle, consolidation idempotency, outbox propagation, `as_of` isolation,
  shard moves) and four Lean 4 developments (tag modes, RRF, packing, temporal windows) are tied to Go
  property tests and trace validation. The design configurations that completed are listed in §7.1:
  the `ShardMove` full-bound configuration passed; the `DocLifecycle` and `Consolidation` full-bound
  configurations remain incomplete and are reported as INCOMPLETE, not as passed; and the three modelling
  gaps the review found (per-version hiding, the previous version's inputs, replay without shard-wide
  knowledge) are open spec work items in §7.6.
- **Size:** about **112 engineer-weeks** in total; the committed six-month scope for three engineers is
  Phases 0–2, **≈ 74 ew** (foundations and a 3-week Phase 0′ of measurements, the MVP with shard moves
  behind an admin flag, consolidation/Reflect/backfill); pages, export, multi-cell and most of the formal
  tooling are a separate 38-ew track. One shard holds ~10 M facts, one API + worker stack serves up to
  32 shards, and 1 B facts is 100 shards in 4 cells — ≈ 29 days and ≈ $160 k to fill at list prices.
