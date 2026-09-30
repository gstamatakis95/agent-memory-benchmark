# Engram: implementation plan for a Go + Postgres agent-memory service

**Status:** design plan, v1 (2026-09-30). **Reference system:** Hindsight (github.com/vectorize-io/hindsight, MIT).
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
  namespace-scoped table, and every write transaction takes a `FOR SHARE` lock on the namespace's
  ownership row at the caller's epoch. That row is the fencing token that makes shard moves safe even when
  the catalog cache is stale.
- **Routing is by catalog.** Clients send `tenant_id` + `namespace_id` and a JWT; one interceptor verifies
  the token, resolves the namespace through a cached catalog (LISTEN/NOTIFY invalidation, 10-minute stale
  tolerance), checks ownership, and binds the request to a shard.
- **Retain is asynchronous and cheap to repeat.** Chunks are content-addressed, extraction results are cached
  in blob storage per (chunk hash, prompt version, model), and unchanged chunks cost nothing. Every activity is
  idempotent; commits are per chunk; visibility is per chunk; `WaitOperation` is the read barrier.
- **Recall makes no LLM calls.** Five arms (semantic, BM25, graph, temporal, raw chunks) run in parallel with
  `as_of`, tag and type filters applied inside each arm; RRF (k = 60), a cross-encoder rerank, bounded boosts
  and token-budget packing follow; results stream as soon as the last stage that fits the deadline completes.
- **Time travel is exact.** Facts carry `mentioned_at`; observations are versioned with
  `effective_at = max(mentioned_at of sources)`; recall at `as_of = T` never returns anything derived from
  content mentioned after T.
- **Derived state is rebuildable and propagated through a transactional outbox** that doubles as the
  change log for shard moves. Kafka is optional and off by default.
- **Correctness is checked, not asserted.** Five TLA+ specifications (document lifecycle, consolidation
  idempotency, outbox propagation, `as_of` isolation, shard moves) and four Lean 4 developments (tag modes, RRF,
  packing, temporal windows) are tied to Go property tests and trace validation.
- **Size:** about 74 engineer-weeks over four phases; one shard holds ~10 M facts, one API + worker stack
  serves up to 32 shards, and 1 B facts is 100 shards in 4 cells.
