# Engram: implementation plan for a Go + Postgres agent-memory service

**Status:** design plan, v1.2 (2026-10-07, after two adversarial reviews — `reviews/round-1.md` / register D20 and `reviews/round-2.md` / register D21). **Reference system:** Hindsight (github.com/vectorize-io/hindsight, MIT).
**Scope:** everything needed to build, verify and operate a Hindsight-class long-term memory service in Go,
exposed as gRPC (`memory.v1`), with PostgreSQL 16 as the per-shard system of record, Temporal for
asynchronous work, an AI gateway for every model call and blob storage for large or immutable data.

This document is standalone. It shares a repository with an unrelated benchmark project and does not
change that project's constraints.

**Second review (2026-10-07).** A second adversarial review (`reviews/round-2.md`, findings G-1 to G-30)
tested the plan as amended by the first. Register block D21 (N79 to N108) is binding and has been
applied to every section: transitive observation hiding through version lineage (N79), bounded
paged outbox events (N80), a total event-driven replay mapping for moves (N81), a try-lock
namespace fence that never holds a pooled connection (N82, N83), reversible per-version
invalidation (N84), versioned evidence and time-safe chunks under `as_of` (N85, N86), a render-aware
extraction cache key (N87), restartable range-snapshot copy without a `BYPASSRLS` loader
(N88 to N91), RPO 0 for acknowledged deletes through a synchronous standby (N92), permanent
`moved_out` fence values (N93) and a cutover whose point of no return is step (c) (N98). **The TLA+
specifications lag the design:** no D20 or D21 mechanism is in any `.tla` file, the `ShardMove`
PASS checks the pre-D20 protocol, and the gate configurations are unrun; the model-checked set
today is the outbox, consolidation exactly-once at 3 facts, `as_of` without deletes and the move
protocol as it was before D20/D21, until work items W-1 to W-12 land (§7.6; W-6 to W-12 are
preconditions of M1.5 and M2.1). The schedule absorbed the review: Phase 0 grows to ≈ 15 ew with a
3.5-ew Phase 0″, `RetainBackfill` leaves the committed scope, and the committed total is ≈ 77 ew
with about 1 ew of slack (§10).

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
  namespace-scoped table, and every write transaction tries a **shared advisory lock** on the namespace
  (`pg_try_advisory_xact_lock_shared`: a refusal is a retryable `NamespaceFrozen`, so a writer never
  holds a pooled connection while a freeze waits, N82) and reads the ownership row at the caller's epoch
  (the row is the fence *value*, the advisory lock the fence *lock*). That row is the
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
- **Time travel is exact for facts, observations and their evidence, and for chunks subject to a stated rule (N85).** Every fact and chunk carries `mentioned_at` **= the item timestamp, set by the
  server** (the time the system learned the content; the extractor's own date is `said_at`, informational);
  observations are versioned with `effective_at = max(mentioned_at over every fact rendered to the
  consolidation prompt, effective_at of the observations shown, effective_at of the previous version)`
  (D9); recall at `as_of = T` never returns anything derived from content the system learned after T.
  Deletion hides derived text **per version and permanently, and transitively through lineage**
  (N79): a version written with deleted content in the prompt, or derived from such a version, is never
  served again at any `as_of`, while a version written without it stays servable. A chunk is
  visible under `as_of = T` only if its embedded header was effective by T (`embedding_effective_at`), and
  chunk headers are not returned.
- **Derived state is rebuildable and propagated through a transactional outbox** that doubles as the
  change log for shard moves. Kafka is optional and off by default.
- **Correctness is checked, not asserted — and the plan says exactly what was checked (and, since the
  second review, what was not).** Five TLA+
  specifications (document lifecycle, consolidation idempotency, outbox propagation, `as_of` isolation,
  shard moves) and four Lean 4 developments (tag modes, RRF, packing, temporal windows) are tied to Go
  property tests and trace validation. The design configurations that completed are listed in §7.1:
  **model-checked today: the outbox; consolidation exactly-once at 3 facts; `as_of` without deletes; and
  the move protocol before D20/D21.** The `ShardMove` full-bound PASS checks that pre-D20 protocol; the
  `DocLifecycle` and `Consolidation` full-bound configurations remain incomplete and are reported as
  INCOMPLETE; no D20/D21 mechanism (lineage hiding, reversible invalidation, total replay, range-snapshot
  copy, try-lock fence, point of no return at (c)) is in any spec yet, the gate configurations are unrun,
  and the work items W-1 to W-12 of §7.6 close the gap.
- **Size:** about **118 engineer-weeks** in total; the committed six-month scope for three engineers is
  Phases 0–2, **≈ 77 ew** (foundations, a 3-week Phase 0′ of measurements and a 3.5-ew Phase 0″ that
  lands the second review's decisions, the MVP with shard moves behind an admin flag, consolidation and
  Reflect) against 78 ew of capacity, i.e. about 1 ew of slack; pages, export, multi-cell, `RetainBackfill`
  and most of the formal tooling are a separate 41-ew track. One shard holds ~10 M facts, one API + worker
  stack serves up to 32 shards, and 1 B facts is 100 shards in 4 cells — ≈ 29 days and ≈ $160 k to fill
  at list prices.
