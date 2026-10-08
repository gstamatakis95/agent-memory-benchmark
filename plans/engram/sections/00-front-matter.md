# Engram: implementation plan for a Go + Postgres agent-memory service

**Status:** design plan, v1.7 (2026-10-08, after eight adversarial reviews: `reviews/round-1.md` to `reviews/round-8.md`; register D20 (round 1), D21, D22 (N111 to N134), D23 (N135 to N143), D24 (N144 to N159), D25 (N160 to N168), D26 (round 7, N169 to N178), D27 (round 8, N179 to N188)). **Reference system:** Hindsight (github.com/vectorize-io/hindsight, MIT).
**Scope:** everything needed to build, verify and operate a Hindsight-class long-term memory service in Go,
exposed as gRPC (`memory.v1`), with PostgreSQL 16 as the per-shard system of record, Temporal for
asynchronous work, an AI gateway for every model call and blob storage for large or immutable data.

This document is standalone. It shares a repository with an unrelated benchmark project and does not
change that project's constraints.

**Reviews and redesigns.** Rounds 1 and 2 were patched. Round 3 found the same areas regressing
twice (storage updates, delete visibility, delete durability, shard moves) and produced a redesign
(advice R1 to R5; D22): **rows that carry vectors or BM25 text are immutable, mutable state lives in
narrow side tables, and visibility is a read-time predicate over small marker sets; deletes are
rare, so a delete is an O(1) soft marker honoured by every read path at ack and all physical work
is an asynchronous, throttled expunge during which SLOs may degrade.** Round 4 kept that core and
changed only the mechanisms that acted outside the predicate's assumptions (D23): evidence outlives
the facts it names and re-extraction is a write; one commit rule for every writer of a derived
version; hard delete reaches derived artefacts; intents are put after the marker commits and the
replay floor lives in the catalog; moves re-copy by an insertion sequence and are arbitrated by a
catalog CAS; and the shard was sized from a measured hot set. Round 5 found no blocker (D24). Round 6 (D25, N160 to N168) replaced the shard-move protocol, which had produced a blocker or major in three rounds, with **freeze-then-copy**: the namespace is read-only for a bounded, size-proportional window (unattended only when the
estimate is ≤ 10 min, operator-scheduled windows up to 8 h), every row is copied from a static source, and verification is
equality on a static set. The rest of D25: the curation subject is `(document_id, content_hash)`; the catalog has **one asynchronous hot standby** and reconciles from the shards on every promotion and restore; filtered recalls are their own class (θ = 10 k, p95 ≤ 1 s); the shard is **5.5 M facts target / 10 M cap** on the same 128 GB instance (182 shards, 6 cells
for 1 B facts). Round 7 (D26, N169 to N178) removed recovery paths around that core (the re-run is deleted, the cleanup gate is a schema fact at the entry to `cleaning`, one replicated-ack helper serves every catalog outcome a shard or a client acts on). Round 8 (D27, N179 to N188) found every major in the paths round 7 added around the target backup, so **it is replaced by a WAL floor**: the copy is sealed when the target's WAL is archived and its standby has replayed through `copy_end_lsn`, and that LSN is the floor of every target restore and failover (N179); a restore that activates a target closes the move (N180). The six TLA+ specifications are **written and model-checked** with their must-fail configurations; §7.1 states what each omits, and Phase 0 spec work (M0.7) is the must-fail manifest, invariant code and trace converter.
The committed scope is now **≈ 86.25 ew against 78 ew of capacity** (8.25 ew over, MVP in week 24,
Phase 2 exit in week 30, stated in §10).

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
  token-budget packing follow. The budget is a critical-path sum, 236 ms p95 at MID, and a skipped rerank
  is an SLO breach, not a degradation.
- **Delete is an O(1) soft marker; physical work is asynchronous.** `DeleteDocument` commits one
  tombstone transaction in the shard, puts an intent object to blob storage (strongly consistent,
  shard-independent key) and acks. From the ack, every surface (Recall, Reflect, GetMemory,
  GetPage, GetDocument, ListMemories, export) hides the document, its chunks, and every observation
  or page version whose **evidence segment** names it: visibility is `NOT IN` two small marker sets
  (`doc_tomb`, `chunk_tomb`), a primary-key anti-join for invalidations and two `EXISTS` over evidence
  rows, evaluated at read time, so there is nothing to walk, stamp or race at delete time; evidence
  rows outlive the facts they name, so a later delete still finds every derived version. A throttled
  per-namespace **Expunge** workflow then materializes derived hiding (≤ 15 min), purges rows and
  blobs at ≤ 25 MB/s of WAL (≤ 24 h), reduces derived versions to content-free stubs and rebuilds touched
  indexes (≤ 48 h); while markers are pending the namespace runs in a documented degraded mode.
  `Invalidate`/`Restore` insert and delete one marker, so `Restore` is exact.
- **Durability without synchronous replication of the shards.** Every shard role commits with
  `synchronous_commit = local`, so a commit cannot hang on a standby; the small, off-hot-path catalog has one
  asynchronous hot standby, every client-acknowledged catalog write waits for the standby's replay (N171), and every promotion or restore first reconciles it from the shards (N163). **Acknowledged** deletes and invalidations have RPO 0: the
  intent object is put after the marker commits and before the ack, and a restore or failover replays
  the intents verbatim with their recorded epochs, per subject in chain order, from a floor kept in the
  catalog and the blob-side `_control/restores/` objects (`frozen/restore` until the replay finishes); a committed-but-unacknowledged delete may be lost
  and the client's retry re-applies it. Retains have RPO ≤ 60 s.
- **Shard moves are freeze-then-copy.** The namespace is frozen (reads continue, writes get a retryable
  `NamespaceFrozen` with an estimate) for a window sized from its bytes and vectors; every table is copied
  from the static source, verified by count and key-hash equality, the target's indexes are built, and
  cutover goes through a `ready` state with a single point of no return, the **catalog CAS
  `cutover → committed`** (checked as replicated before the source is fenced). Rollback exists at every
  step before it and at the window deadline; the copy is sealed (WAL archived and standby-replayed through `copy_end_lsn`) before the commit point, and that LSN is the floor of any target restore; cleanup waits 24 h after activation.
- **Time travel is exact for facts, observations and their evidence, and for chunks subject to a stated rule (N85).**
  Every fact and chunk carries `mentioned_at` = the item timestamp, set by the server; observations are
  versioned with `effective_at = max(mentioned_at over every fact rendered to the writer, effective_at of
  the previous version)` (D9); recall at `as_of = T` never returns anything derived from content the system
  learned after T. Consolidation is two-stage (a routing call returning decisions only, one write call per
  touched observation) so a version's evidence segment contains only its own sources.
- **Derived state is rebuildable and propagated through a transactional outbox** (index and optional Kafka
  consumers; moves no longer read it). Kafka is optional and off by default.
- **Correctness is checked, not asserted, and the plan says exactly what was checked.** Six TLA+
  specifications are written and model-checked, each with must-fail configurations whose logs are in
  `formal/tla/results/`; four Lean 4 developments exist but are not type-checked here (§7). §7.1 lists the
  prose mechanisms each spec omits.
- **Size:** about **125.5 engineer-weeks** in total; the committed six-month scope for three engineers is
  Phases 0 to 2, **≈ 86.25 ew** against 78 ew of capacity (conformance and measurements, the MVP with
  moves behind an admin flag, the expunge and delete-intent log, consolidation, Reflect and
  `RetainBackfill`), so the MVP lands in week 24 and the Phase 2 exit in week 30; pages, export,
  multi-cell and most of the formal tooling are a separate 39.25-ew track. One shard holds **5.5 M facts**
  (10 M hard cap, 600 GB, ≈ 138 GB at the target and ≈ 250 GB at the cap, NVMe ≥ 50 k IOPS), one API + worker stack serves up to 32 shards, and 1 B
  facts is 182 shards in 6 cells: **≈ 67 days to fill online** at 3.5 gateway calls per chunk and
  600 RPM per cell, ≈ $160 k at list prices (Table 6.8-B); `RetainBackfill` through the batch API is a
  launch prerequisite.
