## 11. Risks, open questions, and rejected alternatives

Each row carries one line of reasoning. Risks are scored likelihood × impact on a 1–3 scale
(L×I); mitigations name the mechanism or the test that bounds the risk. Owners are the §10.1
engineers; "by-when" is a §10 milestone. The three tables are reviewed on a fixed cadence:
risks at every phase exit (a risk whose early-warning signal has fired twice is promoted to
a milestone), open questions at the weekly ops meeting (an unanswered question past its
by-when blocks the milestone it names), rejected alternatives only when a register row
changes (D18 is the only way back in).

### 11.1 Risks

| # | Risk | L×I | Reasoning | Mitigation |
|---|---|---|---|---|
| R1 | **pg_search / pgvector coupling in one image.** A ParadeDB image bump changes BM25 scoring or breaks the pgvector version we tuned for. | 2×3 | Two extensions, one upstream release train, and D7 makes BM25 scores part of recall quality. | Pin the image by digest (§9.1); `TsvectorIndex` fallback behind the `index.Index` interface (D7); the bench ablation gate (`hybrid − bm25 ≈ +9 pp R@5`) runs on every image bump before rollout. |
| R2 | **HNSW build/rebuild time at 10 M facts per shard.** ≈ 18 GB of HNSW (D3) takes hours to rebuild after corruption or an opclass change. | 2×2 | HNSW builds are CPU-bound and single-pass; a partition-wide `REINDEX` blocks nothing but takes the shard's CPU. | 16 partitions → 16 smaller indexes built in parallel (`max_parallel_maintenance_workers`); `REINDEX CONCURRENTLY` per partition; capacity soft cap at 10 M; restore drills measure the rebuild. |
| R3 | **Gateway rerank latency blows the 300 ms budget** (A-2 assumes ≤ 120 ms p95 for 150 pairs). | 3×2 | The reranker is the largest single term in D3's latency budget and is outside our control. | Deadline-aware skip (`stage=FUSED`, D10); `rerank_top` per budget is a config knob; weekly p95 tracking (§8.8); if p95 > 150 ms at mid for two weeks, lower `rerank_top.mid` to 75 before considering an in-cell reranker (a rejected alternative, below). |
| R4 | **Gateway RPM caps throttle retain** below the 10 chunks/s cell-wide target. | 3×2 | D3's throughput formula is `min(32/L, RPM/60)`; a 600 RPM cap already dominates. | Client-side `RateLimiter` per model (§2.2.5) so we queue instead of 429-loop; backfills and bench ingestion through `SubmitBatch` (no RPM cap, ~50 % cheaper); quota deferral keeps operations alive; `GatewayRateLimited` alert and runbook (§9.6). |
| R5 | **A move never converges in `catching_up`** for a namespace with a sustained write rate above the replay rate. | 2×3 | D5's freeze bound (30 s) is only safe if lag is < 100 events at freeze time; a hot namespace is exactly the one we want to move. | Lag thresholds before freeze (D5 step 3); `engramctl move throttle` (temporary namespace quota, §9.6); rollback is always available before `cutover`; T5 test with 50 writes/s. |
| R6 | **Missed catalog notifications** leave a cell serving a stale shard/epoch for up to 60 s (TTL). | 2×1 | `LISTEN` is at-most-once per connection; a dropped connection drops notifications. | Full cache flush on reconnect (§1.3); the shard-side ownership fence makes staleness a retry, never a wrong write (D2); `WrongShardOrEpochSpike` alert; the stale-cache fault test (§8.4). |
| R7 | **RLS and `current_setting()` defeat partition pruning or add per-row overhead** on the hot read path. | 2×2 | The planner cannot prune hash partitions on a `current_setting()` expression unless it is folded to a constant at plan time; policies add a quals pass. | Every query carries the explicit `namespace_id = $1` predicate as well (`engramlint sql`, ND-6) so pruning works from the parameter; the policy uses a `STABLE` cast so it is evaluated once per statement; M1.2's p95 gate measures it on a 10 M-fact shard. |
| R8 | **pgbouncer transaction pooling breaks session features** (advisory locks, `LISTEN`, prepared statements, `SET`). | 3×2 | D2 mandates transaction pooling; three subsystems need session state. | Relay election on a direct connection (ND-1); `LISTEN` only against the catalog (no pgbouncer there); `SET LOCAL` only (D2); pgx protocol-level prepared statements with pgbouncer `max_prepared_statements` (§9.1); a T3 test runs the whole store suite through a real pgbouncer container. |
| R9 | **Extraction quality drifts** across model versions or prompt edits and silently lowers accuracy. | 3×2 | Extraction is one LLM call per chunk; the gateway can swap model builds under the same id. | Prompt versions in cache keys and goldens (`RecordReplayClient`); `bench.lock` records the gateway-reported model version and refuses to run on drift (ND-4); weekly bench with a 1 pp regression ticket (§8.8). |
| R10 | **LLM spend overrun** from backfills, consolidation storms or a runaway client. | 2×3 | Consolidation alone is ≈ 40 % of ingest cost (§8.6); a client re-ingesting changing content defeats the cache. | Per-namespace extraction cache (D11); quotas with deferral, not failure (D13); 80 % budget alert; `token_usage` with `price_version` (ND-7); the cost dashboard and weekly report. |
| R11 | **Deleted data persists in backups** for the retention window. | 3×2 | A backup is a copy; nothing short of re-encryption can remove a row from it. | Documented 28-day deletion SLA (ND-11); deletion replay on restore (ND-8) so restored shards never *serve* deleted data; crypto-shredding is an open question (Q6). |
| R12 | **Temporal history growth** for large documents (thousands of chunks) or long consolidation rounds. | 2×2 | Each activity adds events; Temporal's 50 k-event / 50 MB limits are hard. | `ContinueAsNew` at 500 chunks (§2.2.17); thin payloads (N12, N13: embeddings spill to blob above 512 KiB); heartbeats instead of many small activities. |
| R13 | **Noisy neighbour inside a shared shard.** One tenant's recall or retain saturates the shard's CPU/IOPS for the other ≈ 149 namespaces. | 3×2 | Shards are shared by tenants by design (D2) to make 10 000 tenants affordable. | Per-request `statement_timeout` (5 s read / 30 s write); per-tenant and per-namespace rate quotas in the interceptor; `namespace_activity` + hot-namespace playbook + moves (§9.5); `isolation=dedicated` for tenants who pay for it. |
| R14 | **Rolling restarts through Envoy STRICT_DNS** drop requests when Docker DNS lags behind container churn. | 2×1 | `respect_dns_ttl: false` with a 5 s refresh can still race a replica that just exited. | gRPC health checks (2 failures → out), outlier detection, graceful `NOT_SERVING` drain for 35 s before exit (§9.1); retries on `connect-failure,refused-stream` for reads only (ND-12). |
| R15 | **Judge/model drift on the gateway invalidates benchmark trends.** | 2×2 | The same model id can be re-served from a new build; accuracy deltas of 1–2 pp are within that noise. | `bench.lock` with model version pins; re-baselining when a pin changes; judged outputs kept for re-grading (ND-4); 3 repeats + paired bootstrap. |
| R16 | **Restore RTO for a 300 GB shard exceeds 60 min** on slower blob or disk throughput than A-O10. | 2×2 | RTO scales linearly with volume size and inversely with restore throughput. | Quarterly drills measure the real number; if > 60 min, lower the shard volume target (more, smaller shards) rather than accept it; `process-max=8` and zstd. |
| R17 | **nomic prefix or L2-normalisation mistakes** degrade dense recall silently. | 2×3 | The failure mode is "slightly worse", which no unit test catches. | `gateway.Client.Embed` normalises and prefixes in one place with a unit test; the ablation gate (`hybrid − bm25 ≈ +9 pp R@5`) is a required bench-smoke assertion (§8.6, A-B3). |
| R18 | **Two shard handles in one process during a move** (N2: the mover holds target + source pools). | 1×3 | It is the one exception to "a process touches a namespace on one shard"; a bug there is a cross-shard write. | `engram_move` role is read-only on the source except the ownership row (`FOR UPDATE`) and the move cursor; `RecordingPool` isolation tests for the move consumer (§8.3); `Move.tla` `OneWritableOwner`. |
| R19 | **Strict deadline caps (N11) reject existing client defaults** (e.g. a 60 s default on Recall). | 2×1 | `INVALID_ARGUMENT` instead of clamping is correct but surprising. | Caps documented per method in §4 and returned in the `ValidationError` detail; MCP and Connect adapters set the capped deadline themselves. |
| R20 | **Docker DNS + `deploy.replicas` is the only service discovery.** A cell that outgrows one host needs an overlay network (A-O1) that Compose does not manage. | 2×2 | Everything in §9.1 assumes `engram-api` resolves to every replica; across hosts that is a network decision nobody has made. | Q5 decides it in M1.7; until then a cell is one host and the 32-shard ceiling is a capacity statement, not a tested one; the Envoy cluster can switch to a static endpoint list without code changes. |
| R21 | **Formal specs and code drift apart.** A TLA+ spec that models yesterday's protocol proves nothing about today's code. | 2×2 | Trace validation covers only the paths the recorded runs exercised. | Nightly trace validation over the T5 chaos runs (every move phase, relay election, delete-vs-retain); the property tests restate the same invariants (§8.2); a register change to D5/D6 requires a spec change in the same PR. |
| R22 | **Temporal `auto-setup` in the MVP cell is not the production topology** (A-O5). | 2×1 | Split services and history shard counts cannot be changed after the namespace holds history. | Decide the production Temporal layout before Phase 1 exit; the `engram` namespace retention (7 days) and `shard-{id}` queues are the same in both. |
| R23 | **16 hash partitions per shard is fixed at provisioning.** A shard that needs 32 cannot be repartitioned in place. | 1×2 | Repartitioning a 100 GB table under RLS and HNSW is a multi-hour rebuild. | §9.2: never in place — provision a new shard with the new count and move namespaces; the count is a `shard_meta` field so mixed fleets are allowed. |
| R24 | **`as_of` depends on client-supplied timestamps.** A client that omits `timestamp` gets `mentioned_at = ingest time`, which silently breaks time-travel evaluation. | 2×2 | D9's semantics are only as good as the inputs. | `Retain` returns a `ValidationError` warning detail when `timestamp` is absent and `as_of` is used later on that namespace (observable in `RecallStats.untimed_items`); the bench harness refuses untimed items; documented loudly in §4. |

**Early-warning signals.** Every risk above is bound to one observable, so "did it
happen?" is a dashboard question, not an opinion. A signal that fires twice in a quarter
promotes the risk to a milestone in the next phase.

| Risk | Signal (metric, alert or test) | Threshold that promotes it |
|---|---|---|
| R1 | bench-smoke ablation gate `hybrid − bm25` | < +6 pp R@5 after an image bump |
| R2 | `engramctl backup drill` HNSW rebuild time; `pg_stat_progress_create_index` | > 2 h for one partition set |
| R3 | `engram_recall_stage_seconds{stage="rerank"}` p95; `engram_recall_rerank_skipped_total{reason="deadline"}` | p95 > 150 ms at mid, or skip rate > 5 % |
| R4 | `engram_gateway_ratelimited_seconds_total` rate; `engram_retain_throughput_chunks` | waiting > 50 % of wall time, or < 5 chunks/s/worker |
| R5 | `MoveStuck`; `engram_move_lag_events` slope | two stuck moves in a quarter |
| R6 | `WrongShardOrEpochSpike`; `engram_catalog_resolve_total{result="stale"}` | > 1/s for 5 min |
| R7 | `pg_stat_statements` mean time of the semantic arm; `EXPLAIN` shows no partition pruning in the T3 plan test | plan test fails, or arm p95 > 60 ms |
| R8 | T3 suite through a pgbouncer container; `pg_locks` advisory-lock holders | any session-state failure in CI |
| R9 | `bench.lock` drift banner; weekly accuracy delta | > 1 pp regression without a change note |
| R10 | `engram_llm_cost_micros_total` rate vs budget; `engram_xcache_total{result="miss"}` share | > 80 % of the monthly budget by day 20, or cache hit rate < 50 % on re-ingest |
| R11 | tenant deletion requests citing backups | one request we cannot satisfy within the SLA |
| R12 | Temporal history size per workflow (SDK metric `temporal_workflow_task_execution_total`, history length) | > 25 k events in any workflow |
| R13 | `PoolSaturation`; `engramctl hot` top namespace share | one namespace > 40 % of a shard's load for a week |
| R14 | Envoy `upstream_rq_pending_overflow`, `upstream_cx_connect_fail` during a rolling restart | any non-zero count during a deploy |
| R15 | `bench.lock` drift banner frequency | more than one pin change per month |
| R16 | drill RTO measured quarterly | > 60 min |
| R17 | bench-smoke ablation gate; unit test on prefix presence | gate fails |
| R18 | `RecordingPool` isolation tests for the mover; `engram_move_rejected_events_total` | any rejected event in production |
| R19 | `engram_rpc_requests_total{code="INVALID_ARGUMENT"}` with reason `DEADLINE_TOO_LONG` | > 1 % of a tenant's calls |
| R20 | first cell to exceed one host | the day it happens |
| R21 | nightly trace validation result; spec/PR co-change lint (`formal/` untouched while `internal/{move,outbox}` protocol code changed) | one red night, or one lint failure |
| R22 | Temporal namespace history size; `auto-setup` still in the production compose at Phase 1 exit | either |
| R23 | `engram_shard_live_facts` growth rate vs partition count | a shard forecast to hit 20 M facts within 90 days |
| R24 | `RecallStats.untimed_items` share per namespace | > 1 % of items untimed in a namespace that uses `as_of` |

**Interactions.** R3 and R4 compound (a slow reranker *and* a capped gateway make recall
skip rerank more often), so they share one dashboard panel; R5 and R13 are the same
namespace seen from two sides (the hot namespace is the one that is hard to move), which is
why the move throttle exists; R11 and Q6/Q10 decide together whether "delete" means the
same thing in Postgres, backups and the ledger. Residual risk accepted without mitigation:
the gateway itself is a single dependency for every LLM-bound path (R3/R4/R9/R15 are all
facets of it); the task fixes it as given infrastructure, and §1.8 states the degraded
modes rather than pretending a second provider exists.

### 11.2 Open questions

| # | Question | Why it matters | Owner | By when |
|---|---|---|---|---|
| Q1 | What is the gateway rerank endpoint's real latency for 150 and 300 pairs (A-2), and is it TEI-compatible (for the Hindsight H-matched run)? | Sets `rerank_top` per budget and whether Hindsight can share the reranker. | E3 | M0.4 (measure in Phase 0 with a stub corpus) |
| Q2 | Does the blob store issue prefix-scoped credentials (A-4), or do we need one bucket per shard? | Changes `blob.Scoped` enforcement from "client + server" to "client + bucket policy". | E1 | M0.2 |
| Q3 | Does the gateway serve `gpt-oss-120b` (A-B1/A-B2)? If not, which pinned open-weight model is the judge and the extraction model for the comparable run? | Comparability with Hindsight's published numbers. | E3 | B.1 (before the first LME-S run) |
| Q4 | Hindsight image name/tag and whether its migrations take the embedding dimension (768) from config at first run (A-B5/A-B6). | The H-matched configuration may need a schema override. | E3 | B.2 |
| Q5 | How does a cell span hosts (A-O1): overlay network, or one host per cell with 32 shards on NVMe? | Determines whether `deploy.resources` limits are real isolation or advisory. | E1 + ops | M1.7 |
| Q6 | Crypto-shredding of backups per tenant (per-tenant data keys), or accept the 28-day deletion SLA? | Compliance requirement for some tenants; a per-tenant key changes the schema (encrypted columns) or the backup design. | product + E1 | Phase 3 exit |
| Q7 | Export deltas when two snapshots are more than 7 days apart (outbox trimmed, D6): full diff fallback, or longer outbox retention for namespaces with exports enabled? | D12 derives deltas from the outbox range; D6 trims at 7 days — the two conflict for weekly-or-slower exporters. | E2 | M3.2 design |
| Q8 | LME-M budget (≈ $1,000/run) and cadence: monthly, or release candidates only? | Cost vs. the value of the only 1.5 M-token-per-haystack signal we have. | eng manager | Phase 2 exit |
| Q9 | pgx exec mode behind pgbouncer: protocol-level prepared statements (needs pgbouncer ≥ 1.21 and `max_prepared_statements`) or simple protocol? | Affects per-query latency by ~0.2 ms and the pgbouncer version floor. | E1 | M0.2 |
| Q10 | Legal retention of `ingest_ledger` raw bodies: forever (append-only), or purged with the document's delete? | The ledger is append-only by design, but a deleted document's raw text in the ledger is "deleted data". | product + E1 | M1.3 |
| Q11 | Should `namespace_activity` (ND-9) be exposed to tenants through `NamespaceService` (usage API), or stay operator-only? | A tenant-visible usage API is a product feature with its own quota semantics. | product + E3 | Phase 3 |
| Q12 | When Kafka is enabled, who owns the BSR account and the consumer-side schema compatibility policy? | D6 publishes `engram.internal.events.v1.Event` to the BSR; external consumers depend on its evolution rules. | platform | M3.5 |
| Q13 | Where does TLS terminate (A-O3): on Envoy with certificates per cell, or on a front proxy the platform owns? | Decides whether `engram-api` ever sees plaintext from outside the cell and who rotates certificates. | ops + E1 | M1.7 |
| Q14 | Production Temporal layout (A-O5): split frontend/history/matching/worker services and the history shard count for the `engram` namespace. | Cannot be changed after history accumulates; affects worker poller counts (32 queues × 2). | E2 + ops | Phase 1 exit |

**How an open question gets closed.** The owner writes the answer as a register row (D18,
one line, with the rejected option) or as an assumption tag (A-n) in the section that
depends on it; a question whose answer changes an interface in §2 also changes that
section's table in the same PR. Questions are never closed in chat.

### 11.3 Rejected alternatives

| # | Alternative | Rejected in favour of | Reasoning |
|---|---|---|---|
| A1 | Schema-per-shard in one Postgres cluster | one Postgres instance per shard (D2) | Shared buffer pool and WAL mean one hot tenant degrades all; backups and migrations cannot be per shard. |
| A2 | Kafka as the ingest queue | ledger row + Temporal workflow (D6, D16) | A second durable store in the write path; Temporal already gives durable orchestration and Postgres the ordered log. |
| A3 | grpc-gateway for REST/JSON | ConnectRPC handlers generated from the same proto on the same port (§4) | No second process, no HTTP annotations to maintain; error details are byte-identical across transports. |
| A4 | pgvector `vector(768)` full precision | `halfvec(768)` (D3) | Half the storage and index size at negligible recall loss for 768-d nomic vectors. |
| A5 | Per-namespace tables or partitions | 16 hash partitions by `namespace_id` + RLS (D2) | Thousands of relations per shard, DDL on namespace creation, planner and catalog bloat. |
| A6 | An LLM in recall (query rewriting, LLM rerank) | cross-encoder rerank through the gateway, rule-based temporal parsing (D10) | Recall must make no LLM calls (task) and fit 300 ms; a cross-encoder is a model, not a generator. |
| A7 | External search engine first | Postgres HNSW + pg_search on the shard (D7) | One transactional system that is rebuildable with `REINDEX`; the `index.Index` interface keeps the door open. |
| A8 | etcd/Consul as the catalog | a small Postgres `engram_catalog` with a replica (D4) | Transactions across tenant config, moves and epochs; one operational skill set. |
| A9 | Lease-based (timestamp) fencing for moves | per-namespace epoch + `FOR SHARE` ownership row on the shard (D1/D2/D5) | Leases depend on clocks; the ownership row is a fence the database enforces inside the write transaction. |
| A10 | Client-side batching of embeddings in the hot path | unary `Embed` on the hot path, `EmbedBatch`/`SubmitBatch` for backfills (D3, §2.2.5) | The gateway's batch API is where the discount and the RPM exemption are; hot-path batching adds latency without the discount. |
| A11 | A cross-namespace (global) extraction cache | per-namespace cache under `{shard}/{tenant}/{ns}/xcache/` (D11) | A shared cache lets one tenant probe another's content through hit/miss timing. |
| A12 | Redis for the catalog cache | in-process LRU + `LISTEN` (D4) | Another system; the shard-side fence already makes a stale in-process cache safe. |
| A13 | Logical replication / Debezium CDC for change propagation | transactional outbox per shard (D6) | Per-namespace filtering of a replication slot is awkward; the outbox doubles as the move log. |
| A14 | One binary with run modes | four binaries from one image (D1) | The task requires separate API and worker images; separate processes have separate resource limits and restart policies. |
| A15 | `bigserial` ids | UUIDv7 (D1) | Serial ids collide across shards during a move; UUIDv7 keeps index locality. |
| A16 | Fail-closed on any catalog staleness | serve cached entries up to 10 min, fail only on a miss (D4) | A catalog blip would otherwise be a full outage although the shards can fence by themselves. |
| A17 | Kubernetes / Helm | Docker Compose per cell (task, §9) | Explicitly out of scope; Compose plus Envoy gives per-request balancing without a scheduler. |
| A18 | An in-process cross-encoder (Hindsight's ms-marco MiniLM) | gateway rerank (`bge-reranker-v2-m3`, D15) | Keeps the binaries model-free and CPU-only; one place to change the reranker. |
| A19 | `tsvector` + `ts_rank_cd` as the lexical arm | pg_search BM25 (D7) | `ts_rank` is not BM25 (no length normalisation, no IDF saturation); kept as a lower-quality fallback. |
| A20 | Truncating an item that does not fit the token budget | skip and count it (D10, `Engram/Packing.lean`) | A truncated fact is a different fact; the skip rule is provable and the count is observable. |
| A21 | Re-verifying the JWT on every stream message | scope fixed at stream open (N8) | Token expiry mid-Reflect would turn a 300 s answer into a partial one. |
| A22 | `PERMISSION_DENIED` for a namespace of another tenant | `NOT_FOUND` (N5) | An error that distinguishes "exists but not yours" from "does not exist" is an enumeration oracle. |
| A23 | Running the move workflow on the source shard's queue | target queue with an `engram_move` pool to the source (N2) | The target is the shard that must end up consistent; the source may be the one under pressure. |
| A24 | Weakening the retain ack to "ledger row only" when Temporal is down | `UNAVAILABLE` at ack, `op-sweeper` as a safety net only (N3) | An ack without a workflow would silently extend the visibility lag; the sweeper catches crashes between the two writes, not outages. |
| A25 | Fat outbox events carrying text and vectors | thin events; consumers read rows by id at the recorded epoch (N12) | Keeps the outbox small enough to trim and replay; the row at the epoch is the truth anyway. |
| A26 | Always staging embeddings in blob | inline little-endian float32 in Temporal payloads, spill above 512 KiB (N13) | One blob round-trip per chunk would double activity latency for the common small case. |
| A27 | Adding `PageService`/`ExportService` to the proto when phase 3 starts | registered from day one answering `UNIMPLEMENTED` (N14) | Adapters, MCP tool lists and generated clients never change shape. |
| A28 | Retrying every gRPC method in Envoy | retries only for idempotent reads on transport failures (ND-12) | A proxy cannot know a write's idempotency key; duplicates would be the proxy's fault. |
| A29 | `pg_basebackup` + hand-written WAL archiving | pgBackRest (ND-11) | Incremental backups, parallel restore, encryption and retention are solved problems there. |
| A30 | Closed, frontier-class judge for the benchmark | `gpt-oss-120b` at temperature 0 through the gateway (ND-4) | Comparable to Hindsight's paper and pinnable; a closed judge drifts without notice. |

**Seams kept for rejected alternatives.** Rejecting an alternative is not the same as
making it impossible; where a swap is plausible later, the §2 interface that would absorb
it is named here so that nobody re-architects to get it.

| Alternative | Seam that would absorb it | What would have to be true first |
|---|---|---|
| A7 external search engine | `index.Index` (`Async` implementation fed by the outbox `index` consumer, D7) | a shard's HNSW/BM25 no longer fits the D3 budget at 20 M facts, measured, not predicted |
| A13 CDC instead of the outbox | the outbox consumer interface (`outbox.Sink`) | never for the move log; only as an additional sink |
| A18 in-cell reranker | `recall.Reranker` (`GatewayReranker` → an HTTP sidecar implementation) | gateway rerank p95 > 150 ms for a quarter and a CPU reranker shown to be faster on the §8.6 corpus |
| A19 `tsvector` lexical arm | `index.Index` (`TsvectorIndex`, already implemented as the fallback) | pg_search unavailable on a target platform |
| A12 Redis catalog cache | `catalog.Resolver` | more than one API process per host needing a shared negative cache — not a problem a cell of two replicas has |
| A2 Kafka ingest | none — rejected for the write path; the Kafka *sink* (D6) is the supported shape | — |
| A6 LLM in recall | none — would violate the task's "no LLM calls" rule | — |
| A21 per-message JWT re-verification | `authz` stream interceptor | a product requirement for revocation mid-stream, which would also require a revocation list the IdP does not expose today |
