## 12. Explicit non-goals

What Engram deliberately does not do in this plan, with the reason and, where one exists,
the thing that covers the need instead. "Not now" items may become a phase 4; "not ever"
items contradict a register decision.

| # | Non-goal | Status | Reason / what covers it instead |
|---|---|---|---|
| NG1 | A web UI or control-plane console (Hindsight ships one) | not now | `engramctl` + Grafana cover operators; a UI is a separate product with its own auth surface. |
| NG2 | Kubernetes, Helm charts, Swarm | not ever | The task mandates Compose; cells scale by adding shards and cells, not pods (§9.1). |
| NG3 | Supporting databases other than PostgreSQL 16+ (Oracle, MySQL, SQLite, embedded `pg0`) | not ever | The store is written against pgvector, pg_search, RLS and hash partitioning; abstraction over them would cost the guarantees of D2 and D7. |
| NG4 | Multiple LLM providers or direct provider SDKs | not ever | The gateway is the only model endpoint (task); provider choice is a per-request model id (D15). |
| NG5 | In-process embedding or reranking models (ONNX, llama.cpp, local cross-encoders) | not ever | Binaries stay model-free and CPU-only; `models.embed`/`models.rerank` go through the gateway (A18). |
| NG6 | File conversion at ingest (PDF, DOCX, OCR, audio transcription, image VLM) | not now | Clients submit text/markdown; a converter would be a separate service in front of `Retain`. |
| NG7 | Memory-defense PII/secret scrubbing with a regex pattern set | not now | A pre-retain hook point exists in the pipeline (§5.1); shipping 45 regexes without a policy owner is false comfort. |
| NG8 | Read replicas on the recall path | not ever | D16's read barrier (`WaitOperation` then `Recall` sees every fact) relies on primary reads; replicas would reintroduce staleness we chose to exclude. |
| NG9 | Cross-namespace or cross-shard search, federation, "search all my namespaces" | not ever | D2: no query spans shards; a client that wants it fans out namespace by namespace. |
| NG10 | Namespace aliases (Hindsight bank aliases) | not now | Namespaces have a tenant-unique `name` already (D1); aliases complicate the catalog and the allowlist semantics. |
| NG11 | `tag_groups` boolean tag expressions | not now | The five `tag_match` modes are proven in Lean (D10); boolean groups would need a new decision procedure and a new proof. |
| NG12 | Webhooks | not now | Operation state is pollable and `WaitOperation` is the read barrier; external fan-out is the optional Kafka sink (D6). |
| NG13 | Billing, invoicing, price negotiation | not ever | Engram meters tokens and cost per namespace (D13, ND-7); money is the control plane's business. |
| NG14 | Identity provider, user management, API-key issuance | not ever | JWTs from an external IdP via JWKS (D13); Engram verifies, it does not mint. |
| NG15 | Multi-region or active-active deployment, cross-cell replication | not now | A cell is one failure domain; disaster recovery is per-shard restore (§9.3), not replication across cells. |
| NG16 | Autonomous rebalancing (moves triggered by an algorithm without an operator) | not now | Moves are operator-initiated from the playbook (§9.5); the capacity signals and `shard suggest` exist so a later autopilot has inputs. |
| NG17 | A GraphQL or bespoke REST API beyond Connect's generated routes | not ever | The proto is the contract (§4); Connect is the only JSON surface. |
| NG18 | Hand-written client SDKs | not now | Generated gRPC/Connect stubs in Go, TypeScript and Python are the SDKs; a convenience wrapper can come later. |
| NG19 | Mounting pages as a filesystem (`hindsight fs mount`) | not ever | `ExportService` + the thin `engram-sync` client project snapshots to disk for grep-style search (D12). |
| NG20 | Citus or any distributed-Postgres layer | not ever | Shards are independent instances by design; Citus would recentralise the planner and the failure domain. |
| NG21 | An "opinion" fact type or confidence-scored beliefs | not ever | Fact types are `world` and `experience`; observations carry proof counts and versions (D9/D12), not confidence scalars. |
| NG22 | Hindsight-compatible REST paths (`/v1/{tenant}/banks/...`) | not ever | The comparison harness has an adapter (§8.7); API compatibility would freeze us to another project's contract. |
| NG23 | Formal verification of the chunker, entity resolver, prompts or the gateway client | not ever | Pure heuristics and I/O adapters; property tests and goldens are the right tool (§8.2), as §7 states. |
| NG24 | Guaranteed consolidation latency or page freshness bounds | not ever | D16 exposes `consolidation_lag` and page staleness flags instead of promising a bound that LLM latency cannot keep. |
| NG25 | Per-request model selection by clients | not now | Models are configured per operation at system/tenant/namespace level (D12); a per-request override is a cost-control hole. |
| NG26 | Training, fine-tuning or hosting models | not ever | Engram consumes models through the gateway; it has no GPU and no training data pipeline. |
| NG27 | An end-user chat product or agent runtime | not ever | Engram is the memory behind an agent, reached by gRPC/Connect/MCP; the agent loop (other than the bounded Reflect, D12) belongs to the caller. |
| NG28 | Caching recall results (a "semantic cache" keyed by query) | not ever | Recall is ≈ 220 ms on the critical path (N54, N155) and its inputs change per chunk commit (D16); a result cache would reintroduce the staleness the read barrier removes. |
| NG29 | Streaming ingestion of live transcripts (token-by-token `Retain`) | not now | Items are whole documents or appends (D8 `APPEND`); a streaming front end would buffer into those. |
| NG30 | Guaranteed ordering or visibility across namespaces or shards | not ever | D16: no relationship whatsoever; anything that needs it must live in one namespace. |
| NG31 | Backing up blob objects | not ever | The blob store is durable by contract (A-O12); delete intents (N122) and the expunge's blob tombstones make deletes durable (§9.3). |
| NG32 | Running `engram-api` and `engram-worker` in one process for small deployments | not ever | D1 keeps them separate images; a single-shard dev cell is still two containers. |
| NG33 | Per-namespace lexical statistics (BM25 IDF per namespace) to close the term-rarity channel of the partition-shared index | not scheduled (N102; R33) | per-query normalised scores (N67) leave a weak oracle on partition-wide term rarity; the remedy today is `isolation = dedicated`, and a per-namespace IDF would forfeit pg_search's Top-K pushdown. |
| NG34 | Synchronous replication of a **shard** (`synchronous_commit = on`, `remote_apply`, a synchronous standby) | not ever | Every shard role runs `synchronous_commit = local` (N122): a standby outage must not hang commits or break the relay's gap horizon. The small catalog is the one exception and has a synchronous standby (N146): its writes are off the hot path, so a blocked commit degrades operations, never recall or retain. Acknowledged deletes and invalidations reach RPO 0 through intent objects put after the marker commits and before the ack; retains accept RPO ≤ 60 s. |
| NG35 | A synchronous physical delete, or a bounded-latency guarantee for recall while a delete is expunged | not ever | A delete is an O(1) soft marker honoured at ack; rows, vectors, derived versions and blobs go asynchronously (materialize ≤ 15 min, purge ≤ 24 h, index ≤ 48 h, paced by WAL, N119). Deletes are rare, and the namespace's SLOs (rerank-skip, recall latency) may degrade while markers are pending. |
| NG36 | Memory history and in-place edit of a fact (Hindsight's memory history/edit) | not now | Facts are immutable (N113); correction is `Invalidate` plus a new retain. A history API would need a version table with the N116 predicate. |
| NG37 | Bank import/clone | not now | A clone is a move whose source stays live; it would need the N124 reconcile without the freeze. Export plus a fresh retain covers it. |
| NG38 | `retry_operation` (re-run a failed operation by id) | not now | A `FAILED` operation is terminal (no `FAILED → PENDING` edge, N35); the client resubmits with a **new** `operation_id`, and the extraction cache and per-chunk commits make the re-run a resume (N139, §4.1.3). |
| NG39 | Entity-level results (returning entities as recall hits) | not now | Entities are `EntityRef`s on facts (N118); recall returns facts, observations and chunks. The original body of a document is fetched with `GetDocumentBody`, and `ListTags` and `UpdateDocumentTags` exist (N139); neither is a recall result. |
| NG40 | `ListObservationVersions` (observation history) | not now | It needs the N117 evidence-segment predicate on a new read path; version history is internal until a product need names it. |
| NG41 | A unary `Recall` alias next to the streaming RPC | not ever | A second contract test matrix for no capability; the stream carries the stats trailer and future progressive arms (N129). |
| NG42 | Re-enqueueing facts for re-enrichment with a bulk `UPDATE` | not ever | Content rows are insert-only (N113). A prompt or model bump changes the cache and extraction keys; re-extraction goes through the ordinary retain path, which is a write and hides no derived content (§6.0, N135). |
| NG43 | `clear_memories` (empty a namespace without deleting it) and the prompt-preview and page dry-run-refresh endpoints of Hindsight | not now | `DeleteDocument` per document or `DeleteNamespace` plus `CreateNamespace` covers clearing with the same O(1) marker and asynchronous expunge; a preview or dry run would be a second path through the prompt renderer and the page writer (N120) with no stored result. |
| NG44 | Cross-document deduplication of stored bodies | not now | Blobs are owner-keyed (N104): one key per owner row, deleted with it. The price is storage for identical bodies in different documents; identical bodies of one document already create no version. Reopening needs a refcounted design and the adoption race of A55. |
| NG45 | RPO 0 for a delete the client never saw acknowledged | not ever | Intent after commit (N122): a committed-but-unacknowledged marker is live but may be lost by a restore, and the client's retry re-applies it (R38, A56). |
| NG46 | MCP tools for namespace lifecycle (`create_namespace`, `update_namespace`, `delete_namespace`), `stream_snapshot`, and any `memory.admin.v1` service | not ever | The generated MCP surface is an allow-list (§4.6, N157): namespace lifecycle, mission, directives and disposition are admin calls, not agent tools (Hindsight's `create/delete_directive` and `get/update_bank` have no MCP counterpart); binary snapshot parts do not fit a tool result and agents sync with `engram-sync`. The generator fails the build for an RPC that is neither allowed nor listed under `omit:`. |

**Reopening a non-goal.** A "not now" row is reopened by a register row in D18 naming the
phase and the owning engineer, plus a §11.1 risk row for what it adds to the critical path;
a "not ever" row is reopened only by changing the register decision it cites, which means
re-running the affected §7 model checks and §8 tiers before the plan is updated. The table
is deliberately longer than the list of things we will build in phase 4: saying no in
writing is cheaper than saying it in a review.
