# agent-memory-benchmark

A Go gRPC agent-memory system evaluated on the [LoCoMo](https://arxiv.org/abs/2402.17753) and
[LongMemEval](https://arxiv.org/abs/2410.10813) retrieval benchmarks. There are **no LLMs anywhere**
in the pipeline — retrieval is classical IR only: hybrid BM25 + dense embeddings fused with RRF,
rule-based temporal boosting, and MMR diversification. Storage is an S3 blob store plus an
**append-only** Postgres enrichment ledger, with async enrichment driven by a Temporal schedule.

## Architecture

```
 client (ingest|trigger-sweep|wait-enriched|eval)
    │ gRPC :8081
    ▼
 server ──► S3/MinIO (raw blobs, content-addressed)
    │  └──► Postgres (memories rows + append-only memory_enrichment_events)
    │
 Temporal Schedule (1 min, overlap Skip)
    └─► EnrichmentSweepWorkflow (50s soft deadline)
          CountBacklog → PlanRanges → ProcessBatch ×K
             └─► unary Embedder (mock or real nomic), errgroup limit 32
                  └─► INSERT done/failed events + embedding_cache
```

Eval loads all enriched rows client-side and ranks in-process in Go: brute-force cosine over a
contiguous `[]float32` arena + in-memory BM25 → RRF → temporal boost → MMR (λ=0.7) with
per-session caps.

## Quickstart

Prereqs: Docker (with compose), Go 1.25, `protoc` with the Go plugins (`protoc-gen-go`,
`protoc-gen-go-grpc`).

```bash
make test            # tiers 0-2: unit + workflow + integration (testcontainers)
./run.sh --fixtures  # tier 3: full stack e2e, asserts Recall@5 == 1.0
```

While the stack is up: Temporal UI at http://localhost:8080, MinIO console at
http://localhost:9001 (app/appsecret), server gRPC on :8081.

## Test tiers

All five tiers pass. What is claimed here is what is tested:

| Tier | What | Infra | Command |
|---|---|---|---|
| 0 | Pure unit: RRF, MMR, cosine, BM25, tokenizer, date parsing, prefix/cache-key guards | none | `make test-unit` |
| 1 | Temporal workflow/activity logic (in-memory test env) | none | `make test-workflow` |
| 2 | Postgres append-only invariants | testcontainers | `make test-integration` |
| 3 | Full e2e on fixtures (Recall@5 == 1.0); chaos variant with fault injection reaches 100% enrichment | docker compose | `make e2e` / `make e2e-chaos` |
| 4 | Real benchmark eval | docker compose (+ real embedder for meaningful numbers) | `make eval DATASET=... RETRIEVAL=...` |

`make e2e-chaos` runs the fixtures e2e with an injected 45s embedder outage, 10% transient
failures, and 1% wrong-dimension responses; transient failures retry with backoff, permanent ones
(wrong dims) dead-letter, and enrichment still reaches 100%.

## Benchmark results

Measured with the real `nomic-embed-text-v1.5` embedder via Docker Model Runner,
per-conversation retrieval scoping, and strict **turn-level** Recall (a retrieved round counts
via its member turns; results are truncated to top-k *after* round expansion).

**LoCoMo** — 10 conversations, 1,982 scorable questions (defaults: granularity=all, rrf-k=10,
uncapped sessions):

| mode | R@5 | R@10 | NDCG@5 | MRR |
|---|---|---|---|---|
| bm25 | 0.5954 | 0.6805 | 0.4861 | 0.4855 |
| dense | 0.6227 | 0.7263 | 0.4933 | 0.4937 |
| hybrid | 0.6626 | 0.7624 | 0.5197 | 0.5148 |

Category R@5 (hybrid): multi-hop 0.348, temporal 0.730, open-domain 0.292, single-hop 0.746,
adversarial 0.732.

**LongMemEval-S** — 500 questions, 470 scorable after abstention skip (defaults:
granularity=turn, rrf-k=30, max-per-session=4):

| mode | R@5 | R@10 | NDCG@5 | MRR |
|---|---|---|---|---|
| bm25 | 0.6853 | 0.7599 | 0.5977 | 0.6376 |
| dense | 0.6809 | 0.7894 | 0.5775 | 0.6305 |
| hybrid | 0.7430 | 0.8363 | 0.6348 | 0.6688 |

Question-type R@5 (hybrid): single-session-user 0.938, knowledge-update 0.857,
single-session-assistant 0.839, temporal-reasoning 0.701, multi-session 0.625,
single-session-preference 0.533.

Honesty notes: metrics are turn-level, which is stricter than the session-level protocol most
published LongMemEval baselines report. The per-dataset retrieval defaults above were tuned via
ablation sweeps on these same corpora. Embedding documents are truncated to ~1200 chars (the
served model has a hard 512-token limit). The fixtures e2e scores a perfect 1.0
Recall@5/NDCG@5/MRR with the real embedder.

## Retrieval pipeline

- **Dense**: nomic-style embeddings, 768-dim, L2-normalized. Prefixes are mandatory:
  `search_document: ` for corpus text, `search_query: ` for queries.
- **Sparse**: hand-rolled BM25 over NFKC-normalized, stopword-filtered, Snowball-stemmed lexemes.
- **Fusion**: Reciprocal Rank Fusion (k=60 baseline; per-dataset defaults tuned by ablation —
  rrf-k=10 for LoCoMo, rrf-k=30 for LongMemEval-S, see Benchmark results).
- **Temporal**: rule-based date extraction and query-time boosting, with an unfiltered fallback so
  a failed date parse never zeroes out results.
- **Diversification**: MMR (λ=0.7) with per-session caps, indexed at round granularity.

Vectors are stored as `BYTEA` (packed little-endian float32) and ranked client-side — no pgvector.

## The ablation sanity gate

```bash
make ablation   # bm25 / dense / hybrid back to back on LongMemEval-S
```

If hybrid does not beat BM25-only by roughly **+9pp Recall@5**, the embedding path is broken
(missing prefixes, missing L2 normalization, or a dimension mismatch) — fix that before tuning
anything else. This gap is the integration test for the whole embedding path.

## Real datasets (tier 4): real-embedder runbook

Prereq: Docker Desktop with Model Runner enabled and the embedding model pulled:

```bash
docker desktop enable model-runner --tcp=12434
docker model pull ai/nomic-embed-text-v1.5
```

The nomic bridge service (`tools/nomicbridge`, compose service `embedder-nomic`) proxies to the
model runner, micro-batching unary Embedder RPCs into array requests.

```bash
./scripts/download-dataset.sh locomo          # from GitHub raw JSON
./scripts/download-dataset.sh longmemeval_s   # via huggingface-cli (may need login)
EMBEDDER_ADDR=embedder-nomic:9100 WAIT_TIMEOUT=8h ./run.sh --dataset locomo --keep-up
EMBEDDER_ADDR=embedder-nomic:9100 WAIT_TIMEOUT=8h ./run.sh --dataset longmemeval_s --keep-up
```

No image rebuild is needed after downloading — `datasets/` is a read-only volume mount into the
server container.

**Critical gotcha:** one-off `docker compose run server ...` commands do **not** inherit the
long-lived server's environment, and the compose default is the mock embedder. Pass
`-e EMBEDDER_ADDR=embedder-nomic:9100` explicitly to eval one-offs (and `-e PG_DSN=` to bypass
the query cache when the cache may be stale).

To re-enrich after changing the embedder or pipeline, bump `ENRICHMENT_VERSION` (version bumps
are free — the ledger re-derives everything as pending):

```bash
EMBEDDER_ADDR=embedder-nomic:9100 ENRICHMENT_VERSION=2 docker compose up -d server
```

The Temporal schedule pins its version args at creation — delete the schedule before restarting
the server so it is recreated with the new version:

```bash
docker compose exec temporal temporal schedule delete \
  --schedule-id enrichment-sweep --address temporal:7233
```

## Project layout

```
proto/                  Source .proto files (MemoryService, unary Embedder)
genproto/               Generated protobuf/gRPC stubs (make proto)
cmd/server/             gRPC server + embedded Temporal worker + schedule bootstrap
cmd/client/             Harness CLI: ingest | trigger-sweep | wait-enriched | eval
cmd/migrate/            Goose migration runner
internal/pipeline/      NFKC normalization, tokenize/stem, date parsing, round assembly
internal/store/         Append-only Postgres ledger (pgx v5); derived views, no UPDATE/DELETE
internal/embed/         Embedder interface, gRPC adapter, nomic prefixing, embedding_cache
internal/enrich/        Temporal sweep workflow + activities + schedule bootstrap
internal/retrieve/      Cosine, BM25, RRF, temporal boost, MMR — all in-process
internal/eval/          Recall@k / NDCG@k / MRR by category / question_type; dataset loaders
internal/blob/          Content-addressed S3 blob envelope (byte-stable JSON)
migrations/             Goose SQL migrations (ledger schema, partial unique index, views)
testdata/fixtures.json  Hand-built ~20-turn corpus with known evidence
scripts/                init-temporal-dbs.sh, download-dataset.sh
tools/mockembedder/     Deterministic hash-based mock embedder with fault injection
tools/nomicbridge/      Bridge from unary Embedder RPCs to Docker Model Runner (micro-batching)
docs/                   Frozen design docs 01-06 (the deep spec)
```

## Design docs

The docs in `docs/` are the authoritative deep spec (frozen):

- `01-retrieval.md` — benchmark formats, retrieval algorithm, expected metric ranges
- `02-storage.md` — S3 blobs + Postgres, async enrichment, unary-embedder throughput
- `03-temporal.md` — Temporal Schedule + sweeper workflow design
- `04-append-only.md` — immutable ledger, partial unique index invariant, derived views
- `05-diagrams.md` — system, pipeline, retrieval, gRPC sequence, run flow diagrams
- `06-testing.md` — the five test tiers and what each must assert

## Hard design constraints

These are invariants, not preferences (see `AGENTS.md` for the working rules):

- **Append-only Postgres** — never `UPDATE`/`DELETE` a memory or enrichment row; state is derived.
- **No LLMs** — embedding model only; no generative models, no rerankers, no agentic loops.
- **Unary embedder** — one text per RPC; throughput comes from bounded goroutine concurrency.
- **nomic prefixes mandatory** — `search_document: ` / `search_query: `, plus L2 normalization.
- **No pgvector** — vectors are `BYTEA`, ranked client-side in Go.
