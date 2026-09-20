# agent-memory-benchmark

A Go gRPC agent-memory system evaluated on the [LoCoMo](https://arxiv.org/abs/2402.17753) and
[LongMemEval](https://arxiv.org/abs/2410.10813) retrieval benchmarks. The core retrieval path is
classical IR: hybrid BM25 + dense embeddings fused with RRF, rule-based temporal boosting, and MMR
diversification. An external LLM API may be used for optional, cached, write-time enrichment and
bounded query-time helpers (see "Hard design constraints" and `AGENTS.md` rule 2); none of the
numbers below used one. Storage is an S3 blob store plus an **append-only** Postgres enrichment
ledger, with async enrichment driven by a Temporal schedule.

## Architecture

```
 client (ingest|trigger-sweep|wait-enriched|eval|build-snapshot|snapshot-info|search)
    │ gRPC :8081
    ▼
 server ──► S3/MinIO (raw blobs, content-addressed; snapshot tarballs + current.json)
    │  └──► Postgres (memories rows + append-only memory_enrichment_events + snapshot_events)
    │
    ├─ Temporal Schedule (1 min, overlap Skip)
    │    └─► EnrichmentSweepWorkflow (50s soft deadline)
    │          CountBacklog → PlanRanges → ProcessBatch ×K
    │             └─► unary Embedder (mock or real nomic), errgroup limit 32
    │                  └─► INSERT done/failed events + embedding_cache
    │
    └─ Temporal task queue "snapshot" (triggered by BuildSnapshot RPC)
         └─► BuildSnapshotWorkflow: AllocateVersion → BuildAndSeal → Publish
               (streams the enriched ledger into a Bleve+vectors artifact,
                seals it, uploads to S3, flips current.json)
    server also embeds a snapshot.Runtime: loads current.json at boot and on
    every successful BuildSnapshot, serves Search (BM25 ∥ dot-product scan → RRF
    → recency boost → abstention) — see docs/07-snapshot-serving.md.
```

Eval has two engines. `--engine inprocess` (default) loads all enriched rows client-side and ranks
in-process in Go: brute-force cosine over a contiguous `[]float32` arena + in-memory BM25 → RRF →
temporal boost → MMR (λ=0.7) with per-session caps. `--engine snapshot` instead sends one gRPC
`Search` per question against the server's pre-built Bleve + flat-vector snapshot (see "Snapshot
serving" below); it has no MMR or pre-fusion temporal filter, only post-fusion recency boost and
abstention.

## Quickstart (everything runs locally)

Prereqs: Docker with the compose plugin. Go 1.25 is only needed to run the unit tests or edit the
code (the images build Go inside Docker); `protoc` + `protoc-gen-go`/`protoc-gen-go-grpc` only
for `make proto`. The real embedding model is optional — the deterministic mock embedder is the
compose default and every tier below works with it.

```bash
./scripts/preflight.sh          # checks docker, compose, free ports, disk, optional model runner
make test                       # tiers 0-2: unit + workflow + integration (testcontainers)
./run.sh --fixtures             # tier 3: full stack e2e, in-process engine, asserts Recall@5 == 1.0
./scripts/run-snapshot.sh --fixtures   # same on the snapshot (Bleve) engine
```

Both run scripts bring the stack up, migrate, ingest, wait for enrichment, evaluate, and tear the
volumes down unless you pass `--keep-up`.

## Bring your own dataset

`--dataset` takes a built-in name or the path of a JSON file: one or more conversations, each a
list of turns (`id`, `session_id`, `speaker`, `text`, `date_time`) and optional questions with
gold `evidence` turn ids. Drop the file under `datasets/` (mounted read-only into the server
container, no rebuild) and run everything against it:

```bash
cp testdata/custom-example.json datasets/
./scripts/run-snapshot.sh --dataset datasets/custom-example.json --keep-up
docker compose run --rm server /app/client conv-id --dataset datasets/custom-example.json
docker compose run --rm server /app/client search --query "iceberg compaction" --conversation <id>
```

Format, validation rules, and the manual step-by-step are in `docs/08-custom-datasets.md`.

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

## Snapshot serving (Bleve)

An alternative, server-side retrieval engine (`internal/snapshot` + `internal/snapshotflow`, full
design in `docs/07-snapshot-serving.md`): a Temporal workflow builds an immutable, versioned
artifact — a Bleve (scorch, pure Go) BM25 index plus a flat L2-normalized `float32` vector file —
from the version-pinned enrichment ledger, seals it, and publishes it to S3 (content-addressed key,
then an atomic `current.json` pointer flip). The server downloads the current snapshot to local
disk and serves hybrid BM25 + brute-force dot-product + RRF `Search` over gRPC, instead of the
eval client fetching the whole corpus and ranking in-process.

```bash
./scripts/run-snapshot.sh --fixtures                        # full stack, build, snapshot-engine eval
docker compose run --rm server /app/client build-snapshot --version 1
docker compose run --rm server /app/client snapshot-info
docker compose run --rm server /app/client search --query "..." --conversation <id> --mode hybrid
docker compose run --rm server /app/client eval --dataset fixtures --version 1 --engine snapshot
```

Server env: `SNAPSHOT_CACHE_DIR` (default `/var/cache/agentmem/snapshots`), `SNAPSHOT_WORK_DIR`
(default a tempdir), `SNAPSHOT_VECTOR_DIM` (default `embed.Dims` = 768, no truncation),
`QUERY_EMBED_TIMEOUT` (default `2s`, hard timeout before a hybrid query degrades to lexical-only).
No benchmark numbers have been measured for this engine yet — see the honesty note in
`docs/07-snapshot-serving.md` section 9.

## Project layout

```
proto/                  Source .proto files (MemoryService, unary Embedder)
genproto/               Generated protobuf/gRPC stubs (make proto)
cmd/server/             gRPC server + embedded Temporal worker(s) + schedule bootstrap +
                        snapshot build worker + snapshot serving runtime
cmd/client/             Harness CLI: ingest | trigger-sweep | wait-enriched | eval |
                        build-snapshot | snapshot-info | search
cmd/migrate/            Goose migration runner
internal/pipeline/      NFKC normalization, tokenize/stem, date parsing, round assembly
internal/store/         Append-only Postgres ledger (pgx v5); derived views, no UPDATE/DELETE
internal/embed/         Embedder interface, gRPC adapter, nomic prefixing, embedding_cache
internal/enrich/        Temporal sweep workflow + activities + schedule bootstrap
internal/retrieve/      Cosine, BM25, RRF, temporal boost, MMR — all in-process (docs/01 engine)
internal/snapshot/      Bleve+vectors artifact, S3 store, serving runtime (docs/07 engine)
internal/snapshotflow/  Temporal workflow that builds and publishes snapshots (docs/07)
internal/eval/          Recall@k / NDCG@k / MRR by category / question_type; dataset loaders
internal/blob/          Content-addressed S3 blob envelope (byte-stable JSON)
migrations/             Goose SQL migrations (ledger schema, partial unique index, views,
                        snapshot_events + version sequence)
testdata/fixtures.json  Hand-built ~20-turn corpus with known evidence
scripts/                preflight.sh, init-temporal-dbs.sh, download-dataset.sh, run-snapshot.sh
testdata/custom-example.json  Two-conversation example of the bring-your-own dataset format
tools/mockembedder/     Deterministic hash-based mock embedder with fault injection
tools/nomicbridge/      Bridge from unary Embedder RPCs to Docker Model Runner (micro-batching)
docs/                   Frozen design docs 01-06 (the deep spec) + 07 (snapshot serving)
```

## Design docs

The docs in `docs/` are the authoritative deep spec (frozen):

- `01-retrieval.md` — benchmark formats, retrieval algorithm, expected metric ranges
- `02-storage.md` — S3 blobs + Postgres, async enrichment, unary-embedder throughput
- `03-temporal.md` — Temporal Schedule + sweeper workflow design
- `04-append-only.md` — immutable ledger, partial unique index invariant, derived views
- `05-diagrams.md` — system, pipeline, retrieval, gRPC sequence, run flow diagrams
- `06-testing.md` — the five test tiers and what each must assert
- `07-snapshot-serving.md` — Bleve + flat-vector snapshot build/serve engine (alternative to the
  in-process retrieval path above)
- `08-custom-datasets.md` — bring-your-own dataset format and how to run everything on it

## Hard design constraints

These are invariants, not preferences (see `AGENTS.md` for the working rules):

- **Append-only Postgres** — never `UPDATE`/`DELETE` a memory or enrichment row; state is derived.
- **External LLM only, fenced** — one client package, env-configured API, every call cached
  append-only by content hash, write-time enrichment first, query-time use bounded by a timeout
  with a classical fallback, every LLM feature a flag that defaults to off until it wins an
  ablation, and the eval report says which flags were on. No agentic loops in the serving path.
- **Unary embedder** — one text per RPC; throughput comes from bounded goroutine concurrency.
- **nomic prefixes mandatory** — `search_document: ` / `search_query: `, plus L2 normalization.
- **No pgvector** — vectors are `BYTEA`, ranked client-side in Go.
