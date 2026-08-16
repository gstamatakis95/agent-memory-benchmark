# Agent Prompt — Build the Agent-Memory Benchmark System

Paste everything below into your coding agent (Claude Code or similar), with the downloaded artifacts placed in the working directory.

---

## Context

You are building a Go gRPC agent-memory system evaluated on the LoCoMo and LongMemEval retrieval benchmarks. I have already produced the design; your job is to implement it, get it building, and get the test tiers green. Do not redesign — where the design docs and your instincts disagree, follow the docs or ask me.

## Phase 0 — Verify your inputs before doing anything else

The design lives in a set of markdown documents in this directory (likely under `artifacts/` or the repo root; filenames may vary). **Start by listing every `.md`, `.yml`, `.go`, and `Makefile` in the working directory and reading all of the markdown end to end.**

You need documents covering all five of these topics. Identify which file covers which by reading them — do not go by filename:

1. **Retrieval design** — LoCoMo/LongMemEval formats and evidence fields, hybrid BM25+dense with RRF, MMR, temporal boosting, expected metric ranges (R@5 ≈ 0.86 BM25-only vs ≈ 0.95 hybrid on LongMemEval-S)
2. **Storage design** — S3 blobs + Postgres reference rows, async enrichment, unary gRPC embedder throughput and connection config, `embedding_cache`
3. **Temporal design** — Schedule (1 min, overlap Skip) + sweeper workflow + batch activities replacing a hand-rolled queue
4. **Append-only design** — immutable enrichment ledger, the `uq_enrich_success` partial unique index invariant, derived pending/progress/dead-letter views
5. **Testing design** — the five test tiers and what each must assert

Plus these code/infra files, which are already written and should not be rewritten: `docker-compose.yml`, `run.sh`, `Makefile`, the mock embedder Go source, and the Postgres invariant test file.

**If any of the five topics above is not covered by a document you can actually read, STOP and tell me which one is missing.** Do not infer the design from the other documents and do not proceed on assumptions — several of these documents contain exact DDL, index predicates, and formulas that you cannot reconstruct from context.

Also note: downloaded files may have arrived flattened. Before building, move them into the target layout below — in particular the mock embedder source belongs at `tools/mockembedder/main.go` and the invariant tests at `internal/store/append_only_test.go`, matching the paths referenced by `Makefile` and `docker-compose.yml`.

## Hard constraints — violating any of these is a bug, not a tradeoff

1. **No LLMs anywhere.** Embedding model only. No generative models, no agentic loops, no cross-encoder rerankers. Classical IR only: BM25, RRF, MMR, rule-based date extraction.
2. **Append-only Postgres.** Never `UPDATE` or `DELETE` a memory or enrichment row. New information = new row. The test-only immutability trigger in `append_only_test.go` must stay green.
3. **No pgvector, no embedded databases.** Vectors are `BYTEA` (packed little-endian float32). Ranking happens client-side in Go.
4. **The embedder is unary only.** One text per RPC, no batching. Recover throughput with bounded goroutine concurrency, not batch calls.
5. **nomic prefixes are mandatory.** `search_document: ` for corpus text, `search_query: ` for queries. L2-normalize every vector. Getting this wrong is the single most expensive bug in the system — write the guard test first.
6. **Go for everything.** Module path `example.com/agentmem`. Go 1.22+.

## Target layout

```
.
├── proto/
│   ├── agentmem/v1/memory.proto      # MemoryService: UploadMemories, FetchAllMemories,
│   │                                  # GetProgress, TriggerSweep
│   └── embed/v1/embedder.proto       # improvised unary Embedder (see design-storage.md §E.3)
├── cmd/
│   ├── server/                       # gRPC server + embedded Temporal worker + Dockerfile
│   ├── client/                       # ingest | trigger-sweep | wait-enriched | eval
│   └── migrate/                      # goose runner
├── internal/
│   ├── pipeline/                     # normalize, tokenize/stem, date parse, round assembly
│   ├── store/                        # pgx v5: ledger inserts, derived queries, CopyFrom
│   ├── embed/                        # Embedder interface + gRPC adapter + cache
│   ├── enrich/                       # Temporal workflow + activities + schedule bootstrap
│   ├── retrieve/                     # cosine, BM25, RRF, temporal boost, MMR
│   └── eval/                         # Recall@k, NDCG@k, MRR by category/question_type
├── migrations/                       # goose SQL
├── testdata/fixtures.json            # ~20-turn corpus with known evidence
├── scripts/                          # init-temporal-dbs.sh, download-dataset.sh
└── tools/mockembedder/               # provided
```

## Build order — verify each step before moving on

Work in these phases and run the stated check at the end of each. Do not proceed past a red check.

**Phase 1 — Skeleton and protos.** `go mod init`, write both `.proto` files, wire `make proto`. The improvised unary embedder proto is in the storage doc; the memory service proto is in the storage doc's "what changes" section plus the version-pinned RPC signatures in the append-only doc. Every fetch and progress RPC takes an explicit `enrichment_version` — no defaulting.
✅ Check: `make proto && go build ./...` succeeds; mockembedder compiles against the generated stubs.

**Phase 2 — Migrations and store.** Write the goose migrations from the DDL in the append-only doc **verbatim**: `memories`, `memory_enrichment_events`, the `uq_enrich_success` partial unique index, `ix_enrich_attempts`, `embedding_cache`, the `latest_enrichment` view, and the `enrichment_progress` / `dead_letter` functions. Set `fillfactor=100` and `STORAGE EXTERNAL` on the embedding column. Implement the `internal/store` helpers that `append_only_test.go` calls (`insertMemory`, `insertDone`, `insertFailed`, `isPending`, `isDead`, `fetchAtVersion`, `applyMigrations`, `backdateViaFixtureTable`).

Note on `backdateViaFixtureTable`: tests need to simulate an aged failure without updating rows. Insert the failure row with an explicit `created_at` parameter rather than mutating it afterward — add an optional timestamp arg to the insert path used only by tests.

The completion insert must be exactly `ON CONFLICT (memory_id, enrichment_version) WHERE status='done' DO NOTHING`. The predicate is required for arbiter inference; there is a test pinning this.
✅ Check: `make test-integration` — all six invariant tests green.

**Phase 3 — Pipeline and embedder.** Unicode NFKC normalization, stopword+Snowball tokenization, date extraction (`araddon/dateparse` with `ParseStrict`, `olebedev/when` for relative expressions), round assembly (user+assistant pairs, keeping turn rows for LoCoMo turn-level evidence). Then the `Embedder` interface, the gRPC adapter with the retry service config and keepalive from the storage doc, and the `embedding_cache` lookup keyed on the **prefixed** text's SHA-256.
✅ Check: `make test-unit` — including the prefix guard test and the cache-key test (hash the prefixed text, never the raw text, and never include a timestamp).

**Phase 4 — Temporal enrichment.** `EnrichmentSweepWorkflow` (soft-deadline loop, fan-out via futures), `CountBacklog`, `PlanRanges`, `ProcessBatch` (deterministic id-range claiming, `errgroup.SetLimit(32)`, `RecordHeartbeat`, idempotent appends). Schedule bootstrap on server start with `AlreadyExists` swallowed. Worker options with `TaskQueueActivitiesPerSecond` as the global embedder throttle.

Watch the test-env timer-skipping gotcha described in the testing doc's Tier 1: mock `CountBacklog` to drain, and test the soft-deadline exit separately.
✅ Check: `make test-workflow` — both the drain path and the deadline-exit path green.

**Phase 5 — Retrieval and eval.** Client-side: load all enriched rows into a contiguous `[]float32` arena, brute-force cosine, in-memory BM25, RRF (k=60, configurable — try 10–30 for the ~50-session haystacks), rule-based temporal boost with unfiltered fallback, MMR (λ=0.7) with per-session caps. Eval computes Recall@k / NDCG@k / MRR from the benchmark evidence annotations, broken out by LoCoMo `category` and LongMemEval `question_type`, skipping `_abs` questions for retrieval.

Support a `--retrieval bm25|dense|hybrid` flag — the ablation is the system's main correctness gate, not a nice-to-have.
✅ Check: `make test-unit` covers RRF/MMR/cosine against hand-computed values.

**Phase 6 — Wire the e2e.** Server binary (gRPC + worker + schedule), client subcommands, Dockerfiles, `scripts/init-temporal-dbs.sh` (create `temporal` and `temporal_visibility` databases), `scripts/download-dataset.sh` (LoCoMo from the GitHub raw JSON; LongMemEval via `huggingface-cli`, which may need login), and `testdata/fixtures.json` — a hand-built ~20-turn conversation across 3 sessions with 5 questions whose evidence turn IDs you control.
✅ Check: `./run.sh --fixtures` passes with Recall@5 == 1.0, then `make e2e-chaos` also reaches 100% enrichment despite the injected outage.

## Acceptance criteria

- `make test` (tiers 0–2) green, `make e2e` green, `make e2e-chaos` green.
- `docker compose up` comes up clean; Temporal UI at :8080 shows sweep runs with fan-out activities; MinIO console at :9001 shows blobs.
- Re-running `./run.sh --fixtures` twice makes near-zero embed RPCs on the second run (cache working). Instrument the mock with a call counter and assert this.
- No `UPDATE` or `DELETE` statement exists anywhere in `internal/store` against `memories` or `memory_enrichment_events`. Grep for it as a final check.

## Working style

- Read all design docs first, then show me your planned file list and proto definitions before implementing Phase 2.
- Commit at each phase boundary with the verification output in the message.
- If a design doc is ambiguous or a pinned dependency version doesn't resolve, stop and ask rather than guessing — several version numbers in the docs are flagged as needing verification at implementation time (Temporal SDK, auto-setup image, UI image).
- Prefer standard library and the specific libraries named in the docs (`pgx/v5`, `goose`, `go.temporal.io/sdk`, `errgroup`, `dateparse`, `when`, `snowball`, `bluge` or a hand-rolled BM25). Don't introduce a dependency the docs don't name without asking.
- When you finish a phase, run the check and paste the actual output. Don't claim green without showing it.