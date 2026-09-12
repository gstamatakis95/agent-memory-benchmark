# AGENTS.md — working rules for this repo

Read this before changing anything. The project is complete and all five test tiers pass;
your job is to keep them passing.

## Non-negotiable constraints (violating any of these is a bug, not a tradeoff)

1. **Append-only Postgres.** Never `UPDATE` or `DELETE` rows in `memories` or
   `memory_enrichment_events`. New information = new row. Pending, dead-letter, and progress
   are *derived* by queries (anti-join + backoff predicate, views, functions) — never stored
   status flips. The test-only immutability trigger in `internal/store/append_only_test.go`
   must stay green.
2. **External LLM allowed, but fenced.** (Rule changed 2026-09; it used to be "no LLMs
   anywhere".) A generative model may be called only through an external API, behind one
   client package (`internal/llm`, to be added) configured by env (`LLM_API_BASE`,
   `LLM_API_KEY`, `LLM_MODEL`), never vendored or embedded in the image. Fences:
   - **Write-time first.** LLM work belongs in enrichment (contextual prefixes, fact/entity
     extraction, contradiction adjudication) where it is a pure function of a memory and can be
     cached; every call is keyed by SHA-256 of (model, prompt version, input) in an append-only
     `llm_cache` table, exactly like `embedding_cache`.
   - **Query-time use must be bounded and optional.** Anything on the query path (query
     decomposition, time-window extraction, reranking) needs a hard timeout with a graceful
     fallback to the classical path, and an off switch.
   - **Classical IR stays the baseline.** Every LLM-powered feature is a flag that defaults to
     off until an ablation shows it beats the classical path on LongMemEval-S; eval output must
     print which LLM features were on. Never let an LLM produce the benchmark answer itself —
     these are retrieval benchmarks and the metric is evidence recall.
   - **No agentic loops in the serving path.** One bounded call per stage, no tool use, no
     retries that change the prompt.
   - **Secrets via env only.** Never log prompts containing memory text at INFO; never commit
     keys.
3. **No pgvector, no embedded databases.** Vectors are `BYTEA` (packed little-endian float32),
   ranked client-side in Go.
4. **The embedder is unary only.** One text per RPC — do not add batching. Recover throughput
   with bounded concurrency (`errgroup.SetLimit(32)`), not batch calls.
5. **nomic prefixes are mandatory.** `search_document: ` for corpus text, `search_query: ` for
   queries; L2-normalize every vector. `embedding_cache` keys are the SHA-256 of the
   **prefixed** text — never the raw text, never with a timestamp mixed in.
6. **Module path is `example.com/agentmem`**, Go 1.25. Don't rename it.

## Frozen files — do not edit

- `docs/` — frozen design docs (the spec; code follows them, not vice versa)
- `tools/mockembedder/main.go` — provided deterministic mock
- `internal/store/append_only_test.go` — the invariant safety net
- `Makefile`, `docker-compose.yml`, `run.sh` — infra contracts

## Where things live

- `proto/` source protos → `genproto/` generated stubs (`make proto`)
- `cmd/server` gRPC server + embedded Temporal worker + schedule bootstrap + snapshot build
  worker + snapshot serving runtime;
  `cmd/client` harness (ingest | trigger-sweep | wait-enriched | eval | build-snapshot |
  snapshot-info | search); `cmd/migrate` goose runner
- `internal/pipeline` normalize/tokenize/date-parse/round assembly (pure, deterministic)
- `internal/store` ledger inserts + derived queries (no UPDATE/DELETE, by design)
- `internal/embed` Embedder interface, gRPC adapter, prefixing, cache, BYTEA packing
- `internal/enrich` sweep workflow + CountBacklog/PlanRanges/ProcessBatch activities
- `internal/retrieve` cosine, BM25, RRF (k=60), temporal boost, MMR (λ=0.7) — the in-process
  (docs/01) retrieval engine
- `internal/snapshot` (docs/07-snapshot-serving.md) immutable Bleve+flat-vector artifact
  (builder/open/seal), S3 store, and the serving Runtime; `internal/snapshotflow` the Temporal
  workflow that builds and publishes a snapshot version
- `internal/eval` metrics + dataset loaders; `internal/blob` content-addressed S3 envelope
- `migrations/` goose SQL (`00004_snapshots.sql` adds `snapshot_events` + the version sequence);
  `testdata/fixtures.json` the e2e corpus
- `scripts/run-snapshot.sh` the snapshot-engine counterpart of `run.sh`: same infra/ingest steps,
  then `build-snapshot` + `eval --engine snapshot` instead of `run.sh`'s single in-process eval

## How to verify changes (the tier ladder)

```bash
make test-unit          # on every change (<1s)
make test-workflow      # additionally, for any internal/enrich change
make test-integration   # before pushing (testcontainers Postgres)
./run.sh --fixtures     # before/after any schema or pipeline change (asserts R@5 == 1.0)
make e2e-chaos          # if you touched retry/backoff/dead-letter paths
```

`make test` = tiers 0–2. Tier 4 (`make eval DATASET=... RETRIEVAL=...`) needs real datasets and
is not for CI.

## Gotchas (learned the hard way — do not re-learn them)

- **Temporal test env auto-skips timers.** A workflow test whose only exit is the 50s soft
  deadline will burn iterations. Mock `CountBacklog` to return 0 eventually to test the drain
  path; the workflow sleeps 1s between waves precisely so simulated workflow time advances and
  the soft-deadline path terminates — test that path separately.
- **The `ON CONFLICT` arbiter needs the predicate.** Completion insert must be exactly
  `ON CONFLICT (memory_id, enrichment_version) WHERE status='done' DO NOTHING`. Dropping the
  `WHERE` breaks arbiter inference against the partial unique index (`uq_enrich_success`);
  a test pins this — do not "simplify" it.
- **Backdating in tests = insert a new row with an explicit `created_at`.** Never mutate a row
  to age it; the insert path takes an optional timestamp arg used only by tests.
- **Goose function bodies need `StatementBegin`/`StatementEnd`.** plpgsql bodies contain
  semicolons; without the annotations goose splits them mid-statement.
- **`run.sh` tears down volumes.** Its EXIT trap runs `docker compose down -v` unless you pass
  `--keep-up` — anything in Postgres/MinIO dies with the run.
- **Datasets are a read-only volume mount.** `datasets/` is mounted into the server container:
  after `./scripts/download-dataset.sh <name>` no image rebuild is needed.
- **Version bumps are free.** To re-enrich, bump `ENRICHMENT_VERSION`; the anti-join re-derives
  everything as pending. Never write a bulk re-enqueue UPDATE.
- **Fault injection knobs** live on the mock embedder: `MOCK_LATENCY_MS`, `MOCK_FAIL_RATE`,
  `MOCK_FAIL_UNTIL`, `MOCK_BAD_DIMS_RATE`. Wrong-dims responses must dead-letter as `permanent`,
  not retry forever.
- **Ablation is the embedding-path integration test.** If hybrid doesn't beat bm25 by ~+9pp R@5
  on LongMemEval-S, suspect prefixes / L2 normalization / dims before tuning anything.
- **compose-run env trap.** One-off `docker compose run server ...` commands default to the mock
  embedder — always pass `-e EMBEDDER_ADDR=embedder-nomic:9100` for real-embedding evals
  (and `-e PG_DSN=` to bypass a possibly stale query cache).
- **The Temporal schedule pins its workflow args at creation.** An `ENRICHMENT_VERSION` bump
  needs `temporal schedule delete --schedule-id enrichment-sweep` + server restart, or the sweep
  keeps running the old version.
- **The served nomic model hard-rejects >512-token inputs** (HTTP 500 "too large"). The bridge
  truncates to 1200 chars and dead-letters items that still overflow; the embed client treats
  embedder `INTERNAL` status as permanent by contract.
- **zsh on this machine does not word-split unquoted variables.** Use `${=var}` when expanding a
  space-separated list in a loop.
- **Docker Desktop credential-helper leak.** `docker-credential-desktop` processes can
  accumulate and exhaust the per-user process table (fork failures everywhere); fix by
  restarting Docker Desktop or switching `~/.docker/config.json` `credsStore` to `osxkeychain`.
- **Bleve is not the "no embedded databases" exception being made twice.** It is an in-memory/
  on-disk *search index* artifact, not an embedded database in the sense of rule 3 —
  docs/01-retrieval.md §4.3 names Bluge/Bleve as the sanctioned BM25 option. The snapshot it
  builds (`internal/snapshot`) never replaces Postgres as the ledger; Postgres is still the only
  source of truth, and a snapshot is a disposable, rebuildable projection of it.
- **`snapshot_events` is append-only like every other ledger table.** Never `UPDATE`/`DELETE` a
  row; "current" is derived as the latest `published` event (and mirrored to `current.json` in S3
  so the runtime doesn't need a live Postgres round trip to find the pointer) — same shape as
  `memory_enrichment_events`' derived pending/dead-letter state.
