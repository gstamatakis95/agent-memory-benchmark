# Snapshot Serving — Bleve + Flat Vectors over Immutable S3 Artifacts

An alternative retrieval engine alongside the in-process path of `docs/01-retrieval.md`: instead
of the client fetching a conversation's whole enriched corpus and ranking it in Go on every eval
run, a Temporal workflow builds a versioned, immutable retrieval artifact once — a Bleve BM25
index plus a flat L2-normalized `float32` vector file — publishes it to S3, and the server loads
it into memory and serves `Search` over gRPC. Same classical-IR math (BM25, brute-force dot
product, RRF, recency boost, abstention) as `internal/retrieve`; different serving shape.

No LLMs anywhere in this path either. Bleve is a search-index *artifact*, not a database — see
the "what this is not" gotcha in `AGENTS.md`.

---

## 0. Decisions

| Decision | Rationale |
|---|---|
| Bleve (scorch), pure Go | No CGO, no pinned native vector-search fork to build and ship. Static builds, easy cross-compilation. |
| Flat vectors, brute-force dot product, no ANN | At benchmark scale (tens to low hundreds of thousands of rows) an exact parallel scan is tens of milliseconds and gives 100% recall on the dense arm. ANN would trade recall for speed this system does not need. |
| Immutable, versioned snapshots | Atomic cutover (`current.json` flip after upload completes), trivial rollback (old versions stay in S3), cache-by-immutable-key with no invalidation logic. |
| Download artifact to local disk, never read S3 directly at query time | The vector scan touches every row; a range-request VFS would re-pull the whole file per query. |
| Rebuild the whole artifact rather than mutate in place | Bleve is single-writer; a full rebuild is atomic and reproducible, and the embedding_cache (already required by `internal/embed`) makes rebuilds cheap because the corpus's dense vectors already exist in the ledger. |
| Append-only `snapshot_events` ledger + a Postgres sequence for version numbers | Same invariant as `memories` / `memory_enrichment_events`: never `UPDATE`/`DELETE`. "Current" is derived (latest `published` event) and mirrored to `current.json` in S3 so the runtime never needs a live Postgres round trip to find the pointer. |

---

## 1. Embedding

Snapshot vectors go through the same `internal/embed.Client` as everything else in this repo —
`search_document: ` at index time, `search_query: ` at query time, L2-normalized, unary calls only
(`embed.Client.EmbedDocument` / `EmbedQuery`; no batching added for this path either). There is no
separate embedding step in the build workflow: by the time a memory is enrichable at a version, its
embedding already lives in `memory_enrichment_events` via the ordinary unary sweep
(`internal/enrich`), so `BuildAndSeal` reads it straight out of the ledger.

**Dimension.** `SNAPSHOT_VECTOR_DIM` defaults to `embed.Dims` (768, no truncation). A value below
768 slices the leading dims and re-normalizes (Matryoshka truncation) — but that is only
correct if the served model applies its final layer-norm to the *full* vector before any slicing
happens. Nothing on the unary embedder's wire contract lets a caller verify that ordering for the
Docker Model Runner GGUF build in this repo, so truncation is opt-in and off by default; use it
only after checking cosine similarity against a reference implementation at the target dimension
(`docs/06-testing.md` tier 0 style). The runtime slices the *query* vector to the same width at
search time (`SNAPSHOT_VECTOR_DIM < len(vec)` in `cmd/server/main.go`'s `Search`), so build and
query truncation always agree.

**Model pinning.** The unary embedder has no model-revision endpoint to assert against (unlike the
design sketch's `embed_model_rev` field, which this repo cannot populate), so the manifest records
only `embed_model` (`embed.Model`, `"nomic-embed-text-v1.5"`) and `snapshot.Expectations` asserts
it — plus `vector_dim` — at `Runtime.Load` time. A silent model swap behind the gRPC endpoint is
therefore only caught if it also changes `embed.Model` or the dimension; that residual risk is the
same one `docs/01` already accepts for the in-process path.

---

## 2. Artifact layout

```
snapshot-v<version>.tar.zst
├── manifest.json     Manifest (version, doc_count, vector_dim, metric,
│                      embed_model, enrichment_version, bleve_version, created_at)
├── index.bleve/       scorch segment directory: BM25 over pre-tokenized
│                      lexemes (pipeline.Tokenize output), whitespace analyzer
├── vectors.f32        VEC1 header (magic[4] dim(u32 LE) count(u32 LE)
│                      reserved[4]) + count*dim little-endian float32 rows
├── ordinals.txt       row i -> memory id (decimal), one per line
└── docs.jsonl         row i -> Doc payload (memory_id, conversation_id,
                       session_id, ts_unix, content_hash, s3_key)
```

Row *i* of `vectors.f32`, line *i* of `ordinals.txt`, and line *i* of `docs.jsonl` describe the
same memory — that ordinal mapping is the only link between the lexical arm (keyed by the Bleve
document id, which is the decimal memory id) and the dense arm. `Open` asserts all three counts
equal `manifest.doc_count` and that the `VEC1` header's `(dim, count)` match, so a truncated or
drifted artifact fails to load instead of silently returning wrong rows.

---

## 3. Build pipeline (Temporal)

```
BuildSnapshotWorkflow(BuildInput{EnrichmentVersion, VectorDim})
  ├─ AllocateVersion(enrichmentVersion)        → version   (Postgres sequence)
  ├─ BuildAndSeal(version, enrichmentVersion)  → BuildResult (local tar.zst + Manifest)
  └─ Publish(version, tarPath)                 → PublishResult (S3 key; current.json flipped)
```

`BuildAndSeal` streams every `enrichment_at_version` row for the pinned enrichment version
straight from Postgres into a `snapshot.Builder` writing to local disk — no per-row activity
payloads, no separate `EmbedBatch` step, because the dense vector for each row already lives in
the ledger from the ordinary unary embedding sweep. The activity heartbeats after every Bleve
batch (`snapshot.BatchSize`, low thousands — single-document `Index()` calls thrash the segment
merger) so a retry resumes rather than restarts. `Builder.Close()` flushes the last batch, closes
the Bleve index (required before archiving — it flushes segments to disk), writes `manifest.json`,
and re-opens the result to verify it before returning.

`Publish` uploads the sealed tarball to the content-addressed key
`snapshots/snapshot-v<version>.tar.zst` (never overwritten — artifacts are immutable), appends a
`published` row to the append-only `snapshot_events` table, and only then flips `current.json` in
S3. That ordering is the atomic cutover: a reader that lists `current.json` before step 3 finishes
sees the previous (still valid) version; nothing ever points at a partially uploaded artifact.

The workflow id is fixed per enrichment version (`snapshotflow.WorkflowIDPrefix + version`), so
concurrent `BuildSnapshot` calls for the same version collapse into one run — Bleve is
single-writer, and a rebuild is cheap enough (full corpus, cached embeddings) that there is no
reason to support concurrent partial writers instead.

---

## 4. Serving runtime

At startup (`cmd/server/main.go`) the server builds a `snapshot.Runtime` and calls `Reload`: it
reads `current.json`, and — if a version is published — downloads (or reuses a cache hit keyed by
the immutable version) and opens it, asserting `Expectations{VectorDim, EmbedModel}` before
swapping it in. A server that boots with nothing published logs that and serves `Search` with
`FailedPrecondition` until an operator runs `BuildSnapshot`. `Runtime.Search` takes the load's read
lock for the duration of one query, so a concurrent `Load` (from `BuildSnapshot`) can never close a
snapshot out from under an in-flight search.

**Query path** (`memoryService.Search` in `cmd/server/main.go`, delegating fusion/recency/
abstention to `snapshot.Snapshot.Search`):

```
SearchReq{query, conversation_id, mode, top_k, depth, rrf_k, recency_boost, ...}
   │
   ├─ pipeline.Tokenize(query)                          → lexemes (always; BM25 needs no embed call)
   │
   ├─ mode != bm25 → embed.Client.EmbedQuery(query) under a hard QUERY_EMBED_TIMEOUT
   │     ├─ success → optionally slice to SNAPSHOT_VECTOR_DIM + L2Normalize    → qvec
   │     ├─ timeout/error, mode == dense    → codes.Unavailable (fail the RPC)
   │     └─ timeout/error, mode == hybrid   → log + continue with qvec = nil
   │                                            (degrades to lexical-only; response.lexical_only=true)
   │
   ├─► snapshot.Runtime.Search(fn) — read lock, fn runs against the loaded Snapshot:
   │      f := s.ConversationFilter(conversation_id)   (nil filter when conversation_id == 0)
   │      s.Search(lexemes, qvec, f, opts)              → Bleve top-N ∥ vector top-N, concurrently
   │                                                        (both filtered identically) → RRF(k) →
   │                                                        recency boost → abstention → top_k
   └─► map each Hit's Doc (memory_id, content_hash, s3_key, session_id, ts_unix) to a SearchHit
```

The embed call is the only network round trip on this path (single-digit to double-digit ms for
BM25 and the parallel vector scan at this scale, versus tens of ms for the embedder), so it is the
one thing bounded by its own timeout with a graceful fallback instead of failing the whole request.
`QUERY_EMBED_TIMEOUT` defaults to 2s.

---

## 5. Retrieval mechanics

- **Lexical.** Bleve BM25 over the pre-tokenized `lexemes` field (the same `pipeline.Tokenize`
  output the in-process engine indexes), a whitespace-only analyzer so tokenization stays owned by
  `internal/pipeline` rather than Bleve's own analyzer chain. A conjunct `conversation_id` term
  restricts the arm to one conversation. Two Bleve specifics worth knowing:
  - Bleve's BM25 is Lucene-classic flavoured: the same IDF `ln(1 + (N-n+0.5)/(n+0.5))` and
    `k1=1.2, b=0.75` as `internal/retrieve/bm25.go`, but a `sqrt(tf)` term frequency, `k1` rather
    than `k1+1` in the numerator, and query terms weighted by `idf²` via a query norm. Rankings
    therefore differ from the hand-rolled Okapi for multi-term or high-tf cases; the exact formula
    is pinned by `TestBleveBM25UsesTrueAverageLength`.
  - Bleve derives the average field length for the norm from stats the caller may supply per
    request; without them it uses `ceil(distinct terms / doc count)`, which is ~1 on any real
    corpus and makes every long row lose. The builder records the true total lexeme count in
    `manifest.lexeme_count` and `LexicalTopN` feeds `ceil(lexeme_count / doc_count)` back in as
    pre-search data, restoring the standard `1 - b + b·len/avgLen` norm.
- **Dense.** Brute-force dot product over the in-RAM `[]float32` arena (loaded from `vectors.f32` at open), parallelized over
  `GOMAXPROCS` row ranges with a per-worker min-heap (no cross-goroutine contention); a
  `roaring.Bitmap` filter (built once per query from the same conversation id) is tested *before*
  the dot product, since the bitmap test is far cheaper than the multiply-add it skips.
- **Fusion.** Reciprocal Rank Fusion, `k=60` default (`SearchReq.rrf_k` overrides), depth 40 default
  per arm (`SearchReq.depth`) before fusion — retrieval depth was the single largest lever in
  ablations on this corpus family (see `README.md` benchmark results for the in-process engine;
  this engine has not been separately re-measured — see the honesty note in section 9 below). A
  deterministic ascending-memory-id tiebreak on equal fused scores keeps eval runs reproducible.
- **Recency.** Post-fusion multiplicative boost, `final = rrf * (1 + B*exp(-ln2/halfLife *
  ageDays))`, default `B=0` (off). Rows without a timestamp get no boost rather than an arbitrary
  one.
- **Abstention.** If the top fused (post-boost) score is below `abstain_threshold` (default 0 =
  off), the response sets `abstained=true`; hits are still returned so a caller can inspect what
  was *nearly* good enough.
- **Filtering.** The single `Filter` (conversation id → `Ordinals` bitmap) is built once per query
  and applied to both arms — a filter that only reaches one arm would silently bias the fusion
  toward whichever arm saw the unfiltered result set.

---

## 6. What was deliberately not built, and why

Everything below appears in the source design sketch for this feature but needs a generative model
(forbidden outright by `AGENTS.md` rule 2) or is out of scope at this corpus scale:

- **Contextual retrieval prefixes / atomic fact extraction / entity extraction** — the sketch's
  biggest reported wins come from an LLM writing a situating sentence or extracting facts into the
  index key. No non-LLM equivalent was substituted; the index key here is exactly
  `pipeline.Tokenize(normalized_text)`, matching what the in-process engine already indexes.
- **Bi-temporal contradiction detection** (`t_valid_from`/`t_valid_to`, LLM-adjudicated contradiction
  on a knowledge update) — needs a model to decide "does this new assertion contradict that old
  one." The append-only ledger still keeps every version of a fact; there is just no automatic
  superseding.
- **Cross-encoder rerank** — a reranker is itself a (small) learned relevance model in the sense
  this benchmark's "no LLMs" rule cares about, and the design sketch's own citation shows one
  *degrading* nDCG by up to 3% on out-of-distribution corpora while adding 500–2000ms; not worth
  the risk even if it were allowed.
- **Query decomposition for multi-hop** — needs a model to split a query into sub-queries.
- **ANN indexing** — ruled out by scale (section 0), not by the no-LLM constraint; revisit only if
  the corpus grows into the millions of rows.

---

## 7. Failure modes

| Failure | Consequence | Mitigation |
|---|---|---|
| Embedding endpoint down/slow at query time | Dense arm unusable | `QUERY_EMBED_TIMEOUT` + degrade to lexical-only (hybrid), or `codes.Unavailable` (dense-only) |
| Embedding endpoint down at build time | `BuildAndSeal` stalls | Heartbeat-resumable activity; the embedding already lives in the ledger from the ordinary sweep, so a build retry re-reads Postgres, not the embedder |
| Tarball archived without closing the Bleve index first | Corrupt index for readers | `Builder.Close()` closes Bleve before `Seal` runs, and re-opens the result to verify before returning |
| `current.json` flipped before the upload finished | Readers 404 or load a truncated artifact | `Publish` uploads fully; `FlipPointer` is a separate, later call the workflow only reaches on success |
| Filter applied to only one arm | Wrong fusion, cross-conversation leakage | One `Filter` built once per query, passed into both `LexicalTopN` and `VectorTopN` |
| Ordinals / vectors / docs.jsonl drift | Wrong document returned for a hit | Written in one pass by `Builder.Add`; `Open` asserts all three counts against `manifest.doc_count` |
| `SNAPSHOT_VECTOR_DIM` mismatch between build and query, or a model swap changing `embed.Model` | Dense arm scores noise, no error | `Runtime.Load` asserts `Expectations{VectorDim, EmbedModel}` against the manifest before swapping the snapshot in |
| `BuildSnapshot` called before any enrichment exists at that version | Nothing to index | `BuildAndSeal` fails the build (and appends a `failed` event) when zero rows were added — an empty snapshot is a bug, not a result; the allocated version is simply burned |
| `Publish` retried after its upload completed (ledger insert or pointer flip failed) | A second upload would violate "never overwrite" | `S3Store.Publish` treats an existing object of identical size at this version's key as its own earlier attempt and proceeds without re-uploading; a different size is refused |

---

## 8. How to run

```bash
./scripts/run-snapshot.sh --fixtures                       # infra up, ingest, enrich, build, eval
./scripts/run-snapshot.sh --dataset longmemeval_s --version 1
```

Or drive the pieces directly against a running stack (see `run.sh` for steps 1–5: infra, migrate,
dataset fetch, server up, ingest + wait-enriched):

```bash
docker compose run --rm server /app/client build-snapshot --version 1
docker compose run --rm server /app/client snapshot-info
docker compose run --rm server /app/client search --query "when did I move to Paris" --conversation 123 --mode hybrid
docker compose run --rm server /app/client eval --dataset fixtures --version 1 --engine snapshot
```

`build-snapshot`, `snapshot-info`, `search`, and `eval --engine snapshot` are all just gRPC calls
against the long-lived `server` service — the snapshot itself is built, published, and loaded
entirely inside that container, so unlike a real-embedder `eval` one-off (see the compose-run env
trap in `AGENTS.md`) these one-off client invocations need no extra `-e` flags.

**Environment** (server-side; documented in full in `cmd/server/main.go`'s header):
`SNAPSHOT_CACHE_DIR` (default `/var/cache/agentmem/snapshots`), `SNAPSHOT_WORK_DIR` (default a
tempdir), `SNAPSHOT_VECTOR_DIM` (default `embed.Dims`, 768), `QUERY_EMBED_TIMEOUT` (default `2s`).

**Relation to the in-process engine.** `docs/01-retrieval.md`'s client-side engine
(`internal/retrieve`, `eval --engine inprocess`, the default) stays the ablation baseline: it fetches
the whole per-conversation corpus over `FetchAllMemories` and ranks with MMR-diversified,
temporal-boosted hybrid retrieval, entirely in the eval client. The snapshot engine trades that
per-run corpus fetch for a pre-built server-side index and a thinner per-query RPC, at the cost of
losing MMR and the in-process engine's pre-fusion temporal-window filter (only post-fusion recency
boost is available server-side, see section 5) — a client comparing the two should expect different
numbers, not identical ones, since the algorithms genuinely differ past fusion. Two further
differences in the eval harness itself: the snapshot indexes every row (turn and round twins)
regardless of `--granularity`, so the in-process engine's `turn`/`round` corpus filter becomes a
post-hoc filter over the server's top-40 hits, and Bleve's BM25 flavour (section 5) is not the
hand-rolled Okapi the ablation numbers were measured with.

---

## 9. Honesty note

No benchmark numbers for this engine are reported anywhere in this repo. `README.md`'s measured
LoCoMo / LongMemEval-S results are for the in-process engine only. Measuring the snapshot engine
end to end (`./scripts/run-snapshot.sh --dataset ...` with the real embedder, which runs
`eval --engine snapshot`) and
comparing Recall@5/@10 against the in-process numbers is the natural next step before trusting this
path for anything beyond the fixtures gate.
