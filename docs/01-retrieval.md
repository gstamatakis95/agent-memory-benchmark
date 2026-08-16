# Implementation-Ready System Design: A Go gRPC Agent-Memory System Optimized for LoCoMo and LongMemEval (Embedding-Only, No LLM)

## TL;DR
- Build a **retrieval-optimized** system: because no LLM may answer, the only benchmark signal you can move is **evidence retrieval quality** (Recall@k, NDCG@k, MRR), so the entire design targets maximizing recall of gold evidence sessions/turns. The highest-value non-LLM recipe is **hybrid BM25 + dense (nomic-embed-text-v1.5) fused with Reciprocal Rank Fusion (RRF, k=60)**, indexed at **round granularity**, with **rule-based time-aware boosting** and **per-session diversification via MMR**.
- **pgvector counts as acceptable** (it is a Postgres extension, not an embedded database); but since the client fetches ALL memories and ranks in-process, pgvector is optional. Design both paths: (a) the mandated path — Postgres-as-store with `bytea`/`float4[]` embeddings + `tsvector` GIN, client-side brute-force ranking in Go; (b) an optional pgvector `vector(768)` HNSW index for server-side ANN parity checks.
- Realistic targets on LongMemEval-S with client-side hybrid retrieval and nomic v1.5: **Recall@5 ≈ 0.90–0.96, Recall@10 ≈ 0.96–0.99** (matching published all-MiniLM BM25+vector numbers); pure BM25 ≈ 0.86 R@5. The single biggest lever is adding dense vectors to BM25 (+~9pp R@5); the biggest risk is **omitting nomic task prefixes**, which materially degrades retrieval.

## Key Findings

### 1. What "optimizing for the benchmark" means without an LLM
Both benchmarks are ultimately QA benchmarks scored by an LLM judge (GPT-4o) on generated answers. With no LLM allowed in the pipeline you cannot produce the final answer, so you optimize the **retrieval stage** — the component both papers and every memory-system report treat as the dominant bottleneck. Both benchmarks ship **evidence annotations** that make retrieval directly measurable:
- **LoCoMo**: each `qa` item has an `evidence` list of dialog IDs (`dia_id`) that contain the answer, plus a `category`. → compute Recall@k / MRR / NDCG over retrieved `dia_id`s.
- **LongMemEval**: each question has `answer_session_ids`, and evidence turns carry a `has_answer` boolean flag. → compute session-level and turn-level Recall@k / NDCG@k. The official code (`src/evaluation/print_retrieval_metrics.py`) reports Recall@k and NDCG@k, and **skips the 30 abstention instances** for retrieval (they refer to non-existent events with no ground-truth location).

This is exactly how independent reproductions score "retrieval-only" runs, and they explicitly disclaim these are not official QA scores.

### 2. Benchmark formats (verified field names)

**LoCoMo** (`snap-research/locomo`, `./data/locomo10.json`, ACL 2024, arXiv 2402.17753, CC BY-NC 4.0). 10 conversations. Each sample:
- `sample_id`
- `conversation`: contains `speaker_a`, `speaker_b`; sessions as `session_1`, `session_2`, … and timestamps as `session_1_date_time`, `session_2_date_time`, …. Each session is a list of turns; each turn has `speaker`, `dia_id`, `text`, and optionally `img_url`, `blip_caption`, `query`.
- `observation` (generated), `session_summary` (generated), `event_summary` (annotated) — the first two are LLM-generated (GPT-3.5) and **must NOT be used** in a no-LLM pipeline. Use raw dialog turns only.
- `qa`: list of `{question, answer, category, evidence}` where `evidence` is a list of `dia_id`s. Categories: single-hop, multi-hop, temporal, open-domain, adversarial. Standard practice excludes the adversarial category when reporting QA metrics.

**LongMemEval** (`xiaowu0162/LongMemEval`, ICLR 2025, arXiv 2410.10813). Files: `longmemeval_s.json` (~115k tokens/history, ~40–50 sessions), `longmemeval_m.json` (~500 sessions, ~1.5M tokens), `longmemeval_oracle.json` (evidence only). Each item:
- `question_id` (if it ends with `_abs` it is an **abstention** question), `question_type`, `question`, `answer`, `question_date`.
- `question_type` ∈ {single-session-user, single-session-assistant, single-session-preference, temporal-reasoning, knowledge-update, multi-session}. Distribution: 70 single-session-user, 56 single-session-assistant, 30 single-session-preference, 133 multi-session, 78 knowledge-update, 133 temporal-reasoning.
- `haystack_session_ids` (sorted by timestamp for `_s`/`_m`, unsorted for oracle), `haystack_dates` (timestamps), `haystack_sessions` (list of sessions; each session is a list of turns `{"role": "user"/"assistant", "content": "..."}`). Turns that contain evidence carry a `has_answer: true` flag.
- Download via HuggingFace: `xiaowu0162/longmemeval` (original) or `xiaowu0162/longmemeval-cleaned` (`longmemeval_s_cleaned.json`, ~264 MB). May require `huggingface-cli` login / license acceptance.

### 3. Ranked non-LLM techniques with evidence

The LongMemEval paper's own ablations (default retriever **Stella V5 1.5B**; also tested BM25 and Contriever; metrics Recall@k/NDCG@k at k=5,10 on LongMemEval_M) give quantitative guidance. Several of the paper's biggest wins used an LLM (fact extraction, time-range inference); the **non-LLM-portable** subset is what we adopt, substituting classical equivalents where the paper used an LLM.

| Rank | Technique | Non-LLM? | Evidence of impact | Applies to |
|---|---|---|---|---|
| 1 | **Dense embeddings (nomic v1.5) added to BM25 via RRF** | ✅ | +~9pp R@5 over BM25-only (86.2%→95.2%) in an independent LongMemEval-S reproduction; dense > BM25 in the paper (Session, key=value: Contriever R@5 0.723 vs BM25 0.634) | all types |
| 2 | **Round-level granularity** (user+assistant pair) as the indexed unit | ✅ | LongMemEval Finding 1 (§5.2): "Instead of sessions, round is the best granularity for storing and utilizing the interactive history. While further compression into individual user facts harms overall performance due to information loss, it improves the multi-session reasoning performance." | multi-session, single-session |
| 3 | **Correct nomic task prefixes** (`search_document:` / `search_query:`) | ✅ | HF model card: "the text prompt must include a task instruction prefix… embed your documents as `search_document: <text here>` and embed your user queries as `search_query: <text here>`." | all types |
| 4 | **Time-aware indexing + query-time temporal filtering/boost** | ⚠️ paper used LLM for time-range; we use regex/rule-based date parsing | LongMemEval §5.4/Table 4: temporal-reasoning recall improves "by an average of 11.4% when using rounds as the value and by 6.7% when using sessions as the value" (camera-ready reports 11.3%/6.8%); overall "7%∼11%" — **but only with a strong extractor; a weak extractor (Llama-3.1-8B) reduced recall** by hallucinating time ranges | temporal-reasoning |
| 5 | **Key expansion** | ⚠️ paper used LLM facts; we substitute **extractive keyphrases (TextRank/RAKE/YAKE, TF-IDF)** | LongMemEval §5.3: fact-augmented key expansion "yielded an average improvement of 4% in retrieval metrics and 5% in final accuracy across all models" (Finding 2: "4% higher recall@k"). Classical keyphrase is a weaker but LLM-free analog | multi-session, knowledge-update |
| 6 | **Per-session diversification / MMR** (cap hits per session) | ✅ | Improves evidence coverage for multi-session questions where gold spans several sessions | multi-session |
| 7 | **Speaker + timestamp prefix injection** into embedded text | ✅ | Standard practice; gives the encoder temporal/speaker context ("[2023-05-30] user: …") | temporal, single-session-assistant |
| 8 | **Stemming/stopword tokenization for BM25** | ✅ | Independent repro: "keyword search with Porter stemming and synonym expansion is surprisingly effective" (BM25 alone 86.2% R@5) | single-hop, knowledge-update |

**Abstention (`_abs`) questions**: excluded from retrieval scoring by the official script — do not try to "retrieve" for them. In a retrieval-only system, handle abstention via a **score-threshold heuristic**: if the top fused score is below a calibrated floor, emit "no evidence found."

### 4. Published baseline numbers for sanity-checking

- **LongMemEval-S, retrieval-only, session granularity** (independent repro, all-MiniLM-L6-v2): BM25+Vector R@5 **95.2%**, R@10 **98.6%**, R@20 **99.4%**, NDCG@10 **87.9%**, MRR **88.2%**; BM25-only R@5 **86.2%**; vector-only (MemPalace/ChromaDB) R@5 **96.6%**. By type (hybrid): knowledge-update 98.7%, multi-session 97.7%, single-session-assistant 96.4%, temporal-reasoning 95.5%, single-session-user 90.0%, single-session-preference 83.3% (hardest).
- **LongMemEval paper, LongMemEval_M, Table 8** (key=value): Session BM25 R@5 0.634 / Contriever 0.723 / Stella V5 0.720; Round BM25 0.472 / Contriever 0.589 / Stella V5 0.660. With fact key expansion (key=value+fact): Session Contriever R@5 0.762 / R@10 0.862; Session Stella V5 R@5 0.732 / R@10 0.862. Biggest single temporal jump (Table 3, Round, key=value+fact): R@10 0.550 → **0.722** with GPT-4o time-aware query expansion.
- **Another independent system (LETHE)**, LongMemEval-S session granularity, R@5: knowledge-update 0.987, multi-session 0.940, single-session-assistant 0.964, single-session-preference 0.900, single-session-user 0.900.
- **LoCoMo QA** (LLM-judged, for context only): Mem0 J≈67 single-hop; Zep disputed 75.14% vs Mem0's 58.44% for Zep — methodology is contested, so treat LoCoMo end-to-end numbers cautiously. On retrieval, LoCoMo turn-level recall is far harder than session-level (HippoRAG2: session R@3 75.53 vs turn R@3 27.80).
- **nomic-embed-text-v1.5**: per the Nomic technical report (arXiv 2402.01613) / AWS Marketplace listing, "Nomic Embed outperforms on short context (MTEB 62.39 vs 62.26) and long context (LoCo 85.53 vs 82.40) benchmarks" vs OpenAI text-embedding-3-small (ada-002 = 60.99 MTEB / 82.40 LoCo). Matryoshka truncation: per Nomic ("Nomic Embed Matryoshka"), v1.5 "outperforms text-embedding-3-small at both 512 and 768 embedding dimensions… At an embedding dimension of 512, we outperform text-embedding-ada-002 while achieving a 3x memory reduction."

Expectation: with nomic v1.5 (stronger than MiniLM) + hybrid + round granularity, **R@5 in the low-to-mid 0.90s on LongMemEval-S** is a reasonable target. If BM25-only ≈ 0.86 but hybrid doesn't beat it by roughly +9pp, something (most likely the prefix) is wrong.

## Details

### 4.0 High-level architecture diagrams

**System overview**

```
                 ┌────────────────────────────── LOCAL MACHINE ──────────────────────────────┐
                 │                                                                            │
 LoCoMo JSON ─┐  │  ┌──────────────────┐   UploadMemories (client-stream)   ┌─────────────┐  │
 LongMemEval ─┼──┼─▶│  Go Client        │ ─────────────────────────────────▶│  Go gRPC     │  │
 (datasets)   │  │  │  (harness/ingest, │                                   │  Server      │  │
              │  │  │  retrieval, eval) │ ◀───────────────────────────────── │              │  │
              │  │  └──────┬───────────┘   FetchAllMemories (server-stream) └──┬───────┬───┘  │
              │  │         │ embed query only                                  │       │      │
              │  │         │ (search_query:)                     post-process  │       │ COPY │
              │  │         ▼                                     (embed docs,  │       ▼      │
              │  │  ┌──────────────────┐                         tokenize,     │  ┌─────────┐ │
              │  │  │ nomic-embed v1.5 │◀────────────────────────dates, dedup)─┘  │ Postgres│ │
              │  │  │ (internal HTTP,  │   search_document: batches               │ 16      │ │
              │  │  │ OpenAI-compat)   │                                          │ (+opt.  │ │
              │  │  └──────────────────┘                                          │ pgvector│ │
              │  │                                                                └─────────┘ │
              │  └────────────────────────────────────────────────────────────────────────────┘
              └─▶ (evidence annotations feed the eval module directly — never the retriever)
```

**Server-side post-processing pipeline (no LLM)**

```
 raw Memory (text, speaker, session date)
        │
        ▼
 ┌─────────────┐   ┌──────────────┐   ┌────────────────┐   ┌───────────────┐
 │ Normalize    │──▶│ Tokenize     │──▶│ Date parse      │──▶│ Round assembly │
 │ NFKC, lower  │   │ stopwords,   │   │ → TIMESTAMPTZ   │   │ (user+asst     │
 │ whitespace   │   │ Snowball stem│   │ UTC             │   │  pair)         │
 └─────────────┘   └──────────────┘   └────────────────┘   └───────┬───────┘
                                                                    │
        ┌───────────────────────────────────────────────────────────┘
        ▼
 ┌──────────────────────┐   ┌──────────────┐   ┌─────────────────────────┐
 │ Embed batches         │──▶│ Dedup         │──▶│ pgx CopyFrom → memories │
 │ "search_document:     │   │ SHA-256, per- │   │ (text, tsv, lexemes,    │
 │  [date] speaker: txt" │   │ conversation  │   │  embedding BYTEA, ts)   │
 │ L2-normalize          │   │ scope only    │   └─────────────────────────┘
 └──────────────────────┘   └──────────────┘
```

**Client-side retrieval (per query)**

```
                       question ──┬─▶ embed "search_query: q" ──▶ ┌────────────────┐
                                  │                               │ Dense top-N     │──┐
                                  │                               │ (cosine, in-RAM)│  │
   in-RAM memory store            │                               └────────────────┘  │   ┌─────────┐
   (all rows via FetchAll):       └─▶ tokenize/stem ────────────▶ ┌────────────────┐  ├──▶│ RRF      │
   ┌─────────────────────┐                                        │ BM25 top-N      │──┘   │ k=60     │
   │ []float32 arena 768d │                                        │ (in-mem index)  │      └────┬────┘
   │ BM25 inverted index  │                                        └────────────────┘           │
   │ timestamps, sessions │                                                                     ▼
   └─────────────────────┘        rule-based date extraction ──▶ ┌─────────────────────────────────┐
                                  (dateparse / when)             │ Temporal boost/filter (×1.3 or   │
                                                                 │ range filter w/ fallback)        │
                                                                 └───────────────┬─────────────────┘
                                                                                 ▼
                                                                 ┌─────────────────────────────────┐
                                                                 │ MMR (λ=0.7) + per-session cap    │
                                                                 └───────────────┬─────────────────┘
                                                                                 ▼
                                                                     top-k ids ──▶ eval (Recall@k,
                                                                     (k=10)        NDCG@k, MRR;
                                                                                   score<τ → abstain)
```

**One-click run/eval sequence (`run.sh`)**

```
 run.sh
   │ docker compose up -d postgres        # postgres:16 (pgvector image optional)
   │ wait-for healthy ──▶ goose up        # migrations
   │ datasets/download.sh                 # LoCoMo (GitHub raw) + LongMemEval (HF CLI)
   │ docker compose up -d server          # gRPC server + embedding endpoint config
   │
   ├─▶ client ingest ──UploadMemories──▶ server ──post-process──▶ Postgres
   │
   ├─▶ client eval   ──FetchAllMemories─▶ (load to RAM)
   │        └─ for each qa/question: retrieve top-k ──▶ compare vs evidence
   │
   └─▶ print metrics table: Recall@5/@10, NDCG@10, MRR × category/question_type
```

### 4.1 nomic-embed-text-v1.5 usage (critical correctness details)
- **Task prefixes are mandatory.** Embed corpus text as `search_document: <text>` and queries as `search_query: <text>`. The HF model card states the prompt "must include a task instruction prefix." This is the highest-ROI correctness detail in the whole system.
- **Dimensions**: 768 native, Matryoshka-truncatable to 512/256/128/64. For maximum quality keep 768; for RAM/speed at LongMemEval_M scale, 512 is a safe tradeoff (Nomic reports it still beats text-embedding-3-small at 512 dims with ~3x memory reduction vs 768).
- **Matryoshka procedure** (if truncating): take the full output, apply `layer_norm`, slice to the first N dims, then L2-normalize. If using full 768 dims, just L2-normalize.
- **Normalization & similarity**: L2-normalize all vectors so cosine similarity = dot product. This makes client-side ranking a single dot product and makes pgvector inner-product (`<#>`) and cosine (`<=>`) equivalent.
- **Max sequence length** 8192 tokens (note: some servers / llama.cpp default to 2048 — set context explicitly). Rounds/turns are far shorter, so truncation is not a concern.
- **Serving**: expose a configurable OpenAI-compatible `/v1/embeddings` endpoint. HuggingFace TEI, Infinity (`michaelf34/infinity … --model-id nomic-ai/nomic-embed-text-v1.5`), vLLM, and LocalAI all serve nomic v1.5 with an OpenAI-compatible API. The client POSTs `{"model": ..., "input": [...]}` and reads `data[].embedding`. Provide an `embedding-mock` container for offline/CI runs that returns deterministic hashed pseudo-embeddings.

### 4.2 Cross-encoder / reranker question
A non-generative cross-encoder reranker is **not** an "embedding model" — it is a separate scoring transformer. Per the user's constraint (embedding model only), **exclude it by default.** Note as an option: TEI/Infinity can serve a `/rerank` cross-encoder, and independent memory-retrieval reports (e.g., SmartSearch, Nautilus-Compass) show rerankers add measurable MRR/precision. If the user later relaxes the constraint to "any non-generative model," a bge-reranker over the RRF top-50 is the highest-value addition. Until then, rely on classical fusion + MMR.

### 4.3 Retrieval algorithm (client-side, in Go)

Pipeline per query:
1. **Build query variants**: `q_raw` and `search_query: q_raw`. Optionally add a lightweight classical **query expansion** (append top TF-IDF/keyphrase terms; no LLM).
2. **Dense retrieval**: embed `search_query: q_raw`; compute cosine (dot on normalized vecs) against all in-RAM memory vectors; take top-N (N≈100–200).
3. **Lexical retrieval (BM25)**: query an in-memory BM25 index (Bluge/Bleve, or a compact custom BM25) over normalized tokens; take top-N.
4. **RRF fusion**: `RRF(d) = Σ_r 1/(k + rank_r(d))`, k=60. Documents missing from a list contribute 0. RRF was introduced by Cormack, Clarke & Büttcher (SIGIR 2009, doi:10.1145/1571941.1572114), where "the constant k mitigates the impact of high rankings by outlier systems." Consider **k=10–30** for the small per-question haystacks of ~50 sessions, since with fewer docs rank differences are more meaningful; k=60 is the TREC-scale default.
5. **Temporal boost/filter** (for temporal-reasoning): rule-based date extraction from the question — Go `araddon/dateparse` for absolute dates (use `ParseStrict` for ambiguous mm/dd vs dd/mm), `olebedev/when` for relative expressions like "last month." If a date/range is found, multiply fused scores of in-range memories by a boost (e.g., ×1.3) or hard-filter to the range with a fallback to unfiltered if too few candidates survive.
6. **MMR diversification**: greedily select the final top-k maximizing `λ·sim(d,q) − (1−λ)·max_{d'∈S} sim(d,d')` (Carbonell & Goldstein 1998), with λ≈0.7. Use it to **cap hits per session** so multi-session evidence isn't crowded out by one dominant session.
7. **Adaptive-k / abstention**: return fixed k=10 by default; if the top fused/cosine score is below a calibrated threshold, flag abstention.

RRF worked example (k=60): a doc at rank 3 in BM25 and rank 7 in dense scores 1/63 + 1/67 ≈ 0.0308.

### 4.4 gRPC + protobuf service design

```proto
syntax = "proto3";
package agentmem.v1;

message Memory {
  string id = 1;                       // stable hash of (conversation_id, dia_id/turn)
  string conversation_id = 2;          // LoCoMo sample_id / LME question_id
  string session_id = 3;               // "session_1" / haystack session id
  string turn_id = 4;                  // dia_id (LoCoMo) or synthesized turn index
  string speaker = 5;                  // speaker_a/b or user/assistant
  string text = 6;                     // raw dialog text
  string normalized_text = 7;          // server-filled: lowercased, unicode-normalized
  int64  timestamp_unix = 8;           // parsed from session_N_date_time / haystack_dates
  repeated float embedding = 9;        // server-filled (768 floats), or use bytes below
  bytes  embedding_bytes = 10;         // optional packed float32 for compactness
  repeated string lexemes = 11;        // server-filled BM25 tokens
  string granularity = 12;             // "turn" | "round" | "session"
  bool   has_answer = 13;              // eval-only ground-truth passthrough (LME)
  map<string,string> metadata = 14;    // category, question_type, dataset, etc.
}

message UploadAck { uint64 accepted = 1; uint64 deduped = 2; string batch_id = 3; }
message FetchRequest { string conversation_id = 1; string dataset = 2; }
message HealthRequest {}
message HealthReply { bool ok = 1; string version = 2; uint64 memory_count = 3; }

service MemoryService {
  rpc UploadMemories(stream Memory) returns (UploadAck);        // client-streaming
  rpc FetchAllMemories(FetchRequest) returns (stream Memory);   // server-streaming
  rpc Health(HealthRequest) returns (HealthReply);
}
```

Design notes: use client-streaming for upload (backpressure-friendly for 50k–500k rows) and server-streaming for fetch (avoids one huge message; set `grpc.MaxCallRecvMsgSize` generously and stream in batches). Ship raw text on upload; the **server** fills `normalized_text`, `lexemes`, `embedding`, `timestamp_unix` during post-processing so the client harness stays thin. Use `protoc` with `protoc-gen-go` + `protoc-gen-go-grpc`.

### 4.5 Postgres schema (mandated store)

```sql
CREATE TABLE memories (
  id              TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  session_id      TEXT NOT NULL,
  turn_id         TEXT,
  speaker         TEXT,
  text            TEXT NOT NULL,
  normalized_text TEXT,
  ts              TIMESTAMPTZ,
  granularity     TEXT NOT NULL DEFAULT 'round',
  embedding       BYTEA,               -- packed 768 float32 (little-endian)
  -- embedding_vec vector(768),        -- OPTIONAL pgvector variant
  tsv             tsvector,
  has_answer      BOOLEAN DEFAULT FALSE,
  metadata        JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX ix_mem_conv   ON memories (conversation_id);
CREATE INDEX ix_mem_ts     ON memories (ts);
CREATE INDEX ix_mem_tsv    ON memories USING GIN (tsv);
-- pgvector optional:
-- CREATE EXTENSION IF NOT EXISTS vector;
-- CREATE INDEX ix_mem_hnsw ON memories USING hnsw (embedding_vec vector_cosine_ops) WITH (m=16, ef_construction=64);
```

- **Embedding storage without pgvector**: store as `BYTEA` (packed float32) or `float4[]`. `BYTEA` is more compact and trivially decoded in Go. Each 768-dim vector = 3072 bytes (pgvector's own accounting is `4 * dims + 8` bytes).
- **pgvector**: acceptable (it's a Postgres extension). The **2000-dim HNSW limit** does not affect 768. NULL/zero vectors are not indexed. If you use both pgvector ANN and client-side brute force, expect small ranking differences (ANN is approximate) — reconcile with exact search or a high `ef_search` for parity tests; pgvector ≥0.8.0 supports iterative index scans (`hnsw.iterative_scan`) to avoid under-filling top-k under selective filters.
- `tsvector` + GIN enables optional **server-side lexical prefiltering** (`ts_rank_cd`), useful if you ever push retrieval server-side; for the mandated client-side path it's a convenience.

### 4.6 Server-side post-processing pipeline (no LLM)
1. **Normalize**: Unicode NFKC, lowercase, collapse whitespace → `normalized_text`.
2. **Tokenize for BM25**: unicode-aware tokenizer, stopword removal, optional Snowball stemming (`kljensen/snowball`) → `lexemes`; also populate `tsv` via `to_tsvector('english', normalized_text)`.
3. **Date extraction/normalization**: parse `session_N_date_time` (LoCoMo) / `haystack_dates` (LME) into `TIMESTAMPTZ`; store as UTC. Inject a human-readable date + speaker prefix into the text that gets embedded: `"[2023-05-30] user: <text>"`.
4. **Round assembly**: group consecutive (user, assistant) turns into a round; also keep turn-level rows for LoCoMo turn-level evidence scoring. Store `granularity`.
5. **Embed**: batch calls (64–256 texts) to the internal `/v1/embeddings` endpoint with the `search_document:` prefix; exponential-backoff retries; L2-normalize; pack to `BYTEA`.
6. **Dedup**: exact SHA-256 of `normalized_text` for exact dupes; optional MinHash/SimHash for near-dupes. **Scope dedup per `conversation_id`/`question_id`**, not globally — LongMemEval reuses sessions across questions, and global dedup would leak/merge evidence across questions (one repro removed 51k duplicate instances to prevent leakage).
7. **Batch insert**: `pgx v5` `CopyFrom` (COPY protocol); batches of 5k–50k rows; `pgxpool` with warm connections (`MinConns`, `HealthCheckPeriod`). CopyFrom beats multi-row INSERT beyond ~5 rows and sustains 100k+ rows/s.

### 4.7 Client-side retrieval in Go
- **Load**: stream `FetchAllMemories`, decode `BYTEA`→`[]float32`, hold vectors in a contiguous `[]float32` arena (row-major, 768 stride) for cache-friendly scans.
- **Brute-force cosine**: dot product over normalized vecs; tight loop or `gonum` BLAS. For LongMemEval_S (~50 sessions / a few hundred rounds per question) this is microseconds; even LongMemEval_M (~500 sessions) is trivial. For the whole corpus (~50k–100k rows) a single-thread dot scan is ~tens of ms; parallelize with goroutines over row ranges if needed.
- **BM25**: Bluge (in-memory Directory, BM25 scoring built in) or a compact custom BM25 over `lexemes`.
- **Fuse (RRF)** → **temporal boost** → **MMR** → top-k, as in §4.3.

### 4.8 Eval harness (pragmatic recommendation)
Compute retrieval metrics **in Go** directly from evidence annotations — self-contained, fast, no Python:
- **LoCoMo**: for each `qa`, gold = `evidence` dia_ids; retrieved = top-k turn/round ids; report Recall@k, MRR, NDCG@k by `category`.
- **LongMemEval**: gold = `answer_session_ids` (session-level) and `has_answer` turns (turn-level); report Recall@k / NDCG@k by `question_type`; **skip `_abs`** for retrieval; separately report an abstention accuracy from the score-threshold heuristic.
  Optionally also **emit JSONL retrieval logs** in the official LongMemEval format so `src/evaluation/print_retrieval_metrics.py` can cross-validate (the official script takes granularity `session`|`turn`). Recommend Go-native metrics as the primary path, official script as an optional check.

### 4.9 Repo layout & one-click run

```
agent-memory/
├─ proto/agentmem/v1/memory.proto
├─ cmd/server/main.go          # gRPC server + post-processing
├─ cmd/client/main.go          # ingest harness + retrieval + eval
├─ internal/
│  ├─ pipeline/  (normalize, tokenize, dates, embed, dedup)
│  ├─ store/     (pgx v5 CopyFrom, queries)
│  ├─ retrieve/  (cosine, bm25, rrf, mmr, temporal)
│  ├─ embed/     (OpenAI-compatible client + mock)
│  └─ eval/      (recall/ndcg/mrr, per-category)
├─ migrations/   (goose or golang-migrate SQL)
├─ datasets/     (download scripts)
├─ docker-compose.yml          # postgres:16 (+pgvector image), server, embedding-mock
├─ Makefile
└─ run.sh
```

`run.sh` flow: `docker compose up -d postgres` → wait healthy → run migrations → download datasets (LoCoMo JSON from GitHub raw; LongMemEval from HuggingFace via `huggingface-cli`, noting possible license acceptance) → start server (`docker compose up server`) → run client to ingest via gRPC → run eval → print a metrics table (Recall@5/10, NDCG@10, MRR by category/question_type). Use `docker compose up embedding-mock` for offline CI. Migration tool: **goose** for simplicity (SQL migrations with `-- +goose Up/Down`, runs each in a transaction by default) or **golang-migrate** for wider adoption; either is fine. For large index builds, wrap `CREATE INDEX CONCURRENTLY` and set statement timeouts.

## Recommendations

**Stage 0 — Correctness floor (do first).** Wire nomic prefixes correctly (`search_document:`/`search_query:`), L2-normalize, and confirm BM25-only ≈ 0.86 R@5 and hybrid ≈ 0.95 R@5 on LongMemEval-S. If hybrid doesn't beat BM25 by ~+9pp, the prefix or normalization is wrong. **Benchmark to advance:** hybrid R@5 ≥ 0.93.

**Stage 1 — Granularity & fusion.** Index at **round** granularity (keep turn rows for LoCoMo turn-level scoring); implement RRF (k=60, try k=10–30 for the ~50-session haystacks). **Benchmark:** LongMemEval-S R@5 ≥ 0.94, R@10 ≥ 0.97.

**Stage 2 — Temporal & diversification.** Add rule-based date extraction + temporal boost/filter for temporal-reasoning; add MMR/per-session caps for multi-session. **Benchmark:** temporal-reasoning R@5 ≥ 0.95, multi-session R@5 ≥ 0.95. If temporal boosting *lowers* recall (as weak extractors did in the paper), gate it behind a high-confidence date-parse and always keep an unfiltered fallback.

**Stage 3 — Scale & polish.** Validate LongMemEval_M (~500 sessions) latency; parallelize cosine scans; consider Matryoshka-512 to halve RAM. Add extractive keyphrase key expansion (TextRank/YAKE) as an LLM-free analog of fact keys. **Benchmark:** M-scale end-to-end retrieval under target latency with R@5 within ~2pp of S-scale.

**Thresholds that change the plan:** if the user relaxes "embedding-only" to "any non-generative model," add a bge cross-encoder reranker over the RRF top-50 (biggest remaining MRR lever). If single-session-preference stays the laggard (~0.83, as in reproductions), that is expected — preferences need implicit-statement understanding that non-LLM retrieval can't fully capture; don't over-tune for it.

## Caveats
- **These are retrieval metrics, not official QA scores.** Report them as such; official LoCoMo/LongMemEval numbers require an LLM reader + judge, which is out of scope by constraint.
- **nomic prefix omission** is the most common and most damaging bug — verify empirically against the BM25 vs hybrid gap.
- **LongMemEval cross-question session reuse**: dedup within `conversation_id`/`question_id` scope only, or you'll leak/merge evidence across questions.
- **Temporal parsing pitfalls**: timezones and ambiguous mm/dd vs dd/mm (`dateparse` defaults to mm/dd; use `ParseStrict` where possible). Normalize everything to UTC; LoCoMo timestamps are free-form strings.
- **LoCoMo speaker ambiguity**: it's user↔user (two named speakers `speaker_a`/`speaker_b`), not user/assistant; don't assume an "assistant" role. Adversarial category has no evidence → exclude from retrieval scoring.
- **pgvector vs client-side parity**: pgvector ANN is approximate; for exact parity in tests use exact scan or raise `ef_search`/enable iterative scans. The mandated path is client-side exact ranking, so pgvector is strictly optional.
- **Benchmark-war context**: LoCoMo end-to-end vendor numbers (Zep vs Mem0) are disputed; use them only as loose context, not targets.
- **Do not use LoCoMo's `observation`/`session_summary` fields** — they are LLM-generated and violate the no-LLM constraint (and would be "cheating" relative to raw-dialog retrieval).
- **Ablation-number provenance**: LongMemEval retrieval tables (Tables 2, 3, 8) are on LongMemEval_**M**, and the arXiv v1 vs ICLR camera-ready differ slightly on a few figures (e.g., temporal recall 11.4%/6.7% in v1 vs 11.3%/6.8% in camera-ready); the paper's default retriever is Stella V5, so nomic v1.5 numbers will differ but should track the same trends.
