# Agent memory system with gRPC and LoCoMo evaluation

**Created:** 8/15/2026 11:05:38  
**Updated:** 8/16/2026 11:48:15  
**Exported:** 8/16/2026 11:52:01  
**Link:** [https://claude.ai/chat/303174a3-b4d2-4e2a-bdb4-97d8683cf216](https://claude.ai/chat/303174a3-b4d2-4e2a-bdb4-97d8683cf216)

## User:

> 8/15/2026 11:05:42

I want you to help me work on this project. I'm trying to create a server client go application that performs the following. A grpc client uploads Agent memories, produced by a local harness. The server can perform any post processing server side amd store results in a postgres. The client fetches locally all memories and finds the best top k ones. We are using locomo and long mem eval memory benchmark to evaluate this system.  Don't use embedded databases but you have access to aj embedding model only. Optimize for benchmark results

## Assistant:

> 8/15/2026 11:05:50

> Deliberated architectural ambiguities for memory retrieval system.
>
> - Deliberated architectural ambiguities for memory retrieval system.
> - **Done**

Nice project — a memory system for agents with LoCoMo and LongMemEval as the yardstick is a solid setup. Before I dig into research on architecture and benchmark-optimization strategies, a few quick questions:

1. What deliverable do you want first: a full system design doc, working Go code scaffold (client + server + Postgres schema), or a research report on what techniques top LoCoMo/LongMemEval systems use?
2. Which embedding model do you have access to (e.g., OpenAI, Voyage, Cohere API, or a local model behind an endpoint)? And should embedding happen server-side during post-processing or client-side?
3. Since retrieval is client-side over *all* fetched memories, are you okay with brute-force cosine top-k in Go, or do you want the server to also support filtering/reranking (e.g., an LLM reranker) to squeeze out benchmark points?

## User:

> 8/15/2026 11:07:55

1. full system and 1 click run/eval potentially via sh script.
2. nomic 1.5 768 dimensions, hosted internally.
3. if you can squeeze benchmark points yes.

Don't use agents or llms otherwise

## Assistant:

> 8/15/2026 11:16:27

Got it — full Go client/server system, nomic-embed-text-v1.5 (768d) hosted internally, no LLMs/agents in the retrieval loop, and non-LLM techniques to maximize LoCoMo and LongMemEval scores. Diving into the research now.

> Used tool
>
> - launch_extended_search_task
> - **Done**

Your implementation-ready design for a Go gRPC agent-memory system optimized for LoCoMo and LongMemEval is ready. It covers a retrieval-first architecture (hybrid BM25 + nomic-embed-text-v1.5 fused via RRF, round-level indexing, temporal boosting, and MMR diversification), verified benchmark data formats and evidence fields, a ranked table of non-LLM techniques with quantitative impact, published baseline numbers for sanity-checking (targeting R@5 ≈ 0.90–0.96 on LongMemEval-S), a full gRPC/protobuf service definition, Postgres schema with optional pgvector path, server-side post-processing pipeline, Go client-side retrieval details, an eval harness plan, repo layout with one-click run, and a staged rollout with per-stage benchmark gates plus caveats on common pitfalls (nomic prefix omission, cross-question session reuse, temporal parsing, and LoCoMo speaker roles).

**Go gRPC Agent-Memory System for LoCoMo and LongMemEval: Embedding-Only Retrieval Design**

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

---

**References:**

1. [GitHub - xiaowu0162/LongMemEval: Benchmarking Chat Assistants on Long-Term Interactive Memory (ICLR 2025) · GitHub (github.com)](https://github.com/xiaowu0162/longmemeval)
2. [agentmemory/benchmark/LONGMEMEVAL.md at main · rohitg00/agentmemory (github.com)](https://github.com/rohitg00/agentmemory/blob/main/benchmark/LONGMEMEVAL.md)
3. [Control-Plane Placement Shapes Forgetting: An Architectural Study of Agent Memory Across Thirteen System Configurations (arxiv.org)](https://arxiv.org/pdf/2606.15903)
4. [Memory Papers - LLM & Agent Memory Research Library (memorypapers.org)](https://memorypapers.org/)
5. [Zep vs Mem0: Benchmarks, Pricing, and When to Use Each (atlan.com)](https://atlan.com/know/zep-vs-mem0/)
6. [nomic-ai/nomic-embed-text-v1.5 · Hugging Face (huggingface.co)](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5)
7. [The guide to nomic-embed-text-v1.5 - Zilliz (zilliz.com)](https://zilliz.com/ai-models/nomic-embed-text-v1.5)
8. [nomic-embed-text-v1.5-GGUF: Text-to-Text model — overview, use cases, alternatives (aimodels.fyi)](https://www.aimodels.fyi/models/huggingFace/nomic-embed-text-v1.5-gguf-nomic-ai)
9. [GitHub - hwdsl2/docker-embeddings: Docker image to run a self-hosted text embeddings and reranking server powered by Hugging Face TEI, with an OpenAI-compatible API. Supports BGE, Nomic, MiniLM and other models. Features optional reranking via cross-encoder models, API key auth, model management, offline/air-gapped mode, and persistent model cache. · GitHub (github.com)](https://github.com/hwdsl2/docker-embeddings)
10. [The Hitchhiker's Guide to Agentic AI: From Foundations to Systems (arxiv.org)](https://arxiv.org/pdf/2606.24937)
11. [pgvector, a guide for DBA - Part 2: Indexes (update march 2026) (dbi-services.com)](https://www.dbi-services.com/blog/pgvector-a-guide-for-dba-part-2-indexes-update-march-2026/)
12. [GitHub - pgvector/pgvector: Open-source vector similarity search for Postgres · GitHub (github.com)](https://github.com/pgvector/pgvector)
13. [dateparse package - github.com/araddon/dateparse - Go Packages (pkg.go.dev)](https://pkg.go.dev/github.com/araddon/dateparse)

## User:

> 8/15/2026 12:13:12

include some high level diagrams too

## Assistant:

> 8/15/2026 12:14:59

> Determined to augment artifact with visual diagrams.
>
> - Determined to augment artifact with visual diagrams.
> - **Done**

**Go gRPC Agent-Memory System for LoCoMo and LongMemEval: Embedding-Only Retrieval Design** ∙ Version 2

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

> - Check if a markdown skill exists before creating a .md file
> - Companion doc with high-level architecture diagrams for the agent-memory system design
> - Presenting file(s)...
> - **Done**

Added a diagrams companion doc with five views: system overview, server post-processing pipeline, client-side retrieval flow, gRPC sequence diagram, and the one-click run/eval flow.

Each diagram comes in two formats — ASCII (readable anywhere, including this doc) and Mermaid source you can paste straight into your repo README, where GitHub will render it natively. The sequence diagram in particular makes the split explicit: documents get the `search_document:` prefix server-side during ingest, queries get `search_query:` client-side at retrieval time — the asymmetry that's easiest to get wrong.

> File: [/mnt/user-data/outputs/architecture-diagrams.md](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2Farchitecture-diagrams.md)

## User:

> 8/15/2026 13:17:14

We are using a 3rd party grpc service that performs the embedding and not pg vector

## Assistant:

> 8/15/2026 13:17:30

> Recalibrated architecture understanding around gRPC embedding integration.
>
> - Recalibrated architecture understanding around gRPC embedding integration.
> - **Done**

Got it — embedding calls go over gRPC to a third-party service, and pgvector is out entirely (so Postgres stores vectors as plain `BYTEA`/`float4[]` and all ranking stays client-side, which was already the primary path). Before I rework the design around the gRPC embedder, two quick questions:

1. Is the embedding service a known implementation with a published proto (e.g., HuggingFace TEI's gRPC API, Triton/KServe inference protocol), or a custom/proprietary proto? If custom, do you have the `.proto`, or should I design a clean adapter interface around it?
2. Does it support batched requests (many texts per call) or unary single-text only? This drives the ingest pipeline's batching/concurrency design.

## User:

> 8/15/2026 13:32:30

1. it's a hosted nomic model, improvise.
2. it doesn't allow batching.

I want the approach to be easy to integrate. Right now we assume a memory is a byte blob stored on s3 and a reference plus metadata on postgres. I was thinking maybe we can add metadata rows that slowly, incrementally and reliably populate embeddings and other metadata that can be queried by the client. We also need to see how these rows can be filled asynchronously if necessary

## Assistant:

> 8/15/2026 13:44:05

> Architected async enrichment pipeline leveraging Postgres job queues.
>
> - Architected async enrichment pipeline leveraging Postgres job queues.
> - **Done**

This is a meaty architecture revision — S3-blob memories with Postgres metadata rows that get enriched asynchronously, plus a unary-only gRPC embedder. The design is clear enough to dive in, so I'll research reliable incremental-enrichment patterns (Postgres job-queue semantics, idempotency, backfill) and rework the system around it. I'll assume the S3 blob is the raw memory payload (conversation turns/text) and metadata rows are derived from it — noting that assumption in the design.

> Used tool
>
> - launch_extended_search_task
> - **Done**

Your revised system design report for the Go gRPC agent-memory system is ready. It covers the shift to S3/MinIO blob storage with Postgres as a queryable projection, and an async enrichment pipeline built on a hand-rolled `SELECT … FOR UPDATE SKIP LOCKED` job queue with lease-based crash recovery. The report includes complete Postgres DDL, an improvised unary embedder proto with a Go adapter, gRPC connection and retry configuration, exact worker SQL for the claim/reap/complete/fail phases, throughput math against the unary embedder bottleneck, wall-clock estimates for LoCoMo and LongMemEval_s, a global `embedding_cache` design, and pitfalls around lock recovery, NOTIFY durability, queue-table bloat, and S3/PG ordering. It closes with concrete recommendations (start at N=16 concurrency, enable the cache from day one, keep enrichment state as a side table) and caveats on assumed embedder latency and dataset-size source discrepancies.

**Go gRPC Agent-Memory System with S3 Blobs and Async Postgres-Queue Enrichment: Revised Design**

# Revised System Design: Go gRPC Agent-Memory System with S3 Blob Storage and Async Postgres-Queue Enrichment

## TL;DR
- **Build the enrichment pipeline as a hand-rolled Postgres `SELECT … FOR UPDATE SKIP LOCKED` job queue with a lease/heartbeat column for crash recovery, a `pending→processing→done/failed` state machine, and a global `embedding_cache` keyed on `content_hash` — this is the single highest-leverage decision** because the third-party embedder is unary-only (1 text = 1 RPC) and is the sole throughput bottleneck; the cache turns the LongMemEval_s worst case (~246,750 turn-texts across 500 questions' independently-compiled but pool-shared haystacks) into a far smaller number of unique embeddings.
- **Throughput against a unary endpoint is purely `concurrency / latency`**: use ONE gRPC `ClientConn` (HTTP/2 multiplexes concurrent streams over one connection — most servers cap this at 100 by default) bounded by an `errgroup.SetLimit(N)` worker pool plus a `golang.org/x/time/rate` limiter; at 32 concurrent calls × 50 ms you get ~640 embeds/s, finishing LoCoMo (~5,882 turns) in seconds and uncached LongMemEval_s in ~6–7 minutes — with the cache, minutes drop to well under that on re-runs.
- **Keep the entire client-side retrieval stack unchanged** (brute-force cosine + BM25 → RRF k=60 → temporal boost → MMR with per-session caps at round granularity). What changes: memories become S3 blobs referenced by Postgres rows; derived fields (normalized text, lexemes, parsed timestamp, embedding `bytea`) are populated asynchronously and stored in Postgres so the client needs only Postgres + no S3 fan-out at query time.

## Key Findings

1. **`FOR UPDATE SKIP LOCKED` is the correct claim primitive, but row locks alone are not crash-safe.** A worker that claims a row inside a transaction and then makes a slow gRPC call holds the lock for the whole call; if it crashes, the lock vanishes on connection drop but the row may be left in `processing` forever unless you add an explicit lease (`claimed_at`/`locked_until` + lease expiry) reclaimed by a reaper. The canonical fix (heartbeat/visibility-timeout) is well documented.
2. **Never hold a Postgres transaction open across the gRPC embed call.** Use the three-phase pattern: short *claim* transaction (SKIP LOCKED + set `processing`, `locked_until`) → work *outside* any transaction (S3 fetch, normalize, embed) → short *completion* transaction (write embedding, set `done`). This keeps lock/vacuum pressure minimal.
3. **Queue tables are high-churn and bloat fast.** Frequent status `UPDATE`s create dead tuples and index churn. Mitigate with `fillfactor` tuning to enable HOT updates, aggressive per-table autovacuum, and partial indexes (`WHERE status='pending'`) so pollers scan only live work.
4. **`LISTEN/NOTIFY` wakes workers with millisecond latency but is not durable** (payload < 8000 bytes; lost if no listener is connected at NOTIFY time). It must be paired with a polling sweep as the safety net.
5. **The transactional outbox pattern eliminates "lost memories":** insert the `memories` row and the enrichment job in the *same* transaction, so a memory can never exist without a pending enrichment job.
6. **S3 has had strong read-after-write consistency since December 1, 2020**, so "write blob first, then insert the referencing Postgres row" is safe with no eventual-consistency workaround. Per the AWS News Blog "Amazon S3 now delivers strong read-after-write consistency automatically for all applications" (posted Dec 1, 2020): *"After a successful write of a new object or an overwrite of an existing object, any subsequent read request immediately receives the latest version of the object. S3 also provides strong consistency for list operations."* MinIO is a drop-in local S3 for docker-compose and does not violate the "no embedded databases" rule (it is a separate service, like Postgres).
7. **LongMemEval_s haystacks are per-question and independently compiled, but filler sessions are sampled from shared ShareGPT/UltraChat/self-chat pools**, so the same session text recurs across many questions. A global `embedding_cache(content_hash, model, task_prefix, vector)` is therefore both benchmark-legal and a large cost saver: embeddings are deterministic functions of text, so caching by content hash changes nothing about per-question logical scoping.

## Details

### E.1 — Revised architecture (ingest → S3+PG outbox → async workers → unary embedder → enriched rows → client)

```
                         ┌──────────────────────────────────────────────┐
  Local harness          │                 SERVER (single binary)       │
  (client)               │                                              │
  ┌──────────┐  Upload   │  ┌────────────┐   1) PutObject(blob)         │
  │ produce  │──Memories─┼─▶│ Ingest RPC │──────────────▶ ┌─────────┐   │
  │ memories │  (stream) │  │ handler    │   2) BEGIN     │ S3 /    │   │
  └──────────┘           │  └─────┬──────┘   INSERT memories row       │ │
                         │        │          INSERT enrichment (pending)│
                         │        │          COMMIT ; pg_notify         │
                         │        ▼                                     │
                         │  ┌──────────────┐  claim (SKIP LOCKED+lease) │
                         │  │ Enrichment   │◀───────── Postgres ───────┐│
                         │  │ worker pool  │  GetObject(blob)  ┌───────┐││
                         │  │ errgroup     │─────────────────▶│ S3    │││
                         │  │ SetLimit(N)  │  normalize/tok/ts └───────┘││
                         │  │ +rate.Limiter│  Embed(search_document:…)  ││
                         │  └──────┬───────┘        │ unary gRPC        ││
                         │         │                ▼                   ││
                         │         │        ┌────────────────┐         ││
                         │         │        │ 3rd-party nomic│         ││
                         │         │        │ embedder (UNARY)│        ││
                         │         │        └────────────────┘         ││
                         │         │  UPDATE enrichment=done, vector    ││
                         │         └──── check embedding_cache first ───┘│
                         └──────────────────────────────────────────────┘
  ┌──────────┐  GetEnrichmentProgress(conv) ── poll until 100% done
  │  client  │◀─ FetchAllMemories (status='done' rows: text+lexemes+ts+vector)
  │  ranking │   RRF k=60 → temporal boost → MMR (per-session caps)  [UNCHANGED]
  └──────────┘
```

The worker runs as goroutines *inside the server binary* by default (no extra deployable); an env flag `ENRICH_STANDALONE=1` runs it as its own process for scale-out. S3 is the source of truth for raw bytes; Postgres holds the reference plus all derived fields the client needs.

### E.2 — Complete Postgres DDL

```sql
-- Raw memory reference. S3 is source of truth for bytes.
CREATE TABLE memories (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id TEXT NOT NULL,          -- LoCoMo conv / LongMemEval question id
    session_id     TEXT NOT NULL,
    turn_id        TEXT,                     -- LoCoMo dia_id or round id; NULL for session-granular
    s3_bucket      TEXT NOT NULL,
    s3_key         TEXT NOT NULL,            -- content-addressed: sha256 of blob
    byte_size      INTEGER NOT NULL,
    content_hash   BYTEA NOT NULL,           -- sha256 of the *raw blob*
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, session_id, turn_id)
);
CREATE INDEX idx_memories_conv ON memories (conversation_id);

-- Enrichment state = the job queue (side table; see tradeoff note E.3).
CREATE TABLE memory_enrichment (
    memory_id          BIGINT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
    enrichment_version SMALLINT NOT NULL DEFAULT 1,
    status             TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','processing','done','failed','dead')),
    attempts           SMALLINT NOT NULL DEFAULT 0,
    max_attempts       SMALLINT NOT NULL DEFAULT 5,
    locked_until       TIMESTAMPTZ,          -- lease expiry for crash recovery
    next_retry_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- derived fields the client consumes:
    normalized_text    TEXT,
    lexemes            TEXT[],               -- BM25 tokens
    ts                 TIMESTAMPTZ,          -- parsed round/session date
    embedding          BYTEA,                -- 768 float32 LE = 3072 bytes, L2-normalized
    embedded_at        TIMESTAMPTZ,
    error_message      TEXT
) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_vacuum_cost_limit = 1000);

-- Partial index: pollers scan only claimable work.
CREATE INDEX idx_enrich_claimable ON memory_enrichment (next_retry_at)
    WHERE status IN ('pending','failed');
CREATE INDEX idx_enrich_lease ON memory_enrichment (locked_until)
    WHERE status = 'processing';

-- Global, deterministic embedding cache (benchmark-legal: pure fn of text).
CREATE TABLE embedding_cache (
    content_hash BYTEA NOT NULL,   -- sha256 of the exact prefixed text
    model        TEXT  NOT NULL,   -- 'nomic-embed-text-v1.5'
    task_prefix  TEXT  NOT NULL,   -- 'search_document: '
    dims         SMALLINT NOT NULL DEFAULT 768,
    vector       BYTEA NOT NULL,   -- L2-normalized float32
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_hash, model, task_prefix)
);

-- Progress / readiness view for the client to poll.
CREATE VIEW enrichment_progress AS
SELECT m.conversation_id,
       count(*)                                             AS total,
       count(*) FILTER (WHERE e.status = 'done')            AS done,
       count(*) FILTER (WHERE e.status IN ('failed','dead'))AS failed,
       count(*) FILTER (WHERE e.status != 'done')           AS remaining,
       min(m.created_at) FILTER (WHERE e.status='pending')  AS oldest_pending
FROM memories m JOIN memory_enrichment e ON e.memory_id = m.id
GROUP BY m.conversation_id;
```

**Why store `normalized_text` and `lexemes` in Postgres instead of recomputing client-side:** it lets the client fetch everything it needs from Postgres in one streamed query and avoids an S3 GetObject fan-out for every memory at query time (~24k objects for LongMemEval_s). S3 stays the durable source of truth; Postgres is the queryable projection. Store the embedding as raw little-endian float32 `bytea` (768 × 4 = 3072 bytes); it is compact, avoids `pgvector` (explicitly dropped), and decodes directly into `[]float32` in Go.

### E.3 — Improvised unary embedder proto, Go adapter, and throughput/concurrency guidance

**Improvised minimal proto** (the real proto is unpublished; wrap behind an interface so any proto swaps in):

```proto
syntax = "proto3";
package embed.v1;
option go_package = "…/genproto/embedv1";

service Embedder {
  // UNARY ONLY — one text per RPC, no batch/stream.
  rpc Embed(EmbedRequest) returns (EmbedResponse);
}
message EmbedRequest {
  string text      = 1;   // caller has ALREADY prepended the task prefix
  string task_type = 2;   // "search_document" | "search_query" (informational)
}
message EmbedResponse {
  repeated float vector = 1;   // expect 768 dims
}
```

The nomic model card (Hugging Face `nomic-ai/nomic-embed-text-v1.5`) is explicit that *"the text prompt must include a task instruction prefix… you embed your documents as `search_document: <text here>` and embed your user queries as `search_query: <text here>`"* — so the adapter, not the server, owns prefix prepending, and the mock must not double-prefix.

**Go adapter interface** — decouples the pipeline from the concrete proto:

```go
type Embedder interface {
    // text must already include "search_document: " or "search_query: ".
    Embed(ctx context.Context, text, taskType string) ([]float32, error)
}

// grpcEmbedder wraps whatever generated client the real service ships.
type grpcEmbedder struct {
    cli embedv1.EmbedderClient
    lim *rate.Limiter
}

func (g *grpcEmbedder) Embed(ctx context.Context, text, task string) ([]float32, error) {
    if err := g.lim.Wait(ctx); err != nil { return nil, err }         // rate cap
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)            // per-call deadline
    defer cancel()
    resp, err := g.cli.Embed(ctx, &embedv1.EmbedRequest{Text: text, TaskType: task})
    if err != nil { return nil, err }                                 // grpc retry via svc config
    if len(resp.Vector) != 768 {                                      // validate dims
        return nil, fmt.Errorf("bad dims: got %d want 768", len(resp.Vector))
    }
    return l2normalize(resp.Vector), nil
}
```

**Connection strategy — one ClientConn, not a pool (for this workload).** A gRPC channel multiplexes many concurrent RPCs over one HTTP/2 connection. Per Microsoft Learn's "Performance best practices with gRPC": *"By default, most servers set this limit to 100 concurrent streams. A gRPC channel uses a single HTTP/2 connection… When the number of active calls reaches the connection stream limit, additional calls are queued in the client."* (Note: grpc-go removed its own hardcoded client-side default of 100 back in 2017 — issue grpc/grpc-go#1514 — so the effective cap is set by the *server's* `MaxConcurrentStreams`.) Since we deliberately bound concurrency to N ≤ ~64 with an errgroup, a single long-lived `ClientConn` saturates the endpoint without connection churn. Only introduce a small pool of channels (each with a distinct channel arg so they aren't deduped by grpc-go) if you must exceed the server's stream limit or if you observe client-side head-of-line queuing at high N — this is the exact "pool of gRPC channels to distribute RPCs over multiple connections" remedy the official gRPC performance guide recommends for high-load areas.

**grpc-go dial config** (retry service config + keepalive):

```go
const svcConfig = `{
  "methodConfig": [{
    "name": [{"service": "embed.v1.Embedder"}],
    "timeout": "5s",
    "retryPolicy": {
      "maxAttempts": 5,
      "initialBackoff": "0.1s",
      "maxBackoff": "3s",
      "backoffMultiplier": 2.0,
      "retryableStatusCodes": ["UNAVAILABLE","RESOURCE_EXHAUSTED","DEADLINE_EXCEEDED"]
    }
  }]
}`

conn, _ := grpc.NewClient(target,
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithDefaultServiceConfig(svcConfig),
    grpc.WithKeepaliveParams(keepalive.ClientParameters{
        Time:                30 * time.Second, // ping idle conns
        Timeout:             10 * time.Second,
        PermitWithoutStream: true,
    }),
)
```

Notes: grpc-go's built-in retry backoff includes jitter and is enabled purely via service config (the `retryPolicy`'s `retryableStatusCodes` gate which codes retry). `maxAttempts` is also capped internally at 5 unless you additionally pass `grpc.WithMaxCallAttempts(n)` (this "second maxAttempts" caveat is real and version-dependent). Client `Time` must be ≥ the server's `EnforcementPolicy.MinTime` (grpc-go default 5 minutes) or the server may send `GOAWAY`/`too_many_pings` (`ENHANCE_YOUR_CALM`); 30 s is aggressive against a default-configured server, so confirm the vendor's `MinTime` and raise `Time` if you see `ENHANCE_YOUR_CALM`. Circuit-breaking: wrap the adapter with a breaker (e.g., trip after K consecutive `UNAVAILABLE`) so a dead embedder pauses the pool instead of hammering it; the queue rows simply stay `pending`/`failed` with backoff and resume when it recovers.

**Throughput math (the core constraint).** With a unary endpoint, `throughput ≈ concurrency ÷ per-call latency`. Concrete table (steady-state, ignoring cache hits):

| Concurrency N | @ 50 ms latency | @ 100 ms latency | @ 200 ms latency |
|---|---|---|---|
| 8  | 160/s  | 80/s   | 40/s   |
| 16 | 320/s  | 160/s  | 80/s   |
| 32 | 640/s  | 320/s  | 160/s  |
| 64 | 1,280/s| 640/s  | 320/s  |
| 128| 2,560/s| 1,280/s| 640/s  |

Size N to the embedder's tolerance (start at 16–32; raise until latency degrades or you see `RESOURCE_EXHAUSTED`, then back off). Pair the errgroup limit with a `rate.Limiter` set slightly below the endpoint's published QPS so bursts don't trip rate limits. `errgroup.Group.SetLimit(n)` (added in Go 1.20) makes `g.Go` block once N goroutines are active, giving you the semaphore and implicit backpressure in one call; use `golang.org/x/sync/semaphore` instead only if you need a *global* limit shared across multiple errgroups.

### E.4 — Worker algorithm with exact SQL

**Phase 1 — Claim a batch (short tx, SKIP LOCKED + lease):**
```sql
WITH claimed AS (
  SELECT memory_id FROM memory_enrichment
  WHERE status IN ('pending','failed')
    AND next_retry_at <= now()
    AND enrichment_version < :current_version + 1   -- re-enrich support
  ORDER BY next_retry_at
  FOR UPDATE SKIP LOCKED
  LIMIT :batch
)
UPDATE memory_enrichment e
SET status='processing', locked_until = now() + interval '2 minutes',
    attempts = attempts + 1
FROM claimed WHERE e.memory_id = claimed.memory_id
RETURNING e.memory_id;
```
`SKIP LOCKED` skips rows another transaction already locked instead of blocking on them, so many workers claim disjoint batches concurrently with no convoy — the standard multi-consumer queue pattern. The single-statement CTE (SELECT…FOR UPDATE SKIP LOCKED feeding an UPDATE) is atomic and race-free.

**Phase 2 — Reaper (reclaim leases lost to crashes), runs periodically:**
```sql
UPDATE memory_enrichment
SET status='pending', next_retry_at = now()
WHERE status='processing' AND locked_until < now();
```

**Phase 3 — Complete (short tx, after embed succeeds):**
```sql
UPDATE memory_enrichment
SET status='done', embedding=:vec, normalized_text=:nt, lexemes=:lex,
    ts=:ts, embedded_at=now(), error_message=NULL, locked_until=NULL
WHERE memory_id=:id AND status='processing';
```

**Phase 4 — Fail with exponential backoff / dead-letter:**
```sql
UPDATE memory_enrichment
SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'failed' END,
    next_retry_at = now() + (interval '1 second' * pow(2, attempts)),
    error_message = :err, locked_until = NULL
WHERE memory_id = :id;
```

**Go worker pseudocode (claim-then-work-outside-tx, LISTEN/NOTIFY + polling hybrid):**
```go
func runWorkers(ctx context.Context, db *pgxpool.Pool, emb Embedder, s3 *S3, N int) {
    wake := make(chan struct{}, 1)
    go listen(ctx, db, wake)            // pgx conn.WaitForNotification -> wake
    ticker := time.NewTicker(2 * time.Second) // polling safety net for lost NOTIFYs
    for {
        ids := claimBatch(ctx, db, N*2) // Phase-1 SQL
        if len(ids) == 0 {
            select {
            case <-ctx.Done(): return
            case <-wake:                 // NOTIFY arrived
            case <-ticker.C:             // sweep even if NOTIFY missed
            }
            continue
        }
        g, gctx := errgroup.WithContext(ctx)
        g.SetLimit(N)                    // bounded concurrency
        for _, id := range ids {
            id := id
            g.Go(func() error {
                m := load(gctx, db, id)  // s3_key, content_hash, version
                text := normalize(fetchBlob(gctx, s3, m)) // GetObject + ETag check
                key := sha256("search_document: " + text)
                vec, hit := cacheGet(gctx, db, key)       // embedding_cache lookup
                if !hit {
                    var err error
                    vec, err = emb.Embed(gctx, "search_document: "+text, "search_document")
                    if err != nil { failWithBackoff(gctx, db, id, err); return nil }
                    cachePut(gctx, db, key, vec)          // UPSERT ON CONFLICT DO NOTHING
                }
                complete(gctx, db, id, vec, tokenize(text), parseDate(m))
                return nil
            })
        }
        _ = g.Wait()
    }
}
```
`listen` uses pgx v5's `conn.WaitForNotification(ctx)` on a dedicated connection (a listening connection cannot be shared for other queries); it maintains an internal notification queue so brief resets don't drop already-received notifications. Graceful shutdown: on `ctx` cancel, stop claiming, let in-flight `g.Wait()` drain; unfinished `processing` rows are reclaimed by the reaper via `locked_until`. LISTEN/NOTIFY: the ingest tx runs `pg_notify('enrich', '')` (empty/tiny payload — trivially under the 8000-byte cap the Postgres `NOTIFY` docs impose, since we only signal "work exists" and the worker re-queries); the polling ticker guarantees progress even if a NOTIFY is dropped because no listener was connected at commit time (NOTIFY is not durable — *"If the listening connection is not active at the moment a NOTIFY is processed… the notification is gone. There is no dead-letter queue, no replay"*).

### E.5 — Idempotency & re-enrichment versioning

- **Deterministic embedding:** the same prefixed text always yields the same vector, so re-running a job is safe.
- **`embedding_cache` UPSERT:** `INSERT … ON CONFLICT (content_hash, model, task_prefix) DO NOTHING` — concurrent workers embedding the same text race harmlessly.
- **`content_hash` on the blob** lets ingest skip re-writing/re-enriching unchanged memories (`INSERT … ON CONFLICT (conversation_id,session_id,turn_id) DO NOTHING`).
- **`enrichment_version`:** when the pipeline changes (e.g., you fix the `search_document: ` prefix, change tokenization, or switch dims), bump `CURRENT_VERSION` in code and re-enqueue: `UPDATE memory_enrichment SET status='pending', enrichment_version=:new, next_retry_at=now() WHERE enrichment_version < :new;`. Because the cache key includes model+prefix, a prefix fix naturally produces new cache entries rather than serving stale vectors — a critical safeguard given nomic's documented sensitivity to prefixes (*"Without prefixes, embedding quality degrades"*).

### E.6 — Wall-clock enrichment estimates

Dataset sizes confirmed from primary sources:
- **LoCoMo:** 10 conversations, ~272 sessions, ~5,882 dialogue turns total. Per arXiv:2511.21726: *"Each conversation in LoCoMo contains an average of 27.2 sessions (ranging from 19 to 32 sessions), 588.2 turns (ranging from 369 to 689 turns), and approximately 17,390 tokens."* (27.2 × 10 ≈ 272 sessions; 588.2 × 10 ≈ 5,882 turns; the LoCoMo GitHub repo confirms the released set is 10 conversations, each with per-turn `dia_id` and evidence `dia_id`s in the QA annotations.) At round granularity ≈ ~2,900 round-texts.
- **LongMemEval_s:** 500 questions. The official README/project site (xiaowu0162.github.io/long-mem-eval) states *"LongMemEvalS: each question's chat history has roughly 115k tokens (30-40 sessions)"*; the ICLR 2025 paper (arXiv:2410.10813) confirms *"500 manually created questions to test five core memory abilities."* Independent measurement of the released JSON (arXiv:2505.19549, Table 5) reports LongMemEval-s *"Avg. Sessions 50.2, Avg. Token 103,137.4, Avg. Query 1.0"* per question. Construction caps come from the README: *"80 is used for longmemeval_s and 500 is used for longmemeval_m… 115000 [tokens] is used for longmemeval_s."* Taking ~48 sessions/question → ~23,850 session-texts or ~246,750 turn-texts summed across all 500 questions.

Estimates (cold, no cache; use throughput table E.3):

| Workload | Texts | N=16 @50ms (320/s) | N=32 @50ms (640/s) | N=64 @50ms (1280/s) |
|---|---|---|---|---|
| LoCoMo, session-granular | ~272 | <1 s | <1 s | <1 s |
| LoCoMo, round-granular | ~2,900 | ~9 s | ~5 s | ~2 s |
| LoCoMo, turn-granular | ~5,882 | ~18 s | ~9 s | ~5 s |
| LongMemEval_s, session-granular | ~23,850 | ~75 s | ~37 s | ~19 s |
| LongMemEval_s, turn-granular | ~246,750 | ~13 min | ~6.4 min | ~3.2 min |

**With the `embedding_cache`:** because LongMemEval_s filler sessions are drawn from shared ShareGPT/UltraChat/self-chat pools (paper §3.2: *"We draw the irrelevant sessions from two sources: (1) self-chat sessions… and (2) publicly released user-AI style chat data including ShareGPT… and UltraChat"*), the same session text recurs across many of the 500 questions' haystacks. The first pass over one question's ~48 sessions warms the cache; subsequent questions hit the cache for every repeated filler session, so only genuinely new texts (the evidence sessions and first-seen fillers) reach the embedder. On any re-run the whole corpus is served from cache in seconds (bounded by Postgres read throughput, not the embedder). This makes the cache the difference between minutes and seconds on iteration.

### E.7 — Pitfalls

1. **Locks dropped on crash → stuck `processing` rows.** Row locks disappear when the connection dies mid-work, but the status stays `processing`. The `locked_until` lease + reaper (Phase 2) is mandatory; do not rely on the lock alone. (This is the exact "worker grabs a job, then crashes; job stuck as running forever" failure the heartbeat/reaper pattern exists to solve.)
2. **Lost NOTIFY wakeups.** NOTIFY is not durable and is dropped if no session is listening at commit; always keep the polling ticker as a safety net. Keep payloads tiny (signal-only) — the 8000-byte limit is irrelevant if you only send "work exists."
3. **Long transactions during gRPC calls.** Holding a tx open across a 50–200 ms embed call multiplied by many workers bloats the queue table and blocks autovacuum from reclaiming dead tuples (long-running transactions are a primary cause of queue-table bloat). Claim-then-work-outside-tx-then-complete keeps every tx sub-millisecond.
4. **S3/PG ordering.** Write the blob to S3 first, then insert the referencing row; S3's strong read-after-write consistency (since Dec 1, 2020) means the enricher's GetObject will always see the blob. A rare orphan blob (row insert fails after PutObject) is cheap; sweep with a periodic reconcile if desired. The outbox insert (row + job in one tx) guarantees no memory is missed — *"the event exists if and only if the business row exists."*
5. **Autovacuum on the hot queue table.** Set `fillfactor=80` to enable HOT updates (status changes touch non-indexed columns → new tuple version fits on the same page → no index entry, so *"this eliminates index maintenance entirely and makes vacuum significantly cheaper"*), and aggressive per-table autovacuum (`autovacuum_vacuum_scale_factor=0.02`, high cost limit — the recommended high-churn tier). Monitor `n_dead_tup` and `n_tup_hot_upd`. Keep volatile columns (status, attempts, locked_until, next_retry_at) out of indexes so updates stay HOT-eligible.
6. **LongMemEval shared-session dedup interplay.** Cache **embeddings** globally by `content_hash` (safe: deterministic pure function of text), but keep **logical memory rows per-question** — each question's retrieval must rank over its own haystack, and the client's per-session MMR caps and RRF operate within the question's scope. The cache is an implementation optimization beneath the logical model, not a change to it. (No source documents cross-question session dedup in the dataset itself; the "23,867 documents" figure from one vendor blog is total sessions summed over questions, ≈ 500 × 47.7, **not** a verified-unique count.)
7. **Wrong-dimension responses.** Validate `len(vector)==768` before writing; treat mismatches as permanent (dead-letter). Text too long → truncate to the model's 8192-token window before embedding rather than failing.

### E.8 — What changes vs. the previous design; what stays the same

**Unchanged (client-side retrieval):** brute-force cosine over L2-normalized float32 vectors, in-memory BM25, RRF fusion (k=60), rule-based temporal boosting, MMR diversification with per-session caps, round-level granularity, and the Go-computed Recall@k / NDCG@k / MRR eval. The nomic prefixes (`search_document: ` for corpus, `search_query: ` for queries) and 768-dim L2-normalized vectors are unchanged. One-click docker-compose + run.sh remains.

**Changed / added:**
- Memories are now **S3/MinIO blobs** referenced by Postgres rows (was: memories stored directly in Postgres). Recommended blob format: a small protobuf-serialized (or JSON) `Memory` envelope carrying turns/speaker/date so the enricher can parse structure from the blob; content-addressed keys (`sha256`) give free dedup and idempotent re-writes vs. random UUID keys.
- Embeddings come from a **third-party unary gRPC service** via the `Embedder` adapter interface (was: local/batch embedding). Throughput is now `concurrency/latency`-bound.
- A new **async enrichment pipeline** (SKIP LOCKED queue + lease + state machine + cache) populates derived fields incrementally and reliably.
- Proto additions: `Memory` gains `s3_key` and `enrichment_status`; new `GetEnrichmentProgress(conversation_id)` returns status counts; optional `WatchEnrichment` server-stream. `FetchAllMemories` streams only `status='done'` rows (or includes status so the client filters).
- **docker-compose** adds `minio/minio` (`server /data --console-address ":9001"`) + an `mc` bootstrap sidecar to create the bucket, and an `embedder-mock` unary gRPC service implementing the improvised proto. The Go S3 client uses `aws-sdk-go-v2` with `o.BaseEndpoint = "http://minio:9000"` and `o.UsePathStyle = true` (required for MinIO). `run.sh` polls `GetEnrichmentProgress` until 100% `done` per conversation, then runs retrieval + eval.

## Recommendations

1. **Ship the hand-rolled SKIP LOCKED queue, not a library, for v1.** For a benchmark harness the hand-rolled approach (Sections E.2/E.4) is the simplest reliable path and keeps the schema transparent. Keep **River** (pgx-native, transactional enqueue, NOTIFY-driven so *"the job queue can wake workers to begin working a job the moment it's ready, reducing average latency before a job starts to milliseconds,"* plus a Web UI and `COPY FROM` bulk insert) as the documented production upgrade path if this ever leaves the harness; **gue** (transaction-level locks, pgx v5) and **neoq** (queue-agnostic, in-memory/Postgres/Redis backends) are viable alternatives but River is the most actively maintained (4.6k+ GitHub stars). Do not reach for Redis/Kafka — Postgres comfortably handles this scale ("Postgres is the only queue you need until ~50k jobs/sec").
2. **Start at N=16 concurrent embeds with a `rate.Limiter` just below the vendor's QPS; raise toward 32–64** while watching p99 latency and `RESOURCE_EXHAUSTED`. Threshold to stop increasing N: when added concurrency stops increasing embeds/sec (latency rising proportionally) or the vendor starts rate-limiting.
3. **Turn on the `embedding_cache` from day one.** It is the single biggest wall-clock win for LongMemEval iteration and is benchmark-legal because embeddings are deterministic. Benchmark to change this decision: if cache hit-rate is near zero (e.g., truly unique corpus), the cache adds only a cheap Postgres lookup — keep it anyway.
4. **Make the client block on `enrichment_progress` until `remaining=0` per conversation before eval.** For correctness of Recall@k/NDCG@k the corpus must be 100% enriched; expose `remaining` and `failed` so the harness fails loudly if any row is `dead`.
5. **Set queue-table storage params up front** (`fillfactor=80`, aggressive autovacuum). Threshold to revisit: if `n_dead_tup` grows faster than autovacuum reclaims or the `n_tup_hot_upd`/`n_tup_upd` ratio drops below ~0.9, lower fillfactor further or move enrichment state to a dedicated table you can `TRUNCATE`/rebuild between runs (treat the queue table as disposable, per the neoq/River "disposable queue table" learning).
6. **Keep enrichment state as a side table (`memory_enrichment`)** rather than columns on `memories`: it isolates high-churn queue updates from the stable `memories` rows (less bloat on the table the client reads), and lets you drop/rebuild the queue on a version bump without touching source references.

## Caveats

- **Embedder latency is assumed.** All wall-clock numbers use illustrative 50/100/200 ms per-call latencies; measure the real vendor latency and re-derive from `throughput = N/latency`. The vendor's actual QPS ceiling, `MaxConcurrentStreams`, and keepalive `MinTime` are unknown and must be confirmed.
- **The improvised proto is a placeholder.** The real service proto is unpublished; the `Embedder` interface exists precisely so the concrete generated client can be swapped without touching the pipeline. Field numbers/names will differ.
- **LongMemEval_s session counts vary by source:** the official project site says "30–40 sessions/question"; an independent measurement of the released JSON (arXiv:2505.19549) gives an average of 50.2; one paper (LETHE, arXiv:2606.15903) cites "up to 158" as a per-question maximum. Turn/round counts are derived from measured aggregates, not a single quoted per-session statistic. The "23,867 documents" figure is from a vendor (ByteRover) blog and equals total sessions summed across questions (≈ 500 × 47.7, no verified cross-question dedup), not a unique-session count. Use ~48 sessions/question as the planning midpoint.
- **grpc-go retry `maxAttempts` is internally capped at 5** unless `grpc.WithMaxCallAttempts` is also set; verify the effective attempt count in your grpc-go version.
- **MinIO ≠ AWS S3 in every detail** (some multipart/edge behaviors differ), but for GetObject/PutObject with `UsePathStyle=true` it is a faithful local stand-in and satisfies "no embedded databases" (it is a separate networked service, exactly like Postgres).

---

**References:**

1. [Amazon S3 now delivers strong read-after-write consistency automatically for all applications (aws.amazon.com)](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications)
2. [nomic-ai/nomic-embed-text-v1.5 · Hugging Face (huggingface.co)](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5)
3. [Performance best practices with gRPC | Microsoft Learn (learn.microsoft.com)](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-9.0)
4. [gRPC-Go: Built-in Client Retry Mechanism | mtardy (mtardy.com)](https://mtardy.com/posts/grpc-go-client-retry/)
5. [Goroutine Pool Patterns in Go: errgroup & Backpressure (tanhdev.com)](https://tanhdev.com/posts/golang-goroutine-pool-errgroup-worker/)
6. [Limiting goroutines - Boldly Go (boldlygo.tech)](https://boldlygo.tech/archive/2025-09-11-limiting-goroutines/)
7. [Transactional Outbox Pattern in PostgreSQL: Reliable Events (matthewswong.com)](https://www.matthewswong.com/en/blog/transactional-outbox-pattern-postgres/)
8. [Go and Postgres Listen/Notify or: How I Learned to Stop Worrying and Love PubSub :: Jon Brown's Webpage (brojonat.com)](https://brojonat.com/posts/go-postgres-listen-notify/)
9. [nomic-embed-text-v1.5-GGUF: Text-to-Text model — overview, use cases, alternatives (aimodels.fyi)](https://www.aimodels.fyi/models/huggingFace/nomic-embed-text-v1.5-gguf-nomic-ai)
10. [Goal-Directed Search Outperforms Goal-Agnostic Memory Compression in Long-Context Memory Tasks (arxiv.org)](https://arxiv.org/pdf/2511.21726)
11. [GitHub - snap-research/locomo · GitHub (github.com)](https://github.com/snap-research/locomo)
12. [LongMemEval (xiaowu0162.github.io)](https://xiaowu0162.github.io/long-mem-eval/)
13. [longmemeval: benchmarking chat assist (arxiv.org)](https://arxiv.org/pdf/2410.10813)
14. [LongMemEval: Benchmarking Chat Assist- ants on Long-Term Interactive Memory (arxiv.org)](https://arxiv.org/html/2410.10813v1)
15. [LongMemEval/README.md at main · xiaowu0162/LongMemEval (github.com)](https://github.com/xiaowu0162/LongMemEval/blob/main/README.md)
16. [PostgreSQL Autovacuum Tuning: A Practical Guide | by Philip McClarence | Medium (medium.com)](https://medium.com/@philmcc/postgresql-autovacuum-tuning-a-practical-guide-71847badc9d3)
17. [Benchmark AI Agent Memory in Real Production: ByteRover scores 92.8% Top Market Accuracy, 1.6s Latency (LongMemEval-S) (byterover.dev)](https://www.byterover.dev/blog/benchmark_ai_agent_memory_real_production_byterover_top_market_accuracy_longmemeval)
18. [s3 package - github.com/common-library/go/aws/s3 - Go Packages (pkg.go.dev)](https://pkg.go.dev/github.com/common-library/go/aws/s3)
19. [River - Go + Postgres 용 빠르고 단단한 Job Queue (news.hada.io)](https://news.hada.io/topic?id=12078)
20. [GitHub - vgarvardt/gue: Golang queue on top of PostgreSQL · GitHub (github.com)](https://github.com/vgarvardt/gue)
21. [github.com (github.com)](https://github.com/acaloiaro/neoq)
22. [riverqueue vs solid queue (stackshare.io)](https://stackshare.io/stackups/riverqueue-vs-solid-queue)
23. [Control-Plane Placement Shapes Forgetting: An Architectural Study of Agent Memory Across Thirteen System Configurations (arxiv.org)](https://arxiv.org/pdf/2606.15903)

## User:

> 8/15/2026 13:55:19

Can we replace the memory enrichment pipeline with a Twmporal workflow in Go? It can run every minute.

## Assistant:

> 8/15/2026 14:04:41

> Investigated temporal patterns and Postgres queue management for workflow redesign.
>
> - Investigated temporal patterns and Postgres queue management for workflow redesign.
> - launch_extended_search_task
> - **Done**

Your Temporal migration report for the Postgres enrichment queue is ready. It recommends replacing the hand-rolled queue with a Temporal Schedule (1-min interval, Skip overlap) driving an `EnrichmentSweepWorkflow` that fans out coarse `ProcessBatch` activities. The report covers architecture diagrams, Go code sketches (schedule bootstrap, sweeper workflow, batch activity with heartbeat resume, worker rate limits), a Postgres DDL diff dropping `locked_until`/`next_retry_at`/`attempts`, a docker-compose setup sharing your existing Postgres, failure-mode walkthroughs (worker crash, embedder outage, Temporal downtime, zombie double-writes), pitfalls around event-history and payload limits, a side-by-side comparison table, a 250k-item scale sanity check, a 4-stage rollout plan, and caveats on SDK versioning and `serviceerror.AlreadyExists` handling.

**Replacing the Postgres Enrichment Queue with a Temporal Schedule and Sweeper Workflow in Go**

# Replacing the Postgres Enrichment Queue with a Temporal Schedule + Sweeper Workflow (Go)

## TL;DR
- **Yes — replace it.** Adopt a Temporal **Schedule** firing every 1 minute with overlap policy **Skip**, driving an `EnrichmentSweepWorkflow` that fans out a small number of `ProcessBatch` activities (each claiming N rows via `SELECT ... FOR UPDATE SKIP LOCKED`, embedding with internal `errgroup` concurrency, heartbeating progress, and UPSERTing results). Temporal absorbs retries, backoff, crash detection, and rate-limiting; you delete the hand-rolled lease/reaper/backoff machinery.
- **Schema simplifies sharply.** Drop `locked_until`, `next_retry_at`, `attempts`/`max_attempts`, and the reaper/claimable index. Keep a `status`/`embedded_at` marker, `error_message`, `enrichment_version`, the derived columns, a partial index `WHERE status='pending'`, and the `embedding_cache` table unchanged. `SKIP LOCKED` stays — not for crash recovery, but to keep concurrent sibling batch activities in the *same* sweep from claiming the same rows.
- **Net trade:** you gain durable execution, a visibility UI, task-queue-wide rate limiting to protect the unary embedder, and a clean path to future backfill/re-enrichment workflows; you pay with one more infra dependency (Temporal server + its two Postgres databases) and up to ~60 s enrichment-start latency unless you `Trigger` the schedule right after ingest (which restores near-instant start and lets you drop LISTEN/NOTIFY entirely).

## Key Findings
- **Schedules over CronSchedule.** Temporal's Go docs explicitly recommend Schedules over the legacy `CronSchedule` string: *"We recommend using Schedules instead of Cron Jobs. Schedules were built to provide a better developer experience, including more configuration options and the ability to update or pause running Schedules."* Use `client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: time.Minute}}}`.
- **Overlap policy = Skip.** `Skip` is the default and is correct for a sweep. Per Temporal's Schedule doc, verbatim: *"Skip: Default. Nothing happens; the Workflow Execution is not started. BufferOne: Starts the Workflow Execution as soon as the current one completes."* This guarantees exactly one sweeper run at a time; if a run takes >60 s draining a backlog, intervening ticks are dropped, and the next tick after completion picks up leftovers. `BufferOne` is the fallback if you want a single queued run to fire immediately after a long one finishes.
- **Event-history limits are the binding constraint on fan-out.** Per Temporal's Workflow Execution limits doc, verbatim: *"The Workflow Execution's Event History is limited to 51,200 Events or 50 MB and will warn you after 10,240 Events or 10 MB."* Each activity contributes roughly 3–5 events (ScheduleActivityTask, ActivityTaskStarted, ActivityTaskCompleted, plus timer/heartbeat-timeout events). That caps a single sweeper run at well under ~10k activities before you should ContinueAsNew — which is exactly why a **coarse batch-activity** design (tens of activities per run, each processing hundreds of items) is preferred over activity-per-memory (250k activities/run is impossible within one run).
- **Payload limits forbid moving blobs/vectors through history.** Per Temporal's self-hosted defaults doc: *"Temporal warns at 256 KB: Blob size exceeds limit. Temporal errors at 2 MB: ErrBlobSizeExceedsLimit... gRPC has a limit of 4 MB for each message received... The DefaultTransactionSizeLimit limit is 4 MB."* A 768-dim float32 vector is ~3 KB, so a single vector fits, but returning thousands per activity does not — and there's no reason to. Activities exchange **only ids and counts**; embeddings are written to Postgres inside the activity.
- **Task-queue-wide rate limiting is the key knob for the third-party embedder.** Per Temporal's Go SDK foundations doc, `TaskQueueActivitiesPerSecond` is *"managed by the Temporal Service and limits the Activity Tasks per second for the entire Task Queue... This can be used to protect downstream services from flooding."* `WorkerActivitiesPerSecond` is per-worker; `MaxConcurrentActivityExecutionSize` (default 200 in current SDKs) caps parallel activity slots per worker.
- **Heartbeats give you crash recovery for free.** `activity.RecordHeartbeat(ctx, details)` + a `HeartbeatTimeout` replaces the `locked_until` lease + reaper. On retry, `activity.HasHeartbeatDetails`/`activity.GetHeartbeatDetails` return the last progress payload so a batch resumes from where it died. Heartbeats are throttled to `min(0.8 × HeartbeatTimeout, 30 s default)`, so heartbeat *frequently* with a *short* HeartbeatTimeout.
- **Idempotency comes from Postgres state, not from Temporal.** Because per-item `status`/`embedded_at` lives in Postgres and completion is an idempotent UPSERT, at-least-once activity execution and zombie double-writes are harmless: a retried or duplicated batch re-selects only still-`pending` rows and re-writes identical derived data.

## Details

### 1. Recommendation summary
Replace the hand-rolled queue with a **Temporal Schedule (every 1 min, overlap Skip) → `EnrichmentSweepWorkflow` → fan-out of `ProcessBatch` activities**. Each `ProcessBatch` claims a page of unenriched rows with `SELECT ... FOR UPDATE SKIP LOCKED LIMIT N`, embeds them using a bounded internal `errgroup`, heartbeats progress (last processed index/ids), and writes results with an idempotent UPSERT. The sweeper loops (dispatching successive fan-out waves) until the backlog is empty or a **soft deadline (~50 s)** is hit, then exits cleanly so the next scheduled tick starts fresh with a small history. Per-item state remains in Postgres; Temporal owns retries, backoff, crash detection, and rate limiting.

This is the right architecture because (a) the user explicitly framed the model as "run every minute," (b) a stateless-between-runs sweeper keeps event history trivially bounded without ContinueAsNew gymnastics, and (c) coarse batch activities keep history-event counts low while the unary embedder's throughput is recovered via internal goroutine concurrency.

### 2. Architecture
```
Ingest RPC (unchanged):
  server → PUT blob to S3/MinIO
         → BEGIN; INSERT memories row; INSERT memory_enrichment(status='pending'); COMMIT
         → [optional] scheduleHandle.Trigger(ctx)   // fast path, replaces LISTEN/NOTIFY

Temporal Schedule "enrichment-sweep" (Every: 1m, Overlap: Skip)
        │  fires
        ▼
EnrichmentSweepWorkflow(run)
        │  loop until backlog empty OR soft deadline (~50s):
        │    1) CountBacklog activity  → n pending
        │    2) fan out K ProcessBatch activities (futures + selector)
        │    3) aggregate counts; continue loop
        ▼
ProcessBatch activity  (× K in parallel, task-queue rate-limited)
        │  SELECT ... FOR UPDATE SKIP LOCKED LIMIT N   (claim page)
        │  errgroup (concurrency C): per row →
        │        fetch S3 blob → normalize/lex/parse ts
        │        check embedding_cache(content_hash,model,task_prefix)
        │        else one unary embed RPC ("search_document: " + text)
        │        UPSERT memory_enrichment derived fields, status='done', embedded_at=now()
        │  RecordHeartbeat(lastIndex) every K items
        │  return {done, failed, permanent} counts   // NO vectors

Client eval (unchanged): poll GetEnrichmentProgress / enrichment_progress view until 100%.
```
Nothing about the retrieval stack, nomic prefixes, `embedding_cache`, or S3-first-then-Postgres ingest ordering changes.

### 3. Go code sketches

**Schedule creation (idempotent, on server startup).** The latest Go SDK is **`go.temporal.io/sdk v1.47.0` (published Jul 28, 2026, per pkg.go.dev)**. When a schedule ID already exists, `ScheduleClient().Create` returns a generic **`*serviceerror.AlreadyExists`** (`go.temporal.io/api/serviceerror`) — there is no schedule-specific Go error type — so swallow it with `errors.As`.

```go
import (
    "context"
    "errors"
    "time"

    "go.temporal.io/api/enums/v1"
    "go.temporal.io/api/serviceerror"
    "go.temporal.io/sdk/client"
)

func ensureSchedule(ctx context.Context, c client.Client) error {
    _, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
        ID: "enrichment-sweep",
        Spec: client.ScheduleSpec{
            Intervals: []client.ScheduleIntervalSpec{{Every: time.Minute}},
        },
        Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP, // default, explicit for clarity
        Action: &client.ScheduleWorkflowAction{
            ID:        "enrichment-sweep-wf",
            Workflow:  EnrichmentSweepWorkflow,
            TaskQueue: "enrichment",
            // Keep the workflow-run RetryPolicy tight; scheduled runs are self-healing:
            // a failed run just means the next tick retries the sweep.
        },
    })
    var exists *serviceerror.AlreadyExists
    if err != nil && !errors.As(err, &exists) {
        return err
    }
    return nil
}
```

**EnrichmentSweepWorkflow (fan-out with futures + soft deadline).**
```go
const (
    batchSize    = 128 // rows claimed per ProcessBatch
    fanOut       = 8   // parallel ProcessBatch activities per wave
    softDeadline = 50 * time.Second
)

func EnrichmentSweepWorkflow(ctx workflow.Context) error {
    ao := workflow.ActivityOptions{
        StartToCloseTimeout: 5 * time.Minute,
        HeartbeatTimeout:    15 * time.Second, // crash detected within ~15s
        RetryPolicy: &temporal.RetryPolicy{
            InitialInterval:    time.Second,
            BackoffCoefficient: 2.0,
            MaximumInterval:    30 * time.Second,
            MaximumAttempts:    5,
            NonRetryableErrorTypes: []string{"PermanentEnrichmentError"},
        },
    }
    ctx = workflow.WithActivityOptions(ctx, ao)

    deadline := workflow.Now(ctx).Add(softDeadline)
    for workflow.Now(ctx).Before(deadline) {
        var backlog int
        if err := workflow.ExecuteActivity(ctx, CountBacklog).Get(ctx, &backlog); err != nil {
            return err
        }
        if backlog == 0 {
            return nil // drained; exit, next tick will re-check
        }

        waves := (backlog + batchSize - 1) / batchSize
        if waves > fanOut {
            waves = fanOut
        }
        futures := make([]workflow.Future, waves)
        for i := 0; i < waves; i++ {
            futures[i] = workflow.ExecuteActivity(ctx, ProcessBatch, batchSize)
        }
        // Collect; failures inside a batch leave rows 'pending' for the next wave/tick.
        for _, f := range futures {
            var r BatchResult
            _ = f.Get(ctx, &r) // log-and-continue; do not fail the whole sweep
        }
    }
    return nil // soft deadline hit; exit so history stays small, next tick continues
}
```
Note the deliberate choice: because the sweeper exits and re-fires every minute, it never approaches the 51,200-event/50 MB ceiling and needs **no ContinueAsNew** — each run's history spans at most a handful of waves. If you preferred a single long-running loop instead of a schedule, you *would* need ContinueAsNew (the Batch Iterator / Sliding Window patterns exist precisely for that); the schedule design sidesteps it entirely.

**ProcessBatch activity (claim + embed + UPSERT, with heartbeat resume).**
```go
type BatchResult struct{ Done, Failed, Permanent int }

func ProcessBatch(ctx context.Context, limit int) (BatchResult, error) {
    // Resume point (if this is a retry after a heartbeat timeout)
    startIdx := 0
    if activity.HasHeartbeatDetails(ctx) {
        _ = activity.GetHeartbeatDetails(ctx, &startIdx)
    }

    // Claim a page. SKIP LOCKED prevents sibling ProcessBatch activities in the
    // SAME sweep from grabbing the same rows. No lease/reaper needed — the row
    // lock is released at COMMIT, and status='done' makes re-selection idempotent.
    rows, err := claimPending(ctx, limit) // SELECT ... WHERE status='pending'
                                          // FOR UPDATE SKIP LOCKED LIMIT $1
    if err != nil {
        return BatchResult{}, err
    }

    var res BatchResult
    g, gctx := errgroup.WithContext(ctx)
    g.SetLimit(32) // internal concurrency recovers unary-embedder throughput

    for i, row := range rows {
        if i < startIdx {
            continue // already processed before the crash
        }
        i, row := i, row
        g.Go(func() error {
            text := normalize(row.Blob) // fetched from S3 inside claim or here
            vec, err := embedWithCache(gctx, row.ContentHash, "search_document: "+text)
            if err != nil {
                if isPermanent(err) { // wrong dims / text too long
                    markDead(ctx, row.ID, err) // status='dead', error_message set
                    res.Permanent++
                    return nil
                }
                res.Failed++            // leave row 'pending' for next sweep
                return nil
            }
            upsertEnrichment(ctx, row.ID, text, vec) // status='done', embedded_at=now()
            res.Done++
            activity.RecordHeartbeat(ctx, i) // progress = last processed index
            return nil
        })
    }
    _ = g.Wait()
    return res, nil
}
```
Return **counts, never vectors** — this keeps every activity result far under the 256 KB warn / 2 MB error payload thresholds. Permanent failures use `temporal.NewNonRetryableApplicationError(..., "PermanentEnrichmentError", ...)` (matched by `NonRetryableErrorTypes`) or are simply marked `dead` in Postgres directly, as shown.

**Worker setup with rate limits.**
```go
w := worker.New(c, "enrichment", worker.Options{
    MaxConcurrentActivityExecutionSize: 64,   // parallel activity slots on this worker
    TaskQueueActivitiesPerSecond:       600,  // task-queue-wide cap; protects embedder
                                              // across ALL workers (e.g., 32c×50ms≈640/s)
    WorkerActivitiesPerSecond:          0,    // 0 = unlimited (rely on task-queue cap)
})
w.RegisterWorkflow(EnrichmentSweepWorkflow)
w.RegisterActivity(CountBacklog)
w.RegisterActivity(ProcessBatch)
// Register BEFORE creating/starting the schedule, or the first fire finds no worker.
if err := w.Start(); err != nil { log.Fatal(err) }
defer w.Stop() // graceful shutdown drains in-flight activities
```

**Trigger after ingest (fast path).**
```go
// Immediately after the ingest tx commits:
_ = c.ScheduleClient().GetHandle(ctx, "enrichment-sweep").
    Trigger(ctx, client.ScheduleTriggerOptions{
        Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP, // don't pile onto a running sweep
    })
```
This starts enrichment within moments of upload instead of waiting up to 60 s, and lets you delete LISTEN/NOTIFY.

### 4. Simplified Postgres DDL diff
```sql
-- memory_enrichment: DROP the hand-rolled queue machinery -----------------
ALTER TABLE memory_enrichment DROP COLUMN locked_until;     -- lease → HeartbeatTimeout
ALTER TABLE memory_enrichment DROP COLUMN next_retry_at;    -- backoff → RetryPolicy
ALTER TABLE memory_enrichment DROP COLUMN attempts;         -- attempt count → RetryPolicy
ALTER TABLE memory_enrichment DROP COLUMN max_attempts;     -- → MaximumAttempts
DROP INDEX IF EXISTS idx_enrichment_claimable;              -- reaper/claim index gone

-- KEEP (still required) ----------------------------------------------------
--   memory_id            (PK/FK)
--   enrichment_version   (for future re-enrichment / version bumps)
--   status               ('pending' | 'done' | 'dead')  OR use embedded_at IS NULL
--   error_message        (dead-letter reason)
--   normalized_text, lexemes, parsed_ts, embedding  (derived fields)
--   created_at / embedded_at

-- Partial index that makes the sweep's claim query cheap ------------------
CREATE INDEX idx_enrichment_pending
  ON memory_enrichment (memory_id)
  WHERE status = 'pending';

-- embedding_cache: UNCHANGED  (content_hash, model, task_prefix) → embedding
```
`SKIP LOCKED` is retained in the claim query itself — not for crash recovery (Temporal handles that) but because up to `fanOut` sibling `ProcessBatch` activities run concurrently within one sweep and must not claim overlapping rows. The row lock is scoped to the claim transaction; there is no separate lease column and no reaper.

### 5. docker-compose + run.sh
Share the existing `postgres:16` instance; Temporal auto-setup creates its own `temporal` and `temporal_visibility` databases so there is no collision with the app's database. The official image env vars are `DB=postgres12` (the schema plugin name, correct even for PG16), `DB_PORT`, `POSTGRES_USER`, `POSTGRES_PWD`, `POSTGRES_SEEDS` (the Postgres hostname).

```yaml
services:
  postgres:            # existing app DB, now also hosts temporal + temporal_visibility
    image: postgres:16
    environment: { POSTGRES_USER: app, POSTGRES_PASSWORD: app }
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app"]
      interval: 5s
      timeout: 5s
      retries: 30

  temporal:
    image: temporalio/auto-setup:1.28.0   # pin; auto-setup is NOT production-grade
    depends_on:
      postgres: { condition: service_healthy }
    environment:
      - DB=postgres12
      - DB_PORT=5432
      - POSTGRES_USER=app
      - POSTGRES_PWD=app
      - POSTGRES_SEEDS=postgres
    ports: ["7233:7233"]

  temporal-ui:
    image: temporalio/ui:2.34.0
    depends_on: [temporal]
    environment:
      - TEMPORAL_ADDRESS=temporal:7233
    ports: ["8080:8080"]

  server:              # your Go gRPC server + Temporal worker + embedder client
    depends_on:
      temporal: { condition: service_started }
      minio:    { condition: service_healthy }
    environment:
      - TEMPORAL_HOSTPORT=temporal:7233
```
`client.Dial(client.Options{HostPort: "temporal:7233"})` in the server; on a fresh compose the frontend can reject connections for a few seconds after the container is "up," so wrap `Dial` (or the first `ensureSchedule` call) in a short retry loop. `temporal server start-dev` (single binary, SQLite/in-memory) is an alternative, but auto-setup + your existing Postgres avoids introducing an embedded database on the durable path and is the right fit for a compose harness. (Pin the image tags — the official `temporalio/docker-compose` repo was archived read-only in January 2026, so copy the pattern rather than depending on it live.)

**run.sh changes:** none structural. On server startup, call `ensureSchedule` (idempotent, swallows `AlreadyExists`) after the worker is registered and started. The eval flow is unchanged: the client still polls `enrichment_progress`. Optionally `Trigger` the schedule after each ingest batch for instant start.

### 6. Failure-mode walkthrough
- **Worker crash mid-batch.** The activity stops heartbeating; after `HeartbeatTimeout` (~15 s) the server times out the activity task and retries it per `RetryPolicy`. The retry reads `GetHeartbeatDetails` to resume past already-processed items, and the re-run's `SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending'` naturally skips rows already marked `done`. This fully replaces the old `locked_until` + reaper.
- **Embedder outage.** Transient embed errors leave rows `pending` and increment `Failed` counts (or the activity errors and Temporal retries with exponential backoff up to `MaximumAttempts`). Because the sweep re-fires every minute, sustained outages simply mean rows stay `pending` and drain once the embedder recovers — no data loss, no manual intervention.
- **Temporal server down.** Ingest is completely unaffected (S3 + Postgres writes don't touch Temporal). Scheduled sweeps pause; on recovery, the catchup window (default one year, minimum ten seconds) governs whether missed ticks are made up — with overlap Skip the practical effect is just that enrichment resumes at the next tick. No enrichment is lost because backlog state lives in Postgres.
- **Zombie activity double-write.** A network-partitioned worker may keep writing after the server has already retried the batch elsewhere. Both writes are **idempotent UPSERTs of identical deterministic derived data** (embeddings are deterministic for a given content_hash/model/prefix), so the double-write is harmless. No `claimed_by_run_id` column is needed; if you want belt-and-suspenders visibility you may add one, but it is not required for correctness.

### 7. Pitfalls
- **Event-history limits.** 51,200 events / 50 MB hard; warnings at 10,240 events / 10 MB. With ~3–5 events per activity, keep a single sweeper run to well under ~2k activities; the soft-deadline exit design keeps each run far below this.
- **Payload / gRPC limits.** 256 KB payload warn, 2 MB payload error (`BlobSizeLimitError`), 4 MB gRPC message / event-history transaction cap. Never pass memory texts or embeddings through workflow history; activities exchange ids and counts only. Scheduling too many activities in one workflow task can breach the 4 MB *combined* command limit even when each input is tiny — another reason to keep `fanOut` modest.
- **Overlap misconfiguration.** `AllowAll` or `BufferAll` would let sweeps pile up on a backlog and re-claim rows, and `BufferAll` can push buffered actions past the catchup window. Stick with `Skip` (or `BufferOne`).
- **Activities must be registered before the schedule fires.** Register workflow + activities and start the worker *before* `ensureSchedule`, or the first fire has no worker to run it.
- **auto-setup is not production-grade** (fine for a benchmark harness) — the Temporal team explicitly says to use `temporalio/server` with externally managed schema in production.
- **Schedule jitter / catchup window.** Default catchup window is large; if you tighten it, an outage longer than the window silently drops ticks (`schedule_missed_catchup_window` metric).
- **Workflow versioning.** If you evolve the sweeper's activity sequence while runs are in flight, use `workflow.GetVersion` (or Worker Versioning) to avoid non-determinism errors. Because runs are short (≤50 s) and fire every minute, in practice you can just let in-flight runs finish and deploy between ticks.

### 8. What stays the same
Retrieval stack (RRF / BM25 / cosine / MMR / temporal boost, all client-side metrics), nomic `search_document:` / `search_query:` prefixes, the `embedding_cache` table and its (content_hash, model, task_prefix) key, the S3-first-then-Postgres ingest ordering with the memories + enrichment rows in a single transaction, and the client's `enrichment_progress` polling. The transactional outbox collapses to just the two-row insert; **LISTEN/NOTIFY is removed** — the 1-minute schedule plus optional `Trigger`-after-ingest replaces it.

### Comparison: replaced design vs Temporal
| Concern | Hand-rolled Postgres queue | Temporal Schedule + sweeper |
|---|---|---|
| Crash recovery | `locked_until` lease + reaper goroutine | `HeartbeatTimeout` → automatic retry with `GetHeartbeatDetails` resume |
| Backoff | `next_retry_at` + manual exponential math | `RetryPolicy{InitialInterval, BackoffCoefficient, MaximumInterval, MaximumAttempts}` |
| Wakeup latency | LISTEN/NOTIFY (~ms) | Schedule tick (≤60 s) — or `Trigger` (~ms) after ingest |
| Dead-lettering | `status='dead'` after attempts≥max | `NonRetryableErrorTypes` / `MaximumAttempts` + `error_message` |
| Rate-limiting embedder | manual semaphore in-process | `TaskQueueActivitiesPerSecond` (server-enforced, across all workers) |
| Observability | SQL counts / `enrichment_progress` view | Temporal Web UI (per-run, per-activity) **plus** the SQL view (unchanged) |
| Operational deps | Postgres only | Postgres + Temporal server (+ `temporal`/`temporal_visibility` DBs, UI) |
| App-side LOC | claim/lease/reaper/backoff/NOTIFY state machine | schedule bootstrap + sweeper workflow + one batch activity (net deletion) |

### Scale sanity check (LongMemEval_s worst case ≈ 250k turn-level texts)
With `batchSize=128`, `errgroup=32`, and a 50 ms unary embedder, one batch's embed time is ≈ 128 / (32 / 0.05 s) ≈ 0.2 s plus S3/PG overhead. A sweep fanning `fanOut=8` batches per wave processes ~1k items per wave; looping waves for a ~50 s soft deadline conservatively drains on the order of low-tens-of-thousands of *cold* items per 1-minute run (bounded in practice by the ~600/s task-queue rate cap = ~30k/min ceiling). A cold 250k backlog therefore drains in roughly a handful of 1-minute cycles; **warm re-runs are near-instant** because `embedding_cache` short-circuits the embed RPC entirely. Tune `fanOut`/`errgroup`/rate cap to trade drain time against embedder protection.

## Recommendations
1. **Stage 1 — stand up Temporal alongside the existing queue.** Add the auto-setup + UI services, point a worker at task queue `enrichment`, and implement `EnrichmentSweepWorkflow` + `ProcessBatch` reading the *existing* schema (ignore the queue columns). Create the schedule paused; run it manually via `Trigger` and watch the UI. **Benchmark to hit:** a warm re-run (fully cached embeddings) drains a LongMemEval_s-scale backlog with zero embed RPCs.
2. **Stage 2 — tune fan-out and rate limit against the real embedder.** Start at `batchSize=128, fanOut=8, errgroup=32, TaskQueueActivitiesPerSecond≈600`. Measure sweep wall-clock. **Threshold to change:** if a cold 250k-item backlog isn't draining within your target number of 1-minute cycles, raise `fanOut`/`errgroup` first; if the embedder shows elevated latency or errors, *lower* `TaskQueueActivitiesPerSecond` — it is the single global throttle.
3. **Stage 3 — cut over and delete the old machinery.** Flip ingest to `Trigger` the schedule, remove LISTEN/NOTIFY, run the DDL diff to drop `locked_until`/`next_retry_at`/`attempts`/`max_attempts` and the claimable index, and delete the reaper/claim goroutines. Unpause the schedule.
4. **Stage 4 — exploit the new substrate.** Add a one-off `ReenrichWorkflow(enrichment_version)` for model/version bumps and a backfill batch workflow — both trivial now that durable execution + rate limiting exist.
5. **Decision rule on batch-activity vs activity-per-memory:** keep the coarse batch-activity design unless you need *per-item* retry visibility in the Temporal UI. If that ever becomes a requirement, switch to a bounded child-workflow-per-item (sliding-window) pattern — but expect far higher event volume and server load at 250k scale.

## Caveats
- **Version currency:** `go.temporal.io/sdk v1.47.0` (Jul 28, 2026) per pkg.go.dev; the GitHub Releases page showed a cached v1.45.0 at check time. Pin explicitly and re-verify at implementation time. Pin the `auto-setup` and `ui` image tags to versions compatible with your chosen SDK.
- **`*serviceerror.AlreadyExists` for duplicate schedules** is inferred from the serviceerror package definition ("general AlreadyExists gRPC error") plus cross-SDK behavior (server returns gRPC `ALREADY_EXISTS`); the Go docs don't name it explicitly. For maximum robustness also accept `status.Code(err) == codes.AlreadyExists`.
- **Default `MaxConcurrentActivityExecutionSize`** is documented as 200 in current SDK reference material; confirm against the exact SDK version you pin, and set it explicitly rather than relying on the default.
- **The 3–5 events-per-activity figure** is an approximation from Temporal guidance (activity schedule/start/complete plus any timer/heartbeat-timeout events); exact counts depend on retries and heartbeats. Treat history budgeting conservatively.
- **Sharing one Postgres instance** across the app, `temporal`, and `temporal_visibility` databases is fine for a harness but couples their load; for anything beyond benchmarking, give Temporal its own instance and use `temporalio/server` with externally managed schema.
- **Catchup-window and jitter behavior** for Schedules is summarized from Temporal docs; validate the exact default window on your pinned server version if missed-tick behavior matters to your eval reproducibility.

---

**References:**

1. [Schedules - Go SDK | Temporal Documentation (docs.temporal.io)](https://docs.temporal.io/develop/go/workflows/schedules)
2. [Schedules - Go SDK | Temporal Documentation (docs.temporal.io)](https://docs.temporal.io/develop/go/schedules)
3. [Failure detection - Go SDK | Temporal Platform Documentation (docs.temporal.io)](https://docs.temporal.io/docs/go/how-to-heartbeat-an-activity-in-go)
4. [Troubleshoot payload and gRPC message size limit errors | Temporal Platform Documentation (docs.temporal.io)](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
5. [Schedule | Temporal Platform Documentation (docs.temporal.io)](https://docs.temporal.io/schedule)
6. [Troubleshoot missed Schedule Actions | Temporal Platform Documentation (docs.temporal.io)](https://docs.temporal.io/troubleshooting/schedule-missed-actions)
7. [serviceerror package - go.temporal.io/api/serviceerror - Go Packages (pkg.go.dev)](https://pkg.go.dev/go.temporal.io/api/serviceerror)

## User:

> 8/15/2026 16:14:39

help me adjust something. Every memory in postgres is immutable, meaning that adding enrichments to an entry needs to create a new postgres row.

## Assistant:

> 8/15/2026 16:24:59

> Architected immutable append-only schema leveraging anti-joins strategically.
>
> - Deliberating immutability scope and state management implications.
> - Architected immutable append-only schema leveraging anti-joins strategically.
> - launch_extended_search_task
> - **Done**

Your technical design report on immutable, append-only memory enrichment in Postgres is ready. It covers replacing the mutable enrichment side table with an append-only `memory_enrichment_events` ledger anchored by a partial unique index invariant, three claim mechanisms (row-lock SKIP LOCKED, advisory locks, and deterministic id-range partitioning) with a clear recommendation, full DDL with views and functions, exact SQL for pending queries with exponential backoff, Go/Temporal activity diffs, a side-by-side comparison with the mutable design, and pitfalls covering ON CONFLICT partial-index inference, anti-join performance, advisory-lock table sizing, TOAST/compression for 3072-byte embeddings, and autovacuum tuning for insert-only tables. It closes with prioritized rollout recommendations, growth-management guidance via version partitioning, and caveats on duplicate-work windows and historical retention.

**Append-Only Memory Enrichment in Postgres: An Immutable Ledger Design**

# Immutable, Append-Only Memory Enrichment in Postgres: Technical Design

## TL;DR
- Replace the mutable `memory_enrichment` side table with an **append-only `memory_enrichment_events` ledger**: every attempt/result is an INSERT, never an UPDATE. The "current" enrichment is derived by a `latest_enrichment` view; "pending" is derived by an anti-join. The linchpin invariant is a **partial unique index** `UNIQUE (memory_id, enrichment_version) WHERE status='done'` guaranteeing at-most-one success per (memory, version).
- Recommended claim mechanism is **deterministic id-range partitioning** planned by a small Temporal activity (no locks), with **row-lock claiming (`FOR UPDATE OF memories SKIP LOCKED`)** as the simpler fallback. Both rely on idempotent `INSERT ... ON CONFLICT DO NOTHING` so duplicate embed work from zombie activities is harmless (deterministic embeddings + cache absorb it).
- Three biggest wins: **no reaper/stuck-status machine can exist at all**, **version bumps come for free** (change the version constant → the anti-join re-derives everyone as pending), and **ingest simplifies** (no pending marker row — the `memories` row itself is the outbox). Main costs: anti-join+backoff pending query complexity and unbounded row growth (mitigated by list-partitioning on `enrichment_version` + `DROP PARTITION`).

## Key Findings
- Postgres **partial unique indexes DO work with `ON CONFLICT` inference**, but you must repeat the index predicate verbatim: `ON CONFLICT (memory_id, enrichment_version) WHERE status='done' DO NOTHING`. Omitting the WHERE means the arbiter is not inferred and you get "there is no unique or exclusion constraint matching the ON CONFLICT specification." Additionally, the inserted tuple must itself satisfy the predicate, or Postgres raises "inferred arbiter partial unique index's predicate does not cover tuple proposed for insertion."
- The anti-join pending query is cheap if backed by the right partial index. `NOT EXISTS` is the idiomatic and planner-friendly form (compiles to a Hash/Anti Join or nested-loop anti join); `LEFT JOIN ... IS NULL` is equivalent only when the join column is provably non-null and can mis-estimate cardinality because the planner's IS-NULL selectivity is derived from table-level statistics that do not describe outer-join output.
- **Append-only means fillfactor 100** (the default) — the fillfactor=80 tuning from the mutable design is now actively wrong (wasted space, worse cache/scan density). Autovacuum still matters: per the PostgreSQL 16 official docs (runtime-config-autovacuum), `autovacuum_vacuum_insert_threshold` "Specifies the number of inserted tuples needed to trigger a VACUUM in any one table. The default is 1000 tuples," and `autovacuum_vacuum_insert_scale_factor` default "is 0.2 (20% of unfrozen pages in table)."
- A 768-dim float32 vector is **exactly 3072 bytes**. Per the PostgreSQL 16 docs §73.2 TOAST, "The TOAST management code is triggered only when a row value to be stored in a table is wider than TOAST_TUPLE_THRESHOLD bytes (normally 2 kB)," so the `embedding bytea` will be pushed to TOAST. Out-of-line values are "divided (after compression if used) into chunks of at most TOAST_MAX_CHUNK_SIZE bytes (by default ... about 2000 bytes)." Random float bytes are near-incompressible, so compression wastes CPU for ~0 gain — set the column to `STORAGE EXTERNAL` (out-of-line, no compression attempt).
- **Advisory locks never use fast-path locking** and always consume main shared-lock-table entries (~168 bytes each, a combined LOCK+PROCLOCK figure attributed to Laurenz Albe). Fast-path is reserved for "weak" locks (AccessShareLock, RowShareLock, RowExclusiveLock) with exactly 16 slots per backend in PG16 (per the PostgreSQL source README and PostgresAI's "#PostgresMarathon 2-004"). Per the docs §19.12 the shared table tracks locks on `max_locks_per_transaction * (max_connections + max_prepared_transactions)` objects; with PG16 defaults that is 64 × (100 + 0) = 6,400 slots system-wide, so large per-transaction batches risk "out of shared memory." This makes deterministic partitioning or row-lock claiming preferable to advisory-lock claiming at batch sizes ≥128.

## Details

### 1. Summary of the change

The current design keeps mutable enrichment state (`status pending/done/dead`, `error_message`, `embedded_at`) and UPDATEs it in place. Under strict append-only semantics that is forbidden. The redesign:

- Introduces `memory_enrichment_events`, an insert-only ledger. Each row records the *immutable fact* of one enrichment attempt: what happened (`status`), for which `enrichment_version`, on which `attempt`, and (for successes) the produced `normalized_text`, `lexemes`, `ts`, `embedding`.
- Derives all "current state" via **views and indexes** instead of columns:
    - *Current enrichment* = latest successful row at the max version (via a partial-unique-index-backed `DISTINCT ON` / index scan).
    - *Pending* = `memories` with no `status='done'` event at the target version (anti-join), gated by a backoff+max-attempts predicate.
    - *Dead-letter* = `memories` with ≥ max_attempts failed events (or a permanent failure) at the version (a view, not a status flip).
- **Linchpin invariant**: `CREATE UNIQUE INDEX ... ON memory_enrichment_events (memory_id, enrichment_version) WHERE status='done'`. This makes "latest success" deterministic (at most one success per (memory, version)) and makes completion inserts idempotent under `ON CONFLICT DO NOTHING`. Everything else (harmless duplicate work, crash safety, version bumps) follows from this one invariant.

**Latest-wins semantics, defined precisely.** The current enrichment for a memory is the row with the maximum `enrichment_version` among its `status='done'` rows; within that version the partial unique index guarantees there is exactly one. `id DESC` in the `DISTINCT ON` ordering is a belt-and-suspenders tiebreaker that never actually fires given the invariant. The client always prefers the highest version that has a done row, even if lower versions also have done rows.

**Three biggest wins**
1. *No status machine, no reaper.* There is no `processing` state that can get stuck; a crashed worker either produced a row or it didn't, and the next sweep re-derives pending. There is nothing to reap and no "claimed-but-abandoned" reclamation logic even conceptually.
2. *Version bumps for free.* Bump `CURRENT_ENRICHMENT_VERSION` in Go; the anti-join instantly re-derives every memory as pending at the new version. No bulk `UPDATE ... SET status='pending'` re-enqueue.
3. *Ingest simplification.* No enrichment marker row at ingest. Pending is derived from *absence*, so the `memories` row itself is the durable outbox entry — "never miss a memory" is guaranteed structurally.

**Costs**: the pending query is more complex (anti-join + backoff aggregate); the ledger grows without in-place reuse (bounded by operational partition drops).

### 2. Full DDL

```sql
-- Immutable enrichment ledger. INSERT-only. No UPDATE/DELETE on data rows.
CREATE TABLE memory_enrichment_events (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    memory_id         BIGINT      NOT NULL REFERENCES memories(id),
    enrichment_version SMALLINT   NOT NULL,
    attempt           INT         NOT NULL,          -- 1-based per (memory,version)
    status            TEXT        NOT NULL
                        CHECK (status IN ('done','failed')),
    permanent         BOOLEAN     NOT NULL DEFAULT false,  -- 'dead' derived: failed AND permanent
    error_message     TEXT,                          -- NULL for done
    normalized_text   TEXT,                          -- NULL unless done
    lexemes           TEXT[],                        -- NULL unless done
    ts                TIMESTAMPTZ,                   -- content-derived timestamp, NULL unless done
    embedding         BYTEA,                         -- 3072-byte L2-normalized f32, NULL unless done
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 100);   -- insert-only: keep pages fully packed (this is the default)

-- Store the incompressible 3072-byte vector out-of-line, skip the futile compression pass.
ALTER TABLE memory_enrichment_events
    ALTER COLUMN embedding SET STORAGE EXTERNAL;

-- LINCHPIN INVARIANT: at most one success per (memory, version).
CREATE UNIQUE INDEX uq_enrich_success
    ON memory_enrichment_events (memory_id, enrichment_version)
    WHERE status = 'done';

-- Backoff support: locate failed attempts fast by (memory, version, recency).
CREATE INDEX ix_enrich_attempts
    ON memory_enrichment_events (memory_id, enrichment_version, created_at)
    WHERE status = 'failed';

-- Note: uq_enrich_success already serves the "is there a done row at V?" existence probe
-- as an index-only scan, so no separate anti-join support index is needed for successes.

-- Insert-only autovacuum tuning (freezing + visibility map health for index-only scans).
-- PG16 defaults: insert_threshold=1000, insert_scale_factor=0.2. Tighten so the VM stays current.
ALTER TABLE memory_enrichment_events SET (
    autovacuum_vacuum_insert_threshold = 2000,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
```

**Views**

```sql
-- Current enrichment per memory: latest success at the highest version that has a success.
-- DISTINCT ON is the fastest single-row-per-group form and uses uq_enrich_success ordering.
CREATE VIEW latest_enrichment AS
SELECT DISTINCT ON (memory_id)
       memory_id, enrichment_version, normalized_text, lexemes, ts, embedding
FROM   memory_enrichment_events
WHERE  status = 'done'
ORDER  BY memory_id, enrichment_version DESC, id DESC;

-- Version-pinned enrichment for reproducible eval (client passes V):
CREATE VIEW enrichment_at_version AS
SELECT memory_id, enrichment_version, normalized_text, lexemes, ts, embedding
FROM   memory_enrichment_events
WHERE  status = 'done';   -- caller adds: AND enrichment_version = $V

-- Version-scoped progress: total / done / remaining / dead AT a version.
-- Parameterized as a function to keep it version-pinned for benchmark comparability.
CREATE FUNCTION enrichment_progress(p_conversation BIGINT, p_version SMALLINT)
RETURNS TABLE(total BIGINT, done BIGINT, remaining BIGINT, dead BIGINT)
LANGUAGE sql STABLE AS $$
    SELECT
      count(*) AS total,
      count(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'done')) AS done,
      count(*) FILTER (WHERE NOT EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'done')) AS remaining,
      count(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id AND e.enrichment_version = p_version
            AND e.status = 'failed' AND e.permanent)) AS dead
    FROM memories m
    WHERE m.conversation_id = p_conversation;
$$;

-- Dead-letter view: memories whose failures are permanent or exhausted at version V.
CREATE FUNCTION dead_letter(p_version SMALLINT, p_max_attempts INT)
RETURNS TABLE(memory_id BIGINT) LANGUAGE sql STABLE AS $$
    SELECT m.id
    FROM memories m
    WHERE NOT EXISTS (
        SELECT 1 FROM memory_enrichment_events e
        WHERE e.memory_id = m.id AND e.enrichment_version = p_version
          AND e.status = 'done')
      AND (
        EXISTS (SELECT 1 FROM memory_enrichment_events e
                WHERE e.memory_id = m.id AND e.enrichment_version = p_version
                  AND e.status='failed' AND e.permanent)
        OR
        (SELECT count(*) FROM memory_enrichment_events e
         WHERE e.memory_id = m.id AND e.enrichment_version = p_version
           AND e.status='failed') >= p_max_attempts
      );
$$;
```

### 3. Exact SQL

**Pending query with backoff + max-attempts** (the core CountBacklog / claim predicate):

```sql
-- Pending at version V: no success yet, not dead, and past the backoff window.
-- Backoff = exponential in the number of prior failed attempts.
WITH fail AS (
    SELECT memory_id,
           count(*)          AS n_fail,
           max(created_at)    AS last_fail,
           bool_or(permanent) AS has_permanent
    FROM   memory_enrichment_events
    WHERE  enrichment_version = $1        -- V
      AND  status = 'failed'
    GROUP  BY memory_id
)
SELECT m.id
FROM   memories m
LEFT   JOIN fail f ON f.memory_id = m.id
WHERE  NOT EXISTS (
          SELECT 1 FROM memory_enrichment_events e
          WHERE e.memory_id = m.id
            AND e.enrichment_version = $1
            AND e.status = 'done')        -- ANTI-JOIN: not done
  AND  COALESCE(f.has_permanent, false) = false
  AND  COALESCE(f.n_fail, 0) < $2         -- max_attempts
  AND  (f.last_fail IS NULL
        OR f.last_fail < now() - ($3::interval * power(2, f.n_fail)))  -- backoff
ORDER  BY m.id
LIMIT  $4;                                 -- batch size N
```

The `NOT EXISTS` probe rides `uq_enrich_success`; the `fail` CTE aggregates over `ix_enrich_attempts`. Both are partial indexes, so they only contain the relevant subset (successes / failures), keeping them small. Using the *same* predicate in CountBacklog and in the claim avoids livelock: a transiently-failed item only re-appears as pending once its exponential backoff window has elapsed.

**Claim option (1): row-lock the immutable parent** (simplest; claim-then-release):

```sql
BEGIN;
SELECT m.id
FROM   memories m
WHERE  NOT EXISTS (SELECT 1 FROM memory_enrichment_events e
                   WHERE e.memory_id=m.id AND e.enrichment_version=$1 AND e.status='done')
  AND  /* backoff predicate as above */
ORDER  BY m.id
FOR    UPDATE OF m SKIP LOCKED
LIMIT  $2;
-- release immediately (COMMIT), then embed outside the tx; results are idempotent inserts.
COMMIT;
```

Locking the immutable `memories` rows is purely a mutex — no columns are written. Because tx-scoped locks release at COMMIT, you either (a) hold the tx open across the gRPC embed calls (bad: long transactions) or (b) claim-then-release and accept rare duplicate work on overlap. Choose (b): the partial unique index + `ON CONFLICT DO NOTHING` dedupe the result, and duplicate embed RPCs are absorbed by `embedding_cache`.

**Claim option (2): advisory lock** (analyzed, not recommended at batch scale — see Pitfalls):

```sql
-- per candidate id:
SELECT pg_try_advisory_xact_lock($memory_id);
```

**Claim option (3): deterministic partition planning** (recommended):

```sql
-- Planning activity: compute k disjoint id ranges over the pending set using NTILE.
WITH pending AS (
    SELECT m.id
    FROM   memories m
    WHERE  NOT EXISTS (SELECT 1 FROM memory_enrichment_events e
                       WHERE e.memory_id=m.id AND e.enrichment_version=$1 AND e.status='done')
      AND  /* backoff predicate */
), bucketed AS (
    SELECT id, ntile($2) OVER (ORDER BY id) AS bucket   -- $2 = fanOut (e.g. 16)
    FROM pending
)
SELECT bucket, min(id) AS lo, max(id) AS hi, count(*) AS n
FROM   bucketed GROUP BY bucket ORDER BY bucket;
-- Each ProcessBatch activity then scans WHERE id BETWEEN lo AND hi with the same predicate.
```

Because sibling `ProcessBatch` activities within a sweep are handed disjoint id ranges, they never overlap **by construction — no locks at all**. Overlap policy Skip prevents overlap across sweeps. The only residual overlap is a zombie activity from a crashed prior sweep; idempotent inserts make that harmless. Keyset ranges (`min`/`max` id per bucket) are preferable to `NTILE`-emitted id lists because the payload is tiny (16 ranges × 2 bigints) and the worker re-runs the cheap predicate on its slice rather than trusting a possibly-stale id list.

**Completion insert (idempotent, never UPDATE):**

```sql
INSERT INTO memory_enrichment_events
    (memory_id, enrichment_version, attempt, status,
     normalized_text, lexemes, ts, embedding)
VALUES ($1,$2,$3,'done',$4,$5,$6,$7)
ON CONFLICT (memory_id, enrichment_version) WHERE status='done'
DO NOTHING;                          -- predicate MUST be repeated for arbiter inference
```

**Failure insert:**

```sql
INSERT INTO memory_enrichment_events
    (memory_id, enrichment_version, attempt, status, permanent, error_message)
VALUES ($1,$2,$3,'failed',$4,$5);    -- no ON CONFLICT: failures are free-form facts
```

### 4. Go / Temporal diffs

**ProcessBatch activity** (claim → embed → append):

```go
func (a *Activities) ProcessBatch(ctx context.Context, in BatchInput) (BatchResult, error) {
    // in.Lo/in.Hi = deterministic id range (option 3), or claim via SKIP LOCKED (option 1)
    rows, err := a.db.Query(ctx, pendingInRangeSQL, in.Version, in.Lo, in.Hi, in.MaxAttempts, a.backoff)
    // ...
    g, gctx := errgroup.WithContext(ctx)
    g.SetLimit(32) // unchanged embedder concurrency
    for _, m := range mem {
        m := m
        g.Go(func() error {
            activity.RecordHeartbeat(gctx, m.ID)                 // unchanged heartbeat
            blob, _ := a.s3.Get(gctx, m.Bucket, m.Key)
            norm := normalize(blob)
            vec, cached, err := a.embedWithCache(gctx, norm)     // embedding_cache unchanged
            if err != nil {
                if permanent(err) {
                    a.insertFailed(gctx, m.ID, in.Version, m.Attempt+1, true, err.Error())
                    return nil                                    // permanent: no retry
                }
                a.insertFailed(gctx, m.ID, in.Version, m.Attempt+1, false, err.Error())
                return nil                                        // transient: reappears after backoff
            }
            // NEVER UPDATE. Idempotent append; ON CONFLICT DO NOTHING dedupes zombie/dup work.
            return a.insertDone(gctx, m.ID, in.Version, m.Attempt+1, norm, lex(norm), tsOf(m), vec)
        })
    }
    return res, g.Wait()
}
```

**Status taxonomy.** Keep `status IN ('done','failed')` plus a `permanent` boolean; "dead" is *derived* (`failed AND permanent`, or `failed count ≥ max_attempts`) rather than a third stored state. This keeps the ledger a record of what physically happened (an embed either succeeded or failed) and pushes policy (is this memory dead?) into views, where it can change without rewriting history.

**CountBacklog activity** now runs the anti-join + backoff pending count — using the *same predicate* as the claim to avoid livelock (a failed item only re-counts as pending after its backoff window elapses):

```go
func (a *Activities) CountBacklog(ctx context.Context, version int16) (int, error) {
    var n int
    err := a.db.QueryRow(ctx, pendingCountSQL, version, a.maxAttempts, a.backoff).Scan(&n)
    return n, err
}
```

**Planning activity** (option 3) returns ranges only (payload trivial — e.g. 16 ranges × 2 int64):

```go
type IDRange struct{ Bucket int; Lo, Hi, N int64 }
func (a *Activities) PlanRanges(ctx context.Context, version int16, fanOut int) ([]IDRange, error)
```

**Re-enrichment version-bump flow.** Bump `CURRENT_ENRICHMENT_VERSION` (a code constant). On the next sweep, `CountBacklog(V+1)` reports the full corpus as pending because no memory has a `done` row at V+1 yet — no re-enqueue write is needed. Old-version rows remain as history. During rollout the corpus is *mixed* (some memories done at V+1, some only at V); the progress function and fetch are **version-scoped** so a benchmark run pins to one version and waits for `enrichment_progress(conv, V) → remaining=0` before reading, guaranteeing a homogeneous corpus.

**Version-pinned fetch / progress RPC signatures** (client eval targets a fixed version to avoid mixed-version corpora):

```protobuf
rpc FetchAllMemories(FetchReq) returns (stream EnrichedMemory);
message FetchReq { int64 conversation_id = 1; int32 enrichment_version = 2; } // version REQUIRED

rpc GetProgress(ProgressReq) returns (Progress);
message ProgressReq { int64 conversation_id = 1; int32 enrichment_version = 2; }
message Progress { int64 total=1; int64 done=2; int64 remaining=3; int64 dead=4; }
```

`FetchAllMemories` streams:

```sql
SELECT e.memory_id, e.normalized_text, e.lexemes, e.ts, e.embedding,
       m.content_hash, m.s3_key
FROM   enrichment_at_version e
JOIN   memories m ON m.id = e.memory_id
WHERE  m.conversation_id = $1 AND e.enrichment_version = $2
ORDER  BY e.memory_id;
```

**Ingest change.** Previously the server inserted a `memories` row plus an enrichment `pending` marker in one tx. Now it inserts only the `memories` row (after the S3-first blob write). Pending is derived by absence, so the `memories` row *is* the outbox entry — no separate marker is needed to guarantee "never miss a memory."

**Schedules / overlap / heartbeats: unchanged.** The 1-minute Schedule with overlap policy Skip, the fan-out topology, and heartbeat semantics are all identical. Skip (the Temporal default) means a new scheduled run is not started while the prior run is still in flight, which continues to prevent cross-sweep overlap; idempotent inserts handle the zombie-activity edge case.

### 5. Comparison table

| Dimension | Mutable state machine (old) | Append-only ledger (new) |
|---|---|---|
| Crash recovery | `processing` rows can get stuck → reaper needed | No `processing` state possible; next sweep re-derives pending. No reaper. |
| Retries / backoff | UPDATE `status`, `error_message`, retry counters in place | Declarative: count/max(created_at) over `failed` rows + backoff predicate |
| Re-enrichment (version bump) | Bulk UPDATE to reset `status='pending'` | Change version constant; anti-join re-derives pending. Zero writes. |
| Bloat / vacuum profile | UPDATE churn → dead tuples, needs fillfactor 80 + aggressive vacuum | Insert-only, fillfactor 100; vacuum only for freeze/VM (insert-threshold) |
| Query complexity | Trivial `WHERE status='pending'` | Anti-join + backoff aggregate (more complex, but index-cheap) |
| Auditability | History destroyed on UPDATE | Full attempt history retained (every failure + success is a durable fact) |
| Duplicate-work safety | UPSERT could clobber | `ON CONFLICT DO NOTHING` on partial unique = at-most-one success |

### 6. Pitfalls

- **`ON CONFLICT` partial-index inference**: you must repeat the exact predicate (`WHERE status='done'`). If omitted, Postgres will not infer the partial index as arbiter. Per the PostgreSQL docs the arbiter is chosen by unique-index inference, and the medium/CyberTec write-ups confirm the fix is "predicates of the index (WHERE …) must be added after the ON CONFLICT clause." Also, the inserted tuple must satisfy the predicate — inserting a `failed` row with `ON CONFLICT (...) WHERE status='done'` raises "inferred arbiter partial unique index's predicate does not cover tuple proposed for insertion." So only *completion* inserts carry the ON CONFLICT clause; failure inserts do not.
- **Anti-join performance cliff**: without a partial index on successes, the `NOT EXISTS` degrades to scanning all events per memory. `uq_enrich_success` makes the existence probe an index-only scan (the visibility map must be current — see autovacuum). Prefer `NOT EXISTS` over `LEFT JOIN ... IS NULL`; the latter can mis-estimate cardinality because, as a Postgres committer thread documents, "the selectivity for 'IS NULL' is estimated using the table-level statistics [which] the LEFT JOIN entirely breaks."
- **Advisory-lock table sizing**: advisory locks bypass fast-path and always consume main shared-lock-table entries (~168 bytes each). With PG16 defaults (`max_locks_per_transaction=64`, `max_connections=100`) the whole table is ~6,400 slots system-wide; a single txn taking 1024 advisory locks uses ~168 KB / ~16% of it, and as few as ~6 concurrent claimers (6400/1024) could exhaust it and trigger "out of shared memory / You might need to increase max_locks_per_transaction." At batch 128 the per-txn cost is ~21 KB but ~50 concurrent claimers still exhaust the default table. This is why deterministic partitioning / row-locks beat advisory locks at batch ≥128 unless you raise `max_locks_per_transaction` to cover `peak_concurrent_claimers × batch_size`. (Raising it is cheap — Tom Lane noted a 1M-slot table is ~160 MB — but it requires a restart and is avoidable here.)
- **Autovacuum on insert-only**: rely on PG13+ insert-triggered vacuum (`autovacuum_vacuum_insert_threshold=1000`, `insert_scale_factor=0.2` in PG16); tighten per-table so the visibility map stays current for index-only scans. Do NOT keep fillfactor 80 — it only benefits HOT updates, which never occur on an insert-only table, and just wastes 20% of every page.
- **TOAST / compression of embeddings**: 3072-byte f32 vectors exceed the ~2KB threshold and are near-incompressible (LZ4's early-abort will bail and pglz needs ≥25% gain to accept); set `STORAGE EXTERNAL` to store them out-of-line and skip the wasted compression pass entirely. LZ4 vs pglz choice is moot for random floats.
- **Mixed-version corpora during rollout**: mid-rollout, some memories are done at V+1 and some only at V. Always pin the eval to a version (RPC arg) and wait for `progress(V)=remaining 0` so the benchmark corpus is frozen and comparable. MVCC additionally gives each query a consistent snapshot, so a single version-pinned `FetchAllMemories` stream sees a coherent point-in-time corpus.
- **View vs materialized view**: plain views are correct at 250k–1M rows because MVCC gives each query a consistent snapshot and the indexes make the anti-join/DISTINCT ON cheap. Only reach for a materialized view (or `pg_cron` refresh) if progress polling becomes a hot loop; `REFRESH MATERIALIZED VIEW` takes an ACCESS EXCLUSIVE lock (or CONCURRENTLY with a unique index) and recomputes the entire result set regardless of how little changed, so it is usually not worth it here.

### 7. Table growth management

At 768-dim f32, each success row carries a 3072-byte embedding plus normalized text and lexemes ≈ ~3.2KB. A full re-enrichment generation over 250k memories is ~800MB–1GB — trivial for a benchmark harness, so **keeping full history is the default**. If many re-enrichment generations accumulate, list-partition the ledger by `enrichment_version` and `DROP PARTITION` (or `DETACH PARTITION CONCURRENTLY`) for superseded versions. Per the PostgreSQL partitioning docs, "Dropping an individual partition using DROP TABLE, or doing ALTER TABLE DETACH PARTITION, is far faster than a bulk operation [and] entirely avoid the VACUUM overhead caused by a bulk DELETE." Present this as an explicit **operator decision**: dropping/detaching a whole partition is a table-lifecycle metadata operation, not a row-level UPDATE/DELETE, so it is compatible with "immutable rows" (no row is ever mutated; whole generations are retired atomically). If strict "never destroy any historical fact" is required, keep all generations and accept linear growth.

### 8. What stays the same
- Retrieval stack (BM25 + cosine → RRF k=60 → temporal boost → MMR), client-side ranking.
- nomic-embed-text-v1.5, 768 dims, mandatory `search_document: `/`search_query: ` prefixes, L2-normalized f32 as bytea. Per the nomic-ai/nomic-embed-text-v1.5 model card, "the text prompt must include a task instruction prefix... you embed your documents as `search_document: <text here>` and embed your user queries as `search_query: <text here>`."
- `embedding_cache(content_hash, model, task_prefix → vector)` — already append-only via `INSERT ... ON CONFLICT DO NOTHING`; unchanged and confirmed compatible.
- S3-first ingest ordering (blob → then Postgres row).
- Temporal Schedule (1-min, overlap Skip), fan-out topology, heartbeats — unchanged.
- docker-compose (postgres:16, minio+mc, temporal auto-setup+ui, embedded worker), one-click run.sh.

## Recommendations
1. **Adopt the ledger + partial-unique-index invariant first.** Ship `memory_enrichment_events`, `uq_enrich_success`, the completion insert with `ON CONFLICT (...) WHERE status='done' DO NOTHING`, and the derived views. This alone delivers immutability, crash-safety, and free version bumps.
2. **Start with row-lock claiming (option 1)** for its simplicity; measure. Move to **deterministic id-range partitioning (option 3)** if you observe claim contention or want to eliminate even the short claim transaction. **Do not use advisory-lock claiming (option 2)** at batch sizes ≥128 unless you raise `max_locks_per_transaction` to cover `peak_concurrent_claimers × batch_size`.
3. **Set fillfactor 100 and `STORAGE EXTERNAL`** on the embedding column from day one; tune insert autovacuum as in the DDL.
4. **Version-pin every client fetch and progress call.** Gate eval runs on `enrichment_progress(conv, V)` reporting `remaining = 0`.
5. **Defer partitioning.** Keep full history until storage crosses a threshold you care about (e.g. >10 generations / tens of GB); then list-partition by `enrichment_version` and `DROP PARTITION` for retired versions.

**Benchmarks that would change these:** if the anti-join pending count exceeds ~50 ms at your scale, add a covering index or materialize progress; if claim contention appears as lock waits in `pg_stat_activity`, switch to option 3; if the ledger's TOAST table dominates storage growth, enable version partitioning earlier; if you ever need advisory locks and see "out of shared memory," that is the signal to raise `max_locks_per_transaction` or shrink batch size.

## Caveats
- Row-lock claiming with immediate release (claim-then-embed-outside-tx) permits a small window of duplicate embed RPCs for the same memory if two sweeps/zombies overlap; this is harmless (deterministic embeddings + cache + `ON CONFLICT DO NOTHING`) but does cost a few redundant RPCs. Holding the tx open across the embed calls would eliminate it at the cost of long transactions during gRPC calls — not recommended.
- The 168-byte-per-lock figure is an order-of-magnitude estimate (architecture-dependent, combined LOCK+PROCLOCK); the nominal lock-table formula is a sizing target with ~10% slop plus a ~100 KB safety margin (per Tom Lane), not a hard ceiling. The PG18 fast-path expansion (arrays sized by `max_locks_per_transaction`) does *not* help advisory locks — do not let it create false reassurance.
- `DROP PARTITION` for superseded versions is an operator policy choice; if strict "never destroy any historical fact" is required, keep all generations and accept linear storage growth.
- Index-only scans on the partial success index require the visibility map to be current, which is why insert-triggered autovacuum tuning matters.
- Postgres 19 is slated to change the default TOAST compression from pglz to LZ4, but this is irrelevant to the embedding column once it is set `STORAGE EXTERNAL` (no compression is attempted regardless of the default).

---

**References:**

1. [INSERT ... ON CONFLICT error messages (postgresql.org)](https://www.postgresql.org/message-id/5548E727.6040201%40iki.fi)
2. [PostgreSQL Documentation: autovacuum_vacuum_insert_threshold parameter (postgresqlco.nf)](https://postgresqlco.nf/doc/en/param/autovacuum_vacuum_insert_threshold/)
3. [PostgreSQL: Documentation: 16: 73.2. TOAST (postgresql.org)](https://www.postgresql.org/docs/16/storage-toast.html)
4. [PostgreSQL: Documentation: 18: 66.2. TOAST (postgresql.org)](https://www.postgresql.org/docs/current/storage-toast.html)
5. [Schedule | Temporal Platform Documentation (docs.temporal.io)](https://docs.temporal.io/schedule)
6. [why postgresqls on conflict cannot find my partial unique index 552327b85e1 (betakuang.medium.com)](https://betakuang.medium.com/why-postgresqls-on-conflict-cannot-find-my-partial-unique-index-552327b85e1)
7. [Monitor PostgreSQL HOT Updates and Fillfactor | Postgres Scripts (postgresscripts.com)](https://www.postgresscripts.com/post/monitor-postgresql-hot-updates-and-fillfactor/)
8. [Refreshing PostgreSQL Materialized Views Without Downtime - DEV Community (dev.to)](https://dev.to/data_with_jelimo/refreshing-postgresql-materialized-views-without-downtime-28n6)
9. [nomic-ai/nomic-embed-text-v1.5 · Hugging Face (huggingface.co)](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5)

## User:

> 8/15/2026 17:35:16

How do i test this locally?

## Assistant:

> 8/15/2026 17:38:29

> Architected comprehensive local testing infrastructure with containers and mocks.
>
> - Architected comprehensive local testing infrastructure with containers and mocks.
> - **Done**

Testing this has a natural tier structure — most of it shouldn't need Temporal, Postgres, or the real embedder running. Let me build you a runnable scaffold.

> Created 6 files, presented file
>
> - Main local testing guide covering all five test tiers
> - docker-compose for the full local stack
> - One-click run/eval script
> - Deterministic mock unary gRPC embedder with fault injection
> - Postgres integration tests asserting the append-only invariants
> - Makefile wiring the test tiers
> - Presenting file(s)...
> - **Done**

Five tiers, and the useful insight is that most of what determines your benchmark score is testable with no infrastructure at all.

**The two highest-value tests in the whole suite:**

1. **The immutability guard trigger** (test-schema only) — a `BEFORE UPDATE OR DELETE` trigger that raises on the ledger tables. Any accidental in-place update anywhere in your codebase becomes a loud CI failure. Without it, a stray `UPDATE` passes every other test silently while quietly violating your core design constraint.

2. **The ablation gate** — `make ablation` runs bm25 / dense / hybrid back to back. If hybrid doesn't beat BM25-only by roughly +9pp on LongMemEval-S, your embedding path is broken. That single comparison catches missing nomic prefixes, missing L2 normalization, and dimension mismatches all at once, in a way no unit test can.

**On the mock embedder:** it hashes the full input text (prefix included) into a deterministic unit vector. That's deliberate — a missing or doubled prefix produces a visibly different vector, so prefix bugs show up as recall drops even in fixture runs. Fault injection covers the three failure classes your Temporal design distinguishes: transient (`MOCK_FAIL_RATE` → retry), hard outage (`MOCK_FAIL_UNTIL` → backoff then drain), and permanent (`MOCK_BAD_DIMS_RATE` → dead-letter, never retry). `make e2e-chaos` runs all three together.

One gotcha worth flagging in Tier 1: Temporal's test environment auto-skips timers, so a sweeper whose only exit is the 50s soft deadline will spin through simulated time fast but still burn iterations. Mock `CountBacklog` to return 0 eventually, and test the deadline-exit path separately with a backlog that never drains.

> File: [/mnt/user-data/outputs/TESTING.md](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2FTESTING.md)

> File: [/mnt/user-data/outputs/docker-compose.yml](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2Fdocker-compose.yml)

> File: [/mnt/user-data/outputs/run.sh](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2Frun.sh)

> File: [/mnt/user-data/outputs/Makefile](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2FMakefile)

> File: [/mnt/user-data/outputs/tools/mockembedder/main.go](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2Ftools%2Fmockembedder%2Fmain.go)

> File: [/mnt/user-data/outputs/internal/store/append_only_test.go](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2Finternal%2Fstore%2Fappend_only_test.go)

## User:

> 8/16/2026 11:39:58

I have downloaded all these attachments. Now give me a prompt for an agent to create a local project and build/run these.

## Assistant:

> 8/16/2026 11:40:59

> Created file, presented file
>
> - Agent prompt for scaffolding and building the project from the design artifacts
> - Presenting file(s)...
> - **Done**

Structured as six phases with a verification gate on each, so the agent can't drift far before something fails loudly.

Two things I deliberately front-loaded:

**The hard-constraints block.** Coding agents tend to "helpfully" reach for an LLM reranker, swap `BYTEA` for pgvector, or batch the embedder calls — all reasonable instincts that violate your requirements. Stating them as bugs rather than preferences, up front, is what keeps them from creeping in.

**Phase 2 before everything else.** The Postgres invariant tests already exist and are the strictest specification in the whole package. Getting `append_only_test.go` green early means the append-only design is locked in before any code is written on top of it.

One gap worth knowing about: `backdateViaFixtureTable` in the provided test file needs a way to insert an aged failure row without mutating it afterward. I've told the agent to add an optional `created_at` parameter to the insert path rather than doing the obvious-but-forbidden `UPDATE`. If you'd prefer a different approach there, say so before you hand this over.

You'll also want to rename the four design artifacts to match the filenames in the prompt (`design-retrieval.md`, `design-storage.md`, `design-temporal.md`, `design-append-only.md`) — or edit the prompt's artifact list to match whatever you saved them as.

> File: [/mnt/user-data/outputs/AGENT_PROMPT.md](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2FAGENT_PROMPT.md)

## User:

> 8/16/2026 11:46:38

references five design docs (design-retrieval.md, design-storage.md, design-temporal.md, design-append-only.md, architecture-diagrams.md) plus tools/mockembedder/, but they aren't
in the artifacts/ directory.

## Assistant:

> 8/16/2026 11:48:15

> Diagnosed missing file artifacts and devised materialization strategy.
>
> - Diagnosed missing file artifacts and devised materialization strategy.
> - **Done**

Right — the four design docs were rendered as chat artifacts, not written to disk, so they never landed in your download. And nested paths flatten on download, so `tools/mockembedder/main.go` likely arrived as a bare `main.go`. Let me fix the prompt so it doesn't depend on exact filenames, and give you a setup file that maps everything.

> Edited 5 files, created file, presented file
>
> - Make the artifact list filename-agnostic and add a missing-file precondition check
> - Renumber phases now that Phase 0 exists and remove stale filename references
> - Remove filename-specific references in later phases
> - Fix remaining doc-name references in phases 3-6
> - Fix Tier 1 reference to the testing doc
> - Setup guide mapping chat artifacts to expected repo files
> - Presenting file(s)...
> - **Done**

The four design docs were rendered as chat artifacts rather than written to disk, so they never made it into your download. They're still in the conversation — each artifact panel has a copy/download control. `SETUP.md` maps every artifact title to the filename it should become.

I made two changes to the agent prompt:

**Filenames no longer matter.** The prompt now tells the agent to list and read every markdown file in the directory and identify which document covers which of the five required topics *by content*, not by filename. So whatever you name them, it'll sort itself out.

**Added a Phase 0 precondition.** If any of the five topics isn't covered by a readable document, the agent stops and names the missing one instead of proceeding. This matters because the docs contain exact DDL, the `ON CONFLICT ... WHERE status='done'` index predicate, and the RRF/MMR constants — things an agent would confidently reconstruct wrong from surrounding context. A hard stop is much cheaper than a plausible-looking guess.

Two paths are load-bearing and must match exactly: `tools/mockembedder/main.go` (compose build context) and `internal/store/append_only_test.go` (the `make test-integration` target). Downloads flatten nested paths, so both probably arrived bare — the prompt now tells the agent to relocate flattened files before building, but it's cleaner if you place them yourself.

> File: [/mnt/user-data/outputs/SETUP.md](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2FSETUP.md)

> File: [/mnt/user-data/outputs/AGENT_PROMPT.md](https://claude.ai/api/organizations/7e5adaa2-fc1f-4d10-b2cc-79f236f6bdd3/conversations/303174a3-b4d2-4e2a-bdb4-97d8683cf216/wiggle/download-file?path=%2Fmnt%2Fuser-data%2Foutputs%2FAGENT_PROMPT.md)

Powered by Claude Exporter (https://www.ai-chat-exporter.net)
