# Architecture Diagrams — Go gRPC Agent-Memory System (LoCoMo / LongMemEval)

Companion to the main design doc. Each diagram is given twice: as an ASCII block (renders anywhere) and as Mermaid source (paste into your repo README — GitHub renders it natively).

---

## 1. System overview

```
                ┌─────────────────────────── LOCAL MACHINE ───────────────────────────────┐
                │                                                                          │
 LoCoMo JSON ─┐ │  ┌───────────────────┐  UploadMemories (client-stream)  ┌────────────┐  │
 LongMemEval ─┼─┼─▶│ Go Client          │ ────────────────────────────────▶│ Go gRPC     │  │
 (datasets)   │ │  │ · harness/ingest   │                                  │ Server      │  │
              │ │  │ · retrieval (topK) │ ◀──────────────────────────────── │ · post-proc │  │
              │ │  │ · eval metrics     │  FetchAllMemories (server-stream)└──┬──────┬───┘  │
              │ │  └────────┬──────────┘                                      │      │      │
              │ │           │ embed query only                   embed docs   │      │ pgx  │
              │ │           │ ("search_query: …")     ("search_document: …")  │      │ COPY │
              │ │           ▼                                                 ▼      ▼      │
              │ │  ┌──────────────────────────────────────────┐        ┌──────────────┐    │
              │ │  │ nomic-embed-text-v1.5 (768d)              │        │ Postgres 16   │    │
              │ │  │ internal HTTP, OpenAI-compatible endpoint │        │ (+ optional   │    │
              │ │  └──────────────────────────────────────────┘        │   pgvector)   │    │
              │ │                                                       └──────────────┘    │
              │ └──────────────────────────────────────────────────────────────────────────┘
              └─▶ evidence annotations feed ONLY the eval module — never the retriever
```

```mermaid
flowchart LR
    subgraph datasets[Datasets]
        L[LoCoMo JSON]
        M[LongMemEval JSON]
    end
    subgraph client[Go Client]
        H[Harness / ingest]
        R[Retrieval: top-k in RAM]
        E[Eval: Recall@k, NDCG, MRR]
    end
    subgraph server[Go gRPC Server]
        P[Post-processing pipeline]
    end
    DB[(Postgres 16
optional pgvector)]
    EMB[nomic-embed-text-v1.5
internal HTTP endpoint]

    L --> H
    M --> H
    H -- "UploadMemories (client-stream)" --> P
    P -- "pgx CopyFrom" --> DB
    DB -- "FetchAllMemories (server-stream)" --> R
    P -- "search_document: batches" --> EMB
    R -- "search_query: q" --> EMB
    R --> E
    datasets -. "evidence annotations (eval only)" .-> E
```

---

## 2. Server-side post-processing pipeline (no LLM)

```
 raw Memory (text, speaker, session date)
        │
        ▼
 ┌──────────────┐  ┌───────────────┐  ┌────────────────┐  ┌────────────────┐
 │ Normalize     │─▶│ Tokenize       │─▶│ Date parse      │─▶│ Round assembly  │
 │ NFKC, lower,  │  │ stopwords,     │  │ → TIMESTAMPTZ   │  │ (user+assistant │
 │ whitespace    │  │ Snowball stem  │  │   (UTC)         │  │  pair; keep     │
 └──────────────┘  └───────────────┘  └────────────────┘  │  turn rows too) │
                                                           └───────┬────────┘
        ┌───────────────────────────────────────────────────────────┘
        ▼
 ┌────────────────────────┐  ┌────────────────┐  ┌──────────────────────────┐
 │ Embed batches (64–256)  │─▶│ Dedup           │─▶│ pgx v5 CopyFrom →         │
 │ "search_document:       │  │ SHA-256 exact;  │  │ memories(text, tsv,       │
 │  [date] speaker: text"  │  │ scope = per     │  │ lexemes, embedding BYTEA, │
 │ retries + L2-normalize  │  │ conversation_id │  │ ts, granularity, meta)    │
 └────────────────────────┘  └────────────────┘  └──────────────────────────┘
```

```mermaid
flowchart LR
    A[raw Memory] --> B[Normalize
NFKC · lowercase]
    B --> C[Tokenize
stopwords · Snowball stem]
    C --> D[Date parse
to UTC TIMESTAMPTZ]
    D --> E[Round assembly
user+assistant pair
keep turn rows]
    E --> F[Embed batches
search_document prefix
L2-normalize]
    F --> G[Dedup
SHA-256, per-conversation scope]
    G --> H[(pgx CopyFrom
memories table)]
```

---

## 3. Client-side retrieval (per query)

```
                     question ──┬─▶ embed "search_query: q" ─▶ ┌─────────────────┐
                                │                              │ Dense top-N      │──┐
 in-RAM memory store            │                              │ cosine over      │  │
 (via FetchAllMemories):        │                              │ []float32 arena  │  │  ┌──────────┐
 ┌───────────────────────┐      └─▶ tokenize/stem ───────────▶ ┌─────────────────┐  ├─▶│ RRF fuse  │
 │ vector arena 768d      │                                    │ BM25 top-N       │──┘  │ k = 60    │
 │ BM25 inverted index    │                                    │ in-mem index     │     └────┬─────┘
 │ timestamps, session ids│                                    └─────────────────┘          │
 └───────────────────────┘                                                                  ▼
                          rule-based date extraction ─────▶ ┌────────────────────────────────┐
                          (araddon/dateparse, olebedev/when)│ Temporal boost ×1.3 / range     │
                                                            │ filter, unfiltered fallback     │
                                                            └───────────────┬────────────────┘
                                                                            ▼
                                                            ┌────────────────────────────────┐
                                                            │ MMR (λ≈0.7) + per-session cap   │
                                                            └───────────────┬────────────────┘
                                                                            ▼
                                                              top-k (k=10) ─▶ eval
                                                              score < τ  ──▶ abstain flag
```

```mermaid
flowchart TD
    Q[question] --> QE[embed search_query: q]
    Q --> QT[tokenize + stem]
    QE --> DN[Dense top-N
cosine over in-RAM arena]
    QT --> BM[BM25 top-N
in-memory index]
    DN --> RRF[RRF fusion, k=60]
    BM --> RRF
    Q --> DP[rule-based date extraction]
    DP --> TB[temporal boost / range filter
with unfiltered fallback]
    RRF --> TB
    TB --> MMR[MMR lambda 0.7
per-session cap]
    MMR --> K[top-k ids, k=10]
    K --> EV[eval: Recall@k · NDCG@k · MRR]
    K -- "top score < threshold" --> AB[abstain]
```

---

## 4. gRPC interaction (sequence)

```
 Client                    Server                     Embedder            Postgres
   │  UploadMemories ────────▶│                           │                   │
   │  (stream Memory…)        │── normalize/tokenize ──┐  │                   │
   │                          │◀───────────────────────┘  │                   │
   │                          │── batch texts ───────────▶│                   │
   │                          │◀── vectors (768d) ────────│                   │
   │                          │── CopyFrom batches ──────────────────────────▶│
   │◀───── UploadAck ─────────│                           │                   │
   │                          │                           │                   │
   │  FetchAllMemories ──────▶│── SELECT stream ─────────────────────────────▶│
   │◀═ stream Memory batches ═│◀──────────────────────────────────────────────│
   │                          │                           │                   │
   │  (per question)          │                           │                   │
   │── "search_query: q" ─────────────────────────────────▶│                  │
   │◀── query vector ──────────────────────────────────────│                  │
   │  rank locally: dense + BM25 → RRF → temporal → MMR → top-k               │
```

```mermaid
sequenceDiagram
    participant C as Go Client
    participant S as gRPC Server
    participant E as nomic embedder
    participant P as Postgres

    C->>S: UploadMemories (stream Memory)
    S->>S: normalize · tokenize · dates · rounds
    S->>E: batch "search_document: [date] speaker: text"
    E-->>S: 768d vectors (L2-normalized)
    S->>P: CopyFrom batches (+ dedup)
    S-->>C: UploadAck {accepted, deduped}

    C->>S: FetchAllMemories(conversation_id)
    S->>P: SELECT stream
    P-->>S: rows
    S-->>C: stream Memory batches
    loop per benchmark question
        C->>E: "search_query: question"
        E-->>C: query vector
        C->>C: dense + BM25 → RRF → temporal → MMR → top-k
    end
    C->>C: score vs evidence → Recall@k, NDCG@k, MRR
```

---

## 5. One-click run/eval flow (`run.sh`)

```
 run.sh
   │
   ├─ docker compose up -d postgres          # postgres:16 (pgvector image optional)
   ├─ wait-for healthy ─▶ goose up           # migrations
   ├─ datasets/download.sh                   # LoCoMo (GitHub raw) + LongMemEval (HF CLI)
   ├─ docker compose up -d server            # gRPC server (+ embedding-mock for CI)
   │
   ├─ client ingest ──UploadMemories──▶ server ──post-process──▶ Postgres
   │
   ├─ client eval ──FetchAllMemories──▶ load to RAM
   │      └─ per qa/question: retrieve top-k ─▶ compare vs evidence annotations
   │
   └─ print metrics table:
        Recall@5 / Recall@10 / NDCG@10 / MRR
        × LoCoMo category (single-hop, multi-hop, temporal, open-domain)
        × LongMemEval question_type (skipping _abs for retrieval)
```

```mermaid
flowchart TD
    A[run.sh] --> B[docker compose up postgres]
    B --> C[goose migrations]
    C --> D[download datasets
LoCoMo · LongMemEval]
    D --> E[start gRPC server
or embedding-mock for CI]
    E --> F[client ingest via UploadMemories]
    F --> G[server post-process to Postgres]
    G --> H[client FetchAllMemories to RAM]
    H --> I[retrieve top-k per question]
    I --> J[metrics table
Recall@5/10 · NDCG@10 · MRR
by category / question_type]
```
