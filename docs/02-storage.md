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
