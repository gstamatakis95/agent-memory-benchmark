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
