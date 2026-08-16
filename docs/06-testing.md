# Testing the Agent-Memory System Locally

Five tiers, fastest first. Tiers 0–2 need no Temporal server and no real embedder; run them on every save. Tier 3 is the one-click e2e. Tier 4 is the real benchmark.

| Tier | What | Infra needed | Runtime | Command |
|---|---|---|---|---|
| 0 | Pure unit: RRF, MMR, cosine, BM25, tokenizer, date parsing | none | <1s | `make test-unit` |
| 1 | Temporal workflow/activity logic | none (in-memory test env) | <5s | `make test-workflow` |
| 2 | Postgres append-only invariants | testcontainers (Docker) | ~15s | `make test-integration` |
| 3 | Full e2e on fixtures | docker-compose (all) | ~2min | `./run.sh --fixtures` |
| 4 | Real benchmark eval | docker-compose + real embedder | ~10min+ | `./run.sh --dataset longmemeval_s` |

---

## Tier 0 — Pure unit tests (no infra)

This is where most of your benchmark score actually lives, and none of it needs a database. Test the retrieval math against hand-computed values.

**RRF golden test.** The formula is `RRF(d) = Σ_r 1/(k + rank_r(d))` with k=60. A doc at rank 3 in BM25 and rank 7 in dense scores `1/63 + 1/67 = 0.030793…`. Hard-code that.

```go
func TestRRF(t *testing.T) {
    bm25  := []string{"a","b","c","d","e","f","g"} // c is rank 3 (1-indexed)
    dense := []string{"x","y","z","p","q","r","c"} // c is rank 7
    got := RRF(60, bm25, dense)
    want := 1.0/63.0 + 1.0/67.0
    if math.Abs(got["c"]-want) > 1e-9 {
        t.Fatalf("rrf(c)=%v want %v", got["c"], want)
    }
    // Docs absent from a list contribute 0, not 1/k.
    if got["a"] != 1.0/61.0 { t.Fatalf("rank-1-only doc wrong: %v", got["a"]) }
}
```

**Cosine on normalized vectors must equal dot product.** Assert `|cosine(a,b) - dot(a,b)| < 1e-6` for L2-normalized inputs, and assert `|‖v‖ - 1| < 1e-6` after your `l2normalize`. This catches the single most common silent bug: forgetting to normalize, which makes your "cosine" a dot product on unnormalized vectors and quietly degrades ranking.

**MMR ordering.** With λ=0.7, feed three docs where doc B is highly similar to doc A and doc C is dissimilar but slightly less relevant; assert the selection order is A, C, B. Also assert the per-session cap actually caps: 10 candidates from one session with `maxPerSession=2` must yield exactly 2.

**Nomic prefix guard.** This deserves its own test because omitting the prefix is the highest-cost bug in the system:

```go
func TestEmbedTextsAlwaysPrefixed(t *testing.T) {
    mock := &recordingEmbedder{}
    _ = EnrichOne(ctx, mock, memoryFixture)
    if !strings.HasPrefix(mock.lastText, "search_document: ") {
        t.Fatal("document embedded without search_document: prefix")
    }
    q := NewRetriever(mock)
    _, _ = q.Search(ctx, "when did I go to Paris?", 10)
    if !strings.HasPrefix(mock.lastText, "search_query: ") {
        t.Fatal("query embedded without search_query: prefix")
    }
    // And never double-prefixed:
    if strings.Count(mock.lastText, "search_query: ") != 1 {
        t.Fatal("double prefix")
    }
}
```

**Date parsing table test.** Feed the LoCoMo timestamp formats you actually see (`"1:56 pm on 8 May, 2023"` style free-form strings) and LongMemEval's `haystack_dates`, plus the ambiguous cases (`03/04/2023`), and assert UTC normalization. Use `dateparse.ParseStrict` so ambiguous mm/dd vs dd/mm errors instead of silently guessing.

---

## Tier 1 — Temporal workflow tests (no server)

The Go SDK ships an in-memory test environment. No Temporal server, no Docker.

```go
type SweepSuite struct {
    suite.Suite
    testsuite.WorkflowTestSuite
    env *testsuite.TestWorkflowEnvironment
}

func (s *SweepSuite) SetupTest() { s.env = s.NewTestWorkflowEnvironment() }
func (s *SweepSuite) AfterTest(_, _ string) { s.env.AssertExpectations(s.T()) }

func (s *SweepSuite) TestSweepDrainsThenExits() {
    calls := 0
    s.env.OnActivity(CountBacklog, mock.Anything, mock.Anything).
        Return(func(ctx context.Context, v int16) (int, error) {
            calls++
            if calls == 1 { return 256, nil }
            return 0, nil // drained on second poll
        })
    s.env.OnActivity(ProcessBatch, mock.Anything, mock.Anything).
        Return(BatchResult{Done: 128}, nil).Twice()

    s.env.ExecuteWorkflow(EnrichmentSweepWorkflow)
    s.True(s.env.IsWorkflowCompleted())
    s.NoError(s.env.GetWorkflowError())
}
```

**Critical gotcha:** the test environment auto-skips timers, so a sweeper whose exit condition is only the 50s soft deadline will spin through simulated time very fast but still burn iterations. Always mock `CountBacklog` to return 0 eventually, and add a separate test that asserts the soft-deadline path terminates:

```go
func (s *SweepSuite) TestSoftDeadlineExit() {
    s.env.OnActivity(CountBacklog, mock.Anything, mock.Anything).Return(1_000_000, nil) // never drains
    s.env.OnActivity(ProcessBatch, mock.Anything, mock.Anything).Return(BatchResult{Done: 128}, nil)
    s.env.ExecuteWorkflow(EnrichmentSweepWorkflow)
    s.True(s.env.IsWorkflowCompleted()) // exits on deadline, does not hang
}
```

**Activity tests separately**, with `NewTestActivityEnvironment()`, hitting a testcontainer Postgres and the mock embedder. Test heartbeat resume explicitly: run `ProcessBatch`, kill it mid-way via context cancel, then assert a re-run with `SetHeartbeatDetails(lastIdx)` skips the already-processed prefix.

---

## Tier 2 — Postgres append-only invariants (testcontainers)

These tests are the safety net for the immutable-ledger design. Each one encodes an invariant that, if broken, silently corrupts your eval.

```go
func setupPG(t *testing.T) *pgxpool.Pool {
    ctx := context.Background()
    pg, err := postgres.Run(ctx, "postgres:16-alpine",
        postgres.WithDatabase("app"),
        postgres.WithUsername("app"), postgres.WithPassword("app"),
        testcontainers.WithWaitStrategy(
            wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
    require.NoError(t, err)
    t.Cleanup(func() { _ = pg.Terminate(ctx) })
    dsn, _ := pg.ConnectionString(ctx, "sslmode=disable")
    pool, _ := pgxpool.New(ctx, dsn)
    require.NoError(t, migrate(ctx, pool))
    return pool
}
```

Then assert, one test each:

1. **At-most-one success per (memory, version).** Insert a `done` event twice with `ON CONFLICT (memory_id, enrichment_version) WHERE status='done' DO NOTHING`; assert exactly one row and no error. Then insert *without* the `WHERE` clause and assert it errors — this pins the arbiter-inference gotcha so nobody "simplifies" it later.
2. **Failure rows are not deduped.** Three `failed` inserts must produce three rows (they're distinct historical facts).
3. **Pending is derived by absence.** Insert a memory with no events → appears in the pending query. Add a `done` event at V → disappears. Bump to V+1 → reappears. This one test proves the free-version-bump property.
4. **Backoff gating.** Insert a `failed` event with `created_at = now()`; assert the memory is *not* pending. Backdate it past the backoff window; assert it *is*. Parameterize your backoff base so tests can use milliseconds.
5. **Dead-letter derivation.** `max_attempts` failures → appears in `dead_letter(V, max)`, absent from pending. A `permanent=true` failure → dead immediately regardless of count.
6. **No UPDATE/DELETE ever runs.** Add a guard trigger in the *test* schema only:
   ```sql
   CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
   BEGIN RAISE EXCEPTION 'immutability violated: % on %', TG_OP, TG_TABLE_NAME; END $$;
   CREATE TRIGGER no_mutate BEFORE UPDATE OR DELETE ON memory_enrichment_events
     FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
   ```
   Now any accidental in-place update anywhere in your code fails loudly in CI. This is the highest-value test in the whole suite for the append-only requirement.
7. **Version-pinned fetch returns a homogeneous corpus.** Enrich half the memories at V+1, leave half at V, then assert `FetchAllMemories(conv, V+1)` returns only the V+1 subset — never a mixed-version blend.

---

## Tier 3 — Full e2e on fixtures

`./run.sh --fixtures` brings up Postgres, MinIO, Temporal, the mock embedder, and the server, ingests a tiny hand-built fixture conversation (~20 turns with known evidence), waits for `progress(V).remaining == 0`, runs retrieval, and asserts Recall@5 == 1.0 on a corpus small enough that perfect recall is the only acceptable answer.

The **mock embedder** (`tools/mockembedder`) is deterministic: it hashes the input text into a 768-dim unit vector, so identical text always yields identical vectors and semantically-unrelated text yields near-orthogonal ones. That's enough to exercise the whole pipeline without a real model. It also supports fault injection via env vars:

- `MOCK_LATENCY_MS=50` — simulate the real embedder's latency for throughput math
- `MOCK_FAIL_RATE=0.1` — return `UNAVAILABLE` 10% of the time, to exercise Temporal retries and your backoff query
- `MOCK_FAIL_UNTIL=30s` — hard outage for the first 30 seconds, to prove the sweep drains after recovery
- `MOCK_BAD_DIMS_RATE=0.01` — return 512 dims occasionally, to prove your validator marks those `permanent` and dead-letters them instead of retrying forever

Run the outage scenario as an actual test: start with `MOCK_FAIL_UNTIL=45s`, ingest, and assert that after ~2 minutes progress reaches 100% anyway. That single test exercises the Temporal retry policy, the backoff predicate, and the append-only failure ledger together.

Watch it happen in the Temporal UI at `http://localhost:8080` — you'll see each sweep run, the fan-out activities, and retry attempts with their backoff intervals. MinIO console is at `http://localhost:9001` (app/appsecret) if you want to eyeball the blobs.

---

## Tier 4 — Real benchmark eval

```bash
./run.sh --dataset locomo          # 10 conversations, ~5.9k turns, minutes
./run.sh --dataset longmemeval_s   # 500 questions, the real target
```

**The sanity gate that matters most.** Run three configurations and compare:

```bash
make eval RETRIEVAL=bm25    # expect R@5 ≈ 0.86 on LongMemEval-S
make eval RETRIEVAL=dense   # expect R@5 ≈ 0.93+
make eval RETRIEVAL=hybrid  # expect R@5 ≈ 0.95
```

If hybrid doesn't beat BM25-only by roughly +9pp, **stop and check the prefixes** before tuning anything else. That gap is your integration test for the embedding path as a whole — it catches missing prefixes, missing normalization, and dimension mismatches all at once, in a way no unit test can.

Also assert per-question-type floors so a regression in one category doesn't hide inside a healthy average. Expect single-session-preference to be your worst category (~0.83) — that's normal, not a bug; don't over-tune for it.

Cache behavior is worth verifying explicitly: run `--dataset longmemeval_s` twice and assert the second run makes near-zero embed RPCs (count them in the mock, or check `embedding_cache` row growth). A warm re-run that still hits the embedder means your `content_hash` keying is wrong — probably because you're hashing the raw text instead of the prefixed text, or including a timestamp in the hashed payload.

---

## Practical loop

```bash
make test-unit          # on every save (sub-second)
make test-workflow      # before committing workflow changes
make test-integration   # before pushing
./run.sh --fixtures     # before touching the schema or the pipeline
./run.sh --dataset locomo   # nightly / before a benchmark claim
```

Keep Tier 4 out of CI — it needs the real embedder and takes too long. Tiers 0–3 should run in CI on every PR; Tier 3 with `MOCK_FAIL_RATE=0.05` so flaky-retry paths get exercised continuously rather than only when you remember to test them.
