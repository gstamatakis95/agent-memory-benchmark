## 8. Testing and evaluation

Every invariant named in the decision register and in §7 has a test that names it. Tests are
organised by *what they prove* (an invariant, a failure mode, a benchmark number), not by
module; the module-level test seams are the ones §2 already lists (`FakeTx`,
`RecordingPool`, `AllowAllVerifier`, `DeterministicClient`, `RecordReplayClient`,
`FakeActivities`, `MemIndex`, `MemStore`, `MemoryCatalog`, `StaticResolver`). Nothing in this
section requires a GPU, a Kubernetes cluster or a paid model except the tier-6 benchmark runs.

### 8.1 Test tiers

| Tier | Name | What it proves | Infra | Time budget | `make` target | CI gate |
|---|---|---|---|---|---|---|
| T0 | Unit | Pure logic per package: chunker, tag modes, RRF, packer, temporal arithmetic, SQL builders (golden), error mapping, config resolution | none (`go test -short`) | < 1 s per package, < 20 s total | `make test-unit` | every push; required |
| T1 | Property | Invariants stated in §8.2 with `pgregory.net/rapid`; each property is the Go twin of a Lean theorem or a TLA+ invariant (§7) | none | `-rapid.checks=200` on push (< 60 s); `-rapid.checks=20000` nightly | `make test-prop` / `make test-prop-deep` | every push (fast); nightly (deep) |
| T2 | Workflow | Temporal `testsuite.WorkflowTestSuite` with `FakeActivities`: fan-out ≤ 32, `ContinueAsNew` every 100 chunks **or** 20 MB of history (N59), **history-size assertion** (`TestRetain_HistoryBudget`: a 500-chunk document with 50-fact chunks never exceeds 10 k events / 20 MB between continue-as-new points, read from `env.GetWorkflowHistory()`; every activity result > 4 KiB travels by blob key and `CommitChunkInput` is keys-only — review F-18 computed ≈ 200 MB for the inline design), idempotent re-execution, deferral on quota, move state machine, expunge phases, consolidation bisect | none (in-memory test env) | < 90 s | `make test-workflow` | every push; required |
| T3 | Integration | Real SQL against the real schema: RLS canary, ownership fencing, `ON CONFLICT` arbiters, outbox ordering, `as_of` per arm, tombstone visibility and evidence segments, exact `Restore`, expunge purge, per-namespace HNSW and BM25 plans **run as `engram_app`**, migrations up/down | testcontainers `paradedb/paradedb:latest-pg16` (pinned digest), **one container per package** started in `TestMain`, migrations applied by `engramctl migrate --dsn` | ≤ 6 min wall (packages run in parallel, `-p 4`) | `make test-integration` | every PR; required |
| T4 | End-to-end | The public contract through Envoy: gRPC, Connect, MCP; 2 shards; a namespace move under load; export round-trip | `docker compose --profile e2e` (§9.1 topology with `DeterministicClient` as the gateway, 1 api, 1 worker, 2 shards, catalog, Temporal, MinIO) | ≤ 12 min | `make e2e` | every PR to `main`; required |
| T5 | Chaos | §8.4 fault matrix: kills, pauses, partitions (toxiproxy), gateway faults, duplicate activities, relay double election | T4 stack + `toxiproxy` + build tag `faultinject` | ≤ 40 min | `make e2e-chaos` | nightly; **required** on PRs touching `internal/{move,outbox,catalog,router,store,workflows,expunge,intent}` (path filter) |
| T6 | Benchmark | §8.5 leakage grid, §8.6 LongMemEval/LoCoMo and the Postgres baselines of 8.6.1 (`make bench-pg`), §8.7 Hindsight side-by-side, §8.8 cost/latency | T4 stack + the real gateway; `bench.lock` pins; the 8.6.1 fixtures run on the ParadeDB image | smoke ≤ 10 min; LME-S ≈ 8–11 h (Table 6.8-B); LME-M ≈ 2–3 days | `make bench-smoke` (PR), `make bench DATASET=lme_s` (weekly), `make bench DATASET=lme_m` (release) | smoke on every PR touching `internal/{recall,chunk,extract,index}`; full weekly; M on release candidates |
| F | Formal conformance | TLC bounded model checks and `lake build` of the Lean modules; trace validation of recorded Go runs against the TLA+ specs (§7) | Java + TLC, Lean 4 toolchain | ≤ 30 min | `make formal` | nightly; required on PRs touching `formal/` or the packages a spec covers |
| S | Static | `golangci-lint`, `buf lint`, `buf breaking --against main`, `go vet`, the RLS policy check (§8.3), the metric-label linter (no `namespace` and no `tenant` label, D13/N62), the SQL predicate linter (§8.3), `govulncheck` | none | < 3 min | `make lint` | every push; required |

CI gates, stated once: a PR merges when S, T0–T3 are green and T4 is green for PRs into
`main`; T5 and F are required only for the path filters above (otherwise nightly, and a red
nightly blocks the next release tag, not the next merge). `make test` = S + T0 + T1 (fast) +
T2 + T3. Flaky-test policy: a test that fails twice in a week without a code change is
quarantined by label (`//go:build !quarantine`) and a ticket is opened; quarantined tests
cannot exceed 5 at any time (a lint rule counts them). Rejected: retries in CI (they hide
timing bugs, and this system's bugs are timing bugs).

Test doubles used across tiers (all defined in §2; listed here so tests are named consistently):

| Double | Package | Knobs used by tests |
|---|---|---|
| `DeterministicClient` (the "fake gateway") | `internal/gateway/fakegw` | `GW_LATENCY_MS`, `GW_FAIL_RATE`, `GW_FAIL_UNTIL` (RFC3339), `GW_STATUS` (force 429/500/400), `GW_BAD_DIMS_RATE`, `GW_RPM` (returns 429 + `Retry-After` above the rate), `GW_EXTRACT_MODE=sentence|verbatim` |
| `RecordReplayClient` | `internal/gateway` | goldens keyed `sha256(model ‖ prompt id ‖ input)`; `-update` re-records |
| `RecordingPool` | `internal/store` | records `(shard_id, namespace_id, statement)` per call; isolation tests assert the set of shards touched |
| `AllowAllVerifier` | `internal/authz` | claims from the `x-test-claims` header; never compiled into release images (build tag `testauth`) |
| `MemStore` | `internal/blob` | `FailPuts`, `FailGets`, `Latency`, key recorder |
| `MemoryCatalog` + `StaticResolver` | `internal/catalog` | inject stale entries, drop notifications, simulate catalog down |
| `FakeActivities` | `internal/workflows` | per-activity failure scripts, duplicate-execution counters |
| `MemIndex` | `internal/index` | exact brute-force cosine and in-memory BM25 for property tests |
| Mover `--fault-at=<step>` | `internal/move` (`faultinject` tag) | panics after the first committed write of the named step; knobs `--recopy-floor=0`, `--id-keyed-recopy`, `--stamp-after-cut`, `--lineage-less-segments` |
| `FakeIntentStore` | `internal/intent` | `FailPuts`, `Latency`, `CrashBeforePut` (commit the marker, then die); strongly consistent list for replay tests |

### 8.2 Unit and property tests per module

The properties below are the test-level statement of the invariants; the column "mirrors"
names the formal artifact in §7 whose theorem or invariant the property restates (names as
§7 defines them). A property test is written once, in Go, and runs at T1; where §7 provides a
Lean decision procedure or a TLC trace, the Go test additionally consumes the generated
golden table (`formal/lean/out/*.json`) or the trace file so that the two cannot drift.

| Module | Unit tests (representative) | Property tests (rapid) — the property | Mirrors |
|---|---|---|---|
| `internal/chunk` | golden boundaries + hashes for `testdata/*.md` (headings, code fences, tables, lists, 10 languages); empty doc; single 20 KB paragraph | **Determinism**: `Chunk(doc) == Chunk(doc)` byte-for-byte, and equal across `GOMAXPROCS`. **Coverage/no overlap**: `concat(chunks) == normalize(doc)`. **Bounds**: every chunk ∈ [500, 4000] chars except a last chunk of a doc < 500. **Locality**: a single edit inside one section changes ≤ 2 content hashes (content-defined boundaries). **Fence integrity**: no boundary inside a fenced code block ≤ 4000 chars. **Header**: `header` ≤ 200 + heading-path chars and never stored in `text`. | — (pure; not formalised, §7) |
| `internal/recall/tagmatch` | 5-mode truth table, 40 hand-written rows (`Q`, `I` over alphabet {a,b,c}, ∅) | **Lean parity**: for random `Q`, `I` ⊆ {a..f}, `Match(mode,Q,I) == table[mode][Q][I]` where `table` is emitted by `lake exe tagmatch-table`. **Implications**: `ALL_STRICT ⇒ ALL`, `ANY_STRICT ⇒ ANY`, `EXACT ⇒ ALL_STRICT`, `Q = ∅ ∧ mode ∈ {ANY, ALL} ⇒ true`, unset filter ⇒ true. | `Engram/TagMatch.lean` |
| `internal/recall/fuse` | k=60 golden in **exact rational arithmetic** (N67, `math/big.Rat`): rank 3 and rank 7 → `1/63 + 1/67 = 130/4221` (≈ 0.030798); empty arm; duplicate ids within an arm rejected; `TestFuse_FloatOrderCounterexample` documents why: summing the same `1/(k+r)` terms as `float64` in two arm orders differs in the last ulp for some rank multisets and can flip a tie, so the property below is false for floats (review F-31) | **Permutation invariance (exact)**: fusing arms in any order yields *bit-identical* `big.Rat` scores and identical tie-broken order (ties by `memory_id`); ranks are 1-based (`rank 1 = top`, as `RRF.lean` and Hindsight's `1/(k + rank)`). **Monotonicity**: improving `d`'s rank in one arm never lowers `score(d)` and never lowers its final position relative to items whose ranks did not change. **Empty-arm neutrality**: `fuse(A ∪ {∅}) == fuse(A)`. **Cap**: output size ≤ Σ arm sizes. **Normalised exposure**: `Scores.lexical`/`Scores.semantic` returned to clients are per-query normalised to [0, 1], never raw BM25 (N67) | `Engram/RRF.lean` |
| `internal/recall/pack` | budgets 4k/8k/16k with cl100k counts; an item exactly at budget; zero budget | **Never exceeds**: Σ tokens(packed) ≤ `max_tokens`. **Order preserved**: packed is a subsequence of ranked. **Skip, not truncate**: every packed item is byte-identical to its input. **Never stops early**: an item that fits after a skipped one is packed; `skipped_count == len(ranked) − len(packed)`. **Determinism** under equal token counts. | `Engram/Packer.lean` |
| `internal/recall/temporal` | date-phrase parser goldens ("last March", "in 2015", "the week before Christmas 2023"), coarse spans (year → Jan 1–Dec 31) | **Well-formed**: parsed `start ≤ end`. **Overlap is symmetric** and reflexive. **Distance** to `query_timestamp` ≥ 0 and 0 inside the window. **Proximity boost** ∈ [0.9, 1.1]; combined boost factor ∈ [0.75, 1.25] for any (recency, temporal, proof) triple. | `Engram/TemporalWindow.lean` |
| `internal/recall` (planner, visibility) | arm caps 50/150/400 by budget; `stage=FUSED` when remaining deadline < 106 ms (`rerank_min_remaining`, generated from the register); stream batches of 10; `RecallStats` trailer (`partial`); the `doc_tomb` and `chunk_tomb` sets loaded once per request and passed as values; invalidations are a PK anti-join; the arm transaction sets `enable_seqscan = off, max_parallel_workers_per_gather = 0`; the eligible-row estimate (Σ `fact_count` of `$allowed_docs`, or the monthly `mentioned_at` histogram) picks the exact path below θ (5 k) | **Cap adherence** over `MemIndex`: each arm returns ≤ cap. **`as_of` inside every arm**: for random corpora and `T`, no arm returns `mentioned_at > T`, and each arm returns `min(cap, n_visible)` rows under every tag, metadata, `fact_type` and `as_of` filter, whatever the selectivity (property 6; `TestRecall_FilteredArm` runs it against `PostgresIndex` on the real image) — the filter is not post-hoc. **Plan equivalence**: the exact path and the HNSW iterative scan return the same set whenever the HNSW scan is not exhausted, and `RecallStats.partial` is set when it is. **Visibility (N116)**: for random corpora and random marker sets (`DocTomb`, `ChunkTomb`, `fact_hidden` of either cause), no arm returns a fact with `document_id ∈ DocTomb ∨ chunk_id ∈ ChunkTomb ∨` a `fact_hidden` row, and the array-parameter path and the SQL anti-join path above 16 k entries return the same set. **Type/tag/metadata filters** commute with fusion, and resolve once to `$allowed_docs` (above 8 k documents the arm joins `documents`). | `Derivation.tla` (`NoDeletedDerivationServed`, `AsOfNoLeak`) |
| `internal/entity` | trigram thresholds (0.3 candidate, 0.6 accept, 0.85 when a type is unknown; §5.1.2) on a seeded set; names > 256 chars dropped; whitespace normalisation | **Idempotence**: `Resolve(B); Resolve(B)` creates no new entity on the second call. **Order insensitivity**: the *set* of `(mention → canonical entity)` assignments is independent of the order of mentions within a batch. **Monotone growth**: `Resolve(B ∪ B')` then `Resolve(B')` creates no new entity. **Namespace locality**: every returned `entity_id` has `namespace_id == scope.namespace`. | — |
| `internal/link` | weights (`max(0.3, 1 − Δh/24)`, cosine ≥ 0.75, causal 1.0); lock-order sort key | **Caps**: temporal links per fact ≤ 20; semantic ≤ 10 per fact and each with cosine ≥ 0.75; entity ≤ 20; ≤ 60 in all (§5.1.2); causal only to an earlier fact of the same batch. **No self-links**, **no cross-namespace endpoints**, **no orphan endpoint** (both facts exist at insert; at traversal both endpoints are visible). **Determinism** of the link set for a fixed batch. | `Derivation.tla` (`NoDeletedDerivationServed`) |
| `internal/outbox` | batch of 500; cursor persistence; 7-day trim honours every cursor; `Event` proto round-trip; cursor advances batched to one transaction per second per consumer (N114); `engramlint sql` rejects a builder with a statement after `OutboxRepo.Append` (A-F1) | **Gap watchlist** (model: random interleavings of `seq` allocation, commit and abort with `statement_timeout = idle_in_transaction_session_timeout = 30 s` and the outbox `INSERT` last, A-F1): every committed `seq` is delivered **exactly once** per consumer; delivery is a **strict prefix** in ascending `seq` per consumer (no cursor passes an open gap); a `seq` that commits within `2 × statement_timeout` of being skipped is never declared aborted; an aborted `seq` is declared within that bound and never blocks later ones for longer; `TestOutbox_Watch1x` reproduces the loss with a 1× horizon. | `Outbox.tla` (`NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered`; `Outbox_Watch1x.cfg`) |
| `internal/catalog` | LRU 100 k; TTL 60 s; negative 5 s; stale existing entries served indefinitely with an age gauge, negative entries ≤ 10 min (D4 as amended); `LISTEN` payload parsing; full flush on reconnect | **Invalidation model** (ops: `resolve`, `notify(ns)`, `expire`, `catalog_down`, `reconnect`): after `notify(ns)` and while the catalog is reachable, the next `resolve(ns)` returns the new `(shard, epoch)`; while unreachable, an existing entry is served **for as long as the outage lasts** (`UNAVAILABLE` only on a miss) and a negative entry is never served > 5 s reachable / > 10 min unreachable. **CAS**: `MemoryCatalog` and the Postgres catalog give identical results for a random op sequence (T3 twin). | `ShardMove.tla` (stale-cache steps) |
| `internal/authz` | `(claims, method, entry) → code` table; JWKS refresh; expired token; `kid` rotation | **Allowlist**: `ns = ["*"]` admits every namespace of the same tenant and none of another; a token never yields a `RequestScope` whose `tenant != claims.tenant`. **Scope monotonicity**: adding a scope never turns an allowed call into a denied one. | — |
| `internal/api` | `DeadlineGuard` (missing → `INVALID_ARGUMENT`, clamped); `request_id` reuse with a different hash → `ALREADY_EXISTS/OperationConflict{IDEMPOTENCY_KEY_REUSED}` (D1); opaque page tokens (HMAC, tamper → `INVALID_ARGUMENT`); field masks | **Idempotency**: replaying any unary write with the same `request_id` within 24 h returns the stored response and performs no second effect (`FakeTx` effect counter). **Pagination**: walking all pages yields each row exactly once for random inserts between pages (ids are UUIDv7 → stable order). | — |
| `internal/quota` | token bucket refill; `RESOURCE_EXHAUSTED` + `QuotaExceeded` detail (D13); day window reset at UTC midnight | **Bucket**: at most `rate` admissions per window for any arrival pattern. **Deferral**: `llm_tokens_per_day` exhaustion never fails an operation; it is `DEFERRED` and resumes exactly once at the window reset; `quota.Reserve` precedes every gateway call class (N130). | — |
| `internal/store` | SQL builders golden (every query text under `testdata/sql/`), `SET LOCAL` prelude, the write-mode fence prelude (`pg_try_advisory_xact_lock_shared(hashtextextended(ns, 0))` — failure ends the statement with `NamespaceFrozen{retry_after = 200 ms}`, N82 — then a plain `SELECT state, epoch FROM namespace_ownership`, no row lock), the per-document try-lock (two-argument form, `DocumentBusy`, N83) + `status = 'ingesting'` prelude of `CommitChunk` (N40) with no `FOR SHARE`; **no `UPDATE`/`DELETE` text on a content table outside the expunge role** (`engramlint sql`, N113) | **Predicate presence**: every builder output for a namespace-scoped table contains `namespace_id = $n` (also enforced statically, §8.3). **Fencing state machine** in `FakeTx`: a write with `state ≠ active` or the wrong epoch fails with `WrongShardOrEpoch`/`NamespaceFrozen`; a read succeeds only for `active` and `frozen/move` (N122, N125). **Version-row state machine**: a `CommitChunk` against a version whose status is not `ingesting` writes nothing, for any interleaving with `FinalizeVersion`/delete; a commit that wins the race against a tombstone writes rows that no surface returns. | `ShardMove.tla` (`SingleWriter`); `Derivation.tla` (`NoDeletedDerivationServed`) |
| `internal/extract` | schema validation of `ExtractedFact`; degenerate-fact filter; causal `target_index < i`; cache key = `sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)` | **Cache key sensitivity**: changing any one of the four inputs changes the key; **prompt version pinned**: the golden for `extract/v1` fails if the template changes without a version bump. | — |
| `internal/consolidate` | placements/merges/drop_sources schema and application; write call `write`/`retire`; proposal store keyed `(batch_key, attempt)`, write-once, never deleted; `base_version` on every `update` and `merge`; `op_key` recording; 8 facts/call; ≤ 100/round | **Bisect**: for any failure oracle, every fact lands in exactly one successful leaf batch or is stamped failed; routing calls ≤ 2n − 1 plus one write per touched observation; a fact never appears in two successful batches. **Idempotent effect**: applying a batch twice (same `batch_key`, `attempt`) yields one effect per `op_key`, *even when the second attempt's LLM answer differs* — the stored proposal wins (`TestConsolidation_PersistedProposal`, N43); keys and effects are never observed apart (`TestConsolidation_AtomicKey`). **Commit rule (N120)**: for any interleaving of two writers, a rebuild and a delete, no version commits whose base is not current or whose rendered inputs are hidden at commit (`BaseCurrentAtCommit`, `NoLostRebuild`). **Two-stage isolation (N121)**: `inputs(O, v)` ⊆ O's own sources ∪ attached facts, and a `merge` is a root rebuild with no previous text. **Lifecycle**: a discard or capacity retry writes a new `attempt`, an all-skip batch stamps its facts `done` and sets `applied`. **Effective time**: `effective_at == max(max(mentioned_at) over every fact rendered, effective_at of the previous version)` (D9), never `now()`. | `Consolidation.tla` (`ExactlyOnceEffect`, `_VolatileProposal.cfg`, `_NonAtomicKey.cfg`); `Derivation.tla` (`NoOverHiding`, `BaseCurrentAtCommit`, `NoLostRebuild`) |
| `internal/reflect` | iteration cap 10; 100 k tokens; 300 s wall; per-tool 10 s; JSON-schema validation | **Citation filter**: for any set `S` of ids returned by tools and any candidate citation set `C`, `cited ⊆ S`. **Caps** hold for any tool-result sizes (tool results are truncated to the shared ceiling, never the caps exceeded). | — |
| `internal/move` | state transitions table; `ready`, `committed` and rollback edges; `T_copy` and `W_pre` capture; the table-class list rendered from the DDL tags; `engram_seq_floor` arithmetic; column-list hash; watchdog arithmetic | **Transition legality**: a random op sequence never produces an edge outside D5's graph; a rollback action is enabled from every state before the catalog CAS (a″) and restores the source `active` (`TestMove_RollbackEveryStep`); after (a″) none is. **Reconcile correctness over `FakeTx`**: for random interleavings of inserts (any writer lifetime ≤ 60 s, including rows for ids older than the cursor and re-embeds) and source-side deletes of mutable rows, re-copying insert-only rows with `ins_seq ≥ engram_seq_floor(T_pre)` and merge-diffing the mutable class makes the target equal the source at freeze; keying the re-copy by id or `created_at`, or a floor of 0, loses or duplicates rows (the counterexamples). **One owner and one arbiter**: at most one writable owner at any state of Freeze, (b′), (a″), (c), (b″), (d); `ready` serves nothing; a restore-side `cutover → rolled_back` and the mover's `cutover → committed` never both succeed. | `ShardMove.tla` (`SingleWriter`, `NoLossNoDup`, `OneOwner`, `CatalogNamesOwnerAfterDone`, `MoveTerminatesActive`, `RollbackPossibleBeforeC`, `NoRouteToTargetBeforeC`) |
| `internal/export` | manifest schema; 1 MiB parts; zstd round-trip; delete records and `deleted_ids` in the manifest | **Delta correctness**: `apply(snapshot v_{n−1}, delta) == snapshot v_n` for random snapshot pairs (deltas are diffs of consecutive snapshots, N126), including when the base snapshot expired; a snapshot built across a delete is never promoted. | — |
| `internal/expunge` | phase planner; batch size 1,000; WAL pacing arithmetic (≤ 25 MB/s per shard, two expunges per shard, one budget); cursor-pass check; hygiene trigger (1 % of `rows_at_build` or 2 k purged elements) | **Idempotence**: any prefix of a purge batch repeated deletes nothing more and a crashed phase resumes at its marker state (`pending → materialized → purged`). **Order**: no purge before every registered cursor passed the delete's `seq`; no physical delete of a row the visibility predicate still returns. **Evidence outlives facts**: purging facts (`CHUNK_TOMBSTONES`, `REEXTRACTED_FACTS`) deletes no `observation_inputs`, `observation_version_sources` or `page_version_inputs` row (`EvidenceOutlivesFacts`). **Stubs**: `DerivedPurge` leaves a stub for every covered version, never deletes the `derived_hidden` row, and no non-stub version names a purged victim (`NoVictimTextAfterPurge`, `FailClosed`). **Pacing**: for any batch WAL sizes the issued WAL rate never exceeds 25 MB/s over any 10 s window. | `Derivation.tla` (`MaterializeComplete`, `EvidenceOutlivesFacts`, `NoVictimTextAfterPurge`); `Storage.tla` (`RebuildBeforeRepair`) |
| `internal/intent` | key format `_control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json`; the intent body (`subject, kind, up_to_version ∪ memory_ids, deleted_at, operation_id, prev_operation_id`); 35-day retention | **Ack implies intent**: no ack without a durable intent of the marker that was committed, and no intent without a committed marker (`FakeIntentStore`, `CrashBeforePut`). **Replay**: intents are applied verbatim, per subject in `prev_operation_id` chain order (`Invalidate; Restore` leaves the fact visible whichever replica served each); a replay never hides a version acknowledged after the intent's subject state (`NoUnackedEffectOnLaterAck`); replay is idempotent (`deletion_log`); the catalog floor is only ever lowered. | `Durability.tla` (`AckedDeleteSurvives`, `NoUnackedEffectOnLaterAck`, `IntentOrderLastWins`) |
| `internal/telemetry` | metric-label linter; span names | **No `namespace` label** on any registered metric (walks the registry). | — |

Every property test has the shape of `TestPack_NeverExceedsBudget` (`rapid.Check`, a generator
for the inputs, `requireSubsequence`-style helpers, the Lean theorem it mirrors named in its
comment).

### 8.3 Isolation test matrix

**Fixture** (`internal/isolation`, T3 for the store-level rows, T4 for the surface rows): two
tenants `acme` and `zeta`; namespaces `acme/A1` (shard 1), `acme/A2` (shard 2), `zeta/Z1`
(shard 1 — shares the shard with `A1`, the case that matters). Each namespace holds 200 facts
with a namespace-unique sentinel token (`A1-KESTREL`, `Z1-KESTREL`, …) in text and metadata,
one observation, one page, one snapshot. Tokens: `tok(acme, [A1])`, `tok(acme, ["*"])`,
`tok(zeta, [Z1])`, `tok(acme, [A1], scopes=memory.read)`. All surfaces are exercised through
Envoy with `AllowAllVerifier` disabled (real `StaticKeyVerifier` with a fixed key pair) so the
matrix tests the production interceptor path; the store rows use `RecordingPool` to assert the
set of shards touched.

Legend (codes per N5): **NF** = `NOT_FOUND` from the interceptor for a namespace of another
tenant (no existence oracle); **PD** = `PERMISSION_DENIED` for a same-tenant namespace outside
the token's allowlist or for a missing scope (`MISSING_SCOPE`); **FP** = `FAILED_PRECONDITION`
for a namespace in state `deleting`; **0R** = zero rows via RLS
(the request is authorised for its own namespace but names an id of another; the store returns
no row → `NOT_FOUND{RESOURCE_KIND_MEMORY}`); **NS** = "no shard touched" (`RecordingPool`
asserts the only shard in the recording is `shard(ns)`).

| Surface | Cross-tenant (`tok(zeta)` → `acme/A1`) | Cross-namespace, same tenant (`tok(acme,[A1])` → `A2`; and `A1` request naming an `A2` id) | Cross-shard (`A1` on shard 1 vs `A2` on shard 2) |
|---|---|---|---|
| gRPC unary (`GetMemory`, `ListMemories`, `Retain`, `DeleteDocument`, `Invalidate`, `GetNamespace`) | `TestIso_Unary_CrossTenant`: **NF** on every method; response carries no `ErrorInfo` beyond `NotFound{kind: NAMESPACE}` | `TestIso_Unary_CrossNamespace`: `A2` request → **PD**; `GetMemory(A1, id∈A2)` → **0R**; `ListMemories(A1)` never contains `A2-KESTREL` (asserted over 10 random tags) | `TestIso_Unary_CrossShard`: `GetMemory(A1, id∈A2)` → **0R** and **NS** (shard 2 never opened); `Retain(A1)` writes only shard 1 rows + shard 1 outbox |
| gRPC server-streaming (`Recall`, `Reflect`, `StreamSnapshot`) | `TestIso_Stream_CrossTenant`: **NF** before the first message (stream opens, first `Recv` returns the status) | `TestIso_Stream_CrossNamespace`: `Recall(A1, q="KESTREL")` returns only `A1-KESTREL` items across all five arms and at every stage (`FUSED`, `RERANKED`, `PACKED`); `Reflect(A1)` tool results never contain `A2` ids (checked in the `citation` events) | `TestIso_Stream_CrossShard`: **NS**; graph expansion from `A1` seeds touches only shard 1 `fact_links` |
| Connect / JSON (same methods over HTTP/1.1 + JSON) | `TestIso_Connect_CrossTenant`: HTTP 404 with `{"code":"not_found"}`, byte-identical `details` to the gRPC golden | same as gRPC via the shared golden files (`testdata/errors/*.json`) | same as gRPC; additionally asserts no `x-engram-shard` debug header leaks in prod mode |
| MCP tools (`recall`, `retain`, `get_memory`, `list_documents`, `get_page`) on `/mcp/{tenant_id}/{namespace_id}` | `TestIso_MCP_CrossTenant`: tool call returns an MCP error whose `data.grpc_code == NOT_FOUND`; the tool list for `/mcp/acme/A1` with `tok(zeta)` is empty | `TestIso_MCP_CrossNamespace`: `/mcp/acme/A2` with `tok(acme,[A1])` → **PD**; write tools absent from `tools/list` when the token lacks `memory.write` (**PD** if called anyway) | `TestIso_MCP_CrossShard`: **NS** |
| Export download (`CreateSnapshot`, `StreamSnapshot`, and the blob key it reads) | `TestIso_Export_CrossTenant`: **NF** | `TestIso_Export_CrossNamespace`: `CreateSnapshot(A2)` with `tok(acme,[A1])` → **PD**; a forged `snapshot_id` whose key resolves under `{1}/acme/A2/export/` → **NF** (`blob.Scoped` rejects keys outside `{1}/acme/A1/`); streamed JSONL never contains `A2-KESTREL` | `TestIso_Export_CrossShard`: the snapshot key prefix is `{shard(A1)}/…`; a key under `{2}/…` is rejected at the client (`errs.Validation`) and by the shard-1 credential at the server (the test uses a real MinIO policy) |
| Background workers / activities (`RetainDocument`, `Consolidate`, `Expunge`, `PageRefresh`) | `TestIso_Workflow_CrossTenant`: a workflow input `(ns=A1, tenant=zeta, shard=1, epoch=1)` fails its first activity with `WrongShardOrEpoch` (non-retryable); zero rows written | `TestIso_Workflow_CrossNamespace`: `Consolidate(A1)` with `A2` facts planted as the nearest semantic neighbours never cites an `A2` id (RLS in the activity transaction); the extraction cache for an `A2` chunk with the same content hash is a **miss** (second LLM call observed in `DeterministicClient`'s counter) — D11 per-namespace cache | `TestIso_Workflow_CrossShard`: input `(ns=A1, shard=2)` → `WrongShardOrEpoch` at the first activity, no rows on either shard; task queue `shard-2` never receives an `A1` workflow (Temporal `ListWorkflow` by search attribute `namespace_id`) |
| Outbox relay | `TestIso_Relay_TenantMix`: events of `Z1` and `A1` (same shard) are delivered to the `index` consumer with their own `namespace_id`; the Kafka sink (when on) keys by `namespace_id`; no consumer callback ever receives a payload whose `namespace_id ≠ event.namespace_id` (decoded and compared) | `TestIso_Relay_Interleave`: `Z1` rows interleave `A1` rows in `seq`; every consumer sees events in `seq` order and delivery stays a strict prefix per consumer | `TestIso_Relay_CrossShard`: the relay built from `ShardHandle(1)` never opens a connection to shard 2 (`RecordingPool`); shard 2's outbox `seq` space is disjoint by construction and its rows never appear in shard 1 cursors |
| Move (copy and reconcile) | `TestIso_Move_TenantMix`: during BulkCopy and Reconcile of `A1`, `Z1` rows on the source are neither copied nor diffed (target `Z1` count stays 0; every copy and reconcile statement carries `namespace_id = A1`) | `TestIso_Move_NamespaceOnly`: the copy and the reconcile run as `engram_move` under RLS with `engram.namespace_id = A1`; a planted `A2` row in the stream is rejected on the target and counted (`engram_move_rejected_rows_total`) | `TestIso_Move_Epoch`: after (c) a source write at epoch `e` → `WrongShardOrEpoch`; a `ready` target serves no read or write (`NamespaceNotReady`); a rollback restores the permanent `moved_out` fence value where one existed |
| Metrics / logs | `TestIso_Metrics_Labels`: scrape `/metrics`; every series has `shard`; **no series has `tenant` or `namespace`** (N62 — per-tenant metering is served from `token_usage` by `engramctl report` and the optional OpenTelemetry delta export; also the static linter) | `TestIso_Logs_NoContent`: 10 k requests with sentinel content; the JSON log stream contains `namespace_id` fields but never any sentinel string, query text or fact text at `info` | `TestIso_Traces`: span attributes carry `engram.shard = shard(ns)` only; no span of a request for `A1` has `engram.shard = 2` |
| Ownership state machine (N64; review F-23) | `TestIso_Ownership_Transitions` (T3, before any move code exists): every transition of the §3.3.1 states × roles × statements table — `incoming → ready → active`, `active → frozen{move|delete|restore}`, `frozen → active` (`thaw_move`, re-enable), `frozen → moved_out`, `ready → incoming` (`unready_target`), `incoming → moved_out` (`return_abort`), `reconcile_out`, the epoch bump, and every *forbidden* edge — is executed against the real DDL as the role the table names (`engram_app`, `engram_move`, `engram_admin`); the `CHECK (state <> 'frozen' OR freeze_reason IS NOT NULL)` and the role grants are what fail the forbidden edges; the read fence accepts `active` and `frozen/move` only (N122, N125) and the write fence rejects every other state | cross-namespace: a transition for `A1` never touches `A2`'s row (`RecordingPool` statement capture) | cross-shard: the catalog's `restoring` state and the shard's `frozen/restore` row are checked together |
| Temporal histories and payloads | `TestIso_Temporal_PayloadsEncrypted` (N59): retain `A1` and `Z1` with sentinels; read every workflow history of task queue `shard-1` through the SDK client and `temporal workflow show`; no sentinel appears in any payload (results > 4 KiB are blob keys, the rest is AES-GCM through the `DataConverter` codec with `encoding: binary/encrypted` metadata); a worker built without the shard's codec key fails to decode and never runs the activity | `TestIso_Temporal_KeysOnly`: `CommitChunkInput` carries only blob keys; the blob keys are under `{1}/acme/A1/` (`blob.Scoped` rejects any other prefix) | `TestIso_Temporal_QueueScope`: a `shard-1` worker never reads a `{2}/…` key |
| Delete markers and export | `TestIso_Tombstones_NamespaceScoped` and `TestDelete_ExpiresSnapshots` (N59, N126): a tombstone, `fact_hidden` or `derived_hidden` row for `A1/d` hides nothing in `A2` even with the same `document_id` (marker sets are loaded per namespace); create snapshot `v1` of `A1`, delete one document, then `StreamSnapshot(v1)` → `FAILED_PRECONDITION/SnapshotExpired{reason: DOCUMENT_DELETED}` and `ListSnapshots` shows `state = expired` (the marker transaction expires `building` and `ready` rows alike); `CreateSnapshot` produces `v2` without the document | cross-namespace: deleting an `A2` document expires no `A1` snapshot | cross-shard: **NS** |

Two rows apply to every surface at once: `TestIso_Deleting_AllSurfaces` — a namespace in
catalog state `deleting` (shard state `frozen/delete`) returns **FP** (`PreconditionFailed{NAMESPACE_DELETING}`) on every method of every surface (N5, N122), and the
`FakeActivities` for its Expunge workflow still run (the only writer allowed); and
`TestIso_Connect_CodeParity` — for every cell above, the Connect JSON `code` string equals
the gRPC code (`not_found`, `permission_denied`, `failed_precondition`).

**RLS canary** (`TestRLSCanary`, T3, `internal/store`). Every store query is registered at
init in `store.Queries` (name → builder + example arguments); the canary iterates the
registry, opens a transaction with `SET LOCAL engram.namespace_id` = a namespace that owns no
rows (`00000000-…-0000` is rejected by the policy cast, so a real but empty namespace is
used), runs the query, and asserts zero rows for reads and zero affected rows for writes;
it then re-runs with the owning namespace and asserts > 0, proving the query would have
returned rows. A query missing from the registry fails a companion test that greps
`internal/store/**/*.go` for `tx.Query(` / `tx.Exec(` call sites and compares the count.

**Static RLS check** (`TestEveryTableHasRLS`, T3 and `make lint` via `engramctl migrate --check-rls --dsn`): `pg_class`, `pg_attribute`, `pg_policies`: every table with a `namespace_id` column has RLS enabled and forced and carries the policy `ns_isolation` with the expected qualifier; the expected result is zero rows. Tables without `namespace_id` must be on the explicit allowlist `outbox_cursors`, `shard_meta`, `goose_db_version`; extra policies are permitted only for `engram_relay` (`SELECT` on `outbox`, N4), `engram_move` (N2) and `engram_admin`.

The same test asserts `rolbypassrls = false` for `engram_app` and that `engram_app` is not a
member of any role with `BYPASSRLS`. Belt and braces: `make lint` runs `engramlint sql`, which
parses every builder's output (`testdata/sql/*.sql`) with `pg_query_go` and fails if a
namespace-scoped table is referenced without an equality predicate on `namespace_id`
(rationale: RLS makes the omission safe, the explicit predicate keeps partition pruning; both
must hold).

### 8.4 Delete, storage, durability, move and fault tests

This section holds every regression test of the D22 and D23 redesigns. Subsections 8.4.1 to 8.4.4 are
the tests of the four mechanisms (markers and visibility, durability, moves, storage and plans),
8.4.5 is the fault matrix for everything else, 8.4.6 pairs each model-checked property with
its Go twin, and 8.4.7 maps every round-4 blocker and major finding and A-1 to A-4 to the scenario that fails without its fix. Tier T5 rows run against the T4 stack; the others are T1 to T3. Every row also
asserts the §7 safety invariants through the SQL checks of 8.4.3.

#### 8.4.1 Tombstones, visibility, Restore and Expunge

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestDelete_O1` (N115, N122) | documents of 1 k, 10 k, 100 k and 1 M facts, deleted under retain load | the ack path (one marker transaction: document lock, `documents.state = 'deleting'` with the content-bearing columns cleared, one `document_tombstones` row, `deletion_log` with `prev_operation_id`, one outbox event; then the intent `put`; then the ack) is O(1): ack p95 ≤ 100 ms at every size, slope of ack time against size ≈ 0; no content row is touched; `expected_version` is compared inside the marker transaction under the document lock (C-17) | T3 + T5 |
| `TestDelete_ReuseDocumentID` (N133c) | delete `d1` (versions 1 and 2, one still `ingesting`), then at once retain `d1` again with the same text (REPLACE), then an `APPEND`; run the Expunge; delete `d1` a second time while the first tombstone still exists; then a retain after the purge removed the covered `document_versions` rows | the tombstone has `up_to_version = 2` and the retain is **accepted** (no `OperationConflict`, no wait for the expunge) as version 3 (`greatest(max version, max up_to_version) + 1`, also after the covered rows are gone); `engram_visible_facts` returns only version 3 facts at every instant, the v1/v2 facts never, on every arm (`TestVisibility_AllSurfaces` table-driven over the pair); the same text is a **new chunk row** (`life_start` 3), so the purge of `<= 2` deletes no row the new version needs and the `documents` row survives; the `APPEND` base never reaches below `life_start`; `CommitChunk` of an in-flight v2 `aborted` although the document is `active` again; the second delete inserts a second tombstone `(d1, 3)` (the key includes `up_to_version`); `observation_inputs` of version 3 facts are not materialised hidden; the external-index consumer deletes `document_version <= up_to_version` only | T3 + T5 |
| `TestContent_InsertOnly` (N113, N137) | the whole T3 suite and the retain, consolidation and expunge workflows run with a test-only trigger that raises on `UPDATE` of every insert-only table and on `DELETE` outside the `engram_admin` role (the Expunge purge); the class tags are read from `COMMENT ON TABLE` | suite green; `engramlint sql` rejects any builder with `UPDATE` on those tables; every table has exactly one class tag, every insert-only table has `ins_seq` and the `(namespace_id, ins_seq)` index, and the move, the DDL header and N113 render the same list (C-23); no table has `retired_at`, `invalidated_at`, `live`, `stale_*` on a content row, `superseded_at`, `tags` or a generated column (`attgenerated = ''`), `fillfactor = 100`; evidence tables have no foreign key to `facts` | T3 + S |
| `TestVisibility_AllSurfaces` (N116, N135, N136) | table-driven: document delete, `REPLACE` then the chunk purge then a later `DeleteDocument` (C-1), re-extraction, and `Invalidate`, each followed immediately by every read surface: all five Recall arms at `FUSED`/`RERANKED`/`PACKED`, Reflect tools (`search_memories`, `search_observations`, `search_pages`, `get_page`, `expand_fact`), `GetMemory`, `GetDocument`, `ListDocuments(include_deleting)`, `GetDocumentVersion`, `ListMemories`, `GetPage`, `SearchPages`, `StreamSnapshot`, MCP tools, and the async-index join with the `index` consumer paused | from the ack, no fact, chunk, observation version, page version, entity alias or export part derived from the subject is returned by any surface (sentinel absence), including observations built from text that `REPLACE` had already retired; `GetMemory` and `GetPage` answer `NOT_FOUND` / `PreconditionFailed{PAGE_HIDDEN}`; the document surfaces return the tombstone view only (`document_id, state, deleted_at, up_to_version, operation_id`) and `GetDocumentVersion` of a covered version is `NOT_FOUND{DOCUMENT_DELETED}`; marker sets are read once per request; above 16 k `doc_tomb` or `chunk_tomb` entries the arm switches to the SQL anti-join and returns the identical set | T3 |
| `TestVisibility_SegmentHiding` (N117, N135) | observation `o`: v1 create (root), v2 update, v3 root rebuild, v4 update, with inputs `f1`, `f2`, `f3`, `f4`; delete `f2`'s document; a second run where a version row is missing | exactly v2 is hidden (segment of v2 = inputs of v1..v2 names `f2`; v1 = {f1}; v3 and v4 have `root_version` 3); the current v4 stays visible (`NoOverHiding`); a version that commits **after** the marker and names `f2` in its segment is hidden by the read-time predicate (race test with the writer paused between re-verification and commit); a missing version row means hidden (`FailClosed`); a version with no visible source is not served and a rebuild that finds none retires the observation without a gateway call; a configuration without the derivation lock lets Materialize miss it (the counterexample, 8.4.6) | T3 |
| `TestVisibility_Pages` (N117) | page `p` v1 root (fact and observation-version inputs), v2 delta; delete a document behind a fact input, then one behind an observation input | the page version is hidden through the fact input or through a hidden `(O, w)` input (two `EXISTS`, no walk); a hidden current page returns `PreconditionFailed{PAGE_HIDDEN}` until `PageRefresh` lands a root rebuild (`page_full/v1`, no previous text); pages never appear as an input of an observation | T3 |
| `TestInvalidate_RestoreExact` (N115, N135) | random sequences of `Invalidate(f)`, `Restore(f)`, document delete, `REPLACE` and re-extraction over a fixture (rapid) | the hidden set after `Invalidate; Restore` equals the set before (exactness): `Restore` deletes the `fact_hidden(invalidate)` row and the cause-tagged `derived_hidden` rows `('invalidation', f)` and nothing else, so it cannot resurrect a stale extraction next to its re-extracted twin (`cause = 'reextract'` rows stay); a restored fact of a tombstoned document stays hidden; `curation_log` re-applies a curation to the re-extracted twin of the same `content_hash`; a double `Invalidate` is a no-op success | T1 + T3 |
| `TestRetain_ReplaceTombstones`, `TestReextract_Rebuilds` (N115, N58, N135) | `REPLACE` that drops a chunk, flaps it back, and a prompt bump (`reextract`) over a document with observations and a page | `FinalizeVersion` inserts `chunk_tombstones(reason = 'replace')` for chunks missing from the new membership (one statement), a flap is `DELETE FROM chunk_tombstones`; re-extraction inserts `fact_hidden(cause = 'reextract')` for the old-key facts, which only the fact and chunk arms read, so **observations and pages derived from them stay visible at every `as_of`**; the same transaction sets `observations.stale_write`, the new-key facts are consolidated through ordinary rounds, and the old-key facts, their vectors and markers are purged after 1 h (`REEXTRACTED_FACTS`) while the evidence rows stay; marker sets stay bounded by expunge lag; Recall never returns a v1 and a v2 fact for the same chunk | T3 |
| `TestExpunge_Stages` (N119, N136) | delete a 100 k-fact document with observations, pages, a page markdown blob, a Reflect transcript, an export and an `ids_elided` consumer lagging | (1) Materialize re-reads the markers under the exclusive derivation lock per batch, inserts cause-tagged `derived_hidden` rows with `from_version = min(version)` of the victim inputs, marks observations and pages `stale_delete`, nudges root rebuilds and `PageRefresh`, sets `expunge_state = 'materialized'` and starts a system `ExportSnapshot` (debounced 10 min); (2) Purge deletes in batches of 1,000 paced by WAL (facts, vectors, links, mentions, chunks, versions, ledger rows, owner-keyed blobs, then blob tombstones) **only after every registered index and Kafka cursor passed the delete's `seq`**; (2b) `DerivedPurge` reduces every covered observation and page version to a content-free stub; the transcripts written before `deleted_at` are deleted; (3) the touched HNSW is rebuilt by the index runner at 1 % purged, then `VACUUM (INDEX_CLEANUP ON)`; (4) `purged`, tombstone deleted after 24 h, operation `SUCCEEDED`; afterwards the shard holds **zero** victim text, vectors, BM25 hits, evidence-bearing non-stub versions, markdown and transcript objects; `derived_hidden` rows for document causes are never deleted; the `documents` row is deleted only when no open tombstone with a higher `up_to_version` and no `document_versions` row remains (C-12: a second delete of a re-used id during the first expunge completes) | T3 + T2 |
| `TestExpunge_WALBudget` (N119, P-6) | purge a 100 k-fact document and delete a 1 M-fact namespace with two expunges active on one shard, a lagging archiver | the per-batch `pg_current_wal_insert_lsn` delta keeps the shard at ≤ 25 MB/s over any 10 s window and at most two expunges run (shard-wide advisory slot); purge yields while `pg_wal` > 20 GB; the 100 k document finishes in 2.5 to 5.5 min, the 1 M namespace inside the 24 h SLA; a differential backup follows each delete; the RTO term from the WAL since the last backup stays ≤ 60 min | T3 + T5 |
| `TestExpunge_SLAs` (N119) | the same fixture at 1/100 scale on the T4 stack, then the projected figures for 1 M facts from measured rates and the 25 MB/s pacing | materialize ≤ 15 min, purge ≤ 24 h, touched index rebuilt ≤ 48 h (projected at the measured rates, which M1.10 records); other SLOs of the namespace may degrade meanwhile; the `ExpungeMaterializeSlow` alert fires when a marker stays `pending` > 15 min | T4 + T5 |
| `TestExpunge_DegradedMode` (N119) | a namespace with `pending` markers, 150 candidates × ≤ 60 input rows per observation arm | observation and page arms add the per-candidate `observation_inputs` lookup (index-only plan; ≤ 20 ms per arm); `Consolidate` runs only root rebuilds; the rerank-skip SLO is suspended for the namespace (metric labelled, not alerted); after `materialized` the lookup is skipped (only `pending` tombstones are passed in `$doc_tomb`) and latency returns to the baseline within one request | T3 + T5 |
| `TestExpunge_DerivationLock` (N120) | a writer (`ApplyBatch` stage 2 or `PageRefresh`) whose re-verification predates the marker holds the shared lock across its commit; Materialize starts meanwhile | Materialize waits (exclusive lock, one 35 s attempt), then sees the new version and writes its `derived_hidden` row; a legal 30 s writer never starves it; writers refused by a pending Materialize retry (`pg_try_advisory_xact_lock_shared`); markers themselves take no lock; the key spaces of fence (`…, 0`), derivation (`…, 1`) and document locks (two-argument) never collide (`pg_locks` check) | T3 |
| `TestPageRefresh_DeleteMidCall`, `TestDerivation_CommitRule` (N120, C-2, C-3) | a page refresh whose LLM call spans a delete and its Materialize; a stored stage-2 `update` against `O@v2` after an intervening root rebuild (v3); two writers racing a rebuild | the commit re-verifies every rendered input in a fresh statement under the shared lock (fact inputs against all markers of both causes, observation-version inputs against all open tombstones) and checks its base: the refresh is refused and `page_full/v1` re-derives from current evidence; the stale update (base v2, current v3) is discarded whole and re-routed instead of becoming v4 in segment 3; no root rebuild is ever superseded by a version whose base predates it | T3 |
| `TestExpunge_FencedAndPaused` | `start_move` while an expunge is mid-purge; epoch bump during Materialize | every activity is fenced `active` at the epoch and skipped while a move is open; the expunge resumes after the move is `done` or rolled back; large-table purges never run between Plan and done | T3 + T5 |
| `TestDelete_NamespaceAndTenant` (N122) | `DeleteNamespace`, `DeleteTenant` under load | `freeze_delete` precedes the ack; `frozen/delete` rejects reads and writes (`PreconditionFailed{NAMESPACE_DELETING}`, one code everywhere); `DeleteTenant` acks after the catalog `deleting` row and a tenant-level intent, fences namespaces asynchronously with `TENANT_DELETING` as the read barrier, and `TenantDelete` writes one intent per namespace as it fences each; purge is `DROP INDEX` plus paced batched `DELETE` plus the blob prefix, with no HNSW repair; delete-class RPCs and `Restore` have a 40 s deadline cap; a tenant-scoped admin `GetOperation` answers during the delete, derived from the `tenants` row (`deleting` → `RUNNING`, `deleted` → `SUCCEEDED`, N133d) | T3 + T5 |
| `TestExport_ExpiresOnMarker`, `TestExport_HiddenOverlay` (N126) | `BeginSnapshot` (`building`), `RecordSnapshot` race, delete, `StreamSnapshot`; then `Invalidate` and `Restore` with a `ready` snapshot | the marker transaction of a delete expires `building` and `ready` rows alike; `RecordSnapshot` refuses to promote an expired row and re-checks `deleted_at > snapshot_started_at`; `WriteFiles` reads short `READ COMMITTED` ranges bounded by the `ins_seq` watermark (no snapshot older than one range, so index builds and the vacuum horizon are never held); a delta is emitted with delete records and `deleted_ids` even when the base expired; an `Invalidate` never expires a snapshot: `GetSnapshotManifest` carries the live `hidden_overlay` (current `fact_hidden(invalidate)` ids and `derived_hidden` rows), `engram-sync` applies it before serving, and a `Restore` removes it; after a delete a system snapshot restores a servable copy for read-only clients | T3 + T4 |
| `TestEntities_DeleteAndAsOfSafe` (N118) | an alias merge whose evidence document is deleted; `as_of` before the merge | under `as_of`, `EntityRef` carries only `mention`, `canonical_name` and alias merges are suppressed and graph hops use `entity_mentions.mentioned_at ≤ T`; the expunge deletes the victim's aliases and recomputes `canonical_name` from the remaining mentions; both cases join the 8.5 leak canary | T3 |
| `TestDocument_TombstoneView`, `TestOperation_DeleteKinds` (N136) | `GetDocument`, `ListDocuments(include_deleting)` and `GetDocumentVersion` after a delete; `GetOperation`, `WaitOperation` and `CancelOperation` on `DELETE_*`, retain, export and refresh; a move `Restart` with a singleton-backed operation | the summary, context, metadata and tags are cleared at the ack and the summary blob is tombstoned; a revival writes fresh values for the new life; `DELETE_*` are non-cancellable (`PreconditionFailed{OPERATION_NOT_CANCELLABLE}`), `WaitOperation` polls the tombstone and progress comes from `expunge_progress`; the move's `Restart` and reconcile loop skip expunge and consolidate | T3 |

#### 8.4.2 Durability: delete intents, restore and failover

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestIntent_AckImpliesIntent` (N122) | kill the API process (a) after the marker transaction and before the intent `put`, (b) after the `put` and before the ack; blob store down for (c) | (a) the marker is committed and hides the content, the client saw an error and **a restore may lose it**; the retry finds the subject `deleting`, returns the existing operation and puts the marker's own intent before acking; (b) the retry finds the intent object and acks; (c) the marker is committed, the call returns `UNAVAILABLE` without an ack and the retry completes it; in every case an ack implies the intent exists and no intent exists for a marker that did not commit | T3 + T5 |
| `TestIntent_DuplicateAttempt` (N122) | two concurrent `DeleteDocument` calls for one document, and a retry after `CrashBeforePut` | exactly one marker, one `deletion_log` row and **one intent object** under the marker's own name and content (put-if-absent); a duplicate never writes a second object and never acks before the intent exists | T3 |
| `TestRestore_ReplaysIntents`, `TestRestore_ChainOrder` (N122, N123) | base backup; then document deletes, an `Invalidate` followed by `Restore` of the same fact served by two API replicas with clocks 2 s apart, a namespace delete and a retain; restore to a point 5 min before the last delete | the restored shard comes up `frozen/restore` with restricted `listen_addresses`; `engramctl restore replay` lists every hosted namespace's intents with `deleted_at ≥ replay_floor − 10 min` and applies them **verbatim** (the recorded `up_to_version` or `memory_ids`) through the admin variant of the marker transaction, per subject in `prev_operation_id` chain order (the fact is visible again after `Restore` whatever the clocks say; a broken chain starts at its oldest present member); only then `restore_done`; the read fence rejects `frozen/delete` and `frozen/restore` throughout; every acknowledged delete is invisible after the replay | T3 + T5 |
| `TestRestore_NoUnackedEffect`, `TestRestore_FloorInCatalog` (N134, C-4, C-5) | (a) an `Invalidate` committed but never acknowledged, followed by an acknowledged `Restore` and a retain of the same document, then restore; (b) two restores in a row, the second starting before the first replay completes and a third after the replay's own commits; (c) a PITR and a stale promotion | (a) no replay applies an effect that was not acknowledged to something acknowledged later; a replayed `DeleteDocument` hides only its original `up_to_version`, never a version retained after it (the intent-before-commit configuration, which writes orphan intents, fails this); (b) the floor is `catalog.shards.replay_floor`, only lowered by `min()` and never raised, so every acknowledged delete survives all three restores; (c) neither a PITR nor a stale promotion can move the floor; `shard_meta.replay_floor` does not exist | T3 + T5 |
| `TestRestore_DeletingNamespaces`, `TestRestore_PITRBeforeMoveIn` (N123, C-13, C-22, P-12) | restore while a namespace is `deleting` and another is moved in after `T`; a tenant delete before the restore | namespaces whose catalog state is `deleting` are not set `restoring` and the epoch bump skips them; their intents replay through the `restore_delete` edge (`frozen/restore → frozen/delete`) and restart the purge; a tenant delete replays from its per-namespace intents; the moved-in namespace is recovered by an ordinary move from a scratch instance that ends with an intent replay from `cutover_at − margin` before `ready`; `blob gc --reconcile` skips the prefixes the catalog places on the shard until the recovery move is `done`, and the moved-out source prefix still exists 27 days after `CleanupMove` deleted its rows | T5 |
| `TestFailover_ReplaysIntents` (N122, N123) | `ha` profile: an asynchronous standby lagging 5 s; 20 retains in flight; a delete acknowledged 1 s before the kill; both variants (old primary stopped, old primary paused and returning) | the promoted shard follows the restore path (open moves reconciled against the catalog first, epoch = catalog epoch + 1 written to the catalog first, `frozen/restore`, intent replay); the acknowledged delete is invisible (RPO 0); retains lose ≤ 60 s; no acked retain ends `FAILED` (in-flight workflows restart at e + 1 from `operations`); the returning old primary fails `WrongShardOrEpoch` on its first write | T5 (nightly, 8 min) |
| `TestDurability_NoSyncWait` (N122) | pause or kill the standby; run a 32-writer load with deletes | every role has `synchronous_commit = local` (`pg_db_role_setting` check in `engramctl config lint`); commits never hang; the relay's 60 s gap horizon holds (`TestOutbox_Watch1x` unchanged); no `UNAVAILABLE` path exists in the delete code | T3 + T5 |
| `TestIntent_Retention` | 36-day-old intents | objects are kept 35 days (more than the 28-day backup window) and trimmed by `engramctl intent trim`; replay finds every intent inside the window | T3 |
| `TestRestore_OpenMoves`, `TestMove_FailoverBetweenCAndD` (N123, N125, C-10) | restore or fail over a source shard (a) before the catalog CAS (a″), (b) after (a″) and before (c), (c) between (c) and (d) | the reconcile CASes `cutover → rolled_back` in (a) and the move is undone from the target and the row thawed; in (b) and (c) it reads `committed` and completes (c), (b″) and (d) itself (`reconcile_out`); exactly one of the two sides wins and the loser stops; the epoch bump skips a namespace whose move is `committed`, so no state leaves a `moved_out` source against an emptied target and the catalog is never bumped past a pending (d) | T5 |

#### 8.4.3 Move: dirty copy, reconcile, `ready`, rollback at every step

The mover is killed after the first committed write of each step, the load generator keeps
running against `A1` (retain 20/s, delete 2/s, consolidation), and `engramctl move resume` is
issued after 10 s. The invariants are the rewritten `ShardMove.tla` properties as SQL over
source, target and catalog, sampled every 500 ms.

| Killed in | Client-visible during the fault | After `resume` or rollback | Invariant checks (every sample) |
|---|---|---|---|
| Plan (target `incoming`, `start_move` set `move_epoch`, source `system_identifier` and `timeline_id` recorded) | none; the expunge and the schedulers are paused for `A1` | resumes into BulkCopy, or `abort_move` deletes the `incoming` row | **SingleWriter**: `count(ownership WHERE ns = A1 AND state = 'active') = 1` across shards; catalog epoch unchanged |
| BulkCopy (mid range of `facts`) | none | resumes at `(table, last_key)`; ranges are `READ COMMITTED` ≤ 100 k rows loaded through a `TEMP` table under RLS (N91); the target has no HNSW for `A1`; or `abort_move`, delete target rows, drop the target's indexes by their `engram_hnsw_ddl` names and delete the blobs | target has no `active` row; source counts unchanged; blob prefix absent or a subset with one key per copied row |
| Pre-freeze verification (namespace still **active**: counts, `bit_xor` hashes, `VerifyFK`, blob existence over `ins_seq < engram_seq_floor(now)`, one catch-up copy) | none; writers run | resumes (idempotent); a mismatch → `Rollback` | `A1` stays `active` on the source throughout; no verification statement runs after the freeze |
| Freeze (exclusive fence, one 35 s attempt) | writes `NamespaceFrozen`; reads continue | resume reconciles, or `thaw_move` then the BulkCopy rollback | at no sample do two shards hold `active`; **NoLoss**: every retain acked before the freeze is on whichever shard ends `active` |
| Reconcile (mid insert-only re-copy by `ins_seq`, mid mutable merge-diff, mid consumer-cursor wait) | as above | resumes (idempotent: `ON CONFLICT` upserts and deletes of target rows absent on the source); a residual mismatch, a missing blob or a cursor wait > 120 s → `Rollback` | **NoLossNoDup**: `count(*)` and (≤ 2 M rows) `bit_xor(hashtextextended(pk::text, 0))` equal per insert-only table restricted to the re-copied range; `(pk, md5(row minus updated_at))` equal per mutable table; the excluded class matches by `count ≤` |
| (b′) target `incoming → ready` | callers on the target get retryable `NamespaceNotReady`; the source still serves reads | `unready_target` then `thaw_move` (rollback), or continue to (a″) | **NoRouteToTargetBeforeC**: no read or write was served by the target |
| (a″) catalog CAS `namespace_moves.state: cutover → committed` (**point of no return**) | callers on the target still get `NamespaceNotReady`; the source still serves reads | resume reads `committed` and completes (c), (b″), (d); no rollback after (a″) | exactly one of the mover's CAS and a restore's `cutover → rolled_back` succeeds; the catalog names the owner after `done` (`CatalogNamesOwnerAfterDone`) |
| (c) source `frozen/move → moved_out` with the target hint (executed only after the mover read `committed` and re-checked its timeline) | no owner exists until (b″); reads hit `WrongShardOrEpoch{MOVED_OUT}` and re-resolve in a bounded loop (≤ 5 s) | resume completes (b″) `ready → active` (sub-second, retried forever), then (d) | at no sample two writable owners; the gap between (c) and (b″) ≤ 1 s unless the mover is dead, which counts against the availability SLI and raises `CutoverInProgress` |
| (d) catalog flip (retried forever), Restart | brief `WrongShardOrEpoch` on stale caches, one re-resolve | `Restart` starts the recorded `operation_id`s on `shard-{target}` at e + 1 | every operation acked before the freeze reaches `SUCCEEDED` on the target; consolidation `op_key` unique; **ReadsFresh**: no read for `A1` was served by a shard other than `shard(A1)` at the catalog's epoch, except `frozen/move` source reads |
| Cleanup (after 24 h) | none | resume finishes `DROP INDEX`, batched `DELETE`, blob prefix; the `moved_out` row stays | target counts unchanged; `Z1` on the source untouched |

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestMove_DirtyCopyReconcile`, `TestMove_ActiveBacklog` (N124, N137, C-8, P-1) | writers (retain, delete, consolidation) run through BulkCopy and the verification; a writer that drew its ids before `T_copy` and commits 45 s later; stamps, `APPEND` re-embeds and new versions of observations created before the cursor; source-side deletes of `idempotency_keys` and mutable rows; a namespace with a consolidation backlog | insert-only rows with `ins_seq ≥ engram_seq_floor(T_pre)` are re-copied (the floor is 10 min below the sample, longer than the 60 s writer lifetime), so rows inserted for **old ids** are caught and the counts match on an **active** namespace; with the floor forced to 0 (`--recopy-floor=0`) a straddling row is lost and with an id-keyed re-copy (`--id-keyed-recopy`) the move rolls back every time (the counterexamples); mutable tables: target rows absent on the source are deleted; blobs: one key per copied row, a missing key refuses cutover; the freeze holds only the rows since `T_pre − 10 min` plus the mutable rows, no whole-namespace scan and no 100 k `HEAD`s, and its duration is recorded for the M1.5 formula | T3 |
| `TestMove_ExpungeStaysPaused` (N124) | start a move on a namespace with `pending` markers | no purge runs from Plan to done; after done the expunge resumes on the target at the new epoch; a marker created during the move is copied by the mutable merge-diff | T3 |
| `TestMove_RollbackEveryStep` (N125) | fault at every step before the catalog CAS (a″); `thaw_move` with the target unreachable | rollback leaves the source `active` and the target rows, indexes (dropped by their deterministic names) and blobs gone within 60 s; after (b′) `unready_target` runs first (at next contact when the target is unreachable: a `ready` row accepts nothing); onto a shard that had a `moved_out` row, `return_abort` restores the permanent fence value; `outbox_cursors` is never touched by a move | T3 + T5 |
| `TestMove_ReadyState`, `TestMove_ClassLint` (N125, N137) | state-machine test against the real DDL; `engramlint sql` over the DDL | `incoming → ready → active` edges (`ready_target`, `activate_target`, `unready_target`) run as the role the transition table names; `ready` accepts nothing and surfaces `NamespaceNotReady`; schedulers keep `state = 'active'` as their predicate; every table has one class tag and the move's copy, merge-diff and exclusion lists are generated from them | T3 + S |
| `TestMove_ZombieFenced` (N123) | failover during BulkCopy and during Freeze | the mover and relay compare their session's `(system_identifier, timeline_id)` with the catalog at Freeze, at (c) and every 10 s; a source session on a stale timeline fails `MoveFenced` and can be neither frozen nor cut over | T3 + T5 |
| `TestBlob_OwnerKeyed` (N104, C-9) | `APPEND` chains, `REPLACE` and a delete of one document with a body identical to another document's; a move; the purge of the first document | every blob has exactly one owner row (`ledger/{ledger_id}`, `ver/{document_id}/v{n}`) and dies with it, so the other document's identical body and every live `body_key` survive the purge, `APPEND` never reads a deleted object, and the move's blob check (one key per copied row) passes; `xcache`, `ecache` and `staging` stay content-addressed with the 24 h grace and a loss is a recompute | T3 |
| `TestMove_PlanRefusals` (P-8, N22) | schema-version mismatch; a column added on one side; a generated column | `Plan` refuses unless `shard_meta.schema_version` and the per-table column-list hash (`attgenerated = ''`, `NOT attisdropped`, ordered by `attname`) match; `migrate --shard` refuses during a non-terminal move | T3 |
| `TestMove_VerifyFitsFreeze` (N124) | 100 k-fact and 1 M-fact namespaces | freeze < 30 s at 100 k facts, measured and recorded (not gated) at 1 M; the watchdog is `max(120 s, 60 s + 1 s per 10 k facts)`, ≤ 15 min, armed until (c); a seeded one-row mutation is detected by the pre-freeze or the freeze check | T3 + T5 |
| `TestMove_CutoverOrder`, `TestMove_DrainRestartReconcile`, `TestMove_MoveBack` (N93, N97) | an API client with a muted `LISTEN`; a workflow started 0.5 s before freeze; move `A1` 1 → 2 → 1 | reads are served by `shard(A1)` at the catalog epoch or fail `WrongShardOrEpoch` (the catalog-first order fails this); `Drain` reads `operations` (never `ListWorkflow`) and `Restart` uses `TERMINATE_IF_RUNNING` and rejects `AlreadyStarted` unless queue and memo epoch match; `moved_out` is never deleted by cleanup and `moved_out → incoming` needs a strictly greater epoch and no data rows | T3 + T5 |
| `TestMove_RetryAfterCutover` | a retain acked on the source, the namespace moved, the client retries the same idempotency key on the target | the target returns the original operation (idempotency keys are a mutable table, copied by the merge-diff) | T3 + T5 |

#### 8.4.4 Storage and plans (the Postgres side)

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestPlans_AsEngramApp` (N131, P-5) | every N19 `EXPLAIN` assertion executed as `engram_app` with the scope GUCs set, on the real ParadeDB image (never as owner or superuser) | the BM25 arm keeps Top-K pushdown under RLS; the entity trigram lookup goes through the `SECURITY DEFINER` wrapper that re-checks `namespace_id` and uses `entities_trgm_idx` (≤ 10 ms; the unwrapped query seq-scans 85 k entities in ≈ 308 ms); no tag predicate appears under RLS (tags resolve once to `$allowed_docs`); the check is also the M0 gate | T3 |
| `TestHNSW_PerNamespace` (N112) | eight namespaces of 1 k to 100 k vectors sharing one partition, shared-topic vectors | below 2,000 live vectors the plan is an exact scan (no HNSW node) returning `min(cap, n_visible)`; at 2,000 the index runner builds the partial index (CIC on the partition) and the plan uses it with `plan_cache_mode = force_custom_plan`; dropped below 1,000; visited tuples per query ≈ `ef_search` (150 MID, 400 HIGH) within 1.3×, independent of the other namespaces; forcing a shared partition HNSW (`faultinject`) reproduces the 1/selectivity blow-up (26 k buffers at 3 % selectivity) | T3 |
| `TestRecall_FilteredArm` (N138, A-1, P-3) | a 50 k-fact namespace and a 2 k one with tags at 100/20/5/2/1 %, `metadata_filters`, `fact_type` filters and `as_of` at deciles; all arms (semantic, chunks, observations) on the real image as `engram_app` | every arm returns `min(cap, n_visible)` or sets `RecallStats.partial`; below θ eligible rows (5 k, fixed by the sweep) the plan is the exact path driven from `facts_doc_idx` or `facts_mentioned_idx` joined by primary key (a 1 % tag filter on 50 k facts reads ≈ 500 rows, not a 50 k-tuple walk or a 1.3 GB partition scan); above θ it is the HNSW iterative scan with `hnsw.max_scan_tuples = min(4 × ef_search / s, 100 k)`; `enable_seqscan = off` and `max_parallel_workers_per_gather = 0` make a partition scan unchoosable; p95 per arm and the page touches are recorded per selectivity (8.6.1) | T3 |
| `TestVectors_ModelGeneration` (N111) | `ReembedNamespace`; a `models.embed` config flip | arms read `embedding_model = <current>`; the workflow inserts new vectors, builds the new index, flips the namespace's current model, then expunges the old; a config change alone is refused | T3 |
| `TestIndex_PartitionedProcedure`, `TestIndex_RunnerLifecycle` (P-10, N138, P-8, P-9) | `engramctl index` on a 16-partition table; the index runner with a crash before the `CREATE`, a cancelled build, a rollback of a move and a namespace delete, and an open `REPEATABLE READ` transaction | `CREATE INDEX … ON ONLY` the parent, `CREATE INDEX CONCURRENTLY` on each partition, `ATTACH PARTITION`; a migration that issues `CREATE INDEX CONCURRENTLY` on the parent fails `engramlint migrate`; the runner re-leases a `building` row, drops an `indisvalid = false` index before building (never `IF NOT EXISTS`), refuses to start while any `backend_xmin` is older than 5 min, serialises builds per shard with `maintenance_work_mem = 2.4 KB × vectors` and `shm_size: 8g` (two concurrent builds never exceed it), is the only process holding `engram_migrate`, and `rollback_target` and `DropIndexes` leave no orphan index (drop by `engram_hnsw_ddl` names) | T3 + S |
| `TestIndex_Hygiene` (N138, P-4) | delete 1 %, 5 % and 100 % of a 1 M-vector namespace; autovacuum left on | a namespace delete is `DROP INDEX` plus paced batched `DELETE` with no graph repair; a partial purge increments `vector_indexes.purged_since_build` and the runner rebuilds the touched HNSW at 1 % of `rows_at_build` or 2 k elements (≈ 0.35 ms per vector), then `VACUUM (INDEX_CLEANUP ON)`; vector partitions carry `vacuum_index_cleanup = off`, so autovacuum never repairs a graph (the configuration without it spends 5 to 6 times the rebuild); no autovacuum run > 30 min; there is no `pgstattuple` or dead-fraction signal | T3 + T5 |
| `TestIndex_BuildMemory` (N138, P-7) | builds of 100 k, 1 M and 2 M vectors with the runner's setting, then 2.5 M | `maintenance_work_mem` is 240 MB, 2.4 GB and 4.8 GB, builds stay in memory at 0.28 to 0.37 ms per vector (a build at the old 2 GB limit logs "graph no longer fits" above ≈ 0.95 M and drops to ≈ 3 ms per insert); `/dev/shm` stays under `shm_size`; a namespace above ≈ 2.1 M vectors is refused by the quota and flagged for a move | T3 |
| `TestMarkers_BoundedByLag` (N119, N135, P-2) | 20 re-extractions and 1 k `Invalidate`s over a 50 k-fact namespace | `chunk_tomb` and `doc_tomb` sets stay under 16 k and shrink after the purge; invalidations never form a request array (PK anti-join), the per-arm planner cost does not grow with them; old-key facts are purged after 1 h and the consolidation watermark pin ends | T3 |
| `TestCapacity_Budgets` (N114, P-5, P-11) | the real arm SQL (three vector arms, graph arm on the temporal family, exact path, BM25) at 50 QPS on a shard filled to 8 M and to 12 M facts | page touches per MID recall ≈ 10 k and IOPS ≈ 25 k at a 5 % miss rate against the 50 k budget, recorded per arm and replacing the N114 table; hot set (vector heap, HNSW, links reverse index, `ins_seq` indexes) ≈ 80 GB at the target; relation bytes ≈ 150 GB at 8 M and ≈ 230 GB at 12 M fit the 600 GB volume; `ShardNearCapacity` is driven by relation bytes including hidden and unpurged rows and old-model vectors, not `live_facts`, and pages at 70 % | T3 + bench |
| `TestXID_Budget` (N114, P-11) | 4 consumers idle for 5 min; 50 writes/s for 10 min | cursor advances are batched to ≤ 1 per second per consumer (idle XID consumption ≤ 5/s); `age(relfrozenxid)` is exported and the 50 % alert rule passes `promtool test rules`; `vacuum_freeze_min_age = 10 M` on content partitions | T3 |
| `TestConfig_Lint` (N131, N133e, N139) | `engramctl config lint` | the `postgres -c` list and the role GUCs are generated from one table; drift fails; every role that writes the outbox (`engram_app`, `engram_admin`, `engram_move`) has the 30 s `statement_timeout` and `idle_in_transaction_session_timeout` (raised per session only inside `engramctl`); admin, move, relay and migrate DSNs and pool sizes are present and the connection budget sums under `max_connections`; `engram_entity_fuzzy` is not executable by `engram_relay` or `engram_move`; one exclusive-lock timeout (35 s single attempt) is generated into every procedure and migration; `wal_compression = zstd`; `pick_shard` derives `namespaces_count`; `engram_cleanup_namespace`'s outer `DELETE` carries `namespace_id`; scheduler writes to `operations` and the outbox go through `WithNamespaceTx(write)` | S + T3 |
| `TestRecall_SmallNamespaceExact`, `TestLexical_TopKPushdown` | 16-partition fixture, namespaces of 1 k and 50 k facts; the lexical query on the real image | the 1 k namespace is an exact scan returning 150 candidates in < 5 ms warm; BM25 shows the pg_search custom scan with Top-K pushed down, one partition pruned, composite `key_field` accepted, p95 < 60 ms; otherwise the fixture switches to `TsvectorIndex` and records its p95 for M0.2 | T3 |

#### 8.4.5 Retain, consolidation, fence and other faults

| Fault or test | How it is injected | Expected behaviour and assertions | Tier |
|---|---|---|---|
| Stale catalog cache | `MemoryCatalog.DropNotifications()` on api-1, then a completed move of `A1` | first store call fails `WrongShardOrEpoch`; the router invalidates, re-resolves and retries once (a second consecutive failure on a write → `FAILED_PRECONDITION`); a read that sees `MOVED_OUT` re-resolves in a bounded loop ≤ 5 s; exactly 2 shard transactions; latency < 2× baseline | T5 (T3 with `FakeTx`) |
| Misrouted request (phase 3) | cell-2's `StaticResolver` claims shard 3 lives in cell 1 | cell 1 gets `engram-forward-hops: 1`, fails ownership, answers `WrongShardOrEpoch`, **never forwards again**; deadline and metadata preserved | T5 |
| Gateway 429 / 5xx / 4xx | `GW_STATUS=429` with `Retry-After`, `GW_FAIL_RATE=0.3`, `GW_STATUS=400` for one chunk hash | activities back off and honour `Retry-After`; a 400 is `PermanentLLMError` (recorded in `operations.error`, the operation `SUCCEEDED` with `units_failed > 0`); recall drops dense arms on embed 5xx and skips rerank on rerank 5xx; recall p95 under fault ≤ 300 ms | T5, T2 |
| Blob unavailable | `toxiproxy` cuts MinIO | a retain body > 64 KiB fails at ack `UNAVAILABLE`, ≤ 64 KiB succeeds; extraction proceeds with cache misses; expunge purge and export retry; **deletes commit their marker but return `UNAVAILABLE` without an ack because the intent `put` fails; the retry completes it** (8.4.2); after the toxic is removed everything completes within 2 min | T5, T3 |
| Temporal outage and loss | `docker pause` of the cell's Temporal for 90 s with 200 retains; then loss of its Postgres (restore from its own backup) | acks fail `UNAVAILABLE`; the `op-sweeper` starts any `PENDING` operation older than 2 min with no workflow; no operation runs twice (`ns/{ns}/op/{operation_id}`); after a Temporal restore, `operations` rows drive `Restart` of everything in flight; other cells are unaffected (cluster per cell, P-9) | T5 |
| Duplicate activity execution | `env.OnActivity(CommitChunk).Twice()`; likewise `FinalizeVersion`, `StoreProposal`, `ApplyBatch`, `MoveCopyRange`, `Expunge.Purge`, `DerivedPurge`; a second `ConsolidateRoute` answer that differs | the second execution is a no-op; the stored proposal wins (N43); counts, outbox and `consolidation_applied` unchanged; a purge batch repeated deletes nothing more | T2 (+ T3) |
| Outbox relay double election | two workers per shard; `SIGKILL` the lock holder; partition its direct connection | the survivor acquires within 5 s; every event delivered exactly once; leader gauge sums to 1; the partitioned holder's next cursor write fails | T5 |
| Statement timeout during a write | `SET LOCAL statement_timeout = '50ms'` on one writer | the aborted `seq` is declared aborted after `2 × 30 s`; later seqs are delivered on time | T5 |
| Quota exhaustion mid-operation | `llm_tokens_per_day` set to 10 k; a 40-chunk document; a consolidation batch | `quota.Reserve` precedes every gateway call class (extract, routing, write, page refresh, each Reflect iteration); workflows go `DEFERRED` and resume at the window reset, synchronous Reflect returns `RESOURCE_EXHAUSTED`; no chunk extracted twice | T2 + T5 |
| `TestRetain_CommitOrdering` (N40) | `CommitChunk(v, h)` paused after `BuildLinks` while (a) `DeleteDocument` commits, (b) `v₂ > v` is retained and finalised, (c) `v₁`'s `FinalizeVersion` runs after `v₂`'s | (a) the commit aborts on `status ≠ 'ingesting'`, or, if it wins the race, its rows are tombstoned by the predicate and no surface returns them (the expunge purges them); (b) `superseded`, zero rows, `Recall` never returns `h` after `v₂`'s finalise; (c) `v₁` is `superseded` and adds no `chunk_tombstones`; exactly one `active` version | T2 + T3 |
| `TestConsolidation_TwoStage` (N121) | candidates with distinctive phrases; merges; a hidden source | stage 1 persists decisions only; no written observation text contains a phrase that is not in its own sources or attached facts; `inputs(O, v)` = the facts rendered, all of them O's own sources; a `merge` is a root rebuild from the union of live sources with no previous text; only visible sources are rendered; a batch is re-queued at most 3 times then `failed`; the measured `calls_per_chunk` (≈ 3.5) and A-W (0.8) are recorded for Table 6.8-B | T2 + T3 |
| `TestConsolidation_PersistedProposal`, `_AtomicKey`, `_ApplyReverifiesInputs` (N43) | write calls executed twice with different answers; worker killed between effect and key; a source made hidden between `StoreProposal` and `ApplyBatch` | one proposal row, one effect per `op_key`; one transaction or a retry applies once; re-verification (the visibility predicate over `$fact_ids` in a fresh statement, under the shared derivation lock, no `FOR SHARE` on facts) discards the proposal and re-queues without the hidden id | T2 + T3 |
| `TestConsolidation_StaleProposalDiscarded`, `_AllSkipStamps`, `_CapacityAttempt` (N121, C-3, C-11) | a stored `update` whose base was superseded by a root rebuild; a batch whose ops are all skips; a capacity overflow | the whole proposal is discarded (`state = 'discarded'`, new `attempt`, nothing deleted by `engram_app`) and the batch re-routed; an all-skip batch has an empty list, applies zero ops, stamps its facts `done` and sets `applied`, so it terminates; the capacity re-run is a new attempt with `prompt_variant = 'capacity'`, so `ON CONFLICT DO NOTHING` cannot keep the overflowing list; 'already applied' is `consolidation_batches.state = 'applied'`, written with the stamps | T2 + T3 |
| `TestConsolidation_PendingFromWatermark` (H-15) | consolidate 500 facts, stamp failures, hide a fact | `fact_consolidation` is append-only with `PRIMARY KEY (namespace_id, memory_id, stamped_at)`; consolidated = an `EXISTS` row with `note = 'done'`; retryable = latest stamp `failed` and older than 7 days; the watermark stays below the smallest unconsolidated fact, live or marker-hidden | T3 |
| `TestRetain_ConcurrentAppendsChain`, `TestAppend_ReadsOneBodyObject` (N56, H-16) | two `APPEND` items 1 s apart; a chain of 50 appends | the second append's base is the first's version, final text `body(c) ‖ A ‖ B`; `LoadItem(APPEND)` materialises the base body from the ledger chain up to the nearest version with a body (`append_base_version`), under the document lock | T3 |
| `TestFence_TryLockRefusedBehindWaiter`, `TestFence_PoolNotExhausted`, `TestDocLock_NoStarvation` (N82, N83) | session A holds the shared fence, B requests the exclusive one, C tries shared; 32 writers plus a freeze; continuous `CommitChunk`s against `FinalizeVersion` | C is refused immediately (`NamespaceFrozen{retry_after = 200 ms}`); no pooled connection waits behind the freeze; recall p95 on another namespace does not move; exclusive takers make one attempt with `lock_timeout = 35 s` (no 5 s retry storm, P-12); `DocumentBusy` is retryable; no `FOR SHARE` on `documents` or `document_versions` in any `store.Queries` text | T3 + T5 |
| `TestExtraction_RenderHashKey` (N87) | the same chunk under a changed mission, hints, timestamp day or summary | each change is a new key; identical rendered variables hit | T1 + T3 |
| `TestCommit_InputBlobMissing`, `TestIso_Temporal_KeyShredding` (N99, N100) | purge an `xcache` blob between `EmbedChunk` and `CommitChunk`; delete a namespace | retryable `InputBlobMissing`, at most two re-runs; a blob younger than 24 h is never purged; histories hold ciphertext only and are unreadable after the data key is destroyed | T3 |
| `TestEvents_SizeBound` (N80) | every event type at its maxima | `len(Marshal(e)) ≤ 16 384`; `DocumentDeleted` is one small event (no id lists: consumers read by `(namespace_id, document_id)`) | T1 |

#### 8.4.6 Model-checking regressions (N46, N141)

Every counterexample configuration has a Go twin that reproduces the flaw with the fix
disabled (a `faultinject` knob) and proves its absence with the fix on; `formal/MANIFEST.md`
pairs each spec with these tests and `scripts/formal-manifest-check.sh` fails a PR that changes
one without the other. Every must-fail configuration runs in CI and its log is kept in
`formal/tla/results/`. M0.7 is conformance and trace validation of these twins against the
specs (§10).

| Spec | Properties (must pass) | Must fail | Go twin |
|---|---|---|---|
| `Derivation.tla` | `NoDeletedDerivationServed`, `NoInvalidatedDerivationServed`, `NoOverHiding`, `RestoreExact`, `AsOfNoLeak`, `MaterializeComplete`, `EvidenceOutlivesFacts`, `BaseCurrentAtCommit`, `NoLostRebuild`, `NoVictimTextAfterPurge`, `FailClosed`, `ReextractKeepsDerivedVisible` (`Served` equals the SQL rule; `MatInvalid = TRUE` with causes is the design); `Derivation.cfg`, `_Page.cfg` | `_NoLock`, `_RestoreNoLock` (now fails), `_CascadeEvidence`, `_ReextractHides`, `_PageNoVerify`, `_StaleProposal`, `_MatOnce`, `_PurgeDropsStub` | `TestVisibility_AllSurfaces`, `_SegmentHiding`, `TestInvalidate_RestoreExact`, `TestExpunge_DerivationLock`, `_Stages`, `TestReextract_Rebuilds`, `TestPageRefresh_DeleteMidCall`, `TestConsolidation_StaleProposalDiscarded`, `TestAsOf_*` (8.5) |
| `Storage.tla` | `ContentImmutable`, `VectorGenerationConsistent`, `IndexConvergence`, `RebuildBeforeRepair` (`Purge` counts `dead[idx]`) | `_AutoRepair` | `TestContent_InsertOnly`, `TestVectors_ModelGeneration`, `TestHNSW_PerNamespace`, `TestIndex_Hygiene` |
| `Durability.tla` (`MaxT = 7`; `Issue` split into `Commit` and `PutIntent`; `CrashBeforePut`, `Dup`, `Retain`; `rp` outside the restorable state, never raised) | `AckedDeleteSurvives`, `NoUnackedEffectOnLaterAck`, `IntentOrderLastWins`, `RestoreReplaysIntents` | `_IntentBeforeCommit`, `_RaiseOnReopen`, `_ClockOrder` (and the round-3 `_AckBeforeIntent`, `_NarrowWindow`, `_ReopenEarly`, `_RetargetRestore`, `_UnorderedReplay`) | `TestIntent_AckImpliesIntent`, `_DuplicateAttempt`, `TestRestore_ReplaysIntents`, `_ChainOrder`, `TestRestore_NoUnackedEffect`, `_FloorInCatalog` |
| `ShardMove.tla` (`SrcDelete`, `InsertOldId`, seq-keyed re-copy with a bounded `Lag`, catalog `Commit` CAS before `Cut`, recovery `Abort` CAS, `Cleanup`) | `SingleWriter`, `NoRouteToTargetBeforeC`, `RollbackPossibleBeforeC`, `NoLossNoDup`, `OneOwner`, `CatalogNamesOwnerAfterDone`, `ZombieCannotCutOver`, `RestoreReconciles`, liveness `MoveTerminatesActive`; `ShardMove.cfg`, `_ActiveWriters` | `_StampAfterCut`, `_IdKeyedRecopy`, and the round-3 `_NoReady`, `_NoTimelineCheck`, `_NoVerify`, `_RestoreNoReconcile`, `_UnfencedSteps` | `TestMove_RollbackEveryStep`, `_ReadyState`, `_DirtyCopyReconcile`, `_ActiveBacklog`, `_ZombieFenced`, `TestRestore_OpenMoves`, `TestMove_FailoverBetweenCAndD` |
| `Outbox.tla` | `NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered` | `Outbox_Watch1x.cfg`, `_NoWatch` | `TestOutbox_Watch1x` |
| `Consolidation.tla` | `ExactlyOnceEffect` | `_VolatileProposal`, `_NonAtomicKey` | `TestConsolidation_PersistedProposal`, `_AtomicKey` |

#### 8.4.7 Round-4 regression index

One scenario per finding, each failing without its fix (names are the tests above; a finding
with several symptoms has one test per symptom).

| Finding | Scenario that must fail without the fix | Test |
|---|---|---|
| C-1 | `REPLACE`, chunk purge, then `DeleteDocument`: observations built from the retired text are still hidden and stubbed | `TestVisibility_AllSurfaces`, `TestExpunge_Stages` |
| C-2 | a page refresh whose LLM call spans a delete and its Materialize is refused and rebuilt | `TestPageRefresh_DeleteMidCall` |
| C-3 | a stored `update` against `O@v2` after a root rebuild (v3) is discarded, not committed as v4 | `TestDerivation_CommitRule`, `TestConsolidation_StaleProposalDiscarded` |
| C-4 | two restores and a third after the replay's own commits keep every acknowledged delete; the floor is in the catalog and never raised | `TestRestore_FloorInCatalog` |
| C-5 | an unacknowledged `Invalidate` is not replayed over an acknowledged `Restore` and retain | `TestRestore_NoUnackedEffect` |
| C-6 | a prompt bump hides no observation or page at any `as_of`; flagged observations are rebuilt, old-key facts leave after 1 h | `TestReextract_Rebuilds` |
| C-7 | the `Restore` lock is load-bearing and `Materialize` re-reads per batch (the model fails without them) | `TestExpunge_DerivationLock`, `Derivation_RestoreNoLock` |
| C-8 | rows inserted for old ids during an active move are re-copied and the move completes | `TestMove_ActiveBacklog` |
| C-9 | purging a document never deletes a blob a live row references; an acknowledged raw body is never lost | `TestBlob_OwnerKeyed` |
| C-10 | failover between (c) and (d) completes the move; before (a″) it rolls back; no state leaves `moved_out` against an emptied target | `TestMove_FailoverBetweenCAndD`, `TestRestore_OpenMoves` |
| C-11 | discard, capacity retry and an all-skip batch terminate with a new `attempt` and no `DELETE` | `TestConsolidation_StaleProposalDiscarded`, `_AllSkipStamps`, `_CapacityAttempt` |
| P-1 | the freeze carries only the rows since `T_pre − 10 min` and the mutable rows | `TestMove_ActiveBacklog`, `TestMove_VerifyFitsFreeze` |
| P-2 | marker sets stay bounded by expunge lag; invalidations are an anti-join | `TestMarkers_BoundedByLag` |
| P-3 | selective tag or `as_of` filters never seq-scan the partition and keep `min(cap, n_visible)` | `TestRecall_FilteredArm` |
| P-4 | hygiene rebuilds at 1 % before any repair and autovacuum never repairs a graph | `TestIndex_Hygiene` |
| P-5 | measured touches and IOPS replace the N114 table; the reverse index is in the hot set | `TestCapacity_Budgets` |
| P-6 | a 100 k-fact delete and a 1 M-fact namespace delete stay at ≤ 25 MB/s of WAL inside the RTO term | `TestExpunge_WALBudget` |
| A-1 | filtered semantic arms return `min(cap, n_visible)` or `partial`, within the IOPS and latency budget | `TestRecall_FilteredArm`, the selectivity sweep of 8.6.1 |
| A-2 | after the purge no derived text, vector, BM25 hit, markdown or transcript of the victim exists | `TestExpunge_Stages` |
| A-3 | `GetDocument` and `ListDocuments(include_deleting)` return the tombstone view only | `TestDocument_TombstoneView` |
| A-4 | an `Invalidate` reaches synced agents through the manifest overlay within one poll | `TestExport_HiddenOverlay` |

### 8.5 `as_of` leakage tests

**Corpus** (`internal/recall/asoftest`, reused by the bench harness). `Build(n=60)` produces
one document per day `t_1 < … < t_60`, each with a unique sentinel (`ZEBRA-0017`) that appears
in the text, so leakage is detectable lexically (BM25 on the sentinel), semantically (the
`DeterministicClient` embeds the sentinel into a distinct region) and structurally (`document_id`
encodes the day). `GW_EXTRACT_MODE=sentence` yields one fact per sentence with
`mentioned_at = item.timestamp`; one third of the facts carry `occurred_start` **before** their
`mentioned_at` (an event told later) and one tenth carry `occurred_start` **after** (a plan), so
the test distinguishes `mentioned_at` (the `as_of` axis, D9) from `occurred_*`.

**Grid** (`TestAsOf_Grid`, T3 over the real shard schema; T1 twin over `MemIndex`). For `T` in
`{t_1 − 1 d, t_1, t_1 + 12 h, t_2, …, t_60, t_60 + 1 d}` (121 points), for each arm run alone
(`RecallOptions.OnlyArm`, test-only) and for the full pipeline at every stage (`FUSED`,
`RERANKED`, `PACKED`), with budgets low/mid/high, with and without tag filters and type
filters:

1. every returned fact and chunk has `mentioned_at ≤ T` (`NoFutureFact`);
2. every returned observation is the latest version with `effective_at ≤ T`, and none is returned if the first version is later than `T`;
3. every returned page version has `effective_at ≤ T`;
4. graph expansion never *traverses* a fact with `mentioned_at > T` (a plant: a hub fact at `t_59` linking visible facts; with `T = t_30` the visible facts must not be reachable through it — checked by `ArmRank` provenance);
5. the temporal arm never returns a fact whose `occurred_*` window overlaps the query window but whose `mentioned_at > T` (the "plan" facts);
6. budget not consumed by invisible rows: each arm returns `min(cap, n_visible)` (the filter is inside the arm, D9);
7. a query for a sentinel of day `k > T` returns **zero** results at every stage (the strongest form: absence, not just filtering).

**Observation-version test** (`TestAsOf_ObservationVersions`, T3, phase 2). Ingest days
1–5 → consolidate → observation `o` v1 (create, a root, `effective_at = t_5`); ingest 6–8 →
consolidate → v2 (update, `root_version = 1`, `effective_at = t_8`); delete the day-7 document
(a fact input of v2) → v2 is hidden by the read-time predicate (its segment `inputs(o, 1..2)`
names the victim; v1's segment `inputs(o, 1)` does not) → the expunge's root rebuild lands v3
(`root_version = 3`, live sources only, `effective_at = max(mentioned_at over the facts shown,
effective_at(v2)) = t_8`). Assertions: `as_of = t_4` → no `o`; `t_6` → v1 (written without the
victim, servable before and after the delete); `t_8` → v3 (the latest with `effective_at ≤ t_8`,
not v2); `t_9` → v3; between the delete ack and the rebuild `as_of ≥ t_8` returns no `o` (v2 is
the latest version with `effective_at ≤ T` and is hidden; the predicate never falls back to an
older version, N117) and `as_of ∈ [t_5, t_8)` still returns v1; a retired observation is filtered by its own time (`retired_at IS NULL OR retired_at > as_of`) and a version with no visible source is not served. The wall clock is irrelevant: the test
runs consolidation with the fake clock set to `t_60` and asserts `effective_at` never equals
`now()`. "Consolidation after `T` must not leak": with `as_of = t_6`, a version created *later
in wall time* whose writer saw only facts ≤ `t_6` is allowed (it is v1), and any version whose
writer saw a fact > `t_6` is not returned; the property is about `effective_at`, not creation
time.

**Deleted-derived version stays hidden under `as_of`** (`TestAsOf_DeletedDerivedVersionStaysHidden`,
T3, phase 2; the Go twin of `Derivation.tla`'s `AsOfNoLeak`). Observation `o` has v1 (create,
`effective_at = t_1`, inputs `{f_1}`), v2 (update, `t_5`, `{f_5}`) and v3 (update, `t_9`,
`{f_9}`), all in one segment (`root_version = 1`). Delete `f_5`'s document: v2 and v3 are hidden
(their segments contain `f_5`), v1 is not; the expunge's materialize writes `derived_hidden`
rows and a root rebuild lands v4 (`root_version = 4`, `effective_at = t_9`). Assertions:
`Recall(as_of = t_7)` returns **nothing** for `o` (v2 qualifies by `effective_at` but its text
was written with `f_5` in the prompt); `as_of = t_3` → v1; `as_of ≥ t_9` → v4; no surface
returns the text of v2 or v3; the `derived_hidden` rows for the document cause are permanent,
so no later version ever re-exposes them. With the segment start forced to `version`
(`faultinject`, "lineage-less") `as_of = t_9` before the rebuild returns v3, whose text was
written from v2's text, which contained `f_5`: the leak the segment rule closes.

**Inputs, not only citations** (`TestAsOf_InputsNotOnlyCited`, T3; the Go twin of §7's
`AsOf_CitedOnly` counterexample). Facts `f₁` (`mentioned_at = t_1`) and `f₂` (`t_2`) are
attached to `o` in one stage-2 write whose answer cites `f₁` only. Assertions: `o.v1.effective_at
= t_2` (the writer saw `f₂`), `observation_inputs(o, 1) = {f₁, f₂}`, `Recall(as_of = t_1)`
returns no `o`, `Recall(as_of = t_2)` returns `o`. With the cited-only rule (`faultinject`)
`as_of = t_1` returns `o`, the leak the model found.

**Page test** (`TestAsOf_PageVersions`, phase 3): a page refreshed after each consolidation has
`page_versions` v1..v3 with the same `effective_at` values and `page_version_inputs`;
`GetPage(as_of)` and the `get_page` Reflect tool return the version rule above; a page whose
first version is after `T` is `NOT_FOUND{RESOURCE_KIND_PAGE}` at that `as_of`; a page version
whose segment holds a tombstoned fact or a hidden observation version is not returned at any
`as_of`.

**Entities** (`TestAsOf_Entities`, N118): an alias merge learned after `T` is invisible under
`as_of = T` (`EntityRef` carries only `mention`, `canonical_name` is suppressed) and graph entity
hops use `entity_mentions.mentioned_at ≤ T`; deleting the evidence document removes its aliases
and recomputes `canonical_name`.

**Reflect** (`TestAsOf_Reflect`): every tool call in a Reflect session with `as_of = T` passes
`T` through, and every citation in the `final` event has `mentioned_at`/`effective_at ≤ T`
(the citation verifier already restricts to returned ids; this test proves the returned ids
were themselves filtered).

**Harness canary** (§8.6): every benchmark question runs with `as_of = question date + 1 s`
(LongMemEval) or the last session date (LoCoMo) and the harness counts
`leak_canary = #results with mentioned_at > as_of`; the run is invalid if the counter is
non-zero. Conformance to `Derivation.tla`: the T3 grid records `(op, T, returned ids)` traces
that the §7 trace validator replays against the spec nightly.

### 8.6 Benchmark harness

`cmd/engram-bench` (Go, in-repo; datasets under `bench/datasets/`, results under
`bench/results/`). One command per stage so that ingestion (expensive) and querying (cheap)
can be re-run independently: `engram-bench ingest|query|judge|report --dataset … --run …`.

| Dataset | Size | Unit of ingestion | Namespaces | Gold for retrieval metrics | Answer categories |
|---|---|---|---|---|---|
| LongMemEval-S (`lme_s`) | 500 questions, ~40 sessions / ~115 k tokens per haystack | one Retain item per session: `timestamp = haystack_dates[i]`, `document_id = q{qid}/s{session_id}`, `tags = [session:{id}]`, content = the session's turns rendered `user:`/`assistant:` | one namespace per question (`bench/lme-s/{qid}`), tenant `bench` | `answer_session_ids` | single-session-user, single-session-assistant, single-session-preference, multi-session, temporal-reasoning, knowledge-update; abstention subset (`*_abs`) |
| LongMemEval-M (`lme_m`) | 500 questions, ~500 sessions / ~1.5 M tokens per haystack | same | same | same | same |
| LoCoMo (`locomo`) | 10 conversations, 1,986 QA (1,540 excluding adversarial cat. 5), ~19 sessions and ~9.2 k tokens per conversation | one item per session: `timestamp` parsed from `"1:56 pm on 8 May, 2023"`, `document_id = conv{n}/session_{k}`, every turn prefixed with its dialog id (`D1:3`) so chunk offsets map back to turns | one namespace per conversation | `evidence` dialog ids | single-hop, multi-hop, temporal, open-domain (adversarial reported separately, not in the headline) |

**Leak-free ingestion.** Items carry the session timestamp, so `mentioned_at` is the session
date and not the ingestion time; every query runs with `as_of = question_date + 1 s` (LME
gives `question_date`; LoCoMo uses the last session's date). The harness also runs each
question once with `as_of` unset and reports the delta ("`as_of` bonus"); a positive delta
beyond noise indicates leakage in the *dataset*, not the system, and is reported, never
silently absorbed. `WaitOperation` on every retain and a `consolidation_lag == 0` check (phase
2) precede the query stage so retrieval measures a fully enriched namespace.

**Answer generation** (Recall makes no LLM calls, so the harness supplies the answering step
in two arms):

| Arm | How | Purpose |
|---|---|---|
| `recall+answer` | `Recall(budget=mid, max_tokens=4096, include chunks)` → packed context → one chat call with prompt `bench/answer/v1` (temperature 0) | isolates retrieval quality; comparable to "RAG over Engram" |
| `reflect` | `Reflect(query, budget=low)` (phase 2) | the product path; comparable to Hindsight's benchmark path |

**Judge (pinned).** Model `gpt-oss-120b` through the gateway (A-B1: the gateway serves it;
it is the judge Hindsight's paper used at temperature 0, which keeps our numbers comparable
to theirs); `temperature = 0`, `top_p = 1`, `max_tokens = 4`, `seed = 7`. Prompt `judge/v1`
(the same for every category except abstention):

```
System: You are a strict grader. Compare a candidate answer with a reference answer to a
question about a long conversation. Output exactly one word: CORRECT or WRONG.

User:
Question date: {question_date}
Question: {question}
Reference answer: {reference}
Candidate answer: {candidate}

Rules:
1. CORRECT if the candidate conveys the same essential information as the reference. Extra
   correct detail or different wording is fine.
2. WRONG if the candidate contradicts the reference, omits the essential fact, hedges between
   several answers of which only one is the reference, or claims the information is
   unavailable when the reference is a definite answer.
3. Dates and durations: CORRECT only if they match the reference at the precision the
   reference uses (year, month, day, or number of days/weeks). Relative expressions are
   judged against the question date.
4. Names and numbers must match after normalising case, punctuation and whitespace.
Answer with one word: CORRECT or WRONG.
```

`judge/v1-abstain` (LME abstention questions) replaces rules 1–2 with: CORRECT if the
candidate states that the information is not available in the conversation; WRONG if it
produces any specific answer. Verdict parsing accepts only `CORRECT`/`WRONG` (anything else is
re-asked once at temperature 0, then counted `WRONG` and flagged `judge_parse_error`).

**`bench.lock`** (checked in; a run whose environment differs from the lock is refused unless
`--relock` is passed, which writes a new lock and a diff into the report) pins: the Engram git
sha, config hash and `schema_version`; each dataset's sha256 and source file; every model id as
the gateway reports it (`embed` nomic-embed-text-v1.5/768, `rerank` bge-reranker-base, and
`extract`, `consolidate`, `answer`, `reflect` and `judge` all `gpt-oss-120b`, A-B2, the judge at
temperature 0, `top_p` 1, `max_tokens` 4, seed 7); the prompt ids (`extract/v1`, `summarize/v1`,
`consolidate_route/v1`, `consolidate_write/v1`, `dedup_adjudicate/v1`, `reflect/v1`,
`bench/answer/v1`, `judge/v1`, `judge/v1-abstain`) with the sha256 of the judge and answer
prompts; the recall settings (`budget` mid, `max_tokens` 4096, caps 50/150/400, `rerank_top` 0/50/150
per budget until A-R1 is measured, boosts on); and the run settings (3 repeats, `as_of =
question_date + 1 s`).

A model `version` that changes on the gateway (the gateway reports it per response) fails
the lock check: the run stops, and the report of the re-locked run carries a
"judge/model drift" banner. Judged outputs (`results.jsonl`) are kept so a new judge can
re-grade old candidates for a like-for-like comparison.

**Metrics** (all in `stats.json`; the report renders them):

| Metric | Definition | Source |
|---|---|---|
| Accuracy (overall, per category, abstention) | judged `CORRECT` / questions; mean ± stdev over 3 repeats; paired bootstrap 95 % CI for deltas between arms | `judge` stage |
| R@5, R@10 (retrieval stage) | gold session (LME) or gold turn (LoCoMo, via chunk offsets) among the sessions/turns of the top-k **fused** results, and again of the top-k **packed** results; "any-evidence-in-context" = gold ∩ packed ≠ ∅ | `RecallStats` + provenance |
| Tokens per question | packed context tokens (cl100k), answer prompt tokens, completion tokens; for `reflect`: total context tokens across iterations | `RecallStats`, gateway `Usage` |
| Cost per conversation | ingestion cost (summarize + extract + embed + consolidate, from `token_usage` × `CostTable`) per haystack/conversation, plus query cost per question; judge cost reported separately and excluded from product cost | `token_usage`, `bench` gateway hook |
| Latency p50/p95 per stage | `authz`, `catalog`, `embed_query`, each arm, `fuse`, `rerank`, `boost+pack`, `stream`; end-to-end; reflect per iteration | `RecallStats` trailer, traces |
| Leak canary | results with `mentioned_at > as_of` (must be 0; `mentioned_at` is the server-set item timestamp, D9, so the canary cannot be fooled by a backdated model value) | harness From N85 the canary also asserts, under `as_of`, that no returned `quotes` row has `mentioned_at > as_of`, that `ChunkInfo.header` is empty, and that chunk rank is unchanged by content learned after `as_of` (`embedding_effective_at <= as_of`). |
| Ingest throughput | chunks/s per worker; gateway RPM observed; cache hit rate on a re-ingest of the same corpus (must be ≈ 100 %) | metrics |
| Re-embeddings per append (N60; review F-14) | chunk and fact vectors recomputed per `APPEND` item on a growing conversation (`engram_retain_reembed_total{reason}` / appends): must be **≈ 1 chunk vector and its facts per append** (the new chunk only) and **0 fact vectors for unchanged chunks**; a summary refresh (> 25 % growth or `REPLACE`) may re-embed the document's chunk vectors once and is reported with `reason="summary_refresh"` | metrics |
| Rerank skip rate (N53, N106; reviews F-6, G-28) | `engram_recall_rerank_skipped_total{reason="deadline"}` / recalls at the target QPS with a client deadline ≥ 300 ms; **an SLO breach above 1 %**, not a degradation; stated as a **p99 property of the pre-rerank path** (skip only when pre-rerank time exceeds `deadline − 106 ms` = 194 ms at 300 ms; the 106 ms reserve is `rerank p95 + pack + stream + 8 ms`), so the report prints the pre-rerank p95/p99 next to the rate; below 300 ms client deadlines skipping is by design and is excluded; reported next to the measured gateway pairs/s (A-R1) | metrics |
| Marker sets and degraded mode (N116, N119) | `engram_visibility_marker_entries{kind}` histogram per request (alert above 16 k), `engram_degraded_namespaces` gauge and the time until the last `pending` marker clears (`engram_expunge_oldest_pending_seconds{phase}`); the benchmark deletes a 100-fact session and a 100 k-fact document and records recall p95 in degraded mode against the baseline | metrics |
| Critical-path p95 (N54) | end-to-end p95 at MID against the 216 ms critical-path budget (authz 2 → embed 25 → lexical ‖ semantic 60 → graph 30 → fuse 1 → rerank 90 on 50 pairs → pack 3 → stream 5), with per-stage attribution from the `RecallStats` trailer | `RecallStats` |

**Ablations** (each is one `--ablate` flag; the harness re-runs only the query stage so the
whole set costs one judge pass per arm):

| Ablation | Flag | Expected direction (A-B3: from the LongMemEval paper's retrieval ablations and this repository's earlier retrieval-only runs — an unrelated benchmark, not Hindsight: hybrid ≈ +9 pp R@5 over BM25 alone; every row is a direction to measure, not a claimed improvement, N73) |
|---|---|---|
| each arm off | `--ablate arm=semantic|lexical|graph|temporal|chunks` | semantic or lexical off: −5–10 pp R@5; temporal off: temporal-reasoning −3–6 pp; chunks off: single-session-user/preference −2–4 pp; graph off: multi-session −2–3 pp |
| no rerank | `--ablate rerank` | −1–3 pp accuracy, −90 ms p95 (50 pairs at MID, N54) |
| reranker model | `--rerank-model bge-reranker-base|bge-reranker-v2-m3` | v2-m3 (the per-namespace upgrade, N53) expected +0–2 pp over the `base` default at ≈ 3× the pairs/s cost; decides the default from data, not assumption |
| semantic arm plan | `--ablate exact_scan` (force the shared partition HNSW for small namespaces) | small-namespace R@5 drops when the candidate list truncates (N55); measures the F-8 effect on the LME namespaces, which are small by construction |
| no chunks arm and no `include chunks` | `--ablate chunks,chunk_context` | isolates the value of raw text in context |
| budgets | `--budget low|mid|high` | high: +1–2 pp at +40 % tokens |
| no boosts | `--ablate boosts` | ±1 pp; temporal-reasoning −1–2 pp |
| chunk header off | `--ablate header` | −1–2 pp on multi-session |
| fixed 3,000-char chunker (Hindsight baseline) | `--chunker fixed` | −0–2 pp; establishes the chunker's contribution |
| `as_of` unset | `--ablate as_of` | ≈ 0 on these datasets (all sessions precede the question); a positive delta flags dataset leakage |
| observations off (phase 2) | `--ablate observations` | knowledge-update −2–4 pp |

**Report format.** `bench/results/{YYYY-MM-DD}/{dataset}/{run_id}/` holds `bench.lock`,
`results.jsonl` (per question: id, category, gold, candidate, verdict, retrieval hits, tokens,
per-stage latency, `as_of`), `stats.json` and `report.md`, rendered from one template: a header
with the lock summary, one row per metric above for each arm with the delta against last week
and a notes column (accuracy per category, R@5/R@10 fused and packed, tokens and cost per
haystack with the measured A-F and calls per chunk, re-embeddings per append, recall p50/p95
against the 216 ms budget, reflect latency), the ablation rows with CIs, and a failures line
(judge parse errors, gateway errors, deferred operations).

**Cost of a run** — the numbers are **Table 6.8-B** (N74, N130) and are not re-derived here: at
A-F = 10 an LME-S run is 75 k extraction calls (≈ $50), 20 k summaries (≈ $8), embeddings (≈ $2)
and 94 k routing plus 75 k write calls (≈ $59) → **≈ $118 per full LME-S run** (≈ $93 with the
batch API for extraction; ≈ $69 at A-F = 4), ≈ 273 k gateway calls → ≈ 7.6 h wall at the 600
RPM cap when every stage shares it, 10–11 h otherwise. LME-M is ≈ 13× the ingestion → **≈ $1 540
and ≈ 2–3 days**, so it runs on release candidates and monthly, under a `$2 000/run` budget guard in
the harness (`--max-cost-usd 2000`). LoCoMo ≈ $0.19. A re-run of the *query* stage is free of
ingestion cost; a re-ingest of an unchanged corpus is a cache-hit test (§8.6 metrics) and costs only
embeddings of nothing (all hashes unchanged).

#### 8.6.1 Postgres and storage baselines (`make bench-pg`)

The round-3 and round-4 reviews (PostgreSQL 16.15, pgvector 0.8.6, 4 vCPU, 200 k facts in one
partition, warm) measured the designs; those numbers are the baselines, and the D22/D23 design
must beat them by the stated margin. The fixtures are built by `engram-bench synth` on the
ParadeDB image and run in M0.6 (storage, plans, IOPS, the selectivity sweep), M0.4 (Temporal)
and M1.5 (move); rows marked *unmeasured* are predictions, not results, and a miss is a §11 item.

| Benchmark | Baseline (measured) | Target | Where |
|---|---|---|---|
| Hide 2 000 facts (retire / invalidate / delete) | 4 872 ms (2.44 ms per fact: a non-HOT update re-inserts the vector into HNSW); 100 k-fact delete ≈ 250 s | one marker row, ≤ 10 ms at any size; zero HNSW work (`TestDelete_O1`) | M1.3 |
| `INSERT` of 5 000 facts, HNSW present / absent | 9 260 ms / 257 ms (1.85 / 0.05 ms per fact); ≈ 13 KB WAL per fact with HNSW | `CommitChunk` of 10 facts + 1 chunk into a ≤ 1 M-element per-namespace index ≈ 2–5 ms HNSW, p95 ≤ 40 ms end to end (*unmeasured*) | M0.6 |
| HNSW vacuum repair after purging 10 % of a partition | 231 s (228 s CPU); 51 s at 0.5 % dead; round 4: autovacuum repairs the per-namespace graph before any rebuild, 5 to 6 times a rebuild at 4 % dead, ≈ 110 KB of WAL per fact | no shard-wide repair: namespace delete = `DROP INDEX` + paced batched `DELETE`; hygiene = index-runner rebuild of one small index at 1 % of `rows_at_build` or 2 k purged elements, ≈ 0.35 ms per vector; `vacuum_index_cleanup = off`; no autovacuum run > 30 min (`TestIndex_Hygiene`) | M0.6, M1.10 |
| Entity trigram lookup as `engram_app` | 308 ms and 2 576 buffers (seq scan of 85 k entities; 6.7 ms as superuser) | ≤ 10 ms index scan through the `SECURITY DEFINER` wrapper (`TestPlans_AsEngramApp`) | M0.2 |
| Semantic arm with a tag, metadata, `fact_type` or `as_of` filter (**selectivity sweep**: tags 100/20/5/2/1 %, `as_of` deciles, namespaces of 2 k, 50 k and 1 M facts) | shared HNSW 67–111 ms, 26–27 k buffers; exact path at 20 k: 41 ms, 8 000 heap pages (64 MB); round 4: a selective filter on a per-namespace HNSW truncates the arm or seq-scans the partition (1.3 GB at 50 k facts) | every arm returns `min(cap, n_visible)` or `partial`; θ (starts at 5 k eligible rows) is the crossover of the exact path and the iterative scan, fixed from the sweep's p95 per arm on the real image; the exact path at 1 % of 50 k reads ≈ 500 rows; per-namespace HNSW visits ≈ `ef_search` rows, ≤ 15 ms warm per arm (`TestRecall_FilteredArm`) | M0.6 |
| Page touches per MID recall and IOPS | round 3 assumed ≈ 1 400 touches and 3.5 k IOPS; measured: ≈ 10 k touches (3 vector arms ≈ 1 250 each, graph arm ≈ 3 700 on the temporal family, BM25 ≈ 200 unmeasured), ≈ 25 k IOPS at 50 QPS and a 5 % miss rate; exact path 2 060 page reads at 2 100 interleaved rows | the N114 table re-derived from the real arm SQL, the exact path and rows for 10 % and 1 % tag filters and an old `as_of`, within the 50 k IOPS budget and an ≈ 80 GB hot set (`TestCapacity_Budgets`); recall-under-ingest CPU is an M0.5 exit | M0.6 |
| Purge WAL | 35 to 82 MB per 1 000 purged facts (≈ 396 to 778 ms per batch), 70 to 160 MB/s unpaced; 100 k-fact document ≈ 3.5 to 8 GB plus ≈ 11 GB of repair vacuum | ≤ 25 MB/s per shard (WAL-paced, two expunges), no repair vacuum; 100 k facts in 2.5 to 5.5 min, 1 M facts in 25 to 55 min; archive sustains ≥ 50 MB/s (`TestExpunge_WALBudget`) | M1.7, M1.10 |
| Index build memory and time | at 32 MB `maintenance_work_mem`: 2 253 B per element, 83.5 s for 25 k inserts past the limit (≈ 3 ms per on-disk insert); 1 GB parallel build held 1.07 GB of `/dev/shm` | `maintenance_work_mem = 2.4 KB × vectors` (≤ 5 GB), `shm_size: 8g`, one build per shard: 100 k ≈ 35 s, 1 M ≈ 5 to 6 min, 2 M ≈ 12 min at 0.28 to 0.37 ms per vector (`TestIndex_BuildMemory`) | M0.6 |
| XID consumption at 50 writes/s | ≈ 90 XIDs/s (50 writes + cursor updates every 100 ms): the 2×10⁸ freeze age arrives every ≈ 26 days | ≤ 55 XIDs/s (cursors batched to 1/s per consumer): ≈ 4.7 M/day, 50 % of `autovacuum_freeze_max_age` (10⁹) in ≈ 107 days (`TestXID_Budget`) | M1.4 |
| Move copy rate and freeze | ≈ 540 facts/s per stream (HNSW insertion bound); round 4: active-namespace moves rolled back (old-id rows missed) and the freeze carried whole-namespace scans, ≈ 2× its target | ≥ 2 000 facts/s per stream (B-tree and BM25 bound; the target has no HNSW until the copy ends), 4 streams (*unmeasured*); verification runs before the freeze; freeze work = rows since `T_pre − 10 min` + mutable rows, < 30 s for ≤ 1 M facts on a namespace with a consolidation backlog (`TestMove_ActiveBacklog`) | M1.5 |
| Degraded-mode recall (N119) | n/a | per-candidate `observation_inputs` lookup ≤ 20 ms per arm for 150 candidates × ≤ 60 rows; three marker selects ≤ 3 ms at ≤ 16 k entries | M1.3 |
| Exclusive fence taker against legal 30 s writers | 5 s attempts, each failure a 5 s write brownout | one 35 s attempt, no retry storm; writers refused immediately (`TestFence_*`) | M1.8 |
| Temporal per cell (P-9, N139) | one untuned `auto-setup` cluster, ≈ 20 events per chunk, nothing measured | split services, 512 history shards, own Postgres; sustains the `RetainBackfill` peak §9 sizes for (580 to 5 800 events/s; ≈ 10 to 100× the ≈ 58 events/s of a 2.9 chunks/s online cell) at < 70 % history CPU and persistence p99 < 50 ms, or §9 states the lower peak the rig reached | M0.4 |

### 8.7 Side-by-side versus a running Hindsight instance

Compose profile `bench-hindsight` runs Hindsight from its published image next to the Engram
stack, pointed at the **same gateway** through its OpenAI-compatible endpoint, with the same
models, the same corpus (through the harness' Hindsight adapter) and the same judge.

Settings (the `docker-compose.bench.yml` excerpt is generated from `bench.lock`; the image
tag is pinned there, A-B5): the LLM, embedding and reranker endpoints are the **gateway's
OpenAI-compatible surface**, the model is `bench.lock models.extract`
(`gpt-oss-120b`), embeddings are `nomic-embed-text-v1.5` with the `search_query: ` /
`search_document: ` prefixes (A-B6: Hindsight's schema takes the dimension, 768, at its first
migration), the reranker is TEI-compatible through the gateway (else Hindsight's local MiniLM,
and the report says so), auto-consolidation is on, temperature 0.1, and Hindsight runs against
its own `pgvector/pgvector:pg16` container under the `bench-hindsight` profile.

Two Hindsight configurations are measured, because "Hindsight" alone is ambiguous:

| Config | Models | Why |
|---|---|---|
| **H-default** | Hindsight's shipped defaults where they are local (bge-small-en-v1.5 embeddings, ms-marco MiniLM reranker), LLM = the same `gpt-oss-120b` via the gateway | what a user gets out of the box |
| **H-matched** | everything through the gateway with `bench.lock` models (nomic 768-d with prefixes, `bge-reranker-base`, `gpt-oss-120b`) | the fair comparison: same models, so differences are pipeline differences |

The harness adapter (`bench/adapters/hindsight.go`) maps the same items to
`POST /v1/default/banks/{bank}/memories/retain` (`timestamp`, `document_id`, `tags`,
`async=true`, polls `/operations`), waits for the consolidation operation to complete, then
answers with (a) `recall` (`budget=mid`, `max_tokens=4096`, `include.chunks`) + the same
`bench/answer/v1` prompt, and (b) `reflect` (`budget=low`). Banks map 1:1 to Engram namespaces.
Hindsight has no `as_of`; since every session precedes its question in both datasets this
does not affect the numbers, and the Engram `as_of`-off ablation is the like-for-like arm.

**Comparison table template** (rows = metrics, one table per dataset; Δ and CI are paired
over questions, 3 repeats each):

| Metric | H-default | H-matched | Engram recall+answer | Engram reflect | Δ (Engram reflect − H-matched) | 95 % CI |
|---|---|---|---|---|---|---|
| LME-S accuracy overall | | | | | | |
| … per category (6) + abstention | | | | | | |
| LoCoMo accuracy overall (cat. 1–4) | | | | | | |
| … per category (4) | | | | | | |
| R@5 / R@10, session level (recall stage) | | | | — | | |
| Context tokens / question | | | | | | |
| Ingest cost / haystack (USD) | | | | same | | |
| Ingest wall time / haystack | | | | same | | |
| Recall p50 / p95 (ms) | | | | — | | |
| Reflect p50 / p95 (s) | | | — | | | |

**Acceptance.** *Parity*: Engram `reflect` within **2.0 points** of H-matched on LME-S overall
accuracy, with the paired-bootstrap 95 % CI of the difference containing 0 or lying above it
(500 questions → binomial SE ≈ 1.6 points at 85 %, so 2 points ≈ 1.25 SE; hence 3 repeats and
paired statistics rather than single-run comparisons). *Changed, to be measured* (N73; these
were labelled "improvement goals" before the review and are now hypotheses with a direction,
confirmed only by the §8.6 ablations and this table): temporal-reasoning **+3 pp** (temporal
arm over `occurred_*` windows + `as_of`), knowledge-update **+2 pp** (observation versions),
R@5 ≥ **0.95** at the fused stage, context tokens per question **−20 %** at equal accuracy
(skip-not-truncate packing + chunk headers), recall p95 **< 300 ms** at mid budget on a
8 M-fact shard against the 216 ms critical-path budget (Hindsight documents 100–600 ms). A
row whose measured delta is negative or inside the CI is reported as such; §1.9's capability
table says "changed" for every one of them until then. For reference, Hindsight's paper reports 83.6 % (GPT-OSS-20B) and
89.0 % (GPT-OSS-120B) on LME-S and 85.67 % (OSS-120B) on LoCoMo; the marketing figure of
94.6 % has an unverified configuration and is not used as a bar.

### 8.8 Cost and latency reporting

**Metering → cost.** Every gateway `Usage` passes through two hooks (§2.2.5): `quota.Meter`
writes `token_usage_events(usage_key, …)` and the rollup `token_usage(namespace_id, day, op, model,
price_version, prompt_tokens, completion_tokens, cost_micros)` in the shard DB (exactly-once by
`usage_key`, N25; upsert per `(namespace_id, day, op, model, price_version)`, in the same
transaction as the activity's commit where one exists, so it moves with the namespace, D13),
and `telemetry` increments `engram_llm_tokens_total{shard, op, model, kind}` and
`engram_llm_cost_micros_total{shard, op, model}` — **without a `tenant` label** (N62: at
10 000 tenants the label was ≈ 800 k series per counter per process; per-tenant figures come
from `token_usage` through `engramctl report` and the optional OpenTelemetry delta-temporality
export). `cost_micros` is computed at write
time from the `CostTable` version in force (`pricing.yaml`, versioned; the version is stored
in `token_usage.price_version`) so historical rows are never re-priced silently.

Cost per conversation is a derived query, not a stored number:

```sql
-- cost of one namespace over a window, by operation (engramctl report cost --namespace …)
SELECT op, model, sum(prompt_tokens) p, sum(completion_tokens) c, sum(cost_micros)/1e6 usd
FROM token_usage WHERE namespace_id = $1 AND day BETWEEN $2 AND $3 GROUP BY 1,2 ORDER BY usd DESC;
```

"Cost per conversation" for a tenant = that sum divided by the number of distinct
`document_id`s retained in the window (a conversation is a document); "cost per 1 k facts" =
that sum / (facts created in the window / 1000). Expected values are **Table 6.8-B**'s
(A-F = 10, A-P3 prices): extraction ≈ $0.066 per 1 k facts, consolidation ≈ $0.079, summaries
≈ $0.010, embeddings ≈ $0.002 → **≈ $0.157 per 1 k facts**; recall ≈ $0.0009 per call (query
embedding + 50 rerank pairs); reflect ≈ $0.114 per typical call (≈ 133 k billed input tokens, 5.1 k output) and ≈ $0.34 worst case. Budget alerts fire on the
`tenant_usage_daily` rollup in the catalog (fed from `token_usage`, D13) against
`quota.llm_tokens_per_day` — not on a Prometheus series, which has no tenant label (N62); a
deferral is the enforcement and the alert is the early warning at 80 %. The rerank-skip rate
(`engram_recall_rerank_skipped_total{reason="deadline"}` / recalls, evaluated over namespaces without pending markers) is an **SLO** in §9.4
(< 1 % at target QPS, N53) and is on the *Recall pipeline* dashboard next to the measured
gateway pairs/s (A-R1: ≥ 80 k pairs/s per cell = 1 600 recall/s × 50; measured in M0.5).

**Latency histograms per stage.** `engram_recall_stage_seconds{shard, stage, budget}`
(buckets 1, 2, 5, 10, 20, 50, 100, 150, 200, 300, 500, 1000, 2000 ms) with `stage` ∈
{authz, catalog, embed_query, arm_semantic, arm_lexical, arm_graph, arm_temporal, arm_chunks,
fuse, rerank, boost_pack, stream}; `engram_retain_activity_seconds{shard, activity}`;
`engram_reflect_iteration_seconds{shard}`; `engram_rpc_duration_seconds{shard, service,
method}`. The `RecallStats` trailer carries the same per-stage numbers per request so a client
can attribute its own latency; the bench harness reads the trailer rather than timing
externally.

**Dashboards** (Grafana JSON checked in under `deploy/grafana/`): *Expunge and visibility* (pending markers by phase, oldest pending age, marker-set sizes, degraded namespaces, purge rate), *Recall pipeline* (p50/p95
per stage stacked, rerank-skip rate, arm candidate counts, results per stage), *Retain
pipeline* (chunks/s per worker, cache hit rate, gateway RPM vs limit, deferred operations,
activity latency), *Cost* (USD per op and per 1 k facts from Prometheus; per tenant per day from
`engramctl report`/the OTel export, N62; forecast to month end), *Bench* (weekly accuracy,
R@5, facts per chunk, re-embeddings per append, tokens/question and cost/haystack over time, one line
per arm, annotations at model/prompt version changes), plus the operational dashboards of
§9.4.

**Weekly report** (`engramctl report weekly`, run by the Friday bench job; markdown +
CSV posted to the team channel and archived under `bench/results/weekly/`): cost per tenant
and per 1 k facts (top 10 tenants, deltas vs last week), recall p50/p95 per stage per shard
(and the SLO burn for the week), retain throughput and gateway rate-limit time, outbox and
move statistics, and the bench deltas (accuracy, R@5, tokens, cost) against the previous
week's lock, with a red banner if a lock field changed. The report is the artifact reviewed in
the weekly ops meeting; a regression > 1 pp accuracy or > 10 % cost per 1 k facts without a
matching change note opens a ticket automatically.

### New decisions introduced by §8

| Id | Decision | Rationale | Rejected |
|---|---|---|---|
| ND-1 | The outbox relay holds its `pg_try_advisory_lock` on a **dedicated direct connection** to the shard's Postgres (`shards[].direct`), not through pgbouncer. *(adopted as N15 in the register)* | Session-level advisory locks do not survive transaction pooling (D2's pgbouncer sidecar); the T5 double-election test needs a real session to partition. | Transaction-level advisory locks (`pg_try_advisory_xact_lock`) held by a long-running transaction — blocks vacuum and the outbox trim. |
| ND-2 | Fault knobs (`fakegw` `GW_*`, mover `--fault-at`, `AllowAllVerifier`) compile only under build tags `faultinject`/`testauth`; release images are built without them and `make lint` fails if a release binary exports the flags. *(adopted as N16 in the register)* | Fault injection must never ship. | Runtime feature flags (one misconfiguration away from production). |
| ND-3 | A forwarded request carries `engram-forward-hops`; a request with `hops ≥ 1` is never forwarded again (max one hop). *(adopted as N17 in the register)* | Bounded routing under a stale cell map; testable. | Loop detection by request id (needs shared state). |
| ND-4 | Bench pins: judge `gpt-oss-120b` via the gateway, temperature 0, prompt `judge/v1`; all pins recorded in `bench.lock` including the gateway-reported model version; namespace layout one-per-question (LME) / one-per-conversation (LoCoMo) under tenant `bench`. *(adopted as N18 in the register)* | Comparability with Hindsight's paper (same judge model and temperature); drift detection. | GPT-4o-class closed judge (version drift, cost). |
| ND-5 | Two Hindsight configurations (H-default, H-matched) are measured; parity is judged against H-matched. *(adopted as N18 (merged with ND-4) in the register)* | "Same models" is the only fair comparison; H-default answers the user's question. | A single Hindsight run with defaults. |
| ND-6 | Every store query is registered in `store.Queries` so the RLS canary is exhaustive; `engramlint sql` enforces explicit `namespace_id` predicates in addition to RLS. *(adopted as N19 in the register)* | Exhaustiveness is what makes the canary a proof, not a sample. | Sampling a few queries. |
| ND-7 | `token_usage` rows carry `price_version`; cost is computed at write time and never re-priced. *(adopted as N20 in the register)* | Historical cost reports must be reproducible. | Pricing at report time. |

### Round-4 changes

| Area | Removed | Added |
|---|---|---|
| Regression coverage | tests asserting intent before the marker, name-order replay, `created_at` re-copy, 50 ms purge pauses, 5 % dead-fraction rebuilds, the 1 400-touch IOPS row | one scenario per finding (8.4.7); `TestIntent_DuplicateAttempt`, `TestRestore_ChainOrder`, `_NoUnackedEffect`, `_FloorInCatalog`, `TestMove_ActiveBacklog`, `_FailoverBetweenCAndD`, `TestBlob_OwnerKeyed`, `TestPageRefresh_DeleteMidCall`, `TestDerivation_CommitRule`, `TestReextract_Rebuilds`, `TestConsolidation_StaleProposalDiscarded` and siblings, `TestDocument_TombstoneView`, `TestExport_HiddenOverlay`, `TestRecall_FilteredArm`, `TestIndex_RunnerLifecycle`, `_Hygiene`, `_BuildMemory`, `TestExpunge_WALBudget`, `TestMarkers_BoundedByLag`, `TestCapacity_Budgets` |
| Baselines (8.6.1) | the 1 400-touch and 10 k IOPS rows, the 1 000 events/s Temporal gate | measured touches and IOPS, the selectivity sweep, purge WAL, build memory, freeze work, the backfill-peak Temporal gate |
| Model checking (8.4.6) | the D22 spec table | one table per spec with must-pass and must-fail configurations, logs in `formal/tla/results/` |
