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
| Mover `--fault-at=<step>` | `internal/move` (`faultinject` tag) | panics after the first committed write of the named step; knobs `--recopy-margin=0`, `--lineage-less-segments` |
| `FakeIntentStore` | `internal/intent` | `FailPuts`, `Latency`; strongly consistent list for replay tests |

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
| `internal/recall` (planner, visibility) | arm caps 50/150/400 by budget; `stage=FUSED` when remaining deadline < 150 ms; stream batches of 10; `RecallStats` trailer; marker sets loaded once per request (three indexed selects) and passed as array parameters | **Cap adherence** over `MemIndex`: each arm returns ≤ cap. **`as_of` inside every arm**: for random corpora and `T`, no arm returns `mentioned_at > T`, and each arm returns `min(cap, |visible|)` rows — the filter is not post-hoc. **Visibility (N116)**: for random corpora and random marker sets (`DocTomb`, `ChunkTomb`, `FactHidden`), no arm returns a fact with `document_id ∈ DocTomb ∨ chunk_id ∈ ChunkTomb ∨ memory_id ∈ FactHidden`, and the array-parameter path and the SQL anti-join path above 16 k entries return the same set. **Type/tag filters** commute with fusion, and the tag filter resolves once to `$allowed_docs` (above 8 k documents the arm joins `documents`). | `Derivation.tla` (`NoDeletedDerivationServed`, `AsOfNoLeak`) |
| `internal/entity` | trigram thresholds (0.3 candidate, 0.6 accept, 0.85 when a type is unknown; §5.1.2) on a seeded set; names > 256 chars dropped; whitespace normalisation | **Idempotence**: `Resolve(B); Resolve(B)` creates no new entity on the second call. **Order insensitivity**: the *set* of `(mention → canonical entity)` assignments is independent of the order of mentions within a batch. **Monotone growth**: `Resolve(B ∪ B')` then `Resolve(B')` creates no new entity. **Namespace locality**: every returned `entity_id` has `namespace_id == scope.namespace`. | — |
| `internal/link` | weights (`max(0.3, 1 − Δh/24)`, cosine ≥ 0.75, causal 1.0); lock-order sort key | **Caps**: temporal links per fact ≤ 20; semantic ≤ 10 per fact and each with cosine ≥ 0.75; entity ≤ 20; ≤ 60 in all (§5.1.2); causal only to an earlier fact of the same batch. **No self-links**, **no cross-namespace endpoints**, **no orphan endpoint** (both facts exist at insert; at traversal both endpoints are visible). **Determinism** of the link set for a fixed batch. | `Derivation.tla` (`NoDeletedDerivationServed`) |
| `internal/outbox` | batch of 500; cursor persistence; 7-day trim honours every cursor; `Event` proto round-trip; cursor advances batched to one transaction per second per consumer (N114); `engramlint sql` rejects a builder with a statement after `OutboxRepo.Append` (A-F1) | **Gap watchlist** (model: random interleavings of `seq` allocation, commit and abort with `statement_timeout = idle_in_transaction_session_timeout = 30 s` and the outbox `INSERT` last, A-F1): every committed `seq` is delivered **exactly once** per consumer; delivery is a **strict prefix** in ascending `seq` per consumer (no cursor passes an open gap); a `seq` that commits within `2 × statement_timeout` of being skipped is never declared aborted; an aborted `seq` is declared within that bound and never blocks later ones for longer; `TestOutbox_Watch1x` reproduces the loss with a 1× horizon. | `Outbox.tla` (`NoLossSafety`, `PerNamespaceOrder`, `OnlyCommittedDelivered`; `Outbox_Watch1x.cfg`) |
| `internal/catalog` | LRU 100 k; TTL 60 s; negative 5 s; stale existing entries served indefinitely with an age gauge, negative entries ≤ 10 min (D4 as amended); `LISTEN` payload parsing; full flush on reconnect | **Invalidation model** (ops: `resolve`, `notify(ns)`, `expire`, `catalog_down`, `reconnect`): after `notify(ns)` and while the catalog is reachable, the next `resolve(ns)` returns the new `(shard, epoch)`; while unreachable, an existing entry is served **for as long as the outage lasts** (`UNAVAILABLE` only on a miss) and a negative entry is never served > 5 s reachable / > 10 min unreachable. **CAS**: `MemoryCatalog` and the Postgres catalog give identical results for a random op sequence (T3 twin). | `ShardMove.tla` (stale-cache steps) |
| `internal/authz` | `(claims, method, entry) → code` table; JWKS refresh; expired token; `kid` rotation | **Allowlist**: `ns = ["*"]` admits every namespace of the same tenant and none of another; a token never yields a `RequestScope` whose `tenant != claims.tenant`. **Scope monotonicity**: adding a scope never turns an allowed call into a denied one. | — |
| `internal/api` | `DeadlineGuard` (missing → `INVALID_ARGUMENT`, clamped); `request_id` reuse with a different hash → `ALREADY_EXISTS/OperationConflict{IDEMPOTENCY_KEY_REUSED}` (D1); opaque page tokens (HMAC, tamper → `INVALID_ARGUMENT`); field masks | **Idempotency**: replaying any unary write with the same `request_id` within 24 h returns the stored response and performs no second effect (`FakeTx` effect counter). **Pagination**: walking all pages yields each row exactly once for random inserts between pages (ids are UUIDv7 → stable order). | — |
| `internal/quota` | token bucket refill; `RESOURCE_EXHAUSTED` + `QuotaExceeded` detail (D13); day window reset at UTC midnight | **Bucket**: at most `rate` admissions per window for any arrival pattern. **Deferral**: `llm_tokens_per_day` exhaustion never fails an operation; it is `DEFERRED` and resumes exactly once at the window reset; `quota.Reserve` precedes every gateway call class (N130). | — |
| `internal/store` | SQL builders golden (every query text under `testdata/sql/`), `SET LOCAL` prelude, the write-mode fence prelude (`pg_try_advisory_xact_lock_shared(hashtextextended(ns, 0))` — failure ends the statement with `NamespaceFrozen{retry_after = 200 ms}`, N82 — then a plain `SELECT state, epoch FROM namespace_ownership`, no row lock), the per-document try-lock (two-argument form, `DocumentBusy`, N83) + `status = 'ingesting'` prelude of `CommitChunk` (N40) with no `FOR SHARE`; **no `UPDATE`/`DELETE` text on a content table outside the expunge role** (`engramlint sql`, N113) | **Predicate presence**: every builder output for a namespace-scoped table contains `namespace_id = $n` (also enforced statically, §8.3). **Fencing state machine** in `FakeTx`: a write with `state ≠ active` or the wrong epoch fails with `WrongShardOrEpoch`/`NamespaceFrozen`; a read succeeds only for `active` and `frozen/move` (N122, N125). **Version-row state machine**: a `CommitChunk` against a version whose status is not `ingesting` writes nothing, for any interleaving with `FinalizeVersion`/delete; a commit that wins the race against a tombstone writes rows that no surface returns. | `ShardMove.tla` (`SingleWriter`); `Derivation.tla` (`NoDeletedDerivationServed`) |
| `internal/extract` | schema validation of `ExtractedFact`; degenerate-fact filter; causal `target_index < i`; cache key = `sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)` | **Cache key sensitivity**: changing any one of the four inputs changes the key; **prompt version pinned**: the golden for `extract/v1` fails if the template changes without a version bump. | — |
| `internal/consolidate` | placements/merges/drop_sources schema and application; write call `write`/`retire`; proposal store (write-once, `ON CONFLICT DO NOTHING` returns the stored list); `op_key` recording; 8 facts/call; ≤ 100/round | **Bisect**: for any failure oracle, every fact lands in exactly one successful leaf batch or is stamped failed; routing calls ≤ 2n − 1 plus one write per touched observation; a fact never appears in two successful batches. **Idempotent effect**: applying a batch twice (same `batch_key`) yields one effect per `op_key`, *even when the second attempt's LLM answer differs* — the stored proposal wins (`TestConsolidation_PersistedProposal`, N43); keys and effects are never observed apart (`TestConsolidation_AtomicKey`). **Two-stage isolation (N121)**: for any candidate set, `inputs(O, v)` ⊆ O's own sources ∪ attached facts, and a `merge` is a root rebuild whose rendered set holds no previous text. **Apply re-verification**: a rendered source made hidden between propose and apply discards the proposal (`TestConsolidation_ApplyReverifiesInputs`). **Effective time**: `effective_at == max(max(mentioned_at) over every fact rendered, effective_at of the previous version)` (D9), never `now()`. | `Consolidation.tla` (`ExactlyOnceEffect`, `_VolatileProposal.cfg`, `_NonAtomicKey.cfg`); `Derivation.tla` (`NoOverHiding`) |
| `internal/reflect` | iteration cap 10; 100 k tokens; 300 s wall; per-tool 10 s; JSON-schema validation | **Citation filter**: for any set `S` of ids returned by tools and any candidate citation set `C`, `cited ⊆ S`. **Caps** hold for any tool-result sizes (tool results are truncated to the shared ceiling, never the caps exceeded). | — |
| `internal/move` | state transitions table; `ready` and rollback edges; `T_copy` capture; reconcile planner (which tables are immutable, which mutable); column-list hash; watchdog arithmetic | **Transition legality**: a random op sequence never produces an edge outside D5's graph; a rollback action is enabled from every state before (c) and restores the source `active` (`TestMove_RollbackEveryStep`); after (c) none is. **Reconcile correctness over `FakeTx`**: for random interleavings of inserts (any writer lifetime ≤ 60 s) with the dirty copy, the immutable-table re-copy of rows with `created_at ≥ T_copy − 10 min` makes the target equal the source at freeze, and the mutable-table merge-diff makes target = source (deleting target rows absent on the source); with margin 0 a straddling row is lost (the counterexample). **No two writable owners** at any intermediate state of Freeze, (b′), (c), (b″), (d); `ready` serves nothing. | `ShardMove.tla` (`SingleWriter`, `NoLossNoDup`, `RollbackPossibleBeforeC`, `NoRouteToTargetBeforeC`) |
| `internal/export` | manifest schema; 1 MiB parts; zstd round-trip; delete records and `deleted_ids` in the manifest | **Delta correctness**: `apply(snapshot v_{n−1}, delta) == snapshot v_n` for random snapshot pairs (deltas are diffs of consecutive snapshots, N126), including when the base snapshot expired; a snapshot built across a delete is never promoted. | — |
| `internal/expunge` | phase planner; batch size 1,000 with 50 ms pauses; cursor-pass check; dirty-fraction trigger (5 %) | **Idempotence**: any prefix of a purge batch repeated deletes nothing more and a crashed phase resumes at its marker state (`pending → materialized → purged`). **Order**: no purge before every registered cursor passed the delete's `seq`; no physical delete of a row the visibility predicate still returns. **Monotone hiding**: `derived_hidden` rows for document causes are never deleted. | `Derivation.tla` (`MaterializeComplete`) |
| `internal/intent` | key format `_control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json`; request-hash check; 35-day retention | **Ack implies intent**: no marker transaction commits without a durable intent (`FakeIntentStore`). **Replay order**: intents applied in name order, last state per subject wins (`Invalidate; Restore` leaves the fact visible); replay is idempotent (the intent name is the key). | `Durability.tla` (`AckImpliesIntent`, `IntentOrderLastWins`) |
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
| Background workers / activities (`RetainDocument`, `Consolidate`, `PurgeDocument`, `PageRefresh`) | `TestIso_Workflow_CrossTenant`: a workflow input `(ns=A1, tenant=zeta, shard=1, epoch=1)` fails its first activity with `WrongShardOrEpoch` (non-retryable); zero rows written | `TestIso_Workflow_CrossNamespace`: `Consolidate(A1)` with `A2` facts planted as the nearest semantic neighbours never cites an `A2` id (RLS in the activity transaction); the extraction cache for an `A2` chunk with the same content hash is a **miss** (second LLM call observed in `DeterministicClient`'s counter) — D11 per-namespace cache | `TestIso_Workflow_CrossShard`: input `(ns=A1, shard=2)` → `WrongShardOrEpoch` at the first activity, no rows on either shard; task queue `shard-2` never receives an `A1` workflow (Temporal `ListWorkflow` by search attribute `namespace_id`) |
| Outbox relay | `TestIso_Relay_TenantMix`: events of `Z1` and `A1` (same shard) are delivered to the `index` consumer with their own `namespace_id`; the Kafka sink (when on) keys by `namespace_id`; no consumer callback ever receives a payload whose `namespace_id ≠ event.namespace_id` (decoded and compared) | `TestIso_Relay_Interleave`: `Z1` rows interleave `A1` rows in `seq`; every consumer sees events in `seq` order and delivery stays a strict prefix per consumer | `TestIso_Relay_CrossShard`: the relay built from `ShardHandle(1)` never opens a connection to shard 2 (`RecordingPool`); shard 2's outbox `seq` space is disjoint by construction and its rows never appear in shard 1 cursors |
| Move (copy and reconcile) | `TestIso_Move_TenantMix`: during BulkCopy and Reconcile of `A1`, `Z1` rows on the source are neither copied nor diffed (target `Z1` count stays 0; every copy and reconcile statement carries `namespace_id = A1`) | `TestIso_Move_NamespaceOnly`: the copy and the reconcile run as `engram_move` under RLS with `engram.namespace_id = A1`; a planted `A2` row in the stream is rejected on the target and counted (`engram_move_rejected_rows_total`) | `TestIso_Move_Epoch`: after (c) a source write at epoch `e` → `WrongShardOrEpoch`; a `ready` target serves no read or write (`NamespaceNotReady`); a rollback restores the permanent `moved_out` fence value where one existed |
| Metrics / logs | `TestIso_Metrics_Labels`: scrape `/metrics`; every series has `shard`; **no series has `tenant` or `namespace`** (N62 — per-tenant metering is served from `token_usage` by `engramctl report` and the optional OpenTelemetry delta export; also the static linter) | `TestIso_Logs_NoContent`: 10 k requests with sentinel content; the JSON log stream contains `namespace_id` fields but never any sentinel string, query text or fact text at `info` | `TestIso_Traces`: span attributes carry `engram.shard = shard(ns)` only; no span of a request for `A1` has `engram.shard = 2` |
| Ownership state machine (N64; review F-23) | `TestIso_Ownership_Transitions` (T3, before any move code exists): every transition of the §3.3.1 states × roles × statements table — `incoming → ready → active`, `active → frozen{move|delete|restore}`, `frozen → active` (`thaw_move`, re-enable), `frozen → moved_out`, `ready → incoming` (`unready_target`), `incoming → moved_out` (`return_abort`), `reconcile_out`, the epoch bump, and every *forbidden* edge — is executed against the real DDL as the role the table names (`engram_app`, `engram_move`, `engram_admin`); the `CHECK (state <> 'frozen' OR freeze_reason IS NOT NULL)` and the role grants are what fail the forbidden edges; the read fence accepts `active` and `frozen/move` only (N122, N125) and the write fence rejects every other state | cross-namespace: a transition for `A1` never touches `A2`'s row (`RecordingPool` statement capture) | cross-shard: the catalog's `restoring` state and the shard's `frozen/restore` row are checked together |
| Temporal histories and payloads | `TestIso_Temporal_PayloadsEncrypted` (N59): retain `A1` and `Z1` with sentinels; read every workflow history of task queue `shard-1` through the SDK client and `temporal workflow show`; no sentinel appears in any payload (results > 4 KiB are blob keys, the rest is AES-GCM through the `DataConverter` codec with `encoding: binary/encrypted` metadata); a worker built without the shard's codec key fails to decode and never runs the activity | `TestIso_Temporal_KeysOnly`: `CommitChunkInput` carries only blob keys; the blob keys are under `{1}/acme/A1/` (`blob.Scoped` rejects any other prefix) | `TestIso_Temporal_QueueScope`: a `shard-1` worker never reads a `{2}/…` key |
| Delete markers and export | `TestIso_Tombstones_NamespaceScoped` and `TestDelete_ExpiresSnapshots` (N59, N126): a tombstone, `fact_hidden` or `derived_hidden` row for `A1/d` hides nothing in `A2` even with the same `document_id` (marker sets are loaded per namespace); create snapshot `v1` of `A1`, delete one document, then `StreamSnapshot(v1)` → `FAILED_PRECONDITION/SnapshotExpired{reason: DOCUMENT_DELETED}` and `ListSnapshots` shows `state = expired` (the marker transaction expires `building` and `ready` rows alike); `CreateSnapshot` produces `v2` without the document | cross-namespace: deleting an `A2` document expires no `A1` snapshot | cross-shard: **NS** |

Two rows apply to every surface at once: `TestIso_Deleting_AllSurfaces` — a namespace in
catalog state `deleting` (shard state `frozen/delete`) returns **FP** (`PreconditionFailed{NAMESPACE_DELETING}`) on every method of every surface (N5, N122), and the
`FakeActivities` for its purge workflow still run (the only writer allowed); and
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

This section holds every regression test of the D22 redesign. Subsections 8.4.1 to 8.4.4 are
the tests of the four mechanisms (markers and visibility, storage and plans, durability, moves),
8.4.5 is the fault matrix for everything else, and 8.4.6 pairs each model-checked property with
its Go twin. Tier T5 rows run against the T4 stack; the others are T1 to T3. Every row also
asserts the §7 safety invariants through the SQL checks of 8.4.3.

#### 8.4.1 Tombstones, visibility, Restore and Expunge

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestDelete_O1` (N115) | documents of 1 k, 10 k, 100 k and 1 M facts, deleted under retain load | the ack path (`put` intent, then one marker transaction: document lock, `documents.state = 'deleting'`, one `document_tombstones` row, `deletion_log`, one outbox event) is O(1): ack p95 ≤ 100 ms at every size, slope of ack time against size ≈ 0; the transaction writes exactly 4 rows; no content row is touched | T3 + T5 |
| `TestContent_InsertOnly` (N113) | the whole T3 suite and the retain, consolidation and expunge workflows run with a test-only trigger that raises on `UPDATE` of `facts`, `chunks`, `observation_versions`, `page_versions`, `fact_links`, `entity_mentions`, `observation_inputs`, `observation_version_sources`, and on `DELETE` outside the `engram_expunge` role | suite green; `engramlint sql` rejects any builder with `UPDATE` on those tables; no table has `retired_at`, `invalidated_at`, `live`, `stale_*`, `superseded_at`, `tags` or a generated column (`attgenerated = ''`), `fillfactor = 100` | T3 + S |
| `TestVisibility_AllSurfaces` (N116) | table-driven: document delete, `REPLACE` (chunk tombstone) and `Invalidate` (`fact_hidden`), each followed immediately by every read surface: all five Recall arms at `FUSED`/`RERANKED`/`PACKED`, Reflect tools (`search_memories`, `search_observations`, `get_page`, `expand_fact`), `GetMemory`, `ListMemories`, `GetPage`, `SearchPages`, `StreamSnapshot`, MCP tools, and the async-index join with the `index` consumer paused | from the ack, no fact, chunk, observation version, page version, entity alias or export part derived from the subject is returned by any surface (sentinel absence); `GetMemory` and `GetPage` answer `NOT_FOUND` / `PreconditionFailed{PAGE_HIDDEN}`; marker sets are read once per request (three selects, `RecordingPool`); above 16 k entries the arm switches to the SQL anti-join and returns the identical set (property) | T3 |
| `TestVisibility_SegmentHiding` (N117) | observation `o`: v1 create (root), v2 update, v3 root rebuild, v4 update, with inputs `f1`, `f2`, `f3`, `f4`; delete `f2`'s document | exactly v2 is hidden (segment of v2 = inputs of v1..v2 names `f2`; v1 = {f1}; v3 and v4 have `root_version` 3); the current v4 stays visible (`NoOverHiding`); a version that commits **after** the marker and names `f2` in its segment is hidden by the read-time predicate (race test with the writer paused between re-verification and commit); a configuration without the derivation lock lets Materialize miss it (the counterexample, 8.4.6) | T3 |
| `TestVisibility_Pages` (N117) | page `p` v1 root (fact and observation-version inputs), v2 delta; delete a document behind a fact input, then one behind an observation input | the page version is hidden through the fact input or through a hidden `(O, w)` input (two `EXISTS`, no walk); a hidden current page returns `PreconditionFailed{PAGE_HIDDEN}` until `PageRefresh` lands a root rebuild (`page_full/v1`, no previous text); pages never appear as an input of an observation | T3 |
| `TestInvalidate_RestoreExact` (N115) | random sequences of `Invalidate(f)`, `Restore(f)`, document delete and `REPLACE` over a fixture (rapid) | the hidden set after `Invalidate; Restore` equals the set before (exactness): `Restore` deletes the `fact_hidden` row and the `derived_hidden` rows with `cause = ('invalidation', f)` and nothing else; a restored fact of a tombstoned document stays hidden; `curation_log` re-applies a curation to the re-extracted twin of the same `content_hash` at `CommitChunk` | T1 + T3 |
| `TestRetain_ReplaceTombstones` (N115, N58) | `REPLACE` that drops a chunk, flaps it back, and a prompt bump (`reextract`) | `FinalizeVersion` inserts `chunk_tombstones(reason = 'replace')` for chunks missing from the new membership (one statement), a flap is `DELETE FROM chunk_tombstones`, re-extraction hides old facts with `fact_hidden(reason = 'reextract')`; Recall never returns a v1 and a v2 fact for the same chunk; zero `UPDATE` statements | T3 |
| `TestExpunge_Stages` (N119) | delete a 100 k-fact document with observations, pages, an export and an `ids_elided` consumer lagging | (1) Materialize inserts `derived_hidden` rows with `from_version = min(version)` of the victim inputs, marks observations and pages `stale_delete`, nudges root rebuilds and `PageRefresh`, sets `expunge_state = 'materialized'`; (2) Purge deletes in batches of 1,000 with 50 ms pauses (facts, vectors, links, mentions, evidence, chunks, versions, ledger rows, then blob tombstones) **only after every registered index and Kafka cursor passed the delete's `seq`** (`ids_elided` victims by indexed `(namespace_id, document_id)`); (3) `REINDEX INDEX CONCURRENTLY` on a touched partition index above 5 % dead; (4) `purged`, tombstone deleted after 24 h, operation `SUCCEEDED`; `derived_hidden` rows for document causes are never deleted | T3 + T2 |
| `TestExpunge_SLAs` (N119) | the same fixture at 1/100 scale on the T4 stack, then the projected figures for 1 M facts from measured rates | materialize ≤ 15 min, purge ≤ 24 h, index rebuilt ≤ 48 h (projected at the measured rates, which M1.10 records); the `ExpungeMaterializeSlow` alert fires when a marker stays `pending` > 15 min | T4 + T5 |
| `TestExpunge_DegradedMode` (N119) | a namespace with `pending` markers, 150 candidates × ≤ 60 input rows per observation arm | observation and page arms add the per-candidate `observation_inputs` lookup (index-only plan; ≤ 20 ms per arm); `Consolidate` runs only root rebuilds; the rerank-skip SLO is suspended for the namespace (metric labelled, not alerted); after `materialized` the lookup is skipped (only `pending` tombstones are passed in `$doc_tomb`) and latency returns to the baseline within one request | T3 + T5 |
| `TestExpunge_DerivationLock` (N120) | a writer (`ApplyBatch` stage 2 or `PageRefresh`) whose re-verification predates the marker holds the shared lock across its commit; Materialize starts meanwhile | Materialize waits (exclusive lock, one 35 s attempt), then sees the new version and writes its `derived_hidden` row; a legal 30 s writer never starves it; writers refused by a pending Materialize retry (`pg_try_advisory_xact_lock_shared`); markers themselves take no lock; the key spaces of fence (`…, 0`), derivation (`…, 1`) and document locks (two-argument) never collide (`pg_locks` check) | T3 |
| `TestExpunge_FencedAndPaused` | `start_move` while an expunge is mid-purge; epoch bump during Materialize | every activity is fenced `active` at the epoch and skipped while a move is open; the expunge resumes after the move is `done` or rolled back; large-table purges never run between Plan and done | T3 + T5 |
| `TestDelete_NamespaceAndTenant` (N122) | `DeleteNamespace`, `DeleteTenant` under load | `freeze_delete` precedes the ack; `frozen/delete` rejects reads and writes (`PreconditionFailed{NAMESPACE_DELETING}`); purge is `DROP INDEX` plus batched `DELETE` plus the blob prefix, with no HNSW repair; a tenant-scoped admin `GetOperation` answers during the delete | T3 + T5 |
| `TestExport_ExpiresOnMarker` (N126) | `BeginSnapshot` (`building`), `RecordSnapshot` race, delete, `StreamSnapshot` | the marker transaction expires `building` and `ready` rows alike; `RecordSnapshot` refuses to promote an expired row and re-checks `deleted_at > snapshot_started_at`; a delta `delta-v{n−1}-v{n}` is emitted with delete records and `deleted_ids` even when the base expired; `engram-sync` applies deletes first and refuses an expired local copy | T3 + T4 |
| `TestEntities_DeleteAndAsOfSafe` (N118) | an alias merge whose evidence document is deleted; `as_of` before the merge | under `as_of`, `EntityRef` carries only `mention`, `canonical_name` and alias merges are suppressed and graph hops use `entity_mentions.mentioned_at ≤ T`; the expunge deletes the victim's aliases and recomputes `canonical_name` from the remaining mentions; both cases join the 8.5 leak canary | T3 |

#### 8.4.2 Durability: delete intents, restore and failover

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestIntent_AckImpliesIntent` (N122) | kill the API process (a) after the intent `put` and before the marker transaction, (b) after the marker transaction and before the ack; blob store down for (c) | (a) the client saw a transport error, the delete may later take effect through replay (documented); (b) the client may retry, the intent name (`{deleted_at}-{operation_id}`) is the idempotency key; (c) `UNAVAILABLE`, **no marker is written without an intent** | T3 + T5 |
| `TestRestore_ReplaysIntents` (N122, N123) | base backup; then document deletes, an `Invalidate` followed by `Restore` of the same fact, a namespace delete and a retain; restore to a point 5 min before the last delete | the restored shard comes up `frozen/restore` with restricted `listen_addresses`; `engramctl restore replay` lists every hosted namespace's intents with `deleted_at ≥ restore_point − 10 min` and applies them in name order through the admin variant of the marker transaction, last state per subject wins (the fact is visible again after `Restore`); only then `restore_done`; the read fence rejects `frozen/delete` and `frozen/restore` throughout; every acknowledged delete is invisible after the replay | T3 + T5 |
| `TestFailover_ReplaysIntents` (N122, N123) | `ha` profile: an asynchronous standby lagging 5 s; 20 retains in flight; a delete acknowledged 1 s before the kill; both variants (old primary stopped, old primary paused and returning) | the promoted shard follows the restore path (open moves reconciled against the catalog first, epoch = catalog epoch + 1 written to the catalog first, `frozen/restore`, intent replay); the acknowledged delete is invisible (RPO 0); retains lose ≤ 60 s; no acked retain ends `FAILED` (in-flight workflows restart at e + 1 from `operations`); the returning old primary fails `WrongShardOrEpoch` on its first write | T5 (nightly, 8 min) |
| `TestDurability_NoSyncWait` (N122) | pause or kill the standby; run a 32-writer load with deletes | every role has `synchronous_commit = local` (`pg_db_role_setting` check in `engramctl config lint`); commits never hang; the relay's 60 s gap horizon holds (`TestOutbox_Watch1x` unchanged); no `UNAVAILABLE` path exists in the delete code | T3 + T5 |
| `TestIntent_Retention` | 36-day-old intents | objects are kept 35 days (more than the 28-day backup window) and trimmed by `engramctl intent trim`; replay finds every intent inside the window | T3 |
| `TestRestore_OpenMoves` (N123) | restore a source shard (a) after a move reached (c), (b) before it | (a) the restored source row becomes `moved_out(target, e + 1)` (`reconcile_out`); (b) the move is rolled back from the target and the row thawed; only then `frozen/restore` and replay; PITR to a point before a move-in is an ordinary move whose source is a scratch instance restored from the old source's backup | T5 |

#### 8.4.3 Move: dirty copy, reconcile, `ready`, rollback at every step

The mover is killed after the first committed write of each step, the load generator keeps
running against `A1` (retain 20/s, delete 2/s, consolidation), and `engramctl move resume` is
issued after 10 s. The invariants are the rewritten `ShardMove.tla` properties as SQL over
source, target and catalog, sampled every 500 ms.

| Killed in | Client-visible during the fault | After `resume` or rollback | Invariant checks (every sample) |
|---|---|---|---|
| Plan (target `incoming`, `start_move` set `move_epoch`, source `system_identifier` and `timeline_id` recorded) | none; the expunge and the schedulers are paused for `A1` | resumes into BulkCopy, or `abort_move` deletes the `incoming` row | **SingleWriter**: `count(ownership WHERE ns = A1 AND state = 'active') = 1` across shards; catalog epoch unchanged |
| BulkCopy (mid range of `facts`) | none | resumes at `(table, last_key)`; ranges are `READ COMMITTED` ≤ 100 k rows loaded through a `TEMP` table under RLS (N91); the target has no HNSW for `A1`; or `abort_move` and delete target rows and blobs | target has no `active` row; source counts unchanged; blob prefix absent or a subset never referenced by a target row |
| Freeze (exclusive fence, one 35 s attempt) | writes `NamespaceFrozen`; reads continue | resume reconciles, or `thaw_move` then the BulkCopy rollback | at no sample do two shards hold `active`; **NoLoss**: every retain acked before the freeze is on whichever shard ends `active` |
| Reconcile (mid immutable re-copy, mid mutable merge-diff, mid blob check) | as above | resumes (idempotent: `ON CONFLICT` upserts and deletes of target rows absent on the source); a residual mismatch or a missing blob → `Rollback` | **NoLossNoDup**: `count(*)` and (≤ 2 M rows) `bit_xor(hashtextextended(pk::text, 0))` equal per immutable table; `(pk, md5(row minus updated_at))` equal per mutable table |
| (b′) target `incoming → ready` | callers on the target get retryable `NamespaceNotReady`; the source still serves reads | `unready_target` then `thaw_move` (rollback), or continue to (c) | **NoRouteToTargetBeforeC**: no read or write was served by the target |
| (c) source `frozen/move → moved_out` with the target hint (**point of no return**) | no owner exists until (b″); reads hit `WrongShardOrEpoch{MOVED_OUT}` and re-resolve in a bounded loop (≤ 5 s) | resume completes (b″) `ready → active` (sub-second, retried forever), then (d); no rollback after (c) | at no sample two writable owners; the gap between (c) and (b″) ≤ 1 s unless the mover is dead, which counts against the availability SLI and raises `CutoverInProgress` |
| (d) catalog flip (retried forever), Restart | brief `WrongShardOrEpoch` on stale caches, one re-resolve | `Restart` starts the recorded `operation_id`s on `shard-{target}` at e + 1 | every operation acked before the freeze reaches `SUCCEEDED` on the target; consolidation `op_key` unique; **ReadsFresh**: no read for `A1` was served by a shard other than `shard(A1)` at the catalog's epoch, except `frozen/move` source reads |
| Cleanup (after 24 h) | none | resume finishes `DROP INDEX`, batched `DELETE`, blob prefix; the `moved_out` row stays | target counts unchanged; `Z1` on the source untouched |

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestMove_DirtyCopyReconcile` (N124) | writers (retain, delete, consolidation) run through BulkCopy; a writer that drew its ids before `T_copy` and commits 45 s later | immutable tables: rows with `created_at ≥ T_copy − 10 min` are re-copied (the margin exceeds the 60 s writer lifetime), so no row is lost; with the margin forced to 0 (`faultinject`) the straddling row is missing, the count check fails and the move rolls back (the counterexample); mutable tables: target rows absent on the source are deleted; blobs: a missing referenced key refuses cutover | T3 |
| `TestMove_ExpungeStaysPaused` (N124) | start a move on a namespace with `pending` markers | no purge runs from Plan to done; after done the expunge resumes on the target at the new epoch; a marker created during the move is copied by the mutable merge-diff | T3 |
| `TestMove_RollbackEveryStep` (N125) | fault at every step before (c) | rollback leaves the source `active` and the target rows, indexes and blobs gone within 60 s; after (b′) `unready_target` runs first; onto a shard that had a `moved_out` row, `return_abort` restores the permanent fence value; `outbox_cursors` is never touched by a move | T3 + T5 |
| `TestMove_ReadyState` (N125) | state-machine test against the real DDL | `incoming → ready → active` edges (`ready_target`, `activate_target`, `unready_target`) run as the role the transition table names; `ready` accepts nothing and surfaces `NamespaceNotReady`; schedulers keep `state = 'active'` as their predicate | T3 |
| `TestMove_ZombieFenced` (N123) | failover during BulkCopy and during Freeze | the mover and relay compare their session's `(system_identifier, timeline_id)` with the catalog at Freeze, at (c) and every 10 s; a source session on a stale timeline fails `MoveFenced` and can be neither frozen nor cut over | T3 + T5 |
| `TestMove_PlanRefusals` (P-8, N22) | schema-version mismatch; a column added on one side; a generated column | `Plan` refuses unless `shard_meta.schema_version` and the per-table column-list hash (`attgenerated = ''`, `NOT attisdropped`, ordered by `attname`) match; `migrate --shard` refuses during a non-terminal move | T3 |
| `TestMove_VerifyFitsFreeze` (N124) | 100 k-fact and 1 M-fact namespaces | freeze < 30 s at 100 k facts, measured and recorded (not gated) at 1 M; the watchdog is `max(120 s, 60 s + 1 s per 10 k facts)`, ≤ 15 min, armed until (c); a seeded one-row mutation is detected | T3 + T5 |
| `TestMove_CutoverOrder`, `TestMove_DrainRestartReconcile`, `TestMove_MoveBack` (N93, N97) | an API client with a muted `LISTEN`; a workflow started 0.5 s before freeze; move `A1` 1 → 2 → 1 | reads are served by `shard(A1)` at the catalog epoch or fail `WrongShardOrEpoch` (the catalog-first order fails this); `Drain` reads `operations` (never `ListWorkflow`) and `Restart` uses `TERMINATE_IF_RUNNING` and rejects `AlreadyStarted` unless queue and memo epoch match; `moved_out` is never deleted by cleanup and `moved_out → incoming` needs a strictly greater epoch and no data rows | T3 + T5 |
| `TestMove_RetryAfterCutover` | a retain acked on the source, the namespace moved, the client retries the same idempotency key on the target | the target returns the original operation (idempotency keys are a mutable table, copied by the merge-diff) | T3 + T5 |

#### 8.4.4 Storage and plans (the Postgres side)

| Test | Setup | Assert | Tier |
|---|---|---|---|
| `TestPlans_AsEngramApp` (N131, P-5) | every N19 `EXPLAIN` assertion executed as `engram_app` with the scope GUCs set, on the real ParadeDB image (never as owner or superuser) | the BM25 arm keeps Top-K pushdown under RLS; the entity trigram lookup goes through the `SECURITY DEFINER` wrapper that re-checks `namespace_id` and uses `entities_trgm_idx` (≤ 10 ms; the unwrapped query seq-scans 85 k entities in ≈ 308 ms); no tag predicate appears under RLS (tags resolve once to `$allowed_docs`); the check is also the M0 gate | T3 |
| `TestHNSW_PerNamespace` (N112) | eight namespaces of 1 k to 100 k vectors sharing one partition, shared-topic vectors | below 2,000 live vectors the plan is an exact scan (no HNSW node) returning `min(cap, |visible|)`; at 2,000 the stats sweeper creates the partial index through `engramctl index` (CIC on the partition) and the plan uses it with `plan_cache_mode = force_custom_plan`; dropped below 1,000; visited tuples per query ≈ `ef_search` (150 MID, 400 HIGH) within 1.3×, independent of the other namespaces; forcing a shared partition HNSW (`faultinject`) reproduces the 1/selectivity blow-up (26 k buffers at 3 % selectivity) | T3 |
| `TestVectors_ModelGeneration` (N111) | `ReembedNamespace`; a `models.embed` config flip | arms read `embedding_model = <current>`; the workflow inserts new vectors, builds the new index, flips the namespace's current model, then expunges the old; a config change alone is refused | T3 |
| `TestIndex_PartitionedProcedure` (P-10) | `engramctl index` on a 16-partition table | `CREATE INDEX … ON ONLY` the parent, `CREATE INDEX CONCURRENTLY` on each partition, `ATTACH PARTITION`; a migration that issues `CREATE INDEX CONCURRENTLY` on the parent fails `engramlint migrate`; progress is visible in `pg_stat_progress_create_index` | T3 + S |
| `TestHNSW_DropNoRepair` (P-4, N112) | delete a 1 M-fact namespace | `DROP INDEX` plus batched `DELETE` finishes with no autovacuum run longer than 30 min and no shard-wide graph repair; a touched partition index above 5 % dead is rebuilt with `REINDEX INDEX CONCURRENTLY` by the expunge | T3 + T5 |
| `TestXID_Budget` (N114, P-11) | 4 consumers idle for 5 min; 50 writes/s for 10 min | cursor advances are batched to ≤ 1 per second per consumer (idle XID consumption ≤ 5/s); `age(relfrozenxid)` is exported and the 50 % alert rule passes `promtool test rules`; `vacuum_freeze_min_age = 10 M` on content partitions | T3 |
| `TestConfig_Lint` (N131) | `engramctl config lint` | the `postgres -c` list and the role GUCs are generated from one table; drift fails; `pick_shard` derives `namespaces_count`; `engram_cleanup_namespace`'s outer `DELETE` carries `namespace_id`; scheduler writes to `operations` and the outbox go through `WithNamespaceTx(write)` (`engramlint sql` checks the fence prelude before every outbox insert) | S + T3 |
| `TestRecall_SmallNamespaceExact`, `TestLexical_TopKPushdown` | 16-partition fixture, namespaces of 1 k and 50 k facts; the lexical query on the real image | the 1 k namespace is an exact scan returning 150 candidates in < 5 ms warm; BM25 shows the pg_search custom scan with Top-K pushed down, one partition pruned, composite `key_field` accepted, p95 < 60 ms; otherwise the fixture switches to `TsvectorIndex` and records its p95 for M0.2 | T3 |

#### 8.4.5 Retain, consolidation, fence and other faults

| Fault or test | How it is injected | Expected behaviour and assertions | Tier |
|---|---|---|---|
| Stale catalog cache | `MemoryCatalog.DropNotifications()` on api-1, then a completed move of `A1` | first store call fails `WrongShardOrEpoch`; the router invalidates, re-resolves and retries once (a second consecutive failure on a write → `FAILED_PRECONDITION`); a read that sees `MOVED_OUT` re-resolves in a bounded loop ≤ 5 s; exactly 2 shard transactions; latency < 2× baseline | T5 (T3 with `FakeTx`) |
| Misrouted request (phase 3) | cell-2's `StaticResolver` claims shard 3 lives in cell 1 | cell 1 gets `engram-forward-hops: 1`, fails ownership, answers `WrongShardOrEpoch`, **never forwards again**; deadline and metadata preserved | T5 |
| Gateway 429 / 5xx / 4xx | `GW_STATUS=429` with `Retry-After`, `GW_FAIL_RATE=0.3`, `GW_STATUS=400` for one chunk hash | activities back off and honour `Retry-After`; a 400 is `PermanentLLMError` (recorded in `operations.error`, the operation `SUCCEEDED` with `units_failed > 0`); recall drops dense arms on embed 5xx and skips rerank on rerank 5xx; recall p95 under fault ≤ 300 ms | T5, T2 |
| Blob unavailable | `toxiproxy` cuts MinIO | a retain body > 64 KiB fails at ack `UNAVAILABLE`, ≤ 64 KiB succeeds; extraction proceeds with cache misses; expunge purge and export retry; **deletes return `UNAVAILABLE` because the intent `put` fails** (8.4.2); after the toxic is removed everything completes within 2 min | T5, T3 |
| Temporal outage and loss | `docker pause` of the cell's Temporal for 90 s with 200 retains; then loss of its Postgres (restore from its own backup) | acks fail `UNAVAILABLE`; the `op-sweeper` starts any `PENDING` operation older than 2 min with no workflow; no operation runs twice (`ns/{ns}/op/{operation_id}`); after a Temporal restore, `operations` rows drive `Restart` of everything in flight; other cells are unaffected (cluster per cell, P-9) | T5 |
| Duplicate activity execution | `env.OnActivity(CommitChunk).Twice()`; likewise `FinalizeVersion`, `StoreProposal`, `ApplyBatch`, `MoveCopyRange`, `Expunge.Purge`; a second `ConsolidateRoute` answer that differs | the second execution is a no-op; the stored proposal wins (N43); counts, outbox and `consolidation_applied` unchanged; a purge batch repeated deletes nothing more | T2 (+ T3) |
| Outbox relay double election | two workers per shard; `SIGKILL` the lock holder; partition its direct connection | the survivor acquires within 5 s; every event delivered exactly once; leader gauge sums to 1; the partitioned holder's next cursor write fails | T5 |
| Statement timeout during a write | `SET LOCAL statement_timeout = '50ms'` on one writer | the aborted `seq` is declared aborted after `2 × 30 s`; later seqs are delivered on time | T5 |
| Quota exhaustion mid-operation | `llm_tokens_per_day` set to 10 k; a 40-chunk document; a consolidation batch | `quota.Reserve` precedes every gateway call class (extract, routing, write, page refresh, each Reflect iteration); workflows go `DEFERRED` and resume at the window reset, synchronous Reflect returns `RESOURCE_EXHAUSTED`; no chunk extracted twice | T2 + T5 |
| `TestRetain_CommitOrdering` (N40) | `CommitChunk(v, h)` paused after `BuildLinks` while (a) `DeleteDocument` commits, (b) `v₂ > v` is retained and finalised, (c) `v₁`'s `FinalizeVersion` runs after `v₂`'s | (a) the commit aborts on `status ≠ 'ingesting'`, or, if it wins the race, its rows are tombstoned by the predicate and no surface returns them (the expunge purges them); (b) `superseded`, zero rows, `Recall` never returns `h` after `v₂`'s finalise; (c) `v₁` is `superseded` and adds no `chunk_tombstones`; exactly one `active` version | T2 + T3 |
| `TestConsolidation_TwoStage` (N121) | candidates with distinctive phrases; merges; a hidden source | stage 1 persists decisions only; no written observation text contains a phrase that is not in its own sources or attached facts; `inputs(O, v)` = the facts rendered, all of them O's own sources; a `merge` is a root rebuild from the union of live sources with no previous text; only visible sources are rendered; a batch is re-queued at most 3 times then `failed`; the measured `calls_per_chunk` (≈ 3.5) and A-W (0.8) are recorded for Table 6.8-B | T2 + T3 |
| `TestConsolidation_PersistedProposal`, `_AtomicKey`, `_ApplyReverifiesInputs` (N43) | write calls executed twice with different answers; worker killed between effect and key; a source made hidden between `StoreProposal` and `ApplyBatch` | one proposal row, one effect per `op_key`; one transaction or a retry applies once; re-verification (the visibility predicate over `$fact_ids` in a fresh statement, under the shared derivation lock, no `FOR SHARE` on facts) discards the proposal and re-queues without the hidden id | T2 + T3 |
| `TestConsolidation_PendingFromWatermark` (H-15) | consolidate 500 facts, stamp failures, hide a fact | `fact_consolidation` is append-only with `PRIMARY KEY (namespace_id, memory_id, stamped_at)`; consolidated = an `EXISTS` row with `note = 'done'`; retryable = latest stamp `failed` and older than 7 days; the watermark stays below the smallest unconsolidated fact, live or marker-hidden | T3 |
| `TestRetain_ConcurrentAppendsChain`, `TestAppend_ReadsOneBodyObject` (N56, H-16) | two `APPEND` items 1 s apart; a chain of 50 appends | the second append's base is the first's version, final text `body(c) ‖ A ‖ B`; `LoadItem(APPEND)` materialises the base body from the ledger chain up to the nearest version with a body (`append_base_version`), under the document lock | T3 |
| `TestFence_TryLockRefusedBehindWaiter`, `TestFence_PoolNotExhausted`, `TestDocLock_NoStarvation` (N82, N83) | session A holds the shared fence, B requests the exclusive one, C tries shared; 32 writers plus a freeze; continuous `CommitChunk`s against `FinalizeVersion` | C is refused immediately (`NamespaceFrozen{retry_after = 200 ms}`); no pooled connection waits behind the freeze; recall p95 on another namespace does not move; exclusive takers make one attempt with `lock_timeout = 35 s` (no 5 s retry storm, P-12); `DocumentBusy` is retryable; no `FOR SHARE` on `documents` or `document_versions` in any `store.Queries` text | T3 + T5 |
| `TestExtraction_RenderHashKey` (N87) | the same chunk under a changed mission, hints, timestamp day or summary | each change is a new key; identical rendered variables hit | T1 + T3 |
| `TestCommit_InputBlobMissing`, `TestIso_Temporal_KeyShredding` (N99, N100) | purge an `xcache` blob between `EmbedChunk` and `CommitChunk`; delete a namespace | retryable `InputBlobMissing`, at most two re-runs; a blob younger than 24 h is never purged; histories hold ciphertext only and are unreadable after the data key is destroyed | T3 |
| `TestEvents_SizeBound` (N80) | every event type at its maxima | `len(Marshal(e)) ≤ 16 384`; `DocumentDeleted` is one small event (no id lists: consumers read by `(namespace_id, document_id)`) | T1 |

#### 8.4.6 Model-checking regressions (N46)

Every counterexample configuration has a Go twin that reproduces the flaw with the fix
disabled (a `faultinject` knob) and proves its absence with the fix on; `formal/MANIFEST.md`
pairs each spec with these tests and `scripts/formal-manifest-check.sh` fails a PR that changes
one without the other. The D22 specs are written in Phase 0 (M0.7); until they exist the
twins run against the Go model only.

| Spec and configuration | Property or counterexample | Go twin |
|---|---|---|
| `Derivation.tla` | `NoDeletedDerivationServed`, `NoInvalidatedDerivationServed` | `TestVisibility_AllSurfaces` |
| `Derivation.tla` | `NoOverHiding` (a version with no victim in its segment is served) | `TestVisibility_SegmentHiding` |
| `Derivation.tla` | `RestoreExact` | `TestInvalidate_RestoreExact` |
| `Derivation.tla` | `AsOfNoLeak` (segment-wide `effective_at`) | `TestAsOf_*` (8.5) |
| `Derivation.tla` | `MaterializeComplete`, and the configuration **without the derivation lock, which must fail** | `TestExpunge_DerivationLock` |
| `Storage.tla` | `ContentImmutable`, `VectorGenerationConsistent`, `IndexConvergence` | `TestContent_InsertOnly`, `TestVectors_ModelGeneration`, `TestHNSW_PerNamespace` |
| `Durability.tla` | `AckImpliesIntent`, `RestoreReplaysIntents`, `IntentOrderLastWins` | `TestIntent_AckImpliesIntent`, `TestRestore_ReplaysIntents` |
| `ShardMove.tla` (rewritten) | `SingleWriter`, `NoRouteToTargetBeforeC`, `RollbackPossibleBeforeC` | `TestMove_RollbackEveryStep`, `TestMove_ReadyState` |
| `ShardMove.tla` | `NoLossNoDup` under a partial bulk copy, bounded writer lifetime, margin re-copy and mutable diff (the zero-margin configuration fails) | `TestMove_DirtyCopyReconcile` |
| `ShardMove.tla` | `ZombieCannotCutOver`, `RestoreReconciles`, liveness: a started move completes or rolls back | `TestMove_ZombieFenced`, `TestRestore_OpenMoves` |
| `Outbox.tla` (`Outbox_Watch1x.cfg`) | a 1× gap horizon loses an event; rerun without the commit-wait term | `TestOutbox_Watch1x` |
| `Consolidation.tla` (`_VolatileProposal`, `_NonAtomicKey`) | two lists applied under one key | `TestConsolidation_PersistedProposal`, `_AtomicKey` |

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
6. budget not consumed by invisible rows: each arm returns `min(cap, |visible|)` (the filter is inside the arm, D9);
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
older version) and `as_of ∈ [t_5, t_8)` still returns v1. The wall clock is irrelevant: the test
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

REVIEW-4 (round 3, PostgreSQL 16.15, pgvector 0.8.6, 4 vCPU, 200 k facts in one partition, warm)
measured the previous design; those numbers are the baselines, and the D22 design must beat
them by the stated margin. The fixtures are built by `engram-bench synth` on the ParadeDB image
and run in M0.6 (storage, plans, IOPS), M0.4 (Temporal) and M1.5 (move); rows marked *unmeasured*
are predictions, not results, and a miss is a §11 item.

| Benchmark | Baseline (previous design, REVIEW-4) | D22 target | Where |
|---|---|---|---|
| Hide 2 000 facts (retire / invalidate / delete) | 4 872 ms (2.44 ms per fact: a non-HOT update re-inserts the vector into HNSW); 100 k-fact delete ≈ 250 s | one marker row, ≤ 10 ms at any size; zero HNSW work (`TestDelete_O1`) | M1.3 |
| `INSERT` of 5 000 facts, HNSW present / absent | 9 260 ms / 257 ms (1.85 / 0.05 ms per fact); ≈ 13 KB WAL per fact with HNSW | `CommitChunk` of 10 facts + 1 chunk into a ≤ 1 M-element per-namespace index ≈ 2–5 ms HNSW, p95 ≤ 40 ms end to end (*unmeasured*) | M0.6 |
| HNSW vacuum repair after purging 10 % of a partition | 231 s (228 s CPU); 51 s at 0.5 % dead; a full build of the same 200 k rows takes 67 s with 4 workers (≈ 0.34 ms per vector, ≈ 3.5–4 min per 625 k partition) | no shard-wide repair: namespace delete = `DROP INDEX` + batched `DELETE`; hygiene = `REINDEX INDEX CONCURRENTLY` of one small index above 5 % dead; build rate ≥ 3 000 vectors/s with 4 workers; no autovacuum run > 30 min (`TestHNSW_DropNoRepair`) | M0.6, M1.10 |
| Entity trigram lookup as `engram_app` | 308 ms and 2 576 buffers (seq scan of 85 k entities; 6.7 ms as superuser) | ≤ 10 ms index scan through the `SECURITY DEFINER` wrapper (`TestPlans_AsEngramApp`) | M0.2 |
| Semantic arm, 4 k to 6 k-fact namespace in a shared partition with a tag or `as_of` filter | shared HNSW 67–111 ms, 26–27 k buffers; exact path at 20 k: 41 ms, 8 000 heap pages (64 MB) | exact scan ≤ 2 000 vectors (≤ 3.2 MB) ≤ 5 ms; per-namespace HNSW visits ≈ `ef_search` (150 MID) rows, ≤ 15 ms warm per arm (*unmeasured* with shared-topic vectors) | M0.6 |
| Page touches per MID recall and IOPS | not measured (hot set omitted the heap) | ≈ 1 400 touches (3 vector arms × ≈ 150 visited × 2 pages, BM25 ≈ 200, graph ≈ 300); ≈ 3.5 k IOPS at 50 QPS and a 5 % miss rate, within the 10 k budget (N114) | M0.6 |
| XID consumption at 50 writes/s | ≈ 90 XIDs/s (50 writes + cursor updates every 100 ms): the 2×10⁸ freeze age arrives every ≈ 26 days | ≤ 55 XIDs/s (cursors batched to 1/s per consumer): ≈ 4.7 M/day, 50 % of `autovacuum_freeze_max_age` (10⁹) in ≈ 107 days (`TestXID_Budget`) | M1.4 |
| Move copy rate | ≈ 540 facts/s per stream (HNSW insertion bound) | ≥ 2 000 facts/s per stream (B-tree and BM25 bound; the target has no HNSW until the copy ends), 4 streams (*unmeasured*); freeze < 30 s for ≤ 1 M facts | M1.5 |
| Degraded-mode recall (N119) | n/a | per-candidate `observation_inputs` lookup ≤ 20 ms per arm for 150 candidates × ≤ 60 rows; three marker selects ≤ 3 ms at ≤ 16 k entries | M1.3 |
| Exclusive fence taker against legal 30 s writers | 5 s attempts, each failure a 5 s write brownout | one 35 s attempt, no retry storm; writers refused immediately (`TestFence_*`) | M1.8 |
| Temporal per cell (P-9) | one untuned `auto-setup` cluster, ≈ 20 events per chunk, nothing measured | split services, 512 history shards, own Postgres; sustains ≥ 1 000 events/s (≈ 17× the ≈ 58 events/s of a 2.9 chunks/s online cell; the backfill peak) at < 70 % history CPU and persistence p99 < 50 ms | M0.4 |

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
10 M-fact shard against the 216 ms critical-path budget (Hindsight documents 100–600 ms). A
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
embedding + 50 rerank pairs); reflect ≈ $0.085 per typical call (≈ 133 k billed input tokens) and ≈ $0.34 worst case. Budget alerts fire on the
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

### Round-3 changes

| Withdrawn tests | Replaced by |
|---|---|
| `TestDelete_LineageTransitive`, `TestApply_RejectsHiddenCandidate`, `TestDelete_CascadeScales`, `TestInvalidate_SupersededVersions`, `TestEvents_SizeBound` paging | `TestVisibility_SegmentHiding`, `TestDelete_O1`, `TestInvalidate_RestoreExact`, `TestExpunge_*` (8.4.1) |
| `TestMove_ReplayCoversEveryEvent`, `TestQueries_CoveringEvent`, `TestMove_AntiJoinReplay`, `TestMove_CopyBarrier`, `TestMove_CatchUpGivesUp`, `TestMove_LoadUnderRLS` (bypass-role variant), `TestMove_ResumableCopy` (floor `p0`) | `TestMove_DirtyCopyReconcile`, `TestMove_RollbackEveryStep`, `TestMove_ReadyState`, `TestMove_ZombieFenced`, `TestMove_PlanRefusals` (8.4.3) |
| `TestFailover_RestartsInFlight` and `TestDelete_RemoteApplyRPO0` (`remote_apply`) | `TestIntent_AckImpliesIntent`, `TestRestore_ReplaysIntents`, `TestFailover_ReplaysIntents`, `TestDurability_NoSyncWait` (8.4.2) |
| `TestPlanner_CoverageBands`, shared-HNSW band tests | `TestHNSW_PerNamespace`, `TestVectors_ModelGeneration`, `TestPlans_AsEngramApp` (8.4.4) |
| `DocLifecycle.tla` and `AsOf.tla` twins | `Derivation.tla`, `Storage.tla`, `Durability.tla` and the rewritten `ShardMove.tla` twins (8.4.6); measured REVIEW-4 numbers are the baselines of 8.6.1 |
