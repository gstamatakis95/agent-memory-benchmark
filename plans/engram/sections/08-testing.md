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
| T2 | Workflow | Temporal `testsuite.WorkflowTestSuite` with `FakeActivities`: fan-out ≤ 32, `ContinueAsNew` at 500 chunks, idempotent re-execution, deferral on quota, move state machine, consolidation bisect | none (in-memory test env) | < 90 s | `make test-workflow` | every push; required |
| T3 | Integration | Real SQL against the real schema: RLS canary, ownership fencing, `ON CONFLICT` arbiters, outbox ordering, `as_of` per arm, delete cascade, HNSW/BM25 queries, migrations up/down | testcontainers `paradedb/paradedb:latest-pg16` (pinned digest), **one container per package** started in `TestMain`, migrations applied by `engramctl migrate --dsn` | ≤ 6 min wall (packages run in parallel, `-p 4`) | `make test-integration` | every PR; required |
| T4 | End-to-end | The public contract through Envoy: gRPC, Connect, MCP; 2 shards; a namespace move under load; export round-trip | `docker compose --profile e2e` (§9.1 topology with `DeterministicClient` as the gateway, 1 api, 1 worker, 2 shards, catalog, Temporal, MinIO) | ≤ 12 min | `make e2e` | every PR to `main`; required |
| T5 | Chaos | §8.4 fault matrix: kills, pauses, partitions (toxiproxy), gateway faults, duplicate activities, relay double election | T4 stack + `toxiproxy` + build tag `faultinject` | ≤ 40 min | `make e2e-chaos` | nightly; **required** on PRs touching `internal/{move,outbox,catalog,router,store,workflows}` (path filter) |
| T6 | Benchmark | §8.5 leakage grid, §8.6 LongMemEval/LoCoMo, §8.7 Hindsight side-by-side, §8.8 cost/latency | T4 stack + the real gateway; `bench.lock` pins | smoke ≤ 10 min; LME-S ≈ 6 h; LME-M ≈ 2 days | `make bench-smoke` (PR), `make bench DATASET=lme_s` (weekly), `make bench DATASET=lme_m` (release) | smoke on every PR touching `internal/{recall,chunk,extract,index}`; full weekly; M on release candidates |
| F | Formal conformance | TLC bounded model checks and `lake build` of the Lean modules; trace validation of recorded Go runs against the TLA+ specs (§7) | Java + TLC, Lean 4 toolchain | ≤ 30 min | `make formal` | nightly; required on PRs touching `formal/` or the packages a spec covers |
| S | Static | `golangci-lint`, `buf lint`, `buf breaking --against main`, `go vet`, the RLS policy check (§8.3), the metric-label linter (no `namespace` label, D13), the SQL predicate linter (§8.3), `govulncheck` | none | < 3 min | `make lint` | every push; required |

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
| Mover `--fault-at=<phase>` | `internal/move` (`faultinject` tag) | panics after the first committed write of the named phase |

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
| `internal/recall/fuse` | k=60 golden: rank 3 and rank 7 → `1/63 + 1/67 = 0.030793…`; empty arm; duplicate ids within an arm rejected | **Permutation invariance**: fusing arms in any order yields identical scores and identical tie-broken order (ties by `memory_id`). **Monotonicity**: improving `d`'s rank in one arm never lowers `score(d)` and never lowers its final position relative to items whose ranks did not change. **Empty-arm neutrality**: `fuse(A ∪ {∅}) == fuse(A)`. **Cap**: output size ≤ Σ arm sizes. | `Engram/RRF.lean` |
| `internal/recall/pack` | budgets 4k/8k/16k with cl100k counts; an item exactly at budget; zero budget | **Never exceeds**: Σ tokens(packed) ≤ `max_tokens`. **Order preserved**: packed is a subsequence of ranked. **Skip, not truncate**: every packed item is byte-identical to its input. **Never stops early**: an item that fits after a skipped one is packed; `skipped_count == len(ranked) − len(packed)`. **Determinism** under equal token counts. | `Engram/Packer.lean` |
| `internal/recall/temporal` | date-phrase parser goldens ("last March", "in 2015", "the week before Christmas 2023"), coarse spans (year → Jan 1–Dec 31) | **Well-formed**: parsed `start ≤ end`. **Overlap is symmetric** and reflexive. **Distance** to `query_timestamp` ≥ 0 and 0 inside the window. **Proximity boost** ∈ [0.9, 1.1]; combined boost factor ∈ [0.75, 1.25] for any (recency, temporal, proof) triple. | `Engram/TemporalWindow.lean` |
| `internal/recall` (planner) | arm caps 50/150/400 by budget; `stage=FUSED` when remaining deadline < 150 ms; stream batches of 10; `RecallStats` trailer | **Cap adherence** over `MemIndex`: each arm returns ≤ cap. **`as_of` inside every arm**: for random corpora and `T`, no arm returns `mentioned_at > T`, and each arm returns `min(cap, |visible|)` rows — the filter is not post-hoc. **Type/tag filters** commute with fusion (filtering before fusion == filtering after, when caps are not hit). | `AsOf.tla` (invariant `NoFutureFact`) |
| `internal/entity` | trigram thresholds (0.3 candidate, 0.6 accept, 0.85 when a type is unknown; §5.1.2) on a seeded set; names > 256 chars dropped; whitespace normalisation | **Idempotence**: `Resolve(B); Resolve(B)` creates no new entity on the second call. **Order insensitivity**: the *set* of `(mention → canonical entity)` assignments is independent of the order of mentions within a batch. **Monotone growth**: `Resolve(B ∪ B')` then `Resolve(B')` creates no new entity. **Namespace locality**: every returned `entity_id` has `namespace_id == scope.namespace`. | — |
| `internal/link` | weights (`max(0.3, 1 − Δh/24)`, cosine ≥ 0.75, causal 1.0); lock-order sort key | **Caps**: temporal links per fact ≤ 20; semantic ≤ 10 per fact and each with cosine ≥ 0.75; entity ≤ 20; ≤ 60 in all (§5.1.2); causal only to an earlier fact of the same batch. **No self-links**, **no cross-namespace endpoints**, **no orphan endpoint** (both facts exist and are not retired at insert). **Determinism** of the link set for a fixed batch. | `DocLifecycle.tla` (`NoOrphanLinks`) |
| `internal/outbox` | batch of 500; cursor persistence; 7-day trim honours every cursor; `Event` proto round-trip | **Gap watchlist** (model: random interleavings of `seq` allocation, commit and abort with `statement_timeout = 30 s`): every committed `seq` is delivered **exactly once** per consumer; delivery is in ascending `seq` per consumer; a `seq` that commits within `2 × statement_timeout` of being skipped is never declared aborted; an aborted `seq` is declared within that bound and never blocks later ones for longer. | `Outbox.tla` (`NoLoss`, `NoDup`, `Ordered`, `BoundedGap`) |
| `internal/catalog` | LRU 100 k; TTL 60 s; negative 5 s; `stale_max` 10 min; `LISTEN` payload parsing; full flush on reconnect | **Invalidation model** (ops: `resolve`, `notify(ns)`, `expire`, `catalog_down`, `reconnect`): after `notify(ns)` and while the catalog is reachable, the next `resolve(ns)` returns the new `(shard, epoch)`; while unreachable, an entry is served for ≤ `stale_max` from its last refresh, then `UNAVAILABLE`; a negative entry is never served > 5 s. **CAS**: `MemoryCatalog` and the Postgres catalog give identical results for a random op sequence (T3 twin). | `ShardMove.tla` (stale-cache steps) |
| `internal/authz` | `(claims, method, entry) → code` table; JWKS refresh; expired token; `kid` rotation | **Allowlist**: `ns = ["*"]` admits every namespace of the same tenant and none of another; a token never yields a `RequestScope` whose `tenant != claims.tenant`. **Scope monotonicity**: adding a scope never turns an allowed call into a denied one. | — |
| `internal/api` | `DeadlineGuard` (missing → `INVALID_ARGUMENT`, clamped); `request_id` reuse with a different hash → `ALREADY_EXISTS/OperationConflict{IDEMPOTENCY_KEY_REUSED}` (D1); opaque page tokens (HMAC, tamper → `INVALID_ARGUMENT`); field masks | **Idempotency**: replaying any unary write with the same `request_id` within 24 h returns the stored response and performs no second effect (`FakeTx` effect counter). **Pagination**: walking all pages yields each row exactly once for random inserts between pages (ids are UUIDv7 → stable order). | — |
| `internal/quota` | token bucket refill; `RESOURCE_EXHAUSTED` + `QuotaExceeded` detail (D13); day window reset at UTC midnight | **Bucket**: at most `rate` admissions per window for any arrival pattern. **Deferral**: `llm_tokens_per_day` exhaustion never fails an operation; it is `DEFERRED` and resumes exactly once at the window reset. | — |
| `internal/store` | SQL builders golden (every query text under `testdata/sql/`), `SET LOCAL` prelude, ownership `FOR SHARE` prelude on write mode | **Predicate presence**: every builder output for a namespace-scoped table contains `namespace_id = $n` (also enforced statically, §8.3). **Fencing state machine** in `FakeTx`: a write with `state ∉ {active}` or the wrong epoch fails with `WrongShardOrEpoch`/`NamespaceFrozen`; a read with `state ∈ {active, frozen}` succeeds. | `ShardMove.tla` (`OneWritableOwner`) |
| `internal/extract` | schema validation of `ExtractedFact`; degenerate-fact filter; causal `target_index < i`; cache key = `sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)` | **Cache key sensitivity**: changing any one of the four inputs changes the key; **prompt version pinned**: the golden for `extract/v1` fails if the template changes without a version bump. | — |
| `internal/consolidate` | ops `create/update/delete` application; `op_key` recording; 8 facts/call; ≤ 100/round | **Bisect**: for any failure oracle, every fact lands in exactly one successful leaf batch or is stamped failed; LLM calls ≤ 2n − 1; a fact never appears in two successful batches. **Idempotent effect**: applying a batch twice (same `batch_key`) yields one effect per `op_key`. **Effective time**: `effective_at == max(max(mentioned_at) of the cited facts, effective_at of the previous version)` (D9, N29), never `now()`. | `Consolidation.tla` (`ExactlyOnceEffect`) |
| `internal/reflect` | iteration cap 10; 100 k tokens; 300 s wall; per-tool 10 s; JSON-schema validation | **Citation filter**: for any set `S` of ids returned by tools and any candidate citation set `C`, `cited ⊆ S`. **Caps** hold for any tool-result sizes (tool results are truncated to the shared ceiling, never the caps exceeded). | — |
| `internal/move` | state transitions table; `p0` capture; `move_applied_seq` monotone | **Transition legality**: a random op sequence never produces an edge outside D5's graph; `rolled_back` reachable only before `cutover`. **Count preservation** over `FakeTx`: after `done`, per-table row counts and multiset of ids at target == source snapshot ∪ replayed events. | `ShardMove.tla` |
| `internal/export` | manifest schema; 1 MiB parts; zstd round-trip | **Delta correctness**: `apply(snapshot v_{n−1}, delta) == snapshot v_n` for random outbox ranges within the retention window. | — |
| `internal/telemetry` | metric-label linter; span names | **No `namespace` label** on any registered metric (walks the registry). | — |

Example of the shape every property test takes (packer; the Lean theorem it mirrors is
`pack_never_exceeds` in `Engram/Packer.lean`):

```go
func TestPack_NeverExceedsBudget(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		budget := rapid.IntRange(0, 16_000).Draw(t, "budget")
		items := rapid.SliceOfN(genRankedItem(), 0, 400).Draw(t, "items")
		out := pack.Greedy(items, budget, tok.CL100K)
		if got := tok.Sum(out.Packed); got > budget {
			t.Fatalf("packed %d tokens > budget %d", got, budget)
		}
		requireSubsequence(t, out.Packed, items)              // order preserved, no truncation
		if out.SkippedCount != len(items)-len(out.Packed) {  // never stops early
			t.Fatalf("skipped_count %d, want %d", out.SkippedCount, len(items)-len(out.Packed))
		}
	})
}
```

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
| Outbox relay | `TestIso_Relay_TenantMix`: events of `Z1` and `A1` (same shard) are delivered to the `index` consumer with their own `namespace_id`; the Kafka sink (when on) keys by `namespace_id`; no consumer callback ever receives a payload whose `namespace_id ≠ event.namespace_id` (decoded and compared) | `TestIso_Relay_NamespaceCursor`: the `move:A1` consumer receives only `A1` rows although `Z1` rows interleave in `seq` | `TestIso_Relay_CrossShard`: the relay built from `ShardHandle(1)` never opens a connection to shard 2 (`RecordingPool`); shard 2's outbox `seq` space is disjoint by construction and its rows never appear in shard 1 cursors |
| Move consumer (target side) | `TestIso_Move_TenantMix`: during `copy`/`catch-up` of `A1`, `Z1` rows on the source are neither copied nor replayed (target `Z1` count stays 0) | `TestIso_Move_NamespaceOnly`: target applies only `(namespace_id = A1, seq > p0)`; a crafted event with `namespace_id = A2` in the `move:A1` stream is rejected and counted (`engram_move_rejected_events_total`) | `TestIso_Move_Epoch`: a replayed event carrying `epoch ≠ e` is rejected; after `cutover` a source write with epoch `e` → `WrongShardOrEpoch` |
| Metrics / logs | `TestIso_Metrics_Labels`: scrape `/metrics`; every series has `shard`; `tenant` appears only on `engram_llm_tokens_total`, `engram_llm_cost_micros_total`, `engram_quota_events_total`; no series has `namespace` (also the static linter) | `TestIso_Logs_NoContent`: 10 k requests with sentinel content; the JSON log stream contains `namespace_id` fields but never any sentinel string, query text or fact text at `info` | `TestIso_Traces`: span attributes carry `engram.shard = shard(ns)` only; no span of a request for `A1` has `engram.shard = 2` |

Two rows apply to every surface at once: `TestIso_Deleting_AllSurfaces` — a namespace in
catalog state `deleting` returns **FP** on every method of every surface (N5), and the
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

```go
func TestRLSCanary(t *testing.T) {
	db := pgtest.Migrated(t)                   // paradedb container + migrations
	seed := pgtest.SeedNamespace(t, db, "owner")
	empty := pgtest.SeedNamespace(t, db, "stranger")
	for _, q := range store.Queries.All() {
		t.Run(q.Name, func(t *testing.T) {
			n := pgtest.RunAs(t, db, empty, q)   // SET LOCAL engram.namespace_id = stranger
			if n != 0 { t.Fatalf("%s leaked %d rows across namespaces", q.Name, n) }
			if m := pgtest.RunAs(t, db, seed, q); m == 0 && !q.WriteOnly {
				t.Fatalf("%s returned 0 rows for the owner; the canary proves nothing", q.Name)
			}
		})
	}
}
```

**Static RLS check** (`TestEveryTableHasRLS`, T3 and `make lint` via `engramctl migrate --check-rls --dsn`):

```sql
-- every table with a namespace_id column must (a) have RLS enabled and forced,
-- (b) carry the policy ns_isolation, (c) grant engram_app nothing that bypasses it.
SELECT c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'namespace_id' AND NOT a.attisdropped
WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
  AND (NOT c.relrowsecurity OR NOT c.relforcerowsecurity
       OR NOT EXISTS (SELECT 1 FROM pg_policies p WHERE p.tablename = c.relname AND p.policyname = 'ns_isolation'
                      AND p.qual = '(namespace_id = (current_setting(''engram.namespace_id''::text))::uuid)'));
-- expected: zero rows. Tables WITHOUT namespace_id must be in the explicit allowlist:
--   outbox_cursors, shard_meta, goose_db_version  (anything else fails the test).
-- Extra policies are permitted only for engram_relay (relay_read_all: SELECT on outbox only, N4; outbox_cursors has no namespace column), engram_move (N2) and engram_admin.
```

The same test asserts `rolbypassrls = false` for `engram_app` and that `engram_app` is not a
member of any role with `BYPASSRLS`. Belt and braces: `make lint` runs `engramlint sql`, which
parses every builder's output (`testdata/sql/*.sql`) with `pg_query_go` and fails if a
namespace-scoped table is referenced without an equality predicate on `namespace_id`
(rationale: RLS makes the omission safe, the explicit predicate keeps partition pruning; both
must hold).

### 8.4 Fault injection

All rows run at T5 unless marked T2/T3. "Assert" lists the observable the test checks; every
row also asserts the §7 safety invariants via the SQL checks in the crash-mid-move table below.

| Fault | How it is injected | Expected behaviour | Assert | Tier |
|---|---|---|---|---|
| Stale catalog cache | `MemoryCatalog.DropNotifications()` on api-1 (its `LISTEN` goroutine is muted), then `engramctl move` of `A1` shard 1 → 2 completes | First store call on api-1 fails `WrongShardOrEpoch`; router invalidates, `ResolveFresh` once, retries the handler; client sees success | exactly 2 shard transactions (`RecordingPool`: shard 1 then shard 2); `engram_catalog_reresolve_total{shard="1"}` += 1; latency < 2× baseline; a *second* consecutive `WrongShardOrEpoch` (catalog also stale by injection) → `FAILED_PRECONDITION` + `WrongShardOrEpoch{expected, observed}` to the client, no third attempt | T5 (and T3 with `FakeTx`) |
| Misrouted request (forwarded to the wrong cell), phase 3 | cell-2's `StaticResolver` claims shard 3 lives in cell 1 | cell-1 receives the forwarded call with `engram-forward-hops: 1`, its ownership check fails → returns `FAILED_PRECONDITION/WrongShardOrEpoch`; **never forwards again** | exactly one hop (`engram_forward_total{hops="1"}`), zero with `hops="2"`; deadline preserved on the hop (remaining ≥ original − 5 ms); metadata (`authorization`, `request_id`) forwarded verbatim | T5 |
| Crash mid-move at each phase | `engramctl move start --fault-at=<phase>` (build tag `faultinject`: the mover panics after the first committed write of that phase); load generator runs retain (20/s), delete (2/s), consolidation for `A1` throughout; then `engramctl move resume` | see table below | see table below | T5 |
| Postgres failover on a shard | compose profile `ha` adds a streaming standby for shard 1; `docker kill shard-1-postgres`; `pg_ctl promote` on the standby; pgbouncer `RELOAD` with the new host | in-flight writes fail `UNAVAILABLE` + `RetryInfo{1 s}`; retried writes succeed; workflows on `shard-1` retry activities; reads resume; **epoch unchanged** (failover is not a restore) | no acked retain lost: every `operation_id` acked before the kill reaches `SUCCEEDED`; no duplicate facts; relay resumes from its cursor with no gap; recovery < 60 s | T5 (nightly only; 8 min) |
| Gateway 429 / 5xx / 4xx | `GW_STATUS=429` with `Retry-After: 2`, `GW_FAIL_RATE=0.3`, `GW_STATUS=400` for one chunk hash | 429/5xx: activity retries with backoff (100 ms → 5 s, then Temporal policy 1 s → 60 s, unlimited within the workflow deadline), honours `Retry-After`; 400: `PermanentLLMError` → the chunk's hash and reason are recorded in `operations.error`, operation ends `SUCCEEDED` with `progress.units_failed > 0` (N35; the proto has no separate state); **recall**: embed 5xx → dense arms dropped, lexical/temporal/chunk-BM25 answer; rerank 5xx → `stage=FUSED` | operation states as stated; `engram_gateway_ratelimited_total` counts; recall p95 under fault ≤ 300 ms (no waiting on a dead gateway beyond its 100 ms connect timeout); zero facts from the 400 chunk, all facts from the others | T5, and T2 for the activity retry classification |
| Blob unavailable | `toxiproxy` cuts MinIO; `MemStore.FailPuts` at T3 | retain of a body > 64 KiB (N7: raw blob `Put` precedes the ledger tx) fails at ack `UNAVAILABLE`; ≤ 64 KiB (inline in the ledger row) succeeds; extraction proceeds with cache misses (`engram_xcache_total{result="error"}`), never fails; purge and export retry; page refresh cannot persist → retries | no partial ledger rows (raw `Put` precedes the tx); after the toxic is removed, all deferred purges and exports complete within 2 min | T5, T3 |
| Temporal outage | `docker pause temporal` for 90 s while submitting 200 retains | ack fails `UNAVAILABLE` + `RetryInfo` (§1.8, N3: the ack is not weakened); the operation row stays `PENDING` without a workflow; after unpause, clients retrying with the same `operation_id` get the same operation; the per-shard `op-sweeper` schedule (every 60 s, N3) starts any `PENDING` op older than 2 min that has no workflow | all 200 reach `SUCCEEDED`; no operation runs twice (workflow id is `ns/{ns}/op/{operation_id}` → `WorkflowExecutionAlreadyStarted` is swallowed); ledger rows == 200 | T5 |
| Duplicate activity execution | Temporal test env: `env.OnActivity(CommitChunk).Twice()` with identical inputs; likewise `FinalizeVersion`, `ApplyConsolidationBatch`, `MoveCopyTable` | second execution is a no-op | row counts, outbox count and `consolidation_applied` unchanged after the second run; `FinalizeVersion` twice leaves exactly one `active` version | T2 (+ T3 against real `ON CONFLICT` arbiters) |
| Outbox relay double election | two `engram-worker` processes for shard 1; `SIGKILL` the lock holder; `toxiproxy` partitions the holder's **direct** connection (not pgbouncer — the advisory lock lives on a direct session, §8 new decision ND-1) | loser is constructed idle; on holder death the session drops, the lock is released, the survivor acquires within 5 s; the dead holder's uncommitted batch never advances the cursor | every event delivered exactly once across the switch (consumer-side dedup log is empty); `engram_outbox_relay_leader{shard="1"}` sums to 1 at every scrape; the partitioned holder's next cursor write fails (its connection is dead) rather than double-advancing | T5 |
| Statement timeout during a write | `SET LOCAL statement_timeout = '50ms'` on one writer via the `faultinject` hook; concurrent writers proceed | the aborted transaction's `seq` appears in the gap watchlist and is declared aborted after `2 × 30 s`; later seqs are delivered on time | `engram_outbox_aborted_seqs_total` += 1; no consumer stalls > 60 s | T5 |
| Quota exhaustion mid-operation | tenant `llm_tokens_per_day` set to 10 k; a 40-chunk document | operation enters `DEFERRED` after the chunk that exhausts the budget; resumes at the (test-advanced) window reset; per-chunk visibility for committed chunks holds | `Operation.state` sequence `RUNNING → DEFERRED → RUNNING → SUCCEEDED`; no chunk extracted twice (cache hits on resume) | T2 + T5 |

**Crash mid-move, per phase.** The mover is killed after the first committed write of each
phase, the load generator keeps running (retain 20/s, delete 2/s, consolidation) against
`A1`, and `engramctl move resume` is issued after 10 s. The invariants are §7's `ShardMove.tla`
properties expressed as SQL over source, target and catalog; the "observable" column is the
client-visible effect during the fault.

| Killed in | Client-visible during the fault | After `resume` (or automatic rollback) | Invariant checks (all must hold at every sample, sampled every 500 ms) |
|---|---|---|---|
| `planned` (after target `incoming` row) | none: source `active`, writes flow | resumes into `copying`; or `rollback` deletes the `incoming` row | **OneWritableOwner**: `count(ownership WHERE ns=A1 AND state='active') = 1` across shards; catalog `epoch` unchanged |
| `copying` (after `p0` recorded, mid `COPY`) | none | copy restarts from scratch with a new `p0` (the partial target rows are truncated by namespace predicate first); or rollback | target has no `active` row; source counts unchanged; blob prefix `{2}/acme/A1/` either absent or a strict subset — never referenced by any target row |
| `catching_up` (after some replay batches) | none | replay resumes from `move_applied_seq` (idempotent by `(namespace_id, seq)`) | **NoDup**: `count(*) = count(DISTINCT (namespace_id, seq))` in `move_applied`; target facts ⊆ source facts ∪ replayed |
| `frozen` (after source `frozen`, before drain completes) | writes get `NamespaceFrozen`; the API retries ≤ 30 s; reads continue | resume drains and cuts over within the drain wait (15 s default, 60 s max) **or** the 120 s freeze watchdog rolls back (source `active` again; D5 step 4) — the test asserts one of the two happened, and that in the cutover case it completed before the client's 30 s retry budget expired | at no sample do two shards hold `active`; **NoLoss**: every retain acked before the freeze is present on whichever shard ends `active` |
| `cutover` (catalog tx committed, before restarting recorded operations) | brief `WrongShardOrEpoch` on stale caches → single re-resolve | resume restarts the recorded `operation_id`s on `shard-2`; no operation is lost | every operation acked before the freeze reaches `SUCCEEDED` on the target; **ExactlyOnce** for consolidation ops (`op_key` unique); source `moved_out`, target `active`, epoch `e+1` |
| `cleaning` (after the 24 h grace, mid-delete on source) | none (source rows are unreachable: `moved_out`) | resume finishes the delete | target counts unchanged; source `A1` rows reach 0; `Z1` on the source untouched (counts equal to before) |

Rollback is exercised separately for each pre-`cutover` phase with
`engramctl move rollback` while the load generator runs; the same invariant checks apply and
additionally the target's `A1` rows and blob prefix are gone within 60 s.

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
1–5 → consolidate → observation `o` v1 with `effective_at = t_5`; ingest 6–8 → consolidate →
v2 with `effective_at = t_8`; delete the day-7 document → reconsolidate → v3 with
`effective_at = max(mentioned_at of the remaining sources) = t_8`. Assertions: `as_of = t_4` →
no `o`; `t_6` → v1 text; `t_8` → v3 (the latest with `effective_at ≤ t_8`, not v2); `t_9` → v3.
The wall clock is irrelevant: the test runs consolidation with the fake clock set to `t_60`
and asserts `effective_at` never equals `now()`. "Consolidation after `T` must not leak":
with `as_of = t_6`, a version created *later in wall time* but citing only facts ≤ `t_6` is
allowed (it is v1), and any version citing a fact > `t_6` is not returned — the property is
about `effective_at`, not creation time.

**Page test** (`TestAsOf_PageVersions`, phase 3): a page refreshed after each consolidation
above has `page_versions` v1..v3 with the same `effective_at` values; `GetPage(as_of)` and the
`get_page` Reflect tool return the version rule above; a page whose first version is after
`T` is `NOT_FOUND{RESOURCE_KIND_PAGE}` at that `as_of`.

**Reflect** (`TestAsOf_Reflect`): every tool call in a Reflect session with `as_of = T` passes
`T` through, and every citation in the `final` event has `mentioned_at`/`effective_at ≤ T`
(the citation verifier already restricts to returned ids; this test proves the returned ids
were themselves filtered).

**Harness canary** (§8.6): every benchmark question runs with `as_of = question date + 1 s`
(LongMemEval) or the last session date (LoCoMo) and the harness counts
`leak_canary = #results with mentioned_at > as_of`; the run is invalid if the counter is
non-zero. Conformance to `AsOf.tla`: the T3 grid records `(op, T, returned ids)` traces that
the §7 trace validator replays against the spec nightly.

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

**`bench.lock`** (checked in; a run whose environment differs from the lock is refused
unless `--relock` is passed, which writes a new lock and a diff into the report):

```yaml
lock_version: 1
engram: { git_sha: "…", config_sha256: "…", schema_version: 14 }
datasets:
  lme_s:   { sha256: "…", source: "xiaowu0162/LongMemEval", file: longmemeval_s.json }
  locomo:  { sha256: "…", source: "snap-research/locomo", file: locomo10.json }
models:   # exact ids as the gateway reports them (`model` + `version` fields of the response)
  embed:   { id: nomic-embed-text-v1.5, dims: 768, version: "…" }
  rerank:  { id: bge-reranker-v2-m3, version: "…" }
  extract: { id: gpt-oss-120b, version: "…" }      # A-B2: matches Hindsight's 89.0 % configuration
  consolidate: { id: gpt-oss-120b, version: "…" }
  answer:  { id: gpt-oss-120b, version: "…" }
  reflect: { id: gpt-oss-120b, version: "…" }
  judge:   { id: gpt-oss-120b, version: "…", temperature: 0, top_p: 1, max_tokens: 4, seed: 7 }
prompts:  { extract: extract/v1, summarize: summarize/v1, consolidate: consolidate/v1, reflect: reflect/v1,
            answer: bench/answer/v1, judge: judge/v1, judge_abstain: judge/v1-abstain,
            sha256: { judge: "…", judge_abstain: "…", answer: "…" } }
recall:   { budget: mid, max_tokens: 4096, caps: [50, 150, 400], rerank_top: 150, boosts: on }
runs:     { repeats: 3, as_of: question_date+1s }
```

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
| Leak canary | results with `mentioned_at > as_of` (must be 0) | harness |
| Ingest throughput | chunks/s per worker; gateway RPM observed; cache hit rate on a re-ingest of the same corpus (must be ≈ 100 %) | metrics |

**Ablations** (each is one `--ablate` flag; the harness re-runs only the query stage so the
whole set costs one judge pass per arm):

| Ablation | Flag | Expected direction (A-B3: from the LongMemEval paper's retrieval ablations and this repo's earlier retrieval-only runs: hybrid ≈ +9 pp R@5 over BM25 alone) |
|---|---|---|
| each arm off | `--ablate arm=semantic|lexical|graph|temporal|chunks` | semantic or lexical off: −5–10 pp R@5; temporal off: temporal-reasoning −3–6 pp; chunks off: single-session-user/preference −2–4 pp; graph off: multi-session −2–3 pp |
| no rerank | `--ablate rerank` | −1–3 pp accuracy, −120 ms p95 |
| no chunks arm and no `include chunks` | `--ablate chunks,chunk_context` | isolates the value of raw text in context |
| budgets | `--budget low|mid|high` | high: +1–2 pp at +40 % tokens |
| no boosts | `--ablate boosts` | ±1 pp; temporal-reasoning −1–2 pp |
| chunk header off | `--ablate header` | −1–2 pp on multi-session |
| fixed 3,000-char chunker (Hindsight baseline) | `--chunker fixed` | −0–2 pp; establishes the chunker's contribution |
| `as_of` unset | `--ablate as_of` | ≈ 0 on these datasets (all sessions precede the question); a positive delta flags dataset leakage |
| observations off (phase 2) | `--ablate observations` | knowledge-update −2–4 pp |

**Report format.** `bench/results/{YYYY-MM-DD}/{dataset}/{run_id}/` holds `bench.lock`,
`results.jsonl` (per question: id, category, gold, candidate, verdict, retrieval hits, tokens,
per-stage latency, `as_of`), `stats.json` and `report.md` rendered from one template:

```
# Engram bench — lme_s — 2026-11-14 — run 3f9c (git 1a2b3c4)
Lock: judge gpt-oss-120b@v… temp 0 · extract gpt-oss-120b@v… · embed nomic-embed-text-v1.5/768 · rerank bge-reranker-v2-m3
Repeats: 3 · as_of: question_date+1s · leak canary: 0

| Metric                     | recall+answer | reflect | Δ vs last week | notes |
|---|---|---|---|---|
| Accuracy overall           | 86.9 ± 0.8    | 88.4 ± 0.6 | +0.4 | |
| … per category (7 rows) …  |               |         |      | |
| R@5 / R@10 (fused)         | 0.95 / 0.98   | —       |      | session level |
| R@5 / R@10 (packed)        | 0.93 / 0.96   | —       |      | |
| Tokens / question (ctx)    | 3,410         | 12,900  |      | cl100k |
| Cost / haystack (ingest)   | $0.061        | same    |      | extract 71 %, consolidate 22 %, embed 2 %, summarize 5 % |
| Cost / question (query)    | $0.0009       | $0.004  |      | judge excluded |
| Recall p50 / p95 (ms)      | 118 / 236     | —       |      | mid budget; rerank 61 / 118 |
| Reflect p50 / p95 (s)      | —             | 4.1 / 9.8 | | |
Ablations: … (one row per flag, Δ accuracy and Δ R@5 with CI)
Failures: judge_parse_error 0 · gateway errors 2 · deferred ops 0
```

**Cost of a run** (A-B4: gateway list prices of $0.15/M prompt and $0.60/M completion tokens
for `gpt-oss-120b`, $0.02/M for embeddings): LME-S ingestion ≈ 76 k extraction calls
(≈ 115 M prompt + 25 M completion tokens ≈ $32, ≈ $16 through the batch API), summaries
≈ $3, embeddings ≈ $1, consolidation (phase 2) ≈ $25 → **≈ $50–80 per full LME-S run**,
≈ 6 h wall at 10 chunks/s cell-wide (D3). LME-M is ≈ 13× the ingestion → **≈ $700–1,000 and
≈ 2 days**, so it runs on release candidates and monthly, under a `$1,000/run` budget guard
in the harness (`--max-cost-usd`). LoCoMo ≈ $1. The extraction cache makes a re-run of the
*query* stage free of ingestion cost; a re-ingest of an unchanged corpus is a cache-hit test
(§8.6 metrics) and costs only embeddings of nothing (all hashes unchanged).

### 8.7 Side-by-side versus a running Hindsight instance

Compose profile `bench-hindsight` runs Hindsight from its published image next to the Engram
stack, pointed at the **same gateway** through its OpenAI-compatible endpoint, with the same
models, the same corpus (through the harness' Hindsight adapter) and the same judge.

```yaml
# docker-compose.bench.yml (excerpt)
services:
  hindsight-api:
    profiles: ["bench-hindsight"]
    image: ${HINDSIGHT_IMAGE:-vectorize/hindsight-api}:${HINDSIGHT_TAG}   # A-B5: pinned tag from hindsight/docker; recorded in bench.lock
    environment:
      HINDSIGHT_API_DATABASE_URL: postgresql://hindsight:hindsight@hindsight-postgres:5432/hindsight
      HINDSIGHT_API_LLM_PROVIDER: openai
      HINDSIGHT_API_LLM_BASE_URL: ${GATEWAY_OPENAI_BASE_URL}          # the gateway's OpenAI-compatible surface
      HINDSIGHT_API_LLM_API_KEY: ${GATEWAY_KEY}
      HINDSIGHT_API_LLM_MODEL: gpt-oss-120b                            # = bench.lock models.extract
      HINDSIGHT_API_EMBEDDINGS_PROVIDER: openai
      HINDSIGHT_API_EMBEDDINGS_OPENAI_MODEL: nomic-embed-text-v1.5      # A-B6: Hindsight's schema takes the dimension (768) at first migration
      HINDSIGHT_API_EMBEDDINGS_QUERY_PREFIX: "search_query: "
      HINDSIGHT_API_EMBEDDINGS_PASSAGE_PREFIX: "search_document: "
      HINDSIGHT_API_RERANKER_PROVIDER: tei                             # gateway rerank if TEI-compatible; else "local" (ms-marco MiniLM) and the report says so
      HINDSIGHT_API_EMBEDDINGS_TEI_URL: ${GATEWAY_RERANK_URL}
      HINDSIGHT_API_ENABLE_AUTO_CONSOLIDATION: "true"
      HINDSIGHT_API_LLM_TEMPERATURE: "0.1"
    depends_on: { hindsight-postgres: { condition: service_healthy } }
  hindsight-postgres:
    profiles: ["bench-hindsight"]
    image: pgvector/pgvector:pg16
    environment: { POSTGRES_USER: hindsight, POSTGRES_PASSWORD: hindsight, POSTGRES_DB: hindsight }
    healthcheck: { test: ["CMD", "pg_isready", "-U", "hindsight"], interval: 5s, retries: 20 }
```

Two Hindsight configurations are measured, because "Hindsight" alone is ambiguous:

| Config | Models | Why |
|---|---|---|
| **H-default** | Hindsight's shipped defaults where they are local (bge-small-en-v1.5 embeddings, ms-marco MiniLM reranker), LLM = the same `gpt-oss-120b` via the gateway | what a user gets out of the box |
| **H-matched** | everything through the gateway with `bench.lock` models (nomic 768-d with prefixes, `bge-reranker-v2-m3`, `gpt-oss-120b`) | the fair comparison: same models, so differences are pipeline differences |

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
paired statistics rather than single-run comparisons). *Improvement goals* (targets, not
gates, until phase 2 exit): temporal-reasoning **+3 pp** (temporal arm over `occurred_*`
windows + `as_of`), knowledge-update **+2 pp** (observation versions), R@5 ≥ **0.95** at the
fused stage, context tokens per question **−20 %** at equal accuracy (skip-not-truncate
packing + chunk headers), recall p95 **< 300 ms** at mid budget on a 10 M-fact shard (Hindsight
documents 100–600 ms). For reference, Hindsight's paper reports 83.6 % (GPT-OSS-20B) and
89.0 % (GPT-OSS-120B) on LME-S and 85.67 % (OSS-120B) on LoCoMo; the marketing figure of
94.6 % has an unverified configuration and is not used as a bar.

### 8.8 Cost and latency reporting

**Metering → cost.** Every gateway `Usage` passes through two hooks (§2.2.5): `quota.Meter`
writes `token_usage_events(usage_key, …)` and the rollup `token_usage(namespace_id, day, op, model,
price_version, prompt_tokens, completion_tokens, cost_micros)` in the shard DB (exactly-once by
`usage_key`, N25; upsert per `(namespace_id, day, op, model, price_version)`, in the same
transaction as the activity's commit where one exists, so it moves with the namespace, D13),
and `telemetry` increments `engram_llm_tokens_total{shard, tenant, op, model, kind}` and
`engram_llm_cost_micros_total{shard, tenant, op, model}`. `cost_micros` is computed at write
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
that sum / (facts created in the window / 1000). Expected values at the D3 defaults
(A-B4 prices): extraction ≈ $0.40 per 1 k facts, consolidation ≈ $0.30 per 1 k facts,
embeddings ≈ $0.01 per 1 k facts, recall ≈ $0.0009 per call (query embedding + rerank);
reflect ≈ $0.004–0.02 per call. Budget alerts fire on the `engram_llm_cost_micros_total`
rate per tenant against `quota.llm_tokens_per_day` (a deferral is the enforcement; the alert
is the early warning at 80 %).

**Latency histograms per stage.** `engram_recall_stage_seconds{shard, stage, budget}`
(buckets 1, 2, 5, 10, 20, 50, 100, 150, 200, 300, 500, 1000, 2000 ms) with `stage` ∈
{authz, catalog, embed_query, arm_semantic, arm_lexical, arm_graph, arm_temporal, arm_chunks,
fuse, rerank, boost_pack, stream}; `engram_retain_activity_seconds{shard, activity}`;
`engram_reflect_iteration_seconds{shard}`; `engram_rpc_duration_seconds{shard, service,
method}`. The `RecallStats` trailer carries the same per-stage numbers per request so a client
can attribute its own latency; the bench harness reads the trailer rather than timing
externally.

**Dashboards** (Grafana JSON checked in under `deploy/grafana/`): *Recall pipeline* (p50/p95
per stage stacked, rerank-skip rate, arm candidate counts, results per stage), *Retain
pipeline* (chunks/s per worker, cache hit rate, gateway RPM vs limit, deferred operations,
activity latency), *Cost* (USD per tenant per day, per op, per 1 k facts; forecast to month
end), *Bench* (weekly accuracy, R@5, tokens/question and cost/haystack over time, one line
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
