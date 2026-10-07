# Engram plan — adversarial review

Reviewer stance: principal engineer asked to find what is wrong, unrealistic, internally
inconsistent or hand-waved before anyone is staffed. Inputs: `00-decision-register.md`,
`sections/00–12`, `proto/**`, `sql/*.sql`, `formal/tla/*.tla` + `*.cfg` + `results/RESULTS.md`,
`formal/lean/Engram/*.lean`, `reference/hindsight-notes.md`, and the task brief. Every finding
cites the file and line/heading and quotes the claim it attacks. Findings are ranked most
severe first; severity is **blocker** (ships a wrong guarantee or cannot be built as written),
**major** (a stated number, invariant or schedule does not hold without a decision-level change),
**minor** (real but local), **nit**.

Counts: 4 blockers, 20 majors, 13 minors, 8 nits (45 findings).

---

## Findings

### F-1 — blocker — `as_of` time-travel resurrects observation text derived from deleted content after reconsolidation

**Where.** `sections/05-pipelines.md` §5.2.2 step 6.5 (line 633: "`UPDATE observation_versions SET stale_delete = false WHERE observation_id = $1`"); `sql/shard_schema.sql` lines 204–213 (trigger marks *every* version `stale_delete`); `00-decision-register.md` D16 ("from then on nothing from the document is returned by Recall") and D9/N33 (`superseded_at` filter); `sections/08-testing.md` §8.5 `TestAsOf_ObservationVersions`.

**Claim.** D16: "*Delete* acks after the synchronous cascade commits; from then on nothing from the document is returned by Recall, Reflect, GetMemory or Export." N41: an observation whose evidence was deleted "is hidden from recall until reconsolidated".

**Why it is wrong.** Hiding and un-hiding are per *observation*, while `as_of` serves per *version*. Interleaving: o has v1 (`effective_at` t1, inputs {f1}), v2 (t5, inputs {f1, f5}), v3 (t9, inputs {f1, f5, f9}). Delete f5's document → trigger sets `stale_delete` on o and on all three versions (hidden, correct). Consolidation rewrites o → v4 with `effective_at = max(t9, eff(v3)) = t9`; the apply path clears `stale_delete` on **all** versions (`UPDATE observation_versions SET stale_delete = false WHERE observation_id = $1`). Now `Recall(as_of = t7)` evaluates `effective_at <= t7 AND (superseded_at IS NULL OR superseded_at > t7)`: v2 (eff t5, superseded_at = t9) qualifies and is returned — its text was written with f5 in the prompt, after the delete was acknowledged. The `superseded_at`-clamp only protects v3 (eff = eff(v4)). `AsOf.tla` has no deletes and `DocLifecycle.tla` has unversioned observations, so neither spec can see this; the §8.5 test's chosen numbers (delete the *last* day) happen to make `superseded_at(v2) = effective_at(v3)` and hide the bug.

**Recommendation.** Make deletion-derived hiding a *per-version* permanent fact: the cascade/trigger sets `observation_versions.stale_delete` (or a new `derived_from_deleted`) only on versions whose `observation_inputs` row named the victim, and reconsolidation never clears it — only the new version is live; keep the `observations.stale_delete` flag as the "needs rewrite" signal. Add a `Delete` action and versioned observations to `AsOf.tla` (or `deriv` per version to `DocLifecycle.tla`) and re-run; add the interleaving above as a counterexample configuration and a Go twin.

### F-2 — blocker — the `as_of` cut-off is keyed on a model-chosen timestamp that may be backdated, so "leak-free" is not enforced by the system

**Where.** `sections/06-prompts.md` lines 242–243 ("mentioned_at: … Use an earlier value only when the chunk clearly quotes or forwards older material with its own date") and lines 316–318 (validation bounds only the *upper* side: "`mentioned_at ≤ item_timestamp + 5 min`"); `proto/memory/v1/memory.proto` `RetainItem.mentioned_at` ("Overrides `timestamp` … Must be ≤ now"); D9.

**Claim.** §4.4: "The guarantee `as_of = T` gives is therefore: *no fact, chunk, observation version or page version derived from content mentioned after T is returned*" and the harness treats this as leak-free evaluation.

**Why it is wrong.** For a leak-free evaluation the cut-off must be the time the *system learned* the fact (the item timestamp — Hindsight's τm comes from the item, the extractor never sets it). Engram lets the extraction model and the client move `mentioned_at` *earlier* than the item timestamp. A session dated day 30 that quotes "on day 3 Alice wrote …" yields a fact with `mentioned_at = day 3`, visible at `as_of = day 5` although the system only ingested it on day 30. The evaluator's `leak_canary` (`mentioned_at > as_of`) counts zero. The chunk arm uses the item timestamp, the fact arm uses the model's value, so the two arms even disagree on what "as of T" means.

**Recommendation.** Split the two notions: `knowledge_at` (= item timestamp, server-set, the only `as_of` key, on facts, chunks and as the `effective_at` input) and `said_at` (model/client-supplied, display and temporal arm only). Reject client `mentioned_at` overrides that precede `timestamp` unless a separate `knowledge_at` is also supplied, and never let the extractor move the `as_of` key. Update D9, §4.4, the `RecallStats.as_of_applied` echo and the leak canary accordingly.

### F-3 — blocker — the move's catch-up reads `seq > $applied` under a namespace-confined role, so an out-of-order commit is skipped; the spec assumes knowledge the mover cannot have

**Where.** `sections/03-data-model.md` line 965 and lines 1343–1348 ("source `SELECT … FROM outbox WHERE namespace_id = $1 AND seq > $applied ORDER BY seq LIMIT 500`"); `sections/05-pipelines.md` §5.5.1 step 3 (line 1233: "the replay reads `seq > p0`"), step 5(a) ("replay until `move_applied_seq = max(seq)`"); `formal/tla/ShardMove.tla` lines 190–202 (`Resolved(s, n, k) == ∀ w ∈ Holding(s, n) : winfo[w].seq > k`, which the mover is assumed to evaluate); `sql/shard_schema.sql` line 32 (`engram_move` is `NOBYPASSRLS, confined by the ordinary ns_isolation policy`).

**Claim.** §5.5.1 step 3: "it reads with its own cursor through the shared `outbox.Reader` (gap watchlist included, §5.6)" and §3.3.1: "a replay that arrives out of order (a source gap that resolves late) is applied exactly once".

**Why it is wrong.** Interleaving, all before Freeze: writer W1 (namespace A) draws seq 102 in its last statement; W2 (A) draws 103 and commits; the mover reads `seq > 101` → {103}, applies, `move_applied_seq = 103`; W1 commits 102 (legal, A is `active` at epoch e); Freeze waits for holders (none left); Drain reads `seq > 103` → nothing, `max(seq) = 103 = move_applied_seq` → "drained". Seq 102 is never replayed. The gap watchlist cannot help: under RLS the mover sees only A's rows, and a hole between A's seqs is indistinguishable from another namespace's seq, so it has no "gap" to watch. The spec's `Resolved` predicate inspects every holding write of the shard — exactly the shard-wide knowledge `engram_move` is denied. The only safety net is `Verify` (count mismatch → rollback), which turns every move of a namespace with concurrent writers into a rollback.

**Recommendation.** Drop the watermark read. After Freeze (no holders, `max(seq)` final), Drain must anti-join: `SELECT … FROM outbox o WHERE namespace_id = $1 AND seq > p0 AND NOT EXISTS (SELECT 1 FROM move_applied m WHERE m.namespace_id = o.namespace_id AND m.seq = o.seq)` — cheap on `outbox_ns_seq_idx`. During catch-up use the same anti-join per round (rounds are bounded at 10). Alternatively grant `engram_move` the relay's shard-wide `SELECT` on `outbox` and bound the cursor by the relay's safe cursor (the ND-5 rejected option). Amend `ShardMove.tla`: `Replay` must not depend on `Holding` of other writers; add the interleaving above as `ShardMove_WatermarkOnly.cfg`.

### F-4 — blocker — `COPY … FROM STDIN` is refused on row-level-security tables, so the move's target copy cannot run as `engram_move`

**Where.** `sections/05-pipelines.md` §5.5.1 step 2 (lines 1216–1219: "`COPY (SELECT … WHERE namespace_id = $1 ORDER BY <pk>) TO STDOUT BINARY` into the target, `COPY … FROM STDIN BINARY` in batches of 10 k rows, each batch its own target transaction fenced by `state = 'incoming'`"); `sql/shard_schema.sql` lines 1101–1119 (every namespace table has RLS ENABLED + FORCED) and line 56 (`engram_move` is `NOBYPASSRLS`).

**Claim.** The copy phase streams every table with `COPY FROM STDIN BINARY` under the ordinary RLS policy ("the policy is a second fence for free").

**Why it is wrong.** PostgreSQL's `COPY` reference states: "Currently, COPY FROM is not supported for tables with row-level security. Use equivalent INSERT statements instead." The server raises `COPY FROM not supported with row-level security` for any role subject to the policy. With `FORCE ROW LEVEL SECURITY` even the owner is subject to it. The target copy as designed fails on the first table; `pgx.CopyFrom` uses the same protocol and fails the same way. (`COPY … TO STDOUT` on the source is fine.)

**Recommendation.** Decide one of: (a) perform the target load with multi-row `INSERT … VALUES` (or `INSERT … SELECT FROM unnest($arrays)`) in 10 k-row batches — slower than COPY but RLS-checked, keep the `incoming` fence; (b) load with a `BYPASSRLS` role (`engram_admin`) and keep the second fence only on the *source* side; or (c) temporarily `ALTER TABLE … NO FORCE ROW LEVEL SECURITY` is not an option (tables are shared). Update §5.5.1, N2, A-O17 ("copy at ≈ 50 k facts/s") and the M1.5 estimate.

### F-5 — major — every write transaction `FOR SHARE`-locks one hot row; a continuous stream of share lockers can starve the move's `FOR UPDATE` barrier and freeze, and it churns multixacts

**Where.** D2 row 3 (`SELECT 1 FROM namespace_ownership … FOR SHARE`); `sections/05-pipelines.md` §5.5.1 step 2 ("it waits for every in-flight writer … and blocks new writers for the few milliseconds it is held") and step 4 ("The `UPDATE` waits for every in-flight writer holding `FOR SHARE` … ≤ `statement_timeout` = 30 s"); `formal/tla/ShardMove.tla` line 209–211 (`Freeze` is *enabled* only when `Holding = {}`).

**Claim.** The barrier and the freeze "wait for every in-flight writer … and block new writers".

**Why it is risky.** Postgres row locks are not a fair queue for *compatible* lockers: a new `FOR SHARE` request whose mode does not conflict with the current holders is granted without taking the tuple lock, so it bypasses a waiting `FOR UPDATE`. A namespace with overlapping writers (32-way `CommitChunk` fan-out plus consolidation) can keep the row share-locked indefinitely; the `FOR UPDATE` barrier and the `UPDATE … SET state='frozen'` wait until a gap appears. The TLA+ model hides this: `Freeze` is simply not enabled while writers hold, and `MoveTerminates` passes only because `Writes` is a finite pool. In production the 120 s watchdog rolls back exactly the hot namespace you were moving (R5 is scored on replay rate, not on this). Secondary cost: each additional share locker on an already-locked row allocates a new MultiXactId; at shard write rates this is a known SLRU hot spot (`pg_multixact`), with wraparound vacuum pressure.

**Recommendation.** Fence with heavyweight locks, which queue fairly (a conflicting waiter blocks later compatible requesters): writers take `pg_advisory_xact_lock_shared(hashtextextended(namespace_id::text, 0))` plus a plain `SELECT state, epoch FROM namespace_ownership` (no row lock); the barrier/freeze/export take the exclusive advisory lock. Keep the row as the fence *value*, not the fence *lock*. Amend D2, §3.3, §5.0, §5.5 and model the queueing discipline (or at least the starvation) in `ShardMove.tla`.

### F-6 — major — the cross-encoder is the largest term in the latency budget and is never sized for throughput; 120 ms p95 at 240 k pairs/s per cell is an assumption about a GPU fleet nobody has provisioned

**Where.** D3 ("cross-encoder on 150 pairs ≤ 120 ms p95 … (A-2: gateway rerank latency)"); D15 (`bge-reranker-v2-m3`); `sections/01-architecture.md` §1.5 table line 373; `sections/11-risks.md` R3; `sections/10-roadmap.md` M1.2 exit (p95 < 300 ms at 50 QPS/shard).

**Claim.** "Cross-encoder rerank on top 50/150/300 | 120 ms | skipped if remaining deadline < 150 ms".

**Why it is risky.** Recompute: 50 QPS/shard × 32 shards = 1 600 recalls/s per cell × 150 pairs = 240 k query–document pairs/s (750 k/s fleet-wide at 100 shards). `bge-reranker-v2-m3` is a 568 M-parameter XLM-R-large cross-encoder; a single L4/A10-class GPU sustains on the order of 1–3 k pairs/s at ~150-token pairs, so the cell needs ~100 GPUs just for rerank to stay at 120 ms p95 under load, and p95 degrades sharply under queueing. Hindsight ships a 22 M-parameter MiniLM-L6 reranker for a reason. The plan's only mitigation (skip rerank, `stage=FUSED`) silently turns the headline quality feature off whenever load rises, and the SLO counts a FUSED answer as success.

**Recommendation.** Make the reranker a sized dependency: state the pairs/s the gateway must sustain per cell as an A-n assumption with the GPU count it implies; set `rerank_top.mid = 50` (not 150) until Q1 measures the gateway; evaluate MiniLM-L6/`bge-reranker-base` as the default with v2-m3 as a per-namespace upgrade; and report the rerank-skip rate as an SLO breach, not a degradation.

### F-7 — major — the p95 budget counts the five arms as one 60 ms stage, but the graph arm is serialised behind embedding and the seed arms; the real critical path is ≈ 276 ms

**Where.** `sections/01-architecture.md` §1.5 lines 357–376 ("the graph arm … starts when both have returned"; table row "5 arms in parallel … 60 ms"; "**Total** ≈ 215 ms"); `sections/02-modules.md` §2.2.13 ("`NeedsSeeds` starts when semantic and lexical have returned or timed out").

**Claim.** "5 arms in parallel (per-arm caps …) | 60 ms" and "≈ 215 ms".

**Why it is wrong.** By the planner's own scheduling the dependency chain is authz 2 → embed 25 → semantic (≤ 60) → graph (≤ 60) → fuse 1 → rerank 120 → pack 3 → stream 5 = **276 ms** of p95 budgets, not 215; the 300 ms target has 24 ms of headroom and the "skip rerank if remaining < 150 ms" rule fires on most p95 requests under a 300 ms client deadline. The §4.8 example (graph 40 ms, rerank 95 ms, total 231 ms) is a p50-ish sample presented next to a p95 budget.

**Recommendation.** Either seed the graph arm from the lexical arm immediately (lexical needs no embedding) and from semantic as a second wave with its own 30 ms sub-budget, or cap graph at 30 ms at mid; restate the budget as a critical-path sum and make M1.2's synthetic p95 measurement explicit about rerank-skip rate (< 1 % at target QPS).

### F-8 — major — filtered HNSW over a 16-partition shard truncates candidates for small namespaces and costs up to 20 k heap fetches per arm; the common tenant is the small one

**Where.** D3 (`hnsw.ef_search=100`, `hnsw.iterative_scan=relaxed_order`, `hnsw.max_scan_tuples=20000`); `sections/03-data-model.md` §3.8 lines 1131–1150 ("With `relaxed_order` the scan keeps walking the graph until 150 rows pass the filter or `max_scan_tuples` is reached, which is what makes a selective `as_of` or tag filter return a full candidate list instead of a truncated one"); §3.1 principle 3 (16 hash partitions).

**Claim.** The partition-pruned, filtered HNSW scan returns a full per-arm candidate list within the 60 ms arm budget.

**Why it is wrong.** A partition holds ~1/16 of the shard: ≈ 625 k facts spread over ~9–10 namespaces of wildly different sizes. For a namespace with 1 000 facts the `namespace_id` filter is 0.16 % selective; collecting 150 passing rows needs ≈ 94 k visited tuples, far above `max_scan_tuples = 20 000`, so the semantic arm returns ~30 candidates, not 150 — and `as_of`/tag filters multiply the problem. Every visited tuple is a heap fetch (the index holds only the vector; `live`, `namespace_id`, `mentioned_at` live in the heap), so the arm costs up to 20 k random heap reads: ~20 ms warm, seconds cold against the "NVMe ≥ 10 k IOPS" assumption. 10 000 tenants × 100 k memories averages 67 k facts per namespace across 150 namespaces per shard; the median namespace is small. Hindsight solved exactly this with per-bank partial vector indexes (`VECTOR_INDEX_MIN_ROWS`).

**Recommendation.** Plan by namespace size: for namespaces below a threshold (≈ 20 k facts) run an exact scan on `(namespace_id)` + `live` with `embedding <=> $q` (≈ 30 MB of halfvec, 5–15 ms); for large namespaces build a partial HNSW (`WHERE namespace_id = …`) at creation of a "large" flag, with the shared partition index as fallback. Put IOPS in the D3 sizing (heap fetches per recall × QPS) and set `ef_search ≥ cap` (400 at HIGH).

### F-9 — major — recording every prompt input (≈ 210 facts per version) and hiding on any lost input makes one document delete blank out thousands of observations for hours

**Where.** N41/`sections/05-pipelines.md` §5.4.1 step 9 ("every other affected observation `stale_delete` … **hidden from recall** … until a consolidation round rewrites them"); `sections/03-data-model.md` §3.7 line 1033 ("0.75 M versions × ≈ 210 inputs"); §5.2.2 step 1 ("≤ 25 stale observations" per round) and "Caps" (≈ 60–120 s per round).

**Claim.** Hiding until reconsolidation is a sound, bounded consequence of D16.

**Why it is risky.** Recompute the blast radius with the plan's own numbers: 0.75 M versions × 210 inputs / 10 M facts ≈ 16 versions name each fact on average; deleting a 100-fact document (one chat session) hides ≈ 1 600 observations. Reconsolidation handles ≤ 25 stale observations per round at 1–2 min per round → the observation layer of that namespace is dark for 1–2 hours after a routine session delete, and `Reflect`'s forced `search_observations` returns nothing meanwhile. Hindsight deletes only dependent observations and keeps the rest. The input set is inflated because *every* shown candidate's quoted sources count as inputs even when capped at 5 per candidate (8 + 10 × 5 = 58 would already be 3.6× smaller than 210).

**Recommendation.** Keep N41's principle but narrow the surface: inputs = batch facts ∪ sources whose *quotes were rendered* in the prompt (N47 cap), and hide per version (F-1) rather than per observation so the previous live version written without the victim stays servable; raise the stale batch share (hidden-first, up to the full 100-fact round) and make the delete cascade nudge consolidation with `reason = delete` at priority. Record the expected hidden-count per delete as a metric and an SLO.

### F-10 — major — concurrent `APPEND`s lose one append; "resolved by version order" means the later finaliser retires the earlier append's chunks

**Where.** `sections/05-pipelines.md` §5.1.1 step 4.3 (line 132: "For `APPEND`, `append_base_version = current_version` is recorded") and §5.1.2 step 2 (lines 165–168: "if `append_base_version ≠ current_version` at `LoadItem` time the item is still chunked against the recorded base — a concurrent replace is resolved by version order, never by rejecting"); §5.1.2 step 7.5 (the newest version retires every live chunk not in its set); `reference/hindsight-notes.md` §1 (`append_base_hash` + `ConcurrentAppendConflict`).

**Claim.** Version order resolves concurrent versions without loss.

**Why it is wrong.** Two appends A and B submitted 1 s apart both record `append_base_version = c` (the active version) and get v1 < v2. v1 chunks `body(c) ‖ A`, v2 chunks `body(c) ‖ B`. v2 finalises last → it is the newest, computes the retire set as "live chunks not in v2" and retires A's chunks; the document now reads `body(c) ‖ B`. Append A was acknowledged and is gone. The task brief's `update_mode (replace/append)` semantics and Hindsight's monotonic guard both forbid this; the plan trades a rejection for silent loss.

**Recommendation.** For `APPEND`, assign the base at `LoadItem` under the `documents` row lock as the *highest existing version* (chain appends: v2's base is v1's body even while v1 ingests), or reject the second concurrent append with `ABORTED + OperationConflict{DOCUMENT_INGESTING}` as Hindsight does. State which in D8 and add the interleaving to `DocLifecycle.tla` (it currently models only REPLACE sets).

### F-11 — major — the consolidation `update` path fires the zero-sources trigger before the new sources exist, retiring the observation inside its own update

**Where.** `sections/05-pipelines.md` §5.2.2 step 6.5 lines 626–634 ("`DELETE FROM observation_sources WHERE observation_id = $1 AND memory_id <> ALL($new)` (the evidence trigger fires and marks the row `stale_delete`, which the next statement clears inside the same transaction); `INSERT observation_sources … ON CONFLICT DO NOTHING`; `UPDATE observations SET … stale_write = false, stale_delete = false …`"); `sql/shard_schema.sql` lines 189–215 (`engram_observation_sources_after_delete` sets `retired_at` on the observation **and on every `observation_versions` row** when no source remains).

**Claim.** The trigger's side effect is only `stale_delete`, cleared by the next statement.

**Why it is wrong.** An update whose new source set is disjoint from the old one (the normal case for "STATE CHANGES — UPDATE CONCISELY": a move, a new job) deletes all old sources first; at that instant `NOT EXISTS (observation_sources …)` is true, so the trigger retires the observation and all its versions. The subsequent statements insert sources and clear `stale_*` but never clear `retired_at`, and the just-inserted version row (inserted *before* the delete in the listed order) is retired too. Net effect: every source-replacing update silently deletes the belief. The `ApplyBatch` ordering in §5.2.3 and the Go `Applier` comment repeat the same order.

**Recommendation.** Insert the new `observation_sources` rows first, then delete the stale ones (the trigger then sees a non-empty set), or wrap the pair in `SET LOCAL engram.suppress_evidence_trigger = on` checked by the trigger. Add a T3 test "update with disjoint sources keeps the observation live"; this is the kind of statement-order bug the model-based tests should have been derived from (the TLA+ `Effect` applies `src := S` atomically and cannot see it).

### F-12 — major — a prompt or model bump never retires the old-key facts of a kept chunk, so re-extraction doubles live facts

**Where.** `sections/06-prompts.md` §6.0 lines 27–35 ("`FinalizeVersion` retires the v1 facts and activates the v2 ones"); N26; `sections/05-pipelines.md` §5.1.2 step 5.5.4–5 (chunk `ON CONFLICT … DO UPDATE SET … extraction_key = EXCLUDED.extraction_key`; un-retire `WHERE extraction_key = $current`; insert new facts) and step 7.5 (retire set = "`content_hash NOT IN (SELECT content_hash FROM document_version_chunks WHERE version = v)`", facts "`WHERE chunk_id IN (those)`").

**Claim.** Re-extraction after a bump replaces facts "never in place" via the ordinary retain path.

**Why it is wrong.** A `stale_extraction` chunk keeps its `chunk_id` (the row is updated in place) and *is* in version v's set, so step 7.5 does not retire it; its old facts (old `extraction_key`, still `retired_at IS NULL`) are untouched by any listed statement. After `engramctl reextract` every chunk carries two live fact sets (v1 and v2 extraction), both recallable and both consolidated. Nothing in §5.1.2's SQL mentions `extraction_key` in the retire set.

**Recommendation.** Add to `FinalizeVersion`: `UPDATE facts SET retired_at = now(), purge_after = now() + 1 h WHERE namespace_id = $1 AND document_id = $2 AND retired_at IS NULL AND extraction_key <> (SELECT extraction_key FROM chunks c WHERE c.chunk_id = facts.chunk_id)`, emit `ChunksRetired` for them, and cover it in the `TestDocLifecycle_*` family with a prompt-version change.

### F-13 — major — acknowledged deletes keep serving content through existing export snapshots, Temporal histories and consolidation blobs

**Where.** D16 line 172 ("from then on nothing from the document is returned by Recall, Reflect, GetMemory or Export"); `sections/01-architecture.md` §1.6 ("or a *new* export"); `sections/05-pipelines.md` §5.7 step 4 (retention "the last 3 FULL versions and every delta"); `proto/memory/v1/export.proto` `StreamSnapshot` (serves any listed version); `sections/03-data-model.md` §3.6 (`consolidate/{batch_key}.json` "raw LLM ops … 30 d sweep"; Temporal payloads carry `ExtractChunkResult` text inline, N13); `sections/09-operations.md` §9.1 (Temporal namespace retention 7 days, R22).

**Claim.** Delete is invisible everywhere from the ack.

**Why it is wrong.** `StreamSnapshot(version = n−1)` returns the deleted document's facts and chunks until the snapshot expires (days to weeks); the manifest is immutable and nothing in the cascade marks snapshots stale or expired. Temporal histories hold extracted fact text and embeddings for 7 days after the delete and are browsable in the Temporal UI; `consolidate/*.json` holds observation text derived from the facts for 30 days. R11 only covers Postgres backups. For a right-to-erasure request these are three undocumented copies.

**Recommendation.** On delete, insert `blob_tombstones` for every `export/v{n}/` of the namespace (or mark `export_snapshots.state = 'expired'` and refuse `StreamSnapshot` for them, forcing a full re-sync); pass extraction results by blob key always (not inline) so Temporal histories carry only keys, and run a Temporal payload codec with a per-shard key; lower the `consolidate/` blob retention to the purge grace. State all three in the deletion SLA (§9.3).

### F-14 — major — the chunk header embeds the document summary, so every `APPEND` (every chat turn) re-summarises and re-embeds the whole document

**Where.** D11 ("each chunk gets a header `[doc summary ≤ 200 chars] > [heading path]` prepended for embedding"); `sections/05-pipelines.md` §5.1.2 step 3 (summary cached by `document_hash`, which changes with every version) and step 7.5 lines 335–339 (re-embed every member chunk whose `header_hash` changed); `sections/02-modules.md` §2.2.9 line 729 ("recomputed only if the header changed materially" — "materially" is an exact hash compare in §5).

**Claim.** Delta retain makes unchanged content cost nothing ("an unchanged chunk costs nothing").

**Why it is wrong.** The primary workload (Hindsight's and the brief's) is conversation memory appended turn by turn. Each append is a new version with a new `document_hash` → a new summary call (cache miss by construction) → a different ≤ 200-char summary → different `header_hash` on *every* chunk → `ReembedChunk` for all N chunks of the document. A 150-chunk conversation appended 100 times pays 100 summary calls and ≈ 15 000 chunk re-embeddings (plus their facts: "`UPDATE facts SET embedding WHERE chunk_id …`" re-embeds every fact) for content that never changed. The §6.8 cost table assumes one summary per document.

**Recommendation.** Make the header stable: recompute the summary only when the document grows by > 25 % (or on REPLACE), exclude the summary from `header_hash` (keep the heading path), or embed the header only into the chunk vector and not the facts'. Add "re-embeddings per append" to the §8.6 metrics and the cost model.

### F-15 — major — the formal story overstates what TLC checked: the DocLifecycle and Consolidation *design* safety configurations never completed on the final specs, and three modelling gaps hide the properties the prose relies on

**Where.** `sections/07-formal-verification.md` §7.1 table lines 33 and 38 ("Full bounds: **INCOMPLETE** …; `Consolidation_Mid.cfg`: **NOT RUN**"; "`DocLifecycle_TxMid.cfg` on the fixed spec was still running and `DocLifecycle_Mid.cfg` **NOT RUN**"), line 44 ("every design configuration that finished passed"), `sections/00-front-matter.md` ("Correctness is checked, not asserted"); `formal/tla/AsOf.tla` line 72 (`deriv == batch ∪ UNION {Latest(c).deriv : c ∈ cands}` with `cands ⊆ Obs \ {o}`); `formal/tla/DocLifecycle.tla` lines 205–213 (`ApplyCheck = "batch"` commits `src = snap = deriv`); `formal/tla/ShardMove.tla` lines 179–185 (`Copy` atomic) vs `sections/05-pipelines.md` §5.5.4 (per-table restart with `min(p0_old, p0_new)` floor).

**Claim.** Five TLA+ specifications are "tied to Go property tests"; the register's D19 corrections are "exposed by" TLC; N46 runs full bounds nightly under a 30-minute cap.

**Why it is wrong.** (1) The only DocLifecycle/Consolidation design configurations that completed are `_Live` (1 doc × 2 hashes × 1 obs; 2 facts × 1 obs). `Consolidation.cfg` had 13.3 M states queued at depth 9 after 560 s on 4 cores — a 30-minute cap on 8 cores will not finish it either; "nightly" will be permanently red or silently skipped. (2) `AsOf.tla` does not put `o`'s own previous version into `deriv` for an update, so the "monotone clamp" (D9: "`effective_at(v−1)`") is neither modelled nor verified, although §5.2.2 cites TLC for it. (3) In the DocLifecycle design configuration the apply commits *every* seen fact as a source, so `cited ⊂ inputs` with an input-only cascade — the actual N41 mechanism — is never exercised; the counterexample shows cited-only fails, not that the design passes. (4) `ShardMove.tla` models `Copy` as one atomic snapshot; the crash-recovery claim in §5.5.4 (tables copied under different snapshots, replay from the smaller `p0`) is unmodelled. (5) F-3 and F-5 show two places where the spec assumes an ability (`Resolved`, fair `Freeze`) the implementation lacks.

**Recommendation.** Rewrite §7.1 and the executive summary to say exactly which configurations passed; shrink the design bounds until they complete (`Docs={d1,d2}, Hashes={h1,h2}, Obs={o1,o2}, MaxConsolidations=2` is enough for the races named) and make *those* the CI gate; add `Latest(o).deriv` and `eff(v−1)` to `AsOf.tla`; add a `cited ⊂ snap` apply variant with input-based `Delete` to `DocLifecycle.tla`; model multi-snapshot copy and the `Holding`-independent replay in `ShardMove.tla`.

### F-16 — major — 74 engineer-weeks is a lower bound presented as a plan; E2's serial chain alone overruns the calendar by ≈ 9 weeks

**Where.** D17; `sections/10-roadmap.md` §10.2 (M1.5 "move protocol … 5.0" ew with the full crash-mid-move table as exit; M2.1 6.0 + M2.2 5.0 assigned to E2 "weeks 13–17"; M3.1 4.0 + M3.2 3.0 to E2 "weeks 18–22"), §10.3 gantt (m21 4 w, m22 3 w, m31 3 w, m32 2 w), §10.4 ("Everything else floats"), F.1 ("3.0" ew for two specs, TLC configs, trace validators and the manifest check).

**Claim.** "≈ 74 engineer-weeks, 3 engineers ≈ 6 months" with "≈ 4 ew (5 %) of slack".

**Why it is wrong.** Arithmetic: E2 owns M2.1 (6) → M2.2 (5) → M3.1 (4) → M3.2 (3) = 18 ew of strictly sequential work starting week 13, finishing week 31, not 22; the gantt compresses 11 ew into 7 calendar weeks for one person. Phase 1 has zero slack (30 ew over 10 weeks × 3). M1.5 — a cross-database move protocol with crash injection at ten points under load, rollback at every phase, catalog + two shard containers, `engramctl` and schema-version refusal — is 5 ew; comparable protocols take 8–12 ew to get the chaos table green. F.1's trace-to-TLA converter plus five rapid state-machine harnesses against real Postgres is a project, not 3 ew. None of the open questions with schedule impact (Q1 rerank latency, Q2 blob credentials, Q5 multi-host, Q9 pgx/pgbouncer) has a contingency. F-4, F-5, F-8 each add weeks.

**Recommendation.** Re-estimate at 110–140 ew or cut committed scope: ship Phase 1 + 2 with moves behind an admin flag and no multi-cell/export/pages in the committed plan; put Phase 3 and the formal trace-validation tooling on a separate, unstaffed track. Re-draw the gantt from per-engineer serial chains, not from phase totals.

### F-17 — major — facts-per-chunk, consolidation call counts and cost per haystack are mutually inconsistent across §3, §6, §8 and §10

**Where.** `sections/03-data-model.md` §3.7 A-4 ("1 M live chunks (10 facts per chunk)"); `sections/06-prompts.md` §6.8 line 823 ("≈ 20 facts + 1 chunk" embeddings per chunk) and line 827 ("150 extracts + ≈ 40 consolidation batches … **$0.14**"); `sections/06-prompts.md` §6.2 prompt (selective, "fewer, better facts" — Hindsight's paper targets 2–5 facts per chunk); `sections/08-testing.md` lines 441 ("Cost / haystack (ingest) $0.061") and 449–453 ("≈ 76 k extraction calls (≈ 115 M prompt + 25 M completion tokens ≈ $32) … consolidation ≈ $25 → ≈ $50–80 per full LME-S run"); `sections/10-roadmap.md` line 68 ("ingest cost ≤ $0.10 per LME-S haystack").

**Why it does not add up.** At 10 facts/chunk a 150-chunk haystack yields 1 500 facts = 188 consolidation batches, not 40 (≈ 4.7×); at the §6.8 20 facts/chunk, 375. Extraction prompt tokens: 76 k calls × (1 500 system + ≈ 900 variable) ≈ 182 M, not 115 M (the 115 M counts only content); extraction ≈ $27 + $23 output ≈ $50, summaries 20 k × 2 300 × $0.15/M ≈ $7 (not $3), consolidation $13–$67 depending on facts/chunk → $70–$130 per LME-S run, not $50–80. Per haystack: §6.8 says $0.14, the report template $0.061, the Phase-2 gate $0.10 — the gate is below the plan's own estimate. Throughput: the extra consolidation calls add 2–6 h at 600 RPM to the "≈ 6 h" run.

**Recommendation.** Fix one facts-per-chunk assumption (measure on 100 LME chunks in Phase 0, Q1-style), derive every count and cost from it in one table that §3.7, §6.8, §8.6 and §10 cite, and set the M2 gate from the measured number with a margin.

### F-18 — major — Temporal history and payload limits are exceeded by the retain workflow as specified

**Where.** `sections/05-pipelines.md` §5.0 line 70–72 ("continue-as-new before their history reaches 10 k events (retain: every 500 chunks …)"); §5.1.2 step 5.2 and N13 ("spill to a staging blob above 512 KiB"); `sections/02-modules.md` §2.2.17 lines 1162–1164 ("intermediate outputs larger than 64 KiB … passed by blob key"); `proto/engram/internal/workflow/v1/workflow.proto` `CommitChunkInput` (embeds extraction, embeddings, entities and links again); R12.

**Why it does not add up.** Per chunk the history carries ≥ 5 activity scheduled/started/completed triples plus the fan-out commands (≈ 25–30 events) → 500 chunks ≈ 13–15 k events, above the plan's own 10 k rule. Bytes: a 50-fact chunk is ≈ 150 KiB of embeddings (inline below 512 KiB) + ≈ 60 KiB of extraction text, and `CommitChunkInput` repeats both → ≈ 400 KiB per chunk in history; 500 chunks ≈ 200 MB, 4× Temporal's 50 MB history-size limit (default `limit.historySize.error`). The two spill thresholds (64 KiB vs 512 KiB) contradict each other.

**Recommendation.** Pass every activity result ≥ a few KiB by blob key (extraction, embeddings, links), make `CommitChunkInput` keys-only, and continue-as-new by a byte/event budget (e.g. every 100 chunks or 20 MB), measured by the SDK's history-size metric in T2.

### F-19 — major — the synchronous delete cascade is unbounded in document size and will exceed both the 500 ms SLO and the 30 s writer `statement_timeout`

**Where.** `sections/05-pipelines.md` §5.4.1 (one transaction: retire all facts, `DELETE FROM fact_links … IN victims`, mentions, sources, inputs, pages); `sections/09-operations.md` SLO table line 597 ("Delete cascade latency p95 < 500 ms"); `sections/02-modules.md` §2.2.7 `TxOptions` (30 s writer statement timeout); `sections/03-data-model.md` §3.7 ("a 100 k-fact document delete spreads over ~10 s" — for the *purge*, but the cascade is synchronous).

**Why it is wrong.** A 100 k-fact document (a long transcript, a code repository dump) means retiring 100 k facts and deleting ≈ 3 M `fact_links` rows (30/fact, both directions via the reverse index) plus 200 k mentions in one statement group — tens of seconds on a loaded shard, past the 30 s `statement_timeout`, so the delete *fails* and the document cannot be deleted at all through the API. Links need not be removed synchronously: every arm joins `facts` and filters `live` (the plan says so for invalidate, §5.4.5), so dangling links are already harmless to recall.

**Recommendation.** Keep synchronous only what visibility needs (facts/chunks `retired_at`, `observation_sources/inputs` removal with the trigger, page flags, `deletion_log`, outbox) and move `fact_links`/`entity_mentions` deletion to `PurgeDocument`; restate `NoOrphanLinks` as "no link is traversed to or from a retired fact" (the graph arm's join), and size the cascade SLO per 1 k facts.

### F-20 — major — between cutover sub-steps (c) and (d) every read of the namespace fails, and a mover crash there makes reads unavailable until Temporal retries the activity

**Where.** `sections/05-pipelines.md` §5.5.1 step 7 ("With (c) first, every request that reaches the source between (c) and (d) fails `WrongShardOrEpoch`, which the API already retries once with a fresh resolve; nothing can be stale because nothing is served"); §5.5.4 ("`Cutover` after (c) … every request for the namespace fails `WrongShardOrEpoch` … retry does (d) within seconds"); `sections/01-architecture.md` §1.3 table ("re-resolves **once** … A second `WrongShardOrEpoch` is returned to the client").

**Why it is risky.** The single re-resolve asks the catalog, which still names the source at epoch e until (d), so the retry fails identically and the client gets `FAILED_PRECONDITION`. D5 step 4 promised "Reads continue at source" during the freeze; the amended order breaks reads for the (c)–(d) window, which is milliseconds when healthy and unbounded when the mover's worker dies after (c) (activity retry `P-catalog` 500 ms → 5 s, 20 attempts; a fleet-wide worker outage leaves the namespace unreadable). The Recall availability SLI excludes `FAILED_PRECONDITION`, so the outage is invisible to the SLO.

**Recommendation.** Treat `WrongShardOrEpoch{namespace_state = MOVED_OUT}` on *reads* like `NamespaceFrozen`: bounded re-resolve loop (≤ 5 s) rather than once; run (b)–(d) as three activities with sub-second retry and a dedicated "cutover in progress" alert; count `FAILED_PRECONDITION` during moves in the availability SLI.

### F-21 — major — the catalog is a fleet-wide single point of failure after 10 minutes, although the design proves staleness is safe

**Where.** D4 line 54 ("serve cached entries up to `stale_max = 10 min`; a cache miss returns `UNAVAILABLE`"); `sections/01-architecture.md` §1.3 table line 193 ("a miss (or entry older than 10 min) → `UNAVAILABLE` … Correctness is unaffected because the shard's ownership row, not the cache, decides writability"); §1.8 failure table ("after 10 min everyone"); `sections/09-operations.md` §9.1 ("promoted by the runbook, never automatically (A-O4)"), §9.3 ("Catalog RTO ≤ 15 min").

**Why it is risky.** The two sentences contradict each other: if the shard fence makes staleness safe, expiring entries after 10 minutes buys nothing and converts a catalog outage longer than the manual promotion time into an outage of every namespace on 100 shards (reads included — reads do not need the catalog for correctness either, only for `deleting` state and quotas). A manual-only replica promotion with a 15-minute RTO against a 10-minute stale budget is a guaranteed fleet outage on every catalog primary failure that happens outside office hours.

**Recommendation.** Serve stale entries indefinitely for *existing* namespaces (raise a gauge, degrade `deleting`/quota freshness), fail only misses; make `WrongShardOrEpoch` the sole correction path (it already is); automate catalog failover or at least make the API tolerate hours of catalog absence. Keep 10 min only for negative entries.

### F-22 — major — the `tenant` label on metering counters at 10 000 tenants is a cardinality explosion

**Where.** D13 ("Prometheus metrics labelled `shard` (always) and `tenant` (metering counters only)"); `sections/09-operations.md` §9.4 lines 558–560 (`engram_llm_tokens_total{shard, tenant, op, model, kind}`, `engram_llm_cost_micros_total{shard, tenant, op, model}`, `engram_quota_events_total{shard, tenant, kind, action}`); line 539 ("cardinality is bounded by `#shards × #methods`").

**Why it is wrong.** Recompute per worker process: tenants with activity in the window (up to 10 000) × shards they touch (≥ 1) × 8 ops × ~5 models × 2 kinds ≈ 800 k series for the tokens counter alone, ×3 counters, per process, per cell — far beyond what a single Prometheus (and the fleet Prometheus that "also scrapes every cell") tolerates; the stated bound ignores the tenant dimension entirely.

**Recommendation.** Drop `tenant` from Prometheus; metering already lives exactly-once in `token_usage` (N25) and is reported by `engramctl report`. If a live per-tenant signal is required, emit it as an OpenTelemetry metric with delta temporality to a system built for it, or keep a top-N exemplar label.

### F-23 — major — the move/restore `Restart` and the store's epoch handling rely on `namespace_ownership` triggers and grants that the DDL contradicts

**Where.** `sections/05-pipelines.md` §5.5.1 step 4 line 1252–1254 ("`UPDATE namespace_ownership SET state = 'frozen' WHERE namespace_id AND epoch = e AND state = 'active'`"); `sql/shard_schema.sql` line 278 (`CHECK (state <> 'frozen' OR freeze_reason IS NOT NULL)`) and lines 1148–1150 (`engram_app` gets `UPDATE (updated_at)` only); §9.3 restore step 5 ("on the restored shard, `namespace_ownership.epoch = e + 1`") vs `engram_check_ownership` (epoch may not decrease — fine) and `shard_meta` singleton; `sections/02-modules.md` §2.2.7 ("for `Read` it is `SELECT state FROM namespace_ownership WHERE namespace_id=$1`") vs `sections/03-data-model.md` line 256–257 (reads also check `epoch = $3`).

**Why it matters.** The freeze `UPDATE` as written violates the `CHECK` (no `freeze_reason`), the read-mode fence is specified two different ways (with and without the epoch; with the epoch, a reader whose cache lags one epoch fails instead of reading the frozen source as D5 promises), and the `engram_app` grant comment ("`FOR SHARE` requires UPDATE on ≥ 1 column") is right but the restore procedure needs `engram_admin` to rewrite epochs on a shard whose ownership rows may be `frozen` with `freeze_reason = 'restore'` while the catalog says `frozen` too — the catalog `namespace_state` enum has no `restoring`, so a restore and a move are indistinguishable to a client. Small individually, together they show the fence protocol has not been executed end to end.

**Recommendation.** Write the ownership state machine once (states × roles × statements) in §3.3.1, generate the SQL from it, and make `TestIso_Move_Epoch`/the restore drill execute every transition against the real DDL in T3 before any move code exists.

### F-24 — major — HA failover has no fencing of the old primary and the direct connections (relay, move pool) are not repointed

**Where.** `sections/09-operations.md` §9.6 "A shard down" lines 733–736 ("promote the standby, repoint pgbouncer (`DB_HOST`) and `RELOAD`; no epoch bump … Never start two Postgres instances on the same shard's data; the ownership epoch does not protect against two copies of the *same* epoch"); §9.1 (`direct: shard-1-postgres:5432` for the relay and `engramctl`); N15 (relay lock on a direct session).

**Why it is risky.** The runbook names the exact failure and then performs it: after promotion the old primary (paused container, partitioned host) can come back, and (a) the relay's direct connection still points at it (reads a stale outbox, elects a second leader, advances cursors against the wrong database), (b) the move pool's direct source connection copies from it, (c) `engramctl` migrates it. Without STONITH or a promotion epoch in the connection string, "no epoch bump" is the wrong default.

**Recommendation.** Route *all* connections, including direct ones, through a per-shard virtual endpoint that failover flips atomically (pgbouncer for the pooled ones, a DNS/VIP or a second pgbouncer in session mode for the relay/move), and bump the epoch on promotion unless the old primary was verifiably stopped (`pg_ctl stop -m immediate` + volume detach) — the fence then protects against the zombie exactly as it does after a restore.

### F-25 — major — Temporal histories carry cross-tenant content in cleartext in a shared namespace

**Where.** N13 (embeddings inline), `proto/engram/internal/workflow/v1/workflow.proto` (`ExtractedFact.text`, `ChunkWork.text` ≤ 8 KiB, `ObservationOp.text`); `sections/09-operations.md` §9.1 (`temporal: { namespace: engram }`, `temporalio/auto-setup`, Temporal UI not mentioned, no data converter/codec); D13 ("Prompts never contain … other tenants' data").

**Why it is risky.** Every tenant's chunk text, extracted facts and observation text transit and persist (7-day retention) in one Temporal namespace readable by anyone with Temporal UI/CLI access, outside RLS, outside the blob credential scoping and outside the deletion SLA (see F-13). The plan's isolation argument covers Postgres, blob and metrics and is silent on Temporal.

**Recommendation.** Configure a Temporal `DataConverter` with a payload codec (AES-GCM, per-shard key from the same secret store), pass large/textual results by blob key only, and add a `TestIso_Temporal_PayloadsEncrypted` row to the §8.3 matrix.

### F-26 — major — config keys the pipelines and prompts depend on are not in the config schema, which rejects unknown keys

**Where.** D12 ("keys `models.{extract,consolidate,reflect,embed,rerank}`, `chunk.target_chars`, `recall.default_budget`, `quota.*`"); `sections/02-modules.md` §2.2.22 `Resolved` (Models, Chunk, Recall, Quota, Consolidation{Enabled, Debounce}; "unknown keys → errs.Validation"); `proto/memory/v1/namespace.proto` line 102–105 ("Unknown keys are rejected with INVALID_ARGUMENT"). Used but absent: `prompts.extract` (§6.0 line 28), `retain_mission` (§6.2 inputs), `observations_mission` (§6.3), `consolidate.observation_scope` (N39, §5.2.2), `consolidate.max_observations_per_scope` (A-P2), `profile.keep_transcripts` (§3.6).

**Why it matters.** Five namespace-level knobs that the prompts, consolidation grouping and transcript retention read cannot be set through the only configuration path; `ResolvedModels` carries prompt versions but nothing resolves them from config. The Hindsight parity rows for `retain_mission`/`observations_mission` therefore do not exist in practice.

**Recommendation.** Extend D12's key list and `config.Resolved` with `prompts.*`, `retain.mission`, `consolidate.{mission, observation_scope, max_observations_per_scope}`, `reflect.keep_transcripts`, and generate the allow-list and the proto comment from one Go table.

### F-27 — minor — `prefer_observations`, `GetOperation` on a deleting namespace, tag limits, shard states and blob prefixes differ between sections and artifacts

**Where.** `sections/05-pipelines.md` line 795 (`prefer_observations = true` — no such field in `RecallRequest`); `sections/05-pipelines.md` §5.4.3 PD-9 ("`OperationService.GetOperation` special-cases `DELETE_NAMESPACE`") vs §4.1.1 line 58 ("Namespace `DELETING` → `FAILED_PRECONDITION` … Reads and writes are both rejected") — the interceptor rejects before the handler, so the delete operation cannot be polled; `sections/04-api-contracts.md` line 692 ("≤ 64 tags") vs `proto/memory/v1/memory.proto` line 96 ("≤ 32 tags (decision N32)") — the section's inline copy of the proto has drifted from the file; `proto/memory/admin/v1/admin.proto` `ShardState {ACTIVE, DRAINING, EMPTY, DISABLED}` vs `sql/catalog_schema.sql` line 32 (`provisioning, active, full, draining, readonly, retired`); `admin.proto` line 264 and `workflow.proto` line 33 (`blob_prefix` "shard-7") vs `catalog_schema.sql` line 103 (`blob_prefix = shard_id::text`); `Shard.schema_version string` vs `integer`; `sections/09-operations.md` line 30 (`maintenance_work_mem=2GB`) vs `sections/03-data-model.md` line 1074 ("inside `maintenance_work_mem = 4 GB`"); D3 line 42 ("≈ 168 GB total") vs `sections/03-data-model.md` line 1038 ("the D3 row predates N41 and needs this update") and §3.10 line 1411 ("≈ 158 GB total"); `sections/03-data-model.md` line 219–222 (workers report usage deltas into `tenant_usage_daily` through the API) vs `sections/05-pipelines.md` lines 198–201 (that exact mechanism is "rejected"); `sql/shard_schema.sql` line 30–31 (comment: relay policy on `outbox` *and* `deletion_log`) vs line 1123 (policy on `outbox` only); `operations.kind` includes `'reflect'` although Reflect is synchronous.

**Recommendation.** Generate the section excerpts from the artifacts (or delete the excerpts), add `prefer_observations` to `RecallRequest` or drop it from §5.3, exempt `OperationService.Get/Wait` for `DELETE_NAMESPACE` from the `deleting` rejection in the method policy table, and reconcile the enums.

### F-28 — minor — BM25 statistics are computed per hash partition across tenants: scores leak vocabulary and are not reproducible across moves

**Where.** `sections/03-data-model.md` §3.3.4 (`facts_bm25` on the partitioned parent → one index per partition shared by ~10 namespaces); `proto/memory/v1/common.proto` `Scores.lexical` ("BM25 score from pg_search (unbounded, query-dependent)") returned to clients.

**Why it matters.** IDF is per index, i.e. per partition; a tenant observes its own lexical scores change as other tenants ingest, and can probe term rarity (insert a document with term X, read its score) to infer another tenant's vocabulary — a weak but real cross-tenant channel on a shared shard, and after a move the same query gives different raw scores. RRF ranks are unaffected; explain output is.

**Recommendation.** Do not expose raw BM25 scores (expose per-arm *ranks*), or normalise them per query; document that lexical scores are shard-relative; consider `isolation=dedicated` guidance for tenants who care.

### F-29 — minor — the JWT namespace allowlist does not scale to many-namespace tenants, and `ListNamespaces` is bound to it

**Where.** D13 ("`ns` (list of namespace ids or `["*"]`)"); `proto/memory/v1/namespace.proto` (`ListNamespaces` "restricted to the JWT allowlist unless it is `["*"]`"; "A JWT whose `ns` allowlist is not `["*"]` cannot create namespaces"); `sections/08-testing.md` §8.6 (one namespace per LongMemEval question → 500 namespaces per tenant; an agent platform with per-user namespaces has 10⁴–10⁵).

**Why it matters.** A token listing 10 000 UUIDs is ≈ 400 KB; gRPC metadata limits (8 KB default) reject it long before. The only escape is `"*"`, i.e. all-or-nothing, which defeats the allowlist for exactly the tenants that need it.

**Recommendation.** Add a `ns_prefix`/`ns_group` claim (namespaces carry a tenant-defined group) or accept a per-namespace token model where the MCP endpoint path is the scope; keep the explicit list for small cases.

### F-30 — minor — Reflect and `StreamSnapshot` are killed by the 35 s API drain, and Envoy's read route caps `WaitOperation` at 30 s against the 65 s API cap

**Where.** `sections/09-operations.md` line 74 (`stop_grace_period: 35s` for `engram-api`), line 244 (`grpc_timeout_header_max: 30s` on the read route that includes `OperationService/WaitOperation`), N11 (`WaitOperation` 65 s, `Reflect` 330 s, `StreamSnapshot` 600 s), §9.2 rollout step 4 ("api one replica at a time … drain 35 s").

**Why it matters.** Every rolling restart aborts in-flight Reflects (≤ 300 s) and exports; Envoy silently lowers a 60 s `WaitOperation` deadline to 30 s so the long-poll returns `timed_out` early and the client sees behaviour the API contract forbids (N11 says over-cap is rejected, never clamped — Envoy clamps).

**Recommendation.** Set `stop_grace_period ≥ 340 s` with a connection-draining phase, and raise `grpc_timeout_header_max` on the read route to 65 s (or move `WaitOperation` to the no-retry route).

### F-31 — minor — the RRF permutation-invariance theorem is proved over an abstract commutative monoid, but Go sums float64 in arm order

**Where.** `formal/lean/Engram/RRF.lean` lines 19–22 ("instantiate with `Nat` … and with `Float` (what Go computes; no proofs about Float arithmetic are attempted)"); `sections/08-testing.md` §8.2 ("**Permutation invariance**: fusing arms in any order yields identical scores and identical tie-broken order").

**Why it matters.** Float addition is not associative; `1/61 + 1/62 + 1/63` summed in two orders can differ in the last ulp, so two candidates with equal rank multisets can flip order depending on arm iteration order, and the rapid property as stated is false for float64. The Lean file's own remedy (scaled integers `D/(k+r)`) is not what the Go contract specifies.

**Recommendation.** Compute RRF in exact integer arithmetic (`D = lcm(k+1..k+R)` with R = max cap, fits in uint64 for k = 60, R = 400 only if D is chosen per arm count — or simply use `math/big.Rat` for ≤ 2 000 candidates) or sum in a canonical arm order and state the property as "order-independent up to canonical summation". Fix the 0-based/1-based rank comment mismatch in `RRF.lean` (line 65 vs line 175).

### F-32 — minor — the temporal arm sorts every overlapping fact by distance; a wide window ("in 2024") is unbounded

**Where.** `sections/03-data-model.md` §3.8 lines 1189–1197 (`… && tstzrange($4, $5, '[]') … ORDER BY abs(extract(epoch FROM …)) LIMIT 150`); D10 (temporal arm "ordered by distance to `query_timestamp`").

**Why it matters.** The GiST index returns every fact whose window overlaps the query window; for a year-wide window in a 1 M-fact namespace that is up to 1 M rows sorted in memory before `LIMIT 150` — far outside the 60 ms arm budget, and the 5 s read `statement_timeout` turns it into a dropped arm.

**Recommendation.** Probe two-sided around `query_timestamp` with the btree on `(namespace_id, occurred_start)` (nearest N before and after), or bucket the window as Hindsight does and take the top-k per bucket; cap the window to the budget.

### F-33 — minor — Hindsight capabilities silently dropped or weakened, and "improved" rows without evidence

**Where.** `sections/01-architecture.md` §1.9 (rows marked **improved**: chunk arm, `as_of`, versioned observations, extraction cache, chunking); `sections/06-prompts.md` §6.5 tools (`search_memories`, `search_observations`, `get_page(name_or_id)`, `expand_fact`); `reference/hindsight-notes.md` §5 (`search_mental_models`, `read_mental_models`, split synthesis `build_chunk_claims_prompt`), §3 (`include.entities`, `prefer_observations`, `min_scores`), §1 (`observation_scopes` per item, `retain_mission`), §9 (`retry_operation`, memory edit); `sections/12-non-goals.md` NG11, NG7.

**What was dropped.** (a) Reflect cannot *search* pages — `get_page` needs a name/id and `PageService` has no search, while Hindsight's hierarchy starts with `search_mental_models`; (b) the split-synthesis map/reduce at the 100 k context cap is replaced by "forces `done`", a quality regression on long sessions; (c) entity-level results (`entities{name→…}`) and `prefer_observations` suppression are gone; (d) per-item `observation_scopes` became a namespace config key (N39, open); (e) `retain_mission`/`observations_mission` are referenced but unsettable (F-26); (f) operation retry and memory edit have no public RPC. "Improved" claims for CDC chunking and headers rest on expected directions only (§8.6 ablations), and A-B3's "+9 pp hybrid over BM25" is borrowed from this repository's unrelated benchmark code, not from Hindsight.

**Recommendation.** Add `search_pages` (full-text + semantic over `page_versions`, the Hindsight `search_knowledge_base` equivalent) to Reflect and `PageService`; keep the map/reduce fallback; downgrade "improved" to "changed, to be measured" until the ablations exist.

### F-34 — minor — initial fill of 1 B facts at the stated gateway cap takes ~4 weeks per cell and ~$150–200 k; the plan sizes storage but not time-to-fill or spend-to-fill

**Where.** D3 ("a 600 RPM gateway cap yields 10 chunks/s cell-wide"; "1 B facts = 4 cells"); `sections/05-pipelines.md` §5.1.6; `sections/06-prompts.md` §6.8 ($0.0007 per chunk, $0.0007 per consolidation batch).

**Recomputation.** 1 B facts ÷ 10 facts/chunk = 100 M chunks ÷ (10 chunks/s × 4 cells) = 2.5 M s ≈ 29 days of continuous ingest at the cap (116 days for one cell); extraction ≈ $70 k and consolidation ≈ 125 M batches × $0.0007 ≈ $88 k at list prices, before summaries and re-embeddings (F-14). None of this appears in §10 or §11, and the batch API path (`RetainBackfill`) is phase 3 (N31 is in §5 but M-numbered nowhere).

**Recommendation.** State time-to-fill and spend-to-fill as A-n assumptions next to D3, and pull `RetainBackfill` (batch API) into the MVP if any customer migration is planned.

### F-35 — minor — cross-cell moves need two Temporal clusters and the stream RPCs cannot be forwarded with the stated `Forwarder` interface

**Where.** `sections/09-operations.md` §9.1 (a `temporal` service per cell); `sections/05-pipelines.md` §5.5 ("A cross-cell move (phase 3) needs exactly one extra credential — the source read pool"); `sections/02-modules.md` §2.2.4 `Forwarder.Invoke(ctx, cell, fullMethod, req, resp proto.Message)` (unary only) vs `Recall`, `Reflect`, `StreamSnapshot` (server streams); N17.

**Why it matters.** `Drain` lists and terminates the namespace's workflows "on the source queue" — in the *source cell's* Temporal, which the target cell's worker does not talk to; `Restart` must start them in the target's Temporal. The forwarding hop as specified cannot carry the three streaming methods, i.e. cannot forward Recall at all.

**Recommendation.** Decide: one Temporal cluster per fleet (simplest; task queues are already per shard) or an explicit cross-cluster `Drain`/`Restart` protocol; add a streaming `Forwarder` (grpc `ClientStream` proxy) to §2.2.4 and to M3.3's exit criteria.

### F-36 — minor — two hot rows serialise every chunk commit of a namespace and of a document

**Where.** `sections/05-pipelines.md` §5.1.2 step 5.5.7 ("`UPDATE namespace_stats SET live_facts = live_facts + $n`; `UPDATE operations SET progress.units_done = units_done + 1`"); `sections/03-data-model.md` §3.3.1 ("Kept apart from namespace_ownership on purpose: … an UPDATE there would serialise them").

**Why it matters.** The same argument applies to `namespace_stats`: every `CommitChunk`, delete cascade and `FinalizeVersion` of a namespace now serialise on its stats row (row lock held to commit), and all 32 parallel commits of one document serialise on its `operations` row. At ~15 ms per commit that caps a namespace at ~60 chunk commits/s and a document at ~60/s — below the LLM bound today, but it also puts the delete cascade (F-19, seconds) in the same queue as every commit.

**Recommendation.** Make counters derived (`namespace_stats` refreshed by a periodic `count(*) WHERE live` or by summing `document_versions.chunks_done`), and update `operations.progress` from the workflow (one write per wave) instead of per chunk.

### F-37 — minor — `pg_search` on hash-partitioned tables with a composite PK, literal-tokenised `text[]`, `OR tag_count = 0` predicates and a secondary `ORDER BY memory_id` tiebreak are all asserted "parser-checked", not executed

**Where.** `sections/03-data-model.md` §3 intro ("the `USING bm25` statements were parser-checked and written against the pg_search 0.25 syntax") and §3.8 lexical arm ("`ORDER BY pdb.score(memory_id) DESC, memory_id ASC` … pushed down as Top-K"); R1.

**Why it matters.** Four non-trivial pg_search behaviours decide whether the lexical arm meets 60 ms: partition support with `key_field` = the non-partition half of the PK, `text[]` literal tokenisation, a non-`@@@` disjunct (`tag_count = 0 OR …`) inside a Top-K scan, and a secondary sort key. Any one failing means a full score sort per query. M1.2's p95 gate is the first time this is exercised, in week 9.

**Recommendation.** Make "lexical arm plan shows Top-K pushdown on a partitioned fixture" an M0.2 exit criterion on the real ParadeDB image; keep `TsvectorIndex` as the planned fallback with its own p95 number.

### F-38 — minor — the design run of `DocLifecycle` is cited as the reason for N42, but `NoOrphanLinks` as checked contradicts the invalidate rule and the graph arm

**Where.** `formal/tla/DocLifecycle.tla` lines 252–254 (`NoOrphanLinks`: "no link touches acknowledged-deleted content"); `sections/05-pipelines.md` §5.4.5 ("Links are kept in both directions (the graph arm joins facts and applies the filter there)") and F-19's recommendation.

**Why it matters.** The invariant is stated for *deletes* only and the model deletes links synchronously; the design already tolerates dangling links for invalidates and for replace-retires (links kept until purge). Either the invariant is "no *traversal* through non-live facts" (what the SQL enforces) and the sync link delete is unnecessary (F-19), or the invariant is literal and invalidate/replace violate it. The spec and the pipelines disagree on what "orphan link" means.

**Recommendation.** Restate `NoOrphanLinks` as a property of `GraphArm` (both endpoints live at traversal) and drop link deletion from the synchronous cascade.

### F-39 — minor — `WaitOperation` is specified as a long-poll with no mechanism; `TenantDelete`/`DeleteNamespace` operations cannot be awaited

**Where.** `proto/memory/v1/operation.proto` lines 39–45; `sections/04-api-contracts.md` §4.1.7; `sections/05-pipelines.md` §5.4.3 PD-9; `proto/memory/admin/v1/admin.proto` `DeleteTenantResponse.namespace_operations`.

**Why it matters.** 10 000 clients long-polling 60 s each is either 10 000 DB polls per interval or a per-shard `LISTEN` (impossible through pgbouncer, N15) or a Temporal `GetWorkflow().Get()` per waiter (10 000 open long-polls against Temporal frontend). The interceptor's `deleting → FAILED_PRECONDITION` rule (F-27) makes the returned delete operations unpollable at all.

**Recommendation.** Implement `WaitOperation` on Temporal's workflow-result long-poll with a per-process cap and jittered DB fallback, and carve out `OperationService.Get/Wait` for `DELETE_NAMESPACE` in the method policy.

### F-40 — nit — the graph-expansion SQL returns one id

**Where.** `sections/03-data-model.md` line 1237: "`SELECT unnest(visited) AS memory_id FROM hops ORDER BY hop DESC LIMIT 1;`".

**Why.** `unnest` expands rows before `ORDER BY … LIMIT 1`, so exactly one `memory_id` is returned. The intended query selects the deepest row first and unnests it. Since the text says it was "executed against the validated shard schema", the executed query was not this one.

### F-41 — nit — entity upserts are not sorted, so two concurrent `CommitChunk`s can deadlock on `entities`

**Where.** `sections/05-pipelines.md` §5.1.2 step 5.5.5 (links are "inserted in `(src_memory_id, dst_memory_id, link_type)` order so two concurrent chunks never deadlock"; entities `INSERT … ON CONFLICT … DO UPDATE` with no ordering rule); `sections/03-data-model.md` §3.8 retain path.

**Recommendation.** Upsert entities in `canonical_norm` order (one statement with `unnest(sorted)`), as Hindsight's `_lock_order_key` does.

### F-42 — nit — `engram-forward-hops` is client-spoofable and gRPC reflection exposes the admin surface

**Where.** N17; `sections/04-api-contracts.md` §4.8 ("reflection is also served"); `sections/09-operations.md` Envoy config (no header sanitisation).

**Recommendation.** Strip `engram-forward-*` on the external Envoy listener; serve reflection for `memory.v1` only, or on the admin route.

### F-43 — nit — the request hash is "SHA-256 over the canonical proto serialisation", but protobuf has no canonical binary form

**Where.** `sections/04-api-contracts.md` line 100.

**Why.** `Deterministic: true` only orders map entries; unknown fields, field order from different SDKs and `Struct` number formatting all change bytes, so a retry through a newer SDK yields `IDEMPOTENCY_KEY_REUSED`. Hash a normalised protojson (sorted keys, unknown fields dropped) instead.

### F-44 — nit — derived operation ids are UUIDv5 but `operation_id` is validated as UUIDv7

**Where.** `sections/04-api-contracts.md` §4.1.3 line 98 ("`UUIDv5(operation_id, document_id)`"), line 96 ("UUIDv7 (validated)").

**Recommendation.** Derive as UUIDv7 with a deterministic random part (`v7(ts = submit, rand = hmac(op, doc))`) or relax the validation to "any UUID".

### F-45 — nit — `CreateNamespace` is two-phase across two databases with no sweeper for `creating`

**Where.** `sql/catalog_schema.sql` lines 359–363; `sections/02-modules.md` §2.2.3 (`CreateNamespace … also inserts namespace_ownership`).

**Recommendation.** Add `creating` to the per-shard `op-sweeper` (complete or delete rows older than 2 min) and return `UNAVAILABLE` with the same `request_id` replay rule as Retain.

---

## Verdict

1. Not ready to staff as written: four blockers (F-1 to F-4) change guarantees or mechanisms that the brief lists as non-negotiable (leak-free `as_of`, delete invisibility, lossless moves, a move that can run at all under RLS).
2. The protocol core (epoch fence, outbox, versioned documents, write-once proposals) is sound in outline and the counterexample-driven corrections in D19 are genuinely good engineering — but the design configurations of two of the five specs never finished, and three gaps (per-version hiding, previous-version input, shard-wide gap knowledge) sit exactly where the prose claims TLC coverage.
3. The Postgres layer has three "it will not run like this" items (COPY FROM under RLS, share-lock starvation of the barrier, filtered HNSW for small namespaces) that need decisions, not tuning.
4. The headline numbers do not survive recomputation: p95 is ≈ 276 ms on the critical path, the reranker fleet is unsized, consolidation volume is under-counted 4–5×, cost per haystack appears as three different numbers, and the retain history exceeds Temporal's limits.
5. The schedule is internally impossible (E2's serial chain ends at week 31 of a 26-week plan); 74 ew should be read as 110–140 ew or as a smaller committed scope.
6. Operability has two fleet-wide failure modes the plan itself documents and then leaves manual (catalog expiry at 10 min; failover without fencing).
7. Security isolation is thorough for Postgres and blob, and absent for Temporal payloads and metrics cardinality.
8. What is good and should be kept: thin outbox + strict-prefix relay, write-once proposals with keys over the stored list, version-row `FOR SHARE` for commits, the deletion log for restores, Connect over grpc-gateway, the per-namespace extraction cache, mandatory deadlines.
9. Recommended path: a 3–4 week Phase 0' that resolves F-1 to F-8 as register changes (with the TLA+ additions), measures Q1/F-8 on the real image, and re-plans; then staff Phase 1 at ~45 ew with moves and multi-cell explicitly deferred.
10. If staffed unchanged, expect the MVP exit to slip on M1.2 (p95) and M1.5 (moves), and expect the first real tenant delete to expose F-9 and F-13.

## Three decisions I would reverse

1. **D9's `mentioned_at` as the `as_of` key (F-2).** Replace with a server-set `knowledge_at` = item timestamp; the model-chosen date becomes `said_at` for ranking only. Leak-freedom must not depend on an LLM's judgement.
2. **D2's `FOR SHARE` row lock as the fence lock (F-5), together with `COPY FROM` under RLS (F-4).** Keep the ownership *row* as the fence value, but take the fence as a shared advisory lock, and load the move target with batched inserts (or a bypass role) — the "RLS as a second fence for free" argument costs the feature it protects.
3. **N41's observation-level hiding with full-prompt inputs (F-1, F-9).** Hide per *version* and permanently for versions that saw deleted content; narrow inputs to rendered evidence. This keeps D16's promise under `as_of` and stops a session delete from blanking the observation layer for hours.

## What I could not verify in this environment

- Any pg_search behaviour: partition support, `key_field` on a composite-PK partition, `text[]` literal tokenisation, Top-K pushdown with a non-`@@@` disjunct and a secondary sort key (F-37). The plan itself says these were parser-checked only.
- pgvector `halfvec` storage default and the exact heap/TOAST split behind "≈ 800 B hot row" (§3.3.4); the HNSW 18 GB and BM25 4 GB figures are plausible but unmeasured.
- The gateway: rerank latency/throughput for `bge-reranker-v2-m3` (A-2), whether it serves `gpt-oss-120b` (A-B1), batch-API semantics and pricing (A-P3). Every cost and latency number above inherits those assumptions.
- Postgres row-lock queueing behaviour (F-5) is stated from the `heap_lock_tuple` fast path for compatible modes; it should be confirmed with a two-session test on PG 16 before the advisory-lock change is adopted — the recommendation is cheap either way.
- TLC runs: I did not re-run the configurations; findings on formal coverage rest on reading the specs, cfgs and `RESULTS.md`. Lean files were not type-checked (the plan says the same).
- Temporal limits (50 MB / 51 200 events history, 2 MB payload) are the documented defaults; a self-hosted cluster may raise them, which would soften F-18 but not F-13/F-25.
- Hindsight behaviour is taken from `reference/hindsight-notes.md`; items the notes mark UNVERIFIED (e.g. per-arm SQL, reflect wall timeout) were not used as evidence.

---

## Disposition

Applied after the review by the engineering lead; the register (`00-decision-register.md`, amended
rows D2, D4, D9, D17, N41 and the D20 block N50–N77) is binding where it and a finding differ. Every
finding is accepted; none was rejected or deferred by the register. "Where applied" names the sections
edited for the finding; sections §1–§5, `proto/` and `sql/` were edited by their own owners in the same
pass and are listed for completeness.

| Finding | Disposition | Register | Where applied |
|---|---|---|---|
| F-1 | Accepted | N41 (amended): per-version, permanent `derived_from_deleted` | §5.2.2/§5.4 (owner); §8.4 `TestDocLifecycle_InputHidesObservation` reworded, §8.5 `TestAsOf_ObservationVersions` reworded + new `TestAsOf_DeletedDerivedVersionStaysHidden`; §7.6 W-1; §11 A38; §00 executive summary |
| F-2 | Accepted | D9 (amended): `mentioned_at` = item timestamp, server-set; `said_at` informational | §6.2 schema/prompt/validation; §4.4, proto (owner); §8.5–§8.6 leak canary note; §11 R24, A39; §00 |
| F-3 | Accepted | N50 anti-join replay | §5.5 (owner); §8.4 `TestMove_AntiJoinReplay`; §7.2.5 prose, §7.6 W-3; §10 M1.5 |
| F-4 | Accepted (option b: bypass load role) | N51 `engram_move_load` | §5.5, `sql/` (owner); §8.4 `TestMove_LoadUnderBypassRole`; §10 M0.2 roles, M1.5; §11 A37 (+ seam) |
| F-5 | Accepted | D2 (amended): advisory-lock fence, row = fence value | §3.3/§5 (owner); §8.4 `TestFence_AdvisoryLockFairness`; §7.2.5 and §7.5 prose, §7.6 W-4; §10 M1.8; §11 A9, A36; §00 |
| F-6 | Accepted | N53: `bge-reranker-base`, `rerank_top` 0/50/150, A-R1, skip = SLO breach | §8.6 skip-rate metric + `--rerank-model` ablation, §8.8, `bench.lock`; §9.1 config, §9.4 SLO `Rerank coverage` + `RerankSkipRateHigh`; §10 M0.5, M1.2; §11 R3, R25, Q17, A18 |
| F-7 | Accepted | N54: critical-path sum 216 ms at MID | §1.5 (owner); §8.6 critical-path metric and report template; §9.1 `graph_budget_ms`; §10 M1.2 exit, §10.5; §12 NG28; §00 |
| F-8 | Accepted | N55: exact scan < 20 k facts, partial HNSW above | §3.8 (owner); §8.4 `TestRecall_SmallNamespaceExactScan`, §8.6 `--ablate exact_scan`; §9.1 `small_namespace_facts`; §10 M0.6, M1.2; §11 R26 |
| F-9 | Accepted | N41/N47: inputs = rendered quotes (≤ 5 per candidate), per-version hiding, hidden-first batches | §6.3 inputs rows + prompt note; §8.6 hidden-observations metric; §9.4 SLO + `StaleDeleteBacklog`; §10 M2.1; §11 R27 |
| F-10 | Accepted (chain, not reject) | N56 | §5.1 (owner); §8.4 `TestRetain_ConcurrentAppendsChain`; §7.6 W-2; §10 M1.1 |
| F-11 | Accepted | N57 insert-before-delete | §5.2.2 (owner); §8.4 `TestConsolidation_DisjointSourceUpdateKeepsObservation`; §10 M2.1 |
| F-12 | Accepted | N58 `extraction_key` retire | §5.1.2 (owner); §6.0; §8.4 `TestRetain_PromptBumpRetiresOldFacts`; §10 M1.1 |
| F-13 | Accepted | N59: snapshot expiry, keys-only payloads, codec, short blob retention | §8.3 `TestDelete_ExpiresSnapshots`; §9.3 "every other copy" table in the deletion SLA; §10 M1.3, M3.2 |
| F-14 | Accepted | N60 summary refresh rule, header in chunk vector only | §6.1 "When it runs" + usage note; §8.6 re-embeddings-per-append metric; §9.4 `engram_retain_reembed_total`; §10 M1.1 exit |
| F-15 | Accepted, work items | N77 | §7.1 "Reading the table" prose (rows updated separately), §7.6 gate set + open spec work items W-1…W-5; §10 M0.7, F.1, F.2; §00; README |
| F-16 | Accepted, work items (re-estimate) | D17 (amended): 112 ew total, 74 ew committed | §10 rewritten (Phase 0′, per-engineer chains, M1.5 behind a flag at 8–12 ew, F.1 realistic, Gantt redrawn); §00; README |
| F-17 | Accepted | N74 one A-F assumption, one cost table | §6.8 Tables 6.8-A/6.8-B; §8.6 "Cost of a run" and report template, §8.8 per-1 k-facts; §10 Phase 2 gate; §11 R30 |
| F-18 | Accepted | N59 keys-only, continue-as-new by budget | §8.1 T2 history-size assertion (`TestRetain_HistoryBudget`); §9.1 `continue_as_new`; §10 M1.1; §11 R12, R28 |
| F-19 | Accepted | N61 visibility-only cascade, SLO per 1 k facts | §5.4 (owner); §8.4 `TestDelete_CascadeScales`; §9.4 SLO row; §10 M1.3 |
| F-20 | Accepted | N52 bounded read re-resolve, three activities, SLI counts it | §8.4 stale-catalog and cutover rows; §9.4 availability SLI, `CutoverInProgress`, `engram_move_cutover_window_seconds`; §10 M1.5 |
| F-21 | Accepted | D4 (amended): serve existing indefinitely, automated failover | §9.1 compose (`catalog-failover`) and config, §9.4 SLO/alerts, §9.6 runbook; §10 M0.3; §11 R29, A16; §00 |
| F-22 | Accepted | N62 no `tenant` label | §8.1 S linter, §8.3 `TestIso_Metrics_Labels`, §8.8; §9.4 label policy, metrics table, dashboards; §10 M1.7, M2.4; §11 A40 |
| F-23 | Accepted | N64 ownership state machine, `restoring` | §3.3.1 (owner); §10 M1.8 (`TestOwnership_StateMachine` before move code) |
| F-24 | Accepted | N63 virtual endpoints, epoch bump on promotion | §8.4 failover row (two variants); §9.1 `direct: shard-N-primary`; §9.6 "A shard down" rewritten; §10 M1.7 |
| F-25 | Accepted | N59 `DataConverter` codec, per-shard key | §8.3 `TestIso_Temporal_PayloadsEncrypted`; §9.1 `temporal.codec` + secret row; §9.3; §10 M1.7 |
| F-26 | Accepted | N66 config keys from one Go table | §6.0 "Config keys the prompts read"; §9.1 precedence paragraph; §10 M1.6, M3.4 |
| F-27 | Accepted | N70 (ops Get/Wait exemption), N64 (`restoring`); the rest editorial | §9.1 `maintenance_work_mem = 2 GB` stated, §9.5 shard states aligned to the catalog SQL; §10 M1.3; proto/§3/§4/§5 excerpts (owners) |
| F-28 | Accepted | N67 normalised scores | §8.2 fuse row; §11 A41 |
| F-29 | Accepted | N65 `ns_group` claim | §10 M0.3; proto (owner) |
| F-30 | Accepted | N75 | §9.1 `stop_grace_period: 340s`, Envoy read route 65 s; §9.2 rollout step; §11 R14 |
| F-31 | Accepted (exact `big.Rat`) | N67 | §8.2 fuse row (exact golden `130/4221`, float counterexample test); `formal/lean/Engram/RRF.lean` comment fix (1-based ranks); §10 F.3 |
| F-32 | Accepted | N68 two-sided probe | §3.8 (owner); §10 M1.2 |
| F-33 | Accepted | N73 `search_pages`, map/reduce, "changed, to be measured" | §6.5 tool text + fallback; §8.6 ablation wording, §8.7 goals wording; §10 M2.2, M3.1, M3.8; §11 Q19; §1.9 (owner) |
| F-34 | Accepted | N74 time/spend-to-fill, `RetainBackfill` to Phase 2 | §6.8 fill row; §10 M2.5, M3.7; §11 R30 |
| F-35 | Accepted (one Temporal cluster per fleet) | N71 | §9.1 topology and config; §10 M3.3 (streaming `Forwarder`) |
| F-36 | Accepted | N69 derived counters, per-wave progress | §5.1 (owner); §10 M1.1 |
| F-37 | Accepted | N76 M0.2 exit criterion | §10 M0.2 exit; §8.4 `TestLexical_TopKPushdown` (via M1.2); §11 Q18 |
| F-38 | Accepted | N61 `NoOrphanLinks` restated | §8.4 `TestDelete_CascadeScales` assert; §5.4.5/spec (owner) |
| F-39 | Accepted | N70 long-poll on Temporal | §10 M1.6; §4 (owner) |
| F-40 | Accepted (editorial, no register row) | — | §3.8 SQL (owner) |
| F-41 | Accepted | N69 ordered entity upserts | §5.1 (owner); §10 M1.1 |
| F-42 | Accepted | N71 header stripping, reflection scope | §9.1 Envoy `request_headers_to_remove` + reflection note |
| F-43 | Accepted | N72 protojson hash | §4 (owner); §10 M1.6 |
| F-44 | Accepted | N72 any-UUID derived ids | §4 (owner); §10 M1.6 |
| F-45 | Accepted | N72 op-sweeper handles `creating` | §10 M1.1 (op-sweeper); §2.2.3/`sql/` (owner) |

Of the three decisions the reviewer would reverse, all three were reversed (D9 → server-set key, D2 → advisory-lock
fence with the bypass load role, N41 → per-version hiding with rendered-only inputs). The TLC results table in §7.1
was updated separately after longer runs: the `ShardMove` full-bound configuration completed and passed; the
`Consolidation` and `DocLifecycle` full-bound configurations remain INCOMPLETE and the plan says so.
