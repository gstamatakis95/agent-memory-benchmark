# Round 7 review: API, Go module design, numbers and cross-file parity

Reviewer lens: `proto/` (buf lint, build and breaking readiness, reserved numbers and names, the move messages, the
closed MCP set), the §4 embeds, the §2 Go API (method counts, typed ids, the move interfaces against §5.5 and the SQL),
every number that appears in more than one place, stale references to withdrawn move mechanisms, the §10 schedule
arithmetic, and whether the test names cited outside §8 exist in §8.

Findings already dispositioned in rounds 1 to 6 are not repeated unless the fix is wrong or missing. **[regression]**
marks a defect that a D25 (round-6) change introduced. Ids are A7-n.

## What was executed

- `buf lint` and `buf build` on `plans/engram/proto`: both clean (exit 0).
- `buf breaking` (FILE, the repository policy) against the round-5 tree (`cdd1fbc`): it reports 24 items, all of them
  in §4.5's round-6 row (the four deleted workflow reports; `MoveInput` 12; `MoveCheckpoint` 10, 14, 15, 16, 18, 19;
  `MoveResult` 6; `MoveState` 2 and 9; `MoveProgress` 10 to 15; `Move` 18 to 20; `CleanupMoveRequest` 3). This matches
  §4.5's claim that "`buf breaking` against the round-5 tree reports exactly the removals and renames this table lists".
- `buf breaking` with `WIRE_JSON` against the round-5 tree: clean, so every round-6 withdrawal reserves its number and
  its name. With `WIRE_JSON` against the first proto commit (`50304db`), it reports A7-7 below and the
  `expired_at → expires_at` rename that §4.5 already lists.
- §4 embeds: lines 333–656 of `sections/04-api-contracts.md` are byte-identical to `proto/memory/v1/common.proto`, and
  lines 786–1515 are byte-identical to `memory.proto` (`diff` is empty). `PLAN.md` contains every `sections/*.md`
  verbatim.
- RPC count: `memory.v1` has 36 RPCs (Memory 8, Page 7, Document 7, Operation 4, Namespace 6, Export 4). §4.6's 10
  hand-tuned tools, 21 generated tools and 5 omitted RPCs cover each of the 36 exactly once. `memory.admin.v1` has 19 RPCs.
- §2 interfaces: I parsed every Go block of `02-modules.md` (106 interfaces, with embedded interfaces expanded) and none
  has more than five methods. `id` gives every entity its own type. No interface parameter is an untyped string id.
- Recomputed numbers. 1 B / 5.5 M = 181.8, so 182 shards. 182 / 32 = 5.69, so 6 cells (5.7 full). 182 / 4 = 45.5, so 46
  hosts. The fallback is 1 B / 6.5 M = 154 shards on 77 hosts. The hot set is 74 × 6.5 / 5.5 = 87.5, which matches
  ≈ 88 GB. The footprint is 203 × 0.55 = 112 GB.
- Recall path. The stages sum to 2 + 25 + 60 + 30 + 1 + 90 + 3 + 5 = 216 ms. With the pool-wait term of 8 ms the path
  is 224 ms, and the headroom is 300 − 224 = 76 ms. The rerank-skip reserve is 90 + 3 + 5 + 8 = 106 ms.
- Recall pool. 425 connection-ms × 50 QPS = 21.25 Erlangs, plus 2.5 to 3 Erlangs of filtered arms, makes 24 Erlangs on
  32 servers (ρ = 0.75). Erlang-C on 32 servers at 24 Erlangs gives P(wait) = 0.083. With a mean arm transaction of
  71 ms the p95 pool wait is ≈ 4.5 ms, so "≤ 8 ms" is conservative.
- Fill. 600 / (60 × 3.5) = 2.86 chunks/s per cell, 17.1 to 17.4 chunks/s at 6 cells, so 100 M chunks take ≈ 67 days.
- Filtered thresholds. 4θ × 6.6 µs = 264 ms, which matches "≈ 260 ms".
- Roadmap. Phase 1 milestones: 10.5 + 10.5 + 3.5 + 1.5 + 7.25 + 4.0 + 4.5 + 2.0 + 2.25 + 4.0 = 50.0. Phase 0 = 13.0,
  Phase 2 = 21.25, Phase 3 = 25.25, Track F/B = 14. That gives 84.25 committed and 123.5 in total. The register's D25
  re-allocation is consistent: 12.5 + 0.5 = 13.0 and 50.5 − 1.0 + 0.5 = 50.0.
- Per-engineer committed load: E1 27.5, E2 28.25, E3 28.5 ew (sum 84.25). Every Gantt bar equals ew × 7, rounded. E1's
  chain is 195 days, with the MVP at day 160 (week 23, committed as 24). E2's chain is 199 days. E3's chain is 201 days
  (week 29).
- TLC logs in `formal/tla/results/`: the D25 configurations end as `EXPECT` says (used for A7-2).

## Findings

### A7-1: minor [regression]: §2, §7.4 and the register cite move tests that §8 does not define, and round-5 A-16's orphans are still there

**Where:**
- §2.2.19 test seam (`02-modules.md:1305`)
- §7.4(a) table, `ShardMove` and `Derivation` rows (`07-formal-verification.md:400–403`), and §7 line 51 and line 451
- Register N160 and N168
- §10 M1.9 and the register (`TestCatalog_ReconcileFromShards`)

**Claim:** §7.4 says it maps each spec to its Go twin tests, and N46's manifest pairs them.

**Evidence.** The D25 rename in §8 was not carried into the other sections. §8 defines `TestMove_FrozenCopy`,
`_FrozenWritesRetry`, `_VerifyFrozenCatchesFault`, `_WindowDeadlineRollback`, `_IndexesValidBeforeReady` and
`TestCatalog_ReconcileAfterEveryRestore`. The other sections cite these names instead:

| Cited outside §8 | Where | Name in §8 |
|---|---|---|
| `TestMove_FrozenWindow` | §2, §7, register | `TestMove_FrozenCopy` / `_FrozenWritesRetry` |
| `TestMove_VerifyCatchesFault` | §2, §7, register | `TestMove_VerifyFrozenCatchesFault` |
| `TestMove_WindowExceeded` | §2, §7, register | `TestMove_WindowDeadlineRollback` |
| `TestMove_IndexValidAtActivation` | §7, register | `TestMove_IndexesValidBeforeReady` |
| `TestCatalog_ReconcileFromShards` | §10 M1.9, register | `TestCatalog_ReconcileAfterEveryRestore` / `_PromotionRunsReconcile` |
| `TestInvalidate_ResolvesTwin` | register | none (the N162 twin tests are `TestInvalidate_LazyTwinRestored`, `_RestoreUndoesTwinSet`) |

Round 5 found six orphan names in §7.4 (A-16). Its disposition reads "Accepted | N157 | §7.4's test column generated
from §8.4.6; orphan names replaced". Five of those orphans are still in §7.4 verbatim: `TestReplace_ThenDelete`,
`TestPage_CommitReverifies`, `TestReextract_DerivedStaysVisible`, `TestMaterialize_BatchRereadsFactHidden` and
`TestHNSW_RebuildOnly`. That fix is missing.

**Recommendation.** Do what A-16 accepted. Generate the §7.4 column, and the §2 and register test mentions, from §8's
test index (`make gen-docs`). Add a doc-lint that fails when a `Test[A-Z]\w+` token outside §8 has no definition in §8.

### A7-2: minor [regression]: §7.7's results table and `formal/tla/results/RESULTS.md` report the D24 `ShardMove`, not the D25 one; §10 M0.7's exit names configurations that no longer exist

**Where:**
- §7.7 table, `07-formal-verification.md:528–548`, introduced by "All results of this section, as run after the D24 amendment"
- `formal/tla/results/RESULTS.md:63–83`
- §10 M0.7 exit (`10-roadmap.md:45`)

**Evidence.** The table lists `ShardMove_ZeroMargin`, `_NoTimelineCheck`, `_IdKeyedRecopy`, `_MergeNoDeletes`,
`_NoSeqAdvanceTwice`, `_VerifyBeforeCatchUp` and `_CleanupTimeGate`. None of these exists as a `.cfg`, and §7.6 itself
says D25 deleted `_ZeroMargin` and `_NoTimelineCheck`. The table omits `_CopyBeforeFreeze`, `_UnionRepair`,
`_RerunMerges`, `_ReadyBeforeIndex`, `_CutOnUnreplicatedCommit` and `_CleanupBackupBeforeActivate`, which exist, are in
`EXPECT` and have logs.

The rows that do exist carry D24 numbers:

| Configuration | Table | Current log |
|---|---|---|
| `ShardMove.cfg` (states generated) | 145,121,412 | 112,413,371 |
| `ShardMove_SweepNotPaused.cfg` | violates `MoveTerminatesActive` | violates `SourceStaticUnderFreeze` (also what `EXPECT` says) |

M0.7's exit criterion requires `_VerifyBeforeCatchUp` and `_CleanupTimeGate` to be green with logs, so as written it
cannot be met. The logs themselves are correct: every D25 configuration ends as `EXPECT` says.

**Recommendation.** Regenerate §7.7 and `RESULTS.md` from `results/*.log`, the same way `EXPECT` is checked. Drop the
deleted names from M0.7's exit and name the D25 must-fail set instead.

### A7-3: minor: connection-budget numbers are stale in three places, and §2 gives `TestReadSession_ConcurrentArms` an assertion that the §8 test contradicts

**Where:**
- Register N54 ("pool-wait term of ≤ 4 ms p95 … = **220 ms p95 at MID**", with no "rev. D25" marker)
- Register N155 ("`TestReadSession_ConcurrentArms` asserts ≤ 320 connection-ms per MID recall")
- `02-modules.md:590` ("TestReadSession_ConcurrentArms asserts both bounds and ≤ 320 connection-ms per MID recall")
- Register N114 (the "rev. D24, N155" clause: "`engram_app` pool 40 with recall ≤ 32, per-process pool 20, ≈ 53 of `max_connections` 100")

**Evidence.**
- §8:261 states that the same test measures "Σ arm transaction time ≈ 425 connection-ms". N155 also states 425 in its
  own first sentence. An assertion of ≤ 320 against a sized 425 fails by construction.
- D3 (line 47), N164, §1 and §8 all say 224 ms and 8 ms. N54 still says 220 ms and 4 ms.
- N164(5) replaces the pools with 32/8 and the connection count with 57. N114's parenthetical still describes pools of
  40/20 and 53 connections.

**Recommendation.** Mark N54 and N114 "rev. D25" and restate them with N164's figures (224 ms, 8 ms, pools 32/8, 57).
Change N155 and §2 to "≈ 425 connection-ms (recorded; alert above 1.25×)", matching §8.

### A7-4: minor: the move-window arithmetic disagrees with itself, a namespace above ≈ 9.6 M facts cannot be moved at all, and the deadline margin disappears near the window

**Where:**
- "≈ 250 k facts" at `W_est ≤ 10 min`: §5.5 (`05-pipelines.md:1668`), §9 (`09-operations.md:572`), N160
- "≈ 25 min per 1 M live facts" in the same paragraphs
- `namespace_moves.window_seconds … CHECK (… <= 14400)` and `CHECK (w_est_seconds <= window_seconds)`
- §9:543 ("plan moves of the largest namespaces")

**Evidence.**
- **The 10 min figure.** W_est is linear in facts (rows / R_copy + vectors / R_build). At 25 min per 1 M facts,
  10 min is ≈ 400 k facts, not 250 k. 250 k would need ≈ 3.75 min of fixed overhead, and the plan does not state any.
- **The 4 h ceiling.** At the stated rates, 4 h (14,400 s) is 9.6 M facts. Nothing caps a namespace below the shard's
  10 M hard cap: `soft_cap_facts` is per shard, and there is no per-namespace limit. The DDL therefore refuses every move
  of a namespace above ≈ 9.6 M facts. §9's remedy for a full shard ("move the largest namespaces") has no answer for a
  shard that one large namespace fills.
- **The margin.** The deadline is `frozen_at + max(1.5 × W_est, W_est + 10 min)`, capped by the window. StartMove
  accepts `W_est = window`, so the cap can leave a margin of zero. For example, W_est = 3.9 h in a 4 h window leaves
  6 min of slack, against the 10 min minimum the formula promises.

**Recommendation.**
- Restate the unattended threshold as ≈ 400 k facts, or state the fixed terms that make it 250 k.
- Decide one of:
  - (a) a per-namespace live-fact cap at which a move still fits a 4 h window with margin, e.g. ≤ 6 M for
    W_est ≤ 2.5 h (enforced like `soft_cap_facts`, with a `QuotaExceeded` reason); or
  - (b) operator windows above 4 h with explicit tenant agreement, with the DDL bound raised to match.
- Refuse a StartMove whose `max(1.5 × W_est, W_est + 10 min)` exceeds the window, so the deadline margin is never
  silently truncated.

### A7-5: minor [regression]: `Move.rerun_count`, `Move.source_blobs_gc_after` and the re-run index have no catalog source; `CleanupGrace` is an option although the grace is fixed

**Where:**
- `admin.proto` `Move.rerun_count = 26` and `source_blobs_gc_after = 16`
- `workflow.proto` `MoveInput.rerun_attempt` (workflow id `move/{ns}/{epoch}/rerun/{n}`)
- `catalog_schema.sql` `namespace_moves`
- §2 `Orchestrator.Cleanup` ("records `done` and blob_gc_after")
- §5.5 (`05-pipelines.md:1785`, "`blob_gc_after = now() + 28 d`")
- §2 `StartOptions.CleanupGrace 24 h` and `Orchestrator.Cleanup(…, o CleanupOptions)`

**Evidence.**
- `namespace_moves` has no column for `rerun_count`, `blob_gc_after` or `source_blobs_gc_after`. There is no
  `rerun` anywhere in the catalog DDL.
- The re-run index `n` therefore lives only in Temporal history. After a worker restart, or after the catalog reconcile
  of N163, the next re-run cannot derive `n`, and the API cannot fill `rerun_count`.
- `source_blobs_gc_after` could be derived as `finished_at + 28 d`, but §2 and §5.5 say it is written.
- §2 has `StartOptions.CleanupGrace` (24 h) as a parameter, and `Orchestrator.Cleanup` takes a `CleanupOptions` that is
  never defined. Meanwhile the catalog `done` CHECK hard-codes `activated_at + interval '24 hours'`, and the proto
  reserves `skip_grace` because "the grace cannot be shortened".

**Recommendation.**
- Add `rerun_count integer NOT NULL DEFAULT 0` to `namespace_moves`, incremented by the CAS that admits a re-run. Derive
  the workflow id's `n` from it.
- Either add `source_blobs_gc_after` or document it as derived from `finished_at`, and fix §2 and §5.5 to match.
- Delete `CleanupGrace` from `StartOptions` and the `CleanupOptions` parameter, or make the grace a catalog column that
  the CHECK reads.

### A7-6: minor: the shard DDL and §3's edge table still call `cutover_c` "the point of no return"

**Where:**
- `shard_schema.sql:653`: `('cutover_c', …, 'cutover (c), the point of no return; clears freeze_reason')`
- `03-data-model.md:274`: "`cutover_c` | … | (c) **point of no return**"

**Evidence.** Since N125, and restated in N160, N161, the proto, §4, §5.5 and the catalog DDL, the point of no return
is the catalog CAS (a″). (c) runs only after (a″) has been read and replicated. The two descriptions are generated
documentation (§3's edge table is "generated from `ownership_transitions`"), so the stale text propagates.

**Recommendation.** Change both to "(c), after the point of no return (a″) was read and replicated (N163)".

### A7-7: minor: `DocumentDeleted`'s removed fields reserve numbers but not names, contradicting §4.5

**Where:**
- `events.proto` `DocumentDeleted`: `reserved 2 to 6, 8 to 15;` with no name list
- §4.5: "every removed field number and name is `reserved`"
- `events.proto` header: "Removed field numbers are always reserved"

**Evidence.** `buf breaking --against <first commit>` with `WIRE_JSON` reports: "field 2 `fact_ids` … deleted without
reserving the name", and the same for `chunk_ids`, `observations_marked_stale`, `observations_retired` and
`pages_marked_stale`. Every other withdrawn field in the tree reserves its name. A protojson consumer of the outbox (the
Kafka sink) could get one of these JSON keys back with a new meaning.

**Recommendation.** Add `reserved "fact_ids", "chunk_ids", "observations_marked_stale", "observations_retired",
"pages_marked_stale", …;` covering every name that was at 2–6 and 8–15. Add a CI rule: `buf breaking --against
<first tagged tree> --config WIRE_JSON` must be clean.

### A7-8: minor [regression]: what `COPIED` means is stated three ways, and `MoveResult` says there is no re-copy

**Where:**
- `admin.proto` `MoveState` header: "COPIED means they finished and the consumer-cursor wait is next"
- The `MOVE_STATE_COPIED` value comment: "Copy verified by equality, indexes valid, consumers past the final seq"
- §5.5 step 7: `AwaitConsumers`, then `namespace_moves.state = 'copied'`
- `catalog_schema.sql:49`: "copied (FrozenCopy + VerifyFrozen + BuildIndexes done)"
- `workflow.proto` `MoveResult`: "`rows_recopied = 6` (there is no re-copy under the freeze)"

**Evidence.**
- The header comment puts the consumer wait inside `COPIED`. The value comment and §5.5 put it before `COPIED`. The SQL
  comment does not mention it. An operator reading `GetMove` cannot tell whether a `COPIED` move is still waiting on
  consumers, and the 120 s rollback bound applies to that wait.
- `VerifyFrozenReport.tables_recopied`, `MoveProgress.verify_attempts` and §5.5 step 5 all describe exactly one re-copy
  under the freeze, which `MoveResult`'s comment denies.

**Recommendation.** Use §5.5's definition (`copied` = verify, indexes and consumer wait all done) in all four places.
Reword `MoveResult`'s comment to "the per-table re-copy is reported in `VerifyFrozenReport.tables_recopied`".

### A7-9: minor (unverified): a re-run is admitted while the move is `cleaning`, and the live edge `reconcile_in` reuses the withdrawn mechanism's name

**Where:**
- `shard_schema.sql` `ownership_transitions` `reconcile_in` ("only while the catalog move is committed/cleaning")
- §2 `Closer.Rerun`
- `admin.proto` `CleanupMoveResponse` ("CLEANING while batches run")
- `workflow.proto` `MoveCheckpoint` (reserved name `reconcile_in`)

**Evidence.**
- `cleaning` is the state in which `engram_cleanup_namespace` batches are deleting the namespace's rows on the source.
- A re-run copies from "the retained `moved_out` source". If a target is restored to a pre-activation point while
  cleanup batches run, which needs a deliberate PITR to an old time because the gate requires a post-activation backup,
  the re-run copies a partially deleted source.
- `VerifyFrozen` compares the target with that same partial source and passes.
- §5.5 covers only "after cleanup" (the recovery move), not "during cleanup".
- Separately, `reconcile_in` is a live ownership edge name (the re-run's re-admission). The same name is the reserved
  proto field of the withdrawn D24 `ReconcileIn` repair, and it appears throughout the change tables as a withdrawn
  mechanism. That makes the documents hard to grep and invites confusion in review.

I did not model-check this; it belongs to the correctness lens.

**Recommendation.** Allow a re-run (and `reconcile_in`) only while the move is `committed`. Once `cleaning` has begun,
route a pre-activation target restore to the recovery move. Rename the edge to `rerun_admit`.

### A7-10: minor (unverified): the subject-connection count and the "a quarter of the pool" claim assume exactly two API processes per shard

**Where:**
- N164(5) and N155 ("direct `engram_subject` 2 per API process … ≈ 53 + 4 = 57")
- N164(3) ("a second semaphore of 4 per API process per shard inside the 32, so filtered arms cannot take more than a quarter of the recall pool")
- §9.1 host table: "app host | `envoy`, `engram-api` ×2, … | 2 to 3"

**Evidence.**
- Every API process connects to every shard of its cell. A recall pool of 32 is per shard (pgbouncer), and both
  semaphores are per process.
- With *P* API processes, the subject connections are 2*P* and the filtered arms can hold up to 4*P* of the 32 pool
  slots. "+4" and "a quarter" both hold only at *P* = 2.
- §9.1's table can be read as two replicas per app host on two to three hosts, which gives *P* = 4 to 6. That means 8 to
  12 subject connections (still under 100) and filtered arms able to take half to three quarters of the pool.

**Recommendation.**
- State *P* per cell in D3 and §9.1.
- Make the filtered limit a pool-level quantity: pgbouncer has no per-class limit, so either give filtered arms a
  separate alias pool of 8, or set the per-process filtered semaphore to `⌊8 / P⌋`.
- Compute the connection count as 53 + 2*P*.

### A7-11: nit: "≈ 65 ms … fits the 60 ms arm budget"

**Where.** N164(2). §1:425, §2:819 and §3:858 repeat the 65 ms figure.

**Evidence.** 10 k rows × 6.6 µs = 66 ms, which is above the 60 ms arm budget. An unfiltered recall can sit just below
θ, which puts ≈ 6 ms on the 224 ms path. The 300 ms SLO still holds.

**Recommendation.** Say "≈ 65 ms, 5 ms over the arm budget at θ, inside the SLO headroom", or start θ at ≈ 9 k.

### A7-12: nit: small parity slips

- §10 Gantt: the "Phase 2 exit LME-S report" milestone is placed `after e1l` (day 195). The text says the Phase 2 exit
  is E3's M2.2 on day 201. Anchor it `after e3g`.
- `proto/README.md`: "the breaks of rounds 3, 4 and 6 are listed in plan section 4.5". Round 5's `expired_at` rename is
  listed there too (row 5).
- "46 hosts" (D3, §9.1, N165) counts shard hosts only, while §9.1's per-cell table adds 6 to 7 app, Temporal and
  control hosts per cell. Say "46 shard hosts".
- §2 `Moves.Plan` and §5.5/§9 name the refusal `MovePrecondition{WINDOW_REQUIRED}`. The wire detail is
  `PreconditionFailed{type: "MOVE_WINDOW_REQUIRED"}` (and `MOVE_WINDOW_TOO_SHORT`). Name the wire form where clients
  read it.

## Checked and found consistent

- buf lint and build are clean. FILE-category breaking against round 5 equals §4.5's row 6 exactly. Every D25
  withdrawal reserves its number and name (`MoveState` 2/3/9, `MoveProgress` 10–15, `Move` 18–20,
  `CleanupMoveRequest.skip_grace`, `MoveInput` 12, `MoveCheckpoint` 10/14/15/16/18/19, `MoveResult` 6).
- The move fields are present and documented:
  - `MoveState.FROZEN = 4` and `COPIED = 11`
  - `NamespaceFrozen.frozen_until_estimate = 4`
  - `StartMoveRequest.window`, `freeze_not_before` and `estimate_only`
  - `StartMoveResponse.window_estimate`
  - `PreconditionFailed` types `MOVE_WINDOW_REQUIRED` and `MOVE_WINDOW_TOO_SHORT`
- The catalog `move_state` enum and transition trigger match `fsm.MoveState` and the proto order.
- The MCP set is closed at 36 RPCs (10 + 21 + 5).
- §4's `common.proto` and `memory.proto` embeds are byte-identical to the files.
- §2: no interface has more than five methods. `Copier` (5), `Freezer` (2), `Cutover` (3), `Handover` (3), `Closer` (4
  with `Rerun`) and `Orchestrator` (4 with `Cleanup`) match §5.5's step table and the catalog CHECKs (except A7-5).
  Ids are typed.
- Fleet, recall-path, Erlang, fill and θ arithmetic all recompute (see "What was executed").
- The roadmap phase sums and per-engineer chains recompute and agree across §10, §0, README and the register
  (13.0 / 50.0 / 21.25; 84.25 / 123.5; MVP week 24; Phase 2 exit week 29).
- `PreVerify`, `ReconcileIn`, catch-up, `replay_floor_mirror`, `COPYING`/`RECONCILING` and
  `engram_move_indexes_ready` appear only in change tables, rejected-alternative and non-goal rows, reserved
  declarations or must-fail descriptions. The two exceptions are the live `reconcile_in` edge (A7-9) and the stale
  §7.7/M0.7 configuration names (A7-2).

## Counts

| Severity | Count |
|---|---|
| Blocker | 0 |
| Major | 0 |
| Minor | 10 (A7-1 to A7-10; 4 marked [regression]; 2 unverified) |
| Nit | 2 (A7-11, A7-12) |

## Verdict

The API surface is in good shape. buf is clean, the round-6 withdrawals are reserved correctly, the §4 embeds are
exact, the MCP set closes at 36 and the §2 interfaces respect the five-method rule. Every headline number recomputes
and agrees across the sections, README and register: 5.5 M, 182 / 6 / 46, θ = 10 k, 224 ms, pools 32/8, 84.25 / 123.5,
13.0 / 50.0 / 21.25, and MVP week 24. Nothing in this lens rises to blocker or major.

What remains is propagation debt from D25:
- test names renamed in §8 but not in §2, §7 or the register, plus round-5's A-16 fix that was never applied
- the §7.7/`RESULTS.md` tables still describing the D24 `ShardMove`
- stale connection figures in N54, N114 and N155
- three move fields without a catalog column

Two items need a decision rather than an edit: whether namespaces above ≈ 9.6 M facts must stay movable (A7-4), and
whether a re-run may start while the move is `cleaning` (A7-9). Fixing A7-1 and A7-2 by generating the lists, not by
hand, would stop this class of finding from recurring.

## Not verified

- A7-9's interleaving was not model-checked: `ShardMove.tla` has no cleanup-in-progress plus pre-activation-restore
  case that I ran.
- A7-10 depends on how §9.1's "engram-api ×2" is meant to be read.
- I did not re-run TLC. I read the committed logs.
- I did not compile a Go stub module. Method counts and types come from parsing the §2 blocks.
- I did not check the `events.proto` and `workflow.proto` field lists against every §5 activity payload, beyond the
  move messages.
