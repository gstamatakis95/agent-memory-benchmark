# Engram: adversarial review 5 (API contract, numbers, completeness, Hindsight parity)

**Reviewed:** `plans/engram/` at `fce0cd4` (v1.2, after D20 and D21), including `proto/`, `sql/`, `formal/`, the sections and `reference/hindsight-notes.md`, against the task brief.
**Lens:** (1) the API contract: protos, §4, MCP and Connect mappings, the evolution policy and `buf breaking`; (2) the numbers: D3, N54/N106, N109, §3.7, §6.8, §10/N105 and the places that cite them; (3) completeness against the brief; (4) Hindsight parity.
**Not repeated:** findings accepted in `REVIEW.md` (F-1…F-45) and `REVIEW-2.md` (G-1…G-30). I raise one of those again only where its fix is missing or wrong, and I say so at that finding.

**Tool runs (from the repository root, because the `.git#` form fails inside `proto/`):**

```text
$ buf lint / buf build / buf format -d --exit-code     (in plans/engram/proto)  → all exit 0
$ buf breaking plans/engram/proto --against '.git#ref=50304db,subdir=plans/engram/proto'
  25 breaks, all in buf.build/engram/internal, none in memory.v1 / memory.admin.v1:
  23 × events.proto  Field "N" … changed type from "string" to "bytes"   (ChunkCommitted, ChunksRetired,
       DocumentDeleted, FactInvalidated, FactRestored, Observation*, Entity*, Page*)
   1 × events.proto  Previously present field "2" with name "ids" on message "RowsPurged" was deleted
   1 × workflow.proto Previously present field "9" with name "item_index" on message "ChunkWork" was deleted
```

My judgement of these breaks is A-11.

Severity scale (same as the earlier reviews): **blocker** means a stated guarantee is false or a required path cannot run; **major** means a wrong number or contract that changes a decision; **minor** means drift or a gap with a local fix; **nit** means cosmetic.

---

## Findings

### A-1: blocker: deleted content is still served through pages. Page versions have no deletion hiding, so D16 is false for Reflect, `GetPage` and `SearchPages`

**Where.** `proto/memory/v1/page.proto` header and `GetPage`/`SearchPages`; §3.3.6 `page_versions` DDL; §5.4.6 table (row "Explicit delete", column Pages: "`stale_delete` in the cascade"); §2.2.16; D16; §4.4 ("no … page version derived from content mentioned after T is returned").

**Claim.** D16: "from then on nothing from the document is returned by Recall, Reflect, GetMemory or Export." page.proto: "a delete-stale page may be citing content that must not be shown, while a write-stale page is merely incomplete."

**Evidence.**
- The delete cascade does only two things to pages. It deletes `page_sources` and sets `pages.stale_delete = true` (§5.4.1 step 8; §5.4.6).
- `page_versions(… markdown_blob_key, effective_at, superseded_at, evidence_hash …)` has no `derived_from_deleted`, no `hidden_by_invalidation` and no inputs table. Nothing in §2.2.16, §5.3 or `GetPage` withholds a `stale_delete` page.
- So after an acknowledged delete, all of the following still return markdown written from the deleted facts or observations until a refresh lands:
  - `GetPage` (current version, or any older version through `version`/`as_of`);
  - `SearchPages` (`include_content`, and `snippet` ≤ 512 B);
  - Reflect's `get_page` and `search_pages` tools;
  - the MCP `get_page`/`search_pages` tools.
- The refresh is asynchronous. If `refresh_policy.on_delete` is false, it waits for cron. Old versions are never fixed: a refresh writes a new version and leaves the old ones servable by `version`/`as_of`. This is the F-1/G-1 bug again, one level up the derivation chain. N41/N79/N84 were applied to observation versions only.
- `Invalidate` also marks pages `stale_delete` (§5.4.6), with the same consequence.

**Recommendation (decision level).** Treat pages exactly like observations:
- `page_version_inputs(page_id, version, source_kind, source_id)` records every fact and observation version shown to the refresh prompt.
- The cascade sets `page_versions.derived_from_deleted` (permanent) and the reversible `hidden_by_invalidation` counter. It also handles transitive lineage, because the previous page text is in the prompt.
- `GetPage`, `SearchPages` and Reflect skip flagged versions, and a `stale_delete` current version is hidden until rewritten. `GetPage` returns `NOT_FOUND{PAGE version}` or a typed `PreconditionFailed{PAGE_STALE_DELETE}`.
- Add pages to `DocLifecycle.tla`'s `NoDeletedContentRecalled` and to the §8.5 leak canary.

### A-2: blocker: move rollback after cutover (b) cannot execute. `ReleaseNamespace` has a different contract, and the ownership state machine has no edge out of the target's `active` row

**Where.** N98; §5.5.1 step 10 and the §5.5 failure table ("a target outage or watchdog expiry here rolls back by `ReleaseNamespace(target, e + 1)` then thawing the source"); `proto/memory/admin/v1/admin.proto` `ReleaseNamespace`; `sql/shard_schema.sql` `ownership_transitions` (lines 709–726) and `engram_cleanup_namespace` (line 1690).

**Claim.** N98: "a failure before (c), including a target outage after (b), rolls back by `ReleaseNamespace(target, e + 1)` then thawing the source." §5.5.1: "(admin RPC: deletes the target's rows and ownership)".

**Evidence.**
1. admin.proto defines `ReleaseNamespace` as "after the PurgeNamespace workflow removed the last rows it marks the catalog row DELETED". Its request carries `epoch`, and the call is "FAILED_PRECONDITION if the catalog disagrees". During this rollback the catalog still says epoch `e` (step (d) never ran), so the call as contracted fails. If an implementer "fixes" the precondition, the RPC marks the namespace DELETED in the catalog.
2. After (b), the target row is `active @ e+1`. The immutable transition table admits `rollback_target` only `FROM incoming`. The only edges out of `active` are `start_move`, `abort_move`, `freeze_*` and `epoch_bump`. No role may delete or demote an `active` target row, and `engram_cleanup_namespace` deletes data only for `moved_out`/`incoming`.
3. The watchdog-driven rollback therefore fails on every retry. The source stays `frozen` (writes refused with `NamespaceFrozen`) with no automatic exit. This is exactly the window N98 promised to make safe.

**Recommendation.** Make the target revert a first-class edge: `revert_target` = `active → incoming` at the same epoch, role `engram_move`, allowed only while the catalog and the source show the move non-terminal and the source row is `frozen/move`. After it, the existing `rollback_target` delete applies. Give it its own admin RPC (`MoveService.RevertTarget`) instead of overloading `ReleaseNamespace`, whose delete semantics must stay as they are. Add the "target outage after (b)" interleaving to W-12 as a must-pass configuration.

### A-3: major: the hot working set leaves out the facts heap that N94 made 2.4× larger. The "IOPS sizing in 3.7" does not exist

**Where.** §3.7 (facts row; hot-working-set paragraph "≈ 40 GB"); D3 footprint row; §3.3.4 ("heap fetches per recall × QPS enter the IOPS sizing in 3.7"); §2.2.8 (`PlanPartitionHNSW`); N94, N109; A-1 (NVMe ≥ 10 k IOPS).

**Claim.** The hot set is "facts HNSW 18 GB, BM25 4 GB, facts B-trees 3.4 GB, chunk and observation HNSW 3 GB, … ≈ 10 GB → ≈ 40 GB".

**Evidence.**
- **The heap is not in the hot set.**
  - pgvector's HNSW stores only the vector. Every visited tuple is a heap fetch, which applies `namespace_id`, `live`, `mentioned_at` and tags.
  - At the 10 M-fact target, N109 says the partial-index band is empty, so every namespace ≥ 20 k facts uses the shared partition index.
  - With 150 namespaces in 16 partitions, a typical namespace is ≈ 10–11 % of its partition. A MID query (cap 150) then visits ≈ 1,400 tuples, and HIGH (400) ≈ 3,700, each a random heap page.
  - Namespaces < 20 k use the exact plan, which is itself a heap scan of ≤ 20 k × 2.4 KB = 48 MB.
  - So the facts heap is on the read path of every recall.
- **The heap is bigger than stated.** With `STORAGE MAIN` the heap is not 12 GB but "≈ 25 GB". The table's own "3 rows per 8 KB page" gives 10 M / 3 × 8 KiB = **27.3 GB**.
- **The hot set exceeds RAM.**
  - Realistic hot set: ≈ 40 + 27 (facts heap) + ≈ 5.6 (chunks heap, same argument for the chunk arm) ≈ **73 GB**.
  - Available: a 64 GB instance with `shared_buffers = 16 GB`, `work_mem = 64 MB` × connections, and pg_search and autovacuum memory.
- **IOPS.** At 50 QPS × ≈ 1,400 page touches ≈ 70 k page reads/s for the fact arm alone, a miss rate of 15 % saturates the A-1 10 k IOPS.
- **No IOPS sizing exists.** §3.3.4 and §2.2.8 defer to "the IOPS sizing in 3.7", and §3.7 contains none.

**Recommendation.**
- Decide one of:
  - (a) revert facts to `STORAGE EXTERNAL`/TOAST, and accept the TOAST probes only on the exact plan; or
  - (b) size the shard at 128 GB RAM; or
  - (c) lower the soft cap to ≈ 6 M facts.
- Write the IOPS table that §3.3.4 promises: page touches per recall by plan and budget, times QPS, times assumed miss rate. Gate M0.6 on it.
- Restate D3's "≈ 40 GB hot" from the result.

### A-4: major: time-to-fill is about 2.7× too optimistic, and the A-F = 4 sensitivity goes in the wrong direction. This is the F-34/N74 fix, and its arithmetic is wrong

**Where.** D3 "Retain throughput"; §1.4, §5.1.6; §6.8 Table 6.8-B rows "Initial fill of 1 B facts" and "Wall time of an LME-S run"; R30; executive summary ("≈ 29 days and ≈ $160 k"); M3.7.

**Claim.** "≈ 29 days of continuous ingest at 10 chunks/s × 4 cells … At A-F = 4: ≈ $90 k, ≈ 12 days." §5.1.6: "5–10 chunks/s/worker ≈ 15–30 k chunks/h ≈ 60–120 k facts/h."

**Evidence.**
- **Every gateway call per chunk shares the cap.** The same table states that for LME-S every stage shares the 600 RPM cap ("the earlier '≈ 6 h' counted extraction only"). Per chunk at A-F = 10 that is 1 extract + 1.25 consolidation batches + 0.1–0.25 summaries + 0.375 dedup ≈ **2.7 calls**.
  - Sustained ingest at 600 RPM ≈ 10 / 2.7 ≈ 3.7 chunks/s per cell.
  - 100 M chunks / (4 × 3.7) ≈ **78 days**, not 29.
  - The "29 days" figure counts extraction only, which is the error the LME-S row says was fixed.
- **A-F = 4 for 1 B facts needs more chunks, not fewer.**
  - 1 B facts at A-F = 4 is 250 M chunks: ≥ 72 days on extraction alone, ≈ 190 days with every stage sharing the cap.
  - The table's own "Per 1 k facts" row says ≈ $0.21 at A-F = 4, i.e. ≈ **$210 k** for 1 B facts, not $90 k.
  - "12 days" equals 29 × 4/10. It follows from neither reading.
- **Facts per hour.** 5–10 chunks/s = 18–36 k chunks/h = 180–360 k facts/h at A-F = 10. "60–120 k facts/h" is the A-F = 4 figure, which contradicts N74's single-assumption rule.
- **Document count.** The fill row says ≈ 10 M documents, but the per-1 k-facts row (25 documents per 1 k facts) implies 25 M.

**Recommendation.**
- Restate throughput as gateway calls per chunk, and give the D3 formula as `min(32/L, RPM_cap / (60 × calls_per_chunk))`. Note that consolidation can use a different model and therefore a different RPM cap.
- Re-derive the fill row for both A-F values with a fixed fact count.
- Decide explicitly whether `RetainBackfill` (now on the uncommitted track) is a launch prerequisite. At ≈ 78 days per online fill it probably is.

### A-5: major: the public `Operation` cannot express what the pipelines do. The read-barrier promise is false for superseded retains, and tenant deletes have no operation kind

**Where.** `proto/memory/v1/operation.proto` (`OperationResult`, `OperationKind`, file header); N49; §5.1.2 step 7.4; N70; §4.1.1 ("so the `DELETE_NAMESPACE`/`DELETE_TENANT` operations … can be awaited"); D16.

**Claim.**
- N49: "ends `SUCCEEDED` with `superseded_by = <version>` in the operation result".
- operation.proto: "after WaitOperation returns SUCCEEDED for a retain, Recall on the same namespace observes every fact of that document version."

**Evidence.**
- **`superseded_by` is missing.** `OperationResult` has no such field; it exists only in the internal `FinalizeVersionResult`. §5.1.2 also sets `document_version = c` (the *current* version, not the operation's own).
- **Clients cannot see a superseded `REPLACE`.** A client cannot tell that its version never became active. The barrier sentence is then false: superseded chunks are retired by the newer finalise.
- **Retain racing a delete.** This ends `CANCELLED{document_deleted}` (§5.1.2 step 7.3), but `OperationError`/`CANCELLED` carry no reason field for a non-FAILED state.
- **`DELETE_TENANT` is missing.** `OperationKind` has no `DELETE_TENANT`, yet N70 and §4.1.1 promise to await it. `DeleteTenantResponse` returns per-namespace operations only.
- **`MOVE_NAMESPACE` has no backing workflow.** It is a public kind, but the move workflow is `move/{ns}/{epoch}`, not `ns/{ns}/op/{operation_id}`. That contradicts the header ("every … Operation [is] backed by exactly one Temporal workflow id `ns/{namespace_id}/op/{operation_id}`"), and nothing says on which shard's `operations` table it lives.

**Recommendation.**
- Add `OperationResult.document_version` semantics ("this operation's version"), `superseded_by`, and a `cancel_reason` enum.
- Restate the barrier as "…observes every fact of that version unless `superseded_by` is set".
- Either add `OPERATION_KIND_DELETE_TENANT` with a tenant-scoped `GetOperation` (admin API), or delete the promise.
- Define where the MOVE operation row lives, or drop the kind from the public enum.

### A-6: major: idempotency keys at the edges collide or are not deterministic

**Where.** §4.6 MCP table (`retain`: "`request_id` = MCP request id"); §4.1.3; `RetainItem.document_id` comment ("Empty: the server mints a UUIDv7"); `RetainRequest.operation_id` ("per-document ids are derived as UUIDv5(operation_id, document_id)").

**Evidence.**
- **MCP request ids are per-session counters.** JSON-RPC ids in MCP are unique only within a session (typically `1, 2, 3…`). `request_id` is scoped to (tenant, namespace, method) for 24 h. Every reconnecting agent (or two agents mounting the same namespace) reuses `id = 1…`.
  - The first `retain` with different content fails `ALREADY_EXISTS{IDEMPOTENCY_KEY_REUSED}`, and the §4.6 error text tells the agent to "use a fresh id", which it does not control.
  - Identical content is silently answered with the other session's stored response.
- **Minted document ids break retry idempotency.** For items without `document_id`, the minted id is a fresh UUIDv7 per attempt. A retry that carries `operation_id` but no `request_id` (or comes after 24 h) derives different per-document operation ids, so it creates duplicate documents and duplicate facts.

**Recommendation.**
- MCP: derive `request_id = sha256(mcp-session-id ‖ jsonrpc-id ‖ tool)`, or omit it and use `operation_id = UUIDv5(session, id)`.
- Minted documents: derive the document id deterministically, `UUIDv5(operation_id, "item/" ‖ index)`, whenever an `operation_id` is present. Without one, document that such retains are not replay-safe.

### A-7: major: a per-namespace `models.embed` or `EmbedDims` override silently corrupts semantic recall

**Where.** §2.2.22 `Models{… Embed …; EmbedDims int /* 768; 512 with Matryoshka truncation */}`; D12 keys `models.{…,embed,…}` (also in the namespace.proto comment); D15; `sql/shard_schema.sql` (`embedding halfvec(768)` and `embedding_model text NOT NULL` on facts, chunks and observation versions).

**Evidence.**
- **Mixed vector spaces.** A tenant `memory.admin` can set `models.embed` on a namespace. From then on, queries are embedded with the new model and new facts get new-model vectors, but every existing vector stays.
  - Delta retain keeps unchanged chunks with "no embedding".
  - `extraction_key` does not include the embed model.
  - `embedding_model` is written and **never read**: no arm filters on it, and no workflow re-embeds on change. (grep: the column appears only in DDL and INSERT lists.)
- **512-d does not fit.** `EmbedDims = 512` cannot be stored in `halfvec(768)` at all.

**Recommendation.**
- Make `models.embed` and `EmbedDims` system-level only, or fixed at namespace creation.
- If a change is allowed, it is a versioned migration: a `ReembedNamespace` workflow, dual vectors or a shadow column during the switch, and the arms filtering `embedding_model = current`.
- Reject the override in the config allow-list until then.

### A-8: major: quota deferral covers only retain extraction, about 40 % of LLM spend. Consolidation, Reflect and page refresh are unmetered at admission, and the share rule starves new namespaces

**Where.** §5.1.2 step 5 (`CheckQuota` before each retain wave); §5.2 (no quota step); §2.4 limits table (Reflect: "≤ 4 concurrent reflects per namespace"); D13; §6.8 ("consolidation is ≈ 53 % of all-in ingest cost"); PD-4.

**Evidence.**
- **Only retain checks the quota.** `CheckQuota` appears only in the retain step table. Consolidation (53 % of ingest cost), Reflect (strong model, $0.04 typical and $0.45 worst case per call) and page refresh (strong model) have no deferral or admission check on `llm_tokens_per_day`.
- **Reflect has only a concurrency cap.** 4 concurrent × 300 s sessions allow ≈ 48 worst-case reflects per hour per namespace (≈ $21/h), unbounded by the daily quota.
- **The share rule penalises ingesting namespaces.** The per-namespace share is `tenant_limit × max(live_facts, 1000) / Σ`, recomputed daily. It is proportional to facts already stored, not to ingest demand. A tenant with one 1 M-fact namespace and a new one gives the new namespace ≈ 0.1 % of the tenant's tokens, so its first document defers at once. That is the opposite of the workload.

**Recommendation.**
- Put one `quota.Reserve(tokens_estimate)` gate in front of every gateway call class: extract, consolidate batch, refresh, and each Reflect iteration. Reflect gets `RESOURCE_EXHAUSTED` because it is synchronous; workflows defer.
- Compute shares from trailing usage plus a floor, and allow borrowing of unused share at the next daily recompute.
- State the task's "quota enforcement with deferral" per call class in D13.

### A-9: major: `PageService`'s refresh contract cannot express what §5.3 reads, and §5.1.6 cites a `Retain` field that does not exist

**Where.** `page.proto` `RefreshPolicy{RefreshTrigger trigger; Duration interval; Duration debounce}`; §5.3 trigger table (`refresh_policy ? 'cron'`, `refresh_policy.cron`, `refresh_policy.force_on_cron`, `refresh_policy.on_delete = immediate`); §2.2.16 `Page{… TagFilter []string; RefreshPolicy string /* "after_consolidation" | "cron:<expr>" | "manual" */}`; §5.1.6 ("started by `engramctl backfill` or by `Retain{priority = BATCH}`").

**Evidence.**
- **`RefreshPolicy` mismatch.** The proto has a periodic `interval` (1 h…30 d) and no cron, no `force_on_cron` and no `on_delete`. The pipeline evaluates a cron expression and an `on_delete` flag that no client can set.
- **Go `Page` mismatch.** The Go `Page` type uses a bare tag list where the proto has a `TagFilter` with a mode.
- **No `priority` field.** `RetainRequest` has fields 1–4 only. N105 moved `RetainBackfill` off the committed track, but the trigger path through `Retain` is still described.

**Recommendation.**
- Choose one `RefreshPolicy` (add `cron`, `on_delete`, `force_on_cron` to the proto, or rewrite §5.3 to interval semantics) and generate the §2 type from the proto.
- Either add `RetainRequest.priority` (enum `INTERACTIVE|BATCH`) or remove the sentence.
- Extend the N103 `gen-docs` check to proto-field references in §5.

### A-10: major: the propagation pass that N103 accepted was not done. Binding register rows contradict N53, N54, N106 and N82, and the rerank-skip threshold has three values

**Where.** D2 rationale cell; D3 recall row; D5 steps 2 and 6 plus its safety argument; D10 Rerank row; D15; D17; §9.3 config `rerank_min_remaining: 120ms`; executive summary; §11 Q16.

**Claim.** N103: "The propagation pass rewrites D2, D3, D5 and D15 in place (D20/D21 become history)… (8) D3's recall row follows N53/N54 (216 ms p95, `bge-reranker-base` in D15)."

**Evidence.** As of `fce0cd4`:
- **D3** still reads: "cross-encoder on 150 pairs ≤ 120 ms p95 … → ≈ 215 ms", "5 arms in parallel ≤ 60 ms".
- **D15** still names `bge-reranker-v2-m3` as the rerank default.
- **D5** step 2 still takes "`SELECT … FOR UPDATE` on the source ownership row (it waits for every in-flight writer, which holds `FOR SHARE`…)", and its safety argument still relies on "`FOR SHARE` on an ownership row".
- **D2** rationale: "the ownership `FOR SHARE` row is the fencing token for shard moves."
- **D10** (not in G-25's list) still has rerank on the "top 50/150/300" and skip when "remaining deadline < 150 ms". The §4 proto says 0/50/150 and 106 ms (N106), and the production config in §9.3 says `rerank_min_remaining: 120ms`. That is three thresholds, one of them in a binding row and one in the deployed config.
- **Executive summary** still says "a partial HNSW for large ones", which N109 contradicts (the band is empty at the target).
- **D17** still says 112/74, while N105 says "total ≈ 118 ew … unchanged", although the earlier total was 112.
- **§11 Q16** still debates `stale_delete` for invalidation, which N84 removed.

This is G-25 items 1 and 8: the fix was accepted but not applied.

**Recommendation.** Do the fold now; it is mechanical. Make `rerank_min_remaining` a derived constant (`rerank_p95 + pack + stream + 8 ms`) with one source, and lint D-rows against N-rows in `make gen-docs`.

### A-11: major: the internal schema changes since `50304db` break the plan's own evolution policy. They are intended but not recorded as exceptions, and the in-place retype decodes silently wrong

**Where.** `buf breaking` output above; §4.5 ("The internal module follows the same `buf breaking FILE` gate as the public one … a running Temporal history *is* a wire client"; "Change a field's number, type … → Add a new field, deprecate the old one"; "A field whose interpretation changes bumps [`schema_version`]"); events.proto header ("Changes are additive; removed field numbers are reserved"); `proto/README.md` (CI compares against `main`, "the only override is a `v2` package").

**Judgement.**
- **The breaks are intended.** All 25 trace to documented decisions: N80 (ids as 16-byte `bytes`, `RowsPurged.ids` removed) and N86 (`item_index` → `item_indexes`). The two deletions are correctly `reserved` by number and name. Nothing in `memory.v1` or `memory.admin.v1` broke.
- **They are not documented as policy exceptions.** No changelog entry or exception note exists, and `Event.schema_version` and `RetainDocumentInput.schema_version` stay `1`.
- **The CI gate would block the change.** It would fail the very PR that made it, with no sanctioned override.
- **The retype is unsafe.** `string → bytes` on the *same* field numbers is wire-compatible, so a consumer built at the new commit decodes a 36-byte ASCII UUID from a pre-change event (7-day outbox retention, Kafka retention, open Temporal histories) as a 36-byte "id" without an error. That is exactly the silent misinterpretation §4.5 forbids.

**Recommendation.**
- Declare in §4.5 a pre-1.0 exception for both modules, with the list of breaks, and bump both `schema_version`s to 2. Better still, renumber: new `bytes` fields and reserve the old `string` ones.
- Add a CI `buf breaking` baseline tag (`proto/v1.0.0`) so the gate compares against a release, not `main`.

### A-12: minor: internal-only error details live in the public package and are therefore frozen forever

**Where.** `proto/memory/v1/errors.proto` (`WrongShardOrEpoch.target_shard_id/next_epoch`, `DocumentBusy`, `InputBlobMissing`, `FREEZE_REASON_FENCE_BUSY`); admin.proto header ("This is the ONLY place where shard ids and epochs appear in an API"); §4.5 ("Nothing is ever removed from `memory.v1` itself").

**Evidence.**
- §4.1.6 says these "never reach public callers" and that `target_shard_id` is "stripped before the error leaves the API". Stripping is a runtime discipline, not a contract: one missed strip leaks shard ids.
- Under the plan's own rule, the messages can never be removed from `memory.v1`.

**Recommendation.** Move them to `engram.internal.errors.v1` (or `memory.admin.v1`), and keep a public `WrongShardOrEpoch` without shard fields.

### A-13: minor: proto comments, the stated source of truth, contradict the decisions

**Where.** `memory.proto` `Invalidate` ("Observations keep the source row but are marked stale_delete and hidden from Recall until reconsolidated"); `Restore` (no decrement, no `remote_apply`); `namespace.proto` `config_overrides` (lists only the D12 keys and rejects unknown keys); `export.proto` header (delta "derived from the outbox" and present only "when … its outbox range has not been pruned"); §4.5 deprecation text ("the three removals … `wait_for_visibility`, `min_score`, `include_untagged`").

**Evidence.**
- N84: invalidation marks the observation `stale_write` (visible) and uses the per-version counter; the observation-level `stale_delete` was removed.
- N66 says the `config_overrides` comment is generated from `config.Keys`; it is not. `Keys` itself lacks N79's `consolidate.max_rebuilds_per_round`, so the resolver would reject the knob N79 introduced.
- D12/N30 make deltas always producible by diffing snapshots.
- There are four reserved removals, not three: `RetainItem.mentioned_at` (rc5) is missing from the list.

**Recommendation.** Fix the comments. Add `consolidate.max_rebuilds_per_round` to `Keys`. Make the N66 generation and the M3.4 exit ("`buf lint` fails if the proto comment and the Go table disagree") an M0.8 check instead of a Phase 3 one.

### A-14: major: "incremental export versions" degrade to full re-syncs after any delete, and synced local copies keep deleted content indefinitely

**Where.** `export.proto` `SnapshotManifest.expired` ("the next CreateSnapshot must be a full one"); N59; D12 Export; M3.2.

**Evidence.**
- **Every delete forces a full re-download.** Every document delete expires every snapshot that contains the document. In a namespace with routine deletes (chat retention, user data requests), almost every snapshot expires before the next one is taken, so every sync is a full re-download. That defeats the brief's "incremental versions".
- **The local copy is the bigger leak.** A thin client that synced v{n} keeps the deleted facts on the agent's disk until it next polls `ListSnapshots` and notices `expired`. Nothing pushes or forces the deletion, and the sync client has no stated obligation to purge.
- **The delta format already handles deletes.** It carries "ordered upsert/delete records". A delta from an expired base to the new version is exactly what removes the content locally. Expiry only needs to stop *serving the old bytes*; it does not prevent computing a delta from them.

**Recommendation.**
- Separate "base no longer servable" from "no delta from base". Always emit `delta-v{n-1}-v{n}` with delete records, even when v{n-1} expired.
- Add `deleted_ids` (or a tombstone file) to the manifest.
- Make `engram-sync` apply deletes before any read and refuse to serve a local copy whose manifest is expired. State this local-copy obligation in D16.

### A-15: major: entity names bypass `as_of` and the delete cascade

**Where.** `common.proto` `EntityRef{canonical_name, mention}` on every `Memory`; §3.3.5 `entities(canonical_name …)`, `entity_aliases`; §5.1.2 entity upsert; §5.4.1 (entities pruned only when `mention_count = 0` at purge); `EntitiesMerged` event; graph arm entity links.

**Evidence.**
- **`as_of` leak.** `canonical_name` and the alias set are namespace-wide current state, built from every mention including those after T. Under `as_of = T`, a pre-T fact is returned with a canonical name, or merge results, learned after T (for example "Bob" merged into "Robert Smith, CFO of X"). A post-T merge also creates entity-link paths between pre-T facts, so post-T knowledge shapes graph-arm ranking.
- **Delete leak.** If the canonical name or an alias came from a deleted document but the entity still has other mentions, it survives the cascade and the purge (pruning needs `mention_count = 0`). It keeps being returned on other facts' `EntityRef`.
- **Not covered elsewhere.** Neither leak is covered by the §8.5 canary or by `AsOf.tla`.

**Recommendation.**
- Under `as_of`, return only `mention` (the surface form inside the fact's own chunk) and suppress `canonical_name`. Restrict graph entity hops to mentions with `mentioned_at ≤ T`.
- On delete, recompute `canonical_name` and aliases from the remaining mentions in the purge.
- Add both cases to the leak canary.

### A-16: minor: `Invalidate` is lost on re-extraction, although §4.4 says curation "is meant to stick"

**Where.** N26, N58, N87 (new `extraction_key` → re-extract → old facts retired); §5.4.5; §4.4 edge rules ("curation is not time-travelled; a curator's decision is meant to stick"); hindsight-notes §1 ("Reprocessing a document resets any curation applied to extracted facts").

**Evidence.**
- Any re-retain after a prompt or model bump, a `retain.mission` edit, an entity-hint change or a summary/heading change (all in `render_hash`, N87) re-extracts the chunk.
- The re-extraction produces a fresh, un-invalidated twin of the curated-away fact, and N58 retires the invalidated original.
- Nothing carries `invalidated_at` across, and nothing documents the reset. Hindsight at least documents its reset.
- §6.7 names `Invalidate` as the remedy for injected content, which is exactly the content a prompt bump brings back.

**Recommendation.** Keep a `curation(namespace_id, document_id, content_hash, fact_text_hash, action)` table that is applied at `CommitChunk` to the new facts of the same chunk hash. Otherwise, state the reset in the `Invalidate` contract and return a warning in `RetainResponse` when curated chunks were re-extracted.

### A-17: major: rerank depth is cut 6× against the brief and Hindsight without a quality gate

**Where.** N53 (`rerank_top` 0/50/150); task brief ("Rerank the top ~300 candidates"); hindsight-notes §3 (`RERANKER_MAX_CANDIDATES=300`, rerank at every budget); M0.5 (measures throughput and latency only); §8.6 ablations.

**Evidence.**
- MID reranks 50 and LOW reranks nothing. The decision was driven by reranker throughput (F-6), which is legitimate.
- The plan never measures what depth 50 costs in recall. No ablation, exit criterion or risk row compares R@5 or accuracy at depth 50, 150 and 300.
- The Phase 1 exit "LME-S R@5 ≥ 0.93" does not say at which depth. Engram's §1.9 marks the row "same idea".

**Recommendation.** Add `--rerank-top {0,50,150,300}` to the B.1 ablation, required in M1.2's exit. Write the depth/latency/quality trade-off into N53 as measured, and record a departure from the brief in §11 if depth stays at 50.

### A-18: minor: Hindsight capabilities dropped without a non-goal row (part of F-33, never dispositioned)

**Where.** hindsight-notes §9 (`GET /memories/{mid}/history`, `transfer/import`, `clone`, directives CRUD with `priority`/`is_active`/`tags`, `GET/PATCH config` returning resolved and raw config, document `original_text`, `retry_operation`, `PATCH memory`, `sync_retain`); `namespace.proto` (`repeated string directives`); `memory.proto`; §12.

**Evidence.**
- **Observation history.** Engram versions observations, which §1.9 offers as its differentiator, but no RPC lists the versions of an observation. `GetMemory` has neither `version` nor `as_of`.
- **Import and clone.** There is no import or clone, so an export cannot be restored, merged or used to seed a namespace.
- **Directives.** Directives are untyped strings, losing tag-scoped and prioritised directives ("tagged directives apply only when the reflect request carries matching tags").
- **Resolved config.** No RPC shows the effective resolved config, so a tenant cannot see which model a namespace will use.
- **Document body.** `Document` does not return the body.
- **F-33 items (c) and (f).** Entity-level results, and operation retry or memory edit, were accepted in F-33 but are not in N73 or §12.

**Recommendation.** For each item, either add an RPC (cheap ones: `GetMemory.version/as_of`, `ListObservationVersions`, `GetEffectiveConfig`, `Directive{text, priority, active, tags}`) or add an NG row with a reason.

### A-19: minor: Reflect's typical cost is understated about 2×

**Where.** §6.8 prompt table (`reflect/v1`: "≈ 6 000 per iteration of tool results, cumulative; typical session 6 iterations ≈ 40 k input total") and Table 6.8-A ($0.04).

**Evidence.**
- **Billed input.** If tool results accumulate in the context, as the "cumulative" wording and the 100 k context cap imply, the six calls bill Σ (1.2 k + 6 k·i) for i = 1…6 ≈ **133 k** input tokens. Forty thousand is the *final context*, not the billed total.
- **Cost.** At 90 % cache hits that is 13.3 k × $2.50 + 120 k × $0.25 + 2.1 k × $10 per M ≈ **$0.085**.
- **Inconsistent with the worst case.** The worst-case row ($0.45) is computed cumulatively.

**Recommendation.** Use one method for both rows, and re-derive the Phase 2 cost gate and §8.8 from it.

### A-20: minor: the rationale for streaming Recall is numerically wrong, and the Connect "bookmarkable GET" cannot work

**Where.** §4.1.7 Recall row ("Unary with `repeated` results (… 4 MiB message concerns at HIGH budget)"); D10 ("flushed in groups of 10 after the last stage that fits the deadline"); §4.7 ("makes reads cacheable and bookmarkable").

**Evidence.**
- **Message size.** A HIGH response is 16 k tokens ≈ 64 KiB of text. Even 500 results with full metadata are ≈ 1 MiB, far below 4 MiB.
- **No early results.** Results stream only after packing completes, so the stream delivers no early results. A deadline cannot fall mid-stream except during the few milliseconds of serialisation.
- **What streaming costs.** Connect streaming forces POST with the binary envelope, which plain-JSON clients and curl `-d` cannot use.
- **Bookmarkable GET.** GET requires the mandatory `Connect-Timeout-Ms` and `Authorization` headers, so a bookmarked URL is rejected (`INVALID_ARGUMENT`/`UNAUTHENTICATED`). Caching responses that carry `Authorization` needs explicit `Cache-Control`, which the plan does not state.

**Recommendation.**
- Keep streaming because the brief asks for it, but state the honest rationale: the stats trailer and future progressive arms. Add unary `RecallUnary` (or a Connect-only `unary` alias) for JSON clients.
- Drop "bookmarkable", or allow a default deadline for GET only.

### A-21: minor: the delete SLO's outbox arithmetic contradicts N80's own elision rule

**Where.** N80 ("a 100 k-fact document: ≈ 5 s and ≈ 400 rows"; "When a set exceeds 4,096 ids … `ids_elided = true` and counts only").

**Evidence.** 100 k fact ids plus about 10 k chunk ids exceed 4,096, so `DocumentDeleted` is a single elided event. "≈ 400 rows" is the un-elided page count.

**Recommendation.** Restate: "1 elided `DocumentDeleted` + ≤ 16 pages for every list under the elision threshold". Size `TestDelete_CascadeScales` on that.

### A-22: minor: recall connection and CPU sizing contradict the deployed pool

**Where.** §2.4 limits ("5 arms, each one pooled conn; ≤ 50 QPS/shard → ≤ 250 concurrent arm queries per shard ≤ pool headroom"); §9.1 (`DEFAULT_POOL_SIZE: "24"`, `max_connections=100`); §4.1.2 ("the per-instance pool is only 32 (D3)") vs §2.4 and §1.8 (16 per process per shard); D3 (8 vCPU).

**Evidence.**
- **Pool size.** 250 concurrent queries cannot fit a 24-connection pgbouncer pool. The sentence confuses per-second with concurrent: concurrency ≈ 50 × 6 statements × 40 ms ≈ 12. That fits, but only with the cascade, ack and Reflect traffic sharing the same 24.
- **CPU.** No CPU budget exists for 50 QPS × 6 statements (two HNSW scans with ≈ 1.4 k heap rechecks, two BM25 Top-K, graph, temporal) plus `CommitChunk` HNSW inserts and pg_search merges on 8 vCPU.
- **Pool figure.** The "32" in §4.1.2 matches no other number.

**Recommendation.** Write one connection and CPU budget per shard (statements × mean duration × QPS, plus writes), fix §4.1.2 to 16, and add recall-under-ingest CPU to M0.5.

### A-23: minor: the schedule text disagrees with its own Gantt, and M0.8's scope overlaps work the commit already claims

**Where.** §10.2 ("*Phase 1 exit (the MVP, week 20)*" vs "Phase 1 — MVP (45 ew, weeks 4–24)" and the Gantt, where Phase 1 exit follows E1's chain 3+1+2+2+2+2+10+2 = week 24); N105 ("total ≈ 118 ew … unchanged"; it was 112); M0.6 (A-F measured "with E2's prompt" in week 4, while `extract/v1` is built in M1.1, weeks 4–13); M0.8 ("decisions N79 to N108 land in §1 to §12, `sql/` and `proto/`").

**Evidence.**
- **Totals.** The milestone sums are correct: Phase 0 = 15, Phase 1 = 45, Phase 2 = 17, Phase 3 = 27, F/B = 14, committed 77, separate 41, total 118. Per engineer: E1 25.5, E2 26.0, E3 25.5.
- **Two dates and the word "unchanged" are wrong.** The MVP exit is in week 24, not week 20, and the total rose from 112 to 118.
- **M0.8 duplicates work.** Commit `fce0cd4` says D21 was already applied "to DDL, protos and all sections". M0.8's 3.5 ew therefore either duplicates work or is really "make the four must-fail TLA+ configurations exist". None of `DocLifecycle_NoLineage`, `AsOf_InvalidateSuperseded`, `ShardMove_DeleteDuringCatchup` or `ShardMove_PerTableNewSnapshot` is in `formal/tla/`.

**Recommendation.** Fix the dates. Re-scope M0.8 to what is actually missing (the configurations, `gen-docs`, and A-1/A-2/A-9/A-13 of this review). Note that E2 at zero slack is the Phase 2 critical path.

### A-24: minor: formal conformance artefacts claimed in §7 are missing or stale

**Where.** §7.6 ("`formal/lean/` is a Lake project (`lakefile.lean`, `lean-toolchain` …) … `formal/lean/SORRY_BASELINE` (currently 3)"); `formal/lean/` contents; §7.4(c) refinement mapping; §7.5 ("the version row's `FOR SHARE`"); the brief ("Protobuf is also the schema for Temporal payloads").

**Evidence.**
- **Lean.** `formal/lean/` contains only `Engram/*.lean`: no lakefile, no toolchain pin, no baseline, so `lake build` cannot run. `RRF.score_mono`, the monotonicity property the brief names, is one of the three `sorry`s.
- **Refinement mapping.** The mapping still maps `obs[o].st` to an observation-level state, with no per-version `derived_from_deleted`, lineage or `hidden_by_invalidation`. §7.5 still assumes the `FOR SHARE` version-row lock that N83 removed.
- **Workflow payloads.** `TenantDelete` (N36), `RetainBackfill` (N31) and the per-shard sweeper schedules have no message in `workflow.proto`, although "every top-level workflow input … carries `schema_version`".

**Recommendation.** Add the Lake skeleton and baseline, prove `score_mono` in Phase 0 (it is a routine list induction), update §7.4(c) and §7.5 to D21, and add the missing workflow inputs.

### A-25: nit: the MCP adapter does no OAuth resource-server work

**Where.** §4.6 ("the adapter never inspects claims except to decide which tools to list"; `Authorization` forwarded unchanged; 401/403 "before any MCP framing").

**Evidence.**
- Current MCP authorization requires the server to validate that the token was issued for it, and to advertise Protected Resource Metadata (RFC 9728) and a `WWW-Authenticate` challenge so clients can obtain tokens.
- `aud: engram` is shared by the core, so forwarding is acceptable. But the adapter must still validate the token before listing tools, and it publishes no discovery metadata, so standard MCP clients cannot complete an OAuth flow against it.

**Recommendation.** Have the adapter call the same `TokenVerifier`, and serve `/.well-known/oauth-protected-resource` per endpoint.

### A-26: nit: packing can return nothing where Hindsight returns the top result

**Where.** D10 packing ("an item that does not fit is skipped, not truncated"); `RecallRequest.max_tokens` (minimum 256); hindsight-notes §3 ("the top-ranked result is always returned whole").

**Evidence.** With `max_tokens = 256` and a 300-token top chunk, Engram can return zero results. The Lean `Packer` proves the budget bound, not this parity property.

**Recommendation.** Always emit rank 1, and count its overflow in `tokens_used`, or document the divergence in §1.9.

---

## Verdict

**Not ready to staff Phase 1 as written. The blocking set is small and specific.**
- **A-1** (pages leak deleted content) breaks D16 and the brief's delete invariant on three public surfaces. Its fix mirrors N41/N79/N84, so it belongs in Phase 0″, not M3.1.
- **A-2** makes the move's safety valve unexecutable in exactly the window N98 introduced. It must be fixed in the transition table and the admin contract before M1.5.
- **A-3** and **A-4** change D3. Either the shard shrinks or its RAM doubles, and the 1 B-fact fill takes about 2.5 months online, not one. Both are decisions, not edits.
- The API contract is otherwise unusually careful: buf lint, build and format are clean, and pagination, field masks, deadlines and error details are coherent. Its remaining defects are drift between the protos and §5 (A-5, A-9, A-13) and idempotency at the edges (A-6).
- **A-7** and **A-8** are cheap to fix now and expensive after tenants have data.
- The second review's register fold (A-10) was accepted and not done. Until it is, the "binding" register contradicts the sections on the rerank, fence and move rows.

## What I could not verify

- **Runtime behaviour** (no Postgres or pgvector here): whether an HNSW scan on PG 16 with pgvector 0.8 fetches the heap for every visited tuple under `iterative_scan = relaxed_order` with non-indexed filters. A-3 assumes yes, per pgvector's documented filtering. I also could not check actual page-cache hit rates or CPU per arm.
- **Gateway facts:** whether the 600 RPM cap is per model, per cell or per organisation. A-4 assumes per model per cell, as D3 does; per organisation would make it 4× worse. I also could not check whether consolidation's model shares extraction's cap.
- **Proofs and specs:** Lean files were not type-checked (no toolchain). TLA+ was not re-run; I relied on the committed logs and §7's own statements.
- **MCP client behaviour:** that specific clients reuse JSON-RPC ids across sessions (A-6) is per the JSON-RPC and MCP specs, not tested against a client.
- **Hindsight parity:** taken from `reference/hindsight-notes.md` only. Items it marks UNVERIFIED (for example graph hop counts) were not used.
