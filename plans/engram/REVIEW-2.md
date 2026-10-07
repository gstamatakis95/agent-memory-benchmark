# Engram plan: second adversarial review

**Stance.** A principal engineer reviewing the post-`REVIEW.md` plan (v1.1, register D20, N50–N78). I did not re-raise first-review findings. Each item below is one of three kinds: (a) a first-review fix that is wrong or incomplete, (b) a defect the first review missed, or (c) a place where artifacts and prose still disagree.

**Inputs read in full or in the relevant parts:**
- `00-decision-register.md`, `REVIEW.md`, `sections/00–12`
- `proto/**` (in particular `events.proto`, `workflow.proto`, `memory.proto`, `errors.proto`, `common.proto`)
- `sql/shard_schema.sql`, `sql/catalog_schema.sql`
- `formal/tla/*.tla`, every `*.cfg` including the new `*_Gate*.cfg`, and `formal/tla/results/`

I did not run TLC and did not start Postgres. Statements about Postgres, pgvector, pg_search and Temporal behaviour are labelled where they rest on documented or well-known behaviour rather than on an execution.

**Severity scale:**
- **blocker:** ships a wrong guarantee the brief calls non-negotiable, or cannot run as written.
- **major:** a stated number, invariant or schedule does not hold without a decision-level change.
- **minor:** real but local.
- **nit:** small.

`[regression]` marks a defect introduced or exposed by a first-review fix.

**Counts:** 3 blockers, 16 majors, 9 minors, 2 nits (30 findings).

---

## Findings

### G-1: blocker [regression, partial]: delete invisibility (D16) leaks through observation text lineage. Neither the inputs rule nor the apply re-verification tracks the observation texts a version was written from.

**Where.**
- N41 as amended, and N47.
- `sections/05-pipelines.md` §5.2.2:
  - step 3.1 `FindCandidates`;
  - step 3.5 (line ≈ 686): "`input_fact_ids` = the **rendered evidence**: the batch facts ∪ the quoted sources actually shown for each candidate (≤ 5 per candidate)";
  - step 3.6.4 (line ≈ 710): re-verification `SELECT memory_id FROM facts WHERE … memory_id = ANY($input_fact_ids) AND live FOR SHARE`;
  - step 3.6.5 update (line ≈ 736): `DELETE FROM observation_sources WHERE observation_id = $1 AND memory_id <> ALL($new)`.
- `sections/06-prompts.md` §6.3 inputs row: candidates are shown with their **full text**.
- `sql/shard_schema.sql`, `engram_observation_inputs_after_delete`.

**Claim.** D16: "from then on nothing from the document is returned by Recall". The executive summary: "a version written with deleted content in the prompt is never served again at any `as_of`".

**Why it is wrong.**

The consolidation prompt shows each candidate observation's whole current text. That text was itself written from that candidate version's own inputs. `observation_inputs(v)` records only:
- the 8 batch facts, and
- at most 5 *quoted* sources per candidate.

It records neither the candidate versions' inputs nor their unquoted sources. The delete cascade flags exactly the versions whose `observation_inputs` row names a victim. Taint therefore does not propagate through text.

Interleaving, all plain operation:

1. Observation `o` has version v1 with text "Alice lives in Paris and likes tea". Its sources are {f1, f5, f6, …, f12}; f5 is the Paris fact. `inputs(v1)` contains f5.
2. A batch B = {g1…g8} runs. `FindCandidates` shows `o` with v1's text and the 5 most recent quotes, f8…f12; f5 is not rendered.
3. The model writes update v2: "Alice lives in Paris, likes tea, and joined X". It cites {g3, f8}.
4. `inputs(v2)` = {g1…g8, f8…f12}. The update's `DELETE … memory_id <> ALL($new)` removes f1, f5, f6, f7, f9…f12 from `observation_sources`.
5. The document containing f5 is deleted. The cascade deletes `observation_inputs(o, v1, f5)`, so v1 is flagged `derived_from_deleted`. No `observation_sources` row names f5 any more, and v2's inputs do not name f5.
6. Result: v2 stays live and current, `o` is not `stale_delete`, and Recall keeps returning "Alice lives in Paris" after the delete was acknowledged.

The same path runs through any candidate `o'` (not only `o`'s own previous version). For candidates the plan already concedes transitivity on the `as_of` side: `effective_at` takes the maximum over `candidate_versions`. The delete side lacks it.

A second, race-only variant with no cap involved:
1. `ApplyBatch` re-verifies `input_fact_ids` only. It never checks that the `candidate_versions` it was shown are still servable.
2. A concurrent cascade flags candidate `o'` version w as `derived_from_deleted` between `FindCandidates` and `ApplyBatch`.
3. The apply then commits a new version written from w's text **after** the delete was acknowledged.

**Regression aspect.** Narrowing inputs from "every source of every candidate" to "rendered quotes" (F-9) made the hole larger. The update path also drops every unshown source. An observation with 30 sources keeps at most 5 quoted sources plus the batch citations after one update, so `proof_count` and history erode silently.

**Formal.** The specs cannot see this, and they contradict each other.
- `AsOf.tla` line 72 makes `deriv` transitive (`batch ∪ UNION {Latest(c).deriv : c ∈ cands}`).
- `DocLifecycle.tla` `commit(S)` sets `deriv |-> snap`, dropping the previous text's derivation entirely.
- W-2 (§7.6) proposes `deriv := inputs`. That would make `NoDeletedContentRecalled` tautological with respect to exactly this bug.

**Recommendation (decision level).**
1. Record lineage. Add `observation_lineage(namespace_id, observation_id, version, parent_observation_id, parent_version)` for every candidate version shown, including `o`'s own previous version. `candidate_versions` already holds these pairs.
2. Make the cascade flag `derived_from_deleted` transitively: a recursive CTE over lineage from the directly flagged versions.
3. In `ApplyBatch`, re-verify that every `candidate_versions` entry is still `NOT derived_from_deleted AND retired_at IS NULL` (`FOR SHARE`), and discard otherwise.
4. On update, keep unshown sources: delete only sources the model was shown and dropped.
5. State the blast radius honestly. The F-9 tension returns, because transitive flagging hides more. Decide it in the register, for example by rate-limiting rewrites of hidden lineages.
6. Fix W-2: define `deriv` semantically as `batch ∪ deriv(previous) ∪ deriv(candidates)`, and check that flagging driven by `inputs` + lineage covers it. Add `DocLifecycle_NoLineage.cfg`, which must fail with the trace above.

### G-2: blocker: the outbox payload cap (16 KiB) cannot hold `DocumentDeleted`, `ChunksRetired` or `RowsPurged` for ordinary documents, so deletes and purges fail

**Where.**
- `sql/shard_schema.sql` line 477: `payload … CHECK (octet_length(payload) <= 16384)`.
- `proto/engram/internal/events/v1/events.proto`: `DocumentDeleted.fact_ids`/`chunk_ids`/`observations_*`, `ChunksRetired.fact_ids`, `RowsPurged.ids`. All are `repeated string`, i.e. 36-character UUID text plus about 2 bytes of tag/length each.
- `sections/05-pipelines.md` §5.4.1 step 11.
- §5.4.2 step 2 ("outbox `RowsPurged{table, ids, document_id}` per table", batches of 1 000).
- N61 and §5.4.9 ("Delete of a 100 k-fact document … deletable through the API").
- §8 `TestDelete_CascadeScales`.

**Recomputation.**
- 16 384 / 38 ≈ **430 ids per event**.
- A 120 KB document is 40 chunks × A-F = 10 facts, i.e. 400 fact ids + 40 chunk ids, which already exceeds the cap. The cascade's last statement (the outbox `INSERT`, A-F1) violates the `CHECK`, the whole transaction aborts, and the document **cannot be deleted**.
- `FinalizeVersion`'s `ChunksRetired` for a `REPLACE` of such a document fails the same way, so a re-saved document never activates.
- Every `PurgeBatch` emits 1 000 ids, about 38 KB, so **every purge batch fails**. That includes purges of small documents with more than 430 rows in `fact_links`.

The F-19 fix's headline ("a 100 k-fact document deletes in ≈ 5 s") is off by a factor of about 250.

**Recommendation.**
1. Events must be bounded by construction. Either:
   - emit one `DocumentDeleted{document_id, deleted_at, counts}` with no id lists, and let consumers read ids from the store or `deletion_log`; or
   - page id lists into `DocumentDeletedPart{part_no}` events of at most 300 ids, all inside the same transaction.
2. Encode ids as 16-byte `bytes`, not strings.
3. Make `RowsPurged` carry key ranges or counts.
4. Add a T1 property test: for every event type, the encoded size is at most 16 KiB at the stated maxima.

### G-3: blocker [regression, partial]: move replay is not a total function from outbox events to row changes. Writes without events are lost, and the rest trip FKs, triggers or `Verify`.

**Where.**
- §5.5.1 step 3 (`05-pipelines.md` line ≈ 1450): "for each event the mover re-reads the current rows it names from the source by id and upserts them into the target (`INSERT … ON CONFLICT DO UPDATE` with every column …)".
- N12 (thin events).
- `events.proto`.
- §3.5 event table.
- `sql/shard_schema.sql` triggers.
- §5.5.1 step 6 (`Verify`, line ≈ 1508).

**Why it is wrong.** "No lost or duplicated data" depends on a mapping, event → set of rows in about 30 tables, that the plan never writes down. Checking it against the artifacts:

1. **Writes that emit no event**, so nothing names them for replay:
   - `consolidation_proposals` (written by `StoreProposal` before apply);
   - `consolidation_batches.state`;
   - `idempotency_keys` (retain ack);
   - `operations` state transitions (`MarkOperation`, `MarkProgress`; §5.4.2 step 5 says explicitly "No outbox event");
   - `quota_counters`;
   - `blob_tombstones` (§5.5 says they "replay like any row", but no event names them);
   - `observation_versions.superseded_at`, and `derived_from_deleted` on **superseded** versions. `DocumentDeleted.observations_marked_stale` lists only observations whose current version is hidden.
   - `export_snapshots` expiry. §3.5 lists `snapshots_expired[]` in `DocumentDeleted`; `events.proto` does not have that field.

   Concrete loss:
   1. A delete during catch-up flags v2 of `o`, a superseded version.
   2. Replay copies nothing for `o`.
   3. On the target, v2 is servable at `as_of` in its range. **F-1 is resurrected by moving the namespace.**

   A second concrete loss: a retain acknowledged during catch-up has no `idempotency_keys` row on the target. A client retry of an `APPEND` after cutover is accepted again and duplicates the appended content.

2. **FK closure.**
   - `consolidation_applied` has a `RESTRICT` FK to `consolidation_proposals`, which no event names. Replaying `ObservationUpserted{op_key}` therefore fails the FK on the target.
   - `facts` → `chunks` → `documents`, and `document_versions` → `ingest_ledger`, must be replayed in dependency order. Under the anti-join the order is seq order of possibly late events.

3. **Triggers refuse the stated upsert.**
   - `ingest_ledger_append_only` and `consolidation_proposals_write_once` raise `42501` on *any* `UPDATE`, including `ON CONFLICT DO UPDATE` against a row the copy already holds. This happens routinely after a `Copy` restart (G-10) and whenever an event names a row inside the snapshot.
   - The `*_touch` triggers set `updated_at = now()` on `documents`, `entities`, `observations`, `operations`, `pages` and `batch_jobs`. Every replayed update therefore differs from the source.

4. **Consequence.** `Verify` compares `bit_xor(row_to_json)` per table. Every move of a namespace with any concurrent consolidation, retain or ack activity ends in a checksum mismatch, so `Rollback`. If the checksum is narrowed, it ends in silent loss.

**Recommendation.** Replace "re-read the rows it names" with one of two designs.

- **(a) A generated, total replay table.** For each event type, list the exact `(table, key-expression)` set, closed under FKs. Add events for every write that has none: `OperationUpdated`, `ProposalStored`, `IdempotencyKeyRecorded`, `VersionFlagged`, `SnapshotExpired`. Use `ON CONFLICT DO NOTHING` for immutable tables. Disable `*_touch` in the replay session, e.g. `SET LOCAL engram.replay = on` checked by the trigger. CI should fail when a store statement writes a table without emitting an event that the mapping covers. This extends N19's registry.
- **(b) Drop event-driven replay for rows.** After `Freeze`, re-copy every row whose `xmin`/`updated_at` is newer than the snapshot, by key range per table (a "delta copy"). Keep the outbox only for blobs.

Either way, add the delete-during-catch-up and retry-after-cutover interleavings to `ShardMove.tla` (W-3). Today `Replay` applies abstract "writes", not rows.

### G-4: major [regression]: the fair-queue advisory fence blocks writers while they hold pooled server connections. One freeze, barrier or export stalls the whole shard, across tenants.

**Where.**
- D2 row 3 as amended.
- `sections/03-data-model.md` §3.3 lock table (lines ≈ 296–303).
- `sections/09-operations.md` pgbouncer `DEFAULT_POOL_SIZE: "24"`, `RESERVE_POOL_SIZE: "4"`, `POOL_MODE: transaction`.
- Writers have `statement_timeout = 30 s`, and no `lock_timeout` appears anywhere in the plan or the SQL.

**Why it is wrong.** F-5's fix makes the exclusive request (copy barrier, freeze, cutover, delete freeze, restore, **and every export snapshot**) wait behind the longest in-flight writer. The longest writers are:
- the delete cascade, about 4 s at 100 k facts;
- a `PurgeBatch`;
- up to the 30 s timeout in general.

Every *new* writer of that namespace then blocks inside `pg_advisory_xact_lock_shared`, inside an open transaction, holding a pgbouncer server connection. A hot namespace is exactly the one being moved: a 32-way `CommitChunk` fan-out plus API writes plus consolidation fills all 24 + 4 server connections of `engram_app`. Recalls and writes of the other ≈ 149 namespaces on the shard then queue in pgbouncer for up to 30 s.

The old `FOR SHARE` starved the mover. The new design hurts every tenant on the shard, and exports (phase 3, potentially periodic) make it routine.

**Recommendation.**
1. Writers take the fence with `pg_try_advisory_xact_lock_shared`. In PostgreSQL's lock manager a request that conflicts with a *waiting* request's mode is refused under `dontWait`, so the try-lock fails fast while an exclusive request is queued.
2. Alternatively, set `SET LOCAL lock_timeout = '50ms'` for the fence statement.
3. Map the failure to a retryable `NamespaceFrozen{retry_after = 200 ms}` handled by the existing API and `P-frozen` retries, so no connection is held while waiting.
4. Exports should not take the exclusive fence at all. A snapshot plus the outbox `max(seq)` read after `pg_current_snapshot()` gives the prefix property without blocking writers. Otherwise, document the stall.
5. Add `TestFence_PoolNotExhausted`: a 32-writer hot namespace plus a freeze, with recall p95 on a second namespace on the same shard asserted.

### G-5: major: F-5's own argument applies unchanged to the `documents` and `document_versions` rows

**Where.**
- §5.1.2 step 5.5.2: `CommitChunk` takes `SELECT … FROM documents … FOR SHARE`, then the version row `FOR SHARE` (N40).
- §5.1.1 step 4.3: the retain ack runs `INSERT documents … ON CONFLICT … DO UPDATE`, which takes a row lock that conflicts with `FOR SHARE`.
- `FinalizeVersion` takes `FOR UPDATE` and `UPDATE document_versions`.
- §5.4.1 steps 3–4: the delete cascade `UPDATE`s both rows.

**Why it is wrong.** The plan accepted F-5's premise: compatible `FOR SHARE` lockers are granted past a waiting exclusive locker, and each extra share locker on a locked tuple allocates a multixact. The same premise holds on these two rows, where up to 32 `CommitChunk`s of one document overlap continuously while it ingests (minutes for large documents).

The consequences fall on the primary workload, a chat conversation appended every turn:
- The next turn's retain **ack** waits behind the previous turn's commits. That makes ack latency LLM-bound, which §1.4 explicitly rejected.
- The delete cascade's `UPDATE documents` can wait past the 30 s `statement_timeout`. A document being ingested then cannot be deleted.
- Every commit churns `pg_multixact`.

**Recommendation.**
- Use the same discipline as the namespace fence. A per-document advisory lock: `CommitChunk` takes it shared (try-lock); `FinalizeVersion`, the cascade and the ack-side version assignment take it exclusive.
- The version-row check becomes a plain read under that lock.
- Or remove `documents FOR SHARE` from `CommitChunk` entirely: the version-row status check plus the cascade's version `UPDATE` already fence it.
- Add the two-session starvation test that the first review asked for on the ownership row, here.

### G-6: major [regression]: `Invalidate` still uses observation-level hiding, which is exactly the F-1 bug

**Where.**
- §5.4.5 (`05-pipelines.md` line ≈ 1228): Invalidate sets `stale_delete` on the observation and on its *current* version, and "no per-version `derived_from_deleted` flag is set *yet*".
- §3.3.6 (line ≈ 856).
- N48.
- `observation_versions.live` = `… AND NOT (stale_delete AND superseded_at IS NULL)`: a superseded version's `stale_delete` is inert.

**Interleaving.**
1. `o` has v1 (inputs {f1}), v2 (inputs {f1, f5}), v3 (current, inputs {f1, f5, f9}).
2. Invalidate f5. `o` and v3 are hidden. v2 is superseded, so it is unaffected.
3. Then either of two things happens:
   - the round rewrites `o` to v4, and the rule "mark the versions whose inputs named it at that point" must run inside `ApplyBatch` (§5.2.2 lists no such statement); or, more simply,
   - no rewrite happens yet, and `Recall(as_of ∈ [eff(v2), eff(v3)))` returns v2, written with the invalidated f5 in the prompt.

§5.4.5 promises the opposite: "hidden until reconsolidated, exactly as for a delete, because its text was derived from content that must no longer be shown".

**Recommendation.**
1. Make invalidation per version but reversible. Add `observation_versions.hidden_by_invalidation integer`, a counter of invalidated inputs.
2. Invalidate increments it on every version whose inputs name the fact. Restore decrements it.
3. `live` requires the counter to be 0.
4. The "permanent once rewritten" rule then becomes unnecessary.
5. Add `AsOf_InvalidateSuperseded.cfg` and a T3 test with the interleaving above.

### G-7: major: `as_of` leaks through sources, quotes and chunk headers. "Time travel is exact" is false.

**Where.**
- `proto/memory/v1/memory.proto` `ObservationInfo.source_fact_ids`, `quotes` (line 315), `proof_count`, and `ChunkInfo.header` (line 322).
- `observation_sources` is per observation with `added_version` only (no `removed_version`), and stale rows are deleted (N57).
- §4.4 worked example: "O1 v1 … (sources F1, F2)" served at `as_of = 2026-04-01`.
- N60: the summary is recomputed on `REPLACE` or at more than 25 % growth, and `ReembedChunk` rewrites old chunks' vectors with the new header.
- Executive summary: "Time travel is exact … never returns anything derived from content the system learned after T".

**Why it is wrong.** Three paths:

1. **Sources and quotes.** Serving v1 at T attaches the observation's *current* `observation_sources`. That includes F3/F4 (mentioned in May) and their verbatim quotes, which are fact text. The per-version source set the §4.4 example shows cannot be reconstructed from the schema.
2. **Chunk header text.** `ChunkInfo.header` returns the document summary. After an `APPEND`, that summary covers turns mentioned after the chunk's `mentioned_at`, and after T.
3. **Chunk vectors.** The chunk *vector* of an old chunk is re-embedded with that summary. The chunk arm at `as_of = T` therefore ranks pre-T chunks by post-T content: a query about a later turn retrieves an earlier chunk. Leakage through ranking is still leakage for the evaluation the brief cares about.

**Recommendation.**
1. Version the evidence. Persist `source_fact_ids`/`quotes` per observation version (they are already in `consolidation_proposals.ops`; copy them to an `observation_version_sources` table), and serve the version's own set filtered by `mentioned_at ≤ T`.
2. Give chunks an `embedding_effective_at` = the `mentioned_at` of the newest item the header summary covered. Either filter the chunk HNSW arm by it, or keep the first-embedding vector for `as_of` queries.
3. Never return `header` under `as_of`, or freeze it per chunk at first commit.
4. Add these to the §8.5 leak canary (`quotes`, `header`, chunk rank).

### G-8: major [regression]: "`mentioned_at` = the item timestamp" is undefined for chunks spanning items, so non-monotone client timestamps leak

**Where.**
- D9 as amended (F-2 fix).
- N9: several items with one `document_id` in a request are concatenated.
- N56: an `APPEND` re-chunks `body(base) ‖ new` over the whole chain.
- §5.1.2 step 2: a manifest entry carries a single `mentioned_at`.
- `workflow.proto` `ChunkWork.item_index` (singular).
- `RetainItem.timestamp` is client-supplied with no monotonicity rule.

**Why it is wrong.** Content-defined boundaries routinely produce a chunk that contains the tail of item A (`ts = 30`) and the head of item B (`ts = 3`; clients send out-of-order or backfilled timestamps, including the benchmark harness when sessions are re-ingested). If the chunk takes B's timestamp, A's day-30 content is visible at `as_of = 5`. That is exactly the F-2 leak, now produced by the chunker instead of the LLM.

Facts extracted from that chunk inherit the same value. `REPLACE` has the same problem: a changed chunk that keeps most of the old text gets the new item's timestamp.

**Recommendation.**
1. Define `mentioned_at(chunk) = max(timestamp of every item whose bytes the chunk covers)`.
2. For `APPEND`, define it as `max(item ts, mentioned_at of the base chunks it overlaps)`.
3. Either reject per-document timestamp regressions (`timestamp < documents.item_timestamp`) or document that they clamp up.
4. Model it in `AsOf.tla` as a chunk built from two items.

### G-9: major: the extraction cache key omits half of the extraction prompt's inputs

**Where.**
- D11 and N26: key = `sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version)`.
- §6.2 inputs (`06-prompts.md` line 151): `header`, `item_timestamp`, `context`, `metadata`, `entity_hints`, `retain_mission`, `content`.

**Why it is wrong.** The model resolves relative dates ("yesterday", "last week") against `item_timestamp` into `occurred_start/end` and `said_at`. A cache hit for the same chunk text under a different timestamp returns the other item's dates. Cache hits happen:
- after a purge and re-add;
- with identical text in two documents of one namespace (forwarded mail, templates, repeated chat boilerplate above the 500-character minimum);
- on any re-ingest with corrected timestamps.

The §5.1.2 validation `said_at ≤ item_timestamp + 5 min` can then fail on a cache hit and turn a correct chunk into `chunk_failed`.

Changing `retain.mission` (N66) or the entity hints has no effect on any chunk already cached. The "content-addressed, re-indexing never re-pays" claim holds only because the key is incomplete.

**Recommendation.**
- Key the cache on the rendered prompt variables: `sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema ‖ sha256(item_ts_day, context, metadata, hints, mission, header))`.
- Or keep the expensive part timestamp-free: extract with *relative* temporal expressions and resolve them against `item_timestamp` in Go after the cache. That is better for cost.
- Re-state the cache hit-rate assumption in §6.8.

### G-10: major: `Copy` crash recovery (per-table restart with a new snapshot) violates target FKs and replays into append-only tables

**Where.**
- §5.5.1 step 2 (line ≈ 1423).
- §5.5.4 `Copy` row (line 1646): "re-copy behind a **new** barrier and snapshot … tables `< i` were copied from an older snapshot, which is safe because the replay is key-based … floor `min(p0_old, p0_new)`".

**Why it is wrong.**
1. Tables are copied in dependency order. Suppose the crash happens while copying `facts`.
2. Table i = `facts` is re-copied at the new snapshot S_new. It contains facts whose `chunks`/`documents` rows were created after S_old. Those parent tables were copied at S_old.
3. `COPY FROM` fires FK checks, so the batch aborts on the FK to `chunks` every time. The activity retries until `ScheduleToClose` and the move rolls back.
4. The replay that is supposed to patch the gap (events in `(p0_old, p0_new]`) runs only *after* `Copy` finishes, so it never gets the chance.
5. Even if it ran, events in that range name rows the S_new tables already contain. The specified `ON CONFLICT DO UPDATE` then hits `ingest_ledger`'s and `consolidation_proposals`' refuse-all-updates triggers (G-3).
6. W-3 lists "multi-snapshot copy" as unmodelled, so the claim is unchecked.

**Recommendation.**
- Restart the *whole* copy behind a new barrier: truncate the target namespace via the admin RPC.
- Or keep one exported snapshot for every table across restarts. `pg_export_snapshot()` with `SET TRANSACTION SNAPSHOT` lets a retried activity attach to the same snapshot while the exporting session lives. It does not survive the session's death, so restart-all is the robust choice.
- Delete the "safe because key-based" sentence.

### G-11: major: one `REPEATABLE READ` snapshot held for the whole copy pins the source shard's xmin for hours; A-O17's 50 k facts/s ignores live HNSW/BM25/FK maintenance on the target

**Where.**
- §3.3 lock table (line 299): `COPY … TO STDOUT` statements "in the same snapshot".
- §5.5.1 step 2: `StartToClose 12 h`.
- §9.5 (line 749): "copy at ≈ 50 k facts/s (A-O17)".
- §10 M1.5 exit: "a 1 M-fact namespace moves in < 2 h".

**Why it is wrong.**

*Source side.* A snapshot open for the duration of the copy holds back the xmin horizon of the **whole source instance**. Autovacuum cannot remove dead tuples in any table of the ≈ 150 namespaces:
- HNSW and pg_search segments bloat;
- the visibility map goes stale, so pg_search starts heap-fetching;
- every `CREATE INDEX CONCURRENTLY` (N55's partial HNSW builds, `REINDEX CONCURRENTLY`) waits for the old snapshot.

The move's source is, per the plan, the overloaded shard. The mover's direct session must also be exempt from `idle_in_transaction_session_timeout = 30 s` while the target side of each 10 k batch commits; the plan does not say so.

*Target side.* `COPY FROM` into a partition maintains, per row:
- the shared HNSW (and a partial HNSW for large namespaces);
- the BM25 index;
- about 9 B-trees;
- 2 FK lookups.

pgvector HNSW insertion at m = 16, ef_construction = 128, 768-d into a graph of about 600 k nodes costs on the order of 1 ms per row in one backend (documented build-throughput order of magnitude; not measured here). That is about 10³ facts/s per load stream, not 5 × 10⁴. A 1 M-fact namespace needs about 20–30 min for the `facts` HNSW alone. A 10 M-fact hot namespace needs hours, for the whole of which the long snapshot holds.

**Recommendation.**
- Copy per table and per key range, each under a short snapshot, using `seq`-bounded delta replay. This needs G-3's total mapping.
- Alternatively, accept the long snapshot only below a size threshold, and for big namespaces use logical-replication-style copy (`pg_logical_slot` per namespace is not possible; publish per-table with a row filter `namespace_id = …`, which PG 15+ supports), which avoids the xmin hold.
- Re-measure A-O17 on the real image with indexes present, and put the measured copy rate into M1.5's exit criterion.

### G-12: major: `Verify` cannot finish inside the freeze watchdog for namespaces above about 10⁵ facts, so moves of exactly the namespaces worth moving always roll back

**Where.** §5.5.1 step 6 (line 1508): full `bit_xor(hashtextextended(row_to_json(t)::text, 0))` per table for `live_facts ≤ 1 M`. Step 4: freeze watchdog 120 s. Step 5: drain wait 15–60 s. M1.5 exit: freeze < 30 s, and a 1 M-fact move succeeds.

**Recomputation for 1 M facts:**
- `row_to_json` of a `facts` row includes the 768-d `halfvec` rendered as text, about 6–8 KB, and detoasting. That is about 7 GB of JSON per side for `facts`.
- `fact_links` adds 30 M rows of about 150 B, about 4.5 GB.
- `observation_inputs`, `entity_mentions` and the rest add more.
- At a few hundred MB/s of single-backend `row_to_json` + hashing, that is minutes per side, plus `count(*)` on every table.

That is well past 120 s minus the drain wait. The watchdog fires and the move rolls back. Below about 10⁵ facts it fits, but those namespaces are not the ones the hot-namespace playbook moves.

**Recommendation.**
1. Move verification out of the freeze window.
2. Verify the static copy against the snapshot (source side, at copy time) and the replay against `move_applied` counts, while the namespace is still active.
3. During the freeze, compare only per-table counts and checksums of the rows touched since `p0`, a bounded set.
4. Hash a column list that excludes vectors (compare `sha256(embedding::bytea)` instead).

### G-13: major [regression]: the failover epoch bump fails every in-flight workflow on the shard, and failover or restore can resurrect acknowledged deletes

**Where.**
- N63.
- §9.6 "A shard down" (c) step (4): bump every namespace's epoch.
- §5.5 "Stale-cache client path": a workflow that hits a wrong epoch "fails its next fenced transaction with the non-retryable error and ends `FAILED`".
- §9.3 restore step 4: replay `deletion_log` from the catalog.
- N21: catalog replication goes through the asynchronous `deletion-log` outbox consumer, with a 60 s lag alert.
- N23: RPO ≤ 60 s.

**Why it is wrong.**

1. **Failed operations.** Restore step 6 terminates and restarts in-flight operations at the new epoch. The failover runbook has no such step. After an epoch-bumping failover, every `RetainDocument`, `Consolidate`, `PurgeDocument` and page workflow of ≈ 150 namespaces ends `FAILED`. The retains among them were acknowledged as durable.

2. **Resurrected deletes.**
   1. A delete is acknowledged at t.
   2. The relay or `deletion-log` consumer has not yet copied it to the catalog (lag is normal, alert at 60 s).
   3. The shard is lost, and the async standby or the last archived WAL predates t.
   4. Restore replays only catalog `deletion_log` rows. The failover runbook replays nothing.
   5. The document is back, although the plan states "a restore must not resurrect data a customer deleted". Acknowledged retains in that window are explicitly accepted as lost. Acknowledged deletes are the direction that matters for the erasure SLA, and nothing covers them.

**Recommendation.**
- Give the failover runbook restore's step 6 (terminate and restart at e + 1).
- Choose one of the following and record it in N21/N23:
  - (a) synchronous replication (`synchronous_commit = remote_apply` to the standby) on the shard, at least for delete transactions via `SET LOCAL synchronous_commit`;
  - (b) the delete ack waits until the `deletion-log` consumer cursor passes its `seq` (a bounded, delete-only barrier);
  - (c) write `deletion_log` to the catalog before the shard transaction, as an idempotent intent that restore treats as authoritative. The register rejected this as a dual write, but it is an *intent log*, not a dual write of state.

### G-14: major [regression]: `CleanupMove` deletes a row the trigger forbids deleting, and cutover (c) violates the `freeze_reason` CHECK. A namespace's first move then blocks every later move.

**Where.**
- §5.5.1 step 9 (line 1563): cleanup deletes "the `namespace_ownership` row (`moved_out`)".
- `engram_check_ownership` DELETE branch: only `incoming` or `frozen/delete` may be deleted, matching §3.3.1's table, "`active`, `moved_out` → (deleted) | — | refused (`42501`)".
- §5.5.1 step 7(c): `UPDATE namespace_ownership SET state = 'moved_out' WHERE namespace_id AND epoch = e`, with `freeze_reason` not cleared, against `CHECK ((state = 'frozen') = (freeze_reason IS NOT NULL))`.
- Catalog `namespace_moves_live_uq` (`WHERE state NOT IN ('done', 'rolled_back')`).

**Why it is wrong.**
- Cutover (c) as written fails with `23514` on every move. §3.3.1's table has the correct statement; §5.5 does not.
- Cleanup fails at the ownership row. `CleanupMove` sets `done` "by the same RPC", so the move stays `cleaning` and the partial unique index refuses any future `StartMove` for that namespace.

This is exactly the "fence protocol not executed end to end" problem F-23 described, re-introduced by F-23's own CHECK.

**Recommendation.**
- Keep `moved_out` rows: they are the fence value. Cleanup should not delete them.
- Generate §5.5's statements from §3.3.1's table.
- Make `TestIso_Ownership_Transitions` drive the *move workflow's* activities, not a hand-written list.

### G-15: major: the F-8 fix covers only `facts`. Chunks and observations keep the truncation problem, the exact scan is not "30 MB sequential", and partial HNSW per large namespace is unsized.

**Where.**
- N55.
- §3.3.4 (line 607): `halfvec` stored `EXTERNAL`, TOASTed out of line.
- Line 663: "≤ 30 MB of `halfvec`, 5–15 ms".
- §3.3.6: `observation_versions_embedding_hnsw` on an **unpartitioned** table.
- `chunks_embedding_hnsw` on partitions.
- §3.7 totals (168 GB, 40 GB hot).
- §1.5 says observations use the semantic arm.

**Why it is wrong.**

1. *Truncation remains elsewhere.* The chunk HNSW has the same 1/16-partition selectivity problem F-8 described. The observation HNSW spans all ≈ 150 namespaces: a namespace with 3 k of the shard's 0.75 M versions is 0.4 % selective before the `live`/`as_of`/tag filters, so 150 hits need about 37 k visited tuples, above `max_scan_tuples = 20 000`. The Reflect forced `search_observations` arm is the one hit hardest.

2. *Exact-scan cost.* With `EXTERNAL` storage, the exact scan detoasts up to 20 k out-of-line values: a TOAST-index probe plus a page fetch each, cold-cache random I/O. This is not a 30 MB sequential read. At 50 QPS/shard of mostly small namespaces, that is up to 10⁶ detoasts per second per shard, and the TOAST relation must then be in the "hot set", which §3.7 excludes.

3. *Partial HNSW sizing.* The fleet mean is 1 B / (100 × 150) ≈ 67 k facts per namespace, so most namespaces exceed 20 k and get a partial HNSW. These indexes *duplicate* the shared index. That adds up to about 18 GB of HNSW per shard (§3.7 does not count it) and doubles HNSW insert work per fact.

   For a 67 k-fact namespace in a 625 k-row partition, the shared index is already about 11 % selective: about 1.4 k visited tuples for 150 hits. The partial index buys little where it is built.

**Recommendation.**
- Apply N55 to all three vector tables. The cleanest fix is to hash-partition `observation_versions` too.
- Store `embedding` `PLAIN` (or `MAIN`) for the exact-scan path, or keep a narrow side table `(namespace_id, memory_id, embedding)` clustered by namespace.
- Raise the partial-index threshold to where the shared index's selectivity actually fails (≈ < 2 % of a partition), and put partial-index bytes in §3.7.
- M0.6 must measure all three arms with cold cache.

### G-16: major: `facts.consolidated_at` is in an index predicate, so every consolidation stamp is a non-HOT update that re-inserts each fact into HNSW and BM25

**Where.**
- `facts_unconsolidated_idx … WHERE live AND consolidated_at IS NULL` (§3.3.4).
- §5.2.2 step 6.6 (line 765): `UPDATE facts SET consolidated_at = now()`.
- §3.7 vacuum table (line 1255): facts are "insert-heavy" with `fillfactor = 90` for in-page updates.

**Why it is wrong.** PostgreSQL disables HOT when any column referenced by an index *or an index predicate* changes. `consolidated_at` is in the partial index's predicate, so each fact's one consolidation stamp creates a new heap tuple with new entries in every index:
- the shared HNSW;
- the partial HNSW;
- BM25 (`live` is also indexed there);
- about 9 B-trees.

That doubles HNSW insert cost and HNSW size before vacuum. pgvector HNSW vacuum must repair neighbour lists, which makes it the most expensive vacuum on the shard.

The same holds for:
- retire and un-retire (`live` is in the BM25 column list and in predicates);
- `ReembedChunk` (`embedding` is indexed);
- the move replay's blanket `ON CONFLICT DO UPDATE` with every column on the target.

**Recommendation.**
- Move consolidation bookkeeping off `facts`. Use a narrow `fact_consolidation(namespace_id, memory_id, consolidated_at, note)` table, or a per-namespace watermark over UUIDv7 `memory_id` with an exceptions table.
- Size the retire path's index churn into §3.7.
- Make the replay update only changed columns.

### G-17: major: the formal story is weaker than §7.1 and the executive summary say. D20 is not in any spec, the new gates are untested, and W-2 as worded would verify the G-1 bug.

**Where.**
- `formal/tla/*.tla` (unchanged by D20).
- `*_Gate*.cfg`.
- `results/` (no gate logs).
- §7.1, §7.6 W-1…W-5.
- Executive summary: "Correctness is checked, not asserted".

**Findings.**

1. *No D20 mechanism is in a spec.*
   - `ShardMove.tla` still replays with the `Resolved` oracle (lines 190–199) and an atomic `Copy`. Its full-bounds **PASS** therefore says nothing about N50 (anti-join), N51, N52 (three-activity cutover with the bounded read loop), the source-side lagging `move_applied` copy, or G-3/G-10.
   - `DocLifecycle.tla` has no `derived_from_deleted`, no invalidate, no versioned observations and no `APPEND` chain.
   - `AsOf.tla` has no delete and no previous version in `deriv`.

   §7.1 acknowledges three gaps (per-version hiding, the previous version's inputs, replay without shard-wide knowledge). The real list also includes:
   - invalidate (G-6);
   - lineage (G-1);
   - multi-item chunk timestamps (G-8);
   - evidence served with old versions (G-7);
   - replay row coverage (G-3);
   - the fence's effect on other namespaces (G-4).

2. *The gates are not evidence yet.*
   - There is no log for any `*_Gate*.cfg`.
   - `Consolidation_Gate.cfg` (2 facts, 1 crash) is strictly smaller than `Consolidation_Mid.cfg` (3 facts), which already **PASS**ed. It adds speed, not coverage.
   - `DocLifecycle_Gate.cfg` is `DocLifecycle_Mid.cfg` with `Hashes` reduced from 3 to 2. The Mid configuration ran 135 M distinct states without finishing, so nobody knows whether the gate completes. `_Gate1` (1 document) can no longer exercise the cross-document consolidation and delete races, which §7.1 says were the reason for 2 documents.
   - There are no gates for `AsOf` or `ShardMove`.

3. *W-2 is mis-specified.* Defining `deriv := inputs` makes `NoDeletedContentRecalled` hold by definition for the implementation's input rule (G-1). `deriv` must be the semantic closure, and the cascade must be shown to cover it.

4. *Consistency between specs.* `AsOf.tla` treats candidate derivation as transitive; `DocLifecycle.tla` treats it as non-transitive. A design can satisfy both specs and still leak.

**Recommendation.**
1. Treat the D20 spec work as a precondition of M1.5 and M2.1, not as a week-1 item of 1 ew (see G-27).
2. Add counterexample configurations for G-1, G-3 (delete during catch-up), G-6 and G-10, each of which must fail.
3. Run and log every gate.
4. Restate the executive summary: "model-checked: outbox; consolidation exactly-once at 3 facts; `as_of` without deletes; the move protocol *before* D20."

### G-18: major: `Drain` and `Restart` race with Temporal visibility and with the source shard's own schedulers, so operations get stuck `RUNNING` or end `FAILED`

**Where.**
- §5.5.1 step 5(b) (line 1487): `ListWorkflow(...)`.
- Step 8 (line 1554): "`AlreadyStarted` counts as done".
- N3 op-sweeper and DEFERRED resumer: shard-wide `engram_admin` schedulers that read `operations` with no ownership-state filter.
- Source rows remain for 24 h until cleanup.

**Interleavings.**

- *(a) Visibility lag.* Temporal visibility (`ListWorkflow`) is eventually consistent, with seconds of lag on Elasticsearch/SQL visibility. A `RetainDocument` started just before `Freeze` is missing from the list.
  1. It is neither recorded nor restarted.
  2. Its next `CommitChunk` fails `WrongShardOrEpoch`, which is non-retryable, so the execution ends `FAILED`.
  3. On the target its operation row says `RUNNING` with `workflow_started_at` set.
  4. The target op-sweeper only handles `PENDING` operations without a workflow, so `WaitOperation` never returns.

- *(b) Source sweeper.*
  1. After (c), the source op-sweeper finds the copied `PENDING` operation, which still exists on the source.
  2. It starts `ns/{ns}/op/{op}` on queue `shard-S` at epoch e.
  3. The target's `Restart` gets `AlreadyStarted` and "counts it as done".
  4. The source execution fails its fence and ends `FAILED`. The operation is never run.
  5. The same applies to the DEFERRED resumer and `purge-sweep`.

**Recommendation.**
- Every shard-wide scheduler must join `namespace_ownership` and act only on `state = 'active'` rows.
- `Restart` must check that an existing execution with the same id is on the target queue and at e + 1, and otherwise terminate and restart it.
- After `Restart`, re-scan the target's `operations` in `RUNNING`/`PENDING`/`DEFERRED` and reconcile them against Temporal by `DescribeWorkflowExecution`, not by listing.
- Add a "workflow started 0.5 s before freeze" chaos row to §8.4.

### G-19: major: the cutover read outage is bounded only while the catalog is healthy, and the point of no return is too early

**Where.**
- N52.
- §5.5.1 step 7: `CutoverBegin` (a) is "the persisted point of no return" and cancels the freeze watchdog.
- `P-cutover`: ≤ 60 s.
- Read loop: ≤ 5 s.
- D4: catalog failover is automatic, detection 30 s (`catalog-failover` in §9.1).

**Why it is wrong.**

*Read outage.* Between (c) and (d), reads can be served only after the catalog commits (d). If (d) waits on a catalog failover (≥ 30 s detection plus promotion), every read fails after 5 s. This is the only catalog-dependent read outage in a design that otherwise serves stale entries indefinitely (D4).

*Write outage.* After (a), a target-shard outage before (b) commits leaves the namespace frozen with no watchdog and no rollback, so writes are unavailable for the length of the target's outage. Rolling back would still be safe at that point, because nobody routes to the target before (d).

**Recommendation.**
- Put the target `shard_id`/epoch in the `moved_out` row and in `WrongShardOrEpoch{MOVED_OUT, next_epoch}`, so the API can route to the target without the catalog. The API still verifies at the target's fence.
- Move the point of no return to (c).
- Keep the watchdog armed until (c).

---

### G-20: minor: the Temporal payload claims are still overstated, and key retirement can brick running workflows

**Where.**
- §5.4.1: "their Temporal histories hold blob keys and encrypted payloads, never the text".
- `workflow.proto`:
  - `ExtractChunkResult.facts` inline when the result is ≤ 4 KiB (fact text);
  - `ChunkWork.header` (document summary);
  - `ChunkWork.context`, `metadata_json`, `entity_hints`.
- §9.1 secrets row: old codec key deleted once 7-day retention has elapsed.
- `RetainDocument` `WorkflowRunTimeout = 7 d`; Move sleeps 24 h; Consolidate runs ≤ 24 h.

**Why it matters.**
- Inline text exists, in ciphertext. "Never the text" is false.
- Deleting a key 7 days after rotation can leave a run that started before the rotation and is still running unable to decrypt its history on replay.
- A per-shard key cannot shred one tenant's content.

**Recommendation.**
- Key deletion = rotation + max run timeout + retention.
- State "≤ 4 KiB of encrypted text per activity" in the deletion SLA.
- Consider per-namespace data keys wrapped by the shard key, which allows per-tenant shredding.

### G-21: minor [regression]: keys-only `CommitChunk` depends on cache blobs that a concurrent purge may delete

**Where.**
- N59 (`CommitChunkInput` is keys-only and reads the `xcache` entry).
- §5.4.2 step 3(c): delete `xcache` entries "whose chunk hash is no longer referenced by any live chunk".
- §3.6 says `xcache` is deleted "never on retain/delete". That is drift.

**Interleaving.**
1. Document A is deleted while document B, with an identical chunk, is mid-retain.
2. B's `ExtractChunk` hits the cache and returns the key.
3. A's purge sees no live chunk with that hash (B has not committed) and deletes the blob.
4. B's `CommitChunk` fails on a missing blob.
5. The plan specifies no recovery: re-running `ExtractChunk` is not in the workflow's error handling.

**Recommendation.** Two parts:
1. `CommitChunk` treats a missing input blob as a retryable error of the chunk sub-pipeline, and re-runs `ExtractChunk`/`EmbedChunk`.
2. The purge deletes cache blobs only after a grace period longer than the maximum in-flight activity age.

### G-22: minor: `engram_move_load` (BYPASSRLS) is fenced on `$1`, but nothing binds the loaded rows to `$1`. A temp-table load keeps RLS and makes restarts idempotent.

**Where.** N51; §3.3 roles table; self-check 7.

**Why it matters.** The per-batch fence checks the ownership row of the namespace the mover *says* it is loading. The `COPY` stream's `namespace_id` column is never checked against it. A mover bug, or a mixed stream, writes into any namespace on the target, with RLS bypassed. That is the exact class of bug RLS exists for.

**Recommendation.**
1. `COPY` into a session `TEMP` table. Temp tables have no RLS and need no bypass.
2. Then run `INSERT INTO facts SELECT … FROM tmp ON CONFLICT DO NOTHING` as `engram_move`, under `ns_isolation`.
3. This drops the BYPASSRLS role and makes a per-table restart conflict-tolerant, which also helps G-10. The cost is roughly 2× load CPU, which G-11 dominates anyway.

### G-23: minor: the ownership trigger does not enforce the state machine §3.3.1 claims

**Where.** `engram_check_ownership`: `v_ok := (NEW.state = OLD.state) OR …`. The role checks exist only for `frozen/move`.

**Why it matters.**
- A same-state update may change `freeze_reason` and raise `epoch`. So `frozen/delete → frozen/move → active` passes, although "`frozen/delete` … never thaws".
- `engram_move` may set `freeze_reason = 'delete'`.
- An `active` row may get an arbitrary epoch bump.

`TestIso_Ownership_Transitions` (positive cases) would pass.

**Recommendation.**
- Same-state updates may touch only `move_applied_seq`/`updated_at`.
- Add role checks per transition.
- Add negative tests for each forbidden edge.

### G-24: minor: per-query normalised scores do not close the BM25 cross-tenant channel

**Where.** N67; `common.proto` `Scores.lexical`.

**Why.** Per-query normalisation removes the absolute scale but keeps relative scores and the *order* of a tenant's own documents. Both depend on the partition-shared IDF, which other tenants' writes change. An attacker can still insert two documents with candidate terms and read their relative order or normalised scores to learn term rarity in the partition.

**Recommendation.**
- State the residual channel as accepted (with `isolation = dedicated` as the remedy).
- Or compute lexical scores with per-namespace statistics. pg_search cannot do that natively, so this is a later item; leave it as accepted risk with a register row.

### G-25: minor: drift between register, sections, proto and SQL after the fix pass

Each item names two places that disagree:

1. **D5 text vs D2 and N50.** D5 steps 2, 3 and 5 and its safety argument still describe the `FOR UPDATE`/`FOR SHARE` barrier and `move_applied_seq = max(seq)` draining. D2's rationale cell still calls "the ownership `FOR SHARE` row" the fencing token. The register is "binding", and these rows contradict N50 and D2.
2. **§9.6 vs N78 and §5.5.** The "Move stuck" runbook (line 817) says the source copy of `move_applied` is "never for the replay decision". N78 and §5.5 make the source copy *the* replay decision.
3. **Restore state.** §9.3 restore step 1 and §5.5 "Epoch bump on restore" set catalog `state = 'frozen'`, not N64's `restoring`.
4. **`snapshots_expired`.** §3.5 and §5.4.1 step 11 list `snapshots_expired[]` in `DocumentDeleted`; `events.proto` lacks it.
5. **Mover grants.** §5.5's prose says the mover "cannot write a data row on the source" and lists `INSERT/UPDATE` on `outbox_cursors` only. `shard_schema.sql` grants `engram_move` full DML on every namespace table on both shards, while §3.3's role table omits `outbox_cursors`.
6. **`namespace_stats` writers.** §9.5's hot-namespace playbook (line 743) says `namespace_stats` counters are "flushed every 60 s by api/worker". N69 says the stats sweeper derives them and nothing else writes them.
7. **`xcache` lifetime.** §3.6 says `xcache` is "never [deleted] on retain/delete"; §5.4.2 3(c) deletes it.
8. **D3 vs N54 and N53.** D3's recall row still says "cross-encoder on 150 pairs ≤ 120 ms … ≈ 215 ms", and D15 still defaults to `bge-reranker-v2-m3`.
9. **Lock key form.** §5.4.1 step 2 and §5.1.2 step 7.2 use a single-key `hashtextextended(namespace_id || '/' || document_id, 0)`. N27 says the `(namespace_id, document_id)` lock. That is harmless but should be one form.

**Recommendation.** Fold D20 into the original rows (D2, D3, D5, D15) instead of only appending amendments, and generate §5.5's SQL and the events table from the artifacts.

### G-26: minor [regression]: `APPEND` chains (N56) depend on ledger rows that the purge path deletes or keeps forever

**Where.**
- N56: `LoadItem` reconstructs the base "back to the last `REPLACE` version (reconstructed from the chain's ledger rows)".
- §5.4.2 step 3(b): "the ledger rows of the document are **deleted**". `PurgeDocument` also runs for `REPLACE`/`APPEND` retires with `versions = [v…]`.
- The FK `document_versions → ingest_ledger` forbids deleting a ledger row while its version row exists.

**Why it matters.** Either way the chain breaks:
- If a retire purge deletes superseded versions' ledger rows, the next `APPEND` cannot reconstruct the base. A boundary chunk retires on most appends, so this triggers on most appends.
- If it may not delete them (FK), conversation ledgers grow without bound, and every append re-reads the whole chain: O(n) per append, O(n²) per conversation.

**Recommendation.**
- Store each version's full body as a content-addressed blob (or the manifest), so the next append reads one object.
- Scope the purge's ledger deletion to explicit deletes.

### G-27: minor: the schedule absorbed the first review, but not the second

**Where.** §10 M0.7 (1 ew, E2), F.1, M1.5, M2.1, the "4 ew slack" (5 %).

**Why it matters.**
- M0.7 bundles W-1…W-4, which are three specs including `ShardMove` (owned by E1/F.1, and counted again in F.1), plus making every gate complete, into one engineer-week in week 1. The first round of the same work took the whole formal track.
- M1.5's exit ("crash table under load") cannot include concurrent consolidation, which the brief requires during a move: M2.1 lands in week 22, after the Phase 1 exit.
- G-1, G-3, G-10 and G-12 add decision-level work to M2.1 and M1.5. The committed scope has 4 ew of slack.

**Recommendation.**
- Re-estimate M0.7 at 3–4 ew, split by spec owner.
- Move the move-under-consolidation chaos row into M2.1's exit.
- Either add a Phase 0″ for G-1/G-3, or drop `RetainBackfill` (M2.5, 3 ew) from the committed scope.

### G-28: minor: the deadline-skip rule and the 216 ms budget give a rerank-skip rate well above the 1 % SLO under a 300 ms client deadline

**Where.** N53, N54; §1.5: skip if remaining < 150 ms; M1.2 exit: skip < 1 %.

**Recomputation.** The pre-rerank critical path is 2 + 25 + 60 + 30 + 1 = 118 ms at p95. With a 300 ms deadline, rerank is skipped whenever the pre-rerank time exceeds 150 ms. That is somewhere past p95 of a sum of four heavy-tailed stages, i.e. likely 2–5 %, not < 1 %.

Rerank, pack and stream need only 98 ms, so the 150 ms reserve is 52 ms too conservative. The budget also sums p95s, which is conservative for the total but says nothing about the skip quantile.

**Recommendation.**
- Set the reserve to the rerank p95 plus 8 ms.
- Specify the client deadline the SLO assumes.
- State the skip rate as a p99 property of the pre-rerank path, and measure that path in M0.5.

---

### G-29: nit: the `FinalizeVersion` retire subquery is not document-scoped

**Where.** §5.1.2 step 7.5: `content_hash NOT IN (SELECT content_hash FROM document_version_chunks WHERE version = v)`.

**Why.** Under RLS the subquery is confined to the namespace but not to the document. A chunk whose hash appears in *another* document's version v escapes retirement.

**Recommendation.** Add `AND document_id = $2` to the subquery.

### G-30: nit: the delete cascade erases curation history

**Where.** §5.4.1 step 6.

**Why.** The cascade clears `invalidated_at = NULL` on the victims. Harmless for visibility, but it erases curation history that `deletion_log` does not keep.

**Recommendation.** Leave `invalidated_at` unchanged.

---

## Verdict: readiness to staff

1. **Not ready to staff Phases 1–2 as written.** The first-review fixes moved the defects rather than closing them: per-version hiding (F-1) is correct for direct inputs but not through observation text lineage (G-1), and not for invalidate (G-6).
2. Delete and purge cannot run for ordinary documents, because events do not fit the 16 KiB outbox cap (G-2). This is a one-day fix, but it shows the 100 k-fact claim was never executed.
3. The move protocol's correctness now rests on an unspecified event → row mapping (G-3). Its liveness is broken by copy restarts (G-10), by `Verify` inside the freeze (G-12) and by cleanup against the trigger (G-14). Expect M1.5 to slip, or to ship off.
4. The advisory-lock fence fixed starvation of the mover but moved the harm onto every tenant on the shard via the connection pool (G-4). The same starvation pattern also remains on the `documents` and version rows (G-5).
5. "Time travel is exact" overstates the design: evidence, headers and multi-item chunk timestamps leak (G-7, G-8), and the extraction cache returns other items' dates (G-9).
6. The Postgres numbers need re-deriving: long-snapshot copy (G-11), an unrealistic copy rate, non-HOT consolidation stamps (G-16), and N55 applied to one arm out of three (G-15).
7. Formal coverage of every D20 mechanism is zero today. The new gates are either strictly weaker than configurations already passed or unrun. W-2 as written would certify G-1 (G-17).
8. Still sound and worth keeping:
   - the outbox with strict-prefix delivery;
   - write-once proposals;
   - server-set `mentioned_at` as the principle;
   - the anti-join *idea*;
   - the honest INCOMPLETE reporting;
   - `derived_from_deleted` as a per-version flag (it needs lineage, not replacing).
9. **Recommended path:** a 3–4 week Phase 0″ that decides the following as register rows, and models G-1, G-3, G-6 and G-10 as failing configurations before any move or consolidation code:
   - G-1: lineage table and transitive flagging;
   - G-2: bounded events;
   - G-3: total replay mapping or delta-copy;
   - G-4: try-lock fence;
   - G-6: reversible per-version invalidation;
   - G-7: per-version evidence;
   - G-12/G-10: verification and copy restart;
   - G-13: delete durability across failover.
10. With that done, Phase 1 (≈ 45 ew, moves behind the flag) is staffable. Without it, expect the first real tenant delete or the first hot-namespace move to violate a stated guarantee.

## What I could not verify

- **No execution.** No TLC run, no Postgres instance, no pg_search or ParadeDB image:
  - G-2's arithmetic is from the proto field types;
  - G-4's lock-queue behaviour (try-lock refused while an exclusive waiter is queued) is from the PostgreSQL lock-manager source behaviour as I understand it;
  - G-16's HOT rule (predicate columns disable HOT) is documented PG behaviour.
  
  All three should be confirmed with two-session tests on PG 16.
- **pgvector HNSW insert throughput** under `COPY` with live indexes (G-11) is an order-of-magnitude estimate, not a measurement on this hardware or on the `paradedb` image.
- **Temporal visibility lag** (G-18) depends on the visibility store configured; the plan does not name it.
- **Gateway behaviour**, rerank latency distributions and their tails (G-28) are assumptions inherited from A-2 and A-R1.
- **Lean files** were not type-checked, and I did not review them beyond the RRF comment fix.
- **Spec reading.** I read the TLA+ modules and every `.cfg`, but not every line of `ShardMove.tla`'s cutover and restart actions. Statements about it are limited to the replay, copy and freeze actions cited.
