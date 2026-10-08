# Review round 4

Three parallel reviews (correctness, Postgres/operations, API/numbers/parity) and the architect's advice that answered them.

---

## Review round 4: correctness and concurrency (C-1 to C-23)

**Lens.** Correctness and concurrency of the round-3 redesign (D22, N111 to N134):
- tombstones with `up_to_version`, and document-id reuse;
- read-time visibility and evidence segments for observations and pages;
- the derivation lock, `Materialize` and `Expunge`;
- `Invalidate`/`Restore` exactness;
- two-stage consolidation and its idempotency;
- `as_of`;
- delete intents and the monotone replay floor;
- the move: dirty copy, freeze, set-difference reconcile, `ready`, rollback, and the restore/failover reconcile;
- the outbox.

The prose (§3, §5, §7, §9.3, register) is checked against `sql/shard_schema.sql` and `formal/tla/*`. Findings already
dispositioned in rounds 1 to 3 are not repeated unless the fix is wrong or missing. `[regression]`
marks a defect that a round-3 change introduced or exposed.

**Method.**
- **SQL.** `sql/shard_schema.sql` was applied unchanged, apart from stubbing out the pg_search `bm25` indexes, to my own
  database `r4c_shard` on the shared scratch PostgreSQL 16 server (pgvector 0.8). Scenario scripts ran
  against it as the plan's statements would: the plan's own SQL functions, the Materialize `INSERT` of §3.8,
  and the grants and triggers as written. The scripts `s1`…`s6` and `own.sql` are in the session scratchpad
  (`r4c/`), not in the repo. Results are quoted below as **Executed**.
- **TLA+.** Two variants of the plan's own specs were checked with TLC 2.18, 4 workers. Each one changes
  only what the prose says differently from the spec:
  - `DurabilityRaise.tla` is `Durability.tla` with the replay floor raised when a replay completes, as N134
    and §5.5.5 say.
  - `DerivationCause.tla` is `Derivation.tla` with `derived_hidden` rows carrying the invalidation cause
    and `Restore` deleting them, as §5.4.5 says.
- **Severity.**
  - **blocker:** a stated guarantee is false, or the system cannot be built as written.
  - **major:** a stated number, invariant or schedule does not hold without a decision-level change.
  - **minor:** real but local.
  - **nit:** small.

---

## Findings

### C-1: blocker [regression]: the REPLACE purge deletes the evidence that a later document delete needs, so observation versions derived from replaced text survive `DeleteDocument`

**Location.**
- §5.4.2 "Other targets" (`05-pipelines.md:1078`) and §5.4.6, row 1.
- `shard_schema.sql`: `observation_inputs` and `observation_version_sources`, `FOREIGN KEY (…, fact_id/memory_id)
  REFERENCES facts … ON DELETE CASCADE`.
- N42 (restated), N117, N119.

**Claim.** §5.4 says: "*At ack:* … no fact, chunk, observation version, page version … derived from the subject is
returned by Recall, Reflect, …". The same section says: "`CHUNK_TOMBSTONES` purges chunks tombstoned by
`replace`/`reextract` after a 1 h grace: their facts and vectors go … the observations that cited them … follow
the ordinary rebuild path."

**Evidence.** Whether an observation version is visible depends only on the `observation_inputs` rows of its
segment and on `derived_hidden`. A REPLACE keeps the derived observation visible by design. One hour later the
chunk purge deletes the replaced facts. The FK cascade then deletes their `observation_inputs` and
`observation_version_sources` rows. After that, nothing records that `O@v` was written from document `D`. When `D`
is deleted later, two things miss `O@v`:
- the pending predicate (`engram_obs_version_hidden`);
- `Materialize`'s `INSERT … SELECT FROM observation_inputs WHERE document_id = $victim AND document_version <= $up_to`.

**Executed** (`s1`). The fixture:
- document `D` v1 has fact `f1` ("Alice SSN is 123");
- observation `O` v1 is root 1 with inputs `{f1}`;
- `REPLACE` v2 drops `f1`'s chunk, which inserts a `chunk_tombstones` row.

What happened after each step:
1. **Chunk purge** (`DELETE` of `f1` and its chunk): 0 `observation_inputs` rows remain for `O`.
2. **`DeleteDocument(D)`** (`up_to_version = 2`): while the tombstone is pending,
   `engram_visible_observation_versions` returns `O` v1 with its text.
3. **§3.8 Materialize `INSERT`**: inserts **0 rows**.
4. **After `materialized`**: `O` v1 is still served, and also at `as_of = 2026-01-05`.

A `stale_write` root rebuild does not help. `O` v1 stays the version served for every `as_of` between
`effective_at(v1)` and the rebuild. It also stays the current version until the rebuild lands.

The same evidence loss hits an invalidated fact that sits in a replaced chunk. The cascade removes its
`fact_hidden` row as well. Its derivations stay hidden only if `Materialize` already wrote a cause-tagged row,
and `Materialize` is paused during a move.

Pages are not hit: `page_version_inputs` has no FK and survives the chunk purge. So the two derived kinds disagree.

Why the specs and tests miss it:
- `Derivation.tla` has no REPLACE and no chunk purge (`Purge` removes only tombstone victims).
- `TestVisibility_AllSurfaces` runs REPLACE and delete separately, never in sequence.

**Recommendation (decision).** Evidence rows must live as long as the derived version they describe.
- Drop the FK cascade from `observation_inputs` and `observation_version_sources` to `facts`.
- Keep `fact_id`, `document_id` and `document_version` as plain immutable columns, and purge those rows only
  with the observation version.
- Add `ReplaceRetire`/`ChunkPurge` actions to `Derivation.tla`.

### C-2: blocker [regression]: `CommitPageVersion` re-verifies nothing, so the derivation lock protects no page; a refresh whose LLM call spans a delete and its `Materialize` commits a visible page built from deleted content

**Location.**
- §5.3.2 step 2: `GatherEvidence` is a read transaction that takes no lock.
- §5.3.2 step 6: `CommitPageVersion` lists no re-verification statement.
- §5.3.5, row "A delete lands mid-refresh".
- N120.
- `engram_visible_page_versions` passes only *pending* tombstones (`engram_doc_tomb(p_ns, true)`).

**Claim.** §5.3.5: "If the marker commits before the commit, the new version's inputs name the victim and the read
predicate hides it; if after, `Materialize` waited for the shared derivation lock the commit held and records it."

**Evidence.**
- The first half of the claim holds only while the tombstone is `pending`. After `Materialize` sets
  `materialized`, the doc check drops it and only `derived_hidden` can hide a version.
- The lock argument of N120 depends on the writer's *re-verification* happening under the lock. For pages, the
  only check is `GatherEvidence`, which runs before the LLM call and holds no lock.

The interleaving:
1. **t0:** `GatherEvidence` sees fact `f` of document `G` as visible.
2. **t0 to t1:** the `page/v1` or `page_full/v1` call runs (seconds to a minute).
3. **t1:** `DeleteDocument(G)` acks. The marker signals `Materialize`, which runs at once, finds no page version
   that names `f`, and sets `G` to `materialized`.
4. **t2:** `CommitPageVersion` inserts `P@n+1` with a fact input naming `(G, 1)` in a segment that had no victim
   before. No statement checks the inputs, so the version is visible.
5. **Later:** `Purge` deletes that `page_version_inputs` row, so no evidence remains either.

**Executed** (`s3`): after the ack, `engram_visible_page_versions` returns version 2, and it still does after the
purge. `engram_visible_facts` hides `f`.

**Why the spec passes.** `Derivation_Page.cfg` passes only because the spec's writer runs `Verify` for pages too:
it re-checks the inputs under the shared lock. In the prose that step exists only in `ApplyBatch`. The test
`TestExpunge_DerivationLock` names "PageRefresh … whose re-verification predates the marker", a step the
pipeline does not have.

**Recommendation.** Inside the derivation transaction, after taking the shared lock, `CommitPageVersion` must
re-verify every rendered input:
- facts against the full marker sets;
- observation versions with `engram_obs_version_hidden` against **all** open tombstones, not only pending ones.

On failure it discards the result and forces `page_full/v1` from the current evidence.

### C-3: blocker [regression]: a stored stage-2 `update` is applied without checking the version it was written against; after an intervening root rebuild it takes the new root and carries hidden text into a visible segment

**Location.**
- §5.2.2 step 3.5: `StoreProposal`, "the stored list wins".
- §5.2.2 step 3.6.4: re-verifies `input_fact_ids` only.
- §5.2.2 step 3.6.5: `update` is "`current_version + 1`, `root_version` inherited" (`05-pipelines.md:681`).
- §5.2.2 step 1: degraded mode runs only root rebuilds, `stale_delete` first.
- §5.2.5: "Namespace frozen during `ApplyBatch`".
- N43, N117, N121.

**Claim.** §5.2.2 step 3.6.5: "a version written with a victim in view is hidden whether the marker came before or after
this commit". §5.2.5 says the same as "no observation is ever served that was written with a victim in view".

**Evidence.**
- The segment rule covers an `update`'s *previous text* only when the new version lands in the same segment
  as that text.
- `ApplyBatch` takes `root_version` from the version that is current at **apply** time. The text was rendered
  against the version that was current at **stage 2**.
- `input_fact_ids` holds the attached facts and at most 10 quoted sources. A victim can therefore be in the base
  text without being among the inputs.
- Proposals are write-once and are reused by `batch_key` across attempts, workflow restarts and moves.

The interleaving:
1. **Setup:** `O` has v1 with inputs `{a}` and v2 with inputs `{f}`, where `f` is from document `H`; both are root 1.
2. **Round R1:** batch `B` (fact `c`) is routed `attach → O`. Stage 2 renders v2's text, the quotes `{a}` and `c`.
   `StoreProposal` commits.
3. **Freeze:** `ApplyBatch` meets a move freeze and the mover terminates the singleton (the §5.2.5 row).
   Meanwhile `DeleteDocument(H)` is acked; the expunge is paused, so the tombstone stays `pending`.
4. **On the target, degraded mode:** only root rebuilds run, so `O` is rebuilt as v3 (root 3) from its visible
   sources.
5. **`Materialize`:** writes `derived_hidden(O, root 1, from 2)`.
6. **Next round:** `B`'s pending facts are re-selected under the same `batch_key`, so the stored list wins.
   Re-verifying `{c, a}` finds both visible. The update becomes v4 with root 3. That segment names no victim and
   has no `derived_hidden` row, so v4 is served.

**Executed** (`s4`): `engram_visible_observation_versions` serves v4 "Dan, who has diagnosis X, moved to Bergen".

The same holds for `Invalidate` (`fact_hidden`). Without any delete it is still a lost update: text from before a
rebuild overwrites the rebuild.

**Why `Derivation.tla` cannot show it.**
- It has one writer (`Writers = {w1}`).
- Route, Verify and Commit form one writer's lifecycle, and no proposal survives it.
- `Verify` checks that the *current* version is visible, while `Commit` inherits the root of whatever is
  current.

**Recommendation (decision).**
- Record `base_version` (and its root) in every `update` write of the proposal.
- Apply an update only if `current_version = base_version`; otherwise discard it and re-route.
- The alternative is to inherit `root(base)` and require the base to be visible at apply.
- Add a two-writer, persisted-proposal configuration to `Derivation.tla`.

### C-4: major [regression]: N134's floor is "raised when a replay completes", which loses acknowledged deletes when the replay's own commits are lost; the floor also lives in the database being restored

**Location.**
- N134; §5.5.5 step 4 (`05-pipelines.md:1551`: "minimum restore target since the last completed replay"); §9.3
  step 5.
- `shard_meta.replay_floor` comment: "only a completed replay raises it (to NULL)".
- Against these: §7.2.6 item 1 and `Durability.tla`. There the floor is the minimum over every restore within
  intent retention and is never raised.

**Claim.** "it is raised only when a replay completes"; "acknowledged deletes and invalidations RPO 0".

**Evidence.** Replayed markers are ordinary local commits: `synchronous_commit = local`, `archive_timeout = 60 s`,
an asynchronous standby. The next failover or restore can lose them exactly as it lost the originals. Once the
floor has been raised, the next restore uses its own target as the floor and skips the intents whose replays were
lost.

**Executed (TLC).** `DurabilityRaise.tla` changes one thing in `Durability.tla`: `Reopen` sets the floor to NULL
(`MaxT + 1`). With `MaxOps 2`, `MaxT 7`, `Lat 2`, `Margin 2`, 2 restores, `AckedDeleteSurvives` is violated. The
14-state trace:
1. the delete commits at 2;
2. a restore to 0 loses it;
3. the replay re-applies it at 4;
4. reopen raises the floor; the delete is acked;
5. a restore to 3 loses the replayed commit; the new floor is 3, so the intent (at 0, margin 2) is outside the
   window and is skipped;
6. the shard reopens without it.

With the same constants and no raise: no error (27,796,798 states generated, 5,147,264 distinct, depth 22).

The floor itself is a row in `shard_meta`, inside the database that restores and failovers roll back. A
promotion of a replica that has not received the floor update, or a PITR, loses the floor too.
`Durability.tla` keeps `rp` outside the restorable state.

**Recommendation.**
- Adopt the §7 rule: never raise the floor while intents are retained (35 days, and replay is idempotent through
  `deletion_log`). The alternative is to raise it only after a base backup that contains the replay's last
  commit has completed.
- Persist the floor outside the shard: the catalog `shards` row or the intent store.
- Fix N134, §5.5.5, §9.3 and the DDL comment to match the spec.

### C-5: major [regression]: replay applies *orphan* intents against later state, deleting content acknowledged after them

**Location.**
- N122; §5.4.1 step 2; §5.4.9 row 1; §5.5.5 step 4; §9.3 step 5.
- `Durability.tla` `Issue`: a failed operation closes its subject, and retains are not modeled.

**Claim.** §5.4.1: "A retry that finds no idempotency row writes a second object; duplicates are harmless because the
replay is last-state-wins per subject". §5.4.9: "a delete whose client saw a transport error may still take effect".

**Evidence.** The intent is written before the marker transaction and nothing ties it to a commit. The replay applies
every in-window intent that is absent from `deletion_log`. The admin marker transaction recomputes `up_to_version =
max(document_versions.version)` at replay time.

Orphans arise two ways:
- **(i) A failed attempt:** the API crashes or aborts after the put.
- **(ii) A concurrent duplicate of an in-flight delete:** a client retry arrives before the first attempt
  commits, so it finds no idempotency row and puts `I2`. Its marker transaction then sees `state = 'deleting'` and
  returns the existing operation. The client is told the request succeeded, and no `deletion_log` row is written
  for `I2`.

The interleaving for (ii):
1. `DeleteDocument(D)` commits with intent `I1` at t1; the duplicate puts `I2` at t1 + ε.
2. At t2 a retain revives `D` as v3 (acked and visible, as N133c allows).
3. The shard is restored or fails over to a point p in [t2, t1 + 10 min].
4. Replay skips `I1` (it is in `deletion_log`) and applies `I2`: tombstone `up_to = 3`.

**The acknowledged v3 is deleted although no delete was issued after it.**

The same applies to:
- an orphan `DeleteNamespace` intent: the replay runs `freeze_delete` and the purge on a namespace the catalog
  still shows as `active`;
- an orphan `Invalidate` issued after an acked `Restore`.

`Durability.tla` cannot express any of these: after a failure the subject is closed, there are no retains, and
there are no duplicate attempts.

**Recommendation (decision).** Make an intent effective only once its marker has committed:
- put a commit record after the marker transaction and ack only after it, then replay only intents that have a
  commit record, at the cost of one more 10–50 ms put; or
- bound the replayed effect to the state as of the intent's time, e.g. `up_to_version = max(version WHERE
  created_at ≤ deleted_at)`, and skip namespace intents whose catalog row is `active`.

Model retains and duplicate attempts in `Durability.tla`.

### C-6: major [regression]: `fact_hidden(reextract)` hides every observation and page derived from re-extracted facts, at every `as_of`, permanently, contradicting "they stay visible"

**Location.**
- N58, N115; §5.1.2 step 7.4; §5.4.6 row 1; §6 "How a bump takes effect".
- `shard_schema.sql`: `engram_fact_hidden_ids`, `engram_obs_version_hidden`, `engram_page_version_hidden`,
  `engram_consolidation_watermark`.
- N116: "sizes are bounded by expunge lag".

**Claim.** §5.4.6 says "`fact_hidden(reextract)` for stale-key facts | … observations and pages derived from them **stay visible**,
marked `stale_write`". §6 says "Consolidation then sees the v2 facts as unconsolidated and evolves observations through ordinary rounds."

**Evidence.** `engram_fact_hidden_ids` returns every `fact_hidden` row whatever its `cause`. The derived-version predicates
hide any version whose segment names one of them.

**Executed** (`s2`): `O` v1 is visible. After `fact_hidden(f, cause = 'reextract')`, 0 observation versions are
visible, at now and at `as_of = 2026-01-03`. `observations.stale_write` stays false, because `FinalizeVersion` flags
only observations of *tombstoned chunks*.

On `engramctl reextract` (the documented path for prompt bumps), this means:
- **Observations vanish.** Every observation derived from re-extracted facts vanishes at every `as_of`.
- **They are never rebuilt.** Hidden current versions are never candidates in `FindCandidates`, and the
  observations are never selected for a rebuild because they are not stale, so the v2 facts create duplicate
  observations.
- **The hidden facts are never purged.** No `Expunge` target purges `reextract`-hidden facts on kept chunks
  (`CHUNK_TOMBSTONES` purges only tombstoned chunks).
- **The marker set grows without bound.** It grows by the namespace's fact count per bump, so the 16 k alert and
  the anti-join fallback become permanent. "Bounded by expunge lag" is false.
- **The watermark is pinned.** An old-key fact that was never stamped `done` pins the consolidation watermark
  forever, because the watermark counts marker-hidden facts (H-23).

**Recommendation (decision).** Re-extraction is a write, not a hide:
- exclude `cause = 'reextract'` from the derived-version predicate (keep it for the fact and chunk arms);
- flag the derived observations `stale_write`;
- purge `reextract`-hidden facts after the same grace as tombstoned chunks, which is safe only once C-1 is fixed.

### C-7: major: `Derivation.tla` does not model the plan's invalidation materialisation; §7's conclusion that the `Restore` lock is "a candidate for removal" is wrong

**Location.**
- §7.2.1 (the `MatInvalid` and `RestoreNoLock` paragraphs); §7.2.6 item 5.
- The `Derivation.tla` knobs.
- §5.4.5: "so Materialize records the `derived_hidden` rows (cause `invalidation` …)".
- §3.8: the Materialize SQL comment "invalidation causes: the same …".
- N133(b).

**Claim.** §7.2.1: "`Derivation_MatInvalid.cfg` (Materialize also writes permanent rows for invalidated facts) violates
`RestoreExact`". And: "The lock on `Restore` (N133) is therefore not needed for any property stated here; it is … a
candidate for removal."

**Evidence.** The prose, the spec's design configuration and the spec's `MatInvalid` configuration describe three
different designs, and the plan is only the first:

| Source | Does `Materialize` write invalidation rows? | Does `Restore` delete them? |
|---|---|---|
| Prose (§5.4.5) | yes, tagged with cause `('invalidation', f)` | yes |
| Spec design configuration (`MatInvalid = FALSE`) | no | not applicable |
| Spec `MatInvalid = TRUE` | yes, cause-less | never |

**Executed (TLC).** I modelled the plan as `DerivationCause.tla`:
- rows carry a cause;
- `Restore` deletes the rows with its cause;
- `Materialize` may run for invalidations alone.

With the constants of `Derivation.cfg` and `MatInvalid = TRUE`:
- **with the `Restore` lock:** all seven invariants hold (24,394,771 states generated, 7,004,914 distinct, depth 32);
- **without the lock:** `RestoreExact` fails at depth 9. In the trace, `Invalidate f3`, then `MatStart` scans, then
  `Restore f3`, then `MatWrite` inserts `(o1, root 1, from 1, cause f3)`. `o1` v1 is hidden forever, and a second
  `Restore` is refused with `NOT_INVALIDATED`.

The lock is load-bearing. The same race exists between `Materialize`'s per-batch transactions ("one short
transaction per affected observation batch") if the set of invalidated facts is computed once instead of re-read
in every batch under the exclusive lock.

**Recommendation.**
- Make cause-tagged rows the spec's design configuration and keep N133(b).
- State that every `Materialize` batch re-reads `fact_hidden` under the exclusive lock.
- Rerun the configurations and correct §7.2.1 and §7.2.6.

### C-8: major [regression]: moves of active namespaces roll back deterministically, because "immutable tables only gain rows" is false and the reconcile keys miss rows

**Location.**
- N124; §5.5.1 step 4 (`05-pipelines.md:1357`).
- The `shard_schema.sql` class comment: "tables with a UUIDv7 id re-copy WHERE id >= engram_uuid_v7_floor(…);
  … the rest by created_at".
- The `schedulable_namespaces` view and the §3.8 schedulers (`03-data-model.md:1284`).
- `idempotency_keys` (24 h expiry sweep), `token_usage_events` (30-day retention), the §5.2.2 step 3.6.4 discard.
- `ShardMove.tla`: the source store only grows.

**Claim.** "every large table is immutable and only ever loses rows through the expunge, which is paused for the
namespace from `Plan` to `done`". And: "every immutable table has `created_at` and an index `(namespace_id,
created_at)`".

**Evidence.**
- **(a) Rows deleted on the source during the move.** N124 and §5.5.1 class "proposals, applied, idempotency
  keys, token events" as *immutable* (re-copy, then compare counts). Yet idempotency keys expire through the
  shard-wide 24 h sweep, `token_usage_events` have 30-day retention, and the discard path deletes proposals.
  These sweeps join `schedulable_namespaces` (`state = 'active'`), which still includes a namespace with an open
  move; only `purgeable_namespaces` excludes `move_epoch`. A key that was copied early and expires before
  `Freeze` leaves the target count above the source count, so the move rolls back. With a steady retain stream
  this happens in practically every move.
- **(b) Tables the reconcile key cannot reach.** These "immutable" tables have no `created_at` column:
  `fact_links`, `entity_mentions`, `fact_consolidation` (`stamped_at`), `curation_log` (`at`),
  `consolidation_applied` (`applied_at`), `deletion_log` (`deleted_at`). The DDL comment keys "tables with a
  UUIDv7 id" by that id. Stamps and curation rows written after `T_copy` for *old* facts fall outside the range.
  That happens constantly, because `Consolidate` keeps running until `Drain`. The rows are missed, the counts
  differ, and the move rolls back.
- **(c) Safety holds, completion does not.** The count and hash verify turns all of this into rollbacks, not
  loss: `NoLossNoDup` holds, and `MoveTerminates` is met by rolling back. The model cannot show it, because its
  source rows only ever grow.
- **(d) The register and the DDL disagree on the class.** The DDL comment lists `idempotency_keys` as MUTABLE
  (merge-diff); N124 lists it as immutable.

**Recommendation (decision).**
- Class tables by "can lose rows outside the expunge". Move `idempotency_keys`, `token_usage_events` and
  `consolidation_proposals` to the merge-diff class, or make their sweeps join `purgeable_namespaces`.
- Give every re-copied table an explicit reconcile-key column in the DDL. Drop the "UUIDv7 id" shortcut for tables
  whose UUID is another row's id.
- Add source-side deletes to `ShardMove.tla`.

### C-9: major [regression]: the `Expunge` deletes content-addressed blobs that live rows still reference, which breaks APPEND and blocks every later move; an acknowledged raw body can be lost

**Location.**
- §5.4.2 Purge (`05-pipelines.md:1062`): "then blobs: the raw `ledger/` body when no other ledger row references the hash,
  `ver/` bodies, …".
- §9.3: "`engramctl blob gc` (daily, safe at any time) deletes the object and the row".
- §5.1.1 step 3: the ledger blob is put *before* the transaction.
- N104 (`ver/{sha256}`).
- N124: "refuse cutover while any is missing".

**Claim.** "The ack promises durability of the raw input"; a move refuses cutover only while a referenced blob is
missing.

**Evidence.** Version bodies (`ver/`) and raw bodies (`ledger/`) are content-addressed per namespace.
- **(i) `ver/` is deleted unconditionally.** Delete `D` (body X), then retain `D` again with X, the common
  re-upload that N133c revival explicitly supports; or keep another document whose body is X. The live version's
  `body_key` then dangles. `LoadItem(APPEND)` on it fails. Every later move finds a referenced key missing *on the
  source* and refuses cutover forever: a rollback each time.
- **(ii) The `ledger/` check is racy.** It runs when the purge writes the blob tombstone; the deletion happens at
  the daily gc without a re-check (only `xcache_gc` rows are re-checked). A retain of X between the purge and the
  gc writes a ledger row that references `ledger/X`, is acked, and loses its raw body at the gc. Even with a
  re-check at gc time, the retain's put-before-insert order leaves a window.

No spec models blobs.

**Recommendation (decision).**
- Either key bodies by owner row (`ledger/{ledger_id}`, `ver/{document_id}/{version}`) and accept duplicated
  storage;
- or reference-count them: the writer records a pending reference before its put, and gc deletes only
  unreferenced keys older than a grace period, re-checking at deletion time.

### C-10: major [regression]: a restore or failover during cutover is arbitrated by state that is not atomic with (c) and (d), and §5.5.5 step 2 bumps the catalog epoch of a namespace whose (d) is still pending

**Location.**
- N123; §5.5.5 steps 1 and 2 (`05-pipelines.md:1532–1545`); §9.3 steps 1, 2 and 4.
- Catalog `namespace_moves.moved_out_at`, stamped by the mover after (c).
- §5.5.1 (d): "success if already `e + 1`".
- `ShardMove.tla`: `Cut` sets `mp' = "moved"` in the same step as the source row, and `ReconcileDesign` leaves
  `cat` unchanged after (c).

**Claim.** "if (c) is done, the restored source row becomes `moved_out(target, e + 1)`; otherwise the move is rolled back from
the target side and the restored row is thawed". And: "For every namespace the catalog lists on `S`, a catalog transaction
sets `epoch = e + 1`, `state = 'restoring'` … first."

**Evidence.**
- **(1) A failover with a stale arbiter has no legal edge.**
  1. P1 commits (c) and streams it to P2.
  2. The mover dies before it stamps `moved_out_at`.
  3. P1 fails, and the failover's reconcile reads "not done".
  4. It rolls back the target (`ready → incoming`, rows deleted) and must "thaw" P2's row, which is
     `moved_out`.

  There is no edge from `moved_out` back to `active` (the trigger refuses it). The failover cannot finish, and the
  namespace has no owner: P2 is `moved_out` and points at a target whose rows were deleted.
- **(2) A failover in the window between (c) and (d) leaves the catalog naming the wrong shard.**
  1. Step 2 bumps the catalog to `(S, e + 1, restoring)`, because the catalog still lists N on S.
  2. The mover's (d), `… WHERE epoch = e AND state = 'frozen'`, updates nothing.
  3. Its success test ("already at `e + 1`") passes.

  The catalog now names S for good, while N lives on T at `e + 1`. The consequences:
  - every request detours through the `moved_out` hint;
  - `DeleteNamespace` runs `freeze_delete` on S's `moved_out` row, which the trigger refuses;
  - a new move plans from S and is refused;
  - a later restore of **T** neither freezes N nor replays its intents, because the catalog does not list N on T.
    Acknowledged deletes for N are not re-applied.

The spec cannot show either case: it makes (c) and the arbiter one step, and its recovery leaves the catalog
untouched after (c). The timeline comparison at (c) also checks a catalog value read earlier (check-then-act);
§7.2.6 item 5 already places that case outside the model.

**Recommendation (decision).**
- Make the catalog CAS the point of no return: `namespace_moves.state` goes `cutover → moved` *before* (c),
  with (c) conditional on it.
- The restore reconcile CASes `cutover → rolled_back`. Exactly one side wins, and the loser stops.
- Exclude namespaces past the arbiter from step 2's bump and let the restore perform (d).
- Model the arbiter as a separate step in `ShardMove.tla`.

### C-11: major: the proposal lifecycle does not work as written: discard is not permitted, the capacity retry cannot take effect, and an all-skip batch is never stamped

**Location.**
- §5.2.2 steps 3.6.2 to 3.6.4; "Capacity" (`05-pipelines.md:707`); §5.2.5 "Scope at capacity".
- Grants: `engram_app` has only `SELECT, INSERT` on `consolidation_proposals`.
- `op_count BETWEEN 0 AND 16`; N43, N95, N133(e).

**Claim.** §5.2.2: "Fewer rows than inputs → `ROLLBACK`, then in a separate transaction `DELETE FROM consolidation_proposals` …
re-queues the batch"; "re-runs stage 1 once with the capacity note"; "A `skip` stamps the fact `done`".

**Evidence.**
- **(a) Discard is not permitted.** The worker connects as `engram_app` (N133(e)). **Executed** (`s5`): the `DELETE`
  as `engram_app` fails with "permission denied for table consolidation_proposals". The discard path never
  completes.
- **(b) The capacity retry cannot take effect.** The capacity re-run produces new writes under the same `batch_key`.
  `StoreProposal`'s `ON CONFLICT DO NOTHING` keeps the stored, overflowing list (unless the capacity variant has its
  own `prompt_version`, which nothing says), so the second apply overflows as well. The facts are stamped
  `capacity`, which `engram_pending_facts` counts as pending. Every round therefore re-routes and re-writes the
  batch (3 or more LLM calls) and throws the answer away for as long as the scope is at capacity.
- **(c) An all-skip batch is never stamped.** Its stored list is empty, so "zero rows for every write → return
  `already`" is vacuously true before step 6 stamps the facts `done`. The batch is re-selected forever.

**Recommendation.**
- Delete or supersede a proposal on every outcome that does not apply it (discard, capacity), under a role that
  may do so, or key proposals by `(batch_key, attempt)`.
- Key the "already applied" test on a batch-level marker written together with the stamps
  (`consolidation_batches.state = 'applied'`).
- This interacts with C-3: stored lists must never outlive a change of their base.

### C-12: minor: the `Expunge` gets stuck when a re-used id is deleted a second time during the first expunge

**Location.** §5.4.2 step 4 ("the `documents` row is deleted at the end of the purge only while it is still `deleting`");
the `documents` FKs from `document_versions`, `chunks` and `facts`; `TestDelete_ReuseDocumentID`.

**Evidence. Executed** (`s6`). The sequence: life 1, delete, revive as life 2, delete again. The first expunge purges
`version <= 1`, and its final `DELETE FROM documents … WHERE state = 'deleting'` fails with "violates foreign key constraint
document_versions_…_fkey". The workflow processes one tombstone at a time, so the second tombstone never
materialises. Degraded mode and the 15-minute alert then become permanent for the namespace.

**Recommendation.** Delete the `documents` row only when no open tombstone with a higher `up_to_version` exists and no
`document_versions` row remains.

### C-13: minor: the restore replay of a namespace or tenant delete has no legal ownership edge, and the restore runbook rewrites the catalog state of `deleting` namespaces

**Location.** §9.3 steps 1, 5 and 9; §5.5.5 step 4; `ownership_transitions` (`freeze_delete` from `active` only).

**Evidence. Executed** (`own.sql`): as `engram_admin`, the transition `frozen/restore → frozen/delete` is refused with "illegal
ownership transition". "Re-runs `freeze_delete`" therefore requires passing through `active`, which reopens the
shard fence that H-9 added. Separately, §9.3 step 1 sets *every* namespace on the shard to `restoring` in the catalog
and step 9 sets them `active`. That includes a namespace whose catalog state was `deleting`.

**Recommendation.** Add an admin edge `restore_delete` (`frozen/restore → frozen/delete`), and exclude `deleting`
namespaces from steps 1 and 9.

### C-14: minor: the prose, the spec and the SQL disagree on which observation version is served when the version current at `T` is hidden

**Location.**
- §5.2.2 "Stale observations": "the latest *visible* version is what Recall serves".
- §5.2.5 example: "`as_of` between v2 and v3 returns v1".
- `Derivation.tla` `Served(T)` against `engram_visible_observation_versions` / `engram_visible_page_versions`.

**Evidence.**
- **Spec:** `Served(T)` is the latest *visible* version with `effective_at ≤ T`.
- **SQL:** it serves only the version that was current at T (`superseded_at > T`), so it serves nothing when that
  version is hidden. The example's v1 is not returned.
- **Safety:** unaffected, because the SQL's set is a subset of the spec's.
- **Retirement:** the SQL's `o.retired_at IS NULL` and `p.retired_at IS NULL` also remove retired observations and
  pages from every *past* `as_of`.

**Recommendation.** Pick one rule and align the prose, `Served` and `TestVisibility_SegmentHiding`. Decide whether
retirement is filtered by its own time.

### C-15: minor: `fact_hidden` keeps one row per fact although it has two causes, so `Restore` can resurrect a stale-extraction fact

**Location.** `fact_hidden` `PRIMARY KEY (namespace_id, memory_id)` with a `cause` column; the `FinalizeVersion`
`INSERT … ON CONFLICT DO NOTHING` (§3.8); §5.4.5.

**Evidence.** The sequence: `Invalidate(f)`, then a re-extraction of `f`'s chunk. The `reextract` insert conflicts and is dropped,
and the new-key twin is hidden through `curation_log`. `Restore(f)` then deletes the only row: the old-key fact comes back
while its twin stays invalidated. If the twin is restored too, the chunk has two visible fact sets. `RestoreExact`
holds only with respect to `invalidate`.

**Recommendation.** Use `PRIMARY KEY (namespace_id, memory_id, cause)`, and have each predicate read the causes it means
(see C-6).

### C-16: minor: replay order uses API-host clocks

**Location.** N122 intent name `{deleted_at_rfc3339}-{operation_id}`; "applies them in name order … last state per subject wins";
`Durability.tla` (one global clock).

**Evidence.** Sequential `Invalidate`/`Restore` calls on one fact can be served by different API replicas. If clock skew
exceeds the time between the first call's stamp and the second's (one intent put, ≈ 10–50 ms), name order inverts
them and the replay leaves the wrong final state. The plan states no clock-sync bound.

**Recommendation.** Before the put, read the subject's last `deletion_log` entry and chain each intent to its
predecessor, or state an NTP bound and include it in the ordering argument.

### C-17: minor: the `expected_version` check of `DeleteDocument` is outside the marker transaction

**Location.** §5.4.1 step 1.

**Evidence.** A retain can change `current_version` between the check and the marker. The delete then covers a version
the caller did not expect (a TOCTOU race).

**Recommendation.** Compare inside step 3, under the document lock.

### C-18: minor: the derived-version predicates fail open on a missing version row

**Location.** `engram_obs_version_hidden` and `engram_page_version_hidden` (both begin with `EXISTS (SELECT … FROM
observation_versions v WHERE …)`); `page_version_inputs` has no FK; the `derived_hidden` comment: "Removed only with the
observation/page itself"; N119: versions "physically deleted only when the observation or page itself is retired".

**Evidence.** If an observation version that a page version cites is physically deleted, `engram_obs_version_hidden`
returns false and the page version becomes visible again. The `derived_hidden` rows go with it. Pages are protected
only if `Materialize` wrote page-level rows at the time. The path that deletes retired observations is not specified.

**Recommendation.** Make the test fail closed: "hidden unless a visible version row exists". Alternatively, forbid
deleting versions that a page cites.

### C-19: minor (unverified): the 60 s writer-lifetime bound is not enforced, and the admin role has no timeouts while it writes outbox rows

**Location.** N124 margin; N95 watermark `engram_uuid_v7_floor(now() − 60 s)`; `ALTER ROLE` lines (only `engram_app` gets
`statement_timeout` and the idle timeout); A-F1.

**Evidence.**
- **The bound.** `statement_timeout + idle_in_transaction_session_timeout` bounds each statement and each gap. It
  does not bound the transaction: PostgreSQL 16 has no `transaction_timeout`.
- **N124 margin.** A slow transaction only costs a rollback there.
- **N95 watermark.** A fact minted more than 60 s before its commit, by a long multi-statement `CommitChunk` or by a
  retry that reuses minted ids, lands below an already-advanced watermark. It is then never consolidated.
- **Admin role.** `Materialize`, `Finish`, the replay and the schedulers write outbox rows as `engram_admin`, which has
  no statement or idle timeout. A-F1 then depends on pool settings, which §9.1 states for `engram_app` only.

**Recommendation.** Mint ids per attempt inside the transaction. Set both timeouts on every outbox-writing role. Base
the watermark guard on commit, for example the last `seq` the relay has seen, not on an id timestamp.

### C-20: minor (unverified): move liveness corners

**Location.** §5.5.1 steps 4, 5 and 9; §5.1.6 `RetainBackfill`.

**Evidence.**
- **Drain misses some workflows.** `Drain` enumerates `operations` rows plus three singletons. `RetainBackfill`
  parents (`ns/{ns}/backfill/{id}`) have no `operations` row; they keep running on the source queue and starting
  children there.
- **The relay-drain bound is tight and shard-wide.** Its bound (60 s) equals the gap horizon (60 s), and it applies to
  the whole shard: one stalled `index` consumer rolls back every move on that shard.
- **A target outage keeps the source frozen.** Rollback after (b′) must run `unready_target` on the target first, so
  a target outage keeps the source frozen beyond the watchdog bound.

**Recommendation.** Register backfill parents as operations. Make the drain bound exceed the gap horizon. Allow
`thaw_move` while the target is unreachable, with the target fenced at its next contact (the target row in `ready`
accepts nothing).

### C-21: minor: the spec bounds and abstractions cannot express the failures above

**Evidence.**
- **`Derivation`:**
  - one writer, one fact per version;
  - no REPLACE, chunk purge or `reextract` (C-1, C-6);
  - `Materialize` is one atomic transaction (C-7);
  - neither tombstone-row deletion nor version-number reuse after it is modeled;
  - no persisted proposals (C-3);
  - the page writer re-verifies, which the prose does not (C-2);
  - `Served` differs from the SQL (C-14);
  - the design configuration is not the prose design (C-7).
- **`Durability`:**
  - no retains, no duplicate attempts, and a failure closes the subject (C-5);
  - the floor sits outside the restorable state and is never raised (C-4);
  - one global clock (C-16).
- **`ShardMove`:**
  - source rows only grow (C-8);
  - the arbiter is atomic with (c), and the catalog is untouched by recovery after (c) (C-10);
  - no cleanup, so "a restored target is re-filled from Src" is modeled even after the 24 h cleanup made it
    impossible;
  - no intents (C-22).

The must-fail configurations prove that the knobs matter. They do not prove that the design configuration is the
plan.

**Recommendation.** Add the missing actions listed under each finding. State in §7.1 which prose mechanisms each
spec omits.

### C-22: minor: PITR to a point before a move-in has no intent replay

**Location.** §5.5.5 ("PITR to a point before a move-in … an ordinary move whose source is a scratch instance restored from the old
source's backup"); §9.3 step 2; §9.3 last row ("a restored shard never serves deleted data").

**Evidence.**
- The scratch move brings back the state as of the cutover.
- Deletes acked on the target after the move-in are not re-applied. The shard replay ran for the namespaces hosted at
  restore time, with the floor of that restore, and finished before the scratch move landed the namespace.
- Neither `ShardMove.tla` (no intents) nor `Durability.tla` (no moves) composes the two.

**Recommendation.** End the scratch move with a replay of the namespace's intents from the cutover time minus the
margin, before `ready`.

### C-23: nit: the mutability classes disagree

**Location.**
- The `shard_schema.sql` class comment lists `observation_version_meta` and `page_version_meta` as INSERT-ONLY and
  `idempotency_keys` as MUTABLE.
- N113 calls `*_version_meta` mutable.
- N124 and §5.5.1 call idempotency keys immutable.
- §3.1 puts `idempotency_keys` under "log and hot counters".

**Impact.** The class decides the reconcile semantics (C-8). Generate the class list once.

---

## Checked and holding

- **Delete against concurrent writes.** The delete marker against `CommitChunk`, `FinalizeVersion` and a
  concurrent retain ack is ordered correctly by the exclusive document lock and the `documents` row lock.
  `up_to_version` covers in-flight versions. The `chunks` `CHECK (life_start <= document_version)` additionally
  rejects a late commit of an old version into a revived life.
- **Revival version numbering.** Revival assigns `v = greatest(max version, max up_to) + 1` under the row lock.
  The jsonb doc-tomb map (max `up_to` per document) hides exactly the covered versions.
- **Re-deleting a re-used id.** The `derived_hidden` PK conflict (`ON CONFLICT DO NOTHING`) keeps the earlier,
  smaller `from_version`. I found no interleaving in which the second delete needs a smaller one.
- **`ApplyBatch` against `Materialize`.** For the paths that re-verify, the race is closed: re-verification runs in
  a fresh statement under the shared lock, and `Materialize` (READ COMMITTED, lock first) sees the commit. The
  exception is C-3.
- **No lock-order deadlock.** Lock order is consistent: the fence shared, then the derivation lock, for
  `ApplyBatch`, `Materialize` and `Restore`.
- **Floor rule as specified in §7.** `Durability.cfg`'s design (floor never raised) passes again at `MaxT = 7`
  with `MaxOps = 2`.
- **Move safety is independent of the margin.** With the count/hash verify, a short margin costs a rollback,
  never a row (C-8 is about liveness only).

## Counts

**3 blockers** (C-1, C-2, C-3), **8 majors** (C-4 to C-11), **11 minors** (C-12 to C-22), **1 nit** (C-23): 23 findings.

## Verdict

**Not ready.** The round-3 principle is the right one: immutable content, marker tables and a read-time predicate.
The SQL behaves as specified for the cases it covers. Three write or purge paths, however, act outside the
predicate's assumptions, and each serves deleted content after the ack:
- the REPLACE purge cascades away the evidence (C-1);
- the page commit never re-verifies its inputs (C-2);
- a stored `update` can change segment (C-3).

The round-3 durability fix (N134) regressed as written (C-4). Orphan intents make the replay unsafe for re-used
documents (C-5). The specs pass largely because they model a narrower system than the prose (C-7, C-21). Each
fix is local but decision-level:
- keep the evidence rows;
- re-verify every derived writer under the lock;
- record and check the base version of a proposal;
- never raise the floor, and store it outside the shard;
- replay only committed intents;
- treat re-extraction as a write;
- class the move tables by "can lose rows";
- stop deleting content-addressed blobs on a reference check;
- use a CAS arbiter at (c).

## What I could not verify

- Behaviour under Temporal: whether a terminated or restarted Consolidate re-forms the same batch in the same group
  order. C-3 needs only *one* such order, and the plan states "stale first".
- Where memory and observation ids are minted relative to the transaction (C-19), and the pool-level timeouts of the
  admin role.
- Whether the capacity variant of the prompt changes `prompt_version` (C-11(b)).
- Zombie-primary behaviour and the timeline check (outside every spec, as §7 states).
- Recall and Materialize latency under the per-candidate predicate.
- pg_search behaviour: the indexes were stubbed.
- Lean: no toolchain.
- The full `Derivation.tla` extended with the C-1, C-3 and C-6 actions: not written. The SQL scenarios stand in for
  it.

---

## Engram round 4: Postgres, Temporal and operational review of the round-3 design

Scope: the D22 storage and operations design as it would run on PostgreSQL 16 with pgvector,
pgbouncer, pgBackRest and one Temporal cluster per cell. The review covers:

- insert-only content rows with vector side tables, and per-namespace partial HNSW indexes (creation, thresholds, count per partition, hygiene, `REINDEX`);
- the exact-scan fallback, the marker sets loaded on every request, and RLS with the `SECURITY DEFINER` lookups;
- the Expunge purge and the vacuum it causes; the 128 GB sizing and the IOPS table;
- `synchronous_commit = local`, the delete-intent store, restore and failover;
- the cost of the move's copy and reconcile, Temporal per cell, migrations, backups, metrics and runbooks.

Findings dispositioned in rounds 1 to 3 are not repeated unless the fix is wrong or missing; where a
finding continues a round-3 item this is stated. Ids are P-1 onwards for this round.

## How the evidence was produced

- **Server.** The shared scratch PostgreSQL 16.15 with pgvector 0.8.6, pg_trgm and pgstattuple. The VM has 4 vCPUs and 15 GB of RAM.
  - `shared_buffers` is 128 MB. It is the shared server's setting, which I did not change.
  - Buffer counts (hits plus reads) are therefore the primary metric. Timings are warm or OS-cached. `maintenance_work_mem` was set per session.
- **Schema.** Database `r4pg_shard` (mine, dropped at the end) received `sql/shard_schema.sql` unmodified, except that the `USING bm25` statements and `CREATE EXTENSION pg_search` were stripped.
- **Synthetic shard.**
  - Ten namespaces in two partitions.
  - `fact_vectors_p07` holds A (100 k facts) and B, C, D, E (40 k each), 260 k rows in all.
  - `fact_vectors_p03` holds namespaces of 2.1 k, 5 k, 10 k, 20 k and 50 k facts.
  - Rows were inserted interleaved across namespaces, as concurrent ingest leaves them.
  - Embeddings are `halfvec(768)`, built as the L2-normalised sum of one of 64 shared centroids and two noise vectors from a 20 k bank. Every namespace shares the same topic space.
  - Namespace A also has 2.83 M `fact_links` rows (about 30 per fact), 200 k `entity_mentions`, consolidation stamps and 5 k plus 230 k `entities` on the shard.
  - Every namespace has its partial HNSW from `engram_hnsw_ddl` (`m = 16, ef_construction = 128`).
- **Roles and plans.**
  - Arm queries ran as `engram_app` under RLS with the scope GUCs set and `plan_cache_mode = force_custom_plan`, using the §3.8 SQL.
  - The purge ran as `engram_admin`, and the reconcile queries as `engram_move`.
  - WAL figures come from `EXPLAIN (ANALYZE, WAL)` around a wrapper function, so they are backend-local and not polluted by other sessions.

---

## Findings (ranked)

### P-1: major [regression: N124]: the reconcile's re-copy keys miss rows written under old ids, so moves of active namespaces roll back deterministically. The whole-namespace checks it runs under the freeze also put the freeze at about twice its target

**Where.**
- §5.5.1 step 4: "re-copy rows with `created_at ≥ T_copy − 10 min` (every immutable table has `created_at` and an index `(namespace_id, created_at)`; UUIDv7 ids may serve as the key)".
- `shard_schema.sql` table-class comment: "tables with a UUIDv7 id re-copy WHERE id >= engram_uuid_v7_floor(t_copy - 10 min); fact_links by the id of either endpoint; the rest by created_at".
- §5.5.1: "Freeze window ≈ (rows written in the last 10 min) + (mutable rows …) + the blob checks: < 30 s for ≤ 1 M-fact namespaces".
- §9.4 Move SLO.

**Evidence.**

1. **Rows are inserted under old ids during the dirty copy.**
   - `start_move` pauses only the expunge and the shard-wide schedulers. The namespace stays `active`, and the `Consolidate` singleton is terminated only at `Drain`, after the freeze. So during the copy:
     - `fact_consolidation (PK namespace_id, memory_id, stamped_at)` receives `done`/`failed` stamps for facts committed long before `T_copy`. These come from a consolidation backlog, a quota `DEFERRED` until the next window, and N95's 7-day retry of failed stamps.
     - `chunk_vectors (PK …, chunk_id, embedding_model, embedding_effective_at)` receives `ReembedChunk` rows for old `chunk_id`s on every summary-refreshing `APPEND` (§5.1.2 step 8).
     - `fact_vectors` and `observation_version_vectors` receive new-model rows for old ids under `ReembedNamespace`.
     - `observation_versions`, `observation_inputs` and `observation_version_sources` receive new versions of old `observation_id`s.
   - The copy reads each table in PK order. A row inserted after the cursor passed its key is not copied, and a re-copy keyed by "UUIDv7 id ≥ floor" does not select it.
   - `count(*)` then differs, and the move rolls back. The retry meets the same writers, so a namespace with a consolidation backlog or `APPEND` traffic cannot be moved. These are exactly the hot namespaces the move exists for.
   - The failure is detected, not silent: the target is a subset of the source and the counts are compared. It is a liveness failure of rebalancing, not a loss.
2. **The indexes the text relies on do not exist.**
   - The DDL has no `(namespace_id, created_at)` index on any immutable table. The only `created_at` indexes are `operations_created_idx` and `ingest_ledger_received_idx`.
   - `fact_links` and `entity_mentions` have no `created_at` column at all.
   - A `created_at`-keyed re-copy is therefore a scan of the namespace's rows per table, inside the freeze. Examples: `observation_inputs` (≈ 4.5 M rows per 1 M facts, unpartitioned), `document_version_chunks` and `observation_version_sources`.
3. **The freeze formula omits work that scales with the whole namespace.** Measured as `engram_move` on namespace A (96 k facts, 2.83 M links), warm, serial:

| Reconcile step under freeze | A (96 k facts) | Linear at 1 M facts |
|---|---|---|
| `count(*)` of `fact_links` (index-only, visibility map set) | 0.85 s | ≈ 9 s |
| `count(*)` + `bit_xor(hashtextextended(pk::text,0))` of `fact_links` | 3.5 s | (links > 2 M: count only) |
| hash of `facts` / `fact_vectors` / `entity_mentions` | 0.08 / 0.11 / 0.22 s | ≈ 4 s |
| `engram_verify_fk(ns)` (56 FKs) | 1.8 s | ≈ 19 s |
| links re-copy probe (`src ≥ floor OR dst ≥ floor`, nothing new: BitmapOr on PK + reverse) | 0.2 ms | — |
| re-copy conflict path (`INSERT … ON CONFLICT DO NOTHING`, rows already present) | 315 k rows/s | — |

- Add the existence check of every referenced blob: ≥ 100 k `ver/` and `ledger/` keys for 1 M facts, at 64 in parallel and 10–20 ms per `HEAD`, takes ≈ 20–30 s.
- The freeze for a 1 M-fact namespace is then ≈ 55–65 s warm, against the "< 30 s".
- The target is worse: right after a bulk copy its visibility map is mostly unset, so the index-only counts become heap fetches.
- The re-copy window is also not "the last 10 min". It is everything inserted since `T_copy − 10 min`, which spans the whole bulk copy.

**Recommendation (decision level).**
- Give every insert-only table a per-namespace insertion key that is monotone in commit order and indexed `(namespace_id, ins_seq)`. A `bigint` from a shard sequence works, or `created_at` with the index. Key every re-copy and every count by it, never by an entity id.
- Before the freeze, run the whole-namespace counts, hashes, `VerifyFK` and blob check against a recorded `ins_seq` watermark, plus one catch-up pass. Under the freeze, verify only the rows above that watermark and the mutable merge-diff.
- Restate the freeze formula and add a T3 test: a move with an active consolidation backlog and `APPEND` re-embeds must complete.

### P-2: major [regression: N115/N116]: marker sets are not bounded by expunge lag. Re-extraction and curation leave permanent `fact_hidden` rows, and re-extracted facts are never purged

**Where.**
- §5.4.1: "sizes are bounded by expunge lag and alerted above 16 k entries".
- §9.6: "Marker sets above 16 k entries mean the expunge is days behind".
- §3.7: "markers are bounded by expunge lag"; R35.
- §5.4.2 "Other targets"; §6 "`engramctl reextract --namespace` … `FinalizeVersion` hides every live fact … by inserting `fact_hidden(reason = 'reextract')`".
- §5.4.6: "Replace retires a chunk … observations and pages derived from them stay visible".

**Evidence.**

1. **Re-extracted facts are never purged.**
   - `FinalizeVersion` inserts `fact_hidden(cause = 'reextract')` for the old-key facts of a *kept* chunk (N58). The chunk keeps its id and gets no `chunk_tombstones` row.
   - The Expunge targets are `MARKERS` (document tombstones), `CHUNK_TOMBSTONES` and `OLD_EMBEDDING_MODEL`, so nothing purges these facts. Their vectors, HNSW and BM25 entries, about 30 `fact_links` rows each, mentions and the `fact_hidden` row stay forever.
   - What triggers it: one `engramctl reextract` of an N-fact namespace, or any `REPLACE` whose `render_hash` changed. `render_hash` covers context, metadata, mission and the heading path, so editing a document's context re-extracts it.
   - The effect is N permanent marker entries and a doubled footprint for that namespace. `live_facts`, which drives `ShardNearCapacity` and placement, counts visible facts only and does not see it.
2. **`fact_hidden(invalidate)` is permanent by design.** It has no purge phase, so curation-heavy namespaces grow the set without bound. Above 16 k entries the namespace stays permanently in the anti-join mode, with the `MarkerSetLarge` ticket open.
3. **Measured cost near the threshold.**
   - Loading 16 k `fact_hidden`, 10 k `chunk_tombstones` and 1 k document tombstones takes 9.0–10.5 ms as `engram_app`, against the stated "≤ 3 ms at 16 k".
   - The semantic arm with 16 k-element `fact_hidden` and `chunk_tomb` arrays plans in **17.8–19.9 ms**, against 1.5 ms with empty sets. Execution is unchanged at 4–6 ms, because the `<> ALL` is hashed.
   - Under `force_custom_plan` every arm replans with the constants on every call, at about 6 arm statements per recall. That is ≈ 110 ms of planner CPU per recall at the threshold, against a recall CPU budget of ≤ 4 vCPU at 50 QPS (§9.1).
4. **Re-extraction hides derived content.** This is outside my lens and flagged for the correctness reviewer; it was checked against the SQL.
   - `engram_obs_version_hidden`, `engram_page_version_hidden` and the observation-arm SQL test `i.fact_id = ANY($fact_hidden)` over **all** causes.
   - So after a re-extraction, every observation and page version whose segment contains a re-extracted fact is hidden until it is rebuilt. That contradicts §5.4.6 and N42 ("a replace is a write").

**Recommendation (decision level).**
- Make re-extraction a purge target. Either:
  - purge old-key facts after the same 1 h grace as `CHUNK_TOMBSTONES`; or
  - express currency as a narrow `chunk_extraction(namespace_id, chunk_id, current_key)` row and test `f.extraction_key = current_key` by join. That is O(1) per candidate and not a growing set.
- Exclude `reextract` from the derived-version predicate.
- Keep `fact_hidden(invalidate)` out of the per-request arrays: always test it with the PK anti-join, which is cheap per candidate.
- Count hidden-but-unpurged rows in the capacity signals, and restate R35.

### P-3: major [regression of N112/N116; round-3 P-6 fixed only for cross-namespace selectivity]: within a namespace, selective tag or `as_of` filters make the planner seq-scan the whole partition

**Where.**
- §3.8 semantic arm: "The immutable copies on the vector row let every predicate run inside the scan, so the iterative scan walks the graph until 150 rows pass".
- §3.1 principle 5: "A query then visits only its own graph".
- The IOPS row "visited ≈ `ef_search`".

**Evidence.** Measured on namespace A (96 k vectors in a 260 k-row partition with 5 partial HNSW indexes), as `engram_app` with `ef_search = 150`, `iterative_scan = relaxed_order` and `LIMIT 150`:

| Filter (fraction of A admitted) | Plan chosen | Buffers | Time |
|---|---|---|---|
| none | A's partial HNSW | 1,882 | 4 ms warm |
| `as_of` (≈ 16 %) | HNSW, 902 rows removed by filter | 4,091 | 33 ms |
| `as_of` (≈ 5 %) | HNSW, 3,113 removed | 19,431 | 101 ms |
| `as_of` (≈ 1 %) | **Parallel Seq Scan of `fact_vectors_p07`**, all namespaces, 253 k rows removed | 65,080 (≈ 508 MB) | — |
| tag filter, 20 allowed documents (2 %) | **Parallel Seq Scan of the partition** | 65,080 | 86.5 ms warm, 3 processes |

- The vector tables carry only the PK, `*_model_idx` and the HNSW indexes. Nothing supports `document_id = ANY($allowed_docs)` or `mentioned_at <= T`, so the cheapest exact plan the planner can find is the partition scan.
- At target size (625 k rows of ≈ 2 KB, ≈ 1.3 GB per partition) with `max_parallel_workers_per_gather = 4` (§9.1), one tag-filtered arm call reads ≈ 1.3 GB with 5 processes.
- Tag filters are a first-class recall feature, `as_of` is the eval contract, and `chunk_vectors` and `observation_version_vectors` behave the same.

**Recommendation (decision level).**
- Choose the filtered path explicitly in Go.
  - Estimate the candidates: allowed documents × facts per document (a per-document fact count), or the `as_of` window count from `facts_mentioned_idx`.
  - At or below ≈ 20 k candidates, run an exact path driven from `facts_doc_idx` or `facts_mentioned_idx` and join `fact_vectors` by PK.
  - Above that, run the HNSW iterative scan with `hnsw.max_scan_tuples` raised to match.
- Make the arm transactions set `max_parallel_workers_per_gather = 0` and `enable_seqscan = off`, so a partition scan can never be chosen.
- Add a selectivity sweep (tags {100, 20, 5, 2, 1 %} and `as_of` deciles) to M1.2's exit, with p95 per arm.

### P-4: major [regression: N112/N119; round-3 P-4 fix incomplete]: index hygiene cannot run as written, and autovacuum repairs the per-namespace graph before any rebuild. Repair costs 5–6× a rebuild at 4 % dead

**Where.**
- §5.4.2 step 3: "for every touched partition index of the namespace, `pgstattuple` dead fraction above 5 % → `REINDEX INDEX CONCURRENTLY`; else autovacuum".
- N112: "the expunge runs `REINDEX INDEX CONCURRENTLY` … above 5 % dead fraction".
- §3.7: "capped at 20 k rows/s per shard so autovacuum stays ahead; … rebuilds it … instead of waiting for autovacuum to repair".
- §3.1: "a document purge dirties one small index".
- Step table: "`ReindexHygiene` | admin role (owner of the index)".

**Evidence.**

1. **`pgstattuple` cannot read HNSW.**
   - `pgstattuple('fv_a3ab6a9c2931_2db1c3')` returns `ERROR: index … (unknown index) is not supported`.
   - `pgstatindex` returns "not a btree index", and `pgstattuple_approx` returns "wrong relation kind".
   - HNSW elements are not marked dead before `VACUUM` anyway, so there is nothing for such a tool to measure. `HNSWDeadFraction` and `engram_hnsw_dead_fraction` therefore have no source.
2. **The purge feeds autovacuum, which repairs first.** Autovacuum fires at 2 % + 10 k dead tuples, and the insert-triggered runs come every 5 % of inserts. Either way, `ambulkdelete` repairs the namespace's graph before the hygiene step could rebuild it.

| Same partition, warm | Elapsed | Buffer accesses | WAL |
|---|---|---|---|
| `VACUUM` after purging 4 % of a 100 k-vector namespace (4,000 facts) | **207 s** | 89 M | **447 MB** (59 k FPIs) |
| `VACUUM` after purging 1 % of a 40 k namespace, no parallel | 18.9 s | 13 M | 67 MB |
| `REINDEX INDEX CONCURRENTLY` of the 38 k index after a 4 % purge, then `VACUUM` | 10.1 s + **0.57 s** | 0.55 M | 1 MB |
| first build of the 100 k index (2 workers) | 36.7 s | — | — |

- Rebuilding is cheaper than repairing from about 1 % dead upwards. The 5 % threshold is too high, and it is evaluated after the repair has already run.
- Autovacuum on the partition runs with cost delay 2 ms and cost limit 1,000, so the same 4 % repair holds a worker for ≈ 7 min.
- The "20 k rows/s" sentence is the round-3 P-4 figure. The measured repair clears ≈ 20 vectors/s for an indexed namespace.
3. `engram_admin` cannot run the `REINDEX` (P-9).

**Recommendation (decision level).**
- Track dead elements per index in `vector_indexes` (`purged_since_build` against `rows_at_build`), counted by the purge itself.
- Set `vacuum_index_cleanup = off` on the vector partitions permanently.
- Let the hygiene step, at a threshold of ≈ 0.5–1 %, rebuild every touched HNSW on a partition and then run `VACUUM (INDEX_CLEANUP ON)` on that partition. The vacuum then only scans.
- Delete the "20 k rows/s" sentence, and budget hygiene at ≈ 0.35 ms per vector with 2–4 workers.

### P-5: major [regression: N114]: the IOPS table undercounts page touches per MID recall by about 7×, and the hot set leaves out the links reverse index that every graph hop reads

**Where.**
- §3.7 IOPS table: "3 vector arms × ≈ 150 visited × 2 pages … ≈ 900; BM25 ≈ 200; graph arm (≤ 300 nodes) ≈ 300; per recall ≈ 1,400; 50 QPS at a 5 % miss rate ≈ 3,500 IOPS, inside A-1's 10 k".
- §9.1 budget row; D3 hot set; R37.
- §3.8 exact path: "≤ 2,000 rows, ≈ 3.2 MB".
- §3.7 `fact_vectors`: "≈ 1.6 KB each ≈ 16 GB".

**Evidence.** Measured buffer accesses per call:

| Component | Measured | Table |
|---|---|---|
| semantic arm, per-namespace HNSW, MID (ef 150, LIMIT 150), namespaces of 2.1 k–100 k | 943–1,882 (first-touch reads 729–1,655) | 300 |
| same at HIGH (ef 400, LIMIT 400) | 2,648–2,773 | — |
| exact path at 2,100 vectors (rows interleaved across namespaces) | 2,060 page reads (≈ 16 MB, not 3.2 MB) | — |
| graph arm, temporal family, 5 hops, budget 300 (warm 7 ms) | 3,702 | 300 (whole arm) |
| graph arm, semantic + entity family, 2 hops | 1,850 | — |

- Vector rows are 2,048 B each effective (`fact_vectors_p07` is 508 MB for 260 k rows: 4 per page). The 10 M-fact vector heap is therefore ≈ 20.5 GB, not 16 GB.
- §3.8 states that each hop reads both the PK and `fact_links_reverse_idx`. The reverse index (≈ 20 GB) is not in the hot set, which counts only "links PK half 10".
- Per MID recall: 3 × ≈ 1,250 + ≈ 5,500 + BM25 200 (not measured) + temporal and observation joins ≈ 10 k touches.
- At the plan's own 50 QPS and 5 % miss rate that is ≈ 25 k IOPS, against the 10 k budget.
- The hot set comes to ≈ 80 GB, against ≈ 80 GB of usable cache in a 112 GB container. R37's trigger is therefore reached at the stated miss rate.

**Recommendation (decision level).**
- Re-derive N114 from measured touches. M0.6 must use the real arm SQL, including the graph arm and the exact path.
- Raise A-1 to ≥ 50 k IOPS (local NVMe meets this), or cut the graph fan-out.
- Put the reverse index and the corrected vector heap into the hot set and re-check 4 shards per host.

### P-6: major [regression: N119]: purge WAL is unbudgeted. At 35–82 KB per purged fact for the deletes plus ≈ 110 KB per fact for the vacuum repair, one large document or namespace delete exceeds the stated daily WAL, the RTO replay term and the archive rate

**Where.**
- §9.1 Disk: "WAL ≈ 13 KB per fact … ≈ 13 GB/day at 1 M facts/day".
- §9.3 RTO: "WAL replay ≤ 15 min (≤ 13 GB)"; differential "≈ 20 to 40 GB/day".
- §3.7: "batches of 1,000 with 50 ms pauses (≈ 10 s for a 100 k-fact document)".
- §5.4.3 `PurgeRows`: "batches of 5,000", with no pause stated.

**Evidence.**
- **One purge batch.** 1,000 facts of A, each with ≈ 30 links, 2 mentions, 1 stamp and 1 vector, run as `engram_admin` with the §3.8 statement:
  - 396–778 ms per batch;
  - **35–82 MB of WAL** (4.3–10 k full-page images, ≈ 45 k records);
  - the 9 FK cascades are index-driven, with `fact_links(src)` taking 489 ms of 778.
- **The vacuum that follows.** It adds 447 MB for 4,000 facts (P-4).
- **A 100 k-fact document** (§5.4.9) therefore means:
  - ≈ 3.5–8 GB of WAL for the rows, plus up to ≈ 11 GB for the repair vacuums;
  - 45–85 s of purge time, not 10 s.
- **Rate at the specified pacing.** Up to ≈ 2,000 facts/s gives ≈ 70–160 MB/s of WAL for as long as the purge runs.
- **Namespace deletes.** A namespace delete of 1 M facts in 5,000-row batches with no pause produces tens of GB.
- **Moves.** The target's WAL is 535 B per link and 2 KB per vector, measured, so a 1 M-fact move writes ≈ 18–20 GB.
- **Compression.** `wal_compression = zstd` cut a vector-heavy batch only from 14.2 to 8.9 MB.
- **Budgets.** None of this is in the disk, WAL, archive or RTO budgets. `WALSizeHigh` pages at 50 GB on a 300 GB volume that holds 174 GB of data.

**Recommendation (decision level).**
- Throttle `PurgeBatch`, `PurgeRows` and move cleanup by WAL bytes per second (the per-batch `pg_current_wal_insert_lsn` delta, e.g. ≤ 25 MB/s per shard), not by a fixed pause.
- Set `wal_compression = zstd` in `gucs.yaml`.
- Restate the RTO as a function of the WAL since the last backup, and take a differential after every namespace delete and every move.
- Budget pgBackRest `archive-push-queue-max` and archive throughput.

### P-7: minor [regression: §3.7/§9.2]: HNSW builds do not fit `maintenance_work_mem` above ≈ 0.95 M vectors, and two concurrent parallel builds exceed `shm_size: 4g`

**Where.**
- §3.7: "`maintenance_work_mem = 2 GB` … is enough for any single per-namespace index".
- §9.2: "100 k about 35 s, 1 M about 6 min".
- §9.1 `shm_size: 4g`; quota `max_facts: 2000000`; "> 3 M facts" as a move candidate.

**Evidence.**
- **Memory per element.** A build at 32 MB logged "hnsw graph no longer fits into maintenance_work_mem after 14894 tuples". That is 2,253 B per element in memory, so 2 GB holds ≈ 0.95 M.
- **Cost past the limit.** That build (15 k elements in memory, 25 k inserted on disk) took 83.5 s serially, ≈ 3 ms per on-disk insert. In memory the rate is 0.28–0.37 ms per vector with 2 workers.
- **Shared memory.** A parallel build at `maintenance_work_mem = 1 GB` held 1.07 GB in `/dev/shm`: the DSM is sized by `maintenance_work_mem`.
  - At 2 GB, two parallel builds overlapping on one shard (sweeper plus hygiene, or a move target build) exceed 4 GB.
  - This was not reproduced to the error itself.

**Recommendation.**
- Have `engramctl index` set `maintenance_work_mem` per build to ≈ 2.4 KB × vectors.
- Serialize HNSW builds per shard, or size `shm_size` as concurrent builds × `maintenance_work_mem` + 2 GB.
- Restate the build table for namespaces of 1 M vectors and more.

### P-8: minor [regression: N112]: the per-namespace index lifecycle has a stuck state, and its retry silently accepts an invalid index

**Where.** §3.3.4: "The sweeper records `building` before the statement and `ready` after; a crash leaves an INVALID index that `engram_invalid_indexes` lists … (`CREATE … IF NOT EXISTS` makes a retry idempotent)". Also `engram_vector_index_plan`, `rollback_target` and namespace-delete `DropIndexes`.

**Evidence.**
1. **A `building` row is never retried.** `engram_vector_index_plan` returns `create` only when no `vector_indexes` row exists. A row stuck in `building` therefore yields `none` forever: for example, a crash after the row is recorded but before the CREATE INDEX is issued leaves no invalid index for anything to list.
2. **The retry accepts an invalid index.** Executed: a cancelled `CREATE INDEX CONCURRENTLY` leaves `indisvalid = f`. Re-running the generated `CREATE INDEX CONCURRENTLY IF NOT EXISTS` returns `CREATE INDEX` after "relation … already exists, skipping", and the index is still invalid.
   - The sweeper marks it `ready`, the planner ignores it, and the namespace is served by the exact path indefinitely. That path costs 2,060 pages at 2,100 rows and grows linearly (P-5).
3. **Rollbacks leave orphan indexes.** `rollback_target` deletes rows through `engram_cleanup_namespace`, which deletes the `vector_indexes` rows but cannot drop the target's HNSW indexes, built after the copy. Orphan indexes remain and their rows are deleted through the graph.
   - Namespace-delete `DropIndexes` enumerates `vector_indexes` and so misses such orphans.

**Recommendation.**
- Use an explicit `requested → building → ready | failed` machine with a lease.
- Before every CREATE INDEX, check `pg_index.indisvalid` and `DROP INDEX CONCURRENTLY` an invalid index; never rely on `IF NOT EXISTS`.
- Drop by the deterministic `engram_hnsw_ddl` names, and run the drop statements in `rollback_target` before the cleanup.

### P-9: minor [regression: N133e, N131]: runtime privileges, pools and timeouts do not match the role model

**Where.**
- Step table: "`ReindexHygiene` … admin role (owner of the index)" and "`DropIndexes` … admin role".
- N133e: "the worker connects as `engram_app` and the Expunge purge runs as `engram_admin`".
- §9.1 config: only `dsn_file` (`engram_app`) and `relay_dsn_file`.
- §9.1: "pgbouncer pool 24 … **shared**: recall ≈ 12, …, expunge ≤ 2".
- SQL header: writers run with "statement_timeout = idle_in_transaction_session_timeout = 30 s … the relay's 60 s gap watchlist relies on it".
- §3.3: "one attempt under `lock_timeout = 35 s`, longer than any legal 30 s writer"; R36.

**Evidence.**
1. **`engram_admin` cannot manage the indexes.** Executed as `engram_admin`:
   - `REINDEX INDEX CONCURRENTLY fv_…` → "must be owner of index";
   - `CREATE INDEX … ON fact_vectors_p05` → "must be owner of table";
   - `DROP INDEX` → "must be owner of index".
   - Hygiene, `DropIndexes`, `CleanupMove` and the sweeper's builds therefore need `engram_migrate` (owner, `BYPASSRLS`, owner of the `SECURITY DEFINER` functions) inside the long-running worker.
2. **Missing credentials and pools.**
   - The worker's configuration provides no admin, move or migrate credentials.
   - pgbouncer pools are per (database, user), so `engram_admin` and `engram_move` get their own pools. "Expunge ≤ 2" is not enforced by anything: there is one Expunge per namespace, and up to ≈ 150 per shard.
3. **No timeouts on two writing roles.** `engram_admin` and `engram_move` have no `statement_timeout` or `idle_in_transaction_session_timeout`.
   - Yet `engram_admin` holds the shared fence (`PurgeBatch`, `Materialize`, `Finish`) and writes outbox events (`DocumentDeleted{MATERIALIZED, PURGED}`, `NamespacePurged`).
   - So neither the 35 s argument nor A-F1's 60 s horizon covers it, and R36's `config lint` check is stated for `engram_app` only.
4. **The relay can read entities.** `GRANT EXECUTE ON ALL FUNCTIONS` reaches `engram_relay`. Through `engram_entity_fuzzy` (definer, `BYPASSRLS`) it can read any namespace's entity names by setting the GUC.

**Recommendation.**
- Run a separate index runner (the `engramctl` service on the control host) that holds `engram_migrate` and is fed by `vector_indexes` requests.
- List the admin and move DSNs and pools with explicit sizes.
- Set 30 s statement and idle timeouts on `engram_admin` and `engram_move`, raised per session only in `engramctl`.
- Revoke `engram_entity_fuzzy` from `engram_relay` and `engram_move`.

### P-10: minor [regression: N126]: two-hour `REPEATABLE READ` export snapshots block every concurrent index build on the shard and pin the vacuum horizon

**Where.**
- §5.7 `WriteFiles`: "one `REPEATABLE READ READ ONLY` transaction, … `StartToClose` 2 h … exports take no fence (writers are never blocked)".
- §9.2: CREATE INDEX CONCURRENTLY "under `lock_timeout = 5 s` … retried by `engramctl` up to 20 times"; "never holds a lock that blocks writers".

**Evidence.**
- **Executed.** With an open `REPEATABLE READ` transaction that had read only `documents`:
  - `CREATE INDEX CONCURRENTLY` on the unrelated `chunks_p05` with `lock_timeout = 5 s` failed after 5.0 s ("canceling statement due to lock timeout") and left an INVALID index;
  - without `lock_timeout` it waited until the snapshot ended (18 s).
- **What that blocks.** Any export blocks every CREATE INDEX CONCURRENTLY and `REINDEX CONCURRENTLY` on the shard for up to 2 h: the sweeper, hygiene, the mover's target build and the migration procedure. Each 5 s attempt leaves an INVALID index.
- **Side effects of the wait.** The waiting build holds `ShareUpdateExclusiveLock` on its partition, so autovacuum skips that partition.
- **The vacuum horizon.** The snapshot pins the shard's horizon: purge dead tuples cannot be removed, and the HOT tables bloat (`outbox_cursors` alone takes 1 update/s per consumer).

**Recommendation.**
- Drop the long snapshot. Content is insert-only, so `WriteFiles` can read short `READ COMMITTED` ranges bounded by the snapshot start (`ins_seq` of P-1) with marker sets read once; the existing `RecordSnapshot` re-check covers deletes.
- Have `engramctl index` refuse to start while any `backend_xmin` is older than a few minutes.

### P-11: minor: the hard cap does not fit the volume, and the capacity signals ignore stored but invisible rows

**Where.**
- D3: "10 M live facts / 20 M live facts; ~150 namespaces; 300 GB volume"; "50 at hard cap".
- §9.5: "`live_facts` > 20 M (hard cap) → page".

**Evidence.**
- **The volume runs out first.** At §3.7's 174 GB per 10 M facts, the hard cap needs ≈ 348 GB, more than the volume. The 240 GB `full` threshold is crossed at ≈ 13.8 M facts.
- **The volume is underestimated.** Vector rows are 2,048 B, not 1.6 KB, which adds ≈ 4.5 GB to `fact_vectors` (P-5).
- **The signal undercounts.** `live_facts` counts visible facts only. It excludes the rows of P-2, tombstoned rows not yet purged, old-model vectors after `ReembedNamespace`, and the WAL bursts of P-6.

**Recommendation.** Use a volume of ≥ 600 GB or a hard cap of ≈ 13 M, drive capacity signals from relation bytes, and re-derive the fleet count.

### P-12: minor [regression: N123]: restoring to a point before a move-in loses that namespace's blobs, and a tenant intent has no key that the per-namespace replay reads

**Where.**
- §9.3 step 8: "`engramctl blob gc --shard N --reconcile` deletes orphans from after `T`".
- N123/§5.5.5: "PITR to a point before a move-in … run an ordinary move whose source is a scratch instance restored from the old source's backup … `CleanupMove` stays at 24 h".
- §9.3: "Blobs are not backed up".
- §5.4.4: "put one tenant intent object", with the key `_control/deletes/{tenant}/{ns}/…`.

**Evidence (from the procedure text).**
- **The blobs are deleted.**
  - The moved-in namespace's blobs live under `{N}/…`, were written after `T`, and have no rows in the restored database, so step 8 deletes them as orphans.
  - The old source prefix `{S}/…` was deleted by `CleanupMove` 24 h after cutover.
  - The recovery move's blob check ("refuse cutover while any is missing") can therefore never pass.
- **The tenant intent is unreachable.** A tenant intent has no `{ns}` key, so the per-namespace listing of step 5 never sees it.

**Recommendation.**
- Exclude the prefixes of namespaces the catalog places on N from `blob gc --reconcile` until their recovery moves finish.
- Keep moved-out source prefixes, or a copy of them, for the 28-day backup window.
- Write one intent per namespace for a tenant delete.

### P-13: minor (unverified frequency) [regression: N122]: replay orders intents by the API replica's wall clock, so a `Restore` that closely follows an `Invalidate` can be undone by a replay

**Where.** N122 and §5.4.1: intents are named `{deleted_at_rfc3339}-{operation_id}` and applied "in name order …, last state per subject wins".

**Evidence (by reasoning).**
- `deleted_at` is set by whichever `engram-api` replica served the call, and Envoy balances per request.
- Suppose `Invalidate(f)` lands on one replica and `Restore(f)` on another within the clock skew between them. The `Restore` intent then sorts first.
- Replay applies the `Restore`, which is a no-op (`NOT_INVALIDATED`), then the `Invalidate`. The restored shard ends with `f` hidden, though the live shard had restored it: an acknowledged `Restore` is lost.
- Whether `Durability.tla`'s `IntentOrderLastWins` models a clock is unknown; the spec is not yet written (R34).

**Recommendation.** Order intents for one subject by a per-subject generation read under the fence, or make a `Restore` intent name the `Invalidate` operation it reverses and replay it only after that one.

### P-14: nit: lock timeouts for the exclusive document lock disagree

**Where.** §5.1.2 steps 2 and 7, §5.4.1 step 3.2 and §5.4.7 say `lock_timeout = 5 s`, while §3.8 `FinalizeVersion` says "35 s single attempt".

**Problem.** At 5 s, a delete that queues behind a legal 30 s `CommitChunk` fails.

**Recommendation.** Pick one value and generate it from the GUC table.

### P-15: nit: `engram_consumers_passed` scans the namespace's later outbox rows on every poll

**Where.** `engram_consumers_passed`: `max(o.seq) … WHERE o.namespace_id = p_ns AND o.created_at <= p_deleted_at` is served by `outbox_ns_seq_idx (namespace_id, seq)` and filters on `created_at`.

**Problem.** Every poll scans all of the namespace's later events, up to 7 days of them.

**Recommendation.** Store the delete event's `seq` in the tombstone; the marker transaction knows it.

---

## Checked and holding

- **Per-namespace partial HNSW: catalog and planner cost.**
  - With 25 partial HNSW indexes on one partition, planning takes 2.0–2.2 ms in a fresh session and 0.15 ms warm.
  - The plan prunes to one partition, folds RLS into a one-time filter, and picks the namespace's index at every size from 2,100 vectors up. (§3.3.4's "exact preferred at 2,103" did not reproduce; that is harmless.)
  - ≤ 450 indexes per shard is not a planner problem.
- **Marker arrays at execution.** `<> ALL` over 16 k constants is hashed: execution is 4.2–6.4 ms against 4 ms with empty sets. Only planning grows (P-2).
- **`engram_entity_fuzzy`.**
  - It is `SECURITY DEFINER`, owned by a `BYPASSRLS` role, with `search_path = public, pg_temp`, so `pg_temp` comes last and cannot shadow tables.
  - With 235 k entities on the shard it takes 17 ms per lookup. The same predicate run directly as `engram_app` is a parallel seq scan at 118 ms, so the P-5 fix of round 3 works.
- **Build rates.** HNSW builds run at 0.28–0.37 ms per vector in memory with 2 workers, which matches §9.2 below ≈ 0.95 M vectors.
- **Move loader rates.**
  - Inserts run at 104 k link rows/s and 55 k vectors/s without HNSW; the conflict path runs at 315 k rows/s.
  - So "≥ 2,000 facts/s per stream" is plausible; `facts` with BM25 was not measured.
- **The purge's FK cascades** through partitioned children are index-driven (9 RI triggers, 1,000 calls each). The links re-copy probe uses a BitmapOr on the PK and the reverse index.
- **`synchronous_commit = local`** on every role with no synchronous standby: no commit waits on replication, which is consistent with the relay's horizon for `engram_app` writers.
- **Temporal per cell** (512 history shards, a dedicated Postgres with a standby, op-sweeper and `operations`-table restart): no new issue inside this lens. Not load-tested.

## Counts and verdict

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 6 | P-1 to P-6 |
| minor | 7 | P-7 to P-13 |
| nit | 2 | P-14, P-15 |

**Verdict.** The round-3 storage model holds up on PostgreSQL. Its parts are immutable vectored rows, narrow mutable state, read-time markers, and per-namespace partial HNSW indexes, which plan cheaply, prune cleanly and stay correct under RLS. The six majors sit in the loop around that model:

- a move reconcile keyed by entity ids instead of insertion order;
- marker sets fed by causes the expunge never purges;
- filtered queries inside a namespace that fall back to scanning the whole partition;
- an index hygiene step that cannot run and comes after autovacuum's repair;
- an IOPS table about 7× low;
- purge WAL outside every budget.

Each has a decision-level fix that keeps the model.

- **Ready:** staffing the storage and recall layers, with P-2 to P-4 decided first.
- **Not yet:** freezing the hardware budgets (P-5, P-6, P-11) and enabling moves (P-1).

## What I could not verify

- **pg_search.** It is not installable here, so the following were not executed:
  - BM25 plans and Top-K pushdown under RLS;
  - segment merge and vacuum under purges;
  - BM25 WAL;
  - the lexical arm's page touches.
- **Scale and cache.**
  - Partitions were 87 k–260 k rows, not 625 k; HNSW repair and search costs grow with size, so the figures above are lower bounds.
  - Cold-cache latencies were not measured. The server is shared and has a 128 MB `shared_buffers`, so buffer counts are reported instead.
- **Real embeddings.** Synthetic vectors use shared topics. Real filter rejection rates may differ.
- **Not populated.** I generated no observation or page data, so the degraded-mode `observation_inputs` lookups (the "≤ 20 ms per arm") are untested.
- **Not reproduced.** The DSM out-of-space error itself (only `/dev/shm` usage was measured), the pgBackRest archive throughput, pgbouncer behaviour, and Temporal throughput.
- **Not executed.** Restore and failover, and the intent replay ordering (P-12 and P-13 are reasoned from the procedure text).

---

## Review round 4: API contract, Go API, numbers, completeness, Hindsight parity

**Reviewed:** `plans/engram/` at `1c25dbe` (v1.3, after the round-3 redesign D22, N111 to N134), including
`proto/`, `sql/`, `formal/`, the sections, the register and `reference/hindsight-notes.md`, against the task
brief and the product guidance (deletes are rare; O(1) soft delete at ack; asynchronous expunge; SLOs may
degrade while a delete is processed).

**Lens:** (1) the public and internal protos, §4 and the §2 Go API; (2) every number (sizing, latency,
IOPS, throughput, cost §6.8, fill time, engineer-weeks §10) and the citations between sections; (3)
completeness against the brief; (4) Hindsight parity; (5) consistency between sections, register, proto and
SQL after the round-3 rewrite.

**Not repeated:** findings dispositioned in `reviews/round-1.md` to `round-3.md`. Where a round-3 fix is
missing or wrong I say so at the finding.

## How the evidence was produced

```text
buf 1.57.0, from the repository root (the `.git#` input fails inside proto/: no .git there)
  buf lint / buf build / buf format -d --exit-code   (in plans/engram/proto)  → all exit 0
  buf breaking plans/engram/proto --against '.git#ref=50304db,subdir=plans/engram/proto'
      → 50 breaks: events 25, workflow 13, memory.admin.v1 6, memory.v1 6
        (document.proto 3: DeleteDocumentResponse 2-4; errors.proto 1: OperationConflictReason 2;
         namespace.proto 1: Namespace.directives 6; operation.proto 1: OperationKind 7)
  buf breaking ... --against '.git#ref=fce0cd4,...'   (the round-3 review baseline)
      → 88 breaks: events 50, workflow 21, admin 6, memory.v1 11
  buf breaking ... --against '.git#tag=proto/v1.0.0,...'  (the command in README/§4.5/M0.1)
      → "fatal: couldn't find remote ref proto/v1.0.0" (the tag does not exist)
§4.2 "full text" blocks of common.proto and memory.proto: byte-identical to the files.

Scratch Postgres 16.x + pgvector 0.8.6 (own databases rev4_api_scratch, rev4_api_shard, rev4_api_cat;
both DDL files apply cleanly with pg_search stubbed):
  fact_vectors row exactly as in sql/shard_schema.sql (halfvec(768) STORAGE MAIN, 3 immutable copies):
      pg_column_size 1,696 B; 4 rows per 8 KiB page; heap 2,048 B/row → 19.1 GiB (20.5 GB) at 10 M
      HNSW (m=16) on the same rows: 2,048 B/element → 20.5 GB at 10 M
  50,000 clustered 768-d vectors (200 centroids + noise), 1,000 documents, per-namespace-sized HNSW
  (m=16, ef_construction=128), ef_search=150, iterative_scan=relaxed_order, forced index plan:
      unfiltered:           150 rows,   ≈ 780–1,230 buffer accesses, 1–4 ms
      10 % of docs allowed: 150 rows,   ≈ 6,500–8,400 buffers
      1 % of docs allowed:  150 rows,   ≈ 45,000–49,000 buffers, 160–220 ms (rows removed ≈ 15–20 k)
      0.2 % (100 eligible): 7, 16, 54, 68, 98 rows on five runs (rows removed ≈ 20,000 = the
                            hnsw.max_scan_tuples default), ≈ 50,000 buffers, 160–180 ms
      exact scan of the same 50 k rows with the 1 % filter: 24 ms
  600 partial HNSW indexes on one table (namespace predicate): planning 17.6 ms cold, 2.3 ms warm
  (relevant only far outside the ~150-namespaces-per-shard assumption; not a finding).
```

Severity scale as in the earlier rounds: **blocker** = a stated guarantee is false or the system cannot be
built as written; **major** = a stated number, invariant or schedule does not hold without a decision-level
change; **minor** = drift or a gap with a local fix; **nit** = cosmetic. Where a stated guarantee is false
but the fix is a contract-level decision with a narrow blast radius I rate it major and say why.

---

## Findings

### A-1: major: the per-namespace HNSW does not keep "visited ≈ `ef_search`" under tag or `as_of` filters. Selective filters truncate the semantic arms below `min(cap, |visible|)` or blow the IOPS and latency budgets; the unfiltered page-touch count is also 2–4× the table (fix of P-6 incomplete)

**Where.** N114 and §3.7/§9.1 IOPS table ("3 vector arms × ≈ 150 visited × 2 pages … ≈ 1,400 … ≈ 3.5 k
IOPS", basis "per-namespace index: visited ≈ `ef_search`"); R1 in `round-3.md` ("P-6's 1/selectivity term is
gone, visited tuples ≈ `ef_search`"); §8.6.1 ("per-namespace HNSW visits ≈ `ef_search` (150 MID) rows, ≤ 15
ms warm per arm"); §4.3 and D10 (the tag filter is applied "before any ranking, so budgets are never spent on
rows that will be dropped"); §8.2 property 6 ("each arm returns `min(cap, |visible|)` … the filter is inside
the arm"); §3.8 semantic-arm SQL (`document_id = ANY ($allowed_docs)` inside the iterative scan; "Fact-type
filters need the content row and join `facts` after the Top-K").

**Evidence (measured, see the tool runs).**
- P-6 removed the *namespace* selectivity term by giving each namespace its own graph. It did not remove the
  *filter* selectivity term. The allowed-document set (tags), `mentioned_at <= T` (`as_of`) and the marker
  arrays are all evaluated inside the iterative scan, so the scan visits ≈ `ef_search / s` tuples for a
  filter that passes a fraction `s`, up to `hnsw.max_scan_tuples` (20,000 by default; the plan never sets
  it).
- On a 50 k-vector namespace (the average shard namespace is ≈ 67 k facts) a 1 % tag filter costs
  ≈ 45–49 k buffer accesses and 160–220 ms for the fact arm alone, against an arm deadline of 60 ms and an
  IOPS row of 300 touches. A 0.2 % filter (100 eligible rows) returned **7 to 98** of the 100 eligible rows
  across five runs, because the scan stopped at 20,000 tuples. That is exactly the "budget spent on rows that
  will be dropped" that §4.3 says cannot happen, and §8.2's property only passes because it runs on
  `MemIndex`.
- Per-user tags in a shared namespace (`user:alice`, `session:123`) are Hindsight's canonical tag use
  (hindsight-notes §1, Tags), so this is the common filtered query, not a corner case. `as_of` far in the
  past on a large namespace has the same shape (LME namespaces are below 2,000 vectors and take the exact
  path, so the benchmark will not show it).
- The fact-type filter is post-Top-K by design (§3.8), so `fact_types = [EXPERIENCE]` in a namespace that is
  90 % `world` returns ≈ 15 of 150 semantic candidates by construction.
- Whether the planner keeps the HNSW or falls back depends on pgvector's cost model under the real layout
  (partition + partial index + RLS). In my single-table test it chose a sequential exact scan once the
  filter values were constants (24 ms for 50 k rows). In the plan's layout the alternative is the
  `fact_vectors_model_idx` scan of the whole namespace (≈ namespace_rows / 4 heap pages, ≈ 12.5 k pages for
  50 k facts, ≈ 250 k pages for a 1 M-fact namespace). Either outcome breaks a stated number: truncation
  breaks the arm guarantee, the fallback breaks the IOPS and latency rows.
- Even unfiltered, a search touches ≈ 780–1,230 buffers (it reads the element tuple of every neighbour it
  scores, ≈ `ef_search × 2m` candidates), not 150 × 2 = 300. With smaller chunk and observation graphs the
  per-recall total is ≈ 2,500–3,500 touches, i.e. ≈ 6–9 k IOPS at the table's own 5 % miss rate against
  the 10 k budget of A-1. The headroom the table claims (3.5 k of 10 k) is not there.

**Recommendation (decision).** Make the semantic plan selectivity-aware. Estimate eligible rows before the
arm runs (Σ `documents.fact_count` over `$allowed_docs`; a `mentioned_at` histogram for `as_of`). Below a
threshold (≈ 20 k rows; measure in M0.6) run an exact scan restricted to the eligible documents through a
new `(namespace_id, document_id)` index on the vector tables. Above it, use the HNSW with an explicit
`hnsw.max_scan_tuples` and report `RecallStats` partiality when the scan is exhausted. Copy `fact_type` onto
`fact_vectors` (immutable) so the type filter runs inside the arm. Re-derive the N114 IOPS table from
measured buffer accesses, with rows for 10 % and 1 % tag filters and an old `as_of`. Run §8.2 property 6
against `PostgresIndex` on the real image, not only `MemIndex`.

### A-2: major: a hard delete never removes the observation and page versions written from the deleted content. Their text, vectors, BM25 entries and markdown blobs stay on the shard indefinitely, although they can never be served again

**Where.** Brief, Must have 3: "Hard delete by document … cascading to facts, links, entity mentions,
**observations**, search index and blobs". D16 and N119 ("After expunge … no row, vector, index entry or blob
of the document exists on the shard"). §5.4.2 step 2 (the purge list) and step 4 (`derived_hidden` rows for
document causes are permanent); `round-3.md` R2.3 ("their versions are physically deleted only when the
observation or page itself is retired or the namespace is deleted"); §3.6 (page markdown "tombstoned on page
retire / namespace delete"; Reflect transcripts "7 d sweep").

**Evidence.**
- The purge deletes the victim's facts, vectors, links, mentions, chunks, versions and the **evidence rows**
  that name the victim (`observation_inputs`, `observation_version_sources`, `page_version_inputs`). It does
  not delete the `observation_versions` rows whose text was written with the victim in view, their
  `observation_version_vectors`, their BM25 entries, or the `pages/{page_id}/v{n}.md` blobs of hidden page
  versions. No statement in `sql/shard_schema.sql` or §5 deletes from `observation_versions` or `page_versions`
  outside a namespace delete. Retiring an observation sets `retired_at` and deletes nothing.
- These versions are dead weight. A `derived_hidden` row with a document cause hides them at every `as_of`,
  permanently (§4.4 edge rules). An update write shows only the current visible version's text, and a hidden
  current version forces a root rebuild. So keeping the text buys nothing.
- Concrete case: document `d` says "Alice's diagnosis is X". Observation O v3 reads "Alice has X (per the
  medical record)". After `DeleteDocument(d)` and a completed expunge, O v3's text, its vector and its BM25
  entry are still on the shard and in every later backup, for as long as O exists. Hindsight deletes or
  invalidates derived observations on a document delete (hindsight-notes §7), so this is also a parity
  regression for the case the brief names.
- Optional Reflect transcripts (`reflect/{operation_id}.jsonl`, 7-day sweep) quote tool results, i.e. the
  deleted facts, and the expunge does not touch them either.

**Recommendation (decision).** Add an expunge phase after Materialize: for every observation or page version
covered by a document-cause `derived_hidden` row, delete the text, vector, BM25 entry, sources and markdown
blob. If the D9 range arithmetic needs the version number, keep a content-free stub
(`version, root_version, effective_at`). Purge transcripts that reference victim ids, or exclude
`keep_transcripts` namespaces from the RPO-0 delete promise. State in D16 that derived text is erased, and
add "no derived text of a purged document remains" to `Derivation.tla` (ghost evidence already exists for
this) and to `TestExpunge_Stages`.

### A-3: major [regression: new claim in §4.0]: `GetDocument` and `ListDocuments(include_deleting)` return a deleted document's LLM summary, context and metadata after the delete ack

**Where.** §4.0 ("The ack **is** the delete: no read path returns anything derived from the document from this
response on"); `document.proto` header ("From that ack on, every read path … hides everything derived from
the document") and `Document{summary, context, metadata, tags, content_hash}`; `DOCUMENT_STATE_DELETING`
("Not returned by List unless `include_deleting`"); §5.4.1 step 3 and §5.4.2 step 4 (the `documents` row
lives until the end of the purge, ≤ 24 h).

**Evidence.** The marker transaction flips `documents.state` and leaves `summary_blob_key`, `context` and
`metadata` in place. Neither §2.2.7 nor §5 gives `GetDocument` or `ListDocuments` a visibility rule, and both
D16 and §3.10 list the protected read paths without DocumentService. So after `DeleteDocument(d)` acks,
`GetDocument(d)` returns `Document{state: DELETING, summary: <≤ 200-char LLM summary of the deleted
content>, context, metadata, tags}`. `ListDocuments(include_deleting = true)` returns the same for every
deleted document, for up to 24 h. `GetDocumentVersion` also returns content hashes. I rate this major, not
blocker: the leak is a narrow, document-level surface, and the fix is a contract decision, not a mechanism.

**Recommendation.** Define the tombstone view of a `DELETING` document: `document_id`, `state`,
`deleted_at`, the expunge operation id, and nothing derived from content. Clear or ignore
`summary/context/metadata/tags` from the marker on. Add DocumentService to the D16 and §3.10 lists and to
`TestVisibility_AllSurfaces`.

### A-4: major [regression, partial]: exports do not honour `Invalidate`, and "every part applies the read-time visibility predicate" cannot be implemented for a byte stream

**Where.** `memory.proto` `Invalidate` ("soft-hides one FACT from Recall, Reflect **and Export**"); D16
("Invalidate/Restore: visibility flips at commit of the marker row"); N116 ("… and the export apply the same
predicate"); `export.proto` `StreamSnapshot` ("Every part applies the read-time visibility predicate
(decision N116)", new in round 3; parts are byte ranges, resumable by offset, with the whole file's SHA-256
in the last part); §5.4.5 (the Invalidate transaction expires no snapshot); §5.7 step 4 (`RecordSnapshot`
re-checks `document_tombstones` only).

**Evidence.**
1. `CreateSnapshot` → v5 contains fact `f`.
2. `Invalidate(f)` commits `fact_hidden` and touches no `export_snapshots` row.
3. `StreamSnapshot(v5, "facts.jsonl.zst")` streams the stored zstd bytes, `f` included.

The server cannot filter rows out of a compressed file served by byte offset under a fixed SHA-256, so the
proto sentence describes something that cannot be built. A snapshot that is `building` while the Invalidate
commits is promoted with `f` in it. §6.7 names `Invalidate` as the remedy for injected content, which is
exactly what keeps flowing to synced agents. Restore has the mirror problem: it is not reflected until the
next snapshot. That is acceptable staleness, but it is not "visibility flips at commit".

**Recommendation (decision).** Choose one:
- (a) Invalidate also expires snapshots that contain `f`. That brings back the A-14 re-sync churn for every
  curation click.
- (b) Preferred: exports honour curation from the next snapshot, and `GetSnapshotManifest` serves a live
  `hidden_ids` overlay (current `fact_hidden` ids plus `derived_hidden` observation and page versions) that
  `engram-sync` applies before serving.

Either way, delete the "every part applies the predicate" sentence and state the export curation semantics
in D16.

### A-5: minor: the Go API runs the recall arms in parallel on one `store.ReadTx`, which is one pgx connection; as written the arms either fail with "conn busy" or serialise and the 216 ms critical path cannot hold

**Where.** §2.1 Recall walkthrough (`recall.Planner.Recall` runs "inside `store.ReadNamespace`"); §2.2.7
(`ReadNamespace` "runs fn in **one** READ transaction"); §2.2.13 (`Arm.Run(ctx, tx store.ReadTx, …)`, arms
scheduled concurrently by `Needs()`); `index.Searcher` methods take the same `tx`; §2.5 ("5 arms, **one
pooled connection each**"); N54 (lexical ‖ semantic, two graph waves overlapping).

**Evidence.** A Postgres connection executes one statement at a time and a pgx `Tx` is not safe for concurrent
use. Serialising the MID arms (fact semantic, lexical, chunk BM25 and HNSW, temporal, two graph waves, two
observation arms) puts ≈ 300 ms of arm time on the path before rerank. The plan's own pool arithmetic (N114,
"recall concurrency ≈ 12") already assumes a connection per statement. Pre-existing, not raised before.

**Recommendation.** Make `ReadNamespace` yield a `ReadSession` whose `Tx(ctx)` opens one short read
transaction per arm, with the scope GUCs and the read fence. Pass the marker sets and `$allowed_docs` as
values, since they are already parameters. Change `Arm.Run` and `index.Searcher` accordingly, and state in
§2.2.7 that a recall is N read transactions, not one unit of work.

### A-6: minor: the §2 Go API has import cycles, and `api.Deps` and `workflows.Client` cannot implement half of the served RPCs

**Where.** §2.2.7 `store.Ops { …; Outbox() outbox.Writer }`; §2.2.18 `outbox.NewRelay(h *router.ShardHandle, …)`;
§2.2.4 `router.ShardHandle{Store store.Store; …}` and `router.Retry(…, sc authz.RequestScope, …)`; §2.2.2
`authz.MethodPolicy{Bucket quota.Bucket}`; §2.2.21 `quota.Meter.Record(ctx, tx store.Tx, …)`; §2.2.1
`api.Deps`; §2.2.17 `workflows.Client`; §2.1 dependency table (`internal/api` may import authz, router, recall,
pages, export, quota, store, workflows, intent, errs, telemetry).

**Evidence.**
- **Cycles.** `store → outbox → router → store`, and `router → authz → quota → store → outbox → router`. Go
  rejects both at compile time.
- **`api.Deps`** has no Reflect agent, no `catalog.Namespaces`/`Registry`/`Moves`, no `config.Resolver`, no
  `pages.Writer` and no `export.Builder`. So `Server`, which "implements every generated `memory.v1` /
  `memory.admin.v1` server interface", cannot serve:
  - `Reflect`, all of `NamespaceService` and `GetEffectiveConfig`;
  - `Create/Update/Delete/RefreshPage` and `CreateSnapshot`;
  - Tenant, Shard and Move services.
- **Single shard.** `Deps.Store` is a single shard's `store.Store` in a process that serves 32 shards.
- **`workflows.Client`** (5 methods) cannot start `ExportSnapshot`, `PageRefresh`, `Expunge{NAMESPACE}` at
  `ns/{ns}/op/{op}`, `TenantDelete` or `Move`.
- **Signatures.** `export.Streamer.Stream` lacks the `path`/`offset` that `StreamSnapshotRequest` carries;
  `Deleter.DeleteNamespace` drops the client `operation_id`.
- **depguard.** The allow-list omits `blob` (which `Submit` uses), `reflect` and `catalog`.

**Recommendation.** Move `outbox.Writer` (the interface) into `store` or a leaf, give `Relay` a
`store.Store`, and keep `router` free of `authz` by passing `id.Scope`. Complete `Deps` from the proto
service list (a generated check that every RPC has a dependency path). Split `workflows.Client` by capability
(`Starter`, `Signaller`, `Waiter`) to respect the five-method rule. Run `go vet` against a stub module of the
§2 signatures in M0.1, which catches every item above.

### A-7: minor [regression]: the operation-to-workflow contract no longer holds for `DELETE_DOCUMENT`; `CancelOperation`, `WaitOperation` and the move `Restart` are undefined for it

**Where.** `operation.proto` header ("every asynchronous unit of work … is an Operation backed by **exactly
one** Temporal workflow (id `ns/{namespace_id}/op/{operation_id}`)"); `CancelOperation` ("chunks already
committed … stay deleted (purge)"); N70 and §2.2.1 (`WaitOperation` parks on `GetWorkflow(id).Get`); §5.4.1
step 4 (`SignalWithStart("ns/{ns}/expunge", …)`: one singleton per namespace, shared by every delete);
`operations.workflow_id` comment (`ns/{namespace_id}/op/{operation_id}`); §5.5 step 7 (Restart executes
`ns/{ns}/op/{op}` for every `RUNNING` row, and a reconcile loop compares rows with
`DescribeWorkflowExecution`). At `fce0cd4` the delete was `PurgeDocument` at `ns/{ns}/op/{op}`.

**Evidence.**
- **Wait.** `WaitOperation(delete_op)` has no per-operation workflow to long-poll. The singleton's result is
  the whole namespace queue across `ContinueAsNew`. It works only through the DB-poll fallback.
- **Cancel.** `CancelOperation(delete_op)` either cancels the singleton, which stops every pending delete of
  the namespace, or marks the row `CANCELLED`. Operation states are monotone (N35, `fsm`). A `materialized`
  tombstone whose purge was cancelled is not restarted by the sweeper, which looks only for `pending` ones,
  so the 24 h erasure SLA silently lapses.
- **Restart.** After a move, the reconcile loop finds no `ns/{ns}/op/{op}` execution for a `RUNNING` delete
  row. It either starts a second Expunge for the namespace or never converges.

**Recommendation.** State the mapping per kind:
- retain, export, refresh: `ns/{ns}/op/{op}`;
- delete-document: the expunge singleton plus the tombstone's `operation_id`, with progress from
  `expunge_progress`;
- consolidate: the singleton.

Make `DELETE_*` non-cancellable (`PreconditionFailed{OPERATION_NOT_CANCELLABLE}`). Have Restart and the
reconcile loop skip singleton-backed kinds. Fix the proto header.

### A-8: minor [regression]: the proto and the SQL disagree on `operations`, and the §5.4.1 delete transaction violates a `CHECK`

**Where.** `sql/shard_schema.sql` `operations`; `operation.proto` `CancelReason`,
`OperationResult.superseded_by`, `OperationKind`; §5.4.1 step 7; the §3.8 marker SQL; N38; N49; N97; N133d.

**Evidence (on the applied schema).**
- **`cancel_reason`.** The `CHECK` is `cancel_reason IN ('client','superseded','namespace_deleted','move',
  'operator')`, but the proto has `DOCUMENT_DELETED`. §5.4.1 step 7 writes `cancel_reason = DOCUMENT_DELETED`
  whenever a retain of the document is in flight, so the marker transaction aborts on the `CHECK`. That is
  the common "delete while ingesting" case.
- **Contradictory values.** `'superseded'` contradicts N49 (superseded retains end `SUCCEEDED`), and
  `'move'` contradicts N97 (moves restart operations, they do not cancel them).
- **`superseded_by`.** SQL `superseded_by uuid` ("the later operation") against proto `int64 superseded_by`
  ("the newer version number").
- **Kinds.** The kinds include `expunge`, `reembed` and `delete_tenant`, which have no proto counterpart
  (N38's one-to-one mapping). N133d says `DELETE_TENANT` has no operation row at all.
- **Two versions of the delete transaction.** §3.8 inserts `kind = 'expunge'` and neither cancels retains nor
  marks versions `deleted`. §5.4.1 inserts `DELETE_DOCUMENT`, cancels retains and updates
  `document_versions`.

**Recommendation.** Generate the SQL `CHECK` lists from the proto enums (`make gen-docs` already lints
D-rows; lint enums too). Pick one marker transaction and delete the other copy. Make `superseded_by` the
version in both places.

### A-9: minor: the `buf breaking` gate cannot run before `v1.0.0`, and the exception list is incomplete

**Where.** `proto/README.md` CI block ("run from this directory … `--against
'.git#tag=proto/v1.0.0,subdir=plans/engram/proto'`"); §4.5; M0.1 exit ("`buf breaking` blocks a
field-number change"); `events.proto` header; §4.5 "Reserved numbers" rule.

**Evidence.**
- **No gate before 1.0.** The tag does not exist (`fatal: couldn't find remote ref proto/v1.0.0`), and from
  `proto/` the `.git#` input fails anyway. So between now and `v1.0.0`, which covers all of Phases 0 to 2,
  there is no breaking gate on either module. Yet §4.5 argues that a running Temporal history "*is* a wire
  client", and staging and the LME runs keep histories from week 4 on.
- **Missing exception.** Against `fce0cd4` the round-3 breaks in `memory.v1`/`memory.admin.v1` number 18.
  The §4.5 exception table lists all of them except `OperationConflictReason` 2 (`DOCUMENT_PURGING`, N133c),
  which buf also flags against `50304db`.
- **Names reused.** `events.proto` reuses the old *names* on the new `bytes` fields (`reserved 3, 5, 6;` with
  no names). That contradicts §4.5 ("always both `reserved N;` and `reserved "name";` … so that neither the
  number nor the JSON name can be recycled") and changes the JSON type of `chunkId` and friends for protojson
  consumers.
- **Stale sentence.** "Pre-1.0 release candidates were the one exception … the four `reserved` examples" is
  now out of date.

**Recommendation.** Baseline CI on `main@HEAD~1` until `v1.0.0`, with an explicit `buf-breaking-exception`
label and changelog entry per intended break, then switch to release tags. Add `DOCUMENT_PURGING` to the
table. Reserve the old names and give the bytes fields new names (`chunk_id_bytes`). Fix the README to the
repository-root form.

### A-10: minor: the public error and state contract contradicts itself in several places (A-13's fix incomplete)

**Evidence.**
- **`NamespaceNotReady` code.** §4.0 tells clients to treat "`FAILED_PRECONDITION` + `NamespaceFrozen`/
  `NamespaceNotReady`" as retryable, but `errors.proto`, §4.1.6 and `errs` map `NamespaceNotReady` to
  `UNAVAILABLE`.
- **Delete freeze.** D2: "a `delete` freeze surfaces as `PreconditionFailed{NAMESPACE_DELETING}`". But
  `NamespaceFrozen`'s comment, `FREEZE_REASON_DELETE` and the `errs` kind table render it as a retryable
  `NamespaceFrozen{DELETE}`, which the API retries for 30 s. A request that reaches the shard between
  `freeze_delete` and the catalog transaction of §5.4.3 gets the retryable form for a namespace that will
  never come back.
- **`NAMESPACE_STATE_RESTORING`** says "writes rejected …, reads served". N122 and the same file's
  `NamespaceFrozen` say the read fence rejects `frozen/restore`.
- **Invalidating twice.** It is a "no-op success" in `memory.proto` and §5.4.5, but
  `PreconditionFailed{ALREADY_INVALIDATED}` in §4.1.6 and in the `errs` table.
- **`Memory.invalidated_at`** is set for `fact_hidden(reextract)` rows (N58), so a client sees facts it never
  invalidated as "invalidated" and then cannot `Restore` them (`NOT_INVALIDATED`).
- **`ObservationInfo.stale`.** One comment says "hidden by Invalidate … The version is still served" and,
  two lines later, that a version whose segment names a hidden fact "is not served at all".
- **Stale comments:**
  - `UpdateNamespace` still lists `directives`;
  - `OPERATION_KIND_DELETE_TENANT` points at `TenantService.GetOperation`, but the RPC is
    `GetTenantOperation`;
  - `RetainItem.tags` is "applied to every fact and chunk of the item", but tags are a per-document union
    (N116);
  - `Memory.text` "≤ 8 KiB for chunks" against D8's 16 KiB;
  - §1.5 reports arm timeouts in `RecallStats.arms[].status`, which does not exist (`StageTiming.skipped`
    has no reason).

**Recommendation.** One code per detail type, generated into the §4.1.6 table from `errs`. Render the delete
freeze as `NAMESPACE_DELETING` everywhere. Hide `reextract` markers from `invalidated_at`. Extend the N128
comment check to these comments.

### A-11: minor: synchronous RPCs take an exclusive lock with a 35 s single attempt inside a 30 s deadline cap, and `DeleteTenant`'s ack has unbounded fan-out

**Where.** N11 ("others 30 s"); N82 and N120 (exclusive takers: one attempt, `lock_timeout = 35 s`, "longer
than any legal 30 s writer"); §5.4.5 (`Restore` takes the exclusive derivation lock); §5.4.3 and §5.4.4
(`freeze_delete` before the ack, for every namespace of a tenant); `memory.proto` `Restore` ("Synchronous").

**Evidence.**
- **`Restore`.** It waits for every in-flight `ApplyBatch` or `PageRefresh` holding the shared derivation
  lock, up to 30 s. A client calling `Restore` with the 10 s deadline of the §4.0 examples gets
  `DEADLINE_EXCEEDED`, after its intent object was already written. Replay may later apply it (N122 accepts
  that).
- **`DeleteNamespace`** has the same shape: up to 35 s of lock wait inside a 30 s cap.
- **`DeleteTenant`** runs one exclusive freeze per namespace before acking. With N65's per-user namespaces
  (10,000 in a tenant), the ack cannot be bounded by any deadline cap.

**Recommendation.**
- Give delete-class RPCs and `Restore` their own deadline caps (≥ 40 s).
- Make `DeleteTenant` ack after the catalog `deleting` state and intent. Then fence namespaces
  asynchronously, with the resolver's `TENANT_DELETING` as the read barrier and a stated window. Or cap
  namespaces per tenant for synchronous delete.
- Drop "Synchronous" from `Restore`.

### A-12: minor [regression: NG38]: resubmitting a FAILED operation under the same `operation_id` is both allowed and forbidden

**Where.** NG38 ("Clients resubmit with the same `operation_id`; the workflow id makes that idempotent");
§4.1.3 paragraph ("a failed operation may be resubmitted under the same id"); §4.1.3 table ("On replay with
identical request hash … The existing Operation is returned in whatever state it is"); N35 and §2 (`fsm`
operation states are monotone; `Transition` is "monotone; fsm-checked"); `OperationError.retryable`
("resubmitting the same request **with a new operation_id**").

**Evidence.** A resubmission with the same id and hash returns the `FAILED` row. Re-running it needs a
`FAILED → PENDING` transition that the state machine forbids. So NG38's stated replacement for Hindsight's
`retry_operation` does not exist.

**Recommendation.** Decide one rule: a new `operation_id` for every retry (and fix NG38 and §4.1.3), or a
`RetryOperation` RPC that creates a linked new operation.

### A-13: minor: `metadata_filters` on `Recall`, `ListMemories` and `ListDocuments` have no realisation

**Where.** `common.proto` (metadata is "indexed for equality filtering on top-level string values only");
`RecallRequest.metadata_filters`; `index.Filter` and `recall.Query` (§2); §3.8; the shard DDL.

**Evidence.**
- No index covers `facts.metadata` or `documents.metadata`, and `chunks` and `observation_versions` have no
  metadata column (checked on the applied schema).
- No arm SQL, `Filter` field or pipeline step applies the filter.
- On the semantic arm it could only be post-Top-K, which brings back A-1's truncation.

**Recommendation.** Resolve metadata filters at document level into `$allowed_docs`, the same way as tags,
with a GIN `jsonb_path_ops` index on `documents.metadata`. Or mark the fields reserved-for-later and reject
non-empty values with `UNIMPLEMENTED`.

### A-14: minor: `SearchPages` (RPC, Reflect tool, MCP tool) has no storage to search

**Where.** `page.proto` `SearchPages` ("BM25 ∪ HNSW over `page_versions` … no LLM"); §2.2.8
`index.Searcher.SearchPages`; §3.3.6; `sql/shard_schema.sql` `page_versions`.

**Evidence.** `page_versions` holds `markdown_blob_key` and no text. No BM25 index and no page vector table
exist (N111 lists fact, chunk and observation-version vectors only). The Phase 3 exit "`search_pages` R@1 ≥
0.95" has nothing to run on.

**Recommendation.** Store the page text (or a search digest) in `page_versions.text` with a BM25 index, and
add `page_version_vectors` under the N111/N112 rules, so pages follow the same visibility and expunge path as
observations. Or define `SearchPages` as the observation arms over the page's evidence.

### A-15: minor [fix missing: A-10, A-22]: the rerank-skip threshold still has three values, and the connection figures were not corrected

**Evidence.**
- **Three thresholds.** §9.3 `engram.yaml` still says `rerank_min_remaining: 120ms`, and §8.2's recall row
  still says "`stage=FUSED` when remaining deadline < 150 ms". D10 says "one derived constant used by D10,
  the proto comment and `engram.yaml` (N106)" = 106 ms.
- **Concurrency.** §2.5 still says "≤ 50 QPS per shard target → ≤ 250 concurrent arm queries per shard …
  keeps the pool below saturation", the per-second/concurrency confusion A-22 named.
- **Pool size.** §4.1.2 still says "the per-instance pool is only 32 (D3)", although round 3's disposition
  reads "pool figure fixed at 16".

**Recommendation.** Generate `rerank_min_remaining` and the §2.5 and §4.1.2 numbers from the N106 and N114
constants (`make gen-docs`).

### A-16: minor: number drift in sizing, cost and schedule

**Evidence (each recomputed).**
- **Vector heap.** `fact_vectors` packs 4 rows per page (measured 2,048 B/row, not "≈ 1.6 KB each"), so the
  heap is ≈ 20.5 GB, not 16 GB.
- **HNSW.** At ≈ 2 KB/element the index is ≈ 20.5 GB, not 18 GB.
- **Totals.** The hot set is ≈ 72 GB, not 65 GB, and the footprint ≈ 181 GB, not 174 GB. Still inside the
  96 GB cache and the 300 GB volume, so this is not decision-changing, but D3 and §3.7 quote the old figures.
- **Reflect typical cost (A-19 fix incomplete).** Table 6.8-A bills 2.1 k output tokens for 6 iterations, but
  the prompt row says "1,500 + tool-call tokens ≈ 600 per iteration", and the worst case uses exactly that
  rule (1,500 + 10 × 600 = 7.5 k). The same rule gives 1,500 + 6 × 600 = 5.1 k, so the typical Reflect is
  13.3 k × 2.5 + 120 k × 0.25 + 5.1 k × 10 per M ≈ **$0.114**, not $0.085. The Phase 2 cost gate and §8.8
  inherit the error.
- **A-F = 4 embeddings.** The "Per 1 k facts" row at A-F = 4 uses $0.003 for embeddings, but 250 chunks ×
  1,200 tokens × $0.02/M = $0.006. The total stays ≈ $0.21.
- **Temporal gate.** M0.4 gates Temporal at ≥ 1,000 events/s, but §9 sizes `numHistoryShards` for a
  backfill peak "one to two orders above the ≈ 58 events/s of an online cell", i.e. 580–5,800 events/s. The
  gate covers only the low end of the stated peak.
- **MVP week.** D17 and N105 say "MVP exit is week 24"; §10 and its Gantt say week 23.

**Checked and correct:**
- `calls_per_chunk` 3.55 (A-F 10) and 2.1 (A-F 4);
- 600 RPM / (60 × 3.5) = 2.86 chunks/s;
- 100 M chunks / (4 × 2.86) = 101 days; 250 M chunks / (4 × 4.76) = 152 days;
- $0.157 and $0.21 per 1 k facts; $118 per LME-S run; 273 k calls; 7.6 h;
- the batch-API $124 k;
- XIDs 54/s → 214 days;
- every §10 milestone sum (Phase 0 15.0, Phase 1 45.0, Phase 2 20.0, Phase 3 24.0, F/B 14.0; E1 27.0, E2
  26.5, E3 26.5);
- the Gantt day counts.

### A-17: minor: exit criteria and risk triggers assume throughputs the D3 rate cap forbids

**Where.** M2.1 exit ("`consolidation_lag` p95 < 5 min at 10 chunks/s"); R4 trigger ("< 5 chunks/s/worker");
M1.1 exit ("≥ 8 chunks/s/worker with `GW_LATENCY_MS=3000`"); D3 and §5.1.6 formula (labelled "chunks/s per
cell" with the per-worker term `32 / L`); §9.1 (2 workers per cell).

**Evidence.**
- **M2.1.** At 10 chunks/s a cell needs 10 × 3.5 × 60 = 2,100 RPM. At the D3 cap of 600 RPM extraction alone
  takes the whole budget, and consolidation lag grows without bound.
- **R4.** At the D3 operating point each of the 2 workers does 1.45 chunks/s, so R4's "< 5 chunks/s/worker"
  trigger fires permanently in production.
- **M1.1** is achievable only against the uncapped `DeterministicClient`.

**Recommendation.** State the gateway profile of each throughput gate (fake, uncapped, or the D3 cap). Write
the formula as `min(N_workers × 32 / L, RPM_cap / (60 × calls_per_chunk))`. Express R4 per cell against
2.9 chunks/s.

### A-18: minor: the schedule pays for spec work that §7 reports as done, and the front matter and README contradict §7 (A-23/A-24 fixes incomplete)

**Where.** M0.7 (3.5 ew: "`ShardMove.tla` rewritten … `Storage.tla` and `Durability.tla` … `Derivation.tla`");
M0.8 (2.5 ew: "D22 lands (N111 to N132) in §1 to §12, `sql/` and `proto/`"); §7 ("Four specs are new in the
round-3 redesign … Every number below is copied from a log in `formal/tla/results/`"); `formal/tla/` (the
four specs, 15 configurations and their logs exist); front matter ("`Storage`, `Derivation`, `Durability`
and a rewritten `ShardMove` are specified in the review but not yet written"; "Five TLA+ specifications …
not yet model-checked"); `README.md` (same, plus "`*_Gate*.cfg` … written but unrun"; no such files
exist); §7.6 ("`formal/lean/` is a Lake project … `SORRY_BASELINE` (currently 3)") against `formal/lean/`,
which holds only `Engram/*.lean`.

**Evidence.** Either §7 overclaims or 3–6 ew of Phase 0 re-buy finished work. That is more than the 2-ew
overrun §10 reports, which disappears if M0.7 is re-scoped to conformance and trace validation and M0.8 to
the real-repository migrations.

**Recommendation.**
- Re-scope M0.7 and M0.8 to what remains, and re-draw the Gantt.
- Update the front matter, README and executive summary to the §7.1 table.
- Either add the Lake skeleton (`lakefile.lean`, `lean-toolchain`, `SORRY_BASELINE = 3`) or change §7.6 to
  the future tense.

### A-19: minor: the brief's "an observation must never outlive its sources" is relaxed for `REPLACE` without being recorded as a departure

**Where.** Brief, Should have 6; §5.4.6 ("Replace retires a chunk … observations and pages derived from them
**stay visible**, marked `stale_write`"); §5.4.2 "Other targets" (tombstoned chunks purged after 1 h); §5.2
("A rebuild with zero visible sources … Go retires the observation"); D13 and N130 (consolidation defers on
quota).

**Evidence.** After a `REPLACE` that removes every source chunk of O, O is served with an empty visible
`source_fact_ids` and `proof_count = 0`. After 1 h its sources are physically purged while O is still served,
until a rebuild runs. With the namespace's consolidation `DEFERRED` by `llm_tokens_per_day`, that is
unbounded. §11 records no departure, and no TLA+ invariant covers it.

**Recommendation.** Hide an observation version whose visible source set is empty at read time (one more
`EXISTS` in the N117 predicate), or state the departure, its bound and its quota exemption, so zero-source
retirements bypass `Reserve`.

### A-20: minor: curation `Restore` can resurrect a stale extraction next to its re-extracted twin

**Where.** `sql/shard_schema.sql` `fact_hidden` (`PRIMARY KEY (namespace_id, memory_id)`, `cause ∈
{invalidate, reextract}`); §3.8 `FinalizeVersion` (`INSERT … 'reextract' … ON CONFLICT DO NOTHING`);
§5.4.5 `Restore` (`DELETE … WHERE cause = 'invalidate'`); N58; `ListMemories.include_invalidated`.

**Evidence.**
1. `Invalidate(f)`.
2. A prompt bump re-extracts the kept chunk. `CommitChunk` inserts the twin `f'` and re-applies the
   curation (`fact_hidden(f', invalidate)`). `FinalizeVersion`'s `reextract` insert for `f` hits the existing
   `invalidate` row and does nothing.
3. `ListMemories(include_invalidated)` shows both `f` and `f'` as invalidated. A curator who restores both
   makes the stale-key `f` and `f'` visible together, which is the "two live fact sets on a kept chunk" that
   N58 forbids. Restoring only `f` serves the stale extraction.

**Recommendation.** Key `fact_hidden` on `(memory_id, cause)`, hide when any row exists, and let `Restore`
delete only the `invalidate` row.

### A-21: minor: after any delete, read-only sync clients have no servable export until a writer calls `CreateSnapshot`

**Where.** `export.proto` header ("A local copy whose version is `expired` is refused by the sync client until
it has applied the next delta"); §5.7 sync step 3; `CreateSnapshot` needs `memory.write`; no automatic
snapshot exists.

**Evidence.** The marker transaction expires the latest snapshot L. The next delta exists only inside
v{L+1}, which nobody creates automatically. A `memory.read` sync client therefore refuses its local copy
and cannot fetch a replacement, so the agent's local search goes dark until some writer snapshots.

**Recommendation.** Schedule a `CreateSnapshot` (debounced, e.g. 10 min) whenever the marker transaction
expires a `ready` snapshot that is the latest, or ship a delete-only delta that the expiry itself produces.

### A-22: minor: Hindsight parity gaps that are neither built nor listed as non-goals

**Evidence.**
- **Reflect order.** Hindsight forces `search_mental_models` → `read_mental_models` → `search_observations` →
  `recall` and stops forcing when mental models are fresh (hindsight-notes §5). Engram forces
  `search_observations` then `search_memories` and offers `search_pages` only as a free tool (§6.5; a stub
  until Phase 3, M2.2), yet §1.9 marks Reflect "**same** — deliberately kept". The page-first short-cut is
  Hindsight's main cost lever for Reflect.
- **NG39** says "The ledger body is fetched by `GetDocument` only", but `Document` has no body field.
  Hindsight returns `original_text`. A-18 of round 3 asked for an RPC or a non-goal row; the row now claims
  an RPC that does not exist.
- **No row and no RPC:**
  - `list_tags` (`GET /banks/{id}/tags`);
  - document tag update (`PATCH /documents/{doc}`, tags only), which is now a single narrow-row update
    because tags live on `documents`;
  - `clear_memories`;
  - prompt preview;
  - mental-model dry-run refresh.

**Recommendation.** Make the page search the first forced step once pages exist, or mark §1.9 "different".
Fix NG39 or add `GetDocumentBody`. Add `ListTags` and `UpdateDocumentTags` (cheap) or non-goal rows.

### A-23: minor (unverified): `tenant.admin` conflates tenant administration with fleet operation

**Where.** D13 scopes; §4.1.9 ("`tenant.admin` | Everything in `memory.admin.v1`"); §4.1.1 ("Admin API
called with a tenant token → `PERMISSION_DENIED` … behind a separate Envoy route"); §2.2.2
`MethodPolicy.Target` (`Namespace | Tenant | Admin`).

**Evidence.** One scope grants a tenant's own lifecycle (`DeleteTenant`, `GetTenantOperation`) and fleet
control over every tenant (`MoveService`, `ShardService`, `ListTenants`). Nothing states that an `Admin`
target checks `token.tenant_id`, so separation rests on network routing. The brief asks for the JWT check in
the interceptor on every method. Exploitability depends on who can mint `tenant.admin` at the external IdP,
which I could not verify.

**Recommendation.** Split the scopes: `tenant.admin` is tenant-bound, and the interceptor enforces
`token.tenant_id == request.tenant_id`; `engram.operator` is fleet-wide, with a separate issuer or audience.

### A-24: nit: Go API idiom and pattern fit

All 90 interfaces in §2 have ≤ 5 methods (counted). Residual issues:
- **Stdlib clash.** `internal/reflect` shadows the standard `reflect` package.
- **Leaf imports.** The leaves rule says "standard library only", but `id` wraps `github.com/google/uuid`.
- **Stringly-typed ids remain:**
  - `CitationVerifier.Verify(cited []string, returned map[string]struct{})` (the citation invariant itself);
  - `intent.Intent.Subject string`;
  - `workflows.Client.Cancel/WaitResult(workflowID string)`;
  - `router.Forward(cell string)` and `ListShards(cell string)`;
  - `errs.NotFound(resource, id string)` instead of `memoryv1.ResourceKind`.
- **Untyped ids.** `index.Hit.ID` and `recall.Candidate.ID` are untyped `uuid.UUID` unions.
- **One version type.** A single `id.Version` covers document, observation and page versions, against
  N132's "typed ids per entity".
- **Pipeline.** The sequential `pipeline.Step` chain `Embed → Visibility → Arms` cannot express N54's
  schedule (lexical and temporal start at t = 2 ms, overlapping the embedding). The cost is small (≈ 3–20 ms
  of Visibility on the path), but the pattern as named does not describe the planner.
- **"Saga that only goes forward"** (expunge) is a workflow, not a saga.

### A-25: nit: register drift

- D2 says the only shard-wide indexes are the outbox PK and the scheduler partials. The applied schema also
  has `idempotency_keys_expiry_idx (expires_at)`.
- D12's Reflect tool list lacks `search_pages` (N73).
- N116 says "three marker sets"; §3.10 and §5.4.1 pass four (`doc_pending`).
- README still describes D22 as N111 to N132; N133 and N134 exist.

---

## Checked and holding

- **Protos.** `buf lint`, `buf build` and `buf format` are clean. The §4.2 full-text blocks equal the files.
  Every top-level workflow input carries `schema_version`. The `string → bytes` event ids now use new field
  numbers (the A-11 silent-decode hazard is gone). The round-3 exception table covers 17 of the 18
  public/admin breaks.
- **N127 and N129 are applied.** `superseded_by`, `cancel_reason`, `DELETE_TENANT`/`GetTenantOperation`,
  the MCP session-scoped `request_id`, deterministic minted document ids, typed `Directive`,
  `GetEffectiveConfig`, rank 1 always whole, MCP resource metadata.
- **Cost and throughput.** Table 6.8-B is internally consistent apart from A-16. D3 throughput, fill time
  and cost agree across D3, §1, §5.1.6, §6.8, §11 and the front matter.
- **Schedule.** §10 sums and per-engineer chains are exact (A-16 lists the two date and gate mismatches).
- **Brief coverage.** Every service and method the brief names exists with the requested streaming choices,
  page tokens, field masks, typed error details, deadlines and idempotency keys. ConnectRPC is justified
  over grpc-gateway, and the MCP and REST mappings are complete.

## Counts

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 4 | A-1, A-2, A-3, A-4 |
| minor | 19 | A-5 to A-23 |
| nit | 2 | A-24, A-25 |

## Verdict

**The contract is close to buildable, but four decisions are missing.** The round-3 redesign fixed what it
targeted: the protos are clean and mostly consistent, the delete contract is O(1) at ack, and the numbers in
§6.8 and §10 recompute.

The four majors are not mechanical:
- **A-1.** The headline storage claim, "a query visits only its own graph, visited ≈ `ef_search`", holds
  only for unfiltered queries. Tag- and `as_of`-filtered recalls either lose results or blow the IOPS and
  latency budgets. This needs a selectivity-aware semantic plan, measured in M0.6 before M1.2.
- **A-2.** The hard delete the brief requires leaves derived observation and page text on disk forever, for
  no serving benefit.
- **A-3.** DocumentService breaks the new "no read path" promise.
- **A-4.** Exports cannot honour `Invalidate` as stated.

The minors cluster in two places:
- **The §2 Go API** (A-5, A-6, A-24) does not compile as written and does not cover the RPCs it claims to
  implement. It needs one mechanical pass in M0.1 against a stub module.
- **The proto/SQL/section seams** (A-7, A-8, A-10, A-12, A-15) need generation from one source rather than
  more hand edits; this is the third round in which a fold was accepted but not completed.

## What I could not verify

- **pgvector under the real layout.** pgvector's planner choice on the real layout (hash partition + partial
  HNSW + RLS + `force_custom_plan`) for filtered vector queries. A-1 states both outcomes; which one occurs
  decides whether the failure is truncation or IOPS. Buffer counts came from a 4-vCPU sandbox on clustered
  synthetic vectors, not nomic embeddings, and absolute latencies are hardware-dependent.
- **pg_search.** It was stubbed, so BM25 page touches (the IOPS table's 200) are unmeasured.
- **Lean and TLA+.** Lean was not type-checked, and TLA+ results were not re-run (formal is outside this
  lens). §7's claims are taken as written only to show the conflict with the front matter.
- **Gateway rate limits.** Whether the 600 RPM cap is per cell, per model or per organisation (as in round 3).
- **MCP sessions.** Whether MCP clients always send `Mcp-Session-Id`. Without it, `sha256(session ‖
  jsonrpc-id ‖ tool)` collides across clients again; the plan does not say the adapter issues and validates
  session ids statelessly across replicas.
- **`tenant.admin`.** Who can obtain `tenant.admin` from the external IdP (A-23).
- **Hindsight parity.** Taken from `reference/hindsight-notes.md` only. Items marked UNVERIFIED there were
  not used.

---

## ADVICE-R4 — closing the round-4 findings at the edges of the D22 redesign

**Scope.** `round-4-correctness.md` (C-1…C-23), `round-4-postgres.md` (P-1…P-15), `round-4-api.md`
(A-1…A-25). The round-3 core holds and is not reopened: immutable content rows, vector side tables,
read-time visibility over tombstones and evidence segments, one asynchronous Expunge per namespace,
delete intents, dirty-copy moves with a `ready` state. Every decision below keeps that core and
changes only the mechanism that a finding showed acting outside the predicate's assumptions.

**Binding product guidance.** Deletes are rare. The synchronous part of a delete is an O(1) soft
marker honoured by every read path at ack; all physical work is an asynchronous, throttled expunge;
SLOs may degrade for a namespace while a delete is processed. Simple and correct beats cheap.
Ids N135… are proposals for D23; the register author assigns the final numbers.

## 1. Evidence outlives the facts it names (C-1, C-6, P-2; folds C-14, C-15, C-18, A-19, A-20)

**Mechanism (N135).**
1. **No evidence row cascades from `facts`.** `observation_inputs`, `observation_version_sources`
   and `page_version_inputs` drop their `FOREIGN KEY … REFERENCES facts … ON DELETE CASCADE`.
   `fact_id`/`memory_id`, `document_id` and `document_version` stay as plain immutable columns
   (`observation_version_sources` gains the two document columns, like `observation_inputs`).
   Evidence rows keep only the cascade from their own version row: they die with the version
   (decision 3's derived purge, a retire purge, or the namespace delete), never with a fact.
2. **Re-extraction is a write, not a hide.** `FinalizeVersion` keeps inserting
   `fact_hidden(cause = 'reextract')` for the old-key facts of a kept chunk, but only the **fact and
   chunk arms** read that cause. The derived-version predicates (`engram_obs_version_hidden`,
   `engram_page_version_hidden`) test `fact_hidden` with `cause = 'invalidate'` only. In the same
   transaction `FinalizeVersion` marks `observations.stale_write = true` for every observation whose
   current segment names an old-key fact (one indexed update through `observation_inputs_fact_idx`),
   and the new-key facts are unconsolidated, so consolidation evolves the observations through
   ordinary rounds, as §6 already says.
3. **Re-extracted facts are purged.** New Expunge target `REEXTRACTED_FACTS`: facts with a
   `fact_hidden(reextract)` row older than the same 1 h grace as `CHUNK_TOMBSTONES` lose their rows,
   vectors, links, mentions and the marker (the marker's own FK cascade from `facts` stays: it is a
   marker, not evidence). Marker sets are again bounded by expunge lag; the watermark pin (H-23) ends
   with the purge, and `done` stamps for purged facts are written by the purge itself.
4. **`fact_hidden` is keyed `(namespace_id, memory_id, cause)`.** `Restore` deletes only the
   `invalidate` row; a fact is hidden from the fact arm while any row exists (C-15, A-20).
5. **The invalidation set is never a per-request array.** `fact_hidden(invalidate)` is permanent by
   design and tested by PK anti-join (`NOT EXISTS`) in every arm and predicate; the per-request
   arrays carry only `chunk_tomb` and the `doc_tomb` jsonb, both bounded by expunge lag. The 16 k
   alert applies to those two sets; the planner cost P-2 measured (≈ 18 ms per arm at 16 k) is gone.
6. **Predicates fail closed** (C-18): `engram_obs_version_hidden` and `engram_page_version_hidden`
   return hidden when no version row exists (`NOT EXISTS (version row) OR …`). Decision 3 keeps a
   content-free stub for every purged derived version, so the version row always exists.
7. **Served version at `as_of`** (C-14): the SQL rule wins. `Served(T)` is the version current at
   `T` (`effective_at ≤ T`, `superseded_at > T`); nothing is served for that observation when it is
   hidden. Retirement is filtered by its own time (`o.retired_at IS NULL OR o.retired_at > p_as_of`).
   §5.2.5's example, the spec's `Served` and `TestVisibility_SegmentHiding` follow.
8. **Zero-source versions are not served** (A-19): the serving join already computes `proof_count`
   over visible sources; a version with `proof_count = 0` is dropped by that join, and a rebuild that
   finds no visible source retires the observation without a gateway call (exempt from `Reserve`).

**Why it closes the findings.** C-1: after REPLACE + chunk purge the `observation_inputs` rows naming
`(D, v1)` still exist, so the pending predicate and `Materialize`'s `INSERT … SELECT` both find `O@v1`
when `D` is deleted later; pages and observations behave alike. C-6/P-2: a prompt bump hides no
derived content; the observations are flagged and rebuilt, the old-key facts leave after 1 h, and
marker sets are bounded by lag again (capacity signals count unpurged rows meanwhile, decision 6).

**Rejected.** (a) Re-pointing inputs at the new-key twins: a twin may not exist after REPLACE, and it
rewrites immutable evidence. (b) P-2's `chunk_extraction(current_key)` join: a second currency
mechanism that still leaves the old-key facts to purge. (c) The spec's "latest visible ≤ T": safe
but misreports history.

**Spec and tests.** `Derivation.tla` gains `Replace` (chunk tombstone, new facts), `ChunkPurge` /
`ReextractPurge` (remove `born` facts, **leave `finp`/`oinp`**) and `Reextract` (cause `reextract`,
derived not hidden, `stale` set). New invariant `EvidenceOutlivesFacts` (`finp` of a version is
constant after commit); `NoDerivedServedFromVictim` must hold across REPLACE → purge → delete.
`Derivation_CascadeEvidence.cfg` must fail it; `_ReextractHides.cfg` must fail
`ReextractKeepsDerivedVisible`. Tests: `TestVisibility_AllSurfaces` runs REPLACE, purge, delete in
sequence; `TestReextract_Rebuilds`.

## 2. One commit rule for every writer of a derived version (C-2, C-3, C-11; folds C-7)

**Mechanism (N136): the derivation commit rule.** Every writer of an observation or page version
— `ApplyBatch` (stage 2 writes, degraded-mode root rebuilds, merges), `CommitPageVersion`
(`page/v1` and `page_full/v1`), and any rebuild — commits in one derivation transaction that:
1. takes the **shared** derivation lock (`engram_try_derivation_lock`, refused → retry);
2. **re-verifies every rendered input in a fresh statement under that lock**: fact inputs against
   the full marker sets (all open tombstones, `chunk_tomb`, `fact_hidden` of both causes),
   observation-version inputs with `engram_obs_version_hidden` against **all open tombstones**
   (`engram_doc_tomb(ns, false)`), not only pending ones;
3. **checks its base**: the version it rendered from is still current (`observations.current_version
   = base_version`, `pages.current_version = base_version`) and visible; a root rebuild has no base
   and skips this check;
4. on any failure: `ROLLBACK`, discard the rendered result, and re-derive from current evidence (a
   root rebuild for observations, `page_full/v1` for pages). It never "repairs" by adjusting inputs.

**Proposal lifecycle (C-3, C-11).**
- `consolidation_proposals` is keyed `(namespace_id, batch_key, attempt)`; `consolidation_batches`
  gains `attempt` and `state ∈ {routed, stored, applied, discarded, capacity}`. Every `update` and
  `merge` op in the stored list records `base_version` and `base_root`. Nothing is ever `DELETE`d
  by `engram_app` (its grant stays `SELECT, INSERT` on proposals): a discard or a capacity retry
  bumps `attempt` and writes a new list; the old list is dead by key. `consolidation_applied`
  references `(batch_key, attempt)`.
- `ApplyBatch` applies an `update` only if `current_version = base_version` (then `root_version` is
  inherited from the base, which is the same thing as `base_root`); otherwise the **whole** proposal
  is discarded (`state = 'discarded'`, new attempt) and the batch is re-routed. The capacity
  re-run is a new attempt with `prompt_variant = 'capacity'`, so `ON CONFLICT DO NOTHING` can no
  longer keep the overflowing list.
- "Already applied" is keyed on `consolidation_batches.state = 'applied'`, written in the same
  transaction as the `done` stamps; an all-skip batch has an empty list, applies zero ops, stamps
  its facts `done` and sets `applied` (C-11c).
- Degraded mode (markers `pending`) still runs only root rebuilds, and they obey the rule above.

**Materialize and Restore (C-7).** Cause-tagged `derived_hidden` rows are the design; `Restore`
keeps the exclusive derivation lock (N133b); every `Materialize` batch **re-reads `fact_hidden` and
the open tombstones under the exclusive lock**. §7.2.1 and §7.2.6 drop "candidate for removal".

**Why it closes the findings.** C-2: the page refresh whose LLM call spanned the delete re-verifies
`f` under the shared lock after `Materialize` finished; `f` is covered by an open tombstone, so the
commit is refused and `page_full/v1` runs from current evidence. C-3: the stored update against
`O@v2` (root 1) finds `current_version = 3` and is discarded instead of becoming `v4` in segment 3;
the same check closes the lost update without any delete. C-11: discard needs no `DELETE`, the
capacity retry has its own key, all-skip batches terminate.

**Rejected.** (a) Inheriting `root(base)` and requiring the base to be *visible* at apply: a visible
but superseded base still lets stale text overwrite a rebuild. (b) Re-verifying only
`input_fact_ids`: the quoted base text can name a victim that is not among the inputs. (c) Holding
the shared lock across the LLM call: it would block `Materialize` for a minute per refresh.

**Spec and tests.** `Derivation.tla`: `Writers = {w1, w2}`; a persisted proposal `prop[w]` with
`base`; `Commit` enabled only if `cur[o] = prop.base` and `verified` is a fresh read under the lock;
the page writer has its own `Verify`. Invariants `BaseCurrentAtCommit` and `NoLostRebuild` (a root
rebuild is never superseded by a version whose base predates it). `Derivation_PageNoVerify.cfg`
must fail `NoDerivedServedFromVictim`; `_StaleProposal.cfg` (no base check) must fail
`NoLostRebuild`; `_MatOnce.cfg` (invalidated set computed once) must fail `RestoreExact`. Tests:
`TestPageRefresh_DeleteMidCall`, `TestConsolidation_{StaleProposalDiscarded,AllSkipStamps,CapacityAttempt}`.

## 3. Hard delete reaches derived artefacts and document metadata (A-2, A-3, A-4; folds A-7, A-21)

**Mechanism (N137).**
1. **Expunge phase 2b, `DerivedPurge`**, after `Materialize`, under decision 6's WAL throttle: every
   version a document-cause `derived_hidden` row covers (same `root_version`, `version ≥
   from_version`) is reduced to a **content-free stub** in one admin-role transaction per batch:
   delete the version row (cascading its inputs, sources, vector row and BM25 entry) and re-insert
   `(id, version, root_version, effective_at, text = '', stub = true)`; for pages also delete
   `pages/{page_id}/v{n}.md`. The stub keeps the D9 range arithmetic and `*_version_meta` valid, is
   what decision 1's fail-closed predicate finds, and stays covered by the permanent `derived_hidden`
   row. N113 holds: the stub is an insert, never an `UPDATE`.
2. **Reflect transcripts**: the purge deletes every `reflect/{op}.jsonl` of the namespace written
   before `deleted_at`. Transcripts are an optional debugging aid; a blunt rule is correct and O(list).
3. **DocumentService tombstone view (A-3).** The marker transaction already updates the narrow
   `documents` row; it now also clears `summary_blob_key`, `context`, `metadata` and `tags` and
   records the summary blob in `blob_tombstones`. From the ack, `GetDocument` and
   `ListDocuments(include_deleting)` return `{document_id, state = DELETING, deleted_at,
   up_to_version, operation_id}` only; `GetDocumentVersion` for a covered version returns
   `NOT_FOUND{DOCUMENT_DELETED}` (no `content_hash`). A revival (N133c) writes fresh values for
   the new life. DocumentService joins the D16 and §3.10 lists and `TestVisibility_AllSurfaces`.
4. **Export (A-4, option b).** Deletes expire snapshots (N126, unchanged). Curation is honoured
   from the next snapshot, and `GetSnapshotManifest` carries a live `hidden_overlay`: current
   `fact_hidden(invalidate)` ids plus the `(kind, id, root_version, from_version)` rows of
   `derived_hidden`; `engram-sync` applies the overlay before serving. The sentence "every part
   applies the read-time visibility predicate" is deleted; D16 states: *delete → expiry + delta,
   invalidate/restore → overlay now, snapshot later.* `Invalidate` never expires a snapshot.
5. **Snapshots after a delete (A-21).** When the marker transaction expires the latest `ready`
   snapshot, the Expunge's `Materialize` step starts a system-initiated `ExportSnapshot` (debounced
   10 min per namespace, no `memory.write` caller needed), so read-only sync clients regain a
   servable copy without a writer.
6. **Operation ↔ workflow mapping (A-7).** Per kind: retain, export, refresh → `ns/{ns}/op/{op}`;
   delete-document → the expunge singleton plus the tombstone's `operation_id`, progress from
   `expunge_progress`; consolidate → the singleton. `DELETE_*` are non-cancellable
   (`PreconditionFailed{OPERATION_NOT_CANCELLABLE}`); `WaitOperation` for them polls the tombstone;
   move `Restart` and its reconcile loop skip singleton-backed kinds.

**Why it closes the findings.** A-2: after the purge no text, vector, BM25 entry, markdown or
transcript written with the victim in view exists on the shard or in later backups. A-3: the one
document-level surface that leaked is content-free from the ack. A-4: the proto describes something
buildable, curation reaches synced agents within one manifest poll, and A-14's churn stays gone.

**Rejected.** (a) Deleting hidden versions outright (no stub): breaks D9's range rule, the
fail-closed predicate and page inputs that cite the version. (b) A-4 option (a), Invalidate expires
snapshots: every curation click forces a full re-sync. (c) Indexing transcripts by fact id: a new
table for a debugging aid.

**Spec and tests.** `Derivation.tla` gains `DerivedPurge` (replace covered versions by stubs with
empty text; `dh` row kept) and the invariant `NoVictimTextAfterPurge` (no non-stub version whose
segment names a purged victim exists once the tombstone is `purged`); the existing ghost evidence
makes this checkable. `Derivation_PurgeDropsStub.cfg` (versions deleted, no stub) must fail
`FailClosed`. Tests: `TestExpunge_Stages` asserts zero derived rows, vectors, BM25 hits and
blobs for the victim; `TestExport_HiddenOverlay`; `TestDocument_TombstoneView`.

## 4. Durability: committed intents only, floor outside the shard (C-4, C-5, C-16, P-12, P-13; folds C-13, C-22)

**Mechanism (N138).**
1. **The intent is put after the marker commits and before the ack**: marker transaction
   (`deletion_log` row included) → `put` intent → ack. Only the attempt that committed the marker
   writes it, with the marker's exact effect: `{subject, kind, up_to_version | memory_ids,
   deleted_at, operation_id, prev_operation_id}`. A duplicate that finds `state = 'deleting'`
   returns the existing operation and puts nothing. A crash between commit and put leaves a
   committed, unacknowledged marker a restore may lose; the client saw an error and retries. The
   guarantee is exactly N122's "acknowledged deletes and invalidations: RPO 0", now true.
2. **Replay applies intents verbatim**, never recomputing `up_to_version` from the restored state,
   and skips namespace and tenant intents whose catalog row is not `deleting`/`deleted`.
3. **Replay order per subject is a chain**, not a clock: the marker transaction reads the subject's
   last `deletion_log` entry under the document lock and records it as `prev_operation_id`; replay
   applies each subject's intents in chain order (a broken chain starts at its oldest present
   member). Global order across subjects is irrelevant. Intent names keep `deleted_at` for listing
   only, and no NTP bound is needed (C-16, P-13).
4. **The replay floor lives in the catalog** (`catalog.shards.replay_floor`), lowered by `min()` on
   every restore and failover and **never raised** while intents are retained (35 days, the bound on
   replay work; `deletion_log` keeps replay idempotent). `shard_meta.replay_floor` and the "raised
   when a replay completes" text (N134, §5.5.5, §9.3) go.
5. **One intent per namespace for a tenant delete**, written by the `TenantDelete` workflow as it
   fences each namespace (P-12); `DeleteTenant` acks after the catalog `deleting` row and the
   tenant-level intent, fencing namespaces asynchronously with `TENANT_DELETING` as the read barrier
   (A-11). New admin edge `restore_delete` (`frozen/restore → frozen/delete`); §9.3 steps 1 and 9
   exclude namespaces whose catalog state is `deleting` (C-13).
6. **PITR before a move-in** (C-22, P-12): the scratch move ends with a replay of the namespace's
   intents from `cutover_at − margin` before `ready`; `blob gc --reconcile` skips the prefixes of
   namespaces the catalog places on the shard until their recovery moves are `done`; `CleanupMove`
   deletes the moved-out **rows** at 24 h and the source **blob prefix** only after the 28-day
   backup window.

**Why it closes the findings.** C-5: an intent exists only for a committed marker, so no orphan can
be applied to later state, and a replayed intent cannot cover a version retained after it because
its `up_to_version` is the original. C-4: TLC showed the never-raised floor holds at `MaxT = 7`, and a
floor in the catalog cannot be rolled back by a PITR or a stale promotion. C-16/P-13: chain order
is clock-free. P-12: tenant intents are reachable and blobs survive the backup window.

**Rejected.** (a) A separate `.committed` record after the marker: the same guarantee at two puts
per delete; the intent put after commit *is* the commit record. (b) Bounding the replayed effect by
time: one rule per intent kind, and it still applies an orphan `Invalidate` issued after an acked
`Restore`. (c) Raising the floor after a base backup containing the replay's last commit: correct
but couples the floor to pgBackRest; the 35-day window is the simpler bound.

**Spec and tests.** `Durability.tla`: `Issue` splits into `Commit` and `PutIntent` (intent after
commit); `CrashBeforePut` leaves a committed, unacked marker; `Dup` (second attempt on an open
subject) puts nothing; `Retain` revives a subject after its delete; `rp` stays outside the
restorable state and is never raised; replay applies the intent's recorded effect in chain order.
Invariants: `AckedDeleteSurvives` (kept), new `NoUnackedEffectOnLaterAck` (a replay never hides a
version acknowledged after the intent's subject state). `Durability_IntentBeforeCommit.cfg` must
fail `NoUnackedEffectOnLaterAck` (C-5); `Durability_RaiseOnReopen.cfg` must fail
`AckedDeleteSurvives` (C-4; the reviewer's `DurabilityRaise.tla` trace); `Durability_ClockOrder.cfg`
(two clocks, name order) must fail `IntentOrderLastWins`. Tests: `TestIntent_AckImpliesIntent`
(kept), `TestIntent_DuplicateAttemptNoIntent`, `TestRestore_ChainOrder`, `TestRestore_FloorInCatalog`.

## 5. Moves: table classes, insertion sequence, catalog CAS, owner-keyed blobs (C-8, C-9, C-10, P-1; folds C-20, C-23, P-8)

**Mechanism (N139): three table classes, generated from one list.** The class is a `COMMENT ON
TABLE` tag checked by `engramlint sql` and rendered into N113/N124 and the DDL header (C-23):
- **insert-only (re-copy by `ins_seq`)**: `ingest_ledger`, `document_version_chunks`, `chunks`,
  `facts`, `fact_links`, `entity_mentions`, `*_vectors`, `observation_versions`, `observation_inputs`,
  `observation_version_sources`, `*_version_meta`, `page_versions`, `page_version_inputs`,
  `fact_consolidation`, `consolidation_proposals`, `consolidation_applied`, `deletion_log`,
  `curation_log`. Each gains `ins_seq bigint NOT NULL DEFAULT nextval('engram_ins_seq')` and an
  index `(namespace_id, ins_seq)`; the "UUIDv7 id" and "either endpoint" shortcuts are deleted.
  Only the expunge deletes from these tables, and it is paused from `Plan` to `done`.
- **mutable (merge-diff)**: the N124 list plus `idempotency_keys` (its 24 h sweep is a source-side
  delete that merge-diff reproduces) and `observation_sources`/`page_sources`.
- **expiring or derived (excluded, re-derived on the target)**: `token_usage_events` (its 30-day
  sweep joins `purgeable_namespaces`, which pauses during a move; copied once, verified as `count ≤`),
  `vector_indexes`, `namespace_stats`, caches. `consolidation_proposals` leaves this class because
  decision 2 made it truly insert-only.

**Re-copy keyed by the insertion sequence.** `nextval` is assigned at insert, not commit, so
`ins_seq` is commit-ordered only within the 60 s writer lifetime; a shard-local ring `engram_seq_log
(sampled_at, seq)` sampled every minute gives `engram_seq_floor(ts)` = the sample at or before
`ts − 10 min` (the `engram_uuid_v7_floor` analogue on the right key). Reconcile becomes:
1. **Before the freeze** (namespace still active): take `W_pre = nextval()`; run the whole-namespace
   work against rows with `ins_seq < engram_seq_floor(now)`: counts, `bit_xor` hashes, `VerifyFK`,
   and the blob existence checks; one catch-up copy of `ins_seq ≥ engram_seq_floor(T_copy)`.
2. **Under the freeze** (no source write transaction exists): re-copy and verify only rows with
   `ins_seq ≥ engram_seq_floor(T_pre)` per insert-only table, the mutable merge-diff, the relay
   drain (bound 120 s, twice the gap horizon), `VerifyFK` on the target restricted to the same
   range. The freeze formula is restated as (rows inserted since `T_pre − 10 min`) + (mutable rows)
   and measured in M1.5 against a namespace with a consolidation backlog and `APPEND` re-embeds.

**Catalog CAS is the point of no return (C-10).** Cutover order: (a) `cutover` recorded; (b′) target
`ready`; **(a″) catalog CAS `namespace_moves.state: cutover → committed`** (`WHERE move_id AND state
= 'cutover'`) — the point of no return; (c) source `frozen/move → moved_out`, executed only after
the mover read `committed` and re-checked its timeline; (b″) target `active`; (d) catalog
`namespaces` flip `WHERE epoch = e AND state = 'frozen'`, with "already `(target, e + 1)`" as the
only idempotent success. The restore/failover reconcile does its own CAS `cutover → rolled_back`;
if it reads `committed` it completes (c), (b″), (d) itself (`reconcile_out`). Exactly one side wins
and the loser stops. §5.5.5 step 2 bumps the epoch only for namespaces whose open move is not
`committed`; `moved_out_at` becomes informational.

**Blobs are owner-keyed (C-9).** Raw bodies live at `{ns}/ledger/{ledger_id}` and version bodies at
`{ns}/ver/{document_id}/v{n}`; each blob has exactly one owner row and dies with it in the purge,
with no reference check and no race. Identical bodies of one document already create no new version
(same `content_hash`), so the dedup lost is only cross-document, which is paid in storage.
`xcache`/`ecache`/`staging` stay content-addressed with the 24 h grace: they are caches, and a
loss is a recompute. The move's blob check is now "one key per copied row".

**Also (C-20, P-8).** `RetainBackfill` parents get an `operations` row so `Drain` sees them;
`thaw_move` is allowed while the target is unreachable (a `ready` row accepts nothing; `unready_target`
runs at next contact). `vector_indexes` becomes a `requested → building → ready | failed` machine
with a lease; every build checks `pg_index.indisvalid` and drops an invalid index first (never
`IF NOT EXISTS`); `rollback_target` drops the target's indexes by their `engram_hnsw_ddl` names.

**Why it closes the findings.** C-8/P-1: rows inserted for old ids (stamps, re-embeds, new versions
of old observations) carry a fresh `ins_seq` and are caught by the re-copy; source-side deletes are
confined to the merge-diff and excluded classes, so counts match on an active namespace; the freeze
carries no whole-namespace scan or 100 k `HEAD`s. C-10: a failover between (c) and (d) reads
`committed` and finishes the move, one before (a″) rolls it back; no state leaves P2 `moved_out`
against an emptied target or bumps the catalog past a pending (d). C-9: no live row can reference a
deleted blob.

**Rejected.** (a) Reference-counting content-addressed blobs: a reverse index plus check-at-delete
plus an adoption race (a writer that skips the put because the key exists). (b) `created_at` as the
re-copy key: not commit-ordered either and needs the same indexes. (c) Stamping `moved_out_at`
inside (c): (c) runs on the source; the arbiter must be where the restore reads it, in the catalog.

**Spec and tests.** `ShardMove.tla`: source rows gain `SrcDelete` for mutable/expiring rows and
`InsertOldId` (a row whose entity id predates the cursor); re-copy keyed by `seq ≥ floor` with a
bounded `Lag` between assignment and commit; `Commit` (catalog CAS) as a step distinct from `Cut`;
recovery `Abort` CAS; `Cleanup` modelled so a restored target is not refilled after it. Invariants:
`NoLossNoDup`, `OneOwner` (kept), `CatalogNamesOwnerAfterDone`, liveness `MoveTerminatesActive`
under weak fairness with active writers. `ShardMove_ActiveWriters.cfg` must pass (fails today);
`ShardMove_StampAfterCut.cfg` (arbiter stamped after (c)) must fail `CatalogNamesOwnerAfterDone`;
`ShardMove_IdKeyedRecopy.cfg` must fail `MoveTerminatesActive`. Tests: `TestMove_ActiveBacklog`,
`TestMove_FailoverBetweenCAndD`, `TestBlob_OwnerKeyed`.

## 6. Postgres budgets and plans (A-1, P-3, P-4, P-5, P-6, P-7, P-11)

**Mechanism (N140): a selectivity-aware semantic plan, chosen in Go.**
1. **Estimate before the arm runs.** Tags and metadata filters resolve to `$allowed_docs`;
   eligible rows = Σ `document_versions.fact_count` over the current versions of those documents
   (`fact_count` is written once by `FinalizeVersion`). `as_of` uses a per-namespace monthly
   `mentioned_at` histogram refreshed by the stats sweeper. `fact_type` is copied onto
   `fact_vectors` (immutable) so the type filter runs inside the scan instead of after the Top-K.
2. **Below θ eligible rows**: an **exact path** driven from `facts_doc_idx`/`facts_mentioned_idx`,
   joined to the vector table by PK, ordered by distance, `LIMIT cap`. **Above θ**: the HNSW
   iterative scan with an explicit `hnsw.max_scan_tuples = min(4 × ef_search / s, 100 k)` and a
   `RecallStats.partial` flag when the scan is exhausted. θ starts at 5 k and is fixed by M0.6's
   selectivity sweep (tags 100/20/5/2/1 %, `as_of` deciles, p95 per arm on the real image).
3. **The arm transaction forbids the partition scan**: `SET LOCAL enable_seqscan = off,
   max_parallel_workers_per_gather = 0`. Chunk and observation arms follow the same plan.
4. **Index hygiene has an owner and a measurable trigger (P-4, P-7, P-9).** Index DDL is issued
   only by the **index runner**, an `engramctl index` daemon on the control host holding
   `engram_migrate`, fed by `vector_indexes` requests (decision 5's state machine). The purge
   increments `vector_indexes.purged_since_build` (one writer, the expunge's own bookkeeping);
   hygiene rebuilds a touched HNSW at **1 % of `rows_at_build` or 2 k elements**, whichever first,
   then runs `VACUUM (INDEX_CLEANUP ON)`. Vector partitions carry `vacuum_index_cleanup = off`
   permanently, so autovacuum never repairs a graph. `pgstattuple`, `HNSWDeadFraction` and
   "20 k rows/s" are deleted; hygiene is budgeted at ≈ 0.35 ms per vector. Builds are serialised per
   shard with `maintenance_work_mem = 2.4 KB × vectors` (≤ 5 GB) and `shm_size = 8g`; §9.2's build
   table is restated through 2 M vectors.
5. **Purge WAL is budgeted (P-6).** `PurgeBatch`, `PurgeRows`, `DerivedPurge` and move cleanup
   measure the `pg_current_wal_insert_lsn` delta per batch and pace to **≤ 25 MB/s per shard**, with
   at most two expunges active per shard (a shard-wide advisory slot); `wal_compression = zstd` in
   `gucs.yaml`; RTO is stated as a function of WAL since the last backup; a differential backup is
   taken after every namespace delete and every move; `archive-push-queue-max` and archive
   throughput enter §9.1.
6. **The honest numbers change the shard size (P-5, P-11, A-16).** Measured: ≈ 10 k page touches
   per MID recall, 2,048 B per vector row and HNSW element, and a hot set that must include the
   links reverse index: ≈ 100 GB at 10 M facts against ≈ 80 GB of usable cache, and a hard cap that
   does not fit the volume. Decision: **shard target 8 M live facts, hard cap 12 M, 600 GB volume,
   local NVMe ≥ 50 k IOPS**; hot set ≈ 80 GB at target including the reverse index and the `ins_seq`
   indexes (≈ 5 %); D3's fleet count is re-derived (+25 % shards per cell). Capacity signals use
   relation bytes per namespace (hidden and unpurged rows included), not `live_facts`;
   `ShardNearCapacity` pages at 70 % of the volume. N114's IOPS table is re-derived in M0.6 from
   measured touches with the real arm SQL, the graph arm, the exact path, and rows for 10 % and 1 %
   tag filters and an old `as_of`.

**Why it closes the findings.** A-1/P-3: a 1 % tag filter on a 50 k namespace runs the exact path
over ≈ 500 rows instead of a 50 k-tuple graph walk or a 1.3 GB partition scan, and the arm returns
`min(cap, |visible|)` or says it did not; the planner can no longer pick the seq scan. P-4: the
rebuild happens before any repair, with a trigger the purge itself measures. P-6: a 100 k-fact
delete is a bounded, paced WAL stream inside the RTO term. P-5/P-11: the IOPS, cache and volume
budgets are stated from measurements and fit the new shard.

**Rejected.** (a) 10 M facts in a 128 GB container: the hot set does not fit at the stated miss
rate. (b) Post-Top-K filtering for `fact_type` and metadata: A-1's truncation. (c) A `(namespace_id,
document_id)` index on every vector table: the exact path driven from the `facts` indexes needs
none (≈ 3 GB saved per 10 M facts). (d) Fixed-pause purge pacing.

**Checks.** M0.6 exit: the selectivity sweep, the measured IOPS table and `TestRecall_FilteredArm`
(property 6 of §8.2 against `PostgresIndex` on the real image). `Storage.tla` adds `Purge` counting
into `dead[idx]` and the invariant `RebuildBeforeRepair` (no vacuum of a vector index while
`dead > threshold`); `Storage_AutoRepair.cfg` must fail it.

## 7. Remaining majors and the minors to fold in

Decisions 1–6 cover every blocker and major. The minors below fold in without changing a mechanism.

| Finding | Fold |
|---|---|
| C-12 | The `documents` row is deleted at the end of a purge only when no open tombstone with a higher `up_to_version` exists and no `document_versions` row remains. |
| C-17 | `DeleteDocument` compares `expected_version` inside the marker transaction, under the document lock. |
| C-19, P-9 | Ids are minted per attempt inside the transaction; `engram_admin` and `engram_move` get the 30 s statement and idle timeouts (raised per session only in `engramctl`); the consolidation watermark guard uses the relay's last seen `seq`, not an id timestamp; admin and move DSNs and pool sizes are listed in §9.1; `engram_entity_fuzzy` is revoked from `engram_relay` and `engram_move`. |
| P-10 | `WriteFiles` reads short `READ COMMITTED` ranges bounded by the `ins_seq` watermark taken at `BeginSnapshot`, marker sets read once; the `RecordSnapshot` re-check covers deletes. The index runner refuses to start a build while any `backend_xmin` is older than 5 min. |
| P-14, P-15 | One exclusive document-lock timeout (35 s single attempt) generated from the GUC table; the marker transaction stores the delete event's `seq` in the tombstone for `engram_consumers_passed`. |
| A-8 | SQL `CHECK` lists are generated from the proto enums; one marker transaction (§5.4.1's) with `kind = DELETE_DOCUMENT`; `superseded_by` is the version number in both places. |
| A-9 | `buf breaking` baselines on `main@HEAD~1` until `v1.0.0`, with a labelled exception per intended break; `DOCUMENT_PURGING` joins the table; old names reserved, bytes fields renamed `*_bytes`. |
| A-10 | One code per detail type, generated into §4.1.6 from `errs`; the delete freeze is `NAMESPACE_DELETING` everywhere; `invalidated_at` ignores `reextract` rows; `RESTORING` rejects reads. |
| A-11 | Delete-class RPCs and `Restore` get a 40 s deadline cap; "Synchronous" leaves `Restore`; `DeleteTenant` per decision 4. |
| A-12 | A new `operation_id` for every retry; NG38 and §4.1.3 say so. |
| A-13 | Metadata filters resolve to `$allowed_docs` like tags, with a GIN `jsonb_path_ops` index on `documents.metadata`. |
| A-14 | `page_versions.text` with a BM25 index and `page_version_vectors` under the N111/N112 rules, so pages share the visibility and purge path. |
| A-15, A-16, A-17 | `rerank_min_remaining`, the §2.5/§4.1.2 pool numbers, the vector sizes, Reflect typical cost ($0.114) and the throughput formula `min(N × 32 / L, RPM_cap / (60 × calls_per_chunk))` are generated from the register constants by `make gen-docs`; R4 is stated per cell against 2.9 chunks/s; M2.1 and M1.1 name their gateway profile. |
| A-18 | M0.7 re-scoped to conformance and trace validation, M0.8 to repository migrations; front matter and README follow §7.1; the Lake skeleton with `SORRY_BASELINE = 3` is added. |
| A-22, A-23, A-25 | `search_pages` becomes the first forced Reflect step once pages exist; `GetDocumentBody` is added; `ListTags` and `UpdateDocumentTags` are added; `tenant.admin` is tenant-bound and `engram.operator` fleet-wide with its own audience; the register drift items are corrected. |

## Register rows to rewrite or add

**Rewrite:** N113 (class list generated; `ins_seq`; evidence tables have no FK to facts), N114
(measured IOPS table, 8 M/12 M shard, 600 GB, NVMe), N115 (`fact_hidden` keyed by cause; marker
transaction clears document metadata; intent after commit), N116 (invalidation set by anti-join;
arrays only for `chunk_tomb`/`doc_tomb`; DocumentService and the export overlay), N117 (fail-closed
predicate; served-at-T rule; zero-source rule), N119 (`DerivedPurge`, `REEXTRACTED_FACTS`, WAL pacing,
hygiene trigger and owner, transcripts), N120 (the derivation commit rule applies to every writer),
N121 (proposal attempts, `base_version`), N122 (intent after commit, chain order, exact effect,
per-namespace tenant intents), N123 (catalog CAS as arbiter; replay after a scratch move), N124
(three classes; pre-freeze verification; freeze formula), N125 (step (a″)), N126 (curation overlay;
system snapshots), N133(b)(e) (Restore lock kept; index runner holds `engram_migrate`), N134 (floor
in the catalog, never raised), N104 (owner-keyed blobs), N58 (re-extraction is a write), D3, D16.
**Add:** N135–N140 as numbered above, plus the A-minor folds of §7 as N141.

## TLA+ changes

| Spec | Actions added | Invariants | Must pass | Must fail |
|---|---|---|---|---|
| `Derivation` | `Replace`, `ChunkPurge`, `ReextractPurge`, `Reextract`, `DerivedPurge` (stubs); two writers; persisted proposal with `base`; page writer with its own `Verify`; `Materialize` re-reads per batch | `EvidenceOutlivesFacts`, `BaseCurrentAtCommit`, `NoLostRebuild`, `NoVictimTextAfterPurge`, `FailClosed`, `ReextractKeepsDerivedVisible`; `Served` = SQL rule; `MatInvalid = TRUE` with causes is the design | `Derivation.cfg`, `_Page.cfg` | `_CascadeEvidence`, `_ReextractHides`, `_PageNoVerify`, `_StaleProposal`, `_MatOnce`, `_PurgeDropsStub`, `_RestoreNoLock` (now fails, as the reviewer's run showed) |
| `Durability` | `Commit`/`PutIntent` split, `CrashBeforePut`, `Dup`, `Retain`; chain-ordered replay of recorded effects; `rp` outside restorable state, never raised | `AckedDeleteSurvives`, `NoUnackedEffectOnLaterAck`, `IntentOrderLastWins` | `Durability.cfg` at `MaxT = 7` | `_IntentBeforeCommit`, `_RaiseOnReopen`, `_ClockOrder` |
| `ShardMove` | `SrcDelete`, `InsertOldId`, seq-keyed re-copy with `Lag`, catalog `Commit` CAS before `Cut`, recovery `Abort` CAS, `Cleanup` | `NoLossNoDup`, `OneOwner`, `CatalogNamesOwnerAfterDone`, `MoveTerminatesActive` | `ShardMove.cfg`, `_ActiveWriters` | `_StampAfterCut`, `_IdKeyedRecopy` |
| `Storage` | `Purge` counts `dead[idx]` | `RebuildBeforeRepair` | `Storage.cfg` | `_AutoRepair` |

§7.1 gains a column "prose mechanisms this spec omits" (C-21); every §7 number is re-copied from a fresh log.

## Go API fixes for the A-minors

- **Parallel arms, not one transaction (A-5).** `store.ReadNamespace` yields a `ReadSession`;
  `Session.Tx(ctx)` opens one short read transaction per arm with the scope GUCs, the read fence
  and the arm `SET LOCAL`s of decision 6. Marker sets, `$allowed_docs` and the plan choice are
  values computed once and passed in. `Arm.Run(ctx, s store.ReadSession, q Query)` and
  `index.Searcher` take the session; §2.2.7 states that a recall is N read transactions.
- **Import cycles (A-6).** `outbox.Writer` moves into `store` as a leaf interface; `outbox.Relay`
  takes a `store.Store`; `router` takes `id.Scope` and never imports `authz`; `quota.Meter.Record`
  takes `store.Tx` through a leaf `txn` package. Result: `store → outbox` and `router → authz →
  quota → store` are acyclic. `api.Deps` is completed from the proto service list (Reflect agent,
  `catalog.Namespaces/Registry/Moves`, `config.Resolver`, `pages.Writer`, `export.Builder`, a
  per-shard `store.Stores`), with a generated check that every RPC has a dependency path.
- **`workflows.Client`** splits into `Starter`, `Signaller`, `Waiter` (≤ 5 methods each) and covers
  `ExportSnapshot`, `PageRefresh`, `Expunge`, `TenantDelete`, `Move`. `export.Streamer.Stream` takes
  `path`/`offset`; `Deleter.DeleteNamespace` keeps the client `operation_id`.
- **Idiom (A-24).** `internal/reflect` → `internal/reflectagent`; `id` keeps `github.com/google/uuid`
  as the one allowed leaf dependency (stated); typed ids for citations, intents, workflow ids, cells
  and `errs.NotFound(kind memoryv1.ResourceKind, id)`; `id.DocVersion`/`ObsVersion`/`PageVersion`;
  the recall planner is named a DAG scheduler, the expunge a workflow. `go vet` against a stub
  module of the §2 signatures is an M0.1 exit.

## Disposition

Blockers C-1, C-2, C-3 → decisions 1, 2, 2. Majors C-4…C-11 → 4, 4, 1, 2, 5, 5, 5, 2; P-1…P-6 →
5, 1, 6, 6, 6, 6; A-1…A-4 → 6, 3, 3, 3. Minors and nits → §7, the section naming them, or the Go API
list. The round-3 principle is unchanged; each change is local to a writer, a purge target, a key or
a budget, and each has a configuration that must fail without it.

## Disposition (all 63 findings)

Every finding maps to a register row (`00-decision-register.md`; D23 holds N135 to N141, rewritten rows keep their ids and carry "(rev. D23)"). The advice's proposed ids became: N135 evidence (N135), N136 commit rule (rewritten N120, N121), N137 derived purge (N136), N138 durability (rewritten N122, N123, N134), N139 moves (rewritten N124, N125, N104, plus N137 for the table classes), N140 plan (N138; shard size in N114, WAL pacing in N119), N141 minor folds (N139); N140 and N141 of the register are new (code API; specs and schedule). Nothing is rejected outright; where a recommended alternative was not taken, the Note says why. Two points go beyond the advice: a duplicate delete attempt re-puts the committed marker's own intent idempotently (a retry after a crash between commit and put would otherwise be acknowledged with no intent), and M0.4's Temporal gate is raised to the peak that §9 sizes for.

**Judgement calls.** Shard size: 8 M target / 12 M cap / 600 GB / NVMe ≥ 50 k IOPS adopted (N114; the advice offers no better option, and 10 M in a 128 GB container is rejected because the hot set exceeds usable cache). Owner-keyed blobs: adopted, cross-document dedup of bodies is lost and paid in storage (N104). Intent after commit: an acknowledged delete or invalidation survives restore and failover; a committed-but-unacknowledged one may be lost, and the client's retry re-applies it (N122, D16).

| Finding | Disposition | Register id | Note |
|---|---|---|---|
| C-1 | Accepted | N135, N113, N42 | Evidence tables lose the FK to `facts`; evidence dies with its version row. |
| C-2 | Accepted | N120 | `CommitPageVersion` re-verifies under the shared lock against all open tombstones. |
| C-3 | Accepted | N120, N121 | `base_version` recorded and checked; otherwise discard and re-route. Inheriting `root(base)` rejected: a visible but superseded base still overwrites a rebuild. |
| C-4 | Accepted | N134, N122 | Floor never raised, held in the catalog. Raise-after-backup rejected: couples the floor to pgBackRest. |
| C-5 | Accepted, modified | N122 | Intent put after the marker commit. Duplicate attempts re-put the committed marker's own intent (put-if-absent) instead of putting nothing. Separate `.committed` record rejected: two puts per delete. |
| C-6 | Accepted | N135, N58, N119 | Re-extraction is a write; derived predicates read `invalidate` only; `REEXTRACTED_FACTS` purge after 1 h. |
| C-7 | Accepted | N120, N133, N141 | Cause-tagged rows are the design; the `Restore` lock is load-bearing; `_RestoreNoLock` must fail. |
| C-8 | Accepted | N137, N124, N113 | Three classes; `ins_seq` re-copy key; `idempotency_keys` and `observation_sources` move to merge-diff. |
| C-9 | Accepted | N104 | Owner-keyed blobs. Reference counting rejected: reverse index plus adoption race. |
| C-10 | Accepted | N125, N123 | Catalog CAS `cutover -> committed` is the point of no return; restore CASes `cutover -> rolled_back`. |
| C-11 | Accepted | N121, N43 | Proposals keyed `(batch_key, attempt)`; no `DELETE`; batch `applied` state; all-skip batches stamp. |
| C-12 | Accepted | N119 | `documents` row deleted only when no higher open tombstone and no version row remains. |
| C-13 | Accepted | N123, N101, N122 | `restore_delete` edge; `deleting` namespaces excluded from the restore catalog flips. |
| C-14 | Accepted | N117, N135 | SQL rule wins: version current at `T`; nothing served when hidden; retirement filtered by its own time. |
| C-15 | Accepted | N135, N115 | `fact_hidden` keyed by cause; `Restore` deletes only `invalidate`. |
| C-16 | Accepted | N122 | Per-subject `prev_operation_id` chain replaces clock order; no NTP bound needed. |
| C-17 | Accepted | N115 | `expected_version` compared inside the marker transaction. |
| C-18 | Accepted | N117, N135 | Predicates fail closed; DerivedPurge keeps stubs so the row always exists. |
| C-19 | Accepted | N139, N133, N95 | Ids minted per attempt; 30 s timeouts on admin and move; watermark guard uses the relay's last `seq`. |
| C-20 | Accepted | N124, N125 | `RetainBackfill` parents get an `operations` row; drain bound 120 s; `thaw_move` while the target is unreachable. |
| C-21 | Accepted | N141 | Missing actions added to each spec; §7.1 states what each spec omits. |
| C-22 | Accepted | N123 | Scratch move ends with an intent replay before `ready`. |
| C-23 | Accepted | N113, N137 | Class list generated from `COMMENT ON TABLE` tags; `*_version_meta` are write-once inserts. |
| P-1 | Accepted | N137, N124 | Same fix as C-8; whole-namespace checks move before the freeze; freeze formula restated. |
| P-2 | Accepted | N135, N119, N116 | `REEXTRACTED_FACTS` purge; `fact_hidden(invalidate)` tested by anti-join, never an array. `chunk_extraction` join rejected: second currency mechanism. |
| P-3 | Accepted | N138 | Exact path below θ, HNSW with `max_scan_tuples` above; seq scan forbidden in arm transactions. |
| P-4 | Accepted | N138, N112, N119 | `purged_since_build` counter; rebuild at 1 % or 2 k; `vacuum_index_cleanup = off`. |
| P-5 | Accepted | N114, D3 | Measured IOPS and hot set; NVMe ≥ 50 k IOPS; M0.6 re-derives the table. |
| P-6 | Accepted | N119 | Purge paced to 25 MB/s of WAL; `wal_compression = zstd`; differential backup after deletes and moves. |
| P-7 | Accepted | N138 | `maintenance_work_mem` per build, serialised builds, `shm_size = 8g`. |
| P-8 | Accepted | N138 | `requested -> building -> ready` or `failed` with lease; invalid indexes dropped first; rollback drops by name. |
| P-9 | Accepted | N133, N138 | Index runner holds `engram_migrate`; admin and move timeouts and pools; `engram_entity_fuzzy` revoked from relay and move. |
| P-10 | Accepted | N126, N138 | Short `READ COMMITTED` ranges bounded by the `ins_seq` watermark; runner refuses builds under an old `backend_xmin`. |
| P-11 | Accepted | N114, D3 | 12 M hard cap on a 600 GB volume; signals from relation bytes. |
| P-12 | Accepted | N123, N122 | Blob gc skips namespaces mid-recovery; source blob prefix kept 28 days; one intent per namespace for a tenant delete. |
| P-13 | Accepted | N122 | Same chain order as C-16. |
| P-14 | Accepted | N139 | One exclusive document-lock timeout generated from the GUC table. |
| P-15 | Accepted | N115 | The tombstone stores the delete event's `seq`. |
| A-1 | Accepted | N138, N114 | Selectivity-aware plan; `fact_type` copied onto `fact_vectors`; post-Top-K filtering rejected (truncation). |
| A-2 | Accepted | N136, D16 | `DerivedPurge` reduces covered versions to content-free stubs; transcripts purged. Outright deletion rejected: breaks the D9 range rule. |
| A-3 | Accepted | N136, N115 | Marker clears content columns; DocumentService returns a tombstone view. |
| A-4 | Accepted (option b) | N126, D16 | `hidden_overlay` in the manifest; "every part applies the predicate" deleted. Expiring on every `Invalidate` rejected: full re-sync per curation click. |
| A-5 | Accepted | N140 | `ReadSession` with one short read transaction per arm. |
| A-6 | Accepted | N140, N133 | Cycles broken (`outbox.Writer` in `store`, `internal/txn`, `router` free of `authz`); `Deps` completed; `workflows.Client` split. |
| A-7 | Accepted | N136 | Per-kind workflow mapping; `DELETE_*` non-cancellable. |
| A-8 | Accepted | N139 | CHECK lists generated from proto enums; one marker transaction. |
| A-9 | Accepted | N139 | Baseline `main@HEAD~1` until `v1.0.0`; names reserved. |
| A-10 | Accepted | N139 | One code per detail type, generated; `NAMESPACE_DELETING` for the delete freeze. |
| A-11 | Accepted | N139, N122, N11 | 40 s caps for delete-class RPCs and `Restore`; `DeleteTenant` acks after catalog `deleting` plus the tenant intent. |
| A-12 | Accepted | N139 | A new `operation_id` per retry. `RetryOperation` RPC not added. |
| A-13 | Accepted | N139, N116 | Metadata filters resolve into `$allowed_docs` with a GIN index. |
| A-14 | Accepted | N139, D12 | `page_versions.text` with BM25 and `page_version_vectors`. |
| A-15 | Accepted | N139 | Constants generated by `make gen-docs`. |
| A-16 | Accepted | N139, D3, N114, N130 | Sizes, Reflect $0.114, week 23 corrected; M0.4 gate covers the stated peak or §9 states the lower tested one. |
| A-17 | Accepted | N139, D3 | Throughput formula with `N_workers`; gates name their gateway profile. |
| A-18 | Accepted | N141 | M0.7 and M0.8 re-scoped; front matter and README follow §7.1; Lake skeleton added. |
| A-19 | Accepted | N135, N117 | Zero-source versions not served; the empty-source retire is exempt from `Reserve`. |
| A-20 | Accepted | N135, N115 | Same fix as C-15. |
| A-21 | Accepted | N126, N119 | Debounced system `ExportSnapshot` after a delete expires the latest snapshot. |
| A-22 | Accepted | N139, D12 | `search_pages` first forced step once pages exist; `GetDocumentBody`, `ListTags`, `UpdateDocumentTags` added. |
| A-23 | Accepted | D13, N139 | `tenant.admin` tenant-bound; `engram.operator` fleet-wide with its own audience. |
| A-24 | Accepted | N140, N133 | `reflectagent`, typed ids, `uuid` stated as the leaf dependency. |
| A-25 | Accepted | N139, D2, D12, N116 | Register drift corrected in place; README still cites D22 only and is outside this edit. |
