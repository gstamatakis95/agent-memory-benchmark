# Review round 4: correctness and concurrency (C-1 to C-23)

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
