# Review round 5: correctness and concurrency (C-1 to C-14)

Lens: the mechanisms round 4 introduced or rewrote (N135 to N143 and the rows they revised: N104, N115 to N126,
N133, N134). I checked the prose (§3, §5, §7, §9, register D22 and D23) against `sql/shard_schema.sql`,
`sql/catalog_schema.sql` and `formal/tla/*.tla`, built interleavings, and ran the following:

- **SQL.** Both schemas applied to my own scratch databases (`r5c_shard`, `r5c_cat`; PG 16, pgvector, pg_search
  stubbed). A two-transaction retry of `CommitPageVersion` ran against `engram_derivation_base_cas`
  (`scratchpad/r5c/page_retry.sql`). I also inspected the catalog of foreign keys, the column sets and the `CHECK`s.
- **TLC.** `scratchpad/r5c/DerivCasc.tla` is `Derivation.tla` plus one knob, `CascadeHidden` (`ChunkPurge` and
  `ReextractPurge` also drop the fact's `fact_hidden` row, which is what the DDL's `ON DELETE CASCADE` does), and
  one ghost invariant, `NoGhostInvalidatedServed`. With the knob set to `FALSE`, TLC passes (496 distinct states).
  With it set to `TRUE`, TLC finds a 9-state counterexample (C-2).
- **Reading the specs against the prose.** Six findings below (C-2, C-3, C-4, C-5, C-6, C-7) hold because a spec
  models something more favourable than the prose or SQL actually specify. None of these omissions appears in the
  §7.1 column "prose mechanisms this spec omits".

Findings already dispositioned in rounds 1 to 4 are not repeated. A finding is marked [regression: N…] when it was
caused by a round-4 change.

## Findings (ranked)

### C-1: major [regression: N143(1), N120]: `CommitPageVersion` takes the base compare-and-set before its idempotency check. A retried commit deletes the markdown of the version it already committed. A retried full rebuild advances `current_version` to a version that does not exist.

**Where.** §5.0 rule step 3: "as the first write of the commit transaction, before any version row is inserted".
§5.3.2 step 6: "`UPDATE pages SET current_version = n + 1 WHERE … AND current_version = base_version` … `page_full/v1`
has no base … Any failure → `ROLLBACK`, delete the blob this activity put … re-run `FullRebuild` … Otherwise: `INSERT
page_versions(…) ON CONFLICT DO NOTHING` (zero rows → a previous attempt committed: read it and return)". §5.3.5:
"Crash after the tx, before the result is recorded | Retry hits `ON CONFLICT DO NOTHING` … | one version".
`engram_derivation_base_cas`: "A root rebuild has no base (p_base IS NULL): it advances the row it holds".

**Evidence (executed, `page_retry.sql`).**
1. Page at v1. Attempt 1 (delta, base 1): the CAS returns 2, v2 is inserted, and the transaction commits.
2. Attempt 2 is the Temporal retry of the same activity. It happens after a worker crash between COMMIT and result
   recording, or after a `StartToClose` timeout whose zombie attempt committed. The CAS returns NULL, which the rule
   reads as "lost". The rule then rolls back, deletes the blob "this activity put" (`pages/{id}/v2.md`, the
   markdown of the committed v2) and runs a new LLM full rebuild.
3. The same retry for a `page_full/v1` commit gets 3 from the CAS. The `INSERT … version = 2 … ON CONFLICT DO
   NOTHING` inserts 0 rows, and the prose says to "return" (commit). The page row is then at
   `current_version = 3`, but only versions 1 and 2 exist.
4. `engram_page_version_hidden(…, 3, '{}')` returns `t`. The page fails closed: `GetPage` returns `PAGE_HIDDEN`
   until another refresh lands, and that refresh starts from a phantom base.

The idempotent path in §5.3.5 therefore cannot be reached. `ApplyBatch` is not affected, because its
`consolidation_batches.state = 'applied'` and `op_key` checks (steps 2 and 3) come before the CAS.
`Derivation.tla` cannot see this bug: `Commit` is atomic and is never retried.

**Recommendation.** The commit transaction must first decide whether this attempt already committed. Two options:
`SELECT 1 FROM page_versions WHERE page_id AND version = n + 1 AND evidence_hash = $h`, or a per-attempt
`page_commit_key` with the same effect. Only if the answer is no does it run the CAS. The CAS should be
`current_version = $expected` for root rebuilds too, where `$expected` is the version the rebuild read at
`LoadPage`, so that a page has no blind write. The "delete the blob" branch must never delete a key that a committed
row names. Add a retry to the conformance tests (`TestPageRefresh_CommitRetry`) and give `Derivation.tla` a
`RetryCommit` action.

### C-2: major [regression: N135(2), N119 `REEXTRACTED_FACTS`]: an acknowledged `Invalidate` is erased by the 1 h chunk and re-extraction purges, and the derived text it hid becomes visible again

**Where.** DDL `fact_hidden`: "FOREIGN KEY (namespace_id, memory_id) REFERENCES facts … ON DELETE CASCADE … The
'invalidate' set is permanent by design". N116: "`fact_hidden(invalidate)` is permanent by design". N119:
`CHUNK_TOMBSTONES` (1 h grace) and `REEXTRACTED_FACTS` delete the retired facts ("rows, vectors, links, mentions and
the marker"). The derived predicates test `fact_hidden … cause = 'invalidate'` on `observation_inputs.fact_id`, and
evidence outlives the fact (N135(1)).

**Interleaving (TLC, `DerivCasc.tla` with `CascadeHidden = TRUE`, 9 states).**
1. Ingest f1. A writer commits o1@v1 from f1.
2. `Invalidate(f1)`. o1@v1 is hidden by the predicate.
3. `Replace` retires f1's chunk.
4. `ChunkPurge(f1)` runs before Materialize has written `derived_hidden(invalidation, f1)`. The cascade deletes the
   `fact_hidden(invalidate)` row.
5. o1@v1, whose text was written from f1, is served again (`NoGhostInvalidatedServed` violated).

With `CascadeHidden = FALSE`, which is the spec as shipped (`hidden` survives `gone`), TLC passes. The spec keeps a
marker that the DDL deletes.

**Why the window is real.**
- Invalidations have no persistent "pending" state. The expunge sweeper finds only `document_tombstones` rows, so
  nothing records that a Materialize is still owed for an invalidation.
- The Materialize for an `Invalidate` depends on the `SignalWithStart` after the ack. That signal is lost if the API
  dies first. It is also lost when a move drains and terminates the expunge singleton and restarts it only for
  pending markers.
- The expunge is paused for the whole move.
- `Invalidate` accepts a fact that is already chunk-tombstoned or `reextract`-hidden, whose purge may be seconds
  away (§5.4.5 rejects only non-facts and unknown ids).

**Further effects.**
- Invalidating an old-key fact after its re-extraction twin exists does not hide the twin. `curation_log` is
  re-applied only at a later `CommitChunk`. The user's acknowledged curation therefore has no effect on what is
  served, and the purge then removes even the record of it.
- After the purge, a `Restore` of that fact is impossible (`NOT_INVALIDATED`), so any `derived_hidden(invalidation)`
  rows that do exist become permanent. `RestoreExact` silently stops holding.

**Recommendation.**
- Make the invalidation independent of the fact row: drop the FK cascade on `fact_hidden` for cause `invalidate`, or
  move it to a table keyed by `(memory_id)` with no FK that dies only with the namespace or a document tombstone of
  its own version.
- Alternatively, make the purge of a retired fact wait until no `fact_hidden(invalidate)` row exists without its
  `derived_hidden` rows. That needs a durable `materialized_at` on the invalidation, which also gives the sweeper
  something to find.
- Resolve `Invalidate` of a retired or re-extracted fact to its visible twin by `content_hash`, or reject it with
  `NOT_FOUND`.
- Add `CascadeHidden` to `Derivation.tla` as a must-fail configuration.

### C-3: major: the catalog is the move arbiter and the holder of the replay floor, but it is replicated asynchronously; a catalog failover can lose `committed`, the lowered floor, `deleting` and the epoch bumps

**Where.**
- N125 and N123: "(a″) catalog CAS … the point of no return"; restore reconciles by that CAS.
- N134: "The floor lives in the catalog … so neither a PITR nor a stale promotion can lose it … never raised".
- §5.4.3: "steps 2 and 3 together are the marker" (catalog `deleting`). Replay "skip[s] namespace and tenant
  intents whose catalog row is not `deleting` or `deleted`".
- §9.3 step 4: epoch "written to the catalog first".
- §9.1 and §9.6: "`catalog-postgres` + replica + failover agent"; "the agent promotes after 30 s unreachable and
  flips the alias"; a backup restore is RPO 60 s. No synchronous standby is specified for the catalog (N122 rejects
  one for shards).

**Interleavings.**
- **Floor.** Shard S is restored to T and the catalog commits `replay_floor = T`. The replay finishes. Within the
  replica lag, the catalog primary dies and the replica, which still holds the old floor, is promoted. The next
  restore of S (or a failover that loses the replay's own local commits) replays from the old floor, so the
  acknowledged deletes that only the first replay re-applied are lost. This is exactly `Durability_RaiseOnReopen`,
  reached through the catalog instead of through `Reopen`.
- **Arbiter.** The mover's CAS `cutover → committed` commits and (c) and (b″) run, so the target is active and
  accepting writes. The catalog then fails over and loses `committed`. A later restore or failover reconcile of
  either shard CASes `cutover → rolled_back` and "rolls back from the target side": it deletes the target rows,
  including writes acknowledged on the target, while the source is `moved_out`.
- **Delete.** A namespace delete is acknowledged and the catalog loses `deleting`. A shard restore then skips the
  namespace's intent and brings the namespace back `active` through `restore_done`.
- **Epoch.** A lost epoch bump makes the next "catalog epoch + 1" equal to an epoch the shard already holds, so the
  `greater` rule refuses it or two lives share an epoch.

`Durability.tla` and `ShardMove.tla` model `fl` and `cm` as never lost. §7.1 does not list "the catalog is
lossless" as an assumption.

**Recommendation.** Make the catalog RPO 0 for the rows the protocols depend on. The catalog is small, so a
synchronous standby (`synchronous_commit = remote_apply`) is cheap there, unlike on shards. Every promotion and
catalog restore must then run `move reconcile`, which repairs the catalog from the shards (the shards' ownership rows
are the source of truth, per the §9.6 backup path). The floor should also be derivable from durable state: the
minimum of the `pre-restore` volume timestamps or a blob-store record written before the replay, never only a catalog
row. State the assumption in §7.1 and add a catalog-loss action to both specs.

### C-4: major [regression: N137, N126]: `ins_seq` is a per-shard sequence, but moves copy it verbatim and never advance the target's sequence; after a move, exports drop the moved rows and their deltas delete them on sync clients

**Where.**
- N137: "`ins_seq bigint NOT NULL DEFAULT nextval('engram_ins_seq')`".
- §3.3: "The loader copies `ins_seq` verbatim, so counts filtered by it match on both sides".
- §5.7 step 1: "the `ins_seq` watermark (`engram_seq_floor(now())`)", and the files keep `ins_seq ≤ watermark`.
- The delta "carries delete records for every id present in v{n−1} and absent in v{n}".
- `engram_consolidation_watermark`: `f.ins_seq < engram_seq_floor(now())`.

No statement anywhere (`setval`) advances the target sequence.

**Failure.** Rebalancing moves namespaces from old, full shards (sequence ≈ 10⁹) to young ones (≈ 10⁶), and the
copied rows keep the source's values.
- **Exports.** The first snapshot on the target uses its own ring, so every moved row has `ins_seq > watermark` and
  is excluded. `export_snapshots` and the previous snapshot's files moved with the namespace, so the delta
  `delta-v{n−1}-v{n}` emits a delete record for every moved id, and `engram-sync` "applies the delete records
  first". Every synced agent wipes its copy of the namespace. This continues until the young shard's sequence passes
  the old shard's, which may never happen.
- **Consolidation watermark.** It cannot pass the first moved fact. `engram_pending_facts` rescans the namespace on
  every round.
- **Next move of the namespace.** The pre-freeze verification treats every moved row as "recent", and the freeze
  re-copies `ins_seq ≥ engram_seq_floor(T_pre)`, which is the whole namespace. Freeze work then equals the namespace
  size, the 15 min watchdog fires and the move rolls back. The "< 30 s for ≤ 1 M-fact namespaces" claim fails.
- **Comparisons that cross sides.** As written, `engram_seq_floor($t_pre)` and `engram_seq_floor(now())` run "on
  both sides". On the target they read the target's ring, so even the first move's counts are compared over
  different ranges. The floor must be computed on the source and passed in as a number.

`ShardMove.tla` uses one global clock as the sequence (`sqt[r] = now`) and one move, so it cannot see this.

**Recommendation.** At (b′), before the target becomes readable as `ready`, set the target's sequence to at least
the source's: `setval('engram_ins_seq', greatest(last_value, $source_nextval))`. This is safe because the sequence
only moves forward. Have the ring take a sample immediately afterwards. State that every `engram_seq_floor` value
used in a move is computed on the source and passed as a parameter. Add a second move and an export to the move
tests (`TestMove_TwiceAndExport`).

### C-5: major [regression: N124]: `PreVerify` checks completeness before its catch-up copy, so moves of active namespaces roll back deterministically

**Where.** §5.5.1 step 2b: "Run the whole-namespace work … against the insert-only rows with `ins_seq <
engram_seq_floor(now())`: per table `count(*)` and `bit_xor(…)` … ; **then** one catch-up copy of the rows with
`ins_seq ≥ engram_seq_floor(T_copy)` … A mismatch fails the activity and the move rolls back before it ever freezes."
§3.8 gives the same order.

**Failure.** The bulk copy is dirty and ordered by primary key, and it runs for hours (`StartToClose` 12 h).
Rows inserted under old ids after their range was copied are missing from the target. Examples are `fact_consolidation` stamps
for old facts, new `observation_versions` of old observations, `consolidation_applied` rows and re-embeds. Any such
row whose `ins_seq` lies in `[engram_seq_floor(T_copy), engram_seq_floor(now()))` falls inside the counted range and
is absent from the target, because only the catch-up copy that follows would bring it. So any namespace with
consolidation or `APPEND` activity during a copy longer than about 10 min fails the count and rolls back. This is
the round-4 C-8 and P-1 outcome, reached through the new step.

`ShardMove.tla`'s `EndCopy` is enabled only when every row below the floor is already on the target, and
`CopyRange` keeps copying until then. The spec therefore models "copy until verified", which the prose does not
describe.

**Recommendation.** Run the catch-up copy first. Then verify the rows with `ins_seq < engram_seq_floor(T_cu)`, where
`T_cu` is the catch-up start, provided the catch-up takes less than about 9 min (otherwise repeat). Make `EndCopy`
in the spec copy only what the catch-up selects.

### C-6: major: a restore or lossy failover of the TARGET after the catalog CAS completes the move onto a target that may lack the reconciled rows; the spec re-copies from the source, but the prose does not, and the prose's cleanup gate then deletes the only copy

**Where.**
- N123 and §5.5.5 step 1: "If it reads `committed` … the reconcile completes (c), (b″) and (d) itself".
- §5.5.1 step 8: cleanup waits for "a full backup of the target taken after activation"
  (`target_backup_at IS NOT NULL`).
- `ShardMove.tla` `ReconcileDesign` for `s = Tgt` with `cm = "committed"` does
  `store' = [store EXCEPT ![Tgt] = @ \cup store[Src]]`, commented "Src still holds the data". Its `Cleanup` guard is
  content-based: `frozenSet \subseteq bak[Tgt].store`.

**Interleaving.**
1. Reconcile re-copies the last rows, then (b′), the CAS (a″), (c) and (b″) run within a few seconds.
2. The target primary dies, and its asynchronous standby is 5 s behind (the plan accepts ≤ 60 s).
3. The promoted target holds the namespace `incoming`, or `ready` without the last reconciled rows.
4. The failover reconcile reads `committed` and "completes (b″)". Either no edge exists (`activate_target` needs
   `ready`; there is no admin edge from `incoming`), so the restore is stuck, or the target is activated without the
   rows.
5. The next full backup of the target is taken after activation, so `target_backup_at` is set, and `CleanupMove`
   deletes the source rows.

Rows that were committed and acknowledged on the source, possibly long before the move, are then gone. This is not
the RPO loss the plan accepts. The source rows existed until the cleanup, and the spec repairs the target from them.
The "PITR to a point before a move-in" path covers only a deliberate restore after `done`. The §5.5.4 crash table has
rows for the source only.

**Recommendation.** For a target that is restored or promoted while its move is `committed` or `cleaning`:
- re-run Reconcile against the frozen or `moved_out` source (still intact) before (b″);
- give the target an admin edge `incoming → ready` for this case;
- make the cleanup gate content-based, not time-based: before `CleanupMove`, verify the target against the source
  over the namespace (counts and hashes; the source is static once `moved_out`), not only that a backup ran.

Write into §7.1 that the spec carries this re-copy.

### C-7: major: as written, the replay's epoch guard skips the second intent of a subject, because replayed rows commit under the bumped epoch; the spec compares original epochs

**Where.**
- `deletion_log.epoch`: "the namespace epoch the marker committed under … Replay … SKIPS an intent whose epoch is
  older than that of an entry of the same subject already applied (here or by an earlier replay step)".
- §9.3 step 5: intents are applied "through the admin variant of the marker transaction (same statements …)".
- §9.3 step 4 rewrites the shard's epoch to `catalog epoch + 1` before step 5.
- `Durability.tla` `Replay`: the guard reads `ops[e.op].ep`, the original operation's epoch, and the appended entry
  keeps it.

**Interleaving.**
1. `Invalidate(f)` (I1, epoch e) and then `Restore(f)` (R2, epoch e) are both acknowledged.
2. The shard is restored to a point before I1, and the epoch becomes e + 1.
3. The replay applies I1 through the same marker statements, so its new `deletion_log` row records epoch e + 1.
4. R2's intent carries e, which is older than the e + 1 entry the replay just applied, so R2 is skipped.
5. f stays hidden although the client's last acknowledged request was `Restore`. The same happens to any two
   document deletes of a re-used id, where the second is skipped and its new versions stay visible.

**Recommendation.** Replayed rows must store the intent's recorded epoch, and the guard must compare recorded
epochs only. State this in §9.3 and in the `deletion_log` comment, and add `TestIntent_ReplayTwoOfSameSubject`.

### C-8: minor: concurrent curation calls on one fact fork the `prev_operation_id` chain; `Durability.tla` serialises them

**Where.**
- §5.4.5: `Invalidate` takes "no derivation lock"; the DDL header says marker transactions take no lock beyond the
  fence.
- N122: `prev_operation_id` is "read under the document lock". There is none for a fact subject.
- `Durability.tla` `Issue`: for subject x, "one request at a time" (`\A j \in Issued : ~Live(j)`).
- `deletion_log_subject_idx` is keyed `(kind, subject_id)`, where `kind ∈ {invalidate, restore}`.

**Interleaving.**
1. f is hidden and its last chain entry is X.
2. A duplicate `Invalidate` A runs `ON CONFLICT DO NOTHING`, takes no row lock, reads prev = X and holds its
   uncommitted entry.
3. A concurrent `Restore` B reads prev = X before A commits and deletes the row. Both are acknowledged, and the
   state served is visible.
4. After a restore, the replay sees two children of X, and their order is unspecified (it is not a broken chain).
   Applying B before A leaves f hidden.

If the chain lookup uses the index's `kind`, `Invalidate` and `Restore` form separate chains, and every
`Restore`-after-`Invalidate` is unordered.

**Recommendation.**
- Serialise the marker transactions of one fact: a transaction advisory lock on `(ns, memory_id)`, or `FOR UPDATE`
  on the fact's last `deletion_log` row read through a subject key.
- Define the subject as `(document | memory | namespace | tenant, id)`, independent of the action kind.
- Let `Durability.tla` issue concurrent x requests.

### C-9: minor: prose and SQL disagree in ways that make marker and stub transactions fail as written

- §5.4.1 step 7 and §5.7 say `SET state = 'expired', expired_at = now()`. `export_snapshots` has no `expired_at`,
  and its `CHECK ((state = 'expired') = (expired_reason IS NOT NULL))` requires `expired_reason` (verified against
  the applied DDL). As written, every `DeleteDocument` marker transaction fails.
- §5.4.5 inserts `deletion_log(kind = 'memory' …)` and `kind = 'memory_restore'`. The `CHECK` admits only
  `'document', 'invalidate', 'restore', 'namespace', 'tenant'`.
- The N136 stub insert `(id, version, root_version, effective_at, text = '', stub = true)` omits the `NOT NULL`
  columns `pv_id`, `markdown_blob_key` and `evidence_hash` (32 bytes) of `page_versions`, and the matching ones of
  `observation_versions`. The stub's `markdown_blob_key` would name a deleted blob.

**Recommendation.** Generate these statements from the DDL, as N139 does for the operation enums, and give the stub
explicit placeholder values that are documented as never dereferenced.

### C-10: minor: the export `hidden_overlay` misses invalidation-derived text until Materialize, and hides rebuilt versions forever

- **Gap until Materialize.** The overlay is "`fact_hidden(invalidate)` ids plus … `derived_hidden`". Observation
  and page versions derived from a newly invalidated fact are covered only once Materialize writes
  `derived_hidden(invalidation)` rows. Until then (≤ 15 min, unbounded while a move pauses the expunge, and never if
  the signal is lost, see C-2), sync clients serve text the live API hides. §5.4 promises "within one manifest
  poll".
- **Rebuilt versions hidden forever.** The client rule in §5.7 step 6 ("hide … derived `(kind, id, version ≥
  from_version)` rows") drops `root_version`. Document-cause rows are permanent and stay in the overlay, so every
  observation or page ever touched by a delete stays hidden on sync clients after it is rebuilt in a new root.
- **Wrong serving rule.** `observations.jsonl` exports "the latest visible version with `effective_at ≤
  effective_as_of` … the D9 rule", which N117 replaced with "the version current at `T`; nothing if hidden".

**Recommendation.** Compute the overlay for derived versions from the live predicate, not from `derived_hidden`.
Ship `root_version` and compare it on the client. Align the export rule with N117.

### C-11: minor: writers clear `stale_write` unconditionally, so a `Restore` or `REPLACE` that lands while a rewrite is in flight is forgotten

**Where.** §5.2.2 step 3.6.6: "`UPDATE observations SET current_version, proof_count, stale_write = false,
stale_delete = false …`". `observations` has no `stale_seq`; pages do ("clears the flags only with `WHERE stale_seq =
$captured`").

**Interleaving.**
1. A writer W renders a rewrite of O.
2. `Restore(f)` commits. It holds the exclusive derivation lock, so it is serialised before W's commit, and it sets
   `stale_write`.
3. W commits. Its inputs are still visible, and it clears the flag.
4. f was stamped `done` long ago, so it never re-enters O's current text. A `REPLACE` that retires an unshown source
   is lost the same way.

A root rebuild is also a blind write (`p_base IS NULL` skips the comparison). Nothing stated serialises rebuild
batches with update batches of the same observation.

**Recommendation.** Give `observations` a `stale_seq` and use the page rule. Make root rebuilds compare the
`current_version` they read.

### C-12: minor (unverified): restore reconcile for moves past `committed`

§5.5.5 defines the arbiter for `cutover` and `committed` only, and the move enum has `cleaning`. The register's
`reconcile_out` row reads "when the catalog shows (c) done", which covers it, but the prose does not say so.

A source restored after `CleanupMove`, to a point before cleanup, resurrects the namespace's data rows under a
`moved_out` row. Nothing deletes them again. This includes rows of documents deleted on the target since, so it is a
right-to-erasure leak. `return_move` refuses to start while "data rows must be gone".

**Recommendation.** After any restore, run `engram_cleanup_namespace` for every `moved_out` row whose move is `done`.

### C-13: nit: undefined or lossy corners

- "a retire purge" (§5.4.2 step 2, N135) is never defined.
- §5.2.2: "A write whose target observation is now retired is skipped", yet the batch's facts are still stamped
  `done`. Facts attached to that observation are silently dropped from consolidation.

### C-14: nit: the replay margin is justified by the wrong interval

N134 says the "margin (10 min) exceeds the longest time between a marker commit and its intent put". A late put does
not matter: an intent is needed only when the restore point precedes the commit, and then floor ≤ restore point <
commit. The margin actually covers `deleted_at` (transaction start) preceding the restore point while the commit
follows it, which is bounded by the 30 s statement and idle timeouts. The value is safe; the sentence should say why.

## Checked and holding

- **Base CAS concurrency.** `UPDATE … WHERE current_version = p_base` under READ COMMITTED re-checks the row after a
  concurrent update (EvalPlanQual), so two writers from one base cannot both commit. `Materialize` and `Restore` take
  the derivation lock exclusively, which excludes shared-lock writers, so `BaseCurrentAtCommit` and `NoLostRebuild`
  hold for `ApplyBatch` (its `applied` and `op_key` checks precede the CAS).
- **DerivedPurge stubs.**
  - The deferrable FK of `*_version_meta` lets the delete and stub insert run in one transaction.
  - Stubbed versions lose their evidence, but they are hidden permanently, and live versions earlier in the segment
    keep theirs, so a later delete still finds them.
  - Page versions citing a stub are hidden by the fail-closed observation rule.
- **Tombstone view.** `document_tombstone_view` returns only `state = 'deleting'` rows with the highest tombstone. A
  revived id is outside the view.
- **Ack re-read.** The re-read closes the "put after the replay listed" window when the re-read reaches the restored
  shard: it is rejected while the shard is `frozen/restore` and finds no marker after `restore_done`. A failover
  after the re-read is covered because the put preceded it.
- **Read predicates.** The read path's use of pending tombstones plus `derived_hidden` is sound, because writers
  re-verify against all open tombstones, so no post-Materialize version names a victim.
- **Catalog move machine.** The trigger forbids leaving `committed` backwards. Concurrent rollback and commit CASes
  on `namespace_moves.state` admit exactly one winner.

## Counts

- blocker 0
- major 7 (C-1 to C-7)
- minor 5 (C-8 to C-12)
- nit 2 (C-13, C-14)

Five findings are marked [regression: N…]: C-1, C-2, C-4, C-5 and C-7.

## Verdict

The round-4 core holds where the specs model it. Most of the remaining defects sit in the gaps between each spec's
abstraction and the prose or DDL:
- `Commit` is atomic and never retried (C-1);
- `hidden` survives a fact purge that the DDL cascades (C-2);
- the catalog never loses a commit (C-3);
- `ins_seq` is a global clock (C-4);
- the copy runs until it is verified (C-5);
- a committed move re-copies onto a restored target (C-6);
- replayed entries keep their original epoch (C-7).

Each is fixable with a local decision: an idempotency check before the CAS, an invalidation that does not cascade, a
synchronous catalog standby, a `setval` at (b′), an ordering change, a re-reconcile path and a content cleanup gate,
and the recorded epoch. Each also needs the spec change that would have caught it.

None of these overturns the design. Until C-2, C-3, C-6 and C-7 are fixed, "an acknowledged invalidation or delete
survives" and "no lost data across a move" are not established. Until C-4 and C-5 are fixed, moves of active or
previously moved namespaces do not complete as written.

## What I could not verify

- **Zombie primary.** I did not determine whether pgbouncer's `RELOAD` closes every server connection to a returning
  old primary before an API transaction can commit and re-read a marker there. The ack re-read runs on whatever the
  pool reaches, and no spec models a zombie primary.
- **Materialize discovery.** I did not determine how Materialize discovers invalidations that need
  `derived_hidden` rows (a scan of all `fact_hidden(invalidate)` or the signal input). C-2's window depends on it.
- **Catalog synchronous mode.** I found no statement either way for the catalog standby. C-3 assumes the default,
  which is asynchronous.
- **Full design configurations.** I did not run them (they take 5 to 6 min each and were reported as passing). My
  TLC run used a reduced bound (1 writer, 2 versions, 1 curation).
