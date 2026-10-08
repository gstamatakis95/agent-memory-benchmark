# Review round 5

Three parallel reviews (correctness, Postgres/operations, API/numbers/parity).

---

## Review round 5: correctness and concurrency (C-1 to C-14)

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

---

## Engram round 5: Postgres, Temporal and operational review of the round-4 design

Scope: the D23 storage and operations design as it would run on PostgreSQL 16 with pgvector,
pgbouncer, pgBackRest and one Temporal cluster per cell. The review covers:

- the selectivity-aware semantic plan (N138): exact path below θ, HNSW with `hnsw.max_scan_tuples` above, `enable_seqscan = off` in arm transactions, `fact_type` on the vector rows;
- the index runner and hygiene: rebuild at 1 % or 2 k elements, `vacuum_index_cleanup = off`, ownership of index DDL;
- `ins_seq`, `engram_seq_log` and `engram_seq_floor` (N137) as used by the move, the export and the consolidation watermark;
- evidence tables without foreign keys, and stubs (N135, N136);
- the 8 M / 12 M / 600 GB / 50 k IOPS shard and its budgets: IOPS, hot set, WAL pacing, RTO;
- per-role pools, restore and failover, and move cost.

Findings dispositioned in rounds 1 to 4 are not repeated unless the fix is wrong or missing. Ids are P-1 onwards for this round. `[regression: X]` marks a defect introduced by round-4 decision X.

## How the evidence was produced

- **Server.** The shared scratch PostgreSQL 16.15 with pgvector 0.8.6 on a VM with 4 vCPUs and 15 GB of RAM.
  - `shared_buffers` is 128 MB, the shared server's setting, which I did not change.
  - Buffer counts (hits plus reads) are therefore the primary metric. Timings are OS-cache-warm unless stated.
- **Schema.** Databases `r5pg_a` and `r5pg_b` (mine, dropped at the end) received `sql/shard_schema.sql` unmodified, except that `CREATE EXTENSION pg_search` and the `USING bm25` indexes were stripped.
- **Synthetic shard (`r5pg_a`).** Three namespaces share partition `fact_vectors_p07` / `facts_p07`:
  - A: 300 k facts, 3,000 documents of 100 facts;
  - B: 150 k facts, 1,500 documents;
  - C: 40 k facts, 20,000 documents of 2 facts.
  - Rows were inserted in time order, interleaved across namespaces, through the real DDL (FK triggers bypassed with `session_replication_role = replica`). Vector rows measure 2,048 B, matching N114.
  - Embeddings are `l2_normalize(centroid[doc mod 64] + two noise vectors)` as `halfvec(768)`, so a document's facts share a topic.
  - Tags are nested by document number: `all`, `p20`, `p5`, `p2`, `p1`, `p02`.
  - A and B have their partial HNSW from `engram_hnsw_ddl`: A built in 78 s (586 MB), B in 32 s (293 MB).
- **Queries.** Arm queries ran as `engram_app` under RLS, with the scope GUCs set and the N138 `SET LOCAL`s:
  - `plan_cache_mode = force_custom_plan`, `enable_seqscan = off`, `max_parallel_workers_per_gather = 0`;
  - `hnsw.ef_search = 150`, `hnsw.iterative_scan = relaxed_order`, `work_mem = 64 MB` (§9.1);
  - the §3.8 semantic-arm SQL and the §3.8 exact-path SQL verbatim, with empty marker sets.
  - "Off-topic" filters are random documents whose topic is not the query's (or its two neighbours); "random-topic" filters are random documents.
- **Other executions.**
  - Hygiene: purges, `REINDEX INDEX CONCURRENTLY` and `VACUUM (INDEX_CLEANUP ON)` ran as the owner, with WAL from `pg_current_wal_insert_lsn` deltas. The server was otherwise idle, but these are not backend-local.
  - The move copy used `engram_copy_columns` and the documented `COPY … TO STDOUT BINARY` → `TEMP` table → `INSERT … SELECT` path into `r5pg_b`.

---

## Findings (ranked)

### P-1: major [regression: N138]: `hnsw.max_scan_tuples = min(4 × ef_search / s, 100 k)` truncates filtered arms to zero rows at moderate selectivity. Just above θ, the same arm costs 50 to 150 k buffers and 200 to 600 ms. The exact path is cheaper across the whole measured range

**Where.**
- N138(1): "above θ it runs the HNSW iterative scan with `hnsw.max_scan_tuples = min(4 × ef_search / s, 100 k)` and sets `RecallStats.partial` when the scan is exhausted".
- §1 and §3.8 (same text). §8 `TestRecall_FilteredArm`: "every arm returns `min(cap, n_visible)` or sets `RecallStats.partial`".
- §9.1 `exact_path_below_eligible: 5000`.

**Evidence.**

1. **The formula assumes the filter is independent of the query.** `4 × ef / s` is the number of tuples to visit when admitted rows are spread uniformly through the graph. A real tag ("project X") or `metadata_filters` predicate is correlated with topic.
   - When the query is about a different topic than the admitted documents, the iterative scan must first pass every vector closer to the query than the nearest admitted one: the query's own cluster.
   - That count does not depend on `s`. Namespace A, off-topic filters, the plan's routing and formula:

| Eligible (s) | `max_scan_tuples` (formula) | HNSW path: rows, ms, buffers | Exact path: rows, ms, buffers |
|---|---|---|---|
| 5,000 (1.7 %) | 36,000 | 150, 253, 69.1 k | 150, 40, 20.4 k |
| 6,000 (2 %) | 30,000 | 150, 202, 54.9 k | 150, 47, 24.5 k |
| 8,000 (2.7 %) | 22,500 | 150, 178, 46.5 k | 150, 57, 32.6 k |
| 15,000 (5 %) | 12,000 | 150, 100, 28.2 k | 150, 83, 61.2 k |
| 30,000 (10 %) | 6,000 | **139**, 70, 16.8 k | 150, 106, 50.1 k |
| 60,000 (20 %) | 3,000 | **0**, 26, 7.1 k | 150, 87, 42.2 k |

   - Sweeping `max_scan_tuples` for the 60 k filter: 3,000 → 0 rows; 6,000 → 150 rows; 20,000 (pgvector's default) → 150 rows, 33 ms. The 30 k filter needs ≥ 12,000.
   - So for every `s` above ≈ 3 %, the formula sets the bound **below** pgvector's default. It makes A-1's truncation worse than leaving the default alone.
   - `TestRecall_FilteredArm` passes regardless, because "or sets `partial`" accepts an empty arm.
   - The same filters drawn without topic correlation (random-topic) return 150 rows in 10–13 ms. The synthetic data is the easy case for a correlated filter, so this is a lower bound.
2. **Cost just above θ.**
   - With the bound high enough to finish (random-topic filters at 4–6 k eligible, `max_scan_tuples` 100 k), the HNSW path costs 104–147 k buffers and 360–613 ms.
   - In the plan's own routing (5–8 k eligible) it costs 46–69 k buffers and 178–253 ms.
   - Against that, the exact path costs 20–33 k buffers and 40–57 ms. It stays at or under the HNSW cost up to 15 k eligible here, and ≤ 106 ms through 60 k.
   - For a 1 M-fact namespace, θ = 5 k is s = 0.5 %, which gives `max_scan_tuples` = 100 k: about 0.7 s per arm, extrapolated linearly from the 36 k row.
3. **The eligible estimate ignores `fact_type` and the marker sets.**
   - Σ `fact_count` over `$allowed_docs` is the denominator of `s`. A `fact_types = [EXPERIENCE]` filter (often ≈ 10–30 % of facts) inflates `s` by 3–10×, which cuts `max_scan_tuples` by the same factor and truncates further.
4. **Budgets.**
   - None of these per-arm figures is in N114's 10 k-touch MID recall. One filtered vector arm at 20–70 k touches is 2–7 times a whole unfiltered recall.
   - At 100–250 ms per arm, the "recall ≈ 12 of 24 connections" pool (§9.1) saturates at ≈ 20 filtered QPS per shard: 50 QPS × 5 arm transactions × 0.1–0.25 s ≈ 25–60 connection-seconds per second.

**Recommendation (decision level).**
- Choose the path by cost, not by a fixed θ.
  - The exact path is linear in eligible rows: ≈ 0.6 page reads per eligible row cold, ≈ 4 buffers warm.
  - The HNSW path is unbounded under correlation.
  - Raise θ to ≈ 50 k (M0.6 fixes the exact value). Above it, run HNSW with `max_scan_tuples ≥ 20 k` (never below the default) and **fall back to the exact path when the scan exhausts**. That bounds every arm at "HNSW cap + exact(E)" and makes `partial` reachable only above ≈ 50 k eligible.
- Include `fact_type` (a per-version `fact_count_by_type`) and the hidden counts in the estimate.
- Make `TestRecall_FilteredArm` use **topic-correlated** filters at 1, 2, 5, 10 and 20 %, and require `min(cap, n_visible)` (no `partial`) below the fallback ceiling.
- Put the filtered-arm touches into N114. State the share of filtered recalls the 50 k-IOPS budget assumes.

### P-2: major [regression: N137/N124]: `ins_seq` is copied verbatim into a shard with its own sequence. After a move, the export omits every moved row, and its delta deletes them from sync clients. A second move of the namespace re-copies the whole namespace under the freeze

**Where.**
- §3.1: "The loader copies `ins_seq` verbatim, so counts filtered by it match on both sides".
- §5.5.1 step 2 ("includes `ins_seq` (copied verbatim)").
- §5.7 step 1: "the **`ins_seq` watermark** (`engram_seq_floor(now())`)"; step 2: "insert-only rows are bounded by the watermark (`ins_seq ≤ watermark`)".
- N124: the under-freeze re-copy is `ins_seq ≥ engram_seq_floor(T_pre)`.
- `engram_consolidation_watermark`: `f.ins_seq < engram_seq_floor(now())`.

**Evidence (executed).**
- **Setup.**
  - 4,990 facts of A, with `ins_seq` 447,101 to 454,500, were copied into `r5pg_b` exactly as §5.5.1 step 2 prescribes (`engram_copy_columns('facts')` includes `ins_seq`).
  - The target's `engram_ins_seq` stood at 24,000, after two hours of `engram_seq_log` samples from its other namespaces.
- **Result** on the target:

| Quantity | Value |
|---|---|
| `engram_seq_floor(now())`: the export watermark | 22,000 |
| moved facts with `ins_seq ≤ watermark`: in the next export | **0 of 4,990** |
| moved facts with `ins_seq < floor(now())`: covered by a later move's PreVerify | 0 |
| moved facts with `ins_seq ≥ floor(now() − 5 min)`: re-copied under a later move's freeze | **4,990 (all)** |

- **Why this is the common case.** Each shard has its own `engram_ins_seq`, and every row of every insert-only table draws from it: about 350 values per committed chunk, counting links and mentions. An 8 M-fact source shard therefore sits in the hundreds of millions to billions. A new shard, the usual target of a rebalancing move, starts at 1.
- **Effects.**
  - **Export.** The first snapshot on the target omits every moved fact, chunk, observation version and page version. The always-emitted delta (N126) diffs it against the pre-move snapshot, so it carries a **delete record for every moved id**. `engram-sync` applies it, and the local agent's copy is emptied. This persists until the target's sequence passes the source's, which can take months.
  - **Second move.** The next move out of that shard finds every moved row above `engram_seq_floor(T_pre)`. Freeze work becomes the whole namespace, not "rows inserted since `T_pre − 10 min`". For a 1 M-fact namespace that exceeds the 15-min watchdog, and the move rolls back every time.
  - **Consolidation.** Moved facts never qualify as a watermark position until the target sequence catches up, so the watermark cannot advance over them. `engram_pending_facts` keeps scanning them on every sweep.

**Recommendation (decision level).**
- Make the sequence monotone across a move. At `Plan` and again at `(b′)`, run `setval('engram_ins_seq', greatest(last_value, source W) + 1)` on the target, then insert an `engram_seq_log` sample.
  - Copied rows then sort below every row the target inserts after cutover.
  - The export watermark and the move floor are correct 10 min after `(b′)`; the export is refused or deferred until then.
- Alternatively, re-stamp `ins_seq` on the target and key the cross-shard checks by the source's value carried in a separate column. That is a larger change.
- Add `TestMove_ThenExport` and `TestMove_Twice` (young target shard) to T3.

### P-3: major [regression: N124]: PreVerify runs the whole-namespace counts **before** its catch-up copy, so a namespace written during the bulk copy deterministically fails PreVerify and rolls back

**Where.** §5.5.1 step 2b, quoted:

> "Run the whole-namespace work … against the insert-only rows with `ins_seq < engram_seq_floor(now())`: per table `count(*)` and `bit_xor(…)` …, `VerifyFK`, and the blob existence checks; **then one catch-up copy** of the rows with `ins_seq ≥ engram_seq_floor(T_copy)` … A mismatch fails the activity and the move rolls back before it ever freezes."

The same order appears in §3.8 "Move queries", §1's diagram, §2 `PreVerify` and §8 row "Pre-freeze verification".

**Evidence (interleaving).**
1. `BulkCopy` copies tables in dependency order, by PK, starting at `T_copy`. `documents`, `chunks` and `facts` finish long before `deletion_log`; a 1 M-fact copy takes hours (§5.5.1).
2. At time `t` after `facts` finished, `CommitChunk` inserts fact `f`. Its UUIDv7 id lies above every key the copy read, so `f` is not on the target, and its `ins_seq` is drawn at `t`.
3. `PreVerify` starts at `now ≥ t + 10 min`. `engram_seq_floor(now())` is the sample at `now − 10 min ≥ t`, so `f.ins_seq < floor(now())` and `f` is included in the source count but absent from the target.
4. The counts differ → `PreVerify` fails → `Rollback`. The catch-up copy that would have fixed it never runs. The retry meets the same writers.

This is round-4 P-1's failure mode again: every namespace that receives a write more than 10 minutes before `PreVerify` cannot be moved.

`ShardMove.tla` does not catch it, because `EndCopy` is an **enabling guard** (`\A r ∈ store[Src] : KeyOf(r) + Margin < now ⇒ r ∈ store[Tgt]`) interleaved with `CopyRange`. The spec models "copy until the check holds", not "check, then roll back".

**Recommendation.**
- Order PreVerify as catch-up first, then the checks.
  - Record `t_c` at the start of the catch-up and copy `ins_seq ≥ engram_seq_floor(T_copy)`.
  - Then count, hash and run `VerifyFK` over `ins_seq < engram_seq_floor(t_c)`.
  - Rows below that floor committed before `t_c`, so they were read either by the bulk copy (committed before `T_copy`) or by the catch-up.
- Add the check-then-rollback form to `ShardMove.tla` as a must-fail configuration (`_VerifyBeforeCatchUp`). Make `TestMove_ActiveBacklog` write into an already-copied table during the copy.

### P-4: major [regression: N138]: hygiene's `VACUUM (INDEX_CLEANUP ON)` is per partition, so it repairs every **other** namespace's graph on the partition that has sub-threshold purges, the work `vacuum_index_cleanup = off` exists to avoid. It also reads every HNSW on the partition

**Where.**
- N138(3): "a touched HNSW is rebuilt at 1 % of `rows_at_build` or 2 k elements …, then `VACUUM (INDEX_CLEANUP ON)`; vector partitions carry `vacuum_index_cleanup = off` permanently, so autovacuum never repairs a graph".
- `engram_index_hygiene_due` (one index at a time). §8 `TestIndex_Hygiene` uses a single 1 M-vector namespace.

**Evidence (executed on `fact_vectors_p07`, three namespaces).**
- **Setup.** Purge 1,500 facts of B (1 %: due) and 600 of A (0.2 %: not due). The runner rebuilds B (`REINDEX INDEX CONCURRENTLY`, 32 s) and then runs the prescribed `VACUUM (INDEX_CLEANUP ON) fact_vectors_p07`.
- **Result.**

| Quantity | Value |
|---|---|
| elapsed | **82.5 s** |
| WAL | **361 MB** (52 k full-page images, 73 k records) |
| A's HNSW (75 k pages): **repaired**, 600 dead elements | 5.48 M reads + 44.9 M hits |
| B's freshly rebuilt HNSW (37 k pages), no dead TIDs | still fully scanned (`RemoveHeapTids`): 113 k reads + 184 k hits |
| for comparison: a full rebuild of A | 78 s |

- **Why it happens.** A table-level `VACUUM` calls `ambulkdelete` on every index of the relation once any dead item exists. PG 16 has no per-index `INDEX_CLEANUP`.
  - With ≈ 7–8 namespaces per partition (120 per shard / 16), a due index almost always shares its partition with touched-but-not-due graphs.
  - Each hygiene event therefore runs the unplanned, unpaced repair (≈ rebuild cost even at 0.2 %), plus a full read of every HNSW on the partition (≈ 1 GB per partition at the 8 M target).
- **Other sources of the same repair.**
  - A namespace delete (`DROP INDEX`, then `DELETE`) leaves up to 1 M dead TIDs that the next hygiene vacuum of that partition must sweep through every remaining HNSW.
  - Small namespaces (< 2,000 vectors, no HNSW, no `vector_indexes` row) never trigger hygiene. Their dead line pointers and B-tree entries persist until some other namespace's hygiene vacuums the partition. Until then those heap pages cannot become all-visible, and anti-wraparound vacuums rescan them.

**Recommendation (decision level).**
- Make the **partition** the hygiene unit. When any index on a partition is due, rebuild every HNSW on it with `purged_since_build > 0` (the round-4 P-4 recommendation N138 narrowed), then vacuum. Alternatively, defer the partition vacuum until all touched indexes are rebuilt.
- Add a periodic partition hygiene for partitions with dead tuples and no due index (small namespaces, namespace deletes), with the same rule.
- Budget the full HNSW read per partition vacuum. Restate "repair costs 5 to 6 times a rebuild" as the fraction-dependent ratio it is: ≈ 1× at 0.2 %, 2× at 1 %, 5–6× at 4 %.
- Extend `TestIndex_Hygiene` to a partition with ≥ 3 namespaces purged at 0.2 %, 1 % and 0 %.

### P-5: major [regression: N124/N138]: cutover is not gated on the target's HNSW, so a moved namespace is served by whole-namespace exact scans until the serialised, xmin-guarded runner builds its indexes

**Where.**
- §5.5.1 step 2: "after the copy the mover requests the namespace's partial indexes from the **index runner**".
- §3.3.4: "On a move target the indexes are built only **after** the bulk copy".
- Reconcile excludes `vector_indexes` ("re-derived on the target"). §3.1: "Expunge and the index jobs read `purgeable_namespaces`, which also excludes a namespace with an open move".
- No step between Reconcile and (a″) checks `vector_indexes.state = 'ready'`.

**Evidence.**
- **Nothing orders a build before the cutover.**
  - Builds are serialised per shard; they refuse to start while any `backend_xmin` is > 5 min old (P-9); a 1 M build takes 5–6 min per vector table (§9.2).
  - If the "index jobs" that join `purgeable_namespaces` include the runner, the target's builds cannot start until after activation at all.
- **Cost of the arm without its HNSW**, measured with the HNSW unusable (`enable_indexscan = off`, otherwise the same arm SQL):
  - A (300 k): **807 ms and 76.5 k page reads (600 MB) per semantic-arm call**;
  - B (150 k): 411 ms, 38.8 k reads.
  - A 1 M-fact namespace, the size moves exist for, extrapolates to ≈ 2.7 s and 2 GB per arm call, times three vector arms per recall.
  - On the target shard this is an IOPS outage for its other ≈ 120 namespaces while the hot namespace is served. The same holds after a rollback-and-retry or a PITR recovery move.

**Recommendation.**
- Make "every `(vector table, current model)` of the namespace with ≥ 2,000 vectors has a `ready` index whose `rows_at_build` ≥ the reconciled count − tolerance" a precondition of `ReadyTarget` (b′).
- Have the runner serve move-target requests regardless of `purgeable_namespaces`, at priority.
- Builds run before the freeze, so the freeze is not lengthened. The re-copied rows insert into the built index.

### P-6: major: the hot set omits the `fact_links` primary key. Recomputed from §3.7's own table it is ≈ 97 GB at the 8 M target, beyond the "80 to 96 GB" of usable cache, which is the condition under which the plan rejected 10 M

**Where.**
- §3.7 hot set: "HNSW 16 + vector heaps 16 + `facts` heap 6.4 + BM25 3.2 + B-trees and `ins_seq` indexes 5 + the `fact_links` PK and reverse index (≈ 16 GB) + chunk/observation vectors and HNSW 5 + graph working set" ≈ 80 GB.
- The same table: `fact_links` "PK 21 GB, reverse 20 GB" per 10 M facts. §3.8: each graph hop reads both the PK and `fact_links_reverse_idx`.
- Rejected: "10 M facts … hot set ≈ 100 GB against ≈ 80 GB of usable cache".

**Evidence (arithmetic on the plan's numbers).**
- At 8 M facts the two link indexes are 16.8 + 16 = **32.8 GB**, not 16.
- The sum becomes 16 + 16 + 6.4 + 3.2 + 5 + 32.8 + 5 ≈ **84 GB before the graph working set** and the observation/evidence lookups of degraded mode, so ≈ 95–100 GB.
- That is the regime the plan rejects for 10 M. The `ins_seq` indexes are also larger than "≈ 5 %" (P-14).

**Recommendation.**
- Either lower the target to ≈ 6.5 M facts, or raise the instance to 160–192 GB.
- Or decide that the graph arm needs only one link index per hop direction, store both directions in one index, and drop the PK from the hot path.
- Re-derive D3's fleet count. M0.6's `TestCapacity_Budgets` must measure the graph arm's resident set, not assume it.

### P-7: minor [regression: N138]: `enable_seqscan = off` does not stop a whole-namespace scan. The planner routes the HNSW-path SQL through the new `(namespace_id, ins_seq)` index, and can plan the exact-path SQL as an HNSW post-filter

**Where.** N138: "Arm transactions `SET LOCAL enable_seqscan = off, max_parallel_workers_per_gather = 0`, so a partition scan cannot be chosen".

**Evidence (executed, §3.8 SQL).**
- **HNSW-path SQL.**
  - On A, every random-topic filter of ≤ 3,500 eligible rows was planned as `Bitmap Index Scan on fact_vectors_p07_namespace_id_ins_seq_idx` (all 300 k rows of A) + `Sort`: **76.5 k page reads, 360–395 ms**.
  - On B at exactly θ (5,000 eligible): the same plan, 38.8 k reads, 226 ms. At 6,000: HNSW, 45 ms.
  - Go routes on precise `fact_count`; the planner estimates from partition-wide `document_id` statistics (n_distinct 7,544 sampled vs 24.5 k true after C was added). Where they disagree, the planner wins.
  - `SET LOCAL enable_bitmapscan = off` forces the HNSW scan (p1: 133 ms instead of 354 ms).
- **Exact-path SQL.** At a high estimate (`p20`) it was planned as `Index Scan using fv_… on fact_vectors` + nested loop into `facts` with the filter: an HNSW post-filter bounded by the transaction's `max_scan_tuples`. That is the plan the exact path exists to avoid. Go only sends this SQL below θ, where the planner chose `facts_doc_idx` in every case measured, so this is latent.

**Recommendation.**
- Pin both plans structurally.
  - HNSW path: `enable_bitmapscan = off` as well, plus `enable_sort = off`, so the ordered index scan is the only sort-free plan.
  - Exact path: compute the distance in a `MATERIALIZED` CTE over the eligible rows, then `ORDER BY` outside it, so no index can supply the order.
- `EXPLAIN` pins in M0.6 for both shapes at θ ± 20 %.

### P-8: minor: planning `= ANY($allowed_docs)` with up to 8 k constants costs 23–31 ms per arm statement, about 125 ms of planner CPU per tag-filtered recall

**Where.** §3.8: "Every arm adds `document_id = ANY($allowed_docs)`; above 8 k allowed documents the arm joins `documents`". N140: per-arm transactions under `force_custom_plan`.

**Evidence.** Planning time of the §3.8 SQL on namespace C (HNSW path / exact path):

| Allowed documents | HNSW path | Exact path |
|---|---|---|
| 100 | 1.9 ms | 2.7 ms |
| 1,000 | 6.2 ms | 8.6 ms |
| 4,000 | 20.3 ms | 17.2 ms |
| 8,000 | 23.2 ms | 31.1 ms |

- The cost is the per-element selectivity estimation that `force_custom_plan` forces.
- With ≈ 5 arm statements carrying the array, a broad tag (an `ANY` tag on most documents) costs ≈ 125 ms of CPU per recall. At 50 QPS that alone exceeds the "recall ≤ 4 vCPU" budget.

**Recommendation.**
- Lower the join threshold to ≈ 500 documents.
- Above it, pass the set as one array parameter used through `document_id IN (SELECT unnest($x))` or a join, which is not estimated per element. Keep the namespace and model as constants for the partial-index proof.

### P-9: minor: the index runner's old-snapshot guard counts `VACUUM` and autovacuum workers, which `CREATE INDEX CONCURRENTLY` does not wait for, so builds stall behind every long vacuum

**Where.** `engram_old_snapshots`: `backend_xmin IS NOT NULL AND coalesce(xact_start, query_start) < now() − 5 min`, no `backend_type` filter. The runner refuses to start while it is non-empty.

**Evidence.**
- Executed: a running `VACUUM` shows `backend_xmin` and `xact_start` in `pg_stat_activity`.
- `CREATE INDEX CONCURRENTLY` waits only for snapshots that are not `PROC_IN_VACUUM`, so the guard is over-broad.
- Autovacuum of a `fact_links` partition (≈ 4 GB of heap and 2.6 GB of indexes at target, cost delay 2 ms) and the runner's own partition vacuums (P-4: 82 s here, minutes at target) routinely exceed 5 min.
- With a standby and `hot_standby_feedback`, a walsender row with an old `query_start` would block the runner permanently (not executed).

**Recommendation.** Filter the view to `backend_type = 'client backend'`, exclude `VACUUM` commands and walsenders, and alert when the runner has refused for more than 30 min.

### P-10: minor [regression: N138]: hygiene cost and the rebuild mechanism are mis-stated or unspecified

**Where.**
- §9.2: "a 1 M-vector namespace: ≈ 6 min per 10 k purged facts".
- N138: "1 % of `rows_at_build` or 2 k elements, whichever comes first".
- `engram_hnsw_ddl` has only `create` and `drop`.

**Evidence.**
- **The 2 k element rule binds first for every index above 200 k vectors**, so a 1 M-vector namespace rebuilds every **2 k** purged facts, not 10 k: 5× the stated cost.
- **Rebuild WAL** (measured): 264 MB for 150 k vectors, 227 MB with `wal_compression = zstd`, that is 1.5–1.8 KB per vector.
  - Under the 2 k rule a purged fact in a 1 M-vector namespace costs ≈ 0.75–0.9 MB of rebuild WAL. That is 10–25× the purge's own 35–82 KB, and it is neither WAL-paced nor in §9.1's budget.
  - Steady REPLACE churn of 1 %/day in such a namespace means ≈ 5 rebuilds/day per vector table: ≈ 7.5 GB/day of WAL and ≈ 30 min/day of the shard's single build slot.
- **The rebuild statement is unspecified.**
  - Drop-then-create puts the namespace on the whole-namespace exact path for the build (P-5's 807 ms per arm at 300 k).
  - `REINDEX INDEX CONCURRENTLY` leaves `<name>_ccnew` on a cancel or crash. Executed: a cancelled rebuild left `fv_6fcde9d4e414_6d9b3d_ccnew` with `indisvalid = f`, while `engram_hnsw_ddl` still reports the namespace `valid`. Deterministic-name handling therefore never removes it. Cancelled late, such an index is `indisready` and is maintained on every insert.

**Recommendation.**
- Make the trigger relative: 1 %, with a floor of 2 k only for small indexes.
- Add a `rebuild` action to `engram_hnsw_ddl` (`REINDEX INDEX CONCURRENTLY`), and have the runner drop any `<name>_ccnew*` before acting.
- Pace rebuild WAL with the purge budget and fix the §9.2 example.

### P-11: minor: the 13 GB "differential when WAL since backup exceeds it" trigger is owned by the expunge only, so move-in copies, index builds and hygiene rebuilds can push WAL since backup far past 13 GB

**Where.**
- N142: "the expunge takes a differential backup whenever WAL since the last backup exceeds 13 GB (keeps RTO ≤ 60 min)".
- §9.3: "**≤ 60 min while WAL since the last backup is ≤ 13 GB**".
- §5.5.1: the bulk copy is not in the list of WAL-paced writers ("Purge, `DerivedPurge` and move cleanup are paced").

**Evidence (from round-4 measurements and the plan's own text).**
- A 1 M-fact move-in writes ≈ 18–20 GB of WAL on the target (535 B per link, 2 KB per vector), unpaced, at 4 parallel streams. Its HNSW builds add ≈ 1.5 KB per vector.
- The full backup that follows activation (N143) comes only after cutover. Until then the target's ≈ 120 other namespaces sit at an RTO of ≈ 5 + 25 + 1.2 × 20 + 12 ≈ 66–90 min.
- The unpaced copy can also exceed the "archive throughput ≥ 50 MB/s" assumption it shares with ingest and purge.

**Recommendation.**
- Move the 13 GB trigger to a shard-level WAL watcher (the exporter already has `engram_pg_wal_since_backup_bytes`) that takes the differential whatever wrote the WAL.
- WAL-pace `BulkCopy` with the same per-shard budget.

### P-12: minor [continuation of A-1]: the observation arm still post-filters tags after the HNSW `LIMIT`, and N138's "chunk and observation arms follow the same plan" has no eligible estimate for observations

**Where.** §3.8 observation arm: `WITH cand AS (… ORDER BY distance LIMIT $over)`, then `(cardinality(o.tags) = 0 OR o.tags && $q)` and the segment predicate outside it. N138(1): the estimate is Σ `fact_count` over `$allowed_docs`.

**Evidence.**
- Observations are filtered by `observations.tags` (consolidation scope), not by documents. `document_versions.fact_count` says nothing about them, and no per-tag observation count exists.
- A selective observation tag therefore truncates the arm exactly as A-1 described for facts.

**Recommendation.** Keep a per-namespace count of observations per tag, maintained by the stats sweeper. Apply P-1's cost rule to the arm, with the tag test inside the scan (copy `tags` onto `observation_version_vectors` at version insert; versions are immutable).

### P-13: minor (unverified rates): evidence and version history grow without bound under churn. Sizing assumes 1.5 versions per observation, and no retention exists

**Where.**
- §3.7: "one observation per 20 facts, 1.5 versions each"; `observation_inputs` "≤ 58 rows per version".
- N135: evidence "dies only with its own version row (DerivedPurge, retire purge, namespace delete)".

**Evidence.**
- Every consolidation round that touches an observation inserts a version: text, a 2 KB vector row, a 2 KB HNSW element, a BM25 entry and up to 58 input rows (≈ 13 KB in all). Nothing ever removes a superseded version that has no document-cause delete.
- REPLACE and re-extraction (N135) deliberately mark observations `stale_write`, so churn turns directly into versions.
- Example: a 100 k-fact namespace that replaces 5 % of its documents daily, with 25 % of its 5 k observations rebuilt, adds ≈ 16 MB/day. That is ≈ 6 GB/year with flat live facts.
- The capacity signal (relation bytes) will see this growth. The 8 M-facts-per-shard sizing and placement will not.

**Recommendation.**
- Decide a version-retention policy, for example keeping the versions any `as_of ≥ now − H` can serve plus the current one, compacting older segments into stubs as DerivedPurge does.
- Or state the history cost per rebuild in N114 and size by bytes, not facts.

### P-14: nit: the `(namespace_id, ins_seq)` indexes are ≈ 12 %, not "≈ 5 %"

**Evidence.**
- Measured: `fact_vectors_p07_namespace_id_ins_seq_idx` is 3,186 pages for 490 k rows, **53 B per entry**. The keys are unique, so there is no deduplication, and interleaved per-namespace ascending inserts split pages mid-range.
- `fact_links` alone (300 M rows per 10 M facts) is ≈ 16 GB per 10 M facts. All insert-only tables together come to ≈ 22–24 GB per 10 M facts, about 12 %.
- Disk is fine at 600 GB. The 230 GB at the cap and the "≈ 5 %" in §3.7 and N114 are not.

### P-15: nit: the stats sweeper's per-namespace work is unbudgeted and its cadence unstated

**Evidence.**
- `count(*) FROM engram_visible_facts(A)` takes 2.9 s for 300 k facts, so ≈ 80 s per sweep of an 8 M-fact shard.
- `engram_vector_index_plan(A)` takes 93 ms. The `mentioned_at` histogram (38 ms, index-only) is cheap.
- §3.8 says the sweeper "refreshes `namespace_stats`" on the scheduler tick that also samples `engram_seq_log` every minute.

**Recommendation.** State the cadence: hourly, or incrementally from `ins_seq` since the last run. Keep only `engram_seq_sample()` on the minute.

---

## Checked and holding

- **Partial-index planning under RLS.** With `force_custom_plan`, the per-namespace partial HNSW is chosen for the unfiltered arm: 150 rows, 2.8 k buffers, 7.7 ms warm. The RLS qual folds to a one-time filter, and the plan prunes to one partition.
- **The exact path below θ.** For tag filters of 600 to 15 k eligible rows the planner drove the §3.8 exact SQL from `facts_doc_idx`, or from a bitmap on `facts`, and returned `min(cap, eligible)` in 7–92 ms. `as_of`-only filters behave the same through `facts_mentioned_idx`, at 31–47 ms for 1–2 %.
- **`as_of` without topic correlation.** HNSW under the formula returns 150 rows at every decile, at 7–133 ms.
- **`fact_type` on the vector row.** It is present and runs inside the scan: `world`-only unfiltered gives 150 rows and 3.0 k buffers.
- **Unfiltered `max_scan_tuples = 600`** (the formula at `s = 1`) still returns 150 rows.
- **The vector row is 2,048 B** (879 MB for 450 k rows), confirming N114.
- **Build rate and memory.** 300 k vectors build in 78 s with 3 workers and 1 GB, consistent with §9.2.
- **`engram_seq_floor`.** It is safe against a stalled sampler (an older sample means a larger re-copy) and against a trimmed ring (it returns 0, so everything is re-copied).
- **The re-copy key under the freeze** (`ins_seq ≥ floor(T_pre)`) is sound for rows inserted on the source. The two defects are P-2 (the target's sequence) and P-3 (the order inside PreVerify).
- **Role timeouts.** `engram_admin` and `engram_move` now carry 30 s statement and idle timeouts (round-4 P-9), and the admin, move, relay and migrate pools are listed explicitly (§9.1).

## Counts and verdict

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 6 | P-1 to P-6 |
| minor | 7 | P-7 to P-13 |
| nit | 2 | P-14, P-15 |

**Verdict.** The round-4 storage core still holds on PostgreSQL: immutable vectored rows, per-namespace partial HNSW, read-time markers, the WAL-paced purge, and the index runner as the one DDL owner. The six majors sit in the mechanisms round 4 added around it:

- a `max_scan_tuples` formula that truncates correlated filters to zero rows (P-1);
- an `ins_seq` that is not monotone across shards (P-2), which breaks exports after a move and the namespace's next move;
- a PreVerify whose checks run before its catch-up (P-3), which rolls back moves of active namespaces;
- a per-index hygiene trigger executed by a per-partition vacuum that repairs the neighbours' graphs (P-4);
- a cutover that does not wait for the target's indexes (P-5);
- a hot set that leaves out the link PK (P-6).

Each has a decision-level fix inside the current model (P-6 may move the shard target).

- **Ready:** staffing the storage and recall layers, with P-1 and P-7 decided before M1.2.
- **Not yet:**
  - enabling moves (P-2, P-3, P-5);
  - freezing the 8 M hot-set budget (P-6);
  - the hygiene runbook (P-4, P-10).

## What I could not verify

- **pg_search** is not installable here. BM25 plans, Top-K pushdown under RLS, BM25 WAL, its behaviour with `enable_seqscan = off`, and BM25 bloat after purges and stubs were not executed.
- **Scale.**
  - Namespaces were 40–300 k vectors in a 490 k-row partition, not 1–2 M in 625 k-row partitions.
  - The 1 M figures in P-1, P-5 and P-10 are linear extrapolations.
  - `shared_buffers` was 128 MB, so the hit/read split is not production's. Buffer totals are reported.
- **Real embeddings.** Topic correlation was synthetic (64 centroids). Real tag–topic correlation may be stronger or weaker; P-1's zero-row result needs only that the admitted documents lie outside the query's neighbourhood.
- **Not executed:**
  - the observation and chunk arms (no observation data generated; P-12 is from the SQL);
  - pgbouncer pooling under load;
  - Temporal throughput at the 5,800 events/s gate;
  - pgBackRest restore and replay rates (the RTO terms);
  - a walsender with `hot_standby_feedback` (P-9).
- **Not observed:** the `indisready = t` leftover in P-10. Only the early-cancel case was observed.
- **Not measured:** the WAL of the concurrent move copy itself. P-11 cites round 4's per-row figures.

---

## Review round 5: API contract, Go API, numbers, completeness, Hindsight parity

**Reviewed:** `plans/engram/` at `7288fa5` (after D23, N135 to N143), including `proto/`, `sql/`, the sections,
the register and `reference/hindsight-notes.md`, against the task brief and the product guidance (deletes are rare;
O(1) soft delete at ack; asynchronous expunge; SLOs may degrade while a delete is processed).

**Lens:** (1) `proto/**` and §4 (DocumentService tombstone view, export overlay, delete operation contract, evolution
policy and its baseline) and the §2 Go API; (2) every number (sizing at 8 M, latency, connections, throughput, cost
§6.8, fill time, engineer-weeks §10) and cross-section citations; (3) completeness against the brief; (4) Hindsight
parity; (5) consistency between sections, register, proto, SQL and the §7/§8 test names.

**Not repeated:** findings dispositioned in `reviews/round-1.md` to `round-4.md`, except where the round-4 fix is
wrong or missing (marked "fix wrong" or "fix incomplete").

## How the evidence was produced

```text
buf 1.57.0
  (in plans/engram/proto) buf lint; buf build; buf format -d --exit-code      → all exit 0
  buf breaking plans/engram/proto --against '.git#ref=d6e7746,subdir=plans/engram/proto'   (last pre-D23 commit)
      → 33 breaks, all internal: events.proto 32 (16 fields renamed *_bytes: name + json_name each),
        workflow.proto 1 (ExpungeInput.batch_pause deleted, reserved). memory.v1 / memory.admin.v1: 0.
        The §4.5 round-4 exception rows list exactly these.
  buf breaking ... --against '.git#branch=claude/engram-implementation-plan,ref=HEAD~1,…' → no output (syntax works)
  README command '.git#branch=main,ref=HEAD~1,subdir=plans/engram/proto' → "had no .proto files" here
      (main lacks plans/engram; the plan says the real module path is proto/ — not a finding)
  Baseline demo (own scratch repo rev5_api_bufdemo): main tip adds Foo.bar = 7; a PR deletes it.
      --against '.git#branch=main,ref=HEAD~1' → rc 0 (break NOT reported)
      --against '.git#branch=main'            → rc 100 "Previously present field 7 … was deleted"
§4.2 "full text" blocks of common.proto and memory.proto: byte-identical to the files (14,833 / 30,737 B).

Scratch Postgres 16 (own database rev5_api_shard; sql/shard_schema.sql applied with pg_search stubbed):
  §5.4.1 step 4 verbatim  UPDATE documents SET … context = NULL, metadata = NULL, tags = '{}'
      → ERROR: null value in column "context" … violates not-null constraint
  same with context = '', metadata = '{}' (the §2 MarkDeleting column list)
      → ERROR: new row … violates check constraint "documents_check1" (summary_hash, document_hash not cleared)
  §3.x form (also clearing summary_hash, document_hash) → UPDATE 1

Interface method counts: a script over every `type … interface {` in §2's Go blocks (97 interfaces).
Recomputed by hand: §1.5 critical path, D3/§3.7 sizing, §9.1 budgets, Table 6.8-A/B, D3 fill time, §10 sums,
per-engineer chains and Gantt day counts.
```

Severity scale as in the brief to reviewers: **blocker** (a stated guarantee is false or the system cannot be built
as written), **major** (a stated number, invariant or schedule does not hold without a decision-level change),
**minor**, **nit**.

---

## Findings

### A-1: major [regression: N140]: the connection budget was not re-derived when a recall became N concurrent transactions; "recall concurrency ≈ 12 of a 24-connection pool" no longer holds at 50 QPS

**Where.** N114 and §3.7/§9.1 ("recall concurrency ≈ 12 of a 24-connection pgbouncer pool shared with ack, expunge
and Reflect"); §2.5 "Recall arm parallelism" row; §2.1 Recall walkthrough (N140: "every arm that runs concurrently
calls `Session.Tx`, which opens its own pooled connection"); §1.5 latency table.

**Claim.** "recall concurrency ≈ 12 of the 24-connection pgbouncer pool"; "connections ≈ 52 of `max_connections` 100".

**Evidence.** The ≈ 12 matches the old model of one connection per recall: 50 QPS × 0.216 s ≈ 10.8. Under N140 a
recall holds one server connection per running arm transaction (pgbouncer transaction pooling holds the server
connection for the transaction's duration). From §1.5's own budgets: lexical and temporal run t = 2 → 62 ms,
semantic and chunks 27 → 87 ms, graph waves 2 × 30 ms, visibility ≈ 3 ms:

| Arm transaction | conn-ms per recall (at budget) |
|---|---|
| lexical, temporal, semantic, chunks | 4 × 60 = 240 |
| graph wave 1 + wave 2 | 60 |
| visibility (two indexed selects) | ≈ 3–5 |
| **Total** | **≈ 305 conn-ms** |

At 50 QPS that is a mean of **≈ 15 server connections** for recall alone (≈ 7.6 if every arm ran at half its
budget); with Poisson arrivals the p99 is ≈ 25 at budget and ≈ 15 at half budget. Either way it exceeds the 12 the
budget allots, and with ack/commits ≈ 4 and Reflect ≈ 4 the 24-connection pool saturates at the stated load. Pool
wait is not in the 216 ms critical path, and `PoolSaturation` (wait p95 < 50 ms) would fire in steady state. The
pgbouncer `default_pool_size`, the ≈ 52 of 100 `max_connections` and the CPU split are register numbers (N114, D3).

**Recommendation.** Re-derive the per-shard pool from `QPS × Σ arm transaction time` (M0.5 measures the arm times):
either raise the `engram_app` server pool (≈ 40) and the `max_connections` budget, or cap concurrent arm transactions
per recall (and say what that costs the 216 ms path). Add the per-recall connection-time to M0.5's exit and to
`TestReadSession_ConcurrentArms`.

### A-2: major [fix incomplete: r4 P-5]: the hot set counts only the links *reverse* index; the graph arm also reads the PK and the heap, which puts the 8 M shard back in the regime N114 rejected for 10 M

**Where.** D3 ("hot set ≈ 80 GB at target, including the links reverse index (≈ 16 GB)"); §3.7 ("the `fact_links` PK
and reverse index (≈ 16 GB)"); §9.1 Hot set row; N114; `sql/shard_schema.sql` `fact_links` (PK `(namespace_id,
src_memory_id, dst_memory_id, link_type)`, `fact_links_reverse_idx (namespace_id, dst_memory_id, src_memory_id)`;
comment: "the PK serves forward expansion; the reverse index serves the other direction"); §3 graph SQL (`max(e.weight)`).

**Evidence.** Undirected edges are stored once with `src < dst` (N34), so a hop from fact f reads the PK (f as
`src`) and the reverse index (f as `dst`); `weight` and `link_type` are not in the reverse index, so hops also touch
the heap. §3.7's own table at 10 M facts: PK 21 GB, reverse 20 GB, heap 30 GB → at 8 M: PK 16.8, reverse 16.0,
heap 24.0. §3.7 also says "PK and reverse index (≈ 16 GB)", which is half of the two indexes. Using the plan's own
accounting (whole HNSW, whole vector heap and whole `facts` heap counted as hot):

| | GB at 8 M |
|---|---|
| hot set as stated | ≈ 80 (links 16) |
| + links PK | ≈ 97 |
| + links heap | ≈ 121 |

The usable cache is "≈ 80 to 96 GB of the 112 GB container". N114 rejected 10 M facts because the hot set
(≈ 100 GB) exceeded the usable cache; the corrected 8 M figure is in the same place. Fleet size (125 shards, 4
cells), host count and cost all follow from this decision.

**Recommendation.** Restate the hot set with both link indexes (and the heap, or an index-only covering reverse
index `INCLUDE (link_type, weight)` plus a covering PK, if you want to drop it). Then either lower the target (≈ 6.5 M
at the same RAM), raise RAM, or state that M0.6 decides between them with R37's trigger as the gate before M1.2.

### A-3: major: the Phase 0 exit cannot be met in week 8, because M0.7's exit needs fault-injection twins of code written in Phase 1 and Phase 2; the trace converter is placed both in M0.7 and in the uncommitted Track F

**Where.** §10 M0.7 (E1 1.0 + E2 1.0, weeks 6–8; exit "every twin of §8.4.6 compiles and its `faultinject` knob
reproduces the flaw"); Phase 0 exit (week 8: "M0.1–M0.8"); §8.4.6 ("Every counterexample configuration has a Go twin
that reproduces the flaw with the fix disabled (a `faultinject` knob)"); §7.4(b) ("`engramctl formal trace-to-tla` …
M0.7 delivers the converter"); §10 F.1 (uncommitted: "trace validators … `engramctl formal trace-to-tla`, the
`TraceNext` modules"); M2.3 exit ("nightly trace validation green for 7 consecutive days").

**Evidence.**
- **Twins before their subject exists.** The twins of `ShardMove` (`TestMove_CatalogCAS`, `_CleanupWaitsForBackup`),
  `Durability` (`TestIntent_AckRereadsMarker`, `_EpochGuard`), `Derivation` (`TestApply_BaseVersionCAS`,
  `TestPageRefresh_DeleteMidCall`) and `Storage` (`TestIndex_Hygiene`) toggle a knob inside `internal/move`,
  `internal/intent`, `internal/consolidate`, `internal/pages` and the index runner. Per the Gantt those are written in
  M1.5 (E1 days 64–120), M1.9 (days 120–131), M1.10 (days 131–145), M2.1 (E2 days 127–183) and M3.1 (separate track).
  A knob cannot "reproduce the flaw" in weeks 6–8. So either Phase 0 cannot exit in week 8, or the twins are really
  built inside the M1.x/M2.x milestones (whose exits already list most of them) and M0.7's 2 ew is double-counted.
- **Converter.** §7 makes it an M0.7 deliverable; §10 places it in F.1, which is outside the 84.5 committed ew. M2.3
  is committed and its exit needs nightly trace validation, so a committed exit depends on uncommitted work on E2's
  zero-slack chain.
- **Minor.** M1.2 (E3) starts on day 32, a week before M0.6 fixes θ (day 39), but its plan and exit depend on θ.

**Recommendation.** Re-scope M0.7 to the spec-side work that can exist in Phase 0 (manifest, CI wiring of the
must-fail configurations, invariant code in `internal/formal`, the converter). Move each twin into the milestone that
builds its subject, with its effort. Put the converter in exactly one place and inside the committed scope if M2.3
keeps its exit. Re-draw the Gantt.

### A-4: minor [fix wrong: r4 A-9]: `main@HEAD~1` is the wrong baseline for pull requests, and the gate misses breaks against main's latest commit

**Where.** §4.5 ("until then CI compares against `main@HEAD~1`"; "`buf breaking` runs on every pull request");
`proto/README.md`; M0.1 exit ("`buf breaking` blocks a field-number change"); N139.

**Evidence.** Executed (scratch repository, see the evidence block): main's tip adds `Foo.bar = 7`, and a PR deletes it.
Against `.git#branch=main,ref=HEAD~1` buf exits 0, so the break is not reported. Against the main tip it exits 100. The
reverse also fails: when main's tip carries an intended, labelled break, every open PR is compared against the commit
before it, reports that break again and needs the exception label too. That dilutes the label as a record.

**Recommendation.** For pull requests, compare against the merge target (`.git#branch=main`, or the merge base).
Use `HEAD~1` only in the push-to-main job. From `v1.0.0` on, keep the release-tag gate as stated.

### A-5: minor [regression: N136]: the operation-to-workflow contract is wrong for `REFRESH_PAGE`, and "the `operation_id` is the workflow id suffix" is false for three kinds

**Where.** N136(4) and §5.0 ("Retain, export and page refresh run at `ns/{ns}/op/{op}`"); `operation.proto` header;
`sql` `operations.workflow_id` comment; §2.2.17 `OpID` ("retain, export, refresh, namespace delete") versus
`PageRefreshID`, `Signaller.SignalPageRefresh` and §2.2.16 ("`Refresh` runs as workflow `ns/{ns}/page/{page_id}`");
§5.3.1 (manual refresh = `SignalWithStart(ns/{ns}/page/{page_id}, Nudge{manual, operation_id})`); §4.1.3 table
("`operation_id` … *is* the Temporal workflow id suffix").

**Evidence.** §5.3 implements a manual refresh as a nudge to the per-page singleton, while the operation contract
says it has its own workflow:
- `WaitOperation` long-polls `ns/{ns}/op/{op}`, which does not exist, and silently falls back to the DB poll.
- `CancelOperation` → `Waiter.Cancel(OpID)` finds no workflow, or, if it targets the singleton, kills scheduled and
  delete-driven refreshes.
- Move `Restart` "skips singleton-backed kinds" but does not list `REFRESH_PAGE` as one. It therefore starts a
  `PageRefresh` at `ns/{ns}/op/{op}` on the target while the singleton is restarted by `SignalWithStart`. That gives
  two concurrent refreshers of one page: the N143 CAS keeps them safe, but `PAGE_REFRESHING` is defeated and
  strong-model calls ($0.02–0.03 each) are paid twice.

The §4.1.3 claim is also false for `DELETE_DOCUMENT` (expunge singleton) and `DELETE_TENANT`
(`tenant/{tenant}/delete`).

**Recommendation.** Mark `REFRESH_PAGE` singleton-backed everywhere (N136, proto header, SQL comment, `OpID`, the
Restart skip list), or give manual refreshes their own workflow id and make the singleton defer to it. Fix the §4.1.3
row to "the workflow id, per kind (N136)".

### A-6: minor [fix incomplete: r4 A-6]: `api.Deps` is still not complete for the served RPCs, and the coverage check as specified cannot detect it

**Where.** §2.2.1 `Deps` ("complete for every served RPC (N140)"; `NewServer` panics if "an RPC with no non-nil
dependency path"); the repositories of §2.2.7; §2.2.3 `catalog.Namespaces`; §2.2.16 `pages.Writer`; §2.2.20 `export`;
§2.1 dependency table (`internal/api` may not import `move`); §2.2.17 ("The move is started by
`move.Orchestrator.Start`").

**Evidence.** No interface method serves these RPCs (counted against the 58 RPCs in `proto/`):

| RPC | Missing |
|---|---|
| `NamespaceService.ListNamespaces`, `UpdateNamespace` | `catalog.Namespaces` has Resolve, ResolveByName, Create, SetState, BumpEpoch |
| `OperationService.GetOperation`, `ListOperations` | `OperationRepo` has no Get/List; `Deps.Ops` is `Wait` only |
| `MemoryService.ListMemories` | `FactReader` has ByIDs, NearestByOccurrence, Pending |
| `DocumentService.GetDocumentVersion`, `GetDocumentBody`, `ListTags`, `UpdateDocumentTags` | `DocumentReader` has Get, List; `DocumentWriter` has no tag update |
| `PageService.UpdatePage` | `pages.Writer` has Create, Delete, Refresh |
| `ExportService.ListSnapshots` | `Streamer` has Stream, Latest, Overlay |
| `MoveService.StartMove`, `RollbackMove` | `move.Orchestrator` is not in `Deps`, `api` may not import `move`, `Starter` has no move start |

"A non-nil dependency path" is satisfied trivially by `Stores` for every per-shard RPC, so neither the panic nor
`TestDeps_EveryRPCHasPath` can catch the rows above. `go vet` against a stub module checks types, not coverage.

**Recommendation.** Generate the coverage table as (RPC → interface method), not (RPC → field). Add the missing
methods, which interacts with A-7. Add `move.Orchestrator` to `Deps` and `move` to `api`'s allow-list.

### A-7: minor [regression: N143]: the five-method rule is broken, and the missing methods cannot be added without breaking it further

**Where.** §2 preamble ("every interface has at most five methods"); §2.2.3 `catalog.Moves`; §2.2.7 `store.Tx` ("It
also satisfies `txn.Tx`"); §2.1 `txn.Tx { Scope(); Usage() }`.

**Evidence.**
- **`catalog.Moves`** has six methods: Plan, Advance, Commit, RecordTargetBackup (added by N143), Cutover, Rollback.
  The other 96 interfaces in §2 are ≤ 5 (scripted count).
- **`store.Tx`** lists five accessors (Read, Insert, Markers, Derived, Ops). To "satisfy `txn.Tx`" it needs
  `Scope()` and `Usage()` as well, which makes seven, or the claim is false and `quota.Meter.Record(tx txn.Tx)` cannot
  be called with a `store.Tx`.
- **A-6's additions** push `catalog.Namespaces`, `DocumentWriter` and `DocumentReader`/`ReadTx` past five.

**Recommendation.** Split by capability as the rule says: `catalog.MoveBackups` (or move `RecordTargetBackup` to
`MoveReader`'s writer twin), `catalog.NamespaceAdmin{List, Update}`, `DocumentTags{Update, List}`. Make `store.Tx`
expose `Txn() txn.Tx` rather than embed it. Have the M0.1 stub-module vet also run the method-count lint.

### A-8: minor: the stated signatures violate the depguard table and the leaf rule that M0.1's exit enforces (the graph is acyclic)

**Where.** §2.1 dependency table and "The infrastructure edges are exactly …"; §2.2.16, §2.2.20, §2.2.15, §2.2.6,
§2.2.4, `pipeline`, `errs`.

**Evidence.** I checked the stated edges for cycles and found none. The allow-list, however, is violated:
- **Services importing `authz` or `router`**, which are not in the services' list:
  - `pages.Reader.*(sc authz.RequestScope)` and `pages.Writer.Refresh(h *router.ShardHandle, …)`;
  - `export.Builder.WriteFiles(h *router.ShardHandle)` and `export.Streamer.*(sc authz.RequestScope)`;
  - `reflectagent.Tool.Call(sc authz.RequestScope)`.
- **Leaves importing non-standard packages:** `pipeline.Run` "opens one telemetry span" (OTel or `telemetry`), and
  `errs` imports `google.golang.org/grpc/status`, `connectrpc.com/connect` and `proto`. The leaf row allows only the
  standard library, `uuid` and gen/go details.
- **Unlisted infrastructure edges:** `blob → store` (`Tombstoner` takes `store.Tx`) and `router → telemetry`
  (`ShardHandle.Metrics`).

**Recommendation.** Have services take `id.Scope` plus a small `Caller` value instead of `authz.RequestScope`, and a
`store.Store` instead of `*router.ShardHandle`. Name the third-party modules a leaf may import. List the two edges.

### A-9: minor: the read side has no derived-version readers, so `GetMemory` and `GetPage` of an observation or page need a fenced write transaction, which a move freeze refuses

**Where.** §2.2.7 `ReadTx {Documents, Chunks, Facts, Graph, Visible}`; `Derived().Observations().Served` and
`PageRepo.Served` reachable only from `store.Tx` (`InNamespace`: try-lock fence, `active` at epoch); D2 row 3 and N122
("reads … accept `active` and `frozen/move`"); `memory.proto` `GetMemory` ("fact, observation or chunk"),
`BatchGetMemories`, `ListMemories(kinds)`; `pages.Reader.Get`.

**Evidence.** No SQL may exist outside `store` and `index`, so these handlers must use `Served` through
`InNamespace`. During a move freeze that returns `NamespaceFrozen`, although reads are promised. Even when active, every
such read takes the shared fence try-lock and `lock_timeout` path of a write.

**Recommendation.** Add a `Derived()` read accessor (`Observations().Served`, `Pages().Served`) to `ReadTx`. Split
`ReadTx` to keep it at five.

### A-10: minor: the "one" `DELETE_DOCUMENT` marker transaction exists in three versions that disagree; the §5.4.1 and §2 versions fail against the DDL

**Where.** N139 ("one marker transaction (§5.4.1's)"); §5.4.1 steps 3–9; §3 "Marker transactions" SQL ("This is the
one `DELETE_DOCUMENT` marker transaction (§5.4.1)"); §2.2.7 `MarkDeleting`; `sql/shard_schema.sql` `documents`,
`document_tombstones`, `export_snapshots`; D6 / A-F1.

**Evidence.**
- **Executed.** §5.4.1 step 4 (`context = NULL, metadata = NULL`) fails NOT NULL. §2's column list (summary key,
  context, metadata, tags) fails `documents_check1`, because `summary_hash` and `document_hash` must be cleared too.
  Only §3's form succeeds.
- **Step ordering.** §5.4.1 step 6 inserts the tombstone before step 9 obtains `seq`, but `event_seq` is
  `NOT NULL`. Step 7 sets `expired_at`, a column that does not exist (the DDL has `expires_at` and requires
  `expired_reason`), and `SnapshotManifest.expired_at` has no source column.
- **Duplicate path.** §3 says `UPDATE … WHERE state = 'active'` → "0 rows → NOT_FOUND", which turns the duplicate
  attempt on a `deleting` document into `NOT_FOUND`. That is the N122 path that re-puts a missing intent; a client
  reading NOT_FOUND as "already gone" holds an unacknowledged delete with no intent.
- **Sequence drawn early.** §3 draws `nextval('outbox_seq')` before five more statements, so the drawn `seq` is not
  bounded by one `statement_timeout` as A-F1's 60 s gap horizon assumes. This is a regression from the P-15 fix. In
  practice the statements take milliseconds, so this part is unverified.

**Recommendation.** Keep one text, generated or copied from §3's SQL (with the state check and the duplicate branch),
and delete the others. Draw the event `seq` inside the final statement (`WITH s AS (INSERT INTO outbox … RETURNING
seq) UPDATE document_tombstones SET event_seq = s.seq …`, with `event_seq` nullable until then, or a deferred check).
Rename the column or the proto field so `expired_at` has a source.

### A-11: minor: the export `hidden_overlay` misses derived versions hidden by an Invalidate until Materialize runs (unbounded during a move), and the sync rule drops `root_version`

**Where.** D16 ("Exports honour curation … meanwhile, through the manifest `hidden_overlay`"); `export.proto`
`HiddenVersion` ("names a derived version that the read predicate hides now"), `HiddenOverlay`; §5.7 overlay and sync
step 6; §5.4.5 (Invalidate signals the expunge "so Materialize records the `derived_hidden` rows"); §5.4.2 (every
expunge activity "skipped while a move of the namespace is open").

**Evidence.**
- **Pre-Materialize gap.** The read predicate hides every observation or page version whose segment names an
  invalidated fact at commit (N135(3)). The overlay, however, lists only `derived_hidden` rows, which Materialize
  writes: within 15 min normally, and only after a move completes if one is open. Until then a synced agent serves
  observation text built from the invalidated fact, and the overlay only hides the fact row itself.
- **Root dropped.** Sync step 6 hides "`(kind, id, version ≥ from_version)`" without the `root_version` the proto
  requires. After the rebuild (new root), the next snapshot's version is over-hidden by the permanent row.
- **Unbounded overlay.** The overlay is every `invalidate` row and every permanent `derived_hidden` row in the
  namespace's history, re-sent on every poll. Only rows newer than the snapshot's start can matter, because the
  snapshot was built through the predicate.

**Recommendation.** Compute the overlay with the read predicate (the segment test) rather than from `derived_hidden`
alone, restricted to markers newer than the manifest's `as_of` plus `Restore`s. Put `root_version` in sync step 6
and in the exported observation and page records.

### A-12: minor [fix incomplete: r4 A-23]: `tenant.admin` is tenant-bound but served on a route "not exposed to tenants", and it covers `UpdateTenant` quotas and isolation

**Where.** `admin.proto` header ("a separate Envoy route that is not exposed to tenants"); §4.1.1 ("Admin API called
with a tenant token → `PERMISSION_DENIED`"); §4.7 route table; §4.1.9 (`tenant.admin`: "`TenantService` (except
`ListTenants`)"); `UpdateTenant` ("patches display_name, quotas, config and isolation"); §4.1.1 scope list for tenant
tokens.

**Evidence.**
- **Route contradiction.** If the route is closed to tenants, tenant-held `tenant.admin` cannot reach `DeleteTenant`
  or `GetTenantOperation`, and the tenant binding is moot. Separation then rests on routing again, which is what
  A-23 asked to remove.
- **Quota bypass.** If the route is open, a tenant administrator can raise its own `recalls_per_min` and
  `llm_tokens_per_day` and set `isolation = DEDICATED`, which consumes dedicated shards. Quota enforcement becomes
  advisory. Whether this is exploitable depends on who is issued `tenant.admin` (unverified).

**Recommendation.** Make `CreateTenant` and the quota and isolation paths of `UpdateTenant` `engram.operator`-only.
Leave the tenant's own lifecycle (`GetTenant`, `DeleteTenant`, `GetTenantOperation`, display name and config) under
tenant-bound `tenant.admin`, and route those on the tenant-facing listener. Fix §4.1.1.

### A-13: minor: `UpdateDocumentTags`, `ListTags` and `GetDocumentBody` exist only in the proto and §4; nothing builds, schedules or propagates them

**Where.** `document.proto`; §4.1.9, §4.7; N139 (A-22 fold); §2 (no method), §3 (no query or index for distinct tags),
§5 (no step), `events.proto` (no event), §10 (no milestone names them), §4.6 (no MCP tool); D6 ("written in the same
transaction as every state change").

**Evidence.**
- **No outbox event.** `UpdateDocumentTags` changes `documents.tags` with no event, which breaks D6. Kafka consumers
  never see it.
- **Export misses tag changes.** The export delta is keyed by `(id, version)`, and a tag change bumps no version, so
  synced clients keep the old tags and filter locally on them.
- **Retain interaction unspecified.** The document's tags are "the union of item tags of the current version", and
  the plan does not say whether a later `REPLACE`/`APPEND` overwrites or keeps tags set by `UpdateDocumentTags`.
- **`ListTags` has no plan.** It is an unnest-and-group over a namespace's documents on every page call.
- **Error code drift.** `UpdateDocumentTags` on `DELETING` returns `NOT_FOUND{DOCUMENT_DELETED}`, a use the
  `NotFound.reason` comment and the §4.1.6 table do not list.

**Recommendation.** Add a `DocumentTagsUpdated` event, give the export delta a `documents` part (or a tag
generation), state the retain rule, give `ListTags` a plan (a per-namespace tag-count table maintained in the same
transactions, or a bounded scan with a stated cost), and put the three RPCs in M1.3 or M1.6 with an effort.

### A-14: minor: the tombstone view has three gaps: a revived document lists its deleted life's versions, tag filters match every tombstone, and the SQL and proto state enums do not map

**Where.** N136(3); `document.proto` (`Document`, `GetDocumentRequest.include_versions`, `ListDocumentsRequest`);
`sql/shard_schema.sql` `document_state ('active', 'deleting', 'deleted')` against proto `DocumentState {ACTIVE,
INGESTING, DELETING}`; §4.0/§4.8 examples against §5.4.1 step 5.

**Evidence.**
- **Revived document.** After a revival the row is `active` again while the old tombstone is pending, and nothing
  says `GetDocument(include_versions)` filters `version ≤ up_to_version`. Those `DocumentVersion` rows carry
  `content_hash` (a fingerprint of deleted content), counts and `operation_id` until the purge. D16 promises "nothing
  derived from the document".
- **Tag filters.** Tombstones have `tags = {}`, so `ListDocuments(include_deleting, tag_filter{ANY|ALL, …})` returns
  every deleted document of the namespace (I = ∅ matches). This is harmless but surprising.
- **State enums.** SQL `deleted` has no proto value; proto `INGESTING` has no SQL value (presumably `active` with
  `current_version = 0`).
- **Examples.** The examples ack `OPERATION_STATE_PENDING`, while §5.4.1 inserts and returns `RUNNING`.

**Recommendation.** Filter covered versions out of every DocumentService path (the same `up_to` rule). State that
tombstones never match a non-empty tag or metadata filter. Generate both enums from one table (as N139 did for
`operations`). Fix the examples.

### A-15: minor: D16 and `memory.proto` disagree on whether `GetMemory` returns an invalidated fact

**Where.** D16 ("Invalidate/Restore: visibility flips at commit … for Recall, Reflect, GetMemory and ListMemories");
`memory.proto` `Invalidate` ("hidden from Recall, Reflect and ListMemories (GetMemory still reads it, with
`invalidated_at` set)"); `Memory.invalidated_at`; §5.4.1 ("`GetMemory` … apply the same predicate").

**Recommendation.** The proto behaviour (readable with `invalidated_at`) is the useful one for curation. Correct D16
and §5.4.1, and say whether `BatchGetMemories` reports an invalidated id as found or missing.

### A-16: minor: §7.4's spec-to-test pairing names six tests that §8 never defines, so the N46 manifest check pairs specs with tests that do not exist

**Where.** §7.4(a) table; §8.4.6 Go-twin column; §8.4.7; N46.

**Evidence.** These names are in §7 only:

| §7 name | §8 equivalent |
|---|---|
| `TestReplace_ThenDelete` (C-1) | `TestVisibility_AllSurfaces`, `TestExpunge_Stages` |
| `TestPage_CommitReverifies` (C-2) | `TestPageRefresh_DeleteMidCall` |
| `TestReextract_DerivedStaysVisible` (C-6) | `TestReextract_Rebuilds` |
| `TestMaterialize_BatchRereadsFactHidden` (C-7) | `TestExpunge_DerivationLock` |
| `TestMove_InsSeqReconcile` | `TestMove_ActiveBacklog` |
| `TestHNSW_RebuildOnly` | `TestIndex_Hygiene` |

`TestDeps_EveryRPCHasPath`, `TestReadSession_ConcurrentArms` (§2) and `TestReflect_MapReduceAtCap` (§10) are not in
§8 either.

**Recommendation.** One list. Generate §7.4's column from §8.4.6 (`make gen-docs` already owns similar tables).

### A-17: minor: Hindsight parity: the MCP surface is a fraction of Hindsight's, with no non-goal row

**Where.** §4.6 (10 tools); `reference/hindsight-notes.md` §9 (MCP tools: `list_memories`, `get_document`,
`list/get/cancel_operation`, `list_tags`, mental-model CRUD and refresh, `list/create/delete_directive`,
`get/update_bank`, …); §12.

**Evidence.**
- **Missing MCP tools.** Engram's MCP adapter has no `list_memories`, `get_document`, `cancel_operation`,
  `list_tags`, `invalidate`/`restore` (curation), page create/update/refresh or namespace config tools, although
  the RPCs exist.
- **Generation claim.** §2.2.25 says tools are "generated from `memory.v1`", which would expose all of them. The
  §4.6 table is "the golden list", which exposes ten.
- **Brief.** The brief's "read tools; write tools gated" is met; parity and the generator claim are not.

**Recommendation.** Either list the full generated tool set (with gates) in §4.6, or add a non-goal row naming the
omitted tools and an allow-list in the generator.

### A-18: nit: drift

- **Old package name.** §2.3 `reflect.Agent`, §2.5 `reflect.Caps` and §8.2 `internal/reflect` should use
  `reflectagent` (N140).
- **D17 effort figures.** D17 still reads Phase 0 15, Phase 1 45, Phase 2 17 + 3, Phase 3 24. §10, N142 and the
  front matter say 13.5 / 50 / 21 / 25 (84.5 committed, 123.5 total).
- **N38.** It still maps `purge` ↔ `DELETE_DOCUMENT`; the SQL kind is `delete_document`.
- **Index runner name.** R42 says `engramctl index run`; §2.2.27 says `engramctl index`.
- **Scope tables.** §4.1.9 lists `CancelOperation` under `memory.write` and "all OperationService" under
  `memory.read`.
- **Calendar.** E2's Phase 2 work starts on day 127, which is week 19, not "week 18".
- **Unreachable worst case.** The Reflect worst case (10 iterations × 10 k tool tokens) ends at a 101 k context,
  above the 100 k cap, so the map/reduce fallback triggers before that case is reachable.

---

## Checked and holding

- **Protos.** `buf lint`, `build` and `format` are clean. Against the last pre-D23 commit the public and admin modules
  changed only additively (0 breaks), and the 33 internal breaks are exactly the two round-4 rows of the §4.5
  exception table. Old event names are now reserved. The §4.2 full-text blocks are byte-identical to the files.
- **Delete contract at ack.** The tombstone view fields, `NOT_FOUND{DOCUMENT_DELETED}`, non-cancellable `DELETE_*`,
  the 40 s caps, `WaitOperation` exemptions, `DeleteTenant` bounded fan-out, the ack re-read (N143) and duplicate
  re-put are consistent across proto, §4, §5.4 and §2 (apart from A-5, A-10 and A-14).
- **Numbers recomputed and correct:**
  - **Critical path and rerank skip.** 2 + 25 + 60 + 30 + 1 + 90 + 3 + 5 = 216 ms; the skip reserve is 106 ms and
    the skip threshold 194 ms.
  - **Storage and fleet.** 181 GB per 10 M facts → 150 GB at 8 M and 230 GB at 12 M with the 5 % `ins_seq`
    indexes; 125 shards (84 at the hard cap), 3.9 → 4 cells.
  - **Throughput and fill.** 600 / (60 × 3.5) = 2.86 chunks/s per cell; 100 M chunks / 11.4 = 101 days; at
    A-F = 4, 4.9 chunks/s and 152 days. Temporal 58 events/s.
  - **Postgres budgets.** XIDs 54/s → 214 days; purge WAL 2.3–5.5 min per 100 k facts and 23–55 min per 1 M.
  - **Table 6.8-A.** Every row checks: extract $0.00066, batch $0.00063 (→ $0.0000785 per fact), Reflect $0.114
    typical and $0.342 worst.
  - **Table 6.8-B.** The haystack $0.236 (547 calls); LME-S $118.1 ($93.4 with batch), 273 k calls, 7.6 h;
    $0.1567 / $0.21 per 1 k facts; 1 B facts $157 k ($124 k with batch); LME-M $1,534.
  - **§10.** Phase sums 13.5 / 50 / 21 / 25 / 14; committed 84.5 (E1 28.5, E2 28.0, E3 28.0); overrun 6.5; round-4
    deltas +7.0 − 2.5; Gantt chains 202 / 197 / 197 days (MVP at day 167 = week 24, exit week 29).
- **Go API.** The stated import graph is acyclic. 96 of 97 interfaces are within five methods (A-7). Typed ids,
  `MemoryRef`, the `Starter`/`Signaller`/`Waiter` split and `ReadSession` are idiomatic and fit their named patterns
  (DAG plus pipeline for recall, forward-only workflow for the expunge, saga for the move).
- **Brief coverage.** Every required service, streaming choice, cross-cutting rule, adapter and deliverable section
  is present. The `REPLACE` departure, rerank depth and cross-document body dedup are recorded.

## Counts

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 3 | A-1, A-2, A-3 |
| minor | 14 | A-4 to A-17 |
| nit | 1 | A-18 |

## Verdict

**The contract is in good shape; the numbers and the schedule have three decision-level gaps.** The round-4 API
majors (tombstone view, export curation, derived purge) are closed in the proto, the public protos changed only
additively, and the cost, throughput and effort arithmetic recomputes exactly.

The majors are each a premise that N140, N114 or N141 changed without re-deriving what rests on it:
- **A-1, connections.** One connection per recall became N per recall, but the pool budget was not re-derived.
- **A-2, hot set.** Only half of the graph's index footprint is counted, which puts the 8 M decision where 10 M was
  rejected.
- **A-3, schedule.** M0.7's exit tests code that does not exist until Phase 1 and Phase 2, and a committed exit (M2.3)
  depends on the uncommitted converter.

The minors cluster again at the Go API seams (A-6 to A-9: missing methods, a six-method interface, depguard
violations, no read-side derived readers) and at prose/SQL seams (A-5, A-10, A-14). They need one generated source
rather than more hand edits. The `HEAD~1` baseline (A-4) and the tenant-admin route (A-12) are round-4 fixes that went
the wrong way.

## What I could not verify

- **Connection demand.** Real arm durations and the pgbouncer queueing behaviour (A-1 uses §1.5's budgets).
- **Hot-set behaviour.** Whether the link PK and heap pages are really hot under the temporal-family access pattern
  (A-2 uses the plan's own whole-relation accounting).
- **pg_search.** It was stubbed, as in earlier rounds.
- **TLA+ and Lean.** Neither was re-run (outside this lens).
- **`tenant.admin` issuance.** Who is issued `tenant.admin` (A-12).
- **Live Hindsight.** Behaviour beyond `reference/hindsight-notes.md`.
- **Twin interpretation.** Whether "Go twin" in §8.4.6 could mean a model test without the real package. If so,
  A-3's first half becomes a wording fix: §8.4.6 says "the fix disabled by a `faultinject` knob".
