# Round 8 — correctness of protocols and invariants (after D26, N169–N178)

Lens: what D26 changed. That means the move backup inside the freeze and "post-commit target restore = ordinary
restore" (N169), the cleanup gate at `committed → cleaning` (N170), the replicated-ack rule (N171), the owner-row
epoch (N172), the document lock and one tag per subject (N174), and how faithfully `ShardMove.tla` and
`Derivation.tla` model these.

**What was executed.**
- (a) The N171 helper `catalog_replicated`, exactly as in `catalog_schema.sql`, on a scratch PG 16 primary with a live
  streaming standby (`/tmp/engram-r8c`, now stopped). The owner was a non-superuser `catalog_migrate`, and the helper
  was called as `catalog_admin` and as a superuser.
- (b) `shard_schema.sql` applied unmodified (pg_search stubbed) to my own database `r8c_shard`. On it I walked the
  N169 ordinary-restore path for a restored move target: `plan_target` → `freeze_restore` from `incoming` →
  `restore_done`. Then `freeze_delete` and `start_move`.
- (c) TLC on a scratch copy of `ShardMove.tla` with three design-faithful additions. All other state stayed as
  committed. The comparison run used the committed spec and the same small configuration (`_ActiveWriters` constants,
  1 row, 1 client, `MaxCat = 1`, invariants including `OneOwner`):
  - the reconcile on a promotion that loses nothing (N163(1): "every promotion");
  - N172's second sentence (a re-derived `committed` sets `namespaces = (target, target_epoch)`);
  - §5.5.1's "(c) finds `namespaces = (target, ≥ e+1)` ⇒ the mover ends `done`".

Nothing in the plan tree was edited.

## Findings

### C8-1 — major [regression] — The post-commit "ordinary restore" of a target restored before (b″) leaves the move open forever: `move_id` is never cleared and `activated_at` is never set

*Location.*
- N169(2) and N169(4).
- §5.5.5 step 1.
- §8 `TestMove_TargetRestoredAfterMoveBackup` ("the mover ends `done` (it treats a target row that is not `ready` as
  (b″) and (d) done by the reconcile)").
- `shard_schema.sql` `ownership_transitions` (`freeze_restore` from `incoming`/`ready`, and `restore_done`, both
  `move_effect = 'none'`).
- `catalog_schema.sql` `catalog_check_move_transition`.

*Claim.* "a target restored or failed over after (a″) is always the ordinary restore … `restore_done` … activates at
`e_t + 1`" (N169(2)).

*Evidence (executed, `r8c_shard`).*
1. An `incoming` row (move_id `…aa`) taken through `freeze_restore` and then `restore_done` ends as `active/3` with
   `move_id = …aa` still set.
2. `freeze_delete` as `engram_app` then fails with `ERROR: namespace … has an open move` (55006, N177's guard). So
   `DeleteNamespace` answers `NAMESPACE_BUSY` forever, and `TenantDelete` retries the fence forever.
3. `start_move` (move_effect `open` requires `o.move_id IS NULL`) is refused, so the namespace can never be moved
   again.
4. No edge clears `move_id` from `active`: only `activate_target` (`ready → active`) has `close`.

On the catalog side:
- `activated_at` is stamped only by the mover's (b″) `ActivateTarget`, and the restore path bypasses (b″).
- The trigger refuses `committed → cleaning` while `activated_at IS NULL`, and no `committed → done` edge exists.
- So the move row stays `committed` permanently. That blocks every later move of the namespace (the partial unique
  index on live moves), keeps N169(5)'s PITR floor armed forever, and the source is never cleaned.

The same holds for a deliberate PITR after `done` to a point between the move backup and (b″), which N169(5)
explicitly permits: `move_id` is restored as set.

*Why the spec misses it.* `ReconcileDesign(tgt)` sets `cm' = "done"`, `mp' = "done"` in one step and has no
`move_id` or `activated_at`. `Cleanup` then needs only `mp = "done"`, so `MoveTerminates` passes.

*Recommendation (decision level).* The restore path that activates a move target must perform the activation's
side effects:
- `restore_done` from a row whose `move_id` is set uses an edge with `move_effect = close`;
- the reconcile stamps `activated_at` (or the gate uses `coalesce(activated_at, restore_done_at)`);
- the catalog row must reach `cleaning`/`done` through the ordinary gate.

Add a must-fail spec knob (the restore path without `close`) checked against a new invariant "`mp ∈ Final ⇒` the
owner row has no move".

### C8-2 — major [regression] — N172's re-derived routing plus the mover's "(c) finds the catalog at the target ⇒ end `done`" leaves a `ready` target that nobody activates

*Location.* N172 ("When the reconcile re-derives `committed` from a source `moved_out`, it also sets `namespaces =
(target, target_epoch, active)` exactly as (d) would"); §5.5.1 (c) ("If the reconcile already did (b″) and (d)
(`namespaces = (target, ≥ e + 1)` …), the mover ends `done`"); N169(4).

*Interleaving.*
1. The mover takes (c): the source is `moved_out`. The activity commits, but the worker dies before Temporal records
   the completion (ordinary at-least-once).
2. The catalog primary fails, and the promotion runs the reconcile. That runs on every promotion, not only one that
   lost an outcome.
3. The reconcile reads source `moved_out` and target `ready`, and per N172 sets `namespaces = (target, e+1, active)`.
4. The retried (c) activity finds `namespaces = (target, ≥ e+1)` and ends the workflow `done`.

Nobody runs `activate_target`: the catalog promotion reconcile writes only the catalog. Every request now routes to a
`ready` row and gets `NamespaceNotReady` forever. "cutover in progress > 2 s" does not fire, because (d) looks done.
A full outage of the namespace that lasts until an operator steps in.

*Evidence (TLC, executed).* The scratch spec adds `CatalogPromote` (a promotion that loses nothing and sets
`cdirty`), N172's routing sentence in `ApplyReconcile`, and `CutRetryShortcut` (the §5.5.1 rule at `mp = "cut"`).
- Result: `OneOwner` is violated in 16 states:
  `… MoveBackup, MakeReady, CommitCAS, Replicated, Cut, CatalogPromote, ReadShard, ReadShard, ApplyReconcile,
  CutRetryShortcut`, ending with `own = [s2 ↦ ready, s1 ↦ moved_out]`, `cat = (s2, 2)`, `mp = cm = "done"`.
- The committed spec with the same configuration passes (6,427 states).

*Why the spec misses it.* `ApplyReconcile` leaves `cat` unchanged when there is no owner, so N172's routing write is
not modelled. `CatalogLoss` is the only promotion and is enabled only for an unreplicated outcome, so no reconcile ever
runs after (c). `Cut`/`Activate` have no catalog shortcut.

*Recommendation.*
- Make (b″) unconditional: `ActivateTarget` runs whenever the target row reads `ready` and the move reads
  `committed`, whatever `namespaces` says.
- Make the end-of-move test read the target row (`active` or a restore in progress), never the routing row.
- Model the no-loss promotion and N172's routing write in `ShardMove.tla`.

### C8-3 — major [regression] — A target *failover* to a lagging asynchronous standby is not bounded by the move backup, yet is treated as "always at or after a complete copy"

*Location.*
- N169(2)(5), and §5.5.5 step 1 ("the move backup precedes the commit point, so the restored row is `incoming`,
  `ready` or `active` at or after a complete, indexed copy").
- §9.3 failover (`engramctl shard failover N` promotes the `ha` profile's asynchronous standby).
- §9.1 (shards have no synchronous standby).
- `ShardMove.tla` `RestoreCore` ("to the last backup or lossless failover").

*Claim.* "every restore of the target to its latest archived point lands at or after a complete, verified, indexed
copy" (N169(2)).

*Evidence.* The floor of N169(5) guards only `engramctl restore --target <time>`. A failover promotes whatever the
standby has replayed, and its lag is unbounded: the plan's ≤ 60 s is an RPO assumption, not a check. The freeze
writes the copy (paced) and then the HNSW builds (a large, fast WAL burst; standby redo is single-threaded) just
before `MoveBackup`. The backup is taken from the primary, and "complete and archived" says nothing about the
standby's replay position.

*Interleaving.*
1. The standby lags more than (backup duration + consumer wait + cutover).
2. The target host dies after (a″).
3. `shard failover` promotes a standby whose namespace row is `incoming` with a partial copy and no valid index.
4. Step 2 sees `committed` and takes the ordinary restore. It activates the partial copy at `e_t + 1` with no
   row-count or index check (`ready_target`'s preconditions are bypassed by `freeze_restore`/`restore_done`).
5. `reconcile_out` already made the source `moved_out`. Cleanup 24 h later deletes the only complete copy.

The result is the loss of rows acknowledged long before the move, outside any stated RPO, and a violation of
`ServedFromIndex`.

The spec cannot express this. A lossy fault reverts to `bak[s]`, which after `MoveBackup` is never older than the
move backup, so "lagging failover" is the D26 analogue of T7-1. §8's "promote a lagging target standby" assumes the
conclusion.

*Recommendation.*
- Record the move backup's stop LSN (`move_backup_lsn`).
- `shard failover` refuses to promote a standby whose `pg_last_wal_replay_lsn()` is below the `move_backup_lsn` of any
  non-terminal move-in (and, after `done`, of a move-in within the last 24 h). It first lets the standby replay the
  archived WAL (`restore_command`, the WAL is archived by N169(1)), or takes `--lose-move-ins`.
- Alternatively, (b′) waits until the target standby has replayed `move_backup_lsn`.
- Use the LSN, not `move_backup_at`, for the PITR floor too (C8-9).
- Model failover as "any snapshot not older than the floor" in `RestoreCore`.

### C8-4 — major [fix wrong; regression of PG7-2's fix] — `catalog_replicated` returns false forever as specified, because its owner cannot see `pg_stat_replication`

*Location.* `catalog_schema.sql` (helper `SECURITY DEFINER`, owner `catalog_migrate`, no grant of `pg_read_all_stats`
to anyone); N171(1); §9.1 `config lint` ("checks … that `catalog_admin` holds no `pg_read_all_stats`").

*Evidence (executed).* With a live streaming standby:

| Who calls | Helper owner | `catalog_replicated('0/1')` |
|---|---|---|
| `catalog_admin` | non-superuser `catalog_migrate` | **f** (`pg_stat_replication` shows 1 row with `state` NULL) |
| superuser | non-superuser `catalog_migrate` | **f** (a definer function runs as the owner) |
| `catalog_admin` | `catalog_migrate` after `GRANT pg_read_all_stats TO catalog_migrate` | **t** |

The plan makes the owner roles explicit non-superusers (`engram_migrate` is described with `BYPASSRLS`;
`catalog_migrate` is "owner of every object"). Every arbiter action waits on this helper: (c), thaw, unready,
rollback and `reconcile_out`. So do every client-acknowledged lifecycle write (`CreateTenant`, `CreateNamespace`,
`DeleteTenant`, `StartMove`, `CleanupMove`, catalog idempotency rows).

As written, no move ever passes (a″); every frozen namespace pages `MoveFrozenPastDeadline` and stays read-only, since
no rollback is allowed after (a″). All catalog lifecycle RPCs return `UNAVAILABLE` forever. The lint checks the wrong
role.

*Recommendation.*
- `GRANT pg_read_all_stats TO catalog_migrate` (or `pg_monitor`), or own the helper by a dedicated monitoring role.
- `config lint` asserts that the helper's *owner* holds it and that the helper returns true against the live standby.
- `TestCatalog_ReplicatedHelperAsAdmin` must run with the production owner (non-superuser), not as the bootstrap
  superuser.

This is a one-line fix, but as specified the guarantee and liveness of N171 are both false.

### C8-5 — major — An acknowledged `DeleteTenant` is lost by a catalog restore from backup; the reconcile has no rule that repairs it

*Location.* N171(3) ("a catalog restore from backup (RPO 60 s) remains the only lossy catalog path and is repaired
from the shards by the reconcile"); §9.1 delete intents (a tenant delete "acks after the catalog `deleting` row and a
tenant-level intent (namespaces are fenced asynchronously …)"); §5.5.5 step 4 and §9.3 step 5 ("skipping namespace and
tenant intents whose catalog row is not `deleting` or `deleted`").

*Interleaving.*
1. `DeleteTenant` commits `tenants.state = deleting`, waits `catalog_replicated`, puts the tenant intent and acks.
2. Within the catalog's 60 s RPO, both catalog nodes are lost before `TenantDelete` has fenced the namespaces. N171
   covers a promotion, not this.
3. The catalog is restored from backup, and the reconcile from shards finds no `frozen/delete` row, because the
   fences are asynchronous.
4. The tenant is `active` again and its namespaces serve. A later shard restore skips the per-namespace intents
   because the catalog row is not `deleting`.

The acknowledged delete is resurrected. The plan states RPO 0 for acknowledged deletes, and `Durability.tla`
already protects the replay floor against exactly this catalog restore (`_CatalogLossNoBlobFloor`). The reconcile
reads no intents, so "repaired from the shards" is false for a tenant delete in its fencing window and for
catalog-only acknowledged writes.

*Recommendation.* The catalog-restore reconcile lists `_control/deletes/` tenant-level (and namespace) intents newer
than the backup's point minus a margin and re-establishes `deleting`, the same way the replay floor is derived from
`_control/restores/`. Alternatively, `DeleteTenant` acks only after every namespace is fenced. Either way, state which
catalog-only acknowledged writes (`CreateTenant`, limits) a catalog restore may lose and that clients must retry.
Add a `Durability` must-fail case (catalog restore inside the fencing window).

### C8-6 — minor (unverified) — A shard restore's "catalog first" epoch write is an un-awaited internal step-write; a promotion between it and the shard rewrite leaves the catalog one epoch behind the shard. The spec's `~cdirty` gating hides this.

*Location.* §5.5.5 step 2 and §9.3 step 4 (catalog `epoch = e + 1, restoring` first, then the shard rows); N171(3)
("internal step-writes … do not wait: they are re-derived from the shards"); `ShardMove.tla` (`RestoreCore` and
`ReconcileDesign` require `~cdirty`; "a restoring shard reads as no owner").

*Interleaving.*
1. The restore writes the catalog `(S, e+1, restoring)`, which is not yet replicated.
2. The catalog primary dies, and the promotion's reconcile reads the shard. The shard is either restricted by
   `listen_addresses` (unreadable) or still at `e`.
3. The catalog ends at `(S, e)`.
4. The restore continues from its last completed step (it is "resumable", so step 4 is not redone): it rewrites the
   shard rows to `e+1` and reaches `restore_done` at `e+1`.
5. Step 9 sets the catalog `active`, but the catalog epoch stays `e`. Every request at `e` is fenced, and
   re-resolution returns `e` again. All ≈ 120 namespaces of the shard are unroutable until someone re-runs the
   reconcile.

The design also does not say what the reconcile does with a shard it cannot read: wait (the promotion blocks for the
whole restore) or skip.

The spec forbids exactly this overlap (`~cdirty` on `RestoreCore`/`ReconcileDesign`, and the catalog-first write
folded into the atomic `ReconcileDesign`). The §7.2.2 remark that "a shard's own reconcile never waits on the catalog's"
contradicts the `~cdirty` guard on `ReconcileDesign`.

*Recommendation.*
- The restore's catalog writes wait for `catalog_replicated`; restores are rare.
- `restore_done` re-asserts the catalog epoch as `max(catalog, shard)` by CAS.
- State the reconcile's rule for an unreadable shard.
- Drop the `~cdirty` guard on the shard-side restore actions in the spec.

### C8-7 — minor — `restore cleanup-moved-out` runs only for `done`; a source restore that completes while the row still reads `cleaning` leaves a resurrected source copy permanently

*Location.* N170(2); §5.5.1 step 10 (the activity loops `engram_cleanup_namespace` until it returns 0, then
`CleanupMove` records `done`).

*Interleaving.*
1. The last batch returns 0.
2. The worker crashes before `CleanupMove`.
3. The source is restored to a pre-cleanup point, and `restore_done` skips cleanup because the row is `cleaning`.
4. The retried workflow calls `CleanupMove` and records `done` without re-running the batches.

Nothing reads a `moved_out` row, but `return_move` refuses while `documents`/`ingest_ledger` rows exist. The namespace
can never move back to that shard, and the space is never reclaimed. The same applies when a catalog restore reverts a
`done` move to `committed`: the reconcile maps `moved_out` to "at least committed", and no actor drives such a row.

*Recommendation.* `CleanupMove` re-runs `engram_cleanup_namespace` once (expecting 0) in the same activity attempt that
records `done`. Alternatively, `restore cleanup-moved-out` covers `cleaning` too: its predicate deletes are idempotent,
and the D26 argument that a premature cleanup is harmless applies.

### C8-8 — minor — The reconcile's "unique owner" is undefined for a torn snapshot, and (d)'s idempotent success does not match N169(4)

*Location.* N163(2)/§9.6 ("derived from the unique `active`/`frozen` row"); `ApplyReconcile` (`CHOOSE s ∈ owners`);
`02-modules.md` `Cutover` ("success also when already (target, e + 1)") vs N169(4) (`≥ e_t`).

*Evidence.*
- `ReadShard` per shard lets the mover take (c) and (b″) between the two reads, so the snapshot holds source
  `frozen@e` and target `active@e+1`. The design names no tie rule; the spec picks arbitrarily. Picking the source
  with N172's max-over-owners epoch (`e+1`) would produce `(source, e+1)`, which matches no row.
- After a restore path at `e+2`, (d)'s "already `(target, e + 1)`" test is false. If the mover's (d) is reached, it
  "retries forever".

*Recommendation.* The reconcile prefers the higher-epoch owner and re-reads on disagreement. (d)'s success becomes
"`namespaces.shard = target ∧ epoch ≥ e + 1`".

### C8-9 — minor — The PITR floor compares a catalog clock (`move_backup_at = now()` on the catalog) with shard commit timestamps

*Location.* N169(5).

*Evidence.* `--type=time` recovery stops by the shard's WAL commit timestamps. Clock skew between the catalog host and
the shard host (no bound is stated) can admit a target that ends before the copy's last commit or the index build.

*Recommendation.* Floor on the recorded backup stop LSN (`--type=lsn` or a check against it). See C8-3.

### C8-10 — minor (unverified) — The restore path never sets `moved_in_at`

*Location.* `shard_schema.sql` (the trigger sets `moved_in_at` only on `ready → active`); N147's 10-minute guard in
`engram_consolidation_watermark` and `BeginSnapshot`.

*Evidence.* A target activated by `freeze_restore`/`restore_done` (C8-1's path) skips the guard against a sequence
ring with no sample at or above `W_final`. I did not construct a wrong watermark.

*Recommendation.* `restore_done` from a row with `move_id` set also stamps `moved_in_at` (same fix as C8-1).

### C8-11 — minor [regression, doc] — §3.2 still states the pre-D26 rules

*Location.* `03-data-model.md:81`.

*Evidence.* It reads "epochs rise to the maximum over the shard rows" (contradicts N172) and "the one irreversible
step, move step (c), waits for the standby's `replay_lsn`" (contradicts N171's symmetric rule and the helper).

*Recommendation.* Generate the text from N163(2)/N171/N172, as N177 does for tests.

### C8-12 — nit — `Derivation.tla`'s `LazyTwinRead` reads the tag of the hidden old fact (`itag[f]`), whereas the design's `CommitChunk` reads the last `curation_log` invalidate row of the subject

*Evidence.* The two are equal only because of N174's tag reuse. The model has no state in which the subject's live
facts are all purged while `curation_log` still names the tag. That state is reachable in the design through a
REPLACE purge followed by re-ingest of the same content.

*Recommendation.* Model `curation_log` as a per-subject last-action variable, or state the equivalence as an
invariant.

## Spec deviations, judged

| Deviation (§7.2.2) | Verdict |
|---|---|
| `NoLossNoDup` restricted to moved rows (`actSet ⊆ frozenSet`) | Sound for the stated property. |
| Catalog steps gated on `~cdirty` (no one touches the catalog during the reconcile) | Sound for actors that use the alias. It hides C8-6, because shard restore actions are also gated, and the design has no such gate. |
| A restoring shard reads as no owner | Contradicts N172, which counts `frozen/restore` as an owner. Together with the gate above, it hides C8-6. |
| Promotion only through `CatalogLoss` (an unreplicated outcome) | Hides C8-2: the design reconciles on every promotion, and N172 writes routing. |
| `ReconcileDesign(tgt)` sets `cm = done` atomically | Hides C8-1. |
| Lossy fault = revert to the latest `bak` | Hides C8-3, a lagging failover. |

The three new must-fail configurations do exercise the mechanisms they name.

## Counts

0 blockers, 5 majors (C8-1, C8-2, C8-3, C8-4, C8-5), 6 minors (C8-6 to C8-11), 1 nit (C8-12). Four of the majors are
D26 regressions or wrong fixes (C8-1 to C8-4); C8-5 is an incomplete PG7-4 fix.

## Verdict

D26's central idea holds in every interleaving I tried:
- the move backup before the commit point;
- no re-run and no merge;
- the symmetric replicated wait for arbiter outcomes;
- tag reuse under the document lock.

No safety violation was found on the pre-commit path or in curation. The defects are all on the path D26 made
"ordinary":
- the restore-activated target never closes its move (C8-1, executed);
- the routing re-derivation strands a `ready` target (C8-2, TLC);
- failover is not held to the backup floor (C8-3);
- the helper every replicated wait depends on cannot work as specified (C8-4, executed);
- a catalog restore can still undo an acknowledged tenant delete (C8-5).

Each fix is small and local, but the D26 plan should not be called complete until C8-1 to C8-4 are applied and the
spec gains the no-loss promotion, N172's routing write, and a restore path that carries `move_id`/`activated_at`.

## Not verified

- C8-3's lag magnitude: standby redo rate for HNSW-build WAL against the incremental backup duration.
- C8-6's behaviour of the reconcile against an unreadable restoring shard; the plan does not say.
- C8-10's concrete watermark error.
- The `Derivation.tla` and full `ShardMove.cfg` runs. I did not re-run the large configurations; the
  `results/*.log` match `EXPECT`.
- Whether Temporal history would in fact replay (c) as a retry in C8-2. At-least-once activities make it possible;
  the plan's step text makes it reachable.
