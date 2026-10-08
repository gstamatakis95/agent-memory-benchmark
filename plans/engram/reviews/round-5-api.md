# Review round 5: API contract, Go API, numbers, completeness, Hindsight parity

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
