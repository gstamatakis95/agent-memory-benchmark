# Review round 6: API contract, Go API, numbers, completeness, Hindsight parity

Lens: proto and §4 (evolution policy and its PR baseline, RPC → interface coverage, MCP tool set,
`tenant.admin` vs `engram.operator`), §2 (five-method rule, import graph vs depguard, `ReadSession` and the
arm semaphore, typed ids, patterns), the derived numbers (6.5 M / 154 shards / 5 cells, hot set, 220 ms
path, connection budget with the N159 subject locks, §6.8 cost, fill, §10 effort), completeness against the
brief, Hindsight parity, and consistency of sections, register, proto, SQL and §7/§8 test names. Findings
already dispositioned in rounds 1 to 5 are not repeated unless the fix is incomplete or wrong.

## How the evidence was produced

- **buf 1.57.0**, executed. `buf lint`, `buf build` and `buf format -d --exit-code` in `proto/` all exit 0.
  `buf breaking` was run twice from the repository root. Against the round-5 tree (`.git#ref=7288fa5`) it exits 100
  with exactly the two lines of the `expired_at → expires_at` rename. Against the PR baseline §4.5 prescribes
  (`.git#branch=main,subdir=plans/engram/proto`) it fails (A-7).
- **Five-method rule.** A script parsed every `type X interface { … }` in the Go fences of §1 to §12, including
  one-liners, `;`-separated bodies and embedded interfaces. That is 116 interfaces, plus 4 one-liners written
  with aligned spacing (`Chatter`, `Fuser`, `Packer`, `Planner`). The largest has 5 methods. **The rule holds.**
- **Import graph.** A script collected every `pkg.Ident` reference inside each `package` block of §2 and
  compared the edges with the depguard table and the "edges are exactly" list. It found no cycle. The table
  gaps are in A-8.
- **RPC coverage.** The 54 RPCs in `proto/` (36 `memory.v1`, 18 `memory.admin.v1`) were mapped by hand to
  `api.Deps` interface methods (A-3).
- **Embedded protos.** The §4 "full text" blocks of `common.proto` and `memory.proto` were diffed against the
  files. They are identical.
- **Test names.** A script collected 308 distinct `Test*` names cited across the sections and the register,
  expanding the `_Suffix` shorthand. Every real name is defined in §7 or §8; the residue was shorthand that
  points to TLA+ configs. A second script checked every `Spec_Config` citation against `formal/tla/*.cfg`:
  one dangling citation (A-11).
- **Numbers.** Recomputed in Python, including Erlang-C for the arm semaphore.

## Findings

### A-1: minor [fix incomplete: r5 A-1 / N155]: the recall pool share "≤ 32 of 40" is not enforceable, because the workers draw on the same pgbouncer pool, and the 305 connection-ms per recall omits the observation arms

**Where.** N155; D3 cell row; §9.1 "Connections" row and the pgbouncer excerpt (`DEFAULT_POOL_SIZE: "40",
RESERVE_POOL_SIZE: "0"`); §9 hosts ("`shard-N-pgbouncer` (api and worker)"); §2.2.7 `ReadSession`
(`TestReadSession_ConcurrentArms` "≤ 320 connection-ms per MID recall"); §3 table row 8 ("observation arms
(semantic + lexical …)"); `common.proto` `ArmRank.arm` (lists `observation_semantic` and `observation_lexical`);
§2.2.13 `Arm.Name()` (`semantic | lexical | graph | temporal | chunks`).

**Claim.** "`engram_app` pgbouncer pool **40** (recall ≤ 32, ack/commit and Reflect ≤ 8) … a MID recall holds
≈ 305 connection-ms (four 60 ms arms, two graph waves, visibility) … pool wait ≤ 4 ms p95."

**Evidence.**
- **The 32/8 split has no mechanism.** API and worker both connect as `engram_app` to the same
  `shard-N-pgbouncer`, and pgbouncer has one pool per (database, user). The arm semaphore (16 per API process,
  × 2 = 32) bounds recall from the API side. Nothing bounds the workers' share: no worker client pool size is
  stated anywhere, and `RESERVE_POOL_SIZE` is 0.
  - The worker-side database load (`BuildLinks` semantic kNN per fact, `ResolveEntities` fuzzy lookups,
    consolidation `FindCandidates`, `CommitChunk`) scales with chunk rate.
  - Online that is ≈ 0.09 chunks/s per shard, which is negligible.
  - At the `RetainBackfill` peak that §9 sizes Temporal for (580 to 5,800 events/s per cell, ≈ 1 to 9 chunks/s
    per shard), ≈ 10 kNN probes per chunk at arm-like cost is several connections. That is before consolidation's
    candidate searches. Whether it passes 8 is unmeasured, and nothing stops it if it does.
- **The connection-ms figure counts four arms.** A recall with `include_observations` and `include_chunks` set
  runs semantic, lexical, temporal, chunks, plus the two observation arms.
  - That is ≈ 6 × 60 + 2 × 30 + 5 ≈ 425 connection-ms if the observation arms are their own transactions, as §2.1
    says every concurrently running arm is.
  - This is the configuration the IOPS budget sizes ("3 vector arms", facts, chunks and observations).
  - At 50 QPS that is ≈ 21 Erlangs, i.e. 10.6 per API process on 16 semaphore slots.
  - Erlang-C with a 60 ms hold gives P(wait) ≈ 0.09, wait p95 ≈ 6 ms and p99 ≈ 24 ms per arm transaction. At least
    two transactions are in series on the critical path (semantic, then graph wave 2).
  - So the "≤ 4 ms" term and `TestReadSession_ConcurrentArms`'s 320 connection-ms bound do not hold for that
    configuration.
  - The p95 < 300 ms SLO still holds, with ≈ +10 ms.
  - Unverified: whether the observation searches are separate transactions. `Arm.Name()` omits them; §3 and
    `ArmRank` name them as arms.

**Recommendation.**
- Give the workers their own pgbouncer database alias (for example `engram_worker`, `pool_size = 8`) so that
  "recall ≤ 32" is a pool fact, not an intent.
- State the arm set the budget is sized for, in N155, §2.1 and `Arm.Name()`. Either count the observation arms
  (≈ 425 connection-ms, semaphore 20 per process, pgbouncer 48) or fold them into the semantic and lexical arm
  transactions and say so.

### A-2: minor [regression: N159]: the session-level subject-lock connections are missing from the connection budget and have no credential

**Where.** §2.2.7 `SubjectLocker` ("a small pool: 2 per API process per shard, so ≈ 57 of max_connections 100
with N155's ≈ 53"); N155 and D3 ("≈ 53"); §9.1 Connections row (lists admin, move, relay, migrate, runner,
exporter and `engramctl`: "≈ 53"); §9.1 secrets table (`engram_app` DSN is "(pgbouncer)" only).

**Evidence.**
- **Budget.** The lease runs on a **direct** connection, because session locks do not survive transaction
  pooling. That is 2 × 2 API processes = 4 more server connections per shard. §2 counts them (57); N155, D3 and
  the §9 budget table that operators provision from do not (53).
- **Credential.** Only `engram_move` and `engram_relay` have direct DSNs. The subject lock needs `engram_app`
  (or another role) straight to `shard-N-primary`, and no such secret or `pg_hba` entry exists.
- **Pool exhaustion.** The lease is held across the marker transaction. For `DeleteDocument` that includes the
  exclusive document lock (one 35 s attempt); for `Restore`, the 40 s derivation lock. So two slow deletes on one
  shard occupy both slots of a process. What a third Invalidate/Restore/Delete does is unspecified: block on the
  pool, or fail like the 3 s `lock_timeout`.

**Recommendation.**
- Put "subject leases 2 per API process, direct" into N155, D3 and §9 (≈ 57 of 100).
- Add the direct `engram_app` DSN, or a dedicated `engram_subject` role with only `pg_advisory_lock`, to the
  secrets table.
- Specify pool exhaustion as an immediate retryable `UNAVAILABLE`, like the lock refusal.

### A-3: minor [fix incomplete: r5 A-6]: `MoveService.CleanupMove` has no interface method behind it, so the generated (RPC → method) coverage check fails as specified

**Where.** §2.2.1 `Deps` and the coverage-table comment; §2.2.19 `move.Orchestrator { Start, Status, Abort }`
and `Closer.Cleanup`; §2.2.3 `catalog.Moves`; §2.2.7 `AdminTx`; `admin.proto` `CleanupMove`; §5.5 step 8
("the last activity calls the `MoveService.CleanupMove` admin RPC, which has the index runner drop the
namespace's partial indexes … runs `engram_cleanup_namespace` in batches"); §8 `TestDeps_EveryRPCHasPath`
("every public and admin RPC maps to an interface method").

**Evidence.**
- **CleanupMove.** Every other admin RPC maps to a method:

  | RPC | Method |
  |---|---|
  | `DrainShard` | `Registry.SetShardState` |
  | `ReleaseNamespace` | `Namespaces.SetState` |
  | `ResolveNamespace` | `Namespaces.Resolve` |
  | `RollbackMove` | `Orchestrator.Abort` |
  | `GetMove` / `ListMoves` | `MoveReader` |

  `CleanupMove` does not:
  - `Orchestrator` has no cleanup method.
  - `catalog.Moves.Advance` can only set `done`.
  - No `AdminTx` accessor drops a moved-out namespace's indexes or runs `engram_cleanup_namespace`. `Purger` covers
    the expunge targets only.
  - The only `Cleanup` is `move.Closer.Cleanup`, a worker activity, and that activity is the RPC's *caller*. The
    call path is circular: worker activity → admin RPC on `engram-api` → "the index runner", which lives in the
    worker.
- **Weaker gaps of the same kind.** `GetSnapshotManifest(version ≠ 0)` has only `Streamer.Latest` (or a filtered
  `SnapshotLister.List`). `GetPage(name)` has only `pages.Reader.Get(id.PageID)`.

**Recommendation.**
- Add `Cleanup(ctx, m id.MoveID, o CleanupOptions) (*CleanupResult, error)` to `move.Orchestrator` (four methods).
  Its implementation checks the N159 gate on the catalog row and returns `CLEANING` while batches remain.
- Have the activity drive the batches itself (it already holds both shard handles), so the RPC only records
  `done`.
- Add `ByName` and `Manifest(v)` selectors to the reader interfaces, or state the `List` mapping in the
  coverage table.

### A-4: minor: workers call `engram.operator` admin RPCs, but no worker credential exists, and the only scope that would work is fleet-wide

**Where.** D4 ("every other catalog write a workflow needs … goes through an admin RPC on `engram-api`"); N36;
§5.4 (`ReleaseNamespace`), §5.5 step 8 (`CleanupMove`), §9 ("Restart … through the admin RPC"); §4.1.9
(`ShardService`, `MoveService` are `engram.operator`, "fleet-wide, own audience, separate Envoy admin route");
§9.1 secrets table; NG14 ("Engram verifies, it does not mint").

**Evidence.**
- **Missing secret.** The secrets table lists no JWT or mTLS identity for `engram-worker` to reach the admin
  route.
- **Scope.** The scope model offers only `engram.operator`, which also covers `StartMove`, `UpdateTenant`
  quotas and isolation, and `RegisterShard` across every cell. A compromised worker, or a bug in it, would hold
  fleet-wide operator rights, where it needs `ReleaseNamespace` and `CleanupMove` for its own cell.

**Recommendation.**
- Add a service identity: a narrow scope `engram.worker`, valid on exactly `ReleaseNamespace` and `CleanupMove`,
  bound to the worker's cell by an mTLS SAN or a JWT `cell` claim checked against the shard's cell.
- Add its secret and rotation to §9.1.

### A-5: minor [fix incomplete: r5 A-17]: the generated MCP surface does not close: two RPCs are neither allowed nor omitted, one tool name collides, and the core client lacks two services

**Where.** §4.6 (the ten hand-tuned tools, the generated table, the `omit:` list, "the build fails for an RPC
that is neither allowed nor listed under `omit:`"); NG46; §2.2.25 `CoreClient { Memory, Document, Page,
Operation }`.

**Evidence.**
- **Unaccounted RPCs.** Counting the 36 `memory.v1` RPCs:
  - 10 are hand-tuned;
  - 20 are in the generated table;
  - 4 are omitted (`Create/Update/DeleteNamespace`, `StreamSnapshot`);
  - that leaves `OperationService.GetOperation` and `NamespaceService.ListNamespaces` in neither list. By the
    stated rule, the build fails.
- **Name collision.** The hand-tuned `get_operation` maps to **`WaitOperation`**. A generated tool for
  `GetOperation` would take the same snake-case name.
- **Missing clients.** `CoreClient` has no `Namespace()` or `Export()` client. The generated `get_namespace`,
  `get_effective_config`, `create_snapshot`, `get_snapshot_manifest` and `list_snapshots` therefore have no client
  to call through.

**Recommendation.**
- Put `GetOperation` (as `get_operation_status`, or fold it into `get_operation` and list it under `omit:` with
  that reason) and `ListNamespaces` (omit: a per-namespace endpoint has no use for it) into the lists.
- Add `Namespace()` and `Export()` to `CoreClient`. That makes six methods, so split it, for example
  `DataClients` and `AdminClients`.

### A-6: minor [fix incomplete: r5 A-12]: the per-path split of `UpdateTenant` cannot be expressed by the interceptor's `MethodPolicy`, and operators cannot read or delete a tenant

**Where.** §2.2.2 `MethodPolicy { Scope; Target; Bucket; AllowDeleting }`, `Policy map[string]MethodPolicy`
(keyed by method); §4.1.1, §4.1.9 (`UpdateTenant`: `display_name`/`config` under `tenant.admin`;
`quotas`/`isolation`/`state` under `engram.operator`; served on both listeners); D13 ("the interceptor is the only
enforcement point").

**Evidence.**
- **Path split.** One method maps to one `Scope`, so the path-dependent rule must be coded in the `UpdateTenant`
  handler. That makes a second enforcement point, which D13 rules out, and the §8 table test
  `(claims, method, catalog entry) → code` cannot see it.
- **Operator lifecycle.** `GetTenant`, `DeleteTenant` and `GetTenantOperation` are tenant-bound `tenant.admin`
  only. An operator token carries no tenant, so it fails `token.tenant_id == request.tenant_id`. The control plane
  then cannot delete a tenant (offboarding, a legal erasure request) without a tenant-bound token, and NG14 says
  Engram mints none.

**Recommendation.**
- Split the RPC: `UpdateTenant` (tenant paths) and `UpdateTenantLimits` (quotas, isolation, state;
  `engram.operator`). Each stays one-method/one-scope, and the change is additive (the reduced `UpdateTenant` is a
  pre-1.0 semantic tightening, logged in §4.5).
- Let `engram.operator` (Target Operator) call `GetTenant`, `DeleteTenant` and `GetTenantOperation` on the admin
  route.

### A-7: minor [fix wrong: r5 A-4]: the PR baseline that §4.5 and `proto/README.md` prescribe fails to run

**Where.** §4.5 ("a pull request is compared with the merge target …
`buf breaking plans/engram/proto --against '.git#branch=main,subdir=plans/engram/proto'`"); `proto/README.md`
CI commands.

**Evidence (executed, buf 1.57.0, repository root).**

```text
$ buf breaking plans/engram/proto --against '.git#branch=main,subdir=plans/engram/proto'
Failure: Module "path: "plans/engram/proto"" had no .proto files
$ echo $?
1
```

`main` has no `plans/engram/` at all; the plan lives on `claude/engram-implementation-plan`. Every PR that
touches the protos therefore fails the gate, and nothing on this branch was ever gated against `main`. The real
repository hits the same case on the PR that first introduces `proto/`.

Holding: against the round-5 tree (`ref=7288fa5`), `buf breaking` reports exactly the two `expires_at` lines, as
§4.5 states.

**Recommendation.**
- Add a bootstrap rule: when the baseline subdir does not exist on the merge target, run lint and build only, and
  record "no baseline" in the job summary.
- Or compare plan PRs against the plan branch's own merge base (`.git#ref=$(git merge-base …)`).

### A-8: minor: the depguard table still rejects edges the design needs: `pages → recall`, and the leaves for every non-leaf row

**Where.** §2.1 dependency table and its rows: services, `internal/api`, `internal/workflows`; §5.3 step 2
(`GatherEvidence`: "`recall.Planner` over `source_query`"); §2.2.16 `pages.Writer.Refresh`.

**Evidence.**
- **`pages → recall`.** A page refresh gathers its evidence with `recall.Planner`. The services row allows
  `recall` only for `reflectagent`, and `pages.Writer.Refresh` takes no evidence argument, so `pages` must import
  `recall`.
- **Leaves.** The services and `api` rows list `errs` but not `id`, `pipeline`, `fsm` or `txn`, yet every service
  signature uses `id.Scope`, and `recall` uses `pipeline.Step`. Only the infrastructure row says "leaves". A
  depguard config generated from the table as written fails M0.1's vet.
- No cycle results from either fix, because `recall` imports neither `pages` nor any service.

**Recommendation.** Add `pages: recall` to the services row (no cycle), and "leaves" to every row.

### A-9: minor: the delete subject drops its class, contrary to N150(2) and the typed-id rule; a client-chosen `document_id` can alias a fact's or namespace's subject

**Where.** N150(2) ("The subject is `(class ∈ {document, memory, namespace, tenant}, id)`"); §2.2.7
`SubjectLocker.Lock(ctx, ns id.NamespaceID, subject string)`; `shard_schema.sql`
`engram_subject_lock_key(ns uuid, subject text)` (`hashtextextended(ns || ':' || subject, 2)`),
`deletion_log.subject_id text` ("document_id | memory_id::text | namespace_id::text | tenant_id"),
`deletion_log_subject_idx (namespace_id, subject_id, deleted_at DESC)`; §2 preamble ("one typed id per entity
… never a bare `string`").

**Evidence.**
- **Collision.** `document_id` is client-chosen (≤ 256 bytes), so a client may name a document
  `"0192…"`, the text form of one of its own facts' `memory_id`. Then:
  - `DeleteDocument("0192…")` and `Invalidate(0192…)` share one advisory key;
  - `SubjectLease.Latest` returns the other class's last entry;
  - help-previous re-puts that entry's intent;
  - `prev_operation_id` chains across the two subjects, which N150 and the `Durability` spec model as
    independent chains.
- **Impact.** No cross-tenant effect (the key includes `ns`). Within a namespace it causes spurious
  serialisation, and the replay's per-subject grouping and epoch guard compare entries of different classes. That
  case is outside what TLC checked.

**Recommendation.**
- Make the subject a typed value `id.Subject{Class, ID}`, encoded `class || ':' || id`, in the lock key, in
  `deletion_log` (a `subject_class` column, or the encoded `subject_id`) and in the index.
- Add the aliasing case to `TestIntent_ConcurrentCurationOneChain`.

### A-10: nit: register drift

- **D17** still reads "Phase 0 … 13.5 ew; Phase 1 MVP 50 … Phase 2 … 21 … committed 84.5 … (rev. D24: figures
  regenerated from §10, N156)". §10 and N158 give 12.5 / 50.5 / 21.25 / **84.25**. N158 supersedes D17, but D17's
  own "rev. D24, regenerated from §10" label is false, and D17 is what the roadmap's first line cites.
- The **Status** paragraph says D24 is "N144 to N157"; N158 and N159 exist.
- **D13**'s `engram.operator` coverage ("`ShardService`, `MoveService`, `ListTenants`") omits `CreateTenant` and
  the operator paths of `UpdateTenant` (N157).
- **D14**'s proto list omits `proto/engram/internal/errors/v1`.

### A-11: nit: stale citations and small inconsistencies

- **Dangling TLC citation.** §5.5 step 6 cites TLC `ShardMove_D5Order` for the (c)-before-(d) order. That
  configuration was removed in round 3 (`1c25dbe`), and no `formal/tla/*.cfg` of that name exists.
- **Calendar text.** §10.3: "E2's Phase 2 work starts on day 127 … while E1 is still in M1.5". By the Gantt,
  E1's M1.5 runs from day 68 to day 126.
- **Sink.** §2.2.13 calls `Sink` (`Batch`, `Stats`) a one-method interface; it has two methods.
- **Documents per fact.** §3.7 assumes 1 M documents per 10 M facts. Table 6.8-B fixes 25 documents per 1 k facts
  (250 k per 10 M; 163 k per 6.5 M-fact shard). Small bytes, but two figures for one assumption.
- **Link indexes.** The §3.7 table gives link indexes of 41 GB per 10 M facts. That is 32.8 GB at 8 M and 26.7 GB at
  6.5 M, not the ≈ 36 and ≈ 29 GB the hot set uses. The error is on the safe side.
- **Expiry reason.** `SnapshotManifest` exposes `expired` and `expires_at` but not `expired_reason` (`ttl`,
  `document_delete`, `namespace_delete`). The comment says listed expired versions let "a client learn why its
  chain is broken".

## Checked and holding

- **buf.** `buf lint`, `buf build` and `buf format` are clean. `buf breaking` against the round-5 tree reports
  exactly the documented rename (the internal module included). The rename is in the §4.5 changelog, and
  `reserved "expired_at"` is present.
- **Five-method rule.** Holds for all 120 interfaces (largest = 5). `DocumentTags` embeds `TagLister` plus
  `Update` (2).
- **Import graph.** The signature-derived edges match the named infrastructure edges
  (`router → {store, index, blob, catalog, telemetry}`, `blob → store`, `quota → {txn, gateway}`,
  `authz → {catalog, quota, config}`, `catalog → {fsm, config}`, `index`/`outbox → store`), with no cycle. Services
  take `id.Scope` and `id.Caller` (A-8 of round 5 fixed), apart from A-8 above.
- **RPC coverage.** All 54 RPCs have a method except `CleanupMove` (A-3). That includes the round-5 A-6 list
  (`NamespaceAdmin`, `OperationReader`, `FactLister`, `DocumentReader.Version`/`Bodies`, `DocumentTags`,
  `pages.Admin`, `SnapshotLister`, `Orchestrator`).
- **Scope table.** §4.1.9 agrees with the MCP gates and the Connect route table. Operator-only methods are
  refused for tenant tokens by scope, not only by route.
- **SQL against proto.** The `operations.kind` and `state` `CHECK` lists agree with `OperationKind` and
  `OperationState` (`DELETE_TENANT` derived from the catalog, not stored). `cancel_reason` matches
  `CancelReason`. `superseded_by` is a version number in both.
- **Embedded protos.** The §4 copies of `common.proto` and `memory.proto` are identical to the files.
- **Sizing.**
  - 1 B / 6.5 M = 153.8 → 154 shards; / 32 = 4.8 → 5 cells.
  - Footprint 181 × 0.65 × 1.12 = 131.8 GB, and 181 × 1.12 = 202.7 GB.
  - Hot set 0.8125 × 90 = 73.1 GB, with 7 to 23 GB of headroom against 80 to 96 GB of cache.
  - IOPS 10 k × 50 × 5 % = 25 k, plus the filtered arms at 20 % ≈ 12.5 k, so under 50 k.
- **Critical path.** 2 + 25 + 60 + 30 + 1 + 90 + 3 + 5 = 216, + 4 = 220 ms. The rerank reserve is
  90 + 3 + 5 + 8 = 106; the skip threshold at a 300 ms deadline is 194 ms; the pre-rerank path is ≈ 122 ms.
- **Connections (four-arm recall).** 305 connection-ms → 15.25 mean, ≈ 24.4 p99 (Poisson). Erlang-C on two
  16-slot semaphores at 7.6 Erlangs each gives P(wait) ≈ 0.006, so the wait is ≈ 0 at p95.
- **Cost (§6.8).** All recomputed and consistent:
  - $0.157 per 1 k facts;
  - $157 k per 1 B ($124 k with the batch API);
  - $1,020 per 6.5 M-fact shard;
  - LME-S $0.236 per haystack, $118 per run ($93 with the batch API), 7.6 h at 600 RPM;
  - Reflect $0.114 typical and $0.34 worst case (cumulative billing);
  - `calls_per_chunk` 3.475 to 3.625.
- **Fill.** 100 M chunks / (5 × 600 / 210) = 79.8 days. At A-F = 4: 250 M / 23.8 ≈ 121 days. (Per-cell vs
  per-organisation RPM is as unverified as in rounds 3 to 5.)
- **Effort (§10).**
  - Milestone sums: Phase 0 12.5, Phase 1 50.5, Phase 2 21.25 → 84.25 committed; Phase 3 25.25; F/B 14; total 123.5.
  - Per engineer: E1 28.0, E2 28.25, E3 28.0.
  - Gantt chains: 198 / 199 / 197 days; MVP on day 163 (week 24); Phase 2 exit on day 199 (week 29); overrun 6.25.
- **Cross-references.** Every `N…` id cited in the sections, protos and SQL exists in the register. Every
  `Test*` name cited in §2, §5, §9 to §11 and the register is defined in §7 or §8. Every TLC configuration on
  disk is cited.
- **Completeness and parity.** Every deliverable of the brief has a home. The Hindsight features that are
  absent (memory defense, webhooks, import/clone, memory history, `retry_operation`, `clear_memories`, prompt
  preview, entity results, bank aliases) each have a non-goal row (NG7, NG10, NG12, NG36 to NG40, NG43).

## Counts

| Severity | Count | Ids |
|---|---|---|
| blocker | 0 | — |
| major | 0 | — |
| minor | 9 | A-1 to A-9 |
| nit | 2 | A-10, A-11 |

## Verdict

**The API contract, the Go API and the derived numbers hold after round 5.** The protos lint, build and pass
`buf breaking` against the round-5 tree with only the documented rename. The five-method rule and the acyclic
import graph hold mechanically. Every sizing, latency, cost, fill and effort figure recomputes.

What remains is local:
- three fixes of round-5 findings that stop one step short:
  - `CleanupMove` coverage (A-3);
  - MCP list closure (A-5);
  - the `UpdateTenant` split (A-6);
- the PR baseline command that does not run (A-7);
- the connection budget, where the worker pool is unbounded and the subject leases are uncounted (A-1, A-2);
- small typing and depguard gaps (A-8, A-9).

None needs a new mechanism. The plan is ready for implementation once these rows are folded.

## What I could not verify

- **`buf breaking` in the real repository.** I could not run it under the real repository layout (`proto/` at
  the root, with a release tag as baseline).
- **Worker database time.** No measurement exists of worker-side database connection-time per chunk at the
  `RetainBackfill` peak (A-1).
- **Observation arms.** Whether the observation arms run as their own transactions; §2 and §3 disagree (A-1).
- **Gateway rate limit.** Whether the 600 RPM cap is per cell or per organisation (carried from rounds 3 to 5).
- **Hindsight live.** No side-by-side against a running Hindsight instance; parity was judged from
  `reference/hindsight-notes.md` only.
