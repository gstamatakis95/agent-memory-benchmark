# Review round 4: API contract, Go API, numbers, completeness, Hindsight parity

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
