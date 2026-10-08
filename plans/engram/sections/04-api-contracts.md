## 4. API contracts

The `.proto` files are the source of truth for every Engram surface: the generated Go stubs
(`gen/go`), the gRPC server, the ConnectRPC/JSON adapter and the MCP adapter are all derived
from them, never hand-written against them. This section fixes the conventions every method
obeys (4.1), inventories the files and their key semantics (4.2), pins down the two semantics
that are easy to get subtly wrong — tag matching (4.3) and `as_of` (4.4) — states the evolution
policy that CI enforces (4.5), maps MCP tools (4.6) and REST routes (4.7) onto gRPC methods, and
ends with worked calls (4.8). It opens with the four calls every client makes (4.0).

All files live under `plans/engram/proto/` (in the real repository: `proto/`, decision D14):

```
proto/
├── buf.yaml                                  # v2 workspace: 2 modules, lint STANDARD, breaking FILE
├── buf.gen.yaml                              # protoc-gen-go, protoc-gen-go-grpc, protoc-gen-connect-go → gen/go
├── README.md                                 # file list + CI commands
├── memory/v1/{common,errors,operation,memory,document,namespace,export,page}.proto
├── memory/admin/v1/admin.proto
├── engram/internal/errors/v1/errors.proto    # write-path and routing details that never reach a caller (N128)
├── engram/internal/workflow/v1/workflow.proto
└── engram/internal/events/v1/events.proto
```

`buf lint` (STANDARD, no rule disabled) and `buf build` pass with zero findings; `buf format -d
--exit-code` is clean. The only imports are the well-known types buf ships built in, so the
workspace builds offline with no BSR dependency (a deliberate constraint: CI must not depend on
buf.build being reachable to compile the contract).

### 4.0 Using the API

Four calls cover most clients: **Retain** (write), **Recall** (read), **DeleteDocument** (forget)
and **WaitOperation** (wait until a write or an expunge has finished). All examples assume
`api.engram.local:8443`, a token in `$JWT` (scopes `memory.read memory.write`), tenant `acme`,
namespace `018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e`, and `grpcurl` pointed at the proto files. Every
call needs a **deadline** (`-max-time`, 4.1.2) and carries the namespace in the **body**
(`namespace.tenantId`, `namespace.namespaceId`, 4.1.1). Fields not listed are optional.

| Call | Minimum request | What the response means |
|---|---|---|
| `MemoryService.Retain` | `namespace`, `items[]{content, timestamp}` | Durable acceptance only: one `Operation` per document (`PENDING`). Nothing is visible yet. |
| `MemoryService.Recall` | `namespace`, `query` | Stream of ranked `result` messages, then one `stats`. Sees everything whose `Operation` is `SUCCEEDED`, never a deleted document. |
| `DocumentService.DeleteDocument` | `namespace`, `documentId` | The ack **is** the delete: the marker is committed and its intent object durable, and no read path returns anything derived from the document from this response on (`DocumentService` itself shows only a content-free tombstone). The returned `Operation` tracks the physical expunge. |
| `OperationService.WaitOperation` | `namespace`, `operationId` | The operation, `timedOut = true` when it is not terminal yet. After a retain is `SUCCEEDED`, Recall sees its facts unless `result.supersededBy` is set. |

```bash
# 1. Retain: minimum request (content + timestamp). Deadline 10 s.
grpcurl -H "authorization: Bearer $JWT" -max-time 10 \
  -import-path plans/engram/proto -proto memory/v1/memory.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "items":[{"content":"Alice is moving to the search team on April 30th.",
                 "timestamp":"2026-03-10T15:04:05Z"}]}' \
  api.engram.local:8443 memory.v1.MemoryService/Retain
# -> {"operations":[{"id":"01925c1e-…","kind":"OPERATION_KIND_RETAIN_DOCUMENT","state":"OPERATION_STATE_PENDING",
#      "targetId":"0192…"}],"documentIds":["0192…"]}      (a document id is minted; pass operationId for a replay-safe one)

# 2. WaitOperation: block until the retain is terminal (server wait 30 s, deadline 35 s).
grpcurl -H "authorization: Bearer $JWT" -max-time 35 \
  -import-path plans/engram/proto -proto memory/v1/operation.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "operationId":"01925c1e-…","timeout":"30s"}' \
  api.engram.local:8443 memory.v1.OperationService/WaitOperation
# -> {"operation":{"state":"OPERATION_STATE_SUCCEEDED","result":{"documentVersion":"1","factsWritten":1}},"timedOut":false}

# 3. Recall: minimum request (query). Deadline 3 s; the last message is the stats.
grpcurl -H "authorization: Bearer $JWT" -max-time 3 \
  -import-path plans/engram/proto -proto memory/v1/memory.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "query":"which team is Alice on?"}' \
  api.engram.local:8443 memory.v1.MemoryService/Recall
# -> {"result":{"rank":1,"memory":{"kind":"MEMORY_KIND_FACT","text":"Alice is moving to the search team on April 30th."}}} … {"stats":{…}}

# 4. DeleteDocument: the ack is the delete. Deadline 40 s (delete-class cap, 4.1.2).
grpcurl -H "authorization: Bearer $JWT" -max-time 40 \
  -import-path plans/engram/proto -proto memory/v1/document.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "documentId":"session-2026-09-30"}' \
  api.engram.local:8443 memory.v1.DocumentService/DeleteDocument
# -> {"operation":{"kind":"OPERATION_KIND_DELETE_DOCUMENT","state":"OPERATION_STATE_RUNNING"},
#     "deletedAt":"2026-10-07T09:12:44Z","expungeSla":{"materializeWithin":"900s","purgeWithin":"86400s","indexRebuiltWithin":"172800s"}}
```

Rules of thumb: pass `meta.requestId` (or `operationId`) on every write you may retry (4.1.3);
treat `NamespaceFrozen` (`FAILED_PRECONDITION`) and `NamespaceNotReady` (`UNAVAILABLE`) as "retry after
`retry_after`" (4.1.6), but `PreconditionFailed{NAMESPACE_DELETING}` as final; while a delete is being expunged, Recall keeps working but may be slower for that
namespace (5.4). Over Connect/JSON the same four calls are plain `curl` POSTs (4.8).

### 4.1 Conventions

#### 4.1.1 Transport, metadata and authentication

One port serves gRPC (HTTP/2), Connect and gRPC-Web (connect-go multiplexes all three on the same
handler). Envoy in front balances **per request** (HTTP/2 stream), never per connection.

| Metadata key (gRPC) / header (Connect) | Required | Semantics |
|---|---|---|
| `authorization: Bearer <jwt>` | yes | EdDSA/RS256 JWT with claims `tenant_id`, `ns` (list of namespace ids or `["*"]`), `ns_group` (list of tenant-defined namespace groups; a namespace carries one group, so a token for 10 000 per-user namespaces is one claim, not 400 KB of ids — N65, review F-29), `scopes ⊆ {memory.read, memory.write, memory.admin, tenant.admin}` (D13; `tenant.admin` is tenant-bound, and the fleet-wide `engram.operator` scope and the workers' `engram.worker` identity have their own audiences and never appear in a tenant token, 4.1.9). A namespace is allowed when `"*" ∈ ns`, or its id `∈ ns`, or its group `∈ ns_group`. Verified by the single `authz.Interceptor` (unary + stream). Missing/invalid → `UNAUTHENTICATED`; expired → `UNAUTHENTICATED` with message `token expired`. |
| `grpc-timeout` / `Connect-Timeout-Ms` | yes | The deadline (4.1.2). Absent → `INVALID_ARGUMENT`. |
| `traceparent`, `tracestate` | no | W3C trace context; propagated into OpenTelemetry spans and into Temporal workflow headers. |
| `user-agent` | no | Logged; the MCP and Connect adapters append `engram-mcp/<ver>` / `engram-connect/<ver>`. |
| `x-engram-trace-id` (response trailer) | — | Trace id of the call, for support tickets. |

The **request body**, not metadata, carries `NamespaceRef` (tenant + namespace) and `RequestMeta`
(`request_id`, `client_timestamp`). Rationale: everything that participates in idempotency, audit
and replay must be inside the serialised message, so that the idempotency hash, the ingest ledger
and the Temporal history all see the same bytes. Rejected: `x-engram-namespace` header routing
(cannot be hashed with the body; invisible to `grpcurl`-style tooling; drifts from the proto).

**Authorization outcomes** (decided here because the register only names the mechanism):

| Situation | Code | Why |
|---|---|---|
| Token `tenant_id` ≠ `NamespaceRef.tenant_id`, or namespace not in the caller's tenant | `NOT_FOUND` + `NotFound{NAMESPACE}` | Cross-tenant requests must not be an existence oracle; the answer is identical for "no such namespace" and "not yours". |
| Same tenant, namespace neither in the `ns` allowlist nor in a group named by `ns_group` | `PERMISSION_DENIED` | Within a tenant, existence is not secret; the caller should ask for a broader token (or a group claim, N65). |
| Scope missing for the method (table in 4.2) | `PERMISSION_DENIED` | Message names the missing scope. |
| Operator-only admin method called with a tenant token (`ShardService`, `MoveService`, `ListTenants`, `CreateTenant`, `UpdateTenantLimits`) | `PERMISSION_DENIED` | These methods are served only on the separate Envoy admin route that tenants cannot reach, and the interceptor also requires `engram.operator`, so separation does not rest on routing alone. A tenant administrator cannot raise its own quotas or change its isolation (N157, A-12). The tenant's own lifecycle (`GetTenant`, `DeleteTenant`, `GetTenantOperation`, `UpdateTenant`) is tenant-bound `tenant.admin` on the tenant-facing listener; `engram.operator` may also call `GetTenant`, `DeleteTenant` and `GetTenantOperation` on the admin route (N167, A-6). |
| Namespace `DELETING` | `FAILED_PRECONDITION` + `PreconditionFailed{NAMESPACE_DELETING}` | Reads and writes are both rejected from the delete ack on (the shard is `frozen/delete`, N122), while the purge runs (D8, N5) — **except** `OperationService.GetOperation` and `WaitOperation`, which the method policy marks `allow_deleting` so the `DELETE_NAMESPACE`/`DELETE_TENANT` operations the delete returned can be awaited (N70, review F-27/F-39). |
| Namespace `DELETED` (catalog tombstone) | `NOT_FOUND` + `NotFound{NAMESPACE}` | The namespace is gone; within the tenant there is no existence oracle to protect. |

#### 4.1.2 Deadlines are mandatory

Every call must carry a deadline. A call without one is rejected with `INVALID_ARGUMENT` and a
`ValidationError{violations:[{field:"grpc-timeout", reason:"MISSING_DEADLINE"}]}` before any work
is done. Rationale: (1) Recall's rerank stage decides on the *remaining* deadline (skip rerank if
< 106 ms = rerank p95 + pack + stream + 8 ms, N106; the SLO assumes a client deadline ≥ 300 ms) and cannot do that without one; (2) an unbounded call is an unbounded shard
connection through pgbouncer, and the recall pool is 32 connections per shard (API only; the worker has its own pool of 8, D3, N114, N155, N164); (3) Envoy route timeouts
would otherwise silently become the deadline, with a different error code. Rejected alternative:
a server-side default deadline — it hides misconfigured clients until the day the default is
wrong.

Deadlines are also capped, per method, so a client cannot pin a stream or a Postgres connection
for hours:

| Method | Max deadline | Min deadline | Note |
|---|---|---|---|
| `Recall` | 10 s | 200 ms | Below 200 ms even the semantic arm cannot finish; reject rather than always time out. |
| `Retain` | 30 s | 500 ms | Ledger + operation row insert only. |
| `Reflect` | 330 s | 5 s | 300 s wall budget (D12) + 30 s for streaming the answer. |
| `WaitOperation` | 65 s | 1 s | Server wait clamps to `deadline − 500 ms` and 60 s. |
| `StreamSnapshot` | 600 s | 5 s | Resumable by offset, so long streams are unnecessary. |
| `DeleteDocument`, `DeleteNamespace`, `DeleteTenant`, `Restore` | 40 s | 1 s | They take an exclusive lock in one 35 s attempt (N82, N120, N139); a cap below the lock timeout would turn a lock wait into `DEADLINE_EXCEEDED` after the intent object was already written. |
| everything else | 30 s | 100 ms | |

Exceeding the cap is `INVALID_ARGUMENT` (`reason:"DEADLINE_TOO_LONG"`), not a silent clamp:
silent clamping produces `DEADLINE_EXCEEDED` errors that nobody can explain from the client side.

#### 4.1.3 Idempotency: `request_id` versus `operation_id`

Two keys exist because two different things need retry-safety: a *call* and a *unit of work*.

| | `RequestMeta.request_id` | `operation_id` (Retain, Delete*, CreateSnapshot, RefreshPage) |
|---|---|---|
| Scope | (tenant, namespace, method) for **24 h**, stored in the per-shard `idempotency_keys` table (D1); catalog-level writes (`CreateNamespace`, admin) keep theirs in the catalog DB. | Permanent; it names the unit of work, and the workflow id follows **per kind (N136)**: retain, export and namespace delete are the workflow id suffix `ns/{ns}/op/{operation_id}` (D11); a document delete is the expunge singleton `ns/{ns}/expunge` plus the tombstone's `operation_id`; a page refresh is the per-page singleton `ns/{ns}/page/{page_id}` with the `operation_id` carried in its nudge; a tenant delete is `tenant/{tenant}/delete`. |
| On replay with identical request hash | The stored response bytes are returned (no re-execution). | The existing `Operation` is returned in whatever state it is. |
| On reuse with a different hash | `ALREADY_EXISTS` + `OperationConflict{reason: IDEMPOTENCY_KEY_REUSED}`. | Same. |
| Format | Opaque, 1–128 bytes. | Any RFC 4122 UUID (validated as a UUID, UUIDv7 recommended; the per-document ids derived below are UUIDv5, so "UUIDv7 only" would have rejected the server's own ids — N72, review F-44). |
| Optional? | **Required** (`INVALID_ARGUMENT`, reason `REQUEST_ID_REQUIRED`, N184) on `CreateTenant`, `UpdateTenant`, `UpdateTenantLimits`, `DeleteTenant`, `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace`, `StartMove`, `CleanupMove`, `RollbackMove`; the CLI and SDKs mint one. Optional on other writes (a write without it is not replay-safe; not rejected). Ignored on reads except for tracing. | Optional; the server mints one. |
| Multi-document `Retain` | One `request_id` covers the whole call. | With exactly one `document_id` in the request the supplied id is used verbatim; with several, per-document ids are `UUIDv5(operation_id, document_id)` so a retry regenerates the same ids. Items without a `document_id` get the minted id `UUIDv5(operation_id, "item/" ‖ index)` whenever an `operation_id` is present (N127); a retain with neither `operation_id` nor `request_id` mints fresh UUIDv7s and is **not replay-safe** (documented, not rejected). |

The request hash is SHA-256 over the **normalised protojson** rendering of the request with `meta`
cleared — object keys sorted, unknown fields dropped, default values omitted, 64-bit integers and
`Timestamp`/`Duration` in their canonical JSON forms (N72). Protobuf has no canonical binary
encoding: `Deterministic: true` only orders map entries, and unknown fields, SDK field order and
`Struct` number formatting all change the bytes, so a binary hash would make a retry through a
newer SDK fail with `IDEMPOTENCY_KEY_REUSED` (review F-43). The same function (`api.RequestHash`)
is used by the ledger and by Temporal inputs. Rejected alternative: a single key. It conflates "did this HTTP call already happen"
(short-lived, per shard) with "does this unit of work exist" (permanent, cross-move — operations
are restarted on the target shard with the same id, D5 step 6).

Temporal enforces the second half: workflows start with `WorkflowIdReusePolicy =
REJECT_DUPLICATE`, so an operation id is never executed twice. **A `FAILED` operation is final**:
resubmitting the same `operation_id` returns the `FAILED` row, and a retry uses a **new**
`operation_id` (the state machine has no `FAILED → PENDING` edge, N35, N139; `OperationError.retryable`
says whether a retry is expected to help). Rejected: a `RetryOperation` RPC (a second contract and a
linked-operation model nobody asked for).

**MCP edge rule (A-6, N127).** JSON-RPC ids are per-session counters, so the MCP adapter never
forwards one as a key: `request_id = sha256(mcp-session-id ‖ jsonrpc-id ‖ tool)`. Two agents (or a
reconnecting one) therefore never collide on `IDEMPOTENCY_KEY_REUSED`, and a retry inside one MCP
session replays the stored response, minted document ids included.

#### 4.1.4 Pagination

Every `List*` method embeds `PagingRequest{page_size, page_token}` and returns
`PagingResponse{next_page_token, total_size_estimate}`.

- **Keyset, not offset.** Tokens encode the sort key of the last row (e.g. `(mentioned_at, id)`),
  base64url of a small proto, HMAC-signed with a server key so they cannot be forged into another
  namespace. They never expire and are stable under concurrent inserts (a new row sorts before or
  after the cursor, never shifts it).
- **Bound to the query.** The token also carries a hash of (filter, order, namespace, read_mask).
  Reusing it with a different filter is `INVALID_ARGUMENT` (`field:"paging.page_token",
  reason:"INCONSISTENT"`). Rejected: silently applying the new filter — it yields pages that skip
  or repeat rows and nobody notices.
- `page_size` 0 → method default 100; values above the maximum (1000; 200 for `ListMemories` with
  `read_mask` including `text`) are **clamped**, not rejected, because clients routinely ask for
  "as many as you can".
- `total_size_estimate` comes from planner statistics (`-1` when unknown); it is for progress bars,
  never for paging arithmetic.

The pagination messages are named `PagingRequest`/`PagingResponse` rather than
`PageRequest`/`PageResponse` because `Page` is a resource (`PageService`) and `GetPageRequest`
next to `PageRequest` is a trap.

#### 4.1.5 Field masks

- `read_mask` (`google.protobuf.FieldMask`) on every `Get*`, `BatchGet*` and `List*`: paths are
  relative to the resource message (`text`, `scores`, `observation.source_fact_ids`). Empty mask =
  all fields. Unknown path → `INVALID_ARGUMENT` (`UNKNOWN_FIELD_MASK_PATH`). Masks matter on the
  shard: a `ListMemories` without `text` in the mask does not touch the TOAST'd text column.
- `update_mask` on every `Update*`: **required and non-empty**; `*` is rejected (a full replace is
  never what a client means for a resource with server-maintained fields). Each method comments
  its allowed paths; naming an output-only path is `INVALID_ARGUMENT` (`OUTPUT_ONLY`). Updates
  carry an optional `etag` for optimistic concurrency (`FAILED_PRECONDITION` +
  `PreconditionFailed{ETAG_MISMATCH}`).
- Repeated fields in `update_mask` are replaced wholesale (`reflect_directives`), never merged; a client
  that wants to append reads, edits and writes with the etag.

#### 4.1.6 Error model

Every error is a `google.rpc.Status` (`code`, `message`, `details[]`) — the standard gRPC rich
error model that grpc-go, connect-go and grpcurl all understand. The public typed details are Engram
messages defined in `memory/v1/errors.proto` and attached at runtime with `status.WithDetails`;
on the wire they are `google.protobuf.Any` entries with type URL
`type.googleapis.com/memory.v1.<Name>`. `google/rpc/error_details.proto` is **not** imported
(the workspace must build without the BSR, and Engram needs details google.rpc lacks); the two
standard ones that generic clients act on — `google.rpc.RetryInfo` and `google.rpc.ErrorInfo`
— are attached from the `errdetails` Go package, which needs no proto import. A response carries
at most one Engram detail plus, when a retry hint exists, `RetryInfo`. The table below is
generated from `internal/errs` (`make gen-docs`): **one gRPC code per detail type** (and per
`OperationConflict` reason), so the proto comments, the Go mapping and this table cannot disagree
(N139, A-10).

| Typed detail | gRPC code | Raised when | Client action |
|---|---|---|---|
| `ValidationError{violations[]}` | `INVALID_ARGUMENT` | Malformed request: missing/too-long field, bad enum value, inconsistent `TagFilter`, unknown mask path, **missing or over-cap deadline**, bad page token. | Fix the request. Never retry unchanged. |
| `NotFound{kind, id, namespace, reason}` | `NOT_FOUND` | Unknown memory/document/operation/page/snapshot id; namespace unknown **or belonging to another tenant**; page version absent at `as_of`; `reason = DOCUMENT_DELETED` for `GetDocumentVersion`/`GetDocumentBody` of a version a delete tombstone covers and for `UpdateDocumentTags` on a `DELETING` document (N136, N157), and for `Restore` of a fact whose `invalidate` row was purged with its deleted document (N162). `Invalidate` of a fact whose row was purged is plain `NOT_FOUND`. | Do not retry; the id is wrong or the resource is gone. |
| `QuotaExceeded{quota, limit, current, retry_after, scope}` (+ `RetryInfo`) | `RESOURCE_EXHAUSTED` | Admission-time rate quotas `recalls_per_min`, `retains_per_min`, `max_request_bytes`, `max_namespaces` (D13), and a synchronous Reflect whose `quota.Reserve` was refused (N130). *Not* raised for workflow gateway calls: those defer the operation (`DEFERRED`) instead. | Sleep `retry_after`, retry identical request. |
| `WrongShardOrEpoch{namespace_id, expected_epoch, actual_epoch, namespace_state}` | `FAILED_PRECONDITION` | The shard's `namespace_ownership` row disagrees with the resolved (shard, epoch): stale catalog cache, move cut over between resolve and execute, restore bumped the epoch (D2 row 3). The API invalidates its catalog entry, re-resolves and retries the whole call **once** for writes; a call that sees `namespace_state = MOVED_OUT` (the window between cutover sub-steps (c) and (d), §5.5) re-resolves in a **bounded loop of ≤ 5 s**, exactly like `NamespaceFrozen`, because the catalog still names the source until (d) and a single retry would fail identically (N52, N125). It is surfaced only when that also fails and counts against the availability SLI. The routing hint of the permanent `moved_out` row (`target_shard_id`, `next_epoch`, N93) travels as the **internal** `engram.internal.errors.v1.MovedOutHint`, consumed by the router and dropped by the interceptor; no shard id is part of a public message (N128, A-12). | Retry with backoff (a fresh resolve happens server-side). Never persist epochs. |
| `NamespaceFrozen{namespace_id, retry_after, reason, frozen_until_estimate}` (+ `RetryInfo`) | `FAILED_PRECONDITION` | Writes during the freeze window of a move or a restore (D5); a move freeze lasts for the whole copy (minutes to hours) and carries `frozen_until_estimate`, the move's freeze deadline, to which clients back off (N160); or while an exclusive taker is queued on the namespace fence and the writer's `try`-lock was refused (`reason` unspecified, `retry_after` = 200 ms; the fence never waits, N82; the internal `FenceBusy` detail is mapped to this). Reads continue only for a move freeze; a restore freeze rejects them too (`RESTORING`, N122). A **delete** freeze is not reported here: it is `PreconditionFailed{NAMESPACE_DELETING}` everywhere (N139). The API already retried with backoff for up to 30 s. | Retry after `retry_after`. |
| `NamespaceNotReady{namespace_id, retry_after}` (+ `RetryInfo`) | `UNAVAILABLE` | The target shard of a move is `ready` but not yet `active` (cutover sub-steps (b′) to (b″), well under a second, N125). The API retries inside its bounded loop first. | Retry after `retry_after`. |
| `OperationConflict{operation_id, existing_operation_id, reason}` | `ABORTED` | A concurrent operation owns the state: page already refreshing, snapshot already running, or `NAMESPACE_BUSY` (a delete, or a move before activation, is in progress; `DeleteNamespace` during a move carries `RetryInfo{30 s}`, N177). | `WaitOperation(existing_operation_id)` then resubmit. |
| `OperationConflict{reason: IDEMPOTENCY_KEY_REUSED}` | `ALREADY_EXISTS` | `request_id`/`operation_id` reused with a different request hash; namespace/page `name` already taken. | Use a fresh id; the stored one is bound to a different request. |
| `PreconditionFailed{violations[]}` | `FAILED_PRECONDITION` | Cancel on a terminal operation (`OPERATION_TERMINAL`) or on a `DELETE_*` operation (`OPERATION_NOT_CANCELLABLE`, N136), etag mismatch, `Invalidate` on a non-fact id (a second `Invalidate` of the same fact succeeds, not an error, and changes no visibility, but still writes its own `deletion_log` row and intent, N139, N143), `Restore` of a fact whose subject has no `invalidate` marker (`NOT_INVALIDATED`), `StartMove` without the operator window the estimate needs (`PreconditionFailed{type: MOVE_WINDOW_REQUIRED}`) or with one below `max(1.5 × W_est, W_est + 10 min)` or above 8 h (`MOVE_WINDOW_TOO_SHORT`, N173; `MOVE_TOO_LARGE` is unused) `StartMove` onto a target with an unhealthy archive (`MOVE_TARGET_ARCHIVE_UNHEALTHY`, N179), `RollbackMove` after (a″) (`MOVE_PAST_COMMIT`, N184), namespace `DELETING` (the delete freeze included), tenant `DELETING` (`TENANT_DELETING`), snapshot base version pruned, `StreamSnapshot` of a version a delete expired (`SNAPSHOT_EXPIRED`, N126), `GetPage` of a version a delete or invalidation hides until the refresh lands (`PAGE_HIDDEN`, N117). | Read the current state, decide, resubmit. Do not blind-retry. |
| — | `UNAUTHENTICATED` | Missing/invalid/expired JWT. | Refresh the token. |
| — | `PERMISSION_DENIED` | Scope or allowlist (4.1.1). | Obtain a broader token. |
| — (+ `RetryInfo{2 s}`) | `UNAVAILABLE` | Catalog **miss** while the catalog is down (D4 as amended: cached entries of existing namespaces are served indefinitely, only misses fail), shard `READONLY`/`RETIRED` or marked unavailable by the schema-version guard (N22), pgbouncer pool exhausted. | Retry with jittered backoff; idempotent by construction. |
| — (+ `RetryInfo{2 s}`) | `UNAVAILABLE` | A lifecycle write (`CreateTenant`, `UpdateTenant`, `UpdateTenantLimits`, `DeleteTenant`, `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace`'s `deleting` row, `StartMove`, `CleanupMove`, `RollbackMove`, and the catalog `idempotency_keys` row each writes) committed but not yet replayed by the standby (N171, N184). | Retry with the same `request_id`; the retry finds the row and re-checks. |

**Lossy set of a catalog restore (RPO 60 s, N182):** `CreateTenant`, `UpdateTenant`, `UpdateTenantLimits`, `CreateNamespace`/`UpdateNamespace` metadata and `idempotency_keys` rows; clients retry them by `request_id` (routing, moves and deletes are re-derived from the shards).
| — | `DEADLINE_EXCEEDED` | The deadline elapsed. For `Recall` the stream may already have delivered results; the trailing `RecallStats` is then missing. | Retry with a larger deadline or lower budget. |
| — | `UNIMPLEMENTED` | Phase-gated service (`PageService`, `ExportService` before phase 3). | None. |
| — | `INTERNAL` | Bug or invariant violation (e.g. embedding dims mismatch). Logged with trace id. | Report `x-engram-trace-id`. |

Details that only the write path or the router needs live in `engram/internal/errors/v1/errors.proto` and are dropped by the interceptor before a response leaves the process: `MovedOutHint{namespace_id, target_shard_id, next_epoch}`, `FenceBusy{namespace_id, retry_after}` (mapped to `NamespaceFrozen`), `DocumentBusy{document_id, retry_after}` (`CommitChunk` could not take the shared per-document lock, N83) and `InputBlobMissing{blob_key, input, content_hash, attempt}` (a purged cache or staging blob; the workflow re-runs extract and embed at most twice, N100).

Failed **operations** carry the same model inside `Operation.error` (`OperationError{code,
message, details[], retryable}`) so a client that polls sees the same detail types it would have
seen synchronously.

Connect renders the same status as JSON:

```json
{"code":"invalid_argument","message":"tag_filter.mode: UNSPECIFIED with non-empty tags",
 "details":[{"type":"memory.v1.ValidationError","value":"CiQKD3RhZ19maWx0ZXIubW9kZRIH...",
             "debug":{"violations":[{"field":"tag_filter.mode","reason":"INCONSISTENT",
                                     "description":"UNSPECIFIED with non-empty tags"}]}}]}
```

`debug` is the JSON rendering of the detail that connect-go emits alongside the base64 `value`;
it is convenience, not contract (only `value` is guaranteed).

#### 4.1.7 Streaming versus unary

| Method | Choice | Rationale | Rejected |
|---|---|---|---|
| `MemoryService.Retain` | **unary** → `Operation[]` | The ack means only "ledger row + operation row durable" (D16); there is nothing incremental to send. Per-chunk progress is observable through `OperationService`, which keeps long-lived state on the worker side rather than pinning a stream to one API replica for minutes behind a per-request balancer. | Client-streaming items (items are bounded: ≤ 100, ≤ 8 MiB; a bigger corpus is many documents). Server-streaming chunk acks (couples the API to the worker's lifetime). |
| `MemoryService.Recall` | **server-streaming** `RecallResponse{result \| stats}` | Results are emitted in rank order, flushed in groups of 10 after packing, then one trailing `RecallStats` with per-stage timings (D10). The honest rationale is that stats trailer and future progressive arms, **not** message size: a HIGH response is ≈ 64 KiB of text, far below any gRPC limit (N129, A-20). | Unary with `repeated` results (no slot for the timings trailer). A unary alias for JSON clients was rejected as a second contract test matrix (N129); Connect clients consume the stream with `buf curl` (4.8). |
| `MemoryService.Reflect` | **server-streaming** `ReflectResponse{delta \| tool_call \| tool_result \| citation \| answer \| stats}` | Up to 300 s of wall time with visible progress (tool steps) and token deltas; the client must be able to show something before the end. | Bidirectional (no mid-session client input exists; the loop is bounded and server-driven). Unary (300 s of silence). |
| `OperationService.WaitOperation` | **unary long-poll** (server wait ≤ min(timeout, deadline − 500 ms, 60 s)) | It is a barrier, not a feed: one round trip, works from curl/Connect/JSON, no long-lived stream to migrate when an API replica restarts, and Envoy balances each poll. **Mechanism (N70, review F-39):** the handler parks on Temporal's workflow-result long-poll (`GetWorkflow(id).Get` with the remaining wait) — one open poll per waiter, capped per API process (`MaxOpenWaits`, default 2 000); above the cap, or when the workflow no longer exists (namespace purge), it falls back to a jittered poll of the `operations` row (1 s ± 250 ms). Singleton-backed kinds have no workflow of their own to park on: a document delete polls the tombstone, a page refresh the page row (N136, N157). Neither path needs a per-shard `LISTEN` (impossible through pgbouncer, N15). | `WatchOperation` server stream (nice-to-have for progress bars; deferred until someone needs it — `GetOperation` polling every 2 s is adequate). |
| `ExportService.StreamSnapshot` | **server-streaming** 1 MiB parts | Files reach gigabytes; gRPC messages should stay ≤ 4 MiB; resumable by `(path, offset)` so a broken stream costs one part. | Unary presigned-URL redirect (leaks blob layout and credentials model to clients; not all deployments have public blob endpoints). |
| `DocumentService.DeleteDocument`, `NamespaceService.DeleteNamespace` | **unary** → ack + `Operation` | The ack is the committed soft-delete marker followed by the durable intent object (N115, N122); the expunge is async and tracked (N119). `DeleteTenant` returns after the replicated catalog `deleting` row and the tenant intent; the operation's `acknowledged_at` is set once every namespace is fenced (re-resolving each shard per attempt), the client's durability point (N122, N171, N177, N182). | — |
| `PageService.DeletePage` | **unary** (no `Operation`) | Pages are small; the delete is fully synchronous. | — |
| All `Get*`/`List*`/`BatchGet*` | **unary**, `idempotency_level = NO_SIDE_EFFECTS` | Enables HTTP GET in Connect (4.7). | — |

#### 4.1.8 Limits (validated before any work)

| Field | Limit |
|---|---|
| `RetainRequest.items` | ≤ 100 items, ≤ 8 MiB total content; `content` ≤ 1 MiB; `context` ≤ 2 KiB; `metadata` ≤ 16 KiB serialised; `tags` ≤ 32 × 64 B (N32); `document_id` ≤ 256 B |
| `RecallRequest.query` | 1–8 KiB; `max_tokens` 256–65 536; `max_results` ≤ 500; `TagFilter.tags` ≤ 32 |
| `ReflectRequest.query` | ≤ 16 KiB; `context` ≤ 32 KiB; `max_iterations` ≤ 10 |
| `BatchGetMemoriesRequest.memory_ids` | ≤ 100 |
| `Namespace.mission` | ≤ 4 KiB; `reflect_directives` ≤ 32 × 1 KiB; `display_name` ≤ 256 B |
| `PageContent.markdown` | ≤ 256 KiB |
| Any single gRPC message | ≤ 16 MiB (server `MaxRecvMsgSize`), which the item limits keep unreachable |

#### 4.1.9 Scopes per method

| Scope | Methods |
|---|---|
| `memory.read` | Recall, Reflect, GetMemory, ListMemories, BatchGetMemories, Get/ListDocuments, GetDocumentVersion, GetDocumentBody, ListTags, Get/ListNamespaces, GetEffectiveConfig, GetOperation, ListOperations and WaitOperation (Get/Wait also while the namespace is `DELETING`, N70), GetSnapshotManifest, ListSnapshots, StreamSnapshot, GetPage, ListPages, SearchPages (N73) |
| `memory.write` | Retain, Invalidate, Restore, DeleteDocument, UpdateDocumentTags, CreateSnapshot, Create/Update/Delete/RefreshPage, CancelOperation |
| `memory.admin` | Create/Update/DeleteNamespace (within the token's tenant) |
| `tenant.admin` | The tenant's own lifecycle: `GetTenant`, `DeleteTenant`, `GetTenantOperation` (also while the tenant is `DELETING`), and `UpdateTenant` (`display_name`, `config`). **Tenant-bound:** the interceptor enforces `token.tenant_id == request.tenant_id` on every tenant-level method (D13, N139), and these methods are served on the tenant-facing listener (N157, A-12) |
| `engram.operator` | `ShardService`, `MoveService`, `ListTenants`, `CreateTenant`, `UpdateTenantLimits` (quotas, isolation, state), and `GetTenant`, `DeleteTenant`, `GetTenantOperation` on the admin route (the operator token carries no tenant, so the tenant-bound check is skipped for it there). **Fleet-wide**, with its own token audience and the separate Envoy admin route; never present in a tenant token (D13, N139, N157, N167) |
| `engram.worker` | The workers' service identity: valid on exactly `ReleaseNamespace` and `CleanupMove`, bound to the worker's cell by a `cell` claim the interceptor checks against the shard's cell; the secret and its rotation are in §9.1 (N167, A-4) |

### 4.2 File inventory

#### `buf.yaml` and `buf.gen.yaml`

Two BSR modules — `buf.build/engram/memory` (public + admin) and `buf.build/engram/internal`
(Temporal payloads, events) — share the workspace root and are split by `includes: [memory]` /
`includes: [engram]`. This is the only layout that satisfies three constraints at once: the D14
directory tree, buf's `PACKAGE_DIRECTORY_MATCH` lint rule (a module rooted at `memory/` would
expect package `v1`, not `memory.v1`), and independent versioning of internal schemas. `internal`
imports `memory` (for `NamespaceRef`, `FactType`, `UpdateMode`, `TemporalWindow`, `EntityHint`);
`memory` never imports `internal`. `buf.gen.yaml` runs protoc-gen-go, protoc-gen-go-grpc and
protoc-gen-connect-go with `paths=source_relative` into `../gen/go`, i.e. `<repo>/gen/go` (D14).

#### `memory/v1/common.proto` (full text)

Shared vocabulary. Notable decisions: `Scores` uses proto3 explicit presence (`optional float`)
for arm scores, because "the arm did not return this item" must be distinguishable from "score
0"; `Scores.semantic`/`Scores.lexical` are **per-query normalised to [0, 1]**, never raw cosine
or BM25 — BM25 statistics are per hash partition, so a raw score is shard-relative and a weak
cross-tenant channel (N67, review F-28); `Scores.rrf` is computed in exact rational arithmetic
(N67); `Budget` fixes rerank depth 0/50/150 (N53); the file header states the D9 timestamp
vocabulary (`mentioned_at` server-set = item timestamp, `said_at` display only); `TagMatchMode`
follows the register's Hindsight-compatible five modes with `UNSPECIFIED` = no filter; `UpdateMode` lives here (not in `memory.proto`) because `DocumentVersion`, workflow
payloads and events all need it; `Timestamps` is a small output-only sub-message so that every
resource carries `created_at`/`updated_at` with the same field numbers.

```protobuf
// memory/v1/common.proto
//
// Types shared by every public Engram service: request metadata, namespace
// addressing, pagination, enums (budgets, fact types, memory kinds, tag-match
// modes), tag filters, provenance, per-stage scores, temporal windows and
// resource timestamps.
//
// Field-numbering policy (applies to every file in this package):
//   * numbers 1-15 are for hot fields (1-byte tags);
//   * numbers 100-199 are reserved in every top-level request/response and
//     resource message: upstream never assigns them, so downstream forks can
//     add private fields there without ever colliding with a future release;
//   * removed fields are never renumbered; they become `reserved N` +
//     `reserved "name"` with a comment saying which release removed them.
//
// Timestamp vocabulary (decision D9, review F-2):
//   * `mentioned_at` = the RetainItem `timestamp`, set by the server on every
//     fact and chunk; it is the time the system learned the content and the
//     ONLY `as_of` key. Neither the client (no per-item override) nor the
//     extractor can move it.
//   * `said_at` (facts only) = the extractor's judgement of when the source
//     said it; may precede mentioned_at when a chunk quotes older material.
//     Display and ranking only — never a visibility key.
//   * `occurred_start` / `occurred_end` = when the fact happened.
syntax = "proto3";

package memory.v1;

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

option go_package = "example.com/engram/gen/go/memory/v1;memoryv1";

// RequestMeta carries per-call metadata that must travel in the body (not in
// gRPC metadata) so it is part of the idempotency hash and of the audit log.
message RequestMeta {
  // Client-supplied idempotency key for unary writes (Retain, Delete*,
  // Invalidate, Restore, Create*/Update*). Required (INVALID_ARGUMENT,
  // REQUEST_ID_REQUIRED) on every write whose ack waits for catalog
  // replication (decision N184: CreateTenant, UpdateTenant,
  // UpdateTenantLimits, DeleteTenant, CreateNamespace, UpdateNamespace,
  // DeleteNamespace, StartMove, CleanupMove, RollbackMove); a retry after
  // UNAVAILABLE reuses it. Opaque, 1-128 bytes, unique per
  // (tenant, namespace, method) for 24 h. A replay with the same key and an
  // identical request hash returns the stored response; the same key with a
  // different hash fails with ALREADY_EXISTS + OperationConflict
  // {reason = IDEMPOTENCY_KEY_REUSED}. Optional on other writes and on reads
  // (tracing only).
  string request_id = 1;
  // Wall-clock time at the client when the request was built. Informational:
  // logged next to the server time to diagnose clock skew; never used for
  // `mentioned_at` or `as_of` defaults.
  google.protobuf.Timestamp client_timestamp = 2;
}

// NamespaceRef addresses a namespace. Clients never see or supply a shard id
// or an ownership epoch: the API resolves (tenant_id, namespace_id) through the
// catalog and fences the call on the shard (decisions D2 and D4).
message NamespaceRef {
  // Opaque tenant id, `[a-z0-9-]{1,64}`. Must equal the `tenant_id` claim of
  // the caller's JWT, otherwise the call fails with NOT_FOUND (the namespace is
  // indistinguishable from a nonexistent one across tenants).
  string tenant_id = 1;
  // Server-assigned UUIDv7 of the namespace (see NamespaceService.CreateNamespace).
  string namespace_id = 2;
}

// PagingRequest is embedded in every List request. Pagination is keyset-based:
// tokens are opaque, encode the sort key of the last returned row plus a hash
// of the filter, never expire, and are rejected with INVALID_ARGUMENT if
// reused with a different filter, order or namespace.
message PagingRequest {
  // Maximum number of items to return. 0 means the method default (100);
  // values above the method maximum (1000 unless stated) are clamped, not
  // rejected.
  int32 page_size = 1;
  // Token from the previous PagingResponse.next_page_token. Empty for the
  // first page.
  string page_token = 2;
}

// PagingResponse is embedded in every List response.
message PagingResponse {
  // Token for the next page; empty when this was the last page.
  string next_page_token = 1;
  // Best-effort estimate of the total number of matching items (from planner
  // statistics, not a COUNT(*)); -1 when unknown. Never use it for paging.
  int64 total_size_estimate = 2;
}

// Budget selects the recall/reflect effort level. It fixes per-arm candidate
// caps (low/mid/high = 50/150/400), graph node budgets (100/300/1000),
// rerank depth (0/50/150 — decision N53: the cross-encoder is a sized
// dependency, assumption A-R1) and the default token budget (4k/8k/16k) as
// fixed by decisions D10 and N53.
enum Budget {
  // Not set: the namespace's `recall.default_budget` (system default MID).
  BUDGET_UNSPECIFIED = 0;
  // Cheapest: 50 candidates per arm, 100 graph nodes, no rerank (results
  // carry Scores.stage = FUSED), 4k tokens.
  BUDGET_LOW = 1;
  // Default: 150 candidates per arm, 300 graph nodes, rerank top 50, 8k tokens.
  BUDGET_MID = 2;
  // Deepest: 400 candidates per arm, 1000 graph nodes, rerank top 150, 16k tokens.
  BUDGET_HIGH = 3;
}

// UpdateMode says how a retain relates to the previous version of the same
// document_id.
enum UpdateMode {
  // Not set: REPLACE.
  UPDATE_MODE_UNSPECIFIED = 0;
  // The items are the whole new content of the document. Chunks whose content
  // hash is unchanged are kept (no LLM, no embedding); chunks missing from
  // the new set are hidden together with their facts by chunk tombstones
  // (decision N115) and purged later by the expunge.
  UPDATE_MODE_REPLACE = 1;
  // The items are appended to the document; nothing is hidden. Chunk hashes
  // that already exist are skipped.
  UPDATE_MODE_APPEND = 2;
}

// FactType classifies an extracted fact.
enum FactType {
  // Not set. In a filter list it is rejected; on a Memory it never appears.
  FACT_TYPE_UNSPECIFIED = 0;
  // A statement about the world that is true independently of the speaker
  // ("Paris is the capital of France", "the invoice is due on March 3").
  FACT_TYPE_WORLD = 1;
  // A statement about the agent's or user's own experience, preference or
  // action ("I prefer window seats", "we deployed v2 on Friday").
  FACT_TYPE_EXPERIENCE = 2;
}

// MemoryKind distinguishes the three things Recall can return.
enum MemoryKind {
  // Not set. In a filter list it is rejected; on a Memory it never appears.
  MEMORY_KIND_UNSPECIFIED = 0;
  // An extracted fact (the unit of retain, link and invalidate).
  MEMORY_KIND_FACT = 1;
  // A consolidated observation: an evolving belief with cited source facts.
  // Returned only when RecallRequest.include_observations is true.
  MEMORY_KIND_OBSERVATION = 2;
  // A raw document chunk (the fifth recall arm). Returned only when
  // RecallRequest.include_chunks is true.
  MEMORY_KIND_CHUNK = 3;
}

// TagMatchMode defines how TagFilter.tags (Q) is compared with an item's tag
// set (I), Hindsight-compatible (decision D10). Q and I are sets of exact,
// case-sensitive strings. Five filtering modes plus "unset = no filter".
// The non-STRICT modes let untagged items through; the STRICT variants do
// not — that is the only difference between each pair.
enum TagMatchMode {
  // No filtering: every item matches. This is also the meaning of leaving
  // TagFilter unset. A TagFilter with mode UNSPECIFIED and a non-empty
  // `tags` list is rejected with INVALID_ARGUMENT (reason INCONSISTENT),
  // because it is almost certainly a client bug.
  TAG_MATCH_MODE_UNSPECIFIED = 0;
  // ANY: I = ∅ ∨ I ∩ Q ≠ ∅. Untagged items pass; tagged items need at least
  // one query tag.
  TAG_MATCH_MODE_ANY = 1;
  // ANY_STRICT: I ∩ Q ≠ ∅. At least one query tag is on the item; untagged
  // items do not pass.
  TAG_MATCH_MODE_ANY_STRICT = 2;
  // ALL: I = ∅ ∨ Q ⊆ I. Untagged items pass; tagged items must carry every
  // query tag (extra tags allowed).
  TAG_MATCH_MODE_ALL = 3;
  // ALL_STRICT: Q ⊆ I ∧ I ≠ ∅. Every query tag is on the item (extra tags
  // allowed); untagged items do not pass.
  TAG_MATCH_MODE_ALL_STRICT = 4;
  // EXACT: I = Q. The item's tag set equals the query tag set; with Q ≠ ∅
  // untagged items never pass.
  TAG_MATCH_MODE_EXACT = 5;
}

// TagFilter restricts results to items whose tag set satisfies `mode`
// against `tags`. Tags are item-level and live on the document; the filter is
// resolved once into an allowed-document set that every recall arm applies
// (before ranking), never after fusion, so budgets are not spent on
// filtered-out rows (decision N116). Tags are
// filters, never security (decision D13): a caller with access to a
// namespace can read every tag in it.
message TagFilter {
  // Comparison mode. UNSPECIFIED = no filtering (see enum).
  TagMatchMode mode = 1;
  // Query tag set Q. 1-32 tags, each 1-64 bytes, matched byte-for-byte after
  // trimming; duplicates are removed before evaluation. Required (non-empty)
  // for every mode other than UNSPECIFIED.
  repeated string tags = 2;

  // Removed in v1.0.0-rc4: `bool include_untagged = 3`. The ANY/ALL vs
  // ANY_STRICT/ALL_STRICT split expresses untagged-item handling per mode.
  reserved 3;
  reserved "include_untagged";
}

// Provenance says where a memory came from. Every fact, observation source
// and chunk resolves to a document version and a chunk inside it.
message Provenance {
  // Client-chosen document id (the Retain upsert key).
  string document_id = 1;
  // Version of the document the memory was extracted from (1-based,
  // incremented by every REPLACE/APPEND retain).
  int64 document_version = 2;
  // UUIDv7 of the chunk; for a MEMORY_KIND_CHUNK memory this equals Memory.id.
  string chunk_id = 3;
  // 0-based position of the chunk within the document version.
  int32 chunk_ordinal = 4;
  // Half-open [char_start, char_end) offsets of the chunk inside the
  // concatenated item content of the document version (UTF-8 code points).
  int32 char_start = 5;
  int32 char_end = 6;
  // Index of the RetainItem the chunk was cut from, so callers can map back
  // to their own item list.
  int32 item_index = 7;
}

// RecallStage names the last pipeline stage that produced a result's rank.
enum RecallStage {
  RECALL_STAGE_UNSPECIFIED = 0;
  // Ranks come from RRF fusion only; the reranker was skipped (deadline).
  RECALL_STAGE_FUSED = 1;
  // Ranks come from the cross-encoder rerank plus bounded boosts.
  RECALL_STAGE_RERANKED = 2;
}

// Scores exposes every per-stage score of a recall result so that clients
// (and the evaluation harness) can explain a ranking. Arm scores use explicit
// presence: an absent field means the arm did not return the item at all,
// which is different from a score of 0.
message Scores {
  // Semantic-arm score, normalised PER QUERY over the arm's candidate list to
  // [0, 1] (1 = best candidate of this query). The raw cosine is not exposed
  // (decision N67).
  optional float semantic = 1;
  // Lexical-arm score, normalised PER QUERY over the arm's candidate list to
  // [0, 1]. Raw BM25 scores are never exposed: pg_search statistics are per
  // hash partition, so a raw score is shard-relative, changes under other
  // tenants' writes and is a weak cross-tenant channel (review F-28, N67).
  optional float lexical = 2;
  // Graph-expansion score: sum of link weights along the best path from a
  // seed, decayed per hop; in (0, 1].
  optional float graph = 3;
  // Temporal-arm score: 1 / (1 + |distance to query_timestamp| in days) for
  // items whose occurrence window overlaps the query window; in (0, 1].
  optional float temporal = 4;
  // Chunk-arm score (RRF of the chunk BM25 and chunk HNSW sub-arms).
  optional float chunk = 5;
  // Reciprocal-rank-fusion score, sum over arms of 1 / (60 + rank_arm),
  // computed in exact rational arithmetic (math/big.Rat, decision N67) so
  // that fusion is permutation-invariant; rounded to float for transport.
  float rrf = 6;
  // Cross-encoder relevance logit mapped to [0, 1] by a sigmoid; absent when
  // the rerank stage was skipped.
  optional float rerank = 7;
  // Combined multiplicative boost factor (recency ≤ +10 %, temporal proximity
  // ≤ +10 %, observation proof count ≤ +5 %), clamped to [0.75, 1.25].
  float boost = 8;
  // Final ranking score: (rerank if present, else rrf) × boost.
  float final = 9;
  // Stage that produced `final` and the rank.
  RecallStage stage = 10;
  // Per-arm rank (1-based) for the arms that returned the item; only
  // populated when RecallRequest.explain is true.
  repeated ArmRank arm_ranks = 11;
}

// ArmRank is one arm's rank for an item (explain mode only).
message ArmRank {
  // Arm name: "semantic", "lexical", "graph", "temporal", "chunk",
  // "observation_semantic", "observation_lexical".
  string arm = 1;
  // 1-based rank of the item within that arm's candidate list.
  int32 rank = 2;
}

// TemporalWindow is a closed interval of when something happened. Either
// bound may be unset: unset start = "since forever", unset end = "still
// ongoing / unknown end". A point event has start == end.
message TemporalWindow {
  google.protobuf.Timestamp occurred_start = 1;
  google.protobuf.Timestamp occurred_end = 2;
}

// Timestamps are the server-maintained lifecycle timestamps of a resource.
// Output only.
message Timestamps {
  google.protobuf.Timestamp created_at = 1;
  google.protobuf.Timestamp updated_at = 2;
}

// EntityRef is a resolved entity attached to a memory. Under
// RecallRequest.as_of only `mention` is set (decision N118): entity ids,
// canonical names, types and alias merges may postdate the cut-off.
message EntityRef {
  // UUIDv7 of the entity, unique per namespace.
  string entity_id = 1;
  // Canonical name after per-namespace fuzzy resolution.
  string canonical_name = 2;
  // Coarse type label from the extractor: "person", "organization", "place",
  // "product", "event", "concept", "other".
  string type = 3;
  // How the entity was written in the source text.
  string mention = 4;
}

// EntityHint is a client-provided hint that helps entity resolution: a name
// the extractor should treat as a known entity, optionally with its type and
// aliases. Hints are advisory; they never create entities without a mention.
message EntityHint {
  string name = 1;
  string type = 2;
  repeated string aliases = 3;
}

// Metadata attached by clients is a free-form JSON object (google.protobuf.Struct).
// It is stored verbatim (≤ 16 KiB serialised) on the document (the metadata of
// the first item of its current version), returned on Memory and Document,
// and never interpreted by the pipeline. Filters resolve at DOCUMENT level,
// exactly like tags (decision N139): the recall layer evaluates them once
// against `documents.metadata` (GIN `jsonb_path_ops`) into the allowed-document
// set that every arm receives, so facts, chunks and observations are filtered
// through their documents. Only equality on top-level string values is
// supported.
message MetadataFilter {
  // Top-level key whose value must equal `value` (string comparison).
  string key = 1;
  google.protobuf.Value value = 2;
}
```

#### `memory/v1/errors.proto`

The public typed details of 4.1.6 and their enums (`ResourceKind`, `NamespaceState`, `QuotaScope`, `FreezeReason`, `OperationConflictReason`). `NamespaceState` is defined here because `WrongShardOrEpoch` needs it and `errors.proto` must not import service files. The write-path and routing details (`MovedOutHint`, `FenceBusy`, `DocumentBusy`, `InputBlobMissing`) live in `engram/internal/errors/v1/errors.proto` (N128): a public message can never be removed once `v1.0.0` ships, and "stripped before leaving the API" is a runtime discipline, not a contract. `FREEZE_REASON_DELETE` is deprecated and never emitted (the delete freeze is `PreconditionFailed{NAMESPACE_DELETING}`, N139); `FREEZE_REASON_FENCE_BUSY` and `WrongShardOrEpoch` fields 5 and 6 are reserved. Full file under `plans/engram/proto/memory/v1/errors.proto`.

#### `memory/v1/operation.proto`

`OperationService` (GetOperation, ListOperations, CancelOperation, WaitOperation) and the
`Operation` resource: `id`, `kind` (RETAIN_DOCUMENT, DELETE_DOCUMENT, DELETE_NAMESPACE,
**DELETE_TENANT**, CONSOLIDATE, REFRESH_PAGE, CREATE_SNAPSHOT), `state` (PENDING, RUNNING,
DEFERRED, SUCCEEDED, FAILED, CANCELLED), `cancel_reason` (CLIENT_REQUEST, DOCUMENT_DELETED,
NAMESPACE_DELETING), `progress{units_total, units_done, units_failed, units_skipped, phase}`,
`error`, `deferred{quota, resume_at, scope}`, `result`, `timestamps`, `finished_at`, `acknowledged_at` (`DELETE_TENANT`, N182), `request_id`
and the `consolidation_lag` hint (D16). N127 closes the review-3 gaps: `result.document_version` is
**this operation's** version and `result.superseded_by` is **the newer version number** (an `int64`
version, not an operation id; the shard column is a `bigint` version too, N139) set when a newer
version finalised first, so the read barrier reads "after `SUCCEEDED`, Recall observes every fact of
that version **unless `superseded_by` is set**"; a retain that races a delete ends `CANCELLED` with
`CANCEL_REASON_DOCUMENT_DELETED`; `OPERATION_KIND_DELETE_TENANT` (derived from the catalog's tenant
row, read through the tenant-scoped `memory.admin.v1.TenantService.GetTenantOperation`) can be
awaited; `MOVE_NAMESPACE` left the public enum (number 7 reserved) because a move is the catalog's
`namespace_moves` row, not an operation.

**The operation contract per kind (N136, N157).** Retain and export are backed by their own
workflow at `ns/{namespace_id}/op/{operation_id}`; `DELETE_DOCUMENT` is the per-namespace expunge
singleton plus the tombstone's `operation_id`, with progress read from the expunge's bookkeeping;
`REFRESH_PAGE` is singleton-backed too (the per-page workflow `ns/{namespace_id}/page/{page_id}`
receives the `operation_id` in its nudge, `WaitOperation` polls the page row and a move's `Restart`
skips it, so `PAGE_REFRESHING` holds and strong-model calls are not paid twice); `CONSOLIDATE` is
the consolidation singleton. Consequently **`DELETE_*` operations are
non-cancellable** (`CancelOperation` → `PreconditionFailed{OPERATION_NOT_CANCELLABLE}`: the delete is
already effective and the expunge must finish), `WaitOperation` on them polls the tombstone (or the
catalog row for a namespace and a tenant), and a move's `Restart` skips singleton-backed kinds
(`DELETE_DOCUMENT`, `CONSOLIDATE`, `REFRESH_PAGE`). For a
delete kind, `SUCCEEDED` means "physically gone" (the delete itself was effective at the RPC ack);
`progress.phase` walks the expunge phases `materialize`, `purge`, `derived`, `index`, `finish` and
`ExpungeSla` states the bounds (15 min, 24 h, 48 h). A `FAILED` operation is final: a retry uses a
new `operation_id` (4.1.3). Cancellation of the other kinds is cooperative (next activity boundary);
already-committed chunks stay visible. `WaitOperation` returns `{operation, timed_out}` so the two
return paths are unambiguous. Full file under `plans/engram/proto/memory/v1/operation.proto`.

#### `memory/v1/memory.proto` (full text)

`MemoryService`. Key semantics beyond the comments in the file:

- **Retain grouping.** Items are grouped by `document_id` in order of first appearance; each group
  is one document version and one `Operation`; `RetainResponse.operations` is aligned with
  `document_ids` (minted ids for items that had none: `UUIDv5(operation_id, "item/" ‖ index)` when
  the request carries an `operation_id`, N127; otherwise fresh and not replay-safe). All items of a group must agree on
  `update_mode`. The request is rejected as a whole on any validation error — partial acceptance
  would make `request_id` replay ambiguous.
- **Recall stream contract.** Zero or more `result` messages in strictly increasing `rank`, then
  exactly one `stats`. If the deadline expires mid-stream the client gets `DEADLINE_EXCEEDED`
  after the results already sent and no `stats`; results are never re-ordered by a later message.
  Rerank skipped (deadline or `rerank=false`) is signalled by `Scores.stage = FUSED` on every
  result and `RecallStats.rerank_skip_reason`.
- **`metadata_filters` resolve at document level.** Like `tag_filter`, they are evaluated once
  against `documents.metadata` (GIN `jsonb_path_ops`) into the allowed-document set that every arm
  receives (N139); facts, chunks and observations carry no metadata of their own.
- **`optional bool rerank`** and **`optional bool stream_tokens`** use explicit presence so that
  "not set" can mean "default true" without inverting the flag's name (`disable_rerank`) — the
  proto3 idiom for tri-state booleans.
- **Memory is one message for three kinds.** A `oneof` per kind was rejected: Recall results are
  consumed uniformly (text, scores, provenance), and the kind-specific parts (`observation`,
  `chunk`) are sub-messages that are simply unset for the other kinds. Facts' 5W slots are flat
  strings because they are display/explain data, not structured queries.
- **`mentioned_at` is server-set, `said_at` is the model's (D9, review F-2).** `RetainItem.mentioned_at`
  (field 9) is removed and reserved: a per-item override let a client backdate the `as_of` key,
  and the extractor's "earlier if the chunk quotes older material" rule did the same, so
  "leak-free" depended on a client and an LLM. Every fact and chunk now carries
  `mentioned_at = RetainItem.timestamp` exactly; the quoted-older-date judgement lands in
  `Memory.said_at` (facts only, display and ranking). For a chunk, `mentioned_at` is the **maximum**
  timestamp of every item whose bytes it covers (for an `APPEND` re-chunk also the `mentioned_at`
  of every base chunk the new text overlaps), facts inherit it, and a hard chunk boundary is forced
  between items more than 24 h apart (N86); document-local timestamp regressions are accepted,
  clamped up, and reported as `OperationResult.timestamps_clamped`.
- **`as_of` and versioned evidence (N85, review-2 G-7).** Under `as_of = T` an observation
  version's `source_fact_ids`, `quotes` and `proof_count` are that version's own
  `observation_version_sources` rows joined to visible facts with `mentioned_at <= T`; the chunk arm
  also requires `embedding_effective_at <= T` (the newest `mentioned_at` covered by the summary in
  the embedded header), and `ChunkInfo.header` is returned empty whenever `as_of` is set. Entity
  names, aliases and entity hops are `as_of`-safe too (N118): `EntityRef` carries only `mention`
  under `as_of`.
- **Deletion and curation are read-time predicates (N115–N117, N135).** `DeleteDocument`,
  `Invalidate` and `Restore` write one marker row (and then their intent object, N122); every read
  path (Recall arms, GetMemory, ListMemories, Reflect tools, `DocumentService`) evaluates
  `visible(f) ≡ document ∉ tombstones ∧ chunk ∉ chunk tombstones ∧ fact has no fact_hidden row`,
  and an observation or page version is visible iff a version row exists (fail closed) and no
  input in its evidence segment names a tombstoned document or an **invalidated** fact. Exports
  follow N126 (expiry on delete, `hidden_overlay` for curation). An invalidation is independent of
  the fact row (no foreign key, N145): its marker outlives the chunk and re-extraction purges. The
  curation subject is `(document_id, content_hash)` (N162): `Invalidate` hides every live fact of the
  subject (the fact and its same-document, same-content twins) in one transaction under one lock,
  with one invalidation stamp, one deletion record and one intent, a twin created later by
  re-extraction is hidden with the same stamp, and `Restore` removes the rows carrying that stamp. `GetMemory` and
  `BatchGetMemories` return an invalidated fact **found**, with `invalidated_at` set (N157, A-15);
  only an unknown id or one a delete tombstone covers is missing. `fact_hidden` has two causes:
  `Memory.invalidated_at` reports the `invalidate` marker's time only, and a `reextract` row (a
  prompt or model bump hid a stale extraction) neither shows there nor hides any derived version.
  `ObservationInfo.stale` means "a rewrite is pending", not "hidden". A second `Invalidate` of the
  same fact succeeds without changing visibility and still writes its own deletion record and intent (N143); `Restore` is exact by stamp and takes the exclusive derivation lock, so
  `Invalidate`/`Restore` and the delete RPCs have a 40 s deadline cap (4.1.2).
- **Parity rules (N129).** Rank 1 is always emitted whole and its overflow counted in
  `RecallStats.tokens_used` (Hindsight returns the top result whole); the rerank depth 0/50/150 is
  gated by the B.1 ablation `--rerank-top {0,50,150,300}`; `Recall` stays streaming for the
  stats trailer and future progressive arms.
- **`prefer_observations`** (field 18) is the Hindsight knob page refresh relies on (§5.3): rank an
  observation ahead of the facts it cites and suppress those facts when the observation fits.
- **`Operation`/`operation_id` accept any UUID** (N72): server-minted ids are UUIDv7, derived
  per-document ids are UUIDv5, so validation is "a UUID", not "a UUIDv7".
- **Reflect stream contract.** `{delta|tool_call|tool_result|citation}*`, then exactly one
  `answer`, then exactly one `stats`. Citations are only ever to ids that appeared in some
  `ToolResult.memory_ids` of the same stream (server-side verification, D12); the count of
  rejected citations is in `stats.citations_rejected`.

```protobuf
// memory/v1/memory.proto
//
// MemoryService: the core write (Retain), read (Recall, GetMemory,
// ListMemories, BatchGetMemories), reasoning (Reflect) and curation
// (Invalidate, Restore) surface of Engram.
//
// Streaming decisions:
//   * Retain is UNARY. Its ack means "the ingest-ledger row and the operation
//     row are durable" (decision D16), nothing more; there is no incremental
//     result to stream. Per-chunk visibility is observed through
//     OperationService (GetOperation/WaitOperation), which keeps the
//     long-lived state on the worker side instead of pinning a stream to one
//     API replica for minutes behind a per-request load balancer.
//   * Recall is SERVER-STREAMING: results are sent in rank order in flushes of
//     10 after packing, followed by one trailing RecallStats message (decision
//     D10). The honest rationale is that stats trailer and future progressive
//     arms, not message size (a HIGH response is far below 4 MiB); a unary
//     alias was rejected as a second contract test matrix (decision N129).
//   * Reflect is SERVER-STREAMING: token deltas, tool calls/results and
//     citations are emitted as they happen during a bounded agentic loop that
//     may run for up to 300 s (decision D12).
syntax = "proto3";

package memory.v1;

import "google/protobuf/duration.proto";
import "google/protobuf/field_mask.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "memory/v1/common.proto";
import "memory/v1/operation.proto";

option go_package = "example.com/engram/gen/go/memory/v1;memoryv1";

// MemoryService is the primary data-plane service. Every method requires a
// deadline, an `authorization: Bearer <jwt>` metadata entry whose `tenant_id`
// claim equals `namespace.tenant_id` and whose `ns` allowlist contains
// `namespace.namespace_id` (or whose `ns_group` claim names the namespace's
// tenant-defined group, decision N65), and the scope noted per method.
service MemoryService {
  // Retain submits one or more items for asynchronous ingestion. Scope
  // memory.write. Items are grouped by document_id; each distinct document_id
  // becomes one document version and one Operation (kind RETAIN_DOCUMENT).
  // The call returns as soon as the ingest-ledger rows and the operation rows
  // are committed; facts become visible chunk by chunk afterwards. Not
  // read-your-writes: use OperationService.WaitOperation as the read barrier
  // (after SUCCEEDED, every fact of the operation's version is visible unless
  // `superseded_by` is set; decision N127).
  rpc Retain(RetainRequest) returns (RetainResponse);
  // Recall runs the no-LLM retrieval pipeline (semantic, lexical, graph,
  // temporal and chunk arms in parallel → RRF k=60 → cross-encoder rerank →
  // bounded boosts → token-budget packing) and streams the packed results in
  // rank order, then one RecallStats. Every arm applies the read-time
  // visibility predicate over the namespace's delete and curation markers
  // (decision N116), so a document delete or Invalidate is effective at its
  // ack. Scope memory.read. Deadline ≤ 10 s.
  rpc Recall(RecallRequest) returns (stream RecallResponse);
  // Reflect answers a question with a bounded agentic loop over observations,
  // facts and pages (forced searches first, then ≤ 10 free iterations,
  // ≤ 100k context tokens, ≤ 300 s wall). Scope memory.read. Streams token
  // deltas, tool steps, verified citations, the final answer and stats.
  // Deadline ≤ 330 s.
  rpc Reflect(ReflectRequest) returns (stream ReflectResponse);
  // GetMemory fetches one fact, observation or chunk by id. Scope memory.read.
  // An invalidated fact is FOUND and returned with invalidated_at set (it is
  // hidden from Recall, Reflect and ListMemories only; decision N157, review
  // A-15). Observation and page versions are read through the read side's
  // derived readers, so GetMemory is served during a move freeze like any
  // other read.
  rpc GetMemory(GetMemoryRequest) returns (GetMemoryResponse) {
    option idempotency_level = NO_SIDE_EFFECTS;
  }
  // ListMemories pages through memories with structural filters (no
  // relevance ranking). Scope memory.read.
  rpc ListMemories(ListMemoriesRequest) returns (ListMemoriesResponse) {
    option idempotency_level = NO_SIDE_EFFECTS;
  }
  // BatchGetMemories fetches up to 100 memories by id in one round trip;
  // missing ids are reported, not errors. An invalidated fact is found (with
  // invalidated_at set), exactly as in GetMemory; only an unknown id or one
  // covered by a delete tombstone is missing (decision N157, review A-15).
  // Scope memory.read.
  rpc BatchGetMemories(BatchGetMemoriesRequest) returns (BatchGetMemoriesResponse) {
    option idempotency_level = NO_SIDE_EFFECTS;
  }
  // Invalidate soft-hides one FACT from Recall, Reflect and ListMemories
  // (GetMemory still reads it, with invalidated_at set). The synchronous part is one committed `fact_hidden(invalidate)`
  // marker row (decisions N115, N116) followed by the intent object (N122,
  // put after the commit, before the ack): no row is updated and nothing is
  // walked. The marker row does not depend on the fact row (no foreign key,
  // N145): it survives the purge of the fact it names. The curation subject
  // is the pair (document_id, content_hash) of the fact named (decision
  // N162): the same transaction hides every live fact of the subject (the
  // fact and its same-document, same-content twins) under one subject lock,
  // stamps their markers with one invalidation id, writes one deletion-log
  // row and one intent, and a twin created later by re-extraction is hidden
  // with the same stamp. Every observation or page version whose evidence segment names
  // the fact is hidden at read time by the same predicate (N117), and the
  // affected observations are scheduled for a root rebuild. Exports are not
  // expired by it; they honour it from the next snapshot and meanwhile
  // through the manifest's hidden_overlay (decision N126). Reversible:
  // Restore deletes the marker and nothing else encodes the invalidation, so
  // it is exact. Invalidating an already invalidated fact succeeds and changes
  // no visibility, but it still writes its own deletion-log row and intent
  // (decisions N139, N143). The ack follows a re-read of the marker after the
  // intent put; if a restore removed it, the call returns UNAVAILABLE and the
  // client retries (decision N143). Scope memory.write. Milliseconds in the
  // common case.
  rpc Invalidate(InvalidateRequest) returns (InvalidateResponse);
  // Restore deletes the `invalidate` markers that carry the same invalidation
  // stamp as the named fact (the whole subject, twins included, decision
  // N162) and the derived-hidden rows that carry their cause (decisions N115,
  // N135); nothing is looked up by current hidden state, so it is exact. A
  // `reextract` marker is never touched, so a restore cannot resurrect a
  // stale extraction. It is possible after the fact's own row was purged,
  // because the marker outlives it, but not after its document was deleted
  // (NOT_FOUND{DOCUMENT_DELETED}: the delete purged the marker).
  // Visibility flips at commit; the intent object follows, as for Invalidate.
  // Restore takes the exclusive derivation lock and may wait for a running
  // consolidation or page commit (one attempt of up to 35 s), so its deadline
  // cap is 40 s (decision N139). Scope memory.write.
  rpc Restore(RestoreRequest) returns (RestoreResponse);
}

// RetainItem is one unit of raw input. Items sharing a document_id form one
// document version, in list order.
message RetainItem {
  // Raw text or markdown, 1 byte to 1 MiB (UTF-8). Stored verbatim in the
  // append-only ingest ledger and in blob storage.
  string content = 1;
  // When the content was produced (e.g. the message time). Required. The
  // server sets `mentioned_at` of every fact and chunk extracted from it to
  // exactly this value: it is the only `as_of` key and cannot be overridden
  // by the client or moved by the extractor (decision D9, review F-2). A
  // source that quotes older material gets the older date in `said_at`.
  // Timestamps within a document may regress (re-ingest, benchmark replays);
  // they are accepted and clamped up to the running maximum when chunks are
  // built (decision N86), and the operation result reports
  // OperationResult.timestamps_clamped.
  google.protobuf.Timestamp timestamp = 2;
  // Free-text context given to the extractor ("chat with Alice about the
  // Q3 plan"), ≤ 2 KiB. Not searchable; stored on the document version.
  string context = 3;
  // Client-chosen upsert key, ≤ 256 bytes, unique per namespace. Empty: the
  // server mints an id and the item becomes a standalone document (its id is
  // returned in RetainResponse.document_ids). When the request carries an
  // operation_id the minted id is UUIDv5(operation_id, "item/" ‖ index), so
  // a retry reproduces it (decision N127); without an operation_id it is a
  // fresh UUIDv7 and the retain is NOT replay-safe.
  string document_id = 4;
  // Tags of the item; ≤ 32 tags (decision N32), each 1-64 bytes, exact-match
  // strings. Tags are item-level and live on the DOCUMENT: the document's tag
  // set is the union over its items, and every fact and chunk is filtered
  // through its document (decision N116). Filters, never security.
  repeated string tags = 5;
  // Free-form JSON object stored verbatim (≤ 16 KiB), returned on memories,
  // filterable by top-level string equality.
  google.protobuf.Struct metadata = 6;
  // Optional entity hints for resolution.
  repeated EntityHint entity_hints = 7;
  // How this document version relates to the previous one. All items of a
  // document_id in one request must agree, otherwise INVALID_ARGUMENT.
  // APPEND chains (decision N56): the base of an append is the highest
  // existing version, even one still ingesting, so two concurrent appends
  // yield `base ‖ A` and `base ‖ A ‖ B`, never a lost append.
  UpdateMode update_mode = 8;
  // MIME-ish content type: "text/plain" (default) or "text/markdown" (enables
  // heading-anchored chunking).
  string content_type = 10;

  // Removed in v1.0.0-rc5 (review F-2, decision D9): `google.protobuf.Timestamp
  // mentioned_at = 9`, a per-item override of the `as_of` key. A backdated key
  // made "leak-free" depend on the client and the extractor. `mentioned_at` is
  // now server-set = `timestamp`; a quoted older date lands in Memory.said_at.
  reserved 9;
  reserved "mentioned_at";
}

// RetainRequest submits items. Limits: ≤ 100 items, ≤ 8 MiB total content.
message RetainRequest {
  NamespaceRef namespace = 1;
  // request_id is the idempotency key of the whole call (24 h).
  RequestMeta meta = 2;
  repeated RetainItem items = 3;
  // Optional client-chosen UUID (any RFC 4122 version is accepted, UUIDv7
  // recommended; decision N72). With exactly one document in the request it
  // is the Operation id verbatim; with several documents the per-document ids
  // are derived as UUIDv5(operation_id, document_id). Resubmitting the same
  // operation_id with the same request hash (SHA-256 over the normalised
  // protojson of the request, N72) returns the existing operations; with a
  // different hash fails with ALREADY_EXISTS.
  string operation_id = 4;

  // Removed in v1.0.0-rc2: `bool wait_for_visibility = 5` (synchronous
  // retain). Replaced by OperationService.WaitOperation so that the API
  // never holds a request open for the duration of an LLM pipeline.
  reserved 5;
  reserved "wait_for_visibility";

  reserved 100 to 199;
}

// RetainResponse acknowledges durable acceptance: one Operation per distinct
// document_id, in order of first appearance in `items`.
message RetainResponse {
  repeated Operation operations = 1;
  // Ids of documents that were minted because the item had no document_id,
  // aligned with `operations` (deterministic when operation_id was given,
  // decision N127).
  repeated string document_ids = 2;

  reserved 100 to 199;
}

// RecallRequest describes a retrieval.
message RecallRequest {
  NamespaceRef namespace = 1;
  RequestMeta meta = 2;
  // Natural-language query, 1-8 KiB. Embedded once with the
  // `search_query: ` prefix; also tokenised for BM25 and date extraction.
  string query = 3;
  // Effort level; UNSPECIFIED = namespace default (MID).
  Budget budget = 4;
  // Overrides the budget's token budget for packing (cl100k_base tokens),
  // 256-65536. 0 = budget default (4k/8k/16k). Items that do not fit are
  // skipped (never truncated) and counted in RecallStats.skipped_count, with
  // one exception (decision N129): rank 1 is always emitted whole, and its
  // overflow is counted in tokens_used.
  int32 max_tokens = 5;
  // Restrict facts to these types; empty = both. Does not affect chunks.
  repeated FactType fact_types = 6;
  // Tag filter; unset = no tag filtering (see TagMatchMode).
  TagFilter tag_filter = 7;
  // Anchor for temporal reasoning: relative expressions in the query
  // ("last week") are resolved against it and the temporal arm orders by
  // distance to it. Default: server now. Does NOT hide anything.
  google.protobuf.Timestamp query_timestamp = 8;
  // Knowledge cut-off (decision D9). When set to T, every arm filters facts
  // and chunks by `mentioned_at <= T` before ranking, and each observation
  // (and page) is returned in the latest version whose `effective_at <= T`
  // (none if the first version is later). Guarantee: nothing derived from
  // content mentioned after T is returned. Unset = no cut-off. Independent
  // of query_timestamp: as_of hides, query_timestamp anchors.
  // Under as_of = T (decision N85, review-2 G-7):
  //   * an observation version's evidence (`source_fact_ids`, `quotes`,
  //     `proof_count`) is that version's own observation_version_sources rows
  //     joined to facts that are visible AND mentioned_at <= T, never the
  //     current working set;
  //   * entity names, aliases and entity hops are as_of-safe (decision N118):
  //     EntityRef carries only `mention` and graph hops use
  //     entity_mentions.mentioned_at <= T;
  //   * the chunk arm requires `embedding_effective_at <= T AND
  //     mentioned_at <= T`, where embedding_effective_at is the newest
  //     mentioned_at covered by the summary embedded into the chunk header
  //     (accepted cost: after a summary grows, older chunks are invisible to
  //     as_of queries earlier than the summary's coverage; the fact arm
  //     embeds without header and is unaffected);
  //   * `ChunkInfo.header` is returned empty.
  google.protobuf.Timestamp as_of = 9;
  // Include raw chunks (fifth arm) in the results.
  bool include_chunks = 10;
  // Include consolidated observations in the results.
  bool include_observations = 11;
  // Hard cap on returned items after packing, 1-500; 0 = budget default
  // (20/50/100). Packing may return fewer.
  int32 max_results = 12;
  // Run the cross-encoder rerank. Unset = true. The depths 0/50/150 are gated
  // by the B.1 ablation `--rerank-top {0,50,150,300}` (decision N129). When
  // false, when the budget's rerank depth is 0 (LOW, decision N53), or when the remaining deadline is
  // < 106 ms at rerank time (rerank p95 + pack + stream + 8 ms, decision
  // N106), results carry Scores.stage = FUSED. The SLO assumes a client
  // deadline of at least 300 ms; a skip at a shorter deadline is by design
  // and not an SLO breach. At 300 ms or more a deadline skip is counted
  // against the rerank-skip SLO (N53, N106), not treated as a benign
  // degradation.
  optional bool rerank = 13;
  // Populate Scores.arm_ranks and per-stage candidate counts in RecallStats.
  bool explain = 14;
  // Explicit occurrence window for the temporal arm. Unset = derived from
  // the query text by rule-based date extraction (may be empty, in which
  // case the temporal arm is skipped).
  TemporalWindow temporal_window = 15;
  // Restrict to items whose metadata matches all of these (AND).
  repeated MetadataFilter metadata_filters = 16;
  // With include_observations: rank an observation ahead of the facts it
  // cites and suppress those facts from the packed result when the
  // observation fits (Hindsight `prefer_observations`). Page refresh
  // (section 5.3) sets it; default false.
  bool prefer_observations = 18;

  // Removed in v1.0.0-rc3: `float min_score = 17`. A raw score threshold is
  // not comparable across arms and rerankers; budgets and max_results are the
  // supported knobs.
  reserved 17;
  reserved "min_score";

  reserved 100 to 199;
}

// RecallResponse is one stream message: either a ranked result or the single
// trailing stats message. Clients must tolerate results after stats never
// occurring and stats being absent if the stream errors.
message RecallResponse {
  oneof payload {
    RecallResult result = 1;
    RecallStats stats = 2;
  }

  reserved 100 to 199;
}

// RecallResult is one packed result in rank order.
message RecallResult {
  // 1-based final rank across the whole response.
  int32 rank = 1;
  // The memory with `scores` and `provenance` populated. Text fields are
  // complete (packing skips, never truncates).
  Memory memory = 2;
  // cl100k_base tokens this item consumed from the budget.
  int32 tokens = 3;
}

// RecallStats closes a Recall stream.
message RecallStats {
  // Distinct candidates after fusion (before rerank/packing).
  int32 total_candidates = 1;
  // Items streamed.
  int32 returned_count = 2;
  // Items that were ranked but did not fit the token budget (never
  // truncated; packing continues past a skip).
  int32 skipped_count = 3;
  // Tokens consumed by returned items.
  int32 tokens_used = 4;
  // Effective token budget.
  int32 max_tokens = 5;
  // Last stage applied (FUSED when rerank was skipped).
  RecallStage last_stage = 6;
  // Why rerank was skipped ("deadline", "disabled", ""), if it was.
  string rerank_skip_reason = 7;
  // Wall time per stage, in pipeline order: "authz", "embed", "arm:semantic",
  // "arm:lexical", "arm:graph", "arm:temporal", "arm:chunk", "fuse",
  // "rerank", "boost", "pack".
  repeated StageTiming stage_timings = 8;
  // The cut-off actually applied (echo of as_of).
  google.protobuf.Timestamp as_of_applied = 9;
  // The occurrence window the temporal arm used (derived or explicit).
  TemporalWindow temporal_window_applied = 10;
  // Effective query_timestamp.
  google.protobuf.Timestamp query_timestamp_applied = 11;
  // Total server time from admission to last flush.
  google.protobuf.Duration total = 12;
  // True when any vector arm ran at E >= theta eligible rows (theta = 10 k):
  // the recall is counted in the filtered class, p95 <= 1 s, and its arm
  // transactions passed the second semaphore (decision N164).
  bool filtered = 13;
  // True when a filtered arm exhausted its scan bound (4 theta) and returned
  // what it had (decision N151).
  bool partial = 14;
}

// StageTiming is one pipeline stage's timing and candidate counts.
message StageTiming {
  string stage = 1;
  google.protobuf.Duration duration = 2;
  // Candidates entering / leaving the stage (explain only, else 0).
  int32 candidates_in = 3;
  int32 candidates_out = 4;
  // True when the stage was skipped (deadline, disabled, empty window).
  bool skipped = 5;
}

// ObservationInfo is present on MEMORY_KIND_OBSERVATION memories.
message ObservationInfo {
  // Number of distinct source facts supporting the observation.
  int32 proof_count = 1;
  // Ids of the supporting facts (hidden ones are excluded from Recall views
  // but kept in GetMemory with `include_hidden_sources`).
  repeated string source_fact_ids = 2;
  // Version returned: the version current at as_of (effective_at <= as_of <
  // superseded_at) or, without as_of, the current version; when that version
  // is hidden (decision N117) nothing is returned for the observation. A
  // version with no visible source (proof_count = 0) is not returned.
  int64 version = 3;
  // The as_of visibility key (decision D9): max(mentioned_at) over every
  // fact shown to the consolidation prompt that wrote this version (its
  // inputs, a superset of source_fact_ids), clamped to be >= the effective_at
  // of the observations shown and of the previous version.
  google.protobuf.Timestamp effective_at = 4;
  // True when a rewrite is pending for this observation: evidence changed
  // since this version was written (a document replace or a re-extraction
  // retired a source, an Invalidate hid one, a Restore brought one back). It
  // says "a rebuild is pending", never "hidden": a stale version that is
  // visible is still served (decision N135). A version whose evidence
  // segment names a deleted document or an invalidated fact is not served at
  // all, whatever this flag says: the read-time predicate hides it (decision
  // N117), permanently for document causes at every as_of, and nothing is
  // returned for the observation at an `as_of` whose current version is
  // hidden until the rebuild lands.
  bool stale = 5;
  // Verbatim quotes from source facts, aligned with source_fact_ids where
  // available. Evidence is versioned (decision N85): for any returned
  // version, current or as_of, `source_fact_ids`, `quotes` and `proof_count`
  // are that version's rows, filtered to visible facts with mentioned_at <=
  // as_of when as_of is set.
  repeated string quotes = 6;
}

// ChunkInfo is present on MEMORY_KIND_CHUNK memories.
message ChunkInfo {
  // Contextual header prepended for embedding/extraction:
  // "[doc summary ≤ 200 chars] > [heading path]". Not part of `text`.
  // Empty whenever the request carried `as_of` (decision N85): the summary
  // may name content mentioned after T.
  string header = 1;
  // SHA-256 (hex) of the chunk text: its identity within the document.
  string content_hash = 2;
  // Number of facts extracted from the chunk.
  int32 fact_count = 3;
}

// Memory is a fact, an observation or a chunk. Which sub-fields are set
// depends on `kind`; the who/what/when/where/why fields are facts only.
message Memory {
  // UUIDv7 (memory_id, observation_id or chunk_id).
  string id = 1;
  MemoryKind kind = 2;
  // Fact text / observation text / chunk text (≤ 16 KiB for chunks, the
  // stored chunk limit of decision D8).
  string text = 3;
  // Facts only.
  FactType fact_type = 4;
  // Facts only: extracted 5W slots (any may be empty).
  string who = 5;
  string what = 6;
  string when = 7;
  string where = 8;
  string why = 9;
  // When the fact happened (facts; empty window for chunks/observations).
  TemporalWindow occurred = 10;
  // When the system learned it: the RetainItem `timestamp`, set by the
  // server; the as_of visibility key for facts and chunks (decision D9).
  // For a chunk it is the maximum timestamp of every item whose bytes the
  // chunk covers (for an APPEND re-chunk also the mentioned_at of every base
  // chunk the new text overlaps); facts inherit the chunk value (decision
  // N86). A hard chunk boundary is forced between consecutive items whose
  // timestamps differ by more than 24 h.
  google.protobuf.Timestamp mentioned_at = 11;
  // Resolved entities (facts) or entities mentioned (chunks).
  repeated EntityRef entities = 12;
  repeated string tags = 13;
  google.protobuf.Struct metadata = 14;
  // Where it came from; for observations the provenance of the most recent
  // cited source.
  Provenance provenance = 15;
  // Per-stage scores; only on Recall results (unset on Get/List).
  Scores scores = 16;
  // Observations only.
  ObservationInfo observation = 17;
  // Chunks only.
  ChunkInfo chunk = 18;
  // Set when a `fact_hidden` row with cause `invalidate` exists for the fact
  // (Invalidate; decision N115): hidden from Recall and Reflect but readable
  // by GetMemory. `reextract` rows (a prompt or model bump hid a stale
  // extraction) are NOT reported here and cannot be restored (decision N135).
  google.protobuf.Timestamp invalidated_at = 19;
  Timestamps timestamps = 20;
  NamespaceRef namespace = 21;
  // Facts only: when the source SAID it, as judged by the extractor (may
  // precede mentioned_at when the chunk quotes or forwards older material).
  // Display and ranking only — never an as_of key (decision D9, review F-2).
  // Unset when the extractor gave no explicit date.
  google.protobuf.Timestamp said_at = 22;

  reserved 100 to 199;
}

// GetMemoryRequest fetches one memory by id.
message GetMemoryRequest {
  NamespaceRef namespace = 1;
  string memory_id = 2;
  // Projection over Memory paths, e.g. ["text", "observation.source_fact_ids"].
  google.protobuf.FieldMask read_mask = 3;
  // Observations: also list source facts that are invalidated (never
  // tombstoned or purged ones; those are gone).
  bool include_hidden_sources = 4;

  reserved 100 to 199;
}

// GetMemoryResponse wraps the memory.
message GetMemoryResponse {
  Memory memory = 1;

  reserved 100 to 199;
}

// ListMemoriesOrder selects the sort order of ListMemories.
enum ListMemoriesOrder {
  // MENTIONED_AT_DESC.
  LIST_MEMORIES_ORDER_UNSPECIFIED = 0;
  LIST_MEMORIES_ORDER_MENTIONED_AT_DESC = 1;
  LIST_MEMORIES_ORDER_MENTIONED_AT_ASC = 2;
  LIST_MEMORIES_ORDER_CREATED_AT_DESC = 3;
  LIST_MEMORIES_ORDER_OCCURRED_START_ASC = 4;
}

// ListMemoriesRequest is a structural listing (no query, no ranking).
message ListMemoriesRequest {
  NamespaceRef namespace = 1;
  PagingRequest paging = 2;
  // Kinds to include; empty = FACT only.
  repeated MemoryKind kinds = 3;
  repeated FactType fact_types = 4;
  TagFilter tag_filter = 5;
  // Restrict to one document.
  string document_id = 6;
  // Only items whose occurrence window overlaps this window.
  TemporalWindow occurred_overlaps = 7;
  // mentioned_at bounds (inclusive).
  google.protobuf.Timestamp mentioned_after = 8;
  google.protobuf.Timestamp mentioned_before = 9;
  // Include soft-invalidated facts (`fact_hidden(invalidate)` markers;
  // default excluded). Facts of a tombstoned document, and stale extractions
  // hidden by `reextract`, are never returned.
  bool include_invalidated = 10;
  // Restrict to items mentioning this entity id.
  string entity_id = 11;
  ListMemoriesOrder order = 12;
  google.protobuf.FieldMask read_mask = 13;
  repeated MetadataFilter metadata_filters = 14;

  reserved 100 to 199;
}

// ListMemoriesResponse is a page of memories.
message ListMemoriesResponse {
  repeated Memory memories = 1;
  PagingResponse paging = 2;

  reserved 100 to 199;
}

// BatchGetMemoriesRequest fetches up to 100 ids.
message BatchGetMemoriesRequest {
  NamespaceRef namespace = 1;
  repeated string memory_ids = 2;
  google.protobuf.FieldMask read_mask = 3;

  reserved 100 to 199;
}

// BatchGetMemoriesResponse returns found memories in request order and the
// ids that were not found (no error).
message BatchGetMemoriesResponse {
  repeated Memory memories = 1;
  repeated string missing_ids = 2;

  reserved 100 to 199;
}

// InvalidateRequest soft-hides a fact.
message InvalidateRequest {
  NamespaceRef namespace = 1;
  RequestMeta meta = 2;
  // Must be a MEMORY_KIND_FACT id, else FAILED_PRECONDITION
  // {MEMORY_NOT_A_FACT}; a fact whose row was purged after the retire grace is
  // NOT_FOUND (the subject is resolved from the fact row, decision N162).
  // Already-invalidated facts succeed without changing visibility and still
  // write their own deletion-log row and intent (decisions N139, N143).
  string memory_id = 3;
  // Free-text audit reason, ≤ 1 KiB.
  string reason = 4;

  reserved 100 to 199;
}

// InvalidateResponse returns the updated fact.
message InvalidateResponse {
  Memory memory = 1;

  reserved 100 to 199;
}

// RestoreRequest clears a soft invalidation.
message RestoreRequest {
  NamespaceRef namespace = 1;
  RequestMeta meta = 2;
  string memory_id = 3;

  reserved 100 to 199;
}

// RestoreResponse returns the restored fact.
message RestoreResponse {
  Memory memory = 1;

  reserved 100 to 199;
}

// ReflectRequest asks for a reasoned answer.
message ReflectRequest {
  NamespaceRef namespace = 1;
  RequestMeta meta = 2;
  // The question or task, 1-16 KiB.
  string query = 3;
  // Budget used by every internal search tool call.
  Budget budget = 4;
  TagFilter tag_filter = 5;
  // Same semantics as RecallRequest.as_of (including the versioned-evidence
  // and empty-chunk-header rules of decision N85), applied to every tool call
  // of the session (forced and free), so the answer is leak-free at T.
  google.protobuf.Timestamp as_of = 6;
  google.protobuf.Timestamp query_timestamp = 7;
  // Optional JSON Schema (draft 2020-12) the final answer must satisfy; the
  // server validates and, on failure, retries once then reports
  // ReflectAnswer.schema_valid = false.
  google.protobuf.Struct output_schema = 8;
  // Cap on free iterations, 1-10; 0 = namespace default (10).
  int32 max_iterations = 9;
  // Stream TokenDelta events (default true). When false only tool events,
  // citations, the answer and stats are streamed.
  optional bool stream_tokens = 10;
  // Extra context the caller wants considered (e.g. the current conversation
  // turn), ≤ 32 KiB; never retained.
  string context = 11;
  // Overrides the namespace mission for this call only, ≤ 4 KiB.
  string mission_override = 12;

  reserved 100 to 199;
}

// ReflectResponse is one event of the Reflect stream. Order: zero or more
// {delta | tool_call | tool_result | citation} events, exactly one `answer`,
// exactly one `stats` (last). On error the stream terminates with a gRPC
// status and no `answer`.
message ReflectResponse {
  oneof event {
    TokenDelta delta = 1;
    ToolCall tool_call = 2;
    ToolResult tool_result = 3;
    Citation citation = 4;
    ReflectAnswer answer = 5;
    ReflectStats stats = 6;
  }

  reserved 100 to 199;
}

// TokenDelta is a piece of generated text in emission order.
message TokenDelta {
  string text = 1;
  // Iteration that produced it (0 = final synthesis).
  int32 iteration = 2;
}

// ToolCall is an internal tool invocation by the reflect agent.
message ToolCall {
  string call_id = 1;
  // "search_memories", "search_observations", "search_pages" (decision N73,
  // full-text + semantic over page versions), "get_page", "expand_fact".
  string tool = 2;
  google.protobuf.Struct arguments = 3;
  int32 iteration = 4;
  // True for the two forced searches that precede the free iterations.
  bool forced = 5;
}

// ToolResult summarises what a tool returned (ids only; the agent's context
// is not streamed).
message ToolResult {
  string call_id = 1;
  int32 result_count = 2;
  // Ids returned; citations are only valid against this union.
  repeated string memory_ids = 3;
  google.protobuf.Duration duration = 4;
  // Non-empty when the tool failed (per-tool deadline 10 s).
  string error = 5;
}

// Citation links a span of the answer to a memory that was actually
// retrieved during the session. Citations to ids never returned by a tool
// are dropped server-side.
message Citation {
  string memory_id = 1;
  MemoryKind kind = 2;
  // Verbatim supporting quote from the memory text.
  string quote = 3;
  // Half-open [start, end) UTF-8 code-point offsets into ReflectAnswer.text.
  int32 answer_start = 4;
  int32 answer_end = 5;
}

// ReflectAnswer is the final answer.
message ReflectAnswer {
  // Prose answer (also the concatenation of all TokenDelta.text with
  // iteration 0).
  string text = 1;
  // Present when output_schema was given; null when validation failed.
  google.protobuf.Struct structured = 2;
  // All citations retained after verification, in answer order.
  repeated Citation citations = 3;
  // False when output_schema validation failed after the retry.
  bool schema_valid = 4;
}

// ReflectStopReason says why the loop ended.
enum ReflectStopReason {
  REFLECT_STOP_REASON_UNSPECIFIED = 0;
  // The agent produced an answer.
  REFLECT_STOP_REASON_ANSWERED = 1;
  REFLECT_STOP_REASON_MAX_ITERATIONS = 2;
  REFLECT_STOP_REASON_MAX_CONTEXT_TOKENS = 3;
  REFLECT_STOP_REASON_WALL_TIME = 4;
  REFLECT_STOP_REASON_CANCELLED = 5;
}

// ReflectStats closes a Reflect stream.
message ReflectStats {
  int32 iterations = 1;
  int32 tool_calls = 2;
  int32 context_tokens = 3;
  int32 prompt_tokens = 4;
  int32 completion_tokens = 5;
  google.protobuf.Duration wall = 6;
  ReflectStopReason stop_reason = 7;
  // Citations dropped because their id was never retrieved.
  int32 citations_rejected = 8;
  // Model id used (resolved `models.reflect`).
  string model = 9;
}
```

#### `memory/v1/document.proto`

`DocumentService`: `GetDocument` (with optional version list), `ListDocuments` (tag filter,
id prefix, `updated_after`, metadata equality at document level), `DeleteDocument`,
`GetDocumentVersion`, and, added by N139, `GetDocumentBody` (a version's reconstructed body in byte
ranges from its owner-keyed blob), `ListTags` (distinct tags with document counts) and
`UpdateDocumentTags` (add and remove, set semantics; tags live on the document row only, so tag
filters see the change at once). N157 gives the three a build path (A-13): `GetDocumentBody` is a
`DocumentBodies` lookup of the version's `body_key` plus a blob read through the shard handle;
`ListTags` reads the per-namespace `tag_counts` table that every transaction changing tags
maintains; `UpdateDocumentTags` bumps the document's `tag_generation`, emits `DocumentTagsUpdated`
(an outbox event, so Kafka consumers see it), and puts the document into the next export delta's
`documents` part keyed by `(document_id, tag_generation)`, because a tag change creates no version.
A later `REPLACE` or `APPEND` recomputes the tags from the items and drops tags set by
`UpdateDocumentTags` (documented behaviour). The three RPCs are built in M1.6 at 0.5 ew.
`DeleteDocument` is unary and has two halves (D8, N115, N119).
**The synchronous half is an O(1) soft-delete marker**: one shard transaction, under the exclusive
document lock, compares `expected_version`, flips `documents.state` to `deleting` **and clears the
summary, context, metadata and tags in the same row update**, inserts one `document_tombstones`
row, a `deletion_log` row (with the subject's previous entry as chain predecessor) and one outbox
event; **after it commits, the delete intent object is put to blob storage** with the marker's
exact effect (N122), and only then does the call ack, in milliseconds at any document size. A
duplicate attempt that finds the document already `DELETING` returns the existing operation and
first makes sure the intent of the marker it found exists, so an ack always implies an intent; after the put
the handler re-reads the marker and acks only if it is still present, else the call returns `UNAVAILABLE` and the
client retries (N143); a crash between the commit and the put leaves a committed but unacknowledged delete that a restore
may lose and the client's retry re-applies. From the ack on every read path evaluates the
read-time predicate (N116, N117) and returns nothing derived from the document, and every export
snapshot that contains it (also one still `building`) is expired in the same transaction (N126).
**`DocumentService` is one of those read paths (N136):** a `DELETING` document is returned by
`GetDocument` and `ListDocuments(include_deleting)` as its **tombstone only**, with `document_id`,
`state`, `deleted_at`, `up_to_version` and `delete_operation_id`; `GetDocumentVersion` and
`GetDocumentBody` of a covered version answer `NOT_FOUND{DOCUMENT_DELETED}`; a revival writes fresh
values for the new life, and **every `DocumentService` path filters covered versions** (`version ≤
up_to_version` of a pending tombstone), so a revived document never lists its deleted life's
versions through `GetDocument(include_versions)`; a tombstone view never matches a non-empty tag
or metadata filter (A-14). `DocumentState` and the SQL `document_state` are generated from one
table: `deleted` is not exposed and `INGESTING` is `active` with `current_version = 0`. Nothing is counted, walked or stamped, so `DeleteDocumentResponse`
carries `deleted_at` and `expunge_sla` instead of the old `facts_retired`,
`observations_marked_stale` and `pages_marked_stale` (fields 2 to 4 reserved). **The asynchronous
half is the throttled per-namespace `Expunge` workflow** (N119) that the returned `Operation` (kind
`DELETE_DOCUMENT`, not cancellable) tracks: materialize (≤ 15 min), purge rows and blobs (≤ 24 h),
derived versions reduced to content-free stubs, index rebuilt (≤ 48 h); Recall SLOs may degrade
while a marker is pending. `expected_version` gives compare-and-delete, compared inside the marker
transaction. Deleting a document that is already `DELETING` returns the existing operation
(idempotent), deleting an unknown one is `NOT_FOUND`, and a Retain into a deleted `document_id` is
accepted at once: it starts at `up_to_version + 1`, so the new content is visible while the
deleted versions stay hidden and are expunged (N133). Full file under
`plans/engram/proto/memory/v1/document.proto`.

#### `memory/v1/namespace.proto`

`NamespaceService`: `CreateNamespace` (server assigns UUIDv7 id and shard; tenant-unique
immutable `name`; `models.embed` and the dimension are fixed here, N111), `GetNamespace` (optional
`stats`), `ListNamespaces` (restricted to the token's allowlist unless `["*"]`), `UpdateNamespace`
(field mask over `display_name`, `mission`, `reflect_directives`, `disposition`,
`config_overrides`; etag), **`GetEffectiveConfig`** (resolved and raw config, N129) and
`DeleteNamespace` → ack + `Operation` (kind `DELETE_NAMESPACE`, not cancellable; typed
`confirm_name`; a duplicate keeps the client's `operation_id`). The delete takes `freeze_delete` on
the shard and sets the catalog state `deleting` (the marker), then writes the intent object (N122,
after the commit) and acks: from then on reads and writes are rejected as
`PreconditionFailed{NAMESPACE_DELETING}`; the expunge is `DROP INDEX` by deterministic names,
batched `DELETE` and the blob prefix, with no graph repair (N112). Deadline cap 40 s. `Disposition{skepticism, literalism, empathy}` ∈ 1..5, 0 = inherit.
`reflect_directives` are typed `Directive{text, priority, active, tags}` (N129); the plain-string
`directives` (6) is reserved. The `config_overrides` key list in the proto comment is generated
from the same Go table as the server's allow-list (N66, M0.8 check).

**Shard and epoch are both hidden** from the public `Namespace`. Clients address
`(tenant_id, namespace_id)` only (D1); a shard id in the resource would invite clients to cache it
and reason about placement, and the epoch changes on every move and restore without any
client-visible meaning. Operators see both through
`memory.admin.v1.ShardService.ResolveNamespace`. What *is* exposed is `state` (`ACTIVE`, `MOVING`,
`FROZEN`, `DELETING`, …), because a client whose writes were retried for 30 s deserves to learn
why. Full file under `plans/engram/proto/memory/v1/namespace.proto`.

#### `memory/v1/export.proto` (phase 3)

`ExportService`: `CreateSnapshot` → `Operation` (kind `CREATE_SNAPSHOT`, one at a time per
namespace), `GetSnapshotManifest` (`version` 0 = latest, plus the **live `hidden_overlay`**),
`ListSnapshots`, `StreamSnapshot` (server-streaming parts ≤ 1 MiB, resumable by `(path, offset)`,
last part carries the file SHA-256). `BeginSnapshot` inserts the snapshot as `building` and records
an `ins_seq` watermark (`engram_seq_floor(now())`, deferred for ten minutes on a namespace moved in
that recently, N147); `WriteFiles` reads short `READ COMMITTED` ranges bounded by it, with the
marker sets read once (no long snapshot: a two-hour `REPEATABLE READ` would block every index
build and pin the vacuum horizon); `RecordSnapshot` refuses to promote an expired row (N126).
Snapshot v*n* always holds a full file set (`facts.jsonl.zst`, `observations.jsonl.zst`, optional
`chunks.jsonl.zst`, `pages/*.md`, `manifest.json`) and, for *n* > 1, **always**
`delta-v{n-1}-v{n}.jsonl.zst`, computed by diffing the two snapshots by (id, version), with delete
records and `deleted_ids` in the manifest even when v*n−1* expired. The delta also has a
`documents` part keyed by `(document_id, tag_generation)`, so a tag change reaches synced clients
(N157). `SnapshotManifest.expires_at` (renamed from `expired_at`, whose source is the
`export_snapshots.expires_at` column) is set with `state = 'expired'`. **A delete expires; curation
overlays.** A document delete expires every snapshot that contains the document, `building` and
`ready` alike (`SnapshotManifest.expired`): `ListSnapshots` still lists it, `StreamSnapshot`
refuses it with `PreconditionFailed{SNAPSHOT_EXPIRED}`, and `engram-sync` applies the next delta's
deletes before serving and refuses an expired local copy — an acknowledged delete is never served
through an old export — while the expunge starts a system-initiated snapshot (debounced 10 min) so
read-only clients regain a servable copy. `Invalidate` and `Restore` never expire a snapshot: the
parts are static compressed byte streams and cannot be filtered on read (the earlier sentence
"every part applies the read-time visibility predicate" is deleted), so snapshots honour curation
from the next snapshot and `GetSnapshotManifest` carries a live `HiddenOverlay` — the current
`fact_hidden(invalidate)` ids and the `(kind, id, root_version, from_version)` ranges of the derived
versions the read predicate hides now, computed with the evidence-segment test (not from
`derived_hidden` alone, which Materialize writes later, and only after a move completes if one is
open), restricted to markers newer than the manifest's `as_of` plus Restores, paged by
`overlay_page_token`, computed at read time and never stored in `manifest.json` — that
`engram-sync` applies before serving, comparing `root_version` (N145, A-11). In short: *delete → expiry + delta;
invalidate/restore → overlay now, snapshot later.* Full file under
`plans/engram/proto/memory/v1/export.proto`.

#### `memory/v1/page.proto` (phase 3)

`PageService`: `CreatePage` (starts the first refresh unless `defer_refresh`), `GetPage` (by id or
`name`; `include_content`; `version` or `as_of` selects the content version by the D9 rule),
`ListPages` (`stale_only`), `UpdatePage` (mask; changing `source_query`/`tag_filter` sets
`stale_write`), `DeletePage` (synchronous), `RefreshPage` → `Operation` (kind `REFRESH_PAGE`, singleton-backed: a
`SignalWithStart` of the per-page workflow with the `operation_id` in the nudge, N157;
`force` = full rewrite), `SearchPages` (unary, page tokens: `query` + `tag_filter` + `as_of` +
`budget` → ranked `PageHit{rank, page, version, score, snippet}` by BM25 over `page_versions.text`
∪ HNSW over `page_version_vectors` (N111, N112) fused with RRF, no LLM, N73; pages share the
observation visibility and purge path, N139). **There is exactly one `RefreshPolicy`** (N128):
`trigger` = `AFTER_CONSOLIDATION` (debounced), `SCHEDULED` (interval, 1 h to 30 d) or `MANUAL`;
§5.3 and the generated Go `Page` follow it, and there is no `on_delete` knob because the expunge
always refreshes the pages a delete or invalidation touched. Staleness is two booleans on purpose:
`stale_write` (new evidence arrived: incomplete) and `stale_delete` (cited evidence was deleted or
invalidated); a page version whose evidence segment names a tombstoned or hidden input is hidden by
the read predicate (N117) and `GetPage` of a hidden current version answers
`PreconditionFailed{PAGE_HIDDEN}` until the refresh lands. Before phase 3 the service is
registered and answers `UNIMPLEMENTED`. Full file under `plans/engram/proto/memory/v1/page.proto`.

#### `memory/admin/v1/admin.proto`

Separate Envoy admin route for the operator methods. Scopes (D13, N139, N157, N167): the tenant's own
lifecycle (`GetTenant`, `DeleteTenant`, `GetTenantOperation`, and `UpdateTenant` for `display_name` and `config`)
needs the tenant-bound `tenant.admin` (the interceptor enforces `token.tenant_id == request.tenant_id`) and is
served on the tenant-facing listener; `ShardService`, `MoveService`, `ListTenants`, `CreateTenant` and
**`UpdateTenantLimits`** (quotas, isolation, state, split out of `UpdateTenant` so that every method has one
scope, A-6) need the fleet-wide `engram.operator`, which has its own audience, so a tenant administrator cannot
raise its own quotas or change its isolation; the operator may also call `GetTenant`, `DeleteTenant` and
`GetTenantOperation` on the admin route, so offboarding needs no tenant-bound token. **`engram.worker`** is the
workers' service identity, valid on exactly `ReleaseNamespace` and `CleanupMove`, bound to the worker's cell (A-4).
`TenantService` (Create/Get/List/Update/UpdateLimits/Delete; **`GetTenantOperation`**;
`Quotas{recalls_per_min, retains_per_min, llm_tokens_per_day, max_facts, max_namespaces,
max_request_bytes}`, `Isolation{SHARED, DEDICATED}`, config Struct); `DeleteTenant` marks the tenant
`deleting` in the catalog (every request for the tenant then fails
`PreconditionFailed{TENANT_DELETING}`, the read barrier), writes the tenant-level intent object and
returns the `DELETE_TENANT` operation `PENDING`; the `TenantDelete` workflow fences every namespace
(one intent object each) and the operation reports `acknowledged_at` once the last fence is held,
the client's durability point (`FAILED{CATALOG_RESTORED}` if a catalog restore reverted the `deleting` row first; N182). `GetTenantOperation` lists the per-namespace `DELETE_NAMESPACE`
operations as they appear. `ShardService` (RegisterShard,
GetShard, ListShards, DrainShard, UpdateShard, **ResolveNamespace** — the ops view of
shard/epoch/state that `memory.v1` hides, `ReleaseNamespace`; `ShardState` is `PROVISIONING, ACTIVE,
FULL, DRAINING, READONLY, RETIRED`). `MoveService` (StartMove, GetMove, ListMoves, RollbackMove —
allowed at every step **before the point of no return**, the catalog CAS `cutover → committed` (a″) —
and CleanupMove) implements the freeze-then-copy protocol of §5.5 (N160, N169). `StartMove` takes an operator
`window` (required when the window estimate exceeds the 10 min an unattended move may take, about 350 k facts; refused below
`max(1.5 × W_est, W_est + 10 min)`; cap 8 h, default 4 h, N173), an optional `freeze_not_before` (a scheduled window),
`pause_before_freeze`/`resume` and `estimate_only` (returns `window_estimate` without planning). `Move` exposes both epochs,
`MoveState` = `PLANNED, FROZEN, COPIED, CUTOVER, COMMITTED, CLEANING, DONE, ROLLED_BACK, LOST` (`COPIED` = verify, indexes, seal
and consumer wait done; `LOST` is terminal, N183), `window_estimate`, `window`, `freeze_deadline`
(also the `frozen_until_estimate` of `NamespaceFrozen`; the move rolls back by itself at that time unless it passed (a″)),
`cutover_step` (`READY_TARGET`, `CATALOG_COMMIT`, `MOVED_OUT_SOURCE`, `ACTIVE_TARGET`, `CATALOG_FLIP`; set while `COMMITTED` too),
`past_point_of_no_return`, `activated_at`, `copy_end_lsn`, `copy_end_timeline`, `copy_sealed_at` (the target's WAL floor, N179), `committed_replicated_at`, `rolled_back_replicated_at`, `finished_at`, `reconciled_at`, `lost_at`, `lost_restore_id`, `recovered_from_move_id` and
`MoveProgress{rows_copied, rows_estimated, bytes_copied, blobs_copied, tables_done, verify_attempts,
indexes_requested, indexes_ready, restarted_operation_ids}`. Full file
under `plans/engram/proto/memory/admin/v1/admin.proto`. `CleanupMove` takes `committed → cleaning` 24 h after `activated_at` (refused before; N170) and nothing else (the cleanup activity records `done`, N184); `RollbackMove` fails `PreconditionFailed{MOVE_PAST_COMMIT}` after (a″); source blobs go at `finished_at + 28 d`.

#### `engram/internal/workflow/v1/workflow.proto`

Temporal payloads, never served. Every top-level input has `schema_version` (**2** for the D22
build) and a `WorkflowScope{namespace_id, tenant_id, shard_id, epoch, blob_prefix}` — the D4 rule
that workers never consult the catalog on the hot path; the shard's `namespace_ownership` row
verifies the epoch inside each activity transaction. `ResolvedModels` snapshots model ids and
prompt versions at submission. `RetainDocumentInput` carries ledger row ids, not content, and a
`RetainResume` for continue-as-new (every 100 chunks or 20 MB of history, N59). **Payload rule
(N59):** `ChunkWork` carries no chunk text; every activity result larger than 4 KiB travels by blob
key and `CommitChunkInput` is **keys-only**. **Confidentiality (N99):** text appears in histories
only as ciphertext, at most 4 KiB per activity result, plus
`ChunkWork.header`/`context`/`metadata_json`/`entity_hints`; the payload codec uses a per-namespace
data key wrapped by the shard key, and deleting the wrapped key is the shredding step of a
namespace or tenant delete. **Consolidation is two-stage
(N121):** `RouteBatchInput/Result` (stage 1: `RoutingDecision{ATTACH, CREATE, SKIP, MERGE, DROP_SOURCE}`), `WriteObservationInput/Result` (stage 2: one call per touched observation, with the **`base_version`** it was written against), `StoreProposalInput` (write-once under `(batch_key, attempt)`, N43) and `ApplyBatchInput/Result` (the derivation commit rule of N120). **Expunge (N119, N136):** `ExpungeInput` (WAL budget `wal_mb_per_s`), `MaterializeInput/Result` (work discovered from open tombstones and `fact_hidden` rows, never from the payload, N145), `PurgeBatchInput/Result`, `DerivedPurgeInput/Result`, `ExpungeResult`. Also `TenantDeleteInput`, `RetainBackfillInput`,
`ReembedNamespaceInput`, `SweeperInput` (N128: every workflow, including the schedules, has a
`schema_version`), `RefreshPageInput`, `ExportInput` (no outbox cut), and the move messages:
`MoveInput` (window estimate, window, `freeze_not_before`), `CopyProgress` (resumable key-range
heartbeat of the copy from the static source), `VerifyFrozenReport` (count and primary-key hash per table on the
complete copy, per PK range, `VerifyFK`, blobs; one re-copy of mismatching tables, N160, N175), `SealCopyResult` (`copy_end_lsn`, `copy_end_timeline`, `archived_at`, `standby_replayed_at`, N179), `BuildIndexesReport` (the target's
partial indexes valid before cutover, built under the freeze), `MoveCheckpoint` (`w_final` for the one sequence
advance, `freeze_deadline`, source `system_identifier` and `timeline_id`, `CutoverStep` including
`CATALOG_COMMIT`, the report of each phase; the field is `cutover_sub_step`), `MoveResult`. The pre-1.0 breaks of this
file are listed in its header and in 4.5. Full file under `plans/engram/proto/engram/internal/workflow/v1/workflow.proto`.

#### `engram/internal/errors/v1/errors.proto`

`MovedOutHint` (the target shard and epoch of a permanent `moved_out` row, so routing after
cutover sub-step (c) needs no catalog read), `FenceBusy`, `DocumentBusy`, `InputBlobMissing`
(N128). Packed into `Status.details` or a Temporal `ApplicationError` and dropped by the
interceptor before a response leaves the process. Full file under
`plans/engram/proto/engram/internal/errors/v1/errors.proto`.

#### `engram/internal/events/v1/events.proto`

The outbox/Kafka envelope `Event{seq, namespace_id, tenant_id, epoch, occurred_at,
schema_version (2), event_id, operation_id, shard_id, oneof payload}`. Payloads:
`DocumentVersionStarted`, `ChunkCommitted`, `ChunksTombstoned`, `DocumentVersionActivated`,
`DocumentDeleted` (an O(1) marker with `ExpungeState`, emitted at the marker, at materialize and at
purge; the `seq` of the first is the tombstone's `event_seq`, which the purge waits for), `FactInvalidated`, `FactRestored`, `ObservationUpserted`, `ObservationRetired`,
`ObservationsMarkedStale`, `EntityUpserted`, `EntitiesMerged`, `PageVersionCreated`, `PageDeleted`,
`PagesMarkedStale`, `SnapshotCreated`, `TokenUsageRecorded`, `NamespaceDeleted`,
`NamespacePurged`, `RestoreMarker`, `DocumentTagsUpdated` (N157). The N81 events (oneof 40–47) and `RowsPurged` are removed
(reserved): a namespace move reconciles by set difference and never reads the outbox, so there is
no event-to-row replay mapping, no `replay:` annotation and no "covering event" CI rule. Events
stay **thin and bounded** (N80): ids inside a payload are 16-byte `bytes` on **new field numbers
and new `*_bytes` names** (the old `string` numbers and names are reserved, so a consumer built
before the change ignores them instead of decoding a 36-byte UUID string as an id, and the protojson
name is never recycled, N139), an event carries at most 256 ids, larger sets are
paged with `page`/`page_count`, and above 4,096 ids one event carries `ids_elided = true` and
counts only; `DocumentDeleted` has no id list at all and consumers delete by the indexed
`(namespace_id, document_id)` query. Every encoded event is ≤ 16 KiB by construction. Rejected:
fat events carrying full rows (the outbox would be the largest table on the shard). Full file under
`plans/engram/proto/engram/internal/events/v1/events.proto`.

### 4.3 Tag-match modes: truth table

Let Q be the query tag set and I the item's tag set (exact, case-sensitive strings; both
de-duplicated). Tags are item-level and live on `documents` only (N113, N116). The recall layer
resolves the mode **once** against `documents.tags` (GIN on `(namespace_id, tags)`; the strict modes
scan the namespace's documents) into an allowed-document set that it passes to **every arm**
(semantic, lexical, graph expansion, temporal, chunks, observation arms), before any ranking, so
budgets are never spent on rows that will be dropped (D10); on the HNSW path the set is always
passed as `document_id = ANY ($1)` with the array as a constant (N164), and on the exact path above 500 allowed documents as
`document_id IN (SELECT unnest($1))` instead of one array constant (N151). No tag predicate runs under RLS (P-5). The unset filter is the fifth
"mode".

| `TagFilter` | Predicate | SQL on `documents.tags text[]` (`$q` = sorted Q) |
|---|---|---|
| unset / `mode = UNSPECIFIED` | true | — |
| `ANY` | I = ∅ ∨ I ∩ Q ≠ ∅ | `cardinality(tags) = 0 OR tags && $q` |
| `ANY_STRICT` | I ∩ Q ≠ ∅ | `tags && $q` |
| `ALL` | I = ∅ ∨ Q ⊆ I | `cardinality(tags) = 0 OR tags @> $q` |
| `ALL_STRICT` | Q ⊆ I ∧ I ≠ ∅ | `tags @> $q` (Q ≠ ∅ is validated, so `@>` implies I ≠ ∅) |
| `EXACT` | I = Q | `tags = $q` (tags stored sorted and de-duplicated on write) |

Worked truth table for Q = {a, b}:

| Item tags I | unset | ANY | ANY_STRICT | ALL | ALL_STRICT | EXACT |
|---|---|---|---|---|---|---|
| ∅ | ✓ | ✓ | ✗ | ✓ | ✗ | ✗ |
| {a} | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ |
| {a, b} | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| {a, b, c} | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ |
| {b, c} | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ |
| {c} | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |

Properties that the Lean decision procedure (section 7) proves and that `rapid` property tests
check against the SQL predicates: `ANY_STRICT ⇒ ANY`, `ALL_STRICT ⇒ ALL`, `ALL_STRICT ⇒
ANY_STRICT`, `EXACT ⇒ ALL_STRICT`, `ALL ⇒ ANY`; the STRICT variant of each mode differs from the
non-strict one **only** on I = ∅; and with Q = {x} single-tag queries `ANY_STRICT = ALL_STRICT`.

Validation rules: `tags` non-empty (1–32) for every mode other than `UNSPECIFIED`; each tag 1–64
bytes after trimming; `mode = UNSPECIFIED` with a non-empty `tags` list is `INVALID_ARGUMENT`
(`INCONSISTENT`) rather than "no filter", because a client that set tags and forgot the mode
would otherwise silently get the whole namespace back. Rejected alternative: `include_untagged`
as a separate boolean — it doubled the mode count for no expressive gain once the `_STRICT`
variants exist (the field number 3 is reserved in `TagFilter` with the removal note).

Hindsight compatibility: the five names and predicates match Hindsight's `tag_match` so that the
side-by-side evaluation (section 8) sends identical filters to both systems.

### 4.4 `as_of` and `query_timestamp`

Two timestamps, two jobs. **`query_timestamp` anchors; `as_of` hides.** They are independent and
may be combined.

| | `query_timestamp` | `as_of` |
|---|---|---|
| Default | server `now()` | unset (no cut-off) |
| Effect | Resolves relative expressions in the query ("last week", "yesterday") and orders the temporal arm by distance to it; feeds the recency boost. | Filters **every arm** to `mentioned_at ≤ as_of` for facts and chunks, and selects, per observation and per page, the **version current at `as_of`** (`effective_at ≤ as_of < superseded_at`), or nothing when that version is hidden (N117). |
| Can it change *which* items are eligible? | No — only ranks/scores. | Yes — it is a visibility boundary. |
| Applied where | temporal arm, boosts | inside each arm's SQL (`WHERE mentioned_at <= $as_of` on the immutable `facts` row, `WHERE mentioned_at <= $as_of AND embedding_effective_at <= $as_of` on `chunks` (N85); `observation_versions.effective_at <= $as_of` joined to the write-once `observation_version_meta.superseded_at`, the range form of "latest version with `effective_at ≤ T`", N33), and in graph expansion when fetching neighbours, so an invisible fact cannot even be a hop. The visibility predicate of N116 is evaluated in the same statement. |
| Typical use | "what did I plan for next Tuesday?" asked on 2026-06-01 | leak-free evaluation: answer question *k* of a conversation as if later turns did not exist |

Definitions (D9 as amended, review F-2): a fact's `mentioned_at` is **the item's `timestamp`, set
by the server** — the time the system learned the content — and it is the only `as_of` key; it
cannot be overridden per item (the former `RetainItem.mentioned_at` is reserved) and the
extractor cannot move it. The model's judgement of when the source *said* it (a day-30 session
quoting "on day 3 Alice wrote …") is `said_at`, used for display and the temporal/recency ranking
only — never for visibility, because a leak-free cut-off must not depend on an LLM or a client.
`occurred_start/end` is when it *happened*. A chunk's `mentioned_at` is the **maximum** timestamp
of every item whose bytes it covers (for an `APPEND` re-chunk also of every base chunk the new
text overlaps) and its facts inherit it (N86), so the fact arm and the chunk arm agree on what
"as of T" means; a hard chunk boundary is forced between consecutive items more than 24 h apart,
which bounds the relative-date error of the extraction prompt, and per-document timestamp
regressions are clamped up (reported as `timestamps_clamped`) rather than rejected. An observation version's `effective_at = max(mentioned_at)` over
**every fact shown to the consolidation prompt** that wrote it (its inputs, of which the cited
sources are a subset), clamped to be ≥ the `effective_at` of every observation shown and of the
previous version (D9 as amended, N29: the model that wrote it saw all of them; TLC
`AsOf_CitedOnly` shows the leak with cited sources only); a page version's `effective_at`
likewise over every evidence item shown to the refresh prompt, with the same clamp. Evidence is versioned (N85): an observation version's sources, quotes and `proof_count` are the
rows of `observation_version_sources` for that version, joined to visible facts with `mentioned_at ≤
T`. A chunk's embedded header may summarise later items, so the chunk arm adds
`embedding_effective_at ≤ T` (`embedding_effective_at` = the `mentioned_at` of the newest item the
header's summary covers) and `ChunkInfo.header` is returned empty under `as_of`; the accepted cost
is that after a summary grows, older chunks are invisible to `as_of` queries earlier than the
summary's coverage (the fact arm embeds without a header and is unaffected). The guarantee `as_of
= T` gives is therefore: *no fact, chunk, observation version or
page version derived from content mentioned after T is returned* — including through graph
expansion and including the Reflect agent's tool calls (`ReflectRequest.as_of` is applied to every
tool call of the session).

Worked example. A namespace holds:

| Id | Kind | `mentioned_at` | `occurred` | Text |
|---|---|---|---|---|
| F1 | fact | 2026-03-01 | 2026-03-01 | "Alice joined the platform team." |
| F2 | fact | 2026-03-10 | 2026-03-08 → 2026-03-09 | "Alice presented the sharding design at the offsite." |
| F3 | fact | 2026-05-02 | 2026-04-30 | "Alice moved to the search team." |
| F4 | fact | 2026-05-20 | 2025-11-15 | "Alice had interned on search in November 2025." (recalled late) |
| O1 v1 | observation | effective 2026-03-10 | — | "Alice is on the platform team and owns sharding." (sources F1, F2) |
| O1 v2 | observation | effective 2026-05-02 | — | "Alice moved from platform to search." (sources F1, F2, F3) |
| O1 v3 | observation | effective 2026-05-20 | — | "Alice has search background (intern 2025) and rejoined search in April 2026." (sources F1–F4) |

Query "which team is Alice on?" with `include_observations = true`:

| `as_of` | `query_timestamp` | Eligible facts | Observation returned | Note |
|---|---|---|---|---|
| unset | unset (now) | F1–F4 | O1 v3 | Normal recall. |
| 2026-04-01 | unset | F1, F2 | O1 v1 | F3/F4 hidden; v2/v3 have `effective_at > T`. Leak-free "as of April". |
| 2026-04-01 | 2026-04-01 | F1, F2 | O1 v1 | Same eligibility; temporal arm now ranks by distance to April 1 (F2 first). |
| 2026-05-10 | unset | F1–F3 | O1 v2 | F4 hidden **even though it occurred in 2025**: `as_of` is about when it was *mentioned*, not when it happened. |
| 2026-02-01 | unset | none | none | O1's first version is later than T → observation absent, not "empty". |
| unset | 2025-12-01 | F1–F4 | O1 v3 | Anchoring to the past hides nothing; F4 ranks first in the temporal arm (occurred nearest). |

Edge rules:

- `as_of` in the future is accepted and is a no-op (equivalent to unset); `as_of` older than
  every `mentioned_at` returns an empty stream plus `RecallStats` with `total_candidates = 0`.
- `as_of` is orthogonal to the visibility predicate (N116): tombstoned documents, chunk tombstones
  and `fact_hidden` markers always apply, and `as_of` never resurrects an invalidated fact even if it
  was invalidated after T (curation is not time-travelled; a curator's decision is meant to
  stick). A `fact_hidden(invalidate)` marker also hides every observation or page version whose
  **evidence segment** names the fact, superseded ones included (N117), so an old `as_of` cannot
  serve a version written with the invalidated fact in view; `Restore` deletes the marker and
  brings exactly those versions back, because nothing else encodes the invalidation. A
  `fact_hidden(reextract)` row (a prompt or model bump hid a stale extraction) hides only that fact
  from the fact and chunk arms: derived versions that name it stay visible and are rebuilt (N135).
- **The served version is the one current at `T`.** When it is hidden, nothing is served for that
  observation or page at `T`, not an older version; retirement is filtered by its own time
  (`retired_at IS NULL OR retired_at > as_of`); a version with no visible source
  (`proof_count = 0`) is not served; and a predicate that finds no version row fails closed (N117,
  N135).
- `as_of` never resurrects an observation version derived from deleted content: a version whose
  segment (`inputs(O, w)` for `root_version(v) ≤ w ≤ v`) names a tombstoned document stays hidden
  at every T, permanently, through the `derived_hidden` row the expunge materializes (N117, N119),
  even after the observation was rebuilt and is live again. Only versions written without the
  deleted content are servable in their `[effective_at, superseded_at)` range. Nothing is computed
  at delete time, so a version that commits after the marker and names the victim is hidden too.
- Under `as_of`, `EntityRef` carries only `mention`; `canonical_name` and alias merges are
  suppressed and graph entity hops use `entity_mentions.mentioned_at ≤ T` (N118).
- `said_at` plays no role in `as_of`: a fact whose source quotes an older date is still hidden
  until the item that carried it was retained.
- `as_of` is **not** snapshot isolation. A fact with `mentioned_at ≤ T` that is committed while
  the query runs may appear in the next query and not this one. Leak-free means "no future
  content", not "repeatable read"; the evaluation harness waits for `SUCCEEDED` on every retain
  before issuing queries (D16 read barrier).
- `RecallStats.as_of_applied`, `query_timestamp_applied` and `temporal_window_applied` echo what
  the server used, so an evaluator can assert the cut-off it asked for was honoured.
- `GetPage.as_of` selects the page version by the same rule; `NOT_FOUND{PAGE}` when no version is
  effective at T (a page cannot be shown "empty" — it would be a page from the future with the
  text removed).

### 4.5 Evolution policy

**Tooling.** `buf breaking` runs on every pull request with the `FILE` category — the strictest:
besides wire and JSON compatibility it forbids moving a definition between files and changing
`go_package`, because both break generated Go import paths for every consumer. A failure blocks
the merge; there is no `--exclude` override in CI. **The baseline is stated as what is true
(N139, N157, N167).** No release tag exists before `v1.0.0`, so until then a **pull request is compared
with the merge target** (from the repository root: `buf breaking plans/engram/proto --against
'.git#branch=main,subdir=plans/engram/proto'`; in the real repository the module path is `proto`),
so a break against main's latest commit is reported and a labelled break already on main is not
reported again by every open PR. **Bootstrap rule:** `buf breaking` fails ("had no .proto files") when the
baseline subdirectory does not exist on the merge target, which is the case for the pull request that first
introduces `proto/` and for every plan pull request while `main` has no `plans/engram/`; the job therefore first
runs `git cat-file -e origin/main:plans/engram/proto/buf.yaml`, and when that fails it runs lint, build and format
only and records "no baseline" in the job summary. Plan pull requests compare against the plan branch's merge
base (`--against ".git#ref=$(git merge-base origin/claude/engram-implementation-plan HEAD),subdir=plans/engram/proto"`);
the commands are in `proto/README.md`. `main@HEAD~1` (`ref=HEAD~1`) is used **only by the push-to-main
job**. Every intended break needs a `buf-breaking-exception` label on the pull request and an entry
in the changelog table below. From `v1.0.0` on the baseline is the latest release tag
(`.git#tag=proto/v1.0.0,…`), so the gate compares against what clients actually run.

**What counts as breaking** (and is therefore forbidden within `memory.v1`):

| Change | Breaking? | Rule |
|---|---|---|
| Delete or rename a file, message, enum, service, RPC, field or enum value | yes | Deprecate instead (below). Renames are a delete + add. |
| Change a field's number, type, cardinality (`optional`/`repeated`), `json_name`, or move it in/out of a `oneof` | yes | Add a new field, deprecate the old one. |
| Change an RPC's request/response type or streaming kind | yes | Add a new RPC (`RecallV2` is not a name we will ever use — add a field to `RecallRequest` instead; if a new shape is unavoidable it goes to `memory.v2`). |
| Change `package` or `go_package` | yes | Never. |
| Delete a `reserved` range or name | yes (FILE) | Reservations are permanent. |
| Change an enum's zero value | yes | Never. |
| Add a field, message, enum value, RPC, service, `oneof` member | **no** | Allowed in any minor release; new request fields must have a server-side default that reproduces the previous behaviour. |
| Add `[deprecated = true]` or change a comment | no | — |
| Tighten server validation on an existing field (e.g. lower a limit) | *semantically* breaking | Treated as breaking by policy; requires a deprecation window even though buf cannot see it. Loosening is fine. |
| Change the meaning of an existing enum value or field | semantically breaking | Forbidden; add a new value/field. |

**Deprecation.** A field, value or RPC is retired in three steps: (1) mark `[deprecated = true]`
and add a comment `// Deprecated since v1.4: use X.` in the same release that ships the
replacement; (2) keep serving it — identical behaviour — for **at least two minor releases**
(≈ 2 quarters at the planned cadence); (3) removal happens **only in a new major package**
(`memory.v2`), served side by side with `memory.v1` for at least two minor releases, with the
`v1` handler delegating to the `v2` implementation. Nothing is ever removed from `memory.v1`
itself. Before `v1.0.0` ships a break is still possible, but only through the labelled exception
procedure above; the `reserved` entries in the files (`RetainRequest.wait_for_visibility = 5`,
`RecallRequest.min_score = 17`, `TagFilter.include_untagged = 3`, `RetainItem.mentioned_at = 9`)
are the permanent record of those removals.

**Pre-1.0 exceptions (N128, N139; reviews A-9, A-11, A-12, A-13).** Nothing has shipped at
`v1.0.0`, so the D22 and D23 redesigns changed the contract incompatibly where the old shape
encoded withdrawn machinery. The breaks are intended, listed here as the changelog the exception
procedure requires, and every removed number or name is `reserved`. The gate's baseline moved past
the round-3 rows when they were committed, so this table, not the tool, is their record:

| Round | Module | Break | Why |
|---|---|---|---|
| 3 | `memory.v1` | `DeleteDocumentResponse` fields 2 to 4 (cascade counts) reserved; `deleted_at` and `expunge_sla` added | The delete is an O(1) marker; nothing is counted at delete time (N115). |
| 3 | `memory.v1` | `OperationKind` 7 (`MOVE_NAMESPACE`) reserved; `DELETE_TENANT` (8), `Operation.cancel_reason` (14), `OperationResult.superseded_by` (10) added | A move is not an operation; tenant deletes and superseded retains were inexpressible (N127). |
| 3 | `memory.v1` | `Namespace.directives` (6) reserved, `reflect_directives` (13) added; `GetEffectiveConfig` added | Typed directives (N129). |
| 3 | `memory.v1` | `DocumentBusy`, `InputBlobMissing`, `FREEZE_REASON_FENCE_BUSY` (4) and `WrongShardOrEpoch` fields 5 and 6 left the package | Internal details move to `engram.internal.errors.v1` (N128). |
| 3 | `memory.v1` | `OperationConflictReason` 2 (`DOCUMENT_PURGING`) reserved | A deleted `document_id` can be retained again at once (N133c). |
| 3 | `memory.admin.v1` | `MoveState.CATCHING_UP` (3), `MoveProgress` 4 to 7 and `Move.operation_id` (13) reserved; `RECONCILING`, `CutoverStep`, `GetTenantOperation` added | Replay machinery withdrawn (N124, N127). |
| 3 | `internal.events` | Every `string → bytes` id field moved to a new number (old number reserved); `DocumentDeleted` lost its id lists; `ChunksRetired` and `RowsPurged` and oneof 40 to 47 removed; `schema_version` 2 | N80 retype done safely; N81 events withdrawn (N124). |
| 3 | `internal.workflow` | Lineage consolidation messages replaced by the two-stage shapes; `Purge*` replaced by `Expunge*`; `ExportInput` lost the outbox cut; `MoveCheckpoint`/`MoveResult` lost the replay floor; `item_index` (9) stays reserved; `schema_version` 2 | N121, N119, N126, N124. |
| 4 | `internal.events` | The retyped `bytes` fields are renamed `*_bytes` and their old names reserved: `ChunkCommitted` 14 to 16, `FactInvalidated` 4, `FactRestored` 2, `ObservationUpserted` 11 and 12, `ObservationRetired` 4, `ObservationsMarkedStale` 7 and 8, `EntityUpserted` 5, `EntitiesMerged` 7 and 8, `PageVersionCreated` 5, `PageDeleted` 2, `PagesMarkedStale` 8 | The old JSON names stayed in use with a new type, which changes what a protojson consumer decodes (A-9). |
| 4 | `internal.workflow` | `ExpungeInput.batch_pause` (7) reserved, `wal_mb_per_s` (10) added | The purge is paced by the WAL it writes, not by a fixed pause (N119). |
| 5 | `memory.v1` | `SnapshotManifest.expired_at` (16) renamed `expires_at` (same number; the old name is reserved) | The manifest field has the DDL column `export_snapshots.expires_at` as its source (N157, A-10). |
| 6 | `memory.admin.v1` | `MoveState.COPYING` (2) and `RECONCILING` (9) reserved, `COPIED` (11) added; `MoveProgress` 10 to 15 reserved (`copy_started_at`, `rows_recopied`, `mutable_rows_reconciled`, `blobs_reconciled`, `freeze_watchdog`, `rows_catchup_copied`), `rows_estimated` and `verify_attempts` added; `Move` 18 to 20 reserved (`reconciled_in_at`, `cleanup_reconciled_at`, `target_content_checked_at`), `window_estimate`, `window`, `freeze_deadline` and `activated_at` added; `CleanupMoveRequest.skip_grace` (3) reserved; `StartMoveRequest` gains `window`, `freeze_not_before`, `estimate_only` | The move is freeze-then-copy: no dirty copy, no reconcile, no `ReconcileIn`, no content gate (N160, N161). |
| 6 | `memory.admin.v1` | `UpdateTenantRequest.update_mask` is limited to `display_name` and `config` (a semantic tightening, pre-1.0); `UpdateTenantLimits` added | One scope per method, so the interceptor stays the only enforcement point (N167, A-6). |
| 6 | `internal.workflow` | `PreVerifyReport`, `AwaitIndexesReport`, `ReconcileReport` and `ReconcileInReport` deleted, `VerifyFrozenReport` and `BuildIndexesReport` added; `MoveInput.freeze_watchdog` (12) reserved; `MoveCheckpoint` 10, 14, 15, 16, 18 and 19 reserved; `MoveResult.rows_recopied` (6) reserved | The withdrawn machinery has no payload (N160). |
| 7 | `memory.admin.v1` | `Move.target_backup_at` (17) and `target_backup_started_at` (25) renamed (same numbers; old names reserved); `source_blobs_gc_after` (16, derived as `finished_at + 28 d`) and `rerun_count` (26) reserved | The re-run is deleted (N169, N170); the renames were never in a tagged tree. |
| 7 | `internal.workflow` | `MoveInput.rerun_attempt` (16) reserved | The re-run is deleted (N169). |
| 7 | `internal.events` | `DocumentDeleted` reserves the names of fields 2 to 6 and 8 to 15 | protojson names could still be recycled (N177). |
| 8 | `memory.admin.v1` | `Move` 17, 25 (`move_backup_at`, `move_backup_started_at`; second removal of these numbers, pre-1.0) and 11 (`cutover_at`) reserved; `copy_end_lsn`, `copy_end_timeline`, `copy_sealed_at`, `committed_replicated_at`, `rolled_back_replicated_at`, `finished_at`, `reconciled_at`, `lost_at`, `lost_restore_id`, `recovered_from_move_id` and `MOVE_STATE_LOST` added | The seal's WAL floor; a lost move-in is terminal (N179, N183, N184). |
| 8 | `internal.workflow` | `MoveBackupResult` replaced by `SealCopyResult` (name not reused); `MoveCheckpoint` 13 renamed `cutover_sub_step`, `cutover_step` reserved with 9 | N179, N187. |
| 8 | `memory.v1` | `Operation.acknowledged_at` (15) added; `request_id` required on the replicated-ack methods | N182, N184. |

Every removed field number and name is `reserved`. CI runs `buf breaking --config WIRE_JSON` against the merge base, non-gating,
with the generated ignore list of this table's renames (N177, N187).

The `string → bytes` retype of event ids is the one case where the earlier edit was *silently*
unsafe (the same number decoded a 36-byte ASCII UUID as a 16-byte id); it is therefore done with
new numbers and, since round 4, new names, not in place.

**Reserved numbers.** Every top-level request/response and resource message carries
`reserved 100 to 199;` — upstream never assigns those numbers, so a downstream fork (an enterprise
build, a research branch) can add private fields there and still merge upstream releases without
renumbering. Removed fields are always both `reserved N;` and `reserved "name";` with a comment
naming the release and the reason, so that neither the number nor the JSON name can be recycled.

**Enum growth.** Enums are append-only; the zero value is `*_UNSPECIFIED` and never changes
meaning. Clients must treat unknown values as `UNSPECIFIED` (proto3 open enums make this
automatic in Go; JSON clients get the numeric value and must not crash). Servers reject unknown
values **in requests** with `INVALID_ARGUMENT` (`UNKNOWN_ENUM_VALUE`) — a newer client talking to
an older server should learn immediately rather than get silently degraded behaviour. State enums
in responses (`OperationState`, `NamespaceState`, `MoveState`) may grow; clients switch with a
default branch.

**Unknown-field tolerance.** Servers ignore unknown fields in requests (proto3 default; for
Connect JSON the server unmarshals with `DiscardUnknown = true`), so a newer client can send a
field an older server does not know and still be served — but such a field is by construction a
no-op, never a silent change of semantics (this is why every new request field must default to
the previous behaviour). Clients must preserve unknown fields on round-trips of resources they
update (the Go runtime does; JSON clients must send only the masked paths, which `update_mask`
makes natural).

**Versioning of Temporal payloads and events** (`buf.build/engram/internal`):

- Both internal packages are published to the buf registry as module `buf.build/engram/internal`
  (the public/admin packages as `buf.build/engram/memory`) by `buf push --label v<semver>` from
  the release pipeline; consumers (the worker binary, any Kafka consumer, the external index
  adapter) pin a commit in their `buf.lock`. Kafka messages carry header
  `schema=engram.internal.events.v1.Event` and the BSR commit in `schema_commit` (D6).
- Every top-level workflow input and the event envelope carries `schema_version` (an integer;
    **2** for everything written by the D22 and D23 builds: nothing has shipped, so there are no
  running histories whose interpretation round 4 could change, N128, N139). Adding fields never bumps it (histories
  replay with defaults). Workflow inputs that had none (`TenantDeleteInput`, `RetainBackfillInput`,
  `ReembedNamespaceInput`, `SweeperInput`, `ExpungeInput`) carry one from the start. A field whose
  interpretation changes bumps it, and the workflow code branches on `schema_version` for inputs
  and on `workflow.GetVersion(ctx, "change-id", …)` for logic, keeping the old branch until every
  history started under it has finished: retains and purges are hours old at most, the
  per-namespace `Consolidate` workflow is perpetual but `ContinueAsNew`s every ≤ 1000 events, so
  a 30-day drain window covers every branch, after which the old branch is deleted.
- Event consumers must skip an `Event` whose `payload` oneof is unknown to them (it arrives as an
  unknown field), advance their cursor, and increment `events_unknown_payload_total`; they must
  never fail the relay on it. New payload variants are therefore additive and safe; no consumer
  needs every variant any more, since the move replayer is gone.
- The internal module follows the same `buf breaking FILE` gate as the public one, even though it
  is never served: a running Temporal history *is* a wire client that cannot be upgraded.

**Release mechanics.** `memory.v1` is frozen-compatible from `v1.0.0`; the proto module version
and the Go module version move together (`proto/v1.3.0` git tag = `buf push --label v1.3.0` =
Go `v1.3.0`). The changelog lists every added field/RPC with the release it appeared in, so a
client can state its minimum server version.

### 4.6 MCP tool → gRPC method mapping

`engram-mcp` (D1 binaries) is a thin adapter: one Streamable-HTTP MCP endpoint per namespace at
`/mcp/{tenant_id}/{namespace_id}`, one gRPC client to `engram-api`, zero business logic. The
path fills `NamespaceRef`; the MCP client's `Authorization: Bearer <jwt>` header is forwarded
**unchanged** as gRPC metadata, so the core `authz.Interceptor` is the only enforcement point
(D13). The adapter is also an OAuth resource server (N129, A-25): it validates the token with the
same `TokenVerifier` **before listing tools**, answers an unauthenticated request with HTTP 401 and a
`WWW-Authenticate` challenge, and serves `/.well-known/oauth-protected-resource` (RFC 9728) per
endpoint, so standard MCP clients can complete an OAuth flow; it inspects no claim except to decide
which tools to *list*. Write tools are
listed by `tools/list` only when the JWT carries `memory.write` and are enforced by the core on
`tools/call` regardless of the listing. Every tool call sets the gRPC deadline from the table
(clients may lower it with the MCP `timeout` meta field, never raise it).

| MCP tool | Gate | gRPC method | Argument → field mapping | Result shaping | Deadline |
|---|---|---|---|---|---|
| `recall` | read | `MemoryService.Recall` (stream) | `query`, `budget` (`low\|mid\|high`), `as_of`, `query_timestamp` (RFC 3339), `tags` + `tag_match` (`any\|any_strict\|all\|all_strict\|exact`), `fact_types`, `include_chunks`, `include_observations`, `prefer_observations`, `max_results`, `max_tokens`, `explain` | Stream collected; returns `{results:[{rank, id, kind, text, mentioned_at, occurred, tags, provenance, scores}], stats}` as JSON `content` plus a text rendering (one line per result: `[rank] (kind, date) text`) | 10 s |
| `retain` | **write** | `MemoryService.Retain` | `items[]{content, timestamp, context, document_id, tags, metadata, update_mode}`; `request_id = sha256(mcp-session-id ‖ jsonrpc-id ‖ tool)` (N127: JSON-RPC ids are per-session counters and never a key by themselves) | `{operations:[{id, document_id, state}]}` and the sentence "accepted; facts appear asynchronously — poll `get_operation`" | 30 s |
| `reflect` | read | `MemoryService.Reflect` (stream) | `query`, `budget`, `as_of`, `tags`/`tag_match`, `output_schema`, `max_iterations` | Token deltas become MCP `notifications/progress` (`progressToken` from the call); tool calls become progress messages `searching observations…`; the final `content` is the answer text (and `structured` as JSON content when a schema was given) followed by a `citations` list | 330 s |
| `get_memory` | read | `MemoryService.GetMemory` | `memory_id`, optional `fields[]` → `read_mask` | The `Memory` as JSON | 10 s |
| `get_operation` | read | `OperationService.WaitOperation` (`timeout` ≤ 30 s; the non-blocking `OperationService.GetOperation` is the generated `get_operation_status`) | `operation_id`, `wait_seconds` | `{state, progress, error, result}` | 35 s |
| `list_documents` | read | `DocumentService.ListDocuments` | `tags`/`tag_match`, `prefix`, `updated_after`, `page_token`, `page_size` | `{documents:[…], next_page_token}` | 10 s |
| `delete_document` | **write** | `DocumentService.DeleteDocument` | `document_id`, optional `expected_version` | `{operation_id, deleted_at, expunge_sla}` and the sentence "deleted: no longer returned by any read; physical purge tracked by operation …" (the ack is the committed soft-delete marker plus its durable intent, N115, N122) | 40 s |
| `get_page` | read | `PageService.GetPage` (`include_content = true`) | `page_id` or `name`, `as_of`, `version` | Markdown as text `content` plus `{stale_write, stale_delete, version, effective_at}` | 10 s |
| `list_pages` | read | `PageService.ListPages` | `stale_only`, `prefix`, `page_token` | `{pages:[{page_id, name, current_version, stale_write, stale_delete}], next_page_token}` | 10 s |
| `search_pages` | read | `PageService.SearchPages` | `query`, `tags`/`tag_match`, `as_of`, `budget`, `page_token` | `{hits:[{rank, page_id, name, version, score, snippet}], next_page_token}` (N73; the same tool the Reflect agent calls) | 10 s |

**The full generated tool set (N157, A-17).** `protoc-gen-engram-mcp` generates one tool per RPC of
`memory.v1` that appears in the checked-in allow-list `adapters/mcp/allow.txt`, and the build fails
for an RPC that is neither allowed nor listed under `omit:`, so the surface cannot drift in either
direction. The ten tools above are the hand-tuned argument and result shapes; the rest use the
generated JSON mapping of the request and response messages, with the gate of 4.1.9 and the
deadline of 4.1.2:

| MCP tool | Gate | gRPC method | Deadline |
|---|---|---|---|
| `list_memories`, `batch_get_memories` | read | `MemoryService.ListMemories`, `BatchGetMemories` | 10 s |
| `invalidate` | **write** | `MemoryService.Invalidate` | 30 s |
| `restore` | **write** | `MemoryService.Restore` (exclusive derivation lock) | 40 s |
| `get_document`, `get_document_version`, `get_document_body`, `list_tags` | read | `DocumentService.GetDocument`, `GetDocumentVersion`, `GetDocumentBody`, `ListTags` | 10 s |
| `update_document_tags` | **write** | `DocumentService.UpdateDocumentTags` | 10 s |
| `list_operations`, `get_operation_status` | read | `OperationService.ListOperations`, `GetOperation` | 10 s |
| `cancel_operation` | **write** | `OperationService.CancelOperation` | 10 s |
| `get_namespace`, `get_effective_config` | read | `NamespaceService.GetNamespace`, `GetEffectiveConfig` | 10 s |
| `create_snapshot` | **write** | `ExportService.CreateSnapshot` | 30 s |
| `get_snapshot_manifest`, `list_snapshots` | read | `ExportService.GetSnapshotManifest`, `ListSnapshots` | 10 s |
| `create_page`, `update_page`, `delete_page`, `refresh_page` | **write** | `PageService.CreatePage`, `UpdatePage`, `DeletePage`, `RefreshPage` | 30 s |

**The set closes (N167, A-5).** The 36 `memory.v1` RPCs are the ten hand-tuned tools above, the twenty-one generated
tools of the table (`get_operation_status` for `GetOperation` included) and the five omitted ones below
(`ListNamespaces` among them: a per-namespace endpoint has no use for a directory), so no RPC is in neither list. The generated tools call through `DataClients{Memory, Document, Page,
Operation}` and `AdminClients{Namespace, Export}` (§2.2.25).

**Omitted on purpose** (the `omit:` list, named in the §12 non-goal row): `list_namespaces` (`ListNamespaces`), `create_namespace`,
`update_namespace` and `delete_namespace` (namespace lifecycle, mission, directives and disposition
are `memory.admin` calls through the API, not agent tools; this is where Hindsight's
`create/delete_directive` and `get/update_bank` have no MCP counterpart), `stream_snapshot` (binary
parts do not fit a tool result; agents sync with `engram-sync`) and every `memory.admin.v1` service.

Error mapping: a gRPC error becomes an MCP tool result with `isError: true` and text
`<CODE>: <message>` followed by the JSON of the typed detail (so an agent can read
`retry_after`). `UNAUTHENTICATED`/`PERMISSION_DENIED` on the endpoint itself are HTTP 401/403
before any MCP framing. Optional MCP **resources** (read-only, no gate beyond `memory.read`):
`engram://memory/{memory_id}` → `GetMemory`, `engram://page/{name}` → `GetPage`. Rejected: a
single multi-namespace endpoint with `namespace_id` as a tool argument — per-namespace URLs make
the allowlist check trivial, let one agent process mount several namespaces as distinct servers,
and make the audit log unambiguous.

### 4.7 REST/JSON: ConnectRPC

**Decision: ConnectRPC (connect-go), not grpc-gateway.** Rationale: the *same* handler serves gRPC,
gRPC-Web and Connect on one port — no second process, no second deployment, no proxy hop in the
latency budget; no `google.api.http` annotations that drift from the methods they decorate (and
that would have forced a BSR dependency the workspace deliberately avoids); Connect speaks
HTTP/1.1 **and** HTTP/2 with plain JSON bodies, so curl, browsers and serverless runtimes work
without a gRPC stack; server streaming is supported natively (enveloped frames over a chunked
response), which grpc-gateway only approximates with newline-delimited JSON and no trailer
semantics; the generated Connect client is idiomatic Go and the protocol is documented enough to
call from any HTTP library. Rejected: **grpc-gateway** — a separate reverse-proxy process with its
own config, its own timeouts and its own error format, driven by HTTP annotations in the protos
that must be kept in sync by hand; it exists to produce "pretty" REST paths (`GET
/v1/namespaces/{id}/memories/{mid}`), which no client of a memory service needs and which cost a
routing table to maintain. Also rejected: hand-written REST — an unbounded source of drift.

**Route table.** Every method is `POST /<package>.<Service>/<Method>` with `Content-Type:
application/json` (or `application/proto`); methods marked `idempotency_level = NO_SIDE_EFFECTS`
additionally accept `GET` with the request in the `message` query parameter
(`?encoding=json&message=<url-encoded JSON>`). A GET still needs the `Authorization` and
`Connect-Timeout-Ms` headers, so a URL is **not** bookmarkable and responses are `Cache-Control:
private, no-store` (N129, A-20); GET exists so that simple read clients need no request body.

| Route (POST unless noted) | gRPC method | Kind |
|---|---|---|
| `/memory.v1.MemoryService/Retain` | Retain | unary |
| `/memory.v1.MemoryService/Recall` | Recall | server stream |
| `/memory.v1.MemoryService/Reflect` | Reflect | server stream |
| `/memory.v1.MemoryService/GetMemory` (+GET) | GetMemory | unary |
| `/memory.v1.MemoryService/ListMemories` (+GET) | ListMemories | unary |
| `/memory.v1.MemoryService/BatchGetMemories` (+GET) | BatchGetMemories | unary |
| `/memory.v1.MemoryService/Invalidate`, `/Restore` | Invalidate, Restore | unary |
| `/memory.v1.DocumentService/GetDocument` (+GET), `/ListDocuments` (+GET), `/GetDocumentVersion` (+GET), `/GetDocumentBody` (+GET), `/ListTags` (+GET), `/UpdateDocumentTags`, `/DeleteDocument` | DocumentService | unary |
| `/memory.v1.NamespaceService/CreateNamespace`, `/GetNamespace` (+GET), `/ListNamespaces` (+GET), `/GetEffectiveConfig` (+GET), `/UpdateNamespace`, `/DeleteNamespace` | NamespaceService | unary |
| `/memory.v1.OperationService/GetOperation` (+GET), `/ListOperations` (+GET), `/CancelOperation`, `/WaitOperation` | OperationService | unary (Wait = long-poll) |
| `/memory.v1.ExportService/CreateSnapshot`, `/GetSnapshotManifest` (+GET), `/ListSnapshots` (+GET) | ExportService | unary |
| `/memory.v1.ExportService/StreamSnapshot` | StreamSnapshot | server stream |
| `/memory.v1.PageService/CreatePage`, `/GetPage` (+GET), `/ListPages` (+GET), `/SearchPages` (+GET), `/UpdatePage`, `/DeletePage`, `/RefreshPage` | PageService | unary |
| `/memory.admin.v1.TenantService/GetTenant`, `/DeleteTenant`, `/GetTenantOperation`, `/UpdateTenant` | tenant lifecycle (`tenant.admin`, tenant-bound) | unary; tenant-facing listener |
| `/memory.admin.v1.TenantService/CreateTenant`, `/ListTenants`, `/UpdateTenantLimits`, `/GetTenant`, `/DeleteTenant`, `/GetTenantOperation`, `/memory.admin.v1.ShardService/*`, `/memory.admin.v1.MoveService/*` | admin (`engram.operator`) | unary; separate Envoy route, not exposed to tenants |
| `/memory.admin.v1.ShardService/ReleaseNamespace`, `/memory.admin.v1.MoveService/CleanupMove` | worker (`engram.worker`, bound to its cell) | unary; separate Envoy route |

**Streaming behaviour under Connect.** A server-streaming call is `POST` with `Content-Type:
application/connect+json` (or `application/connect+proto`); the request body is one enveloped
message (1 flag byte + 4-byte big-endian length + payload), the response is a chunked stream of
enveloped messages, terminated by an *EndStreamResponse* envelope (flag `0x02`) whose JSON body
carries the error (if any) and trailers. Errors that occur mid-stream therefore arrive in-band,
after any results already flushed — exactly the gRPC semantics of "results then status". This
works over HTTP/1.1 (chunked transfer) and HTTP/2; browsers use the same protocol through
`fetch` streaming, and `buf curl --protocol connect` consumes it from a shell (4.8). The deadline
header is `Connect-Timeout-Ms` and is required like `grpc-timeout`.

**JSON mapping notes** (protobuf-JSON canonical mapping, applied by connect-go): field names are
lowerCamelCase (`namespaceId`, `asOf`) with the proto names also accepted on input; `Timestamp` is
RFC 3339 (`"2026-04-01T00:00:00Z"`); `Duration` is `"1.5s"`; `bytes` is base64; enums are the
string names (`"BUDGET_MID"`); `int64` is a JSON string; `FieldMask` is `"text,scores"`;
`Struct` is a JSON object; `oneof` members appear as at most one key; `optional bool` absent vs
`false` is preserved. Unknown JSON keys are discarded (4.5). CORS is enabled for the Connect
routes with an allowlist of origins from config; gRPC-Web is served for browsers that need it.

### 4.8 Example calls

Assume `engram-api` at `api.engram.local:8443`, `$JWT` with scopes `memory.read memory.write`,
tenant `acme`, namespace `018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e`, and `grpcurl` given the proto
files (`-import-path plans/engram/proto -proto memory/v1/memory.proto` — gRPC reflection is also
served, for `memory.v1` only; the admin surface is not discoverable through it, N71).

**Retain** (unary; deadline 10 s; idempotent by `request_id`):

```bash
grpcurl -H "authorization: Bearer $JWT" -max-time 10 \
  -import-path plans/engram/proto -proto memory/v1/memory.proto \
  -d @ api.engram.local:8443 memory.v1.MemoryService/Retain <<'JSON'
{
  "namespace": {"tenantId": "acme", "namespaceId": "018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
  "meta": {"requestId": "ingest-session-2026-09-30-0007"},
  "operationId": "01925c1e-0f3a-7f6b-8a2c-2f1e4d5c6b7a",
  "items": [{
    "documentId": "session-2026-09-30",
    "content": "Alice: I'm moving to the search team next month.\nBob: Congrats — when exactly?\nAlice: April 30th.",
    "timestamp": "2026-03-10T15:04:05Z",
    "context": "Slack DM between Alice and Bob",
    "tags": ["slack", "team-changes"],
    "metadata": {"channel": "D0123"},
    "updateMode": "UPDATE_MODE_REPLACE"
  }]
}
JSON
```

Response (`RetainResponse`): one operation, `state: OPERATION_STATE_PENDING`, `kind:
OPERATION_KIND_RETAIN_DOCUMENT`, `targetId: "session-2026-09-30"`, `consolidationLag: "45s"`.

**WaitOperation** (the read barrier; deadline 35 s, server wait 30 s):

```bash
grpcurl -H "authorization: Bearer $JWT" -max-time 35 \
  -import-path plans/engram/proto -proto memory/v1/operation.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "operationId":"01925c1e-0f3a-7f6b-8a2c-2f1e4d5c6b7a","timeout":"30s"}' \
  api.engram.local:8443 memory.v1.OperationService/WaitOperation
```

```json
{"operation": {"id": "01925c1e-0f3a-7f6b-8a2c-2f1e4d5c6b7a", "kind": "OPERATION_KIND_RETAIN_DOCUMENT",
  "state": "OPERATION_STATE_SUCCEEDED", "targetId": "session-2026-09-30",
  "progress": {"unitsTotal": 1, "unitsDone": 1, "phase": "finalize"},
  "result": {"documentVersion": "1", "factsWritten": 3, "chunksReused": 0},
  "finishedAt": "2026-09-30T22:58:11.204Z"}, "timedOut": false}
```

**Recall with `as_of`** (server stream; deadline 3 s; the last message is the stats):

```bash
grpcurl -H "authorization: Bearer $JWT" -max-time 3 \
  -import-path plans/engram/proto -proto memory/v1/memory.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "query":"which team is Alice on?","budget":"BUDGET_MID",
       "asOf":"2026-04-01T00:00:00Z","queryTimestamp":"2026-04-01T00:00:00Z",
       "tagFilter":{"mode":"TAG_MATCH_MODE_ANY","tags":["slack","hr"]},
       "includeObservations":true,"explain":true}' \
  api.engram.local:8443 memory.v1.MemoryService/Recall
```

```json
{"result": {"rank": 1, "tokens": 21, "memory": {"id": "01925c1e-1a2b-7c3d-8e4f-5a6b7c8d9e0f",
  "kind": "MEMORY_KIND_FACT", "text": "Alice is moving to the search team next month.",
  "factType": "FACT_TYPE_EXPERIENCE", "who": "Alice", "what": "moving to the search team",
  "occurred": {"occurredStart": "2026-04-30T00:00:00Z", "occurredEnd": "2026-04-30T00:00:00Z"},
  "mentionedAt": "2026-03-10T15:04:05Z", "tags": ["slack", "team-changes"],
  "provenance": {"documentId": "session-2026-09-30", "documentVersion": "1",
                 "chunkId": "01925c1e-1a2b-7c3d-8e4f-000000000001", "chunkOrdinal": 0,
                 "charStart": 0, "charEnd": 98},
  "scores": {"semantic": 1.0, "lexical": 0.71, "rrf": 0.0325, "rerank": 0.93, "boost": 1.06,
             "final": 0.9858, "stage": "RECALL_STAGE_RERANKED",
             "armRanks": [{"arm": "semantic", "rank": 1}, {"arm": "lexical", "rank": 2}]}}}}
{"result": {"rank": 2, "...": "..."}}
{"stats": {"totalCandidates": 37, "returnedCount": 2, "skippedCount": 0, "tokensUsed": 44,
  "maxTokens": 8192, "lastStage": "RECALL_STAGE_RERANKED",
  "stageTimings": [{"stage": "authz", "duration": "0.001s"}, {"stage": "embed", "duration": "0.024s"},
                   {"stage": "arm:semantic", "duration": "0.031s", "candidatesOut": 12},
                   {"stage": "arm:lexical", "duration": "0.018s", "candidatesOut": 9},
                   {"stage": "arm:graph", "duration": "0.027s", "candidatesOut": 21},
                   {"stage": "arm:temporal", "duration": "0.012s", "candidatesOut": 4},
                   {"stage": "arm:chunk", "duration": "0.001s", "skipped": true},
                   {"stage": "fuse", "duration": "0.001s", "candidatesIn": 46, "candidatesOut": 37},
                   {"stage": "rerank", "duration": "0.095s", "candidatesIn": 37, "candidatesOut": 37},
                   {"stage": "boost", "duration": "0.001s"}, {"stage": "pack", "duration": "0.002s"}],
  "asOfApplied": "2026-04-01T00:00:00Z", "queryTimestampApplied": "2026-04-01T00:00:00Z",
  "total": "0.231s"}}
```

Nothing mentioned after 2026-04-01 appears, and `asOfApplied` echoes the cut-off so an evaluator
can assert it. `semantic`/`lexical` are the per-query normalised arm scores (the top candidate of
an arm scores 1.0; N67), `arm:graph` is the second seeding wave (30 ms sub-budget, N54) and the
rerank ran on ≤ 50 pairs (MID, N53).

**Connect, unary, plain curl** (`GetMemory` via GET because it is `NO_SIDE_EFFECTS`; the deadline
header is mandatory):

```bash
curl -sS "https://api.engram.local:8443/memory.v1.MemoryService/GetMemory?encoding=json&message=$(
  python3 -c 'import urllib.parse,json;print(urllib.parse.quote(json.dumps({
    "namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
    "memoryId":"01925c1e-1a2b-7c3d-8e4f-5a6b7c8d9e0f","readMask":"text,mentionedAt,tags"})))')" \
  -H "Authorization: Bearer $JWT" -H "Connect-Protocol-Version: 1" -H "Connect-Timeout-Ms: 5000"
```

```bash
curl -sS -X POST https://api.engram.local:8443/memory.v1.MemoryService/Invalidate \
  -H "Authorization: Bearer $JWT" -H "Content-Type: application/json" \
  -H "Connect-Protocol-Version: 1" -H "Connect-Timeout-Ms: 5000" \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "meta":{"requestId":"curate-0042"},"memoryId":"01925c1e-1a2b-7c3d-8e4f-5a6b7c8d9e0f",
       "reason":"superseded by HR record"}'
```

A missing `Connect-Timeout-Ms` yields HTTP 400 with body
`{"code":"invalid_argument","message":"deadline required","details":[{"type":"memory.v1.ValidationError",…}]}`.

**Connect, server stream** (`buf curl` speaks the envelope so a shell can consume the stream):

```bash
buf curl --protocol connect --schema plans/engram/proto \
  -H "Authorization: Bearer $JWT" -H "Connect-Timeout-Ms: 3000" \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "query":"which team is Alice on?","asOf":"2026-04-01T00:00:00Z"}' \
  https://api.engram.local:8443/memory.v1.MemoryService/Recall
```

The output is the same sequence of `{"result":…}` messages and the trailing `{"stats":…}` as the
gRPC call; an error mid-stream appears as the EndStreamResponse `{"error":{"code":…}}` after the
results already delivered.

**Delete a document** (the ack is the delete; the operation tracks the expunge; delete-class deadline cap 40 s):

```bash
grpcurl -H "authorization: Bearer $JWT" -max-time 40 \
  -import-path plans/engram/proto -proto memory/v1/document.proto \
  -d '{"namespace":{"tenantId":"acme","namespaceId":"018f4c9e-7d3a-7c2b-9c1e-3f2a1b4c5d6e"},
       "meta":{"requestId":"forget-0042"},"documentId":"session-2026-09-30"}' \
  api.engram.local:8443 memory.v1.DocumentService/DeleteDocument
```

```json
{"operation": {"id": "01925c20-4b1d-7e0a-9f3c-6a7b8c9d0e1f", "kind": "OPERATION_KIND_DELETE_DOCUMENT",
  "state": "OPERATION_STATE_RUNNING", "targetId": "session-2026-09-30"},
 "deletedAt": "2026-10-07T09:12:44.118Z",
 "expungeSla": {"materializeWithin": "900s", "purgeWithin": "86400s", "indexRebuiltWithin": "172800s"}}
```

From `deletedAt` on, Recall, Reflect, GetMemory, ListMemories, GetPage, SearchPages and
StreamSnapshot return nothing derived from the document, and `GetDocument` shows only the
tombstone (`documentId`, `state: DOCUMENT_STATE_DELETING`, `deletedAt`, `upToVersion`,
`deleteOperationId`); `WaitOperation` on the returned id completes when the rows, blobs and derived
text are physically gone (≤ 24 h), and `CancelOperation` on it is refused with
`OPERATION_NOT_CANCELLABLE`. If the call times out before the ack, retry it: the retry finds the
marker, makes sure its intent exists and acks. A Retain into the same
`documentId` is accepted at once and starts a new version above the tombstone (N133).

### Round-8 changes

| Finding | Register | What changed in §4 and `proto/` |
|---|---|---|
| PG8-3, A8-1, PG8-4 | N179, N183 | `Move`: seal floor and `lost` fields, `MOVE_STATE_LOST`; `SealCopyResult`; `MOVE_TARGET_ARCHIVE_UNHEALTHY` |
| C8-5 | N182 | `Operation.acknowledged_at`; `DeleteTenant` durability point; declared lossy set (§4.1.6) |
| A8-3, A8-5, A8-12 | N184 | `request_id` required (`REQUEST_ID_REQUIRED`); `MOVE_PAST_COMMIT`; `CleanupMove` one transition |
| A8-6, A8-10 | N187 | `cutover_sub_step`; WIRE_JSON non-gating; changelog in round order |
