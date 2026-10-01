## 4. API contracts

The `.proto` files are the source of truth for every Engram surface: the generated Go stubs
(`gen/go`), the gRPC server, the ConnectRPC/JSON adapter and the MCP adapter are all derived
from them, never hand-written against them. This section fixes the conventions every method
obeys (4.1), inventories the files and their key semantics (4.2), pins down the two semantics
that are easy to get subtly wrong — tag matching (4.3) and `as_of` (4.4) — states the evolution
policy that CI enforces (4.5), maps MCP tools (4.6) and REST routes (4.7) onto gRPC methods, and
ends with worked calls (4.8).

All files live under `plans/engram/proto/` (in the real repository: `proto/`, decision D14):

```
proto/
├── buf.yaml                                  # v2 workspace: 2 modules, lint STANDARD, breaking FILE
├── buf.gen.yaml                              # protoc-gen-go, protoc-gen-go-grpc, protoc-gen-connect-go → gen/go
├── README.md                                 # file list + CI commands
├── memory/v1/{common,errors,operation,memory,document,namespace,export,page}.proto
├── memory/admin/v1/admin.proto
├── engram/internal/workflow/v1/workflow.proto
└── engram/internal/events/v1/events.proto
```

`buf lint` (STANDARD, no rule disabled) and `buf build` pass with zero findings; `buf format -d
--exit-code` is clean. The only imports are the well-known types buf ships built in, so the
workspace builds offline with no BSR dependency (a deliberate constraint: CI must not depend on
buf.build being reachable to compile the contract).

### 4.1 Conventions

#### 4.1.1 Transport, metadata and authentication

One port serves gRPC (HTTP/2), Connect and gRPC-Web (connect-go multiplexes all three on the same
handler). Envoy in front balances **per request** (HTTP/2 stream), never per connection.

| Metadata key (gRPC) / header (Connect) | Required | Semantics |
|---|---|---|
| `authorization: Bearer <jwt>` | yes | EdDSA/RS256 JWT with claims `tenant_id`, `ns` (list of namespace ids or `["*"]`), `scopes ⊆ {memory.read, memory.write, memory.admin, tenant.admin}` (D13). Verified by the single `authz.Interceptor` (unary + stream). Missing/invalid → `UNAUTHENTICATED`; expired → `UNAUTHENTICATED` with message `token expired`. |
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
| Same tenant, namespace not in the `ns` allowlist | `PERMISSION_DENIED` | Within a tenant, existence is not secret; the caller should ask for a broader token. |
| Scope missing for the method (table in 4.2) | `PERMISSION_DENIED` | Message names the missing scope. |
| Admin API called with a tenant token | `PERMISSION_DENIED` | Admin services are additionally behind a separate Envoy route not exposed to tenants. |
| Namespace `DELETING`/`DELETED` | `FAILED_PRECONDITION` + `PreconditionFailed{NAMESPACE_DELETING}` | Reads and writes are both rejected (D8). |

#### 4.1.2 Deadlines are mandatory

Every call must carry a deadline. A call without one is rejected with `INVALID_ARGUMENT` and a
`ValidationError{violations:[{field:"grpc-timeout", reason:"MISSING_DEADLINE"}]}` before any work
is done. Rationale: (1) Recall's rerank stage decides on the *remaining* deadline (skip rerank if
< 150 ms, D10) and cannot do that without one; (2) an unbounded call is an unbounded shard
connection through pgbouncer, and the per-instance pool is only 32 (D3); (3) Envoy route timeouts
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
| everything else | 30 s | 100 ms | |

Exceeding the cap is `INVALID_ARGUMENT` (`reason:"DEADLINE_TOO_LONG"`), not a silent clamp:
silent clamping produces `DEADLINE_EXCEEDED` errors that nobody can explain from the client side.

#### 4.1.3 Idempotency: `request_id` versus `operation_id`

Two keys exist because two different things need retry-safety: a *call* and a *unit of work*.

| | `RequestMeta.request_id` | `operation_id` (Retain, Delete*, CreateSnapshot, RefreshPage) |
|---|---|---|
| Scope | (tenant, namespace, method) for **24 h**, stored in the per-shard `idempotency_keys` table (D1); catalog-level writes (`CreateNamespace`, admin) keep theirs in the catalog DB. | Permanent; it *is* the Temporal workflow id suffix `ns/{ns}/op/{operation_id}` (D11). |
| On replay with identical request hash | The stored response bytes are returned (no re-execution). | The existing `Operation` is returned in whatever state it is. |
| On reuse with a different hash | `ALREADY_EXISTS` + `OperationConflict{reason: IDEMPOTENCY_KEY_REUSED}`. | Same. |
| Format | Opaque, 1–128 bytes. | UUIDv7 (validated). |
| Optional? | Optional on writes (a write without it is simply not replay-safe; documented, not rejected). Ignored on reads except for tracing. | Optional; the server mints one. |
| Multi-document `Retain` | One `request_id` covers the whole call. | With exactly one `document_id` in the request the supplied id is used verbatim; with several, per-document ids are `UUIDv5(operation_id, document_id)` so a retry regenerates the same ids. |

The request hash is SHA-256 over the canonical proto serialisation of the request with `meta`
cleared. Rejected alternative: a single key. It conflates "did this HTTP call already happen"
(short-lived, per shard) with "does this unit of work exist" (permanent, cross-move — operations
are restarted on the target shard with the same id, D5 step 6).

Temporal enforces the second half: workflows start with `WorkflowIdReusePolicy =
REJECT_DUPLICATE` while running and `ALLOW_DUPLICATE_FAILED_ONLY` afterwards — a failed operation
may be resubmitted under the same id (the API checks the request hash first), a succeeded one may
not.

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
- Repeated fields in `update_mask` are replaced wholesale (`directives`), never merged; a client
  that wants to append reads, edits and writes with the etag.

#### 4.1.6 Error model

Every error is a `google.rpc.Status` (`code`, `message`, `details[]`) — the standard gRPC rich
error model that grpc-go, connect-go and grpcurl all understand. The typed details are Engram
messages defined in `memory/v1/errors.proto` and attached at runtime with `status.WithDetails`;
on the wire they are `google.protobuf.Any` entries with type URL
`type.googleapis.com/memory.v1.<Name>`. `google/rpc/error_details.proto` is **not** imported
(the workspace must build without the BSR, and Engram needs details google.rpc lacks); the two
standard ones that generic clients act on — `google.rpc.RetryInfo` and `google.rpc.ErrorInfo`
— are attached from the `errdetails` Go package, which needs no proto import. A response carries
at most one Engram detail plus, when a retry hint exists, `RetryInfo`.

| Typed detail | gRPC code | Raised when | Client action |
|---|---|---|---|
| `ValidationError{violations[]}` | `INVALID_ARGUMENT` | Malformed request: missing/too-long field, bad enum value, inconsistent `TagFilter`, unknown mask path, **missing or over-cap deadline**, bad page token. | Fix the request. Never retry unchanged. |
| `NotFound{kind, id, namespace}` | `NOT_FOUND` | Unknown memory/document/operation/page/snapshot id; namespace unknown **or belonging to another tenant**; page version absent at `as_of`. | Do not retry; the id is wrong or the resource is gone. |
| `QuotaExceeded{quota, limit, current, retry_after, scope}` (+ `RetryInfo`) | `RESOURCE_EXHAUSTED` | Admission-time rate quotas `recalls_per_min`, `retains_per_min`, `max_request_bytes`, `max_namespaces` (D13). *Not* raised for `llm_tokens_per_day`/`max_facts`: those defer the operation (`DEFERRED`) instead. | Sleep `retry_after`, retry identical request. |
| `WrongShardOrEpoch{namespace_id, expected_epoch, actual_epoch, namespace_state}` | `FAILED_PRECONDITION` | The shard's `namespace_ownership` row disagrees with the resolved (shard, epoch): stale catalog cache, move cut over between resolve and execute, restore bumped the epoch (D2 row 3). The API invalidates its catalog entry and retries the whole call **up to 3 times** before surfacing it. | Retry with backoff (a fresh resolve happens server-side). Never persist epochs. |
| `NamespaceFrozen{namespace_id, retry_after, reason}` (+ `RetryInfo`) | `FAILED_PRECONDITION` | Writes during the freeze window of a move or a restore (D5 step 4). The API already retried with backoff for up to 30 s. | Retry after `retry_after`. Reads are unaffected. |
| `OperationConflict{operation_id, existing_operation_id, reason}` | `ABORTED` | A concurrent operation owns the state: purge running on the document being retained, namespace cutover in progress, page already refreshing, snapshot already running. | `WaitOperation(existing_operation_id)` then resubmit. |
| `OperationConflict{reason: IDEMPOTENCY_KEY_REUSED}` | `ALREADY_EXISTS` | `request_id`/`operation_id` reused with a different request hash; namespace/page `name` already taken. | Use a fresh id; the stored one is bound to a different request. |
| `PreconditionFailed{violations[]}` | `FAILED_PRECONDITION` | Cancel on a terminal operation, etag mismatch, `Invalidate` on a non-fact or already-invalidated id, namespace `DELETING`, snapshot base version pruned. | Read the current state, decide, resubmit. Do not blind-retry. |
| — | `UNAUTHENTICATED` | Missing/invalid/expired JWT. | Refresh the token. |
| — | `PERMISSION_DENIED` | Scope or allowlist (4.1.1). | Obtain a broader token. |
| — (+ `RetryInfo{2 s}`) | `UNAVAILABLE` | Catalog miss while the catalog is down (D4), shard `DISABLED`, pgbouncer pool exhausted. | Retry with jittered backoff; idempotent by construction. |
| — | `DEADLINE_EXCEEDED` | The deadline elapsed. For `Recall` the stream may already have delivered results; the trailing `RecallStats` is then missing. | Retry with a larger deadline or lower budget. |
| — | `UNIMPLEMENTED` | Phase-gated service (`PageService`, `ExportService` before phase 3). | None. |
| — | `INTERNAL` | Bug or invariant violation (e.g. embedding dims mismatch). Logged with trace id. | Report `x-engram-trace-id`. |

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
| `MemoryService.Recall` | **server-streaming** `RecallResponse{result \| stats}` | Results are emitted in rank order, flushed in groups of 10 after the last stage that fits the deadline, then one trailing `RecallStats` with per-stage timings (D10). A HIGH-budget response is up to 16k tokens of text; streaming lets the client render as it arrives and gives a natural slot for the stats trailer. | Unary with `repeated` results (no trailer for timings without a second message type; 4 MiB message concerns at HIGH budget). |
| `MemoryService.Reflect` | **server-streaming** `ReflectResponse{delta \| tool_call \| tool_result \| citation \| answer \| stats}` | Up to 300 s of wall time with visible progress (tool steps) and token deltas; the client must be able to show something before the end. | Bidirectional (no mid-session client input exists; the loop is bounded and server-driven). Unary (300 s of silence). |
| `OperationService.WaitOperation` | **unary long-poll** (server wait ≤ min(timeout, deadline − 500 ms, 60 s)) | It is a barrier, not a feed: one round trip, works from curl/Connect/JSON, no long-lived stream to migrate when an API replica restarts, and Envoy balances each poll. | `WatchOperation` server stream (nice-to-have for progress bars; deferred until someone needs it — `GetOperation` polling every 2 s is adequate). |
| `ExportService.StreamSnapshot` | **server-streaming** 1 MiB parts | Files reach gigabytes; gRPC messages should stay ≤ 4 MiB; resumable by `(path, offset)` so a broken stream costs one part. | Unary presigned-URL redirect (leaks blob layout and credentials model to clients; not all deployments have public blob endpoints). |
| `DocumentService.DeleteDocument`, `NamespaceService.DeleteNamespace` | **unary** → `Operation` | The synchronous cascade commits before return; the purge is async and tracked (D8). | — |
| `PageService.DeletePage` | **unary** (no `Operation`) | Pages are small; the delete is fully synchronous. | — |
| All `Get*`/`List*`/`BatchGet*` | **unary**, `idempotency_level = NO_SIDE_EFFECTS` | Enables HTTP GET in Connect (cacheable, bookmarkable). | — |

#### 4.1.8 Limits (validated before any work)

| Field | Limit |
|---|---|
| `RetainRequest.items` | ≤ 100 items, ≤ 8 MiB total content; `content` ≤ 1 MiB; `context` ≤ 2 KiB; `metadata` ≤ 16 KiB serialised; `tags` ≤ 64 × 64 B; `document_id` ≤ 256 B |
| `RecallRequest.query` | 1–8 KiB; `max_tokens` 256–65 536; `max_results` ≤ 500; `TagFilter.tags` ≤ 32 |
| `ReflectRequest.query` | ≤ 16 KiB; `context` ≤ 32 KiB; `max_iterations` ≤ 10 |
| `BatchGetMemoriesRequest.memory_ids` | ≤ 100 |
| `Namespace.mission` | ≤ 4 KiB; `directives` ≤ 32 × 1 KiB; `display_name` ≤ 256 B |
| `PageContent.markdown` | ≤ 256 KiB |
| Any single gRPC message | ≤ 16 MiB (server `MaxRecvMsgSize`), which the item limits keep unreachable |

#### 4.1.9 Scopes per method

| Scope | Methods |
|---|---|
| `memory.read` | Recall, Reflect, GetMemory, ListMemories, BatchGetMemories, Get/ListDocuments, GetDocumentVersion, Get/ListNamespaces, all OperationService, GetSnapshotManifest, ListSnapshots, StreamSnapshot, GetPage, ListPages |
| `memory.write` | Retain, Invalidate, Restore, DeleteDocument, CreateSnapshot, Create/Update/Delete/RefreshPage, CancelOperation |
| `memory.admin` | Create/Update/DeleteNamespace (within the token's tenant) |
| `tenant.admin` | Everything in `memory.admin.v1` |

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
0"; `TagMatchMode` follows the register's Hindsight-compatible five modes with `UNSPECIFIED` = no
filter; `UpdateMode` lives here (not in `memory.proto`) because `DocumentVersion`, workflow
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
syntax = "proto3";

package memory.v1;

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

option go_package = "example.com/engram/gen/go/memory/v1;memoryv1";

// RequestMeta carries per-call metadata that must travel in the body (not in
// gRPC metadata) so it is part of the idempotency hash and of the audit log.
message RequestMeta {
  // Client-supplied idempotency key for unary writes (Retain, Delete*,
  // Invalidate, Restore, Create*/Update*). Opaque, 1-128 bytes, unique per
  // (tenant, namespace, method) for 24 h. A replay with the same key and an
  // identical request hash returns the stored response; the same key with a
  // different hash fails with ALREADY_EXISTS + OperationConflict
  // {reason = IDEMPOTENCY_KEY_REUSED}. Optional on reads (tracing only).
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
// rerank depth (50/150/300) and the default token budget (4k/8k/16k)
// as fixed by decision D10.
enum Budget {
  // Not set: the namespace's `recall.default_budget` (system default MID).
  BUDGET_UNSPECIFIED = 0;
  // Cheapest: 50 candidates per arm, 100 graph nodes, rerank top 50, 4k tokens.
  BUDGET_LOW = 1;
  // Default: 150 candidates per arm, 300 graph nodes, rerank top 150, 8k tokens.
  BUDGET_MID = 2;
  // Deepest: 400 candidates per arm, 1000 graph nodes, rerank top 300, 16k tokens.
  BUDGET_HIGH = 3;
}

// UpdateMode says how a retain relates to the previous version of the same
// document_id.
enum UpdateMode {
  // Not set: REPLACE.
  UPDATE_MODE_UNSPECIFIED = 0;
  // The items are the whole new content of the document. Chunks whose content
  // hash is unchanged are kept (no LLM, no embedding); chunks missing from
  // the new set are retired together with their facts.
  UPDATE_MODE_REPLACE = 1;
  // The items are appended to the document; nothing is retired. Chunk hashes
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
// against `tags`. Applied inside every recall arm (before ranking), never
// after fusion, so budgets are not spent on filtered-out rows. Tags are
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
  // Cosine similarity of the query embedding and the item embedding, in
  // [-1, 1] (L2-normalised nomic vectors; practically [0, 1]).
  optional float semantic = 1;
  // BM25 score from pg_search (unbounded, query-dependent).
  optional float lexical = 2;
  // Graph-expansion score: sum of link weights along the best path from a
  // seed, decayed per hop; in (0, 1].
  optional float graph = 3;
  // Temporal-arm score: 1 / (1 + |distance to query_timestamp| in days) for
  // items whose occurrence window overlaps the query window; in (0, 1].
  optional float temporal = 4;
  // Chunk-arm score (RRF of the chunk BM25 and chunk HNSW sub-arms).
  optional float chunk = 5;
  // Reciprocal-rank-fusion score, sum over arms of 1 / (60 + rank_arm).
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

// EntityRef is a resolved entity attached to a memory.
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
// It is stored verbatim (≤ 16 KiB serialised), returned on Memory and
// Document, indexed for equality filtering on top-level string values only,
// and never interpreted by the pipeline.
message MetadataFilter {
  // Top-level key whose value must equal `value` (string comparison).
  string key = 1;
  google.protobuf.Value value = 2;
}
```

#### `memory/v1/errors.proto`

The seven typed details of 4.1.6 plus the enums they need (`ResourceKind`, `NamespaceState`,
`QuotaScope`, `FreezeReason`, `OperationConflictReason`). `NamespaceState` is defined here, not in
`namespace.proto`, because `WrongShardOrEpoch` needs it and `errors.proto` must not import service
files. Each message's comment names its gRPC code and the client action. Full file under
`plans/engram/proto/memory/v1/errors.proto`.

#### `memory/v1/operation.proto`

`OperationService` (GetOperation, ListOperations, CancelOperation, WaitOperation) and the
`Operation` resource: `id`, `kind` (RETAIN_DOCUMENT, DELETE_DOCUMENT, DELETE_NAMESPACE,
CONSOLIDATE, REFRESH_PAGE, CREATE_SNAPSHOT, MOVE_NAMESPACE), `state` (PENDING, RUNNING, DEFERRED,
SUCCEEDED, FAILED, CANCELLED), `progress{units_total, units_done, units_failed, units_skipped,
phase}` (units = chunks for retain), `error` (Status-like), `deferred{quota, resume_at, scope}`,
`result` (kind-specific flat message: `document_version`, `facts_written`, `chunks_reused`,
`rows_purged`, `snapshot_version`, …), `timestamps`, `finished_at`, `request_id` and the
`consolidation_lag` hint promised by D16. Cancellation is cooperative (next activity boundary);
already-committed chunks stay visible. `WaitOperation` returns `{operation, timed_out}` so the two
return paths are unambiguous. `CONSOLIDATE` and `MOVE_NAMESPACE` are listed (never client-created)
so that `ListOperations` explains why `consolidation_lag` is what it is and why writes were
briefly retried. Full file under `plans/engram/proto/memory/v1/operation.proto`.

#### `memory/v1/memory.proto` (full text)

`MemoryService`. Key semantics beyond the comments in the file:

- **Retain grouping.** Items are grouped by `document_id` in order of first appearance; each group
  is one document version and one `Operation`; `RetainResponse.operations` is aligned with
  `document_ids` (minted ids for items that had none). All items of a group must agree on
  `update_mode`. The request is rejected as a whole on any validation error — partial acceptance
  would make `request_id` replay ambiguous.
- **Recall stream contract.** Zero or more `result` messages in strictly increasing `rank`, then
  exactly one `stats`. If the deadline expires mid-stream the client gets `DEADLINE_EXCEEDED`
  after the results already sent and no `stats`; results are never re-ordered by a later message.
  Rerank skipped (deadline or `rerank=false`) is signalled by `Scores.stage = FUSED` on every
  result and `RecallStats.rerank_skip_reason`.
- **`optional bool rerank`** and **`optional bool stream_tokens`** use explicit presence so that
  "not set" can mean "default true" without inverting the flag's name (`disable_rerank`) — the
  proto3 idiom for tri-state booleans.
- **Memory is one message for three kinds.** A `oneof` per kind was rejected: Recall results are
  consumed uniformly (text, scores, provenance), and the kind-specific parts (`observation`,
  `chunk`) are sub-messages that are simply unset for the other kinds. Facts' 5W slots are flat
  strings because they are display/explain data, not structured queries.
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
//     10 after the last pipeline stage that fits the deadline, followed by one
//     trailing RecallStats message (decision D10).
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
// `namespace.namespace_id`, and the scope noted per method.
service MemoryService {
  // Retain submits one or more items for asynchronous ingestion. Scope
  // memory.write. Items are grouped by document_id; each distinct document_id
  // becomes one document version and one Operation (kind RETAIN_DOCUMENT).
  // The call returns as soon as the ingest-ledger rows and the operation rows
  // are committed; facts become visible chunk by chunk afterwards. Not
  // read-your-writes: use OperationService.WaitOperation as the read barrier.
  rpc Retain(RetainRequest) returns (RetainResponse);
  // Recall runs the no-LLM retrieval pipeline (semantic, lexical, graph,
  // temporal and chunk arms in parallel → RRF k=60 → cross-encoder rerank →
  // bounded boosts → token-budget packing) and streams the packed results in
  // rank order, then one RecallStats. Scope memory.read. Deadline ≤ 10 s.
  rpc Recall(RecallRequest) returns (stream RecallResponse);
  // Reflect answers a question with a bounded agentic loop over observations,
  // facts and pages (forced searches first, then ≤ 10 free iterations,
  // ≤ 100k context tokens, ≤ 300 s wall). Scope memory.read. Streams token
  // deltas, tool steps, verified citations, the final answer and stats.
  // Deadline ≤ 330 s.
  rpc Reflect(ReflectRequest) returns (stream ReflectResponse);
  // GetMemory fetches one fact, observation or chunk by id. Scope memory.read.
  rpc GetMemory(GetMemoryRequest) returns (GetMemoryResponse) {
    option idempotency_level = NO_SIDE_EFFECTS;
  }
  // ListMemories pages through memories with structural filters (no
  // relevance ranking). Scope memory.read.
  rpc ListMemories(ListMemoriesRequest) returns (ListMemoriesResponse) {
    option idempotency_level = NO_SIDE_EFFECTS;
  }
  // BatchGetMemories fetches up to 100 memories by id in one round trip;
  // missing ids are reported, not errors. Scope memory.read.
  rpc BatchGetMemories(BatchGetMemoriesRequest) returns (BatchGetMemoriesResponse) {
    option idempotency_level = NO_SIDE_EFFECTS;
  }
  // Invalidate soft-hides one FACT from Recall/Reflect/Export (sets
  // `invalidated_at`). Observations keep the source but are marked stale
  // (decision D8). Scope memory.write. Synchronous.
  rpc Invalidate(InvalidateRequest) returns (InvalidateResponse);
  // Restore clears `invalidated_at` on a fact. Scope memory.write. Synchronous.
  rpc Restore(RestoreRequest) returns (RestoreResponse);
}

// RetainItem is one unit of raw input. Items sharing a document_id form one
// document version, in list order.
message RetainItem {
  // Raw text or markdown, 1 byte to 1 MiB (UTF-8). Stored verbatim in the
  // append-only ingest ledger and in blob storage.
  string content = 1;
  // When the content was produced (e.g. the message time). Required. Becomes
  // the default `mentioned_at` of every fact and chunk extracted from it.
  google.protobuf.Timestamp timestamp = 2;
  // Free-text context given to the extractor ("chat with Alice about the
  // Q3 plan"), ≤ 2 KiB. Not searchable; stored on the document version.
  string context = 3;
  // Client-chosen upsert key, ≤ 256 bytes, unique per namespace. Empty: the
  // server mints a UUIDv7 and the item becomes a standalone document (its id
  // is returned in Operation.target_id).
  string document_id = 4;
  // Tags applied to every fact and chunk of the item; ≤ 64 tags, each
  // 1-64 bytes, exact-match strings. Filters, never security.
  repeated string tags = 5;
  // Free-form JSON object stored verbatim (≤ 16 KiB), returned on memories,
  // filterable by top-level string equality.
  google.protobuf.Struct metadata = 6;
  // Optional entity hints for resolution.
  repeated EntityHint entity_hints = 7;
  // How this document version relates to the previous one. All items of a
  // document_id in one request must agree, otherwise INVALID_ARGUMENT.
  UpdateMode update_mode = 8;
  // Overrides `timestamp` as the `mentioned_at` of extracted facts, for
  // sources that quote older material (a forwarded e-mail). Must be ≤ now.
  google.protobuf.Timestamp mentioned_at = 9;
  // MIME-ish content type: "text/plain" (default) or "text/markdown" (enables
  // heading-anchored chunking).
  string content_type = 10;
}

// RetainRequest submits items. Limits: ≤ 100 items, ≤ 8 MiB total content.
message RetainRequest {
  NamespaceRef namespace = 1;
  // request_id is the idempotency key of the whole call (24 h).
  RequestMeta meta = 2;
  repeated RetainItem items = 3;
  // Optional client-chosen UUIDv7. With exactly one document in the request
  // it is the Operation id verbatim; with several documents the per-document
  // ids are derived as UUIDv5(operation_id, document_id). Resubmitting the
  // same operation_id with the same request hash returns the existing
  // operations; with a different hash fails with ALREADY_EXISTS.
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
  // aligned with `operations`.
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
  // skipped (never truncated) and counted in RecallStats.skipped_count.
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
  google.protobuf.Timestamp as_of = 9;
  // Include raw chunks (fifth arm) in the results.
  bool include_chunks = 10;
  // Include consolidated observations in the results.
  bool include_observations = 11;
  // Hard cap on returned items after packing, 1-500; 0 = budget default
  // (20/50/100). Packing may return fewer.
  int32 max_results = 12;
  // Run the cross-encoder rerank. Unset = true. When false, or when the
  // remaining deadline is < 150 ms at rerank time, results carry
  // Scores.stage = FUSED.
  optional bool rerank = 13;
  // Populate Scores.arm_ranks and per-stage candidate counts in RecallStats.
  bool explain = 14;
  // Explicit occurrence window for the temporal arm. Unset = derived from
  // the query text by rule-based date extraction (may be empty, in which
  // case the temporal arm is skipped).
  TemporalWindow temporal_window = 15;
  // Restrict to items whose metadata matches all of these (AND).
  repeated MetadataFilter metadata_filters = 16;

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
  // Ids of the supporting facts (retired/invalidated ones are excluded from
  // Recall views but kept in GetMemory with `include_hidden_sources`).
  repeated string source_fact_ids = 2;
  // Version returned (observations are versioned; see as_of).
  int64 version = 3;
  // max(mentioned_at) over the source facts cited by this version; the
  // as_of visibility key.
  google.protobuf.Timestamp effective_at = 4;
  // True when a source was deleted/invalidated since this version was
  // written and reconsolidation has not yet run.
  bool stale = 5;
  // Verbatim quotes from source facts, aligned with source_fact_ids where
  // available.
  repeated string quotes = 6;
}

// ChunkInfo is present on MEMORY_KIND_CHUNK memories.
message ChunkInfo {
  // Contextual header prepended for embedding/extraction:
  // "[doc summary ≤ 200 chars] > [heading path]". Not part of `text`.
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
  // Fact text / observation text / chunk text (≤ 8 KiB for chunks).
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
  // When the source said it; the as_of visibility key for facts and chunks.
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
  // Set when the fact is soft-invalidated (hidden from Recall/Reflect/Export
  // but readable by GetMemory).
  google.protobuf.Timestamp invalidated_at = 19;
  Timestamps timestamps = 20;
  NamespaceRef namespace = 21;

  reserved 100 to 199;
}

// GetMemoryRequest fetches one memory by id.
message GetMemoryRequest {
  NamespaceRef namespace = 1;
  string memory_id = 2;
  // Projection over Memory paths, e.g. ["text", "observation.source_fact_ids"].
  google.protobuf.FieldMask read_mask = 3;
  // Observations: also list source facts that are invalidated (never
  // retired ones; those are gone).
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
  // Include soft-invalidated facts (default excluded).
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
  // {MEMORY_NOT_A_FACT}. Already-invalidated facts are a no-op success.
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
  // Same semantics as RecallRequest.as_of, applied to every tool call of the
  // session (forced and free), so the answer is leak-free at T.
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
  // "search_memories", "search_observations", "get_page", "expand_fact".
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
id prefix, `updated_after`, metadata equality), `DeleteDocument`, `GetDocumentVersion`.
`DeleteDocument` is unary and returns an `Operation` because the delete has two halves (D8):
everything Recall/Reflect/GetMemory/Export can see is removed **synchronously in one shard
transaction** — facts `retired_at`, `fact_links` and `entity_mentions` deleted,
`observation_sources` removed, observations with zero sources retired, observations that lost a
source marked `stale`, `page_sources` removed and pages marked `stale_delete` — and the call
returns only after that commits; blob deletion and physical row purge run asynchronously in the
`PurgeDocument` the returned `Operation` (kind `DELETE_DOCUMENT`) tracks. The response also
reports `facts_retired`, `observations_marked_stale` and `pages_marked_stale` so a client can see
the cascade it caused. `expected_version` gives compare-and-delete. Deleting a document whose
purge is already running returns the existing operation (idempotent), deleting an unknown one is
`NOT_FOUND`. Full file under `plans/engram/proto/memory/v1/document.proto`.

#### `memory/v1/namespace.proto`

`NamespaceService`: `CreateNamespace` (server assigns UUIDv7 id and shard; tenant-unique
immutable `name`), `GetNamespace` (optional `stats`), `ListNamespaces` (restricted to the token's
allowlist unless `["*"]`), `UpdateNamespace` (field mask over `display_name`, `mission`,
`directives`, `disposition`, `config_overrides`; etag), `DeleteNamespace` → `Operation`
(kind `DELETE_NAMESPACE`; typed `confirm_name`). `Disposition{skepticism, literalism, empathy}`
∈ 1..5, 0 = inherit.

**Shard and epoch are both hidden** from the public `Namespace`. Clients address
`(tenant_id, namespace_id)` only (D1); a shard id in the resource would invite clients to cache it
and reason about placement, and the epoch changes on every move and restore without any
client-visible meaning — exposing it would only generate support questions. Operators see both
through `memory.admin.v1.ShardService.ResolveNamespace`. What *is* exposed is `state`
(`ACTIVE`, `MOVING`, `FROZEN`, `DELETING`, …), because a client whose writes were retried for 30 s
deserves to learn why. Rejected: exposing `epoch` as an opaque "generation" — it is not needed for
any client operation (etags cover optimistic concurrency). Full file under
`plans/engram/proto/memory/v1/namespace.proto`.

#### `memory/v1/export.proto` (phase 3)

`ExportService`: `CreateSnapshot` → `Operation` (kind `CREATE_SNAPSHOT`, one at a time per
namespace), `GetSnapshotManifest` (`version` 0 = latest), `ListSnapshots`, `StreamSnapshot`
(server-streaming parts ≤ 1 MiB, resumable by `(path, offset)`, last part carries the file
SHA-256). Snapshot v*n* always holds a full file set (`facts.jsonl.zst`, `observations.jsonl.zst`,
optional `chunks.jsonl.zst`, `pages/*.md`, `manifest.json`) and, when v*n−1* exists and its outbox
range is still retained (7 days, D6), `delta-v{n-1}-v{n}.jsonl.zst` derived from the outbox; the
manifest's `delta_available` and `base_version` tell a client at v*n−1* to fetch the delta only,
and a client further behind to chain deltas or take the full files. Full file under
`plans/engram/proto/memory/v1/export.proto`.

#### `memory/v1/page.proto` (phase 3)

`PageService`: `CreatePage` (starts the first refresh unless `defer_refresh`), `GetPage` (by id or
`name`; `include_content`; `version` or `as_of` selects the content version by the D9 rule),
`ListPages` (`stale_only`), `UpdatePage` (mask; changing `source_query`/`tag_filter` sets
`stale_write`), `DeletePage` (synchronous), `RefreshPage` → `Operation` (kind `REFRESH_PAGE`;
`force` = full rewrite). Staleness is two booleans on purpose: `stale_write` (new matching
evidence arrived — the page is incomplete) and `stale_delete` (cited evidence was deleted or
invalidated — the page may say something it must not), plus `stale_since`. `RefreshPolicy` is
`AFTER_CONSOLIDATION` (debounced), `SCHEDULED` (interval) or `MANUAL`. Before phase 3 the service
is registered and answers `UNIMPLEMENTED`, so adapters and clients can be generated now. Full file
under `plans/engram/proto/memory/v1/page.proto`.

#### `memory/admin/v1/admin.proto`

Scope `tenant.admin`, separate Envoy route. `TenantService` (Create/Get/List/Update/Delete;
`Quotas{recalls_per_min, retains_per_min, llm_tokens_per_day, max_facts, max_namespaces,
max_request_bytes}`, `Isolation{SHARED, DEDICATED}`, config Struct); `ShardService`
(RegisterShard, GetShard, ListShards, DrainShard, UpdateShard, **ResolveNamespace** — the ops view
of shard/epoch/state that `memory.v1` hides); `MoveService` (StartMove with optional
`pause_before_freeze`/`resume`, GetMove, ListMoves, RollbackMove — allowed only before `CUTOVER`;
afterwards a rollback is a new move). `Move` exposes both epochs, the D5 state machine and
`MoveProgress{copy_start_seq, applied_seq, replay_lag, restarted_operation_ids, tables_done}`.
`DeleteTenant` returns one `DELETE_NAMESPACE` operation per namespace. Full file under
`plans/engram/proto/memory/admin/v1/admin.proto`.

#### `engram/internal/workflow/v1/workflow.proto`

Temporal payloads, never served. Every top-level input has `schema_version` and a
`WorkflowScope{namespace_id, tenant_id, shard_id, epoch, blob_prefix}` — the D4 rule that workers
never consult the catalog on the hot path; the shard's `namespace_ownership` row verifies the
epoch inside each activity transaction. `ResolvedModels` snapshots model ids and prompt versions
at submission so a workflow stays deterministic when namespace config changes mid-flight.
`RetainDocumentInput` carries ledger row ids, not content (activities read the ledger/blob).
Activity results: `ChunkPlan` (the content-hash delta: new / unchanged / un-retired / retired
hashes), `SummarizeDocumentResult`, `ExtractChunkResult` (`ExtractedFact` with 5W, occurrence
window, `mentioned_at`, entities, causal relations, cache key), `EmbedChunkResult` (vectors as
little-endian float32 bytes in the payload, spilled to a staging blob above 512 KiB),
`ResolveEntitiesResult`, `BuildLinksResult`, `CommitChunkInput/Result` (one transaction; the
idempotency key is `sha256(namespace_id ‖ document_id ‖ version ‖ content_hash)` — the epoch is a
fence, not an identity component, so an operation restarted at epoch *e+1* after a move recognises
chunks committed at *e*), `FinalizeVersion*`, `ConsolidateTrigger`/`ConsolidateInput`/
`ConsolidateBatch*` (`batch_key`, `op_key`, bisect level), `RefreshPageInput/Result`,
`PurgeInput/Result` (targets DOCUMENT, NAMESPACE, RETIRED_SWEEP, MOVED_OUT), `ExportInput/Result`,
`MoveInput`/`MoveCheckpoint`/`MoveResult`. Full file under
`plans/engram/proto/engram/internal/workflow/v1/workflow.proto`.

#### `engram/internal/events/v1/events.proto`

The outbox/Kafka envelope `Event{seq, namespace_id, tenant_id, epoch, occurred_at,
schema_version, event_id, operation_id, shard_id, oneof payload}`. Payloads:
`DocumentVersionStarted`, `ChunkCommitted`, `ChunksRetired`, `DocumentVersionActivated`,
`DocumentDeleted`, `FactInvalidated`, `FactRestored`, `ObservationUpserted`,
`ObservationRetired`, `ObservationsMarkedStale`, `EntityUpserted`, `EntitiesMerged`,
`PageVersionCreated`, `PageDeleted`, `PagesMarkedStale`, `SnapshotCreated`, `RowsPurged`,
`TokenUsageRecorded`, `NamespacePurged` — one per state change that a move must replay or an index
must learn about. Events are **thin**: ids, hashes and the small scalar changes, never embeddings
or long text; the move replayer and an external index fetch full rows by id from the shard at
the same epoch (rows are immutable except for the flags the events carry). This keeps an outbox
row at a few hundred bytes and makes replay idempotent by `(namespace_id, seq)`; a fetch that
finds no row (a later purge already ran) is a no-op, and the later purge event restores
consistency. Rejected: fat events carrying full rows (≈ 3 KB of vectors per fact ⇒ the outbox
would be the largest table on the shard). Full file under
`plans/engram/proto/engram/internal/events/v1/events.proto`.

### 4.3 Tag-match modes: truth table

Let Q be the query tag set and I the item's tag set (exact, case-sensitive strings; both
de-duplicated). Filtering is applied **inside every arm** (semantic, lexical, graph expansion,
temporal, chunks, observation arms) as a SQL predicate on the `tags text[]` column, before any
ranking, so budgets are never spent on rows that will be dropped (D10). The unset filter is the
fifth "mode".

| `TagFilter` | Predicate | SQL (`tags text[]`, `$q` = sorted Q) |
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
| Effect | Resolves relative expressions in the query ("last week", "yesterday") and orders the temporal arm by distance to it; feeds the recency boost. | Filters **every arm** to `mentioned_at ≤ as_of` for facts and chunks, and selects, per observation and per page, the latest version with `effective_at ≤ as_of`. |
| Can it change *which* items are eligible? | No — only ranks/scores. | Yes — it is a visibility boundary. |
| Applied where | temporal arm, boosts | inside each arm's SQL (`WHERE mentioned_at <= $as_of` on `facts`/`chunks`; `observation_versions.effective_at <= $as_of` with `DISTINCT ON (observation_id) ORDER BY version DESC`), and in graph expansion when fetching neighbours, so an invisible fact cannot even be a hop. |
| Typical use | "what did I plan for next Tuesday?" asked on 2026-06-01 | leak-free evaluation: answer question *k* of a conversation as if later turns did not exist |

Definitions (D9): a fact's `mentioned_at` is when the source *said* it (default: the item's
`timestamp`; overridable per item); `occurred_start/end` is when it *happened*. Chunks carry
`mentioned_at = item.timestamp`. An observation version's `effective_at = max(mentioned_at)` over
the source facts that version cites; a page version's `effective_at` likewise over its cited
evidence. The guarantee `as_of = T` gives is therefore: *no fact, chunk, observation version or
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
- `as_of` is orthogonal to `invalidated_at`/`retired_at`: those filters always apply, and
  `as_of` never resurrects an invalidated fact even if it was invalidated after T (curation is
  not time-travelled; a curator's decision is meant to stick).
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

**Tooling.** `buf breaking --against '.git#branch=main,subdir=plans/engram/proto'` runs on every
pull request with the `FILE` category — the strictest: besides wire and JSON compatibility it
forbids moving a definition between files and changing `go_package`, because both break generated
Go import paths for every consumer. A failure blocks the merge; there is no `--exclude` override
in CI. Demonstration on a scratch copy (four deliberate edits: `RecallRequest.as_of` changed to
`string`, `Scores.boost` renumbered/renamed/retyped, `BatchGetMemories` renamed, `WaitOperation`
made streaming):

```text
$ buf breaking --against ../proto-baseline
memory/v1/common.proto:231:3:Field "8" with name "recency_boost" on message "Scores" changed option "json_name" from "boost" to "recencyBoost".
memory/v1/common.proto:231:3:Field "8" with name "recency_boost" on message "Scores" changed type from "float" to "double".
memory/v1/common.proto:231:10:Field "8" on message "Scores" changed name from "boost" to "recency_boost".
memory/v1/memory.proto:37:1:Previously present RPC "BatchGetMemories" on service "MemoryService" was deleted.
memory/v1/memory.proto:173:3:Field "9" with name "as_of" on message "RecallRequest" changed cardinality from "optional with explicit presence" to "optional with implicit presence".
memory/v1/memory.proto:173:3:Field "9" with name "as_of" on message "RecallRequest" changed type from "message" to "string".
memory/v1/operation.proto:45:3:RPC "WaitOperation" on service "OperationService" changed from server unary to server streaming.
$ echo $?
100
```

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
itself. Pre-1.0 release candidates were the one exception, and the three removals made then are
the `reserved` examples in the files (`RetainRequest.wait_for_visibility = 5`,
`RecallRequest.min_score = 17`, `TagFilter.include_untagged = 3`).

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
- Every top-level workflow input and the event envelope carries `schema_version` (an integer,
  starts at 1). Adding fields never bumps it (histories replay with defaults). A field whose
  interpretation changes bumps it, and the workflow code branches on `schema_version` for inputs
  and on `workflow.GetVersion(ctx, "change-id", …)` for logic, keeping the old branch until every
  history started under it has finished: retains and purges are hours old at most, the
  per-namespace `Consolidate` workflow is perpetual but `ContinueAsNew`s every ≤ 1000 events, so
  a 30-day drain window covers every branch, after which the old branch is deleted.
- Event consumers must skip an `Event` whose `payload` oneof is unknown to them (it arrives as an
  unknown field), advance their cursor, and increment `events_unknown_payload_total`; they must
  never fail the relay on it. New payload variants are therefore additive and safe; the move
  replayer, which needs every variant, is always built from the same commit as the writer.
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
(D13) — the adapter never inspects claims except to decide which tools to *list*. Write tools are
listed by `tools/list` only when the JWT carries `memory.write` and are enforced by the core on
`tools/call` regardless of the listing. Every tool call sets the gRPC deadline from the table
(clients may lower it with the MCP `timeout` meta field, never raise it).

| MCP tool | Gate | gRPC method | Argument → field mapping | Result shaping | Deadline |
|---|---|---|---|---|---|
| `recall` | read | `MemoryService.Recall` (stream) | `query`, `budget` (`low\|mid\|high`), `as_of`, `query_timestamp` (RFC 3339), `tags` + `tag_match` (`any\|any_strict\|all\|all_strict\|exact`), `fact_types`, `include_chunks`, `include_observations`, `max_results`, `max_tokens`, `explain` | Stream collected; returns `{results:[{rank, id, kind, text, mentioned_at, occurred, tags, provenance, scores}], stats}` as JSON `content` plus a text rendering (one line per result: `[rank] (kind, date) text`) | 10 s |
| `retain` | **write** | `MemoryService.Retain` | `items[]{content, timestamp, context, document_id, tags, metadata, update_mode}`; `request_id` = MCP request id | `{operations:[{id, document_id, state}]}` and the sentence "accepted; facts appear asynchronously — poll `get_operation`" | 30 s |
| `reflect` | read | `MemoryService.Reflect` (stream) | `query`, `budget`, `as_of`, `tags`/`tag_match`, `output_schema`, `max_iterations` | Token deltas become MCP `notifications/progress` (`progressToken` from the call); tool calls become progress messages `searching observations…`; the final `content` is the answer text (and `structured` as JSON content when a schema was given) followed by a `citations` list | 330 s |
| `get_memory` | read | `MemoryService.GetMemory` | `memory_id`, optional `fields[]` → `read_mask` | The `Memory` as JSON | 10 s |
| `get_operation` | read | `OperationService.WaitOperation` (`timeout` ≤ 30 s) | `operation_id`, `wait_seconds` | `{state, progress, error, result}` | 35 s |
| `list_documents` | read | `DocumentService.ListDocuments` | `tags`/`tag_match`, `prefix`, `updated_after`, `page_token`, `page_size` | `{documents:[…], next_page_token}` | 10 s |
| `delete_document` | **write** | `DocumentService.DeleteDocument` | `document_id`, optional `expected_version` | `{operation_id, facts_retired, observations_marked_stale, pages_marked_stale}` and the sentence "no longer returned by recall; storage purge tracked by operation …" | 30 s |
| `get_page` | read | `PageService.GetPage` (`include_content = true`) | `page_id` or `name`, `as_of`, `version` | Markdown as text `content` plus `{stale_write, stale_delete, version, effective_at}` | 10 s |
| `list_pages` | read | `PageService.ListPages` | `stale_only`, `prefix`, `page_token` | `{pages:[{page_id, name, current_version, stale_write, stale_delete}], next_page_token}` | 10 s |

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
(`?encoding=json&message=<url-encoded JSON>`), which makes reads cacheable and bookmarkable.

| Route (POST unless noted) | gRPC method | Kind |
|---|---|---|
| `/memory.v1.MemoryService/Retain` | Retain | unary |
| `/memory.v1.MemoryService/Recall` | Recall | server stream |
| `/memory.v1.MemoryService/Reflect` | Reflect | server stream |
| `/memory.v1.MemoryService/GetMemory` (+GET) | GetMemory | unary |
| `/memory.v1.MemoryService/ListMemories` (+GET) | ListMemories | unary |
| `/memory.v1.MemoryService/BatchGetMemories` (+GET) | BatchGetMemories | unary |
| `/memory.v1.MemoryService/Invalidate`, `/Restore` | Invalidate, Restore | unary |
| `/memory.v1.DocumentService/GetDocument` (+GET), `/ListDocuments` (+GET), `/GetDocumentVersion` (+GET), `/DeleteDocument` | DocumentService | unary |
| `/memory.v1.NamespaceService/CreateNamespace`, `/GetNamespace` (+GET), `/ListNamespaces` (+GET), `/UpdateNamespace`, `/DeleteNamespace` | NamespaceService | unary |
| `/memory.v1.OperationService/GetOperation` (+GET), `/ListOperations` (+GET), `/CancelOperation`, `/WaitOperation` | OperationService | unary (Wait = long-poll) |
| `/memory.v1.ExportService/CreateSnapshot`, `/GetSnapshotManifest` (+GET), `/ListSnapshots` (+GET) | ExportService | unary |
| `/memory.v1.ExportService/StreamSnapshot` | StreamSnapshot | server stream |
| `/memory.v1.PageService/CreatePage`, `/GetPage` (+GET), `/ListPages` (+GET), `/UpdatePage`, `/DeletePage`, `/RefreshPage` | PageService | unary |
| `/memory.admin.v1.TenantService/*`, `/memory.admin.v1.ShardService/*`, `/memory.admin.v1.MoveService/*` | admin | unary; separate Envoy route, not exposed to tenants |

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
files (`-import-path plans/engram/proto -proto memory/v1/memory.proto` — reflection is also
served).

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
  "scores": {"semantic": 0.81, "lexical": 7.42, "rrf": 0.0325, "rerank": 0.93, "boost": 1.06,
             "final": 0.9858, "stage": "RECALL_STAGE_RERANKED",
             "armRanks": [{"arm": "semantic", "rank": 1}, {"arm": "lexical", "rank": 2}]}}}}
{"result": {"rank": 2, "...": "..."}}
{"stats": {"totalCandidates": 37, "returnedCount": 2, "skippedCount": 0, "tokensUsed": 44,
  "maxTokens": 8192, "lastStage": "RECALL_STAGE_RERANKED",
  "stageTimings": [{"stage": "authz", "duration": "0.001s"}, {"stage": "embed", "duration": "0.024s"},
                   {"stage": "arm:semantic", "duration": "0.031s", "candidatesOut": 12},
                   {"stage": "arm:lexical", "duration": "0.018s", "candidatesOut": 9},
                   {"stage": "arm:graph", "duration": "0.040s", "candidatesOut": 21},
                   {"stage": "arm:temporal", "duration": "0.012s", "candidatesOut": 4},
                   {"stage": "arm:chunk", "duration": "0.001s", "skipped": true},
                   {"stage": "fuse", "duration": "0.001s", "candidatesIn": 46, "candidatesOut": 37},
                   {"stage": "rerank", "duration": "0.095s", "candidatesIn": 37, "candidatesOut": 37},
                   {"stage": "boost", "duration": "0.001s"}, {"stage": "pack", "duration": "0.002s"}],
  "asOfApplied": "2026-04-01T00:00:00Z", "queryTimestampApplied": "2026-04-01T00:00:00Z",
  "total": "0.231s"}}
```

Nothing mentioned after 2026-04-01 appears, and `asOfApplied` echoes the cut-off so an evaluator
can assert it.

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
