## 2. Module breakdown: the code API

This section presents the Go API as a **small set of interfaces that compose in one obvious way**
(register N132). Rules every signature follows: Go 1.25, module `example.com/engram` (D1);
`context.Context` first and `error` last; optional parameters in `…Options` structs, never in
variadic option funcs; **one typed id per entity** (`id.FactID`, `id.NamespaceID`, …, never a bare
`string`); times are UTC `time.Time`; **every interface has at most five methods** (a wider
surface is split by capability, not grown); errors are the typed values of `internal/errs` (2.4).
Generated types appear under the aliases `memoryv1`, `adminv1`, `workflowv1`, `eventsv1`. DDL is in
§3, protos in §4, pipelines in §5. Each module names the **pattern** it uses and why, in one line.

### 2.1 How the code fits together

Four ideas carry the design; everything else is a detail of one of them.

| Pattern | Where | Why (one line) |
|---|---|---|
| **Repository + Unit of Work** | `store.Store.InNamespace(ctx, ns, func(tx Tx) error)`, `store.Tx` and its repositories | one fenced transaction per request means the ownership check, RLS scope and outbox append cannot be forgotten |
| **Strategy** | `recall.Arm`, `index.Searcher`, `consolidate.Router`/`Writer`, `gateway` capabilities | each swappable behaviour sits behind a 1–5 method interface chosen at wiring time |
| **Pipeline** | `pipeline.Step[S]` for recall; the ordered activity groups of retain | a request is a fixed sequence of named steps with per-step timing, deadline and skip rules |
| **State machine** | `fsm.Table` for namespace ownership, operations and moves | legal transitions are data (generated from SQL), so code, SQL and TLA+ share one table |
| **Adapter** | `adapters/mcp`, `adapters/connect` over the gRPC core | adapters hold a *client* to the core, so they cannot bypass the interceptor (D13) |
| **Transactional outbox** | `outbox.Writer.Append` as the last statement; `outbox.Relay` | every consumer sees changes without dual writes |
| **Saga** | `move` and `expunge` as Temporal workflows with compensations | multi-database work is a durable sequence of idempotent steps, each with a defined undo before its point of no return |

**A Retain call, through the packages.**

1. Client → Envoy → `adapters/connect` or the gRPC server → `authz.Interceptor` verifies the JWT,
   `catalog.Resolver` returns `{tenant, shard, epoch, state}`, `quota.Limiter` takes a token, and a
   `RequestScope` lands in the context.
2. `api.MemoryServer.Retain` validates (`errs.Validation`), mints `UUIDv5(operation_id, "item/" ‖ index)`
   for items without a `document_id`, computes `api.RequestHash` and calls `api.Submitter.Submit(ctx, scope, req)`.
3. `Submit` writes a large body to `blob.Store` (content-addressed), then runs **one unit of work**:
   `store.InNamespace(ctx, ns, func(tx store.Tx) error { … })`. The store opens a transaction, sets
   the RLS GUCs, takes the shared fence try-lock and checks `namespace_ownership` is `active` at the
   epoch.
4. Inside: `tx.Ops().Idempotency().Begin`, `Ops().Ledger().Append`, `Insert().Documents().BeginVersion`,
   `Ops().Operations().Insert`, then `Ops().Outbox().Append(DocumentVersionStarted)` as the last
   statement. Commit; the ack is now durable.
5. `workflows.Client.StartRetain` starts `ns/{ns}/op/{op}` on task queue `shard-N`; the handler returns
   `Operation{PENDING}`.
6. A worker runs the `RetainDocument` workflow, which is the **pipeline**: `Prepare` activities
   (`LoadItem → Chunk → SummarizeDocument → PlanChunks`), then per chunk the `ChunkPipeline` group
   (`ExtractChunk → EmbedChunk → ResolveEntities → BuildLinks → CommitChunk`), then `Finish`
   (`FinalizeVersion → ReembedChunk → MarkOperation`).
7. Each activity is a method on a struct built in `cmd/engram-worker` from the service packages:
   `chunk.Chunker`, `extract.Extractor` (over `gateway.Structured` and `extract.Cache` on
   `blob.Store`), `entity.Resolver`, `link.Linker`; it re-derives its `router.ShardHandle` from the
   `shard_id` in the input (never the catalog) and opens its own `store.InNamespace`.
8. `CommitChunk` is one unit of work again: inserts only (chunk, facts, vectors, links, mentions,
   curation re-applied), then `outbox.Writer.Append(ChunkCommitted)` last. Facts are visible at
   commit.
9. `outbox.Relay` (one elected per shard) reads `outbox` in `seq` order and drives the `index` and
   `kafka` sinks; `FinalizeVersion` inserts tombstones; a `Nudge` wakes `consolidate`.
10. The client calls `WaitOperation`; `api.OperationWaiter` parks on `workflows.Client.WaitResult`.

**A Recall call, through the packages.**

1. Same edge as above → `api.MemoryServer.Recall` (server stream) → `recall.Planner.Recall(ctx, q, sink)`
   inside `store.ReadNamespace` (no lock; accepts `active` and `frozen/move`).
2. The planner is a `pipeline.Pipeline[*recall.State]`; its steps run in order and each records a
   `StageTiming`:
3. `Embed` — `gateway.Embedder.Embed("search_query: " + text)` (LRU hit skips it).
4. `Visibility` — `tx.Visible().Sets(ctx)` loads `doc_tomb` (document → `up_to_version`), `chunk_tomb`, `fact_hidden` once
   (three indexed selects) and resolves the tag filter into an allowed-document set (N116).
5. `Arms` — runs the configured `[]recall.Arm` (Strategy): lexical and temporal immediately, semantic
   and chunks when the embedding arrives, graph in two seeded waves; each arm calls
   `index.Searcher` or a repository with the visibility sets and `as_of` in its predicate, under its
   own deadline.
6. `Fuse` — `recall.Fuser` (RRF, k = 60, exact rationals). `Rerank` — `gateway.Reranker` on the top
   0/50/150, skipped below the 106 ms reserve. `Boost`, `Pack` (rank 1 always whole).
7. `Stream` — batches of 10 to the gRPC stream, then `RecallStats` with the stage timings.
8. Errors travel as `errs.Error` and are rendered once by `errs.ToStatus` / `errs.ToConnect`; a
   `WrongShardOrEpoch{MOVED_OUT}` is caught by `router.Retry`, which follows the internal hint to the
   new owner.

#### Package layout (D14) and dependency rule

```
cmd/
  engram-api/     engram-worker/     engram-mcp/     engramctl/        composition roots
proto/            memory/v1  memory/admin/v1  engram/internal/{errors,workflow,events}/v1
gen/go/...        buf generate output (committed)
internal/
  id/             typed ids and Scope (leaf; N132)
  errs/           typed errors + gRPC/Connect/Temporal mapping (leaf; N1)
  pipeline/       generic ordered Step[S] runner (leaf; N132)
  fsm/            transition tables for ownership, operations, moves (leaf; N132)
  api/            gRPC service implementations, deadline + request_id enforcement, retain.Submitter
  authz/ catalog/ router/ config/ quota/ telemetry/
  gateway/ blob/ intent/ store/ index/ outbox/ ledger/
  chunk/ extract/ entity/ link/ recall/ consolidate/ reflect/ pages/ export/
  expunge/ move/ workflows/
adapters/         mcp/  connect/
formal/           tla/*.tla  lean/Engram/*.lean
```

`internal/id`, `internal/pipeline` and `internal/fsm` are three small leaf packages added to D14 by
N132 (they contain no I/O and import nothing from `internal/`). `internal/intent` and
`internal/expunge` are the D22 packages.

**Dependency rule** (enforced by `go vet` + a `depguard` config in CI):

| Layer | May import | Must not import |
|---|---|---|
| leaves (`id`, `errs`, `pipeline`, `fsm`) | the standard library, `gen/go` detail messages (`errs` only) | any other `internal/` package |
| `internal/api` | `authz, router, recall, pages, export, quota, store, workflows (client only), intent, errs, telemetry`, `gen/go` | adapters, `cmd` |
| Services (`recall, consolidate, reflect, pages, export, move, expunge, entity, link, extract, chunk`) | `store, index, gateway, blob, intent, config, quota, ledger, errs, telemetry`, `gen/go` | `internal/api`, adapters, `workflows` |
| Infrastructure (`store, index, gateway, blob, intent, catalog, router, outbox, ledger, config, quota, telemetry`) | leaves, `gen/go`, drivers | any service package, `internal/api` |
| `internal/workflows` | activity **interfaces** declared in itself, `gen/go/engram/internal/workflow/v1`, the Temporal SDK | concrete service packages (injected in `cmd/engram-worker`) |
| `adapters/*` | `gen/go` (+ generated clients), `authz` (scope names), `errs` (code mapping) | any other `internal/*` |
| `cmd/*` | everything | — |

Rationale: the rule keeps `internal/api` a thin translation layer, lets every service be tested with
fakes for store/index/gateway, and guarantees the MCP/Connect adapters cannot bypass the interceptor.
Rejected: adapters importing services directly (one more enforcement point to audit, D13).

#### Shared vocabulary

```go
package id // leaf: every entity has its own type; a FactID cannot be passed where a ChunkID is wanted

type TenantID string      // [a-z0-9-]{1,64}
type DocumentID string    // client-chosen, ≤ 256 bytes
type ShardID int32
type Epoch int64

type NamespaceID uuid.UUID  // UUIDv7. FactID, ChunkID, EntityID, ObservationID, PageID, OperationID and MoveID
type FactID uuid.UUID       // are defined the same way; each has String(), Parse<T>(string) and Bytes() (the
type ChunkID uuid.UUID      // 16-byte form is what events carry, N80). Versions are typed too:
type Version int64          // document, observation and page versions; SnapshotVersion for exports.

// Scope is the fencing token that travels with every request and every workflow input.
type Scope struct { Tenant TenantID; Namespace NamespaceID; Shard ShardID; Epoch Epoch }
```

`authz.RequestScope` is `id.Scope` plus the caller's `Scopes`, `Subject`, `RequestID` and the resolved
`config.Resolved`; the store only ever sees an `id.Scope`.

**Pipeline and state machine, the two generic leaves** (no I/O, a few dozen lines each):

```go
package pipeline

// Step is one named stage of a request. Run mutates the shared state S; returning ErrSkip records the stage as
// skipped (deadline, disabled, empty window) and continues; any other error stops the pipeline.
type Step[S any] interface {
	Name() string
	Run(ctx context.Context, s S) error
}
// Run executes the steps in order, opening one telemetry span and one StageTiming per step.
func Run[S any](ctx context.Context, s S, steps ...Step[S]) ([]StageTiming, error)

package fsm

// Table is a transition table keyed by (from state, edge, role). The ownership table is GENERATED from
// ownership_transitions (§3.3.1), the move and operation tables from §5; the TLA+ specs use the same rows.
type Table[S, E ~string] interface {
	Next(from S, edge E, role string) (S, error) // ErrIllegalTransition for an illegal pair; callers render it as errs.PreconditionFailed
	Edges(from S) []E
	States() []S
}
```

`fsm.OwnershipState` is `incoming | ready | active | frozen/{move,delete,restore} | moved_out`;
`fsm.OperationState` is `PENDING | RUNNING | DEFERRED | SUCCEEDED | FAILED | CANCELLED` (N35, monotone);
`fsm.MoveState` is `planned | copying | frozen | reconciling | cutover | cleaning | done | rolled_back`.

### 2.2 Modules

Format per module: purpose, the pattern in one line, the interfaces (≤ 5 methods each), then only the
rules that are not obvious from the signatures. Dependencies follow the table in 2.1; the
swappable implementations of all modules are collected in one table in 2.3.

#### 2.2.1 `internal/api` — gRPC servers and Connect handlers

Implements every generated `memory.v1` / `memory.admin.v1` server interface; the Connect handlers
delegate to the same struct. *Pattern: Adapter between the wire contract and the services; the
cross-cutting rules (deadline, `request_id`, page tokens, field masks, error rendering) live here once.*

```go
package api

type Options struct {
	MaxDeadline map[string]time.Duration // full method → cap (N11); over the cap is INVALID_ARGUMENT, never clamped
	MaxPageSize int32                    // 1000 (200 for ListMemories with text in the mask)
	RequestIDTTL time.Duration           // 24 h (D1)
	MaxOpenWaits int                     // 2 000 WaitOperation long-polls per process (N70)
	ReflectionServices []string          // ["memory.v1"] only, never the admin surface (N71)
}
type Deps struct { // every field is an interface (2.3)
	Router router.ShardRouter; Store store.Store; Recall recall.Planner; Retain Submitter
	Deleter Deleter; Ops OperationWaiter; Temporal workflows.Client; Pages pages.Reader; Export export.Streamer
	Intents intent.Log; Quota quota.Limiter; Clock func() time.Time
}
func NewServer(d Deps, o Options) *Server
func (s *Server) RegisterGRPC(g *grpc.Server)
func (s *Server) RegisterConnect(mux *http.ServeMux, ic ...connect.Interceptor)

// Submitter is the API half of retain (D16): durable ack, then workflow start (§5.1.1).
type Submitter interface { Submit(ctx context.Context, sc authz.RequestScope, req *memoryv1.RetainRequest) (*memoryv1.Operation, error) }

// Deleter is the synchronous half of every delete: the intent object, then ONE marker transaction (N115, N122).
type Deleter interface {
	DeleteDocument(ctx context.Context, sc authz.RequestScope, doc id.DocumentID, o DeleteOptions) (*memoryv1.DeleteDocumentResponse, error)
	DeleteNamespace(ctx context.Context, sc authz.RequestScope, confirmName string) (*memoryv1.DeleteNamespaceResponse, error)
	Invalidate(ctx context.Context, sc authz.RequestScope, fact id.FactID, reason string) (*memoryv1.Memory, error)
	Restore(ctx context.Context, sc authz.RequestScope, fact id.FactID) (*memoryv1.Memory, error)
}

// OperationWaiter implements WaitOperation: park on Temporal's workflow-result long-poll (≤ MaxOpenWaits per
// process), else poll the operations row every 1 s ± 250 ms; DELETE_NAMESPACE and DELETE_TENANT are served from the catalog, derived from `namespaces.state` / the `tenants` row (N70, N133d).
type OperationWaiter interface {
	Wait(ctx context.Context, sc authz.RequestScope, op id.OperationID, timeout time.Duration) (*memoryv1.Operation, bool /*timedOut*/, error)
}

// RequestHash is the one request-hash function: SHA-256 over the NORMALISED protojson of the request with
// `meta` cleared (keys sorted, unknown fields dropped, canonical Timestamp/Duration/int64 rendering; N72).
func RequestHash(m proto.Message) [32]byte
```

Streaming helpers: `StreamBatcher[T]` (batches of 10, flush on deadline pressure), `TokenStreamer`
(Reflect), `PartStreamer` (1 MiB parts). A `request_id` reused with a different hash is
`ALREADY_EXISTS` + `OperationConflict{IDEMPOTENCY_KEY_REUSED}`. *Test seam:* `bufconn` for gRPC and
`httptest` for Connect over the same `Server`, golden error details per method on both transports.

#### 2.2.2 `internal/authz` — the single enforcement point

Verifies the JWT, resolves the namespace, checks tenant ownership, allowlist (`ns`, `ns_group`, N65)
and scope, takes the rate token, and attaches a `RequestScope`. One implementation serves gRPC
unary, gRPC stream and Connect; MCP and Connect forward the JWT. *Pattern: Interceptor chain (a
Pipeline of three checks) with a declarative `Policy` table instead of per-handler code.*

```go
package authz

type Scope string // "memory.read" | "memory.write" | "memory.admin" | "tenant.admin"

type Claims struct { Subject string; Tenant id.TenantID; Namespaces []id.NamespaceID /* or ["*"] */; Groups []string; Scopes []Scope; ExpiresAt time.Time }

type TokenVerifier interface { Verify(ctx context.Context, raw string) (*Claims, error) } // signature, expiry, issuer, audience; no catalog

type RequestScope struct { id.Scope; Scopes []Scope; Subject string; RequestID string; Config *config.Resolved }

type MethodPolicy struct {
	Scope Scope; Target Target /* Namespace | Tenant | Admin */; Bucket quota.Bucket
	AllowDeleting bool // only OperationService.GetOperation/WaitOperation (and TenantService.GetTenantOperation): a `deleting` namespace is otherwise FAILED_PRECONDITION (N70, N127)
}
type Policy map[string]MethodPolicy // key: "/memory.v1.MemoryService/Recall"

type Interceptor struct { /* verifier, resolver, limiter, policy */ }
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor
func (i *Interceptor) Stream() grpc.StreamServerInterceptor // scope fixed for the stream's life (N8)
func (i *Interceptor) Connect() connect.Interceptor
func FromContext(ctx context.Context) (RequestScope, bool)
```

Outcomes: other tenant → `NOT_FOUND` (no existence oracle); same tenant outside the allowlist or
missing scope → `PERMISSION_DENIED`; `deleting` → `FAILED_PRECONDITION` except `AllowDeleting`
(N5). The interceptor also drops every detail of `engram.internal.errors.v1` before a response
leaves the process (N128). *Test seam:* table tests over
`(claims, method, catalog entry) → code`, and the cross-tenant matrix of §8 on gRPC, Connect and MCP.

#### 2.2.3 `internal/catalog` — control plane and resolver cache

Owns the `engram_catalog` tables (D4) and the in-process `Resolver` (LRU 100 k, TTL 60 s, negative
5 s, `LISTEN catalog_changes`; existing namespaces are served indefinitely while the catalog is
unreachable, only misses fail `UNAVAILABLE` — F-21). *Pattern: Repository for the control plane,
split by capability so no interface passes five methods; the Resolver is a read-through cache.*

```go
package catalog

type Entry struct { Namespace id.NamespaceID; Tenant id.TenantID; Name string; Shard id.ShardID; Epoch id.Epoch; State NamespaceState
	EmbeddingModel string; EmbeddingDims int /* fixed at creation, N111 */; Config config.Layer; TenantEntry *TenantEntry }

type Namespaces interface { // the directory
	Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error)
	ResolveByName(ctx context.Context, t id.TenantID, name string) (*Entry, error)
	Create(ctx context.Context, p CreateParams) (*Entry, error)            // also inserts namespace_ownership(active, epoch 1) via the shard bootstrapper
	SetState(ctx context.Context, ns id.NamespaceID, from, to NamespaceState) error // creating | active | moving | frozen | restoring | deleting | deleted
	BumpEpoch(ctx context.Context, ns id.NamespaceID, expected id.Epoch, why EpochReason) (id.Epoch, error)
}
type Moves interface { // the move ledger: every transition is a CAS on the current state
	Plan(ctx context.Context, ns id.NamespaceID, target id.ShardID) (*MoveRow, error)   // source_system_id, source_timeline_id, t_copy recorded
	Advance(ctx context.Context, m id.MoveID, from, to fsm.MoveState) error
	Cutover(ctx context.Context, p CutoverParams) error                                 // step (d) only: shard, epoch, state, NOTIFY
	Rollback(ctx context.Context, m id.MoveID, reason string) error
}
type Registry interface { // shards and tenants (admin surface, §9)
	RegisterShard(ctx context.Context, s Shard) error
	SetShardState(ctx context.Context, s id.ShardID, to ShardState) error
	ListShards(ctx context.Context, cell string) ([]Shard, error)
	GetTenant(ctx context.Context, t id.TenantID) (*TenantEntry, error)
	TenantOperation(ctx context.Context, t id.TenantID, op id.OperationID) (*memoryv1.Operation, error) // DELETE_TENANT lives here, derived from tenants.state / deleted_at (N127, N133d)
}
type Resolver interface {
	Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error)      // never blocks on the catalog with a fresh entry
	ResolveFresh(ctx context.Context, ns id.NamespaceID) (*Entry, error) // bypass the cache (after WrongShardOrEpoch)
	Invalidate(ns id.NamespaceID)
	Run(ctx context.Context) error // LISTEN loop; full flush on reconnect
}
```

The catalog has no delete log: acknowledged deletes live in the intent log (N122). `Entry`
carries `embedding_model`/`embedding_dims` (`namespaces` columns) so no worker reads config for
the vector space. `ResolverOptions` has `MaxEntries` 100 000, `TTL`, `NegativeTTL` and `StaleMax`
(0 = unbounded). *Test seam:* a rapid
state-machine test drives random `Plan/Advance/Cutover/Rollback` sequences against the move table
(no `cutover` without `frozen`; at most one active shard per namespace).

#### 2.2.4 `internal/router` — scope → shard handle

Maps a scope (or a workflow input) to the resources of its shard, built at process start for the
cell's shard list. *Pattern: Registry of per-shard handles; `Retry` is the one place that encodes
the move/freeze retry contract.*

```go
package router

type ShardHandle struct { Shard id.ShardID; Cell string; Store store.Store; Blob blob.Store; Index index.Searcher; TaskQueue string; Metrics telemetry.ShardLabels }

type ShardRouter interface {
	For(ctx context.Context, sc id.Scope) (*ShardHandle, error)            // local handle or errs.ShardNotLocal(cell)
	ForShard(s id.ShardID) (*ShardHandle, bool)                              // workers carry shard_id in inputs (D4)
	Forward(ctx context.Context, cell, method string, req, resp proto.Message) error // cross-cell hop (phase 3), at most one (N17)
	Local() []id.ShardID
}

// Retry runs fn under the retry contract: one re-resolve on WrongShardOrEpoch for writes; on MOVED_OUT it first
// follows the internal MovedOutHint to the new owner (no catalog read, N93); calls that meet MOVED_OUT,
// NamespaceNotReady or NamespaceFrozen re-resolve in a bounded loop (≤ 5 s, jittered 50 → 500 ms; writes on
// NamespaceFrozen up to 30 s). Only handlers that are safe to re-execute call it (all writes are idempotent).
func Retry(ctx context.Context, r catalog.Resolver, sr ShardRouter, sc authz.RequestScope, fn func(ctx context.Context, h *ShardHandle, sc authz.RequestScope) error) error
```

`Forwarder` proxies unary calls with `Invoke` and the three server-streaming methods with a gRPC
`ClientStream`; the external Envoy strips `engram-forward-*` (N17, N71).

#### 2.2.5 `internal/gateway` — AI gateway client

The only HTTP client to the AI gateway. *Pattern: Strategy by capability (interface segregation) —
a consumer depends on the one capability it uses, so an extractor test fakes `Structured` and nothing
else.*

```go
package gateway

type Model string
type Usage struct { Model Model; PromptTokens, CompletionTokens int; CostMicros int64 }

type Structured interface { ChatStructured(ctx context.Context, r StructuredRequest, out any) (*Usage, error) } // JSON decoded into out; PermanentLLMError on a 4xx
type Chatter interface    { Chat(ctx context.Context, r ChatRequest, sink ChatSink) (*Usage, error) }            // streaming tokens + tool calls (Reflect)
type Embedder interface {
	Embed(ctx context.Context, r EmbedRequest) ([]float32, *Usage, error)              // L2-normalised by the client; the caller adds the nomic prefix
	EmbedBatch(ctx context.Context, r EmbedBatchRequest) ([][]float32, *Usage, error)  // ≤ 64 texts
}
type Reranker interface { Rerank(ctx context.Context, r RerankRequest) ([]RerankScore, *Usage, error) }           // ≤ 300 docs per call (D15)
type Batcher interface {
	SubmitBatch(ctx context.Context, r BatchJobRequest) (*BatchJob, error)
	PollBatch(ctx context.Context, job string) (*BatchJobStatus, error)
	BatchResults(ctx context.Context, job string) iter.Seq2[BatchResult, error]
}
```

One `HTTPClient` implements all five. Retries 429/502/503/504 with jittered backoff (100 ms → 5 s, ≤
5 attempts) → `errs.Unavailable`; other 4xx → `errs.PermanentLLM` (non-retryable; an extraction
failure records the chunk and the operation still ends `SUCCEEDED` with `units_failed > 0`, N35).
A per-model `RateLimiter` blocks (bounded by ctx) rather than fails; `UsageHook`s feed `quota.Meter`
and telemetry. **Admission** (N130): no gateway call is made without a prior `quota.Reserve`; the
client itself does not enforce it, the activities do.

#### 2.2.6 `internal/blob` — blob store, prefix scoping, content addressing

*Pattern: Decorator — `Scoped` wraps any `Store` and rejects keys outside a shard prefix.*

```go
package blob

type Store interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, o PutOptions) (*ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, p ListPage) (*ListResult, error)
}
func Scoped(root Store, p Prefix, cred Credential) Store   // rejects any key not under {shard}/{tenant}/{ns}/ (../, absolute, other shard)
func ContentKey(p Prefix, k Kind, sum [32]byte, ext string) string // "{prefix}{kind}/{hex sha256}[.ext]"; Kind: ledger | xcache | ecache | docsum | consolidate | pages | export | staging | ver

// Tombstoner makes purges resumable: a blob_tombstones row is inserted first, the object deleted, then the row.
type Tombstoner interface {
	MarkDeleted(ctx context.Context, tx store.Tx, key, reason string) error
	PurgeMarked(ctx context.Context, tx store.Tx, limit int) (int, error)
}
```

The expunge deletes `xcache`/`ecache`/`staging` objects only when no visible chunk references the
hash **and** the object is older than `xcache_grace = 24 h` (`Head` supplies `LastModified`; N100).
Backups live under `_backups/shard-{id}/`, outside every tenant prefix (N23).

#### 2.2.7 `internal/store` — per-shard Postgres, fencing, repositories

Everything that touches a shard database: pools, the fenced transaction, one repository per table
family. No SQL exists outside this package and `internal/index`. *Pattern: **Repository + Unit of
Work.** `InNamespace` is the unit of work; the repositories reachable from its `Tx` are the only way to
write, so the fence prelude, RLS scope and outbox append cannot be skipped.*

```go
package store

// Store is the only entry point to a shard. Reads take no lock; writes run the fence prelude (N82).
type Store interface {
	// InNamespace runs fn in one WRITE transaction: SET LOCAL engram.namespace_id/tenant_id/epoch and
	// statement_timeout (30 s = idle_in_transaction_session_timeout, A-F1), engram_try_ns_fence (shared
	// try-lock, never waits: a refusal is NamespaceFrozen{200 ms}), then a plain SELECT state, epoch FROM
	// namespace_ownership; abort with WrongShardOrEpoch unless state = 'active' AND epoch = ns.Epoch.
	InNamespace(ctx context.Context, ns id.Scope, fn func(tx Tx) error) error
	// ReadNamespace runs fn in one READ transaction: the ownership SELECT only, accepting 'active' and
	// 'frozen/move', epoch ignored, no lock. 'frozen/delete', 'frozen/restore', 'incoming', 'ready' and
	// 'moved_out' are refused (a moved_out row also returns the internal MovedOutHint).
	ReadNamespace(ctx context.Context, ns id.Scope, fn func(tx ReadTx) error) error
	// Admin runs fn as engram_admin (BYPASSRLS): sweepers that enumerate namespaces, the restore replay.
	Admin(ctx context.Context, fn func(tx AdminTx) error) error
	Shard() id.ShardID
	Close() error
}

// Tx groups the repositories by role; every accessor returns an interface of at most five methods.
type Tx interface {
	Read() ReadTx       // the read side, also usable inside a write transaction
	Insert() Inserts    // insert-only content writers (N113): no method updates a content row
	Markers() Markers   // tombstones, fact_hidden, curation_log, derived_hidden, expunge progress (N115, N119)
	Derived() Derived   // observations, pages, consolidation bookkeeping, derivation lock (N117, N120)
	Ops() Ops           // operations, idempotency keys, ledger, token usage, outbox
}
type ReadTx interface {
	Documents() DocumentReader; Chunks() ChunkReader; Facts() FactReader
	Graph() GraphReader        // links and entities
	Visible() MarkerReader     // the marker sets and curation state behind the visibility predicate
}
type AdminTx interface {
	Ownership() OwnershipRepo  // Get, Transition(edge, …) generated from §3.3.1, FenceExclusive (one 35 s attempt)
	Sweep() Sweepers           // namespaces with pending facts, stale observations, pending tombstones, deferred operations
	Restore() RestoreRepo      // apply an intent through the admin variant of the marker transaction (N122)
	Indexes() IndexRepo        // vector_indexes bookkeeping for engramctl index (N112)
}
```

**Content** (insert-only):

```go
type Inserts interface {
	Documents() DocumentWriter; Chunks() ChunkWriter; Facts() FactWriter; Links() LinkWriter; Entities() EntityWriter
}
type DocumentWriter interface {
	BeginVersion(ctx context.Context, d DocumentDraft) (id.Version, error)   // under the documents row lock (N56): assigns the version and `append_base_version`; refuses a tombstoned document
	Activate(ctx context.Context, doc id.DocumentID, v id.Version) error
	MarkDeleting(ctx context.Context, doc id.DocumentID, at time.Time) (current id.Version, err error) // state = 'deleting'; the marker tx (§5.4.1)
	SetBody(ctx context.Context, doc id.DocumentID, v id.Version, key string, sum [32]byte) error     // WHERE body_key IS NULL (N104)
	Lock(ctx context.Context, doc id.DocumentID, m LockMode) error  // LockShared = try-lock (errs.DocumentBusy, 100 ms); LockExclusive under lock_timeout 5 s (N83)
}
type ChunkWriter interface {
	Insert(ctx context.Context, c ChunkInsert) (id.ChunkID, bool /*inserted*/, error) // ON CONFLICT DO NOTHING; ordinals live in document_version_chunks
	AddVector(ctx context.Context, c id.ChunkID, v VectorRow) error                  // chunk_vectors keyed with embedding_effective_at; ReembedChunk adds a NEW row (N111)
	Member(ctx context.Context, doc id.DocumentID, v id.Version, h [32]byte, c id.ChunkID, ordinal int) error
}
type FactWriter interface {
	Insert(ctx context.Context, fs []Fact, vs []VectorRow) error  // facts + fact_vectors (document_id, chunk_id, mentioned_at copied); unique (chunk, extraction_key, content_hash)
	Stamp(ctx context.Context, batch [32]byte, fs []id.FactID, note StampNote) error // fact_consolidation, insert-only (N95)
}
type LinkWriter interface { Insert(ctx context.Context, ls []Link) error } // ON CONFLICT DO NOTHING in key order; undirected edges once with src < dst
type EntityWriter interface {
	UpsertSorted(ctx context.Context, es []EntityUpsert) ([]id.EntityID, error) // one statement, canonical_norm order (N69)
	AddAliases(ctx context.Context, as []Alias) error                           // with the producing document_id (N118)
	AddMentions(ctx context.Context, ms []Mention) error                        // with the fact's mentioned_at (N118)
}
type DocumentReader interface { Get(ctx context.Context, doc id.DocumentID) (*Document, error); List(ctx context.Context, q DocumentQuery) ([]Document, string, error) }
type ChunkReader interface {
	Plan(ctx context.Context, doc id.DocumentID, hashes [][32]byte) ([]ChunkState, error) // member | live | tombstoned | stale_extraction | absent
	Membership(ctx context.Context, doc id.DocumentID, v id.Version) ([]id.ChunkID, error)
	Get(ctx context.Context, c id.ChunkID) (*Chunk, error)
}
type FactReader interface {
	ByIDs(ctx context.Context, ids []id.FactID, o ReadOptions) ([]Fact, error) // visibility predicate + as_of applied
	NearestByOccurrence(ctx context.Context, anchor time.Time, w *TemporalWindow, n int, f Filter) ([]Fact, error) // temporal arm, two-sided btree probe (N68)
	Pending(ctx context.Context, limit int) ([]Fact, error)  // engram_pending_facts: visible, above the watermark, no 'done' stamp (N95)
}
type GraphReader interface {
	Neighbours(ctx context.Context, seeds []id.FactID, kinds []LinkKind, limit int, f Filter) ([]Link, error) // BOTH endpoints must be visible (N116)
	Similar(ctx context.Context, name, typ string, min float32) ([]EntityCandidate, error)                  // via engram_entity_fuzzy (SECURITY DEFINER, N131)
}
```

**Markers and derived state:**

```go
type Markers interface {
	TombstoneDocument(ctx context.Context, t DocumentTombstone) error                  // document_tombstones 'pending' covering (document, t.UpToVersion = highest version assigned, N133c); intent_key recorded
	TombstoneChunks(ctx context.Context, reason ChunkReason, cs []id.ChunkID) error    // 'replace' | 'reextract'
	Hide(ctx context.Context, f id.FactID, cause HideCause, reason, intentKey string) (changed bool, err error) // fact_hidden: 'invalidate' | 'reextract'
	Unhide(ctx context.Context, f id.FactID) (changed bool, err error)                 // refuses a 'reextract' marker
	Expunge() ExpungeRepo                                                              // expunge_progress, derived_hidden, consumer-cursor check
}
// Every marker method also inserts the shard-local deletion_log row keyed by the intent name (N122), in the same transaction.
type MarkerReader interface {
	Sets(ctx context.Context) (MarkerSets, error)  // doc_tomb and doc_pending ({document → up_to_version}, N133c), chunk_tomb, fact_hidden: three indexed selects (N116)
	Curation(ctx context.Context, doc id.DocumentID, hash [32]byte) (CurationState, error) // last action for a twin, re-applied at CommitChunk
}
type Derived interface {
	Observations() ObservationRepo; Pages() PageRepo; Consolidation() ConsolidationRepo
	TryDerivationLock(ctx context.Context) error       // shared; ErrRefused → retry (N120)
	DerivationLockExclusive(ctx context.Context) error // Materialize and Restore: one 35 s attempt
}
type ObservationRepo interface {
	InsertVersion(ctx context.Context, v ObservationVersion, inputs []id.FactID, sources []Source) error // root_version, observation_inputs, observation_version_sources, vector, meta.superseded_at
	ReplaceSources(ctx context.Context, o id.ObservationID, add, drop []id.FactID) error   // the working set; only under the derivation lock; add before drop (N57)
	Latest(ctx context.Context, os []id.ObservationID, asOf time.Time) ([]ObservationVersion, error) // latest VISIBLE version with effective_at ≤ asOf (N117)
	MarkStale(ctx context.Context, os []id.ObservationID, k StaleKind) error                // narrow mutable row; hides nothing
	Hidden(ctx context.Context, o id.ObservationID, v id.Version) (bool, error)             // engram_obs_version_hidden
}
```

`PageRepo` (`InsertVersion`, `SetSources`, `Latest`, `MarkStale`, `Hidden`), `ConsolidationRepo`
(`Watermark`, `Advance`, `Proposals`, `Applied`, `Batches`) and `ExpungeRepo` (`Next`, `Record`,
`Materialize`, `ConsumersPassed`, `Finish`) follow the same shape.

**Operations and metering:**

```go
type Ops interface { Operations() OperationRepo; Idempotency() IdempotencyRepo; Ledger() LedgerRepo; Usage() UsageRepo; Outbox() outbox.Writer }
type OperationRepo interface {
	Insert(ctx context.Context, o OperationRow) error
	Transition(ctx context.Context, op id.OperationID, from, to fsm.OperationState, r Result) error // monotone; fsm-checked
	SetProgress(ctx context.Context, op id.OperationID, p Progress) error      // absolute values, once per wave (N69)
	Defer(ctx context.Context, op id.OperationID, until time.Time, d DeferredInfo) error
	PendingWithoutWorkflow(ctx context.Context, olderThan time.Duration) ([]OperationRow, error) // op-sweeper (N3)
}
```

`Idempotency` (`Begin`, `Commit`: the stored response returns on a replay within 24 h), `Ledger`
(`Append`, `Get`; no update or delete exists, a trigger rejects both), `Usage` (`Record`, `Day`;
exactly-once through `usage_key`, PD-1) are the same size. Ledger rows are deleted only by an
explicit document, namespace or tenant delete under the admin role (§5.4.2), never by a replace.

**Locks.** The per-document lock is `DocumentWriter.Lock` (shared try-lock for `CommitChunk`, exclusive
for `FinalizeVersion` and the delete marker transaction), the derivation lock is on `Derived`, the
exclusive fence on `AdminTx.Ownership().FenceExclusive` (`Freeze`, delete freeze, restore: one 35 s
attempt). Key spaces are disjoint (N113): namespace fence, derivation lock, document lock.

Roles: `engram_app` (API and worker alike; there is no worker role) runs `lock_timeout = 2 s`, `engram_move` and `engram_admin`
10 s, all with `synchronous_commit = local`. *Test seam:* testcontainers Postgres with the real
migrations — RLS (a query without `SET LOCAL` returns nothing), the append-only ledger,
`TestContent_InsertOnly` (N113: a test-only trigger raises on any `UPDATE` of a content row),
`TestFence_TryLockRefusedBehindWaiter`, `TestFence_PoolNotExhausted`,
`TestDocLock_NoStarvation`, `TestIso_Ownership_Transitions`, and the property test that every event
type at its maxima encodes to ≤ 16 KiB (N80).

#### 2.2.8 `internal/index` — search index abstraction

*Pattern: Strategy — recall arms see only `Searcher`; the index lives in the shard's Postgres
(`Transactional`) or in an external engine fed by the outbox (`Async`). The D7 name `index.Index`
denotes a `Searcher` plus, for an `Async` engine, the `Applier` that the `index` outbox sink drives.*

```go
package index

type Filter struct {
	AsOf *time.Time              // mentioned_at ≤ AsOf inside the predicate; chunk arm also embedding_effective_at ≤ AsOf (N85)
	FactTypes []memoryv1.FactType
	AllowedDocs []id.DocumentID  // the tag filter, resolved once (N116)
	Markers store.MarkerSets     // doc_tomb (document → up_to_version) / chunk_tomb / fact_hidden: applied to EVERY hit, also an external engine's (N44, N116)
}
type Hit struct { ID uuid.UUID; Kind Kind; Score float64 /* arm-local; normalised per query by the planner (N67) */; MentionedAt time.Time }

type Searcher interface {
	SearchSemantic(ctx context.Context, tx store.ReadTx, q SemanticQuery) ([]Hit, error)
	SearchLexical(ctx context.Context, tx store.ReadTx, q LexicalQuery) ([]Hit, error)
	SearchChunks(ctx context.Context, tx store.ReadTx, q ChunkQuery) ([]Hit, error)
	SearchPages(ctx context.Context, tx store.ReadTx, q ChunkQuery) ([]Hit, error)  // BM25 ∪ HNSW over visible page_versions (N73)
	Plan(ctx context.Context, tx store.ReadTx) (SemanticPlan, error)                // PlanExact | PlanHNSW
}
type Applier interface { Apply(ctx context.Context, events []*eventsv1.Event) error } // Async only; idempotent by (namespace_id, seq)
```

`SemanticPlan` (N112): **exact scan below 2,000 vectors** of the namespace's current embedding model
(≤ 3.2 MB, never truncated), otherwise the namespace's own **partial HNSW**
(`engram_vector_index_plan` decides, `engram_hnsw_ddl` generates the `CREATE INDEX CONCURRENTLY`
that `engramctl index` runs as the owner role; `plan_cache_mode = force_custom_plan`, `ef_search` =
the arm cap). One index per namespace and model; nothing is shared across namespaces. Neither `engram-api` nor
`engram-worker` runs DDL. Under `Filter.AsOf` a chunk hit's header is empty. Tag semantics exist once
(`index/tags.go`, mirrored by the Lean decision procedure). *Test seam:* the same retrieval golden set on all
implementations; `as_of` and visibility leakage tests assert zero hidden or future hits per arm.

#### 2.2.9 `internal/chunk` — heading-anchored content-defined chunker

Pure Go, no I/O: splits a document into deterministic chunks (target 3,000 chars, min 500, max
4,000, no overlap) at headings and then at content-defined cut points, forces a hard boundary
between items more than 24 h apart (N86) and builds the contextual header
`[doc summary ≤ 200 chars] > [heading path]`. *Pattern: Strategy (`CDCChunker` / `FixedChunker`).*

```go
package chunk

type Document struct { ID id.DocumentID; Content string; Items []Item /* timestamp + byte span */; BaseChunks []BaseChunk /* APPEND re-chunk */; Context string }
type Options struct { MaxItemGap time.Duration /* 24 h */; TargetChars, MinChars, MaxChars int; Summary, SummaryID string }
type Chunk struct {
	ContentHash [32]byte; Text, Header string; HeadingPath []string; ByteStart, ByteEnd, Tokens int
	ItemIndexes []int; MentionedAt time.Time // max over the covered items (and, on APPEND, the overlapped base chunks): the as_of key (N86)
}
type Chunked struct { Chunks []Chunk; TimestampsClamped int } // regressions are accepted and clamped up, never rejected

type Chunker interface { Chunk(ctx context.Context, d Document, o Options) (Chunked, error) }
type HeaderBuilder interface {
	Build(summary string, headingPath []string, docContext string) string
	HeaderHash(summaryID string, headingPath []string) [32]byte // sha256(heading path ‖ summary id), N60
}
func SummaryPolicy(mode memoryv1.UpdateMode, prevBytes, newBytes int) (recompute bool) // REPLACE, or > 25 % growth (N60)
```

`content_hash = sha256(text)` excludes the header (N6), so a chunk's identity survives a new
summary; the header is embedded into the **chunk vector only**, never into fact vectors. *Test seam:*
golden files and rapid properties (concatenation equals input, size bounds, determinism, the later
timestamp wins for two items, a boundary across a > 24 h gap).

#### 2.2.10 `internal/extract` — structured extraction with prompt versioning and cache

One structured LLM call per chunk, document summarisation, and a content-addressed per-namespace
cache in blob (D11). Owns the JSON schema and the prompt versions (`extract/v1`, `summarize/v1`, §6).
*Pattern: Decorator — `Cached` wraps any `Extractor`; cache errors are logged, never returned.*

```go
package extract

type Fact struct { Text string; Type FactType; Who, What, When, Where, Why string; OccurredStart, OccurredEnd *time.Time
	MentionedAt time.Time /* ALWAYS the chunk's mentioned_at, set by the activity: the model has no say in the as_of key (D9) */
	SaidAt *time.Time /* display and ranking only */; Entities []EntityMention; Causes []CausalRef; Confidence float32 }
type ChunkInput struct { Header, Text string; ContentHash [32]byte; ItemTimestamp time.Time; Context string; Metadata map[string]any; EntityHints, Tags []string; Mission string; HeaderHash [32]byte }
func RenderHash(in ChunkInput) [32]byte // sha256(day(mentioned_at) ‖ context ‖ canonical(metadata) ‖ sorted(entity_hints) ‖ retain.mission ‖ heading path): N87, N110(a) — NOT the document summary

type Extractor interface {
	Extract(ctx context.Context, in ChunkInput) (*Extraction, error)
	Summarize(ctx context.Context, content string) (string, *gateway.Usage, error)
}
type Cache interface {
	Get(ctx context.Context, k CacheKey) (*Extraction, bool, error) // "{prefix}xcache/{sha256(chunk_hash‖prompt‖model‖schema‖render_hash)}.json" = the extraction_key
	Put(ctx context.Context, k CacheKey, x *Extraction) error
}
func Cached(e Extractor, c Cache) Extractor
```

Validation after decode: non-empty text, `OccurredStart ≤ OccurredEnd`, causal indices in range,
`SaidAt ≤ ItemTimestamp + 5 min`; the decoder discards any `mentioned_at` the model emits. A violation
after one repair prompt is `PermanentLLMError` for that chunk. *Test seam:*
record/replay gateway; cache tests assert the key changes with each of its five inputs and each
component of `render_hash`, and with nothing else.

#### 2.2.11 `internal/entity` — per-namespace fuzzy resolution

Maps extracted mentions to `entities` using trigram similarity (reached only through the
`SECURITY DEFINER` function, N131), an alias table, mandatory type agreement and hints.
*Pattern: Strategy (`MergePolicy`).*

```go
package entity

type Resolution struct { Mention Mention; Entity id.EntityID; Created bool; Method string /* exact | alias | trigram | hint | new */; Score float32 }
type Options struct { Threshold float32 /* 0.6; 0.85 when a type is unknown */; MaxCandidates int; TypeStrict bool }

type Resolver interface { Resolve(ctx context.Context, tx store.ReadTx, ms []Mention, hints []string, o Options) ([]Resolution, error) }
type MergePolicy interface { ShouldMerge(m Mention, c Candidate) bool }
```

Creates and merges are applied by `EntityWriter.UpsertSorted` in `canonical_norm` order in one
statement, so concurrent commits cannot deadlock (N69). Under `as_of` a read returns only the
mention (N118); the expunge recomputes `canonical_name` from the remaining mentions. Resolution is
idempotent; a wrong merge is repaired by `engramctl entity split` (phase 3). Rejected: LLM-assisted
resolution.

#### 2.2.12 `internal/link` — link building under a budget

For each new fact, `fact_links` of four kinds: entity, temporal (±24 h, ≤ 20), semantic (kNN k = 10,
cosine ≥ 0.75 over `fact_vectors` of the current model) and causal; ≤ 60 per fact; links to
invisible facts are never built. Deterministic for equal input (ties by weight then id).
*Pattern: Strategy (`BudgetLinker`).*

```go
package link

type Linker interface { Build(ctx context.Context, tx store.ReadTx, idx index.Searcher, in BuildInput, b LinkBudget) (*BuildResult, error) }
type LinkBudget struct { TemporalPerFact, SemanticK, EntityPerFact, MaxPerFact int; SemanticMinCosine float32 }
```

*Test seam:* `MemIndex` +
`FakeTx`; no fact exceeds `MaxPerFact`, deterministic output.

#### 2.2.13 `internal/recall` — arms, fusion, rerank, boosts, packing, planner

The whole D10 read path with zero generative calls. *Patterns: **Strategy** (`Arm`) for what is
searched and **Pipeline** (`pipeline.Step[*State]`) for the order of the stages; the planner is the
composition and nothing else.*

```go
package recall

type Query struct {
	Scope authz.RequestScope; Text string; Vector []float32 /* nil: dense arms skipped */
	Budget Budget; Filter index.Filter; QueryTimestamp *time.Time; Window *TemporalWindow; MaxTokens int; Deadline time.Time
}
type Candidate struct { ID uuid.UUID; Kind index.Kind; Arm string; Rank int; ArmScore float64; MentionedAt time.Time; Provenance Provenance }

// Arm is the Strategy: one retrieval method. The planner schedules by Needs().
type Arm interface {
	Name() string // semantic | lexical | graph | temporal | chunks
	Needs() Needs // NeedsNone | NeedsVector | NeedsSeeds (the graph arm runs two seeded waves, N54)
	Run(ctx context.Context, tx store.ReadTx, q *Query, seeds []Candidate, capN int) ([]Candidate, error)
}
type Fuser interface    { Fuse(lists [][]Candidate, k int) []Fused }     // RRF k = 60 in math/big.Rat: permutation-invariant exactly (N67)
type Reranker interface { Rerank(ctx context.Context, query string, in []Fused, top int) ([]Ranked, error) } // top 0/50/150 (N53); bge-reranker-base
type Packer interface   { Pack(in []Ranked, maxTokens int) PackResult }  // greedy; skip, never truncate; rank 1 always whole (N129)
type Planner interface  { Recall(ctx context.Context, q *Query, sink Sink) error }

// The steps of the planner, in order; each is a pipeline.Step[*State].
//   Embed → Visibility (marker sets + allowed documents, N116) → Arms → Fuse → Rerank → Boost → Pack → Stream
```

`Booster` (`Boost`, recency ≤ +10 %, temporal ≤ +10 %, proof ≤ +5 %, clamp [0.75, 1.25]) and `Sink`
(`Batch`, `Stats`) are the two remaining one-method interfaces. Scheduling (§1.5, N54):
`NeedsNone` arms start immediately and overlap the embedding round-trip, `NeedsVector` arms when the
embedding arrives, the graph arm in two waves of 30 ms from the lexical and semantic top-20. Every
other arm gets `min(ArmDeadline, remaining − rerankReserve − packReserve)`. **Rerank is skipped when
the remaining deadline is below 106 ms** (= rerank p95 90 + pack 3 + stream 5 + 8, N106); at client
deadlines ≥ 300 ms a skip counts against the rerank-skip SLO (N53), below 300 ms it is by design. The
critical path is 216 ms p95 at MID. While markers are `pending` the observation and page arms pay a
per-candidate lookup and the skip SLO is suspended for the namespace (N119). Under `as_of` the chunk
arm adds `embedding_effective_at ≤ T`, headers are empty, entity names are suppressed. *Test seam:* rapid properties mirror the Lean theorems (RRF permutation invariance, packing
within budget, boost in [0.75, 1.25]); a fake-clock test asserts the 216 ms path and the 106 ms
reserve; leakage tests assert nothing hidden or future in any arm.

#### 2.2.14 `internal/consolidate` — facts → observations (two stages)

*Pattern: **Pipeline** of five single-purpose strategies (route, write, dedup, store, apply) so each
LLM call sees exactly what a delete may later need to hide (N121).* Stage 1 routes a batch of ≤ 8
facts against candidate observations and returns **decisions only**; stage 2 writes one version per
touched observation from that observation's own text and visible sources; a `merge` is a root rebuild.

```go
package consolidate

type Router interface { // stage 1: consolidate_route/v1
	Route(ctx context.Context, b Batch, cands []Candidate) (Routing, *gateway.Usage, error) // placements{attach|create|skip}, merges, drop_sources; nothing textual persisted
}
type Writer interface { // stage 2: consolidate_write/v1, one call per touched observation
	Write(ctx context.Context, in WriteInput) (ObservationWrite, *gateway.Usage, error)    // mode update | create | rebuild; only VISIBLE sources are rendered
}
type Adjudicator interface { Adjudicate(ctx context.Context, a, b string) (merge bool, u *gateway.Usage, err error) } // dedup_adjudicate/v1: a decision, never text
type ProposalStore interface {
	Store(ctx context.Context, tx store.Tx, p Proposal) (stored Proposal, err error) // ON CONFLICT DO NOTHING; the stored list always wins (N43)
	Load(ctx context.Context, tx store.Tx, key [32]byte) (Proposal, error)
	Discard(ctx context.Context, tx store.Tx, key [32]byte) error                    // only before any op was applied (RESTRICT FK)
}
type Applier interface { Apply(ctx context.Context, tx store.Tx, p Proposal) (Applied, error) }
```

`Apply` runs in one fenced derivation transaction (shared derivation lock, N120): `consolidation_applied(op_key)`
gates each write and commits with the effects (N43); the inputs are re-verified by the visibility predicate in a
fresh statement (facts are immutable, no row lock); versions carry `root_version`, `observation_inputs`
(the facts shown), `observation_version_sources` and a vector; the previous version's write-once
`superseded_at` is inserted; the working set `observation_sources` is rewritten only here. Nothing
hides anything: hiding is the read predicate. `effective_at(v) = max(mentioned_at of every fact shown,
effective_at(v − 1))`. `op_key = sha256(batch_key ‖ op_index)` over the **stored** list. While markers
are pending only root rebuilds run (degraded mode). *Test seam:* model-based tests from `Consolidation.tla` and
`Derivation.tla`: at-least-once re-execution yields one effect per `op_key`, and a version written with a
victim in view is hidden whichever of marker and apply commits first.

#### 2.2.15 `internal/reflect` — bounded agent loop (phase 2)

Forced searches (observations, then facts), ≤ 10 free iterations, ≤ 100 k context tokens, ≤ 300 s,
tool deadline 10 s; tools `search_memories`, `search_observations`, `search_pages` (N73), `get_page`,
`expand_fact`; citations filtered to ids a tool actually returned; optional JSON-schema output;
at the context cap a map/reduce fallback instead of forcing `done`. Every tool reads through the same
visibility predicate. *Pattern: Strategy (`Tool`) in a bounded loop; the agent runs inside `engram-api`
with the caller's scope, no second authz path.*

```go
package reflect

type Tool interface {
	Name() string
	Schema() json.RawMessage
	Call(ctx context.Context, sc authz.RequestScope, args json.RawMessage) (ToolResult, error) // ToolResult.ReturnedIDs feeds the citation verifier
}
type Agent interface { Run(ctx context.Context, req Request, caps Caps, sink EventSink) (*Result, error) }
type CitationVerifier interface { Verify(cited []string, returned map[string]struct{}) (kept, dropped []string) }
type SchemaValidator interface { Validate(schema, doc json.RawMessage) error }
type Registry interface { Register(t Tool); Get(name string) (Tool, bool); Specs() []gateway.ToolSpec }
```

`Request` carries the persona (`mission`, typed `Directive{text, priority, active, tags}` filtered by
the call's tags, `disposition`). Each iteration passes `quota.Reserve` (a refusal is
`RESOURCE_EXHAUSTED`, N130). Rejected: running Reflect as a workflow (interactive, streamed, ≤ 300 s).
*Test seam:* transcript replay; every emitted citation ∈ the
union of `ReturnedIDs`; caps with a fake clock.

#### 2.2.16 `internal/pages` — mental models / knowledge pages (phase 3)

Source query + tag filter, versioned markdown in blob, evidence segments per version (N117), one
`RefreshPolicy{trigger, interval, debounce}` (N128), delta refresh by `page/v1` and root rebuild by
`page_full/v1`. *Pattern: Repository, split into a read and a write capability.*

```go
package pages

type Reader interface {
	Get(ctx context.Context, sc authz.RequestScope, p id.PageID, sel VersionSelector) (*Page, string /*markdown*/, error) // PAGE_HIDDEN when hidden
	List(ctx context.Context, sc authz.RequestScope, q ListQuery) ([]Page, string, error)
	Search(ctx context.Context, sc authz.RequestScope, q SearchQuery) ([]PageHit, string, error) // BM25 ∪ HNSW over visible page_versions, RRF, no LLM (N73)
}
type Writer interface {
	Create(ctx context.Context, sc authz.RequestScope, p PageDraft) (*Page, error)
	Delete(ctx context.Context, sc authz.RequestScope, p id.PageID) error
	Refresh(ctx context.Context, h *router.ShardHandle, sc id.Scope, p id.PageID, why RefreshReason) (*RefreshResult, error)
}
```

`Page.RefreshPolicy` is the generated `memoryv1.RefreshPolicy`; there is no `on_delete`, `cron` or
string policy. `Refresh` runs as workflow `ns/{ns}/page/{page_id}`; a refresh whose base version is
hidden is a root rebuild. *Test seam:* testcontainers; staleness-flag and `PAGE_HIDDEN` tests.

#### 2.2.17 `internal/workflows` — Temporal workflows, activities, policies

All workflow definitions and the activity *interfaces* they call; task-queue naming; `ContinueAsNew`
rules; retry policies; the API's client wrapper. Concrete activities are structs in
`cmd/engram-worker` that wire service packages into these interfaces. *Pattern: **Pipeline** (the
workflow body is the ordered list of activity groups) and **Saga** (move, expunge, tenant delete) —
each activity is idempotent by key and fenced by the epoch, so a restart resumes instead of repeating.*

```go
package workflows

func TaskQueue(s id.ShardID) string            // "shard-{id}"
func OpID(ns id.NamespaceID, op id.OperationID) string // "ns/{ns}/op/{op}"
func ExpungeID(ns id.NamespaceID) string        // "ns/{ns}/expunge"
func ConsolidateID(ns id.NamespaceID) string    // "ns/{ns}/consolidate"
func PageRefreshID(ns id.NamespaceID, p id.PageID) string // "ns/{ns}/page/{page}"
func MoveWorkflowID(ns id.NamespaceID, e id.Epoch) string // "move/{ns}/{epoch}" on shard-{target}

// Workflows (inputs and results are protos in engram.internal.workflow.v1; every input carries schema_version 2).
func RetainDocument(ctx workflow.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.RetainDocumentResult, error)
func Consolidate(ctx workflow.Context, in *workflowv1.ConsolidateInput) error    // long-lived; Nudge signal; debounce 30 s; ContinueAsNew every round
func Expunge(ctx workflow.Context, in *workflowv1.ExpungeInput) (*workflowv1.ExpungeResult, error) // also target NAMESPACE (§5.4)
func PageRefresh(ctx workflow.Context, in *workflowv1.RefreshPageInput) (*workflowv1.RefreshPageResult, error)
func Move(ctx workflow.Context, in *workflowv1.MoveInput) (*workflowv1.MoveResult, error)
// ExportSnapshot, TenantDelete, RetainBackfill, ReembedNamespace and the schedules (SweeperInput) follow the same shape.

// The retain pipeline is three ordered activity groups; the workflow runs Prepare once, ChunkPipeline per chunk
// (≤ 32 in flight, a workflow-side semaphore), then Finish.
type Prepare interface {
	LoadItem(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.LoadItemResult, error)     // APPEND base under the documents row lock (N56); single ver/{sha256} body blob (N104)
	Chunk(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.ChunkPlan, error)
	SummarizeDocument(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.SummarizeDocumentResult, error)
	PlanChunks(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.ChunkPlan, error)         // member | live | tombstoned | stale_extraction | absent
	MarkProgress(ctx context.Context, in *workflowv1.MarkProgressInput) error                                  // once per wave (N69)
}
type ChunkPipeline interface {
	ExtractChunk(ctx context.Context, sc *workflowv1.WorkflowScope, w *workflowv1.ChunkWork) (*workflowv1.ExtractChunkResult, error)
	EmbedChunk(ctx context.Context, sc *workflowv1.WorkflowScope, w *workflowv1.ChunkWork, x *workflowv1.ExtractChunkResult) (*workflowv1.EmbedChunkResult, error)
	ResolveEntities(ctx context.Context, sc *workflowv1.WorkflowScope, x *workflowv1.ExtractChunkResult) (*workflowv1.ResolveEntitiesResult, error)
	BuildLinks(ctx context.Context, sc *workflowv1.WorkflowScope, x *workflowv1.ExtractChunkResult, e *workflowv1.EmbedChunkResult, r *workflowv1.ResolveEntitiesResult) (*workflowv1.BuildLinksResult, error)
	CommitChunk(ctx context.Context, in *workflowv1.CommitChunkInput) (*workflowv1.CommitChunkResult, error) // inserts only; DocumentBusy retryable; InputBlobMissing re-runs extract+embed ≤ 2× (N100)
}
type Finish interface {
	FinalizeVersion(ctx context.Context, in *workflowv1.FinalizeVersionInput) (*workflowv1.FinalizeVersionResult, error) // inserts tombstones and fact_hidden; superseded_by (N49)
	ReembedChunk(ctx context.Context, in *workflowv1.ChunkWork) error                                                     // inserts one chunk_vectors row (N60, N111)
	MarkOperation(ctx context.Context, in *workflowv1.MarkOperationInput) error
}

// Consolidation steps: RouteBatch, WriteObservation, StoreProposal, ApplyBatch, StampFailed (≤ 5 per interface); the
// quota gate is a method of quota.Reserver, called inside the LLM activities, not a separate activity (N130).

type Client interface { // used by engram-api; wraps client.Client
	StartRetain(ctx context.Context, in *workflowv1.RetainDocumentInput) error        // AlreadyStarted → nil
	SignalConsolidate(ctx context.Context, t *workflowv1.ConsolidateTrigger) error     // SignalWithStart
	SignalExpunge(ctx context.Context, in *workflowv1.ExpungeInput) error             // SignalWithStart ns/{ns}/expunge
	Cancel(ctx context.Context, workflowID string) error
	WaitResult(ctx context.Context, workflowID string, timeout time.Duration) (done bool, err error) // WaitOperation long-poll (N70)
}
func DataConverter(keys KeyProvider) converter.DataConverter // AES-256-GCM codec, per-namespace data key wrapped by the shard key (N59, N99)
type KeyProvider interface {
	DataKey(ctx context.Context, ns id.NamespaceID, keyID string) ([]byte, error)
	CurrentKeyID(ns id.NamespaceID) string
	Shred(ctx context.Context, ns id.NamespaceID) error // the shredding step of a namespace or tenant delete
}
const MaxInlineResultBytes = 4 << 10
```

Payload rule (N59): every activity result above 4 KiB travels by blob key, `CommitChunkInput` is
keys-only, `ChunkWork` carries no text; `RetainDocument` continues-as-new every 100 chunks or 20 MB of
history. Retry policies are the named ones of §5.0 and §5.8 (`P-pure`, `P-db`, `P-frozen`, `P-llm`,
`P-embed`, `P-blob`, `P-catalog`, `P-cutover`, `P-temporal`); a `WrongShardOrEpoch` in any activity fails
the workflow with a typed non-retryable failure. *Test seam:* Temporal `testsuite` with mocked activities (fan-out bound, idempotent
re-execution, `ContinueAsNew` at 100 chunks and at 20 MB, deferral on quota); a payload test asserts no
recorded payload exceeds `MaxInlineResultBytes`, that every payload is ciphertext, and that a
namespace's payloads are unreadable after `Shred`; a chaos test kills workers mid-`CommitChunk`.

#### 2.2.18 `internal/outbox` — transactional outbox, relay, cursors, sinks

*Pattern: **Transactional outbox** — `Writer.Append` is the last statement of every write transaction,
so a consumer sees exactly the committed changes, with no dual write.* One relay per shard, elected
with `pg_try_advisory_lock` on a dedicated direct connection, reads `outbox` in `seq` order in batches
of 500, drives named sinks with independent cursors and delivers a strict prefix (no cursor passes an
open gap; sound under A-F1, 60 s watchlist).

```go
package outbox

type Writer interface { // inside the same transaction as the state change (D6)
	Append(ctx context.Context, ev *eventsv1.Event) (seq int64, err error)
	AppendAll(ctx context.Context, evs []*eventsv1.Event) ([]int64, error) // ONE multi-row INSERT: still the last statement
}
type Sink interface {
	Name() string // "index" | "kafka"
	Apply(ctx context.Context, events []Event) error // idempotent by (namespace_id, seq)
	Filter() func(Event) bool
}
type Relay struct { /* handle, sinks, cursors, gaps */ }
func NewRelay(h *router.ShardHandle, sinks []Sink, o RelayOptions) *Relay
func (r *Relay) Run(ctx context.Context) error           // acquires the lock; returns when lost
func (r *Relay) AddSink(ctx context.Context, s Sink, fromSeq int64) error
type GapWatch interface { Note(seq int64, seenAt time.Time); Due(now time.Time) []int64; Resolve(seq int64) } // persisted in outbox_cursors.gaps (≤ 1 000)

// Group reassembles a paged event group (N80): complete only when every page was seen; an ids_elided event is complete alone.
type Group interface { Add(e Event) (complete bool); Ids() [][]byte; Elided() bool }
```

The relay uses the shard-level `engram_relay` role (RLS bypass for `SELECT` on `outbox` only). Events
are thin and bounded (≤ 256 ids, ≤ 16 KiB; `DocumentDeleted` has no id list); cursor advances are
batched to one per second per consumer (N114); the expunge reads the cursors through
`engram_consumers_passed` before it purges. **Moves are not consumers** (N124). *Test seam:* a model-based test from
`Outbox.tla` — random commit orders with holes deliver every seq exactly once or declare it aborted
after the watch window.

#### 2.2.19 `internal/move` — namespace move saga (D5)

The D5 protocol as Temporal workflow `move/{ns}/{epoch}` on `shard-{target}` (N2): **dirty copy →
freeze → reconcile by set difference → cutover with a `ready` state** (N124, N125). *Pattern: **Saga**
with a **state machine** — every step is idempotent, every step before cutover (c) has a compensation,
and (c) is the point of no return. It is the only code path that holds two shard handles.*

```go
package move

type Fence struct { Ns id.NamespaceID; Tenant id.TenantID; Source, Target id.ShardID; Epoch id.Epoch /* e; the target holds e+1 */; Move id.MoveID }

// Activities are split by phase so each interface stays small. Every activity first re-checks the fence against
// the catalog row and both ownership rows and the source session's system_identifier/timeline (MoveFenced).
type Copier interface {
	Plan(ctx context.Context, f Fence) (*PlanResult, error)         // plan_target / start_move edges; records system_id, timeline, t_copy; pauses expunge and schedulers
	BulkCopy(ctx context.Context, f Fence) (*CopyResult, error)     // READ COMMITTED key ranges ≤ 100 k rows; resumable at (table, last_key)
	Freeze(ctx context.Context, f Fence) error                      // exclusive fence, one 35 s attempt, freeze_move edge; watchdog armed until (c)
	Reconcile(ctx context.Context, f Fence) (*workflowv1.ReconcileReport, error) // re-copy created_at ≥ t_copy − 10 min; mutable merge-diff; blobs; relay drain; VerifyFK
	Drain(ctx context.Context, f Fence) (*DrainResult, error)       // in-flight set read from the source operations rows (N97)
}
type Cutover interface {
	Begin(ctx context.Context, f Fence) error          // (a) intent only
	ReadyTarget(ctx context.Context, f Fence) error    // (b′) ready_target
	Source(ctx context.Context, f Fence) error         // (c) cutover_c: frozen/move → moved_out + target hint — THE point of no return
	ActivateTarget(ctx context.Context, f Fence) error // (b″) activate_target, retried indefinitely
	Catalog(ctx context.Context, f Fence) error        // (d) shard, epoch, state, NOTIFY — retried indefinitely
}
type Closer interface {
	Restart(ctx context.Context, f Fence, d *DrainResult) error // ns/{ns}/op/{op} on shard-{target}, TERMINATE_IF_RUNNING, memo epoch e+1; reconcile loop
	Cleanup(ctx context.Context, f Fence) error                  // after 24 h: DROP INDEX, engram_cleanup_namespace, old prefix; the moved_out row stays
	Rollback(ctx context.Context, f Fence, why string) error     // only before (c); see the table
}
type Orchestrator interface { // MoveService
	Start(ctx context.Context, ns id.NamespaceID, target id.ShardID, o StartOptions) (*Ref, error)
	Status(ctx context.Context, m id.MoveID) (*Status, error)
	Abort(ctx context.Context, m id.MoveID) error // rollback before (c), else an error
}
```

| Step | Compensation (what `Rollback` runs when this step has completed) |
|---|---|
| `Plan` | `rollback_target` (delete the target rows and ownership row; for a move back onto a `moved_out` shard `return_abort` restores the permanent fence value); `abort_move` on the source |
| `BulkCopy`, `Reconcile` | the same (copied rows are discarded) |
| `Freeze` | `thaw_move` first, then the above |
| `ReadyTarget` (b′) | `unready_target` first, then `thaw_move` and the above |
| `Source` (c) onward | none: the move completes forward; a reverse move is an ordinary new move |

`StartOptions` carries `DrainWait` (15 s, max 60 s), `FreezeWatchdog(liveFacts)` (`max(120 s, 60 s + 1 s per
10 k facts)`, ≤ 15 min), `CopyRangeRows` 100 000, `CopyStreams` 4, `CleanupGrace` 24 h and the cutover retry
(100 ms ×1.5 → 1 s within 60 s; (b″) and (d) unlimited). *Test seam:* a model-based test from `ShardMove.tla`
with `MemoryCatalog` and two `FakeTx` shards: concurrent retain, consolidate and delete during the copy,
a row committed between the last range copy and `Freeze`, rollback from every step before (c), a zombie primary
with a stale timeline; a chaos test kills the worker in every phase and between (c) and (b″).

#### 2.2.20 `internal/export` — snapshots for local agentic search (phase 3)

`building` → `ready` snapshots under `{shard}/{tenant}/{ns}/export/v{n}/` (manifest, facts, observations,
chunks, pages, the always-emitted delta with `deleted_ids`) and 1 MiB part streaming (N126).
*Pattern: Builder with a commit point — the manifest is written last.*

```go
package export

type Builder interface {
	Begin(ctx context.Context, tx store.Tx, o BuildOptions) (id.SnapshotVersion, error)       // inserts the row as 'building'
	WriteFiles(ctx context.Context, h *router.ShardHandle, sc id.Scope, v id.SnapshotVersion) (*Manifest, error) // one REPEATABLE READ snapshot, visibility predicate on every row
	Record(ctx context.Context, tx store.Tx, m *Manifest) error                                // refuses to promote an expired row; re-checks tombstones newer than the start
}
type Streamer interface {
	Stream(ctx context.Context, sc authz.RequestScope, v id.SnapshotVersion, part func(ctx context.Context, p Part) error) error // refuses expired versions
	Latest(ctx context.Context, sc authz.RequestScope) (*Manifest, error)
}
```

The delete marker transaction expires `building` and `ready` snapshots alike
(`SnapshotRepo.ExpireContaining`); the delta is a diff of the two snapshots' id indexes, so a delete
reaches a client even when the base expired.

#### 2.2.21 `internal/quota` — rates, token metering, admission

*Pattern: Gate — one `Reserve` in front of every gateway call class (N130).*

```go
package quota

type Limiter interface { Allow(ctx context.Context, k Key, n int64) (Decision, error) } // in-process token buckets (recalls/retains per min); per-process share limit/N_api
type Reserver interface {
	Reserve(ctx context.Context, sc id.Scope, call CallClass, tokens int64) (Reservation, error) // extract | summarize | route | write | refresh | reflect-iteration
}
type Meter interface {
	Record(ctx context.Context, tx store.Tx, op string, u gateway.Usage, day time.Time) error // exactly-once through usage_key (PD-1)
	Remaining(ctx context.Context, tx store.ReadTx, l Limits, day time.Time) (tokens, facts int64, err error)
}
type Deferral interface {
	Defer(ctx context.Context, tx store.Tx, op id.OperationID, until time.Time, why DeferredInfo) error
	Resume(ctx context.Context, tx store.Tx, op id.OperationID) error
}
```

A refused `Reserve` defers a workflow (`DEFERRED`, `resume_at` = the window reset) and returns
`RESOURCE_EXHAUSTED` to a synchronous Reflect. Tenant limits are per-namespace shares computed by the
catalog from trailing usage plus a floor, with unused share borrowable at the daily recompute.
Rejected: a global Redis limiter.

#### 2.2.22 `internal/config` — layered configuration

```go
package config

type Resolver interface { Resolve(system, tenant, namespace Layer) (*Resolved, error) } // namespace ⊃ tenant ⊃ system; unknown keys → errs.Validation
var Keys []Key // the single allow-list (N66): the resolver, the Namespace.config_overrides proto comment and the docs are generated from it
```

`Resolved` has `Models` (`Extract`, `Consolidate`, `Reflect`, `Rerank`; **`Embed` and `EmbedDims` are
fixed at namespace creation and read from the catalog entry, N111**), `Prompts`, `Chunk`, `Retain.Mission`,
`Recall`, `Quota`, `Consolidation{Enabled, Debounce, Mission, ObservationScope,
MaxObservationsPerScope, MaxRebuildsPerRound}`, `Reflect.KeepTranscripts` and a `Version` hash.
Resolution happens once per catalog entry and is cached with it; the `Version` travels in activity
inputs so a mid-operation change is visible in traces but does not change an in-flight activity's model.

#### 2.2.23 `internal/telemetry` — tracing, metrics, label policy

```go
package telemetry

type Registry interface { Counter(name string, labels ...string) Counter; Histogram(name string, b []float64, labels ...string) Histogram; Gauge(name string, labels ...string) Gauge }
type TenantMeter interface { Report(ctx context.Context, t id.TenantID, from, to time.Time) (*UsageReport, error) } // from token_usage; replaces a tenant label (N62)
func StartStage(ctx context.Context, stage string) (context.Context, func(err error))
```

The shard label is mandatory and `tenant`/`namespace` labels panic at registration (N62). The names
and labels are those of the §9.4 table; the ones this section's code emits are
`engram_recall_stage_seconds{shard,stage,budget}`, `engram_recall_rerank_skipped_total{shard,reason}`,
`engram_visibility_marker_entries{shard,kind}` (alert above 16 k),
`engram_expunge_oldest_pending_seconds{shard,phase}` (the SLA metric; `ExpungeMaterializeSlow` pages at 900 s),
`engram_degraded_namespaces{shard}` and `engram_move_phase{shard}`.

#### 2.2.24 `internal/ledger` — append-only ingest ledger

`ledger.Writer.Append(ctx, tx, Entry)` and `Reader` (`Get`, `ListByDocument`); there is no update or delete
method and a trigger rejects both. Inline body ≤ 64 KiB, else the blob key. Ledger rows are deleted only by
an explicit document, namespace or tenant delete under the admin role (the expunge, §5.4.2), never by a
replace or append retire (N104).

#### 2.2.25 `adapters/mcp` — MCP server

Per-namespace endpoints `/mcp/{tenant_id}/{namespace_id}`; tools generated from `memory.v1` at build
time (`protoc-gen-engram-mcp`), so the surface cannot drift from the API (the §4.6 table is the golden
list). *Pattern: **Adapter** over a gRPC *client* of the core — it holds no service and no storage, so it
cannot bypass the interceptor.*

```go
package mcp

type Tool struct { Name, Method string; InputSchema json.RawMessage; Scope authz.Scope; Streaming bool } // generated
type CoreClient interface { Memory() memoryv1.MemoryServiceClient; Document() memoryv1.DocumentServiceClient; Page() memoryv1.PageServiceClient; Operation() memoryv1.OperationServiceClient }
type Adapter interface { Tools() []Tool; Handler() http.Handler } // streamable HTTP; the JWT is forwarded unchanged
func New(core CoreClient, tools []Tool, v authz.TokenVerifier, o Options) Adapter
```

The adapter validates the token with the same `TokenVerifier` before listing tools and serves
`/.well-known/oauth-protected-resource` (N129). Idempotency: `request_id = sha256(mcp-session-id ‖
jsonrpc-id ‖ tool)` (N127). Read tools are always listed, write tools only when the JWT carries
`memory.write`; the core re-checks. *Test seam:* the isolation matrix (§8) runs every tool with foreign
tokens and asserts code parity with gRPC.

#### 2.2.26 `adapters/connect` — ConnectRPC handlers

`connect.Mount(mux, server, interceptors...)` registers the generated `memoryv1connect.New*Handler`s on the
same `http.ServeMux` as the gRPC server (h2c, one port), with `authz.Interceptor.Connect()` and
`api.DeadlineGuard` (`Connect-Timeout-Ms` maps to the deadline). *Pattern: **Adapter** — the same handler
implementation, no second annotation layer.* Rejected: grpc-gateway.

#### 2.2.27 `cmd/engramctl` — operator CLI

| Command | Does |
|---|---|
| `shard provision / migrate / activate / full / drain / readonly / retire` | creates DB, roles (`engram_migrate`, `engram_app`, `engram_relay`, `engram_move`, `engram_admin`), extensions, partitions, RLS; goose rollout with `--canary`; catalog state changes |
| `index --shard 7 --namespace <ns>` | runs the statements of `engram_hnsw_ddl` chosen by `engram_vector_index_plan` as the owner role (`CREATE INDEX CONCURRENTLY` per partition, `DROP INDEX`, `REINDEX INDEX CONCURRENTLY`); records `vector_indexes` |
| `move start / status / abort / cleanup` | `MoveService` |
| `restore --shard 7 --to …` / `restore replay` | pgBackRest restore, open-move reconcile, epoch bump (`catalog epoch + 1`), `frozen/restore`, then replay of the delete intents newer than `restore_point − 10 min` (N122, N123) |
| `report --tenant acme --from … --to …` | per-tenant token and cost report from `token_usage` (N62) |
| `config lint` | checks the `postgres -c` list and role GUCs against the generated table (N131) |
| `outbox trim / skip`, `catalog flush-cache`, `shard stats`, `backup create`, `entity split` (phase 3) | as named |

#### 2.2.28 `internal/intent` — the delete-intent log (N122)

*Pattern: Append-only log in the blob store — an acknowledged delete exists in two places before the ack.*

```go
package intent

type Intent struct { Kind Kind /* document | namespace | tenant | invalidate | restore */; Subject string; Tenant id.TenantID; Namespace id.NamespaceID; Operation id.OperationID; DeletedAt time.Time; RequestHash [32]byte; Epoch id.Epoch }

type Log interface {
	Put(ctx context.Context, in Intent) (key string, err error) // _control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json: shard-independent, strongly consistent
	List(ctx context.Context, ns id.NamespaceID, since time.Time) ([]Intent, error) // in name order, for the restore replay
	Trim(ctx context.Context, olderThan time.Duration) (int, error)                  // 35 days
}
```

`Put` precedes the marker transaction; a replay applies each namespace's intents with `deleted_at ≥
restore_point − 10 min` in name order through the admin variant of the marker transaction, last state
per subject wins (`Restore` after `Invalidate` is honoured).

#### 2.2.29 `internal/expunge` — the asynchronous half of a delete (N119)

*Pattern: **Saga that only goes forward** — Materialize, Purge, Index, Finish are idempotent, resumable
from `expunge_progress`, throttled (1,000 rows, 50 ms) and paused while a move is open; there is no
compensation because the marker already made the delete effective.*

```go
package expunge

type Expunger interface {
	Materialize(ctx context.Context, in *workflowv1.MaterializeInput) (*workflowv1.MaterializeResult, error) // exclusive derivation lock, one 35 s attempt
	PurgeBatch(ctx context.Context, in *workflowv1.PurgeBatchInput) (*workflowv1.PurgeBatchResult, error)    // waits for every consumer cursor past the delete's seq
	PurgeBlobs(ctx context.Context, in *workflowv1.ExpungeInput) (int64, error)
	ReindexHygiene(ctx context.Context, in *workflowv1.ExpungeInput) (int, error)  // REINDEX INDEX CONCURRENTLY above 5 % dead
	Finish(ctx context.Context, in *workflowv1.ExpungeInput) error                  // expunge_state 'purged'; tombstone dropped 24 h later
}
```

SLA: materialize ≤ 15 min, purge ≤ 24 h, index ≤ 48 h. *Test seam:* `Derivation.tla` and `Durability.tla`
model-based tests; a T3 test that a purge of a 100 k-fact document never exceeds the writer timeout.

### 2.3 Swappable implementations

Every seam below is an interface of at most five methods chosen once, at wiring time, in a `cmd/*`
composition root. Test seams stay with their module in 2.2; this table is the only place that lists
what can be substituted.

| Seam (package) | Default | Alternates (what for) |
|---|---|---|
| `authz.TokenVerifier` | `JWKSVerifier` | `StaticKeyVerifier` (dev), `AllowAllVerifier` (isolation tests only, never in release images) |
| `catalog.Namespaces` / `Resolver` | `PostgresCatalog` + `LRUResolver` | `MemoryCatalog` (CAS semantics, epoch-flip faults), `StaticResolver` (single-shard dev) |
| `router.ShardRouter` | `StaticRouter` | `SingleShardRouter` (dev), `RecordingRouter` (cardinality assertions) |
| `gateway.Embedder`, `Structured`, `Chatter`, `Reranker`, `Batcher` | `HTTPClient` | `RecordReplayClient` (golden JSON), `DeterministicClient` (hash embeddings, canned extraction, fault knobs `GW_LATENCY_MS`, `GW_FAIL_RATE`, `GW_BAD_DIMS_RATE`) |
| `blob.Store` | `S3Store` | `FSStore` (dev), `MemStore` (tests, fault knobs); one conformance suite runs against all three |
| `store.Store` | `PgxStore` | `RecordingStore` (records shard and namespace per statement), `FakeTx` (in-memory repositories with the same fencing state machine) |
| `index.Searcher` | `PostgresIndex` (pg_search BM25 + per-namespace partial HNSW or exact scan) | `TsvectorIndex` (no pg_search), `ExternalIndex` (`Async`, `engram-shard-{id}`), `MemIndex` (brute force, recall unit tests) |
| `chunk.Chunker` | `CDCChunker` | `FixedChunker` (baselines) |
| `extract.Extractor` / `Cache` | `LLMExtractor` + `BlobCache` | `BatchExtractor` (gateway batch API, same cache key), `RuleExtractor` (smoke runs), `MemCache` |
| `entity.Resolver` | `TrigramResolver` | `ExactResolver` |
| `link.Linker` | `BudgetLinker` | `NoSemanticLinker` (embeddings unavailable) |
| `recall.Arm` | semantic, lexical, graph, temporal, chunks | any subset (a budget or a test switches arms off); `MemIndex`-backed arms in unit tests |
| `recall.Fuser`, `Reranker`, `Packer` | `RRFFuser`, `GatewayReranker`, `GreedyPacker` | `NoopReranker` (`stage = FUSED`) |
| `recall.Planner` | the `pipeline.Step` planner (2.2.13) | `FakePlanner` (API handler tests) |
| `consolidate.Router`, `Writer`, `Adjudicator` | `LLMRouter`, `LLMWriter`, `LLMAdjudicator` | `RecordReplay*`, `NoopConsolidator` |
| `reflect.Agent` | `LoopAgent` | `ScriptedAgent` (transcript replay) |
| `workflows` activity interfaces | the structs wired in `cmd/engram-worker` | `FakeActivities` (Temporal `testsuite`) |
| `outbox.Sink` | `IndexSink` (a no-op for `Transactional` indexes) | `KafkaSink`, `MemSink` |
| `move.Orchestrator` | `TemporalOrchestrator` | `InlineOrchestrator` (phases in-process) |
| `export.Builder` / `Streamer` | blob-backed | `MemStore`-backed |
| `intent.Log` | `BlobLog` | `MemLog` |
| `quota.Limiter` / `Reserver` | token bucket + `token_usage` | `UnlimitedLimiter` (tests) |
| `telemetry.Registry` | OTel + Prometheus | `NoopRegistry` |

Not swappable by design: the generated gRPC servers, `adapters/*` (one wire shape, no logic) and
`internal/ledger`, whose only implementation is the append-only table.

### 2.4 Error taxonomy — `internal/errs`

A leaf package (N1): store, services and workflows produce typed errors but may not import
`internal/api`. `errs` imports the generated detail messages of `memory.v1` (public) and
`engram.internal.errors.v1` (internal, N128) and the gRPC/Connect status packages. *Pattern: a closed
sum type — `Kind` is the classification, each Kind maps to one gRPC code, and the mapping lives once.*

```go
package errs

// Kind is the stable classification. Public kinds are rendered to callers; the three internal kinds are
// consumed by the router or the retry policy and are dropped by the interceptor (N128).
type Kind int

const (
	KindValidation         Kind = iota + 1 // INVALID_ARGUMENT     + memoryv1.ValidationError
	KindNotFound                           // NOT_FOUND            + memoryv1.NotFound
	KindQuotaExceeded                      // RESOURCE_EXHAUSTED   + memoryv1.QuotaExceeded + RetryInfo
	KindWrongShardOrEpoch                  // FAILED_PRECONDITION  + memoryv1.WrongShardOrEpoch (epochs and state only)
	KindNamespaceFrozen                    // FAILED_PRECONDITION  + memoryv1.NamespaceFrozen{reason: MOVE|RESTORE|DELETE|UNSPECIFIED}
	KindNamespaceNotReady                  // UNAVAILABLE          + memoryv1.NamespaceNotReady (target shard `ready`, N125)
	KindOperationConflict                  // ABORTED | ALREADY_EXISTS + memoryv1.OperationConflict{reason}
	KindPreconditionFailed                 // FAILED_PRECONDITION  + memoryv1.PreconditionFailed
	KindUnavailable                        // UNAVAILABLE          + RetryInfo
	KindPermanentLLM                       // INTERNAL (never retried) + ErrorInfo{PERMANENT_LLM_ERROR}
	KindUnauthenticated                    // UNAUTHENTICATED
	KindPermissionDenied                   // PERMISSION_DENIED    + ErrorInfo{MISSING_SCOPE}
	KindDeadline                           // DEADLINE_EXCEEDED
	KindInternal                           // INTERNAL

	KindFenceBusy        // internal: the shared try-lock was refused; rendered as NamespaceFrozen{UNSPECIFIED, 200 ms}
	KindDocumentBusy     // internal, ABORTED on the write path only; retried by P-frozen at 100 ms (N83)
	KindInputBlobMissing // internal, activity error: the chunk sub-pipeline re-runs extract + embed ≤ 2 times (N100)
)

type Error struct {
	Kind     Kind
	Msg      string
	Public   proto.Message // typed detail from memory/v1/errors.proto, may be nil
	Internal proto.Message // typed detail from engram/internal/errors/v1 (MovedOutHint, FenceBusy, …), never rendered
	Retry    time.Duration // 0 = no RetryInfo
	Cause    error
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
func (e *Error) GRPCStatus() *status.Status // renders Public only

// Constructors: the only way to create typed errors.
func Validation(field, desc string) *Error
func NotFound(resource, id string) *Error
func QuotaExceeded(quota string, scope memoryv1.QuotaScope, limit, current int64, retryAfter time.Duration) *Error
func WrongShardOrEpoch(ns id.NamespaceID, expected, actual id.Epoch, st memoryv1.NamespaceState) *Error
func MovedOut(ns id.NamespaceID, expected, actual id.Epoch, hint *errorsv1.MovedOutHint) *Error // hint is Internal
func NamespaceFrozen(ns id.NamespaceID, r memoryv1.FreezeReason, retryAfter time.Duration) *Error
func NamespaceNotReady(ns id.NamespaceID, retryAfter time.Duration) *Error
func FenceBusy(ns id.NamespaceID) *Error
func DocumentBusy(doc id.DocumentID) *Error
func InputBlobMissing(blobKey, input string, contentHash [32]byte, attempt int) *Error
func OperationConflict(op, existing id.OperationID, why memoryv1.OperationConflictReason) *Error
func PreconditionFailed(typ, subject, desc string) *Error
func Unavailable(desc string, retryAfter time.Duration) *Error
func ShardNotLocal(cell string) *Error // KindUnavailable; the router's Forwarder consumes it (D4, N71)
func PermanentLLM(model, status string, cause error) *Error
func Wrap(kind Kind, msg string, cause error) *Error

// Predicates and mapping.
func Is(err error, k Kind) bool
func IsRetryable(err error) bool                  // Unavailable, Deadline, NamespaceFrozen, NamespaceNotReady, FenceBusy, DocumentBusy (bounded by the caller)
func ToStatus(err error) *status.Status           // unknown errors → INTERNAL with a redacted message; Internal details dropped
func ToConnect(err error) *connect.Error          // same codes and details via AddDetail
func FromStatus(st *status.Status) *Error         // client side (router.Forward, engram-mcp); keeps Internal details when the peer is internal
func TemporalType(err error) string               // ApplicationError type: "WrongShardOrEpoch", "NamespaceFrozen", "NamespaceNotReady", "DocumentBusy", "InputBlobMissing", "ValidationError", "PermanentLLMError", "QuotaExceeded"
```

| `errs.Kind` | gRPC code | Retryable | Produced by |
|---|---|---|---|
| `Validation` | `INVALID_ARGUMENT` | no | api, chunk, extract, config, index (tags) |
| `NotFound` | `NOT_FOUND` | no | authz (cross-tenant namespace, by design), store |
| `QuotaExceeded` | `RESOURCE_EXHAUSTED` | after `retry_after` | rate limits via authz; token and fact quotas never fail a workflow, `quota.Reserve` defers it (N130) |
| `WrongShardOrEpoch` | `FAILED_PRECONDITION` | writes: once, after a re-resolve; `MOVED_OUT`: follow the internal hint, reads then a bounded loop ≤ 5 s (N52, N98); never for a workflow | store ownership check, move fence |
| `NamespaceFrozen` | `FAILED_PRECONDITION` | bounded (≤ 30 s in the API; `P-frozen` in workflows) | store ownership check (`frozen/*`), the refused fence try-lock (`FenceBusy`, 200 ms) |
| `NamespaceNotReady` | `UNAVAILABLE` | yes, inside the API's 5 s re-resolve loop (N125) | store ownership check on a `ready` shard |
| `OperationConflict` | `ABORTED` (concurrent conflict) / `ALREADY_EXISTS` (`IDEMPOTENCY_KEY_REUSED`) | `ABORTED`: wait for `existing_operation_id`, resubmit | api idempotency; `NAMESPACE_BUSY`, `PAGE_REFRESHING` (a retain into a deleted document id is not a conflict, N133c) |
| `PreconditionFailed` | `FAILED_PRECONDITION` | no | catalog CAS, move cutover, etag and version checks, snapshot expiry, `PAGE_HIDDEN`, `ALREADY_INVALIDATED` (types in §4) |
| `Unavailable` | `UNAVAILABLE` | yes | catalog miss when down, gateway 429/5xx, Temporal start failure, shard down, `ShardNotLocal` |
| `PermanentLLM` | `INTERNAL` | no | gateway 4xx, wrong embedding dims, schema-invalid output after one repair prompt |
| `DocumentBusy`, `InputBlobMissing`, `FenceBusy` | not rendered | by `P-frozen` / the workflow (see above) | `CommitChunk` try-lock (N83); `CommitChunk` reading a purged blob (N100); the fence try-lock (N82) |

`OperationConflict` alone selects its code from the reason; the reverse mapping is not injective
(`FAILED_PRECONDITION` carries three public details), so clients switch on the detail type, not the
code (§4). Rejected: string-matching error messages in workflows (the Temporal non-retryable list is by
type).

### 2.5 Concurrency and resource limits per process

| Limit | Value | Where enforced | Rationale |
|---|---|---|---|
| Extraction concurrency | 32 in flight per `RetainDocument`; 32 per worker process across workflows (`gateway` per-model semaphore); one `quota.Reserve` per call | workflow semaphore + `gateway.RateLimiter` | D3 formula; more only burns the RPM cap |
| Embedding concurrency | 64 in flight per process; `EmbedBatch` ≤ 64 texts per call; prefixes `search_document: ` / `search_query: ` | `gateway.RateLimiter` | cheap and fast; the cap protects the gateway |
| Per-shard Postgres pool | 16 conns per process per shard; ≤ 32 shards per cell → ≤ 512 per process; pgbouncer `default_pool_size=24`, `max_client_conn=500` (§9.1) | `router.Options.PoolSize` | D3 cell bound |
| Recall arm parallelism | 5 arms, one pooled connection each; ≤ 50 QPS per shard target → ≤ 250 concurrent arm queries per shard | `recall.Planner` | keeps the pool below saturation |
| Visibility sets | three indexed selects per recall; marker table cap 16 k rows of one kind before the alert; degraded-mode lookup ≤ 20 ms per arm | `store.MarkerReader` | N116, N119 |
| Rerank depth | 0 / 50 / 150 pairs by budget (N53), ≤ 300 per gateway call (D15), one call per recall; skipped below a 106 ms remaining deadline (N106) | `recall.GatewayReranker` | the reranker is a sized dependency |
| Graph arm | two seeded waves × 30 ms, shared node budget 100/300/1000 | `recall.Planner` | N54 critical path |
| Vector index plan | exact scan below 2,000 vectors; partial HNSW created at 2,000 and dropped below 1,000, per namespace and model; `ef_search` = arm cap | `engramctl index`, `engram_vector_index_plan` | N112 |
| `WaitOperation` long-polls | ≤ 2,000 parked on Temporal per API process; beyond: jittered DB poll | `api.Options.MaxOpenWaits` | N70 |
| Temporal history | continue-as-new every 100 chunks or 20 MB; no inline activity result > 4 KiB | `workflows` | N59 |
| Outbox relay | 500 rows per read; one relay per shard; cursor advance ≤ 1 per second per consumer | `outbox.RelayOptions` | D6, N114 |
| Gap watchlist | ≤ 1,000 entries in `outbox_cursors.gaps`, expiring after `2 × statement_timeout` = 60 s | `outbox.GapWatch` | bounded and lossless across relay failover |
| Temporal pollers | 2 workflow + 2 activity pollers per `shard-{id}` queue; max 64 concurrent activities per worker | worker options | D3 |
| Consolidation | route 8 facts per call; ≤ 100 facts per round; one write call per touched observation; ≤ 50 root rebuilds per round; one round in flight per namespace; rebuilds only while markers are pending | `Consolidate` workflow (singleton id), `config.Consolidation` | D12, N121 |
| Expunge | one per namespace; purge in 1,000-row batches with a 50 ms pause behind the consumer-cursor check; SLAs materialize ≤ 15 min, purge ≤ 24 h, index ≤ 48 h; `ExpungeMaterializeSlow` pages at 900 s | `expunge.Expunger`, `engram_expunge_oldest_pending_seconds` | N119 |
| Reflect | ≤ 10 iterations, ≤ 100 k context tokens, ≤ 300 s, tool deadline 10 s, ≤ 4 concurrent reflects per namespace; one `Reserve` per iteration | `reflect.Caps`, api semaphore | D12, N130 |
| Move | dirty copy in key ranges of ≤ 100,000 rows, 4 parallel streams, the writer path stays live; freeze watchdog `max(120 s, 60 s + 1 s per 10 k facts)`, ≤ 15 min; ≤ 4 moves per cell, one per namespace; cutover (b″) and (d) retried indefinitely | `move.StartOptions` | N124, N125 |
| Fence acquisition | writers never wait (`pg_try_advisory_xact_lock_shared`, refusal → `NamespaceFrozen{200 ms}`); exclusive takers make one attempt with `lock_timeout` 35 s; role defaults 2 s (`engram_app`), 10 s (`engram_move`, `engram_admin`) | `store.Store.InNamespace`, role defaults | N82: no pooled connection waits behind a queued freeze |
| Derivation lock | shared by every writer of derived versions; exclusive for `Materialize` and `Restore`, one 35 s attempt | `store.Derived` | N120 |
| Per-document lock | `CommitChunk` shared try-lock (`DocumentBusy`, 100 ms); `FinalizeVersion` and the delete marker exclusive | `store.DocumentWriter.Lock` | N83 |
| Outbox event size | ≤ 256 ids and ≤ 16 KiB per event; elided above 4,096 ids | `outbox.Writer`, `outbox.Group` | N80; the CHECK is a backstop |
| Catalog resolver | LRU 100 k entries; single-flight per key; LISTEN reconnect backoff 1 s → 30 s | `catalog.ResolverOptions` | D4 |
| Query embedding LRU | 10,000 entries per process, keyed `(namespace_id, sha256("search_query: " + text))` | `recall.QueryEmbedder` | repeat queries skip the 25 ms hop |
| Streaming / request size | `Recall` batches of 10; `StreamSnapshot` 1 MiB parts; `Retain` ≤ 100 items, ≤ 8 MiB total, ≤ 1 MiB per item, raw body > 64 KiB to blob (N7) | `api` helpers | D10, D12 |

### Round-3 changes

| Register | What changed in this section |
|---|---|
| N111–N113 | `store.Tx` splits into `Read`, `Insert`, `Markers`, `Derived`, `Ops`; the `Inserts` writers have no update method; vector rows live in side tables (`AddVector`, `Fact.Insert` with vectors); `ReembedChunk` adds a row |
| N112 | `index.SemanticPlan`: exact scan below 2,000 vectors, per-namespace partial HNSW through `engramctl index`, no band, no shared partition index |
| N114, N116 | `store.MarkerReader` and the read-time visibility predicate replace the `live` and `retired_at` flags; the `Visibility` recall step loads the marker sets once |
| N115, N117, N118 | `store.Markers` (`TombstoneDocument`, `TombstoneChunks`, `Hide`, `Unhide`, `Expunge`), evidence segments (`root_version`), `derived_hidden`; `pages.Reader` and `Writer` split |
| N119, N120 | new `internal/expunge`; `Derived.TryDerivationLock` (shared) and `DerivationLockExclusive`; degraded mode in `consolidate` and `recall` |
| N121 | `consolidate` is two stages (`Router`, `Writer`) plus `Adjudicator`, `ProposalStore`, `Applier`; lineage walks and `FlagLineageDescendants` removed |
| N122 | new `internal/intent` (`BlobLog`); the catalog delete log and `remote_apply` are gone (the shard keeps its insert-only `deletion_log` keyed by the intent name); `AdminTx.Restore` replays intents |
| N123–N125 | `move` is a saga over `Copier`, `Cutover`, `Closer` with a `ready` target and rollback before (c); the copy barrier, `p0`, catch-up, `ReplayRepo` and `MoveAppliedRepo` are deleted; moves are not outbox consumers |
| N126 | `export.Builder` with a `building` state, expiry by the marker transaction, always-emitted delta |
| N127–N129 | `internal/errs` splits public and internal kinds and gains `NamespaceNotReady`; MCP idempotency `request_id` and the `/.well-known/oauth-protected-resource` check; typed `Directive` in `reflect.Request`; `Packer` keeps rank 1 whole |
| N130 | `quota.Reserver` in front of every gateway call class; `RetainBackfill` activities in `workflows` |
| N131, N132 | SECURITY DEFINER trigram lookup behind `entity.Resolver`; this section re-cut around five patterns, interfaces of at most five methods, typed ids and the walkthrough in 2.1 |
