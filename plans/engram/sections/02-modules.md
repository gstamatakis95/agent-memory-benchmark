## 2. Module breakdown: the code API

This section presents the Go API as a **small set of interfaces that compose in one obvious way**
(registers N132, N140, N157). Rules every signature follows: Go 1.25, module `example.com/engram` (D1);
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
| **Repository + Unit of Work** | `store.Store.InNamespace(ctx, ns, func(tx Tx) error)`, `store.Tx` and its repositories; reads open a `store.ReadSession` whose `Tx` is one short transaction per arm | one fenced transaction per write means the ownership check, RLS scope and outbox append cannot be forgotten; a recall is N read transactions, never one shared connection |
| **Strategy** | `recall.Arm`, `index.Searcher`, `consolidate.Router`/`Writer`, `gateway` capabilities | each swappable behaviour sits behind a 1–5 method interface chosen at wiring time |
| **Pipeline** | `pipeline.Step[S]` for the sequential stages of recall (fuse, rerank, boost, pack, stream); the ordered activity groups of retain | a request is a fixed sequence of named steps with per-step timing, deadline and skip rules; the retrieval stage in front of them is a small DAG scheduler (the arms declare `Needs()`, N54) |
| **State machine** | `fsm.Table` for namespace ownership, operations and moves | legal transitions are data (generated from SQL), so code, SQL and TLA+ share one table |
| **Adapter** | `adapters/mcp`, `adapters/connect` over the gRPC core | adapters hold a *client* to the core, so they cannot bypass the interceptor (D13) |
| **Transactional outbox** | `outbox.Writer.Append` as the last statement; `outbox.Relay` | every consumer sees changes without dual writes |
| **Saga** | `move` as a Temporal workflow with compensations (`expunge` and `TenantDelete` are forward-only workflows, not sagas) | multi-database work is a durable sequence of idempotent steps, each with a defined undo before its point of no return (the catalog CAS) |

**A Retain call, through the packages.**

1. Client → Envoy → `adapters/connect` or the gRPC server → `authz.Interceptor` verifies the JWT,
   `catalog.Resolver` returns `{tenant, shard, epoch, state}`, `quota.Limiter` takes a token, and a
   `RequestScope` lands in the context.
2. `api.MemoryServer.Retain` validates (`errs.Validation`), mints `UUIDv5(operation_id, "item/" ‖ index)`
   for items without a `document_id`, computes `api.RequestHash` and calls `api.Submitter.Submit(ctx, scope, req)`.
3. `Submit` writes a large body to `blob.Store` under the `ledger_id` minted for this attempt (owner-keyed, N104), then runs **one unit of work**:
   `store.InNamespace(ctx, ns, func(tx store.Tx) error { … })`. The store opens a transaction, sets
   the RLS GUCs, takes the shared fence try-lock and checks `namespace_ownership` is `active` at the
   epoch.
4. Inside: `tx.Ops().Idempotency().Begin`, `Ops().Ledger().Append`, `Write().Insert().Documents().BeginVersion`,
   `Ops().Operations().Insert`, then `Ops().Outbox().Append(DocumentVersionStarted)` as the last
   statement. Commit; the ack is now durable.
5. `workflows.Starter.StartRetain` starts `ns/{ns}/op/{op}` on task queue `shard-N`; the handler returns
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
   `kafka` sinks; `FinalizeVersion` inserts tombstones and flags the affected observations stale
   (evidence rows are kept, N135); a `Nudge` wakes `consolidate`.
10. The client calls `WaitOperation`; `api.OperationWaiter` parks on `workflows.Waiter.WaitResult`.

**A Recall call, through the packages.**

1. Same edge as above → `api.MemoryServer.Recall` (server stream) → `store.ReadNamespace` opens a
   **`ReadSession`** (no transaction yet, no lock) and `recall.Planner.Recall(ctx, q, sink)` runs on it.
   **A recall is N short read transactions, not one unit of work (N140):** every arm that runs
   concurrently calls `Session.Tx`, which opens its own pooled connection, sets the scope GUCs, runs the
   read fence (`active` or `frozen/move`) and the arm's `SET LOCAL`s (`enable_seqscan = off`, no parallel
   workers, `hnsw.ef_search`, N138). One connection runs one statement at a time, so a shared
   transaction would serialise the arms and the 236 ms path could not hold; a semaphore admits at most 32 concurrent arm transactions per API process per shard, 2 of them filtered arms (N155, N164, N176).
2. The planner is a `pipeline.Pipeline[*recall.State]` whose first stage is a DAG scheduler and whose
   remaining stages run in order; each records a `StageTiming`:
3. `Retrieve` — a DAG over named nodes. `visibility` loads, once, the two marker sets
   (`doc_tomb`: document → `up_to_version`, and `chunk_tomb`; two indexed selects, `fact_hidden` is tested
   per candidate), resolves the tag and metadata filters into an allowed-document set (N116, N139) and
   estimates the eligible rows for the semantic plan (N138, N151); `embed` calls
   `gateway.Embedder.Embed("search_query: " + text)` (LRU hit skips it). These are **values** computed once
   and passed into every arm.
4. The arms are the other nodes (Strategy): lexical and temporal as soon as `visibility` is done, semantic
   and chunks when `embed` is done too, graph in two seeded waves; each arm opens its own `Session.Tx`
   and calls `index.Searcher` or a repository with the marker sets, the allowed documents, the plan
   and `as_of` in its predicate, under its own deadline.
5. `Fuse` — `recall.Fuser` (RRF, k = 60, exact rationals). `Rerank` — `gateway.Reranker` on the top
   0/50/150, skipped below the 106 ms reserve. `Boost`, `Pack` (rank 1 always whole).
6. `Stream` — batches of 10 to the gRPC stream, then `RecallStats` with the stage timings.
7. Errors travel as `errs.Error` and are rendered once by `errs.ToStatus` / `errs.ToConnect`; a
   `WrongShardOrEpoch{MOVED_OUT}` is caught by `router.Retry`, which follows the internal hint to the
   new owner.

#### Package layout (D14) and dependency rule

```
cmd/
  engram-api/     engram-worker/     engram-mcp/     engramctl/        composition roots
proto/            memory/v1  memory/admin/v1  engram/internal/{errors,workflow,events}/v1
gen/go/...        buf generate output (committed)
internal/
    id/             typed ids and Scope (leaf; N132; github.com/google/uuid is its one allowed dependency)
  errs/           typed errors + gRPC/Connect/Temporal mapping (leaf; N1)
  pipeline/       generic ordered Step[S] runner (leaf; N132)
  fsm/            transition tables for ownership, operations, moves (leaf; N132)
  txn/            the capability handle of the caller's open transaction, shared by quota and store (leaf; N133a)
  api/            gRPC service implementations, deadline + request_id enforcement, retain.Submitter
  authz/ catalog/ router/ config/ quota/ telemetry/
  gateway/ blob/ intent/ store/ index/ outbox/ ledger/
  chunk/ extract/ entity/ link/ recall/ consolidate/ reflectagent/ pages/ export/
  expunge/ move/ workflows/
adapters/         mcp/  connect/
formal/           tla/*.tla  lean/Engram/*.lean
```

`internal/id`, `internal/pipeline`, `internal/fsm` and `internal/txn` are four small leaf packages added
to D14 by N132 and N133a (they contain no I/O; a leaf imports only the standard library, the leaf-permitted
modules named in the table below, and other leaves). `internal/intent` and `internal/expunge` are the
D22 packages. `internal/reflect` is `internal/reflectagent` (N140): the old name shadowed the standard
`reflect` package.

**Dependency rule** (enforced by `go vet` + a `depguard` config in CI):

| Layer | May import | Must not import |
|---|---|---|
| leaves (`id`, `errs`, `pipeline`, `fsm`, `txn`) | the standard library, other leaves, `gen/go`, and the **leaf-permitted modules** `github.com/google/uuid` (`id`), `go.opentelemetry.io/otel` (`pipeline`: its span), `google.golang.org/grpc/status`, `connectrpc.com/connect` and `google.golang.org/protobuf` (`errs`) | any non-leaf `internal/` package |
| `internal/api` | `authz, catalog, router, recall, reflectagent, pages, export, move, quota, config, store, blob, workflows (clients only), intent, telemetry`, the leaves, `gen/go` | adapters, `cmd` |
| Services (`recall, consolidate, reflectagent, pages, export, move, expunge, entity, link, extract, chunk`) | `store, index, gateway, blob, intent, config, quota, ledger, telemetry`, the leaves, `gen/go`; `reflectagent` also `recall`, `pages`; `pages` also `recall` (a page refresh gathers its evidence with `recall.Planner`; no cycle, `recall` imports neither `pages` nor a service); `move` also `catalog` | `internal/api`, adapters, `workflows` |
| Infrastructure (`store, index, gateway, blob, intent, catalog, router, outbox, ledger, config, quota, telemetry`) | leaves, `gen/go`, drivers, and only the edges named below | any service package, `internal/api` |
| `internal/workflows` | activity **interfaces** declared in itself, the leaves, `gen/go/engram/internal/workflow/v1`, the Temporal SDK | concrete service packages (injected in `cmd/engram-worker`) |
| `adapters/*` | `gen/go` (+ generated clients), `authz` (scope names), the leaf `errs` (code mapping) | any other `internal/*` |
| `cmd/*` | everything | — |

**The import graph is acyclic by construction (N140).** The infrastructure edges are exactly:
`store → {id, errs, fsm, txn}`; `index → store`; `outbox → store` (the outbox *writer* is a `store`
interface, so `store` never imports `outbox`); `quota → {txn, gateway}`; `catalog → {fsm, config}`;
`router → {store, index, blob, catalog, telemetry}` and it takes an `id.Scope`, never `authz.RequestScope`;
`blob → store` (`Tombstoner` takes a `store.Tx`; `store` never imports `blob`);
`authz → {catalog, quota, config}`. **Services take `id.Scope` plus an `id.Caller` value and a
`store.Store`, never `authz.RequestScope` or `*router.ShardHandle`** (N157, A-8): the API unpacks the
handle and the request scope at the edge. The old cycles (`store → outbox → router → store` and
`router → authz → quota → store → outbox → router`) are gone. `go vet` against a stub module of the §2
signatures is an M0.1 exit: it catches an import cycle before any body exists, and the same vet runs the
**method-count lint** (every `type X interface { … }` block of §2, one-line ones included, parsed and counted ≤ 5) and the
generated (RPC → interface method) coverage check (N157, A-7).

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

type NamespaceID uuid.UUID  // UUIDv7. FactID, ChunkID, EntityID, ObservationID, PageID, LedgerID, OperationID and
type FactID uuid.UUID       // MoveID are defined the same way; each has String(), Parse<T>(string) and Bytes() (the
type ChunkID uuid.UUID      // 16-byte form is what events carry, N80).

// Versions are one type per entity (N140): a DocVersion cannot be passed where an ObsVersion is wanted.
type DocVersion int64; type ObsVersion int64; type PageVersion int64; type SnapshotVersion int64

// Subject is the typed curation/delete subject (N162): Class ∈ {document, memory, namespace, tenant}; Key is the
// document_id, `document_id ‖ ':' ‖ content_hash` for memory (a fact and its twins are ONE subject), the namespace id or
// the tenant id. Encoded `class:key` in subject locks, deletion_log and intents, so a client-chosen document_id cannot alias a fact's subject.
type SubjectClass uint8
type Subject struct { Class SubjectClass; Key string }

type CellID string          // a cell (Temporal cluster + its shards)
type WorkflowID string      // "ns/{ns}/op/{op}" etc.; built only by internal/workflows

// MemoryRef is the typed union of the three things a recall result can be; constructors FactRef, ObservationRef,
// ChunkRef. It replaces untyped uuid.UUID in index.Hit, recall.Candidate and the citation verifier.
type MemoryKind uint8       // KindFact | KindObservation | KindChunk
type MemoryRef struct { Kind MemoryKind; ID uuid.UUID }

// Scope is the fencing token that travels with every request and every workflow input.
type Scope struct { Tenant TenantID; Namespace NamespaceID; Shard ShardID; Epoch Epoch }

// Caller is the small value services take with a Scope instead of authz.RequestScope (N157, A-8).
type Caller struct { Subject string; RequestID string }
```

`authz.RequestScope` is `id.Scope` plus the `id.Caller`, the caller's `Scopes` and the resolved
`config.Resolved`; the store only ever sees an `id.Scope`, services an `id.Scope` and an `id.Caller`.

**Pipeline and state machine, the two generic leaves** (no I/O, a few dozen lines each):

```go
package pipeline

// Step is one named stage of a request. Run mutates the shared state S; returning ErrSkip records the stage as
// skipped (deadline, disabled, empty window) and continues; any other error stops the pipeline.
type Step[S any] interface {
	Name() string
	Run(ctx context.Context, s S) error
}
// Run executes the steps in order, opening one OpenTelemetry span (go.opentelemetry.io/otel, a leaf-permitted module) and one StageTiming per step.
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

```go
package txn // leaf (N133a, N140): what quota and outbox-adjacent code may do inside the caller's transaction

// Tx is implemented by store.Tx. quota.Meter takes this, never store.Tx, so quota does not import store.
type Tx interface {
	Scope() id.Scope
	Usage() Usage
}
type Usage interface {
	Record(ctx context.Context, key UsageKey, e UsageEvent) (inserted bool, err error) // exactly-once through usage_key (PD-1)
	Day(ctx context.Context, day time.Time) (tokens, facts int64, err error)           // rollup read for quota.Meter.Remaining
}
```

`fsm.OwnershipState` is `incoming | ready | active | frozen/{move,delete,restore} | moved_out`;
`fsm.OperationState` is `PENDING | RUNNING | DEFERRED | SUCCEEDED | FAILED | CANCELLED` (N35, monotone,
no `FAILED → PENDING` edge: a retry is a new operation, N139);
`fsm.MoveState` is `planned | frozen | copied | cutover | committed | cleaning | done | rolled_back`
(`committed` is the catalog CAS, N125; N160).

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
	MaxDeadline map[string]time.Duration // full method → cap (N11); over the cap is INVALID_ARGUMENT, never clamped; delete-class RPCs and Restore 40 s (N139)
	MaxPageSize int32                    // 1000 (200 for ListMemories with text in the mask)
	RequestIDTTL time.Duration           // 24 h (D1)
	MaxOpenWaits int                     // 2 000 WaitOperation long-polls per process (N70)
	ReflectionServices []string          // ["memory.v1"] only, never the admin surface (N71)
}
type Deps struct { // every field is an interface (2.3); complete for every served RPC (N140)
	Router router.ShardRouter; Stores store.Stores          // one store.Store per shard of the cell: a process serves up to 32 shards
	Catalog   catalog.Namespaces; NsAdmin catalog.NamespaceAdmin; Registry catalog.Registry; Tenants catalog.Tenants; TenantOps catalog.TenantOps
	Moves     catalog.Moves; MoveReader catalog.MoveReader; Floor catalog.ReplayFloor // NamespaceService (List/Update via NsAdmin), TenantService (UpdateTenant and UpdateTenantLimits both via Tenants.Update, one scope each), ShardService, MoveService
	Mover     move.Orchestrator                               // MoveService.StartMove/RollbackMove/CleanupMove = Start/Abort/Cleanup (api may import move, N157, N167)
	Config    config.Resolver                                // GetEffectiveConfig
	Recall    recall.Planner; Reflect reflectagent.Agent
	Retain    Submitter; Deleter Deleter; Ops OperationWaiter
	Start     workflows.Starter; Signal workflows.Signaller; Wait workflows.Waiter
	Pages     pages.Reader; PageWriter pages.Writer; PageAdmin pages.Admin // PageService: Create/Delete/Refresh, UpdatePage via PageAdmin, GetPage(name) via Reader.ByName
	Snapshots export.Builder; Stream export.Streamer; SnapList export.SnapshotLister // ExportService: CreateSnapshot, Stream/Latest, GetSnapshotManifest(version) via SnapList.Manifest, ListSnapshots
	Intents   intent.Log; Quota quota.Limiter; Clock func() time.Time
}
// The generated coverage table (`api/deps_coverage_gen.go`, produced from the proto service list by `make gen-docs`) is
// (RPC → interface method), not (RPC → field): a field such as Stores satisfies every per-shard RPC trivially, so only the
// method level can show that ListOperations, ListMemories, GetDocumentBody, ListTags, UpdateDocumentTags, UpdatePage,
// ListSnapshots, ListNamespaces/UpdateNamespace, StartMove and CleanupMove have an implementation behind them (N157, A-6, N167). NewServer
// panics at wiring time if a row names a method no non-nil dependency provides; TestDeps_EveryRPCHasPath asserts the same
// on every CI run (and that every non-derived `namespace_moves` column has exactly one writer, N184), so a new RPC cannot ship without the method that implements it.
func NewServer(d Deps, o Options) *Server
func (s *Server) RegisterGRPC(g *grpc.Server)
func (s *Server) RegisterConnect(mux *http.ServeMux, ic ...connect.Interceptor)

// Submitter is the API half of retain (D16): durable ack, then workflow start (§5.1.1).
type Submitter interface { Submit(ctx context.Context, sc authz.RequestScope, req *memoryv1.RetainRequest) (*memoryv1.Operation, error) }

// Deleter is the synchronous half of every delete: ONE marker transaction, then the intent object (put after the
// commit, before the ack), then the ack (N115, N122). A duplicate attempt that finds the subject already deleted
// returns the existing operation and first ensures the intent of the marker it found (put-if-absent). The ack is
// conditional (N143): after the intent put the handler RE-READS the marker (a plain read of the tombstone or
// fact_hidden row and its deletion_log row) and acks only if it is still present; if a restore or failover removed
// it in between, the request returns UNAVAILABLE and the client retries (TestIntent_AckRereadsMarker).
// N159: every method takes the SUBJECT lock (SubjectLocker, session level, direct connection) BEFORE its marker
// transaction and releases it only AFTER the intent put and that re-read, on every exit path; under the lock it first
// runs help-previous (intent.HelpPrevious: put the intent of the subject's latest deletion_log entry if absent), so a
// crash between a predecessor's commit and put never leaves a hole in the prev_operation_id chain
// (Durability_NoHelpPrev, TestIntent_HelpPrev).
type Deleter interface {
	DeleteDocument(ctx context.Context, sc authz.RequestScope, doc id.DocumentID, o DeleteOptions) (*memoryv1.DeleteDocumentResponse, error) // DeleteOptions{OperationID, ExpectedVersion id.DocVersion (compared inside the marker tx)}
	DeleteNamespace(ctx context.Context, sc authz.RequestScope, op id.OperationID, confirmName string) (*memoryv1.DeleteNamespaceResponse, error) // keeps the client's operation_id
	DeleteTenant(ctx context.Context, sc authz.RequestScope, op id.OperationID, confirm id.TenantID) (*adminv1.DeleteTenantResponse, error)       // returns the PENDING operation after the replicated catalog row and the tenant intent; TenantDelete fences every namespace, then stamps acknowledged_at (N182)
	Invalidate(ctx context.Context, sc authz.RequestScope, fact id.FactID, reason string) (*memoryv1.Memory, error)                               // subject lock → exclusive document lock (CommitChunk holds it shared) held until the intent put + help-previous (N150, N159, N162, N174); hides every live fact of the subject under its one invalidation_op (reused when invalidate rows exist; recorded on deletion_log and the intent); a second Invalidate changes no visibility but writes its own row and intent (N143)
	Restore(ctx context.Context, sc authz.RequestScope, fact id.FactID) (*memoryv1.Memory, error)                                                // subject lock → exclusive document lock → exclusive derivation lock (N174): 40 s deadline cap; removes the rows with the invalidation's stamp (N162); a purged fact resolves its subject from curation_log (N174); a repeated Restore writes its own row and intent (N143)
}

// OperationWaiter implements WaitOperation: park on Temporal's workflow-result long-poll (≤ MaxOpenWaits per
// process), else poll the operations row every 1 s ± 250 ms. DELETE_DOCUMENT polls the tombstone (its workflow is the
// expunge singleton), REFRESH_PAGE polls the page row (singleton-backed, N157), DELETE_NAMESPACE and DELETE_TENANT are served from the catalog, derived from `namespaces.state`
// / the `tenants` row (N70, N133d, N136). CancelOperation on a DELETE_* operation is OPERATION_NOT_CANCELLABLE.
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

type Scope string // "memory.read" | "memory.write" | "memory.admin" | "tenant.admin" (tenant-bound: the tenant's own lifecycle, tenant-facing listener) | "engram.operator" (fleet-wide, own audience; CreateTenant, UpdateTenantLimits, and GetTenant/DeleteTenant/GetTenantOperation on the admin route; N139, N157, N167) | "engram.worker" (the worker's service identity: valid on exactly ReleaseNamespace and CleanupMove, bound to its cell, N167)

type Claims struct { Subject string; Tenant id.TenantID; Namespaces []id.NamespaceID /* or ["*"] */; Groups []string; Scopes []Scope; ExpiresAt time.Time }

type TokenVerifier interface { Verify(ctx context.Context, raw string) (*Claims, error) } // signature, expiry, issuer, audience; no catalog

type RequestScope struct { id.Scope; Caller id.Caller; Scopes []Scope; Config *config.Resolved }

type MethodPolicy struct {
	Scope Scope; Target Target /* Namespace | Tenant (token.tenant == request.tenant enforced) | Operator */; Bucket quota.Bucket
	AltScope Scope // a second scope accepted on the admin route only: engram.operator for GetTenant, DeleteTenant, GetTenantOperation (it carries no tenant, so the Tenant target check is skipped for it)
	CellBound bool // engram.worker: the token's `cell` claim must equal the shard's cell (ReleaseNamespace, CleanupMove)
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
leaves the process (N128). The per-path split of `UpdateTenant` is gone: `UpdateTenant` (display name, config) is `tenant.admin`, `UpdateTenantLimits` (quotas, isolation, state) is `engram.operator`, so every method has one policy row and the interceptor stays the only enforcement point (A-6). *Test seam:* table tests over
`(claims, method, catalog entry) → code` (the §8 policy table covers the worker scope, the cell claim and the operator's tenant methods), and the cross-tenant matrix of §8 on gRPC, Connect and MCP.

#### 2.2.3 `internal/catalog` — control plane and resolver cache

Owns the `engram_catalog` tables (D4) and the in-process `Resolver` (LRU 100 k, TTL 60 s, negative
5 s, `LISTEN catalog_changes`; existing namespaces are served indefinitely while the catalog is
unreachable, only misses fail `UNAVAILABLE` — F-21). The catalog database has **one asynchronous hot
standby** (N163): everything it arbitrates is also derivable from the shards' ownership rows, so **every promotion and
every restore runs `engramctl catalog reconcile --from-shards` before the catalog serves writes**, and the one step that
acts irreversibly on a catalog outcome (the move's (c), thaw, rollback) first waits for the standby to replay it and re-reads the row (N171): routing, moves and shard-side deletes are re-derived from the shards, every other acknowledged catalog write waits for replay (`catalog.AckAfterReplay`). *Pattern: Repository for the control plane,
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
type NamespaceAdmin interface { // NamespaceService.ListNamespaces / UpdateNamespace
	List(ctx context.Context, t id.TenantID, q NamespaceQuery) ([]Entry, string, error)
	Update(ctx context.Context, ns id.NamespaceID, p NamespacePatch, etag string) (*Entry, error)
}
type Moves interface { // the move ledger: every transition is a CAS on the current state (five methods; the rest is MoveStamps, Replication)
	Plan(ctx context.Context, ns id.NamespaceID, target id.ShardID, p PlanParams) (*MoveRow, error) // source_system_id, source_timeline_id, w_est_seconds, window_seconds recorded; refused unless window ≥ max(1.5 × w_est, w_est + 10 min), cap 8 h, or an unattended move exceeds 10 min (PreconditionFailed{MOVE_WINDOW_REQUIRED | MOVE_WINDOW_TOO_SHORT}, N173)
	Advance(ctx context.Context, m id.MoveID, from, to fsm.MoveState) error              // stamps frozen_at, freeze_deadline = frozen_at + max(1.5 × w_est, w_est + 10 min) capped by window_seconds, and w_final on planned → frozen
	Commit(ctx context.Context, p CommitParams) error                                   // (a″) CAS cutover → committed WHERE move_id, state = 'cutover' AND the verified source/target shards, epochs, timeline AND the catalog namespace row (source, e, frozen) (N143): THE point of no return; ErrLost if a rollback or restore won the row (N125) or any verified value changed
	
	Cutover(ctx context.Context, p CutoverParams) error                                 // step (d) only: shard, epoch, state, NOTIFY; success also when already (target, e + 1)
	Rollback(ctx context.Context, m id.MoveID, reason string) error                     // CAS cutover → rolled_back (or any earlier state); ErrCommitted if (a″) won
}
type MoveStamps interface { // every non-derived `namespace_moves` column has exactly one writer (N184)
	Stamp(ctx context.Context, m id.MoveID, k StampKind, at time.Time) error // k ∈ {ready, moved_out, activated, finished}: a same-state update (`activated` is the 24 h gate's input, N170, N180)
	RecordFloor(ctx context.Context, m id.MoveID, lsn pg.LSN, timeline int32, sealedAt time.Time) error // copy_end_lsn, copy_end_timeline, copy_sealed_at: what the `cutover → …` CHECK reads (N179)
	RecordReplicated(ctx context.Context, m id.MoveID, o Outcome, at time.Time) error // o ∈ {committed, rolled_back}: committed_replicated_at / rolled_back_replicated_at (N171)
}
type Replication interface { // catalog_replicated(lsn) and the commit LSN (N171(1))
	Replicated(ctx context.Context, lsn LSN) (bool, error) // a streaming standby has replay_lsn ≥ lsn
	CommitLSN(ctx context.Context) (LSN, error)            // pg_current_wal_lsn() after COMMIT, same session
}
// catalog.AckAfterReplay decorates the catalog write path (N171(3)): CreateTenant, UpdateTenant, UpdateTenantLimits, DeleteTenant,
// CreateNamespace, UpdateNamespace, DeleteNamespace's `deleting` row, StartMove, CleanupMove, RollbackMove and the idempotency_keys row each writes
// ack only after Replicated(CommitLSN); on timeout UNAVAILABLE{RetryInfo 2 s}, the retry idempotent by key. Step-writes do not wait.
type Reconciler interface { // engramctl catalog reconcile --from-shards: the first step of every catalog promotion and restore (N163)
	FromShards(ctx context.Context, rows []OwnershipObservation) (*ReconcileReport, error) // the shards' ownership rows are the source of truth (a moved_out source with a hint ⇒ the move is at least committed; no target row and no moved_out source ⇒ rolled_back); raises each epoch to the max over its `active`/`frozen/*` rows (`frozen/restore` included; the higher epoch wins a torn snapshot) and a `moved_out` row's target_epoch (N172, N180); a re-derived `committed` takes its floor from the target row's `floor_lsn`, stamps `reconciled_at` and writes routing only, never activating a shard row (N184); a shard unreadable within 5 s keeps the catalog's rows and pages `ReconcileIncomplete` (N185)
}
type MoveReader interface { // MoveService.GetMove / ListMoves
	Get(ctx context.Context, m id.MoveID) (*MoveRow, error)
	List(ctx context.Context, q MoveQuery) ([]MoveRow, string, error)
}
type Registry interface { // shards (admin surface, §9)
	RegisterShard(ctx context.Context, s Shard) error
	GetShard(ctx context.Context, s id.ShardID) (*Shard, error)
	ListShards(ctx context.Context, cell id.CellID) ([]Shard, error)
	SetShardState(ctx context.Context, s id.ShardID, to ShardState) error
	UpdateShard(ctx context.Context, s id.ShardID, p ShardPatch) error
}
type Tenants interface { // tenants (admin surface)
	Create(ctx context.Context, t TenantEntry) error
	Get(ctx context.Context, t id.TenantID) (*TenantEntry, error)
	List(ctx context.Context, q TenantQuery) ([]TenantEntry, string, error)
	Update(ctx context.Context, t id.TenantID, p TenantPatch) (*TenantEntry, error)
	BeginDelete(ctx context.Context, t id.TenantID, op id.OperationID) error            // tenants.state = 'deleting' + delete_operation_id: the TENANT_DELETING read barrier (N122)
}
type TenantOps interface { // DELETE_TENANT lives in the catalog, derived from tenants.state / deleted_at (N127, N133d)
	Operation(ctx context.Context, t id.TenantID, op id.OperationID) (*memoryv1.Operation, []*memoryv1.Operation /* per-namespace DELETE_NAMESPACE created so far */, error)
}
type ReplayFloor interface { // catalog.shards.replay_floor: outside the restorable shard state (N134, N163(4)); the effective floor is min(this, the _control/restores/ markers)
	Get(ctx context.Context, s id.ShardID) (time.Time, error) // the catalog value; a replay takes min(this, intent.Log.RestoreFloor) so a catalog that lost the lowered floor cannot raise it
	Lower(ctx context.Context, s id.ShardID, restoreTarget time.Time) (time.Time, error) // min(); there is NO method that raises it while intents are retained (35 days)
}
type Resolver interface {
	Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error)      // never blocks on the catalog with a fresh entry
	ResolveFresh(ctx context.Context, ns id.NamespaceID) (*Entry, error) // bypass the cache (after WrongShardOrEpoch)
	Invalidate(ns id.NamespaceID)
	Run(ctx context.Context) error // LISTEN loop; full flush on reconnect
}
```

The catalog has no delete log: acknowledged deletes live in the intent log (N122); it does hold the
replay floor, which restore and failover only lower (N134). `Entry`
carries `embedding_model`/`embedding_dims` (`namespaces` columns) so no worker reads config for
the vector space. `ResolverOptions` has `MaxEntries` 100 000, `TTL`, `NegativeTTL` and `StaleMax`
(0 = unbounded). *Test seam:* a rapid
state-machine test (plus `TestCatalog_PromotionRunsReconcile`, `TestCatalog_ReconcileDerivesRouting`, `TestMove_CutWaitsForReplicatedCommit`) drives random `Plan/Advance/Commit/Cutover/Rollback` sequences against the move table
(no `cutover` without `frozen`; `Commit` and `Rollback` race on one row and exactly one wins; at most one
active shard per namespace).

#### 2.2.4 `internal/router` — scope → shard handle

Maps a scope (or a workflow input) to the resources of its shard, built at process start for the
cell's shard list. It takes the plain `id.Scope`, never `authz.RequestScope`, so `router` does not import
`authz` (N140); `store.Stores` (the per-shard `store.Store`s the API's `Deps` needs) is built from the same list. *Pattern: Registry of per-shard handles; `Retry` is the one place that encodes
the move/freeze retry contract.*

```go
package router

type ShardHandle struct { Shard id.ShardID; Cell id.CellID; Store store.Store; Blob blob.Store; Index index.Searcher; TaskQueue string; Metrics telemetry.ShardLabels } // router imports telemetry (listed edge, N157)

type ShardRouter interface {
		For(ctx context.Context, sc id.Scope) (*ShardHandle, error)            // local handle or errs.ShardNotLocal(cell)
	ForShard(s id.ShardID) (*ShardHandle, bool)                              // workers carry shard_id in inputs (D4)
	Forward(ctx context.Context, cell id.CellID, method string, req, resp proto.Message) error // cross-cell hop (phase 3), at most one (N17)
	Local() []id.ShardID
}

// Retry runs fn under the retry contract: one re-resolve on WrongShardOrEpoch for writes; on MOVED_OUT it first
// follows the internal MovedOutHint to the new owner (no catalog read, N93); calls that meet MOVED_OUT,
// NamespaceNotReady or NamespaceFrozen re-resolve in a bounded loop (≤ 5 s, jittered 50 → 500 ms; writes on
// NamespaceFrozen up to 30 s). Only handlers that are safe to re-execute call it (all writes are idempotent).
func Retry(ctx context.Context, r catalog.Resolver, sr ShardRouter, sc id.Scope, fn func(ctx context.Context, h *ShardHandle, sc id.Scope) error) error
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

*Pattern: Decorator — `Scoped` wraps any `Store` and rejects keys outside a shard prefix.* Two key families
(N104): **content-addressed caches** (`xcache`, `ecache`, `docsum`, `staging`: a loss is a recompute) and
**owner-keyed blobs** (`ledger/{ledger_id}`, `ver/{document_id}/v{n}`, page markdown, export parts), each with
exactly one owner row that deletes it in the purge, so no reference check or adoption race exists.

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
func ContentKey(p Prefix, k Kind, sum [32]byte, ext string) string // "{prefix}{kind}/{hex sha256}[.ext]"; Kind: xcache | ecache | docsum | staging (content-addressed, 24 h grace on delete)
func LedgerKey(p Prefix, l id.LedgerID) string                       // "{prefix}ledger/{ledger_id}": owner = the ingest_ledger row, minted per attempt before the put (N7)
func VersionBodyKey(p Prefix, d id.DocumentID, v id.DocVersion) string // "{prefix}ver/{document_id}/v{n}": owner = the document_versions row (N104)

// Tombstoner makes purges resumable: a blob_tombstones row is inserted first, the object deleted, then the row.
type Tombstoner interface {
	MarkDeleted(ctx context.Context, tx store.Tx, key, reason string) error
	PurgeMarked(ctx context.Context, tx store.Tx, limit int) (int, error)
}
```

The expunge deletes `xcache`/`ecache`/`staging` objects only when no visible chunk references the
hash **and** the object is older than `xcache_grace = 24 h` (`Head` supplies `LastModified`; N100); an
owner-keyed object is deleted together with its owner row. A move's blob check is one key per copied row.
Backups live under `_backups/shard-{id}/`, outside every tenant prefix (N23).

#### 2.2.7 `internal/store` — per-shard Postgres, fencing, repositories

Everything that touches a shard database: pools, the fenced transaction, one repository per table
family. No SQL exists outside this package and `internal/index`. *Pattern: **Repository + Unit of
Work.** `InNamespace` is the unit of work; the repositories reachable from its `Tx` are the only way to
write, so the fence prelude, RLS scope and outbox append cannot be skipped.*

```go
package store

// Store is the only entry point to a shard. Reads take no lock; writes run the fence prelude (N82). Five methods.
type Store interface {
	// InNamespace runs fn in one WRITE transaction: SET LOCAL engram.namespace_id/tenant_id/epoch and
	// statement_timeout (30 s = idle_in_transaction_session_timeout, A-F1), engram_try_ns_fence (shared
	// try-lock, never waits: a refusal is NamespaceFrozen{200 ms}), then a plain SELECT state, epoch FROM
	// namespace_ownership; abort with WrongShardOrEpoch unless state = 'active' AND epoch = ns.Epoch.
	InNamespace(ctx context.Context, ns id.Scope, fn func(tx Tx) error) error
	// ReadNamespace opens a ReadSession for a request: no transaction yet, no lock. Each Session.Tx is one
	// short READ transaction (below); a recall is N of them, not one unit of work (N140).
	ReadNamespace(ctx context.Context, ns id.Scope, fn func(s ReadSession) error) error
	// Admin runs fn as engram_admin (BYPASSRLS, 30 s timeouts): sweepers that enumerate namespaces, the
	// expunge's purge, the restore replay.
	Admin(ctx context.Context, fn func(tx AdminTx) error) error
	// Direct returns a dedicated, non-pooled connection as engram_relay for the outbox relay's advisory-lock
	// election and ordered outbox reads (N4, N15); outbox.Relay takes a Store and calls this.
	Direct(ctx context.Context) (DirectConn, error)
	Close() error
}
type DirectConn interface {
	TryLock(ctx context.Context, key int64) (bool, error)                      // session-level pg_try_advisory_lock; lost when the connection drops
	ReadOutbox(ctx context.Context, after int64, limit int) ([]OutboxRow, error) // seq order, every namespace
	Cursors() CursorRepo                                                         // outbox_cursors: guarded UPDATE, gap watchlist
	Close() error
}
// SubjectLocker (N150, N159, N162) is the second interface the Store implementation satisfies (Store itself stays at five methods).
// Lock takes the SESSION-level subject lock pg_advisory_lock(engram_subject_lock_key(ns, subject)) on a dedicated direct
// connection of the `engram_subject` alias (2 per API process per shard, a role holding only pg_advisory_lock on the subject
// key space; 63 = 55 + 2P of max_connections 100 at P = 4, §9.1). The lease is taken with pg_try_advisory_lock polled every 50 ms
// for at most 3 s, so one blocked lease never holds a slot; pool exhaustion is an immediate retryable UNAVAILABLE. The marker
// writer holds the lease from before its marker transaction until the intent put and the marker re-read are done; Release is
// called on every exit path and a crashed handler's lease dies with its connection (tcp_user_timeout 30 s). The lease is outside
// the pgbouncer pool, because session locks do not survive transaction pooling.
type SubjectLocker interface {
	Lock(ctx context.Context, ns id.NamespaceID, subject id.Subject) (SubjectLease, error)
}
type SubjectLease interface {
	Latest(ctx context.Context) (DeletionRecord, bool, error) // the subject's last deletion_log entry of either kind, read while the lease is held (help-previous input)
	Release()
}
// Stores is the per-shard directory the API's Deps needs: a process serves up to 32 shards (D3).
type Stores interface { For(s id.ShardID) (Store, bool); Local() []id.ShardID }

// ReadSession is the scope of one request on one shard. It holds no connection; every Tx opens its own, so arms
// that run concurrently (N54) never share one (a pgx connection executes one statement at a time). A semaphore shared
// by the process admits at most 32 concurrent arm transactions per shard (N155, N164: the `engram_app` pgbouncer recall pool
// is 32 and API-only, the worker has its own `engram_worker` pool of 8), and a second semaphore of 2 inside it admits the
// filtered arms (E ≥ θ) (≤ 8 of 32 at P = 4); TestReadSession_ConcurrentArms asserts both bounds and records ≈ 425
// connection-ms per MID recall, alerting above 1.25×.
type ReadSession interface {
	Scope() id.Scope
	// Tx runs fn in ONE short READ transaction on its own pooled connection: SET LOCAL scope GUCs, the read fence
	// (SELECT state FROM namespace_ownership: 'active' and 'frozen/move' are accepted, epoch ignored; 'frozen/delete',
	// 'frozen/restore', 'incoming', 'ready' and 'moved_out' are refused, a moved_out row also returns the internal
	// MovedOutHint), then o.SetLocal (enable_seqscan = off, max_parallel_workers_per_gather = 0, hnsw.ef_search, an
	// arm statement_timeout; N138). Marker sets, $allowed_docs and the plan choice are values passed in, not re-read.
	Tx(ctx context.Context, o TxOptions, fn func(tx ReadTx) error) error
}

// Tx groups the repositories by role; every accessor returns an interface of at most five methods. It does not embed
// txn.Tx: Txn() hands out the leaf capability handle, so quota.Meter can record usage inside the caller's
// transaction without importing store (N157, A-7).
type Tx interface {
	Txn() txn.Tx        // Scope() and Usage(): what quota.Meter takes
	Read() ReadTx       // the read side, also usable inside a write transaction
	Write() Writes      // insert-only content, markers, document tags
	Derived() Derived   // observations, pages, consolidation bookkeeping, derivation lock (N117, N120)
	Ops() Ops           // operations, idempotency keys, ledger, outbox
}
type Writes interface {
	Insert() Inserts        // insert-only content writers (N113): no method updates a content row
	Markers() Markers       // tombstones, fact_hidden, curation_log, derived_hidden, expunge progress (N115, N119)
	Tags() DocumentTags     // the one mutable document edit: tags, tag_generation, tag_counts, in one transaction (N157)
}
// ReadTx is what a ReadSession arm and a write transaction's Read() see. It is split to stay at four accessors plus
// the derived readers: GetMemory and GetPage of an observation or page version use Derived() and so are served during
// a move freeze, not through a fenced write transaction (N157, A-9).
type ReadTx interface {
	Content() ContentReader      // documents, chunks, facts, graph, tags
	Visible() MarkerReader       // the marker sets and curation state behind the visibility predicate
	Derived() DerivedReader      // Observations().Served, Pages().Served
	Operations() OperationReader // OperationService.GetOperation / ListOperations
}
type ContentReader interface {
	Documents() DocumentReader; Chunks() ChunkReader; Facts() FactReader
	Graph() GraphReader          // links and entities
	Tags() TagLister             // ListTags, from the tag_counts table
}
type DerivedReader interface { Observations() ObservationReader; Pages() PageReader }
type ObservationReader interface { Served(ctx context.Context, os []id.ObservationID, asOf time.Time) ([]ObservationVersion, error) } // the version CURRENT at asOf if visible, nothing if hidden; zero visible sources are not served; fail closed (N117, N135)
type PageReader interface { Served(ctx context.Context, ps []id.PageID, asOf time.Time) ([]PageVersion, error) }
type OperationReader interface {
	Get(ctx context.Context, op id.OperationID) (*OperationRow, error)
	List(ctx context.Context, q OperationQuery) ([]OperationRow, string, error)
}
type AdminTx interface {
	Ownership() OwnershipRepo  // Get, Transition(edge, …) generated from §3.3.1, FenceExclusive (one 35 s attempt)
	Sweep() Sweepers           // namespaces with pending facts, stale observations, pending tombstones, deferred operations
	Restore() RestoreRepo      // apply an intent verbatim through the admin variant of the marker transaction (N122)
	Indexes() IndexRepo        // vector_indexes for the index runner: requested → building → ready | failed, lease, purged_since_build (N138)
	Purge() Purger             // Materialize, purge batches and DerivedPurge as engram_admin (N119, N136)
}
```

**Content** (insert-only):

```go
type Inserts interface {
	Documents() DocumentWriter; Chunks() ChunkWriter; Facts() FactWriter; Links() LinkWriter; Entities() EntityWriter
}
type DocumentWriter interface {
		BeginVersion(ctx context.Context, d DocumentDraft) (id.DocVersion, error) // under the documents row lock (N56): assigns the version (above any tombstone's up_to_version) and `append_base_version`; revives a deleting document at up_to_version + 1 (N133c)
	Activate(ctx context.Context, doc id.DocumentID, v id.DocVersion) error
	MarkDeleting(ctx context.Context, doc id.DocumentID, expected id.DocVersion, at time.Time) (current id.DocVersion, err error) // the ONE DELETE_DOCUMENT marker tx (§3, §5.4.1): compares expected under the lock, sets state = 'deleting' and clears summary_blob_key, summary_hash, document_hash, context (''), metadata ('{}') and tags in the same row update; a repeat on a deleting document returns the existing operation, not NOT_FOUND; the outbox seq is drawn in the final statement (N115, N136, N157)
	SetBody(ctx context.Context, doc id.DocumentID, v id.DocVersion, key string, sum [32]byte) error // WHERE body_key IS NULL (N104)
	Lock(ctx context.Context, doc id.DocumentID, m LockMode) error  // LockShared = try-lock (errs.DocumentBusy, 100 ms); LockExclusive = one 35 s attempt, generated from the GUC table (N83, N139)
}
type ChunkWriter interface {
	Insert(ctx context.Context, c ChunkInsert) (id.ChunkID, bool /*inserted*/, error) // ON CONFLICT DO NOTHING; ordinals live in document_version_chunks
	AddVector(ctx context.Context, c id.ChunkID, v VectorRow) error                  // chunk_vectors keyed with embedding_effective_at; ReembedChunk adds a NEW row (N111)
		Member(ctx context.Context, doc id.DocumentID, v id.DocVersion, h [32]byte, c id.ChunkID, ordinal int) error
}
type FactWriter interface {
		Insert(ctx context.Context, fs []Fact, vs []VectorRow) error  // facts + fact_vectors (document_id, chunk_id, mentioned_at, fact_type copied, N138); unique (chunk, extraction_key, content_hash)
	Stamp(ctx context.Context, batch [32]byte, fs []id.FactID, note StampNote) error // fact_consolidation, insert-only (N95)
}
type LinkWriter interface { Insert(ctx context.Context, ls []Link) error } // ON CONFLICT DO NOTHING in key order; undirected edges once with src < dst
type EntityWriter interface {
	UpsertSorted(ctx context.Context, es []EntityUpsert) ([]id.EntityID, error) // one statement, canonical_norm order (N69)
	AddAliases(ctx context.Context, as []Alias) error                           // with the producing document_id (N118)
	AddMentions(ctx context.Context, ms []Mention) error                        // with the fact's mentioned_at (N118)
}
type DocumentReader interface {
	Get(ctx context.Context, doc id.DocumentID) (*Document, error)                      // a DELETING document is its content-free tombstone view; covered versions are never listed (N136, N157)
	List(ctx context.Context, q DocumentQuery) ([]Document, string, error)              // a tombstone never matches a non-empty tag or metadata filter (A-14)
	Version(ctx context.Context, doc id.DocumentID, v id.DocVersion) (*DocVersion, error) // GetDocumentVersion; NOT_FOUND{DOCUMENT_DELETED} for a covered version
	Bodies() DocumentBodies                                                              // GetDocumentBody
}
type DocumentBodies interface { Ref(ctx context.Context, doc id.DocumentID, v id.DocVersion) (BodyRef, error) } // the owner-keyed body key and hash; the API reads the bytes through the shard handle's blob.Store
type TagLister interface { List(ctx context.Context, q TagQuery) ([]TagCount, string, error) }
type DocumentTags interface { // Writes.Tags(): the transaction bumps tag_generation, maintains tag_counts and appends DocumentTagsUpdated last
	TagLister
	Update(ctx context.Context, doc id.DocumentID, p TagPatch) (*Document, error) // NOT_FOUND{DOCUMENT_DELETED} on a deleting document
}
type ChunkReader interface {
	Plan(ctx context.Context, doc id.DocumentID, hashes [][32]byte) ([]ChunkState, error) // member | live | tombstoned | stale_extraction | absent
		Membership(ctx context.Context, doc id.DocumentID, v id.DocVersion) ([]id.ChunkID, error)
	Get(ctx context.Context, c id.ChunkID) (*Chunk, error)
}
type FactReader interface {
		ByIDs(ctx context.Context, ids []id.FactID, o ReadOptions) ([]Fact, error) // visibility predicate + as_of applied; an invalidated fact is FOUND with InvalidatedAt set (GetMemory, BatchGetMemories, N157)
	NearestByOccurrence(ctx context.Context, anchor time.Time, w *TemporalWindow, n int, f Filter) ([]Fact, error) // temporal arm, two-sided btree probe (N68)
		Pending(ctx context.Context, limit int) ([]Fact, error)  // engram_pending_facts: visible, above the watermark, no 'done' stamp (N95)
	Lister() FactLister                                      // ListMemories
}
type FactLister interface { List(ctx context.Context, q MemoryQuery) ([]Fact, string, error) } // structural filters, no ranking; an invalidated fact is listed only on request
type GraphReader interface {
	Hop(ctx context.Context, frontier []id.FactID, kinds []LinkKind, perNode int, f Filter) ([]Link, error) // ONE statement per hop (N165): UNION ALL of `src_memory_id = ANY (frontier)` on the PK and `dst_memory_id = ANY (frontier)` on fact_links_reverse_idx (causal edges forward only), both Index Only Scans; ordered and limited per frontier node BEFORE the facts visibility join, which requires BOTH endpoints visible (N116)
	Similar(ctx context.Context, name, typ string, min float32) ([]EntityCandidate, error)                  // via engram_entity_fuzzy (SECURITY DEFINER, N131)
}
```

**Markers and derived state:**

```go
type Markers interface {
		TombstoneDocument(ctx context.Context, t DocumentTombstone) (DeletionRecord, error)                       // document_tombstones 'pending' covering (document, t.UpToVersion = highest version assigned, N133c); event_seq is NULL until the final statement, WITH s AS (INSERT INTO outbox … RETURNING seq) UPDATE document_tombstones SET event_seq = s.seq (A-10)
	TombstoneChunks(ctx context.Context, reason ChunkReason, cs []id.ChunkID) error                            // 'replace' | 'reextract'
	Hide(ctx context.Context, f id.FactID, cause HideCause, reason string) (rec DeletionRecord, changed bool, err error) // fact_hidden keyed (memory_id, cause), no FK to facts (N145): 'invalidate' | 'reextract'; for 'invalidate', under the subject lock of (document_id, content_hash) that the caller holds until its intent is put (SubjectLocker, N150, N159, N162) it hides EVERY live fact of the subject, each stamped invalidation_op = the operation id (rec.MemoryIDs); a second invalidate is changed = false and a success (N139), but it still inserts its own deletion_log row (N143)
	Unhide(ctx context.Context, f id.FactID) (rec DeletionRecord, changed bool, err error)                    // resolves I = fact_hidden(f).invalidation_op and deletes fact_hidden WHERE invalidation_op = I and the derived_hidden rows of those ids (N162); a 'reextract' row is never touched (N135); works after the fact's own purge (N145), NOT_FOUND{DOCUMENT_DELETED} after its document's purge
	Expunge() ExpungeRepo                                                                                      // expunge_progress, derived_hidden, consumer-cursor check
}
// Every marker method also inserts the shard-local deletion_log row in the same transaction and returns the marker's exact
// effect as a DeletionRecord{Subject, Operation, Prev, DeletedAt, UpToVersion, MemoryIDs}: Subject is the typed id.Subject and Prev
// the subject's chain tip read BY ins_seq under the subject lock (never deleted_at; the predecessor, N122, N162). The intent object is built from that record.
type MarkerReader interface {
	Sets(ctx context.Context) (MarkerSets, error)  // doc_tomb and doc_pending ({document → up_to_version}, N133c) and chunk_tomb: two indexed selects, bounded by expunge lag (N116). fact_hidden is NOT in the set: every arm tests it per candidate by primary-key anti-join
	Curation(ctx context.Context, doc id.DocumentID, hash [32]byte) (CurationState, error) // last action for the subject (document_id, content_hash) with its invalidation_op, re-applied at CommitChunk; a later restore row suppresses the re-application (N162)
}
type Derived interface {
		Observations() ObservationRepo; Pages() PageRepo; Consolidation() ConsolidationRepo // Served lives on the read side (DerivedReader)
	TryDerivationLock(ctx context.Context) error       // shared; ErrRefused → retry (N120)
	DerivationLockExclusive(ctx context.Context) error // Materialize and Restore: one 35 s attempt
}
// The derivation commit rule (N120, N143, N144) is two repository calls under the shared lock, in this order: Verify (a fresh statement), then InsertVersion, whose FIRST statement is the idempotency lookup (engram_derivation_commit_seen), then the base compare-and-set (engram_derivation_base_cas($expected, check_visible)).
type ObservationRepo interface {
	Verify(ctx context.Context, in VerifyInput) (VerifyResult, error) // every rendered fact input against the FULL marker sets (all open tombstones, chunk_tomb, fact_hidden of both causes), observation-version inputs with engram_obs_version_hidden against ALL open tombstones; an early, advisory base read only (the base check that counts is the CAS in InsertVersion)
	InsertVersion(ctx context.Context, v ObservationVersion, inputs []id.FactID, sources []Source) error // FIRST `commit_key` seen → return the committed version, touch nothing (N144); THEN the base CAS `UPDATE observations SET current_version = $expected + 1 WHERE … AND current_version = $expected` for EVERY writer, root rebuilds included (engram_derivation_base_cas), zero rows = ErrBaseLost, the caller rolls back and discards (N143, BaseCurrentAtCommit, TestApply_BaseVersionCAS); then root_version, observation_inputs, observation_version_sources, vector (with obs_tags), meta.superseded_at; the stale flags clear only WHERE stale_seq = $captured; no FK to facts (N135)
	ReplaceSources(ctx context.Context, o id.ObservationID, add, drop []id.FactID) error   // the working set; only under the derivation lock; add before drop (N57)
	
	MarkStale(ctx context.Context, os []id.ObservationID, k StaleKind) error                // narrow mutable row; hides nothing
}
```

`PageRepo` (`Verify`, `InsertVersion`, `SetSources`, `MarkStale`; `Served` is on `PageReader`; `InsertVersion` also writes
`text`, `commit_key` and the `page_version_vectors` row, and its markdown key is attempt-unique and deleted on a failed commit only when no row names it, N144) follows the same shape. `ConsolidationRepo` (`Watermark`,
`Advance`, `Proposals`, `Applied`, `Batches`): proposals are insert-only keyed `(batch_key, attempt)`
and a discard or a capacity retry writes the next attempt (nothing is deleted); `Batches` holds
`state ∈ {routed, stored, applied, discarded, capacity}` and `applied` is written in the same transaction as the `done` stamps; the
watermark is bounded by `ins_seq < engram_seq_floor(now())`, not an id timestamp (N95, C-19). `ExpungeRepo` (`Next`,
`Record`, `ConsumersPassed`, `Finish`) is the bookkeeping and `Purger` (`Materialize`, `Batch`, `Derived`)
runs as `engram_admin`: `Batch` deletes rows, vectors and owner-keyed blobs but never evidence rows (N135),
`Derived` replaces each covered observation or page version by a content-free stub in one transaction, naming every `NOT NULL` column with a documented placeholder (`pv_id` fresh, `markdown_blob_key = '_stub'`, `evidence_hash = '\x00…'`; N136, N157). `Materialize` discovers its work from `fact_hidden WHERE cause = 'invalidate' AND materialized_at IS NULL` and the open tombstones, never from the signal, and stamps `materialized_at` in the batch that writes the `derived_hidden` rows (N145).

**Operations and metering:**

```go
type Ops interface { Operations() OperationRepo; Idempotency() IdempotencyRepo; Ledger() LedgerRepo; Outbox() OutboxWriter } // usage is Tx.Txn().Usage()

// OutboxWriter is the writer half of the transactional outbox (D6). It lives in store so that store does not import
// outbox (N140); internal/outbox holds the relay, cursors and sinks and imports store.
type OutboxWriter interface { // inside the same transaction as the state change; Append is the LAST statement of every write transaction
	Append(ctx context.Context, ev *eventsv1.Event) (seq int64, err error)
	AppendAll(ctx context.Context, evs []*eventsv1.Event) ([]int64, error) // ONE multi-row INSERT: still the last statement
}
type OperationRepo interface {
	Insert(ctx context.Context, o OperationRow) error
	Transition(ctx context.Context, op id.OperationID, from, to fsm.OperationState, r Result) error // monotone; fsm-checked
	SetProgress(ctx context.Context, op id.OperationID, p Progress) error      // absolute values, once per wave (N69)
	Defer(ctx context.Context, op id.OperationID, until time.Time, d DeferredInfo) error
	PendingWithoutWorkflow(ctx context.Context, olderThan time.Duration) ([]OperationRow, error) // op-sweeper (N3)
}
```

`Idempotency` (`Begin`, `Commit`: the stored response returns on a replay within 24 h), `Ledger`
(`Append`, `Get`; no update or delete exists, a trigger rejects both) and `txn.Usage` (`Record`, `Day`; reached as `Tx.Txn().Usage()`;
exactly-once through `usage_key`, PD-1) are the same size. Ids a repository inserts are minted per attempt inside the
transaction (C-19, N139). Ledger rows are deleted only by an
explicit document, namespace or tenant delete under the admin role (§5.4.2), never by a replace.

**Locks.** The per-document lock is `DocumentWriter.Lock` (shared try-lock for `CommitChunk`, exclusive
for `FinalizeVersion` and the delete marker transaction: one 35 s attempt), the derivation lock is on
`Derived`, the **subject lock** `engram_subject_lock_key(ns, subject)` (subject encoded `class:key`, N162; one-argument, seed 2; `store.SubjectLocker`, session level on a dedicated direct connection of the `engram_subject` alias, polled `pg_try_advisory_lock` for ≤ 3 s, held by the marker writers from before the marker transaction until the intent put and the marker re-read, N150, N159), the exclusive fence on `AdminTx.Ownership().FenceExclusive` (`Freeze`, delete freeze, restore:
one 35 s attempt). Key spaces are disjoint (N113): namespace fence, derivation lock, document lock, subject lock.

Roles: `engram_app` (API and worker alike; there is no worker role) runs `lock_timeout = 2 s`, `engram_move` and `engram_admin`
10 s, all with `synchronous_commit = local`, and every outbox-writing role (`engram_admin` and
`engram_move` included) with the 30 s statement and idle timeouts that invariant A-F1 relies on (N133e). *Test seam:* testcontainers Postgres with the real
migrations — RLS (a query without `SET LOCAL` returns nothing), the append-only ledger,
`TestContent_InsertOnly` (N113: a test-only trigger raises on any `UPDATE` of a content row),
`TestFence_TryLockRefusedBehindWaiter`, `TestFence_PoolNotExhausted`,
`TestDocLock_NoStarvation`, `TestIso_Ownership_Transitions`, `TestReadSession_ConcurrentArms` (N arms
run on N connections, none "conn busy"), and the property test that every event
type at its maxima encodes to ≤ 16 KiB (N80).

#### 2.2.8 `internal/index` — search index abstraction

*Pattern: Strategy — recall arms see only `Searcher`; the index lives in the shard's Postgres
(`Transactional`) or in an external engine fed by the outbox (`Async`). The D7 name `index.Index`
denotes a `Searcher` plus, for an `Async` engine, the `Applier` that the `index` outbox sink drives.*

```go
package index

type Filter struct {
	AsOf *time.Time              // mentioned_at ≤ AsOf inside the predicate; chunk arm also embedding_effective_at ≤ AsOf (N85)
	FactTypes []memoryv1.FactType // copied onto fact_vectors, so the type filter runs inside the scan (N138)
	AllowedDocs []id.DocumentID  // the tag and metadata filters, resolved once at document level (N116, N139)
	Markers store.MarkerSets     // doc_tomb (document → up_to_version) and chunk_tomb arrays, applied to EVERY hit, also an external engine's (N44, N116); fact_hidden is an anti-join per candidate, never an array
}
type Hit struct { Ref id.MemoryRef; Score float64 /* arm-local; normalised per query by the planner (N67) */; MentionedAt time.Time }

// Every method takes the ReadSession and opens its own short transaction(s) with s.Tx: arms run concurrently (N140).
type Searcher interface {
	SearchSemantic(ctx context.Context, s store.ReadSession, q SemanticQuery) ([]Hit, error)
	SearchLexical(ctx context.Context, s store.ReadSession, q LexicalQuery) ([]Hit, error)
	SearchChunks(ctx context.Context, s store.ReadSession, q ChunkQuery) ([]Hit, error)
	SearchPages(ctx context.Context, s store.ReadSession, q PageQuery) ([]Hit, error)  // BM25 over page_versions.text ∪ HNSW over page_version_vectors, visible versions only (N73, N139)
	Plan(ctx context.Context, s store.ReadSession, f Filter) (SemanticPlan, error)    // the cost-based estimate and plan, chosen in Go (N138, N151): PlanExact | PlanHNSW{MaxScanTuples, ExactFallback}
}
type Applier interface { Apply(ctx context.Context, events []*eventsv1.Event) error } // Async only; idempotent by (namespace_id, seq)
```

`SemanticPlan` (N112, N138, N151) is **cost-based and chosen in Go before the arm runs**. `Plan` estimates
the eligible rows `E` (`engram_eligible_facts`): the sum over the current versions of `Filter.AllowedDocs` of
`document_versions.fact_count_by_type[requested types]` (written at `FinalizeVersion`), minus the namespace's hidden
fraction from `namespace_stats`; under `AsOf` the per-namespace monthly `mentioned_at` histogram. **Below θ**
(starts at **10 k**, fixed by M0.6 in [5 k, 20 k] from the measured crossover; an exact arm at θ costs ≈ 42 k buffers and ≈ 65 ms warm, ≈ 4.2 buffers and 6.6 µs per eligible row, N164) the arm runs the **exact path**,
driven from `facts_doc_idx` or `facts_mentioned_idx`, joined to the vector table by primary key, the distance in a
`MATERIALIZED` CTE over the eligible rows and ordered outside it, `LIMIT cap`. **At or above θ** it uses the
namespace's own **partial HNSW** (built by the index runner; `plan_cache_mode = force_custom_plan`, `ef_search` = the
arm cap) as an iterative scan with `hnsw.max_scan_tuples = max(20 000, min(4 × ef_search / s, 100 000))`, never
below pgvector's default, and `SET LOCAL enable_bitmapscan = off, enable_sort = off`; if the scan exhausts with fewer
than `cap` rows and `E ≤ 4 θ`, **the arm re-runs the exact path in the same transaction** (bounded cost: HNSW cap plus
exact(E)); only above `4 θ` does it return what it has with `RecallStats.partial`. On the
**HNSW path** `$allowed_docs` is always `document_id = ANY ($1)` with the array as a custom-plan constant (a hashed
`ScalarArrayOp` inside the index scan; planning 5–13 ms at 1–5 k documents, accepted; M0.6's `EXPLAIN` pins require no
`Nested Loop Semi Join` and no `Join Filter` on `document_id`); on the **exact path** the 500-document rule applies:
one array constant up to 500 documents and `document_id IN (SELECT unnest($1))` above, which is correct there (namespace and model stay
constants for the partial-index proof). **Filtered arms are their own class** (N164): a recall with any vector arm at
`E ≥ θ` is counted as filtered (`RecallStats.filtered`), has p95 ≤ 1 s and passes the second arm semaphore (2 per API process per
shard). The observation arm has its own estimate (`namespace_stats.observations_by_tag`)
and tests tags inside the scan (`observation_version_vectors.obs_tags`, copied at insert); the chunk arm uses the fact
estimate of its documents. A namespace below 2,000 vectors has no index and always takes the exact path. The arm's
transaction forbids a partition scan (`enable_seqscan = off`, no parallel workers). One index per namespace and model;
nothing is shared across namespaces. **Index DDL has one owner (N138):** the index runner, an `engramctl index` daemon
on the control host holding `engram_migrate`, fed by `vector_indexes` requests (`requested → building → ready |
failed`, with a lease); it refuses to start a build while a *client* backend's `backend_xmin` is older than 5 min
(autovacuum, `VACUUM` and walsenders are excluded; `IndexRunnerStarved` alerts after 30 min of refusals), serialises
builds per shard (`maintenance_work_mem = 2.4 KB × vectors`, ≤ 5 GB), checks `pg_index.indisvalid` and drops an
invalid index first, and drops by the deterministic `engram_hnsw_ddl` names. **The partition is the hygiene unit
(N152, N166):** a touched HNSW is due at `purged_since_build ≥ max(1 % × rows_at_build, 2,000)`; when any index on a vector
partition is due, the runner rebuilds the neighbour graph of an index only when its `purged_since_build ≥ 0.05 % × rows_at_build`
(≥ 50 elements; below that the partition vacuum repairs it) with `engram_hnsw_ddl(…, 'rebuild')` =
`REINDEX INDEX CONCURRENTLY` after dropping `_ccnew*` leftovers (`engram_index_partition_hygiene`), and only then runs
one `VACUUM (INDEX_CLEANUP ON)` of the partition. A rebuild writes its graph in one burst, so the runner starts one only while archive
lag is under 30 s and `pg_wal` headroom is at least twice the expected burst, and holds a per-partition advisory key exclusively
from selection until the `VACUUM` has started (purge batches take it shared with a try-lock and skip the partition meanwhile).
It serves a move target's requests at priority (N160). Neither
`engram-api` nor `engram-worker` runs DDL. Under `Filter.AsOf` a chunk hit's header is empty. Tag semantics exist once
(`index/tags.go`, mirrored by the Lean decision procedure). *Test seam:* the same retrieval golden set on all
implementations; `as_of` and visibility leakage tests assert zero hidden or future hits per arm; `TestRecall_FilteredArm`
(topic-correlated filters at 1, 2, 5, 10 and 20 % eligible) checks `min(cap, |visible|)` with `partial = false` below
`4 θ` on the real image and runs at 5,000 allowed documents; `TestIndex_Hygiene` (three namespaces purged at 0.2, 1 and 0 %, a 1 M neighbour touched once and not rebuilt, a purge during the rebuild set skipped).

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
searched, a small **DAG scheduler** for the retrieval stage (nodes declare `Needs()`, so lexical and
temporal overlap the embedding round-trip, N54: a purely sequential `Pipeline` cannot express that) and
**Pipeline** (`pipeline.Step[*State]`) for the stages after it; the planner is the composition and nothing
else.*

```go
package recall

type Query struct {
	Scope id.Scope; Text string; Vector []float32 /* nil: dense arms skipped */
	Budget Budget; Filter index.Filter; Plan index.SemanticPlan; QueryTimestamp *time.Time; Window *TemporalWindow; MaxTokens int; Deadline time.Time
}
type Candidate struct { Ref id.MemoryRef; Arm string; Rank int; ArmScore float64; MentionedAt time.Time; Provenance Provenance }

// Arm is the Strategy: one retrieval method. The planner schedules by Needs(). Each Run opens its own short read
// transaction(s) on the session; Filter, Plan and the marker sets arrive in q as values computed once (N140).
type Arm interface {
	Name() string // semantic | lexical | graph | temporal | chunks
	Needs() Needs // NeedsNone | NeedsVector | NeedsSeeds (the graph arm runs two seeded waves, N54)
	Run(ctx context.Context, s store.ReadSession, q *Query, seeds []Candidate, capN int) ([]Candidate, error)
}
type Fuser interface    { Fuse(lists [][]Candidate, k int) []Fused }     // RRF k = 60 in math/big.Rat: permutation-invariant exactly (N67)
type Reranker interface { Rerank(ctx context.Context, query string, in []Fused, top int) ([]Ranked, error) } // top 0/50/150 (N53); bge-reranker-base
type Packer interface   { Pack(in []Ranked, maxTokens int) PackResult }  // greedy; skip, never truncate; rank 1 always whole (N129)
type Planner interface  { Recall(ctx context.Context, q *Query, sink Sink) error }

// The planner is a small DAG scheduler followed by sequential pipeline steps (N140):
//   Retrieve = DAG{ visibility (marker sets, allowed documents, plan estimate: N116, N138), embed, arms by Needs() } →
//   Fuse → Rerank → Boost → Pack → Stream      (each of the last five is a pipeline.Step[*State])
```

`Booster` (`Boost`, recency ≤ +10 %, temporal ≤ +10 %, proof ≤ +5 %, clamp [0.75, 1.25]) is a one-method
interface and `Sink` has two (`Batch`, `Stats`). Scheduling (§1.5, N54):
`NeedsNone` arms start as soon as `visibility` is done and overlap the embedding round-trip, `NeedsVector` arms when the
embedding arrives, the graph arm in two waves of 30 ms from the lexical and semantic top-20. Every
other arm gets `min(ArmDeadline, remaining − rerankReserve − packReserve)`. **Rerank is skipped when
the remaining deadline is below 106 ms** (= rerank p95 90 + pack 3 + stream 5 + 8, N106); at client
deadlines ≥ 300 ms a skip counts against the rerank-skip SLO (N53), below 300 ms it is by design. The
critical path is 236 ms p95 at MID (216 ms of stages plus a ≈ 20 ms pool-wait term, N54, N164, N176). The graph arm is driven from Go, **one statement per hop** in the arm's read transaction (a cut at the sub-budget keeps the completed hops); the HIGH graph arm (1,000 nodes, 2 hops) has an explicit 100 ms sub-budget (N165). Filtered arms are a separate class with their own SLO and semaphore (N164). While markers are `pending` the observation and page arms pay a
per-candidate lookup and the skip SLO is suspended for the namespace (N119). Under `as_of` the chunk
arm adds `embedding_effective_at ≤ T`, headers are empty, entity names are suppressed; the version served
for an observation or page is the one current at `T`, or nothing when it is hidden (N117). *Test seam:* rapid properties mirror the Lean theorems (RRF permutation invariance, packing
within budget, boost in [0.75, 1.25]); a fake-clock test asserts the 236 ms path and the 106 ms
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
type ProposalStore interface { // insert-only: nothing is updated or deleted (engram_app holds SELECT, INSERT), N121
	Store(ctx context.Context, tx store.Tx, p Proposal) (stored Proposal, err error)  // keyed (batch_key, attempt); ON CONFLICT DO NOTHING, the stored list wins (N43); every update and merge op records base_version
	Load(ctx context.Context, tx store.Tx, key BatchKey, attempt int) (Proposal, error)
	Retry(ctx context.Context, tx store.Tx, key BatchKey, why RetryWhy) (next int, err error) // a discard or a capacity retry opens attempt+1 (capacity: prompt_variant = 'capacity'); the old list is dead by key
}
type Applier interface { Apply(ctx context.Context, tx store.Tx, p Proposal) (Applied, error) }
```

`Apply` runs in one fenced derivation transaction and obeys the commit rule of N120 (shared derivation lock;
`Derived.Observations().Verify` re-verifies every rendered input in a fresh statement against the full marker
sets, and the base is checked as a **compare-and-set at commit** (`UPDATE … SET current_version = v + 1 WHERE …
AND current_version = $expected`, base visible, for every writer, root rebuilds included, after the `commit_key` lookup
that returns an already committed version first; zero rows means the writer lost; the shared lock does not serialise
two writers, the row lock of the CAS does, N143, N144); on any failure it rolls back, the
**whole** proposal is discarded to the next attempt and the batch re-routed, never "repaired"):
`consolidation_applied(op_key)` gates each write and commits with the effects (N43); `consolidation_batches.state =
'applied'` is written with the `done` stamps, and an all-skip batch applies zero ops, stamps and terminates; versions carry `root_version`, `observation_inputs`
(the facts shown), `observation_version_sources` and a vector; the previous version's write-once
`superseded_at` is inserted; the working set `observation_sources` is rewritten only here. Nothing
hides anything: hiding is the read predicate. `effective_at(v) = max(mentioned_at of every fact shown,
effective_at(v − 1))`. `op_key = sha256(batch_key ‖ attempt ‖ op_index)` over the **stored** list (attempts are capped at 4). While markers
are pending only root rebuilds run (degraded mode), and they obey the same rule. A rebuild that finds no
visible source retires the observation without a gateway call (exempt from `quota.Reserve`, N135); a stage-2 write whose
target observation is retired re-routes its facts (new attempt) instead of stamping them `done` (C-13). *Test seam:* model-based tests from `Consolidation.tla` and
`Derivation.tla`: at-least-once re-execution yields one effect per `op_key`, and a version written with a
victim in view is hidden whichever of marker and apply commits first; `TestConsolidation_StaleProposalDiscarded`,
`_AllSkipStamps`, `_CapacityAttempt`.

#### 2.2.15 `internal/reflectagent` — bounded agent loop (phase 2)

Forced searches (`search_pages` first once pages exist, then observations, then facts; N139), ≤ 10 free iterations, ≤ 100 k context tokens, ≤ 300 s,
tool deadline 10 s; tools `search_memories`, `search_observations`, `search_pages` (N73), `get_page`,
`expand_fact`; citations filtered to ids a tool actually returned; optional JSON-schema output;
at the context cap a map/reduce fallback instead of forcing `done`. Every tool reads through the same
visibility predicate. *Pattern: Strategy (`Tool`) in a bounded loop; the agent runs inside `engram-api`
with the caller's scope, no second authz path.*

```go
package reflectagent // not "reflect": the old name shadowed the standard library package (N140)

type Tool interface {
	Name() string
	Schema() json.RawMessage
	Call(ctx context.Context, sc id.Scope, c id.Caller, args json.RawMessage) (ToolResult, error) // ToolResult.Returned []id.MemoryRef feeds the citation verifier; no authz import (N157)
}
type Agent interface { Run(ctx context.Context, req Request, caps Caps, sink EventSink) (*Result, error) }
type CitationVerifier interface { Verify(cited []id.MemoryRef, returned map[id.MemoryRef]struct{}) (kept, dropped []id.MemoryRef) }
type SchemaValidator interface { Validate(schema, doc json.RawMessage) error }
type Registry interface { Register(t Tool); Get(name string) (Tool, bool); Specs() []gateway.ToolSpec }
```

`Request` carries the persona (`mission`, typed `Directive{text, priority, active, tags}` filtered by
the call's tags, `disposition`). Each iteration passes `quota.Reserve` (a refusal is
`RESOURCE_EXHAUSTED`, N130). Rejected: running Reflect as a workflow (interactive, streamed, ≤ 300 s).
*Test seam:* transcript replay; every emitted citation ∈ the
union of `Returned`; caps with a fake clock. A transcript (`reflect/{op}.jsonl`) is an optional debugging
aid that the delete's `DerivedPurge` removes wholesale (N136).

#### 2.2.16 `internal/pages` — mental models / knowledge pages (phase 3)

Source query + tag filter, versioned markdown in blob, evidence segments per version (N117), one
`RefreshPolicy{trigger, interval, debounce}` (N128), delta refresh by `page/v1` and root rebuild by
`page_full/v1`. *Pattern: Repository, split into a read and a write capability.*

```go
package pages

type Reader interface {
		Get(ctx context.Context, sc id.Scope, c id.Caller, p id.PageID, sel VersionSelector) (*Page, string /*markdown*/, error) // PAGE_HIDDEN when hidden; reads through store.ReadTx.Derived() (N157)
	ByName(ctx context.Context, sc id.Scope, c id.Caller, name string, sel VersionSelector) (*Page, string /*markdown*/, error) // GetPage by name (A-3)
	List(ctx context.Context, sc id.Scope, c id.Caller, q ListQuery) ([]Page, string, error)
	Search(ctx context.Context, sc id.Scope, c id.Caller, q SearchQuery) ([]PageHit, string, error) // BM25 over page_versions.text ∪ HNSW over page_version_vectors, visible versions, RRF, no LLM (N73, N139)
}
type Writer interface {
		Create(ctx context.Context, sc id.Scope, c id.Caller, p PageDraft) (*Page, error)
	Delete(ctx context.Context, sc id.Scope, c id.Caller, p id.PageID) error
	Refresh(ctx context.Context, s store.Store, sc id.Scope, p id.PageID, why RefreshReason, op id.OperationID) (*RefreshResult, error) // takes a store.Store, never a ShardHandle (N157)
}
type Admin interface { // PageService.UpdatePage; changing source_query or tag_filter sets stale_write
	Update(ctx context.Context, sc id.Scope, c id.Caller, p id.PageID, patch PagePatch, etag string) (*Page, error)
}
```

`Page.RefreshPolicy` is the generated `memoryv1.RefreshPolicy`; there is no `on_delete`, `cron` or
string policy. `Refresh` runs as the per-page singleton `ns/{ns}/page/{page_id}`, a manual one by `SignalWithStart` with the `operation_id` in the nudge (`REFRESH_PAGE` is singleton-backed, `WaitOperation` polls the page row, `Restart` skips it; N157); a refresh whose base version is
hidden is a root rebuild, and `CommitPageVersion` obeys the derivation commit rule (N120): it re-verifies every
rendered input under the shared lock and commits with the idempotency lookup first (`commit_key`, `attempt_nonce` per activity execution id) and then the same compare-and-set on `current_version = $expected` for every writer, root rebuilds included (N143, N144; attempt-unique markdown keys, `TestPageRefresh_CommitRetry`), and a refused commit re-runs
`page_full/v1` from current evidence (`TestPageRefresh_DeleteMidCall`). Page versions carry `text` (BM25) and a
`page_version_vectors` row, so pages share the observation visibility and purge path (N139). *Test seam:* testcontainers; staleness-flag and `PAGE_HIDDEN` tests.

#### 2.2.17 `internal/workflows` — Temporal workflows, activities, policies

All workflow definitions and the activity *interfaces* they call; task-queue naming; `ContinueAsNew`
rules; retry policies; the API's client wrapper. Concrete activities are structs in
`cmd/engram-worker` that wire service packages into these interfaces. *Pattern: **Pipeline** (the
workflow body is the ordered list of activity groups) and **Saga** for the move only — it is the one
workflow with compensations before a point of no return; the expunge and tenant delete are forward-only
workflows, not sagas). Each activity is idempotent by key and fenced by the epoch, so a restart resumes
instead of repeating.*

```go
package workflows

func TaskQueue(s id.ShardID) string            // "shard-{id}"
func OpID(ns id.NamespaceID, op id.OperationID) id.WorkflowID // "ns/{ns}/op/{op}": retain, export, namespace delete (refresh is singleton-backed: PageRefreshID)
func ExpungeID(ns id.NamespaceID) id.WorkflowID        // "ns/{ns}/expunge": the singleton behind DELETE_DOCUMENT (N136)
func ConsolidateID(ns id.NamespaceID) id.WorkflowID    // "ns/{ns}/consolidate"
func PageRefreshID(ns id.NamespaceID, p id.PageID) id.WorkflowID // "ns/{ns}/page/{page}": scheduled, delete-driven and manual refreshes (the operation_id rides in the nudge)
func MoveWorkflowID(ns id.NamespaceID, e id.Epoch) id.WorkflowID // "move/{ns}/{epoch}" on shard-{target}

// Workflows (inputs and results are protos in engram.internal.workflow.v1; every input carries schema_version 2).
func RetainDocument(ctx workflow.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.RetainDocumentResult, error)
func Consolidate(ctx workflow.Context, in *workflowv1.ConsolidateInput) error    // long-lived; Nudge signal; debounce 30 s; ContinueAsNew every round
func Expunge(ctx workflow.Context, in *workflowv1.ExpungeInput) (*workflowv1.ExpungeResult, error) // also target NAMESPACE (§5.4)
func PageRefresh(ctx workflow.Context, in *workflowv1.RefreshPageInput) (*workflowv1.RefreshPageResult, error)
func Move(ctx workflow.Context, in *workflowv1.MoveInput) (*workflowv1.MoveResult, error)
// ExportSnapshot, TenantDelete (fences every namespace, then stamps acknowledged_at, N182), RetainBackfill, ReembedNamespace and the schedules (SweeperInput) follow the same shape.

// The retain pipeline is three ordered activity groups; the workflow runs Prepare once, ChunkPipeline per chunk
// (≤ 32 in flight, a workflow-side semaphore), then Finish.
type Prepare interface {
		LoadItem(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.LoadItemResult, error)     // APPEND base under the documents row lock (N56); single owner-keyed ver/{document_id}/v{n} body blob (N104)
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
		FinalizeVersion(ctx context.Context, in *workflowv1.FinalizeVersionInput) (*workflowv1.FinalizeVersionResult, error) // inserts chunk tombstones and fact_hidden(reextract), flags the affected observations stale (evidence rows are kept, derived versions stay visible, N135); superseded_by (N49)
	ReembedChunk(ctx context.Context, in *workflowv1.ChunkWork) error                                                     // inserts one chunk_vectors row (N60, N111)
	MarkOperation(ctx context.Context, in *workflowv1.MarkOperationInput) error
}

// Consolidation steps: RouteBatch, WriteObservation, StoreProposal, ApplyBatch, StampFailed (≤ 5 per interface); the
// quota gate is a method of quota.Reserver, called inside the LLM activities, not a separate activity (N130).
// ApplyBatch and CommitPageVersion are the two derivation transactions that obey the commit rule of N120.

// The Temporal client is split by capability (N140), each interface with at most five methods, workflow ids typed.
type Starter interface { // engram-api (and the delete handlers); AlreadyStarted → nil
	StartRetain(ctx context.Context, in *workflowv1.RetainDocumentInput) error         // ns/{ns}/op/{op}
	StartExport(ctx context.Context, in *workflowv1.ExportInput) error                 // ExportSnapshot at ns/{ns}/op/{op}
	
	StartNamespaceDelete(ctx context.Context, in *workflowv1.ExpungeInput) error       // Expunge{NAMESPACE} at ns/{ns}/op/{op}
	StartTenantDelete(ctx context.Context, in *workflowv1.TenantDeleteInput) error     // tenant/{tenant}/delete on the cell-wide control queue
}
type Signaller interface { // SignalWithStart of the per-namespace singletons
	SignalConsolidate(ctx context.Context, t *workflowv1.ConsolidateTrigger) error     // ns/{ns}/consolidate
	SignalExpunge(ctx context.Context, in *workflowv1.ExpungeInput) error              // ns/{ns}/expunge
	SignalPageRefresh(ctx context.Context, in *workflowv1.RefreshPageInput) error      // ns/{ns}/page/{page}
}
type Waiter interface {
	Cancel(ctx context.Context, wf id.WorkflowID) error                                                // refused for DELETE_* operations before it gets here (N136)
	WaitResult(ctx context.Context, wf id.WorkflowID, timeout time.Duration) (done bool, err error)    // WaitOperation long-poll (N70)
}
// A manual page refresh is Signaller.SignalPageRefresh with the operation_id in the nudge. The move is started by move.Orchestrator.Start (api.Deps.Mover); RetainBackfill and ReembedNamespace are started by engramctl's own client.
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

*Pattern: **Transactional outbox** — `store.OutboxWriter.Append` is the last statement of every write transaction,
so a consumer sees exactly the committed changes, with no dual write.* One relay per shard, elected
with `pg_try_advisory_lock` on a dedicated direct connection, reads `outbox` in `seq` order in batches
of 500, drives named sinks with independent cursors and delivers a strict prefix (no cursor passes an
open gap; sound under A-F1, 60 s watchlist).

```go
package outbox

// The writer half of the outbox (Append, AppendAll) lives in store as store.OutboxWriter (N140): store does not import
// outbox, and this package imports store.

type Sink interface {
	Name() string // "index" | "kafka"
	Apply(ctx context.Context, events []Event) error // idempotent by (namespace_id, seq)
	Filter() func(Event) bool
}
type Relay struct { /* handle, sinks, cursors, gaps */ }
func NewRelay(s store.Store, sinks []Sink, o RelayOptions) *Relay // takes a store.Store and uses s.Direct for the election lock
func (r *Relay) Run(ctx context.Context) error           // acquires the lock; returns when lost
func (r *Relay) AddSink(ctx context.Context, s Sink, fromSeq int64) error
type GapWatch interface { Note(seq int64, seenAt time.Time); Due(now time.Time) []int64; Resolve(seq int64) } // persisted in outbox_cursors.gaps (≤ 1 000)

// Group reassembles a paged event group (N80): complete only when every page was seen; an ids_elided event is complete alone.
type Group interface { Add(e Event) (complete bool); Ids() [][]byte; Elided() bool }
```

The relay uses the shard-level `engram_relay` role (RLS bypass for `SELECT` on `outbox` only). Events
are thin and bounded (≤ 256 ids, ≤ 16 KiB; `DocumentDeleted` has no id list); cursor advances are
batched to one per second per consumer (N114); the expunge reads the cursors through
`engram_consumers_passed(event_seq)` before it purges (the tombstone stores the `seq` of its own delete event). **Moves are not consumers** (N124). *Test seam:* a model-based test from
`Outbox.tla` — random commit orders with holes deliver every seq exactly once or declare it aborted
after the watch window.

#### 2.2.19 `internal/move` — namespace move saga (D5)

The D5 protocol as Temporal workflow `move/{ns}/{epoch}` on `shard-{target}` (N2): **freeze, then copy every table
from the static source, verify by equality, build the indexes and seal the copy under the freeze, then cut over with a `ready` state and a
catalog CAS** (N160, N161, N125). *Pattern: **Saga** with a **state machine** — every step is idempotent,
every step before the catalog CAS (a″) has a compensation, and (a″) is the point of no return. It is the only
code path that holds two shard handles.*

```go
package move

type Fence struct { Ns id.NamespaceID; Tenant id.TenantID; Source, Target id.ShardID; Epoch id.Epoch /* e; the target holds e+1 */; Move id.MoveID
	SrcRow, DstRow OwnershipRow /* the exact source and target namespace_ownership rows the activity VERIFIED (state, freeze_reason, epoch, move_id, target hint) */ }

// The source is static from Freeze to thaw or cleanup, so no floor, catch-up or merge exists: the target's sequence is advanced
// past the source's final value W_final ONCE, at the start of FrozenCopy (N147, N160).
// Activities are split by phase so each interface stays small (≤ 5 methods). Every activity first re-checks the fence
// against the catalog row and both ownership rows and the source session's system_identifier/timeline (MoveFenced).
// Every STEP, including the catalog CAS, is then a compare-and-set on the exact rows it verified (N143, ShardMove_UnfencedSteps):
// each ownership edge is `UPDATE namespace_ownership … WHERE namespace_id AND state, freeze_reason, epoch, move_id = <verified values>`;
// Moves.Commit adds the verified shards, epochs and timeline; zero rows = MoveFenced, the step stops (TestMove_CatalogCAS).
type Copier interface {
	Plan(ctx context.Context, f Fence) (*PlanResult, error)               // plan_target / start_move edges; records system_id, timeline, W_est; pauses expunge; blob pre-warm (the only pre-freeze work)
	FrozenCopy(ctx context.Context, f Fence) (*CopyResult, error)         // engram_seq_advance(W_final) once, then classes 1 and 2 and token_usage_events once by PK ranges ≤ 100 k rows, 4 streams, WAL-paced; resumable at (table, last_key)
	VerifyFrozen(ctx context.Context, f Fence) (*workflowv1.VerifyFrozenReport, error) // per PK range (≤ 100 k rows, bit_xor combined in Go, each statement inside the 30 s role timeout) and per-table counts, VerifyFK, blob existence; one re-copy of mismatching tables, then MoveVerifyFailed
}
type Readier interface {
	BuildIndexes(ctx context.Context, f Fence) (*workflowv1.BuildIndexesReport, error) // under the freeze (maintenance_work_mem = min(2.4 KB × v, 24 GB), shm 32g): request the target's partial indexes for every table, wait for engram_move_indexes_valid; a failed build retries once
	SealCopy(ctx context.Context, f Fence) (*workflowv1.SealCopyResult, error)       // target: pg_switch_wal() → copy_end_lsn; pgbackrest check (archived); shard.Replication.Replayed (standby; vacuous on `single`); then MoveStamps.RecordFloor (N179)
	AwaitConsumers(ctx context.Context, f Fence) error                    // every source outbox consumer cursor past the namespace's final max(seq), bound 120 s
}
type Freezer interface {
	Freeze(ctx context.Context, f Fence) (deadline time.Time, err error)  // exclusive fence, one 35 s attempt, freeze_move edge; returns the freeze deadline the workflow arms as a timer until (a″)
	Drain(ctx context.Context, f Fence) (*DrainResult, error)             // in-flight set read from the static source operations rows, backfill parents included (N97, C-20)
}
type Cutover interface {
	Begin(ctx context.Context, f Fence) error          // (a) intent only
	ReadyTarget(ctx context.Context, f Fence) error    // (b′) ready_target(floor_lsn); the ownership trigger refuses `ready` unless engram_move_indexes_valid, last_value > w_final and a floor (N147, N160, N179)
	Commit(ctx context.Context, f Fence) error         // (a″) catalog Moves.Commit: CAS cutover → committed on the VERIFIED source/target rows — THE point of no return; then waits for Replicated(CommitLSN) (≤ 10 s per attempt) and re-reads the row (N171)
}
type Handover interface { // only after Commit returned; (c) is fenced on the source row and the replicated `committed` only (N169(4))
	Source(ctx context.Context, f Fence) error         // (c) cutover_c: frozen/move → moved_out + target hint
	ActivateTarget(ctx context.Context, f Fence) error // (b″) activate_target, unconditional whenever the target row reads `ready` and the move `committed`, whatever `namespaces` says; retried indefinitely; stamps activated_at, clears move_id (N180)
	Catalog(ctx context.Context, f Fence) error        // (d) shard, epoch, state, NOTIFY — retried indefinitely; success is `shard = target ∧ epoch ≥ e_t + 1`; the end test after (c) reads the target row, not `namespaces` (N180)
}
type Closer interface {
	Restart(ctx context.Context, f Fence, d *DrainResult) error // ns/{ns}/op/{op} on shard-{target}, TERMINATE_IF_RUNNING, memo epoch e+1; singleton-backed kinds by SignalWithStart
	Cleanup(ctx context.Context, f Fence) error                 // the worker activity that DRIVES the batches once the row reads `cleaning` (N170): DROP INDEX by name, engram_cleanup_namespace on the source; CleanupMove (committed → cleaning) precedes it; it records `done` through Moves.Advance after one more zero-row engram_cleanup_namespace; the source blob prefix at finished_at + 28 d; the moved_out row stays
	Rollback(ctx context.Context, f Fence, why string) error   // only before (a″): wins the cutover → rolled_back CAS, waits for it to replicate (the actor ends otherwise), then thaws; run at the freeze deadline (MoveWindowExceeded); after (a″) the page MoveFrozenPastDeadline instead (N171); see the table
}
type Orchestrator interface { // MoveService (api imports move, N157)
	Start(ctx context.Context, ns id.NamespaceID, target id.ShardID, o StartOptions) (*Ref, error) // o.EstimateOnly returns Ref.WindowEstimate without planning
	Status(ctx context.Context, m id.MoveID) (*Status, error)
	Abort(ctx context.Context, m id.MoveID) error // rollback before (a″), else an error
	Cleanup(ctx context.Context, m id.MoveID) (*CleanupResult, error) // MoveService.CleanupMove: takes `committed → cleaning` only (the schema trigger refuses before activated_at + 24 h); Closer.Cleanup drives the batches and records `done` (N167, N170, N184)
}
```

| Step | Compensation (what `Rollback` runs when this step has completed) |
|---|---|
| `Plan` | `rollback_target` (delete the target rows and ownership row; for a move back onto a `moved_out` shard `return_abort` restores the permanent fence value); `abort_move` on the source; the pre-warmed blob prefix is deleted in the background |
| `Freeze` | `thaw_move` first (allowed while the target is unreachable; after a replicated `rolled_back`), then the above |
| `Drain`, `FrozenCopy`, `VerifyFrozen`, `BuildIndexes`, `SealCopy`, `AwaitConsumers` | the same (copied rows and indexes are discarded; the workflows recorded by `Drain` restart on the source queue) |
| `ReadyTarget` (b′), `Begin` (a) | the CAS `cutover → rolled_back`, `unready_target` (at next contact if the target is down), then `thaw_move` and the above |
| `Commit` (a″) onward | none: the move completes forward (by the mover or by the restore reconcile); a reverse move is an ordinary new move |

A target restored or failed over after the seal is an ordinary restore bounded below by the floor `(copy_end_timeline, copy_end_lsn)` (N179); `restore cleanup-moved-out` handles `cleaning` and `done` moves (N184). `shard.Replication{Replayed(ctx, lsn pg.LSN) (bool, error)}` (`internal/store`, `engram_standby_replayed`) is the seam of the seal's standby wait. `StartOptions` carries `Window` (default 4 h, cap 8 h, at least max(1.5 × `W_est`, `W_est` + 10 min); required above 10 min, ≈ 350 k facts, except for the rebalancer), `FreezeNotBefore`, `EstimateOnly`, `DrainWait` (15 s, max 60 s), `CopyRangeRows` 100 000, `CopyStreams` 4 and the cutover retry. `W_est` (≈ 27 min per 1 M live facts) is §5.5's (N173). *Test seam:* a model-based test from
`ShardMove.tla` with `MemoryCatalog` and two `FakeTx` shards: writers refused `NamespaceFrozen` and resumed on the target with nothing lost or duplicated (`TestMove_FrozenCopy`), a dropped range detected (`TestMove_VerifyFrozenCatchesFault`, `TestMove_VerifyPerRange`), indexes for every table (`TestMove_IndexesRequestedForEveryTable`), rollback at the deadline (`TestMove_WindowDeadlineRollback`), a short window refused (`TestMove_PlanRefusesOversizedWindow`), no thaw before the abort replicated (`TestMove_RollbackWaitsForReplicatedAbort`), a restore racing `Commit`, the cut waiting for the sealed copy (`TestMove_CutWaitsForSealedCopy`) and the replicated commit (`TestMove_CutWaitsForReplicatedCommit`), a target restored after the seal (`TestMove_TargetRestoredAfterSeal`), a PITR or failover below the floor refused (`TestMove_TargetPITRBelowFloor`, `TestMove_FailoverBelowFloorRefused`), activation after a catalog promotion (`TestMove_ActivateAfterCatalogPromotion`), expunge paused from Plan to activation (`TestMove_ExpungeStaysPaused`), `cleaning` refused before 24 h (`TestMove_CleanupGateAtEntry`); chaos: `TestMove_FailoverBetweenCAndD`.

#### 2.2.20 `internal/export` — snapshots for local agentic search (phase 3)

`building` → `ready` snapshots under `{shard}/{tenant}/{ns}/export/v{n}/` (manifest, facts, observations,
chunks, pages, the always-emitted delta with `deleted_ids`) and 1 MiB part streaming (N126).
*Pattern: Builder with a commit point — the manifest is written last.*

```go
package export

// Shard is the pair of resources a snapshot needs; the API builds it from the shard handle, so export imports neither router nor authz (N157).
type Shard struct { Store store.Store; Blob blob.Store }
type Builder interface {
	Begin(ctx context.Context, tx store.Tx, o BuildOptions) (id.SnapshotVersion, error)       // inserts the row as 'building' and records the ins_seq watermark; deferred ten minutes on a namespace moved in that recently (N147)
	WriteFiles(ctx context.Context, sh Shard, sc id.Scope, v id.SnapshotVersion) (*Manifest, error) // short READ COMMITTED ranges bounded by the watermark, marker sets read once; NO long snapshot (it would block every index build and pin the vacuum horizon, P-10)
	Record(ctx context.Context, tx store.Tx, m *Manifest) error                                // refuses to promote an expired row; re-checks tombstones newer than the start
}
type Streamer interface {
		Stream(ctx context.Context, sh Shard, sc id.Scope, v id.SnapshotVersion, path string, offset int64, part func(ctx context.Context, p Part) error) error // resumable by (path, offset); refuses expired versions; the stored bytes are NOT filtered on read
		Latest(ctx context.Context, sh Shard, sc id.Scope) (*Manifest, error)
	Overlay(ctx context.Context, sh Shard, sc id.Scope, page string) (*Overlay, error)       // GetSnapshotManifest's live hidden_overlay, computed with the READ PREDICATE (segment test over fact_hidden(invalidate) and open tombstones), markers newer than the manifest as_of plus Restores, with root_version (N126, N145)
}
type SnapshotLister interface {
	List(ctx context.Context, sh Shard, sc id.Scope, q SnapshotQuery) ([]Manifest, string, error) // ExportService.ListSnapshots
	Manifest(ctx context.Context, sh Shard, sc id.Scope, v id.SnapshotVersion) (*Manifest, error)  // GetSnapshotManifest(version ≠ 0), with expired_reason (A-3, A-11)
}
```

The delete marker transaction expires `building` and `ready` snapshots alike
(`SnapshotRepo.ExpireContaining`, `expired_reason`/`expires_at`, the source of `SnapshotManifest.expires_at`); the delta also has a `documents` part keyed by `(document_id, tag_generation)` (N157); the delta is a diff of the two snapshots' id indexes, so a delete
reaches a client even when the base expired. `Invalidate` never expires a snapshot: curation reaches synced
clients through `Streamer.Overlay` now and the next snapshot later. When a delete expired the latest `ready`
snapshot, the expunge's `Materialize` starts a system `ExportSnapshot` (debounced 10 min per namespace, no
`memory.write` caller).

#### 2.2.21 `internal/quota` — rates, token metering, admission

*Pattern: Gate — one `Reserve` in front of every gateway call class (N130).*

```go
package quota

type Limiter interface { Allow(ctx context.Context, k Key, n int64) (Decision, error) } // in-process token buckets (recalls/retains per min); per-process share limit/N_api
type Reserver interface {
	Reserve(ctx context.Context, sc id.Scope, call CallClass, tokens int64) (Reservation, error) // extract | summarize | route | write | refresh | reflect-iteration
}
type Meter interface {
	Record(ctx context.Context, tx txn.Tx, op string, u gateway.Usage, day time.Time) error // exactly-once through usage_key (PD-1); takes the leaf txn handle, so quota does not import store (N140)
	Remaining(ctx context.Context, tx txn.Tx, l Limits, day time.Time) (tokens, facts int64, err error)
}
```

A refused `Reserve` defers a workflow (`DEFERRED`, `resume_at` = the window reset, written through
`store.OperationRepo.Defer`) and returns `RESOURCE_EXHAUSTED` to a synchronous Reflect. A rebuild that finds no
visible source retires the observation without a gateway call and is exempt (N135). Tenant limits are per-namespace shares computed by the
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
method and a trigger rejects both. Inline body ≤ 64 KiB, else the owner-keyed blob `ledger/{ledger_id}`, which dies with its row. Ledger rows are deleted only by
an explicit document, namespace or tenant delete under the admin role (the expunge, §5.4.2), never by a
replace or append retire (N104).

#### 2.2.25 `adapters/mcp` — MCP server

Per-namespace endpoints `/mcp/{tenant_id}/{namespace_id}`; tools generated from `memory.v1` at build
time (`protoc-gen-engram-mcp`) through the checked-in allow-list `adapters/mcp/allow.txt`, so the surface cannot drift from the
API (§4.6 lists the closed set: ten hand-tuned tools, the generated table, and the `omit:` list, which now names `ListNamespaces` and `OperationService.GetOperation`, the latter exposed as the generated `get_operation_status` so that it does not collide with the hand-tuned `get_operation` = `WaitOperation`; N157, N167). *Pattern: **Adapter** over a gRPC *client* of the core — it holds no service and no storage, so it
cannot bypass the interceptor.*

```go
package mcp

type Tool struct { Name, Method string; InputSchema json.RawMessage; Scope authz.Scope; Streaming bool } // generated
// The core clients, split so each interface stays within five methods and every generated tool has a client to call through (A-5):
type DataClients interface { Memory() memoryv1.MemoryServiceClient; Document() memoryv1.DocumentServiceClient; Page() memoryv1.PageServiceClient; Operation() memoryv1.OperationServiceClient }
type AdminClients interface { Namespace() memoryv1.NamespaceServiceClient; Export() memoryv1.ExportServiceClient } // get_namespace, get_effective_config, create_snapshot, get_snapshot_manifest, list_snapshots
type Adapter interface { Tools() []Tool; Handler() http.Handler } // streamable HTTP; the JWT is forwarded unchanged
func New(d DataClients, a AdminClients, tools []Tool, v authz.TokenVerifier, o Options) Adapter
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
| `index [--shard 7]` | the **index runner** daemon (N138): the one process that holds `engram_migrate`; executes `vector_indexes` requests (`requested → building → ready \| failed`, lease) with the statements of `engram_hnsw_ddl` (`CREATE INDEX CONCURRENTLY` per partition, `DROP INDEX`, and per due partition (any index at `max(1 % × rows_at_build, 2 k)` purged) `REINDEX INDEX CONCURRENTLY` of every HNSW with ≥ 0.05 % purged, while archive lag is under 30 s and `pg_wal` headroom covers the burst, then one `VACUUM (INDEX_CLEANUP ON)`, N152, N166), refusing to start a build while any `backend_xmin` is older than 5 min; `--namespace <ns>` requests one by hand |
| `move start [--window 2h] [--not-before …] [--estimate] / status / abort / cleanup` | `MoveService`; `--estimate` prints `W_est` without planning |
| `catalog reconcile --from-shards` | the first step of every catalog promotion and every catalog restore: rebuild move, deleting and epoch state from the shards' ownership rows before the alias flips and the catalog serves writes (N163); `CatalogStandbyDown` pages at 60 s |
| `restore --shard 7 --to …` / `restore replay` | pgBackRest restore, open-move reconcile by the catalog CAS (N125), epoch bump (`catalog epoch + 1`, skipping `committed` moves and `deleting` namespaces), `frozen/restore`, then replay of the delete intents newer than `catalog.shards.replay_floor − 10 min` (lowered by this restore, never raised), verbatim and per subject in chain order, the floor being min(catalog, `_control/restores/` markers) (N122, N123, N134, N163); `restore cleanup-moved-out`; a target restored after a move's catalog CAS is an ordinary restore when its row is `active` and is wiped and re-copied from the retained source when it is `incoming`/`ready` (N161) |
| `report --tenant acme --from … --to …` | per-tenant token and cost report from `token_usage` (N62) |
| `config lint` | checks the `postgres -c` list and role GUCs against the generated table (N131) |
| `outbox trim / skip`, `catalog flush-cache`, `shard stats`, `backup create`, `entity split` (phase 3) | as named |

#### 2.2.28 `internal/intent` — the delete-intent log (N122)

*Pattern: Append-only log in the blob store — an acknowledged delete exists in two places before the ack:
the committed marker and its intent object.*

```go
package intent

type Key string // _control/deletes/{tenant}/{ns}/{deleted_at}-{operation_id}.json

// Intent records the marker's EXACT effect, taken from the store.DeletionRecord that the marker transaction returned.
type Intent struct {
	Kind        Kind                 // document | namespace | tenant | invalidate | restore
	Subject     id.Subject           // typed {Class, Key} (N162): the memory subject is (document_id, content_hash), so a fact and its twins share one chain
	Tenant      id.TenantID; Namespace id.NamespaceID
	Operation   id.OperationID
	Prev        id.OperationID       // the subject's last deletion_log entry read under the document lock: the chain predecessor (zero = first)
	DeletedAt   time.Time
		Epoch       id.Epoch             // the namespace epoch the marker committed under (N143): replay stores THIS recorded epoch (never the shard's current one; applied_epoch is informational) and skips an intent older than an applied entry of its subject (N150)
	UpToVersion id.DocVersion        // document kind: the tombstone's up_to_version, applied verbatim by the replay
	MemoryIDs   []id.FactID          // invalidate and restore kinds: the resolved set the marker hid (every fact of the subject) or removed (the stamped set)
}

type Log interface {
	// Put is put-if-absent under the marker's own name and content (If-None-Match: *). The attempt that committed the marker
	// calls it after the commit and before the ack; a duplicate attempt that finds the subject already deleted calls it with the
	// marker it observed, so an ack always implies an intent and a duplicate never writes a second object. After Put the
	// handler re-reads the marker and acks only if it is still present, else UNAVAILABLE (N143, TestIntent_AckRereadsMarker).
	Put(ctx context.Context, in Intent) (Key, error)
	List(ctx context.Context, ns id.NamespaceID, since time.Time) ([]Intent, error) // in name order; replay order is Order, not name order
		Trim(ctx context.Context, olderThan time.Duration) (int, error)                  // 35 days
	PutRestore(ctx context.Context, shard id.ShardID, target, floor time.Time) error   // _control/restores/{shard}/{restore_target}.json, written before a replay starts (N163)
	RestoreFloor(ctx context.Context, shard id.ShardID) (time.Time, error)             // min over the shard's restore markers; the replay floor is min(catalog.ReplayFloor.Get, this)
}

// HelpPrevious (N159) runs under the subject lock, before the caller's own marker transaction: if the subject's latest deletion_log
// entry (either kind; rebuilt from the row's effect, epoch, operation_id, prev_operation_id and deleted_at) has no intent object, it
// puts it (put-if-absent under that entry's own name). It reports whether it had to put.
func HelpPrevious(ctx context.Context, l Log, latest Intent) (put bool, err error)

// Order returns the replay order: per subject in Prev-chain order (a broken chain starts at its oldest present member),
// subjects in any order. It reads no clock, so no clock-sync bound is needed (C-16, P-13).
func Order(in []Intent) []Intent
```

`Put` follows the marker commit and precedes the ack, and the ack is conditional: **after the put the handler re-reads the
marker and acks only if it is still present**, otherwise it returns `UNAVAILABLE` and the client retries (a restore or
failover between the put and the ack removed the marker; N143, `Durability_AckNoRecheck`). Intents carry the namespace
epoch; **replay skips an intent older than an already-applied entry of the same subject** (`Durability_NoEpochGuard`), and a
repeated `Invalidate` or `Restore` of the same fact writes its own `deletion_log` row and intent. Restore and failover replay a namespace's intents with
`deleted_at ≥ replay_floor − 10 min`, where the floor is `catalog.ReplayFloor` (lowered by every restore, never raised
while intents are retained), applying each **verbatim** — never recomputing `up_to_version` from restored state —
through the admin variant of the marker transaction, idempotently through the shard `deletion_log` (the marker writers of one subject serialise on the subject lock, held until the intent is put, so concurrent curation forms one chain without holes, N150, N159), and skipping
namespace and tenant intents whose catalog row is not `deleting` or `deleted`. **A skipped intent is recorded as settled** (N159): the replay inserts its `deletion_log` row with `replay_outcome = 'skipped'` (an applied one with `'applied'`), and `restore replay` is complete when every in-window intent has a row, so reopening never waits on a skipped one (`TestReplay_SkippedIntentSettled`). A tenant delete writes one intent per
namespace as the workflow fences it, so the per-namespace listing reaches it. *Test seam:* `Durability.tla` model-based
tests; `TestIntent_AckImpliesIntent`, `TestIntent_DuplicateAttempt`, `TestIntent_AckRereadsMarker`, `TestIntent_EpochGuard`, `TestIntent_HelpPrev`, `TestReplay_SkippedIntentSettled`, `TestRestore_ChainOrder`, `TestRestore_FloorInCatalog`.

#### 2.2.29 `internal/expunge` — the asynchronous half of a delete (N119)

*Pattern: a forward-only **workflow** (not a saga: nothing is compensated) — Materialize, Purge, DerivedPurge,
Index and Finish are idempotent, resumable from `expunge_progress`, paced by the WAL they write (≤ 25 MB/s,
measured per batch, not a fixed pause), run at most two per shard and are paused while a move is open; there is
no compensation because the marker already made the delete effective.*

```go
package expunge

type Expunger interface {
	Materialize(ctx context.Context, in *workflowv1.MaterializeInput) (*workflowv1.MaterializeResult, error) // exclusive derivation lock, one 35 s attempt; EVERY batch re-reads fact_hidden and the open tombstones under it (N120); the work list is the marker set (fact_hidden with materialized_at IS NULL, open tombstones), stamped in the same batch, never the signal payload (N145)
	PurgeBatch(ctx context.Context, in *workflowv1.PurgeBatchInput) (*workflowv1.PurgeBatchResult, error)    // waits for every consumer cursor past the tombstone's event_seq; targets MARKERS, CHUNK_TOMBSTONES, REEXTRACTED_FACTS, OLD_EMBEDDING_MODEL; never deletes evidence rows (N135)
	DerivedPurge(ctx context.Context, in *workflowv1.DerivedPurgeInput) (*workflowv1.DerivedPurgeResult, error) // covered observation and page versions → content-free stubs; page markdown and Reflect transcripts deleted (N136)
	PurgeBlobs(ctx context.Context, in *workflowv1.ExpungeInput) (int64, error)                              // owner-keyed blobs with their rows; caches after xcache_grace
	Finish(ctx context.Context, in *workflowv1.ExpungeInput) error                                            // expunge_state 'purged'; tombstone dropped 24 h later; the documents row only if no higher tombstone is open and no version row remains (C-12)
}
type Hygiene interface {
	RequestRebuild(ctx context.Context, in *workflowv1.ExpungeInput) (int, error) // marks every touched HNSW of a due partition (max(1 % × rows_at_build, 2 k) purged on any index) as `requested` when ≥ 0.05 % of its rows were purged since the build (≥ 50 elements), else the partition vacuum repairs it; the index runner rebuilds with REINDEX INDEX CONCURRENTLY, then vacuums the partition once (N138, N152, N166)
}
```

SLA: materialize ≤ 15 min, purge ≤ 24 h, index ≤ 48 h. *Test seam:* `Derivation.tla` and `Durability.tla`
model-based tests; `TestExpunge_Stages` (zero derived rows, vectors, BM25 hits, blobs and transcripts for the
victim); a T3 test that a purge of a 100 k-fact document never exceeds the writer timeout or the WAL budget.

### 2.3 Swappable implementations

Every seam below is an interface of at most five methods chosen once, at wiring time, in a `cmd/*`
composition root. Test seams stay with their module in 2.2; this table is the only place that lists
what can be substituted.

| Seam (package) | Default | Alternates (what for) |
|---|---|---|
| `authz.TokenVerifier` | `JWKSVerifier` | `StaticKeyVerifier` (dev), `AllowAllVerifier` (isolation tests only, never in release images) |
| `catalog.Namespaces` / `NamespaceAdmin` / `Moves` / `MoveStamps` / `Registry` / `Tenants` / `Resolver` | `PostgresCatalog` + `LRUResolver` | `MemoryCatalog` (CAS semantics, `Commit` versus `Rollback` races, epoch-flip faults), `StaticResolver` (single-shard dev) |
| `router.ShardRouter` | `StaticRouter` | `SingleShardRouter` (dev), `RecordingRouter` (cardinality assertions) |
| `gateway.Embedder`, `Structured`, `Chatter`, `Reranker`, `Batcher` | `HTTPClient` | `RecordReplayClient` (golden JSON), `DeterministicClient` (hash embeddings, canned extraction, fault knobs `GW_LATENCY_MS`, `GW_FAIL_RATE`, `GW_BAD_DIMS_RATE`) |
| `blob.Store` | `S3Store` | `FSStore` (dev), `MemStore` (tests, fault knobs); one conformance suite runs against all three |
| `store.Store` / `Stores` | `PgxStore` | `RecordingStore` (records shard and namespace per statement), `FakeTx` (in-memory repositories with the same fencing state machine; a `FakeSession` hands out one `FakeTx` per arm) |
| `workflows.Starter` / `Signaller` / `Waiter` | `TemporalClient` | `FakeClient` (records starts and signals by workflow id) |
| `index.Searcher` | `PostgresIndex` (pg_search BM25 + per-namespace partial HNSW or exact scan) | `TsvectorIndex` (no pg_search), `ExternalIndex` (`Async`, `engram-shard-{id}`), `MemIndex` (brute force, recall unit tests) |
| `chunk.Chunker` | `CDCChunker` | `FixedChunker` (baselines) |
| `extract.Extractor` / `Cache` | `LLMExtractor` + `BlobCache` | `BatchExtractor` (gateway batch API, same cache key), `RuleExtractor` (smoke runs), `MemCache` |
| `entity.Resolver` | `TrigramResolver` | `ExactResolver` |
| `link.Linker` | `BudgetLinker` | `NoSemanticLinker` (embeddings unavailable) |
| `recall.Arm` | semantic, lexical, graph, temporal, chunks | any subset (a budget or a test switches arms off); `MemIndex`-backed arms in unit tests |
| `recall.Fuser`, `Reranker`, `Packer` | `RRFFuser`, `GatewayReranker`, `GreedyPacker` | `NoopReranker` (`stage = FUSED`) |
| `recall.Planner` | the `pipeline.Step` planner (2.2.13) | `FakePlanner` (API handler tests) |
| `consolidate.Router`, `Writer`, `Adjudicator` | `LLMRouter`, `LLMWriter`, `LLMAdjudicator` | `RecordReplay*`, `NoopConsolidator` |
| `reflectagent.Agent` | `LoopAgent` | `ScriptedAgent` (transcript replay) |
| `workflows` activity interfaces | the structs wired in `cmd/engram-worker` | `FakeActivities` (Temporal `testsuite`) |
| `outbox.Sink` | `IndexSink` (a no-op for `Transactional` indexes) | `KafkaSink`, `MemSink` |
| `move.Orchestrator` (in `api.Deps`) | `TemporalOrchestrator` | `InlineOrchestrator` (phases in-process) |
| `export.Builder` / `Streamer` / `SnapshotLister` | blob-backed | `MemStore`-backed |
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
	KindNamespaceFrozen                    // FAILED_PRECONDITION  + memoryv1.NamespaceFrozen{reason: MOVE|RESTORE|UNSPECIFIED}; a delete freeze is KindPreconditionFailed{NAMESPACE_DELETING} (N139)
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
func NotFound(kind memoryv1.ResourceKind, ref fmt.Stringer) *Error // typed (N140); reason DOCUMENT_DELETED for a version a tombstone covers (N136)
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
| `PreconditionFailed` | `FAILED_PRECONDITION` | no | catalog CAS, move cutover, etag and version checks, snapshot expiry, `PAGE_HIDDEN`, `OPERATION_NOT_CANCELLABLE`, `NAMESPACE_DELETING` (types in §4; a second `Invalidate` is a no-op success, not an error) |
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
| Per-shard Postgres pools | pgbouncer aliases on each shard: `engram_app` recall pool **32** (API only), `engram_worker` pool **8** (worker, same role), and the direct `engram_subject` connections (2 per API process, P = 4); `max_connections` 100 holds 63 = 55 + 2P at P = 4 (32 + 8 + subject 2P + admin 2 + move 4 + relay 1 + migrate 1 + runner 2 + exporter 2 + `engramctl` 2 + pgBackRest 1; §9.1) | `router.Options.PoolSize`, pgbouncer config | D3 cell bound |
| Recall arm parallelism | one short read transaction (`ReadSession.Tx`) per concurrently running arm, one pooled connection each; a MID recall holds ≈ 425 connection-ms over six arm transactions (≈ 21 Erlangs at 50 QPS plus ≈ 3 of filtered arms: 24 on a pool of 32, ρ ≈ 0.75); a semaphore admits ≤ 32 concurrent arm transactions per API process per shard, so `N − 1` processes carry a rollout, and a second one admits ≤ 2 filtered arms (pool wait ≈ 20 ms p95 on the 236 ms path; N114, N155, N164, N176); ≤ 50 QPS per shard target | `recall.Planner`, `store.ReadSession` | keeps the pool below saturation (`PoolSaturation` wait p95 < 50 ms) |
| Visibility sets | two indexed selects per recall (`doc_tomb`, `chunk_tomb`; `fact_hidden` is a per-candidate anti-join); marker cap 16 k rows of one kind before the alert; degraded-mode lookup ≤ 20 ms per arm | `store.MarkerReader` | N116, N119 |
| Rerank depth | 0 / 50 / 150 pairs by budget (N53), ≤ 300 per gateway call (D15), one call per recall; skipped below a 106 ms remaining deadline (N106) | `recall.GatewayReranker` | the reranker is a sized dependency |
| Graph arm | two seeded waves × 30 ms, shared node budget 100/300/1000; one statement per hop from Go, the HIGH arm (1,000 nodes, 2 hops) with a 100 ms sub-budget | `recall.Planner`, `store.GraphReader.Hop` | N54 critical path, N165 |
| Vector index plan | no index below 2,000 vectors; partial HNSW created at 2,000 and dropped below 1,000, per namespace and model; `ef_search` = arm cap; semantic arm exact path below θ eligible rows (10 k, M0.6 in [5 k, 20 k]), HNSW iterative scan above with an exact fallback up to 4 θ (≤ 40 k rows); recalls with a vector arm at `E ≥ θ` are the filtered class, p95 ≤ 1 s (N138, N151, N164) | index runner (`engramctl index`), `engram_vector_index_plan`, `index.Searcher.Plan` | N112, N138 |
| `WaitOperation` long-polls | ≤ 2,000 parked on Temporal per API process; beyond: jittered DB poll | `api.Options.MaxOpenWaits` | N70 |
| Temporal history | continue-as-new every 100 chunks or 20 MB; no inline activity result > 4 KiB | `workflows` | N59 |
| Outbox relay | 500 rows per read; one relay per shard; cursor advance ≤ 1 per second per consumer | `outbox.RelayOptions` | D6, N114 |
| Gap watchlist | ≤ 1,000 entries in `outbox_cursors.gaps`, expiring after `2 × statement_timeout` = 60 s | `outbox.GapWatch` | bounded and lossless across relay failover |
| Temporal pollers | 2 workflow + 2 activity pollers per `shard-{id}` queue; max 64 concurrent activities per worker | worker options | D3 |
| Consolidation | route 8 facts per call; ≤ 100 facts per round; one write call per touched observation; ≤ 50 root rebuilds per round; one round in flight per namespace; rebuilds only while markers are pending | `Consolidate` workflow (singleton id), `config.Consolidation` | D12, N121 |
| Expunge | one per namespace, at most two per shard; purge in 1,000-row batches paced to ≤ 25 MB/s of WAL behind the consumer-cursor check; SLAs materialize ≤ 15 min, purge ≤ 24 h, index ≤ 48 h; `ExpungeMaterializeSlow` pages at 900 s | `expunge.Expunger`, `engram_expunge_oldest_pending_seconds` | N119 |
| Reflect | ≤ 10 iterations, ≤ 100 k context tokens, ≤ 300 s, tool deadline 10 s, ≤ 4 concurrent reflects per namespace; one `Reserve` per iteration | `reflectagent.Caps`, api semaphore | D12, N130 |
| Move | freeze, then copy every table from the static source in primary-key ranges of ≤ 100,000 rows, 4 parallel streams, WAL-paced at 25 MB/s; `VerifyFrozen`, the index build and the seal under the freeze; freeze deadline `frozen_at + max(1.5 × W_est, W_est + 10 min)` capped by the operator window (default 4 h, cap 8 h), automatic rollback at the deadline; `SealCopy` before the cut; catalog CAS as the point of no return; ≤ 4 moves per cell, one per namespace; cutover (b″) and (d) retried indefinitely | `move.StartOptions` | N160, N161, N125 |
| Fence acquisition | writers never wait (`pg_try_advisory_xact_lock_shared`, refusal → `NamespaceFrozen{200 ms}`); exclusive takers (fence, document lock, derivation lock) make one attempt with `lock_timeout` 35 s; role defaults 2 s (`engram_app`), 10 s (`engram_move`, `engram_admin`), 30 s statement and idle timeouts on every outbox-writing role | `store.Store.InNamespace`, role defaults | N82: no pooled connection waits behind a queued freeze |
| Derivation lock | shared by every writer of derived versions; exclusive for `Materialize` and `Restore`, one 35 s attempt | `store.Derived` | N120 |
| Subject lock | every marker writer takes the session-level `engram_subject_lock_key(ns, subject)` (subject `class:key`, N162) on a direct `engram_subject` connection before its marker transaction and holds it until the intent put and the marker re-read; polled try-lock ≤ 3 s; help-previous under it | `store.SubjectLocker`, `intent.HelpPrevious` | N150, N159 |
| Per-document lock | `CommitChunk` shared try-lock (`DocumentBusy`, 100 ms); `FinalizeVersion` and the delete marker exclusive, one 35 s attempt | `store.DocumentWriter.Lock` | N83 |
| Outbox event size | ≤ 256 ids and ≤ 16 KiB per event; elided above 4,096 ids | `outbox.Writer`, `outbox.Group` | N80; the CHECK is a backstop |
| Catalog resolver | LRU 100 k entries; single-flight per key; LISTEN reconnect backoff 1 s → 30 s | `catalog.ResolverOptions` | D4 |
| Query embedding LRU | 10,000 entries per process, keyed `(namespace_id, sha256("search_query: " + text))` | `recall.QueryEmbedder` | repeat queries skip the 25 ms hop |
| Streaming / request size | `Recall` batches of 10; `StreamSnapshot` 1 MiB parts; `Retain` ≤ 100 items, ≤ 8 MiB total, ≤ 1 MiB per item, raw body > 64 KiB to blob (N7) | `api` helpers | D10, D12 |

### Round-8 changes

| Register | What changed in this section |
|---|---|
| N179 | `Readier.SealCopy`; `W_est` ≈ 27 min per 1 M |
| N180 | (b″) unconditional on `ready` + `committed`; the end test reads the target row; (d) success `epoch ≥ e_t + 1` |
| N182 | `TenantDelete` fences every namespace, then stamps `acknowledged_at` |
| N184 | `catalog.MoveStamps{Stamp, RecordFloor, RecordReplicated}`; `CleanupMove` is one transition; every non-derived column has one writer; `RollbackMove` waits for replication |
| N185 | `Reconciler` skips unreadable shards (`ReconcileIncomplete`) |
| N186 | connection list 55 + 2P = 63 at P = 4 |
