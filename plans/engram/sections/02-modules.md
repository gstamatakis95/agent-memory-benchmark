## 2. Module breakdown

Conventions used throughout: Go 1.25, module `example.com/engram` (D1). Every interface takes `context.Context`
first and returns `error` last; optional parameters travel in `…Options` structs; ids are strings (UUIDv7 unless
stated); all times are `time.Time` in UTC. Errors are the typed values of `internal/errs` (§2.4). Interfaces are
shown with the methods that matter for the design, not every accessor. Where a signature refers to a generated
type it uses the `memoryv1`, `adminv1`, `workflowv1` and `eventsv1` aliases for `gen/go/memory/v1`,
`gen/go/memory/admin/v1`, `gen/go/engram/internal/workflow/v1` and `gen/go/engram/internal/events/v1`. DDL is in
§3, protos in §4, pipelines in §5.

### 2.1 Package layout (D14) and dependency rule

```
cmd/
  engram-api/            main: wires authz → catalog → router → services → grpc + connect mux
  engram-worker/         main: Temporal worker per cell shard list, outbox relays, move executor
  engram-mcp/            main: MCP server over adapters/mcp, thin gRPC client to engram-api
  engramctl/             main: cobra CLI for provisioning, migrations, moves, backups
proto/
  memory/v1/             public API: MemoryService, DocumentService, NamespaceService, OperationService, ExportService, PageService, errors.proto
  memory/admin/v1/       admin API: ShardService, MoveService, TenantService
  engram/internal/workflow/v1/   Temporal inputs/results/signals (never served)
  engram/internal/events/v1/     outbox / Kafka Event envelope (never served)
gen/go/...               buf generate output (protoc-gen-go, -go-grpc, -connect-go); committed
internal/
  errs/                  typed errors + gRPC/Connect mapping (leaf; new decision N1)
  api/                   gRPC service implementations, deadline + request_id enforcement, streaming helpers
  authz/                 JWT verification, RequestScope, unary/stream/Connect interceptors, method policy
  catalog/               Catalog (control-plane Postgres) + Resolver (LRU + LISTEN/NOTIFY)
  router/                ShardRouter: scope → ShardHandle; cross-cell forwarding
  gateway/               AI gateway client: chat/structured, embed, rerank, batch; limits, cost, usage hooks
  blob/                  Blob store client, prefix scoping, content-addressed keys, tombstones
  store/                 per-shard Postgres: pools, WithNamespaceTx fencing, repositories
  index/                 Index interface; PostgresIndex (HNSW + pg_search), TsvectorIndex, ExternalIndex
  chunk/                 heading-anchored content-defined chunker; chunk header builder
  extract/               Extractor (structured LLM), prompt versions, ExtractionCache, schema types
  entity/                per-namespace fuzzy entity resolution (pg_trgm + aliases), merge policy
  link/                  Linker: entity, temporal, semantic kNN, causal links; LinkBudget
  recall/                Arms, Fuser (RRF), Reranker, Booster, Packer, Planner
  consolidate/           Consolidator: batches, candidates, LLM ops, validator, bisect, applier
  reflect/               bounded agent loop, tool registry, citation verifier, schema validator
  pages/                 PageService core: sources, staleness, delta refresh
  workflows/             Temporal workflows + activity interfaces + retry policies + queue naming
  outbox/                Relay (advisory-lock election, cursor, gap watchlist) + Sink implementations
  move/                  MoveOrchestrator: D5 phases as workflow + fenced activities; `replay_map.go` = the total event → rows table generated from `events.proto` annotations (N81)
  export/                SnapshotBuilder + Streamer
  quota/                 Limiter (rates), Meter (tokens), Deferral
  config/                layered config system → tenant → namespace; typed Resolved struct
  telemetry/             OTel tracing + Prometheus conventions; label policy linter
  ledger/                append-only ingest ledger writer
adapters/
  mcp/                   MCP tool definitions derived from protos; per-namespace endpoints; scope gating
  connect/               ConnectRPC handlers mounted on the same mux as gRPC
formal/
  tla/*.tla              Outbox, ShardMove, DocLifecycle, Consolidation, AsOf specs (§7)
  lean/Engram/*.lean     TagMatch, RRF, Packer, TemporalWindow proofs (§7)
```

**Dependency rule** (enforced by `go vet` + a `depguard` config in CI; a violation fails the build):

| Layer | May import | Must not import |
|---|---|---|
| `internal/api` | `internal/{authz,router,recall,pages,export,quota,store,workflows(client only),errs,telemetry}`, `gen/go` | adapters, `cmd` |
| Services (`recall`, `consolidate`, `reflect`, `pages`, `export`, `move`, `entity`, `link`, `extract`, `chunk`) | `internal/{store,index,gateway,blob,config,quota,ledger,errs,telemetry}`, `gen/go` | `internal/api`, adapters, `internal/workflows` |
| Infrastructure (`store`, `index`, `gateway`, `blob`, `catalog`, `router`, `outbox`, `ledger`, `config`, `quota`, `telemetry`) | `internal/errs`, `gen/go`, third-party drivers | any service package, `internal/api` |
| `internal/workflows` | activity **interfaces** declared in `internal/workflows` itself, `gen/go/engram/internal/workflow/v1`, Temporal SDK | concrete service packages — those are injected into activity structs in `cmd/engram-worker` |
| `adapters/*` | `gen/go` (+ generated Connect/gRPC clients), `internal/authz` (scope names only), `internal/errs` (code mapping) | any other `internal/*` |
| `internal/errs` | `gen/go/memory/v1` (detail messages), `google.golang.org/grpc/status`, `connectrpc.com/connect` | anything under `internal/` |
| `cmd/*` | everything (composition roots) | — |

Rationale: the rule keeps `internal/api` a thin translation layer, lets every service be tested with fakes for
store/index/gateway, and guarantees the MCP/Connect adapters cannot bypass the interceptor because they only
hold a *client* to the core. Rejected: letting adapters import services directly (one more enforcement point to
audit, D13).

### 2.2 Modules

#### 2.2.1 `internal/api` — gRPC servers and Connect handlers

**(a) Responsibility.** Implements every `memory.v1` / `memory.admin.v1` service interface generated by
`protoc-gen-go-grpc`; the Connect handlers are generated by `protoc-gen-connect-go` from the same protos and
delegate to the *same* Go implementation (one struct satisfies both generated interfaces because both use the
generated request/response types). Enforces cross-cutting rules: deadline present and clamped, `request_id`
idempotency for unary writes, page tokens, field masks, error translation via `errs`.

**(b) Interfaces.**

```go
package api

// Server bundles the service implementations registered on the gRPC server and the mux.
type Server struct {
	Memory memoryv1.MemoryServiceServer
	Document memoryv1.DocumentServiceServer
	Namespace memoryv1.NamespaceServiceServer
	Operation memoryv1.OperationServiceServer
	Export memoryv1.ExportServiceServer
	Page memoryv1.PageServiceServer
	Admin AdminServers                        // ShardService, MoveService, TenantService
}
type Options struct {
	MaxDeadline map[string]time.Duration // full method → cap (N11: default 30 s, Recall 10 s, Retain 30 s, Reflect 330 s, WaitOperation 65 s, StreamSnapshot 600 s); over the cap is INVALID_ARGUMENT, never clamped
	DefaultPageSize int32                // 100 (§4.1.4)
	MaxPageSize int32                    // 1000 (200 for ListMemories with text in the mask)
	RequestIDTTL time.Duration           // 24 h (D1)
	MaxOpenWaits int                     // 2 000: WaitOperation long-polls parked on Temporal per process; above it the handler falls back to a jittered DB poll (N70)
	ReflectionServices []string          // gRPC reflection exposes these only — ["memory.v1"], never the admin surface (N71, review F-42)
}

// Deps are the seams the servers use; every field is an interface.
type Deps struct {
	Router router.ShardRouter
	Recall recall.Planner
	Retainer RetainSubmitter  // ledger + operation + StartWorkflow (§1.4)
	Deleter DeleteCascader    // synchronous cascade (§1.6)
	Ops OperationReader       // operations table + Temporal describe
	Temporal workflows.Client // StartWorkflow / SignalWithStart / Cancel
	Pages pages.Service
	Export export.Streamer
	Clock func() time.Time
}

func NewServer(d Deps, o Options) *Server
func (s *Server) RegisterGRPC(g *grpc.Server)
func (s *Server) RegisterConnect(mux *http.ServeMux, interceptors ...connect.Interceptor)

// RetainSubmitter is the API-side half of retain (D16): durable ack, then workflow start.
type RetainSubmitter interface {
	Submit(ctx context.Context, scope authz.RequestScope, req *memoryv1.RetainRequest) (*memoryv1.Operation, error)
}

// DeleteCascader performs the synchronous part of D8 in one namespace transaction.
type DeleteCascader interface {
	DeleteDocument(ctx context.Context, scope authz.RequestScope, documentID string, opts DeleteOptions) (*memoryv1.Operation, error)
}

// Idempotency guards unary writes by request_id (D1) over store.IdempotencyRepo: Begin returns the stored
// response when the key was seen in the last 24 h; Commit stores it in the same tx as the write.
type Idempotency interface {
	Begin(ctx context.Context, tx store.Tx, requestID, method string, reqHash [32]byte) (stored proto.Message, seen bool, err error)
	Commit(ctx context.Context, tx store.Tx, requestID string, resp proto.Message) error
}

// RequestHash is the one request-hash function (idempotency keys, ledger, Temporal inputs): SHA-256 over the
// NORMALISED protojson of the request with `meta` cleared — keys sorted, unknown fields dropped, defaults
// omitted, canonical Timestamp/Duration/int64 rendering (N72). Protobuf binary has no canonical form
// (Deterministic only orders maps; SDK field order, unknown fields and Struct number formatting all change the
// bytes), so hashing bytes made a retry through a newer SDK fail with IDEMPOTENCY_KEY_REUSED (review F-43).
func RequestHash(m proto.Message) [32]byte

// OperationWaiter implements WaitOperation (N70, review F-39): the call is parked on Temporal's
// workflow-result long-poll (client.GetWorkflow(id).Get with the remaining wait), one open poll per waiter and
// at most Options.MaxOpenWaits per process; beyond the cap, or when the workflow is gone (purged namespace,
// catalog-served DELETE_NAMESPACE operations), it polls the operations row every 1 s ± 250 ms. No per-shard
// LISTEN (impossible through pgbouncer, N15).
type OperationWaiter interface {
	Wait(ctx context.Context, scope authz.RequestScope, operationID string, timeout time.Duration) (op *memoryv1.Operation, timedOut bool, err error)
}

// DeadlineGuard is the unary/stream interceptor that rejects missing deadlines and deadlines over the per-method cap (N11).
func DeadlineGuard(o Options) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor)
```

Streaming helpers: `StreamBatcher[T]` groups results into batches of 10 and flushes on deadline pressure
(`Recall`), `TokenStreamer` for `Reflect`, and `PartStreamer` (1 MiB parts) for `StreamSnapshot`. A `request_id`
collision with a different request hash returns `ALREADY_EXISTS` + `OperationConflict{IDEMPOTENCY_KEY_REUSED}` (D1;
a client reusing keys is a bug, not a retry).

**(c) Dependencies.** `authz`, `router`, `recall`, `pages`, `export`, `quota`, `store`, `workflows` (client
interface), `errs`, `telemetry`, `gen/go`.

**(d) Swappable.** `api.Server` over real deps (production) → over `store.FakeTx` + `recall.FakePlanner` +
`workflows.FakeClient` (handler unit tests) → Connect-only mount (local dev for `engram-mcp`).

**(e) Test seam.** `Deps` is all interfaces; tests use `bufconn` for gRPC and `httptest` for Connect with the
same `Server`, asserting byte-identical error details across both transports (golden files per method).

#### 2.2.2 `internal/authz` — the single enforcement point

**(a) Responsibility.** Verify the JWT, resolve the namespace, check tenant ownership and allowlist, check
scopes, apply rate quotas, and place `RequestScope` in the context (D13). One implementation serves gRPC unary,
gRPC stream and Connect. MCP and Connect never make authz decisions: they forward the JWT.

**(b) Interfaces.**

```go
package authz

type Scope string

const (
	ScopeMemoryRead  Scope = "memory.read"
	ScopeMemoryWrite Scope = "memory.write"
	ScopeMemoryAdmin Scope = "memory.admin"
	ScopeTenantAdmin Scope = "tenant.admin"
)

type Claims struct {
	Subject string
	TenantID string
	Namespaces []string      // `ns`: namespace ids, or ["*"] (small cases)
	NamespaceGroups []string // `ns_group`: tenant-defined groups; a namespace carries one group, so a per-user-namespace platform needs one claim, not 10⁴ ids in 8 KB of metadata (N65, review F-29)
	Scopes []Scope
	ExpiresAt time.Time
	KeyID string
}

// TokenVerifier verifies signature, expiry, issuer and audience; it does not consult the catalog.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (*Claims, error)
}

// RequestScope is what every downstream component reads; it is immutable once attached.
type RequestScope struct {
	TenantID string
	NamespaceID string
	ShardID int32
	Epoch int64
	Scopes []Scope
	Subject string
	RequestID string
	Config *config.Resolved // resolved system→tenant→namespace (D12), cached with the entry
}

func (s RequestScope) Has(sc Scope) bool
func (s RequestScope) StoreScope(mode store.AccessMode) store.Scope

// MethodPolicy says, per full method name, what a call needs.
type MethodPolicy struct {
	Scope Scope
	Target Target            // TargetNamespace | TargetTenant | TargetAdmin
	RateBucket quota.Bucket  // quota.BucketRecall | quota.BucketRetain | quota.BucketNone
	AllowDeleting bool       // true only for OperationService.GetOperation/WaitOperation: a namespace in state `deleting` is otherwise FAILED_PRECONDITION, but the delete operation itself must stay awaitable (N70, review F-27)
}

type Policy map[string]MethodPolicy // key: "/memory.v1.MemoryService/Recall"

type Interceptor struct { /* verifier, resolver, limiter, policy, clock, metrics */ }

func NewInterceptor(v TokenVerifier, r catalog.Resolver, l quota.Limiter, p Policy, o InterceptorOptions) *Interceptor

func (i *Interceptor) Unary() grpc.UnaryServerInterceptor
func (i *Interceptor) Stream() grpc.StreamServerInterceptor // authz at stream open; scope fixed for the stream's life
func (i *Interceptor) Connect() connect.Interceptor          // same decisions, Connect error codes via errs

func FromContext(ctx context.Context) (RequestScope, bool)
func Require(ctx context.Context, sc Scope) error // PERMISSION_DENIED with ErrorInfo{reason:"MISSING_SCOPE"}

// NamespaceCarrier is implemented by every namespace-scoped request message (generated).
type NamespaceCarrier interface {
	GetTenantId() string
	GetNamespaceId() string
}
```

`Verify` failures map to `UNAUTHENTICATED`; a namespace of another tenant maps to `NOT_FOUND` (deliberate,
§1.3 step 5: no existence oracle); a same-tenant namespace outside the allowlist — neither `"*"`, nor its id in
`ns`, nor its group in `ns_group` (N65) — or a missing scope, maps to `PERMISSION_DENIED`; a namespace in state
`deleting` maps to `FAILED_PRECONDITION` except for methods whose policy sets `AllowDeleting` (N70). For streams, the scope is fixed
at open — a token expiring mid-stream does not abort the stream (Reflect can run 300 s; rejected: re-verifying
per message, which would turn token expiry into partial results).

**(c) Dependencies.** `catalog` (Resolver), `quota` (Limiter), `config`, `errs`, `telemetry`;
`github.com/lestrrat-go/jwx/v3` for JWKS/JWT (A-6).

**(d) Swappable.** `JWKSVerifier` (EdDSA/RS256, JWKS cached 5 min; production) → `StaticKeyVerifier` (local dev,
integration tests) → `AllowAllVerifier` (claims from a header; isolation tests only, never built into release images).

**(e) Test seam.** Table tests over `(claims, method, catalog entry) → code`, plus the cross-tenant matrix (§8):
every method × {other tenant, other namespace, missing scope, expired} must produce the expected code on gRPC,
Connect and MCP.

#### 2.2.3 `internal/catalog` — control plane and resolver cache

**(a) Responsibility.** Own the `engram_catalog` tables (D4) and expose the transactional operations the API,
`engramctl` and the move workflow need; provide the in-process `Resolver` (LRU 100 k, TTL 60 s, negative 5 s,
`LISTEN catalog_changes`; when the catalog is unreachable, entries of **existing namespaces are served
indefinitely** with a `catalog_stale` gauge — the shard fence makes staleness safe, so expiring them only
converted a catalog outage into a fleet outage; negative entries expire after 10 min; only a miss fails
`UNAVAILABLE` — D4 as amended, review F-21).

**(b) Interfaces.**

```go
package catalog

type NamespaceState string // "creating" | "active" | "moving" | "frozen" | "restoring" (N64) | "deleting" | "deleted"
type ShardState string     // "provisioning" | "active" | "full" | "draining" | "readonly" | "retired" — one-to-one with sql/catalog_schema.sql and adminv1.ShardState (review F-27)

type Entry struct {
	NamespaceID string
	TenantID string
	Name string
	ShardID int32
	Epoch int64
	State NamespaceState
	Config config.Layer               // namespace JSONB layer
	tenant layer is in TenantEntry */
	Tenant *TenantEntry
	ResolvedAt time.Time
}

type TenantEntry struct { TenantID, Isolation string; Config config.Layer; Quotas quota.Limits } // Isolation: "shared" | "dedicated"

type Shard struct { ShardID int32; Cell, DSN, BlobCredentialRef string; State ShardState; DedicatedTenantID string; LiveFacts int64 }
type CreateNamespaceParams struct { TenantID string; Name string; Config config.Layer; ShardHint int32 /* 0 = placement policy decides (least-loaded active shard honouring isolation) */ }

type EpochReason string // "move_cutover" | "restore_from_backup"

type Catalog interface {
	ResolveNamespace(ctx context.Context, namespaceID string) (*Entry, error)
	ResolveNamespaceByName(ctx context.Context, tenantID, name string) (*Entry, error)
	CreateNamespace(ctx context.Context, p CreateNamespaceParams) (*Entry, error) // also inserts namespace_ownership(active, epoch 1) on the shard via ShardBootstrapper
	SetNamespaceState(ctx context.Context, namespaceID string, from, to NamespaceState) error
	BumpEpoch(ctx context.Context, namespaceID string, expected int64, reason EpochReason) (newEpoch int64, err error)
	ListNamespacesByShard(ctx context.Context, shardID int32, page Page) ([]*Entry, string, error)

	// Moves (D5); every transition is CAS on the current state.
	PlanMove(ctx context.Context, namespaceID string, targetShard int32) (*Move, error)
	AdvanceMove(ctx context.Context, moveID string, from, to MoveState) error
	Cutover(ctx context.Context, p CutoverParams) error // D5 step 6(d) only — the catalog switch (namespaces.shard_id/epoch/state, NOTIFY) after both ownership rows were flipped by the move activities (N45)
	RollbackMove(ctx context.Context, moveID string, reason string) error

	// Shards and tenants (admin surface; details in §9): RegisterShard, SetShardState, ListShards, GetTenant, …
	RegisterShard(ctx context.Context, s Shard) error
	GetTenant(ctx context.Context, tenantID string) (*TenantEntry, error)

	// Events yields decoded catalog_changes notifications until ctx ends.
	Events(ctx context.Context) (<-chan ChangeEvent, error)
}

type ChangeEvent struct { NamespaceID string; ShardID int32; Epoch int64; State NamespaceState }

// Resolver is the hot-path cache. It never blocks on the catalog when it has a fresh entry.
type Resolver interface {
	Resolve(ctx context.Context, namespaceID string) (*Entry, error)
	ResolveFresh(ctx context.Context, namespaceID string) (*Entry, error) // bypass cache (used after WrongShardOrEpoch)
	Invalidate(namespaceID string)
	Run(ctx context.Context) error // LISTEN loop; full flush on reconnect
}

type ResolverOptions struct {
	MaxEntries int             // 100_000
	TTL time.Duration          // 60 s: refresh interval while the catalog answers
	NegativeTTL time.Duration  // 5 s; a negative entry is kept at most 10 min when the catalog is down
	StaleMax time.Duration     // 0 = unbounded (default, D4 as amended): an existing entry is never evicted for age while the catalog is unreachable; `engram_catalog_stale_seconds` reports the oldest entry's age
}
```

**(c) Dependencies.** `config`, `quota` (types only), `errs`, `telemetry`, `pgx/v5`, `hashicorp/golang-lru/v2`
(A-7).

**(d) Swappable.** `PostgresCatalog` + `LRUResolver` (production) → `MemoryCatalog` (map + broadcast channel;
authz/router/move unit tests, fault injection that flips epochs under a request) → `StaticResolver` (file-backed;
single-shard dev mode and `engramctl` offline commands).

**(e) Test seam.** `MemoryCatalog` implements the same CAS semantics; a rapid property test drives random
`PlanMove/Advance/Cutover/Rollback` sequences and checks the state machine of D5 (no `cutover` without prior
`frozen`; at most one active shard per namespace at any point).

#### 2.2.4 `internal/router` — scope → shard handle

**(a) Responsibility.** Map a `RequestScope` (or a workflow input) to the concrete resources of its shard, all
pre-built at process start for the cell's shard list; forward to another cell when the shard is not local (phase
3).

**(b) Interfaces.**

```go
package router

// ShardHandle is the only way code obtains shard-bound resources.
type ShardHandle struct {
	ShardID int32
	Cell string
	Pool store.Pool                                           // pgbouncer pool, 16 conns per process (D3)
	Blob blob.Store                                           // Scoped to "{shard}/"
	BlobPrefix func(tenantID, namespaceID string) blob.Prefix
	Index index.Index                                         // PostgresIndex over Pool, or ExternalIndex "engram-shard-{id}"
	TaskQueue string                                          // "shard-{id}"
	Metrics telemetry.ShardLabels
}

type ShardRouter interface {
	// For returns the local handle for scope.ShardID or errs.ShardNotLocal{Cell}.
	For(ctx context.Context, scope authz.RequestScope) (*ShardHandle, error)
	// ForShard is used by workers, which carry shard_id in workflow inputs (D4).
	ForShard(shardID int32) (*ShardHandle, bool)
	// Forward proxies a call to the cell that owns the shard with identical metadata and remaining deadline.
	Forward(ctx context.Context, cell string, fullMethod string, req, resp proto.Message) error
	Local() []int32
}

type Options struct { Cell string; PoolSize int32 /* 16 */; Forwarder Forwarder /* nil in MVP → ShardNotLocal */ }

// Forwarder is the cross-cell hop (phase 3): one grpc.ClientConn per remote Envoy. Unary methods are proxied
// with Invoke; the three server-streaming methods (Recall, Reflect, StreamSnapshot) with Stream, which opens a
// grpc.ClientStream on the remote cell and relays messages and the trailing status verbatim (N71, review F-35).
// The hop adds `engram-forward-hops: 1` and is never re-forwarded (N17); the EXTERNAL Envoy listener strips
// every `engram-forward-*` header from client traffic, so the hop count cannot be spoofed (N71, review F-42).
type Forwarder interface {
	Invoke(ctx context.Context, cell, fullMethod string, req, resp proto.Message) error
	Stream(ctx context.Context, cell, fullMethod string, desc *grpc.StreamDesc, req proto.Message) (grpc.ClientStream, error)
}

// Retry executes fn with the D2/D5 retry contract: on WrongShardOrEpoch one re-resolve for writes; on
// namespace_state = MOVED_OUT (the cutover (c)–(d) window, §5.5) it FIRST follows the detail's target hint
// (target_shard_id, next_epoch from the `moved_out` ownership row, N98) straight to the target shard, using no
// catalog at all, and verifies ownership at the target's fence; a READ that cannot follow the hint re-resolves
// in a bounded loop of ≤ 5 s with jittered 50 → 500 ms backoff, like NamespaceFrozen (N52, review F-20); bounded
// backoff (≤ 30 s) on NamespaceFrozen for writes, including the N82 FENCE_BUSY refusal (200 ms). The hint fields
// are stripped before an error is returned to a caller outside the API. FAILED_PRECONDITION surfaced during a move is counted against the availability
// SLI by the telemetry hook.
func Retry(ctx context.Context, r catalog.Resolver, sr ShardRouter, scope authz.RequestScope, mode store.AccessMode, fn func(ctx context.Context, h *ShardHandle, scope authz.RequestScope) error) error
```

`Retry` is deliberately a function rather than middleware: only handlers that are safe to re-execute call it
(all of ours are, because writes are idempotent by `request_id`); the re-resolved scope is passed to `fn` so the
retry uses the new `epoch`.

**(c) Dependencies.** `store`, `blob`, `index`, `catalog`, `authz` (types), `errs`, `telemetry`.

**(d) Swappable.** `StaticRouter` (handles built from the catalog `shards` rows for this cell at boot, re-read
on admin RPC) → `SingleShardRouter` (dev mode, service unit tests) → `RecordingRouter` (records every `ShardID`
a request touched; isolation tests).

**(e) Test seam.** `RecordingRouter` plus `store.RecordingPool` produce a trace of `(request_id, shard_id,
namespace_id)` triples; the cross-shard isolation suite (§8) asserts the set of shards per request has
cardinality 1 and equals the catalog's answer.

#### 2.2.5 `internal/gateway` — AI gateway client

**(a) Responsibility.** The only HTTP client to the AI gateway. Structured chat, embeddings (unary and batch),
rerank, batch jobs; retries with jittered backoff on 429/5xx; per-model token-bucket rate limiting; token/cost
accounting hooks; a cost table per model.

**(b) Interfaces.**

```go
package gateway

type Model string

type Usage struct { Model Model; PromptTokens int; CompletionTokens int; CostMicros int64 /* from CostTable; 0 if unknown */ }
type StructuredRequest struct {
	Model Model
	System string
	User string
	Schema []byte          // JSON schema the gateway enforces
	Temperature float32
	MaxTokens int
	PromptID string        // "extract/v1" — for tracing and the cache key
	Tags map[string]string // tenant/namespace for gateway-side attribution (never logged with content)
}
type EmbedRequest struct { Model Model; Text string /* already prefixed by the caller ("search_document: " / "search_query: ") */; Dims int /* 768; 512 when Matryoshka truncation is enabled */ }
type EmbedBatchRequest struct { Model Model; Texts []string /* ≤ 64 */; Dims int }
type RerankRequest struct { Model Model; Query string; Docs []string /* ≤ 300 (D15) */; TopN int }
type ChatRequest struct { Model Model; Messages []Message; Tools []ToolSpec; MaxTokens int; Stream bool } // reflect

type Client interface {
	ChatStructured(ctx context.Context, req StructuredRequest, out any) (*Usage, error) // decodes JSON into out; ValidationError on schema mismatch
	Chat(ctx context.Context, req ChatRequest, sink ChatSink) (*Usage, error)          // streaming tokens + tool calls (reflect)
	Embed(ctx context.Context, req EmbedRequest) ([]float32, *Usage, error)             // L2-normalised by the client
	EmbedBatch(ctx context.Context, req EmbedBatchRequest) ([][]float32, *Usage, error)
	Rerank(ctx context.Context, req RerankRequest) ([]RerankScore, *Usage, error)
	SubmitBatch(ctx context.Context, req BatchJobRequest) (*BatchJob, error)              // backfills; ~50 % cheaper
	PollBatch(ctx context.Context, jobID string) (*BatchJobStatus, error)
	BatchResults(ctx context.Context, jobID string) iter.Seq2[BatchResult, error]
}

type RerankScore struct{ Index int; Score float32 }

type UsageHook func(ctx context.Context, u Usage) // quota.Meter and telemetry each register one

// RateLimiter is per model; the client blocks (bounded by ctx) rather than failing.
type RateLimiter interface {
	Acquire(ctx context.Context, m Model, tokensEstimate int) (release func(), err error)
}

type CostTable map[Model]struct{ PromptPerMTok, CompletionPerMTok int64 } // micros

type Options struct {
	BaseURL string
	Retry RetryPolicy                          // 429/502/503/504: 100 ms → 5 s, ≤ 5 attempts
	other 4xx: no retry → PermanentLLMError */
	Limits map[Model]Limit                     // RPM/TPM per model
	Costs CostTable
	Hooks []UsageHook
}

func New(o Options) Client
```

Error contract: HTTP 429/5xx → `errs.Unavailable` (retryable); 400/404/413/422 → `errs.PermanentLLMError{Status,
Model}` (activities treat it as non-retryable and, for extraction, record the chunk hash and reason in
`operations.error`; the operation finishes `SUCCEEDED` with `progress.units_failed > 0`, N35). Rationale: a schema-rejecting model will
not succeed on retry; a rate-limited one will.

**(c) Dependencies.** `errs`, `telemetry`, `quota` (Meter via hook), `net/http`.

**(d) Swappable.** `HTTPClient` (production) → `RecordReplayClient` (golden JSON per prompt hash, records on
`-update`; extractor/consolidator/reflect tests without network) → `DeterministicClient` (hash-based embeddings,
canned extraction; integration/e2e with fault knobs `GW_LATENCY_MS`, `GW_FAIL_RATE`, `GW_BAD_DIMS_RATE`).

**(e) Test seam.** `Client` is an interface; the record/replay client keys on `sha256(model ‖ prompt id ‖
input)` so prompt changes invalidate goldens visibly.

#### 2.2.6 `internal/blob` — blob store, prefix scoping, content addressing

**(a) Responsibility.** Put/Get/Delete/List by key with prefix-scoped credentials; content- addressed helpers;
tombstone deletes (a delete marker written before the object is removed, so a crashed purge can resume and so
exports can honour deletes).

**(b) Interfaces.**

```go
package blob

type Prefix struct { ShardID int32; TenantID string; NamespaceID string }

func (p Prefix) String() string // "7/acme/018f…/"
func (p Prefix) Key(parts ...string) string

type Kind string // "ledger" | "xcache" | "docsum" | "consolidate" | "pages" | "export" | "staging" | "ver" (§3.6; "ver" = the full reconstructed body of a document version, N104)

type ObjectInfo struct { Key string; Size int64; ETag string; SHA256 [32]byte; LastModified time.Time; Metadata map[string]string }
type PutOptions struct { ContentType string; Metadata map[string]string; IfNoneMatch bool /* content-addressed puts: skip if present */ }
type ListPage struct { Cursor string; Limit int }
type ListResult struct { Objects []ObjectInfo; NextCursor string }

type Store interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, o PutOptions) (*ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error)
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, page ListPage) (*ListResult, error)
}

// Scoped returns a Store that rejects any key not under p (errs.Validation) and
// authenticates with the shard's prefix-scoped credential.
func Scoped(root Store, p Prefix, cred Credential) Store

// ContentKey builds "{prefix}{kind}/{hex sha256}[.ext]".
func ContentKey(p Prefix, k Kind, sum [32]byte, ext string) string

// Tombstones make purges resumable: the logical delete inserts a blob_tombstones row (§3.3.7) in its own
// transaction; PurgeBlobs deletes the object and then the row, so a crash leaves a worklist, never a leak.
type Tombstoner interface {
	MarkDeleted(ctx context.Context, tx store.Tx, key, reason string) error
	PurgeMarked(ctx context.Context, tx store.Tx, limit int) (int, error)
}
```

`PurgeDocument` deletes `xcache`/`ecache`/`staging` objects only when no live chunk references the hash **and** the
object is older than `xcache_grace = 24 h` (`Store.Head` supplies `LastModified`; N100) — a purge never races an
in-flight activity, and a `CommitChunk` that still meets a missing blob fails with `InputBlobMissing` (§5.1.2).
Key layout (§3.6 has the full table): `{shard}/{tenant}/{ns}/ledger/{sha256}` for raw bodies > 64 KiB (N7),
`…/xcache/{sha256(chunk_hash‖prompt_version‖model‖schema_version‖render_hash)}.json` (D11, N87), `…/ver/{sha256}`
(version body, N104), `…/docsum/…`, `…/pages/{page_id}/v{n}.md`,
`…/export/v{n}/…` (D12), `…/staging/…` (N13); pgBackRest repositories live under `_backups/shard-{id}/`, outside
every tenant prefix (N23).

**(c) Dependencies.** `errs`, `telemetry`; the blob store SDK (A-8: S3-compatible API).

**(d) Swappable.** `S3Store` (production) → `FSStore` (local dev, `engramctl` offline) → `MemStore` (map with
fault knobs; unit tests and move/export tests that assert exact key sets).

**(e) Test seam.** A conformance suite (`blobtest.RunSuite(t, newStore)`) runs against all three; `Scoped` has
its own tests asserting prefix escapes (`../`, absolute keys, other shard) are rejected.

#### 2.2.7 `internal/store` — per-shard Postgres, fencing, repositories

**(a) Responsibility.** Everything that touches a shard database: pools, the fenced transaction wrapper, and one
repository per table family. No SQL exists outside this package and `internal/index` (search predicates). The
store never issues a statement without an active `Scope`.

**(b) Interfaces.**

```go
package store

type AccessMode int

const (
	Read  AccessMode = iota + 1 // plain SELECT: ownership state IN ('active','frozen'), epoch ignored, no lock
	Write                       // fence prelude: engram_try_ns_fence(namespace_id) = pg_try_advisory_xact_lock_shared over engram_ns_lock_keys (never waits, N82; refused → NamespaceFrozen{FENCE_BUSY, 200 ms}) then a plain SELECT state, epoch; requires state = 'active' AND epoch = scope.Epoch (D2 as amended)
)

type Scope struct { TenantID string; NamespaceID string; Epoch int64; Mode AccessMode }
type Pool interface { Acquire(ctx context.Context) (Conn, error); Stats() PoolStats; Close() }

// Tx is a namespace-fenced transaction; every repository method requires one.
type Tx interface {
	Scope() Scope
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	Documents() DocumentRepo
	Chunks() ChunkRepo
	Facts() FactRepo
	Links() LinkRepo
	Entities() EntityRepo
	Observations() ObservationRepo
	Pages() PageRepo
	Operations() OperationRepo
	Outbox() OutboxRepo
	Ledger() LedgerRepo
	Idempotency() IdempotencyRepo
	TokenUsage() TokenUsageRepo
	Ownership() OwnershipRepo // read-only for engram_app; writes are admin/move-only
	DocLocks() DocLockRepo    // per-document advisory lock (N83)
	Consolidation() ConsolidationRepo // fact_consolidation + consolidation_state (N95)
}

// DocLockRepo is the per-document lock of N83, one key form `engram_doc_lock_keys(namespace_id, document_id)` (the two
// int4 keys hashtext(namespace_id), hashtext(document_id), N103). CommitChunk uses TryShared and maps a refusal to the
// retryable errs.DocumentBusy (100 ms); FinalizeVersion, the delete cascade and PurgeDocument use Exclusive under
// lock_timeout = 5 s. The ack's version assignment takes no advisory lock (documents row FOR UPDATE only, N56).
type DocLockRepo interface {
	TryShared(ctx context.Context, documentID string) error // errs.DocumentBusy on refusal
	Exclusive(ctx context.Context, documentID string) error // pg_advisory_xact_lock under lock_timeout 5 s
}

type TxOptions struct { StatementTimeout time.Duration /* 30 s writers (= idle_in_transaction_session_timeout, A-F1: the D6 gap watch and the D5 copy barrier depend on it), 5 s recall */; Isolation pgx.TxIsoLevel; ReadOnly bool }

// WithNamespaceTx runs fn inside a transaction that (1) SET LOCALs engram.namespace_id,
// engram.tenant_id, engram.epoch and statement_timeout, (2) runs the FENCE PRELUDE for scope.Mode —
// for Write: `SELECT engram_try_ns_fence($ns)`, i.e. `pg_try_advisory_xact_lock_shared` over
// engram_ns_lock_keys (held to commit, so the move's barrier/freeze, the delete freeze and a restore, which take
// the EXCLUSIVE advisory lock under lock_timeout 5 s, wait for every in-flight writer; a writer that arrives
// while an exclusive request holds or is queued is REFUSED AT ONCE and the statement ends with the retryable
// NamespaceFrozen{FENCE_BUSY, 200 ms}, so no pooled connection waits — N82; exports take no exclusive fence at
// all) followed by a plain
// `SELECT state, epoch FROM namespace_ownership WHERE namespace_id = $1` (no row lock: the row is the fence
// VALUE); for Read: the SELECT only — (3) commits if fn returns nil. Ownership failures surface as
// errs.WrongShardOrEpoch or errs.NamespaceFrozen before fn runs. The former `FOR SHARE` row lock on `namespace_ownership` was dropped
// (D2 as amended, review F-5): Postgres grants a compatible row lock without queueing behind a waiting
// FOR UPDATE, so a continuous stream of writers could starve the barrier, and each extra share locker
// allocates a MultiXactId. Heavyweight (advisory) locks queue fairly and cost no multixacts.
func WithNamespaceTx(ctx context.Context, p Pool, scope Scope, o TxOptions, fn func(ctx context.Context, tx Tx) error) error

// OutboxRepo.Append is the only way to emit an event; it is called inside the same tx as the
// state change (D6) and returns the seq assigned by the sequence. It MUST be the last statement
// before commit (A-F1; engramlint sql fails a builder that runs anything after it).
type OutboxRepo interface {
	Append(ctx context.Context, ev *eventsv1.Event) (seq int64, err error)
	AppendAll(ctx context.Context, evs []*eventsv1.Event) (seqs []int64, err error) // ONE multi-row INSERT for a transaction that emits several events (paged events, N80; the N81 companions): still the last statement
	PagesOf(ids [][]byte, perEvent int) [][][]byte // splits an id set into pages of ≤ 256; callers set page/page_count; above 4,096 ids the caller emits ONE event with ids_elided and counts (N80)
	ReadFrom(ctx context.Context, afterSeq int64, limit int) ([]OutboxRow, error)        // relay; no namespace fence (shard-level, admin conn)
	ReadNamespaceUnapplied(ctx context.Context, p0 int64, limit int) ([]OutboxRow, error)  // move consumer (N50): `WHERE namespace_id = $ns AND seq > $p0 AND NOT EXISTS (SELECT 1 FROM move_applied m WHERE m.namespace_id = o.namespace_id AND m.seq = o.seq) ORDER BY seq LIMIT $n` — a SINGLE source-side statement against the source's lagging copy of move_applied (the table exists on both shards, §3.3.1); an anti-join, never `seq > applied`, so a seq that commits out of order below the highest applied one is still found (review F-3)
	Cursor(ctx context.Context, consumer string) (int64, error)
	AdvanceCursor(ctx context.Context, consumer string, seq int64) error
	Exists(ctx context.Context, seq int64) (bool, error) // gap watchlist probe
	TrimBelow(ctx context.Context, seq int64, olderThan time.Duration, batch int) (int64, error)
}

type FactRepo interface { // representative; the other repositories follow the same shape (table below)
	InsertBatch(ctx context.Context, facts []Fact) error
	RetireByChunkHashes(ctx context.Context, documentID string, hashes [][32]byte, now time.Time) (int64, error)
	RetireStaleExtraction(ctx context.Context, documentID string, now time.Time) ([]string, error) // N58: `UPDATE facts SET retired_at = now(), purge_after = now() + 1 h WHERE … document_id = $1 AND retired_at IS NULL AND extraction_key <> (SELECT extraction_key FROM chunks c WHERE c.chunk_id = facts.chunk_id) RETURNING memory_id` — run by FinalizeVersion so a prompt/model bump never leaves two live fact sets on a kept chunk
	Unretire(ctx context.Context, documentID string, hashes [][32]byte) (int64, error)
	SetInvalidated(ctx context.Context, memoryID string, at *time.Time) (changed bool, err error) // Invalidate / Restore (D8); `changed` is true only on the NULL ↔ set transition, which is the only time the N84 counter moves
	ByIDs(ctx context.Context, ids []string, asOf *time.Time) ([]Fact, error)
	NearestByOccurrence(ctx context.Context, anchor time.Time, window *TemporalWindow, n int, f index.Filter) ([]Fact, error) // temporal arm (N68): two btree probes on (namespace_id, occurred_start) — the n nearest before and after `anchor`, clipped to `window` — instead of sorting every overlapping fact
}
```

The ownership check is exactly the D2 statement; for `Read` it is `SELECT state FROM namespace_ownership WHERE
namespace_id=$1` (no epoch comparison, no lock) and the store rejects `incoming`/`moved_out`/missing with
`WrongShardOrEpoch{State}` and accepts `active`/`frozen`; a `moved_out` row also yields the target hint
`{TargetShardID, NextEpoch}` (N98). The write check compares the epoch; a mismatch is
`WrongShardOrEpoch{Expected: scope.Epoch, Observed: row.Epoch}`. Because every writer holds the *shared*
advisory lock on `engram_ns_lock_keys(namespace_id)` until commit and the move executor's barrier and
freeze take the *exclusive* one on the same key, an in-flight write either commits before the freeze or
observes it; heavyweight locks queue fairly for the exclusive taker (it waits for the writers in flight when it
asked) while writers that arrive behind it are refused by their try-lock (N82), so a busy namespace cannot
starve its own move and cannot exhaust the pool while it waits — the mechanism §7 `ShardMove.tla` models (W-11:
`OtherNamespaceProgress`, starvation-freedom of the mover) and the F-5 review made explicit. Roles: `engram_app`
and `engram_worker` run with `lock_timeout = 2 s`, `engram_move` and `engram_admin` with 10 s. Statements that need the row *value* under the lock (freeze, cutover) still `UPDATE` the row with a
state predicate; nothing takes a row lock as a fence any more.

Repository summary (full DDL in §3):

| Repository | Key methods | Notes |
|---|---|---|
| `DocumentRepo` | `Upsert`, `Get`, `List`, `BeginVersion`, `ActivateVersion`, `MarkDeleted`, `SetBody(version, bodyKey, bodyHash)` (`WHERE body_key IS NULL`, N104) | `document_id` client-chosen, ≤ 256 B; `LoadItem` stores the full version body at `ver/{sha256}` and the next APPEND reads that single object |
| `ChunkRepo` | `LiveByHashes`, `RetiredByHashes`, `InsertBatch` (writes `mentioned_at` = max over covered items and `embedding_effective_at`, N85/N86), `Retire(documentID, version)` (retire subquery scoped to `(namespace_id, document_id, version)`, N107), `Unretire` | identity `(ns, document_id, content_hash)` (D8) |
| `LinkRepo` | `InsertBatch`, `DeleteTouching(factIDs)`, `Neighbours(seed, kinds, limit)` | `fact_links` hash-partitioned; graph arm uses `Neighbours`, which joins `facts` and requires `live` on **both** endpoints (N61: `NoOrphanLinks` = "no traversal through a non-live fact"); `DeleteTouching` runs in `PurgeDocument`, never in the synchronous cascade |
| `EntityRepo` | `Similar(name, type, threshold)` (pg_trgm), `UpsertSorted(entities)` (one `INSERT … SELECT FROM unnest($sorted) ON CONFLICT … DO UPDATE`, rows in `canonical_norm` order so two concurrent `CommitChunk`s never deadlock — N69, review F-41), `AddAlias`, `InsertMentions`, `DeleteMentionsByFacts` | per-namespace; merge writes aliases, never deletes entities; mentions are deleted by the purge (N61) |
| `ObservationRepo` | `Insert`, `NewVersion`, `AddSources`, `AddInputs`, `DeleteStaleSources(obsID, keep)`, `DeleteSourcesByFacts`, `DeleteInputsByFacts`, `LockLiveInputs(factIDs)` (`FOR SHARE`, N41), `LockCandidateVersions(refs)` (`FOR SHARE`, requires `NOT derived_from_deleted AND hidden_by_invalidation = 0 AND retired_at IS NULL`, N79/N84), `InsertLineage(obsID, version, candidates)` (`observation_version_lineage`, same tx as the version row), `InsertVersionSources` (N85), `MarkStale(write\|delete)`, `MarkVersionsDerivedFromDeleted(factIDs)` + `FlagLineageDescendants(seed, maxDepth=64)` (recursive CTE; fails closed with `stale_delete` on the frontier), `AdjustInvalidation(factID, +1\|-1)` (the N84 counter over naming versions and descendants, superseded included), `LatestAsOf(ids, T)` | D9 versioning; the trigger retires at 0 sources, marks the observation `stale_delete` and sets `observation_versions.derived_from_deleted` on exactly the versions whose inputs named the victim — per version, permanent, never cleared by an apply (N41, review F-1); an update calls `AddSources`/`AddInputs` **before** `DeleteStaleSources` so the zero-sources trigger never fires inside the update (N57, review F-11); `LatestAsOf` skips `derived_from_deleted` versions at every T and versions with `hidden_by_invalidation > 0`; `update` deletes only sources the model was shown and dropped (N79) |
| `PageRepo` | `Insert`, `NewVersion`, `SetSources`, `DeleteSourcesByFacts`, `MarkStale(write\|delete)`, `Search(query, vector, filter, asOf, limit)` (BM25 ∪ HNSW over `page_versions`, N73) | markdown lives in blob |
| `OperationRepo` | `Insert`, `Get`, `List`, `Transition(from,to)`, `SetProgress(unitsDone, unitsFailed, phase)` (absolute values, written **once per wave** by the workflow — N69, review F-36: a per-chunk increment serialised all 32 commits of a document on one row), `Defer(until)`, `PendingWithoutWorkflow(olderThan)` | states `PENDING/RUNNING/DEFERRED/SUCCEEDED/FAILED/CANCELLED` (N35; a partial failure is `SUCCEEDED` with `progress.units_failed > 0`) |
| `StatsRepo` | `Refresh()` (`count(*)` over the `WHERE live` partial indexes per namespace, run only by the **stats sweeper** — `engram_admin`, every 60 s per namespace, one read per tick — which also flips `namespace_stats.large` at 20 k live facts with hysteresis at 10 k, N55), `Get` | `namespace_stats` is **derived**, never written on the commit path by `CommitChunk`, `FinalizeVersion` or the delete cascade (N69); `max_facts` tolerates one refresh interval of staleness |
| `DocumentRepo.SetChunksDone(v, n)` / `OperationRepo.SetProgress` | written **once per wave** by the workflow (`MarkProgress`), never per chunk | a per-chunk `UPDATE` of the version row would serialise the 32 parallel commits of a document on one row (§3.3.1) |
| `SnapshotRepo` | `Insert`, `ExpireContaining(documentID)` (`UPDATE export_snapshots SET state = 'expired', expired_at = now() WHERE … state = 'ready' AND created_at >= <document's first version>`, run by the delete cascade — N59), `List`, `Get` | `StreamSnapshot` refuses `expired` versions with `PreconditionFailed{SNAPSHOT_EXPIRED}` |
| `LedgerRepo` | `Append` | append-only; no update/delete method exists; ledger rows are deleted only by explicit document, namespace or tenant delete (admin role, after the `document_versions` rows), never by a replace/append retire (N104) |
| `ConsolidationRepo` | `PendingFacts(limit)` (live facts above `consolidation_state.watermark_memory_id` with no `fact_consolidation` row), `MarkConsolidated(batchKey, factIDs)` (insert-only, `ON CONFLICT DO NOTHING`), `Stamp(factID, note)` (`failed`/`capacity`), `AdvanceWatermark()` (never past `uuidv7(now() − 2 × statement_timeout)`, N95) | `facts.consolidated_at` and `facts_unconsolidated_idx` no longer exist; covered by `BatchApplied` |
| `IdempotencyRepo` | `Get`, `Put`, `Expire(olderThan)` | 24 h (D1) |
| `TokenUsageRepo` | `Add(day, op, model, prompt, completion, cost)`, `SumDay` | moves with the namespace (D13) |
| `OwnershipRepo` | `Get`, `Insert(incoming)`, `LockExclusive()` (transaction-scoped `pg_advisory_xact_lock` over `engram_ns_lock_keys(ns)` under `lock_timeout` 5 s with jittered retry — freeze, cutover (b)/(c), rollback, delete freeze, restore; waits for every shared holder, later writers are refused by their try-lock, N82), `LockExclusiveSession()` / `UnlockSession()` (the **session-level** `pg_advisory_lock`/`pg_advisory_unlock` form on a direct connection, N15 — the copy barrier, which must release the lock without ending its `REPEATABLE READ` transaction, §3.3 lock-discipline table; exports no longer use it, N82), `Transition(edge, epoch, …)` (state-predicate `UPDATE`; the edges, their roles and the `moved_out` target fields are generated from the §3.3.1 transition table, N101/N103, including `moved_out → incoming` for a move back, N93), `SetMoveEpoch` (N88: `move_epoch` = target epoch, set by the `start_move` edge, cleared by abort/thaw/activate; the views `purgeable_namespaces` and `schedulable_namespaces` exclude such namespaces) | write methods require the `engram_move` (or admin) role; **there is no `engram_move_load` any more** (N91): `engram_move` has no `BYPASSRLS`, loads each range through a session `TEMP` table and `INSERT … SELECT … ON CONFLICT` under `ns_isolation` after re-checking the target row (`state = 'incoming'`, matching epoch) with `SET LOCAL session_replication_role = replica` (`GRANT SET ON PARAMETER`, PG 15+); source cleanup is the `engram_migrate`-owned `SECURITY DEFINER` function `engram_cleanup_namespace`, which never deletes the ownership row (N93) |
| `ReplayRepo` | `Upsert(table, rows, class)` with the N81 semantics (immutable tables `DO NOTHING`; mutable tables `DO UPDATE` of mutable columns `WHERE … IS DISTINCT FROM`, `engram.replay = 'on'`), `VerifyFK()` (calls `engram_verify_fk(ns)`: orphan count per foreign key, must be 0), `Purged(table, documentID, minKey, maxKey)` | target side of the move; the event → `(table, key expression)` mapping is `internal/move/replay_map.go`, generated from the `replay:` annotations of `events.proto`; each `store.Queries` entry declares `writes` plus the covering event, `frozen_recopy` or `derived`, and the build fails on an uncovered table (N81) |
| `MoveAppliedRepo` | `RecordApplied(seq)` (target, `INSERT … ON CONFLICT DO NOTHING RETURNING seq` in the same tx as the event's apply — the authoritative exactly-once guard), `MirrorApplied(seqs)` (source, the mover's lagging copy, written after the target commit so the anti-join is one source-side statement), `SetLagSeq(seq)` (`namespace_ownership.move_applied_seq`, lag reporting only), `DeleteForMove` (both copies at done/rolled_back) | N50, §3.3.1: a crash between the two inserts only makes the next round re-read an event the target then drops |

**(c) Dependencies.** `errs`, `telemetry`, `gen/go/engram/internal/events/v1`, `pgx/v5`, `pgxpool`.

**(d) Swappable.** `PgxPool` + `pgxTx` (production; pgbouncer transaction pooling → no session state, `SET LOCAL`
only) → `RecordingPool` (records shard/namespace per statement; isolation tests) → `FakeTx` (in-memory repos with
the same fencing state machine; service unit and property tests).

**(e) Test seam.** testcontainers Postgres with the real migrations; a test-only trigger suite asserts the RLS
policy (a query without `SET LOCAL` returns zero rows), the append-only ledger (any `UPDATE`/`DELETE` on
`ingest_ledger` raises), and the shared/exclusive advisory-lock interaction (a writer that started before
freeze commits; one that starts after fails; a stream of writers arriving every 5 ms cannot delay the exclusive
lock past the longest in-flight writer — the F-5 starvation case, run as a two-session test on PG 16 before any
move code exists), plus the target-side range load as `engram_move` under RLS (a stream carrying another `namespace_id` fails the policy, a load into a non-`incoming` namespace is refused, replica mode skips the append-only and `*_touch` triggers — N91), `TestFence_TryLockRefusedBehindWaiter` (a shared try-lock behind a queued exclusive request is refused; PG 16), `TestFence_PoolNotExhausted` (32-writer hot namespace plus a freeze: recall p95 on a second namespace of the same shard does not move), `TestDocLock_NoStarvation` (continuous `CommitChunk`s must not delay `FinalizeVersion`, the ack or the cascade beyond `lock_timeout`), and the property test that every event type at its stated maxima encodes to ≤ 16 KiB (N80).

#### 2.2.8 `internal/index` — search index abstraction

**(a) Responsibility.** Hide where dense/lexical search runs. `Transactional` mode means the index *is* the
shard's Postgres indexes and nothing extra is written; `Async` means an external engine fed by the outbox
`index` consumer (D7). Recall arms only see `Index`.

**(b) Interfaces.**

```go
package index

type Mode int

const (
	Transactional Mode = iota + 1 // PostgresIndex, TsvectorIndex: written by the same tx as facts
	Async                         // ExternalIndex: written by the outbox "index" consumer
)

type Kind int // KindFact | KindObservation | KindChunk

type Hit struct { ID string; Kind Kind; Score float64 /* arm-local raw score (cosine or BM25); the planner min–max normalises it PER QUERY to [0, 1] before anything is exposed in Scores (N67: raw BM25 is partition-relative and a weak cross-tenant channel) */; MentionedAt time.Time }
type Filter struct {
	AsOf *time.Time               // mentioned_at ≤ AsOf, applied inside the predicate (D9); the CHUNK arm also requires embedding_effective_at ≤ AsOf (N85), and observation hits are the version's own evidence rows (observation_version_sources) joined to live facts with mentioned_at ≤ AsOf
	FactTypes []memoryv1.FactType
	Tags []string
	TagMode memoryv1.TagMatchMode // ANY | ANY_STRICT | ALL | ALL_STRICT | EXACT (D10, §4.3)
	DocumentIDs []string
}
// SemanticPlan is chosen per query by namespace size (N55, review F-8): a shared partition HNSW over
// ~625 k facts of ~10 namespaces truncates the candidate list of a small namespace (0.16 % selectivity needs
// ≈ 94 k visited tuples against hnsw.max_scan_tuples = 20 000) and costs up to 20 k heap fetches per arm.
type SemanticPlan int

const (
	PlanExact      SemanticPlan = iota + 1 // namespace below 20 k live rows: exact scan over (namespace_id) + live, `embedding <=> $q` ordered; ≤ 20,000 rows × ≈ 3 KB heap with `STORAGE MAIN` embeddings (N94: the earlier "30 MB, 5–15 ms" figure is withdrawn until M0.6 measures it cold for facts, chunks and observation versions), never truncated
	PlanPartialHNSW                        // only in the band where the shared index fails (N94): `live_rows >= 20,000 AND live_rows < 2 % of the partition's live rows`; its own partial index `WHERE namespace_id = …`, built by `engramctl index` when the flag flips (on a move target only after copy and catch-up, N89)
	PlanPartitionHNSW                      // fallback: the shared partition index with iterative relaxed-order scan
)

type SemanticQuery struct { Vector []float32; Kinds []Kind /* facts and/or observation_versions */; Filter Filter; Limit int /* 50/150/400 by budget */; EfSearch int /* hnsw.ef_search ≥ Limit (400 at HIGH), N55 */; Plan SemanticPlan /* 0 = let Index.PlanSemantic decide from namespace_stats.live_facts and the `large` flag */ }
type LexicalQuery struct { Text string; Kinds []Kind; Filter Filter; Limit int }
type ChunkQuery struct { Text string; Vector []float32 /* nil → BM25 only */; Filter Filter; Limit int }

// Index is bound to one shard; the write methods are no-ops in Transactional mode.
type Index interface {
	Mode() Mode
	UpsertFacts(ctx context.Context, tx store.Tx, facts []FactDoc) error
	RetireFacts(ctx context.Context, tx store.Tx, ids []string) error
	UpsertChunks(ctx context.Context, tx store.Tx, chunks []ChunkDoc) error
	UpsertObservationVersions(ctx context.Context, tx store.Tx, obs []ObservationDoc) error
	SearchSemantic(ctx context.Context, tx store.Tx, q SemanticQuery) ([]Hit, error)
	SearchLexical(ctx context.Context, tx store.Tx, q LexicalQuery) ([]Hit, error)
	SearchChunks(ctx context.Context, tx store.Tx, q ChunkQuery) ([]Hit, error)
	SearchPages(ctx context.Context, tx store.Tx, q ChunkQuery) ([]Hit, error)         // BM25 ∪ HNSW over page_versions (SearchPages / reflect `search_pages`, N73)
	PlanSemantic(ctx context.Context, tx store.Tx) (SemanticPlan, error)              // N55: from namespace_stats.large (flipped by the stats sweeper at 20 k live facts, hysteresis 10 k) and whether the namespace's partial index exists; the exact plan sets `SET LOCAL enable_indexscan = off` for the statement (bitmap scan on facts_mentioned_idx + top-K sort)
}

// The per-namespace partial HNSW (`CREATE INDEX CONCURRENTLY facts_hnsw_ns_<12 hex> ON facts_pNN USING hnsw (…)
// WHERE namespace_id = '<ns>'`) is OWNER-RUN DDL: `engramctl index --shard 7 --namespace <ns>` as the owner role
// `engram_migrate` (only the owner may create an index on PG 16), queued when the sweeper flips `large` and
// dropped at purge or move cleanup. Neither engram-api nor engram-worker ever runs DDL; Index only plans.

// Sink is what the outbox "index" consumer drives for Async indexes (§2.2.18).
type Sink interface {
	Apply(ctx context.Context, events []*eventsv1.Event) error // idempotent by (namespace_id, seq)
}
```

Under `Filter.AsOf` a chunk hit's `Header` is returned empty (N85). Search methods take the fenced `tx` so `SET LOCAL engram.namespace_id` (partition pruning + RLS) and `SET LOCAL
hnsw.ef_search / hnsw.iterative_scan` apply. `Filter.AsOf` is rendered into the `WHERE` of every arm, never
applied post hoc (D9). **Liveness join (N44):** every implementation's hits are joined with
`facts.retired_at IS NULL AND invalidated_at IS NULL` (observation versions: `live`) at read time, inside `tx`,
before ranking — `PostgresIndex`/`TsvectorIndex` do it inside the scan through the `live` column, `ExternalIndex`
by a `WHERE memory_id = ANY($hits) AND live` re-read — so a hit the store no longer considers live is dropped even
when the engine has not caught up (TLC `DocLifecycle_UnfilteredIndex`, §7); the contract test in §8 proves the
join on every implementation. Tag semantics are implemented once in `index/tags.go` and mirrored by the Lean decision
procedure (§7).

**(c) Dependencies.** `store`, `errs`, `telemetry`, `gen/go`.

**(d) Swappable.** `PostgresIndex` (HNSW `halfvec_cosine_ops` + pg_search BM25 + pg_trgm; MVP default, D7) →
`TsvectorIndex` (HNSW + `tsvector`/`ts_rank_cd`; Postgres without pg_search, documented lower lexical quality) →
`ExternalIndex` (`Async`, index `engram-shard-{id}`, fed by the outbox; phase 3+ when BM25 outgrows the instance) →
`MemIndex` (brute-force cosine + in-memory BM25; recall unit and property tests).

**(e) Test seam.** The same retrieval golden set (§8) runs against `PostgresIndex`, `TsvectorIndex` and
`MemIndex`; `as_of` leakage tests assert zero hits with `mentioned_at > T` for every arm; a plan test seeds a
16-partition shard with one 1 000-fact namespace next to 600 k facts of others and asserts the semantic arm
returns the full cap under `PlanExact` and the heap-fetch count per recall (N55 puts fetches × QPS into the
IOPS sizing).

#### 2.2.9 `internal/chunk` — heading-anchored content-defined chunker

**(a) Responsibility.** Split a document into deterministic chunks (target 3 000 chars, min 500, max 4 000, no
overlap) whose boundaries prefer markdown headings and then content-defined cut points (rolling-hash breakpoints
on paragraph boundaries), and build the contextual header `[doc summary ≤ 200 chars] > [heading path]` (D11).
Pure Go, no I/O.

**(b) Interfaces.**

```go
package chunk

type Item struct { Timestamp time.Time /* the item's server-set timestamp */; ByteStart, ByteEnd int /* span in Document.Content */ }
type Document struct { DocumentID string; Content string /* raw text/markdown */; Items []Item /* in order; Content is their concatenation (APPEND: base body ‖ "\n\n" ‖ new bodies) */; BaseChunks []BaseChunk /* APPEND only: the last base chunk(s) the new text re-chunks, with their MentionedAt (N86) */; Context string /* item.context, appended to the header if present */ }
type BaseChunk struct { ByteStart, ByteEnd int; MentionedAt time.Time }
type Options struct { MaxItemGap time.Duration /* 24 h: a HARD boundary is forced between consecutive items whose timestamps differ by more than this (N86) */; TargetChars int /* 3000 */; MinChars int /* 500 */; MaxChars int /* 4000 */; Summary string /* ≤ 200 chars, from SummarizeDocument; "" on first pass */; SummaryID string /* id of the summary in use; part of header_hash (N60) */ }
type Chunk struct {
	Ordinal int
	ContentHash [32]byte                         // sha256(Text) — identity within the document (D8)
	Text string                                  // stored in chunks.text (≤ 4,000 chars and ≤ 16 KiB, D8)
	Header string                                // stored in chunks.header
	prepended for embedding + extraction only */
	HeadingPath []string
	ByteStart int
	ByteEnd int
	Tokens int                                   // cl100k_base estimate
	ItemIndexes []int                            // every item whose bytes the chunk covers, ascending (N86; replaces the singular item index)
	MentionedAt time.Time                        // max over ItemIndexes' timestamps and, for an APPEND re-chunk, over the BaseChunks the new text overlaps (N86); facts inherit it; the as_of key
}

// Result of Chunker.Chunk also reports how many item timestamps were clamped up to the running maximum (regressions are
// accepted, not rejected, so re-ingest and benchmark replays keep working) — surfaced as OperationResult.timestamps_clamped.
type Chunked struct { Chunks []Chunk; TimestampsClamped int }

type Chunker interface {
	Chunk(ctx context.Context, doc Document, o Options) (Chunked, error)
}

// HeaderBuilder is separate so the header can be recomputed when only the summary changed
// without re-chunking (the content hash excludes the header on purpose).
type HeaderBuilder interface {
	Build(summary string, headingPath []string, docContext string) string
	// HeaderHash = sha256(heading path ‖ summary id) — compared at FinalizeVersion (N6, N60).
	HeaderHash(summaryID string, headingPath []string) [32]byte
}

// SummaryPolicy decides whether SummarizeDocument runs for a version (N60, review F-14): on REPLACE, or when
// the document grew by more than 25 % since the version whose summary is in use; otherwise the current
// summary_id is reused. Without it every APPEND (every chat turn) produced a new summary, a new header hash
// on every chunk and a re-embedding of the whole document.
func SummaryPolicy(mode memoryv1.UpdateMode, prevBytes, newBytes int) (recompute bool)
```

The *content* hash excludes the header, so chunk identity survives a re-summary; since N87 the extraction cache key
does include `header_hash` (through `render_hash`), so a changed summary or heading path re-extracts through the
ordinary retain path (open interaction with N60 recorded as PD-26). Three rules keep the header cheap (N60): the summary is recomputed only on
`REPLACE` or when the document has grown by more than 25 % since the summary in use, so an append-only
conversation re-summarises every ~4th doubling, not every turn; `header_hash` covers the heading path and the
*summary id*, so a byte-identical summary never trips it; and the header is embedded into the **chunk vector
only** — fact vectors are embedded from the fact text alone — so a header change re-embeds N chunks, never
N × facts. `FinalizeVersion` compares `header_hash` and re-embeds only the chunks whose hash changed. Rejected:
hashing header+text (defeats delta retain whenever the summary drifts); embedding the header into fact
vectors (a 150-chunk conversation appended 100 times would pay ≈ 15 000 re-embeddings for unchanged content,
review F-14).

**(c) Dependencies.** none beyond `tiktoken-go`.

**(d) Swappable.** `CDCChunker` (default) → `FixedChunker` (3 000-char windows, the Hindsight baseline, kept for
A/B in §8) → `SentenceChunker` (evaluation only).

**(e) Test seam.** Golden files (`testdata/*.md` → expected boundaries + hashes) and rapid properties:
a chunk built from two items with non-monotone timestamps takes the later timestamp (`AsOf_ChunkTwoItems.cfg`, W-10),
a boundary is forced across a gap over 24 h, concatenation of chunks equals input, every chunk within `[min, max]` except a possibly short last chunk,
determinism.

#### 2.2.10 `internal/extract` — structured extraction with prompt versioning and cache

**(a) Responsibility.** One structured LLM call per chunk producing typed facts; document summarisation; a
content-addressed per-namespace cache in blob (D11). Owns the JSON schema (`schema_version`) and the prompt
versions (`extract/v1`, `summarize/v1`, §6).

**(b) Interfaces.**

```go
package extract

type FactType string // "world" | "experience"

type EntityMention struct { Name string; Type string /* person | org | place | thing | concept */; Role string /* who | what | where */ }
type CausalRef struct { CauseIndex int /* index of the causing fact in the same extraction, or -1 */; CauseText string /* free text when the cause is not a fact in this chunk */ }

type Fact struct {
	Text          string
	Type          FactType
	Who, What     string
	When, Where   string
	Why           string
	OccurredStart *time.Time
	OccurredEnd   *time.Time
	MentionedAt   time.Time  // ALWAYS the chunk's mentioned_at (max over the items it covers, N86), set by the activity from ChunkInput.ItemTimestamp (D9 as amended): the extractor's output has no say in the as_of key (review F-2)
	SaidAt        *time.Time // the extractor's "as said on" date when the chunk quotes older material; display and ranking only, never a visibility key
	Entities      []EntityMention
	Causes        []CausalRef
	Confidence    float32
}

type ChunkInput struct { Header, Text string; ContentHash [32]byte; ItemTimestamp time.Time /* = chunk.MentionedAt */; Context string; Metadata map[string]any; EntityHints, Tags []string; Mission string /* retain.mission */; HeaderHash [32]byte }

// RenderHash covers every prompt input (N87, review-2 G-9): sha256(day(mentioned_at) ‖ context ‖ canonical(metadata) ‖
// sorted(entity_hints) ‖ retain.mission ‖ header_hash). A change to any of them is a new cache key and re-extracts.
func RenderHash(in ChunkInput) [32]byte
type Extraction struct { Facts []Fact; PromptVersion string; Model gateway.Model; SchemaVersion string; Usage gateway.Usage; FromCache bool }

type Extractor interface {
	Extract(ctx context.Context, in ChunkInput) (*Extraction, error)
	Summarize(ctx context.Context, content string) (summary string, u *gateway.Usage, err error)
	PromptVersion() string
	SchemaVersion() string
}

type CacheKey struct { Prefix blob.Prefix; ChunkHash [32]byte; PromptVersion string; Model gateway.Model; SchemaVersion string; RenderHash [32]byte }

func (k CacheKey) BlobKey() string // "{shard}/{tenant}/{ns}/xcache/{sha256(chunk_hash‖prompt‖model‖schema‖render_hash)}.json" — equal to chunks.extraction_key (N87)

type Cache interface {
	Get(ctx context.Context, k CacheKey) (*Extraction, bool, error)
	Put(ctx context.Context, k CacheKey, x *Extraction) error
}

// CachedExtractor wraps an Extractor with a Cache; cache errors are logged, never returned.
func Cached(e Extractor, c Cache) Extractor
```

Validation after decode: `Text` non-empty, `OccurredStart ≤ OccurredEnd`, entity roles in the enum, causal
indices in range, `SaidAt ≤ ItemTimestamp + 5 min` (an earlier `SaidAt` is legal and informational); a
violation is `errs.Validation` and, in the activity, counts as `PermanentLLMError` after one re-prompt with the
validation message appended (§6). The decoder discards any `mentioned_at` the model emits and copies
`ItemTimestamp` instead.

**(c) Dependencies.** `gateway`, `blob`, `errs`, `telemetry`.

**(d) Swappable.** `LLMExtractor` (gateway structured call) → `BatchExtractor` (gateway batch API for backfills;
same cache key) → `RuleExtractor` (regex "one fact per paragraph", tests and cost-free smoke runs).

**(e) Test seam.** Record/replay gateway; goldens keyed by prompt version so a prompt bump shows exactly which
extractions changed; cache tests assert the key changes with each of the five inputs (chunk hash, prompt, model, schema, `render_hash`) and with each component of `render_hash` (timestamp day, context, metadata, hints, mission, header hash), and not with anything outside them; hit rate is that of identical text under identical rendered variables (§6.8).

#### 2.2.11 `internal/entity` — per-namespace fuzzy resolution

**(a) Responsibility.** Map extracted mentions to `entities` rows within the namespace using `pg_trgm`
similarity on `canonical_name`, an alias table, type agreement and item hints; decide merges.

**(b) Interfaces.**

```go
package entity

type Mention struct { Name string; Type string; FactIdx int; Role string }
type Candidate struct { EntityID string; Canonical string; Type string; Similarity float32 /* pg_trgm similarity, 0..1 */; Aliases []string; Mentions int64 }
type Resolution struct { Mention Mention; EntityID string; Created bool; Method string /* "exact" | "alias" | "trigram" | "hint" | "new" */; Score float32 }
type Options struct { Threshold float32 /* 0.6 trigram similarity, 0.85 when a side's type is unknown (A-9, tuned in §8; §5.1.2) */; MaxCandidates int /* 5 */; TypeStrict bool /* true: never merge across types */ }

type Resolver interface {
	Resolve(ctx context.Context, tx store.Tx, mentions []Mention, hints []string, o Options) ([]Resolution, error)
}

type MergePolicy interface {
	ShouldMerge(m Mention, c Candidate) bool
}
```

Merge policy (default): exact canonical or alias match → merge; trigram ≥ threshold *and* same type → merge and
record the mention text as an alias; otherwise create. The resulting creates/merges are handed to
`EntityRepo.UpsertSorted`, which applies them in `canonical_norm` order in one statement, so two concurrent
`CommitChunk`s of the same namespace acquire entity row locks in the same order and cannot deadlock (N69,
review F-41; Hindsight's `_lock_order_key`). Entities are never deleted by resolution; a wrong merge is repaired
by `engramctl entity split` (phase 3). Rejected: LLM-assisted resolution (cost on every chunk; the
alias table captures most of the benefit).

**(c) Dependencies.** `store`, `errs`. **(d) Swappable.** `TrigramResolver` (default) → `ExactResolver` (tests,
deterministic corpora). **(e) Test seam.** testcontainers with a seeded entity set and a rapid property:
resolution is idempotent (resolving the same mentions twice creates nothing new).

#### 2.2.12 `internal/link` — link building under a budget

**(a) Responsibility.** For each new fact, produce `fact_links` rows of four kinds: entity (shared entity),
temporal (nearest facts by occurrence, capped 20/fact), semantic (kNN k=10, cosine ≥ 0.75, via
`index.SearchSemantic`), causal (from extraction). Everything inside the chunk's transaction.

**(b) Interfaces.**

```go
package link

type Kind string // "entity" | "temporal" | "semantic" | "causal"

type Link struct { FromFactID string; ToFactID string; Kind Kind; Weight float32 /* cosine for semantic, 1/(1+days) for temporal, 1 otherwise */ }
type LinkBudget struct { TemporalPerFact int /* 20 */; SemanticK int /* 10 */; SemanticMinCosine float32 /* 0.75 */; EntityPerFact int /* 20: the 10 most recent facts per shared entity (guards hub entities) */; MaxPerFact int /* 60 hard cap across kinds (§5.1.2) */ }
type BuildInput struct { Facts []store.Fact /* with ids and embeddings */; Resolutions []entity.Resolution; Causes map[int][]int /* fact index → cause indices (same chunk) */ }
type BuildResult struct { Links []Link; Dropped map[Kind]int /* over-budget counts, reported in operation stats */ }

type Linker interface {
	Build(ctx context.Context, tx store.Tx, idx index.Index, in BuildInput, b LinkBudget) (*BuildResult, error)
}
```

Budget enforcement is per kind then global; ties broken by weight then by `to_fact_id` so the result is
deterministic (needed for idempotent `CommitChunk` retries). Rejected: unbounded entity links (hub entities like
"the user" would create O(n²) rows; D3's 30 links/fact average is the sizing assumption).

**(c) Dependencies.** `store`, `index`, `entity`, `errs`. **(d) Swappable.** `BudgetLinker` (default) →
`NoSemanticLinker` (when embeddings are unavailable; semantic links backfilled by a later `RelinkWorkflow`).
**(e) Test seam.** `MemIndex` + `FakeTx`; rapid property: no fact exceeds `MaxPerFact`, temporal ≤ 20, semantic
cosine ≥ 0.75, output deterministic for equal input.

#### 2.2.13 `internal/recall` — arms, fusion, rerank, boosts, packing, planner

**(a) Responsibility.** The whole D10 read path with zero generative calls: five arms in parallel with per-arm
deadlines, RRF, cross-encoder rerank via the gateway, bounded boosts, greedy token packing, streaming with
deadline-aware degradation.

**(b) Interfaces.**

```go
package recall

type Budget int // BudgetLow | BudgetMid | BudgetHigh → arm caps 50/150/400, graph nodes 100/300/1000, rerank_top 0/50/150 (N53), tokens 4k/8k/16k

type Query struct {
	Scope authz.RequestScope
	Text string
	Vector []float32          // nil if the embedding arm failed → dense arms skipped
	Budget Budget
	Filter index.Filter       // incl. AsOf (D9)
	QueryTimestamp *time.Time // temporal anchor
	Window *TemporalWindow
	MaxTokens int
	Deadline time.Time
}
type Candidate struct { ID string; Kind index.Kind; Arm string; Rank int /* 1-based within the arm */; ArmScore float64; MentionedAt time.Time; Provenance Provenance /* document_id, chunk_id, observation_id */ }

type Arm interface {
	Name() string // "semantic" | "lexical" | "graph" | "temporal" | "chunks"
	Needs() Needs // NeedsVector | NeedsSeeds | NeedsNone — the planner schedules by this; the graph arm runs as two seeded waves (N54)
	Run(ctx context.Context, tx store.Tx, idx index.Index, q *Query, seeds []Candidate, capN int) ([]Candidate, error)
}

type Fused struct { Candidate; RRF *big.Rat /* exact; rounded to float64 only for Scores.rrf */; ArmRanks map[string]int /* per-stage scores returned to the client */; ArmScores map[string]float64 /* per-query min–max normalised to [0, 1] (N67) */ }

type Fuser interface {
	Fuse(lists [][]Candidate, k int) []Fused // k = 60; sums 1/(k+rank) in math/big.Rat (≤ 2 000 candidates × 5 arms), so the result is permutation-invariant exactly as the Lean theorem states (N67, review F-31: float64 summation in arm order is not associative and could flip equal-rank ties)
}

type Reranker interface {
	Rerank(ctx context.Context, query string, in []Fused, top int) ([]Ranked, error) // top = rerank_top 0/50/150 by budget (N53); ≤ 300 pairs per gateway call (D15); default model `bge-reranker-base`, `bge-reranker-v2-m3` per-namespace (`models.rerank`)
}

type Ranked struct { Fused; Rerank float64; Boost float64 /* combined factor ∈ [0.75, 1.25] */; Final float64; Stage memoryv1.RecallStage /* FUSED | RERANKED */; Text string; Tokens int }

type Booster interface {
	Boost(q *Query, in []Ranked, now time.Time) []Ranked // recency ≤ +10 %, temporal ≤ +10 %, proof ≤ +5 %, clamp
}

type PackResult struct { Items []Ranked; UsedTokens int; SkippedCount int /* items that did not fit; packing never stops early (D10) */ }

type Packer interface {
	Pack(in []Ranked, maxTokens int) PackResult
}

type Sink interface { // batches of 10, then the trailing stats
	Batch(ctx context.Context, items []Ranked) error
	Stats(ctx context.Context, s *memoryv1.RecallStats) error
}

type PlannerDeps struct { Embedder QueryEmbedder /* LRU keyed (namespace, sha256("search_query: "+text)) */; Arms []Arm; Fuser Fuser; Reranker Reranker; Booster Booster; Packer Packer; Counter TokenCounter /* cl100k_base */ }
type PlannerOptions struct { ArmDeadline time.Duration /* 60 ms */; GraphWaveBudget time.Duration /* 30 ms per seeding wave (N54) */; RerankReserve time.Duration /* 106 ms = rerank p95 90 + pack 3 + stream 5 + 8 (N106) */; RerankBudget time.Duration /* 90 ms at 50 pairs (N54) */; EmbedDeadline time.Duration /* 40 ms */; RerankTop map[Budget]int /* 0/50/150 (N53) */ }

type Planner interface {
	Run(ctx context.Context, tx store.Tx, idx index.Index, q *Query, sink Sink) error
}
```

Scheduling (§1.5, N54): `NeedsNone` arms (lexical, temporal) start immediately and overlap the embedding
round-trip; `NeedsVector` arms (semantic, chunks) start when the embedding arrives (or are skipped when it
fails); the graph arm (`NeedsSeeds`) runs **two waves**, each with a `GraphWaveBudget` of 30 ms and a shared node
budget: wave 1 is seeded from the lexical top-20 the moment lexical returns (so it runs under the dense arms),
wave 2 from the semantic top-20 when semantic returns. The critical path is authz 2 → embed 25 → semantic 60 →
graph wave 2 30 → fuse 1 → rerank 90 → pack 3 → stream 5 = 216 ms p95 at MID (review F-7: seeding one graph
pass behind *both* seed arms at a full 60 ms made it ≈ 276 ms). Every other arm gets
`min(ArmDeadline, remaining − rerankReserve − packReserve)`. The rerank-skip rule (N106, review-2 G-28): rerank
is skipped when the remaining deadline is below `RerankReserve` = 106 ms; the SLO assumes a client deadline of at
least 300 ms, under which a skip occurs only when the pre-rerank path exceeds `deadline − 106 ms` = 194 ms — a p99
property of that path (M0.5 measures its p95/p99, M1.2's < 1 % criterion is judged against it); at client deadlines
below 300 ms skipping is by design and is not an SLO breach. The temporal arm is a two-sided btree probe
around `query_timestamp` (`FactRepo.NearestByOccurrence`, N68), never a sort of every overlapping fact. Arm
scores are min–max normalised per query before fusion statistics are exposed (N67). Rerank runs on
`RerankTop[budget]` pairs (0 at LOW); a skip for lack of deadline sets `stage = FUSED`, increments
`engram_recall_rerank_skipped_total{shard, reason}` and is an **SLO breach** (N53), not a benign degradation —
the reranker is a sized dependency (A-R1: ≥ 80 k pairs/s per cell; deadline-skips at client deadlines < 300 ms are excluded from the breach count, N106). Under `as_of` the chunk arm adds `embedding_effective_at ≤ T`, `ChunkInfo.header` is returned empty and observation evidence is the version's own rows (N85). The planner records per-stage timings and
arm statuses into `RecallStats`. Rejected: `errgroup` with a shared deadline (one slow arm would take the
others with it); `rerank_top.mid = 150` with `bge-reranker-v2-m3` (240 k pairs/s per cell — a GPU fleet nobody
has provisioned, review F-6).

**(c) Dependencies.** `store`, `index`, `gateway` (reranker, query embedder), `authz` (types), `errs`,
`telemetry`, `tiktoken-go`.

**(d) Swappable.** Five `Arm`s over `index.Index`, `RRFFuser` (exact), `GatewayReranker` (`bge-reranker-base`
default, N53), `BoundedBooster`, `GreedyPacker` (production) → `NoopReranker` (rerank disabled by config or
`rerank_top = 0`; `stage = FUSED`) → `FakePlanner` (API handler tests) → `MemIndex`-backed arms (property tests:
as_of leakage, cap adherence).

**(e) Test seam.** Each stage is pure or takes `tx`+`idx`; rapid properties mirror the Lean theorems: RRF
permutation invariance (exact, over `big.Rat`) and monotonicity, packing never exceeds the budget and skips
exactly the items that do not fit, boost factor always in `[0.75, 1.25]`; a deadline test with a fake clock
asserts the 216 ms critical path, that the rerank-skip counter stays at zero under the budgeted latencies at a 300 ms deadline, and the 106 ms reserve.

#### 2.2.14 `internal/consolidate` — facts → observations (phase 2)

**(a) Responsibility.** D12: build batches of 8 unconsolidated facts (≤ 100 per round), fetch top-10 candidate
observations per batch by semantic similarity, ask the LLM for `create/update/delete` ops with cited
`source_fact_ids`, validate, bisect on failure 8 → 4 → 2 → 1, persist the proposal write-once (N43), apply
idempotently by `op_key` over the stored list after re-verifying every input `FOR SHARE` (N41).

**(b) Interfaces.**

```go
package consolidate

type Batch struct { FactIDs []string /* sorted; selected as store.ConsolidationRepo.PendingFacts — live facts above the watermark with no fact_consolidation row (N95) */; Facts []store.Fact; BatchKey [32]byte /* sha256(sorted fact ids ‖ prompt_version ‖ model) */ }

type OpKind string // "create" | "update" | "delete"

type Op struct { Kind OpKind; ObservationID string /* update/delete */; Text string /* create/update */; SourceFactIDs []string /* must ⊆ batch ∪ existing sources */; Quotes []string; OpIndex int }

func OpKey(batchKey [32]byte, opIndex int) [32]byte // sha256(batch_key ‖ op_index)

type BatchBuilder interface {
	Build(ctx context.Context, tx store.Tx, maxFacts, perBatch int) ([]Batch, error)
}

type CandidateFinder interface {
	Find(ctx context.Context, tx store.Tx, idx index.Index, b Batch, k int) ([]store.Observation, error)
}

type Proposer interface { // the LLM step (prompt consolidate/v1)
	Propose(ctx context.Context, b Batch, cands []store.Observation) ([]Op, *gateway.Usage, error)
}

type Validator interface {
	Validate(ops []Op, b Batch, cands []store.Observation) error // errs.Validation → bisect
}

// Proposal is the write-once record of what the prompt saw and what it answered (N43).
type Proposal struct { BatchKey [32]byte; Ops []Op /* op_index order; creates carry a pre-minted ObservationID */; InputFactIDs []string /* the RENDERED evidence: batch facts ∪ the quoted sources actually shown for each candidate (≤ 5 per candidate, N47) — not every source of every candidate, which inflated inputs to ≈ 210 per version and let one session delete hide ≈ 1 600 observations (N41 as amended, review F-9) */; CandidateVersions []store.ObservationVersionRef /* EVERY candidate version whose text was shown, including the observation's own previous version; one observation_version_lineage edge per entry (N79) */ }

type ProposalStore interface {
	// Store inserts ON CONFLICT (namespace_id, batch_key) DO NOTHING and returns the stored proposal — the
	// caller's own list when it won, an earlier attempt's otherwise (the stored list always wins).
	Store(ctx context.Context, tx store.Tx, p Proposal) (stored Proposal, err error)
	Load(ctx context.Context, tx store.Tx, batchKey [32]byte) (Proposal, error)
	Discard(ctx context.Context, tx store.Tx, batchKey [32]byte) error // only before any op was applied (FK RESTRICT)
}

type Applier interface {
	// Apply runs in one fenced tx over the STORED proposal: consolidation_applied(op_key) insert ON CONFLICT
	// DO NOTHING gates each op and commits with the effects (N43); every InputFactIDs row is re-read FOR SHARE
	// and must be live, AND every CandidateVersions entry is locked FOR SHARE and must satisfy
	// NOT derived_from_deleted AND hidden_by_invalidation = 0 AND retired_at IS NULL, else errs.ProposalDiscarded
	// (carrying the missing facts and the flagged versions) and nothing is written (N41, N79, N84); each new
	// version also writes its observation_version_lineage edges (one per CandidateVersions entry) and its
	// observation_version_sources rows (N85), and the batch is recorded with fact_consolidation rows, not a column
	// on facts (N95); new observation_versions get
	// effective_at = max(mentioned_at over inputs, effective_at over CandidateVersions, previous version) (D9)
	// and their observation_inputs rows; outbox events last (A-F1).
	// Statement order for an update (N57, review F-11): INSERT the new version, its observation_sources
	// (ON CONFLICT DO NOTHING) and observation_inputs FIRST, then DELETE the sources not in the new set — the
	// zero-sources trigger then always sees a non-empty set and cannot retire the observation inside its own
	// update. An update deletes only observation_sources rows the model was shown and dropped, never unshown ones
	// (N79: proof_count does not erode). Apply clears observations.stale_write/stale_delete; it NEVER touches
	// observation_versions.derived_from_deleted or hidden_by_invalidation — those are per version (N41, N84): the
	// new version is live, an older version written with since-deleted content stays hidden at every as_of. A
	// stale or flagged observation is rebuilt as a ROOT version from live sources (no flagged text in the prompt,
	// no lineage edge to a flagged ancestor), at most consolidate.max_rebuilds_per_round (default 50 per
	// namespace per wave, oldest first); the rest stay hidden (N79).
	Apply(ctx context.Context, tx store.Tx, p Proposal) (applied int, err error)
}

type Consolidator interface {
	Round(ctx context.Context, h *router.ShardHandle, scope store.Scope, o RoundOptions) (*RoundResult, error)
}
```

**(c) Dependencies.** `store`, `index`, `gateway`, `router`, `quota` (token meter and deferral), `errs`. **(d)
Swappable.** `LLMProposer` → `RecordReplayProposer` (tests) → `NoopConsolidator` (namespaces with consolidation
disabled). **(e) Test seam.** Model-based test derived from `Consolidation.tla` (§7): random at-least-once
re-execution of `Apply` must yield exactly one effect per `op_key`; the source-count trigger is tested on
testcontainers; T1 property test: any version reachable by lineage from a flagged version is flagged
(`DocLifecycle_NoLineage.cfg` must fail with the G-1 trace, W-2).

#### 2.2.15 `internal/reflect` — bounded agent loop (phase 2)

**(a) Responsibility.** D12: forced searches (observations, then facts), ≤ 10 free iterations, ≤ 100 k context
tokens, ≤ 300 s wall, per-tool deadline 10 s; tools `search_memories`, `search_observations`, `search_pages`
(full-text + semantic over `page_versions` through `PageService.SearchPages` — the Hindsight
`search_mental_models` step the loop previously lacked, N73), `get_page`, `expand_fact`; citations filtered to
ids actually returned by tools in the session; optional JSON-schema output validated server-side; per-namespace
mission/directives/disposition. At the context cap the loop does **not** force `done`: it runs a map/reduce
fallback (summarise each tool result into claims with citations, then synthesise from the claims — N73).

**(b) Interfaces.**

```go
package reflect

type Caps struct { MaxIterations int /* 10 (after forced searches) */; MaxContextTokens int /* 100_000 */; MaxWall time.Duration /* 300 s */; ToolDeadline time.Duration /* 10 s */; MapReduceAtCap bool /* true: at MaxContextTokens fold tool results into cited claims and synthesise from them instead of forcing done (N73) */ }

type Tool interface {
	Name() string
	Schema() json.RawMessage // JSON schema for arguments, derived from the proto request type
	Call(ctx context.Context, scope authz.RequestScope, args json.RawMessage) (ToolResult, error)
}

type ToolResult struct { Content string; ReturnedIDs []string /* feeds the citation verifier */; Tokens int }
type Registry interface { Register(t Tool); Get(name string) (Tool, bool); Specs() []gateway.ToolSpec }

type Event struct { Kind string /* token | tool_call | tool_result | citation | final | stats */; Text, ToolName string; Payload json.RawMessage } // → memoryv1.ReflectEvent
type EventSink func(ctx context.Context, e Event) error

type Request struct { Scope authz.RequestScope; Question string; AsOf *time.Time; OutputSchema json.RawMessage /* optional */; Persona Persona /* mission, directives, disposition{skepticism, literalism, empathy ∈ 1..5} */ }
type Result struct { Answer string; Citations []string /* verified subset */; Structured json.RawMessage; Usage gateway.Usage; Iterations int }

type Agent interface {
	Run(ctx context.Context, req Request, caps Caps, sink EventSink) (*Result, error)
}

type CitationVerifier interface {
	Verify(cited []string, returned map[string]struct{}) (kept, dropped []string)
}

type SchemaValidator interface {
	Validate(schema, doc json.RawMessage) error // errs.Validation with path details
}
```

Tools are thin wrappers over the recall planner and store (they run *inside* `engram-api` with the caller's
scope; no second authz path). Rejected: running reflect as a Temporal workflow (it is interactive, streamed and
≤ 300 s; durability would add latency without value).

**(c) Dependencies.** `recall`, `pages`, `store`, `gateway`, `authz`, `quota`, `errs`. **(d) Swappable.**
`LoopAgent` → `ScriptedAgent` (deterministic tool sequence for tests). **(e) Test seam.** Record/replay gateway
with tool-call transcripts; property: every emitted citation ∈ union of `ReturnedIDs`; caps tests with a fake
clock.

#### 2.2.16 `internal/pages` — mental models / knowledge pages (phase 3)

**(a) Responsibility.** D12 pages: source query + tag filter, `page_sources`, staleness for writes
(`stale_write`) and deletes (`stale_delete`), versioned markdown in blob (`effective_at` per D9), delta refresh
via LLM (prompt `page/v1`).

**(b) Interfaces.**

```go
package pages

type Page struct { PageID string; Name string; SourceQuery string; TagFilter []string; RefreshPolicy string /* "after_consolidation" | "cron:<expr>" | "manual" */; CurrentVersion int64; StaleWrite bool; StaleDelete bool }
type Delta struct { Added []Evidence /* facts/observations now matching the source query */; Removed []string /* ids no longer live */ }

type Service interface {
	Create(ctx context.Context, scope authz.RequestScope, p Page) (*Page, error)
	Get(ctx context.Context, scope authz.RequestScope, pageID string, version *int64, asOf *time.Time) (*Page, string /*markdown*/, error)
	List(ctx context.Context, scope authz.RequestScope, page store.Page) ([]Page, string, error)
	Search(ctx context.Context, scope authz.RequestScope, query string, f index.Filter, asOf *time.Time, page store.Page) ([]PageHit, string, error) // PageService.SearchPages / reflect `search_pages` (N73): BM25 ∪ HNSW over page_versions, RRF, no LLM
	Delete(ctx context.Context, scope authz.RequestScope, pageID string) error
	Refresh(ctx context.Context, h *router.ShardHandle, scope store.Scope, pageID string, reason string) (*RefreshResult, error)
}

type Refresher interface { // LLM delta edit
	Refresh(ctx context.Context, prev string, d Delta, persona reflect.Persona) (markdown string, u *gateway.Usage, err error)
}
```

`Refresh` runs as workflow `ns/{ns}/page/{page_id}` (signalled by `Consolidate` on completion or by a Temporal
schedule); a refresh with an empty delta is a no-op that clears the stale flags. **(c) Dependencies.** `store`,
`blob`, `recall` (source query), `gateway`, `router`, `errs`. **(d)/(e)** see §2.3.

#### 2.2.17 `internal/workflows` — Temporal workflows, activities, policies

**(a) Responsibility.** All workflow definitions and the activity *interfaces* they call; task-queue naming;
`ContinueAsNew` rules; retry policies; the client wrapper used by the API. Concrete activities are structs in
`cmd/engram-worker` wiring service packages into these interfaces (dependency rule §2.1).

**(b) Interfaces.**

```go
package workflows

func TaskQueue(shardID int32) string // "shard-{id}"
func RetainID(ns, op string) string   // "ns/{ns}/op/{op}"
func ConsolidateID(ns string) string  // "ns/{ns}/consolidate"
func PageID(ns, page string) string   // "ns/{ns}/page/{page}"
func MoveID(ns string, epoch int64) string // "move/{ns}/{epoch}"
func OpSweeperID(shardID int32) string     // "shard/{id}/op-sweeper" (schedule)

// Workflows (inputs/results are protos in engram.internal.workflow.v1).
func RetainDocument(ctx workflow.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.RetainDocumentResult, error)
func PurgeDocument(ctx workflow.Context, in *workflowv1.PurgeInput) (*workflowv1.PurgeResult, error) // PurgeNamespace: same input, PurgeTarget NAMESPACE
func Consolidate(ctx workflow.Context, in *workflowv1.ConsolidateInput) error   // long-lived; signal Nudge (ConsolidateTrigger); debounce 30 s; ContinueAsNew after every round (§5.2.1)
func PageRefresh(ctx workflow.Context, in *workflowv1.RefreshPageInput) (*workflowv1.RefreshPageResult, error)
func ExportSnapshot(ctx workflow.Context, in *workflowv1.ExportInput) (*workflowv1.ExportResult, error)
func Move(ctx workflow.Context, in *workflowv1.MoveInput) (*workflowv1.MoveResult, error)
func OpSweeper(ctx workflow.Context, in *workflowv1.OpSweeperInput) error

// Every activity input embeds workflowv1.WorkflowScope{namespace_id, tenant_id, shard_id, epoch, blob_prefix};
// activities re-check ownership through store.WithNamespaceTx (D4).
type RetainActivities interface {
	LoadItem(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.LoadItemResult, error) // assigns the APPEND base under the documents row lock (N56); reads the base's single `ver/{sha256}` body blob and writes this version's (N104)
	Chunk(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.ChunkPlan, error)
	SummarizeDocument(ctx context.Context, in *workflowv1.RetainDocumentInput) (*workflowv1.SummarizeDocumentResult, error)
	ExtractChunk(ctx context.Context, scope *workflowv1.WorkflowScope, w *workflowv1.ChunkWork) (*workflowv1.ExtractChunkResult, error)
	EmbedChunk(ctx context.Context, scope *workflowv1.WorkflowScope, w *workflowv1.ChunkWork, x *workflowv1.ExtractChunkResult) (*workflowv1.EmbedChunkResult, error)
	ResolveEntities(ctx context.Context, scope *workflowv1.WorkflowScope, x *workflowv1.ExtractChunkResult) (*workflowv1.ResolveEntitiesResult, error)
	BuildLinks(ctx context.Context, scope *workflowv1.WorkflowScope, x *workflowv1.ExtractChunkResult, e *workflowv1.EmbedChunkResult, r *workflowv1.ResolveEntitiesResult) (*workflowv1.BuildLinksResult, error)
	CommitChunk(ctx context.Context, in *workflowv1.CommitChunkInput) (*workflowv1.CommitChunkResult, error) // errors: errs.DocumentBusy (retryable, N83), errs.InputBlobMissing (non-retryable here; the WORKFLOW re-runs ExtractChunk + EmbedChunk, ≤ 2 re-runs, then chunk_failed — N100)
	FinalizeVersion(ctx context.Context, in *workflowv1.FinalizeVersionInput) (*workflowv1.FinalizeVersionResult, error)
}

type ConsolidateActivities interface {
	ConsolidateBatch(ctx context.Context, in *workflowv1.ConsolidateBatchInput) (*workflowv1.ConsolidateBatchResult, error)
	StoreProposal(ctx context.Context, in *workflowv1.StoreProposalInput) (*workflowv1.StoreProposalResult, error) // write-once; returns the stored list (N43)
	ApplyBatch(ctx context.Context, in *workflowv1.ApplyBatchInput) (*workflowv1.ApplyBatchResult, error)          // applied | already | discarded{missing_fact_ids} (N41)
	CheckQuota(ctx context.Context, scope *workflowv1.WorkflowScope) (*memoryv1.DeferredInfo, error) // non-nil → DEFERRED until window reset
}

type Client interface { // used by engram-api; wraps client.Client
	StartRetain(ctx context.Context, in *workflowv1.RetainDocumentInput) error // AlreadyStarted → nil
	StartPurge(ctx context.Context, in *workflowv1.PurgeInput) error
	SignalConsolidate(ctx context.Context, t *workflowv1.ConsolidateTrigger) error // SignalWithStart (Nudge)
	Cancel(ctx context.Context, workflowID string) error
	Describe(ctx context.Context, workflowID string) (*Status, error)
	WaitResult(ctx context.Context, workflowID string, timeout time.Duration) (done bool, err error) // WaitOperation long-poll (N70): GetWorkflow(id).Get bounded by timeout; the api caps open waits per process
}

// DataConverter returns the Temporal converter every client and worker is built with (N59, N99, review F-25,
// review-2 G-20): the default proto converter wrapped in a payload codec that encrypts each payload with AES-256-GCM
// under a PER-NAMESPACE data key wrapped by the shard key (key id in the payload metadata, namespace id in the
// workflow header; rotated by `engramctl`). Deleting the wrapped data key is the shredding step of a namespace or
// tenant delete. A shard-key or data-key version is destroyed only after rotation time + max workflow run timeout
// (7 d) + Temporal retention (7 d). Temporal UI/CLI users see ciphertext; a codec server for the UI is deliberately
// not deployed. Accurate claim: text appears in histories only as ciphertext, at most 4 KiB per activity result, plus
// ChunkWork.header/context/metadata_json/entity_hints; histories expire with retention 7 d and are unreadable after
// key destruction.
func DataConverter(keys KeyProvider) converter.DataConverter

type KeyProvider interface {
	DataKey(ctx context.Context, namespaceID string, keyID string) (key []byte, err error) // unwraps the namespace data key with the shard key
	CurrentKeyID(namespaceID string) string
	Shred(ctx context.Context, namespaceID string) error // deletes the wrapped data key (namespace/tenant delete)
}

// Payload rule (N59, review F-18): every activity result larger than 4 KiB travels by blob key —
// ExtractChunkResult.cache_key, EmbedChunkResult.staging_blob_key, ResolveEntitiesResult.result_blob_key,
// BuildLinksResult.result_blob_key — and CommitChunkInput is keys-only; ChunkWork carries no text (activities
// read the chunk from the manifest blob). A 50-fact chunk otherwise put ≈ 400 KiB into the history and 500
// chunks exceeded Temporal's 50 MB history limit 4×.
const MaxInlineResultBytes = 4 << 10
```

Per-chunk fan-out in `RetainDocument` uses a workflow-side semaphore of 32; `RetainDocument` `ContinueAsNew`s
**every 100 chunks or 20 MB of history**, whichever first (the SDK's history-size and event-count are read from
`workflow.GetInfo`; 100 chunks ≈ 2.5–3 k events, under the 10 k rule with margin — N59). `operations.progress`
is written once per wave by the workflow (`OperationRepo.SetProgress`, N69), not per chunk.

**Retry policies (summary; the consolidated table in §5.8 is authoritative where the two differ):**

| Activity | Initial | Backoff | Max attempts | Non-retryable error types |
|---|---|---|---|---|
| `Chunk` | 1 s | ×2, cap 10 s | 3 | `ValidationError` |
| `SummarizeDocument`, `ExtractChunk` | 2 s | ×2, cap 60 s | 8 | `PermanentLLMError`, `ValidationError` (after 1 re-prompt), `WrongShardOrEpoch` |
| `EmbedChunk` | 1 s | ×2, cap 30 s | 8 | `PermanentLLMError` (incl. wrong dims), `WrongShardOrEpoch` |
| `ResolveEntities`, `BuildLinks` | 1 s | ×2, cap 30 s | 5 | `WrongShardOrEpoch` |
| `CommitChunk`, `FinalizeVersion` | 1 s | ×1.5, cap 5 s | unlimited within a 10 min `ScheduleToClose` (`P-frozen`, §5.8) | `WrongShardOrEpoch`, `InputBlobMissing` (handled by the workflow, N100); `DocumentBusy` (100 ms) and the N82 `FENCE_BUSY` refusal (200 ms) are retryable; `NamespaceFrozen` is retryable because the mover terminates and restarts the workflow on the target within the freeze window (D5 step 5) |
| `ConsolidateBatch` | 5 s | ×2, cap 5 min | 6 | `PermanentLLMError`, `WrongShardOrEpoch`; `QuotaExceeded` → workflow sleeps until reset (`DEFERRED`) |
| `PurgeBlobs`, `PurgeRows` | 5 s | ×2, cap 10 min | unlimited (heartbeat) | `WrongShardOrEpoch` |
| Move `Copy` (resumable key ranges), `CatchUp` | 5 s | ×2, cap 5 min | 20 | `WrongShardOrEpoch` (fence broken → rollback) |
| Move `Freeze`, `CutoverBegin` | 1 s | ×2, cap 10 s | 5 | `MovePrecondition` (CAS lost → rollback) |
| Move `CutoverTarget`/`CutoverSource` (`P-cutover`) | 100 ms | ×1.5, cap 1 s | within 60 s | `MovePrecondition`; failure before (c) → rollback, after (c) never |
| Move `CutoverCatalog` (d) | 100 ms | ×1.5, cap 1 s | **unlimited, idempotent** (off every read path, N98) | `MovePrecondition` |
| `Export*` | 5 s | ×2, cap 5 min | 10 | `ValidationError` |

A `WrongShardOrEpoch` in any activity fails the workflow with a typed failure; the move executor restarts the
recorded operations on the target queue with the same ids (D5 step 6).

**(c) Dependencies.** `go.temporal.io/sdk`, `gen/go/engram/internal/workflow/v1`, `errs`. **(d) Swappable.**
Activities are interfaces; `FakeActivities` for `testsuite` workflow tests. **(e) Test seam.** Temporal
`testsuite.WorkflowTestSuite` with mocked activities: fan-out bound, idempotent re-execution, `ContinueAsNew` at
100 chunks and at 20 MB of history (measured through the SDK's history-size metric in T2), deferral on quota;
a payload test asserts no activity input or result in a recorded history exceeds `MaxInlineResultBytes` and
that every payload is ciphertext under the codec (`TestIso_Temporal_PayloadsEncrypted`) and that a namespace's payloads
are unreadable after `KeyProvider.Shred`; a chaos test on
testcontainers Temporal kills workers mid-`CommitChunk` and asserts no duplicate facts.

#### 2.2.18 `internal/outbox` — relay, cursors, gap watchlist, sinks

**(a) Responsibility.** D6: one active relay per shard elected with `pg_try_advisory_lock`, reading `outbox` in
`seq` order in batches of 500, driving named sinks with independent cursors, tracking skipped sequence numbers
in a gap watchlist for `2 × statement_timeout` and delivering a strict prefix (no cursor passes an open gap;
sound under A-F1: writers' outbox `INSERT` is their last statement and `statement_timeout =
idle_in_transaction_session_timeout = 30 s`).

**(b) Interfaces.**

```go
package outbox

type Event struct { Seq int64; NamespaceID string; Epoch int64; Type string; Payload *eventsv1.Event; CreatedAt time.Time }

// Group reassembles a paged event group (N80): pages of the same type and group key (document_id, deleted_at or
// batch_key) are complete only when all `page_count` pages have been seen; an `ids_elided` event is complete on its
// own and the consumer reads the ids from the store by document_id. The move replay does NOT use Group: it treats
// each page independently (N81).
type Group interface { Add(e Event) (complete bool); Ids() [][]byte; Elided() bool }

// Sink consumes events in seq order; Apply must be idempotent by (namespace_id, seq).
type Sink interface {
	Name() string // "index" | "kafka" — the move's "move:<ns>" cursor belongs to a pull consumer driven by the mover, not to a relay sink (N28)
	Apply(ctx context.Context, events []Event) error
	Filter() func(e Event) bool // optional per-sink filter; nil = every event
}

type RelayOptions struct { Batch int /* 500 */; PollInterval time.Duration /* 200 ms idle */; StatementTimeout time.Duration /* 30 s → gap watch 60 s */; LockKey int64 /* hashtext("engram-outbox-relay") — one per shard DB (§5.6) */ }
type Relay struct { /* handle, sinks, cursors, gaps, metrics */ }

func NewRelay(h *router.ShardHandle, sinks []Sink, o RelayOptions) *Relay
func (r *Relay) Run(ctx context.Context) error          // acquires the advisory lock on a dedicated conn; returns when lost
func (r *Relay) AddSink(ctx context.Context, s Sink, fromSeq int64) error // registers a push sink (index, kafka) at a cursor; the move's catch-up is not a sink (N28)
func (r *Relay) RemoveSink(name string) error
func (r *Relay) Lag(sink string) (int64, error)

type GapWatch interface { Note(seq int64, seenAt time.Time); Due(now time.Time) []int64; Resolve(seq int64) } // re-probe skipped seqs for 2 × statement_timeout; persisted in outbox_cursors.gaps (relay row, ≤ 1 000 entries, §3.5)
```

The relay holds a *shard-level* connection (`engram_relay` role, RLS-bypassing read on `outbox` only, since it
must see every namespace) — the one deliberate exception to "every connection has a namespace scope", justified
because the outbox carries no content beyond the proto payload the consumer needs and the relay never writes
namespace tables. Every event encodes to ≤ 16 KiB by construction (ids are 16-byte `bytes`, ≤ 256 per event, paged above that, elided
above 4,096 — N80); the relay still treats a payload that fails to decode as a poison event. Cursor advancement per sink is a separate transaction from sink `Apply`, so at-least-once
delivery is the contract and sinks are idempotent. Rejected: exactly-once via two-phase commit into the external
engine (not available; idempotent keys are cheaper and provable, §7 `Outbox.tla`).

**(c) Dependencies.** `store`, `router`, `index` (Sink adapter), `errs`, `telemetry`, Kafka client (optional
build tag `kafka`). **(d) Swappable.** Sinks: `IndexSink` (no-op for `Transactional` indexes), `KafkaSink`; the mover's
catch-up reads through `outbox.Reader` with its own `move:<ns>` cursor (N28). **(e) Test seam.** Model-based test derived from `Outbox.tla`: random commit orderings with holes;
assert every seq is delivered exactly once to each sink or declared aborted after the watch window, in order per
namespace.

#### 2.2.19 `internal/move` — namespace move orchestrator (D5)

**(a) Responsibility.** Implement the D5 state machine as Temporal workflow `move/{namespace_id}/{epoch}` on
task queue `shard-{target}` (new decision N2) with activities `Plan, Copy, CatchUp, VerifyDeep, Freeze, Drain, Verify,
VerifyFK, CutoverBegin/Target/Source/Catalog, Restart, Cleanup, Rollback` (§5.5.1), each re-checking the fence before
acting. This is the only code path allowed two shard handles at once; it never runs a statement that references both.

**(b) Interfaces.**

```go
package move

type Phase string // planned | copying | catching_up | frozen | cutover | cleaning | done | rolled_back

// Fence is asserted at the start of every activity against the catalog row, the source
// ownership row and the target ownership row; any mismatch → errs.WrongShardOrEpoch (non-retryable → Rollback).
type Fence struct { NamespaceID string; TenantID string; Source, Target int32; Epoch int64 /* e; target holds e+1 'incoming' until cutover (or a stale 'moved_out' row it is advanced from, N93) */; MoveID string; ExpectPhase Phase }

type Activities interface {
	Plan(ctx context.Context, f Fence) (*PlanResult, error)                       // target ownership(e+1, incoming) or moved_out → incoming (N93); source outbox_cursors + move_epoch (pauses purges and shard schedulers, N88/N97); catalog namespaces.state='moving'; fixes the freeze watchdog max(120 s, 60 s + 1 s per 10,000 live facts) ≤ 15 min (N90)
	Copy(ctx context.Context, f Fence) (*CopyResult, error)                       // copy barrier ONCE (N45 + D2 amended): EXCLUSIVE SESSION-level advisory lock on the mover's direct source connection under lock_timeout 5 s, then `BEGIN REPEATABLE READ; SELECT max(seq) …` (first statement fixes snapshot and p0 together), `pg_advisory_unlock`; p0 recorded once = the SINGLE replay floor (N88). Each table in key ranges (≤ 100,000 rows or ≤ 60 s, up to 4 parallel `facts` streams), each range under its own short REPEATABLE READ snapshot (N89), COPY into a session TEMP table on the target and `INSERT … SELECT … ON CONFLICT` under RLS with `SET LOCAL session_replication_role = replica`, per-range fence = engram_try_ns_fence + `state='incoming' AND epoch=e+1` (N91); resumes at the first range without a completion mark from `CopyProgress{table,last_key}`; blob prefix copy; heartbeats; the frozen re-copy class is NOT copied here
	CatchUp(ctx context.Context, f Fence, p0 int64) (*CatchUpResult, error)      // anti-join replay (N50) as ONE source-side statement against the source's lagging copy of move_applied: rows with seq > p0 AND NOT EXISTS move_applied, replayed through the TOTAL event → (table, key) mapping `replay_map.go` (N81; FK-closed, immutable tables DO NOTHING, mutable tables DO UPDATE of mutable columns IS DISTINCT FROM, ids_elided events by document_id), `move_applied … ON CONFLICT DO NOTHING RETURNING seq` in the same target tx (authoritative), then mirrored into the source copy and move_applied_seq updated for lag only; rounds until lag < 100 events or < 5 s; gives up after 10 rounds → Rollback (N45). Never `seq > applied` (review F-3)
	VerifyDeep(ctx context.Context, f Fence) error                               // pre-freeze, namespace still active (N90): per table count(*) and index-only bit_xor(hashtextextended(pk::text,0)) over the stable cohort created_at < W, plus a full-content sample (hash % 64 = 0, 1,000–20,000 rows per table); mismatch → one more catch-up round, then Rollback
	Freeze(ctx context.Context, f Fence) error                                    // catalog 'frozen'; source ownership 'frozen', freeze_reason='move' (same epoch) under the EXCLUSIVE advisory lock (lock_timeout 5 s, jittered retry); arms the watchdog until cutover (c) (N98)
	Drain(ctx context.Context, f Fence) (*DrainResult, error)                     // final anti-join pass (complete: no writer holds a seq after Freeze); re-copy the frozen class (quota_counters, namespace_stats, batch_jobs, outbox_cursors rows; ≤ 10,000 rows else MOVE_STATE_TOO_LARGE, N81); read in-flight operations from the SOURCE `operations` rows RUNNING/PENDING/DEFERRED — never ListWorkflow (N97); terminate their workflows on the source queue (same Temporal cluster, N71); record workflow ids
	Verify(ctx context.Context, f Fence) error                                    // freeze window (N90): count(*) both sides, PK hash of the delta cohort (created_at/updated_at ≥ W), 4,096 sampled rows' content, full PK hash for tables ≤ 2 M rows; hashes exclude updated_at and use sha256(embedding::bytea), never row_to_json
	VerifyFK(ctx context.Context, f Fence) error                                  // anti-join count of orphans per foreign key on the target, must be 0 — outside the freeze window and again over the rows touched by the final pass (N88)
	CutoverBegin(ctx context.Context, f Fence) error                              // (a) namespace_moves 'cutover' — records intent, NOT the point of no return (N98)
	CutoverTarget(ctx context.Context, f Fence) error                             // (b) target ownership 'active' @ e+1 — own activity, sub-second retry (N52)
	CutoverSource(ctx context.Context, f Fence) error                             // (c) source `frozen → moved_out`, freeze_reason NULL, target_shard_id + target_epoch recorded — THE point of no return (N98); watchdog cancelled here
	CutoverCatalog(ctx context.Context, f Fence) error                            // (d) catalog shard=target, epoch=e+1, state='active' + NOTIFY — retried indefinitely and idempotently, off every read path. Four txs in three databases, never one; requests in the (c)–(d) window follow the MOVED_OUT target hint (N98)
	Restart(ctx context.Context, f Fence, d *DrainResult) error                   // each recorded operation: ns/{ns}/op/{op} on shard-{target}, WorkflowIDReusePolicy TERMINATE_IF_RUNNING, memo epoch e+1; AlreadyStarted is done only on queue shard-{target} with memo epoch e+1; then the reconcile loop compares target operations with DescribeWorkflowExecution until stable (N97)
	Cleanup(ctx context.Context, f Fence) error                                   // after 24 h grace: MoveService.CleanupMove admin RPC → `engram_cleanup_namespace` (SECURITY DEFINER) deletes data rows, cursor, move_applied, old blob prefix; NEVER the ownership row (moved_out is a permanent fence value, N93)
	Rollback(ctx context.Context, f Fence, reason string) error                   // before (c) only: ReleaseNamespace(target, e+1) if (b) committed, then source 'frozen → active', catalog 'active'; target rows removed; move_epoch cleared (lifts the purge pause)
}

type Orchestrator interface { // admin surface (MoveService)
	Start(ctx context.Context, namespaceID string, target int32, o Options) (*Ref, error)
	Status(ctx context.Context, moveID string) (*Status, error)
	Abort(ctx context.Context, moveID string) error // rollback if before cutover sub-step (c), else error
}

type Options struct {
	DrainWait time.Duration     // 15 s default, 60 s max (D5 step 4)
	FreezeWatchdog func(liveFacts int64) time.Duration // max(120 s, 60 s + 1 s per 10,000 live facts), cap 15 min (N90); armed until cutover (c), not (a)
	CatchUpMaxLag int           // 100 events
	CatchUpMaxAge time.Duration // 5 s
	CatchUpMaxRounds int        // 10 (N45): then Rollback
	CleanupGrace time.Duration  // 24 h
	CopyRangeRows int           // 100 000 rows per key range (N88) …
	CopyRangeMaxAge time.Duration // … or 60 s, each range under its own short snapshot (alert at 5 min, N89)
	CopyStreams int             // up to 4 parallel `facts` streams (one per hash-partition group)
	FrozenClassMaxRows int      // 10 000 (N81)
	CutoverRetry RetryPolicy    // 100 ms / ×1.5 / 1 s, unlimited within a 60 s ScheduleToClose before (c); (d) unlimited (N52, N98)
	CutoverAlertAfter time.Duration // 2 s: "cutover in progress" alert while (c) has committed and (d) has not
}
```

Fencing details: `Copy` takes the **exclusive session-level advisory lock** (the keys of `engram_ns_lock_keys(ns)`)
on its *direct* source connection (N15: session locks do not survive transaction pooling) **once**, for the few
milliseconds it needs to open its first `REPEATABLE READ` snapshot (the copy barrier, N45, N88): the snapshot
transaction's first statement `SELECT max(seq) FROM outbox WHERE namespace_id = $ns` fixes the snapshot and `p0`
together while the lock is held, then `pg_advisory_unlock` releases it without ending the transaction — so no writer
that drew a lower `seq` can commit after it (TLC `ShardMove_NoBarrier`). Because every writer holds the *shared*
transaction-level lock to commit, the barrier waits exactly for the writers in flight when it was requested, and a
writer that arrives behind it is refused by its try-lock and retries after 200 ms (N82), no stream of new writers can
starve it and none holds a pooled connection while it waits (D2 as amended, review F-5; W-11). Later ranges use their
own short snapshots, not the barrier: `p0` stays the single floor and the skew between range snapshots is repaired by
the replay, which is total and FK-closed (N81); purges of the namespace are paused through `move_epoch` so no parent can
vanish between range snapshots (N88). The target is loaded **without `BYPASSRLS`** (N91): `COPY` into a session
`TEMP` table, then `INSERT … SELECT … ON CONFLICT` under `ns_isolation`, in a transaction that re-checks the target
row (`state='incoming' AND epoch=e+1`, after `engram_try_ns_fence`) and runs `SET LOCAL session_replication_role =
replica`; `engram_check_ownership` is `ENABLE ALWAYS`, and `VerifyFK` replaces the foreign-key triggers the load
skips. `CatchUp` and `Drain` replay by **anti-join** against `move_applied` (N50), which exists on *both* shards: the
target copy is authoritative (`ON CONFLICT DO NOTHING RETURNING seq` in the apply transaction), the source keeps the
mover's lagging copy written after each target commit, so the anti-join is a single source-side statement and
`move_applied_seq` is a lag indicator only. The namespace-confined `engram_move` cannot see other namespaces'
sequence numbers, so a hole between two of its own seqs is indistinguishable from another namespace's seq and a `seq >
applied` cursor silently skipped a lower seq that committed late (review F-3); the anti-join on `outbox_ns_seq_idx` +
the `move_applied` PK re-finds it on the next round, and after `Freeze` (no writer holds a seq) one pass is complete.
`Freeze` takes the same exclusive advisory lock and therefore waits for in-flight writers to finish — after it
commits, no new writer can pass the D2 check; it sets `freeze_reason = 'move'` (the `CHECK` constraint requires it).
`CutoverBegin` is a CAS on `namespace_moves.state='frozen'` and `CutoverCatalog` on `namespaces.epoch=e`, so a
concurrent restore-from-backup (which also bumps epoch) fails the CAS and the move fails loudly rather than fighting.
**Rollback is possible until sub-step (c)** (N98): a failure before (c), including a target outage after (b), calls
`ReleaseNamespace(target, e+1)` and thaws the source — safe because nothing routes to the target before (c); the
watchdog stays armed until (c) commits. After (c) the move can only complete: the `moved_out` row carries
`target_shard_id`/`target_epoch`, requests in the window follow that hint to the target without the catalog, and
sub-step (d) is retried indefinitely and idempotently, so a catalog failover (≥ 30 s) causes no read outage beyond
the N52 bounded loop. Sub-steps (b), (c), (d) are three activities with `CutoverRetry` (N52). One Temporal cluster
serves the fleet (N71), so `Drain` terminates source-queue workflows and `Restart` starts them on the target queue
through the same client even across cells. `Restart` re-executes the recorded operations with their original ids
(hence the same `operation_id`s) and the new epoch; per-chunk idempotency keys exclude the epoch (D11), so
already-committed chunks on the target (copied or replayed) are detected by content hash and skipped, not
re-extracted. Every shard-wide scheduler (op-sweeper, DEFERRED resumer, `purge-sweep`, `consolidate-sweep`,
`page-cron`, `outbox-trim`, stats sweeper) joins `namespace_ownership` and acts only on `active` rows (N97), so the
source never starts a copied operation.

**(c) Dependencies.** `catalog`, `router`, `store`, `blob`, `outbox` (MoveSink), `workflows` (client), `errs`,
`telemetry`. **(d) Swappable.** `TemporalOrchestrator` → `InlineOrchestrator` (runs phases sequentially
in-process for tests). **(e) Test seam.** Model-based test from `ShardMove.tla` (§7) with `MemoryCatalog` + two
`FakeTx` shards and randomised concurrent retain/consolidate/delete, including the F-3 interleaving (writer
draws seq 102, a later writer commits 103 first, the mover applies 103, 102 commits, freeze — the anti-join must
replay 102), a version flagged on the source during catch-up with no covering event (`ShardMove_DeleteDuringCatchup`),
an idempotency key not replayed (`ShardMove_RetryAfterCutover`), a per-table restart with a new snapshot under enforced
foreign keys (`ShardMove_PerTableNewSnapshot`) and a target outage after (b) (rollback by `ReleaseNamespace`); chaos test
on testcontainers kills the worker in every phase, including between `CutoverSource`
and `CutoverCatalog`, and a workflow started 0.5 s before the freeze, and asserts the invariants (one active owner, no loss/duplication, eventual
done/rolled_back, reads unavailable for less than the bounded re-resolve window).

#### 2.2.20 `internal/export` — snapshots for local agentic search (phase 3)

**(a) Responsibility.** D12 export layout under `{shard}/{tenant}/{ns}/export/v{n}/` (manifest,
`facts/observations/chunks.jsonl.zst`, `pages/*.md`, `delta-v{n-1}-v{n}.jsonl.zst` derived from the outbox
range) and 1 MiB part streaming through `ExportService`.

```go
package export

type Manifest struct { Version int64; CreatedAt time.Time; AsOf time.Time; OutboxSeq int64 /* for the next delta */; Files []FilePart /* name, size, sha256 */; Epoch int64; Expired bool; ExpiredAt *time.Time /* set by the delete cascade when a document contained in the snapshot was deleted (N59) */ }

type SnapshotBuilder interface {
	Build(ctx context.Context, h *router.ShardHandle, scope store.Scope, o BuildOptions) (*Manifest, error) // runs as ExportSnapshot workflow
}

type Streamer interface {
	Stream(ctx context.Context, scope authz.RequestScope, version int64, part func(ctx context.Context, p Part) error) error // 1 MiB parts, resumable by (file, offset)
	Latest(ctx context.Context, scope authz.RequestScope) (*Manifest, error)
}
```

Snapshots honour deletes twice over: a version is built from live rows only, and the delta lists
`retired`/`deleted` ids so a synced client removes them; and an **existing** snapshot that contains a
since-deleted document is marked `expired` by the delete cascade (`SnapshotRepo.ExpireContaining`, N59), after
which `Streamer.Stream` refuses it with `PreconditionFailed{SNAPSHOT_EXPIRED}` and the client re-syncs from a
fresh full snapshot — an acknowledged delete is never served through an old export (review F-13). The
`WriteFiles` snapshot is opened behind the exclusive advisory lock (the same barrier as the move, D2 as
amended), never behind a row lock. **(c)** `store`, `blob`, `outbox` (range read), `router`, `errs`. **(d)/(e)** see
§2.3.

#### 2.2.21 `internal/quota` — rates, token metering, deferral

```go
package quota

type Bucket string // BucketRecall | BucketRetain | BucketNone

type Key struct { TenantID string; NamespaceID string /* "" for tenant-level */; Bucket Bucket }
type Limits struct { RecallsPerMin, RetainsPerMin int64; LLMTokensPerDay int64; MaxFacts int64 }
type Decision struct { Allowed bool; RetryAfter time.Duration; Remaining int64 }

// Limiter is the in-process token bucket used by authz (D13). Per-process buckets are
// sized limit/N_api (N from Envoy's endpoint count, refreshed every 30 s): approximate, not global.
type Limiter interface {
	Allow(ctx context.Context, k Key, n int64) (Decision, error)
}

// Meter records token usage in the shard's token_usage table (moves with the namespace) and
// answers "may this operation spend more today?".
type Meter interface {
	Record(ctx context.Context, tx store.Tx, op string, u gateway.Usage, day time.Time) error
	Remaining(ctx context.Context, tx store.Tx, l Limits, day time.Time) (tokens int64, facts int64, err error)
}

// Deferral parks an operation until the window resets (state DEFERRED) rather than failing it.
type Deferral interface {
	Defer(ctx context.Context, tx store.Tx, operationID string, until time.Time, reason string) error
	Resume(ctx context.Context, tx store.Tx, operationID string) error
}
```

Rejected: a global rate limiter in Redis (extra system; per-process approximation is within the tolerance a
per-minute quota implies). **(c)** `store`, `gateway` (Usage), `errs`. **(d)/(e)** see §2.3.

#### 2.2.22 `internal/config` — layered configuration

```go
package config

type Layer map[string]json.RawMessage // one of: system (file), tenant (catalog JSONB), namespace (catalog JSONB)

type Models struct { Extract, Consolidate, Reflect, Embed, Rerank gateway.Model /* Rerank default bge-reranker-base (N53) */; EmbedDims int /* 768; 512 with Matryoshka truncation */ }
type Resolved struct {
	Models        Models
	Prompts       map[string]string // `prompts.*`: prompt-version pins per prompt family ("extract" → "extract/v1", …), N66
	Chunk         struct{ TargetChars, MinChars, MaxChars int }
	Retain        struct{ Mission string }                        // `retain.mission` (≤ 500 chars, rendered into extract/v1), N66
	Recall        struct{ DefaultBudget recall.Budget; RerankEnabled bool; RerankTop map[recall.Budget]int /* 0/50/150 default */ }
	Quota         quota.Limits
	Consolidation struct{ Enabled bool; Debounce time.Duration; Mission string; ObservationScope string /* combined | shared | per_tag | custom */; MaxObservationsPerScope int /* 2 000; -1 unlimited */ } // `consolidate.{mission, observation_scope, max_observations_per_scope}`, N66
	Reflect       struct{ KeepTranscripts bool }                  // `reflect.keep_transcripts`, N66
	Version       uint64 // hash of the merged layers; changes invalidate derived caches
}

// Keys is the single allow-list of configuration keys (name, type, layer, default, validator). The resolver's
// unknown-key rejection, the `Namespace.config_overrides` proto comment and the operator documentation are all
// generated from this table by `go generate`, so a key used by a pipeline or prompt cannot be missing from the
// schema again (N66, review F-26: `prompts.extract`, `retain_mission`, `observations_mission`,
// `consolidate.observation_scope`, `consolidate.max_observations_per_scope` and `profile.keep_transcripts` were
// read by §5/§6 but rejected by the resolver).
var Keys = []Key{ /* models.{extract,consolidate,reflect,embed,rerank}, prompts.*, chunk.target_chars, retain.mission, recall.default_budget, recall.rerank_top.*, consolidate.{enabled,debounce,mission,observation_scope,max_observations_per_scope}, reflect.keep_transcripts, quota.* */ }

type Resolver interface {
	Resolve(system, tenant, namespace Layer) (*Resolved, error) // namespace ⊃ tenant ⊃ system; unknown keys (not in Keys) → errs.Validation
}
```

Resolution happens once per catalog entry and is cached with it (D12); the `Version` hash is carried in activity
inputs so a config change mid-operation is visible in traces but does not change an in-flight activity's model
(idempotency keys include the model). **(c)** `gateway`, `quota`, `recall` (types), `errs`. **(d)/(e)** see
§2.3.

#### 2.2.23 `internal/telemetry` — tracing, metrics, label policy

```go
package telemetry

type ShardLabels struct{ Shard string } // "7"

// Metrics constructors enforce D13 as amended (N62): shard label mandatory, NEVER tenant, NEVER namespace.
// Registration with a forbidden label panics at init (caught by a unit test). A tenant label on the metering
// counters at 10 000 tenants × 8 ops × 5 models × 2 kinds was ≈ 800 k series per process (review F-22).
type Registry interface {
	Counter(name string, labels ...string) Counter
	Histogram(name string, buckets []float64, labels ...string) Histogram
	Gauge(name string, labels ...string) Gauge
}

// TenantMeter is the per-tenant signal that replaced the tenant label: it reads the exactly-once
// token_usage rollup (N25) and serves `engramctl report`; optionally it exports OpenTelemetry metrics with
// DELTA temporality (per-tenant attributes are fine for a backend built for that cardinality, not for Prometheus).
type TenantMeter interface {
	Report(ctx context.Context, tenantID string, from, to time.Time) (*UsageReport, error)
}

func StartStage(ctx context.Context, stage string) (context.Context, func(err error)) // one span per pipeline stage
```

Metric names: `engram_recall_stage_seconds{shard,stage}`, `engram_recall_rerank_skipped_total{shard,reason}`
(the rerank-skip SLO, N53), `engram_retain_chunks_total{shard, result}`,
`engram_outbox_lag_seconds{shard,sink}`, `engram_catalog_stale_seconds{}`,
`engram_llm_tokens_total{shard,op,model}` (metering; no tenant label — N62). **(c)** OTel SDK, Prometheus client. **(d)/(e)**
see §2.3.

#### 2.2.24 `internal/ledger` — append-only ingest ledger

```go
package ledger

type Entry struct {
	DocumentID string
	Version int64
	OperationID string
	Timestamp time.Time      // item timestamp
	Context string
	Tags []string
	Metadata json.RawMessage
	Body []byte              // inline if ≤ 64 KiB
	BodyBlobKey string       // "{prefix}ledger/{sha256}" otherwise
	BodySHA256 [32]byte
}
type Ref struct{ LedgerID string; Seq int64 }

type Writer interface {
	Append(ctx context.Context, tx store.Tx, e Entry) (Ref, error) // no Update/Delete exists; a DB trigger rejects both
}

type Reader interface { Get(ctx context.Context, tx store.Tx, ledgerID string) (*Entry, error); ListByDocument(ctx context.Context, tx store.Tx, documentID string, page store.Page) ([]Entry, string, error) }
```

Explicit document delete, namespace delete and tenant delete remove ledger rows via the admin role only (the trigger
admits `DELETE` for `engram_admin` and the move rollback path and nothing else, §3.1); a `REPLACE`/`APPEND`
retire never deletes ledger rows — the ledger holds only the per-request delta, and the superseded versions' `ver/{sha256}` body blobs are what the purge removes (N104). **(c)** `store`, `blob`, `errs`. **(d)/(e)** see §2.3.

#### 2.2.25 `adapters/mcp` — MCP server

**(a) Responsibility.** Expose per-namespace MCP endpoints `/mcp/{tenant_id}/{namespace_id}` with tools derived
from `memory.v1` (`recall`, `retain`, `reflect`, `get_memory`, `get_operation`, `list_documents`,
`delete_document`, `get_page`, `list_pages`, `search_pages` (N73) — the §4.6 mapping table is the golden list); read tools are always listed, write tools are listed only when the
JWT carries `memory.write` (the core re-checks, so listing is a UX nicety, not security).

```go
package mcp

type ToolDef struct { // generated from the proto method + its request message
	Name        string
	Method      string          // "/memory.v1.MemoryService/Recall"
	InputSchema json.RawMessage // protojson schema of the request minus tenant_id/namespace_id (bound by the endpoint)
	Scope       authz.Scope
	Streaming   bool            // stream → collected into one MCP result with progress notifications
}

type Server struct { /* client conn to engram-api, tool table, endpoint router */ }

func New(core CoreClient, tools []ToolDef, o Options) *Server
func (s *Server) Handler() http.Handler // streamable HTTP; JWT from Authorization header forwarded as gRPC metadata

type CoreClient interface { // thin: generated gRPC clients only
	Memory() memoryv1.MemoryServiceClient
	Document() memoryv1.DocumentServiceClient
	Page() memoryv1.PageServiceClient
	Operation() memoryv1.OperationServiceClient
}
```

Tool schemas are generated at build time from the protos (`buf generate` plugin `protoc-gen-engram-mcp`,
in-repo) so the MCP surface cannot drift from the API. Rejected: hand-written tool schemas. **(c)** `gen/go`,
`authz` (scope names), `errs` (code → MCP error mapping), MCP Go SDK (A-10). **(d)** `Server` over real conn →
over `bufconn` to an in-process `api.Server` (tests). **(e)** Isolation matrix (§8) runs every tool with foreign
tenant/namespace tokens and asserts `NOT_FOUND`/`PERMISSION_DENIED` parity with gRPC.

#### 2.2.26 `adapters/connect` — ConnectRPC handlers

Mounted on the same `http.ServeMux` as the gRPC server (h2c, one port) using the generated
`memoryv1connect.New*Handler` constructors with the `authz.Interceptor.Connect()` and `api.DeadlineGuard`
(Connect's `Connect-Timeout-Ms` header maps to the deadline). Justification for Connect over grpc-gateway: same
handler implementation, no second proto annotation layer, native streaming over HTTP/1.1 and browsers, and the
JSON mapping is protojson. Rejected: grpc-gateway (separate reverse proxy, REST annotations to maintain).

```go
package connect

func Mount(mux *http.ServeMux, s *api.Server, interceptors ...connect.Interceptor) // registers every service; paths "/memory.v1.MemoryService/Recall"
```

**(c)** `gen/go` (Connect stubs), `api` (the `Server` type only — this is the one adapter that lives in-process;
it does not implement logic), `authz`, `errs`. **(d)/(e)** see §2.3.

#### 2.2.27 `cmd/engramctl` — operator CLI

Cobra commands (each maps to `memory.admin.v1` or to direct DB access with an admin role):

| Command | Does |
|---|---|
| `shard provision --id 7 --cell a --dsn …` (operator alias: `shard add`, §9.5) | creates DB, roles (`engram_migrate` owner/DDL only, `engram_app` NOBYPASSRLS, `engram_relay`, `engram_move` NOBYPASSRLS — the BYPASSRLS loader role `engram_move_load` of N51 is removed, N91 —, `engram_admin`; sets the role `lock_timeout` defaults of N82 and `GRANT SET ON PARAMETER session_replication_role TO engram_move`), extensions, partitions, RLS policies; registers in the catalog as `provisioning` |
| `index --shard 7 --namespace <ns>` | owner-run DDL as `engram_migrate`: `CREATE INDEX CONCURRENTLY facts_hnsw_ns_<12 hex> … WHERE namespace_id = '<ns>'` on the namespace's partition when the stats sweeper flipped `large` (N55); `index drop` at purge or move cleanup. The only path that creates the per-namespace partial HNSW — api and worker never run DDL |
| `shard migrate --id 7 [--all --cell a]` | goose migrations per shard with lock; `--canary` runs one shard and stops (§9) |
| `shard activate / full / drain / readonly / retire` | catalog `SetShardState` over the aligned states `provisioning → active ↔ full ↔ draining → readonly → retired` (review F-27) |
| `report --tenant acme --from … --to …` | per-tenant token/cost report from the exactly-once `token_usage` rollup — the replacement for the `tenant` metric label (N62) |
| `shard stats --id 7` | live facts, partitions sizes, index bloat, outbox lag |
| `namespace create / move / purge` | `CreateNamespace`; `MoveService.StartMove`; post-grace source purge (`MoveService.CleanupMove`) |
| `move status / abort / cleanup` | `MoveService.GetMove`, `RollbackMove`, `CleanupMove` |
| `backup create --shard 7` / `restore --shard 7 --to …` | pgBackRest stanza per shard under `_backups/shard-{id}/` (N23); restore bumps the epoch of every namespace on the shard (D1) and re-applies the `deletion_log` entries recorded after the backup point (N21, §9.3) |
| `outbox trim --shard 7` | manual trigger of the daily trim |
| `catalog flush-cache` | admin RPC that broadcasts a synthetic `catalog_changes` flush |
| `entity split` (phase 3) | repair a wrong merge |

**(c)** `catalog`, `store` (admin role), `blob`, `gen/go/memory/admin/v1`. **(d)/(e)** each command is a
function over interfaces; tests run against testcontainers Postgres.

### 2.3 Module summary table

| Module | Primary interface | Default impl | Alternate impl | Isolation test strategy |
|---|---|---|---|---|
| `internal/api` | generated `*ServiceServer` | `api.Server` | — | bufconn + httptest, golden error details |
| `internal/authz` | `TokenVerifier`, `Interceptor` | `JWKSVerifier` | `StaticKeyVerifier`, `AllowAllVerifier` | table tests; cross-tenant matrix |
| `internal/catalog` | `Catalog`, `Resolver` | `PostgresCatalog` + `LRUResolver` | `MemoryCatalog`, `StaticResolver` | rapid state-machine test; testcontainers |
| `internal/router` | `ShardRouter` | `StaticRouter` | `SingleShardRouter`, `RecordingRouter` | shard-cardinality assertions |
| `internal/gateway` | `Client` | `HTTPClient` | `RecordReplayClient`, `DeterministicClient` | golden JSON per prompt hash |
| `internal/blob` | `Store` | `S3Store` | `FSStore`, `MemStore` | conformance suite; prefix-escape tests |
| `internal/store` | `Pool`, `Tx`, repos | `PgxPool` | `RecordingPool`, `FakeTx` | testcontainers + RLS/append-only trigger tests |
| `internal/index` | `Index` | `PostgresIndex` | `TsvectorIndex`, `ExternalIndex`, `MemIndex` | retrieval goldens across impls; as_of leakage |
| `internal/chunk` | `Chunker` | `CDCChunker` | `FixedChunker`, `SentenceChunker` | golden boundaries; rapid properties |
| `internal/extract` | `Extractor`, `Cache` | `LLMExtractor` + `BlobCache` | `BatchExtractor`, `RuleExtractor`, `MemCache` | record/replay; cache-key tests |
| `internal/entity` | `Resolver` | `TrigramResolver` | `ExactResolver` | testcontainers; idempotency property |
| `internal/link` | `Linker` | `BudgetLinker` | `NoSemanticLinker` | `MemIndex` + `FakeTx`; budget properties |
| `internal/recall` | `Planner`, `Arm`, `Fuser`, `Reranker`, `Booster`, `Packer` | five arms, `RRFFuser`, `GatewayReranker`, `BoundedBooster`, `GreedyPacker` | `NoopReranker`, `FakePlanner` | Lean-mirrored rapid properties; deadline tests with fake clock |
| `internal/consolidate` | `Consolidator` (+ `Proposer`, `Applier`) | `LLMProposer`, `TxApplier` | `RecordReplayProposer`, `NoopConsolidator` | model-based from `Consolidation.tla` |
| `internal/reflect` | `Agent`, `Tool` | `LoopAgent` | `ScriptedAgent` | transcript replay; citation property |
| `internal/pages` | `Service`, `Refresher` | `PageService`, `LLMRefresher` | `NoopRefresher` | testcontainers; staleness flag tests |
| `internal/workflows` | workflow funcs, `*Activities` | Temporal SDK | `FakeActivities` | `testsuite`; chaos on testcontainers Temporal |
| `internal/outbox` | `Relay`, `Sink` | `Relay` + `IndexSink` | `KafkaSink`, `MoveSink`, `MemSink` | model-based from `Outbox.tla` |
| `internal/move` | `Orchestrator`, `Activities` | `TemporalOrchestrator` | `InlineOrchestrator` | model-based from `ShardMove.tla`; phase-kill chaos |
| `internal/export` | `SnapshotBuilder`, `Streamer` | blob-backed | `MemStore`-backed | manifest goldens; resume tests |
| `internal/quota` | `Limiter`, `Meter`, `Deferral` | token bucket + `token_usage` | `UnlimitedLimiter` | fake clock; deferral round-trip |
| `internal/config` | `Resolver` | JSON merge | — | golden merges; unknown-key rejection |
| `internal/telemetry` | `Registry` | OTel + Prometheus | `NoopRegistry` | label-policy unit test |
| `internal/ledger` | `Writer`, `Reader` | Postgres + blob overflow | `FakeTx` | append-only trigger test |
| `adapters/mcp` | `Server`, `ToolDef` | MCP SDK server | bufconn-backed | isolation matrix parity with gRPC |
| `adapters/connect` | `Mount` | Connect handlers | — | same goldens as gRPC |
| `cmd/engramctl` | cobra commands | — | — | testcontainers per command |

### 2.4 Error taxonomy — `internal/errs`

Decision: a tiny leaf package `internal/errs` (new decision N1), not `internal/api/errors.go`, because store,
services and workflows must *produce* typed errors while the dependency rule forbids them from importing
`internal/api`. `errs` imports only the generated detail messages from `memory/v1/errors.proto` (§4) and the
gRPC/Connect status packages.

```go
package errs

// Kind is the stable classification; each Kind maps to exactly one gRPC code.
type Kind int

const (
	KindValidation         Kind = iota + 1 // INVALID_ARGUMENT  + memoryv1.ValidationError{field_violations}
	KindNotFound                           // NOT_FOUND         + memoryv1.NotFound{resource, id}
	KindQuotaExceeded                      // RESOURCE_EXHAUSTED+ memoryv1.QuotaExceeded{quota, limit, retry_after} (+ google.rpc.RetryInfo)
	KindWrongShardOrEpoch                  // FAILED_PRECONDITION + memoryv1.WrongShardOrEpoch{namespace_id, expected_epoch, actual_epoch, namespace_state, target_shard_id, next_epoch} (the last two only for MOVED_OUT, API-internal, N98)
	KindNamespaceFrozen                    // FAILED_PRECONDITION + memoryv1.NamespaceFrozen{namespace_id, retry_after, reason} (also the N82 FENCE_BUSY refusal, 200 ms)
	KindDocumentBusy                       // ABORTED (never public) + memoryv1.DocumentBusy{document_id, retry_after}; retryable under P-frozen, 100 ms (N83)
	KindInputBlobMissing                   // never public; memoryv1.InputBlobMissing{blob_key, input, content_hash, attempt}; non-retryable as an activity error, the workflow re-runs extract + embed ≤ 2 times (N100)
	KindOperationConflict                  // ABORTED (concurrent conflict) or ALREADY_EXISTS (idempotency-key reuse, D1) + memoryv1.OperationConflict{operation_id, existing_operation_id, reason}
	KindPreconditionFailed                 // FAILED_PRECONDITION + memoryv1.PreconditionFailed{violations[]{type, subject, description}}
	KindUnavailable                        // UNAVAILABLE       + google.rpc.RetryInfo
	KindPermanentLLM                       // INTERNAL (never retried) + google.rpc.ErrorInfo{reason:"PERMANENT_LLM_ERROR"}
	KindUnauthenticated                    // UNAUTHENTICATED
	KindPermissionDenied                   // PERMISSION_DENIED + google.rpc.ErrorInfo{reason:"MISSING_SCOPE"}
	KindDeadline                           // DEADLINE_EXCEEDED
	KindInternal                           // INTERNAL
)

type Error struct { Kind Kind; Msg string; Detail proto.Message /* the typed detail from memory/v1/errors.proto, may be nil */; Retry time.Duration /* 0 = no RetryInfo */; Cause error }

func (e *Error) Error() string
func (e *Error) Unwrap() error
func (e *Error) GRPCStatus() *status.Status // implements the interface grpc-go looks for

// Constructors (the only way to create typed errors).
func Validation(field, desc string) *Error
func NotFound(resource, id string) *Error
func QuotaExceeded(quota string, limit int64, retryAfter time.Duration) *Error
func WrongShardOrEpoch(ns string, expected, actual int64, state memoryv1.NamespaceState) *Error
func WrongShardOrEpochMovedOut(ns string, expected, actual int64, targetShard int32, nextEpoch int64) *Error // carries the routing hint (N98); StripShardHint(err) removes it before the error leaves the API
func NamespaceFrozen(ns string, retryAfter time.Duration, reason memoryv1.FreezeReason) *Error
func DocumentBusy(documentID string) *Error
func InputBlobMissing(blobKey, input string, contentHash [32]byte, attempt int) *Error
func OperationConflict(operationID, existingOperationID string, reason memoryv1.OperationConflictReason) *Error
func PreconditionFailed(typ, subject, desc string) *Error
func Unavailable(desc string, retryAfter time.Duration) *Error
func PermanentLLM(model, status string, cause error) *Error
func Wrap(kind Kind, msg string, cause error) *Error

// Predicates used by workflows and the router.
func Is(err error, k Kind) bool
func IsRetryable(err error) bool // Unavailable, Deadline, NamespaceFrozen, DocumentBusy (bounded by the caller)
func Retryable(err error) bool   // alias kept for Temporal's ApplicationError classification

// Mapping helpers.
func ToStatus(err error) *status.Status        // unknown errors → INTERNAL with a redacted message; details attached
func ToConnect(err error) *connect.Error       // same codes/details via connect.Error.AddDetail
func FromStatus(st *status.Status) *Error      // client side (router.Forward, engram-mcp)
func TemporalType(err error) string            // ApplicationError type name: "WrongShardOrEpoch", "NamespaceFrozen", "DocumentBusy", "InputBlobMissing", "ValidationError", "PermanentLLMError", "QuotaExceeded"
```

Mapping table (authoritative for §4 and for the retry policies in §2.2.17):

| `errs.Kind` | gRPC code | Typed detail (`memory.v1`) | Retryable | Produced by |
|---|---|---|---|---|
| `Validation` | `INVALID_ARGUMENT` | `ValidationError` | no | api, chunk, extract, config, index (tags) |
| `NotFound` | `NOT_FOUND` | `NotFound` | no | authz (cross-tenant namespace, by design), store |
| `QuotaExceeded` | `RESOURCE_EXHAUSTED` | `QuotaExceeded` + `RetryInfo` | yes (after `retry_after`) | quota via authz (rates); workflows defer instead of failing (tokens/facts) |
| `WrongShardOrEpoch` | `FAILED_PRECONDITION` | `WrongShardOrEpoch` | writes: once (router re-resolve); `MOVED_OUT`: follow the target hint first, reads then a bounded loop ≤ 5 s (N52, N98) | store ownership check, move fence |
| `NamespaceFrozen` | `FAILED_PRECONDITION` | `NamespaceFrozen` | bounded (≤ 30 s) | store ownership check (write mode); the fence try-lock refusal (`FENCE_BUSY`, N82) |
| `DocumentBusy` | `ABORTED` (write path only) | `DocumentBusy` | yes (100 ms, within `P-frozen`) | `CommitChunk` shared try-lock on `engram_doc_lock_keys` (N83) |
| `InputBlobMissing` | — (activity error only) | `InputBlobMissing` | not by Temporal; the workflow re-runs extract + embed ≤ 2× (N100) | `CommitChunk` reading a purged cache/staging blob |
| `OperationConflict` | `ABORTED` (concurrent conflict) / `ALREADY_EXISTS` (`IDEMPOTENCY_KEY_REUSED`, D1) | `OperationConflict` | no | api idempotency (same `request_id`/`operation_id`, different payload) → `ALREADY_EXISTS`; purge, cutover or refresh in progress → `ABORTED` |
| `PreconditionFailed` | `FAILED_PRECONDITION` | `PreconditionFailed` | no | catalog CAS, move Cutover, field-mask/version checks |
| `Unavailable` | `UNAVAILABLE` | `RetryInfo` | yes | catalog miss when down, gateway 429/5xx, Temporal start failure, shard down |
| `PermanentLLM` | `INTERNAL` | `ErrorInfo` | no | gateway 4xx, wrong embedding dims, schema-invalid output after re-prompt |

Every `Kind` has one code, except `OperationConflict`, whose `reason` selects `ABORTED` or `ALREADY_EXISTS`; the reverse is not true (`FAILED_PRECONDITION` carries three details), which
is why clients must switch on the detail type, not the code (§4). Rejected: string-matching error messages in
workflows (the Temporal non-retryable list must be by type).

### 2.5 Concurrency and resource limits per process

| Limit | Value | Where enforced | Rationale |
|---|---|---|---|
| Extraction concurrency (LLM structured calls) | 32 in flight per `RetainDocument`; 32 per worker process across workflows (`gateway` per-model semaphore) | workflow semaphore + `gateway.RateLimiter` | D3 formula; more only burns the RPM cap |
| Embedding concurrency | 64 in flight per process; batch ≤ 64 texts per call | `gateway.RateLimiter` | embedding is cheap and fast; cap protects the gateway, not us |
| Per-shard Postgres pool | 16 conns per process per shard; ≤ 32 shards per cell → ≤ 512 per process; pgbouncer `default_pool_size=24` per shard, `max_client_conn=500` (§9.1) | `router.Options.PoolSize` | D3 cell bound |
| Recall arm parallelism | 5 arms, each one pooled conn; ≤ 50 QPS/shard target → ≤ 250 concurrent arm queries per shard ≤ pool headroom with p95 60 ms | `recall.Planner` | keeps the pool below saturation at target QPS |
| Rerank depth | 0 / 50 / 150 pairs by budget (N53), ≤ 300 per gateway call (D15); 1 call per recall; the cell must sustain ≥ 80 k pairs/s (A-R1); skipped below a 106 ms remaining deadline (N106) | `recall.GatewayReranker`, `config.Resolved.Recall.RerankTop` | review F-6: the reranker is a sized dependency |
| Graph arm | two seeded waves × 30 ms sub-budget, shared node budget 100/300/1000 | `recall.Planner` | N54 critical path |
| `WaitOperation` long-polls | ≤ 2 000 parked on Temporal per API process; beyond: jittered DB poll | `api.Options.MaxOpenWaits` | N70 |
| Temporal history | continue-as-new every 100 chunks or 20 MB; no inline activity result > 4 KiB | `workflows` | N59 |
| Outbox relay batch | 500 rows per read; 1 relay per shard; sinks applied sequentially | `outbox.RelayOptions.Batch` | D6 |
| Gap watchlist | ≤ 1 000 entries, persisted in `outbox_cursors.gaps` (§3.3.2); entries expire after `2 × statement_timeout = 60 s` | `outbox.GapWatch` | bounded and lossless across relay failover; beyond the cap the relay stops advancing and alerts |
| Temporal pollers | 2 workflow + 2 activity pollers per `shard-{id}` queue; max concurrent activities 64 per worker (§5.1.6, §9.1) | worker options | D3 |
| Consolidation | 8 facts per LLM call, ≤ 100 facts per round, 1 round in flight per namespace | `Consolidate` workflow (singleton id) | D12 |
| Reflect | ≤ 10 iterations, ≤ 100 k context tokens, ≤ 300 s, tool deadline 10 s, ≤ 4 concurrent reflects per namespace (`RESOURCE_EXHAUSTED` beyond) | `reflect.Caps`, api semaphore | D12 |
| Move copy | key ranges of ≤ 100 000 rows or ≤ 60 s, each under its own short snapshot (alert at 5 min), up to 4 parallel `facts` streams, loaded as `engram_move` through a `TEMP` table under RLS in replica mode (N88, N89, N91); barrier taken once = session-level exclusive lock on a direct connection under `lock_timeout` 5 s; 1 move in flight per namespace; ≤ 4 concurrent moves per cell; cutover sub-steps retried sub-second, (d) indefinitely (N52, N98) | `move.Options`, admin API | keeps freeze windows short, source `xmin` pinning bounded and source IOPS bounded |
| Fence acquisition | writers never wait: `pg_try_advisory_xact_lock_shared`, refusal → `NamespaceFrozen{FENCE_BUSY, 200 ms}`; exclusive takers under `lock_timeout` 5 s with jittered retry; role `lock_timeout` 2 s (`engram_app`, `engram_worker`) / 10 s (`engram_move`, `engram_admin`) | `store.WithNamespaceTx`, role defaults | N82: no pooled connection is held behind a queued freeze |
| Per-document lock | `CommitChunk` shared try-lock (`DocumentBusy`, 100 ms); `FinalizeVersion`, cascade, `PurgeDocument` exclusive under 5 s | `store.DocLockRepo` | N83: no `FOR SHARE` multixact churn, no ack latency behind ingest |
| Outbox event size | ≤ 256 ids and ≤ 16 KiB per event; paged; elided above 4,096 ids | `store.OutboxRepo.PagesOf`, `outbox.Group` | N80: the CHECK is only a backstop |
| Catalog resolver | LRU 100 k entries; single-flight per key; LISTEN reconnect backoff 1 s → 30 s | `catalog.ResolverOptions` | D4 |
| Embedding LRU (queries) | 10 000 entries per process, keyed `(namespace_id, sha256(prefixed text))` | `recall.QueryEmbedder` | repeat queries skip the 25 ms hop |
| Streaming / request size | `Recall` batches of 10; `StreamSnapshot` 1 MiB parts; `Retain` ≤ 100 items, ≤ 8 MiB total and ≤ 1 MiB per item (§4.1.8), raw body > 64 KiB to blob (N7) | `api` helpers and validation | D10, D12; keeps the ledger tx small |

### New decisions (beyond the register)

| Id | Decision | Rationale | Rejected |
|---|---|---|---|
| N1 | Add leaf package `internal/errs` to the D14 layout: typed errors + gRPC/Connect/Temporal mapping; nothing under `internal/` may be imported by it. *(adopted as N1 in the register)* | Store, services and workflows produce typed errors but may not import `internal/api`. | `internal/api/errors.go` (violates the dependency rule). |
| N2 | The move workflow `move/{namespace_id}/{epoch}` runs on task queue `shard-{target}`; the executing worker opens a second, move-scoped pool to the source (`engram_move` role: read + outbox read + ownership `UPDATE` under the exclusive advisory lock) and loads the target through a `TEMP` table under RLS in replica mode with the same role (N91; the `engram_move_load` role of N51 is removed). It is the only code path allowed two shard handles. *(adopted as N2 in the register)* | Keeps "one task queue per shard"; the target worker is the one that must be healthy for the move to be useful. | A cell-wide `moves` queue (breaks the per-shard queue rule; harder to reason about pollers). |
| N3 | Per-shard Temporal schedule `shard/{id}/op-sweeper` (every 60 s) starts a workflow for any `PENDING` operation older than 2 min with no workflow; the API still returns `UNAVAILABLE` when `StartWorkflow` fails after the ledger commit. *(adopted as N3 in the register)* | Closes the crash window between the ledger commit and `StartWorkflow` without weakening the ack. | Ack before `StartWorkflow` and rely solely on the sweeper (silently extends visibility lag). |
| N4 | The outbox relay uses a shard-level `engram_relay` role that bypasses RLS for `SELECT` on `outbox` only (`outbox_cursors` has no namespace column); events carry every field a consumer needs, so the relay never reads another table. *(adopted as N4 in the register, reworded as here)* | The relay must read every namespace's events in `seq` order; per-namespace scans would be O(namespaces) per batch. | Running the relay under `engram_app` with a loop over namespaces. |
| N5 | A cross-tenant namespace returns `NOT_FOUND`; a same-tenant namespace outside the allowlist, or a missing scope, returns `PERMISSION_DENIED`; `deleting` returns `FAILED_PRECONDITION`. *(adopted as N5 in the register)* | Prevents namespace-id enumeration across tenants while keeping same-tenant misconfiguration diagnosable. | `PERMISSION_DENIED` for cross-tenant too (an existence oracle); `NOT_FOUND` for same-tenant (hides the real problem). |
| N6 | Chunk `content_hash` = `sha256(text)` excluding the contextual header; the header hash is compared separately at `FinalizeVersion` to decide re-embedding. *(adopted as N6 in the register)* | Delta retain and the extraction cache survive summary drift. | Hashing header+text. |
| N7 | Retain items with a raw body > 64 KiB store the body in blob (`…/ledger/{sha256}`) *before* the ledger transaction; the ledger row keeps the hash and key. *(adopted as N7 in the register)* | Keeps the ack transaction small; content addressing makes the pre-write idempotent. | Inline bodies of any size (bloats the ledger table and the tx). |
| N8 | Streams fix `RequestScope` at open; token expiry mid-stream does not abort the stream. *(adopted as N8 in the register)* | Reflect may legitimately run 300 s. | Per-message re-verification. |

**Review follow-ups applied (reviews/round-1.md → register D20 → this section):**

| Finding | Register | What changed here |
|---|---|---|
| F-5 | D2 (amended) | `store.WithNamespaceTx` write prelude = shared advisory lock + plain `SELECT state, epoch`; `OwnershipRepo.LockExclusive` (transaction-scoped: freeze/cutover/rollback) and `LockExclusiveSession`/`UnlockSession` (session-level on a direct connection: copy barrier and export snapshot, released after the snapshot's first statement); row-lock wording removed from §2.2.7, §2.2.19, N2. |
| F-3, F-4 | N50, N51 | `OutboxRepo.ReadNamespaceUnapplied` (anti-join as one source-side statement against the source's lagging copy of `move_applied`); `MoveAppliedRepo` (target copy authoritative with `ON CONFLICT` in the apply tx, source copy mirrored after commit, `move_applied_seq` lag only); `move.Activities.CatchUp(p0)`/`Drain` replay by anti-join; `engram_move_load` BYPASSRLS loader with per-batch fence = shared advisory lock + `incoming @ e + 1`; `engramctl shard provision` creates the role. |
| F-20 | N52 | `CutoverBegin/Target/Source/Catalog` activities with `CutoverRetry`; `router.Retry` bounded re-resolve (≤ 5 s) for reads on `MOVED_OUT`; `errs` mapping row. |
| F-6, F-7 | N53, N54 | `rerank_top` 0/50/150, `bge-reranker-base` default, `engram_recall_rerank_skipped_total` as an SLO breach; two-wave graph seeding with `GraphWaveBudget`; 216 ms critical path. |
| F-8 | N55 | `index.SemanticPlan` (exact scan < 20 k facts, partial HNSW when `large`, partition fallback), `PlanSemantic`, `EfSearch ≥ cap`; the partial index itself is owner-run DDL (`engramctl index` as `engram_migrate`, queued when the sweeper flips `large`), never created by api/worker. |
| F-21 | D4 (amended) | `ResolverOptions.StaleMax = 0` (serve existing entries indefinitely), negative entries ≤ 10 min, only misses fail. |
| F-22 | N62 | No `tenant` metric label; `MeteringCounter` removed; `TenantMeter`/`engramctl report`; label linter rejects `tenant`. |
| F-26 | N66 | `config.Resolved` gains `Prompts`, `Retain.Mission`, `Consolidation.{Mission, ObservationScope, MaxObservationsPerScope}`, `Reflect.KeepTranscripts`; one `Keys` table generates the allow-list and proto comment. |
| F-29 | N65 | `authz.Claims.NamespaceGroups` (`ns_group`); allowlist check includes the group. |
| F-27, F-39 | N70 | `MethodPolicy.AllowDeleting` for `OperationService.Get/Wait`; `api.OperationWaiter` + `workflows.Client.WaitResult` (Temporal long-poll, per-process cap, jittered DB fallback). |
| F-31, F-28 | N67 | `Fuser` sums in `math/big.Rat`; `Fused.ArmScores`/`index.Hit.Score` per-query normalised to [0, 1]. |
| F-32 | N68 | `FactRepo.NearestByOccurrence` two-sided btree probe for the temporal arm. |
| F-35, F-42 | N71 | `Forwarder.Stream` (gRPC `ClientStream` proxy); external Envoy strips `engram-forward-*`; `api.Options.ReflectionServices = ["memory.v1"]`; one Temporal cluster per fleet in the move seam. |
| F-43 | N72 | `api.RequestHash` = SHA-256 over normalised protojson. |
| F-13, F-18, F-25 | N59 | `workflows.DataConverter` (AES-GCM payload codec); `MaxInlineResultBytes = 4 KiB`; continue-as-new at 100 chunks / 20 MB; `SnapshotRepo.ExpireContaining`, `Manifest.Expired`, `Streamer` refusal. |
| F-14 | N60 | `chunk.SummaryPolicy` (REPLACE or > 25 % growth), `HeaderHash(summary id, heading path)`, header embedded into chunk vectors only. |
| F-12 | N58 | `FactRepo.RetireStaleExtraction` run by `FinalizeVersion`. |
| F-1, F-9, F-11 | N41, N47, N57 | `ObservationRepo.MarkVersionsDerivedFromDeleted` (per version, permanent; `LatestAsOf` skips them); `Proposal.InputFactIDs` = rendered evidence (≤ 5 quotes per candidate); `Applier.Apply` inserts sources/inputs before deleting stale ones and never clears version flags. |
| F-19, F-38 | N61 | `LinkRepo.DeleteTouching`/`EntityRepo.DeleteMentionsByFacts` run in the purge; `Neighbours` requires `live` on both endpoints. |
| F-36, F-41 | N69 | `OperationRepo.SetProgress` and `DocumentRepo.SetChunksDone` once per wave, never per chunk; `StatsRepo` derived counters refreshed only by the stats sweeper (`engram_admin`, 60 s); `EntityRepo.UpsertSorted`. |
| F-2 | D9 (amended) | `extract.Fact.MentionedAt` = item timestamp set by the activity; `SaidAt` added; decoder discards model `mentioned_at`. |
| F-33 | N73 | Reflect `search_pages` tool + map/reduce at the context cap; `pages.Service.Search`, `index.SearchPages`, `PageRepo.Search`; MCP `search_pages`. |
| F-23, F-27 | N64, F-27 | `catalog.NamespaceState` gains `creating`/`restoring`; `catalog.ShardState` aligned with the catalog SQL and `adminv1.ShardState`; `engramctl shard` states. |

**Review-2 follow-ups applied (reviews/round-2.md → register D21 → this section):**

| Finding | Register | What changed here |
|---|---|---|
| G-1 | N79 | `consolidate.Proposal.CandidateVersions` = every shown candidate version incl. the previous one; `Applier.Apply` re-verifies them `FOR SHARE` (not flagged, no invalidation, not retired), writes lineage and version-source rows, deletes only shown-and-dropped sources, root rebuilds with `max_rebuilds_per_round`; `ObservationRepo.LockCandidateVersions`, `InsertLineage`, `FlagLineageDescendants` (depth 64, fail closed). |
| G-2 | N80 | `OutboxRepo.AppendAll` (one multi-row last statement), `PagesOf`; `outbox.Group` reassembly; event-size limits in §2.5 and the T1 property test. |
| G-3 | N81 | `store.ReplayRepo`, `Queries` coverage rule, `internal/move/replay_map.go`; `move.Activities.CatchUp` replays through the total mapping; `Drain` re-copies the frozen class (`MOVE_STATE_TOO_LARGE`); the new events are emitted by `OperationRepo`, `IdempotencyRepo`, `ConsolidationRepo`, `SnapshotRepo` and the blob tombstoner. |
| G-4 | N82 | `store.WithNamespaceTx` write prelude = `engram_try_ns_fence` (never waits, `NamespaceFrozen{FENCE_BUSY, 200 ms}`); `OwnershipRepo.LockExclusive` under `lock_timeout` 5 s with jitter; role `lock_timeout` defaults; exports no longer use the session lock; new store tests; §2.5 rows. |
| G-5 | N83 | `store.DocLockRepo` (`TryShared` → `errs.DocumentBusy`, `Exclusive`); `errs.KindDocumentBusy`; retry-table rows. |
| G-6 | N84 | `FactRepo.SetInvalidated` returns `changed`; `ObservationRepo.AdjustInvalidation(±1)`; `LatestAsOf` skips `hidden_by_invalidation > 0`. |
| G-7 | N85 | `index.Filter.AsOf` comment (chunk `embedding_effective_at`, versioned evidence), empty chunk header under `as_of`, `ChunkRepo.InsertBatch`, `ObservationRepo.InsertVersionSources`. |
| G-8 | N86 | `chunk.Item`, `chunk.Chunk.ItemIndexes/MentionedAt`, `Options.MaxItemGap` (24 h forced boundary), `Chunked.TimestampsClamped`; `extract.ChunkInput.ItemTimestamp` = chunk `mentioned_at`. |
| G-9 | N87 | `extract.RenderHash`, `CacheKey.RenderHash`, `BlobKey` and `chunks.extraction_key` include it; cache test seam; N87 × N60 interaction noted (PD-26 in §5). |
| G-10, G-11, G-12, G-22 | N88–N91 | `move.Activities` rewritten: barrier once, single floor `p0`, key-range `Copy` with `CopyProgress`, `VerifyDeep`/`Verify`/`VerifyFK`, scaled `FreezeWatchdog`, `engram_move` TEMP-table load under RLS (no `engram_move_load`), `Options` fields; N2 row corrected. |
| G-13 | N92 | Failover/delete-durability behaviour referenced from `Drain`/`Restart` reuse (restore procedure) and the `UNAVAILABLE` rule; §5.4.1 and §5.5.4 hold the procedure. |
| G-14 | N93 | `OwnershipRepo.Transition` edges from the §3.3.1 table incl. `moved_out → incoming`; `Cleanup` through `engram_cleanup_namespace`, never deleting the ownership row. |
| G-16 | N95 | `store.ConsolidationRepo` (`PendingFacts`, `MarkConsolidated`, `Stamp`, `AdvanceWatermark`); `consolidate.Batch` selection. |
| G-18 | N97 | `Drain` reads `operations`, `Restart` with `TERMINATE_IF_RUNNING` + memo epoch + reconcile loop; every scheduler acts on `active` ownership only. |
| G-19 | N98 | `router.Retry` follows the `MOVED_OUT` target hint first; `errs.WrongShardOrEpochMovedOut`/`StripShardHint`; cutover (c) is the point of no return, (d) indefinite; `Rollback` before (c) only. |
| G-20 | N99 | `workflows.DataConverter` per-namespace data key wrapped by the shard key; `KeyProvider.DataKey/CurrentKeyID/Shred`; key-destruction timing; payload test seam. |
| G-21 | N100 | `RetainActivities.CommitChunk` contract (`DocumentBusy`, `InputBlobMissing`), `errs.KindInputBlobMissing`, `blob` `xcache_grace` rule, `ver` key kind. |
| G-26 | N104 | `RetainActivities.LoadItem`, `blob.Kind "ver"`, `DocumentRepo.SetBody`, `LedgerRepo` retire rule. |
| G-28 | N106 | `recall.PlannerOptions.RerankReserve` = 106 ms; skip-rate statement; §2.5 rerank row. |
| G-29 | N107 | `ChunkRepo.Retire(documentID, version)` document-scoped subquery. |
| G-15 | N94 | `index.SemanticPlan` comments: withdrawn 30 MB / 5–15 ms figure, partial HNSW band `20,000 ≤ live_rows < 2 % of the partition`. |
| G-17, G-23, G-24, G-25, G-27, G-30 | N96, N101, N102, N103, N105, N108 | No module-interface change beyond the rows above (formal work items, transition table, accepted residual channel, one lock-key form `engram_ns_lock_keys`/`engram_doc_lock_keys`, schedule, cascade leaves `invalidated_at`). |
