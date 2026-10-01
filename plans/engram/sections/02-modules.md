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
  move/                  MoveOrchestrator: D5 phases as workflow + fenced activities
  export/                SnapshotBuilder + Streamer
  quota/                 Limiter (rates), Meter (tokens), Deferral
  config/                layered config system → tenant → namespace; typed Resolved struct
  telemetry/             OTel tracing + Prometheus conventions; label policy linter
  ledger/                append-only ingest ledger writer
adapters/
  mcp/                   MCP tool definitions derived from protos; per-namespace endpoints; scope gating
  connect/               ConnectRPC handlers mounted on the same mux as gRPC
formal/
  tla/*.tla              Outbox, Move, DeleteVsRetain, Consolidation, AsOf specs (§7)
  lean/Engram/*.lean     TagMatch, RRF, Packing, TemporalWindow proofs (§7)
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
	MaxDeadline map[string]time.Duration // full method → clamp (default 30 s
	Recall 5 s
	Reflect 300 s
	StreamSnapshot 600 s) */
	DefaultPageSize int32                // 50
	MaxPageSize 500 */
	RequestIDTTL time.Duration           // 24 h (D1)
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

// DeadlineGuard is the unary/stream interceptor that rejects missing deadlines and clamps them.
func DeadlineGuard(o Options) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor)
```

Streaming helpers: `StreamBatcher[T]` groups results into batches of 10 and flushes on deadline pressure
(`Recall`), `TokenStreamer` for `Reflect`, and `PartStreamer` (1 MiB parts) for `StreamSnapshot`. A `request_id`
collision with a different request hash returns `ABORTED` + `OperationConflict` (a client reusing keys is a bug,
not a retry).

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

type Claims struct { Subject string; TenantID string; Namespaces []string /* namespace ids, or ["*"] */; Scopes []Scope; ExpiresAt time.Time; KeyID string }

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
type MethodPolicy struct { Scope Scope; Target Target /* TargetNamespace | TargetTenant | TargetAdmin */; RateBucket quota.Bucket /* quota.BucketRecall | quota.BucketRetain | quota.BucketNone */ }

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
§1.3 step 5: no existence oracle); a same-tenant namespace outside the allowlist, or a missing scope, maps to
`PERMISSION_DENIED`; a namespace in state `deleting` maps to `FAILED_PRECONDITION`. For streams, the scope is fixed
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
`LISTEN catalog_changes`, stale-serve 10 min).

**(b) Interfaces.**

```go
package catalog

type NamespaceState string // "active" | "moving" | "frozen" | "deleting" | "deleted"
type ShardState string     // "provisioning" | "active" | "draining" | "retired"

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
	Cutover(ctx context.Context, p CutoverParams) error // one tx: namespaces.shard_id/epoch/state, NOTIFY
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

type ResolverOptions struct { MaxEntries int /* 100_000 */; TTL time.Duration /* 60 s */; NegativeTTL time.Duration /* 5 s */; StaleMax time.Duration /* 10 min */ }
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

// Forwarder is the cross-cell hop (phase 3): one grpc.ClientConn per remote Envoy.
type Forwarder interface {
	Invoke(ctx context.Context, cell, fullMethod string, req, resp proto.Message) error
}

// Retry executes fn with the D2/D5 retry contract: one re-resolve on WrongShardOrEpoch,
// bounded backoff (≤ 30 s) on NamespaceFrozen for writes.
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
Model}` (activities treat it as non-retryable and, for extraction, dead-letter the chunk into `operation_errors`
with the chunk hash; the operation finishes `SUCCEEDED (with a non-empty `errors` list)`). Rationale: a schema-rejecting model will
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

type Kind string // "raw" | "xcache" | "pages" | "export" | "ledger" | "backup"

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

// Tombstones make purges resumable: MarkDeleted writes "{key}.tomb" then Delete removes both.
type Tombstoner interface {
	MarkDeleted(ctx context.Context, key string) error
	PurgeMarked(ctx context.Context, prefix string, olderThan time.Duration) (int, error)
}
```

Key layout (§3 has the full table): `{shard}/{tenant}/{ns}/raw/{sha256}`,
`…/xcache/{sha256(chunk_hash‖prompt_version‖model‖schema_version)}.json` (D11), `…/pages/{page_id}/v{n}.md`,
`…/export/v{n}/…` (D12), `…/ledger/{ledger_id}` for oversized bodies, and `{shard}/backup/{ts}/…` at shard
level.

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
	Read  AccessMode = iota + 1 // ownership state IN ('active','frozen'), no lock
	Write                       // ownership state = 'active' AND epoch = $2 FOR SHARE
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
}

type TxOptions struct { StatementTimeout time.Duration /* 30 s writers, 5 s recall (D6 gap watch depends on it) */; Isolation pgx.TxIsoLevel; ReadOnly bool }

// WithNamespaceTx runs fn inside a transaction that (1) SET LOCALs engram.namespace_id,
// engram.tenant_id, engram.epoch and statement_timeout, (2) performs the ownership check for
// scope.Mode, (3) commits if fn returns nil. Ownership failures surface as
// errs.WrongShardOrEpoch or errs.NamespaceFrozen before fn runs.
func WithNamespaceTx(ctx context.Context, p Pool, scope Scope, o TxOptions, fn func(ctx context.Context, tx Tx) error) error

// OutboxRepo.Append is the only way to emit an event; it is called inside the same tx as the
// state change (D6) and returns the seq assigned by the sequence.
type OutboxRepo interface {
	Append(ctx context.Context, ev *eventsv1.Event) (seq int64, err error)
	ReadFrom(ctx context.Context, afterSeq int64, limit int) ([]OutboxRow, error)        // relay; no namespace fence (shard-level, admin conn)
	ReadNamespaceFrom(ctx context.Context, afterSeq int64, limit int) ([]OutboxRow, error) // move consumer; fenced
	Cursor(ctx context.Context, consumer string) (int64, error)
	AdvanceCursor(ctx context.Context, consumer string, seq int64) error
	Exists(ctx context.Context, seq int64) (bool, error) // gap watchlist probe
	TrimBelow(ctx context.Context, seq int64, olderThan time.Duration, batch int) (int64, error)
}

type FactRepo interface { // representative; the other repositories follow the same shape (table below)
	InsertBatch(ctx context.Context, facts []Fact) error
	RetireByChunkHashes(ctx context.Context, documentID string, hashes [][32]byte, now time.Time) (int64, error)
	Unretire(ctx context.Context, documentID string, hashes [][32]byte) (int64, error)
	SetInvalidated(ctx context.Context, memoryID string, at *time.Time) error // Invalidate / Restore (D8)
	ByIDs(ctx context.Context, ids []string, asOf *time.Time) ([]Fact, error)
}
```

The ownership check is exactly the D2 statement; for `Read` it is `SELECT state FROM namespace_ownership WHERE
namespace_id=$1` and the store rejects `incoming`/`moved_out`/missing with `WrongShardOrEpoch` and accepts
`active`/`frozen`. The write check compares the epoch; a mismatch is `WrongShardOrEpoch{Expected: scope.Epoch,
Observed: row.Epoch}`. Because `FOR SHARE` conflicts with the move executor's `FOR UPDATE` when it flips the row
to `frozen`, an in-flight write either commits before the freeze or observes it — the mechanism §7 `Move.tla`
models.

Repository summary (full DDL in §3):

| Repository | Key methods | Notes |
|---|---|---|
| `DocumentRepo` | `Upsert`, `Get`, `List`, `BeginVersion`, `ActivateVersion`, `MarkDeleted` | `document_id` client-chosen, ≤ 256 B |
| `ChunkRepo` | `LiveByHashes`, `RetiredByHashes`, `InsertBatch`, `Retire`, `Unretire` | identity `(ns, document_id, content_hash)` (D8) |
| `LinkRepo` | `InsertBatch`, `DeleteTouching(factIDs)`, `Neighbours(seed, kinds, limit)` | `fact_links` hash-partitioned; graph arm uses `Neighbours` |
| `EntityRepo` | `Similar(name, type, threshold)` (pg_trgm), `Insert`, `AddAlias`, `InsertMentions`, `DeleteMentionsByFacts` | per-namespace; merge writes aliases, never deletes entities |
| `ObservationRepo` | `Insert`, `NewVersion`, `AddSources`, `DeleteSourcesByFacts`, `MarkStale`, `LatestAsOf(ids, T)` | D9 versioning; trigger retires at 0 sources (D12) |
| `PageRepo` | `Insert`, `NewVersion`, `SetSources`, `DeleteSourcesByFacts`, `MarkStale(write|delete)` | markdown lives in blob |
| `OperationRepo` | `Insert`, `Get`, `List`, `Transition(from,to)`, `SetStats`, `Defer(until)`, `PendingWithoutWorkflow(olderThan)` | states `PENDING/RUNNING/DEFERRED/SUCCEEDED/SUCCEEDED (with a non-empty `errors` list)/FAILED/CANCELLED` |
| `LedgerRepo` | `Append` | append-only; no update/delete method exists |
| `IdempotencyRepo` | `Get`, `Put`, `Expire(olderThan)` | 24 h (D1) |
| `TokenUsageRepo` | `Add(day, op, model, prompt, completion, cost)`, `SumDay` | moves with the namespace (D13) |
| `OwnershipRepo` | `Get`, `Insert(incoming)`, `Transition(from,to,epoch)` `FOR UPDATE` | write methods require the `engram_move` role |

**(c) Dependencies.** `errs`, `telemetry`, `gen/go/engram/internal/events/v1`, `pgx/v5`, `pgxpool`.

**(d) Swappable.** `PgxPool` + `pgxTx` (production; pgbouncer transaction pooling → no session state, `SET LOCAL`
only) → `RecordingPool` (records shard/namespace per statement; isolation tests) → `FakeTx` (in-memory repos with
the same fencing state machine; service unit and property tests).

**(e) Test seam.** testcontainers Postgres with the real migrations; a test-only trigger suite asserts the RLS
policy (a query without `SET LOCAL` returns zero rows), the append-only ledger (any `UPDATE`/`DELETE` on
`ingest_ledger` raises), and the `FOR SHARE`/`FOR UPDATE` interaction (a writer that started before freeze
commits; one that starts after fails).

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

type Hit struct { ID string; Kind Kind; Score float64 /* cosine similarity or BM25 score, arm-local */; MentionedAt time.Time }
type Filter struct {
	AsOf *time.Time               // mentioned_at ≤ AsOf, applied inside the predicate (D9)
	FactTypes []memoryv1.FactType
	Tags []string
	TagMode memoryv1.TagMatchMode // ANY | ALL | STRICT | EXACT | NONE (§4)
	DocumentIDs []string
}
type SemanticQuery struct { Vector []float32; Kinds []Kind /* facts and/or observation_versions */; Filter Filter; Limit int /* 50/150/400 by budget */; EfSearch int /* hnsw.ef_search, default 100 (D3) */ }
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
}

// Sink is what the outbox "index" consumer drives for Async indexes (§2.2.18).
type Sink interface {
	Apply(ctx context.Context, events []*eventsv1.Event) error // idempotent by (namespace_id, seq)
}
```

Search methods take the fenced `tx` so `SET LOCAL engram.namespace_id` (partition pruning + RLS) and `SET LOCAL
hnsw.ef_search / hnsw.iterative_scan` apply. `Filter.AsOf` is rendered into the `WHERE` of every arm, never
applied post hoc (D9). Tag semantics are implemented once in `index/tags.go` and mirrored by the Lean decision
procedure (§7).

**(c) Dependencies.** `store`, `errs`, `telemetry`, `gen/go`.

**(d) Swappable.** `PostgresIndex` (HNSW `halfvec_cosine_ops` + pg_search BM25 + pg_trgm; MVP default, D7) →
`TsvectorIndex` (HNSW + `tsvector`/`ts_rank_cd`; Postgres without pg_search, documented lower lexical quality) →
`ExternalIndex` (`Async`, index `engram-shard-{id}`, fed by the outbox; phase 3+ when BM25 outgrows the instance) →
`MemIndex` (brute-force cosine + in-memory BM25; recall unit and property tests).

**(e) Test seam.** The same retrieval golden set (§8) runs against `PostgresIndex`, `TsvectorIndex` and
`MemIndex`; `as_of` leakage tests assert zero hits with `mentioned_at > T` for every arm.

#### 2.2.9 `internal/chunk` — heading-anchored content-defined chunker

**(a) Responsibility.** Split a document into deterministic chunks (target 3 000 chars, min 500, max 4 000, no
overlap) whose boundaries prefer markdown headings and then content-defined cut points (rolling-hash breakpoints
on paragraph boundaries), and build the contextual header `[doc summary ≤ 200 chars] > [heading path]` (D11).
Pure Go, no I/O.

**(b) Interfaces.**

```go
package chunk

type Document struct { DocumentID string; Content string /* raw text/markdown */; Context string /* item.context, appended to the header if present */ }
type Options struct { TargetChars int /* 3000 */; MinChars int /* 500 */; MaxChars int /* 4000 */; Summary string /* ≤ 200 chars, from SummarizeDocument; "" on first pass */ }
type Chunk struct {
	Ordinal int
	ContentHash [32]byte                         // sha256(Text) — identity within the document (D8)
	Text string                                  // stored in chunks.text (≤ 8 KB)
	Header string                                // stored in chunks.header
	prepended for embedding + extraction only */
	HeadingPath []string
	ByteStart int
	ByteEnd int
	Tokens int                                   // cl100k_base estimate
}

type Chunker interface {
	Chunk(ctx context.Context, doc Document, o Options) ([]Chunk, error)
}

// HeaderBuilder is separate so the header can be recomputed when only the summary changed
// without re-chunking (the content hash excludes the header on purpose).
type HeaderBuilder interface {
	Build(summary string, headingPath []string, docContext string) string
}
```

The hash excludes the header so a re-summarised document does not invalidate the extraction cache or force
re-embedding of unchanged sections — the embedding is recomputed only if the header changed materially
(`FinalizeVersion` compares a header hash). Rejected: hashing header+text (defeats delta retain whenever the
summary drifts).

**(c) Dependencies.** none beyond `tiktoken-go`.

**(d) Swappable.** `CDCChunker` (default) → `FixedChunker` (3 000-char windows, the Hindsight baseline, kept for
A/B in §8) → `SentenceChunker` (evaluation only).

**(e) Test seam.** Golden files (`testdata/*.md` → expected boundaries + hashes) and rapid properties:
concatenation of chunks equals input, every chunk within `[min, max]` except a possibly short last chunk,
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
	MentionedAt   time.Time // defaults to the item timestamp (D9)
	Entities      []EntityMention
	Causes        []CausalRef
	Confidence    float32
}

type ChunkInput struct { Header, Text string; ContentHash [32]byte; ItemTimestamp time.Time; EntityHints, Tags []string }
type Extraction struct { Facts []Fact; PromptVersion string; Model gateway.Model; SchemaVersion string; Usage gateway.Usage; FromCache bool }

type Extractor interface {
	Extract(ctx context.Context, in ChunkInput) (*Extraction, error)
	Summarize(ctx context.Context, content string) (summary string, u *gateway.Usage, err error)
	PromptVersion() string
	SchemaVersion() string
}

type CacheKey struct { Prefix blob.Prefix; ChunkHash [32]byte; PromptVersion string; Model gateway.Model; SchemaVersion string }

func (k CacheKey) BlobKey() string // "{shard}/{tenant}/{ns}/xcache/{sha256(chunk_hash‖prompt‖model‖schema)}.json"

type Cache interface {
	Get(ctx context.Context, k CacheKey) (*Extraction, bool, error)
	Put(ctx context.Context, k CacheKey, x *Extraction) error
}

// CachedExtractor wraps an Extractor with a Cache; cache errors are logged, never returned.
func Cached(e Extractor, c Cache) Extractor
```

Validation after decode: `Text` non-empty, `OccurredStart ≤ OccurredEnd`, entity roles in the enum, causal
indices in range; a violation is `errs.Validation` and, in the activity, counts as `PermanentLLMError` after one
re-prompt with the validation message appended (§6).

**(c) Dependencies.** `gateway`, `blob`, `errs`, `telemetry`.

**(d) Swappable.** `LLMExtractor` (gateway structured call) → `BatchExtractor` (gateway batch API for backfills;
same cache key) → `RuleExtractor` (regex "one fact per paragraph", tests and cost-free smoke runs).

**(e) Test seam.** Record/replay gateway; goldens keyed by prompt version so a prompt bump shows exactly which
extractions changed; cache tests assert the key changes with each of the four inputs and never with the header
alone.

#### 2.2.11 `internal/entity` — per-namespace fuzzy resolution

**(a) Responsibility.** Map extracted mentions to `entities` rows within the namespace using `pg_trgm`
similarity on `canonical_name`, an alias table, type agreement and item hints; decide merges.

**(b) Interfaces.**

```go
package entity

type Mention struct { Name string; Type string; FactIdx int; Role string }
type Candidate struct { EntityID string; Canonical string; Type string; Similarity float32 /* pg_trgm similarity, 0..1 */; Aliases []string; Mentions int64 }
type Resolution struct { Mention Mention; EntityID string; Created bool; Method string /* "exact" | "alias" | "trigram" | "hint" | "new" */; Score float32 }
type Options struct { Threshold float32 /* 0.62 trigram similarity (A-9, tuned in §8) */; MaxCandidates int /* 5 */; TypeStrict bool /* true: never merge across types */ }

type Resolver interface {
	Resolve(ctx context.Context, tx store.Tx, mentions []Mention, hints []string, o Options) ([]Resolution, error)
}

type MergePolicy interface {
	ShouldMerge(m Mention, c Candidate) bool
}
```

Merge policy (default): exact canonical or alias match → merge; trigram ≥ threshold *and* same type → merge and
record the mention text as an alias; otherwise create. Entities are never deleted by resolution; a wrong merge
is repaired by `engramctl entity split` (phase 3). Rejected: LLM-assisted resolution (cost on every chunk; the
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
type LinkBudget struct { TemporalPerFact int /* 20 */; SemanticK int /* 10 */; SemanticMinCosine float32 /* 0.75 */; EntityPerFact int /* 50 (guards hub entities) */; MaxPerFact int /* 100 hard cap across kinds */ }
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

type Budget int // BudgetLow | BudgetMid | BudgetHigh → arm caps 50/150/400, graph nodes 100/300/1000, rerank 50/150/300, tokens 4k/8k/16k

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
	Needs() Needs // NeedsVector | NeedsSeeds | NeedsNone — the planner schedules by this
	Run(ctx context.Context, tx store.Tx, idx index.Index, q *Query, seeds []Candidate, capN int) ([]Candidate, error)
}

type Fused struct { Candidate; RRF float64; ArmRanks map[string]int /* per-stage scores returned to the client */ }

type Fuser interface {
	Fuse(lists [][]Candidate, k int) []Fused // k = 60; permutation-invariant (Lean, §7)
}

type Reranker interface {
	Rerank(ctx context.Context, query string, in []Fused, top int) ([]Ranked, error) // ≤ 300 pairs
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
type PlannerOptions struct { ArmDeadline time.Duration /* 60 ms */; RerankReserve time.Duration /* 150 ms */; EmbedDeadline time.Duration /* 40 ms */ }

type Planner interface {
	Run(ctx context.Context, tx store.Tx, idx index.Index, q *Query, sink Sink) error
}
```

Scheduling (§1.5): `NeedsNone` arms start immediately; `NeedsVector` arms start when the embedding arrives (or
are skipped when it fails); the graph arm (`NeedsSeeds`) starts when semantic and lexical have returned or timed
out. Each arm gets `min(ArmDeadline, remaining − rerankReserve − packReserve)`. The planner records per-stage
timings and arm statuses into `RecallStats`. Rejected: `errgroup` with a shared deadline (one slow arm would
take the others with it).

**(c) Dependencies.** `store`, `index`, `gateway` (reranker, query embedder), `authz` (types), `errs`,
`telemetry`, `tiktoken-go`.

**(d) Swappable.** Five `Arm`s over `index.Index`, `RRFFuser`, `GatewayReranker`, `BoundedBooster`, `GreedyPacker`
(production) → `NoopReranker` (rerank disabled by config; degraded mode) → `FakePlanner` (API handler tests) →
`MemIndex`-backed arms (property tests: as_of leakage, cap adherence).

**(e) Test seam.** Each stage is pure or takes `tx`+`idx`; rapid properties mirror the Lean theorems: RRF
permutation invariance and monotonicity, packing never exceeds the budget and skips exactly the items that do
not fit, boost factor always in `[0.75, 1.25]`.

#### 2.2.14 `internal/consolidate` — facts → observations (phase 2)

**(a) Responsibility.** D12: build batches of 8 unconsolidated facts (≤ 100 per round), fetch top-10 candidate
observations per batch by semantic similarity, ask the LLM for `create/update/delete` ops with cited
`source_fact_ids`, validate, bisect on failure 8 → 4 → 2 → 1, apply idempotently by `op_key`.

**(b) Interfaces.**

```go
package consolidate

type Batch struct { FactIDs []string /* sorted */; Facts []store.Fact; BatchKey [32]byte /* sha256(sorted fact ids ‖ prompt_version ‖ model) */ }

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

type Applier interface {
	// Apply runs in one fenced tx: consolidation_applied(op_key) insert ON CONFLICT DO NOTHING gates each op;
	// new observation_versions get effective_at = max(mentioned_at) of cited facts (D9); outbox events.
	Apply(ctx context.Context, tx store.Tx, b Batch, ops []Op) (applied int, err error)
}

type Consolidator interface {
	Round(ctx context.Context, h *router.ShardHandle, scope store.Scope, o RoundOptions) (*RoundResult, error)
}
```

**(c) Dependencies.** `store`, `index`, `gateway`, `router`, `quota` (token meter and deferral), `errs`. **(d)
Swappable.** `LLMProposer` → `RecordReplayProposer` (tests) → `NoopConsolidator` (namespaces with consolidation
disabled). **(e) Test seam.** Model-based test derived from `Consolidation.tla` (§7): random at-least-once
re-execution of `Apply` must yield exactly one effect per `op_key`; the source-count trigger is tested on
testcontainers.

#### 2.2.15 `internal/reflect` — bounded agent loop (phase 2)

**(a) Responsibility.** D12: forced searches (observations, then facts), ≤ 10 free iterations, ≤ 100 k context
tokens, ≤ 300 s wall, per-tool deadline 10 s; tools `search_memories`, `search_observations`, `get_page`,
`expand_fact`; citations filtered to ids actually returned by tools in the session; optional JSON-schema output
validated server-side; per-namespace mission/directives/disposition.

**(b) Interfaces.**

```go
package reflect

type Caps struct { MaxIterations int /* 10 (after forced searches) */; MaxContextTokens int /* 100_000 */; MaxWall time.Duration /* 300 s */; ToolDeadline time.Duration /* 10 s */ }

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
func PurgeDocument(ctx workflow.Context, in *workflowv1.PurgeDocumentInput) error
func Consolidate(ctx workflow.Context, in *workflowv1.ConsolidateInput) error   // long-lived; signal "wake"; debounce 30 s; ContinueAsNew every 100 rounds or 24 h
func PageRefresh(ctx workflow.Context, in *workflowv1.PageRefreshInput) error
func ExportSnapshot(ctx workflow.Context, in *workflowv1.ExportInput) (*workflowv1.ExportResult, error)
func Move(ctx workflow.Context, in *workflowv1.MoveInput) (*workflowv1.MoveResult, error)
func OpSweeper(ctx workflow.Context, in *workflowv1.OpSweeperInput) error

// Every activity input embeds workflowv1.NamespaceRef{namespace_id, tenant_id, shard_id, epoch};
// activities re-check ownership through store.WithNamespaceTx (D4).
type RetainActivities interface {
	Chunk(ctx context.Context, in *workflowv1.ChunkInput) (*workflowv1.ChunkOutput, error)
	SummarizeDocument(ctx context.Context, in *workflowv1.SummarizeInput) (*workflowv1.SummarizeOutput, error)
	ExtractChunk(ctx context.Context, in *workflowv1.ExtractInput) (*workflowv1.ExtractOutput, error)
	EmbedChunk(ctx context.Context, in *workflowv1.EmbedInput) (*workflowv1.EmbedOutput, error)
	ResolveEntities(ctx context.Context, in *workflowv1.ResolveInput) (*workflowv1.ResolveOutput, error)
	BuildLinks(ctx context.Context, in *workflowv1.LinkInput) (*workflowv1.LinkOutput, error)
	CommitChunk(ctx context.Context, in *workflowv1.CommitInput) (*workflowv1.CommitOutput, error)
	FinalizeVersion(ctx context.Context, in *workflowv1.FinalizeInput) (*workflowv1.FinalizeOutput, error)
}

type ConsolidateActivities interface {
	Round(ctx context.Context, in *workflowv1.RoundInput) (*workflowv1.RoundOutput, error)
	CheckQuota(ctx context.Context, in *workflowv1.QuotaInput) (*workflowv1.QuotaOutput, error) // DEFERRED until window reset
}

type Client interface { // used by engram-api; wraps client.Client
	StartRetain(ctx context.Context, in *workflowv1.RetainDocumentInput) error // AlreadyStarted → nil
	StartPurge(ctx context.Context, in *workflowv1.PurgeDocumentInput) error
	SignalConsolidate(ctx context.Context, ref *workflowv1.NamespaceRef) error // SignalWithStart
	Cancel(ctx context.Context, workflowID string) error
	Describe(ctx context.Context, workflowID string) (*Status, error)
}
```

Per-chunk fan-out in `RetainDocument` uses a workflow-side semaphore of 32; intermediate outputs larger than 64
KiB (extraction results) are passed by blob key, not inline, to keep Temporal history small; `RetainDocument`
`ContinueAsNew`s after 500 chunks.

**Retry policies (activity → initial interval, backoff, max attempts, non-retryable):**

| Activity | Initial | Backoff | Max attempts | Non-retryable error types |
|---|---|---|---|---|
| `Chunk` | 1 s | ×2, cap 10 s | 3 | `ValidationError` |
| `SummarizeDocument`, `ExtractChunk` | 2 s | ×2, cap 60 s | 8 | `PermanentLLMError`, `ValidationError` (after 1 re-prompt), `WrongShardOrEpoch` |
| `EmbedChunk` | 1 s | ×2, cap 30 s | 8 | `PermanentLLMError` (incl. wrong dims), `WrongShardOrEpoch` |
| `ResolveEntities`, `BuildLinks` | 1 s | ×2, cap 30 s | 5 | `WrongShardOrEpoch` |
| `CommitChunk`, `FinalizeVersion` | 500 ms | ×2, cap 30 s | 10 | `WrongShardOrEpoch`; `NamespaceFrozen` is retryable but the *workflow* stops scheduling after 30 s and waits for the move's restart (D5 step 5) |
| `Round` (consolidate) | 5 s | ×2, cap 5 min | 6 | `PermanentLLMError`, `WrongShardOrEpoch`; `QuotaExceeded` → workflow sleeps until reset (`DEFERRED`) |
| `PurgeBlobs`, `PurgeRows` | 5 s | ×2, cap 10 min | unlimited (heartbeat) | `WrongShardOrEpoch` |
| Move `Copy`, `CatchUp` | 5 s | ×2, cap 5 min | 20 | `WrongShardOrEpoch` (fence broken → rollback) |
| Move `Freeze`, `Cutover` | 1 s | ×2, cap 10 s | 5 | `PreconditionFailed` (CAS lost → rollback) |
| `Export*` | 5 s | ×2, cap 5 min | 10 | `ValidationError` |

A `WrongShardOrEpoch` in any activity fails the workflow with a typed failure; the move executor restarts the
recorded operations on the target queue with the same ids (D5 step 6).

**(c) Dependencies.** `go.temporal.io/sdk`, `gen/go/engram/internal/workflow/v1`, `errs`. **(d) Swappable.**
Activities are interfaces; `FakeActivities` for `testsuite` workflow tests. **(e) Test seam.** Temporal
`testsuite.WorkflowTestSuite` with mocked activities: fan-out bound, idempotent re-execution, `ContinueAsNew` at
500 chunks, deferral on quota; a chaos test on testcontainers Temporal kills workers mid-`CommitChunk` and
asserts no duplicate facts.

#### 2.2.18 `internal/outbox` — relay, cursors, gap watchlist, sinks

**(a) Responsibility.** D6: one active relay per shard elected with `pg_try_advisory_lock`, reading `outbox` in
`seq` order in batches of 500, driving named sinks with independent cursors, tracking skipped sequence numbers
in a gap watchlist for `2 × statement_timeout`.

**(b) Interfaces.**

```go
package outbox

type Event struct { Seq int64; NamespaceID string; Epoch int64; Type string; Payload *eventsv1.Event; CreatedAt time.Time }

// Sink consumes events in seq order; Apply must be idempotent by (namespace_id, seq).
type Sink interface {
	Name() string // "index" | "kafka" | "move:<ns>"
	Apply(ctx context.Context, events []Event) error
	Filter() func(e Event) bool // move sinks filter by namespace
}

type RelayOptions struct { Batch int /* 500 */; PollInterval time.Duration /* 200 ms idle */; StatementTimeout time.Duration /* 30 s → gap watch 60 s */; LockKey int64 /* hashtext("engram.relay") — one per shard DB */ }
type Relay struct { /* handle, sinks, cursors, gaps, metrics */ }

func NewRelay(h *router.ShardHandle, sinks []Sink, o RelayOptions) *Relay
func (r *Relay) Run(ctx context.Context) error          // acquires the advisory lock on a dedicated conn; returns when lost
func (r *Relay) AddSink(ctx context.Context, s Sink, fromSeq int64) error // used by move; cursor initialised at p0
func (r *Relay) RemoveSink(name string) error
func (r *Relay) Lag(sink string) (int64, error)

type GapWatch interface { Note(seq int64, seenAt time.Time); Due(now time.Time) []int64; Resolve(seq int64) } // re-probe skipped seqs for 2 × statement_timeout
```

The relay holds a *shard-level* connection (`engram_relay` role, RLS-bypassing read on `outbox` only, since it
must see every namespace) — the one deliberate exception to "every connection has a namespace scope", justified
because the outbox carries no content beyond the proto payload the consumer needs and the relay never writes
namespace tables. Cursor advancement per sink is a separate transaction from sink `Apply`, so at-least-once
delivery is the contract and sinks are idempotent. Rejected: exactly-once via two-phase commit into the external
engine (not available; idempotent keys are cheaper and provable, §7 `Outbox.tla`).

**(c) Dependencies.** `store`, `router`, `index` (Sink adapter), `errs`, `telemetry`, Kafka client (optional
build tag `kafka`). **(d) Swappable.** Sinks: `IndexSink` (no-op for `Transactional` indexes), `KafkaSink`,
`MoveSink`. **(e) Test seam.** Model-based test derived from `Outbox.tla`: random commit orderings with holes;
assert every seq is delivered exactly once to each sink or declared aborted after the watch window, in order per
namespace.

#### 2.2.19 `internal/move` — namespace move orchestrator (D5)

**(a) Responsibility.** Implement the D5 state machine as Temporal workflow `move/{namespace_id}/{epoch}` on
task queue `shard-{target}` (new decision N2) with activities `Plan, Copy, CatchUp, Freeze, Drain, Cutover,
Cleanup, Rollback`, each re-checking the fence before acting. This is the only code path allowed two shard
handles at once; it never runs a statement that references both.

**(b) Interfaces.**

```go
package move

type Phase string // planned | copying | catching_up | frozen | cutover | cleaning | done | rolled_back

// Fence is asserted at the start of every activity against the catalog row, the source
// ownership row and the target ownership row; any mismatch → errs.WrongShardOrEpoch (non-retryable → Rollback).
type Fence struct { NamespaceID string; TenantID string; Source, Target int32; Epoch int64 /* e; target holds e+1 'incoming' until cutover */; MoveID string; ExpectPhase Phase }

type Activities interface {
	Plan(ctx context.Context, f Fence) (*PlanResult, error)                       // target ownership(e+1, incoming); catalog namespaces.state='moving'
	Copy(ctx context.Context, f Fence) (*CopyResult, error)                       // REPEATABLE READ on source; p0 = max(outbox.seq); table-by-table COPY; blob prefix copy; heartbeats
	CatchUp(ctx context.Context, f Fence, fromSeq int64) (*CatchUpResult, error) // replay outbox seq > fromSeq via MoveSink until lag < 100 events or < 5 s
	Freeze(ctx context.Context, f Fence) error                                    // catalog 'frozen'; source ownership 'frozen' FOR UPDATE (same epoch)
	Drain(ctx context.Context, f Fence) (*DrainResult, error)                     // replay to max(seq); terminate ns workflows on source queue; record operation ids
	Cutover(ctx context.Context, f Fence, d *DrainResult) error                   // one catalog tx: shard=target, epoch=e+1, state='active'; target 'active'; source 'moved_out'; NOTIFY; restart ops on target queue
	Cleanup(ctx context.Context, f Fence) error                                   // after 24 h grace: engramctl-equivalent purge of source rows + old blob prefix (admin role)
	Rollback(ctx context.Context, f Fence, reason string) error                   // target ownership → moved_out/deleted rows; source 'active'; catalog 'active'; only before cutover
}

type Orchestrator interface { // admin surface (MoveService)
	Start(ctx context.Context, namespaceID string, target int32, o Options) (*Ref, error)
	Status(ctx context.Context, moveID string) (*Status, error)
	Abort(ctx context.Context, moveID string) error // rollback if before cutover, else error
}

type Options struct {
	FreezeBound time.Duration   // 30 s: if Drain cannot finish, Rollback
	CatchUpMaxLag int           // 100 events
	CatchUpMaxAge time.Duration // 5 s
	CleanupGrace time.Duration  // 24 h
	CopyBatchRows int           // 10 000 per COPY segment (heartbeat granularity)
}
```

Fencing details: `Copy` reads under `REPEATABLE READ` with the source ownership row selected (not locked) at
epoch e; `Freeze` takes `FOR UPDATE` on that row and therefore waits for in-flight `FOR SHARE` writers to finish
— after it commits, no new writer can pass the D2 check; `Cutover` is a CAS on `namespace_moves.state='frozen'`
and `namespaces.epoch=e`, so a concurrent restore-from-backup (which also bumps epoch) causes the CAS to fail
and the move rolls back rather than fighting. `Drain` restarts recorded operations with their original
`operation_id`s and the new epoch; their per-chunk idempotency keys include the epoch, so already-committed
chunks on the target (copied or replayed) are detected by content hash and skipped, not re-extracted.

**(c) Dependencies.** `catalog`, `router`, `store`, `blob`, `outbox` (MoveSink), `workflows` (client), `errs`,
`telemetry`. **(d) Swappable.** `TemporalOrchestrator` → `InlineOrchestrator` (runs phases sequentially
in-process for tests). **(e) Test seam.** Model-based test from `Move.tla` (§7) with `MemoryCatalog` + two
`FakeTx` shards and randomised concurrent retain/consolidate/delete; chaos test on testcontainers kills the
worker in every phase and asserts the invariants (one active owner, no loss/duplication, eventual
done/rolled_back).

#### 2.2.20 `internal/export` — snapshots for local agentic search (phase 3)

**(a) Responsibility.** D12 export layout under `{shard}/{tenant}/{ns}/export/v{n}/` (manifest,
`facts/observations/chunks.jsonl.zst`, `pages/*.md`, `delta-v{n-1}-v{n}.jsonl.zst` derived from the outbox
range) and 1 MiB part streaming through `ExportService`.

```go
package export

type Manifest struct { Version int64; CreatedAt time.Time; AsOf time.Time; OutboxSeq int64 /* for the next delta */; Files []FilePart /* name, size, sha256 */; Epoch int64 }

type SnapshotBuilder interface {
	Build(ctx context.Context, h *router.ShardHandle, scope store.Scope, o BuildOptions) (*Manifest, error) // runs as ExportSnapshot workflow
}

type Streamer interface {
	Stream(ctx context.Context, scope authz.RequestScope, version int64, part func(ctx context.Context, p Part) error) error // 1 MiB parts, resumable by (file, offset)
	Latest(ctx context.Context, scope authz.RequestScope) (*Manifest, error)
}
```

Snapshots honour deletes: a version is built from live rows only, and the delta lists `retired`/`deleted` ids so
a synced client removes them. **(c)** `store`, `blob`, `outbox` (range read), `router`, `errs`. **(d)/(e)** see
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

type Models struct { Extract, Consolidate, Reflect, Embed, Rerank gateway.Model; EmbedDims int /* 768; 512 with Matryoshka truncation */ }
type Resolved struct {
	Models        Models
	Chunk         struct{ TargetChars, MinChars, MaxChars int }
	Recall        struct{ DefaultBudget recall.Budget; RerankEnabled bool }
	Quota         quota.Limits
	Consolidation struct{ Enabled bool; Debounce time.Duration }
	Version       uint64 // hash of the merged layers; changes invalidate derived caches
}

type Resolver interface {
	Resolve(system, tenant, namespace Layer) (*Resolved, error) // namespace ⊃ tenant ⊃ system; unknown keys → errs.Validation
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

// Metrics constructors enforce D13: shard label mandatory, tenant only on metering counters,
// namespace never. Registration with a forbidden label panics at init (caught by a unit test).
type Registry interface {
	Counter(name string, labels ...string) Counter
	Histogram(name string, buckets []float64, labels ...string) Histogram
	Gauge(name string, labels ...string) Gauge
	MeteringCounter(name string) Counter // labels fixed: shard, tenant, op, model
}

func StartStage(ctx context.Context, stage string) (context.Context, func(err error)) // one span per pipeline stage
```

Metric names: `engram_recall_stage_seconds{shard,stage}`, `engram_retain_chunks_total{shard, result}`,
`engram_outbox_lag_seconds{shard,sink}`, `engram_catalog_stale{}`,
`engram_llm_tokens_total{shard,tenant,op,model}` (metering). **(c)** OTel SDK, Prometheus client. **(d)/(e)**
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

Namespace delete removes ledger rows via the admin purge role only (the trigger checks
`current_setting('engram.purge')='on'`). **(c)** `store`, `blob`, `errs`. **(d)/(e)** see §2.3.

#### 2.2.25 `adapters/mcp` — MCP server

**(a) Responsibility.** Expose per-namespace MCP endpoints `/mcp/{tenant_id}/{namespace_id}` with tools derived
from `memory.v1` (`retain`, `recall`, `reflect`, `get_memory`, `list_memories`, `invalidate`, `restore`,
`get_page`, `list_pages`, `get_operation`); read tools are always listed, write tools are listed only when the
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
| `shard provision --id 7 --cell a --dsn …` | creates DB, roles (`engram_app` NOBYPASSRLS, `engram_relay`, `engram_move`, `engram_admin`), extensions, partitions, RLS policies; registers in the catalog as `provisioning` |
| `shard migrate --id 7 [--all --cell a]` | goose migrations per shard with lock; `--canary` runs one shard and stops (§9) |
| `shard activate / drain / retire` | catalog `SetShardState` |
| `shard stats --id 7` | live facts, partitions sizes, index bloat, outbox lag |
| `namespace create / move / purge` | `CreateNamespace`; `MoveService.Start`; post-grace source purge (`Cleanup`) |
| `move status / abort` | `MoveService` |
| `backup create --shard 7` / `restore --shard 7 --to …` | pg_basebackup + WAL to `{shard}/backup/…`; restore bumps epoch for every namespace on the shard (D1) and re-applies delete tombstones recorded after the backup point (§9) |
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
| `internal/move` | `Orchestrator`, `Activities` | `TemporalOrchestrator` | `InlineOrchestrator` | model-based from `Move.tla`; phase-kill chaos |
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
	KindWrongShardOrEpoch                  // FAILED_PRECONDITION + memoryv1.WrongShardOrEpoch{namespace_id, expected_epoch, observed_epoch, observed_state}
	KindNamespaceFrozen                    // FAILED_PRECONDITION + memoryv1.NamespaceFrozen{namespace_id, retry_after}
	KindOperationConflict                  // ABORTED           + memoryv1.OperationConflict{operation_id, request_id}
	KindPreconditionFailed                 // FAILED_PRECONDITION + memoryv1.PreconditionFailed{description}
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
func WrongShardOrEpoch(ns string, expected, observed int64, state string) *Error
func NamespaceFrozen(ns string, retryAfter time.Duration) *Error
func OperationConflict(operationID, requestID string) *Error
func PreconditionFailed(desc string) *Error
func Unavailable(desc string, retryAfter time.Duration) *Error
func PermanentLLM(model, status string, cause error) *Error
func Wrap(kind Kind, msg string, cause error) *Error

// Predicates used by workflows and the router.
func Is(err error, k Kind) bool
func IsRetryable(err error) bool // Unavailable, Deadline, NamespaceFrozen (bounded by the caller)
func Retryable(err error) bool   // alias kept for Temporal's ApplicationError classification

// Mapping helpers.
func ToStatus(err error) *status.Status        // unknown errors → INTERNAL with a redacted message; details attached
func ToConnect(err error) *connect.Error       // same codes/details via connect.Error.AddDetail
func FromStatus(st *status.Status) *Error      // client side (router.Forward, engram-mcp)
func TemporalType(err error) string            // ApplicationError type name: "WrongShardOrEpoch", "NamespaceFrozen", "ValidationError", "PermanentLLMError", "QuotaExceeded"
```

Mapping table (authoritative for §4 and for the retry policies in §2.2.17):

| `errs.Kind` | gRPC code | Typed detail (`memory.v1`) | Retryable | Produced by |
|---|---|---|---|---|
| `Validation` | `INVALID_ARGUMENT` | `ValidationError` | no | api, chunk, extract, config, index (tags) |
| `NotFound` | `NOT_FOUND` | `NotFound` | no | authz (cross-tenant namespace, by design), store |
| `QuotaExceeded` | `RESOURCE_EXHAUSTED` | `QuotaExceeded` + `RetryInfo` | yes (after `retry_after`) | quota via authz (rates); workflows defer instead of failing (tokens/facts) |
| `WrongShardOrEpoch` | `FAILED_PRECONDITION` | `WrongShardOrEpoch` | once (router re-resolve) | store ownership check, move fence |
| `NamespaceFrozen` | `FAILED_PRECONDITION` | `NamespaceFrozen` | bounded (≤ 30 s) | store ownership check (write mode) |
| `OperationConflict` | `ABORTED` | `OperationConflict` | no | api idempotency (same `request_id`, different payload), workflows (`operation_id` reuse) |
| `PreconditionFailed` | `FAILED_PRECONDITION` | `PreconditionFailed` | no | catalog CAS, move Cutover, field-mask/version checks |
| `Unavailable` | `UNAVAILABLE` | `RetryInfo` | yes | catalog miss when down, gateway 429/5xx, Temporal start failure, shard down |
| `PermanentLLM` | `INTERNAL` | `ErrorInfo` | no | gateway 4xx, wrong embedding dims, schema-invalid output after re-prompt |

Every `Kind` has exactly one code; the reverse is not true (`FAILED_PRECONDITION` carries three details), which
is why clients must switch on the detail type, not the code (§4). Rejected: string-matching error messages in
workflows (the Temporal non-retryable list must be by type).

### 2.5 Concurrency and resource limits per process

| Limit | Value | Where enforced | Rationale |
|---|---|---|---|
| Extraction concurrency (LLM structured calls) | 32 in flight per `RetainDocument`; 32 per worker process across workflows (`gateway` per-model semaphore) | workflow semaphore + `gateway.RateLimiter` | D3 formula; more only burns the RPM cap |
| Embedding concurrency | 64 in flight per process; batch ≤ 64 texts per call | `gateway.RateLimiter` | embedding is cheap and fast; cap protects the gateway, not us |
| Per-shard Postgres pool | 16 conns per process per shard; ≤ 32 shards per cell → ≤ 512 per process; pgbouncer `default_pool_size=64` per shard, `max_client_conn=2000` | `router.Options.PoolSize` | D3 cell bound |
| Recall arm parallelism | 5 arms, each one pooled conn; ≤ 50 QPS/shard target → ≤ 250 concurrent arm queries per shard ≤ pool headroom with p95 60 ms | `recall.Planner` | keeps the pool below saturation at target QPS |
| Rerank batch | ≤ 300 pairs per call; 1 call per recall | `recall.GatewayReranker` | D15 |
| Outbox relay batch | 500 rows per read; 1 relay per shard; sinks applied sequentially | `outbox.RelayOptions.Batch` | D6 |
| Gap watchlist | ≤ 10 000 entries; entries expire after `2 × statement_timeout = 60 s` | `outbox.GapWatch` | bounded memory; beyond the cap the relay pauses and alerts |
| Temporal pollers | 2 workflow + 2 activity pollers per `shard-{id}` queue; max concurrent activities 256 per worker | worker options | D3 |
| Consolidation | 8 facts per LLM call, ≤ 100 facts per round, 1 round in flight per namespace | `Consolidate` workflow (singleton id) | D12 |
| Reflect | ≤ 10 iterations, ≤ 100 k context tokens, ≤ 300 s, tool deadline 10 s, ≤ 4 concurrent reflects per namespace (`RESOURCE_EXHAUSTED` beyond) | `reflect.Caps`, api semaphore | D12 |
| Move copy | 10 000 rows per COPY segment; 1 move in flight per namespace; ≤ 4 concurrent moves per cell | `move.Options`, admin API | keeps freeze windows short and source IOPS bounded |
| Catalog resolver | LRU 100 k entries; single-flight per key; LISTEN reconnect backoff 1 s → 30 s | `catalog.ResolverOptions` | D4 |
| Embedding LRU (queries) | 10 000 entries per process, keyed `(namespace_id, sha256(prefixed text))` | `recall.QueryEmbedder` | repeat queries skip the 25 ms hop |
| Streaming / request size | `Recall` batches of 10; `StreamSnapshot` 1 MiB parts; `Retain` ≤ 100 items, ≤ 4 MiB per call, raw body > 64 KiB to blob | `api` helpers and validation | D10, D12; keeps the ledger tx small |

### New decisions (beyond the register)

| Id | Decision | Rationale | Rejected |
|---|---|---|---|
| N1 | Add leaf package `internal/errs` to the D14 layout: typed errors + gRPC/Connect/Temporal mapping; nothing under `internal/` may be imported by it. | Store, services and workflows produce typed errors but may not import `internal/api`. | `internal/api/errors.go` (violates the dependency rule). |
| N2 | The move workflow `move/{namespace_id}/{epoch}` runs on task queue `shard-{target}`; the executing worker opens a second, move-scoped pool to the source (`engram_move` role: read + outbox read + ownership `FOR UPDATE`). It is the only code path allowed two shard handles. | Keeps "one task queue per shard"; the target worker is the one that must be healthy for the move to be useful. | A cell-wide `moves` queue (breaks the per-shard queue rule; harder to reason about pollers). |
| N3 | Per-shard Temporal schedule `shard/{id}/op-sweeper` (every 60 s) starts a workflow for any `PENDING` operation older than 2 min with no workflow; the API still returns `UNAVAILABLE` when `StartWorkflow` fails after the ledger commit. | Closes the crash window between the ledger commit and `StartWorkflow` without weakening the ack. | Ack before `StartWorkflow` and rely solely on the sweeper (silently extends visibility lag). |
| N4 | The outbox relay uses a shard-level `engram_relay` role that bypasses RLS for `SELECT` on `outbox` and `outbox_cursors` only. | The relay must read every namespace's events in `seq` order; per-namespace scans would be O(namespaces) per batch. | Running the relay under `engram_app` with a loop over namespaces. |
| N5 | A cross-tenant namespace returns `NOT_FOUND`; a same-tenant namespace outside the allowlist, or a missing scope, returns `PERMISSION_DENIED`; `deleting` returns `FAILED_PRECONDITION`. | Prevents namespace-id enumeration across tenants while keeping same-tenant misconfiguration diagnosable. | `PERMISSION_DENIED` for cross-tenant too (an existence oracle); `NOT_FOUND` for same-tenant (hides the real problem). |
| N6 | Chunk `content_hash` = `sha256(text)` excluding the contextual header; the header hash is compared separately at `FinalizeVersion` to decide re-embedding. | Delta retain and the extraction cache survive summary drift. | Hashing header+text. |
| N7 | Retain items with a raw body > 64 KiB store the body in blob (`…/ledger/{sha256}`) *before* the ledger transaction; the ledger row keeps the hash and key. | Keeps the ack transaction small; content addressing makes the pre-write idempotent. | Inline bodies of any size (bloats the ledger table and the tx). |
| N8 | Streams fix `RequestScope` at open; token expiry mid-stream does not abort the stream. | Reflect may legitimately run 300 s. | Per-message re-verification. |
