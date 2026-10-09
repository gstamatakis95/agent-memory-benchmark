// Package api implements every generated memory.v1 / memory.admin.v1 server interface; the Connect handlers delegate to
// the same struct (PLAN.md section 2.2.1; register D1, N11, N14, N70, N71, N72, N115, N122, N143, N157, N159, N162).
// Pattern: Adapter between the wire contract and the services; the cross-cutting rules (deadline, request_id, page
// tokens, field masks, error rendering) live here once. M0.1 declares the signatures only; PageService and
// ExportService are registered answering UNIMPLEMENTED until phase 3 (N14, see unimplemented.go).
package api

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	adminv1 "github.com/gstamatakis95/engram/gen/go/memory/admin/v1"
	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/authz"
	"github.com/gstamatakis95/engram/internal/catalog"
	"github.com/gstamatakis95/engram/internal/config"
	"github.com/gstamatakis95/engram/internal/export"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/intent"
	"github.com/gstamatakis95/engram/internal/move"
	"github.com/gstamatakis95/engram/internal/pages"
	"github.com/gstamatakis95/engram/internal/quota"
	"github.com/gstamatakis95/engram/internal/recall"
	"github.com/gstamatakis95/engram/internal/reflectagent"
	"github.com/gstamatakis95/engram/internal/router"
	"github.com/gstamatakis95/engram/internal/store"
	"github.com/gstamatakis95/engram/internal/workflows"
)

// Options configure the server.
type Options struct {
	// MaxDeadline maps a full method to its cap (N11); over the cap is INVALID_ARGUMENT, never clamped; delete-class
	// RPCs and Restore 40 s (N139).
	MaxDeadline        map[string]time.Duration
	MaxPageSize        int32         // 1000 (200 for ListMemories with text in the mask)
	RequestIDTTL       time.Duration // 24 h (D1)
	MaxOpenWaits       int           // 2 000 WaitOperation long-polls per process (N70)
	ReflectionServices []string      // ["memory.v1"] only, never the admin surface (N71)
}

// Deps is every dependency of every served RPC, each an interface (section 2.3); it is complete for every served RPC
// (N140). The generated coverage table (deps_coverage.go) maps each RPC to a method path through these fields.
type Deps struct {
	// Router and Stores: one store.Store per shard of the cell; a process serves up to 32 shards.
	Router router.ShardRouter
	Stores store.Stores
	// Catalog and its capabilities.
	Catalog   catalog.Namespaces
	NsAdmin   catalog.NamespaceAdmin
	Registry  catalog.Registry
	Tenants   catalog.Tenants
	TenantOps catalog.TenantOps
	// NamespaceService (List/Update via NsAdmin), TenantService (UpdateTenant and UpdateTenantLimits both via
	// Tenants.Update, one scope each), ShardService, MoveService.
	Moves      catalog.Moves
	MoveReader catalog.MoveReader
	Floor      catalog.ReplayFloor
	// Mover serves MoveService.StartMove/RollbackMove/CleanupMove = Start/Abort/Cleanup (api may import move, N157,
	// N167).
	Mover   move.Orchestrator
	Config  config.Resolver // GetEffectiveConfig
	Recall  recall.Planner
	Reflect reflectagent.Agent
	Retain  Submitter
	Deleter Deleter
	Ops     OperationWaiter
	Start   workflows.Starter
	Signal  workflows.Signaller
	Wait    workflows.Waiter
	// PageService: Create/Delete/Refresh, UpdatePage via PageAdmin, GetPage(name) via Reader.ByName.
	Pages      pages.Reader
	PageWriter pages.Writer
	PageAdmin  pages.Admin
	// ExportService: CreateSnapshot, Stream/Latest, GetSnapshotManifest(version) via SnapList.Manifest, ListSnapshots.
	Snapshots export.Builder
	Stream    export.Streamer
	SnapList  export.SnapshotLister
	Intents   intent.Log
	Quota     quota.Limiter
	Clock     func() time.Time
}

// Server implements the nine served services. NewServer panics at wiring time if a row of the coverage table names a
// method no non-nil dependency provides; TestDeps_EveryRPCHasPath asserts the same on every CI run.
type Server struct{}

// NewServer wires the server.
func NewServer(d Deps, o Options) *Server { panic("stub") }

// RegisterGRPC registers the services on g. M0.1 registers only PageService and ExportService, which answer
// UNIMPLEMENTED until phase 3 (N14, RegisterPhase3GRPC); each other service is added here by the milestone that
// implements it.
func (s *Server) RegisterGRPC(g *grpc.Server) { RegisterPhase3GRPC(g) }

// RegisterConnect mounts the Connect handlers on mux with the same scope as RegisterGRPC (MountPhase3Connect).
func (s *Server) RegisterConnect(mux *http.ServeMux, ic ...connect.Interceptor) {
	MountPhase3Connect(mux, ic...)
}

// DeleteOptions are the options of Deleter.DeleteDocument.
type DeleteOptions struct {
	OperationID id.OperationID
	// ExpectedVersion is compared inside the marker transaction.
	ExpectedVersion id.DocVersion
}

// Submitter is the API half of retain (D16): durable ack, then workflow start (5.1.1).
type Submitter interface {
	Submit(ctx context.Context, sc authz.RequestScope, req *memoryv1.RetainRequest) (*memoryv1.Operation, error)
}

// Deleter is the synchronous half of every delete: ONE marker transaction, then the intent object (put after the
// commit, before the ack), then the ack (N115, N122). A duplicate attempt that finds the subject already deleted
// returns the existing operation and first ensures the intent of the marker it found (put-if-absent). The ack is
// conditional (N143): after the intent put the handler RE-READS the marker (a plain read of the tombstone or
// fact_hidden row and its deletion_log row) and acks only if it is still present; if a restore or failover removed it
// in between, the request returns UNAVAILABLE and the client retries (TestIntent_AckRereadsMarker). N159: every method
// takes the SUBJECT lock (store.SubjectLocker, session level, direct connection) BEFORE its marker transaction and
// releases it only AFTER the intent put and that re-read, on every exit path; under the lock it first runs
// help-previous (intent.HelpPrevious: put the intent of the subject's latest deletion_log entry if absent), so a crash
// between a predecessor's commit and put never leaves a hole in the prev_operation_id chain (Durability_NoHelpPrev,
// TestIntent_HelpPrev).
type Deleter interface {
	DeleteDocument(ctx context.Context, sc authz.RequestScope, doc id.DocumentID,
		o DeleteOptions) (*memoryv1.DeleteDocumentResponse, error)
	// DeleteNamespace keeps the client's operation_id.
	DeleteNamespace(ctx context.Context, sc authz.RequestScope, op id.OperationID,
		confirmName string) (*memoryv1.DeleteNamespaceResponse, error)
	// DeleteTenant returns the PENDING operation after the replicated catalog row and the tenant intent; TenantDelete
	// fences every namespace, then stamps acknowledged_at (N182).
	DeleteTenant(ctx context.Context, sc authz.RequestScope, op id.OperationID,
		confirm id.TenantID) (*adminv1.DeleteTenantResponse, error)
	// Invalidate takes the subject lock, then the exclusive document lock (CommitChunk holds it shared), held until the
	// intent put + help-previous (N150, N159, N162, N174); it hides every live fact of the subject under its one
	// invalidation_op (reused when invalidate rows exist; recorded on deletion_log and the intent); a second Invalidate
	// changes no visibility but writes its own row and intent (N143).
	Invalidate(ctx context.Context, sc authz.RequestScope, fact id.FactID, reason string) (*memoryv1.Memory, error)
	// Restore takes subject lock, exclusive document lock, then exclusive derivation lock (N174): 40 s deadline cap; it
	// removes the rows with the invalidation's stamp (N162); a purged fact resolves its subject from curation_log
	// (N174); a repeated Restore writes its own row and intent (N143).
	Restore(ctx context.Context, sc authz.RequestScope, fact id.FactID) (*memoryv1.Memory, error)
}

// OperationWaiter implements WaitOperation: park on Temporal's workflow-result long-poll (at most MaxOpenWaits per
// process), else poll the operations row every 1 s +- 250 ms. DELETE_DOCUMENT polls the tombstone (its workflow is the
// expunge singleton), REFRESH_PAGE polls the page row (singleton-backed, N157), DELETE_NAMESPACE and DELETE_TENANT are
// served from the catalog, derived from `namespaces.state` / the `tenants` row (N70, N133d, N136). CancelOperation on a
// DELETE_* operation is OPERATION_NOT_CANCELLABLE.
type OperationWaiter interface {
	Wait(ctx context.Context, sc authz.RequestScope, op id.OperationID,
		timeout time.Duration) (*memoryv1.Operation, bool /*timedOut*/, error)
}

// RequestHash is the one request-hash function: SHA-256 over the NORMALISED protojson of the request with `meta`
// cleared (keys sorted, unknown fields dropped, canonical Timestamp/Duration/int64 rendering; N72).
func RequestHash(m proto.Message) [32]byte { panic("stub") }

// StreamBatcher batches stream elements in groups of 10 and flushes on deadline pressure (Recall).
type StreamBatcher[T any] struct {
	Size  int
	Flush func(ctx context.Context, batch []T) error
}

// TokenStreamer streams Reflect events.
type TokenStreamer struct{}

// PartStreamer streams 1 MiB export parts.
type PartStreamer struct{}
