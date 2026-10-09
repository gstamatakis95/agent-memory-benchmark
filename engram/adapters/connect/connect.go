// Package connect mounts the generated ConnectRPC handlers (PLAN.md section 2.2.26; register N71, N157). Mount
// registers the generated memoryv1connect.New*Handler on the same http.ServeMux as the gRPC server (h2c, one port),
// with authz.Interceptor.Connect() and api.DeadlineGuard (Connect-Timeout-Ms maps to the deadline). Pattern: Adapter,
// the same handler implementation, no second annotation layer. Rejected: grpc-gateway. It may import only gen/go, authz
// and the leaf errs (never internal/api), so the handlers arrive as the generated interfaces. M0.1 declares the
// signatures only.
package connect

import (
	"net/http"

	connectrpc "connectrpc.com/connect"

	"github.com/gstamatakis95/engram/gen/go/memory/admin/v1/adminv1connect"
	"github.com/gstamatakis95/engram/gen/go/memory/v1/memoryv1connect"
)

// Services are the nine generated Connect service handlers; the api.Server implements all of them.
type Services struct {
	Memory    memoryv1connect.MemoryServiceHandler
	Document  memoryv1connect.DocumentServiceHandler
	Namespace memoryv1connect.NamespaceServiceHandler
	Operation memoryv1connect.OperationServiceHandler
	Export    memoryv1connect.ExportServiceHandler
	Page      memoryv1connect.PageServiceHandler
	Tenant    adminv1connect.TenantServiceHandler
	Shard     adminv1connect.ShardServiceHandler
	Move      adminv1connect.MoveServiceHandler
}

// Mount registers every handler on mux with the interceptors.
func Mount(mux *http.ServeMux, s Services, ic ...connectrpc.Interceptor) { panic("stub") }
