package api

import (
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/grpc"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/gen/go/memory/v1/memoryv1connect"
)

// RegisterPhase3GRPC registers PageService and ExportService on g with the generated Unimplemented servers: every
// method answers codes.Unimplemented until phase 3, so the contract and the adapters never change shape (N14, section
// 4.1.6). Server.RegisterGRPC calls it, and replaces each registration by the real service when its dependencies are
// wired.
func RegisterPhase3GRPC(g grpc.ServiceRegistrar) {
	memoryv1.RegisterPageServiceServer(g, memoryv1.UnimplementedPageServiceServer{})
	memoryv1.RegisterExportServiceServer(g, memoryv1.UnimplementedExportServiceServer{})
}

// MountPhase3Connect mounts the Connect handlers of PageService and ExportService on mux; every method answers
// connect.CodeUnimplemented (N14).
func MountPhase3Connect(mux *http.ServeMux, ic ...connect.Interceptor) {
	opts := connect.WithInterceptors(ic...)
	path, h := memoryv1connect.NewPageServiceHandler(memoryv1connect.UnimplementedPageServiceHandler{}, opts)
	mux.Handle(path, h)
	path, h = memoryv1connect.NewExportServiceHandler(memoryv1connect.UnimplementedExportServiceHandler{}, opts)
	mux.Handle(path, h)
}
