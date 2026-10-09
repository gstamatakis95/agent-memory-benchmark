// Package mcp is the MCP server adapter (PLAN.md section 2.2.25; register N127, N129, N157, N167). Per-namespace
// endpoints `/mcp/{tenant_id}/{namespace_id}`; tools are generated from memory.v1 at build time
// (`protoc-gen-engram-mcp`) through the checked-in allow-list `adapters/mcp/allow.txt`, so the surface cannot drift
// from the API (4.6). Pattern: Adapter over a gRPC CLIENT of the core: it holds no service and no storage, so it cannot
// bypass the interceptor. It may import only gen/go, authz (scope names, TokenVerifier) and the leaf errs. M0.1
// declares the signatures only.
package mcp

import (
	"encoding/json"
	"net/http"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/authz"
)

// Tool is one generated MCP tool.
type Tool struct {
	Name        string
	Method      string
	InputSchema json.RawMessage
	Scope       authz.Scope
	Streaming   bool
}

// DataClients are the core clients, split so that each interface stays within five methods and every generated tool has
// a client to call through (A-5).
type DataClients interface {
	Memory() memoryv1.MemoryServiceClient
	Document() memoryv1.DocumentServiceClient
	Page() memoryv1.PageServiceClient
	Operation() memoryv1.OperationServiceClient
}

// AdminClients serve get_namespace, get_effective_config, create_snapshot, get_snapshot_manifest and list_snapshots.
type AdminClients interface {
	Namespace() memoryv1.NamespaceServiceClient
	Export() memoryv1.ExportServiceClient
}

// Options configure the adapter.
type Options struct {
	// ResourceMetadataURL is served at /.well-known/oauth-protected-resource (N129).
	ResourceMetadataURL string
}

// Adapter serves MCP over streamable HTTP; the JWT is forwarded unchanged. Idempotency: request_id =
// sha256(mcp-session-id, jsonrpc-id, tool) (N127). Read tools are always listed, write tools only when the JWT carries
// memory.write; the core re-checks.
type Adapter interface {
	Tools() []Tool
	Handler() http.Handler
}

// New builds the adapter. It validates the token with the same TokenVerifier before listing tools (N129).
func New(d DataClients, a AdminClients, tools []Tool, v authz.TokenVerifier, o Options) Adapter {
	panic("stub")
}
