// Package authz is the single enforcement point (PLAN.md section 2.2.2; register D13, N5, N65, N70, N71, N127, N128,
// N139, N157, N167). It verifies the JWT, resolves the namespace, checks tenant ownership, the allowlist (ns, ns_group)
// and the scope, takes the rate token and attaches a RequestScope. One implementation serves gRPC unary, gRPC stream
// and Connect; MCP and Connect forward the JWT. Pattern: interceptor chain (a pipeline of three checks) with a
// declarative Policy table instead of per-handler code.
package authz

import (
	"context"
	"time"

	"github.com/gstamatakis95/engram/internal/config"
	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/quota"
)

// Scope is a token scope: "memory.read" | "memory.write" | "memory.admin" | "tenant.admin" (tenant-bound: the tenant's
// own lifecycle, tenant-facing listener) | "engram.operator" (fleet-wide, own audience; CreateTenant,
// UpdateTenantLimits, and GetTenant/DeleteTenant/GetTenantOperation on the admin route; N139, N157, N167) |
// "engram.worker" (the worker's service identity: valid on exactly ReleaseNamespace and CleanupMove, bound to its cell;
// N167).
type Scope string

// The scopes.
const (
	ScopeMemoryRead  Scope = "memory.read"
	ScopeMemoryWrite Scope = "memory.write"
	ScopeMemoryAdmin Scope = "memory.admin"
	ScopeTenantAdmin Scope = "tenant.admin"
	ScopeOperator    Scope = "engram.operator"
	ScopeWorker      Scope = "engram.worker"
)

// Claims are the verified token claims.
type Claims struct {
	Subject       string
	Tenant        id.TenantID
	Namespaces    []id.NamespaceID // the `ns` claim
	AllNamespaces bool             // `ns` contains "*"
	Groups        []string         // the `ns_group` claim (N65)
	Scopes        []Scope
	Cell          id.CellID // the `cell` claim of engram.worker (N167)
	ExpiresAt     time.Time
}

// TokenVerifier checks signature, expiry, issuer and audience; it does not touch the catalog.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (*Claims, error)
}

// RequestScope is id.Scope plus the id.Caller, the caller's Scopes and the resolved config.Resolved. The store only
// ever sees an id.Scope, services an id.Scope and an id.Caller.
type RequestScope struct {
	id.Scope
	Caller id.Caller
	Scopes []Scope
	Config *config.Resolved
}

// Target says what a method's request addresses.
type Target uint8

// The policy targets: a Namespace, a Tenant (token.tenant == request.tenant is enforced) or the Operator surface.
const (
	TargetNamespace Target = iota + 1
	TargetTenant
	TargetOperator
)

// MethodPolicy is one row of the policy table.
type MethodPolicy struct {
	Scope  Scope
	Target Target
	Bucket quota.Bucket
	// AltScope is a second scope accepted on the admin route only: engram.operator for GetTenant, DeleteTenant,
	// GetTenantOperation (it carries no tenant, so the Tenant target check is skipped for it).
	AltScope Scope
	// CellBound: engram.worker's `cell` claim must equal the shard's cell (ReleaseNamespace, CleanupMove).
	CellBound bool
	// AllowDeleting is only for OperationService.GetOperation/WaitOperation (and TenantService.GetTenantOperation): a
	// `deleting` namespace is otherwise FAILED_PRECONDITION (N70, N127).
	AllowDeleting bool
}

// Policy maps a full method name, e.g. "/memory.v1.MemoryService/Recall", to its row.
type Policy map[string]MethodPolicy
